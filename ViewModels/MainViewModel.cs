using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ExcelConsumerApp.Commands;
using ExcelConsumerApp.Utils;
using ExcelConsumerApp.Services;
using ExcelConsumerApp.Models;

namespace ExcelConsumerApp.ViewModels
{
    public sealed class MainViewModel : ViewModelBase
    {
        // Servicios inyectados
        private readonly IFileDialogService _fileDialog;
        private readonly IExcelReader _excelReader;
        private readonly IHeaderNormalizer _normalizer;
        private readonly IMergeService _mergeService;
        private readonly IExcelWriter _excelWriter;

        // Estado interno
        private List<string> _selectedFiles = new();
        private List<FileSheetSelection> _fileSheetSelections = new();

        // Backing fields
        private string? _selectedColumn;
        private string _status = "Sin archivos cargados.";
        private int _filesCount;
        private bool _showSheetSelection = false;
        private int _progressPercentage = 0;
        private string _progressMessage = "";
        private bool _isProcessing = false;
        private CancellationTokenSource? _cancellationTokenSource;

        // Propiedades bindables
        public ObservableCollection<string> CommonColumns { get; } = new();
        public ObservableCollection<FileSheetSelection> FileSheetSelections { get; } = new();

        public string? SelectedColumn
        {
            get => _selectedColumn;
            set
            {
                if (SetProperty(ref _selectedColumn, value))
                    StartMergeCommand.RaiseCanExecuteChanged();
            }
        }

        public int FilesCount
        {
            get => _filesCount;
            private set
            {
                if (SetProperty(ref _filesCount, value))
                    StartMergeCommand.RaiseCanExecuteChanged();
            }
        }

        public string Status
        {
            get => _status;
            private set => SetProperty(ref _status, value);
        }

        public bool ShowSheetSelection
        {
            get => _showSheetSelection;
            private set => SetProperty(ref _showSheetSelection, value);
        }

        public int ProgressPercentage
        {
            get => _progressPercentage;
            set => SetProperty(ref _progressPercentage, value);
        }

        public string ProgressMessage
        {
            get => _progressMessage;
            set => SetProperty(ref _progressMessage, value);
        }

        public bool IsProcessing
        {
            get => _isProcessing;
            private set
            {
                if (SetProperty(ref _isProcessing, value))
                {
                    StartMergeCommand.RaiseCanExecuteChanged();
                    SelectFilesCommand.RaiseCanExecuteChanged();
                }
            }
        }

        // Comandos
        public AsyncRelayCommand SelectFilesCommand { get; }
        public AsyncRelayCommand StartMergeCommand { get; }
        public AsyncRelayCommand ConfigureSheetsCommand { get; }
        public AsyncRelayCommand ConfirmSheetSelectionCommand { get; }
        public AsyncRelayCommand SelectAllSheetsCommand { get; }
        public RelayCommand CancelOperationCommand { get; }

        // Ctor: inyecta servicios
        public MainViewModel(IFileDialogService fileDialog,
                             IExcelReader excelReader,
                             IHeaderNormalizer normalizer,
                             IMergeService mergeService,
                             IExcelWriter excelWriter)
        {
            _fileDialog = fileDialog ?? throw new ArgumentNullException(nameof(fileDialog));
            _excelReader = excelReader ?? throw new ArgumentNullException(nameof(excelReader));
            _normalizer = normalizer ?? throw new ArgumentNullException(nameof(normalizer));
            _mergeService = mergeService ?? throw new ArgumentNullException(nameof(mergeService));
            _excelWriter = excelWriter ?? throw new ArgumentNullException(nameof(excelWriter));

            SelectFilesCommand = new AsyncRelayCommand(OnSelectFilesAsync, CanSelectFiles);
            StartMergeCommand = new AsyncRelayCommand(OnStartMergeAsync, CanStartMerge);
            ConfigureSheetsCommand = new AsyncRelayCommand(OnConfigureSheetsAsync, CanConfigureSheets);
            ConfirmSheetSelectionCommand = new AsyncRelayCommand(OnConfirmSheetSelectionAsync);
            SelectAllSheetsCommand = new AsyncRelayCommand(OnSelectAllSheetsAsync, CanSelectAllSheets);
            CancelOperationCommand = new RelayCommand(OnCancelOperation, CanCancelOperation);
        }

        // === Selección de archivos y cálculo de intersección por HOJA ===
        private async Task OnSelectFilesAsync()
        {
            var files = _fileDialog.ShowOpenFiles("Excel (*.xlsx)|*.xlsx").ToList();
            _selectedFiles = files;
            FilesCount = files.Count;

            CommonColumns.Clear();
            FileSheetSelections.Clear();
            SelectedColumn = null;
            ShowSheetSelection = false;

            if (FilesCount == 0)
            {
                Status = "Selección cancelada.";
                return;
            }

            Status = "Leyendo hojas disponibles...";

            // Cargar hojas disponibles para cada archivo (solo headers, sin datos)
            var readTasks = files.Select(async filePath =>
            {
                try
                {
                    Status = $"Leyendo headers de {System.IO.Path.GetFileName(filePath)}...";
                    var perSheet = await _excelReader.ReadHeadersOnlyAsync(filePath);
                    var sheetNames = perSheet.Select(s => s.SheetName).ToList();
                    
                    Status = $"Encontradas {sheetNames.Count} hojas en {System.IO.Path.GetFileName(filePath)}: [{string.Join(", ", sheetNames)}]";
                    
                    return (Success: true, FilePath: filePath, SheetNames: sheetNames, Error: (string?)null);
                }
                catch (Exception ex)
                {
                    Status = $"❌ Error leyendo {System.IO.Path.GetFileName(filePath)}: {ex.Message}";
                    return (Success: false, FilePath: filePath, SheetNames: (List<string>?)null, Error: ex.Message);
                }
            });

            var results = await Task.WhenAll(readTasks);
            
            foreach (var result in results)
            {
                if (result.Success && result.SheetNames != null && result.SheetNames.Any())
                {
                    var fileSelection = FileSheetSelection.CreateWithAllSheets(result.FilePath, result.SheetNames);
                    FileSheetSelections.Add(fileSelection);
                    Status = $"✅ {System.IO.Path.GetFileName(result.FilePath)}: {fileSelection.SheetItems.Count} hojas agregadas";
                }
                else if (!result.Success)
                {
                    Status = $"⚠️ {System.IO.Path.GetFileName(result.FilePath)}: {result.Error}";
                }
                else
                {
                    Status = $"⚠️ {System.IO.Path.GetFileName(result.FilePath)}: No hay hojas válidas";
                }
            }

            Status = $"📊 Total de selecciones creadas: {FileSheetSelections.Count}";
            
            if (!FileSheetSelections.Any())
            {
                Status = "No se encontraron hojas válidas en los archivos.";
                return;
            }

            // Calcular columnas comunes de todas las hojas seleccionadas
            Status = "Calculando columnas comunes...";
            var allSheetsNormalizedSets = new List<HashSet<string>>();
            
            foreach (var fileSelection in FileSheetSelections)
            {
                var perSheet = await _excelReader.ReadHeadersPerSheetAsync(fileSelection.FilePath);
                var filteredSheets = perSheet.Where(s => fileSelection.SelectedSheets.Contains(s.SheetName));
                
                foreach (var sh in filteredSheets)
                {
                    var normalized = sh.Headers
                        .Select(h => _normalizer.Normalize(h))
                        .Where(n => !string.IsNullOrWhiteSpace(n))
                        .ToHashSet();

                    if (normalized.Count > 0)
                        allSheetsNormalizedSets.Add(normalized);
                }
            }

            if (allSheetsNormalizedSets.Count == 0)
            {
                Status = "No se encontraron hojas con encabezados válidos.";
                return;
            }

            // Intersección global entre TODAS las hojas seleccionadas
            var common = allSheetsNormalizedSets
                .Skip(1)
                .Aggregate(new HashSet<string>(allSheetsNormalizedSets.First()),
                           (acc, next) => { acc.IntersectWith(next); return acc; });

            CommonColumns.Clear();
            foreach (var name in common.OrderBy(s => s))
                CommonColumns.Add(name);

            // Si solo hay una columna común, pasar directamente al merge
            if (CommonColumns.Count == 1)
            {
                SelectedColumn = CommonColumns.First();
                ShowSheetSelection = false;
                Status = $"Solo hay una columna común: '{SelectedColumn}'. Procediendo directamente al merge.";
            }
            else if (CommonColumns.Count > 1)
            {
                // Mostrar interfaz de selección de hojas
                ShowSheetSelection = true;
                Status = $"Selecciona las hojas a usar de cada archivo. Por defecto todas están seleccionadas.";
            }
            else
            {
                Status = "No hay columnas en común entre las hojas seleccionadas.";
            }
        }

        private bool CanConfigureSheets()
            => FilesCount > 0 && ShowSheetSelection;

        private bool CanSelectAllSheets()
            => FilesCount > 0 && ShowSheetSelection;

        private async Task OnConfigureSheetsAsync()
        {
            // Este método se puede usar para abrir una ventana de configuración
            // Por ahora, solo confirmamos la selección actual
            await OnConfirmSheetSelectionAsync();
        }

        private Task OnSelectAllSheetsAsync()
        {
            try
            {
                Status = "Seleccionando todas las hojas...";
                
                foreach (var fileSelection in FileSheetSelections)
                {
                    fileSelection.SelectAll();
                }
                
                Status = "Todas las hojas han sido seleccionadas.";
            }
            catch (Exception ex)
            {
                Status = $"Error al seleccionar todas las hojas: {ex.Message}";
            }
            
            return Task.CompletedTask;
        }

        private async Task OnConfirmSheetSelectionAsync()
        {
            try
            {
                Status = "Calculando columnas comunes...";
                
                // Obtener solo las hojas seleccionadas (solo headers, sin datos)
                var selectedSheets = new List<SheetHeaders>();
                var readTasks = FileSheetSelections
                    .Where(fs => fs.HasSelectedSheets)
                    .Select(async fileSelection =>
                    {
                        var perSheet = await _excelReader.ReadHeadersOnlyAsync(fileSelection.FilePath);
                        return perSheet.Where(s => fileSelection.SelectedSheets.Contains(s.SheetName));
                    });

                var results = await Task.WhenAll(readTasks);
                foreach (var filteredSheets in results)
                {
                    selectedSheets.AddRange(filteredSheets);
                }

                if (!selectedSheets.Any())
                {
                    Status = "No hay hojas seleccionadas.";
                    return;
                }

                // Calcular intersección con las hojas seleccionadas
                var allSheetsNormalizedSets = new List<HashSet<string>>();
                foreach (var sh in selectedSheets)
                {
                    var normalized = sh.Headers
                        .Select(h => _normalizer.Normalize(h))
                        .Where(n => !string.IsNullOrWhiteSpace(n))
                        .ToHashSet();

                    if (normalized.Count > 0)
                        allSheetsNormalizedSets.Add(normalized);
                }

                if (allSheetsNormalizedSets.Count == 0)
                {
                    Status = "No se encontraron hojas con encabezados válidos.";
                    return;
                }

                // Intersección global entre TODAS las hojas seleccionadas
                var common = allSheetsNormalizedSets
                    .Skip(1)
                    .Aggregate(new HashSet<string>(allSheetsNormalizedSets.First()),
                               (acc, next) => { acc.IntersectWith(next); return acc; });

                CommonColumns.Clear();
                foreach (var name in common.OrderBy(s => s))
                    CommonColumns.Add(name);

                SelectedColumn = CommonColumns.FirstOrDefault();
                ShowSheetSelection = false;
                
                Status = CommonColumns.Count > 0
                    ? $"Encontradas {CommonColumns.Count} columna(s) común(es) en las hojas seleccionadas."
                    : "No hay columnas en común entre las hojas seleccionadas.";
            }
            catch (Exception ex)
            {
                Status = $"Error al procesar la selección: {ex.Message}";
            }
        }

        private async Task OnStartMergeAsync()
        {
            try
            {
                IsProcessing = true;
                _cancellationTokenSource = new CancellationTokenSource();
                var ct = _cancellationTokenSource.Token;

                Status = "Iniciando merge...";
                ProgressPercentage = 0;
                ProgressMessage = "Preparando merge...";
                
                // Solicitar ruta de destino
                Status = "Selecciona dónde guardar el archivo...";
                var savePath = _fileDialog.ShowSaveFile("Excel (.xlsx)|*.xlsx", "merged_data.xlsx");
                
                if (string.IsNullOrWhiteSpace(savePath))
                {
                    Status = "Operación cancelada por el usuario.";
                    return;
                }

                Status = $"Ruta seleccionada: {savePath}";
                
                // Verificar cancelación
                ct.ThrowIfCancellationRequested();
                
                // Usar las hojas seleccionadas correctamente
                Status = "📋 Usando solo las hojas seleccionadas...";
                ProgressPercentage = 10;
                ProgressMessage = "Verificando hojas seleccionadas...";
                
                // Verificar que hay hojas seleccionadas
                var totalSelectedSheets = FileSheetSelections.Sum(fs => fs.SelectedSheets.Count);
                Status = $"📊 Total de hojas seleccionadas: {totalSelectedSheets}";
                
                if (totalSelectedSheets == 0)
                {
                    Status = "❌ No hay hojas seleccionadas para procesar.";
                    return;
                }
                
                // Verificar cancelación
                ct.ThrowIfCancellationRequested();
                
                // Mostrar detalles de las hojas seleccionadas
                foreach (var fileSelection in FileSheetSelections)
                {
                    var selectedNames = fileSelection.SelectedSheets;
                    Status = $"🔍 {fileSelection.FileName}: {selectedNames.Count} hojas seleccionadas ({string.Join(", ", selectedNames)})";
                }
                
                Status = $"🔑 Columna clave: {SelectedColumn}";
                Status = "🔄 Ejecutando merge de archivos...";
                ProgressPercentage = 20;
                ProgressMessage = "Ejecutando merge...";
                
                // Verificar cancelación
                ct.ThrowIfCancellationRequested();
                
                // Usar el método ultra-optimizado que escribe directamente al Excel
                var outputPath = await ((MergeService)_mergeService).MergeDirectToExcelAsync(
                    FileSheetSelections, 
                    SelectedColumn!, 
                    _excelReader, 
                    _normalizer,
                    savePath,
                    ct);

                Status = $"Merge completado y guardado directamente en: {outputPath}";
                ProgressPercentage = 100;
                ProgressMessage = "Completado";
                
                // Verificar que el archivo se creó
                if (System.IO.File.Exists(savePath))
                {
                    var fileInfo = new System.IO.FileInfo(savePath);
                    Status = $"✅ Archivo generado exitosamente en: {savePath} ({fileInfo.Length} bytes)";
                    ProgressPercentage = 100;
                    ProgressMessage = "Completado";
                }
                else
                {
                    Status = $"❌ Error: El archivo no se creó en {savePath}";
                }
            }
            catch (OperationCanceledException)
            {
                Status = "Operación cancelada por el usuario.";
            }
            catch (Exception ex)
            {
                Status = $"❌ Error durante el merge: {ex.Message}";
                Status = $"❌ Detalles del error: {ex.StackTrace}";
            }
            finally
            {
                IsProcessing = false;
                ProgressPercentage = 0;
                ProgressMessage = "";
                _cancellationTokenSource?.Dispose();
                _cancellationTokenSource = null;
            }
        }

        // === Métodos de cancelación ===
        private void OnCancelOperation()
        {
            _cancellationTokenSource?.Cancel();
            Status = "Operación cancelada por el usuario.";
            IsProcessing = false;
            ProgressPercentage = 0;
            ProgressMessage = "";
        }

        private bool CanCancelOperation() => IsProcessing;

        // === Métodos CanExecute actualizados ===
        private bool CanSelectFiles() => !IsProcessing;

        private bool CanStartMerge()
            => FilesCount > 0 && !string.IsNullOrWhiteSpace(SelectedColumn) && !ShowSheetSelection && !IsProcessing;
    }
}
