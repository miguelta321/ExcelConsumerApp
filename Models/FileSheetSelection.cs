using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;

namespace ExcelConsumerApp.Models
{
    /// <summary>Representa la selección de hojas para un archivo específico.</summary>
    public class FileSheetSelection : INotifyPropertyChanged
    {
        private ObservableCollection<SheetSelectionItem> _sheetItems = new();

        public string FileName { get; init; } = "";
        public string FilePath { get; init; } = "";
        public List<string> AvailableSheets { get; init; } = new();
        
        public ObservableCollection<SheetSelectionItem> SheetItems 
        { 
            get => _sheetItems; 
            set 
            { 
                _sheetItems = value; 
                OnPropertyChanged(); 
                OnPropertyChanged(nameof(SelectedSheets));
            } 
        }

        public List<string> SelectedSheets 
        { 
            get 
            {
                var selected = SheetItems.Where(s => s.IsSelected).Select(s => s.SheetName).ToList();
                Console.WriteLine($"FileSheetSelection.SelectedSheets: {FileName} -> {selected.Count} hojas: [{string.Join(", ", selected)}]");
                return selected;
            }
        }

        /// <summary>Inicializa con todas las hojas seleccionadas por defecto.</summary>
        public static FileSheetSelection CreateWithAllSheets(string filePath, List<string> availableSheets)
        {
            var selection = new FileSheetSelection
            {
                FileName = System.IO.Path.GetFileName(filePath),
                FilePath = filePath,
                AvailableSheets = availableSheets
            };
            
            // Crear items para cada hoja, todas seleccionadas por defecto
            foreach (var sheet in availableSheets)
            {
                var item = new SheetSelectionItem 
                { 
                    SheetName = sheet, 
                    IsSelected = true,
                    Parent = selection
                };
                selection.SheetItems.Add(item);
            }
            
            return selection;
        }

        /// <summary>Verifica si hay hojas seleccionadas.</summary>
        public bool HasSelectedSheets => SelectedSheets.Any();

        /// <summary>Obtiene las hojas no seleccionadas.</summary>
        public List<string> UnselectedSheets => AvailableSheets.Except(SelectedSheets).ToList();

        /// <summary>Obtiene una representación de texto de las hojas disponibles.</summary>
        public string AvailableSheetsText => string.Join(", ", AvailableSheets);

        /// <summary>Selecciona todas las hojas.</summary>
        public void SelectAll()
        {
            foreach (var item in SheetItems)
            {
                item.IsSelected = true;
            }
            OnPropertyChanged(nameof(SelectedSheets));
        }

        public event PropertyChangedEventHandler? PropertyChanged;

        public virtual void OnPropertyChanged([CallerMemberName] string? propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }
}
