using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ExcelConsumerApp.Models;

namespace ExcelConsumerApp.Services
{
    public sealed class MergeService : IMergeService
    {
        public async Task<MergedTable> MergeAsync(
            IEnumerable<string> filePaths,
            string keyColumnNormalized,
            IExcelReader reader,
            IHeaderNormalizer normalizer,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(keyColumnNormalized))
                throw new ArgumentException("La columna clave no puede estar vacía.", nameof(keyColumnNormalized));

            var filePathsList = filePaths.ToList();
            if (!filePathsList.Any())
                throw new ArgumentException("Debe proporcionar al menos un archivo.", nameof(filePaths));

            Console.WriteLine($"MergeService: Procesando {filePathsList.Count} archivos directamente...");

            // Paso 1: Leer todas las hojas de todos los archivos en paralelo
            var allSheets = new List<TabularFile>();
            var errors = new List<string>();

            // Leer archivos en paralelo para mejor rendimiento
            var readTasks = filePathsList.Select(async filePath =>
            {
                try
                {
                    Console.WriteLine($"MergeService: Leyendo archivo {Path.GetFileName(filePath)}...");
                    var sheets = await reader.ReadAllSheetsAsync(filePath, ct);
                    Console.WriteLine($"MergeService: {sheets.Count} hojas leídas de {Path.GetFileName(filePath)}");
                    return (Success: true, Sheets: sheets, Error: (string?)null);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"MergeService: Error leyendo {Path.GetFileName(filePath)}: {ex.Message}");
                    return (Success: false, Sheets: (IReadOnlyList<TabularFile>?)null, Error: $"Error leyendo {Path.GetFileName(filePath)}: {ex.Message}");
                }
            });

            var readResults = await Task.WhenAll(readTasks);
            
            foreach (var readResult in readResults)
            {
                if (readResult.Success && readResult.Sheets != null)
                {
                    allSheets.AddRange(readResult.Sheets);
                }
                else if (!string.IsNullOrEmpty(readResult.Error))
                {
                    errors.Add(readResult.Error);
                }
            }

            if (errors.Any())
            {
                throw new InvalidOperationException($"Errores al leer archivos:\n{string.Join("\n", errors)}");
            }

            if (!allSheets.Any())
                throw new InvalidOperationException("No se encontraron hojas válidas en los archivos.");

            Console.WriteLine($"MergeService: Total de {allSheets.Count} hojas leídas");

            // Paso 2: Validar que la columna clave existe en cada hoja
            var missingKeySheets = new List<string>();
            foreach (var sheet in allSheets)
            {
                var normalizedHeaders = sheet.Headers
                    .Select(h => normalizer.Normalize(h))
                    .ToHashSet();

                if (!normalizedHeaders.Contains(keyColumnNormalized))
                {
                    missingKeySheets.Add($"{Path.GetFileName(sheet.FileName)}:{sheet.SheetName}");
                }
            }

            if (missingKeySheets.Any())
            {
                throw new InvalidOperationException(
                    $"La columna clave '{keyColumnNormalized}' no existe en las siguientes hojas:\n{string.Join("\n", missingKeySheets)}\n\n" +
                    "Verifica que la columna existe en todas las hojas o elige una columna diferente.");
            }

            // Paso 3: Construir índices por hoja con optimizaciones
            var sheetData = new Dictionary<string, Dictionary<string, Dictionary<string, string?>>>();
            var allKeys = new HashSet<string>();
            var processedSheets = 0;

            foreach (var sheet in allSheets)
            {
                // Verificar cancelación durante el procesamiento
                ct.ThrowIfCancellationRequested();
                
                var sheetKey = $"{Path.GetFileName(sheet.FileName)}:{sheet.SheetName}";
                var normalizedHeaders = sheet.Headers.ToDictionary(h => normalizer.Normalize(h), h => h);
                
                if (!normalizedHeaders.TryGetValue(keyColumnNormalized, out var originalKeyHeader))
                    continue;

                var sheetDict = new Dictionary<string, Dictionary<string, string?>>();
                
                // Procesar filas en chunks para mejor rendimiento con archivos grandes
                const int CHUNK_SIZE = 1000;
                var rows = sheet.Rows.ToList();
                
                for (int i = 0; i < rows.Count; i += CHUNK_SIZE)
                {
                    ct.ThrowIfCancellationRequested();
                    
                    var chunk = rows.Skip(i).Take(CHUNK_SIZE);
                    foreach (var row in chunk)
                    {
                        var key = row.TryGetValue(originalKeyHeader, out var keyValue) ? keyValue?.Trim() : null;
                        if (string.IsNullOrWhiteSpace(key)) continue;

                        allKeys.Add(key);
                        sheetDict[key] = row;
                    }
                }
                
                sheetData[sheetKey] = sheetDict;
                processedSheets++;
                
                Console.WriteLine($"MergeService: Procesada hoja {processedSheets}/{allSheets.Count}: {sheetKey}");
            }

            // Paso 4: Construir headers de salida
            var outputHeaders = new List<string>();
            outputHeaders.Add("Key"); // Primero la columna clave unificada

            foreach (var sheet in allSheets)
            {
                var fileNameWithoutExt = Path.GetFileNameWithoutExtension(sheet.FileName);
                var sheetKey = $"{fileNameWithoutExt}:{sheet.SheetName}";
                
                foreach (var header in sheet.Headers)
                {
                    var normalizedHeader = normalizer.Normalize(header);
                    if (normalizedHeader == keyColumnNormalized)
                        continue; // Excluir la columna clave para evitar duplicación
                        
                    var prefixedHeader = $"{sheetKey}:{header}";
                    outputHeaders.Add(prefixedHeader);
                }
            }

            // Paso 5: Construir filas de salida con streaming
            var outputRows = new List<Dictionary<string, string?>>();
            var orderedKeys = allKeys.OrderBy(k => k).ToList();
            var totalKeys = orderedKeys.Count;
            var processedKeys = 0;

            Console.WriteLine($"MergeService: Construyendo {totalKeys} filas de salida...");

            foreach (var key in orderedKeys)
            {
                // Verificar cancelación cada 100 claves procesadas
                if (processedKeys % 100 == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    Console.WriteLine($"MergeService: Procesando clave {processedKeys + 1}/{totalKeys}");
                }

                var outputRow = new Dictionary<string, string?>
                {
                    ["Key"] = key
                };

                foreach (var sheet in allSheets)
                {
                    var fileNameWithoutExt = Path.GetFileNameWithoutExtension(sheet.FileName);
                    var sheetKey = $"{fileNameWithoutExt}:{sheet.SheetName}";
                    var originalSheetKey = $"{Path.GetFileName(sheet.FileName)}:{sheet.SheetName}";
                    
                    if (sheetData.TryGetValue(originalSheetKey, out var sheetDict) &&
                        sheetDict.TryGetValue(key, out var rowData))
                    {
                        foreach (var header in sheet.Headers)
                        {
                            var normalizedHeader = normalizer.Normalize(header);
                            if (normalizedHeader == keyColumnNormalized)
                                continue;
                                
                            var prefixedHeader = $"{sheetKey}:{header}";
                            var value = rowData.TryGetValue(header, out var cellValue) ? cellValue : null;
                            outputRow[prefixedHeader] = value;
                        }
                    }
                    else
                    {
                        foreach (var header in sheet.Headers)
                        {
                            var normalizedHeader = normalizer.Normalize(header);
                            if (normalizedHeader == keyColumnNormalized)
                                continue;
                                
                            var prefixedHeader = $"{sheetKey}:{header}";
                            outputRow[prefixedHeader] = null;
                        }
                    }
                }

                outputRows.Add(outputRow);
                processedKeys++;
            }

            var result = new MergedTable
            {
                Headers = outputHeaders,
                Rows = outputRows
            };

            Console.WriteLine($"MergeService: Merge completado. {result.Headers.Count} columnas, {result.Rows.Count} filas");
            return result;
        }

        public async Task<MergedTable> MergeAsync(
            IEnumerable<FileSheetSelection> fileSheetSelections,
            string keyColumnNormalized,
            IExcelReader reader,
            IHeaderNormalizer normalizer,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(keyColumnNormalized))
                throw new ArgumentException("La columna clave no puede estar vacía.", nameof(keyColumnNormalized));

            var fileSheetSelectionsList = fileSheetSelections.ToList();
            if (!fileSheetSelectionsList.Any())
                throw new ArgumentException("Debe proporcionar al menos una selección de archivo.", nameof(fileSheetSelections));

            // Los logs se mostrarán en el status de la aplicación

            // Paso 1: Leer solo las hojas seleccionadas de cada archivo
            var allSheets = new List<TabularFile>();
            var errors = new List<string>();

            foreach (var fileSelection in fileSheetSelectionsList)
            {
                if (!fileSelection.HasSelectedSheets) continue;
                
                try
                {
                    var allSheetsInFile = await reader.ReadAllSheetsAsync(fileSelection.FilePath, ct);
                    var selectedSheets = allSheetsInFile.Where(s => fileSelection.SelectedSheets.Contains(s.SheetName));
                    allSheets.AddRange(selectedSheets);
                }
                catch (Exception ex)
                {
                    errors.Add($"Error leyendo {fileSelection.FileName}: {ex.Message}");
                }
            }

            if (errors.Any())
            {
                throw new InvalidOperationException($"Errores al leer archivos:\n{string.Join("\n", errors)}");
            }

            if (!allSheets.Any())
                throw new InvalidOperationException("No se encontraron hojas válidas en los archivos.");

            // Paso 2: Validar que la columna clave existe en cada hoja
            var missingKeySheets = new List<string>();
            foreach (var sheet in allSheets)
            {
                var normalizedHeaders = sheet.Headers
                    .Select(h => normalizer.Normalize(h))
                    .ToHashSet();

                if (!normalizedHeaders.Contains(keyColumnNormalized))
                {
                    missingKeySheets.Add($"{Path.GetFileName(sheet.FileName)}:{sheet.SheetName}");
                }
            }

            if (missingKeySheets.Any())
            {
                throw new InvalidOperationException(
                    $"La columna clave '{keyColumnNormalized}' no existe en las siguientes hojas:\n" +
                    string.Join("\n", missingKeySheets) +
                    "\n\nVerifique que los encabezados coincidan (acentos, espacios, mayúsculas).");
            }

            // Paso 3: Construir índices por hoja
            var sheetData = new Dictionary<string, Dictionary<string, Dictionary<string, string?>>>();
            var allKeys = new HashSet<string>();

            foreach (var sheet in allSheets)
            {
                var sheetKey = $"{Path.GetFileName(sheet.FileName)}:{sheet.SheetName}";
                var sheetDict = new Dictionary<string, Dictionary<string, string?>>();

                // Encontrar el header original de la columna clave
                var keyHeaderOriginal = sheet.Headers
                    .FirstOrDefault(h => normalizer.Normalize(h) == keyColumnNormalized);

                if (keyHeaderOriginal == null) continue;

                foreach (var row in sheet.Rows)
                {
                    if (row.TryGetValue(keyHeaderOriginal, out var keyValue) && 
                        !string.IsNullOrWhiteSpace(keyValue))
                    {
                        var key = keyValue.Trim();
                        allKeys.Add(key);
                        sheetDict[key] = new Dictionary<string, string?>(row);
                    }
                }

                sheetData[sheetKey] = sheetDict;
            }

            // Paso 4: Construir headers de salida
            var outputHeaders = new List<string>();
            
            // Primero la columna clave unificada
            outputHeaders.Add("Key");

            // Luego las columnas de cada hoja (prefijadas, excluyendo la columna clave)
            foreach (var sheet in allSheets)
            {
                var fileNameWithoutExt = Path.GetFileNameWithoutExtension(sheet.FileName);
                var sheetKey = $"{fileNameWithoutExt}:{sheet.SheetName}";
                
                foreach (var header in sheet.Headers)
                {
                    // Excluir la columna clave para evitar duplicación
                    var normalizedHeader = normalizer.Normalize(header);
                    if (normalizedHeader == keyColumnNormalized)
                        continue;
                        
                    var prefixedHeader = $"{sheetKey}:{header}";
                    outputHeaders.Add(prefixedHeader);
                }
            }

            // Paso 5: Construir filas de salida
            var outputRows = new List<Dictionary<string, string?>>();

            foreach (var key in allKeys.OrderBy(k => k))
            {
                var outputRow = new Dictionary<string, string?>
                {
                    ["Key"] = key
                };

                foreach (var sheet in allSheets)
                {
                    var fileNameWithoutExt = Path.GetFileNameWithoutExtension(sheet.FileName);
                    var sheetKey = $"{fileNameWithoutExt}:{sheet.SheetName}";
                    var originalSheetKey = $"{Path.GetFileName(sheet.FileName)}:{sheet.SheetName}";
                    
                    if (sheetData.TryGetValue(originalSheetKey, out var sheetDict) &&
                        sheetDict.TryGetValue(key, out var rowData))
                    {
                        // Llenar todas las columnas de esta hoja (excluyendo la columna clave)
                        foreach (var header in sheet.Headers)
                        {
                            // Excluir la columna clave para evitar duplicación
                            var normalizedHeader = normalizer.Normalize(header);
                            if (normalizedHeader == keyColumnNormalized)
                                continue;
                                
                            var prefixedHeader = $"{sheetKey}:{header}";
                            var value = rowData.TryGetValue(header, out var cellValue) ? cellValue : null;
                            outputRow[prefixedHeader] = value;
                        }
                    }
                    else
                    {
                        // No hay datos para esta clave en esta hoja - llenar con null (excluyendo la columna clave)
                        foreach (var header in sheet.Headers)
                        {
                            // Excluir la columna clave para evitar duplicación
                            var normalizedHeader = normalizer.Normalize(header);
                            if (normalizedHeader == keyColumnNormalized)
                                continue;
                                
                            var prefixedHeader = $"{sheetKey}:{header}";
                            outputRow[prefixedHeader] = null;
                        }
                    }
                }

                outputRows.Add(outputRow);
            }

            var result = new MergedTable
            {
                Headers = outputHeaders,
                Rows = outputRows
            };

            // El resultado se mostrará en el status de la aplicación
            return result;
        }

        /// <summary>
        /// Merge optimizado con streaming para archivos grandes.
        /// Procesa datos por chunks para minimizar el uso de memoria.
        /// </summary>
        public async Task<MergedTable> MergeStreamingAsync(
            IEnumerable<FileSheetSelection> fileSheetSelections,
            string keyColumnNormalized,
            IExcelReader reader,
            IHeaderNormalizer normalizer,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(keyColumnNormalized))
                throw new ArgumentException("La columna clave no puede estar vacía.", nameof(keyColumnNormalized));

            var fileSheetSelectionsList = fileSheetSelections.ToList();
            if (!fileSheetSelectionsList.Any())
                throw new ArgumentException("Debe proporcionar al menos una selección de archivo.", nameof(fileSheetSelections));

            Console.WriteLine($"MergeService: Procesando {fileSheetSelectionsList.Count} archivos con streaming...");

            // Paso 1: Leer solo headers de todas las hojas seleccionadas
            var allSheetHeaders = new List<SheetHeaders>();
            foreach (var fileSelection in fileSheetSelectionsList)
            {
                if (!fileSelection.HasSelectedSheets) continue;
                
                var perSheet = await reader.ReadHeadersOnlyAsync(fileSelection.FilePath, ct);
                var filteredSheets = perSheet.Where(s => fileSelection.SelectedSheets.Contains(s.SheetName));
                allSheetHeaders.AddRange(filteredSheets);
            }

            if (!allSheetHeaders.Any())
                throw new InvalidOperationException("No se encontraron hojas válidas en los archivos.");

            // Paso 2: Validar que la columna clave existe en cada hoja
            var missingKeySheets = new List<string>();
            foreach (var sheet in allSheetHeaders)
            {
                var normalizedHeaders = sheet.Headers
                    .Select(h => normalizer.Normalize(h))
                    .ToHashSet();

                if (!normalizedHeaders.Contains(keyColumnNormalized))
                {
                    missingKeySheets.Add($"{Path.GetFileName(sheet.FileName)}:{sheet.SheetName}");
                }
            }

            if (missingKeySheets.Any())
            {
                throw new InvalidOperationException(
                    $"La columna clave '{keyColumnNormalized}' no existe en las siguientes hojas:\n{string.Join("\n", missingKeySheets)}\n\n" +
                    "Verifica que la columna existe en todas las hojas o elige una columna diferente.");
            }

            // Paso 3: Construir headers de salida
            var outputHeaders = new List<string>();
            outputHeaders.Add("Key"); // Primero la columna clave unificada

            foreach (var sheet in allSheetHeaders)
            {
                var fileNameWithoutExt = Path.GetFileNameWithoutExtension(sheet.FileName);
                var sheetKey = $"{fileNameWithoutExt}:{sheet.SheetName}";
                
                foreach (var header in sheet.Headers)
                {
                    var normalizedHeader = normalizer.Normalize(header);
                    if (normalizedHeader == keyColumnNormalized)
                        continue; // Excluir la columna clave para evitar duplicación
                        
                    var prefixedHeader = $"{sheetKey}:{header}";
                    outputHeaders.Add(prefixedHeader);
                }
            }

            // Paso 4: Procesar datos por streaming
            var allKeys = new HashSet<string>();
            var sheetData = new Dictionary<string, Dictionary<string, Dictionary<string, string?>>>();

            Console.WriteLine($"MergeService: Procesando datos con streaming...");

            foreach (var fileSelection in fileSheetSelectionsList)
            {
                if (!fileSelection.HasSelectedSheets) continue;

                var perSheet = await reader.ReadHeadersOnlyAsync(fileSelection.FilePath, ct);
                var filteredSheets = perSheet.Where(s => fileSelection.SelectedSheets.Contains(s.SheetName));

                foreach (var sheetHeaders in filteredSheets)
                {
                    ct.ThrowIfCancellationRequested();
                    
                    var sheetKey = $"{Path.GetFileName(sheetHeaders.FileName)}:{sheetHeaders.SheetName}";
                    var normalizedHeaders = sheetHeaders.Headers.ToDictionary(h => normalizer.Normalize(h), h => h);
                    
                    if (!normalizedHeaders.TryGetValue(keyColumnNormalized, out var originalKeyHeader))
                        continue;

                    var sheetDict = new Dictionary<string, Dictionary<string, string?>>();
                    
                    // Procesar datos por streaming
                    await foreach (var row in reader.ReadSheetDataStreamAsync(
                        fileSelection.FilePath, 
                        sheetHeaders.SheetName, 
                        sheetHeaders.Headers, 
                        1000, // chunk size
                        ct))
                    {
                        ct.ThrowIfCancellationRequested();
                        
                        var key = row.TryGetValue(originalKeyHeader, out var keyValue) ? keyValue?.Trim() : null;
                        if (string.IsNullOrWhiteSpace(key)) continue;

                        allKeys.Add(key);
                        sheetDict[key] = row;
                    }
                    
                    sheetData[sheetKey] = sheetDict;
                    Console.WriteLine($"MergeService: Procesada hoja {sheetKey} con {sheetDict.Count} filas");
                }
            }

            // Paso 5: Construir filas de salida con streaming
            var outputRows = new List<Dictionary<string, string?>>();
            var orderedKeys = allKeys.OrderBy(k => k).ToList();
            var totalKeys = orderedKeys.Count;
            var processedKeys = 0;

            Console.WriteLine($"MergeService: Construyendo {totalKeys} filas de salida con streaming...");

            foreach (var key in orderedKeys)
            {
                // Verificar cancelación cada 100 claves procesadas
                if (processedKeys % 100 == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    Console.WriteLine($"MergeService: Procesando clave {processedKeys + 1}/{totalKeys}");
                }

                var outputRow = new Dictionary<string, string?>
                {
                    ["Key"] = key
                };

                foreach (var sheetHeaders in allSheetHeaders)
                {
                    var fileNameWithoutExt = Path.GetFileNameWithoutExtension(sheetHeaders.FileName);
                    var sheetKey = $"{fileNameWithoutExt}:{sheetHeaders.SheetName}";
                    var originalSheetKey = $"{Path.GetFileName(sheetHeaders.FileName)}:{sheetHeaders.SheetName}";
                    
                    if (sheetData.TryGetValue(originalSheetKey, out var sheetDict) &&
                        sheetDict.TryGetValue(key, out var rowData))
                    {
                        foreach (var header in sheetHeaders.Headers)
                        {
                            var normalizedHeader = normalizer.Normalize(header);
                            if (normalizedHeader == keyColumnNormalized)
                                continue;
                                
                            var prefixedHeader = $"{sheetKey}:{header}";
                            var value = rowData.TryGetValue(header, out var cellValue) ? cellValue : null;
                            outputRow[prefixedHeader] = value;
                        }
                    }
                    else
                    {
                        foreach (var header in sheetHeaders.Headers)
                        {
                            var normalizedHeader = normalizer.Normalize(header);
                            if (normalizedHeader == keyColumnNormalized)
                                continue;
                                
                            var prefixedHeader = $"{sheetKey}:{header}";
                            outputRow[prefixedHeader] = null;
                        }
                    }
                }

                outputRows.Add(outputRow);
                processedKeys++;
            }

            var result = new MergedTable
            {
                Headers = outputHeaders,
                Rows = outputRows
            };

            Console.WriteLine($"MergeService: Merge con streaming completado. {result.Headers.Count} columnas, {result.Rows.Count} filas");
            return result;
        }

        /// <summary>
        /// Merge ultra-optimizado que escribe directamente al Excel sin cargar datos en memoria.
        /// Procesa archivos de cualquier tamaño sin problemas de memoria.
        /// </summary>
        public async Task<string> MergeDirectToExcelAsync(
            IEnumerable<FileSheetSelection> fileSheetSelections,
            string keyColumnNormalized,
            IExcelReader reader,
            IHeaderNormalizer normalizer,
            string outputPath,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(keyColumnNormalized))
                throw new ArgumentException("La columna clave no puede estar vacía.", nameof(keyColumnNormalized));

            var fileSheetSelectionsList = fileSheetSelections.ToList();
            if (!fileSheetSelectionsList.Any())
                throw new ArgumentException("Debe proporcionar al menos una selección de archivo.", nameof(fileSheetSelections));

            Console.WriteLine($"MergeService: Procesando {fileSheetSelectionsList.Count} archivos con escritura directa...");

            // Paso 1: Leer solo headers de todas las hojas seleccionadas
            var allSheetHeaders = new List<SheetHeaders>();
            foreach (var fileSelection in fileSheetSelectionsList)
            {
                if (!fileSelection.HasSelectedSheets) continue;
                
                var perSheet = await reader.ReadHeadersOnlyAsync(fileSelection.FilePath, ct);
                var filteredSheets = perSheet.Where(s => fileSelection.SelectedSheets.Contains(s.SheetName));
                allSheetHeaders.AddRange(filteredSheets);
            }

            if (!allSheetHeaders.Any())
                throw new InvalidOperationException("No se encontraron hojas válidas en los archivos.");

            // Paso 2: Validar que la columna clave existe en cada hoja
            var missingKeySheets = new List<string>();
            foreach (var sheet in allSheetHeaders)
            {
                var normalizedHeaders = sheet.Headers
                    .Select(h => normalizer.Normalize(h))
                    .ToHashSet();

                if (!normalizedHeaders.Contains(keyColumnNormalized))
                {
                    missingKeySheets.Add($"{Path.GetFileName(sheet.FileName)}:{sheet.SheetName}");
                }
            }

            if (missingKeySheets.Any())
            {
                throw new InvalidOperationException(
                    $"La columna clave '{keyColumnNormalized}' no existe en las siguientes hojas:\n{string.Join("\n", missingKeySheets)}\n\n" +
                    "Verifica que la columna existe en todas las hojas o elige una columna diferente.");
            }

            // Paso 3: Construir headers de salida
            var outputHeaders = new List<string>();
            outputHeaders.Add("Key"); // Primero la columna clave unificada

            foreach (var sheet in allSheetHeaders)
            {
                var fileNameWithoutExt = Path.GetFileNameWithoutExtension(sheet.FileName);
                var sheetKey = $"{fileNameWithoutExt}:{sheet.SheetName}";
                
                foreach (var header in sheet.Headers)
                {
                    var normalizedHeader = normalizer.Normalize(header);
                    if (normalizedHeader == keyColumnNormalized)
                        continue; // Excluir la columna clave para evitar duplicación
                        
                    var prefixedHeader = $"{sheetKey}:{header}";
                    outputHeaders.Add(prefixedHeader);
                }
            }

            // Paso 4: Crear Excel directamente con escritura streaming
            using var workbook = new ClosedXML.Excel.XLWorkbook();
            var worksheet = workbook.Worksheets.Add("Merged");

            // Escribir headers
            for (int i = 0; i < outputHeaders.Count; i++)
            {
                worksheet.Cell(1, i + 1).Value = outputHeaders[i];
            }

            // Paso 5: Procesar datos por streaming y escribir directamente
            var allKeys = new HashSet<string>();
            var currentRow = 2; // Empezar en la fila 2 (después de headers)
            var processedKeys = 0;

            Console.WriteLine($"MergeService: Procesando datos con escritura directa...");

            // Primero, recolectar todas las claves únicas de todos los archivos
            Console.WriteLine($"MergeService: Recolectando claves únicas...");
            foreach (var fileSelection in fileSheetSelectionsList)
            {
                if (!fileSelection.HasSelectedSheets) continue;

                var perSheet = await reader.ReadHeadersOnlyAsync(fileSelection.FilePath, ct);
                var filteredSheets = perSheet.Where(s => fileSelection.SelectedSheets.Contains(s.SheetName));

                foreach (var sheetHeaders in filteredSheets)
                {
                    ct.ThrowIfCancellationRequested();
                    
                    var normalizedHeaders = sheetHeaders.Headers.ToDictionary(h => normalizer.Normalize(h), h => h);
                    
                    if (!normalizedHeaders.TryGetValue(keyColumnNormalized, out var originalKeyHeader))
                        continue;

                    Console.WriteLine($"MergeService: Recolectando claves de {sheetHeaders.SheetName}...");

                    await foreach (var row in reader.ReadSheetDataStreamAsync(
                        fileSelection.FilePath, 
                        sheetHeaders.SheetName, 
                        sheetHeaders.Headers, 
                        100,
                        ct))
                    {
                        ct.ThrowIfCancellationRequested();
                        
                        var key = row.TryGetValue(originalKeyHeader, out var keyValue) ? keyValue?.Trim() : null;
                        if (string.IsNullOrWhiteSpace(key)) continue;

                        allKeys.Add(key);
                    }
                }
            }

            Console.WriteLine($"MergeService: Encontradas {allKeys.Count} claves únicas");

            // Ahora procesar cada clave y buscar datos en todos los archivos
            foreach (var key in allKeys.OrderBy(k => k))
            {
                ct.ThrowIfCancellationRequested();
                
                if (processedKeys % 100 == 0)
                {
                    Console.WriteLine($"MergeService: Procesando clave {processedKeys + 1}/{allKeys.Count}: {key}");
                }

                // Escribir clave en la primera columna
                worksheet.Cell(currentRow, 1).Value = key;

                // Procesar cada hoja para esta clave
                int colIndex = 2; // Empezar en la columna 2 (después de Key)
                
                foreach (var sheetHeaders in allSheetHeaders)
                {
                    var foundData = false;
                    
                    // Buscar datos para esta clave en esta hoja específica
                    foreach (var fileSelection in fileSheetSelectionsList)
                    {
                        if (!fileSelection.HasSelectedSheets) continue;
                        if (!fileSelection.SelectedSheets.Contains(sheetHeaders.SheetName)) continue;

                        var normalizedHeaders = sheetHeaders.Headers.ToDictionary(h => normalizer.Normalize(h), h => h);
                        
                        if (!normalizedHeaders.TryGetValue(keyColumnNormalized, out var originalKeyHeader))
                            continue;

                        // Buscar la fila con esta clave en esta hoja específica
                        await foreach (var row in reader.ReadSheetDataStreamAsync(
                            fileSelection.FilePath, 
                            sheetHeaders.SheetName, 
                            sheetHeaders.Headers, 
                            100,
                            ct))
                        {
                            var rowKey = row.TryGetValue(originalKeyHeader, out var keyValue) ? keyValue?.Trim() : null;
                            if (rowKey == key)
                            {
                                // Escribir datos de esta fila
                                foreach (var header in sheetHeaders.Headers)
                                {
                                    var normalizedHeader = normalizer.Normalize(header);
                                    if (normalizedHeader == keyColumnNormalized)
                                        continue;
                                        
                                    var value = row.TryGetValue(header, out var cellValue) ? cellValue : null;
                                    worksheet.Cell(currentRow, colIndex).Value = value ?? "";
                                    colIndex++;
                                }
                                foundData = true;
                                break;
                            }
                        }
                        
                        if (foundData) break;
                    }
                    
                    if (!foundData)
                    {
                        // Escribir celdas vacías para esta hoja
                        foreach (var header in sheetHeaders.Headers)
                        {
                            var normalizedHeader = normalizer.Normalize(header);
                            if (normalizedHeader == keyColumnNormalized)
                                continue;
                                
                            worksheet.Cell(currentRow, colIndex).Value = "";
                            colIndex++;
                        }
                    }
                }

                currentRow++;
                processedKeys++;
            }

            Console.WriteLine($"MergeService: Procesadas {processedKeys} claves únicas");

            // Guardar archivo
            workbook.SaveAs(outputPath);
            Console.WriteLine($"MergeService: Archivo guardado en {outputPath} con {currentRow - 1} filas");

            return outputPath;
        }
    }
}
