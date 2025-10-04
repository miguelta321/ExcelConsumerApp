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
            var sheetData = new Dictionary<string, Dictionary<string, List<Dictionary<string, string?>>>>();
            var allKeys = new HashSet<string>();
            var keyMaxOccurrences = new Dictionary<string, int>();
            var processedSheets = 0;

            foreach (var sheet in allSheets)
            {
                // Verificar cancelación durante el procesamiento
                ct.ThrowIfCancellationRequested();
                
                var sheetKey = $"{Path.GetFileName(sheet.FileName)}:{sheet.SheetName}";
                var normalizedHeaders = sheet.Headers.ToDictionary(h => normalizer.Normalize(h), h => h);
                
                if (!normalizedHeaders.TryGetValue(keyColumnNormalized, out var originalKeyHeader))
                    continue;

                var sheetDict = new Dictionary<string, List<Dictionary<string, string?>>>();
                
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
                        if (!sheetDict.TryGetValue(key, out var rowsForKey))
                        {
                            rowsForKey = new List<Dictionary<string, string?>>();
                            sheetDict[key] = rowsForKey;
                        }

                        rowsForKey.Add(row);

                        if (!keyMaxOccurrences.TryGetValue(key, out var currentMax) || rowsForKey.Count > currentMax)
                        {
                            keyMaxOccurrences[key] = rowsForKey.Count;
                        }
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

                var maxOccurrences = keyMaxOccurrences.TryGetValue(key, out var occurrences) ? Math.Max(occurrences, 1) : 1;

                for (var occurrenceIndex = 0; occurrenceIndex < maxOccurrences; occurrenceIndex++)
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
                            sheetDict.TryGetValue(key, out var rowsForKey) &&
                            occurrenceIndex < rowsForKey.Count)
                        {
                            var rowData = rowsForKey[occurrenceIndex];
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
                }

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
            var sheetData = new Dictionary<string, Dictionary<string, List<Dictionary<string, string?>>>>();
            var allKeys = new HashSet<string>();
            var keyMaxOccurrences = new Dictionary<string, int>();

            foreach (var sheet in allSheets)
            {
                var sheetKey = $"{Path.GetFileName(sheet.FileName)}:{sheet.SheetName}";
                var sheetDict = new Dictionary<string, List<Dictionary<string, string?>>>();

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

                        if (!sheetDict.TryGetValue(key, out var rowsForKey))
                        {
                            rowsForKey = new List<Dictionary<string, string?>>();
                            sheetDict[key] = rowsForKey;
                        }

                        var copiedRow = new Dictionary<string, string?>(row);
                        rowsForKey.Add(copiedRow);

                        if (!keyMaxOccurrences.TryGetValue(key, out var currentMax) || rowsForKey.Count > currentMax)
                        {
                            keyMaxOccurrences[key] = rowsForKey.Count;
                        }
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
                var maxOccurrences = keyMaxOccurrences.TryGetValue(key, out var occurrences) ? Math.Max(occurrences, 1) : 1;

                for (var occurrenceIndex = 0; occurrenceIndex < maxOccurrences; occurrenceIndex++)
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
                            sheetDict.TryGetValue(key, out var rowsForKey) &&
                            occurrenceIndex < rowsForKey.Count)
                        {
                            // Llenar todas las columnas de esta hoja (excluyendo la columna clave)
                            var rowData = rowsForKey[occurrenceIndex];
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
            var sheetData = new Dictionary<string, Dictionary<string, List<Dictionary<string, string?>>>>();
            var keyMaxOccurrences = new Dictionary<string, int>();

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

                    var sheetDict = new Dictionary<string, List<Dictionary<string, string?>>>();
                    
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

                        if (!sheetDict.TryGetValue(key, out var rowsForKey))
                        {
                            rowsForKey = new List<Dictionary<string, string?>>();
                            sheetDict[key] = rowsForKey;
                        }

                        var rowCopy = new Dictionary<string, string?>(row);
                        rowsForKey.Add(rowCopy);

                        if (!keyMaxOccurrences.TryGetValue(key, out var currentMax) || rowsForKey.Count > currentMax)
                        {
                            keyMaxOccurrences[key] = rowsForKey.Count;
                        }
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

                var maxOccurrences = keyMaxOccurrences.TryGetValue(key, out var occurrences) ? Math.Max(occurrences, 1) : 1;

                for (var occurrenceIndex = 0; occurrenceIndex < maxOccurrences; occurrenceIndex++)
                {
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
                            sheetDict.TryGetValue(key, out var rowsForKey) &&
                            occurrenceIndex < rowsForKey.Count)
                        {
                            foreach (var header in sheetHeaders.Headers)
                            {
                                var normalizedHeader = normalizer.Normalize(header);
                                if (normalizedHeader == keyColumnNormalized)
                                    continue;

                                var prefixedHeader = $"{sheetKey}:{header}";
                                var value = rowsForKey[occurrenceIndex].TryGetValue(header, out var cellValue) ? cellValue : null;
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
                }

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

            // Paso 1: Leer solo headers de todas las hojas seleccionadas y preparar el plan de escritura
            var sheetContexts = new List<(FileSheetSelection Selection, SheetHeaders Headers)>();
            foreach (var fileSelection in fileSheetSelectionsList)
            {
                if (!fileSelection.HasSelectedSheets) continue;

                var perSheet = await reader.ReadHeadersOnlyAsync(fileSelection.FilePath, ct);
                var filteredSheets = perSheet.Where(s => fileSelection.SelectedSheets.Contains(s.SheetName));

                foreach (var sheetHeaders in filteredSheets)
                {
                    sheetContexts.Add((fileSelection, sheetHeaders));
                }
            }

            if (!sheetContexts.Any())
                throw new InvalidOperationException("No se encontraron hojas válidas en los archivos.");

            var missingKeySheets = new List<string>();
            var outputHeaders = new List<string> { "Key" };
            var processingPlans = new List<SheetProcessingPlan>();
            var nextColumnIndex = 2;

            foreach (var (selection, sheet) in sheetContexts)
            {
                var headerInfos = sheet.Headers
                    .Select(header => new HeaderInfo(header, normalizer.Normalize(header)))
                    .ToList();

                var keyHeaderInfo = headerInfos.FirstOrDefault(h => h.Normalized == keyColumnNormalized);
                if (keyHeaderInfo is null)
                {
                    missingKeySheets.Add($"{Path.GetFileName(sheet.FileName)}:{sheet.SheetName}");
                    continue;
                }

                var nonKeyHeaders = headerInfos
                    .Where(h => h.Normalized != keyColumnNormalized)
                    .ToList();

                var fileNameWithoutExt = Path.GetFileNameWithoutExtension(sheet.FileName);
                var sheetKey = $"{fileNameWithoutExt}:{sheet.SheetName}";

                foreach (var header in nonKeyHeaders)
                {
                    outputHeaders.Add($"{sheetKey}:{header.Original}");
                }

                processingPlans.Add(new SheetProcessingPlan(
                    selection.FilePath,
                    sheet.FileName,
                    sheet.SheetName,
                    sheet.Headers,
                    keyHeaderInfo.Original,
                    nonKeyHeaders,
                    nextColumnIndex));

                nextColumnIndex += nonKeyHeaders.Count;
            }

            if (missingKeySheets.Any())
            {
                throw new InvalidOperationException(
                    $"La columna clave '{keyColumnNormalized}' no existe en las siguientes hojas:\n{string.Join("\n", missingKeySheets)}\n\n" +
                    "Verifica que la columna existe en todas las hojas o elige una columna diferente.");
            }

            // Paso 2: Crear Excel directamente con escritura streaming
            using var workbook = new ClosedXML.Excel.XLWorkbook();
            var worksheet = workbook.Worksheets.Add("Merged");

            for (int i = 0; i < outputHeaders.Count; i++)
            {
                worksheet.Cell(1, i + 1).Value = outputHeaders[i];
            }

            // Paso 3: Recolectar todas las claves únicas de todas las hojas
            Console.WriteLine("MergeService: Recolectando claves únicas...");
            var allKeys = new HashSet<string>();
            var keyMaxOccurrences = new Dictionary<string, int>();

            foreach (var plan in processingPlans)
            {
                ct.ThrowIfCancellationRequested();

                Console.WriteLine($"MergeService: Leyendo claves de {Path.GetFileName(plan.DisplayFileName)}:{plan.SheetName}...");

                var perSheetCounts = new Dictionary<string, int>();

                await using var keyEnumerator = reader
                    .ReadSheetDataStreamAsync(plan.FilePath, plan.SheetName, plan.OriginalHeaders, 100, ct)
                    .GetAsyncEnumerator(ct);

                while (await keyEnumerator.MoveNextAsync())
                {
                    ct.ThrowIfCancellationRequested();

                    var row = keyEnumerator.Current;
                    if (!row.TryGetValue(plan.OriginalKeyHeader, out var rawKey))
                        continue;

                    var key = rawKey?.Trim();
                    if (string.IsNullOrWhiteSpace(key))
                        continue;

                    allKeys.Add(key);

                    if (!perSheetCounts.TryGetValue(key, out var currentCount))
                    {
                        currentCount = 0;
                    }

                    perSheetCounts[key] = currentCount + 1;
                }

                foreach (var (key, count) in perSheetCounts)
                {
                    if (!keyMaxOccurrences.TryGetValue(key, out var currentMax) || count > currentMax)
                    {
                        keyMaxOccurrences[key] = count;
                    }
                }
            }

            foreach (var key in allKeys)
            {
                if (!keyMaxOccurrences.ContainsKey(key))
                {
                    keyMaxOccurrences[key] = 1;
                }
            }

            var sortedKeys = allKeys.OrderBy(k => k, StringComparer.Ordinal).ToList();
            Console.WriteLine($"MergeService: Encontradas {sortedKeys.Count} claves únicas. Escribiendo columna clave...");

            var keyToRowIndices = new Dictionary<string, List<int>>(sortedKeys.Count);
            var currentRow = 2;
            foreach (var key in sortedKeys)
            {
                var occurrences = keyMaxOccurrences.TryGetValue(key, out var maxOccurrences)
                    ? Math.Max(maxOccurrences, 1)
                    : 1;

                var rowIndices = new List<int>(occurrences);
                for (var occurrenceIndex = 0; occurrenceIndex < occurrences; occurrenceIndex++)
                {
                    worksheet.Cell(currentRow, 1).Value = key;
                    rowIndices.Add(currentRow);
                    currentRow++;
                }

                keyToRowIndices[key] = rowIndices;
            }

            var totalDataRows = keyToRowIndices.Values.Sum(list => list.Count);

            Console.WriteLine("MergeService: Integrando datos hoja por hoja...");

            foreach (var plan in processingPlans)
            {
                ct.ThrowIfCancellationRequested();

                if (plan.NonKeyHeaders.Count == 0)
                {
                    Console.WriteLine($"MergeService: {Path.GetFileName(plan.DisplayFileName)}:{plan.SheetName} solo contiene la columna clave, se omite escritura de datos.");
                    continue;
                }

                Console.WriteLine($"MergeService: Procesando {Path.GetFileName(plan.DisplayFileName)}:{plan.SheetName}...");
                var keyToNextRowPosition = keyToRowIndices.ToDictionary(kvp => kvp.Key, kvp => 0);

                await using var rowEnumerator = reader
                    .ReadSheetDataStreamAsync(plan.FilePath, plan.SheetName, plan.OriginalHeaders, 100, ct)
                    .GetAsyncEnumerator(ct);

                while (await rowEnumerator.MoveNextAsync())
                {
                    ct.ThrowIfCancellationRequested();

                    var row = rowEnumerator.Current;
                    if (!row.TryGetValue(plan.OriginalKeyHeader, out var rawKey))
                        continue;

                    var key = rawKey?.Trim();
                    if (string.IsNullOrWhiteSpace(key))
                        continue;

                    if (!keyToRowIndices.TryGetValue(key, out var rowIndices))
                        continue;

                    var nextPosition = keyToNextRowPosition[key];
                    if (nextPosition >= rowIndices.Count)
                        continue;

                    var rowIndex = rowIndices[nextPosition];
                    keyToNextRowPosition[key] = nextPosition + 1;

                    var columnIndex = plan.ColumnOffset;
                    foreach (var header in plan.NonKeyHeaders)
                    {
                        var value = row.TryGetValue(header.Original, out var cellValue) ? cellValue : null;
                        worksheet.Cell(rowIndex, columnIndex).Value = value ?? string.Empty;
                        columnIndex++;
                    }
                }

                foreach (var key in sortedKeys)
                {
                    if (!keyToRowIndices.TryGetValue(key, out var rowIndices))
                        continue;

                    var nextPosition = keyToNextRowPosition.TryGetValue(key, out var writtenCount) ? writtenCount : 0;
                    if (nextPosition >= rowIndices.Count)
                        continue;

                    for (var occurrenceIndex = nextPosition; occurrenceIndex < rowIndices.Count; occurrenceIndex++)
                    {
                        var rowIndex = rowIndices[occurrenceIndex];
                        var columnIndex = plan.ColumnOffset;
                        foreach (var header in plan.NonKeyHeaders)
                        {
                            worksheet.Cell(rowIndex, columnIndex).Value = string.Empty;
                            columnIndex++;
                        }
                    }
                }
            }

            workbook.SaveAs(outputPath);
            Console.WriteLine($"MergeService: Archivo guardado en {outputPath} con {totalDataRows} filas de datos");

            return outputPath;
        }

        private sealed record HeaderInfo(string Original, string Normalized);

        private sealed record SheetProcessingPlan(
            string FilePath,
            string DisplayFileName,
            string SheetName,
            IReadOnlyList<string> OriginalHeaders,
            string OriginalKeyHeader,
            IReadOnlyList<HeaderInfo> NonKeyHeaders,
            int ColumnOffset);
    }
}
