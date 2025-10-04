using ClosedXML.Excel;
using ExcelConsumerApp.Models;

namespace ExcelConsumerApp.Services
{
    public sealed class ClosedXmlExcelReader : IExcelReader
    {
        public Task<IReadOnlyList<string>> ReadHeadersAsync(string path, CancellationToken ct = default)
            => Task.Run(() =>
            {
                using var wb = new XLWorkbook(path);
                // Toma la primera hoja que tenga rango usado
                var ws = wb.Worksheets.FirstOrDefault(w => w.RangeUsed() != null);
                if (ws is null) return (IReadOnlyList<string>)new List<string>();

                var used = ws.RangeUsed()!;
                var firstCol = used.RangeAddress.FirstAddress.ColumnNumber;
                var lastCol = used.RangeAddress.LastAddress.ColumnNumber;
                var headerRowNumber = used.RangeAddress.FirstAddress.RowNumber;

                var headers = ws.Row(headerRowNumber)
                                .Cells(firstCol, lastCol)
                                .Select(c => c.GetString().Trim())
                                .Where(s => !string.IsNullOrWhiteSpace(s))
                                .ToList();

                return (IReadOnlyList<string>)headers;
            }, ct);


        public Task<TabularFile> ReadAsync(string path, CancellationToken ct = default)
            => Task.Run(() =>
            {
                using var wb = new XLWorkbook(path);
                var ws = wb.Worksheets.FirstOrDefault(w => w.RangeUsed() != null);
                var result = new TabularFile { FileName = System.IO.Path.GetFileName(path) };
                if (ws is null) return result;

                var used = ws.RangeUsed()!;
                var firstCol = used.RangeAddress.FirstAddress.ColumnNumber;
                var lastCol = used.RangeAddress.LastAddress.ColumnNumber;
                var headerRowNumber = used.RangeAddress.FirstAddress.RowNumber;
                var lastRowNumber = used.RangeAddress.LastAddress.RowNumber;

                // Headers
                var headers = ws.Row(headerRowNumber)
                                .Cells(firstCol, lastCol)
                                .Select(c => c.GetString().Trim())
                                .ToList();
                result.Headers.AddRange(headers);

                // Filas
                for (int r = headerRowNumber + 1; r <= lastRowNumber; r++)
                {
                    var rowDict = new Dictionary<string, string?>(headers.Count);
                    for (int c = firstCol; c <= lastCol; c++)
                    {
                        var header = headers[c - firstCol];
                        rowDict[header] = ws.Cell(r, c).GetString();
                    }
                    result.Rows.Add(rowDict);
                }

                return result;
            }, ct);

        public Task<IReadOnlyList<TabularFile>> ReadAllSheetsAsync(string path, CancellationToken ct = default)
            => Task.Run(() =>
            {
                var list = new List<TabularFile>();
                using var wb = new XLWorkbook(path);
                var fileName = System.IO.Path.GetFileName(path);

                foreach (var ws in wb.Worksheets)
                {
                    var used = ws.RangeUsed();
                    if (used is null) continue;

                    var firstCol = used.RangeAddress.FirstAddress.ColumnNumber;
                    var lastCol = used.RangeAddress.LastAddress.ColumnNumber;
                    var headerRowNumber = used.RangeAddress.FirstAddress.RowNumber;
                    var lastRowNumber = used.RangeAddress.LastAddress.RowNumber;

                    // Headers
                    var headers = ws.Row(headerRowNumber)
                                    .Cells(firstCol, lastCol)
                                    .Select(c => c.GetString().Trim())
                                    .Where(s => !string.IsNullOrWhiteSpace(s))
                                    .ToList();

                    if (headers.Count == 0) continue;

                    var tabularFile = new TabularFile
                    {
                        FileName = fileName,
                        SheetName = ws.Name,
                        Headers = headers
                    };

                    // Filas
                    for (int r = headerRowNumber + 1; r <= lastRowNumber; r++)
                    {
                        var rowDict = new Dictionary<string, string?>(headers.Count);
                        for (int c = firstCol; c <= lastCol; c++)
                        {
                            var header = headers[c - firstCol];
                            rowDict[header] = ws.Cell(r, c).GetString();
                        }
                        tabularFile.Rows.Add(rowDict);
                    }

                    list.Add(tabularFile);
                }

                return (IReadOnlyList<TabularFile>)list;
            }, ct);

        public Task<IReadOnlyList<SheetHeaders>> ReadHeadersPerSheetAsync(string path, CancellationToken ct = default)
            => Task.Run(() =>
            {
                var list = new List<SheetHeaders>();
                using var wb = new XLWorkbook(path);

                foreach (var ws in wb.Worksheets)
                {
                    var used = ws.RangeUsed();
                    if (used is null) continue;

                    var firstCol = used.RangeAddress.FirstAddress.ColumnNumber;
                    var lastCol = used.RangeAddress.LastAddress.ColumnNumber;
                    var headerRowNumber = used.RangeAddress.FirstAddress.RowNumber;

                    var headers = ws.Row(headerRowNumber)
                                    .Cells(firstCol, lastCol)
                                    .Select(c => c.GetString().Trim())
                                    .Where(s => !string.IsNullOrWhiteSpace(s))
                                    .ToList();

                    if (headers.Count == 0) continue;

                    list.Add(new SheetHeaders
                    {
                        FileName = System.IO.Path.GetFileName(path),
                        SheetName = ws.Name,
                        Headers = headers
                    });
                }

                return (IReadOnlyList<SheetHeaders>)list;
            }, ct);

        public Task<IReadOnlyList<SheetHeaders>> ReadHeadersOnlyAsync(string path, CancellationToken ct = default)
            => Task.Run(() =>
            {
                var list = new List<SheetHeaders>();
                using var wb = new XLWorkbook(path);
                var fileName = System.IO.Path.GetFileName(path);

                foreach (var ws in wb.Worksheets)
                {
                    var used = ws.RangeUsed();
                    if (used is null) continue;

                    var firstCol = used.RangeAddress.FirstAddress.ColumnNumber;
                    var lastCol = used.RangeAddress.LastAddress.ColumnNumber;
                    var headerRowNumber = used.RangeAddress.FirstAddress.RowNumber;

                    var headers = ws.Row(headerRowNumber)
                                    .Cells(firstCol, lastCol)
                                    .Select(c => c.GetString().Trim())
                                    .Where(s => !string.IsNullOrWhiteSpace(s))
                                    .ToList();

                    if (headers.Count == 0) continue;

                    list.Add(new SheetHeaders
                    {
                        FileName = fileName,
                        SheetName = ws.Name,
                        Headers = headers
                    });
                }

                return (IReadOnlyList<SheetHeaders>)list;
            }, ct);

        public async IAsyncEnumerable<Dictionary<string, string?>> ReadSheetDataStreamAsync(
            string path, 
            string sheetName, 
            IReadOnlyList<string> headers, 
            int chunkSize = 1000, 
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            using var wb = new XLWorkbook(path);
            var ws = wb.Worksheet(sheetName);
            if (ws is null) yield break;

            var used = ws.RangeUsed();
            if (used is null) yield break;

            var firstCol = used.RangeAddress.FirstAddress.ColumnNumber;
            var lastCol = used.RangeAddress.LastAddress.ColumnNumber;
            var headerRowNumber = used.RangeAddress.FirstAddress.RowNumber;
            var lastRowNumber = used.RangeAddress.LastAddress.RowNumber;

            var currentChunk = new List<Dictionary<string, string?>>();
            
            for (int r = headerRowNumber + 1; r <= lastRowNumber; r++)
            {
                ct.ThrowIfCancellationRequested();
                
                var rowDict = new Dictionary<string, string?>(headers.Count);
                for (int c = firstCol; c <= lastCol; c++)
                {
                    var header = headers[c - firstCol];
                    rowDict[header] = ws.Cell(r, c).GetString();
                }
                currentChunk.Add(rowDict);

                if (currentChunk.Count >= chunkSize)
                {
                    foreach (var item in currentChunk)
                    {
                        yield return item;
                    }
                    currentChunk.Clear();
                }
            }

            // Yield remaining items
            foreach (var item in currentChunk)
            {
                yield return item;
            }
        }
    }
}
