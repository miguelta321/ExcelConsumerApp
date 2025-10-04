using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using ExcelConsumerApp.Models;
using ExcelDataReader;

namespace ExcelConsumerApp.Services
{
    public sealed class ExcelDataReaderExcelReader : IExcelReader
    {
        public Task<IReadOnlyList<string>> ReadHeadersAsync(string path, CancellationToken ct = default)
            => Task.Run(() =>
            {
                using var stream = OpenRead(path);
                using var reader = ExcelReaderFactory.CreateReader(stream);
                do
                {
                    var headers = ReadCurrentSheetHeaders(reader, ct);
                    if (headers != null && headers.Count > 0)
                    {
                        return (IReadOnlyList<string>)headers;
                    }
                }
                while (reader.NextResult());

                return (IReadOnlyList<string>)Array.Empty<string>();
            }, ct);

        public Task<TabularFile> ReadAsync(string path, CancellationToken ct = default)
            => Task.Run(() =>
            {
                using var stream = OpenRead(path);
                using var reader = ExcelReaderFactory.CreateReader(stream);
                var fileName = Path.GetFileName(path);

                do
                {
                    var sheet = ReadCurrentSheet(reader, fileName, ct);
                    if (sheet != null)
                    {
                        return sheet;
                    }
                }
                while (reader.NextResult());

                return new TabularFile { FileName = fileName };
            }, ct);

        public Task<IReadOnlyList<TabularFile>> ReadAllSheetsAsync(string path, CancellationToken ct = default)
            => Task.Run(() =>
            {
                using var stream = OpenRead(path);
                using var reader = ExcelReaderFactory.CreateReader(stream);
                var fileName = Path.GetFileName(path);
                var result = new List<TabularFile>();

                do
                {
                    var sheet = ReadCurrentSheet(reader, fileName, ct);
                    if (sheet != null)
                    {
                        result.Add(sheet);
                    }
                }
                while (reader.NextResult());

                return (IReadOnlyList<TabularFile>)result;
            }, ct);

        public Task<IReadOnlyList<SheetHeaders>> ReadHeadersPerSheetAsync(string path, CancellationToken ct = default)
            => Task.Run(() =>
            {
                using var stream = OpenRead(path);
                using var reader = ExcelReaderFactory.CreateReader(stream);
                var fileName = Path.GetFileName(path);
                var result = new List<SheetHeaders>();

                do
                {
                    var headers = ReadCurrentSheetHeaders(reader, ct);
                    if (headers != null && headers.Count > 0)
                    {
                        result.Add(new SheetHeaders
                        {
                            FileName = fileName,
                            SheetName = reader.Name ?? string.Empty,
                            Headers = headers
                        });
                    }
                }
                while (reader.NextResult());

                return (IReadOnlyList<SheetHeaders>)result;
            }, ct);

        public Task<IReadOnlyList<SheetHeaders>> ReadHeadersOnlyAsync(string path, CancellationToken ct = default)
            => ReadHeadersPerSheetAsync(path, ct);

        public async IAsyncEnumerable<Dictionary<string, string?>> ReadSheetDataStreamAsync(
            string path,
            string sheetName,
            IReadOnlyList<string> headers,
            int chunkSize = 1000,
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            _ = chunkSize;
            await Task.Yield();

            FileStream? stream = null;
            IExcelDataReader? reader = null;

            try
            {
                stream = OpenRead(path);
                reader = ExcelReaderFactory.CreateReader(stream);

                do
                {
                    if (!string.Equals(reader.Name, sheetName, StringComparison.OrdinalIgnoreCase))
                    {
                        while (reader.Read())
                        {
                            ct.ThrowIfCancellationRequested();
                        }
                        continue;
                    }

                    var headerSkipped = false;

                    while (reader.Read())
                    {
                        ct.ThrowIfCancellationRequested();

                        if (!headerSkipped)
                        {
                            if (RowIsEmpty(reader))
                            {
                                continue;
                            }

                            headerSkipped = true;
                            continue;
                        }

                        if (RowIsEmpty(reader))
                        {
                            continue;
                        }

                        var row = new Dictionary<string, string?>(headers.Count);
                        var maxColumns = Math.Min(headers.Count, reader.FieldCount);

                        for (int i = 0; i < maxColumns; i++)
                        {
                            var header = headers[i];
                            row[header] = reader.GetValue(i)?.ToString();
                        }

                        yield return row;
                    }

                    break;
                }
                while (reader.NextResult());
            }
            finally
            {
                reader?.Dispose();
                stream?.Dispose();
            }
        }

        private static FileStream OpenRead(string path)
            => new(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);

        private static TabularFile? ReadCurrentSheet(IExcelDataReader reader, string fileName, CancellationToken ct)
        {
            var headers = ReadCurrentSheetHeaders(reader, ct);
            if (headers == null || headers.Count == 0)
            {
                SkipRemainingRows(reader, ct);
                return null;
            }

            var rows = new List<Dictionary<string, string?>>();

            while (reader.Read())
            {
                ct.ThrowIfCancellationRequested();

                if (RowIsEmpty(reader))
                {
                    continue;
                }

                rows.Add(ReadRow(reader, headers));
            }

            return new TabularFile
            {
                FileName = fileName,
                SheetName = reader.Name ?? string.Empty,
                Headers = headers,
                Rows = rows
            };
        }

        private static List<string>? ReadCurrentSheetHeaders(IExcelDataReader reader, CancellationToken ct)
        {
            while (reader.Read())
            {
                ct.ThrowIfCancellationRequested();

                if (RowIsEmpty(reader))
                {
                    continue;
                }

                var headers = new List<string>(reader.FieldCount);

                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var value = reader.GetValue(i)?.ToString()?.Trim();
                    headers.Add(value ?? string.Empty);
                }

                TrimTrailingEmpty(headers);
                return headers;
            }

            return null;
        }

        private static void SkipRemainingRows(IExcelDataReader reader, CancellationToken ct)
        {
            while (reader.Read())
            {
                ct.ThrowIfCancellationRequested();
            }
        }

        private static Dictionary<string, string?> ReadRow(IExcelDataReader reader, IReadOnlyList<string> headers)
        {
            var row = new Dictionary<string, string?>(headers.Count);
            var maxColumns = Math.Min(headers.Count, reader.FieldCount);

            for (int i = 0; i < maxColumns; i++)
            {
                var header = headers[i];
                row[header] = reader.GetValue(i)?.ToString();
            }

            return row;
        }

        private static bool RowIsEmpty(IExcelDataReader reader)
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var value = reader.GetValue(i);
                if (value is string str)
                {
                    if (!string.IsNullOrWhiteSpace(str))
                    {
                        return false;
                    }
                }
                else if (value != null)
                {
                    return false;
                }
            }

            return true;
        }

        private static void TrimTrailingEmpty(List<string> headers)
        {
            for (int i = headers.Count - 1; i >= 0; i--)
            {
                if (!string.IsNullOrWhiteSpace(headers[i]))
                {
                    break;
                }

                headers.RemoveAt(i);
            }
        }
    }
}
