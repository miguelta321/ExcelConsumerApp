using System;
using System.Threading;
using System.Threading.Tasks;
using ClosedXML.Excel;
using ExcelConsumerApp.Models;

namespace ExcelConsumerApp.Services
{
    public sealed class ClosedXmlExcelWriter : IExcelWriter
    {
        public async Task WriteAsync(string path, MergedTable table, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("La ruta no puede estar vacía.", nameof(path));

            if (table == null)
                throw new ArgumentNullException(nameof(table));

            await Task.Run(() =>
            {
                using var workbook = new XLWorkbook();
                var worksheet = workbook.Worksheets.Add("Merged");

                // Escribir headers (fila 1)
                for (int col = 0; col < table.Headers.Count; col++)
                {
                    worksheet.Cell(1, col + 1).Value = table.Headers[col];
                }

                // Escribir filas
                for (int row = 0; row < table.Rows.Count; row++)
                {
                    var dataRow = table.Rows[row];
                    for (int col = 0; col < table.Headers.Count; col++)
                    {
                        var header = table.Headers[col];
                        var value = dataRow.TryGetValue(header, out var cellValue) ? cellValue : null;
                        worksheet.Cell(row + 2, col + 1).Value = value ?? "";
                    }
                }

                // Ajustar ancho de columnas automáticamente
                worksheet.ColumnsUsed().AdjustToContents();

                workbook.SaveAs(path);
            }, ct);
        }
    }
}
