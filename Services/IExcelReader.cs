using ExcelConsumerApp.Models;

namespace ExcelConsumerApp.Services
{
    public interface IExcelReader
    {
        /// <summary>Lee solo los encabezados de la primera hoja no vacía.</summary>
        Task<IReadOnlyList<string>> ReadHeadersAsync(string path, CancellationToken ct = default);

        /// <summary>Lee toda la hoja como tabla: encabezados + filas (strings).</summary>
        Task<TabularFile> ReadAsync(string path, CancellationToken ct = default);

        /// <summary>Lee todas las hojas del archivo como tablas.</summary>
        Task<IReadOnlyList<TabularFile>> ReadAllSheetsAsync(string path, CancellationToken ct = default);

        /// <summary>
        /// Devuelve los encabezados de todas las hojas no vacías del archivo.
        /// Cada entrada identifica (archivo, hoja) y su lista de headers.
        /// </summary>
        Task<IReadOnlyList<SheetHeaders>> ReadHeadersPerSheetAsync(string path, CancellationToken ct = default);

        /// <summary>
        /// Lee solo los encabezados de todas las hojas (streaming, sin cargar datos).
        /// Optimizado para archivos grandes.
        /// </summary>
        Task<IReadOnlyList<SheetHeaders>> ReadHeadersOnlyAsync(string path, CancellationToken ct = default);

        /// <summary>
        /// Lee datos de una hoja específica por chunks para archivos grandes.
        /// </summary>
        IAsyncEnumerable<Dictionary<string, string?>> ReadSheetDataStreamAsync(
            string path, 
            string sheetName, 
            IReadOnlyList<string> headers, 
            int chunkSize = 1000, 
            CancellationToken ct = default);
    }
}
