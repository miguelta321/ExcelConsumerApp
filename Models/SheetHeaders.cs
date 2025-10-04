namespace ExcelConsumerApp.Models
{
    /// <summary>Encabezados de una hoja específica.</summary>
    public sealed class SheetHeaders
    {
        public string FileName { get; init; } = "";  // e.g., "ventas.xlsx"
        public string SheetName { get; init; } = ""; // e.g., "Enero"
        public List<string> Headers { get; init; } = new();
    }
}
