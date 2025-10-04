using System.Collections.Generic;

namespace ExcelConsumerApp.Models
{
    /// <summary>Representa una hoja tabular sin esquema fijo.</summary>
    public sealed class TabularFile
    {
        public string FileName { get; init; } = "";
        public string SheetName { get; init; } = "";
        public List<string> Headers { get; init; } = new();
        public List<Dictionary<string, string?>> Rows { get; init; } = new();
    }
}
