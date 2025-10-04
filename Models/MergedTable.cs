using System.Collections.Generic;

namespace ExcelConsumerApp.Models
{
    /// <summary>Resultado del merge de múltiples hojas Excel.</summary>
    public sealed class MergedTable
    {
        public List<string> Headers { get; set; } = new();
        public List<Dictionary<string, string?>> Rows { get; set; } = new();
    }
}
