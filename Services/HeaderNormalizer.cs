using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace ExcelConsumerApp.Services
{
    public sealed class HeaderNormalizer : IHeaderNormalizer
    {
        private static readonly Regex MultiSpace = new(@"\s+", RegexOptions.Compiled);

        public string Normalize(string header)
        {
            if (string.IsNullOrWhiteSpace(header)) return string.Empty;

            var t = header.Trim();
            t = MultiSpace.Replace(t, " ");               // " Código   Cliente " -> "Código Cliente"
            t = t.ToLowerInvariant();                     // case-insensitive
            t = RemoveDiacritics(t);                      // "código" -> "codigo"
            return t;
        }

        private static string RemoveDiacritics(string text)
        {
            var norm = text.Normalize(NormalizationForm.FormD);
            var sb = new StringBuilder(capacity: norm.Length);
            foreach (var c in norm)
            {
                var uc = CharUnicodeInfo.GetUnicodeCategory(c);
                if (uc != UnicodeCategory.NonSpacingMark)
                    sb.Append(c);
            }
            return sb.ToString().Normalize(NormalizationForm.FormC);
        }
    }
}
