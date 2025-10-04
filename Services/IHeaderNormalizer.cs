namespace ExcelConsumerApp.Services
{
    public interface IHeaderNormalizer
    {
        /// <summary>Normaliza un header para comparaciones: trim, colapsa espacios, lower, sin acentos.</summary>
        string Normalize(string header);
    }
}
