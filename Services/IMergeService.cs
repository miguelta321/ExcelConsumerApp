using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ExcelConsumerApp.Models;

namespace ExcelConsumerApp.Services
{
    public interface IMergeService
    {
        /// <summary>
        /// Hace full-outer join por 'keyColumnNormalized' considerando TODAS las hojas de TODOS los archivos.
        /// Prefija columnas como "file.xlsx:sheet:headerOriginal".
        /// </summary>
        Task<MergedTable> MergeAsync(
            IEnumerable<string> filePaths,
            string keyColumnNormalized,
            IExcelReader reader,
            IHeaderNormalizer normalizer,
            CancellationToken ct = default);

        /// <summary>
        /// Hace full-outer join por 'keyColumnNormalized' considerando solo las hojas seleccionadas.
        /// Prefija columnas como "file.xlsx:sheet:headerOriginal".
        /// </summary>
        Task<MergedTable> MergeAsync(
            IEnumerable<FileSheetSelection> fileSheetSelections,
            string keyColumnNormalized,
            IExcelReader reader,
            IHeaderNormalizer normalizer,
            CancellationToken ct = default);
    }
}
