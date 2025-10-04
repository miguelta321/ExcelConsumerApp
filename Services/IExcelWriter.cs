using System.Threading;
using System.Threading.Tasks;
using ExcelConsumerApp.Models;

namespace ExcelConsumerApp.Services
{
    public interface IExcelWriter
    {
        Task WriteAsync(string path, MergedTable table, CancellationToken ct = default);
    }
}
