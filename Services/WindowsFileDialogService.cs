using Microsoft.Win32;

namespace ExcelConsumerApp.Services
{
    public sealed class WindowsFileDialogService : IFileDialogService
    {
        public IEnumerable<string> ShowOpenFiles(params string[] filters)
        {
            var dlg = new OpenFileDialog
            {
                Title = "Selecciona archivos Excel",
                Filter = filters?.Length > 0 ? string.Join("|", filters) : "Excel (*.xlsx)|*.xlsx|Todos (*.*)|*.*",
                Multiselect = true,
                CheckFileExists = true
            };

            return dlg.ShowDialog() == true ? dlg.FileNames : [];
        }

        public string? ShowSaveFile(string filter, string defaultFileName = "")
        {
            var dlg = new SaveFileDialog
            {
                Title = "Guardar archivo Excel combinado",
                Filter = filter,
                FileName = defaultFileName,
                DefaultExt = ".xlsx"
            };

            return dlg.ShowDialog() == true ? dlg.FileName : null;
        }
    }
}
