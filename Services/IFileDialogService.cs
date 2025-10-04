namespace ExcelConsumerApp.Services
{
    public interface IFileDialogService
    {
        /// <summary>Devuelve rutas seleccionadas o colección vacía si se cancela.</summary>
        IEnumerable<string> ShowOpenFiles(params string[] filters);
        
        /// <summary>Devuelve la ruta donde guardar el archivo o null si se cancela.</summary>
        string? ShowSaveFile(string filter, string defaultFileName = "");
    }
}
