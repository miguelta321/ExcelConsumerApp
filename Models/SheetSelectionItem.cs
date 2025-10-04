using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace ExcelConsumerApp.Models
{
    public class SheetSelectionItem : INotifyPropertyChanged
    {
        private bool _isSelected = true;
        private FileSheetSelection? _parent;

        public string SheetName { get; init; } = "";
        
        public bool IsSelected 
        { 
            get => _isSelected; 
            set 
            { 
                _isSelected = value; 
                OnPropertyChanged();
                _parent?.OnPropertyChanged(nameof(FileSheetSelection.SelectedSheets));
            } 
        }

        public FileSheetSelection? Parent 
        { 
            get => _parent; 
            set => _parent = value; 
        }

        public event PropertyChangedEventHandler? PropertyChanged;

        protected virtual void OnPropertyChanged([CallerMemberName] string? propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }
}
