using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace ExcelConsumerApp.Utils
{
    /// <summary>
    /// Base para ViewModels: notifica cambios de propiedades.
    /// </summary>
    public abstract class ViewModelBase : INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler? PropertyChanged;

        protected void RaisePropertyChanged([CallerMemberName] string? propertyName = null)
            => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));

        /// <summary>
        /// Asigna y notifica cambio solo si el valor es distinto.
        /// </summary>
        protected bool SetProperty<T>(ref T backingField, T value, [CallerMemberName] string? propertyName = null)
        {
            if (Equals(backingField, value)) return false;
            backingField = value;
            RaisePropertyChanged(propertyName);
            return true;
        }
    }
}
