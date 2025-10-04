// File: Commands/AsyncRelayCommand.cs
using System;
using System.Threading.Tasks;
using System.Windows.Input;
using System.Windows; // Dispatcher

namespace ExcelConsumerApp.Commands
{
    /// <summary>
    /// ICommand para acciones asíncronas con control de reentrada, seguro para UI thread.
    /// </summary>
    public sealed class AsyncRelayCommand : ICommand
    {
        private readonly Func<Task> _executeAsync;
        private readonly Func<bool>? _canExecute;
        private bool _isRunning;

        public AsyncRelayCommand(Func<Task> executeAsync, Func<bool>? canExecute = null)
        {
            _executeAsync = executeAsync ?? throw new ArgumentNullException(nameof(executeAsync));
            _canExecute = canExecute;
        }

        public bool CanExecute(object? parameter) => !_isRunning && (_canExecute?.Invoke() ?? true);

        public async void Execute(object? parameter)
        {
            if (!CanExecute(parameter)) return;

            try
            {
                _isRunning = true;
                RaiseCanExecuteChangedOnUI();

                // IMPORTANTE: sin ConfigureAwait(false) para volver al hilo de UI
                await _executeAsync();
            }
            finally
            {
                _isRunning = false;
                RaiseCanExecuteChangedOnUI();
            }
        }

        public event EventHandler? CanExecuteChanged;

        public void RaiseCanExecuteChanged() => RaiseCanExecuteChangedOnUI();

        private static void InvokeOnUI(Action action)
        {
            var dispatcher = Application.Current?.Dispatcher;
            if (dispatcher == null) { action(); return; }
            if (dispatcher.CheckAccess()) action();
            else dispatcher.Invoke(action);
        }

        private void RaiseCanExecuteChangedOnUI()
            => InvokeOnUI(() => CanExecuteChanged?.Invoke(this, EventArgs.Empty));
    }
}
