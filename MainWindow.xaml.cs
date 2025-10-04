// File: MainWindow.xaml.cs
using System.Windows;
using ExcelConsumerApp.Services;
using ExcelConsumerApp.ViewModels;

namespace ExcelConsumerApp
{
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();

            DataContext = new MainViewModel(
                new WindowsFileDialogService(),
                new ClosedXmlExcelReader(),
                new HeaderNormalizer(),
                new MergeService(),
                new ClosedXmlExcelWriter()
            );
        }
    }
}
