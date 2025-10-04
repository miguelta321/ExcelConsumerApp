using System;
using System.Collections;
using System.Globalization;
using System.Linq;
using System.Windows.Data;

namespace ExcelConsumerApp.Converters
{
    public class ListToStringConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is IEnumerable enumerable)
            {
                return string.Join(", ", enumerable.Cast<object>());
            }
            return string.Empty;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}
