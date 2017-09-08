using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NostreetsORM.Utilities
{
    public static class Extensions
    {
        public static DataTable ToDataTable<T>(this List<T> iList)
        { return ToDataTable(iList, new string[0]); }

        public static DataTable ToDataTable<T>(this List<T> iList, params string[] excludedProps)
        {
            DataTable dataTable = new DataTable();
            //PropertyDescriptorCollection
            List<PropertyDescriptor> propertyDescriptorCollection = TypeDescriptor.GetProperties(typeof(T)).Cast<PropertyDescriptor>()
              .Where((a) => a.Name != excludedProps.SingleOrDefault((b) => b == a.Name)).ToList();

            foreach (PropertyDescriptor item in propertyDescriptorCollection)
            {

                PropertyDescriptor propertyDescriptor = item;
                Type type = propertyDescriptor.PropertyType;

                if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    type = Nullable.GetUnderlyingType(type);

                dataTable.Columns.Add(propertyDescriptor.Name, type);

            }

            //new object[propertyDescriptorCollection.Count];
            int id = 0;
            foreach (T iListItem in iList)
            {
                ArrayList values = new ArrayList();
                for (int i = 0; i < propertyDescriptorCollection.Count; i++)
                {
                    values.Add(propertyDescriptorCollection[i].GetValue(iListItem) == null
                        && propertyDescriptorCollection[i].PropertyType == typeof(string)
                        ? String.Empty
                        : (i == 0)
                        ? id += 1
                        : propertyDescriptorCollection[i].GetValue(iListItem));

                    //values[i] = (propertyDescriptorCollection[i].GetValue(iListItem).GetType() != typeof(string)) ? propertyDescriptorCollection[i].GetValue(iListItem) : DBNull.Value; 
                }
                dataTable.Rows.Add(values.ToArray());

                values = null;
            }
            return dataTable;
        }
    }
}
