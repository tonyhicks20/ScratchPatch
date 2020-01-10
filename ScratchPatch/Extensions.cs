using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace ScratchPatch.Extensions
{
    public static class Extensions
    {
        #region Public Methods

        public static bool Between(this int itm, int min, int max) => itm >= min && itm <= max;

        public static bool Between(this decimal itm, decimal min, decimal max) => itm >= min && itm <= max;

        public static bool Between(this DateTime itm, DateTime min, DateTime max) => itm >= min && itm <= max;

        public static void ForEach<T>(this IEnumerable<T> collection, Action<T> action)
        {
            if (collection.IsNullOrEmpty())
                return;

            foreach (T item in collection)
            {
                action(item);
            }
        }

        public static void ForEach<T>(this IEnumerable<T> collection, Action<T, int> action)
        {
            if (collection.IsNullOrEmpty())
                return;

            int i = 0;
            foreach (T item in collection)
            {
                action(item, i);
                i++;
            }
        }

        /// <summary>
        /// This is not thread safe. Do not confuse it with the ConcurrentDictionary's GetOrAdd method!
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="dictionary"></param>
        /// <param name="valueGetter"></param>
        /// <returns></returns>
        public static TValue GetOrAdd<TKey, TValue>(this Dictionary<TKey, TValue> dictionary, TKey key, Func<TKey, TValue> valueGetter)
        {
            if (dictionary.TryGetValue(key, out TValue value))
                return value;
            value = valueGetter(key);
            dictionary[key] = value;
            return value;
        }

        public static bool In<T>(this T itemToCheck, params T[] items) => items != null && items.Any(itm => itm.Equals(itemToCheck));

        public static bool In<T>(this T itemToCheck, IEnumerable<T> items) => items != null && items.Any(itm => itm.Equals(itemToCheck));

        public static bool In(this string itemToCheck, StringComparison comparison, params string[] items) => items != null && items.Any(itm => string.Equals(itemToCheck, itm, comparison));

        public static bool IsNullOrEmpty<T>(this IEnumerable<T> collection) => collection == null || !collection.Any();

        public static bool IsNullOrEmpty(this string self) => string.IsNullOrEmpty(self);

        public static bool IsNullOrWhiteSpace(this string self) => string.IsNullOrWhiteSpace(self);

        public static KeyValuePair<string, object> PairWith(this string key, object value) => new KeyValuePair<string, object>(key, value);

        public static DataTable ToDataTable<T>(
               this IEnumerable<T> items,
               IEnumerable<string> propertyNamesToUse = null,
               string autoIncrementProperty = null) where T : class
        {
            var tb = new DataTable(typeof(T).Name);

            IEnumerable<string> validPropNames = propertyNamesToUse ?? typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(prop => prop.CanRead).Select(prop => prop.Name);
            PropertyInfo[] props = validPropNames.Select(name => typeof(T).GetProperty(name)).ToArray();

            foreach (PropertyInfo prop in props)
            {
                Type t = getCoreType(prop.PropertyType);
                tb.Columns.Add(prop.Name, t);
            }

            if (!string.IsNullOrWhiteSpace(autoIncrementProperty))
            {
                DataColumn autoInc = new DataColumn();
                autoInc.DataType = typeof(int);
                autoInc.AutoIncrement = true;
                autoInc.AutoIncrementSeed = 1;
                autoInc.AutoIncrementStep = 1;
                autoInc.ColumnName = autoIncrementProperty;
                tb.Columns.Add(autoInc);
            }

            foreach (T item in items)
            {
                var values = new object[props.Length];

                for (int i = 0; i < props.Length; i++)
                {
                    if (props[i].PropertyType == typeof(string))
                    {
                        string stringVal = (string)props[i].GetValue(item, null);
                        values[i] = stringVal ?? string.Empty;
                    }
                    else
                        values[i] = props[i].GetValue(item, null);
                }

                tb.Rows.Add(values);
            }
            return tb;
        }

        #endregion Public Methods

        #region Private Methods

        private static Type getCoreType(Type t)
        {
            bool isNullable(Type type) => !type.IsValueType || (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>));

            if (t == null)
                return null;

            if (isNullable(t))
                return t.IsValueType ? Nullable.GetUnderlyingType(t) : t;

            if (t.IsEnum)
                return Enum.GetUnderlyingType(t);

            return t;
        }

        #endregion Private Methods
    }
}