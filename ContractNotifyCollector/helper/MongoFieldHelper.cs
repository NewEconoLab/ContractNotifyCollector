using Newtonsoft.Json.Linq;
using System.Linq;

namespace ContactNotifyCollector.lib
{
    /// <summary>
    /// mongodb字段拼接帮助类
    /// 
    /// </summary>
    public class MongoFieldHelper
    {
        public static JObject toFilter(long[] blockindexArr, string field, string logicalOperator = "$or")
        {
            if (blockindexArr.Count() == 1)
            {
                return new JObject() { { field, blockindexArr[0] } };
            }
            return new JObject() { { logicalOperator, new JArray() { blockindexArr.Select(item => new JObject() { { field, item } }).ToArray() } } };
        }
        public static JObject toFilter(string[] blockindexArr, string field, string logicalOperator = "$or")
        {
            if (blockindexArr.Count() == 1)
            {
                return new JObject() { { field, blockindexArr[0] } };
            }
            return new JObject() { { logicalOperator, new JArray() { blockindexArr.Select(item => new JObject() { { field, item } }).ToArray() } } };
        }
        public static JObject toReturn(string[] fieldArr)
        {
            JObject obj = new JObject();
            foreach (var field in fieldArr)
            {
                obj.Add(field, 1);
            }
            return obj;
        }
        public static JObject toSort(string[] fieldArr, bool order = false)
        {
            int flag = order ? 1 : -1;
            JObject obj = new JObject();
            foreach (var field in fieldArr)
            {
                obj.Add(field, flag);
            }
            return obj;
        }
    }
}
