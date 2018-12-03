using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace ContractNotifyCollector.helper
{
    class NumberDecimalHelper
    {
        public static string formatDecimal(string numberDecimalStr)
        {
            string value = numberDecimalStr;
            if (numberDecimalStr.Contains("$numberDecimal"))
            {
                value = Convert.ToString(JObject.Parse(numberDecimalStr)["$numberDecimal"]);
            }
            if (value.Contains("E"))
            {
                value = decimal.Parse(value, NumberStyles.Float).ToString();
            }
            return value;
        }
        public static decimal formatDecimalDouble(string numberDecimalStr)
        {
            return decimal.Parse(formatDecimal(numberDecimalStr), NumberStyles.Float);
        }
    }
}
