using contractNotifyExtractor.Helper;
using Newtonsoft.Json.Linq;

namespace ContractNotifyCollector.helper
{
    public class escapeHelper
    {
        public static string contractDataEscap(string type, string value, JObject taskEscapeInfo)
        {
            string result = value;

            string escap = (string)taskEscapeInfo["escape"];
            int decimals = 0;
            switch (escap)
            {
                case "String"://处理成字符串
                    if (isZeroEmpty(type, value))
                    { result = ""; }
                    else
                    {
                        try
                        {
                            result = value.Hexstring2String();
                        }
                        catch { }
                    }

                    break;
                case "Address"://处理成地址
                    if (isZeroEmpty(type, value))
                    { result = ""; }
                    else
                    {
                        try
                        {
                            result = ThinNeo.Helper.GetAddressFromScriptHash(value.HexString2Bytes());
                        }
                        catch { }
                    }

                    break;
                case "BigInteger"://处理成大整数                  
                    if (taskEscapeInfo["decimals"] != null) decimals = int.Parse(taskEscapeInfo["decimals"].ToString());

                    if (isZeroEmpty(type, value))
                    {
                        result = "0";
                    }
                    else
                    {
                        try
                        {
                            if (type == "ByteArray")
                            {
                                result = value.getNumStrFromHexStr(decimals);
                            }
                            else//Integer
                            {
                                result = value.getNumStrFromIntStr(decimals);
                            }
                        }
                        catch { }
                    }

                    break;
                default://未定义，默认是hexString，合约中是byte[]，尝试逆转操作
                    if (isZeroEmpty(type, value))
                    {
                        result = "";
                    }
                    else
                    {
                        try
                        {
                            result = "0x" + value.hexstringReverse();
                        }
                        catch { }
                    }

                    break;
            }

            return result;
        }

        //为0.为空，为false再NEOvm处理中，有时通知输出类型会变成各种奇怪东西，此处统一判断
        public static bool isZeroEmpty(string type, string value)
        {
            if (type == "Boolean" && value == "False") return true;
            if (type == "ByteArray" && value == "") return true;

            return false;
        }

        
    }
}
