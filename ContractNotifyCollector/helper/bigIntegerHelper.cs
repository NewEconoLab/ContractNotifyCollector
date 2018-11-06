using System;
using System.Linq;
using System.Numerics;
using System.Text;

namespace contractNotifyExtractor.Helper
{
    public static class bigIntegerHelper
    {
        //十六进制转数值（考虑精度调整）
        public static string getNumStrFromHexStr(this string hexStr, int decimals)
        {
            bool flag = true;
            if(flag)
            {
                BigInteger b = new BigInteger(ThinNeo.Helper.HexString2Bytes(hexStr));
                string bStr = b.ToString();
                if(bStr.StartsWith("-"))
                {
                    return "-" + changeDecimals(BigInteger.Multiply(-1, b), decimals);
                }
                return changeDecimals(b, decimals);

            }
            //小头换大头
            byte[] bytes = ThinNeo.Helper.HexString2Bytes(hexStr).Reverse().ToArray();
            string hex = ThinNeo.Helper.Bytes2HexString(bytes);
            //大整数处理，默认第一位为符号位，0代表正数，需要补位
            hex = "0" + hex;

            BigInteger bi = BigInteger.Parse(hex, System.Globalization.NumberStyles.AllowHexSpecifier);

            return changeDecimals(bi, decimals);
        }

        //大整数文本转数值（考虑精度调整）
        public static string getNumStrFromIntStr(this string intStr, int decimals)
        {
            BigInteger bi = BigInteger.Parse(intStr);

            return changeDecimals(bi, decimals);
        }

        //根据精度处理小数点（大整数模式处理）
        private static string changeDecimals(BigInteger value, int decimals)
        {
            BigInteger bi = BigInteger.DivRem(value, BigInteger.Pow(10, decimals), out BigInteger remainder);
            string numStr = bi.ToString();
            if (remainder != 0)//如果余数不为零才添加小数点
            {
                //按照精度，处理小数部分左侧补零与右侧去零
                int AddLeftZeoCount = decimals - remainder.ToString().Length;
                string remainderStr = cloneStr("0", AddLeftZeoCount) + removeRightZero(remainder);

                numStr = string.Format("{0}.{1}", bi, remainderStr);
            }

            return numStr;
        }

        //生成左侧补零字符串
        private static string cloneStr(string str, int cloneCount)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= cloneCount; i++)
            {
                sb.Append(str);
            }
            return sb.ToString();
        }

        //去除大整数小数（余数）部分的右侧0
        private static BigInteger removeRightZero(BigInteger bi)
        {
            string strReverse0 = strReverse(bi.ToString());
            BigInteger bi0 = BigInteger.Parse(strReverse0);
            string strReverse1 = strReverse(bi0.ToString());

            return BigInteger.Parse(strReverse1);
        }

        //反转字符串
        private static string strReverse(string str)
        {
            char[] arr = str.ToCharArray();
            Array.Reverse(arr);

            return new string(arr);
        }
    }
}
