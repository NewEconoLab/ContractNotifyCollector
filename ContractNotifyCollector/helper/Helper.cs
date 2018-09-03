using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ContractNotifyCollector.helper
{
    static class Helper
    {
        public static string hexstringReverse(this string hexstr)
        {
            return hexstr.HexString2Bytes().Reverse().ToArray().BytesToHexString();
        }

        public static byte[] HexString2Bytes(this string str)
        {
            byte[] b = new byte[str.Length / 2];
            for (var i = 0; i < b.Length; i++)
            {
                b[i] = byte.Parse(str.Substring(i * 2, 2), System.Globalization.NumberStyles.HexNumber);
            }
            return b;
        }
        public static string BytesToHexString(this byte[] data)
        {
            StringBuilder sb = new StringBuilder();
            foreach (var b in data)
            {
                sb.Append(b.ToString("x02"));
            }
            return sb.ToString();
        }
        
        public static string ToHexString(byte[] data)
        {
            StringBuilder sb = new StringBuilder();
            foreach (var b in data)
            {
                sb.Append(b.ToString("x02"));
            }
            return sb.ToString();
        }

        public static string Hexstring2String(this string hexstr)
        {
            List<byte> byteArray = new List<byte>();

            for (int i = 0; i < hexstr.Length; i = i + 2)
            {
                string s = hexstr.Substring(i, 2);
                byteArray.Add(Convert.ToByte(s, 16));
            }

            string str = Encoding.UTF8.GetString(byteArray.ToArray());

            return str;
        }

        public static string String2Hexstring(this string str)
        {
            byte[] byteArray = Encoding.UTF8.GetBytes(str);
            string byteStr = string.Empty;
            foreach (byte b in byteArray)
            {
                byteStr += Convert.ToString(b, 16);
            }

            return byteStr;
        }
        
    }
}
