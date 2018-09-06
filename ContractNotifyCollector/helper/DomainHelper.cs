using System;
using System.Text;
using System.Linq;
using System.Security.Cryptography;

namespace ContractNotifyCollector.helper
{
    /// <summary>
    /// 域名哈希类
    /// 
    /// </summary>
    public class DomainHelper
    {

        public static Hash256 nameHash(string domain)
        {
            SHA256 sha256 = SHA256.Create();
            byte[] data = Encoding.UTF8.GetBytes(domain);
            return new Hash256(sha256.ComputeHash(data));
        }
        public static Hash256 nameHashSub(byte[] roothash, string subdomain)
        {
            var bs = Encoding.UTF8.GetBytes(subdomain);
            if (bs.Length == 0)
                return roothash;
            SHA256 sha256 = SHA256.Create();
            var domain = sha256.ComputeHash(bs).Concat(roothash).ToArray();
            return new Hash256(sha256.ComputeHash(domain));
        }
        public static Hash256 nameHashFull(string domain, string parent)
        {
            var ps = nameHash(parent);
            return nameHashSub(ps.data, domain);
        }
    }
    public class Hash256 : IComparable<Hash256>
    {
        public Hash256(byte[] data)
        {
            if (data.Length != 32)
                throw new Exception("error length.");
            this.data = data;
        }
        public Hash256(string hexstr)
        {
            var bts = Helper.HexString2Bytes(hexstr);
            if (bts.Length != 32)
                throw new Exception("error length.");
            this.data = bts.Reverse().ToArray();
        }
        public override string ToString()
        {
            return "0x" + Helper.ToHexString(this.data.Reverse().ToArray());
        }
        public byte[] data;

        public int CompareTo(Hash256 other)
        {
            byte[] x = data;
            byte[] y = other.data;
            for (int i = x.Length - 1; i >= 0; i--)
            {
                if (x[i] > y[i])
                    return 1;
                if (x[i] < y[i])
                    return -1;
            }
            return 0;
        }
        public override bool Equals(object obj)
        {
            return CompareTo(obj as Hash256) == 0;
        }

        public static implicit operator byte[] (Hash256 value)
        {
            return value.data;
        }
        public static implicit operator Hash256(byte[] value)
        {
            return new Hash256(value);
        }
    }

}

