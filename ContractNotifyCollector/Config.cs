using Newtonsoft.Json.Linq;
using System.IO;
using System.Linq;

namespace ContractNotifyCollector
{
    /// <summary>
    /// 配置主类
    /// 
    /// </summary>
    class Config
    {
        // 主配置文件
        private static JObject config;
        public static JObject getConfig()
        {
            return config;
        }
        public static void loadConfig(string filename)
        {
            if(config == null)
            {
                config = JObject.Parse(File.ReadAllText(filename));
                initDb();
            }
        }

        // DB连接信息
        public static DbConnInfo remoteDbConnInfo;
        public static DbConnInfo localDbConnInfo;
        public static DbConnInfo blockDbConnInfo;
        private static void initDb()
        {
            string startNetType = config["startNetType"].ToString();
            var connInfo = config["connList"].Children().Where(p => p["netType"].ToString() == startNetType).First();
            remoteDbConnInfo = getDbConnInfo(connInfo, 1);
            localDbConnInfo = getDbConnInfo(connInfo, 2);
            blockDbConnInfo = getDbConnInfo(connInfo, 3);
        }
        private static DbConnInfo getDbConnInfo(JToken conn, int flag)
        {
            if (flag == 1)
            {
                return new DbConnInfo
                {
                    connStr = conn["remoteConnStr"].ToString(),
                    connDB = conn["remoteDatabase"].ToString()
                };
            }
            else if(flag == 2)
            {
                return new DbConnInfo
                {
                    connStr = conn["localConnStr"].ToString(),
                    connDB = conn["localDatabase"].ToString()
                };
            }
            else if(flag == 3)
            {
                return new DbConnInfo
                {
                    connStr = conn["blockConnStr"].ToString(),
                    connDB = conn["blockDatabase"].ToString()
                };
            }
            return null;
        }
    }
    class DbConnInfo
    {
        public string connStr { set; get; }
        public string connDB { set; get; }
    }
}
