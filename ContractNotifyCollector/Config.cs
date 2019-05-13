using ContractNotifyCollector.helper;
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
            LogHelper.initLogger("log4net.config");
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
        public static DbConnInfo notifyDbConnInfo;
        public static string nelApiUrl;
        public static bool fromApiFlag;
        private static void initDb()
        {
            string startNetType = config["startNetType"].ToString();
            var connInfo = config["DBConnInfoList"].Children().Where(p => p["netType"].ToString() == startNetType).First();
            remoteDbConnInfo = getDbConnInfo(connInfo, 1);
            localDbConnInfo = getDbConnInfo(connInfo, 2);
            blockDbConnInfo = getDbConnInfo(connInfo, 3);
            notifyDbConnInfo = getDbConnInfo(connInfo, 4);
            nelApiUrl = connInfo["NELApiUrl"].ToString();
            string flag = connInfo["fromApiFlag"].ToString();
            fromApiFlag = (flag == "1" || flag.ToLower() == "true");
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
            else if (flag == 4)
            {
                return new DbConnInfo
                {
                    connStr = conn["notifyConnStr"].ToString(),
                    connDB = conn["notifyDatabase"].ToString()
                };
            }
            return null;
        }

        public string getNetType()
        {
            return config["startNetType"].ToString();
        }
    }
    class DbConnInfo
    {
        public string connStr { set; get; }
        public string connDB { set; get; }
    }
}
