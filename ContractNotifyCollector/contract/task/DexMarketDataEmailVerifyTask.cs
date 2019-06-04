using ContractNotifyCollector.core;
using ContractNotifyCollector.helper;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;
using System.Text;
using System.Threading;

namespace ContractNotifyCollector.contract.task
{
    class DexMarketDataEmailVerifyTask : ContractTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private string dexEmailStateCol;
        private int batchSize;
        private int batchInterval;

        private DbConnInfo localConn;
        private bool initSuccFlag = false;
        private bool hasCreateIndex = false;

        public DexMarketDataEmailVerifyTask(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];
            dexEmailStateCol = cfg["dexEmailStateCol"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());
            //
            initEmailConfig(cfg["email"]);

            localConn = Config.localDbConnInfo;
            initSuccFlag = true;
        }

        public override void startTask()
        {
            if (!initSuccFlag) return;
            while (true)
            {
                try
                {
                    ping();
                    process();
                }
                catch (Exception ex)
                {
                    LogHelper.printEx(ex);
                    Thread.Sleep(10 * 1000);
                    // continue
                }
            }
        }
        
        private void process()
        {
            string findStr = new JObject { { "verifyState", VerifyState.WaitSend } }.ToString();
            string fieldStr = new JObject { { "address", 1}, { "email", 1 }, { "_id",0} }.ToString();
            string sortStr = new JObject { { "time", 1} }.ToString();
            var queryRes = mh.GetDataPagesWithField(localConn.connStr, localConn.connDB, dexEmailStateCol, fieldStr, batchSize, 1, sortStr, findStr);
            if (queryRes == null || queryRes.Count == 0) return;

            string updateStr = null;
            foreach(var item in queryRes)
            {
                // 生成验证码
                string verifyUid = generateUid();
                string email = item["email"].ToString();
                string address = item["address"].ToString();
                string verifyInfo = formatData(address, email, verifyUid);
                // 发送
                if (sendVerifyInfo(verifyInfo, email))
                {
                    // 更新
                    updateStr = new JObject { { "$set", new JObject { { "verifyState", VerifyState.HaveSend }, { "verifyUid", verifyUid } } } }.ToString();
                    mh.UpdateData(localConn.connStr, localConn.connDB, dexEmailStateCol, updateStr, findStr);
                }
            }
            if (hasCreateIndex) return;
            mh.setIndex(localConn.connStr, localConn.connDB, dexEmailStateCol, "{'address':1}", "i_address");
            mh.setIndex(localConn.connStr, localConn.connDB, dexEmailStateCol, "{'verifyState':1}", "i_verifyState");
            hasCreateIndex = true;
        }

        private string formatData(string address, string email, string verifyUid)
        {
            //string VerifyUidUrl = "https://apiwallet.nel.group/api/testnet?jsonrpc=2.0&method=verifyEmail&params=[%22{0}%22,%22{1}%22,%22{2}%22]&id=1";
            string verifyUidUrl = Config.nelApiWalletUrl;
            if (verifyUidUrl.StartsWith("/"))
            {
                verifyUidUrl = verifyUidUrl.Substring(0, verifyUidUrl.Length-1);
            }
            verifyUidUrl += "?jsonrpc=2.0&method=verifyEmail&params=[%22{0}%22,%22{1}%22,%22{2}%22]&id=1";
            return string.Format(verifyUidUrl, address, email, verifyUid);
        }
        private string generateUid()
        {
            string ss = "01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
            int len = ss.Length;

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 8; ++i)
            {
                sb.Append(ss[new Random().Next(len)]);
            }
            return sb.ToString();
        }
        private bool sendVerifyInfo(string verifyInfo, string toEmail)
        {
            try
            {
                eh.send(verifyInfo, toEmail, true);
                return true;
            } catch(Exception ex)
            {
                LogHelper.printEx(ex);
                return false;
            }
        }
        private void ping()
        {
            LogHelper.ping(batchInterval, name());
        }

        private EmailHelper eh = null;
        private void initEmailConfig(JToken root)
        {
            var config = Config.emailInfo.clone();
            if (root["subject"] != null)
            {
                config.subject = root["subject"].ToString();
            }
            if (root["body"] != null)
            {
                config.body = root["body"].ToString();
            }
            eh = new EmailHelper
            {
                config = config
            };
            /*
            eh = new EmailHelper
            {
                config = new EmailConfig
                {
                    mailFrom = root["mailFrom"].ToString(),
                    mailPwd = root["mailPwd"].ToString(),
                    smtpHost = root["smtpHost"].ToString(),
                    smtpPort = int.Parse(root["smtpPort"].ToString()),
                    smtpEnableSsl = bool.Parse(root["smtpEnableSsl"].ToString()),
                    subject = root["subject"].ToString(),
                    body = root["body"].ToString(),
                    listener = root["listener"].ToString(),
                }
            };
            */
        }
    }
    class VerifyState
    {
        public const string WaitSend = "2";
        public const string HaveSend = "3";
        public const string YES = "1";
        public const string NOT = "0";
    }


}
