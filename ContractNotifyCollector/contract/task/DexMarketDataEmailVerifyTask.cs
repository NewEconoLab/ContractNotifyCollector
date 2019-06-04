using ContractNotifyCollector.core;
using ContractNotifyCollector.helper;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;
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
        private bool firstRunFlag = true;

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
            string fieldStr = new JObject { { "email", 1},{ "_id",0} }.ToString();
            string sortStr = new JObject { { "time", 1} }.ToString();
            var queryRes = mh.GetDataPagesWithField(localConn.connStr, localConn.connDB, dexEmailStateCol, fieldStr, batchSize, 1, sortStr, findStr);
            if (queryRes == null || queryRes.Count == 0) return;

            string updateStr = null;
            foreach(var item in queryRes)
            {
                // 生成验证码
                string verifyUid = "123456";
                string email = item["email"].ToString();
                // 发送
                if(sendVerifyUid(verifyUid, email))
                {
                    // 更新
                    updateStr = new JObject { { "$set", new JObject { { "verifyState", VerifyState.HaveSend }, { "verifyUid", verifyUid } } } }.ToString();
                    mh.UpdateData(localConn.connStr, localConn.connDB, dexEmailStateCol, updateStr, findStr);
                }
            }
        }
        private bool sendVerifyUid(string verifyUid, string toEmail)
        {
            try
            {
                eh.send(verifyUid, toEmail);
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
