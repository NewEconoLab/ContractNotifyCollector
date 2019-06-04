using ContractNotifyCollector.core;
using ContractNotifyCollector.helper;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace ContractNotifyCollector.contract.task
{
    /*
类型包括：求购类型和成交类型
这个版本要通知的数据：
1求购我拥有的域名的时候通知我：域名xx正在被求购中，求购价格xx
2我的挂单被成交的时候通知我：域名XX已成交，成交价格XX。
暂时用这个，具体文案格式我后面再发你
*/
    class DexMarketDataNotifySendTask : ContractTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private string dexNotifyCollectStateCol;
        private int batchSize;
        private int batchInterval;

        private DbConnInfo localConn;
        private bool initSuccFlag = false;
        private bool hasCreateIndex = false;

        public DexMarketDataNotifySendTask(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];
            dexNotifyCollectStateCol = cfg["dexNotifyCollectStateCol"].ToString();
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
            string findStr = new JObject { { "notifyState", NotifyState.WaitSend } }.ToString();
            var count = mh.GetDataCount(localConn.connStr, localConn.connDB, dexNotifyCollectStateCol, findStr);
            if (count == 0) return;

            long hasProcessCount = 0;
            while(hasProcessCount < count)
            {
                string sortStr = new JObject { { "lastUpdateTime", 1 } }.ToString();
                var queryRes = mh.GetData(localConn.connStr, localConn.connDB, dexNotifyCollectStateCol, findStr, sortStr, 0, 100);
                if (queryRes == null || queryRes.Count == 0) break;
                var res = queryRes.Select(p => {
                    string txid = p["txid"].ToString();
                    string email = p["notifyEmail"].ToString();
                    string domain = p["domain"].ToString();
                    string price = p["price"].ToString();
                    string assetName = p["assetName"].ToString();
                    string data = "";

                    string type = p["type"].ToString();
                    if (type == NotifyType.SomeOneWant2Buy)
                    {
                        data = NotifyDataFmt.s1;
                    }
                    else
                    {
                        data = NotifyDataFmt.s2;
                    }
                    data = string.Format(data, domain, price, assetName);

                    return new
                    {
                        notifyData = data,
                        notifyEmail = email,
                        updateTxid = txid
                    };
                }).ToArray();
                foreach (var item in res)
                {
                    sendMsg(item.notifyData, item.notifyEmail);
                    updateState(item.updateTxid);
                }
                hasProcessCount += res.Count();
            }

            if (hasCreateIndex) return;
            mh.setIndex(localConn.connStr, localConn.connDB, dexNotifyCollectStateCol, "{'notifyState':1}", "i_notifyState");
            hasCreateIndex = true;
        }

        private void sendMsg(string data, string email)
        {
            eh.send(data, email);
        }
        private void updateState(string txid)
        {
            string findStr = new JObject { { "txid", txid } }.ToString();
            string updateStr = new JObject { { "$set", new JObject { {"notifyState", NotifyState.Succ },{ "lastUpdateTime", TimeHelper.GetTimeStamp()} } } }.ToString();
            mh.UpdateData(localConn.connStr, localConn.connDB, dexNotifyCollectStateCol, updateStr, findStr);
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

        private void ping()
        {
            LogHelper.ping(batchInterval, name());
        }

        class NotifyDataFmt
        {
            public static string s1 = "域名{0}正在被求购中，求购价格{1} {2}";
            public static string s2 = "域名{0}已成交，成交价格{1} {2}";
        }

    }
}
