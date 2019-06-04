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
    class DexMarketDataNotifyCollectTask : ContractTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private string remoteConnRecordCol;
        private string remoteConnStateCol;
        private string dexNotifyCollectRecordCol;
        private string dexNotifyCollectStateCol;
        private string dexEmailStateCol;
        private string domainOwnerCol;
        private string nnsSellingAddr = "AR5HjX5XfHTVXJCQQuAkYz47RgTUvn7mNr";
        private string dexSellingAddr = "Ae4hA91KNKV9jqCtPKdmcW42ixhSYzuH9S";
        private int batchSize;
        private int batchInterval;

        private DbConnInfo localConn;
        private DbConnInfo remoteConn;
        private DbConnInfo blockConn;
        private bool initSuccFlag = false;
        private bool hasCreateIndex = false;

        public DexMarketDataNotifyCollectTask(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];
            remoteConnRecordCol = cfg["remoteConnRecordCol"].ToString();
            remoteConnStateCol = cfg["remoteConnStateCol"].ToString();
            dexNotifyCollectRecordCol = cfg["dexNotifyCollectRecordCol"].ToString();
            dexNotifyCollectStateCol = cfg["dexNotifyCollectStateCol"].ToString();
            dexEmailStateCol = cfg["dexEmailStateCol"].ToString();
            domainOwnerCol = cfg["domainOwnerCol"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());
            //
            localConn = Config.localDbConnInfo;
            remoteConn = Config.remoteDbConnInfo;
            blockConn = Config.blockDbConnInfo;
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
            /*
类型包括：求购类型和成交类型
这个版本要通知的数据：
1求购我拥有的域名的时候通知我：域名xx正在被求购中，求购价格xx
2我的挂单被成交的时候通知我：域名XX已成交，成交价格XX。
暂时用这个，具体文案格式我后面再发你
             */

            long rh = getRemoteHeight();
            long lh = getLocalHeight();
            if (lh >= rh) return;

            for(long index=lh+1; index<=rh; ++index)
            {
                string findStr = new JObject() { {"blockindex", index }, { "$or", new JArray {
                    new JObject() { {"displayName", "NNSbet" } },
                    new JObject() { {"displayName", "NNSsell" }},
                    new JObject() { {"displayName", "NNSofferToBuy" } }
                } } }.ToString();
                var queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, remoteConnStateCol, findStr);
                if(queryRes != null && queryRes.Count > 0)
                {
                    var
                    rr = queryRes.Where(p => p["displayName"].ToString() == "NNSbet").ToList();
                    if (rr != null && rr.Count > 0) handleNNSbet(rr);
                    rr = queryRes.Where(p => p["displayName"].ToString() == "NNSsell").ToList();
                    if (rr != null && rr.Count > 0) handleNNSsell(rr);
                    rr = queryRes.Where(p => p["displayName"].ToString() == "NNSofferToBuy").ToList();
                    if (rr != null && rr.Count > 0) handleNNSofferToBuy(rr);
                }

                //
                updateRecord(index);
                log(index, rh);

            }

            if (hasCreateIndex) return;
            mh.setIndex(localConn.connStr, localConn.connDB, dexNotifyCollectStateCol, "{'txid':1}", "i_txid");
            mh.setIndex(localConn.connStr, localConn.connDB, dexNotifyCollectStateCol, "{'notifyState':1}", "i_notifyState");
            hasCreateIndex = true;
        }
        private void handleNNSbet(List<JToken> rr)
        {
            rr.ForEach(p => {
                string txid = p["txid"].ToString();
                if (hasTxid(txid)) return;
                //
                string orderid = p["auctionid"].ToString();
                string domain = p["fullDomain"].ToString();
                string price = p["price"].ToString();
                string notifyAddress = p["auctioner"].ToString();
                if (!tryGetNotifyEmail(notifyAddress, out string notifyEmail)) return;

                string assetHash = p["assetHash"].ToString();
                string assetName = getAssetName(assetHash);

                long time = TimeHelper.GetTimeStamp();
                bool isOnePriceDeal = p["startPrice"].ToString() == p["endPrice"].ToString();
                string type = isOnePriceDeal ? NotifyType.OnePriceSellDeal : NotifyType.AucPriceSellDeal;
                //
                var info = new JObject {
                    { "type", type},
                    { "txid", txid },
                    { "domain", domain },
                    { "price", price},
                    { "assetName", assetName},
                    { "notifyAddress", notifyAddress},
                    { "notifyEmail", notifyEmail},
                    { "notifyState", NotifyState.WaitSend},
                    { "time", time},
                    { "lastUpdateTime", time}
                }.ToString();
                mh.PutData(localConn.connStr, localConn.connDB, dexNotifyCollectStateCol, info);
            });
        }
        private void handleNNSsell(List<JToken> rr)
        {
            rr.ForEach(p => {
                string txid = p["txid"].ToString();
                if (hasTxid(txid)) return;
                //
                string orderid = p["offerid"].ToString();
                string fullDomain = p["fullDomain"].ToString();
                string price = p["price"].ToString();
                string notifyAddress = p["buyer"].ToString();
                if (!tryGetNotifyEmail(notifyAddress, out string notifyEmail)) return;

                long time = TimeHelper.GetTimeStamp();
                string type = NotifyType.BuyDeal;
                //
                var info = new JObject {
                    { "type", type},
                    { "txid", txid },
                    { "domain", fullDomain },
                    { "price", price},
                    { "notifyAddress", notifyAddress},
                    { "notifyEmail", notifyEmail},
                    { "notifyState", NotifyState.WaitSend},
                    { "time", time},
                    { "lastUpdateTime", time}
                }.ToString();
                mh.PutData(localConn.connStr, localConn.connDB, dexNotifyCollectStateCol, info);
            });
        }
        private void handleNNSofferToBuy(List<JToken> rr)
        {
            rr.ForEach(p =>
            {
                string txid = p["txid"].ToString();
                if (hasTxid(txid)) return;
                //
                string orderid = p["offerid"].ToString();
                string fullDomain = p["fullDomain"].ToString();
                string price = p["price"].ToString();

                // 获取域名owner，除去nns.addr和dex.addr
                string fullHash = p["fullHash"].ToString();
                if (!tryGetNotifyAddress(fullHash, out string notifyAddress)) return;
                if (!tryGetNotifyEmail(notifyAddress, out string notifyEmail)) return;

                long time = TimeHelper.GetTimeStamp();
                string type = NotifyType.SomeOneWant2Buy;
                //
                var info = new JObject {
                    { "type", type},
                    { "txid", txid },
                    { "domain", fullDomain },
                    { "price", price},
                    { "notifyAddress", notifyAddress},
                    { "notifyEmail", notifyEmail},
                    { "notifyState", NotifyState.WaitSend},
                    { "time", time},
                    { "lastUpdateTime", time}
                }.ToString();
                mh.PutData(localConn.connStr, localConn.connDB, dexNotifyCollectStateCol, info);
            });
        }

        private Dictionary<string, string> assetNameDict;
        private string getAssetName(string assetHash)
        {
            if (assetNameDict == null) assetNameDict = new Dictionary<string, string>();
            if (assetNameDict.TryGetValue(assetHash, out string assetName)) return assetName;

            // 
            string findStr = new JObject() { { "assetid", assetHash } }.ToString();
            var queryRes = mh.GetData(blockConn.connStr, blockConn.connDB, "NEP5asset", findStr);
            if (queryRes != null && queryRes.Count > 0)
            {
                string name = queryRes[0]["symbol"].ToString();
                assetNameDict.Add(assetHash, name);
                return name;
            }
            return "";
        }
        private bool hasTxid(string txid)
        {
            string findStr = new JObject { { "txid", txid } }.ToString();
            return mh.GetDataCount(localConn.connStr, localConn.connDB, dexNotifyCollectStateCol, findStr) > 0;
        }
        private bool tryGetNotifyEmail(string notifyAddress, out string NotifyEmail)
        {
            string findStr = new JObject { { "address", notifyAddress }, { "activeState", ActiveState.YES }, { "verifyState", VerifyState.YES } }.ToString();
            string fieldStr = new JObject { { "email", 1 }, { "_id", 1 } }.ToString();
            var queryRes = mh.GetDataWithField(localConn.connStr, localConn.connDB, dexEmailStateCol, fieldStr, findStr);
            if(queryRes == null || queryRes.Count == 0)
            {
                NotifyEmail = "";
                return false;
            }
            NotifyEmail = queryRes[0]["email"].ToString();
            return true;
        }
        private bool tryGetNotifyAddress(string namehash, out string NotifyAddress)
        {
            string findStr = new JObject { { "namehash", namehash },{ "$and", new JArray {
                new JObject { {"owner",new JObject { { "$ne", nnsSellingAddr }} }},
                new JObject { {"owner",new JObject { { "$ne", dexSellingAddr }} }}
            } } }.ToString();
            string sortStr = new JObject { { "blockindex", -1} }.ToString();
            var queryRes = mh.GetData(localConn.connStr, localConn.connDB, domainOwnerCol, findStr, sortStr, 0,1);
            if(queryRes == null || queryRes.Count ==0)
            {
                NotifyAddress = "";
                return false;
            }
            NotifyAddress = queryRes[0]["owner"].ToString();
            return true;
        }

        private void updateRecord(long height)
        {
            string newdata = new JObject() { { "counter", dexNotifyCollectStateCol }, { "lastBlockindex", height } }.ToString();
            string findstr = new JObject() { { "counter", dexNotifyCollectStateCol } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, dexNotifyCollectRecordCol, findstr);
            if (res == null || res.Count == 0)
            {
                mh.PutData(localConn.connStr, localConn.connDB, dexNotifyCollectRecordCol, newdata);
            }
            else
            {
                mh.ReplaceData(localConn.connStr, localConn.connDB, dexNotifyCollectRecordCol, newdata, findstr);
            }
        }
        private long getRemoteHeight()
        {
            string findStr = new JObject() { { "counter", "notify" } }.ToString();
            JArray res = mh.GetData(remoteConn.connStr, remoteConn.connDB, remoteConnRecordCol, findStr);
            if (res == null || res.Count == 0)
            {
                return 0;
            }
            return long.Parse(res[0]["lastBlockindex"].ToString());
        }
        private long getLocalHeight()
        {
            string findStr = new JObject() { { "counter", dexNotifyCollectStateCol } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, dexNotifyCollectRecordCol, findStr);
            if (res == null || res.Count == 0)
            {
                return -1;
            }
            return long.Parse(res[0]["lastBlockindex"].ToString());
        }
        private void log(long localHeight, long remoteHeight)
        {
            Console.WriteLine(DateTime.Now + string.Format(" {0}.self processed at {1}/{2}", name(), localHeight, remoteHeight));
        }

        private void ping()
        {
            LogHelper.ping(batchInterval, name());
        }

        
    }
    class ActiveState
    {
        public const string YES = "1";
        public const string NOT = "0";
    }
    class NotifyType
    {
        public const string OnePriceSellDeal = "1";
        public const string AucPriceSellDeal = "2";
        public const string BuyDeal = "3";
        public const string SomeOneWant2Buy = "4";
    }
    class NotifyState
    {
        public const string Succ = "1";
        public const string Fail = "0";
        public const string WaitSend = "2";
    }
}
