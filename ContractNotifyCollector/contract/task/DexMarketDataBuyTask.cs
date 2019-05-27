using ContractNotifyCollector.core;
using ContractNotifyCollector.helper;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace ContractNotifyCollector.contract.task
{
    class DexMarketDataBuyTask : ContractTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private string remoteConnRecordCol;
        private string remoteConnStateCol;
        private string dexDomainBuyRecordCol;
        private string dexDomainBuyStateCol;
        private string domainCenterHash = "0xbd3fa97e2bc841292c1e77f9a97a1393d5208b48";
        private string dexRecordTimeCol;
        private string dexStarStateCol;
        private string dexSellingAddr;
        private int batchInterval;
        private int batchSize;

        private DbConnInfo localConn;
        private DbConnInfo remoteConn;
        private DbConnInfo blockConn;
        private bool initSuccFlag = false;
        private bool hasCreateIndex = false;
        private bool firstRunFlag = true;

        public DexMarketDataBuyTask(string name) : base(name)
        {

        }

        public override void initConfig(JObject config)
        {
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];
            remoteConnRecordCol = cfg["remoteConnRecordCol"].ToString();
            remoteConnStateCol = cfg["remoteConnStateCol"].ToString();
            dexDomainBuyRecordCol = cfg["dexDomainBuyRecordCol"].ToString();
            dexDomainBuyStateCol = cfg["dexDomainBuyStateCol"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());
            //
            dexRecordTimeCol = cfg["dexRecordTimeCol"].ToString();
            dexStarStateCol = cfg["dexStarStateCol"].ToString();
            dexSellingAddr = cfg["dexSellingAddr"].ToString();
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
                    processStarCount();
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
            long rh = getRemoteHeight();
            long lh = getLocalHeight();
            if (lh >= rh)
            {
                log(lh, rh);
                return;
            }
            if (firstRunFlag)
            {
                rh = lh + 1;
                firstRunFlag = false;
            }
            for (long index = lh + 1; index <= rh; ++index)
            {
                string findStr = new JObject() { {"blockindex", index }, { "$or", new JArray {
                    new JObject() { {"displayName", "NNSofferToBuy" } },
                    new JObject() { {"displayName", "NNSofferToBuyDiscontinued" } },
                    new JObject() { {"displayName", "NNSsell" } },
                    new JObject() { {"displayName", "handleSellMoney" } }
                } } }.ToString();
                var queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, remoteConnStateCol, findStr);
                if (queryRes != null && queryRes.Count > 0)
                {
                    long time = getBlockTime(index);
                    //
                    var
                    rr = queryRes.Where(p => p["displayName"].ToString() == "NNSofferToBuy").ToList();
                    if (rr != null && rr.Count() > 0) handleOnOfferToBuy(rr, index, time);
                    rr = queryRes.Where(p => p["displayName"].ToString() == "NNSofferToBuyDiscontinued").ToList();
                    if (rr != null && rr.Count() > 0) handleOnOfferToBuyDiscontinued(rr, index);
                    rr = queryRes.Where(p => p["displayName"].ToString() == "NNSsell").ToList();
                    if (rr != null && rr.Count() > 0) handleOnSell(rr, index);
                    rr = queryRes.Where(p => p["displayName"].ToString() == "handleSellMoney").ToList();
                    if (rr != null && rr.Count() > 0) handleOnHandleSellMoney(rr, index);
                }

                findStr = new JObject() { { "blockindex", index } }.ToString();
                queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, domainCenterHash, findStr);
                if (queryRes != null && queryRes.Count > 0)
                {
                    //
                    handleDomainCenter(queryRes, index);
                }

                updateRecord(index);
                log(index, rh);
            }

            // 更新starCount
            // 。。。

            if (hasCreateIndex) return;
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainBuyStateCol, "{'fullHash':1}", "i_fullHash");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainBuyStateCol, "{'fullDomain':1}", "i_fullDomain");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainBuyStateCol, "{'fullDomain':1,'buyer':1}", "i_fullDomain_buyer");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainBuyStateCol, "{'ttl':1}", "i_ttl");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainBuyStateCol, "{'starCount':1}", "i_starCount");
            hasCreateIndex = true;
        }

        private void handleOnOfferToBuy(List<JToken> rr, long blockindex, long blocktime)
        {
            rr.ToList().ForEach(p => {
                string fullDomain = p["fullDomain"].ToString();
                string buyer = p["buyer"].ToString();

                string findStr = new JObject { {"fullDomain", fullDomain }, { "buyer", buyer} }.ToString();
                var queryRes = mh.GetData<DexMarketDataBuyInfo>(localConn.connStr, localConn.connDB, dexDomainBuyStateCol, findStr);
                if(queryRes == null || queryRes.Count == 0)
                {
                    var jo = getDomainTTL(p["fullHash"].ToString());
                    var info = new DexMarketDataBuyInfo
                    {
                        fullHash = p["fullHash"].ToString(),
                        fullDomain = p["fullDomain"].ToString(),
                        assetHash = p["assetHash"].ToString(),
                        assetName = getAssetName(p["assetHash"].ToString()),
                        buyer = p["buyer"].ToString(),
                        time = blocktime,
                        price = decimal.Parse(p["price"].ToString()).format(),
                        mortgagePayments = long.Parse(p["mortgagePayments"].ToString()),
                        owner = jo["owner"].ToString(),
                        ttl = long.Parse(jo["TTL"].ToString()),
                        starCount = calcStarCount(p["fullDomain"].ToString())
                    };
                    mh.PutData(localConn.connStr, localConn.connDB, dexDomainBuyStateCol, info);
                }

            });
        }
        private void handleOnOfferToBuyDiscontinued(List<JToken> rr, long blockindex)
        {
            rr.ToList().ForEach(p => {
                string fullDomain = p["fullDomain"].ToString();
                string buyer = p["buyer"].ToString();

                string findStr = new JObject { { "fullDomain", fullDomain }, { "buyer", buyer } }.ToString();
                mh.DeleteData(localConn.connStr, localConn.connDB, dexDomainBuyStateCol, findStr);
            });
        }
        private void handleOnSell(List<JToken> rr, long blockindex)
        {
            handleOnOfferToBuyDiscontinued(rr, blockindex);
        }
        private void handleOnHandleSellMoney(List<JToken> rr, long blockindex)
        {

        }

        //
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
        private long getBlockTime(long index)
        {
            string findStr = new JObject { { "index", index } }.ToString();
            string fieldStr = new JObject { { "time", 1 } }.ToString();
            var queryRes = mh.GetDataWithField(blockConn.connStr, blockConn.connDB, "block", fieldStr, findStr);
            return (long)queryRes[0]["time"];
        }
        private JToken getDomainTTL(string fullHash)
        {
            string findStr = new JObject { { "namehash", fullHash } }.ToString();
            string fieldStr = new JObject { { "owner", 1 }, { "TTL", 1 } }.ToString();
            string sortStr = new JObject { { "blockindex", -1 } }.ToString();
            var queryRes = mh.GetDataPagesWithField(localConn.connStr, localConn.connDB, domainCenterHash, fieldStr, 1, 1, sortStr, findStr);
            return queryRes[0];
        }
        //
        private long calcStarCount(string fullDomain)
        {
            string findStr = new JObject { { "fullDomain", fullDomain }, { "state", "1" } }.ToString();
            long count = mh.GetDataCount(localConn.connStr, localConn.connDB, dexStarStateCol, findStr);
            return count;
        }
        private void processStarCount()
        {
            string starKey = "starCount.dexDomainSellState";
            long lt = getLastCalcTime(starKey);
            if (lt == -1) return;
            string findStr = new JObject { { "lastUpdateTime", new JObject { { "$gt", lt } } } }.ToString();
            var queryRes = mh.GetData(localConn.connStr, localConn.connDB, dexStarStateCol, findStr);
            if (queryRes == null || queryRes.Count == 0) return;

            var res = queryRes.GroupBy(p => p["fullDomain"], (k, g) =>
            {
                return new
                {
                    fullDomain = k.ToString(),
                    lastUpdateTime = g.Sum(pg => long.Parse(pg["lastUpdateTime"].ToString()))
                };
            }).GroupBy(p => p.lastUpdateTime, (k, g) => {
                return new
                {
                    lastUpdateTime = k,
                    fullDomainSet = g
                };
            }).OrderBy(p => p.lastUpdateTime);

            string updateStr = null;
            long starCount = 0;
            foreach (var item in res)
            {

                foreach (var sub in item.fullDomainSet)
                {
                    // 获取最新的关注数量
                    findStr = new JObject { { "fullDomain", sub.fullDomain }, { "state", "1" } }.ToString();
                    starCount = mh.GetDataCount(localConn.connStr, localConn.connDB, dexStarStateCol, findStr);

                    // 更新出售合约列表
                    updateStr = new JObject { { "$set", new JObject { { "starCount", starCount } } } }.ToString();
                    mh.UpdateData(localConn.connStr, localConn.connDB, dexDomainBuyStateCol, updateStr, findStr);
                }

                // 更新高度
                updateLastCalcTime(starKey, item.lastUpdateTime);
            }
        }
        private void updateLastCalcTime(string key, long time)
        {
            string findStr = new JObject { { "key", key } }.ToString();
            string updateStr = new JObject { { "lastUpdateTime", time } }.ToString();
            mh.UpdateData(localConn.connStr, localConn.connDB, dexRecordTimeCol, updateStr, findStr);
        }
        private long getLastCalcTime(string key)
        {
            string findStr = new JObject { { "key", key } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, dexRecordTimeCol, findStr);
            if (res == null || res.Count == 0)
            {
                return -1;
            }
            return long.Parse(res[0]["lastUpdateTime"].ToString());
        }
        //
        private void handleDomainCenter(JArray ja, long blockindex)
        {
            foreach (var jo in ja)
            {
                if (jo["owner"].ToString() == dexSellingAddr) continue;

                string fullHash = jo["namehash"].ToString();
                string owner = jo["owner"].ToString();
                string ttl = jo["TTL"].ToString();
                string findStr = new JObject() { { "fullHash", fullHash } }.ToString();
                string fieldStr = new JObject() { { "owner", 1 }, { "ttl", 1 }, { "_id", 0 } }.ToString();
                var queryRes = mh.GetDataWithField(localConn.connStr, localConn.connDB, domainCenterHash, fieldStr, findStr);
                if (queryRes != null && queryRes.Count > 0)
                {
                    if (queryRes[0]["owner"].ToString() != owner || queryRes[0]["ttl"].ToString() != ttl)
                    {
                        string updateStr = new JObject() { { "$set", new JObject() { { "owner", owner }, { "ttl", long.Parse(ttl) } } } }.ToString();
                        mh.UpdateData(localConn.connStr, localConn.connDB, dexDomainBuyStateCol, updateStr, findStr);
                    }
                }
            }
        }


        private void updateRecord(long height, string domainCenterCounter = "")
        {
            string newdata = new JObject() { { "counter", dexDomainBuyStateCol }, { "lastBlockindex", height } }.ToString();
            string findstr = new JObject() { { "counter", dexDomainBuyStateCol } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, dexDomainBuyRecordCol, findstr);
            if (res == null || res.Count == 0)
            {
                mh.PutData(localConn.connStr, localConn.connDB, dexDomainBuyRecordCol, newdata);
            }
            else
            {
                mh.ReplaceData(localConn.connStr, localConn.connDB, dexDomainBuyRecordCol, newdata, findstr);
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
            string findStr = new JObject() { { "counter", dexDomainBuyStateCol } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, dexDomainBuyRecordCol, findStr);
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


        [BsonIgnoreExtraElements]
        class DexMarketDataBuyInfo
        {
            public ObjectId _id { get; set; }
            public string fullHash { get; set; }
            public string fullDomain { get; set; }
            public string assetHash { get; set; }
            public string assetName { get; set; }
            public string buyer { get; set; }
            public BsonDecimal128 price { get; set; }
            public long time { get; set; }
            public long mortgagePayments { get; set; }
            public string owner { get; set; }
            public long ttl { get; set; }
            public long starCount { get; set; } = 0;
        }
    }


}

