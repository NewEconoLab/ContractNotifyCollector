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
            for(long index=lh+1; index<=rh; ++index)
            {
                string findStr = new JObject() { {"blockindex", index }, { "$or", new JArray {
                    new JObject() { {"displayName", "NNSofferToBuy" } },
                    new JObject() { {"displayName", "NNSofferToBuyDiscontinued" } },
                    new JObject() { {"displayName", "NNSsell" } },
                    new JObject() { {"displayName", "handleSellMoney" } }
                } } }.ToString();
                var queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, remoteConnStateCol, findStr);
                if(queryRes != null && queryRes.Count > 0)
                {
                    long time = getBlockTime(index);
                    //
                    var
                    rr = queryRes.Where(p => p["displayName"].ToString() == "NNSofferToBuy").ToArray();
                    if (rr != null && rr.Count() > 0) handleOnOfferToBuy(rr, index, time);
                    rr = queryRes.Where(p => p["displayName"].ToString() == "NNSofferToBuyDiscontinued").ToArray();
                    if (rr != null && rr.Count() > 0) handleOnOfferToBuyDiscontinued(rr, index);
                    rr = queryRes.Where(p => p["displayName"].ToString() == "NNSsell").ToArray();
                    if (rr != null && rr.Count() > 0) handleOnSell(rr, index);
                    rr = queryRes.Where(p => p["displayName"].ToString() == "handleSellMoney").ToArray();
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
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainBuyStateCol, "{'buyerList.buyer':1}", "i_buyerList_buyer");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainBuyStateCol, "{'ttl':1}", "i_ttl");
            hasCreateIndex = true;
        }

        private void handleOnOfferToBuy(JToken[] rr, long blockindex, long blocktime)
        {
            rr.GroupBy(p => p["fullHash"], (k, g) =>
            {
                return new
                {
                    kk = k,
                    gg = g
                };
            }).ToList().ForEach(pp => {
                var k = pp.kk;
                var g = pp.gg;
                //
                string fullHash = k.ToString();
                string fullDomain = g.ToArray()[0]["fullDomain"].ToString();
                string findStr = new JObject() { { "fullHash", fullHash } }.ToString();
                var queryRes = mh.GetData<DexMarketDataBuyInfo>(localConn.connStr, localConn.connDB, dexDomainBuyStateCol, findStr);

                DexMarketDataBuyInfo info = null;
                if (queryRes == null || queryRes.Count == 0)
                {
                    info = new DexMarketDataBuyInfo
                    {
                        fullHash = fullHash,
                        fullDomain = fullDomain,
                    };
                    var jo = getDomainTTL(fullHash);
                    info.owner = jo["owner"].ToString();
                    info.ttl = long.Parse(jo["TTL"].ToString());
                    // 
                    var maxBuyerInfo = g.OrderByDescending(p => decimal.Parse(p["price"].ToString())).First();
                    info.maxAssetHash = maxBuyerInfo["assetHash"].ToString();
                    info.maxAssetName = getAssetName(info.maxAssetHash);
                    info.maxBuyer = maxBuyerInfo["buyer"].ToString();
                    info.maxPrice = decimal.Parse(maxBuyerInfo["price"].ToString()).format();
                    info.maxTime = blocktime;
                    //
                    info.buyerList = g.Select(pg => new DexBuyerInfo
                    {
                        assetHash = pg["assetHash"].ToString(),
                        assetName = getAssetName(pg["assetHash"].ToString()),
                        buyer = pg["buyer"].ToString(),
                        price = decimal.Parse(pg["price"].ToString()).format(),
                        time = blocktime,
                    }).ToList();

                    //
                    mh.PutData(localConn.connStr, localConn.connDB, dexDomainBuyStateCol, info);
                }
                else
                {
                    info = queryRes[0];
                    var maxBuyerInfo = g.OrderByDescending(p => decimal.Parse(p["price"].ToString())).First();
                    if (info.maxPrice.format() < decimal.Parse(maxBuyerInfo["price"].ToString()))
                    {
                        info.maxAssetHash = maxBuyerInfo["assetHash"].ToString();
                        info.maxAssetName = getAssetName(info.maxAssetHash);
                        info.maxBuyer = maxBuyerInfo["buyer"].ToString();
                        info.maxPrice = decimal.Parse(maxBuyerInfo["price"].ToString()).format();
                        info.maxTime = blocktime;
                    }
                    foreach (var item in g)
                    {
                        var hasBuyer = info.buyerList.Where(gx => gx.buyer == item["buyer"].ToString()).First();
                        if(hasBuyer != null) info.buyerList.Remove(hasBuyer);
                        if(hasBuyer == null)
                        {
                            info.buyerList.Add(new DexBuyerInfo
                            {
                                assetHash = item["assetHash"].ToString(),
                                assetName = getAssetName(item["assetHash"].ToString()),
                                buyer = item["buy"].ToString(),
                                price = decimal.Parse(item["price"].ToString()).format(),
                                time = blocktime
                            });
                        }
                    }

                    //
                    mh.ReplaceData(localConn.connStr, localConn.connDB, dexDomainBuyStateCol, info, findStr);
                }
            });
        }
        private void handleOnOfferToBuyDiscontinued(JToken[] rr, long blockindex)
        {
            rr.GroupBy(p => p["fullHash"], (k, g) =>
            {
                return new {
                    kk = k,
                    gg = g
                };
            }).ToList().ForEach(pp => {
                var k = pp.kk;
                var g = pp.gg;
                string fullHash = k.ToString();
                string findStr = new JObject { { "fullHash", fullHash } }.ToString();
                var queryRes = mh.GetData<DexMarketDataBuyInfo>(localConn.connStr, localConn.connDB, dexDomainBuyStateCol, findStr);
                var info = queryRes[0];
                //
                info.buyerList =
                info.buyerList.Where(pg => g.All(pgx => pgx["buyer"].ToString() != pg.buyer)).ToList();
                
                // 
                if(info.buyerList.Count == 0)
                {
                    mh.DeleteData(localConn.connStr, localConn.connDB, dexDomainBuyStateCol, findStr);
                    return;
                }

                //
                mh.ReplaceData(localConn.connStr, localConn.connDB, dexDomainBuyStateCol, info, findStr);
            });
        }
        private void handleOnSell(JToken[] rr, long blockindex)
        {
            handleOnOfferToBuyDiscontinued(rr, blockindex);
        }
        private void handleOnHandleSellMoney(JToken[] rr, long blockindex)
        {

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
        private void handleDomainCenter(JArray ja, long blockindex)
        {
            foreach (var jo in ja)
            {
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
            public string buyState { get; set; }
            public string owner { get; set; }
            public long ttl { get; set; }
            public long starCount { get; set; } = 0;

            // 所有资产的最大出价者信息
            public string maxAssetHash { get; set; }
            public string maxAssetName { get; set; }
            public string maxBuyer { get; set; }
            public BsonDecimal128 maxPrice { get; set; }
            public long maxTime { get; set; }

            public List<DexBuyerInfo> buyerList { get; set; }


        }
        class DexBuyerInfo
        {
            public string assetHash { get; set; }
            public string assetName { get; set; }
            public string buyer { get; set; }
            public BsonDecimal128 price { get; set; }
            public long time { get; set; }

        }
    }

    
}

