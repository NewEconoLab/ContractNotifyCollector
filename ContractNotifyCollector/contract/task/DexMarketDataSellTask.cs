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
    class DexMarketDataSellTask : ContractTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private string remoteConnRecordCol;
        private string remoteConnStateCol;
        private string dexDomainSellRecordCol;
        private string dexDomainSellStateCol;
        private string domainCenterHash = "0xbd3fa97e2bc841292c1e77f9a97a1393d5208b48";
        private string dexRecordTimeCol;
        private string dexStarStateCol;
        private string dexSellingAddr;
        private string nnsSellingAddr;
        private int batchInterval;
        private int batchSize;

        private DbConnInfo localConn;
        private DbConnInfo remoteConn;
        private DbConnInfo blockConn;
        private bool initSuccFlag = false;
        private bool hasCreateIndex = false;
        private bool firstRunFlag = true;

        public DexMarketDataSellTask(string name) : base(name)
        {

        }

        public override void initConfig(JObject config)
        {
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];
            remoteConnRecordCol = cfg["remoteConnRecordCol"].ToString();
            remoteConnStateCol = cfg["remoteConnStateCol"].ToString();
            dexDomainSellRecordCol = cfg["dexDomainSellRecordCol"].ToString();
            dexDomainSellStateCol = cfg["dexDomainSellStateCol"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());
            //
            dexRecordTimeCol = cfg["dexRecordTimeCol"].ToString();
            dexStarStateCol = cfg["dexStarStateCol"].ToString();
            dexSellingAddr = cfg["dexSellingAddr"].ToString();
            nnsSellingAddr = cfg["nnsSellingAddr"].ToString();
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
                    processDomainTTL();
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
                    new JObject() { {"displayName", "NNSauction" } },
                    new JObject() { {"displayName", "NNSauctionDiscontinued" } },
                    new JObject() { {"displayName", "NNSbet" } },
                    new JObject() { {"displayName", "handleBetMoney" } }
                } } }.ToString();
                var queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, remoteConnStateCol, findStr);
                if (queryRes != null && queryRes.Count > 0)
                {
                    long time = getBlockTime(index);
                    //
                    var
                    rr = queryRes.Where(p => p["displayName"].ToString() == "NNSauction").ToList();
                    if (rr != null && rr.Count() > 0) handleOnAuction(rr, index, time);
                    rr = queryRes.Where(p => p["displayName"].ToString() == "NNSauctionDiscontinued").ToList();
                    if (rr != null && rr.Count() > 0) handleOnAuctionDiscontinued(rr, index);
                    rr = queryRes.Where(p => p["displayName"].ToString() == "NNSbet").ToList();
                    if (rr != null && rr.Count() > 0) handleOnBet(rr, index);
                    rr = queryRes.Where(p => p["displayName"].ToString() == "handleBetMoney").ToList();
                    if (rr != null && rr.Count() > 0) handleOnHandleBetMoney(rr, index);
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

            // 更新nowPriceAndSaleRate
            loopCalcNowPriceAndSaleRate(rh);

            // 更新starCount
            // 。。。


            if (hasCreateIndex) return;
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainSellStateCol, "{'orderid':1}", "i_orderid");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainSellStateCol, "{'fullHash':1}", "i_fullHash");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainSellStateCol, "{'fullDomain':1}", "i_fullDomain");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainSellStateCol, "{'lastCalcTime':1}", "i_lastCalcTime");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainSellStateCol, "{'lastCalcTime':1,'calcType':1}", "i_lastCalcTime_calcType");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainSellStateCol, "{'ttl':1}", "i_ttl");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainSellStateCol, "{'ttl':1,'sellState':1}", "i_ttl_sellState");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainSellStateCol, "{'fullDomain':1,'sellState':1}", "i_fullDomain_sellState");
            hasCreateIndex = true;
        }

        private void handleOnAuction(List<JToken> rr, long blockindex, long blocktime)
        {
            rr.ForEach(p => {

                string orderid = p["auctionid"].ToString();

                string findStr = new JObject { { "orderid", orderid } }.ToString();
                var queryRes = mh.GetData<DexMarketDataSellInfo>(localConn.connStr, localConn.connDB, dexDomainSellStateCol, findStr);
                if (queryRes == null || queryRes.Count == 0)
                {
                    var jo = getDomainTTL(p["fullHash"].ToString());
                    var startPrice = decimal.Parse(p["startPrice"].ToString());
                    var endPrice = decimal.Parse(p["endPrice"].ToString());
                    var salePrice = decimal.Parse(p["salePrice"].ToString());
                    var startTimeStamp = long.Parse(p["startTimeStamp"].ToString());
                    calcNowPriceAndSaleRate(startPrice, endPrice, salePrice, startTimeStamp, blocktime, out decimal nowPrice, out decimal saleRate, out int calcType);
                    var info = new DexMarketDataSellInfo
                    {
                        orderid = orderid,
                        fullHash = p["fullHash"].ToString(),
                        fullDomain = p["fullDomain"].ToString(),
                        sellType = startPrice == endPrice ? DexSellType.OneSell : DexSellType.SaleSell,
                        sellState = DexSellState.Normal_YES,
                        assetHash = p["assetHash"].ToString(),
                        assetName = getAssetName(p["assetHash"].ToString()),
                        seller = p["auctioner"].ToString(),
                        startTimeStamp = long.Parse(p["startTimeStamp"].ToString()),
                        sellTime = blocktime,
                        startPrice = startPrice.format(),
                        endPrice = endPrice.format(),
                        salePrice = salePrice.format(),
                        nowPrice = nowPrice.format(),
                        saleRate = saleRate.format(),
                        lastCalcTime = blocktime,
                        calcType = calcType,
                        mortgagePayments = long.Parse(p["mortgagePayments"].ToString()),
                        owner = jo["owner"].ToString(),
                        ttl = long.Parse(jo["TTL"].ToString()),
                        starCount = calcStarCount(orderid),
                    };
                    mh.PutData(localConn.connStr, localConn.connDB, dexDomainSellStateCol, info);
                } 
            });
        }
        private void handleOnAuctionDiscontinued(List<JToken> rr, long blockindex)
        {
            rr.ForEach(p =>
            {
                string findStr = new JObject() { { "orderid", p["auctionid"] } }.ToString();
                mh.DeleteData(localConn.connStr, localConn.connDB, dexDomainSellStateCol, findStr);
            });
        }
        private void handleOnBet(List<JToken> rr, long blockindex)
        {
            handleOnAuctionDiscontinued(rr, blockindex);
        }
        private void handleOnHandleBetMoney(List<JToken> rr, long blockindex)
        {
            // 
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
            //string findStr = new JObject { { "namehash", fullHash } }.ToString();
            string findStr = new JObject { { "namehash", fullHash },{ "$and", new JArray {
                new JObject { {"owner",new JObject { { "$ne", nnsSellingAddr }} }},
                new JObject { {"owner",new JObject { { "$ne", dexSellingAddr }} }}
            } } }.ToString();
            string fieldStr = new JObject { { "owner", 1 }, { "TTL", 1 } }.ToString();
            string sortStr = new JObject { { "blockindex", -1 } }.ToString();
            var queryRes = mh.GetDataPagesWithField(localConn.connStr, localConn.connDB, domainCenterHash, fieldStr, 1, 1, sortStr, findStr);
            return queryRes[0];
        }
        //
        private long ONE_DAY_SECONDS = 24 * 60 * 60;
        private void calcNowPriceAndSaleRate(decimal startPrice, decimal endPrice, decimal salePrice, long startTimeStamp, long blocktime, out decimal nowPrice, out decimal saleRate, out int calcType)
        {
            nowPrice = startPrice;
            saleRate = 0;
            calcType = DexCalcType.DoneCalc;
            if (startPrice == endPrice) return;

            long div = (blocktime - startTimeStamp) / ONE_DAY_SECONDS;
            long rem = (blocktime - startTimeStamp) % ONE_DAY_SECONDS;
            if (div > 0)
            {
                nowPrice = startPrice - salePrice * div;
            }
            if (nowPrice < endPrice)
            {
                nowPrice = endPrice;
            }

            saleRate = (startPrice - nowPrice) / (startPrice - endPrice);
            saleRate = decimal.Parse(saleRate.ToString("#0.0000"));
            if (nowPrice != endPrice) calcType = DexCalcType.NeedCalc;
        }
        private void loopCalcNowPriceAndSaleRate(long blockindex)
        {
            long blocktime = getBlockTime(blockindex);
            string findStr = new JObject { { "calcType", DexCalcType.NeedCalc }, { "lastCalcTime", new JObject { { "$lt", blocktime - ONE_DAY_SECONDS } } } }.ToString();
            var queryRes = mh.GetData<DexMarketDataSellInfo>(localConn.connStr, localConn.connDB, dexDomainSellStateCol, findStr);
            if (queryRes == null || queryRes.Count == 0) return;

            queryRes.ForEach(p =>
            {
                var startPrice = p.startPrice.format();
                var endPrice = p.endPrice.format();
                var salePrice = p.salePrice.format();
                var startTimeStamp = p.startTimeStamp;
                calcNowPriceAndSaleRate(startPrice, endPrice, salePrice, startTimeStamp, blocktime, out decimal nowPrice, out decimal saleRate, out int calcType);
                p.nowPrice = nowPrice.format();
                p.saleRate = saleRate.format();
                p.calcType = calcType;
                p.lastCalcTime = blocktime;

                var filter = new JObject { { "orderid", p.orderid } }.ToString();
                mh.ReplaceData(localConn.connStr, localConn.connDB, dexDomainSellStateCol, p, filter);
            });

        }
        //
        private long calcStarCount(string orderid)
        {
            string findStr = new JObject { { "orderid", orderid }, { "state", StarState.Star_Yes } }.ToString();
            long count = mh.GetDataCount(localConn.connStr, localConn.connDB, dexStarStateCol, findStr);
            return count;
        }
        private void processStarCount()
        {
            string starKey = "starCount.dexDomainSellState";
            long lt = getLastCalcTime(starKey);
            //if (lt == -1) return;
            string findStr = new JObject { { "lastUpdateTime", new JObject { { "$gt", lt } } } }.ToString();
            var queryRes = mh.GetData(localConn.connStr, localConn.connDB, dexStarStateCol, findStr);
            if (queryRes == null || queryRes.Count == 0) return;

            var res = queryRes.GroupBy(p => p["orderid"], (k, g) =>
            {
                return new
                {
                    orderid = k.ToString(),
                    lastUpdateTime = g.Max(pg => long.Parse(pg["lastUpdateTime"].ToString()))
                };
            }).GroupBy(p => p.lastUpdateTime, (k, g) => {
                return new
                {
                    lastUpdateTime = k,
                    orderidSet = g
                };
            }).OrderBy(p => p.lastUpdateTime);

            string updateStr = null;
            long starCount = 0;
            foreach (var item in res)
            {

                foreach (var sub in item.orderidSet)
                {
                    // 获取最新的关注数量
                    findStr = new JObject { { "orderid", sub.orderid }, { "state", StarState.Star_Yes } }.ToString();
                    starCount = mh.GetDataCount(localConn.connStr, localConn.connDB, dexStarStateCol, findStr);

                    // 更新出售合约列表
                    findStr = new JObject { { "orderid", sub.orderid }, { "sellState", DexSellState.Normal_YES } }.ToString();
                    updateStr = new JObject { { "$set", new JObject { { "starCount", starCount } } } }.ToString();
                    mh.UpdateData(localConn.connStr, localConn.connDB, dexDomainSellStateCol, updateStr, findStr);
                }

                // 更新高度
                updateLastCalcTime(starKey, item.lastUpdateTime);
            }
        }
        private void updateLastCalcTime(string key, long time)
        {
            string findStr = new JObject { { "key", key } }.ToString();
            string updateStr = new JObject { { "$set", new JObject { { "lastUpdateTime", time } } } }.ToString();
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
                if (jo["owner"].ToString() == nnsSellingAddr) continue;

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
                        findStr = getOrderIdFindStr(fullHash);
                        if (findStr == null) continue;

                        string updateStr = new JObject() { { "$set", new JObject() { { "owner", owner }, { "ttl", long.Parse(ttl) } } } }.ToString();
                        mh.UpdateData(localConn.connStr, localConn.connDB, dexDomainSellStateCol, updateStr, findStr);
                    }
                }
            }
        }
        private string getOrderIdFindStr(string fullHash)
        {
            string findStr = new JObject { { "fullHash", fullHash } }.ToString();
            string fieldStr = new JObject { { "orderid", 1 } }.ToString();
            string sortStr = new JObject { { "sellTime", -1 } }.ToString();
            var queryRes = mh.GetDataPagesWithField(localConn.connStr, localConn.connDB, dexDomainSellStateCol, fieldStr, 1, 1, sortStr, findStr);
            if (queryRes != null && queryRes.Count > 0)
            {
                return new JObject { { "orderid", queryRes[0]["orderid"] } }.ToString();
            }
            return null;
        }
        //
        private void processDomainTTL()
        {
            string findStr = new JObject { { "ttl", new JObject { { "$lt", TimeHelper.GetTimeStamp() } } }, { "sellState", DexSellState.Normal_YES } }.ToString();
            string fieldStr = new JObject { { "orderid", 1 } }.ToString();
            var queryRes = mh.GetDataWithField(localConn.connStr, localConn.connDB, dexDomainSellStateCol, fieldStr, findStr);
            if (queryRes == null || queryRes.Count == 0) return;

            queryRes.ToList().ForEach(p => {
                string findstr = new JObject { { "orderid", p["orderid"] } }.ToString();
                string updatestr = new JObject { { "$set", new JObject { { "sellState", DexSellState.Normal_Not } } } }.ToString();
                mh.UpdateData(localConn.connStr, localConn.connDB, dexDomainSellStateCol, updatestr, findstr);
            });
        }


        private void updateRecord(long height)
        {
            string newdata = new JObject() { { "counter", dexDomainSellStateCol }, { "lastBlockindex", height } }.ToString();
            string findstr = new JObject() { { "counter", dexDomainSellStateCol } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, dexDomainSellRecordCol, findstr);
            if (res == null || res.Count == 0)
            {
                mh.PutData(localConn.connStr, localConn.connDB, dexDomainSellRecordCol, newdata);
            }
            else
            {
                mh.ReplaceData(localConn.connStr, localConn.connDB, dexDomainSellRecordCol, newdata, findstr);
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
            string findStr = new JObject() { { "counter", dexDomainSellStateCol } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, dexDomainSellRecordCol, findStr);
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
    [BsonIgnoreExtraElements]
    class DexMarketDataSellInfo
    {
        public ObjectId _id { get; set; }
        public string orderid { get; set; }
        public string fullHash { get; set; }
        public string fullDomain { get; set; }
        public int sellType { get; set; } // 降价出售/一口价
        public int sellState { get; set; } // 1正常出售,0出售中过期无需更新ttl
        public string assetHash { get; set; }
        public string assetName { get; set; }
        public string seller { get; set; }
        public long startTimeStamp { get; set; }
        public long sellTime { get; set; }
        public BsonDecimal128 startPrice { get; set; }
        public BsonDecimal128 endPrice { get; set; }
        public BsonDecimal128 salePrice { get; set; }
        public BsonDecimal128 nowPrice { get; set; }
        public BsonDecimal128 saleRate { get; set; }
        public long lastCalcTime { get; set; }
        public int calcType { get; set; }
        public long mortgagePayments { get; set; }
        public string owner { get; set; }
        public long ttl { get; set; }
        public long starCount { get; set; } = 0;
    }

    class StarState
    {
        public const string Star_Yes = "1";
        public const string Star_Not = "0";
    }
    class DexSellState
    {
        public const int Normal_YES = 1; // 正常出售
        public const int Normal_Not = 0; // 出售中域名过期，无需更新该挂单ttl
    }
    class DexSellType
    {
        public const int SaleSell = 0;
        public const int OneSell = 1;
    }
    class DexCalcType
    {
        public const int NeedCalc = 1;
        public const int DoneCalc = 0;
    }
}
