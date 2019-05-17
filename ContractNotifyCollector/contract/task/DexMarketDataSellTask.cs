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
        private int batchInterval;
        private int batchSize;

        private DbConnInfo localConn;
        private DbConnInfo remoteConn;
        private DbConnInfo blockConn;
        private bool initSuccFlag = false;
        private bool hasCreateIndex = false;
        private bool firstRunFlag = true;

        public DexMarketDataSellTask(string name): base(name)
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
            if(firstRunFlag)
            {
                rh = lh + 1;
                firstRunFlag = false;
            }

            for (long index = lh + 1; index <= rh; ++index) {
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
                    rr = queryRes.Where(p => p["displayName"].ToString() == "NNSauction").ToArray();
                    if (rr != null && rr.Count() > 0) handleOnAuction(rr, index, time);
                    rr = queryRes.Where(p => p["displayName"].ToString() == "NNSauctionDiscontinued").ToArray();
                    if (rr != null && rr.Count() > 0) handleOnAuctionDiscontinued(rr, index);
                    rr = queryRes.Where(p => p["displayName"].ToString() == "NNSbet").ToArray();
                    if (rr != null && rr.Count() > 0) handleOnBet(rr, index);
                    rr = queryRes.Where(p => p["displayName"].ToString() == "handleBetMoney").ToArray();
                    if (rr != null && rr.Count() > 0) handleOnHandleBetMoney(rr, index);
                }

                findStr = new JObject() { { "blockindex", index} }.ToString();
                queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, domainCenterHash, findStr);
                if(queryRes != null && queryRes.Count > 0)
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
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainSellStateCol, "{'fullHash':1}", "i_fullHash");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainSellStateCol, "{'fullDomain':1}", "i_fullDomain");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainSellStateCol, "{'lastCalcTime':1}", "i_lastCalcTime");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainSellStateCol, "{'lastCalcTime':1,'calcType':1}", "i_lastCalcTime_calcType");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainSellStateCol, "{'ttl':1}", "i_ttl");
            hasCreateIndex = true;
        }
        
        private void handleOnAuction(JToken[] rr, long blockindex, long blocktime)
        {
            var res = rr.Select(p => {

                var jo = getDomainTTL(p["fullHash"].ToString());

                var startPrice = decimal.Parse(p["startPrice"].ToString());
                var endPrice = decimal.Parse(p["endPrice"].ToString());
                var salePrice = decimal.Parse(p["salePrice"].ToString());
                var startTimeStamp = long.Parse(p["startTimeStamp"].ToString());
                calcNowPriceAndSaleRate(startPrice, endPrice, salePrice, startTimeStamp, blocktime, out decimal nowPrice, out decimal saleRate, out int calcType);

                return new DexMarketDataSellInfo
                {
                    fullHash = p["fullHash"].ToString(),
                    fullDomain = p["fullDomain"].ToString(),
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
                    starCount = 0,
                };
            }).ToArray();
            foreach(var item in res)
            {
                string findstr = new JObject() { {"fullHash", item.fullHash }, { "sellState", DexState.NNSauction } }.ToString();
                if(mh.GetDataCount(localConn.connStr, localConn.connDB, dexDomainSellStateCol, findstr) == 0)
                {
                    mh.PutData(localConn.connStr, localConn.connDB, dexDomainSellStateCol, item);
                }
                else
                {
                    mh.ReplaceData(localConn.connStr, localConn.connDB, dexDomainSellStateCol, item, findstr);
                }
                
            }
        }
        private void handleOnAuctionDiscontinued(JToken[] rr, long blockindex)
        {
            foreach(var item in rr)
            {
                string findStr = new JObject() { {"fullHash", item["fullHash"] } }.ToString();
                mh.DeleteData(localConn.connStr, localConn.connDB, dexDomainSellStateCol, findStr);
            }
        }
        private void handleOnBet(JToken[] rr, long blockindex)
        {
            handleOnAuctionDiscontinued(rr, blockindex);
        }
        private void handleOnHandleBetMoney(JToken[] rr, long blockindex)
        {
            // 
        }

        
        private Dictionary<string, string> assetNameDict;
        private string getAssetName(string assetHash)
        {
            if (assetNameDict == null) assetNameDict = new Dictionary<string, string>();
            if(assetNameDict.TryGetValue(assetHash, out string assetName)) return assetName;

            // 
            string findStr = new JObject() { { "assetid", assetHash } }.ToString();
            var queryRes = mh.GetData(blockConn.connStr, blockConn.connDB, "NEP5asset", findStr);
            if(queryRes != null && queryRes.Count > 0)
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
        private long ONE_DAY_SECONDS = 24 * 60 * 60;
        private void calcNowPriceAndSaleRate(decimal startPrice, decimal endPrice, decimal salePrice, long startTimeStamp, long blocktime, out decimal nowPrice, out decimal saleRate, out int calcType)
        {
            nowPrice = startPrice;
            saleRate = 0;
            calcType = DexCalcType.DoneCalc;
            if (startPrice == endPrice) return;

            long div = (blocktime - startTimeStamp) / ONE_DAY_SECONDS;
            long rem = (blocktime - startTimeStamp) % ONE_DAY_SECONDS;
            if(div > 0)
            {
                nowPrice = startPrice - salePrice * div;
            }
            if(nowPrice < endPrice)
            {
                nowPrice = endPrice;
            }

            saleRate = (startPrice - nowPrice) % (startPrice - endPrice);// 计算比例待修改
            if (nowPrice != endPrice) calcType = DexCalcType.NeedCalc;
        }
        private void loopCalcNowPriceAndSaleRate(long blockindex)
        {
            long blocktime = getBlockTime(blockindex);
            string findStr = new JObject { {"calcType", DexCalcType.NeedCalc }, {"lastCalcTime", new JObject { { "$lt", blocktime - ONE_DAY_SECONDS} } } }.ToString();
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

                var filter = new JObject { {"fullHash", p.fullHash } }.ToString();
                mh.ReplaceData(localConn.connStr, localConn.connDB, dexDomainSellStateCol, p, filter);
            });

        }

        private void handleDomainCenter(JArray ja, long blockindex)
        {
            foreach(var jo in ja)
            {
                string fullHash = jo["namehash"].ToString();
                string owner = jo["owner"].ToString();
                string ttl = jo["TTL"].ToString();
                string findStr = new JObject() { {"fullHash", fullHash } }.ToString();
                string fieldStr = new JObject() { { "owner", 1 }, { "ttl", 1 }, { "_id", 0 } }.ToString();
                var queryRes = mh.GetDataWithField(localConn.connStr, localConn.connDB, domainCenterHash, fieldStr, findStr);
                if(queryRes != null && queryRes.Count > 0)
                {
                    if(queryRes[0]["owner"].ToString() != owner || queryRes[0]["ttl"].ToString() != ttl)
                    {
                        string updateStr = new JObject() { { "$set", new JObject() { { "owner", owner}, { "ttl", long.Parse(ttl)} } } }.ToString();
                        mh.UpdateData(localConn.connStr, localConn.connDB, dexDomainSellStateCol, updateStr, findStr);
                    }
                }
            }
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
        public string fullHash { get; set; }
        public string fullDomain { get; set; }
        public int sellType { get; set; } // 降价出售/一口价
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
    
    class DexState
    {
        public static string NNSauction = "010001"; // 上架
        public static string NNSauctionDiscontinued = "010002"; // 下架
        public static string NNSbet = "010003"; // 成交
        public static string handleBetMoney = "010004"; // 领取盈利
    }
    class DexSellType
    {
        public const int SaleSell = 0;
        public const int OneSell = 1;
    }
    class DexCalcType
    {
        public const int NeedCalc = 0;
        public const int DoneCalc = 1;
    }
}
