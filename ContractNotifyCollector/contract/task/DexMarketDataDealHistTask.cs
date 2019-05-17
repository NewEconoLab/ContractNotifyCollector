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
    class DexMarketDataDealHistTask : ContractTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private string remoteConnRecordCol;
        private string remoteConnStateCol;
        private string dexDomainDealHistRecordCol;
        private string dexDomainDealHistStateCol;
        private int batchInterval;
        private int batchSize;

        private DbConnInfo localConn;
        private DbConnInfo remoteConn;
        private DbConnInfo blockConn;
        private bool initSuccFlag = false;
        private bool hasCreateIndex = false;
        private bool firstRunFlag = true;

        public DexMarketDataDealHistTask(string name): base(name)
        {

        }

        public override void initConfig(JObject config)
        {
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];
            remoteConnRecordCol = cfg["remoteConnRecordCol"].ToString();
            remoteConnStateCol = cfg["remoteConnStateCol"].ToString();
            dexDomainDealHistRecordCol = cfg["dexDomainDealHistRecordCol"].ToString();
            dexDomainDealHistStateCol = cfg["dexDomainDealHistStateCol"].ToString();
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
            for (long index = lh + 1; index <= rh; ++index)
            {
                string findStr = new JObject() { {"blockindex", index }, { "$or", new JArray {
                    new JObject() { {"displayName", "NNSbet" } },
                    new JObject() { {"displayName", "NNSsell" } }
                } } }.ToString();
                var queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, remoteConnStateCol, findStr);
                if(queryRes != null && queryRes.Count > 0)
                {
                    long time = getBlockTime(index);
                    handleDealHist(queryRes, index, time);
                }

                updateRecord(index);
                log(index, rh);    
            }

            if (hasCreateIndex) return;
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainDealHistStateCol, "{'txid':1}", "i_txid");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainDealHistStateCol, "{'fullHash':1}", "i_fullHash");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainDealHistStateCol, "{'fullDomain':1}", "i_fullDomain");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainDealHistStateCol, "{'blockindex':1}", "i_blockindex");
            mh.setIndex(localConn.connStr, localConn.connDB, dexDomainDealHistStateCol, "{'blocktime':1}", "i_blocktime");
            hasCreateIndex = true;
        }

        private void handleDealHist(JArray ja, long index, long time)
        {
            ja.ToList().ForEach(p =>
            {
                string findStr = new JObject { { "txid", p["txid"] } }.ToString();
                if (mh.GetDataCount(localConn.connStr, localConn.connDB, dexDomainDealHistStateCol, findStr) > 0)
                {
                    return;
                }
                string displayName = p["displayName"].ToString();
                string seller = null;
                if (displayName == "NNSbet")
                {
                    seller = p["auctioner"].ToString();
                }
                else
                {
                    seller = p["addr"].ToString();
                }
                var info = new DexMarketDataDealHistInfo {
                    txid = p["txid"].ToString(),
                    fullHash = p["fullHash"].ToString(),
                    fullDomain = p["fullDomain"].ToString(),
                    displayName = displayName,
                    seller = seller,
                    buyer = p["buyer"].ToString(),
                    price = decimal.Parse(p["price"].ToString()).format(),
                    mortgagePayments = (long)p["mortgagePayments"],
                    assetHash = p["assetHash"].ToString(),
                    assetName = getAssetName(p["assetHash"].ToString()),
                    blockindex = index,
                    blocktime = time
                };
                mh.PutData(localConn.connStr, localConn.connDB, dexDomainDealHistStateCol, info);
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
        private long getBlockTime(long index)
        {
            string findStr = new JObject { { "index", index } }.ToString();
            string fieldStr = new JObject { { "time", 1} }.ToString();
            var queryRes = mh.GetDataWithField(blockConn.connStr, blockConn.connDB, "block", fieldStr, findStr);
            return (long)queryRes[0]["time"];
        }
        
        
        private void updateRecord(long height)
        {
            string newdata = new JObject() { { "counter", dexDomainDealHistStateCol }, { "lastBlockindex", height } }.ToString();
            string findstr = new JObject() { { "counter", dexDomainDealHistStateCol } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, dexDomainDealHistRecordCol, findstr);
            if (res == null || res.Count == 0)
            {
                mh.PutData(localConn.connStr, localConn.connDB, dexDomainDealHistRecordCol, newdata);
            }
            else
            {
                mh.ReplaceData(localConn.connStr, localConn.connDB, dexDomainDealHistRecordCol, newdata, findstr);
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
            string findStr = new JObject() { { "counter", dexDomainDealHistStateCol } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, dexDomainDealHistRecordCol, findStr);
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
    class DexMarketDataDealHistInfo
    {
        public ObjectId _id { get; set; }
        public string txid { get; set; }
        public string fullHash { get; set; }
        public string fullDomain { get; set; }
        public string displayName { get; set; }
        public string seller { get; set; }
        public string buyer { get; set; }
        public BsonDecimal128 price { get; set; }
        public string assetHash { get; set; }
        public string assetName { get; set; }
        public long mortgagePayments { get; set; }
        public long blockindex { get; set; }
        public long blocktime { get; set; }
    }

}
