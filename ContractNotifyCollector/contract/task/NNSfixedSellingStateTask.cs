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
    class NNSfixedSellingStateTask : ContractTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo localConn;
        private DbConnInfo remoteConn;
        private DbConnInfo blockConn;
        private string nnsFixedSellingRecordCol;
        private string nnsFixedSellingStateCol;
        private string remoteConnRecordColl;
        private string remoteConnStateColl;
        private int batchSize;
        private int batchInterval;
        private bool initSuccFlag = false;
        private bool hasCreateIndex = false;

        public NNSfixedSellingStateTask(string name): base(name)
        {
        }

        public override void initConfig(JObject config)
        {
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];
            nnsFixedSellingRecordCol = cfg["nnsFixedSellingRecordCol"].ToString();
            nnsFixedSellingStateCol = cfg["nnsFixedSellingStateCol"].ToString();
            remoteConnRecordColl = cfg["remoteConnRecordColl"].ToString();
            remoteConnStateColl = cfg["remoteConnStateColl"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());

            //
            localConn = Config.localDbConnInfo;
            remoteConn = Config.notifyDbConnInfo;
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
            // 获取远程已同步高度
            long remoteHeight = getRemoteHeight();
            long blockHeight = getBlockHeight();
            if (blockHeight < remoteHeight)
            {
                remoteHeight = blockHeight;
            }

            // 获取本地已处理高度
            long localHeight = getLocalHeight();

            if (remoteHeight <= localHeight) return;

            for (long startIndex = localHeight; startIndex <= remoteHeight; startIndex += batchSize)
            {
                long nextIndex = startIndex + batchSize;
                long endIndex = nextIndex < remoteHeight ? nextIndex : remoteHeight;
                string findStr = new JObject() {
                    { "blockindex", new JObject() { {"$gt", startIndex }, { "$lte", endIndex } } },
                    { "$or", new JArray{
                        new JObject() {{"displayName", "NNSfixedSellingLaunched" } },
                        new JObject() {{"displayName", "NNSfixedSellingDiscontinued" } },
                        new JObject() {{"displayName", "NNSfixedSellingBuy" } }
                                    }
                    }
                }.ToString();

                string fieldStr = new JObject() { {"state",0 }, { "_id", 0} }.ToString();
                var query = mh.GetDataWithField(remoteConn.connStr, remoteConn.connDB, remoteConnStateColl, fieldStr, findStr);
                if(query == null || query.Count == 0)
                {
                    updateRecord(endIndex);
                    log(endIndex, remoteHeight);
                    continue;
                }
                //
                var res = query.GroupBy(p => p["fullDomain"].ToString(), (k, g) =>
                {
                    return g.OrderByDescending(pg => long.Parse(pg["blockindex"].ToString())).ToArray()[0];
                }).ToList();

                // 
                var indexArr = res.Select(p => long.Parse(p["blockindex"].ToString())).Distinct().ToArray();
                var indexDict = getBlockTime(indexArr);

                //
                var namehashArr = res.Select(p => p["fullHash"].ToString()).Distinct().ToArray();
                var namehashDict = getDomainOwner(namehashArr);

                foreach(var pf in res) { 
                //res.ForEach(pf => {

                    JObject jo = (JObject)pf;

                    long blockindex = long.Parse(jo["blockindex"].ToString());
                    long blocktime = indexDict.GetValueOrDefault(blockindex);
                    jo.Add("lauchTime", blocktime);

                    string namehash = jo["fullHash"].ToString();
                    string owner = namehashDict.GetValueOrDefault(namehash)["owner"].ToString();
                    long ttl = long.Parse(namehashDict.GetValueOrDefault(namehash)["TTL"].ToString());
                    jo.Add("owner", owner);
                    jo.Add("TTL", ttl);
                    
                    string subfindStr = new JObject() { { "fullDomain", pf["fullDomain"] } }.ToString();
                    //string subfieldStr = new JObject() { { "blockindex", 1 } }.ToString();
                    var subquery = mh.GetData<NNSFixedSellingState>(localConn.connStr, localConn.connDB, nnsFixedSellingStateCol, subfindStr);
                    if (subquery == null || subquery.Count == 0)
                    {
                        mh.PutData(localConn.connStr, localConn.connDB, nnsFixedSellingStateCol, transferObject(jo));
                    }
                    else
                    {
                        if (long.Parse(pf["blockindex"].ToString()) >= subquery[0].blockindex)
                        {
                            var newdata = transferObject(jo);
                            newdata._id = subquery[0]._id;
                            mh.ReplaceData(localConn.connStr, localConn.connDB, nnsFixedSellingStateCol, newdata, subfindStr);
                        }
                    }
                    /*
                    JObject jo = (JObject)pf;
                    string subfindStr = new JObject() { { "fullDomain", pf["fullDomain"] } }.ToString();
                    string subfieldStr = new JObject() { { "blockindex",1} }.ToString();
                    var subquery = mh.GetDataWithField(localConn.connStr, localConn.connDB, nnsFixedSellingStateCol, subfieldStr, subfindStr);
                    if(subquery == null || subquery.Count == 0)
                    {
                        mh.PutData(localConn.connStr, localConn.connDB, nnsFixedSellingStateCol, ((JObject)pf).ToString());
                    } else
                    {
                        if(long.Parse(pf["blockindex"].ToString()) >= long.Parse(subquery[0]["blockindex"].ToString()))
                        {
                            mh.ReplaceData(localConn.connStr, localConn.connDB, nnsFixedSellingStateCol, ((JObject)pf).ToString(), subfindStr);
                        }
                    }
                    */
                    //});
                }
                //
                updateRecord(endIndex);
                log(endIndex, remoteHeight);
            }

            // 添加索引
            if (hasCreateIndex) return;
            mh.setIndex(localConn.connStr, localConn.connDB, nnsFixedSellingStateCol, "{'fullDomain':1}", "i_fullDomain");
            mh.setIndex(localConn.connStr, localConn.connDB, nnsFixedSellingStateCol, "{'seller':1}", "i_seller");
            mh.setIndex(localConn.connStr, localConn.connDB, nnsFixedSellingStateCol, "{'seller':1,'displayName':1}", "i_seller_displayName");
            mh.setIndex(localConn.connStr, localConn.connDB, nnsFixedSellingStateCol, "{'owner':1}", "i_owner");
            mh.setIndex(localConn.connStr, localConn.connDB, nnsFixedSellingStateCol, "{'owner':1,'displayName':1}", "i_owner_displayName");
            hasCreateIndex = true;
        }

        private Dictionary<string, JObject> getDomainOwner(string[] fullhashArr)
        {
            string findStr = MongoFieldHelper.toFilter(fullhashArr, "namehash").ToString();
            string fieldStr = new JObject() { { "namehash", 1 },{ "owner",1},{"TTL",1 } }.ToString();
            var query = mh.GetDataWithField(remoteConn.connStr, remoteConn.connDB, "domainOwnerCol", fieldStr, findStr);
            return query.ToDictionary(k => k["namehash"].ToString(), v=>(JObject)v);
        }
        private Dictionary<long, long> getBlockTime(long[] indexArr)
        {
            string findStr = MongoFieldHelper.toFilter(indexArr, "index").ToString();
            string fieldStr = new JObject() { {"index", 1 }, { "time", 1 } }.ToString();
            var query = mh.GetDataWithField(blockConn.connStr, blockConn.connDB, "block", fieldStr, findStr);
            return query.ToDictionary(k => long.Parse(k["index"].ToString()), v=>long.Parse(v["time"].ToString()));
        }

        private NNSFixedSellingState transferObject(JObject jo)
        {
            return new NNSFixedSellingState
            {
                blockindex = long.Parse(jo["blockindex"].ToString()),
                n = long.Parse(jo["n"].ToString()),
                vmstate = jo["vmstate"].ToString(),
                contractHash = jo["contractHash"].ToString(),
                displayName = jo["displayName"].ToString(),
                fullHash = jo["fullHash"].ToString(),
                fullDomain = jo["fullDomain"].ToString(),
                seller = jo["seller"].ToString(),
                price = BsonDecimalHelper.format(decimal.Parse(jo["price"].ToString(), System.Globalization.NumberStyles.Float)),
                launchTime = long.Parse(jo["lauchTime"].ToString()),
                owner = jo["owner"].ToString(),
                ttl = long.Parse(jo["TTL"].ToString()),
            };
        }

        [BsonIgnoreExtraElements]
        class NNSFixedSellingState
        {
            public ObjectId _id { get; set; }
            public long blockindex { get; set; }
            public long n { get; set; }
            public string vmstate { get; set; }
            public string contractHash { get; set; }
            public string displayName { get; set; }
            public string fullHash { get; set; }
            public string fullDomain { get; set; }
            public string seller { get; set; }
            public BsonDecimal128 price { get; set; }
            public long launchTime { get; set; }
            public string owner { get; set; }
            public long ttl { get; set; }

        }
        private void updateRecord(long height)
        {
            string newdata = new JObject() { { "counter", "nnsFixedSellingState" }, { "lastBlockindex", height } }.ToString();
            string findstr = new JObject() { { "counter", "nnsFixedSellingState" } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, nnsFixedSellingRecordCol, findstr);
            if (res == null || res.Count == 0)
            {
                mh.PutData(localConn.connStr, localConn.connDB, nnsFixedSellingRecordCol, newdata);
            }
            else
            {
                mh.ReplaceData(localConn.connStr, localConn.connDB, nnsFixedSellingRecordCol, newdata, findstr);
            }
        }
        private long getBlockHeight()
        {
            string findStr = new JObject() { { "counter", "block" } }.ToString();
            var query = mh.GetData(Config.blockDbConnInfo.connStr, Config.blockDbConnInfo.connDB, "system_counter", findStr);
            return long.Parse(query[0]["lastBlockindex"].ToString());
        }
        private long getRemoteHeight()
        {
            string findStr = new JObject() { { "counter", "notify" } }.ToString();
            JArray res = mh.GetData(remoteConn.connStr, remoteConn.connDB, remoteConnRecordColl, findStr);
            if (res == null || res.Count == 0)
            {
                return 0;
            }
            return long.Parse(res[0]["lastBlockindex"].ToString());
        }
        private long getLocalHeight()
        {
            string findStr = new JObject() { { "counter", "nnsFixedSellingState" } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, nnsFixedSellingRecordCol, findStr);
            if (res == null || res.Count == 0)
            {
                return 0;
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
}
