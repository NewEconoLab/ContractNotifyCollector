using ContractNotifyCollector.core;
using ContractNotifyCollector.helper;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace ContractNotifyCollector.contract.task
{
    class SwapUniTask : ContractTask
    {
        private MongoDBHelper mh = new MongoDBHelper();

        //private string remoteRecord;
        //private string remoteState;
        private string localRecord = "swapRecord";
        private string localState = "swapUniUserState";
        private string localPoolState = "swapUniPoolState";
        private int batchInterval = 2000;
        private DbConnInfo remoteConn;
        private DbConnInfo localConn;
        private bool initSuccFlag = false;
        private bool hasCreateIndex = false;

        public SwapUniTask(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];

            //remoteRecord = cfg["remoteRecordCol"].ToString();
            //remoteState = cfg["remoteStateCol"].ToString();
            batchInterval = int.Parse(cfg["batchInterval"].ToString());

            remoteConn = Config.notifyDbConnInfo;
            localConn = Config.notifyDbConnInfo;
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
            long rh = getR();
            long lh = getL();
            if(lh >= rh)
            {
                log(lh, rh);
                return;
            }

            var hashs = getPoolHashArr();
            for (long index=lh+1; index <= rh; ++index)
            {
                string findStr = new JObject { { "blockindex", index }, { "$or", new JArray {
                    new JObject { { "displayName", "addLiquidity" } },
                    new JObject { { "displayName", "removeLiquidity" } }
                } } }.ToString();

                foreach(var hash in hashs)
                {
                    var queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, hash, findStr);
                    if (queryRes.Count > 0)
                    {
                        // 用户
                        handleUserInfo(queryRes, index, out List<UniUserInfo> res);

                        // 总额
                        handlePoolInfo(res, index);
                    }
                }
                //
                updateL(index);
                log(index, rh);
            }
            if (hasCreateIndex) return;
            mh.setIndex(localConn.connStr, localConn.connDB, localState, "{'address':1,'contractHash':1}", "i_address_contractHash");
            mh.setIndex(localConn.connStr, localConn.connDB, localState, "{'contractHash':1}", "i_contractHash");
            hasCreateIndex = true;
        }

        private string[] getPoolHashArr()
        {
            string findStr = new JObject { {"contractHash", new JObject { { "$ne", ""} } } }.ToString();
            string fieldStr = new JObject { { "contractHash", 1 }, { "_id", 0 } }.ToString();
            var queryRes = mh.GetDataWithField(remoteConn.connStr, remoteConn.connDB, "swapPoolInfo", fieldStr, findStr);
            if (queryRes.Count == 0) return null;
            return queryRes.Select(p => p["contractHash"].ToString()).Distinct().ToArray();
        }
        private void handleUserInfo(JArray queryRes, long index, out List<UniUserInfo> res)
        {
            res =
                queryRes.GroupBy(p => p["contractHash"].ToString(), (k, g) =>
                {
                    return g.GroupBy(pg => pg["who"], (kg, gg) =>
                    {
                        return new UniUserInfo
                        {
                            contractHash = k.ToString(),
                            address = kg.ToString(),
                            uniMinted = gg.Sum(pgg =>
                            {
                                string displayName = pgg["displayName"].ToString();
                                string uniMinted = pgg["uniMinted"].ToString();
                                int signum = 0;
                                if (displayName == "addLiquidity") signum = 1;
                                if (displayName == "removeLiquidity") signum = -1;
                                return signum * decimal.Parse(uniMinted);
                            }).format(),
                            blockindex = index
                        };
                    }).ToList();
                }).SelectMany(ps => ps).ToList();

            //
            foreach (var item in res)
            {
                string substr = new JObject {
                        { "address", item.address},
                        { "contractHash", item.contractHash}
                    }.ToString();
                var subres = mh.GetData<UniUserInfo>(localConn.connStr, localConn.connDB, localState, substr);
                if (subres.Count == 0)
                {
                    // insert
                    mh.PutData<UniUserInfo>(localConn.connStr, localConn.connDB, localState, item);
                    continue;
                }
                if (subres[0].blockindex < index)
                {
                    // replace
                    item._id = subres[0]._id;
                    item.uniMinted = (item.uniMinted.format() + subres[0].uniMinted.format()).format();
                    mh.ReplaceData<UniUserInfo>(localConn.connStr, localConn.connDB, localState, item, substr);
                    continue;
                }

            }
        }
        private void handlePoolInfo(List<UniUserInfo> res, long index)
        {
            var sum =
                res.GroupBy(p => p.contractHash, (k, g) =>
                {
                    return new UniPoolInfo
                    {
                        contractHash = k.ToString(),
                        uniMinted = g.Sum(pg => pg.uniMinted.format()).format(),
                        blockindex = index
                    };
                }).ToList();
            foreach (var item in sum)
            {
                string substr = new JObject { { "contractHash", item.contractHash } }.ToString();
                var subres = mh.GetData<UniPoolInfo>(localConn.connStr, localConn.connDB, localPoolState, substr);
                if (subres.Count == 0)
                {
                    //insert
                    mh.PutData<UniPoolInfo>(localConn.connStr, localConn.connDB, localPoolState, item);
                    continue;
                }
                if (subres[0].blockindex < index)
                {
                    // replace
                    item._id = subres[0]._id;
                    item.uniMinted = (item.uniMinted.format() + subres[0].uniMinted.format()).format();
                    mh.ReplaceData<UniPoolInfo>(localConn.connStr, localConn.connDB, localPoolState, item, substr);
                    continue;
                }
            }
        }

        class UniPo
        {
            public string hash { get; set; }
            public string addr { get; set; }
            public decimal val { get; set; }
        }
        
        [BsonIgnoreExtraElements]
        class UniUserInfo
        {
            public ObjectId _id { get; set; }
            public string address { get; set; }
            public string contractHash { get; set; }
            public BsonDecimal128 uniMinted { get; set; }
            public long blockindex { get; set; }
        }
        [BsonIgnoreExtraElements]
        class UniPoolInfo
        {
            public ObjectId _id { get; set; }
            public string contractHash { get; set; }
            public BsonDecimal128 uniMinted { get; set; }
            public long blockindex { get; set; }
        }


        private void updateL(long index)
        {
            string findStr = new JObject { { "counter", localState } }.ToString();
            string newdata = new JObject { { "counter", localState }, { "lastBlockindex", index } }.ToString();
            if(mh.GetDataCount(localConn.connStr, localConn.connDB, localRecord, findStr) == 0)
            {
                mh.PutData(localConn.connStr, localConn.connDB, localRecord, newdata);
            } else
            {
                mh.ReplaceData(localConn.connStr, localConn.connDB, localRecord, newdata, findStr);
            }
        }
        private long getL()
        {
            string findStr = new JObject { { "counter", localState } }.ToString();
            var queryRes = mh.GetData(localConn.connStr, localConn.connDB, localRecord, findStr);
            if (queryRes.Count == 0) return -1;
            return long.Parse(queryRes[0]["lastBlockindex"].ToString());
        }
        private long getR()
        {
            string findStr = new JObject { { "counter", "swapPoolInfo" } }.ToString();
            var queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, "swapRecord", findStr);
            if (queryRes.Count == 0) return -1;
            return long.Parse(queryRes[0]["lastBlockindex"].ToString());
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
