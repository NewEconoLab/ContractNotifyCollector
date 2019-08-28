using ContractNotifyCollector.core;
using ContractNotifyCollector.helper;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace ContractNotifyCollector.contract.task
{
    class ContractCallTask : ContractTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo remoteConn;
        private DbConnInfo localConn;
        private DbConnInfo blockConn;
        private string nelApiUrl;
        
        private string rColl = "contract_call_info";
        private string rcColl = "contract_create_info";
        private string lColl = "contractCallState";
        private int batchInterval = 2000;
        private bool hasInitSuccess = false;
        public ContractCallTask(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            nelApiUrl = Config.nelApiUrl;
            remoteConn = Config.analyDbConnInfo;
            localConn = Config.localDbConnInfo;
            blockConn = Config.blockDbConnInfo;
            hasInitSuccess = true;
        }

        public override void startTask()
        {
            if (!hasInitSuccess) return;
            while (true)
            {
                try
                {
                    ping();
                    process();
                    processCreate();
                    process24h();
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
            for (long index = lh + 1; index <= rh; ++index)
            {
                string findStr = new JObject { { "blockIndex", index } }.ToString();
                var queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, rColl, findStr);
                if (queryRes == null || queryRes.Count == 0)
                {
                    updateHeight(index);
                    log(index, rh);
                    continue;
                }
                var hashs = queryRes.Select(p => p["contractHash"].ToString()).Distinct().ToArray();
                foreach (var hash in hashs)
                {
                    findStr = new JObject { { "hash", hash } }.ToString();
                    if (mh.GetDataCount(localConn.connStr, localConn.connDB, lColl, findStr) > 0)
                    {
                        //
                        var txCount = getTxCount(hash);
                        var usrCount = getUsrCount(hash);
                        if (txCount == 0 && usrCount == 0) continue; 
                        var updateStr = new JObject { { "$set", new JObject { { "txCount", txCount},{ "usrCount", usrCount} } } }.ToString();
                        mh.UpdateData(localConn.connStr, localConn.connDB, lColl, updateStr, findStr);
                        continue;
                    }

                    //
                    var res = HttpHelper.Post(nelApiUrl, "getcontractstate", new JArray { hash });
                    var result = JObject.Parse(res)["result"];
                    if (result != null && ((JArray)result).Count > 0)
                    {
                        var data = (JObject)result[0];
                        data.Add("createIndex", 0);
                        data.Add("createDate", 0);
                        data.Add("txCount", getTxCount(hash));
                        data.Add("txCount24h", 1);
                        data.Add("usrCount", getUsrCount(hash));
                        data.Add("usrCount24h", 1);
                        data.Add("lastCalcTime", 0);
                        mh.PutData(localConn.connStr, localConn.connDB, lColl, result[0].ToString());
                    }
                }
                //
                updateHeight(index);
                log(index, rh);
            }
        }
        private void processCreate()
        {
            string findStr = "{'createDate':0}";
            string sortStr = "{}";
            string fieldStr = "{'hash':1}";
            var queryRes = mh.GetDataPagesWithField(localConn.connStr, localConn.connDB, lColl, fieldStr, 100, 1, sortStr, findStr);
            if (queryRes == null || queryRes.Count == 0) return;

            string subfindStr = null;
            JArray subRes = null;
            string subupdateStr = null;
            foreach(var item in queryRes)
            {
                subfindStr = "{'contractHash':'"+item["hash"]+"'}";
                subRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, rcColl, subfindStr);
                if (subRes == null || subRes.Count == 0) continue;

                subfindStr = "{'hash':'" + item["hash"] + "'}";
                subupdateStr = new JObject { { "$set", new JObject { { "createDate", (long)subRes[0]["time"] },{ "createIndex", (long)subRes[0]["blockIndex"]} } } }.ToString();
                mh.UpdateData(localConn.connStr, localConn.connDB, lColl, subupdateStr, subfindStr);
            }
        }

        private int lastCalcHour = -1;
        private void process24h()
        {
            int hour = DateTime.Now.Hour;
            if (hour == lastCalcHour) return;

            long nowtime = TimeHelper.GetTimeStamp();
            long timeLimit = nowtime - TimeHelper.OneHourSecons;
            string findStr = new JObject { { "lastCalcTime", new JObject { { "$lt", timeLimit } } } }.ToString();
            string sortStr = "{}";
            string fieldStr = "{'hash':1}";
            
            while(true)
            {
                var queryRes = mh.GetDataPagesWithField(localConn.connStr, localConn.connDB, lColl, fieldStr, 100, 1, sortStr, findStr);
                if (queryRes == null || queryRes.Count == 0) break;
                var indexBefore24h = getBlockHeightBefore24h();
                foreach(var item in queryRes)
                {
                    string hash = item["hash"].ToString();
                    var txCount24h = getTxCount(hash, true, indexBefore24h);
                    var usrCount24h = getUsrCount(hash, true, indexBefore24h);
                    var subfindstr = new JObject { {"hash",hash } }.ToString();
                    var subupdatestr = new JObject { {"$set", new JObject { { "txCount24h", txCount24h},{ "usrCount24h",usrCount24h},{ "lastCalcTime", nowtime } } } }.ToString();
                    mh.UpdateData(localConn.connStr, localConn.connDB, lColl, subupdatestr, subfindstr);
                }
            }
            lastCalcHour = hour;
        }
        private long getTxCount(string hash, bool isOnly24h=false, long indexBefore24h=0)
        {
            var findJo = new JObject { { "contractHash", hash } };
            if(isOnly24h)
            {
                findJo.Add("blockIndex", new JObject { { "$gte", indexBefore24h } });
            }
            return mh.GetDataCount(remoteConn.connStr, remoteConn.connDB, "contract_call_info", findJo.ToString());
        }
        private long getUsrCount(string hash, bool isOnly24h=false, long indexBefore24h = 0)
        {
            var findJo = new JObject { { "contractHash", hash } };
            if (isOnly24h)
            {
                findJo.Add("blockIndex", new JObject { { "$gte", indexBefore24h } });
            }
            var list = new List<string>();
            list.Add(new JObject { { "$match", findJo} }.ToString());
            list.Add(new JObject { { "$group", new JObject { { "_id", "$address" }, { "sum", new JObject { { "$sum", 1 } } } } } }.ToString());
            list.Add(new JObject { { "$group", new JObject { { "_id", "$id" }, { "sum", new JObject { { "$sum", 1 } } } } } }.ToString());
            return mh.AggregateCount(remoteConn.connStr, remoteConn.connDB, "contract_call_info", list, false);
        }
        private long getBlockHeightBefore24h()
        {
            long now = TimeHelper.GetTimeStamp();
            long bfr = now - 24 * 60 * 60;
            string findStr = new JObject { { "time", new JObject { { "$gte", bfr } } } }.ToString();
            string sortStr = new JObject { { "time", 1 } }.ToString();
            string fieldStr = new JObject { { "index", 1 } }.ToString();
            var queryRes = mh.GetDataPagesWithField(blockConn.connStr, blockConn.connDB, "block", fieldStr, 1, 1, sortStr, findStr);
            if (queryRes == null || queryRes.Count == 0) return 0;
            return (long)queryRes[0]["index"];
        }


        private void updateHeight(long height)
        {
            string findStr = new JObject { { "counter", "contractCallState" } }.ToString();
            string newdata = new JObject { { "counter", "contractCallState" }, { "lastBlockindex", height } }.ToString();
            var queryRes = mh.GetData(localConn.connStr, localConn.connDB, "contractCallRecord", findStr);
            if(queryRes == null || queryRes.Count == 0)
            {
                mh.PutData(localConn.connStr, localConn.connDB, "contractCallRecord", newdata);
            } else
            {
                mh.ReplaceData(localConn.connStr, localConn.connDB, "contractCallRecord", newdata, findStr);
            }
        }
        private long getLocalHeight()
        {
            string findStr = new JObject { { "counter", "contractCallState" } }.ToString();
            var queryRes = mh.GetData(localConn.connStr, localConn.connDB, "contractCallRecord", findStr);
            if (queryRes != null && queryRes.Count > 0)
            {
                return (long)queryRes[0]["lastBlockindex"];
            }
            return -1;
        }
        private long getRemoteHeight()
        {
            string findStr = new JObject { {"counter", "dumpInfos"} }.ToString();
            var queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, "system_counter", findStr);
            if(queryRes != null && queryRes.Count > 0)
            {
                return (long)queryRes[0]["lastBlockindex"];
            }
            return -1;
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
