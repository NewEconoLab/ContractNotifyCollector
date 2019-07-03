using ContractNotifyCollector.core;
using ContractNotifyCollector.helper;
using Newtonsoft.Json.Linq;
using System;
using System.Threading;

namespace ContractNotifyCollector.contract.task
{
    class ContractCallTask : ContractTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo remoteConn;
        private DbConnInfo localConn;

        private string nelApiUrl;
        private int batchInterval = 2000;
        private bool hasInitSuccess = false;
        public ContractCallTask(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            nelApiUrl = Config.nelApiUrl;
            remoteConn = Config.analyDbConnInfo;
            localConn = Config.notifyDbConnInfo;
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
                var queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, "contract_call_info", findStr);
                if (queryRes == null || queryRes.Count == 0)
                {
                    updateHeight(index);
                    log(index, rh);
                    continue;
                }
                //
                foreach (var item in queryRes)
                {
                    string contractHash = item["contractHash"].ToString();
                    findStr = new JObject { { "hash", contractHash } }.ToString();
                    if (mh.GetDataCount(localConn.connStr, localConn.connDB, "contractCallState", findStr) > 0) continue;

                    //
                    var res = HttpHelper.Post(nelApiUrl, "getcontractstate", new JArray { contractHash });
                    var result = JObject.Parse(res)["result"];
                    if (result != null && ((JArray)result).Count > 0)
                    {
                        mh.PutData(localConn.connStr, localConn.connDB, "contractCallState", result[0].ToString());
                    }
                }
                //
                updateHeight(index);
                log(index, rh);
            }

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
