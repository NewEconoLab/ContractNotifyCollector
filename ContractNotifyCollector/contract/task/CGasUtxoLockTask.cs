using ContractNotifyCollector.core;
using ContractNotifyCollector.helper;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;
using System.Threading;

namespace ContractNotifyCollector.contract.task
{
    class CGasUtxoLockTask : ContractTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo remoteConn;
        private DbConnInfo localConn;

        private string cgasRefundCol;
        private string cgasRefundCounterCol;
        private string cgasUtxoCol;
        private string cgasUtxoCounterCol;
        private int batchSize;
        private int batchInterval;
        private bool hasInitSuccess = false;

        public CGasUtxoLockTask(string name) : base(name)
        {
        }

        public override void initConfig(JObject config)
        {
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];

            cgasRefundCol = cfg["cgasRefundCol"].ToString();
            cgasRefundCounterCol = cfg["cgasRefundCounterCol"].ToString();
            cgasUtxoCol = cfg["cgasUtxoCol"].ToString();
            cgasUtxoCounterCol = cfg["cgasUtxoCounterCol"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());
            //
            localConn = Config.localDbConnInfo;
            remoteConn = Config.notifyDbConnInfo;
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
            // 获取远程已同步高度
            long remoteHeight = getRemoteHeight();

            // 获取本地已处理高度
            long localHeight = getLocalHeight();

            // 
            long utxoHeight = getUtxoHeight();
            if (remoteHeight > utxoHeight) remoteHeight = utxoHeight;

            if (remoteHeight <= localHeight)
            {
                log(localHeight, remoteHeight);
                return;
            }

            for (long index = localHeight; index <= remoteHeight; index += batchSize)
            {
                long nextIndex = index + batchSize;
                long endIndex = nextIndex < remoteHeight ? nextIndex : remoteHeight;

                string findstr = new JObject() { { "blockindex", new JObject() { { "$gt", index }, { "$lte", endIndex } } }, { "displayName", "refund" } }.ToString();
                string fieldstr = new JObject() { { "_txid", 1 }, { "who", 1 }, { "blockindex", 1 } }.ToString();
                JArray queryRes = mh.GetDataWithField(remoteConn.connStr, remoteConn.connDB, cgasRefundCol, fieldstr, findstr);
                if (queryRes == null || queryRes.Count == 0)
                {
                    updateRecord(endIndex);
                    log(endIndex, remoteHeight);
                    continue;
                }

                long[] blockindexArr = queryRes.Select(p => long.Parse(p["blockindex"].ToString())).Distinct().OrderBy(p => p).ToArray();
                foreach (long blockindex in blockindexArr)
                {
                    JToken[] jtArr = queryRes.Where(p => long.Parse(p["blockindex"].ToString()) == blockindex).ToArray();
                    foreach (JToken jt in jtArr)
                    {
                        string _txid = jt["_txid"].ToString();
                        string who = jt["who"].ToString();
                        findstr = new JObject() { { "txid", _txid }, { "n", 0 } }.ToString();
                        if (mh.GetDataCount(localConn.connStr, localConn.connDB, cgasUtxoCol, findstr) <= 0)
                        {
                            continue;
                        };
                        JArray rr = mh.GetData(localConn.connStr, localConn.connDB, cgasUtxoCol, findstr);
                        if (rr == null || rr.Count == 0) continue;
                        JObject jo = (JObject)rr[0];
                        jo.Remove("lockAddress");
                        jo.Add("lockAddress", who);
                        jo.Remove("lockHeight");
                        jo.Add("lockHeight", blockindex);

                        mh.ReplaceData(localConn.connStr, localConn.connDB, cgasUtxoCol, jo.ToString(), findstr);

                    }
                    updateRecord(blockindex);
                    log(blockindex, remoteHeight);
                }
            }
        }


        private void updateRecord(long blockindex)
        {
            string newdata = new JObject() { { "counter", "markTx" }, { "lastBlockindex", blockindex } }.ToString();
            string findstr = new JObject() { { "counter", "markTx" } }.ToString();
            long count = mh.GetDataCount(localConn.connStr, localConn.connDB, cgasUtxoCounterCol, findstr);
            if (count <= 0)
            {
                mh.PutData(localConn.connStr, localConn.connDB, cgasUtxoCounterCol, newdata);
            }
            else
            {
                mh.ReplaceData(localConn.connStr, localConn.connDB, cgasUtxoCounterCol, newdata, findstr);
            }
        }
        private long getUtxoHeight()
        {
            string findstr = new JObject() { { "counter", "tx" } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, cgasUtxoCounterCol, findstr);
            if (res == null || res.Count == 0)
            {
                return 0;
            }
            return long.Parse(res[0]["lastBlockindex"].ToString());
        }
        private long getLocalHeight()
        {
            string findstr = new JObject() { { "counter", "markTx" } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, cgasUtxoCounterCol, findstr);
            if (res == null || res.Count == 0)
            {
                return 0;
            }
            return long.Parse(res[0]["lastBlockindex"].ToString());
        }
        private long getRemoteHeight()
        {
            string findstr = new JObject() { { "counter", "notify" } }.ToString();
            JArray res = mh.GetData(remoteConn.connStr, remoteConn.connDB, cgasRefundCounterCol, findstr);
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
