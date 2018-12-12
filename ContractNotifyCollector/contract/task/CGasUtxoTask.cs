using ContractNotifyCollector.core;
using ContractNotifyCollector.helper;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace ContractNotifyCollector.contract.task
{
    class CGasUtxoTask : ContractTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo remoteConn;
        private DbConnInfo localConn;

        private string utxoCol;
        private string utxoCounterCol;
        private string cgasUtxoCol;
        private string cgasUtxoCounterCol;
        private string cgasAddress;
        private int expireRange;
        private int batchSize;
        private int batchInterval;
        private bool hasInitSuccess = false;
        private bool hasCreateIndex = false;

        public CGasUtxoTask(string name) : base(name)
        {
        }

        public override void initConfig(JObject config)
        {
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];

            utxoCol = cfg["utxoCol"].ToString();
            utxoCounterCol = cfg["utxoCounterCol"].ToString();
            cgasUtxoCol = cfg["cgasUtxoCol"].ToString();
            cgasUtxoCounterCol = cfg["cgasUtxoCounterCol"].ToString();
            cgasAddress = cfg["cgasAddress"].ToString();
            expireRange = int.Parse(cfg["expireRange"].ToString());
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());
            
            localConn = Config.localDbConnInfo;
            remoteConn = Config.blockDbConnInfo;
            hasInitSuccess = true;
        }

        public override void startTask()
        {
            if (!hasInitSuccess) return;
            while(true)
            {
                try
                {
                    ping();
                    process();
                }
                catch (Exception ex)
                {
                    LogHelper.printEx(ex);
                    Thread.Sleep(10 * 000);
                    // continue
                }

            }
        }

        private void process()
        {
            // 
            updateState();

            // 获取远程已同步高度
            long remoteHeight = getRemoteHeight();

            // 获取本地已处理高度
            long localHeight = getLocalHeight();

            // 
            if (remoteHeight <= localHeight)
            {
                log(localHeight, remoteHeight);
                return ;
            }

            for (long index = localHeight; index <= remoteHeight; index += batchSize)
            {
                long nextIndex = index + batchSize;
                long endIndex = nextIndex < remoteHeight ? nextIndex : remoteHeight;

                // 待处理数据
                //string findstr = new JObject() { {"addr", cgasAddress },{ "createHeight", new JObject() { { "$gt", index }, { "$lte", endIndex } } } }.ToString();
                //string fieldstr = new JObject() { { "state", 0 } }.ToString();
                string findstr = new JObject() { { "blockindex", new JObject() { { "$gt", index }, { "$lte", endIndex } } }, { "type", new JObject() { { "$ne", "MinerTransaction" } } } }.ToString();
                string fieldstr = new JObject() { { "vin", 1 }, { "vout", 1 }, { "blockindex", 1 }, { "txid", 1 } }.ToString();
                JArray queryRes = mh.GetDataWithField(remoteConn.connStr, remoteConn.connDB, utxoCol, fieldstr, findstr);
                if (queryRes == null || queryRes.Count == 0)
                {
                    updateRecord(endIndex);
                    log(endIndex, remoteHeight);
                    continue;
                }

                //long[] blockindexArr = queryRes.Select(p => long.Parse(p["createHeight"].ToString())).Distinct().ToArray();
                long[] blockindexArr = queryRes.Select(p => long.Parse(p["blockindex"].ToString())).Distinct().OrderBy(p => p).ToArray();

                foreach (long blockindex in blockindexArr)
                {
                    //JToken[] jtArr = queryRes.Where(p => long.Parse(p["createHeight"].ToString()) == blockindex).ToArray();
                    JToken[] jtArr = queryRes.Where(p => long.Parse(p["blockindex"].ToString()) == blockindex).ToArray();
                    foreach (JToken jt in jtArr)
                    {
                        if (jt["vin"] != null)
                        {
                            foreach (JToken jvin in (JArray)jt["vin"])
                            {
                                string prevTxid = jvin["txid"].ToString();
                                long prevIndex = long.Parse(jvin["vout"].ToString());
                                findstr = new JObject() { { "txid", prevTxid }, { "n", prevIndex } }.ToString();
                                if (mh.GetDataCount(localConn.connStr, localConn.connDB, cgasUtxoCol, findstr) > 0)
                                {
                                    mh.DeleteData(localConn.connStr, localConn.connDB, cgasUtxoCol, findstr);
                                }
                            }
                        }
                        if (jt["vout"] != null)
                        {
                            foreach (JToken jvout in (JArray)jt["vout"])
                            {
                                if (jvout["address"].ToString() != cgasAddress) continue;
                                string newData = new JObject()
                                    {
                                        {"addr", jvout["address"] },
                                        {"txid", jt["txid"] },
                                        {"n", jvout["n"] },
                                        {"asset", jvout["asset"] },
                                        {"value", jvout["value"] },
                                        {"createHeight", blockindex },
                                        {"markAddress", "0" },
                                        {"markTime", 0 },
                                        {"lockAddress", "0" },
                                        {"lockHeight", 0 },
                                    }.ToString();
                                findstr = new JObject() { { "txid", jt["txid"] }, { "n", jvout["n"] } }.ToString();
                                if (mh.GetDataCount(localConn.connStr, localConn.connDB, cgasUtxoCol, findstr) <= 0)
                                {
                                    mh.PutData(localConn.connStr, localConn.connDB, cgasUtxoCol, newData);
                                }
                            }
                        }
                    }

                    //
                    updateRecord(blockindex);
                    log(blockindex, remoteHeight);

                }

            }


            // 添加索引
            if (hasCreateIndex) return;
            mh.setIndex(localConn.connStr, localConn.connDB, cgasUtxoCol, "{'txid':1,'n':1}", "i_txid_n");
            mh.setIndex(localConn.connStr, localConn.connDB, cgasUtxoCol, "{'markAddress':1}", "i_markAddress");
            mh.setIndex(localConn.connStr, localConn.connDB, cgasUtxoCol, "{'markAddress':1, 'lockAddress':1}", "i_markAddress_lockAddress");
            hasCreateIndex = true;
        }
        private void updateState()
        {
            long remoteHeight = getRemoteHeight();
            long nowtime = TimeHelper.GetTimeStamp();
            string findstr = new JObject()
            {
                {"addr", cgasAddress },
                {"markAddress", new JObject(){ {"$ne","0" } } },
                {"markTime", new JObject(){ {"$lt", nowtime - expireRange } } },
            }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, cgasUtxoCol, findstr);
            if (res == null || res.Count == 0) return;
            foreach (JObject jo in res)
            {
                JObject newJo = (JObject)jo.DeepClone();
                newJo.Remove("markAddress");
                newJo.Add("markAddress", "0");
                newJo.Remove("markTime");
                newJo.Add("markTime", 0);
                mh.ReplaceData(localConn.connStr, localConn.connDB, cgasUtxoCol, newJo.ToString(), jo.ToString());
            }
        }
        private void updateRecord(long blockindex)
        {
            string newdata = new JObject() { { "counter", "tx" }, { "lastBlockindex", blockindex } }.ToString();
            string findstr = new JObject() { { "counter", "tx" } }.ToString();
            long count = mh.GetDataCount(localConn.connStr, localConn.connDB, cgasUtxoCounterCol, findstr);
            if (count <= 0)
            {
                mh.PutData(localConn.connStr, localConn.connDB, cgasUtxoCounterCol, newdata);
            } else
            {
                mh.ReplaceData(localConn.connStr, localConn.connDB, cgasUtxoCounterCol, newdata, findstr);
            }
        }
        private long getRemoteHeight()
        {
            string findstr = new JObject() { { "counter", "block" } }.ToString();
            JArray res = mh.GetData(remoteConn.connStr, remoteConn.connDB, utxoCounterCol, findstr);
            if (res == null || res.Count == 0)
            {
                return 0;
            }
            return long.Parse(res[0]["lastBlockindex"].ToString());
        }
        private long getLocalHeight()
        {
            string findstr = new JObject() { { "counter", "tx" } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, cgasUtxoCounterCol, findstr);
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
