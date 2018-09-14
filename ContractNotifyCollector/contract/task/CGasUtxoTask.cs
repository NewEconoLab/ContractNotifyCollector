using ContractNotifyCollector.core;
using ContractNotifyCollector.helper;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ContractNotifyCollector.contract.task
{
    class CGasUtxoTask : ContractTask
    {
        private JObject config;
        public CGasUtxoTask(string name) : base(name)
        {
        }

        public override void initConfig(JObject config)
        {
            this.config = config;
            initConfig();
        }

        public override void startTask()
        {
            run();
        }

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
        private void initConfig()
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

            remoteConn = Config.blockDbConnInfo;
            localConn = Config.notifyDbConnInfo;
            // 
            hasInitSuccess = true;
        }
        private void run()
        {
            if (!hasInitSuccess) return;
            while(true)
            {
                //
                ping();
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
                    continue;
                }

                for (long index = localHeight; index <= remoteHeight; index += batchSize)
                {
                    long nextIndex = index + batchSize;
                    long endIndex = nextIndex < remoteHeight ? nextIndex : remoteHeight;

                    // 待处理数据
                    string findstr = new JObject() { {"addr", cgasAddress },{ "createHeight", new JObject() { { "$gt", index }, { "$lte", endIndex } } } }.ToString();
                    string fieldstr = new JObject() { { "state", 0 } }.ToString();
                    JArray queryRes = mh.GetDataWithField(remoteConn.connStr, remoteConn.connDB, utxoCol, fieldstr, findstr);
                    if(queryRes == null || queryRes.Count == 0)
                    {
                        updateRecord(endIndex);
                        log(endIndex, remoteHeight);
                        continue;
                    }

                    long[] blockindexArr = queryRes.Select(p => long.Parse(p["createHeight"].ToString())).Distinct().ToArray();
                    
                    foreach (long blockindex in blockindexArr)
                    {
                        JToken[] jtArr = queryRes.Where(p => long.Parse(p["createHeight"].ToString()) == blockindex).ToArray();
                        foreach (JToken jt in jtArr)
                        {
                            string used = jt["used"].ToString();
                            string useHeight = jt["useHeight"].ToString();
                            if(used == string.Empty || useHeight == "-1")
                            {
                                string newData = new JObject()
                                {
                                    {"addr", jt["addr"] },
                                    {"txid", jt["txid"] },
                                    {"n", jt["n"] },
                                    {"asset", jt["asset"] },
                                    {"value", jt["value"] },
                                    {"createHeight", jt["createHeight"] },
                                    {"markAddress", "0" },
                                }.ToString();
                                mh.PutData(localConn.connStr, localConn.connDB, cgasUtxoCol, newData);

                            } else
                            {
                                string findStr = new JObject()
                                {
                                    {"txid", jt["txid"] },
                                    {"n", jt["n"] },
                                }.ToString();
                                mh.DeleteData(localConn.connStr, localConn.connDB, cgasUtxoCol, findStr);
                            }
                        }

                        //
                        updateRecord(blockindex);
                        log(blockindex, remoteHeight);

                    }
                    
                }
            }
        }
        private void updateState()
        {
            long remoteHeight = getRemoteHeight();
            string findstr = new JObject()
            {
                {"addr", cgasAddress },
                {"markAddress", new JObject(){ {"$ne","0" } } },
                {"createHeight", new JObject(){ {"$lt",remoteHeight- expireRange } } },
            }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, cgasUtxoCol, findstr);
            if (res == null || res.Count == 0) return;
            foreach (JObject jo in res)
            {
                JObject newJo = (JObject)jo.DeepClone();
                newJo.Remove("markAddress");
                newJo.Add("markAddress", "0");
                mh.ReplaceData(localConn.connStr, localConn.connDB, cgasUtxoCol, newJo.ToString(), jo.ToString());
            }
        }
        private void updateRecord(long blockindex)
        {
            string newdata = new JObject() { { "counter", "utxo" }, { "lastBlockindex", blockindex } }.ToString();
            string findstr = new JObject() { { "counter", "utxo" } }.ToString();
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
            string findstr = new JObject() { { "counter", "utxo" } }.ToString();
            JArray res = mh.GetData(remoteConn.connStr, remoteConn.connDB, utxoCounterCol, findstr);
            if (res == null || res.Count == 0)
            {
                return 0;
            }
            return long.Parse(res[0]["lastBlockindex"].ToString());
        }
        private long getLocalHeight()
        {
            string findstr = new JObject() { { "counter", "utxo" } }.ToString();
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
