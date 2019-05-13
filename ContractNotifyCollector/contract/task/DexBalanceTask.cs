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
    class DexBalanceTask : ContractTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private string remoteConnRecordCol;
        private string remoteConnStateCol;
        private string dexBalanceRecordCol;
        private string dexBalanceStateCol;
        private int batchInterval;
        private int batchSize;

        private DbConnInfo localConn;
        private DbConnInfo remoteConn;
        private bool initSuccFlag = false;
        private bool hasCreateIndex = false;

        public DexBalanceTask(string name):base(name)
        {
        }

        public override void initConfig(JObject config)
        {
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];
            remoteConnRecordCol = cfg["remoteConnRecordCol"].ToString();
            remoteConnStateCol = cfg["remoteConnStateCol"].ToString();
            dexBalanceRecordCol = cfg["dexBalanceRecordCol"].ToString();
            dexBalanceStateCol = cfg["dexBalanceStateCol"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());

            //
            localConn = Config.localDbConnInfo;
            remoteConn = Config.remoteDbConnInfo;
            initSuccFlag = true;
        }

        public override void startTask()
        {
            if (!initSuccFlag) return;
            reset();
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
                    Thread.Sleep(10 * 000);
                    // continue
                }
            }
        }
        private void process()
        {
            long rh = getRemoteHeight();
            long lh = getLocalHeight();
            if(lh >= rh)
            {
                log(lh, rh);
                return;
            }
            for(long index=lh+1; index <=rh; ++index)
            {
                string findStr = new JObject() { { "blockindex", index},{ "displayName", "dexTransfer" } }.ToString();
                var queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, remoteConnStateCol, findStr);
                if (queryRes == null || queryRes.Count == 0)
                {
                    updateRecord(index);
                    log(index, rh);
                    continue;
                }
                //
                queryRes.GroupBy(p => p["from"].ToString(), (k, g) => {
                    return new
                    {
                        addr = k.ToString(),
                        value = g.Sum(pg => decimal.Parse(pg["value"].ToString()))
                    };
                }).Where(p => p.addr != "").ToList().ForEach(p => {
                    subBalance(p.addr, remoteConnStateCol, p.value);
                });
                //
                queryRes.GroupBy(p => p["to"].ToString(), (k, g) => {
                    return new
                    {
                        addr = k.ToString(),
                        value = g.Sum(pg => decimal.Parse(pg["value"].ToString()))
                    };
                }).Where(p => p.addr != "").ToList().ForEach(p => {
                    addBalance(p.addr, remoteConnStateCol, p.value);
                });
                //
                confirm();
                updateRecord(index);
                log(index, rh);
            }

            // 添加索引
            if (hasCreateIndex) return;
            mh.setIndex(localConn.connStr, localConn.connDB, dexBalanceStateCol, "{'address':1}", "i_address");
            mh.setIndex(localConn.connStr, localConn.connDB, dexBalanceStateCol, "{'address':1,'contractHash':1}", "i_address_contractHash");
            hasCreateIndex = true;
        }

        [BsonIgnoreExtraElements]
        private class DexBalanceBody
        {
            public ObjectId _id { get; set; }
            public string address { get; set; }
            public string contractHash { get; set; }
            //public string balance { get; set; }
            public BsonDecimal128 balance { get; set; }
            //public string curvalue { get; set; }
            public BsonDecimal128 curvalue { get; set; }
        }
        private void addBalance(string address, string contractHash, decimal value)
        {
            DexBalanceBody data = null;
            string findstr = new JObject() { { "address", address }, { "contractHash", contractHash } }.ToString();
            List<DexBalanceBody> res = mh.GetData<DexBalanceBody>(localConn.connStr, localConn.connDB, dexBalanceStateCol, findstr);
            if (res == null || res.Count == 0)
            {
                // insert
                data = new DexBalanceBody
                {
                    address = address,
                    contractHash = contractHash,
                    balance = format(value),
                    curvalue = format(value)
                };
                mh.PutData<DexBalanceBody>(localConn.connStr, localConn.connDB, dexBalanceStateCol, data);
            }
            else
            {
                // update or replace
                data = res[0];
                data.balance = format(format(data.balance) + value);
                data.curvalue = format(format(data.curvalue) + value);
                mh.ReplaceData<DexBalanceBody>(localConn.connStr, localConn.connDB, dexBalanceStateCol, data, findstr);
            }
        }
        private void subBalance(string address, string contractHash, decimal value)
        {
            DexBalanceBody data = null;
            string findstr = new JObject() { { "address", address }, { "contractHash", contractHash } }.ToString();
            List<DexBalanceBody> res = mh.GetData<DexBalanceBody>(localConn.connStr, localConn.connDB, dexBalanceStateCol, findstr);
            if (res == null || res.Count == 0)
            {
                data = new DexBalanceBody
                {
                    address = address,
                    contractHash = contractHash,
                    balance = format(value * -1),
                    curvalue = format(value * -1)
                };
                mh.PutData<DexBalanceBody>(localConn.connStr, localConn.connDB, dexBalanceStateCol, data);
            }
            else
            {
                // update or replace
                data = res[0];
                data.balance = format(format(data.balance) - value);
                data.curvalue = format(format(data.curvalue) - value);
                mh.ReplaceData<DexBalanceBody>(localConn.connStr, localConn.connDB, dexBalanceStateCol, data, findstr);
            }

        }
        private void confirm()
        {
            string findstr = new JObject() { { "curvalue", new JObject() { { "$ne", 0 } } } }.ToString();
            if (mh.GetDataCount(localConn.connStr, localConn.connDB, dexBalanceStateCol, findstr) <= 0) return;

            List<DexBalanceBody> res = mh.GetData<DexBalanceBody>(localConn.connStr, localConn.connDB, dexBalanceStateCol, findstr);
            foreach (DexBalanceBody jo in res)
            {
                jo.curvalue = format(0);
                findstr = new JObject() { { "address", jo.address }, { "contractHash", jo.contractHash } }.ToString();
                mh.ReplaceData<DexBalanceBody>(localConn.connStr, localConn.connDB, dexBalanceStateCol, jo, findstr);
            }
        }
        private void reset()
        {
            string findstr = new JObject() { { "curvalue", new JObject() { { "$ne", 0 } } } }.ToString();
            List<DexBalanceBody> res = mh.GetData<DexBalanceBody>(localConn.connStr, localConn.connDB, dexBalanceStateCol, findstr);
            if (res == null || res.Count == 0) return;

            foreach (DexBalanceBody item in res)
            {
                item.balance = format(format(item.balance) - format(item.curvalue));
                item.curvalue = format(0);
                findstr = new JObject() { { "address", item.address }, { "contractHash", item.contractHash } }.ToString();
                mh.ReplaceData<DexBalanceBody>(localConn.connStr, localConn.connDB, dexBalanceStateCol, item, findstr);
            }
        }

        private void updateRecord(long height)
        {
            string newdata = new JObject() { { "counter", "dexBalanceState" }, { "lastBlockindex", height } }.ToString();
            string findstr = new JObject() { { "counter", "dexBalanceState" } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, dexBalanceRecordCol, findstr);
            if (res == null || res.Count == 0)
            {
                mh.PutData(localConn.connStr, localConn.connDB, dexBalanceRecordCol, newdata);
            }
            else
            {
                mh.ReplaceData(localConn.connStr, localConn.connDB, dexBalanceRecordCol, newdata, findstr);
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
            string findStr = new JObject() { { "counter", "dexBalanceState" } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, dexBalanceRecordCol, findStr);
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

        public static BsonDecimal128 format(decimal value)
        {
            return BsonDecimalHelper.format(value);
        }

        public static decimal format(BsonDecimal128 value)
        {
            return BsonDecimalHelper.format(value);
        }
    }
}
