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
    class NNSfixedSellingBalanceTask : ContractTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo localConn;
        private DbConnInfo remoteConn;
        private string nnsFixedSellingBalanceRecordCol;
        private string nnsFixedSellingBalanceStateCol;
        private string remoteConnRecordColl;
        private string remoteConnStateColl;
        private int batchSize;
        private int batchInterval;
        private bool initSuccFlag = false;
        private bool hasCreateIndex = false;

        public NNSfixedSellingBalanceTask(string name) : base(name)
        {
        }

        public override void initConfig(JObject config)
        {
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];
            nnsFixedSellingBalanceRecordCol = cfg["nnsFixedSellingBalanceRecordCol"].ToString();
            nnsFixedSellingBalanceStateCol = cfg["nnsFixedSellingBalanceStateCol"].ToString();
            remoteConnRecordColl = cfg["remoteConnRecordColl"].ToString();
            remoteConnStateColl = cfg["remoteConnStateColl"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());

            //
            localConn = Config.localDbConnInfo;
            remoteConn = Config.notifyDbConnInfo;
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

            for(long startIndex=localHeight; startIndex <= remoteHeight; startIndex += batchSize)
            {
                long nextIndex = startIndex + batchSize;
                long endIndex = nextIndex < remoteHeight ? nextIndex : remoteHeight;
                string findStr = new JObject() {
                    { "blockindex", new JObject() { {"$gt", startIndex }, { "$lte", endIndex } } },
                    { "$or", new JArray{
                        new JObject() {{"displayName", "setMoneyIn"} },
                        new JObject() {{"displayName", "getMoneyBack"} },
                        new JObject() {{"displayName", "NNSfixedSellingBuy" } }
                                    }
                    }
                }.ToString();
                var query = mh.GetData(remoteConn.connStr, remoteConn.connDB, remoteConnStateColl, findStr);
                if(query == null || query.Count == 0)
                {
                    updateRecord(endIndex);
                    log(endIndex, remoteHeight);
                    continue;
                }

                var 
                jt = query.Where(p => p["displayName"].ToString() == "setMoneyIn").ToArray();
                processSetMoneyIn(jt, remoteConnStateColl);
                jt = query.Where(p => p["displayName"].ToString() == "getMoneyBack").ToArray();
                processGetMoneyBack(jt, remoteConnStateColl);
                jt = query.Where(p => p["displayName"].ToString() == "NNSfixedSellingBuy").ToArray();
                processNNSfixedSellingBuy(jt, remoteConnStateColl);

                //
                confirm();
                updateRecord(endIndex);
                log(endIndex, remoteHeight);

            }

            // 添加索引
            if (hasCreateIndex) return;
            mh.setIndex(localConn.connStr, localConn.connDB, nnsFixedSellingBalanceStateCol, "{'address':1}", "i_address");
            mh.setIndex(localConn.connStr, localConn.connDB, nnsFixedSellingBalanceStateCol, "{'address':1,'register':1}", "i_address_register");
            hasCreateIndex = true;
        }

        private void processSetMoneyIn(JToken[] jt, string sellinghash)
        {
            if (jt == null || jt.Count() == 0) return;
            // 充值
            jt.GroupBy(p => p["who"].ToString(), (k, g) =>
            {
                return new
                {
                    addr = k.ToString(),
                    value = g.Select(pg => decimal.Parse(pg["value"].ToString())).Sum()
                };
            }).ToList().ForEach(pf =>
            {
                addBalance(pf.addr, sellinghash, pf.value);
            });
        }
        private void processGetMoneyBack(JToken[] res, string sellinghash)
        {
            if (res == null || res.Count() == 0) return;

            // 提取
            res.GroupBy(p => p["who"], (k, g) => {
                return new
                {
                    addr = k.ToString(),
                    value = g.Select(pg => decimal.Parse(pg["value"].ToString())).Sum()
                };
            }).ToList().ForEach(pf => {
                subBalance(pf.addr, sellinghash, pf.value);
            });
        }
        private void processNNSfixedSellingBuy(JToken[] res, string sellinghash)
        {
            // buy
            res.GroupBy(p => p["addr"].ToString(), (k, g) =>
            {
                return new
                {
                    addr = k.ToString(),
                    value = g.Select(pg => decimal.Parse(pg["price"].ToString())).Sum()
                };
            }).ToList().ForEach(pf =>
            {
                subBalance(pf.addr, sellinghash, pf.value);
            });

            // sell
            res.GroupBy(p => p["seller"].ToString(), (k, g) =>
            {
                return new
                {
                    addr = k.ToString(),
                    value = g.Select(pg => decimal.Parse(pg["price"].ToString())).Sum()
                };
            }).ToList().ForEach(pf =>
            {
                addBalance(pf.addr, sellinghash, pf.value);
            });
        }



        private void reset()
        {
            string findstr = new JObject() { { "curvalue", new JObject() { { "$ne", 0 } } } }.ToString();
            List<CGasBalanceBody> res = mh.GetData<CGasBalanceBody>(localConn.connStr, localConn.connDB, nnsFixedSellingBalanceStateCol, findstr);
            if (res == null || res.Count == 0) return;

            foreach (CGasBalanceBody item in res)
            {
                item.balance = format(format(item.balance) - format(item.curvalue));
                item.curvalue = format(0);
                findstr = new JObject() { { "address", item.address }, { "register", item.register } }.ToString();
                mh.ReplaceData<CGasBalanceBody>(localConn.connStr, localConn.connDB, nnsFixedSellingBalanceStateCol, item, findstr);
            }
        }
        private void confirm()
        {
            string findstr = new JObject() { { "curvalue", new JObject() { { "$ne", 0 } } } }.ToString();
            if (mh.GetDataCount(localConn.connStr, localConn.connDB, nnsFixedSellingBalanceStateCol, findstr) <= 0) return;

            List<CGasBalanceBody> res = mh.GetData<CGasBalanceBody>(localConn.connStr, localConn.connDB, nnsFixedSellingBalanceStateCol, findstr);
            foreach (CGasBalanceBody jo in res)
            {
                jo.curvalue = format(0);
                findstr = new JObject() { { "address", jo.address }, { "register", jo.register } }.ToString();
                mh.ReplaceData<CGasBalanceBody>(localConn.connStr, localConn.connDB, nnsFixedSellingBalanceStateCol, jo, findstr);
            }

            /*
             * 
            string updatestr = new JObject() { { "$set", new JObject() { { "curvalue", "0" } } } }.ToString();
            mh.UpdateData(localConn.connStr, localConn.connDB, cgasBalanceStateCol, updatestr, findstr);
            */
        }

        [BsonIgnoreExtraElements]
        private class CGasBalanceBody
        {
            public ObjectId _id { get; set; }
            public string address { get; set; }
            public string register { get; set; }
            //public string balance { get; set; }
            public BsonDecimal128 balance { get; set; }
            //public string curvalue { get; set; }
            public BsonDecimal128 curvalue { get; set; }
        }
        private void addBalance(string address, string register, decimal value)
        {
            CGasBalanceBody data = null;
            string findstr = new JObject() { { "address", address }, { "register", register } }.ToString();
            List<CGasBalanceBody> res = mh.GetData<CGasBalanceBody>(localConn.connStr, localConn.connDB, nnsFixedSellingBalanceStateCol, findstr);
            if (res == null || res.Count == 0)
            {
                // insert
                data = new CGasBalanceBody
                {
                    address = address,
                    register = register,
                    balance = format(value),
                    curvalue = format(value)
                };
                mh.PutData<CGasBalanceBody>(localConn.connStr, localConn.connDB, nnsFixedSellingBalanceStateCol, data);
            }
            else
            {
                // update or replace
                data = res[0];
                data.balance = format(format(data.balance) + value);
                data.curvalue = format(format(data.curvalue) + value);
                mh.ReplaceData<CGasBalanceBody>(localConn.connStr, localConn.connDB, nnsFixedSellingBalanceStateCol, data, findstr);
            }
        }
        private void subBalance(string address, string register, decimal value)
        {
            CGasBalanceBody data = null;
            string findstr = new JObject() { { "address", address }, { "register", register } }.ToString();
            List<CGasBalanceBody> res = mh.GetData<CGasBalanceBody>(localConn.connStr, localConn.connDB, nnsFixedSellingBalanceStateCol, findstr);
            if (res == null || res.Count == 0)
            {
                data = new CGasBalanceBody
                {
                    address = address,
                    register = register,
                    balance = format(value * -1),
                    curvalue = format(value * -1)
                };
                mh.PutData<CGasBalanceBody>(localConn.connStr, localConn.connDB, nnsFixedSellingBalanceStateCol, data);
            }
            else
            {
                // update or replace
                data = res[0];
                data.balance = format(format(data.balance) - value);
                data.curvalue = format(format(data.curvalue) - value);
                mh.ReplaceData<CGasBalanceBody>(localConn.connStr, localConn.connDB, nnsFixedSellingBalanceStateCol, data, findstr);
            }

        }
        public static BsonDecimal128 format(decimal value)
        {
            return BsonDecimalHelper.format(value);
        }

        public static decimal format(BsonDecimal128 value)
        {
            return BsonDecimalHelper.format(value);
        }


        private void updateRecord(long height)
        {
            string newdata = new JObject() { { "counter", "nnsFixedSellingBalance" }, { "lastBlockindex", height } }.ToString();
            string findstr = new JObject() { { "counter", "nnsFixedSellingBalance" } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, nnsFixedSellingBalanceRecordCol, findstr);
            if (res == null || res.Count == 0)
            {
                mh.PutData(localConn.connStr, localConn.connDB, nnsFixedSellingBalanceRecordCol, newdata);
            }
            else
            {
                mh.ReplaceData(localConn.connStr, localConn.connDB, nnsFixedSellingBalanceRecordCol, newdata, findstr);
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
            string findStr = new JObject() { { "counter", "nnsFixedSellingBalance" } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, nnsFixedSellingBalanceRecordCol, findStr);
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
