using ContractNotifyCollector.core;
using ContractNotifyCollector.helper;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ContractNotifyCollector.contract.task
{
    class CGasBalanceTask : ContractTask
    {
        private JObject config;
        private MongoDBHelper mh = new MongoDBHelper();

        public CGasBalanceTask(string name): base(name)
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

        private string contractRecordCol;
        private string cgasBalanceRecordCol;
        private string cgasBalanceStateCol;
        private string[] registerHashArr;
        private string cgasHash;
        private int batchInterval;
        private int batchSize;
        
        private string[] registerAddrArr;
        private Dictionary<string, string> registerAddrDict;
        private JObject sgasFilter;

        private DbConnInfo localConn;
        private DbConnInfo remoteConn;
        private bool initSuccFlag = false;
        private void initConfig()
        {
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];
            contractRecordCol = cfg["contractRecordCol"].ToString();
            cgasBalanceRecordCol = cfg["cgasBalanceRecordCol"].ToString();
            cgasBalanceStateCol = cfg["cgasBalanceStateCol"].ToString();
            registerHashArr = ((JArray)cfg["registerHashArr"]).Select(p => p.ToString()).ToArray() ;
            cgasHash = cfg["cgasHash"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());

            registerAddrDict = toAddress(registerHashArr);
            registerAddrArr = registerAddrDict.Keys.ToArray();
            sgasFilter = new JObject() { { "$or", merge(new string[] { "to", "from" }, registerAddrArr) } };
            
            //localConn = Config.notifyDbConnInfo;
            localConn = Config.localDbConnInfo;
            remoteConn = Config.notifyDbConnInfo;
            initSuccFlag = true;
        }
        private JArray merge(string[] keys, string[] vals)
        {
            JArray ja = new JArray();
            foreach(string key in keys)
            {
                foreach(string val in vals)
                {
                    ja.Add(new JObject() { { key, val } });
                }
            }
            return ja;
        }
        private void run()
        {
            if (!initSuccFlag) return;
            //
            reset();
            while (true)
            {
                ping();

                // 获取远程已同步高度
                long remoteHeight =  getRemoteHeight();

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

                    /**
                     * Cgas 计算充值/提取方式替换为Sell setMoneyIn/getMoneyBack方式
                     * 
                    // 获取Cgas充值、提取
                    sgasFilter.Remove("blockindex");
                    sgasFilter.Add("blockindex", new JObject() { { "$gt", index }, { "$lte", endIndex } });
                    string findstr = sgasFilter.ToString();
                    string fieldstr = new JObject() { { "state", 0 } }.ToString();
                    JArray res = mh.GetDataWithField(remoteConn.connStr, remoteConn.connDB, cgasHash, fieldstr, findstr);
                    if(1 ==1 && res != null && res.Count >0)
                    {
                        res.GroupBy(p => p["to"], (k, g) =>
                        {
                            string to = k.ToString();
                            g.GroupBy(cp => cp["from"], (ck, cg) =>
                            {
                                string from = ck.ToString();
                                decimal value = cg.Sum(sp => decimal.Parse(sp["value"].ToString()));
                                // 入库(充值+提取)
                                if (registerAddrArr.Contains(to))
                                {
                                    // 充值
                                    string address = from;
                                    string register = registerAddrDict.GetValueOrDefault(to);
                                    addBalance(address, register, value);

                                } else if (registerAddrArr.Contains(from))
                                {
                                    // 提取
                                    string address = to;
                                    string register = registerAddrDict.GetValueOrDefault(from);
                                    subBalance(address, register, value);
                                } else
                                {
                                    // 其他注册器
                                }
                                return new JObject();
                            }).ToArray();
                            
                            return new JObject();
                        }).ToArray();
                    }

                    // 获取Sell加价、取回
                    foreach(string reghash in registerHashArr)
                    {
                        findstr = new JObject() { { "blockindex", new JObject() { { "$gt", index }, { "$lte", endIndex } } },{ "displayName", "assetManagement"} }.ToString();
                        res = mh.GetData(remoteConn.connStr, remoteConn.connDB, reghash, findstr);
                        if(res != null && res.Count > 0)
                        {
                            var r1 = res.Where(p => p["from"].ToString().Length == 34).GroupBy(rp => rp["from"], (rk, rg) => {

                                string address = rk.ToString();
                                decimal value = rg.Sum(sp => decimal.Parse(sp["value"].ToString()));
                                // 入库(加价)
                                //subBalance(address, reghash, value);
                                return new {address, value };
                            }).ToDictionary(k => k.address, v => v.value);
                            foreach(var item in r1)
                            {
                                if (item.Value == 0) continue;
                                subBalance(item.Key, reghash, item.Value);
                            }
                            var r2 = res.Where(p => p["to"].ToString().Length == 34).GroupBy(rp => rp["to"], (rk, rg) => {

                                string address = rk.ToString();
                                decimal value = rg.Sum(sp => decimal.Parse(sp["value"].ToString()));
                                // 入库(取回)
                                //addBalance(address, reghash, value);
                                return new { address, value };
                            }).ToDictionary(k => k.address, v => v.value);
                            foreach (var item in r2)
                            {
                                if (item.Value == 0) continue;
                                addBalance(item.Key, reghash, item.Value);
                            }
                        }
                    }
                    */
                    string findstr = null;
                    JArray res = null;
                    foreach (string reghash in registerHashArr)
                    {
                        findstr = new JObject() { { "blockindex", new JObject() { { "$gt", index }, { "$lte", endIndex } } }, { "$or", new JArray(){
                            //new JObject(){{"displayName", "assetManagement"} },
                            new JObject(){{"displayName", "raise"} },
                            new JObject(){{"displayName", "bidSettlement" } },
                            new JObject(){{"displayName", "setMoneyIn"} },
                            new JObject(){{"displayName", "getMoneyBack"} }
                        } } }.ToString();
                        res = mh.GetData(remoteConn.connStr, remoteConn.connDB, reghash, findstr);
                        var
                        jt = res.Where(p => p["displayName"].ToString() == "setMoneyIn").ToArray();
                        processSetMoneyIn(jt, reghash);
                        jt = res.Where(p => p["displayName"].ToString() == "raise").ToArray();
                        processRaise(jt, reghash);
                        jt = res.Where(p => p["displayName"].ToString() == "bidSettlement").ToArray();
                        processBidSettlement(jt, reghash);
                        jt = res.Where(p => p["displayName"].ToString() == "getMoneyBack").ToArray();
                        processGetMoneyBack(jt, reghash);
                    }

                    confirm();
                    updateRecord(endIndex);
                    log(endIndex, remoteHeight);
                }
            }
        }
        private void processSetMoneyIn(JToken[] res, string reghash)
        {
            if (res == null || res.Count() == 0) return;

            // 充值cgas
            res.GroupBy(p => p["who"], (k,g) => {
                return new
                {
                    addr = k.ToString(),
                    value = g.Select(pg => decimal.Parse(pg["value"].ToString())).Sum()
                };
            }).ToList().ForEach(pf => {
                addBalance(pf.addr, reghash, pf.value);
            });
        }
        private void processGetMoneyBack(JToken[] res, string reghash)
        {
            if (res == null || res.Count() == 0) return;

            // 提取cgas
            res.GroupBy(p => p["who"], (k, g) => {
                return new
                {
                    addr = k.ToString(),
                    value = g.Select(pg => decimal.Parse(pg["value"].ToString())).Sum()
                };
            }).ToList().ForEach(pf => {
                subBalance(pf.addr, reghash, pf.value);
            });
        }
        private void processRaise(JToken[] res, string reghash)
        {
            if (res == null || res.Count() == 0) return;

            // 加价-
            // usrAddr=100%(to=auctionId)
            res.GroupBy(p => p["who"], (k, g) => {
                return new
                {
                    addr = k.ToString(),
                    value = g.Select(pg => decimal.Parse(pg["value"].ToString())).Sum()
                };
            }).ToList().ForEach(pf => {
                subBalance(pf.addr, reghash, pf.value);
            });
        }
        private void processBidSettlement(JToken[] res, string reghash)
        {
            if (res == null || res.Count() == 0) return;

            // 结算+
            /**
             * 领取域名：regAddr=100%, usrAddr=0%(from=auctionId)
             * 取回cgas：regAddr=10%, usrAddr=90%(from=auctionId)
             */
            res.GroupBy(p => p["who"], (k, g) => {
                return new
                {
                    addr = k.ToString(),
                    value = g.Select(pg => decimal.Parse(pg["value"].ToString())).Sum()
                };
            }).ToList().ForEach(pf => {
                addBalance(pf.addr, reghash, pf.value);
            });

        }
        
        private void reset()
        {
            string findstr = new JObject() { { "curvalue", new JObject() { { "$ne", 0 } } } }.ToString();
            List<CGasBalanceBody> res = mh.GetData<CGasBalanceBody>(localConn.connStr, localConn.connDB, cgasBalanceStateCol, findstr);
            if(res == null || res.Count == 0) return;

            foreach(CGasBalanceBody item in res)
            {
                item.balance = format(format(item.balance) - format(item.curvalue));
                item.curvalue = format(0);
                findstr = new JObject() { { "address", item.address }, { "register", item.register } }.ToString();
                mh.ReplaceData<CGasBalanceBody>(localConn.connStr, localConn.connDB, cgasBalanceStateCol, item, findstr);
            }
        }
        private void confirm()
        {
            string findstr = new JObject() { { "curvalue", new JObject() { { "$ne", 0 } } } }.ToString();
            if (mh.GetDataCount(localConn.connStr, localConn.connDB, cgasBalanceStateCol, findstr) <= 0) return;

            List<CGasBalanceBody> res = mh.GetData<CGasBalanceBody>(localConn.connStr, localConn.connDB, cgasBalanceStateCol, findstr);
            foreach(CGasBalanceBody jo in res)
            {
                jo.curvalue = format(0);
                findstr = new JObject() { { "address", jo.address }, { "register", jo.register } }.ToString();
                mh.ReplaceData<CGasBalanceBody>(localConn.connStr, localConn.connDB, cgasBalanceStateCol, jo, findstr);
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
            List<CGasBalanceBody> res = mh.GetData<CGasBalanceBody>(localConn.connStr, localConn.connDB, cgasBalanceStateCol, findstr);
            if(res == null || res.Count == 0)
            {
                // insert
                data = new CGasBalanceBody
                {
                    address = address,
                    register = register,
                    balance = format(value),
                    curvalue = format(value)
                };
                mh.PutData<CGasBalanceBody>(localConn.connStr, localConn.connDB, cgasBalanceStateCol, data);
            }
            else
            {
                // update or replace
                data = res[0];
                data.balance = format(format(data.balance) + value);
                data.curvalue = format(format(data.curvalue) + value);
                mh.ReplaceData<CGasBalanceBody>(localConn.connStr, localConn.connDB, cgasBalanceStateCol, data, findstr);
            }
        }
        private void subBalance(string address, string register, decimal value)
        {
            CGasBalanceBody data = null;
            string findstr = new JObject() { { "address", address }, { "register", register } }.ToString();
            List<CGasBalanceBody> res = mh.GetData<CGasBalanceBody>(localConn.connStr, localConn.connDB, cgasBalanceStateCol, findstr);
            if(res == null || res.Count ==0)
            {
                data = new CGasBalanceBody
                {
                    address = address,
                    register = register,
                    balance = format(value*-1),
                    curvalue = format(value*-1)
                };
                mh.PutData<CGasBalanceBody>(localConn.connStr, localConn.connDB, cgasBalanceStateCol, data);
            } else
            {
                // update or replace
                data = res[0];
                data.balance = format(format(data.balance) - value);
                data.curvalue = format(format(data.curvalue) - value);
                mh.ReplaceData<CGasBalanceBody>(localConn.connStr, localConn.connDB, cgasBalanceStateCol, data, findstr);
            }

        }
        
        private Dictionary<string,string> toAddress(string[] scripthashArr)
        {
            return scripthashArr.Select(p => new { address = toAddress(p), scripthash = p }).ToDictionary(k => k.address, v => v.scripthash);//.ToArray();
        }
        private string toAddress(string scripthash)
        {
            return ThinNeo.Helper.GetAddressFromScriptHash(new ThinNeo.Hash160(scripthash)); ;
        }
        private string toAccount(string scripthash)
        {
            if(scripthash.StartsWith("0x"))
            {
                return scripthash.Substring(2).hexstringReverse();
            }
            return scripthash.hexstringReverse();
        }

        private void updateRecord(long height)
        {
            string newdata = new JObject() { { "counter", "cgasbalance" }, { "lastBlockindex", height } }.ToString();
            string findstr = new JObject() { { "counter", "cgasbalance" } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, cgasBalanceRecordCol, findstr);
            if (res == null || res.Count == 0)
            {
                mh.PutData(localConn.connStr, localConn.connDB, cgasBalanceRecordCol, newdata);
            }
            else
            {
                mh.ReplaceData(localConn.connStr, localConn.connDB, cgasBalanceRecordCol, newdata, findstr);
            }
        }

        private long getLocalHeight()
        {
            string findStr = new JObject() { { "counter", "cgasbalance" } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, cgasBalanceRecordCol, findStr);
            if (res == null || res.Count == 0)
            {
                return 0;
            }
            return long.Parse(res[0]["lastBlockindex"].ToString());
        }
        private long getRemoteHeight()
        {
            string findStr = new JObject() { { "counter", "notify" } }.ToString();
            JArray res = mh.GetData(remoteConn.connStr, remoteConn.connDB, contractRecordCol, findStr);
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
