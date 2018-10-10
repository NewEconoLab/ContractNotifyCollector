using ContractNotifyCollector.core.dao;
using ContractNotifyCollector.helper;
using MongoDB.Bson;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ContractNotifyCollector.core.task
{
    class DomainSellAnalyzer : ContractTask
    {
        private JObject config;
        private MongoDBHelper mh = new MongoDBHelper();

        public DomainSellAnalyzer(string name) : base(name)
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


        private DbConnInfo localDbConnInfo;
        private DbConnInfo remoteDbConnInfo;
        private DbConnInfo blockDbConnInfo;
        private string auctionRecordColl;
        private string auctionStateColl;
        private string notifyRecordColl;
        private string notifyDomainSellColl;
        private string notifyDomainCenterColl;
        private int batchSize { set; get; }
        private int batchInterval { set; get; }
        private string bonusAddress { get; set; }
        private string bonusAddressColl { get; set; }
        private bool initSuccFlag = false;
        private void initConfig()
        {
            //JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name()).ToArray()[0]["taskInfo"];
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];
            
            auctionRecordColl = cfg["auctionRecordColl"].ToString();
            auctionStateColl = cfg["auctionStateColl"].ToString();
            notifyRecordColl = cfg["notifyRecordColl"].ToString();
            notifyDomainSellColl = cfg["notifyDomainSellColl"].ToString();
            notifyDomainCenterColl = cfg["notifyDomainCenterColl"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());
            bonusAddress = cfg["bonusAddress"].ToString();

            // db info
            localDbConnInfo = Config.localDbConnInfo;
            //remoteDbConnInfo = Config.remoteDbConnInfo;
            remoteDbConnInfo = Config.notifyDbConnInfo;
            blockDbConnInfo = Config.blockDbConnInfo;
            //
            initSuccFlag = true;
        }

        private void run()
        {
            if (!initSuccFlag) return;
            //clearCurAddprice();
            bool hasCreateIndex = false;
            while (true)
            {
                ping();

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
                
                // 
                for (long index = localHeight; index <= remoteHeight; index += batchSize)
                {
                    long nextIndex = index + batchSize;
                    long endIndex = nextIndex < remoteHeight ? nextIndex : remoteHeight;
                    // 待处理数据
                    JObject queryFilter = new JObject() { { "blockindex", new JObject() { { "$gt", index },{ "$lte", endIndex } } } };
                    JObject queryField = new JObject() { { "state", 0 } };
                    JArray queryRes = getDataWithField(remoteDbConnInfo, notifyDomainSellColl, queryField.ToString(), queryFilter.ToString());
                    if (queryRes == null || queryRes.Count() == 0)
                    {
                        updateDomainRecord(endIndex);
                        log(endIndex, remoteHeight);
                        continue;
                    }
                    // 高度时间列表
                    long[] blockindexArr = queryRes.Select(item => long.Parse(item["blockindex"].ToString())).Distinct().OrderBy(p => p).ToArray();
                    long[] blockindexArrs = new long[] { };
                    blockindexArrs = blockindexArrs.Concat(queryRes.Select(item => long.Parse(item["blockindex"].ToString())).ToArray()).ToArray();
                    JToken[] tmp = queryRes.Where(item => item["startBlockSelling"] != null && item["startBlockSelling"].ToString() != "").ToArray() ;
                    if(tmp != null && tmp.Count() > 0)
                    {
                        blockindexArrs = blockindexArrs.Concat(tmp.Select(item => long.Parse(item["startBlockSelling"].ToString())).ToArray()).ToArray();
                    }
                    tmp = queryRes.Where(item => item["endBlock"] != null && item["endBlock"].ToString() != "").ToArray();
                    if(tmp != null && tmp.Count() > 0)
                    {
                        blockindexArrs = blockindexArrs.Concat(tmp.Select(item => long.Parse(item["endBlock"].ToString())).ToArray()).ToArray();
                    }
                    tmp = queryRes.Where(item => item["lastBlock"] != null && item["lastBlock"].ToString() != "").ToArray();
                    if (tmp != null && tmp.Count() > 0)
                    {
                        blockindexArrs = blockindexArrs.Concat(tmp.Select(item => long.Parse(item["lastBlock"].ToString())).ToArray()).ToArray();
                    }
                    blockindexArrs = blockindexArrs.Distinct().ToArray();
                    Dictionary<string, long> blockindexDict = getBlockTimeByIndex(blockindexArrs.ToArray());
                    Dictionary<string, string> blockindexDictFmt = blockindexDict.ToDictionary(key => key.Key, val => TimeHelper.toBlockindexTimeFmt(val.Value));
                    
                    //
                    int cnt = blockindexArr.Count();
                    for (int i = 0; i < cnt; ++i)
                    {
                        long blockindex = blockindexArr[i];
                        JToken[] res = queryRes.Where(p => int.Parse(p["blockindex"].ToString()) == blockindex).ToArray();
                        JToken[] rr = null;
                        rr = res.Where(p2 => p2["displayName"].ToString() == "startAuction").ToArray();
                        if(rr != null && rr.Count() != 0)
                        {
                            updateR1(rr, blockindex, blockindexDict);
                        }
                        rr = res.Where(p2 => p2["displayName"].ToString() == "changeAuctionState").ToArray();
                        if (rr != null && rr.Count() != 0)
                        {
                            updateR2(rr, blockindex, blockindexDict);
                        }
                        rr = res.Where(p2 => p2["displayName"].ToString() == "assetManagement").ToArray();
                        if (rr != null && rr.Count() != 0)
                        {
                            updateR3(rr, blockindex, blockindexDict);
                        }
                        rr = res.Where(p2 => p2["displayName"].ToString() == "raiseEndsAuction").ToArray();
                        if (rr != null && rr.Count() != 0)
                        {
                            updateR4(rr, blockindex, blockindexDict);
                        }
                        rr = res.Where(p2 => p2["displayName"].ToString() == "collectDomain").ToArray();
                        if (rr != null && rr.Count() != 0)
                        {
                            updateR5(rr, blockindex, blockindexDict);
                        }

                        // 更新已处理累加
                        updateCurAddprice();

                        // 更新高度
                        if (blockindex != localHeight)
                        {
                            updateDomainRecord(blockindex);
                        }
                        log(blockindex, remoteHeight);
                    }

                }

                // 
                if (hasCreateIndex) continue;
                mh.setIndex(localDbConnInfo.connStr, localDbConnInfo.connDB, auctionStateColl, "{'auctionId':1}", "i_auctionId");
                mh.setIndex(localDbConnInfo.connStr, localDbConnInfo.connDB, auctionStateColl, "{'auctionState':1}", "i_auctionState");
                mh.setIndex(localDbConnInfo.connStr, localDbConnInfo.connDB, auctionStateColl, "{'addwholist.address':1}", "i_addwholist_address");
                hasCreateIndex = true;
            }
        }
        
        private void updateR1(JToken[] rr, long blockindex, Dictionary<string, long> blockindexDict)
        {// startAuction

            // 哈希域名列表(父域名-父哈希)
            string[] phArr = rr.Select(item => item["parenthash"].ToString()).Distinct().Where(pItem => pItem != "").ToArray();
            Dictionary<string, string> phDict = getDomainByHash(phArr);
            foreach (JToken jt in rr)
            {
                string auctionId = jt["auctionId"].ToString();
                string domain = jt["domain"].ToString();
                string parenthash = jt["parenthash"].ToString();
                string who = jt["who"].ToString();
                string txid = jt["txid"].ToString();
                long time = blockindexDict.GetValueOrDefault(blockindex + "");
                AuctionTx at = queryAuctionTx(auctionId);
                if (at == null)
                {
                    at = new AuctionTx()
                    {
                        auctionId = auctionId,
                        domain = domain,
                        parenthash = parenthash,
                        fulldomain = domain + "." + phDict.GetValueOrDefault(parenthash),
                        startAddress = who,
                        /*startTime = new AuctionTime
                        {
                            blockindex = blockindex,
                            blocktime = time,
                            txid = txid
                        },*/
                        endTime = new AuctionTime
                        {
                            blockindex = 0,
                            blocktime = 0,// time + THREE_DAY_SECONDS,
                            txid = ""
                        },
                        auctionState = AuctionState.STATE_CONFIRM,
                        maxPrice = format(0),
                        //ttl = time + TimeConst.getTimeSetter("." + phDict.GetValueOrDefault(parenthash)).ONE_DAY_SECONDS,
                        ttl = 0,

                        // 最后操作时间(包括最后出价时间和领取域名/取回Gas时间)
                        lastTime = new AuctionTime
                        {
                            blockindex = blockindex,
                            blocktime = blockindex == 0 ? 0 : time,
                            txid = blockindex == 0 ? "" : txid
                        }
                    };
                    insertAuctionTx(at);
                }
                else
                {
                    // 本无需处理，但为了后面数据重跑，新增如下更新
                    at.domain = domain;
                    at.parenthash = parenthash;
                    at.fulldomain = domain + "." + phDict.GetValueOrDefault(parenthash);
                    at.startAddress = who;
                    /*
                    at.startTime = new AuctionTime
                    {
                        blockindex = blockindex,
                        blocktime = time,
                        txid = txid
                    };*/
                    at.endTime = new AuctionTime
                    {
                        blockindex = 0,
                        blocktime = 0,//time + THREE_DAY_SECONDS,
                        txid = ""
                    };
                    at.auctionState = AuctionState.STATE_CONFIRM;
                    at.maxPrice = format(0);
                    //at.ttl = time + TimeConst.getTimeSetter("." + phDict.GetValueOrDefault(parenthash)).ONE_YEAR_SECONDS;
                    replaceAuctionTx(at, auctionId);
                }

            }
        }
        private void updateR2(JToken[] rr, long blockindex, Dictionary<string, long> blockindexDict)
        {// changeAuctionState
            foreach (JToken jt in rr) 
            {
                string auctionId = jt["auctionId"].ToString();
                string domain = jt["domain"].ToString();
                string parenthash = jt["parenthash"].ToString();
                //string domainTTL = jt["domainTTL"].ToString();
                long startBlockSelling = long.Parse(jt["startBlockSelling"].ToString());
                long endBlock = long.Parse(jt["endBlock"].ToString());
                decimal maxPrice = decimal.Parse(jt["maxPrice"].ToString());
                string maxBuyer = jt["maxBuyer"].ToString();
                long lastBlock = long.Parse(jt["lastBlock"].ToString());
                string txid = jt["txid"].ToString();

                AuctionTx at = queryAuctionTx(auctionId);
                if(at == null)
                {
                    // 没有竞拍信息，报错停止处理
                    error(auctionId); return;
                }
                if(format(at.maxPrice) > maxPrice)
                {
                    continue;
                }
                at.domain = domain;
                at.parenthash = parenthash;
                //at.domainTTL = domainTTL;
                if(endBlock != 0)
                {
                    at.endTime = new AuctionTime
                    {
                        blockindex = endBlock,
                        blocktime = blockindexDict.GetValueOrDefault(blockindex + ""),
                        txid = txid
                    };
                }
                if(at.startTime == null || at.startTime.blockindex == 0)
                {
                    at.startTime = new AuctionTime
                    {
                        blockindex = startBlockSelling,
                        blocktime = blockindexDict.GetValueOrDefault(startBlockSelling + ""),
                        txid = txid
                    };
                }
                if(at.ttl == 0)
                {
                    at.ttl = at.startTime.blocktime + TimeConst.getTimeSetter(at.fulldomain.Substring(at.fulldomain.IndexOf("."))).ONE_YEAR_SECONDS;
                }
                
                at.maxPrice = format(maxPrice);
                at.maxBuyer = maxBuyer;
                replaceAuctionTx(at, auctionId);
               
            }
        }

        private void updateR3(JToken[] rr, long blockindex, Dictionary<string, long> blockindexDict)
        {// assetManagement
            foreach (JToken jt in rr) 
            {
                //string auctionId = jt["auctionId"].ToString();
                string from = jt["from"].ToString();
                string to = jt["to"].ToString();

                decimal value = decimal.Parse(jt["value"].ToString());
                string txid = jt["txid"].ToString();
                if(txid == "0xc97606fd0db030369a9848ef7c19543f66aad752bb70984b94f165c69b6301ca")
                {
                   //throw new Exception("TTT");
                }
                
                bool auctionidIsTo = true;
                string auctionId = null;
                string address = null;
                if(from.Length > to.Length)
                {
                    auctionidIsTo = false;
                    auctionId = "0x" + from.hexstringReverse();
                    address = to;
                } else
                {
                    auctionidIsTo = true;
                    auctionId = "0x" + to.hexstringReverse();
                    address = from;
                }
                // 如果to为分红地址
                if(!auctionidIsTo && to == bonusAddress)
                {
                    //string data = new JObject() { { "from", from }, { "to", to }, { "value", value } }.ToString();
                    //mh.PutData(localDbConnInfo.connStr, localDbConnInfo.connDB, bonusAddressColl, data);
                    //return;
                }
                AuctionTx at = queryAuctionTx(auctionId);
                if (at == null)
                {
                    // 没有竞拍信息，报错停止处理
                    error(auctionId); return;
                }
                if(at.addwholist == null)
                {
                    at.addwholist = new List<AuctionAddWho>();
                }
                AuctionAddWho addwho = null;
                AuctionAddWho[] addwhoArr = at.addwholist.Where(p => p.address == address).ToArray();
                if(addwhoArr != null && addwhoArr.Count() > 0)
                {
                    addwho = addwhoArr[0];
                    at.addwholist.Remove(addwho);
                    //addwho.totalValue = auctionidIsTo ? addwho.totalValue + value : addwho.totalValue - value;
                    if (addwho.addpricelist != null && addwho.addpricelist.Any(p => p != null && p.time.txid == txid && format(p.value) == value)) continue;
                } else
                {
                    addwho = new AuctionAddWho();
                    addwho.address = address;
                    addwho.totalValue = format(0);
                    addwho.curTotalValue = format(0);
                    //addwho.totalValue = value;
                }
                bool isPositiveFlag = auctionidIsTo || (!auctionidIsTo && address == bonusAddress);
                if (isPositiveFlag)
                {
                    addwho.totalValue = format(format(addwho.totalValue) + value);
                    addwho.curTotalValue = format(format(addwho.curTotalValue ) + value);
                }
                if (auctionidIsTo)
                {
                    addwho.lastTime = new AuctionTime
                    {
                        blockindex = blockindex,
                        blocktime = blockindexDict.GetValueOrDefault(blockindex + ""),
                        txid = txid
                    };
                    // 最后操作时间(包括最后出价时间和领取域名/取回Gas时间)
                    at.lastTime = new AuctionTime
                    {
                        blockindex = blockindex,
                        blocktime = blockindex == 0 ? 0 : blockindexDict.GetValueOrDefault(blockindex + ""),
                        txid = blockindex == 0 ? "" : txid
                    };
                } else
                {
                    if (bonusAddress != address && value > 0)
                    {
                        addwho.accountTime = new AuctionTime
                        {
                            blockindex = blockindex,
                            blocktime = blockindexDict.GetValueOrDefault(blockindex + ""),
                            txid = txid
                        };
                    }
                }

                // 正在处理高度累计加价：待讨论
                // totalValue = curTotalValue
                // 开始：totalValue -= curTotalValue， curTotalValue = 0
                // 加价：totalValue += value        ， curTotalValue += value
                // 完成：curTotalValue = 0， 更新已处理高度
                
                // 加价列表
                if (addwho.addpricelist == null )
                {
                    addwho.addpricelist = new List<AuctionAddPrice>();
                }
                bool hasExist = addwho.addpricelist.Any(p => p != null && p.time.txid == txid && p.value == value);
                if(!hasExist)
                {
                    addwho.addpricelist.Add(new AuctionAddPrice
                    {
                        time = new AuctionTime
                        {
                            blockindex = blockindex,
                            blocktime = blockindexDict.GetValueOrDefault(blockindex + ""),
                            txid = txid
                        },
                        value = isPositiveFlag ? format(value) : format(value * -1),
                        isEnd = isPositiveFlag ? "0" : "1"
                    });
                }
                at.addwholist.Add(addwho);
                replaceAuctionTx(at, auctionId);
                
            }
        }
        private void updateR4(JToken[] rr, long blockindex, Dictionary<string, long> blockindexDict)
        {//raiseEndsAuction
            foreach (JToken jt in rr)
            {
                string auctionId = jt["auctionId"].ToString();
                string who = jt["who"].ToString();
                string txid = jt["txid"].ToString();
                AuctionTx at = queryAuctionTx(auctionId);
                if (at == null)
                {
                    // 没有竞拍信息，报错停止处理
                    error(auctionId); return;
                }
                at.endAddress = who;
                at.endTime = new AuctionTime
                {
                    blockindex = blockindex,
                    blocktime = blockindexDict.GetValueOrDefault(blockindex + ""),
                    txid = txid
                };
                replaceAuctionTx(at, auctionId);

            }
        }
        private void updateR5(JToken[] rr, long blockindex, Dictionary<string, long> blockindexDict)
        {// collectDomain
            foreach (JToken jt in rr)
            {
                string auctionId = jt["auctionId"].ToString();
                string domain = jt["domain"].ToString();
                string parenthash = jt["parenthash"].ToString();
                string who = jt["who"].ToString();
                string txid = jt["txid"].ToString();
                AuctionTx at = queryAuctionTx(auctionId);
                if(at == null)
                {
                    // 没有竞拍信息，报错停止处理
                    error(auctionId); return;
                } 
                if(at.addwholist == null || at.addwholist.Count == 0)
                {
                    at.addwholist = new List<AuctionAddWho>();
                }
                AuctionAddWho addwho = null;
                AuctionAddWho[] addwhoArr = at.addwholist.Where(p => p.address == who).ToArray();
                if (addwhoArr != null && addwhoArr.Count() > 0)
                {
                    addwho = addwhoArr[0];
                    at.addwholist.Remove(addwho);
                }
                else
                {
                    addwho = new AuctionAddWho();
                    addwho.address = who;
                }
                addwho.getdomainTime = new AuctionTime
                {
                    blockindex = blockindex,
                    blocktime = blockindexDict.GetValueOrDefault(blockindex + ""),
                    txid = txid
                };
                at.addwholist.Add(addwho);
                replaceAuctionTx(at, auctionId);
            }
        }
        
        private void clearCurAddprice()
        {
            clearOrUpdateCurAddprice(true);
        }
        private void updateCurAddprice()
        {
            clearOrUpdateCurAddprice(false);
        }
        private void clearOrUpdateCurAddprice(bool isClear)
        {
            string filter = new JObject() { { "addwholist.curTotalValue", new JObject() { { "$gt", 0} } } }.ToString();
            List<AuctionTx> list = mh.GetData<AuctionTx>(localDbConnInfo.connStr, localDbConnInfo.connDB, auctionStateColl, filter);
            if (list == null || list.Count == 0)
            {
                return;
            }
            list.Where(p => p.addwholist != null).Select(p =>
            {
                p.addwholist =
                    p.addwholist.Select(pk =>
                    {
                        if(isClear)
                        {
                            pk.totalValue = format(format(pk.totalValue) - format(pk.curTotalValue));
                        }
                        pk.curTotalValue = format(0);
                        return pk;
                    }).ToList();
                // 更新数据
                string findStr = new JObject() { {"auctionId", p.auctionId } }.ToString();
                mh.ReplaceData<AuctionTx>(localDbConnInfo.connStr, localDbConnInfo.connDB, auctionStateColl, p, findStr);
                return p;
            }).ToArray();
        }
        private void error(string auctionId="")
        {
            Console.WriteLine("not find data,auctionId:"+ auctionId);
            throw new Exception("not find data,auctionId:"+ auctionId);
        }
        
        private AuctionTx queryAuctionTx(string auctionId)
        {
            string filter = new JObject() { { "auctionId", auctionId } }.ToString();
            List<AuctionTx> res = mh.GetData<AuctionTx>(localDbConnInfo.connStr, localDbConnInfo.connDB, auctionStateColl, filter);
            if(res == null || res.Count == 0)
            {
                return null;
            }
            return res[0];
        }
        private void insertAuctionTx(AuctionTx at)
        {
            mh.PutData(localDbConnInfo.connStr, localDbConnInfo.connDB, auctionStateColl, at);
        }
        private void replaceAuctionTx(AuctionTx at, string auctionId)
        {
            string findstr = new JObject() { { "auctionId", auctionId } }.ToString();
            mh.ReplaceData(localDbConnInfo.connStr, localDbConnInfo.connDB, auctionStateColl, at, findstr);
        }
        private void updateAuctionState(string newState, string oldState, string auctionId)
        {
            string findstr = new JObject() { { "auctionState", oldState },{ "auctionId", auctionId } }.ToString();
            string newdata = new JObject() { { "$set", new JObject() { { "auctionState", newState } } } }.ToString();
            mh.UpdateData(localDbConnInfo.connStr, localDbConnInfo.connDB, auctionStateColl, newdata, findstr);

        }

        private Dictionary<string, string> getDomainByHash(string[] parentHashArr)
        {
            JObject queryFilter = new JObject() { { "$or", new JArray() { parentHashArr.Select(item => new JObject() { { "namehash", item }, { "root", "1" } }).ToArray() } } };
            JObject sortFilter = new JObject() { { "blockindex", -1 } };
            JObject fieldFilter = MongoFieldHelper.toReturn(new string[] { "namehash", "domain" });
            JArray res = mh.GetDataWithField(remoteDbConnInfo.connStr, remoteDbConnInfo.connDB, notifyDomainCenterColl, fieldFilter.ToString(), queryFilter.ToString());
            return res.GroupBy(item => item["namehash"], (k, g) =>
            {
                JObject obj = new JObject();
                obj.Add("namehash", k.ToString());
                obj.Add("domain", g.ToArray()[0]["domain"].ToString());
                return obj;
            }).ToArray().ToDictionary(key => key["namehash"].ToString(), val => val["domain"].ToString());
        }
        private Dictionary<string, long> getBlockTimeByIndex(long[] blockindexArr)
        {
            JObject queryFilter = MongoFieldHelper.toFilter(blockindexArr, "index", "$or");
            JObject returnFilter = MongoFieldHelper.toReturn(new string[] { "index", "time" });
            JArray blocktimeRes = mh.GetDataWithField(blockDbConnInfo.connStr, blockDbConnInfo.connDB, "block", returnFilter.ToString(), queryFilter.ToString());
            return blocktimeRes.ToDictionary(key => key["index"].ToString(), val => long.Parse(val["time"].ToString()));
        }
       
        private void updateDomainRecord(long blockindex)
        {
            string newdata = new JObject() { { "contractColl", notifyDomainSellColl }, { "lastBlockindex", blockindex } }.ToString();
            string findstr = new JObject() { { "contractColl", notifyDomainSellColl } }.ToString();
            long count = getDataCount(localDbConnInfo, auctionRecordColl, findstr);
            if (count <= 0)
            {
                putData(localDbConnInfo, auctionRecordColl, newdata);
            } else
            {
                replaceData(localDbConnInfo, auctionRecordColl, newdata, findstr);
            }
        }
        private long getRemoteHeight()
        {
            string findstr = new JObject() { { "counter", "notify" } }.ToString();
            JArray res = getData(remoteDbConnInfo, notifyRecordColl, findstr);
            if (res == null || res.Count == 0)
            {
                return 0;
            }
            return long.Parse(Convert.ToString(res.OrderBy(p => int.Parse(p["lastBlockindex"].ToString())).ToArray()[0]["lastBlockindex"]));
        }
        private long getLocalHeight()
        {
            string findstr = new JObject() { { "contractColl", notifyDomainSellColl } }.ToString();
            JArray res = getData(localDbConnInfo, auctionRecordColl, findstr);
            if (res == null || res.Count == 0)
            {
                return 0;
            }
            return long.Parse(Convert.ToString(res.OrderBy(p => int.Parse(p["lastBlockindex"].ToString())).ToArray()[0]["lastBlockindex"]));
        }
        private JArray getData(DbConnInfo db, string coll, string filter)
        {
            return mh.GetData(db.connStr, db.connDB, coll, filter);
        }
        private void putData(DbConnInfo db, string coll, string data)
        {
            mh.PutData(db.connStr, db.connDB, coll, data);
        }
        private void updateData(DbConnInfo conn, string coll, JObject Jdata, string Jcondition)
        {
            updateData(conn, coll, new JObject() { { "$set", Jdata } }.ToString(), Jcondition);
        }
        private void updateData(DbConnInfo conn, string coll, string Jdata, string Jcondition)
        {
            mh.UpdateData(conn.connStr, conn.connDB, coll, Jdata, Jcondition);
        }
        private void replaceData(DbConnInfo db, string coll, string data, string condition)
        {
            mh.ReplaceData(db.connStr, db.connDB, coll, data, condition);
        }
        private JArray getDataWithField(DbConnInfo db, string coll, string fieldBson, string findBson)
        {
            return mh.GetDataWithField(db.connStr, db.connDB, coll, fieldBson, findBson);
        }
       
        private long getDataCount(DbConnInfo db, string coll, string filter)
        {
            return mh.GetDataCount(db.connStr, db.connDB, coll, filter);
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
