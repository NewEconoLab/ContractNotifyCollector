using ContractNotifyCollector.helper;
using MongoDB.Bson;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ContractNotifyCollector.core.task
{
    class NewDomainSellAnalyzer : ContractTask
    {
        private JObject config;
        private MongoDBHelper mh = new MongoDBHelper();

        public NewDomainSellAnalyzer(string name) : base(name)
        {
        }

        public override void Init(JObject config)
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
        private string auctionRecordColl { set; get; }
        private string auctionStateColl { set; get; }
        private string notifyRecordColl { set; get; }
        private string notifyDomainSellColl { set; get; }
        private string notifyDomainCenterColl { set; get; }
        private int batchSize { set; get; }
        private int batchInterval { set; get; }
        private string bonusAddress { get; set; }
        private string bonusAddressColl { get; set; }
        private void initConfig()
        {
            localDbConnInfo = Config.localDbConnInfo;
            remoteDbConnInfo = Config.remoteDbConnInfo;
            blockDbConnInfo = Config.blockDbConnInfo;
            
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name()).ToArray()[0]["taskInfo"];

            auctionRecordColl = cfg["auctionRecordColl"].ToString();
            auctionStateColl = cfg["auctionStateColl"].ToString();
            notifyRecordColl = cfg["notifyRecordColl"].ToString();
            notifyDomainSellColl = cfg["notifyDomainSellColl"].ToString();
            notifyDomainCenterColl = cfg["notifyDomainCenterColl"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());
            bonusAddress = cfg["bonusAddress"].ToString();
        }

        private void run()
        {
            while(true)
            {
                ping();

                // 更新状态
                updateState();

                // 获取远程已同步高度
                JObject filter = new JObject() { { "contractHash", notifyDomainSellColl } };
                long remoteHeight = getContractHeight(remoteDbConnInfo, notifyRecordColl, filter.ToString());

                // 获取本地已处理高度
                filter = new JObject() { { "contractColl", notifyDomainSellColl } };
                long localHeight = getContractHeight(localDbConnInfo, auctionRecordColl, filter.ToString());
                
                // 
                for (long index = localHeight; index <= remoteHeight+1; index += batchSize)
                {
                    long nextIndex = index + batchSize;
                    long endIndex = nextIndex < remoteHeight ? nextIndex : remoteHeight+1;
                    // 待处理数据
                    JObject queryFilter = new JObject() { { "blockindex", new JObject() { { "$gte", index },{ "$lte", endIndex } } } };
                    JObject querySortBy = new JObject() { { "blockindex", 1 } };
                    JObject queryField = new JObject() { { "state", 0 } };
                    JArray queryRes = GetDataPagesWithField(remoteDbConnInfo, notifyDomainSellColl, queryField.ToString(), querySortBy.ToString(), queryFilter.ToString());
                    if (queryRes == null || queryRes.Count() == 0)
                    {
                        if (remoteHeight <= localHeight)
                        {
                            log(localHeight, remoteHeight);
                            break;
                        }
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
                    //blockindexArrs = blockindexArrs.Concat(queryRes.Select(item => long.Parse(item["startBlockSelling"].ToString())).ToArray()).ToArray();
                    tmp = queryRes.Where(item => item["endBlock"] != null && item["endBlock"].ToString() != "").ToArray();
                    if(tmp != null && tmp.Count() > 0)
                    {
                        blockindexArrs = blockindexArrs.Concat(tmp.Select(item => long.Parse(item["endBlock"].ToString())).ToArray()).ToArray();
                    }
                    //blockindexArrs = blockindexArrs.Concat(queryRes.Select(item => long.Parse(item["endBlock"].ToString())).ToArray()).ToArray();
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
                        if(blockindex != localHeight)
                        {
                            updateDomainRecord(blockindex);
                        }
                        log(blockindex, remoteHeight);
                    }

                    
                }
            }
        }
        
        private const long ONE_DAY_SECONDS = 1 * /*24 * 60 * */60 /*测试时5分钟一天*/* 5;
        private const long TWO_DAY_SECONDS = ONE_DAY_SECONDS * 2;
        private const long THREE_DAY_SECONDS = ONE_DAY_SECONDS * 3;
        private const long FIVE_DAY_SECONDS = ONE_DAY_SECONDS * 5;
        private const long ONE_YEAR_SECONDS = ONE_DAY_SECONDS * 365;

        private void updateState()
        {
            string filter = MongoFieldHelper.toFilter(new string[] { AuctionState.STATE_START, AuctionState.STATE_CONFIRM, AuctionState.STATE_RANDOM}, "auctionState").ToString();
            List<AuctionTx> list = mh.GetData<AuctionTx>(localDbConnInfo.connStr, localDbConnInfo.connDB, auctionStateColl, filter);
            if(list == null || list.Count == 0)
            {
                return;
            }
            foreach (AuctionTx jo in list)
            {
                long nowtime = TimeHelper.GetTimeStamp();
                long starttime = jo.startTime.blocktime;
                string oldState = jo.auctionState;

                /**
                 * 状态判断逻辑：
                 * 0. 开标为开标期
                 * 1. 小于等于三天，确定期
                 * 2. 大于三天，则：
                 *          a. 结束时间无值且前三天无人出价，则流拍
                 *          b. 超过1Y，则过期
                 *          c. 超过5D，则结束
                 *          d. (3,5)结束时间有值且大于开拍时间，则结束
                 *          e. (3,5)结束时间无值且最后出价在开拍后两天内，则结束
                 *          f. (3,5)其余为随机
                 */

                string newState = null;
                if(nowtime - starttime <= THREE_DAY_SECONDS)
                {
                    // 小于三天
                    newState = AuctionState.STATE_CONFIRM;
                } else
                {
                    // 大于三天
                    if (jo.lastTime == null || jo.lastTime.blockindex == 0)
                    {
                        // (3,5)结束时间无值且前三天无人出价，则流拍
                        newState = AuctionState.STATE_ABORT;
                    }
                    else if (nowtime > starttime + ONE_YEAR_SECONDS)
                    {
                        // 超过1Y，则过期
                        newState = AuctionState.STATE_EXPIRED;
                    }
                    else if (nowtime >= starttime + FIVE_DAY_SECONDS)
                    {
                        // 超过5D，则结束
                        newState = AuctionState.STATE_END;
                    }
                    else if (jo.endTime != null && jo.endTime.blocktime > starttime)
                    {
                        // (3,5)结束时间有值，且大于开拍时间，则结束
                        newState = AuctionState.STATE_END;
                    }
                    else if (jo.lastTime.blocktime <= starttime + TWO_DAY_SECONDS)
                    {
                        // (3,5)结束时间无值且最后出价在开拍后两天内，则超时3D结束
                        newState = AuctionState.STATE_END;
                    }
                    else
                    {
                        // 其余为随机期
                        newState = AuctionState.STATE_RANDOM;
                    }
                }

                if(oldState != newState)
                {
                    updateAuctionState(newState, oldState, jo.auctionId);
                }
                
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
                        startTime = new AuctionTime
                        {
                            blockindex = blockindex,
                            blocktime = blockindexDict.GetValueOrDefault(blockindex + ""),
                            txid = txid
                        },
                        endTime = new AuctionTime
                        {
                            blockindex = 0,
                            blocktime = 0,
                            txid = ""
                        },
                        auctionState = AuctionState.STATE_CONFIRM

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
                    at.startTime = new AuctionTime
                    {
                        blockindex = blockindex,
                        blocktime = blockindexDict.GetValueOrDefault(blockindex + ""),
                        txid = txid
                    };
                    at.endTime = new AuctionTime
                    {
                        blockindex = 0,
                        blocktime = 0,
                        txid = ""
                    };
                    at.auctionState = AuctionState.STATE_CONFIRM;
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
                string domainTTL = jt["domainTTL"].ToString();
                long startBlockSelling = long.Parse(jt["startBlockSelling"].ToString());
                long endBlock = long.Parse(jt["endBlock"].ToString());
                string maxPrice = jt["maxPrice"].ToString();
                string maxBuyer = jt["maxBuyer"].ToString();
                long lastBlock = long.Parse(jt["lastBlock"].ToString());
                string txid = jt["txid"].ToString();

                AuctionTx at = queryAuctionTx(auctionId);
                if(at == null)
                {
                    // 没有竞拍信息，报错停止处理
                    error(auctionId); return;
                }
                // 相同高度特殊处理
                if(at.domain == domain 
                    && at.parenthash == parenthash
                    && at.domainTTL == domainTTL
                    && at.endTime.blockindex == endBlock
                    && at.endTime.txid == (endBlock == 0 ? "":txid)
                    && decimal.Parse(at.maxPrice) > decimal.Parse(maxPrice)
                    && at.maxBuyer == maxBuyer)
                {
                    continue;
                }

                at.domain = domain;
                at.parenthash = parenthash;
                at.domainTTL = domainTTL;
                at.endTime = new AuctionTime
                {
                    blockindex = endBlock,
                    blocktime = endBlock == 0 ? 0 : blockindexDict.GetValueOrDefault(blockindex + ""),
                    txid = endBlock == 0 ? "" : txid
                };
                at.maxPrice = maxPrice;
                at.maxBuyer = maxBuyer;
                at.lastTime = new AuctionTime
                {
                    blockindex = lastBlock,
                    blocktime = lastBlock == 0 ? 0 : blockindexDict.GetValueOrDefault(blockindex + ""),
                    txid = lastBlock == 0 ? "":txid
                };
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
                    addwho.totalValue = auctionidIsTo ? addwho.totalValue + value : addwho.totalValue - value;
                    
                } else
                {
                    addwho = new AuctionAddWho();
                    addwho.address = address;
                    addwho.totalValue = value;
                }
                // 相同高度特殊处理
                /*
                if (addwho.lastTime != null 
                    && addwho.lastTime.blockindex == blockindex
                    && addwho.lastTime.txid == txid)
                {
                    continue;
                }*/
                if(auctionidIsTo)
                {
                    addwho.lastTime = new AuctionTime
                    {
                        blockindex = blockindex,
                        blocktime = blockindexDict.GetValueOrDefault(blockindex + ""),
                        txid = txid
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
                // 相同高度特殊处理
                if(at.endAddress == who
                    && at.endTime != null
                    && at.endTime.blockindex == blockindex
                    && at.endTime.txid == txid)
                {
                    continue;
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
                // 相同高度特殊处理
                if(addwho.getdomainTime != null 
                    && addwho.getdomainTime.blockindex == blockindex
                    && addwho.getdomainTime.txid == txid)
                {
                    continue;
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
        
        class AuctionTx
        {
            public ObjectId _id { get; set; }
            public string auctionId { get; set; }
            public string domain { get; set; }
            public string parenthash { get; set; }
            public string fulldomain { get; set; }
            public string domainTTL { get; set; }
            public string auctionState { get; set; }
            public AuctionTime startTime { get; set; }
            public string startAddress { get; set; }
            public string maxPrice { get; set; }
            public string maxBuyer { get; set; }
            public AuctionTime endTime { get; set; }
            public string endAddress { get; set; }
            public AuctionTime lastTime { get; set; }
            public List<AuctionAddWho> addwholist {get; set;}

        }
        class AuctionAddWho
        {
            public string address { get; set; }
            public decimal totalValue { get; set; }
            public AuctionTime lastTime { get; set; }
            public AuctionTime accountTime { get; set; }
            public AuctionTime getdomainTime { get; set; }
            public List<AuctionAddPrice> addpricelist { get; set; }

        }
        class AuctionAddPrice
        {
            public AuctionTime time { get; set; }
            public decimal value { get; set; }
            public string isEnd { get; set; }
        }
        class AuctionTime
        {
            public long blockindex { get; set; }
            public long blocktime { get; set; }
            public string txid { get; set; }
        }
        class AuctionState
        {
            public const string STATE_START = "0101";
            public const string STATE_CONFIRM = "0201";
            public const string STATE_RANDOM = "0301";
            public const string STATE_END = "0401"; // 触发结束、3D/5D到期结束
            public const string STATE_ABORT = "0501";
            public const string STATE_EXPIRED = "0601";
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
            long count = getDataCount(localDbConnInfo, auctionRecordColl, new JObject() { { "contractColl", notifyDomainSellColl } }.ToString());
            if (count <= 0)
            {
                putData(localDbConnInfo, auctionRecordColl, new JObject() { { "contractColl", notifyDomainSellColl }, { "lastBlockindex", blockindex } }.ToString());
            }
            else
            {
                replaceData(localDbConnInfo, auctionRecordColl, new JObject() { { "contractColl", notifyDomainSellColl }, { "lastBlockindex", blockindex } }.ToString(), new JObject() { { "contractColl", notifyDomainSellColl } }.ToString());
            }
        }
        private long getContractHeight(DbConnInfo db, string coll, string filter)
        {
            JArray res = getData(db, coll, filter);
            if (res != null && res.Count > 0)
            {
                return long.Parse(Convert.ToString(res.OrderByDescending(p => int.Parse(p["lastBlockindex"].ToString())).ToArray()[0]["lastBlockindex"]));
            }
            return -1;
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
        private JArray GetDataPagesWithField(DbConnInfo db, string coll, string fieldBson, string sortBson, string findBson)
        {
            return mh.GetDataPagesWithField(db.connStr, db.connDB, coll, fieldBson, 0, 0, sortBson, findBson);
        }

        private long getDataCount(DbConnInfo db, string coll, string filter)
        {
            return mh.GetDataCount(db.connStr, db.connDB, coll, filter);
        }
        private void log(long localHeight, long remoteHeight)
        {
            Console.WriteLine(DateTime.Now + string.Format(" {0}.nnsRegisterSellContract processed at {1}/{2}", name(), localHeight, remoteHeight));
        }
        private void ping()
        {
            LogHelper.ping(batchInterval, name());
        }
    }
}
