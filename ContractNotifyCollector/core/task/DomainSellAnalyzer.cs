using ContactNotifyCollector.lib;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace ContactNotifyCollector.core.task
{
    /// <summary>
    /// 域名拍卖解析器
    /// 
    /// <para>
    /// 对域名的拍卖过程去快照
    /// </para>
    /// 
    /// </summary>
    class DomainSellAnalyzer : ContractTask
    {
        private JObject config;

        public DomainSellAnalyzer(string name): base(name)
        {
        }

        public override void Init(JObject config)
        {
            this.config = config;
            init();
        }

        public override void startTask()
        {
            run();
        }
        

        private const long THREE_DAY_SECONDS = 3 * /*24 * 60 * */60 /*测试时5分钟一天*/* 5;
        private const long TWO_DAY_SECONDS = 2 * /*24 * 60 * */60 /*测试时5分钟一天*/* 5;
        private const long FIVE_DAY_SECONDS = 5 * /*24 * 60 * */60 /*测试时5分钟一天*/ * 5;
        private MongoDBHelper mh = new MongoDBHelper();

        private DbConnInfo localDbConnInfo;
        private DbConnInfo remoteDbConnInfo;
        private DbConnInfo blockDbConnInfo;
        private string domainColl { set; get; }
        private string domainStateColl { set; get; }
        private string domainUserStateColl { set; get; }
        private string domainRecordColl { set; get; }
        private string notifyRecordColl { set; get; }
        private string notifyDomainSellColl { set; get; }
        private string notifyDomainCenterColl { set; get; }
        private int batchSize { set; get; }
        private int batchInterval { set; get; }

        private void init()
        {
            localDbConnInfo = Config.localDbConnInfo;
            remoteDbConnInfo = Config.remoteDbConnInfo;
            blockDbConnInfo = Config.blockDbConnInfo;

            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name()).ToArray()[0]["taskInfo"];
            domainColl = cfg["domainColl"].ToString();
            domainStateColl = cfg["domainStateColl"].ToString();
            domainUserStateColl = cfg["domainUserStateColl"].ToString();
            domainRecordColl = cfg["domainRecordColl"].ToString();
            notifyRecordColl = cfg["notifyRecordColl"].ToString();
            notifyDomainSellColl = cfg["notifyDomainSellColl"].ToString();
            notifyDomainCenterColl = cfg["notifyDomainCenterColl"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());
        }
        private void run()
        {
            while (true)
            {
                ping();
                
                // 获取远程已同步高度
                //JObject filter = new JObject() { { "contractColl", notifyDomainSellColl } };
                JObject filter = new JObject() { { "contractHash", notifyDomainSellColl } };
                long remoteHeight = getContractHeight(remoteDbConnInfo, notifyRecordColl, filter.ToString());
                
                // 获取本地已处理高度
                filter = new JObject() { { "contractColl", notifyDomainSellColl } };
                long localHeight = getContractHeight(localDbConnInfo, domainRecordColl, filter.ToString());

                // 本地高度小于远程高度时才需处理
                if (remoteHeight < localHeight)
                {
                    // 更新过期结束的域名
                    updateExpiredAndEndedDomain(remoteHeight);
                    
                    log(localHeight, remoteHeight);
                    continue;
                }
                //int batchSize = 500;
                for (long index = localHeight; index < remoteHeight; index += batchSize)
                {
                    // 待处理数据
                    JObject queryFilter = new JObject() { { "blockindex", new JObject() { { "$lte", index + batchSize }, { "$gt", index } } } };
                    JObject querySortBy = new JObject() { { "blockindex", 1 } };
                    JObject queryField = new JObject() { { "state", 0 } };
                    JArray queryRes = GetDataPagesWithField(remoteDbConnInfo, notifyDomainSellColl, queryField.ToString(), querySortBy.ToString(), queryFilter.ToString());
                    if (queryRes == null || queryRes.Count() == 0)
                    {
                        updateDomainRecord((index + batchSize < remoteHeight ? index + batchSize: remoteHeight));
                        //updateExpiredAndEndedDomain(index + batchSize);
                        log(index + batchSize, remoteHeight);
                        continue;
                    }

                    // 高度时间列表
                    long[] blockindexArr = queryRes.Select(item => long.Parse(item["blockindex"].ToString())).Distinct().OrderBy(p => p).ToArray();
                    long[] blockindexArrs = new long[] { };
                    blockindexArrs = blockindexArrs.Concat(queryRes.Select(item => long.Parse(item["blockindex"].ToString())).ToArray()).ToArray();
                    blockindexArrs = blockindexArrs.Concat(queryRes.Select(item => long.Parse(item["startBlockSelling"].ToString())).ToArray()).ToArray();
                    blockindexArrs = blockindexArrs.Concat(queryRes.Select(item => long.Parse(item["endBlock"].ToString())).ToArray()).ToArray();
                    blockindexArrs = blockindexArrs.Distinct().OrderBy(p => p).ToArray();
                    Dictionary<string, long> blockindexDict = getBlockTimeByIndex(blockindexArrs.ToArray());
                    Dictionary<string, string> blockindexDictFmt = blockindexDict.ToDictionary(key => key.Key, val => TimeHelper.toBlockindexTimeFmt(val.Value));
                    
                    // 哈希域名列表(父域名-父哈希)
                    string[] phArr = queryRes.Select(item => item["parenthash"].ToString()).Distinct().Where(pItem => pItem != "").ToArray();
                    Dictionary<string, string> phDict = getDomainByHash(phArr);

                    int cnt = blockindexArr.Count();
                    for (int i = 0; i < cnt; ++i)
                    {
                        long blockindex = blockindexArr[i];
                        JToken[] res = queryRes.Where(p => int.Parse(p["blockindex"].ToString()) == blockindex).ToArray();
                        JToken[] r1 = res.Where(p1 => p1["displayName"].ToString() == "addprice").OrderBy(p1 => double.Parse(p1["maxPrice"].ToString())).ThenBy(p1 => p1["getTime"].ToString()).ToArray();
                        JToken[] r2 = res.Where(p2 => p2["displayName"].ToString() == "domainstate"
                            && isEndAuction(p2["endBlock"].ToString())
                            && double.Parse(p2["maxPrice"].ToString()) != 0.0).OrderBy(p2 => p2["getTime"].ToString()).ToArray();

                        // 这里不能直接取最大max和最后bid，会漏掉出价了但即不是max也不是最后bid的who
                        dealR1(r1, blockindex, phDict, blockindexDict, blockindexDictFmt);
                        dealR2(r2, blockindex, phDict, blockindexDict, blockindexDictFmt);

                        // 更新已处理高度
                        updateDomainRecord(blockindex);
                        localHeight = blockindex;
                        log(localHeight, remoteHeight);

                        // 未触发结束标志的域名数据状态处理
                        //updateExpiredAndEndedDomain(blockindex);
                    }
                }// end of batchSize

                log(localHeight, remoteHeight);

                // 未触发结束标志的域名数据状态处理
                updateExpiredAndEndedDomain(remoteHeight);
                
            }
        }

        private void dealR1(JToken[] r1, long blockindex, Dictionary<string, string> phDict, Dictionary<string, long> blockindexDict, Dictionary<string, string> blockindexDictFmt)
        {
            foreach (JToken jt1 in r1)
            {
                string domain = jt1["domain"].ToString();
                string parenthash = jt1["parenthash"].ToString();
                // 全域名
                string parent = phDict.GetValueOrDefault(parenthash);
                string fulldomain = domain + "." + parent;
                // 
                string maxBuyer = jt1["maxBuyer"].ToString();
                double maxPrice = double.Parse(jt1["maxPrice"].ToString());
                // 开标区块
                string startBlockSelling = jt1["startBlockSelling"].ToString();
                // 交易ID
                string txid = jt1["txid"].ToString();
                // 竞拍ID
                string id = jt1["id"].ToString();
                // 出价人
                string who = jt1["who"].ToString();
                // 合约哈希
                //string contractHash = jt1["contractHash"].ToString();

                JObject newJData = new JObject();
                newJData.Add("domain", domain);
                newJData.Add("parenthash", parenthash);
                newJData.Add("fulldomain", fulldomain);
                //newJData.Add("fulldomainhash", fulldomainhash);
                newJData.Add("maxBuyer", maxBuyer);
                newJData.Add("maxPrice", maxPrice);
                newJData.Add("startBlockSelling", long.Parse(startBlockSelling));
                newJData.Add("startBlockSellingTime", blockindexDict.GetValueOrDefault(startBlockSelling));
                newJData.Add("startBlockSellingTimeFmt", blockindexDictFmt.GetValueOrDefault(startBlockSelling));
                newJData.Add("startAuctionTime", blockindexDict.GetValueOrDefault(startBlockSelling));
                newJData.Add("blockindex", blockindex);
                newJData.Add("blockindexTime", blockindexDict.GetValueOrDefault(blockindex + ""));
                newJData.Add("blockindexTimeFmt", blockindexDictFmt.GetValueOrDefault(blockindex + ""));
                newJData.Add("txid", txid);
                newJData.Add("id", id);
                //newJData.Add("contractHash", contractHash);

                // 待计算字段
                newJData.Add("endBlock", "0");
                newJData.Add("endBlockTime", "");
                newJData.Add("endBlockTimeFmt", "");
                newJData.Add("auctionSpentTime", "0");
                newJData.Add("auctionState", "1");
                newJData.Add("owner", "");
                newJData.Add("ttl", "0");

                // 最后出价者
                newJData.Add("lastWho", who);
                newJData.Add("lastBlockindex", blockindex);
                newJData.Add("lastBlockindexTime", blockindexDict.GetValueOrDefault(blockindex + ""));
                newJData.Add("lastBlockindexTimeFmt", blockindexDictFmt.GetValueOrDefault(blockindex + ""));
                newJData.Add("lastTxid", txid);
                newJData.Add("endBlockindex", "");          // domainstate类型更新该字段
                newJData.Add("endBlockindexTime", "");      // domainstate类型更新该字段
                newJData.Add("endBlockindexTimeFmt", "");   // domainstate类型更新该字段
                newJData.Add("endTxid", "");                // domainstate类型更新该字段
                                                            // 1.域名维度-domainState
                JObject domainFilter = new JObject() { { "fulldomain", fulldomain } };
                JArray domainRes = getData(localDbConnInfo, domainStateColl, domainFilter.ToString());
                if (domainRes == null || domainRes.Count == 0)
                {
                    putData(localDbConnInfo, domainStateColl, newJData.ToString());
                }
                else
                {
                    JObject hasJData = (JObject)domainRes[0];
                    string hasId = hasJData["id"].ToString();
                    if (id != hasId)
                    {
                        replaceData(localDbConnInfo, domainStateColl, newJData.ToString(), domainFilter.ToString());
                    }
                    else
                    {
                        long hasLastBlockindex = long.Parse(hasJData["lastBlockindex"].ToString());
                        double hasMaxPrice = double.Parse(hasJData["maxPrice"].ToString());
                        if (blockindex >= hasLastBlockindex)
                        {
                            newJData.Remove("id");
                            newJData.Remove("fulldoamin");
                            newJData.Remove("fulldoaminhash");
                            newJData.Remove("startBlockSelling");
                            newJData.Remove("startBlockSellingTime");
                            newJData.Remove("startBlockSellingTimeFmt");
                            if (!(who == maxBuyer && maxPrice >= hasMaxPrice || (maxBuyer == "")))
                            {
                                newJData.Remove("maxBuyer");
                                newJData.Remove("maxPrice");
                                newJData.Remove("blockindex");
                                newJData.Remove("blockindexTime");
                                newJData.Remove("blockindexTimeFmt");
                                newJData.Remove("txid");
                            }
                            updateData(localDbConnInfo, domainStateColl, newJData, domainFilter.ToString());
                        }
                    }

                }
                // 2.用户维度-domainUserState
                JObject whoJData = new JObject() { { "who", who }, { "fulldomain", fulldomain }, /*{ "fulldomainhash", fulldomainhash },*/ { "id", id } };
                JObject whoFilter = new JObject() { { "who", who }, { "fulldomain", fulldomain } };
                JArray whoRes = getData(localDbConnInfo, domainUserStateColl, whoFilter.ToString());
                if (whoRes == null || whoRes.Count() == 0)
                {
                    putData(localDbConnInfo, domainUserStateColl, whoJData.ToString());
                }
                else
                {
                    JObject hasJData = (JObject)whoRes[0];
                    if (id != hasJData["id"].ToString())
                    {
                        whoJData.Remove("who");
                        whoJData.Remove("fulldomain");
                        whoJData.Remove("contractHash");
                        updateData(localDbConnInfo, domainUserStateColl, whoJData, whoFilter.ToString());
                    }

                }
            }
        }
        private void dealR2(JToken[] r2, long blockindex, Dictionary<string, string> phDict, Dictionary<string, long> blockindexDict, Dictionary<string, string> blockindexDictFmt)
        {
            r2.GroupBy(p => p["domain"].ToString(), (kk, gg) =>
            {
                string domain = kk.ToString();
                gg.GroupBy(pp => pp["parenthash"].ToString(), (k2, g2) =>
                {
                    string parenthash = k2.ToString();
                    JToken theOnlyOneItem = g2.ToArray()[0];

                    // 获取全域名
                    string parent = phDict.GetValueOrDefault(parenthash);
                    string fulldomain = domain + "." + parent;
                    //string fulldomainhash = nameHashFull(domain, parent);
                    //string fulldomainhash = nameHashFull(g2.ToArray()[0]["domain"].ToString(), parent);
                    //
                    string startBlockSelling = theOnlyOneItem["startBlockSelling"].ToString();
                    string endBlock = theOnlyOneItem["endBlock"].ToString();
                    long startBlockSellingTime = blockindexDict.GetValueOrDefault(startBlockSelling);
                    long endBlockTime = blockindexDict.GetValueOrDefault(endBlock);
                    // 
                    JObject ownerObj = getOwner(domain, parenthash);
                    string owner = ownerObj["owner"].ToString();
                    long ttl = long.Parse(ownerObj["ttl"].ToString());

                    // 合约哈希
                    //string contractHash = theOnlyOneItem["contractHash"].ToString();

                    JObject updateJData = new JObject();
                    updateJData.Add("auctionState", "0");
                    updateJData.Add("endBlock", endBlock);
                    updateJData.Add("endBlockTime", endBlockTime);
                    updateJData.Add("endBlockTimeFmt", blockindexDictFmt.GetValueOrDefault(endBlock));
                    updateJData.Add("auctionSpentTime", endBlockTime - startBlockSellingTime - THREE_DAY_SECONDS);
                    updateJData.Add("owner", owner);
                    updateJData.Add("ttl", ttl); // 与owner一起获取

                    updateJData.Add("endBlockindex", blockindex);
                    updateJData.Add("endBlockindexTime", blockindexDict.GetValueOrDefault(blockindex + ""));
                    updateJData.Add("endBlockindexTimeFmt", blockindexDictFmt.GetValueOrDefault(blockindex + ""));
                    updateJData.Add("endTxid", theOnlyOneItem["txid"].ToString());  // 触发结束的txid


                    // 1.域名维度-domainState
                    JObject domainFilter = new JObject() { { "fulldomain", fulldomain } };
                    JArray domainRes = getData(localDbConnInfo, domainStateColl, domainFilter.ToString());
                    if (domainRes != null && domainRes.Count != 0)
                    {
                        updateData(localDbConnInfo, domainStateColl, updateJData, domainFilter.ToString());
                    }
                    // 2.用户维度-domainUserState
                    // null

                    return new JObject();
                }).ToArray();
                return new JObject();
            }).ToArray();   // end of r2.GroupBy...
        }

        private void updateExpiredAndEndedDomain(long blockindex)
        {
            // 非终态数据state更新 + 终态数据owner更新
            /**
               newJData.Add("endBlock", "0");
               newJData.Add("endBlockTime", "");
               newJData.Add("auctionSpentTime", "0");
               newJData.Add("auctionState", "1");
               newJData.Add("owner", "");
               newJData.Add("ttl", "0");
               */

            // 更新1,2 ==> auctionSpentTime + auctionState
            JObject updateFilter = toFilter(new string[] { "1", "2" }, "auctionState");
            JArray updateRes = getData(localDbConnInfo, domainStateColl, updateFilter.ToString());
            if(updateRes != null && updateRes.Count() != 0)
            {

            
            foreach (JObject jo in updateRes)
            {
                //
                string fulldomain = jo["fulldomain"].ToString();
                string endBlock = jo["endBlock"].ToString();
                string maxBuyer = jo["maxBuyer"].ToString();
                // 开标时间
                //long startAuctionTime = long.Parse(jo["startAuctionTime"].ToString());
                long startAuctionTime = long.Parse(jo["startBlockSellingTime"].ToString());
                // 竞拍已耗时(竞拍中显示)
                long startAuctionSpentTime = getAuctionSpentTime(startAuctionTime);

                // 最后加价时间(计算第三天有无出价)
                long lastBidTime = long.Parse(jo["lastBlockindexTime"].ToString());
                //long lastBidSpentTime = getAuctionSpentTime(lastBidTime);
                long lastBidSpentTimeFromStartAuction = lastBidTime - startAuctionTime;

                    // 竞拍状态=>已过时间
                    long auctionSpentTime = 0;
                string auctionState = getAuctionState(startAuctionSpentTime, endBlock, maxBuyer == "", lastBidSpentTimeFromStartAuction);
                if(auctionState == "0")
                {
                    // 结束
                    //continue;

                }
                else if(auctionState == "1")
                {
                    auctionSpentTime = getAuctionSpentTime(startAuctionTime);
                }
                else if (auctionState == "2")
                {
                    auctionSpentTime = getAuctionSpentTime(startAuctionTime) - THREE_DAY_SECONDS;
                }
                else if (auctionState == "3")
                {
                    auctionSpentTime = THREE_DAY_SECONDS;
                }
                else if (auctionState == "4")
                {
                    // 流拍
                    //continue;
                }
                else if (auctionState == "5")
                {
                    auctionSpentTime = TWO_DAY_SECONDS;
                }
                JObject domainFilter = new JObject() { { "fulldomain", fulldomain } };
                JObject updateJData = new JObject() { { "auctionState", auctionState }, { "auctionSpentTime", auctionSpentTime } };
                updateData(localDbConnInfo, domainStateColl, updateJData, domainFilter.ToString());
            }
            }

            // 更新0,3,5 ==> owner + ttl
            updateFilter = toFilter(new string[] { "0", "3", "5" }, "auctionState");
            //updateFilter.Add("owner", new JObject() { { "$ne", "" } });
            //updateFilter.Add("ttl", new JObject() { { "$ne", "0" } });
            updateRes = getData(localDbConnInfo, domainStateColl, updateFilter.ToString());
            if (updateRes == null || updateRes.Count() == 0) return;
            foreach (JObject jo in updateRes)
            {
                string fulldomain = jo["fulldomain"].ToString();
                if(fulldomain == "apple.neo")
                {
                    Console.WriteLine("");
                }
                string domain = jo["domain"].ToString();
                string parenthash = jo["parenthash"].ToString();
                JObject ownerObj = getOwner(domain, parenthash);
                string owner = ownerObj["owner"].ToString();
                long ttl = long.Parse(ownerObj["ttl"].ToString());
                JObject domainFilter = new JObject() { { "fulldomain", fulldomain } };
                JObject updateJData = new JObject() { { "owner", owner }, { "ttl", ttl } };
                JObject fieldJData = new JObject() { { "owner", 1 }, { "ttl", 1 } };
                JArray res = getDataWithField(localDbConnInfo, domainStateColl, fieldJData.ToString(), domainFilter.ToString());
                if(res != null && res.Count() != 0)
                {
                    if(res[0]["owner"].ToString() != owner || long.Parse(res[0]["ttl"].ToString()) != ttl)
                    {
                        updateData(localDbConnInfo, domainStateColl, updateJData, domainFilter.ToString());
                    }
                }
            }
           
        }
        private string nameHashFull(string domain, string parent)
        {
            return DomainHelper.nameHashFull(domain, parent).ToString();
        }
        private void log(long localHeight, long remoteHeight)
        {
            Console.WriteLine(DateTime.Now + string.Format(" {0}.nnsRegisterSellContract processed at {1}/{2}", name(), localHeight, remoteHeight));
        }

        private void updateDomainRecord(long blockindex)
        {
            long count = getDataCount(localDbConnInfo, domainRecordColl, new JObject() { { "contractColl", notifyDomainSellColl } }.ToString());
            if (count <= 0)
            {
                putData(localDbConnInfo, domainRecordColl, new JObject() { { "contractColl", notifyDomainSellColl }, { "lastBlockindex", blockindex } }.ToString());
            }
            else
            {
                replaceData(localDbConnInfo, domainRecordColl, new JObject() { { "contractColl", notifyDomainSellColl }, { "lastBlockindex", blockindex } }.ToString(), new JObject() { { "contractColl", notifyDomainSellColl } }.ToString());
            }
        }
        private JObject getOwner(string domain, string parenthash)
        {
            string owner = "";
            string ttl = "0";
            JObject ownerFilter = new JObject() { { "domain", domain }, { "parenthash", parenthash } };
            JObject fieldFilter = new JObject() { { "domain", 1 }, { "parenthash", 1 }, { "owner", 1 }, { "blockindex", 1 }, { "TTL", 1 } };
            JObject sortBy = new JObject() { { "blockindex", -1 } };
            JArray ownerRes = mh.GetDataPagesWithField(remoteDbConnInfo.connStr, remoteDbConnInfo.connDB, notifyDomainCenterColl, fieldFilter.ToString(),1,1,sortBy.ToString(), ownerFilter.ToString());
            if (ownerRes != null && ownerRes.Count > 0)
            {
                owner = ownerRes[0]["owner"].ToString();
                ttl = ownerRes[0]["TTL"].ToString();
            }
            return new JObject() { {"owner", owner }, { "ttl", ttl } };
        }
        private long getAuctionSpentTime(long startAuctionTime)
        {
            long timeStamp = TimeHelper.GetTimeStamp();
            return timeStamp - startAuctionTime;
        }

        private string getAuctionState(long auctionSpentTime, string blockHeightStrEd, bool noAnyAddPriceFlag, long lastBidSpentTimeFromStartAuction = 0)
        {
            string auctionState = "";
            if (auctionSpentTime < THREE_DAY_SECONDS)
            {
                // 竞拍中
                auctionState = "1";// "Fixed period";
            }
            else
            {
                if (!isEndAuction(blockHeightStrEd))
                {
                    // 随机
                    auctionState = "2";// "Random period";

                    if (noAnyAddPriceFlag)
                    {
                        // 流拍：超过三天无任何人出价则流拍
                        auctionState = "4";
                    }
                    // else if (auctionSpentTime - lastAddPriceAuctionSpentTime <= TWO_DAY_SECONDS)
                    else if (lastBidSpentTimeFromStartAuction <= TWO_DAY_SECONDS)
                    {
                        // 3D超时结束：超过三天且第三天无出价则结束
                        auctionState = "3";
                    }
                    else if (auctionSpentTime > FIVE_DAY_SECONDS)
                    {
                        // 5D超时结束：超过三天且第三天有出价且开拍时间超过5天时间则结束
                        auctionState = "5";
                    }
                }
                else
                {
                    // 结束
                    auctionState = "0";// "Ended";
                }
            }
            return auctionState;
        }
        
        private bool isEndAuction(string blockHeightStrEd)
        {
            return blockHeightStrEd != null && !blockHeightStrEd.Equals("") && !blockHeightStrEd.Equals("0");
        }
        
        private Dictionary<string, string> getDomainByHash(string[] parentHashArr)
        {
            //JObject queryFilter = toFilter(parentHashArr, "namehash");
            JObject queryFilter = new JObject() { { "$or", new JArray() { parentHashArr.Select(item => new JObject() { { "namehash", item }, { "root", "1" } }).ToArray() } } };
            JObject sortFilter = new JObject() { { "blockindex", -1 } };
            JObject fieldFilter = toReturn(new string[] { "namehash", "domain" });
            //JArray res = mh.GetDataPagesWithField(remoteDbConnInfo.connStr, remoteDbConnInfo.connDB, notifyDomainCenterColl, fieldFilter.ToString(), 1,1, sortFilter.ToString(), queryFilter.ToString());
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
            JObject queryFilter = toFilter(blockindexArr, "index", "$or");
            JObject returnFilter = toReturn(new string[] { "index", "time" });
            JArray blocktimeRes = mh.GetDataWithField(blockDbConnInfo.connStr, blockDbConnInfo.connDB, "block", returnFilter.ToString(), queryFilter.ToString());
            return blocktimeRes.ToDictionary(key => key["index"].ToString(), val => long.Parse(val["time"].ToString()));
        }
        private JObject toFilter(long[] blockindexArr, string field, string logicalOperator = "$or")
        {
            if (blockindexArr.Count() == 1)
            {
                return new JObject() { { field, blockindexArr[0] } };
            }
            return new JObject() { { logicalOperator, new JArray() { blockindexArr.Select(item => new JObject() { { field, item } }).ToArray() } } };
        }
        private JObject toFilter(string[] blockindexArr, string field, string logicalOperator = "$or")
        {
            if (blockindexArr.Count() == 1)
            {
                return new JObject() { { field, blockindexArr[0] } };
            }
            return new JObject() { { logicalOperator, new JArray() { blockindexArr.Select(item => new JObject() { { field, item } }).ToArray() } } };
        }
        private JObject toReturn(string[] fieldArr)
        {
            JObject obj = new JObject();
            foreach (var field in fieldArr)
            {
                obj.Add(field, 1);
            }
            return obj;
        }
        private JObject toSort(string[] fieldArr, bool order = false)
        {
            int flag = order ? 1 : -1;
            JObject obj = new JObject();
            foreach (var field in fieldArr)
            {
                obj.Add(field, flag);
            }
            return obj;
        }
        

        private bool hasExistDomain(string fulldomain)
        {
            JObject data = new JObject() { { "fulldomain", fulldomain } };
            return mh.GetDataCount(localDbConnInfo.connStr, localDbConnInfo.connDB, domainColl, data.ToString()) > 0;
        }
        private void insertDomain(string fulldomain)
        {
            JObject data = new JObject() { { "fulldomain", fulldomain } };
            mh.PutData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainColl, data.ToString());
        }
        
        private long getContractHeight(DbConnInfo db, string coll, string filter)
        {
            JArray res = getData(db, coll, filter);
            if(res != null && res.Count > 0)
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

        private void ping()
        {
            LogHelper.ping(batchInterval, name());
        }
        
    }
    
}
