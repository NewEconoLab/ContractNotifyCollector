using ContractNotifyCollector.helper;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace ContractNotifyCollector.core.task
{
    /// <summary>
    /// 域名中心解析器
    /// 
    /// <para>
    /// 将不同表中的域名数据，按照域名维度将拥有者、映射内容等汇总入同一张表
    /// </para>
    /// 
    /// </summary>
    class DomainCenterAnalyzer : ContractTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo localConn;
        private DbConnInfo remoteConn;
        private DbConnInfo blockConn;
        private string notifyRecordColl;
        private string domainCenterCol;
        private string domainResolverCol;
        private string domainFixedSellingCol;
        private string domainCreditCol;
        private string domainRecord;
        private string domainOwnerCol;
        private string nnsSellingAddr;
        private string dexContractHash;
        private string dexSellingAddr;
        private int batchSize;
        private int batchInterval;
        private bool initSuccFlag = false;
        private bool hasCreateIndex = false;
        private Dictionary<string, string> parenthashDict = new Dictionary<string, string>();

        public DomainCenterAnalyzer(string name):base(name)
        {
        }

        public override void initConfig(JObject config)
        {
            //JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name()).ToArray()[0]["taskInfo"];
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];

            notifyRecordColl = cfg["notifyRecordColl"].ToString();
            domainCenterCol = cfg["domainCenterCol"].ToString();
            domainResolverCol = cfg["domainResolverCol"].ToString();
            domainFixedSellingCol = cfg["domainFixedSellingCol"].ToString();
            domainCreditCol = cfg["domainCreditCol"].ToString();
            domainRecord = cfg["domainRecord"].ToString();
            domainOwnerCol = cfg["domainOwnerCol"].ToString() + "_test";
            nnsSellingAddr = cfg["nnsSellingAddr"].ToString();
            dexContractHash = cfg["dexContractHash"].ToString();
            dexSellingAddr = cfg["dexSellingAddr"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());

            // db info
            localConn = Config.localDbConnInfo;
            remoteConn = Config.notifyDbConnInfo;
            blockConn = Config.blockDbConnInfo;
            initSuccFlag = true;
        }

        public override void startTask()
        {
            if (!initSuccFlag) return;
            while (true)
            {
                try
                {
                    ping();
                    process();
                    //processDomainDex();
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
            long blockHeight = getBlockHeight();
            if (blockHeight < remoteHeight)
            {
                remoteHeight = blockHeight;
            }

            // 获取本地已处理高度
            long localHeight = getLocalHeight();
            localHeight = 2775815;
            localHeight = 2790178;
            localHeight = 2839178;
            localHeight = 2848178;
            localHeight = 3094178;
            localHeight = 3546863;

            batchSize = 2000;
            hasCreateIndex = true;

            // 
            if (remoteHeight <= localHeight)
            {
                log(localHeight, remoteHeight);
                return;
            }
            for (long index = localHeight; index <= remoteHeight; index += batchSize)
            {
                long nextIndex = index + batchSize;
                long endIndex = nextIndex < remoteHeight ? nextIndex : remoteHeight;

                // 域名中心
                JObject queryFilter = new JObject() { { "blockindex", new JObject() { { "$gt", index }, { "$lte", endIndex } } } };
                JObject queryField = new JObject() { { "state", 0 } };
                JArray queryRes = mh.GetDataWithField(remoteConn.connStr, remoteConn.connDB, domainCenterCol, queryField.ToString(), queryFilter.ToString());
                
                if (queryRes != null && queryRes.Count() > 0)
                {
                    //processDomainCenter(queryRes);
                    var ja = queryRes.Where(p => 
                        p["owner"].ToString() != nnsSellingAddr
                        && p["owner"].ToString() != dexSellingAddr
                        ).ToArray();
                    if(ja != null && ja.Count() > 0)
                    {
                        processDomainCenter(ja);
                    }
                    
                }

                // 解析器
                queryFilter = new JObject() { { "blockindex", new JObject() { { "$gt", index }, { "$lte", endIndex } } }, { "protocol", "addr" } };
                queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, domainResolverCol, queryFilter.ToString());
                if(queryRes != null && queryRes.Count() > 0)
                {
                    procesDomainResolver(queryRes);
                }

                // 域名出售信息
                queryFilter = new JObject() { { "blockindex", new JObject() { { "$gt", index }, { "$lte", endIndex } } },
                    { "$or", new JArray{
                        new JObject() {{"displayName", "NNSfixedSellingLaunched" } },
                        new JObject() {{"displayName", "NNSfixedSellingDiscontinued" } },
                        new JObject() {{"displayName", "NNSfixedSellingBuy" } }
                                    }
                    }
                };
                queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, domainFixedSellingCol, queryFilter.ToString());
                if (queryRes != null && queryRes.Count() > 0)
                {
                    processDomainFixedSelling(queryRes);
                }

                // 地址绑定信息
                queryFilter = new JObject() { { "blockindex", new JObject() { { "$gt", index }, { "$lte", endIndex } } } };
                queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, domainCreditCol, queryFilter.ToString());
                if (queryRes != null && queryRes.Count() > 0)
                {
                    processDomainCredit(queryRes);
                }

                //updateRecord(endIndex);
                log(endIndex, remoteHeight);
                if (!hasCreateIndex) break ;
            }

            // 添加索引
            if (hasCreateIndex) return;
            mh.setIndex(localConn.connStr, localConn.connDB, domainOwnerCol, "{'namehash':1}", "i_namehash");
            mh.setIndex(localConn.connStr, localConn.connDB, domainOwnerCol, "{'owner':1}", "i_owner");
            mh.setIndex(localConn.connStr, localConn.connDB, domainOwnerCol, "{'domain':1,'parenthash':1}", "i_domain_parenthash");
            mh.setIndex(localConn.connStr, localConn.connDB, domainOwnerCol, "{'owner':1, 'type':1}", "i_owner_type");
            mh.setIndex(localConn.connStr, localConn.connDB, domainOwnerCol, "{'owner':1, 'bindflag':1}", "i_owner_bindflag");
            hasCreateIndex = true;
        }

        private void processDomainCenter(JToken[] res)
        {
            var pRes = res.GroupBy(p => p["namehash"].ToString(), (k, g) => new { namehash = k.ToString(), item = g.OrderByDescending(pg => long.Parse(pg["blockindex"].ToString())).OrderByDescending(pg => long.Parse(pg["TTL"].ToString())).First() }).ToArray();
            foreach (var pItem in pRes)
            {
                var namehash = pItem.namehash;
                var item = pItem.item;
                var findStr = new JObject { { "namehash", namehash } }.ToString();
                var queryRes = mh.GetData(localConn.connStr, localConn.connDB, domainOwnerCol, findStr);
                if(queryRes.Count == 0)
                {
                    var jo = (JObject)item;
                    jo.Remove("_id");
                    jo["TTL"] = long.Parse(jo["TTL"].ToString());
                    jo["newdomainIndex"] = long.Parse(jo["blockindex"].ToString());
                    jo["fulldomain"] = getFullDomain(jo["domain"].ToString(), jo["parenthash"].ToString());
                    jo["protocol"] = "";
                    jo["data"] = "";
                    jo["bindflag"] = "0";
                    mh.PutData(localConn.connStr, localConn.connDB, domainOwnerCol, jo.ToString());
                    continue;
                }

                var olddata = queryRes[0];
                bool flag = false;
                var updateJo = new JObject();
                var tp = item["blockindex"].ToString();
                if (olddata["blockindex"].ToString() != tp)
                {
                    updateJo["blockindex"] = long.Parse(tp);
                    flag = true;
                }
                tp = item["txid"].ToString();
                if (olddata["txid"].ToString() != tp)
                {
                    updateJo["txid"] = tp;
                    flag = true;
                }
                tp = item["vmstate"].ToString();
                if (olddata["vmstate"].ToString() != tp)
                {
                    updateJo["vmstate"] = tp;
                    flag = true;
                }
                tp = item["contractHash"].ToString();
                if (olddata["contractHash"].ToString() != tp)
                {
                    updateJo["contractHash"] = tp;
                    flag = true;
                }
                tp = item["owner"].ToString();
                if (olddata["owner"].ToString() != tp)
                {
                    updateJo["owner"] = tp;
                    flag = true;
                }
                tp = item["register"].ToString();
                if (olddata["register"].ToString() != tp)
                {
                    updateJo["register"] = tp;
                    flag = true;
                }
                tp = item["resolver"].ToString();
                if (olddata["resolver"].ToString() != tp)
                {
                    updateJo["resolver"] = tp;
                    flag = true;
                }
                tp = item["parentOwner"].ToString();
                if (olddata["parentOwner"].ToString() != tp)
                {
                    updateJo["parentOwner"] = tp;
                    flag = true;
                }
                tp = item["newdomain"].ToString();
                if (olddata["newdomain"].ToString() != tp)
                {
                    updateJo["newdomain"] = tp;
                    if("1" == tp)
                    {
                        updateJo["newdomainIndex"] = long.Parse(item["blockindex"].ToString());
                        updateJo["protocol"] = "";
                        updateJo["data"] = "";
                    }
                    flag = true;
                }

                var newTTL = long.Parse(item["TTL"].ToString());
                var oldTTL = long.Parse(olddata["TTL"].ToString());
                if (newTTL > oldTTL)
                {
                    updateJo["TTL"] = newTTL;
                    flag = true;
                }
                if (flag)
                {
                    var updateStr = new JObject { { "$set", updateJo } }.ToString();
                    mh.UpdateData(localConn.connStr, localConn.connDB, domainOwnerCol, updateStr, findStr);
                }

            }
        }
        private Dictionary<string, string> domainDict = new Dictionary<string, string>();
        private string getFullDomain(string domain, string parenthash)
        {
            string key = parenthash;
            if(!domainDict.ContainsKey(key))
            {
                if(key == "")
                {
                    domainDict.Add(key, "");
                } else
                {
                    var findStr = new JObject { { "namehash", key } }.ToString();
                    var fieldStr = new JObject { { "domain", 1 } }.ToString();
                    var queryRes = mh.GetDataPagesWithField(localConn.connStr, localConn.connDB, domainOwnerCol, fieldStr, 1, 1, "{}", findStr);
                    if (queryRes.Count == 0) throw new Exception("Not find domain by namehash:"+parenthash);

                    var root = queryRes[0]["domain"].ToString();
                    domainDict.Add(key, root);
                }
            }
            var val = domainDict.GetValueOrDefault(key);
            if (val == "") return domain;
            return domain + "." + val;
        }
        
        private void procesDomainResolver(JArray res)
        {
            var pRes = res.GroupBy(p => p["namehash"].ToString(), (k, g) => new { namehash = k.ToString(), item = g.OrderByDescending(pg => long.Parse(pg["blockindex"].ToString())).First() }).ToArray();
            foreach(var pItem in pRes)
            {
                var namehash = pItem.namehash;
                var item = pItem.item;
                var findStr = new JObject { { "namehash", namehash } }.ToString();
                var queryRes = mh.GetData(localConn.connStr, localConn.connDB, domainOwnerCol, findStr);
                if (queryRes.Count == 0) continue;
                var qItem = queryRes[0];

                bool flag = false;
                var updateJo = new JObject();

                var blockindex = long.Parse(item["blockindex"].ToString());
                if(long.Parse(qItem["newdomainIndex"].ToString()) > blockindex)
                {
                    continue;
                }

                var newProtocol = item["protocol"].ToString();
                if(qItem["protocol"].ToString() != newProtocol)
                {
                    updateJo.Add("protocol", newProtocol);
                    flag = true;
                }
                var newData = item["data"].ToString();
                if (qItem["data"].ToString() != newData)
                {
                    updateJo.Add("data", newData);
                    flag = true;
                }
                if(flag)
                {
                    var updateStr = new JObject { { "$set", updateJo } }.ToString();
                    mh.UpdateData(localConn.connStr, localConn.connDB, domainOwnerCol, updateStr, findStr);
                }
            }
        }


        private void processDomainCenter_(JToken[] domainCenterRes)
        {
            var pRes = domainCenterRes.GroupBy(p => p["namehash"].ToString(), (k, g) => new { namehash = k.ToString(), item = g.OrderByDescending(pg => long.Parse(pg["blockindex"].ToString())).First() }).ToArray();
            foreach(var item in pRes)
            {
                string namehash = item.namehash;
                JObject jo = (JObject)item.item;
                long ttl = long.Parse(jo["TTL"].ToString());
                jo.Remove("TTL");
                jo.Add("TTL", ttl);

                string findStr = new JObject() { {"namehash", namehash } }.ToString();
                long cnt = mh.GetDataCount(localConn.connStr, localConn.connDB, domainOwnerCol, findStr);
                if (cnt > 0)
                {
                    jo.Remove("_id");
                    JArray res = mh.GetData(localConn.connStr, localConn.connDB, domainOwnerCol, findStr);
                    JObject hasJo = (JObject)res[0];
                    if (hasJo["blockindex"].ToString() != jo["blockindex"].ToString()
                        || hasJo["register"].ToString() != jo["register"].ToString())
                    {
                        // TODO
                        if (hasJo["resolver"].ToString() != jo["resolver"].ToString())
                        {
                            string findstr = new JObject() { { "namehash", hasJo["namehash"] } }.ToString();
                            string sortstr = new JObject() { { "blockindex", -1 } }.ToString();
                            string fieldstr = MongoFieldHelper.toReturn(new string[] { "protocol", "data" }).ToString();
                            JArray resolveData = mh.GetDataPagesWithField(remoteConn.connStr, remoteConn.connDB, domainResolverCol, fieldstr, 1, 1, sortstr, findstr);
                            if (resolveData != null && resolveData.Count() > 0)
                            {
                                jo.Add("protocol", resolveData[0]["protocol"]);
                                jo.Add("data", resolveData[0]["data"]);
                            }
                        }
                        mh.UpdateData(localConn.connStr, localConn.connDB, domainOwnerCol, new JObject() { { "$set", jo } }.ToString(), findStr);
                    }
                }
                else
                {
                    string parenthash = jo["parenthash"].ToString();
                    if (jo["root"].ToString() == "0" && parenthash != "")
                    {
                        var parentdomain = parenthashDict.GetValueOrDefault(parenthash);
                        if (parentdomain == null || parentdomain == "")
                        {
                            string subFindStr = new JObject() { { "namehash", parenthash } }.ToString();
                            string subFieldStr = new JObject() { { "domain", 1 } }.ToString();
                            string subSortStr = new JObject() { {"blockindex",-1 } }.ToString();
                            parentdomain = mh.GetDataPagesWithField(localConn.connStr, localConn.connDB, domainOwnerCol, subFieldStr, 1, 1, subSortStr, subFindStr)[0]["domain"].ToString();
                            parenthashDict.Add(parenthash, parentdomain);
                        }
                        if(parentdomain != "")
                        {
                            jo.Add("fulldomain", jo["domain"].ToString() + "." + parentdomain);
                        } else
                        {
                            jo.Add("fulldomain", jo["domain"]);
                        }
                        
                    } else
                    {
                        jo.Add("fulldomain", jo["domain"]);
                    }


                    jo.Add("protocol", "");
                    jo.Add("data", "");
                    jo.Add("bindflag","0");
                    mh.PutData(localConn.connStr, localConn.connDB, domainOwnerCol, jo.ToString());
                }
            }

        }
        private void procesDomainResolver_(JArray domainResolverRes)
        {
            var pRes = domainResolverRes.GroupBy(p => p["namehash"].ToString(), (k, g) => new { namehash = k.ToString(), item = g.OrderByDescending(pg => long.Parse(pg["blockindex"].ToString())).First() }).ToArray();
            foreach (var item in pRes)
            {
                string namehash = item.namehash;
                JObject jo = (JObject)item.item;
                string findStr = new JObject() { { "namehash", namehash }, { "blockindex", new JObject() { { "$lte", long.Parse(jo["blockindex"].ToString()) } } } }.ToString();
                long cnt = mh.GetDataCount(localConn.connStr, localConn.connDB, domainOwnerCol, findStr);
                if (cnt > 0)
                {
                    //JObject updateData = new JObject();
                    //updateData.Add("protocol", "addr");
                    //updateData.Add("data", jo["data"].ToString());
                    //mh.UpdateData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, new JObject() { { "$set", updateData } }.ToString(), findStr);
                    JArray res = mh.GetData(localConn.connStr, localConn.connDB, domainOwnerCol, findStr);
                    JObject hasJo = (JObject)res[0];
                    if (hasJo["protocol"].ToString() != "addr"
                        || hasJo["data"].ToString() != jo["data"].ToString())
                    {
                        JObject updateData = new JObject();
                        updateData.Add("protocol", "addr");
                        updateData.Add("data", jo["data"].ToString());
                        mh.UpdateData(localConn.connStr, localConn.connDB, domainOwnerCol, new JObject() { { "$set", updateData } }.ToString(), findStr);
                    }
                }
            }

        }
        
        private void processDomainFixedSelling(JArray domainSellingRes)
        {
            var res = domainSellingRes.GroupBy(p => p["fullDomain"].ToString(), (k, g) =>
            {
                return g.OrderByDescending(pg => long.Parse(pg["blockindex"].ToString())).ToArray()[0];
            }).ToList();
            var indexArr = res.Select(p => long.Parse(p["blockindex"].ToString())).Distinct().ToArray();
            var indexDict = getBlockTime(indexArr);

            foreach(JObject jo in res)
            {
                string type = jo["displayName"].ToString();
                string seller = jo["seller"].ToString(); // owner
                string price = jo["price"].ToString();
                string fullHash = jo["fullHash"].ToString();
                long launchTime = indexDict.GetValueOrDefault(long.Parse(jo["blockindex"].ToString()));

                string findStr = new JObject() { { "namehash", fullHash} }.ToString();
                string updateStr = new JObject() { {"$set", new JObject() {
                    { "type", type},
                    { "seller", seller},
                    { "price", price},
                    { "launchTime", launchTime},
                } } }.ToString();
                mh.UpdateData(localConn.connStr, localConn.connDB, domainOwnerCol, updateStr, findStr);
            }


        }

        private void processDomainCredit(JArray domainCreditRes)
        {
            var res = domainCreditRes.GroupBy(p => p["addr"].ToString(), (k, g) =>
            {
                return g.OrderByDescending(pg => long.Parse(pg["blockindex"].ToString())).ToArray()[0];
            }).ToList();
            foreach(JObject jo in res)
            {
                /*
                string findStr = new JObject() { { "owner", jo["addr"] }, { "bindflag", "1" } }.ToString();
                string updateStr = new JObject() { {"$set", new JObject() { { "bindflag", "0"}  } } }.ToString();
                mh.UpdateData(localConn.connStr, localConn.connDB, domainOwnerCol, updateStr, findStr);
                if(jo["displayName"].ToString() == "addrCreditRegistered")
                {
                    findStr = new JObject() { { "namehash", jo["namehash"] } }.ToString();
                    updateStr = new JObject() { { "$set", new JObject() { { "bindflag", "1" } } } }.ToString();
                    mh.UpdateData(localConn.connStr, localConn.connDB, domainOwnerCol, updateStr, findStr);
                }
                */
                var findStr = new JObject() { { "owner", jo["addr"] }}.ToString();
                var queryRes = mh.GetData(localConn.connStr, localConn.connDB, domainOwnerCol, findStr);
                if (queryRes.Count == 0) continue;

                var qItem = queryRes[0];
                var bindflag = qItem["bindflag"].ToString();
                var displayName = jo["displayName"].ToString();
                bool flag = false;
                if(bindflag == "1" && displayName != "addrCreditRegistered")
                {
                    bindflag = "0";
                    flag = true;
                }
                if(bindflag == "0" && displayName == "addrCreditRegistered")
                {
                    bindflag = "1";
                    flag = true;
                }
                if(flag)
                {
                    findStr = new JObject() { { "namehash", jo["namehash"] } }.ToString();
                    var updateStr = new JObject() { { "$set", new JObject() { { "bindflag", bindflag } } } }.ToString();
                    mh.UpdateData(localConn.connStr, localConn.connDB, domainOwnerCol, updateStr, findStr);
                }
            }
        }


        private void processDomainDex()
        {
            string subCounter = ".dexDomainSell";
            long rh = getRemoteHeight();
            long lh = getLocalHeight(subCounter);
            long llh = getLocalHeight();
            if (rh > llh) rh = llh;

            if (lh >= rh) return;
            for(long index = lh+1; index<=rh; ++index)
            {
                string findStr = new JObject() { {"blockindex", index }, { "$or", new JArray {
                    new JObject() { {"displayName", "NNSauction" } },
                    new JObject() { {"displayName", "NNSauctionDiscontinued" } },
                    new JObject() { {"displayName", "NNSbet" } }
                } } }.ToString();
                var queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, dexContractHash, findStr);
                if (queryRes != null && queryRes.Count > 0)
                {
                    processDomainDexNNSauction(queryRes);
                }

                updateRecord(index, subCounter);
                log(index, rh, subCounter);
            }
        }
        private void processDomainDexNNSauction(JArray rr)
        {
            rr.ToList().ForEach(p => {
                string findStr = new JObject { {"namehash", p["fullHash"] } }.ToString();
                var queryRes = mh.GetData(localConn.connStr, localConn.connDB, domainOwnerCol, findStr);
                if (queryRes == null || queryRes.Count == 0) return;

                var dexLaunchPrice = p["startPrice"];
                var dexLaunchAssetName = getAssetName(p["assetHash"].ToString());
                var dexLaunchOrderid = p["auctionid"].ToString();
                var dexLaunchFlag = p["displayName"].ToString() == "NNSauction" ? "1" : "0";
                if (queryRes[0]["dexLaunchFlag"] != null && queryRes[0]["dexLaunchFlag"].ToString() == dexLaunchFlag
                    && queryRes[0]["dexLaunchOrderid"] != null && queryRes[0]["dexLaunchOrderid"].ToString() == dexLaunchOrderid
                    && queryRes[0]["dexLaunchAssetName"] != null && queryRes[0]["dexLaunchAssetName"].ToString() == dexLaunchAssetName
                ) return;

                
                string updateStr = new JObject { { "$set", new JObject { { "dexLaunchFlag", dexLaunchFlag },{ "dexLaunchPrice", dexLaunchPrice }, { "dexLaunchAssetName", dexLaunchAssetName }, { "dexLaunchOrderid", dexLaunchOrderid } } } }.ToString();
                mh.UpdateData(localConn.connStr, localConn.connDB, domainOwnerCol, updateStr, findStr);
            });
        }
        private Dictionary<string, string> assetNameDict;
        private string getAssetName(string assetHash)
        {
            if (assetNameDict == null) assetNameDict = new Dictionary<string, string>();
            if (assetNameDict.TryGetValue(assetHash, out string assetName)) return assetName;

            // 
            string findStr = new JObject() { { "assetid", assetHash } }.ToString();
            var queryRes = mh.GetData(blockConn.connStr, blockConn.connDB, "NEP5asset", findStr);
            if (queryRes != null && queryRes.Count > 0)
            {
                string name = queryRes[0]["symbol"].ToString();
                assetNameDict.Add(assetHash, name);
                return name;
            }
            return "";
        }
        private long getBlockHeight()
        {
            string findStr = new JObject() { { "counter", "block" } }.ToString();
            var query = mh.GetData(Config.blockDbConnInfo.connStr, Config.blockDbConnInfo.connDB, "system_counter", findStr);
            return long.Parse(query[0]["lastBlockindex"].ToString());
        }
        private Dictionary<long, long> getBlockTime(long[] indexArr)
        {
            string findStr = MongoFieldHelper.toFilter(indexArr, "index").ToString();
            string fieldStr = new JObject() { { "index", 1 }, { "time", 1 } }.ToString();
            var query = mh.GetDataWithField(blockConn.connStr, blockConn.connDB, "block", fieldStr, findStr);
            return query.ToDictionary(k => long.Parse(k["index"].ToString()), v => long.Parse(v["time"].ToString()));
        }

        private void updateRecord(long maxBlockindex, string subCounter="")
        {
            string newdata = new JObject() { { "counter", domainOwnerCol + subCounter }, { "lastBlockindex", maxBlockindex } }.ToString();
            string findstr = new JObject() { { "counter", domainOwnerCol + subCounter } }.ToString();
            long cnt = mh.GetDataCount(localConn.connStr, localConn.connDB, domainRecord, findstr);
            if (cnt <= 0)
            {
                mh.PutData(localConn.connStr, localConn.connDB, domainRecord, newdata);
            }
            else
            {
                mh.ReplaceData(localConn.connStr, localConn.connDB, domainRecord, newdata, findstr);
            }
        }
        private long getLocalHeight(string subCounter="")
        {
            string findstr = new JObject() { { "counter", domainOwnerCol + subCounter } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, domainRecord, findstr);
            if (res == null || res.Count == 0)
            {
                return 0;
            }
            return long.Parse(Convert.ToString(res.OrderBy(p => int.Parse(p["lastBlockindex"].ToString())).ToArray()[0]["lastBlockindex"]));
        }
        private long getRemoteHeight()
        {
            string findstr = new JObject() { { "counter", "notify" } }.ToString();
            JArray res = mh.GetData(remoteConn.connStr, remoteConn.connDB, notifyRecordColl, findstr);
            if (res == null || res.Count == 0)
            {
                return 0;
            }
            return long.Parse(Convert.ToString(res.OrderBy(p => int.Parse(p["lastBlockindex"].ToString())).ToArray()[0]["lastBlockindex"]));
        }
        private void log(long localHeight, long remoteHeight, string subCounter="")
        {
            Console.WriteLine(DateTime.Now + string.Format(" {0}.self processed at {1}/{2}", name()+ subCounter, localHeight, remoteHeight));
        }
        private void ping()
        {
            LogHelper.ping(batchInterval, name());
        }
    }
}
