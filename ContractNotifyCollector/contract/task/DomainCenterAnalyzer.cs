﻿using ContractNotifyCollector.helper;
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
            domainOwnerCol = cfg["domainOwnerCol"].ToString();
            nnsSellingAddr = cfg["nnsSellingAddr"].ToString();
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
                JObject queryFilter = new JObject() { { "blockindex", new JObject() { { "$gt", index }, { "$lte", endIndex } } }, { "owner", new JObject() { { "$ne", nnsSellingAddr } } } };
                JObject queryField = new JObject() { { "state", 0 } };
                JArray queryRes = mh.GetDataWithField(remoteConn.connStr, remoteConn.connDB, domainCenterCol, queryField.ToString(), queryFilter.ToString());
                if (queryRes != null && queryRes.Count() > 0)
                {
                    processDomainCenter(queryRes);
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

                updateRecord(endIndex);
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

        private void processDomainCenter(JArray domainCenterRes)
        {
            var pRes = domainCenterRes.GroupBy(p => p["namehash"].ToString(), (k, g) => new { namehash = k.ToString(), item = g.OrderByDescending(pg => long.Parse(pg["blockindex"].ToString())).First() }).ToArray();
            foreach(var item in pRes)
            {
                string namehash = item.namehash;
                JObject jo = (JObject)item.item;
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
                    long ttl = long.Parse(jo["TTL"].ToString());
                    jo.Remove("TTL");
                    jo.Add("TTL", ttl);

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

        private void procesDomainResolver(JArray domainResolverRes)
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
                string findStr = new JObject() { { "owner", jo["addr"] }, { "bindflag", "1" } }.ToString();
                string updateStr = new JObject() { {"$set", new JObject() { { "bindflag", "0"}  } } }.ToString();
                mh.UpdateData(localConn.connStr, localConn.connDB, domainOwnerCol, updateStr, findStr);
                if(jo["displayName"].ToString() == "addrCreditRegistered")
                {
                    findStr = new JObject() { { "namehash", jo["namehash"] } }.ToString();
                    updateStr = new JObject() { { "$set", new JObject() { { "bindflag", "1" } } } }.ToString();
                    mh.UpdateData(localConn.connStr, localConn.connDB, domainOwnerCol, updateStr, findStr);
                }
            }
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

        private void updateRecord(long maxBlockindex)
        {
            string newdata = new JObject() { { "counter", domainOwnerCol }, { "lastBlockindex", maxBlockindex } }.ToString();
            string findstr = new JObject() { { "counter", domainOwnerCol } }.ToString();
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
        private long getLocalHeight()
        {
            string findstr = new JObject() { { "counter", domainOwnerCol } }.ToString();
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
