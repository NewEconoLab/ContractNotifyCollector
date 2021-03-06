﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using ContractNotifyCollector.helper;
using Newtonsoft.Json.Linq;

namespace ContractNotifyCollector.core.task
{
    /// <summary>
    /// 合约通知解析主类
    /// 
    /// 其余任务均基于该类分析结果数据
    /// 
    /// </summary>
    class ContractNotify : ContractTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo localConn;
        private DbConnInfo remoteConn;
        private DbConnInfo blockConn;
        Dictionary<string, JArray> structDict;
        private string notifyCounterColl;
        private string notifyColl;
        private string contractCounterColl;
        private int batchSize;
        private int batchInterval;
        private bool initSuccFlag = false;
        private List<string> hasCreateIndex = new List<string>();
        private string nelApiUrl = Config.nelApiUrl;
        private bool fromApiFlag = Config.fromApiFlag;
        private string assetColl;
        private string[] assetids;

        public ContractNotify(string name) : base(name)
        {
        }

        public override void initConfig(JObject config)
        {
            string filename = config["TaskConfig"][name()].ToString();
            JObject subConfig = JObject.Parse(File.ReadAllText(filename));
            if (subConfig["taskNet"].ToString() != networkType())
            {
                throw new Exception("NotFindConfig");
            }

            structDict = ((JArray)subConfig["taskList"]).Where(p => p["netType"].ToString() == networkType()).ToDictionary(
                k => getKey(k["contractHash"].ToString(), k["notifyDisplayName"].ToString()),
                v => (JArray)v["notifyStructure"]);
            notifyCounterColl = subConfig["notifyCounterColl"].ToString();
            notifyColl = subConfig["notifyColl"].ToString();
            contractCounterColl = subConfig["contractCounterColl"].ToString();
            batchSize = int.Parse(subConfig["batchSize"].ToString());
            batchInterval = int.Parse(subConfig["batchInterval"].ToString());
            assetColl = subConfig["assetColl"].ToString();
            assetids = ((JArray)subConfig["assetids"]).Select(p => p.ToString()).ToArray();


            // db info
            localConn = Config.localDbConnInfo;
            remoteConn = Config.blockDbConnInfo;
            blockConn = Config.blockDbConnInfo;
            //
            initAssetPricision();
            initSuccFlag = true;
        }

        private string getKey(string contractHash, string displayName)
        {
            return contractHash + displayName;
        }
        private JArray getVal(string contractHash, string displayName)
        {
            return structDict.GetValueOrDefault(getKey(contractHash, displayName));
        }
        private bool hasKey(string contractHash)
        {
            return structDict.Keys.Any(p => p.StartsWith(contractHash));
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

            // 获取本地已处理高度
            long localHeight = getLocalHeight();

            //
            if (remoteHeight <= localHeight)
            {
                log(localHeight, remoteHeight);
                return;
            }

            for (long startIndex = localHeight; startIndex <= remoteHeight; startIndex += batchSize)
            {
                long nextIndex = startIndex + batchSize;
                long endIndex = nextIndex < remoteHeight ? nextIndex : remoteHeight;
                // 待处理数据
                JArray res = GetRemoteData(startIndex, endIndex);
                if (res == null || res.Count == 0)
                {
                    updateLocalRecord(endIndex);
                    log(endIndex, remoteHeight);
                    continue;
                }
                // 分析数据
                long[] blockindexArr = res.Select(p => long.Parse(p["blockindex"].ToString())).Distinct().OrderBy(p => p).ToArray();
                foreach (long blockindex in blockindexArr)
                {
                    // 解析
                    List<JObject> list = new List<JObject>();
                    foreach (JObject jo in res.Where(p => long.Parse(p["blockindex"].ToString()) == blockindex))
                    {
                        string txid = jo["txid"].ToString();

                        // 新版解析方式,NEO-CLI Version: 2.9.0.0
                        if (jo["executions"] != null)
                        {
                            foreach (JObject execution in (JArray)jo["executions"])
                            {
                                string state = execution["vmstate"].ToString();
                                //if (state == "FAULT, BREAK") continue;
                                if (state.ToUpper().StartsWith("FAULT")) continue;
                                // stack...
                                var c = (JArray)execution["notifications"];
                                if (c == null || c.Count() == 0) continue;
                                List<JObject> r = processNotifications(c, blockindex, txid, state);
                                if (r == null || r.Count() == 0) continue;
                                list.AddRange(r);
                            }
                            continue;
                        }

                        // 默认解析方式,NEO-CLI Version: 2.7.6.1
                        else
                        {
                            string vmstate = jo["vmstate"].ToString();
                            //if (vmstate == "FAULT, BREAK") continue;
                            if (vmstate.ToUpper().StartsWith("FAULT")) continue;
                            // stack...
                            JArray notifications = (JArray)jo["notifications"];
                            List<JObject> r = processNotifications(notifications, blockindex, txid, vmstate);
                            if (r == null || r.Count() == 0) continue;
                            list.AddRange(r);
                        }
                    }

                    // 入库==>分组(分表)
                    var notifySet = list.GroupBy(p => p["contractHash"], (k, g) => new { hash = k.ToString(), sets = g.ToArray() } ).ToArray();
                    foreach(var item in notifySet)
                    {
                        string contractHash = item.hash;
                        long count = mh.GetDataCount(localConn.connStr, localConn.connDB, contractHash, new JObject() { { "blockindex", blockindex } }.ToString());
                        if(count <=0)
                        {
                            mh.PutData(localConn.connStr, localConn.connDB, contractHash, new JArray() { item.sets });
                        } else
                        {
                            foreach(var subItem in item.sets)
                            {
                                string findstr = new JObject() { { "txid", subItem["txid"] }, { "n", subItem["n"] }, { "displayName", subItem["displayName"] } }.ToString();
                                long cnt2 = mh.GetDataCount(localConn.connStr, localConn.connDB, contractHash, findstr);
                                if (cnt2 <= 0)
                                {
                                    mh.PutData(localConn.connStr, localConn.connDB, contractHash, subItem);
                                }
                            }
                        }
                        // 添加索引
                        if (hasCreateIndex.Contains(contractHash)) continue;
                        mh.setIndex(localConn.connStr, localConn.connDB, contractHash, "{'blockindex':1,'txid':1,'n':1}", "i_blockindex_txid_n");
                        mh.setIndex(localConn.connStr, localConn.connDB, contractHash, "{'blockindex':1}", "i_blockindex");
                        hasCreateIndex.Add(contractHash);
                    }

                    // 更新高度
                    updateLocalRecord(blockindex);
                    log(blockindex, remoteHeight);
                }

            }

        }

        private List<JObject> processNotifications(JArray notifications, long blockindex, string txid, string vmstate/*, JArray vmstate*/)
        {
            List<JObject> list = new List<JObject>();
            int n = 0;
            foreach (JObject notification in notifications)
            {
                try { 
                string contractHash = notification["contract"].ToString();
                if (!hasKey(contractHash))
                {
                    ++n;
                    continue;
                }
                JArray JAstate = (JArray)notification["state"]["value"];
                string displayName = JAstate[0]["value"].ToString().Hexstring2String();
                JArray notifyStruct = getVal(contractHash, displayName);
                if (notifyStruct == null || notifyStruct.Count == 0)
                {
                    ++n;
                    continue;
                }
                // 索引信息
                JObject notifyInfo = new JObject();
                notifyInfo.Add("blockindex", blockindex);
                notifyInfo.Add("txid", txid);
                notifyInfo.Add("n", n);
                notifyInfo.Add("vmstate", vmstate);
                notifyInfo.Add("contractHash", contractHash);

                // 存储解析数据
                int i = 0;
                foreach (JObject jv in JAstate)
                {
                    string type = jv["type"].ToString();
                    if (type == "Array")
                    {
                        // Array
                        JArray value = (JArray)jv["value"];
                        int j = 0;
                        foreach (JObject jvv in value)
                        {
                            string typeLevel2 = jvv["type"].ToString();
                            string valueLevel2 = jvv["value"].ToString();
                            JObject taskEscape = (JObject)notifyStruct[i]["arrayData"][j];
                            string taskName = taskEscape["name"].ToString();
                            updateAssetPricision(notifyInfo, taskEscape);
                            string taskValue = escapeHelper.contractDataEscap(typeLevel2, valueLevel2, taskEscape);
                            try
                            {
                                notifyInfo.Add(taskName, taskValue);
                            }
                            catch
                            {// txid重名
                                notifyInfo.Add("_" + taskName, taskValue);
                            }
                            ++j;
                        }
                    }
                    else
                    {
                        // ByteArray + other
                        string value = jv["value"].ToString();
                        try
                        {
                            var r = (JObject)notifyStruct[i]; ;
                        } catch (Exception ex)
                        {
                            Console.WriteLine(ex.StackTrace);
                        }
                        JObject taskEscape = (JObject)notifyStruct[i];
                        string taskName = taskEscape["name"].ToString();
                        updateAssetPricision(notifyInfo, taskEscape);
                        string taskValue = escapeHelper.contractDataEscap(type, value, taskEscape);
                        try
                        {
                            notifyInfo.Add(taskName, taskValue);
                        }
                        catch
                        {// txid重名
                            notifyInfo.Add("_" + taskName, taskValue);
                        }
                    }
                    ++i;
                }
                // 原始state数据
                notifyInfo.Add("state", notification["state"]);

                // 单条入库或者批量入库，这里采用批量入库方式
                list.Add(notifyInfo);

                ++n;
                } catch
                {
                    ++n;
                }
            }
            return list;
        }

        private void updateAssetPricision(JObject notifyInfo, JObject taskEscape)
        {
            if (taskEscape["dependency"] != null)
            {
                string dependency = taskEscape["dependency"].ToString();
                string assetid = notifyInfo[dependency].ToString();

                if (assetPricisionDict.ContainsKey(assetid))
                {
                    taskEscape.Remove("decimals");
                    taskEscape.Add("decimals", assetPricisionDict.GetValueOrDefault(assetid, 8));
                }
            }
        }

        private Dictionary<string, int> assetPricisionDict;
        private void initAssetPricision()
        {
            if (assetids == null || assetids.Count() == 0)
            {
                return;
            }

            if (fromApiFlag)
            {
                var respRes = HttpHelper.Post(nelApiUrl, "getNep5AssetInfo", new JArray { new JArray { assetids } });
                var res = (JArray)JObject.Parse(respRes)["result"];
                assetPricisionDict = res.ToDictionary(k => k["assetid"].ToString(), v => (int)v["decimals"]);
                return;
            }

            // 
            string findStr = null;
            if (assetids.Count() == 1)
            {
                findStr = new JObject() { { "assetid", assetids[0] } }.ToString();
            }
            else
            {
                findStr = new JObject() { { "$or", new JArray() { assetids.Select(item => new JObject() { { "assetid", item } }).ToArray() } } }.ToString();
            }
            string fieldStr = new JObject() { { "assetid", 1 }, { "decimals", 1 }, { "_id", 0 } }.ToString();
            var queryRes = mh.GetDataWithField(blockConn.connStr, blockConn.connDB, assetColl, fieldStr, findStr);
            if (queryRes == null || queryRes.Count() == 0)
            {
                throw new Exception("Not find asset'pricision");
            }
            assetPricisionDict = queryRes.ToDictionary(k => k["assetid"].ToString(), v => (int)v["decimals"]);
        }

        private void updateLocalRecord(long height)
        {
            string findStr = new JObject() { { "counter", "notify" } }.ToString();
            string newData = new JObject() { { "counter", "notify" }, { "lastBlockindex", height } }.ToString();
            long count = mh.GetDataCount(localConn.connStr, localConn.connDB, contractCounterColl, findStr);
            if (count <= 0)
            {
                mh.PutData(localConn.connStr, localConn.connDB, contractCounterColl, newData);
            }
            else
            {
                mh.ReplaceData(localConn.connStr, localConn.connDB, contractCounterColl, newData, findStr);
            }
        }
        private long getLocalHeight()
        {
            string findStr = new JObject() { { "counter", "notify" } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, contractCounterColl, findStr);
            if (res == null || res.Count == 0)
            {
                return 0;
            }
            return long.Parse(res[0]["lastBlockindex"].ToString());
        }
        private long getRemoteHeight()
        {
            string findStr = new JObject() { { "counter", "notify" } }.ToString();
            JArray res = mh.GetData(remoteConn.connStr, remoteConn.connDB, notifyCounterColl, findStr);
            if (res == null || res.Count == 0)
            {
                return 0;
            }
            return long.Parse(res[0]["lastBlockindex"].ToString());
        }
        private JArray GetRemoteData(long startIndex, long endIndex)
        {
            string findStr = new JObject() { { "blockindex", new JObject() { { "$gt", startIndex }, { "$lte", endIndex } } } }.ToString();
            return mh.GetData(remoteConn.connStr, remoteConn.connDB, notifyColl, findStr);
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
