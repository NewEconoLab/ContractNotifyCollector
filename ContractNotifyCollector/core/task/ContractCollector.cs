using System;
using System.Collections.Generic;
using System.Linq;
using ContractNotifyCollector.helper;
using Newtonsoft.Json.Linq;

namespace ContractNotifyCollector.core.task
{
    /// <summary>
    /// 汇总合约任务
    /// 
    /// <para>
    /// 将由升级合约引起的相同功能不同合约哈希汇总入同一张表中
    /// </para>
    /// 
    /// </summary>
    class ContractCollector : ContractTask
    {
        private JObject config;

        public ContractCollector(string name) : base(name)
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


        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo remoteDbConnInfo;
        private DbConnInfo localDbConnInfo;
        private Dictionary<string, JObject> collDict;
        private string remoteColl;
        private string localColl;
        private int interval = 1000;
        private void init()
        {
            // DB连接信息
            string startNetType = config["startNetType"].ToString();
            var connInfo = config["connList"].Children().Where(p => p["netType"].ToString() == startNetType).First();
            remoteDbConnInfo = getDbConnInfo(connInfo, true);
            localDbConnInfo = getDbConnInfo(connInfo, false);

            // 合约与集合关系
            JArray collList = (JArray)config["collList"];
            collDict = collList.SelectMany(p => {
                string contractColl = Convert.ToString(p["contractColl"]);

                return p["contractHash"].Select(pItem =>
                {
                    JObject pItemNew = (JObject)pItem;
                    pItemNew.Add("contractColl", contractColl);
                    return pItemNew;
                }).ToList();

            }).ToDictionary(itemKey => Convert.ToString(itemKey["name"]), itemVal => itemVal);

            // 高度记录表对应关系
            JObject recordMap = (JObject)config["HeightRecordMap"];
            remoteColl = recordMap["remoteColl"].ToString();
            localColl = recordMap["localColl"].ToString();
        }
        
        internal void run()
        {
            while (true)
            {
                ping();

                // 获取数据源所有合约高度信息
                JArray remoteContractAtHeightInfoList = getData(remoteDbConnInfo, remoteColl);
                foreach (JObject item in remoteContractAtHeightInfoList)
                {
                    // 单个合约远程高度信息
                    string contractHash = Convert.ToString(item["contractHash"]);
                    string displayName = Convert.ToString(item["displayName"]);
                    int lastBlockindex = int.Parse(Convert.ToString(item["lastBlockindex"]));


                    // 获取合约/集合配置
                    JObject contractConfig = collDict.GetValueOrDefault(contractHash);
                    if (contractConfig == null)
                    {
                        warn("Not find config of ContractHash=" + contractHash);
                        continue;
                    }
                    string contractColl = Convert.ToString(contractConfig["contractColl"]);
                    string createDate = Convert.ToString(contractConfig["createDate"]);


                    // 单个合约本地高度信息
                    JObject heightFilterObj = new JObject();
                    heightFilterObj.Add("contractHash", contractHash);
                    heightFilterObj.Add("displayName", displayName);
                    string heightFilter = heightFilterObj.ToString();
                    int localBlockHeight = getLocalContractStorageHeight(localDbConnInfo, localColl, heightFilter);

                    // 本地高度小于远程高度时才需处理
                    if (lastBlockindex <= localBlockHeight) continue;

                    int batchSize = 1000;
                    for(int i = localBlockHeight+1; i<=lastBlockindex; i+=batchSize)
                    {
                        // 获取待入库的合约
                        int insertCount = 0;
                        JObject contractFilterObj = new JObject();
                        JObject subContractFilterObj = new JObject();
                        subContractFilterObj.Add("$lte", i+ batchSize);
                        subContractFilterObj.Add("$gte", i);
                        contractFilterObj.Add("blockindex", subContractFilterObj);
                        contractFilterObj.Add("displayName", displayName);
                        string contractFilter = contractFilterObj.ToString();
                        JArray remoteContractRes = getData(remoteDbConnInfo, contractHash, contractFilter);
                        if(remoteContractRes == null || remoteContractRes.Count() == 0)
                        {
                            continue;
                        }
                        foreach (JObject subItem in remoteContractRes)
                        {
                            int blockindex = int.Parse(Convert.ToString(subItem["blockindex"]));
                            JObject subFilterObj = new JObject();
                            subFilterObj.Add("contractHash", contractHash);
                            subFilterObj.Add("blockindex", blockindex);
                            subFilterObj.Add("displayName", displayName);
                            subFilterObj.Add("txid", subItem["txid"].ToString());
                            if(subItem["subItem"] != null && subItem["subItem"].ToString() != "")
                            {
                                subFilterObj.Add("id", subItem["id"].ToString());
                            }
                            string subFilter = subFilterObj.ToString();
                        
                            JArray localContractRes = getData(localDbConnInfo, contractColl, subFilter);
                            if (localContractRes == null || localContractRes.Count == 0)
                            {
                                // 入库集合表
                                subItem.Add("contractHash", contractHash);
                                PutData(localDbConnInfo, contractColl, new JArray() { subItem });


                                // 更新合约和集合关系表
                                if (localBlockHeight > 0)
                                {
                                    JObject condition = new JObject();
                                    condition.Add("contractColl", contractColl);
                                    condition.Add("contractHash", contractHash);
                                    condition.Add("displayName", displayName);
                                    JObject subCondiction = new JObject();
                                    subCondiction.Add("$lt", blockindex);
                                    condition.Add("lastBlockindex", subCondiction);
                                    JObject data = new JObject();
                                    JObject subData = new JObject();
                                    subData.Add("lastBlockindex", blockindex);
                                    data.Add("$set", subData);
                                    UpdateData(localDbConnInfo, localColl, data.ToString(), condition.ToString());

                                }
                                else
                                {
                                    JObject localContractStorageHeightInfo = new JObject();
                                    localContractStorageHeightInfo.Add("contractColl", contractColl);
                                    localContractStorageHeightInfo.Add("contractHash", contractHash);
                                    localContractStorageHeightInfo.Add("displayName", displayName);
                                    localContractStorageHeightInfo.Add("lastBlockindex", blockindex);
                                    localContractStorageHeightInfo.Add("createDate", createDate);
                                    PutData(localDbConnInfo, localColl, new JArray() { localContractStorageHeightInfo });
                                    localBlockHeight = blockindex;
                                }
                                ++insertCount;

                            } 
                        }
                        Console.WriteLine("{0}_{1} processed, updateCount:{2}-[{3}/{4}]", contractHash, displayName, insertCount, /*localBlockHeight*/i+insertCount, lastBlockindex);
                    }
                }
                
            }
        }

        private void test()
        {
            // 测试使用
            string startNetType = config["startNetType"].ToString();
            JArray connList = (JArray)config["connList"];
            JArray collList = (JArray)config["collList"];
            print("startNetType=" + startNetType);
            print("connList[0].netType=" + connList[0]["netType"].ToString());
            print("collList[0].contractColl=" + collList[0]["contractColl"].ToString());
        }

        private int getLocalContractStorageHeight(DbConnInfo conn, string coll, string filter)
        {
            JArray res = getData(conn, coll, filter);
            if (res == null || res.Count == 0) return 0;
            return int.Parse(Convert.ToString(res[0]["lastBlockindex"]));
        }
        private JArray getData(DbConnInfo conn, string coll, string filter="{}")
        {
            return mh.GetData(conn.connStr, conn.connDB, coll, filter);
        }
        private void PutData(DbConnInfo conn, string coll, JArray Jdata)
        {
            mh.PutData(conn.connStr, conn.connDB, coll, Jdata);
        }
        private void UpdateData(DbConnInfo conn, string coll, string Jdata, string Jcondition)
        {
            mh.UpdateData(conn.connStr, conn.connDB, coll, Jdata, Jcondition);
        }

        private void ReplaceData(DbConnInfo conn, string coll, JObject Jdata, JObject Jcondition)
        {
            mh.ReplaceData(conn.connStr, conn.connDB, coll, Jdata, Jcondition);
        }
        
        private DbConnInfo getDbConnInfo(JToken conn,  bool isRemoteNotLocal)
        {
            if(isRemoteNotLocal)
            {
                return new DbConnInfo
                {
                    connStr = conn["remoteConnStr"].ToString(),
                    connDB = conn["remoteDatabase"].ToString()
                };
            } 
            else
            {
                return new DbConnInfo
                {
                    connStr = conn["localConnStr"].ToString(),
                    connDB = conn["localDatabase"].ToString()
                };
            }
        }
        
        internal void warn(string ss)
        {
            Console.WriteLine("warn:"+ss);
        }
        internal void print(string ss)
        {
            Console.WriteLine(DateTime.Now + " " + ss);
        }
        private void ping()
        {
            LogHelper.ping(interval, name());
        }
    }
}
