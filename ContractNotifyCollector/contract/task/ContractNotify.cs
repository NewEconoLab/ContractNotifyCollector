using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ContractNotifyCollector.helper;
using Newtonsoft.Json.Linq;

namespace ContractNotifyCollector.core.task
{
    class ContractNotify : ContractTask
    {
        private JObject config;
        public ContractNotify(string name) : base(name)
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

        private JObject subConfig;
        private DbConnInfo localConn;
        private DbConnInfo remoteConn;
        private int batchSize = 500;
        private int batchInterval = 2000;
        private void initConfig()
        {
            string filename = config["TaskConfig"][name()].ToString();
            subConfig = JObject.Parse(File.ReadAllText(filename));

            localConn = Config.remoteDbConnInfo;
            remoteConn = Config.blockDbConnInfo;
        }
        private void run()
        {
            //
            int taskID = 0;
            foreach (JObject task in subConfig["taskList"])
            {
                string netType = task["netType"].ToString();
                new Task(() => {
                    while(true)
                    {
                        extractNotifyInfo(task);
                        Thread.Sleep(batchInterval);
                    }
                }).Start();
                ++taskID;
            }
        }

        private MongoDBHelper mh = new MongoDBHelper();
        private void extractNotifyInfo(JObject task)
        {
            string taskContractHash = task["contractHash"].ToString();
            string taskNotifyDisplayName = task["notifyDisplayName"].ToString();
            JArray taskNotifyStructure = (JArray)task["notifyStructure"];

            // 本地已同步高度
            long localHeight = GetLocalHeight(taskContractHash, taskNotifyDisplayName);

            // 远程高度
            long remoteHeight = GetRemoteHeight("notify");

            // 
            for (long index = localHeight; index <= remoteHeight + 1; index += batchSize)
            {
                long nextIndex = index + batchSize;
                long endIndex = nextIndex < remoteHeight ? nextIndex : remoteHeight + 1;
                // 待处理数据
                JObject queryFilter = new JObject() { { "blockindex", new JObject() { { "$gte", index }, { "$lte", endIndex } } } };
                JObject querySortBy = new JObject() { { "blockindex", 1 } };
                JObject queryField = new JObject() { { "state", 0 } };
                JArray queryRes = GetRemoteData(taskContractHash, taskNotifyDisplayName, index, endIndex);
                if (queryRes == null || queryRes.Count == 0)
                {
                    if (remoteHeight <= localHeight)
                    {
                        log(localHeight, remoteHeight, taskContractHash, taskNotifyDisplayName);
                        break;
                    }
                    UpdateLocalHeight(taskContractHash, taskNotifyDisplayName, endIndex);
                    log(endIndex, remoteHeight, taskContractHash, taskNotifyDisplayName);
                    continue;
                }

                foreach (JObject jo in queryRes)
                {
                    long blockindex = long.Parse(jo["blockindex"].ToString());
                    string txid = jo["txid"].ToString();
                    JArray notifications = (JArray)jo["notifications"];

                    int n = 0; //标记notify在一个tx里的序号
                    foreach (JObject notify in notifications)
                    {
                        string dataContractHash = notify["contract"].ToString();
                        string stateType = notify["state"]["type"].ToString();
                        if (stateType != "Array")
                        {
                            continue;
                        }
                        JArray stateValue = (JArray)notify["state"]["value"];
                        string dataNotifyDisplayName = stateValue[0]["value"].ToString().Hexstring2String();
                        if (dataContractHash != taskContractHash || dataNotifyDisplayName != taskNotifyDisplayName)
                        {
                            continue;
                        }

                        JObject newNotifyInfo = new JObject();
                        newNotifyInfo.Add("blockindex", blockindex);
                        newNotifyInfo.Add("txid", txid);
                        newNotifyInfo.Add("n", n++);

                        int i = 0;
                        foreach (JObject jv in stateValue)
                        {
                            string notifyType = jv["type"].ToString();
                            if (notifyType == "Array")
                            {
                                JArray JAarrayValue = (JArray)jv["value"];
                                int j = 0;
                                foreach (JObject JvalueLevel2 in JAarrayValue)
                                {
                                    JObject JtaskEscapeInfo = (JObject)taskNotifyStructure[i]["arrayData"][j];
                                    string taskName = JtaskEscapeInfo["name"].ToString();
                                    string notifyType2 = JvalueLevel2["type"].ToString();
                                    string notifyValueSrc2 = JvalueLevel2["value"].ToString();
                                    //如果处理失败则不处理（用原值）
                                    string notifyValueDst2 = escapeHelper.contractDataEscap(notifyType2, notifyValueSrc2, JtaskEscapeInfo);
                                    try
                                    {
                                        newNotifyInfo.Add(taskName, notifyValueDst2);
                                    }
                                    catch
                                    {//重名
                                        newNotifyInfo.Add("_" + taskName, notifyValueDst2);
                                    }
                                    ++j;
                                }
                            }
                            else
                            {
                                JObject taskEscapeInfo = (JObject)taskNotifyStructure[i];
                                string taskName = taskEscapeInfo["name"].ToString();
                                string notifyValueSrc = jv["value"].ToString();
                                string notifyValueDst = escapeHelper.contractDataEscap(notifyType, notifyValueSrc, taskEscapeInfo);
                                try
                                {
                                    newNotifyInfo.Add(taskName, notifyValueDst);
                                }
                                catch
                                {//重名
                                    newNotifyInfo.Add("_" + taskName, notifyValueDst);
                                }
                            }
                            ++i;
                        }


                        // 入库
                        //string filterStr = new JObject() { { "blockindex", blockindex }, { "txid", txid }, { "n", n++ }, { "displayName", taskNotifyDisplayName } }.ToString();
                        JObject cc = (JObject)newNotifyInfo.DeepClone();
                        cc.Remove("state");
                        cc.Remove("getTime");
                        string filterStr = cc.ToString();

                        if (mh.GetDataCount(localConn.connStr, localConn.connDB, taskContractHash, filterStr) <= 0)
                        {
                            mh.PutData(localConn.connStr, localConn.connDB, taskContractHash, newNotifyInfo.ToString());

                            // 更新高度
                            UpdateLocalHeight(taskContractHash, taskNotifyDisplayName, blockindex);
                        }
                        log(blockindex, remoteHeight, taskContractHash, taskNotifyDisplayName);

                    }

                }
            }
            

        }

        
        private long GetLocalHeight(string contractHash, string displayName)
        {
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, "contractStorageHeight", new JObject() { { "contractHash", contractHash }, { "displayName", displayName } }.ToString());
            if(res == null || res.Count == 0)
            {
                return 0;
            }
            return long.Parse(res[0]["lastBlockindex"].ToString());
        }
        private long GetRemoteHeight(string key)
        {
            JArray res = mh.GetData(remoteConn.connStr, remoteConn.connDB, "system_counter", new JObject() { { "counter", key } }.ToString());
            if (res == null || res.Count == 0)
            {
                return 0;
            }
            return long.Parse(res[0]["lastBlockindex"].ToString());

        }
        private JArray GetRemoteData(string contractHash, string displayName, long startIndex, long endIndex)
        {
            JObject filter = new JObject();
            filter.Add("notifications.contract", contractHash);
            filter.Add("notifications.state.value.value", Helper.BytesToHexString(Encoding.UTF8.GetBytes(displayName)));
            filter.Add("blockindex", new JObject() { { "$gte", startIndex },{ "$lte", endIndex  } });
            return mh.GetData(remoteConn.connStr, remoteConn.connDB, "notify", filter.ToString());
        }
        private void UpdateLocalHeight(string contractHash, string displayName, long blockindex)
        {
            string newData = new JObject() { { "contractHash", contractHash }, { "displayName", displayName }, {"lastBlockindex", blockindex } }.ToString();
            string findStr = new JObject() { { "contractHash", contractHash }, { "displayName", displayName } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, "contractStorageHeight", findStr);
            if(res == null || res.Count == 0)
            {

                mh.PutData(localConn.connStr, localConn.connDB, "contractStorageHeight", newData);
            } else
            {
                if(long.Parse(res[0]["lastBlockindex"].ToString()) < blockindex)
                {
                    mh.ReplaceData(localConn.connStr, localConn.connDB, "contractStorageHeight", newData, findStr);
                }
            }

            // 更新高度
            //string findStr = new JObject() { { "contractHash", taskContractHash }, { "displayName", taskNotifyDisplayName } }.ToString();
            //string updateStr = new JObject() { { "$set", new JObject() { { "contractHash", taskContractHash }, { "displayName", taskNotifyDisplayName }, { "lastBlockindex", blockindex } } } }.ToString();
            //mh.UpdateData(localConn.connStr, localConn.connDB, "contractStorageHeight", updateStr, findStr);
        }
        

        private void log(long localHeight, long remoteHeight, string contractHash, string displayName)
        {
            Console.WriteLine(DateTime.Now + string.Format(" {0}.{1}.{2} processed at {3}/{4}", name(), contractHash, displayName, localHeight, remoteHeight));
        }
    }
}
