using ContractNotifyCollector.helper;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;

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
        private JObject config;

        public DomainCenterAnalyzer(string name):base(name)
        {
        }

        public override void initConfig(JObject config)
        {
            this.config = config;
            init();
        }

        public override void startTask()
        {
            run();
        }


        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo localDbConnInfo;
        private DbConnInfo remoteDbConnInfo;
        private string domainCenterCol;
        private string domainResolverCol;
        private string domainRecord;
        private string domainOwnerCol;
        private int batchSize;
        private int batchInterval;
        private bool initSuccFlag = false;

        private void init()
        {
            //JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name()).ToArray()[0]["taskInfo"];
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];
            
            domainCenterCol = cfg["domainCenterCol"].ToString();
            domainResolverCol = cfg["domainResolverCol"].ToString();
            domainRecord = cfg["domainRecord"].ToString();
            domainOwnerCol = cfg["domainOwnerCol"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());

            // db info
            localDbConnInfo = Config.localDbConnInfo;
            remoteDbConnInfo = Config.remoteDbConnInfo;
            //
            initSuccFlag = true;
        }
        
        private void run()
        {
            if (!initSuccFlag) return;
            while (true)
            {
                ping();

                // 本地高度
                long domainCenterBlockindex = 0;
                long domainResoverBlockindex = 0;
                JObject domainFilter = new JObject() { { "$or", new JArray() { new JObject() { { "contractHash", domainCenterCol } }, new JObject() { { "contractHash", domainResolverCol } } } } };
                JArray domainRes = mh.GetData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainRecord, domainFilter.ToString());
                if (domainRes != null && domainRes.Count != 0)
                {
                    JToken j1 = domainRes.Where(p => p["contractHash"].ToString() == domainCenterCol).FirstOrDefault();
                    JToken j2 = domainRes.Where(p => p["contractHash"].ToString() == domainResolverCol).FirstOrDefault();
                    domainCenterBlockindex = j1 == null ? 0 : long.Parse(j1["blockindex"].ToString());
                    domainResoverBlockindex = j2 == null ? 0 : long.Parse(j2["blockindex"].ToString());
                }


                // 域名中心-远程高度
                long maxDomainCenterBlockindex = 0;
                long hasDomainCenterBlockindex = domainCenterBlockindex;
                JArray arr = mh.GetDataPagesWithField(remoteDbConnInfo.connStr, remoteDbConnInfo.connDB, domainCenterCol, new JObject() { { "blockindex", 1 } }.ToString(), 1, 1, new JObject() { { "blockindex", -1 } }.ToString());
                if (arr != null && arr.Count() > 0)
                {
                    maxDomainCenterBlockindex = long.Parse(arr[0]["blockindex"].ToString());
                }
                for (long startIndex= hasDomainCenterBlockindex; startIndex<= maxDomainCenterBlockindex; startIndex+= batchSize)
                {
                    long nextIndex = startIndex + batchSize;
                    long endIndex = nextIndex < maxDomainCenterBlockindex ? nextIndex : maxDomainCenterBlockindex;
                    JArray andFilter = new JArray();
                    andFilter.Add(new JObject() { { "blockindex", new JObject() { { "$gte", startIndex } } } });
                    andFilter.Add(new JObject() { { "blockindex", new JObject() { { "$lte", endIndex } } } });
                    JObject domainCenterFilter = new JObject() { { "$and", andFilter } };
                    JObject domainCenterField = new JObject() { { "state", 0 } };
                    JArray domainCenterRes = mh.GetDataWithField(remoteDbConnInfo.connStr, remoteDbConnInfo.connDB, domainCenterCol, domainCenterField.ToString(), domainCenterFilter.ToString());
                    if (domainCenterRes != null && domainCenterRes.Count != 0)
                    {
                        processDomainCenter(domainCenterRes);
                    }
                    hasDomainCenterBlockindex = endIndex;
                    if(hasDomainCenterBlockindex != domainCenterBlockindex)
                    {
                        updateRecord(hasDomainCenterBlockindex, domainCenterCol);
                    }
                    log("domainCenter", hasDomainCenterBlockindex, maxDomainCenterBlockindex);
                }


                // 解析器-远程高度
                long maxDomainResoverBlockindex = 0;
                long hasDomainResoverBlockindex = domainResoverBlockindex;
                arr = mh.GetDataPagesWithField(remoteDbConnInfo.connStr, remoteDbConnInfo.connDB, domainResolverCol, new JObject() { { "blockindex", 1 } }.ToString(), 1, 1, new JObject() { { "blockindex", -1 } }.ToString());
                if (arr != null && arr.Count() > 0)
                {
                    maxDomainResoverBlockindex = long.Parse(arr[0]["blockindex"].ToString());
                }
                for (long startIndex= hasDomainResoverBlockindex; startIndex<= maxDomainResoverBlockindex; startIndex+= batchSize)
                {
                    long nextIndex = startIndex + batchSize;
                    long endIndex = nextIndex < maxDomainResoverBlockindex ? nextIndex : maxDomainResoverBlockindex;
                    JArray andFilter = new JArray();
                    andFilter.Add(new JObject() { { "blockindex", new JObject() { { "$gte", startIndex } } } });
                    andFilter.Add(new JObject() { { "blockindex", new JObject() { { "$lte", endIndex } } } });
                    JObject domainCenterFilter = new JObject() { { "$and", andFilter } };
                    domainCenterFilter.Add("protocol", "addr");
                    JArray domainCenterRes = mh.GetData(remoteDbConnInfo.connStr, remoteDbConnInfo.connDB, domainResolverCol, domainCenterFilter.ToString());
                    if (domainCenterRes != null && domainCenterRes.Count != 0)
                    {
                        procesDomainResolver(domainCenterRes);
                    }
                    hasDomainResoverBlockindex = endIndex;
                    if(hasDomainResoverBlockindex != domainResoverBlockindex)
                    {
                        updateRecord(hasDomainResoverBlockindex, domainResolverCol);
                    }
                    log("domainResolver", hasDomainResoverBlockindex, maxDomainResoverBlockindex);
                }

            }
        }

        private void processDomainCenter(JArray domainCenterRes)
        {
            domainCenterRes.GroupBy(p => p["domain"].ToString(), (k, g) =>
            {
                g.GroupBy(pp => pp["parenthash"].ToString(), (kk, gg) =>
                {
                    JObject jo = (JObject)gg.OrderByDescending(ppp => long.Parse(ppp["blockindex"].ToString())).First();
                    string domain = k.ToString();
                    string parenthash = kk.ToString();
                    JObject domainOwnerFilter = new JObject() { { "domain", domain }, { "parenthash", parenthash } };
                    long cnt = mh.GetDataCount(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, domainOwnerFilter.ToString());
                    if (cnt > 0)
                    {
                        jo.Remove("_id");
                        //mh.UpdateData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, new JObject() { { "$set", jo } }.ToString(), domainOwnerFilter.ToString());
                        JArray res = mh.GetData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, domainOwnerFilter.ToString());
                        JObject hasJo = (JObject)res[0];
                        if(hasJo["blockindex"].ToString() != jo["blockindex"].ToString() 
                            || hasJo["register"].ToString() != jo["register"].ToString())
                        {
                            mh.UpdateData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, new JObject() { { "$set", jo } }.ToString(), domainOwnerFilter.ToString());
                        }
                    }
                    else
                    {
                        jo.Add("protocol", "");
                        jo.Add("data", "");
                        mh.PutData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, jo.ToString());
                    }
                    return new JObject();
                }).ToArray();
                return new JObject();
            }).ToArray();
        }

        private void procesDomainResolver(JArray domainResolverRes)
        {
            domainResolverRes.GroupBy(p => p["namehash"].ToString(), (k, g) =>
            {
                JToken jo = g.OrderByDescending(ppp => long.Parse(ppp["blockindex"].ToString())).First();
                string namehash = k.ToString(); ;
                JObject domainOwnerFilter = new JObject() { { "namehash", namehash }, { "blockindex", new JObject() { { "$lte", long.Parse(jo["blockindex"].ToString()) } } } };
                long cnt = mh.GetDataCount(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, domainOwnerFilter.ToString());
                if (cnt > 0)
                {
                    //JObject updateData = new JObject();
                    //updateData.Add("protocol", "addr");
                    //updateData.Add("data", jo["data"].ToString());
                    //mh.UpdateData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, new JObject() { { "$set", updateData } }.ToString(), domainOwnerFilter.ToString());
                    JArray res = mh.GetData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, domainOwnerFilter.ToString());
                    JObject hasJo = (JObject)res[0];
                    if(hasJo["protocol"].ToString() != "addr" 
                        || hasJo["data"].ToString() != jo["data"].ToString())
                    {
                        JObject updateData = new JObject();
                        updateData.Add("protocol", "addr");
                        updateData.Add("data", jo["data"].ToString());
                        mh.UpdateData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, new JObject() { { "$set", updateData } }.ToString(), domainOwnerFilter.ToString());
                    }
                }
                return new JObject();
            }).ToArray();
        }

        private void updateRecord(long maxBlockindex, string contractHash)
        {
            string filter = new JObject() { { "contractHash", contractHash } }.ToString();
            long cnt = mh.GetDataCount(localDbConnInfo.connStr, localDbConnInfo.connDB, domainRecord, filter);
            if (cnt <= 0)
            {
                mh.PutData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainRecord, new JObject() { { "contractHash", contractHash }, { "blockindex", maxBlockindex } }.ToString());
            }
            else
            {
                mh.ReplaceData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainRecord, new JObject() { { "contractHash", contractHash }, { "blockindex", maxBlockindex } }.ToString(), filter);
            }
        }

        private void log(string contractName, long localMaxBlockindex, long remoteMaxBlockindex)
        {
            Console.WriteLine(DateTime.Now + string.Format(" {0}.{1}.localMaxBlockindex/remoteMaxBlockindex processed at {2}/{3}", name(), contractName, localMaxBlockindex, remoteMaxBlockindex));
        }
        private void ping()
        {
            LogHelper.ping(batchInterval, name());
        }
    }
}
