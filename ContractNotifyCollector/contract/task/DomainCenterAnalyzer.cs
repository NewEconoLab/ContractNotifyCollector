using ContractNotifyCollector.helper;
using Newtonsoft.Json.Linq;
using System;
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
        private DbConnInfo localDbConnInfo;
        private DbConnInfo remoteDbConnInfo;
        private string notifyRecordColl;
        private string domainCenterCol;
        private string domainResolverCol;
        private string domainRecord;
        private string domainOwnerCol;
        private string nnsSellingAddr;
        private int batchSize;
        private int batchInterval;
        private bool initSuccFlag = false;
        private bool hasCreateIndex = false;

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
            domainRecord = cfg["domainRecord"].ToString();
            domainOwnerCol = cfg["domainOwnerCol"].ToString();
            nnsSellingAddr = cfg["nnsSellingAddr"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());

            // db info
            localDbConnInfo = Config.localDbConnInfo;
            remoteDbConnInfo = Config.notifyDbConnInfo;
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
                    Thread.Sleep(10 * 000);
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
                processDomainFixSelling();
                return;
            }

            for (long index = localHeight; index <= remoteHeight; index += batchSize)
            {
                long nextIndex = index + batchSize;
                long endIndex = nextIndex < remoteHeight ? nextIndex : remoteHeight;

                // 域名中心
                JObject queryFilter = new JObject() { { "blockindex", new JObject() { { "$gt", index }, { "$lte", endIndex } } }, { "owner", new JObject() { { "$ne", nnsSellingAddr } } } };
                JObject queryField = new JObject() { { "state", 0 } };
                JArray queryRes = mh.GetDataWithField(remoteDbConnInfo.connStr, remoteDbConnInfo.connDB, domainCenterCol, queryField.ToString(), queryFilter.ToString());
                if (queryRes != null && queryRes.Count() > 0)
                {
                    processDomainCenter(queryRes);
                }

                // 解析器
                queryFilter = new JObject() { { "blockindex", new JObject() { { "$gt", index }, { "$lte", endIndex } } }, { "protocol", "addr" } };
                queryRes = mh.GetData(remoteDbConnInfo.connStr, remoteDbConnInfo.connDB, domainResolverCol, queryFilter.ToString());
                if(queryRes != null && queryRes.Count() > 0)
                {
                    procesDomainResolver(queryRes);
                }

                updateRecord(endIndex);
                log(endIndex, remoteHeight);
            }

            // 添加索引
            if (hasCreateIndex) return;
            mh.setIndex(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, "{'namehash':1}", "i_namehash");
            mh.setIndex(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, "{'owner':1}", "i_owner");
            mh.setIndex(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, "{'domain':1,'parenthash':1}", "i_domain_parenthash");
            hasCreateIndex = true;
            processDomainFixSelling();
        }
        
        private void processDomainCenter(JArray domainCenterRes)
        {
            var pRes = domainCenterRes.GroupBy(p => p["namehash"].ToString(), (k, g) => new { namehash = k.ToString(), item = g.OrderByDescending(pg => long.Parse(pg["blockindex"].ToString())).First() }).ToArray();
            foreach(var item in pRes)
            {
                string namehash = item.namehash;
                JObject jo = (JObject)item.item;
                string findStr = new JObject() { {"namehash", namehash } }.ToString();
                long cnt = mh.GetDataCount(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, findStr);
                if (cnt > 0)
                {
                    jo.Remove("_id");
                    JArray res = mh.GetData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, findStr);
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
                            JArray resolveData = mh.GetDataPagesWithField(remoteDbConnInfo.connStr, remoteDbConnInfo.connDB, domainResolverCol, fieldstr, 1, 1, sortstr, findstr);
                            if (resolveData != null && resolveData.Count() > 0)
                            {
                                jo.Add("protocol", resolveData[0]["protocol"]);
                                jo.Add("data", resolveData[0]["data"]);
                            }
                        }
                        mh.UpdateData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, new JObject() { { "$set", jo } }.ToString(), findStr);
                    }
                }
                else
                {
                    jo.Add("protocol", "");
                    jo.Add("data", "");
                    mh.PutData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, jo.ToString());
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
                long cnt = mh.GetDataCount(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, findStr);
                if (cnt > 0)
                {
                    //JObject updateData = new JObject();
                    //updateData.Add("protocol", "addr");
                    //updateData.Add("data", jo["data"].ToString());
                    //mh.UpdateData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, new JObject() { { "$set", updateData } }.ToString(), findStr);
                    JArray res = mh.GetData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, findStr);
                    JObject hasJo = (JObject)res[0];
                    if (hasJo["protocol"].ToString() != "addr"
                        || hasJo["data"].ToString() != jo["data"].ToString())
                    {
                        JObject updateData = new JObject();
                        updateData.Add("protocol", "addr");
                        updateData.Add("data", jo["data"].ToString());
                        mh.UpdateData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainOwnerCol, new JObject() { { "$set", updateData } }.ToString(), findStr);
                    }
                }
            }

        }
        
        private void processDomainFixSelling()
        {
            // remote
            long fixedSellingHeight = 0;
            string findstr = new JObject() { { "counter", "nnsFixedSellingState" } }.ToString();
            JArray res = mh.GetData(remoteDbConnInfo.connStr, remoteDbConnInfo.connDB, "nnsFixedSellingRecord", findstr);
            if (res != null && res.Count() > 0)
            {
                fixedSellingHeight = long.Parse(Convert.ToString(res.OrderBy(p => int.Parse(p["lastBlockindex"].ToString())).ToArray()[0]["lastBlockindex"]));
            }

            // local
            long domainSellingHeight = 0;
            res = mh.GetData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainRecord, findstr);
            if (res != null && res.Count() > 0)
            {
                domainSellingHeight = long.Parse(Convert.ToString(res.OrderBy(p => int.Parse(p["lastBlockindex"].ToString())).ToArray()[0]["lastBlockindex"]));
            }
            if (fixedSellingHeight == 0 || domainSellingHeight >= fixedSellingHeight)
            {
                Console.WriteLine(DateTime.Now + string.Format(" {0}.fixedSelling.self processed at {1}/{2}", name(), fixedSellingHeight, fixedSellingHeight));
                return;
            }

            // data
            findstr = new JObject() { {"blockindex", new JObject() { {"$gt", domainSellingHeight }, { "$lte", fixedSellingHeight} } } }.ToString();
            res = mh.GetData(remoteDbConnInfo.connStr, remoteDbConnInfo.connDB, "nnsFixedSellingState", findstr);
            if(res != null && res.Count() > 0)
            {
                var fixedRes =
                    res.GroupBy(p => p["fullHash"].ToString(), (k, g) =>
                    {
                        return g.OrderByDescending(pg => long.Parse(pg["blockindex"].ToString())).First();
                    }).ToArray();
                foreach (var fixedItem in fixedRes)
                {
                    string fullHash = fixedItem["fullHash"].ToString();
                    var seller = fixedItem["seller"];
                    var price = fixedItem["price"];
                    var launchTime = fixedItem["launchTime"];
                    var displayName = fixedItem["displayName"];

                    findstr = new JObject() { { "namehash", fullHash } }.ToString();
                    string updateStr = new JObject() { {"$set", new JObject() {
                    { "type", displayName},
                    { "seller", seller},
                    { "price", price},
                    { "launchTime", launchTime},
                } } }.ToString();
                    mh.UpdateData(localDbConnInfo.connStr, localDbConnInfo.connDB, "domainOwnerCol", updateStr, findstr);
                }
            }

            // update
            findstr = new JObject() { { "counter", "nnsFixedSellingState" } }.ToString();
            string newdata = new JObject() { { "counter", "nnsFixedSellingState" }, { "lastBlockindex", fixedSellingHeight } }.ToString();
            long cnt = mh.GetDataCount(localDbConnInfo.connStr, localDbConnInfo.connDB, domainRecord, findstr);
            if (cnt <= 0)
            {
                mh.PutData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainRecord, newdata);
            }
            else
            {
                mh.ReplaceData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainRecord, newdata, findstr);
            }

            // log
            Console.WriteLine(DateTime.Now + string.Format(" {0}.fixedSelling.self processed at {1}/{2}", name(), fixedSellingHeight, fixedSellingHeight));

        }
        private void updateRecord(long maxBlockindex)
        {
            string newdata = new JObject() { { "counter", domainOwnerCol }, { "lastBlockindex", maxBlockindex } }.ToString();
            string findstr = new JObject() { { "counter", domainOwnerCol } }.ToString();
            long cnt = mh.GetDataCount(localDbConnInfo.connStr, localDbConnInfo.connDB, domainRecord, findstr);
            if (cnt <= 0)
            {
                mh.PutData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainRecord, newdata);
            }
            else
            {
                mh.ReplaceData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainRecord, newdata, findstr);
            }
        }
        private long getRemoteHeight()
        {
            string findstr = new JObject() { { "counter", "notify" } }.ToString();
            JArray res = mh.GetData(remoteDbConnInfo.connStr, remoteDbConnInfo.connDB, notifyRecordColl, findstr);
            if (res == null || res.Count == 0)
            {
                return 0;
            }
            return long.Parse(Convert.ToString(res.OrderBy(p => int.Parse(p["lastBlockindex"].ToString())).ToArray()[0]["lastBlockindex"]));
        }

        private long getLocalHeight()
        {
            string findstr = new JObject() { { "counter", domainOwnerCol } }.ToString();
            JArray res = mh.GetData(localDbConnInfo.connStr, localDbConnInfo.connDB, domainRecord, findstr);
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
