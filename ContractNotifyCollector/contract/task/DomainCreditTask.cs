using ContractNotifyCollector.core;
using ContractNotifyCollector.helper;
using MongoDB.Bson.Serialization.Attributes;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace ContractNotifyCollector.contract.task
{
    class DomainCreditTask : ContractTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo localConn;
        private DbConnInfo remoteConn;
        private DbConnInfo blockConn;
        private string nnsDomainCreditRecordColl;
        private string nnsDomainCreditStateColl;
        private string remoteConnRecordColl;
        private string remoteConnStateColl;
        private int batchSize;
        private int batchInterval;
        private bool initSuccFlag = false;
        private bool hasCreateIndex = false;

        public DomainCreditTask(string name) : base(name)
        {

        }
        public override void initConfig(JObject config)
        {
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];
            nnsDomainCreditRecordColl = cfg["nnsDomainCreditRecordColl"].ToString();
            nnsDomainCreditStateColl = cfg["nnsDomainCreditStateColl"].ToString();
            remoteConnRecordColl = cfg["remoteConnRecordColl"].ToString();
            remoteConnStateColl = cfg["remoteConnStateColl"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());

            //
            localConn = Config.localDbConnInfo;
            remoteConn = Config.notifyDbConnInfo;
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
            long remoteHeight = getRemoteHeight();
            long localHeight = getLocalHeight();
            if (remoteHeight <= localHeight) return;

            for (long startIndex = localHeight; startIndex <= remoteHeight; startIndex += batchSize)
            {
                long nextIndex = startIndex + batchSize;
                long endIndex = nextIndex < remoteHeight ? nextIndex : remoteHeight;
                string findStr = new JObject() {
                    { "blockindex", new JObject() { {"$gt", startIndex }, { "$lte", endIndex } } },
                }.ToString();

                string fieldStr = new JObject() { { "state", 0 }, { "_id", 0 } }.ToString();
                var query = mh.GetDataWithField(remoteConn.connStr, remoteConn.connDB, remoteConnStateColl, fieldStr, findStr);
                if (query == null || query.Count == 0)
                {
                    updateRecord(endIndex);
                    log(endIndex, remoteHeight);
                    continue;
                }

                var blockindexArr = query.Select(p => long.Parse(p["blockindex"].ToString())).Distinct().OrderBy(p => p).ToArray();
                int cnt = blockindexArr.Count();
                for(int i=0; i<cnt; ++i)
                {
                    var blockindex = blockindexArr[i];
                    var res = query.Where(p => long.Parse(p["blockindex"].ToString()) == blockindex).ToArray();
                    var r1 = res.Where(p => p["displayName"].ToString() == "addrCreditRegistered").ToArray();
                    if(r1 != null && r1.Count() > 0)
                    {
                        processAddrCreditRegistered(r1, blockindex);
                    }
                    var r2 = res.Where(p => p["displayName"].ToString() == "addrCreditRevoke").ToArray();
                    if (r2 != null && r2.Count() > 0)
                    {
                        processAddrCreditRevoke(r2, blockindex);
                    }

                    updateRecord(blockindex);
                    log(blockindex, remoteHeight);
                }
            }


            // 添加索引
            if (hasCreateIndex) return;
            mh.setIndex(localConn.connStr, localConn.connDB, nnsDomainCreditStateColl, "{'address':1}", "i_address");
            mh.setIndex(localConn.connStr, localConn.connDB, nnsDomainCreditStateColl, "{'fulldomain':1}", "i_fullDomain");
            mh.setIndex(localConn.connStr, localConn.connDB, nnsDomainCreditStateColl, "{'namehash':1}", "i_namehash");
            mh.setIndex(localConn.connStr, localConn.connDB, nnsDomainCreditStateColl, "{'blockindex':1}", "i_blockindex");
            hasCreateIndex = true;
        }

        private void processAddrCreditRevoke(JToken[] res, long blockindex)
        {
            foreach(var item in res)
            {
                string findStr = new JObject() { { "address", item["addr"].ToString() } }.ToString();
                string updateStr = new JObject() { { "$set", new JObject() { { "namehash", "" }, { "fulldomain", "" }, { "ttl", 0 }, { "lastBlockindex", blockindex }, { "lastTxid", item["txid"] } } } }.ToString();
                mh.UpdateData(localConn.connStr, localConn.connDB, nnsDomainCreditStateColl, updateStr, findStr);
            }
        }
        private void processAddrCreditRegistered(JToken[] res, long blockindex)
        {
            foreach(var item in res)
            {
                string addr = item["addr"].ToString();
                string namehash = item["namehash"].ToString();
                string fulldomain = item["fullDomainName"].ToString();
                long ttl = long.Parse(item["ttl"].ToString());
                string txid = item["txid"].ToString();
                
                string findStr = new JObject() { {"fulldomain", fulldomain } }.ToString();
                var queryRes = mh.GetData<DomainCreditStateInfo>(localConn.connStr, localConn.connDB, nnsDomainCreditStateColl, findStr);
                if(queryRes == null || queryRes.Count == 0)
                {
                    var newdata = new DomainCreditStateInfo
                    {
                        blockindex = blockindex,
                        address = addr,
                        namehash = namehash,
                        fulldomain = fulldomain,
                        ttl = ttl,
                        lastBlockindex = blockindex,
                        lastTxid = txid
                    };
                    mh.PutData<DomainCreditStateInfo>(localConn.connStr, localConn.connDB, nnsDomainCreditStateColl, newdata);
                } else
                {
                    var replacedata = queryRes[0];
                    replacedata.namehash = namehash;
                    replacedata.fulldomain = fulldomain;
                    replacedata.ttl = ttl;
                    replacedata.lastBlockindex = blockindex;
                    replacedata.lastTxid = txid;
                    mh.ReplaceData<DomainCreditStateInfo>(localConn.connStr, localConn.connDB, nnsDomainCreditStateColl, replacedata, findStr);
                }
            }
        }

        [BsonIgnoreExtraElements]
        class DomainCreditStateInfo
        {
            public long blockindex { get; set; }
            public string fulldomain { get; set; }
            public string namehash { get; set; }
            public string address { get; set; }
            public long ttl { get; set; }
            public long lastBlockindex { get; set; }
            public string lastTxid { get; set; }
        }
        private void updateRecord(long height)
        {
            string newdata = new JObject() { { "counter", nnsDomainCreditStateColl }, { "lastBlockindex", height } }.ToString();
            string findstr = new JObject() { { "counter", nnsDomainCreditStateColl } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, nnsDomainCreditRecordColl, findstr);
            if (res == null || res.Count == 0)
            {
                mh.PutData(localConn.connStr, localConn.connDB, nnsDomainCreditRecordColl, newdata);
            }
            else
            {
                mh.ReplaceData(localConn.connStr, localConn.connDB, nnsDomainCreditRecordColl, newdata, findstr);
            }
        }
        private long getLocalHeight()
        {
            string findStr = new JObject() { { "counter", nnsDomainCreditStateColl } }.ToString();
            JArray res = mh.GetData(localConn.connStr, localConn.connDB, nnsDomainCreditRecordColl, findStr);
            if (res == null || res.Count == 0)
            {
                return 0;
            }
            return long.Parse(res[0]["lastBlockindex"].ToString());
        }
        private long getRemoteHeight()
        {
            string findStr = new JObject() { { "counter", "notify" } }.ToString();
            JArray res = mh.GetData(remoteConn.connStr, remoteConn.connDB, remoteConnRecordColl, findStr);
            if (res == null || res.Count == 0)
            {
                return 0;
            }
            return long.Parse(res[0]["lastBlockindex"].ToString());
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
