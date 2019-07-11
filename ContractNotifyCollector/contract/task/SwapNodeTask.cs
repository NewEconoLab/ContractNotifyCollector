using ContractNotifyCollector.core;
using ContractNotifyCollector.helper;
using contractNotifyExtractor.Helper;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;
using System.Threading;

namespace ContractNotifyCollector.contract.task
{
    internal class SwapNodeTask : ContractTask
    {
        private MongoDBHelper mh = new MongoDBHelper();

        private string remoteRecord;
        private string remoteState;
        private string localRecord = "swapRecord";
        private string localState = "swapPoolInfo";

        private string nelRPCUrl;
        private string rateRPCUrl;
        private string rateColl = "swapExchangeRateInfo";
        private string priceRPCUrl;
        private string priceColl = "swapExchangePriceInfo";
        private DbConnInfo remoteConn;
        private DbConnInfo localConn;
        private int batchInterval;
        private bool initSuccFlag = false;
        private bool hasCreateIndex = false;


        public SwapNodeTask(string name):base(name) { }

        public override void initConfig(JObject config)
        {
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];

            remoteRecord = cfg["remoteRecordCol"].ToString();
            remoteState = cfg["remoteStateCol"].ToString();
            rateRPCUrl = cfg["rateRPCUrl"].ToString();
            priceRPCUrl = cfg["priceRPCUrl"].ToString();
            batchInterval = int.Parse(cfg["batchInterval"].ToString());

            nelRPCUrl = Config.nelApiUrl;
            remoteConn = Config.notifyDbConnInfo;
            localConn = Config.notifyDbConnInfo;
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
                    processContract();
                    processExchangeRate();
                    processExchangePrice();
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
            var rh = getR();
            var lh = getL();
            if(lh >= rh)
            {
                log(lh, rh);
                return;
            }
            for(long index=lh+1; index<=rh; ++index)
            {
                string findStr = new JObject { { "blockindex", index } }.ToString();
                var queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, remoteState, findStr);
                if(queryRes.Count == 0)
                {
                    updateL(index);
                    log(index, rh);
                    continue;
                }

                handlePoolInfo(queryRes, index);
                //
                updateL(index);
                log(index, rh);
            }

            if (hasCreateIndex) return;
            mh.setIndex(localConn.connStr, localConn.connDB, localState, "{'tokenHash':1,'assetHash':1}", "i_tokenHash_assetHash");
            mh.setIndex(localConn.connStr, localConn.connDB, localState, "{'validState':1,'contractHash':1,'time':1}", "i_validState_contractHash_time");
            hasCreateIndex = true;
        }

        private void handlePoolInfo(JArray ja, long index)
        {
            foreach(var item in ja)
            {
                string displayName = item["displayName"].ToString();
                string tokenHash = item["tokenHash"].ToString();
                string assetHash = item["assetHash"].ToString();
                string ratio = item["ratio"] != null ? item["ratio"].ToString():"0";
                string exchangeFee = ((int)(decimal.Parse(ratio)*10000)).ToString();
                string contractHash = item["exchangeContractHash"] != null ? item["exchangeContractHash"].ToString():"";

                string findStr = new JObject { { "tokenHash", tokenHash },{ "assetHash", assetHash } }.ToString();
                var queryRes = mh.GetData(localConn.connStr, localConn.connDB, localState, findStr);
                if(queryRes.Count == 0)
                {
                    if (displayName == "removeExchange") continue;

                    string newdata = new JObject {
                        {"blockinex", index },
                        {"tokenHash", tokenHash },
                        {"assetHash", assetHash },
                        {"ratio", ratio },
                        {"exchangeFee", exchangeFee },
                        {"contractHash", contractHash },
                        {"tokenTotal", "0" },
                        {"assetTotal", "0" },
                        {"validState", ContractPairValidState.Valid},
                        {"time", 0 }
                    }.ToString();
                    mh.PutData(localConn.connStr, localConn.connDB, localState, newdata);
                    continue;
                }

                if(displayName == "setExchangeFee")
                {
                    string updateStr = new JObject { { "$set", new JObject {
                        //{"blockinex", index },
                        { "ratio", ratio},
                        { "exchangeFee", exchangeFee},
                        { "validState", ContractPairValidState.Valid},
                    } } }.ToString();
                    mh.UpdateData(localConn.connStr, localConn.connDB, localState, updateStr, findStr);
                } 
                if(displayName == "createExchange")
                {
                    string updateStr = new JObject { { "$set", new JObject {
                        //{"blockinex", index },
                        { "contractHash", contractHash},
                        { "validState", ContractPairValidState.Valid},
                    } } }.ToString();
                    mh.UpdateData(localConn.connStr, localConn.connDB, localState, updateStr, findStr);
                }
                if(displayName == "removeExchange")
                {
                    string updateStr = new JObject { { "$set", new JObject {
                        //{"blockinex", index },
                        { "validState", ContractPairValidState.InValid},
                    } } }.ToString();
                    mh.UpdateData(localConn.connStr, localConn.connDB, localState, updateStr, findStr);
                }
            }
        }
        private void updateL(long index)
        {
            string findStr = new JObject { { "counter", localState } }.ToString();
            string newdata = new JObject { { "counter", localState }, { "lastBlockindex", index } }.ToString();
            if (mh.GetDataCount(localConn.connStr, localConn.connDB, localRecord, findStr) == 0)
            {
                mh.PutData(localConn.connStr, localConn.connDB, localRecord, newdata);
            }
            else
            {
                mh.ReplaceData(localConn.connStr, localConn.connDB, localRecord, newdata, findStr);
            }
        }
        private long getL()
        {
            string findStr = new JObject { { "counter", localState } }.ToString();
            var queryRes = mh.GetData(localConn.connStr, localConn.connDB, localRecord, findStr);
            if (queryRes.Count == 0) return -1;
            return long.Parse(queryRes[0]["lastBlockindex"].ToString());
        }
        private long getR()
        {
            string findStr = new JObject { { "counter", "notify" } }.ToString();
            var queryRes = mh.GetData(remoteConn.connStr, remoteConn.connDB, remoteRecord, findStr);
            if (queryRes.Count == 0) return -1;
            return long.Parse(queryRes[0]["lastBlockindex"].ToString());
        }
        private void log(long localHeight, long remoteHeight)
        {
            Console.WriteLine(DateTime.Now + string.Format(" {0}.self processed at {1}/{2}", name(), localHeight, remoteHeight));
        }
        private void ping()
        {
            LogHelper.ping(batchInterval, name());
        }

        private void processContract()
        {
            while(true)
            {
                string findStr = new JObject { { "validState", ContractPairValidState.Valid },{ "contractHash", new JObject { { "$ne", ""} } },{ "time", new JObject { { "$lt", TimeHelper.GetTimeStamp() - 10/**/} } } }.ToString();
                string sortStr = "{'time':1}";
                var queryRes = mh.GetData(localConn.connStr, localConn.connDB, localState, findStr, sortStr, 0, 100);
                if (queryRes.Count == 0) break;

                foreach(var item in queryRes)
                {
                    string tokenHash = item["tokenHash"].ToString();
                    string assetHash = item["assetHash"].ToString();
                    string contractHash = item["contractHash"].ToString();

                    GetLiquidityInfo(contractHash, tokenHash, assetHash, out string tokenTotal, out string assetTotal);
                    if (tokenTotal == "0" && assetTotal == "0") continue;

                    //if(item["tokenTotal"].ToString() != tokenTotal
                    //    || item["assetTotal"].ToString() != assetTotal)
                    {
                        string updateStr = new JObject { {"$set", new JObject {
                            {"tokenTotal",tokenTotal },
                            {"assetTotal",assetTotal },
                            {"time",TimeHelper.GetTimeStamp() }
                        } } }.ToString();
                        mh.UpdateData(localConn.connStr, localConn.connDB, localState, updateStr, findStr);
                    }
                }
            }
        }
        
        private void processExchangeRate()
        {
            // 处理间隔时间1h
            long time = TimeHelper.GetTimeStamp();
            string findStr = "{}";
            string sortStr = "{'time':-1}";
            string fieldStr = "{'time':1}";
            var res = mh.GetDataPagesWithField(localConn.connStr, localConn.connDB, rateColl, fieldStr, 1, 1, sortStr, findStr);
            if(res == null || res.Count == 0 || long.Parse(res[0]["time"].ToString()) < time - TimeHelper.OneHourSecons)
            {
                GetExchangeRate();
            }

        }
        private void processExchangePrice()
        {
            // 处理间隔时间1h
            long time = TimeHelper.GetTimeStamp();
            string findStr = "{}";
            string sortStr = "{'time':-1}";
            string fieldStr = "{'time':1}";
            var res = mh.GetDataPagesWithField(localConn.connStr, localConn.connDB, priceColl, fieldStr, 1, 1, sortStr, findStr);
            if (res == null || res.Count == 0 || long.Parse(res[0]["time"].ToString()) < time - TimeHelper.OneHourSecons)
            {
                GetExchangePrice();
            }
        }


        // 查询合约
        private void GetLiquidityInfo(string schash, string nndhash, string nnchash, out string tokenTotal, out string assetTotal)
        {
            string url = nelRPCUrl;
            string method = "invokescript";
            //string schash = this.schash;
            string scmethod = "getLiquidityInfo";
            string[] scparams = new string[] { toHashParam(nndhash), toHashParam(nnchash) };

            var res = ScriptHelper.InvokeScript(url, method, schash, scmethod, scparams);
            var resJo = JObject.Parse(res);
            var resRe = (JArray)resJo["result"][0]["stack"];
            var v1 = resRe[0]["value"].ToString();
            var v2 = resRe[1]["value"].ToString();
            tokenTotal = v1.getNumStrFromHexStr(0);
            assetTotal = v2.getNumStrFromHexStr(0);
        }
        private string toHashParam(string hash)
        {
            return "(hex160)" + hash;
        }

        // 查询外汇
        private void GetExchangeRate()
        {
            var res = HttpHelper.Get(rateRPCUrl);
            var resJo = JObject.Parse(res);
            if(resJo["result"] != null)
            {
                JObject resData = null;
                string resExchange = null;
                string resResult = null;
                var resJA = (JArray)resJo["result"];
                for(int i=0; i<resJA.Count; ++i)
                {
                    var item = resJA[i];
                    if(item["currencyF"].ToString() == "CNY")
                    {
                        resData = (JObject)item;
                    } else
                    {
                        resExchange = item["exchange"].ToString();
                        resResult = item["result"].ToString();
                    }
                }
                if (resExchange == null) resExchange = "0";
                if (resResult == null) resResult = "0";
                if(resData != null)
                {
                    resData.Add("exchangeT", resExchange);
                    resData.Add("resultT", resResult);
                    resData.Add("time",TimeHelper.GetTimeStamp());
                    mh.PutData(localConn.connStr, localConn.connDB, rateColl, resData.ToString()); ;
                }
            }

        }

        // 查询交易所
        private void GetExchangePrice()
        {
            var res = HttpHelper.Get(priceRPCUrl);
            var resJo = JObject.Parse(res);
            if (resJo["instrument_id"] != null && resJo["instrument_id"].ToString() == "GAS-USDT")
            {
                resJo.Add("time", TimeHelper.GetTimeStamp());
                mh.PutData(localConn.connStr, localConn.connDB, priceColl, resJo.ToString()); ;
            }
        }
    }
    class ContractPairValidState
    {
        public const string Valid = "1";
        public const string InValid = "0";
    }
}
