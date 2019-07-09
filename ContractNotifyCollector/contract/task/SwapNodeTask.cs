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

        private string nelRPCUrl;
        private string schash;
        private string nnchash;
        private string nndhash;
        private string poolColl = "swapPoolInfo";
        private string rateRPCUrl;
        private string rateColl = "swapExchangeRateInfo";
        private string priceRPCUrl;
        private string priceColl = "swapExchangePriceInfo";
        private DbConnInfo localConn;
        private int batchInterval;
        private bool initSuccFlag = false;


        public SwapNodeTask(string name):base(name) { }

        public override void initConfig(JObject config)
        {
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];

            schash = cfg["schash"].ToString();
            nnchash = cfg["nnchash"].ToString();
            nndhash = cfg["nndhash"].ToString();
            rateRPCUrl = cfg["rateRPCUrl"].ToString();
            priceRPCUrl = cfg["priceRPCUrl"].ToString();
            batchInterval = int.Parse(cfg["batchInterval"].ToString());

            nelRPCUrl = Config.nelApiUrl;
            localConn = Config.localDbConnInfo;
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
            processContract();
            processExchangeRate();
            processExchangePrice();
        }

        private void processContract()
        {
            // 处理间隔时间2s
            GetLiquidityInfo(schash, nndhash, nnchash, out string tokenTotal, out string assetTotal);
            

            string findStr = new JObject { { "hash", schash} }.ToString();
            var queryRes = mh.GetData(localConn.connStr, localConn.connDB, poolColl, findStr);
            if(queryRes == null ||queryRes.Count == 0)
            {
                string newdata = new JObject {
                    {"hash", schash },
                    {"tokenHash", nndhash },
                    {"assetHash", nnchash },
                    {"tokenTotal", tokenTotal },
                    {"assetTotal", assetTotal },
                    {"time", TimeHelper.GetTimeStamp() }
                }.ToString();
                mh.PutData(localConn.connStr, localConn.connDB, poolColl, newdata);
                return;
            } 
            if(queryRes[0]["tokenTotal"].ToString() != tokenTotal 
                || queryRes[0]["assetTotal"].ToString() != assetTotal
                )
            {
                string updateStr = new JObject { {"$set", new JObject {
                    {"tokenTotal",tokenTotal },
                    {"assetTotal",assetTotal },
                    {"time",TimeHelper.GetTimeStamp() }
                } } }.ToString();
                mh.UpdateData(localConn.connStr, localConn.connDB, poolColl, updateStr, findStr);
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
            tokenTotal = v1.getNumStrFromHexStr(2);
            assetTotal = v2.getNumStrFromHexStr(8);
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

        private void ping()
        {
            LogHelper.ping(batchInterval, name());
        }
    }
}
