using ContractNotifyCollector.core.dao;
using ContractNotifyCollector.helper;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ContractNotifyCollector.core.task
{
    /// <summary>
    /// 状态更新任务
    /// 
    /// </summary>
    class AuctionStateUpdateTask : ContractTask
    {
        private JObject config;
        private MongoDBHelper mh = new MongoDBHelper();

        public AuctionStateUpdateTask(string name) : base(name)
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


        private DbConnInfo localDbConnInfo;
        private string auctionStateColl;
        private int batchSize;
        private int batchInterval;

        private void initConfig()
        {
            localDbConnInfo = Config.localDbConnInfo;

            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name()).ToArray()[0]["taskInfo"];
            
            auctionStateColl = cfg["auctionStateColl"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());
        }
        private void run()
        {
            while(true)
            {
                ping();

                try
                {
                    updateState();
                } catch (Exception ex)
                {
                    // 发生异常不需要退出线程
                    LogHelper.printEx(ex);
                }
            }
        }

        private void updateState()
        {
            //string filter = MongoFieldHelper.toFilter(new string[] { AuctionState.STATE_START, AuctionState.STATE_CONFIRM, AuctionState.STATE_RANDOM }, auctionStateColl).ToString();
            long nowtime = TimeHelper.GetTimeStamp();
            JObject FiveDayFilter = MongoFieldHelper.toFilter(new string[] { AuctionState.STATE_START, AuctionState.STATE_CONFIRM, AuctionState.STATE_RANDOM }, auctionStateColl);
            JObject OneYearFilter = new JObject() { { "auctionState", AuctionState.STATE_END }, { "startTime.blocktime", new JObject() { { "$lt", nowtime - ONE_YEAR_SECONDS } } } };
            string filter = new JObject() { { "$or", new JArray() { FiveDayFilter, OneYearFilter } } }.ToString();
            List<AuctionTx> list = mh.GetData<AuctionTx>(localDbConnInfo.connStr, localDbConnInfo.connDB, auctionStateColl, filter);
            if (list == null || list.Count == 0)
            {
                log(0);
                return;
            }
            foreach (AuctionTx jo in list)
            {
                long starttime = jo.startTime.blocktime;
                string oldState = jo.auctionState;
                string newState = null;
                long endTimeBlocktime = 0;

                // 结束并且超过1Y，直接更新状态
                if (jo.auctionState == AuctionState.STATE_END)
                {
                    newState = AuctionState.STATE_EXPIRED;
                    updateAuctionState(newState, oldState, jo.auctionId);
                    continue;
                }

                /**
                    * 状态判断逻辑：
                    * 0. 开标为开标期
                    * 1. 小于等于三天，确定期
                    * 2. 大于三天，则：
                    *          a. 结束时间无值且前三天无人出价，则流拍
                    *          b. 超过1Y，则过期
                    *          c. 超过5D，则结束
                    *          d. (3,5)结束时间有值且大于开拍时间，则结束
                    *          e. (3,5)结束时间无值且最后出价在开拍后两天内，则结束
                    *          f. (3,5)其余为随机
                    */
                if (nowtime - starttime <= THREE_DAY_SECONDS)
                {
                    // 小于三天
                    newState = AuctionState.STATE_CONFIRM;
                }
                else
                {
                    // 大于三天
                    if (jo.lastTime == null || jo.lastTime.blockindex == 0)
                    {
                        // (3,5)结束时间无值且前三天无人出价，则流拍
                        newState = AuctionState.STATE_ABORT;
                    }
                    else if (nowtime > starttime + ONE_YEAR_SECONDS)
                    {
                        // 超过1Y，则过期
                        newState = AuctionState.STATE_EXPIRED;
                    }
                    else if (nowtime >= starttime + FIVE_DAY_SECONDS)
                    {
                        // 超过5D，则结束
                        newState = AuctionState.STATE_END;
                        endTimeBlocktime = starttime + FIVE_DAY_SECONDS;
                    }
                    else if (jo.endTime != null && jo.endTime.blocktime > starttime)
                    {
                        // (3,5)结束时间有值，且大于开拍时间，则结束
                        newState = AuctionState.STATE_END;
                    }
                    else if (jo.lastTime.blocktime <= starttime + TWO_DAY_SECONDS)
                    {
                        // (3,5)结束时间无值且最后出价在开拍后两天内，则超时3D结束
                        newState = AuctionState.STATE_END;
                    }
                    else
                    {
                        // 其余为随机期
                        newState = AuctionState.STATE_RANDOM;
                    }
                }

                if (oldState != newState)
                {
                    updateAuctionState(newState, oldState, jo.auctionId, endTimeBlocktime);
                }

            }
            log(list.Count());
        }
        private void updateAuctionState(string newState, string oldState, string auctionId, long endTimeBlocktime=0)
        {
            string findstr = new JObject() { { "auctionState", oldState }, { "auctionId", auctionId } }.ToString();
            string newdata = new JObject() { { "$set", new JObject() { { "auctionState", newState } } } }.ToString();
            mh.UpdateData(localDbConnInfo.connStr, localDbConnInfo.connDB, auctionStateColl, newdata, findstr);
        }

        private const long ONE_DAY_SECONDS = 1 * /*24 * 60 * */60 /*测试时5分钟一天*/* 5;
        private const long TWO_DAY_SECONDS = ONE_DAY_SECONDS * 2;
        private const long THREE_DAY_SECONDS = ONE_DAY_SECONDS * 3;
        private const long FIVE_DAY_SECONDS = ONE_DAY_SECONDS * 5;
        private const long ONE_YEAR_SECONDS = ONE_DAY_SECONDS * 365;

        private void log(long count)
        {
            Console.WriteLine(DateTime.Now + string.Format(" {0}.self processed,cnt: {1}", name(), count));
        }

        private void ping()
        {
            LogHelper.ping(batchInterval, name());
        }
    }
}
