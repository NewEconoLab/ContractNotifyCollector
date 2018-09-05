using ContractNotifyCollector.contract.core;
using ContractNotifyCollector.helper;
using Newtonsoft.Json.Linq;
using System;

namespace ContractNotifyCollector.core
{
    /// <summary>
    /// 合约任务
    /// 
    /// <para>
    /// 对合约处理的任务都需要继承该类
    /// </para>
    /// 
    /// </summary>
    abstract class ContractTask : IContractTask, ITask, INetType
    {
        /* 任务名称 */
        private string taskname;
        public string name()
        {
            return taskname;
        }

        protected ContractTask(string taskname)
        {
            this.taskname = taskname;
        }

        /// <summary>
        /// 初始化配置
        /// 
        /// </summary>
        /// <param name="config"></param>
        public void Init(JObject config)
        {
            startNetType = config["startNetType"].ToString();
            try
            {
                initConfig(config);
                LogHelper.printLog("InitTask success:" + name() + "_" + networkType());
            } catch(Exception ex)
            {
                LogHelper.printLog("InitTask failed:" + name() + "_" + networkType() + ",exMsg:" + ex.Message);
            }
        }
        public abstract void initConfig(JObject config);

        /// <summary>
        /// 启动任务
        /// 
        /// </summary>
        public void Start()
        {
            LogHelper.initThread(taskname);
            try
            {
                startTask();
            }
            catch (Exception ex)
            {
                LogHelper.printEx(ex);
            }
        }
        public abstract void startTask();

        /*启动网络类型*/
        private string startNetType;
        public string networkType()
        {
            return startNetType;
        }
    }
}
