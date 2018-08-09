using Newtonsoft.Json.Linq;
using System;

namespace ContactNotifyCollector.core
{
    /// <summary>
    /// 合约任务
    /// 
    /// <para>
    /// 对合约处理的任务都需要继承该类
    /// </para>
    /// 
    /// </summary>
    abstract class ContractTask : IContractTask, ITask
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
        public abstract void Init(JObject config);

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
    }
}
