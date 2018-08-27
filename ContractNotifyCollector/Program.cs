using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ContractNotifyCollector.core;
using ContractNotifyCollector.core.task;
using ContractNotifyCollector.helper;

/// <summary>
/// 合约汇总进程
/// 
/// </summary>
namespace ContractNotifyCollector
{
    class Program
    {
        /// <summary>
        /// 添加任务列表
        /// 
        /// </summary>
        private static void InitTask()
        {
            Config.loadConfig("config.json");
            AddTask(new ContractCollector("ContractCollector"));
            AddTask(new DomainSellAnalyzer("DomainSellAnalyzer"));
            AddTask(new DomainCenterAnalyzer("DomainCenterAnalyzer"));
            AddTask(new DomainCenterAnalyzer("NewDomainCenterAnalyzer"));
            AddTask(new NewDomainSellAnalyzer("NewDomainSellAnalyzer"));
        }

        /// <summary>
        /// 启动任务列表
        /// 
        /// </summary>
        private static void StartTask()
        {
            foreach (var func in list)
            {
                new Task(() => {
                    func.Init(Config.getConfig());
                    func.Start();
                }).Start();
                
            }
        }

        private static List<ITask> list = new List<ITask>();
        private static void AddTask(ITask handler)
        {
            list.Add(handler);
        }

        /// <summary>
        /// 程序启动入口
        /// 
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            ProjectInfo.head();
            InitTask();
            StartTask();
            ProjectInfo.tail();
            while (true)
            {
                System.Threading.Thread.Sleep(1000);
            }
        }
    }

    class ProjectInfo
    {
        static private string appName = "ContactNotifyCollector";
        public static void head()
        {
            string[] info = new string[] {
                "*** Start to run "+appName,
                "*** Auth:tsc",
                "*** Version:0.0.0.1",
                "*** CreateDate:2018-07-25",
                "*** LastModify:2018-08-08"
            };
            foreach (string ss in info)
            {
                log(ss);
            }
            LogHelper.printHeader(info);
        }
        public static void tail()
        {
            log("Program." + appName + " exit");
        }

        static void log(string ss)
        {
            Console.WriteLine(DateTime.Now + " " + ss);
        }
    }
}
