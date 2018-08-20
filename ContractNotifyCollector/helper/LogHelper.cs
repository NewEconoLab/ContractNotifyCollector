using System;
using System.Threading;

namespace ContractNotifyCollector.helper
{
    /// <summary>
    /// 日志输出帮助类
    /// 
    /// </summary>
    class LogHelper
    {
        public static void printEx(Exception ex) 
        {
            string threadName = Thread.CurrentThread.Name;
            Console.WriteLine(threadName + " failed, errMsg:" + ex.Message);
            Console.WriteLine(ex.GetType());
            Console.WriteLine(ex.Message);
            Console.WriteLine(ex.StackTrace);
            Console.WriteLine();
        }

        public static void initThread(string name)
        {
            Thread.CurrentThread.Name = name + Thread.CurrentThread.ManagedThreadId;
        }

        public static void ping(int interval, string name)
        {
            Thread.Sleep(interval);
            Console.WriteLine(DateTime.Now + " " + name + " is running...");
        }

    }
}
