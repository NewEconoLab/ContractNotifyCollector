using System;
using System.IO;
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

            PrintEx2File(threadName, ex);
        }
        
        private static void PrintEx2File(string threadName, Exception ex)
        {
            using (FileStream fs = new FileStream("error.log", FileMode.Append, FileAccess.Write, FileShare.None))
            using (StreamWriter w = new StreamWriter(fs))
            {
                PrintErrorLogs(w, threadName, ex);
            }
        }
        private static void PrintErrorLogs(StreamWriter writer, string threadName, Exception ex)
        {
            string nowtime = DateTime.Now.ToString() + " ["+ threadName + "]";
            writer.WriteLine(nowtime + " " + "errinfo:");
            writer.WriteLine(nowtime + " " + ex.GetType());
            writer.WriteLine(nowtime + " " + ex.Message);
            writer.WriteLine(nowtime + " " + ex.StackTrace);
            if (ex is AggregateException ex2)
            {
                foreach (Exception inner in ex2.InnerExceptions)
                {
                    writer.WriteLine();
                    PrintErrorLogs(writer, threadName, inner);
                }
            }
            else if (ex.InnerException != null)
            {
                writer.WriteLine();
                PrintErrorLogs(writer, threadName, ex.InnerException);
            }
        }
        public static void printHeader(string[] ss)
        {
            string logfile = "error.log";
            if (File.Exists(logfile))
            {
                new FileInfo(logfile).MoveTo(logfile+"_bak"+DateTime.Now.ToFileTimeUtc());
            }
            using (FileStream fs = new FileStream(logfile, FileMode.Create, FileAccess.Write, FileShare.None))
            using (StreamWriter w = new StreamWriter(fs))
            {
                string nowtime = DateTime.Now.ToString() + " [" + "main" + "]";
                foreach(string s in ss)
                {
                    w.WriteLine(nowtime + " " + s);
                }
            }
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
