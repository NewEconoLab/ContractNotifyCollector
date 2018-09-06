using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;

namespace ContractNotifyCollector.helper
{
    class TimeConst
    {
        public const long ONE_DAY_SECONDS_Test = 1 * /*24 * 60 * */60 /*测试时5分钟一天*/* 5;
        public const long ONE_DAY_SECONDS = 1 * 24 * 60 * 60;
        public const long TWO_DAY_SECONDS = ONE_DAY_SECONDS * 2;
        public const long THREE_DAY_SECONDS = ONE_DAY_SECONDS * 3;
        public const long FIVE_DAY_SECONDS = ONE_DAY_SECONDS * 5;
        public const long ONE_YEAR_SECONDS = ONE_DAY_SECONDS * 365;

        public static TimeSetter getTimeConst(string test)
        {
            return new TimeSetter(test == "test" ? ONE_DAY_SECONDS_Test : ONE_DAY_SECONDS);
        }

        private static Dictionary<string, TimeSetter> OneDaySecondsDict =
            new Dictionary<string, TimeSetter>() {
                { ".test", new TimeSetter(300) },      //.test
                { ".neo", new TimeSetter(86400) },     //.neo
                { ".all", new TimeSetter(86400) },
            };

        public static TimeSetter getTimeSetter(string root)
        {
            if (OneDaySecondsDict.ContainsKey(root))
            {
                return OneDaySecondsDict.GetValueOrDefault(root);
            }
            return OneDaySecondsDict.GetValueOrDefault(".all");
        }

    }
    class TimeSetter
    {
        public long ONE_DAY_SECONDS;
        public long TWO_DAY_SECONDS;
        public long THREE_DAY_SECONDS;
        public long FIVE_DAY_SECONDS;
        public long ONE_YEAR_SECONDS;

        public TimeSetter(long oneDay)
        {
            ONE_DAY_SECONDS = oneDay;
            TWO_DAY_SECONDS = ONE_DAY_SECONDS * 2;
            THREE_DAY_SECONDS = ONE_DAY_SECONDS * 3;
            FIVE_DAY_SECONDS = ONE_DAY_SECONDS * 5;
            ONE_YEAR_SECONDS = ONE_DAY_SECONDS * 365;
        }
    }


    class RootType
    {
        public const string TEST = ".test";
        public const string NEO = ".neo";
    }
}
