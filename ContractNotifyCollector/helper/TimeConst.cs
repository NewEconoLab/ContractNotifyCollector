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
}
