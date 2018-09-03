using System;
using System.Collections.Generic;
using System.Text;

namespace ContractNotifyCollector.core.dao
{
    class AuctionState
    {
        public const string STATE_START = "0101";   // 开标
        public const string STATE_CONFIRM = "0201"; // 确定期
        public const string STATE_RANDOM = "0301";  // 随机期
        public const string STATE_END = "0401"; // 触发结束、3D/5D到期结束
        public const string STATE_ABORT = "0501";   // 流拍
        public const string STATE_EXPIRED = "0601"; // 过期
    }
}
