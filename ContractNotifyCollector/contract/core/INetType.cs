using System;
using System.Collections.Generic;
using System.Text;

namespace ContractNotifyCollector.contract.core
{
    /// <summary>
    /// 网络类型
    /// 
    /// 分为主网和测试
    /// 
    /// </summary>
    interface INetType
    {
        string networkType();
    }
}
