
using MongoDB.Bson.Serialization.Attributes;

namespace ContractNotifyCollector.core.dao
{
    [BsonIgnoreExtraElements]
    class AuctionTime
    {
        public long blockindex { get; set; }
        public long blocktime { get; set; }
        public string txid { get; set; }
    }
}
