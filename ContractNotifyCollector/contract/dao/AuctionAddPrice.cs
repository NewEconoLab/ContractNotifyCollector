
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace ContractNotifyCollector.core.dao
{
    [BsonIgnoreExtraElements]
    class AuctionAddPrice
    {
        public AuctionTime time { get; set; }
        //public decimal value { get; set; }
        public BsonDecimal128 value { get; set; }
        public string isEnd { get; set; }
    }
}
