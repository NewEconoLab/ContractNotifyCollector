using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Collections.Generic;

namespace ContractNotifyCollector.core.dao
{
    [BsonIgnoreExtraElements]
    class AuctionAddWho
    {
        public string address { get; set; }
        //public decimal totalValue { get; set; }
        public BsonDecimal128 totalValue { get; set; }
        //public decimal curTotalValue { get; set; }
        public BsonDecimal128 curTotalValue { get; set; }
        public AuctionTime lastTime { get; set; }
        public AuctionTime accountTime { get; set; }
        public AuctionTime getdomainTime { get; set; }
        public List<AuctionAddPrice> addpricelist { get; set; }
    }
}
