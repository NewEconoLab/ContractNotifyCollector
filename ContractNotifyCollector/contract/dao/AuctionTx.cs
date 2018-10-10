using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Collections.Generic;

namespace ContractNotifyCollector.core.dao
{
    [BsonIgnoreExtraElements]
    class AuctionTx
    {
        public ObjectId _id { get; set; }
        public string auctionId { get; set; }
        public string domain { get; set; }
        public string parenthash { get; set; }
        public string fulldomain { get; set; }
        public long ttl { get; set; }
        public string auctionState { get; set; }
        public AuctionTime startTime { get; set; }
        public string startAddress { get; set; }
        //public decimal maxPrice { get; set; }
        public BsonDecimal128 maxPrice { get; set; }
        public string maxBuyer { get; set; }
        public AuctionTime endTime { get; set; }
        public string endAddress { get; set; }
        public AuctionTime lastTime { get; set; }
        public List<AuctionAddWho> addwholist { get; set; }
    }
}
