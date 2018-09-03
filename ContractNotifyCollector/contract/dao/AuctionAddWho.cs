using System.Collections.Generic;

namespace ContractNotifyCollector.core.dao
{
    class AuctionAddWho
    {
        public string address { get; set; }
        public decimal totalValue { get; set; }
        public decimal curTotalValue { get; set; }
        public AuctionTime lastTime { get; set; }
        public AuctionTime accountTime { get; set; }
        public AuctionTime getdomainTime { get; set; }
        public List<AuctionAddPrice> addpricelist { get; set; }
    }
}
