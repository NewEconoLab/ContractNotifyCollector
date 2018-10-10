using MongoDB.Bson;

namespace ContractNotifyCollector.helper
{
    class BsonDecimalHelper
    {
        public static BsonDecimal128 format(decimal value)
        {
            return BsonDecimal128.Create(value);
        }

        public static decimal format(BsonDecimal128 value)
        {
            return value.AsDecimal;
        }
    }
}
