using MongoDB.Bson;

namespace ContractNotifyCollector.helper
{
    static class DexExtension
    {
        public static BsonDecimal128 format(this decimal value)
        {
            return BsonDecimalHelper.format(value);
        }

        public static decimal format(this BsonDecimal128 value)
        {
            return BsonDecimalHelper.format(value);
        }
    }
}
