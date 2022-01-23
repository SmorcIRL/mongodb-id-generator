using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace SmorcIRL.MongoDB.IdGenerator
{
    [BsonIgnoreExtraElements]
    public class CustomIdentifierDocument
    {
        [BsonId]
        public ObjectId Id { get; set; }
        
        public long StartValue { get; set; }

        public int HighValue { get; set; }

        public int LowValue { get; set; }
    }
}