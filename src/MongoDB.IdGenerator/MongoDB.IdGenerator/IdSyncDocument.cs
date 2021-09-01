using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace SmorcIRL.MongoDB.IdGenerator
{
    public class IdSyncDocument
    {
        [BsonId]
        public ObjectId Id { get; set; }

        public string Tag { get; set; }

        public long StartValue { get; set; }

        public int HighValue { get; set; }

        public int LowValue { get; set; }
    }
}