using MongoDB.Bson.Serialization.Attributes;

namespace Aco228.MongoDb.Base;

[BsonIgnoreExtraElements]
[Serializable]
public abstract class MongoAccountDocument : MongoDocument
{
    public required Guid AccountId { get; set; }
}