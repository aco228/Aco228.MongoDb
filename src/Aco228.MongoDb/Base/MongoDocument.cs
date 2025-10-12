using Aco228.MongoDb.Extensions;
using MessagePack;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using Newtonsoft.Json;

namespace Aco228.MongoDb.Base;

[Serializable]
[BsonIgnoreExtraElements]
public abstract class MongoDocument
{
    [BsonId]
    [JsonIgnore] [IgnoreMember] 
    [BsonRepresentation(BsonType.String)]
    public ObjectId Id { get; set; }

    [IgnoreMember] public DateTime CreatedUtc { get; set; } = DateTime.MinValue;
    [IgnoreMember] public DateTime UpdatedUtc { get; set; } = DateTime.MinValue;
    
    [BsonIgnore] [JsonIgnore] [IgnoreMember] internal string? Hash { get; set; }
    [BsonIgnore] [JsonIgnore] [IgnoreMember] public bool ShouldUpdate { get; set; } = false;

    public virtual string? GetId() => null;

    public void CreateHash()
    {
        if (Id == ObjectId.Empty)
            return;
        
        Hash = GetCurrentHash();
    }

    public bool AnyChange(bool isHashEmpty = false)
    {
        if (string.IsNullOrEmpty(Hash) && !isHashEmpty)
            return true;

        return !string.Equals(Hash, GetCurrentHash());
    }

    private string GetCurrentHash()
    {
        var json = JsonConvert.SerializeObject(this);
        return json.Base64Encode();
    }
}