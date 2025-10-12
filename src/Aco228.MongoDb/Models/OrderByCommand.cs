using MongoDB.Driver;

namespace Aco228.MongoDb.Models;

public enum OrderByType
{
    ASC,
    DESC,
}

public class OrderByCommand
{
    public required string ParameterName { get; set; }
    public OrderByType Type { get; set; } = OrderByType.DESC;

    public SortDefinition<TDocument> GetDefinition<TDocument>()
        => Type == OrderByType.ASC
            ? Builders<TDocument>.Sort.Ascending(ParameterName)
            : Builders<TDocument>.Sort.Descending(ParameterName);
}