using System.Reflection;
using Aco228.MongoDb.Base;
using Arbitrage.Mongo.Core;
using MongoDB.Driver;

namespace Aco228.MongoDb.Infrastructure;

public class ProjectionMapper<TProject, TDocument> where TDocument : MongoDocument
{
    private List<PropertyInfo> _documentProperties = new();

    private Dictionary<string, PropertyInfo> _projectionProperties = new();

    private void Prepare()
    {
        _documentProperties = typeof(TDocument).GetProperties().ToList();
        foreach (var propertyInfo in typeof(TProject).GetProperties().ToList())
        {
            var attribute = propertyInfo.GetCustomAttribute<ProjectMapAttribute>();
            if (attribute?.Ignore == true) 
                continue;
            _projectionProperties.Add(attribute?.PropertyName ?? propertyInfo.Name, propertyInfo);
        }
    }

    public ProjectionDefinition<TDocument> GetProjection()
    {
        Prepare();
        
        var projection = Builders<TDocument>.Projection.Include("Id");
        foreach (var prop in _projectionProperties)
            projection = projection.Include(prop.Key);
        
        return projection;
    }

    public TProject CreateObjectFrom(TDocument document)
    {
        var result = Activator.CreateInstance<TProject>();
        foreach (var (propertyName, propertyInfo) in _projectionProperties)
        {
            var documentProp = _documentProperties.FirstOrDefault(x => x.Name.Equals(propertyName));
            if (documentProp == null)
                continue;
            
            propertyInfo.SetValue(result, documentProp.GetValue(document));
        }

        return result;
    }
}