using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;

namespace Aco228.MongoDb.Base.Services;

public interface IMongoDbContext
{
    string DatabaseName { get; }
    IMongoDatabase GetDatabase();
}

public abstract class MongoDbContext : IMongoDbContext
{
    private MongoClient? _client;
    
    public abstract string DatabaseName { get; }
    

    protected abstract string GetConnectionString();

    public IMongoDatabase GetDatabase()
    {
        if (_client == null)
        {
            var settings = MongoClientSettings.FromConnectionString(GetConnectionString());
            settings.ServerApi = new ServerApi(ServerApiVersion.V1);
            settings.SocketTimeout = TimeSpan.FromMinutes(5); // Adjust as needed
            settings.ConnectTimeout = TimeSpan.FromSeconds(10); // Adjust as needed
            settings.MaxConnectionIdleTime = TimeSpan.FromMinutes(5);
            settings.MaxConnectionLifeTime = TimeSpan.FromMinutes(10);
        
            _client = new MongoClient(settings);
        }
        var pack = new ConventionPack
        {
            new IgnoreIfNullConvention(true)
        };

        ConventionRegistry.Register("Ignore null values globally", pack, t => true);
        return _client.GetDatabase(DatabaseName);
    }
}