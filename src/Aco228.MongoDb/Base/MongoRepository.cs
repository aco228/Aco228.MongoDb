using System.Collections.Concurrent;
using System.Linq.Expressions;
using Aco228.Common;
using Aco228.Common.Extensions;
using Aco228.Common.Models;
using Aco228.MongoDb.Base.Services;
using Aco228.MongoDb.Extensions;
using Aco228.MongoDb.Infrastructure;
using Aco228.MongoDb.Models;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace Aco228.MongoDb.Base;

public class MongoRepository<TDbContext, TDocument> : IMongoRepository<TDbContext, TDocument>
    where TDbContext : IMongoDbContext
    where TDocument : MongoDocument
{
    public bool EnforceAccountIsolation {get; set; } = true;
    public string CollectionName => GetCollectionName(typeof(TDocument));

    private ConcurrentDictionary<ObjectId, TDocument> _transactionUpdates = new();
    private ConcurrentList<TDocument> _transactionInserts = new();
    private ConcurrentList<TDocument> _transactionDeletes = new();
    
    private IMongoCollection<TDocument> _collection;
    private IMongoDatabase _database;

    protected void Initialize(IMongoDbContext context)
    {
        _database = context!.GetDatabase();
        _collection = _database.GetCollection<TDocument>(GetCollectionName(typeof(TDocument)));
    }

    private string? GetCollectionName(Type documentType)
    {
        return ((BsonCollectionAttribute) documentType.GetCustomAttributes(
                typeof(BsonCollectionAttribute),
                true)
            .FirstOrDefault())?.CollectionName;
    }

    public List<TDocument> TryGetAll()
    {
        try
        {
            return AsQueryable().ToList();
        }
        catch
        {
            return new();
        }
    }

    public IMongoCollection<TDocument> GetCollection() => _collection;

    public virtual Task CreateIndex(string nameOfParameter, bool isUnique)
    {
        var options = new CreateIndexOptions() { Unique = isUnique };
        var field = new StringFieldDefinition<TDocument>(nameOfParameter);
        var indexDefinition = new IndexKeysDefinitionBuilder<TDocument>().Ascending(field);
        return _collection.Indexes.CreateOneAsync(indexDefinition, options);
    }
    
    public virtual void DeleteIndex(string nameOfParameter)
    {
        _collection.Indexes.DropOne(nameOfParameter, CancellationToken.None);
    }

    public IMongoRepoTransactionalManager<TDbContext, TDocument> CreateTransactionManager(uint numberOfDocuments = 15)
        => new MongoRepoTransactionalManager<TDbContext, TDocument>(this, numberOfDocuments);

    public IEnumerable<T> ProjectAll<T>()
    {
        var projectionMapper = new ProjectionMapper<T, TDocument>();
        var projection = projectionMapper.GetProjection();
        
        var enumeration =  _collection.Find(x => x != null).Project<TDocument>(projection).ToEnumerable();
        
        var result = new List<T>();
        foreach (var element in enumeration)
            result.Add(projectionMapper.CreateObjectFrom(element));

        return result;
    }

    public virtual IQueryable<TDocument> AsQueryable()
    {
        return _collection.AsQueryable();
    }

    public virtual IEnumerable<TDocument> FilterBy(
        Expression<Func<TDocument, bool>> filterExpression)
    {
        var result =  _collection.Find(filterExpression).ToEnumerable();
        return result;
    }

    public IEnumerable<T> ProjectFilterBy<T>(Expression<Func<TDocument, bool>> filterExpression)
    {
        var projectionMapper = new ProjectionMapper<T, TDocument>();
        var projection = projectionMapper.GetProjection();
        
        var enumeration =  _collection.Find(filterExpression).Project<TDocument>(projection).ToEnumerable();
        
        var result = new List<T>();
        foreach (var element in enumeration)
            result.Add(projectionMapper.CreateObjectFrom(element));

        return result;
    }


    public virtual async Task<List<TDocument>> FilterByAsync(
        Expression<Func<TDocument, bool>> filterExpression,
        int? limit = null, int? skip = null, OrderByCommand? orderByCommand = null
    )
    {
        FindOptions<TDocument> options = new FindOptions<TDocument>();
        if(limit != null) options.Limit = limit;
        if(skip != null) options.Skip = skip; 
        if (orderByCommand != null) options.Sort = orderByCommand.GetDefinition<TDocument>();
        
        var result = await _collection.FindAsync(filterExpression, options);
        return result.ToEnumerable().ToList();
    }

    public async Task<List<TProperty>> FilterPropertyByAsync<TProperty>( Expression<Func<TDocument, bool>> filterExpression, Expression<Func<TDocument, TProperty>> selector)
    {
        var projection = Builders<TDocument>.Projection.Expression(selector);

        var results = await _collection
            .Find(filterExpression)
            .Project(projection)
            .ToListAsync();

        return results;
    }

    public async Task<T?> ProjectFindOneByAsync<T>(Expression<Func<TDocument, bool>> filterExpression)
    {
        var projectionMapper = new ProjectionMapper<T, TDocument>();
        var projection = projectionMapper.GetProjection();
        var result =  await _collection.Find(filterExpression).Project<T>(projection).FirstOrDefaultAsync();
        return result;
    }

    public async Task<List<T>> ProjectFilterByAsync<T>(Expression<Func<TDocument, bool>> filterExpression)
    {
        var projectionMapper = new ProjectionMapper<T, TDocument>();
        var projection = projectionMapper.GetProjection();
        
        var enumeration =  await _collection.Find(filterExpression).Project<TDocument>(projection).ToListAsync();
        
        var result = new List<T>();
        foreach (var element in enumeration)
            result.Add(projectionMapper.CreateObjectFrom(element));

        return result;
    }

    public async IAsyncEnumerable<TDocument> FilterInBatchAsync(Expression<Func<TDocument, bool>> filterExpression, int batchSize = 100, OrderByCommand? orderByCommand = null)
    {
        FindOptions<TDocument> options = new FindOptions<TDocument>
        {
            BatchSize = batchSize,
            NoCursorTimeout = false, 
        };
        if (orderByCommand != null)
            options.Sort = orderByCommand.GetDefinition<TDocument>();
        
        using var cursor = await _collection.FindAsync(filterExpression, options);

        int max = 15000;
        List<TDocument>? current = null;

        for(int i = 0; i < max; i++)
        {
            var task = cursor.MoveNextAsync();
            if (current != null && current.Any())
            {
                foreach (var elem in current)
                    yield return elem;

                current = null;
            }

            await task;
            
            if(task.Result == false)
                break;

            current = cursor.Current.ToList();
        }
    }

    public virtual IEnumerable<TProjected> FilterBy<TProjected>(
        Expression<Func<TDocument, bool>> filterExpression,
        Expression<Func<TDocument, TProjected>> projectionExpression)
    {
        var result = _collection.Find(filterExpression).Project(projectionExpression).ToEnumerable();
        return result;
    }

    public virtual TDocument? FindOne(Expression<Func<TDocument, bool>> filterExpression)
    {
        var result = _collection.Find(filterExpression).FirstOrDefault();
        return result;
    }

    public virtual async Task<TDocument?> FindOneAsync(Expression<Func<TDocument, bool>> filterExpression)
    {
        var result =  await _collection.Find(filterExpression).FirstOrDefaultAsync();
        return result;
    }

    public async Task<long> Count(Expression<Func<TDocument, bool>> filterExpression)
    {
        var result =  await _collection.CountDocumentsAsync(filterExpression);
        return result;
    }

    public virtual TDocument? FindById(string id)
    {
        var objectId = ObjectId.Parse(id);
        var filter = Builders<TDocument>.Filter.Eq(doc => doc.Id, objectId);
        var result = _collection.Find(filter).SingleOrDefault();
        return result;
    }

    public virtual TDocument? FindById(ObjectId objectId)
    {
        var filter = Builders<TDocument?>.Filter.Eq(doc => doc.Id, objectId);
        var result = _collection.Find(filter).SingleOrDefault();

        return result;
    }

    public virtual async Task<TDocument?> FindByIdAsync(string id)
    {
        var objectId = ObjectId.Parse(id);
        var filter = Builders<TDocument?>.Filter.Eq(doc => doc.Id, objectId);
        var result = await _collection.Find(filter).SingleOrDefaultAsync();

        return result;
    }

    public async Task<TDocument?> FindByIdAsync(ObjectId objectId)
    {
        var filter = Builders<TDocument?>.Filter.Eq(doc => doc.Id, objectId);
        var result = await _collection.Find(filter).SingleOrDefaultAsync();

        return result;
    }

    public Task<TDocument> Reload(TDocument document)
        => FindByIdAsync(document.Id)!;

    public void InsertOrUpdate(TDocument document)
    {
        if (document.Id == ObjectId.Empty)
        {
            var genericId = document.GetId();
            document.Id = !string.IsNullOrEmpty(genericId) 
                ? ObjectId.Parse(genericId.StringToLimitHex()) 
                : ObjectId.GenerateNewId();
            document.CreatedUtc = DateTime.UtcNow;
            document.UpdatedUtc = DateTime.UtcNow;
        }
        else
        {
            document.UpdatedUtc = DateTime.UtcNow;
        }
        
        var filter = Builders<TDocument>.Filter.Eq(doc => doc.Id, document.Id);
        _collection.ReplaceOne(filter, document, new ReplaceOptions{IsUpsert = true});
    }

    public virtual async Task InsertOrUpdateAsync(TDocument document)
    {
        if (document.Id == ObjectId.Empty)
        {
            var genericId = document.GetId();
            document.Id = !string.IsNullOrEmpty(genericId) 
                ? ObjectId.Parse(genericId.StringToLimitHex()) 
                : ObjectId.GenerateNewId();
            
            document.CreatedUtc = DateTime.UtcNow;
            document.UpdatedUtc = DateTime.UtcNow;
            await Task.Delay(TimeSpan.FromMilliseconds(150));
        }
        else
        {
            document.UpdatedUtc = DateTime.UtcNow;
        }

        var filter = Builders<TDocument>.Filter.Eq(doc => doc.Id, document.Id);
        var result = await _collection.ReplaceOneAsync(filter, document, new ReplaceOptions{IsUpsert = true});
        int a = 0;
    }

    public async Task TryInsertOrUpdateAsync(TDocument documents)
    {
        try
        {
            await InsertOrUpdateAsync(documents);
        }
        catch
        {
            // nothign
        }
    }

    public async Task InsertOrUpdateMultipleAsync(List<TDocument> documents)
    {
        if(!documents.Any())
            return;
        
        var operations = new List<WriteModel<TDocument>>();
        foreach (var document in documents)
        {
            if (document.Id == ObjectId.Empty)
            {
                var genericId = document.GetId();
                document.Id = !string.IsNullOrEmpty(genericId) 
                    ? ObjectId.Parse(genericId.StringToLimitHex()) 
                    : ObjectId.GenerateNewId();
                
                document.CreatedUtc = DateTime.UtcNow;
                document.UpdatedUtc = DateTime.UtcNow;
                operations.Add(new InsertOneModel<TDocument>(document));
            }
            else
            {
                document.UpdatedUtc = DateTime.UtcNow;
                var filter = Builders<TDocument>.Filter.Eq(doc => doc.Id, document.Id);
                operations.Add(new ReplaceOneModel<TDocument>(filter, document));
            }
        }

        if(!operations.Any())
            return;
        
        var result = await _collection.BulkWriteAsync(operations, new() { IsOrdered = false });
        int a = 0;
    }

    public async Task InsertOrUpdateMultipleInBatchAsync(List<TDocument> document, int batchSize = 50, TimeSpan? delayBetween = null)
    {
        if(!document.Any())
            return;
        
        var toUpdate = new List<TDocument>();
        foreach (var doc in document)
        {
            toUpdate.Add(doc);
            if (toUpdate.Count() >= 50)
            {
                await InsertOrUpdateMultipleAsync(toUpdate);
                toUpdate.Clear();
                if(delayBetween != null)
                    await Task.Delay(delayBetween.Value);
            }
        }
        
        if(toUpdate.Any())
            await InsertOrUpdateMultipleAsync(toUpdate);
    }

    public void DeleteOne(Expression<Func<TDocument, bool>> filterExpression)
    {
        _collection.FindOneAndDelete(filterExpression);
    }

    public Task DeleteOneAsync(Expression<Func<TDocument, bool>> filterExpression)
    {
        return _collection.FindOneAndDeleteAsync(filterExpression);
    }

    public void DeleteById(ObjectId objectId)
    {
        var filter = Builders<TDocument>.Filter.Eq(doc => doc.Id, objectId);
        _collection.FindOneAndDelete(filter);
    }

    public void Delete(TDocument doc)
        => DeleteById(doc.Id);

    public Task DeleteByIdAsync(ObjectId id)
    {
        var filter = Builders<TDocument>.Filter.Eq(doc => doc.Id, id);
        return _collection.FindOneAndDeleteAsync(filter);
    }

    public Task DeleteAsync(TDocument doc)
        => DeleteByIdAsync(doc.Id);

    public async Task DeleteManyAsync(List<TDocument> docs)
    {
        if(!docs.Any()) return;
        var ids = docs.Select(d => d.Id).ToList();
        var filter = Builders<TDocument>.Filter.In(d => d.Id, ids);
        await _collection.DeleteManyAsync(filter);
    }

    public async Task DeleteAll()
    {
        await _database.DropCollectionAsync(CollectionName);
    }

    public long DeleteMany(Expression<Func<TDocument, bool>> filterExpression)
    {   
        var result = _collection.DeleteMany(filterExpression);
        return result.DeletedCount;
    }

    public async Task<long> DeleteManyAsync(Expression<Func<TDocument, bool>> filterExpression)
    {   
        var result = await _collection.DeleteManyAsync(filterExpression);
        return result.DeletedCount;
    }

    public async Task CreateAscendingIndex(Expression<Func<TDocument, object>> expression)
    {
        var indexKeysDefinition = Builders<TDocument>.IndexKeys.Ascending(expression);
        await _collection.Indexes.CreateOneAsync(new CreateIndexModel<TDocument>(indexKeysDefinition));
    }
    public async Task CreateDescendingIndex(Expression<Func<TDocument, object>> expression)
    {
        var indexKeysDefinition = Builders<TDocument>.IndexKeys.Descending(expression);
        await _collection.Indexes.CreateOneAsync(new CreateIndexModel<TDocument>(indexKeysDefinition));
    }
    public async Task CreateTextIndex(Expression<Func<TDocument, object>> expression)
    {
        var indexKeysDefinition = Builders<TDocument>.IndexKeys.Text(expression);
        await _collection.Indexes.CreateOneAsync(new CreateIndexModel<TDocument>(indexKeysDefinition));
    }
    public async Task CreateHashedIndex(Expression<Func<TDocument, object>> expression)
    {
        var indexKeysDefinition = Builders<TDocument>.IndexKeys.Hashed(expression);
        await _collection.Indexes.CreateOneAsync(new CreateIndexModel<TDocument>(indexKeysDefinition));
    }

    #region Order by and filtering

    public async Task<List<TDocument>> OrderBy(string orderByParameter, int? limit)
    {
        var cursor = await _collection.FindAsync(x => x.Id != ObjectId.Empty, new()
        {
            Sort = Builders<TDocument>.Sort.Ascending(orderByParameter),
            Limit = limit
        });
        return cursor.ToEnumerable().ToList();
    }

    public async Task<List<TDocument>> OrderByDesc(string orderByParameter, int? limit)
    {
        var cursor = await _collection.FindAsync(x => x.Id != ObjectId.Empty, new()
        {
            Sort = Builders<TDocument>.Sort.Descending(orderByParameter),
            Limit = limit
        });
        return cursor.ToEnumerable().ToList();
    }

    public async Task<List<TDocument>> FilterAndOrderBy(Expression<Func<TDocument, bool>> filter, string orderByParameter, int? limit)
    {
        var cursor = await _collection.FindAsync(filter, new()
        {
            Sort = Builders<TDocument>.Sort.Ascending(orderByParameter),
            Limit = limit
        });
        return cursor.ToEnumerable().ToList();
    }

    public async Task<List<TDocument>> FilterAndOrderByDesc(Expression<Func<TDocument, bool>> filter, string orderByParameter, int? limit)
    {
        var cursor = await _collection.FindAsync(filter, new()
        {
            Sort = Builders<TDocument>.Sort.Descending(orderByParameter),
            Limit = limit
        });
        return cursor.ToEnumerable().ToList();
    }

    public void TransactionInsertOrUpdate(TDocument doc)
    {
        if (doc.Id == ObjectId.Empty)
        {
            _transactionInserts.Add(doc);
            return;
        }

        _transactionUpdates.AddOrUpdate(doc.Id, doc);
    }

    public void TransactionDelete(TDocument doc)
    {
        if (doc.Id == ObjectId.Empty) return;
        _transactionDeletes.Add(doc);
    }

    public async Task ExecuteTransaction()
    {
        await InsertOrUpdateMultipleInBatchAsync(_transactionInserts.ToList(), batchSize: 100);
        await InsertOrUpdateMultipleInBatchAsync(_transactionUpdates.Select(x => x.Value).ToList(), batchSize: 100);
        await DeleteManyAsync(_transactionDeletes.ToList());
    }

    #endregion

    #region # Dyanmic configure indexes

    public async Task ConfigureIndexesAsync()
    {
        var indexProps = typeof(TDocument).GetPropertyWithAttribute<MongoIndexAttribute>();
        
        var collection = GetCollection();
        var currentIndexes = new List<MongoIndex>();
        var indexes = (collection.Indexes.List() as IAsyncCursor<BsonDocument>).ToList() as List<BsonDocument>;
        foreach (var indexName in indexes.Select(x => x["name"].ToString()))
        {
            if (indexName == "_id_")
                continue;
                
            currentIndexes.Add(new()
            {
                Name = indexName.Split("_").First(),
                MongoName = indexName, 
            });
        }
        
        foreach (var (indexProperty, indexAttribute) in indexProps)
        {
            if (currentIndexes.Any(x => x.Name == indexProperty.Name))
                continue;

            Console.WriteLine($"Creating index {typeof(TDocument).Name}.{indexProperty.Name}");
            await CreateIndex(indexProperty.Name, indexAttribute.IsUnique);
        }

        foreach (var indexName in currentIndexes)
        {
            if (indexProps.Any(x => x.Info.Name == indexName.Name))
                continue;
            
            Console.WriteLine($"Delete index {typeof(TDocument).Name}.{indexName}");
            DeleteIndex(indexName.MongoName);
        }
    }

    private record MongoIndex
    {
        public string MongoName { get; set; }
        public string Name { get; set; }
    }

    #endregion
}