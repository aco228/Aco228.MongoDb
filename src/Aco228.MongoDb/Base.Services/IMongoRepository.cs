using System.Linq.Expressions;
using Aco228.MongoDb.Models;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Aco228.MongoDb.Base.Services;

public interface IMongoBaseCollection
{
    Task ConfigureIndexesAsync();
}

public interface IMongoRepository<TDbContext, TDocument> : IMongoBaseCollection
    where TDocument : MongoDocument
    where TDbContext : IMongoDbContext 
{
    bool EnforceAccountIsolation {get; set; }
    string CollectionName { get; }
    IQueryable<TDocument> AsQueryable();
    List<TDocument> TryGetAll();
    IMongoCollection<TDocument> GetCollection();
    Task CreateIndex(string nameOfParameter, bool isUnique);
    void DeleteIndex(string nameOfParameter);
    IMongoRepoTransactionalManager<TDbContext, TDocument> CreateTransactionManager(uint numberOfDocuments = 15);

    IEnumerable<T> ProjectAll<T>();
    IEnumerable<TDocument> FilterBy(Expression<Func<TDocument, bool>> filterExpression);
    IEnumerable<T> ProjectFilterBy<T>(Expression<Func<TDocument, bool>> filterExpression);
    Task<List<TDocument>> FilterByAsync(Expression<Func<TDocument, bool>> filterExpression, int? limit = null, int? skip = null, OrderByCommand? orderByCommand = null);
    Task<List<TProperty>> FilterPropertyByAsync<TProperty>(Expression<Func<TDocument, bool>> filterExpression, Expression<Func<TDocument, TProperty>> selector);
    Task<T?> ProjectFindOneByAsync<T>(Expression<Func<TDocument, bool>> filterExpression);
    Task<List<T>> ProjectFilterByAsync<T>(Expression<Func<TDocument, bool>> filterExpression);
    IAsyncEnumerable<TDocument> FilterInBatchAsync(Expression<Func<TDocument, bool>> filterExpression, int batchSize = 100, OrderByCommand? orderByCommand = null);

    IEnumerable<TProjected> FilterBy<TProjected>(
        Expression<Func<TDocument, bool>> filterExpression,
        Expression<Func<TDocument, TProjected>> projectionExpression);

    TDocument? FindOne(Expression<Func<TDocument, bool>> filterExpression);
    Task<TDocument?> FindOneAsync(Expression<Func<TDocument, bool>> filterExpression);
    
    Task<long> Count(Expression<Func<TDocument, bool>> filterExpression);

    TDocument? FindById(string id);
    TDocument? FindById(ObjectId objectId);

    Task<TDocument?> FindByIdAsync(string id);
    Task<TDocument?> FindByIdAsync(ObjectId objectId);
    Task<TDocument> Reload(TDocument document);

    void InsertOrUpdate(TDocument document);

    Task InsertOrUpdateAsync(TDocument documents);
    Task TryInsertOrUpdateAsync(TDocument documents);
    Task InsertOrUpdateMultipleAsync(List<TDocument> document);
    Task InsertOrUpdateMultipleInBatchAsync(List<TDocument> document, int batchSize = 50, TimeSpan? delayBetween = null);

    void DeleteOne(Expression<Func<TDocument, bool>> filterExpression);

    Task DeleteOneAsync(Expression<Func<TDocument, bool>> filterExpression);

    void DeleteById(ObjectId objectId);
    void Delete(TDocument doc);
    Task DeleteByIdAsync(ObjectId id);
    Task DeleteAsync(TDocument doc);
    Task DeleteManyAsync(List<TDocument> doc);
    Task DeleteAll();

    long DeleteMany(Expression<Func<TDocument, bool>> filterExpression);

    Task<long> DeleteManyAsync(Expression<Func<TDocument, bool>> filterExpression);
    
    Task CreateAscendingIndex(Expression<Func<TDocument, object>> expression);
    Task CreateDescendingIndex(Expression<Func<TDocument, object>> expression);
    Task CreateTextIndex(Expression<Func<TDocument, object>> expression);
    Task CreateHashedIndex(Expression<Func<TDocument, object>> expression);

    Task<List<TDocument>> OrderBy(string orderByParameter, int? limit);
    Task<List<TDocument>> OrderByDesc(string orderByParameter, int? limit);
    Task<List<TDocument>> FilterAndOrderBy(Expression<Func<TDocument, bool>> filter, string orderByParameter, int? limit);
    Task<List<TDocument>> FilterAndOrderByDesc(Expression<Func<TDocument, bool>> filter, string orderByParameter, int? limit);
    
    // for later
    void TransactionInsertOrUpdate(TDocument doc);
    void TransactionDelete(TDocument doc);
    Task ExecuteTransaction();
}