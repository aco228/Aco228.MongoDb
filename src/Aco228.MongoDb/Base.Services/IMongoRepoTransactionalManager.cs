namespace Aco228.MongoDb.Base.Services;

public interface IMongoRepoTransactionalManager<TDbContext, T>  
    where TDbContext : IMongoDbContext 
    where T : MongoDocument
{
    void SetInsertAfter(uint newVal);
    void DontInsertAfter();
    Task InsertOrUpdateAsync(T document);
    Task DeleteAsync(T document);
    Task Finish();
}

public class MongoRepoTransactionalManager<TDbContext, T> : IMongoRepoTransactionalManager<TDbContext, T>
    where TDbContext : IMongoDbContext 
    where T : MongoDocument
{
    private List<T> _insertRequests = new();
    private List<T> _deleteRequests = new();
    private readonly IMongoRepository<TDbContext, T> _repository;
    private uint _insertAfter = 10;
    private int CurrentCount => _insertRequests.Count + _deleteRequests.Count;

    public MongoRepoTransactionalManager(IMongoRepository<TDbContext, T> repository, uint insertAfter = 10)
    {
        _repository = repository;
        _insertAfter = insertAfter;
    }

    public void SetInsertAfter(uint newVal) => _insertAfter = newVal;
    public void DontInsertAfter() => SetInsertAfter(uint.MaxValue);

    public Task InsertOrUpdateAsync(T document)
    {
        _insertRequests.Add(document);
        return Execute(force: false);
    }

    public Task DeleteAsync(T document)
    {
        _deleteRequests.Add(document);
        return Execute(force: true);
    }

    public async Task Execute(bool force = false)
    {
        if(!_insertRequests.Any()) return;
        if(!force && CurrentCount < _insertAfter) return;

        if (_insertRequests.Any())
        {
            await _repository.InsertOrUpdateMultipleAsync(_insertRequests);
            _insertRequests.Clear();
        }

        if (_deleteRequests.Any())
        {
            await _repository.DeleteManyAsync(_deleteRequests);
            _deleteRequests.Clear();
        }
    }

    public Task Finish() => Execute(force: true);
}