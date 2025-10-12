namespace Aco228.MongoDb.Base;

public class MongoIndexAttribute : Attribute
{
    public bool IsUnique { get; set; } = false;
}