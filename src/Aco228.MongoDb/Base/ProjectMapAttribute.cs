namespace Arbitrage.Mongo.Core;

public class ProjectMapAttribute : Attribute
{
    public string PropertyName { get; set; }
    public bool Ignore { get; set; } = false;

    public ProjectMapAttribute() { }
    public ProjectMapAttribute (string propertyName)
    {
        PropertyName = propertyName;
    }
}