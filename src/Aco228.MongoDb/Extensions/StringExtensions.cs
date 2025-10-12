using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.WebUtilities;

namespace Aco228.MongoDb.Extensions;

public static class StringExtensions
{
    public static string Base64Encode(this string input)
    {
        var crypt = new SHA256Managed();
        var hash = crypt.ComputeHash(Encoding.UTF8.GetBytes(input));
        return Base64UrlTextEncoder.Encode(hash);
    }
    
    public static string StringToLimitHex(this string input, int minimumLenght = 24)
    {
        StringBuilder sb = new StringBuilder();
        foreach (char t in input)
        { 
            //Note: X for upper, x for lower case letters
            sb.Append(Convert.ToInt32(t).ToString("x2")); 
        }

        if (sb.Length > minimumLenght)
            throw new ArgumentException($"Hex is larger thatn {minimumLenght} characters");

        if (sb.Length != minimumLenght)
            for(int i = sb.Length; i < minimumLenght; i++)
                sb.Append("0");
        
        return sb.ToString();
    }
}