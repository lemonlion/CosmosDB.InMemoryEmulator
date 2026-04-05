using System.Net;
using Jint;
using Jint.Native;
using Jint.Runtime;
using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator.JsTriggers;

/// <summary>
/// Jint-based implementation of <see cref="ISprocEngine"/>.
/// Executes Cosmos DB stored procedure JavaScript bodies using the Jint interpreter.
/// Provides the Cosmos DB server-side API: getContext(), getResponse().setBody(), and argument passing.
/// </summary>
public class JintSprocEngine : ISprocEngine
{
    private List<string> _capturedLogs = new();

    public IReadOnlyList<string> CapturedLogs => _capturedLogs;

    public string? Execute(string jsBody, PartitionKey partitionKey, dynamic[] args)
    {
        string? result = null;
        _capturedLogs = new List<string>();

        var engine = new Engine(options =>
        {
            options.TimeoutInterval(TimeSpan.FromSeconds(5));
            options.MaxStatements(10_000);
        });

        // Wire up the Cosmos DB server-side stored procedure API
        engine.SetValue("__setBody", new Action<JsValue>(val =>
        {
            engine.SetValue("__toSerialize", val);
            result = engine.Evaluate("JSON.stringify(__toSerialize)").AsString();
            // Unwrap JSON string wrapping for simple string values
            if (result is not null && result.StartsWith('"') && result.EndsWith('"'))
            {
                result = result[1..^1]
                    .Replace("\\\"", "\"")
                    .Replace("\\\\", "\\")
                    .Replace("\\n", "\n")
                    .Replace("\\r", "\r")
                    .Replace("\\t", "\t");
            }
        }));
        engine.SetValue("__log", new Action<JsValue>(msg =>
        {
            _capturedLogs.Add(msg.ToString());
        }));

        engine.Execute("""
            var console = { log: function(msg) { __log(msg); } };
            function getContext() {
                return {
                    getResponse: function() {
                        return {
                            setBody: function(val) { __setBody(val); }
                        };
                    },
                    getCollection: function() {
                        return {
                            getSelfLink: function() { return ""; }
                        };
                    }
                };
            }
            """);

        // Convert C# args to JS values
        var jsArgs = new JsValue[args?.Length ?? 0];
        for (var i = 0; i < jsArgs.Length; i++)
        {
            var arg = args![i];
            if (arg is null)
                jsArgs[i] = JsValue.Null;
            else if (arg is string s)
                jsArgs[i] = new JsString(s);
            else if (arg is int intVal)
                jsArgs[i] = new JsNumber(intVal);
            else if (arg is long longVal)
                jsArgs[i] = new JsNumber(longVal);
            else if (arg is double dblVal)
                jsArgs[i] = new JsNumber(dblVal);
            else if (arg is bool bVal)
                jsArgs[i] = bVal ? JsBoolean.True : JsBoolean.False;
            else
                jsArgs[i] = engine.Evaluate($"({Newtonsoft.Json.JsonConvert.SerializeObject(arg)})");
        }

        try
        {
            // Check if the body defines a named function
            var matches = System.Text.RegularExpressions.Regex.Matches(jsBody, @"\bfunction\s+(\w+)\s*\(");
            if (matches.Count > 0)
            {
                engine.Execute(jsBody);
                engine.Invoke(matches[0].Groups[1].Value, jsArgs);
            }
            else
            {
                // Anonymous function: wrap as expression and invoke
                var func = engine.Evaluate($"({jsBody})");
                engine.Invoke(func, jsArgs);
            }
        }
        catch (JavaScriptException ex)
        {
            throw new CosmosException(
                $"Stored procedure failed: {ex.Message}",
                HttpStatusCode.BadRequest, 0, string.Empty, 0);
        }

        return result;
    }
}
