using System.Net;
using Jint;
using Jint.Native;
using Jint.Runtime;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

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

    // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-use-stored-procedures-triggers-udfs
    //   "For partitioned containers, when you run a stored procedure, you must provide a partition key
    //    value in the request options. Stored procedures are always scoped to a partition key.
    //    Items that have a different partition key value aren't visible to the stored procedure."
    // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/execute-a-stored-procedure
    //   Stored procedure execution takes an array of parameters passed as the request body.
    public string? Execute(string jsBody, PartitionKey partitionKey, dynamic[] args)
        => Execute(jsBody, partitionKey, args, null!);

    public string? Execute(string jsBody, PartitionKey partitionKey, dynamic[] args, ICollectionContext context)
    {
        string? result = null;
        _capturedLogs = new List<string>();

        // TODO: No official source found for timeout/statement limits — these are emulator-imposed execution guards.
        var engine = new Engine(options =>
        {
            options.TimeoutInterval(TimeSpan.FromSeconds(5));
            options.MaxStatements(10_000);
        });

        // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-write-stored-procedures-triggers-udfs
        //   "The context object provides access to all operations that can be performed in Azure Cosmos DB,
        //    as well as access to the request and response objects."
        //   Example: "var response = context.getResponse(); response.setBody('Hello, World');"
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
        // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-write-stored-procedures-triggers-udfs#logging
        //   "you can log the steps by enabling script logging."
        //   Stored procedures can use console.log() for diagnostic logging.
        engine.SetValue("__log", new Action<JsValue>(msg =>
        {
            _capturedLogs.Add(msg.ToString());
        }));

        WireCollectionContext(engine, context);

        engine.Execute("""
            var console = { log: function(msg) { __log(msg); } };
            function getContext() {
                return {
                    getResponse: function() {
                        return {
                            setBody: function(val) { __setBody(val); }
                        };
                    },
                    getCollection: function() { return __collection; }
                };
            }
            """);

        // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/execute-a-stored-procedure
        //   "An array of the parameters to pass to the stored procedure."
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

        // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/create-a-stored-procedure
        //   Stored procedure bodies are JavaScript functions. Official examples show both named
        //   functions (e.g., "function createToDoItems(items) { ... }") and anonymous functions.
        try
        {
            // Check if the body starts with a named function declaration
            var match = System.Text.RegularExpressions.Regex.Match(jsBody.TrimStart(), @"^function\s+(\w+)\s*\(");
            if (match.Success)
            {
                engine.Execute(jsBody);
                engine.Invoke(match.Groups[1].Value, jsArgs);
            }
            else
            {
                // Anonymous function: wrap as expression and invoke
                var func = engine.Evaluate($"({jsBody})");
                engine.Invoke(func, jsArgs);
            }
        }
        // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-write-stored-procedures-triggers-udfs
        //   "If a callback isn't provided and there's an error, the Azure Cosmos DB runtime throws an error."
        catch (JavaScriptException ex)
        {
            throw InMemoryCosmosException.Create(
                $"Stored procedure failed: {ex.Message}",
                HttpStatusCode.BadRequest, 0, string.Empty, 0);
        }

        return result;
    }

    // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-write-stored-procedures-triggers-udfs
    //   "var collection = getContext().getCollection();" — provides access to CRUD and query operations.
    //   Collection methods: createDocument, queryDocuments, replaceDocument, deleteDocument.
    //   Methods use an async callback pattern: callback(err, result) with a boolean return indicating acceptance.
    //   getSelfLink() returns the collection's self-link used as the first argument to CRUD operations.
    // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/javascript-query-api
    //   "__ (double-underscore) is an alias to getContext().getCollection()"
    internal static void WireCollectionContext(Engine engine, ICollectionContext context)
    {
        if (context == null)
        {
            engine.Execute("""
                var __collection = { getSelfLink: function() { return ""; } };
                """);
            return;
        }

        var selfLink = context.SelfLink ?? "";

        engine.SetValue("__selfLink", selfLink);
        engine.SetValue("__createDocument", new Func<string, string>(jsonDoc =>
        {
            var doc = JObject.Parse(jsonDoc);
            var created = context.CreateDocument(doc);
            return created.ToString(Formatting.None);
        }));
        engine.SetValue("__readDocument", new Func<string, string>(docId =>
        {
            var doc = context.ReadDocument(docId);
            return doc.ToString(Formatting.None);
        }));
        engine.SetValue("__queryDocuments", new Func<string, string>(sql =>
        {
            var docs = context.QueryDocuments(sql);
            return JsonConvert.SerializeObject(docs, Formatting.None);
        }));
        engine.SetValue("__replaceDocument", new Func<string, string, string>((docId, jsonDoc) =>
        {
            var doc = JObject.Parse(jsonDoc);
            var replaced = context.ReplaceDocument(docId, doc);
            return replaced.ToString(Formatting.None);
        }));
        engine.SetValue("__deleteDocument", new Action<string>(docId =>
        {
            context.DeleteDocument(docId);
        }));

        // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-write-stored-procedures-triggers-udfs
        //   Collection methods follow the pattern: collection.createDocument(collectionLink, item, options, callback)
        //   where callback is function(err, resource, options) and the method returns a boolean (bounded execution).
        //   "If the request was accepted, the callback will be called."
        engine.Execute("""
            var __collection = {
                getSelfLink: function() { return __selfLink; },
                createDocument: function(link, doc, opts, cb) {
                    var result = JSON.parse(__createDocument(JSON.stringify(doc)));
                    if (typeof opts === 'function') { opts(null, result); }
                    else if (cb) { cb(null, result); }
                    return true;
                },
                readDocument: function(link, opts, cb) {
                    var docId = link.split('/').pop();
                    var result = JSON.parse(__readDocument(docId));
                    if (typeof opts === 'function') { opts(null, result); }
                    else if (cb) { cb(null, result); }
                    return true;
                },
                queryDocuments: function(link, query, opts, cb) {
                    var results = JSON.parse(__queryDocuments(query));
                    var responseOptions = { continuation: null };
                    if (typeof opts === 'function') { opts(null, results, responseOptions); }
                    else if (cb) { cb(null, results, responseOptions); }
                    return true;
                },
                replaceDocument: function(link, doc, opts, cb) {
                    var docId = doc.id || link.split('/').pop();
                    var result = JSON.parse(__replaceDocument(docId, JSON.stringify(doc)));
                    if (typeof opts === 'function') { opts(null, result); }
                    else if (cb) { cb(null, result); }
                    return true;
                },
                deleteDocument: function(link, opts, cb) {
                    var docId = link.split('/').pop();
                    __deleteDocument(docId);
                    if (typeof opts === 'function') { opts(null); }
                    else if (cb) { cb(null); }
                    return true;
                }
            };
            """);
    }
}
