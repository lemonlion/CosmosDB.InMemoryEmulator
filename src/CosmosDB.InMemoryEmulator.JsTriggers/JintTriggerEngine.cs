using System.Net;
using Jint;
using Jint.Native;
using Jint.Runtime;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator.JsTriggers;

/// <summary>
/// Jint-based implementation of <see cref="IJsTriggerEngine"/>.
/// Executes Cosmos DB trigger JavaScript bodies using the Jint interpreter.
/// Provides the Cosmos DB server-side API: getContext(), getRequest()/getResponse(), getBody(), setBody().
/// </summary>
public class JintTriggerEngine : IJsTriggerEngine
{
    public JObject ExecutePreTrigger(string jsBody, JObject document)
    {
        var bodyJson = document.ToString(Newtonsoft.Json.Formatting.None);
        JsValue? updatedBody = null;

        var engine = new Engine(options =>
        {
            options.TimeoutInterval(TimeSpan.FromSeconds(5));
            options.MaxStatements(10_000);
        });

        // Parse the document into JS land once
        var jsDoc = engine.Evaluate($"({bodyJson})");

        // Wire up the Cosmos DB server-side pre-trigger API
        engine.SetValue("__getBody", new Func<JsValue>(() => jsDoc));
        engine.SetValue("__setBody", new Action<JsValue>(val => updatedBody = val));
        engine.Execute("""
            function getContext() {
                return {
                    getRequest: function() {
                        return {
                            getBody: function() { return __getBody(); },
                            setBody: function(doc) { __setBody(doc); }
                        };
                    }
                };
            }
            """);

        try
        {
            engine.Execute(jsBody);
            InvokeFirstFunction(engine, jsBody);
        }
        catch (JavaScriptException ex)
        {
            throw new CosmosException(
                $"Pre-trigger failed: {ex.Message}",
                HttpStatusCode.BadRequest, 0, string.Empty, 0);
        }

        if (updatedBody is not null)
        {
            engine.SetValue("__toSerialize", updatedBody);
            var json = engine.Evaluate("JSON.stringify(__toSerialize)").AsString();
            return JObject.Parse(json);
        }

        return document;
    }

    public void ExecutePostTrigger(string jsBody, JObject document)
    {
        var bodyJson = document.ToString(Newtonsoft.Json.Formatting.None);

        var engine = new Engine(options =>
        {
            options.TimeoutInterval(TimeSpan.FromSeconds(5));
            options.MaxStatements(10_000);
        });

        // Parse the document into JS land once
        var jsDoc = engine.Evaluate($"({bodyJson})");

        // Wire up the Cosmos DB server-side post-trigger API
        engine.SetValue("__getBody", new Func<JsValue>(() => jsDoc));
        engine.Execute("""
            function getContext() {
                return {
                    getResponse: function() {
                        return {
                            getBody: function() { return __getBody(); }
                        };
                    }
                };
            }
            """);

        try
        {
            engine.Execute(jsBody);
            InvokeFirstFunction(engine, jsBody);
        }
        catch (JavaScriptException ex)
        {
            throw new CosmosException(
                $"Post-trigger failed: {ex.Message}",
                HttpStatusCode.BadRequest, 0, string.Empty, 0);
        }
    }

    private static void InvokeFirstFunction(Engine engine, string jsBody)
    {
        var matches = System.Text.RegularExpressions.Regex.Matches(jsBody, @"\bfunction\s+(\w+)\s*\(");
        if (matches.Count > 0)
        {
            engine.Invoke(matches[0].Groups[1].Value);
        }
    }
}
