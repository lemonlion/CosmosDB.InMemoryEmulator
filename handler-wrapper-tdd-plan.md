# TDD Plan: HttpMessageHandlerWrapper on InMemoryCosmosOptions

## Feature Summary
Add `HttpMessageHandlerWrapper` property and `WithHttpMessageHandlerWrapper()` fluent method to `InMemoryCosmosOptions`, allowing consumers to wrap `FakeCosmosHandler` with a custom `DelegatingHandler` before it's passed to `HttpClientFactory`.

## Test Plan

### Test 1: Wrapper function is invoked with the handler
- **Status**: [x] RED → [x] GREEN
- Set `HttpMessageHandlerWrapper` to a function that captures the input handler
- Assert the captured handler is not null and is the `FakeCosmosHandler` (single container)

### Test 2: Wrapper's return value is used — DelegatingHandler SendAsync called
- **Status**: [x] RED → [x] GREEN
- Set a wrapper that returns a custom `DelegatingHandler`
- Perform a CRUD operation through the DI-resolved `Container`
- Assert the custom handler's `SendAsync` was invoked

### Test 3: Null wrapper (default) — existing behaviour preserved
- **Status**: [x] RED → [x] GREEN
- Don't set `HttpMessageHandlerWrapper`
- Assert `UseInMemoryCosmosDB()` works exactly as before (CRUD works)

### Test 4: Multi-container — wrapper receives the router, not individual handlers
- **Status**: [x] RED → [x] GREEN
- Use `AddContainer()` twice with a wrapper that captures the handler
- Assert the captured handler is NOT a `FakeCosmosHandler` (it should be the router)

### Test 5: Fluent method `WithHttpMessageHandlerWrapper()` works
- **Status**: [x] RED → [x] GREEN
- Use the fluent API: `options.AddContainer(...).WithHttpMessageHandlerWrapper(...)`
- Assert the wrapper is invoked

### Test 6: Full DelegatingHandler chaining — CRUD + query through wrapper
- **Status**: [x] RED → [x] GREEN
- Wire a real `DelegatingHandler` subclass that counts requests
- Perform Create + Read + Query operations
- Assert the handler saw all requests AND data is correct

## Implementation Steps

1. Add `HttpMessageHandlerWrapper` property to `InMemoryCosmosOptions`
2. Add `WithHttpMessageHandlerWrapper()` fluent method to `InMemoryCosmosOptions`
3. Apply wrapper in `ServiceCollectionExtensions.UseInMemoryCosmosDB()` (1 line)

## Documentation Updates

- [x] Wiki: API-Reference.md — add property + method to `InMemoryCosmosOptions` table
- [x] Wiki: Dependency-Injection.md — add to Configuration Options section
- [x] Wiki: Integration-Approaches.md — add note about wrapping the handler
- [x] Wiki: Features.md — N/A (DI/integration feature, not standalone feature section)
- [x] Wiki: Feature-Comparison-With-Alternatives.md — add row for handler wrapping
- [x] README.md — mention in features list
- [ ] src/README.md — not needed (NuGet readme is already concise)
- [x] Increment version from 2.0.2 → 2.0.3
