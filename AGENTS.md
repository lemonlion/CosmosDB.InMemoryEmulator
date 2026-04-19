# Contribution Instructions

## TDD Workflow

- Always use Test-Driven Development (TDD): write tests first, then follow the red-green-refactor cycle.
- Write a failing test (red), implement the minimum code to make it pass (green), then refactor.
- Write additional failing tests to cover edge cases and error conditions, and repeat the cycle until you have comprehensive test coverage for the feature or bug fix you're working on.
- For every unit test written, if possible, write the equivalent integration test, testing the same functionality from the entry point.

## Bug Fixing

- Always fix all bugs you find along the way, even if they are outside the immediate scope of the current task.
- When fixing a bug, identify missing test coverage in and around the affected area and create that coverage — again following the TDD red-green-refactor cycle.
- Fix any additional bugs discovered during that expanded test coverage work.

## Versioning & Release

- After every session of bug fixes is complete and the full test suite has passed, increment the patch version in `src/Directory.Build.props` (the single `<Version>` property shared by all three packages).
- **On `main`:** Commit, create a git tag (`v{version}`), and push both the commit and the tag to origin.
- **On any other branch:** Commit and push the code changes and version bump only. Do not create or push a tag.

## Test Classification Rules

Tests are split into two projects. When creating or moving tests, follow these rules:

### Tests.Integration
- Uses `TestFixtureFactory.Create()` / `ITestContainerFixture` to obtain a container
- Goes through the real CosmosClient SDK HTTP pipeline via `FakeCosmosHandler`
- Must **not** use `new InMemoryCosmosClient()`, `new FaultInjector()`, `FaultInjection`, or any `internal` API
- Can run against in-memory, Linux emulator, or Windows emulator via `COSMOS_TEST_TARGET`

### Tests.Unit
- Uses `new InMemoryContainer()`, `new InMemoryCosmosClient()`, or any `internal` API directly
- Tests that use `FakeCosmosHandler` but also touch internal APIs (e.g. cache internals, `FaultInjection`) belong here
- Only runs in-memory — never against a real emulator

### Tests.Shared
- Class library (not a test project) — shared infrastructure, fixtures, traits, and models
- Referenced by both Unit and Integration projects

### Key constraint
The Integration project does **not** have `InternalsVisibleTo` access. If a test needs internal APIs, it belongs in Unit.