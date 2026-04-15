# Copilot Instructions

## TDD Workflow

- Always use Test-Driven Development (TDD): write tests first, then follow the red-green-refactor cycle.
- Write a failing test (red), implement the minimum code to make it pass (green), then refactor.
- Write additional failing tests to cover edge cases and error conditions, and repeat the cycle until you have comprehensive test coverage for the feature or bug fix you're working on.

## Bug Fixing

- Always fix all bugs you find along the way, even if they are outside the immediate scope of the current task.
- When fixing a bug, identify missing test coverage in and around the affected area and create that coverage — again following the TDD red-green-refactor cycle.
- Fix any additional bugs discovered during that expanded test coverage work.

## Versioning & Release

- After every session of bug fixes is complete and the full test suite has passed, increment the patch version in **all** packages (not just the main one):
  - `src/CosmosDB.InMemoryEmulator/CosmosDB.InMemoryEmulator.csproj`
  - `src/CosmosDB.InMemoryEmulator.JsTriggers/CosmosDB.InMemoryEmulator.JsTriggers.csproj`
  - `src/CosmosDB.InMemoryEmulator.ProductionExtensions/CosmosDB.InMemoryEmulator.ProductionExtensions.csproj`
- All three packages must use the same version number.
- Commit, create a git tag (`v{version}`), and push both the commit and the tag to origin.
