# Triggers TDD Implementation Plan

## Overview
Implement C# trigger execution support following the existing `RegisterStoredProcedure` pattern.
Pre-triggers can modify documents before writes; post-triggers run after writes with rollback on failure.

## Phase 1: Trigger Storage & Registration ✅

- [x] `_triggers` dictionary, `RegisteredTrigger` record, `RegisterTrigger` method
- [x] `DeregisterTrigger` method
- [x] `CreateTriggerAsync` stores trigger properties
- [x] `ReadTriggerAsync` returns stored trigger / throws NotFound
- [x] `DeleteTriggerAsync` removes trigger
- [x] `ReplaceTriggerAsync` updates trigger
- [x] `GetTriggerQueryIterator` — SKIPPED (too complex for NSubstitute mock pattern)

## Phase 2: Pre-Trigger Execution ✅

- [x] Pre-trigger modifies document on CreateItemAsync (typed)
- [x] Pre-trigger modifies document on UpsertItemAsync (typed)
- [x] Pre-trigger modifies document on ReplaceItemAsync (typed)
- [x] Pre-trigger on stream CreateItemStreamAsync
- [x] Pre-trigger on stream UpsertItemStreamAsync
- [x] Pre-trigger on stream ReplaceItemStreamAsync
- [x] Non-existent trigger throws BadRequest (400)
- [x] TriggerOperation mismatch — trigger not fired
- [x] Multiple pre-triggers chain in order
- [x] TriggerOperation.All fires on any operation
- [x] No PreTriggers specified — triggers not fired

## Phase 3: Post-Trigger Execution ✅

- [x] Post-trigger fires after CreateItemAsync
- [x] Post-trigger exception rolls back the write (wrapped in CosmosException 500)
- [x] Post-trigger on UpsertItemAsync
- [x] Post-trigger on ReplaceItemAsync
- [x] Post-trigger on stream variants
- [x] Non-existent post-trigger throws BadRequest (400)
- [x] Post-trigger operation mismatch — not fired
- [x] No PostTriggers specified — triggers not fired

## Phase 4: Update Existing Tests ✅

- [x] Unskipped and rewrote `PreTrigger_ShouldModifyDocumentOnCreate` (GapCoverageTests6.cs) to use RegisterTrigger
- [x] Updated `PreTrigger_EmulatorBehavior_TriggerRegistersButNeverFires` → renamed to `PreTrigger_CreateTriggerAsyncAlone_DoesNotFireWithoutRegisterTrigger`
- [x] `SkippedBehaviorTests.PreTrigger_ShouldFireOnCreate` — no change needed (registration only)

## Phase 5: Full Regression ✅

- [x] `dotnet test` — 1184 passed, 0 failures, 28 skipped

## Phase 6: Documentation ✅

- [x] Known-Limitations.md: Updated triggers row to "⚠️ C# handlers"
- [x] Known-Limitations.md: Updated behavioural difference #4 to describe C# handler approach
- [x] Features.md: Added Triggers section with pre/post/deregister examples
- [x] API-Reference.md: Added RegisterTrigger (pre + post overloads), DeregisterTrigger
- [x] Comparison.md: Added "Triggers (pre/post)" row in both comparison tables
- [x] README.md: Updated test count, added Triggers bullet

## Phase 7: Version, Tag, Push
- [ ] Increment version 1.0.3 → 1.0.4
- [ ] git commit, tag v1.0.4, push main + wiki

## Files Changed
- `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs` — trigger dict, RegisterTrigger, ExecutePreTriggers, ExecutePostTriggers, CRUD hooks
- `tests/CosmosDB.InMemoryEmulator.Tests/TriggerTests.cs` — NEW test file (25 tests)
- `tests/CosmosDB.InMemoryEmulator.Tests/GapCoverageTests6.cs` — Updated 2 existing trigger tests
- Wiki: Known-Limitations.md, Features.md, API-Reference.md, Comparison.md
- README.md
