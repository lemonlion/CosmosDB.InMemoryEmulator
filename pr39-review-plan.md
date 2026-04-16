# PR #39 Review — Implementation Plan

PR: https://github.com/lemonlion/CosmosDB.InMemoryEmulator/pull/39
Branch: `agents/local-implementation-validation-plan`

## Changes to Make

### 1. `emulator-parity.yml` — Add `net10.0` support

`ci.yml` tests both `net8.0` and `net10.0`, but `emulator-parity.yml` only installs `8.0.x`. Parity divergences that surface only on `net10.0` would be invisible.

**Files:** `.github/workflows/emulator-parity.yml`
**Action:** Add `10.0.x` to `dotnet-version` in each test job's setup-dotnet step (or add a framework matrix).

### 2. `emulator-parity.yml` — Add timeout-minutes

None of the 4 jobs set `timeout-minutes`. A hung Docker or Chocolatey install would burn the default 6-hour GitHub Actions timeout.

**Files:** `.github/workflows/emulator-parity.yml`
**Action:**
- Add `timeout-minutes: 30` to the 3 test jobs (`test-inmemory`, `test-emulator-linux`, `test-windows`)
- Add `timeout-minutes: 10` to the `compare` job

### 3. Delete all references to `check-test-classification.ps1`

The script is referenced in the PR description's scripts table but the file does not exist (confirmed 404). No references exist in AGENTS.md or README.md — only the PR body.

**Files:** PR description (edit via GitHub)
**Action:** Remove the `check-test-classification.ps1` row from the scripts table in the PR description.

### 4. Remove original `CosmosDB.InMemoryEmulator.Tests` project

Migration is verified complete:
- All 87 unit test files exist in `Tests.Unit` with identical SHAs
- All 8 integration test files exist in `Tests.Integration` with identical SHAs
- `TestDocument.cs` and all `Infrastructure/` helpers are in `Tests.Shared`
- Both `Tests.Unit` and `Tests.Integration` reference `Tests.Shared` via `<ProjectReference>`
- `ci.yml` already only runs the new projects
- The original project's files are fully redundant duplicates

**Files:**
- `CosmosDB.InMemoryEmulator.sln` — remove the project entry, build config lines, and nesting entry for `{63F85A41-5264-6904-80EA-F3447B5E43D8}`
- `tests/CosmosDB.InMemoryEmulator.Tests/` — delete entire directory

**Action:**
1. Remove from `.sln`: project declaration, 4 build configuration lines, 1 nested-project line
2. Delete `tests/CosmosDB.InMemoryEmulator.Tests/` directory
3. Remove the "Migration note" paragraph from the PR description

## No Changes Needed (Accepted As-Is)

| Finding | Reason |
|---------|--------|
| Thread-safety in `EmulatorTestFixture` | xUnit creates per-class instances; methods run sequentially. Low practical risk. |
| `EmulatorDetector` hardcodes `https://localhost:8081/` | Only affects `[RequiresEmulatorFact]` skip logic, not main test path. Track as follow-up. |
| `validate-parity.ps1` no fail-fast on baseline failure | Local dev script; proceeding after baseline failure is useful for comparison. |
| Unpinned Chocolatey emulator version | Parity validation *should* catch new emulator breakage; pinning would hide that signal. |
| `compare-trx.ps1` exits non-zero only for suspects | Intentional design; suspects are actionable, gaps are informational. |
| `IsEmulator` redundancy on `ITestContainerFixture` | Convenience property, low risk with only 2 implementations. |
| Different retry/timeout settings between fixtures | Intentional; emulator needs retries for transient network issues, in-memory doesn't. |

## Execution Order

1. Checkout `agents/local-implementation-validation-plan` branch (or worktree)
2. Apply change #1 (net10.0 in emulator-parity.yml)
3. Apply change #2 (timeout-minutes in emulator-parity.yml)
4. Apply change #4 (remove old Tests project from sln + delete directory)
5. Commit and push
6. Edit PR description on GitHub for changes #3 and #4's migration note removal
7. Verify CI passes
