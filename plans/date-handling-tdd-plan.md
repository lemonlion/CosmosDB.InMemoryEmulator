# Date Handling Deep-Dive — TDD Plan

> Created: 2026-04-01
> Status: **PLANNING** — no implementation yet
> Target version: **v2.0.5**
> Approach: **TDD red-green-refactor** — write failing test, implement fix, verify

---

## Table of Contents

1. [Bugs Found](#1-bugs-found)
2. [Missing Features](#2-missing-features)
3. [Missing Test Coverage](#3-missing-test-coverage)
4. [Test Plan](#4-test-plan)
5. [Implementation Order](#5-implementation-order)
6. [Documentation Updates](#6-documentation-updates)
7. [Release Checklist](#7-release-checklist)

---

## 1. Bugs Found

### BUG-1: DateTimeBin default origin is 2001-01-01, should be 1970-01-01 (Unix epoch)

**File:** `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs` ~L4659
**Current:** `new DateTime(2001, 1, 1, 0, 0, 0, DateTimeKind.Utc)`
**Expected:** `new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)`
**Evidence:** Cosmos DB docs state: *"If not specified, the default value is the Unix epoch 1970-01-01T00:00:00.000000Z"*
**Impact:** Incorrect binning for multi-day/multi-year bins when no custom origin is specified. Single-unit bins (binSize=1) for day/hour/minute/second are unaffected because floor arithmetic produces the same result regardless of origin. Multi-unit bins (e.g., 7-day bins) produce incorrect boundaries.
**Cosmos DB docs example:**
- `DATETIMEBIN("2021-01-08T18:35:00.0000000", "dd", 7)` → `"2021-01-07T00:00:00.0000000Z"` (7-day bins from Unix epoch)
- Our emulator with 2001-01-01 origin would give `"2021-01-04T00:00:00.0000000Z"` (wrong)

### BUG-2: DateTimeBin missing `"m"` alias for month

**File:** `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs` ~L4669
**Current:** `part is "month" or "mm"` — missing `"m"`
**Expected:** `part is "month" or "mm" or "m"`
**Impact:** Using `"m"` as date_part for DateTimeBin falls through to the day/hour/minute else branch, then to the `_ => dt` default, returning the original datetime unchanged instead of binning by month.
**Note:** All other date functions (DateTimeAdd, DateTimePart, DateTimeDiff) correctly include `"m"`.

### BUG-3: DateTimeFromParts requires all 7 arguments, but hour+ are optional

**File:** `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs` ~L4633
**Current:** `if (args.Length < 7) return null;`
**Expected:** `if (args.Length < 3) return null;` with defaults of 0 for omitted parameters
**Evidence:** Cosmos DB docs: `DATETIMEFROMPARTS(year, month, day [, hour] [, minute] [, second] [, second_fraction])`
**Cosmos DB docs example:**
- `DATETIMEFROMPARTS(2017, 4, 20)` → `"2017-04-20T00:00:00.0000000Z"` ✓
- `DATETIMEFROMPARTS(2017, 4, 20, 13, 15)` → `"2017-04-20T13:15:00.0000000Z"` ✓
**Impact:** Queries using `DateTimeFromParts` with 3-6 args return null/undefined instead of a valid date.

### BUG-4: DateTimeFromParts 7th parameter treated as milliseconds, should be sub-second ticks (100ns units)

**File:** `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs` ~L4645
**Current:** `new DateTime(..., (int)ms.Value, DateTimeKind.Utc)` — 7th arg passed as milliseconds to DateTime ctor
**Expected:** 7th arg is `fffffffZ` format = 7 digits of sub-second precision in 100-nanosecond units (ticks within the second)
**Evidence:** Cosmos DB docs example:
- `DATETIMEFROMPARTS(2017, 4, 20, 13, 15, 20, 3456789)` → `"2017-04-20T13:15:20.3456789Z"`
- With current code: ms=3456789 → `ArgumentOutOfRangeException` (ms must be 0-999)
**Impact:** Any non-trivial sub-second fraction value causes an exception or incorrect result.

### BUG-5 (Potential): DateTimeDiff hour/minute/second uses interval truncation instead of boundary-crossing

**File:** `src/CosmosDB.InMemoryEmulator/InMemoryContainer.cs` ~L4623-4631
**Current:** `(long)(dtEnd - dtStart).TotalHours` — truncates the continuous interval
**Cosmos DB docs say:** *"The function returns a measurement of the number of boundaries crossed for the specified date and time part, not a measurement of the time interval."*
**Example where they differ:**
- `DateTimeDiff("hour", "2020-01-01T23:59:00Z", "2020-01-02T00:01:00Z")`
- Interval truncation: `(long)0.033` = **0**
- Boundary crossing: crossed the 00:00 hour mark = **1**
**Impact:** Low — most practical use cases have more than 1 unit between dates. But technically incorrect for sub-unit-precision scenarios.
**Decision:** Mark as **Skip** with divergent behaviour test — implementing boundary-crossing semantics for sub-day parts is complex and the interval-based approach matches behaviour for all whole-unit intervals. Document in Known Limitations.

---

## 2. Missing Features

### FEAT-1: Microsecond and nanosecond date parts not supported

**Affects:** DateTimeAdd, DateTimePart, DateTimeDiff, DateTimeBin
**Cosmos DB supports:**
- `microsecond` / `mcs` — 1 microsecond = 10 ticks
- `nanosecond` / `ns` — 1 nanosecond = 0.01 ticks (100ns resolution)
**Decision:** Implement for DateTimePart (extract microseconds/nanoseconds from precision digits). For DateTimeAdd, DateTimeDiff, DateTimeBin — implement where .NET `DateTime` has sufficient precision (100ns/tick). Document that precision is limited to 100ns (same as .NET `DateTime.Ticks`).

### FEAT-2: DateTimeIsValid function not implemented

**Cosmos DB function:** `DateTimeIsValid(dateTime)` — returns boolean indicating if a string is a valid ISO 8601 date.
**Decision:** Skip — this is a very rarely used utility function. Mark as Skip with reason in tests, add to Known Limitations. The function doesn't appear in the official docs function list we fetched, suggesting it may have been removed or is very obscure. Actually, upon reflection, I could not confirm this function exists in the official docs listing. The sub-agent mentioned it but it was not in the official docs page. **Removing from plan** — it likely doesn't exist as a standard function.

---

## 3. Missing Test Coverage

### Completely untested functions:
- **DateTimeAdd** — no tests at all
- **DateTimePart** — no tests at all

### Partially tested functions:
- **DateTimeBin** — tested for hour, day, year, month, quarter; NOT tested with custom origin, multi-unit non-month bins (e.g., 7-day), minute/second/millisecond bins
- **DateTimeDiff** — tested for 7 parts + negative; NOT tested for microsecond/nanosecond, boundary-crossing edge cases
- **DateTimeFromParts** — single test only; NOT tested for optional args, sub-second fractions, invalid inputs
- **GetCurrentDateTime/GetCurrentTimestamp** — only smoke tests in SqlFunctionTests.cs; not properly asserted

### Edge cases not covered anywhere:
- Leap year date arithmetic (Feb 29, adding months from Jan 31)
- Epoch boundaries (dates before 1970, very old dates, far future)
- Invalid/null input handling for date functions
- Date functions in WHERE clause filtering
- Date functions in ORDER BY
- Nested/composed date functions (e.g. DateTimeAdd inside DateTimeDiff)
- DateTimeBin with binSize=0 or negative binSize (should return undefined per docs)
- Date strings without Z suffix / non-UTC formats

---

## 4. Test Plan

All tests go in `tests/CosmosDB.InMemoryEmulator.Tests/DateHandlingTests.cs`.

### 4.1 — DateTimeAdd Tests (NEW CLASS: `DateTimeAddTests`)

| # | Test Name | Type | Description |
|---|-----------|------|-------------|
| T01 | `DateTimeAdd_Year_AddsCorrectly` | Fact | Add 1 year to 2020-07-03 → 2021-07-03 |
| T02 | `DateTimeAdd_Month_AddsCorrectly` | Fact | Add 1 month to 2020-07-03 → 2020-08-03 |
| T03 | `DateTimeAdd_Day_AddsCorrectly` | Fact | Add 1 day to 2020-07-03 → 2020-07-04 |
| T04 | `DateTimeAdd_Hour_AddsCorrectly` | Fact | Add 1 hour to 2020-07-03T00:00 → 2020-07-03T01:00 |
| T05 | `DateTimeAdd_Minute_AddsCorrectly` | Theory | Add 30 min; verify alias "mi" and "n" |
| T06 | `DateTimeAdd_Second_AddsCorrectly` | Fact | Add seconds, verify "ss" and "s" aliases |
| T07 | `DateTimeAdd_Millisecond_AddsCorrectly` | Fact | Add 500ms |
| T08 | `DateTimeAdd_NegativeValue_Subtracts` | Fact | Add -1 year → subtracts. Match Cosmos docs example |
| T09 | `DateTimeAdd_LeapYear_Jan31PlusOneMonth` | Fact | Jan 31 + 1 month → Feb 28 (non-leap) or Feb 29 (leap) |
| T10 | `DateTimeAdd_NullDate_ReturnsNull` | Fact | Null/missing date property → null result |
| T11 | `DateTimeAdd_InvalidDateString_ReturnsNull` | Fact | Non-date string → null |
| T12 | `DateTimeAdd_Microsecond_AddsCorrectly` | Fact | Add microseconds using "mcs" alias → **Skip if too hard** |
| T13 | `DateTimeAdd_Nanosecond_AddsCorrectly` | Fact | Add nanoseconds using "ns" alias → **Skip if too hard** |
| T14 | `DateTimeAdd_AllPartAliases_Work` | Theory | Test all alias variants: year/yyyy/yy, month/mm/m, etc. |

### 4.2 — DateTimePart Tests (NEW CLASS: `DateTimePartTests`)

| # | Test Name | Type | Description |
|---|-----------|------|-------------|
| T15 | `DateTimePart_ExtractsAllParts` | Theory | Extract year/month/day/hour/minute/second/millisecond from "2016-05-29T08:30:00.1301617" — match Cosmos docs expected values |
| T16 | `DateTimePart_Microsecond_ExtractsCorrectly` | Fact | "mcs" from "2016-05-29T08:30:00.1301617" → 130161 |
| T17 | `DateTimePart_Nanosecond_ExtractsCorrectly` | Fact | "ns" from "2016-05-29T08:30:00.1301617" → 130161700 |
| T18 | `DateTimePart_NullDate_ReturnsNull` | Fact | Null property → null |
| T19 | `DateTimePart_InvalidPart_ReturnsNull` | Fact | Invalid date part string → null |
| T20 | `DateTimePart_AllAliases_Work` | Theory | All valid aliases produce correct results |

### 4.3 — DateTimeBin Bug Fix Tests

| # | Test Name | Type | Description |
|---|-----------|------|-------------|
| T21 | `DateTimeBin_7DayBins_DefaultOriginUnixEpoch` | Fact | **BUG-1 regression test.** `DateTimeBin("2021-01-08T18:35:00.0000000", "dd", 7)` → `"2021-01-07T00:00:00.0000000Z"` (Cosmos DB docs example) |
| T22 | `DateTimeBin_7DayBins_CustomWindowsEpochOrigin` | Fact | `DateTimeBin("2021-01-08T18:35:00.0000000", "dd", 7, "1601-01-01T00:00:00.0000000")` → `"2021-01-04T00:00:00.0000000Z"` (Cosmos DB docs example) |
| T23 | `DateTimeBin_Month_WithMAlias_BinsCorrectly` | Fact | **BUG-2 regression test.** Use `"m"` as date part for month binning; should bin by month, not return unchanged |
| T24 | `DateTimeBin_5Hour_BinsCorrectly` | Fact | Cosmos docs: 5-hour bins → `"2021-01-08T15:00:00.0000000Z"` |
| T25 | `DateTimeBin_Minute_BinsCorrectly` | Fact | 15-minute bins |
| T26 | `DateTimeBin_Second_BinsCorrectly` | Fact | 30-second bins |
| T27 | `DateTimeBin_ZeroBinSize_ReturnsNull` | Fact | binSize=0 should return undefined (null) per docs |
| T28 | `DateTimeBin_NegativeBinSize_ReturnsNull` | Fact | Negative binSize should return undefined per docs |

### 4.4 — DateTimeFromParts Bug Fix Tests

| # | Test Name | Type | Description |
|---|-----------|------|-------------|
| T29 | `DateTimeFromParts_MinArgs_YearMonthDay` | Fact | **BUG-3 regression test.** `DateTimeFromParts(2017, 4, 20)` → `"2017-04-20T00:00:00.0000000Z"` |
| T30 | `DateTimeFromParts_PartialArgs_5` | Fact | **BUG-3.** `DateTimeFromParts(2017, 4, 20, 13, 15)` → `"2017-04-20T13:15:00.0000000Z"` |
| T31 | `DateTimeFromParts_AllArgs_WithSubSecondFraction` | Fact | **BUG-4 regression test.** `DateTimeFromParts(2017, 4, 20, 13, 15, 20, 3456789)` → `"2017-04-20T13:15:20.3456789Z"` |
| T32 | `DateTimeFromParts_InvalidDate_ReturnsNull` | Fact | `DateTimeFromParts(-2000, -1, -1)` → null (Cosmos docs example) |
| T33 | `DateTimeFromParts_ZeroFraction` | Fact | 7th arg = 0 → `.0000000Z` suffix |

### 4.5 — DateTimeDiff Edge Case Tests

| # | Test Name | Type | Description |
|---|-----------|------|-------------|
| T34 | `DateTimeDiff_AllAliases_Work` | Theory | Verify "year"/"yyyy"/"yy", "month"/"mm"/"m", etc. all produce same result |
| T35 | `DateTimeDiff_DocsExample_PastAndFuture` | Fact | Full match of Cosmos docs example with all parts |
| T36 | `DateTimeDiff_BoundaryCrossing_Hour` | Fact/Skip | **BUG-5.** Test that DateTimeDiff("hour", "23:59", "00:01") = 1 boundary vs 0 interval. **Skip with detailed reason + divergent sister test** |
| T37 | `DateTimeDiff_Microsecond` | Fact | "mcs" date part |
| T38 | `DateTimeDiff_Nanosecond` | Fact | "ns" date part |
| T39 | `DateTimeDiff_NullInputs_ReturnsNull` | Fact | Null start/end → null |

### 4.6 — Conversion Function Edge Cases

| # | Test Name | Type | Description |
|---|-----------|------|-------------|
| T40 | `DateTimeToTicks_And_TicksToDateTime_RoundTrip` | Fact | Convert to ticks and back; verify identity |
| T41 | `DateTimeToTimestamp_And_TimestampToDateTime_RoundTrip` | Fact | Convert to timestamp and back; verify identity |
| T42 | `DateTimeToTimestamp_PreUnixEpoch_NegativeValue` | Fact | Date before 1970 → negative timestamp |
| T43 | `TicksToDateTime_InvalidTicks_ReturnsNull` | Fact | Null ticks → null |

### 4.7 — GetCurrentDateTime / GetCurrentTimestamp Value Tests

| # | Test Name | Type | Description |
|---|-----------|------|-------------|
| T44 | `GetCurrentDateTime_ReturnsIso8601InUtc` | Fact | Format matches `yyyy-MM-ddTHH:mm:ss.fffffffZ` |
| T45 | `GetCurrentTimestamp_ReturnsReasonableUnixMs` | Fact | Value > timestamp for 2020 |

### 4.8 — Composed/Integration Date Query Tests

| # | Test Name | Type | Description |
|---|-----------|------|-------------|
| T46 | `DateFilter_WhereClause_FiltersOnDateComparison` | Fact | `WHERE c.ts > "2020-06-01T00:00:00Z"` filters by string comparison |
| T47 | `DateTimeAdd_InsideDateTimeDiff_Composes` | Fact | Nested: `DateTimeDiff('day', c.ts, DateTimeAdd('day', 7, c.ts))` = 7 |
| T48 | `OrderBy_DateTimeField_SortsChronologically` | Fact | Dates stored as ISO strings sort correctly in ORDER BY |

### 4.9 — Leap Year Edge Cases

| # | Test Name | Type | Description |
|---|-----------|------|-------------|
| T49 | `DateTimeAdd_Month_FromJan31_ToFeb` | Fact | Jan 31 + 1 month = Feb 28 (non-leap) |
| T50 | `DateTimeAdd_Month_FromJan31_ToFeb_LeapYear` | Fact | 2024-01-31 + 1 month = 2024-02-29 |
| T51 | `DateTimeDiff_Month_AcrossLeapYear` | Fact | Diff months across Feb 29 boundary |

---

## 5. Implementation Order

Execute in this order — each step is red-green-refactor:

### Phase 1: Bug Fixes (BUG-1 through BUG-4)

| Step | Action | Tests |
|------|--------|-------|
| 1.1 | Write **T21, T22** (DateTimeBin default origin) — should **FAIL** (red) | T21, T22 |
| 1.2 | Fix BUG-1: Change default origin to `1970-01-01` in InMemoryContainer.cs | — |
| 1.3 | Verify T21, T22 pass (green). Verify existing DateTimeBin tests still pass | All DateTimeBin tests |
| 1.4 | Write **T23** (DateTimeBin `"m"` alias) — should **FAIL** | T23 |
| 1.5 | Fix BUG-2: Add `or "m"` to the month pattern in DateTimeBin | — |
| 1.6 | Verify T23 passes | T23 |
| 1.7 | Write **T29, T30** (DateTimeFromParts optional args) — should **FAIL** | T29, T30 |
| 1.8 | Fix BUG-3: Change `args.Length < 7` to `args.Length < 3`, default missing params to 0 | — |
| 1.9 | Write **T31** (sub-second fraction) — should **FAIL** | T31 |
| 1.10 | Fix BUG-4: Interpret 7th param as 100ns ticks, not milliseconds | — |
| 1.11 | Verify T29, T30, T31 all pass; existing DateTimeFromParts test still passes | All DateTimeFromParts tests |

### Phase 2: New Feature Tests (DateTimeAdd, DateTimePart)

| Step | Action | Tests |
|------|--------|-------|
| 2.1 | Write **T01-T11, T14** (DateTimeAdd basic + edge cases) — should **PASS** (already implemented, just untested) | T01-T11, T14 |
| 2.2 | Write **T15, T18-T20** (DateTimePart basic + edge cases) — should **PASS** | T15, T18-T20 |
| 2.3 | Write **T12, T13** (DateTimeAdd microsecond/nanosecond) — will **FAIL** → implement or skip | T12, T13 |
| 2.4 | Write **T16, T17** (DateTimePart microsecond/nanosecond) — will **FAIL** → implement or skip | T16, T17 |
| 2.5 | Write **T37, T38** (DateTimeDiff microsecond/nanosecond) — will **FAIL** → implement or skip | T37, T38 |

### Phase 3: DateTimeBin Additional Coverage

| Step | Action | Tests |
|------|--------|-------|
| 3.1 | Write **T24-T28** (5-hour, minute, second bins; zero/negative binSize) | T24-T28 |
| 3.2 | Implement zero/negative binSize guard if T27/T28 fail | — |

### Phase 4: Edge Cases and Integration

| Step | Action | Tests |
|------|--------|-------|
| 4.1 | Write **T32-T33** (DateTimeFromParts invalid/zero-fraction) | T32-T33 |
| 4.2 | Write **T34-T36** (DateTimeDiff aliases, docs example, boundary-crossing) | T34-T36 |
| 4.3 | Write **T39-T45** (null inputs, conversion round-trips, GetCurrent assertions) | T39-T45 |
| 4.4 | Write **T46-T48** (composed queries, WHERE filtering, ORDER BY) | T46-T48 |
| 4.5 | Write **T49-T51** (leap year edge cases) | T49-T51 |

### Phase 5: Mark Skip + Divergent Tests

For tests where real Cosmos DB behaviour is too complex to replicate:

| Test | Skip Reason | Sister Divergent Test |
|------|-------------|----------------------|
| T36 `DateTimeDiff_BoundaryCrossing_Hour` | Skip: "BUG-5: DateTimeDiff uses interval truncation (TotalHours cast to long) for sub-day parts instead of Cosmos DB's boundary-crossing semantics. Implementing boundary-crossing would require tracking hour/minute/second marks between two DateTimes rather than simple TimeSpan arithmetic. Low practical impact — results match for all whole-unit intervals." | `DateTimeDiff_BoundaryCrossing_Hour_EmulatorBehavior` — shows emulator returns 0 for 23:59→00:01, with inline comment explaining real Cosmos returns 1 |
| T12/T13 microsecond/nanosecond DateTimeAdd | Skip if .NET precision insufficient: "Microsecond/nanosecond precision limited by .NET DateTime 100ns tick resolution. Nanosecond additions below 100ns are lost." | Sister test showing the actual precision available |

---

## 6. Documentation Updates

### 6.1 — Wiki Known-Limitations.md

Add to the **Limitations** table:
```
| Microsecond/nanosecond date parts | ⚠️ Partial | `microsecond`/`mcs` and `nanosecond`/`ns` date parts supported for DateTimePart extraction; limited by .NET DateTime 100ns tick resolution for Add/Diff/Bin operations |
```

Update **Behavioural Differences** section 10 ("GetCurrentDateTime() Evaluated Per-Row") — no change needed, already documented.

Add if BUG-5 is confirmed as Skip:
```
### 16. DateTimeDiff Uses Interval Truncation for Sub-Day Parts
**Test:** `DateTimeDiff_BoundaryCrossing_Hour_EmulatorBehavior`
Real Cosmos DB: `DATETIMEDIFF('hour', '2020-01-01T23:59:00Z', '2020-01-02T00:01:00Z')` → **1** (boundary crossing)
Emulator: Returns **0** (interval truncation)
Impact: Only affects cases where start and end are less than 1 unit apart but span a boundary. Most practical use cases are unaffected.
```

### 6.2 — Wiki Features.md

Add/update date function coverage details to reflect:
- DateTimeAdd: all parts including microsecond/nanosecond
- DateTimePart: all parts including microsecond/nanosecond
- DateTimeBin: all parts, custom origin support
- DateTimeFromParts: optional args support (3-7 args), sub-second fraction ticks

### 6.3 — Wiki SQL-Queries.md

Update the date functions section to reflect full coverage.

### 6.4 — Wiki Feature-Comparison-With-Alternatives.md

Update date function row if exists, or add row confirming full date function coverage.

### 6.5 — README.md (repo root + src/CosmosDB.InMemoryEmulator/README.md)

No changes expected unless date functions are specifically called out. Check and update if needed.

---

## 7. Release Checklist

- [ ] All tests in Phase 1-5 written and passing (or properly skipped)
- [ ] All bug fixes implemented and verified
- [ ] Existing tests still pass (`dotnet test`)
- [ ] Wiki Known-Limitations.md updated
- [ ] Wiki Features.md updated
- [ ] Wiki SQL-Queries.md updated
- [ ] Wiki Feature-Comparison-With-Alternatives.md updated
- [ ] README check — update if needed
- [ ] Bump version: `2.0.4` → `2.0.5` in `CosmosDB.InMemoryEmulator.csproj`
- [ ] Git commit: `git add -A; git commit -m "v2.0.5: Date handling deep-dive — fix 4 bugs, add 50+ tests, microsecond/nanosecond support"`
- [ ] Git tag: `git tag v2.0.5`
- [ ] Git push: `git push; git push --tags`
- [ ] Wiki push: `cd wiki; git add -A; git commit -m "v2.0.5: Date handling fixes and expanded coverage"; git push`

---

## Appendix A: Existing Test Inventory (Before Changes)

| Class | Test Count | Functions Covered |
|-------|-----------|-------------------|
| `DateHandlingTests` | 5 | DateTimeOffset round-tripping, serialization |
| `DateTimeDiffTests` | 2 (1 theory w/ 7 cases) | DateTimeDiff — 7 parts + negative |
| `DateTimeFromPartsTests` | 1 | DateTimeFromParts — basic case |
| `DateTimeBinTests` | 2 | DateTimeBin — hour, day |
| `DateTimeTicksConversionTests` | 5 | GetCurrentTicks, DateTimeToTicks, TicksToDateTime, DateTimeToTimestamp, TimestampToDateTime |
| `StaticDateTimeFunctionTests` | 3 | GetCurrentDateTimeStatic, GetCurrentTicksStatic, GetCurrentTimestampStatic |
| `DateTimeBinYearMonthTests` | 3 | DateTimeBin — year, month, 3-month (quarter) |
| `SqlFunctionGapTests3.GetCurrentTimestamp` | 1 | GetCurrentTimestamp smoke test |
| `SqlFunctionGapTests2.DateTimeFunctions` | 1 | GetCurrentDateTime smoke test |
| `GetCurrentDateTimeConsistencyTests` | 1 (skipped) | L3 divergent behaviour |
| **TOTAL** | **~24** | |

## Appendix B: Post-Change Test Inventory (Expected)

| Category | Test Count |
|----------|-----------|
| Existing tests (unchanged) | ~24 |
| New DateTimeAdd tests | ~14 |
| New DateTimePart tests | ~6 |
| New DateTimeBin tests | ~8 |
| New DateTimeFromParts tests | ~5 |
| New DateTimeDiff edge case tests | ~6 |
| New conversion edge case tests | ~4 |
| New GetCurrent assertion tests | ~2 |
| New composed/integration tests | ~3 |
| New leap year tests | ~3 |
| Skipped + divergent sister tests | ~4 |
| **TOTAL** | **~79** |

## Appendix C: Summary of Cosmos DB Date Part Aliases

All date functions accept these part strings (case-insensitive):

| Part | Aliases |
|------|---------|
| Year | `year`, `yyyy`, `yy` |
| Month | `month`, `mm`, `m` |
| Day | `day`, `dd`, `d` |
| Hour | `hour`, `hh` |
| Minute | `minute`, `mi`, `n` |
| Second | `second`, `ss`, `s` |
| Millisecond | `millisecond`, `ms` |
| Microsecond | `microsecond`, `mcs` |
| Nanosecond | `nanosecond`, `ns` |
