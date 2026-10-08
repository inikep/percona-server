# PS-11118 validation record

Recorded on 2026-10-08 against origin/8.4
`ce138ece09384fee0dcbee440d98231899544fff` plus this change. This is implementation
QA on one Linux x86-64 host, not certification of every supported package.

## Builds and automated tests

Source: `/work/percona-server-gpt`; primary build:
`/work/percona-server-gpt-gcc16`. GCC/G++ 16.0.1, ccache, Ninja, GNU gold 1.16.
Build concurrency never exceeded 30. CMake's linker-flags check rejects
`-fuse-ld=gold` here; the equivalent `-B<build>/gold` selects a directory containing
`ld -> /usr/bin/ld.gold`. The executable's gold note was checked.

| Configuration | Result |
|---|---|
| Debug, bundled ICU 77.1 | Server, component and four unit-test targets built |
| Debug unit tests | All 25 tests in `audit_regex`, `audit_field_regex`, `audit_query_charset`, `audit_json_handler` passed |
| Complete component MTR suite | 157 successful, six skipped because `ddl_rewriter` was initially unavailable; after building it all six passed in a focused run (plus the harness shutdown check) |
| Added malformed-byte runtime fixture | Both JSON and NEW/XML combinations passed after the complete-suite run |
| Release, system ICU 74.2 | Component and all four unit-test targets built; all 25 tests passed |
| Release/system component in Debug server | Validation and both format combinations of runtime and reload passed (five feature cases plus shutdown check); this is not a Release-server suite run |
| ASan + UBSan | All seven wrapper tests passed with system ICU 74.2 |
| TSan | Shared immutable pattern/local UText ownership test passed; the system ICU shared libraries themselves were not instrumented |
| Valgrind 3.22.0 | Attempted reload/lifecycle MTR, not passed: unsupported amd64 syscall 333 (`statx`) warnings interleaved with the server error log; harness pid-file/shutdown timeouts also occurred |

Main repeat commands, from the primary build:

```sh
cmake --build . -j30 --target component_audit_log_filter audit_regex-t audit_field_regex-t audit_query_charset-t audit_json_handler-t
ctest --output-on-failure -R '^(audit_regex|audit_field_regex|audit_query_charset|audit_json_handler)$'
cd mysql-test
./mtr --parallel=4 --force --retry=0 --suite=component_audit_log_filter
./mtr --parallel=4 --retry=0 component_audit_log_filter.log_charset_rewrite component_audit_log_filter.verify_event_occurrence_full component_audit_log_filter.verify_event_occurrence_reduced
```

The component suite requires the reference-cache component to be loaded at
startup and the usual test components, keyring component, and `ddl_rewriter`
plugin. The build-system ICU data environment override applies only to the
standalone bundled wrapper test. Production uses the server's data resolution.

The Release build is in `<build>/regex-qa/release-system`, configured with
`-DCMAKE_BUILD_TYPE=Release -DWITH_ICU=system` and the same compiler, launcher,
linker and source. For the mixed-build integration run a separate plugin
directory links the ordinary test modules and this Release audit component;
MTR receives `--mysqld=--plugin-dir=<that-directory>`. The primary component
binary was not replaced.

Sanitizer runs compile `audit_regex.cc`, `audit_regex-t.cc`, and the repository's
GoogleTest 1.17.0 `gtest-all.cc`/`gtest_main.cc` together, using `-std=c++20 -O1 -g
-pthread`, system `icu-i18n`/`icu-uc` pkg-config flags, and respectively
`-fsanitize=address,undefined` or `-fsanitize=thread`. The TSan run selects
`AuditRegex.ConcurrentLocalUTextAndRetainedOwnership`. C++ allocation injection
is confined to the wrapper test executable; ICU C allocation and process-wide
OOM are not claimed to be covered.

The committed tests cover strict member validation and exact errors, temporary
pattern ownership, Unicode positions, NUL/nonterminated inputs, raw field maps,
real timeout/stack errors and recovery, all action contexts, output-failure
accounting, killed/timeout terminal events, reload ownership, both log formats,
and independent monotonic warning suppression. The malformed UTF-8 SELECT
fixture confirms U+FFFD matching and `?` output conversion on the same event.

## Replication and downgrade rehearsal

Disposable data directories were initialized under `<build>/regex-qa`.
The pre-feature installation was
`/data/mysql-server/percona-8.4-deb-gcc16-rocks`, reporting
`8.4.11-11-debug`. Its files were only read. It is a pre-feature Percona build,
not the commercial MySQL binary and not a release-version downgrade claim.

SHA-256 identifiers for that installation:

- mysqld: `525d0c0dc594d3214a9fdc85540ae0be0fac21ddff3e14910b34838213ec8e4a`
- audit component: `574425d642160ca9c2d53641eb67529f9e335048140cdb61d1c24479d7a09025`

Procedure used for both old/new propagation comparisons:

1. Start two isolated servers on loopback ports, ROW binlog, GTID enabled,
   separate server IDs, synchronous JSON audit output. Load reference cache
   using a read-only executable manifest. Create the standard filter/user
   tables, install the audit component, and create `qa.events(id INT)` and
   a measured `rx_qa@localhost` account. Keep the observing root unassigned.
   Initialize each server's schema with `sql_log_bin=0`.
2. Start replication at the source's fresh `SHOW BINARY LOG STATUS` position,
   after independent setup. For each `sql_log_bin` setting 0 and 1, use fresh
   names to call `audit_log_filter_set_filter()` and `set_user()`, then insert
   a separate JSON definition directly into the table. Use an explicit unused
   filter ID (maximum + 1000); the existing UDF's explicit-ID insertion does
   not advance the SQL AUTO_INCREMENT cache.
3. Restore `sql_log_bin=1`, wait with `SOURCE_POS_WAIT(file, position, 30)`,
   and count each named filter and mapping on the replica. First run with the
   pre-feature binaries and `value:"events"`; stop both, switch to the patched
   binaries, and repeat with `regex:"^events$"` and new names.
4. Flush the replica, restart it, and flush again. Map its measured account to
   the propagated regex. On a fresh connection, insert one row followed by
   `DO 0` to wait past audit status notification. Read written-count deltas
   from the excluded administrative connection. Repeat after explicit flush
   on a retained connection, after reconnect, and after restart.

| Source setting | Old UDF filter / mapping / direct row | Patched UDF filter / mapping / direct row |
|---|---|---|
| `sql_log_bin=0` | 0 / 0 / 0 | 0 / 0 / 0 |
| `sql_log_bin=1` | 1 / 1 / 1 | 1 / 1 / 1 |

Replica regex coverage was one ordinary insert record after mapping, zero on
an old session after successful explicit flush, one after reconnect, and one
after restart. Replication of definitions does not itself reattach sessions.

The cross-binary downgrade procedure reused an isolated datadir:

1. Patched binary, enabled `^events$` regex, fresh measured connection: one
   ordinary insert record.
2. Stop, start pre-feature binary without rewriting: zero ordinary records;
   flush reports `ERROR: Filter 'qa_rule' has wrong format: event field definition
   'field' must have field 'name' provided as a string and 'value' as a string
   or integer`.
3. Remove the incompatible filter through the removal UDF, install the equality
   definition, map, flush and reconnect: one ordinary record.
4. Return to the patched binary, install regex again, then rewrite to equality,
   flush and verify one record **before** stopping. Start the pre-feature
   binary: ordinary coverage remains one record.

These deltas exclude component start/stop records. The procedure and raw output
are retained locally as `regex-qa/deployment.py`, `replica_load.py`, and
`/tmp/ps11118-{deployment,replica-load}.log`.

## ICU data and lifecycle

A staged basedir contains copied `lib/private/icudt77l` data, English error
messages and audit/reference-cache modules. It has no `icudt77l.lnk`, and
`ICU_DATA` is unset. A separate executable manifest loads reference cache.
The Debug executable uses this basedir, with its existing build-library RPATH;
this is a data-discovery smoke test, not a complete relocatable package test.

`\N{LATIN SMALL LETTER E}` compiled and selected `events` using the component.
SQL `REGEXP_LIKE()` succeeded before and after unloading the audit component;
reinstalling it restored one selected insert record. The ordinary MTR lifecycle
cases also exercise restart, unload/reinstall and named-character compilation.

## Component performance characterization

Measurements use the patched Debug/bundled server, synchronous JSON, a 64 MiB
InnoDB buffer pool, an excluded administrator, and one fresh measured
connection per scenario. Each sample includes the statement and a following
`DO 0` barrier. Timings include client round trips, SQL, storage and output;
they do **not** isolate audit CPU. Other jobs, including instrumented QA, shared
this machine. These numbers are not production latency guarantees or a claim
that regex is faster than equality.

Equality fixtures OR together `table_name.str = ordersN`; equivalent regexes
are `^orders(?:0|1|...|N)$`. Each early/late/miss scenario has 150 samples.
The largest fixture was chosen against the normalized stored JSON bound,
not merely compact UDF input: 268 names fit. The compact serialized sizes were
409/146 bytes (six-name equality/regex), 5575/424 (100 names), and 14983/1096
(268 names). Validation/insertion times include the UDF and persistence.

| Names | Operator | Early / late / miss median ms | Early / late / miss p99 ms | Set-filter ms |
|---|---|---|---|---|
| 6 | equality | 3.28 / 3.34 / 2.78 | 5.43 / 4.36 / 8.73 | 6.83 |
| 6 | regex | 4.89 / 3.30 / 3.01 | 12.33 / 8.73 / 8.19 | 11.54 |
| 100 | equality | 3.63 / 3.55 / 3.00 | 12.01 / 6.32 / 8.74 | 12.41 |
| 100 | regex | 3.71 / 4.01 / 3.04 | 11.49 / 10.21 / 9.20 | 9.75 |
| 268 | equality | 3.45 / 3.74 / 2.82 | 8.19 / 11.51 / 5.69 | 16.03 |
| 268 | regex | 3.37 / 3.54 / 2.86 | 9.83 / 10.05 / 8.03 | 7.86 |

Full-miss general/status query scans use `orders[0-9]+` against
`DO 1 /*` + padding + `*/`, three samples per length:

| Subject MiB | Equality mean ms | Regex mean ms |
|---|---|---|
| 1 | 9.42 | 23.10 |
| 16 | 184.47 | 319.95 |
| 64 | 831.33 | 1440.39 |

Additional observations:

- Unicode identifier match/miss median 3.79/2.81 ms; six reached regex leaves
  median 3.44 ms, p99 6.71 ms (150 samples each).
- The specified 49-byte `(a+)+$` timeout fixture: mean 34.15 ms, p99 34.26 ms,
  ten samples. This does not redefine ICU's engine-work limit as milliseconds.
- Eight clients, 800 matching inserts: 454 statements/second. Per-client
  p99 was 38.55–42.99 ms. Server RSS/HWM after the preceding long-query runs
  was 608336/991092 KiB; this is whole-server memory, not regex-only memory.
- Ten remove/validate/map/flush cycles: mean 24.00 ms, median 18.33 ms,
  p99 32.55 ms. Each cycle constructs more than one compiled condition.

The local `regex-qa/benchmark.py` and `/tmp/ps11118-benchmark.log` retain the
fixture generator and samples. Small-sample percentiles should not be used
as capacity-planning estimates.

For a size comparison with the exact base, the Release/system component was
relinked with its three changed existing translation units (`audit_rule`,
`audit_rule_parser`, `sys_vars`) compiled from origin/8.4 headers/sources,
omitting the two new regex objects and retaining the same common objects and
compiler/linker options. Unstripped module sizes were 1732872 bytes at baseline
and 1765240 bytes patched (+32368). After `strip --strip-unneeded`, the sizes
were 1355720 and 1380488 bytes (+24768). This measures the module with system
ICU; it excludes shared-library/package dependencies and is not the bundled
ICU package-size delta.

## Remaining release checks

A complete Release-server run, supported packaged-platform matrix and fully
instrumented ICU concurrency testing remain release QA. Valgrind lifecycle
needs a compatible host/tool run. Isolated audit-evaluation CPU and latency
from a kill received **during** matching were not measured; the passing MTR
killed-event cases verify terminal-event selection, not cancellation latency.
The staged data lookup does not certify package dependency/RPATH handling.
