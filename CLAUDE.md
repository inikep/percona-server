# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is the **Percona Server 8.4** repository — a Percona fork of MySQL 8.4 LTS (currently 8.4.8-8). It is a C++17 database server built with CMake. Percona-specific contributions are concentrated in `components/audit_log_filter/`, `components/keyrings/`, `storage/innobase/`, and `plugin/` (procfs, percona-pam-for-mysql), with the rest being upstream Oracle MySQL code.

Remote tracking: `percona` remote → `https://github.com/percona/percona-server.git`; `mysql` remote → `https://github.com/mysql/mysql-server.git`.

Bugs and features are tracked in [Jira PS project](https://perconadev.atlassian.net/jira/software/c/projects/PS/issues).

## First-Time Setup

Fetch submodules before building:

```bash
git submodule update --init
```

## Build System

Always use **out-of-source builds**. The source tree must not be used as the build directory.

```bash
mkdir -p /build/percona-8.4 && cd /build/percona-8.4
cmake /data/mysql-server/percona-8.4
make -j$(nproc)
```

### Key CMake flags for development

| Flag | Purpose |
|---|---|
| `-DWITH_DEBUG=1` | Debug build with DBUG/safemutex support |
| `-DWITH_UNIT_TESTS=ON` | Compile GTest-based unit tests |
| `-DWITH_ASAN=1` | Address sanitizer |
| `-DWITH_LSAN=1` | Leak sanitizer |
| `-DWITH_MSAN=1` | Memory sanitizer (requires clang) |
| `-DWITH_VALGRIND=1` | Valgrind instrumentation |
| `-DMYSQL_MAINTAINER_MODE=ON` | Treat warnings as errors (developer mode) |
| `-DWITH_DEFAULT_COMPILER_OPTIONS=OFF` | Override built-in compiler flag defaults |
| `-DWITH_ROUTER=OFF` | Skip building MySQL Router |
| `-DWITH_PERCONA_TELEMETRY=ON` | Include Percona telemetry component |

The default build type is `RelWithDebInfo`. Boost is bundled in `extra/` and does not need to be installed separately.

## Running Tests

### MySQL Test Framework (MTR)

The primary test suite uses `mysql-test/mysql-test-run.pl`. No `make install` is needed — tests run directly against the build directory.

```bash
cd /build/percona-8.4/mysql-test   # run from the BUILD tree's mysql-test dir

# Smoke test: verify mysqld starts and shuts down correctly
./mtr --debug-server main.1st

# Run a single test
./mysql-test-run.pl <test_name>

# Run tests from a specific suite
./mysql-test-run.pl --suite=component_audit_log_filter <test_name>

# Run in parallel
./mysql-test-run.pl --parallel=8

# Record expected output for a new test
./mysql-test-run.pl --record <test_name>

# Run against an external server
./mysql-test-run.pl --extern --socket=/path/to/socket <test_name>

# Debug server mode (use with --manual-debug to get mysqld args for IDE)
./mtr --debug-server --manual-debug main.1st
```

Test files: `mysql-test/t/*.test`; expected results: `mysql-test/r/*.result`.

**Percona-specific MTR suites** (in `mysql-test/suite/`):
- `percona` — general Percona Server tests
- `percona_innodb` — Percona InnoDB-specific tests
- `component_audit_log_filter` — audit log filter component tests
- `component_percona_telemetry` — telemetry component tests
- `component_encryption_udf`, `component_masking_functions` — other Percona components

### Unit Tests (GTest/ctest)

Requires `-DWITH_UNIT_TESTS=ON` at configure time. Unit tests live in `unittest/gunit/`.

```bash
cd /build/percona-8.4
ctest --parallel $(nproc)
ctest -R <test_pattern>   # Run matching tests
```

## Code Style

Code uses **Google C++ style** via clang-format **version 15** (must match exactly — other versions will produce different output). Config: `.clang-format` in the repo root.

- Column limit: **80 characters**
- Indent width: **2 spaces**
- Pointer alignment: Right (`int *p`)
- Include order is **not sorted** by clang-format (include dependencies are sensitive)

Format a file: `clang-format-15 -i <file>`

Before committing, run `git clang-format` to format only the changed lines.

`clang-tidy` config is also present in `.clang-tidy`.

## Branch and Commit Conventions

Branch naming pattern (Jira issue number is mandatory):
```
PS-9876-8.4-short_description
```

Commit message format:
```
PS-9876 Fix compression bug with zlib call parameters

https://perconadev.atlassian.net/browse/PS-9876

Full description of what and why.
```

Squash to one commit per logical change before submitting a PR (e.g., one commit for a bug fix; two commits for a feature that requires preparatory changes).

## Architecture

### Query Processing (`sql/`)
The SQL layer handles parsing, analysis, optimization, and execution. Key files: `sql/sql_parse.cc` (query dispatch), `sql/sql_optimizer.cc` (join optimizer), `sql/sql_executor.cc` (execution engine).

### Storage Engines (`storage/`)
- **InnoDB** (`storage/innobase/`) — the primary transactional engine; most Percona-specific patches live here. Subdirs map to subsystems: `buf/` (buffer pool), `log/` (redo log), `lock/` (lock manager), `trx/` (transactions), `row/` (row operations), `btr/` (B-tree), `os/` (OS abstraction).
- **MyISAM**, **Archive**, **CSV**, **Memory**, **NDB** — other engines, largely unmodified.

### Percona Components (`components/`)

Server components use the service API defined in `libservices/`. Percona-specific components:

- **`audit_log_filter/`** — the primary active-development area. Implements rule-based audit logging with multiple output formats (JSON, JSONL, XML old/new) and writers (file, syslog). Key subdirs:
  - `log_writer/` — file/syslog writers with buffering, compression, and encryption decorators
  - `log_record_formatter/` — JSON, JSONL, and XML (old/new) formatters
  - `event_field_action/` — filter actions including `replace_field` and `block`/`log`
  - `event_field_condition/` — filter condition evaluation
- **`keyrings/`** — keyring backends: `keyring_file/`, `keyring_kmip/`, `keyring_kms/`, `keyring_vault/`
- **`masking_functions/`** — data masking UDFs
- **`percona_telemetry/`** — usage telemetry reporting
- **`audit_api_message_emit/`**, **`encryption_udf/`**, **`binlog_utils_udf/`** — additional UDF components

### Percona Plugins (`plugin/`)
- `procfs/` — exposes `/proc` filesystem data via SQL
- `percona-pam-for-mysql/` — PAM authentication plugin

### Pluggable Components (upstream, `components/`)
Audit null, validate_password, query_attributes, reference_cache — upstream components, largely unmodified.

### Replication & Binary Log
- `libbinlogevents/` — binary log event class definitions
- `libchangestreams/` — logical replication / change stream processing

### MySQL Router (`router/`)
Standalone high-availability proxy built alongside the server. Can be excluded with `-DWITH_ROUTER=OFF`.

### Version
Version is read from `MYSQL_VERSION` at CMake configure time. The version ID formula is `10000*MAJOR + 100*MINOR + PATCH`.
