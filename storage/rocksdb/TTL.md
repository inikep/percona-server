# Row expiry and transaction session time

All new modes default to OFF. Existing table TTL keeps its encoding, format
versions, wall-clock behavior and SQL interface.

## Absolute row expiry

```sql
CREATE TABLE events (
  id BIGINT PRIMARY KEY,
  created_at BIGINT UNSIGNED NOT NULL,
  expires_at BIGINT NULL,
  KEY (expires_at)
) ENGINE=ROCKSDB
COMMENT='ttl_duration=1000;ttl_col=created_at;row_ttl_col=expires_at';
```

`row_ttl_col` names a stored BIGINT, BIGINT UNSIGNED or TIMESTAMP column.
Nullable columns and STORED generated columns are supported; virtual generated
columns and negative BIGINT expiry values are rejected. A real primary key is
required. TIMESTAMP values use UTC Unix seconds, truncating fractional seconds.
Partition-qualified comments use the existing syntax, such as
`p0_row_ttl_col=expires_at`.

The row value is an absolute expiration time, and takes precedence over table
TTL. A row is expired when `expiry <= read_time`. NULL or zero uses the explicit
`ttl_col + ttl_duration` fallback, or never expires if no fallback exists.
Unsigned `18446744073709551615` always means never expire. Fallback addition
saturates at that value. Row TTL with implicit insert-time table TTL is rejected:
the eight-byte field cannot retain both an override and its original fallback
base. Existing implicit table TTL remains supported on its own.

The resolved expiry is stored in the existing eight-byte TTL slot, with a new
EXPIRY_TS index flag (2) and PK/SK KV versions 16/17. Legacy indexes continue to
use versions 15/16 and TTL flag 1. PK and secondary index expiry bytes are updated
together, including when only the expiry or a generated-column dependency changes.
The partial-index sentinel retains its never-expire meaning.

Changing the row TTL column, enabling/disabling row TTL, or changing an active
row TTL fallback requires `ALGORITHM=COPY`. Unchanged definitions support existing
in-place index operations. A `row_ttl_col` comment ignored by an older binary
remains inactive after upgrade, including during in-place ADD INDEX and UPDATE.
Opening such a table warns; use `ALTER TABLE ... FORCE, ALGORITHM=COPY` to activate
it. The persisted flag, rather than the presence of a comment, selects decoding.

An old binary cannot read the new absolute-expiry formats and must reject them.
Before downgrade, convert every affected table and partition with COPY to a
legacy TTL definition or no TTL. Conversion copies rows visible to that operation;
turning off read filtering before conversion is a deliberate choice to include
physically retained expired rows. This does not restore rows already purged.
Session-mode policy also requires the transition procedure below before downgrade.
No compatibility with unrelated pre-PS-9766 format changes is implied.

## Session time and the durable purge boundary

Enable `rocksdb_ttl_use_session_timestamp=ON` at startup. The first MyRocks access
in each transaction reserves `THD::query_start_in_secs()`, including SQL `SET
timestamp`. This time stays fixed through statement rollback, savepoints and
snapshot renewal. Physical RocksDB snapshot time remains separate. Implicit-TTL
INSERT stamps use each statement's session time in this mode; UPDATE preserves
the existing implicit stamp. With the option OFF, stamps remain wall-clock based.

A session timestamp alone cannot safely drive physical deletion. A delayed,
queued or retried transaction may need an earlier row even while no transaction
is active locally. The durable `rocksdb_ttl_safe_purge_timestamp` (W) establishes
both an admission fence and a compaction upper bound:

* New transactions with time T < W are rejected before engine access. T == W is
  accepted. Changing `timestamp` during a transaction does not change its T.
* Compaction never expires rows beyond W, the active transaction minimum, the
  oldest physical snapshot, or wall time, whichever is earliest.
* `SET GLOBAL rocksdb_ttl_safe_purge_timestamp=N` requires session mode and a
  nonzero, nondecreasing N no greater than any active or detached XA time.
* The SET synchronously writes the policy to the system column family before
  publication. New admissions wait during this write. A write error with uncertain
  durability closes admission and TTL purge until restart reloads the record.
  Ordinary transaction completion can continue.
* Recovered prepared XA transactions have unknown logical times: admission and
  TTL purge remain closed until all are committed or rolled back. Recovery
  administration remains available. In-memory detached XA retains its known time.

The caller advancing W must establish that **all** work below the candidate is
complete and no longer retryable: active and queued transactions, stopped or
parallel replica workers, every relevant channel, delayed source transactions,
and work whose replay checkpoint is not durable. A largest committed/applied
timestamp, commit-order preservation, an idle replica, or a fixed lag allowance
is not that proof. MyRocks does not implement this external coordinator. For before-image bypass or
maintenance that can read already-expired rows, the producer must additionally
keep W below the expiry of every required before image until application and
retry/checkpoint dependencies are discharged. The replica event timestamp and
its active-transaction reservation do not identify those earlier dependencies.
Without a producer establishing these conditions, keep W fixed: visibility works,
but later physical reclamation is bounded.

### First enable, restart and return to wall time

The policy record stores version 1, state SESSION_ACTIVE or LEGACY_AUTHORIZED,
and W. Do not delete or edit this record to evade a startup failure.

1. Quiesce writers/appliers, resolve prepared transactions, and establish a
   verified history horizon H. No future transaction or retry may need data
   earlier than H. A fresh empty database can use H=1. A previously compacted
   database needs an assessment of the retained history; enabling this feature
   cannot recover expired rows.
2. Start with `rocksdb_ttl_use_session_timestamp=ON` and the one-shot
   `rocksdb_ttl_session_bootstrap_timestamp=H`. H must be nonzero and at least any
   retained W. Durable bootstrap must succeed before admission opens.
3. Remove the bootstrap option after success. Ordinary session-mode restarts
   require bootstrap=0 and recover W from the record. Repeating a nonzero
   bootstrap while SESSION_ACTIVE is rejected, including after a crash where
   the bootstrap already became durable.
4. Advance W only under the coordinator contract above. Supported boundaries are
   integer Unix seconds through 2147483647, matching the SQL session-time domain.
5. To return to wall time, drain all old logical work and resolve prepared XA.
   Restart with session mode OFF, bootstrap=0, and the one-shot
   `rocksdb_ttl_session_allow_legacy_transition=ON`. This durably authorizes legacy
   purging. Remove the transition option afterwards. Starting in legacy mode
   directly from SESSION_ACTIVE is rejected.
6. Re-entering session mode after a legacy interval requires a newly assessed H
   and the bootstrap procedure again. The former W is only a lower bound.

Physical backup/restore must preserve the policy record with the data, and the
external producer must restore its dependencies/checkpoints consistently. A
logical dump contains table definitions and rows, not this policy record; restore
it into a fresh, quiesced target, establish a new horizon, and bootstrap before
accepting session-time work. `SET PERSIST` is not supported for W: its sole durable
source is the engine record. Startup configuration is not a replacement for it.

## Replication before-image lookup

`rocksdb_rpl_skip_ttl_read_filtering=ON` is a separate startup-only option.
UPDATE_ROWS and DELETE_ROWS before-image searches can find physically retained
expired rows. It covers point and scan paths and is cleared after the event.
Ordinary SELECT, statement-based replication, INSERT_ROWS and new-key uniqueness
checks retain their normal TTL policy. A truly absent row remains an error.

This helps when a source transaction first sees a live row but its later update
is replayed with a later first-access timestamp. It does not protect against
physical purge; session mode and a correctly maintained W provide that boundary.
For deterministic implicit TTL and visibility, producers must supply consistent
statement times and pin the required transaction timestamp. Binlog event time
alone cannot reconstruct every arbitrary source read history.

## Retention controls and diagnostics

| Variable | Scope and default | Effect |
|---|---|---|
| `rocksdb_ttl_use_session_timestamp` | Startup, OFF | Stable logical read time and durable admission policy |
| `rocksdb_ttl_session_bootstrap_timestamp` | Startup, 0 | One-shot verified history horizon |
| `rocksdb_ttl_session_allow_legacy_transition` | Startup, OFF | Explicit drained exit from session mode |
| `rocksdb_ttl_safe_purge_timestamp` | Dynamic global, durable | Monotonic admission/purge boundary in session mode |
| `rocksdb_ttl_dry_run` | Startup, OFF | Return expired rows and retain them during TTL compaction |
| `rocksdb_pause_ttl_compaction_filter` | Dynamic global, OFF | Stop TTL deletion; reads still filter |
| `rocksdb_rpl_skip_ttl_read_filtering` | Startup, OFF | Retained RBR before-image lookup |

Dry run changes query results and DML/uniqueness behavior: it is a full-retention
mode, not an invisible preview. It still enforces admission and recovery fences.
`rocksdb_ttl_would_hide` and `rocksdb_ttl_would_purge` count candidate decisions,
not unique rows; normal filtered/expired counters count only actual actions.
The read-filter OFF switch does not suppress dry-run measurement. The master
`rocksdb_enable_ttl=OFF` still disables TTL decisions. Pause suppresses purge
measurement as well as deletion; dropped-index cleanup remains enabled.
`rocksdb_ttl_rejected_admissions` counts refused engine admissions.
`INFORMATION_SCHEMA.ROCKSDB_DDL` exposes flags, KV versions and fallback duration.
`SHOW ENGINE ROCKSDB TRANSACTION STATUS` distinguishes logical TTL time from
physical snapshot age in session mode.

## Implementation references

The row-expiry design and scenarios draw on patches
`21cf26af0aa5faa34a7a594809eed14fa324424a` and
`90882e32a8a8598551075495c18f3c1edc6b52ef`. The existing transaction list and snapshot
publication fix on PS-9766 are reused. Facebook's
`1fb186d93d1c` provides the compaction-pause precedent; no HLC service, binlog format
extensions or SQL-layer clock interface is imported. The explicit durable
admission/purge protocol is an additional safeguard against delayed and retryable
work that is absent from the local active-transaction list.
