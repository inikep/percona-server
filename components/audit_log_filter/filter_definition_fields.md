# Audit Log Filter Definition Fields

This reference lists the canonical class, event, and field names accepted by
filter-definition validation through `audit_log_filter_set_filter()`.

## Notes

- Query string filters compare the original client-charset bytes (or the
  password-obfuscated statement selected by the server). Query length fields
  count those bytes, before conversion or replacement. For example, a UTF-8
  filter value containing `café` matches a utf8mb4 statement but not the same
  statement sent as latin1.
- SQL text written to JSON, JSONL, NEW XML, OLD XML, and syslog is converted to
  UTF-8 (utf8mb4) after filtering and before escaping. Digest replacements are
  already UTF-8. `audit_log_read()` returns the converted file bytes; historical
  files are not rewritten. A record larger than `read_buffer_size` is returned
  whole in its own batch; the reader grows its output buffer for that record.
- Known limitation: the server reports the query charset in effect when an
  event is generated, not the one prepared statement text was parsed with.
  Events that carry the text of a prepared statement (for example the
  `general`, `query` status and `table_access` events of `EXECUTE` or
  `COM_STMT_EXECUTE`) are therefore converted from the wrong charset if
  `character_set_client` changed after `PREPARE`, e.g.
  `SET NAMES utf8mb4; PREPARE s FROM 'SELECT "é"'; SET NAMES latin1; EXECUTE s;`
  logs `SELECT "Ã©"`. `PREPARE ... FROM '<literal>'` in a non-UTF-8 session is
  affected the same way, because the server stores the literal as utf8mb3.
- Malformed query bytes, characters without a Unicode mapping and bytes of an
  incomplete final sequence are replaced with `?`. Conversion resumes after
  each bad byte, preserving trailing ASCII and multibyte text.
  Binary-labeled text is validated as UTF-8, preserving password-rewritten
  UTF-8 identifiers. Missing or unknown source charsets and resource failures
  lose the event through the
  counted `Audit_log_filter_events_lost` path; they never emit raw query bytes.
  The same applies to memory exhaustion while the record is formatted and
  escaped for the log: the event is counted as lost, nothing is written, and
  `audit_log_read_bookmark()` is not advanced.

- The names below are filter-definition names, not necessarily the names used by
  the JSON log formatter.
- `Field Type` reflects the type accepted by the current validator in
  `get_event_field_value_type()`.
- Some numeric-looking fields are currently validated as `string` because they
  are not explicitly typed in `get_event_field_value_type()`.
- Filter-definition validation only accepts the class names documented below.
- When `audit_log_filter.event_mode=REDUCED` (the default), only the following
  events are tracked and accepted by filter-definition validation:
  - `general`: `status`
  - `connection`: `connect`, `disconnect`, `change_user`
  - `table_access`: `read`, `insert`, `update`, `delete`
  - `message`: `internal`, `user`

  Class names that have no allowed events in REDUCED mode (`global_variable`,
  `command`, `query`, `stored_program`, `authentication`, `parse`) are rejected
  entirely. Subclass names that are not in the list above (e.g. `general/log`,
  `connection/pre_authenticate`) are also rejected during filter validation.
  At runtime, events not in the REDUCED set are silently skipped.
- Lifecycle-related records with class names `audit`, `server_startup`, and
  `server_shutdown` are not valid filter-definition targets. Startup and
  shutdown lifecycle events are ignored by the audit log filter if they are
  received.
- For `connection.connection_type`, the validator accepts numeric values `0..5`
  and the pseudo-constants `::undefined`, `::tcp/ip`, `::socket`,
  `::named_pipe`, `::ssl`, and `::shared_memory`.

### Differences from MySQL Enterprise Audit 8.4.7

For ordinary client character sets, query output is converted to UTF-8 in the
same way as Enterprise Audit. Binary-labeled query text is instead validated as
UTF-8: valid sequences are preserved and malformed sequences are replaced with
`?`. Enterprise 8.4.7 re-encodes each binary byte as a Unicode code point, which
also double-encodes UTF-8 usernames in password-obfuscated statements. Validation
avoids that double encoding; mixed-encoding literal fragments in rewritten SQL
may still require replacements.

This conversion applies only to SQL statement text. Connection attributes and
other non-query fields retain their existing behavior. In particular, this
change does not add Enterprise's charset conversion for connection attributes.

## `general`

Supported events: `log`, `error`, `result`, `status`
REDUCED mode: only `status`

| Field Name | Field Type | Description |
| --- | --- | --- |
| `general_error_code` | integer | Event error code. |
| `general_thread_id` | unsigned integer | Event thread ID. Currently an alias of `general_connection_id`. |
| `general_connection_id` | unsigned integer | Event connection ID. |
| `general_user.str` | string | User name recorded for the general event. |
| `general_user.length` | unsigned integer | User name length. |
| `general_command.str` | string | General command text, for example `Query`. |
| `general_command.length` | unsigned integer | General command text length. |
| `general_query.str` | string | SQL statement text associated with the event. |
| `general_query.length` | unsigned integer | SQL statement text length. |
| `general_host.str` | string | Client host name. |
| `general_host.length` | unsigned integer | Client host name length. |
| `general_sql_command.str` | string | SQL command name associated with the statement, for example `select`. |
| `general_sql_command.length` | unsigned integer | SQL command name length. |
| `general_external_user.str` | string | External user or OS login associated with the event. |
| `general_external_user.length` | unsigned integer | External user or OS login length. |
| `general_ip.str` | string | Client IP address. |
| `general_ip.length` | unsigned integer | Client IP address length. |

## `connection`

Supported events: `connect`, `disconnect`, `change_user`, `pre_authenticate`
REDUCED mode: `connect`, `disconnect`, `change_user`

| Field Name | Field Type | Description |
| --- | --- | --- |
| `status` | integer | Current connection event status. |
| `connection_id` | unsigned integer | Connection ID. |
| `user.str` | string | User name of this connection. |
| `user.length` | unsigned integer | User name length. |
| `priv_user.str` | string | Privileged user name. |
| `priv_user.length` | unsigned integer | Privileged user name length. |
| `external_user.str` | string | External user name or OS login. |
| `external_user.length` | unsigned integer | External user name length. |
| `proxy_user.str` | string | Proxy user used for the connection. |
| `proxy_user.length` | unsigned integer | Proxy user name length. |
| `host.str` | string | Connection host name. |
| `host.length` | unsigned integer | Connection host name length. |
| `ip.str` | string | Connection IP address. |
| `ip.length` | unsigned integer | Connection IP address length. |
| `database.str` | string | Default database specified at connection time. |
| `database.length` | unsigned integer | Default database name length. |
| `connection_type` | integer | Connection type code. |
|  |  | `0` or `::undefined`: Undefined |
|  |  | `1` or `::tcp/ip`: TCP/IP |
|  |  | `2` or `::socket`: Socket |
|  |  | `3` or `::named_pipe`: Named pipe |
|  |  | `4` or `::ssl`: TCP/IP with encryption |
|  |  | `5` or `::shared_memory`: Shared memory |

## `table_access`

Supported events: `read`, `insert`, `update`, `delete`
REDUCED mode: all events

| Field Name | Field Type | Description |
| --- | --- | --- |
| `connection_id` | unsigned integer | Event connection ID. |
| `sql_command_id` | integer | SQL command ID. |
| `query.str` | string | SQL statement text. |
| `query.length` | unsigned integer | SQL statement text length. |
| `table_database.str` | string | Database name associated with event. |
| `table_database.length` | unsigned integer | Database name length. |
| `table_name.str` | string | Table name associated with event. |
| `table_name.length` | unsigned integer | Table name length. |

## `global_variable` *(FULL mode only)*

Supported events: `get`, `set`

| Field Name | Field Type | Description |
| --- | --- | --- |
| `connection_id` | string | Event connection ID. |
| `variable_name.str` | string | Variable name. |
| `variable_name.length` | string | Variable name length. |
| `variable_value.str` | string | Variable value. |
| `variable_value.length` | string | Variable value length. |

## `command` *(FULL mode only)*

Supported events: `start`, `end`

| Field Name | Field Type | Description |
| --- | --- | --- |
| `status` | string | Command event status code. |
| `connection_id` | string | Event connection ID. |
| `command.str` | string | Command text. |
| `command.length` | string | Command text length. |

## `query` *(FULL mode only)*

Supported events: `start`, `nested_start`, `status_end`, `nested_status_end`

| Field Name | Field Type | Description |
| --- | --- | --- |
| `status` | string | Query event status code. |
| `connection_id` | string | Event connection ID. |
| `sql_command_id` | string | SQL command string associated with the query event. The field name is retained as `sql_command_id` for compatibility. |
| `query.str` | string | SQL query text. |
| `query.length` | string | SQL query text length. |
| `query_charset` | string | SQL query character set name. |

## `stored_program` *(FULL mode only)*

Supported events: `execute`

| Field Name | Field Type | Description |
| --- | --- | --- |
| `connection_id` | string | Event connection ID. |
| `database.str` | string | Database where the stored program is defined. |
| `database.length` | string | Database name length. |
| `name.str` | string | Stored program name. |
| `name.length` | string | Stored program name length. |

## `authentication` *(FULL mode only)*

Supported events: `flush`, `authid_create`, `credential_change`, `authid_rename`, `authid_drop`

| Field Name | Field Type | Description |
| --- | --- | --- |
| `status` | string | Authentication event status. |
| `connection_id` | string | Event connection ID. |
| `user.str` | string | User name. |
| `user.length` | string | User name length. |
| `host.str` | string | Host name. |
| `host.length` | string | Host name length. |

## `message`

Supported events: `internal`, `user`
REDUCED mode: all events

| Field Name | Field Type | Description |
| --- | --- | --- |
| `connection_id` | string | Event connection ID. |
| `component.str` | string | Component name. |
| `component.length` | string | Component name length. |
| `producer.str` | string | Message producer name. |
| `producer.length` | string | Message producer name length. |
| `message.str` | string | Message text. |
| `message.length` | string | Message text length. |

## `parse` *(FULL mode only)*

Supported events: `preparse`, `postparse`

| Field Name | Field Type | Description |
| --- | --- | --- |
| `connection_id` | string | Event connection ID. |
| `flags` | string | Parse rewrite flags value. |
| `query.str` | string | Original SQL query text. |
| `query.length` | string | Original SQL query text length. |
| `rewritten_query.str` | string | Rewritten SQL query text. |
| `rewritten_query.length` | string | Rewritten SQL query text length. |

## Regular expressions on string fields

A field condition may use `regex` instead of `value`:

```json
{
  "filter": {
    "class": {
      "name": "table_access",
      "event": {
        "name": ["insert", "update", "delete"],
        "log": {
          "and": [
            {"field": {"name": "table_database.str", "value": "tpcc"}},
            {"field": {"name": "table_name.str", "regex": "^(new_orders|orders|history)[0-9]+$"}}
          ]
        }
      }
    }
  }
}
```

This selects writes to the numbered tables in `tpcc`, including newly created
shards, without adding each table to the definition. Put cheap equality checks
before regex checks in an `and`. An equality `value` such as `"orders.*"` remains
literal; the existing `string_find` function remains a literal substring search.

A regex-bearing field object must contain exactly one `name` and one `regex`.
`value`, duplicate members and unknown members are rejected. The named field
must exist for the event class and have declared string type. Integer fields,
including `connection_type`, are rejected; FULL-mode fields already represented
as strings remain eligible. Legacy value-only objects retain their existing
validation and extra-member behavior.

The pattern must be a nonempty, valid UTF-8 JSON string. An empty pattern is a
configuration error in every action context, including `abort` and stored
rules loaded by `audit_log_filter_flush()`. To match a present empty value use
`"value":""`, `"regex":"^$"`, or `"regex":"\\A\\z"`. Explicit match-all
expressions such as `.*` are allowed. Missing fields and non-string runtime
values do not match. When general/table-access query capture is unavailable,
the existing field maps expose a present empty string and a zero length.

Matching uses ICU Unicode regular expressions and searches anywhere in the
available field value. It is case-sensitive by default; `(?i)` enables ICU
case-insensitive matching. SQL collation and `lower_case_table_names` do not
supply regex flags: the predicate matches the event's reported value. ICU is
neither an exact POSIX ERE nor a PCRE compatibility contract. `^…$` supplies
conventional anchors, but `$` can match before a final line terminator. Use
`\A…\z` for absolute anchoring.

JSON requires backslashes to be escaped. Ordinary SQL string literals add a
second escaping layer (unless `NO_BACKSLASH_ESCAPES` is enabled). SQL JSON
constructors can keep the layers separate, for example:

```sql
SET @pattern = CONCAT(CHAR(92 USING utf8mb4), 'Aorders[0-9]+', CHAR(92 USING utf8mb4), 'z');
SET @field = JSON_OBJECT('field', JSON_OBJECT(
  'name', 'table_name.str', 'regex', @pattern));
```

Embedded NULs are preserved in decoded patterns and in the available field
string. The pre-existing `mysql_cstring_to_string()` helper uses `strlen`, so
some event fields have already lost bytes after a NUL before filtering. Regex
does not repair that extraction limitation. General and table-access raw query
maps preserve captured byte lengths and can include embedded NULs.

### Raw subjects and output encoding

Regex interprets the complete available field string as UTF-8, replacing
malformed sequences with U+FFFD. It does not transcode the subject from the
client's MySQL charset or use the converted log-output string. Thus UTF-8
`café` does not match raw Latin-1 `caf\xE9`, although an ASCII marker elsewhere
in either subject remains searchable. Different malformed sequences can map
to the same replacement character. This differs from byte equality and is not
charset-aware matching.

Selected query text is converted to UTF-8 later, for output. Latin-1 text
selected by an ASCII predicate can therefore appear correctly as `café` in
the log. Output conversion replaces malformed bytes with `?`, independently
of ICU's U+FFFD subject decoding. Raw `.length` predicates, UTF-8 digests,
password obfuscation, and the prepared-statement charset limitation described
above remain unchanged. Regex evaluation does not fetch SQL text for parse
events or use `query_output` as its subject.

### Resource limits and failures

Each constructed condition owns a compiled pattern. Each evaluation creates
its own matcher with limits of 32 ICU engine-work units and 8,000,000 bytes of
backtracking stack, independent of session SQL regex variables. The existing
16 KiB UDF definition limit remains; the engine wrapper also bounds decoded
patterns to 16 KiB. Compilation errors include ICU's reported line and
character position when available, not a UTF-8 byte or UTF-16 code-unit offset.

These limits are not a wall-clock deadline or total-memory ceiling. Long
linear scans, compilation, multiple regex leaves and concurrent matcher
allocations still have costs. Anchoring can reduce search work without making
arbitrary patterns constant-time. Identifier predicates generally scan less
text than query predicates. KILL received during evaluation does not interrupt
regex matching; it finishes normally or reaches an engine limit. An already
killed or timed-out session does not by itself make a regex condition fail, so
terminal audit events can still match.

A runtime engine/resource error evaluates to **false at the leaf** and
increments `Audit_log_filter_regex_match_errors` exactly once. Normal misses,
missing fields and non-string values do not increment it. The Boolean/action
semantics then apply:

| Context | Effect of a failed regex leaf |
|---|---|
| Positive `log` | Does not select the event |
| Negated `log` | `not(false)` can select the event |
| Positive `abort` | Does not block the statement |
| `print.field.print` | Uses the replacement instead of the original value |
| Replacement `activate` | Does not activate the replacement rule |

Failures can therefore cause audit gaps or bypass a positive abort predicate.
Short-circuited leaves are not evaluated and do not count as errors. A handled
regex error alone does not increment `Audit_log_filter_events_lost`. Separate
capture/output failures retain their existing accounting; an event can have
both counters increase only when both failures are reached.

The first failure of each compiled condition is eligible for a warning;
subsequent warnings from that instance are suppressed for 60 seconds using a
monotonic clock. Every error is counted even while warnings are suppressed.
Warnings identify the filter, field, escaped pattern preview, category and ICU
status, never the subject. Previews expose pattern text and are bounded to 96
bytes, with `...` for truncation; they are not unique identifiers. Different
conditions have independent limiters. Reloads construct new conditions and
reset those limiters, while sessions retaining an old condition retain its
limiter. Many conditions or repeated reloads can therefore produce many warnings.

### Upgrade, reload and downgrade

An older value-only field could contain an ignored `regex` member. Such a
field now specifies mutually exclusive operators and is rejected. Inventory
actual keys anywhere in stored definitions, including nested replacements,
before upgrading:

```sql
SELECT name
FROM mysql.audit_log_filter
WHERE JSON_CONTAINS_PATH(filter, 'one', '$**.regex');
```

This is a conservative inventory, not a validator. Inspect the returned rows
before modifying them. An ordinary string value equal to `"regex"` without a
member of that name is not returned. JSON constructors may normalize duplicate
members; raw input validation also rejects duplicates.

`set_filter()` validates and stores a definition but does not reload the
registry. `set_user()` changes the mapping before attempting reload. A failed
reload/explicit flush preserves the last published snapshot; a successful
explicit flush detaches ordinary sessions on their next event. Reconnect or
change user when verifying a new mapping. An invalid enabled definition can
prevent publication of the entire registry. On a fresh start without a valid
snapshot, ordinary filtered events can go unaudited even though internal
start/stop records still appear. REDUCED mode can skip disabled classes before
validating their nested conditions.

Upgrade every server that may load these definitions before distributing them.
Do not assume that UDF or direct table writes cannot propagate through
replication; check the deployment's binlog settings and replica loading behavior.

Before downgrading, remove or rewrite **all** regex definitions, including
nested replacements, while still running the newer binary. Successfully flush
compatible rules, reconnect and verify ordinary filtered coverage before
starting an older binary. Incompatible enabled definitions may block the whole
registry on the older server. Recovery can require repairing/removing stored
rows and flushing; a valid `set_filter()` alone does not publish a new registry.
