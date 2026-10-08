# Wait for an actual row-lock wait before killing; observer has no regex rule.
CREATE TABLE rx_kill_data(id INT PRIMARY KEY,v INT) ENGINE=InnoDB;
INSERT INTO rx_kill_data VALUES(1,0);
connect(killer,localhost,rx,,test);
connection rx;
--let victim_id = `SELECT CONNECTION_ID()`
connection default;
SET @before=(SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Audit_log_filter_regex_match_errors');
--let kill_mode = 0
while ($kill_mode < 2) {
  if ($kill_mode == 0) {
    SET GLOBAL audit_log_filter.event_mode=REDUCED;
    SET @definition='{"filter":{"class":{"name":"general","event":{"name":"status","log":{"field":{"name":"general_query.str","regex":"rx_kill_(stmt|recovery)"}}}}}}';
  }
  if ($kill_mode == 1) {
    SET GLOBAL audit_log_filter.event_mode=FULL;
    SET @definition='{"filter":{"class":{"name":"query","event":{"name":"status_end","log":{"field":{"name":"query.str","regex":"rx_kill_(stmt|recovery)"}}}}}}';
  }
  --source regex_assign.inc
  SELECT audit_log_rotate();
  BEGIN;
  SELECT * FROM rx_kill_data WHERE id=1 FOR UPDATE;
  connection rx;
  --send UPDATE rx_kill_data SET v=v+1 WHERE id=1 /*rx_kill_stmt*/
  connection default;
  --let wait_condition = SELECT COUNT(*)=1 FROM performance_schema.data_lock_waits w JOIN performance_schema.threads t ON w.REQUESTING_THREAD_ID=t.THREAD_ID WHERE t.PROCESSLIST_ID=$victim_id
  --source include/wait_condition.inc
  connection killer;
  --eval KILL QUERY $victim_id
  connection rx;
  --error ER_QUERY_INTERRUPTED
  --reap
  SELECT 1 /*rx_kill_recovery*/;
  DO 0;
  connection default;
  ROLLBACK;
  --let rx_closed = `SELECT audit_log_rotate()`
  perl;
    require "$ENV{MYSQL_TEST_DIR}/suite/component_audit_log_filter/t/regex_records.pl";
    my $r=regex_records();
    die 'Killed statement/recovery count' unless @$r==2;
    for my $marker(qw(rx_kill_stmt rx_kill_recovery)) {
      die "Missing $marker" unless 1==grep {$_->{query}=~/\Q$marker\E/} @$r;
    }
EOF
  --inc $kill_mode
}
# Same-account KILL CONNECTION and a normal-disconnect control.
SET @definition='{"filter":{"class":{"name":"connection","event":{"name":"disconnect","log":{"field":{"name":"user.str","regex":"^rx$"}}}}}}';
--source regex_assign.inc
connection rx;
DO 0;
connection default;
SELECT audit_log_rotate();
connection killer;
--eval KILL CONNECTION $victim_id
connection default;
--let wait_condition = SELECT COUNT(*)=0 FROM performance_schema.threads WHERE PROCESSLIST_ID=$victim_id
--source include/wait_condition.inc
--let rx_count = 1
--source regex_check_count.inc
disconnect rx;
connect(rx,localhost,rx,,test);
DO 0;
disconnect rx;
connection default;
--let rx_count = 1
--source regex_check_count.inc
connect(rx,localhost,rx,,test);
--let victim_id = `SELECT CONNECTION_ID()`
connection default;
SET @definition='{"filter":{"class":[{"name":"general","event":{"name":"status","log":{"field":{"name":"general_query.str","regex":"rx_active_kill"}}}},{"name":"connection","event":{"name":"disconnect","log":{"field":{"name":"user.str","regex":"^rx$"}}}}]}}';
--source regex_assign.inc
BEGIN;
SELECT * FROM rx_kill_data WHERE id=1 FOR UPDATE;
SELECT audit_log_rotate();
connection rx;
--send UPDATE rx_kill_data SET v=v+1 WHERE id=1 /*rx_active_kill*/
connection default;
--let wait_condition = SELECT COUNT(*)=1 FROM performance_schema.data_lock_waits w JOIN performance_schema.threads t ON w.REQUESTING_THREAD_ID=t.THREAD_ID WHERE t.PROCESSLIST_ID=$victim_id
--source include/wait_condition.inc
connection killer;
--eval KILL CONNECTION $victim_id
connection rx;
--error ER_QUERY_INTERRUPTED,CR_SERVER_LOST,CR_SERVER_GONE_ERROR
--reap
connection default;
--let wait_condition = SELECT COUNT(*)=0 FROM performance_schema.threads WHERE PROCESSLIST_ID=$victim_id
--source include/wait_condition.inc
ROLLBACK;
--let rx_closed = `SELECT audit_log_rotate()`
perl;
  require "$ENV{MYSQL_TEST_DIR}/suite/component_audit_log_filter/t/regex_records.pl";
  my $r=regex_records();
  die 'Active kill count' unless @$r==2;
  die 'Active kill terminal record' unless 1==grep {($_->{query} || '')=~/rx_active_kill/} @$r;
  die 'Active kill disconnect' unless 1==grep {$_->{event} eq 'disconnect' || $_->{event} eq 'quit'} @$r;
EOF
disconnect rx;
connect(rx,localhost,rx,,test);
connection default;
SET @definition='{"filter":{"class":[{"name":"query","event":{"name":"status_end","log":{"field":{"name":"query.str","regex":"rx_statement_timeout"}}}},{"name":"general","event":{"name":"status","log":{"field":{"name":"general_query.str","regex":"rx_statement_timeout"}}}}]}}';
--source regex_assign.inc
--let have_timeout = `SELECT @@have_statement_timeout='YES'`
if ($have_timeout) {
  SELECT audit_log_rotate();
  connection rx;
  --error ER_QUERY_TIMEOUT
  SELECT /*+ MAX_EXECUTION_TIME(10) */ SLEEP(10) FROM rx_kill_data /*rx_statement_timeout*/;
  DO 0;
  connection default;
  --let rx_count = 2
  --source regex_check_count.inc
}
--enable_result_log
SELECT v AS killed_updates_committed FROM rx_kill_data;
SELECT VARIABLE_VALUE-@before AS killed_event_regex_errors FROM performance_schema.global_status WHERE VARIABLE_NAME='Audit_log_filter_regex_match_errors';
--disable_result_log
DROP TABLE rx_kill_data;
disconnect killer;
