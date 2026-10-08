# Wait until non-root sessions are idle. general/status is notified after the
# server has sent the statement result, and the session enters Sleep only
# after that notification.
--disable_query_log
let $wait_condition = SELECT COUNT(*) = 0 FROM performance_schema.threads
  WHERE TYPE = 'FOREGROUND' AND PROCESSLIST_USER IS NOT NULL
    AND PROCESSLIST_USER <> 'root'
    AND PROCESSLIST_COMMAND NOT IN ('Sleep', 'Daemon');
--source include/wait_condition.inc
--disable_query_log
--disable_result_log
