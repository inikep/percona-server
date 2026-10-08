--let rx_exists = `SELECT COUNT(*) FROM mysql.audit_log_filter WHERE name='rx'`
if ($rx_exists) {
  SELECT audit_log_filter_remove_filter('rx');
}
--let rx_ok = `SELECT audit_log_filter_set_filter('rx', @definition)='OK'`
if (!$rx_ok) {
  --die Regex definition was rejected
}
--let rx_ok = `SELECT audit_log_filter_set_user('rx@localhost','rx')='OK'`
if (!$rx_ok) {
  --die Regex mapping failed
}
--let rx_ok = `SELECT audit_log_filter_flush()='OK'`
if (!$rx_ok) {
  --die Regex flush failed
}
if ($rx_connected) {
  connection rx;
  --let rx_old_id = `SELECT CONNECTION_ID()`
  disconnect rx;
  connection default;
  --let wait_condition = SELECT COUNT(*)=0 FROM performance_schema.threads WHERE PROCESSLIST_ID=$rx_old_id
  --source include/wait_condition.inc
  if (!$success) {
    --die Previous audited session did not finish disconnecting
  }
  connect(rx,localhost,rx,,test);
  --let victim_id = `SELECT CONNECTION_ID()`
  connection default;
}
