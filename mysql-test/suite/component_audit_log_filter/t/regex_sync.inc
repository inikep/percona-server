# Run from the unfiltered root observer after all asynchronous queries are reaped.
# The response can precede terminal audit callbacks; Sleep is reached afterwards.
--let wait_condition = SELECT COUNT(*)=0 FROM performance_schema.threads WHERE TYPE='FOREGROUND' AND PROCESSLIST_USER IS NOT NULL AND PROCESSLIST_USER <> 'root' AND PROCESSLIST_COMMAND NOT IN ('Sleep','Daemon')
--source include/wait_condition.inc
if (!$success) {
  --die Audited sessions did not finish their terminal callbacks
}
