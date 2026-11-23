# ==== Purpose ====
#
# Check the log after a failed reopen in log_rotate_reopen_failure.test:
# events are lost while the log is closed, and the next rotation opens it
# again.
#
# ==== Usage ====
#
# --let $reopen_failure_step = step number
# --source log_rotate_reopen_failure_step.inc

--echo # Events are lost while the log is closed
--let $lost_before = `$events_lost`
--eval SELECT $reopen_failure_step AS step
--let $lost_after = `$events_lost`
--let $lost = `SELECT $lost_after > $lost_before`
--let $size = `$current_size`
--echo events lost: $lost, current log size: $size
--source log_rotate_reopen_failure_files.inc

--echo # The next rotation opens the log again
if ($reopen_failure_step >= 4) {
  SELECT audit_log_rotate() <> '' AS is_file_rotated;
}
if ($reopen_failure_step < 4) {
  SELECT audit_log_rotate() = '' AS is_nothing_rotated;
}
--let $written_before = `$events_written`
--let $lost_before = `$events_lost`
--eval SELECT $reopen_failure_step AS step
--let $written_after = `$events_written`
--let $lost_after = `$events_lost`
--let $written = `SELECT $written_after > $written_before`
--let $lost = `SELECT $lost_after - $lost_before`
--let $size = `SELECT ($current_size) > 0`
--echo events written: $written, events lost: $lost, current log size > 0: $size
--source log_rotate_reopen_failure_files.inc
