SET @saved_definition=@definition;
SET @definition=JSON_OBJECT('filter',JSON_OBJECT('class',JSON_OBJECT('name','general','event',JSON_OBJECT('name','status','log',CAST(@guard AS JSON)))));
--source regex_assign.inc
--source regex_error_baseline.inc
connection rx;
--eval SET SESSION debug='+d,$capture_hook,audit_log_filter_regex_runtime_error'
SELECT 11118 AS rx_error;
--eval SET SESSION debug='-d,$capture_hook,audit_log_filter_regex_runtime_error'
DO 0;
connection default;
--source regex_sync.inc
SET @control_lost=(SELECT VARIABLE_VALUE-@lost FROM performance_schema.global_status WHERE VARIABLE_NAME='Audit_log_filter_events_lost');
--let control_valid = `SELECT @control_lost>0 AND VARIABLE_VALUE=@errors FROM performance_schema.global_status WHERE VARIABLE_NAME='Audit_log_filter_regex_match_errors'`
if (!$control_valid) {
  --die Capture failure control did not lose events without regex errors
}
--let rx_count = 0
--source regex_check_count.inc
SET @definition=@saved_definition;
--source regex_assign.inc
--source regex_error_baseline.inc
connection rx;
# If evaluation is reached despite the capture failure, this hook increments
# regex_errors. Zero errors therefore proves that evaluation was skipped.
--eval SET SESSION debug='+d,$capture_hook,audit_log_filter_regex_runtime_error'
SELECT 11118 AS rx_error;
--eval SET SESSION debug='-d,$capture_hook,audit_log_filter_regex_runtime_error'
DO 0;
connection default;
--source regex_sync.inc
--enable_result_log
SELECT VARIABLE_VALUE-@errors AS regex_errors FROM performance_schema.global_status WHERE VARIABLE_NAME='Audit_log_filter_regex_match_errors';
SELECT VARIABLE_VALUE-@lost=@control_lost AS events_lost_matches_control FROM performance_schema.global_status WHERE VARIABLE_NAME='Audit_log_filter_events_lost';
--disable_result_log
--let rx_count = 0
--source regex_check_count.inc
