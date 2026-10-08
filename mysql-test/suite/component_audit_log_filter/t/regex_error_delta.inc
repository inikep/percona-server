--enable_result_log
SELECT VARIABLE_VALUE-@errors AS regex_errors FROM performance_schema.global_status WHERE VARIABLE_NAME='Audit_log_filter_regex_match_errors';
SELECT VARIABLE_VALUE-@lost AS events_lost FROM performance_schema.global_status WHERE VARIABLE_NAME='Audit_log_filter_events_lost';
--disable_result_log
