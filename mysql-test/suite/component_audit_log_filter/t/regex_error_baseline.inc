SET @errors=(SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Audit_log_filter_regex_match_errors');
SET @lost=(SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Audit_log_filter_events_lost');
SELECT audit_log_rotate();
