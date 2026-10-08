--source include/have_component_audit_log.inc
--source ../inc/audit_tables_init.inc
--source ../inc/audit_comp_install.inc
--disable_query_log
--disable_result_log
SET NAMES utf8mb4;
SET GLOBAL audit_log_filter.event_mode=FULL;
CREATE USER rx@localhost;
GRANT ALL ON test.* TO rx@localhost;
GRANT ALL ON tpcc.* TO rx@localhost;
--let rx_dir = `SELECT @@datadir`
--let rx_format = `SELECT @@global.audit_log_filter.format`
