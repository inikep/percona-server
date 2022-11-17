#
# This file is used to perform checks on the SR slave after it has been restarted
# after it was crashed using a sync point in the applier

# Initially, we expect:
# - no traces from the 'secondary' transaction
# - primary transaction remains running
# 

--connection node_2_check
--enable_reconnect
--let $wait_condition = SELECT VARIABLE_VALUE = 3 FROM performance_schema.global_status WHERE VARIABLE_NAME = 'wsrep_cluster_size';
--source include/wait_condition.inc

SELECT * FROM mysql.wsrep_streaming_log;
SELECT COUNT(*) > 0 FROM t1 WHERE f1 = 'primary';
SELECT COUNT(*) = 0 FROM t1 WHERE f1 = 'secondary';
SELECT COUNT(*) = 0 FROM mysql.wsrep_streaming_log;

--connection node_3_check
--enable_reconnect
--source include/wait_until_connected_again.inc
SELECT * FROM mysql.wsrep_streaming_log;
SELECT COUNT(*) > 0 FROM t1 WHERE f1 = 'primary';
SELECT COUNT(*) = 0 FROM t1 WHERE f1 = 'secondary';
SELECT COUNT(*) = 0 FROM mysql.wsrep_streaming_log;

--connection node_1
SELECT * FROM mysql.wsrep_streaming_log;
SELECT COUNT(*) > 0 FROM t1 WHERE f1 = 'primary';
SELECT COUNT(*) = 0 FROM t1 WHERE f1 = 'secondary';
SELECT COUNT(*) = 0 FROM mysql.wsrep_streaming_log;
