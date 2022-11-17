#
# Used in SR crash recovery testing. Starts SR transactions on two nodes simultaneously
# The expectation is that the transaction on node_3 may fail due to an assert in
# the applier on that node.
#

--connection node_3
--disable_warnings
DROP TABLE IF EXISTS t1;
--enable_warnings
CREATE TABLE t1 (f1 VARCHAR(10)) ENGINE = InnoDB;

SET AUTOCOMMIT=OFF;
SET SESSION wsrep_trx_fragment_size=1;
START TRANSACTION;

INSERT INTO t1 VALUES ('secondary'),('secondary'),('secondary'),('secondary'),('secondary');

--connection node_2
SET SESSION wsrep_trx_fragment_size=1;
INSERT INTO t1 VALUES ('primary'),('primary'),('primary'),('primary'),('primary'),('primary'),('primary'),('primary'),('primary'),('primary');

--sleep 5
--connection node_3
--error 2006,2013
SELECT 1 FROM t1;

--connection node_2
--let $wait_condition = SELECT VARIABLE_VALUE = 2 FROM performance_schema.global_status WHERE VARIABLE_NAME = 'wsrep_cluster_size';
--source include/wait_condition.inc
