#
# Used in SR crash recovery testing. Starts SR transactions on two nodes simultaneously
#

--connection node_2
--disable_warnings
DROP TABLE IF EXISTS t1;
--enable_warnings
CREATE TABLE t1 (f1 VARCHAR(10)) ENGINE = InnoDB;

SET AUTOCOMMIT=OFF;
SET SESSION wsrep_trx_fragment_size=1;
START TRANSACTION;

--error 0,2013
INSERT INTO t1 VALUES ('primary'),('primary'),('primary'),('primary'),('primary');

--connection node_3
SET AUTOCOMMIT=OFF;
SET SESSION wsrep_trx_fragment_size=1;
START TRANSACTION;

if ($applier_crash) {
    --error 0,2013
}

INSERT INTO t1 VALUES ('secondary'),('secondary'),('secondary'),('secondary'),('secondary');

--connection node_2
--error 0,2006,2013
INSERT INTO t1 VALUES ('primary'),('primary'),('primary'),('primary'),('primary');

--connection node_3

if ($applier_crash) {
    --error 0,2006,2013
}
INSERT INTO t1 VALUES ('secondary'),('secondary'),('secondary'),('secondary'),('secondary');
