#
# set $inconsistent_fragment to determine at which fragment inconsistency
# happens
#

--connect node_1_ctrl, 127.0.0.1, root, , test, $NODE_MYPORT_1

--connection node_1
CREATE TABLE t1 (f1 INTEGER PRIMARY KEY, f2 BLOB) ENGINE=InnoDB;

# Introduce inconsistency
--connection node_2
SET SESSION wsrep_on=OFF;
--eval INSERT INTO t1 VALUES ($inconsistent_fragment, 'X')
SET SESSION wsrep_on=ON;
DELETE FROM t1 WHERE f1 = 2;

# Perform an SR transaction that will hit the inconsistency
--connection node_1
--let $fragment_size = 16384
SET AUTOCOMMIT=OFF;
--eval SET SESSION wsrep_trx_fragment_size = $fragment_size
START TRANSACTION;
--eval INSERT INTO t1 VALUES (1, REPEAT('A', $fragment_size))
--eval INSERT INTO t1 VALUES (2, REPEAT('A', $fragment_size))
--eval INSERT INTO t1 VALUES (3, REPEAT('A', $fragment_size))
--eval INSERT INTO t1 VALUES (4, REPEAT('A', $fragment_size))
--eval INSERT INTO t1 VALUES (5, REPEAT('A', $fragment_size))
COMMIT;
SELECT COUNT(*) AS expect_0 FROM mysql.wsrep_streaming_log;

# Perform another SR transaction in order to have stuff in the
# wsrep_streaming_log table
START TRANSACTION;
--eval INSERT INTO t1 VALUES (11, REPEAT('A', $fragment_size))
--eval INSERT INTO t1 VALUES (12, REPEAT('A', $fragment_size))
--eval INSERT INTO t1 VALUES (13, REPEAT('A', $fragment_size))
--eval INSERT INTO t1 VALUES (14, REPEAT('A', $fragment_size))
--eval INSERT INTO t1 VALUES (15, REPEAT('A', $fragment_size))


--connection node_1_ctrl
# Node #2 has dropped from the cluster due to voting
--let $members = 2
--source include/wsrep_wait_membership.inc

# Bring node #2 back via SST
--connection node_2
SET SESSION wsrep_on=OFF;
--source include/shutdown_mysqld.inc
--source include/wait_until_disconnected.inc
--echo Starting mysqld
--source include/start_mysqld.inc

--connection node_1_ctrl
--let $members = 3
--source include/wsrep_wait_membership.inc

--connection node_2
--source include/wait_wsrep_ready.inc

# Node #2 should have some entries in its SR table post-restart
SELECT COUNT(*) > 0 FROM mysql.wsrep_streaming_log;

# Commit second SR transaction
--connection node_1
COMMIT;

# Confirm that all nodes are identical
--connection node_1
SELECT COUNT(*) AS expect_0 FROM mysql.wsrep_streaming_log;
SELECT COUNT(*) AS expect_10 FROM t1;

--connection node_2
SELECT COUNT(*) AS expect_0 FROM mysql.wsrep_streaming_log;
SELECT COUNT(*) AS expect_10 FROM t1;

--connection node_3
SELECT COUNT(*) AS expect_0 FROM mysql.wsrep_streaming_log;
SELECT COUNT(*) AS expect_10 FROM t1;

--connection node_1
DROP TABLE t1;

--disconnect node_1_ctrl
