#
# Immediately after the restart, we should have 
# - no traces from the 'primary' transaction
# - secondary transaction remains running
# 

--connection node_2_check
--enable_reconnect
SELECT COUNT(*) = 0 FROM t1 WHERE f1 = 'primary';
SELECT COUNT(DISTINCT node_uuid) = 1 FROM mysql.wsrep_streaming_log;

--connection node_3_check
--enable_reconnect
SELECT COUNT(*) = 0 FROM t1 WHERE f1 = 'primary';
SELECT COUNT(DISTINCT node_uuid) = 1 FROM mysql.wsrep_streaming_log;

# We can now commit the 'secondary' transaction

--connection node_3
COMMIT;

# And perform further checks

--connection node_2_check
SELECT COUNT(*) = 0 FROM t1 WHERE f1 = 'primary';
SELECT COUNT(*) > 0 FROM t1 WHERE f1 = 'secondary';

#
# At the end, we check that the SR table is empty on all nodes
#

--connection node_1
SELECT COUNT(*) = 0 FROM t1 WHERE f1 = 'primary';
SELECT COUNT(*) > 0 FROM t1 WHERE f1 = 'secondary';
SELECT COUNT(*) = 0 FROM mysql.wsrep_streaming_log;

--connection node_2_check
SELECT COUNT(*) = 0 FROM mysql.wsrep_streaming_log;

--connection node_3_check
SELECT COUNT(*) = 0 FROM mysql.wsrep_streaming_log;

--connection node_2_check
SET GLOBAL debug = '';
