--exec suite/rocksdb_hotbackup/include/setup_replication_gtid.sh

let $slave_sync_timeout = 1800;
source include/rpl/deprecated/wait_for_replica_to_sync_with_source.inc;
