# Check transaction state without the FB SHOW ENGINE TRANSACTION extension.
SELECT WRITE_COUNT, LOCK_COUNT, READ_ONLY
FROM information_schema.rocksdb_trx
WHERE THREAD_ID = CONNECTION_ID();
