--echo #
--echo # PS-11926: audit_log_read() dropped the data that the last compressed
--echo # input chunk of a log inflates to beyond the first 32 KB
--echo #

CREATE TABLE t1 (c1 INT);

SELECT audit_log_filter_set_filter('log_table_insert', '{"filter": {"class": {"name": "table_access", "event": {"name": "insert", "log": true}}}}');
SELECT audit_log_filter_set_user('%', 'log_table_insert');

--source clean_all_audit_logs.inc

delimiter |;
CREATE PROCEDURE insert_rows(n INT)
BEGIN
  DECLARE i INT DEFAULT 0;
  WHILE i < n DO
    INSERT INTO t1 VALUES (i);
    SET i = i + 1;
  END WHILE;
END|

CREATE PROCEDURE count_events()
BEGIN
  DECLARE batch JSON;
  SET @events = 0;
  SET batch = audit_log_read('{"start": {"timestamp": "2000-01-01 00:00:00"}}');
  read_loop: LOOP
    IF batch IS NULL OR JSON_LENGTH(batch) = 0 THEN
      LEAVE read_loop;
    END IF;
    IF JSON_TYPE(JSON_EXTRACT(batch, '$[last]')) = 'NULL' THEN
      SET @events = @events + JSON_LENGTH(batch) - 1;
      LEAVE read_loop;
    END IF;
    SET @events = @events + JSON_LENGTH(batch);
    SET batch = audit_log_read();
  END LOOP;
END|
delimiter ;|

--echo # The rotated log inflates to about 1 MB and takes more than one 32 KB
--echo # chunk of compressed input
CALL insert_rows(3000);
--let $rotated_log_name = `SELECT audit_log_rotate()`
--let ROTATED_LOG_FILE = `SELECT CONCAT(@@global.datadir, '$rotated_log_name')`

# The data loss needs a compressed log of more than one 32 KB input chunk
perl;
  my $file = $ENV{'ROTATED_LOG_FILE'};
  my $size = -s $file or die "Can't stat $file";
  die "$file has $size bytes, more than 32768 are needed" if $size <= 32768;
EOF

CALL count_events();
SELECT @events;

--echo #
--echo # Cleanup
DROP PROCEDURE insert_rows;
DROP PROCEDURE count_events;
DROP TABLE t1;
