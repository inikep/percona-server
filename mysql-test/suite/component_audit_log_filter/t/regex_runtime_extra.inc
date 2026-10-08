# Replacement activation selects a subsequent sentinel only after temp_1.
connection default;
SET @definition='{"filter":{"id":"main","class":{"name":"table_access","event":{"name":"insert","log":false,"filter":{"activate":{"field":{"name":"table_name.str","regex":"^temp_[0-9]+$"}},"class":{"name":"general","event":{"name":"status","log":{"field":{"name":"general_query.str","regex":"rx_sentinel"}}}}}}}}}';
--source regex_assign.inc
--source regex_sync.inc
SELECT audit_log_rotate();
connection rx;
INSERT INTO tpcc.temp_1 VALUES (1);
SELECT 1 AS rx_sentinel;
DO 0;
connection default;
--let rx_count = 1
--source regex_check_count.inc
# The existing replacement rule remains active until reset. Republish the
# original definition before opening the negative-control session.
--source regex_assign.inc
disconnect rx;
connect(rx,localhost,rx,,test);
INSERT INTO tpcc.keep_1 VALUES (1);
SELECT 1 AS rx_sentinel;
DO 0;
connection default;
--let rx_count = 0
--source regex_check_count.inc

# Raw charset subjects and UTF-8 output remain separate.
CREATE TABLE charset_filter_data(id INT);
INSERT INTO charset_filter_data VALUES(1);
--let rx_sql = $MYSQLTEST_VARDIR/tmp/regex.sql
perl;
  use Encode qw(encode);
  my $q="SELECT 'caf\x{e9}' AS c";
  for my $cs ('latin1','utf8mb4') {
    open my $f,'>:raw',"$ENV{rx_sql}.$cs" or die $!;
    print $f encode($cs eq 'latin1' ? 'iso-8859-1':'UTF-8',"$q;\n");
    close $f;
  }
EOF
SET @definition=JSON_OBJECT('filter',JSON_OBJECT('class',JSON_OBJECT('name','general','event',JSON_OBJECT('name','status','log',JSON_OBJECT('field',JSON_OBJECT('name','general_query.str','regex','café'))))));
--source regex_assign.inc
--source regex_sync.inc
SELECT audit_log_rotate();
--exec $MYSQL --default-character-set=latin1 -u rx test < $rx_sql.latin1 > $MYSQLTEST_VARDIR/tmp/regex.out
--exec $MYSQL --default-character-set=utf8mb4 -u rx test < $rx_sql.utf8mb4 > $MYSQLTEST_VARDIR/tmp/regex.out
--let rx_count = 1
--source regex_check_count.inc
SET @definition=JSON_OBJECT('filter',JSON_OBJECT('class',JSON_OBJECT('name','general','event',JSON_OBJECT('name','status','log',JSON_OBJECT('field',JSON_OBJECT('name','general_query.str','regex','AS c'))))));
--source regex_assign.inc
--exec $MYSQL --default-character-set=latin1 -u rx test < $rx_sql.latin1 > $MYSQLTEST_VARDIR/tmp/regex.out
--exec $MYSQL --default-character-set=utf8mb4 -u rx test < $rx_sql.utf8mb4 > $MYSQLTEST_VARDIR/tmp/regex.out
--source regex_sync.inc
--let rx_closed = `SELECT audit_log_rotate()`
perl;
  require "$ENV{MYSQL_TEST_DIR}/suite/component_audit_log_filter/t/regex_records.pl";
  my $r=regex_records();
  die 'Charset output mismatch' unless @$r==2 && 2==grep {$_->{query} eq "SELECT 'caf\x{e9}' AS c"} @$r;
EOF
SET @definition=JSON_OBJECT('filter',JSON_OBJECT('class',JSON_OBJECT('name','general','event',JSON_OBJECT('name','status','log',JSON_OBJECT('and',JSON_ARRAY(JSON_OBJECT('field',JSON_OBJECT('name','general_query.str','regex','AS c')),JSON_OBJECT('field',JSON_OBJECT('name','general_query.length','value',LENGTH(CONVERT("SELECT 'café' AS c" USING latin1))))))))));
--source regex_assign.inc
--exec $MYSQL --default-character-set=latin1 -u rx test < $rx_sql.latin1 > $MYSQLTEST_VARDIR/tmp/regex.out
--exec $MYSQL --default-character-set=utf8mb4 -u rx test < $rx_sql.utf8mb4 > $MYSQLTEST_VARDIR/tmp/regex.out
--let rx_count = 1
--source regex_check_count.inc
perl;
  use Encode qw(encode);
  for my $cs ('latin1','utf8mb4') {
    open my $f,'>:raw',"$ENV{rx_sql}.$cs" or die $!;
    print $f encode($cs eq 'latin1' ? 'iso-8859-1':'UTF-8',"SELECT 'caf\x{e9}' AS caf\x{e9} FROM charset_filter_data;\n");
    close $f;
  }
EOF
SET @definition=JSON_OBJECT('filter',JSON_OBJECT('class',JSON_OBJECT('name','general','event',JSON_OBJECT('name','status','log',JSON_OBJECT('field',JSON_OBJECT('name','general_query.str','regex','charset_filter_data')),'print',JSON_OBJECT('field',JSON_OBJECT('name','general_query.str','print',JSON_OBJECT('field',JSON_OBJECT('name','general_query.str','regex','café')),'replace',JSON_OBJECT('function',JSON_OBJECT('name','query_digest'))))))));
--source regex_assign.inc
--exec $MYSQL --default-character-set=latin1 -u rx test < $rx_sql.latin1 > $MYSQLTEST_VARDIR/tmp/regex.out
--exec $MYSQL --default-character-set=utf8mb4 -u rx test < $rx_sql.utf8mb4 > $MYSQLTEST_VARDIR/tmp/regex.out
--source regex_sync.inc
--let rx_closed = `SELECT audit_log_rotate()`
perl;
  require "$ENV{MYSQL_TEST_DIR}/suite/component_audit_log_filter/t/regex_records.pl";
  my $r=regex_records();
  die 'Charset print count' unless @$r==2;
  die 'Original missing' unless 1==grep {$_->{query} eq "SELECT 'caf\x{e9}' AS caf\x{e9} FROM charset_filter_data"} @$r;
  die 'Digest double decoded' unless 1==grep {$_->{query} eq "SELECT ? AS `caf\x{e9}` FROM `charset_filter_data`"} @$r;
EOF
DROP TABLE charset_filter_data;
# ICU substitutes U+FFFD in a malformed UTF-8 subject; output substitutes '?'.
# The malformed byte occurs inside a literal, accepted by this SELECT path.
perl;
  open my $f,'>:raw',"$ENV{rx_sql}.malformed" or die $!;
  print $f "SELECT 'malformed_probe_\xe9' AS p;\n";
  close $f;
EOF
SET @definition=JSON_OBJECT('filter',JSON_OBJECT('class',JSON_OBJECT('name','general','event',JSON_OBJECT('name','status','log',JSON_OBJECT('field',JSON_OBJECT('name','general_query.str','regex',CONCAT('malformed_probe_',CONVERT(0xEFBFBD USING utf8mb4))))))));
--source regex_assign.inc
--source regex_error_baseline.inc
--exec $MYSQL --binary-mode --default-character-set=utf8mb4 -u rx test < $rx_sql.malformed > $MYSQLTEST_VARDIR/tmp/regex.out
--source regex_sync.inc
--let rx_closed = `SELECT audit_log_rotate()`
perl;
  require "$ENV{MYSQL_TEST_DIR}/suite/component_audit_log_filter/t/regex_records.pl";
  my $r=regex_records();
  die 'Malformed subject/output policies changed' unless @$r==1 && $r->[0]->{query} eq "SELECT 'malformed_probe_?' AS p";
EOF
--let rx_no_error = `SELECT VARIABLE_VALUE=@errors FROM performance_schema.global_status WHERE VARIABLE_NAME='Audit_log_filter_regex_match_errors'`
if (!$rx_no_error)
{
  --die Malformed subject decoding incremented regex errors
}
--remove_file $rx_sql.malformed
# Binary-safe client sends actual NULs. Verify both statements execute first.
perl;
  open my $f,'>:raw',"$ENV{rx_sql}.nul" or die $!;
  print $f "SELECT 'nul_probe_a\x00b' AS p;\nSELECT 'nul_probe_ab' AS p;\n";
  close $f;
EOF
SET @definition='{"filter":{"class":{"name":"general","event":{"name":"status","log":{"field":{"name":"general_query.str","regex":"nul_probe"}}}}}}';
--source regex_assign.inc
--exec $MYSQL --binary-mode -u rx test < $rx_sql.nul > $MYSQLTEST_VARDIR/tmp/regex.out
--source regex_sync.inc
--let rx_closed = `SELECT audit_log_rotate()`
perl;
  open my $f,'<:raw',"$ENV{rx_dir}/$ENV{rx_closed}" or die $!;
  my $log=do { local $/; <$f> };
  die 'Binary control not executed' unless 2==(()=$log=~/nul_probe_a/g);
EOF
SET @pattern=CONCAT('nul_probe_a',CHAR(0 USING utf8mb4),'b');
SET @definition=JSON_OBJECT('filter',JSON_OBJECT('class',JSON_OBJECT('name','general','event',JSON_OBJECT('name','status','log',JSON_OBJECT('field',JSON_OBJECT('name','general_query.str','regex',@pattern))))));
--source regex_assign.inc
--enable_result_log
SELECT HEX(JSON_UNQUOTE(JSON_EXTRACT(filter,'$.filter.class.event.log.field.regex')))='6E756C5F70726F62655F610062' AS stored_pattern_exact FROM mysql.audit_log_filter WHERE name='rx';
--disable_result_log
--exec $MYSQL --binary-mode -u rx test < $rx_sql.nul > $MYSQLTEST_VARDIR/tmp/regex.out
--source regex_sync.inc
--let rx_closed = `SELECT audit_log_rotate()`
perl;
  if ($ENV{rx_format} eq 'JSON') {
    require "$ENV{MYSQL_TEST_DIR}/suite/component_audit_log_filter/t/regex_records.pl";
    my $r=regex_records();
    die 'NUL subject lost' unless @$r==1 && $r->[0]->{query} eq "SELECT 'nul_probe_a\0b' AS p";
  } else {
    open my $f,'<:raw',"$ENV{rx_dir}/$ENV{rx_closed}" or die $!;
    my $log=do { local $/; <$f> };
    die 'Wrong NUL XML count' unless 1==(()=$log=~/nul_probe_a/g);
    die 'Control selected' if $log=~/nul_probe_ab/;
  }
EOF
--remove_file $rx_sql.nul
--remove_file $rx_sql.latin1
--remove_file $rx_sql.utf8mb4
--remove_file $MYSQLTEST_VARDIR/tmp/regex.out
# Username regex applies independently to multiple connections.
CREATE USER app_svc_1@localhost, app_svc_2@localhost, other_user@localhost;
SET @definition='{"filter":{"class":{"name":"connection","event":{"name":"connect","log":{"field":{"name":"user.str","regex":"^app_svc_"}}}}}}';
SELECT audit_log_filter_set_filter('rx_users',@definition);
SELECT audit_log_filter_set_user('app_svc_1@localhost','rx_users');
SELECT audit_log_filter_set_user('app_svc_2@localhost','rx_users');
SELECT audit_log_filter_set_user('other_user@localhost','rx_users');
--source regex_sync.inc
SELECT audit_log_rotate();
connect(u1,localhost,app_svc_1,,);
connect(u2,localhost,app_svc_2,,);
connect(u3,localhost,other_user,,);
connection default;
--source regex_sync.inc
--let rx_closed = `SELECT audit_log_rotate()`
perl;
  require "$ENV{MYSQL_TEST_DIR}/suite/component_audit_log_filter/t/regex_records.pl";
  my $r=regex_records();
  die 'User predicate count' unless @$r==2;
  for my $user(qw(app_svc_1 app_svc_2)) {
    die "Missing $user" unless 1==grep {($_->{user} || '') =~ /^\Q$user\E(?:\[|$)/} @$r;
  }
EOF
disconnect u1;
disconnect u2;
disconnect u3;
DROP USER app_svc_1@localhost, app_svc_2@localhost, other_user@localhost;
SELECT audit_log_filter_remove_filter('rx_users');
--source regex_kill.inc
