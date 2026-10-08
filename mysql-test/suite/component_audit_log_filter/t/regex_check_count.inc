--source regex_sync.inc
--let rx_closed = `SELECT audit_log_rotate()`
perl;
  require "$ENV{MYSQL_TEST_DIR}/suite/component_audit_log_filter/t/regex_records.pl";
  my $r = regex_records();
  die "Expected $ENV{rx_count} records, got ".scalar(@$r) unless @$r == $ENV{rx_count};
EOF
