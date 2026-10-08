# A decisive Boolean operand wins over an error in either order. Exercise
# action boundaries too: abort AND false, print OR true, activate OR true.
--let rx_order = 0
while ($rx_order < 2) {
  --echo abort_and_false error_last=$rx_order
  SET @operands=JSON_ARRAY(JSON_OBJECT('field',JSON_OBJECT('name','table_name.str','regex','rx_errors')),JSON_OBJECT('field',JSON_OBJECT('name','table_database.str','value','no_such_db')));
  if ($rx_order) {
    SET @operands=JSON_ARRAY(JSON_EXTRACT(@operands,'$[1]'),JSON_EXTRACT(@operands,'$[0]'));
  }
  SET @definition=JSON_OBJECT('filter',JSON_OBJECT('class',JSON_OBJECT('name','table_access','event',JSON_OBJECT('name','insert','abort',JSON_OBJECT('and',CAST(@operands AS JSON))))));
  --source regex_assign.inc
  --source regex_error_baseline.inc
  SET @rows=(SELECT COUNT(*) FROM rx_errors);
  connection rx;
  SET SESSION debug='+d,audit_log_filter_regex_runtime_error';
  INSERT INTO rx_errors VALUES(3);
  SET SESSION debug='-d,audit_log_filter_regex_runtime_error';
  connection default;
  --enable_result_log
  SELECT COUNT(*)-@rows AS allowed_rows FROM rx_errors;
  --disable_result_log
  --source regex_error_delta.inc

  --echo print_or_true error_last=$rx_order
  SET @operands=JSON_ARRAY(CAST(@regex AS JSON),CAST(@guard AS JSON));
  if ($rx_order) {
    SET @operands=JSON_ARRAY(JSON_EXTRACT(@operands,'$[1]'),JSON_EXTRACT(@operands,'$[0]'));
  }
  SET @definition=JSON_OBJECT('filter',JSON_OBJECT('class',JSON_OBJECT('name','general','event',JSON_OBJECT('name','status','log',CAST(@guard AS JSON),'print',JSON_OBJECT('field',JSON_OBJECT('name','general_query.str','print',JSON_OBJECT('and',JSON_ARRAY(CAST(@guard AS JSON),JSON_OBJECT('or',CAST(@operands AS JSON)))),'replace',JSON_OBJECT('function',JSON_OBJECT('name','query_digest'))))))));
  --source regex_assign.inc
  --source regex_error_baseline.inc
  connection rx;
  SET SESSION debug='+d,audit_log_filter_regex_runtime_error';
  SELECT 11118 AS rx_error;
  SET SESSION debug='-d,audit_log_filter_regex_runtime_error';
  connection default;
  --source regex_error_delta.inc
  --let rx_count = 1
  --source regex_check_count.inc
  perl;
    require "$ENV{MYSQL_TEST_DIR}/suite/component_audit_log_filter/t/regex_records.pl";
    my $r=regex_records();
    die 'A true print condition must preserve the original text'
      unless @$r==1 && $r->[0]->{query} eq 'SELECT 11118 AS rx_error';
EOF

  --echo activate_or_true error_last=$rx_order
  SET @operands=JSON_ARRAY(JSON_OBJECT('field',JSON_OBJECT('name','table_name.str','regex','rx_errors')),JSON_OBJECT('field',JSON_OBJECT('name','table_database.str','value','test')));
  if ($rx_order) {
    SET @operands=JSON_ARRAY(JSON_EXTRACT(@operands,'$[1]'),JSON_EXTRACT(@operands,'$[0]'));
  }
  SET @definition=JSON_OBJECT('filter',JSON_OBJECT('class',JSON_OBJECT('name','table_access','event',JSON_OBJECT('name','insert','log',CAST('false' AS JSON),'filter',JSON_OBJECT('activate',JSON_OBJECT('or',CAST(@operands AS JSON)),'class',JSON_OBJECT('name','general','event',JSON_OBJECT('name','status','log',CAST(@guard AS JSON))))))));
  --source regex_assign.inc
  --source regex_error_baseline.inc
  connection rx;
  SET SESSION debug='+d,audit_log_filter_regex_runtime_error';
  INSERT INTO rx_errors VALUES(4);
  SET SESSION debug='-d,audit_log_filter_regex_runtime_error';
  SELECT 11118 AS rx_error;
  connection default;
  --source regex_error_delta.inc
  --let rx_count = 1
  --source regex_check_count.inc
  --inc $rx_order
}
