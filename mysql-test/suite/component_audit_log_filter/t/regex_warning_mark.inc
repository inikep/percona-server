# Start an isolated error-log interval and a monotonic suppression clock.
perl;
  use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);
  my $offset=-s "$ENV{MYSQLTEST_VARDIR}/log/mysqld.1.err";
  open my $f,'>',"$ENV{MYSQLTEST_VARDIR}/tmp/regex-warning-mark" or die $!;
  print $f "$offset ",clock_gettime(CLOCK_MONOTONIC);
EOF
