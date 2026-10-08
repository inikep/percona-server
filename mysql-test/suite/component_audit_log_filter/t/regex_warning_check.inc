# Count literal rx_warning_text since regex_warning_mark.inc. A slow interval
# may have up to rx_warning_max warnings; fast intervals require rx_warning_min.
perl;
  use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);
  open my $mark,'<',"$ENV{MYSQLTEST_VARDIR}/tmp/regex-warning-mark" or die $!;
  my ($offset,$start)=split ' ',<$mark>;
  my $elapsed=clock_gettime(CLOCK_MONOTONIC)-$start;
  open my $f,'<:raw',"$ENV{MYSQLTEST_VARDIR}/log/mysqld.1.err" or die $!;
  seek $f,$offset,0 or die $!;
  my $count=0;
  while (<$f>) { ++$count if index($_,$ENV{rx_warning_text})>=0; }
  my $min=$ENV{rx_warning_min};
  my $max=$elapsed>=60 ? $ENV{rx_warning_max} : $min;
  die "Wrong warning count: $count, expected $min..$max in ${elapsed}s"
    unless $count >= $min && $count <= $max;
EOF
