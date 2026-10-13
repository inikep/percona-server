#
# Skip the test unless the Random Query Generator can be run.
# Source it before the test changes any server state.
#

let RQG_BASE = $MYSQL_TEST_DIR/../rqg/rqg/common/mariadb-patches;

# RQG is not bundled with the source tree and needs Perl DBD::mysql
--perl
my $have = (-f "$ENV{'RQG_BASE'}/gentest.pl" && eval { require DBD::mysql; 1 })
  ? 1 : 0;
open(my $fh, '>', "$ENV{'MYSQLTEST_VARDIR'}/tmp/have_rqg.inc") or die;
print $fh "let \$have_rqg = $have;\n";
close($fh);
EOF
--source $MYSQLTEST_VARDIR/tmp/have_rqg.inc
--remove_file $MYSQLTEST_VARDIR/tmp/have_rqg.inc
if (!$have_rqg) {
  --skip Test requires RQG ($RQG_BASE) and Perl DBD::mysql
}
