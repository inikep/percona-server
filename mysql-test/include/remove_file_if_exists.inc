--perl
use strict;
my $file= $ENV{'_file'} or die "File is not set";
unlink($file) if (-e $file);
EOF
