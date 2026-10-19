# ==== Purpose ====
#
# Print the number of rotated audit logs and whether the current log exists.
# Rotated plain JSON logs are checked to be complete JSON documents.
#
# ==== Usage ====
#
# --source log_rotate_reopen_failure_files.inc

--let audit_filter_log_path = `SELECT @@global.datadir`
--let audit_filter_log_name = `SELECT @@global.audit_log_filter.file`

perl;
  use strict;
  use warnings;
  use JSON;

  my $dir_name = $ENV{'audit_filter_log_path'} or die "Missing audit_filter_log_path\n";
  my $base_file_name = $ENV{'audit_filter_log_name'} =~ s/\..*$//r;
  my $rotated = 0;
  my $current = 0;

  opendir(my $dh, $dir_name) or die "Can't opendir $dir_name: $!";
  while (my $file_name = readdir $dh) {
    next if ($file_name !~ /^\Q$base_file_name\E\./);

    if ($file_name !~ /^\Q$base_file_name\E\.\d{8}T\d{6}(-\d+)?\./) {
      $current += 1;
      next;
    }

    $rotated += 1;
    next if ($file_name =~ /\.gz|\.enc$/);

    open(my $fh, "<:encoding(UTF-8)", "$dir_name$file_name") or
        die("Can't open '$file_name': $!\n");
    local $/;
    my $json_text = <$fh>;
    close($fh);
    eval { JSON->new()->decode($json_text); 1 } or
        die("Rotated log '$file_name' is not valid JSON: $@\n");
  }
  closedir $dh;

  print "rotated logs: $rotated, current log: $current\n";
EOF
