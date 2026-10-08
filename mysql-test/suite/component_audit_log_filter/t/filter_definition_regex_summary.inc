# ==== Purpose ====
#
# Close the current measured interval by rotating the audit log and print
# a format independent summary of the records written in it: one line per
# distinct record identity with the number of its occurrences.
#
#   table_access <event> <db>.<table>
#   general <query>
#   query <query>
#   connection <event> <user>
#
# Internal audit start/stop records are excluded. JSON, JSONL and NEW (XML)
# formats are supported, NUL bytes in query text are shown as '?', the XML
# writer's representation. For JSON the record text is decoded as UTF-8 and
# checked byte-exactly by the caller using $rx_expect_query.
#
# ==== Usage ====
#
# [--let rx_expect_query = query text which must be present in a JSON record,
#                          given as a Perl string with escapes]
# --source filter_definition_regex_summary.inc

--source filter_definition_regex_sync.inc
--disable_query_log
--disable_result_log
--let rx_log_dir = `SELECT @@global.datadir`
--let rx_log_file = `SELECT audit_log_rotate()`
--enable_result_log
--enable_query_log

perl;
  use strict;
  use warnings;
  use Encode qw(decode FB_CROAK);

  my $path = "$ENV{rx_log_dir}/$ENV{rx_log_file}";
  open my $f, '<:raw', $path or die "Cannot open $path: $!";
  my $bytes = do { local $/; <$f> };
  close $f;
  my $text = decode('UTF-8', $bytes, FB_CROAK);
  my @records;
  my $is_json = $text =~ /^\s*[\[{]/;

  sub xml_unescape {
    my ($s) = @_;
    $s =~ s/&#x([0-9A-Fa-f]+);/chr(hex($1))/ge;
    $s =~ s/&#([0-9]+);/chr($1)/ge;
    $s =~ s/&lt;/</g;
    $s =~ s/&gt;/>/g;
    $s =~ s/&quot;/"/g;
    $s =~ s/&apos;/'/g;
    $s =~ s/&amp;/&/g;
    return $s;
  }

  my @json_queries;

  if ($is_json) {
    require JSON;
    my $json = JSON->new;
    my $list;
    if ($text =~ /^\s*\[/) {
      $list = $json->decode($text);
    } else {
      $list = [ map { $json->decode($_) } grep { /\S/ } split /\n/, $text ];
    }
    for my $r (@$list) {
      my $class = $r->{class};
      next if $class eq 'audit';
      if ($class eq 'table_access') {
        my $d = $r->{table_access_data};
        push @records, "table_access $r->{event} $d->{db}.$d->{table}";
      } elsif ($class eq 'general') {
        push @json_queries, $r->{general_data}->{query};
        push @records, "general $r->{general_data}->{query}";
      } elsif ($class eq 'query') {
        push @json_queries, $r->{query_data}->{query};
        push @records, "query $r->{query_data}->{query}";
      } elsif ($class eq 'connection') {
        push @records, "connection $r->{event} $r->{login}->{user}";
      } else {
        push @records, "$class $r->{event}";
      }
    }
  } else {
    my %table_events = (TableRead => 'read', TableInsert => 'insert',
                        TableUpdate => 'update', TableDelete => 'delete');
    my %connection_events = (Connect => 'connect', Quit => 'disconnect',
                             'Change user' => 'change_user');
    while ($text =~ m{<AUDIT_RECORD>(.*?)</AUDIT_RECORD>}sg) {
      my $body = $1;
      my %r;
      while ($body =~ m{<(\w+)>(.*?)</\1>}sg) {
        $r{$1} = xml_unescape($2);
      }
      my $name = $r{NAME} // '';
      next if $name eq 'Audit' || $name eq 'NoAudit';
      if (exists $table_events{$name}) {
        push @records, "table_access $table_events{$name} $r{DB}.$r{TABLE}";
      } elsif (exists $connection_events{$name} &&
               ($r{COMMAND_CLASS} // '') eq 'connect') {
        push @records, "connection $connection_events{$name} " . ($r{USER} // '');
      } elsif ($name =~ /^Query (Start|Status End|Nested)/) {
        push @records, "query " . ($r{SQLTEXT} // '');
      } else {
        push @records, "general " . ($r{SQLTEXT} // '');
      }
    }
  }

  if (defined $ENV{rx_expect_query} && $ENV{rx_expect_query} ne '' && $is_json) {
    my $expected = eval "\"$ENV{rx_expect_query}\"";
    die "Expected query not found byte-exactly in JSON records"
      unless grep { $_ eq $expected } @json_queries;
  }

  my %count;
  for my $r (@records) {
    $r =~ s/\x00/?/g;
    $count{$r}++;
  }
  binmode STDOUT, ':encoding(UTF-8)';
  print "# Records: " . scalar(@records) . "\n";
  print "#   $count{$_} x $_\n" for sort keys %count;
EOF
