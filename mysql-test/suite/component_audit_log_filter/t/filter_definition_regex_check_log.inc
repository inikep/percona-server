# Count records in a closed audit log.
#
# $rx_log     Closed log file name, relative to the data directory.
# $rx_spec_file  Expectations, one per line, fields separated by '|':
#                table|db|table|event|count   event is insert/update/delete/read
#                sql|count|substring
#                sql_exact|count|full query text
#                connect|user|count
#                disconnect|user|connection_id|count
#                sql_nul|count|prefix|suffix

--let rx_datadir = `SELECT @@global.datadir`
--let rx_format = `SELECT @@global.audit_log_filter.format`
perl;
  use strict;
  use warnings;
  use Encode qw(decode_utf8);
  my $path = $ENV{rx_datadir} . '/' . $ENV{rx_log};
  open my $fh, '<:raw', $path or die "cannot open $path: $!";
  my $bytes = do { local $/; <$fh> };
  close $fh;
  my $format = $ENV{rx_format};
  my @records;
  if ($format eq 'JSON') {
    require JSON;
    my $decoded = JSON->new->utf8->decode($bytes);
    @records = @$decoded;
  } elsif ($format eq 'NEW') {
    while ($bytes =~ m{<AUDIT_RECORD>(.*?)</AUDIT_RECORD>}sg) {
      my $body = $1;
      my %rec;
      $rec{name} = $1 if $body =~ m{<NAME>([^<]*)</NAME>};
      $rec{db} = $1 if $body =~ m{<DB>([^<]*)</DB>};
      $rec{table} = $1 if $body =~ m{<TABLE>([^<]*)</TABLE>};
      $rec{user} = $1 if $body =~ m{<USER>([^<]*)</USER>};
      $rec{connection_id} = $1 if $body =~ m{<CONNECTION_ID>([^<]*)</CONNECTION_ID>};
      if ($body =~ m{<SQLTEXT>(.*?)</SQLTEXT>}s) {
        my $sql = $1;
        $sql =~ s/&quot;/"/g;
        $sql =~ s/&apos;/'/g;
        $sql =~ s/&lt;/</g;
        $sql =~ s/&gt;/>/g;
        $sql =~ s/&amp;/&/g;
        $rec{sql} = decode_utf8($sql);
      }
      for my $field (qw(name db table user)) {
        $rec{$field} = decode_utf8($rec{$field}) if defined $rec{$field};
      }
      push @records, \%rec;
    }
  } else {
    die "unsupported audit log format $format";
  }

  sub json_sql {
    my ($rec) = @_;
    return '' unless ref $rec eq 'HASH';
    for my $key (qw(general_data table_access_data query_data connection_data)) {
      next unless ref $rec->{$key} eq 'HASH';
      return $rec->{$key}{query} if exists $rec->{$key}{query};
    }
    return '';
  }

  sub canon_event {
    my ($name) = @_;
    return 'insert' if $name eq 'TableInsert' || $name eq 'insert';
    return 'update' if $name eq 'TableUpdate' || $name eq 'update';
    return 'delete' if $name eq 'TableDelete' || $name eq 'delete';
    return 'read' if $name eq 'TableRead' || $name eq 'read';
    return 'connect' if $name eq 'Connect' || $name eq 'connect';
    return 'disconnect' if $name eq 'Quit' || $name eq 'disconnect';
    return 'status' if $name eq 'Status' || $name eq 'status';
    return 'status_end' if $name eq 'Query Status End' || $name eq 'query_status_end';
    return $name;
  }

  my %table;
  my %sql;
  my %connect;
  my %disconnect;
  for my $rec (@records) {
    if ($format eq 'JSON') {
      my $class = $rec->{class} // '';
      my $event = $rec->{event} // '';
      if ($class eq 'table_access') {
        my $data = $rec->{table_access_data} // {};
        $table{($data->{db} // '') . "\t" . ($data->{table} // '') . "\t" . canon_event($event)}++;
      } elsif ($class eq 'connection') {
        my $user = $rec->{account}{user} // '';
        my $id = $rec->{connection_id} // '';
        my $canon = canon_event($event);
        if ($canon eq 'connect') {
          $connect{$user}++;
        } elsif ($canon eq 'disconnect') {
          $disconnect{"$user\t$id"}++;
        }
      }
      my $sql = json_sql($rec);
      $sql{$sql}++ if length $sql;
    } else {
      my $name = $rec->{name} // '';
      my $canon = canon_event($name);
      if (defined $rec->{table}) {
        $table{($rec->{db} // '') . "\t" . $rec->{table} . "\t" . $canon}++;
      }
      if ($canon eq 'connect') {
        $connect{$rec->{user} // ''}++;
      } elsif ($canon eq 'disconnect') {
        $disconnect{($rec->{user} // '') . "\t" . ($rec->{connection_id} // '')}++;
      }
      $sql{$rec->{sql}}++ if defined $rec->{sql} && length $rec->{sql};
    }
  }

  open my $spec, '<:encoding(UTF-8)', $ENV{rx_spec_file} or die "cannot open spec $ENV{rx_spec_file}: $!";
  my $failed = 0;
  while (my $line = <$spec>) {
    chomp $line;
    next if $line =~ /^\s*$/ || $line =~ /^\s*#/;
    my @part = split /\|/, $line, -1;
    if ($part[0] eq 'table') {
      my $key = "$part[1]\t$part[2]\t$part[3]";
      my $got = $table{$key} // 0;
      if ($got != $part[4]) {
        warn "table $part[1].$part[2] $part[3]: got $got expected $part[4]\n";
        $failed = 1;
      }
    } elsif ($part[0] eq 'sql') {
      my $needle = $part[2];
      my $got = 0;
      for my $text (keys %sql) {
        $got += $sql{$text} if index($text, $needle) >= 0;
      }
      if ($got != $part[1]) {
        warn "sql '$needle': got $got expected $part[1]\n";
        $failed = 1;
      }
    } elsif ($part[0] eq 'sql_exact') {
      my $needle = $part[2];
      my $got = $sql{$needle} // 0;
      if ($got != $part[1]) {
        warn "sql_exact '$needle': got $got expected $part[1]\n";
        $failed = 1;
      }
    } elsif ($part[0] eq 'sql_nul') {
      my ($prefix, $suffix) = ($part[2], $part[3]);
      my $got = 0;
      for my $text (keys %sql) {
        my $nul = index($text, $prefix . "\x00" . $suffix) >= 0;
        my $xml = index($text, $prefix . '?' . $suffix) >= 0;
        $got += $sql{$text} if $nul || $xml;
      }
      if ($got != $part[1]) {
        warn "sql_nul '$prefix\\0$suffix': got $got expected $part[1]\n";
        $failed = 1;
      }
    } elsif ($part[0] eq 'connect') {
      my $got = $connect{$part[1]} // 0;
      if ($got != $part[2]) {
        warn "connect $part[1]: got $got expected $part[2]\n";
        $failed = 1;
      }
    } elsif ($part[0] eq 'disconnect') {
      my $key = "$part[1]\t$part[2]";
      my $got = $disconnect{$key} // 0;
      if ($got != $part[3]) {
        warn "disconnect $part[1] id $part[2]: got $got expected $part[3]\n";
        $failed = 1;
      }
    } else {
      die "unknown spec $line";
    }
  }
  close $spec;
  if ($failed) {
    warn "audit log $path format $format records " . scalar(@records) . "\n";
    if (@records) {
      use Data::Dumper;
      warn Dumper($records[0]);
    } else {
      warn substr($bytes, 0, 400), "\n";
    }
    for my $key (sort keys %table) {
      warn "  table $key => $table{$key}\n";
    }
    for my $text (sort keys %sql) {
      warn "  sql [$sql{$text}] $text\n";
    }
    for my $user (sort keys %connect) {
      warn "  connect $user => $connect{$user}\n";
    }
    for my $key (sort keys %disconnect) {
      warn "  disconnect $key => $disconnect{$key}\n";
    }
    die "audit log assertions failed for $path\n";
  }
EOF
