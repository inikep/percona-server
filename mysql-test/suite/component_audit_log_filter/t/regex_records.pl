# Copyright (c) 2026 Percona LLC and/or its affiliates. GPLv2.
use strict;
use warnings;
use Encode qw(decode FB_CROAK);
use JSON;
sub regex_records {
  open my $f, '<:raw', "$ENV{rx_dir}/$ENV{rx_closed}" or die $!;
  my $bytes = do { local $/; <$f> };
  my @records;
  if ($ENV{rx_format} eq 'JSON') {
    for my $r (@{JSON->new->decode(decode('UTF-8', $bytes, FB_CROAK))}) {
      next if $r->{class} eq 'audit';
      my $d = $r->{$r->{class} . '_data'} || {};
      push @records, { class => $r->{class}, event => $r->{event}, query => $d->{query},
        table => $d->{table}, db => $d->{db}, user => $r->{account}->{user},
        connection => $r->{connection_id}, status => $d->{status} };
    }
  } else {
    # NEW XML has no class member. The ordinary NAME and field names identify
    # the events in these fixtures. Do not rely on debug-only metadata.
    require XML::Parser;
    my ($r, $key);
    my $parser = XML::Parser->new(Handlers => {
      Start => sub { my ($p,$name)=@_; if ($name eq 'AUDIT_RECORD') { $r={}; } $key=$name; },
      Char => sub { my ($p,$text)=@_; $r->{$key}.=$text if $r && defined $key; },
      End => sub { my ($p,$name)=@_; if ($name eq 'AUDIT_RECORD') {
        my $event=lc($r->{NAME} || '');
        $event =~ s/^table(?=insert|update|delete|read)//;
        if ($event ne 'audit' && $event ne 'noaudit') {
          push @records, {event=>$event, query=>$r->{SQLTEXT}, table=>$r->{TABLE},
            db=>$r->{DB}, user=>$r->{USER}, connection=>$r->{CONNECTION_ID}, status=>$r->{STATUS}};
        }
        undef $r;
      } undef $key; }
    });
    $parser->parse($bytes);
  }
  return \@records;
}
1;
