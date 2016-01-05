# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

package Mapi;

use strict;
use Socket;
use IO::Socket;
use Digest::MD5 'md5_hex';
use Digest::SHA qw(sha1_hex sha256_hex sha512_hex);

sub pass_chal {
  my ($passwd, @challenge) = @_;
  if ($challenge[2] == 9) {
    my $pwhash = $challenge[5];
    if ($pwhash eq 'SHA512') {
      $passwd = sha512_hex($passwd);
    } elsif ($pwhash eq 'SHA256') {
      $passwd = sha256_hex($passwd);
    } elsif ($pwhash eq 'SHA1') {
      $passwd = sha1_hex($passwd);
    } elsif ($pwhash eq 'MD5') {
      $passwd = md5_hex($passwd);
    } else {
      warn "unsupported password hash: ".$pwhash;
      return;
    }
  } else {
    warn "unsupported protocol version: ".$challenge[2];
    return;
  }

  my @cyphers = split(/,/, $challenge[3]);
  my $chal;
  foreach (@cyphers) {
    if ($_ eq 'SHA512') {
      $chal = "{$_}".sha512_hex($passwd.$challenge[0]);
      last;
    } elsif ($_ eq 'SHA256') {
      $chal = "{$_}".sha256_hex($passwd.$challenge[0]);
      last;
    } elsif ($_ eq 'SHA1') {
      $chal = "{$_}".sha1_hex($passwd.$challenge[0]);
      last;
    } elsif ($_ eq 'MD5') {
      $chal = "{$_}".md5_hex($passwd.$challenge[0]);
      last;
    }
  }
  if (!$chal) {
    warn "unsupported hash algorithm necessary for login: ".$challenge[3];
    return;
  }

  return $chal;
}

sub new {
  my $mapi = shift;
  my $host  = shift || 'localhost';
  my $port  = shift || 50000;
  my $user  = shift || 'monetdb';
  my $passwd  = shift || 'monetdb';
  my $lang  = shift || 'sql';
  my $db  = shift || '';
  my $trace  = shift || 0;
  my $self = {};

  bless( $self, $mapi );

  $self->{trace} = $trace;

  print "new:$host,$port,$user,$passwd,$lang,$db\n" if ($self->{trace});
  $self->{host} = $host;
  $self->{port} = $port;
  $self->{user} = $user;
  $self->{passwd} = $passwd;
  $self->{lang} = $lang;
  $self->{db} = $db;
  $self->{socket} = IO::Socket::INET->new(
        PeerAddr => $host,
        PeerPort => $port,
        Proto    => 'tcp'
  ) || die "!ERROR can't connect to $host:$port $!";
  $self->{piggyback} = [];
  $self->{skip_in} = 0;

  #binmode($self->{socket},":utf8");

  #block challenge:mserver:9:cypher(s):content_byteorder(BIG/LIT):pwhash\n");
  my $block = $self->getblock();
  my @challenge = split(/:/, $block);
  print "Connection to socket established ($block)\n" if ($self->{trace});

  my $passchal = pass_chal($passwd, @challenge) || die;

  # content_byteorder(BIG/LIT):user:{cypher_algo}mypasswordchallenge_cyphered:lang:database: 
  $self->putblock("LIT:$user:$passchal:$lang:$db:\n");
  my $prompt = $self->getblock();
  if ($prompt =~ /^\^mapi:monetdb:/) {
    # full reconnect
    $self->{socket}->close;
    print "Following redirect: $prompt\n" if ($self->{trace});
    my @tokens = split(/[\n\/:\?]+/, $prompt); # dirty, but it's Perl anyway
    return new Mapi($tokens[3], $tokens[4], $user, $passwd, $lang, $tokens[5], $trace);
  } elsif ($prompt =~ /^\^mapi:merovingian:\/\/proxy/) {
    # proxied redirect
    do {
      print "Being proxied by $host:$port\n" if ($self->{trace});
      $block = $self->getblock();
      @challenge = split(/:/, $block);
      $passchal = pass_chal($passwd, @challenge) || die;
      $self->putblock("LIT:$user:$passchal:$lang:$db:\n");
      $prompt = $self->getblock();
    } while ($prompt =~ /^\^mapi:merovingian:proxy/);
  } # TODO: don't die on warnings (#)
  die $prompt if ($prompt ne "");
  print "Logged on $user\@$db with $lang\n" if ($self->{trace});
  return $self;
}

# How to create a duplicate
sub clone {
  my ($self,$src)= @_;
  bless($self,"Mapi");
  print "cloning\n" if ($self->{trace});
  $self->{host} = $src->{host};
  $self->{port} = $src->{port};
  $self->{user} = $src->{user};
  $self->{passwd} = $src->{passwd};
  $self->{lang} = $src->{lang};
  $self->{db} = $src->{db};
  $self->{socket} = $src->{socket};
  $self->resetState();
}

sub mapiport_intern {
  my $mapiport = 'localhost:50000';
  $mapiport = $ENV{'MAPIPORT'} if defined($ENV{'MAPIPORT'});
  return $mapiport;
}

sub hostname {
  my ($hostname) = mapiport_intern() =~ /([^:]*)/;
  $hostname = 'localhost' if ($hostname eq '');
  return $hostname;
}

sub portnr {
  my ($portnr) = mapiport_intern() =~ /:([^:]*)/;
  $portnr = 50000 if ($portnr eq '');
  return $portnr;
}

sub disconnect {
  my ($self) = @_;
  print "disconnect\n" if ($self->{trace});
  $self->{socket}->close;
  print "Disconnected from server\n" if ($self->{trace});
}

sub showState {
  my ($self) = @_;
  if ($self->{trace}) {
    print "mapi.error :".$self->{error}."\n";
    print "mapi.errstr:".$self->{errstr}."\n";
    print "mapi.active:".$self->{active}."\n";
    print "mapi.row[".length($self->{row})."]:".$self->{row}."\n";
  }
}

sub resetState {
  my ($self) = @_;
  print "resetState\n" if ($self->{trace});
  $self->{errstr}="";
  $self->{error}=0;
  $self->{active}=0;  
}

#packge the request and ship it, the back-end reads blocks!
sub doRequest {
  my($self,$cmd) = @_;

  $cmd = "S" . $cmd if $self->{lang} eq 'sql';
  # even if the query ends with a ;, this never hurts and fixes the -- on last line bug
  $cmd = $cmd . "\n;\n";
  print "doRequest:$cmd\n" if ($self->{trace});
  $self->putblock($cmd); # TODO handle exceptions || die "!ERROR can't send $cmd: $!";
  $self->resetState();
}

# Analyse a single line for errors
sub error {
  my ($self,$line) = @_;
  my $err = $self->{errstr};
  $err = "$err\n" if (length($err) > 0);
  $line =~ s/^\!//;
  $self->{errstr} = $err . $line;
# $self->showState();
  $self->{row}= "";
  $self->{error} = 1;
  print "Error found $self->{error}\n" if ($self->{trace});
}

# analyse commentary lines for auxiliary information
sub propertyTest {
  my ($self) =@_;
  my $err= $self->{error};
  my $row= $self->{row};
#   $self->showState();
  if ($row =~ /^\#---/) {
    $self->{row}= "";
    return 1;
  }
  if ($row =~ /^\#.*\#/) {
    $self->{row}= "";
    return 1;
  }
  return 0;
}


sub getRow {
  my ($self)= @_;
  my $row = $self->{lines}[$self->{next}++];
  my @chars = split(//, $row,3);

  if ($chars[0] eq '!') { 
    $self->error($row);
    my $i = 1;
    while ($self->{lines}[$i] =~ '!') {
      $self->error($self->{lines}[$i]);
      $i++;
    }
    $self->{active} = 0;
    return -1
  } elsif ($chars[0] eq '&') {
    # not expected
  } elsif ($chars[0] eq '%') {
    # header line
  } elsif ($chars[0] eq '[') {
    # row result
    $self->{row} = $row;
    if ($self->{nrcols} < 0) {
      $self->{nrcols} = () = $row =~ /,\t/g;
      $self->{nrcols}++;
    }
    $self->{active} = 1;
  } elsif ($chars[0] eq '=') {
    # xml result line
    $self->{row} = substr($row, 1); # skip = 
    $self->{active} = 1;
  } elsif ($chars[0] eq '^') {
    # ^ redirect, ie use different server
  } elsif ($chars[0] eq '#') {
    # warnings etc, skip, and return what follows
    return $self->getRow;
  }
  return $self->{active};
}

sub getBlock {
  my ($self)= @_;
  print "getBlock $self->{active}\n" if ($self->{trace});
  my $block = $self->getblock();
  @{$self->{lines}} = split(/\n/, $block);

  # skip diagnostic messages before the header
  shift @{$self->{lines}} while @{$self->{lines}} && $self->{lines}[0] =~ /\A#/;

  die "implausible return from MonetDB: $self->{lines}[0]\n" if $self->{lines}[0] =~ /\A[^ -~]/;

  my $header = $self->{lines}[0];
  my @chars = split(//, $header);

  $self->{id} = -1;
  $self->{nrcols} = -1;
  $self->{replysize} = scalar(@{$self->{lines}});
  $self->{active} = 0;
  $self->{skip} = 0; # next+skip is current result row
  $self->{next} = 0; # all done
  $self->{offset} = 0;
  $self->{hdrs} = [];

  if ($chars[0] eq '&') {
    if ($chars[1] eq '1' || $chars[1] eq 6) {
      if ($chars[1] eq '1') {
        # &1 id result-count nr-cols rows-in-this-block
        my ($dummy,$id,$cnt,$nrcols,$replysize) = split(' ', $header);
        $self->{id} = $id;
        $self->{count} = $cnt;
        $self->{nrcols} = $nrcols;
        $self->{replysize} = $replysize;
      } else {
        # &6 id nr-cols,rows-in-this-block,offset
        my ($dummy,$id,$nrcols,$replysize,$offset) = split(' ', $header);
        $self->{id} = $id;
        $self->{nrcols} = $nrcols;
        $self->{replysize} = $replysize;
        $self->{offset} = $offset;
      }
      # for now skip table header information
      my $i = 1;
      while ($self->{lines}[$i] =~ /\A%/) {
        $self->{hdrs}[$i - 1] = $self->{lines}[$i];
        $i++;
      }
      $self->{skip} = $i;
      $self->{next} = $i;
      $self->{row} = $self->{lines}[$self->{next}++];

      $self->{active} = 1;
    } elsif ($chars[1] eq '2') { # updates
      my ($dummy,$cnt) = split(' ', $header);
      $self->{count} = $cnt;
      $self->{nrcols} = 1;
      $self->{replysize} = 1;
      $self->{row} = "" . $cnt;
      $self->{next} = $cnt; # all done
      return -2;
    } elsif ($chars[1] eq '3') { # transaction 
      # nothing todo
    } elsif ($chars[1] eq '4') { # auto_commit 
      my ($dummy,$ac) = split(' ', $header);
      if ($ac eq 't') {
        $self->{auto_commit} = 1;
      } else {
        $self->{auto_commit} = 0;
      }
    } elsif ($chars[1] eq '5') { # prepare 
      my ($dummy,$id,$cnt,$nrcols,$replysize) = split(' ', $header);
      # TODO parse result, rows (type, digits, scale)
      $self->{count} = $cnt;
      $self->{nrcols} = $nrcols;
      $self->{replysize} = $replysize;
      $self->{row} = "";
      $self->{next} = $cnt; # all done
    }
  } else {
    return $self->getRow();
  } 
  return $self->{active};
}

sub getReply {
  my ($self)= @_;

  if ($self->{active} == 0) {
    return $self->getBlock();
  } elsif ($self->{next} < $self->{replysize} + $self->{skip}) {
    return $self->getRow();
  } elsif (${self}->{offset} + $self->{replysize} < $self->{count}) {
    # get next slice
    my $rs = $self->{replysize};
    my $offset = $self->{offset} + $rs;
    $self->putblock("Xexport $self->{id} $offset $rs");
    return $self->getBlock();
  } else {
    # close large results, but only send on next query
    if ($self->{id} > 0 && $self->{count} != $self->{replysize}) {
      push @{$self->{piggyback}}, "Xclose $self->{id}";
      $self->{skip_in}++;
    }
    $self->{active} = 0;
  } 
  return $self->{active};

}

sub readFromSocket {
  my ($self, $ref, $count) = @_;

  die "invalid buffer reference" unless (ref($ref) eq 'SCALAR');

  my $rcount = 0;
  $$ref ||= "";

  while ($count > 0) {
    $rcount = $self->{socket}->sysread($$ref, $count, length($$ref));

    die "read error: $!" unless (defined($rcount));
    die "no more data on socket" if ($rcount == 0);

    $count -= $rcount;
  }
}

sub getblock {
  my ($self) = @_;

  # now read back the same way
  my $result = "";
  my $last_block = 0;
  do {
    my $flag;

    $self->readFromSocket(\$flag, 2); # read block info

    my $unpacked = unpack( 'v', $flag );  # unpack (little endian short)
    my $len = ( $unpacked >> 1 );    # get length
    $last_block = $unpacked & 1;    # get last-block-flag

    print "getblock: $last_block $len\n" if ($self->{trace});
    if ($len > 0 ) {
      my $data;
      $self->readFromSocket(\$data, $len); # read
      $result .= $data;
      print "getblock: $data\n" if ($self->{trace});
    }
  } while ( !$last_block );
  print "IN:\n$result\n" if $ENV{MAPI_TRACE};

  if ($self->{skip_in}) {
    $self->{skip_in}--;
    goto &getblock;
  }

  return $result;
}

sub putblock {
  my $self = shift;

  # there maybe something in the piggyback buffer
  my @blocks = (\(@{ $self->{piggyback} }), \(@_));
  @{ $self->{piggyback} } = ();

  # create blocks of data with max 0xffff length,
  # then loop over the data and send it.
  my $out = '';
  for my $blk (@blocks) {
    print "OUT:\n$$blk\n" if $ENV{MAPI_TRACE};
    utf8::downgrade($$blk); # deny wide chars
    my $pos        = 0;
    my $last_block = 0;
    my $blocksize  = 0x7fff >> 1;       # max len per block
    my $data;

    while ( !$last_block ) {
      my $data = substr($$blk, 0, $blocksize, "");
      my $len = length($data);
      # set last-block-flag
      $last_block = 1 if !length $$blk;
      my $flag = pack( 'v', ( $len << 1 ) + $last_block );
      print "putblock: $last_block ".$data."\n" if ($self->{trace});
      $out .= $flag . $data;
    }
  }
  $self->{socket}->syswrite($out); #send it
}

1;

# vim: set ts=2 sw=2 expandtab:
