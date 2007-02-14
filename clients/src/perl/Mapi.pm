# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2007 CWI.
# All Rights Reserved.

package Mapi;

use strict;
use Socket;
use IO::Socket;
#use IO::Handle;

sub putline {
  my($mapi,$cmd) = @_;

  print "putline: $cmd\n" if ($mapi->{trace});
  $mapi->{socket}->write($cmd) || die "!ERROR can't send $cmd: $!";
  # ignore all answers except error messages
  my $block = $mapi->getblock();
  print "putline getAnswer:".$block."\n" if ($mapi->{trace});
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

  #binmode($self->{socket},":utf8");

  #my $block = $self->getblock();
  #block len:challenge:mserver:7:cypher(s):content_byteorder(BIG/LIT)\n");
  my $block = $self->{socket}->getline();	# still in line mode
  my @challenge = split(/:/, $block);
  print "Connection to socket established ($block)\n" if ($self->{trace});

  # content_byteorder(BIG/LIT):user:{cypher_algo}mypasswordchallenge_cyphered:lang:database: 
  $self->putline("LIT:$user:{plain}$passwd" . @challenge[1] . ":$lang:$db:\n");
  print "logged on:$user:{plain}$passwd" . @challenge[1] . ":$lang:$db:\n" if ($self->{trace});
  return $self;
}

# How to create a duplicate
sub clone {
  my ($mapi,$src)= @_;
  bless($mapi,"Mapi");
  print "cloning\n" if ($mapi->{trace});
  $mapi->{host} = $src->{host};
  $mapi->{port} = $src->{port};
  $mapi->{user} = $src->{user};
  $mapi->{passwd} = $src->{passwd};
  $mapi->{lang} = $src->{lang};
  $mapi->{db} = $src->{db};
  $mapi->{socket} = $src->{socket};
  $mapi->resetState();
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
  my ($mapi) = @_;
  print "disconnect\n" if ($mapi->{trace});
  $mapi->{socket}->close;
  print "Disconnected from server\n" if ($mapi->{trace});
}

sub showState {
  my ($mapi) = @_;
  if ($mapi->{trace}) {
    print "mapi.error :".$mapi->{error}."\n";
    print "mapi.errstr:".$mapi->{errstr}."\n";
    print "mapi.active:".$mapi->{active}."\n";
    print "mapi.BUF[".length($mapi->{BUF})."]:".$mapi->{BUF}."\n";
  }
}

sub resetState {
  my ($mapi) = @_;
  print "resetState\n" if ($mapi->{trace});
  $mapi->{errstr}="";
  $mapi->{error}=0;
  $mapi->{active}=0;	
}

#packge the request and ship it, the back-end reads blocks!
sub doRequest {
  my($mapi,$cmd) = @_;

  $cmd =~ s/\n/ /g;		# remove newlines ???
  $cmd = "S" . $cmd if ($mapi->{lang} eq 'sql' || $mapi->{lang} eq 'xquery');
  print "doRequest:$cmd\n" if ($mapi->{trace});
  $mapi->putblock($cmd); # TODO handle exceptions || die "!ERROR can't send $cmd: $!";
  $mapi->resetState();
}

# Analyse a single line for errors
sub errorTest {
  my ($mapi) =@_;
  my $err= $mapi->{errstr};
  $err= "$err\n" if (length($err) > 0);
  my $row= $mapi->{row};
#   $mapi->showState();
  if ($row =~ /^!/) {
    $mapi->{errstr} = "$err$row";
    $mapi->{row}= "";
    $mapi->{error} = 1;
    print "Error found\n" if ($mapi->{trace});
    return 1;
  }
  return 0;
}

# analyse commentary lines for auxiliary information
sub propertyTest {
  my ($mapi) =@_;
  my $err= $mapi->{error};
  my $row= $mapi->{row};
#   $mapi->showState();
  if ($row =~ /^\#---/) {
    $mapi->{row}= "";
    return 1;
  }
  if ($row =~ /^\#.*\#/) {
    $mapi->{row}= "";
    return 1;
  }
  return 0;
}


sub getSQL {
  my ($mapi)= @_;
  print "getSQL $mapi->{active}\n" if ($mapi->{trace});
  my $block = $mapi->getblock();
  @{$mapi->{lines}} = split(/\n/, $block);
  my $header = $mapi->{lines}[0];
  if ($header =~ '&1') {
	# &1 id result-count nr-cols rows-in-this-block
	my ($dummy,$id,$cnt,$nrcols,$replysize) = split(' ', $header);
	$mapi->{id} = $id;
	$mapi->{count} = $cnt;
	$mapi->{nrcols} = $nrcols;
	$mapi->{replysize} = $replysize;

	# for now skip table header information
	my $i = 1;
  	while ($mapi->{lines}[$i] =~ '%') {
		$i++;
	}
	$mapi->{next} = $i;
	$mapi->{row} = $mapi->{lines}[$mapi->{next}++];
  	$mapi->{active} = 1;
  } 
# todo all the other result types
# handle errors
  return $mapi->{active};
}

sub getXQUERY {
  my ($mapi)= @_;
  print "getXQUERY $mapi->{active}\n" if ($mapi->{trace});
  my $block = $mapi->getblock();
  @{$mapi->{lines}} = split(/\n/, $block);
  $mapi->{count} = scalar(@{$mapi->{lines}}); 
  $mapi->{nrcols} = 1;
  $mapi->{replysize} = $mapi->{count};

  $mapi->{next} = 0;
  $mapi->{row} = substr($mapi->{lines}[$mapi->{next}++], 1); # skip = 
  $mapi->{active} = 1;
# handle errors
  return $mapi->{active};
}

sub getReply {
  	my ($mapi)= @_;

	if ($mapi->{active} == 0) {
		if ($mapi->{lang} eq 'sql') {
			$mapi->getSQL();
		} elsif ($mapi->{lang} eq 'xquery') {
			$mapi->getXQUERY();
		}
		# todo mal / mil 
	} elsif ($mapi->{next} < $mapi->{replysize}) {
		$mapi->{row} = $mapi->{lines}[$mapi->{next}++];
	} else {
		# todo check if we are on the end, if not ask for more rows
		$mapi->{active} = 0;
	} 
	return $mapi->{active};

}

sub getblock {
    my ($self) = @_;

    # now read back the same way
    my $result;
    my $last_block = 0;
    do {
        my $flag;

        $self->{socket}->sysread( $flag, 2 );	# read block info

        my $unpacked = unpack( 'v', $flag );	# unpack (little endian short)
        my $len = ( $unpacked >> 1 );		# get length
        $last_block = $unpacked & 1;		# get last-block-flag

  	print "getblock: $last_block $len\n" if ($self->{trace});
	if ($len > 0 ) {
        	my $data;
        	$self->{socket}->sysread( $data, $len );# read
        	$result .= $data;
  		print "getblock: $data\n" if ($self->{trace});
	}
    } while ( !$last_block );
    return $result;
}

sub putblock {
    my ($self,$blk) = @_;

    my $pos        = 0;
    my $last_block = 0;
    my $blocksize  = 0xffff >> 1;       # max len per block
    my $data;

    # create blocks of data with max 0xffff length,
    # then loop over the data and send it.
    while ( $data = substr( $blk, $pos, $blocksize ) ) {
	my $len = length($data);
	# set last-block-flag
        $last_block = 1 if ( $len < $blocksize );    
	my $flag = pack( 'v', ( $len << 1 ) + $last_block );
  	print "putblock: $last_block ".$data."\n" if ($self->{trace});
        $self->{socket}->syswrite($flag);	# len<<1 + last-block-flag
        $self->{socket}->syswrite($data);	# send it
        $pos += $len;		# next block
    }
}

1;
