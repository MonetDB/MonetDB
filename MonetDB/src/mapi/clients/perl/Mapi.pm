# The contents of this file are subject to the MonetDB Public
# License Version 1.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of
# the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
# 
# Software distributed under the License is distributed on an "AS
# IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
# implied. See the License for the specific language governing
# rights and limitations under the License.
# 
# The Original Code is the Monet Database System.
# 
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2004 CWI.
# All Rights Reserved.
# 
# Contributor(s):
# 		Martin Kersten <Martin.Kersten@cwi.nl>
# 		Peter Boncz <Peter.Boncz@cwi.nl>
# 		Niels Nes <Niels.Nes@cwi.nl>
# 		Stefan Manegold  <Stefan.Manegold@cwi.nl>

package Mapi;

use strict;
use IO::Socket;
use IO::Handle;

sub new {
	my($mapi,$server,$user,$passwd,$language) = @_;
	my $mapi = {};

	$mapi->{trace} = 0;
	if ($mapi->{trace}) {
	  print "new:$server,$user,$passwd,$language\n";
	}
	$mapi->{SERVER} = $server;
	$mapi->{USER} = $user;
	$mapi->{LANG} = $language;
	#print "new mapi:$server\n";
	$mapi->{SOCKET} = new IO::Socket::INET( $server )
	|| die "!ERROR can't connect to $server : $!";
	# binmode($mapi->{SOCKET},":utf8");
	if( $mapi->{trace}){
		print "Connection to socket established\n";
	}
	bless($mapi,"Mapi");
	$mapi->doCmd("$user:$passwd\n");
	if ($mapi->{trace}) {
	  print "logged on:$user:$passwd\n";
	}
# 	print "switch to $language scenario\n";
# 	if( $language eq 'sql'){
# 		$mapi->doCmd("sql();");
# 	}
	return $mapi;
}

# How to create a duplicate
sub clone {
	my ($mapi,$src)= @_;
	bless($mapi,"Mapi");
	if ($mapi->{trace}) {
	  print "cloning\n";
	}
	$mapi->{SERVER} = $src->{SERVER};
	$mapi->{USER} = $src->{USER};
	$mapi->{SOCKET} = $src->{SOCKET};
	$mapi->resetState();
}

sub mapiport_intern {
  my $mapiport = 'localhost:50001';
  $mapiport = $ENV{'MAPIPORT'} if defined($ENV{'MAPIPORT'});
  return $mapiport;
}

sub hostname {
  my ( $hostname ) = mapiport_intern() =~ /([^:]*)/;
  $hostname = 'localhost' if ( $hostname eq '' );
  return $hostname;
}

sub portnr {
  my ( $portnr ) = mapiport_intern() =~ /:([^:]*)/;
  $portnr = 50001 if ( $portnr eq '' );
  return $portnr;
}

sub disconnect {
	my ($mapi) = @_;
	if ($mapi->{trace}) {
	  print "disconnect\n";
	}
	$mapi->wrapup();
# 	$mapi->doRequest("quit();\n");
	$mapi->{SOCKET}->close;
	if( $mapi->{trace} ) { print "Disconnected from server\n";}
}

sub showState {
	my ($mapi) = @_;
	if( $mapi->{trace} ) {
	  print "mapi.error :".$mapi->{error}."\n";
	  print "mapi.errstr:".$mapi->{errstr}."\n";
	  print "mapi.active:".$mapi->{active}."\n";
	  print "mapi.BUF[".length($mapi->{BUF})."]:".$mapi->{BUF}."\n";
	}
}

sub resetState{
  my ($mapi) = @_;
  if ($mapi->{trace}) {
    print "resetState\n";
  }
  $mapi->{errstr}="";
  $mapi->{error}=0;
  $mapi->{active}=0;	
}

#packge the request and ship it, the back-end reads blocks!
sub doRequest {
	my($mapi,$cmd) = @_;
	if( $mapi->{trace}) {
		print "Send:$cmd\n";
	}
	#my($missing) = 256 - length($cmd) % 256;
	#my($blk) = $cmd . ' ' x $missing; 
	$cmd =~ s/\n/ /g;
	$cmd .= "\n";
	if ($mapi->{LANG} eq 'sql') {
	  $cmd = "S" . $cmd;
	}
	$mapi->{SOCKET}->send( $cmd )
		|| die "!ERROR can't send $cmd: $!";
}

# reading the next answer may require removal of the left-overs
# of the previous instruction
sub wrapup {
	my($mapi) = @_;
	if( $mapi->{trace})
	   {print "wrapup:".$mapi->{active}." buf:".$mapi->{BUF}."\n";}
	while( $mapi->{active} >0 ){
		$mapi->getReply();		
		if( $mapi->{trace}) {
		   print "change:".$mapi->{active}." buf:".$mapi->{BUF}."\n";}
	}
}

# read the remainder of the answer and consume the next prompt.
sub getFirstAnswer {
	my($self) = @_;
	my $res;
	if( $self->getReply() ){
		return $self->{row};
	}
	if( $self->{trace}){
		$self->showState();
		print "getFirstAnswer:".$res."\n";
	}
	return $res;
}

# Analyse a single line for errors
sub errorTest{
    my ($mapi) =@_;
    my $err= $mapi->{errstr};
    if( length($err) > 0) { $err= "$err\n";}
    my $row= $mapi->{row};
    #$mapi->showState();
    if( $row =~ /^!/ ){
	$mapi->{errstr} = "$err$row";
	$mapi->{row}= "";
	$mapi->{error} = 1;
	if( $mapi->{trace}) { print "Error found\n";}
	return 1;
    }
    return 0;
}

# analyse commentary lines for auxiliary information
sub propertyTest{
    my ($mapi) =@_;
    my $err= $mapi->{error};
    my $row= $mapi->{row};
    #$mapi->showState();
    if( $row =~ /^\#---/ ){
	$mapi->{row}= "";
	return 1;
    }
    if( $row =~ /^\#.*\#/ ){
	$mapi->{row}= "";
	return 1;
    }
    return 0;
}

# read a single line from the server
sub getLine {
	my ($mapi)= @_;
	my $buf= $mapi->{BUF};
	if($mapi->{trace}) {
		my $i= index($buf,"\n");
		print "getLine start $i\n";
	}
	while ( index($buf,"\n") <0 ) {
		$mapi->{SOCKET}->recv($buf,256);
#		|| die "!ERROR can't receive: $!";
		if (length($buf) == 0) {
		  if ($mapi->{trace}) {
		    print "received empty\n";
		  }
		  last;
		}
		if($mapi->{trace}) {
		  print "received:".$buf."\n";
		}
		$mapi->{BUF} = "$mapi->{BUF}$buf";
	}
	if($mapi->{trace}) {
		print "getLine[".length($mapi->{BUF})."]:".$mapi->{BUF}."\n";
	}
}
sub getReply {
	my ($mapi)= @_;
 	my $doit =1;
	if ($mapi->{trace}) {
	  print "getReply\n";
	}
	#return 0 if $mapi->active == 0;
	#if( $mapi->active == undef || $mapi->active == 0) {
		#return 0;
	#}
	while( $doit > 0 ){
		if($mapi->{trace}) {
			#my $i= $mapi->{BUF};
			#my $l= length($i);
			#print "doit leftover:$l:$i!\n";
		}
		$mapi->{BUF} =~ s/ //g;
		#if( $mapi->{BUF} eq "" ) { $mapi->getLine(); }
		{ $mapi->getLine(); }
		my $row= $mapi->{BUF};
		my $e = index($row,"\001");
		#if($e == undef){ print "002 not found\n";}
		#if($e < 0){ print "002 not found (-1)\n";}
		my $n = index($row,"\n");
		#print "e=$e n=$n \n";
		if(($e < 0) || ( $n < $e) ){
			$row = substr($mapi->{BUF},0,$n);
			if($mapi->{trace}){ print "new row:".$row."\n";}
			$n= $n+1;
			$mapi->{BUF} = substr($mapi->{BUF},$n,length($mapi->{BUF}));
		}
		if( ($e >= 0) && ($e < $n) ){
			$mapi->{BUF}= undef;
			$mapi->{row}= undef;
			$mapi->{active} = 0;
			$doit = 0;
			if($mapi->{trace}){print "getreply finished:".$row."\n";}
		} else  {
			$mapi->{active} = 1;
			$mapi->{row}= $row;
			#decode the line
			$doit=  $mapi->errorTest();
			if( $mapi->propertyTest() > 0) { $doit =1;}
			if($mapi->{trace}){print "getreply :".$row."\n";}
		}
	}
	return $mapi->{active};
}

sub doCmd {
	my($mapi,$cmd) = @_;
	my $res;
	if ($mapi->{trace}) {
	  print "doCmd: ".$cmd."\n";
	}
	$mapi->resetState();
	$mapi->{SOCKET}->send( $cmd )
		|| die "!ERROR can't send $cmd: $!";
	# ignore all answers except error messages
	while( $mapi->getReply()){
		if($mapi->{trace}){ print "getAnswer:".$res."\n";}
	}
	#$mapi->showState();
	return $mapi->{rows};
}
1;

