package Mapi;

use strict;
use IO::Socket;
use IO::Handle;

sub new {
    my($self,$server,$user) = @_;
    my $self = {};
    $self->{SERVER} = $server;
    $self->{USER} = $user;
    $self->{SOCKET} = new IO::Socket::INET( $server )
	|| die "can't connect to $server : $!";
	bless($self);
    $self->{BUF} = "";
    $self->{PROMPT} = "";
    $self->cmd_intern("$user\n");
    $self->result();
    return $self;
}

sub mapiport_intern {
  my $mapiport = 'localhost:50000';
  $mapiport = $ENV{'MAPIPORT'} if defined($ENV{'MAPIPORT'});
}

sub hostname {
  my ( $hostname ) = mapiport_intern() =~ /([^:]*)/;
  $hostname = 'localhost' if ( $hostname eq '' );
  return $hostname;
}

sub portnr {
  my ( $portnr ) = mapiport_intern() =~ /:([^:]*)/;
  $portnr = 50000 if ( $portnr eq '' );
  return $portnr;
}

sub disconnect {
    my ($self) = @_;
    $self->cmd_intern("quit;\n");
    $self->{SOCKET}->close;
}

sub cmd_intern {
    my($self,$cmd) = @_;
    $self->{SOCKET}->send( $cmd ) 
	|| die "can't send $cmd: $!";
}

sub answer_intern {
    my($self) = @_;
    my($res,$buf,$index);
    $buf = $self->{BUF};
    while( ($index = index($self->{BUF},"\001")) < 0){
        $self->{SOCKET}->recv($buf,4096);
        $self->{BUF} = "$self->{BUF}$buf";
    }
    $res = substr($self->{BUF},0,$index);
    $index += 1;
    $self->{BUF} = substr($self->{BUF},$index,length($self->{BUF}));
    return $res;
}

sub getprompt {
    my($self) = @_;
    $self->answer_intern();
}

sub result {
    my($self) = @_;
    my $res;
    $res = $self->answer_intern();
    $self->getprompt();
    return $res;
}

sub cmd {
    my($self,$cmd) = @_;
    if (! ($cmd =~ /^.*\n$/)){
	$cmd = "$cmd\n";
    }
    $self->cmd_intern($cmd);
    return $self->result();
}

1;

