#!/usr/bin/perl

use Mapi;

my ($monet, $line);
$monet = new Mapi( 
  Mapi::hostname() . ':' . Mapi::portnr(), $ENV{'USER'} );

print "> ";
while ( !(($line=<>) =~ /quit;/) ){
	print $monet->cmd($line);
	print "> ";
}

$monet->disconnect();

1;

