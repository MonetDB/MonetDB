#!/usr/bin/perl

use Mapi;

my ($monet, $line);

$monet = new Mapi( "localhost:50000",  $ENV{'USER'});
#$monet = new Mapi( $ENV{'MAPIPORT'},  $ENV{'USER'});

print ">";
while ( !(($line=<>) =~ /quit;/) ){
	print $monet->cmd($line);
	print ">";
}

$monet->disconnect();

1;

