#!/usr/bin/perl
# This script implements the supersmack benchmark obtained from Mysql

use Mapi;

my ($monet, $line);
$monet = new Mapi( Mapi::hostname() . ':' . Mapi::portnr(), $ENV{'USER'}, '' );

print "Start the application\n";
while(($line= <>) !~ "quit.*"){
	print "send req";
	$reply= $monet->cmd($line);
	print "got reply";
	if( $monet->error() ){
		$monet->explain();
	} else { print $reply;}
}

$monet->disconnect();

1;

