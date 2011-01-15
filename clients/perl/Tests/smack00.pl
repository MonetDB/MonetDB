#!/usr/bin/perl
# This script implements the supersmack benchmark obtained from Mysql

use Mapi;

my ($monet, $line);
$monet = new Mapi( Mapi::hostname() . ':' . Mapi::portnr(), $ENV{'USER'}, '' );

print "Start the application\n";
my($i)=0;
for($i=0; $i<100000; $i++){
	$reply= $monet->cmd("print(1);");
	#if( $monet->error() ){
		#$monet->explain();
	#}  else { print $reply;}
	#printf $reply;
}

$monet->disconnect();

1;

