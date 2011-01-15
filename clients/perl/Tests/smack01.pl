#!/usr/bin/perl
# This script implements the supersmack benchmark obtained from Mysql

use Mapi;

my ($monet, $line);
$monet = new Mapi( Mapi::hostname() . ':' . Mapi::portnr(), $ENV{'USER'}, '' );

print "Start SQL\n";
$reply= $monet->cmd("sql();");
if( $monet->error() ){
	$monet->explain();
}
my($i)=0;
for($i=0; $i<10; $i++){
	$reply= $monet->cmd("select count(*) from tables;");
	if( $monet->error() ){
		$monet->explain();
	} else { print $reply;}
}

$monet->disconnect();

1;

