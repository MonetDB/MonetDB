#!/usr/bin/perl

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

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

