#!/usr/bin/perl

# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024, 2025 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

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

