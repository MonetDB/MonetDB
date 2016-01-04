#!/usr/bin/env perl

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

use Mapi;

my $db = shift || '';
my $port = shift || 50000;

my ($monet, $line);
$monet = new Mapi('localhost', $port, 'monetdb', 'monetdb', 'sql', $db, 0);

print "> ";
while ( !(($line=<>) =~ /\q/) ){
	my $res = 0;
	$monet->doRequest($line);
	while( ($res = $monet->getReply()) > 0 )  {
		print $monet->{row} . "\n";
	}
	if ($res < 0) {
		if ($res == -1) {
			print $monet->{errstr};
		} elsif ($res == -2) {
			print "$monet->{count} rows affected\n";
		}
	}
	print "> ";
}

$monet->disconnect();

1;

