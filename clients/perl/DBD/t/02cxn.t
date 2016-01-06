#!perl -I./t

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

$| = 1;

use strict;
use warnings;
use DBI();

use Test::More;

if (defined $ENV{DBI_DSN}) {
  plan tests => 8;
} else {
  plan skip_all => 'Cannot test without DB info';
}

pass('Connection tests');

my $dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
pass('Database connection created');

SKIP: {
  skip('DBD::monetdb specific test', 1 ) if $dbh->{Driver}{Name} ne 'monetdb';

  my $Cxn = $dbh->{monetdb_connection};

  ok( $Cxn,"Connection object: $Cxn");
}
ok( $dbh->ping,'Ping');

ok( $dbh->{Active},'Active');

ok( $dbh->disconnect,'Disconnect');

ok(!$dbh->ping,'Ping');

ok(!$dbh->{Active},'Active');
