#!perl -I./t

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2006 CWI.
# All Rights Reserved.

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
