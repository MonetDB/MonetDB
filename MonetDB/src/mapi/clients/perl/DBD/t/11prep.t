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
use DBD_TEST();

use Test::More;

if (defined $ENV{DBI_DSN}) {
  plan tests => 15;
} else {
  plan skip_all => 'Cannot test without DB info';
}

pass('Simple prepare/execute/finish tests');

my $dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
pass('Database connection created');

my $tbl = $DBD_TEST::table_name;

{
  local ($dbh->{PrintError}, $dbh->{RaiseError}, $dbh->{Warn});
  $dbh->{PrintError} = 0; $dbh->{RaiseError} = 0; $dbh->{Warn} = 0;
  $dbh->do("DROP TABLE $tbl");
}

ok( $dbh->disconnect,'Disconnect');

$dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
pass('Database connection created');
{
  local $dbh->{PrintError} = 0;
  local $dbh->{RaiseError} = 1;
  ok( !eval{ $dbh->do("DROP TABLE $tbl") },"DROP TABLE $tbl");
  print $@, "\n";
}
ok( $dbh->do("CREATE TABLE $tbl( chr char( 1 ) )"),"CREATE TABLE $tbl");

my $sth;
ok( $sth = $dbh->prepare("SELECT * FROM $tbl"),"SELECT * FROM $tbl");
ok( $sth->execute,'Execute');
ok( $sth->finish,'Finish');
ok( $sth = $dbh->prepare("SELECT * FROM $tbl"),"SELECT * FROM $tbl");
ok( $sth->finish,'Finish');
ok( $sth = $dbh->prepare("SELECT * FROM $tbl"),"SELECT * FROM $tbl");
ok( !( $sth = undef ),'Set sth to undefined');
#ok( $sth = $dbh->prepare("SELECT * FROM $tbl", { monetdb_ => ... } ),"SELECT * FROM $tbl ( monetdb_ => ... )");
#ok( $sth->execute,'Execute');
#ok( !( $sth = undef ),'Set sth to undefined');
ok( $dbh->do("DROP TABLE $tbl"),"DROP TABLE $tbl");

ok( $dbh->disconnect,'Disconnect');
