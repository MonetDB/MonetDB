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
# Portions created by CWI are Copyright (C) 1997-2005 CWI.
# All Rights Reserved.

$| = 1;

use strict;
use warnings;
use DBI();
use DBD_TEST();

use Test::More;

if ( defined $ENV{DBI_DSN} ) {
  plan tests => 19;
} else {
  plan skip_all => 'Cannot test without DB info';
}

my $tbl = $DBD_TEST::table_name;

pass('Transaction / AutoCommit tests');

my $dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
   $dbh->{RaiseError} = 1;
   $dbh->{PrintError} = 0;
pass('Database connection created');

for ('rollback','commit')
{
  my $Warning;
  local $SIG{__WARN__} = sub { $Warning = $_[0]; chomp $Warning; };
  local $dbh->{Warn} = 1;
  $dbh->$_;
  like( $Warning, qr/ineffective/, "Warning expected: $Warning");
}
ok( DBD_TEST::tab_create( $dbh ),"CREATE TABLE $tbl");

$dbh->{AutoCommit} = 1;
ok( $dbh->{AutoCommit}, "AutoCommit ON: $dbh->{AutoCommit}");

is( commitTest( $dbh ), 1,'Commit Test, AutoCommit ON');

$dbh->{AutoCommit} = 0;
ok( !$dbh->{AutoCommit}, "AutoCommit OFF: $dbh->{AutoCommit}");

is( commitTest( $dbh ), 0,'Commit Test, AutoCommit OFF');

$dbh->{AutoCommit} = 1;
ok( $dbh->{AutoCommit}, "AutoCommit ON: $dbh->{AutoCommit}");

is( commitTest( $dbh ), 1,'Commit Test, AutoCommit ON');

ok( $dbh->begin_work  ,'begin_work');
ok( $dbh->{BegunWork} ,'BegunWork ON');
ok(!$dbh->{AutoCommit},'AutoCommit OFF');
ok( $dbh->rollback    ,'rollback');
ok(!$dbh->{BegunWork} ,'BegunWork OFF');
ok( $dbh->{AutoCommit},'AutoCommit ON');

ok( $dbh->do("DROP TABLE $tbl"),"DROP TABLE $tbl");

ok( $dbh->disconnect,'Disconnect');

# -----------------------------------------------------------------------------
# Returns true when a row remains inserted after a rollback.
# This means that AutoCommit is ON.
# -----------------------------------------------------------------------------
sub commitTest {
  my $dbh = shift;

  $dbh->do("DELETE FROM $tbl WHERE A = 100") or return undef;
  {
    local $SIG{__WARN__} = sub {}; # suppress the "commit ineffective" warning
    local $dbh->{RaiseError} = 0;
    $dbh->commit;
  }
  $dbh->do("INSERT INTO $tbl( A, B ) VALUES( 100,'T100')");
  {
    local $SIG{__WARN__} = sub {}; # suppress the "rollback ineffective" warning
    local $dbh->{RaiseError} = 0;
    $dbh->rollback;
  }
  my $sth = $dbh->prepare("SELECT A, B FROM $tbl WHERE A = 100");
  $sth->execute;
  my $rc = 0;
  while ( my $row = $sth->fetch ) {
    print "-- @$row\n";
    $rc = 1;
  }
  $rc;
}
