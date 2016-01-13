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
