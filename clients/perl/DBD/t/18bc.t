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

if (defined $ENV{DBI_DSN}) {
  plan tests => 8;
} else {
  plan skip_all => 'Cannot test without DB info';
}

pass('Bind column tests');

my $dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
pass('Database connection created');

my $tbl = $DBD_TEST::table_name;

ok( DBD_TEST::tab_create( $dbh ),"CREATE TABLE $tbl");

$dbh->{AutoCommit} = 0;

$dbh->do("INSERT INTO $tbl( A, B ) VALUES( 10,'Stuff here')");

$dbh->commit;

my $sth;

$sth = $dbh->prepare("DELETE FROM $tbl");
$sth->execute;
my $s = $sth->rows;
my $t = $DBI::rows;
is( $s, $t,"sth->rows: $s DBI::rows: $t");

$dbh->rollback;

$sth = $dbh->prepare("SELECT * FROM $tbl WHERE 1 = 0");
$sth->execute;
my @row = $sth->fetchrow;
if ( $sth->err ) {
  print ' $sth->err   : ', $sth->err   , "\n";
  print ' $sth->errstr: ', $sth->errstr, "\n";
  print ' $dbh->state : ', $dbh->state , "\n";
# print ' $sth->state : ', $sth->state , "\n";
}
pass("Fetched empty result set: (@row)");

$sth = $dbh->prepare("SELECT A, B FROM $tbl");
$sth->execute;
while ( my $row = $sth->fetch ) {
  print '# @row     a, b : ', $row->[0], ',', $row->[1], "\n";
}

my $Ok;

$Ok = 1;
my ( $a, $b );
$sth->execute;
$sth->bind_col( 1, \$a );
$sth->bind_col( 2, \$b );
while ( $sth->fetch ) {
  print '# bind_col a, b : ', $a, ',', $b, "\n";
  unless ( defined $a && defined $b ) {
    $Ok = 0;
    $sth->finish;
    last;
  }
}
is( $Ok, 1,'All fields defined');

$Ok = 1;
( $a, $b ) = ( undef, undef );
$sth->execute;
$sth->bind_columns( undef, \$b, \$a );
while ( $sth->fetch )
{
  print '# bind_columns a, b : ', $b, ',', $a, "\n";
  unless ( defined $a && defined $b ) {
    $Ok = 0;
    $sth->finish;
    last;
  }
}
is( $Ok, 1,'All fields defined');

ok( $dbh->disconnect,'Disconnect');
