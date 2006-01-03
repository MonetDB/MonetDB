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
