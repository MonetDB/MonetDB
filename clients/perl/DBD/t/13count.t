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
  plan tests => 30;
} else {
  plan skip_all => 'Cannot test without DB info';
}

pass('Row count tests');

my $dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
   $dbh->{RaiseError} = 1;
   $dbh->{PrintError} = 0;
pass('Database connection created');

my $tbl = $DBD_TEST::table_name;
my $cnt = 7;

ok( DBD_TEST::tab_create( $dbh ),"CREATE TABLE $tbl");

is( $dbh->do("INSERT INTO $tbl( A, B ) VALUES( $_,'T$_')"), 1,"($_) Insert")
  for 1..$cnt;

my $sth = $dbh->prepare("SELECT * FROM $tbl");
is( $sth->rows, -1,'Rows (prepare) :'. $sth->rows );

for ( 1..2 ) {  # (re)execute
  $sth->execute;
# is( $sth->rows, 0,'Rows (execute) : '. $sth->rows );
  my $i = 0;
  while ( my $row = $sth->fetch ) {
    is( $sth->rows, ++$i,"($_) Rows so far: $i");
#   print "# Row $i: ", DBI::neat_list( $row ),"\n";
  }
  is( $sth->rows, $cnt,"($_) Rows total : $cnt");
}

#$sth = $dbh->prepare("SELECT count(*) FROM $tbl");

is( $dbh->do("DELETE FROM $tbl"), $cnt,"Delete: $cnt");
is( $dbh->do("DELETE FROM $tbl"),'0E0',"Delete: 0E0");

ok( $dbh->disconnect,'Disconnect');
