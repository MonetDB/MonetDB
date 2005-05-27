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
