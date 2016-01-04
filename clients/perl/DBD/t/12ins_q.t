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

my $tbl = $DBD_TEST::table_name;
my @col = sort keys %DBD_TEST::TestFieldInfo;
my $dat = [
  [ 1,'A123'  ,'A' x 12,'1998-05-13']
, [ 2,'B12'   ,'B' x  2,'1998-05-14']
, [ 3,'C1234' ,'C' x 22,'1998-05-15']
, [ 4,'D12345','D' x 32,'1998-05-16']
];
if ( defined $ENV{DBI_DSN} ) {
  plan tests => 4 + ( 2 + 2 * @$dat ) * @col;
} else {
  plan skip_all => 'Cannot test without DB info';
}

pass('Insert tests (quoted literals)');

my $dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
   $dbh->{RaiseError} = 1;
   $dbh->{PrintError} = 0;
   $dbh->{ChopBlanks} = 1;
pass('Database connection created');

ok( DBD_TEST::tab_create( $dbh ),"CREATE TABLE $tbl");

for my $i ( 0..$#col ) {
  ok( $dbh->do( $_ ),"do $i: $_") for "DELETE FROM $tbl";
  my $ti  = DBD_TEST::get_type_for_column( $dbh, $col[$i] );
  for ( @$dat ) {
    my $v = $dbh->quote( $_->[$i], $ti->{DATA_TYPE} );
    ok( $dbh->do( $_ ),"do $i: $_") for "INSERT INTO $tbl( $col[$i] ) VALUES( $v )";
  }
  my $a = $dbh->selectcol_arrayref("SELECT $col[$i] FROM $tbl");
  ok( defined $a,"selectcol_arrayref $i: $#$a");
  @$a = sort @$a;
  is( $a->[$_], $dat->[$_][$i],"compare: $dat->[$_][$i]") for 0..$#$dat;
}
ok( $dbh->disconnect,'Disconnect');
