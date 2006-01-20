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

my $tbl = $DBD_TEST::table_name;
my @col = sort keys %DBD_TEST::TestFieldInfo;
my $dat = [
  [ 1,'A123'  ,'A' x 12,'1998-05-13']
, [ 2,'B12'   ,'B' x  2,'1998-05-14']
, [ 3,'C1234' ,'C' x 22,'1998-05-15']
, [ 4,'D12345','D' x 32,'1998-05-16']
];
if ( defined $ENV{DBI_DSN} ) {
  plan tests => 4 + ( 3 + 3 * @$dat ) * @col;
} else {
  plan skip_all => 'Cannot test without DB info';
}

pass('Insert tests');

my $dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
   $dbh->{RaiseError} = 1;
   $dbh->{PrintError} = 0;
   $dbh->{ChopBlanks} = 1;
pass('Database connection created');

ok( DBD_TEST::tab_create( $dbh ),"CREATE TABLE $tbl");

for my $i ( 0..$#col ) {
  ok( $dbh->do( $_ ),"do $i: $_") for "DELETE FROM $tbl";
  my $ti  = DBD_TEST::get_type_for_column( $dbh, $col[$i] );
  my $sth = $dbh->prepare("INSERT INTO $tbl( $col[$i] ) VALUES( ? )");
  ok( defined $sth,"prepare $i: $sth->{Statement}");
  for ( @$dat ) {
    ok( $sth->bind_param( 1, $_->[$i], { TYPE => $ti->{DATA_TYPE} } ),"bind_param: $col[$i] => $_->[$i]");
    ok( $sth->$_, $_ ) for 'execute';
  }
  my $a = $dbh->selectcol_arrayref("SELECT $col[$i] FROM $tbl");
  ok( defined $a,"selectcol_arrayref $i: $#$a");
  @$a = sort @$a;
  is( $a->[$_], $dat->[$_][$i],"compare: $dat->[$_][$i]") for 0..$#$dat;
}
ok( $dbh->disconnect,'Disconnect');
