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
  plan tests => 6;
} else {
  plan skip_all => 'Cannot test without DB info';
}

my $dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
ok ( defined $dbh, 'Connection');

ok( DBD_TEST::tab_create( $dbh ),"Create the test table $DBD_TEST::table_name");

my $data =
[
  [ 1,'foo'   ,'me' x 120      ,'1998-05-13','1988-05-13 01:12:33']
, [ 2,'bar'   ,'bar varchar'   ,'1998-05-14','1998-05-14 01:25:33']
, [ 3,'bletch','bletch varchar','1998-05-15','1998-05-15 01:15:33']
, [ 4,'bletch','me' x 14       ,'1998-05-15','1998-05-15 01:15:33']
];

ok( tab_insert( $dbh, $data ),'Insert test data');

ok( tab_select( $dbh ),'Select test data');

ok( DBD_TEST::tab_delete( $dbh ),'Drop test table');

ok( $dbh->disconnect,'Disconnect');


sub tab_select
{
  my $dbh = shift;

  my $sth = $dbh->prepare("SELECT A,B,C,D FROM $DBD_TEST::table_name WHERE a = ?")
    or return undef;
  my $ti = DBD_TEST::get_type_for_column( $dbh,'A');
  for my $v ( 1, 3, 2, 4, 10 ) {
    $sth->bind_param( 1, $v, { TYPE => $ti->{DATA_TYPE} } );
    $sth->execute;
    while ( my $row = $sth->fetch ) {
      print "# -- $row->[0] length:", length $row->[1]," $row->[1] $row->[2] $row->[3]\n";
      if ( $row->[0] != $v ) {
        print "# Bind value failed! bind value = $v, returned value = $row->[0]\n";
        return undef;
      }
    }
  }
  return 1;
}

sub tab_insert
{
  my $dbh  = shift;
  my $data = shift;

  my $sth = $dbh->prepare("INSERT INTO $DBD_TEST::table_name (A, B, C, D) VALUES (?, ?, ?, ?)");
  unless ( $sth ) {
    print $DBI::errstr;
    return 0;
  }
  $sth->{PrintError} = 1;
  for ( @$data ) {
    my $ti;

    $ti = DBD_TEST::get_type_for_column( $dbh,'A');
    $sth->bind_param( 1, $_->[ 0], { TYPE => $ti->{DATA_TYPE} } );

    $ti = DBD_TEST::get_type_for_column( $dbh,'B');
#   $_->[1] = $_->[1] x (int( int( $ti->{COLUMN_SIZE} / 2 ) / length( $_->[1] ) ) );  # XXX
    $sth->bind_param( 2, $_->[ 1], { TYPE => $ti->{DATA_TYPE} } );

    $ti = DBD_TEST::get_type_for_column( $dbh,'C');
    $sth->bind_param( 3, $_->[ 2], { TYPE => $ti->{DATA_TYPE} } );

    $ti = DBD_TEST::get_type_for_column( $dbh,'D');
    my $i = ( $ti->{DATA_TYPE} == DBI::SQL_TYPE_DATE || $ti->{DATA_TYPE} == DBI::SQL_DATE ) ? 3 : 4;
    $sth->bind_param( 4, $_->[$i], { TYPE => $ti->{DATA_TYPE} } );

    return 0 unless $sth->execute;
  }
  1;
}
