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
  plan tests => 25;
} else {
  plan skip_all => 'Cannot test without DB info';
}

pass('Column info tests');

my $dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
pass('Database connection created');

my $tbl = lc $DBD_TEST::table_name;

{
  ok( DBD_TEST::tab_create( $dbh ),"CREATE TABLE $tbl");
}
# TODO: handle catalog and schema ($tbl may exist in more then one schema)
{
  my $sth;

  eval { $sth = $dbh->column_info( undef, undef, undef, undef ) };
  ok( (!$@ and defined $sth ),'column_info tested');
  $sth = undef;
}
{
  my $sth = $dbh->column_info( undef, undef, $tbl,'b');
  ok( defined $sth,'Statement handle defined');

  my $row = $sth->fetch;
  is( $row->[ 2], $tbl,"Is this table name $tbl?");
  is( $row->[ 3], 'b','Is this column name b?');
}
{
  my $sth = $dbh->column_info( undef, undef, $tbl, undef );
  ok( defined $sth,'Statement handle defined');

  my @ColNames = sort keys %DBD_TEST::TestFieldInfo;
  print "# Columns:\n";
  my $i = 0;
  while ( my $row = $sth->fetch )
  {
    $i++;
    {
      no warnings 'uninitialized';
      local $,  = ":"; print '# ', @$row, "\n";
    }
    $row->[ 3] = uc $row->[ 3];
    is( $row->[ 2], $tbl             ,"Is this table name $tbl?");
    is( $row->[16], $i               ,"Is this ordinal position $i?");
    is( $row->[ 3], $ColNames[$i-1]  ,"Is this column name $ColNames[$i-1]?");
    my $ti = DBD_TEST::get_type_for_column( $dbh, $row->[3] );
#   is( $row->[ 4] , $ti->{DATA_TYPE},"Is this data type $ti->{DATA_TYPE}?");
    is( $row->[ 5] , $ti->{TYPE_NAME},"Is this type name $ti->{TYPE_NAME}?");
  }
}

ok( $dbh->disconnect,'Disconnect');
