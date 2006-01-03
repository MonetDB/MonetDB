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
