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
  plan tests => 14;
} else {
  plan skip_all => 'Cannot test without DB info';
}

pass('Foreign key tests');

my $dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
pass('Database connection created');

eval { $dbh->foreign_key_info };
ok( $@,"Call to foreign_key_info with 0 arguments, error expected: $@");

{
  local $dbh->{PrintWarn} = 0;

#  my $sth = $dbh->foreign_key_info('', undef, undef, undef, undef, undef );
#  ok( $dbh->errstr,'Call to foreign_key_info with catalog argument, warning expected: ' . $dbh->errstr );
}
# -----------------------------------------------------------------------------

my $catalog = undef;
my $schema  = $dbh->selectrow_array(<<'SQL');
select name from sys.schemas where name = current_schema
SQL
ok( $schema,"Current schema: $schema");
my $tbl     = lc $DBD_TEST::table_name;
my $tbl2    = $tbl . '_2';

my $ti = DBD_TEST::get_type_for_column( $dbh,'A');
is( ref $ti,'HASH','Type info');

{
  local ($dbh->{Warn}, $dbh->{PrintError});
  $dbh->{PrintError} = $dbh->{Warn} = 0;
  $dbh->do("DROP TABLE $tbl2");
  $dbh->do("DROP TABLE $tbl");
}
# -----------------------------------------------------------------------------
SKIP: {
  my $sql = <<"SQL";
create table $tbl
(
  n integer
, s varchar(9)
, d date
, constraint pk_t primary key ( n, s )
, constraint uk_t unique ( d )
)
SQL
  $dbh->do( $sql );
  is( $dbh->err, undef,"$sql");

  skip('FK test 1', 4 ) if $dbh->err;

  $sql = <<"SQL";
create table $tbl2
(
  n2 integer
, s2 varchar(9)
, d2 date
, constraint pk2_t primary key ( n2, s2 )
, constraint uk2_t unique ( d2 )
, constraint fkp_t foreign key ( n2, s2 ) references $tbl
, constraint fku_t foreign key ( d2 ) references $tbl( d )
)
SQL
  $dbh->do( $sql );
  is( $dbh->err, undef,"$sql");

  my $sth = $dbh->foreign_key_info( $catalog, $schema, $tbl, $catalog, $schema, $tbl2 );
  ok( defined $sth,'Statement handle defined');

  my $a = $sth->fetchall_arrayref;

  print "# Foreign key columns:\n";
  print '# ', DBI::neat_list( $_ ), "\n" for @$a;

  is( $#$a, 2,'Exactly 3 foreign key columns');
  is( $a->[2][3],'s', 'Foreign key column name');

  ok( $dbh->do( $_ ), $_ ) for "DROP TABLE $tbl2";
  ok( $dbh->do( $_ ), $_ ) for "DROP TABLE $tbl";
}
# -----------------------------------------------------------------------------
SKIP: {
  skip('Invalid use of null pointer (SQL-HY009) when using DBD::ODBC', 1 )
    if $dbh->{Driver}{Name} eq 'ODBC';
  my $sth = $dbh->foreign_key_info( undef, undef, undef, undef, undef, undef );
  ok( defined $sth,'Statement handle defined for foreign_key_info()');
  DBD_TEST::dump_results( $sth );
}
# -----------------------------------------------------------------------------

ok( $dbh->disconnect,'Disconnect');
