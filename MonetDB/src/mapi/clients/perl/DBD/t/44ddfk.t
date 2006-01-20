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
