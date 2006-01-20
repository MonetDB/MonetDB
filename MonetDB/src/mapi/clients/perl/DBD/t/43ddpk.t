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
  plan tests => 19;
} else {
  plan skip_all => 'Cannot test without DB info';
}

pass('Primary key tests');

my $dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
pass('Database connection created');

eval { $dbh->primary_key_info };
ok( $@,"Call to primary_key_info with 0 arguments, error expected: $@");

eval { $dbh->primary_key };
ok( $@,"Call to primary_key with 0 arguments, error expected: $@");

{
  local $dbh->{PrintError} = 0;

  my $sth = $dbh->primary_key_info( undef, undef, '');
  ok( $dbh->err,'Call to primary_key with undefined schema argument, error expected: ' . $dbh->errstr );

  $sth = $dbh->primary_key_info( undef,'', undef );
  ok( $dbh->err,'Call to primary_key with undefined table argument, error expected: ' . $dbh->errstr );
}
# -----------------------------------------------------------------------------

my $catalog = undef;
my $schema  = $dbh->selectrow_array(<<'SQL');
select name from sys.schemas where name = current_schema
SQL
ok( $schema,"Current schema: $schema");
my $tbl     = lc $DBD_TEST::table_name;

my $ti = DBD_TEST::get_type_for_column( $dbh,'A');
is( ref $ti,'HASH','Type info');

{
  local ($dbh->{Warn}, $dbh->{PrintError});
  $dbh->{PrintError} = $dbh->{Warn} = 0;
  $dbh->do("DROP TABLE $tbl");
}
# -----------------------------------------------------------------------------
SKIP: {
  my $sql = <<"SQL";
CREATE TABLE $tbl
(
  K1 $ti->{TYPE_NAME} PRIMARY KEY
, K2 $ti->{TYPE_NAME}
)
SQL
  $dbh->do( $sql );
  is( $dbh->err, undef,"$sql");

  skip('PK test 1', 4 ) if $dbh->err;

  my $sth = $dbh->primary_key_info( $catalog, $schema, $tbl );
  ok( defined $sth,'Statement handle defined');

  my $a = $sth->fetchall_arrayref;

  print "# Primary key columns:\n";
  print '# ', DBI::neat_list( $_ ), "\n" for @$a;

  is( $#$a, 0,'Exactly one primary key column');
  is( uc( $a->[0][3] ),'K1', 'Primary key column name');

  ok( $dbh->do( $_ ), $_ ) for "DROP TABLE $tbl";
}
# -----------------------------------------------------------------------------
SKIP: {
  my $sql = <<"SQL";
CREATE TABLE $tbl
(
  K1 $ti->{TYPE_NAME}
, K2 $ti->{TYPE_NAME}
, PRIMARY KEY ( K1, K2 )
)
SQL
  {
    local $dbh->{PrintError} = 0;
    $dbh->do( $sql );
  }
  is( $dbh->err, undef,"$sql");

  skip('PK test 2', 4 ) if $dbh->err;

  my $sth = $dbh->primary_key_info( $catalog, $schema, $tbl );
  ok( defined $sth,'Statement handle defined');

  my $a = $sth->fetchall_arrayref;

  print "# Primary key columns:\n";
  print '# ', DBI::neat_list( $_ ), "\n" for @$a;

  is( $#$a, 1,'Exactly two primary key columns');
  is( uc( $a->[$_-1][3] ),"K$_","Primary key column name: K$_") for 1, 2;
}
# -----------------------------------------------------------------------------

ok( $dbh->disconnect,'Disconnect');
