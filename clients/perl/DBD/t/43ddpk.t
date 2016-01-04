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
