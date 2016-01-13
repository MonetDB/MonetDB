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

use Test::More;

if (defined $ENV{DBI_DSN}) {
  plan tests => 9;
} else {
  plan skip_all => 'Cannot test without DB info';
}

pass('Get info tests');

my $dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
pass('Database connection created');

eval { $dbh->get_info };
ok( $@,"Call to get_info with 0 arguments, error expected: $@");

my $get_info = {
  SQL_DBMS_NAME              =>  17
, SQL_DBMS_VER               =>  18
, SQL_IDENTIFIER_QUOTE_CHAR  =>  29
, SQL_CATALOG_NAME_SEPARATOR =>  41
, SQL_CATALOG_LOCATION       => 114
};

for ( sort keys %$get_info ) {
  my $info =  $dbh->get_info( $get_info->{$_} );
  ok( defined $info,"get_info( $get_info->{$_} ) ($_): $info");
}

eval {

  print "\nList of all defined GetInfo types:\n";

  require DBI::Const::GetInfoType;
  require DBI::Const::GetInfoReturn;

  for ( sort keys %DBI::Const::GetInfoType::GetInfoType ) {
    my $Nr  = $DBI::Const::GetInfoType::GetInfoType{$_};
    my $Val = $dbh->get_info( $Nr );
    next unless defined $Val;
    my $Str = DBI::Const::GetInfoReturn::Format( $_, $Val );
    my $Exp = join ' | ', DBI::Const::GetInfoReturn::Explain( $_, $Val );
    printf " %6d %-35s %-13s %s\n", $Nr, $_, $Str, $Exp;
  }
};
ok( $dbh->disconnect,'Disconnect');

__END__

  SQL_CATALOG_LOCATION         => 114
, SQL_CATALOG_NAME_SEPARATOR   =>  41
, SQL_CATALOG_TERM             =>  42
, SQL_CONCAT_NULL_BEHAVIOR     =>  22
, SQL_DATA_SOURCE_NAME         =>   2
, SQL_DBMS_NAME                =>  17
, SQL_DBMS_VER                 =>  18
, SQL_DBMS_VERSION             =>  18
, SQL_DRIVER_NAME              =>   6
, SQL_DRIVER_VER               =>   7
, SQL_IDENTIFIER_CASE          =>  28
, SQL_IDENTIFIER_QUOTE_CHAR    =>  29
, SQL_KEYWORDS                 =>  89
, SQL_OWNER_TERM               =>  39
, SQL_PROCEDURE_TERM           =>  40
, SQL_QUALIFIER_LOCATION       => 114
, SQL_QUALIFIER_NAME_SEPARATOR =>  41
, SQL_QUALIFIER_TERM           =>  42
, SQL_SCHEMA_TERM              =>  39
, SQL_TABLE_TERM               =>  45
, SQL_USER_NAME                =>  47
