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

if ( defined $ENV{DBI_DSN} ) {
  plan tests => 14;
} else {
  plan skip_all => 'Cannot test without DB info';
}

pass('NULL tests');

my $dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
   $dbh->{RaiseError} = 1;
   $dbh->{PrintError} = 0;
pass('Database connection created');

my $tbl = $DBD_TEST::table_name;
my @col = sort keys %DBD_TEST::TestFieldInfo;

ok( DBD_TEST::tab_create( $dbh ),"CREATE TABLE $tbl");

ok( $dbh->do("INSERT INTO $tbl( $_ ) VALUES( ? )", undef, undef ),"Inserting NULL into $_")
  for @col;

my $Cols = join ', ', @col;
my $Qs   = join ', ', map {'?'} @col;
my $sth = $dbh->prepare("INSERT INTO $tbl( $Cols ) VALUES( $Qs )");
ok( defined $sth,'Prepare insert statement');

my $i = 0;
for ( @col ) {
  my $ti = DBD_TEST::get_type_for_column( $dbh, $_ );
  ok( $sth->bind_param( ++$i, undef, { TYPE => $ti->{DATA_TYPE} } ),"Bind parameter for column $_");
}
ok( $sth->execute,'Execute prepared statement with bind params');

ok( $dbh->disconnect,'Disconnect');
