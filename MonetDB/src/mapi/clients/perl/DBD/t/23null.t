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
# Portions created by CWI are Copyright (C) 1997-2005 CWI.
# All Rights Reserved.

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
