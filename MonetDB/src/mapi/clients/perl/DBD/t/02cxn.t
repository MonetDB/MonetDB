#!perl -I./t

$| = 1;

use strict;
use warnings;
use DBI();

use Test::More;

if (defined $ENV{DBI_DSN}) {
  plan tests => 8;
} else {
  plan skip_all => 'Cannot test without DB info';
}

pass('Connection tests');

my $dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
pass('Database connection created');

SKIP: {
  skip('DBD::monetdb specific test', 1 ) if $dbh->{Driver}{Name} ne 'monetdb';

  my $Cxn = $dbh->{monetdb_connection};

  ok( $Cxn,"Connection object: $Cxn");
}
ok( $dbh->ping,'Ping');

ok( $dbh->{Active},'Active');

ok( $dbh->disconnect,'Disconnect');

ok(!$dbh->ping,'Ping');

ok(!$dbh->{Active},'Active');
