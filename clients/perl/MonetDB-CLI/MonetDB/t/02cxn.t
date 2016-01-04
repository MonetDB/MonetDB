# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

$| = 1;

use strict;
use warnings;

use Test::More tests => 18;

use MonetDB::CLI;

pass('Connection tests');

my $host = $ENV{MONETDB_HOST} || 'localhost';
my $port = $ENV{MONETDB_PORT} || 50000;
my $user = $ENV{MONETDB_USER} || 'monetdb';
my $pass = $ENV{MONETDB_PASS} || 'monetdb';
my $lang = 'sql';

my $cxn = eval {
  MonetDB::CLI->connect( $host, $port, $user, $pass, $lang )
};
ok(!$@,'connect') or print "# $@";
ok( $cxn,"Connection object: $cxn");

my $req = eval { $cxn->query('select * from env') };
ok(!$@,'query') or print "# $@";
ok( $req,"Request object: $req");

my $cnt = eval { $req->columncount };
is( $cnt, 2,"columncount: $cnt");

my $querytype = eval { $req->querytype };
is( $querytype, 3,"querytype: $querytype");

for my $k ('id','rows_affected') {
  my $v = eval { $req->$k };
  ok( $v,"$k: $v");
}
for my $k ('name','type','length') {
  for my $i ( 0, 1 ) {
    my $v = eval { $req->$k( $i ) };
    ok( $v,"$k( $i ): $v");
  }
}
my $rows = 0;
while ( my $cnt = eval { $req->fetch } ) {
  print '#';
  print "\t", $req->field( $_ ) for 0 .. $cnt-1;
  print "\n";
  $rows++;
}
is( $rows, $req->rows_affected,"rows: $rows");

{
  my $req = eval { $cxn->query('select * from non_existent_table') };
  ok( $@,"Error expected: $@");
  ok(!$req,'No request object');
}
