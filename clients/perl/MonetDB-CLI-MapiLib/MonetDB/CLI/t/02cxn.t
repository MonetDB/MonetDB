# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2011 MonetDB B.V.
# All Rights Reserved.

$| = 1;

use strict;
use warnings;

use Test::More tests => 18;

use MonetDB::CLI::MapiLib;

pass('Connection tests');

my $host = $ENV{MONETDB_HOST} || 'localhost';
my $port = $ENV{MONETDB_PORT} || 50000;
my $user = $ENV{MONETDB_USER} || 'monetdb';
my $pass = $ENV{MONETDB_PASS} || 'monetdb';
my $lang = 'sql';

my $cxn = eval {
  MonetDB::CLI::MapiLib->connect( $host, $port, $user, $pass, $lang )
};
ok(!$@,'connect') or print "# $@";
ok( $cxn,"Connection object: $cxn");

my $req = eval { $cxn->query('select * from env() env') };
ok(!$@,'query') or print "# $@";
ok( $req,"Request object: $req");

my $cnt = eval { $req->columncount };
is( $cnt, 2,"columncount: $cnt");

my $querytype = eval { $req->querytype };
is( $querytype, 1,"querytype: $querytype");

for my $k ('id','rows_affected') {
  my $v = eval { $req->$k };
  ok( defined $v,"$k: $v");
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
