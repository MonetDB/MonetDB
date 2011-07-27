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

use Test::More tests => 14;
use Devel::Peek;

use MonetDB::CLI::MapiXS;

pass('Reference count tests');

my $host = $ENV{MONETDB_HOST} || 'localhost';
my $port = $ENV{MONETDB_PORT} || 50000;
my $user = $ENV{MONETDB_USER} || 'monetdb';
my $pass = $ENV{MONETDB_PASS} || 'monetdb';
my $lang = 'sql';

my $cxn = eval {
  MonetDB::CLI::MapiXS->connect( $host, $port, $user, $pass, $lang )
};
ok(!$@,'connect') or print "# $@";
ok( $cxn,"Connection object: $cxn");
is( Devel::Peek::SvREFCNT( $cxn ), 1,'SvREFCNT cxn');

my $req1 = eval { $cxn->query('select * from env() env') };
ok(!$@,'query') or print "# $@";
ok( $req1,"Request object: $req1");
is( Devel::Peek::SvREFCNT( $req1 ), 1,'SvREFCNT req1');
is( Devel::Peek::SvREFCNT( $cxn  ), 2,'SvREFCNT cxn' );

my $req2 = eval { $cxn->query('select * from env() env') };
ok(!$@,'query') or print "# $@";
ok( $req2,"Request object: $req2");
is( Devel::Peek::SvREFCNT( $req2 ), 1,'SvREFCNT req2');
is( Devel::Peek::SvREFCNT( $cxn  ), 3,'SvREFCNT cxn' );

undef $req1;
is( Devel::Peek::SvREFCNT( $cxn  ), 2,'SvREFCNT cxn' );

undef $req2;
is( Devel::Peek::SvREFCNT( $cxn  ), 1,'SvREFCNT cxn' );
