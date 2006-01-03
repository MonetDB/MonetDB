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
  if ($ENV{DBI_DSN} =~ /dbi:monetdb:/) {
    plan tests => 30;
  } else {
    plan skip_all => 'dbi:monetdb: specific tests';
  }
} else {
  plan skip_all => 'Cannot test without DB info';
}

pass('MIL tests');

$ENV{DBI_DSN} .= ';language=mil';

my $dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
pass('Database connection created');

for ( 7 .. 9 )
{
  my $sth = $dbh->prepare("print( $_ );");
  ok( defined $sth,'Statement handle defined');
  ok( $sth->execute,'execute');
  my $row = $sth->fetch;
  is( $#$row, 0,'last index');
  is( $row->[0], $_,'field 0');
}
{
  local $dbh->{PrintError} = 0;
  my $sth = $dbh->prepare('( xyz 1);');
  ok(!$sth->execute,'execute');
  like( $sth->errstr, qr/!ERROR:/,'Error expected');
}
ok( $dbh->do( $_ ), $_) for 'var b := new( int, str );';
ok( $dbh->do( $_ ), $_) for 'insert( b, 3,"T3");';
{
  my $sth = $dbh->prepare('insert( b, ?, ? );');
  ok( defined $sth,'Statement handle defined');
  ok( $sth->bind_param( 1,  7 , DBI::SQL_INTEGER() ),'bind');
  ok( $sth->bind_param( 2,'T7' ),'bind');
  ok( $sth->execute,'execute');
}
{
  my $sth = $dbh->prepare('print( b );');
  ok( defined $sth,'Statement handle defined');
  ok( $sth->execute,'execute');
  for ( 3, 7 )
  {
    my $row = $sth->fetch;
    is( $row->[0],  $_ ,"fetch  $_");
    is( $row->[1],"T$_","fetch T$_");
  }
}
ok( $dbh->rollback,'Rollback');
ok( $dbh->disconnect,'Disconnect');
