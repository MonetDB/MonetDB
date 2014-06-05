#!/usr/bin/env perl

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
# Copyright August 2008-2014 MonetDB B.V.
# All Rights Reserved.

use strict;
use warnings;
use DBI();

# determine the data sources:
my @ds = DBI->data_sources('monetdb');
print "data sources: @ds\n";

# connect to the database:
my $dsn = "dbi:monetdb:database=$ARGV[1];host=localhost;port=$ARGV[0];language=sql";
my $dbh = DBI->connect( $dsn,
  "monetdb", "monetdb",
  { PrintError => 0, RaiseError => 1 }  # turn on exception handling
);

$dbh->do('CREATE TABLE perl_dec38 (d38_0 DECIMAL(38,0), d38_19 DECIMAL(38,19), d38_38 DECIMAL(38,38));');
$dbh->do('INSERT INTO perl_dec38 VALUES (12345678901234567899876543210987654321, 1234567890123456789.9876543210987654321, .12345678901234567899876543210987654321);');
# "variable binding stuff" does not work, yet(?), due to missing type DBI::SQL_HUGEINT
#{
#  # variable binding stuff:
#  my $sth = $dbh->prepare('INSERT INTO perl_dec38 VALUES (?,?,?);');
#  $sth->bind_param( 1, 12345678909876543211234567890987654321 );
#  $sth->bind_param( 2, 1234567890987654321.1234567890987654321 );
#  $sth->bind_param( 3, .12345678909876543211234567890987654321 );
#  $sth->execute;
#}
{
  my $sth = $dbh->prepare('SELECT * FROM perl_dec38;');
  # get all rows one at a time:
  $sth->execute;
  while ( my $row = $sth->fetch ) {
    print "row: $row->[0], $row->[1], $row->[2]\n";
  }
  # get all rows at once:
  $sth->execute;
  my $t = $sth->fetchall_arrayref;
  my $r = @$t;         # row count
  my $f = @{$t->[0]};  # field count
  print "rows: $r, fields: $f\n";
  for my $i ( 0 .. $r-1 ) {
    for my $j ( 0 .. $f-1 ) {
      print "field[$i,$j]: $t->[$i][$j]\n";
    }
  }
}
{
  # get values of the first column from each row:
  my $row = $dbh->selectcol_arrayref('SELECT * FROM perl_dec38;');
  print "head[$_]: $row->[$_]\n" for 0 .. 0;
}
{
  my @row = $dbh->selectrow_array('SELECT * FROM perl_dec38;');
  print "field[0]: $row[0]\n";
  print "field[1]: $row[1]\n";
  print "field[2]: $row[2]\n";
}
{
  my $row = $dbh->selectrow_arrayref('SELECT * FROM perl_dec38;');
  print "field[0]: $row->[0]\n";
  print "field[1]: $row->[1]\n";
  print "field[2]: $row->[2]\n";
}
my $sth = $dbh->prepare('DROP TABLE perl_dec38;');
$sth->execute;
$dbh->disconnect;
print "\nFinished\n";
