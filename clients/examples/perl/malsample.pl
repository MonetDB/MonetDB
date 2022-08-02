#!/usr/bin/env perl

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.

use strict;
use warnings;
use DBI();

print "\nStart a simple Monet MAL interaction\n\n";

# determine the data sources:
my @ds = DBI->data_sources('monetdb');
print "data sources: @ds\n";

# connect to the database:
my $dsn = "dbi:monetdb:database=$ARGV[1];host=localhost;port=$ARGV[0];language=mal";
my $dbh = DBI->connect( $dsn,
  "monetdb", "monetdb",  # no authentication in MAL
  { PrintError => 0, RaiseError => 1 }  # turn on exception handling
);
{
  # simple MAL statement:
  my $sth = $dbh->prepare('io.print(2);');
  $sth->execute;
  my @row = $sth->fetchrow_array;
  print "field[0]: $row[0], last index: $#row\n";
}
{
  my $sth = $dbh->prepare('io.print(3);');
  $sth->execute;
  my @row = $sth->fetchrow_array;
  print "field[0]: $row[0], last index: $#row\n";
}
{
  # deliberately executing a wrong MAL statement:
  my $sth = $dbh->prepare('( xyz 1);');
  eval { $sth->execute }; print "ERROR REPORTED: $@" if $@;
}
$dbh->do('b:=bat.new(:oid,:str);');
$dbh->do('bat.append(b,"one");');
$dbh->do('bat.append(b,"two");');
$dbh->do('bat.append(b,"three");');
{
  # variable binding stuff:
	$dbh->do('c:=bat.new(:oid,:int);');
  my $sth = $dbh->prepare('bat.append(c,?);');
  $sth->bind_param( 1,     7 , DBI::SQL_INTEGER() );
  $sth->execute;
}
{
  my $sth = $dbh->prepare('io.print(b);');
  # get all rows one at a time:
  $sth->execute;
  while ( my $row = $sth->fetch ) {
    print "bun: $row->[0], $row->[1]\n";
  }
  # get all rows at once:
  $sth->execute;
  my $t = $sth->fetchall_arrayref;
  my $r = @$t;         # row count
  my $f = 0;
  $f = @{$t->[0]} if $r > 0;  # field count
  print "rows: $r, fields: $f\n";
  for my $i ( 0 .. $r-1 ) {
    for my $j ( 0 .. $f-1 ) {
      print "field[$i,$j]: $t->[$i][$j]\n";
    }
  }
}
{
  # get values of the first column from each row:
  my $row = $dbh->selectcol_arrayref('io.print(b);');
  print "head[$_]: $row->[$_]\n" for 0 .. 1;
}
{
  my @row = $dbh->selectrow_array('io.print(b);');
  print "field[0]: $row[0]\n";
  print "field[1]: $row[1]\n";
}
{
  my $row = $dbh->selectrow_arrayref('io.print(b);');
  print "field[0]: $row->[0]\n";
  print "field[1]: $row->[1]\n";
}
$dbh->disconnect;
print "\nFinished\n";
