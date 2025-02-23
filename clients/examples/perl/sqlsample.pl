#!/usr/bin/env perl

# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024, 2025 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

use strict;
use warnings;
use DBI();

print "\nStart a simple Monet SQL interaction\n\n";

# determine the data sources:
my @ds = DBI->data_sources('monetdb');
print "data sources: @ds\n";

# connect to the database:
my $dsn = "dbi:monetdb:database=$ARGV[1];host=localhost;port=$ARGV[0];language=sql";
my $dbh = DBI->connect( $dsn,
  "monetdb", "monetdb",
  { PrintError => 0, RaiseError => 1 }  # turn on exception handling
);
{
  # simple SQL statement:
  my $sth = $dbh->prepare('select 2;');
  $sth->execute;
  my @row = $sth->fetchrow_array;
  print "field[0]: $row[0], last index: $#row\n";
}
{
  my $sth = $dbh->prepare('select 3;');
  $sth->execute;
  my @row = $sth->fetchrow_array;
  print "field[0]: $row[0], last index: $#row\n";
}
{
  # deliberately executing a wrong SQL statement:
  my $sth = $dbh->prepare('select commit_action, access from tables group by access;');
  eval { $sth->execute }; print "ERROR REPORTED: $@" if $@;
}
$dbh->do('create table perl_table (i smallint,s string);');
$dbh->do('insert into perl_table values ( 3, \'three\');');
{
  # variable binding stuff:
  my $sth = $dbh->prepare('insert into perl_table values(?,?);');
  $sth->bind_param( 1,     7 , DBI::SQL_INTEGER() );
  $sth->bind_param( 2,'seven' );
  $sth->execute;
  $sth->bind_param( 1,    42 , DBI::SQL_INTEGER() );
  $sth->bind_param( 2,  '\\n' );
  $sth->execute;
}
{
  my $sth = $dbh->prepare('select * from perl_table;');
  # get all rows one at a time:
  $sth->execute;
  while ( my $row = $sth->fetch ) {
    print "bun: $row->[0], $row->[1]\n";
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
  my $row = $dbh->selectcol_arrayref('select * from perl_table;');
  print "head[$_]: $row->[$_]\n" for 0 .. 1;
}
{
  my @row = $dbh->selectrow_array('select * from perl_table;');
  print "field[0]: $row[0]\n";
  print "field[1]: $row[1]\n";
}
{
  my $row = $dbh->selectrow_arrayref('select * from perl_table;');
  print "field[0]: $row->[0]\n";
  print "field[1]: $row->[1]\n";
}
my $sth = $dbh->prepare('drop table perl_table;');
$sth->execute;
$dbh->disconnect;
print "\nFinished\n";
