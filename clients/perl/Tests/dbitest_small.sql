#!/usr/bin/perl -w

use strict;
use DBI;
use DBI qw(:sql_types);
use Data::Dump qw(dump);

# Connect to the database.
my $dbh = DBI->connect("dbi:monetdb:",
		       "monetdb", "monetdb",
		       {'PrintError' =>1, 'RaiseError' => 1});

my $sth = $dbh->prepare("SELECT id, schema_id, name FROM tables;");
$sth->execute;

print dump($sth->fetchall_hashref("id"));
