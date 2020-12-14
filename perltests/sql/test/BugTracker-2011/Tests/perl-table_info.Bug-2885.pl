#!/usr/bin/env perl

use strict;
use warnings;

$|++;

use DBI();

my $dbh = DBI->connect("dbi:monetdb:database=$ARGV[1];host=localhost;port=$ARGV[0];language=sql", 'monetdb', 'monetdb');

my $sth = $dbh->table_info('', '%', '%');
my $sth2 = $dbh->table_info('foo', '%', '%');
