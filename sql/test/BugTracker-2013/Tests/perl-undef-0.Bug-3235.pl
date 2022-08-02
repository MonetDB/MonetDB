#!/usr/bin/env perl

use strict;
use warnings;
use Data::Dumper;

$|++;

use DBI();

my $dbh = DBI->connect(
    "dbi:monetdb:host=localhost;port=$ARGV[0];database=$ARGV[1]", 'monetdb', 'monetdb'
);

my $query = qq{
    select 0;
};

my $sth = $dbh->prepare($query);
$sth->execute;

my $r = $sth->fetchall_arrayref();

print Dumper($r);

$dbh->disconnect();
