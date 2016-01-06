# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

#/usr/bin/env perl

use strict;
use warnings;

$|++;

use DBI();

my $dsn = "dbi:monetdb:database=$ARGV[1];host=localhost;port=$ARGV[0]";
my $dbh = DBI->connect(
    $dsn, 'monetdb', 'monetdb'
);

my $query = qq{
SELECT
    *
FROM
    functions
UNION ALL
SELECT
    *
FROM
    functions
UNION ALL
SELECT
    *
FROM
    functions
UNION ALL
SELECT
    *
FROM
    functions
UNION ALL
SELECT
    *
FROM
    functions
;
};

my $sth = $dbh->prepare($query);
$sth->execute;

# Here we tell DBI to fetch at most 1000 lines (out of ~5000 available)
my $r = $sth->fetchall_arrayref(undef, 1000);

# Print "200 rows" in my case, should print "1000 rows"
print scalar(@{$r}) . " rows\n";

$dbh->disconnect();

