#!/usr/bin/perl -w

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

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
