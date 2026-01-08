#!/usr/bin/perl -w

# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# For copyright information, see the file debian/copyright.

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
