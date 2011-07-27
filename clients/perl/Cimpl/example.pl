#!/usr/bin/perl -w

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
# Copyright August 2008-2011 MonetDB B.V.
# All Rights Reserved.

use MapiLib;
use strict;

my $dbh;

$dbh = MapiLib::mapi_connect("localhost", 50000, "monetdb", "monetdb", "sql");

die MapiLib::mapi_error_str($dbh) if MapiLib::mapi_error($dbh);

my $sth = MapiLib::mapi_query($dbh, "SELECT name FROM tables");

die MapiLib::mapi_error_str($dbh) if MapiLib::mapi_error($dbh);

print MapiLib::mapi_get_name($sth, 0) . "\n------------\n";

while (MapiLib::mapi_fetch_row($sth)) {
  print MapiLib::mapi_fetch_field($sth, 0) . "\n";
}
