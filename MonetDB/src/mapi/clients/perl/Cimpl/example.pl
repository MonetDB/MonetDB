#!/usr/bin/perl -w

# The contents of this file are subject to the MonetDB Public
# License Version 1.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of
# the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
#
# Software distributed under the License is distributed on an "AS
# IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
# implied. See the License for the specific language governing
# rights and limitations under the License.
#
# The Original Code is the Monet Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2004 CWI.
# All Rights Reserved.

use MapiLib;
use strict;

my $dbh;

$dbh = mapi_connect("localhost", 45123, "monetdb", "monetdb", "sql");

die mapi_error_str($dbh) if mapi_error($dbh);

my $sth = mapi_query($dbh, "SELECT name FROM tables");

die mapi_error_str($dbh) if mapi_error($dbh);

print mapi_get_name($sth, 0) . "\n------------\n";

while (mapi_fetch_row($sth)) {
  print mapi_fetch_field($sth, 0) . "\n";
}
