#!/usr/bin/perl -w

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
