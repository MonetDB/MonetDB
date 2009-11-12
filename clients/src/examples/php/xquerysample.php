#!/usr/bin/php
<?php
require 'monetdb/php_monetdb.php';

$db = monetdb_connect("xquery", "localhost", $argv[1], "monetdb", "monetdb");
$result = monetdb_query('1 to 4,"aap",1.0,attribute { "aap" } { "beer" },<aap/>');

while ($row = @monetdb_fetch_row($result)) {
        print($row[0]."\n");
}
?>
