#!/usr/bin/php
<?php
if (!extension_loaded('monetdb')) {
        $prefix = (PHP_SHLIB_SUFFIX == 'dll') ? 'php_' : '';
        // note: PHP5 says it deprecates this, but I can't find
        //       how to make PHP5 happy...
        dl($prefix.'monetdb.'.PHP_SHLIB_SUFFIX) or
                die("Unable to load monetdb module!");
}

$db = monetdb_connect("xquery", "localhost", $argv[1], "monetdb", "monetdb");
$result = monetdb_query('1 to 4,"aap",1.0,attribute { "aap" } { "beer" },<aap/>');

while ($row = @monetdb_fetch_row($result)) {
        print($row[0]."\n");
}
?>
