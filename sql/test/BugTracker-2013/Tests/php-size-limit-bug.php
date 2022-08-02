<?php
    require 'monetdb/php_monetdb.php';

    $db = monetdb_connect("sql", "localhost", $argv[1], "monetdb", "monetdb", $argv[2]) or die(monetdb_last_error());

    $packet_size = 20000;

    $sql = 'select 1';
    $sql = str_pad($sql, $packet_size , ' ');

    echo strlen($sql) . "\n";

    $res = monetdb_query($sql);
    while ( $row = monetdb_fetch_assoc($res) ) { print_r($row); }

    monetdb_disconnect();
?>
