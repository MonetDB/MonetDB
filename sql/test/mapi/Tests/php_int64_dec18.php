<?php
require 'monetdb/php_monetdb.php';

$db = monetdb_connect("sql", "localhost", $argv[1], "monetdb", "monetdb", $argv[2]) or die(monetdb_last_error());

$res = monetdb_query('START TRANSACTION;') or die(monetdb_last_error());
while ( $row = monetdb_fetch_assoc($res) ) { print_r($row); }

$res = monetdb_query('CREATE TABLE php_int64_dec18 (i BIGINT, d0 DECIMAL(18,0), d9 DECIMAL(18,9));') or die(monetdb_last_error());
while ( $row = monetdb_fetch_assoc($res) ) { print_r($row); }

$res = monetdb_query('INSERT INTO php_int64_dec18 VALUES (1234567890987654321, 123456789987654321, 123456789.987654321);') or die(monetdb_last_error());
while ( $row = monetdb_fetch_assoc($res) ) { print_r($row); }

$res = monetdb_query('SELECT * FROM php_int64_dec18;') or die(monetdb_last_error());
while ( $row = monetdb_fetch_assoc($res) ) { print_r($row); }

$res = monetdb_query('SELECT * FROM php_int64_dec18;') or die(monetdb_last_error());
$cols = monetdb_num_fields($res);
for ($i = 0; $i < $cols; $i++) {
	print(monetdb_field_name($res, $i)."\t");
}
print("\n");
while ($row = @monetdb_fetch_row($res)) {
	for ($i = 0; $i < $cols; $i++) {
		print($row[$i]."\t");
	}
	print("\n");
}
?>
