<?php
require 'monetdb/php_monetdb.php';

$db = monetdb_connect("sql", "localhost", $argv[1], "monetdb", "monetdb", $argv[2]) or die(monetdb_last_error());

$res = monetdb_query('START TRANSACTION;') or die(monetdb_last_error());
while ( $row = monetdb_fetch_assoc($res) ) { print_r($row); }

$res = monetdb_query('CREATE TABLE php_dec38 (d38_0 DECIMAL(38,0), d38_19 DECIMAL(38,19), d38_38 DECIMAL(38,38));') or die(monetdb_last_error());
while ( $row = monetdb_fetch_assoc($res) ) { print_r($row); }

$res = monetdb_query('INSERT INTO php_dec38 VALUES (12345678901234567899876543210987654321, 1234567890123456789.9876543210987654321, .12345678901234567899876543210987654321);') or die(monetdb_last_error());
while ( $row = monetdb_fetch_assoc($res) ) { print_r($row); }

$res = monetdb_query('SELECT * FROM php_dec38;') or die(monetdb_last_error());
while ( $row = monetdb_fetch_assoc($res) ) { print_r($row); }

$res = monetdb_query('SELECT * FROM php_dec38;') or die(monetdb_last_error());
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
