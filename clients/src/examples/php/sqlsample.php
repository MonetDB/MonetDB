#!/usr/bin/php 
<?
if (!extension_loaded('monetdb')) {
	$prefix = (PHP_SHLIB_SUFFIX == 'dll') ? 'php_' : '';
	// note: PHP5 says it deprecates this, but I can't find
	//       how to make PHP5 happy...
	dl($prefix.'monetdb.'.PHP_SHLIB_SUFFIX) or
		die("Unable to load monetdb module!");
}
$db = monetdb_connect("sql", "localhost", $argv[1], "monetdb", "monetdb");
$tables = monetdb_query('SELECT name FROM tables');

for ($i = 0; $line = @monetdb_fetch_assoc($tables); $i++) {
	print($line['name']."\n");
}
$result = monetdb_query('SELECT * FROM tables');
$cols = monetdb_num_fields($result);
for ($i = 0; $i < $cols; $i++) {
	print(monetdb_field_name($result, $i)."\t");
}
print("\n");
while ($row = @monetdb_fetch_row($result)) {
	for ($i = 0; $i < $cols; $i++) {
		print($row[$i]."\t");
	}
	print("\n");
}
?>
