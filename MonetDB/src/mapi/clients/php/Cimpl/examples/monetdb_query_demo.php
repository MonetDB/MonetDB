<?php
# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2006 CWI.
# All Rights Reserved.
?>

<?php
# @author Fabian Groffen <Fabian.Groffen@cwi.nl>
?>

<html>
  <head>
    <title>MonetDB/SQL PHP demo</title>
  </head>

  <body>
<?php
dl("monetdb.so");

$db = monetdb_connect("sql", "localhost", 50000, "monetdb", "monetdb");

$tables = monetdb_query('SELECT name FROM tables');

?>
<form action="<?=$PHP_SELF?>" method="post" name="theform">
  <input type="hidden" name="postback" value="query" />
  <select name="table" onchange="document.theform.query.value='SELECT * FROM ' + document.theform.table.value;">
    <option value=""></option>
<?
	for ($i = 0; $line = @monetdb_fetch_assoc($tables); $i++) {
?>
	<option value="<?=htmlspecialchars($line['name'])?>">
	  <?=htmlspecialchars($line['name'])?>
	</option>
<?
	}
?>
  </select>
  <input type="text" name="query" value="<?=htmlspecialchars($_REQUEST['query'])?>" />
  <input type="submit" value="Query!" />
</form>
<?

	if ($_REQUEST['postback'] == "query") {
		print "<table>";
		// note there is no monetdb_escape_string!
		$result = monetdb_query(pg_escape_string($_REQUEST['query']));
		$cols = monetdb_num_fields($result);
		print "<tr style='background: Gray;'>";
		for ($i = 0; $i < $cols; $i++) {
			// Beware! monetdb_field_name expects resource, fieldnum
			// unlike stated in the documentation
			print "<td>".htmlspecialchars(monetdb_field_name($result, $i))."</td>";
		}
		print "</tr>";
		while ($row = @monetdb_fetch_row($result)) {
			print "<tr style='background: Silver;'>";
			for ($i = 0; $i < $cols; $i++) {
				print "<td>".htmlspecialchars($row[$i])."</td>";
			}
			print "</tr>";
		}
		print "</table>";
	}
?>

  </body>
</html>
<?
// vim:ts=4:sw=4:tw=0:noexpandtab
?>
