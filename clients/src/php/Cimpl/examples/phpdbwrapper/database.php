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
# Portions created by CWI are Copyright (C) 1997-2007 CWI.
# All Rights Reserved.
?>

<?
/**
 * database.php - an abstraction layer for a data storage engine
 * Monica Manzano Hidalgo
 * Fabian Groffen
 *
 * This file should be included by all pages that want to make use of a
 * database connection.  Based on the configuration file, the right
 * database functions are being issued through the wrapper functions of
 * this module.
 */

// deal with hacking attempts
if (!defined('LEGAL_CALL'))
	die("Nothing to see here, move along!");

// we need user configuration, so let's load it if we haven't yet
require_once("config.php");

// if we don't have a configuration, we bail out here
if (!defined('DATABASE'))
	die("Configuration error: no database given");

// PHP does not support function overloading, nor is it possible to
// undefine or redefine previously-declared functions.  So, we define
// the wrapper functions here, and just switch on the database type all
// the time.  We more or less define the interface for the database
// layer here.

$db_resource = FALSE;

/**
 * Opens a connection to a server (the link identifier will be located
 * in the global variable $db_resource).  The DBMS which is going to be
 * used will be specified in the value defined in DATABASE.  In some
 * cases, e.g. MySQL, it is needed to connect to the server and then
 * select a certain database, while others can select the database
 * during connect.
 * 
 * @return TRUE on success or FALSE on failure 
 */
function db_connect() {
	global $db_conf, $db_resource;

	switch (DATABASE) {
		case 'mysql':
			$db_resource = mysql_connect(
					$db_conf['mysql']['host'].":".$db_conf['mysql']['port'],
					$db_conf['mysql']['user'],
					$db_conf['mysql']['pass']);
			if (!mysql_select_db($db_conf['mysql']['dbname'], $db_resource)) {
				mysql_close($db_resource);
				$db_resource = FALSE;
			}
		break;
		case 'pgsql':
			$con  = "host=".$db_conf['pgsql']['host']." ";
			$con .= "port=".$db_conf['pgsql']['port']." ";
			$con .= "dbname=".$db_conf['pgsql']['dbname']." ";
			$con .= "user=".$db_conf['pgsql']['user']." ";
			$con .= "password=".$db_conf['pgsql']['pass'];
			$db_resource = pg_connect($con);
		break;
		case 'monetdb4':
		case 'monetdb5':
			if (!extension_loaded('monetdb')) {
				$prefix = (PHP_SHLIB_SUFFIX == 'dll') ? 'php_' : '';
				// note: PHP5 says it deprecates this, but I can't find
				//       how to make PHP5 happy...
				dl($prefix.'monetdb.'.PHP_SHLIB_SUFFIX) or
					die("Unable to load monetdb module!");
			}
			if (defined('PERSISTENT') && constant('PERSISTENT') == TRUE )
			{
				$db_resource = monetdb_pconnect(
					'sql',
					$db_conf[DATABASE]['host'],
					$db_conf[DATABASE]['port'],
					$db_conf[DATABASE]['user'],
					$db_conf[DATABASE]['pass']
				);
			}
			else
			{
				$db_resource = monetdb_connect(
					'sql',
					$db_conf[DATABASE]['host'],
					$db_conf[DATABASE]['port'],
					$db_conf[DATABASE]['user'],
					$db_conf[DATABASE]['pass']
				);
			}
			// TODO: do something with the database, perhaps create a
			//       warning stack...
		break;
		default:
			die("db_connect: not implemented for database ".DATABASE);
	}

	return(db_connected());
}

/**
 * Disconnects the connection to the database.
 */
function db_disconnect() {
	global $db_resource;
	switch (DATABASE) {
		case 'mysql':
			$ret = mysql_close($db_resource);
			$db_resource = FALSE;
		break;
		case 'pgsql':
			$ret = pg_close($db_resource);
			$db_resource = FALSE;
		break;
		case 'monetdb4':
		case 'monetdb5':
			if (defined(PERSISTENT) && constant('PERSISTENT') == FALSE)
				$ret = monetdb_close($db_resource);
			else
				$ret = TRUE;
			$db_resource = FALSE;
		break;
		default:
			die("db_disconnect: not implemented for database ".DATABASE);
	}

	return($ret);
}

/**
 * Returns whether a connection to the database has been made, and has
 * not been closed yet.  Note that this function doesn't guarantee that
 * the connection is alive or usable.
 *
 * @return TRUE if there is a connection, FALSE otherwise
 */
function db_connected() {
	global $db_resource;

	return($db_resource != FALSE);
}

/**
 * Executes the given query on the database.
 *
 * @param query the SQL query to execute
 * @return a query handle or FALSE on failure
 */
function db_query($query) {
	global $db_resource;
	
	switch (DATABASE) {
		case 'mysql':
			return(mysql_query($query, $db_resource));

		case 'pgsql':
			return(pg_query($db_resource, $query));

		case 'monetdb4':
		case 'monetdb5':
			return(monetdb_query($db_resource, $query));

		default:
			die("db_query: not implemented for database ".DATABASE);
	}
}

/**
 * Returns an associative array containing the column names as keys, and
 * column values as value.
 *
 * @param resource the query handle
 * @return the next row in the query result as associative array or
 *         FALSE if no more rows exist
 */
function db_fetch_assoc($resource) {
	switch (DATABASE) {
		case 'mysql':
			return(mysql_fetch_assoc($resource));

		case 'pgsql':
			return(pg_fetch_assoc($resource));
			
		case 'monetdb4':
		case 'monetdb5':
			return(monetdb_fetch_assoc($resource));
			
		default:
			die("db_fetch_assoc: not implemented for database ".DATABASE);
	}
}

/**
 * Returns the result in the given query resource as object.  Column
 * names become members of the object through which the column values
 * can be retrieved.
 *
 * @param resource the query handle
 * @return the query result as object or FALSE if there are no more rows
 */
function db_fetch_object($resource) {
	switch (DATABASE) {
		case 'mysql':
			return(mysql_fetch_object($resource));

		case 'pgsql':
			return(pg_fetch_assoc($resource));
			
		case 'monetdb4':
		case 'monetdb5':
			return(monetdb_fetch_object($resource));
			
		default:
			die("db_fetch_object: not implemented for database ".DATABASE);
	}
}

/**
 * Returns the number of rows in the query result.
 *
 * @param resource the query resource
 * @return the number of rows in the result
 */
function db_num_rows($resource) {
	switch (DATABASE) {
		case 'mysql':
			return(mysql_num_rows($resource));

		case 'pgsql':
			return(pg_num_rows($resource));
			
		case 'monetdb4':
		case 'monetdb5':
			return(monetdb_num_rows($resource));
			
		default:
			die("db_num_rows: not implemented for database ".DATABASE);
	}
}

/**
 * Returns the number of affected rows for an UPDATE, INSERT or DELETE
 * query.  The number of affected rows typically is 1 for INSERT
 * queries.
 *
 * @param resource the query resource
 * @return the number of affected rows
 */
function db_affected_rows($resource) {
	switch (DATABASE) {
		case 'mysql':
			return(mysql_affected_rows($resource));

		case 'pgsql':
			return(pg_affected_rows($resource));
			
		case 'monetdb4':
		case 'monetdb5':
			return(monetdb_affected_rows($resource));
			
		default:
			die("db_affected_rows: not implemented for database ".DATABASE);
	}
}

/**
 * Returns the last error reported by the database.
 *
 * @return the last error
 */
function db_last_error() {
	switch (DATABASE) {
		case 'mysql':
			return(mysql_error());

		case 'pgsql':
			return(pg_last_error());
			
		case 'monetdb4':
		case 'monetdb5':
			return(monetdb_last_error());
			
		default:
			die("db_last_error: not implemented for database ".DATABASE);
	}
}

/**
 * Generates the next id in the sequence $seq
 *
 * @param seq Sequence (implemented as SEQUENCE or TABLE) whose next
 * value we want to retrieve
 * @return the ID of the last tuple inserted 
 */
function db_insert_id($seq) {
	switch (DATABASE) {
		case 'mysql':
			//We simulate a SEQUENCE with a table 
			$insert = "INSERT INTO ".$seq." VALUES();";
			@db_query($insert) or die("Error while dealing with the
			database()");
			
			//We let PHP make sure that the last inserted id retrieved
			//is the correct one. Otherwise, we would use the query
			//"SELECT last_insert_id()", which is more risky because we
			//could have another INSERT operation in between this INSERT
			//operation above and the SELECT operation.
			return(mysql_insert_id());	
	
		case 'pgsql':
//			return(pg_last_error());
			
		case 'monetdb4':
		case 'monetdb5':
			$select = "SELECT NEXT VALUE FOR
						".db_quote_ident($seq).";";
			$result = @db_query($select) or die("Error while dealing
			with the database(4)");
			$row = db_fetch_assoc($result);
			return($row[$seq]);
				
		default:
			die("db_insert_id: not implemented for database ".DATABASE);
	}
}

/**
 * Returns a 'quoted identifier' suitable for the current DBMS in use.
 * This utility function can be used in queries to for instance quote
 * names of tables of columns that otherwise would be a mistaken for a
 * keyword.
 * NOTE: the given string is currently not checked for validity, hence
 *       the output of this function may be an invalid identifier.
 *
 * @param str the identifier to quote
 * @return the quoted identifier
 */
function db_quote_ident($str) {
	switch (DATABASE) {
		case 'mysql':
			return('`'.$str.'`');

		case 'pgsql':
		case 'monetdb4':
		case 'monetdb5':
			return('"'.$str.'"');

		default:
			die("db_quote_ident: not implemented for database ".DATABASE);
	}
}

/**
 * Returns an 'escaped' string that can be used for instance within
 * single quotes to represent a CHARACTER VARYING object in SQL.
 *
 * @param str the string to escape
 * @return the escaped string
 */
function db_escape_string($str) {
	switch (DATABASE) {
		case 'mysql':
			return(mysql_escape_string($str));

		case 'pgsql':
			return(pg_escape_string($str));

		case 'monetdb4':
		case 'monetdb5':
			return(monetdb_escape_string($str));

		default:
			die("db_escape_string: not implemented for database ".DATABASE);
	}
}

?>
