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
//
// +----------------------------------------------------------------------+
// | PHP Version 4                                                        |
// +----------------------------------------------------------------------+
// | Copyright (c) 1997-2002 The PHP Group                                |
// +----------------------------------------------------------------------+
// | This source file is subject to version 2.02 of the PHP license,      |
// | that is bundled with this package in the file LICENSE, and is        |
// | available at through the world-wide-web at                           |
// | http://www.php.net/license/2_02.txt.                                 |
// | If you did not receive a copy of the PHP license and are unable to   |
// | obtain it through the world-wide-web, please send a note to          |
// | license@php.net so we can mail you a copy immediately.               |
// +----------------------------------------------------------------------+
// | Author: Arjan Scherpenisse <A.C.Scherpenisse@CWI.nl                  |
// +----------------------------------------------------------------------+
//
// $Id$
//
// Database independent query interface definition for PHP's MonetDB
// extension.
//

//
// XXX legend:
//
// XXX ERRORMSG: The error message from the monetdb function should
//               be registered here.
//

require_once "DB/common.php";

class DB_monetdb extends DB_common
{
    // {{{ properties

    var $connection;
    var $phptype, $dbsyntax;
    var $prepare_tokens = array();
    var $prepare_types = array();
    var $num_rows = array();
    var $transaction_opcount = 0;
    var $autocommit = true;
    var $fetchmode = DB_FETCHMODE_ORDERED; /* Default fetch mode */
    var $_db = false;

    // }}}
    // {{{ constructor

    /**
     * DB_monetdb constructor.
     *
     * @access public
     */

    function DB_monetdb()
    {
        $this->DB_common();
        $this->phptype = 'monetdb';
        $this->dbsyntax = 'sql';
        $this->features = array(
            'prepare' => false,
            'transactions' => true,
            'limit' => 'alter'
        );

        $this->errorcode_map = array(-1 => DB_ERROR_SYNTAX);
        /*
            1004 => DB_ERROR_CANNOT_CREATE,
            1005 => DB_ERROR_CANNOT_CREATE,
            1006 => DB_ERROR_CANNOT_CREATE,
            1007 => DB_ERROR_ALREADY_EXISTS,
            1008 => DB_ERROR_CANNOT_DROP,
            1046 => DB_ERROR_NODBSELECTED,
            1050 => DB_ERROR_ALREADY_EXISTS,
            1051 => DB_ERROR_NOSUCHTABLE,
            1054 => DB_ERROR_NOSUCHFIELD,
            1062 => DB_ERROR_ALREADY_EXISTS,
            1064 => DB_ERROR_SYNTAX,
            1100 => DB_ERROR_NOT_LOCKED,
            1136 => DB_ERROR_VALUE_COUNT_ON_ROW,
            1146 => DB_ERROR_NOSUCHTABLE,
            1048 => DB_ERROR_CONSTRAINT,
        ); */
    }

    // }}}

    // {{{ connect()

    /**
     * Connect to a database and log in as the specified user.
     *
     * @param $dsn the data source name (see DB::parseDSN for syntax)
     * @param $persistent (optional) whether the connection should
     *        be persistent
     * @access public
     * @return int DB_OK on success, a DB error on failure
     */

    function connect($dsninfo, $persistent = false)
    {
        if (!PEAR::loadExtension('monetdb'))
            return $this->raiseError(DB_ERROR_EXTENSION_NOT_FOUND);

        $this->dsn = $dsninfo;

        $dbhost = $dsninfo['hostspec'] ? $dsninfo['hostspec'] : 'localhost';
        if (!empty($dsninfo['port'])) {
            $port = (int)$dsninfo['port'];
        } else {
            $port = 50000;
        }
        if (!empty($dsninfo['dbsyntax']) && $dsninfo['dbsyntax']!='monetdb') {
            $lang = $dsninfo['dbsyntax'];
            $this->dbsyntax = $dsninfo['dbsyntax'];
        } else {
            $lang = "sql";
        }
	  
        $user = ($u=$dsninfo['username'])?$u:"monetdb";
        $pw = ($p=$dsninfo['password'])?$p:"monetdb";

        //$connect_function = $persistent ? 'monetdb_pconnect' : 'monetdb_connect';
        $connect_function = 'monetdb_connect';

        @ini_set('track_errors', true);
        $conn = @$connect_function($lang, $dbhost, $port, $user, $pw);
        @ini_restore('track_errors');
	
        if (empty($conn)) {
            if (($err = @monetdb_error()) != '') {
                return $this->raiseError(DB_ERROR_CONNECT_FAILED, null, null,
                                         null, $err);
            } elseif (empty($php_errormsg)) {
                return $this->raiseError(DB_ERROR_CONNECT_FAILED);
            } else {
                return $this->raiseError(DB_ERROR_CONNECT_FAILED, null, null,
                                         null, $php_errormsg);
            }
        }

    
        $this->connection = $conn;
        $this->autoCommit($this->autocommit);

        return DB_OK;
    }

    // }}}
    // {{{ disconnect()

    /**
     * Log out and disconnect from the database.
     *
     * @access public
     *
     * @return bool TRUE on success, FALSE if not connected.
     */
    function disconnect()
    {
        $ret = monetdb_close($this->connection);
        $this->connection = null;
        return $ret;
    }

    // }}}
    // {{{ simpleQuery()

    /**
     * Send a query to monetdb and return the results as a monetdb resource
     * identifier.
     *
     * @param the SQL query
     *
     * @access public
     *
     * @return mixed returns a valid monetdb result for successful SELECT
     * queries, DB_OK for other successful queries.  A DB error is
     * returned on failure.
     */
    function simpleQuery($query)
    {
        $this->last_query = $query;
        $query = $this->modifyQuery($query);
        $ismanip = DB::isManip($query);
        if (!$this->autocommit && $ismanip) {
            $this->transaction_opcount++;
        }
        if (defined("MONETDB_DEBUG") && MONETDB_DEBUG) {
            echo "<pre>$query</pre>";
        }
        $result = @monetdb_query($query, $this->connection);

        if (!$result) {
            return $this->monetdbRaiseError();
        }
        if (!$ismanip && is_resource($result)) {
            $numrows = $this->numrows($result);
            if (is_object($numrows)) {
                return $numrows;
            }
            $this->num_rows[$result] = $numrows;
            return $result;
        } elseif (DB::isManip($query)){
            $this->affected = @monetdb_affected_rows($result);
	}
        return DB_OK;
    }

    // }}}

    // {{{ fetchRow()

    /**
     * Fetch and return a row of data (it uses fetchInto for that)
     * @param $result monetdb result identifier
     * @param   $fetchmode  format of fetched row array
     * @param   $rownum     the absolute row number to fetch
     *
     * @return  array   a row of data, or false on error
     */
    function fetchRow($result, $fetchmode = DB_FETCHMODE_DEFAULT, $rownum=null)
    {
        if ($fetchmode == DB_FETCHMODE_DEFAULT) {
            $fetchmode = $this->fetchmode;
        }
        $res = $this->fetchInto ($result, $arr, $fetchmode, $rownum);
        if ($res !== DB_OK) {
            return $res;
        }
        return $arr;
    }

    // }}}
    // {{{ fetchInto()

    /**
     * Fetch a row and insert the data into an existing array.
     *
     * @param $result monetdb result identifier
     * @param $arr (reference) array where data from the row is stored
     * @param $fetchmode how the array data should be indexed
     * @param   $rownum the row number to fetch
     * @access public
     *
     * @return int DB_OK on success, a DB error on failure
     */
    function fetchInto($result, &$arr, $fetchmode, $rownum=null)
    {
        if ($rownum !== null) {
            if (!@monetdb_data_seek($result, $rownum)) {
                return null;
            }
        }
	
        if ($fetchmode & DB_FETCHMODE_ASSOC) {
            $arr = monetdb_fetch_assoc($result);
        } else {
            $arr = monetdb_fetch_row($result);
        } // other fetchmodes are handled internally by db_common.

        if (!$arr) {
            $errno = @monetdb_errno($this->connection);
            if (!$errno) {
                return NULL;
            }
            return $this->monetdbRaiseError($errno);
        }
        return DB_OK;
    }

    // }}}
    // {{{ freeResult()

    /**
     * Free the internal resources associated with $result.
     *
     * @param $result monetdb result identifier or DB statement identifier
     *
     * @access public
     *
     * @return bool TRUE on success, FALSE if $result is invalid
     */
    function freeResult($result)
    {
        if (is_resource($result)) {
	/* help help
            unset($this->row[(int)$result]);
            unset($this->num_rows[(int)$result]);
	*/
            $this->affected = 0;
	    return true;
            // return @monetdb_free_result($result);
        }
        return false;
    }

    // }}}
    // {{{ numCols()

    /**
     * Get the number of columns in a result set.
     *
     * @param $result monetdb result identifier
     *
     * @access public
     *
     * @return int the number of columns per row in $result
     */
    function numCols($result)
    {
        $cols = @monetdb_num_fields($result);
        if ($cols === null) {
            return $this->monetdbRaiseError();
        }

        return $cols;
    }

    // }}}
    // {{{ numRows()

    /**
     * Get the number of rows in a result set.
     *
     * @param $result monetdb result identifier
     *
     * @access public
     *
     * @return int the number of rows in $result
     */
    function numRows($result)
    {
        $rows = @monetdb_num_rows($result);
        if ($rows === null) {
            return $this->monetdbRaiseError();
        }
        return $rows;
    }

    // }}}
    // {{{ autoCommit()

    /**
     * Enable/disable automatic commits
     */
    function autoCommit($onoff = false)
    {
        if ($onoff === true) {
        	@monetdb_setAutocommit($this->connection, 1);
        } else {
        	@monetdb_setAutocommit($this->connection, 0);
        }
        $this->autocommit = ($onoff===true) ? true : false;
        $this->transaction_opcount = 0;
        return DB_OK;
    }

    // }}}
    // {{{ commit()

    /**
     * Commit the current transaction.
     */
    function commit()
    {
        $result = @monetdb_query('COMMIT;', $this->connection);
        if (!$result) {
            $this->transaction_opcount = 0;
            return $this->monetdbRaiseError();
        }
        if ($this->autocommit) 
		$this->autocommit = false;
        $this->transaction_opcount = 0;
        return DB_OK;
    }

    // }}}
    // {{{ rollback()

    /**
     * Roll back (undo) the current transaction.
     */
    function rollback()
    {
	if ( $this->autocommit === true) 
		return DB_OK;

        $result = @monetdb_query('ROLLBACK;', $this->connection);
        if (!$result) {
            $this->transaction_opcount = 0;
            return $this->monetdbRaiseError();
        }
        $this->transaction_opcount = 0;
        return DB_OK;
    }

    // }}}
    // {{{ affectedRows()

    /**
     * Gets the number of rows affected by the data manipulation
     * query.  For other queries, this function returns 0.
     *
     * @return number of rows affected by the last query
     */

    function affectedRows()
    {
        if (DB::isManip($this->last_query)) {
	    $result = $this->affected;
	} else {
	    $result = 0;
        }
        return $result;
     }

    // }}}
    // {{{ errorNative()

    /**
     * Get the native error code of the last error (if any) that
     * occured on the current connection.
     *
     * @access public
     *
     * @return int native monetdb error code
     */

    function errorNative()
    {
        return @monetdb_errno($this->connection);
    }

    // }}}
    // {{{ nextId()

    /**
     * Get the next value in a sequence.  We emulate sequences
     * for monetdb.  Will create the sequence if it does not exist.
     *
     * @access public
     *
     * @param string $seq_name the name of the sequence
     *
     * @param bool $ondemand whether to create the sequence table on demand
     * (default is true)
     *
     * @return mixed a sequence integer, or a DB error
     */
    function nextId($seq_name, $ondemand = true)
    {
        $seqname = $this->getSequenceName($seq_name);
        do {
            $repeat = 0;
            $this->pushErrorHandling(PEAR_ERROR_RETURN);
            $result = $this->query("UPDATE ${seqname} SET id=id+1");
            $this->popErrorHandling();
            if ($result == DB_OK) {
                /** COMMON CASE **/
                $id = $this->getOne("SELECT id FROM ${seqname}");
                if ($id !== null) {
                    return $id;
                }
                /** EMPTY SEQ TABLE **/
                // add the default value
                $result = $this->query("INSERT INTO ${seqname} VALUES (1)");
                if (DB::isError($result)) {
                    return $this->raiseError($result);
                }

                // We know what the result will be, so no need to try again
                return 1;

            /** ONDEMAND TABLE CREATION **/
            } elseif ($ondemand && DB::isError($result)) {
                $result = $this->createSequence($seq_name);

                // Since createSequence initializes the ID to be 1,
                // we do not need to retrieve the ID again (or we will get 2)
                if (DB::isError($result)) {
                    return $this->raiseError($result);
                } else {
                    // First ID of a newly created sequence is 1
                    return 1;
                }
            }
        } while ($repeat);
            
        return $this->raiseError($result);
    }

    // }}}
    // {{{ createSequence()

    function createSequence($seq_name)
    {
        $seqname = $this->getSequenceName($seq_name);
        $res = $this->query("CREATE TABLE ${seqname} ".
                            '(id INTEGER NOT NULL,'.
                            ' PRIMARY KEY(id))');
        if (DB::isError($res)) {
            return $res;
        }
        // insert yields value 1, nextId call will generate ID 2
        return $this->query("INSERT INTO ${seqname} VALUES(1)");
    }

    // }}}
    // {{{ dropSequence()

    function dropSequence($seq_name)
    {
        $seqname = $this->getSequenceName($seq_name);
        return $this->query("DROP TABLE ${seqname}");
    }

    // }}}

    // {{{ modifyQuery()

    function modifyQuery($query, $subject = null)
    {
        return $query;
    }

    // }}}
    // {{{ modifyLimitQuery()

    function modifyLimitQuery($query, $from, $count)
    {
        $query = $query . " LIMIT $count";
	if ($from > 0)
        	$query = $query . " OFFSET $from";
        return $query;
    }

    // }}}
    // {{{ monetdbRaiseError()

    function monetdbRaiseError($errno = null)
    {
        if ($errno === null) {
            $errno = $this->errorCode(monetdb_errno($this->connection));
        }
        if (defined("MONETDB_DEBUG") && MONETDB_DEBUG) {
            echo "<p><strong>MonetDB Error ($errno) raised:</strong> " . @monetdb_error($this->connection);
        }
        return $this->raiseError($errno, null, null, null,
                                 @monetdb_errno($this->connection) . " ** " .
                                 @monetdb_error($this->connection));
    }

    // }}}
    // {{{ tableInfo()
    /**
     * Returns information about a table or a result set.
     *
     * NOTE: only supports 'table' and 'flags' if <var>$result</var>
     * is a table name.
     *
     * @param object|string  $result  DB_result object from a query or a
     *                                string containing the name of a table
     * @param int            $mode    a valid tableInfo mode
     * @return array  an associative array with the information requested
     *                or an error object if something is wrong
     * @access public
     * @internal
     * @see DB_common::tableInfo()
     */
    function tableInfo($result, $mode = null) 
    {
	/* TODO 
		the 'len' field should hold the types maximum length, but
		we return the maximum length in the column (values).
		
		We do not return primary/foreing key flags. Could be 
		added in the string case.
 	 */
	$id = $result;
        if (is_object($result) && isset($result->result)) {
            /*
             * Probably received a result object.
             * Extract the result resource identifier.
             */
            $id = $result->result;
	} elseif (is_string($result)) {
            $res = array();
	    $tmp = $this->getAll("select tables.name as \"table\" ,columns.name,columns.type, case columns.\"null\" when true then 'null' else 'not_null' end as flags, type_digits as len,".
				 "number as number from tables inner join columns on tables.id=columns.table_id " .
				 "where tables.name='${result}' order by number", DB_FETCHMODE_ASSOC);
            $res['num_fields']= sizeof($tmp);
	    foreach ( $tmp as $tmprow ) {
                $row = array();
		foreach( $tmprow as $key => $val) {
			if ($key == 'number') {
                		if ($mode & DB_TABLEINFO_ORDER) {
                    			$res['order'][$row['name']] = $val;
                		}
                		if ($mode & DB_TABLEINFO_ORDERTABLE) {
                    			$res['ordertable'][$row['table']][$row['name']] = $val;
                		}
			} else {
				$row[$key] = $val;
			}
		}
                $res[] = $row;
	    }
	    return $res;
	} 
        if (is_resource($id)) {
	    $count = monetdb_num_fields($id);
            $res = array();
            $res['num_fields']= $count;
            for ($i=0; $i<$count; $i++) {
                $row = array();
		$row['table'] = monetdb_field_table($id, $i);
                $row['name'] = monetdb_field_name($id, $i);
                $row['type'] = monetdb_field_type($id, $i);
                $row['len']  = monetdb_field_len($id, $i);
                if ($mode & DB_TABLEINFO_ORDER) {
                    $res['order'][$row['name']] = $i;
                }
                if ($mode & DB_TABLEINFO_ORDERTABLE) {
                    $res['ordertable'][$row['table']][$row['name']] = $i;
                }
                $res[] = $row;
            }
        } else {
            return $this->raiseError(DB_ERROR_NOT_CAPABLE);
	}
	
	return $res;
    }
    // }}}
    // {{{ getTablesQuery()

    /**
    * Returns the query needed to get some backend info
    * @param string $type What kind of info you want to retrieve
    * @return string The SQL query string
    */
    function getSpecialQuery($type)
    {
        switch ($type) {
            case 'tables':
                $sql = "SELECT name FROM ptables WHERE istable=true and system=false;";
                break;
            case 'views':
	        $sql = "SELECT name FROM ptables WHERE istable=false and system=false;";
                break;
            case 'users':
                $sql = "SELECT name FROM users;";
                break;
            case 'databases':
	        // For now (v4.3.19), only a single database is supported...
                return DB_ERROR_NOT_CAPABLE;

            default:
                return null;
        }
        return $sql;
    }

    // }}}

    // {{{ nextResult()

    /**
     * Move the internal monetdb result pointer to the next available result
     *
     * @param a valid monetdb result resource
     *
     * @access public
     *
     * @return true if a result is available otherwise return false
     */
    function nextResult($result)
    {
        return @monetdb_next_result($result)==1;
    }

    // }}}


    // {{{ quote()

    /**
     * @deprecated  Deprecated in release 1.6.0
     * @internal
     */
    function quote($str) {
        return $this->quoteSmart($str);
    }

    // }}}

    // {{{ quoteSmart()

    /**
     * Format input so it can be safely used in a query
     *
     * @param mixed $in  data to be quoted
     *
     * @return mixed Submitted variable's type = returned value:
     *               + null = the string <samp>NULL</samp>
     *               + boolean = string <samp>TRUE</samp> or <samp>FALSE</samp>
     *               + integer or double = the unquoted number
     *               + other (including strings and numeric strings) =
     *                 the data escaped according to MySQL's settings
     *                 then encapsulated between single quotes
     *
     * @internal
     */
    function quoteSmart($in)
    {
        if (is_int($in) || is_double($in)) {
            return $in;
        } elseif (is_bool($in)) {
            return $in ? 'TRUE' : 'FALSE';
        } elseif (is_null($in)) {
            return 'NULL';
        } else {
            return "'" . $this->escapeSimple($in) . "'";
        }
    }
    // }}}

    /**
     * Escape a string according to the current DBMS's standards
     *
     * @param string $str  the string to be escaped
     *
     * @return string  the escaped string
     *
     * @internal
     */
    function escapeSimple($str) {
        #return str_replace("'", "''", str_replace('\\', '\\\\', @monetdb_escape_string($str)));
        return str_replace("'", "''", @monetdb_escape_string($str));
    }

    // {{{ quoteIdentifier()

    /**
     * Quote a string so it can be safely used as a table / column name
     *
     * Quoting style depends on which database driver is being used.
     *
     * @param string $str  identifier name to be quoted
     *
     * @return string  quoted identifier string
     *
     * @since 1.6.0
     * @access public
     */
    function quoteIdentifier($str)
    {
        return '"' . str_replace('"', '\\"', $str) . '"';
    }

    // }}}
}

?>
