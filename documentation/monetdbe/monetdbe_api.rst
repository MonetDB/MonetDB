MonetDBe API
============

MonetDBe is embedded version of the MonetDB server code.
The application is built around the libmonetdbe.so library. In line with the conventions of the MonetDB code base,
all instructions return a string which contains possible error messages encountered during the interpretation.
If all went well they return a NULL. Otherwise it the exception message thrown by the MonetDB kernel.

General considerations
----------------------

There can be a single :memory: database or local persistent database open at a time.
The database location should be passed as a full path. Relative paths are currently not supported.

As soon as you create a connection with another database, the content of the :memory: data store is lost.
MonetDB/e can also be used as a proxy to a remote database.
It is possible to have multiple such connections open.

Data Types
----------

The API wraps the internal data types to those more convenient for programming directly in C.

| MonetDBe type           | MonetDB internal type |
| ----------------------- | --------------------- |
| int8_t  |  bool|
| int8_t  |  int8_t|
| int16_t  |  int16_t|
| int32_t  |  int32_t|
| int64_t  |  int64_t|
| __int128  |  int128_t|
| float  |  float|
| double  |  double|
| char *  |  str|
| monetdbe_data_blob  |  blob|
| monetdbe_data_date  |  date|
| monetdbe_data_time  |  time|
| monetdbe_data_timestamp  |  timestamp|
| ----------------------- | --------------------- |

Connection and server options
-----------------------------

.. c:function:: int monetdbe_open(monetdbe_database *db, char *url, monetdbe_options *options)

    Initialize a monetdbe_database structure. The database of interest is denoted by an url and denote either ':memory:', /fullpath/directory,
    mapi:monetdb://company.nl:50000/database. The latter refers to a MonetDB server location.
    The :memory: and local path options lead to an exclusive lock on the database storage..
    Opening the same database multiple times concurrently is allowed, but opening another one concurrently will throw an error for now.
    There may be multiple connections to multiple MonetDB servers.

.. c:function:: int monetdbe_close(monetdbe_database db)

    Close the database handler and release the resources for another database connection.
    From here on the connection can not be used anymore to pass queries and any pending result set is cleaned up.
    Be aware that the content of an ':memory:' database is discarded.

Transaction management
----------------------

.. c:function:: char* monetdbe_get_autocommit(monetdbe_database db, int* result)

    Retrieve the current transaction mode, i.e. 'autocommit' or 'no-autocommit'

.. c:function:: char *monetdbe_set_autocommit(monetdbe_database db, int value)

    Set the auto-commit mode to either true or false.

.. c:function:: int monetdbe_in_transaction(monetdbe_database db);

    Boolean function to check if we are in a compound transaction.


Query execution
_______________

.. c:function:: char* monetdbe_query(monetdbe_database db, char* query, monetdbe_result** result, monetdbe_cnt* affected_rows)

    The main query interface to the database kernel. The query should obey the MonetDB SQL syntax. It returns a
    result set, i.e. a collection of columns in binary form and the number of rows affected by an update.

.. c:function:: char* monetdbe_result_fetch(monetdbe_result *mres, monetdbe_column** res, size_t column_index);

    Given a result set from a query obtain an individual column description.
    It contains the type and a C-array of values. The number of rows is part of the monetdbe_result structure.

.. c:function:: char* monetdbe_cleanup(monetdbe_database db, monetdbe_result *result);

    Remove the result set structure. The result is assigned NULL afterwards.

Query prepare, bind, execute
----------------------------

.. c:function:: char* monetdbe_prepare(monetdbe_database db, char *query, monetdbe_statement **stmt);

    Sent a query to the database server and prepare an execution plan. The plan is assigned to
    the monetdbe_statement structure for subsequent execution.

.. c:function:: char* monetdbe_bind(monetdbe_statement *stmt, void *data, size_t parameter_nr);

    Bind a local variable to a parameter in the prepared query structure. [TODO by pointer, do do you take a copy??]]

.. c:function:: char* monetdbe_execute(monetdbe_statement *stmt, monetdbe_result **result, monetdbe_cnt* affected_rows);

    When all parameters are bound, the statement is executed by the database server. An error is thrown if the
    number of parameters does not match.

.. c:function:: char* monetdbe_cleanup_statement(monetdbe_database db, monetdbe_statement *stmt);

    Remove the execution pland and all bound variables.

Database append
---------------

.. c:function:: char* monetdbe_append(monetdbe_database db, const char* schema, const char* table, monetdbe_result *result, size_t column_count);

    The result set obtained from any query can be assigned to a new database table.

Backup and restore
------------------
.. c:function:: char* monetdbe_dump_database(monetdbe_database db, char *backupfile);

    Dump a :memory: database as a collection of SQL statements on a local file

.. c:function:: char* monetdbe_dump_table(monetdbe_database db, const char *schema_name, const char *table_name, const char *backupfile);

    Dump a specific tables

.. c:function:: char* monetdbe_restore(monetdbe_database db, char *localfile);

    [TODO] Restore a SQL dump to initialize the ':memory:' case. This is similar  to loading a SQL script.

Miscellaneous
-------------

.. c:function:: char * monetdbe_error(monetdbe_database db)

    return the last error associated with the connection object.

Caveats and errors
------------------

  If the program with the monetdbe.so library is killed forcefully then there may be some garbage files left behind in the
  database directory. In particular, you may have to remove the .gdk_lock and uuid files.

