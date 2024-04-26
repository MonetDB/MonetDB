MonetDBe API
============

MonetDBe is embedded version of the MonetDB server code.
The application is built around the libmonetdbe.so library. In line with the conventions of the MonetDB code base,
all instructions return a string which contains possible error messages encountered during the interpretation.
If all went well they return a NULL. Otherwise it the exception message thrown by the MonetDB kernel.

General considerations
----------------------

There can be a single in-memory database or local persistent database open at a time.
The database location should be passed as a full path. Relative paths are currently not supported.

As soon as you create a connection with another database, the content of the in-memory data store is lost.
MonetDB/e can also be used as a proxy to a remote database.
It is possible to have multiple such connections open.

Data Types
----------

The API wraps the internal data types to those more convenient for programming directly in C.

=======================   =====================   ================
MonetDBe type             MonetDB internal type   MonetDB SQL type
=======================   =====================   ================
int8_t                    bit                     boolean
int8_t                    bte                     tinyint
int16_t                   sht                     smallint
int32_t                   int                     int
int64_t                   lng                     bigint
__int128                  hge                     hugeint
float                     flt                     real
double                    dbl                     double
char *                    str                     clob or varchar
monetdbe_data_blob        blob                    blob
monetdbe_data_date        date                    date
monetdbe_data_time        daytime                 time
monetdbe_data_timestamp   timestamp               timestamp
=======================   =====================   ================

Other SQL types such as timestamptz, timetz, day_interval, month_interval,
sec_interval, decimal, inet, json, url, uuid and geometry are currently not supported.

Connection and server options
-----------------------------

.. c:function:: int monetdbe_open(monetdbe_database *db, char *url, monetdbe_options *options)

    Initialize a monetdbe_database structure. The database of interest is denoted by an url and denote either ``NULL``, /fullpath/directory,
    mapi:monetdb://company.nl:50000/database. The latter refers to a
    MonetDB server location.  The value ``NULL`` denotes an in-memory database.
    The ``NULL`` and local path options lead to an exclusive lock on the database storage.
    Opening the same database multiple times concurrently is allowed, but opening another one concurrently will throw an error for now.
    There may be multiple connections to multiple MonetDB servers.
    Return: 0 for success, else errno: -1 (allocation failed) or -2 (error in db)

.. c:function:: int monetdbe_close(monetdbe_database db)

    Close the database handler and release the resources for another database connection.
    From here on the connection can not be used anymore to pass queries and any pending result set is cleaned up.
    Be aware that the content of an 'in-memory' database is discarded.
    Return: 0 for success, else errno: -2 (error in db)

Transaction management
----------------------

.. c:function:: char* monetdbe_get_autocommit(monetdbe_database db, int* result)

    Retrieve the current transaction mode, i.e. 'autocommit' or 'no-autocommit' in the result parameter.
    Return: NULL for success, else error message.

.. c:function:: char *monetdbe_set_autocommit(monetdbe_database db, int value)

    Set the auto-commit mode to either true or false.
    Return: NULL for success, else error message.

.. c:function:: int monetdbe_in_transaction(monetdbe_database db);

    Boolean function to check if we are in a compound transaction.


Query execution
_______________

.. c:function:: char* monetdbe_query(monetdbe_database db, char* query, monetdbe_result** result, monetdbe_cnt* affected_rows)

    The main SQL command interface to the database kernel. The query should obey the MonetDB SQL syntax.
    If the query produced a result set, it sets the result, i.e. a collection of columns in binary form.
    If the query changed data the affected_rows parameter will contain the number of rows affected.
    Return: NULL for success, else error message.

.. c:function:: char* monetdbe_result_fetch(monetdbe_result* mres, monetdbe_column** res, size_t column_index);

    Given a result set from a query obtain an individual column description.
    It contains the type and a C-array of values.
    The number of rows is part of the monetdbe_result structure.
    Return: NULL for success, else error message.

.. c:function:: char* monetdbe_cleanup_result(monetdbe_database db, monetdbe_result* result);

    Remove the result set structure. The result is assigned NULL afterwards.
    Return: NULL for success, else error message.

Query prepare, bind, execute
----------------------------

.. c:function:: char* monetdbe_prepare(monetdbe_database db, char *query, monetdbe_statement **stmt);

    Sent a query to the database server and prepare an execution plan. The plan is assigned to
    the monetdbe_statement structure for subsequent execution.
    Return: NULL for success, else error message.

.. c:function:: char* monetdbe_bind(monetdbe_statement *stmt, void *data, size_t parameter_nr);

    Bind a local variable to a parameter in the prepared query structure. [TODO by pointer, do do you take a copy??]]
    Return: NULL for success, else error message.

.. c:function:: char* monetdbe_execute(monetdbe_statement *stmt, monetdbe_result **result, monetdbe_cnt* affected_rows);

    When all parameters are bound, the statement is executed by the database server. An error is thrown if the
    number of parameters does not match.
    Return: NULL for success, else error message.

.. c:function:: char* monetdbe_cleanup_statement(monetdbe_database db, monetdbe_statement *stmt);

    Remove the execution plan and all bound variables.
    Return: NULL for success, else error message.

Metadata
--------

.. c:function:: char* monetdbe_get_columns(monetdbe_database db, const char* schema_name, const char *table_name, size_t *column_count, monetdbe_column **columns);

    Get column count and columns info of a specific table or view in a specific schema or current schema when schema_name is NULL.
    Return: NULL for success, else error message.

Table append
------------

.. c:function:: char* monetdbe_append(monetdbe_database db, const char* schema, const char* table, monetdbe_result *result, size_t column_count);

    Append the result set data obtained from a query to an existing table.
    This is a faster way than using an SQL: INSERT INTO . . .  SELECT . . .
    Return: NULL for success, else error message.

Backup and restore
------------------
.. c:function:: char* monetdbe_dump_database(monetdbe_database db, char *backupfile);

    Dump an in-memory database as a collection of SQL statements on a local file.
    Return: NULL for success, else error message.

.. c:function:: char* monetdbe_dump_table(monetdbe_database db, const char *schema_name, const char *table_name, const char *backupfile);

    Dump a specific table
    Return: NULL for success, else error message.

Miscellaneous
-------------

.. c:function:: char * monetdbe_error(monetdbe_database db)

    Return the last error message associated with the connection object. It can return NULL.

.. c:function:: const void * monetdbe_null(monetdbe_database dbhdl, monetdbe_types t)

    Return the internal NULL representation for the specific monetdbe type or NULL when the type is not found/supported.

.. c:function:: const char * monetdbe_get_mapi_port(void)

    Return the mapi port or NULL if not used.

.. c:function:: const char * monetdbe_version(void)

    Return the MonetDBe version.

.. c:function:: const char * monetdbe_load_extension(monetdbe_database dbhdl, char *module_name)

    Load the module with name, module_name. The lib_module_name.so (or ddl) should be in the applications current working directory.

Caveats and errors
------------------

  If the program with the monetdbe.so library is killed forcefully then there may be some garbage files left behind in the
  database directory. In particular, you may have to remove the .gdk_lock and uuid files.

