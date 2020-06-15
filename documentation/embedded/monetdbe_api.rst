MonetDBe API
============

The application built around the MonetDBe library. All instructions return a string which contains possible
errors encountered during the interpretation. If all went well they return a NULL.

General considerations
---------------------
[TODO!] An embedded application can have multiple database open concurrently. This enables easy
transport of data from one to another, and forms the basis for distributed query processing.

Data Types
---------------
The API wraps the internal data types to those more convenient for programming directly in C.


| MonetDBe type   | MonetDB internal type |
| --------------- | --------------------- |
|int8_t  |  bool|
|int8_t  |  int8_t|
|int16_t  |  int16_t|
|int32_t  |  int32_t|
|int64_t  |  int64_t|
|__int128  |  int128_t|
|size_t  |  size_t      [TODO really needed ?]|
|float  |  float|
|double  |  double|
|char *  |  str|
|monetdbe_data_blob  |  blob|
|monetdbe_data_date  |  date|
|monetdbe_data_time  |  time|
|monetdbe_data_timestamp  |  timestamp|

Data Types
----------
[TODO]The MonetDBe interface uses a few structures you need to be aware of.

Connection and server options
---------------------------------
.. c:char* monetdbe_open(char* dbname, bool sequential);

    Initialize access to a database component. The dbname is an URL and denote either ':memory:', /path/directory,
    mapi:monetdb://company.nl:50000/database. The latter refers to a MonetDB server location.
    The :memory: and local path options lead to an exclusive lock. There may be multiple connections to the MonetDB servers.
    
    The sequential argument indicates [WHAT]


.. c:char* monetdbe_close(monetdbe_connection *conn);

    From here on the connection can not be used anymore to pass queries or any pending result set is cleaned up.
    Be aware that the content of an ':inmemory:' database is discarded.

Transaction management
---------------------
.. c: char* monetdbe_get_autocommit(monetdbe_connection conn, int* result);

    Retrieve the current transaction mode, i.e. 'autocommit' or 'no-autocommit' [TODO ?]

.. c:char *monetdbe_set_autocommit(monetdbe_connection conn, int value);

    Set the auto-commit mode to either true or false. An error is raised when you attempt
    to ...??


Query execution
______________
.. c:char* monetdbe_query(monetdbe_connection conn, char* query, monetdbe_result** result, monetdbe_cnt* affected_rows)

    The main query interface to the database kernel. The query should obey the MonetDB syntax. It returns a nested
    structure with the result set in binary form and the number of rows in the result set or affected by an update.

.. c:char* monetdbe_prepare(monetdbe_connection conn, char *query, monetdbe_statement **stmt);

    Sent a query to the database server and prepare an execution plan. Its arguments should be passed when called.

.. c:char* monetdbe_bind(monetdbe_statement *stmt, void *data, size_t parameter_nr);

    Bind a local variable to a parameter in the prepared query structure. [TODO by pointer, do do you take a copy??]]

.. c:char* monetdbe_execute(monetdbe_statement *stmt, monetdbe_result **result, monetdbe_cnt* affected_rows);

    When all parameters are bound, the statement is executed by the database server. An error is thrown if the
    number of parameters does not match. 

.. c: char* monetdbe_cleanup_statement(monetdbe_connection conn, monetdbe_statement *stmt);

    Remove the execution pland and all bound variables.


Database insert
--------------

.. c: char* monetdbe_append(monetdbe_connection conn, const char* schema, const char* table, monetdbe_column **input, size_t column_count);

    The result set obtained from any query can be assigned to a new database table. [TODO which schema...]


Result set
----------
.. c: char* monetdbe_result_fetch(monetdbe_connection conn, monetdbe_result *mres, monetdbe_column** res, size_t column_index);

    Given a result set from a query obtain its structure as a collection of column descriptors. [TODO]

.. c: char* monetdbe_backup(monetdbe_connection conn, char *localfile);

    [TODO] Dump an :inmemory: database as a collection of SQL statements on a local file

.. c: char* monetdbe_restore(monetdbe_connection conn, char *localfile);

    [TODO] Restore a SQL dump to initialize the ':inmemory:' case.

Schema inspection
----------------

.. c:char* monetdbe_get_table(monetdbe_connection conn, monetdbe_table** table, const char* schema_name, const char* table_name);

    Retrieve the structure of the schema.table  into the monetdbe_table structure.

.. c:char* monetdbe_get_columns(monetdbe_connection conn, const char* schema_name, const char *table_name, size_t *column_count, char ***column_names, int **column_types);

    Retrieve the details of each column.


Miscellaneous
-------------

.. c:bool  monetdbe_is_initialized(void)

    Simple function to check if MonetDBe has already been started. [TODO For a remote connection
    it behaves like a 'ping', telling if the remote server is available for interactions.]

.. c:char * monetdbe_error(monetdbe_connection conn)

    [TODO] return the last error associated with the connection object.
