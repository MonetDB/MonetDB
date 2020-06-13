MonetDBe API
============

The application built around the MonetDBe library. All instructions return a string which contains possible
errors encountered during the interpretation. If all went well they return a NULL.

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
|monetdb_data_blob  |  blob|
|monetdb_data_date  |  date|
|monetdb_data_time  |  time|
|monetdb_data_timestamp  |  timestamp|

Data Types
----------
The MonetDBe interface uses a few structures you need to be aware of.
Data Types
----------
The MonetDBe interface uses a few structures you need to be aware of.
See the monetdbe.h file

Connection and server options
---------------------------------
.. c:char* monetdbe_startup(char* dbdir, bool sequential);

    Initialize the database component. The dbname identifies the path to the directory holding the persistent data.
    It will be locked for exclusive access. An in-memory only database is created by passing a NULL argument.
    
    The sequential argument indicates [WHAT]

[TODO, consider this seriously!  ]
.. c:char* monetdbe_connect_mapi(char* dbdir, char *username, char *password, char *host, int port);

    A simplified wrapper around the official MAPI protocol. It allows for development of the application
    using a local directory and simply replacing the startup call with a mapi_connect.

.. c:char* monetdb_connect(monetdb_connection *conn);

    Create a separate connection channel with the database. Don't use it in parallel threads to submit
    queries, because they would become mangled and even could hang your application. 
    [TODO protect against it]


.. c:char* monetdb_disconnect(monetdb_connection *conn);

    From here on the connection can not be used anymore to pass queries or any pending result set is cleaned up.

.. c:char* monetdb_shutdown(void);

    Force a shutdown of the database server. This will not shutdown your application, you can simply restart
    the database server. However, be aware that the content of an ':inmemory:' database is discarded at shutdown.

Transaction management
---------------------
.. c: char* monetdb_get_autocommit(monetdb_connection conn, int* result);

    Retrieve the current transaction mode, i.e. 'autocommit' or 'no-autocommit' [TODO ?]

.. c:char *monetdb_set_autocommit(monetdb_connection conn, int value);

    Set the auto-commit mode to either true or false. An error is raised when you attempt
    to ...??


Query execution
______________
.. c:char* monetdb_query(monetdb_connection conn, char* query, monetdb_result** result, monetdb_cnt* affected_rows)

    The main query interface to the database kernel. The query should obey the MonetDB syntax. It returns a nested
    structure with the result set in binary form and the number of rows in the result set or affected by an update.

.. c:char* monetdb_prepare(monetdb_connection conn, char *query, monetdb_statement **stmt);

    Sent a query to the database server and prepare an execution plan. Its arguments should be passed when called.

.. c:char* monetdb_bind(monetdb_statement *stmt, void *data, size_t parameter_nr);

    Bind a local variable to a parameter in the prepared query structure. [TODO by pointer, do do you take a copy??]]

.. c:char* monetdb_execute(monetdb_statement *stmt, monetdb_result **result, monetdb_cnt* affected_rows);

    When all parameters are bound, the statement is executed by the database server. An error is thrown if the
    number of parameters does not match. 

.. c: char* monetdb_cleanup_statement(monetdb_connection conn, monetdb_statement *stmt);

    Remove the execution pland and all bound variables.


Database insert
--------------

.. c: char* monetdb_append(monetdb_connection conn, const char* schema, const char* table, monetdb_column **input, size_t column_count);

    The result set obtained from any query can be assigned to a new database table. [TODO which schema...]


Result set
----------
.. c: char* monetdb_result_fetch(monetdb_connection conn, monetdb_result *mres, monetdb_column** res, size_t column_index);

    Given a result set from a query obtain its structure as a collection of column descriptors. [TODO]



Schema inspection
----------------

.. c:char* monetdb_get_table(monetdb_connection conn, monetdb_table** table, const char* schema_name, const char* table_name);

    Retrieve the structure of the schema.table  into the monetdb_table structure.

.. c:char* monetdb_get_columns(monetdb_connection conn, const char* schema_name, const char *table_name, size_t *column_count, char ***column_names, int **column_types);

    Retrieve the details of each column.

Miscellaneous
-------------

..c:bool  monetdb_is_initialized(void)

    Simple function to check if MonetDBe has already been started. [TODO For a remote connection
    it behaves like a 'ping', telling if the remote server is available for interactions.]


