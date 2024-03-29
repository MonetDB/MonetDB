/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/*
 * @f mapi
 * @a M.L. Kersten, K.S. Mullender, Fabian Groffen
 * @v 2.0
 * @* The MonetDB Programming Interface
 * @+ The Mapi Library
 *
 * The easiest way to extend the functionality of MonetDB is to construct
 * an independent application, which communicates with a running server
 * using a database driver with a simple API and a textual protocol.  The
 * effectiveness of such an approach has been demonstrated by the wide
 * use of database API implementations, such as Perl DBI, PHP, ODBC,...
 *
 * @menu
 * * An Example:: a C/C++ code example to get going.
 * * Command Summary:: the list of API functions.
 * * Library Synopsis:: an short explanation of how MAPI works.
 * * Mapi Function Reference:: per API function its parameters and expected results.
 * @end menu
 *
 * @ifclear XQRYmanual
 * @node An Example, Command Summary, The Mapi Library, The Mapi Library
 * @subsection Sample MAPI Application
 *
 * The database driver implementation given in this document focuses on
 * developing applications in C. The command collection has been
 * chosen to align with common practice, i.e. queries follow a prepare,
 * execute, and fetch_row paradigm. The output is considered a regular
 * table. An example of a mini application below illustrates the main
 * operations.
 *
 * @example
 * @verbatim
 * #include <mapi.h>
 * #include <stdio.h>
 * #include <stdlib.h>
 *
 * void die(Mapi dbh, MapiHdl hdl)
 * {
 * 	if (hdl != NULL) {
 * 		mapi_explain_query(hdl, stderr);
 * 		do {
 * 			if (mapi_result_error(hdl) != NULL)
 * 				mapi_explain_result(hdl, stderr);
 * 		} while (mapi_next_result(hdl) == 1);
 * 		mapi_close_handle(hdl);
 * 		mapi_destroy(dbh);
 * 	} else if (dbh != NULL) {
 * 		mapi_explain(dbh, stderr);
 * 		mapi_destroy(dbh);
 * 	} else {
 * 		fprintf(stderr, "command failed\n");
 * 	}
 * 	exit(-1);
 * }
 *
 * MapiHdl query(Mapi dbh, char *q)
 * {
 * 	MapiHdl ret = NULL;
 * 	if ((ret = mapi_query(dbh, q)) == NULL || mapi_error(dbh) != MOK)
 * 		die(dbh, ret);
 * 	return(ret);
 * }
 *
 * void update(Mapi dbh, char *q)
 * {
 * 	MapiHdl ret = query(dbh, q);
 * 	if (mapi_close_handle(ret) != MOK)
 * 		die(dbh, ret);
 * }
 *
 * int main(int argc, char *argv[])
 * {
 *     Mapi dbh;
 *     MapiHdl hdl = NULL;
 * 	char *name;
 * 	char *age;
 *
 *     dbh = mapi_connect("localhost", 50000, "monetdb", "monetdb", "sql", "demo");
 *     if (mapi_error(dbh))
 *         die(dbh, hdl);
 *
 * 	update(dbh, "CREATE TABLE emp (name VARCHAR(20), age INT)");
 * 	update(dbh, "INSERT INTO emp VALUES ('John', 23)");
 * 	update(dbh, "INSERT INTO emp VALUES ('Mary', 22)");
 *
 * 	hdl = query(dbh, "SELECT * FROM emp");
 *
 *     while (mapi_fetch_row(hdl)) {
 *         name = mapi_fetch_field(hdl, 0);
 *         age = mapi_fetch_field(hdl, 1);
 *         printf("%s is %s\n", name, age);
 *     }
 *
 *     mapi_close_handle(hdl);
 *     mapi_destroy(dbh);
 *
 *     return(0);
 * }
 * @end verbatim
 * @end example
 *
 * The @code{mapi_connect()} operation establishes a communication channel with
 * a running server.
 * The query language interface is either "sql" or "mal".
 *
 * Errors on the interaction can be captured using @code{mapi_error()},
 * possibly followed by a request to dump a short error message
 * explanation on a standard file location. It has been abstracted away
 * in a macro.
 *
 * Provided we can establish a connection, the interaction proceeds as in
 * many similar application development packages. Queries are shipped for
 * execution using @code{mapi_query()} and an answer table can be consumed one
 * row at a time. In many cases these functions suffice.
 *
 * The Mapi interface provides caching of rows at the client side.
 * @code{mapi_query()} will load tuples into the cache, after which they can be
 * read repeatedly using @code{mapi_fetch_row()} or directly accessed
 * (@code{mapi_seek_row()}). This facility is particularly handy when small,
 * but stable query results are repeatedly used in the client program.
 *
 * To ease communication between application code and the cache entries,
 * the user can bind the C-variables both for input and output to the
 * query parameters, and output columns, respectively.  The query
 * parameters are indicated by '?' and may appear anywhere in the query
 * template.
 *
 * The Mapi library expects complete lines from the server as answers to
 * query actions. Incomplete lines leads to Mapi waiting forever on the
 * server. Thus formatted printing is discouraged in favor of tabular
 * printing as offered by the @code{table.print()} commands.
 * @end ifclear
 *
 * @ifset XQRYmanual
 * @node An Example
 * @subsection An Example
 *
 * C and C++ programs can use the MAPI library to execute queries on MonetDB.
 *
 * We give a short example with a minimal Mapi program:
 * @itemize
 * @item @code{mapi_connect()} and @code{mapi_disconnect()}: make a connection to a database server (@code{Mapi mid;}).
 *       @strong{note:} pass the value @code{"sql"} in the @code{language} parameter, when connecting.
 * @item @code{mapi_error()} and @code{mapi_error_str()}: check for and print connection errors (on @code{Mapi mid}).
 * @item @code{mapi_query()} and @code{mapi_close_handle()} do a query and get a handle to it (@code{MapiHdl hdl}).
 * @item @code{mapi_result_error()}: check for query evaluation errors (on @code{MapiHdl hdl}).
 * @item @code{mapi_fetch_line()}: get a line of (result or error) output from the server (on @code{MapiHdl hdl}).
 *       @strong{note:} output lines are prefixed with a @code{'='} character that must be escaped.
 * @end itemize
 *
 * @example
 * @verbatim
 * #include <stdio.h>
 * #include <mapi.h>
 * #include <stdlib.h>
 *
 * int
 * main(int argc, char** argv) {
 * 	const char *prog  = argv[0];
 * 	const char *host  = argv[1]; // where Mserver is started, e.g. localhost
 * 	const char *db    = argv[2]; // database name e.g. demo
 * 	int  port         = atoi(argv[3]); // mapi_port e.g. 50000
 * 	char *mode        = argv[4]; // output format e.g. xml
 * 	const char *query = argv[5]; // single-line query e.g. '1+1' (use quotes)
 * 	FILE *fp          = stderr;
 * 	char *line;
 *
 * 	if (argc != 6) {
 * 		fprintf(fp, "usage: %s <host>    <db> <port> <mode> <query>\n", prog);
 * 		fprintf(fp, "  e.g. %s localhost demo 50000  xml    '1+1'\n",   prog);
 * 	} else {
 * 		// CONNECT TO SERVER, default unsecure user/password, language="sql"
 * 		Mapi    mid = mapi_connect(host, port, "monetdb", "monetdb", "sql", db);
 * 		MapiHdl hdl;
 * 		if (mid == NULL) {
 * 			fprintf(fp, "%s: failed to connect.\n", prog);
 * 		} else {
 * 			hdl = mapi_query(mid, query); // FIRE OFF A QUERY
 *
 * 			if (hdl == NULL || mapi_error(mid) != MOK) // CHECK CONNECTION ERROR
 * 				fprintf(fp, "%s: connection error: %s\n", prog, mapi_error_str(mid)); // GET CONNECTION ERROR STRING
 * 			if (hdl) {
 * 				if (mapi_result_error(hdl) != MOK) // CHECK QUERY ERROR
 * 					fprintf(fp, "%s: query error\n", prog);
 * 				else
 * 					fp = stdout; // success: connection&query went ok
 *
 * 				// FETCH SERVER QUERY ANSWER LINE-BY-LINE
 * 				while((line = mapi_fetch_line(hdl)) != NULL) {
 * 					if (*line == '=') line++; // XML result lines start with '='
 * 					fprintf(fp, "%s\n", line);
 * 				}
 * 			}
 * 			mapi_close_handle(hdl); // CLOSE QUERY HANDLE
 * 		}
 * 		mapi_disconnect(mid); // CLOSE CONNECTION
 * 	}
 * 	return (fp == stdout)? 0 : -1;
 * }
 * @end verbatim
 * @end example
 * @end ifset
 *
 * The following action is needed to get a working program.
 * Compilation of the application relies on the @emph{monetdb-config}
 * program shipped with the distribution.
 * It localizes the include files and library directories.
 * Once properly installed, the application can be compiled and linked as
 * follows:
 * @example
 * @verbatim
 * cc sample.c `monetdb-clients-config --cflags --libs` -lmapi -o sample
 * ./sample
 * @end verbatim
 * @end example
 *
 * It assumes that the dynamic loadable libraries are in public places.
 * If, however, the system is installed in your private environment
 * then the following option can be used on most ELF platforms.
 *
 * @example
 * @verbatim
 * cc sample.c `monetdb-clients-config --cflags --libs` -lmapi -o sample \
 * `monetdb-clients-config --libs | sed -e's:-L:-R:g'`
 * ./sample
 * @end verbatim
 * @end example
 *
 * The compilation on Windows is slightly more complicated. It requires
 * more attention towards the location of the include files and libraries.
 *
 * @ifclear XQRYmanual
 * @node Command Summary, Library Synopsis, An Example, The Mapi Library
 * @subsection Command Summary
 * @end ifclear
 * @ifset XQRYmanual
 * @node Command Summary
 * @subsection Command Summary
 * @end ifset
 *
 * The quick reference guide to the Mapi library is given below.  More
 * details on their constraints and defaults are given in the next
 * section.
 *
 *
 * @multitable @columnfractions 0.25 0.75
 * @item mapi_bind()	@tab	Bind string C-variable to a field
 * @item mapi_bind_numeric()	@tab Bind numeric C-variable to field
 * @item mapi_bind_var()	@tab	Bind typed C-variable to a field
 * @item mapi_cache_freeup()	@tab Forcefully shuffle fraction for cache refreshment
 * @item mapi_cache_limit()	@tab Set the tuple cache limit
 * @item mapi_clear_bindings()	@tab Clear all field bindings
 * @item mapi_clear_params()	@tab Clear all parameter bindings
 * @item mapi_close_handle()	@tab	Close query handle and free resources
 * @item mapi_connect()	@tab	Connect to a Mserver
 * @item mapi_destroy()	@tab	Free handle resources
 * @item mapi_disconnect()	@tab Disconnect from server
 * @item mapi_error()	@tab	Test for error occurrence
 * @item mapi_execute()	@tab	Execute a query
 * @item mapi_explain()	@tab	Display error message and context on stream
 * @item mapi_explain_query()	@tab	Display error message and context on stream
 * @item mapi_fetch_all_rows()	@tab	Fetch all answers from server into cache
 * @item mapi_fetch_field()	@tab Fetch a field from the current row
 * @item mapi_fetch_field_len()	@tab Fetch the length of a field from the current row
 * @item mapi_fetch_line()	@tab	Retrieve the next line
 * @item mapi_fetch_reset()	@tab	Set the cache reader to the beginning
 * @item mapi_fetch_row()	@tab	Fetch row of values
 * @item mapi_finish()	@tab	Terminate the current query
 * @item mapi_get_dbname()	@tab	Database being served
 * @item mapi_get_field_count()	@tab Number of fields in current row
 * @item mapi_get_host()	@tab	Host name of server
 * @item mapi_get_query()	@tab	Query being executed
 * @item mapi_get_language()	@tab Query language name
 * @item mapi_get_mapi_version()	@tab Mapi version name
 * @item mapi_get_monet_version()	@tab MonetDB version name
 * @item mapi_get_motd()	@tab	Get server welcome message
 * @item mapi_get_row_count()	@tab	Number of rows in cache or -1
 * @item mapi_get_last_id()	@tab	last inserted id of an auto_increment (or alike) column
 * @item mapi_get_from()	@tab	Get the stream 'from'
 * @item mapi_get_to()	@tab	Get the stream 'to'
 * @item mapi_get_trace()	@tab	Get trace flag
 * @item mapi_get_user()	@tab	Current user name
 * @item mapi_log()	@tab Keep log of client/server interaction
 * @item mapi_next_result()	@tab	Go to next result set
 * @item mapi_needmore()	@tab	Return whether more data is needed
 * @item mapi_ping()	@tab	Test server for accessibility
 * @item mapi_prepare()	@tab	Prepare a query for execution
 * @item mapi_query()	@tab	Send a query for execution
 * @item mapi_query_handle()	@tab	Send a query for execution
 * @item mapi_quote()	@tab Escape characters
 * @item mapi_reconnect()	@tab Reconnect with a clean session context
 * @item mapi_rows_affected()	@tab Obtain number of rows changed
 * @item mapi_seek_row()	@tab	Move row reader to specific location in cache
 * @item mapi_setAutocommit()	@tab	Set auto-commit flag
 * @item mapi_table()	@tab	Get current table name
 * @item mapi_timeout()	@tab	Set timeout for long-running queries[TODO]
 * @item mapi_trace()	@tab	Set trace flag
 * @item mapi_unquote()	@tab	remove escaped characters
 * @end multitable
 *
 * @ifclear XQRYmanual
 * @node Library Synopsis, Mapi Function Reference, Command Summary, The Mapi Library
 * @subsection Library Synopsis
 * @end ifclear
 * @ifset XQRYmanual
 * @node Library Synopsis
 * @subsection Library Synopsis
 * @end ifset
 *
 * The routines to build a MonetDB application are grouped in the library
 * MonetDB Programming Interface, or shorthand Mapi.
 *
 * The protocol information is stored in a Mapi interface descriptor
 * (mid).  This descriptor can be used to ship queries, which return a
 * MapiHdl to represent the query answer.  The application can set up
 * several channels with the same or a different @code{mserver}. It is the
 * programmer's responsibility not to mix the descriptors in retrieving
 * the results.
 *
 * The application may be multi-threaded as long as the user respects the
 * individual connections represented by the database handlers.
 *
 * The interface assumes a cautious user, who understands and has
 * experience with the query or programming language model. It should also be
 * clear that references returned by the API point directly into the
 * administrative structures of Mapi.  This means that they are valid
 * only for a short period, mostly between successive @code{mapi_fetch_row()}
 * commands. It also means that it the values are to retained, they have
 * to be copied.  A defensive programming style is advised.
 *
 * Upon an error, the routines @code{mapi_explain()} and @code{mapi_explain_query()}
 * give information about the context of the failed call, including the
 * expression shipped and any response received.  The side-effect is
 * clearing the error status.
 *
 * @subsection Error Message
 * Almost every call can fail since the connection with the database
 * server can fail at any time.  Functions that return a handle (either
 * @code{Mapi} or @code{MapiHdl}) may return NULL on failure, or they may return the
 * handle with the error flag set.  If the function returns a non-NULL
 * handle, always check for errors with mapi_error.
 *
 *
 * Functions that return MapiMsg indicate success and failure with the
 * following codes.
 *
 * @multitable @columnfractions 0.15 0.7
 * @item MOK  @tab No error
 * @item MERROR  @tab Mapi internal error.
 * @item MTIMEOUT  @tab Error communicating with the server.
 * @end multitable
 *
 * When these functions return MERROR or MTIMEOUT, an explanation of the
 * error can be had by calling one of the functions @code{mapi_error_str()},
 * @code{mapi_explain()}, or @code{mapi_explain_query()}.
 *
 * To check for error messages from the server, call @code{mapi_result_error()}.
 * This function returns NULL if there was no error, or the error message
 * if there was.  A user-friendly message can be printed using
 * @code{map_explain_result()}.  Typical usage is:
 * @verbatim
 * do {
 *     if ((error = mapi_result_error(hdl)) != NULL)
 *         mapi_explain_result(hdl, stderr);
 *     while ((line = mapi_fetch_line(hdl)) != NULL)
 *         ; // use output
 * } while (mapi_next_result(hdl) == 1);
 * @end verbatim
 *
 * @ifclear XQRYmanual
 * @node Mapi Function Reference, The Perl Library , Library Synopsis, The Mapi Library
 * @subsection Mapi Function Reference
 * @end ifclear
 * @ifset XQRYmanual
 * @node Mapi Function Reference
 * @subsection Mapi Function Reference
 * @end ifset
 *
 * @subsection Connecting and Disconnecting
 * @itemize
 * @item Mapi mapi_connect(const char *host, int port, const char *username, const char *password, const char *lang, const char *dbname)
 *
 * Setup a connection with a Mserver at a @emph{host}:@emph{port} and login
 * with @emph{username} and @emph{password}. If host == NULL, the local
 * host is accessed.  If host starts with a '/' and the system supports it,
 * host is the directory where should be searched for UNIX domain
 * sockets.  Port is not ignored, but used to identify which socket to
 * use.  If port == 0, a default port is used.
 * The preferred query language is
 * @verb{ { }sql,mal @verb{ } }.  On success, the function returns a
 * pointer to a structure with administration about the connection.
 *
 * @item MapiMsg mapi_disconnect(Mapi mid)
 *
 * Terminate the session described by @emph{mid}.  The only possible uses
 * of the handle after this call is @emph{mapi_destroy()} and
 * @code{mapi_reconnect()}.
 * Other uses lead to failure.
 *
 * @item MapiMsg mapi_destroy(Mapi mid)
 *
 * Terminate the session described by @emph{ mid} if not already done so,
 * and free all resources. The handle cannot be used anymore.
 *
 * @item MapiMsg mapi_reconnect(Mapi mid)
 *
 * Close the current channel (if still open) and re-establish a fresh
 * connection. This will remove all global session variables.
 *
 * @item MapiMsg mapi_ping(Mapi mid)
 *
 * Test availability of the server. Returns zero upon success.
 * @end itemize
 *
 * @subsection Sending Queries
 * @itemize
 * @item MapiHdl mapi_query(Mapi mid, const char *Command)
 *
 * Send the Command to the database server represented by mid.  This
 * function returns a query handle with which the results of the query
 * can be retrieved.  The handle should be closed with
 * @code{mapi_close_handle()}.  The command response is buffered for
 * consumption, c.f. mapi\_fetch\_row().
 *
 * @item MapiMsg mapi_query_handle(MapiHdl hdl, const char *Command)
 *
 * Send the Command to the database server represented by hdl, reusing
 * the handle from a previous query.  If Command is zero it takes the
 * last query string kept around.  The command response is buffered for
 * consumption, e.g. @code{mapi_fetch_row()}.
 *
 * @item MapiHdl mapi_prepare(Mapi mid, const char *Command)
 *
 * Move the query to a newly allocated query handle (which is returned).
 * Possibly interact with the back-end to prepare the query for
 * execution.
 *
 * @item MapiMsg mapi_execute(MapiHdl hdl)
 *
 * Ship a previously prepared command to the backend for execution. A
 * single answer is pre-fetched to detect any runtime error. MOK is
 * returned upon success.
 *
 * @item MapiMsg mapi_finish(MapiHdl hdl)
 *
 * Terminate a query.  This routine is used in the rare cases that
 * consumption of the tuple stream produced should be prematurely
 * terminated. It is automatically called when a new query using the same
 * query handle is shipped to the database and when the query handle is
 * closed with @code{mapi_close_handle()}.
 *
 * @subsection Getting Results
 * @itemize
 * @item int mapi_get_field_count(MapiHdl mid)
 *
 * Return the number of fields in the current row.
 *
 * @item int64_t mapi_get_row_count(MapiHdl mid)
 *
 * If possible, return the number of rows in the last select call.  A -1
 * is returned if this information is not available.
 *
 * @item int64_t mapi_get_last_id(MapiHdl mid)
 *
 * If possible, return the last inserted id of auto_increment (or alike) column.
 * A -1 is returned if this information is not available. We restrict this to
 * single row inserts and one auto_increment column per table. If the restrictions
 * do not hold, the result is unspecified.
 *
 * @item int64_t mapi_rows_affected(MapiHdl hdl)
 *
 * Return the number of rows affected by a database update command
 * such as SQL's INSERT/DELETE/UPDATE statements.
 *
 * @item int mapi_fetch_row(MapiHdl hdl)
 *
 * Retrieve a row from the server.  The text retrieved is kept around in
 * a buffer linked with the query handle from which selective fields can
 * be extracted.  It returns the number of fields recognized.  A zero is
 * returned upon encountering end of sequence or error. This can be
 * analyzed in using @code{mapi_error()}.
 *
 * @item int64_t mapi_fetch_all_rows(MapiHdl hdl)
 *
 * All rows are cached at the client side first. Subsequent calls to
 * @code{mapi_fetch_row()} will take the row from the cache. The number or
 * rows cached is returned.
 *
 * @item MapiMsg mapi_seek_row(MapiHdl hdl, int64_t rownr, int whence)
 *
 * Reset the row pointer to the requested row number.  If whence is
 * @code{MAPI_SEEK_SET}, rownr is the absolute row number (0 being the
 * first row); if whence is @code{MAPI_SEEK_CUR}, rownr is relative to the
 * current row; if whence is @code{MAPI_SEEK_END}, rownr is relative to
 * the last row.
 *
 * @item MapiMsg mapi_fetch_reset(MapiHdl hdl)
 *
 * Reset the row pointer to the first line in the cache.  This need not
 * be a tuple.  This is mostly used in combination with fetching all
 * tuples at once.
 *
 * @item char *mapi_fetch_field(MapiHdl hdl, int fnr)
 *
 * Return a pointer a C-string representation of the value returned.  A
 * zero is returned upon encountering an error or when the database value
 * is NULL; this can be analyzed in using @code{mapi\_error()}.
 *
 * @item size_t mapi_fetch_fiels_len(MapiHdl hdl, int fnr)
 *
 * Return the length of the C-string representation excluding trailing NULL
 * byte of the value.  Zero is returned upon encountering an error, when the
 * database value is NULL, of when the string is the empty string.  This can
 * be analyzed by using @code{mapi\_error()} and @code{mapi\_fetch\_field()}.
 *
 * @item MapiMsg mapi_next_result(MapiHdl hdl)
 *
 * Go to the next result set, discarding the rest of the output of the
 * current result set.
 * @end itemize
 *
 * @subsection Errors
 * @itemize
 * @item MapiMsg mapi_error(Mapi mid)
 *
 * Return the last error code or 0 if there is no error.
 *
 * @item char *mapi_error_str(Mapi mid)
 *
 * Return a pointer to the last error message.
 *
 * @item char *mapi_result_error(MapiHdl hdl)
 *
 * Return a pointer to the last error message from the server.
 *
 * @item void mapi_explain(Mapi mid, FILE *fd)
 *
 * Write the error message obtained from @code{mserver} to a file.
 *
 * @item void mapi_explain_query(MapiHdl hdl, FILE *fd)
 *
 * Write the error message obtained from @code{mserver} to a file.
 *
 * @item void mapi_explain_result(MapiHdl hdl, FILE *fd)
 *
 * Write the error message obtained from @code{mserver} to a file.
 * @end itemize
 *
 * @subsection Parameters
 *
 * @itemize
 * @item MapiMsg mapi_bind(MapiHdl hdl, int fldnr, char **val)
 *
 * Bind a string variable with a field in the return table.  Upon a
 * successful subsequent @code{mapi\_fetch\_row()} the indicated field is stored
 * in the space pointed to by val.  Returns an error if the field
 * identified does not exist.
 *
 * @item MapiMsg mapi_bind_var(MapiHdl hdl, int fldnr, int type, void *val)
 *
 * Bind a variable to a field in the return table.  Upon a successful
 * subsequent @code{mapi\_fetch\_row()}, the indicated field is converted to the
 * given type and stored in the space pointed to by val.  The types
 * recognized are @verb{ { } @code{MAPI\_TINY, MAPI\_UTINY, MAPI\_SHORT, MAPI\_USHORT,
 * MAPI_INT, MAPI_UINT, MAPI_LONG, MAPI_ULONG, MAPI_LONGLONG,
 * MAPI_ULONGLONG, MAPI_CHAR, MAPI_VARCHAR, MAPI_FLOAT, MAPI_DOUBLE,
 * MAPI_DATE, MAPI_TIME, MAPI_DATETIME} @verb{ } }.  The binding operations
 * should be performed after the mapi_execute command.  Subsequently all
 * rows being fetched also involve delivery of the field values in the
 * C-variables using proper conversion. For variable length strings a
 * pointer is set into the cache.
 *
 * @item MapiMsg mapi_bind_numeric(MapiHdl hdl, int fldnr, int scale, int precision, void *val)
 *
 * Bind to a numeric variable, internally represented by MAPI_INT
 * Describe the location of a numeric parameter in a query template.
 *
 * @item MapiMsg mapi_clear_bindings(MapiHdl hdl)
 *
 * Clear all field bindings.
 *
 * @item MapiMsg mapi_param(MapiHdl hdl, int fldnr, char **val)
 *
 * Bind a string variable with the n-th placeholder in the query
 * template.  No conversion takes place.
 *
 * @item MapiMsg mapi_param_type(MapiHdl hdl, int fldnr, int ctype, int sqltype, void *val)
 *
 * Bind a variable whose type is described by ctype to a parameter whose
 * type is described by sqltype.
 *
 * @item MapiMsg mapi_param_numeric(MapiHdl hdl, int fldnr, int scale, int precision, void *val)
 *
 * Bind to a numeric variable, internally represented by MAPI_INT.
 *
 * @item MapiMsg mapi_param_string(MapiHdl hdl, int fldnr, int sqltype, char *val, int *sizeptr)
 *
 * Bind a string variable, internally represented by MAPI_VARCHAR, to a
 * parameter.  The sizeptr parameter points to the length of the string
 * pointed to by val.  If sizeptr == NULL or *sizeptr == -1, the string
 * is NULL-terminated.
 *
 * @item MapiMsg mapi_clear_params(MapiHdl hdl)
 *
 * Clear all parameter bindings.
 * @end itemize
 *
 * @subsection Miscellaneous
 * @itemize
 * @item MapiMsg mapi_setAutocommit(Mapi mid, bool autocommit)
 *
 * Set the autocommit flag (default is on).  This only has an effect
 * when the language is SQL.  In that case, the server commits after each
 * statement sent to the server.
 *
 * @item MapiMsg mapi_cache_limit(Mapi mid, int maxrows)
 *
 * A limited number of tuples are pre-fetched after each @code{execute()}.  If
 * maxrows is negative, all rows will be fetched before the application
 * is permitted to continue. Once the cache is filled, a number of tuples
 * are shuffled to make room for new ones, but taking into account
 * non-read elements.  Filling the cache quicker than reading leads to an
 * error.
 *
 * @item MapiMsg mapi_cache_freeup(MapiHdl hdl, int percentage)
 *
 * Forcefully shuffle the cache making room for new rows.  It ignores the
 * read counter, so rows may be lost.
 *
 * @item char * mapi_quote(const char *str, int size)
 *
 * Escape special characters such as @code{\n}, @code{\t} in str with
 * backslashes.  The returned value is a newly allocated string which
 * should be freed by the caller.
 *
 * @item char * mapi_unquote(const char *name)
 *
 * The reverse action of @code{mapi_quote()}, turning the database
 * representation into a C-representation. The storage space is
 * dynamically created and should be freed after use.
 *
 * @item MapiMsg  mapi_trace(Mapi mid, bool flag)
 *
 * Set the trace flag to monitor interaction of the client
 * with the library. It is primarilly used for debugging
 * Mapi applications.
 *
 * @item int mapi_get_trace(Mapi mid)
 *
 * Return the current value of the trace flag.
 *
 * @item MapiMsg  mapi\_log(Mapi mid, const char *fname)
 *
 * Log the interaction between the client and server for offline
 * inspection. Beware that the log file overwrites any previous log.
 * For detailed interaction trace with the Mapi library itself use mapi\_trace().
 * @end itemize
 * The remaining operations are wrappers around the data structures
 * maintained. Note that column properties are derived from the table
 * output returned from the server.
 * @itemize
 * @item  char *mapi_get_name(MapiHdl hdl, int fnr)
 * @item  char *mapi_get_type(MapiHdl hdl, int fnr)
 * @item  char *mapi_get_table(MapiHdl hdl, int fnr)
 * @item  int mapi_get_len(Mapi mid, int fnr)
 *
 * @item  const char *mapi_get_dbname(Mapi mid)
 * @item  const char *mapi_get_host(Mapi mid)
 * @item  const char *mapi_get_user(Mapi mid)
 * @item  const char *mapi_get_lang(Mapi mid)
 * @item  const char *mapi_get_motd(Mapi mid)
 *
 * @end itemize
 * @- Implementation
 */

#include "monetdb_config.h"
#include "stream.h"		/* include before mapi.h */
#include "stream_socket.h"
#include "mapi.h"
#include "mapi_prompt.h"
#include "mcrypt.h"
#include "matomic.h"
#include "mstring.h"
#include "mutils.h"

#include "mapi_intern.h"

#ifndef INVALID_SOCKET
#define INVALID_SOCKET (-1)
#endif

#define MAPIBLKSIZE	256	/* minimum buffer shipped */

#define MAPI_AUTO	0	/* automatic type detection */
#define MAPI_TINY	1
#define MAPI_UTINY	2
#define MAPI_SHORT	3
#define MAPI_USHORT	4
#define MAPI_INT	5
#define MAPI_UINT	6
#define MAPI_LONG	7
#define MAPI_ULONG	8
#define MAPI_LONGLONG	9
#define MAPI_ULONGLONG	10
#define MAPI_CHAR	11
#define MAPI_VARCHAR	12
#define MAPI_FLOAT	13
#define MAPI_DOUBLE	14
#define MAPI_DATE	15
#define MAPI_TIME	16
#define MAPI_DATETIME	17
#define MAPI_NUMERIC	18

#define PLACEHOLDER	'?'


#ifdef DEBUG
#define debugprint(fmt,arg)	printf(fmt,arg)
#else
#define debugprint(fmt,arg)	((void) 0)
#endif

/*
 * All external calls to the library should pass the mapi-check
 * routine. It assures a working connection and proper reset of
 * the error status of the Mapi structure.
 */
#define mapi_check(X)							\
	do {								\
		debugprint("entering %s\n", __func__);			\
		assert(X);						\
		if (!(X)->connected) {					\
			mapi_setError((X), "Connection lost",		\
				      __func__, MERROR);		\
			return (X)->error;				\
		}							\
		mapi_clrError(X);					\
	} while (0)
#define mapi_check0(X)							\
	do {								\
		debugprint("entering %s\n", __func__);			\
		assert(X);						\
		if (!(X)->connected) {					\
			mapi_setError((X), "Connection lost",		\
				      __func__, MERROR);		\
			return 0;					\
		}							\
		mapi_clrError(X);					\
	} while (0)
#define mapi_hdl_check(X)						\
	do {								\
		debugprint("entering %s\n", __func__);			\
		assert(X);						\
		assert((X)->mid);					\
		if (!(X)->mid->connected) {				\
			mapi_setError((X)->mid, "Connection lost",	\
				      __func__, MERROR);		\
			return (X)->mid->error;				\
		}							\
		mapi_clrError((X)->mid);				\
	} while (0)
#define mapi_hdl_check0(X)						\
	do {								\
		debugprint("entering %s\n", __func__);			\
		assert(X);						\
		assert((X)->mid);					\
		if (!(X)->mid->connected) {				\
			mapi_setError((X)->mid, "Connection lost",	\
				      __func__, MERROR);		\
			return 0;					\
		}							\
		mapi_clrError((X)->mid);				\
	} while (0)

static int mapi_extend_bindings(MapiHdl hdl, int minbindings);
static int mapi_extend_params(MapiHdl hdl, int minparams);
static int unquote(const char *msg, char **start, const char **next, int endchar, size_t *lenp);
static int mapi_slice_row(struct MapiResultSet *result, int cr);
static void mapi_store_bind(struct MapiResultSet *result, int cr);

static ATOMIC_FLAG mapi_initialized = ATOMIC_FLAG_INIT;
/*
 * Blocking
 * --------
 *
 * The server side code works with a common/stream package, a fast
 * buffered IO scheme.  Nowadays this should be the only protocol used,
 * while historical uses were line-based instead.
 *
 *
 * Error Handling
 * --------------
 *
 * All externally visible functions should first call mapi_clrError (usually
 * though a call to one of the check macros above) to clear the error flag.
 * When an error is detected, the library calls mapi_setError to set the error
 * flag.  The application can call mapi_error or mapi_error_str to check for
 * errors, and mapi_explain or mapi_explain_query to print a formatted error
 * report.
 */
char mapi_nomem[] = "Memory allocation failed";

void
mapi_clrError(Mapi mid)
{
	assert(mid);
	if (mid->errorstr && mid->errorstr != mapi_nomem)
		free(mid->errorstr);
	mid->action = 0;	/* contains references to constants */
	mid->error = 0;
	mid->errorstr = 0;
}

MapiMsg
mapi_setError(Mapi mid, const char *msg, const char *action, MapiMsg error)
{
	assert(msg);
	REALLOC(mid->errorstr, strlen(msg) + 1);
	if (mid->errorstr == NULL)
		mid->errorstr = mapi_nomem;
	else
		strcpy(mid->errorstr, msg);
	mapi_log_record(mid, "ERROR", "%s: %s", action, mid->errorstr);
	mid->error = error;
	mid->action = action;
	return mid->error;
}

MapiMsg mapi_printError(Mapi mid, const char *action, MapiMsg error, const char *fmt, ...)
{
	size_t size = 81; // just a guess

	while (1) {
		REALLOC(mid->errorstr, size);
		if (mid->errorstr == NULL) {
			mid->errorstr = mapi_nomem;
			break;
		}
		va_list ap;
		va_start(ap, fmt);
		int n = vsnprintf(mid->errorstr, size, fmt, ap);
		va_end(ap);
		if (n < 0) {
			// is it even possible for vsnprintf to fail?
			break;
		}
		if ((size_t)n < size) {
			// it fit
			break;
		} else {
			// try again larger
			size = (size_t)n + 1;
		}
	}
	mapi_log_record(mid, "ERROR", "%s: %s", action, mid->errorstr);
	mid->error = error;
	mid->action = action;
	return mid->error;
}

MapiMsg
mapi_error(Mapi mid)
{
	assert(mid);
	return mid->error;
}

const char *
mapi_error_str(Mapi mid)
{
	assert(mid);
	return mid->errorstr;
}

#ifdef _MSC_VER
static const struct {
	int e;
	const char *m;
} wsaerrlist[] = {
	{ WSA_INVALID_HANDLE, "Specified event object handle is invalid" },
	{ WSA_NOT_ENOUGH_MEMORY, "Insufficient memory available" },
	{ WSA_INVALID_PARAMETER, "One or more parameters are invalid" },
	{ WSA_OPERATION_ABORTED, "Overlapped operation aborted" },
	{ WSA_IO_INCOMPLETE, "Overlapped I/O event object not in signaled state" },
	{ WSA_IO_PENDING, "Overlapped operations will complete later" },
	{ WSAEINTR, "Interrupted function call" },
	{ WSAEBADF, "File handle is not valid" },
	{ WSAEACCES, "Permission denied" },
	{ WSAEFAULT, "Bad address" },
	{ WSAEINVAL, "Invalid argument" },
	{ WSAEMFILE, "Too many open files" },
	{ WSAEWOULDBLOCK, "Resource temporarily unavailable" },
	{ WSAEINPROGRESS, "Operation now in progress" },
	{ WSAEALREADY, "Operation already in progress" },
	{ WSAENOTSOCK, "Socket operation on nonsocket" },
	{ WSAEDESTADDRREQ, "Destination address required" },
	{ WSAEMSGSIZE, "Message too long" },
	{ WSAEPROTOTYPE, "Protocol wrong type for socket" },
	{ WSAENOPROTOOPT, "Bad protocol option" },
	{ WSAEPROTONOSUPPORT, "Protocol not supported" },
	{ WSAESOCKTNOSUPPORT, "Socket type not supported" },
	{ WSAEOPNOTSUPP, "Operation not supported" },
	{ WSAEPFNOSUPPORT, "Protocol family not supported" },
	{ WSAEAFNOSUPPORT, "Address family not supported by protocol family" },
	{ WSAEADDRINUSE, "Address already in use" },
	{ WSAEADDRNOTAVAIL, "Cannot assign requested address" },
	{ WSAENETDOWN, "Network is down" },
	{ WSAENETUNREACH, "Network is unreachable" },
	{ WSAENETRESET, "Network dropped connection on reset" },
	{ WSAECONNABORTED, "Software caused connection abort" },
	{ WSAECONNRESET, "Connection reset by peer" },
	{ WSAENOBUFS, "No buffer space available" },
	{ WSAEISCONN, "Socket is already connected" },
	{ WSAENOTCONN, "Socket is not connected" },
	{ WSAESHUTDOWN, "Cannot send after socket shutdown" },
	{ WSAETOOMANYREFS, "Too many references" },
	{ WSAETIMEDOUT, "Connection timed out" },
	{ WSAECONNREFUSED, "Connection refused" },
	{ WSAELOOP, "Cannot translate name" },
	{ WSAENAMETOOLONG, "Name too long" },
	{ WSAEHOSTDOWN, "Host is down" },
	{ WSAEHOSTUNREACH, "No route to host" },
	{ WSAENOTEMPTY, "Directory not empty" },
	{ WSAEPROCLIM, "Too many processes" },
	{ WSAEUSERS, "User quota exceeded" },
	{ WSAEDQUOT, "Disk quota exceeded" },
	{ WSAESTALE, "Stale file handle reference" },
	{ WSAEREMOTE, "Item is remote" },
	{ WSASYSNOTREADY, "Network subsystem is unavailable" },
	{ WSAVERNOTSUPPORTED, "Winsock.dll version out of range" },
	{ WSANOTINITIALISED, "Successful WSAStartup not yet performed" },
	{ WSAEDISCON, "Graceful shutdown in progress" },
	{ WSAENOMORE, "No more results" },
	{ WSAECANCELLED, "Call has been canceled" },
	{ WSAEINVALIDPROCTABLE, "Procedure call table is invalid" },
	{ WSAEINVALIDPROVIDER, "Service provider is invalid" },
	{ WSAEPROVIDERFAILEDINIT, "Service provider failed to initialize" },
	{ WSASYSCALLFAILURE, "System call failure" },
	{ WSASERVICE_NOT_FOUND, "Service not found" },
	{ WSATYPE_NOT_FOUND, "Class type not found" },
	{ WSA_E_NO_MORE, "No more results" },
	{ WSA_E_CANCELLED, "Call was canceled" },
	{ WSAEREFUSED, "Database query was refused" },
	{ WSAHOST_NOT_FOUND, "Host not found" },
	{ WSATRY_AGAIN, "Nonauthoritative host not found" },
	{ WSANO_RECOVERY, "This is a nonrecoverable error" },
	{ WSANO_DATA, "Valid name, no data record of requested type" },
	{ WSA_QOS_RECEIVERS, "QOS receivers" },
	{ WSA_QOS_SENDERS, "QOS senders" },
	{ WSA_QOS_NO_SENDERS, "No QOS senders" },
	{ WSA_QOS_NO_RECEIVERS, "QOS no receivers" },
	{ WSA_QOS_REQUEST_CONFIRMED, "QOS request confirmed" },
	{ WSA_QOS_ADMISSION_FAILURE, "QOS admission error" },
	{ WSA_QOS_POLICY_FAILURE, "QOS policy failure" },
	{ WSA_QOS_BAD_STYLE, "QOS bad style" },
	{ WSA_QOS_BAD_OBJECT, "QOS bad object" },
	{ WSA_QOS_TRAFFIC_CTRL_ERROR, "QOS traffic control error" },
	{ WSA_QOS_GENERIC_ERROR, "QOS generic error" },
	{ WSA_QOS_ESERVICETYPE, "QOS service type error" },
	{ WSA_QOS_EFLOWSPEC, "QOS flowspec error" },
	{ WSA_QOS_EPROVSPECBUF, "Invalid QOS provider buffer" },
	{ WSA_QOS_EFILTERSTYLE, "Invalid QOS filter style" },
	{ WSA_QOS_EFILTERTYPE, "Invalid QOS filter type" },
	{ WSA_QOS_EFILTERCOUNT, "Incorrect QOS filter count" },
	{ WSA_QOS_EOBJLENGTH, "Invalid QOS object length" },
	{ WSA_QOS_EFLOWCOUNT, "Incorrect QOS flow count" },
	{ WSA_QOS_EUNKOWNPSOBJ, "Unrecognized QOS object" },
	{ WSA_QOS_EPOLICYOBJ, "Invalid QOS policy object" },
	{ WSA_QOS_EFLOWDESC, "Invalid QOS flow descriptor" },
	{ WSA_QOS_EPSFLOWSPEC, "Invalid QOS provider-specific flowspec" },
	{ WSA_QOS_EPSFILTERSPEC, "Invalid QOS provider-specific filterspec" },
	{ WSA_QOS_ESDMODEOBJ, "Invalid QOS shape discard mode object" },
	{ WSA_QOS_ESHAPERATEOBJ, "Invalid QOS shaping rate object" },
	{ WSA_QOS_RESERVED_PETYPE, "Reserved policy QOS element type" },
};
const char *
wsaerror(int err)
{
	int i;

	for (i = 0; i < NELEM(wsaerrlist); i++)
		if (wsaerrlist[i].e == err)
			return wsaerrlist[i].m;
	return "Unknown error";
}
#endif

static void
clean_print(char *msg, const char *prefix, FILE *fd)
{
	size_t len = strlen(prefix);

	while (msg && *msg) {
		/* cut by line */
		char *p = strchr(msg, '\n');

		if (p)
			*p++ = 0;

		/* skip over prefix */
		if (strncmp(msg, prefix, len) == 0)
			msg += len;

		/* output line */
		fputs(msg, fd);
		fputc('\n', fd);
		msg = p;
	}
}

static void
indented_print(const char *msg, const char *prefix, FILE *fd)
{
	/* for multiline error messages, indent all subsequent
	   lines with the space it takes to print "ERROR = " */
	const char *s = prefix, *p = msg, *q;
	const int len = (int) strlen(s);
	const char t = s[len - 1];

	while (p && *p) {
		fprintf(fd, "%*.*s%c", len - 1, len - 1, s, t);
		s = "";

		q = strchr(p, '\n');
		if (q) {
			q++;	/* also print the newline */
			fprintf(fd, "%.*s", (int) (q - p), p);
		} else {
			/* print bit after last newline,
			   adding one ourselves */
			fprintf(fd, "%s\n", p);
			break;	/* nothing more to do */
		}
		p = q;
	}
}

void
mapi_noexplain(Mapi mid, const char *errorprefix)
{
	assert(mid);
	if (mid->noexplain)
		free(mid->noexplain);
	mid->noexplain = errorprefix ? strdup(errorprefix) : NULL;
}

void
mapi_explain(Mapi mid, FILE *fd)
{
	assert(mid);
	if (mid->noexplain == NULL) {
		const char *host = msetting_string(mid->settings, MP_HOST);
		const char *user = msetting_string(mid->settings, MP_USER);
		int port = msetting_long(mid->settings, MP_PORT);
		if (host[0] == '/')
			fprintf(fd, "MAPI  = (%s) %s\n", user, host);
		else
			fprintf(fd, "MAPI  = %s@%s:%d\n",
				user, host, port);
		if (mid->action)
			fprintf(fd, "ACTION= %s\n", mid->action);
		if (mid->errorstr)
			indented_print(mid->errorstr, "ERROR = !", fd);
	} else if (mid->errorstr) {
		clean_print(mid->errorstr, mid->noexplain, fd);
	}
	fflush(fd);
	mapi_clrError(mid);
}

void
mapi_explain_query(MapiHdl hdl, FILE *fd)
{
	Mapi mid;

	assert(hdl);
	mid = hdl->mid;
	assert(mid);
	if (mid->noexplain == NULL) {
		const char *host = msetting_string(mid->settings, MP_HOST);
		const char *user = msetting_string(mid->settings, MP_USER);
		int port = msetting_long(mid->settings, MP_PORT);
		if (host[0] == '/')
			fprintf(fd, "MAPI  = (%s) %s\n", user, host);
		else
			fprintf(fd, "MAPI  = %s@%s:%d\n",
				user, host, port);
		if (mid->action)
			fprintf(fd, "ACTION= %s\n", mid->action);
		if (hdl->query)
			indented_print(hdl->query, "QUERY = ", fd);
		if (mid->errorstr)
			indented_print(mid->errorstr, "ERROR = !", fd);
	} else if (mid->errorstr) {
		clean_print(mid->errorstr, mid->noexplain, fd);
	}
	fflush(fd);
	mapi_clrError(mid);
}

void
mapi_explain_result(MapiHdl hdl, FILE *fd)
{
	Mapi mid;

	if (hdl == NULL ||
	    hdl->result == NULL ||
	    hdl->result->errorstr == NULL)
		return;
	assert(hdl);
	assert(hdl->result);
	assert(hdl->result->errorstr);
	mid = hdl->mid;
	assert(mid);
	if (mid->noexplain == NULL) {
		const char *host = msetting_string(mid->settings, MP_HOST);
		const char *user = msetting_string(mid->settings, MP_USER);
		int port = msetting_long(mid->settings, MP_PORT);
		if (host[0] == '/')
			fprintf(fd, "MAPI  = (%s) %s\n", user, host);
		else
			fprintf(fd, "MAPI  = %s@%s:%d\n",
				user, host, port);
		if (mid->action)
			fprintf(fd, "ACTION= %s\n", mid->action);
		if (hdl->query)
			indented_print(hdl->query, "QUERY = ", fd);
		indented_print(hdl->result->errorstr, "ERROR = !", fd);
		if (msettings_lang_is_sql(mid->settings) && hdl->result->sqlstate[0])
			indented_print(hdl->result->sqlstate, "CODE  = ", fd);
	} else {
		clean_print(hdl->result->errorstr, mid->noexplain, fd);
	}
	fflush(fd);
}

stream *
mapi_get_to(Mapi mid)
{
	mapi_check0(mid);
	return mid->to;
}

stream *
mapi_get_from(Mapi mid)
{
	mapi_check0(mid);
	return mid->from;
}

bool
mapi_get_trace(Mapi mid)
{
	mapi_check0(mid);
	return mid->trace;
}

bool
mapi_get_autocommit(Mapi mid)
{
	mapi_check0(mid);
	return msetting_bool(mid->settings, MP_AUTOCOMMIT);
}

bool
mapi_get_columnar_protocol(Mapi mid)
{
	mapi_check0(mid);
	return mid->columnar_protocol;
}

int
mapi_get_time_zone(Mapi mid)
{
	mapi_check0(mid);
	return msetting_long(mid->settings, MP_TIMEZONE);
}

static int64_t
usec(void)
{
#ifdef HAVE_GETTIMEOFDAY
	struct timeval tp;

	gettimeofday(&tp, NULL);
	return ((int64_t) tp.tv_sec) * 1000000 + (int64_t) tp.tv_usec;
#else
#ifdef HAVE_FTIME
	struct timeb tb;

	ftime(&tb);
	return ((int64_t) tb.time) * 1000000 + ((int64_t) tb.millitm) * 1000;
#endif
#endif
}

static void
mapi_log_header(Mapi mid, const char *funcname, long line, const char *mark1, const char *mark2)
{
	int64_t now = usec();
	static int64_t firstcall = 0;
	if (firstcall == 0)
		firstcall = now;
	double seconds = (double)(now - firstcall) / 1e6;
	mnstr_printf(mid->tracelog, "\342\226\266 [%u] t=%.3fs %s%s %s(), line %ld\n", mid->index, seconds, mark1, mark2, funcname, line); /* U+25B6: right-pointing triangle */
}

void
mapi_impl_log_record(Mapi mid, const char *funcname, long line, const char *mark, const char *fmt, ...)
{
	va_list ap;

	if (mid->tracelog == NULL)
		return;

	size_t needed = 128;
	size_t to_print;
	while (1) {
		if (mid->tracebuffersize < needed) {
			free(mid->tracebuffer);
			mid->tracebuffer = malloc(needed);
			if (mid->tracebuffer) {
				mid->tracebuffersize = needed;
			} else {
				mid->tracebuffersize = 0;
				to_print = 0;
				break;
			}
		}
		va_start(ap, fmt);
		int n = vsnprintf(mid->tracebuffer, mid->tracebuffersize, fmt, ap);
		va_end(ap);
		if (n < 0) {
			to_print = 0;
			break;
		}
		if ((size_t)n < mid->tracebuffersize) {
			to_print = n;
			break;
		}
		// need to reallocate
		needed = n + 1;
	}

	mapi_log_header(mid, funcname, line, mark, "");
	if (to_print > 0) {
		mnstr_write(mid->tracelog, mid->tracebuffer, to_print, 1);
		mnstr_writeChr(mid->tracelog, '\n');
	}
	mnstr_flush(mid->tracelog, MNSTR_FLUSH_DATA);
}

#define MAPILOG_OPEN "\tb'"
#define MAPILOG_CLOSE "'"

void
mapi_impl_log_data(Mapi mid, const char *filename, long line, const char *mark, const char *start, size_t len)
{
	const char hexdigits[] = "0123456789abcdef";
	if (mid->tracelog == NULL)
		return;

	mapi_log_header(mid, filename, line, mark, " (DATA)");

	const size_t margin = strlen("\\xNN" MAPILOG_CLOSE "\n," MAPILOG_OPEN);
	char buffer[128] = { 0 };
	char *pos = buffer;
	mnstr_writeStr(mid->tracelog, MAPILOG_OPEN);
	bool inside = true;
	for (unsigned char *p = (unsigned char*)start; (char*)p < start + len; p++) {
		unsigned char c = *p;
		if (!inside) {
			for (char *text = "\n" MAPILOG_OPEN; *text; text++)
				*pos++ = *text;
			inside = true;
		}
		if (pos >= buffer + sizeof(buffer) - margin) {
			mnstr_write(mid->tracelog, buffer, 1, pos - buffer);
			pos = buffer;
		}
		switch (c) {
			case '\\':
			case '\'':
				*pos++ = '\\';
				*pos++ = c;
				break;
			case '\t':
				*pos++ = '\\';
				*pos++ = 't';
				break;
			case '\n':
				for (char *text = "\\n" MAPILOG_CLOSE; *text; text++)
					*pos++ = *text;
				inside = false;
				break;
			default:
				if (c >= 32 && c < 127) {
					*pos++ = c;
				} else {
					*pos++ = '\\';
					*pos++ = 'x';
					*pos++ = hexdigits[c / 16];
					*pos++ = hexdigits[c % 16];
				}
			break;
		}
	}
	if (inside) {
		for (char *text = MAPILOG_CLOSE; *text; text++)
			*pos++ = *text;
	}
	*pos++ = ',';
	*pos++ = '\n';
	mnstr_write(mid->tracelog, buffer, 1, pos - buffer);

	mnstr_flush(mid->tracelog, MNSTR_FLUSH_DATA);
}

MapiMsg
mapi_log(Mapi mid, const char *nme)
{
	mapi_clrError(mid);
	if (mid->tracelog) {
		close_stream(mid->tracelog);
		mid->tracelog = NULL;
	}
	if (nme == NULL)
		return MOK;
	if (nme[0] == '-' && nme[1] == '\0')
		mid->tracelog = stderr_wastream();
	else
		mid->tracelog = open_wastream(nme);
	if (mid->tracelog == NULL || mnstr_errnr(mid->tracelog) != MNSTR_NO__ERROR) {
		if (mid->tracelog)
			close_stream(mid->tracelog);
		mid->tracelog = NULL;
		return mapi_setError(mid, "Could not create log file", __func__, MERROR);
	}
	return MOK;
}

/* send a dummy request to the server to see whether the connection is
   still alive */
MapiMsg
mapi_ping(Mapi mid)
{
	mapi_check(mid);

	MapiHdl hdl;
	if (msettings_lang_is_sql(mid->settings))
		hdl = mapi_query(mid, "select true;");
	else if (msettings_lang_is_mal(mid->settings))
		hdl = mapi_query(mid, "io.print(1);");
	else
		hdl = NULL;

	if (hdl)
		mapi_close_handle(hdl);
	return mid->error;
}

/* allocate a new structure to represent a result set */
static struct MapiResultSet *
new_result(MapiHdl hdl)
{
	struct MapiResultSet *result;

	assert((hdl->lastresult == NULL && hdl->result == NULL) ||
	       (hdl->result != NULL && hdl->lastresult != NULL && hdl->lastresult->next == NULL));

	if (hdl->mid->trace)
		printf("allocating new result set\n");
	/* append a newly allocated struct to the end of the linked list */
	result = malloc(sizeof(*result));
	if (result == NULL)
		return NULL;
	*result = (struct MapiResultSet) {
		.hdl = hdl,
		.tableid = -1,
		.querytype = -1,
		.last_id = -1,
		.cache.rowlimit = msetting_long(hdl->mid->settings, MP_REPLYSIZE),
		.cache.reader = -1,
		.commentonly = true,
	};
	if (hdl->lastresult == NULL)
		hdl->result = hdl->lastresult = result;
	else {
		hdl->lastresult->next = result;
		hdl->lastresult = result;
	}

	return result;
}

/* close a result set, discarding any unread results */
static MapiMsg
close_result(MapiHdl hdl)
{
	struct MapiResultSet *result;
	Mapi mid;
	int i;

	result = hdl->result;
	if (result == NULL)
		return MERROR;
	mid = hdl->mid;
	assert(mid != NULL);
	if (mid->trace)
		printf("closing result set\n");
	if (result->tableid >= 0 && result->querytype != Q_PREPARE) {
		if (mid->active &&
		    result->next == NULL &&
		    !mid->active->needmore &&
		    read_into_cache(mid->active, -1) != MOK)
			return MERROR;
		assert(hdl->npending_close == 0 ||
		       (hdl->npending_close > 0 && hdl->pending_close != NULL));
		if (mid->active &&
		    (mid->active->active != result ||
		     result->cache.tuplecount < result->row_count)) {
			/* results for which we got all tuples at the initial
			 * response, need not to be closed as the server already
			 * did that immediately */
			if (result->row_count > result->tuple_count) {
				/* can't write "X" commands now, so save for later */
				REALLOC(hdl->pending_close, hdl->npending_close + 1);
				hdl->pending_close[hdl->npending_close] = result->tableid;
				hdl->npending_close++;
			}
		} else if (mid->to != NULL) {
			/* first close saved up to-be-closed tables */
			for (i = 0; i < hdl->npending_close; i++) {
				char msg[256];

				snprintf(msg, sizeof(msg), "Xclose %d\n", hdl->pending_close[i]);
				mapi_log_record(mid, "CMD", "%s", msg);
				mid->active = hdl;
				if (mnstr_printf(mid->to, "%s", msg) < 0 ||
				    mnstr_flush(mid->to, MNSTR_FLUSH_DATA)) {
					close_connection(mid);
					mapi_setError(mid, mnstr_peek_error(mid->to), __func__, MTIMEOUT);
					break;
				}
				read_into_cache(hdl, 0);
			}
			hdl->npending_close = 0;
			if (hdl->pending_close)
				free(hdl->pending_close);
			hdl->pending_close = NULL;
			if (mid->to != NULL && result->tuple_count < result->row_count) {
				char msg[256];

				snprintf(msg, sizeof(msg), "Xclose %d\n", result->tableid);
				mapi_log_record(mid, "CMD", "%s", msg);
				mid->active = hdl;
				if (mnstr_printf(mid->to, "%s", msg) < 0 ||
				    mnstr_flush(mid->to, MNSTR_FLUSH_DATA)) {
					close_connection(mid);
					mapi_setError(mid, mnstr_peek_error(mid->to), __func__, MTIMEOUT);
				} else
					read_into_cache(hdl, 0);
			}
		}
		result->tableid = -1;
	}
	if (mid->active == hdl &&
	    hdl->active == result &&
	    read_into_cache(hdl, -1) != MOK)
		return MERROR;
	if( hdl->active == result)
		return MERROR;
	//assert(hdl->active != result);
	if (result->fields) {
		for (i = 0; i < result->maxfields; i++) {
			if (result->fields[i].tablename)
				free(result->fields[i].tablename);
			if (result->fields[i].columnname)
				free(result->fields[i].columnname);
			if (result->fields[i].columntype)
				free(result->fields[i].columntype);
		}
		free(result->fields);
	}
	result->fields = NULL;
	result->maxfields = result->fieldcnt = 0;
	if (result->cache.line) {
		for (i = 0; i < result->cache.writer; i++) {
			if (result->cache.line[i].rows)
				free(result->cache.line[i].rows);
			if (result->cache.line[i].anchors) {
				int j;

				for (j = 0; j < result->cache.line[i].fldcnt; j++)
					if (result->cache.line[i].anchors[j]) {
						free(result->cache.line[i].anchors[j]);
						result->cache.line[i].anchors[j] = NULL;
					}
				free(result->cache.line[i].anchors);
			}
			if (result->cache.line[i].lens)
				free(result->cache.line[i].lens);
		}
		free(result->cache.line);
		result->cache.line = NULL;
		result->cache.tuplecount = 0;
	}
	if (result->errorstr && result->errorstr != mapi_nomem)
		free(result->errorstr);
	result->errorstr = NULL;
	memset(result->sqlstate, 0, sizeof(result->sqlstate));
	result->hdl = NULL;
	hdl->result = result->next;
	if (hdl->result == NULL)
		hdl->lastresult = NULL;
	result->next = NULL;
	free(result);
	return MOK;
}

static void
add_error(struct MapiResultSet *result, char *error)
{
	/* concatenate the error messages */
	size_t size = result->errorstr ? strlen(result->errorstr) : 0;

	if (strlen(error) > 6 && error[5] == '!' &&
	    (isdigit((unsigned char) error[0]) ||
	     (error[0] >= 'A' && error[0] <= 'Z')) &&
	    (isdigit((unsigned char) error[1]) ||
	     (error[1] >= 'A' && error[1] <= 'Z')) &&
	    (isdigit((unsigned char) error[2]) ||
	     (error[2] >= 'A' && error[2] <= 'Z')) &&
	    (isdigit((unsigned char) error[3]) ||
	     (error[3] >= 'A' && error[3] <= 'Z')) &&
	    (isdigit((unsigned char) error[4]) ||
	     (error[4] >= 'A' && error[4] <= 'Z'))) {
		if (result->errorstr == NULL) {
			/* remeber SQLSTATE for first error */
			strcpy_len(result->sqlstate, error,
				   sizeof(result->sqlstate));
		}
		/* skip SQLSTATE */
		error += 6;
	}
	REALLOC(result->errorstr, size + strlen(error) + 2);
	if (result->errorstr == NULL)
		result->errorstr = mapi_nomem;
	else {
		strcpy(result->errorstr + size, error);
		strcat(result->errorstr + size, "\n");
	}
}

const char *
mapi_result_error(MapiHdl hdl)
{
	return hdl && hdl->result ? hdl->result->errorstr : NULL;
}

const char *
mapi_result_errorcode(MapiHdl hdl)
{
	return hdl && hdl->result && hdl->result->sqlstate[0] ? hdl->result->sqlstate : NULL;
}

/* Go to the next result set, if any, and close the current result
   set.  This function returns 1 if there are more result sets after
   the one that was closed, otherwise, if more input is needed, return
   MMORE, else, return MOK */
MapiMsg
mapi_next_result(MapiHdl hdl)
{
	mapi_hdl_check(hdl);

	while (hdl->result != NULL) {
		if (close_result(hdl) != MOK)
			return MERROR;
		if (hdl->result &&
		    (hdl->result->querytype == -1 ||
		     /* basically exclude Q_PARSE and Q_BLOCK */
		     (hdl->result->querytype >= Q_TABLE &&
		      hdl->result->querytype <= Q_PREPARE) ||
		     hdl->result->errorstr != NULL))
			return 1;
	}
	return hdl->needmore ? MMORE : MOK;
}

MapiMsg
mapi_needmore(MapiHdl hdl)
{
	return hdl->needmore ? MMORE : MOK;
}

bool
mapi_more_results(MapiHdl hdl)
{
	struct MapiResultSet *result;

	mapi_hdl_check(hdl);

	if ((result = hdl->result) == 0) {
		/* there are no results at all */
		return false;
	}
	if (result->querytype == Q_TABLE && hdl->mid->active == hdl) {
		/* read until next result (if any) */
		read_into_cache(hdl, -1);
	}
	if (hdl->needmore) {
		/* assume the application will provide more data and
		   that we will then have a result */
		return true;
	}
	while (result->next) {
		result = result->next;
		if (result->querytype == -1 ||
		    /* basically exclude Q_PARSE and Q_BLOCK */
		    (hdl->result->querytype >= Q_TABLE &&
		     hdl->result->querytype <= Q_PREPARE) ||
		    result->errorstr != NULL)
			return true;
	}
	/* no more results */
	return false;
}

MapiHdl
mapi_new_handle(Mapi mid)
{
	MapiHdl hdl;

	mapi_check0(mid);

	hdl = malloc(sizeof(*hdl));
	if (hdl == NULL) {
		mapi_setError(mid, "Memory allocation failure", __func__, MERROR);
		return NULL;
	}
	/* initialize and add to doubly-linked list */
	*hdl = (struct MapiStatement) {
		.mid = mid,
		.next = mid->first,
	};
	mid->first = hdl;
	if (hdl->next)
		hdl->next->prev = hdl;
	return hdl;
}

/* close all result sets on the handle but don't close the handle itself */
static MapiMsg
finish_handle(MapiHdl hdl)
{
	Mapi mid;
	int i;

	if (hdl == NULL)
		return MERROR;
	mid = hdl->mid;
	if (mid->active == hdl && !hdl->needmore && !mnstr_eof(mid->from) &&
	    read_into_cache(hdl, 0) != MOK)
		return MERROR;
	if (mid->to) {
		if (hdl->needmore) {
			assert(mid->active == NULL || mid->active == hdl);
			hdl->needmore = false;
			mid->active = hdl;
			mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
			check_stream(mid, mid->to, "write error on stream", mid->error);
			read_into_cache(hdl, 0);
		}
		for (i = 0; i < hdl->npending_close; i++) {
			char msg[256];

			snprintf(msg, sizeof(msg), "Xclose %d\n", hdl->pending_close[i]);
			mapi_log_record(mid, "CMD", "%s", msg);
			mid->active = hdl;
			if (mnstr_printf(mid->to, "%s", msg) < 0 ||
			    mnstr_flush(mid->to, MNSTR_FLUSH_DATA)) {
				close_connection(mid);
				mapi_setError(mid, mnstr_peek_error(mid->to), __func__, MTIMEOUT);
				break;
			}
			read_into_cache(hdl, 0);
		}
	}
	hdl->npending_close = 0;
	if (hdl->pending_close)
		free(hdl->pending_close);
	hdl->pending_close = NULL;
	while (hdl->result) {
		if (close_result(hdl) != MOK)
			return MERROR;
		if (hdl->needmore) {
			assert(mid->active == NULL || mid->active == hdl);
			hdl->needmore = false;
			mid->active = hdl;
			mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
			check_stream(mid, mid->to, "write error on stream", mid->error);
			read_into_cache(hdl, 0);
		}
	}
	return MOK;
}

/* Close a statement handle, discarding any unread output. */
MapiMsg
mapi_close_handle(MapiHdl hdl)
{
	if (hdl == NULL)
		return MOK;
	debugprint("entering %s\n", "mapi_close_handle");

	/* don't use mapi_check_hdl: it's ok if we're not connected */
	mapi_clrError(hdl->mid);

	(void) finish_handle(hdl);
	hdl->npending_close = 0;
	if (hdl->pending_close)
		free(hdl->pending_close);
	hdl->pending_close = NULL;
	if (hdl->bindings)
		free(hdl->bindings);
	hdl->bindings = NULL;
	hdl->maxbindings = 0;
	if (hdl->params)
		free(hdl->params);
	hdl->params = NULL;
	hdl->maxparams = 0;
	if (hdl->query)
		free(hdl->query);
	hdl->query = NULL;
	if (hdl->template)
		free(hdl->template);
	hdl->template = NULL;
	/* remove from doubly-linked list */
	if (hdl->prev)
		hdl->prev->next = hdl->next;
	if (hdl->next)
		hdl->next->prev = hdl->prev;
	if (hdl->mid->first == hdl)
		hdl->mid->first = hdl->next;
	hdl->prev = NULL;
	hdl->next = NULL;
	hdl->mid = NULL;
	free(hdl);
	return MOK;
}

const struct MapiStruct MapiStructDefaults = {
	.error = MOK,
	.redirmax = 10,
	.blk.eos = false,
	.blk.lim = BLOCK,
};

/* Allocate a new connection handle. */
Mapi
mapi_new(msettings *settings)
{
	Mapi mid;
	static ATOMIC_TYPE index = ATOMIC_VAR_INIT(0);

	mid = malloc(sizeof(*mid));
	if (mid == NULL)
		return NULL;
	if (settings == NULL)
		settings = msettings_create();
	if (settings == NULL) {
		free(mid);
		return NULL;
	}

	/* then fill in some details */
	*mid = MapiStructDefaults;
	mid->settings = settings;
	mid->index =  (uint32_t) ATOMIC_ADD(&index, 1); /* for distinctions in log records */
	if ((mid->blk.buf = malloc(mid->blk.lim + 1)) == NULL) {
		mapi_destroy(mid);
		return NULL;
	}
	mid->blk.buf[0] = 0;
	mid->blk.buf[mid->blk.lim] = 0;

	/* also the current timezone, seconds EAST of UTC */
	long time_zone;
#if defined(_MSC_VER)
	DYNAMIC_TIME_ZONE_INFORMATION tzinf;

	/* documentation says: UTC = localtime + Bias (in minutes),
	 * but experimentation during DST period says, UTC = localtime
	 * + Bias + DaylightBias, and presumably during non DST
	 * period, UTC = localtime + Bias */
	switch (GetDynamicTimeZoneInformation(&tzinf)) {
	case TIME_ZONE_ID_STANDARD:	/* using standard time */
	case TIME_ZONE_ID_UNKNOWN:	/* no daylight saving time in this zone */
		time_zone = -(int) tzinf.Bias * 60;
		break;
	case TIME_ZONE_ID_DAYLIGHT:	/* using daylight saving time */
		time_zone = -(int) (tzinf.Bias + tzinf.DaylightBias) * 60;
		break;
	default:					/* aka TIME_ZONE_ID_INVALID */
		/* call failed, we don't know the time zone */
		time_zone = 0;
		break;
	}
#else
	time_t t = time(NULL);
	struct tm *local_tm = localtime_r(&t, &(struct tm){0});
#ifdef HAVE_TM_GMTOFF
	time_zone = local_tm->tm_gmtoff;
#else
	struct tm *gm_tm = gmtime_r(&t, &(struct tm){0});
	time_t gt = mktime(gm_tm);
	local_tm->tm_isdst=0; /* We need the difference without dst */
	time_t lt = mktime(local_tm);
	assert((int64_t) gt - (int64_t) lt >= (int64_t) INT_MIN && (int64_t) gt - (int64_t) lt <= (int64_t) INT_MAX);
	time_zone = (long) (lt - gt);
#endif
#endif
	msettings_error err =  msetting_set_long(mid->settings, MP_TIMEZONE, time_zone);
	if (err)
		mapi_setError(mid, err, __func__, MERROR);

	return mid;
}

/* construct the uri field of a Mapi struct */
void
set_uri(Mapi mid)
{
	const char *host = msetting_string(mid->settings, MP_HOST);
	const char *database = msetting_string(mid->settings, MP_DATABASE);
	int port = msetting_long(mid->settings, MP_PORT);
	size_t urilen = strlen(host) + (database ? strlen(database) : 0) + 32;
	char *uri = malloc(urilen);

	/* uri looks as follows:
	 *  mapi:monetdb://host:port/database
	 * or
	 *  mapi:monetdb:///some/path/to?database=database
	 */

	if (database != NULL) {
		if (host[0] == '/') {
			snprintf(uri, urilen, "mapi:monetdb://%s?database=%s",
				 host, database);
		} else {
			snprintf(uri, urilen, "mapi:monetdb://%s:%d/%s",
				 host, port, database);
		}
	} else {
		if (host[0] == '/') {
			snprintf(uri, urilen, "mapi:monetdb://%s",
				 host);
		} else {
			snprintf(uri, urilen, "mapi:monetdb://%s:%d",
				 host, port);
		}
	}

	if (mid->uri != NULL)
		free(mid->uri);
	mid->uri = uri;
}

Mapi
mapi_mapiuri(const char *url, const char *user, const char *pass, const char *lang)
{
	Mapi mid;

	if (!ATOMIC_TAS(&mapi_initialized)) {
		if (mnstr_init() < 0)
			return NULL;
	}

	mid = mapi_new(NULL);
	if (mid == NULL)
		return NULL;

	if (url == NULL) {
		mapi_setError(mid, "url is null", __func__, MERROR);
		return mid;
	}
	if (user == NULL) {
		mapi_setError(mid, "user is null", __func__, MERROR);
		return mid;
	}
	if (pass == NULL) {
		mapi_setError(mid, "pass is null", __func__, MERROR);
		return mid;
	}
	if (lang == NULL) {
		mapi_setError(mid, "lang is null", __func__, MERROR);
		return mid;
	}

	msettings_error err = NULL;
	if (	(err = msetting_set_string(mid->settings, MP_USER, user))
		|| (err = msetting_set_string(mid->settings, MP_PASSWORD, pass))
		|| (err = msetting_set_string(mid->settings, MP_LANGUAGE, lang))
	) {
		// in the old implementation we returned NULL but that doesn't
		// make sense to me because above we already started returning
		// mid with the error set.
		mapi_setError(mid, err, __func__, MERROR);
		return mid;
	}

	char *error_message = NULL;
	if (!msettings_parse_url(mid->settings, url, &error_message)) {
		char *msg = error_message ? error_message : "malloc failed";
		mapi_setError(mid, msg, __func__, MERROR);
		free(error_message);
		return mid;
	}

	set_uri(mid);

	return mid;
}

/* Allocate a new connection handle and fill in the information needed
   to connect to a server, but don't connect yet. */
Mapi
mapi_mapi(const char *host, int port, const char *username,
	  const char *password, const char *lang, const char *dbname)
{
	Mapi mid;

	if (!ATOMIC_TAS(&mapi_initialized)) {
		if (mnstr_init() < 0)
			return NULL;
	}

	mid = mapi_new(NULL);
	if (mid == NULL)
		return NULL;
	msettings *settings = mid->settings;

	if (lang == NULL)
		lang = "sql";

	const char *sockdir = NULL;
	if (host && host[0] == '/') {
		sockdir = host;
		host = NULL;
	}

	msettings_error err = NULL;
	do {
		if (host && (err = msetting_set_string(settings, MP_HOST, host)))
			break;
		if (sockdir && (err = msetting_set_string(settings, MP_SOCKDIR, sockdir)))
			break;
		if (username && (err = msetting_set_string(settings, MP_USER, username)))
			break;
		if (password && (err = msetting_set_string(settings, MP_PASSWORD, password)))
			break;
		if (lang && (err = msetting_set_string(settings, MP_LANGUAGE, lang)))
			break;
		if (dbname && (err = msetting_set_string(settings, MP_DATABASE, dbname)))
			break;
		if (port > 0 && (err = msetting_set_long(settings, MP_PORT, port)))
			break;
	} while (0);
	if (err) {
		mapi_setError(mid, err, __func__, MERROR);
		return mid;
	}

	return mid;
}

Mapi
mapi_settings(msettings *settings)
{
	assert(settings);
	Mapi mid = mapi_new(settings);
	if (mid == NULL)
		return mid;

	set_uri(mid);
	return mid;
}


/* Close a connection and free all memory associated with the
   connection handle. */
MapiMsg
mapi_destroy(Mapi mid)
{
	char **r;

	mapi_clrError(mid);

	while (mid->first)
		mapi_close_handle(mid->first);
	if (mid->connected)
		(void) mapi_disconnect(mid);
	if (mid->tracelog)
		close_stream(mid->tracelog);

	free(mid->blk.buf);
	free(mid->motd);
	free(mid->server);
	free(mid->uri);
	free(mid->tracebuffer);
	free(mid->noexplain);
	if (mid->errorstr && mid->errorstr != mapi_nomem)
		free(mid->errorstr);

	msettings_destroy(mid->settings);

	r = mid->redirects;
	while (*r) {
		free(*r);
		r++;
	}

	free(mid);
	return MOK;
}

/* Create a connection handle and connect to the server using the
   specified parameters. */
Mapi
mapi_connect(const char *host, int port, const char *username, const char *password, const char *lang, const char *dbname)
{
	Mapi mid;

	mid = mapi_mapi(host, port, username, password, lang, dbname);
	if (mid && mid->error == MOK)
		mapi_reconnect(mid);	/* actually, initial connect */
	return mid;
}

/* Returns an malloced NULL-terminated array with redirects */
char **
mapi_resolve(const char *host, int port, const char *pattern)
{
	int rmax;
	Mapi mid;

	/* if it doesn't make sense, don't try to crash */
	if (pattern == NULL)
		return NULL;

	mid = mapi_mapi(host, port, "mero", "mero", "resolve", pattern);
	if (mid) {
		if (mid->error == MOK) {
			rmax = mid->redirmax;
			mid->redirmax = 0;
			mapi_reconnect(mid);	/* real connect, don't follow redirects */
			mid->redirmax = rmax;
			if (mid->error == MOK) {
				close_connection(mid);	/* we didn't expect a connection actually */
			} else {
				char **ret = malloc(sizeof(char *) * MAXREDIR);
				memcpy(ret, mid->redirects, sizeof(char *) * MAXREDIR);
				mid->redirects[0] = NULL;	/* make sure the members aren't freed */
				mapi_destroy(mid);
				return ret;
			}
		}
		mapi_destroy(mid);
	}
	return NULL;
}

void
close_connection(Mapi mid)
{
	MapiHdl hdl;
	struct MapiResultSet *result;

	mid->connected = false;
	mid->active = NULL;
	for (hdl = mid->first; hdl; hdl = hdl->next) {
		hdl->active = NULL;
		for (result = hdl->result; result; result = result->next)
			result->tableid = -1;
	}
	/* finish channels */
	/* Make sure that the write- (to-) stream is closed first,
	 * as the related read- (from-) stream closes the shared
	 * socket; see also src/common/stream.c:socket_close .
	 */
	if (mid->to) {
		close_stream(mid->to);
		mid->to = 0;
	}
	if (mid->from) {
		close_stream(mid->from);
		mid->from = 0;
	}
	mid->redircnt = 0;
	mapi_log_record(mid, "C", "Connection closed");
}

MapiMsg
mapi_disconnect(Mapi mid)
{
	mapi_check(mid);

	close_connection(mid);
	return MOK;
}

/* Set callback function to retrieve or send file content for COPY
 * INTO queries.
 *
 * char *getfile(void *private, const char *filename, bool binary,
 *               uint64_6 offset, size_t *size);
 * Retrieve data from a file.
 *
 * The arguments are:
 * private - the value of the filecontentprivate argument to
 *           mapi_setfilecallback;
 * filename - the file to read (the application is free to interpret
 *            this any way it wants, including getting data over the
 *            Internet);
 * binary - if set, the file is expected to contain binary data and
 *          should therefore be opened in binary mode, otherwise the
 *          file is expected to contain data in the UTF-8 encoding (of
 *          course, the application is free to transparently convert
 *          from the actual encoding to UTF-8);
 * offset - the line number of the first line to be retrieved (this is
 *          one-based, i.e. the start of the file has line number one;
 *          lines are terminated by '\n');
 * size - pointer in which to return the size of the chunk that is
 *        being returned.
 *
 * The callback function is expected to return data in chunks until it
 * indicates to the caller that there is no more data or an error has
 * occurred.  Chunks can be any size.  The caller does not modify or
 * free the data returned.  The size of the chunk being returned is
 * stored in the size argument.  Errors are indicated by returning a
 * string containing an error message and setting *size to zero.  The
 * error message should not contain any newlines.  Any call to the
 * callback function is allowed to return an error.
 *
 * The first call to the callback function contains values for
 * filename, binary, and offset.  These parameters are all 0 for all
 * subsequent calls for continuation data from the same file.
 *
 * If the caller has retrieved enough data before the file is
 * exhausted, it calls the callback function one more time with a NULL
 * pointer for the size argument.  This gives the callback function
 * the opportunity to free its resources (e.g. close the file).
 *
 * If there is no more data to be returned, the callback function
 * returns a NULL pointer and sets *size to zero.  No more calls for
 * the current file will be made.
 *
 * Note that if the file to be read is empty, or contains fewer lines
 * than the requested offset, the first call to the callback function
 * may return NULL.
 *
 * char *putfile(void *private, const char *filename, bool binary,
 *               const void *data, size_t size);
 * Send data to a file.
 *
 * The arguments are:
 * private - the value of the filecontentprivate argument to
 *           mapi_setfilecallback;
 * filename - the file to be written;
 * binary - if set, the data to be written is binary and the file
 *          should therefore be opened in binary mode, otherwise the
 *          data is UTF-8 encoded text;
 * data - the data to be written;
 * size - the size of the data to be written.
 *
 * The callback is called multiple time to write a single file.  The
 * first time, a filename is specified, all subsequent times, the
 * filename argument is NULL.  When all data has been written, the
 * callback function is called one last time with NULL pointer for the
 * data argument so that the callback function can free any resources.
 *
 * When an error occurs, the callback function returns a string
 * containing an error message after which the callback will not be
 * called again for the same file.  Otherwise, the callback function
 * returns NULL.
 *
 * Note also that multibyte sequences may be split over two calls.
 */
void
mapi_setfilecallback2(Mapi mid,
		     char *(*getfilecontent)(void *,
					     const char *, bool,
					     uint64_t, size_t *),
		     char *(*putfilecontent)(void *,
					     const char *, bool,
					     const void *, size_t),
		     void *filecontentprivate)
{
	mid->getfilecontent = getfilecontent;
	mid->putfilecontent = putfilecontent;
	mid->filecontentprivate = filecontentprivate;
	mid->putfilecontent_old = NULL;
	mid->filecontentprivate_old = NULL;
}

static char *
putfilecontent_wrap(void *priv, const char *filename, bool binary, const void *data, size_t size)
{
	Mapi mid = priv;
	void *priv_old = mid->filecontentprivate_old;
	if (filename && binary)
		return "Client does not support writing binary files";
	return mid->putfilecontent_old(priv_old, filename, data, size);
}

/* DEPRECATED. Set callback function to retrieve or send file content for COPY
 * INTO queries.
 *
 * Deprecated because it does not support binary downloads.
 * Use mapi_setfilecallback2 instead.
 */
void
mapi_setfilecallback(Mapi mid,
		     char *(*getfilecontent)(void *,
					     const char *, bool,
					     uint64_t, size_t *),
		     char *(*putfilecontent)(void *,
					     const char *,
					     const void *, size_t),
		     void *filecontentprivate)
{
	mid->getfilecontent = getfilecontent;
	mid->putfilecontent = putfilecontent_wrap;
	mid->filecontentprivate = mid;
	mid->putfilecontent_old = putfilecontent;
	mid->filecontentprivate_old = filecontentprivate;
}

#define testBinding(hdl,fnr)						\
	do {								\
		mapi_hdl_check(hdl);					\
		if (fnr < 0) {						\
			return mapi_setError(hdl->mid,			\
					     "Illegal field number",	\
					     __func__, MERROR);		\
		}							\
		/* make sure there is enough space */			\
		if (fnr >= hdl->maxbindings)				\
			mapi_extend_bindings(hdl, fnr);			\
	} while (0)

#define testParam(hdl, fnr)						\
	do {								\
		mapi_hdl_check(hdl);					\
		if (fnr < 0) {						\
			return mapi_setError(hdl->mid,			\
					     "Illegal param number",	\
					     __func__, MERROR);		\
		}							\
		if (fnr >= hdl->maxparams)				\
			mapi_extend_params(hdl, fnr);			\
	} while (0)

MapiMsg
mapi_bind(MapiHdl hdl, int fnr, char **ptr)
{
	testBinding(hdl, fnr);
	hdl->bindings[fnr].outparam = ptr;

	hdl->bindings[fnr].outtype = MAPI_AUTO;
	return MOK;
}

MapiMsg
mapi_bind_var(MapiHdl hdl, int fnr, int type, void *ptr)
{
	testBinding(hdl, fnr);
	hdl->bindings[fnr].outparam = ptr;

	if (type >= 0 && type < MAPI_NUMERIC)
		hdl->bindings[fnr].outtype = type;
	else
		return mapi_setError(hdl->mid, "Illegal SQL type identifier", __func__, MERROR);
	return MOK;
}

MapiMsg
mapi_bind_numeric(MapiHdl hdl, int fnr, int scale, int prec, void *ptr)
{
	if (mapi_bind_var(hdl, fnr, MAPI_NUMERIC, ptr))
		return hdl->mid->error;

	hdl->bindings[fnr].scale = scale;
	hdl->bindings[fnr].precision = prec;
	return MOK;
}

MapiMsg
mapi_clear_bindings(MapiHdl hdl)
{
	mapi_hdl_check(hdl);
	if (hdl->bindings)
		memset(hdl->bindings, 0, hdl->maxbindings * sizeof(*hdl->bindings));
	return MOK;
}

MapiMsg
mapi_param_type(MapiHdl hdl, int fnr, int ctype, int sqltype, void *ptr)
{
	testParam(hdl, fnr);
	hdl->params[fnr].inparam = ptr;

	if (ctype >= 0 && ctype < MAPI_NUMERIC)
		hdl->params[fnr].intype = ctype;
	else
		return mapi_setError(hdl->mid, "Illegal SQL type identifier", __func__, MERROR);
	hdl->params[fnr].sizeptr = NULL;
	hdl->params[fnr].outtype = sqltype;
	hdl->params[fnr].scale = 0;
	hdl->params[fnr].precision = 0;
	return MOK;
}

MapiMsg
mapi_param_string(MapiHdl hdl, int fnr, int sqltype, char *ptr, int *sizeptr)
{
	testParam(hdl, fnr);
	hdl->params[fnr].inparam = (void *) ptr;

	hdl->params[fnr].intype = MAPI_VARCHAR;
	hdl->params[fnr].sizeptr = sizeptr;
	hdl->params[fnr].outtype = sqltype;
	hdl->params[fnr].scale = 0;
	hdl->params[fnr].precision = 0;
	return MOK;
}

MapiMsg
mapi_param(MapiHdl hdl, int fnr, char **ptr)
{
	return mapi_param_type(hdl, fnr, MAPI_AUTO, MAPI_AUTO, ptr);
}

MapiMsg
mapi_param_numeric(MapiHdl hdl, int fnr, int scale, int prec, void *ptr)
{
	if (mapi_param_type(hdl, fnr, MAPI_NUMERIC, MAPI_NUMERIC, ptr))
		return hdl->mid->error;

	hdl->params[fnr].scale = scale;
	hdl->params[fnr].precision = prec;
	return MOK;
}

MapiMsg
mapi_clear_params(MapiHdl hdl)
{
	mapi_hdl_check(hdl);
	if (hdl->params)
		memset(hdl->params, 0, hdl->maxparams * sizeof(*hdl->params));
	return MOK;
}

static MapiHdl
prepareQuery(MapiHdl hdl, const char *cmd)
{
	if (hdl && cmd) {
		if (hdl->query)
			free(hdl->query);
		hdl->query = strdup(cmd);
		assert(hdl->query);
		if (hdl->template) {
			free(hdl->template);
			hdl->template = NULL;
		}
	}
	return hdl;
}


MapiMsg
mapi_set_timeout(Mapi mid, unsigned int timeout, bool (*callback)(void *), void *callback_data)
{
	mapi_check(mid);
	if (mid->trace)
		printf("Set timeout to %u\n", timeout);
	mnstr_settimeout(mid->to, timeout, callback, callback_data);
	mnstr_settimeout(mid->from, timeout, callback, callback_data);
	return MOK;
}

MapiMsg
mapi_timeout(Mapi mid, unsigned int timeout)
{
	return mapi_set_timeout(mid, timeout, NULL, NULL);
}

MapiMsg
mapi_Xcommand(Mapi mid, const char *cmdname, const char *cmdvalue)
{
	MapiHdl hdl;

	mapi_check(mid);
	if (mid->active && read_into_cache(mid->active, 0) != MOK)
		return MERROR;
	if (mnstr_printf(mid->to, "X" "%s %s\n", cmdname, cmdvalue) < 0 ||
	    mnstr_flush(mid->to, MNSTR_FLUSH_DATA)) {
		close_connection(mid);
		mapi_setError(mid, mnstr_peek_error(mid->to), __func__, MTIMEOUT);
		return MERROR;
	}
	mapi_log_record(mid, "X", "X" "%s %s\n", cmdname, cmdvalue);
	hdl = prepareQuery(mapi_new_handle(mid), "Xcommand");
	if (hdl == NULL)
		return MERROR;
	mid->active = hdl;
	read_into_cache(hdl, 0);
	mapi_close_handle(hdl);	/* reads away any output */
	return MOK;
}

MapiMsg
mapi_prepare_handle(MapiHdl hdl, const char *cmd)
{
	mapi_hdl_check(hdl);
	if (finish_handle(hdl) != MOK)
		return MERROR;
	prepareQuery(hdl, cmd);
	hdl->template = strdup(hdl->query);
	assert(hdl->template);
	return hdl->mid->error;
}

MapiHdl
mapi_prepare(Mapi mid, const char *cmd)
{
	MapiHdl hdl;

	mapi_check0(mid);
	hdl = mapi_new_handle(mid);
	if (hdl == NULL)
		return NULL;
	mapi_prepare_handle(hdl, cmd);
	return hdl;
}

/*
 * Building the query string using replacement of values requires
 * some care to not overflow the space allocated.
 */
#define checkSpace(len)						\
	do {							\
		/* note: k==strlen(hdl->query) */		\
		if (k+len >= lim) {				\
			lim = k + len + MAPIBLKSIZE;		\
			char *q = realloc(hdl->query, lim);	\
			if (q == NULL) {			\
				free(hdl->query);		\
				hdl->query = NULL;		\
				return;				\
			}					\
			hdl->query = q;				\
		}						\
	} while (0)

static void
mapi_param_store(MapiHdl hdl)
{
	char *val, buf[MAPIBLKSIZE];
	char *p = hdl->template, *q;
	int i;
	size_t k;
	size_t lim;

	if (hdl->template == 0)
		return;

	lim = strlen(hdl->template) + MAPIBLKSIZE;
	REALLOC(hdl->query, lim);
	if (hdl->query == NULL)
		return;
	hdl->query[0] = 0;
	k = 0;

	q = strchr(hdl->template, PLACEHOLDER);
	i = 0;
	/* loop invariant: k == strlen(hdl->query) */
	while (q && i < hdl->maxparams) {
		if (q > p && *(q - 1) == '\\') {
			q = strchr(q + 1, PLACEHOLDER);
			continue;
		}

		if (k + (q - p) >= lim) {
			lim += MAPIBLKSIZE;
			REALLOC(hdl->query, lim);
			if (hdl->query == NULL)
				return;
		}
		memcpy(hdl->query + k, p, q - p);
		k += q - p;
		hdl->query[k] = 0;

		if (hdl->params[i].inparam == 0) {
			char *nullstr = "NULL";
			checkSpace(5);
			if (msettings_lang_is_mal(hdl->mid->settings))
				nullstr = "nil";
			strcpy(hdl->query + k, nullstr);
		} else {
			void *src = hdl->params[i].inparam;	/* abbrev */

			switch (hdl->params[i].intype) {
			case MAPI_TINY:
				checkSpace(5);
				snprintf(hdl->query + k, lim - k, "%hhd", *(signed char *) src);
				break;
			case MAPI_UTINY:
				checkSpace(5);
				snprintf(hdl->query + k, lim - k, "%hhu", *(unsigned char *) src);
				break;
			case MAPI_SHORT:
				checkSpace(10);
				snprintf(hdl->query + k, lim - k, "%hd", *(short *) src);
				break;
			case MAPI_USHORT:
				checkSpace(10);
				snprintf(hdl->query + k, lim - k, "%hu", *(unsigned short *) src);
				break;
			case MAPI_INT:
				checkSpace(20);
				snprintf(hdl->query + k, lim - k, "%d", *(int *) src);
				break;
			case MAPI_UINT:
				checkSpace(20);
				snprintf(hdl->query + k, lim - k, "%u", *(unsigned int *) src);
				break;
			case MAPI_LONG:
				checkSpace(20);
				snprintf(hdl->query + k, lim - k, "%ld", *(long *) src);
				break;
			case MAPI_ULONG:
				checkSpace(20);
				snprintf(hdl->query + k, lim - k, "%lu", *(unsigned long *) src);
				break;
			case MAPI_LONGLONG:
				checkSpace(30);
				snprintf(hdl->query + k, lim - k, "%"PRId64, *(int64_t *) src);
				break;
			case MAPI_ULONGLONG:
				checkSpace(30);
				snprintf(hdl->query + k, lim - k, "%"PRIu64, *(uint64_t *) src);
				break;
			case MAPI_FLOAT:
				checkSpace(30);
				snprintf(hdl->query + k, lim - k, "%.9g", *(float *) src);
				break;
			case MAPI_DOUBLE:
				checkSpace(30);
				snprintf(hdl->query + k, lim - k, "%.17g", *(double *) src);
				break;
			case MAPI_DATE:
				checkSpace(50);
				snprintf(hdl->query + k, lim - k,
					 "DATE '%04hd-%02hu-%02hu'",
					 ((MapiDate *) src)->year,
					 ((MapiDate *) src)->month,
					 ((MapiDate *) src)->day);
				break;
			case MAPI_TIME:
				checkSpace(60);
				snprintf(hdl->query + k, lim - k,
					 "TIME '%02hu:%02hu:%02hu'",
					 ((MapiTime *) src)->hour,
					 ((MapiTime *) src)->minute,
					 ((MapiTime *) src)->second);
				break;
			case MAPI_DATETIME:
				checkSpace(110);
				snprintf(hdl->query + k, lim - k,
					 "TIMESTAMP '%04hd-%02hu-%02hu %02hu:%02hu:%02hu.%09u'",
					 ((MapiDateTime *) src)->year,
					 ((MapiDateTime *) src)->month,
					 ((MapiDateTime *) src)->day,
					 ((MapiDateTime *) src)->hour,
					 ((MapiDateTime *) src)->minute,
					 ((MapiDateTime *) src)->second,
					 ((MapiDateTime *) src)->fraction);
				break;
			case MAPI_CHAR:
				buf[0] = *(char *) src;
				buf[1] = 0;
				val = mapi_quote(buf, 1);
				/* note: k==strlen(hdl->query) */
				if (k + strlen(val) + 3 >= lim) {
					lim = k + strlen(val) + 3 + MAPIBLKSIZE;
					char *q = realloc(hdl->query, lim);
					if (q == NULL) {
						free(hdl->query);
						hdl->query = NULL;
						free(val);
						return;
					}
					hdl->query = q;
				}
				snprintf(hdl->query + k, lim - k, "'%s'", val);
				free(val);
				break;
			case MAPI_VARCHAR:
				val = mapi_quote((char *) src, hdl->params[i].sizeptr ? *hdl->params[i].sizeptr : -1);
				/* note: k==strlen(hdl->query) */
				if (k + strlen(val) + 3 >= lim) {
					lim = k + strlen(val) + 3 + MAPIBLKSIZE;
					char *q = realloc(hdl->query, lim);
					if (q == NULL) {
						free(hdl->query);
						hdl->query = NULL;
						free(val);
						return;
					}
					hdl->query = q;
				}
				snprintf(hdl->query + k, lim - k, "'%s'", val);
				free(val);
				break;
			default:
				strcpy_len(hdl->query + k, src, lim - k);
				break;
			}
		}
		k += strlen(hdl->query + k);

		i++;
		p = q + 1;
		q = strchr(p, PLACEHOLDER);
	}
	checkSpace(strlen(p) + 1);
	strcpy(hdl->query + k, p);
	if (hdl->mid->trace)
		printf("param_store: result=%s\n", hdl->query);
	return;
}

/* Read one more line from the input stream and return it.  This
   returns a pointer into the input buffer, so the data needs to be
   copied if it is to be retained. */
static char *
read_line(Mapi mid)
{
	char *reply;
	char *nl;
	char *s;		/* from where to search for newline */

	if (mid->active == NULL)
		return NULL;

	/* check if we need to read more blocks to get a new line */
	mid->blk.eos = false;
	s = mid->blk.buf + mid->blk.nxt;
	while ((nl = strchr(s, '\n')) == NULL && !mid->blk.eos) {
		ssize_t len;

		if (mid->blk.lim - mid->blk.end < BLOCK) {
			int len;

			len = mid->blk.lim;
			if (mid->blk.nxt <= BLOCK) {
				/* extend space */
				len += BLOCK;
			}
			REALLOC(mid->blk.buf, len + 1);
			if (mid->blk.nxt > 0) {
				memmove(mid->blk.buf, mid->blk.buf + mid->blk.nxt, mid->blk.end - mid->blk.nxt + 1);
				mid->blk.end -= mid->blk.nxt;
				mid->blk.nxt = 0;
			}
			mid->blk.lim = len;
		}

		s = mid->blk.buf + mid->blk.end;

		/* fetch one more block */
		if (mid->trace)
			printf("fetch next block: start at:%d\n", mid->blk.end);
		for (;;) {
			len = mnstr_read(mid->from, mid->blk.buf + mid->blk.end, 1, BLOCK);
			if (len == -1 && mnstr_errnr(mid->from) == MNSTR_INTERRUPT) {
				mnstr_clearerr(mid->from);
				if (mid->oobintr && !mid->active->aborted) {
					mid->active->aborted = true;
					mnstr_putoob(mid->to, 1);
				}
			} else
				break;
		}
		check_stream(mid, mid->from, "Connection terminated during read line", (mid->blk.eos = true, (char *) 0));
		mapi_log_data(mid, "RECV", mid->blk.buf + mid->blk.end, len);
		mid->blk.buf[mid->blk.end + len] = 0;
		if (mid->trace) {
			printf("got next block: length:%zd\n", len);
			printf("text:%s\n", mid->blk.buf + mid->blk.end);
		}
		if (len == 0) {	/* add prompt */
			if (mnstr_eof(mid->from))
				return NULL;
			if (mid->blk.end > mid->blk.nxt) {
				/* add fake newline since newline was
				 * missing from server */
				nl = mid->blk.buf + mid->blk.end;
				*nl = '\n';
				mid->blk.end++;
			}
			len = 2;
			mid->blk.buf[mid->blk.end] = PROMPTBEG;
			mid->blk.buf[mid->blk.end + 1] = '\n';
			mid->blk.buf[mid->blk.end + 2] = 0;
		}
		mid->blk.end += (int) len;
	}
	if (mid->trace) {
		printf("got complete block: \n");
		printf("text:%s\n", mid->blk.buf + mid->blk.nxt);
	}

	/* we have a complete line in the buffer */
	assert(nl);
	*nl++ = 0;
	reply = mid->blk.buf + mid->blk.nxt;
	mid->blk.nxt = (int) (nl - mid->blk.buf);

	if (mid->trace)
		printf("read_line:%s\n", reply);
	return reply;
}

/* set or unset the autocommit flag in the server */
MapiMsg
mapi_setAutocommit(Mapi mid, bool autocommit)
{
	if (msetting_bool(mid->settings, MP_AUTOCOMMIT) == autocommit)
		return MOK;
	if (!msettings_lang_is_sql(mid->settings)) {
		mapi_setError(mid, "autocommit only supported in SQL", __func__, MERROR);
		return MERROR;
	}
	msettings_error err = msetting_set_bool(mid->settings, MP_AUTOCOMMIT, autocommit);
	if (err)
		return mapi_setError(mid, err, __func__, MERROR);
	if (!mid->connected)
		return MOK;
	if (autocommit)
		return mapi_Xcommand(mid, "auto_commit", "1");
	else
		return mapi_Xcommand(mid, "auto_commit", "0");
}

MapiMsg
mapi_set_time_zone(Mapi mid, int time_zone)
{
	msettings_error err = msetting_set_long(mid->settings, MP_TIMEZONE, time_zone);
	if (err)
		return mapi_setError(mid, err, __func__, MERROR);
	if (!mid->connected)
		return MOK;

	char buf[100];
	if (time_zone < 0)
		snprintf(buf, sizeof(buf),
			 "SET TIME ZONE INTERVAL '-%02d:%02d' HOUR TO MINUTE",
			 -time_zone / 3600, (-time_zone % 3600) / 60);
	else
		snprintf(buf, sizeof(buf),
			 "SET TIME ZONE INTERVAL '+%02d:%02d' HOUR TO MINUTE",
			 time_zone / 3600, (time_zone % 3600) / 60);

	MapiHdl hdl = mapi_query(mid, buf);
	if (hdl == NULL)
		return mid->error;
	mapi_close_handle(hdl);

	return MOK;
}

MapiMsg
mapi_set_columnar_protocol(Mapi mid, bool columnar_protocol)
{
	if (mid->columnar_protocol == columnar_protocol)
		return MOK;
	mid->columnar_protocol = columnar_protocol;
	if (!mid->connected)
		return MOK;
	if (columnar_protocol)
		return mapi_Xcommand(mid, "columnar_protocol", "1");
	else
		return mapi_Xcommand(mid, "columnar_protocol", "0");
}

MapiMsg
mapi_set_size_header(Mapi mid, bool value)
{
	if (!msettings_lang_is_sql(mid->settings)) {
		mapi_setError(mid, "size header only supported in SQL", __func__, MERROR);
		return MERROR;
	}
	if (mid->sizeheader == value)
		return MOK;
	mid->sizeheader = value;
	if (!mid->connected)
		return MOK;
	if (value)
		return mapi_Xcommand(mid, "sizeheader", "1");
	else
		return mapi_Xcommand(mid, "sizeheader", "0");
}

MapiMsg
mapi_release_id(Mapi mid, int id)
{
	char buf[10];

	if (!msettings_lang_is_sql(mid->settings)) {
		mapi_setError(mid, "release only supported in SQL", __func__, MERROR);
		return MERROR;
	}
	snprintf(buf, sizeof(buf), "%d", id);
	return mapi_Xcommand(mid, "release", buf);
}

void
mapi_trace(Mapi mid, bool flag)
{
	mapi_clrError(mid);
	mid->trace = flag;
}


static int
slice_row(const char *reply, char *null, char ***anchorsp, size_t **lensp, int length, int endchar)
{
	/* This function does the actual work for splicing a real,
	   multi-column row into columns.  It skips over the first
	   character and ends at the end of the string or at endchar,
	   whichever comes first. */
	char *start;
	char **anchors;
	int i;
	size_t len;
	size_t *lens;

	reply++;		/* skip over initial char (usually '[') */
	i = 0;
	anchors = length == 0 ? NULL : malloc(length * sizeof(*anchors));
	lens = length == 0 ? NULL : malloc(length * sizeof(*lens));
	for (;;) {
		if (i >= length) {
			length = i + 1;
			REALLOC(anchors, length);
			REALLOC(lens, length);
		}
		if (!unquote(reply, &start, &reply, endchar, &len) && null && strcmp(start, null) == 0) {
			/* indicate NULL/nil with NULL pointer */
			free(start);
			start = NULL;
			len = 0;
		}
		lens[i] = len;
		anchors[i++] = start;
		if (reply == NULL)
			break;
		while (*reply && isspace((unsigned char) *reply))
			reply++;
		if (*reply == ',') {
			reply++;
			while (*reply && isspace((unsigned char) *reply))
				reply++;
		} else if (*reply == 0 || *reply == endchar)
			break;
	}
	*anchorsp = anchors;
	*lensp = lens;
	return i;
}

static MapiMsg
mapi_cache_freeup_internal(struct MapiResultSet *result, int k)
{
	int i;			/* just a counter */
	int64_t n = 0;	/* # of tuples being deleted from front */

	result->cache.tuplecount = 0;
	for (i = 0; i < result->cache.writer - k; i++) {
		if (result->cache.line[i].rows) {
			if (result->cache.line[i].rows[0] == '[' ||
			    result->cache.line[i].rows[0] == '=')
				n++;
			free(result->cache.line[i].rows);
		}
		result->cache.line[i].rows = result->cache.line[i + k].rows;
		result->cache.line[i + k].rows = 0;
		if (result->cache.line[i].anchors) {
			int j = 0;

			for (j = 0; j < result->cache.line[i].fldcnt; j++)
				free(result->cache.line[i].anchors[j]);
			free(result->cache.line[i].anchors);
		}
		if (result->cache.line[i].lens)
			free(result->cache.line[i].lens);
		result->cache.line[i].anchors = result->cache.line[i + k].anchors;
		result->cache.line[i + k].anchors = 0;
		result->cache.line[i].lens = result->cache.line[i + k].lens;
		result->cache.line[i + k].lens = 0;
		result->cache.line[i].fldcnt = result->cache.line[i + k].fldcnt;
		if (result->cache.line[i].rows &&
		    (result->cache.line[i].rows[0] == '[' ||
		     result->cache.line[i].rows[0] == '=')) {
			result->cache.line[i].tuplerev = result->cache.tuplecount;
			result->cache.line[result->cache.tuplecount++].tupleindex = i;
		}
	}
	/* after the previous loop, i == result->cache.writer - k, and
	   the last (result->cache.writer - k) cache entries have been
	   cleared already , so we don't need to go the Full Monty
	   here */
	for ( /*i = result->cache.writer - k */ ; i < k /*result->cache.writer */ ; i++) {
		if (result->cache.line[i].rows) {
			if (result->cache.line[i].rows[0] == '[' ||
			    result->cache.line[i].rows[0] == '=')
				n++;
			free(result->cache.line[i].rows);
		}
		result->cache.line[i].rows = 0;
		if (result->cache.line[i].anchors) {
			int j = 0;

			for (j = 0; j < result->cache.line[i].fldcnt; j++)
				free(result->cache.line[i].anchors[j]);
			free(result->cache.line[i].anchors);
		}
		if (result->cache.line[i].lens)
			free(result->cache.line[i].lens);
		result->cache.line[i].anchors = 0;
		result->cache.line[i].lens = 0;
		result->cache.line[i].fldcnt = 0;
	}
	result->cache.reader -= k;
	if (result->cache.reader < 0)
		result->cache.reader = -1;
	result->cache.writer -= k;
	if (result->cache.writer < 0)	/* "cannot happen" */
		result->cache.writer = 0;
	result->cache.first += n;

	return MOK;
}

static void
mapi_extend_cache(struct MapiResultSet *result, int cacheall)
{
	int incr, newsize, oldsize = result->cache.limit, i;

	/* if there are read entries, delete them */
	if (result->cache.reader >= 0) {
		mapi_cache_freeup_internal(result, result->cache.reader + 1);
		/* since we've made space, we can return */
		return;
	}

	/* extend row cache */
  retry:;
	if (oldsize == 0)
		incr = 100;
	else
		incr = oldsize * 2;
	if (incr > 200000)
		incr = 20000;
	newsize = oldsize + incr;
	if (result->cache.rowlimit > 0 &&
	    newsize > result->cache.rowlimit &&
	    !cacheall) {
		newsize = result->cache.rowlimit;
		incr = newsize - oldsize;
		if (incr <= 0) {
			/* not enough space, so increase limit and try again */
			result->cache.rowlimit += 100;
			goto retry;
		}
	}

	REALLOC(result->cache.line, newsize + 1);
	assert(result->cache.line);
	for (i = oldsize; i <= newsize; i++) {
		result->cache.line[i].fldcnt = 0;
		result->cache.line[i].rows = NULL;
		result->cache.line[i].tupleindex = -1;
		result->cache.line[i].tuplerev = -1;
		result->cache.line[i].anchors = NULL;
		result->cache.line[i].lens = NULL;
	}
	result->cache.limit = newsize;
}

/* store a line in the cache */
static void
add_cache(struct MapiResultSet *result, char *line, int cacheall)
{
	/* manage the row cache space first */
	if (result->cache.writer >= result->cache.limit)
		mapi_extend_cache(result, cacheall);

	result->cache.line[result->cache.writer].rows = line;
	result->cache.line[result->cache.writer].tuplerev = result->cache.tuplecount;
	result->cache.line[result->cache.writer + 1].tuplerev = result->cache.tuplecount + 1;
	if (*line == '[' || *line == '=') {
		result->cache.line[result->cache.tuplecount++].tupleindex = result->cache.writer;
		if (result->row_count < result->cache.first + result->cache.tuplecount)
			result->row_count = result->cache.first + result->cache.tuplecount;
	}
	result->cache.writer++;
}

static struct MapiResultSet *
parse_header_line(MapiHdl hdl, char *line, struct MapiResultSet *result)
{
	char *tag, *etag;
	int i, n;
	char **anchors;
	size_t *lens;

	if (line[0] == '&') {
		char *nline = line;
		int qt;
		uint64_t queryid;

		/* handle fields &qt */

		nline++;	/* query type */
		qt = (int) strtol(nline, &nline, 0);

		if (result == NULL || (qt != Q_BLOCK && !result->commentonly))
			result = new_result(hdl);
		result->querytype = qt;
		result->commentonly = false;
		result->querytime = 0;
		result->maloptimizertime = 0;
		result->sqloptimizertime = 0;

		nline++;	/* skip space */
		switch (qt) {
		case Q_SCHEMA:
			result->querytime = strtoll(nline, &nline, 10);
			result->maloptimizertime = strtoll(nline, &nline, 10);
			result->sqloptimizertime = strtoll(nline, &nline, 10);
			break;
		case Q_TRANS:
			msetting_set_bool(hdl->mid->settings, MP_AUTOCOMMIT, *nline != 'f');
			break;
		case Q_UPDATE:
			result->row_count = strtoll(nline, &nline, 10);
			result->last_id = strtoll(nline, &nline, 10);
			queryid = strtoll(nline, &nline, 10);
			result->querytime = strtoll(nline, &nline, 10);
			result->maloptimizertime = strtoll(nline, &nline, 10);
			result->sqloptimizertime = strtoll(nline, &nline, 10);
			break;
		case Q_TABLE:
			if (sscanf(nline,
				   "%d %" SCNd64 " %d %" SCNd64 " %" SCNu64
				   " %" SCNd64 " %" SCNd64 " %" SCNd64,
				   &result->tableid, &result->row_count,
				   &result->fieldcnt, &result->tuple_count,
				   &queryid, &result->querytime,
				   &result->maloptimizertime,
				   &result->sqloptimizertime) < 8){
				result->querytime = 0;
				result->maloptimizertime = 0;
				result->sqloptimizertime = 0;
			}
			(void) queryid; /* ignored for now */
			break;
		case Q_PREPARE:
			sscanf(nline, "%d %" SCNd64 " %d %" SCNd64,
			       &result->tableid, &result->row_count,
			       &result->fieldcnt, &result->tuple_count);
			break;
		case Q_BLOCK:
			/* Mapi ignores the Q_BLOCK header, so spoof
			 * the querytype back to a Q_TABLE to let it
			 * go unnoticed */
			result->querytype = Q_TABLE;
			break;
		}


		if (result->fieldcnt > result->maxfields) {
			REALLOC(result->fields, result->fieldcnt);
			memset(result->fields + result->maxfields, 0, (result->fieldcnt - result->maxfields) * sizeof(*result->fields));
			result->maxfields = result->fieldcnt;
		}

		/* start of new SQL result */
		return result;
	}
	if (result == NULL)
		result = new_result(hdl);

	if (line[0] == '#' && !msettings_lang_is_mal(hdl->mid->settings)) {
		/* comment */
		return result;
	}

	line = strdup(line);	/* make copy we can play with */
	etag = strrchr(line, '#');
	if (etag == 0 || etag == line) {
		/* not a useful header line */
		free(line);
		return result;
	}

	n = slice_row(line, NULL, &anchors, &lens, 10, '#');

	result->commentonly = false;

	tag = etag + 1;
	while (*tag && isspace((unsigned char) *tag))
		tag++;

	if (n > result->fieldcnt) {
		result->fieldcnt = n;
		if (n > result->maxfields) {
			REALLOC(result->fields, n);
			memset(result->fields + result->maxfields, 0, (n - result->maxfields) * sizeof(*result->fields));
			result->maxfields = n;
		}
	}

	if (strcmp(tag, "name") == 0) {
		result->fieldcnt = n;
		for (i = 0; i < n; i++) {
			if (anchors[i]) {
				if (result->fields[i].columnname)
					free(result->fields[i].columnname);
				result->fields[i].columnname = anchors[i];
				anchors[i] = NULL;
			}
		}
	} else if (strcmp(tag, "type") == 0) {
		result->fieldcnt = n;
		for (i = 0; i < n; i++) {
			if (anchors[i]) {
				if (result->fields[i].columntype)
					free(result->fields[i].columntype);
				result->fields[i].columntype = anchors[i];
				anchors[i] = NULL;
			}
		}
	} else if (strcmp(tag, "length") == 0) {
		result->fieldcnt = n;
		for (i = 0; i < n; i++) {
			if (anchors[i])
				result->fields[i].columnlength = atoi(anchors[i]);
		}
	} else if (strcmp(tag, "table_name") == 0) {
		result->fieldcnt = n;
		for (i = 0; i < n; i++) {
			if (anchors[i]) {
				if (result->fields[i].tablename)
					free(result->fields[i].tablename);
				result->fields[i].tablename = anchors[i];
				anchors[i] = NULL;
			}
		}
	} else if (strcmp(tag, "typesizes") == 0) {
		result->fieldcnt = n;
		for (i = 0; i < n; i++) {
			if (anchors[i]) {
				char *p;
				result->fields[i].digits = atoi(anchors[i]);
				p = strchr(anchors[i], ' ');
				if (p)
					result->fields[i].scale = atoi(p + 1);
			}
		}
	}

	/* clean up */
	free(line);
	for (i = 0; i < n; i++)
		if (anchors[i])
			free(anchors[i]);
	free(anchors);
	free(lens);

	return result;
}

static void
write_file(MapiHdl hdl, char *filename, bool binary)
{
	Mapi mid = hdl->mid;
	char *line;
	char data[BLOCK];
	ssize_t len;

	(void) read_line(mid);	/* read flush marker */
	if (filename == NULL) {
		/* malloc failure */
		mnstr_printf(mid->to, "!HY001!allocation failure\n");
		mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
		return;
	}
	if (mid->putfilecontent == NULL) {
		free(filename);
		mnstr_printf(mid->to, "!HY000!cannot send files\n");
		mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
		return;
	}
	line = mid->putfilecontent(mid->filecontentprivate, filename, binary, NULL, 0);
	free(filename);
	if (line != NULL) {
		if (strchr(line, '\n'))
			line = "incorrect response from application";
		mnstr_printf(mid->to, "!HY000!%.64s\n", line);
		mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
		return;
	}
	mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
	for (;;) {
		len = mnstr_read(mid->from, data, 1, sizeof(data));
		if (len == -1) {
			if (mnstr_errnr(mid->from) == MNSTR_INTERRUPT) {
				mnstr_clearerr(mid->from);
				if (mid->oobintr && !hdl->aborted) {
					hdl->aborted = true;
					mnstr_putoob(mid->to, 1);
				}
			} else {
				break;
			}
		} else if (len == 0) {
			break;
		} else if (line == NULL) {
			line = mid->putfilecontent(mid->filecontentprivate,
						   NULL, binary, data, len);
		}
	}
	if (line == NULL)
		line = mid->putfilecontent(mid->filecontentprivate,
					   NULL, binary, NULL, 0);
	if (line && strchr(line, '\n'))
		line = "incorrect response from application";
	mnstr_printf(mid->to, "%s\n", line ? line : "");
	mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
}

#define MiB	(1 << 20)	/* a megabyte */

static void
read_file(MapiHdl hdl, uint64_t off, char *filename, bool binary)
{
	Mapi mid = hdl->mid;
	size_t size = 0, flushsize = 0;
	char *data, *line;

	(void) read_line(mid);	/* read flush marker */
	if (filename == NULL) {
		/* malloc failure */
		mnstr_printf(mid->to, "!HY001!allocation failure\n");
		mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
		return;
	}
	if (mid->getfilecontent == NULL) {
		free(filename);
		mnstr_printf(mid->to, "!HY000!cannot retrieve files\n");
		mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
		return;
	}
	data = mid->getfilecontent(mid->filecontentprivate, filename, binary,
				   off, &size);
	free(filename);
	if (data != NULL && size == 0) {
		if (strchr(data, '\n'))
			data = "incorrect response from application";
		mnstr_printf(mid->to, "!HY000!%.64s\n", data);
		mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
		return;
	}
	mnstr_printf(mid->to, "\n");
	while (data != NULL && size != 0) {
		if (flushsize >= MiB) {
			/* after every MiB give the server the
			 * opportunity to stop reading more data */
			mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
			/* at this point we expect to get a PROMPT2 if
			 * the server wants more data, or a PROMPT3 if
			 * the server had enough; anything else is a
			 * protocol violation */
			line = read_line(mid);
			if (line == NULL) {
				/* error */
				(void) mid->getfilecontent(mid->filecontentprivate, NULL, false, 0, NULL);
				return;
			}
			assert(line[0] == PROMPTBEG);
			if (line[0] != PROMPTBEG) {
				/* error in protocol */
				(void) mid->getfilecontent(mid->filecontentprivate, NULL, false, 0, NULL);
				return;
			}
			if (line[1] == PROMPT3[1]) {
				/* done reading: close file */
				(void) mid->getfilecontent(mid->filecontentprivate, NULL, false, 0, NULL);
				(void) read_line(mid);
				return;
			}
			assert(line[1] == PROMPT2[1]);
			if (line[1] != PROMPT2[1]) {
				/* error  in protocol */
				(void) mid->getfilecontent(mid->filecontentprivate, NULL, false, 0, NULL);
				return;
			}
			/* clear the flush marker */
			(void) read_line(mid);
			flushsize = 0;
		}
		if (size > MiB) {
			if (mnstr_write(mid->to, data, 1, MiB) != MiB) {
				mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
				return;
			}
			size -= MiB;
			data += MiB;
			flushsize += MiB;
		} else {
			if (mnstr_write(mid->to, data, 1, size) != (ssize_t) size) {
				mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
				return;
			}
			flushsize += size;
			data = mid->getfilecontent(mid->filecontentprivate, NULL, false, 0, &size);
		}
	}
	if (data != NULL && size == 0) {
		/* some error occurred */
		mnstr_clearerr(mid->from);
		if (mid->oobintr && !hdl->aborted) {
			hdl->aborted = true;
			mnstr_putoob(mid->to, 1);
		}
	}
	mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
	line = read_line(mid);
	if (line == NULL)
		return;
	assert(line[0] == PROMPTBEG);
	if (line[0] != PROMPTBEG)
		return;
	if (line[1] == PROMPT3[1]) {
		(void) read_line(mid);
		return;
	}
	assert(line[1] == PROMPT2[1]);
	if (line[1] != PROMPT2[1])
		return;
	(void) read_line(mid);
	mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
	line = read_line(mid);
	if (line == NULL)
		return;
	assert(line[0] == PROMPTBEG);
	assert(line[1] == PROMPT3[1]);
	(void) read_line(mid);
}


/* Read ahead and cache data read.  Depending on the second argument,
   reading may stop at the first non-header and non-error line, or at
   a prompt.
   This function is called either after a command has been sent to the
   server (in which case the second argument is 1), when the
   application asks for a result tuple that hadn't been cached yet (in
   which case the second argument is also 1), or whenever all pending
   data needs to be read in order to send a new command to the server
   (in which case the second argument is 0).
   Header lines result tuples are stored in the cache.  Certain header
   lines may cause a new result set to be created in which case all
   subsequent lines are added to that result set.
*/
MapiMsg
read_into_cache(MapiHdl hdl, int lookahead)
{
	char *line;
	Mapi mid;
	struct MapiResultSet *result;

	mid = hdl->mid;
	assert(mid->active == hdl);
	if (hdl->needmore) {
		hdl->needmore = false;
		mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
		check_stream(mid, mid->to, "write error on stream", mid->error);
	}
	if ((result = hdl->active) == NULL)
		result = hdl->result;	/* may also be NULL */

	for (;;) {
		line = read_line(mid);
		if (line == NULL) {
			if (mid->from && mnstr_eof(mid->from)) {
				return mapi_setError(mid, "unexpected end of file", __func__, MTIMEOUT);
			}

			return mid->error;
		}
		switch (*line) {
		case PROMPTBEG: /* \001 */
			mid->active = NULL;
			hdl->active = NULL;
			/* set needmore flag if line equals PROMPT2 up
			   to newline */
			if (line[1] == PROMPT2[1] && line[2] == '\0') {
				/* skip end of block */
				mid->active = hdl;
				(void) read_line(mid);
				hdl->needmore = true;
				mid->active = hdl;
			} else if (line[1] == PROMPT3[1] && line[2] == '\0') {
				mid->active = hdl;
				line = read_line(mid);
				bool binary = false;
				/* rb FILE
				 * r OFF FILE
				 * w FILE
				 * wb FILE
				 */
				switch (*line++) {
				case 'r': {
					uint64_t off = 0;
					if (*line == 'b') {
						line++;
						binary = true;
					} else {
						off = strtoul(line, &line, 10);
					}
					if (*line++ != ' ') {
						mnstr_printf(mid->to, "!HY000!unrecognized command from server\n");
						mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
						break;
					}
					read_file(hdl, off, strdup(line), binary);
					break;
				}
				case 'w':
					if (*line == 'b') {
						line++;
						binary = true;
					}
					if (*line++ != ' ') {
						mnstr_printf(mid->to, "!HY000!unrecognized command from server\n");
						mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
						break;
					}
					write_file(hdl, strdup(line), binary);
					break;
				}
				continue;
			}
			return mid->error;
		case '!':
			/* start a new result set if we don't have one
			   yet (duh!), or if we've already seen
			   normal output for the current one */
			if (result == NULL ||
			    result->cache.writer > 0 ||
			    result->querytype > 0) {
				result = new_result(hdl);
				result->commentonly = false;
				hdl->active = result;
			}
			add_error(result, line + 1 /* skip ! */ );
			if (!mid->error)
				mid->error = MSERVER;
			break;
		case '%':
		case '#':
		case '&':
			if (lookahead < 0)
				lookahead = 1;
			result = parse_header_line(hdl, line, result);
			hdl->active = result;
			if (result && *line != '&')
				add_cache(result, strdup(line), !lookahead);
			break;
		default:
			if (result == NULL) {
				result = new_result(hdl);
				hdl->active = result;
			}
			add_cache(result, strdup(line), !lookahead);
			if (lookahead > 0 &&
			    (result->querytype == -1 /* unknown (not SQL) */ ||
			     result->querytype == Q_TABLE ||
			     result->querytype == Q_UPDATE)) {
				return mid->error;
			}
			break;
		}
	}
}

static MapiMsg
mapi_execute_internal(MapiHdl hdl)
{
	size_t size;
	char *cmd;
	Mapi mid;

	mid = hdl->mid;
	if (mid->active && read_into_cache(mid->active, 0) != MOK)
		return MERROR;
	assert(mid->active == NULL);
	finish_handle(hdl);
	mapi_param_store(hdl);
	cmd = hdl->query;
	if (cmd == NULL)
		return MERROR;
	size = strlen(cmd);

	bool is_sql = msettings_lang_is_sql(mid->settings);
	char *prefix = is_sql ? "s" : "";
	char *suffix = is_sql ? "\n;" : "";
	mapi_log_record(mid, "SEND", "%s%s%s", prefix, cmd, suffix);

	if (is_sql) {
		/* indicate to server this is a SQL command */
		mnstr_write(mid->to, "s", 1, 1);
		check_stream(mid, mid->to, "write error on stream", mid->error);
	}
	mnstr_write(mid->to, cmd, 1, size);
	check_stream(mid, mid->to, "write error on stream", mid->error);
	/* all SQL statements should end with a semicolon */
	/* for the other languages it is assumed that the statements are correct */
	if (is_sql) {
		mnstr_write(mid->to, "\n;", 2, 1);
		check_stream(mid, mid->to, "write error on stream", mid->error);
	}
	mnstr_write(mid->to, "\n", 1, 1);
	check_stream(mid, mid->to, "write error on stream", mid->error);
	mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
	check_stream(mid, mid->to, "write error on stream", mid->error);
	mid->active = hdl;

	return MOK;
}

MapiMsg
mapi_execute(MapiHdl hdl)
{
	int ret;

	mapi_hdl_check(hdl);
	if ((ret = mapi_execute_internal(hdl)) == MOK)
		return read_into_cache(hdl, 1);

	return ret;
}

/*
 * The routine mapi_query is one of the most heavily used ones.
 * It sends a complete statement for execution
 * (i.e., ending in a newline; possibly including additional newlines).
 * Interaction with the server is sped up using block based interaction.
 * The query is retained in the Mapi structure to repeat shipping.
 */
MapiHdl
mapi_query(Mapi mid, const char *cmd)
{
	int ret;
	MapiHdl hdl;

	mapi_check0(mid);
	hdl = prepareQuery(mapi_new_handle(mid), cmd);
	ret = mid->error;
	if (ret == MOK)
		ret = mapi_execute_internal(hdl);
	if (ret == MOK)
		ret = read_into_cache(hdl, 1);
	return hdl;
}

/* version of mapi_query that does not wait for a response */
MapiHdl
mapi_send(Mapi mid, const char *cmd)
{
	int ret;
	MapiHdl hdl;

	mapi_check0(mid);
	hdl = prepareQuery(mapi_new_handle(mid), cmd);
	ret = mid->error;
	if (ret == MOK)
		ret = mapi_execute_internal(hdl);
	return hdl;
}

MapiMsg
mapi_read_response(MapiHdl hdl)
{
	return read_into_cache(hdl, 1);
}

MapiMsg
mapi_query_handle(MapiHdl hdl, const char *cmd)
{
	int ret;

	mapi_hdl_check(hdl);
	if (finish_handle(hdl) != MOK)
		return MERROR;
	prepareQuery(hdl, cmd);
	ret = hdl->mid->error;
	if (ret == MOK)
		ret = mapi_execute_internal(hdl);
	if (ret == MOK)
		ret = read_into_cache(hdl, 1);
	return ret;
}

MapiHdl
mapi_query_prep(Mapi mid)
{
	mapi_check0(mid);
	if (mid->active && read_into_cache(mid->active, 0) != MOK)
		return NULL;
	assert(mid->active == NULL);
	if (msettings_lang_is_sql(mid->settings)) {
		/* indicate to server this is a SQL command */
		mnstr_write(mid->to, "S", 1, 1);
		mapi_log_data(mid, "SEND", "S", 1);
	}
	return (mid->active = mapi_new_handle(mid));
}

MapiMsg
mapi_query_part(MapiHdl hdl, const char *query, size_t size)
{
	Mapi mid;

	mapi_hdl_check(hdl);
	mid = hdl->mid;
	assert(mid->active == NULL || mid->active == hdl);
	mid->active = hdl;
	/* remember the query just for the error messages */
	if (hdl->query == NULL) {
		hdl->query = malloc(size + 1);
		if (hdl->query) {
			strcpy_len(hdl->query, query, size + 1);
		}
	} else {
		size_t sz = strlen(hdl->query);
		char *q;

		if (sz < 512 &&
		    (q = realloc(hdl->query, sz + size + 1)) != NULL) {
			strcpy_len(q + sz, query, size + 1);
			hdl->query = q;
		}
	}

	if (mid->trace) {
		printf("mapi_query_part:%zu:%.*s\n", size, (int) size, query);
	}
	hdl->needmore = false;
	mnstr_write(mid->to, query, 1, size);
	if (mid->tracelog) {
		mnstr_write(mid->tracelog, query, 1, size);
		mnstr_flush(mid->tracelog, MNSTR_FLUSH_DATA);
	}
	check_stream(mid, mid->to, "write error on stream", mid->error);
	return mid->error;
}

MapiMsg
mapi_query_done(MapiHdl hdl)
{
	int ret;
	Mapi mid;

	mapi_hdl_check(hdl);
	mid = hdl->mid;
	assert(mid->active == NULL || mid->active == hdl);
	mid->active = hdl;
	hdl->needmore = false;
	mnstr_flush(mid->to, MNSTR_FLUSH_DATA);
	check_stream(mid, mid->to, "write error on stream", mid->error);
	ret = mid->error;
	if (ret == MOK)
		ret = read_into_cache(hdl, 1);
	return ret == MOK && hdl->needmore ? MMORE : ret;
}

MapiMsg
mapi_query_abort(MapiHdl hdl, int reason)
{
	Mapi mid;

	assert(reason > 0 && reason <= 127);
	mapi_hdl_check(hdl);
	mid = hdl->mid;
	assert(mid->active == NULL || mid->active == hdl);
	if (mid->oobintr && !hdl->aborted) {
		mnstr_putoob(mid->to, reason);
		hdl->aborted = true;
		return MOK;
	}
	return MERROR;
}

MapiMsg
mapi_cache_limit(Mapi mid, int limit)
{
	/* clean out superflous space TODO */
	msettings_error err = msetting_set_long(mid->settings, MP_REPLYSIZE, limit);
	if (err)
		return mapi_setError(mid, err, __func__, MERROR);
	if (!mid->connected)
		return MOK;
	mapi_check(mid);
/* 	if (hdl->cache.rowlimit < hdl->cache.limit) { */
	/* TODO: decide what to do here */
	/*              hdl->cache.limit = hdl->cache.rowlimit; *//* arbitrarily throw away cache lines */
/* 		if (hdl->cache.writer > hdl->cache.limit) { */
/* 			hdl->cache.writer = hdl->cache.limit; */
/* 			if (hdl->cache.reader > hdl->cache.writer) */
/* 				hdl->cache.reader = hdl->cache.writer; */
/* 		} */
/* 	} */
	if (msettings_lang_is_sql(mid->settings)) {
		MapiHdl hdl;

		if (mid->active)
			read_into_cache(mid->active, 0);

		mapi_log_record(mid, "X", "X" "reply_size %d\n", limit);
		if (mnstr_printf(mid->to, "X" "reply_size %d\n", limit) < 0 ||
		    mnstr_flush(mid->to, MNSTR_FLUSH_DATA)) {
			close_connection(mid);
			mapi_setError(mid, mnstr_peek_error(mid->to), __func__, MTIMEOUT);
			return MERROR;
		}
		hdl = prepareQuery(mapi_new_handle(mid), "reply_size");
		if (hdl == NULL)
			return MERROR;
		mid->active = hdl;
		read_into_cache(hdl, 0);
		mapi_close_handle(hdl);	/* reads away any output */
	}
	return MOK;
}

MapiMsg
mapi_fetch_reset(MapiHdl hdl)
{
	mapi_hdl_check(hdl);
	if (hdl->result)
		hdl->result->cache.reader = -1;
	return MOK;
}

MapiMsg
mapi_seek_row(MapiHdl hdl, int64_t rownr, int whence)
{
	struct MapiResultSet *result;

	mapi_hdl_check(hdl);
	result = hdl->result;
	switch (whence) {
	case MAPI_SEEK_SET:
		break;
	case MAPI_SEEK_CUR:
		rownr += result->cache.line[result->cache.reader + 1].tuplerev;
		break;
	case MAPI_SEEK_END:
		if (hdl->mid->active && read_into_cache(hdl->mid->active, 0) != MOK)
			return MERROR;
		rownr += result->row_count;
		break;
	default:
		return mapi_setError(hdl->mid, "Illegal whence value", __func__, MERROR);
	}
	if (rownr > result->row_count && hdl->mid->active && read_into_cache(hdl->mid->active, 0) != MOK)
		return MERROR;
	if (rownr < 0 || rownr > result->row_count)
		return mapi_setError(hdl->mid, "Illegal row number", __func__, MERROR);
	if (result->cache.first <= rownr && rownr < result->cache.first + result->cache.tuplecount) {
		/* we've got the requested tuple in the cache */
		result->cache.reader = result->cache.line[rownr - result->cache.first].tupleindex - 1;
	} else {
		/* we don't have the requested tuple in the cache
		   reset the cache and at the next fetch we'll get the data */
		if (mapi_cache_freeup(hdl, 100) == MOK) {
			result->cache.first = rownr;
		}
	}
	return hdl->mid->error;
}

/* Make space in the cache for new tuples, ignore the read pointer */
MapiMsg
mapi_cache_freeup(MapiHdl hdl, int percentage)
{
	struct MapiResultSet *result;
	int k;			/* # of cache lines to be deleted from front */

	mapi_hdl_check(hdl);
	result = hdl->result;
	if (result == NULL || (result->cache.writer == 0 && result->cache.reader == -1))
		return MOK;
	if (percentage < 0 || percentage > 100)
		percentage = 100;
	k = (result->cache.writer * percentage) / 100;
	if (k < 1)
		k = 1;
	return mapi_cache_freeup_internal(result, k);
}

static char *
mapi_fetch_line_internal(MapiHdl hdl)
{
	Mapi mid;
	struct MapiResultSet *result;
	char *reply;

	/* try to read a line from the cache */
	if ((result = hdl->result) == NULL || result->cache.writer <= 0 || result->cache.reader + 1 >= result->cache.writer) {
		mid = hdl->mid;
		if (mid->active != hdl || hdl->needmore)
			return NULL;

		if (read_into_cache(hdl, 1) != MOK)
			return NULL;
		if ((result = hdl->result) == NULL || result->cache.writer <= 0 || result->cache.reader + 1 >= result->cache.writer)
			return NULL;
	}
	reply = result->cache.line[++result->cache.reader].rows;
	if (hdl->bindings && (*reply == '[' || *reply == '=')) {
		mapi_slice_row(result, result->cache.reader);
		mapi_store_bind(result, result->cache.reader);
	}
	return reply;
}

/*
 * The routine mapi_fetch_line forms the basic interaction with the server.
 * It simply retrieves the next line and stores it in the row cache.
 * The field anchor structure is prepared for subsequent use by
 * mapi_fetch_row.
 * The content received is analyzed further by mapi_getRow()
 */
char *
mapi_fetch_line(MapiHdl hdl)
{
	char *reply;
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl);
	reply = mapi_fetch_line_internal(hdl);
	if (reply == NULL &&
	    (result = hdl->result) != NULL &&
	    msettings_lang_is_sql(hdl->mid->settings) &&
	    result->querytype == Q_TABLE &&
	    result->row_count > 0 &&
	    result->cache.first + result->cache.tuplecount < result->row_count) {
		if (hdl->needmore)	/* escalate */
			return NULL;
		if (hdl->mid->active != NULL)
			read_into_cache(hdl->mid->active, 0);
		hdl->mid->active = hdl;
		hdl->active = result;
		mapi_log_record(hdl->mid, "W", "X" "export %d %" PRId64 "\n",
				     result->tableid,
				     result->cache.first + result->cache.tuplecount);
		if (mnstr_printf(hdl->mid->to, "X" "export %d %" PRId64 "\n",
				 result->tableid,
				 result->cache.first + result->cache.tuplecount) < 0 ||
		    mnstr_flush(hdl->mid->to, MNSTR_FLUSH_DATA))
			check_stream(hdl->mid, hdl->mid->to, "sending export command", NULL);
		reply = mapi_fetch_line_internal(hdl);
	}
	return reply;
}

/*
 * To synchronize on a prompt, the low level routine mapi_finish can be used.
 * It discards all output received.
 */
MapiMsg
mapi_finish(MapiHdl hdl)
{
	mapi_hdl_check(hdl);
	return finish_handle(hdl);
}

/* msg is a string consisting comma-separated values.  The list of
   values is terminated by endchar or by the end-of-string NULL byte.
   Values can be quoted strings or unquoted values.  Upon return,
   *start points to the start of the first value which is stripped of
   leading and trailing white space, and if it was a quoted string,
   also of the quotes.  Also, backslash-escaped characters in the
   quoted string are replaced by the values the escapes represent.
   *next points to either the start of the next value (i.e. after the
   separating comma, possibly to the leading white space of the next
   value), or to the trailing ] or NULL byte if this was the last
   value.  *lenp is the number of bytes occupied by the (possibly
   converted) value, excluding final NULL byte.
   msg is *not* a const string: it is altered by this function.
   The function returns true if the string was quoted.
*/
static int
unquote(const char *msg, char **str, const char **next, int endchar, size_t *lenp)
{
	const char *p = msg;
	char quote;

	/* first skip over leading white space */
	while (*p && isspace((unsigned char) *p))
		p++;
	quote = *p;
	if (quote == '\'' || quote == '"') {
		size_t len = 0;
		char *s, *start;

		/* get quoted string and remove trailing bracket first */
		p++;
		/* first count how much space we need */
		msg = p;	/* save for later */
		while (*p && *p != quote) {
			if (*p == '\\') {
				p++;
				switch (*p) {
				case '0':
				case '1':
				case '2':
				case '3':
					/* this could be the start of
					   an octal sequence, check it
					   out */
					if (p[1] && p[2] &&
					    p[1] >= '0' && p[1] <= '7' &&
					    p[2] >= '0' && p[2] <= '7') {
						p += 2;
						break;
					}
					/* fall through */
				default:
					break;
				}
			}
			p++;
			len++;
		}
		/* now allocate space and copy string into new space */
		p = msg;	/* start over */
		start = s = malloc(len + 1);
		while (*p && *p != quote) {
			if (*p == '\\') {
				p++;
				switch (*p) {
				/* later
				   case '0': case '1': case '2': case '3': case '4':
				   case '5': case '6': case '7': case '8': case '9':
				*/
				case 'n':
					*s = '\n';
					break;
				case 't':
					*s = '\t';
					break;
				case 'r':
					*s = '\r';
					break;
				case 'f':
					*s = '\f';
					break;
				case '0':
				case '1':
				case '2':
				case '3':
					/* this could be the start of
					   an octal sequence, check it
					   out */
					if (p[1] && p[2] &&
					    p[1] >= '0' && p[1] <= '7' &&
					    p[2] >= '0' && p[2] <= '7') {
						*s = ((p[0] - '0') << 6) | ((p[1] - '0') << 3) | (p[2] - '0');
						p += 2;
						break;
					}
					/* fall through */
				default:
					*s = *p;
					break;
				}
				p++;
			} else {
				*s = *p++;
			}
			s++;
		}
		*s = 0;		/* close string */
		p++;		/* skip over end-of-string quote */
		/* skip over trailing junk (presumably white space) */
		while (*p && *p != ',' && *p != endchar)
			p++;
		if (next)
			*next = p;
		*str = start;
		if (lenp)
			*lenp = len;

		return 1;
	} else {
		const char *s;
		size_t len;

		/* p points at first non-white space character */
		msg = p;	/* record start of value */
		/* find separator or terminator */
		while (*p && *p != ',' && *p != '\t' && *p != endchar)
			p++;
		/* search back over trailing white space */
		for (s = p - 1; s > msg && isspace((unsigned char) *s); s--)
			;
		if (s < msg || !isspace((unsigned char) *s))	/* gone one too far */
			s++;
		if (*p == '\t') {
			p++;
		}
		len = s - msg;
		*str = malloc(len + 1);
		strcpy_len(*str, msg, len + 1);

		if (next)
			*next = p;
		if (lenp)
			*lenp = len;
		return 0;
	}
}

char *
mapi_unquote(char *msg)
{
	char *start;

	unquote(msg, &start, NULL, ']', NULL);
	return start;
}

char *
mapi_quote(const char *msg, int size)
{
	/* we absolutely don't need more than this (until we start
	   producing octal escapes */
	char *s = malloc((size < 0 ? strlen(msg) : (size_t) size) * 2 + 1);
	char *t = s;

	/* the condition is tricky: if initially size < 0, we must
	   continue until a NULL byte, else, size gives the number of
	   bytes to be copied */
	while (size < 0 ? *msg : size > 0) {
		if (size > 0)
			size--;
		switch (*msg) {
		case '\n':
			*t++ = '\\';
			*t++ = 'n';
			break;
		case '\t':
			*t++ = '\\';
			*t++ = 't';
			break;
		case PLACEHOLDER:
			*t++ = '\\';
			*t++ = PLACEHOLDER;
			break;
		case '\\':
			*t++ = '\\';
			*t++ = '\\';
			break;
		case '\'':
			*t++ = '\\';
			*t++ = '\'';
			break;
		case '"':
			*t++ = '\\';
			*t++ = '"';
			break;
		case '\0':
			*t++ = '\\';
			*t++ = '0';
			break;
		default:
			*t++ = *msg;
			break;
		}
		msg++;
		/* also deal with binaries */
	}
	*t = 0;
	return s;
}

static int
mapi_extend_bindings(MapiHdl hdl, int minbindings)
{
	/* extend the bindings table */
	int nm = hdl->maxbindings + 32;

	if (nm <= minbindings)
		nm = minbindings + 32;
	REALLOC(hdl->bindings, nm);
	assert(hdl->bindings);
	/* clear new entries */
	memset(hdl->bindings + hdl->maxbindings, 0, (nm - hdl->maxbindings) * sizeof(*hdl->bindings));
	hdl->maxbindings = nm;
	return MOK;
}

static int
mapi_extend_params(MapiHdl hdl, int minparams)
{
	/* extend the params table */
	int nm = hdl->maxparams + 32;

	if (nm <= minparams)
		nm = minparams + 32;
	REALLOC(hdl->params, nm);
	assert(hdl->params);
	/* clear new entries */
	memset(hdl->params + hdl->maxparams, 0, (nm - hdl->maxparams) * sizeof(*hdl->params));
	hdl->maxparams = nm;
	return MOK;
}

static MapiMsg
store_field(struct MapiResultSet *result, int cr, int fnr, int outtype, void *dst)
{
	char *val;

	val = result->cache.line[cr].anchors[fnr];

	if (val == 0) {
		return mapi_setError(result->hdl->mid, "Field value undefined or nil", __func__, MERROR);
	}

	/* auto convert to C-type */
	switch (outtype) {
	case MAPI_TINY:
		*(signed char *) dst = (signed char) strtol(val, NULL, 0);
		break;
	case MAPI_UTINY:
		*(unsigned char *) dst = (unsigned char) strtoul(val, NULL, 0);
		break;
	case MAPI_SHORT:
		*(short *) dst = (short) strtol(val, NULL, 0);
		break;
	case MAPI_USHORT:
		*(unsigned short *) dst = (unsigned short) strtoul(val, NULL, 0);
		break;
	case MAPI_NUMERIC:
	case MAPI_INT:
		*(int *) dst = (int) strtol(val, NULL, 0);
		break;
	case MAPI_UINT:
		*(unsigned int *) dst = (unsigned int) strtoul(val, NULL, 0);
		break;
	case MAPI_LONG:
		*(long *) dst = strtol(val, NULL, 0);
		break;
	case MAPI_ULONG:
		*(unsigned long *) dst = strtoul(val, NULL, 0);
		break;
	case MAPI_LONGLONG:
		*(int64_t *) dst = strtoll(val, NULL, 0);
		break;
	case MAPI_ULONGLONG:
		*(uint64_t *) dst = strtoull(val, NULL, 0);
		break;
	case MAPI_CHAR:
		*(char *) dst = *val;
		break;
	case MAPI_FLOAT:
		*(float *) dst = strtof(val, NULL);
		break;
	case MAPI_DOUBLE:
		*(double *) dst = strtod(val, NULL);
		break;
	case MAPI_DATE:
		sscanf(val, "%hd-%hu-%hu",
		       &((MapiDate *) dst)->year,
		       &((MapiDate *) dst)->month,
		       &((MapiDate *) dst)->day);
		break;
	case MAPI_TIME:
		sscanf(val, "%hu:%hu:%hu",
		       &((MapiTime *) dst)->hour,
		       &((MapiTime *) dst)->minute,
		       &((MapiTime *) dst)->second);
		break;
	case MAPI_DATETIME:{
		int n;

		((MapiDateTime *) dst)->fraction = 0;
		sscanf(val, "%hd-%hu-%hu %hu:%hu:%hu%n",
		       &((MapiDateTime *) dst)->year,
		       &((MapiDateTime *) dst)->month,
		       &((MapiDateTime *) dst)->day,
		       &((MapiDateTime *) dst)->hour,
		       &((MapiDateTime *) dst)->minute,
		       &((MapiDateTime *) dst)->second,
		       &n);
		if (val[n] == '.') {
			unsigned int fac = 1000000000;
			unsigned int nsec = 0;

			for (n++; isdigit((unsigned char) val[n]); n++) {
				fac /= 10;
				nsec += (val[n] - '0') * fac;
			}
			((MapiDateTime *) dst)->fraction = nsec;
		}
		break;
	}
	case MAPI_AUTO:
	case MAPI_VARCHAR:
	default:
		*(char **) dst = val;
	}
	return MOK;
}

MapiMsg
mapi_store_field(MapiHdl hdl, int fnr, int outtype, void *dst)
{
	struct MapiResultSet *result;

	mapi_hdl_check(hdl);

	if ((result = hdl->result) == NULL) {
		return mapi_setError(hdl->mid, "No data read", __func__, MERROR);
	}

	if (fnr < 0 || fnr >= result->fieldcnt) {
		return mapi_setError(hdl->mid, "Illegal field number", __func__, MERROR);
	}

	return store_field(result, result->cache.reader, fnr, outtype, dst);
}

static void
mapi_store_bind(struct MapiResultSet *result, int cr)
{
	int i;
	MapiHdl hdl = result->hdl;

	for (i = 0; i < hdl->maxbindings; i++)
		if (hdl->bindings[i].outparam)
			store_field(result, cr, i, hdl->bindings[i].outtype, hdl->bindings[i].outparam);
}

/*
 * The low level routine mapi_slice_row breaks the last row received
 * into pieces and binds the field descriptors with their location. All
 * escaped characters are immediately replaced, such that we end with a
 * list of C-strings.  It overwrites the contents of the row buffer,
 * because de-escaping only reduces the size.  It also silently extends
 * the field descriptor table.
 */
static int
mapi_slice_row(struct MapiResultSet *result, int cr)
{
	char *p;
	int i = 0;

	p = result->cache.line[cr].rows;
	if (p == NULL)
		return mapi_setError(result->hdl->mid, "Current row missing", __func__, MERROR);
	if (result->cache.line[cr].fldcnt)
		return result->cache.line[cr].fldcnt;	/* already sliced */

	if (*p != '[') {
		/* nothing to slice */
		i = 1;
		REALLOC(result->cache.line[cr].anchors, 1);
		REALLOC(result->cache.line[cr].lens, 1);
		/* skip initial '=' if present */
		if (*p == '=')
			p++;
		result->cache.line[cr].anchors[0] = strdup(p);
		result->cache.line[cr].lens[0] = strlen(p);
	} else {
		/* work on a copy to preserve the original */
		p = strdup(p);
		i = slice_row(p,
			      msettings_lang_is_sql(result->hdl->mid->settings) ? "NULL" : "nil",
			      &result->cache.line[cr].anchors,
			      &result->cache.line[cr].lens,
			      result->fieldcnt, ']');
		free(p);
	}
	if (i != result->fieldcnt) {
		int j;
		for (j = 0; j < result->fieldcnt; j++) {
			if (result->fields[j].columnname)
				free(result->fields[j].columnname);
			result->fields[j].columnname = NULL;
			if (result->fields[j].columntype)
				free(result->fields[j].columntype);
			result->fields[j].columntype = NULL;
			if (result->fields[j].tablename)
				free(result->fields[j].tablename);
			result->fields[j].tablename = NULL;
			result->fields[j].columnlength = 0;
		}
	}
	if (i > result->fieldcnt) {
		result->fieldcnt = i;
		if (i > result->maxfields) {
			REALLOC(result->fields, i);
			memset(result->fields + result->maxfields, 0, (i - result->maxfields) * sizeof(*result->fields));
			result->maxfields = i;
		}
	}
	result->cache.line[cr].fldcnt = i;
	return i;
}

/*
 * The rows presented are broken down into pieces to
 * simplify access later on. However, mclient may
 * first want to check the content of the line for
 * useful information (e.g. #EOD)
 */
int
mapi_split_line(MapiHdl hdl)
{
	int n;
	struct MapiResultSet *result;

	result = hdl->result;
	assert(result != NULL);
	if ((n = result->cache.line[result->cache.reader].fldcnt) == 0) {
		n = mapi_slice_row(result, result->cache.reader);
		/* no need to call mapi_store_bind since
		   mapi_fetch_line would have done that if needed */
	}
	return n;
}

int
mapi_fetch_row(MapiHdl hdl)
{
	char *reply;
	int n;
	struct MapiResultSet *result;

	mapi_hdl_check(hdl);
	do {
		if ((reply = mapi_fetch_line(hdl)) == NULL)
			return 0;
	} while (*reply != '[' && *reply != '=');
	result = hdl->result;
	assert(result != NULL);
	if ((n = result->cache.line[result->cache.reader].fldcnt) == 0) {
		n = mapi_slice_row(result, result->cache.reader);
		/* no need to call mapi_store_bind since
		   mapi_fetch_line would have done that if needed */
	}
	return n;
}

/*
 * All rows can be cached first as well.
 */
int64_t
mapi_fetch_all_rows(MapiHdl hdl)
{
	Mapi mid;
	struct MapiResultSet *result;

	mapi_hdl_check(hdl);

	mid = hdl->mid;
	for (;;) {
		if ((result = hdl->result) != NULL &&
		    msettings_lang_is_sql(mid->settings) &&
		    mid->active == NULL &&
		    result->row_count > 0 &&
		    result->cache.first + result->cache.tuplecount < result->row_count) {
			mid->active = hdl;
			hdl->active = result;
			mapi_log_record(mid, "SEND", "X" "export %d %" PRId64 "\n",
					     result->tableid, result->cache.first + result->cache.tuplecount);
			if (mnstr_printf(mid->to, "X" "export %d %" PRId64 "\n",
					 result->tableid, result->cache.first + result->cache.tuplecount) < 0 ||
			    mnstr_flush(mid->to, MNSTR_FLUSH_DATA))
				check_stream(mid, mid->to, "sending export command", 0);
		}
		if (mid->active)
			read_into_cache(mid->active, 0);
		else
			break;
	}
	return result ? result->cache.tuplecount : 0;
}

char *
mapi_fetch_field(MapiHdl hdl, int fnr)
{
	int cr;
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl);

	if ((result = hdl->result) == NULL ||
	    (cr = result->cache.reader) < 0 ||
	    (result->cache.line[cr].rows[0] != '[' &&
	     result->cache.line[cr].rows[0] != '=')) {
		mapi_setError(hdl->mid, "Must do a successful mapi_fetch_row first", __func__, MERROR);
		return 0;
	}
	assert(result->cache.line != NULL);
	if (fnr >= 0) {
		/* slice if needed */
		if (result->cache.line[cr].fldcnt == 0)
			mapi_slice_row(result, cr);
		if (fnr < result->cache.line[cr].fldcnt)
			return result->cache.line[cr].anchors[fnr];
	}
	mapi_setError(hdl->mid, "Illegal field number", __func__, MERROR);
	return 0;
}

size_t
mapi_fetch_field_len(MapiHdl hdl, int fnr)
{
	int cr;
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl);

	if ((result = hdl->result) == NULL ||
	    (cr = result->cache.reader) < 0 ||
	    (result->cache.line[cr].rows[0] != '[' &&
	     result->cache.line[cr].rows[0] != '=')) {
		mapi_setError(hdl->mid, "Must do a successful mapi_fetch_row first", __func__, MERROR);
		return 0;
	}
	assert(result->cache.line != NULL);
	if (fnr >= 0) {
		/* slice if needed */
		if (result->cache.line[cr].fldcnt == 0)
			mapi_slice_row(result, cr);
		if (fnr < result->cache.line[cr].fldcnt)
			return result->cache.line[cr].lens[fnr];
	}
	mapi_setError(hdl->mid, "Illegal field number", __func__, MERROR);
	return 0;
}

int
mapi_get_field_count(MapiHdl hdl)
{
	mapi_hdl_check(hdl);
	if (hdl->result && hdl->result->fieldcnt == 0) {
		/* no rows have been sliced yet, and there was no
		   header, so try to figure out how many columns there
		   are for ourselves */
		int i;

		for (i = 0; i < hdl->result->cache.writer; i++)
			if (hdl->result->cache.line[i].rows[0] == '[' ||
			    hdl->result->cache.line[i].rows[0] == '=')
				mapi_slice_row(hdl->result, i);
	}
	return hdl->result ? hdl->result->fieldcnt : 0;
}

int64_t
mapi_get_row_count(MapiHdl hdl)
{
	mapi_hdl_check(hdl);
	return hdl->result ? hdl->result->row_count : 0;
}

int64_t
mapi_get_last_id(MapiHdl hdl)
{
	mapi_hdl_check(hdl);
	return hdl->result ? hdl->result->last_id : -1;
}

char *
mapi_get_name(MapiHdl hdl, int fnr)
{
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl);
	if ((result = hdl->result) != 0 && fnr >= 0 && fnr < result->fieldcnt)
		return result->fields[fnr].columnname;
	mapi_setError(hdl->mid, "Illegal field number", __func__, MERROR);
	return 0;
}

char *
mapi_get_type(MapiHdl hdl, int fnr)
{
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl);
	if ((result = hdl->result) != 0 &&
	    fnr >= 0 && fnr < result->fieldcnt) {
		if (result->fields[fnr].columntype == NULL)
			return "unknown";
		return result->fields[fnr].columntype;
	}
	mapi_setError(hdl->mid, "Illegal field number", __func__, MERROR);
	return 0;
}

char *
mapi_get_table(MapiHdl hdl, int fnr)
{
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl);
	if ((result = hdl->result) != 0 && fnr >= 0 && fnr < result->fieldcnt)
		return result->fields[fnr].tablename;
	mapi_setError(hdl->mid, "Illegal field number", __func__, MERROR);
	return 0;
}

int
mapi_get_len(MapiHdl hdl, int fnr)
{
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl);
	if ((result = hdl->result) != 0 && fnr >= 0 && fnr < result->fieldcnt)
		return result->fields[fnr].columnlength;
	mapi_setError(hdl->mid, "Illegal field number", __func__, MERROR);
	return 0;
}

int
mapi_get_digits(MapiHdl hdl, int fnr)
{
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl);
	if ((result = hdl->result) != 0 && fnr >= 0 && fnr < result->fieldcnt)
		return result->fields[fnr].digits;
	mapi_setError(hdl->mid, "Illegal field number", __func__, MERROR);
	return 0;
}

int
mapi_get_scale(MapiHdl hdl, int fnr)
{
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl);
	if ((result = hdl->result) != 0 && fnr >= 0 && fnr < result->fieldcnt)
		return result->fields[fnr].scale;
	mapi_setError(hdl->mid, "Illegal field number", __func__, MERROR);
	return 0;
}

char *
mapi_get_query(MapiHdl hdl)
{
	mapi_hdl_check0(hdl);
	if (hdl->query != NULL) {
		return strdup(hdl->query);
	} else {
		return NULL;
	}
}


int
mapi_get_querytype(MapiHdl hdl)
{
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl);
	if ((result = hdl->result) != 0)
		return result->querytype;
	mapi_setError(hdl->mid, "No query result", __func__, MERROR);
	return 0; /* Q_PARSE! */
}

int
mapi_get_tableid(MapiHdl hdl)
{
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl);
	if ((result = hdl->result) != 0)
		return result->tableid;
	mapi_setError(hdl->mid, "No query result", __func__, MERROR);
	return 0;
}

int64_t
mapi_rows_affected(MapiHdl hdl)
{
	struct MapiResultSet *result;

	mapi_hdl_check(hdl);
	if ((result = hdl->result) == NULL)
		return 0;
	return result->row_count;
}

int64_t
mapi_get_querytime(MapiHdl hdl)
{
	struct MapiResultSet *result;

	mapi_hdl_check(hdl);
	if ((result = hdl->result) == NULL)
		return 0;
	return result->querytime;
}

int64_t
mapi_get_maloptimizertime(MapiHdl hdl)
{
	struct MapiResultSet *result;

	mapi_hdl_check(hdl);
	if ((result = hdl->result) == NULL)
		return 0;
	return result->maloptimizertime;
}

int64_t
mapi_get_sqloptimizertime(MapiHdl hdl)
{
	struct MapiResultSet *result;

	mapi_hdl_check(hdl);
	if ((result = hdl->result) == NULL)
		return 0;
	return result->sqloptimizertime;
}

const char *
mapi_get_dbname(Mapi mid)
{
	return msetting_string(mid->settings, MP_DATABASE);
}

const char *
mapi_get_host(Mapi mid)
{
	return msetting_string(mid->settings, MP_HOST);
}

const char *
mapi_get_user(Mapi mid)
{
	return msetting_string(mid->settings, MP_USER);;
}

const char *
mapi_get_lang(Mapi mid)
{
	return msetting_string(mid->settings, MP_LANGUAGE);;
}

const char *
mapi_get_uri(Mapi mid)
{
	return mid->uri;
}

const char *
mapi_get_mapi_version(void)
{
	return MAPI_VERSION;
}

const char *
mapi_get_monet_version(Mapi mid)
{
	mapi_check0(mid);
	return mid->server ? mid->server : "";
}

const char *
mapi_get_motd(Mapi mid)
{
	mapi_check0(mid);
	return mid->motd;
}

bool
mapi_is_connected(Mapi mid)
{
	return mid->connected;
}

MapiHdl
mapi_get_active(Mapi mid)
{
	return mid->active;
}

msettings*
mapi_get_settings(Mapi mid)
{
	return mid->settings;
}


MapiMsg
mapi_wrap_streams(Mapi mid, stream *rstream, stream *wstream)
{
	// do not use check_stream here yet because the socket is not yet in 'mid'
	const char *error_message;
	stream *error_stream;

	assert(!isa_block_stream(rstream));
	assert(!isa_block_stream(wstream));

	// First send some NUL bytes. If we're accidentally connecting to the wrong
	// port or protocol this may cause the remote server to close the
	// connection. If we don't do this, the connection will often hang
	// because the server expects us to speak first and we expect the server
	// to speak first.
	//
	// Note that a pair of NUL bytes is a no-op message in MAPI.
	//
	// Surprisingly, it seems sending these NUL bytes makes non-TLS
	// connection setup a little faster rather than slower!
	static const char zeroes[8] = { 0 };
	ssize_t to_write = sizeof(zeroes);
	while (to_write > 0) {
		ssize_t n = mnstr_write(wstream, zeroes, 1, to_write);
		if (n < 0) {
			close_connection(mid);
			return mapi_printError(mid, __func__, MERROR, "could not send leader block: %s", mnstr_peek_error(wstream));
		}
		to_write -= (size_t)n;
	}
	if (mnstr_flush(wstream, MNSTR_FLUSH_DATA) != 0) {
		close_connection(mid);
		return mapi_printError(mid, __func__, MERROR, "could not flush leader block: %s", mnstr_peek_error(wstream));
	}


	stream *brstream = NULL;
	stream *bwstream = NULL;

	bwstream = block_stream(wstream);
	if (bwstream == NULL || mnstr_errnr(bwstream) != MNSTR_NO__ERROR) {
		error_stream = bwstream;
		error_message = "block_stream wstream";
		goto bailout;
	}
	brstream = block_stream(rstream);
	if (brstream == NULL || mnstr_errnr(brstream) != MNSTR_NO__ERROR) {
		error_stream = brstream;
		error_message = "block_stream rstream";
		goto bailout;
	}

	mid->to = bwstream;
	mid->from = brstream;
	return MOK;
bailout:
	// adapted from the check_stream macro
	if (brstream)
		mnstr_destroy(brstream);
	if (bwstream)
		mnstr_destroy(bwstream);
	// malloc failure is the only way these calls could have failed
	return mapi_printError(mid, __func__, MERROR, "%s: %s", error_message, mnstr_peek_error(error_stream));
}
