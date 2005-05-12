/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 *
 * The Original Code is the Monet Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2005 CWI.
 * All Rights Reserved.
 */

/*
  +----------------------------------------------------------------------+
  | PHP Version 4                                                        |
  +----------------------------------------------------------------------+
  | Copyright (c) 1997-2003 The PHP Group                                |
  +----------------------------------------------------------------------+
  | This source file is subject to version 2.02 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available at through the world-wide-web at                           |
  | http://www.php.net/license/2_02.txt.                                 |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Authors: Marcin Zukowski <marcin@cwi.nl>,                            |
  |          Arjan Scherpenisse <A.C.Scherpenisse@cwi.nl>                |
  | partly derived from work of authors of MySQL PHP module and          |
  | Manfred Stienstra <manfred.stienstra@dwerg.net>                      |
  +----------------------------------------------------------------------+
*/

/* $Id$ */

#include "monetdb_config.h"

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"

#include "php_monetdb.h"

#define MONETDB_PHP_VERSION "0.02"

#include <Mapi.h>
#include <stdlib.h>
#include <string.h>

#define MONETDB_ASSOC	1<<0
#define MONETDB_NUM		1<<1
#define MONETDB_BOTH	(MONETDB_ASSOC|MONETDB_NUM)

ZEND_DECLARE_MODULE_GLOBALS(monetdb)

/* True global resources - no need for thread safety here */
static int le_link;
static int le_handle;

struct _phpMonetHandle {
	int resno;
	struct _phpMonetHandle *next;
};
typedef struct _phpMonetHandle phpMonetHandle;


struct _phpMonetConn {
	Mapi mid;
	phpMonetHandle *first;
};
typedef struct _phpMonetConn phpMonetConn;

/* TODO: maybe we want to introduce persistant connections?
static int le_plink; */

/* {{{ monetdb_functions[]
 *
 * Every user visible function must have an entry in monet_functions[].
 */
function_entry monetdb_functions[] = {
	PHP_FE(monetdb_connect, NULL)
	PHP_FE(monetdb_close, NULL)
	PHP_FE(monetdb_query, NULL)
	PHP_FE(monetdb_num_rows, NULL)
	PHP_FE(monetdb_num_fields, NULL)
	PHP_FE(monetdb_next_result, NULL)
	PHP_FE(monetdb_field_table, NULL)
	PHP_FE(monetdb_field_name, NULL)
	PHP_FE(monetdb_field_type, NULL)
	PHP_FE(monetdb_field_len, NULL)
	PHP_FE(monetdb_errno, NULL)
	PHP_FE(monetdb_error, NULL)
	PHP_FE(monetdb_fetch_array, NULL)
	PHP_FE(monetdb_fetch_assoc, NULL)
	PHP_FE(monetdb_fetch_object, NULL)
	PHP_FE(monetdb_fetch_row, NULL)
	PHP_FE(monetdb_free_result, NULL)
	PHP_FE(monetdb_data_seek, NULL)
	PHP_FE(monetdb_escape_string, NULL)
	PHP_FE(monetdb_affected_rows, NULL)
	PHP_FE(monetdb_ping, NULL)
	PHP_FE(monetdb_info, NULL)
	{NULL, NULL, NULL}	/* Must be the last line in monetdb_functions[] */
};

/* }}} */

/* {{{ monetdb_module_entry
 */
zend_module_entry monetdb_module_entry = {
#if ZEND_MODULE_API_NO >= 20010901
	STANDARD_MODULE_HEADER,
#endif
	"monetdb",
	monetdb_functions,
	PHP_MINIT(monetdb),
	PHP_MSHUTDOWN(monetdb),
	PHP_RINIT(monetdb),	/* Replace with NULL if there's nothing to do at request start */
	PHP_RSHUTDOWN(monetdb),	/* Replace with NULL if there's nothing to do at request end */
	PHP_MINFO(monetdb),
#if ZEND_MODULE_API_NO >= 20010901
	MONETDB_PHP_VERSION,	/* Replace with version number for your extension */
#endif
	STANDARD_MODULE_PROPERTIES
};

/* }}} */

#ifdef COMPILE_DL_MONETDB
ZEND_GET_MODULE(monetdb)
#endif
/* {{{ _free_monetdb_link
 */
static void
_free_monetdb_link(zend_rsrc_list_entry * rsrc TSRMLS_DC)
{
	phpMonetConn *monet_link = (phpMonetConn *) rsrc->ptr;
	phpMonetHandle *h = monet_link->first, *next;
	int i = 0;

	while (h) {
		i++;
		fflush(stderr);
		next = h->next;
		free(h);
		h = next;
	}
	/* fprintf(stderr, "Freed link and %d handles\n", i); */
	mapi_destroy(monet_link->mid);
	monet_link->mid = NULL;
	free(monet_link);
	monet_link = NULL;
}

/* }}} */

/* {{{ _free_monetdb_handle
 */
static void
_free_monetdb_handle(zend_rsrc_list_entry * rsrc TSRMLS_DC)
{
	MapiHdl monet_handle = (MapiHdl) rsrc->ptr;

	mapi_close_handle(monet_handle);
}

/* }}} */

/* {{{ PHP_INI
 */
PHP_INI_BEGIN()
    STD_PHP_INI_ENTRY("monetdb.default_port", "50000", PHP_INI_ALL, OnUpdateInt, default_port, zend_monetdb_globals, monetdb_globals)
    STD_PHP_INI_ENTRY("monetdb.default_language", "mil", PHP_INI_ALL, OnUpdateString, default_language, zend_monetdb_globals, monetdb_globals)
    STD_PHP_INI_ENTRY("monetdb.default_hostname", "localhost", PHP_INI_ALL, OnUpdateString, default_hostname, zend_monetdb_globals, monetdb_globals)
    STD_PHP_INI_ENTRY("monetdb.default_username", "monetdb", PHP_INI_ALL, OnUpdateString, default_username, zend_monetdb_globals, monetdb_globals)
    STD_PHP_INI_ENTRY("monetdb.default_password", "monetdb", PHP_INI_ALL, OnUpdateString, default_password, zend_monetdb_globals, monetdb_globals)
    STD_PHP_INI_ENTRY("monetdb.query_timeout", "0", PHP_INI_ALL, OnUpdateInt, query_timeout, zend_monetdb_globals, monetdb_globals)
    PHP_INI_END()
/* }}} */
/* {{{ php_monetdb_init_globals
 */
static void
php_monetdb_init_globals(zend_monetdb_globals * monetdb_globals)
{
	monetdb_globals->default_port = 0;
	monetdb_globals->default_language = NULL;
	monetdb_globals->default_hostname = NULL;
	monetdb_globals->default_username = NULL;
	monetdb_globals->default_password = NULL;
	monetdb_globals->query_timeout = 0;
}

/* }}} */

/* {{{ PHP_MINIT_FUNCTION
 */
PHP_MINIT_FUNCTION(monetdb)
{
	ZEND_INIT_MODULE_GLOBALS(monetdb, php_monetdb_init_globals, NULL);
	REGISTER_INI_ENTRIES();

	le_handle = zend_register_list_destructors_ex(_free_monetdb_handle, NULL, "MonetDB result handle", module_number);
	le_link = zend_register_list_destructors_ex(_free_monetdb_link, NULL, "MonetDB connection", module_number);

	REGISTER_LONG_CONSTANT("MONETDB_ASSOC", MONETDB_ASSOC, CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("MONETDB_NUM", MONETDB_NUM, CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("MONETDB_BOTH", MONETDB_BOTH, CONST_CS | CONST_PERSISTENT);

	return SUCCESS;
}

/* }}} */

/* {{{ PHP_MSHUTDOWN_FUNCTION
 */
PHP_MSHUTDOWN_FUNCTION(monetdb)
{
	UNREGISTER_INI_ENTRIES();
	return SUCCESS;
}

/* }}} */

/* Remove if there's nothing to do at request start */
/* {{{ PHP_RINIT_FUNCTION
 */
PHP_RINIT_FUNCTION(monetdb)
{
	return SUCCESS;
}

/* }}} */

/* Remove if there's nothing to do at request end */
/* {{{ PHP_RSHUTDOWN_FUNCTION
 */
PHP_RSHUTDOWN_FUNCTION(monetdb)
{
	return SUCCESS;
}

/* }}} */

/* {{{ PHP_MINFO_FUNCTION
 */
PHP_MINFO_FUNCTION(monetdb)
{
	php_info_print_table_start();
	php_info_print_table_header(2, "MonetDB support", "enabled");
	php_info_print_table_row(2, "MonetDB PHP module version", MONETDB_PHP_VERSION);
	/* TODO: more information, mapi version etc. */
	php_info_print_table_end();

	DISPLAY_INI_ENTRIES();
}

/* }}} */

/* {{{ php_monetdb_set_default_link
 */
static void
php_monetdb_set_default_link(int id TSRMLS_DC)
{
	zend_list_addref(id);

	/*
	   if (MONET_G(default_link) != -1) {
	   zend_list_delete(MONET_G(default_link));
	   } */

	MONET_G(default_link) = id;
}

/* }}} */

#define MONETDB_CONNECT_CLEANUP()

#define MONETDB_CONNECT_RETURN_FALSE()		\
	MONETDB_CONNECT_CLEANUP();				\
	RETURN_FALSE;

/* {{{ proto resource monetdb_connect(string language [, string hostname [, int port [, string username [, string password]]]])
   Open a connection to a MonetDB server */
PHP_FUNCTION(monetdb_connect)
{
	phpMonetConn *conn;
	char *hostname = NULL;
	char *username = NULL;
	char *password = NULL;
	char *language = NULL;
	int port = 50000;
	int timeout = 0;
	zval **z_hostname = NULL, **z_username = NULL, **z_password = NULL, **z_language = NULL, **z_port = NULL;

	hostname = MONET_G(default_hostname);
	username = MONET_G(default_username);
	password = MONET_G(default_password);
	language = MONET_G(default_language);
	port = MONET_G(default_port);
	timeout = MONET_G(query_timeout);

	/* Parse parameters */
	switch (ZEND_NUM_ARGS()) {
	case 0:		/* defaults */
		break;
	case 1:		/* language */
		if (zend_get_parameters_ex(1, &z_language) == FAILURE) {
			MONETDB_CONNECT_RETURN_FALSE();
		}
		break;
	case 2:		/* language, hostname */
		if (zend_get_parameters_ex(2, &z_language, z_hostname) == FAILURE) {
			MONETDB_CONNECT_RETURN_FALSE();
		}
		break;
	case 3:		/* language, hostname, port */
		if (zend_get_parameters_ex(3, &z_language, &z_hostname, &z_port) == FAILURE) {
			MONETDB_CONNECT_RETURN_FALSE();
		}
		break;
	case 4:		/* language, hostname, port, username */
		if (zend_get_parameters_ex(3, &z_language, &z_hostname, &z_port, &z_username) == FAILURE) {
			MONETDB_CONNECT_RETURN_FALSE();
		}
		break;
	case 5:		/* language, hostname, port, password */
		if (zend_get_parameters_ex(3, &z_language, &z_hostname, &z_port, &z_username, &z_password) == FAILURE) {
			MONETDB_CONNECT_RETURN_FALSE();
		}
		break;
	default:
		WRONG_PARAM_COUNT;
		break;
	}

	/* Take values of parsed parameters */
	if (z_language) {
		convert_to_string_ex(z_language);
		language = Z_STRVAL_PP(z_language);
	}
	if (z_hostname) {
		convert_to_string_ex(z_hostname);
		hostname = Z_STRVAL_PP(z_hostname);
	}
	if (z_port) {
		convert_to_long_ex(z_port);
		port = Z_LVAL_PP(z_port);
	}
	if (z_username) {
		convert_to_string_ex(z_username);
		username = Z_STRVAL_PP(z_username);
	}
	if (z_password) {
		convert_to_string_ex(z_password);
		password = Z_STRVAL_PP(z_password);
	}
	/* Provide the default SQL port in case it isnt given */
	if (!z_port && z_language && !strcasecmp(language, "sql"))
		port = 45123;

	/* ~printf("MON: Connecting to: hostname=%s port=%d username=%s password=%s language=%s\n",
	   ~ hostname, port, username, password, language); */

	/* Connect to MonetDB server */
	conn = (phpMonetConn *) malloc(sizeof(phpMonetConn));
	conn->first = NULL;
	conn->mid = mapi_connect(hostname, port, username, password, language);

	if (mapi_error(conn->mid) || mapi_error_str(conn->mid)) {
		/*~ printf("MON: failed\n"); */
		MONETDB_CONNECT_RETURN_FALSE();
	}
#if 0
	/* mapi_timeout is not yet implemented :-/ */

	/* Set the query timeout, if any. */
	if (timeout > 0)
		/* Timeout in .ini file is given in seconds, so convert to msecs */
		mapi_timeout(conn, 1000 * timeout);
#endif

	/*~ printf("MON: succeeded, conn=%p\n", conn); */
	/* return value is the return value in the PHP_FUNCTION */
	ZEND_REGISTER_RESOURCE(return_value, conn, le_link);
	php_monetdb_set_default_link(Z_LVAL_P(return_value) TSRMLS_CC);

	MONETDB_CONNECT_CLEANUP();
}

/* }}} */

/* {{{ proto bool monetdb_close([resource db])
   close the connection to a MonetDB server */
PHP_FUNCTION(monetdb_close)
{
	zval **mapi_link = NULL;
	int id;
	phpMonetConn *conn;

	switch (ZEND_NUM_ARGS()) {
	case 0:
		id = MONET_G(default_link);

		break;
	case 1:
		if (zend_get_parameters_ex(1, &mapi_link) == FAILURE) {
			RETURN_FALSE;
		}
		id = -1;

		break;
	default:
		WRONG_PARAM_COUNT;
		break;
	}

	ZEND_FETCH_RESOURCE(conn, phpMonetConn *, mapi_link, id, "MonetDB connection", le_link);

	/*~ printf("MON: disconnecting %p\n", conn); */
	mapi_disconnect(conn->mid);

	if (mapi_error(conn->mid) && mapi_error_str(conn->mid)) ;
	RETURN_FALSE;

	if (id == -1) {		/* explicit resource number */
		zend_list_delete(Z_RESVAL_PP(mapi_link));
	}

	if (id !=-1 || (mapi_link && Z_RESVAL_PP(mapi_link) == MONET_G(default_link))) {
		zend_list_delete(MONET_G(default_link));
		MONET_G(default_link) = -1;
	}
	RETURN_TRUE;
}

/* }}} */


/* {{{ proto resource monetdb_query(string query[, resource db])
   run a query on the MonetDB server */
PHP_FUNCTION(monetdb_query)
{
	zval **z_query = NULL, **z_mapi_link = NULL;
	int id;
	phpMonetConn *conn;
	phpMonetHandle *h;
	MapiHdl handle;
	char *query = NULL;
	char *error = NULL;

	switch (ZEND_NUM_ARGS()) {
	case 1:
		if (zend_get_parameters_ex(1, &z_query) == FAILURE) {
			RETURN_FALSE;
		}
		id = MONET_G(default_link);

		break;
	case 2:
		if (zend_get_parameters_ex(2, &z_query, &z_mapi_link) == FAILURE) {
			RETURN_FALSE;
		}
		id = -1;

		break;
	default:
		WRONG_PARAM_COUNT;
		break;
	}

	ZEND_FETCH_RESOURCE(conn, phpMonetConn *, z_mapi_link, id, "MonetDB connection", le_link);

	convert_to_string_ex(z_query);
	query = Z_STRVAL_PP(z_query);

	if (!conn || !conn->mid) {
		php_error(E_WARNING, "monetdb_query: Query on uninitialized/closed connection");
		/* php_error_docref("function.monetdb_query" TSRMLS_CC, E_WARNING, "Query on uninitialized/closed connection"); */
		RETURN_FALSE;
	}

	handle = mapi_new_handle(conn->mid);
	if (!handle) {
		php_error(E_WARNING, "monetdb_query: Query on uninitialized/closed connection");
		/* php_error_docref("function.monetdb_query" TSRMLS_CC, E_WARNING, "Query on uninitialized/closed connection"); */
		RETURN_FALSE;
	}

	mapi_query_handle(handle, query);

	if ((error = mapi_result_error(handle)) != NULL) {
		/* mapi_close_handle(handle); */
		php_error(E_WARNING, "monetdb_query: Error: %s", error);
		/* php_error_docref("function.monetdb_query" TSRMLS_CC, E_WARNING, "MonetDB Error: %s", error); */
		RETURN_FALSE;
	}

	if (mapi_get_querytype(handle) != 7)
		mapi_fetch_all_rows(handle);

	/* We need to cache all rows directly, otherwise things get confusing. This is a mapi bug/feature. */
	/* mapi_fetch_all_rows(handle); */

	/*~ printf("MON: successfull, handle=%p\n", handle); */
	/*~ printf("MON: -- error=%d, \n%s \n", mapi_error(conn), mapi_error_str(conn)); */

	h = (phpMonetHandle *) malloc(sizeof(phpMonetHandle));
	h->resno = ZEND_REGISTER_RESOURCE(return_value, handle, le_handle);

	/* add resource id to linked list for automatic cleanup on disconnect(). */
	if (conn->first)
		h->next = conn->first;
	else
		h->next = NULL;
	conn->first = h;
}

/* }}} */


/* {{{ proto long monetdb_num_rows(resource handle)
   return number of rows (tuples) in a query result */
PHP_FUNCTION(monetdb_num_rows)
{
	zval **z_handle = NULL;
	MapiHdl handle;

	if ((ZEND_NUM_ARGS() != 1) || (zend_get_parameters_ex(1, &z_handle) == FAILURE)) {
		WRONG_PARAM_COUNT;
	}

	if (Z_TYPE_PP(z_handle) == IS_RESOURCE && Z_LVAL_PP(z_handle) == 0)
		RETURN_FALSE;

	ZEND_FETCH_RESOURCE(handle, MapiHdl, z_handle, -1, "MonetDB result handle", le_handle);

	/*~ printf("MON: num rows handle=%p\n", handle); */

	Z_LVAL_P(return_value) = (long) mapi_get_row_count(handle);
	Z_TYPE_P(return_value) = IS_LONG;
}

/* }}} */

/* {{{ proto long monetdb_num_fields(resource handle)
   return number of fields (columns) in a query result */
PHP_FUNCTION(monetdb_num_fields)
{
	zval **z_handle = NULL;
	MapiHdl handle;

	if ((ZEND_NUM_ARGS() != 1) || (zend_get_parameters_ex(1, &z_handle) == FAILURE)) {
		WRONG_PARAM_COUNT;
	}

	if (Z_TYPE_PP(z_handle) == IS_RESOURCE && Z_LVAL_PP(z_handle) == 0)
		RETURN_FALSE;

	ZEND_FETCH_RESOURCE(handle, MapiHdl, z_handle, -1, "MonetDB result handle", le_handle);

	/*~ printf("MON: num fields handle=%p\n", handle); */

	Z_LVAL_P(return_value) = (long) mapi_get_field_count(handle);
	Z_TYPE_P(return_value) = IS_LONG;
}

/* }}} */

/* {{{ proto bool monetdb_num_fields(resource handle)
   move the result set pointer to the next result set. Returns TRUE if
   there is a next result set, otherwise FALSE. */
PHP_FUNCTION(monetdb_next_result)
{
	zval **z_handle = NULL;
	MapiHdl handle;
	int result = -1;
	char *error = NULL;

	if ((ZEND_NUM_ARGS() != 1) || (zend_get_parameters_ex(1, &z_handle) == FAILURE)) {
		WRONG_PARAM_COUNT;
	}

	if (Z_TYPE_PP(z_handle) == IS_RESOURCE && Z_LVAL_PP(z_handle) == 0)
		RETURN_FALSE;

	ZEND_FETCH_RESOURCE(handle, MapiHdl, z_handle, -1, "MonetDB result handle", le_handle);

	result = (int) mapi_next_result(handle);

	if ((error = mapi_result_error(handle)) != NULL) {
		/* mapi_close_handle(handle); */
		php_error(E_WARNING, "monetdb_query: Error: %s", error);
		/* php_error_docref("function.monetdb_query" TSRMLS_CC, E_WARNING, "MonetDB Error: %s", error); */
		RETURN_FALSE;
	}

	Z_LVAL_P(return_value) = result;
	Z_TYPE_P(return_value) = IS_LONG;
}

/* }}} */

/* {{{ proto string monetdb_field_table(resource handle, int index)
   return the table name of a specified field (column) */
PHP_FUNCTION(monetdb_field_table)
{
	zval **z_handle = NULL, **z_index = NULL;
	MapiHdl handle;
	long index;
	char *table;

	if ((ZEND_NUM_ARGS() != 2) || (zend_get_parameters_ex(2, &z_handle, &z_index) == FAILURE)) {
		WRONG_PARAM_COUNT;
	}

	if (Z_TYPE_PP(z_handle) == IS_RESOURCE && Z_LVAL_PP(z_handle) == 0)
		RETURN_FALSE;

	ZEND_FETCH_RESOURCE(handle, MapiHdl, z_handle, -1, "MonetDB result handle", le_handle);

	convert_to_long_ex(z_index);
	index = Z_LVAL_PP(z_index);

	/*~ printf("MON: _field_table: index=%d handle=%p\n", index, handle); */

	if (index >= mapi_get_field_count(handle)) {
		php_error(E_WARNING, "monetdb_field_table: Accessing field number #%ld, which is out of range", index);
		/* php_error_docref("function.monetdb_field_table" TSRMLS_CC, E_WARNING, "Accessing field number #%ld, which is out of range", index); */
		RETURN_FALSE;
	}

	table = mapi_get_table(handle, index);

	/*~ printf("MON: _field_table: table=%s\n", table); */

	Z_STRLEN_P(return_value) = strlen(table);
	Z_STRVAL_P(return_value) = estrndup(table, Z_STRLEN_P(return_value));
	Z_TYPE_P(return_value) = IS_STRING;
}

/* }}} */

/* {{{ proto string monetdb_field_name(resource handle, int index)
   return a name of a specified field (column) */
PHP_FUNCTION(monetdb_field_name)
{
	zval **z_handle = NULL, **z_index = NULL;
	MapiHdl handle;
	long index;
	char *name;

	if ((ZEND_NUM_ARGS() != 2) || (zend_get_parameters_ex(2, &z_handle, &z_index) == FAILURE)) {
		WRONG_PARAM_COUNT;
	}

	if (Z_TYPE_PP(z_handle) == IS_RESOURCE && Z_LVAL_PP(z_handle) == 0)
		RETURN_FALSE;

	ZEND_FETCH_RESOURCE(handle, MapiHdl, z_handle, -1, "MonetDB result handle", le_handle);

	convert_to_long_ex(z_index);
	index = Z_LVAL_PP(z_index);

	/*~ printf("MON: _field_name: index=%d handle=%p\n", index, handle); */

	if (index >= mapi_get_field_count(handle)) {
		php_error(E_WARNING, "monetdb_field_name: Accessing field number #%ld, which is out of range", index);
		/* php_error_docref("function.monetdb_field_name" TSRMLS_CC, E_WARNING, "Accessing field number #%ld, which is out of range", index); */
		RETURN_FALSE;
	}

	name = mapi_get_name(handle, index);

	/*~ printf("MON: _field_name: name=%s\n", name); */

	Z_STRLEN_P(return_value) = strlen(name);
	Z_STRVAL_P(return_value) = estrndup(name, Z_STRLEN_P(return_value));
	Z_TYPE_P(return_value) = IS_STRING;
}

/* }}} */

/* {{{ proto string monetdb_field_type(resource handle, int index)
   return a type (as string) of a specified field (column) */
PHP_FUNCTION(monetdb_field_type)
{
	zval **z_handle = NULL, **z_index = NULL;
	MapiHdl handle;
	long index;
	char *type;

	if ((ZEND_NUM_ARGS() != 2) || (zend_get_parameters_ex(2, &z_handle, &z_index) == FAILURE)) {
		WRONG_PARAM_COUNT;
	}

	if (Z_TYPE_PP(z_handle) == IS_RESOURCE && Z_LVAL_PP(z_handle) == 0)
		RETURN_FALSE;

	ZEND_FETCH_RESOURCE(handle, MapiHdl, z_handle, -1, "MonetDB result handle", le_handle);

	convert_to_long_ex(z_index);
	index = Z_LVAL_PP(z_index);

	/*~ printf("MON: _field_type: index=%d handle=%p\n", index, handle); */

	if (index >= mapi_get_field_count(handle)) {
		php_error(E_WARNING, "monetdb_field_type: Accessing field number #%ld, which is out of range", index);
		/* php_error_docref("function.monetdb_field_type" TSRMLS_CC, E_WARNING, "Accessing field number #%ld, which is out of range", index); */
		RETURN_FALSE;
	}

	type = mapi_get_type(handle, index);

	/*~ printf("MON: _field_type: type=%s\n", type); */

	Z_STRLEN_P(return_value) = strlen(type);
	Z_STRVAL_P(return_value) = estrndup(type, Z_STRLEN_P(return_value));
	Z_TYPE_P(return_value) = IS_STRING;
}

/* }}} */

/* {{{ proto string monetdb_field_len(resource handle, int index)
   return the lenght (as long) of a specified field (column) */
PHP_FUNCTION(monetdb_field_len)
{
	zval **z_handle = NULL, **z_index = NULL;
	MapiHdl handle;
	long index;
	long len;

	if ((ZEND_NUM_ARGS() != 2) || (zend_get_parameters_ex(2, &z_handle, &z_index) == FAILURE)) {
		WRONG_PARAM_COUNT;
	}

	if (Z_TYPE_PP(z_handle) == IS_RESOURCE && Z_LVAL_PP(z_handle) == 0)
		RETURN_FALSE;

	ZEND_FETCH_RESOURCE(handle, MapiHdl, z_handle, -1, "MonetDB result handle", le_handle);

	convert_to_long_ex(z_index);
	index = Z_LVAL_PP(z_index);

	/*~ printf("MON: _field_len: index=%d handle=%p\n", index, handle); */

	if (index >= mapi_get_field_count(handle)) {
		php_error(E_WARNING, "monetdb_field_len: Accessing field number #%ld, which is out of range", index);
		/* php_error_docref("function.monetdb_field_len" TSRMLS_CC, E_WARNING, "Accessing field number #%ld, which is out of range", index); */
		RETURN_FALSE;
	}

	len = mapi_get_len(handle, index);

	/*~ printf("MON: _field_len: len=%d\n", len); */

	RETURN_LONG(len);
}

/* }}} */

/* {{{ proto int monetdb_errno([resource db])
   Returns the numerical value of the error message from previous MonetDB operation */
PHP_FUNCTION(monetdb_errno)
{
	zval **mapi_link = NULL;
	int id;
	phpMonetConn *conn;

	switch (ZEND_NUM_ARGS()) {
	case 0:
		id = MONET_G(default_link);

		break;
	case 1:
		if (zend_get_parameters_ex(1, &mapi_link) == FAILURE) {
			RETURN_FALSE;
		}
		id = -1;

		break;
	default:
		WRONG_PARAM_COUNT;
		break;
	}

	ZEND_FETCH_RESOURCE(conn, phpMonetConn *, mapi_link, id, "MonetDB connection", le_link);

	RETURN_LONG(mapi_error(conn->mid));
}

/* }}} */

/* {{{ proto string monetdb_error([resource db])
   Returns the text of the error message from previous MonetDB operation */
PHP_FUNCTION(monetdb_error)
{
	zval **mapi_link = NULL;
	int id;
	phpMonetConn *conn;
	char *errstr;

	switch (ZEND_NUM_ARGS()) {
	case 0:
		id = MONET_G(default_link);

		break;
	case 1:
		if (zend_get_parameters_ex(1, &mapi_link) == FAILURE) {
			RETURN_FALSE;
		}
		id = -1;

		break;
	default:
		WRONG_PARAM_COUNT;
		break;
	}

	ZEND_FETCH_RESOURCE(conn, phpMonetConn *, mapi_link, id, "MonetDB connection", le_link);

	errstr = mapi_error_str(conn->mid);
	if (errstr != NULL) {
		RETURN_STRING(errstr, 1);
	} else {
		RETURN_STRING("", 1);
	}

}

/* }}} */


/* {{{ php_monetdb_fetch_hash
 */
static void
php_monetdb_fetch_hash(INTERNAL_FUNCTION_PARAMETERS, int result_type)
{
	zval **z_handle = NULL;
	MapiHdl handle;
	int i;

	if ((ZEND_NUM_ARGS() != 1) || (zend_get_parameters_ex(1, &z_handle) == FAILURE)) {
		WRONG_PARAM_COUNT;
	}

	if (Z_TYPE_PP(z_handle) == IS_RESOURCE && Z_LVAL_PP(z_handle) == 0)
		RETURN_FALSE;

	ZEND_FETCH_RESOURCE(handle, MapiHdl, z_handle, -1, "MonetDB result handle", le_handle);

	if ((result_type & MONETDB_BOTH) == 0) {
		php_error(E_CORE_ERROR, "php_monetdb_fetch_hash: result_type==%d is incorrect", result_type);
		/* php_error_docref(NULL TSRMLS_CC, E_CORE_ERROR, "php_monetdb_fetch_hash: result_type==%d is incorrect", result_type); */
		RETURN_FALSE;
	}

	if (!mapi_fetch_row(handle)) {	/* EOF or ERROR */
		/* TODO: check error */
		RETURN_FALSE;
	}

	if (array_init(return_value) == FAILURE) {
		RETURN_FALSE;
	}

	for (i = 0; i < mapi_get_field_count(handle); i++) {
		char *fieldtype = mapi_get_type(handle, i);
		char *value = mapi_fetch_field(handle, i);
		char *fieldname = mapi_get_name(handle, i);

		if (!value || !strcmp(value, "nil")) {	/* NULL VALUE */
			if (result_type & MONETDB_NUM) {
				add_index_null(return_value, i);
			}
			if (result_type & MONETDB_ASSOC) {
				add_assoc_null(return_value, fieldname);
			}
		} else {	/* not null */
			if ((strcmp(fieldtype, "tinyint") == 0) || (strcmp(fieldtype, "smallint") == 0) || (strcmp(fieldtype, "int") == 0) || (strcmp(fieldtype, "month_interval") == 0) || (strcmp(fieldtype, "sec_interval") == 0)) {
				/* long value */
				long lval = strtol(value, NULL, 10);

				if (result_type & MONETDB_NUM) {
					add_index_long(return_value, i, lval);
				}
				if (result_type & MONETDB_ASSOC) {
					add_assoc_long(return_value, fieldname, lval);
				}
			} else if ((strcmp(fieldtype, "double") == 0) || (strcmp(fieldtype, "real") == 0)) {
				/* double value */
				double dval = strtod(value, NULL);

				if (result_type & MONETDB_NUM) {
					add_index_double(return_value, i, dval);
				}
				if (result_type & MONETDB_ASSOC) {
					add_assoc_double(return_value, fieldname, dval);
				}
			} else if ((strcmp(fieldtype, "boolean") == 0) || (strcmp(fieldtype, "bool") == 0)) {
				int dval = (value[0] == 'f' || value[0] == 'F' || value[0] == '0') ? 0 : 1;

				if (result_type & MONETDB_NUM) {
					add_index_bool(return_value, i, dval);
				}
				if (result_type & MONETDB_ASSOC) {
					add_assoc_bool(return_value, fieldname, dval);
				}

			} else {
				/* strings */
				/* (strcmp(fieldtype, "decimal") == 0) */
				if (result_type & MONETDB_NUM) {
					add_index_string(return_value, i, value, 1);
				}
				if (result_type & MONETDB_ASSOC) {
					add_assoc_string(return_value, fieldname, value, 1);
				}
			}
		}		/* not null */
	}			/* for each row */
}

/* }}} */


/* {{{ proto array monetdb_fetch_row(resource result)
   Gets a result row as an enumerated array */
PHP_FUNCTION(monetdb_fetch_row)
{
	php_monetdb_fetch_hash(INTERNAL_FUNCTION_PARAM_PASSTHRU, MONETDB_NUM);
}

/* }}} */

/* {{{ proto object monetdb_fetch_object(resource result)
   Fetch a result row as an object */
PHP_FUNCTION(monetdb_fetch_object)
{
	php_monetdb_fetch_hash(INTERNAL_FUNCTION_PARAM_PASSTHRU, MONETDB_ASSOC);

	if (Z_TYPE_P(return_value) == IS_ARRAY) {
		object_and_properties_init(return_value, &zend_standard_class_def, Z_ARRVAL_P(return_value));
	}
}

/* }}} */


/* {{{ proto array monetdb_fetch_array(resource result)
   Fetch a result row as an array (associative and numeric ) */
PHP_FUNCTION(monetdb_fetch_array)
{
	php_monetdb_fetch_hash(INTERNAL_FUNCTION_PARAM_PASSTHRU, MONETDB_BOTH);
}

/* }}} */


/* {{{ proto array monetdb_fetch_assoc(resource result)
   Fetch a result row as an associative array */
PHP_FUNCTION(monetdb_fetch_assoc)
{
	php_monetdb_fetch_hash(INTERNAL_FUNCTION_PARAM_PASSTHRU, MONETDB_ASSOC);
}

/* }}} */

/* {{{ proto bool monetdb_data_seek(resource result, int row_number)
   Move internal result pointer */
PHP_FUNCTION(monetdb_data_seek)
{
	zval **result, **offset;
	MapiHdl handle;

	if (ZEND_NUM_ARGS() != 2 || zend_get_parameters_ex(2, &result, &offset) == FAILURE) {
		WRONG_PARAM_COUNT;
	}

	ZEND_FETCH_RESOURCE(handle, MapiHdl, result, -1, "MonetDB result handle", le_handle);

	convert_to_long_ex(offset);
	if (Z_LVAL_PP(offset) < 0 || Z_LVAL_PP(offset) >= (int) mapi_get_row_count(handle)) {
		php_error(E_WARNING, "monetdb_data_seek: Offset %ld is invalid for MonetDB result index %ld", Z_LVAL_PP(offset), Z_LVAL_PP(result));
		/* php_error_docref(NULL TSRMLS_CC, E_WARNING, "Offset %ld is invalid for MonetDB result index %ld", Z_LVAL_PP(offset), Z_LVAL_PP(result)); */
		RETURN_FALSE;
	}
	mapi_seek_row(handle, Z_LVAL_PP(offset), MAPI_SEEK_SET);
	RETURN_TRUE;
}

/* }}} */

/* {{{ proto string monetdb_escape_string(string to_be_escaped)
   Escape string for monetdb query */
PHP_FUNCTION(monetdb_escape_string)
{
	zval **str;
	char *tmp;
	char *realstr;

	if (ZEND_NUM_ARGS() != 1 || zend_get_parameters_ex(1, &str) == FAILURE) {
		ZEND_WRONG_PARAM_COUNT();
	}
	convert_to_string_ex(str);

	tmp = mapi_quote(Z_STRVAL_PP(str), Z_STRLEN_PP(str));

	realstr = (char *) emalloc(strlen(tmp));
	strcpy(realstr, tmp);
	free(tmp);
	RETURN_STRINGL(realstr, strlen(realstr), 0);
}

/* }}} */

/* {{{ proto long monetdb_affected_rows([int link_identifier])
   Get affected row count */
PHP_FUNCTION(monetdb_affected_rows)
{
	zval **result;
	MapiHdl handle;

	if (ZEND_NUM_ARGS() != 1 || zend_get_parameters_ex(1, &result) == FAILURE) {
		WRONG_PARAM_COUNT;
	}

	ZEND_FETCH_RESOURCE(handle, MapiHdl, result, -1, "MonetDB result handle", le_handle);

	RETURN_LONG(mapi_rows_affected(handle));
}

/* }}} */

/* {{{ proto bool monetdb_ping([int link_identifier])
   Ping a server connection. If no connection then reconnect. */
PHP_FUNCTION(monetdb_ping)
{
	zval **mapi_link = NULL;
	int id;
	phpMonetConn *conn;

	switch (ZEND_NUM_ARGS()) {
	case 0:
		id = MONET_G(default_link);

		break;
	case 1:
		if (zend_get_parameters_ex(1, &mapi_link) == FAILURE) {
			RETURN_FALSE;
		}
		id = -1;

		break;
	default:
		WRONG_PARAM_COUNT;
		break;
	}

	ZEND_FETCH_RESOURCE(conn, phpMonetConn *, mapi_link, id, "MonetDB connection", le_link);

	RETURN_BOOL(!mapi_ping(conn->mid));
}

/* }}} */

/* {{{ proto bool mysql_free_result(resource result)
   Free result memory */
PHP_FUNCTION(monetdb_free_result)
{
	zval **z_handle = NULL;
	MapiHdl handle;

	if ((ZEND_NUM_ARGS() != 1) || (zend_get_parameters_ex(1, &z_handle) == FAILURE)) {
		WRONG_PARAM_COUNT;
	}

	if (Z_TYPE_PP(z_handle) == IS_RESOURCE && Z_LVAL_PP(z_handle) == 0)
		RETURN_FALSE;

	ZEND_FETCH_RESOURCE(handle, MapiHdl, z_handle, -1, "MonetDB result handle", le_handle);

	zend_list_delete(Z_LVAL_PP(z_handle));
	RETURN_TRUE;
}

/* }}} */

/* {{{ proto bool monetdb_info([int link_identifier])
   Returns associative array with information about the connection and server. */
PHP_FUNCTION(monetdb_info)
{
	zval **mapi_link = NULL;
	int id;
	phpMonetConn *conn;
	char *val;
	long l;

	switch (ZEND_NUM_ARGS()) {
	case 0:
		id = MONET_G(default_link);

		break;
	case 1:
		if (zend_get_parameters_ex(1, &mapi_link) == FAILURE) {
			RETURN_FALSE;
		}
		id = -1;

		break;
	default:
		WRONG_PARAM_COUNT;
		break;
	}

	ZEND_FETCH_RESOURCE(conn, phpMonetConn *, mapi_link, id, "MonetDB connection", le_link);

	if (array_init(return_value) == FAILURE) {
		RETURN_FALSE;
	}

	if ((val = mapi_get_dbname(conn->mid)))
		add_assoc_string(return_value, "dbname", val, 1);
	if ((val = mapi_get_host(conn->mid)))
		add_assoc_string(return_value, "host", val, 1);
	if ((val = mapi_get_user(conn->mid)))
		add_assoc_string(return_value, "user", val, 1);

	if ((val = mapi_get_lang(conn->mid)))
		add_assoc_string(return_value, "language", val, 1);

	/* add_assoc_string(return_value, "motd", mapi_get_motd(conn->mid), 1);  */
	if ((val = mapi_get_mapi_version(conn->mid)))
		add_assoc_string(return_value, "mapi version", val, 1);

	if ((val = mapi_get_monet_version(conn->mid)))
		add_assoc_string(return_value, "monet version", val, 1);

	if ((l = mapi_get_monet_versionId(conn->mid)))
		add_assoc_long(return_value, "monet version id", l);
}


/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
