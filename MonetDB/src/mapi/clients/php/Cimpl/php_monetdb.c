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
  | Author: Marcin Zukowski <marcin@cwi.nl>                              |
  | partly derived from work of authors of MySQL PHP module and          |
  | Manfred Stienstra <manfred.stienstra@dwerg.net>                      |
  +----------------------------------------------------------------------+
*/

/* $Id$ */

#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "php_monetdb.h"

#define MONETDB_PHP_VERSION "0.01"

#include <Mapi.h>
#include <stdlib.h>
#include <string.h>

#define MONETDB_ASSOC	1<<0
#define MONETDB_NUM		1<<1
#define MONETDB_BOTH	(MONETDB_ASSOC|MONETDB_NUM)

ZEND_DECLARE_MODULE_GLOBALS(monetdb)

/* True global resources - no need for thread safety here */
static int le_link ;
static int le_handle ;

/* TODO: maybe we want to introduce persistant connections?
static int le_plink; */

/* {{{ monetdb_functions[]
 *
 * Every user visible function must have an entry in monet_functions[].
 */
function_entry monetdb_functions[] = {
    PHP_FE(monetdb_connect,                 NULL)
    PHP_FE(monetdb_close,                   NULL)
    PHP_FE(monetdb_query,                   NULL)
    PHP_FE(monetdb_num_rows,	           	NULL)
    PHP_FE(monetdb_num_fields,				NULL)
    PHP_FE(monetdb_field_name,				NULL)
    PHP_FE(monetdb_field_type,				NULL)
    PHP_FE(monetdb_errno,         			NULL)
    PHP_FE(monetdb_error,         			NULL)
    PHP_FE(monetdb_fetch_array,         	NULL)
    PHP_FE(monetdb_fetch_assoc,         	NULL)
    PHP_FE(monetdb_fetch_object,        	NULL)
    PHP_FE(monetdb_fetch_row,               NULL)
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
	PHP_RINIT(monetdb),		/* Replace with NULL if there's nothing to do at request start */
	PHP_RSHUTDOWN(monetdb),	/* Replace with NULL if there's nothing to do at request end */
	PHP_MINFO(monetdb),
#if ZEND_MODULE_API_NO >= 20010901
	MONETDB_PHP_VERSION, /* Replace with version number for your extension */
#endif
	STANDARD_MODULE_PROPERTIES
};
/* }}} */

#ifdef COMPILE_DL_MONETDB
ZEND_GET_MODULE(monetdb)
#endif

/* {{{ _free_monetdb_link
 */
static void _free_monetdb_link(zend_rsrc_list_entry *rsrc TSRMLS_DC)
{
    Mapi monet_link = (Mapi)rsrc->ptr;
	mapi_destroy(monet_link);
}
/* }}} */

/* {{{ _free_monetdb_link
 */
static void _free_monetdb_handle(zend_rsrc_list_entry *rsrc TSRMLS_DC)
{
    MapiHdl monet_handle = (MapiHdl)rsrc->ptr;
	mapi_close_handle(monet_handle);
}
/* }}} */

/* {{{ PHP_INI
 */
/* Remove comments and fill if you need to have entries in php.ini
*/
PHP_INI_BEGIN()
    STD_PHP_INI_ENTRY("monetdb.default_port",		"50000",		PHP_INI_ALL, OnUpdateInt,		default_port,		zend_monetdb_globals, monetdb_globals)
    STD_PHP_INI_ENTRY("monetdb.default_language",	"mil",			PHP_INI_ALL, OnUpdateString,	default_language,	zend_monetdb_globals, monetdb_globals)
    STD_PHP_INI_ENTRY("monetdb.default_hostname",	"localhost", 	PHP_INI_ALL, OnUpdateString,	default_hostname,	zend_monetdb_globals, monetdb_globals)
    STD_PHP_INI_ENTRY("monetdb.default_username",	"monetdb",		PHP_INI_ALL, OnUpdateString,	default_username,	zend_monetdb_globals, monetdb_globals)
    STD_PHP_INI_ENTRY("monetdb.default_password",	"monetdb",		PHP_INI_ALL, OnUpdateString,	default_password,	zend_monetdb_globals, monetdb_globals)
PHP_INI_END()
/* }}} */

/* {{{ php_monetdb_init_globals
 */
static void php_monetdb_init_globals(zend_monetdb_globals *monetdb_globals)
{
	monetdb_globals->default_port = 0;
	monetdb_globals->default_language = NULL;
	monetdb_globals->default_hostname = NULL;
	monetdb_globals->default_username = NULL;
	monetdb_globals->default_password = NULL;
}
/* }}} */

/* {{{ PHP_MINIT_FUNCTION
 */
PHP_MINIT_FUNCTION(monetdb)
{
	ZEND_INIT_MODULE_GLOBALS(monetdb, php_monetdb_init_globals, NULL);
	REGISTER_INI_ENTRIES();

    le_link = zend_register_list_destructors_ex(_free_monetdb_link, NULL,
		"MonetDB connection", module_number);
	le_handle = zend_register_list_destructors_ex(_free_monetdb_handle, NULL, 
		"MonetDB result handle", module_number);

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
static void php_monetdb_set_default_link(int id TSRMLS_DC)
{
	MONET_G(default_link) = id;
	zend_list_addref(id);
}
/* }}} */

/* {{{ php_monetdb_set_default_link
 */
static void php_monetdb_set_default_handle(int id TSRMLS_DC)
{
	MONET_G(default_handle) = id;
	zend_list_addref(id);
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
	Mapi conn;
	char *hostname = NULL ;
	char *username = NULL ;
	char *password = NULL ;
	char *language = NULL ;
	int port = 50000 ;
//	int host_len;
//	int username_len;
//	int password_len;
	zval **z_hostname=NULL, **z_username=NULL, **z_password=NULL, **z_language=NULL, **z_port=NULL;

//	zval *new_string;
	
	hostname = MONET_G(default_hostname);
	username = MONET_G(default_username);
	password = MONET_G(default_password);
	language = MONET_G(default_language);
	port = MONET_G(default_port);
	
	/* Parse parameters */
	switch( ZEND_NUM_ARGS() ) {
		case 0 : /* defaults */
			break;
		case 1 : /* language */ 
			if (zend_get_parameters_ex(1, &z_language)==FAILURE) {
				MONETDB_CONNECT_RETURN_FALSE();
			}
			break ;
		case 2 : /* language, hostname */ 
			if (zend_get_parameters_ex(2, &z_language, z_hostname)==FAILURE) {
				MONETDB_CONNECT_RETURN_FALSE();
			}
			break ;
		case 3 : /* language, hostname, port */
			if (zend_get_parameters_ex(3, &z_language, &z_hostname, &z_port)==FAILURE) {
				MONETDB_CONNECT_RETURN_FALSE();
			}
			break ;
		case 4 : /* language, hostname, port, username */
			if (zend_get_parameters_ex(3, &z_language, &z_hostname, &z_port, &z_username)==FAILURE) {
				MONETDB_CONNECT_RETURN_FALSE();
			}
			break ;
		case 5 : /* language, hostname, port, password */
			if (zend_get_parameters_ex(3, &z_language, &z_hostname, &z_port, &z_username, &z_password)==FAILURE) {
				MONETDB_CONNECT_RETURN_FALSE();
			}
			break ;
		default:
			WRONG_PARAM_COUNT ;
			break ;
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
	
	//~ printf("MON: Connecting to: hostname=%s port=%d username=%s password=%s language=%s\n",
		//~ hostname, port, username, password, language);
	
	/* Connect to MonetDB server */
	conn = mapi_connect(hostname, port, username, password, language);
	
	if (mapi_error(conn) || mapi_error_str(conn))
	{
		//~ printf("MON: failed\n");
		MONETDB_CONNECT_RETURN_FALSE();
	}
	
	//~ printf("MON: succeeded, conn=%p\n", conn);
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
	zval **mapi_link=NULL ;
	int id ;
	Mapi conn ;
	
	switch( ZEND_NUM_ARGS() ) {
		case 0:
			id = MONET_G(default_link);
			break ;
		case 1:
			if (zend_get_parameters_ex(1, &mapi_link)==FAILURE) {
				RETURN_FALSE;
			}
			id = -1 ;
			break ;
		default:
			WRONG_PARAM_COUNT; 
			break;
	}
	
	ZEND_FETCH_RESOURCE(conn, Mapi, mapi_link, id, "MonetDB connection", le_link);
	
	//~ printf("MON: disconnecting %p\n", conn);
	mapi_disconnect(conn);

	if (id==-1) { /* explicit resource number */
		zend_list_delete(Z_RESVAL_PP(mapi_link));
	} 

	if (id!=-1 || (mapi_link && Z_RESVAL_PP(mapi_link)==MONET_G(default_link))) {
		zend_list_delete(MONET_G(default_link));
		MONET_G(default_link) = -1;
	}
		
	if (mapi_error(conn) && mapi_error_str(conn)) {
		RETURN_FALSE;
	} else {
		RETURN_TRUE;
	}
}
/* }}} */


/* {{{ proto bool monetdb_query(string query[, resource db])
   run a query on the MonetDB server */
PHP_FUNCTION(monetdb_query)
{
	zval **z_query=NULL, **z_mapi_link=NULL ;
	int id ;
	Mapi conn ;
	char *query = NULL ;
	
	switch( ZEND_NUM_ARGS() ) {
		case 1:
			if (zend_get_parameters_ex(1, &z_query)==FAILURE) {
				RETURN_FALSE;
			}
			id = MONET_G(default_link);
			break ;
		case 2:
			if (zend_get_parameters_ex(2, &z_query, &z_mapi_link)==FAILURE) {
				RETURN_FALSE;
			}
			id = -1 ;
			break ;
		default:
			WRONG_PARAM_COUNT; 
			break;
	}
	
	ZEND_FETCH_RESOURCE(conn, Mapi, z_mapi_link, id, "MonetDB connection", le_link);
	
	convert_to_string_ex(z_query);
	query = Z_STRVAL_PP(z_query);
	
	//~ printf("MON: query=%s conn=%p\n", query, conn);
	
	MapiHdl handle = mapi_query(conn, query);
	if (mapi_error(conn)) {
		mapi_close_handle(handle);
		php_error_docref("function.monetdb_query" TSRMLS_CC, E_WARNING, 
			"Mapi Error #%d", mapi_error(conn));
		RETURN_FALSE ;
	}
	//~ printf("MON: successfull, handle=%p\n", handle);
	//~ printf("MON: -- error=%d, \n%s \n", mapi_error(conn), mapi_error_str(conn));
	ZEND_REGISTER_RESOURCE(return_value, handle, le_handle);
	php_monetdb_set_default_handle(Z_LVAL_P(return_value) TSRMLS_CC);
}
/* }}} */


/* {{{ proto bool monetdb_num_rows([resource handle])
   return number of rows (tuples) in a query result */
PHP_FUNCTION(monetdb_num_rows)
{
	zval **z_handle=NULL ;
	int id ;
	MapiHdl handle;
	
	switch( ZEND_NUM_ARGS() ) {
		case 0:
			id = MONET_G(default_handle);
			break ;
		case 1:
			if (zend_get_parameters_ex(1, &z_handle)==FAILURE) {
				RETURN_FALSE;
			}
			id = -1 ;
			break ;
		default:
			WRONG_PARAM_COUNT; 
			break;
	}
	
	ZEND_FETCH_RESOURCE(handle, MapiHdl, z_handle, id, "MonetDB result handle", le_handle);
	
	//~ printf("MON: num rows handle=%p\n", handle);
	
	Z_LVAL_P(return_value) = (long) mapi_get_row_count(handle);
	Z_TYPE_P(return_value) = IS_LONG;
}
/* }}} */

/* {{{ proto bool monetdb_num_fields([resource handle])
   return number of fields (columns) in a query result */
PHP_FUNCTION(monetdb_num_fields)
{
	zval **z_handle=NULL ;
	int id ;
	MapiHdl handle;
	
	switch( ZEND_NUM_ARGS() ) {
		case 0:
			id = MONET_G(default_handle);
			break ;
		case 1:
			if (zend_get_parameters_ex(1, &z_handle)==FAILURE) {
				RETURN_FALSE;
			}
			id = -1 ;
			break ;
		default:
			WRONG_PARAM_COUNT; 
			break;
	}
	
	ZEND_FETCH_RESOURCE(handle, MapiHdl, z_handle, id, "MonetDB result handle", le_handle);
	
	//~ printf("MON: num fields handle=%p\n", handle);
	
	Z_LVAL_P(return_value) = (long) mapi_get_field_count(handle);
	Z_TYPE_P(return_value) = IS_LONG;
}
/* }}} */

/* {{{ proto string monetdb_field_name(int index[, resource handle])
   return a name of a specified field (column) */
PHP_FUNCTION(monetdb_field_name)
{
	zval **z_index=NULL, **z_handle=NULL ;
	int id ;
	MapiHdl handle ;
	long index ;
	
	switch( ZEND_NUM_ARGS() ) {
		case 1:
			if (zend_get_parameters_ex(1, &z_index)==FAILURE) {
				RETURN_FALSE;
			}
			id = MONET_G(default_handle);
			break ;
		case 2:
			if (zend_get_parameters_ex(2, &z_index, &z_handle)==FAILURE) {
				RETURN_FALSE;
			}
			id = -1 ;
			break ;
		default:
			WRONG_PARAM_COUNT; 
			break;
	}
	
	ZEND_FETCH_RESOURCE(handle, MapiHdl, z_handle, id, "MonetDB result handle", le_handle);
	
	convert_to_long_ex(z_index);
	index = Z_LVAL_PP(z_index);

	//~ printf("MON: _field_name: index=%d handle=%p\n", index, handle);
	
	if (index >= mapi_get_field_count(handle)) {
		php_error_docref("function.monetdb_field_name" TSRMLS_CC, E_ERROR, 
			"Accessing field number #%d, which is out of range", index);
		RETURN_FALSE ;
	}
	
	char *name = mapi_get_name(handle, index);
	
	//~ printf("MON: _field_name: name=%s\n", name);
	
	Z_STRLEN_P(return_value) = strlen(name);
	Z_STRVAL_P(return_value) = estrndup(name, Z_STRLEN_P(return_value));
	Z_TYPE_P(return_value) = IS_STRING;
}
/* }}} */

/* {{{ proto string monetdb_field_type(int index[, resource handle])
   return a type (as string) of a specified field (column) */
PHP_FUNCTION(monetdb_field_type)
{
	zval **z_index=NULL, **z_handle=NULL ;
	int id ;
	MapiHdl handle ;
	long index ;
	
	switch( ZEND_NUM_ARGS() ) {
		case 1:
			if (zend_get_parameters_ex(1, &z_index)==FAILURE) {
				RETURN_FALSE;
			}
			id = MONET_G(default_handle);
			break ;
		case 2:
			if (zend_get_parameters_ex(2, &z_index, &z_handle)==FAILURE) {
				RETURN_FALSE;
			}
			id = -1 ;
			break ;
		default:
			WRONG_PARAM_COUNT; 
			break;
	}
	
	ZEND_FETCH_RESOURCE(handle, MapiHdl, z_handle, id, "MonetDB result handle", le_handle);
	
	convert_to_long_ex(z_index);
	index = Z_LVAL_PP(z_index);
	
	//~ printf("MON: _field_type: index=%d handle=%p\n", index, handle);
	
	if (index >= mapi_get_field_count(handle)) {
		php_error_docref("function.monetdb_field_type" TSRMLS_CC, E_ERROR, 
			"Accessing field number #%d, which is out of range", index);
		RETURN_FALSE ;
	}
	
	char *type = mapi_get_type(handle, index);
	
	//~ printf("MON: _field_type: type=%s\n", type);
	
	Z_STRLEN_P(return_value) = strlen(type);
	Z_STRVAL_P(return_value) = estrndup(type, Z_STRLEN_P(return_value));
	Z_TYPE_P(return_value) = IS_STRING;
}
/* }}} */

/* {{{ proto int monetdb_errno([resource db])
   Returns the numerical value of the error message from previous MonetDB operation */
PHP_FUNCTION(monetdb_errno)
{
	zval **mapi_link=NULL ;
	int id ;
	Mapi conn ;
	
	switch( ZEND_NUM_ARGS() ) {
		case 0:
			id = MONET_G(default_link);
			break ;
		case 1:
			if (zend_get_parameters_ex(1, &mapi_link)==FAILURE) {
				RETURN_FALSE;
			}
			id = -1 ;
			break ;
		default:
			WRONG_PARAM_COUNT; 
			break;
	}
	
	ZEND_FETCH_RESOURCE(conn, Mapi, mapi_link, id, "MonetDB connection", le_link);
	
	RETURN_LONG(mapi_error(conn));
}
/* }}} */

/* {{{ proto string monetdb_error([resource db])
   Returns the text of the error message from previous MonetDB operation */
PHP_FUNCTION(monetdb_error)
{
	zval **mapi_link=NULL ;
	int id ;
	Mapi conn ;
	
	switch( ZEND_NUM_ARGS() ) {
		case 0:
			id = MONET_G(default_link);
			break ;
		case 1:
			if (zend_get_parameters_ex(1, &mapi_link)==FAILURE) {
				RETURN_FALSE;
			}
			id = -1 ;
			break ;
		default:
			WRONG_PARAM_COUNT; 
			break;
	}
	
	ZEND_FETCH_RESOURCE(conn, Mapi, mapi_link, id, "MonetDB connection", le_link);
	
	char *errstr = mapi_error_str(conn);
	if (errstr != NULL) {
		RETURN_STRING(errstr, 1);
	} else {
		RETURN_STRING("", 1);
	}

}
/* }}} */


/* {{{ php_monetdb_fetch_hash
 */
static void php_monetdb_fetch_hash(INTERNAL_FUNCTION_PARAMETERS, int result_type)
{
	
	zval **z_handle=NULL ;
	int id ;
	MapiHdl handle;
	int i ;
	
	switch( ZEND_NUM_ARGS() ) {
		case 0:
			id = MONET_G(default_handle);
			break ;
		case 1:
			if (zend_get_parameters_ex(1, &z_handle)==FAILURE) {
				RETURN_FALSE;
			}
			id = -1 ;
			break ;
		default:
			WRONG_PARAM_COUNT; 
			break;
	}
	
	ZEND_FETCH_RESOURCE(handle, MapiHdl, z_handle, id, "MonetDB result handle", le_handle);
	
	if ((result_type & MONETDB_BOTH) == 0) {
		php_error_docref(NULL TSRMLS_CC, E_CORE_ERROR, 
			"php_monetdb_fetch_hash: result_type==%d is incorrect", result_type);
		RETURN_FALSE ;
	}
	
	if (!mapi_fetch_row(handle)) { /* EOF or ERROR*/
		/* TODO: check error */
		RETURN_FALSE ;
	}
	
	if (array_init(return_value)==FAILURE) {
		RETURN_FALSE;
	}
	
	for(i=0; i<mapi_get_field_count(handle) ; i++) {
		char *fieldtype = mapi_get_type(handle, i);
		char *value = mapi_fetch_field(handle, i);
		char *fieldname = mapi_get_name(handle, i);
		
		if (!strcmp(value, "nil")) { /* NULL VALUE */
			if (result_type & MONETDB_NUM) {
				add_index_null(return_value, i);
			}
			if (result_type & MONETDB_ASSOC) {
				add_assoc_null(return_value, fieldname);
			}
		} else { /* not null */
			if ((strcmp(fieldtype, "tinyint")==0) ||
					(strcmp(fieldtype, "smallint")==0) ||
					(strcmp(fieldtype, "mediumint")==0) ||
					(strcmp(fieldtype, "integer")==0) ||
					(strcmp(fieldtype, "number")==0) ||
					(strcmp(fieldtype, "int")==0) ||
					(strcmp(fieldtype, "decimal")==0) ||
					(strcmp(fieldtype, "numeric")==0) ||
					(strcmp(fieldtype, "month_interval")==0) ||
					(strcmp(fieldtype, "sec_interval")==0) ||
					(strcmp(fieldtype, "boolean")==0) ||
					(strcmp(fieldtype, "bool")==0)) {
				/* long value */
				long lval = strtol(value, NULL, 10);
				if (result_type & MONETDB_NUM) {
					add_index_long(return_value, i, lval);
				} 
				if (result_type & MONETDB_ASSOC) {
					add_assoc_long(return_value, fieldname, lval);
				}
			} else if ((strcmp(fieldtype, "float")==0) ||
					(strcmp(fieldtype, "double")==0) ||
					(strcmp(fieldtype, "real")==0)) {
				/* double value */
				double dval = strtod(value, NULL);
				if (result_type & MONETDB_NUM) {
					add_index_double(return_value, i, dval);
				} 
				if (result_type & MONETDB_ASSOC) {
					add_assoc_double(return_value, fieldname, dval);
				}
			} else {
				/* strings */
				if (result_type & MONETDB_NUM) {
					add_index_string(return_value, i, value, 1);
				} 
				if (result_type & MONETDB_ASSOC) {
					add_assoc_string(return_value, fieldname, value, 1);
				}
			}
		} /* not null */
	} /* for each row */
}
/* }}} */


/* {{{ proto array monetdb_fetch_row([resource result])
   Gets a result row as an enumerated array */
PHP_FUNCTION(monetdb_fetch_row)
{
	php_monetdb_fetch_hash(INTERNAL_FUNCTION_PARAM_PASSTHRU, MONETDB_NUM);
}
/* }}} */

/* {{{ proto object monetdb_fetch_object([resource result])
   Fetch a result row as an object */
PHP_FUNCTION(monetdb_fetch_object)
{
	php_monetdb_fetch_hash(INTERNAL_FUNCTION_PARAM_PASSTHRU, MONETDB_ASSOC);

	if (Z_TYPE_P(return_value) == IS_ARRAY) {
		object_and_properties_init(return_value, ZEND_STANDARD_CLASS_DEF_PTR, Z_ARRVAL_P(return_value));
	}
}
/* }}} */


/* {{{ proto array monetdb_fetch_array([resource result])
   Fetch a result row as an array (associative and numeric ) */
PHP_FUNCTION(monetdb_fetch_array)
{
	php_monetdb_fetch_hash(INTERNAL_FUNCTION_PARAM_PASSTHRU, MONETDB_BOTH);
}
/* }}} */


/* {{{ proto array monetdb_fetch_assoc([resource result])
   Fetch a result row as an associative array */
PHP_FUNCTION(monetdb_fetch_assoc)
{
	php_monetdb_fetch_hash(INTERNAL_FUNCTION_PARAM_PASSTHRU, MONETDB_ASSOC);
}
/* }}} */

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
