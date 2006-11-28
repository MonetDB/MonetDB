/*
   +----------------------------------------------------------------------+
   | PHP Version 5                                                        |
   +----------------------------------------------------------------------+
   | Copyright (c) 1997-2006 The PHP Group                                |
   +----------------------------------------------------------------------+
   | This source file is subject to version 3.01 of the PHP license,      |
   | that is bundled with this package in the file LICENSE, and is        |
   | available through the world-wide-web at the following url:           |
   | http://www.php.net/license/3_01.txt                                  |
   | If you did not receive a copy of the PHP license and are unable to   |
   | obtain it through the world-wide-web, please send a note to          |
   | license@php.net so we can mail you a copy immediately.               |
   +----------------------------------------------------------------------+
   | Authors: Zeev Suraski <zeev@zend.com>                                |
   |          Jouni Ahto <jouni.ahto@exdec.fi>                            |
   |          Yasuo Ohgaki <yohgaki@php.net>                              |
   |          Youichi Iwakiri <yiwakiri@st.rim.or.jp> (pg_copy_*)         | 
   |          Chris Kings-Lynne <chriskl@php.net> (v3 protocol)           | 
   |          Fabian Groffen <fabian@cwi.nl> (MonetDB version)            |
   +----------------------------------------------------------------------+
 */
 
/* $Id$ */

#define SMART_STR_PREALLOC 512

#ifdef _MSC_VER
#define WIN32 1
#define WINNT 1
#endif

#include <stdlib.h>

#include "php.h"
#include "php_ini.h"
#include "ext/standard/php_standard.h"
#include "ext/standard/php_smart_str.h"

#undef PACKAGE_BUGREPORT
#undef PACKAGE_NAME
#undef PACKAGE_STRING
#undef PACKAGE_TARNAME
#undef PACKAGE_VERSION
#include "php_monetdb.h"
#include "php_globals.h"
#include "zend_exceptions.h"

#define MONETDB_ASSOC		1<<0
#define MONETDB_NUM			1<<1
#define MONETDB_BOTH		(MONETDB_ASSOC|MONETDB_NUM)

#define MONETDB_CONNECTION_OK	1
#define MONETDB_CONNECTION_BAD	2

#define MONETDB_STATUS_LONG     1
#define MONETDB_STATUS_STRING   2

#define MONETDB_MAX_LENGTH_OF_LONG   30
#define MONETDB_MAX_LENGTH_OF_DOUBLE 60

#define CHECK_DEFAULT_LINK(x) if ((x) == -1) { \
	php_error_docref(NULL TSRMLS_CC, E_WARNING, "No MonetDB link opened yet"); \
} \

/* {{{ monetdb_functions[]
 */
zend_function_entry monetdb_functions[] = {
	/* connection functions */
	PHP_FE(monetdb_connect,				NULL)
	PHP_FE(monetdb_pconnect,			NULL)
	PHP_FE(monetdb_close,				NULL)
	PHP_FE(monetdb_connection_status,	NULL)
	PHP_FE(monetdb_connection_reset,	NULL)
	PHP_FE(monetdb_host,				NULL)
	PHP_FE(monetdb_dbname,				NULL)
	PHP_FE(monetdb_version,				NULL)
	PHP_FE(monetdb_ping,				NULL)
	/* query functions */
	PHP_FE(monetdb_query,				NULL)
#ifdef I_FEEL_LIKE_IMPLEMENTING_PREPARED_STUFF
	PHP_FE(monetdb_connection_busy,		NULL)
	PHP_FE(monetdb_query_params,		NULL)
	PHP_FE(monetdb_prepare,				NULL)
	PHP_FE(monetdb_execute,				NULL)
	PHP_FE(monetdb_send_query,			NULL)
	PHP_FE(monetdb_send_query_params,	NULL)
	PHP_FE(monetdb_send_prepare,		NULL)
	PHP_FE(monetdb_send_execute,		NULL)
	PHP_FE(monetdb_get_result,			NULL)
	PHP_FE(monetdb_result_status,		NULL)
#endif
	/* result functions */
	PHP_FE(monetdb_fetch_result,		NULL)
	PHP_FE(monetdb_fetch_row,			NULL)
	PHP_FE(monetdb_fetch_assoc,			NULL)
	PHP_FE(monetdb_fetch_array,			NULL)
	PHP_FE(monetdb_fetch_object,		NULL)
	PHP_FE(monetdb_affected_rows,		NULL)
	PHP_FE(monetdb_result_seek,			NULL)
	PHP_FE(monetdb_free_result,			NULL)
	PHP_FE(monetdb_num_rows,			NULL)
	PHP_FE(monetdb_num_fields,			NULL)
	PHP_FE(monetdb_field_name,			NULL)
	PHP_FE(monetdb_field_table,			NULL)
	PHP_FE(monetdb_field_type,			NULL)
	PHP_FE(monetdb_field_num,			NULL)
	PHP_FE(monetdb_field_prtlen,		NULL)
	PHP_FE(monetdb_field_is_null,		NULL)
	/* error message functions */
	PHP_FE(monetdb_last_error,			NULL)
	PHP_FE(monetdb_last_notice,			NULL)
	/* utility functions */
	PHP_FE(monetdb_escape_string,		NULL)
#ifdef I_FEEL_LIKE_IMPLEMENTING_COPY_INTO_PROPERLY
	/* copy functions */
	PHP_FE(monetdb_put_line,			NULL)
	PHP_FE(monetdb_end_copy,			NULL)
	PHP_FE(monetdb_copy_to,				NULL)
	PHP_FE(monetdb_copy_from,			NULL)
#endif
#ifdef I_FEEL_LIKE_IMPLEMENTING_POSTGRES_EXPERIMENTAL_FUNCTIONS
	/* misc functions */
	PHP_FE(monetdb_meta_data,			NULL)
	PHP_FE(monetdb_convert,				NULL)
	PHP_FE(monetdb_insert,				NULL)
	PHP_FE(monetdb_update,				NULL)
	PHP_FE(monetdb_delete,				NULL)
	PHP_FE(monetdb_select,				NULL)
#endif
	{NULL, NULL, NULL} 
};
/* }}} */

/* {{{ pgsql_module_entry
 */
zend_module_entry monetdb_module_entry = {
	STANDARD_MODULE_HEADER,
	"monetdb",
	monetdb_functions,
	PHP_MINIT(monetdb),
	PHP_MSHUTDOWN(monetdb),
	PHP_RINIT(monetdb),
	PHP_RSHUTDOWN(monetdb),
	PHP_MINFO(monetdb),
	NO_VERSION_YET,
	STANDARD_MODULE_PROPERTIES
};
/* }}} */

#ifdef COMPILE_DL_MONETDB
ZEND_GET_MODULE(monetdb)
#endif

static int le_link, le_plink, le_result, le_lofp, le_string;

ZEND_DECLARE_MODULE_GLOBALS(monetdb)

/* {{{ _php_monetdb_trim_message */
static char * _php_monetdb_trim_message(const char *message, int *len)
{
	register int i;
	register const char* j = message;

	if (message == NULL)
		return(estrdup("(null)"));

	i = strlen(message)-1;

	/* trim newlines from the end */
	while (i>0 && (message[i] == '\r' || message[i] == '\n'))
		i--;
	i++;

	/* remove (!(ERROR: )?)? from the start */
	if (*j == '!')
		j++;
	if (strncmp(j, "ERROR: ", 7) == 0)
		j += 7;

	/* correct the length based on the new start */
	i -= (int)(j - message);

	if (len)
		*len = i;

	return(estrndup(j, i));
}
/* }}} */

#define PHP_MONETDB_ERROR(text, monetdb) { \
	char *msgbuf = _php_monetdb_trim_message(mapi_error_str(monetdb), NULL); \
	php_error_docref(NULL TSRMLS_CC, E_WARNING, text, msgbuf); \
	_php_monetdb_error_handler(monetdb, msgbuf); \
} \

#define PHP_MONETDB_ERROR_RESULT(text, monetdb_rh) { \
	char *msgbuf = _php_monetdb_trim_message(mapi_result_error(monetdb_rh->result), NULL); \
	php_error_docref(NULL TSRMLS_CC, E_WARNING, text, msgbuf); \
	_php_monetdb_error_handler(monetdb_rh->conn, msgbuf); \
} \

/* {{{ php_monetdb_set_default_link
 */
static void php_monetdb_set_default_link(int id TSRMLS_DC)
{   
	zend_list_addref(id);

	if (MG(default_link) != -1) {
		zend_list_delete(MG(default_link));
	}

	MG(default_link) = id;
}
/* }}} */

/* {{{ _close_monetdb_link
 */
static void _close_monetdb_link(zend_rsrc_list_entry *rsrc TSRMLS_DC)
{
	Mconn *link = (Mconn *)rsrc->ptr;

	mapi_disconnect(link);
	mapi_destroy(link);
	MG(num_links)--;
}
/* }}} */

/* {{{ _close_monetdb_plink
 */
static void _close_monetdb_plink(zend_rsrc_list_entry *rsrc TSRMLS_DC)
{
	Mconn *link = (Mconn *)rsrc->ptr;

	mapi_disconnect(link);
	mapi_destroy(link);
	MG(num_persistent)--;
	MG(num_links)--;
}
/* }}} */

#if 0
	reserved for future use
/* {{{ _php_monetdb_notice_handler
 */
static void _php_monetdb_notice_handler(void *resource_id, const char *message)
{
	php_monetdb_notice *notice;
	
	TSRMLS_FETCH();
	if (! MG(ignore_notices)) {
		notice = (php_monetdb_notice *)emalloc(sizeof(php_monetdb_notice));
		notice->message = _php_monetdb_trim_message(message, &(notice->len));
		if (MG(log_notices)) {
			php_error_docref(NULL TSRMLS_CC, E_NOTICE, "%s", notice->message);
		}
		zend_hash_index_update(&MG(notices), (ulong)resource_id, (void **)&notice, sizeof(php_monetdb_notice *), NULL);
	}
}
/* }}} */
#endif

#define PHP_MONETDB_NOTICE_PTR_DTOR (void (*)(void *))_php_monetdb_notice_ptr_dtor

/* {{{ _php_monetdb_notice_dtor
 */
static void _php_monetdb_notice_ptr_dtor(void **ptr) 
{
	php_monetdb_notice *notice = (php_monetdb_notice *)*ptr;
	if (notice) {
 		efree(notice->message);
  		efree(notice);
  		notice = NULL;
  	}
}
/* }}} */

/* {{{ _php_monetdb_error_handler
 */
static void _php_monetdb_error_handler(void *resource_id, const char *message)
{
	php_monetdb_notice *notice;
	
	TSRMLS_FETCH();
	notice = (php_monetdb_notice *)emalloc(sizeof(php_monetdb_notice));
	notice->message = _php_monetdb_trim_message(message, &(notice->len));
	zend_hash_index_update(
			&MG(errors),
			(ulong)resource_id,
			(void **)&notice,
			sizeof(php_monetdb_notice *),
			NULL
		);
}
/* }}} */

#define PHP_MONETDB_ERROR_PTR_DTOR (void (*)(void *))_php_monetdb_notice_ptr_dtor

/* {{{ _rollback_transactions
 */
static int _rollback_transactions(zend_rsrc_list_entry *rsrc TSRMLS_DC)
{
	Mconn *link;
	Mresult *res;
	int orig;

	if (Z_TYPE_P(rsrc) != le_plink) 
		return 0;

	link = (Mconn *) rsrc->ptr;

	orig = MG(ignore_notices);
	MG(ignore_notices) = 1;

	if ((res = mapi_query(link, "START TRANSACTION;")) != NULL)
		mapi_close_handle(res);
	if ((res = mapi_query(link, "ROLLBACK;")) != NULL)
		mapi_close_handle(res);

	MG(ignore_notices) = orig;

	return 0;
}
/* }}} */

/* {{{ _free_ptr
 */
static void _free_ptr(zend_rsrc_list_entry *rsrc TSRMLS_DC)
{
	mLofp *lofp = (mLofp *)rsrc->ptr;
	efree(lofp);
}
/* }}} */

/* {{{ _free_result
 */
static void _free_result(zend_rsrc_list_entry *rsrc TSRMLS_DC)
{
	php_monetdb_result_handle *monetdb_result =
		(php_monetdb_result_handle *)rsrc->ptr;

	mapi_close_handle(monetdb_result->result);
	efree(monetdb_result);
}
/* }}} */

/* {{{ PHP_INI
 */
PHP_INI_BEGIN()
STD_PHP_INI_BOOLEAN( "monetdb.allow_persistent",      "1",  PHP_INI_SYSTEM, OnUpdateBool, allow_persistent,      zend_monetdb_globals, monetdb_globals)
STD_PHP_INI_ENTRY_EX("monetdb.max_persistent",       "-1",  PHP_INI_SYSTEM, OnUpdateLong, max_persistent,        zend_monetdb_globals, monetdb_globals, display_link_numbers)
STD_PHP_INI_ENTRY_EX("monetdb.max_links",            "-1",  PHP_INI_SYSTEM, OnUpdateLong, max_links,             zend_monetdb_globals, monetdb_globals, display_link_numbers)
STD_PHP_INI_BOOLEAN( "monetdb.auto_reset_persistent", "0",  PHP_INI_SYSTEM, OnUpdateBool, auto_reset_persistent, zend_monetdb_globals, monetdb_globals)
STD_PHP_INI_BOOLEAN( "monetdb.ignore_notice",         "0",  PHP_INI_ALL,    OnUpdateBool, ignore_notices,        zend_monetdb_globals, monetdb_globals)
STD_PHP_INI_BOOLEAN( "monetdb.log_notice",            "0",  PHP_INI_ALL,    OnUpdateBool, log_notices,           zend_monetdb_globals, monetdb_globals)
PHP_INI_END()
/* }}} */

/* {{{ php_monetdb_init_globals
 */
static void php_monetdb_init_globals(zend_monetdb_globals *monetdb_globals)
{
	memset(monetdb_globals, 0, sizeof(zend_monetdb_globals));
	/* Initialise notice message hash at MINIT only */
	zend_hash_init_ex(&monetdb_globals->notices, 0, NULL, PHP_MONETDB_NOTICE_PTR_DTOR, 1, 0); 
	zend_hash_init_ex(&monetdb_globals->errors, 0, NULL, PHP_MONETDB_ERROR_PTR_DTOR, 1, 0); 
	/* Initialise user, pass, lang, etc defaults */
	monetdb_globals->default_hostname = "localhost";
	monetdb_globals->default_username = "monetdb";
	monetdb_globals->default_password = "monetdb";
	monetdb_globals->default_language = "sql";
	monetdb_globals->default_port = 50000;
}
/* }}} */

/* {{{ PHP_MINIT_FUNCTION
 */
PHP_MINIT_FUNCTION(monetdb)
{
	ZEND_INIT_MODULE_GLOBALS(monetdb, php_monetdb_init_globals, NULL);

	REGISTER_INI_ENTRIES();
	
	le_link = zend_register_list_destructors_ex(_close_monetdb_link, NULL, "monetdb link", module_number);
	le_plink = zend_register_list_destructors_ex(NULL, _close_monetdb_plink, "monetdb link persistent", module_number);
	le_result = zend_register_list_destructors_ex(_free_result, NULL, "monetdb result", module_number);
	le_lofp = zend_register_list_destructors_ex(_free_ptr, NULL, "monetdb large object", module_number);
	le_string = zend_register_list_destructors_ex(_free_ptr, NULL, "monetdb string", module_number);
	/* For connection option */
	REGISTER_LONG_CONSTANT("MONETDB_CONNECT_FORCE_NEW", MONETDB_CONNECT_FORCE_NEW, CONST_CS | CONST_PERSISTENT);
	/* For pg_fetch_array() */
	REGISTER_LONG_CONSTANT("MONETDB_ASSOC", MONETDB_ASSOC, CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("MONETDB_NUM", MONETDB_NUM, CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("MONETDB_BOTH", MONETDB_BOTH, CONST_CS | CONST_PERSISTENT);
	/* For pg_connection_status() */
	REGISTER_LONG_CONSTANT("MONETDB_CONNECTION_BAD", MONETDB_CONNECTION_BAD, CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("MONETDB_CONNECTION_OK", MONETDB_CONNECTION_OK, CONST_CS | CONST_PERSISTENT);
#ifdef I_FEEL_LIKE_IMPLEMENTING_PREPARED_STUFF
	/* For monetdb_result_h() return value type */
	REGISTER_LONG_CONSTANT("MONETDB_STATUS_LONG", MONETDB_STATUS_LONG, CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("MONETDB_STATUS_STRING", MONETDB_STATUS_STRING, CONST_CS | CONST_PERSISTENT);
	/* For monetdb_result_h() return value */
	REGISTER_LONG_CONSTANT("MONETDB_EMPTY_QUERY", PGRES_EMPTY_QUERY, CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("MONETDB_COMMAND_OK", PGRES_COMMAND_OK, CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("MONETDB_TUPLES_OK", PGRES_TUPLES_OK, CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("MONETDB_COPY_OUT", PGRES_COPY_OUT, CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("MONETDB_COPY_IN", PGRES_COPY_IN, CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("MONETDB_BAD_RESPONSE", PGRES_BAD_RESPONSE, CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("MONETDB_NONFATAL_ERROR", PGRES_NONFATAL_ERROR, CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("MONETDB_FATAL_ERROR", PGRES_FATAL_ERROR, CONST_CS | CONST_PERSISTENT);
#endif /* I_FEEL_LIKE_IMPLEMENTING_PREPARED_STUFF */
	/* pg_convert options */
	REGISTER_LONG_CONSTANT("MONETDB_CONV_IGNORE_DEFAULT", MONETDB_CONV_IGNORE_DEFAULT, CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("MONETDB_CONV_FORCE_NULL", MONETDB_CONV_FORCE_NULL, CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("MONETDB_CONV_IGNORE_NOT_NULL", MONETDB_CONV_IGNORE_NOT_NULL, CONST_CS | CONST_PERSISTENT);
	/* pg_insert/update/delete/select options */
	REGISTER_LONG_CONSTANT("MONETDB_DML_NO_CONV", MONETDB_DML_NO_CONV, CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("MONETDB_DML_EXEC", MONETDB_DML_EXEC, CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("MONETDB_DML_ASYNC", MONETDB_DML_ASYNC, CONST_CS | CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("MONETDB_DML_STRING", MONETDB_DML_STRING, CONST_CS | CONST_PERSISTENT);
	return SUCCESS;
}
/* }}} */

/* {{{ PHP_MSHUTDOWN_FUNCTION
 */
PHP_MSHUTDOWN_FUNCTION(monetdb)
{
	UNREGISTER_INI_ENTRIES();
	zend_hash_destroy(&MG(notices));
	zend_hash_destroy(&MG(errors));

	return SUCCESS;
}
/* }}} */

/* {{{ PHP_RINIT_FUNCTION
 */
PHP_RINIT_FUNCTION(monetdb)
{
	MG(default_link)=-1;
	MG(num_links) = MG(num_persistent);
	return SUCCESS;
}
/* }}} */

/* {{{ PHP_RSHUTDOWN_FUNCTION
 */
PHP_RSHUTDOWN_FUNCTION(monetdb)
{
	/* clean up notice messages */
	zend_hash_clean(&MG(notices));
	zend_hash_clean(&MG(errors));
	/* clean up persistent connection */
	zend_hash_apply(&EG(persistent_list), (apply_func_t) _rollback_transactions TSRMLS_CC);
	return SUCCESS;
}
/* }}} */

/* {{{ PHP_MINFO_FUNCTION
 */
PHP_MINFO_FUNCTION(monetdb)
{
	char buf[256];
	/* make new Mapi struct with nonsense values in order to obtain the
	 * Mapi version */
	Mapi m = mapi_mapi("localhost", 0, "user", "pass", "lang", NULL);

	php_info_print_table_start();
	php_info_print_table_header(2, "MonetDB Support", "enabled");
	php_info_print_table_row(2, "Mapi Version", mapi_get_mapi_version(m));
#ifdef HAVE_OPENSSL
	php_info_print_table_row(2, "SSL support", "enabled");
#else
	php_info_print_table_row(2, "SSL support", "disabled");
#endif
	sprintf(buf, "%ld", MG(num_persistent));
	php_info_print_table_row(2, "Active Persistent Links", buf);
	sprintf(buf, "%ld", MG(num_links));
	php_info_print_table_row(2, "Active Links", buf);
	php_info_print_table_end();

	mapi_destroy(m);
	DISPLAY_INI_ENTRIES();
}
/* }}} */

/* {{{ php_monetdb_do_connect
 */
static void php_monetdb_do_connect(INTERNAL_FUNCTION_PARAMETERS, int persistent)
{
	char *hostname = MG(default_hostname);
	char *username = MG(default_username);
	char *password = MG(default_password);
	char *language = MG(default_language);
	char *database = NULL;
	int port = MG(default_port);

	zval **args[5];
	int argc, i, connect_type = 0;
	smart_str str = {0};

	Mconn *monetdb;

	argc = ZEND_NUM_ARGS();
	if (argc > 7 ||
		zend_get_parameters_array_ex(argc, args) != SUCCESS)
		WRONG_PARAM_COUNT;

	switch (argc) {
		case 7:		/* connect type */
			convert_to_long_ex(args[6]);
			connect_type = Z_LVAL_PP(args[6]);
			/* fall through */
		case 6:		/* database */
			convert_to_string_ex(args[5]);
			database = Z_STRVAL_PP(args[5]);
			/* fall through */
		case 5:		/* password */
			convert_to_string_ex(args[4]);
			password = Z_STRVAL_PP(args[4]);
			/* fall through */
		case 4:		/* username */
			convert_to_string_ex(args[3]);
			username = Z_STRVAL_PP(args[3]);
			/* fall through */
		case 3:		/* port */
			convert_to_long_ex(args[2]);
			port = Z_LVAL_PP(args[2]);
			/* fall through */
		case 2:		/* hostname */
			convert_to_string_ex(args[1]);
			hostname = Z_STRVAL_PP(args[1]);
			/* fall through */
		case 1:		/* language */
			convert_to_string_ex(args[0]);
			language = Z_STRVAL_PP(args[0]);
		break;
	}

	smart_str_appends(&str, "monetdb");
	
	for (i = 0; i < ZEND_NUM_ARGS(); i++) {
		convert_to_string_ex(args[i]);
		smart_str_appendc(&str, '_');
		smart_str_appendl(&str, Z_STRVAL_PP(args[i]), Z_STRLEN_PP(args[i]));
	}

	smart_str_0(&str);

	/* only allow persistant links if the global configuration allows it */
	if (persistent && MG(allow_persistent)) {
		zend_rsrc_list_entry *le;
		
		/* try to find if we already have this link in our persistent list */
		if (zend_hash_find(&EG(persistent_list), str.c, str.len+1, (void **) &le)==FAILURE) {  /* we don't */
			zend_rsrc_list_entry new_le;
			
			if (MG(max_links)!=-1 && MG(num_links)>=MG(max_links)) {
				php_error_docref(NULL TSRMLS_CC, E_WARNING,
								 "Cannot create new link. Too many open links (%ld)", MG(num_links));
				goto err;
			}
			if (MG(max_persistent)!=-1 && MG(num_persistent)>=MG(max_persistent)) {
				php_error_docref(NULL TSRMLS_CC, E_WARNING,
								 "Cannot create new link. Too many open persistent links (%ld)", MG(num_persistent));
				goto err;
			}

			/* create the link */
			monetdb = mapi_connect(hostname, port, username, password, language, database);
			if (mapi_error(monetdb) != 0) {
				PHP_MONETDB_ERROR("Unable to connect to MonetDB server: %s", monetdb);
				mapi_disconnect(monetdb);
				goto err;
			}
			/* in XQuery mode, we request "tabular" seq mode */
			if (strcmp(language, "xquery") == 0) {
				mapi_output(monetdb, "seq");
			}

			/* hash it up */
			Z_TYPE(new_le) = le_plink;
			new_le.ptr = monetdb;
			if (zend_hash_update(&EG(persistent_list), str.c, str.len+1, (void *) &new_le, sizeof(zend_rsrc_list_entry), NULL)==FAILURE) {
				goto err;
			}
			MG(num_links)++;
			MG(num_persistent)++;
		} else {  /* we do already have a persistent link */
			if (Z_TYPE_P(le) != le_plink) {
				RETURN_FALSE;
			}
			/* ensure that the link did not die */
			if (MG(auto_reset_persistent) & 1) {
				/* need to send & get something from backend to
				   make sure we catch MONETDB_CONNECTION_BAD everytime */
				Mresult *monetdb_result;
				monetdb_result = mapi_query(le->ptr, "select 1;");
				if (monetdb_result != NULL)
					mapi_close_handle(monetdb_result);
			}
			if (mapi_is_connected(le->ptr) == 0) { /* the link died */
				mapi_reconnect(le->ptr);
				if (mapi_is_connected(le->ptr) == 0) {
					php_error_docref(NULL TSRMLS_CC, E_WARNING,"MonetDB link lost, unable to reconnect");
					zend_hash_del(&EG(persistent_list),str.c,str.len+1);
					mapi_disconnect(le->ptr);
					goto err;
				}
			}
			monetdb = (Mconn *) le->ptr;
		}
		ZEND_REGISTER_RESOURCE(return_value, monetdb, le_plink);
	} else { /* non persistent connection */
		zend_rsrc_list_entry *index_ptr,new_index_ptr;
		
		/* first we check the hash for the hashed_details key.  if it
		 * exists, it should point us to the right offset where the
		 * actual monetdb link sits.  If it doesn't, open a new monetdb
		 * link, add it to the resource list, and add a pointer to it
		 * with hashed_details as the key.
		 */
		if (!(connect_type & MONETDB_CONNECT_FORCE_NEW)
			&& zend_hash_find(&EG(regular_list),str.c,str.len+1,(void **) &index_ptr)==SUCCESS) {
			int type, link;
			void *ptr;

			if (Z_TYPE_P(index_ptr) != le_index_ptr) {
				RETURN_FALSE;
			}
			link = (int)(size_t) index_ptr->ptr;
			ptr = zend_list_find(link,&type);   /* check if the link is still there */
			if (ptr && (type==le_link || type==le_plink)) {
				Z_LVAL_P(return_value) = link;
				zend_list_addref(link);
				php_monetdb_set_default_link(link TSRMLS_CC);
				Z_TYPE_P(return_value) = IS_RESOURCE;
				goto cleanup;
			} else {
				zend_hash_del(&EG(regular_list),str.c,str.len+1);
			}
		}
		if (MG(max_links)!=-1 && MG(num_links)>=MG(max_links)) {
			php_error_docref(NULL TSRMLS_CC, E_WARNING, "Cannot create new link. Too many open links (%ld)", MG(num_links));
			goto err;
		}
		monetdb = mapi_connect(hostname, port, username, password, language, database);
		if (mapi_error(monetdb) != 0) {
			PHP_MONETDB_ERROR("Unable to connect to MonetDB server: %s", monetdb);
			mapi_disconnect(monetdb);
			goto err;
		}

		/* add it to the list */
		ZEND_REGISTER_RESOURCE(return_value, monetdb, le_link);

		/* add it to the hash */
		new_index_ptr.ptr = (void *) Z_LVAL_P(return_value);
		Z_TYPE(new_index_ptr) = le_index_ptr;
		if (zend_hash_update(&EG(regular_list),str.c,str.len+1,(void *) &new_index_ptr, sizeof(zend_rsrc_list_entry), NULL)==FAILURE) {
			goto err;
		}
		MG(num_links)++;
	}
	/* set notice processer: TODO, we don't have notices yet
	if (! MG(ignore_notices) && Z_TYPE_P(return_value) == IS_RESOURCE) {
		PQsetNoticeProcessor(monetdb, _php_monetdb_notice_handler, (void*)Z_RESVAL_P(return_value));
	}
	*/
	php_monetdb_set_default_link(Z_LVAL_P(return_value) TSRMLS_CC);
	
cleanup:
	smart_str_free(&str);
	return;
	
err:
	smart_str_free(&str);
	RETURN_FALSE;
}
/* }}} */

/* {{{ proto resource monetdb_connect([string language [, string host [, string port [, string username [, string port [, string database [, int connect_type]]]]]]])
   Open a MonetDB connection */
PHP_FUNCTION(monetdb_connect)
{
	php_monetdb_do_connect(INTERNAL_FUNCTION_PARAM_PASSTHRU,0);
}
/* }}} */

/* {{{ proto resource monetdb_pconnect([string language [, string host [, string port [, string username [, string port [, string database [, int connect_type]]]]]]])
   Open a persistent MonetDB connection */
PHP_FUNCTION(monetdb_pconnect)
{
	php_monetdb_do_connect(INTERNAL_FUNCTION_PARAM_PASSTHRU,1);
}
/* }}} */

/* {{{ proto bool monetdb_close([resource connection])
   Close a PostgreSQL connection */ 
PHP_FUNCTION(monetdb_close)
{
	zval **monetdb_link = NULL;
	int id;
	Mconn *monetdb;
	
	switch (ZEND_NUM_ARGS()) {
		case 0:
			id = MG(default_link);
			CHECK_DEFAULT_LINK(id);
			break;
		case 1:
			if (zend_get_parameters_ex(1, &monetdb_link)==FAILURE) {
				RETURN_FALSE;
			}
			id = -1;
			break;
		default:
			WRONG_PARAM_COUNT;
			break;
	}
	if (monetdb_link == NULL && id == -1) {
		RETURN_FALSE;
	}	

	ZEND_FETCH_RESOURCE2(monetdb, Mconn *, monetdb_link, id, "MonetDB link", le_link, le_plink);

	if (id==-1) { /* explicit resource number */
		zend_list_delete(Z_RESVAL_PP(monetdb_link));
	}

	if (id!=-1 
		|| (monetdb_link && Z_RESVAL_PP(monetdb_link)==MG(default_link))) {
		zend_list_delete(MG(default_link));
		MG(default_link) = -1;
	}

	RETURN_TRUE;
}
/* }}} */

#define PHP_MONETDB_DBNAME 1
#define PHP_MONETDB_ERROR_MESSAGE 2
#define PHP_MONETDB_HOST 6
#define PHP_MONETDB_VERSION 7

/* {{{ php_monetdb_get_link_info
 */
static void php_monetdb_get_link_info(INTERNAL_FUNCTION_PARAMETERS, int entry_type)
{
	zval **monetdb_link = NULL;
	int id = -1;
	Mconn *monetdb;

	switch(ZEND_NUM_ARGS()) {
		case 0:
			id = MG(default_link);
			CHECK_DEFAULT_LINK(id);
		break;
		case 1:
			if (zend_get_parameters_ex(1, &monetdb_link)==FAILURE) {
				RETURN_FALSE;
			}
		break;
		default:
			WRONG_PARAM_COUNT;
		break;
	}
	if (monetdb_link == NULL && id == -1) {
		RETURN_FALSE;
	}

	ZEND_FETCH_RESOURCE2(monetdb, Mconn *, monetdb_link, id, "MonetDB link", le_link, le_plink);

	switch(entry_type) {
		case PHP_MONETDB_DBNAME:
			Z_STRVAL_P(return_value) = mapi_get_dbname(monetdb);
		break;
		case PHP_MONETDB_ERROR_MESSAGE: {
			php_monetdb_notice** error;

			if (zend_hash_index_find(&MG(errors), (ulong)monetdb, (void **)&error) == FAILURE) {
				RETURN_FALSE;
			}
			if ((*error)->message == NULL) {
				RETURN_STRING(estrdup("(no error message)"), 1);
			} else {
				RETURN_STRINGL((*error)->message, (*error)->len, 1);
			}
		} return;
		case PHP_MONETDB_HOST:
			Z_STRVAL_P(return_value) = mapi_get_host(monetdb);
		break;
		case PHP_MONETDB_VERSION:
			array_init(return_value);
			add_assoc_string(return_value, "mapi", mapi_get_mapi_version(monetdb), 1);
			add_assoc_string(return_value, "mserver", mapi_get_monet_version(monetdb), 1);
			return;
		default:
			RETURN_FALSE;
	}
	if (Z_STRVAL_P(return_value)) {
		Z_STRLEN_P(return_value) = strlen(Z_STRVAL_P(return_value));
		Z_STRVAL_P(return_value) = (char *) estrdup(Z_STRVAL_P(return_value));
	} else {
		Z_STRLEN_P(return_value) = 0;
		Z_STRVAL_P(return_value) = (char *) estrdup("");
	}
	Z_TYPE_P(return_value) = IS_STRING;
}
/* }}} */

/* {{{ proto string monetdb_dbname([resource connection])
   Get the database name */ 
PHP_FUNCTION(monetdb_dbname)
{
	php_monetdb_get_link_info(INTERNAL_FUNCTION_PARAM_PASSTHRU,PHP_MONETDB_DBNAME);
}
/* }}} */

/* {{{ proto string monetdb_last_error([resource connection])
   Get the error message string */
PHP_FUNCTION(monetdb_last_error)
{
	php_monetdb_get_link_info(INTERNAL_FUNCTION_PARAM_PASSTHRU,PHP_MONETDB_ERROR_MESSAGE);
}
/* }}} */

/* {{{ proto string monetdb_host([resource connection])
   Returns the host name associated with the connection */
PHP_FUNCTION(monetdb_host)
{
	php_monetdb_get_link_info(INTERNAL_FUNCTION_PARAM_PASSTHRU,PHP_MONETDB_HOST);
}
/* }}} */

/* {{{ proto array monetdb_version([resource connection])
   Returns an array with client, protocol and server version (when available) */
PHP_FUNCTION(monetdb_version)
{
	php_monetdb_get_link_info(INTERNAL_FUNCTION_PARAM_PASSTHRU,PHP_MONETDB_VERSION);
}
/* }}} */

/* {{{ proto bool monetdb_ping([resource connection])
   Ping database. If connection is bad, try to reconnect. */
PHP_FUNCTION(monetdb_ping)
{
	zval *monetdb_link;
	int id;
	Mconn *monetdb;

	if (zend_parse_parameters_ex(ZEND_PARSE_PARAMS_QUIET, ZEND_NUM_ARGS() TSRMLS_CC, "r", &monetdb_link) == SUCCESS) {
		id = -1;
	} else {
		monetdb_link = NULL;
		id = MG(default_link);
	}
	if (monetdb_link == NULL && id == -1) {
		RETURN_FALSE;
	}

	ZEND_FETCH_RESOURCE2(monetdb, Mconn *, &monetdb_link, id, "MonetDB link", le_link, le_plink);

	/* ping connection and see if the connection is ok */
	if (mapi_ping(monetdb) == 0)
		RETURN_TRUE;

	/* reset connection if it's broken */
	if (mapi_reconnect(monetdb) == MOK) {
		RETURN_TRUE;
	}
	RETURN_FALSE;
}
/* }}} */

/* {{{ proto resource monetdb_query([resource connection,] string query)
   Execute a query */
PHP_FUNCTION(monetdb_query)
{
	zval **query, **monetdb_link = NULL;
	int id = -1;
	char *myq;
	Mconn *monetdb;
	Mresult *monetdb_result;
	php_monetdb_result_handle *monetdb_result_h;

	switch(ZEND_NUM_ARGS()) {
		case 1:
			if (zend_get_parameters_ex(1, &query)==FAILURE) {
				RETURN_FALSE;
			}
			id = MG(default_link);
			CHECK_DEFAULT_LINK(id);
			break;
		case 2:
			if (zend_get_parameters_ex(2, &monetdb_link, &query)==FAILURE) {
				RETURN_FALSE;
			}
			break;
		default:
			WRONG_PARAM_COUNT;
			break;
	}
	if (monetdb_link == NULL && id == -1) {
		RETURN_FALSE;
	}	

	ZEND_FETCH_RESOURCE2(monetdb, Mconn *, monetdb_link, id, "MonetDB link", le_link, le_plink);

	/* append ; to the query so it is terminated in those cases where
	 * the user forgot it */
	convert_to_string_ex(query);
	myq = alloca(sizeof(char) * (strlen(Z_STRVAL_PP(query)) + 2));
	memcpy(myq, Z_STRVAL_PP(query), strlen(Z_STRVAL_PP(query)) + 1);
	strcat(myq, ";");
	monetdb_result = mapi_query(monetdb, myq);
	if ((MG(auto_reset_persistent) & 2) && monetdb_result == NULL) {
		mapi_reconnect(monetdb);
		monetdb_result = mapi_query(monetdb, myq);
	}

	if (monetdb_result == NULL) {
		/* connection appears to be dead */
		PHP_MONETDB_ERROR("Connection appears to be lost: %s", monetdb);
		RETURN_FALSE;
	} else {
		monetdb_result_h = (php_monetdb_result_handle *) emalloc(sizeof(php_monetdb_result_handle));
		monetdb_result_h->conn = monetdb;
		monetdb_result_h->result = monetdb_result;
		monetdb_result_h->row = 0;
		if (mapi_result_error(monetdb_result) != NULL) {
			/* something went wrong */
			PHP_MONETDB_ERROR_RESULT("Query failed: %s", monetdb_result_h);
			mapi_close_handle(monetdb_result);
			efree(monetdb_result_h);
			RETURN_FALSE;
		}
		ZEND_REGISTER_RESOURCE(return_value, monetdb_result_h, le_result);
	}
}
/* }}} */

#ifdef I_FEEL_LIKE_IMPLEMENTING_PREPARED_STUFF
/* {{{ _php_monetdb_free_params */
static void _php_monetdb_free_params(char **params, int num_params)
{
	if (num_params > 0) {
		efree(params);
	}
}
/* }}} */

/* {{{ proto resource monetdb_query_params([resource connection,] string query, array params)
   Execute a query */
PHP_FUNCTION(monetdb_query_params)
{
	zval **query, **monetdb_link = NULL;
	zval **pv_param_arr, **tmp;
	int id = -1;
	int leftover = 0;
	int num_params = 0;
	char **params = NULL;
	unsigned char otype;
	Mconn *monetdb;
	Mresult *monetdb_result;
	ExecStatusType status;
	php_monetdb_result_handle *monetdb_result_h;

	switch(ZEND_NUM_ARGS()) {
		case 2:
			if (zend_get_parameters_ex(2, &query, &pv_param_arr)==FAILURE) {
				RETURN_FALSE;
			}
			id = MG(default_link);
			CHECK_DEFAULT_LINK(id);
			break;
		case 3:
			if (zend_get_parameters_ex(3, &monetdb_link, &query, &pv_param_arr)==FAILURE) {
				RETURN_FALSE;
			}
			break;
		default:
			WRONG_PARAM_COUNT;
			break;
	}
	if (monetdb_link == NULL && id == -1) {
		RETURN_FALSE;
	}	

	if (Z_TYPE_PP(pv_param_arr) != IS_ARRAY) {
		php_error_docref(NULL TSRMLS_CC, E_WARNING, "No array passed");
		RETURN_FALSE;
	}

	ZEND_FETCH_RESOURCE2(monetdb, Mconn *, monetdb_link, id, "PostgreSQL link", le_link, le_plink);

	convert_to_string_ex(query);
	while ((monetdb_result = MgetResult(monetdb))) {
		Mclear(monetdb_result);
		leftover = 1;
	}
	if (leftover) {
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Found results on this connection. Use pg_get_result() to get these results first");
	}

	zend_hash_internal_pointer_reset(Z_ARRVAL_PP(pv_param_arr));
	num_params = zend_hash_num_elements(Z_ARRVAL_PP(pv_param_arr));
	if (num_params > 0) {
		int i = 0;
		params = (char **)safe_emalloc(sizeof(char *), num_params, 0);
		
		for(i = 0; i < num_params; i++) {
			if (zend_hash_get_current_data(Z_ARRVAL_PP(pv_param_arr), (void **) &tmp) == FAILURE) {
				php_error_docref(NULL TSRMLS_CC, E_WARNING,"Error getting parameter");
				_php_monetdb_free_params(params, num_params);
				RETURN_FALSE;
			}

			otype = (*tmp)->type;
			convert_to_string_ex(tmp);
			if (Z_TYPE_PP(tmp) != IS_STRING) {
				php_error_docref(NULL TSRMLS_CC, E_WARNING,"Error converting parameter");
				_php_monetdb_free_params(params, num_params);
				RETURN_FALSE;
			}

			if (otype == IS_NULL) {
				params[i] = NULL;
			}
			else {
				params[i] = Z_STRVAL_PP(tmp);
			}

			zend_hash_move_forward(Z_ARRVAL_PP(pv_param_arr));
		}
	}

	monetdb_result = PQexecParams(monetdb, Z_STRVAL_PP(query), num_params, 
					NULL, (const char * const *)params, NULL, NULL, 0);
	if ((MG(auto_reset_persistent) & 2) && PQstatus(monetdb) != MONETDB_CONNECTION_OK) {
		Mclear(monetdb_result);
		PQreset(monetdb);
		monetdb_result = PQexecParams(monetdb, Z_STRVAL_PP(query), num_params, 
						NULL, (const char * const *)params, NULL, NULL, 0);
	}

	if (monetdb_result) {
		status = PQresultStatus(monetdb_result);
	} else {
		status = (ExecStatusType) PQstatus(monetdb);
	}
	
	_php_monetdb_free_params(params, num_params);

	switch (status) {
		case PGRES_EMPTY_QUERY:
		case PGRES_BAD_RESPONSE:
		case PGRES_NONFATAL_ERROR:
		case PGRES_FATAL_ERROR:
			PHP_PQ_ERROR("Query failed: %s", monetdb);
			Mclear(monetdb_result);
			RETURN_FALSE;
			break;
		case PGRES_COMMAND_OK: /* successful command that did not return rows */
		default:
			if (monetdb_result) {
				monetdb_result_h = (php_monetdb_result_handle *) emalloc(sizeof(php_monetdb_result_handle));
				monetdb_result_h->conn = monetdb;
				monetdb_result_h->result = monetdb_result;
				monetdb_result_h->row = 0;
				ZEND_REGISTER_RESOURCE(return_value, monetdb_result_h, le_result);
			} else {
				Mclear(monetdb_result);
				RETURN_FALSE;
			}
			break;
	}
}
/* }}} */

/* {{{ proto resource monetdb_prepare([resource connection,] string stmtname, string query)
   Prepare a query for future execution */
PHP_FUNCTION(monetdb_prepare)
{
	zval **query, **stmtname, **monetdb_link = NULL;
	int id = -1;
	int leftover = 0;
	Mconn *monetdb;
	Mresult *monetdb_result;
	ExecStatusType status;
	php_monetdb_result_handle *monetdb_result_h;

	switch(ZEND_NUM_ARGS()) {
		case 2:
			if (zend_get_parameters_ex(2, &stmtname, &query)==FAILURE) {
				RETURN_FALSE;
			}
			id = MG(default_link);
			CHECK_DEFAULT_LINK(id);
			break;
		case 3:
			if (zend_get_parameters_ex(3, &monetdb_link, &stmtname, &query)==FAILURE) {
				RETURN_FALSE;
			}
			break;
		default:
			WRONG_PARAM_COUNT;
			break;
	}
	if (monetdb_link == NULL && id == -1) {
		RETURN_FALSE;
	}	

	ZEND_FETCH_RESOURCE2(monetdb, Mconn *, monetdb_link, id, "PostgreSQL link", le_link, le_plink);

	convert_to_string_ex(stmtname);
	convert_to_string_ex(query);
	while ((monetdb_result = MgetResult(monetdb))) {
		Mclear(monetdb_result);
		leftover = 1;
	}
	if (leftover) {
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Found results on this connection. Use pg_get_result() to get these results first");
	}
	monetdb_result = PQprepare(monetdb, Z_STRVAL_PP(stmtname), Z_STRVAL_PP(query), 0, NULL);
	if ((MG(auto_reset_persistent) & 2) && PQstatus(monetdb) != MONETDB_CONNECTION_OK) {
		Mclear(monetdb_result);
		PQreset(monetdb);
		monetdb_result = PQprepare(monetdb, Z_STRVAL_PP(stmtname), Z_STRVAL_PP(query), 0, NULL);
	}

	if (monetdb_result) {
		status = PQresultStatus(monetdb_result);
	} else {
		status = (ExecStatusType) PQstatus(monetdb);
	}
	
	switch (status) {
		case PGRES_EMPTY_QUERY:
		case PGRES_BAD_RESPONSE:
		case PGRES_NONFATAL_ERROR:
		case PGRES_FATAL_ERROR:
			PHP_PQ_ERROR("Query failed: %s", monetdb);
			Mclear(monetdb_result);
			RETURN_FALSE;
			break;
		case PGRES_COMMAND_OK: /* successful command that did not return rows */
		default:
			if (monetdb_result) {
				monetdb_result_h = (php_monetdb_result_handle *) emalloc(sizeof(php_monetdb_result_handle));
				monetdb_result_h->conn = monetdb;
				monetdb_result_h->result = monetdb_result;
				monetdb_result_h->row = 0;
				ZEND_REGISTER_RESOURCE(return_value, monetdb_result_h, le_result);
			} else {
				Mclear(monetdb_result);
				RETURN_FALSE;
			}
			break;
	}
}
/* }}} */

/* {{{ proto resource monetdb_execute([resource connection,] string stmtname, array params)
   Execute a prepared query  */
PHP_FUNCTION(monetdb_execute)
{
	zval **stmtname, **monetdb_link = NULL;
	zval **pv_param_arr, **tmp;
	int id = -1;
	int leftover = 0;
	int num_params = 0;
	char **params = NULL;
	unsigned char otype;
	Mconn *monetdb;
	Mresult *monetdb_result;
	ExecStatusType status;
	php_monetdb_result_handle *monetdb_result_h;

	switch(ZEND_NUM_ARGS()) {
		case 2:
			if (zend_get_parameters_ex(2, &stmtname, &pv_param_arr)==FAILURE) {
				RETURN_FALSE;
			}
			id = MG(default_link);
			CHECK_DEFAULT_LINK(id);
			break;
		case 3:
			if (zend_get_parameters_ex(3, &monetdb_link, &stmtname, &pv_param_arr)==FAILURE) {
				RETURN_FALSE;
			}
			break;
		default:
			WRONG_PARAM_COUNT;
			break;
	}
	if (monetdb_link == NULL && id == -1) {
		RETURN_FALSE;
	}	

	if (Z_TYPE_PP(pv_param_arr) != IS_ARRAY) {
		php_error_docref(NULL TSRMLS_CC, E_WARNING, "No array passed");
		RETURN_FALSE;
	}

	ZEND_FETCH_RESOURCE2(monetdb, Mconn *, monetdb_link, id, "PostgreSQL link", le_link, le_plink);

	convert_to_string_ex(stmtname);
	while ((monetdb_result = MgetResult(monetdb))) {
		Mclear(monetdb_result);
		leftover = 1;
	}
	if (leftover) {
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Found results on this connection. Use pg_get_result() to get these results first");
	}

	zend_hash_internal_pointer_reset(Z_ARRVAL_PP(pv_param_arr));
	num_params = zend_hash_num_elements(Z_ARRVAL_PP(pv_param_arr));
	if (num_params > 0) {
		int i = 0;
		params = (char **)safe_emalloc(sizeof(char *), num_params, 0);
		
		for(i = 0; i < num_params; i++) {
			if (zend_hash_get_current_data(Z_ARRVAL_PP(pv_param_arr), (void **) &tmp) == FAILURE) {
				php_error_docref(NULL TSRMLS_CC, E_WARNING,"Error getting parameter");
				_php_monetdb_free_params(params, num_params);
				RETURN_FALSE;
			}

			otype = (*tmp)->type;
			convert_to_string(*tmp);
			if (Z_TYPE_PP(tmp) != IS_STRING) {
				php_error_docref(NULL TSRMLS_CC, E_WARNING,"Error converting parameter");
				_php_monetdb_free_params(params, num_params);
				RETURN_FALSE;
			}

			if (otype == IS_NULL) {
				params[i] = NULL;
			}
			else {
				params[i] = Z_STRVAL_PP(tmp);
			}

			zend_hash_move_forward(Z_ARRVAL_PP(pv_param_arr));
		}
	}

	monetdb_result = PQexecPrepared(monetdb, Z_STRVAL_PP(stmtname), num_params, 
					(const char * const *)params, NULL, NULL, 0);
	if ((MG(auto_reset_persistent) & 2) && PQstatus(monetdb) != MONETDB_CONNECTION_OK) {
		Mclear(monetdb_result);
		PQreset(monetdb);
		monetdb_result = PQexecPrepared(monetdb, Z_STRVAL_PP(stmtname), num_params, 
						(const char * const *)params, NULL, NULL, 0);
	}

	if (monetdb_result) {
		status = PQresultStatus(monetdb_result);
	} else {
		status = (ExecStatusType) PQstatus(monetdb);
	}
	
	_php_monetdb_free_params(params, num_params);

	switch (status) {
		case PGRES_EMPTY_QUERY:
		case PGRES_BAD_RESPONSE:
		case PGRES_NONFATAL_ERROR:
		case PGRES_FATAL_ERROR:
			Mclear(monetdb_result);
			PHP_PQ_ERROR("Query failed: %s", monetdb);
			RETURN_FALSE;
			break;
		case PGRES_COMMAND_OK: /* successful command that did not return rows */
		default:
			if (monetdb_result) {
				monetdb_result_h = (php_monetdb_result_handle *) emalloc(sizeof(php_monetdb_result_handle));
				monetdb_result_h->conn = monetdb;
				monetdb_result_h->result = monetdb_result;
				monetdb_result_h->row = 0;
				ZEND_REGISTER_RESOURCE(return_value, monetdb_result_h, le_result);
			} else {
				Mclear(monetdb_result);
				RETURN_FALSE;
			}
			break;
	}
}
/* }}} */
#endif

#define PHP_MONETDB_NUM_ROWS 1
#define PHP_MONETDB_NUM_FIELDS 2
#define PHP_MONETDB_CMD_TUPLES 3

/* {{{ php_monetdb_get_result_info
 */
static void php_monetdb_get_result_info(INTERNAL_FUNCTION_PARAMETERS, int entry_type)
{
	zval **result;
	Mresult *monetdb_result;
	php_monetdb_result_handle *monetdb_result_h;

	if (ZEND_NUM_ARGS() != 1 || zend_get_parameters_ex(1, &result)==FAILURE) {
		WRONG_PARAM_COUNT;
	}
	
	ZEND_FETCH_RESOURCE(monetdb_result_h, php_monetdb_result_handle *, result, -1, "MonetDB result", le_result);

	monetdb_result = monetdb_result_h->result;

	switch (entry_type) {
		case PHP_MONETDB_NUM_ROWS:
			Z_LVAL_P(return_value) = mapi_get_row_count(monetdb_result);
			break;
		case PHP_MONETDB_NUM_FIELDS:
			Z_LVAL_P(return_value) = mapi_get_field_count(monetdb_result);
			break;
		case PHP_MONETDB_CMD_TUPLES:
			Z_LVAL_P(return_value) = mapi_rows_affected(monetdb_result);
			break;
		default:
			RETURN_FALSE;
	}
	Z_TYPE_P(return_value) = IS_LONG;
}
/* }}} */

/* {{{ proto int monetdb_num_rows(resource result)
   Return the number of rows in the result */
PHP_FUNCTION(monetdb_num_rows)
{
	php_monetdb_get_result_info(INTERNAL_FUNCTION_PARAM_PASSTHRU,PHP_MONETDB_NUM_ROWS);
}
/* }}} */

/* {{{ proto int monetdb_num_fields(resource result)
   Return the number of fields in the result */
PHP_FUNCTION(monetdb_num_fields)
{
	php_monetdb_get_result_info(INTERNAL_FUNCTION_PARAM_PASSTHRU,PHP_MONETDB_NUM_FIELDS);
}
/* }}} */

/* {{{ proto int monetdb_affected_rows(resource result)
   Returns the number of affected tuples */
PHP_FUNCTION(monetdb_affected_rows)
{
	php_monetdb_get_result_info(INTERNAL_FUNCTION_PARAM_PASSTHRU,PHP_MONETDB_CMD_TUPLES);
}
/* }}} */

/* {{{ proto string pg_last_notice(resource connection)
   Returns the last notice set by the backend */
PHP_FUNCTION(monetdb_last_notice) 
{
	zval *monetdb_link;
	Mconn *monet_conn;
	int id = -1;
	php_monetdb_notice **notice;
	
	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r",
							  &monetdb_link) == FAILURE) {
		return;
	}
	/* Just to check if user passed valid resource */
	ZEND_FETCH_RESOURCE2(monet_conn, Mconn *, &monetdb_link, id, "MonetDB link", le_link, le_plink);

	if (zend_hash_index_find(&MG(notices), Z_RESVAL_P(monetdb_link), (void **)&notice) == FAILURE) {
		RETURN_FALSE;
	}
	RETURN_STRINGL((*notice)->message, (*notice)->len, 1);
}
/* }}} */

#define PHP_MONETDB_FIELD_NAME 1
#define PHP_MONETDB_FIELD_TABLE 2
#define PHP_MONETDB_FIELD_TYPE 3

/* {{{ php_monetdb_get_field_info
 */
static void php_monetdb_get_field_info(INTERNAL_FUNCTION_PARAMETERS, int entry_type)
{
	zval **result, **field;
	Mresult *monetdb_result;
	php_monetdb_result_handle *monetdb_result_h;
	
	if (ZEND_NUM_ARGS() != 2 || zend_get_parameters_ex(2, &result, &field)==FAILURE) {
		WRONG_PARAM_COUNT;
	}
	
	ZEND_FETCH_RESOURCE(monetdb_result_h, php_monetdb_result_handle *, result, -1, "MonetDB result", le_result);

	monetdb_result = monetdb_result_h->result;
	convert_to_long_ex(field);
	
	if (Z_LVAL_PP(field) < 0 || Z_LVAL_PP(field) >= mapi_get_field_count(monetdb_result)) {
		php_error_docref(NULL TSRMLS_CC, E_WARNING, "Bad field offset specified");
		RETURN_FALSE;
	}
	
	switch (entry_type) {
		case PHP_MONETDB_FIELD_NAME:
			Z_STRVAL_P(return_value) = mapi_get_name(monetdb_result, Z_LVAL_PP(field));
			Z_STRLEN_P(return_value) = strlen(Z_STRVAL_P(return_value));
			Z_STRVAL_P(return_value) = estrndup(Z_STRVAL_P(return_value),Z_STRLEN_P(return_value));
			Z_TYPE_P(return_value) = IS_STRING;
		break;
		case PHP_MONETDB_FIELD_TABLE:
			Z_STRVAL_P(return_value) = mapi_get_table(monetdb_result, Z_LVAL_PP(field));
			Z_STRLEN_P(return_value) = strlen(Z_STRVAL_P(return_value));
			Z_STRVAL_P(return_value) = estrndup(Z_STRVAL_P(return_value),Z_STRLEN_P(return_value));
			Z_TYPE_P(return_value) = IS_STRING;
		break;
		case PHP_MONETDB_FIELD_TYPE:
			Z_STRVAL_P(return_value) = mapi_get_type(monetdb_result, Z_LVAL_PP(field));
			Z_STRLEN_P(return_value) = strlen(Z_STRVAL_P(return_value));
			Z_STRVAL_P(return_value) = estrndup(Z_STRVAL_P(return_value),Z_STRLEN_P(return_value));
			Z_TYPE_P(return_value) = IS_STRING;
		break;
		default:
			RETURN_FALSE;
	}
}
/* }}} */

/* {{{ proto string monetdb_field_name(resource result, int field_number)
   Returns the name of the field */
PHP_FUNCTION(monetdb_field_name)
{
	php_monetdb_get_field_info(INTERNAL_FUNCTION_PARAM_PASSTHRU,PHP_MONETDB_FIELD_NAME);
}
/* }}} */

/* {{{ proto string monetdb_field_table(resource result, int field_number)
   Returns the name of the table field belongs to */
PHP_FUNCTION(monetdb_field_table)
{
	php_monetdb_get_field_info(INTERNAL_FUNCTION_PARAM_PASSTHRU,PHP_MONETDB_FIELD_TABLE);
}
/* }}} */

/* {{{ proto string monetdb_field_type(resource result, int field_number)
   Returns the type of the field */
PHP_FUNCTION(monetdb_field_type)
{
	php_monetdb_get_field_info(INTERNAL_FUNCTION_PARAM_PASSTHRU,PHP_MONETDB_FIELD_TYPE);
}
/* }}} */

/* {{{ proto int monetdb_field_num(resource result, string field_name)
   Returns the field number of the named field */
PHP_FUNCTION(monetdb_field_num)
{
	zval **result, **field;
	Mresult *monetdb_result;
	php_monetdb_result_handle *monetdb_result_h;
	int i;

	if (ZEND_NUM_ARGS() != 2 || zend_get_parameters_ex(2, &result, &field)==FAILURE) {
		WRONG_PARAM_COUNT;
	}
	
	ZEND_FETCH_RESOURCE(monetdb_result_h, php_monetdb_result_handle *, result, -1, "MonetDB result", le_result);

	monetdb_result = monetdb_result_h->result;
	
	convert_to_string_ex(field);
	for (i = 0; i < mapi_get_field_count(monetdb_result); i++) {
		if (strcmp(mapi_get_name(monetdb_result, i), Z_STRVAL_PP(field)) == 0)
		{
			Z_LVAL_P(return_value) = i;
			Z_TYPE_P(return_value) = IS_LONG;
			break;
		}
	}

	RETURN_FALSE;
}
/* }}} */

/* {{{ proto mixed monetdb_fetch_result(resource result, [int row_number,] mixed field_name)
   Returns values from a result identifier */
PHP_FUNCTION(monetdb_fetch_result)
{
	zval **result, **row, **field=NULL;
	Mresult *monetdb_result;
	php_monetdb_result_handle *monetdb_result_h;
	int field_offset, monetdb_row;
	char *data;
	
	if ((ZEND_NUM_ARGS() != 3 || zend_get_parameters_ex(3, &result, &row, &field)==FAILURE) &&
	    (ZEND_NUM_ARGS() != 2 || zend_get_parameters_ex(2, &result, &field)==FAILURE)) {
		WRONG_PARAM_COUNT;
	}
	
	ZEND_FETCH_RESOURCE(monetdb_result_h, php_monetdb_result_handle *, result, -1, "MonetDB result", le_result);

	monetdb_result = monetdb_result_h->result;
	if (ZEND_NUM_ARGS() == 2) {
		if (monetdb_result_h->row < 0)
			monetdb_result_h->row = 0;
		monetdb_row = monetdb_result_h->row;
		if (monetdb_row >= mapi_get_row_count(monetdb_result)) {
			RETURN_FALSE;
		}
	} else {
		convert_to_long_ex(row);
		monetdb_row = Z_LVAL_PP(row);
		if (monetdb_row < 0 || monetdb_row >= mapi_get_row_count(monetdb_result)) {
			php_error_docref(NULL TSRMLS_CC, E_WARNING, "Unable to jump to row %ld on MonetDB result index %ld",
							Z_LVAL_PP(row), Z_LVAL_PP(result));
			RETURN_FALSE;
		}
	}
	switch(Z_TYPE_PP(field)) {
		case IS_STRING: {
			int i;
			field_offset = -1;
			for (i = 0; i < mapi_get_field_count(monetdb_result); i++) {
				if (strcmp(mapi_get_name(monetdb_result, i), Z_STRVAL_PP(field)) == 0)
				{
					field_offset = i;
					break;
				}
			}
		} break;
		default:
			convert_to_long_ex(field);
			field_offset = Z_LVAL_PP(field);
			break;
	}
	if (field_offset<0 || field_offset>=mapi_get_field_count(monetdb_result)) {
		php_error_docref(NULL TSRMLS_CC, E_WARNING, "Bad column offset specified");
		RETURN_FALSE;
	}

	/* get the data in the field */
	if (mapi_seek_row(monetdb_result, monetdb_row, MAPI_SEEK_SET) != MOK) {
		PHP_MONETDB_ERROR_RESULT("Can't jump to row: %s", monetdb_result_h);
		RETURN_FALSE;
	}
	if (mapi_fetch_row(monetdb_result) == 0 &&
			 mapi_error(monetdb_result_h->conn) != 0)
	{
		PHP_MONETDB_ERROR("Can't get row: %s", monetdb_result_h->conn);
		RETURN_FALSE;
	}
	data = mapi_fetch_field(monetdb_result, field_offset);

	if (data == NULL) {
		if (mapi_error(monetdb_result_h->conn) != 0) {
			PHP_MONETDB_ERROR("Can't fetch field: %s", monetdb_result_h->conn);
			RETURN_FALSE;
		}
		Z_TYPE_P(return_value) = IS_NULL;
	} else {
		Z_STRVAL_P(return_value) = estrndup(data, strlen(data));
		Z_STRLEN_P(return_value) = strlen(Z_STRVAL_P(return_value));
		Z_TYPE_P(return_value) = IS_STRING;
	}
}
/* }}} */

/* {{{ void php_monetdb_fetch_hash */
static void php_monetdb_fetch_hash(INTERNAL_FUNCTION_PARAMETERS, long result_type, int into_object)
{
	zval            *result, *zrow = NULL;
	Mresult         *monetdb_result;
	php_monetdb_result_handle *monetdb_result_h;
	int             i, num_fields, monetdb_row, use_row;
	long            row = -1;
	char            *element, *field_name;
	uint            element_len;
	zval            *ctor_params = NULL;
	zend_class_entry *ce = NULL;

	if (into_object) {
		char *class_name;
		int class_name_len;

		if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r|z!sz", &result, &zrow, &class_name, &class_name_len, &ctor_params) == FAILURE) {
			return;
			}
		if (ZEND_NUM_ARGS() < 3) {
			ce = zend_standard_class_def;
		} else {
			ce = zend_fetch_class(class_name, class_name_len, ZEND_FETCH_CLASS_AUTO TSRMLS_CC);
		}
		if (!ce) {
			php_error_docref(NULL TSRMLS_CC, E_WARNING, "Could not find class '%s'", class_name);
			return;
		}
		result_type = MONETDB_ASSOC;
	} else {
		if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r|z!l", &result, &zrow, &result_type) == FAILURE) {
			return;
		}
	}
	if (zrow == NULL) {
		row = -1;
	} else {
		convert_to_long(zrow);
		row = Z_LVAL_P(zrow);
	}
	use_row = ZEND_NUM_ARGS() > 1 && row != -1;

	if (!(result_type & MONETDB_BOTH)) {
		php_error_docref(NULL TSRMLS_CC, E_WARNING, "Invalid result type");
		RETURN_FALSE;
	}
	
	ZEND_FETCH_RESOURCE(monetdb_result_h, php_monetdb_result_handle *, &result, -1, "MonetDB result", le_result);

	monetdb_result = monetdb_result_h->result;

	if (use_row) { 
		monetdb_row = row;
		monetdb_result_h->row = monetdb_row;
		if (monetdb_row < 0 || monetdb_row >= mapi_get_row_count(monetdb_result)) {
			php_error_docref(NULL TSRMLS_CC, E_WARNING, "Unable to jump to row %ld on MonetDB result index %ld",
							row, Z_LVAL_P(result));
			RETURN_FALSE;
		}
	} else {
		/* If 2nd param is NULL, use internal row counter to access next row */
		monetdb_row = monetdb_result_h->row;
		if (monetdb_row < 0 || monetdb_row >= mapi_get_row_count(monetdb_result)) {
			RETURN_FALSE;
		}
		monetdb_result_h->row++;
	}

	array_init(return_value);
	for (i = 0, num_fields = mapi_get_field_count(monetdb_result); i < num_fields; i++) {
		/* get the data in the field */
		if (mapi_seek_row(monetdb_result, monetdb_row, MAPI_SEEK_SET) != MOK) {
			PHP_MONETDB_ERROR_RESULT("Can't jump to row: %s", monetdb_result_h);
			RETURN_FALSE;
		}
		if (mapi_fetch_row(monetdb_result) == 0 &&
				mapi_error(monetdb_result_h->conn) != 0)
		{
			PHP_MONETDB_ERROR("Can't get row: %s", monetdb_result_h->conn);
			RETURN_FALSE;
		}
		element = mapi_fetch_field(monetdb_result, i);

		if (element == NULL) {
			if (mapi_error(monetdb_result_h->conn) != 0) {
				PHP_MONETDB_ERROR("Can't fetch field: %s", monetdb_result_h->conn);
				RETURN_FALSE;
			}
			if (result_type & MONETDB_NUM) {
				add_index_null(return_value, i);
			}
			if (result_type & MONETDB_ASSOC) {
				field_name = mapi_get_name(monetdb_result, i);
				add_assoc_null(return_value, field_name);
			}
		} else {
			char *data;
			int data_len;
			int should_copy=0;

			element_len = strlen(element);

			data = safe_estrndup(element, element_len);
			data_len = element_len;

			if (result_type & MONETDB_NUM) {
				add_index_stringl(return_value, i, data, data_len, should_copy);
				should_copy=1;
			}

			if (result_type & MONETDB_ASSOC) {
				field_name = mapi_get_name(monetdb_result, i);
				add_assoc_stringl(return_value, field_name, data, data_len, should_copy);
			}
		}
	}

	if (into_object) {
		zval dataset = *return_value;
		zend_fcall_info fci;
		zend_fcall_info_cache fcc;
		zval *retval_ptr; 
	
		object_and_properties_init(return_value, ce, NULL);
		zend_merge_properties(return_value, Z_ARRVAL(dataset), 1 TSRMLS_CC);
	
		if (ce->constructor) {
			fci.size = sizeof(fci);
			fci.function_table = &ce->function_table;
			fci.function_name = NULL;
			fci.symbol_table = NULL;
			fci.object_pp = &return_value;
			fci.retval_ptr_ptr = &retval_ptr;
			if (ctor_params && Z_TYPE_P(ctor_params) != IS_NULL) {
				if (Z_TYPE_P(ctor_params) == IS_ARRAY) {
					HashTable *ht = Z_ARRVAL_P(ctor_params);
					Bucket *p;
	
					fci.param_count = 0;
					fci.params = emalloc(sizeof(zval*) * ht->nNumOfElements);
					p = ht->pListHead;
					while (p != NULL) {
						fci.params[fci.param_count++] = (zval**)p->pData;
						p = p->pListNext;
					}
				} else {
					/* Two problems why we throw exceptions here: PHP is
					 * typeless and hence passing one argument that's
					 * not an array could be by mistake and the other
					 * way round is possible, too. The single value is
					 * an array. Also we'd have to make that one
					 * argument passed by reference.
					 */
					zend_throw_exception(zend_exception_get_default(), "Parameter ctor_params must be an array", 0 TSRMLS_CC);
					return;
				}
			} else {
				fci.param_count = 0;
				fci.params = NULL;
			}
			fci.no_separation = 1;

			fcc.initialized = 1;
			fcc.function_handler = ce->constructor;
			fcc.calling_scope = EG(scope);
			fcc.object_pp = &return_value;
		
			if (zend_call_function(&fci, &fcc TSRMLS_CC) == FAILURE) {
				zend_throw_exception_ex(zend_exception_get_default(), 0 TSRMLS_CC, "Could not execute %s::%s()", ce->name, ce->constructor->common.function_name);
			} else {
				if (retval_ptr) {
					zval_ptr_dtor(&retval_ptr);
				}
			}
			if (fci.params) {
				efree(fci.params);
			}
		} else if (ctor_params) {
			zend_throw_exception_ex(zend_exception_get_default(), 0 TSRMLS_CC, "Class %s does not have a constructor hence you cannot use ctor_params", ce->name);
		}
	}
}
/* }}} */

/* {{{ proto array monetdb_fetch_row(resource result [, int row [, int result_type]])
   Get a row as an enumerated array */ 
PHP_FUNCTION(monetdb_fetch_row)
{
	php_monetdb_fetch_hash(INTERNAL_FUNCTION_PARAM_PASSTHRU, MONETDB_NUM, 0);
}
/* }}} */

/* {{{ proto array monetdb_fetch_assoc(resource result [, int row])
   Fetch a row as an assoc array */
PHP_FUNCTION(monetdb_fetch_assoc)
{
	/* monetdb_fetch_assoc() is added from PHP 4.3.0. It should raise
	 * error, when there is 3rd parameter */
	if (ZEND_NUM_ARGS() > 2)
		WRONG_PARAM_COUNT;
	php_monetdb_fetch_hash(INTERNAL_FUNCTION_PARAM_PASSTHRU, MONETDB_ASSOC, 0);
}
/* }}} */

/* {{{ proto array monetdb_fetch_array(resource result [, int row [, int result_type]])
   Fetch a row as an array */
PHP_FUNCTION(monetdb_fetch_array)
{
	php_monetdb_fetch_hash(INTERNAL_FUNCTION_PARAM_PASSTHRU, MONETDB_BOTH, 0);
}
/* }}} */

/* {{{ proto object monetdb_fetch_object(resource result [, int row [, string class_name [, NULL|array ctor_params]]])
   Fetch a row as an object */
PHP_FUNCTION(monetdb_fetch_object)
{
	/* monetdb_fetch_object() allowed result_type used to be. 3rd parameter
	   must be allowed for compatibility */
	php_monetdb_fetch_hash(INTERNAL_FUNCTION_PARAM_PASSTHRU, MONETDB_ASSOC, 1);
}
/* }}} */

/* {{{ proto bool monetdb_result_seek(resource result, int offset)
   Set internal row offset */
PHP_FUNCTION(monetdb_result_seek)
{
	zval *result;
	long row;
	php_monetdb_result_handle *monetdb_result_h;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rl", &result, &row) == FAILURE) {
		return;
	}

	ZEND_FETCH_RESOURCE(monetdb_result_h, php_monetdb_result_handle *, &result, -1, "MonetDB result", le_result);

	if (row < 0 || row >= mapi_get_row_count(monetdb_result_h->result)) {
		RETURN_FALSE;
	}
	
	/* seek to offset */
	monetdb_result_h->row = row;
	RETURN_TRUE;
}
/* }}} */

#define PHP_MONETDB_DATA_LENGTH 1
#define PHP_MONETDB_DATA_ISNULL 2

/* {{{ php_monetdb_data_info
 */
static void php_monetdb_data_info(INTERNAL_FUNCTION_PARAMETERS, int entry_type)
{
	zval **result, **row, **field;
	Mresult *monetdb_result;
	php_monetdb_result_handle *monetdb_result_h;
	int field_offset, monetdb_row;
	
	if ((ZEND_NUM_ARGS() != 3 || zend_get_parameters_ex(3, &result, &row, &field)==FAILURE) &&
	    (ZEND_NUM_ARGS() != 2 || zend_get_parameters_ex(2, &result, &field)==FAILURE)) {
		WRONG_PARAM_COUNT;
	}
	
	ZEND_FETCH_RESOURCE(monetdb_result_h, php_monetdb_result_handle *, result, -1, "MonetDB result", le_result);

	monetdb_result = monetdb_result_h->result;
	if (ZEND_NUM_ARGS() == 2) {
		if (monetdb_result_h->row < 0)
			monetdb_result_h->row = 0;
		monetdb_row = monetdb_result_h->row;
		if (monetdb_row < 0 || monetdb_row >= mapi_get_row_count(monetdb_result)) {
			RETURN_FALSE;
		}
	} else {
		convert_to_long_ex(row);
		monetdb_row = Z_LVAL_PP(row);
		if (monetdb_row < 0 || monetdb_row >= mapi_get_row_count(monetdb_result)) {
			php_error_docref(NULL TSRMLS_CC, E_WARNING, "Unable to jump to row %ld on MonetDB result index %ld",
							Z_LVAL_PP(row), Z_LVAL_PP(result));
			RETURN_FALSE;
		}
	}
	
	switch(Z_TYPE_PP(field)) {
		case IS_STRING: {
			int i;
			convert_to_string_ex(field);
			field_offset = -1;
			for (i = 0; i < mapi_get_field_count(monetdb_result); i++) {
				if (strcmp(mapi_get_name(monetdb_result, i), Z_STRVAL_PP(field)) == 0)
				{
					field_offset = i;
					break;
				}
			}
		} break;
		default:
			convert_to_long_ex(field);
			field_offset = Z_LVAL_PP(field);
		break;
	}
	if (field_offset < 0 || field_offset >= mapi_get_row_count(monetdb_result)) {
		php_error_docref(NULL TSRMLS_CC, E_WARNING, "Bad column offset specified");
		RETURN_FALSE;
	}
	
	switch (entry_type) {
		case PHP_MONETDB_DATA_LENGTH:
			Z_LVAL_P(return_value) = mapi_get_len(monetdb_result, field_offset);
		break;
		case PHP_MONETDB_DATA_ISNULL:
			/* get the data in the field */
			if (mapi_seek_row(monetdb_result, monetdb_row, MAPI_SEEK_SET) != MOK) {
				PHP_MONETDB_ERROR_RESULT("Can't jump to row: %s", monetdb_result_h);
				RETURN_FALSE;
			}
			if (mapi_fetch_row(monetdb_result) == 0 &&
					mapi_error(monetdb_result_h->conn) != 0)
			{
				PHP_MONETDB_ERROR("Can't get row: %s", monetdb_result_h->conn);
				RETURN_FALSE;
			}
			Z_LVAL_P(return_value) = (mapi_fetch_field(monetdb_result, field_offset) == NULL);
			if (mapi_error(monetdb_result_h->conn) != 0) {
				PHP_MONETDB_ERROR("Can't fetch field: %s", monetdb_result_h->conn);
				RETURN_FALSE;
			}
		break;
	}
	Z_TYPE_P(return_value) = IS_LONG;
}
/* }}} */

/* {{{ proto int monetdb_field_prtlen(resource result, [int row,] mixed field_name_or_number)
   Returns the printed length */
PHP_FUNCTION(monetdb_field_prtlen)
{
	php_monetdb_data_info(INTERNAL_FUNCTION_PARAM_PASSTHRU, PHP_MONETDB_DATA_LENGTH);
}
/* }}} */

/* {{{ proto int monetdb_field_is_null(resource result, [int row,] mixed field_name_or_number)
   Test if a field is NULL */
PHP_FUNCTION(monetdb_field_is_null)
{
	php_monetdb_data_info(INTERNAL_FUNCTION_PARAM_PASSTHRU, PHP_MONETDB_DATA_ISNULL);
}
/* }}} */

/* {{{ proto bool monetdb_free_result(resource result)
   Free result memory */
PHP_FUNCTION(monetdb_free_result)
{
	zval **result;
	php_monetdb_result_handle *monetdb_result_h;
	
	if (ZEND_NUM_ARGS() != 1 || zend_get_parameters_ex(1, &result)==FAILURE) {
		WRONG_PARAM_COUNT;
	}

	ZEND_FETCH_RESOURCE(monetdb_result_h, php_monetdb_result_handle *, result, -1, "MonetDB result", le_result);
	if (Z_LVAL_PP(result) == 0) {
		RETURN_FALSE;
	}
	zend_list_delete(Z_LVAL_PP(result));
	RETURN_TRUE;
}
/* }}} */

/* {{{ proto string monetdb_escape_string(string data)
   Escape string for text/char type */
PHP_FUNCTION(monetdb_escape_string)
{
	char *from = NULL, *to = NULL, *ret = NULL;
	int to_len;
	int from_len;
	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s",
							  &from, &from_len) == FAILURE) {
		return;
	}

	to = mapi_quote(from, from_len);
	to_len = strlen(to);
	ret = estrdup(to);
	free(to);	/* mapi allocated a new string using malloc, don't leak it */
	RETURN_STRINGL(ret, to_len, 0);
}
/* }}} */

/* {{{ proto int monetdb_connection_status(resource connnection)
   Get connection status */
PHP_FUNCTION(monetdb_connection_status)
{
	zval *monetdb_link = NULL;
	int id = -1;
	Mconn *monetdb_conn;

	if (zend_parse_parameters_ex(ZEND_PARSE_PARAMS_QUIET, ZEND_NUM_ARGS() TSRMLS_CC, "r",
								 &monetdb_link) == FAILURE) {
		RETURN_FALSE;
	}

	ZEND_FETCH_RESOURCE2(monetdb_conn, Mconn *, &monetdb_link, id, "MonetDB link", le_link, le_plink);

	RETURN_LONG(mapi_is_connected(monetdb_conn) ? MONETDB_CONNECTION_OK : MONETDB_CONNECTION_BAD);
}
/* }}} */

/* {{{ proto bool monetdb_connection_reset(resource connection)
   Reset connection (reconnect) */
PHP_FUNCTION(monetdb_connection_reset)
{
	zval *monetdb_link;
	int id = -1;
	Mconn *monetdb_conn;
	
	if (zend_parse_parameters_ex(ZEND_PARSE_PARAMS_QUIET, ZEND_NUM_ARGS() TSRMLS_CC, "r",
								 &monetdb_link) == FAILURE) {
		RETURN_FALSE;
	}

	ZEND_FETCH_RESOURCE2(monetdb_conn, Mconn *, &monetdb_link, id, "MonetDB link", le_link, le_plink);
	
	if (mapi_reconnect(monetdb_conn) != MOK) {
		RETURN_FALSE;
	}
	RETURN_TRUE;
}

/* }}} */

#ifdef I_FEEL_LIKE_IMPLEMENTING_COPY_INTO_PROPERLY
#define	COPYBUFSIZ	8192
/* {{{ proto bool monetdb_put_line([resource connection,] string query)
   Send null-terminated string to backend server */
PHP_FUNCTION(monetdb_put_line)
{
	zval **query, **pgsql_link = NULL;
	int id = -1;
	Mconn *pgsql;
	int result = 0;

	switch(ZEND_NUM_ARGS()) {
		case 1:
			if (zend_get_parameters_ex(1, &query)==FAILURE) {
				RETURN_FALSE;
			}
			id = MG(default_link);
			CHECK_DEFAULT_LINK(id);
			break;
		case 2:
			if (zend_get_parameters_ex(2, &pgsql_link, &query)==FAILURE) {
				RETURN_FALSE;
			}
			break;
		default:
			WRONG_PARAM_COUNT;
			break;
	}
	if (pgsql_link == NULL && id == -1) {
		RETURN_FALSE;
	}	

	ZEND_FETCH_RESOURCE2(pgsql, Mconn *, pgsql_link, id, "PostgreSQL link", le_link, le_plink);

	convert_to_string_ex(query);
	result = PQputline(pgsql, Z_STRVAL_PP(query));
	if (result==EOF) {
		PHP_PQ_ERROR("Query failed: %s", pgsql);
		RETURN_FALSE;
	}
	RETURN_TRUE;
}
/* }}} */

/* {{{ proto bool monetdb_end_copy([resource connection])
   Sync with backend. Completes the Copy command */
PHP_FUNCTION(monetdb_end_copy)
{
	zval **pgsql_link = NULL;
	int id = -1;
	Mconn *pgsql;
	int result = 0;

	switch(ZEND_NUM_ARGS()) {
		case 0:
			id = MG(default_link);
			CHECK_DEFAULT_LINK(id);
			break;
		case 1:
			if (zend_get_parameters_ex(1, &pgsql_link)==FAILURE) {
				RETURN_FALSE;
			}
			break;
		default:
			WRONG_PARAM_COUNT;
			break;
	}
	if (pgsql_link == NULL && id == -1) {
		RETURN_FALSE;
	}	

	ZEND_FETCH_RESOURCE2(pgsql, Mconn *, pgsql_link, id, "PostgreSQL link", le_link, le_plink);

	result = PQendcopy(pgsql);

	if (result!=0) {
		PHP_PQ_ERROR("Query failed: %s", pgsql);
		RETURN_FALSE;
	}
	RETURN_TRUE;
}
/* }}} */

/* {{{ proto array monetdb_copy_to(resource connection, string table_name [, string delimiter [, string null_as]])
   Copy table to array */
PHP_FUNCTION(monetdb_copy_to)
{
	zval *pgsql_link;
	char *table_name, *pg_delim = NULL, *pg_null_as = NULL;
	int table_name_len, pg_delim_len, pg_null_as_len;
	char *query;
	char *query_template = "COPY \"\" TO STDOUT DELIMITERS ':' WITH NULL AS ''";
	int id = -1;
	Mconn *pgsql;
	Mresult *pgsql_result;
	ExecStatusType status;
	int copydone = 0;
#if !HAVE_PQGETCOPYDATA
	char copybuf[COPYBUFSIZ];
#endif
	char *csv = (char *)NULL;
	int ret;
	int argc = ZEND_NUM_ARGS();

	if (zend_parse_parameters(argc TSRMLS_CC, "rs|ss",
							  &pgsql_link, &table_name, &table_name_len,
							  &pg_delim, &pg_delim_len, &pg_null_as, &pg_null_as_len) == FAILURE) {
		return;
	}
	if (!pg_delim) {
		pg_delim = "\t";
	}
	if (!pg_null_as) {
		pg_null_as = safe_estrdup("\\\\N");
	}

	ZEND_FETCH_RESOURCE2(pgsql, Mconn *, &pgsql_link, id, "PostgreSQL link", le_link, le_plink);

	query = (char *)emalloc(strlen(query_template) + strlen(table_name) + strlen(pg_null_as) + 1);
	sprintf(query, "COPY \"%s\" TO STDOUT DELIMITERS '%c' WITH NULL AS '%s'",
			table_name, *pg_delim, pg_null_as);

	while ((pgsql_result = MgetResult(pgsql))) {
		Mclear(pgsql_result);
	}
	pgsql_result = PQexec(pgsql, query);
	efree(pg_null_as);
	efree(query);

	if (pgsql_result) {
		status = PQresultStatus(pgsql_result);
	} else {
		status = (ExecStatusType) PQstatus(pgsql);
	}

	switch (status) {
		case PGRES_COPY_OUT:
			if (pgsql_result) {
				Mclear(pgsql_result);
				array_init(return_value);
#if HAVE_PQGETCOPYDATA
				while (!copydone)
				{
					ret = PQgetCopyData(pgsql, &csv, 0);
					switch (ret) {
						case -1:
							copydone = 1;
							break;
						case 0:
						case -2:
							PHP_PQ_ERROR("getline failed: %s", pgsql);
							RETURN_FALSE;
							break;
						default:
							add_next_index_string(return_value, csv, 1);
							PQfreemem(csv);
							break;
					}
				}
#else
				while (!copydone)
				{
					if ((ret = PQgetline(pgsql, copybuf, COPYBUFSIZ))) {
						PHP_PQ_ERROR("getline failed: %s", pgsql);
						RETURN_FALSE;
					}
			
					if (copybuf[0] == '\\' &&
						copybuf[1] == '.' &&
						copybuf[2] == '\0')
					{
						copydone = 1;
					}
					else
					{
						if (csv == (char *)NULL) {
							csv = estrdup(copybuf);
						} else {
							csv = (char *)erealloc(csv, strlen(csv) + sizeof(char)*(COPYBUFSIZ+1));
							strcat(csv, copybuf);
						}
							
						switch (ret)
						{
							case EOF:
								copydone = 1;
							case 0:
								add_next_index_string(return_value, csv, 1);
								efree(csv);
								csv = (char *)NULL;
								break;
							case 1:
								break;
						}
					}
				}
				if (PQendcopy(pgsql)) {
					PHP_PQ_ERROR("endcopy failed: %s", pgsql);
					RETURN_FALSE;
				}
#endif
				while ((pgsql_result = MgetResult(pgsql))) {
					Mclear(pgsql_result);
				}
			} else {
				Mclear(pgsql_result);
				RETURN_FALSE;
			}
			break;
		default:
			Mclear(pgsql_result);
			PHP_PQ_ERROR("Copy command failed: %s", pgsql);
			RETURN_FALSE;
			break;
	}
}
/* }}} */

/* {{{ proto bool monetdb_copy_from(resource connection, string table_name , array rows [, string delimiter [, string null_as]])
   Copy table from array */
PHP_FUNCTION(monetdb_copy_from)
{
	zval *pgsql_link = NULL, *pg_rows;
	zval **tmp;
	char *table_name, *pg_delim = NULL, *pg_null_as = NULL;
	int  table_name_len, pg_delim_len, pg_null_as_len;
	int  pg_null_as_free = 0;
	char *query;
	char *query_template = "COPY \"\" FROM STDIN DELIMITERS ':' WITH NULL AS ''";
	HashPosition pos;
	int id = -1;
	Mconn *pgsql;
	Mresult *pgsql_result;
	ExecStatusType status;
	int argc = ZEND_NUM_ARGS();

	if (zend_parse_parameters(argc TSRMLS_CC, "rs/a|ss",
							  &pgsql_link, &table_name, &table_name_len, &pg_rows,
							  &pg_delim, &pg_delim_len, &pg_null_as, &pg_null_as_len) == FAILURE) {
		return;
	}
	if (!pg_delim) {
		pg_delim = "\t";
	}
	if (!pg_null_as) {
		pg_null_as = safe_estrdup("\\\\N");
		pg_null_as_free = 1;
	}

	ZEND_FETCH_RESOURCE2(pgsql, Mconn *, &pgsql_link, id, "PostgreSQL link", le_link, le_plink);

	query = (char *)emalloc(strlen(query_template) + strlen(table_name) + strlen(pg_null_as) + 1);
	sprintf(query, "COPY \"%s\" FROM STDIN DELIMITERS '%c' WITH NULL AS '%s'",
			table_name, *pg_delim, pg_null_as);
	while ((pgsql_result = MgetResult(pgsql))) {
		Mclear(pgsql_result);
	}
	pgsql_result = PQexec(pgsql, query);

	if (pg_null_as_free) {
		efree(pg_null_as);
	}
	efree(query);

	if (pgsql_result) {
		status = PQresultStatus(pgsql_result);
	} else {
		status = (ExecStatusType) PQstatus(pgsql);
	}

	switch (status) {
		case PGRES_COPY_IN:
			if (pgsql_result) {
				int command_failed = 0;
				Mclear(pgsql_result);
				zend_hash_internal_pointer_reset_ex(Z_ARRVAL_P(pg_rows), &pos);
#if HAVE_PQPUTCOPYDATA
				while (zend_hash_get_current_data_ex(Z_ARRVAL_P(pg_rows), (void **) &tmp, &pos) == SUCCESS) {
					convert_to_string_ex(tmp);
					query = (char *)emalloc(Z_STRLEN_PP(tmp) +2);
					strcpy(query, Z_STRVAL_PP(tmp));
					if(*(query+Z_STRLEN_PP(tmp)-1) != '\n')
						strcat(query, "\n");
					if (PQputCopyData(pgsql, query, strlen(query)) != 1) {
						efree(query);
						PHP_PQ_ERROR("copy failed: %s", pgsql);
						RETURN_FALSE;
					}
					efree(query);
					zend_hash_move_forward_ex(Z_ARRVAL_P(pg_rows), &pos);
				}
				if (PQputCopyEnd(pgsql, NULL) != 1) {
					PHP_PQ_ERROR("putcopyend failed: %s", pgsql);
					RETURN_FALSE;
				}
#else
				while (zend_hash_get_current_data_ex(Z_ARRVAL_P(pg_rows), (void **) &tmp, &pos) == SUCCESS) {
					convert_to_string_ex(tmp);
					query = (char *)emalloc(Z_STRLEN_PP(tmp) +2);
					strcpy(query, Z_STRVAL_PP(tmp));
					if(*(query+Z_STRLEN_PP(tmp)-1) != '\n')
						strcat(query, "\n");
					if (PQputline(pgsql, query)==EOF) {
						efree(query);
						PHP_PQ_ERROR("copy failed: %s", pgsql);
						RETURN_FALSE;
					}
					efree(query);
					zend_hash_move_forward_ex(Z_ARRVAL_P(pg_rows), &pos);
				}
				if (PQputline(pgsql, "\\.\n") == EOF) {
					PHP_PQ_ERROR("putline failed: %s", pgsql);
					RETURN_FALSE;
				}
				if (PQendcopy(pgsql)) {
					PHP_PQ_ERROR("endcopy failed: %s", pgsql);
					RETURN_FALSE;
				}
#endif
				while ((pgsql_result = MgetResult(pgsql))) {
					if (PGRES_COMMAND_OK != PQresultStatus(pgsql_result)) {
						PHP_PQ_ERROR("Copy command failed: %s", pgsql);
						command_failed = 1;
					}
					Mclear(pgsql_result);
				}
				if (command_failed) {
					RETURN_FALSE;
				}
			} else {
				Mclear(pgsql_result);
				RETURN_FALSE;
			}
			RETURN_TRUE;
			break;
		default:
			Mclear(pgsql_result);
			PHP_PQ_ERROR("Copy command failed: %s", pgsql);
			RETURN_FALSE;
			break;
	}
}
/* }}} */
#endif /* I_FEEL_LIKE_IMPLEMENTING_COPY_INTO_PROPERLY */

#ifdef I_FEEL_LIKE_IMPLEMENTING_PREPARED_STUFF
#define PHP_PG_ASYNC_IS_BUSY		1
#define PHP_PG_ASYNC_REQUEST_CANCEL 2
/* {{{ php_monetdb_flush_query
 */
static int php_monetdb_flush_query(Mconn *pgsql TSRMLS_DC) 
{
	Mresult *res;
	int leftover = 0;
	
	while ((res = MgetResult(pgsql))) {
		Mclear(res);
		leftover++;
	}
	return leftover;
}
/* }}} */

/* {{{ php_monetdb_do_async
 */
static void php_monetdb_do_async(INTERNAL_FUNCTION_PARAMETERS, int entry_type) 
{
	zval *pgsql_link;
	int id = -1;
	Mconn *pgsql;
	Mresult *pgsql_result;

	if (zend_parse_parameters_ex(ZEND_PARSE_PARAMS_QUIET, ZEND_NUM_ARGS() TSRMLS_CC, "r",
								 &pgsql_link) == FAILURE) {
		RETURN_FALSE;
	}

	ZEND_FETCH_RESOURCE2(pgsql, Mconn *, &pgsql_link, id, "PostgreSQL link", le_link, le_plink);

	switch(entry_type) {
		case PHP_PG_ASYNC_IS_BUSY:
			PQconsumeInput(pgsql);
			Z_LVAL_P(return_value) = PQisBusy(pgsql);
			Z_TYPE_P(return_value) = IS_LONG;
			break;
		case PHP_PG_ASYNC_REQUEST_CANCEL:
			Z_LVAL_P(return_value) = PQrequestCancel(pgsql);
			Z_TYPE_P(return_value) = IS_LONG;
			while ((pgsql_result = MgetResult(pgsql))) {
				Mclear(pgsql_result);
			}
			break;
		default:
			php_error_docref(NULL TSRMLS_CC, E_ERROR, "PostgreSQL module error, please report this error");
			break;
	}
	convert_to_boolean_ex(&return_value);
}
/* }}} */

/* {{{ proto bool monetdb_connection_busy(resource connection)
   Get connection is busy or not */
PHP_FUNCTION(monetdb_connection_busy)
{
	php_monetdb_do_async(INTERNAL_FUNCTION_PARAM_PASSTHRU, PHP_PG_ASYNC_IS_BUSY);
}
/* }}} */

/* {{{ proto bool monetdb_send_query(resource connection, string query)
   Send asynchronous query */
PHP_FUNCTION(monetdb_send_query)
{
	zval *pgsql_link;
	char *query;
	int len;
	int id = -1;
	Mconn *pgsql;
	Mresult *res;
	int leftover = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rs",
							  &pgsql_link, &query, &len) == FAILURE) {
		return;
	}

	ZEND_FETCH_RESOURCE2(pgsql, Mconn *, &pgsql_link, id, "PostgreSQL link", le_link, le_plink);

	while ((res = MgetResult(pgsql))) {
		Mclear(res);
		leftover = 1;
	}
	if (leftover) {
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "There are results on this connection. Call pg_get_result() until it returns FALSE");
	}
	if (!PQsendQuery(pgsql, query)) {
		if ((MG(auto_reset_persistent) & 2) && PQstatus(pgsql) != MONETDB_CONNECTION_OK) {
			PQreset(pgsql);
		}
		if (!PQsendQuery(pgsql, query)) {
			RETURN_FALSE;
		}
	}
	RETURN_TRUE;
}
/* }}} */

/* {{{ proto bool monetdb_send_query_params(resource connection, string query)
   Send asynchronous parameterized query */
PHP_FUNCTION(monetdb_send_query_params)
{
	zval **pgsql_link;
	zval **pv_param_arr, **tmp;
	int num_params = 0;
	char **params = NULL;
	unsigned char otype;
	zval **query;
	int id = -1;
	Mconn *pgsql;
	Mresult *res;
	int leftover = 0;

	if (zend_get_parameters_ex(3, &pgsql_link, &query, &pv_param_arr) == FAILURE) {
		return;
	}

	if (pgsql_link == NULL && id == -1) {
		RETURN_FALSE;
	}	

	if (Z_TYPE_PP(pv_param_arr) != IS_ARRAY) {
		php_error_docref(NULL TSRMLS_CC, E_WARNING, "No array passed");
		RETURN_FALSE;
	}

	ZEND_FETCH_RESOURCE2(pgsql, Mconn *, pgsql_link, id, "PostgreSQL link", le_link, le_plink);

	convert_to_string_ex(query);
	while ((res = MgetResult(pgsql))) {
		Mclear(res);
		leftover = 1;
	}
	if (leftover) {
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "There are results on this connection. Call pg_get_result() until it returns FALSE");
	}

	zend_hash_internal_pointer_reset(Z_ARRVAL_PP(pv_param_arr));
	num_params = zend_hash_num_elements(Z_ARRVAL_PP(pv_param_arr));
	if (num_params > 0) {
		int i = 0;
		params = (char **)safe_emalloc(sizeof(char *), num_params, 0);
		
		for(i = 0; i < num_params; i++) {
			if (zend_hash_get_current_data(Z_ARRVAL_PP(pv_param_arr), (void **) &tmp) == FAILURE) {
				php_error_docref(NULL TSRMLS_CC, E_WARNING,"Error getting parameter");
				_php_pgsql_free_params(params, num_params);
				RETURN_FALSE;
			}

			otype = (*tmp)->type;
			convert_to_string(*tmp);
			if (Z_TYPE_PP(tmp) != IS_STRING) {
				php_error_docref(NULL TSRMLS_CC, E_WARNING,"Error converting parameter");
				_php_pgsql_free_params(params, num_params);
				RETURN_FALSE;
			}

			if (otype == IS_NULL) {
				params[i] = NULL;
			}
			else {
				params[i] = Z_STRVAL_PP(tmp);
			}

			zend_hash_move_forward(Z_ARRVAL_PP(pv_param_arr));
		}
	}

	if (!PQsendQueryParams(pgsql, Z_STRVAL_PP(query), num_params, NULL, (const char * const *)params, NULL, NULL, 0)) {
		if ((MG(auto_reset_persistent) & 2) && PQstatus(pgsql) != MONETDB_CONNECTION_OK) {
			PQreset(pgsql);
		}
		if (!PQsendQueryParams(pgsql, Z_STRVAL_PP(query), num_params, NULL, (const char * const *)params, NULL, NULL, 0)) {
			_php_pgsql_free_params(params, num_params);
			RETURN_FALSE;
		}
	}
	_php_pgsql_free_params(params, num_params);
	RETURN_TRUE;
}
/* }}} */

/* {{{ proto bool monetdb_send_prepare(resource connection, string stmtname, string query)
   Asynchronously prepare a query for future execution */
PHP_FUNCTION(monetdb_send_prepare)
{
	zval **pgsql_link;
	zval **query, **stmtname;
	int id = -1;
	Mconn *pgsql;
	Mresult *res;
	int leftover = 0;

	if (zend_get_parameters_ex(3, &pgsql_link, &stmtname, &query) == FAILURE) {
		return;
	}
	if (pgsql_link == NULL && id == -1) {
		RETURN_FALSE;
	}	

	ZEND_FETCH_RESOURCE2(pgsql, Mconn *, pgsql_link, id, "PostgreSQL link", le_link, le_plink);

	convert_to_string_ex(stmtname);
	convert_to_string_ex(query);
	while ((res = MgetResult(pgsql))) {
		Mclear(res);
		leftover = 1;
	}
	if (leftover) {
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "There are results on this connection. Call pg_get_result() until it returns FALSE");
	}
	if (!PQsendPrepare(pgsql, Z_STRVAL_PP(stmtname), Z_STRVAL_PP(query), 0, NULL)) {
		if ((MG(auto_reset_persistent) & 2) && PQstatus(pgsql) != MONETDB_CONNECTION_OK) {
			PQreset(pgsql);
		}
		if (!PQsendPrepare(pgsql, Z_STRVAL_PP(stmtname), Z_STRVAL_PP(query), 0, NULL)) {
			RETURN_FALSE;
		}
	}
	RETURN_TRUE;
}
/* }}} */

/* {{{ proto bool monetdb_send_execute(resource connection, string stmtname, array params)
   Executes prevriously prepared stmtname asynchronously */
PHP_FUNCTION(monetdb_send_execute)
{
	zval **pgsql_link;
	zval **pv_param_arr, **tmp;
	int num_params = 0;
	char **params = NULL;
	unsigned char otype;
	zval **stmtname;
	int id = -1;
	Mconn *pgsql;
	Mresult *res;
	int leftover = 0;

	if (zend_get_parameters_ex(3, &pgsql_link, &stmtname, &pv_param_arr)==FAILURE) {
		return;
	}
	if (pgsql_link == NULL && id == -1) {
		RETURN_FALSE;
	}	

	if (Z_TYPE_PP(pv_param_arr) != IS_ARRAY) {
		php_error_docref(NULL TSRMLS_CC, E_WARNING, "No array passed");
		RETURN_FALSE;
	}

	ZEND_FETCH_RESOURCE2(pgsql, Mconn *, pgsql_link, id, "PostgreSQL link", le_link, le_plink);

	convert_to_string_ex(stmtname);
	while ((res = MgetResult(pgsql))) {
		Mclear(res);
		leftover = 1;
	}
	if (leftover) {
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "There are results on this connection. Call pg_get_result() until it returns FALSE");
	}

	zend_hash_internal_pointer_reset(Z_ARRVAL_PP(pv_param_arr));
	num_params = zend_hash_num_elements(Z_ARRVAL_PP(pv_param_arr));
	if (num_params > 0) {
		int i = 0;
		params = (char **)safe_emalloc(sizeof(char *), num_params, 0);
		
		for(i = 0; i < num_params; i++) {
			if (zend_hash_get_current_data(Z_ARRVAL_PP(pv_param_arr), (void **) &tmp) == FAILURE) {
				php_error_docref(NULL TSRMLS_CC, E_WARNING,"Error getting parameter");
				_php_pgsql_free_params(params, num_params);
				RETURN_FALSE;
			}

			otype = (*tmp)->type;
			convert_to_string(*tmp);
			if (Z_TYPE_PP(tmp) != IS_STRING) {
				php_error_docref(NULL TSRMLS_CC, E_WARNING,"Error converting parameter");
				_php_pgsql_free_params(params, num_params);
				RETURN_FALSE;
			}

			if (otype == IS_NULL) {
				params[i] = NULL;
			}
			else {
				params[i] = Z_STRVAL_PP(tmp);
			}

			zend_hash_move_forward(Z_ARRVAL_PP(pv_param_arr));
		}
	}

	if (!PQsendQueryPrepared(pgsql, Z_STRVAL_PP(stmtname), num_params, (const char * const *)params, NULL, NULL, 0)) {
		if ((MG(auto_reset_persistent) & 2) && PQstatus(pgsql) != MONETDB_CONNECTION_OK) {
			PQreset(pgsql);
		}
		if (!PQsendQueryPrepared(pgsql, Z_STRVAL_PP(stmtname), num_params, (const char * const *)params, NULL, NULL, 0)) {
			_php_pgsql_free_params(params, num_params);
			RETURN_FALSE;
		}
	}
	_php_pgsql_free_params(params, num_params);
	RETURN_TRUE;
}
/* }}} */

/* {{{ proto resource monetdb_get_result(resource connection)
   Get asynchronous query result */
PHP_FUNCTION(monetdb_get_result)
{
	zval *pgsql_link;
	int id = -1;
	Mconn *pgsql;
	Mresult *pgsql_result;
	php_monetdb_result_handle *monetdb_result_h;

	if (zend_parse_parameters_ex(ZEND_PARSE_PARAMS_QUIET, ZEND_NUM_ARGS() TSRMLS_CC, "r",
								 &pgsql_link) == FAILURE) {
		RETURN_FALSE;
	}

	ZEND_FETCH_RESOURCE2(pgsql, Mconn *, &pgsql_link, id, "PostgreSQL link", le_link, le_plink);
	
	pgsql_result = MgetResult(pgsql);
	if (!pgsql_result) {
		/* no result */
		RETURN_FALSE;
	}
	monetdb_result_h = (php_monetdb_result_handle *) emalloc(sizeof(php_monetdb_result_handle));
	monetdb_result_h->conn = pgsql;
	monetdb_result_h->result = pgsql_result;
	monetdb_result_h->row = 0;
	ZEND_REGISTER_RESOURCE(return_value, monetdb_result_h, le_result);
}
/* }}} */

/* {{{ proto mixed monetdb_result_status(resource result[, long result_type])
   Get status of query result */
PHP_FUNCTION(monetdb_result_status)
{
	zval *result;
	long result_type = MONETDB_STATUS_LONG;
	ExecStatusType status;
	Mresult *pgsql_result;
	php_monetdb_result_handle *monetdb_result_h;

	if (zend_parse_parameters_ex(ZEND_PARSE_PARAMS_QUIET, ZEND_NUM_ARGS() TSRMLS_CC, "r|l",
								 &result, &result_type) == FAILURE) {
		RETURN_FALSE;
	}

	ZEND_FETCH_RESOURCE(monetdb_result_h, php_monetdb_result_handle *, &result, -1, "PostgreSQL result", le_result);

	pgsql_result = monetdb_result_h->result;
	if (result_type == MONETDB_STATUS_LONG) {
		status = PQresultStatus(pgsql_result);
		RETURN_LONG((int)status);
	}
	else if (result_type == MONETDB_STATUS_STRING) {
		RETURN_STRING(PQcmdStatus(pgsql_result), 1);
	}
	else {
		php_error_docref(NULL TSRMLS_CC, E_WARNING, "Optional 2nd parameter should be MONETDB_STATUS_LONG or MONETDB_STATUS_STRING");
		RETURN_FALSE;
	}
}
/* }}} */
#endif /* I_FEEL_LIKE_IMPLEMENTING_PREPARED_STUFF */

#ifdef I_FEEL_LIKE_IMPLEMENTING_POSTGRES_EXPERIMENTAL_FUNCTIONS
/* {{{ php_monetdb_meta_data
 * TODO: Add meta_data cache for better performance
 */
PHP_MONETDB_API int php_monetdb_meta_data(Mconn *pg_link, const char *table_name, zval *meta TSRMLS_DC) 
{
	Mresult *monetdb_result_h;
	char *tmp_name;
	smart_str querystr = {0};
	int new_len;
	int i, num_rows;
	zval *elem;
	
	smart_str_appends(&querystr, 
			"SELECT a.attname, a.attnum, t.typname, a.attlen, a.attnotNULL, a.atthasdef, a.attndims "
			"FROM pg_class as c, pg_attribute a, pg_type t "
			"WHERE a.attnum > 0 AND a.attrelid = c.oid AND c.relname = '");
	
	tmp_name = php_addslashes((char *)table_name, strlen(table_name), &new_len, 0 TSRMLS_CC);
	smart_str_appendl(&querystr, tmp_name, new_len);
	efree(tmp_name);

	smart_str_appends(&querystr, "' AND a.atttypid = t.oid ORDER BY a.attnum;");
	smart_str_0(&querystr);
	
	monetdb_result_h = PQexec(pg_link, querystr.c);
	if (PQresultStatus(monetdb_result_h) != PGRES_TUPLES_OK || (num_rows = PQntuples(monetdb_result_h)) == 0) {
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Failed to query meta_data for '%s' table %s", table_name, querystr.c);
		smart_str_free(&querystr);
		Mclear(monetdb_result_h);
		return FAILURE;
	}
	smart_str_free(&querystr);

	for (i = 0; i < num_rows; i++) {
		char *name;
		MAKE_STD_ZVAL(elem);
		array_init(elem);
		add_assoc_long(elem, "num", atoi(PQgetvalue(monetdb_result_h,i,1)));
		add_assoc_string(elem, "type", PQgetvalue(monetdb_result_h,i,2), 1);
		add_assoc_long(elem, "len", atoi(PQgetvalue(monetdb_result_h,i,3)));
		if (!strcmp(PQgetvalue(monetdb_result_h,i,4), "t")) {
			add_assoc_bool(elem, "not null", 1);
		}
		else {
			add_assoc_bool(elem, "not null", 0);
		}
		if (!strcmp(PQgetvalue(monetdb_result_h,i,5), "t")) {
			add_assoc_bool(elem, "has default", 1);
		}
		else {
			add_assoc_bool(elem, "has default", 0);
		}
		add_assoc_long(elem, "array dims", atoi(PQgetvalue(monetdb_result_h,i,6)));
		name = PQgetvalue(monetdb_result_h,i,0);
		add_assoc_zval(meta, name, elem);
	}
	Mclear(monetdb_result_h);
	
	return SUCCESS;
}

/* }}} */

/* {{{ proto array monetdb_meta_data(resource db, string table)
   Get meta_data */
PHP_FUNCTION(monetdb_meta_data)
{
	zval *pgsql_link;
	char *table_name;
	uint table_name_len;
	Mconn *pgsql;
	int id = -1;
	
	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rs",
							  &pgsql_link, &table_name, &table_name_len) == FAILURE) {
		return;
	}

	ZEND_FETCH_RESOURCE2(pgsql, Mconn *, &pgsql_link, id, "PostgreSQL link", le_link, le_plink);
	
	array_init(return_value);
	if (php_pgsql_meta_data(pgsql, table_name, return_value TSRMLS_CC) == FAILURE) {
		zval_dtor(return_value); /* destroy array */
		RETURN_FALSE;
	}
} 
/* }}} */

/* {{{ php_monetdb_get_data_type
 */
static php_monetdb_data_type php_monetdb_get_data_type(const char *type_name, size_t len)
{
    /* This is stupid way to do. I'll fix it when I decied how to support
	   user defined types. (Yasuo) */
	
	/* boolean */
	if (!strcmp(type_name, "bool")|| !strcmp(type_name, "boolean"))
		return PG_BOOL;
	/* object id */
	if (!strcmp(type_name, "oid"))
		return PG_OID;
	/* integer */
	if (!strcmp(type_name, "int2") || !strcmp(type_name, "smallint"))
		return PG_INT2;
	if (!strcmp(type_name, "int4") || !strcmp(type_name, "integer"))
		return PG_INT4;
	if (!strcmp(type_name, "int8") || !strcmp(type_name, "bigint"))
		return PG_INT8;
	/* real and other */
	if (!strcmp(type_name, "float4") || !strcmp(type_name, "real"))
		return PG_FLOAT4;
	if (!strcmp(type_name, "float8") || !strcmp(type_name, "double precision"))
		return PG_FLOAT8;
	if (!strcmp(type_name, "numeric"))
		return PG_NUMERIC;
	if (!strcmp(type_name, "money"))
		return PG_MONEY;
	/* character */
	if (!strcmp(type_name, "text"))
		return PG_TEXT;
	if (!strcmp(type_name, "bpchar") || !strcmp(type_name, "character"))
		return PG_CHAR;
	if (!strcmp(type_name, "varchar") || !strcmp(type_name, "character varying"))
		return PG_VARCHAR;
	/* time and interval */
	if (!strcmp(type_name, "abstime"))
		return PG_UNIX_TIME;
	if (!strcmp(type_name, "reltime"))
		return PG_UNIX_TIME_INTERVAL;
	if (!strcmp(type_name, "tinterval"))
		return PG_UNIX_TIME_INTERVAL;
	if (!strcmp(type_name, "date"))
		return PG_DATE;
	if (!strcmp(type_name, "time"))
		return PG_TIME;
	if (!strcmp(type_name, "time with time zone") || !strcmp(type_name, "timetz"))
		return PG_TIME_WITH_TIMEZONE;
	if (!strcmp(type_name, "timestamp without time zone") || !strcmp(type_name, "timestamp"))
		return PG_TIMESTAMP;
	if (!strcmp(type_name, "timestamp with time zone") || !strcmp(type_name, "timestamptz"))
		return PG_TIMESTAMP_WITH_TIMEZONE;
	if (!strcmp(type_name, "interval"))
		return PG_INTERVAL;
	/* binary */
	if (!strcmp(type_name, "bytea"))
		return PG_BYTEA;
	/* network */
	if (!strcmp(type_name, "cidr"))
		return PG_CIDR;
	if (!strcmp(type_name, "inet"))
		return PG_INET;
	if (!strcmp(type_name, "macaddr"))
		return PG_MACADDR;
	/* bit */
	if (!strcmp(type_name, "bit"))
		return PG_BIT;
	if (!strcmp(type_name, "bit varying"))
		return PG_VARBIT;
	/* geometric */
	if (!strcmp(type_name, "line"))
		return PG_LINE;
	if (!strcmp(type_name, "lseg"))
		return PG_LSEG;
	if (!strcmp(type_name, "box"))
		return PG_BOX;
	if (!strcmp(type_name, "path"))
		return PG_PATH;
	if (!strcmp(type_name, "point"))
		return PG_POINT;
	if (!strcmp(type_name, "polygon"))
		return PG_POLYGON;
	if (!strcmp(type_name, "circle"))
		return PG_CIRCLE;
		
	return PG_UNKNOWN;
}
/* }}} */

/* {{{ php_monetdb_convert_match
 * test field value with regular expression specified.  
 */
static int php_monetdb_convert_match(const char *str, const char *regex , int icase TSRMLS_DC)
{
	regex_t re;	
	regmatch_t *subs;
	int regopt = REG_EXTENDED;
	int regerr, ret = SUCCESS;

	if (icase) {
		regopt |= REG_ICASE;
	}
	
	regerr = regcomp(&re, regex, regopt);
	if (regerr) {
		php_error_docref(NULL TSRMLS_CC, E_WARNING, "Cannot compile regex");
		regfree(&re);
		return FAILURE;
	}
	subs = (regmatch_t *)ecalloc(sizeof(regmatch_t), re.re_nsub+1);

	regerr = regexec(&re, str, re.re_nsub+1, subs, 0);
	if (regerr == REG_NOMATCH) {
#ifdef PHP_DEBUG		
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "'%s' does not match with '%s'", str, regex);
#endif
		ret = FAILURE;
	}
	else if (regerr) {
		php_error_docref(NULL TSRMLS_CC, E_WARNING, "Cannot exec regex");
		ret = FAILURE;
	}
	regfree(&re);
	efree(subs);
	return ret;
}

/* }}} */

/* {{{ php_monetdb_add_quote
 * add quotes around string.
 */
static int php_monetdb_add_quotes(zval *src, zend_bool should_free TSRMLS_DC) 
{
	smart_str str = {0};
	
	assert(Z_TYPE_P(src) == IS_STRING);
	assert(should_free == 1 || should_free == 0);

	smart_str_appendc(&str, '\'');
	smart_str_appendl(&str, Z_STRVAL_P(src), Z_STRLEN_P(src));
	smart_str_appendc(&str, '\'');
	smart_str_0(&str);
	
	if (should_free) {
		efree(Z_STRVAL_P(src));
	}
	Z_STRVAL_P(src) = str.c;
	Z_STRLEN_P(src) = str.len;

	return SUCCESS;
}
/* }}} */

/* {{{ MONETDB_CONV_CHECK_IGNORE */
#define MONETDB_CONV_CHECK_IGNORE() \
				if (!err && Z_TYPE_P(new_val) == IS_STRING && !strcmp(Z_STRVAL_P(new_val), "NULL")) { \
					/* if new_value is string "NULL" and field has default value, remove element to use default value */ \
					if (!(opt & MONETDB_CONV_IGNORE_DEFAULT) && Z_BVAL_PP(has_default)) { \
						zval_dtor(new_val); \
						FREE_ZVAL(new_val); \
						skip_field = 1; \
					} \
					/* raise error if it's not null and cannot be ignored */ \
					else if (!(opt & MONETDB_CONV_IGNORE_NOT_NULL) && Z_BVAL_PP(not_null)) { \
						php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Detected NULL for 'NOT NULL' field '%s'", field ); \
						err = 1; \
					} \
				}
/* }}} */

/* {{{ php_monetdb_convert
 * check and convert array values (fieldname=>vlaue pair) for sql
 */
PHP_MONETDB_API int php_monetdb_convert(Mconn *pg_link, const char *table_name, const zval *values, zval *result, ulong opt TSRMLS_DC) 
{
	HashPosition pos;
	char *field = NULL;
	uint field_len = -1;
	ulong num_idx = -1;
	zval *meta, **def, **type, **not_null, **has_default, **val, *new_val;
	int new_len, key_type, err = 0, skip_field;
	
	assert(pg_link != NULL);
	assert(Z_TYPE_P(values) == IS_ARRAY);
	assert(Z_TYPE_P(result) == IS_ARRAY);
	assert(!(opt & ~MONETDB_CONV_OPTS));

	if (!table_name) {
		return FAILURE;
	}
	MAKE_STD_ZVAL(meta);
	array_init(meta);
	if (php_pgsql_meta_data(pg_link, table_name, meta TSRMLS_CC) == FAILURE) {
		zval_dtor(meta);
		FREE_ZVAL(meta);
		return FAILURE;
	}
	for (zend_hash_internal_pointer_reset_ex(Z_ARRVAL_P(values), &pos);
		 zend_hash_get_current_data_ex(Z_ARRVAL_P(values), (void **)&val, &pos) == SUCCESS;
		 zend_hash_move_forward_ex(Z_ARRVAL_P(values), &pos)) {
		skip_field = 0;
		new_val = NULL;
		
		if ((key_type = zend_hash_get_current_key_ex(Z_ARRVAL_P(values), &field, &field_len, &num_idx, 0, &pos)) == HASH_KEY_NON_EXISTANT) {
			php_error_docref(NULL TSRMLS_CC, E_WARNING, "Failed to get array key type");
			err = 1;
		}
		if (!err && key_type == HASH_KEY_IS_LONG) {
			php_error_docref(NULL TSRMLS_CC, E_WARNING, "Accepts only string key for values");
			err = 1;
		}
		if (!err && key_type == HASH_KEY_NON_EXISTANT) {
			php_error_docref(NULL TSRMLS_CC, E_WARNING, "Accepts only string key for values");
			err = 1;
		}
		if (!err && zend_hash_find(Z_ARRVAL_P(meta), field, field_len, (void **)&def) == FAILURE) {
			php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Invalid field name (%s) in values", field);
			err = 1;
		}
		if (!err && zend_hash_find(Z_ARRVAL_PP(def), "type", sizeof("type"), (void **)&type) == FAILURE) {
			php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Detected broken meta data. Missing 'type'");
			err = 1;
		}
		if (!err && zend_hash_find(Z_ARRVAL_PP(def), "not null", sizeof("not null"), (void **)&not_null) == FAILURE) {
			php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Detected broken meta data. Missing 'not null'");
			err = 1;
		}
		if (!err && zend_hash_find(Z_ARRVAL_PP(def), "has default", sizeof("has default"), (void **)&has_default) == FAILURE) {
			php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Detected broken meta data. Missing 'has default'");
			err = 1;
		}
		if (!err && (Z_TYPE_PP(val) == IS_ARRAY ||
			 Z_TYPE_PP(val) == IS_OBJECT ||
			 Z_TYPE_PP(val) == IS_CONSTANT_ARRAY)) {
			php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Expects scaler values as field values");
			err = 1;
		}
		if (err) {
			break; /* break out for() */
		}
		ALLOC_INIT_ZVAL(new_val);
		switch(php_pgsql_get_data_type(Z_STRVAL_PP(type), Z_STRLEN_PP(type)))
		{
			case PG_BOOL:
				switch (Z_TYPE_PP(val)) {
					case IS_STRING:
						if (Z_STRLEN_PP(val) == 0) {
							ZVAL_STRING(new_val, "NULL", 1);
						}
						else {
							if (!strcmp(Z_STRVAL_PP(val), "t") || !strcmp(Z_STRVAL_PP(val), "T") ||
								!strcmp(Z_STRVAL_PP(val), "y") || !strcmp(Z_STRVAL_PP(val), "Y") ||
								!strcmp(Z_STRVAL_PP(val), "true") || !strcmp(Z_STRVAL_PP(val), "True") ||
								!strcmp(Z_STRVAL_PP(val), "yes") || !strcmp(Z_STRVAL_PP(val), "Yes") ||
								!strcmp(Z_STRVAL_PP(val), "1")) {
								ZVAL_STRING(new_val, "'t'", 1);
							}
							else if (!strcmp(Z_STRVAL_PP(val), "f") || !strcmp(Z_STRVAL_PP(val), "F") ||
									 !strcmp(Z_STRVAL_PP(val), "n") || !strcmp(Z_STRVAL_PP(val), "N") ||
									 !strcmp(Z_STRVAL_PP(val), "false") ||  !strcmp(Z_STRVAL_PP(val), "False") ||
									 !strcmp(Z_STRVAL_PP(val), "no") ||  !strcmp(Z_STRVAL_PP(val), "No") ||
									 !strcmp(Z_STRVAL_PP(val), "0")) {
								ZVAL_STRING(new_val, "'f'", 1);
							}
							else {
								php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Detected invalid value (%s) for PostgreSQL %s field (%s)", Z_STRVAL_PP(val), Z_STRVAL_PP(type), field);
								err = 1;
							}
						}
						break;
						
					case IS_LONG:
					case IS_BOOL:
						if (Z_LVAL_PP(val)) {
							ZVAL_STRING(new_val, "'t'", 1);
						}
						else {
							ZVAL_STRING(new_val, "'f'", 1);
						}
						break;

					case IS_NULL:
						ZVAL_STRING(new_val, "NULL", 1);
						break;

					default:
						err = 1;
				}
				MONETDB_CONV_CHECK_IGNORE();
				if (err) {
					php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Expects string, null, long or boolelan value for PostgreSQL '%s' (%s)", Z_STRVAL_PP(type), field);
				}
				break;
					
			case PG_OID:
			case PG_INT2:
			case PG_INT4:
			case PG_INT8:
				switch (Z_TYPE_PP(val)) {
					case IS_STRING:
						if (Z_STRLEN_PP(val) == 0) {
							ZVAL_STRING(new_val, "NULL", 1);
						}
						else {
							/* FIXME: better regex must be used */
							if (php_pgsql_convert_match(Z_STRVAL_PP(val), "^([+-]{0,1}[0-9]+)$", 0 TSRMLS_CC) == FAILURE) {
								err = 1;
							}
							else {
								ZVAL_STRING(new_val, Z_STRVAL_PP(val), 1);
							}
						}
						break;
						
					case IS_DOUBLE:
						ZVAL_DOUBLE(new_val, Z_DVAL_PP(val));
						convert_to_long_ex(&new_val);
						break;
						
					case IS_LONG:
						ZVAL_LONG(new_val, Z_LVAL_PP(val));
						break;
						
					case IS_NULL:
						ZVAL_STRING(new_val, "NULL", 1);
						break;

					default:
						err = 1;
				}
				MONETDB_CONV_CHECK_IGNORE();
				if (err) {
					php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Expects NULL, string, long or double value for pgsql '%s' (%s)", Z_STRVAL_PP(type), field);
				}
				break;

			case PG_NUMERIC:
			case PG_MONEY:
			case PG_FLOAT4:
			case PG_FLOAT8:
				switch (Z_TYPE_PP(val)) {
					case IS_STRING:
						if (Z_STRLEN_PP(val) == 0) {
							ZVAL_STRING(new_val, "NULL", 1);
						}
						else {
							/* FIXME: better regex must be used */
							if (php_pgsql_convert_match(Z_STRVAL_PP(val), "^([+-]{0,1}[0-9]+)|([+-]{0,1}[0-9]*[\\.][0-9]+)|([+-]{0,1}[0-9]+[\\.][0-9]*)$", 0 TSRMLS_CC) == FAILURE) {
								err = 1;
							}
							else {
								ZVAL_STRING(new_val, Z_STRVAL_PP(val), 1);
							}
						}
						break;
						
					case IS_LONG:
						ZVAL_LONG(new_val, Z_LVAL_PP(val));
						break;
						
					case IS_DOUBLE:
						ZVAL_DOUBLE(new_val, Z_DVAL_PP(val));
						break;
						
					case IS_NULL:
						ZVAL_STRING(new_val, "NULL", 1);
						break;

					default:
						err = 1;
				}
				MONETDB_CONV_CHECK_IGNORE();
				if (err) {
					php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Expects NULL, string, long or double value for PostgreSQL '%s' (%s)", Z_STRVAL_PP(type), field);
				}
				break;

			case PG_TEXT:
			case PG_CHAR:
			case PG_VARCHAR:
				switch (Z_TYPE_PP(val)) {
					case IS_STRING:
						if (Z_STRLEN_PP(val) == 0) {
							if (opt & MONETDB_CONV_FORCE_NULL) {
								ZVAL_STRING(new_val, "NULL", 1);
							} else {
								ZVAL_STRING(new_val, "''", 1);
							}
						}
						else {
							Z_TYPE_P(new_val) = IS_STRING;
							{
								char *tmp;
	 							tmp = (char *)safe_emalloc(Z_STRLEN_PP(val), 2, 1);
								Z_STRLEN_P(new_val) = (int)PQescapeString(tmp, Z_STRVAL_PP(val), Z_STRLEN_PP(val));
								Z_STRVAL_P(new_val) = tmp;
							}
							php_pgsql_add_quotes(new_val, 1 TSRMLS_CC);
						}
						break;
						
					case IS_LONG:
						ZVAL_LONG(new_val, Z_LVAL_PP(val));
						convert_to_string_ex(&new_val);
						break;
						
					case IS_DOUBLE:
						ZVAL_DOUBLE(new_val, Z_DVAL_PP(val));
						convert_to_string_ex(&new_val);
						break;

					case IS_NULL:
						ZVAL_STRING(new_val, "NULL", 1);
						break;

					default:
						err = 1;
				}
				MONETDB_CONV_CHECK_IGNORE();
				if (err) {
					php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Expects NULL, string, long or double value for PostgreSQL '%s' (%s)", Z_STRVAL_PP(type), field);
				}
				break;
					
			case PG_UNIX_TIME:
			case PG_UNIX_TIME_INTERVAL:
				/* these are the actallay a integer */
				switch (Z_TYPE_PP(val)) {
					case IS_STRING:
						if (Z_STRLEN_PP(val) == 0) {
							ZVAL_STRING(new_val, "NULL", 1);
						}
						else {
							/* FIXME: Better regex must be used */
							if (php_pgsql_convert_match(Z_STRVAL_PP(val), "^[0-9]+$", 0 TSRMLS_CC) == FAILURE) {
								err = 1;
							} 
							else {
								ZVAL_STRING(new_val, Z_STRVAL_PP(val), 1);
								convert_to_long_ex(&new_val);
							}
						}
						break;
						
					case IS_DOUBLE:
						ZVAL_DOUBLE(new_val, Z_DVAL_PP(val));
						convert_to_long_ex(&new_val);
						break;
						
					case IS_LONG:
						ZVAL_LONG(new_val, Z_LVAL_PP(val));
						break;
						
					case IS_NULL:
						ZVAL_STRING(new_val, "NULL", 1);
						break;

					default:
						err = 1;
				}
				MONETDB_CONV_CHECK_IGNORE();
				if (err) {
					php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Expects NULL, string, long or double value for '%s' (%s)", Z_STRVAL_PP(type), field);
				}
				break;
				
			case PG_CIDR:
			case PG_INET:
				switch (Z_TYPE_PP(val)) {
					case IS_STRING:
						if (Z_STRLEN_PP(val) == 0) {
							ZVAL_STRING(new_val, "NULL", 1);
						}
						else {
							/* FIXME: Better regex must be used */
							if (php_pgsql_convert_match(Z_STRVAL_PP(val), "^([0-9]{1,3}\\.){3}[0-9]{1,3}(/[0-9]{1,2}){0,1}$", 0 TSRMLS_CC) == FAILURE) {
								err = 1;
							}
							else {
								ZVAL_STRING(new_val, Z_STRVAL_PP(val), 1);
								php_pgsql_add_quotes(new_val, 1 TSRMLS_CC);
							}
						}
						break;
						
					case IS_NULL:
						ZVAL_STRING(new_val, "NULL", 1);
						break;

					default:
						err = 1;
				}				
				MONETDB_CONV_CHECK_IGNORE();
				if (err) {
					php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Expects NULL or string for '%s' (%s)", Z_STRVAL_PP(type), field);
				}
				break;
				
			case PG_TIME_WITH_TIMEZONE:
			case PG_TIMESTAMP:
			case PG_TIMESTAMP_WITH_TIMEZONE:
				switch(Z_TYPE_PP(val)) {
					case IS_STRING:
						if (Z_STRLEN_PP(val) == 0) {
							ZVAL_STRING(new_val, "NULL", 1);
						}
						else {
							/* FIXME: better regex must be used */
							if (php_pgsql_convert_match(Z_STRVAL_PP(val), "^([0-9]{4}[/-][0-9]{1,2}[/-][0-9]{1,2})([ \\t]+(([0-9]{1,2}:[0-9]{1,2}){1}(:[0-9]{1,2}){0,1}(\\.[0-9]+){0,1}([ \\t]*([+-][0-9]{1,2}(:[0-9]{1,2}){0,1}|[a-zA-Z]{1,5})){0,1})){0,1}$", 1 TSRMLS_CC) == FAILURE) {
								err = 1;
							}
							else {
								ZVAL_STRING(new_val, Z_STRVAL_PP(val), 1);
								php_pgsql_add_quotes(new_val, 1 TSRMLS_CC);
							}
						}
						break;
				
					case IS_NULL:
						ZVAL_STRING(new_val, "NULL", 1);
						break;

					default:
						err = 1;
				}
				MONETDB_CONV_CHECK_IGNORE();
				if (err) {
					php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Expects NULL or string for PostgreSQL %s field (%s)", Z_STRVAL_PP(type), field);
				}
				break;
				
			case PG_DATE:
				switch(Z_TYPE_PP(val)) {
					case IS_STRING:
						if (Z_STRLEN_PP(val) == 0) {
							ZVAL_STRING(new_val, "NULL", 1);
						}
						else {
							/* FIXME: better regex must be used */
							if (php_pgsql_convert_match(Z_STRVAL_PP(val), "^([0-9]{4}[/-][0-9]{1,2}[/-][0-9]{1,2})$", 1 TSRMLS_CC) == FAILURE) {
								err = 1;
							}
							else {
								ZVAL_STRING(new_val, Z_STRVAL_PP(val), 1);
								php_pgsql_add_quotes(new_val, 1 TSRMLS_CC);
							}
						}
						break;
				
					case IS_NULL:
						ZVAL_STRING(new_val, "NULL", 1);
						break;

					default:
						err = 1;
				}
				MONETDB_CONV_CHECK_IGNORE();
				if (err) {
					php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Expects NULL or string for PostgreSQL %s field (%s)", Z_STRVAL_PP(type), field);
				}
				break;

			case PG_TIME:
				switch(Z_TYPE_PP(val)) {
					case IS_STRING:
						if (Z_STRLEN_PP(val) == 0) {
							ZVAL_STRING(new_val, "NULL", 1);
						}
						else {
							/* FIXME: better regex must be used */
							if (php_pgsql_convert_match(Z_STRVAL_PP(val), "^(([0-9]{1,2}:[0-9]{1,2}){1}(:[0-9]{1,2}){0,1})){0,1}$", 1 TSRMLS_CC) == FAILURE) {
								err = 1;
							}
							else {
								ZVAL_STRING(new_val, Z_STRVAL_PP(val), 1);
								php_pgsql_add_quotes(new_val, 1 TSRMLS_CC);
							}
						}
						break;
				
					case IS_NULL:
						ZVAL_STRING(new_val, "NULL", 1);
						break;

					default:
						err = 1;
				}
				MONETDB_CONV_CHECK_IGNORE();
				if (err) {
					php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Expects NULL or string for PostgreSQL %s field (%s)", Z_STRVAL_PP(type), field);
				}
				break;

			case PG_INTERVAL:
				switch(Z_TYPE_PP(val)) {
					case IS_STRING:
						if (Z_STRLEN_PP(val) == 0) {
							ZVAL_STRING(new_val, "NULL", 1);
						}
						else {

							/* From the Postgres docs:

							   interval values can be written with the following syntax:
							   [@] quantity unit [quantity unit...] [direction]
							   
							   Where: quantity is a number (possibly signed); unit is second, minute, hour,
							   day, week, month, year, decade, century, millennium, or abbreviations or
							   plurals of these units [note not *all* abbreviations] ; direction can be
							   ago or empty. The at sign (@) is optional noise.
							   
							   ...
							   
							   Quantities of days, hours, minutes, and seconds can be specified without explicit
							   unit markings. For example, '1 12:59:10' is read the same as '1 day 12 hours 59 min 10
							   sec'.
							*/							  
							if (php_pgsql_convert_match(Z_STRVAL_PP(val),
														"^(@?[ \\t]+)?("
														
														/* Textual time units and their abbreviations: */
														"(([-+]?[ \\t]+)?"
														"[0-9]+(\\.[0-9]*)?[ \\t]*"
														"(millenniums|millennia|millennium|mil|mils|"
														"centuries|century|cent|c|"
														"decades|decade|dec|decs|"
														"years|year|y|"
														"months|month|mon|"
														"weeks|week|w|" 
														"days|day|d|"
														"hours|hour|hr|hrs|h|"
														"minutes|minute|mins|min|m|"
														"seconds|second|secs|sec|s))+|"

														/* Textual time units plus (dd)* hh[:mm[:ss]] */
														"((([-+]?[ \\t]+)?"
														"[0-9]+(\\.[0-9]*)?[ \\t]*"
														"(millenniums|millennia|millennium|mil|mils|"
														"centuries|century|cent|c|"
														"decades|decade|dec|decs|"
														"years|year|y|"
														"months|month|mon|"
														"weeks|week|w|"
														"days|day|d))+" 
														"([-+]?[ \\t]+"
														"([0-9]+[ \\t]+)+"				 /* dd */
														"(([0-9]{1,2}:){0,2}[0-9]{0,2})" /* hh:[mm:[ss]] */
														")?))"
														"([ \\t]+ago)?$",
														1 TSRMLS_CC) == FAILURE) {
								err = 1;
							}
							else {
								ZVAL_STRING(new_val, Z_STRVAL_PP(val), 1);
								php_pgsql_add_quotes(new_val, 1 TSRMLS_CC);
							}
						}	
						break;
				
					case IS_NULL:
						ZVAL_STRING(new_val, "NULL", 1);
						break;

					default:
						err = 1;
				}
				MONETDB_CONV_CHECK_IGNORE();
				if (err) {
					php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Expects NULL or string for PostgreSQL %s field (%s)", Z_STRVAL_PP(type), field);
				}
				break;
			case PG_BYTEA:
				switch (Z_TYPE_PP(val)) {
					case IS_STRING:
						if (Z_STRLEN_PP(val) == 0) {
							ZVAL_STRING(new_val, "NULL", 1);
						}
						else {
							unsigned char *tmp;
							size_t to_len;
							tmp = PQescapeBytea(Z_STRVAL_PP(val), Z_STRLEN_PP(val), &to_len);
							Z_TYPE_P(new_val) = IS_STRING;
							Z_STRLEN_P(new_val) = to_len-1; /* PQescapeBytea's to_len includes additional '\0' */
							Z_STRVAL_P(new_val) = emalloc(to_len);
							memcpy(Z_STRVAL_P(new_val), tmp, to_len);
							free(tmp);
							php_pgsql_add_quotes(new_val, 1 TSRMLS_CC);
								
						}
						break;
						
					case IS_LONG:
						ZVAL_LONG(new_val, Z_LVAL_PP(val));
						convert_to_string_ex(&new_val);
						break;
						
					case IS_DOUBLE:
						ZVAL_DOUBLE(new_val, Z_DVAL_PP(val));
						convert_to_string_ex(&new_val);
						break;

					case IS_NULL:
						ZVAL_STRING(new_val, "NULL", 1);
						break;

					default:
						err = 1;
				}
				MONETDB_CONV_CHECK_IGNORE();
				if (err) {
					php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Expects NULL, string, long or double value for PostgreSQL '%s' (%s)", Z_STRVAL_PP(type), field);
				}
				break;
				
			case PG_MACADDR:
				switch(Z_TYPE_PP(val)) {
					case IS_STRING:
						if (Z_STRLEN_PP(val) == 0) {
							ZVAL_STRING(new_val, "NULL", 1);
						}
						else {
							if (php_pgsql_convert_match(Z_STRVAL_PP(val), "^([0-9a-f]{2,2}:){5,5}[0-9a-f]{2,2}$", 1 TSRMLS_CC) == FAILURE) {
								err = 1;
							}
							else {
								ZVAL_STRING(new_val, Z_STRVAL_PP(val), 1);
								php_pgsql_add_quotes(new_val, 1 TSRMLS_CC);
							}
						}
						break;
				
					case IS_NULL:
						ZVAL_STRING(new_val, "NULL", 1);
						break;

					default:
						err = 1;
				}
				MONETDB_CONV_CHECK_IGNORE();
				if (err) {
					php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Expects NULL or string for PostgreSQL %s field (%s)", Z_STRVAL_PP(type), field);
				}
				break;

				/* bit */
			case PG_BIT:
			case PG_VARBIT:
				/* geometric */
			case PG_LINE:
			case PG_LSEG:
			case PG_POINT:
			case PG_BOX:
			case PG_PATH:
			case PG_POLYGON:
			case PG_CIRCLE:
				php_error_docref(NULL TSRMLS_CC, E_NOTICE, "PostgreSQL '%s' type (%s) is not supported", Z_STRVAL_PP(type), field);
				err = 1;
				break;
				
			case PG_UNKNOWN:
			default:
				php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Unknown or system data type '%s' for '%s'", Z_STRVAL_PP(type), field);
				err = 1;
				break;
		} /* switch */
		
		if (err && new_val) {
			zval_dtor(new_val);
			FREE_ZVAL(new_val);
			break; /* break out for() */
		}
		if (!skip_field) {
			/* If field is NULL and HAS DEFAULT, should be skipped */
			field = php_addslashes(field, strlen(field), &new_len, 0 TSRMLS_CC);
			add_assoc_zval(result, field, new_val);
			efree(field);
		}
	} /* for */
	zval_dtor(meta);
	FREE_ZVAL(meta);

	if (err) {
		/* shouldn't destroy & free zval here */
		return FAILURE;
	}
	return SUCCESS;
}
/* }}} */

/* {{{ proto array monetdb_convert(resource db, string table, array values[, int options])
   Check and convert values for PostgreSQL SQL statement */
PHP_FUNCTION(monetdb_convert)
{
	zval *pgsql_link, *values;
	char *table_name;
	int table_name_len;
	ulong option = 0;
	Mconn *pg_link;
	int id = -1;
	
	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC,
							  "rsa|l", &pgsql_link, &table_name, &table_name_len, &values, &option) == FAILURE) {
		return;
	}
	if (option & ~MONETDB_CONV_OPTS) {
		php_error_docref(NULL TSRMLS_CC, E_WARNING, "Invalid option is specified");
		RETURN_FALSE;
	}
	if (!table_name_len) {
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Table name is invalid");
		RETURN_FALSE;
	}

	ZEND_FETCH_RESOURCE2(pg_link, Mconn *, &pgsql_link, id, "PostgreSQL link", le_link, le_plink);

	if (php_pgsql_flush_query(pg_link TSRMLS_CC)) {
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Detected unhandled result(s) in connection");
	}
	array_init(return_value);
	if (php_pgsql_convert(pg_link, table_name, values, return_value, option TSRMLS_CC) == FAILURE) {
		zval_dtor(return_value);
		RETURN_FALSE;
	}
}
/* }}} */

//{{{ do_exec
static int do_exec(smart_str *querystr, int expect, Mconn *pg_link, ulong opt TSRMLS_DC)
{
	if (opt & MONETDB_DML_ASYNC) {
		if (PQsendQuery(pg_link, querystr->c)) {
			return 0;
		}
	}
	else {
		Mresult *monetdb_result_h;

		monetdb_result_h = PQexec(pg_link, querystr->c);
		if (PQresultStatus(monetdb_result_h) == expect) {
			Mclear(monetdb_result_h);
			return 0;
		} else {
			php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Failed to execute '%s'", querystr->c);
			Mclear(monetdb_result_h);
		}
	}

	return -1;
}
//}}}

/* {{{ php_monetdb_insert
 */
PHP_MONETDB_API int php_monetdb_insert(Mconn *pg_link, const char *table, zval *var_array, ulong opt, char **sql TSRMLS_DC)
{
	zval **val, *converted = NULL;
	char buf[256];
	char *fld;
	smart_str querystr = {0};
	int key_type, ret = FAILURE;
	uint fld_len;
	ulong num_idx;
	HashPosition pos;

	assert(pg_link != NULL);
	assert(table != NULL);
	assert(Z_TYPE_P(var_array) == IS_ARRAY);

	if (zend_hash_num_elements(Z_ARRVAL_P(var_array)) == 0) {
		return FAILURE;
	}

	/* convert input array if needed */
	if (!(opt & MONETDB_DML_NO_CONV)) {
		MAKE_STD_ZVAL(converted);
		array_init(converted);
		if (php_pgsql_convert(pg_link, table, var_array, converted, (opt & MONETDB_CONV_OPTS) TSRMLS_CC) == FAILURE) {
			goto cleanup;
		}
		var_array = converted;
	}
	
	smart_str_appends(&querystr, "INSERT INTO ");
	smart_str_appends(&querystr, table);
	smart_str_appends(&querystr, " (");
	
	zend_hash_internal_pointer_reset_ex(Z_ARRVAL_P(var_array), &pos);
	while ((key_type = zend_hash_get_current_key_ex(Z_ARRVAL_P(var_array), &fld,
					&fld_len, &num_idx, 0, &pos)) != HASH_KEY_NON_EXISTANT) {
		if (key_type == HASH_KEY_IS_LONG) {
			php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Expects associative array for values to be inserted");
			goto cleanup;
		}
		smart_str_appendl(&querystr, fld, fld_len - 1);
		smart_str_appendc(&querystr, ',');
		zend_hash_move_forward_ex(Z_ARRVAL_P(var_array), &pos);
	}
	querystr.len--;
	smart_str_appends(&querystr, ") VALUES (");
	
	/* make values string */
	for (zend_hash_internal_pointer_reset_ex(Z_ARRVAL_P(var_array), &pos);
		 zend_hash_get_current_data_ex(Z_ARRVAL_P(var_array), (void **)&val, &pos) == SUCCESS;
		 zend_hash_move_forward_ex(Z_ARRVAL_P(var_array), &pos)) {
		
		/* we can avoid the key_type check here, because we tested it in the other loop */
		switch(Z_TYPE_PP(val)) {
			case IS_STRING:
				smart_str_appendl(&querystr, Z_STRVAL_PP(val), Z_STRLEN_PP(val));
				break;
			case IS_LONG:
				smart_str_append_long(&querystr, Z_LVAL_PP(val));
				break;
			case IS_DOUBLE:
				smart_str_appendl(&querystr, buf, snprintf(buf, sizeof(buf), "%f", Z_DVAL_PP(val)));
				break;
			default:
				/* should not happen */
				php_error_docref(NULL TSRMLS_CC, E_WARNING, "Report this error to php-dev@lists.php.net, type = %d", Z_TYPE_PP(val));
				goto cleanup;
				break;
		}
		smart_str_appendc(&querystr, ',');
	}
	/* Remove the trailing "," */
	querystr.len--;
	smart_str_appends(&querystr, ");");
	smart_str_0(&querystr);

	if ((opt & (MONETDB_DML_EXEC|MONETDB_DML_ASYNC)) &&
		do_exec(&querystr, PGRES_COMMAND_OK, pg_link, (opt & MONETDB_CONV_OPTS) TSRMLS_CC) == 0) {
		ret = SUCCESS;
	}
	else if (opt & MONETDB_DML_STRING) {
		ret = SUCCESS;
	}
	
cleanup:
	if (!(opt & MONETDB_DML_NO_CONV)) {
		zval_dtor(converted);			
		FREE_ZVAL(converted);
	}
	if (ret == SUCCESS && (opt & MONETDB_DML_STRING)) {
		*sql = querystr.c;
	}
	else {
		smart_str_free(&querystr);
	}
	return ret;
}
/* }}} */

/* {{{ proto mixed monetdb_insert(resource db, string table, array values[, int options])
   Insert values (filed=>value) to table */
PHP_FUNCTION(monetdb_insert)
{
	zval *pgsql_link, *values;
	char *table, *sql = NULL;
	int table_len;
	ulong option = MONETDB_DML_EXEC;
	Mconn *pg_link;
	int id = -1, argc = ZEND_NUM_ARGS();

	if (zend_parse_parameters(argc TSRMLS_CC, "rsa|l",
							  &pgsql_link, &table, &table_len, &values, &option) == FAILURE) {
		return;
	}
	if (option & ~(MONETDB_CONV_OPTS|MONETDB_DML_NO_CONV|MONETDB_DML_EXEC|MONETDB_DML_ASYNC|MONETDB_DML_STRING)) {
		php_error_docref(NULL TSRMLS_CC, E_WARNING, "Invalid option is specified");
		RETURN_FALSE;
	}
	
	ZEND_FETCH_RESOURCE2(pg_link, Mconn *, &pgsql_link, id, "PostgreSQL link", le_link, le_plink);

	if (php_pgsql_flush_query(pg_link TSRMLS_CC)) {
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Detected unhandled result(s) in connection");
	}
	if (php_pgsql_insert(pg_link, table, values, option, &sql TSRMLS_CC) == FAILURE) {
		RETURN_FALSE;
	}
	if (option & MONETDB_DML_STRING) {
		RETURN_STRING(sql, 0);
	}
	RETURN_TRUE;
}
/* }}} */

//{{{ build_assignment_string
static inline int build_assignment_string(smart_str *querystr, HashTable *ht, const char *pad, int pad_len TSRMLS_DC)
{
	HashPosition pos;
	uint fld_len;
	int key_type;
	ulong num_idx;
	char *fld;
	char buf[256];
	zval **val;

	for (zend_hash_internal_pointer_reset_ex(ht, &pos);
		 zend_hash_get_current_data_ex(ht, (void **)&val, &pos) == SUCCESS;
		 zend_hash_move_forward_ex(ht, &pos)) {
		 key_type = zend_hash_get_current_key_ex(ht, &fld, &fld_len, &num_idx, 0, &pos);		
		if (key_type == HASH_KEY_IS_LONG) {
			php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Expects associative array for values to be inserted");
			return -1;
		}
		smart_str_appendl(querystr, fld, fld_len - 1);
		smart_str_appendc(querystr, '=');
		
		switch(Z_TYPE_PP(val)) {
			case IS_STRING:
				smart_str_appendl(querystr, Z_STRVAL_PP(val), Z_STRLEN_PP(val));
				break;
			case IS_LONG:
				smart_str_append_long(querystr, Z_LVAL_PP(val));
				break;
			case IS_DOUBLE:
				smart_str_appendl(querystr, buf, sprintf(buf, "%f", Z_DVAL_PP(val)));
				break;
			default:
				/* should not happen */
				php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Expects scaler values other than NULL. Need to convert?");
				return -1;
		}
		smart_str_appendl(querystr, pad, pad_len);
	}
	querystr->len -= pad_len;

	return 0;
}
//}}}

/* {{{ php_monetdb_update
 */
PHP_MONETDB_API int php_monetdb_update(Mconn *pg_link, const char *table, zval *var_array, zval *ids_array, ulong opt, char **sql TSRMLS_DC) 
{
	zval *var_converted = NULL, *ids_converted = NULL;
	smart_str querystr = {0};
	int ret = FAILURE;

	assert(pg_link != NULL);
	assert(table != NULL);
	assert(Z_TYPE_P(var_array) == IS_ARRAY);
	assert(Z_TYPE_P(ids_array) == IS_ARRAY);
	assert(!(opt & ~(MONETDB_CONV_OPTS|MONETDB_DML_NO_CONV|MONETDB_DML_EXEC|MONETDB_DML_STRING)));

	if (zend_hash_num_elements(Z_ARRVAL_P(var_array)) == 0
			|| zend_hash_num_elements(Z_ARRVAL_P(ids_array)) == 0) {
		return FAILURE;
	}

	if (!(opt & MONETDB_DML_NO_CONV)) {
		MAKE_STD_ZVAL(var_converted);
		array_init(var_converted);
		if (php_pgsql_convert(pg_link, table, var_array, var_converted, (opt & MONETDB_CONV_OPTS) TSRMLS_CC) == FAILURE) {
			goto cleanup;
		}
		var_array = var_converted;
		MAKE_STD_ZVAL(ids_converted);
		array_init(ids_converted);
		if (php_pgsql_convert(pg_link, table, ids_array, ids_converted, (opt & MONETDB_CONV_OPTS) TSRMLS_CC) == FAILURE) {
			goto cleanup;
		}
		ids_array = ids_converted;
	}

	smart_str_appends(&querystr, "UPDATE ");
	smart_str_appends(&querystr, table);
	smart_str_appends(&querystr, " SET ");

	if (build_assignment_string(&querystr, Z_ARRVAL_P(var_array), ",", 1 TSRMLS_CC))
		goto cleanup;
	
	smart_str_appends(&querystr, " WHERE ");
	
	if (build_assignment_string(&querystr, Z_ARRVAL_P(ids_array), " AND ", sizeof(" AND ")-1 TSRMLS_CC))
		goto cleanup;

	smart_str_appendc(&querystr, ';');	
	smart_str_0(&querystr);

	if ((opt & MONETDB_DML_EXEC) && do_exec(&querystr, PGRES_COMMAND_OK, pg_link, opt TSRMLS_CC) == 0) {
		ret = SUCCESS;
	} else if (opt & MONETDB_DML_STRING) {
		ret = SUCCESS;
	}

cleanup:
	if (var_converted) {
		zval_dtor(var_converted);
		FREE_ZVAL(var_converted);
	}
	if (ids_converted) {
		zval_dtor(ids_converted);
		FREE_ZVAL(ids_converted);
	}
	if (ret == SUCCESS && (opt & MONETDB_DML_STRING)) {
		*sql = querystr.c;
	}
	else {
		smart_str_free(&querystr);
	}
	return ret;
}
/* }}} */

/* {{{ proto mixed monetdb_update(resource db, string table, array fields, array ids[, int options])
   Update table using values (field=>value) and ids (id=>value) */
PHP_FUNCTION(monetdb_update)
{
	zval *pgsql_link, *values, *ids;
	char *table, *sql = NULL;
	int table_len;
	ulong option =  MONETDB_DML_EXEC;
	Mconn *pg_link;
	int id = -1, argc = ZEND_NUM_ARGS();

	if (zend_parse_parameters(argc TSRMLS_CC, "rsaa|l",
							  &pgsql_link, &table, &table_len, &values, &ids, &option) == FAILURE) {
		return;
	}
	if (option & ~(MONETDB_CONV_OPTS|MONETDB_DML_NO_CONV|MONETDB_DML_EXEC|MONETDB_DML_STRING)) {
		php_error_docref(NULL TSRMLS_CC, E_WARNING, "Invalid option is specified");
		RETURN_FALSE;
	}
	
	ZEND_FETCH_RESOURCE2(pg_link, Mconn *, &pgsql_link, id, "PostgreSQL link", le_link, le_plink);

	if (php_pgsql_flush_query(pg_link TSRMLS_CC)) {
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Detected unhandled result(s) in connection");
	}
	if (php_pgsql_update(pg_link, table, values, ids, option, &sql TSRMLS_CC) == FAILURE) {
		RETURN_FALSE;
	}
	if (option & MONETDB_DML_STRING) {
		RETURN_STRING(sql, 0);
	}
	RETURN_TRUE;
} 
/* }}} */

/* {{{ php_monetdb_delete
 */
PHP_MONETDB_API int php_monetdb_delete(Mconn *pg_link, const char *table, zval *ids_array, ulong opt, char **sql TSRMLS_DC) 
{
	zval *ids_converted = NULL;
	smart_str querystr = {0};
	int ret = FAILURE;

	assert(pg_link != NULL);
	assert(table != NULL);
	assert(Z_TYPE_P(ids_array) == IS_ARRAY);
	assert(!(opt & ~(MONETDB_CONV_FORCE_NULL|MONETDB_DML_EXEC|MONETDB_DML_STRING)));
	
	if (zend_hash_num_elements(Z_ARRVAL_P(ids_array)) == 0) {
		return FAILURE;
	}

	if (!(opt & MONETDB_DML_NO_CONV)) {
		MAKE_STD_ZVAL(ids_converted);
		array_init(ids_converted);
		if (php_pgsql_convert(pg_link, table, ids_array, ids_converted, (opt & MONETDB_CONV_OPTS) TSRMLS_CC) == FAILURE) {
			goto cleanup;
		}
		ids_array = ids_converted;
	}

	smart_str_appends(&querystr, "DELETE FROM ");
	smart_str_appends(&querystr, table);
	smart_str_appends(&querystr, " WHERE ");

	if (build_assignment_string(&querystr, Z_ARRVAL_P(ids_array), " AND ", sizeof(" AND ")-1 TSRMLS_CC))
		goto cleanup;

	smart_str_appendc(&querystr, ';');
	smart_str_0(&querystr);

	if ((opt & MONETDB_DML_EXEC) && do_exec(&querystr, PGRES_COMMAND_OK, pg_link, opt TSRMLS_CC) == 0) {
		ret = SUCCESS;
	} else if (opt & MONETDB_DML_STRING) {
		ret = SUCCESS;
	}

cleanup:
	if (!(opt & MONETDB_DML_NO_CONV)) {
		zval_dtor(ids_converted);			
		FREE_ZVAL(ids_converted);
	}
	if (ret == SUCCESS && (opt & MONETDB_DML_STRING)) {
		*sql = estrdup(querystr.c);
	}
	else {
		smart_str_free(&querystr);
	}
	return ret;
}
/* }}} */

/* {{{ proto mixed monetdb_delete(resource db, string table, array ids[, int options])
   Delete records has ids (id=>value) */
PHP_FUNCTION(monetdb_delete)
{
	zval *pgsql_link, *ids;
	char *table, *sql = NULL;
	int table_len;
	ulong option = MONETDB_DML_EXEC;
	Mconn *pg_link;
	int id = -1, argc = ZEND_NUM_ARGS();

	if (zend_parse_parameters(argc TSRMLS_CC, "rsa|l",
							  &pgsql_link, &table, &table_len, &ids, &option) == FAILURE) {
		return;
	}
	if (option & ~(MONETDB_CONV_FORCE_NULL|MONETDB_DML_NO_CONV|MONETDB_DML_EXEC|MONETDB_DML_STRING)) {
		php_error_docref(NULL TSRMLS_CC, E_WARNING, "Invalid option is specified");
		RETURN_FALSE;
	}
	
	ZEND_FETCH_RESOURCE2(pg_link, Mconn *, &pgsql_link, id, "PostgreSQL link", le_link, le_plink);

	if (php_pgsql_flush_query(pg_link TSRMLS_CC)) {
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Detected unhandled result(s) in connection");
	}
	if (php_pgsql_delete(pg_link, table, ids, option, &sql TSRMLS_CC) == FAILURE) {
		RETURN_FALSE;
	}
	if (option & MONETDB_DML_STRING) {
		RETURN_STRING(sql, 0);
	}
	RETURN_TRUE;
} 
/* }}} */

/* {{{ php_monetdb_result2array
 */
PHP_MONETDB_API int php_monetdb_result2array(Mresult *monetdb_result, zval *ret_array TSRMLS_DC) 
{
	zval *row;
	char *field_name, *element, *data;
	size_t num_fields, element_len, data_len;
	int pg_numrows, pg_row;
	uint i;
	assert(Z_TYPE_P(ret_array) == IS_ARRAY);

	if ((pg_numrows = PQntuples(monetdb_result)) <= 0) {
		return FAILURE;
	}
	for (pg_row = 0; pg_row < pg_numrows; pg_row++) {
		MAKE_STD_ZVAL(row);
		array_init(row);
		add_index_zval(ret_array, pg_row, row);
		for (i = 0, num_fields = PQnfields(monetdb_result); i < num_fields; i++) {
			if (PQgetisnull(monetdb_result, pg_row, i)) {
				field_name = PQfname(monetdb_result, i);
				add_assoc_null(row, field_name);
			} else {
				element = PQgetvalue(monetdb_result, pg_row, i);
				element_len = (element ? strlen(element) : 0);
				if (element) {
					data = safe_estrndup(element, element_len);
					data_len = element_len;

					field_name = PQfname(monetdb_result, i);
					add_assoc_stringl(row, field_name, data, data_len, 0);
				}
			}
		}
	}
	return SUCCESS;
}
/* }}} */

/* {{{ php_monetdb_select
 */
PHP_MONETDB_API int php_monetdb_select(Mconn *pg_link, const char *table, zval *ids_array, zval *ret_array, ulong opt, char **sql TSRMLS_DC) 
{
	zval *ids_converted = NULL;
	smart_str querystr = {0};
	int ret = FAILURE;
	Mresult *monetdb_result;

	assert(pg_link != NULL);
	assert(table != NULL);
	assert(Z_TYPE_P(ids_array) == IS_ARRAY);
	assert(Z_TYPE_P(ret_array) == IS_ARRAY);
	assert(!(opt & ~(MONETDB_CONV_OPTS|MONETDB_DML_NO_CONV|MONETDB_DML_EXEC|MONETDB_DML_ASYNC|MONETDB_DML_STRING)));
	
	if (zend_hash_num_elements(Z_ARRVAL_P(ids_array)) == 0) {
		return FAILURE;
	}

	if (!(opt & MONETDB_DML_NO_CONV)) {
		MAKE_STD_ZVAL(ids_converted);
		array_init(ids_converted);
		if (php_pgsql_convert(pg_link, table, ids_array, ids_converted, (opt & MONETDB_CONV_OPTS) TSRMLS_CC) == FAILURE) {
			goto cleanup;
		}
		ids_array = ids_converted;
	}

	smart_str_appends(&querystr, "SELECT * FROM ");
	smart_str_appends(&querystr, table);
	smart_str_appends(&querystr, " WHERE ");

	if (build_assignment_string(&querystr, Z_ARRVAL_P(ids_array), " AND ", sizeof(" AND ")-1 TSRMLS_CC))
		goto cleanup;

	smart_str_appendc(&querystr, ';');
	smart_str_0(&querystr);

	monetdb_result = PQexec(pg_link, querystr.c);
	if (PQresultStatus(monetdb_result) == PGRES_TUPLES_OK) {
		ret = php_pgsql_result2array(monetdb_result, ret_array TSRMLS_CC);
	} else {
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Failed to execute '%s'", querystr.c);
	}
	Mclear(monetdb_result_h);

cleanup:
	if (!(opt & MONETDB_DML_NO_CONV)) {
		zval_dtor(ids_converted);			
		FREE_ZVAL(ids_converted);
	}
	if (ret == SUCCESS && (opt & MONETDB_DML_STRING)) {
		*sql = querystr.c;
	}
	else {
		smart_str_free(&querystr);
	}
	return ret;
}
/* }}} */

/* {{{ proto mixed monetdb_select(resource db, string table, array ids[, int options])
   Select records that has ids (id=>value) */
PHP_FUNCTION(monetdb_select)
{
	zval *pgsql_link, *ids;
	char *table, *sql = NULL;
	int table_len;
	ulong option = MONETDB_DML_EXEC;
	Mconn *pg_link;
	int id = -1, argc = ZEND_NUM_ARGS();

	if (zend_parse_parameters(argc TSRMLS_CC, "rsa|l",
							  &pgsql_link, &table, &table_len, &ids, &option) == FAILURE) {
		return;
	}
	if (option & ~(MONETDB_CONV_FORCE_NULL|MONETDB_DML_NO_CONV|MONETDB_DML_EXEC|MONETDB_DML_ASYNC|MONETDB_DML_STRING)) {
		php_error_docref(NULL TSRMLS_CC, E_WARNING, "Invalid option is specified");
		RETURN_FALSE;
	}
	
	ZEND_FETCH_RESOURCE2(pg_link, Mconn *, &pgsql_link, id, "PostgreSQL link", le_link, le_plink);

	if (php_pgsql_flush_query(pg_link TSRMLS_CC)) {
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "Detected unhandled result(s) in connection");
	}
	array_init(return_value);
	if (php_pgsql_select(pg_link, table, ids, return_value, option, &sql TSRMLS_CC) == FAILURE) {
		zval_dtor(return_value);
		RETURN_FALSE;
	}
	if (option & MONETDB_DML_STRING) {
		zval_dtor(return_value);
		RETURN_STRING(sql, 0);
	}
	return;
} 
/* }}} */
#endif /* I_FEEL_LIKE_IMPLEMENTING_POSTGRES_EXPERIMENTAL_FUNCTIONS */

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: sw=4 ts=4 fdm=marker
 * vim<600: sw=4 ts=4
 */
