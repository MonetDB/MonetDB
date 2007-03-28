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
   |          Fabian Groffen <fabian@cwi.nl> (monetdb version)            |
   +----------------------------------------------------------------------+
 */
 
/* $Id$ */

#ifndef PHP_MONETDB_H
#define PHP_MONETDB_H

extern zend_module_entry monetdb_module_entry;
#define monetdb_module_ptr &monetdb_module_entry

# ifdef PHP_WIN32
#  define INV_WRITE            0x00020000
#  define INV_READ             0x00040000
#  undef PHP_MONETDB_API
#  ifdef MONETDB_EXPORTS
#   define PHP_MONETDB_API __declspec(dllexport)
#  else
#   define PHP_MONETDB_API __declspec(dllimport)
#  endif
# else
#  define PHP_MONETDB_API /* nothing special */
# endif

# ifdef HAVE_MONETDB_WITH_MULTIBYTE_SUPPORT
const char * pg_encoding_to_char(int encoding);
# endif

PHP_MINIT_FUNCTION(monetdb);
PHP_MSHUTDOWN_FUNCTION(monetdb);
PHP_RINIT_FUNCTION(monetdb);
PHP_RSHUTDOWN_FUNCTION(monetdb);
PHP_MINFO_FUNCTION(monetdb);
/* connection functions */
PHP_FUNCTION(monetdb_connect);
PHP_FUNCTION(monetdb_pconnect);
PHP_FUNCTION(monetdb_close);
PHP_FUNCTION(monetdb_connection_reset);
PHP_FUNCTION(monetdb_connection_status);
PHP_FUNCTION(monetdb_connection_busy);
PHP_FUNCTION(monetdb_host);
PHP_FUNCTION(monetdb_dbname);
PHP_FUNCTION(monetdb_version);
PHP_FUNCTION(monetdb_ping);
/* query functions */
PHP_FUNCTION(monetdb_query);
#ifdef I_FEEL_LIKE_IMPLEMENTING_PREPARED_STUFF
PHP_FUNCTION(monetdb_query_params);
PHP_FUNCTION(monetdb_prepare);
PHP_FUNCTION(monetdb_execute);
PHP_FUNCTION(monetdb_send_query);
PHP_FUNCTION(monetdb_send_query_params);
PHP_FUNCTION(monetdb_send_prepare);
PHP_FUNCTION(monetdb_send_execute);
PHP_FUNCTION(monetdb_get_result);
PHP_FUNCTION(monetdb_result_status);
#endif
/* result functions */
PHP_FUNCTION(monetdb_fetch_result);
PHP_FUNCTION(monetdb_fetch_row);
PHP_FUNCTION(monetdb_fetch_assoc);
PHP_FUNCTION(monetdb_fetch_array);
PHP_FUNCTION(monetdb_fetch_object);
PHP_FUNCTION(monetdb_affected_rows);
PHP_FUNCTION(monetdb_result_seek);
PHP_FUNCTION(monetdb_free_result);
PHP_FUNCTION(monetdb_num_rows);
PHP_FUNCTION(monetdb_num_fields);
PHP_FUNCTION(monetdb_field_name);
PHP_FUNCTION(monetdb_field_table);
PHP_FUNCTION(monetdb_field_type);
PHP_FUNCTION(monetdb_field_num);
PHP_FUNCTION(monetdb_field_prtlen);
PHP_FUNCTION(monetdb_field_is_null);
/* error message functions */
PHP_FUNCTION(monetdb_last_error);
PHP_FUNCTION(monetdb_last_notice);
/* copy functions */
#ifdef I_FEEL_LIKE_IMPLEMENTING_COPY_INTO_PROPERLY
PHP_FUNCTION(monetdb_put_line);
PHP_FUNCTION(monetdb_end_copy);
PHP_FUNCTION(monetdb_copy_to);
PHP_FUNCTION(monetdb_copy_from);
#endif
/* utility functions */
PHP_FUNCTION(monetdb_escape_string);
/* misc functions */
#ifdef I_FEEL_LIKE_IMPLEMENTING_POSTGRES_EXPERIMENTAL_FUNCTIONS
PHP_FUNCTION(monetdb_meta_data);
PHP_FUNCTION(monetdb_convert);
PHP_FUNCTION(monetdb_insert);
PHP_FUNCTION(monetdb_update);
PHP_FUNCTION(monetdb_delete);
PHP_FUNCTION(monetdb_select);
#endif

/* connection options - TODO: Add async connection option */
#define MONETDB_CONNECT_FORCE_NEW     (1<<1)
/* php_monetdb_convert options */
#define MONETDB_CONV_IGNORE_DEFAULT   (1<<1)     /* Do not use DEAFULT value by removing field from returned array */
#define MONETDB_CONV_FORCE_NULL       (1<<2)     /* Convert to NULL if string is null string */
#define MONETDB_CONV_IGNORE_NOT_NULL  (1<<3)     /* Ignore NOT NULL constraints */
#define MONETDB_CONV_OPTS             (MONETDB_CONV_IGNORE_DEFAULT|MONETDB_CONV_FORCE_NULL|MONETDB_CONV_IGNORE_NOT_NULL)
/* php_monetdb_insert/update/select/delete options */
#define MONETDB_DML_NO_CONV           (1<<8)     /* Do not call php_monetdb_convert() */
#define MONETDB_DML_EXEC              (1<<9)     /* Execute query */
#define MONETDB_DML_ASYNC             (1<<10)    /* Do async query */
#define MONETDB_DML_STRING            (1<<11)    /* Return query string */

/* avoid redeclaration */
#ifdef _POSIX_C_SOURCE
# undef _POSIX_C_SOURCE
#endif

#include "Mapi.h"
/* typedefs for convenience; not using Mapi's as those already are a
 * pointer to the struct*/
typedef struct MapiStruct Mconn;
typedef struct MapiStatement Mresult;

/* exported functions */
PHP_MONETDB_API int php_monetdb_meta_data(Mconn *m_link, const char *table_name, zval *meta TSRMLS_DC);
PHP_MONETDB_API int php_monetdb_convert(Mconn *m_link, const char *table_name, const zval *values, zval *result, ulong opt TSRMLS_DC);
PHP_MONETDB_API int php_monetdb_insert(Mconn *m_link, const char *table, zval *values, ulong opt, char **sql TSRMLS_DC);
PHP_MONETDB_API int php_monetdb_update(Mconn *m_link, const char *table, zval *values, zval *ids, ulong opt , char **sql TSRMLS_DC);
PHP_MONETDB_API int php_monetdb_delete(Mconn *m_link, const char *table, zval *ids, ulong opt, char **sql TSRMLS_DC);
PHP_MONETDB_API int php_monetdb_select(Mconn *m_link, const char *table, zval *ids, zval *ret_array, ulong opt, char **sql  TSRMLS_DC);
PHP_MONETDB_API int php_monetdb_result2array(Mresult *m_result, zval *ret_array TSRMLS_DC);

/* internal functions */
static void php_monetdb_do_connect(INTERNAL_FUNCTION_PARAMETERS,int persistent);
static void php_monetdb_get_link_info(INTERNAL_FUNCTION_PARAMETERS, int entry_type);
static void php_monetdb_get_result_info(INTERNAL_FUNCTION_PARAMETERS, int entry_type);
static void php_monetdb_get_field_info(INTERNAL_FUNCTION_PARAMETERS, int entry_type);
static void php_monetdb_data_info(INTERNAL_FUNCTION_PARAMETERS, int entry_type);
#ifdef I_FEEL_LIKE_IMPLEMENTING_PREPARED_STUFF
static void php_monetdb_do_async(INTERNAL_FUNCTION_PARAMETERS,int entry_type);
#endif

typedef enum _php_monetdb_data_type {
	/* boolean */
	M_BOOL,
	/* number */
	M_OID,
	M_INT2,
	M_INT4,
	M_INT8,
	M_FLOAT4,
	M_FLOAT8,
	M_NUMERIC,
	M_MONEY,
	/* character */
	M_TEXT,
	M_CHAR,
	M_VARCHAR,
	/* time and interval */
	M_UNIX_TIME,
	M_UNIX_TIME_INTERVAL,
	M_DATE,
	M_TIME,
	M_TIME_WITH_TIMEZONE,
	M_TIMESTAMP,
	M_TIMESTAMP_WITH_TIMEZONE,
	M_INTERVAL,
	/* binary */
	M_BYTEA,
	/* network */
	M_CIDR,
	M_INET,
	M_MACADDR,
	/* bit */
	M_BIT,
	M_VARBIT,
	/* geometoric */
	M_LINE,
	M_LSEG,
	M_POINT,
	M_BOX,
	M_PATH,
	M_POLYGON,
	M_CIRCLE,
	/* unkown and system */
	M_UNKNOWN
} php_monetdb_data_type;

typedef struct mLofp {
	Mconn *conn;
	int lofd;
} mLofp;

typedef struct _php_monetdb_result_handle {
	Mconn *conn;
	Mresult *result;
	int row;
} php_monetdb_result_handle;

typedef struct _php_monetdb_notice {
	char *message;
	int len;
} php_monetdb_notice;

ZEND_BEGIN_MODULE_GLOBALS(monetdb)
	long default_link; /* default link when connection is omitted */
	long num_links,num_persistent;
	long max_links,max_persistent;
	long allow_persistent;
	long auto_reset_persistent;
	int le_lofp,le_string;
	int ignore_notices,log_notices;
	HashTable errors;  /* error message for each connection */
	HashTable notices;  /* notice message for each connection */
	char* default_hostname;
	char* default_username;
	char* default_password;
	char* default_language;
	int default_port;
ZEND_END_MODULE_GLOBALS(monetdb)

ZEND_EXTERN_MODULE_GLOBALS(monetdb)

#ifdef ZTS
# define MG(v) TSRMG(monetdb_globals_id, zend_monetdb_globals *, v)
#else
# define MG(v) (monetdb_globals.v)
#endif

#define phpext_monetdb_ptr monetdb_module_ptr

#endif /* PHP_MONETDB_H */
