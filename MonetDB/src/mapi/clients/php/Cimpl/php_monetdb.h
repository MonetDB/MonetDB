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
 * Portions created by CWI are Copyright (C) 1997-2004 CWI.
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

#ifndef PHP_MONETDB_H
#define PHP_MONETDB_H

extern zend_module_entry monetdb_module_entry;

#define phpext_monetdb_ptr &monetdb_module_entry

#ifdef PHP_WIN32
#define PHP_MONET_API __declspec(dllexport)
#else
#define PHP_MONET_API
#endif

#ifdef ZTS
#include "TSRM.h"
#endif

PHP_MINIT_FUNCTION(monetdb);
PHP_MSHUTDOWN_FUNCTION(monetdb);
PHP_RINIT_FUNCTION(monetdb);
PHP_RSHUTDOWN_FUNCTION(monetdb);
PHP_MINFO_FUNCTION(monetdb);

PHP_FUNCTION(monetdb_connect);
PHP_FUNCTION(monetdb_close);
PHP_FUNCTION(monetdb_query);
PHP_FUNCTION(monetdb_num_rows);
PHP_FUNCTION(monetdb_num_fields);
PHP_FUNCTION(monetdb_next_result);
PHP_FUNCTION(monetdb_field_name);
PHP_FUNCTION(monetdb_field_type);
PHP_FUNCTION(monetdb_errno);
PHP_FUNCTION(monetdb_error);
PHP_FUNCTION(monetdb_fetch_array);
PHP_FUNCTION(monetdb_fetch_assoc);
PHP_FUNCTION(monetdb_fetch_object);
PHP_FUNCTION(monetdb_fetch_row);
PHP_FUNCTION(monetdb_free_result);
PHP_FUNCTION(monetdb_data_seek);
PHP_FUNCTION(monetdb_escape_string);
PHP_FUNCTION(monetdb_ping);
PHP_FUNCTION(monetdb_info);

/*
static void php_monetdb_fetch_row(
    INTERNAL_FUNCTION_PARAMETERS,
    const char *function_name,
    int type);
*/

/* 
  	Declare any global variables you may need between the BEGIN
	and END macros here:     
*/

ZEND_BEGIN_MODULE_GLOBALS(monetdb)
	long default_link; /* default mapi used */
	long default_handle; /* default query handle used */
	long default_port;
	char *default_language;
	char *default_hostname;
	char *default_username;
	char *default_password;
    long query_timeout;
ZEND_END_MODULE_GLOBALS(monetdb)

/* In every utility function you add that needs to use variables 
   in php_monetdb_globals, call TSRM_FETCH(); after declaring other 
   variables used by that function, or better yet, pass in TSRMLS_CC
   after the last function argument and declare your utility function
   with TSRMLS_DC after the last declared argument.  Always refer to
   the globals in your function as MONET_G(variable).  You are 
   encouraged to rename these macros something shorter, see
   examples in any other php module directory.
*/

#ifdef ZTS
#define MONET_G(v) TSRMG(monetdb_globals_id, zend_monetdb_globals *, v)
#else
#define MONET_G(v) (monetdb_globals.v)
#endif

#endif	/* PHP_MONETDB_H */

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * indent-tabs-mode: t
 * End:
 */
