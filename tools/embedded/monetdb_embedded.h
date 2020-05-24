/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 * H. Muehleisen, M. Raasveldt
 * Inverse RAPI
 */
#ifndef _EMBEDDED_LIB_
#define _EMBEDDED_LIB_

#include "monetdb_config.h"
// #include "res_table.h"
#include "sql_catalog.h"
#include "gdk.h"

/* change api, do not expose internals !
typedef int64_t lng;
typedef uint64_t ulng;

typedef struct res_col {
  char *tn;
  char *name;
  //sql_subtype type;
  //bat b;
  //int mtype;
  //ptr *p;
} res_col;

typedef int bat;
typedef struct sql_table {
	char *table_name;
} sql_table;
*/

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

#ifdef WIN32
#if !defined(LIBEMBEDDED)
#define embedded_export extern __declspec(dllimport)
#else
#define embedded_export extern __declspec(dllexport)
#endif
#else
#define embedded_export extern
#endif

typedef struct {
	unsigned char day;
	unsigned char month;
	int year;
} monetdb_data_date;

typedef struct {
	unsigned int ms;
	unsigned char seconds;
	unsigned char minutes;
	unsigned char hours;
} monetdb_data_time;

typedef struct {
	monetdb_data_date date;
	monetdb_data_time time;
} monetdb_data_timestamp;

typedef struct {
	size_t size;
	void* data;
} monetdb_data_blob;

typedef enum  {
	monetdb_bool, monetdb_int8_t, monetdb_int16_t, monetdb_int32_t, monetdb_int64_t, monetdb_size_t,
	monetdb_float, monetdb_double,
	monetdb_str, monetdb_blob,
	monetdb_date, monetdb_time, monetdb_timestamp
} monetdb_types;

typedef struct {
	monetdb_types type;
	void *data;
	size_t count;
	char* name;
} monetdb_column;

typedef struct {
	lng nrows;
	size_t ncols;
	int type;
	lng id;
} monetdb_result;

typedef void* monetdb_connection;

#define DEFAULT_STRUCT_DEFINITION(ctype, typename)         \
	typedef struct                                     \
	{                                                  \
		monetdb_types type;                        \
		ctype *data;                               \
		size_t count;                              \
		char *name;				   \
		ctype null_value;                          \
		double scale;                              \
		int (*is_null)(ctype value);               \
	} monetdb_column_##typename

DEFAULT_STRUCT_DEFINITION(int8_t, bool);
DEFAULT_STRUCT_DEFINITION(int8_t, int8_t);
DEFAULT_STRUCT_DEFINITION(int16_t, int16_t);
DEFAULT_STRUCT_DEFINITION(int32_t, int32_t);
DEFAULT_STRUCT_DEFINITION(int64_t, int64_t);
// HUGE INT ?
DEFAULT_STRUCT_DEFINITION(size_t, size_t);

DEFAULT_STRUCT_DEFINITION(float, float);
DEFAULT_STRUCT_DEFINITION(double, double);

DEFAULT_STRUCT_DEFINITION(char *, str);
DEFAULT_STRUCT_DEFINITION(monetdb_data_blob, blob);

DEFAULT_STRUCT_DEFINITION(monetdb_data_date, date);
DEFAULT_STRUCT_DEFINITION(monetdb_data_time, time);
DEFAULT_STRUCT_DEFINITION(monetdb_data_timestamp, timestamp);
// UUID, INET, XML ?

embedded_export char* monetdb_connect(monetdb_connection *conn);
embedded_export char* monetdb_disconnect(monetdb_connection conn);
embedded_export char* monetdb_startup(char* dbdir, bool sequential);
embedded_export bool  monetdb_is_initialized(void);

embedded_export char* monetdb_get_autocommit(monetdb_connection conn, int* result);
embedded_export char* monetdb_set_autocommit(monetdb_connection conn, int value);
/* TODO split query in prepare/bind/execute */
embedded_export char* monetdb_query(monetdb_connection conn, char* query, monetdb_result** result, lng* affected_rows, int* prepare_id);

embedded_export char* monetdb_result_fetch(monetdb_connection conn, monetdb_column** res, monetdb_result* mres, size_t column_index);
embedded_export char* monetdb_result_fetch_rawcol(monetdb_connection conn, res_col** res, monetdb_result* mres, size_t column_index);

embedded_export char* monetdb_clear_prepare(monetdb_connection conn, int id);
embedded_export char* monetdb_send_close(monetdb_connection conn, int id);

embedded_export char* monetdb_append(monetdb_connection conn, const char* schema, const char* table, bat *batids, size_t column_count);
embedded_export char* monetdb_cleanup_result(monetdb_connection conn, monetdb_result* result);
embedded_export char* monetdb_get_table(monetdb_connection conn, sql_table** table, const char* schema_name, const char* table_name);
embedded_export char* monetdb_get_columns(monetdb_connection conn, const char* schema_name, const char *table_name, size_t *column_count, char ***column_names, int **column_types);

embedded_export char* monetdb_shutdown(void);

#ifdef __cplusplus
}
#endif


#endif
