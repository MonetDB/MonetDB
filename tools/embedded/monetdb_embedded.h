/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#ifndef _EMBEDDED_LIB_
#define _EMBEDDED_LIB_

#include "monetdb_config.h"

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

typedef int64_t monetdb_cnt;

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
	char* data;
} monetdb_data_blob;

typedef enum  {
	monetdb_bool, monetdb_int8_t, monetdb_int16_t, monetdb_int32_t, monetdb_int64_t, 
#if HAVE_HGE
	monetdb_int128_t, 
#endif
	monetdb_size_t, monetdb_float, monetdb_double,
	monetdb_str, monetdb_blob,
	monetdb_date, monetdb_time, monetdb_timestamp
} monetdb_types;

typedef struct {
	monetdb_types type;
	void *data;
	size_t count;
	char* name;
} monetdb_column;

struct monetdb_table_t;
typedef struct monetdb_table_t monetdb_table;

typedef struct {
	monetdb_cnt nrows;
	size_t ncols;
	int type;
	monetdb_cnt id;
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
#if HAVE_HGE
DEFAULT_STRUCT_DEFINITION(__int128, int128_t);
#endif
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
embedded_export int   monetdb_in_transaction(monetdb_connection conn);

/* TODO split query in prepare/bind/execute */
embedded_export char* monetdb_query(monetdb_connection conn, char* query, monetdb_result** result, monetdb_cnt* affected_rows, int* prepare_id);
embedded_export char* monetdb_result_fetch(monetdb_connection conn, monetdb_result *mres, monetdb_column** res, size_t column_index);

embedded_export char* monetdb_append(monetdb_connection conn, const char* schema, const char* table, monetdb_column **input, size_t column_count);

embedded_export char* monetdb_cleanup_result(monetdb_connection conn, monetdb_result* result);
embedded_export char* monetdb_get_table(monetdb_connection conn, monetdb_table** table, const char* schema_name, const char* table_name);
/* TODO add get table info functions */
embedded_export char* monetdb_get_columns(monetdb_connection conn, const char* schema_name, const char *table_name, size_t *column_count, char ***column_names, int **column_types);

embedded_export char* monetdb_shutdown(void);

#ifdef __cplusplus
}
#endif


#endif
