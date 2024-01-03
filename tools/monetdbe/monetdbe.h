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

#ifndef _MONETDBE_LIB_
#define _MONETDBE_LIB_

#include "monetdb_config.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

#ifdef WIN32
#ifndef LIBMONETDBE
#define monetdbe_export extern __declspec(dllimport)
#else
#define monetdbe_export extern __declspec(dllexport)
#endif
#else
#define monetdbe_export extern
#endif

typedef int64_t monetdbe_cnt;

typedef struct {
	unsigned char day;
	unsigned char month;
	short year;
} monetdbe_data_date;

typedef struct {
	unsigned int ms;
	unsigned char seconds;
	unsigned char minutes;
	unsigned char hours;
} monetdbe_data_time;

typedef struct {
	monetdbe_data_date date;
	monetdbe_data_time time;
} monetdbe_data_timestamp;

typedef struct {
	size_t size;
	char* data;
} monetdbe_data_blob;

typedef enum  {
	monetdbe_bool, monetdbe_int8_t, monetdbe_int16_t, monetdbe_int32_t, monetdbe_int64_t,
#ifdef HAVE_HGE
	monetdbe_int128_t,
#endif
	monetdbe_size_t, monetdbe_float, monetdbe_double,
	monetdbe_str, monetdbe_blob,
	monetdbe_date, monetdbe_time, monetdbe_timestamp,

	// should be last:
	monetdbe_type_unknown
} monetdbe_types;

typedef struct {
	char* name;
	unsigned int scale;
	unsigned int digits;
} monetdbe_sql_type;

typedef struct {
	monetdbe_types type;
	monetdbe_sql_type sql_type;
	void *data;
	size_t count;
	char* name;
} monetdbe_column;

typedef struct {
	size_t nparam;
	monetdbe_types  *type;
} monetdbe_statement;

typedef struct {
	monetdbe_cnt nrows;
	size_t ncols;
	char *name;
	monetdbe_cnt last_id;		/* last auto incremented id */
} monetdbe_result;

typedef void* monetdbe_database;

typedef struct {
	const char *host;
	int port;
	const char *database;
	const char *username;
	const char *password;
	const char *lang;
} monetdbe_remote;

typedef struct {
	const char *host;
	const char* port;
	const char* usock;
} monetdbe_mapi_server;

typedef struct {
	int memorylimit;  // top off the amount of RAM to be used, in MB
	int querytimeout;  // graceful terminate query after a few seconds
	int sessiontimeout;  // graceful terminate the session after a few seconds
	int nr_threads;  // maximum number of worker treads, limits level of parallelism
	monetdbe_remote* remote;
	monetdbe_mapi_server* mapi_server;
	const char *trace_file;		// file to which log output should be written
} monetdbe_options;

#define DEFAULT_STRUCT_DEFINITION(ctype, typename)         \
	typedef struct                                     \
	{                                                  \
		monetdbe_types type;                        \
		monetdbe_sql_type sql_type;                \
		ctype *data;                               \
		size_t count;                              \
		char *name;				   \
		ctype null_value;                          \
		double scale;                              \
		int (*is_null)(ctype *value);               \
	} monetdbe_column_##typename

DEFAULT_STRUCT_DEFINITION(int8_t, bool);
DEFAULT_STRUCT_DEFINITION(int8_t, int8_t);
DEFAULT_STRUCT_DEFINITION(int16_t, int16_t);
DEFAULT_STRUCT_DEFINITION(int32_t, int32_t);
DEFAULT_STRUCT_DEFINITION(int64_t, int64_t);
#ifdef HAVE_HGE
DEFAULT_STRUCT_DEFINITION(__int128, int128_t);
#endif
DEFAULT_STRUCT_DEFINITION(size_t, size_t);

DEFAULT_STRUCT_DEFINITION(float, float);
DEFAULT_STRUCT_DEFINITION(double, double);

DEFAULT_STRUCT_DEFINITION(char *, str);
DEFAULT_STRUCT_DEFINITION(monetdbe_data_blob, blob);

DEFAULT_STRUCT_DEFINITION(monetdbe_data_date, date);
DEFAULT_STRUCT_DEFINITION(monetdbe_data_time, time);
DEFAULT_STRUCT_DEFINITION(monetdbe_data_timestamp, timestamp);
// UUID, INET, XML ?

monetdbe_export const char *monetdbe_version(void);

monetdbe_export int   monetdbe_open(monetdbe_database *db, char *url, monetdbe_options *opts);
/* 0 ok, -1 (allocation failed),  -2 error in db */
monetdbe_export int   monetdbe_close(monetdbe_database db);

monetdbe_export char* monetdbe_error(monetdbe_database db);

monetdbe_export char* monetdbe_get_autocommit(monetdbe_database dbhdl, int* result);
monetdbe_export char* monetdbe_set_autocommit(monetdbe_database dbhdl, int value);
monetdbe_export int   monetdbe_in_transaction(monetdbe_database dbhdl);

monetdbe_export char* monetdbe_query(monetdbe_database dbhdl, char* query, monetdbe_result** result, monetdbe_cnt* affected_rows);
monetdbe_export char* monetdbe_result_fetch(monetdbe_result *mres, monetdbe_column** res, size_t column_index);
monetdbe_export char* monetdbe_cleanup_result(monetdbe_database dbhdl, monetdbe_result* result);

monetdbe_export char* monetdbe_prepare(monetdbe_database dbhdl, char *query, monetdbe_statement **stmt, monetdbe_result** result);
monetdbe_export char* monetdbe_bind(monetdbe_statement *stmt, void *data, size_t parameter_nr);
monetdbe_export char* monetdbe_execute(monetdbe_statement *stmt, monetdbe_result **result, monetdbe_cnt* affected_rows);
monetdbe_export char* monetdbe_cleanup_statement(monetdbe_database dbhdl, monetdbe_statement *stmt);

monetdbe_export char* monetdbe_append(monetdbe_database dbhdl, const char* schema, const char* table, monetdbe_column **input, size_t column_count);
monetdbe_export const void* monetdbe_null(monetdbe_database dbhdl, monetdbe_types t);

monetdbe_export char* monetdbe_get_columns(monetdbe_database dbhdl, const char* schema_name, const char *table_name, size_t *column_count, monetdbe_column **columns);

monetdbe_export char* monetdbe_dump_database(monetdbe_database dbhdl, const char *backupfile);
monetdbe_export char* monetdbe_dump_table(monetdbe_database dbhdl, const char *schema_name, const char *table_name, const char *backupfile);
monetdbe_export const char* monetdbe_get_mapi_port(void);

#ifdef __cplusplus
}
#endif


#endif
