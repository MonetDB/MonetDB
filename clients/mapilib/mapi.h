/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _MAPI_H_INCLUDED
#define _MAPI_H_INCLUDED 1

#include <stdio.h>		/* for FILE * */

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

#define MAPI_SEEK_SET	0
#define MAPI_SEEK_CUR	1
#define MAPI_SEEK_END	2

#define MAPI_TRACE	1
#define MAPI_TRACE_LANG	2

typedef int MapiMsg;

#define MOK		0
#define MERROR		(-1)
#define MTIMEOUT	(-2)
#define MMORE		(-3)
#define MSERVER		(-4)

#define LANG_MAL	0
#define LANG_SQL	2
#define LANG_PROFILER	3

/* prompts for MAPI protocol, also in monetdb_config.h.in */
#define PROMPTBEG	'\001'	/* start prompt bracket */
#define PROMPT1		"\001\001\n"	/* prompt: ready for new query */
#define PROMPT2		"\001\002\n"	/* prompt: more data needed */

/*
 * The table field information is extracted from the table headers
 * obtained from the server. This list may be extended in the future.
 * The type of both the 'param' and 'binding'
 * variables refer to their underlying C-type. They are used for
 * automatic type coercion between back-end and application.
 */
typedef struct MapiStruct *Mapi;

/* this definition is a straight copy from sql/include/sql_query.h */
typedef enum sql_query_t {
	Q_PARSE = 0,
	Q_TABLE = 1,
	Q_UPDATE = 2,
	Q_SCHEMA = 3,
	Q_TRANS = 4,
	Q_PREPARE = 5,
	Q_BLOCK = 6
} sql_query_t;

typedef struct MapiStatement *MapiHdl;

#ifdef __cplusplus
extern "C" {
#endif

/* avoid using "#ifdef WIN32" so that this file does not need our config.h */
#if defined(_MSC_VER) || defined(__CYGWIN__) || defined(__MINGW32__)
#ifndef LIBMAPI
#define mapi_export extern __declspec(dllimport)
#else
#define mapi_export extern __declspec(dllexport)
#endif
#else
#define mapi_export extern
#endif

#if defined(_MSC_VER)
/* Microsoft & Intel compilers under Windows have type __int64 */
typedef unsigned __int64 mapi_uint64;
typedef __int64 mapi_int64;
#else
/* gcc and other (Unix-) compilers (usually) have type long long */
typedef unsigned long long mapi_uint64;
typedef long long mapi_int64;
#endif

/* three structures used for communicating date/time information */
/* these structs are deliberately compatible with the ODBC versions
   SQL_DATE_STRUCT, SQL_TIME_STRUCT, and SQL_TIMESTAMP_STRUCT */
typedef struct {		/* used by MAPI_DATE */
	short year;
	unsigned short month;
	unsigned short day;
} MapiDate;

typedef struct {		/* used by MAPI_TIME */
	unsigned short hour;
	unsigned short minute;
	unsigned short second;
} MapiTime;

typedef struct {		/* used by MAPI_DATETIME */
	short year;
	unsigned short month;
	unsigned short day;
	unsigned short hour;
	unsigned short minute;
	unsigned short second;
	unsigned int fraction;	/* in 1000 millionths of a second (10e-9) */
} MapiDateTime;

/* connection-oriented functions */
mapi_export Mapi mapi_mapi(const char *host, int port, const char *username, const char *password, const char *lang, const char *dbname);
mapi_export Mapi mapi_mapiuri(const char *url, const char *user, const char *pass, const char *lang);
mapi_export MapiMsg mapi_destroy(Mapi mid);
mapi_export MapiMsg mapi_start_talking(Mapi mid);
mapi_export Mapi mapi_connect(const char *host, int port, const char *username, const char *password, const char *lang, const char *dbname);
mapi_export char **mapi_resolve(const char *host, int port, const char *pattern);
mapi_export MapiMsg mapi_disconnect(Mapi mid);
mapi_export MapiMsg mapi_reconnect(Mapi mid);
mapi_export MapiMsg mapi_ping(Mapi mid);

mapi_export MapiMsg mapi_error(Mapi mid);
mapi_export char *mapi_error_str(Mapi mid);
mapi_export void mapi_noexplain(Mapi mid, char *errorprefix);
mapi_export MapiMsg mapi_explain(Mapi mid, FILE *fd);
mapi_export MapiMsg mapi_explain_query(MapiHdl hdl, FILE *fd);
mapi_export MapiMsg mapi_explain_result(MapiHdl hdl, FILE *fd);
mapi_export MapiMsg mapi_trace(Mapi mid, int flag);
#ifdef ST_READ			/* if stream.h was included */
mapi_export stream *mapi_get_from(Mapi mid);
mapi_export stream *mapi_get_to(Mapi mid);
#endif
mapi_export int mapi_get_trace(Mapi mid);
mapi_export int mapi_get_autocommit(Mapi mid);
mapi_export MapiMsg mapi_log(Mapi mid, const char *nme);
mapi_export MapiMsg mapi_setAutocommit(Mapi mid, int autocommit);
mapi_export MapiMsg mapi_set_size_header(Mapi mid, int value);
mapi_export MapiMsg mapi_release_id(Mapi mid, int id);
mapi_export char *mapi_result_error(MapiHdl hdl);
mapi_export MapiMsg mapi_next_result(MapiHdl hdl);
mapi_export MapiMsg mapi_needmore(MapiHdl hdl);
mapi_export int mapi_more_results(MapiHdl hdl);
mapi_export MapiHdl mapi_new_handle(Mapi mid);
mapi_export MapiMsg mapi_close_handle(MapiHdl hdl);
mapi_export MapiMsg mapi_bind(MapiHdl hdl, int fnr, char **ptr);
mapi_export MapiMsg mapi_bind_var(MapiHdl hdl, int fnr, int type, void *ptr);
mapi_export MapiMsg mapi_bind_numeric(MapiHdl hdl, int fnr, int scale, int precision, void *ptr);
mapi_export MapiMsg mapi_clear_bindings(MapiHdl hdl);
mapi_export MapiMsg mapi_param_type(MapiHdl hdl, int fnr, int ctype, int sqltype, void *ptr);
mapi_export MapiMsg mapi_param_string(MapiHdl hdl, int fnr, int sqltype, char *ptr, int *sizeptr);
mapi_export MapiMsg mapi_param(MapiHdl hdl, int fnr, char **ptr);
mapi_export MapiMsg mapi_param_numeric(MapiHdl hdl, int fnr, int scale, int precision, void *ptr);
mapi_export MapiMsg mapi_clear_params(MapiHdl hdl);
mapi_export MapiHdl mapi_prepare(Mapi mid, const char *cmd);
mapi_export MapiMsg mapi_prepare_handle(MapiHdl hdl, const char *cmd);
mapi_export MapiMsg mapi_virtual_result(MapiHdl hdl, int columns, const char **columnnames, const char **columntypes, const int *columnlengths, int tuplecount, const char ***tuples);
mapi_export MapiMsg mapi_execute(MapiHdl hdl);
mapi_export MapiMsg mapi_execute_array(MapiHdl hdl, char **val);
mapi_export MapiMsg mapi_fetch_reset(MapiHdl hdl);
mapi_export MapiMsg mapi_finish(MapiHdl hdl);
mapi_export MapiHdl mapi_prepare_array(Mapi mid, const char *cmd, char **val);
mapi_export MapiHdl mapi_query(Mapi mid, const char *cmd);
mapi_export MapiMsg mapi_query_handle(MapiHdl hdl, const char *cmd);
mapi_export MapiHdl mapi_query_prep(Mapi mid);
mapi_export MapiMsg mapi_query_part(MapiHdl hdl, const char *cmd, size_t size);
mapi_export MapiMsg mapi_query_done(MapiHdl hdl);
mapi_export MapiHdl mapi_quick_query(Mapi mid, const char *cmd, FILE *fd);
mapi_export MapiHdl mapi_query_array(Mapi mid, const char *cmd, char **val);
mapi_export MapiHdl mapi_quick_query_array(Mapi mid, const char *cmd, char **val, FILE *fd);
mapi_export MapiHdl mapi_send(Mapi mid, const char *cmd);
mapi_export MapiMsg mapi_read_response(MapiHdl hdl);
mapi_export MapiHdl mapi_stream_query(Mapi mid, const char *cmd, int windowsize);
mapi_export MapiMsg mapi_cache_limit(Mapi mid, int limit);
mapi_export MapiMsg mapi_cache_shuffle(MapiHdl hdl, int percentage);
mapi_export MapiMsg mapi_cache_freeup(MapiHdl hdl, int percentage);
mapi_export MapiMsg mapi_quick_response(MapiHdl hdl, FILE *fd);
mapi_export MapiMsg mapi_seek_row(MapiHdl hdl, mapi_int64 rowne, int whence);

mapi_export MapiMsg mapi_timeout(Mapi mid, unsigned int time);
mapi_export int mapi_fetch_row(MapiHdl hdl);
mapi_export mapi_int64 mapi_fetch_all_rows(MapiHdl hdl);
mapi_export int mapi_get_field_count(MapiHdl hdl);
mapi_export mapi_int64 mapi_get_row_count(MapiHdl hdl);
mapi_export mapi_int64 mapi_get_last_id(MapiHdl hdl);
mapi_export mapi_int64 mapi_rows_affected(MapiHdl hdl);

mapi_export char *mapi_fetch_field(MapiHdl hdl, int fnr);
mapi_export size_t mapi_fetch_field_len(MapiHdl hdl, int fnr);
mapi_export MapiMsg mapi_store_field(MapiHdl hdl, int fnr, int outtype, void *outparam);
mapi_export char **mapi_fetch_field_array(MapiHdl hdl);
mapi_export char *mapi_fetch_line(MapiHdl hdl);
mapi_export int mapi_split_line(MapiHdl hdl);
mapi_export char *mapi_get_lang(Mapi mid);
mapi_export char *mapi_get_uri(Mapi mid);
mapi_export char *mapi_get_dbname(Mapi mid);
mapi_export char *mapi_get_host(Mapi mid);
mapi_export char *mapi_get_user(Mapi mid);
mapi_export char *mapi_get_mapi_version(Mapi mid);
mapi_export char *mapi_get_monet_version(Mapi mid);
mapi_export char *mapi_get_motd(Mapi mid);
mapi_export int mapi_is_connected(Mapi mid);
mapi_export char *mapi_get_table(MapiHdl hdl, int fnr);
mapi_export char *mapi_get_name(MapiHdl hdl, int fnr);
mapi_export char *mapi_get_type(MapiHdl hdl, int fnr);
mapi_export int mapi_get_len(MapiHdl hdl, int fnr);
mapi_export int mapi_get_digits(MapiHdl hdl, int fnr);
mapi_export int mapi_get_scale(MapiHdl hdl, int fnr);
mapi_export char *mapi_get_query(MapiHdl hdl);
mapi_export int mapi_get_querytype(MapiHdl hdl);
mapi_export int mapi_get_tableid(MapiHdl hdl);
mapi_export char *mapi_quote(const char *msg, int size);
mapi_export char *mapi_unquote(char *msg);
mapi_export MapiHdl mapi_get_active(Mapi mid);
#ifdef _MSC_VER
mapi_export const char *wsaerror(int);
#endif

#ifdef __cplusplus
}
#endif
#endif				/* _MAPI_H_INCLUDED */
