/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*

  Author: Steffen Goeldner <sgoeldner@cpan.org>

  This header is derived from src/mapi/clients/C/Mapi.mx and
  exposes the internal Mapi structures - dangerous!

*/

typedef int MapiMsg;

typedef struct MapiStruct *Mapi;
typedef struct MapiStatement *MapiHdl;

struct MapiRowBuf {
	int rowlimit;		/* maximum number of rows to cache */
	int shuffle;		/* percentage of rows to shuffle upon overflow */
	int limit;		/* current storage space limit */
	int writer;
	int reader;
	int first;		/* row # of first tuple */
	int *fldcnt;		/* actual number of fields in each row */
	char **rows;		/* string representation of rows received */
	int tuplecount;		/* number of tuples in the cache */
	int *tupleindex;	/* index of tuple rows */
	int *tuplerev;		/* reverse map of tupleindex */
	char ***anchors;	/* corresponding field pointers */
};

struct BlockCache {
	char *buf;
	int lim;
	int nxt;
	int end;
	int eos;		/* end of sequence */
};

typedef struct stream stream;

struct MapiStruct {
	char *server;		/* server version */
	char *mapiversion;	/* mapi version */
	char *hostname;
	int port;
	char *username;
	char *password;
	char *language;
	char *database;		/* to obtain from server */
	int languageId;
	int versionId;		/* Monet 4 or 5 */
	char *motd;		/* welcome message from server */

	int trace;		/* Trace Mapi interaction */
	int blocked;		/* blocked mode */
	int auto_commit;
	char* noexplain;	/* on error, don't explain, only print result */
	MapiMsg error;		/* Error occurred */
	char *errorstr;		/* pointer to constant string */
	const char *action;	/* pointer to constant string */

	struct BlockCache blk;
	int connected;
	MapiHdl first;		/* start of doubly-linked list */
	MapiHdl active;		/* set when not all rows have been received */

	int cachelimit;		/* default maximum number of rows to cache */

	stream *tracelog;	/* keep a log for inspection */
	stream *from, *to;
};

struct MapiResultSet {
	struct MapiResultSet *next;
	struct MapiStatement *hdl;
	int tableid;		/* SQL id of current result set */
	int querytype;		/* type of SQL query */
	int row_count;
	int fieldcnt;
	int maxfields;
	char *errorstr;		/* error from server */
	struct MapiColumn *fields;
	struct MapiRowBuf cache;
};

struct MapiStatement {
	struct MapiStruct *mid;
	char *template;		/* keep parameterized query text around */
	char *query;
	int maxbindings;
	struct MapiBinding *bindings;
	int maxparams;
	struct MapiParam *params;
	struct MapiResultSet *result, *active, *lastresult;
	int needmore;		/* need more input */
	int *pending_close;
	int npending_close;
	MapiHdl prev, next;
};

#ifdef __cplusplus
extern "C" {
#endif

#ifdef NATIVE_WIN32
#ifndef LIBMAPI
#define mapi_export extern __declspec(dllimport)
#else
#define mapi_export extern __declspec(dllexport)
#endif
#else
#define mapi_export extern
#endif

mapi_export MapiMsg mapi_destroy(Mapi mid);
mapi_export Mapi mapi_connect(const char *host, int port, const char *username, const char *password, const char *lang);
mapi_export MapiHdl mapi_new_handle(Mapi mid);
mapi_export MapiMsg mapi_close_handle(MapiHdl hdl);
mapi_export MapiMsg mapi_finish(MapiHdl hdl);
mapi_export MapiHdl mapi_query(Mapi mid, const char *cmd);
mapi_export MapiMsg mapi_query_handle(MapiHdl hdl, const char *cmd);
mapi_export int mapi_fetch_row(MapiHdl hdl);
mapi_export int mapi_get_field_count(MapiHdl hdl);
mapi_export int mapi_rows_affected(MapiHdl hdl);
mapi_export char *mapi_fetch_field(MapiHdl hdl, int fnr);
mapi_export char *mapi_get_name(MapiHdl hdl, int fnr);
mapi_export char *mapi_get_type(MapiHdl hdl, int fnr);
mapi_export int mapi_get_len(MapiHdl hdl, int fnr);
mapi_export int mapi_get_querytype(MapiHdl hdl);
mapi_export int mapi_get_tableid(MapiHdl hdl);

#ifdef __cplusplus
}
#endif
