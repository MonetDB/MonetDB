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

#include "msettings.h"

#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif
#ifdef HAVE_PWD_H
#include  <pwd.h>
#endif
#include  <sys/types.h>

#ifdef HAVE_SYS_UN_H
# include <sys/un.h>
# include <sys/stat.h>
# ifdef HAVE_DIRENT_H
#  include <dirent.h>
# endif
#endif
#ifdef HAVE_NETDB_H
# include <netdb.h>
# include <netinet/in.h>
#endif
#ifdef HAVE_SYS_UIO_H
# include <sys/uio.h>
#endif

#include <signal.h>
#include <string.h>
#include <memory.h>
#include <time.h>
#ifdef HAVE_FTIME
# include <sys/timeb.h>		/* ftime */
#endif
#ifdef HAVE_SYS_TIME_H
# include <sys/time.h>		/* gettimeofday */
#endif

#ifdef HAVE_OPENSSL
#include <openssl/ssl.h>
#endif


/* Copied from gdk_posix, but without taking a lock because we don't have access to
 * MT_lock_set/unset here. We just have to hope for the best
 */
#ifndef HAVE_LOCALTIME_R
struct tm *
localtime_r(const time_t *restrict timep, struct tm *restrict result)
{
	struct tm *tmp;
	tmp = localtime(timep);
	if (tmp)
		*result = *tmp;
	return tmp ? result : NULL;
}
#endif

/* Copied from gdk_posix, but without taking a lock because we don't have access to
 * MT_lock_set/unset here. We just have to hope for the best
 */
#ifndef HAVE_GMTIME_R
struct tm *
gmtime_r(const time_t *restrict timep, struct tm *restrict result)
{
	struct tm *tmp;
	tmp = gmtime(timep);
	if (tmp)
		*result = *tmp;
	return tmp ? result : NULL;
}
#endif


#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif


/* number of elements in an array */
#define NELEM(arr)	(sizeof(arr) / sizeof(arr[0]))



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

/* information about the columns in a result set */
struct MapiColumn {
	char *tablename;
	char *columnname;
	char *columntype;
	int columnlength;
	int digits;
	int scale;
};

/* information about bound columns */
struct MapiBinding {
	void *outparam;		/* pointer to application variable */
	int outtype;		/* type of application variable */
	int precision;
	int scale;
};

/* information about statement parameters */
struct MapiParam {
	void *inparam;		/* pointer to application variable */
	int *sizeptr;		/* if string, points to length of string or -1 */
	int intype;		/* type of application variable */
	int outtype;		/* type of value */
	int precision;
	int scale;
};

/*
 * The row cache contains a string representation of each (non-error) line
 * received from the backend. After a mapi_fetch_row() or mapi_fetch_field()
 * this string has been indexed from the anchor table, which holds a pointer
 * to the start of the field. A sliced version is recognized by looking
 * at the fldcnt table, which tells you the number of fields recognized.
 * Lines received from the server without 'standard' line headers are
 * considered a single field.
 */
struct MapiRowBuf {
	int rowlimit;		/* maximum number of rows to cache */
	int limit;		/* current storage space limit */
	int writer;
	int reader;
	int64_t first;		/* row # of first tuple */
	int64_t tuplecount;	/* number of tuples in the cache */
	struct {
		int fldcnt;	/* actual number of fields in each row */
		char *rows;	/* string representation of rows received */
		int tupleindex;	/* index of tuple rows */
		int64_t tuplerev;	/* reverse map of tupleindex */
		char **anchors;	/* corresponding field pointers */
		size_t *lens;	/* corresponding field lenghts */
	} *line;
};

struct MapiResultSet {
	struct MapiResultSet *next;
	struct MapiStatement *hdl;
	int tableid;		/* SQL id of current result set */
	int querytype;		/* type of SQL query */
	int64_t tuple_count;
	int64_t row_count;
	int64_t last_id;
	int64_t querytime;
	int64_t maloptimizertime;
	int64_t sqloptimizertime;
	int fieldcnt;
	int maxfields;
	char *errorstr;		/* error from server */
	char sqlstate[6];	/* the SQL state code */
	struct MapiColumn *fields;
	struct MapiRowBuf cache;
	bool commentonly;	/* only comments seen so far */
};

struct MapiStatement {
	struct MapiStruct *mid;
	char *template;		/* keep parameterized query text around */
	char *query;
	int maxbindings;
	int maxparams;
	struct MapiBinding *bindings;
	struct MapiParam *params;
	struct MapiResultSet *result, *active, *lastresult;
	int *pending_close;
	int npending_close;
	bool needmore;		/* need more input */
	bool aborted;		/* this query was aborted */
	MapiHdl prev, next;
};


struct BlockCache {
	char *buf;
	int lim;
	int nxt;
	int end;
	bool eos;		/* end of sequence */
};

enum mapi_lang_t {
	LANG_MAL = 0,
	LANG_SQL = 2,
	LANG_PROFILER = 3
};

/* A connection to a server is represented by a struct MapiStruct.  An
   application can have any number of connections to any number of
   servers.  Connections are completely independent of each other.
*/
struct MapiStruct {
	msettings *settings;

	char *uri;

	char *server;		/* server version */
	char *motd;		/* welcome message from server */

	char *noexplain;	/* on error, don't explain, only print result */
	MapiMsg error;		/* Error occurred */
	char *errorstr;		/* error from server */
	const char *action;	/* pointer to constant string */

	struct BlockCache blk;
	bool connected;
	bool trace;		/* Trace Mapi interaction */
	int handshake_options;	/* which settings can be sent during challenge/response? */
	bool columnar_protocol;
	bool sizeheader;
	bool oobintr;
	MapiHdl first;		/* start of doubly-linked list */
	MapiHdl active;		/* set when not all rows have been received */

	int redircnt;		/* redirection count, used to cut of redirect loops */
	int redirmax;		/* maximum redirects before giving up */
#define MAXREDIR 50
	char *redirects[MAXREDIR];	/* NULL-terminated list of redirects */

	stream *tracelog;	/* keep a log for inspection */
	char *tracebuffer;	/* used for formatting to tracelog */
	size_t tracebuffersize; /* allocated size of tracebuffer */

	stream *from, *to;
	uint32_t index;		/* to mark the log records */
	void *filecontentprivate;
	void *filecontentprivate_old;
	char *(*getfilecontent)(void *, const char *, bool, uint64_t, size_t *);
	char *(*putfilecontent)(void *, const char *, bool, const void *, size_t);
	char *(*putfilecontent_old)(void *, const char *, const void *, size_t);
};

extern char mapi_nomem[];
void mapi_clrError(Mapi mid)
	__attribute__((__nonnull__(1)));
MapiMsg mapi_setError(Mapi mid, const char *msg, const char *action, MapiMsg error)
	__attribute__((__nonnull__(2))) __attribute__((__nonnull__(3)));
MapiMsg mapi_printError(Mapi mid, const char *action, MapiMsg error, const char *fmt, ...)
	__attribute__((__nonnull__(2))) __attribute__((__format__(__printf__, 4, 5)));

void mapi_impl_log_data(Mapi mid, const char *filename, long line, const char *mark, const char *data, size_t len);
void mapi_impl_log_record(Mapi mid, const char *filename, long line, const char *mark, const char *fmt, ...)
	__attribute__((__format__(__printf__, 5, 6)));
#define mapi_log_data(mid, mark, start, len)  do { if ((mid)->tracelog) mapi_impl_log_data(mid, __func__, __LINE__, mark, start, len); } while (0)
#define mapi_log_record(mid, mark, ...)  do { if ((mid)->tracelog) mapi_impl_log_record(mid, __func__, __LINE__, mark, __VA_ARGS__); } while (0)

#define check_stream(mid, s, msg, e)					\
	do {								\
		if ((s) == NULL || mnstr_errnr(s) != MNSTR_NO__ERROR) {	\
			if (mnstr_peek_error(s))			\
				mapi_printError((mid), __func__, MTIMEOUT, "%s: %s", (msg), mnstr_peek_error(s)); \
			else						\
				mapi_printError((mid), __func__, MTIMEOUT, "%s", (msg)); \
			close_connection(mid);				\
			return (e);					\
		}							\
	} while (0)
#define REALLOC(p, c)							\
	do {								\
		if (p) {						\
			void *tmp = realloc((p), (c) * sizeof(*(p)));	\
			if (tmp == NULL)				\
				free(p);				\
			(p) = tmp;					\
		} else							\
			(p) = malloc((c) * sizeof(*(p)));		\
	} while (0)

MapiMsg read_into_cache(MapiHdl hdl, int lookahead);
MapiMsg mapi_Xcommand(Mapi mid, const char *cmdname, const char *cmdvalue);


extern const struct MapiStruct MapiStructDefaults;

// 'settings' will be newly allocated if NULL
Mapi mapi_new(msettings *settings);

MapiMsg wrap_tls(Mapi mid, SOCKET sock);
MapiMsg mapi_wrap_streams(Mapi mid, stream *rstream, stream *wstream);

MapiMsg scan_unix_sockets(Mapi mid);
MapiMsg connect_socket_unix(Mapi mid);
MapiMsg establish_connection(Mapi mid);
MapiMsg wrap_socket(Mapi mid, SOCKET sock);

void close_connection(Mapi mid);
void set_uri(Mapi mid);

#ifdef HAVE_OPENSSL
MapiMsg croak_openssl(Mapi mid, const char *action, const char *fmt, ...)
	__attribute__(( __format__(__printf__, 3, 4) ));

MapiMsg add_system_certificates(Mapi mid, SSL_CTX *ctx);
#endif
