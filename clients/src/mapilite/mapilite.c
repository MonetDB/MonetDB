
#include "clients_config.h"
#include <monet_utils.h>
#include <stream.h>		/* include before Mapi.h */
#include <stream_socket.h>
#include "mapilite.h"

#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif
#include  <stdio.h>
#ifdef HAVE_PWD_H
#include  <pwd.h>
#endif
#include  <sys/types.h>

#ifdef HAVE_SYS_UN_H
#include <sys/un.h>
#endif
#ifdef HAVE_NETDB_H
# include <netdb.h>
# include <netinet/in.h>
#endif

#include  <signal.h>
#include  <string.h>
#include  <memory.h>

/*additional definitions for date and time*/
#ifdef HAVE_FTIME
#include <sys/timeb.h>
#endif

#ifdef TIME_WITH_SYS_TIME
# include <sys/time.h>
# include <time.h>
#else
# ifdef HAVE_SYS_TIME_H
#  include <sys/time.h>
# else
#  include <time.h>
# endif
#endif

#ifdef NATIVE_WIN32
#define strdup _strdup
#endif

#ifdef HAVE_CRYPT_H
# include <crypt.h>
#else
# if defined(HAVE_CRYPT) && defined(__MINGW32__)
_CRTIMP char *__cdecl crypt(const char *key, const char *salt);
# endif
#endif

#ifdef HAVE_OPENSSL
# include <openssl/md5.h>
# include <openssl/sha.h>
# include <openssl/ripemd.h>
#endif

#ifndef INVALID_SOCKET
#define INVALID_SOCKET (-1)
#endif


#define MAPIBLKSIZE	256	/* minimum buffer shipped */
#define MAXQUERYSIZE	(100*1024)
#define QUERYBLOCK	(16*1024)


/* information about the columns in a result set */
struct MapiColumn {
	char *tablename;
	char *columnname;
	char *columntype;
	int columnlength;
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

struct MapiRowBuf {
	int rowlimit;		/* maximum number of rows to cache */
	int shuffle;		/* percentage of rows to shuffle upon overflow */
	int limit;		/* current storage space limit */
	int writer;
	int reader;
	mapi_int64 first;	/* row # of first tuple */
	mapi_int64 tuplecount;	/* number of tuples in the cache */
	struct {
		int fldcnt;	/* actual number of fields in each row */
		char *rows;	/* string representation of rows received */
		int tupleindex;	/* index of tuple rows */
		mapi_int64 tuplerev;	/* reverse map of tupleindex */
		char **anchors;	/* corresponding field pointers */
	} *line;
};

struct BlockCache {
	char *buf;
	int lim;
	int nxt;
	int end;
	int eos;		/* end of sequence */
};

/* A connection to a server is represented by a struct MapiStruct.  An
   application can have any number of connections to any number of
   servers.  Connections are completely independent of each other.
*/
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

	int profile;		/* profile Mapi interaction */
	int trace;		/* Trace Mapi interaction */
	int auto_commit;
	char *noexplain;	/* on error, don't explain, only print result */
	MapiMsg error;		/* Error occurred */
	char *errorstr;		/* error from server */
	const char *action;	/* pointer to constant string */

	struct BlockCache blk;
	int connected;
	MapiHdl first;		/* start of doubly-linked list */
	MapiHdl active;		/* set when not all rows have been received */

	int cachelimit;		/* default maximum number of rows to cache */
	int redircnt;		/* redirection count, used to cut of redirect loops */
	int redirmax;	   /* maximum redirects before giving up */
	char *redirects[50];/* NULL-terminated list of redirects */

	stream *tracelog;	/* keep a log for inspection */
	stream *from, *to;
	int index;		/* to mark the log records */
};

struct MapiResultSet {
	struct MapiResultSet *next;
	struct MapiStatement *hdl;
	int tableid;		/* SQL id of current result set */
	int querytype;		/* type of SQL query */
	mapi_int64 row_count;
	mapi_int64 last_id;
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

/*
#ifdef DEBUG
#define debugprint(fmt,arg)	printf(fmt,arg)
#else
#define debugprint(fmt,arg)	((void) 0)
#endif
*/

/*
#define mapi_check(X,C)							\
	do {								\
		debugprint("entering %s\n", (C));			\
		assert(X);						\
		if ((X)->connected == 0) {				\
			mapi_setError((X), "Connection lost", (C), MERROR); \
			return (X)->error;				\
		}							\
		mapi_clrError(X);					\
	} while (0)
*/

#define mapi_check0(X,C)						\
	do {								\
		debugprint("entering %s\n", (C));			\
		assert(X);						\
		if ((X)->connected == 0) {				\
			mapi_setError((X), "Connection lost", (C), MERROR); \
			return 0;					\
		}							\
		mapi_clrError(X);					\
	} while (0)

#define mapi_hdl_check(X,C)						\
	do {								\
		debugprint("entering %s\n", (C));			\
		assert(X);						\
		assert((X)->mid);					\
		if ((X)->mid->connected == 0) {				\
			mapi_setError((X)->mid, "Connection lost", (C), MERROR); \
			return (X)->mid->error;				\
		}							\
		mapi_clrError((X)->mid);				\
	} while (0)

#define mapi_hdl_check0(X,C)						\
	do {								\
		debugprint("entering %s\n", (C));			\
		assert(X);						\
		assert((X)->mid);					\
		if ((X)->mid->connected == 0) {				\
			mapi_setError((X)->mid, "Connection lost", (C), MERROR); \
			return 0;					\
		}							\
		mapi_clrError((X)->mid);				\
	} while (0)

#define testBinding(hdl,fnr,funcname)					\
	do {								\
		mapi_hdl_check(hdl, funcname);				\
		if (fnr < 0) {						\
			return mapi_setError(hdl->mid,			\
						 "Illegal field number",	\
						 funcname, MERROR);		\
		}							\
		/* make sure there is enough space */			\
		if (fnr >= hdl->maxbindings)				\
			mapi_extend_bindings(hdl, fnr);			\
	} while (0)

#define testParam(hdl, fnr, funcname)					\
	do {								\
		mapi_hdl_check(hdl, funcname);				\
		if (fnr < 0) {						\
			return mapi_setError(hdl->mid,			\
						 "Illegal param number",	\
						 funcname, MERROR);		\
		}							\
		if (fnr >= hdl->maxparams)				\
			mapi_extend_params(hdl, fnr);			\
	} while (0)

#define check_stream(mid,s,msg,f,e)					\
	do {								\
		if ((s) == NULL || stream_errnr(s)) {			\
			mapi_log_record(mid,msg);			\
			mapi_log_record(mid,f);			\
			close_connection(mid);			\
			mapi_setError((mid), (msg), (f), MTIMEOUT);	\
			return (e);					\
		}							\
	} while (0)

#define REALLOC(p,c)	((p) = ((p) ? realloc((p),(c)*sizeof(*(p))) : malloc((c)*sizeof(*(p)))))

#define checkSpace(len)						\
	do {							\
		/* note: k==strlen(hdl->query) */		\
		if (k+len >= lim) {				\
			lim = k + len + MAPIBLKSIZE;		\
			hdl->query = realloc(hdl->query, lim);	\
			assert(hdl->query);			\
		}						\
	} while (0)


/* local declerations */
static Mapi mapi_new(void);
static int mapi_extend_bindings(MapiHdl hdl, int minbindings);
static int mapi_extend_params(MapiHdl hdl, int minparams);
static MapiMsg mapi_setError(Mapi mid, const char *msg, const char *action, MapiMsg error);
static void close_connection(Mapi mid);
static MapiMsg read_into_cache(MapiHdl hdl, int lookahead);
static int unquote(const char *msg, char **start, const char **next, int endchar);
static int mapi_slice_row(struct MapiResultSet *result, int cr);
static void mapi_store_bind(struct MapiResultSet *result, int cr);

/* define replacements */
static void debugprint(char *fmt, char *arg);
static MapiMsg mapi_check(Mapi mid, char *C);
static void mapi_clrError(Mapi mid);

static int mapi_initialized = 0;

static void
debugprint(char *fmt, char *arg)
{
#ifdef DEBUG
	printf(fmt, arg);
#else
	(void)fmt;
	(void)arg;
#endif
}
 
static MapiMsg
mapi_check(Mapi mid,char *C)
{
	debugprint("entering %s\n", C);
	assert(mid);
	if (mid->connected == 0) {
		mapi_setError(mid, "Connection lost", C, MERROR);
		return false;
	} else {
		mapi_clrError(mid);
		return true;
	}
}

static void
mapi_clrError(Mapi mid)
{
	assert(mid);
	if (mid->errorstr)
		free(mid->errorstr);
	mid->action = 0;	/* contains references to constants */
	mid->error = 0;
	mid->errorstr = 0;
}

static MapiMsg
mapi_setError(Mapi mid, const char *msg, const char *action, MapiMsg error)
{
	assert(msg);
	REALLOC(mid->errorstr, strlen(msg) + 1);
	strcpy(mid->errorstr, msg);
	mid->error = error;
	mid->action = action;
	return mid->error;
}

MapiMsg
mapi_error(Mapi mid)
{
	assert(mid);
	return mid->error;
}

char *
mapi_error_str(Mapi mid)
{
	assert(mid);
	return mid->errorstr;
}

static void
clean_print(char *msg, const char *prefix, FILE *fd)
{
	size_t len = strlen(prefix);

	while (msg && *msg) {
		/* cut by line */
		char *p = strchr(msg, '\n');

		if (p)
			*p++ = 0;

		/* skip over prefix */
		if (strncmp(msg, prefix, len) == 0)
			msg += len;

		/* output line */
		fputs(msg, fd);
		fputc('\n', fd);
		msg = p;
	}
}

static void
indented_print(const char *msg, const char *prefix, FILE *fd)
{
	/* for multiline error messages, indent all subsequent
	   lines with the space it takes to print "ERROR = " */
	const char *s, *p, *q;

	s = prefix;
	p = msg;
	while (p && *p) {
		fprintf(fd, "%s", s);
		s = "		";
		q = strchr(p, '\n');
		if (q) {
			q++;	/* also print the newline */
			fprintf(fd, "%.*s", (int) (q - p), p);
		} else {
			/* print bit after last newline,
			   adding one ourselves */
			fprintf(fd, "%s\n", p);
			break;	/* nothing more to do */
		}
		p = q;
	}
}

void
mapi_noexplain(Mapi mid, char *errorprefix)
{
	assert(mid);
	mid->noexplain = errorprefix;
}

MapiMsg
mapi_explain(Mapi mid, FILE *fd)
{
	assert(mid);
	if (mid->noexplain == NULL) {
		fprintf(fd, "MAPI  = %s@%s:%d\n", mid->username, mid->hostname, mid->port);
		if (mid->action)
			fprintf(fd, "ACTION= %s\n", mid->action);
		if (mid->errorstr)
			indented_print(mid->errorstr, "ERROR = ", fd);
	} else if (mid->errorstr) {
		clean_print(mid->errorstr, mid->noexplain, fd);
	}
	fflush(fd);
	mapi_clrError(mid);
	return MOK;
}

MapiMsg
mapi_explain_query(MapiHdl hdl, FILE *fd)
{
	Mapi mid;

	assert(hdl);
	mid = hdl->mid;
	assert(mid);
	if (mid->noexplain == NULL) {
		fprintf(fd, "MAPI  = %s@%s:%d\n", mid->username, mid->hostname, mid->port);
		if (mid->action)
			fprintf(fd, "ACTION= %s\n", mid->action);
		if (hdl->query)
			indented_print(hdl->query, "QUERY = ", fd);
		if (mid->errorstr)
			indented_print(mid->errorstr, "ERROR = ", fd);
	} else if (mid->errorstr) {
		clean_print(mid->errorstr, mid->noexplain, fd);
	}
	fflush(fd);
	mapi_clrError(mid);
	return MOK;
}

MapiMsg
mapi_explain_result(MapiHdl hdl, FILE *fd)
{
	Mapi mid;

	if (hdl == NULL || hdl->result == NULL || hdl->result->errorstr == NULL)
		return MOK;
	assert(hdl);
	assert(hdl->result);
	assert(hdl->result->errorstr);
	mid = hdl->mid;
	assert(mid);
	if (mid->noexplain == NULL) {
		fprintf(fd, "MAPI  = %s@%s:%d\n", mid->username, mid->hostname, mid->port);
		if (mid->action)
			fprintf(fd, "ACTION= %s\n", mid->action);
		if (hdl->query)
			indented_print(hdl->query, "QUERY = ", fd);
		indented_print(hdl->result->errorstr, "ERROR = ", fd);
	} else {
		clean_print(hdl->result->errorstr, mid->noexplain, fd);
	}
	fflush(fd);
	return MOK;
}

int
mapi_get_trace(Mapi mid)
{
	mapi_check0(mid, "mapi_get_trace");
	return mid->trace;
}

long
usec()
{
#ifdef HAVE_GETTIMEOFDAY
	struct timeval tp;

	gettimeofday(&tp, NULL);
	return ((long) tp.tv_sec) * 1000000 + (long) tp.tv_usec;
#else
#ifdef HAVE_FTIME
	struct timeb tb;

	ftime(&tb);
	return ((long) tb.time) * 1000000 + ((long) tb.millitm) * 1000;
#endif
#endif
}

void
mapi_log_header(Mapi mid, char *mark)
{
	static long firstcall = 0;
	long now;

	if (mid->tracelog == NULL)
		return;
	if (firstcall == 0)
		firstcall = usec();
	now = (usec() - firstcall) / 1000;
	stream_printf(mid->tracelog, ":%ld[%d]:%s\n", now, mid->index, mark);
	stream_flush(mid->tracelog);
}

void
mapi_log_record(Mapi mid, const char *msg)
{
	mapi_log_header(mid, "W");
	stream_printf(mid->tracelog, "%s", msg);
	stream_flush(mid->tracelog);
}

MapiMsg
mapi_log(Mapi mid, const char *nme)
{
	mapi_clrError(mid);
	if (mid->tracelog) {
		stream_close(mid->tracelog);
		stream_destroy(mid->tracelog);
		mid->tracelog = NULL;
	}
	if (nme == NULL)
		return MOK;
	mid->tracelog = open_wastream(nme);
	if (mid->tracelog == NULL || stream_errnr(mid->tracelog)) {
		if (mid->tracelog)
			stream_destroy(mid->tracelog);
		mid->tracelog = NULL;
		return mapi_setError(mid, "Could not create log file", "mapi_log", MERROR);
	}
	return MOK;
}

/* send a dummy request to the server to see whether the connection is
   still alive */
MapiMsg
mapi_ping(Mapi mid)
{
	MapiHdl hdl = NULL;

	mapi_check(mid, "mapi_ping");
	switch (mid->languageId) {
	case LANG_SQL:
		hdl = mapi_query(mid, "select true;");
		break;
	case LANG_MAL:
		hdl = mapi_query(mid, "io.print(1);");
		break;
	case LANG_MIL:
		hdl = mapi_query(mid, "print(1);");
		break;
	}
	if (hdl)
		mapi_close_handle(hdl);
	return mid->error;
}

/* allocate a new structure to represent a result set */
static struct MapiResultSet *
new_result(MapiHdl hdl)
{
	struct MapiResultSet *result;

	assert((hdl->lastresult == NULL && hdl->result == NULL) || (hdl->result != NULL && hdl->lastresult != NULL && hdl->lastresult->next == NULL));

	if (hdl->mid->trace == MAPI_TRACE)
		printf("allocating new result set\n");
	/* append a newly allocated struct to the end of the linked list */
	result = malloc(sizeof(*result));
	result->next = NULL;
	if (hdl->lastresult == NULL)
		hdl->result = hdl->lastresult = result;
	else {
		hdl->lastresult->next = result;
		hdl->lastresult = result;
	}

	result->hdl = hdl;
	result->tableid = -1;
	result->querytype = -1;
	result->errorstr = NULL;

	result->row_count = 0;
	result->last_id = -1;

	result->fieldcnt = 0;
	result->maxfields = 0;
	result->fields = NULL;

	result->cache.rowlimit = hdl->mid->cachelimit;
	result->cache.shuffle = 100;
	result->cache.limit = 0;
	result->cache.writer = 0;
	result->cache.reader = -1;
	result->cache.first = 0;
	result->cache.tuplecount = 0;
	result->cache.line = NULL;

	return result;
}

/* close a result set, discarding any unread results */
static MapiMsg
close_result(MapiHdl hdl)
{
	struct MapiResultSet *result;
	Mapi mid;
	int i;

	result = hdl->result;
	if (result == NULL)
		return MERROR;
	mid = hdl->mid;
	assert(mid != NULL);
	if (mid->trace == MAPI_TRACE)
		printf("closing result set\n");
	if (result->tableid >= 0 && result->querytype != Q_PREPARE) {
		if (mid->active && result->next == NULL && !mid->active->needmore && read_into_cache(mid->active, -1) != MOK)
			return MERROR;
		assert(hdl->npending_close == 0 || (hdl->npending_close > 0 && hdl->pending_close != NULL));
		if (mid->active && (mid->active->active != result || result->cache.tuplecount < result->row_count)) {
			/* can't write "X" commands now, so save for later */
			REALLOC(hdl->pending_close, hdl->npending_close + 1);
			hdl->pending_close[hdl->npending_close] = result->tableid;
			hdl->npending_close++;
		} else if (mid->to != NULL) {
			/* first close saved up to-be-closed tables */
			for (i = 0; i < hdl->npending_close; i++) {
				char msg[256];

				snprintf(msg, sizeof(msg), "Xclose %d\n", hdl->pending_close[i]);
				mapi_log_record(mid, msg);
				mid->active = hdl;
				if (stream_printf(mid->to, msg) < 0 ||
					stream_flush(mid->to)) {
					close_connection(mid);
					mapi_setError(mid, stream_error(mid->to), "mapi_close_handle", MTIMEOUT);
					break;
				}
				read_into_cache(hdl, 0);
			}
			hdl->npending_close = 0;
			if (hdl->pending_close)
				free(hdl->pending_close);
			hdl->pending_close = NULL;
			if (mid->to != NULL && result->cache.tuplecount < result->row_count) {
				char msg[256];

				snprintf(msg, sizeof(msg), "Xclose %d\n", result->tableid);
				mapi_log_record(mid, msg);
				mid->active = hdl;
				if (stream_printf(mid->to, msg) < 0 ||
					stream_flush(mid->to)) {
					close_connection(mid);
					mapi_setError(mid, stream_error(mid->to), "mapi_close_handle", MTIMEOUT);
				} else
					read_into_cache(hdl, 0);
			}
		}
		result->tableid = -1;
	}
	if (mid->active == hdl && hdl->active == result && read_into_cache(hdl, -1) != MOK)
		return MERROR;
	assert(hdl->active != result);
	if (result->fields) {
		for (i = 0; i < result->maxfields; i++) {
			if (result->fields[i].tablename)
				free(result->fields[i].tablename);
			if (result->fields[i].columnname)
				free(result->fields[i].columnname);
			if (result->fields[i].columntype)
				free(result->fields[i].columntype);
		}
		free(result->fields);
	}
	result->fields = NULL;
	result->maxfields = result->fieldcnt = 0;
	if (result->cache.line) {
		for (i = 0; i < result->cache.writer; i++) {
			if (result->cache.line[i].rows)
				free(result->cache.line[i].rows);
			if (result->cache.line[i].anchors) {
				int j;

				for (j = 0; j < result->cache.line[i].fldcnt; j++)
					if (result->cache.line[i].anchors[j]) {
						free(result->cache.line[i].anchors[j]);
						result->cache.line[i].anchors[j] = NULL;
					}
				free(result->cache.line[i].anchors);
			}
		}
		free(result->cache.line);
		result->cache.line = NULL;
		result->cache.tuplecount = 0;
	}
	if (result->errorstr)
		free(result->errorstr);
	result->errorstr = NULL;
	result->hdl = NULL;
	hdl->result = result->next;
	if (hdl->result == NULL)
		hdl->lastresult = NULL;
	result->next = NULL;
	free(result);
	return MOK;
}

static void
add_error(struct MapiResultSet *result, char *error)
{
	/* concatenate the error messages */
	size_t size = result->errorstr ? strlen(result->errorstr) : 0;

	REALLOC(result->errorstr, size + strlen(error) + 2);
	strcpy(result->errorstr + size, error);
	strcat(result->errorstr + size, "\n");
}

char *
mapi_result_error(MapiHdl hdl)
{
	return hdl && hdl->result ? hdl->result->errorstr : NULL;
}

/* Go to the next result set, if any, and close the current result
   set.  This function returns 1 if there are more result sets after
   the one that was closed, otherwise, if more input is needed, return
   MMORE, else, return MOK */
MapiMsg
mapi_next_result(MapiHdl hdl)
{
	mapi_hdl_check(hdl, "mapi_next_result");

	while (hdl->result != NULL) {
		if (close_result(hdl) != MOK)
			return MERROR;
		if (hdl->result &&
			(hdl->result->querytype == -1 ||
			 hdl->result->querytype == Q_TABLE ||
			 hdl->result->querytype == Q_UPDATE ||
			 hdl->result->errorstr != NULL))
			return 1;
	}
	return hdl->needmore ? MMORE : MOK;
}

MapiMsg
mapi_needmore(MapiHdl hdl)
{
	return hdl->needmore ? MMORE : MOK;
}

int
mapi_more_results(MapiHdl hdl)
{
	struct MapiResultSet *result;

	mapi_hdl_check(hdl, "mapi_more_results");

	if ((result = hdl->result) == 0) {
		/* there are no results at all */
		return 0;
	}
	if (result->querytype == Q_TABLE && hdl->mid->active == hdl) {
		/* read until next result (if any) */
		read_into_cache(hdl, -1);
	}
	if (hdl->needmore) {
		/* assume the application will provide more data and
		   that we will then have a result */
		return 1;
	}
	while (result->next) {
		result = result->next;
		if (result->querytype == -1 ||
			result->querytype == Q_TABLE ||
			result->querytype == Q_UPDATE ||
			result->errorstr != NULL)
			return 1;
	}
	/* no more results */
	return 0;
}

MapiHdl
mapi_new_handle(Mapi mid)
{
	MapiHdl hdl;

	mapi_check0(mid, "mapi_new_handle");

	hdl = malloc(sizeof(*hdl));
	assert(hdl);
	if (hdl == NULL) {
		mapi_setError(mid, "Memory allocation failure", "mapi_new_handle", MERROR);
		return NULL;
	}
	hdl->mid = mid;
	hdl->template = NULL;
	hdl->query = NULL;
	hdl->maxbindings = 0;
	hdl->bindings = NULL;
	hdl->maxparams = 0;
	hdl->params = NULL;
	hdl->result = NULL;
	hdl->lastresult = NULL;
	hdl->active = NULL;
	hdl->needmore = 0;
	hdl->pending_close = NULL;
	hdl->npending_close = 0;
	/* add to doubly-linked list */
	hdl->prev = NULL;
	hdl->next = mid->first;
	mid->first = hdl;
	if (hdl->next)
		hdl->next->prev = hdl;
	return hdl;
}

/* close all result sets on the handle but don't close the handle itself */
static MapiMsg
finish_handle(MapiHdl hdl)
{
	Mapi mid;
	int i;

	if (hdl == NULL)
		return MERROR;
	mid = hdl->mid;
	if (mid->active == hdl && !hdl->needmore && read_into_cache(hdl, 0) != MOK)
		return MERROR;
	if (mid->to) {
		if (hdl->needmore) {
			assert(mid->active == NULL || mid->active == hdl);
			hdl->needmore = 0;
			mid->active = hdl;
			stream_flush(mid->to);
			check_stream(mid, mid->to, "write error on stream", "finish_handle", mid->error);
			read_into_cache(hdl, 0);
		}
		for (i = 0; i < hdl->npending_close; i++) {
			char msg[256];

			snprintf(msg, sizeof(msg), "Xclose %d\n", hdl->pending_close[i]);
			mapi_log_record(mid, msg);
			mid->active = hdl;
			if (stream_printf(mid->to, msg) < 0 || stream_flush(mid->to)) {
				close_connection(mid);
				mapi_setError(mid, stream_error(mid->to), "finish_handle", MTIMEOUT);
				break;
			}
			read_into_cache(hdl, 0);
		}
	}
	hdl->npending_close = 0;
	if (hdl->pending_close)
		free(hdl->pending_close);
	hdl->pending_close = NULL;
	while (hdl->result) {
		if (close_result(hdl) != MOK)
			return MERROR;
		if (hdl->needmore) {
			assert(mid->active == NULL || mid->active == hdl);
			hdl->needmore = 0;
			mid->active = hdl;
			stream_flush(mid->to);
			check_stream(mid, mid->to, "write error on stream", "finish_handle", mid->error);
			read_into_cache(hdl, 0);
		}
	}
	return MOK;
}

/* Close a statement handle, discarding any unread output. */
MapiMsg
mapi_close_handle(MapiHdl hdl)
{
	debugprint("entering %s\n", "mapi_close_handle");

	/* don't use mapi_check_hdl: it's ok if we're not connected */
	mapi_clrError(hdl->mid);

	if (finish_handle(hdl) != MOK)
		return MERROR;
	hdl->npending_close = 0;
	if (hdl->pending_close)
		free(hdl->pending_close);
	hdl->pending_close = NULL;
	if (hdl->bindings)
		free(hdl->bindings);
	hdl->bindings = NULL;
	hdl->maxbindings = 0;
	if (hdl->params)
		free(hdl->params);
	hdl->params = NULL;
	hdl->maxparams = 0;
	if (hdl->query)
		free(hdl->query);
	hdl->query = NULL;
	if (hdl->template)
		free(hdl->template);
	hdl->template = NULL;
	/* remove from doubly-linked list */
	if (hdl->prev)
		hdl->prev->next = hdl->next;
	if (hdl->next)
		hdl->next->prev = hdl->prev;
	if (hdl->mid->first == hdl)
		hdl->mid->first = hdl->next;
	hdl->prev = NULL;
	hdl->next = NULL;
	hdl->mid = NULL;
	free(hdl);
	return MOK;
}

/* Allocate a new connection handle. */
static Mapi
mapi_new(void)
{
	Mapi mid;
	static int index = 0;

	mid = malloc(sizeof(*mid));
	if (mid == NULL)
		return NULL;
	assert(mid);

	/* initialize everything to 0 */
	memset(mid, 0, sizeof(*mid));

	/* then fill in some details */
	mid->index = index++;	/* for distinctions in log records */
	mid->auto_commit = 1;
	mid->error = MOK;
	mid->hostname = strdup("localhost");
	mid->server = NULL;
	mid->language = strdup("mil");

	mid->languageId = LANG_MIL;
	mid->versionId = 4;
	mid->noexplain = NULL;
	mid->motd = NULL;
	mid->mapiversion = "mapi 1.0";
	mid->username = NULL;
	mid->password = NULL;

	mid->cachelimit = 100;
	mid->redircnt = 0;
	mid->redirmax = 10;
	mid->tracelog = NULL;
	mid->blk.eos = 0;
	mid->blk.buf = malloc(BLOCK + 1);
	mid->blk.buf[BLOCK] = 0;
	mid->blk.buf[0] = 0;
	mid->blk.nxt = 0;
	mid->blk.end = 0;
	mid->blk.lim = BLOCK;

	mid->first = NULL;
	return mid;
}

/* Allocate a new connection handle and fill in the information needed
   to connect to a server, but don't connect yet. */
Mapi
mapi_mapi(const char *host, int port, const char *username, const char *password, const char *lang, const char *dbname)
{
	Mapi mid;

	if (!mapi_initialized) {
		mapi_initialized = 1;
		if (stream_init() < 0)
			return NULL;
	}

	mid = mapi_new();
	if (mid == NULL)
		return NULL;

	if (host) {
		free(mid->hostname);
		mid->hostname = strdup(host);
	}
	if (port == 0) {
		char *def;

		if ((def = getenv("MAPIPORT")) != NULL)
			port = atoi(def);
	}
	if (port == 0)
		port = 50000;	/* hardwired default */

	/* fill some defaults for user/pass, this should actually never happen */
	if (username == NULL)
		username = "guest";
	if (mid->username != NULL)
		free(mid->username);
	mid->username = strdup(username);

	if (password == NULL)
		password = "guest";
	if (mid->password)
		free(mid->password);
	mid->password = strdup(password);

	mid->port = port;

	if (lang == NULL)
		lang = "mil";
	free(mid->language);
	mid->language = strdup(lang);

	if (strcmp(lang, "mil") == 0)
		mid->languageId = LANG_MIL;
	else if (strcmp(lang, "mal") == 0)
		mid->languageId = LANG_MAL;
	else if (strcmp(lang, "sql") == 0)
		mid->languageId = LANG_SQL;
	else if (strcmp(lang, "xquery") == 0)
		mid->languageId = LANG_XQUERY;

	if (mid->database)
		free(mid->database);
	mid->database = dbname ? strdup(dbname) : NULL;

	return mid;
}

/* Close a connection and free all memory associated with the
   connection handle. */
MapiMsg
mapi_destroy(Mapi mid)
{
	char **r;

	mapi_clrError(mid);

	while (mid->first)
		mapi_close_handle(mid->first);
	if (mid->connected)
		(void) mapi_disconnect(mid);
	if (mid->blk.buf)
		free(mid->blk.buf);
	if (mid->errorstr)
		free(mid->errorstr);
	if (mid->hostname)
		free(mid->hostname);
	if (mid->username)
		free(mid->username);
	if (mid->password)
		free(mid->password);
	if (mid->language)
		free(mid->language);

	if (mid->database)
		free(mid->database);
	if (mid->server)
		free(mid->server);

	r = mid->redirects;
	while (*r) {
		free(*r);
		r++;
	}

	free(mid);
	return MOK;
}

static void
parse_uri_query(Mapi mid, char *uri)
{
	char *amp;
	char *val;

	/* just don't care where it is, assume it all starts from '?' */
	if ((uri = strchr(uri, '?')) == NULL)
		return;

	uri++; /* skip '?' */

	while ((amp = strchr(uri, '&')) != NULL) {
		*amp++ = '\0';
		if ((val = strchr(uri, '=')) != NULL) {
			*val++ = '\0';
			if (strcmp("database", uri) == 0) {
				free(mid->database);
				mid->database = strdup(val);
			} else if (strcmp("language", uri) == 0) {
				free(mid->language);
				mid->language = strdup(val);
			} else if (strcmp("user", uri) == 0) {
				/* until we figure out how this can be done safely wrt
				 * security, ignore */
			} else if (strcmp("password", uri) == 0) {
				/* until we figure out how this can be done safely wrt
				 * security, ignore */
			} /* can't warn, ignore */
		} /* else: invalid argument, can't warn, just skip */
		uri = amp;
	}
}

/* (Re-)establish a connection with the server. */
static MapiMsg
connect_to_server(Mapi mid)
{
	struct sockaddr_in server;

#ifdef HAVE_SYS_UN_H
	struct sockaddr_un userver;
#endif
	struct sockaddr *serv;
	socklen_t servsize;
	SOCKET s;

	if (mid->connected)
		close_connection(mid);

#ifdef HAVE_SYS_UN_H
	if (mid->hostname && mid->hostname[0] == '/') {
		if (strlen(mid->hostname) >= sizeof(userver.sun_path)) {
			return mapi_setError(mid, "path name too long", "mapi_reconnect", MERROR);
		}
		userver.sun_family = AF_UNIX;
		strncpy(userver.sun_path, mid->hostname, sizeof(userver.sun_path));
		serv = (struct sockaddr *) &userver;
		servsize = sizeof(userver);
	} else
#endif
	{
		struct hostent *hp;

		hp = gethostbyname(mid->hostname);
		if (hp == NULL) {
			return mapi_setError(mid, "gethostbyname failed", "mapi_reconnect", MERROR);
		}
		memset(&server, 0, sizeof(server));
		memcpy(&server.sin_addr, hp->h_addr_list[0], hp->h_length);
		server.sin_family = hp->h_addrtype;
		server.sin_port = htons((unsigned short) (mid->port & 0xFFFF));
		serv = (struct sockaddr *) &server;
		servsize = sizeof(server);
	}

	s = socket(serv->sa_family, SOCK_STREAM, IPPROTO_TCP);
	if (s == INVALID_SOCKET) {
		return mapi_setError(mid, "Open socket failed", "mapi_reconnect", MERROR);
	}

	if (connect(s, serv, servsize) < 0) {
		return mapi_setError(mid, "Setup connection failed", "mapi_reconnect", MERROR);
	}
	mid->to = socket_wastream(s, "Mapi client write");
	mapi_log_record(mid, "Mapi client write");
	mid->from = socket_rastream(s, "Mapi client read");
	mapi_log_record(mid, "Mapi client read");
	check_stream(mid, mid->to, "Cannot open socket for writing", "mapi_reconnect", mid->error);
	check_stream(mid, mid->from, "Cannot open socket for reading", "mapi_reconnect", mid->error);

	mid->connected = 1;

	return MOK;
}

MapiMsg
mapi_start_talking(Mapi mid)
{
	char buf[BLOCK];
	size_t len;
	MapiHdl hdl;
	int pversion = 0;
	char *chal;
	char *server;
	char *protover;
	char *rest;

	if (!isa_block_stream(mid->to)) {
		mid->to = block_stream(mid->to);
		check_stream(mid, mid->to, stream_error(mid->to), "mapi_start_talking", mid->error);

		mid->from = block_stream(mid->from);
		check_stream(mid, mid->from, stream_error(mid->from), "mapi_start_talking", mid->error);
	}

	/* consume server challenge */
	len = stream_read_block(mid->from, buf, 1, BLOCK);

	check_stream(mid, mid->from, "Connection terminated", "mapi_start_talking", (mid->blk.eos = 1, mid->error));

	assert(len < BLOCK);

	/* buf at this point looks like "challenge:servertype:protover[:.*]" */
	chal = buf;
	server = strchr(chal, ':');
	if (server == NULL) {
		mapi_setError(mid, "Challenge string is not valid", "mapi_start_talking", MERROR);
		return mid->error;
	}
	*server++ = '\0';
	protover = strchr(server, ':');
	if (protover == NULL) {
		mapi_setError(mid, "Challenge string is not valid", "mapi_start_talking", MERROR);
		return mid->error;
	}
	*protover++ = '\0';
	rest = strchr(protover, ':');
	if (rest != NULL) {
		*rest++ = '\0';
	}
	pversion = atoi(protover);

	if (pversion < 8) {
		/* because the headers changed, and because it makes no sense to
		 * try and be backwards compatible, we bail out with a friendly
		 * message saying so.
		 */
		snprintf(buf, BLOCK, "Unsupported protocol version: %d.  "
			 "This client only supports version 8 and 9.  "
			 "Sorry, can't help you here!", pversion);
		mapi_setError(mid, buf, "mapi_start_talking", MERROR);
		return mid->error;
	} else if (pversion == 8 || pversion == 9) {
		char *hash = NULL;
		char *hashes = NULL;
		char *byteo = NULL;
		char *serverhash = NULL;

		/* rBuCQ9WTn3:mserver:9:RIPEMD160,SHA256,SHA1,MD5:LIT:SHA1: */

		/* the database has sent a list of supported hashes to us, it's
		 * in the form of a comma separated list and in the variable
		 * rest.  We try to use the strongest algorithm.
		 */
		hashes = rest;
		hash = strchr(hashes, ':');	/* temp misuse hash */
		if (hash) {
			*hash = '\0';
			rest = hash + 1;
		}
		/* in rest now should be the byte order of the server */
		byteo = rest;
		hash = strchr(byteo, ':');
		if (hash) {
			*hash = '\0';
			rest = hash + 1;
		}
		hash = NULL;

		/* Proto v9 is like v8, but mandates that the password is a
		 * hash, that is salted like in v8.  The hash algorithm is
		 * specified in the 6th field.  If we don't support it, we
		 * can't login. */
		if (pversion == 9) {
			serverhash = rest;
			hash = strchr(serverhash, ':');
			if (hash) {
				*hash = '\0';
				rest = hash + 1;
			}
			hash = NULL;
#ifndef HAVE_OPENSSL
			/* can't hash the password as required (no algorithms available) */
			snprintf(buf, BLOCK, "server requires '%s' hash, "
					"but client support was not compiled in",
					serverhash);
			close_connection(mid);
			return mapi_setError(mid, buf, "mapi_start_talking", MERROR);
#else
			/* hash password, if not already */
			if (mid->password[0] != '\1') {
				unsigned char md[64];	/* should be SHA512_DIGEST_LENGTH */
				int n = strlen(mid->password);
				char *key = alloca(n + 1);
				int len;

				key[0] = '\0';
				strncat(key, mid->password, n);

				if (strcmp(serverhash, "RIPEMD160") == 0) {
					RIPEMD160((unsigned char *)key, n, md);
					len = 40;
				} else if (strcmp(serverhash, "SHA512") == 0) {
					SHA512((unsigned char *)key, n, md);
					len = 128;
				} else if (strcmp(serverhash, "SHA384") == 0) {
					SHA384((unsigned char *)key, n, md);
					len = 96;
				} else if (strcmp(serverhash, "SHA256") == 0) {
					SHA256((unsigned char *)key, n, md);
					len = 64;
				} else if (strcmp(serverhash, "SHA224") == 0) {
					SHA224((unsigned char *)key, n, md);
					len = 56;
				} else if (strcmp(serverhash, "SHA1") == 0) {
					SHA1((unsigned char *)key, n, md);
					len = 40;
				} else if (strcmp(serverhash, "MD5") == 0) {
					MD5((unsigned char *)key, n, md);
					len = 32;
				} else {
					snprintf(buf, BLOCK, "server requires unknown hash '%s'",
							serverhash);
					close_connection(mid);
					return mapi_setError(mid, buf, "mapi_start_talking",
							MERROR);
				}

				free(mid->password);
				mid->password = malloc(sizeof(char) * (1 + 64 * 2 + 1));
				sprintf(mid->password,
						"\1%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
						"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
						"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
						"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
						"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
						"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
						"%02x%02x%02x%02x",
						md[0], md[1], md[2], md[3], md[4],
						md[5], md[6], md[7], md[8], md[9],
						md[10], md[11], md[12], md[13], md[14],
						md[15], md[16], md[17], md[18], md[19],
						md[20], md[21], md[22], md[23], md[24],
						md[25], md[26], md[27], md[28], md[29],
						md[30], md[31], md[32], md[33], md[34],
						md[35], md[36], md[37], md[38], md[39],
						md[40], md[41], md[42], md[43], md[44],
						md[45], md[46], md[47], md[48], md[49],
						md[50], md[51], md[52], md[53], md[54],
						md[55], md[56], md[57], md[58], md[59],
						md[60], md[61], md[62], md[63]
					   );
				mid->password[1 + len] = '\0';
			}
#endif
		}

		/* TODO: make this actually obey the separation by commas, and
		 * only allow full matches */
		if (1 == 0) {	/* language construct issue */
#ifdef HAVE_OPENSSL
		} else if (strstr(hashes, "RIPEMD160") != NULL) {
			/* The RIPEMD160 hash algorithm is a 160 bit hash.  In order to
			 * use in a string, a hexadecimal representation of the bit
			 * sequence is used.
			 */
			unsigned char md[20];	/* should be RIPEMD160_DIGEST_LENGTH */
			int n = strlen(mid->password) + strlen(chal);
			char *key = alloca(n + 1);
			key[0] = '\0';

			if (pversion == 9) {
				strcpy(key, mid->password + 1);
				n--;
			} else {
				strcpy(key, mid->password);
			}
			strncat(key, chal, strlen(chal));
			RIPEMD160((unsigned char *)key, n, md);
			hash = malloc(sizeof(char) * (/*{RIPEMD160}*/ 11 + 20 * 2 + 1));
			sprintf(hash, "{RIPEMD160}%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
				"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
				md[0], md[1], md[2], md[3], md[4],
				md[5], md[6], md[7], md[8], md[9],
				md[10], md[11], md[12], md[13], md[14],
				md[15], md[16], md[17], md[18], md[19]);
		} else if (strstr(hashes, "SHA1") != NULL) {
			/* The SHA-1 RSA hash algorithm is a 160 bit hash.  In order to
			 * use in a string, a hexadecimal representation of the bit
			 * sequence is used.
			 */
			unsigned char md[20];	/* should be SHA_DIGEST_LENGTH */
			int n = strlen(mid->password) + strlen(chal);
			char *key = alloca(n + 1);
			key[0] = '\0';

			if (pversion == 9) {
				strcpy(key, mid->password + 1);
				n--;
			} else {
				strcpy(key, mid->password);
			}
			strncat(key, chal, strlen(chal));
			SHA1((unsigned char *)key, n, md);
			hash = malloc(sizeof(char) * (/*{SHA1}*/ 6 + 20 * 2 + 1));
			sprintf(hash, "{SHA1}%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
				"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
				md[0], md[1], md[2], md[3], md[4],
				md[5], md[6], md[7], md[8], md[9],
				md[10], md[11], md[12], md[13], md[14],
				md[15], md[16], md[17], md[18], md[19]);
		} else if (strstr(hashes, "MD5") != NULL) {
			/* The MD5 hash algorithm is a 128 bit hash.  In order to
			 * use in a string, a hexadecimal representation of the bit
			 * sequence is used.
			 */
			unsigned char md[16];	/* should be MD5_DIGEST_LENGTH */
			int n = strlen(mid->password) + strlen(chal);
			char *key = alloca(n + 1);
			key[0] = '\0';

			if (pversion == 9) {
				strcpy(key, mid->password + 1);
				n--;
			} else {
				strcpy(key, mid->password);
			}
			strncat(key, chal, strlen(chal));
			MD5((unsigned char *)key, n, md);
			hash = malloc(sizeof(char) * (/*{MD5}*/ 5 + 16 * 2 + 1));
			sprintf(hash, "{MD5}%02x%02x%02x%02x%02x%02x%02x%02x"
				"%02x%02x%02x%02x%02x%02x%02x%02x",
				md[0], md[1], md[2], md[3], md[4],
				md[5], md[6], md[7], md[8], md[9],
				md[10], md[11], md[12], md[13], md[14], md[15]);
#endif
#ifdef HAVE_CRYPT
		} else if (pversion == 8 && strstr(hashes, "crypt") != NULL) {
			/* The crypt hash algorithm uses UNIX crypt, a modification of
			 * DES which uses a 2-char wide salt.  Because crypt only cares
			 * about the first eight characters of the given password, the
			 * challenge may not be taken into account at all.  As salt, the
			 * last two characters of the challenge are used.
			 * As of proto v9 this weak hash has been dropped. */
			char key[8];	/* NULL termination is not necessary */
			char salt[3];	/* NULL termination is a necessity! */
			char *cr;
			int n;

			/* prepare the key */
			n = strlen(mid->password);
			if (n >= 8) {
				strncpy(key, mid->password, 8);
			} else {
				/* pad with the challenge, we know it is always 8+ chars */
				strncpy(key, mid->password, n);
				strncpy(key + n, chal, 8 - n);
			}

			/* prepare the salt */
			n = strlen(chal);
			salt[0] = chal[n - 2];
			salt[1] = chal[n - 1];
			salt[2] = '\0';

			/* call crypt to do the work */
			cr = crypt(key, salt);
			assert(cr != NULL);
			hash = malloc(sizeof(char) * (/*{crypt}*/ 7 + strlen(cr) + 1));
			sprintf(hash, "{crypt}%s", cr);
#endif
		} else if (pversion == 8 && strstr(hashes, "plain") != NULL) {
			/* The plain text algorithm, doesn't really hash at all.  It's
			 * the easiest algorithm, as it just appends the challenge to
			 * the password and returns it.
			 * As of proto v9 this super insecure "hash" has been dropped. */
			if (strcmp(server, "merovingian") == 0) {
				hash = strdup("{plain}Mellon..."); /* Elfish word for friend */
			} else {
				hash = malloc(sizeof(char) * (/*{plain}*/ 7 +
								  strlen(mid->password) + strlen(chal) + 1));
				sprintf(hash, "{plain}%s%s", mid->password, chal);
			}
		}
		/* could use else here, but below looks cleaner */
		if (hash == NULL) {
			char *algo = strdup(hashes);

			/* the server doesn't support what we do (no plain?!?) */
			snprintf(buf, BLOCK, "unsupported hash algorithms: %s", algo);
			free(algo);
			close_connection(mid);
			return mapi_setError(mid, buf, "mapi_start_talking", MERROR);
		}

		stream_set_byteorder(mid->from, strcmp(byteo, "BIG") == 0);

		/* note: if we make the database field an empty string, it
		 * means we want the default.  However, it *should* be there. */
		snprintf(buf, BLOCK, "%s:%s:%s:%s:%s:\n",
#ifdef WORDS_BIGENDIAN
			 "BIG",
#else
			 "LIT",
#endif
			 mid->username, hash, mid->language,
			 mid->database == NULL ? "" : mid->database);

		free(hash);
	} else {
		/* we don't know what this is, so don't try to do anything with
		 * it */
		snprintf(buf, BLOCK, "Unsupported protocol version: %d  "
			 "Sorry, can't help you here!", pversion);
		mapi_setError(mid, buf, "mapi_start_talking", MERROR);
		return mid->error;
	}
	if (mid->trace == MAPI_TRACE) {
		printf("sending first request [%d]:%s", BLOCK, buf);
		fflush(stdout);
	}
	len = strlen(buf);
	stream_write(mid->to, buf, 1, len);
	mapi_log_record(mid, buf);
	check_stream(mid, mid->to, "Could not send initial byte sequence", "mapi_start_talking", mid->error);
	stream_flush(mid->to);
	check_stream(mid, mid->to, "Could not send initial byte sequence", "mapi_start_talking", mid->error);

	/* consume the welcome message from the server */
	hdl = mapi_new_handle(mid);
	mid->active = hdl;
	read_into_cache(hdl, 0);
	if (mid->error) {
		if (hdl) {
			/* propagate error from result to mid
			   mapi_close_handle clears the errors, so
			   save them first */
			char *errorstr = hdl->result ? hdl->result->errorstr : mid->errorstr;
			MapiMsg error = mid->error;

			if (hdl->result)
				hdl->result->errorstr = NULL;	/* clear these so errorstr doesn't get freed */
			mid->errorstr = NULL;
			mapi_close_handle(hdl);
			mapi_setError(mid, errorstr, "mapi_start_talking", error);
			free(errorstr);	/* now free it after a copy has been made */
		}
		return mid->error;
	}
	if (hdl->result && hdl->result->cache.line) {
		int i;
		size_t motdlen = 0;
		struct MapiResultSet *result = hdl->result;

		for (i = 0; i < result->cache.writer; i++) {
			if (result->cache.line[i].rows) {
				switch (result->cache.line[i].rows[0]) {
				case '#':
					motdlen += strlen(result->cache.line[i].rows) + 1;
					break;
				case '^': {
					char **r = mid->redirects;
					int m = sizeof(mid->redirects) - 1;
					for (; *r != NULL && m > 0; r++)
						m--;
					if (m == 0)
						break;
					*r++ = strdup(result->cache.line[i].rows + 1);
					*r = NULL;
				} break;
				}
			}
		}
		if (motdlen > 0) {
			mid->motd = malloc(motdlen + 1);
			*mid->motd = 0;
			for (i = 0; i < result->cache.writer; i++)
				if (result->cache.line[i].rows && result->cache.line[i].rows[0] == '#') {
					strcat(mid->motd, result->cache.line[i].rows);
					strcat(mid->motd, "\n");
				}
		}

		if (*mid->redirects != NULL) {
			char *red;
			char *p, *q;
			char **fr;

			/* redirect, looks like:
			 * ^mapi:monetdb://localhost:50001/test?lang=sql&user=monetdb
			 * or
			 * ^mapi:merovingian://proxy?database=test */

			/* first see if we reached our redirection limit */
			if (mid->redircnt >= mid->redirmax) {
				mapi_close_handle(hdl);
				mapi_setError(mid, "too many redirects",
						"mapi_start_talking", MERROR);
				return (mid->error);
			}
			/* we only implement following the first */
			red = mid->redirects[0];

			/* see if we can possibly handle the redirect */
			if (strncmp("mapi:monetdb", red, 12) == 0) {
				char *db = NULL;
				/* parse components (we store the args
				 * immediately in the mid... ok, that's dirty) */
				red += strlen("mapi:monetdb://");
				p = red;
				q = NULL;
				if ((red = strchr(red, ':')) != NULL) {
					*red++ = '\0';
					q = red;
				} else {
					red = p;
				}
				if ((red = strchr(red, '/')) != NULL) {
					*red++ = '\0';
					if (q != NULL) {
						mid->port = atoi(q);
						if (mid->port == 0)
							mid->port = 50000; /* hardwired default */
					}
					db = red;
				} else {
					red = p;
					db = NULL;
				}
				if (mid->hostname)
					free(mid->hostname);
				mid->hostname = strdup(p);
				if (mid->database) 
					free(mid->database);
				mid->database = db != NULL ? strdup(db) : NULL;

				parse_uri_query(mid, red);

				mid->redircnt++;
				mapi_close_handle(hdl);
				/* free all redirects */
				fr = mid->redirects;
				mid->redirects[0] = NULL;
				while (*fr != NULL) {
					free(*fr);
					*fr = NULL;
					fr++;
				}
				/* reconnect using the new values */
				return mapi_reconnect(mid);
			} else if (strncmp("mapi:merovingian", red, 16) == 0) {
				/* this is a proxy "offer", it means we should
				 * restart the login ritual, without
				 * disconnecting */
				parse_uri_query(mid, red + 16);
				mid->redircnt++;
				/* free all redirects */
				fr = mid->redirects;
				mid->redirects[0] = NULL;
				while (*fr != NULL) {
					free(*fr);
					*fr = NULL;
					fr++;
				}
				return mapi_start_talking(mid);
			} else {
				q = alloca(sizeof(char) * (strlen(red) + 50));
				snprintf(q, strlen(red) + 50,
						"error while parsing redirect: %s\n", red);
				mapi_close_handle(hdl);
				mapi_setError(mid, q, "mapi_start_talking", MERROR);
				return (mid->error);
			}
		}

		mid->versionId = strcmp("Mserver 5.0", result->cache.line[0].rows) == 0 ? 5 : 4;
	}
	mapi_close_handle(hdl);

	if (mid->trace == MAPI_TRACE)
		printf("connection established\n");
	if (mid->languageId == LANG_MAL)
		return mid->error;

	/* tell server about cachelimit */
	mapi_cache_limit(mid, mid->cachelimit);
	return mid->error;
}

MapiMsg
mapi_reconnect(Mapi mid)
{
	MapiMsg rc;

	rc = connect_to_server(mid);
	if (rc == MOK)
		rc = mapi_start_talking(mid);
	return rc;
}

/* Create a connection handle and connect to the server using the
   specified parameters. */
Mapi
mapi_connect(const char *host, int port, const char *username, const char *password, const char *lang, const char *dbname)
{
	Mapi mid;

	mid = mapi_mapi(host, port, username, password, lang, dbname);
	if (mid && mid->error == MOK)
		mapi_reconnect(mid);	/* actually, initial connect */
	return mid;
}

/* Returns an malloced NULL-terminated array with redirects */
char **
mapi_resolve(const char *host, int port, const char *pattern)
{
	int rmax;
	Mapi mid = mapi_mapi(host, port, "mero", "mero", "resolve", pattern);
	if (mid && mid->error == MOK) {
		rmax = mid->redirmax;
		mid->redirmax = 0;
		mapi_reconnect(mid);  /* real connect, don't follow redirects */
		mid->redirmax = rmax;
		if (mid->error == MOK) {
			close_connection(mid); /* we didn't expect a connection actually */
		} else {
			char **ret = malloc(sizeof(char *) * sizeof(mid->redirects));
			memcpy(ret, mid->redirects,
					sizeof(char *) * sizeof(mid->redirects));
			mid->redirects[0] = NULL;  /* make sure the members aren't freed */
			mapi_destroy(mid);
			return ret;
		}
	}
	mapi_destroy(mid);
	return NULL;
}

stream **
mapi_embedded_init(Mapi *midp, char *lang)
{
	Mapi mid;
	stream **streams;

	mid = mapi_mapi(NULL, -1, "monetdb", "monetdb", lang, NULL);
	if (mid == NULL)
		return NULL;
	streams = malloc(2 * sizeof(*streams));
	if (rendezvous_streams(&streams[0], &mid->to, "to server") == 0 ||
		rendezvous_streams(&mid->from, &streams[1], "from server") == 0) {
		mapi_destroy(mid);
		return NULL;
	}
	mapi_log_record(mid, "Connection established\n");
	mid->connected = 1;
	*midp = mid;
	return streams;
}

static void
close_connection(Mapi mid)
{
	MapiHdl hdl;
	struct MapiResultSet *result;

	mid->connected = 0;
	mid->active = NULL;
	for (hdl = mid->first; hdl; hdl = hdl->next) {
		hdl->active = NULL;
		for (result = hdl->result; result; result = result->next)
			result->tableid = -1;
	}
	/* finish channels */
	/* Make sure that the write- (to-) stream is closed first,
	 * as the related read- (from-) stream closes the shared
	 * socket; see also src/common/stream.mx:socket_close .
	 */
	if (mid->to) {
		stream_close(mid->to);
		stream_destroy(mid->to);
		mid->to = 0;
	}
	if (mid->from) {
		stream_close(mid->from);
		stream_destroy(mid->from);
		mid->from = 0;
	}
	mapi_log_record(mid, "Connection closed\n");
}

MapiMsg
mapi_disconnect(Mapi mid)
{
	mapi_check(mid, "mapi_disconnect");

	close_connection(mid);
	return MOK;
}

MapiMsg
mapi_bind(MapiHdl hdl, int fnr, char **ptr)
{
	testBinding(hdl, fnr, "mapi_bind");
	hdl->bindings[fnr].outparam = ptr;

	hdl->bindings[fnr].outtype = MAPI_AUTO;
	return MOK;
}

MapiMsg
mapi_bind_var(MapiHdl hdl, int fnr, int type, void *ptr)
{
	testBinding(hdl, fnr, "mapi_bind_var");
	hdl->bindings[fnr].outparam = ptr;

	if (type >= 0 && type < MAPI_NUMERIC)
		hdl->bindings[fnr].outtype = type;
	else
		return mapi_setError(hdl->mid, "Illegal SQL type identifier", "mapi_bind_var", MERROR);
	return MOK;
}

MapiMsg
mapi_bind_numeric(MapiHdl hdl, int fnr, int scale, int prec, void *ptr)
{
	if (mapi_bind_var(hdl, fnr, MAPI_NUMERIC, ptr))
		 return hdl->mid->error;

	hdl->bindings[fnr].scale = scale;
	hdl->bindings[fnr].precision = prec;
	return MOK;
}

MapiMsg
mapi_clear_bindings(MapiHdl hdl)
{
	mapi_hdl_check(hdl, "mapi_clear_bindings");
	if (hdl->bindings)
		memset(hdl->bindings, 0, hdl->maxbindings * sizeof(*hdl->bindings));
	return MOK;
}

MapiMsg
mapi_param_type(MapiHdl hdl, int fnr, int ctype, int sqltype, void *ptr)
{
	testParam(hdl, fnr, "mapi_param_type");
	hdl->params[fnr].inparam = ptr;

	if (ctype >= 0 && ctype < MAPI_NUMERIC)
		hdl->params[fnr].intype = ctype;
	else
		return mapi_setError(hdl->mid, "Illegal SQL type identifier", "mapi_param_type", MERROR);
	hdl->params[fnr].sizeptr = NULL;
	hdl->params[fnr].outtype = sqltype;
	hdl->params[fnr].scale = 0;
	hdl->params[fnr].precision = 0;
	return MOK;
}

MapiMsg
mapi_param_string(MapiHdl hdl, int fnr, int sqltype, char *ptr, int *sizeptr)
{
	testParam(hdl, fnr, "mapi_param_type");
	hdl->params[fnr].inparam = (void *) ptr;

	hdl->params[fnr].intype = MAPI_VARCHAR;
	hdl->params[fnr].sizeptr = sizeptr;
	hdl->params[fnr].outtype = sqltype;
	hdl->params[fnr].scale = 0;
	hdl->params[fnr].precision = 0;
	return MOK;
}

MapiMsg
mapi_param(MapiHdl hdl, int fnr, char **ptr)
{
	return mapi_param_type(hdl, fnr, MAPI_AUTO, MAPI_AUTO, ptr);
}

MapiMsg
mapi_param_numeric(MapiHdl hdl, int fnr, int scale, int prec, void *ptr)
{
	if (mapi_param_type(hdl, fnr, MAPI_NUMERIC, MAPI_NUMERIC, ptr))
		 return hdl->mid->error;

	hdl->params[fnr].scale = scale;
	hdl->params[fnr].precision = prec;
	return MOK;
}

MapiMsg
mapi_clear_params(MapiHdl hdl)
{
	mapi_hdl_check(hdl, "mapi_clear_params");
	if (hdl->params)
		memset(hdl->params, 0, hdl->maxparams * sizeof(*hdl->params));
	return MOK;
}

static void
mapi_check_query(MapiHdl hdl)
{
	char *c, bracket = 0, *comm = 0;

	assert(hdl->query);
	for (c = hdl->query; *c; c++) {
		if (*c == '\'' || *c == '"') {
			if (bracket == *c)
				bracket = 0;
			else if (bracket == 0)
				bracket = *c;
		}
		if (*c == '#' && bracket == 0) {
			comm = c;
			while (*c && *c != '\n')
				c++;
		}
	}
	if (comm) {
		/* remove comment and white space before comment start */
		while (--comm >= hdl->query && isspace((int) (unsigned char) *comm))
			;
		*++comm = 0;
	}
}

static MapiHdl
prepareQuery(MapiHdl hdl, const char *cmd)
{
	if (hdl && cmd) {
		if (hdl->query)
			free(hdl->query);
		hdl->query = strdup(cmd);
		assert(hdl->query);
		if (hdl->template) {
			free(hdl->template);
			hdl->template = NULL;
		}
		mapi_check_query(hdl);
	}
	return hdl;
}

MapiMsg
mapi_timeout(Mapi mid, int timeout)
{
	mapi_check(mid, "mapi_timeout");
	if (mid->trace == MAPI_TRACE)
		printf("Set timeout to %d\n", timeout);
	return mapi_setError(mid, "Operation not yet implemented", "mapi_timeout", MERROR);
}

static MapiMsg
mapi_Xcommand(Mapi mid, char *cmdname, char *cmdvalue)
{
	MapiHdl hdl;

	mapi_check(mid, "mapi_Xcommand");
	if (stream_printf(mid->to, "X" "%s %s\n", cmdname, cmdvalue) < 0 ||
		stream_flush(mid->to)) {
		close_connection(mid);
		mapi_setError(mid, stream_error(mid->to), "mapi_Xcommand", MTIMEOUT);
		return MERROR;
	}
	if (mid->tracelog) {
		mapi_log_header(mid, "W");
		stream_printf(mid->tracelog, "X" "%s %s\n", cmdname, cmdvalue);
		stream_flush(mid->tracelog);
	}
	hdl = prepareQuery(mapi_new_handle(mid), "Xcommand");
	mid->active = hdl;
	read_into_cache(hdl, 0);
	mapi_close_handle(hdl);	/* reads away any output */
	return MOK;
}

MapiMsg
mapi_prepare_handle(MapiHdl hdl, const char *cmd)
{
	mapi_hdl_check(hdl, "mapi_prepare_handle");
	if (finish_handle(hdl) != MOK)
		return MERROR;
	prepareQuery(hdl, cmd);
	hdl->template = strdup(hdl->query);
	assert(hdl->template);
	return hdl->mid->error;
}

MapiHdl
mapi_prepare(Mapi mid, const char *cmd)
{
	MapiHdl hdl;

	mapi_check0(mid, "mapi_prepare");
	hdl = mapi_new_handle(mid);
	mapi_prepare_handle(hdl, cmd);
	return hdl;
}

static MapiMsg
mapi_prepare_array_internal(MapiHdl hdl, char **val)
{
	int i;

	for (i = 0; val[i]; i++) {
		if (i >= hdl->maxparams)
			mapi_extend_params(hdl, i + 1);
		hdl->params[i].inparam = val[i];
		hdl->params[i].intype = MAPI_AUTO;
		hdl->params[i].outtype = MAPI_AUTO;
		hdl->params[i].sizeptr = NULL;
		hdl->params[i].scale = 0;
		hdl->params[i].precision = 0;
	}
	return MOK;
}

MapiHdl
mapi_prepare_array(Mapi mid, const char *cmd, char **val)
{
	MapiHdl hdl;

	mapi_check0(mid, "mapi_prepare_array");
	hdl = mapi_new_handle(mid);
	mapi_prepare_handle(hdl, cmd);
	mapi_prepare_array_internal(hdl, val);
	return hdl;
}



static void
mapi_param_store(MapiHdl hdl)
{
	char *val, buf[MAPIBLKSIZE];
	char *p = hdl->template, *q;
	int i;
	size_t k;
	size_t lim;

	if (hdl->template == 0)
		return;

	lim = strlen(hdl->template) + MAPIBLKSIZE;
	REALLOC(hdl->query, lim);
	assert(hdl->query);
	hdl->query[0] = 0;
	k = 0;

	q = strchr(hdl->template, PLACEHOLDER);
	i = 0;
	/* loop invariant: k == strlen(hdl->query) */
	while (q && i < hdl->maxparams) {
		if (q > p && *(q - 1) == '\\') {
			q = strchr(q + 1, PLACEHOLDER);
			continue;
		}

		strncpy(hdl->query + k, p, q - p);
		k += q - p;
		hdl->query[k] = 0;

		if (hdl->params[i].inparam == 0) {
			checkSpace(5);
			strcpy(hdl->query + k, hdl->mid->languageId == LANG_SQL ? "NULL" : "nil");
		} else {
			void *src = hdl->params[i].inparam;	/* abbrev */

			switch (hdl->params[i].intype) {
			case MAPI_TINY:
				checkSpace(5);
				sprintf(hdl->query + k, "%hhd", *(signed char *) src);
				break;
			case MAPI_UTINY:
				checkSpace(5);
				sprintf(hdl->query + k, "%hhu", *(unsigned char *) src);
				break;
			case MAPI_SHORT:
				checkSpace(10);
				sprintf(hdl->query + k, "%d", *(short *) src);
				break;
			case MAPI_USHORT:
				checkSpace(10);
				sprintf(hdl->query + k, "%hu", *(unsigned short *) src);
				break;
			case MAPI_INT:
				checkSpace(20);
				sprintf(hdl->query + k, "%d", *(int *) src);
				break;
			case MAPI_UINT:
				checkSpace(20);
				sprintf(hdl->query + k, "%u", *(unsigned int *) src);
				break;
			case MAPI_LONG:
				checkSpace(20);
				sprintf(hdl->query + k, "%ld", *(long *) src);
				break;
			case MAPI_ULONG:
				checkSpace(20);
				sprintf(hdl->query + k, "%lu", *(unsigned long *) src);
				break;
			case MAPI_LONGLONG:
				checkSpace(30);
				sprintf(hdl->query + k, LLFMT, *(mapi_int64 *) src);
				break;
			case MAPI_ULONGLONG:
				checkSpace(30);
				sprintf(hdl->query + k, ULLFMT, *(mapi_uint64 *) src);
				break;
			case MAPI_FLOAT:
				checkSpace(30);
				sprintf(hdl->query + k, "%.9g", *(float *) src);
				break;
			case MAPI_DOUBLE:
				checkSpace(20);
				sprintf(hdl->query + k, "%.17g", *(double *) src);
				break;
			case MAPI_DATE:
				checkSpace(50);
				sprintf(hdl->query + k,
					"DATE '%04d-%02hu-%02hu'",
					((MapiDate *) src)->year,
					((MapiDate *) src)->month,
					((MapiDate *) src)->day);
				break;
			case MAPI_TIME:
				checkSpace(60);
				sprintf(hdl->query + k,
					"TIME '%02hu:%02hu:%02hu'",
					((MapiTime *) src)->hour,
					((MapiTime *) src)->minute,
					((MapiTime *) src)->second);
				break;
			case MAPI_DATETIME:
				checkSpace(110);
				sprintf(hdl->query + k,
					"TIMESTAMP '%04d-%02hu-%02hu %02hu:%02hu:%02hu.%09u'",
					((MapiDateTime *) src)->year,
					((MapiDateTime *) src)->month,
					((MapiDateTime *) src)->day,
					((MapiDateTime *) src)->hour,
					((MapiDateTime *) src)->minute,
					((MapiDateTime *) src)->second,
					((MapiDateTime *) src)->fraction);
				break;
			case MAPI_CHAR:
				buf[0] = *(char *) src;
				buf[1] = 0;
				val = mapi_quote(buf, 1);
				checkSpace(strlen(val) + 3);
				sprintf(hdl->query + k, "'%s'", val);
				free(val);
				break;
			case MAPI_VARCHAR:
				val = mapi_quote((char *) src, hdl->params[i].sizeptr ? *hdl->params[i].sizeptr : -1);
				checkSpace(strlen(val) + 3);
				sprintf(hdl->query + k, "'%s'", val);
				free(val);
				break;
			default:
				strcpy(hdl->query + k, src);
				break;
			}
		}
		k += strlen(hdl->query + k);

		i++;
		p = q + 1;
		q = strchr(p, PLACEHOLDER);
	}
	checkSpace(strlen(p) + 1);
	strcpy(hdl->query + k, p);
	if (hdl->mid->trace == MAPI_TRACE)
		printf("param_store: result=%s\n", hdl->query);
	return;
}

/* Read one more line from the input stream and return it.  This
   returns a pointer into the input buffer, so the data needs to be
   copied if it is to be retained. */
static char *
read_line(Mapi mid)
{
	char *reply;
	char *nl;
	char *s;		/* from where to search for newline */

	if (mid->active == NULL)
		return 0;

	/* check if we need to read more blocks to get a new line */
	mid->blk.eos = 0;
	s = mid->blk.buf + mid->blk.nxt;
	while ((nl = strchr(s, '\n')) == NULL && !mid->blk.eos) {
		ssize_t len;

		if (mid->blk.lim - mid->blk.end < BLOCK) {
			int len;

			len = mid->blk.lim;
			if (mid->blk.nxt <= BLOCK) {
				/* extend space */
				len += BLOCK;
			}
			REALLOC(mid->blk.buf, len + 1);
			if (mid->blk.nxt > 0) {
				memmove(mid->blk.buf, mid->blk.buf + mid->blk.nxt, mid->blk.end - mid->blk.nxt + 1);
				mid->blk.end -= mid->blk.nxt;
				mid->blk.nxt = 0;
			}
			mid->blk.lim = len;
		}

		s = mid->blk.buf + mid->blk.end;

		/* fetch one more block */
		if (mid->trace == MAPI_TRACE)
			printf("fetch next block: start at:%d\n", mid->blk.end);
		len = stream_read(mid->from, mid->blk.buf + mid->blk.end, 1, BLOCK);
		if (mid->tracelog) {
			mapi_log_header(mid, "R");
			stream_write(mid->tracelog, mid->blk.buf + mid->blk.end, 1, len);
			stream_flush(mid->tracelog);
		}
		check_stream(mid, mid->from, "Connection terminated", "read_line", (mid->blk.eos = 1, (char *) 0));
		mid->blk.buf[mid->blk.end + len] = 0;
		if (mid->trace == MAPI_TRACE) {
			printf("got next block: length:" SSZFMT "\n", len);
			printf("text:%s\n", mid->blk.buf + mid->blk.end);
		}
		if (!len) {	/* add prompt */
			len = 2;
			mid->blk.buf[mid->blk.end] = PROMPTBEG;
			mid->blk.buf[mid->blk.end + 1] = '\n';
			mid->blk.buf[mid->blk.end + 2] = 0;
			if (!nl)
				nl = mid->blk.buf + mid->blk.end + 1;
		}
		mid->blk.end += (int) len;
	}
	if (mid->trace == MAPI_TRACE) {
		printf("got complete block: \n");
		printf("text:%s\n", mid->blk.buf + mid->blk.nxt);
	}

	/* we have a complete line in the buffer */
	assert(nl);
	*nl++ = 0;
	reply = mid->blk.buf + mid->blk.nxt;
	mid->blk.nxt = (int) (nl - mid->blk.buf);

	if (mid->trace == MAPI_TRACE)
		printf("read_line:%s\n", reply);
	return reply;
}

/* set or unset the autocommit flag in the server */
MapiMsg
mapi_setAutocommit(Mapi mid, int autocommit)
{
	if (mid->auto_commit == autocommit)
		return MOK;
	if (mid->languageId != LANG_SQL) {
		mapi_setError(mid, "autocommit only supported in SQL", "mapi_setAutocommit", MERROR);
		return MERROR;
	}
	mid->auto_commit = autocommit;
	if (autocommit)
		return mapi_Xcommand(mid, "auto_commit", "1");
	else
		return mapi_Xcommand(mid, "auto_commit", "0");
}

MapiMsg
mapi_setAlgebra(Mapi mid, int algebra)
{
	return mapi_Xcommand(mid, "algebra", algebra ? "1" : "0");
}

MapiMsg
mapi_output(Mapi mid, char *output)
{
	mapi_clrError(mid);
	if (mid->languageId == LANG_XQUERY)
		return mapi_Xcommand(mid, "output", output);
	return MOK;
}

MapiMsg
mapi_stream_into(Mapi mid, char *docname, char *colname, FILE *fp)
{
	char buf[BUFSIZ];
	int i;
	size_t length;
	MapiMsg rc;
	MapiHdl hdl;
	char *err;

	mapi_clrError(mid);
	if (mid->languageId != LANG_XQUERY)
		mapi_setError(mid, "only allowed in XQuery mode", "mapi_stream_into", MERROR);

	i = snprintf(buf, sizeof(buf), "%s%s%s", docname, colname ? "," : "", colname ? colname : "");
	if (i < 0)
		return MERROR;
	rc = mapi_Xcommand(mid, "copy", buf);
	if (rc != MOK)
		return rc;
	while ((length = fread(buf, 1, sizeof(buf), fp)) > 0) {
		stream_write(mid->to, buf, 1, length);
		check_stream(mid, mid->to, "write error on stream", "mapi_stream_into", mid->error);
	}
	stream_flush(mid->to);
	check_stream(mid, mid->to, "write error on stream", "mapi_stream_into", mid->error);
	hdl = mapi_new_handle(mid);
	mid->active = hdl;
	rc = read_into_cache(hdl, 0);
	if ((err = mapi_result_error(hdl)) != NULL)
		err = strdup(err);
	mapi_close_handle(hdl);
	if (err != NULL) {
		mapi_setError(mid, err, "mapi_stream_into", rc);
		free(err);
	}
	return rc;
}

MapiMsg
mapi_profile(Mapi mid, int flag)
{
	mapi_clrError(mid);
	mid->profile = flag;
	if (mid->profile && mid->languageId == LANG_XQUERY)
		return mapi_Xcommand(mid, "profile", "");
	return MOK;
}

MapiMsg
mapi_trace(Mapi mid, int flag)
{
	mapi_clrError(mid);
	mid->trace = flag;
	return MOK;
}

static int
slice_row(const char *reply, char *null, char ***anchorsp, int length, int endchar)
{
	/* This function does the actual work for splicing a real,
	   multi-column row into columns.  It skips over the first
	   character and ends at the end of the string or at endchar,
	   whichever comes first. */
	char *start;
	char **anchors;
	int i;

	reply++;		/* skip over initial char (usually '[') */
	i = 0;
	anchors = length == 0 ? NULL : malloc(length * sizeof(*anchors));
	do {
		if (i >= length) {
			length = i + 1;
			REALLOC(anchors, length);
		}
		if (!unquote(reply, &start, &reply, endchar) && null && strcmp(start, null) == 0) {
			/* indicate NULL/nil with NULL pointer */
			free(start);
			start = NULL;
		}
		anchors[i++] = start;
		while (reply && *reply && isspace((int) (unsigned char) *reply))
			reply++;
	} while (reply && *reply && *reply != endchar);
	*anchorsp = anchors;
	return i;
}

static MapiMsg
mapi_cache_freeup_internal(struct MapiResultSet *result, int k)
{
	int i;			/* just a counter */
	mapi_int64 n = 0;	/* # of tuples being deleted from front */

	result->cache.tuplecount = 0;
	for (i = 0; i < result->cache.writer - k; i++) {
		if (result->cache.line[i].rows) {
			if (result->cache.line[i].rows[0] == '[' ||
				result->cache.line[i].rows[0] == '=')
				n++;
			free(result->cache.line[i].rows);
		}
		result->cache.line[i].rows = result->cache.line[i + k].rows;
		result->cache.line[i + k].rows = 0;
		if (result->cache.line[i].anchors) {
			int j = 0;

			for (j = 0; j < result->cache.line[i].fldcnt; j++)
				free(result->cache.line[i].anchors[j]);
			free(result->cache.line[i].anchors);
		}
		result->cache.line[i].anchors = result->cache.line[i + k].anchors;
		result->cache.line[i + k].anchors = 0;
		result->cache.line[i].fldcnt = result->cache.line[i + k].fldcnt;
		if (result->cache.line[i].rows &&
			(result->cache.line[i].rows[0] == '[' ||
			 result->cache.line[i].rows[0] == '=')) {
			result->cache.line[i].tuplerev = result->cache.tuplecount;
			result->cache.line[result->cache.tuplecount++].tupleindex = i;
		}
	}
	/* after the previous loop, i == result->cache.writer - k, and
	   the last (result->cache.writer - k) cache entries have been
	   cleared already , so we don't need to go the Full Monty
	   here */
	for ( /*i = result->cache.writer - k */ ; i < k /*result->cache.writer */ ; i++) {
		if (result->cache.line[i].rows) {
			if (result->cache.line[i].rows[0] == '[' ||
				result->cache.line[i].rows[0] == '=')
				n++;
			free(result->cache.line[i].rows);
		}
		result->cache.line[i].rows = 0;
		if (result->cache.line[i].anchors) {
			int j = 0;

			for (j = 0; j < result->cache.line[i].fldcnt; j++)
				free(result->cache.line[i].anchors[j]);
			free(result->cache.line[i].anchors);
		}
		result->cache.line[i].anchors = 0;
		result->cache.line[i].fldcnt = 0;
	}
	result->cache.reader -= k;
	if (result->cache.reader < 0)
		result->cache.reader = -1;
	result->cache.writer -= k;
	if (result->cache.writer < 0)	/* "cannot happen" */
		result->cache.writer = 0;
	result->cache.first += n;

	return MOK;
}

static void
mapi_extend_cache(struct MapiResultSet *result, int cacheall)
{
	int incr, newsize, oldsize = result->cache.limit, i;

	/* if there are read entries, delete them */
	if (result->cache.reader >= 0) {
		mapi_cache_freeup_internal(result, result->cache.reader + 1);
		/* since we've made space, we can return */
		return;
	}

	/* extend row cache */
  retry:;
	if (oldsize == 0)
		incr = 100;
	else
		incr = oldsize * 2;
	if (incr > 200000)
		incr = 20000;
	newsize = oldsize + incr;
	if (result->cache.rowlimit > 0 && newsize > result->cache.rowlimit && !cacheall) {
		newsize = result->cache.rowlimit;
		incr = newsize - oldsize;
		if (incr <= 0) {
			/* not enough space, so increase limit and try again */
			result->cache.rowlimit += 100;
			goto retry;
		}
	}

	REALLOC(result->cache.line, newsize + 1);
	assert(result->cache.line);
	for (i = oldsize; i <= newsize; i++) {
		result->cache.line[i].fldcnt = 0;
		result->cache.line[i].rows = NULL;
		result->cache.line[i].tupleindex = -1;
		result->cache.line[i].tuplerev = -1;
		result->cache.line[i].anchors = NULL;
	}
	result->cache.limit = newsize;
}

/* store a line in the cache */
static void
add_cache(struct MapiResultSet *result, char *line, int cacheall)
{
	/* manage the row cache space first */
	if (result->cache.writer >= result->cache.limit)
		mapi_extend_cache(result, cacheall);

	result->cache.line[result->cache.writer].rows = line;
	result->cache.line[result->cache.writer].tuplerev = result->cache.tuplecount;
	result->cache.line[result->cache.writer + 1].tuplerev = result->cache.tuplecount + 1;
	if (*line == '[' || *line == '=') {
		result->cache.line[result->cache.tuplecount++].tupleindex = result->cache.writer;
		if (result->row_count < result->cache.first + result->cache.tuplecount)
			result->row_count = result->cache.first + result->cache.tuplecount;
	}
	result->cache.writer++;
}

static struct MapiResultSet *
parse_header_line(MapiHdl hdl, char *line, struct MapiResultSet *result)
{
	char *tag, *etag;
	int i, n;
	char **anchors;

	if (line[0] == '&') {
		char *nline = line;
		int qt;

		/* handle fields &qt */

		nline++;	/* query type */
		qt = strtol(nline, &nline, 0);

		if (qt != Q_BLOCK || result == NULL)
			result = new_result(hdl);
		result->querytype = qt;

		nline++;	/* skip space */
		switch (qt) {
		case Q_TRANS:
			if (*nline == 'f')
				hdl->mid->auto_commit = 0;
			else
				hdl->mid->auto_commit = 1;
			break;
		case Q_UPDATE:
			result->row_count = strtol(nline, &nline, 0);
			result->last_id = strtol(nline, &nline, 0);
			break;
		case Q_TABLE:
		case Q_PREPARE:{
			int ntuples;	/* not used */

			sscanf(nline, "%d " LLFMT " %d %d", &result->tableid, &result->row_count, &result->fieldcnt, &ntuples);
			break;
		}
		case Q_BLOCK:
			/* Mapi ignores the Q_BLOCK header, so spoof the querytype
			 * back to a Q_TABLE to let it go unnoticed */
			result->querytype = Q_TABLE;
			break;
		}


		if (result->fieldcnt > result->maxfields) {
			REALLOC(result->fields, result->fieldcnt);
			memset(result->fields + result->maxfields, 0, (result->fieldcnt - result->maxfields) * sizeof(*result->fields));
			result->maxfields = result->fieldcnt;
		}

		/* start of new SQL result */
		return result;
	}
	if (result == NULL)
		result = new_result(hdl);

	if (line[0] == '#' && hdl->mid->languageId != LANG_MAL && hdl->mid->languageId != LANG_MIL) {
		/* comment */
		return result;
	}

	line = strdup(line);	/* make copy we can play with */
	etag = strrchr(line, '#');
	if (etag == 0 || etag == line) {
		/* not a useful header line */
		free(line);
		return result;
	}

	n = slice_row(line, NULL, &anchors, 10, '#');

	tag = etag + 1;
	while (*tag && isspace((int) (unsigned char) *tag))
		tag++;

	if (n > result->fieldcnt) {
		result->fieldcnt = n;
		if (n > result->maxfields) {
			REALLOC(result->fields, n);
			memset(result->fields + result->maxfields, 0, (n - result->maxfields) * sizeof(*result->fields));
			result->maxfields = n;
		}
	}

	if (strcmp(tag, "name") == 0) {
		for (i = 0; i < n; i++) {
			if (anchors[i]) {
				if (result->fields[i].columnname)
					free(result->fields[i].columnname);
				result->fields[i].columnname = anchors[i];
				anchors[i] = NULL;
			}
		}
	} else if (strcmp(tag, "type") == 0) {
		for (i = 0; i < n; i++) {
			if (anchors[i]) {
				if (result->fields[i].columntype)
					free(result->fields[i].columntype);
				result->fields[i].columntype = anchors[i];
				anchors[i] = NULL;
			}
		}
	} else if (strcmp(tag, "length") == 0) {
		for (i = 0; i < n; i++) {
			if (anchors[i])
				result->fields[i].columnlength = atoi(anchors[i]);
		}
	} else if (strcmp(tag, "table_name") == 0) {
		for (i = 0; i < n; i++) {
			if (anchors[i]) {
				if (result->fields[i].tablename)
					free(result->fields[i].tablename);
				result->fields[i].tablename = anchors[i];
				anchors[i] = NULL;
			}
		}
	}

	/* clean up */
	free(line);
	for (i = 0; i < n; i++)
		if (anchors[i])
			free(anchors[i]);
	free(anchors);

	return result;
}

/* Read ahead and cache data read.  Depending on the second argument,
   reading may stop at the first non-header and non-error line, or at
   a prompt.
   This function is called either after a command has been sent to the
   server (in which case the second argument is 1), when the
   application asks for a result tuple that hadn't been cached yet (in
   which case the second argument is also 1), or whenever all pending
   data needs to be read in order to send a new command to the server
   (in which case the second argument is 0).
   Header lines result tuples are stored in the cache.  Certain header
   lines may cause a new result set to be created in which case all
   subsequent lines are added to that result set.
*/
static MapiMsg
read_into_cache(MapiHdl hdl, int lookahead)
{
	char *line, *copy;
	Mapi mid;
	struct MapiResultSet *result;

	mid = hdl->mid;
	assert(mid->active == hdl);
	if (hdl->needmore) {
		hdl->needmore = 0;
		stream_flush(mid->to);
		check_stream(mid, mid->to, "write error on stream", "read_into_cache", mid->error);
	}
	if ((result = hdl->active) == NULL)
		result = hdl->result;	/* may also be NULL */
	for (;;) {
		line = read_line(mid);
		if (line == NULL)
			return mid->error;
		switch (*line) {
		case PROMPTBEG:
			mid->active = NULL;
			hdl->active = NULL;
			/* set needmore flag if line equals PROMPT2 up
			   to newline */
			copy = PROMPT2;
			while (*line) {
				if (*line != *copy)
					return mid->error;
				line++;
				copy++;
			}
			if (*copy == '\n' || *copy == 0) {
				/* skip end of block */
				mid->active = hdl;
				read_line(mid);
				hdl->needmore = 1;
				mid->active = hdl;
			}
			return mid->error;
		case '!':
			/* start a new result set if we don't have one
			   yet (duh!), or if we've already seen
			   normal output for the current one */
			if (result == NULL ||
				result->cache.writer > 0 ||
				result->querytype == Q_TABLE ||
				result->querytype == Q_UPDATE) {
				result = new_result(hdl);
				hdl->active = result;
			}
			add_error(result, line);
			if (!mid->error)
				mid->error = MSERVER;
			break;
		case '%':
		case '#':
		case '&':
			if (lookahead < 0)
				lookahead = 1;
			result = parse_header_line(hdl, line, result);
			hdl->active = result;
			if (result && *line != '&')
				add_cache(result, strdup(line), !lookahead);
			break;
		default:
			if (result == NULL) {
				result = new_result(hdl);
				hdl->active = result;
			}
			add_cache(result, strdup(line), !lookahead);
			if (lookahead > 0 && (result->querytype == -1 /* unknown (not SQL) */  ||
						  result->querytype == Q_TABLE ||
						  result->querytype == Q_UPDATE))
				return mid->error;
			break;
		}
	}
}

MapiMsg
mapi_virtual_result(MapiHdl hdl, int columns, const char **columnnames, const char **columntypes, const int *columnlengths, int tuplecount, const char ***tuples)
{
	Mapi mid;
	struct MapiResultSet *result;
	int i, n;
	const char **tuple;
	char **anchors;

	if (columns <= 0)
		return MERROR;
	mid = hdl->mid;
	if (mid->active && read_into_cache(mid->active, 0) != MOK)
		return MERROR;
	assert(mid->active == NULL);
	finish_handle(hdl);
	assert(hdl->result == NULL);
	assert(hdl->active == NULL);
	hdl->active = result = new_result(hdl);
	result->fieldcnt = result->maxfields = columns;
	REALLOC(result->fields, columns);
	memset(result->fields, 0, columns * sizeof(*result->fields));
	result->querytype = Q_TABLE;
	for (i = 0; i < columns; i++) {
		if (columnnames && columnnames[i])
			result->fields[i].columnname = strdup(columnnames[i]);
		if (columntypes && columntypes[i])
			result->fields[i].columntype = strdup(columntypes[i]);
		if (columnlengths)
			result->fields[i].columnlength = columnlengths[i];
	}
	if (tuplecount > 0) {
		result->row_count = tuplecount;
		result->cache.rowlimit = tuplecount;
	}

	for (tuple = *tuples++, n = 0; tuplecount < 0 ? tuple !=NULL : n < tuplecount; tuple = *tuples++, n++) {
		add_cache(result, strdup("[ ]"), 1);
		result->cache.line[n].fldcnt = columns;
		anchors = malloc(columns * sizeof(*anchors));
		result->cache.line[n].anchors = anchors;
		for (i = 0; i < columns; i++)
			anchors[i] = tuple[i] ? strdup(tuple[i]) : NULL;
	}
	hdl->active = NULL;
	return mid->error;
}

static MapiMsg
mapi_execute_internal(MapiHdl hdl)
{
	size_t size;
	char *cmd;
	Mapi mid;

	mid = hdl->mid;
	if (mid->active && read_into_cache(mid->active, 0) != MOK)
		return MERROR;
	assert(mid->active == NULL);
	finish_handle(hdl);
	mapi_param_store(hdl);
	cmd = hdl->query;
	if (cmd == NULL)
		return MERROR;
	size = strlen(cmd);

	if (mid->trace == MAPI_TRACE) {
		printf("mapi_query:" SZFMT ":%s\n", size, cmd);
	}
	if (mid->languageId == LANG_SQL || mid->languageId == LANG_XQUERY) {
		if (size > MAXQUERYSIZE) {
			/* If the query is large and we don't do
			   anything about it, deadlock may occur: the
			   client (we) blocks writing the query while
			   the server blocks writing the initial
			   results.  Therefore we split up large
			   queries into smaller batches.  The split is
			   done simplistically, but we are prepared to
			   receive the secondary prompt.  We cache the
			   results of all but the last batch.
			   There is  a problem  if auto-commit is  on:
			   the server  commits on batch boundaries, so
			   if one of our batches  ends at the end of a
			   (sub)query, the server commits prematurely.
			   Another problem is when the query we
			   received is incomplete.  We should tell the
			   server that there is no more, but we
			   don't. */
			size_t i = 0;

			while (i < size) {
				mid->active = hdl;
				stream_write(mid->to, "S", 1, 1);
				if (mid->tracelog) {
					mapi_log_header(mid, "W");
					stream_printf(mid->tracelog, "S");
				}
				check_stream(mid, mid->to, "write error on stream", "mapi_execute", mid->error);
				do {
					size_t n;

					hdl->needmore = 0;
					if ((n = QUERYBLOCK) > size - i)
						n = size - i;
					stream_write(mid->to, cmd + i, 1, n);
					if (mid->tracelog) {
						stream_write(mid->tracelog, cmd + i, 1, n);
						stream_flush(mid->tracelog);
					}
					check_stream(mid, mid->to, "write error on stream", "mapi_execute", mid->error);
					stream_flush(mid->to);
					check_stream(mid, mid->to, "write error on stream", "mapi_execute", mid->error);
					i += n;
					if (read_into_cache(hdl, 0) != MOK)
						return mid->error;
					if (i >= size && hdl->needmore) {
						hdl->needmore = 0;
						stream_flush(mid->to);
						check_stream(mid, mid->to, "write error on stream", "mapi_execute", mid->error);
					}
				} while (hdl->needmore);
			}
			return MOK;
		} else {
			/* indicate to server this is a SQL command */
			stream_write(mid->to, "s", 1, 1);
			if (mid->tracelog) {
				mapi_log_header(mid, "W");
				stream_write(mid->tracelog, "s", 1, 1);
				stream_flush(mid->tracelog);
			}
		}
	}
	stream_write(mid->to, cmd, 1, size);
	if (mid->tracelog) {
		stream_write(mid->tracelog, cmd, 1, size);
		stream_flush(mid->tracelog);
	}
	check_stream(mid, mid->to, "write error on stream", "mapi_execute", mid->error);
	/* all SQL statements should end with a semicolon */
	/* for the other languages it is assumed that the statements are correct */
	if (mid->languageId == LANG_SQL) {
		stream_write(mid->to, ";", 1, 1);
		check_stream(mid, mid->to, "write error on stream", "mapi_execute", mid->error);
		if (mid->tracelog) {
			stream_write(mid->tracelog, ";", 1, 1);
			stream_flush(mid->tracelog);
		}
	}
	stream_write(mid->to, "\n", 1, 1);
	if (mid->tracelog) {
		stream_write(mid->tracelog, "\n", 1, 1);
		stream_flush(mid->tracelog);
	}
	check_stream(mid, mid->to, "write error on stream", "mapi_execute", mid->error);
	stream_flush(mid->to);
	check_stream(mid, mid->to, "write error on stream", "mapi_execute", mid->error);
	mid->active = hdl;
	return read_into_cache(hdl, 1);
}

MapiMsg
mapi_execute(MapiHdl hdl)
{
	int ret;

	mapi_hdl_check(hdl, "mapi_execute");
	ret = mapi_execute_internal(hdl);
	return ret;
}

MapiMsg
mapi_execute_array(MapiHdl hdl, char **val)
{
	int ret;

	mapi_hdl_check(hdl, "mapi_execute_array");
	ret = mapi_prepare_array_internal(hdl, val);
	if (ret == MOK)
		ret = mapi_execute_internal(hdl);
	if (ret == MOK)
		ret = read_into_cache(hdl, 1);
	return ret;
}

MapiHdl
mapi_query(Mapi mid, const char *cmd)
{
	int ret;
	MapiHdl hdl;

	mapi_check0(mid, "mapi_query");
	hdl = prepareQuery(mapi_new_handle(mid), cmd);
	ret = mid->error;
	if (ret == MOK)
		ret = mapi_execute_internal(hdl);
	return hdl;
}

MapiMsg
mapi_query_handle(MapiHdl hdl, const char *cmd)
{
	int ret;

	mapi_hdl_check(hdl, "mapi_query_handle");
	if (finish_handle(hdl) != MOK)
		return MERROR;
	prepareQuery(hdl, cmd);
	ret = hdl->mid->error;
	if (ret == MOK)
		ret = mapi_execute_internal(hdl);
	return ret;
}

MapiHdl
mapi_query_array(Mapi mid, const char *cmd, char **val)
{
	int ret;
	MapiHdl hdl;

	mapi_check0(mid, "mapi_query_array");
	hdl = mapi_prepare(mid, cmd);
	ret = hdl->mid->error;
	if (ret == MOK)
		ret = mapi_prepare_array_internal(hdl, val);
	if (ret == MOK)
		ret = mapi_execute_internal(hdl);
	return hdl;
}

MapiHdl
mapi_query_prep(Mapi mid)
{
	mapi_check0(mid, "mapi_query_prep");
	if (mid->active && read_into_cache(mid->active, 0) != MOK)
		return NULL;
	assert(mid->active == NULL);
	if (mid->languageId == LANG_SQL || mid->languageId == LANG_XQUERY) {
		/* indicate to server this is a SQL command */
		stream_write(mid->to, "S", 1, 1);
		if (mid->tracelog) {
			mapi_log_header(mid, "W");
			stream_write(mid->tracelog, "S", 1, 1);
			stream_flush(mid->tracelog);
		}
	}
	return (mid->active = mapi_new_handle(mid));
}

MapiMsg
mapi_query_part(MapiHdl hdl, const char *query, size_t size)
{
	Mapi mid;

	mapi_hdl_check(hdl, "mapi_query_part");
	mid = hdl->mid;
	assert(mid->active == NULL || mid->active == hdl);
	mid->active = hdl;
	/* remember the query just for the error messages */
	if (hdl->query == NULL) {
		size_t sz = size;

		if (sz > 512)
			sz = 512;
		hdl->query = malloc(sz + 1);
		strncpy(hdl->query, query, sz);
		hdl->query[sz] = 0;
	}

	if (mid->trace == MAPI_TRACE) {
		printf("mapi_query_part:" SZFMT ":%.*s\n", size, (int) size, query);
	}
	hdl->needmore = 0;
	stream_write(mid->to, (char *) query, 1, size);
	if (mid->tracelog) {
		stream_write(mid->tracelog, (char *) query, 1, size);
		stream_flush(mid->tracelog);
	}
	check_stream(mid, mid->to, "write error on stream", "mapi_query_part", mid->error);
	return MOK;
}

MapiMsg
mapi_query_done(MapiHdl hdl)
{
	int ret;
	Mapi mid;

	mapi_hdl_check(hdl, "mapi_query_done");
	mid = hdl->mid;
	assert(mid->active == NULL || mid->active == hdl);
	mid->active = hdl;
	hdl->needmore = 0;
	stream_flush(mid->to);
	check_stream(mid, mid->to, "write error on stream", "mapi_query_done", mid->error);
	ret = mid->error;
	if (ret == MOK)
		ret = read_into_cache(hdl, 1);
	return ret == MOK && hdl->needmore ? MMORE : ret;
}

MapiHdl
mapi_quick_query(Mapi mid, const char *cmd, FILE *fd)
{
	int ret;
	MapiHdl hdl;

	mapi_check0(mid, "mapi_quick_query");
	hdl = prepareQuery(mapi_new_handle(mid), cmd);
	ret = hdl->mid->error;
	if (ret == MOK)
		ret = mapi_execute_internal(hdl);
	if (ret == MOK)
		ret = mapi_quick_response(hdl, fd);
	if (mid->trace == MAPI_TRACE)
		printf("mapi_quick_query return:%d\n", ret);
	return hdl;
}

MapiHdl
mapi_quick_query_array(Mapi mid, const char *cmd, char **val, FILE *fd)
{
	int ret;
	MapiHdl hdl;

	mapi_check0(mid, "mapi_quick_query_array");
	hdl = prepareQuery(mapi_new_handle(mid), cmd);
	ret = hdl->mid->error;
	if (ret == MOK)
		ret = mapi_prepare_array_internal(hdl, val);
	if (ret == MOK)
		ret = mapi_execute_internal(hdl);
	if (ret == MOK) {
		/* look ahead to detect errors */
		ret = mapi_quick_response(hdl, fd);
	}
	if (mid->trace == MAPI_TRACE)
		printf("mapi_quick_query return:%d\n", ret);
	return hdl;
}

MapiHdl
mapi_stream_query(Mapi mid, const char *cmd, int windowsize)
{
	MapiHdl hdl;
	int cachelimit = mid->cachelimit;

	mapi_check0(mid, "mapi_stream_query");

	mid->cachelimit = windowsize;
	hdl = mapi_query(mid, cmd);
	mid->cachelimit = cachelimit;
	if (hdl != NULL)
		mapi_cache_shuffle(hdl, 100);
	return hdl;
}

MapiMsg
mapi_cache_limit(Mapi mid, int limit)
{
	/* clean out superflous space TODO */
	mapi_check(mid, "mapi_cache_limit");
	mid->cachelimit = limit;
/* 	if (hdl->cache.rowlimit < hdl->cache.limit) { */
	/* TODO: decide what to do here */
	/*			  hdl->cache.limit = hdl->cache.rowlimit; *//* arbitrarily throw away cache lines */
/* 		if (hdl->cache.writer > hdl->cache.limit) { */
/* 			hdl->cache.writer = hdl->cache.limit; */
/* 			if (hdl->cache.reader > hdl->cache.writer) */
/* 				hdl->cache.reader = hdl->cache.writer; */
/* 		} */
/* 	} */
	if (mid->languageId == LANG_SQL) {
		MapiHdl hdl;

		if (mid->active)
			read_into_cache(mid->active, 0);

		if (mid->tracelog) {
			mapi_log_header(mid, "W");
			stream_printf(mid->tracelog, "X" "reply_size %d\n", limit);
			stream_flush(mid->tracelog);
		}
		if (stream_printf(mid->to, "X" "reply_size %d\n", limit) < 0 ||
			stream_flush(mid->to)) {
			close_connection(mid);
			mapi_setError(mid, stream_error(mid->to), "mapi_cache_limit", MTIMEOUT);
			return MERROR;
		}
		hdl = prepareQuery(mapi_new_handle(mid), "reply_size");
		mid->active = hdl;
		read_into_cache(hdl, 0);
		mapi_close_handle(hdl);	/* reads away any output */
	}
	return MOK;
}

MapiMsg
mapi_cache_shuffle(MapiHdl hdl, int percentage)
{
	/* clean out superflous space TODO */
	mapi_hdl_check(hdl, "mapi_cache_shuffle");
	if (percentage < 0 || percentage > 100) {
		return mapi_setError(hdl->mid, "Illegal percentage", "mapi_cache_shuffle", MERROR);
	}
	if (hdl->result)
		hdl->result->cache.shuffle = percentage;
	return MOK;
}

MapiMsg
mapi_fetch_reset(MapiHdl hdl)
{
	mapi_hdl_check(hdl, "mapi_fetch_reset");
	if (hdl->result)
		hdl->result->cache.reader = -1;
	return MOK;
}

MapiMsg
mapi_seek_row(MapiHdl hdl, mapi_int64 rownr, int whence)
{
	struct MapiResultSet *result;

	mapi_hdl_check(hdl, "mapi_seek_row");
	result = hdl->result;
	switch (whence) {
	case MAPI_SEEK_SET:
		break;
	case MAPI_SEEK_CUR:
		rownr += result->cache.line[result->cache.reader + 1].tuplerev;
		break;
	case MAPI_SEEK_END:
		if (hdl->mid->active && read_into_cache(hdl->mid->active, 0) != MOK)
			return MERROR;
		rownr += result->row_count;
		break;
	default:
		return mapi_setError(hdl->mid, "Illegal whence value", "mapi_seek_row", MERROR);
	}
	if (rownr > result->row_count && hdl->mid->active && read_into_cache(hdl->mid->active, 0) != MOK)
		return MERROR;
	if (rownr < 0 || rownr > result->row_count)
		return mapi_setError(hdl->mid, "Illegal row number", "mapi_seek_row", MERROR);
	if (result->cache.first <= rownr && rownr < result->cache.first + result->cache.tuplecount) {
		/* we've got the requested tuple in the cache */
		result->cache.reader = result->cache.line[rownr - result->cache.first].tupleindex - 1;
	} else {
		/* we don't have the requested tuple in the cache
		   reset the cache and at the next fetch we'll get the data */
		if (mapi_cache_freeup(hdl, 100) == MOK) {
			result->cache.first = rownr;
		}
	}
	return hdl->mid->error;
}

/* Make space in the cache for new tuples, ignore the read pointer */
MapiMsg
mapi_cache_freeup(MapiHdl hdl, int percentage)
{
	struct MapiResultSet *result;
	int k;			/* # of cache lines to be deleted from front */

	mapi_hdl_check(hdl, "mapi_cache_freeup");
	result = hdl->result;
	if (result == NULL || (result->cache.writer == 0 && result->cache.reader == -1))
		return MOK;
	if (percentage < 0 || percentage > 100)
		percentage = 100;
	k = (result->cache.writer * percentage) / 100;
	if (k < 1)
		k = 1;
	return mapi_cache_freeup_internal(result, k);
}

static char *
mapi_fetch_line_internal(MapiHdl hdl)
{
	Mapi mid;
	struct MapiResultSet *result;
	char *reply;

	/* try to read a line from the cache */
	if ((result = hdl->result) == NULL ||
		result->cache.writer <= 0 ||
		result->cache.reader + 1 >= result->cache.writer) {
		mid = hdl->mid;
		if (mid->active != hdl || hdl->needmore)
			return NULL;

		if (read_into_cache(hdl, 1) != MOK)
			return NULL;
		if ((result = hdl->result) == NULL ||
			result->cache.writer <= 0 ||
			result->cache.reader + 1 >= result->cache.writer)
			return NULL;
	}
	reply = result->cache.line[++result->cache.reader].rows;
	if (hdl->bindings && (*reply == '[' || *reply == '=')) {
		mapi_slice_row(result, result->cache.reader);
		mapi_store_bind(result, result->cache.reader);
	}
	return reply;
}

char *
mapi_fetch_line(MapiHdl hdl)
{
	char *reply;
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl, "mapi_fetch_line");
	reply = mapi_fetch_line_internal(hdl);
	if (reply == NULL &&
		(result = hdl->result) != NULL &&
		hdl->mid->languageId == LANG_SQL &&
		result->querytype == Q_TABLE &&
		result->row_count > 0 &&
		result->cache.first + result->cache.tuplecount < result->row_count) {
		if (hdl->needmore)	/* escalate */
			return NULL;
		if (hdl->mid->active != NULL)
			read_into_cache(hdl->mid->active, 0);
		hdl->mid->active = hdl;
		hdl->active = result;
		if (hdl->mid->tracelog) {
			mapi_log_header(hdl->mid, "W");
			stream_printf(hdl->mid->tracelog, "X" "export %d %d\n", result->tableid, result->cache.first + result->cache.tuplecount);
			stream_flush(hdl->mid->tracelog);
		}
		if (stream_printf(hdl->mid->to, "X" "export %d %d\n", result->tableid, result->cache.first + result->cache.tuplecount) < 0 ||
			stream_flush(hdl->mid->to))
			check_stream(hdl->mid, hdl->mid->to, stream_error(hdl->mid->to), "mapi_fetch_line", NULL);
		reply = mapi_fetch_line_internal(hdl);
	}
	return reply;
}

MapiMsg
mapi_finish(MapiHdl hdl)
{
	mapi_hdl_check(hdl, "mapi_finish");
	return finish_handle(hdl);
}

MapiMsg
mapi_quick_response(MapiHdl hdl, FILE *fd)
{
	char *line;

	mapi_hdl_check(hdl, "mapi_quick_response");
	do {
		if ((line = mapi_result_error(hdl)) != NULL)
			mapi_explain_result(hdl, fd);
		while ((line = mapi_fetch_line(hdl)) != NULL)
			fprintf(fd, "%s\n", line);
	} while (mapi_next_result(hdl) == 1);
	return hdl->mid->error ? hdl->mid->error : (hdl->needmore ? MMORE : MOK);
}

/* msg is a string consisting comma-separated values.  The list of
   values is terminated by endchar or by the end-of-string NULL byte.
   Values can be quoted strings or unquoted values.  Upon return,
   *start points to the start of the first value which is stripped of
   leading and trailing white space, and if it was a quoted string,
   also of the quotes.  Also, backslash-escaped characters in the
   quoted string are replaced by the values the escapes represent.
   *next points to either the start of the next value (i.e. after the
   separating comma, possibly to the leading white space of the next
   value), or to the trailing ] or NULL byte if this was the last
   value.
   msg is *not* a const string: it is altered by this function.
   The function returns true if the string was quoted.
*/
static int
unquote(const char *msg, char **str, const char **next, int endchar)
{
	const char *p = msg;
	char quote;

	/* first skip over leading white space */
	while (*p && isspace((int) (unsigned char) *p))
		p++;
	quote = *p;
	if (quote == '\'' || quote == '"') {
		size_t len = 0;
		char *s, *start;

		/* get quoted string and remove trailing bracket first */
		p++;
		/* first count how much space we need */
		msg = p;	/* save for later */
		while (*p && *p != quote) {
			if (*p == '\\') {
				p++;
				switch (*p) {
				case '0':
				case '1':
				case '2':
				case '3':
					/* this could be the start of
					   an octal sequence, check it
					   out */
					if (p[1] && p[2] && p[1] >= '0' && p[1] <= '7' && p[2] >= '0' && p[2] <= '7') {
						p += 2;
						break;
					}
					/* fall through */
				default:
					break;
				}
			}
			p++;
			len++;
		}
		/* now allocate space and copy string into new space */
		p = msg;	/* start over */
		start = s = malloc(len + 1);
		while (*p && *p != quote) {
			if (*p == '\\') {
				p++;
				switch (*p) {
				/* later
				   case '0': case '1': case '2': case '3': case '4':
				   case '5': case '6': case '7': case '8': case '9':
				 */
				case 'n':
					*s = '\n';
					break;
				case 't':
					*s = '\t';
					break;
				case 'r':
					*s = '\r';
					break;
				case 'f':
					*s = '\f';
					break;
				case '0':
				case '1':
				case '2':
				case '3':
					/* this could be the start of
					   an octal sequence, check it
					   out */
					if (p[1] && p[2] && p[1] >= '0' && p[1] <= '7' && p[2] >= '0' && p[2] <= '7') {
						*s = ((p[0] - '0') << 6) | ((p[1] - '0') << 3) | (p[2] - '0');
						p += 2;
						break;
					}
					/* fall through */
				default:
					*s = *p;
					break;
				}
				p++;
			} else {
				*s = *p++;
			}
			s++;
		}
		*s = 0;		/* close string */
		p++;		/* skip over end-of-string quote */
		/* skip over trailing junk (presumably white space) */
		while (*p && *p != ',' && *p != endchar)
			p++;
		if (*p == ',')
			p++;
		if (next)
			*next = p;
		*str = start;

		return 1;
	} else {
		const char *s;
		size_t len;

		/* p points at first non-white space character */
		msg = p;	/* record start of value */
		/* find separator or terminator */
		while (*p && *p != ',' && *p != '\t' && *p != endchar)
			p++;
		/* search back over trailing white space */
		for (s = p - 1; s > msg && isspace((int) (unsigned char) *s); s--)
			;
		if (s < msg || !isspace((int) (unsigned char) *s))	/* gone one too far */
			s++;
		if (*p == ',' || *p == '\t') {
			/* there is more to come; skip over separator */
			p++;
		}
		len = s - msg;
		*str = malloc(len + 1);
		strncpy(*str, msg, len);

		/* make sure value is NULL terminated */
		(*str)[len] = 0;
		if (next)
			*next = p;
		return 0;
	}
}

char *
mapi_unquote(char *msg)
{
	char *start;

	unquote(msg, &start, NULL, ']');
	return start;
}

char *
mapi_quote(const char *msg, int size)
{
	/* we absolutely don't need more than this (until we start
	   producing octal escapes */
	char *s = malloc((size < 0 ? strlen(msg) : (size_t) size) * 2 + 1);
	char *t = s;

	/* the condition is tricky: if initially size < 0, we must
	   continue until a NULL byte, else, size gives the number of
	   bytes to be copied */
	while (size < 0 ? *msg : size > 0) {
		if (size > 0)
			size--;
		switch (*msg) {
		case '\n':
			*t++ = '\\';
			*t++ = 'n';
			break;
		case '\t':
			*t++ = '\\';
			*t++ = 't';
			break;
		case PLACEHOLDER:
			*t++ = '\\';
			*t++ = PLACEHOLDER;
			break;
		case '\\':
			*t++ = '\\';
			*t++ = '\\';
			break;
		case '\'':
			*t++ = '\\';
			*t++ = '\'';
			break;
		case '"':
			*t++ = '\\';
			*t++ = '"';
			break;
		case '\0':
			*t++ = '\\';
			*t++ = '0';
			break;
		default:
			*t++ = *msg;
			break;
		}
		msg++;
		/* also deal with binaries */
	}
	*t = 0;
	return s;
}

static int
mapi_extend_bindings(MapiHdl hdl, int minbindings)
{
	/* extend the bindings table */
	int nm = hdl->maxbindings + 32;

	if (nm <= minbindings)
		 nm = minbindings + 32;
	REALLOC(hdl->bindings, nm);
	assert(hdl->bindings);
	/* clear new entries */
	memset(hdl->bindings + hdl->maxbindings, 0, (nm - hdl->maxbindings) * sizeof(*hdl->bindings));
	hdl->maxbindings = nm;
	return MOK;
}

static int
mapi_extend_params(MapiHdl hdl, int minparams)
{
	/* extend the params table */
	int nm = hdl->maxparams + 32;

	if (nm <= minparams)
		 nm = minparams + 32;
	REALLOC(hdl->params, nm);
	assert(hdl->params);
	/* clear new entries */
	memset(hdl->params + hdl->maxparams, 0, (nm - hdl->maxparams) * sizeof(*hdl->params));
	hdl->maxparams = nm;
	return MOK;
}

#if defined(HAVE_STRTOF) && !HAVE_DECL_STRTOF
extern float strtof(const char *, char **);
#endif
#if defined(HAVE_STRTOLL) && !HAVE_DECL_STRTOLL
extern long long strtoll(const char *, char **, int);
#endif
#if defined(HAVE_STRTOLL) && !HAVE_DECL_STRTOLL
extern unsigned long long strtoull(const char *, char **, int);
#endif

static MapiMsg
store_field(struct MapiResultSet *result, int cr, int fnr, int outtype, void *dst)
{
	char *val;

	val = result->cache.line[cr].anchors[fnr];

	if (val == 0) {
		return mapi_setError(result->hdl->mid, "Field value undefined or nil", "mapi_store_field", MERROR);
	}

	/* auto convert to C-type */
	switch (outtype) {
	case MAPI_TINY:
		*(signed char *) dst = (signed char) strtol(val, NULL, 0);
		break;
	case MAPI_UTINY:
		*(unsigned char *) dst = (unsigned char) strtoul(val, NULL, 0);
		break;
	case MAPI_SHORT:
		*(short *) dst = (short) strtol(val, NULL, 0);
		break;
	case MAPI_USHORT:
		*(unsigned short *) dst = (unsigned short) strtoul(val, NULL, 0);
		break;
	case MAPI_NUMERIC:
	case MAPI_INT:
		*(int *) dst = (int) strtol(val, NULL, 0);
		break;
	case MAPI_UINT:
		*(unsigned int *) dst = (unsigned int) strtoul(val, NULL, 0);
		break;
	case MAPI_LONG:
		*(long *) dst = strtol(val, NULL, 0);
		break;
	case MAPI_ULONG:
		*(unsigned long *) dst = strtoul(val, NULL, 0);
		break;
#ifdef HAVE_STRTOLL
	case MAPI_LONGLONG:
		*(mapi_int64 *) dst = strtoll(val, NULL, 0);
		break;
#endif
#ifdef HAVE_STRTOULL
	case MAPI_ULONGLONG:
		*(mapi_uint64 *) dst = strtoull(val, NULL, 0);
		break;
#endif
	case MAPI_CHAR:
		*(char *) dst = *val;
		break;
#ifdef HAVE_STRTOF
	case MAPI_FLOAT:
		*(float *) dst = strtof(val, NULL);
		break;
#endif
#ifdef HAVE_STRTOD
	case MAPI_DOUBLE:
		*(double *) dst = strtod(val, NULL);
		break;
#endif
	case MAPI_DATE:
		sscanf(val,
			   "%hd-%hu-%hu",
			   &((MapiDate *) dst)->year,
			   &((MapiDate *) dst)->month,
			   &((MapiDate *) dst)->day);
		break;
	case MAPI_TIME:
		sscanf(val,
			   "%hu:%hu:%hu",
			   &((MapiTime *) dst)->hour,
			   &((MapiTime *) dst)->minute,
			   &((MapiTime *) dst)->second);
		break;
	case MAPI_DATETIME:{
		int n;

		((MapiDateTime *) dst)->fraction = 0;
		sscanf(val,
			   "%hd-%hu-%hu %hu:%hu:%hu%n",
			   &((MapiDateTime *) dst)->year,
			   &((MapiDateTime *) dst)->month,
			   &((MapiDateTime *) dst)->day,
			   &((MapiDateTime *) dst)->hour,
			   &((MapiDateTime *) dst)->minute,
			   &((MapiDateTime *) dst)->second,
			   &n);
		if (val[n] == '.') {
			unsigned int fac = 1000000000;
			unsigned int nsec = 0;

			for (n++; isdigit((int) val[n]); n++) {
				fac /= 10;
				nsec += (val[n] - '0') * fac;
			}
			((MapiDateTime *) dst)->fraction = nsec;
		}
		break;
	}
	case MAPI_AUTO:
	case MAPI_VARCHAR:
	default:
		*(char **) dst = val;
	}
	return MOK;
}

MapiMsg
mapi_store_field(MapiHdl hdl, int fnr, int outtype, void *dst)
{
	struct MapiResultSet *result;

	mapi_hdl_check(hdl, "mapi_store_field");

	if ((result = hdl->result) == NULL) {
		return mapi_setError(hdl->mid, "No data read", "mapi_store_field", MERROR);
	}

	if (fnr < 0 || fnr >= result->fieldcnt) {
		return mapi_setError(hdl->mid, "Illegal field number", "mapi_store_field", MERROR);
	}

	return store_field(result, result->cache.reader, fnr, outtype, dst);
}

static void
mapi_store_bind(struct MapiResultSet *result, int cr)
{
	int i;
	MapiHdl hdl = result->hdl;

	for (i = 0; i < hdl->maxbindings; i++)
		if (hdl->bindings[i].outparam)
			store_field(result, cr, i, hdl->bindings[i].outtype, hdl->bindings[i].outparam);
}

static int
mapi_slice_row(struct MapiResultSet *result, int cr)
{
	char *p;
	int i = 0;

	p = result->cache.line[cr].rows;
	if (p == NULL)
		return mapi_setError(result->hdl->mid, "Current row missing", "mapi_slice_row", MERROR);
	if (result->cache.line[cr].fldcnt)
		return result->cache.line[cr].fldcnt;	/* already sliced */

	if (*p != '[') {
		/* nothing to slice */
		i = 1;
		REALLOC(result->cache.line[cr].anchors, 1);
		/* skip initial '=' if present */
		if (*p == '=')
			p++;
		result->cache.line[cr].anchors[0] = strdup(p);
	} else {
		/* work on a copy to preserve the original */
		p = strdup(p);
		i = slice_row(p, result->hdl->mid->languageId == LANG_SQL ? "NULL" : "nil", &result->cache.line[cr].anchors, result->fieldcnt, ']');
		free(p);
	}
	if (i > result->fieldcnt) {
		result->fieldcnt = i;
		if (i > result->maxfields) {
			REALLOC(result->fields, i);
			memset(result->fields + result->maxfields, 0, (i - result->maxfields) * sizeof(*result->fields));
			result->maxfields = i;
		}
	}
	result->cache.line[cr].fldcnt = i;
	return i;
}

int
mapi_split_line(MapiHdl hdl)
{
	int n;
	struct MapiResultSet *result;

	result = hdl->result;
	assert(result != NULL);
	if ((n = result->cache.line[result->cache.reader].fldcnt) == 0) {
		n = mapi_slice_row(result, result->cache.reader);
		/* no need to call mapi_store_bind since
		   mapi_fetch_line would have done that if needed */
	}
	return n;
}

int
mapi_fetch_row(MapiHdl hdl)
{
	char *reply;
	int n;
	struct MapiResultSet *result;

	mapi_hdl_check(hdl, "mapi_fetch_row");
	do {
		if ((reply = mapi_fetch_line(hdl)) == NULL)
			return 0;
	} while (*reply != '[' && *reply != '=');
	result = hdl->result;
	assert(result != NULL);
	if ((n = result->cache.line[result->cache.reader].fldcnt) == 0) {
		n = mapi_slice_row(result, result->cache.reader);
		/* no need to call mapi_store_bind since
		   mapi_fetch_line would have done that if needed */
	}
	return n;
}

mapi_int64
mapi_fetch_all_rows(MapiHdl hdl)
{
	Mapi mid;
	struct MapiResultSet *result;

	mapi_hdl_check(hdl, "mapi_fetch_all_rows");

	mid = hdl->mid;
	for (;;) {
		if ((result = hdl->result) != NULL &&
			mid->languageId == LANG_SQL &&
			mid->active == NULL &&
			result->row_count > 0 &&
			result->cache.first + result->cache.tuplecount < result->row_count) {
			mid->active = hdl;
			hdl->active = result;
			if (mid->tracelog) {
				mapi_log_header(mid, "W");
				stream_printf(mid->tracelog, "X" "export %d " LLFMT "\n", result->tableid, result->cache.first + result->cache.tuplecount);
				stream_flush(mid->tracelog);
			}
			if (stream_printf(mid->to, "X" "export %d " LLFMT "\n", result->tableid, result->cache.first + result->cache.tuplecount) < 0 ||
				stream_flush(mid->to))
				check_stream(mid, mid->to, stream_error(mid->to), "mapi_fetch_line", 0);
		}
		if (mid->active)
			read_into_cache(mid->active, 0);
		else
			break;
	}
	return result ? result->cache.tuplecount : 0;
}

char *
mapi_fetch_field(MapiHdl hdl, int fnr)
{
	int cr;
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl, "mapi_fetch_field");

	if ((result = hdl->result) == NULL ||
		(cr = result->cache.reader) < 0 ||
		(result->cache.line[cr].rows[0] != '[' &&
		 result->cache.line[cr].rows[0] != '=')) {
		mapi_setError(hdl->mid, "Must do a successful mapi_fetch_row first", "mapi_fetch_field", MERROR);
		return 0;
	}
	assert(result->cache.line != NULL);
	if (fnr >= 0) {
		/* slice if needed */
		if (result->cache.line[cr].fldcnt == 0)
			mapi_slice_row(result, cr);
		if (fnr < result->cache.line[cr].fldcnt)
			return result->cache.line[cr].anchors[fnr];
	}
	mapi_setError(hdl->mid, "Illegal field number", "mapi_fetch_field", MERROR);
	return 0;
}

char **
mapi_fetch_field_array(MapiHdl hdl)
{
	int cr;
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl, "mapi_fetch_field_array");

	if ((result = hdl->result) == NULL || (cr = result->cache.reader) < 0) {
		mapi_setError(hdl->mid, "Must do a successful mapi_fetch_row first", "mapi_fetch_field_array", MERROR);
		return 0;
	}
	assert(result->cache.line != NULL);
	/* slice if needed */
	if (result->cache.line[cr].fldcnt == 0)
		mapi_slice_row(result, cr);
	return result->cache.line[cr].anchors;
}

int
mapi_get_field_count(MapiHdl hdl)
{
	mapi_hdl_check(hdl, "mapi_get_field_count");
	if (hdl->result && hdl->result->fieldcnt == 0) {
		/* no rows have been sliced yet, and there was no
		   header, so try to figure out how many columns there
		   are for ourselves */
		int i;

		for (i = 0; i < hdl->result->cache.writer; i++)
			if (hdl->result->cache.line[i].rows[0] == '[' || hdl->result->cache.line[i].rows[0] == '=')
				mapi_slice_row(hdl->result, i);
	}
	return hdl->result ? hdl->result->fieldcnt : 0;
}

mapi_int64
mapi_get_row_count(MapiHdl hdl)
{
	mapi_hdl_check(hdl, "mapi_get_row_count");
	return hdl->result ? hdl->result->row_count : 0;
}

mapi_int64
mapi_get_last_id(MapiHdl hdl)
{
	mapi_hdl_check(hdl, "mapi_get_last_id");
	return hdl->result ? hdl->result->last_id : -1;
}

char *
mapi_get_name(MapiHdl hdl, int fnr)
{
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl, "mapi_get_name");
	if ((result = hdl->result) != 0 && fnr >= 0 && fnr < result->fieldcnt)
		return result->fields[fnr].columnname;
	mapi_setError(hdl->mid, "Illegal field number", "mapi_get_name", MERROR);
	return 0;
}

char *
mapi_get_type(MapiHdl hdl, int fnr)
{
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl, "mapi_get_type");
	if ((result = hdl->result) != 0 && fnr >= 0 && fnr < result->fieldcnt) {
		if (result->fields[fnr].columntype == NULL)
			return "unknown";
		return result->fields[fnr].columntype;
	}
	mapi_setError(hdl->mid, "Illegal field number", "mapi_get_type", MERROR);
	return 0;
}

char *
mapi_get_table(MapiHdl hdl, int fnr)
{
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl, "mapi_get_table");
	if ((result = hdl->result) != 0 && fnr >= 0 && fnr < result->fieldcnt)
		return result->fields[fnr].tablename;
	mapi_setError(hdl->mid, "Illegal field number", "mapi_get_table", MERROR);
	return 0;
}

int
mapi_get_len(MapiHdl hdl, int fnr)
{
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl, "mapi_get_len");
	if ((result = hdl->result) != 0 && fnr >= 0 && fnr < result->fieldcnt)
		return result->fields[fnr].columnlength;
	mapi_setError(hdl->mid, "Illegal field number", "mapi_get_len", MERROR);
	return 0;
}

char *
mapi_get_query(MapiHdl hdl)
{
	mapi_hdl_check0(hdl, "mapi_get_query");
	if (hdl->query != NULL) {
		return strdup(hdl->query);
	} else {
		return(NULL);
	}
}

int
mapi_get_querytype(MapiHdl hdl)
{
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl, "mapi_get_querytype");
	if ((result = hdl->result) != 0)
		return result->querytype;
	mapi_setError(hdl->mid, "No query result", "mapi_get_querytype", MERROR);
	return 0;
}

int
mapi_get_tableid(MapiHdl hdl)
{
	struct MapiResultSet *result;

	mapi_hdl_check0(hdl, "mapi_get_tableid");
	if ((result = hdl->result) != 0)
		return result->tableid;
	mapi_setError(hdl->mid, "No query result", "mapi_get_tableid", MERROR);
	return 0;
}

mapi_int64
mapi_rows_affected(MapiHdl hdl)
{
	struct MapiResultSet *result;

	mapi_hdl_check(hdl, "mapi_rows_affected");
	if ((result = hdl->result) == NULL)
		return 0;
	return result->row_count;
}

char *
mapi_get_dbname(Mapi mid)
{
	mapi_check0(mid, "mapi_get_dbname");
	return mid->database ? mid->database : "";
}

char *
mapi_get_host(Mapi mid)
{
	mapi_check0(mid, "mapi_get_host");
	return mid->hostname;
}

char *
mapi_get_user(Mapi mid)
{
	mapi_check0(mid, "mapi_get_user");
	return mid->username;
}

char *
mapi_get_lang(Mapi mid)
{
	mapi_check0(mid, "mapi_get_lang");
	return mid->language;
}

char *
mapi_get_mapi_version(Mapi mid)
{
	return mid->mapiversion;
}

char *
mapi_get_monet_version(Mapi mid)
{
	mapi_check0(mid, "mapi_get_monet_version");
	return mid->server ? mid->server : "";
}

int
mapi_get_monet_versionId(Mapi mid)
{
	mapi_check0(mid, "mapi_get_monet_versionId");
	return mid->versionId;
}

char *
mapi_get_motd(Mapi mid)
{
	mapi_check0(mid, "mapi_get_motd");
	return mid->motd;
}

int
mapi_is_connected(Mapi mid)
{
	return mid->connected;
}

MapiHdl
mapi_get_active(Mapi mid)
{
	return mid->active;
}

