

/* this file contains all the structs from Mapi.c */

#define HAVE_LONG_LONG
#define HAVE___INT64

#include <mapi.h>
#include <stream.h>


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
	char *template_;		/* ATTENTION: template is a reserved word in C++, so renamed it to template_ */
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


