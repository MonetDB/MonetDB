/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _MEROVINGIAN_H
#define _MEROVINGIAN_H 1

#include <netinet/in.h> /* struct sockaddr_in */
#include <pthread.h> /* pthread_mutex_t */
#include <signal.h>	 /* sig_atomic_t */

#include "utils/utils.h" /* confkeyval */

#define MERO_PORT      MAPI_PORT_STR
#define MERO_SOCK      ".s.monetdb."
#define CONTROL_SOCK   ".s.merovingian."

#ifdef HAVE_SOCKLEN_T
#define SOCKLEN socklen_t
#else
#define SOCKLEN int
#endif

typedef char* err;

#define freeErr(X) free(X)
#define getErrMsg(X) X
#define NO_ERR (err)0

/* when not writing to stderr, one has to flush, make it easy to do so */
#define Mfprintf(S, ...)						\
	do {										\
		if (S) {								\
			fprintf(S, __VA_ARGS__);			\
			fflush(S);							\
		}										\
	} while (0)

typedef enum _mtype {
	MERO = 1,
	MERODB,
	MEROFUN
} mtype;

typedef struct _dpair {
	struct _dpair_input {
		int fd;
		int cnt;
		char buf[8096];
		time_t ts;
	} input[2];
	mtype type;       /* type of process */
	short flag;		  /* flag internal to logListener */
	pid_t pid;        /* this process' id */
	char *dbname;     /* the database that this server serves */
	pthread_mutex_t fork_lock;
	struct _dpair* next;
}* dpair;

char *newErr(_In_z_ _Printf_format_string_ const char *fmt, ...)
	__attribute__((__format__(__printf__, 1, 2)));
bool terminateProcess(char *dbname, pid_t pid, mtype type);
void logFD(dpair dp, int fd, const char *type, const char *dbname, long long pid, FILE *stream, bool rest);

extern char *_mero_mserver;
extern dpair _mero_topdp;
extern pthread_mutex_t _mero_topdp_lock;
extern volatile int _mero_keep_logging;
extern volatile sig_atomic_t _mero_keep_listening;
extern FILE *_mero_logfile;
extern unsigned short _mero_port;
extern FILE *_mero_discout;
extern FILE *_mero_discerr;
extern unsigned short _mero_controlport;
extern FILE *_mero_ctlout;
extern FILE *_mero_ctlerr;
extern int _mero_broadcastsock;
extern struct sockaddr_in _mero_broadcastaddr;
extern char _mero_hostname[128];
extern confkeyval *_mero_db_props;
extern confkeyval *_mero_props;

#endif
