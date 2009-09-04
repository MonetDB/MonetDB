/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
 * Copyright August 2008-2009 MonetDB B.V.
 * All Rights Reserved.
 */

/**
 * Merovingian
 * Fabian Groffen
 * The MonetDB keeper
 *
 * The role of Merovingian within the MonetDB suite is to act as a smart
 * proxy, with capabilities to start mserver5s when necessary.
 * 
 * Since some people appear to have trouble pronouncing or remembering
 * its name, one can also refer to Merovingian as Mero.  In any case,
 * people having difficulties here should watch The Matrix once more.
 * 
 * Most of Merovingian's decisions are based on information provided by
 * Sabaoth.  Sabaoth is a file-system based administration shared
 * between all mserver5s in the same farm on a local machine.  It keeps
 * track of how mserver5s can be reached, with which scenarios, and what
 * the crashcounter of each server is.
 * 
 * Merovingian will fork off an mserver5 whenever a client requests a
 * database which is not running yet.  Sabaoth will assure Merovingian
 * can find already running or previously forked mserver5s.
 * 
 * While Merovingian currently just starts a database on the fly when a
 * client asks for it, in the future, Merovingian can decide to only
 * start a database if the crashlog information maintained by Sabaoth
 * shows that the mserver5 doesn't behave badly.  For example 
 * Merovingian can refuse to start an database if it has crashed a
 * number of times over a recent period.
 */

#define MERO_VERSION   "1.1"
#define MERO_PORT      50000

#include "sql_config.h"
#include "mal_sabaoth.h"
#include "utils.h"
#include "properties.h"
#include "glob.h"
#include <stdlib.h> /* exit, getenv, rand, srand */
#include <stdarg.h>	/* variadic stuff */
#include <stdio.h> /* fprintf */
#include <sys/types.h>
#include <sys/stat.h> /* stat */
#include <sys/wait.h> /* wait */
#include <sys/socket.h>
#include <sys/un.h>
#include <netdb.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h> /* unlink, isatty */
#include <string.h> /* strerror */
#ifdef HAVE_ALLOCA_H
#include <alloca.h>
#endif
#include <errno.h>
#include <signal.h> /* handle Ctrl-C, etc. */
#include <pthread.h>
#include <time.h>
#include <stream.h>
#include <stream_socket.h>

#define SOCKPTR struct sockaddr *
#ifdef HAVE_SOCKLEN_T
#define SOCKLEN socklen_t
#else
#define SOCKLEN int
#endif

typedef char* err;

#define freeErr(X) GDKfree(X)
#define getErrMsg(X) X
#define NO_ERR (err)0

/* when not writing to stderr, one has to flush, make it easy to do so */
#define Mfprintf(S, ...) \
	fprintf(S, __VA_ARGS__); \
	fflush(S);


/* private structs */

typedef struct _dpair {
	int out;          /* where to read stdout messages from */
	int err;          /* where to read stderr messages from */
	pid_t pid;        /* this process' id */
	str dbname;       /* the database that this server serves */
	struct _dpair* next;
}* dpair;

typedef struct _remotedb {
	str dbname;       /* remote database name */
	str tag;          /* database tag, if any, default = "" */
	str fullname;     /* dbname + tag */
	str conn;         /* remote connection, use in redirect */
	int ttl;          /* time-to-live in seconds */
	struct _remotedb* next;
}* remotedb;

typedef struct _threadlist {
	pthread_t tid;    /* thread id */
	struct _threadlist* next;
}* threadlist;


/* globals */

/* full path to the mserver5 binary */
static str _mero_mserver = NULL;
/* full path to the monetdb5 config file */
static str _mero_conffile = NULL;
/* list of databases that we have started */
static dpair _mero_topdp = NULL;
/* lock to _mero_topdp, initialised as recursive lateron */
static pthread_mutex_t _mero_topdp_lock;
/* list of remote databases as discovered */
static remotedb _mero_remotedbs = NULL;
/* lock to _mero_remotedbs */
static pthread_mutex_t _mero_remotedb_lock = PTHREAD_MUTEX_INITIALIZER;
/* for the logger, when set to 0, the logger terminates */
static int _mero_keep_logging = 1;
/* for accepting connections, when set to 0, listening socket terminates */
static int _mero_keep_listening = 1;
/* stream to the stdout output device (tty or file) */
static FILE *_mero_streamout = NULL;
/* stream to the stderr output device (tty or file) */
static FILE *_mero_streamerr = NULL;
/* timeout when waiting for a database to shutdown (seconds) */
static int _mero_exit_timeout = 60;
/* the port merovingian listens on for client connections */
static unsigned short _mero_port = MERO_PORT;
/* the time-to-live to announce for each shared database (seconds) */
static int _mero_discoveryttl = 600;
/* stream to the stdout for the neighbour discovery service */
static FILE *_mero_discout = NULL;
/* stream to the stderr for the neighbour discovery service */
static FILE *_mero_discerr = NULL;
/* broadcast socket for announcements */
static int _mero_broadcastsock;
/* broadcast address/port */
static struct sockaddr_in _mero_broadcastaddr;
/* hostname of this machine */
static char _mero_hostname[128];
/* full path to logfile for stdout messages, or NULL if tty */
static str _mero_msglogfile = NULL;
/* full path to logfile for stderr messages, or NULL if tty */
static str _mero_errlogfile = NULL;
/* default options read from config file */
static confkeyval *_mero_props = NULL;


/* funcs */

inline static void
logFD(int fd, char *type, char *dbname, long long int pid, FILE *stream)
{
	time_t now;
	char buf[8096];
	size_t len;
	char *p, *q;
	struct tm *tmp;
	char mytime[20];
	char writeident = 1;

	do {
		if ((len = read(fd, buf, 8095)) <= 0)
			break;
		buf[len] = '\0';
		q = buf;
		now = time(NULL);
		tmp = localtime(&now);
		strftime(mytime, sizeof(mytime), "%Y-%m-%d %H:%M:%S", tmp);
		while ((p = strchr(q, '\n')) != NULL) {
			if (writeident == 1)
				fprintf(stream, "%s %s %s[" LLFMT "]: ",
						mytime, type, dbname, pid);
			*p = '\0';
			fprintf(stream, "%s\n", q);
			q = p + 1;
			writeident = 1;
		}
		if ((size_t)(q - buf) < len) {
			if (writeident == 1)
				fprintf(stream, "%s %s %s[" LLFMT "]: ",
						mytime, type, dbname, pid);
			writeident = 0;
			fprintf(stream, "%s", q);
		}
	} while (len == 8095);
	fflush(stream);
}

static void
logListener(void *x)
{
	dpair d = _mero_topdp;
	dpair w;
	char equalouterr;
	struct timeval tv;
	fd_set readfds;
	int nfds;

	(void)x;

	equalouterr = 0;
	if (_mero_streamout == _mero_streamerr)
		equalouterr = 1;

	/* the first entry in the list of d is where our output should go to
	 * but we only use the streams, so we don't care about it in the
	 * normal loop */
	d = d->next;

	do {
		/* wait max 1 second, tradeoff between performance and being
		 * able to catch up new logger streams */
		tv.tv_sec = 1;
		tv.tv_usec = 0;
		FD_ZERO(&readfds);
		nfds = 0;

		/* make sure noone is killing or adding entries here */
		pthread_mutex_lock(&_mero_topdp_lock);

		w = d;
		while (w != NULL) {
			FD_SET(w->out, &readfds);
			if (nfds < w->out)
				nfds = w->out;
			FD_SET(w->err, &readfds);
			if (nfds < w->err)
				nfds = w->err;
			w = w->next;
		}

		pthread_mutex_unlock(&_mero_topdp_lock);
		
		if (select(nfds + 1, &readfds, NULL, NULL, &tv) <= 0)
			continue;

		pthread_mutex_lock(&_mero_topdp_lock);

		w = d;
		while (w != NULL) {
			if (FD_ISSET(w->out, &readfds) != 0)
				logFD(w->out, "MSG", w->dbname,
						(long long int)w->pid, _mero_streamout);
			if (w->err != w->out && FD_ISSET(w->err, &readfds) != 0)
				logFD(w->err, "ERR", w->dbname,
						(long long int)w->pid, _mero_streamerr);
			w = w->next;
		}

		pthread_mutex_unlock(&_mero_topdp_lock);

		fflush(_mero_streamout);
		if (equalouterr == 0)
			fflush(_mero_streamerr);
	} while (_mero_keep_logging != 0);
}

/**
 * The terminateProcess function tries to let the given mserver process
 * shut down gracefully within a given time-out.  If that fails, it
 * sends the deadly SIGKILL signal to the mserver process and returns.
 */
static void
terminateProcess(void *p)
{
	dpair d = (dpair)p;
	sabdb *stats;
	str er;
	int i;
	/* make local copies since d will disappear when killed */
	pid_t pid = d->pid;
	char *dbname = alloca(sizeof(char) * (strlen(d->dbname) + 1));
	memcpy(dbname, d->dbname, strlen(d->dbname) + 1);

	er = SABAOTHgetStatus(&stats, dbname);
	if (er != MAL_SUCCEED) {
		Mfprintf(stderr, "cannot terminate process " LLFMT ": %s\n",
				(long long int)pid, er);
		GDKfree(er);
		return;
	}

	if (stats == NULL) {
		Mfprintf(stderr, "strange, process " LLFMT " serves database '%s' "
				"which does not exist\n", (long long int)pid, dbname);
		return;
	}

	switch (stats->state) {
		case SABdbRunning:
			/* ok, what we expect */
		break;
		case SABdbCrashed:
			Mfprintf(stderr, "cannot shut down database '%s', mserver "
					"(pid " LLFMT ") has crashed\n",
					dbname, (long long int)pid);
			SABAOTHfreeStatus(&stats);
			return;
		case SABdbInactive:
			Mfprintf(stdout, "database '%s' appears to have shut down already\n",
					dbname);
			fflush(stdout);
			SABAOTHfreeStatus(&stats);
			return;
		default:
			Mfprintf(stderr, "unknown state: %d", (int)stats->state);
			SABAOTHfreeStatus(&stats);
			return;
	}

	/* ok, once we get here, we'll be shutting down the server */
	Mfprintf(stdout, "sending process " LLFMT " (database '%s') the "
			"TERM signal\n", (long long int)pid, dbname);
	kill(pid, SIGTERM);
	for (i = 0; i < _mero_exit_timeout * 2; i++) {
		if (stats != NULL)
			SABAOTHfreeStatus(&stats);
		MT_sleep_ms(500);
		er = SABAOTHgetStatus(&stats, dbname);
		if (er != MAL_SUCCEED) {
			Mfprintf(stderr, "unexpected problem: %s\n", er);
			GDKfree(er);
			/* don't die, just continue, so we KILL in the end */
		} else if (stats == NULL) {
			Mfprintf(stderr, "hmmmm, database '%s' suddenly doesn't exist "
					"any more\n", dbname);
		} else {
			switch (stats->state) {
				case SABdbRunning:
					/* ok, try again */
				break;
				case SABdbCrashed:
					Mfprintf (stderr, "database '%s' crashed after SIGTERM\n",
							dbname);
					SABAOTHfreeStatus(&stats);
					return;
				case SABdbInactive:
					Mfprintf(stdout, "database '%s' has shut down\n", dbname);
					fflush(stdout);
					SABAOTHfreeStatus(&stats);
					return;
				default:
					Mfprintf(stderr, "unknown state: %d", (int)stats->state);
				break;
			}
		}
	}
	Mfprintf(stderr, "timeout of %d seconds expired, sending process " LLFMT
			" (database '%s') the KILL signal\n",
			_mero_exit_timeout, (long long int)pid, dbname);
	kill(pid, SIGKILL);
	return;
}

/**
 * Creates a new error, allocated with malloc.  The error should be
 * freed using freeErr().
 */
static str
newErr(str fmt, ...)
{
	va_list ap;
	char message[4096];
	str ret;
	int len;

	va_start(ap, fmt);

	len = vsnprintf(message, 4095, fmt, ap);
	message[len] = '\0';

	va_end(ap);

	ret = GDKstrdup(message);
	return(ret);
}

/**
 * Fork an Mserver and detach.  Before forking off, Sabaoth is consulted
 * to see if forking makes sense, or whether it is necessary at all, or
 * forbidden by restart policy, e.g. when in maintenance.
 */
static err
forkMserver(str database, sabdb** stats, int force)
{
	pid_t pid;
	str er;
	sabuplog info;
	struct tm *t;
	char tstr[20];
	int pfdo[2];
	int pfde[2];
	dpair dp;
	str vaultkey = NULL;
	str nthreads = NULL;
	char mydoproxy;
	confkeyval *ckv, *kv;
	struct stat statbuf;
	char upmin[8];
	char upavg[8];
	char upmax[8];

	er = SABAOTHgetStatus(stats, database);
	if (er != MAL_SUCCEED) {
		err e = newErr("%s", er);
		GDKfree(er);
		return(e);
	}

	/* NOTE: remotes also include locals through self announcement */
	if (*stats == NULL) {
		struct _remotedb dummy = { NULL, NULL, NULL, NULL, 0, NULL };
		remotedb rdb = NULL;
		remotedb pdb = NULL;
		remotedb down = NULL;
		sabdb *walk = *stats;
		size_t dbsize = strlen(database);
		char *mdatabase = GDKmalloc(sizeof(char) * (dbsize + 2 + 1));
		char mfullname[8096];  /* should be enough for everyone... */

		/* each request has an implicit /'* (without ') added to match
		 * all sub-levels to the request, such that a request for e.g. X
		 * will return X/level1/level2/... */
		memcpy(mdatabase, database, dbsize + 1);
		if (dbsize <= 2 ||
				mdatabase[dbsize - 2] != '/' ||
				mdatabase[dbsize - 1] != '*')
		{
			mdatabase[dbsize++] = '/';
			mdatabase[dbsize++] = '*';
			mdatabase[dbsize++] = '\0';
		}

		/* check the remote databases, in private */
		pthread_mutex_lock(&_mero_remotedb_lock);

		dummy.next = _mero_remotedbs;
		rdb = dummy.next;
		pdb = &dummy;
		while (rdb != NULL) {
			snprintf(mfullname, sizeof(mfullname), "%s/", rdb->fullname);
			if (glob(mdatabase, mfullname) == 1) {
				/* create a fake sabdb struct, chain where necessary */
				if (walk != NULL) {
					walk = walk->next = GDKmalloc(sizeof(sabdb));
				} else {
					walk = *stats = GDKmalloc(sizeof(sabdb));
				}
				walk->dbname = GDKstrdup(rdb->dbname);
				walk->path = walk->dbname; /* only freed by sabaoth */
				walk->locked = 0;
				walk->state = SABdbRunning;
				walk->scens = GDKmalloc(sizeof(sablist));
				walk->scens->val = GDKstrdup("sql");
				walk->scens->next = NULL;
				walk->conns = GDKmalloc(sizeof(sablist));
				walk->conns->val = GDKstrdup(rdb->conn);
				walk->conns->next = NULL;
				walk->next = NULL;

				/* cut out first returned entry, put it down the list
				 * later, as to implement a round-robin DNS-like
				 * algorithm */
				if (down == NULL) {
					down = rdb;
					if (pdb->next == _mero_remotedbs) {
						_mero_remotedbs = pdb->next = rdb->next;
					} else {
						pdb->next = rdb->next;
					}
					rdb->next = NULL;
					rdb = pdb;
				}
			}
			pdb = rdb;
			rdb = rdb->next;
		}

		if (down != NULL)
			pdb->next = down;

		pthread_mutex_unlock(&_mero_remotedb_lock);

		GDKfree(mdatabase);

		if (*stats != NULL)
			return(NO_ERR);

		return(newErr("no such database: %s", database));
	}

	/* Since we ask for a specific database, it should be either there
	 * or not there.  Since we checked the latter case above, it should
	 * just be there, and be the right one.  There also shouldn't be
	 * more than one entries in the list, so we assume we have the right
	 * one here. */

	/* retrieve uplog information to print a short conclusion */
	er = SABAOTHgetUplogInfo(&info, *stats);
	if (er != MAL_SUCCEED) {
		err e = newErr("could not retrieve uplog information: %s", er);
		GDKfree(er);
		SABAOTHfreeStatus(stats);
		return(e);
	}

	if ((*stats)->locked == 1) {
		Mfprintf(stdout, "database '%s' is under maintenance\n", database);
		if (force == 0)
			return(NO_ERR);
	}

	switch ((*stats)->state) {
		case SABdbRunning:
			return(NO_ERR);
		break;
		case SABdbCrashed:
			t = localtime(&info.lastcrash);
			strftime(tstr, sizeof(tstr), "%Y-%m-%d %H:%M:%S", t);
			secondsToString(upmin, info.minuptime, 1);
			secondsToString(upavg, info.avguptime, 1);
			secondsToString(upmax, info.maxuptime, 1);
			Mfprintf(stdout, "database '%s' has crashed after start on %s, "
					"attempting restart, "
					"up min/avg/max: %s/%s/%s, "
					"crash average: %d.00 %.2f %.2f (%d-%d=%d)\n",
					database, tstr,
					upmin, upavg, upmax,
					info.crashavg1, info.crashavg10, info.crashavg30,
					info.startcntr, info.stopcntr, info.crashcntr);
		break;
		case SABdbInactive:
			secondsToString(upmin, info.minuptime, 1);
			secondsToString(upavg, info.avguptime, 1);
			secondsToString(upmax, info.maxuptime, 1);
			Mfprintf(stdout, "starting database '%s', "
					"up min/avg/max: %s/%s/%s, "
					"crash average: %d.00 %.2f %.2f (%d-%d=%d)\n",
					database,
					upmin, upavg, upmax,
					info.crashavg1, info.crashavg10, info.crashavg30,
					info.startcntr, info.stopcntr, info.crashcntr);
		break;
		default:
			SABAOTHfreeStatus(stats);
			return(newErr("unknown state: %d", (int)(*stats)->state));
	}

	if ((*stats)->locked == 1 && force == 1)
		Mfprintf(stdout, "startup of database under maintenance "
				"'%s' forced\n", database);

	/* check if the vaultkey is there, otherwise abort early (value
	 * lateron reused when server is started) */
	vaultkey = alloca(sizeof(char) * 512);
	snprintf(vaultkey, 511, "%s/.vaultkey", (*stats)->path);
	if (stat(vaultkey, &statbuf) == -1) {
		SABAOTHfreeStatus(stats);
		return(newErr("cannot start database '%s': no .vaultkey found "
					"(did you create the database with `monetdb create %s`?)",
					database, database));
	}

	ckv = getDefaultProps();
	readProps(ckv, (*stats)->path);

	kv = findConfKey(ckv, "forward");
	if (kv->val == NULL)
		kv = findConfKey(_mero_props, "forward");
	mydoproxy = strcmp(kv->val, "proxy") == 0;

	kv = findConfKey(ckv, "nthreads");
	if (kv->val == NULL)
		kv = findConfKey(_mero_props, "nthreads");
	if (kv->val != NULL) {
		nthreads = alloca(sizeof(char) * 24);
		snprintf(nthreads, 24, "gdk_nr_threads=%s", kv->val);
	}

	freeConfFile(ckv);
	GDKfree(ckv); /* can make ckv static and reuse it all the time */

	/* create the pipes (filedescriptors) now, such that we and the
	 * child have the same descriptor set */
	if (pipe(pfdo) == -1) {
		SABAOTHfreeStatus(stats);
		return(newErr("unable to create pipe: %s", strerror(errno)));
	}
	if (pipe(pfde) == -1) {
		close(pfdo[0]);
		close(pfdo[1]);
		SABAOTHfreeStatus(stats);
		return(newErr("unable to create pipe: %s", strerror(errno)));
	}

	pid = fork();
	if (pid == 0) {
		str conffile = alloca(sizeof(char) * 512);
		str dbname = alloca(sizeof(char) * 512);
		str port = alloca(sizeof(char) * 24);
		str argv[17];	/* for the exec arguments */
		int c = 0;

		/* redirect stdout and stderr to a new pair of fds for
		 * logging help */
		close(pfdo[0]);
		dup2(pfdo[1], 1);
		close(pfdo[1]);

		close(pfde[0]);
		dup2(pfde[1], 2);
		close(pfde[1]);

		/* ok, now exec that mserver we want */
		snprintf(conffile, 512, "--config=%s", _mero_conffile);
		snprintf(dbname, 512, "--dbname=%s", database);
		snprintf(vaultkey, 512, "monet_vault_key=%s/.vaultkey", (*stats)->path);
		/* avoid this mserver binding to the same port as merovingian
		 * but on another interface, (INADDR_ANY ... sigh) causing
		 * endless redirects since 0.0.0.0 is not a valid address to
		 * connect to, and hence the hostname is advertised instead */
		snprintf(port, 24, "mapi_port=%d", _mero_port + 1);
		argv[c++] = _mero_mserver;
		argv[c++] = conffile;
		argv[c++] = dbname;
		argv[c++] = "--dbinit=include sql;"; /* yep, no quotes needed! */
		argv[c++] = "--set"; argv[c++] = "monet_daemon=yes";
		if (mydoproxy == 1) {
			argv[c++] = "--set"; argv[c++] = "mapi_open=false";
		} else {
			argv[c++] = "--set"; argv[c++] = "mapi_open=true";
		}
		argv[c++] = "--set"; argv[c++] = "mapi_autosense=true";
		argv[c++] = "--set"; argv[c++] = port;
		argv[c++] = "--set"; argv[c++] = vaultkey;
		if (nthreads != NULL) {
			argv[c++] = "--set"; argv[c++] = nthreads;
		}
		argv[c++] = NULL;

		fprintf(stdout, "arguments:");
		for (c = 0; argv[c] != NULL; c++)
			fprintf(stdout, " %s", argv[c]);
		Mfprintf(stdout, "\n");

		execv(_mero_mserver, argv);
		/* if the exec returns, it is because of a failure */
		Mfprintf(stderr, "executing failed: %s\n", strerror(errno));
		exit(1);
	} else if (pid > 0) {
		int i;

		/* make sure no entries are shot while adding and that we
		 * deliver a consistent state */
		pthread_mutex_lock(&_mero_topdp_lock);

		/* parent: fine, let's add the pipes for this child */
		dp = _mero_topdp;
		while (dp->next != NULL)
			dp = dp->next;
		dp = dp->next = GDKmalloc(sizeof(struct _dpair));
		dp->out = pfdo[0];
		close(pfdo[1]);
		dp->err = pfde[0];
		close(pfde[1]);
		dp->next = NULL;
		dp->pid = pid;
		dp->dbname = GDKstrdup(database);

		pthread_mutex_unlock(&_mero_topdp_lock);

		/* wait for the child to open up a communication channel */
		for (i = 0; i < 20; i++) {	/* wait up to 10 seconds */
			/* give the database a break */
			MT_sleep_ms(500);
			/* stats cannot be NULL, as we don't allow starting non
			 * existing databases, note that we need to run this loop at
			 * least once not to leak */
			SABAOTHfreeStatus(stats);
			er = SABAOTHgetStatus(stats, database);
			if (er != MAL_SUCCEED) {
				/* since the client mserver lives its own life anyway,
				 * it's not really a problem we exit here */
				err e = newErr("%s", er);
				GDKfree(er);
				return(e);
			}
			if ((*stats)->state == SABdbRunning &&
					(*stats)->conns != NULL &&
					(*stats)->conns->val != NULL &&
					(*stats)->scens != NULL &&
					(*stats)->scens->val != NULL)
			{
				sablist *scen = (*stats)->scens;
				do {
					if (scen->val != NULL && strcmp(scen->val, "sql") == 0)
						break;
				} while ((scen = scen->next) != NULL);
				if (scen != NULL)
					break;
			}
		}
		/* if we've never found a connection, try to figure out why */
		if (i >= 20) {
			int state = (*stats)->state;
			dpair pdp;

			/* starting failed */
			SABAOTHfreeStatus(stats);

			/* in the meanwhile the list may have changed so refetch the
			 * parent and self */
			pthread_mutex_lock(&_mero_topdp_lock);
			dp = NULL;
			pdp = _mero_topdp;
			while (pdp != NULL && pdp->next != NULL && pdp->next->pid != pid)
				pdp = pdp->next;

			/* pdp is NULL when the database terminated somehow while
			 * starting */
			if (pdp != NULL) {
				pthread_mutex_unlock(&_mero_topdp_lock);
				switch (state) {
					case SABdbRunning:
						/* right, it's not there, but it's running */
						return(newErr(
									"database '%s' has inconsistent state "
									"(running but dead), review merovingian's "
									"logfile for any peculiarities", database));
					case SABdbCrashed:
						return(newErr(
									"database '%s' has crashed after starting, "
									"manual intervention needed, "
									"check merovingian's logfile for details",
									database));
					case SABdbInactive:
						return(newErr(
									"database '%s' appears to shut "
									"itself down after starting, "
									"check merovingian's logfile for possible "
									"hints", database));
					default:
						return(newErr("unknown state: %d", (int)(*stats)->state));
				}
			}

			/* in this case something seems still to be running, which
			 * we don't want */
			dp = pdp->next;
			terminateProcess(dp);
			pthread_mutex_unlock(&_mero_topdp_lock);

			switch (state) {
				case SABdbRunning:
					return(newErr(
								"timeout when waiting for database '%s' to "
								"open up a communication channel or to "
								"initialise the sql scenario", database));
				case SABdbCrashed:
					return(newErr(
								"database '%s' has crashed after starting, "
								"manual intervention needed, "
								"check merovingian's logfile for details",
								database));
				case SABdbInactive:
					/* due to GDK only locking once it has loaded all
					 * its stuff, Sabaoth cannot "see" if a database is
					 * starting up, or just shut down, this means that
					 * in this case GDK may still be trying to start up,
					 * or that it indeed cleanly shut itself down after
					 * starting... kill it in any case. */
					return(newErr(
								"database '%s' either needs a longer timeout "
								"to start up, or appears to shut "
								"itself down after starting, "
								"review merovingian's logfile for any "
								"peculiarities", database));
				default:
					return(newErr("unknown state: %d", (int)(*stats)->state));
			}
		}
		if ((*stats)->locked == 1) {
			Mfprintf(stdout, "database '%s' has been put into maintenance "
					"mode during startup\n", database);
		}

		return(NO_ERR);
	}
	/* forking failed somehow, cleanup the pipes */
	close(pfdo[0]);
	close(pfdo[1]);
	close(pfde[0]);
	close(pfde[1]);
	return(newErr(strerror(errno)));
}

typedef struct _merovingian_proxy {
	stream *in;      /* the input to read from and to dispatch to out */
	stream *out;     /* where to write the read input to */
	stream *co_in;   /* the input stream of the co-thread,
	                    don't read from this stream!  close only */
	stream *co_out;  /* the output stream of the co-thread,
	                    don't write to this stream!  close only */
	char *name;      /* a description to log when this thread ends */
	pthread_t co_thr;/* the other proxyThread */
} merovingian_proxy;

static void
proxyThread(void *d)
{
	merovingian_proxy *p = (merovingian_proxy *)d;
	int len;
	char data[8 * 1024];

	/* pass everything from in to out, until either reading from in, or
	 * writing to out fails, then close in and its related out-stream
	 * (not out!) to make sure the co-thread dies as well */
	while ((len = stream_read(p->in, data, 1, sizeof(data))) >= 0) {
		if (len > 0 && stream_write(p->out, data, len, 1) != 1)
			break;
		if (len == 0 &&	stream_flush(p->out) == -1)
			break;
	}

	stream_close(p->co_out);  /* out towards target B */
	stream_close(p->in);      /* related in from target B */

	stream_close(p->out);     /* out towards target A */
	stream_close(p->co_in);   /* related in from target A */

	if (p->name != NULL) {
		/* name is only set on the client-to-server thread */
		if (len <= 0) {
			Mfprintf(stdout, "client %s has disconnected from proxy\n",
					p->name);
		} else {
			Mfprintf(stdout, "server has terminated proxy connection, "
					"disconnecting client %s\n", p->name);
		}
		GDKfree(p->name);

		/* wait for the other thread to finish, after which we can
		 * finally destroy the streams */
		pthread_join(p->co_thr, NULL);
		stream_destroy(p->co_out);
		stream_destroy(p->in);
		stream_destroy(p->out);
		stream_destroy(p->co_in);
	}

	GDKfree(p);
}

static err
startProxy(stream *cfdin, stream *cfout, char *url, char *client)
{
	struct hostent *hp;
	struct sockaddr_in server;
	struct sockaddr *serv;
	socklen_t servsize;
	int ssock;
	char *port, *t;
	char *conn;
	stream *sfdin, *sfout;
	merovingian_proxy *pctos, *pstoc;
	pthread_t ptid;
	pthread_attr_t detachattr;

	/* quick 'n' dirty parsing */
	if (strncmp(url, "mapi:monetdb://", sizeof("mapi:monetdb://") - 1) == 0) {
		conn = alloca(sizeof(char) * (strlen(url) + 1));
		memcpy(conn, url, strlen(url) + 1);
		conn += sizeof("mapi:monetdb://") - 1;
		/* drop anything off after the hostname */
		if ((port = strchr(conn, ':')) != NULL) {
			*port = '\0';
			port++;
			if ((t = strchr(port, '/')) != NULL)
				*t = '\0';
		} else {
			return(newErr("can't find a port in redirect, "
						"this is not going to work: %s", url));
		}
	} else {
		return(newErr("unsupported protocol/scheme in redirect: %s", url));
	}

	hp = gethostbyname(conn);
	if (hp == NULL)
		return(newErr("cannot get address for hostname '%s': %s",
					conn, strerror(errno)));

	memset(&server, 0, sizeof(server));
	memcpy(&server.sin_addr, hp->h_addr_list[0], hp->h_length);
	server.sin_family = hp->h_addrtype;
	server.sin_port = htons((unsigned short) (atoi(port) & 0xFFFF));
	serv = (struct sockaddr *) &server;
	servsize = sizeof(server);

	ssock = socket(serv->sa_family, SOCK_STREAM, IPPROTO_TCP);
	if (ssock == INVALID_SOCKET)
		return(newErr("failed to open socket: %s", strerror(errno)));

	if (connect(ssock, serv, servsize) < 0)
		return(newErr("failed to connect: %s", strerror(errno)));

	sfdin = block_stream(socket_rastream(ssock, "merovingian<-server (proxy read)"));
	sfout = block_stream(socket_wastream(ssock, "merovingian->server (proxy write)"));

	if (sfdin == 0 || sfout == 0) {
		close_stream(sfout);
		close_stream(sfdin);
		return(newErr("merovingian-server inputstream or outputstream problems"));
	}

	/* our proxy schematically looks like this:
	 *
	 *                  A___>___B
	 *        out     in |     | out     in
	 * client  --------- |  M  | ---------  server
	 *        in     out |_____| in     out
	 *                  C   <   D
	 *
	 * the thread that does A -> B is called ctos, C -> D stoc
	 * the merovingian_proxy structs are filled like:
	 * ctos: in = A, out = B, co_in = D, co_out = C
	 * stoc: in = D, out = C, co_in = A, co_out = B
	 */

	pstoc = GDKmalloc(sizeof(merovingian_proxy));
	pstoc->in     = sfdin;
	pstoc->out    = cfout;
	pstoc->co_in  = cfdin;
	pstoc->co_out = sfout;
	pstoc->name   = NULL;  /* we want only one log-message on disconnect */
	pstoc->co_thr = 0;

	if (pthread_create(&ptid, NULL,
				(void *(*)(void *))proxyThread, (void *)pstoc) < 0)
	{
		close_stream(sfout);
		close_stream(sfdin);
		return(newErr("failed to create proxy thread"));
	}

	pctos = GDKmalloc(sizeof(merovingian_proxy));
	pctos->in     = cfdin;
	pctos->out    = sfout;
	pctos->co_in  = sfdin;
	pctos->co_out = cfout;
	pctos->name   = GDKstrdup(client);
	pctos->co_thr = ptid;

	pthread_attr_init(&detachattr);
	pthread_attr_setdetachstate(&detachattr, PTHREAD_CREATE_DETACHED);
	if (pthread_create(&ptid, &detachattr,
				(void *(*)(void *))proxyThread, (void *)pctos) < 0)
	{
		close_stream(sfout);
		close_stream(sfdin);
		return(newErr("failed to create proxy thread"));
	}

	return(NO_ERR);
}

#ifdef MYSQL_EMULATION_BLEEDING_EDGE_STUFF
static err
handleMySQLClient(int sock)
{
	stream *fdin, *fout;
	str buf = alloca(sizeof(char) * 8096);
	str p;
	int len;

	fdin = socket_rastream(sock, "merovingian<-mysqlclient (read)");
	if (fdin == 0)
		return(newErr("merovingian-mysqlclient inputstream problems"));

	fout = socket_wastream(sock, "merovingian->mysqlclient (write)");
	if (fout == 0) {
		close_stream(fdin);
		return(newErr("merovingian-mysqlclient outputstream problems"));
	}

#ifdef WORDS_BIGENDIAN
#define le_int(P, X) \
	*(P)++ = (unsigned int)X & 255; \
	*(P)++ = ((unsigned int)X >> 8) & 255; \
	*(P)++ = ((unsigned int)X >> 16) & 255; \
	*(P)++ = ((unsigned int)X >> 24) & 255;
#define le_sht(P, X) \
	*(P)++ = (unsigned short)X & 255; \
	*(P)++ = ((unsigned short)X >> 8) & 255;
#else
#define le_int(P, X) \
	*(P)++ = ((unsigned int)X >> 24) & 255; \
	*(P)++ = ((unsigned int)X >> 16) & 255; \
	*(P)++ = ((unsigned int)X >> 8) & 255; \
	*(P)++ = (unsigned int)X & 255;
#define le_sht(P, X) \
	*(P)++ = ((unsigned short)X >> 8) & 255; \
	*(P)++ = (unsigned short)X & 255;
#endif

	/* Handshake Initialization Packet */
	p = buf + 4;   /* skip bytes for package header */
	*p++ = 0x10;   /* protocol_version */
	p += sprintf(p, MERO_VERSION "-merovingian") + 1; /* server_version\0 */
	le_int(p, 0);  /* thread_number */
	p += sprintf(p, "voidvoid"); /* scramble_buff */
	*p++ = 0x00;   /* filler */
	/* server_capabilities:
	 * CLIENT_CONNECT_WITH_DB CLIENT_NO_SCHEMA CLIENT_PROTOCOL_41
	 * CLIENT_INTERACTIVE CLIENT_MULTI_STATEMENTS CLIENT_MULTI_RESULTS
	 */
	le_sht(p, (8 | 16 | 512 | 1024 | 8192 | 65536 | 131072));
	*p++ = 0x33;   /* server_language = utf8_general_ci */
	le_sht(p, 2);  /* server_status = SERVER_STATUS_AUTOCOMMIT */
	p += sprintf(p, "             ");  /* filler 14 bytes */

	/* packet header */
	len = p - buf;
	p = buf;
	le_int(p, len);
	*p = *(p + 1); p++;
	*p = *(p + 1); p++;
	*p = *(p + 1); p++;
	*p = 0x00;   /* packet number */
	stream_flush(fout);

	return(NO_ERR);
}
#endif

static err
handleClient(int sock)
{
	stream *fdin, *fout;
	str buf = alloca(sizeof(char) * 8096);
	char *user = NULL, *algo = NULL, *passwd = NULL, *lang = NULL;
	char *database = NULL, *s;
	char *host = NULL;
	sabdb *top = NULL;
	sabdb *stat = NULL;
	struct sockaddr_in saddr;
	socklen_t saddrlen = sizeof(struct sockaddr_in);
	err e;
	confkeyval *ckv, *kv;
	char mydoproxy;
	sabdb redirs[24];  /* do we need more? */
	int r = 0;

	fdin = socket_rastream(sock, "merovingian<-client (read)");
	if (fdin == 0)
		return(newErr("merovingian-client inputstream problems"));
	fdin = block_stream(fdin);

	fout = socket_wastream(sock, "merovingian->client (write)");
	if (fout == 0) {
		close_stream(fdin);
		return(newErr("merovingian-client outputstream problems"));
	}
	fout = block_stream(fout);

	/* note that we claim to speak proto 8 here */
	stream_printf(fout, "%s:merovingian:8:%s:%s",
			"void",  /* some bs */
			"md5,plain", /* we actually don't look at the password */
#ifdef WORDS_BIGENDIAN
			"BIG"
#else
			"LIT"
#endif
			);
	stream_flush(fout);

	/* get response */
	if (stream_read_block(fdin, buf, 8095, 1) < 0) {
		/* we didn't get a terminated block :/ */
		e = newErr("client sent challenge in incomplete block: %s", buf);
		stream_printf(fout, "!merovingian: client sent something this server could not understand, sorry\n", user);
		stream_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		return(e);
	}
	buf[8095] = '\0';

	/* decode BIG/LIT:user:{cypher}passwordchal:lang:database: line */

	user = buf;
	/* byte order */
	s = strchr(user, ':');
	if (s) {
		*s = 0;
		/* we don't use this in merovingian */
		/* stream_set_byteorder(fin->s, strcmp(user, "BIG") == 0); */
		user = s + 1;
	} else {
		e = newErr("client challenge error: %s", buf);
		stream_printf(fout, "!merovingian: incomplete challenge '%s'\n", user);
		stream_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		return(e);
	}

	/* passwd */
	s = strchr(user, ':');
	if (s) {
		*s = 0;
		passwd = s + 1;
		/* decode algorithm, i.e. {plain}mypasswordchallenge */
		if (*passwd != '{') {
			e = newErr("client challenge error: %s", buf);
			stream_printf(fout, "!merovingian: invalid password entry\n");
			stream_flush(fout);
			close_stream(fout);
			close_stream(fdin);
			return(e);
		}
		algo = passwd + 1;
		s = strchr(algo, '}');
		if (!s) {
			e = newErr("client challenge error: %s", buf);
			stream_printf(fout, "!merovingian: invalid password entry\n");
			stream_flush(fout);
			close_stream(fout);
			close_stream(fdin);
			return(e);
		}
		*s = 0;
		passwd = s + 1;
	} else {
		e = newErr("client challenge error: %s", buf);
		stream_printf(fout, "!merovingian: incomplete challenge '%s'\n", user);
		stream_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		return(e);
	}

	/* lang */
	s = strchr(passwd, ':');
	if (s) {
		*s = 0;
		lang = s + 1;
	} else {
		e = newErr("client challenge error: %s", buf);
		stream_printf(fout, "!merovingian: incomplete challenge, missing language\n");
		stream_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		return(e);
	}

	/* database */
	s = strchr(lang, ':');
	if (s) {
		*s = 0;
		database = s + 1;
		/* since we don't know where the string ends, we need to look
		 * for another : */
		s = strchr(database, ':');
		if (s == NULL) {
			e = newErr("client challenge error: %s", buf);
			stream_printf(fout, "!merovingian: incomplete challenge, missing trailing colon\n");
			stream_flush(fout);
			close_stream(fout);
			close_stream(fdin);
			return(e);
		} else {
			*s = '\0';
		}
	}

	if (*database == '\0') {
		/* we need to have a database, if we haven't gotten one,
		 * complain */
		stream_printf(fout, "!merovingian: please specify a database\n");
		stream_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		return(newErr("no database specified"));
	} else {
		if (strcmp(lang, "resolve") == 0) {
			/* ensure the pattern ends with '/\*' such that we force a
			 * remote entry, including those for local databases, this
			 * way we will get a redirect back to merovingian for such
			 * database if it is proxied and hence not remotely
			 * available */
			size_t len = strlen(database);
			if (len > 2 &&
					database[len - 2] != '/' &&
					database[len - 1] != '*')
			{
				char *n = alloca(sizeof(char) * len + 2 + 1);
				snprintf(n, len + 2 + 1, "%s/*", database);
				database = n;
			}
		}
		if ((e = forkMserver(database, &top, 0)) != NO_ERR) {
			if (top == NULL) {
				stream_printf(fout, "!merovingian: no such database '%s', please create it first\n", database);
			} else {
				stream_printf(fout, "!merovingian: internal error while starting mserver, please refer to the logs\n");
			}
			stream_flush(fout);
			close_stream(fout);
			close_stream(fdin);
			return(e);
		}
		stat = top;
	}

	/* collect possible redirects */
	for (stat = top; stat != NULL; stat = stat->next) {
		if (stat->conns == NULL || stat->conns->val == NULL) {
			Mfprintf(stdout, "dropping database without available "
					"connections: '%s'\n", stat->dbname);
		} else if (r == 24) {
			Mfprintf(stdout, "dropping database connection because of "
					"too many already: %s\n", stat->conns->val);
		} else {
			redirs[r++] = *stat;
		}
	}

	/* if we can't redirect, our mission ends here */
	if (r == 0) {
		e = newErr("there are no available connections for '%s'", database);
		stream_printf(fout, "!merovingian: %s\n", e);
		stream_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		SABAOTHfreeStatus(&top);
		return(e);
	}

	if (getpeername(sock, (struct sockaddr *)&saddr, &saddrlen) == -1) {
		Mfprintf(stderr, "couldn't get peername of client: %s\n",
				strerror(errno));
		host = "(unknown)";
	} else {
		/* avoid doing this, it requires some includes that probably
		 * give trouble on windowz
		host = inet_ntoa(saddr.sin_addr);
		 */
		struct hostent *hoste = 
			gethostbyaddr(&saddr.sin_addr.s_addr, 4, saddr.sin_family);
		if (hoste == NULL) {
			host = alloca(sizeof(char) * ((3 + 1 + 3 + 1 + 3 + 1 + 3) + 1));
			sprintf(host, "%u.%u.%u.%u:%u",
					(unsigned) ((ntohl(saddr.sin_addr.s_addr) >> 24) & 0xff),
					(unsigned) ((ntohl(saddr.sin_addr.s_addr) >> 16) & 0xff),
					(unsigned) ((ntohl(saddr.sin_addr.s_addr) >> 8) & 0xff),
					(unsigned) (ntohl(saddr.sin_addr.s_addr) & 0xff),
					(unsigned) (ntohs(saddr.sin_port)));
		} else {
			host = alloca(sizeof(char) * (strlen(hoste->h_name) + 1 + 5 + 1));
			sprintf(host, "%s:%u",
					hoste->h_name, (unsigned) (ntohs(saddr.sin_port)));
		}
	}

	/* need to send a response, either we are going to proxy, or we send
	 * a redirect, if we have multiple options, a redirect is our only
	 * option, but if the redir is a single remote we need to stick to
	 * our default, there is a special case when the client indicates it
	 * is only resolving a pattern, in which we always need to send
	 * redirects, even if it's one */
	mydoproxy = 0;
	if (r == 1 && strcmp(lang, "resolve") != 0) {
		if (redirs[0].dbname != redirs[0].path) {
			/* this is a real local database (not a remote) */
			ckv = getDefaultProps();
			readProps(ckv, redirs[0].path);
			kv = findConfKey(ckv, "forward");
		} else {
			ckv = NULL;
			kv = NULL;
		}
		if (kv == NULL || kv->val == NULL)
			kv = findConfKey(_mero_props, "forward");
		mydoproxy = strcmp(kv->val, "proxy") == 0;
		if (ckv != NULL) {
			freeConfFile(ckv);
			GDKfree(ckv);
		}
	}

	if (mydoproxy == 0) {
		fprintf(stdout, "redirecting client %s for database '%s' to",
				host, database);
		/* client is in control, send all redirects */
		while (--r >= 0) {
			fprintf(stdout, " %s%s",
					redirs[r].conns->val, redirs[r].dbname);
			stream_printf(fout, "^%s%s\n",
					redirs[r].conns->val, redirs[r].dbname);
		}
		/* flush redirect */
		fprintf(stdout, "\n");
		fflush(stdout);
		stream_flush(fout);
	} else {
		Mfprintf(stdout, "proxying client %s for database '%s' to %s%s\n",
				host, database, redirs[0].conns->val, redirs[0].dbname);
		/* merovingian is in control, only consider the first redirect */
		stream_printf(fout, "^mapi:merovingian://proxy?database=%s\n",
				redirs[0].dbname);
		/* flush redirect */
		stream_flush(fout);

		/* wait for input, or disconnect in a proxy runner */
		if ((e = startProxy(fdin, fout,
						redirs[0].conns->val, host)) != NO_ERR)
		{
			/* we need to let the client login in order not to violate
			 * the protocol */
			stream_printf(fout, "void:merovingian:8:plain:BIG");
			stream_flush(fout);
			stream_read_block(fdin, buf, 8095, 1); /* eat away client response */
			stream_printf(fout, "!merovingian: an internal error has occurred, refer to the logs for details, please try again later\n");
			stream_flush(fout);
			close_stream(fout);
			close_stream(fdin);
			Mfprintf(stdout, "starting a proxy failed: %s\n", e);
			SABAOTHfreeStatus(&top);
			return(e);
		};
	}

	SABAOTHfreeStatus(&top);
	return(NO_ERR);
}

static err
openConnectionTCP(int *ret, unsigned short port)
{
	struct sockaddr_in server;
	int sock = -1;
	socklen_t length = 0;
	int on = 1;
	int i = 0;
	struct hostent *hoste;
	char *host;
#ifdef CONTROL_BINDADDR
	char bindaddr[512];   /* eligable for configuration */
#endif

	sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock < 0)
		return(newErr("creation of stream socket failed: %s",
					strerror(errno)));

	setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (char *) &on, sizeof on);

	server.sin_family = AF_INET;
#ifdef CONTROL_BINDADDR
	gethostname(bindaddr, 512);
	hoste = gethostbyname(bindaddr);
	memcpy(&server.sin_addr.s_addr, *(hoste->h_addr_list),
			sizeof(server.sin_addr.s_addr));
#else
	server.sin_addr.s_addr = htonl(INADDR_ANY);
#endif
	for (i = 0; i < 8; i++)
		server.sin_zero[i] = 0;
	length = (socklen_t) sizeof(server);

	server.sin_port = htons((unsigned short) ((port) & 0xFFFF));
	if (bind(sock, (SOCKPTR) &server, length) < 0) {
		return(newErr("binding to stream socket port %hu failed: %s",
				port, strerror(errno)));
	}

	if (getsockname(sock, (SOCKPTR) &server, &length) < 0)
		return(newErr("failed getting socket name: %s",
				strerror(errno)));
	hoste = gethostbyaddr(&server.sin_addr.s_addr, 4, server.sin_family);
	if (hoste == NULL) {
		host = alloca(sizeof(char) * ((3 + 1 + 3 + 1 + 3 + 1 + 3) + 1));
		sprintf(host, "%u.%u.%u.%u",
				(unsigned) ((ntohl(server.sin_addr.s_addr) >> 24) & 0xff),
				(unsigned) ((ntohl(server.sin_addr.s_addr) >> 16) & 0xff),
				(unsigned) ((ntohl(server.sin_addr.s_addr) >> 8) & 0xff),
				(unsigned) (ntohl(server.sin_addr.s_addr) & 0xff));
	} else {
		host = hoste->h_name;
	}

	/* keep queue of 5 */
	listen(sock, 5);

	Mfprintf(stdout, "listening for TCP connections on %s:%hu\n", host, port);

	*ret = sock;
	return(NO_ERR);
}

static err
openConnectionUDP(int *ret, unsigned short port)
{
	struct addrinfo hints;
	struct addrinfo *result, *rp;
	int sock = -1;

	char sport[10];
	char host[512];

	if (port == 0) {
		Mfprintf(stdout, "neighbour discovery service disabled "
				"by configuration\n");
		*ret = -1;
		return(NO_ERR);
	}

	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_INET;      /* Allow IPv4 only (broadcasting) */
	hints.ai_socktype = SOCK_DGRAM; /* Datagram socket */
	hints.ai_flags = AI_PASSIVE;    /* For wildcard IP address */
	hints.ai_protocol = 0;          /* Any protocol */
	hints.ai_canonname = NULL;
	hints.ai_addr = NULL;
	hints.ai_next = NULL;

	snprintf(sport, 10, "%hu", port);
	sock = getaddrinfo(NULL, sport, &hints, &result);
	if (sock != 0)
		return(newErr("failed getting address info: %s", gai_strerror(sock)));

	for (rp = result; rp != NULL; rp = rp->ai_next) {
		sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (sock == -1)
			continue;

		if (bind(sock, rp->ai_addr, rp->ai_addrlen) == 0)
			break; /* working */

		close(sock);
	}

	if (rp == NULL)
		return(newErr("binding to datagram socket port %hu failed: "
					"no available address", port));

	/* retrieve information from the socket */
	getnameinfo(rp->ai_addr, rp->ai_addrlen,
			host, sizeof(host),
			sport, sizeof(sport),
			NI_NUMERICSERV | NI_DGRAM);

	freeaddrinfo(result);

	Mfprintf(_mero_discout, "listening for UDP messages on %s:%s\n", host, sport);

	*ret = sock;
	return(NO_ERR);
}

static err
openConnectionUNIX(int *ret, char *path)
{
	struct sockaddr_un server;
	int sock = -1;

	sock = socket(AF_UNIX, SOCK_STREAM, 0);
	if (sock < 0)
		return(newErr("creation of UNIX stream socket failed: %s",
					strerror(errno)));

	memset(&server, 0, sizeof(struct sockaddr_un));
	server.sun_family = AF_UNIX;
	strncpy(server.sun_path, path, sizeof(server.sun_path) - 1);

	if (bind(sock, (SOCKPTR) &server, sizeof(struct sockaddr_un)) < 0)
		return(newErr("binding to UNIX stream socket at %s failed: %s",
				path, strerror(errno)));

	/* keep queue of 5 */
	listen(sock, 5);

	Mfprintf(stdout, "handling commands over UNIX socket %s\n", path);

	*ret = sock;
	return(NO_ERR);
}

static str
acceptConnections(int sock)
{
	str msg;
	int retval;
	fd_set fds;
	int msgsock;
	err e;

	do {
		/* handle socket connections */
		FD_ZERO(&fds);
		FD_SET(sock, &fds);

		retval = select(sock + 1, &fds, NULL, NULL, NULL);
		if (retval == 0) {
			/* nothing interesting has happened */
			continue;
		}
		if (retval < 0) {
			if (_mero_keep_listening == 0)
				break;
			if (errno != EINTR) {
				msg = strerror(errno);
				goto error;
			}
			continue;
		}
		if (FD_ISSET(sock, &fds)) {
			if ((msgsock = accept(sock, (SOCKPTR) 0, (socklen_t *) 0)) < 0) {
				if (_mero_keep_listening == 0)
					break;
				if (errno != EINTR) {
					msg = strerror(errno);
					goto error;
				}
				continue;
			}
		} else
			continue;
		e = handleClient(msgsock);
		if (e != NO_ERR) {
			Mfprintf(stderr, "client error: %s\n", getErrMsg(e));
			freeErr(e);
		}
	} while (_mero_keep_listening);
	shutdown(sock, SHUT_RDWR);
	close(sock);
	return(NO_ERR);

error:
	_mero_keep_listening = 0;
	shutdown(sock, SHUT_RDWR);
	close(sock);
	return(newErr("accept connection: %s", msg));
}

static void
broadcast(char *msg)
{
	int len = strlen(msg) + 1;
	if (sendto(_mero_broadcastsock, msg, len, 0,
				(struct sockaddr *)&_mero_broadcastaddr,
				sizeof(_mero_broadcastaddr)) != len)
		Mfprintf(_mero_discerr, "error while sending broadcast "
				"message: %s\n", strerror(errno));
}

static void
controlRunner(void *d)
{
	int sock = *(int *)d;
	char buf[256];
	char buf2[256];
	char *p, *q;
	sabdb *stats;
	int pos = 0;
	int retval;
	fd_set fds;
	struct timeval tv;
	int msgsock;
	size_t len;
	err e;

	do {
		FD_ZERO(&fds);
		FD_SET(sock, &fds);

		/* Wait up to 5 seconds. */
		tv.tv_sec = 5;
		tv.tv_usec = 0;
		retval = select(sock + 1, &fds, NULL, NULL, &tv);
		if (retval == 0) {
			/* nothing interesting has happened */
			continue;
		}
		if (retval < 0) {
			if (_mero_keep_listening == 0)
				break;
			if (errno != EINTR) {
				e = newErr("control runner: error during select: %s",
						strerror(errno));
				goto error;
			}
			continue;
		}
		if (FD_ISSET(sock, &fds)) {
			if ((msgsock = accept(sock, (SOCKPTR) 0, (socklen_t *) 0)) < 0) {
				if (_mero_keep_listening == 0)
					break;
				if (errno != EINTR) {
					e = newErr("control runner: error during accept: %s",
							strerror(errno));
					goto error;
				}
				continue;
			}
		} else
			continue;

		while (_mero_keep_listening) {
			if (pos == 0) {
				if ((pos = recv(msgsock, buf, sizeof(buf), 0)) == 0) {
					/* EOF */
					break;
				} else if (pos == -1) {
					/* we got interrupted ... so what? */
					if (errno == EINTR) {
						pos = 0;
						continue;
					}
					/* hmmm error ... give up */
					Mfprintf(stderr, "error reading from control channel: %s\n",
							strerror(errno));
					break;
				} else {
					buf[pos] = '\0';
					pos = 0;
				}
			}
			q = buf + pos;
			p = strchr(q, '\n');
			if (p == NULL) {
				/* skip, must be garbage */
				Mfprintf(stderr, "skipping garbage on control channel: %s\n",
						buf);
				pos = 0;
				continue;
			}
			*p++ = '\0';
			if (*p == '\0') {
				pos = 0;
			} else {
				pos = p - buf;
			}

			/* format is simple: database<space>command */
			if ((p = strchr(q, ' ')) == NULL) {
				Mfprintf(stderr, "malformed control signal: %s\n", q);
			} else {
				*p++ = '\0';
				if (strcmp(p, "start") == 0) {
					err e;
					Mfprintf(stdout, "starting database '%s' "
							"due to control signal\n", q);
					if ((e = forkMserver(q, &stats, 1)) != NO_ERR) {
						Mfprintf(stderr, "failed to fork mserver: %s\n",
								getErrMsg(e));
						len = snprintf(buf2, sizeof(buf2),
								"starting '%s' failed: %s\n",
								q, getErrMsg(e));
						send(msgsock, buf2, len, 0);
						freeErr(e);
						stats = NULL;
					} else {
						len = snprintf(buf2, sizeof(buf2), "OK\n");
						send(msgsock, buf2, len, 0);
					}

					if (stats != NULL)
						SABAOTHfreeStatus(&stats);
				} else if (strcmp(p, "stop") == 0 ||
						strcmp(p, "kill") == 0)
				{
					dpair dp;
					/* we need to find the right dpair, that is we
					 * sort of assume the control signal is right */
					pthread_mutex_lock(&_mero_topdp_lock);
					dp = _mero_topdp->next; /* don't need the console/log */
					while (dp != NULL) {
						if (strcmp(dp->dbname, q) == 0) {
							if (strcmp(p, "stop") == 0) {
								Mfprintf(stdout, "stopping database '%s' "
										"due to control signal\n", q);
								terminateProcess(dp);
							} else {
								Mfprintf(stdout, "killing database '%s' "
										"due to control signal\n", q);
								kill(dp->pid, SIGKILL);
							}
							len = snprintf(buf2, sizeof(buf2), "OK\n");
							send(msgsock, buf2, len, 0);
							break;
						}
						dp = dp->next;
					}
					if (dp == NULL) {
						Mfprintf(stderr, "received control stop signal for "
								"database not under merovingian control: %s\n",
								q);
						len = snprintf(buf2, sizeof(buf2),
								"'%s' is not controlled by merovingian\n", q);
						send(msgsock, buf2, len, 0);
					}
					pthread_mutex_unlock(&_mero_topdp_lock);
				} else if (strncmp(p, "share=", strlen("share=")) == 0) {
					sabdb *stats;
					sabdb *topdb;
					err e;
					confkeyval *kv, *props = getDefaultProps();

					kv = findConfKey(_mero_props, "shared");
					if (strcmp(kv->val, "no") == 0) {
						/* can't do much */
						len = snprintf(buf2, sizeof(buf2),
								"discovery service is globally disabled, "
								"enable it first\n");
						send(msgsock, buf2, len, 0);
						Mfprintf(stderr, "share: cannot perform client share "
								"request: discovery service is globally "
								"disabled in %s\n", _mero_conffile);
						continue;
					}

					if ((e = SABAOTHgetStatus(&stats, NULL)) != MAL_SUCCEED) {
						len = snprintf(buf2, sizeof(buf2),
								"internal error, please review the logs\n");
						send(msgsock, buf2, len, 0);
						Mfprintf(stderr, "share: SABAOTHgetStatus: %s\n", e);
						freeErr(e);
						continue;
					}

					topdb = stats;
					while (stats != NULL) {
						if (strcmp(q, stats->dbname) == 0) {
							readProps(props, stats->path);
							kv = findConfKey(props, "shared");
							if (kv->val == NULL || strcmp(kv->val, "no") != 0) {
								/* we can leave without tag, will remove all */
								snprintf(buf2, sizeof(buf2),
										"LEAV %s mapi:monetdb://%s:%hu/",
										stats->dbname, _mero_hostname, _mero_port);
								broadcast(buf2);
							}
							p += strlen("share=");
							if (kv->val != NULL) {
								GDKfree(kv->val);
								kv->val = NULL;
							}
							/* the prophecy:
							 * <empty> inherit (bit useless)
							 * yes     share without tag
							 * no      don't share
							 * *       share with * as tag
							 */
							if (*p == '\0') {
								/* empty, inherit (e.g. remove local opt),
								 * but keep enabled, otherwise we never
								 * came here */
							} else if (strcmp(p, "yes") == 0) {
								/* make this share an empty tag */
								*p = '\0';
								kv->val = GDKstrdup("yes");
							} else if (strcmp(p, "no") == 0) {
								/* do not share (any more) */
								kv->val = GDKstrdup("no");
								writeProps(props, stats->path);
								break;
							} else {
								/* tag, include '/' for easy life
								 * afterwards */
								*--p = '/';
								kv->val = GDKstrdup(p + 1);
							}

							snprintf(buf2, sizeof(buf2),
									"ANNC %s%s mapi:monetdb://%s:%hu/ %d",
									stats->dbname, p, _mero_hostname,
									_mero_port, _mero_discoveryttl + 60);
							broadcast(buf2);
							writeProps(props, stats->path);
							break;
						}
						stats = stats->next;
					}
					if (stats == NULL) {
						Mfprintf(stderr, "received control share signal for "
								"database not under merovingian control: %s\n",
								q);
						len = snprintf(buf2, sizeof(buf2),
								"'%s' is not controlled by merovingian\n", q);
						send(msgsock, buf2, len, 0);
					}
					len = snprintf(buf2, sizeof(buf2), "OK\n");
					send(msgsock, buf2, len, 0);
					SABAOTHfreeStatus(&topdb);
					freeConfFile(props);
					GDKfree(props);
				} else if (strcmp(q, "anelosimus") == 0 &&
						strcmp(p, "eximius") == 0)
				{
					/* return a list of remote databases from our Aranita */
					remotedb rdb;

					pthread_mutex_lock(&_mero_remotedb_lock);

					rdb = _mero_remotedbs;
					while (rdb != NULL) {
						len = snprintf(buf2, sizeof(buf2), "%s%s%s\t%s\n",
								rdb->dbname,
								rdb->tag == NULL ? "" : "/",
								rdb->tag == NULL ? "" : rdb->tag,
								rdb->conn);
						send(msgsock, buf2, len, 0);
						rdb = rdb->next;
					}

					pthread_mutex_unlock(&_mero_remotedb_lock);

					/* because this command is multi line, you can't
					 * combine it, disconnect the client */
					break;
				} else {
					Mfprintf(stderr, "unknown control command: %s\n", p);
					len = snprintf(buf2, sizeof(buf2),
							"unknown command: %s\n", p);
					send(msgsock, buf2, len, 0);
				}
			}
		}
		close(msgsock);
		continue;

error:
		Mfprintf(stderr, "%s\n", e);
	} while (_mero_keep_listening);
	shutdown(sock, SHUT_RDWR);
	close(sock);
	Mfprintf(stdout, "control channel closed\n");
}

static void
discoveryRunner(void *d)
{
	int sock = *(int *)d;
	int s = -1;
	struct sockaddr_storage peer_addr;
	socklen_t peer_addr_len;
	fd_set fds;
	struct timeval tv;
	int c;
	/* avoid first announce, the HELO will cause an announce when it's
	 * received by ourself */
	time_t deadline = 1;
	time_t now = 0;
	int forceannc = 0;
	sabdb *orig;
	sabdb *stats;
	confkeyval *ckv;
	confkeyval *kv;
	err e;
	remotedb rdb;
	remotedb prv;
	char *val;

	ssize_t nread;
	char buf[512]; /* our packages should be pretty small */
	char host[128];
	char service[8];

	/* seed random number generation for random delay in HELO response */
	srand(time(NULL));

	/* start shouting around that we're here ;) request others to tell
	 * what databases they have */
	snprintf(buf, 512, "HELO %s", _mero_hostname);
	broadcast(buf);

	ckv = getDefaultProps();

	/* main loop */
	while (_mero_keep_listening == 1) {
		now = time(NULL);
		/* do a round of announcements, we're ahead of the ttl because
		 * when we announce, we add 60 seconds to avoid a "gap" */
		if (forceannc == 1 || deadline <= now) {
			/* set new deadline */
			deadline = now + _mero_discoveryttl;
			forceannc = 0;

			/* list all known databases */
			if ((e = SABAOTHgetStatus(&stats, NULL)) != MAL_SUCCEED) {
				Mfprintf(_mero_discerr, "SABAOTHgetStatus error: %s, "
						"discovery services disabled\n", e);
				GDKfree(e);
				return;
			}

			for (orig = stats; stats != NULL; stats = stats->next) {
				readProps(ckv, stats->path);
				kv = findConfKey(ckv, "shared");
				val = kv->val == NULL ? "" : kv->val;
				if (strcmp(val, "no") != 0) {
					/* craft ANNC message for this db */
					if (strcmp(val, "yes") == 0)
						val = "";
					snprintf(buf, 512, "ANNC %s%s%s mapi:monetdb://%s:%hu/ %d",
							stats->dbname, val[0] == '\0' ? "" : "/", val,
							_mero_hostname, _mero_port,
							_mero_discoveryttl + 60);
					broadcast(buf);
				}
				freeConfFile(ckv);
			}

			if (orig != NULL)
				SABAOTHfreeStatus(&orig);
		}

		/* do a round to see if we have to cleanup anything (expired
		 * ttl) */
		pthread_mutex_lock(&_mero_remotedb_lock);

		prv = NULL;
		rdb = _mero_remotedbs;
		while (rdb != NULL) {
			if (rdb->ttl <= now) {
				/* expired, let's remove */
				if (prv == NULL) {
					_mero_remotedbs = rdb->next;
				} else {
					prv->next = rdb->next;
				}
				Mfprintf(_mero_discout, "neighbour database %s%s "
						"has expired\n", rdb->conn, rdb->fullname);
				free(rdb->dbname);
				free(rdb->conn);
				free(rdb->tag);
				free(rdb->fullname);
				free(rdb);
				break;
			}
			prv = rdb;
			rdb = rdb->next;
		}

		pthread_mutex_unlock(&_mero_remotedb_lock);

		peer_addr_len = sizeof(struct sockaddr_storage);
		FD_ZERO(&fds);
		FD_SET(sock, &fds);
		/* Wait up to 5 seconds. */
		tv.tv_sec = 5;
		tv.tv_usec = 0;
		nread = select(sock + 1, &fds, NULL, NULL, &tv);
		if (nread == 0) {
			/* nothing interesting has happened */
			buf[0] = '\0';
			continue;
		}
		nread = recvfrom(sock, buf, 512, 0,
				(struct sockaddr *)&peer_addr, &peer_addr_len);
		if (nread == -1) {
			buf[0] = '\0';
			continue; /* ignore failed request */
		}

		s = getnameinfo((struct sockaddr *)&peer_addr,
				peer_addr_len, host, 128,
				service, 8, NI_NUMERICSERV);
		if (s != 0) {
			Mfprintf(_mero_discerr, "cannot retrieve name info: %s\n",
					gai_strerror(s));
			continue; /* skip this message */
		}

		/* ignore messages from broadcast interface */
		if (strcmp(host, "0.0.0.0") == 0)
			continue;

		if (strncmp(buf, "HELO ", 5) == 0) {
			/* HELLO message, respond with current databases */
			Mfprintf(_mero_discout, "new neighbour %s\n", host);
			/* sleep a random amount of time to avoid an avalanche of
			 * ANNC messages flooding the network */
			c = 1 + (int)(2500.0 * (rand() / (RAND_MAX + 1.0)));
			MT_sleep_ms(c);
			/* force an announcement round by dropping the deadline */
			forceannc = 1;
			continue;
		} else if (strncmp(buf, "LEAV ", 5) == 0) {
			/* LEAVE message, unregister database */
			char *sp = NULL;
			char *dbname;
			char *conn;
			char hadmatch = 0;

			strtok_r(buf, " ", &sp); /* discard the msg type */
			dbname = strtok_r(NULL, " ", &sp);
			conn = strtok_r(NULL, " ", &sp);

			if (dbname == NULL || conn == NULL)
				continue;

			/* look for the database, and verify that its "conn"
			 * (merovingian) is the same */

			/* technically, we could use Diffie-Hellman (without Debian
			 * modifications) to negotiate a shared secret key, such
			 * that only the original registrant can unregister a
			 * database, however... do we really care that much? */

			pthread_mutex_lock(&_mero_remotedb_lock);

			prv = NULL;
			rdb = _mero_remotedbs;
			while (rdb != NULL) {
				if (strcmp(dbname, rdb->dbname) == 0 &&
						strcmp(conn, rdb->conn) == 0)
				{
					/* found, let's remove */
					if (prv == NULL) {
						_mero_remotedbs = rdb->next;
					} else {
						prv->next = rdb->next;
					}
					Mfprintf(_mero_discout,
							"removed neighbour database %s%s\n",
							conn, rdb->fullname);
					free(rdb->dbname);
					free(rdb->conn);
					free(rdb->tag);
					free(rdb->fullname);
					free(rdb);
					hadmatch = 1;
					/* there may be more, keep looking */
				}
				prv = rdb;
				rdb = rdb->next;
			}
			if (hadmatch == 0)
				Mfprintf(_mero_discout,
						"received leave request for unknown database "
						"%s%s from %s\n", conn, dbname, host);

			pthread_mutex_unlock(&_mero_remotedb_lock);
		} else if (strncmp(buf, "ANNC ", 5) == 0) {
			/* ANNOUNCE message, register database */
			char *sp = NULL;
			char *dbname;
			char *tag = NULL;
			char *conn;
			char *ttl;

			strtok_r(buf, " ", &sp); /* discard the msg type */
			dbname = strtok_r(NULL, " ", &sp);
			conn = strtok_r(NULL, " ", &sp);
			ttl = strtok_r(NULL, " ", &sp);

			if (dbname == NULL || conn == NULL || ttl == NULL)
				continue;

			pthread_mutex_lock(&_mero_remotedb_lock);

			if (_mero_remotedbs == NULL) {
				rdb = _mero_remotedbs = malloc(sizeof(struct _remotedb));
			} else {
				prv = NULL;
				rdb = _mero_remotedbs;
				while (rdb != NULL) {
					if (strcmp(dbname, rdb->fullname) == 0 &&
							strcmp(conn, rdb->conn) == 0)
					{
						/* refresh ttl */
						rdb->ttl = time(NULL) + atoi(ttl);
						rdb = prv;
						break;
					}
					prv = rdb;
					rdb = rdb->next;
				}
				if (rdb == prv) {
					pthread_mutex_unlock(&_mero_remotedb_lock);
					continue;
				}
				rdb = prv->next = malloc(sizeof(struct _remotedb));
			}
			rdb->fullname = strdup(dbname);
			if ((tag = strchr(dbname, '/')) != NULL)
				*tag++ = '\0';
			rdb->dbname = strdup(dbname);
			rdb->tag = tag != NULL ? strdup(tag) : NULL;
			rdb->conn = strdup(conn);
			rdb->ttl = time(NULL) + atoi(ttl);
			rdb->next = NULL;

			pthread_mutex_unlock(&_mero_remotedb_lock);

			Mfprintf(_mero_discout, "new database "
					"%s%s (ttl=%ss)\n",
					conn, rdb->fullname, ttl);
		} else {
			Mfprintf(_mero_discout, "ignoring unknown message from "
					"%s:%s: '%s'\n", host, service, buf);
		}
	}

	/* now notify of our soon to be absence ;) */

	/* list all known databases */
	if ((e = SABAOTHgetStatus(&stats, NULL)) != MAL_SUCCEED) {
		Mfprintf(_mero_discerr, "SABAOTHgetStatus error: %s, "
				"discovery services disabled\n", e);
		GDKfree(e);
		return;
	}

	/* craft LEAV messages for each db */
	orig = stats;
	while (stats != NULL) {
		readProps(ckv, stats->path);
		kv = findConfKey(ckv, "shared");
		if (kv->val != NULL && strcmp(kv->val, "no") != 0) {
			snprintf(buf, 512, "LEAV %s mapi:monetdb://%s:%hu/",
					stats->dbname, _mero_hostname, _mero_port);
			broadcast(buf);
		}
		freeConfFile(ckv);
		stats = stats->next;
	}

	if (orig != NULL)
		SABAOTHfreeStatus(&orig);

	GDKfree(ckv);
}

/**
 * Handler for SIGINT, SIGTERM and SIGQUIT.  This starts a graceful
 * shutdown of merovingian.
 */
static void
handler(int sig)
{
	char *signame = NULL;
	switch (sig) {
		case SIGINT:
			signame = "SIGINT";
		break;
		case SIGTERM:
			signame = "SIGTERM";
		break;
		case SIGQUIT:
			signame = "SIGQUIT";
		break;
		default:
			assert(0);
	}
	Mfprintf(stdout, "caught %s, starting shutdown sequence\n", signame);
	_mero_keep_listening = 0;
}

/**
 * Handler for SIGHUP, causes logfiles to be reopened, if not attached
 * to a terminal.
 */
static void
huphandler(int sig)
{
	(void)sig;

	if (!isatty(_mero_topdp->out) || !isatty(_mero_topdp->err)) {
		int t;
		time_t now = time(NULL);
		struct tm *tmp = localtime(&now);
		char mytime[20];

		/* have to make sure the logger is not logging anything */
		pthread_mutex_lock(&_mero_topdp_lock);

		strftime(mytime, sizeof(mytime), "%Y-%m-%d %H:%M:%S", tmp);

		if (_mero_msglogfile != NULL) {
			/* reopen original file */
			t = open(_mero_msglogfile, O_WRONLY | O_APPEND | O_CREAT,
					S_IRUSR | S_IWUSR);
			if (t == -1) {
				Mfprintf(stderr, "forced to ignore SIGHUP: unable to open "
						"'%s': %s\n", _mero_msglogfile, strerror(errno));
			} else {
				Mfprintf(_mero_streamout, "%s END merovingian[" LLFMT "]: "
						"caught SIGHUP, closing logfile\n",
						mytime, (long long int)_mero_topdp->next->pid);
				fflush(_mero_streamout);
				fclose(_mero_streamout);
				_mero_topdp->out = t;
				_mero_streamout = fdopen(_mero_topdp->out, "a");
				Mfprintf(_mero_streamout, "%s BEG merovingian[" LLFMT "]: "
						"reopening logfile\n",
						mytime, (long long int)_mero_topdp->next->pid);
			}
		}
		if (_mero_errlogfile != NULL) {
			/* reopen original file */
			if (strcmp(_mero_msglogfile, _mero_errlogfile) == 0) {
				_mero_topdp->err = _mero_topdp->out;
			} else {
				t = open(_mero_errlogfile, O_WRONLY | O_APPEND | O_CREAT,
						S_IRUSR | S_IWUSR);
				if (t == -1) {
					Mfprintf(stderr, "forced to ignore SIGHUP: "
							"unable to open '%s': %s\n",
							_mero_errlogfile, strerror(errno));
				} else {
					Mfprintf(_mero_streamerr, "%s END merovingian[" LLFMT "]: "
							"caught SIGHUP, closing logfile\n",
							mytime, (long long int)_mero_topdp->next->pid);
					fclose(_mero_streamerr);
					_mero_topdp->err = t;
					_mero_streamerr = fdopen(_mero_topdp->err, "a");
					Mfprintf(_mero_streamerr, "%s BEG merovingian[" LLFMT "]: "
							"reopening logfile\n",
							mytime, (long long int)_mero_topdp->next->pid);
				}
			}
		}

		/* logger go ahead! */
		pthread_mutex_unlock(&_mero_topdp_lock);
	} else {
		Mfprintf(stdout, "caught SIGHUP, ignoring signal "
				"(logging to terminal)");
	}
}

/**
 * Handles SIGCHLD signals, that is, signals that a parent recieves
 * about its children.  This handler deals with terminated children, by
 * deregistering them from the internal administration (_mero_topdp)
 * with the necessary cleanup.
 */
static void
childhandler(int sig, siginfo_t *si, void *unused)
{
	dpair p, q;

	(void)sig;
	(void)unused;

	/* wait for the child to get properly terminated, hopefully filling
	 * in the siginfo struct on FreeBSD */
	wait(NULL);

	if (si->si_code != CLD_EXITED &&
			si->si_code != CLD_KILLED &&
			si->si_code != CLD_DUMPED)
	{
		/* ignore traps, stops and continues, we only want terminations
		 * of the client process */
		return;
	}

	pthread_mutex_lock(&_mero_topdp_lock);

	/* get the pid from the former child, and locate it in our list */
	q = _mero_topdp->next;
	p = q->next;
	while (p != NULL) {
		if (p->pid == si->si_pid) {
			/* log everything that's still in the pipes */
			logFD(p->out, "MSG", p->dbname, (long long int)p->pid, _mero_streamout);
			logFD(p->err, "ERR", p->dbname, (long long int)p->pid, _mero_streamerr);
			/* remove from the list */
			q->next = p->next;
			/* close the descriptors */
			close(p->out);
			close(p->err);
			if (si->si_code == CLD_EXITED) {
				Mfprintf(stdout, "database '%s' (%lld) has exited with "
						"exit status %d\n", p->dbname,
						(long long int)p->pid, si->si_status);
			} else if (si->si_code == CLD_KILLED) {
				Mfprintf(stdout, "database '%s' (%lld) was killed by signal "
						"%d\n", p->dbname,
						(long long int)p->pid, si->si_status);
			} else if (si->si_code == CLD_DUMPED) {
				Mfprintf(stdout, "database '%s' (%lld) has crashed "
						"(dumped core)\n", p->dbname,
						(long long int)p->pid);
			}
			if (p->dbname)
				GDKfree(p->dbname);
			GDKfree(p);
			pthread_mutex_unlock(&_mero_topdp_lock);
			return;
		}
		q = p;
		p = q->next;
	}

	pthread_mutex_unlock(&_mero_topdp_lock);

	Mfprintf(stdout, "received SIGCHLD from unknown child with pid %lld\n",
			(long long int)si->si_pid);
}

int
main(int argc, char *argv[])
{
	err e;
	int argp;
	str dbfarm, pidfilename;
	str p, prefix;
	FILE *cnf = NULL, *pidfile = NULL;
	char buf[1024];
	char lockfile[512];
	sabdb* stats = NULL;
	dpair d;
	int pfd[2];
	int retfd = -1;
	pthread_t tid;
	struct sigaction sa;
	int ret;
	int sock = -1;
	int usock = -1;
	int unsock = -1;
	char doproxy = 1;
	unsigned short discoveryport;
	struct stat sb;
	FILE *oerr = NULL;
	pthread_mutexattr_t mta;
	confkeyval ckv[] = {
		{"prefix",             GDKstrdup(MONETDB5_PREFIX), STR},
		{"gdk_dbfarm",         NULL,                       STR},
		{"gdk_nr_threads",     NULL,                       INT},
		{"sql_logdir",         NULL,                       STR},
		{"mero_msglog",        NULL,                       STR},
		{"mero_errlog",        NULL,                       STR},
		{"mero_port",          NULL,                       INT},
		{"mero_exittimeout",   NULL,                       INT},
		{"mero_pidfile",       NULL,                       STR},
		{"mero_doproxy",       NULL,                       BOOL},
		{"mero_discoveryttl",  NULL,                       INT},
		{"mero_discoveryport", NULL,                       INT},
		{ NULL,                NULL,                       INVALID}
	};
	confkeyval *kv;

	/* Fork into the background immediately.  By doing this our child
	 * can simply do everything it needs to do itself.  Via a pipe it
	 * will tell us if it is happy or not. */
	if (pipe(pfd) == -1) {
		Mfprintf(stderr, "unable to create pipe: %s\n", strerror(errno));
		return(1);
	}
#ifndef MERO_DONTFORK
	switch (fork()) {
		case -1:
			/* oops, forking went wrong! */
			Mfprintf(stderr, "unable to fork into background: %s\n",
					strerror(errno));
			return(1);
		case 0:
			/* detach client from controlling tty, we only write to the
			 * pipe to daddy */
			if (setsid() < 0)
				Mfprintf(stderr, "hmmm, can't detach from controlling tty, "
						"continuing anyway\n");
			retfd = open("/dev/null", O_RDONLY);
			dup2(retfd, 0);
			close(retfd);
			close(pfd[0]); /* close unused read end */
			retfd = pfd[1]; /* store the write end */
		break;
		default:
			/* the parent, we want it to die, after we know the child
			 * has a good time */
			close(pfd[1]); /* close unused write end */
			if (read(pfd[0], &buf, 1) != 1) {
				Mfprintf(stderr, "unable to retrieve startup status\n");
				return(1);
			}
			close(pfd[0]);
			return(buf[0]); /* whatever the child returned, we return */
	}
#endif

	/* Paranoia umask, but good, because why would people have to sniff
	 * our private parts? */
	umask(S_IRWXG | S_IRWXO);

	/* hunt for the config file, and read it, allow the caller to
	 * specify where to look using the MONETDB5CONF environment variable */
	p = getenv("MONETDB5CONF");
	if (p == NULL)
		p = MONETDB5_CONFFILE;
	cnf = fopen(p, "r");
	if (cnf == NULL) {
		Mfprintf(stderr, "cannot open config file %s\n", p);
		return(1);
	}
	/* store this conffile for later use in forkMserver */
	_mero_conffile = p;

#define MERO_EXIT(status) \
	buf[0] = status; \
	if (write(retfd, &buf, 1) != 1 || close(retfd) != 0) { \
		Mfprintf(stderr, "could not write to parent\n"); \
	} \
	if (status != 0) \
		return(status);

	readConfFile(ckv, cnf);
	fclose(cnf);

	kv = findConfKey(ckv, "prefix");
	prefix = kv->val; /* has default, must be set */
	kv = findConfKey(ckv, "gdk_dbfarm");
	dbfarm = replacePrefix(kv ? kv->val : NULL, prefix);
	kv = findConfKey(ckv, "mero_msglog");
	_mero_msglogfile = replacePrefix(kv ? kv->val : NULL, prefix);
	kv = findConfKey(ckv, "mero_errlog");
	_mero_errlogfile = replacePrefix(kv ? kv->val : NULL, prefix);
	kv = findConfKey(ckv, "mero_exittimeout");
	if (kv && kv->val != NULL)
		_mero_exit_timeout = atoi(kv->val);
	kv = findConfKey(ckv, "mero_pidfile");
	pidfilename = replacePrefix(kv ? kv->val : NULL, prefix);
	kv = findConfKey(ckv, "mero_port");
	if (kv && kv->val != NULL) {
		ret = atoi(kv->val);
		if (ret <= 0 || ret > 65535) {
			Mfprintf(stderr, "invalid port number: %s\n", kv->val);
			MERO_EXIT(1);
		}
		_mero_port = (unsigned short)ret;
	}
	kv = findConfKey(ckv, "mero_doproxy");
	if (kv && kv->val != NULL) {
		if (strcmp(kv->val, "yes") == 0 ||
				strcmp(kv->val, "true") == 0 ||
				strcmp(kv->val, "1") == 0)
		{
			doproxy = 1;
		} else {
			doproxy = 0;
		}
	}
	kv = findConfKey(ckv, "mero_discoveryttl");
	if (kv && kv->val != NULL)
		_mero_discoveryttl = atoi(kv->val);
	discoveryport = _mero_port;  /* defaults to same port as mero_port */
	kv = findConfKey(ckv, "mero_discoveryport");
	if (kv && kv->val != NULL) {
		ret = atoi(kv->val);
		if (ret < 0 || ret > 65535) {
			Mfprintf(stderr, "invalid port number: %s\n", kv->val);
			MERO_EXIT(1);
		}
		discoveryport = (unsigned short)ret;
	}

	/* where is the mserver5 binary we fork on demand? */
	snprintf(buf, 1023, "%s/bin/mserver5", prefix);
	_mero_mserver = alloca(sizeof(char) * (strlen(buf) + 1));
	memcpy(_mero_mserver, buf, strlen(buf) + 1);
	/* exit early if this is not going to work well */
	if (stat(_mero_mserver, &sb) == -1) {
		Mfprintf(stderr, "cannot stat %s executable: %s\n",
				_mero_mserver, strerror(errno));

		MERO_EXIT(1);
	}

	/* setup default properties */
	_mero_props = getDefaultProps();
	kv = findConfKey(_mero_props, "forward");
	kv->val = GDKstrdup(doproxy == 1 ? "proxy" : "redirect");
	kv = findConfKey(_mero_props, "shared");
	kv->val = GDKstrdup(discoveryport == 0 ? "no" : "yes");
	kv = findConfKey(ckv, "gdk_nr_threads");
	if (kv->val != NULL) {
		ret = atoi(kv->val);
		kv = findConfKey(_mero_props, "nthreads");
		snprintf(buf, sizeof(buf), "%d", ret);
		kv->val = GDKstrdup(buf);
	}

	/* we no longer need prefix */
	freeConfFile(ckv);
	prefix = NULL;

	/* we need a dbfarm */
	if (dbfarm == NULL) {
		Mfprintf(stderr, "cannot find dbfarm via config file\n");
		MERO_EXIT(1);
	} else {
		/* check if dbfarm actually exists */
		struct stat statbuf;
		if (stat(dbfarm, &statbuf) == -1) {
			/* try to create the dbfarm */
			char *p = dbfarm;
			while ((p = strchr(p + 1, '/')) != NULL) {
				*p = '\0';
				if (stat(dbfarm, &statbuf) == -1 && mkdir(dbfarm, 0755)) {
					Mfprintf(stderr, "unable to create directory '%s': %s\n",
							dbfarm, strerror(errno));
					MERO_EXIT(1);
				}
				*p = '/';
			}
			if (mkdir(dbfarm, 0755)) {
				Mfprintf(stderr, "unable to create directory '%s': %s\n",
						dbfarm, strerror(errno));
				MERO_EXIT(1);
			}
		}
	}

	/* chdir to dbfarm so we are at least in a known to exist location */
	if (chdir(dbfarm) < 0) {
		Mfprintf(stderr, "could not move to dbfarm '%s': %s\n",
				dbfarm, strerror(errno));
		MERO_EXIT(1);
	}

	/* we need a pidfile */
	if (pidfilename == NULL) {
		Mfprintf(stderr, "cannot find pidfilename via config file\n");
		MERO_EXIT(1);
	}

	snprintf(lockfile, 512, "%s/.merovingian_lock", dbfarm);
	/* lock such that we are alone on this world */
	if ((ret = MT_lockf(lockfile, F_TLOCK, 4, 1)) == -1) {
		/* locking failed */
		Mfprintf(stderr, "another merovingian is already running\n");
		MERO_EXIT(1);
	} else if (ret == -2) {
		/* directory or something doesn't exist */
		Mfprintf(stderr, "unable to create .merovingian_lock file in %s: %s\n",
				dbfarm, strerror(errno));
		MERO_EXIT(1);
	}

	_mero_topdp = alloca(sizeof(struct _dpair));
	_mero_topdp->pid = 0;
	_mero_topdp->dbname = NULL;

	/* where should our msg output go to? */
	if (_mero_msglogfile == NULL) {
		/* stdout, save it */
		argp = dup(1);
		_mero_topdp->out = argp;
	} else {
		/* write to the given file */
		_mero_topdp->out = open(_mero_msglogfile, O_WRONLY | O_APPEND | O_CREAT,
				S_IRUSR | S_IWUSR);
		if (_mero_topdp->out == -1) {
			Mfprintf(stderr, "unable to open '%s': %s\n",
					_mero_msglogfile, strerror(errno));
			MERO_EXIT(1);
		}
	}

	/* where should our err output go to? */
	if (_mero_errlogfile == NULL) {
		/* stderr, save it */
		argp = dup(2);
		_mero_topdp->err = argp;
	} else {
		/* write to the given file */
		if (strcmp(_mero_msglogfile, _mero_errlogfile) == 0) {
			_mero_topdp->err = _mero_topdp->out;
		} else {
			_mero_topdp->err = open(_mero_errlogfile, O_WRONLY | O_APPEND | O_CREAT,
					S_IRUSR | S_IWUSR);
			if (_mero_topdp->err == -1) {
				Mfprintf(stderr, "unable to open '%s': %s\n",
						_mero_errlogfile, strerror(errno));
				MERO_EXIT(1);
			}
		}
	}

	_mero_streamout = fdopen(_mero_topdp->out, "a");
	if (_mero_topdp->out == _mero_topdp->err) {
		_mero_streamerr = _mero_streamout;
	} else {
		_mero_streamerr = fdopen(_mero_topdp->err, "a");
	}

	d = _mero_topdp->next = alloca(sizeof(struct _dpair));

	/* make sure we will be able to write our pid */
	if ((pidfile = fopen(pidfilename, "w")) == NULL) {
		Mfprintf(stderr, "unable to open '%s' for writing: %s\n",
				pidfilename, strerror(errno));
		MERO_EXIT(1);
	}

	/* redirect stdout */
	if (pipe(pfd) == -1) {
		Mfprintf(stderr, "unable to create pipe: %s\n",
				strerror(errno));
		MERO_EXIT(1);
	}
	d->out = pfd[0];
	dup2(pfd[1], 1);
	close(pfd[1]);

	/* redirect stderr */
	if (pipe(pfd) == -1) {
		Mfprintf(stderr, "unable to create pipe: %s\n",
				strerror(errno));
		MERO_EXIT(1);
	}
	/* before it is too late, save original stderr */
	oerr = fdopen(dup(2), "w");
	d->err = pfd[0];
	dup2(pfd[1], 2);
	close(pfd[1]);

	d->pid = getpid();
	d->dbname = "merovingian";

	/* separate entry for the neighbour discovery service */
	d = d->next = alloca(sizeof(struct _dpair));
	if (pipe(pfd) == -1) {
		Mfprintf(stderr, "unable to create pipe: %s\n",
				strerror(errno));
		MERO_EXIT(1);
	}
	d->out = pfd[0];
	_mero_discout = fdopen(pfd[1], "a");
	if (pipe(pfd) == -1) {
		Mfprintf(stderr, "unable to create pipe: %s\n",
				strerror(errno));
		MERO_EXIT(1);
	}
	d->err = pfd[0];
	_mero_discerr = fdopen(pfd[1], "a");
	d->pid = getpid();
	d->dbname = "discovery";
	d->next = NULL;

	/* figure out our hostname */
	gethostname(_mero_hostname, 128);

	/* write out the pid */
	Mfprintf(pidfile, "%d\n", (int)d->pid);
	fclose(pidfile);

	/* allow a thread to relock this mutex */
	pthread_mutexattr_init(&mta);
	pthread_mutexattr_settype(&mta, PTHREAD_MUTEX_RECURSIVE);
	pthread_mutex_init(&_mero_topdp_lock, &mta);

	if (pthread_create(&tid, NULL, (void *(*)(void *))logListener, (void *)NULL) < 0) {
		Mfprintf(oerr, "%s: unable to create logthread, exiting\n", argv[0]);
		MERO_EXIT(1);
	}

	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sa.sa_handler = handler;
	if (
			sigaction(SIGINT, &sa, NULL) == -1 ||
			sigaction(SIGQUIT, &sa, NULL) == -1 ||
			sigaction(SIGTERM, &sa, NULL) == -1)
	{
		Mfprintf(oerr, "%s: unable to create signal handlers\n", argv[0]);
		MERO_EXIT(1);
	}

	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sa.sa_handler = huphandler;
	if (sigaction(SIGHUP, &sa, NULL) == -1) {
		Mfprintf(oerr, "%s: unable to create signal handlers\n", argv[0]);
		MERO_EXIT(1);
	}

	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sa.sa_handler = SIG_IGN;
	if (sigaction(SIGPIPE, &sa, NULL) == -1) {
		Mfprintf(oerr, "%s: unable to create signal handlers\n", argv[0]);
		MERO_EXIT(1);
	}

	sa.sa_flags = SA_SIGINFO;
	sigemptyset(&sa.sa_mask);
	sa.sa_sigaction = childhandler;
	if (sigaction(SIGCHLD, &sa, NULL) == -1) {
		Mfprintf(oerr, "%s: unable to create signal handlers\n", argv[0]);
		MERO_EXIT(1);
	}

	Mfprintf(stdout, "starting Merovingian %s for dbfarm %s\n",
			MERO_VERSION, dbfarm);

	SABAOTHinit(dbfarm, NULL);

	/* set up control channel path */
	snprintf(buf, 1024, "%s/.merovingian_control", dbfarm);
	unlink(buf);
	GDKfree(dbfarm);

	/* open up connections */
	if (
			(e = openConnectionTCP(&sock, _mero_port)) == NO_ERR &&
			(e = openConnectionUDP(&usock, discoveryport)) == NO_ERR &&
			(e = openConnectionUNIX(&unsock, buf)) == NO_ERR)
	{
		pthread_t ctid = 0;
		pthread_t dtid = 0;

		_mero_broadcastsock = socket(AF_INET, SOCK_DGRAM, 0);
		ret = 1;
		if ((setsockopt(_mero_broadcastsock,
						SOL_SOCKET, SO_BROADCAST, &ret, sizeof(ret))) == -1)
		{
			Mfprintf(stderr, "cannot create broadcast package, "
					"discovery services disabled\n");
			close(usock);
			usock = -1;
		}

		_mero_broadcastaddr.sin_family = AF_INET;
		_mero_broadcastaddr.sin_addr.s_addr = htonl(INADDR_BROADCAST);
		/* the target port is our configured port, not elegant, but how
		 * else can we do it? can't broadcast to all ports or something */
		_mero_broadcastaddr.sin_port = htons(discoveryport);

		/* From this point merovingian considers itself to be in position to
		 * start running, so flag the parent we will have fun. */
		MERO_EXIT(0);

		for (argp = 1; argp < argc; argp++) {
			e = forkMserver(argv[argp], &stats, 0);
			if (e != NO_ERR) {
				Mfprintf(stderr, "failed to fork mserver: %s\n", getErrMsg(e));
				freeErr(e);
				stats = NULL;
			}
			if (stats != NULL)
				SABAOTHfreeStatus(&stats);
		}

		/* handle control commands */
		if (pthread_create(&ctid, NULL, (void *(*)(void *))controlRunner,
					(void *)&unsock) < 0)
		{
			Mfprintf(stderr, "unable to create control command thread\n");
			ctid = 0;
		}

		/* start neighbour discovery and notification thread */ 
		if (usock >= 0 && pthread_create(&dtid, NULL,
					(void *(*)(void *))discoveryRunner, (void *)&usock) < 0)
		{
			Mfprintf(stderr, "unable to start neighbour discovery thread\n");
			dtid = 0;
		}

		/* handle external connections main loop */
		e = acceptConnections(sock);

		/* wait for the control runner and discovery thread to have
		 * finished announcing they're going down */
		close(unsock);
		if (ctid != 0)
			pthread_join(ctid, NULL);
		if (usock >= 0) {
			close(usock);
			if (dtid != 0)
				pthread_join(dtid, NULL);
		}
	}

	/* control channel is already closed at this point */
	unlink(buf);

	if (e != NO_ERR) {
		/* console */
		Mfprintf(oerr, "%s: %s\n", argv[0], e);
		MERO_EXIT(1);
		/* logfile */
		Mfprintf(stderr, "%s\n", e);
	}

	/* we don't need merovingian itself */
	d = d->next;

	/* stop started mservers */
	if (_mero_exit_timeout > 0) {
		dpair t;
		threadlist tl = NULL, tlw = tl;

		pthread_mutex_lock(&_mero_topdp_lock);
		t = d;
		while (t != NULL) {
			if (tl == NULL) {
				tl = tlw = malloc(sizeof(struct _threadlist));
			} else {
				tlw = tlw->next = malloc(sizeof(struct _threadlist));
			}

			tlw->next = NULL;
			if (pthread_create(&(tlw->tid), NULL,
						(void *(*)(void *))terminateProcess, (void *)t) < 0)
			{
				Mfprintf(stderr, "%s: unable to create thread to terminate "
						"database '%s'\n", argv[0], d->dbname);
				tlw->tid = 0;
			}

			t = t->next;
		}
		pthread_mutex_unlock(&_mero_topdp_lock);

		/* wait for all processes to be terminated */
		tlw = tl;
		while (tlw != NULL) {
			if (tlw->tid != 0 && (argp = pthread_join(tlw->tid, NULL)) != 0) {
				Mfprintf(stderr, "failed to wait for termination thread: "
						"%s\n", strerror(argp));
			}
			tl = tlw->next;
			free(tlw);
			tlw = tl;
		}
	}

	/* need to do this here, since the logging thread is shut down as
	 * next thing */
	Mfprintf(stdout, "Merovingian %s stopped\n", MERO_VERSION);

	_mero_keep_logging = 0;
	if ((argp = pthread_join(tid, NULL)) != 0) {
		Mfprintf(oerr, "failed to wait for logging thread: %s\n",
				strerror(argp));
	}

	close(_mero_topdp->out);
	if (_mero_topdp->out != _mero_topdp->err)
		close(_mero_topdp->err);

	GDKfree(_mero_msglogfile);
	GDKfree(_mero_errlogfile);

	/* remove files that suggest our existence */
	unlink(lockfile);
	unlink(pidfilename);
	GDKfree(pidfilename);

	/* mostly for valgrind... */
	freeConfFile(_mero_props);
	GDKfree(_mero_props);

	/* the child's return code at this point doesn't matter, as noone
	 * will see it */
	return(0);
}

/* vim:set ts=4 sw=4 noexpandtab: */
