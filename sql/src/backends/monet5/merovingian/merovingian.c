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
 * Copyright August 2008-2010 MonetDB B.V.
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

#define MERO_VERSION   "1.3"
#define MERO_PORT      50000

#include "sql_config.h"
#include "mal_sabaoth.h"
#include "utils.h"
#include "properties.h"
#include "glob.h"
#include "database.h"
#include "utils.h"
#include "control.h"
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
/* the port merovingian listens for TCP control commands */
static unsigned short _mero_controlport = 0;
/* stream to the stdout for the control runner */
static FILE *_mero_ctlout = NULL;
/* stream to the stderr for the control runner */
static FILE *_mero_ctlerr = NULL;
/* broadcast socket for announcements */
static int _mero_broadcastsock;
/* broadcast address/port */
static struct sockaddr_in _mero_broadcastaddr;
/* hostname of this machine */
static char _mero_hostname[128];
/* control channel passphrase */
static char _mero_controlpass[128];
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

#include "merovingian_forkmserver.c"
#include "merovingian_proxy.c"
#include "merovingian_client.c"
#include "merovingian_connections.c"
#include "merovingian_controlrunner.c"
#include "merovingian_discoveryrunner.c"
#include "merovingian_handlers.c"

int
main(int argc, char *argv[])
{
	err e;
	int argp;
	str dbfarm, pidfilename;
	str p, prefix;
	FILE *cnf = NULL, *pidfile = NULL;
	char buf[1024];
	char bufu[1024];
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
	int csock = -1;
	int socku = -1;
	char doproxy = 1;
	unsigned short discoveryport;
	struct stat sb;
	FILE *oerr = NULL;
	pthread_mutexattr_t mta;
	confkeyval ckv[] = {
		{"prefix",             GDKstrdup(MONETDB5_PREFIX), STR},
		{"gdk_dbfarm",         NULL,                       STR},
		{"gdk_nr_threads",     NULL,                       INT},
		{"sql_optimizer",      NULL,                       STR},
		{"mero_msglog",        GDKstrdup(MERO_LOG),        STR},
		{"mero_errlog",        GDKstrdup(MERO_LOG),        STR},
		{"mero_port",          NULL,                       INT},
		{"mero_exittimeout",   NULL,                       INT},
		{"mero_pidfile",       NULL,                       STR},
		{"mero_doproxy",       NULL,                       BOOL},
		{"mero_discoveryttl",  NULL,                       INT},
		{"mero_discoveryport", NULL,                       INT},
		{"mero_controlport",   NULL,                       INT},
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
	_mero_msglogfile = replacePrefix(kv->val, prefix);
	if (strcmp(kv->val, "") == 0) { /* has default, must be set */
		GDKfree(kv->val);
		kv->val = NULL;
	}
	kv = findConfKey(ckv, "mero_errlog");
	_mero_errlogfile = replacePrefix(kv->val, prefix);
	if (strcmp(kv->val, "") == 0) { /* has default, must be set */
		GDKfree(kv->val);
		kv->val = NULL;
	}
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
	kv = findConfKey(ckv, "mero_controlport");
	if (kv && kv->val != NULL) {
		ret = atoi(kv->val);
		if (ret < 0 || ret > 65535) {
			Mfprintf(stderr, "invalid port number: %s\n", kv->val);
			MERO_EXIT(1);
		}
		_mero_controlport = (unsigned short)ret;
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
	kv = findConfKey(_mero_props, "master");
	kv->val = GDKstrdup("no");
	kv = findConfKey(_mero_props, "slave");
	kv->val = NULL; /* MURI */
	kv = findConfKey(ckv, "sql_optimizer");
	p = kv->val;
	if (p != NULL) {
		kv = findConfKey(_mero_props, "optpipe");
		kv->val = GDKstrdup(p);
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

	/* seed the randomiser for when we create a database, send responses
	 * to HELO, etc */
	srand(time(NULL));

	/* see if we have the passphrase if we do remote control stuff */
	if (_mero_controlport != 0) {
		struct stat statbuf;
		FILE *secretf;
		size_t len;

		if (stat(".merovingian_pass", &statbuf) == -1) {
			if ((e = generatePassphraseFile(".merovingian_pass")) != NULL) {
				Mfprintf(stderr, "cannot open .merovingian_pass for "
						"writing: %s\n", e);
				free(e);
				MERO_EXIT(1);
			}
		}

		if ((secretf = fopen(".merovingian_pass", "r")) == NULL) {
			Mfprintf(stderr, "unable to open .merovingian_pass: %s\n",
					strerror(errno));
			MERO_EXIT(1);
		}

		len = fread(_mero_controlpass, 1,
				sizeof(_mero_controlpass) - 1, secretf);
		fclose(secretf);
		_mero_controlpass[len] = '\0';
		len = strlen(_mero_controlpass); /* secret can contain null-bytes */
		/* strip trailing newlines */
		for (; len > 0 && _mero_controlpass[len - 1] == '\n'; len--)
			_mero_controlpass[len - 1] = '\0';
		if (len == 0) {
			Mfprintf(stderr, "control passphrase has zero-length\n");
			MERO_EXIT(1);
		}
	}

	/* we need a pidfile */
	if (pidfilename == NULL) {
		Mfprintf(stderr, "cannot find pidfilename via config file\n");
		MERO_EXIT(1);
	}

	/* lock such that we are alone on this world */
	if ((ret = MT_lockf(".merovingian_lock", F_TLOCK, 4, 1)) == -1) {
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

	/* separate entry for the control runner */
	d = d->next = alloca(sizeof(struct _dpair));
	if (pipe(pfd) == -1) {
		Mfprintf(stderr, "unable to create pipe: %s\n",
				strerror(errno));
		MERO_EXIT(1);
	}
	d->out = pfd[0];
	_mero_ctlout = fdopen(pfd[1], "a");
	if (pipe(pfd) == -1) {
		Mfprintf(stderr, "unable to create pipe: %s\n",
				strerror(errno));
		MERO_EXIT(1);
	}
	d->err = pfd[0];
	_mero_ctlerr = fdopen(pfd[1], "a");
	d->pid = getpid();
	d->dbname = "control";
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

	Mfprintf(stdout, "Merovingian %s starting\n", MERO_VERSION);
	Mfprintf(stdout, "monitoring dbfarm %s\n", dbfarm);

	SABAOTHinit(dbfarm, NULL);

	/* set up control channel path */
	snprintf(buf, 1024, "%s/.merovingian_control", dbfarm);
	unlink(buf);
	snprintf(bufu, 1024, "%s/mapi_socket", dbfarm);
	unlink(bufu);
	GDKfree(dbfarm);

	/* open up connections */
	if (
			(e = openConnectionTCP(&sock, _mero_port, stdout)) == NO_ERR &&
			(e = openConnectionUNIX(&socku, bufu, stdout)) == NO_ERR &&
			(e = openConnectionUDP(&usock, discoveryport)) == NO_ERR &&
			(e = openConnectionUNIX(&unsock, buf, _mero_ctlout)) == NO_ERR &&
			(_mero_controlport == 0 || (e = openConnectionTCP(&csock, _mero_controlport, _mero_ctlout)) == NO_ERR)
	   )
	{
		pthread_t ctid = 0;
		pthread_t dtid = 0;
		int csocks[2];

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
		csocks[0] = unsock;
		csocks[1] = csock;
		if (pthread_create(&ctid, NULL, (void *(*)(void *))controlRunner,
					(void *)&csocks) < 0)
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
		e = acceptConnections(sock, socku);

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
	unlink(bufu);

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
	unlink(".merovingian_lock");
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
