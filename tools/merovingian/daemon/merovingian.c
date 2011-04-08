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
 * Copyright August 2008-2011 MonetDB B.V.
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
 * its name, one can also refer to Merovingian as Mero or its name for
 * dummies: monetdbd.  In any case, people having difficulties here
 * should watch The Matrix once more.
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
 * Merovingian can refuse to start a database if it has crashed a
 * number of times over a recent period.  Note that to date, no such
 * thing has been implemented as the need for it has not arisen yet.
 *
 * By default, merovingian will monitor and control the dbfarm in the
 * build-time configured prefix under var/monetdb5/dbfarm.  However,
 * when a path is given as first argument, merovingian will attempt to
 * monitor and control the directory the path points to.  This allows
 * users to create their own dbfarm, but also expert users to run
 * multiple merovingians on the same system easily, since the
 * (persistent) configuration is read from the dbfarm directory.
 */

#include "monetdb_config.h"
#include <msabaoth.h>
#include <mutils.h>
#include <utils/utils.h>
#include <utils/properties.h>
#include <utils/glob.h>
#include <utils/database.h>
#include <utils/control.h>

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
#include <errno.h>
#include <signal.h> /* handle Ctrl-C, etc. */
#include <pthread.h>
#include <time.h>

#include "merovingian.h"
#include "client.h"
#include "connections.h"
#include "controlrunner.h"
#include "discoveryrunner.h"
#include "handlers.h"
#include "argvcmds.h"


/* private structs */

typedef struct _threadlist {
	pthread_t tid;    /* thread id */
	struct _threadlist* next;
}* threadlist;


/* globals */

/* full path to the mserver5 binary */
char *_mero_mserver = NULL;
/* list of databases that we have started */
dpair _mero_topdp = NULL;
/* lock to _mero_topdp, initialised as recursive lateron */
pthread_mutex_t _mero_topdp_lock;
/* for the logger, when set to 0, the logger terminates */
int _mero_keep_logging = 1;
/* for accepting connections, when set to 0, listening socket terminates */
char _mero_keep_listening = 1;
/* stream to where to write the log */
FILE *_mero_logfile = NULL;
/* stream to the stdout for the neighbour discovery service */
FILE *_mero_discout = NULL;
/* stream to the stderr for the neighbour discovery service */
FILE *_mero_discerr = NULL;
/* stream to the stdout for the control runner */
FILE *_mero_ctlout = NULL;
/* stream to the stderr for the control runner */
FILE *_mero_ctlerr = NULL;
/* broadcast socket for announcements */
int _mero_broadcastsock = -1;
/* broadcast address/port */
struct sockaddr_in _mero_broadcastaddr;
/* hostname of this machine */
char _mero_hostname[128];
/* default options read from config file */
confkeyval *_mero_db_props = NULL;
/* merovingian's own properties */
confkeyval *_mero_props = NULL;


/* funcs */

inline void
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
	struct timeval tv;
	fd_set readfds;
	int nfds;

	(void)x;

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
						(long long int)w->pid, _mero_logfile);
			if (w->err != w->out && FD_ISSET(w->err, &readfds) != 0)
				logFD(w->err, "ERR", w->dbname,
						(long long int)w->pid, _mero_logfile);
			w = w->next;
		}

		pthread_mutex_unlock(&_mero_topdp_lock);

		fflush(_mero_logfile);
	} while (_mero_keep_logging != 0);
}

/**
 * The terminateProcess function tries to let the given mserver process
 * shut down gracefully within a given time-out.  If that fails, it
 * sends the deadly SIGKILL signal to the mserver process and returns.
 */
void
terminateProcess(void *p)
{
	dpair d = (dpair)p;
	sabdb *stats;
	char *er;
	int i;
	confkeyval *kv;
	/* make local copies since d will disappear when killed */
	pid_t pid = d->pid;
	char *dbname = alloca(sizeof(char) * (strlen(d->dbname) + 1));
	memcpy(dbname, d->dbname, strlen(d->dbname) + 1);

	er = msab_getStatus(&stats, dbname);
	if (er != NULL) {
		Mfprintf(stderr, "cannot terminate process " LLFMT ": %s\n",
				(long long int)pid, er);
		free(er);
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
			msab_freeStatus(&stats);
			return;
		case SABdbInactive:
			Mfprintf(stdout, "database '%s' appears to have shut down already\n",
					dbname);
			fflush(stdout);
			msab_freeStatus(&stats);
			return;
		default:
			Mfprintf(stderr, "unknown state: %d", (int)stats->state);
			msab_freeStatus(&stats);
			return;
	}

	/* ok, once we get here, we'll be shutting down the server */
	Mfprintf(stdout, "sending process " LLFMT " (database '%s') the "
			"TERM signal\n", (long long int)pid, dbname);
	kill(pid, SIGTERM);
	kv = findConfKey(_mero_props, "exittimeout");
	for (i = 0; i < atoi(kv->val) * 2; i++) {
		if (stats != NULL)
			msab_freeStatus(&stats);
		sleep_ms(500);
		er = msab_getStatus(&stats, dbname);
		if (er != NULL) {
			Mfprintf(stderr, "unexpected problem: %s\n", er);
			free(er);
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
					msab_freeStatus(&stats);
					return;
				case SABdbInactive:
					Mfprintf(stdout, "database '%s' has shut down\n", dbname);
					fflush(stdout);
					msab_freeStatus(&stats);
					return;
				default:
					Mfprintf(stderr, "unknown state: %d", (int)stats->state);
				break;
			}
		}
	}
	Mfprintf(stderr, "timeout of %s seconds expired, sending process " LLFMT
			" (database '%s') the KILL signal\n",
			kv->val, (long long int)pid, dbname);
	kill(pid, SIGKILL);
	return;
}

/**
 * Creates a new error, allocated with malloc.  The error should be
 * freed using freeErr().
 */
char *
newErr(char *fmt, ...)
{
	va_list ap;
	char message[4096];
	char *ret;
	int len;

	va_start(ap, fmt);

	len = vsnprintf(message, 4095, fmt, ap);
	message[len] = '\0';

	va_end(ap);

	ret = strdup(message);
	return(ret);
}

/**
 * Explicitly pulled out functionality that is just implemented to
 * obtain an automatic upgrade path.
 *
 * Starting from the Apr2011 release, the .merovingian_pass file is no
 * longer used.  Instead, the phrase is inside the
 * .merovingian_properties file that is used since that release also.
 * If the file exists, read its contents, and set it as value for the
 * key "passphrase" in the given confkeyval.  Afterwards, delete the old
 * .merovingian_pass file.
 *
 * Returns 0 if a .merovingian_pass file existed, contained a valid key,
 * was set in ckv, and the .merovingian_pass file was removed.
 */
static char
autoUpgradePassphraseMar2011Apr2011(confkeyval *ckv)
{
	struct stat statbuf;
	FILE *secretf;
	size_t len;
	char buf[128];

	if (stat(".merovingian_pass", &statbuf) == -1)
		return(1);

	if ((secretf = fopen(".merovingian_pass", "r")) == NULL)
		return(1);

	len = fread(buf, 1, sizeof(buf) - 1, secretf);
	fclose(secretf);
	buf[len] = '\0';
	len = strlen(buf); /* secret can contain null-bytes */
	/* strip trailing newlines */
	for (; len > 0 && buf[len - 1] == '\n'; len--)
		buf[len - 1] = '\0';
	if (len == 0)
		return(1);

	ckv = findConfKey(ckv, "passphrase");
	setConfVal(ckv, buf);

	if (!unlink(".merovingian_pass"))
		return(1);

	return(0);
}

int
main(int argc, char *argv[])
{
	err e;
	int argp;
	char *dbfarm = LOCALSTATEDIR "/monetdb5/dbfarm";
	char *pidfilename;
	char *p;
	FILE *pidfile = NULL;
	char control_usock[1024];
	char mapi_usock[1024];
	dpair d = NULL;
	int pfd[2];
	pthread_t tid = 0;
	struct sigaction sa;
	int ret;
	int sock = -1;
	int usock = -1;
	int unsock = -1;
	int csock = -1;
	int socku = -1;
	unsigned short port = 0;
	unsigned short discoveryport = 0;
	unsigned short controlport = 0;
	struct stat sb;
	FILE *oerr = NULL;
	pthread_mutexattr_t mta;
	int thret;
	confkeyval ckv[] = {
		{"logfile",       strdup("merovingian.log"), 0,                STR},
		{"pidfile",       strdup("merovingian.pid"), 0,                STR},

		{"sockdir",       strdup("/tmp"),          0,                  STR},
		{"port",          strdup(MERO_PORT),       atoi(MERO_PORT),    INT},
		{"controlport",   strdup(CONTROL_PORT),    atoi(CONTROL_PORT), INT},
		{"discoveryport", strdup(MERO_PORT),       atoi(MERO_PORT),    INT},

		{"exittimeout",   strdup("60"),            60,                 INT},
		{"forward",       strdup("proxy"),         0,                  OTHER},
		{"discoveryttl",  strdup("600"),           600,                INT},

		{"passphrase",    NULL,                    0,                  STR},
		{ NULL,           NULL,                    0,                  INVALID}
	};
	confkeyval *kv;
#ifndef MERO_DONTFORK
	char buf[4];
	int retfd = -1;

	/* Fork into the background immediately.  By doing this our child
	 * can simply do everything it needs to do itself.  Via a pipe it
	 * will tell us if it is happy or not. */
	if (pipe(pfd) == -1) {
		Mfprintf(stderr, "unable to create pipe: %s\n", strerror(errno));
		return(1);
	}
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

/* use after the logger thread has started */
#define MERO_EXIT(status) { \
		char s = status; \
		if (write(retfd, &s, 1) != 1 || close(retfd) != 0) { \
			Mfprintf(stderr, "could not write to parent\n"); \
		} \
		if (status != 0) { \
			Mfprintf(stderr, "fatal startup condition encountered, " \
					"aborting startup\n"); \
			goto shutdown; \
		} \
	}
/* use before logger thread has started */
#define MERO_EXIT_CLEAN(status) { \
		char s = status; \
		if (write(retfd, &s, 1) != 1 || close(retfd) != 0) { \
			Mfprintf(stderr, "could not write to parent\n"); \
		} \
		exit(s); \
	}
#else
#define MERO_EXIT(status) \
	if (status != 0) { \
		Mfprintf(stderr, "fatal startup condition encountered, " \
				"aborting startup\n"); \
		goto shutdown; \
	}
#define MERO_EXIT_CLEAN(status) \
		exit(status);
#endif

	/* seed the randomiser for when we create a database, send responses
	 * to HELO, etc */
	srand(time(NULL));
	/* figure out our hostname */
	gethostname(_mero_hostname, 128);
	/* where is the mserver5 binary we fork on demand?
	 * first try to locate it based on our binary location, fall-back to
	 * hardcoded bin-dir */
	_mero_mserver = get_bin_path();
	if (_mero_mserver != NULL) {
		/* replace the trailing monetdbd by mserver5, fits nicely since
		 * they happen to be of same length */
		char *s = strrchr(_mero_mserver, '/');
		if (s != NULL && strcmp(s + 1, "monetdbd") == 0) {
			s++;
			*s++ = 'm'; *s++ = 's'; *s++ = 'e'; *s++ = 'r';
			*s++ = 'v'; *s++ = 'e'; *s++ = 'r'; *s++ = '5';
			if (stat(_mero_mserver, &sb) == -1)
				_mero_mserver = NULL;
		}
	}
	/* setup default database properties, constants: unlike previous
	 * versions, we do not want changing defaults any more */
	_mero_db_props = getDefaultProps();
	kv = findConfKey(_mero_db_props, "shared");
	kv->val = strdup("yes");
	kv = findConfKey(_mero_db_props, "master");
	kv->val = strdup("no");
	kv = findConfKey(_mero_db_props, "slave");
	kv->val = NULL; /* MURI */
	kv = findConfKey(_mero_db_props, "readonly");
	kv->val = strdup("no");

	/* in case of no arguments, we act backwards compatible: start
	 * merovingian in the hardwired dbfarm location */
	if (argc > 1) {
		/* future: support -v or something like monetdb(1), for now we
		 * just don't */
		if (strcmp(argv[1], "--help") == 0 ||
				strcmp(argv[1], "-h") == 0 ||
				strcmp(argv[1], "help") == 0)
		{
			MERO_EXIT_CLEAN(command_help(argc - 1, &argv[1]));
		} else if (strcmp(argv[1], "--version") == 0 ||
				strcmp(argv[1], "-v") == 0 ||
				strcmp(argv[1], "version") == 0)
		{
			MERO_EXIT_CLEAN(command_version());
		} else if (strcmp(argv[1], "create") == 0) {
			MERO_EXIT_CLEAN(command_create(argc - 1, &argv[1]));
		} else if (strcmp(argv[1], "get") == 0) {
			MERO_EXIT_CLEAN(command_get(ckv, argc - 1, &argv[1]));
		} else if (strcmp(argv[1], "set") == 0) {
			MERO_EXIT_CLEAN(command_set(ckv, argc - 1, &argv[1]));
		} else if (strcmp(argv[1], "start") == 0) {
			/* start without argument just means start hardwired dbfarm */
			if (argc > 2)
				dbfarm = argv[2];
		} else if (strcmp(argv[1], "stop") == 0) {
			MERO_EXIT_CLEAN(command_stop(ckv, argc - 1, &argv[1]));
		} else {
			fprintf(stderr, "monetdbd: unknown command: %s\n", argv[1]);
			command_help(0, NULL);
			MERO_EXIT_CLEAN(1);
		}
	}

	/* check if dbfarm actually exists */
	if (stat(dbfarm, &sb) == -1) {
		Mfprintf(stderr, "dbfarm directory '%s' does not exist, "
				"use monetdbd create first\n", dbfarm);
		MERO_EXIT_CLEAN(1);
	}

	/* chdir to dbfarm so we are at least in a known to exist location */
	if (chdir(dbfarm) < 0) {
		Mfprintf(stderr, "could not move to dbfarm '%s': %s\n",
				dbfarm, strerror(errno));
		MERO_EXIT_CLEAN(1);
	}

	if (_mero_mserver == NULL) {
		_mero_mserver = BINDIR "/mserver5";
		if (stat(_mero_mserver, &sb) == -1) {
			/* exit early if this is not going to work well */
			Mfprintf(stderr, "cannot stat %s executable: %s\n",
					_mero_mserver, strerror(errno));
			MERO_EXIT_CLEAN(1);
		}
	}

	/* read the merovingian properties from the dbfarm */
	readProps(ckv, ".");
	_mero_props = ckv;

	kv = findConfKey(_mero_props, "pidfile");
	pidfilename = kv->val;

	kv = findConfKey(_mero_props, "forward");
	if (strcmp(kv->val, "redirect") != 0 &&
			strcmp(kv->val, "proxy") != 0)
	{
		Mfprintf(stderr, "invalid forwarding mode: %s, defaulting to proxy\n",
				kv->val);
		setConfVal(kv, "proxy");
		writeProps(_mero_props, ".");
	}

	kv = findConfKey(_mero_props, "passphrase");
	if (kv->val == NULL || strlen(kv->val) == 0) {
		if (!autoUpgradePassphraseMar2011Apr2011(_mero_props)) {
			char phrase[128];
			Mfprintf(stderr, "control passphrase unset or has zero-length, "
					"generating one\n");
			generateSalt(phrase, sizeof(phrase));
			setConfVal(kv, phrase);
		}
		writeProps(_mero_props, ".");
	}

	kv = findConfKey(_mero_props, "port");
	if (kv->ival <= 0 || kv->ival > 65535) {
		Mfprintf(stderr, "invalid port number: %s, defaulting to %s\n",
				kv->val, MERO_PORT);
		setConfVal(kv, MERO_PORT);
		writeProps(_mero_props, ".");
	}
	port = (unsigned short)kv->ival;
	kv = findConfKey(_mero_props, "discoveryport");
	if (kv->ival < 0 || kv->ival > 65535) {
		Mfprintf(stderr, "invalid discovery port number: %s, defaulting to %s\n",
				kv->val, MERO_PORT);
		setConfVal(kv, MERO_PORT);
		writeProps(_mero_props, ".");
	}
	discoveryport = (unsigned int)kv->ival;
	kv = findConfKey(_mero_props, "controlport");
	if (kv->ival <= 0 || kv->ival > 65535) {
		Mfprintf(stderr, "invalid control port number: %s, defaulting to %s\n",
				kv->val, CONTROL_PORT);
		setConfVal(kv, CONTROL_SOCK);
		writeProps(_mero_props, ".");
	}
	controlport = (unsigned short)kv->ival;

	/* set up UNIX socket paths for control and mapi */
	p = getConfVal(_mero_props, "sockdir");
	snprintf(control_usock, sizeof(control_usock), "%s/" CONTROL_SOCK "%d",
			p, getConfNum(_mero_props, "controlport"));
	snprintf(mapi_usock, sizeof(control_usock), "%s/" MERO_SOCK "%d",
			p, getConfNum(_mero_props, "port"));

	/* lock such that we are alone on this world */
	if ((ret = MT_lockf(".merovingian_lock", F_TLOCK, 4, 1)) == -1) {
		/* locking failed */
		Mfprintf(stderr, "another merovingian is already running\n");
		MERO_EXIT_CLEAN(1);
	} else if (ret == -2) {
		/* directory or something doesn't exist */
		Mfprintf(stderr, "unable to create %s/.merovingian_lock file: %s\n",
				dbfarm, strerror(errno));
		MERO_EXIT_CLEAN(1);
	}

	_mero_topdp = alloca(sizeof(struct _dpair));
	_mero_topdp->pid = 0;
	_mero_topdp->dbname = NULL;

	/* where should our msg output go to? */
	p = getConfVal(_mero_props, "logfile");
	/* write to the given file */
	_mero_topdp->out = open(p, O_WRONLY | O_APPEND | O_CREAT,
			S_IRUSR | S_IWUSR);
	if (_mero_topdp->out == -1) {
		Mfprintf(stderr, "unable to open '%s': %s\n",
				p, strerror(errno));
		MERO_EXIT_CLEAN(1);
	}
	_mero_topdp->err = _mero_topdp->out;

	_mero_logfile = fdopen(_mero_topdp->out, "a");

	d = _mero_topdp->next = alloca(sizeof(struct _dpair));

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

	/* allow a thread to relock this mutex */
	pthread_mutexattr_init(&mta);
	pthread_mutexattr_settype(&mta, PTHREAD_MUTEX_RECURSIVE);
	pthread_mutex_init(&_mero_topdp_lock, &mta);

	if ((thret = pthread_create(&tid, NULL, (void *(*)(void *))logListener, (void *)NULL)) != 0) {
		Mfprintf(oerr, "%s: FATAL: unable to create logthread: %s\n",
				argv[0], strerror(thret));
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
		Mfprintf(oerr, "%s: FATAL: unable to create signal handlers: %s\n",
				argv[0], strerror(errno));
		MERO_EXIT(1);
	}

	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sa.sa_handler = huphandler;
	if (sigaction(SIGHUP, &sa, NULL) == -1) {
		Mfprintf(oerr, "%s: FATAL: unable to create signal handlers: %s\n",
				argv[0], strerror(errno));
		MERO_EXIT(1);
	}

	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sa.sa_handler = SIG_IGN;
	if (sigaction(SIGPIPE, &sa, NULL) == -1) {
		Mfprintf(oerr, "%s: FATAL: unable to create signal handlers: %s\n",
				argv[0], strerror(errno));
		MERO_EXIT(1);
	}

	sa.sa_flags = SA_SIGINFO;
	sigemptyset(&sa.sa_mask);
	sa.sa_sigaction = childhandler;
	if (sigaction(SIGCHLD, &sa, NULL) == -1) {
		Mfprintf(oerr, "%s: FATAL: unable to create signal handlers: %s\n",
				argv[0], strerror(errno));
		MERO_EXIT(1);
	}

	/* make sure we will be able to write our pid */
	if ((pidfile = fopen(pidfilename, "w")) == NULL) {
		Mfprintf(stderr, "unable to open '%s%s%s' for writing: %s\n",
				pidfilename[0] == '/' ? dbfarm : "",
				pidfilename[0] == '/' ? "/" : "",
				pidfilename, strerror(errno));
		MERO_EXIT(1);
	}

	{
		char cwd[1024];
		if (getcwd(cwd, sizeof(cwd)) == NULL) {
			Mfprintf(stderr, "could not get current working directory: %s\n",
					strerror(errno));
			MERO_EXIT(1);
		}
		msab_init(cwd, NULL);
	}

	unlink(control_usock);
	unlink(mapi_usock);

	/* write out the pid */
	Mfprintf(pidfile, "%d\n", (int)d->pid);
	fclose(pidfile);

	Mfprintf(stdout, "Merovingian %s (%s) starting\n",
			MERO_VERSION, MONETDB_RELEASE);
	Mfprintf(stdout, "monitoring dbfarm %s\n", dbfarm);

	/* open up connections */
	if (
			(e = openConnectionTCP(&sock, port, stdout)) == NO_ERR &&
			(e = openConnectionUNIX(&socku, mapi_usock, 0, stdout)) == NO_ERR &&
			(e = openConnectionUDP(&usock, discoveryport)) == NO_ERR &&
			(e = openConnectionUNIX(&unsock, control_usock, S_IRWXO, _mero_ctlout)) == NO_ERR &&
			(controlport == 0 || (e = openConnectionTCP(&csock, controlport, _mero_ctlout)) == NO_ERR)
	   )
	{
		pthread_t ctid = 0;
		pthread_t dtid = 0;
		int csocks[2];

		if (discoveryport > 0) {
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
		}

		/* From this point merovingian considers itself to be in position to
		 * start running, so flag the parent we will have fun. */
		MERO_EXIT(0);

		/* Paranoia umask, but good, because why would people have to sniff
		 * our private parts? */
		umask(S_IRWXG | S_IRWXO);

		/* handle control commands */
		csocks[0] = unsock;
		csocks[1] = csock;
		if ((thret = pthread_create(&ctid, NULL,
						(void *(*)(void *))controlRunner,
						(void *)&csocks)) != 0)
		{
			Mfprintf(stderr, "unable to create control command thread: %s\n",
					strerror(thret));
			ctid = 0;
		}

		/* start neighbour discovery and notification thread */ 
		if (usock >= 0 && (thret = pthread_create(&dtid, NULL,
					(void *(*)(void *))discoveryRunner, (void *)&usock)) != 0)
		{
			Mfprintf(stderr, "unable to start neighbour discovery thread: %s\n",
					strerror(thret));
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
	if (unlink(control_usock) == -1)
		Mfprintf(stderr, "unable to unlink control socket '%s': %s\n",
				control_usock, strerror(errno));
	if (unlink(mapi_usock) == -1)
		Mfprintf(stderr, "unable to unlink mapi socket '%s': %s\n",
				mapi_usock, strerror(errno));

	if (e != NO_ERR) {
		/* console */
		Mfprintf(oerr, "%s: %s\n", argv[0], e);
		/* logfile */
		Mfprintf(stderr, "%s\n", e);
		MERO_EXIT(1);
	}

shutdown:
	/* stop started mservers */

	kv = findConfKey(ckv, "exittimeout");
	if (d != NULL && atoi(kv->val) > 0) {
		dpair t;
		threadlist tl = NULL, tlw = tl;

		/* we don't need merovingian itself */
		d = d->next;

		pthread_mutex_lock(&_mero_topdp_lock);
		t = d;
		while (t != NULL) {
			if (tl == NULL) {
				tl = tlw = malloc(sizeof(struct _threadlist));
			} else {
				tlw = tlw->next = malloc(sizeof(struct _threadlist));
			}

			tlw->next = NULL;
			if ((thret = pthread_create(&(tlw->tid), NULL,
						(void *(*)(void *))terminateProcess, (void *)t)) != 0)
			{
				Mfprintf(stderr, "%s: unable to create thread to terminate "
						"database '%s': %s\n",
						argv[0], d->dbname, strerror(thret));
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
	if (tid != 0 && (argp = pthread_join(tid, NULL)) != 0) {
		Mfprintf(oerr, "failed to wait for logging thread: %s\n",
				strerror(argp));
	}

	if (_mero_topdp != NULL) {
		close(_mero_topdp->out);
		if (_mero_topdp->out != _mero_topdp->err)
			close(_mero_topdp->err);
	}

	/* remove files that suggest our existence */
	unlink(".merovingian_lock");
	if (pidfilename != NULL) {
		unlink(pidfilename);
		free(pidfilename);
	}

	/* mostly for valgrind... */
	freeConfFile(ckv);
	if (_mero_db_props != NULL) {
		freeConfFile(_mero_db_props);
		free(_mero_db_props);
	}

	/* the child's return code at this point doesn't matter, as noone
	 * will see it */
	return(0);
}

/* vim:set ts=4 sw=4 noexpandtab: */
