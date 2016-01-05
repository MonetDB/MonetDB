/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
 * merovingian will monitor and control the dbfarm given by path in the
 * first argument.  This allows users to create their own dbfarm, but
 * also expert users to run multiple merovingians on the same system
 * easily, since the (persistent) configuration is read from the dbfarm
 * directory.
 */

#include "monetdb_config.h"
#include <msabaoth.h>
#include <mutils.h> /* MT_lockf */
#include <mcrypt.h> /* mcrypt_BackendSum */
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
#include "multiplex-funnel.h"


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
	int len = 0;
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
		if ((int)(q - buf) < len) {
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
		
		if (select(nfds + 1, &readfds, NULL, NULL, &tv) <= 0) {
			if (_mero_keep_logging != 0) {
				continue;
			} else {
				break;
			}
		}

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
	} while (_mero_keep_logging);
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
	char *dbname = strdup(d->dbname);

	er = msab_getStatus(&stats, dbname);
	if (er != NULL) {
		Mfprintf(stderr, "cannot terminate process " LLFMT ": %s\n",
				(long long int)pid, er);
		free(er);
		free(dbname);
		return;
	}

	if (stats == NULL) {
		Mfprintf(stderr, "strange, process " LLFMT " serves database '%s' "
				"which does not exist\n", (long long int)pid, dbname);
		free(dbname);
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
			free(dbname);
			return;
		case SABdbInactive:
			Mfprintf(stdout, "database '%s' appears to have shut down already\n",
					dbname);
			fflush(stdout);
			msab_freeStatus(&stats);
			free(dbname);
			return;
		case SABdbStarting:
			Mfprintf(stderr, "database '%s' appears to be starting up\n",
					 dbname);
			/* starting up, so we'll go to the shut down phase */
			break;
		default:
			Mfprintf(stderr, "unknown state: %d\n", (int)stats->state);
			msab_freeStatus(&stats);
			free(dbname);
			return;
	}

	if (d->type == MEROFUN) {
		multiplexDestroy(dbname);
		msab_freeStatus(&stats);
		free(dbname);
		return;
	} else if (d->type != MERODB) {
		/* barf */
		Mfprintf(stderr, "cannot stop merovingian process role: %s\n", dbname);
		msab_freeStatus(&stats);
		free(dbname);
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
				case SABdbStarting:
					/* ok, try again */
				break;
				case SABdbCrashed:
					Mfprintf (stderr, "database '%s' crashed after SIGTERM\n",
							dbname);
					msab_freeStatus(&stats);
					free(dbname);
					return;
				case SABdbInactive:
					Mfprintf(stdout, "database '%s' has shut down\n", dbname);
					fflush(stdout);
					msab_freeStatus(&stats);
					free(dbname);
					return;
				default:
					Mfprintf(stderr, "unknown state: %d\n", (int)stats->state);
				break;
			}
		}
	}
	Mfprintf(stderr, "timeout of %s seconds expired, sending process " LLFMT
			" (database '%s') the KILL signal\n",
			kv->val, (long long int)pid, dbname);
	kill(pid, SIGKILL);
	msab_freeStatus(&stats);
	free(dbname);
	return;
}

/**
 * Creates a new error, allocated with malloc.  The error should be
 * freed using freeErr().
 */
char *
newErr(const char *fmt, ...)
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


int
main(int argc, char *argv[])
{
	err e;
	int argp;
	char dbfarm[1024];
	char *pidfilename = NULL;
	char *p;
	FILE *pidfile = NULL;
	char control_usock[1024];
	char mapi_usock[1024];
	dpair d = NULL;
	struct _dpair dpcons;
	struct _dpair dpmero;
	struct _dpair dpdisc;
	struct _dpair dpcont;
	int pfd[2];
	pthread_t tid = 0;
	struct sigaction sa;
	int ret;
	int lockfd = -1;
	int sock = -1;
	int usock = -1;
	int unsock = -1;
	int socku = -1;
	unsigned short port = 0;
	char discovery = 0;
	struct stat sb;
	FILE *oerr = NULL;
	pthread_mutexattr_t mta;
	int thret;
	char merodontfork = 0;
	confkeyval ckv[] = {
		{"logfile",       strdup("merovingian.log"), 0,                STR},
		{"pidfile",       strdup("merovingian.pid"), 0,                STR},

		{"sockdir",       strdup("/tmp"),          0,                  STR},
		{"port",          strdup(MERO_PORT),       atoi(MERO_PORT),    INT},

		{"exittimeout",   strdup("60"),            60,                 INT},
		{"forward",       strdup("proxy"),         0,                  OTHER},

		{"discovery",     strdup("true"),          1,                  BOOLEAN},
		{"discoveryttl",  strdup("600"),           600,                INT},

		{"control",       strdup("false"),         0,                  BOOLEAN},
		{"passphrase",    NULL,                    0,                  STR},

		{ NULL,           NULL,                    0,                  INVALID}
	};
	confkeyval *kv;
	int retfd = -1;

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
	/* setup default database properties, constants: unlike historical
	 * versions, we do not want changing defaults any more */
	_mero_db_props = getDefaultProps();
	kv = findConfKey(_mero_db_props, "shared");
	kv->val = strdup("yes");
	kv = findConfKey(_mero_db_props, "readonly");
	kv->val = strdup("no");
	kv = findConfKey(_mero_db_props, "embedr");
	kv->val = strdup("no");
	kv = findConfKey(_mero_db_props, "nclients");
	kv->val = strdup("64");
	kv = findConfKey(_mero_db_props, "type");
	kv->val = strdup("database");
	kv = findConfKey(_mero_db_props, "optpipe");
	kv->val = strdup("default_pipe");
	{ /* nrthreads */
		int ncpus = -1;
		char cnt[8];

#if defined(HAVE_SYSCONF) && defined(_SC_NPROCESSORS_ONLN)
		/* this works on Linux, Solaris and AIX */
		ncpus = sysconf(_SC_NPROCESSORS_ONLN);
#elif defined(HAVE_SYS_SYSCTL_H) && defined(HW_NCPU)   /* BSD */
		size_t len = sizeof(int);
		int mib[3];

		/* Everyone should have permission to make this call,
		 * if we get a failure something is really wrong. */
		mib[0] = CTL_HW;
		mib[1] = HW_NCPU;
		mib[2] = -1;
		sysctl(mib, 3, &ncpus, &len, NULL, 0);
#elif defined(WIN32)
		SYSTEM_INFO sysinfo;

		GetSystemInfo(&sysinfo);
		ncpus = sysinfo.dwNumberOfProcessors;
#endif
		if (ncpus > 0) {
			snprintf(cnt, sizeof(cnt), "%d", ncpus);
			kv = findConfKey(_mero_db_props, "nthreads");
			kv->val = strdup(cnt);
		}
	}

	*dbfarm = '\0';
	if (argc > 1) {
		if (strcmp(argv[1], "--help") == 0 ||
				strcmp(argv[1], "-h") == 0 ||
				strcmp(argv[1], "help") == 0)
		{
			exit(command_help(argc - 1, &argv[1]));
		} else if (strcmp(argv[1], "--version") == 0 ||
				strcmp(argv[1], "-v") == 0 ||
				strcmp(argv[1], "version") == 0)
		{
			exit(command_version());
		} else if (strcmp(argv[1], "create") == 0) {
			exit(command_create(argc - 1, &argv[1]));
		} else if (strcmp(argv[1], "get") == 0) {
			exit(command_get(ckv, argc - 1, &argv[1]));
		} else if (strcmp(argv[1], "set") == 0) {
			exit(command_set(ckv, argc - 1, &argv[1]));
		} else if (strcmp(argv[1], "start") == 0) {
			if (argc > 3 && strcmp(argv[2], "-n") == 0)
					merodontfork = 1;
			if (argc == 3 + merodontfork) {
				int len;
				len = snprintf(dbfarm, sizeof(dbfarm), "%s",
						argv[2 + merodontfork]);
			
				if (len > 0 && (size_t)len >= sizeof(dbfarm)) {
					Mfprintf(stderr, "fatal: dbfarm exceeds allocated " \
							"path length, please file a bug at " \
							"http://bugs.monetdb.org/\n");
					exit(1);
				}
			} else {
				command_help(argc, argv);
				exit(1);
			}
		} else if (strcmp(argv[1], "stop") == 0) {
			exit(command_stop(ckv, argc - 1, &argv[1]));
		} else {
			fprintf(stderr, "monetdbd: unknown command: %s\n", argv[1]);
			command_help(0, NULL);
			exit(1);
		}
	} else {
		command_help(0, NULL);
		exit(1);
	}

	assert(*dbfarm != '\0');

	/* fork into background before doing anything more */
	if (!merodontfork) {
		char buf[4];

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
	}

/* use after the logger thread has started */
#define MERO_EXIT(status) if (!merodontfork) { \
		char s = status; \
		if (write(retfd, &s, 1) != 1 || close(retfd) != 0) { \
			Mfprintf(stderr, "could not write to parent\n"); \
		} \
		if (status != 0) { \
			Mfprintf(stderr, "fatal startup condition encountered, " \
					"aborting startup\n"); \
			goto shutdown; \
		} \
	} else { \
		if (status != 0) { \
			Mfprintf(stderr, "fatal startup condition encountered, " \
					"aborting startup\n"); \
			goto shutdown; \
		} \
	}
/* use before logger thread has started */
#define MERO_EXIT_CLEAN(status) if (!merodontfork) { \
		char s = status; \
		if (write(retfd, &s, 1) != 1 || close(retfd) != 0) { \
			Mfprintf(stderr, "could not write to parent\n"); \
		} \
		exit(s); \
	} else { \
		exit(status); \
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
	/* absolutise dbfarm if it isn't yet (we're in it now) */
	if (dbfarm[0] != '/') {
		if (getcwd(dbfarm, sizeof(dbfarm)) == NULL) {
			if (errno == ERANGE) {
				Mfprintf(stderr, "current path exceeds allocated path length" \
						"please file a bug at http://bugs.monetdb.org\n");
			} else {
				Mfprintf(stderr, "could not get dbfarm working directory: %s\n",
						strerror(errno));
			}
			MERO_EXIT_CLEAN(1);
		}
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
	if (readProps(ckv, ".") != 0) {
		Mfprintf(stderr, "cannot find or read properties file, was "
				"this dbfarm created by `monetdbd create`?\n");
		MERO_EXIT_CLEAN(1);
	}
	_mero_props = ckv;

	pidfilename = getConfVal(_mero_props, "pidfile");

	p = getConfVal(_mero_props, "forward");
	if (strcmp(p, "redirect") != 0 && strcmp(p, "proxy") != 0) {
		Mfprintf(stderr, "invalid forwarding mode: %s, defaulting to proxy\n",
				p);
		kv = findConfKey(_mero_props, "forward");
		setConfVal(kv, "proxy");
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

	discovery = getConfNum(_mero_props, "discovery");

	/* check and trim the hash-algo from the passphrase for easy use
	 * lateron */
	kv = findConfKey(_mero_props, "passphrase");
	if (kv->val != NULL) {
		char *h = kv->val + 1;
		if ((p = strchr(h, '}')) == NULL) {
			Mfprintf(stderr, "warning: incompatible passphrase (not hashed as "
					MONETDB5_PASSWDHASH "), disabling passphrase\n");
		} else {
			*p = '\0';
			if (strcmp(h, MONETDB5_PASSWDHASH) != 0) {
				Mfprintf(stderr, "warning: passphrase hash '%s' incompatible, "
						"expected '%s', disabling passphrase\n",
						h, MONETDB5_PASSWDHASH);
			} else {
				setConfVal(kv, p + 1);
			}
		}
	}

	/* set up UNIX socket paths for control and mapi */
	p = getConfVal(_mero_props, "sockdir");
	snprintf(control_usock, sizeof(control_usock), "%s/" CONTROL_SOCK "%d",
			p, port);
	snprintf(mapi_usock, sizeof(control_usock), "%s/" MERO_SOCK "%d",
			p, port);

	/* lock such that we are alone on this world */
	if ((lockfd = MT_lockf(".merovingian_lock", F_TLOCK, 4, 1)) == -1) {
		/* locking failed */
		Mfprintf(stderr, "another monetdbd is already running\n");
		MERO_EXIT_CLEAN(1);
	} else if (lockfd == -2) {
		/* directory or something doesn't exist */
		Mfprintf(stderr, "unable to create %s/.merovingian_lock file: %s\n",
				dbfarm, strerror(errno));
		MERO_EXIT_CLEAN(1);
	}

	_mero_topdp = &dpcons;
	_mero_topdp->pid = 0;
	_mero_topdp->type = MERO;
	_mero_topdp->dbname = NULL;

	/* where should our msg output go to? */
	p = getConfVal(_mero_props, "logfile");
	/* write to the given file */
	_mero_topdp->out = open(p, O_WRONLY | O_APPEND | O_CREAT,
			S_IRUSR | S_IWUSR);
	if (_mero_topdp->out == -1) {
		Mfprintf(stderr, "unable to open '%s': %s\n",
				p, strerror(errno));
		MT_lockf(".merovingian_lock", F_ULOCK, 4, 1);
		close(lockfd);
		MERO_EXIT_CLEAN(1);
	}
	_mero_topdp->err = _mero_topdp->out;

	_mero_logfile = fdopen(_mero_topdp->out, "a");

	d = _mero_topdp->next = &dpmero;

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
	d->type = MERO;
	d->dbname = "merovingian";

	/* separate entry for the neighbour discovery service */
	d = d->next = &dpdisc;
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
	d->type = MERO;
	d->dbname = "discovery";
	d->next = NULL;

	/* separate entry for the control runner */
	d = d->next = &dpcont;
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
	d->type = MERO;
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
	sa.sa_handler = segvhandler;
	if (sigaction(SIGSEGV, &sa, NULL) == -1) {
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
				pidfilename[0] != '/' ? dbfarm : "",
				pidfilename[0] != '/' ? "/" : "",
				pidfilename, strerror(errno));
		MERO_EXIT(1);
	}

	msab_dbfarminit(dbfarm);


	/* write out the pid */
	Mfprintf(pidfile, "%d\n", (int)d->pid);
	fclose(pidfile);

	Mfprintf(stdout, "Merovingian %s (%s) starting\n",
			MERO_VERSION, MONETDB_RELEASE);
	Mfprintf(stdout, "monitoring dbfarm %s\n", dbfarm);

	/* open up connections */
	if (
			(e = openConnectionTCP(&sock, port, stdout)) == NO_ERR &&
			/* coverity[operator_confusion] */
			(unlink(control_usock) | unlink(mapi_usock) | 1) &&
			(e = openConnectionUNIX(&socku, mapi_usock, 0, stdout)) == NO_ERR &&
			(discovery == 0 || (e = openConnectionUDP(&usock, port)) == NO_ERR) &&
			(e = openConnectionUNIX(&unsock, control_usock, S_IRWXO, _mero_ctlout)) == NO_ERR
	   )
	{
		pthread_t ctid = 0;
		pthread_t dtid = 0;

		if (discovery == 1) {
			_mero_broadcastsock = socket(AF_INET, SOCK_DGRAM, 0);
			ret = 1;
			if (_mero_broadcastsock == -1 ||
				setsockopt(_mero_broadcastsock,
						   SOL_SOCKET, SO_BROADCAST, &ret, sizeof(ret)) == -1)
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
			_mero_broadcastaddr.sin_port = htons(port);
		}

		/* From this point merovingian considers itself to be in position to
		 * start running, so flag the parent we will have fun. */
		MERO_EXIT(0);

		/* Paranoia umask, but good, because why would people have to sniff
		 * our private parts? */
		umask(S_IRWXG | S_IRWXO);

		/* handle control commands */
		if ((thret = pthread_create(&ctid, NULL,
						(void *(*)(void *))controlRunner,
						(void *)&unsock)) != 0)
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
	if (unsock != -1 && unlink(control_usock) == -1)
		Mfprintf(stderr, "unable to unlink control socket '%s': %s\n",
				control_usock, strerror(errno));
	if (socku != -1 && unlink(mapi_usock) == -1)
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
	if (d->next != NULL && atoi(kv->val) > 0) {
		dpair t;
		threadlist tl = NULL, tlw = tl;

		pthread_mutex_lock(&_mero_topdp_lock);
		t = d->next;
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
						argv[0], t->dbname, strerror(thret));
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

	if (lockfd >= 0) {
		MT_lockf(".merovingian_lock", F_ULOCK, 4, 1);
		close(lockfd);
	}

	/* the child's return code at this point doesn't matter, as noone
	 * will see it */
	return(0);
}

/* vim:set ts=4 sw=4 noexpandtab: */
