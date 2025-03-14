/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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
#include "msabaoth.h"
#include "mutils.h" /* MT_lockf */
#include "mcrypt.h" /* mcrypt_BackendSum */
#include "mstring.h"			/* strcpy_len */
#include "utils/utils.h"
#include "utils/properties.h"
#include "utils/glob.h"
#include "utils/database.h"
#include "utils/control.h"

#include <sys/types.h>
#include <sys/stat.h> /* stat */
#include <sys/wait.h> /* wait */
#include <sys/socket.h>
#include <sys/un.h>
#include <netdb.h>
#include <netinet/in.h>
#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#include <fcntl.h>
#include <unistd.h> /* unlink, isatty */
#include <string.h> /* strerror */
#include <signal.h> /* handle Ctrl-C, etc. */
#include <time.h>

#include "merovingian.h"
#include "client.h"
#include "connections.h"
#include "controlrunner.h"
#include "discoveryrunner.h"
#include "handlers.h"
#include "argvcmds.h"
#include "multiplex-funnel.h"

#ifndef O_CLOEXEC
#define O_CLOEXEC		0
#endif

#ifndef HAVE_PIPE2
#define pipe2(pipefd, flags)	pipe(pipefd)
#endif

/* private structs */

typedef struct _threadlist {
	pthread_t tid;    /* thread id */
	struct _threadlist* next;
}* threadlist;


/* globals */

/* full path to the mserver5 binary */
#ifndef PATH_MAX
# define PATH_MAX 1024
#endif
char _mero_mserver[PATH_MAX];
/* list of databases that we have started */
dpair _mero_topdp = NULL;
/* lock to _mero_topdp, initialised as recursive later on */
pthread_mutex_t _mero_topdp_lock = PTHREAD_MUTEX_INITIALIZER;
/* for the logger, when set to 0, the logger terminates */
volatile int _mero_keep_logging = 1;
/* for accepting connections, when set to 0, listening socket terminates */
volatile sig_atomic_t _mero_keep_listening = 1;
/* merovingian log level, default is to log ERROR, WARNING and INFORMATION messages */
static loglevel _mero_loglevel = INFORMATION;
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
logFD(dpair dp, int fd, const char *type, const char *dbname, long long pid, FILE *stream, bool rest)
{
	time_t now;
	char buf[8096];
	ssize_t len = 0;
	ssize_t last = 0;
	char *p, *q;
	struct tm *tmp;
	char mytime[20];

	assert(fd == 0 || fd == 1);
	do {
		do {
			ssize_t n;
		  repeat:
			n = read(dp->input[fd].fd, buf + len, sizeof(buf) - len - 1);
			if (n <= 0) {
				rest = false;
				break;
			}
			len += n;
			buf[len] = 0;
		} while (buf[len - 1] != '\n' && len < (ssize_t) sizeof(buf) - 1);
		if (len == 0)
			break;
		now = time(NULL);
		tmp = localtime(&now);
		strftime(mytime, sizeof(mytime), "%Y-%m-%d %H:%M:%S", tmp);
		for (q = buf + last; *q; q = p + 1) {
			p = strchr(q, '\n');
			if (p == NULL) {
				if (q > buf) {
					/* the last part of the last line didn't fit, so
					 * just continue reading */
					len = strlen(q);
					memmove(buf, q, len);
					last = 0;
					goto repeat;
				}
				/* we must have received a ridiculously long line */
				dp->input[fd].ts = now;
				dp->input[fd].cnt = 0;
				dp->input[fd].buf[0] = '\0';
				fprintf(stream, "%s %s %s[%lld]: %s\n",
						mytime, type, dbname, pid, q);
				break;
			}
			last = p + 1 - buf;
			if (p == q) {
				/* empty line, don't bother */
				continue;
			}
			*p = 0;				/* was '\n' which we do not store */
			char *s;
			if ((s = strstr(q, "monetdbd's logfile")) != NULL) {
				while (--s > q && *s != ',')
					;
				/* shorten message with reference to logfile */
				*s = '\0';
			}
			if (dp->input[fd].cnt < 30000 && strcmp(dp->input[fd].buf, q) == 0) {
				/* repeat of last message */
				dp->input[fd].cnt++;
				dp->input[fd].ts = now;
			} else {
				if (dp->input[fd].cnt > 0) {
					/* last message was repeated but not all repeats reported */
					char tmptime[20];
					strftime(tmptime, sizeof(tmptime), "%Y-%m-%d %H:%M:%S",
							 localtime(&dp->input[fd].ts));
					if (dp->input[fd].cnt == 1)
						fprintf(stream, "%s %s %s[%lld]: %s\n",
								tmptime, type, dbname, pid, dp->input[fd].buf);
					else
						fprintf(stream, "%s %s %s[%lld]: message repeated %d times: %s\n",
								tmptime, type, dbname, pid, dp->input[fd].cnt, dp->input[fd].buf);
				}
				dp->input[fd].ts = now;
				dp->input[fd].cnt = 0;
				strcpy(dp->input[fd].buf, q);
				fprintf(stream, "%s %s %s[%lld]: %.*s\n",
						mytime, type, dbname, pid, (int) (p - q), q);
			}
		}
		fflush(stream);
	} while (rest);
	fflush(stream);
}

static void *
logListener(void *x)
{
	dpair d = _mero_topdp;
	dpair w;
#ifdef HAVE_POLL
	struct pollfd *pfd;
#else
	struct timeval tv;
	fd_set readfds;
#endif
	int nfds;

	(void)x;

#ifdef HAVE_PTHREAD_SETNAME_NP
	pthread_setname_np(
#ifndef __APPLE__
		pthread_self(),
#endif
		__func__);
#endif

	/* the first entry in the list of d is where our output should go to
	 * but we only use the streams, so we don't care about it in the
	 * normal loop */
	d = d->next;

	do {
		/* wait max 1 second, tradeoff between performance and being
		 * able to catch up new logger streams */
		nfds = 0;

		/* make sure no one is killing or adding entries here */
		pthread_mutex_lock(&_mero_topdp_lock);

#ifdef HAVE_POLL
		for (w = d; w != NULL; w = w->next) {
			if (w->pid > 0)
				nfds += 2;
		}
		/* +1 for freebsd compiler issue with stringop-overflow error */
		pfd = malloc((nfds+1) * sizeof(struct pollfd));
		nfds = 0;
		for (w = d; w != NULL; w = w->next) {
			if (w->pid <= 0)
				continue;
			pfd[nfds++] = (struct pollfd) {.fd = w->input[0].fd, .events = POLLIN};
			if (w->input[0].fd != w->input[1].fd)
				pfd[nfds++] = (struct pollfd) {.fd = w->input[1].fd, .events = POLLIN};
			w->flag |= 1;
		}
#else
		tv = (struct timeval) {.tv_sec = 1};
		FD_ZERO(&readfds);
		for (w = d; w != NULL; w = w->next) {
			if (w->pid <= 0)
				continue;
			FD_SET(w->input[0].fd, &readfds);
			if (nfds < w->input[0].fd)
				nfds = w->input[0].fd;
			FD_SET(w->input[1].fd, &readfds);
			if (nfds < w->input[1].fd)
				nfds = w->input[1].fd;
			w->flag |= 1;
		}
#endif

		pthread_mutex_unlock(&_mero_topdp_lock);

		if (
#ifdef HAVE_POLL
			poll(pfd, nfds, 1000)
#else
			select(nfds + 1, &readfds, NULL, NULL, &tv)
#endif
			<= 0) {
#ifdef HAVE_POLL
			free(pfd);
#endif
			if (_mero_keep_logging != 0) {
				continue;
			} else {
				break;
			}
		}
		reinitialize();

		pthread_mutex_lock(&_mero_topdp_lock);

		w = d;
		while (w != NULL) {
			/* only look at records we've added in the previous loop */
			if (w->pid > 0 && w->flag & 1) {
#ifdef HAVE_POLL
				for (int i = 0; i < nfds; i++) {
					if (pfd[i].fd == w->input[0].fd && pfd[i].revents & POLLIN)
						logFD(w, 0, "MSG", w->dbname,
							  (long long int)w->pid, _mero_logfile, false);
					else if (pfd[i].fd == w->input[1].fd && pfd[i].revents & POLLIN)
						logFD(w, 1, "ERR", w->dbname,
							  (long long int)w->pid, _mero_logfile, false);
				}
#else
				if (FD_ISSET(w->input[0].fd, &readfds) != 0)
					logFD(w, 0, "MSG", w->dbname,
						  (long long int)w->pid, _mero_logfile, false);
				if (w->input[1].fd != w->input[0].fd && FD_ISSET(w->input[1].fd, &readfds) != 0)
					logFD(w, 1, "ERR", w->dbname,
						  (long long int)w->pid, _mero_logfile, false);
#endif
				w->flag &= ~1;
			}
			w = w->next;
		}

		pthread_mutex_unlock(&_mero_topdp_lock);

#ifdef HAVE_POLL
		free(pfd);
#endif
		fflush(_mero_logfile);
	} while (_mero_keep_logging);
	return NULL;
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

	va_start(ap, fmt);

	(void) vsnprintf(message, sizeof(message), fmt, ap);

	va_end(ap);

	ret = strdup(message);
	return(ret);
}

loglevel
getLogLevel(void)
{
	return _mero_loglevel;
}

void
setLogLevel(loglevel level)
{
	_mero_loglevel = level;
}

static void *
doTerminateProcess(void *p)
{
	dpair dp = p;
#ifdef HAVE_PTHREAD_SETNAME_NP
	pthread_setname_np(
#ifndef __APPLE__
		pthread_self(),
#endif
		__func__);
#endif
	pthread_mutex_lock(&dp->fork_lock);
	(void) terminateProcess(dp->dbname, dp->pid, dp->type);
	pthread_mutex_unlock(&dp->fork_lock);
	return NULL;
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
	int socks[3] = {-1, -1, -1};
	int discsocks[2] = {-1, -1};
	int unsock = -1;
	char* host = NULL;
	unsigned short port = 0;
	bool discovery = false;
	struct stat sb;
	FILE *oerr = NULL;
	int thret;
	bool merodontfork = false;
	confkeyval ckv[] = {
		{"logfile",       strdup("merovingian.log"), 0,                STR},
		{"pidfile",       strdup("merovingian.pid"), 0,                STR},
		{"loglevel",      strdup("information"),   INFORMATION,        LOGLEVEL},

		{"sockdir",       strdup("/tmp"),          0,                  STR},
		{"listenaddr",    strdup("localhost"),     0,                  LADDR},
		{"port",          strdup(MERO_PORT),       atoi(MERO_PORT),    INT},

		{"exittimeout",   strdup("60"),            60,                 SINT},
		{"forward",       strdup("proxy"),         0,                  OTHER},

		{"discovery",     strdup("true"),          1,                  BOOLEAN},
		{"discoveryttl",  strdup("600"),           600,                INT},

		{"control",       strdup("false"),         0,                  BOOLEAN},
		{"passphrase",    NULL,                    0,                  STR},

		{"snapshotdir",   NULL,                    0,                  STR},
#ifdef HAVE_LIBLZ4
		{"snapshotcompression", strdup(".tar.lz4"), 0,                 STR},
#else
		{"snapshotcompression", strdup(".tar"),     0,                 STR},
#endif
		{"keepalive",     strdup("60"),            60,                 INT},

		{ NULL,           NULL,                    0,                  INVALID}
	};
	confkeyval *kv;
	int retfd = -1;
	int dup_err;

	/* seed the randomiser for when we create a database, send responses
	 * to HELO, etc */
	srand(time(NULL));
	/* figure out our hostname */
	gethostname(_mero_hostname, 128);
	/* where is the mserver5 binary we fork on demand?
	 * first try to locate it based on our binary location, fall-back to
	 * hardcoded bin-dir */
	p = get_bin_path();
	if (p != NULL) {
		if (strcpy_len(_mero_mserver, p, sizeof(_mero_mserver)) >= sizeof(_mero_mserver)) {
			Mlevelfprintf(ERROR, stderr, "fatal: monetdbd full path name is too long\n");
			exit(1);
		}
		/* Find where the string monetdbd actually starts */
		char *s = strstr(_mero_mserver, "monetdbd");
		if (s != NULL) {
			/* Replace the 8 following characters with the characters mserver5.
			 * This should work even if the executables have prefixes or
			 * suffixes */
			for (int i = 0; i < 8; i++)
				s[i] = "mserver5"[i];
			if (stat(_mero_mserver, &sb) == -1)
				_mero_mserver[0] = 0;
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
	kv = findConfKey(_mero_db_props, "embedpy3");
	kv->val = strdup("no");
	kv = findConfKey(_mero_db_props, "embedc");
	kv->val = strdup("no");
	kv = findConfKey(_mero_db_props, "nclients");
	kv->val = strdup("64");
	kv = findConfKey(_mero_db_props, "type");
	kv->val = strdup("database");
	kv = findConfKey(_mero_db_props, "optpipe");
	kv->val = strdup("default_pipe");
	{ /* nrthreads */
		int ncpus = -1;
		char cnt[11];

#if defined(HAVE_SYSCONF) && defined(_SC_NPROCESSORS_ONLN)
		/* this works on Linux, Solaris and AIX */
		ncpus = sysconf(_SC_NPROCESSORS_ONLN);
#elif defined(HW_NCPU)   /* BSD */
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
			kv = findConfKey(_mero_db_props, "ncopyintothreads");
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
				merodontfork = true;
			if (argc == 3 + merodontfork) {
				int len;
				len = snprintf(dbfarm, sizeof(dbfarm), "%s",
						argv[2 + merodontfork]);

				if (len > 0 && (size_t)len >= sizeof(dbfarm)) {
					Mlevelfprintf(ERROR, stderr, "fatal: dbfarm exceeds allocated " \
							"path length, please file a bug at " \
							"https://github.com/MonetDB/MonetDB/issues/\n");
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

		/* Fork into the background immediately.  By doing this, our child
		 * can simply do everything it needs to do itself.  Via a pipe it
		 * will tell us if it is happy or not. */
		if (pipe2(pfd, O_CLOEXEC) == -1) {
			Mlevelfprintf(ERROR, stderr, "unable to create pipe: %s\n", strerror(errno));
			return(1);
		}
		switch (fork()) {
			case -1:
				/* oops, forking went wrong! */
				Mlevelfprintf(ERROR, stderr, "unable to fork into background: %s\n",
						strerror(errno));
				close(pfd[0]);
				close(pfd[1]);
				return(1);
			case 0:
				/* detach client from controlling tty, we only write to the
				 * pipe to daddy */
				if (setsid() < 0)
					Mlevelfprintf(WARNING, stderr, "hmmm, can't detach from controlling tty, "
							"continuing anyway\n");
				if ((retfd = open("/dev/null", O_RDONLY | O_CLOEXEC)) < 0) {
					Mlevelfprintf(ERROR, stderr, "unable to dup stdin: %s\n", strerror(errno));
					return(1);
				}
				dup_err = dup2(retfd, 0);
				if (dup_err == -1) {
					Mlevelfprintf(ERROR, stderr, "unable to dup stdin: %s\n", strerror(errno));
				}
				close(retfd);
				close(pfd[0]); /* close unused read end */
				retfd = pfd[1]; /* store the write end */
				if (dup_err == -1) {
					return(1);
				}
#if !defined(HAVE_PIPE2) || O_CLOEXEC == 0
				(void) fcntl(retfd, F_SETFD, FD_CLOEXEC);
#endif
				break;
			default:
				/* the parent, we want it to die, after we know the child
				 * is having a good time */
				close(pfd[1]); /* close unused write end */
				freeConfFile(ckv); /* make debug tools happy */
				if (read(pfd[0], &buf, 1) != 1) {
					Mlevelfprintf(ERROR, stderr, "unable to retrieve startup status\n");
					close(pfd[0]);
					return(1);
				}
				close(pfd[0]);
				return(buf[0]); /* whatever the child returned, we return */
		}
	}

/* use after the logger thread has started */
#define MERO_EXIT(status)											\
	do {															\
		if (!merodontfork) {										\
			char s = (char) status;									\
			if (write(retfd, &s, 1) != 1 || close(retfd) != 0) {	\
				Mlevelfprintf(ERROR, stderr, "could not write to parent\n");	\
			}														\
		}															\
		if (status != 0) {											\
			Mlevelfprintf(ERROR, stderr, "fatal startup condition encountered, " \
					 "aborting startup\n");							\
			goto shutdown;											\
		}															\
	} while (0)

/* use before logger thread has started */
#define MERO_EXIT_CLEAN(status)										\
	do {															\
		if (!merodontfork) {										\
			char s = (char) status;									\
			if (write(retfd, &s, 1) != 1 || close(retfd) != 0) {	\
				Mlevelfprintf(ERROR, stderr, "could not write to parent\n");	\
			}														\
		}															\
		exit(status);												\
	} while (0)

	/* chdir to dbfarm so we are at least in a known to exist location */
	if (chdir(dbfarm) < 0) {
		if (errno == ENOENT)
			Mlevelfprintf(ERROR, stderr, "dbfarm directory '%s' does not exist, "
					 "use monetdbd create first\n", dbfarm);
		else
			Mlevelfprintf(ERROR, stderr, "could not move to dbfarm '%s': %s\n",
					 dbfarm, strerror(errno));
		MERO_EXIT_CLEAN(1);
	}
	/* absolutise dbfarm if it isn't yet (we're in it now) */
	if (dbfarm[0] != '/') {
		if (getcwd(dbfarm, sizeof(dbfarm)) == NULL) {
			if (errno == ERANGE) {
				Mlevelfprintf(ERROR, stderr, "current path exceeds allocated path length" \
						"please file a bug at https://github.com/MonetDB/MonetDB/issues/\n");
			} else {
				Mlevelfprintf(ERROR, stderr, "could not get dbfarm working directory: %s\n",
						strerror(errno));
			}
			MERO_EXIT_CLEAN(1);
		}
	}

	if (_mero_mserver[0] == 0) {
		if (strcpy_len(_mero_mserver, BINDIR "/mserver5", sizeof(_mero_mserver)) >= sizeof(_mero_mserver)) {
			Mlevelfprintf(ERROR, stderr, "fatal: mserver5 full path name is too long\n");
			MERO_EXIT_CLEAN(1);
		}
		if (stat(_mero_mserver, &sb) == -1) {
			/* exit early if this is not going to work well */
			Mlevelfprintf(ERROR, stderr, "cannot stat %s executable: %s\n",
					_mero_mserver, strerror(errno));
			MERO_EXIT_CLEAN(1);
		}
	}

	/* read the merovingian properties from the dbfarm */
	if (readProps(ckv, ".") != 0) {
		Mlevelfprintf(ERROR, stderr, "cannot find or read properties file, was "
				"this dbfarm created by `monetdbd create`?\n");
		MERO_EXIT_CLEAN(1);
	}
	_mero_props = ckv;

	pidfilename = getConfVal(_mero_props, "pidfile");

	p = getConfVal(_mero_props, "forward");
	if (strcmp(p, "redirect") != 0 && strcmp(p, "proxy") != 0) {
		Mlevelfprintf(ERROR, stderr, "invalid forwarding mode: %s, defaulting to proxy\n",
				p);
		kv = findConfKey(_mero_props, "forward");
		setConfVal(kv, "proxy");
		writeProps(_mero_props, ".");
	}

	kv = findConfKey(_mero_props, "listenaddr");
	if (kv->val == NULL || strlen(kv->val) < 1) {
		Mlevelfprintf(ERROR, stderr, "invalid host name: %s, defaulting to localhost\n",
				kv->val);
		setConfVal(kv, "localhost");
		writeProps(_mero_props, ".");
	}
	host = kv->val;

	kv = findConfKey(_mero_props, "port");
	if (kv->ival <= 0 || kv->ival > 65535) {
		Mlevelfprintf(ERROR, stderr, "invalid port number: %s, defaulting to %s\n",
				kv->val, MERO_PORT);
		setConfVal(kv, MERO_PORT);
		writeProps(_mero_props, ".");
	}
	port = (unsigned short)kv->ival;

	discovery = getConfNum(_mero_props, "discovery");
	_mero_loglevel = getConfNum(_mero_props, "loglevel");

	/* check and trim the hash-algo from the passphrase for easy use
	 * later on */
	kv = findConfKey(_mero_props, "passphrase");
	if (kv->val != NULL) {
		char *h = kv->val + 1;
		if ((p = strchr(h, '}')) == NULL) {
			Mlevelfprintf(WARNING, stderr, "warning: incompatible passphrase (not hashed as "
					MONETDB5_PASSWDHASH "), disabling passphrase\n");
		} else {
			*p = '\0';
			if (strcmp(h, MONETDB5_PASSWDHASH) != 0) {
				Mlevelfprintf(WARNING, stderr, "warning: passphrase hash '%s' incompatible, "
						"expected '%s', disabling passphrase\n",
						h, MONETDB5_PASSWDHASH);
			} else {
				/* p points into kv->val which gets freed before p
				 * gets copied inside setConfVal, hence we need to
				 * make a temporary copy */
				p = strdup(p + 1);
				setConfVal(kv, p);
				free(p);
			}
		}
	}

	/* lock such that we are alone on this world */
	if ((lockfd = MT_lockf(".merovingian_lock", F_TLOCK)) == -1) {
		/* locking failed */
		Mlevelfprintf(ERROR, stderr, "another monetdbd is already running\n");
		MERO_EXIT_CLEAN(1);
	} else if (lockfd == -2) {
		/* directory or something doesn't exist */
		Mlevelfprintf(ERROR, stderr, "unable to create %s/.merovingian_lock file: %s\n",
				dbfarm, strerror(errno));
		MERO_EXIT_CLEAN(1);
	}

	/* set up UNIX socket paths for control and mapi */
	p = getConfVal(_mero_props, "sockdir");
	snprintf(control_usock, sizeof(control_usock), "%s/" CONTROL_SOCK "%d",
			p, port);
	snprintf(mapi_usock, sizeof(mapi_usock), "%s/" MERO_SOCK "%d",
			p, port);

	if ((remove(control_usock) != 0 && errno != ENOENT) ||
		(remove(mapi_usock) != 0 && errno != ENOENT)) {
		/* cannot remove socket files */
		Mlevelfprintf(ERROR, stderr, "cannot remove socket files: %s\n", strerror(errno));
		MERO_EXIT_CLEAN(1);
	}

	/* time to initialize the stream library */
	if (mnstr_init() < 0) {
		Mlevelfprintf(ERROR, stderr, "cannot initialize stream library\n");
		MERO_EXIT_CLEAN(1);
	}

	dpcons = (struct _dpair) {
		.type = MERO,
		.fork_lock = PTHREAD_MUTEX_INITIALIZER,
	};
	_mero_topdp = &dpcons;

	/* where should our msg output go to? */
	p = getConfVal(_mero_props, "logfile");
	/* write to the given file */
	_mero_topdp->input[0].fd = open(p, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC,
			S_IRUSR | S_IWUSR);
	if (_mero_topdp->input[0].fd == -1) {
		Mlevelfprintf(ERROR, stderr, "unable to open '%s': %s\n",
				p, strerror(errno));
		MERO_EXIT_CLEAN(1);
	}
#if O_CLOEXEC == 0
	(void) fcntl(_mero_topdp->input[0].fd, F_SETFD, FD_CLOEXEC);
#endif
	_mero_topdp->input[1].fd = _mero_topdp->input[0].fd;

	if(!(_mero_logfile = fdopen(_mero_topdp->input[0].fd, "a"))) {
		Mlevelfprintf(ERROR, stderr, "unable to open file descriptor: %s\n",
				 strerror(errno));
		MERO_EXIT(1);
	}

	dpmero = (struct _dpair) {
		.fork_lock = PTHREAD_MUTEX_INITIALIZER,
		.pid = getpid(),
		.type = MERO,
		.dbname = "merovingian",
	};
	d = _mero_topdp->next = &dpmero;

	/* redirect stdout */
	if (pipe2(pfd, O_CLOEXEC) == -1) {
		Mlevelfprintf(ERROR, stderr, "unable to create pipe: %s\n",
				strerror(errno));
		MERO_EXIT(1);
	}
#if !defined(HAVE_PIPE2) || O_CLOEXEC == 0
	(void) fcntl(pfd[0], F_SETFD, FD_CLOEXEC);
#endif
	d->input[0].fd = pfd[0];
	dup_err = dup2(pfd[1], 1);
	close(pfd[1]);
	if (dup_err == -1) {
		Mlevelfprintf(ERROR, stderr, "unable to dup stderr\n");
		MERO_EXIT(1);
	}

	/* redirect stderr */
	if (pipe2(pfd, O_CLOEXEC) == -1) {
		Mlevelfprintf(ERROR, stderr, "unable to create pipe: %s\n",
				strerror(errno));
		MERO_EXIT(1);
	}
#if !defined(HAVE_PIPE2) || O_CLOEXEC == 0
	(void) fcntl(pfd[0], F_SETFD, FD_CLOEXEC);
#endif
	/* before it is too late, save original stderr */
	// the duplications is happening so we can write err messages both
	// to the console stderr, through `oerr` FILE*, and through the
	// logging thread to the merovingian logfile. the latter is
	// happening through replacing the standard fd for stderr (namely
	// fd=2) with a writing end of a pipe which reading end goes to the
	// logging thread
#ifdef F_DUPFD_CLOEXEC
	if ((ret = fcntl(2, F_DUPFD_CLOEXEC, 3)) < 0) {
		Mlevelfprintf(ERROR, stderr, "unable to dup stderr\n");
		MERO_EXIT(1);
	}
#else
	if ((ret = dup(2)) < 0) {
		Mlevelfprintf(ERROR, stderr, "unable to dup stderr\n");
		MERO_EXIT(1);
	}
	(void) fcntl(ret, F_SETFD, FD_CLOEXEC);
#endif
	oerr = fdopen(ret, "w");
	if (oerr == NULL) {
		Mlevelfprintf(ERROR, stderr, "unable to dup stderr\n");
		MERO_EXIT(1);
	}
	d->input[1].fd = pfd[0];
	dup_err = dup2(pfd[1], 2);
	close(pfd[1]);
	if (dup_err == -1) {
		Mlevelfprintf(ERROR, stderr, "unable to dup stderr\n");
		MERO_EXIT(1);
	}

	/* separate entry for the neighbour discovery service */
	dpdisc = (struct _dpair) {
		.fork_lock = PTHREAD_MUTEX_INITIALIZER,
		.pid = getpid(),
		.type = MERO,
		.dbname = "discovery",
	};
	d = d->next = &dpdisc;
	pthread_mutex_init(&d->fork_lock, NULL);
	if (pipe2(pfd, O_CLOEXEC) == -1) {
		Mlevelfprintf(ERROR, stderr, "unable to create pipe: %s\n",
				strerror(errno));
		MERO_EXIT(1);
	}
#if !defined(HAVE_PIPE2) || O_CLOEXEC == 0
	(void) fcntl(pfd[0], F_SETFD, FD_CLOEXEC);
	(void) fcntl(pfd[1], F_SETFD, FD_CLOEXEC);
#endif
	d->input[0].fd = pfd[0];
	if(!(_mero_discout = fdopen(pfd[1], "a"))) {
		Mlevelfprintf(ERROR, stderr, "unable to open file descriptor: %s\n",
				 strerror(errno));
		MERO_EXIT(1);
	}
	if (pipe2(pfd, O_CLOEXEC) == -1) {
		Mlevelfprintf(ERROR, stderr, "unable to create pipe: %s\n",
				strerror(errno));
		MERO_EXIT(1);
	}
#if !defined(HAVE_PIPE2) || O_CLOEXEC == 0
	(void) fcntl(pfd[0], F_SETFD, FD_CLOEXEC);
	(void) fcntl(pfd[1], F_SETFD, FD_CLOEXEC);
#endif
	d->input[1].fd = pfd[0];
	if(!(_mero_discerr = fdopen(pfd[1], "a"))) {
		Mlevelfprintf(ERROR, stderr, "unable to open file descriptor: %s\n",
				 strerror(errno));
		MERO_EXIT(1);
	}

	/* separate entry for the control runner */
	dpcont = (struct _dpair) {
		.fork_lock = PTHREAD_MUTEX_INITIALIZER,
		.pid = getpid(),
		.type = MERO,
		.dbname = "control",
	};
	d = d->next = &dpcont;
	pthread_mutex_init(&d->fork_lock, NULL);
	if (pipe2(pfd, O_CLOEXEC) == -1) {
		Mlevelfprintf(ERROR, stderr, "unable to create pipe: %s\n",
				strerror(errno));
		MERO_EXIT(1);
	}
#if !defined(HAVE_PIPE2) || O_CLOEXEC == 0
	(void) fcntl(pfd[0], F_SETFD, FD_CLOEXEC);
	(void) fcntl(pfd[1], F_SETFD, FD_CLOEXEC);
#endif
	d->input[0].fd = pfd[0];
	if(!(_mero_ctlout = fdopen(pfd[1], "a"))) {
		Mlevelfprintf(ERROR, stderr, "unable to open file descriptor: %s\n",
				 strerror(errno));
		MERO_EXIT(1);
	}
	if (pipe2(pfd, O_CLOEXEC) == -1) {
		Mlevelfprintf(ERROR, stderr, "unable to create pipe: %s\n",
				strerror(errno));
		MERO_EXIT(1);
	}
#if !defined(HAVE_PIPE2) || O_CLOEXEC == 0
	(void) fcntl(pfd[0], F_SETFD, FD_CLOEXEC);
	(void) fcntl(pfd[1], F_SETFD, FD_CLOEXEC);
#endif
	d->input[1].fd = pfd[0];
	if(!(_mero_ctlerr = fdopen(pfd[1], "a"))) {
		Mlevelfprintf(ERROR, stderr, "unable to open file descriptor: %s\n",
				 strerror(errno));
		MERO_EXIT(1);
	}

	if ((thret = pthread_create(&tid, NULL, logListener, NULL)) != 0) {
		Mlevelfprintf(ERROR, oerr, "%s: FATAL: unable to create logthread: %s\n",
				argv[0], strerror(thret));
		MERO_EXIT(1);
	}

	(void) sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sa.sa_handler = handler;
	if (sigaction(SIGINT, &sa, NULL) == -1 ||
		sigaction(SIGQUIT, &sa, NULL) == -1 ||
		sigaction(SIGTERM, &sa, NULL) == -1) {
		Mlevelfprintf(ERROR, oerr, "%s: FATAL: unable to create signal handlers: %s\n",
				 argv[0], strerror(errno));
		MERO_EXIT(1);
	}

	(void) sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sa.sa_handler = huphandler;
	if (sigaction(SIGHUP, &sa, NULL) == -1) {
		Mlevelfprintf(ERROR, oerr, "%s: FATAL: unable to create signal handlers: %s\n",
				 argv[0], strerror(errno));
		MERO_EXIT(1);
	}

	(void) sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sa.sa_handler = segvhandler;
	if (sigaction(SIGSEGV, &sa, NULL) == -1) {
		Mlevelfprintf(ERROR, oerr, "%s: FATAL: unable to create signal handlers: %s\n",
				 argv[0], strerror(errno));
		MERO_EXIT(1);
	}

	(void) sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sa.sa_handler = SIG_IGN;
	if (sigaction(SIGPIPE, &sa, NULL) == -1) {
		Mlevelfprintf(ERROR, oerr, "%s: FATAL: unable to create signal handlers: %s\n",
				 argv[0], strerror(errno));
		MERO_EXIT(1);
	}

	/* make sure we will be able to write our pid */
	if ((pidfile = fopen(pidfilename, "w")) == NULL) {
		Mlevelfprintf(ERROR, stderr, "unable to open '%s%s%s' for writing: %s\n",
				pidfilename[0] != '/' ? dbfarm : "",
				pidfilename[0] != '/' ? "/" : "",
				pidfilename, strerror(errno));
		MERO_EXIT(1);
	}

	msab_dbfarminit(dbfarm);

	/* write out the pid */
	fprintf(pidfile, "%d\n", (int)d->pid);
	fflush(pidfile);
	fclose(pidfile);

	{
		Mlevelfprintf(INFORMATION, stdout, "Merovingian %s", MONETDB_VERSION);
#ifdef MONETDB_RELEASE
		Mlevelfprintf(INFORMATION, stdout, " (%s)", MONETDB_RELEASE);
#else
		const char *rev = mercurial_revision();
		if (strcmp(rev, "Unknown") != 0)
			Mlevelfprintf(INFORMATION, stdout, " (hg id: %s)", rev);
#endif
		Mlevelfprintf(INFORMATION, stdout, " starting\n");
	}
	Mlevelfprintf(INFORMATION, stdout, "monitoring dbfarm %s\n", dbfarm);

	/* open up connections */
	if ((e = openConnectionIP(socks, false, host, port, stdout)) == NO_ERR &&
		(e = openConnectionUNIX(&socks[2], mapi_usock, 0, stdout)) == NO_ERR &&
		(!discovery || (e = openConnectionIP(discsocks, true, host, port, _mero_discout)) == NO_ERR) &&
		(e = openConnectionUNIX(&unsock, control_usock, S_IRWXO, _mero_ctlout)) == NO_ERR) {
		pthread_t ctid = 0;
		pthread_t dtid = 0;

		if (discovery) {
			_mero_broadcastsock = socket(AF_INET, SOCK_DGRAM
#ifdef SOCK_CLOEXEC
										 | SOCK_CLOEXEC
#endif
										 , 0);
			ret = 1;
			if (_mero_broadcastsock == -1 ||
				setsockopt(_mero_broadcastsock,
						   SOL_SOCKET, SO_BROADCAST, &ret, sizeof(ret)) == -1)
			{
				Mlevelfprintf(ERROR, stderr, "cannot create broadcast package, "
						"discovery services disabled\n");
				if (discsocks[0] >= 0)
					closesocket(discsocks[0]);
				if (discsocks[1] >= 0)
					closesocket(discsocks[1]);
				discsocks[0] = discsocks[1] = -1;
			}
#ifndef SOCK_CLOEXEC
			(void) fcntl(_mero_broadcastsock, F_SETFD, FD_CLOEXEC);
#endif

			_mero_broadcastaddr.sin_family = AF_INET;
			_mero_broadcastaddr.sin_addr.s_addr = htonl(INADDR_BROADCAST);
			/* the target port is our configured port, not elegant, but how
			 * else can we do it? can't broadcast to all ports or something */
			_mero_broadcastaddr.sin_port = htons(port);
		}

		/* Paranoia umask, but good, because why would people have to sniff
		 * our private parts? */
		umask(S_IRWXG | S_IRWXO);

		/* handle control commands */
		if ((thret = pthread_create(&ctid, NULL,
						controlRunner,
						(void *) &unsock)) != 0)
		{
			Mlevelfprintf(ERROR, stderr, "unable to create control command thread: %s\n",
					strerror(thret));
			ctid = 0;
			closesocket(unsock);
			if (discsocks[0] >= 0)
				closesocket(discsocks[0]);
			if (discsocks[1] >= 0)
				closesocket(discsocks[1]);
			MERO_EXIT(1);
		}

		/* start neighbour discovery and notification thread */
		if ((discsocks[0] >= 0 || discsocks[1] >= 0) &&
			(thret = pthread_create(&dtid, NULL,
					discoveryRunner, (void *) discsocks)) != 0)
		{
			Mlevelfprintf(ERROR, stderr, "unable to start neighbour discovery thread: %s\n",
					strerror(thret));
			dtid = 0;
			if (discsocks[0] >= 0)
				closesocket(discsocks[0]);
			if (discsocks[1] >= 0)
				closesocket(discsocks[1]);
		}

		/* From this point merovingian considers itself to be in position to
		 * start running, so flag the parent we will have fun. */
		MERO_EXIT(0);

		// if *not* on merodontfork mode and after notifying the parent
		// that it can terminate make sure that we close and we do not
		// use the original stderr (oerr).  The reason for this is
		// because there is a chance that this is not a pty but a pipe
		// instead. e.g. a Python script might have called the `monetdbd
		// start` command and waits for an EOF at its reading pipes
		if (!merodontfork) {
			if ((ret = fclose(oerr)) != 0) {
				// remember stderr is send to the log through the logger
				Mlevelfprintf(ERROR, stderr, "unable to close stderr: %s\n", strerror(ret));
			}
			// in any casy oerr is either closed or EBADF
			oerr = NULL;
		}

		/* handle external connections main loop */
		e = acceptConnections(socks);

		/* wait for the control runner and discovery thread to have
		 * finished announcing they're going down */
		if (ctid != 0)
			pthread_join(ctid, NULL);
		if (dtid != 0)
			pthread_join(dtid, NULL);
	}

	/* control channel is already closed at this point */
	if (unsock != -1 && remove(control_usock) != 0)
		Mlevelfprintf(ERROR, stderr, "unable to remove control socket '%s': %s\n",
				control_usock, strerror(errno));
	if (socks[2] != -1 && remove(mapi_usock) != 0)
		Mlevelfprintf(ERROR, stderr, "unable to remove mapi socket '%s': %s\n",
				mapi_usock, strerror(errno));

	if (e != NO_ERR) {
		/* console */
		if (oerr)
			Mlevelfprintf(ERROR, oerr, "%s: %s\n", argv[0], e);
		/* logfile */
		Mlevelfprintf(ERROR, stderr, "%s\n", e);
		MERO_EXIT(1);
	}

shutdown:
	/* stop started mservers */

	kv = findConfKey(ckv, "exittimeout");
	if (d->next != NULL && atoi(kv->val) != 0) {
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
						doTerminateProcess, t)) != 0)
			{
				Mlevelfprintf(ERROR, stderr, "%s: unable to create thread to terminate "
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
				Mlevelfprintf(ERROR, stderr, "failed to wait for termination thread: "
						"%s\n", strerror(argp));
			}
			tl = tlw->next;
			free(tlw);
			tlw = tl;
		}
	}

	/* need to do this here, since the logging thread is shut down as next thing */
	Mlevelfprintf(INFORMATION, stdout, "Merovingian %s stopped\n", MONETDB_VERSION);

	_mero_keep_logging = 0;
	if (tid != 0 && (argp = pthread_join(tid, NULL)) != 0) {
		// if we are still connecting to the stderr we can print that.
		// it does not make sense to go to the logging thread since it
		// is almost sure gone if we fail to join it.
		if (oerr)
			Mlevelfprintf(ERROR, oerr, "failed to wait for logging thread: %s\n", strerror(argp));
	}

	if (_mero_topdp != NULL) {
		close(_mero_topdp->input[0].fd);
		if (_mero_topdp->input[0].fd != _mero_topdp->input[1].fd)
			close(_mero_topdp->input[1].fd);
	}

	/* remove files that suggest our existence */
	if (pidfilename != NULL) {
		remove(pidfilename);
	}

	/* mostly for valgrind... */
	freeConfFile(ckv);
	if (_mero_db_props != NULL) {
		freeConfFile(_mero_db_props);
		free(_mero_db_props);
	}

	if (lockfd >= 0) {
		MT_lockf(".merovingian_lock", F_ULOCK);
		close(lockfd);
	}

	/* the child's return code at this point doesn't matter, as no one
	 * will see it */
	return(0);
}
