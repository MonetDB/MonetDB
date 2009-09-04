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
 * monetdb
 * Fabian Groffen
 * MonetDB Database Administrator's Toolkit
 *
 * A group of MonetDB servers in a dbfarm can be under control of
 * Merovingian, a daemon which by itself does not allow any user
 * interaction.  The monetdb utility is designed to be the interface for
 * the DBA to the dbfarm.  Creating or deleting databases next to
 * retrieving status information about them are the primary goals of
 * this tool.
 */

#define TOOLKIT_VERSION   "0.5"

#include "sql_config.h"
#include "mal_sabaoth.h"
#include "utils.h"
#include "properties.h"
#include "glob.h"
#include <stdlib.h> /* exit, getenv */
#include <stdarg.h>	/* variadic stuff */
#include <stdio.h> /* fprintf, rename */
#include <string.h> /* strerror */
#include <sys/stat.h> /* mkdir, stat, umask */
#include <sys/types.h> /* mkdir, readdir */
#include <dirent.h> /* readdir */
#include <unistd.h> /* stat, rmdir, unlink, ioctl */
#include <time.h> /* strftime */
#include <sys/socket.h> /* socket */
#ifdef HAVE_SYS_UN_H
#include <sys/un.h> /* sockaddr_un */
#endif
#ifdef HAVE_STROPTS_H
#include <stropts.h> /* ioctl */
#endif
#ifdef HAVE_SYS_IOCTL_H
#include <sys/ioctl.h>
#endif
#ifdef HAVE_TERMIOS_H
#include <termios.h> /* TIOCGWINSZ/TIOCSWINSZ */
#endif
#ifdef HAVE_ALLOCA_H
#include <alloca.h>
#endif
#include <errno.h>

#define SOCKPTR struct sockaddr *

typedef char* err;

#define freeErr(X) GDKfree(X)
#define getErrMsg(X) X
#define NO_ERR (err)0

static str dbfarm = NULL;
static int mero_running = 0;
static int TERMWIDTH = 80;  /* default to classic terminal width */

static void
command_help(int argc, char *argv[])
{
	if (argc < 2) {
		printf("Usage: monetdb command [command-options-and-arguments]\n");
		printf("  where command is one of:\n");
		printf("    create, destroy, lock, release\n");
		printf("    status, start, stop, kill\n");
		printf("    set, get, inherit\n");
		printf("    discover, help, version\n");
		printf("  use the help command to get help for a particular command\n");
	} else if (strcmp(argv[1], "create") == 0) {
		printf("Usage: monetdb create [-l] database [database ...]\n");
		printf("  Initialises a new database in the MonetDB Server.  A\n");
		printf("  database created with this command makes it available\n");
		printf("  for use.\n");
		printf("Options:\n");
		printf("  -l  put the database in maintenance mode after creation\n");
	} else if (strcmp(argv[1], "destroy") == 0) {
		printf("Usage: monetdb destroy [-f] database [database ...]\n");
		printf("  Removes the given database, including all its data and\n");
		printf("  logfiles.  Once destroy has completed, all data is lost.\n");
		printf("  Be careful when using this command.\n");
		printf("Options:\n");
		printf("  -f  do not ask for confirmation, destroy right away\n");
	} else if (strcmp(argv[1], "lock") == 0) {
		printf("Usage: monetdb lock database [database ...]\n");
		printf("  Puts the given database in maintenance mode.  A database\n");
		printf("  under maintenance can only be connected to by the DBA.\n");
		printf("  A database which is under maintenance is not started\n");
		printf("  automatically.  Use the \"release\" command to bring\n");
		printf("  the database back for normal usage.\n");
	} else if (strcmp(argv[1], "release") == 0) {
		printf("Usage: monetdb release database [database ...]\n");
		printf("  Brings back a database from maintenance mode.  A released\n");
		printf("  database is available again for normal use.  Use the\n");
		printf("  \"lock\" command to take a database under maintenance.\n");
	} else if (strcmp(argv[1], "status") == 0) {
		printf("Usage: monetdb status [-lc] [expression ...]\n");
		printf("  Shows the state of a given glob-style database match, or\n");
		printf("  all known if none given.  Instead of the normal mode, a\n");
		printf("  long and crash mode control what information is displayed.\n");
		printf("Options:\n");
		printf("  -l  extended information listing\n");
		printf("  -c  crash statistics listing\n");
		printf("  -s  only show databases matching a state, combination\n");
		printf("      possible from r (running), s (stopped), c (crashed)\n");
		printf("      and l (locked).\n");
	} else if (strcmp(argv[1], "start") == 0) {
		printf("Usage: monetdb start [-a] database [database ...]\n");
		printf("  Starts the given database, if the MonetDB Database Server\n");
		printf("  is running.\n");
		printf("Options:\n");
		printf("  -a  start all known databases\n");
	} else if (strcmp(argv[1], "stop") == 0) {
		printf("Usage: monetdb stop [-a] database [database ...]\n");
		printf("  Stops the given database, if the MonetDB Database Server\n");
		printf("  is running.\n");
		printf("Options:\n");
		printf("  -a  stop all known databases\n");
	} else if (strcmp(argv[1], "kill") == 0) {
		printf("Usage: monetdb kill [-a] database [database ...]\n");
		printf("  Kills the given database, if the MonetDB Database Server\n");
		printf("  is running.  Note: killing a database should only be done\n");
		printf("  as last resort to stop a database.  A database being\n");
		printf("  killed may end up with data loss.\n");
		printf("Options:\n");
		printf("  -a  kill all known databases\n");
	} else if (strcmp(argv[1], "set") == 0) {
		printf("Usage: monetdb set property=value database [database ...]\n");
		printf("  sets property to value for the given database\n");
		printf("  for a list of properties, use `monetdb get all`\n");
	} else if (strcmp(argv[1], "get") == 0) {
		printf("Usage: monetdb get <\"all\" | property,...> [database ...]\n");
		printf("  gets value for property for the given database, or\n");
		printf("  retrieves all properties for the given database\n");
	} else if (strcmp(argv[1], "inherit") == 0) {
		printf("Usage: monetdb inherit property database [database ...]\n");
		printf("  unsets property, reverting to its inherited value from\n");
		printf("  the default configuration for the given database\n");
	} else if (strcmp(argv[1], "discover") == 0) {
		printf("Usage: monetdb discover [expression]\n");
		printf("  Lists the remote databases discovered by the MonetDB\n");
		printf("  Database Server.  Databases in this list can be connected\n");
		printf("  to as well.  If expression is given, all entries are\n");
		printf("  matched against a limited glob-style expression.\n");
	} else if (strcmp(argv[1], "help") == 0) {
		printf("Yeah , help on help, how desparate can you be? ;)\n");
	} else if (strcmp(argv[1], "version") == 0) {
		printf("Usage: monetdb version\n");
		printf("  prints the version of this monetdb utility\n");
	} else {
		printf("help: unknown command: %s\n", argv[1]);
	}
}

static void
command_version()
{
	printf("MonetDB Database Server Toolkit v%s\n", TOOLKIT_VERSION);
}

static void
printStatus(sabdb *stats, int mode, int twidth)
{
	sabuplog uplog;
	str e;

	if ((e = SABAOTHgetUplogInfo(&uplog, stats)) != MAL_SUCCEED) {
		fprintf(stderr, "status: internal error: %s\n", e);
		GDKfree(e);
		return;
	}

	if (mode == 1) {
		/* short one-line (default) mode */
		char *state;
		char uptime[12];
		char avg[8];
		char *crash;
		char *dbname;

		switch (stats->state) {
			case SABdbRunning:
				state = "running";
			break;
			case SABdbCrashed:
				state = "crashed";
			break;
			case SABdbInactive:
				state = "stopped";
			break;
			default:
				state = "unknown";
			break;
		}
		/* override if locked for brevity */
		if (stats->locked == 1)
			state = "locked ";

		if (uplog.lastcrash == -1) {
			crash = "-";
		} else {
			struct tm *t;
			crash = alloca(sizeof(char) * 20);
			t = localtime(&uplog.lastcrash);
			strftime(crash, 20, "%Y-%m-%d %H:%M:%S", t);
		}

		if (stats->state != SABdbRunning) {
			uptime[0] = '\0';
		} else {
			secondsToString(uptime, time(NULL) - uplog.laststart, 3);
		}

		/* cut too long database names */
		dbname = alloca(sizeof(char) * (twidth + 1));
		abbreviateString(dbname, stats->dbname, twidth);
		/* dbname | state | uptime | health */
		secondsToString(avg, uplog.avguptime, 1);
			printf("%-*s  %s %12s", twidth,
					dbname, state, uptime);
		if (uplog.startcntr)
			printf("  %3d%%, %3s  %s",
					100 - (uplog.crashcntr * 100 / uplog.startcntr),
					avg, crash);
		printf("\n");
	} else if (mode == 2) {
		/* long mode */
		char *state;
		sablist *entry;
		char up[32];
		struct tm *t;

		switch (stats->state) {
			case SABdbRunning:
				state = "running";
			break;
			case SABdbCrashed:
				state = "crashed";
			break;
			case SABdbInactive:
				state = "stopped";
			break;
			default:
				state = "unknown";
			break;
		}

		printf("%s:\n", stats->dbname);
		printf("  location: %s\n", stats->path);
		printf("  database name: %s\n", stats->dbname);
		printf("  state: %s\n", state);
		printf("  locked: %s\n", stats->locked == 1 ? "yes" : "no");
		entry = stats->scens;
		printf("  scenarios:");
		if (entry == NULL) {
			printf(" (none)");
		} else while (entry != NULL) {
			printf(" %s", entry->val);
			entry = entry->next;
		}
		printf("\n");
		entry = stats->conns;
		printf("  connections:");
		if (entry == NULL) {
			printf(" (none)");
		} else while (entry != NULL) {
			printf(" %s", entry->val);
			entry = entry->next;
		}
		printf("\n");
		printf("  start count: %d\n  stop count: %d\n  crash count: %d\n",
				uplog.startcntr, uplog.stopcntr, uplog.crashcntr);
		if (stats->state == SABdbRunning) {
			secondsToString(up, time(NULL) - uplog.laststart, 999);
			printf("  current uptime: %s\n", up);
		}
		secondsToString(up, uplog.avguptime, 999);
		printf("  average uptime: %s\n", up);
		secondsToString(up, uplog.maxuptime, 999);
		printf("  maximum uptime: %s\n", up);
		secondsToString(up, uplog.minuptime, 999);
		printf("  minimum uptime: %s\n", up);
		if (uplog.lastcrash != -1) {
			t = localtime(&uplog.lastcrash);
			strftime(up, 32, "%Y-%m-%d %H:%M:%S", t);
		} else {
			sprintf(up, "(unknown)");
		}
		printf("  last crash: %s\n", up);
		if (uplog.laststart != -1) {
			t = localtime(&uplog.laststart);
			strftime(up, 32, "%Y-%m-%d %H:%M:%S", t);
		} else {
			sprintf(up, "(unknown)");
		}
		printf("  last start: %s\n", up);
		printf("  average of crashes in the last start attempt: %d\n",
				uplog.crashavg1);
		printf("  average of crashes in the last 10 start attempts: %.2f\n",
				uplog.crashavg10);
		printf("  average of crashes in the last 30 start attempts: %.2f\n",
				uplog.crashavg30);
	} else {
		/* this shows most used properties, and is shown also for modes
		 * that are added but we don't understand (yet) */
		char buf[64];
		char min[8], avg[8], max[8];
		struct tm *t;
		/* dbname, status -- since, crash averages */

		switch (stats->state) {
			case SABdbRunning: {
				char up[32];
				t = localtime(&uplog.laststart);
				strftime(buf, 64, "up since %Y-%m-%d %H:%M:%S, ", t);
				secondsToString(up, time(NULL) - uplog.laststart, 999);
				strcat(buf, up);
			} break;
			case SABdbCrashed:
				t = localtime(&uplog.lastcrash);
				strftime(buf, 64, "crashed on %Y-%m-%d %H:%M:%S", t);
			break;
			case SABdbInactive:
				snprintf(buf, 64, "not running");
			break;
			default:
				snprintf(buf, 64, "unknown");
			break;
		}
		if (stats->locked == 1)
			strcat(buf, ", locked");
		printf("database %s, %s\n", stats->dbname, buf);
		printf("  crash average: %d.00 %.2f %.2f (over 1, 15, 30 starts) "
				"in total %d crashes\n",
				uplog.crashavg1, uplog.crashavg10, uplog.crashavg30,
				uplog.crashcntr);
		secondsToString(min, uplog.minuptime, 1);
		secondsToString(avg, uplog.avguptime, 1);
		secondsToString(max, uplog.maxuptime, 1);
		printf("  uptime stats (min/avg/max): %s/%s/%s over %d runs\n",
				min, avg, max, uplog.stopcntr);
	}
}

static void
command_status(int argc, char *argv[])
{
	int doall = 1; /* we default to showing all */
	int mode = 1;  /* 0=crash, 1=short, 2=long */
	char *state = "rscl"; /* contains states to show */
	int i;
	char *p;
	str e;
	sabdb *stats;
	sabdb *orig;
	int t;
	int dbwidth = 0;
	int twidth = TERMWIDTH;

	if (argc == 0) {
		exit(2);
	}

	/* time to collect some option flags */
	for (i = 1; i < argc; i++) {
		if (argv[i][0] == '-') {
			for (p = argv[i] + 1; *p != '\0'; p++) {
				switch (*p) {
					case 'c':
						mode = 0;
					break;
					case 'l':
						mode = 2;
					break;
					case 's':
						if (*(p + 1) != '\0') {
							state = ++p;
						} else if (i + 1 < argc && argv[i + 1][0] != '-') {
							state = argv[++i];
						} else {
							fprintf(stderr, "status: -s needs an argument\n");
							command_help(2, &argv[-1]);
							exit(1);
						}
						for (p = state; *p != '\0'; p++) {
							switch (*p) {
								case 'r': /* running (started) */
								case 's': /* stopped */
								case 'c': /* crashed */
								case 'l': /* locked */
								break;
								default:
									fprintf(stderr, "status: unknown flag for -s: -%c\n", *p);
									command_help(2, &argv[-1]);
									exit(1);
								break;
							}
						}
						p--;
					break;
					case '-':
						if (p[1] == '\0') {
							if (argc - 1 > i) 
								doall = 0;
							i = argc;
							break;
						}
					default:
						fprintf(stderr, "status: unknown option: -%c\n", *p);
						command_help(2, &argv[-1]);
						exit(1);
					break;
				}
			}
			/* make this option no longer available, for easy use
			 * lateron */
			argv[i] = NULL;
		} else {
			doall = 0;
		}
	}

	if ((e = SABAOTHgetStatus(&orig, NULL)) != MAL_SUCCEED) {
		fprintf(stderr, "status: internal error: %s\n", e);
		GDKfree(e);
		exit(2);
	}
	/* don't even look at the arguments, if we are instructed
	 * to list all known databases */
	if (doall != 1) {
		sabdb *w = NULL;
		sabdb *top = w;
		sabdb *prev;
		for (i = 1; i < argc; i++) {
			t = 0;
			if (argv[i] != NULL) {
				prev = NULL;
				for (stats = orig; stats != NULL; stats = stats->next) {
					if (glob(argv[i], stats->dbname)) {
						t = 1;
						/* move out of orig into w, such that we can't
						 * get double matches in the same output list
						 * (as side effect also avoids a double free
						 * lateron) */
						if (w == NULL) {
							top = w = stats;
						} else {
							w = w->next = stats;
						}
						if (prev == NULL) {
							orig = stats->next;
							/* little hack to revisit the now top of the
							 * list */
							w->next = orig;
							stats = w;
							continue;
						} else {
							prev->next = stats->next;
							stats = prev;
						}
					}
					prev = stats;
				}
				if (w != NULL)
					w->next = NULL;
				if (t == 0) {
					fprintf(stderr, "status: no such database: %s\n", argv[i]);
					argv[i] = NULL;
				}
			}
		}
		SABAOTHfreeStatus(&orig);
		orig = top;
	}
	/* calculate width, BUG: SABdbState selection is only done at
	 * printing */
	for (stats = orig; stats != NULL; stats = stats->next) {
		if ((t = strlen(stats->dbname)) > dbwidth)
			dbwidth = t;
	}

	if (mode == 1 && orig != NULL) {
		/* print header for short mode, state -- last crash = 54 chars */
		twidth -= 54;
		if (twidth < 6)
			twidth = 6;
		if (dbwidth < 14)
			dbwidth = 14;
		if (dbwidth < twidth)
			twidth = dbwidth;
		printf("%*sname%*s  ",
				twidth - 4 /* name */ - ((twidth - 4) / 2), "",
				(twidth - 4) / 2, "");
		printf(" state     uptime       health       last crash\n");
	}

	for (p = state; *p != '\0'; p++) {
		int curLock = 0;
		SABdbState curMode = SABdbIllegal;
		switch (*p) {
			case 'r':
				curMode = SABdbRunning;
			break;
			case 's':
				curMode = SABdbInactive;
			break;
			case 'c':
				curMode = SABdbCrashed;
			break;
			case 'l':
				curLock = 1;
			break;
		}
		stats = orig;
		while (stats != NULL) {
			if (stats->locked == curLock &&
					(curLock == 1 || 
					 (curLock == 0 && stats->state == curMode)))
				printStatus(stats, mode, twidth);
			stats = stats->next;
		}
	}

	if (orig != NULL)
		SABAOTHfreeStatus(&orig);
}

static void
command_discover(int argc, char *argv[])
{
	char path[8096];
	int sock = -1;
	struct sockaddr_un server;
	char buf[256];
	char *p, *q;
	int len;
	int pos;
	size_t twidth = TERMWIDTH;
	char location[twidth + 1];
	char *match = NULL;
	size_t numlocs = 50;
	size_t posloc = 0;
	size_t loclen = 0;
	char **locations = malloc(sizeof(char*) * numlocs);

	/* if Merovingian isn't running, there's not much we can do */
	if (mero_running == 0) {
		fprintf(stderr, "discover: cannot perform: MonetDB Database Server "
				"(merovingian) is not running\n");
		exit(1);
	}

	if (argc == 0) {
		exit(2);
	} else if (argc > 2) {
		/* print help message for this command */
		command_help(2, &argv[-1]);
		exit(1);
	} else if (argc == 2) {
		match = argv[1];
	}

	snprintf(path, 8095, "%s/.merovingian_control", dbfarm);
	path[8095] = '\0';

	if ((sock = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
		fprintf(stderr, "discover: cannot open connection: %s\n",
				strerror(errno));
		exit(2);
	}
	memset(&server, 0, sizeof(struct sockaddr_un));
	server.sun_family = AF_UNIX;
	strncpy(server.sun_path, path, sizeof(server.sun_path) - 1);
	if (connect(sock, (SOCKPTR) &server, sizeof(struct sockaddr_un))) {
		fprintf(stderr, "discover: cannot connect: %s\n", strerror(errno));
		exit(2);
	}

 	/* Send the pass phrase to unlock the information available in
	 * merovingian.  Anelosimus eximius is a social species of spiders,
	 * which help each other, just like merovingians do among each
	 * other. */
	len = snprintf(buf, sizeof(buf), "anelosimus eximius\n");
	send(sock, buf, len, 0);
	pos = 0;
	len = 0;
	buf[len] = '\0';
	do {
		if ((p = strchr(buf, '\n')) == NULL) {
			if (len == sizeof(buf) - 1) {
				/* no newline in this block, too large, warn and discard */
				printf("discover: WARNING: discarding (too long?) line: %s\n",
						buf);
				pos = 0;
			} else {
				pos = len;
			}
			len = recv(sock, buf + pos, sizeof(buf) - 1 - pos, 0);
			/* if there is no more... */
			if (len == 0) {
				break;
			} else if (len < 0) {
				fprintf(stderr, "discover: error while reading "
						"from merovingian\n");
				exit(2);
			}
			len += pos;
			buf[len] = '\0';
			continue;
		}
		*p++ = '\0';

		if ((q = strchr(buf, '\t')) == NULL) {
			/* doesn't look correct */
			printf("discover: WARNING: discarding incorrect line: %s\n", buf);
			len -= p - buf;
			memmove(buf, p, len + 1 /* include \0 */);
			continue;
		}
		*q++ = '\0';

		snprintf(path, sizeof(path), "%s%s", q, buf);

		if (match == NULL || glob(match, path)) {
			/* cut too long location name */
			abbreviateString(location, path, twidth);
			/* store what we found */
			if (posloc == numlocs)
				locations = realloc(locations,
						sizeof(char) * (numlocs = numlocs * 2));
			locations[posloc++] = strdup(location);
			if (strlen(location) > loclen)
				loclen = strlen(location);
		}

		/* move it away */
		len -= p - buf;
		memmove(buf, p, len + 1 /* include \0 */);
	} while (1);

	close(sock);

	if (posloc > 0) {
		printf("%*slocation\n",
				(int)(loclen - 8 /* "location" */ - ((loclen - 8) / 2)), "");
		/* could qsort the array here but we don't :P */
		for (loclen = 0; loclen < posloc; loclen++) {
			printf("%s\n", locations[loclen]);
			free(locations[loclen]);
		}
	}

	free(locations);
}

typedef enum {
	START = 0,
	STOP,
	KILL,
	SHARE
} merocom;

static void
command_merocom(int argc, char *argv[], merocom mode)
{
	int doall = 0;
	char path[8096];
	int sock = -1;
	struct sockaddr_un server;
	char buf[256];
	int i;
	err e;
	sabdb *orig;
	sabdb *stats;
	char *type = NULL;
	char *p;
	int ret = 0;
	int len;

	snprintf(path, 8095, "%s/.merovingian_control", dbfarm);
	path[8095] = '\0';

	switch (mode) {
		case START:
			type = "start";
		break;
		case STOP:
			type = "stop";
		break;
		case KILL:
			type = "kill";
		break;
		case SHARE:
			type = "share";
		break;
	}

	if (argc == 1) {
		/* print help message for this command */
		command_help(2, &argv[-1]);
		exit(1);
	} else if (argc == 0) {
		exit(2);
	}

	/* time to collect some option flags */
	for (i = 1; i < argc; i++) {
		if (argv[i][0] == '-') {
			for (p = argv[i] + 1; *p != '\0'; p++) {
				switch (*p) {
					case 'a':
						doall = 1;
					break;
					case '-':
						if (p[1] == '\0') {
							if (argc - 1 > i) 
								doall = 0;
							i = argc;
							break;
						}
					default:
						fprintf(stderr, "%s: unknown option: -%c\n", type, *p);
						command_help(2, &argv[-1]);
						exit(1);
					break;
				}
			}
			/* make this option no longer available, for easy use
			 * lateron */
			argv[i] = NULL;
		}
	}

	/* if Merovingian isn't running, there's not much we can do */
	if (mero_running == 0) {
		fprintf(stderr, "%s: cannot perform: MonetDB Database Server "
				"(merovingian) is not running\n", type);
		exit(1);
	}

	if ((sock = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
		fprintf(stderr, "%s: cannot open connection: %s\n",
				type, strerror(errno));
		exit(2);
	}
	memset(&server, 0, sizeof(struct sockaddr_un));
	server.sun_family = AF_UNIX;
	strncpy(server.sun_path, path, sizeof(server.sun_path) - 1);
	if (connect(sock, (SOCKPTR) &server, sizeof(struct sockaddr_un))) {
		fprintf(stderr, "%s: cannot connect: %s\n",
				type, strerror(errno));
		exit(2);
	}

	if (doall == 1) {
		/* don't even look at the arguments, because we are instructed
		 * to list all known databases */
		if ((e = SABAOTHgetStatus(&orig, NULL)) != MAL_SUCCEED) {
			fprintf(stderr, "%s: internal error: %s\n", type, e);
			GDKfree(e);
			exit(2);
		}
	} else {
		sabdb *w = NULL;
		orig = NULL;
		for (i = mode == SHARE ? 2 : 1; i < argc; i++) {
			if (argv[i] != NULL) {
				if ((e = SABAOTHgetStatus(&stats, argv[i])) != MAL_SUCCEED) {
					fprintf(stderr, "%s: internal error: %s\n", type, e);
					GDKfree(e);
					exit(2);
				}

				if (stats == NULL) {
					fprintf(stderr, "%s: no such database: %s\n", type, argv[i]);
					argv[i] = NULL;
				} else {
					if (orig == NULL) {
						orig = stats;
						w = stats;
					} else {
						w = w->next = stats;
					}
				}
			}
		}
	}
	
	stats = orig;
	while (stats != NULL) {
		if (mode == STOP || mode == KILL) {
			if (stats->state == SABdbRunning) {
				printf("%s%sing database '%s'... ", type, mode == STOP ? "p" : "", stats->dbname);
				fflush(stdout);
				len = snprintf(buf, sizeof(buf),
						"%s %s\n", stats->dbname, type);
				send(sock, buf, len, 0);
				if ((len = recv(sock, buf, sizeof(buf), 0)) <= 0) {
					fprintf(stderr, "\n%s: no response from merovingian\n",
							type);
					exit(2);
				}
				buf[len] = '\0';
				if (strcmp(buf, "OK\n") == 0) {
					printf("done\n");
				} else {
					printf("FAILED:\n%s", buf);
					ret = 1;
				}
			} else if (doall != 1) {
				printf("%s: database is not running: %s\n", type, stats->dbname);
			}
		} else if (mode == START) {
			if (stats->state != SABdbRunning) {
				printf("starting database '%s'... ", stats->dbname);
				fflush(stdout);
				len = snprintf(buf, sizeof(buf),
						"%s %s\n", stats->dbname, type);
				send(sock, buf, len, 0);
				if ((len = recv(sock, buf, sizeof(buf), 0)) <= 0) {
					fprintf(stderr, "\n%s: no response from merovingian\n",
							type);
					exit(2);
				}
				buf[len] = '\0';
				if (strcmp(buf, "OK\n") == 0) {
					printf("done\n");
				} else {
					printf("FAILED:\n%s", buf);
					ret = 1;
				}
			} else if (doall != 1 && stats->state == SABdbRunning) {
				printf("%s: database is already running: %s\n",
						type, stats->dbname);
			}
		} else if (mode == SHARE) {
			char *value = argv[1];

			/* stay quiet, we're part of monetdb set property=value */
			len = snprintf(buf, sizeof(buf),
					"%s share=%s\n", stats->dbname, value);
			send(sock, buf, len, 0);
			if ((len = recv(sock, buf, sizeof(buf), 0)) <= 0) {
				fprintf(stderr, "\n%s: no response from merovingian\n",
						type);
				exit(2);
			}
			buf[len] = '\0';
			if (strcmp(buf, "OK\n") != 0) {
				printf("FAILED:\n%s", buf);
				ret = 1;
			}
		}
		stats = stats->next;
	}

	close(sock);

	if (orig != NULL)
		SABAOTHfreeStatus(&orig);

	exit(ret);
}

typedef enum {
	SET = 0,
	INHERIT
} meroset;

static void
command_set(int argc, char *argv[], meroset type)
{
	char *p;
	char *property = NULL, *value = NULL;
	err e;
	int i;
	int state = 0;
	sabdb *orig, *stats, *w;
	confkeyval *kv, *props = getDefaultProps();

	if (argc >= 1 && argc <= 2) {
		/* print help message for this command */
		command_help(2, &argv[-1]);
		exit(1);
	} else if (argc == 0) {
		exit(2);
	}

	/* time to collect some option flags */
	for (i = 1; i < argc; i++) {
		if (argv[i][0] == '-') {
			for (p = argv[i] + 1; *p != '\0'; p++) {
				switch (*p) {
					case '-':
						if (p[1] == '\0') {
							i = argc;
							break;
						}
					default:
						fprintf(stderr, "%s: unknown option: -%c\n",
								argv[0], *p);
						command_help(2, &argv[-1]);
						exit(1);
					break;
				}
			}
			/* make this option no longer available, for easy use
			 * lateron */
			argv[i] = NULL;
		} else if (property == NULL) {
			/* first non-option is property, rest is database */
			property = argv[i];
			argv[i] = NULL;
			if (type == SET) {
				if ((p = strchr(property, '=')) == NULL) {
					fprintf(stderr, "set: need property=value\n");
					command_help(2, &argv[-1]);
					exit(1);
				}
				*p++ = '\0';
				value = p;
			}
		}
	}

	if (property == NULL) {
		fprintf(stderr, "%s: need a property argument\n", argv[0]);
		command_help(2, &argv[0]);
		exit(1);
	}

	if (strcmp(property, "shared") == 0) {
		/* mess around with first argument (property) to become value
		 * only, such that can see if it had an argument or not lateron */
		if (type == INHERIT) {
			argv[1] = property;
			property[0] = '\0';
		} else {
			argv[1] = value;
			/* check if tag matches [A-Za-z0-9./]+ */
			while (*value != '\0') {
				if (!(
						(*value >= 'A' && *value <= 'Z') ||
						(*value >= 'a' && *value <= 'z') ||
						(*value >= '0' && *value <= '9') ||
						(*value == '.' || *value == '/')
				   ))
				{
					fprintf(stderr, "set: invalid character '%c' at %d "
							"in tag name '%s'\n",
							*value, (int)(value - argv[1]), argv[1]);
					exit(1);
				}
				value++;
			}
		}
		return(command_merocom(argc, &argv[0], SHARE));
	}

	w = NULL;
	orig = NULL;
	for (i = 1; i < argc; i++) {
		if (argv[i] != NULL) {
			if ((e = SABAOTHgetStatus(&stats, argv[i])) != MAL_SUCCEED) {
				fprintf(stderr, "%s: internal error: %s\n", argv[0], e);
				GDKfree(e);
				exit(2);
			}

			if (stats == NULL) {
				fprintf(stderr, "%s: no such database: %s\n",
						argv[0], argv[i]);
				argv[i] = NULL;
			} else {
				if (stats->state == SABdbRunning) {
					fprintf(stderr, "%s: database '%s' is still running, "
							"stop database first\n", argv[0], stats->dbname);
					SABAOTHfreeStatus(&stats);
					state |= 1;
					continue;
				}
				if (orig == NULL) {
					orig = stats;
					w = stats;
				} else {
					w = w->next = stats;
				}
			}
		}
	}

	for (stats = orig; stats != NULL; stats = stats->next) {
		if (strcmp(property, "name") == 0) {
			char new[512];

			/* special virtual case */
			if (type == INHERIT) {
				fprintf(stderr, "inherit: cannot default to a database name\n");
				state |= 1;
				continue;
			}

			/* check if dbname matches [A-Za-z0-9-_]+ */
			for (i = 0; value[i] != '\0'; i++) {
				if (
						!(value[i] >= 'A' && value[i] <= 'Z') &&
						!(value[i] >= 'a' && value[i] <= 'z') &&
						!(value[i] >= '0' && value[i] <= '9') &&
						!(value[i] == '-') &&
						!(value[i] == '_')
				   )
				{
					fprintf(stderr, "set: invalid character '%c' at %d "
							"in database name '%s'\n",
							value[i], i, value);
					value[0] = '\0';
					state |= 1;
					break;
				}
			}
			if (value[0] == '\0')
				continue;

			/* construct path to new database */
			snprintf(new, 512, "%s", stats->path);
			p = strrchr(new, '/');
			if (p == NULL) {
				fprintf(stderr, "set: non-absolute database path? '%s'\n",
						stats->path);
				state |= 1;
				continue;
			}
			snprintf(p + 1, 512 - (p + 1 - new), "%s", value);

			/* Renaming is as simple as changing the directory name.
			 * Since the logdir is relative to it, we don't need to
			 * bother about that either. */
			if (rename(stats->path, new) != 0) {
				fprintf(stderr, "%s: failed to rename database from "
						"'%s' to '%s': %s\n", argv[0], stats->path, new,
						strerror(errno));
				state |= 1;
			}
		} else {
			char *err;
			readProps(props, stats->path);
			kv = findConfKey(props, property);
			if (kv == NULL) {
				fprintf(stderr, "%s: no such property: %s\n", argv[0], property);
				state |= 1;
				continue;
			}
			if ((err = setConfVal(kv, type == SET ? value : NULL)) != NULL) {
				fprintf(stderr, "%s: %s\n", argv[0], err);
				GDKfree(err);
				state |= 1;
				continue;
			}

			if (strcmp(kv->key, "forward") == 0 && kv->val != NULL) {
				if (strcmp(kv->val, "proxy") != 0 &&
						strcmp(kv->val, "redirect") != 0)
				{
					fprintf(stderr, "%s: expected 'proxy' or 'redirect' for "
							"property 'forward', got: %s\n", argv[0], kv->val);
					state |= 1;
					continue;
				}
			}

			writeProps(props, stats->path);
			freeConfFile(props);
		}
	}

	if (orig != NULL)
		SABAOTHfreeStatus(&orig);
	GDKfree(props);
	exit(state);
}

static void
command_get(int argc, char *argv[], confkeyval *defprops)
{
	char doall = 1;
	char *p;
	char *property = NULL;
	err e;
	int i;
	sabdb *orig, *stats;
	int twidth = TERMWIDTH;
	char *source, *value;
	confkeyval *kv, *props = getDefaultProps();

	if (argc == 1) {
		/* print help message for this command */
		command_help(2, &argv[-1]);
		exit(1);
	} else if (argc == 0) {
		exit(2);
	}

	/* time to collect some option flags */
	for (i = 1; i < argc; i++) {
		if (argv[i][0] == '-') {
			for (p = argv[i] + 1; *p != '\0'; p++) {
				switch (*p) {
					case '-':
						if (p[1] == '\0') {
							if (argc - 1 > i) 
								doall = 0;
							i = argc;
							break;
						}
					default:
						fprintf(stderr, "get: unknown option: -%c\n", *p);
						command_help(2, &argv[-1]);
						exit(1);
					break;
				}
			}
			/* make this option no longer available, for easy use
			 * lateron */
			argv[i] = NULL;
		} else if (property == NULL) {
			/* first non-option is property, rest is database */
			property = argv[i];
			argv[i] = NULL;
			if (strcmp(property, "all") == 0) {
				/* die hard leak (can't use constant, strtok modifies
				 * (and hence crashes)) */
				property = GDKstrdup("name,forward,shared,nthreads");
			}
		} else {
			doall = 0;
		}
	}

	if (property == NULL) {
		fprintf(stderr, "get: need a property argument\n");
		command_help(2, &argv[-1]);
		exit(1);
	}

	if (doall == 1) {
		/* don't even look at the arguments, because we are instructed
		 * to list all known databases */
		if ((e = SABAOTHgetStatus(&orig, NULL)) != MAL_SUCCEED) {
			fprintf(stderr, "get: internal error: %s\n", e);
			GDKfree(e);
			exit(2);
		}
	} else {
		sabdb *w = NULL;
		orig = NULL;
		for (i = 1; i < argc; i++) {
			if (argv[i] != NULL) {
				if ((e = SABAOTHgetStatus(&stats, argv[i])) != MAL_SUCCEED) {
					fprintf(stderr, "get: internal error: %s\n", e);
					GDKfree(e);
					exit(2);
				}

				if (stats == NULL) {
					fprintf(stderr, "get: no such database: %s\n", argv[i]);
					argv[i] = NULL;
				} else {
					if (orig == NULL) {
						orig = stats;
						w = stats;
					} else {
						w = w->next = stats;
					}
				}
			}
		}
	}

	/* name = 15 */
	/* prop = 8 */
	/* source = 7 */
	twidth -= 15 - 2 - 8 - 2 - 7 - 2;
	if (twidth < 6)
		twidth = 6;
	printf("     name          prop     source           value\n");
	while ((p = strtok(property, ",")) != NULL) {
		property = NULL;
		stats = orig;
		while (stats != NULL) {
			/* special virtual case */
			if (strcmp(p, "name") == 0) {
				source = "-";
				value = stats->dbname;
			} else {
				readProps(props, stats->path);
				kv = findConfKey(props, p);
				if (kv == NULL) {
					fprintf(stderr, "get: no such property: %s\n", p);
					stats = NULL;
					continue;
				}
				if (kv->val == NULL) {
					kv = findConfKey(defprops, p);
					source = "default";
					value = kv->val != NULL ? kv->val : "<unknown>";
				} else {
					source = "local";
					value = kv->val;
				}
			}
			printf("%-15s  %-8s  %-7s  %s\n",
					stats->dbname, p, source, value);
			freeConfFile(props);
			stats = stats->next;
		}
	}

	if (orig != NULL)
		SABAOTHfreeStatus(&orig);
	GDKfree(props);
}

static char seedChars[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
	'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
	'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
	'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
	'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'};

static void
command_create(int argc, char *argv[])
{
	int i;
	int maintenance = 0;  /* create locked database */
	int state = 0;        /* return status */
	int hadwork = 0;      /* if we actually did something */

	if (argc == 1) {
		/* print help message for this command */
		command_help(2, &argv[-1]);
		exit(1);
	}
	
	/* walk through the arguments and hunt for "options" */
	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--") == 0) {
			argv[i][0] = '\0';
			break;
		}
		if (argv[i][0] == '-') {
			if (argv[i][1] == 'l') {
				maintenance = 1;
				argv[i][0] = '\0';
			} else {
				fprintf(stderr, "create: unknown option: %s\n", argv[i]);
				command_help(argc + 1, &argv[-1]);
				exit(1);
			}
		}
	}

	/* do for each listed database */
	for (i = 1; i < argc; i++) {
		sabdb *stats;
		err e;
		char *dbname = argv[i];
		int c;

		/* check if dbname matches [A-Za-z0-9-_]+ */
		for (c = 0; dbname[c] != '\0'; c++) {
			if (
					!(dbname[c] >= 'A' && dbname[c] <= 'Z') &&
					!(dbname[c] >= 'a' && dbname[c] <= 'z') &&
					!(dbname[c] >= '0' && dbname[c] <= '9') &&
					!(dbname[c] == '-') &&
					!(dbname[c] == '_')
			   )
			{
				fprintf(stderr, "create: invalid character '%c' at %d "
						"in database name '%s'\n",
						dbname[c], c, dbname);
				dbname[0] = '\0';
				state |= 1;
				/* avoid usage message */
				hadwork = 1;
				break;
			}
		}
		if (dbname[0] == '\0')
			continue;

		/* the argument is the database to create, see what Sabaoth can
		 * tell us about it */
		if ((e = SABAOTHgetStatus(&stats, dbname)) != MAL_SUCCEED) {
			fprintf(stderr, "create: internal error: %s\n", e);
			GDKfree(e);
			exit(2);
		}

		if (stats == NULL) { /* sabaoth doesn't know, green light for us! */
			char path[8096];
			FILE *f;
			int size;
			char buf[48];

			snprintf(path, 8095, "%s/%s", dbfarm, dbname);
			path[8095] = '\0';
			if (mkdir(path, 0755) == -1) {
				fprintf(stderr, "create: unable to create %s: %s\n",
						argv[1], strerror(errno));

				SABAOTHfreeStatus(&stats);
				state |= 1;
				continue;
			}
			/* if we should put this database in maintenance, make sure
			 * no race condition ever can happen, by putting it into
			 * maintenance before it even exists for Merovingian */
			if (maintenance == 1) {
				snprintf(path, 8095, "%s/%s/.maintenance", dbfarm, dbname);
				fclose(fopen(path, "w"));
			}
			/* avoid GDK from making fugly complaints */
			snprintf(path, 8095, "%s/%s/.gdk_lock", dbfarm, dbname);
			f = fopen(path, "w");
			/* to all insanity, .gdk_lock is "valid" if it contains a
			 * ':', which it does by pure coincidence of a time having a
			 * ':' in there twice... */
			if (fwrite("bla:", 1, 4, f) < 4)
				fprintf(stderr, "create: failure in writing lock file\n");
			fclose(f);
			/* generate a vault key */
			size = rand();
			size = (size % (36 - 20)) + 20;
			for (c = 0; c < size; c++) {
				buf[c] = seedChars[rand() % 62];
			}
			for ( ; c < 48; c++) {
				buf[c] = '\0';
			}
			snprintf(path, 8095, "%s/%s/.vaultkey", dbfarm, dbname);
			f = fopen(path, "w");
			if (fwrite(buf, 1, 48, f) < 48)
				fprintf(stderr, "create: failure in writing vaultkey file\n");
			fclose(f);

			/* without an .uplog file, Merovingian won't work, this
			 * needs to be last to avoid race conditions */
			snprintf(path, 8095, "%s/%s/.uplog", dbfarm, dbname);
			fclose(fopen(path, "w"));

			printf("successfully created database '%s'%s\n", dbname,
					(maintenance == 1 ? " in maintenance mode" : ""));
		} else {
			fprintf(stderr, "create: database '%s' already exists\n", dbname);

			SABAOTHfreeStatus(&stats);
			state |= 1;
		}

		hadwork = 1;
	}

	if (hadwork == 0) {
		command_help(2, &argv[-1]);
		state |= 1;
	}
	exit(state);
}

/* recursive helper function to delete a directory */
static err
deletedir(char *dir)
{
	DIR *d;
	struct dirent *e;
	struct stat s;
	str buf = alloca(sizeof(char) * (PATHLENGTH + 1));
	str data = alloca(sizeof(char) * 8096);
	str path = alloca(sizeof(char) * (PATHLENGTH + 1));

	buf[PATHLENGTH] = '\0';
	d = opendir(dir);
	if (d == NULL) {
		/* silently return if we cannot find the directory; it's
		 * probably already deleted */
		if (errno == ENOENT)
			return(NO_ERR);
		snprintf(data, 8095, "unable to open dir %s: %s",
				dir, strerror(errno));
		return(GDKstrdup(data));
	}
	while ((e = readdir(d)) != NULL) {
		snprintf(path, PATHLENGTH, "%s/%s", dir, e->d_name);
		if (stat(path, &s) == -1) {
			snprintf(data, 8095, "unable to stat file %s: %s",
					path, strerror(errno));
			closedir(d);
			return(GDKstrdup(data));
		}

		if (S_ISREG(s.st_mode) || S_ISLNK(s.st_mode)) {
			if (unlink(path) == -1) {
				snprintf(data, 8095, "unable to unlink file %s: %s",
						path, strerror(errno));
				closedir(d);
				return(GDKstrdup(data));
			}
		} else if (S_ISDIR(s.st_mode)) {
			err er;
			/* recurse, ignore . and .. */
			if (strcmp(e->d_name, ".") != 0 &&
					strcmp(e->d_name, "..") != 0 &&
					(er = deletedir(path)) != NO_ERR)
			{
				closedir(d);
				return(er);
			}
		} else {
			/* fifos, block, char devices etc, we don't do */
			snprintf(data, 8095, "not a regular file: %s", path);
			closedir(d);
			return(GDKstrdup(data));
		}
	}
	closedir(d);
	if (rmdir(dir) == -1) {
		snprintf(data, 8095, "unable to remove directory %s: %s",
				dir, strerror(errno));
		return(GDKstrdup(data));
	}

	return(NO_ERR);
}

static void
command_destroy(int argc, char *argv[])
{
	int i;
	int force = 0;    /* ask for confirmation */
	int state = 0;    /* return status */
	int hadwork = 0;  /* did we do anything useful? */

	if (argc == 1) {
		/* print help message for this command */
		command_help(argc + 1, &argv[-1]);
		exit(1);
	}

	/* walk through the arguments and hunt for "options" */
	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--") == 0) {
			argv[i][0] = '\0';
			break;
		}
		if (argv[i][0] == '-') {
			if (argv[i][1] == 'f') {
				force = 1;
				argv[i][0] = '\0';
			} else {
				fprintf(stderr, "create: unknown option: %s\n", argv[i]);
				command_help(argc + 1, &argv[-1]);
				exit(1);
			}
		}
	}

	if (force == 0) {
		char answ;
		printf("you are about to remove database%s ", argc > 2 ? "s" : "");
		for (i = 1; i < argc; i++)
			printf("%s'%s'", i > 1 ? ", " : "", argv[i]);
		printf("\nALL data in %s will be lost, are you sure? [y/N] ",
				argc > 2 ? "these databases" : "this database");
		if (scanf("%c", &answ) >= 1 &&
				(answ == 'y' || answ == 'Y'))
		{
			/* do it! */
		} else {
			printf("aborted\n");
			exit(0);
		}
	}

	/* do for each listed database */
	for (i = 1; i < argc; i++) {
		sabdb *stats;
		err e;
		char *dbname = argv[i];

		if (dbname[0] == '\0')
			continue;

		/* the argument is the database to destroy, see what Sabaoth can
		 * tell us about it */
		if ((e = SABAOTHgetStatus(&stats, dbname)) != MAL_SUCCEED) {
			fprintf(stderr, "destroy: internal error: %s\n", e);
			GDKfree(e);
			exit(2);
		}

		if (stats != NULL) {
			err e;

			if (stats->state == SABdbRunning) {
				fprintf(stderr, "destroy: database '%s' is still running, "
						"stop database first\n", dbname);
				SABAOTHfreeStatus(&stats);
				state |= 1;
				hadwork = 1;
				continue;
			}

			/* annoyingly we have to delete file by file, and
			 * directories recursively... */
			if ((e = deletedir(stats->path)) != NULL) {
				fprintf(stderr, "destroy: failed to destroy '%s': %s\n",
						argv[1], e);
				GDKfree(e);
				SABAOTHfreeStatus(&stats);
				state |= 1;
				hadwork = 1;
				continue;
			}
			SABAOTHfreeStatus(&stats);
			printf("successfully destroyed database '%s'\n", dbname);
		} else {
			fprintf(stderr, "destroy: no such database: %s\n", dbname);
			state |= 1;
		}

		hadwork = 1;
	}

	if (hadwork == 0) {
		command_help(2, &argv[-1]);
		state |= 1;
	}
	exit(state);
}

static void
command_lock(int argc, char *argv[])
{
	int i;
	int state = 0;    /* return status */
	int hadwork = 0;  /* did we do anything useful? */

	if (argc == 1) {
		/* print help message for this command */
		command_help(argc + 1, &argv[-1]);
		exit(1);
	}

	/* do for each listed database */
	for (i = 1; i < argc; i++) {
		sabdb *stats;
		err e;
		char *dbname = argv[i];

		/* the argument is the database to take under maintenance, see
		 * what Sabaoth can tell us about it */
		if ((e = SABAOTHgetStatus(&stats, dbname)) != MAL_SUCCEED) {
			fprintf(stderr, "lock: internal error: %s\n", e);
			GDKfree(e);
			exit(2);
		}

		if (stats != NULL) {
			char path[8096];

			if (stats->locked == 1) {
				fprintf(stderr, "lock: database '%s' already is "
						"under maintenance\n", dbname);
				SABAOTHfreeStatus(&stats);
				hadwork = 1;
				state |= 1;
				continue;
			}

			/* put this database in maintenance mode */
			snprintf(path, 8095, "%s/.maintenance", stats->path);
			fclose(fopen(path, "w"));
			printf("database %s is now under maintenance\n", dbname);
			SABAOTHfreeStatus(&stats);
		} else {
			fprintf(stderr, "lock: no such database: %s\n", dbname);
			state |= 1;
		}
		hadwork = 1;
	}

	if (hadwork == 0) {
		command_help(2, &argv[-1]);
		state |= 1;
	}
	exit(state);
}

static void
command_release(int argc, char *argv[])
{
	int i;
	int state = 0;    /* return status */
	int hadwork = 0;  /* did we do anything useful? */

	if (argc == 1) {
		/* print help message for this command */
		command_help(argc + 1, &argv[-1]);
		exit(1);
	}

	/* do for each listed database */
	for (i = 1; i < argc; i++) {
		sabdb *stats;
		err e;
		char *dbname = argv[i];

		/* the argument is the database to take under maintenance, see
		 * what Sabaoth can tell us about it */
		if ((e = SABAOTHgetStatus(&stats, dbname)) != MAL_SUCCEED) {
			fprintf(stderr, "release: internal error: %s\n", e);
			GDKfree(e);
			exit(2);
		}

		if (stats != NULL) {
			char path[8096];

			if (stats->locked != 1) {
				fprintf(stderr, "release: database '%s' is not "
						"under maintenance\n", dbname);
				SABAOTHfreeStatus(&stats);
				hadwork = 1;
				state |= 1;
				continue;
			}

			/* put this database in maintenance mode */
			snprintf(path, 8095, "%s/.maintenance", stats->path);
			if (unlink(path) != 0) {
				fprintf(stderr, "failed to take database '%s' out of "
						" maintenance mode: %s\n", dbname, strerror(errno));
				SABAOTHfreeStatus(&stats);
				hadwork = 1;
				state |= 1;
				continue;
			}
			printf("database '%s' has been taken out of maintenance mode\n",
					dbname);
			SABAOTHfreeStatus(&stats);
		} else {
			fprintf(stderr, "release: no such database: %s\n", dbname);
			state |= 1;
		}
		hadwork = 1;
	}

	if (hadwork == 0) {
		command_help(2, &argv[-1]);
		state |= 1;
	}
	exit(state);
}


int
main(int argc, char *argv[])
{
	str p, prefix;
	FILE *cnf = NULL;
	char buf[1024];
	int fd;
	confkeyval ckv[] = {
		{"prefix",             GDKstrdup(MONETDB5_PREFIX), STR},
		{"gdk_dbfarm",         NULL,                       STR},
		{"gdk_nr_threads",     NULL,                       INT},
		{"sql_logdir",         NULL,                       STR},
		{"mero_doproxy",       GDKstrdup("yes"),           BOOL},
		{"mero_discoveryport", NULL,                       INT},
		{ NULL,                NULL,                       INVALID}
	};
	confkeyval *kv;
#ifdef TIOCGWINSZ
	struct winsize ws;

	if (ioctl(fileno(stdin), TIOCGWINSZ, &ws) == 0 && ws.ws_col > 0)
		TERMWIDTH = ws.ws_col;
#endif

	/* seed the randomiser for when we create a database */
	srand(time(NULL));
	
	/* My preciousssssssssss!  Set umask such that only /us/ can read
	 * things we created, which is a good idea with the vault rin... eh
	 * key around here and all. */
	umask(S_IRWXG | S_IRWXO);

	/* hunt for the config file, and read it, allow the caller to
	 * specify where to look using the MONETDB5CONF environment variable */
	p = getenv("MONETDB5CONF");
	if (p == NULL)
		p = MONETDB5_CONFFILE;
	cnf = fopen(p, "r");
	if (cnf == NULL) {
		fprintf(stderr, "cannot open config file %s\n", p);
		exit(1);
	}

	readConfFile(ckv, cnf);
	fclose(cnf);

	kv = findConfKey(ckv, "prefix");
	prefix = kv->val;

	kv = findConfKey(ckv, "gdk_dbfarm");
	dbfarm = replacePrefix(kv->val, prefix);
	if (dbfarm == NULL) {
		fprintf(stderr, "%s: cannot find gdk_dbfarm in config file\n", argv[0]);
		exit(2);
	}

	mero_running = 1;
	snprintf(buf, 1024, "%s/.merovingian_lock", dbfarm);
	fd = MT_lockf(buf, F_TLOCK, 4, 1);
	if (fd >= 0 || fd <= -2) {
		if (fd >= 0) {
			close(fd);
		} else {
			/* see if it is a permission problem, if so nicely abort */
			if (errno == EACCES) {
				fprintf(stderr, "permission denied\n");
				exit(1);
			}
		}
		/* locking succeed or locking was impossible */
		fprintf(stderr, "warning: merovingian is not running\n");
		mero_running = 0;
	}

	/* initialise Sabaoth so it knows where to look */
	SABAOTHinit(dbfarm, NULL);

	/* Start handling the arguments.
	 * monetdb command [options] [database [...]]
	 */
	if (argc <= 1) {
		command_help(0, NULL);
	} else if (strcmp(argv[1], "create") == 0) {
		command_create(argc - 1, &argv[1]);
	} else if (strcmp(argv[1], "destroy") == 0) {
		command_destroy(argc - 1, &argv[1]);
	} else if (strcmp(argv[1], "lock") == 0) {
		command_lock(argc - 1, &argv[1]);
	} else if (strcmp(argv[1], "release") == 0) {
		command_release(argc - 1, &argv[1]);
	} else if (strcmp(argv[1], "status") == 0) {
		command_status(argc - 1, &argv[1]);
	} else if (strcmp(argv[1], "start") == 0) {
		command_merocom(argc - 1, &argv[1], START);
	} else if (strcmp(argv[1], "stop") == 0) {
		command_merocom(argc - 1, &argv[1], STOP);
	} else if (strcmp(argv[1], "kill") == 0) {
		command_merocom(argc - 1, &argv[1], KILL);
	} else if (strcmp(argv[1], "set") == 0) {
		command_set(argc - 1, &argv[1], SET);
	} else if (strcmp(argv[1], "get") == 0) {
		/* change the keys to our names */
		kv = findConfKey(ckv, "mero_doproxy");
		kv->key = "forward";
		kv->type = OTHER;
		if (strcmp(kv->val, "yes") == 0) {
			GDKfree(kv->val);
			kv->val = GDKstrdup("proxy");
		} else {
			GDKfree(kv->val);
			kv->val = GDKstrdup("redirect");
		}
		kv = findConfKey(ckv, "mero_discoveryport");
		kv->key = "shared";
		kv->type = STR;
		if (kv->val == NULL) {
			kv->val = GDKstrdup("yes");
		} else if (strcmp(kv->val, "0") == 0) {
			GDKfree(kv->val);
			kv->val = GDKstrdup("no");
		} else {
			GDKfree(kv->val);
			kv->val = GDKstrdup("yes");
		}
		kv = findConfKey(ckv, "gdk_nr_threads");
		kv->key = "nthreads";
		command_get(argc - 1, &argv[1], ckv);
	} else if (strcmp(argv[1], "inherit") == 0) {
		command_set(argc - 1, &argv[1], INHERIT);
	} else if (strcmp(argv[1], "discover") == 0) {
		command_discover(argc - 1, &argv[1]);
	} else if (strcmp(argv[1], "help") == 0 || strcmp(argv[1], "-h") == 0) {
		command_help(argc - 1, &argv[1]);
	} else if (strcmp(argv[1], "version") == 0 || strcmp(argv[1], "-v") == 0) {
		command_version();
	} else {
		fprintf(stderr, "%s: unknown command: %s\n", argv[0], argv[1]);
		command_help(0, NULL);
	}

	freeConfFile(ckv);

	return(0);
}

/* vim:set ts=4 sw=4 noexpandtab: */
