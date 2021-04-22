/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/**
 * monetdb
 * Fabian Groffen
 * MonetDB Database Administrator's Toolkit
 *
 * A group of MonetDB servers in a dbfarm can be under control of
 * Merovingian, a daemon which by itself does not allow any user
 * interaction.  The monetdb utility is designed to be the interface for
 * the DBA to the dbfarm and its vicinity.  Creating or deleting
 * databases next to retrieving status information about them are the
 * primary goals of this tool.
 */

#include "monetdb_config.h"
#include "utils.h"
#include "properties.h"
#include "glob.h"
#include "control.h"
#include "msabaoth.h"
#include "mutils.h"
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
#include <stropts.h>		/* ioctl on Solaris */
#endif
#ifdef HAVE_SYS_IOCTL_H
#include <sys/ioctl.h>
#endif
#ifdef HAVE_TERMIOS_H
#include <termios.h> /* TIOCGWINSZ/TIOCSWINSZ */
#endif

static char *mero_host = NULL;
static int mero_port = -1;
static char *mero_pass = NULL;
static bool monetdb_quiet = false;
static int TERMWIDTH = 0;  /* default to no wrapping */

static void
command_help(int argc, char *argv[])
{
	if (argc < 2) {
		printf("Usage: monetdb [options] command [command-options-and-arguments]\n");
		printf("  where command is one of:\n");
		printf("    create, destroy, lock, release,\n");
		printf("    status, start, stop, kill,\n");
		printf("    profilerstart, profilerstop,\n");
		printf("    snapshot,\n");
		printf("    set, get, inherit,\n");
		printf("    discover, help, version\n");
		printf("  options can be:\n");
		printf("    -q       suppress status output\n");
		printf("    -h host  hostname to contact (remote merovingian)\n");
		printf("    -p port  port to contact\n");
		printf("    -P pass  password to use to login at remote merovingian\n");
		printf("  use the help command to get help for a particular command\n");
	} else if (strcmp(argv[1], "create") == 0) {
		printf("Usage: monetdb create [-m pattern] [-p pass] database [database ...]\n");
		printf("  Initialises a new database or multiplexfunnel in the MonetDB Server.  A\n");
		printf("  database created with this command makes it available\n");
		printf("  for use, however in maintenance mode (see monetdb lock).\n");
		printf("Options:\n");
		printf("  -m pattern  create a multiplex funnel for pattern.\n");
		printf("  -p pass     create database with given password for database user.\n");
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
	} else if (strcmp(argv[1], "profilerstart") == 0) {
		printf("Usage: monetdb profilerstart database [database ...]\n");
		printf("  Starts the collection of profiling events. The property\n");
		printf("  \""PROFILERLOGPROPERTY"\" should be set. Use the \"profilerstop\"\n");
		printf("  command to stop the profiler.\n");
	} else if (strcmp(argv[1], "profilerstop") == 0) {
		printf("Usage: monetdb profilerstop database [database ...]\n");
		printf("  Stops the collection of profiling events.\n");
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
		printf("      b (booting) and l (locked).\n");
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
	} else if (strcmp(argv[1], "master") == 0) {
		printf("Usage: monetdb master <dbname> [path]\n");
		printf("  Sets the database <dbname> into master mode.\n");
		printf("  This will actually stop the database take a snapshot\n");
		printf("  set the server into master mode and restart it.\n");
	} else if (strcmp(argv[1], "replica") == 0) {
		printf("Usage: monetdb replica <dbname> <mastername>\n");
		printf("  Creates a new replica with name <dbname> from the\n");
		printf("  database <mastername> The database <mastername> must \n");
		printf("  have been declared as master with the \"monetdb master\"\n");
		printf("  command.\n");
	} else if (strcmp(argv[1], "help") == 0) {
		printf("Yeah , help on help, how desparate can you be? ;)\n");
	} else if (strcmp(argv[1], "version") == 0) {
		printf("Usage: monetdb version\n");
		printf("  prints the version of this monetdb utility\n");
	} else if (strcmp(argv[1], "snapshot") == 0) {
		if (argc > 2 && strcmp(argv[2], "list") == 0) {
			printf("Usage: monetdb snapshot list [<dbname>...]\n");
			printf("  List snapshots for the given database, or all databases\n");
			printf("  if none given.\n");
		} else if (argc > 2 && strcmp(argv[2], "create") == 0) {
			printf("Usage: monetdb snapshot create [-t <targetfile>] <dbname> [<dbname>..]\n");
			printf("  Take a snapshot of the listed databases. Unless -t is given, the snapshots\n");
			printf("  are written to files named\n");
			printf("  <snapshotdir>/<dbname>_<YYYY><MM><DD>T<HH><MM>UTC<snapshotcompression>.\n");
			printf("Options:\n");
			printf("  -t <targetfile>  File on the server to write the snapshot to.\n");
		} else if (argc > 2 && strcmp(argv[2], "restore") == 0) {
			printf("Usage: monetdb snapshot restore [-f] <snapid> [dbname]\n");
			printf("  Create a database from the given snapshot, where  <snapid> is either\n");
			printf("  a path on the server or <dbname>@<num> as produced by\n");
			printf("  'monetdb snapshot list'\n");
			printf("Options:\n");
			printf("  -f  do not ask for confirmation\n");
		} else if (argc > 2 && strcmp(argv[2], "destroy") == 0) {
			printf("Usage: monetdb snapshot destroy [-f] <snapid>...\n");
			printf("       monetdb snapshot destroy [-f] -r <N> <dbname>...\n");
			printf("  Destroy one or more database snapshots, identified by a database name\n");
			printf("  and a sequence number as given by 'monetdb snapshot list'.\n");
			printf("  In the first form, the sequence numbers are part of the <snapid>.\n");
			printf("  In the second form. <dbname> is a database name or pattern such as 'staging*'\n");
			printf("  and N is the number of snapshots to retain.\n");
			printf("Options:\n");
			printf("  -f  Do not ask for confirmation\n");
			printf("  -r  Number of snapshots to retain.\n");
		} else if (argc > 2 && strcmp(argv[2], "write") == 0) {
			printf("Usage: monetdb snapshot write <dbname>\n");
			printf("  Write a snapshot of database <dbname> to standard out.\n");
		} else {
			printf("Usage: monetdb <create|list|restore|destroy|write> [arguments]\n");
			printf("  Manage database snapshots\n");
		}
	} else {
		printf("help: unknown command: %s\n", argv[1]);
	}
}

static void
command_version(void)
{
	printf("MonetDB Database Server Toolkit v%s", MONETDB_VERSION);
#ifdef MONETDB_RELEASE
	printf(" (%s)", MONETDB_RELEASE);
#else
	const char *rev = mercurial_revision();
	if (strcmp(rev, "Unknown") != 0)
		printf(" (hg id: %s)", rev);
#endif
	printf("\n");
}

static int
cmpsabdb(const void *p1, const void *p2)
{
	const sabdb *q1 = *(sabdb* const*)p1;
	const sabdb *q2 = *(sabdb* const*)p2;

	return strcmp(q1->dbname, q2->dbname);
}

/**
 * Helper function to perform the equivalent of
 * msab_getStatus(&stats, x) but over the network.
 */
static char *
MEROgetStatus(sabdb **ret, char *database)
{
	sabdb *orig;
	sabdb *stats;
	sabdb *w = NULL;
	size_t swlen = 50;
	size_t swpos = 0;
	sabdb **sw;
	char *p;
	char *buf;
	char *e;
	char *sp;

	if (database == NULL)
		database = "#all";

	e = control_send(&buf, mero_host, mero_port,
			database, "status", 1, mero_pass);
	if (e != NULL)
		return(e);

	sw = malloc(sizeof(sabdb *) * swlen);
	orig = NULL;
	if ((p = strtok_r(buf, "\n", &sp)) != NULL) {
		if (strcmp(p, "OK") != 0) {
			p = strdup(p);
			free(buf);
			free(sw);
			return(p);
		}
		for (swpos = 0; (p = strtok_r(NULL, "\n", &sp)) != NULL; swpos++) {
			e = msab_deserialise(&stats, p);
			if (e != NULL) {
				printf("WARNING: failed to parse response from "
						"monetdbd: %s\n", e);
				free(e);
				swpos--;
				continue;
			}
			if (swpos == swlen)
				sw = realloc(sw, sizeof(sabdb *) * (swlen = swlen * 2));
			sw[swpos] = stats;
		}
	}

	free(buf);

	if (swpos > 1) {
		qsort(sw, swpos, sizeof(sabdb *), cmpsabdb);
		orig = w = sw[0];
		for (swlen = 1; swlen < swpos; swlen++)
			w = w->next = sw[swlen];
	} else if (swpos == 1) {
		orig = sw[0];
		orig->next = NULL;
	}

	free(sw);

	*ret = orig;
	return(NULL);
}

static void
printStatus(sabdb *stats, int mode, int dbwidth, int uriwidth)
{
	sabuplog uplog;
	char *e;

	if ((e = msab_getUplogInfo(&uplog, stats)) != NULL) {
		fprintf(stderr, "status: internal error: %s\n", e);
		free(e);
		return;
	}

	if (mode == 1) {
		/* short one-line (default) mode */
		char state = '\0';
		char locked = '\0';
		char uptime[12];
		char avg[8];
		char info[64];
		char *dbname;
		char *uri;

		switch (stats->state) {
			case SABdbStarting:
				state = 'B';
			break;
			case SABdbRunning:
				state = 'R';
			break;
			case SABdbCrashed:
				state = 'C';
			break;
			case SABdbInactive:
				state = 'S';
			break;
			default:
				state = ' ';
			break;
		}
		/* override if locked for brevity */
		if (stats->locked)
			locked = 'L';

		info[0] = '\0';
		if (stats->state == SABdbStarting) {
			struct tm *t;
			t = localtime(&uplog.laststart);
			strftime(info, sizeof(info), "starting up since %Y-%m-%d %H:%M:%S", t);
		} else if (uplog.lastcrash != -1 &&
				stats->state != SABdbRunning &&
				uplog.crashavg1 == 1)
		{
			struct tm *t;
			t = localtime(&uplog.lastcrash);
			strftime(info, sizeof(info), "crashed (started on %Y-%m-%d %H:%M:%S)", t);
		}

		switch (stats->state) {
			case SABdbRunning:
			case SABdbStarting:
				secondsToString(uptime, time(NULL) - uplog.laststart, 1);
				break;
			case SABdbCrashed:
				secondsToString(uptime, time(NULL) - uplog.lastcrash, 1);
				break;
			case SABdbInactive:
				if (uplog.laststop != -1) {
					secondsToString(uptime, time(NULL) - uplog.laststop, 1);
					break;
				} /* else fall through */
			default:
				uptime[0] = '\0';
				break;
		}

		/* cut too long names */
		dbname = malloc(sizeof(char) * (dbwidth + 1));
		abbreviateString(dbname, stats->dbname, dbwidth);
		uri = malloc(sizeof(char) * (uriwidth + 1));
		abbreviateString(uri,
				info[0] != '\0' ? info : stats->uri ? stats->uri : "",
				uriwidth);
		/* dbname | state | health | uri/crash */
		printf("%-*s  %c%c%3s", dbwidth, dbname,
				locked ? locked : state, locked ? state : ' ', uptime);
		free(dbname);
		if (uplog.startcntr) {
			secondsToString(avg, uplog.avguptime, 1);
			printf("  %3d%% %3s",
					100 - (uplog.crashcntr * 100 / uplog.startcntr), avg);
		} else {
			printf("           ");
		}
		printf("  %-*s\n", uriwidth, uri);
		free(uri);
	} else if (mode == 2) {
		/* long mode */
		char *state;
		sablist *entry;
		char up[32];
		struct tm *t;

		switch (stats->state) {
			case SABdbStarting:
				state = "starting up";
			break;
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
		printf("  connection uri: %s\n", stats->uri);
		printf("  database name: %s\n", stats->dbname);
		printf("  state: %s\n", state);
		printf("  locked: %s\n", stats->locked ? "yes" : "no");
		entry = stats->scens;
		printf("  scenarios:");
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
		printf("  last start with crash: %s\n", up);
		if (uplog.laststart != -1) {
			t = localtime(&uplog.laststart);
			strftime(up, 32, "%Y-%m-%d %H:%M:%S", t);
		} else {
			sprintf(up, "(unknown)");
		}
		printf("  last start: %s\n", up);
		if (uplog.laststop != -1) {
			t = localtime(&uplog.laststop);
			strftime(up, 32, "%Y-%m-%d %H:%M:%S", t);
		} else {
			sprintf(up, "(unknown)");
		}
		printf("  last stop: %s\n", up);
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
		char up[32];
		char min[8], avg[8], max[8];
		struct tm *t;
		size_t off = 0;
		/* dbname, status -- since, crash averages */

		switch (stats->state) {
			case SABdbStarting:
				snprintf(buf, sizeof(buf), "starting ");
				off = sizeof("starting ") - 1;
				/* fall through */
			case SABdbRunning:
				t = localtime(&uplog.laststart);
				strftime(buf + off, sizeof(buf) - off,
						"up since %Y-%m-%d %H:%M:%S, ", t);
				secondsToString(up, time(NULL) - uplog.laststart, 999);
				strcat(buf, up);
			break;
			case SABdbCrashed:
				t = localtime(&uplog.lastcrash);
				strftime(buf, sizeof(buf), "crashed (started on %Y-%m-%d %H:%M:%S)", t);
			break;
			case SABdbInactive:
				snprintf(buf, sizeof(buf), "not running");
			break;
			default:
				snprintf(buf, sizeof(buf), "unknown");
			break;
		}
		if (stats->locked)
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

static sabdb *
globMatchDBS(int *fails, int argc, char *argv[], sabdb **orig, char *cmd)
{
	sabdb *w = NULL;
	sabdb *top = NULL;
	sabdb *prev;
	sabdb *stats;
	int i;
	int failcount = 0;
	bool matched;

	for (i = 1; i < argc; i++) {
		matched = false;
		if (argv[i] != NULL) {
			prev = NULL;
			for (stats = *orig; stats != NULL; stats = stats->next) {
				if (db_glob(argv[i], stats->dbname)) {
					matched = true;
					/* move out of orig into w, such that we can't
					 * get double matches in the same output list
					 * (as side effect also avoids a double free
					 * later on) */
					if (w == NULL) {
						top = w = stats;
					} else {
						w = w->next = stats;
					}
					if (prev == NULL) {
						*orig = stats->next;
						/* little hack to revisit the now top of the
						 * list */
						w->next = *orig;
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
			if (!matched) {
				fprintf(stderr, "%s: no such database: %s\n", cmd, argv[i]);
				argv[i] = NULL;
				failcount++;
			}
		}
	}
	if (fails)
		*fails = failcount;
	return(top);
}

/**
 * Helper function to run over the sabdb list and perform merocmd for
 * the value and reporting status on the performed command.  Either a
 * message is printed when success, or when premsg is not NULL, premsg
 * is printed before the action, and "done" printed afterwards.
 */
static void
simple_argv_cmd(char *cmd, sabdb *dbs, char *merocmd,
		char *successmsg, char *premsg)
{
	int state = 0;        /* return status */
	int hadwork = 0;      /* if we actually did something */
	char *ret;
	char *out;

	/* do for each listed database */
	for (; dbs != NULL; dbs = dbs->next) {
		if (premsg != NULL && !monetdb_quiet) {
			printf("%s '%s'... ", premsg, dbs->dbname);
			fflush(stdout);
		}

		ret = control_send(&out, mero_host, mero_port,
				dbs->dbname, merocmd, 0, mero_pass);

		if (ret != NULL) {
			if (premsg != NULL && !monetdb_quiet)
				printf("FAILED\n");
			fprintf(stderr, "%s: %s\n",
					cmd, ret);
			free(ret);
			exit(2);
		}

		if (strcmp(out, "OK") == 0) {
			if (!monetdb_quiet) {
				if (premsg != NULL) {
					printf("done\n");
				} else {
					printf("%s: %s\n", successmsg, dbs->dbname);
				}
			}
		} else {
			if (premsg != NULL && !monetdb_quiet)
				printf("FAILED\n");
			fprintf(stderr, "%s: %s\n", cmd, out);

			state |= 1;
		}
		free(out);

		hadwork = 1;
	}

	if (hadwork == 0) {
		char *argv[2] = { "monetdb", cmd };
		command_help(2, argv);
		exit(1);
	}

	if (state != 0)
		exit(state);
}

/**
 * Helper function for commands in their most general form: no option
 * flags and just pushing all (database) arguments over to merovingian
 * for performing merocmd action.
 */
static int
simple_command(int argc, char *argv[], char *merocmd, char *successmsg, bool glob)
{
	int i;
	sabdb *orig = NULL;
	sabdb *stats = NULL;
	char *e;
	int fails = 0;

	if (argc == 1) {
		/* print help message for this command */
		command_help(2, &argv[-1]);
		exit(1);
	}

	/* walk through the arguments and hunt for "options" */
	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--") == 0) {
			argv[i] = NULL;
			break;
		}
		if (argv[i][0] == '-') {
			fprintf(stderr, "%s: unknown option: %s\n", argv[0], argv[i]);
			command_help(argc + 1, &argv[-1]);
			exit(1);
		}
	}

	if (glob) {
		if ((e = MEROgetStatus(&orig, NULL)) != NULL) {
			fprintf(stderr, "%s: %s\n", argv[0], e);
			free(e);
			exit(2);
		}
		stats = globMatchDBS(&fails, argc, argv, &orig, argv[0]);
		msab_freeStatus(&orig);
		orig = stats;

		if (orig == NULL)
			return 1;
	} else {
		for (i = 1; i < argc; i++) {
			if (argv[i] != NULL) {
				/* maintain input order */
				if (orig == NULL) {
					stats = orig = calloc(1, sizeof(sabdb));
				} else {
					stats = stats->next = calloc(1, sizeof(sabdb));
				}
				stats->dbname = strdup(argv[i]);
			}
		}
	}

	simple_argv_cmd(argv[0], orig, merocmd, successmsg, NULL);
	msab_freeStatus(&orig);
	return fails > 0;
}

static int
command_status(int argc, char *argv[])
{
	bool doall = true; /* we default to showing all */
	int mode = 1;  /* 0=crash, 1=short, 2=long */
	char *state = "rbscl"; /* contains states to show */
	int i;
	char *p;
	char *e;
	sabdb *stats;
	sabdb *orig;
	sabdb *prev;
	sabdb *neworig = NULL;
	int t;
	int twidth = TERMWIDTH;
	int dbwidth = 0;
	int uriwidth = 0;
	int fails = 0;

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
								case 'b': /* booting (starting up) */
								case 'r': /* running (started) */
								case 's': /* stopped */
								case 'c': /* crashed */
								case 'l': /* locked */
								break;
								default:
									fprintf(stderr, "status: unknown flag for -s: -%c\n", *p);
									command_help(2, &argv[-1]);
									exit(1);
							}
						}
						p--;
					break;
					case '-':
						if (p[1] == '\0') {
							if (argc - 1 > i)
								doall = false;
							i = argc;
							break;
						}
						/* fall through */
					default:
						fprintf(stderr, "status: unknown option: -%c\n", *p);
						command_help(2, &argv[-1]);
						exit(1);
				}
			}
			/* make this option no longer available, for easy use
			 * later on */
			argv[i] = NULL;
		} else {
			doall = false;
		}
	}

	if ((e = MEROgetStatus(&orig, NULL)) != NULL) {
		fprintf(stderr, "status: %s\n", e);
		free(e);
		exit(2);
	}

	/* look at the arguments and evaluate them based on a glob (hence we
	 * listed all databases before) */
	if (!doall) {
		stats = globMatchDBS(&fails, argc, argv, &orig, "status");
		msab_freeStatus(&orig);
		orig = stats;
	}

	/* perform selection based on state (and order at the same time) */
	for (p = &state[strlen(state) - 1]; p >= state; p--) {
		bool curLock = false;
		SABdbState curMode = SABdbIllegal;
		switch (*p) {
			case 'b':
				curMode = SABdbStarting;
			break;
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
				curLock = true;
			break;
		}
		stats = orig;
		prev = NULL;
		while (stats != NULL) {
			if (stats->locked == curLock &&
					(curLock ||
					 (!curLock && stats->state == curMode)))
			{
				sabdb *next = stats->next;
				stats->next = neworig;
				neworig = stats;
				if (prev == NULL) {
					orig = next;
				} else {
					prev->next = next;
				}
				stats = next;
			} else {
				prev = stats;
				stats = stats->next;
			}
		}
	}
	msab_freeStatus(&orig);
	orig = neworig;

	if (mode == 1 && orig != NULL) {
		int len = 0;

		/* calculate dbwidth and uriwidth */
		uriwidth = 32;
		for (stats = orig; stats != NULL; stats = stats->next) {
			if ((t = strlen(stats->dbname)) > dbwidth)
				dbwidth = t;
			if (stats->uri != NULL && (t = strlen(stats->uri)) > uriwidth)
				uriwidth = t;
		}

		/* Ultra Condensed State(tm) since Feb2013:
		state
		R  6s     (Running)
		R 14w
		R 99y     (purely hypothetical)
		B  3s     (Booting: in practice cannot be observed yet due to lock)
		S  1w     (Stopped)
		LR12h     (Locked/Running)
		LS        (Locked/Stopped)
		C         (Crashed)
		      = 5 chars
		*/

		/* health
		 health
		100% 12d
		 42%  4s
		         = 8 chars
		*/

		len = (dbwidth < 4 ? 4 : dbwidth) + 2 + 5 + 2 + 8 + 2 + uriwidth;
		if (twidth > 0 && len > twidth) {
			if (len - twidth < 10) {
				uriwidth -= len - twidth;
				if (dbwidth < 4)
					dbwidth = 4;
			} else {
				/* reduce relative to usage */
				if (dbwidth < 4) {
					dbwidth = 4;
				} else {
					dbwidth = (int)(dbwidth * 1.0 / (dbwidth + uriwidth) * (len - twidth));
					if (dbwidth < 4)
						dbwidth = 4;
				}
				uriwidth = twidth - (dbwidth + 2 + 5 + 2 + 8 + 2);
				if (uriwidth < 8)
					uriwidth = 8;
			}
		} else {
			if (dbwidth < 4)
				dbwidth = 4;
		}

		/* print header */
		printf("%*sname%*s  state   health   %*sremarks\n",
				(dbwidth - 4) / 2, "", (dbwidth - 4 + 1) / 2, "",
				(uriwidth - 7) / 2, "");
	}

	for (stats = orig; stats != NULL; stats = stats->next)
		printStatus(stats, mode, dbwidth, uriwidth);

	if (orig != NULL)
		msab_freeStatus(&orig);

	return fails > 0;
}

static int
cmpurl(const void *p1, const void *p2)
{
	const char *q1 = *(char* const*)p1;
	const char *q2 = *(char* const*)p2;

	if (strncmp("mapi:monetdb://", q1, 15) == 0)
		q1 += 15;
	if (strncmp("mapi:monetdb://", q2, 15) == 0)
		q2 += 15;
	return strcmp(q1, q2);
}

static void
command_discover(int argc, char *argv[])
{
	char path[8096];
	char *buf;
	char *p, *q;
	size_t twidth = TERMWIDTH;
	char *location = NULL;
	char *match = NULL;
	size_t numlocs = 50;
	size_t posloc = 0;
	size_t loclen = 0;
	char **locations = malloc(sizeof(char*) * numlocs);
	char *sp;

	if (argc == 0) {
		exit(2);
	} else if (argc > 2) {
		/* print help message for this command */
		command_help(2, &argv[-1]);
		exit(1);
	} else if (argc == 2) {
		match = argv[1];
	}

 	/* Send the pass phrase to unlock the information available in
	 * merovingian.  Anelosimus eximius is a social species of spiders,
	 * which help each other, just like merovingians do among each
	 * other. */
	p = control_send(&buf, mero_host, mero_port,
			"anelosimus", "eximius", 1, mero_pass);
	if (p != NULL) {
		printf("%s: %s\n", argv[0], p);
		free(p);
		exit(2);
	}

	if ((p = strtok_r(buf, "\n", &sp)) != NULL) {
		if (strcmp(p, "OK") != 0) {
			fprintf(stderr, "%s: %s\n", argv[0], p);
			free(buf);
			exit(1);
		}
		if (twidth > 0)
			location = malloc(twidth + 1);
		while ((p = strtok_r(NULL, "\n", &sp)) != NULL) {
			if ((q = strchr(p, '\t')) == NULL) {
				/* doesn't look correct */
				printf("%s: WARNING: discarding incorrect line: %s\n",
						argv[0], p);
				continue;
			}
			*q++ = '\0';

			snprintf(path, sizeof(path), "%s%s", q, p);

			if (match == NULL || db_glob(match, path)) {
				if (twidth > 0) {
					/* cut too long location name */
					abbreviateString(location, path, twidth);
				} else {
					location = path;
				}
				/* store what we found */
				if (posloc == numlocs)
					locations = realloc(locations,
							sizeof(char *) * (numlocs = numlocs * 2));
				locations[posloc++] = strdup(location);
				if (strlen(location) > loclen)
					loclen = strlen(location);
			}
		}
		if (twidth > 0)
			free(location);
	}

	free(buf);

	if (posloc > 0) {
		printf("%*slocation\n",
				(int)(loclen - 8 /* "location" */ - ((loclen - 8) / 2)), "");
		qsort(locations, posloc, sizeof(char *), cmpurl);
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
	KILL
} startstop;

static int
command_startstop(int argc, char *argv[], startstop mode)
{
	bool doall = false;
	int i;
	char *e;
	sabdb *orig = NULL;
	sabdb *stats;
	sabdb *prev;
	char *type = NULL;
	char *action = NULL;
	char *p;
	char *nargv[64];
	int fails = 0;

	switch (mode) {
		case START:
			type = "start";
			action = "starting database";
		break;
		case STOP:
			type = "stop";
			action = "stopping database";
		break;
		case KILL:
			type = "kill";
			action = "killing database";
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
						doall = true;
					break;
					case '-':
						if (p[1] == '\0') {
							if (argc - 1 > i)
								doall = false;
							i = argc;
							break;
						}
						/* fall through */
					default:
						fprintf(stderr, "%s: unknown option: -%c\n", type, *p);
						command_help(2, &argv[-1]);
						exit(1);
					break;
				}
			}
			/* make this option no longer available, for easy use
			 * later on */
			argv[i] = NULL;
		}
	}

	if ((e = MEROgetStatus(&orig, NULL)) != NULL) {
		fprintf(stderr, "%s: %s\n", type, e);
		free(e);
		exit(2);
	}
	if (!doall) {
		stats = globMatchDBS(&fails, argc, argv, &orig, type);
		msab_freeStatus(&orig);
		orig = stats;
	}

	argv = nargv;
	i = 0;
	argv[i++] = type;

	stats = orig;
	prev = NULL;
	while (stats != NULL) {
		/* When -a was given, we're supposed to start all known
		 * databases.  In this mode we should omit starting already
		 * started databases, so we need to check first. */

		if (doall && (
				((mode == STOP || mode == KILL) && (stats->state != SABdbRunning && stats->state != SABdbStarting))
				|| (mode == START && stats->state == SABdbRunning)))
		{
			/* needs not to be started/stopped, remove from list */
			if (prev == NULL) {
				orig = stats->next;
			} else {
				prev->next = stats->next;
			}
			stats->next = NULL;
			msab_freeStatus(&stats);
			if (prev == NULL) {
				stats = orig;
				continue;
			}
			stats = prev;
		}
		prev = stats;
		stats = stats->next;
	}

	if (orig != NULL) {
		simple_argv_cmd(argv[0], orig, type, NULL, action);
		msab_freeStatus(&orig);
	}

	return fails > 0;
}

typedef enum {
	SET = 0,
	INHERIT
} meroset;

static _Noreturn void command_set(int argc, char *argv[], meroset type);

static void
command_set(int argc, char *argv[], meroset type)
{
	char *p = NULL;
	char property[24] = "";
	int i;
	int state = 0;
	char *res;
	char *out;
	sabdb *orig = NULL;
	sabdb *stats = NULL;
	char *e;
	int fails = 0;

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
						/* fall through */
					default:
						fprintf(stderr, "%s: unknown option: -%c\n",
								argv[0], *p);
						command_help(2, &argv[-1]);
						exit(1);
					break;
				}
			}
			/* make this option no longer available, for easy use
			 * later on */
			argv[i] = NULL;
		} else if (property[0] == '\0') {
			/* first non-option is property, rest is database */
			p = argv[i];
			if (type == SET) {
				if ((p = strchr(argv[i], '=')) == NULL) {
					fprintf(stderr, "set: need property=value\n");
					command_help(2, &argv[-1]);
					exit(1);
				}
				*p = '\0';
				snprintf(property, sizeof(property), "%s", argv[i]);
				*p++ = '=';
				p = argv[i];
			} else {
				snprintf(property, sizeof(property), "%s", argv[i]);
			}
			argv[i] = NULL;
		}
	}

	if (property[0] == '\0') {
		fprintf(stderr, "%s: need a property argument\n", argv[0]);
		command_help(2, &argv[-1]);
		exit(1);
	}

	if ((e = MEROgetStatus(&orig, NULL)) != NULL) {
		fprintf(stderr, "%s: %s\n", argv[0], e);
		free(e);
		exit(2);
	}
	stats = globMatchDBS(&fails, argc, argv, &orig, argv[0]);
	msab_freeStatus(&orig);
	orig = stats;

	if (orig == NULL) {
		/* error already printed by globMatchDBS */
		exit(1);
	}

	/* handle rename separately due to single argument constraint */
	if (strcmp(property, "name") == 0) {
		if (type == INHERIT) {
			fprintf(stderr, "inherit: cannot default to a database name\n");
			exit(1);
		}

		if (orig->next != NULL) {
			fprintf(stderr, "%s: cannot rename multiple databases to "
					"the same name\n", argv[0]);
			exit(1);
		}

		out = control_send(&res, mero_host, mero_port,
				orig->dbname, p, 0, mero_pass);
		if (out != NULL || strcmp(res, "OK") != 0) {
			res = out == NULL ? res : out;
			fprintf(stderr, "%s: %s\n", argv[0], res);
			state |= 1;
		}
		free(res);

		msab_freeStatus(&orig);
		exit(state);
	}

	for (stats = orig; stats != NULL; stats = stats->next) {
		if (type == INHERIT) {
			strncat(property, "=", sizeof(property) - strlen(property) - 1);
			p = property;
		}
		out = control_send(&res, mero_host, mero_port,
				stats->dbname, p, 0, mero_pass);
		if (out != NULL || strcmp(res, "OK") != 0) {
			res = out == NULL ? res : out;
			fprintf(stderr, "%s: %s\n", argv[0], res);
			state |= 1;
		}
		free(res);
	}

	msab_freeStatus(&orig);
	exit(state || fails > 0);
}

static int
command_get(int argc, char *argv[])
{
	bool doall = true;
	char *p;
	char *property = NULL;
	char propall = 0;
	char vbuf[512];
	char *buf = 0;
	char *e;
	int i;
	sabdb *orig, *stats;
	int twidth = TERMWIDTH;
	char *source, *value = NULL;
	confkeyval *kv;
	confkeyval *defprops = getDefaultProps();
	confkeyval *props = getDefaultProps();
	int fails = 0;

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
								doall = false;
							i = argc;
							break;
						}
						/* fall through */
					default:
						fprintf(stderr, "get: unknown option: -%c\n", *p);
						command_help(2, &argv[-1]);
						exit(1);
					break;
				}
			}
			/* make this option no longer available, for easy use
			 * later on */
			argv[i] = NULL;
		} else if (property == NULL) {
			/* first non-option is property, rest is database */
			property = argv[i];
			argv[i] = NULL;
			if (strcmp(property, "all") == 0)
				propall = 1;
		} else {
			doall = false;
		}
	}

	if (property == NULL) {
		fprintf(stderr, "get: need a property argument\n");
		command_help(2, &argv[-1]);
		exit(1);
	}
	if ((e = MEROgetStatus(&orig, NULL)) != NULL) {
		fprintf(stderr, "get: %s\n", e);
		free(e);
		exit(2);
	}

	/* look at the arguments and evaluate them based on a glob (hence we
	 * listed all databases before) */
	if (!doall) {
		stats = globMatchDBS(&fails, argc, argv, &orig, "get");
		msab_freeStatus(&orig);
		orig = stats;
	}

	/* avoid work when there are no results */
	if (orig == NULL) {
		free(props);
		free(defprops);
		return 1;
	}

	e = control_send(&buf, mero_host, mero_port,
			"#defaults", "get", 1, mero_pass);
	if (e != NULL) {
		fprintf(stderr, "get: %s\n", e);
		free(e);
		exit(2);
	} else if (buf == NULL) {
		fprintf(stderr, "get: malloc failed\n");
		exit(2);
	} else if (strncmp(buf, "OK\n", 3) != 0) {
		fprintf(stderr, "get: %s\n", buf);
		free(buf);
		exit(1);
	}
	readPropsBuf(defprops, buf + 3);
	free(buf);

	if (twidth > 0) {
		/* name = 15 */
		/* prop = 8 */
		/* source = 7 */
		twidth -= 15 + 2 + 8 + 2 + 7 + 2;
		if (twidth < 6)
			twidth = 6;
		value = malloc(sizeof(char) * twidth + 1);
	}
	stats = orig;
	while (stats != NULL) {
		e = control_send(&buf, mero_host, mero_port,
				stats->dbname, "get", 1, mero_pass);
		if (e != NULL) {
			fprintf(stderr, "get: %s\n", e);
			free(e);
			exit(2);
		} else if (buf == NULL) {
			fprintf(stderr, "get: malloc failed\n");
			exit(2);
		} else if (strncmp(buf, "OK\n", 3) != 0) {
			fprintf(stderr, "get: %s\n", buf);
			free(buf);
			exit(1);
		}
		readPropsBuf(props, buf + 3);
		free(buf);

		if (propall == 1) {
			size_t off = 0;
			kv = props;
			off += snprintf(vbuf, sizeof(vbuf), "name");
			while (kv->key != NULL) {
				off += snprintf(vbuf + off, sizeof(vbuf) - off,
						",%s", kv->key);
				kv++;
			}
		} else {
			/* check validity of properties before printing them */
			if (stats == orig) {
				char *sp;
				snprintf(vbuf, sizeof(vbuf), "%s", property);
				buf = vbuf;
				while ((p = strtok_r(buf, ",", &sp)) != NULL) {
					buf = NULL;
					if (strcmp(p, "name") == 0)
						continue;
					kv = findConfKey(props, p);
					if (kv == NULL)
						fprintf(stderr, "get: no such property: %s\n", p);
				}
			}
			snprintf(vbuf, sizeof(vbuf), "%s", property);
		}
		buf = vbuf;
		/* print header after errors */
		if (stats == orig)
			printf("     name          prop     source           value\n");

		char *sp;
		while ((p = strtok_r(buf, ",", &sp)) != NULL) {
			buf = NULL;

			/* filter properties based on object type */
			kv = findConfKey(props, "type");
			if (kv != NULL && kv->val != NULL) {
				if (strcmp(kv->val, "mfunnel") == 0) {
					if (strcmp(p, "name") != 0 &&
							strcmp(p, "type") != 0 &&
							strcmp(p, "mfunnel") != 0 &&
							strcmp(p, "shared") != 0)
						continue;
				}
			} else { /* no type == database (default) */
				if (strcmp(p, "mfunnel") == 0)
					continue;
			}

			/* special virtual case */
			if (strcmp(p, "name") == 0) {
				source = "-";
				if (twidth > 0) {
					abbreviateString(value, stats->dbname, twidth);
				} else {
					value = stats->dbname;
				}
			} else {
				kv = findConfKey(props, p);
				if (kv == NULL)
					continue;
				if (kv->val == NULL) {
					char *y = NULL;
					kv = findConfKey(defprops, p);
					source = "default";
					y = kv != NULL && kv->val != NULL ? kv->val : "<unknown>";
					if (twidth > 0) {
						abbreviateString(value, y, twidth);
					} else {
						value = y;
					}
				} else {
					source = "local";
					if (twidth > 0) {
						abbreviateString(value, kv->val, twidth);
					} else {
						value = kv->val;
					}
				}
			}

			printf("%-15s  %-8s  %-7s  %s\n",
					stats->dbname, p, source, value);
		}

		freeConfFile(props);
		stats = stats->next;
	}

	if (twidth > 0)
		free(value);
	msab_freeStatus(&orig);
	free(props);
	freeConfFile(defprops);
	free(defprops);
	return fails > 0;
}

static void
command_create(int argc, char *argv[])
{
	int i;
	char *mfunnel = NULL;
	char *password = NULL;
	sabdb *orig = NULL;
	sabdb *stats = NULL;

	if (argc == 1) {
		/* print help message for this command */
		command_help(argc + 1, &argv[-1]);
		exit(1);
	}

	/* walk through the arguments and hunt for "options" */
	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--") == 0) {
			argv[i] = NULL;
			break;
		}
		if (argv[i][0] == '-') {
			if (argv[i][1] == 'm') {
				if (argv[i][2] != '\0') {
					mfunnel = &argv[i][2];
					argv[i] = NULL;
				} else if (i + 1 < argc && argv[i + 1][0] != '-') {
					argv[i] = NULL;
					mfunnel = argv[++i];
					argv[i] = NULL;
				} else {
					fprintf(stderr, "create: -m needs an argument\n");
					command_help(2, &argv[-1]);
					exit(1);
				}
			} else if (argv[i][1] == 'p') {
				if (argv[i][2] != '\0') {
					password = &argv[i][2];
					argv[i] = NULL;
				} else if (i + 1 < argc && argv[i + 1][0] != '-') {
					argv[i] = NULL;
					password = argv[++i];
					argv[i] = NULL;
				} else {
					fprintf(stderr, "create: -p needs an argument\n");
					command_help(2, &argv[-1]);
					exit(1);
				}
			} else {
				fprintf(stderr, "create: unknown option: %s\n", argv[i]);
				command_help(argc + 1, &argv[-1]);
				exit(1);
			}
		}
	}

	for (i = 1; i < argc; i++) {
		if (argv[i] != NULL) {
			/* maintain input order */
			if (orig == NULL) {
				stats = orig = calloc(1, sizeof(sabdb));
			} else {
				stats = stats->next = calloc(1, sizeof(sabdb));
			}
			stats->dbname = strdup(argv[i]);
		}
	}

	if (mfunnel != NULL) {
		size_t len = strlen("create mfunnel=") + strlen(mfunnel) + 1;
		char *cmd = malloc(len);
		snprintf(cmd, len, "create mfunnel=%s", mfunnel);
		simple_argv_cmd(argv[0], orig, cmd,
				"created multiplex-funnel in maintenance mode", NULL);
		free(cmd);
	} else if (password != NULL) {
		size_t len = strlen("create password=") + strlen(password) + 1;
		char *cmd = malloc(len);
		snprintf(cmd, len, "create password=%s", password);
		simple_argv_cmd(argv[0], orig, cmd,
				"created database with password for monetdb user", NULL);
		free(cmd);
	} else {
		simple_argv_cmd(argv[0], orig, "create",
				"created database in maintenance mode", NULL);
	}
	/* msab_freeStatus does not free dbname */
	for (stats = orig; stats; stats = stats->next) {
		free(stats->dbname);
		stats->dbname = NULL;
	}
	msab_freeStatus(&orig);
}

static void
command_destroy(int argc, char *argv[])
{
	int i;
	int force = 0;    /* ask for confirmation */
	char *e;
	sabdb *orig = NULL;
	sabdb *stats = NULL;

	if (argc == 1) {
		/* print help message for this command */
		command_help(argc + 1, &argv[-1]);
		exit(1);
	}

	/* walk through the arguments and hunt for "options" */
	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--") == 0) {
			argv[i] = NULL;
			break;
		}
		if (argv[i][0] == '-') {
			if (argv[i][1] == 'f') {
				force = 1;
				argv[i] = NULL;
			} else {
				fprintf(stderr, "destroy: unknown option: %s\n", argv[i]);
				command_help(argc + 1, &argv[-1]);
				exit(1);
			}
		}
	}

	if ((e = MEROgetStatus(&orig, NULL)) != NULL) {
		fprintf(stderr, "destroy: %s\n", e);
		free(e);
		exit(2);
	}
	stats = globMatchDBS(NULL, argc, argv, &orig, "destroy");
	msab_freeStatus(&orig);
	orig = stats;

	if (orig == NULL)
		exit(1);

	if (force == 0) {
		char answ;
		printf("you are about to remove database%s ", orig->next != NULL ? "s" : "");
		for (stats = orig; stats != NULL; stats = stats->next)
			printf("%s'%s'", stats != orig ? ", " : "", stats->dbname);
		printf("\nALL data in %s will be lost, are you sure? [y/N] ",
				orig->next != NULL ? "these databases" : "this database");
		if (scanf("%c", &answ) >= 1 &&
				(answ == 'y' || answ == 'Y'))
		{
			/* do it! */
		} else {
			printf("aborted\n");
			exit(1);
		}
	} else {
		char *ret;
		char *out;
		for (stats = orig; stats != NULL; stats = stats->next) {
			if (stats->state == SABdbRunning || stats->state == SABdbStarting) {
				ret = control_send(&out, mero_host, mero_port,
						stats->dbname, "stop", 0, mero_pass);
				if (ret != NULL)
					free(ret);
				else
					free(out);
			}
		}
	}

	simple_argv_cmd(argv[0], orig, "destroy", "destroyed database", NULL);
	msab_freeStatus(&orig);
}

static int
command_lock(int argc, char *argv[])
{
	return simple_command(argc, argv, "lock", "put database under maintenance", true);
}

static int
command_release(int argc, char *argv[])
{
	return simple_command(argc, argv, "release", "taken database out of maintenance mode", true);
}

static int
command_profilerstart(int argc, char *argv[])
{
	return simple_command(argc, argv, "profilerstart", "started profiler", true);
}

static int
command_profilerstop(int argc, char *argv[])
{
	return simple_command(argc, argv, "profilerstop", "stopped profiler", true);
}

/* Snapshot this single database to the given file */
static void
snapshot_create_adhoc(sabdb *databases, char *filename) {
	/* databases is supposed to only hold a single database */
	assert(databases != NULL);
	assert(databases->next == NULL);

	char *merocmd = malloc(100 + strlen(filename));
	sprintf(merocmd, "snapshot create adhoc %s", filename);

	simple_argv_cmd("snapshot", databases, merocmd, NULL, "snapshotting database");

	free(merocmd);
}

/* Create automatic snapshots of the given databases */
static void
snapshot_create_automatic(sabdb *databases) {
	simple_argv_cmd("snapshot", databases, "snapshot create automatic", NULL, "snapshotting database");
}

/* Comparison function used for qsort */
static int
snapshot_enumerate_helper(const void *left, const void *right)
{
	const struct snapshot *left_snap = left;
	const struct snapshot *right_snap = right;
	int cmp;

	cmp = strcmp(left_snap->dbname, right_snap->dbname);
	if (cmp != 0)
		return cmp;

	// Careful! Sort newest to oldest
	if (left_snap->time < right_snap->time)
		return +1; // !!
	if (left_snap->time > right_snap->time)
		return -1; // !!

	// No preference
	return 0;
}

/* Retrieve a list of all snapshots and Store it in the array. */
static char*
snapshot_enumerate(struct snapshot **snapshots, int *nsnapshots)
{
	int ninitial = *nsnapshots;
	char *out = NULL;
	char *ret = control_send(&out, mero_host, mero_port, "", "snapshot list", 1, mero_pass);
	if (ret != NULL)
		return ret;

	if (strcmp(out, "OK1") == 0) {
		// ok, empty resultset
		free(out);
	} else if (strncmp(out, "OK1\n", 4) == 0) {
		// ok, nonempty resultset. Parse it.
		char *p = out + 4;
		char *end = p + strlen(p);
		while (p < end) {
			char datebuf[100];
			char *parse_result;
			struct tm tm = {0};
			char *eol = strchr(p, '\n');
			eol = (eol != NULL) ? eol : end;
			time_t timestamp, pre, post;
			uint64_t size;
			int len;
			if (sscanf(p, "%99s %" SCNu64 " %n", datebuf, &size, &len) != 2) {
				free(out);
				return strdup("internal parse error");
			}
			parse_result = strptime(datebuf, "%Y%m%dT%H%M%S", &tm);
			if (parse_result == NULL || *parse_result != '\0') {
				free(out);
				return strdup("internal timestamp parse error");
			}
			// Unfortunately mktime interprets tm as local time, we have
			// to correct for that.
			timestamp = mktime(&tm);
			pre = time(NULL);
			gmtime_r(&pre, &tm);
			post = mktime(&tm);
			timestamp += pre - post;
			p += len;
			char *dbend = strchr(p, ' ');
			if (dbend == NULL) {
				free(out);
				return strdup("Internal parse error");
			}
			int dblen = dbend - p;
			char *path = dbend + 1;
			int pathlen = eol - path;
			struct snapshot *snap = push_snapshot(snapshots, nsnapshots);
			snap->dbname = malloc(dblen + 1);
			memmove(snap->dbname, p, dblen);
			snap->dbname[dblen] = '\0';
			snap->time = timestamp;
			snap->size = size;
			snap->path = malloc(pathlen + 1);
			memmove(snap->path, path, pathlen);
			snap->path[pathlen] = '\0';
			p = eol + 1;
		};
		free(out);
	} else {
		return out;
	}

	// Sort them and give names of the form dbname@seqno
	if (*nsnapshots > ninitial) {
		int sort_len = *nsnapshots - ninitial;
		struct snapshot *sort_start = *snapshots + ninitial;
		qsort(sort_start, sort_len, sizeof(struct snapshot), snapshot_enumerate_helper);
		struct snapshot *prev = NULL;
		int counter;
		for (struct snapshot *cur = sort_start; cur < sort_start + sort_len; cur++) {
			if (prev == NULL || strcmp(prev->dbname, cur->dbname) != 0)
				counter = 0;
			counter++;
			cur->name = malloc(strlen(cur->dbname) + 10);
			sprintf(cur->name, "%s@%d", cur->dbname, counter);
			prev = cur;
		}
	}

	return NULL;
}

static void
snapshot_list(int nglobs, char *globs[]) {
	struct snapshot *snapshots = NULL;
	int nsnapshots = 0;

	// Retrieve the full snapshot list
	char *err = snapshot_enumerate(&snapshots, &nsnapshots);
	if (err != NULL) {
		fprintf(stderr, "snapshot list: %s\n", err);
		exit(1);
	}

	// Narrow it down
	struct snapshot *wanted = NULL;
	int nwanted = 0;
	for (struct snapshot *snap = snapshots; snap < snapshots + nsnapshots; snap++) {
		for (int i = 0; i < nglobs; i++) {
			char *glob = globs[i];
			if (glob == NULL)
				continue;
			if (db_glob(glob, snap->dbname)) {
				struct snapshot *w = push_snapshot(&wanted, &nwanted);
				copy_snapshot(w, snap);
				break;
			}
		}
	}

	int width = 0;
	for (struct snapshot *snap = wanted; snap < wanted + nwanted; snap++) {
		int w = strlen(snap->name);
		width = (width >= w) ? width : w;
	}

	printf("%-*s    %-25s    %s\n", width, "name", "time", "size");

	char *name_buf = malloc(width + 100);
	for (struct snapshot *snap = wanted; snap < wanted + nwanted; snap++) {
		char tm_buf[100];
		struct tm tm;
		// format name
		char *name;
		if (snap == wanted || strcmp(snap[0].dbname, snap[-1].dbname) != 0) {
			// subheader, show whole name
			name = snap->name;
		} else {
			// continuation, show only sequence number
			strcpy(name_buf, snap->name);
			for (size_t i = 0; i < strlen(snap->dbname); i++)
				name_buf[i] = ' ';
			name = name_buf;
		}
		// format time
		localtime_r(&snap->time, &tm);
		strftime(tm_buf, sizeof(tm_buf), "%a %Y-%m-%d %H:%M:%S", &tm);
		// format size
		double size = snap->size;
		char *units[] = {"B", "KiB", "MiB", "GiB", "TiB", NULL};
		char **unit = &units[0];
		while (size >= 1024 && unit[1] != NULL) {
			size /= 1024;
			unit++;
		}
		printf("%-*s    %-25s    %.1f %s\n", width, name, tm_buf, size, *unit);
		// output
	}

	free(name_buf);
	free_snapshots(wanted, nwanted);
	free_snapshots(snapshots, nsnapshots);
}

static void
snapshot_restore_file(char *sourcefile, char *dbname)
{
	char *ret;
	char *out;
	char *merocmd = malloc(100 + strlen(sourcefile));

	if (!monetdb_quiet) {
		printf("Restore '%s' from '%s'... ", dbname, sourcefile);
		fflush(stdout);
	}

	sprintf(merocmd, "snapshot restore adhoc %s", sourcefile);
	ret = control_send(&out, mero_host, mero_port, dbname, merocmd, 0, mero_pass);
	free(merocmd);

	if (ret != NULL) {
		fprintf(stderr, "snapshot restore: %s", ret);
		exit(2);
	}
	if (strcmp(out, "OK") == 0) {
		if (!monetdb_quiet) {
			printf("done\n");
		}
	} else {
		fprintf(stderr, "failed: %s\n", out);
		free(out);
		exit(1);
	}
	free(out);
}

static void
snapshot_destroy_file(char *path)
{
	char *ret;
	char *out = NULL;
	char *merocmd = malloc(100 + strlen(path));

	sprintf(merocmd, "snapshot destroy %s", path);
	ret = control_send(&out, mero_host, mero_port, "", merocmd, 0, mero_pass);
	if (ret != NULL) {
		fprintf(stderr, "snapshot destroy %s failed: %s", path, ret);
		exit(2);
	}

	if (strcmp(out, "OK") != 0) {
		fprintf(stderr, "snapshot destroy %s: %s\n", path, out);
		free(out);
		exit(2);
	}

	if (!monetdb_quiet)
		printf("Destroyed %s\n", path);

	free(out);
	free(merocmd);
}

static void
command_snapshot_create(int argc, char *argv[])
{
	char *targetfile = NULL;
	char *err;

	/* walk through the arguments and hunt for "options" */
	for (int i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--") == 0) {
			argv[i] = NULL;
			break;
		}
		if (argv[i][0] == '-') {
			if (argv[i][1] == 't') {
				if (argv[i][2] != '\0') {
					targetfile = &argv[i][2];
					argv[i] = NULL;
				} else if (i + 1 < argc && argv[i+1][0] != '-') {
					targetfile = argv[i+1];
					argv[i] = NULL;
					argv[i+1] = NULL;
					i++;
				} else {
					fprintf(stderr, "snapshot: -t needs an argument\n");
					command_help(argc + 2, &argv[-2]);
					exit(1);
				}
			} else {
				fprintf(stderr, "snapshot create: unknown option: %s\n", argv[i]);
				command_help(argc + 2, &argv[-2]);
				exit(1);
			}
		}
	}

	/* Look up the databases to snapshot */
	sabdb *all = NULL;
	err = MEROgetStatus(&all, NULL);
	if (err != NULL) {
		fprintf(stderr, "snapshot: %s\n", err);
		free(err);
		exit(2);
	}
	sabdb *databases = globMatchDBS(NULL, argc, argv, &all, "snapshot");
	msab_freeStatus(&all);
	if (databases == NULL)
		exit(1);

	/* Go do the work */
	if (targetfile != NULL) {
		if (databases->next != NULL) {
			fprintf(stderr, "snapshot: -t only allows a single database\n");
			exit(1);
		}
		snapshot_create_adhoc(databases, targetfile);
	} else {
		snapshot_create_automatic(databases);
	}

	msab_freeStatus(&databases);
}


static void
command_snapshot_list(int argc, char *argv[])
{
	/* walk through the arguments and hunt for "options" */
	for (int i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--") == 0) {
			argv[i] = NULL;
			break;
		}
		if (argv[i][0] == '-') {
			fprintf(stderr, "snapshot create: unknown option: %s\n", argv[i]);
			command_help(argc + 2, &argv[-2]);
			exit(1);
		}
	}

	if (argc == 1) {
		char *args[] = {"*"};
		snapshot_list(1, args);
	}
	else
		snapshot_list(argc - 1, &argv[1]);
}


static void
command_snapshot_restore(int argc, char *argv[])
{
	int force = 0;
	char *snapid = NULL;
	char *snapfile = NULL;
	char *dbname = NULL;

	/* walk through the arguments and hunt for "options" */
	for (int i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--") == 0) {
			argv[i] = NULL;
			break;
		}
		if (argv[i][0] == '-') {
			if (argv[i][1] == 'f') {
				force = 1;
				argv[i] = 0;
			} else {
				fprintf(stderr, "snapshot restore: unknown option: %s\n", argv[i]);
				command_help(argc + 2, &argv[-2]);
				exit(1);
			}
		}
	}

	/* Find snapid and dbname */
	for (int i = 1; i < argc; i++) {
		if (argv[i] == NULL)
			continue;
		if (snapid == NULL)
			snapid = argv[i];
		else if (dbname == NULL)
			dbname = strdup(argv[i]);
		else {
			fprintf(stderr, "snapshot restore: unexpected argument: %s\n", argv[i]);
			command_help(argc + 2, &argv[-2]);
			exit(1);
		}
	}

	if (snapid == NULL) {
		fprintf(stderr, "snapshot restore: snapid is mandatory\n");
		command_help(argc + 2, &argv[-2]);
		exit(1);
	}

	// is snapid a file name?
	if (strchr(snapid, DIR_SEP) != NULL) {
		// filename, so dbname argument is mandatory
		if (dbname == NULL) {
			fprintf(stderr, "snapshot restore: dbname is mandatory\n");
			exit(1);
		}
		snapfile = strdup(snapid);
	} else {
		// it must be <dbname>@<seqno> then.
		if (strchr(snapid, '@') == NULL) {
			fprintf(stderr, "snapshot restore: please provide either a snapshot id or a filename\n");
			exit(1);
		}
		struct snapshot *snapshots = NULL;
		int nsnapshots = 0;
		char *err = snapshot_enumerate(&snapshots, &nsnapshots);
		if (err != NULL) {
			fprintf(stderr, "snapshot restore: %s", err);
			exit(2);
		}
		struct snapshot *snap = NULL;
		struct snapshot *s;
		for (int i = 0; i < nsnapshots; i++) {
			s = &snapshots[i];
			if (strcmp(s->name, snapid) == 0) {
				// found it
				snap = s;
				break;
			}
		}
		if (snap == NULL) {
			fprintf(stderr, "snapshot restore: unknown snapshot '%s'\n", snapid);
			exit(1);
		}
		if (dbname == NULL)
			dbname = strdup(snap->dbname);
		snapfile = strdup(snap->path);

		free_snapshots(snapshots, nsnapshots);
	}

	// check if the database exists
	sabdb *db = NULL;
	char *e = MEROgetStatus(&db, dbname); // ignore errors
	free(e);

	if (db != NULL && !force) {
		char answ;
		printf("you are about to overwrite database '%s'.\n", db->dbname);
		printf("ALL data in this database will be lost, are you sure? [y/N] ");
		if (scanf("%c", &answ) < 1 || (answ != 'y' && answ != 'Y')) {
			printf("aborted\n");
			exit(1);
		}
	}

	if (db)
		msab_freeStatus(&db);

	snapshot_restore_file(snapfile, dbname);
	free(snapfile);
	free(dbname);
}


static void
command_snapshot_destroy(int argc, char *argv[])
{
	int force = 0;
	long retain = -1;
	struct snapshot *snapshots = NULL;
	int nsnapshots = 0;
	char *hitlist = NULL;


	/* walk through the arguments and hunt for "options" */
	for (int i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--") == 0) {
			argv[i] = NULL;
			break;
		}
		if (argv[i][0] == '-') {
			if (argv[i][1] == 'f') {
				force = 1;
				argv[i] = 0;
			} else if (argv[i][1] == 'r') {
				char *n_str;
				if (argv[i][2] != '\0') {
					n_str = &argv[i][2];
					argv[i] = NULL;
				} else if (i + 1 < argc && argv[i+1][0] != '-') {
					n_str = argv[i+1];
					argv[i] = NULL;
					argv[i+1] = NULL;
					i++;
				} else {
					fprintf(stderr, "snapshot: -t needs an argument\n");
					command_help(argc + 2, &argv[-2]);
					exit(1);
				}
				char *end = NULL;
				retain = strtol(n_str, &end, 10);
				if (*end != '\0' || retain < 0) {
					fprintf(stderr, "snapshot: -r takes a nonnegative integer\n");
					command_help(argc + 2, &argv[-2]);
					exit(1);
				}
			} else {
				fprintf(stderr, "snapshot destroy: unknown option: %s\n", argv[i]);
				command_help(argc + 2, &argv[-2]);
				exit(1);
			}
		}
	}

	char *err = snapshot_enumerate(&snapshots, &nsnapshots);
	if (err != NULL) {
		fprintf(stderr, "snapshot: %s\n", err);
		exit(2);
	}

	/* this is where we will mark the snapshots to be destroyed */
	hitlist = calloc(nsnapshots, 1);

	/* Go over the arguments and mark the snapshots to be destroyed.
	 * Relies on the snapshots array to be sorted correctly
	 */
	for (int a = 1; a< argc; a++) {
		char *arg = argv[a];
		if (arg == NULL)
			continue;
		bool matched_something = false;
		for (int s = 0; s < nsnapshots; s++) {
			struct snapshot *snap = &snapshots[s];
			if (retain < 0) {
				// args are snapshot id's
				if (strcmp(arg, snap->name) != 0)
					continue;
				matched_something = true; // only on full match
			} else {
				// args are database names.
				if (!db_glob(arg, snap->dbname))
					continue;
				matched_something = true; // already on db name match
				int seqno = atoi(strrchr(snap->name, '@') + 1);
				if (seqno <= retain)
					continue;
			}
			// if we get here it must be destroyed
			hitlist[s] = 1;
		}
		if (!matched_something) {
			fprintf(stderr, "snapshot destroy: no matching snapshots: %s\n", arg);
			exit(1);
		}
	}

	int nhits = 0;
	for (int i = 0; i < nsnapshots; i++)
		nhits += (hitlist[i] != 0);
	if (nhits == 0)
		goto end;

	if (nhits > 0 && !force) {
		printf("About to destroy %d snapshots:\n", nhits);
		for (int i = 0; i < nsnapshots; i++)
			if (hitlist[i]) {
				char buf[100];
				struct snapshot *snap = &snapshots[i];
				struct tm tm = {0};
				localtime_r(&snap->time, &tm);
				strftime(buf, sizeof(buf),"%a %Y-%m-%d %H:%M:%S", &tm);
				printf("    %-25s %s\n", snap->name, buf);
			}
		char answ;
		printf("ALL data in %s will be lost, are you sure? [y/N] ",
			nhits > 1 ? "these snapshots": "this snapshot");
		if (scanf("%c", &answ) < 1 || (answ != 'y' && answ != 'Y')) {
			printf("aborted\n");
			exit(1);
		}
	}

	for (int i = 0; i < nsnapshots; i++)
		if (hitlist[i] != 0)
			snapshot_destroy_file(snapshots[i].path);

end:
	free_snapshots(snapshots, nsnapshots);
	free(hitlist);
}

static void
command_snapshot_write_cb(const void *buf, size_t count, void *private) {
	FILE *f = (FILE*)private;
	fwrite(buf, 1, count, f);
}

static void
command_snapshot_write(int argc, char *argv[])
{
	char *msg;
	char *out = NULL;

	if (argc != 2) {
		command_help(argc + 2, &argv[-2]);
		exit(1);
	}

	if (isatty(fileno(stdout))) {
		fprintf(stderr, "Refusing to write binary data to tty\n");
		exit(2);
	}

	char *dbname = argv[1];
	sabdb *stats = NULL;
	msg = MEROgetStatus(&stats, dbname);
	if (msg) {
		fprintf(stderr, "snapshot: %s\n", msg);
		free(msg);
		exit(2);
	}
	if (!stats) {
		fprintf(stderr, "snapshot: database '%s' not found\n", dbname);
		exit(2);
	}
	msab_freeStatus(&stats);

	char *merocmd = "snapshot stream";

	msg = control_send_callback(
			&out, mero_host, mero_port, dbname,
			merocmd, command_snapshot_write_cb, stdout,
			mero_pass);
	if (msg) {
		fprintf(stderr, "snapshot: database '%s': %s\n", dbname, msg);
		exit(2);
	}
	if (out == NULL) {
		fprintf(stderr, "snapshot: database '%s': unknown error\n", dbname);
		exit(2);
	}
	if (strcmp(out, "OK") != 0) {
		fprintf(stderr, "snapshot: database '%s': %s\n", dbname, out);
		exit(2);
	}
	free(out);
}

static void
command_snapshot(int argc, char *argv[])
{
	if (argc <= 1) {
		/* print help message for this command */
		command_help(argc + 1, &argv[-1]);
		exit(1);
	}
	if (argv[1][0] == '-') {
		/* print help message for this command */
		command_help(argc + 1, &argv[-1]);
		exit(1);
	}

	/* pick the right subcommand */
	if (strcmp(argv[1], "create") == 0) {
		command_snapshot_create(argc - 1, &argv[1]);
	} else if (strcmp(argv[1], "list") == 0) {
		command_snapshot_list(argc - 1, &argv[1]);
	} else if (strcmp(argv[1], "restore") == 0) {
		command_snapshot_restore(argc - 1, &argv[1]);
	} else if (strcmp(argv[1], "destroy") == 0) {
		command_snapshot_destroy(argc - 1, &argv[1]);
	} else if (strcmp(argv[1], "write") == 0) {
		command_snapshot_write(argc - 1, &argv[1]);
	} else {
		/* print help message for this command */
		command_help(argc - 1, &argv[1]);
	}
}

int
main(int argc, char *argv[])
{
	char buf[1024];
	int i;
	int retval = 0;
#ifdef TIOCGWINSZ
	struct winsize ws;

	if (ioctl(fileno(stdout), TIOCGWINSZ, &ws) == 0 && ws.ws_col > 0)
		TERMWIDTH = ws.ws_col;
#endif

	/* Start handling the arguments.
	 * monetdb [monetdb_options] command [options] [database [...]]
	 * this means we first scout for monetdb_options which stops as soon
	 * as we find a non-option argument, which then must be command */

	/* first handle the simple no argument case */
	if (argc <= 1) {
		command_help(0, NULL);
		return(1);
	}

	/* handle monetdb_options */
	for (i = 1; argc > i && argv[i][0] == '-'; i++) {
		switch (argv[i][1]) {
			case 'v':
				command_version();
			return(0);
			case 'q':
				monetdb_quiet = true;
			break;
			case 'h':
				if (strlen(&argv[i][2]) > 0) {
					mero_host = &argv[i][2];
				} else {
					if (i + 1 < argc) {
						mero_host = argv[++i];
					} else {
						fprintf(stderr, "monetdb: -h needs an argument\n");
						return(1);
					}
				}
			break;
			case 'p':
				if (strlen(&argv[i][2]) > 0) {
					mero_port = atoi(&argv[i][2]);
				} else {
					if (i + 1 < argc) {
						mero_port = atoi(argv[++i]);
					} else {
						fprintf(stderr, "monetdb: -p needs an argument\n");
						return(1);
					}
				}
			break;
			case 'P':
				/* take care we remove the password from argv so it
				 * doesn't show up in e.g. ps -ef output */
				if (strlen(&argv[i][2]) > 0) {
					mero_pass = strdup(&argv[i][2]);
					memset(&argv[i][2], 0, strlen(mero_pass));
				} else {
					if (i + 1 < argc) {
						mero_pass = strdup(argv[++i]);
						memset(argv[i], 0, strlen(mero_pass));
					} else {
						fprintf(stderr, "monetdb: -P needs an argument\n");
						return(1);
					}
				}
			break;
			case '-':
				/* skip -- */
				if (argv[i][2] == '\0')
					break;
				if (strcmp(&argv[i][2], "version") == 0) {
					command_version();
					return(0);
				} else if (strcmp(&argv[i][2], "help") == 0) {
					command_help(0, NULL);
					return(0);
				}
				/* fall through */
			default:
				fprintf(stderr, "monetdb: unknown option: %s\n", argv[i]);
				command_help(0, NULL);
				return(1);
			break;
		}
	}

	/* check consistency of -h -p and -P args */
	if (mero_pass != NULL && (mero_host == NULL || *mero_host == '/')) {
		fprintf(stderr, "monetdb: -P requires -h to be used with a TCP hostname\n");
		exit(1);
	} else if (mero_host != NULL && *mero_host != '/' && mero_pass == NULL) {
		fprintf(stderr, "monetdb: -h requires -P to be used\n");
		exit(1);
	}

	/* see if we still have arguments at this stage */
	if (i >= argc) {
		command_help(0, NULL);
		return(1);
	}

	/* commands that do not need merovingian to be running */
	if (strcmp(argv[i], "help") == 0) {
		command_help(argc - i, &argv[i]);
		return(0);
	} else if (strcmp(argv[i], "version") == 0) {
		command_version();
		return(0);
	}

	/* use UNIX socket if no hostname given */
	if (mero_host == NULL || *mero_host == '/') {
		/* a socket looks like /tmp/.s.merovingian.<tcpport>, try
		 * finding such port.  If mero_host is set, it is the location
		 * where we should search, which defaults to '/tmp' */
		char *err;
		if (mero_host == NULL)
			mero_host = "/tmp";
		/* first try the port given (or else its default) */
		snprintf(buf, sizeof(buf), "%s/.s.merovingian.%d",
			 mero_host, mero_port == -1 ? MAPI_PORT : mero_port);
		if ((err = control_ping(buf, -1, NULL)) == NULL) {
			mero_host = buf;
		} else {
			/* if port wasn't given, we can try and search
			 * for available sockets */
			if (mero_port == -1) {
				DIR *d;
				struct dirent *e;
				struct stat s;

				d = opendir(mero_host);
				if (d == NULL) {
					fprintf(stderr, "monetdb: %s: %s\n",
							mero_host, strerror(errno));
					exit(1);
				}
				while ((e = readdir(d)) != NULL) {
					if (strncmp(e->d_name, ".s.merovingian.", 15) != 0)
						continue;
					snprintf(buf, sizeof(buf), "%s/%s", mero_host, e->d_name);
					if (stat(buf, &s) == -1)
						continue;
					if (S_ISSOCK(s.st_mode)) {
						char *nerr;
						if ((nerr = control_ping(buf, -1, NULL)) == NULL) {
							mero_host = buf;
							free(err);
							err = NULL;
							break;
						}
						free(nerr);
					}
				}
				closedir(d);
			}
		}

		if (mero_host != buf) {
			fprintf(stderr, "monetdb: %s\n", err);
			exit(1);
		}
		/* don't confuse control_send later on */
		mero_port = -1;
	}
	/* for TCP connections */
	if (mero_host != NULL && *mero_host != '/' && mero_port == -1)
		mero_port = MAPI_PORT;

	/* handle regular commands */
	if (strcmp(argv[i], "create") == 0) {
		command_create(argc - i, &argv[i]);
	} else if (strcmp(argv[i], "destroy") == 0) {
		command_destroy(argc - i, &argv[i]);
	} else if (strcmp(argv[i], "lock") == 0) {
		retval = command_lock(argc - i, &argv[i]);
	} else if (strcmp(argv[i], "release") == 0) {
		retval = command_release(argc - i, &argv[i]);
	} else if (strcmp(argv[i], "profilerstart") == 0) {
		retval = command_profilerstart(argc - i, &argv[i]);
	} else if (strcmp(argv[i], "profilerstop") == 0) {
		retval = command_profilerstop(argc - i, &argv[i]);
	} else if (strcmp(argv[i], "status") == 0) {
		retval = command_status(argc - i, &argv[i]);
	} else if (strcmp(argv[i], "start") == 0) {
		retval = command_startstop(argc - i, &argv[i], START);
	} else if (strcmp(argv[i], "stop") == 0) {
		retval = command_startstop(argc - i, &argv[i], STOP);
	} else if (strcmp(argv[i], "kill") == 0) {
		retval = command_startstop(argc - i, &argv[i], KILL);
	} else if (strcmp(argv[i], "set") == 0) {
		command_set(argc - i, &argv[i], SET);
	} else if (strcmp(argv[i], "get") == 0) {
		retval = command_get(argc - i, &argv[i]);
	} else if (strcmp(argv[i], "inherit") == 0) {
		command_set(argc - i, &argv[i], INHERIT);
	} else if (strcmp(argv[i], "discover") == 0) {
		command_discover(argc - i, &argv[i]);
	} else if (strcmp(argv[i], "snapshot") == 0) {
		command_snapshot(argc - i, &argv[i]);
	} else {
		fprintf(stderr, "monetdb: unknown command: %s\n", argv[i]);
		command_help(0, NULL);
		retval = 1;
	}

	if (mero_pass != NULL)
		free(mero_pass);

	return retval;
}

/* vim:set ts=4 sw=4 noexpandtab: */
