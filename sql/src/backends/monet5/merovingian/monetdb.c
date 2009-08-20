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
#include "database.h"
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
		printf("Usage: monetdb create database [database ...]\n");
		printf("  Initialises a new database in the MonetDB Server.  A\n");
		printf("  database created with this command makes it available\n");
		printf("  for use, however in maintenance mode (see monetdb lock).\n");
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


#include "monetdb_status.c"
#include "monetdb_discover.c"
#include "monetdb_merocom.c"
#include "monetdb_set.c"
#include "monetdb_get.c"

static void
command_create(int argc, char *argv[])
{
	int i;
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
			argv[i] = NULL;
			break;
		}
		if (argv[i][0] == '-') {
			fprintf(stderr, "create: unknown option: %s\n", argv[i]);
			command_help(argc + 1, &argv[-1]);
			exit(1);
		}
	}

	/* do for each listed database */
	for (i = 1; i < argc; i++) {
		char *ret;
		
		if (argv[i] == NULL)
			continue;

		ret = db_create(dbfarm, argv[i]);

		if (ret == NULL) {
			printf("successfully created database '%s' "
					"in maintenance mode\n", argv[i]);
		} else {
			fprintf(stderr, "create: %s\n", ret);
			free(ret);

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
		char* ret;
		
		if (argv[i] == NULL)
			continue;

		ret = db_destroy(argv[i]);

		if (ret == NULL) {
			printf("successfully destroyed database '%s'\n", argv[i]);
		} else {
			fprintf(stderr, "destroy: %s\n", ret);
			free(ret);
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

#include "monetdb_lock.c"
#include "monetdb_release.c"


int
main(int argc, char *argv[])
{
	str p, prefix;
	FILE *cnf = NULL;
	char buf[1024];
	int fd;
	confkeyval ckv[] = {
		{"prefix",             GDKstrdup(MONETDB5_PREFIX)},
		{"gdk_dbfarm",         NULL},
		{"gdk_nr_threads",     NULL},
		{"sql_logdir",         NULL},
		{"mero_doproxy",       GDKstrdup("yes")},
		{"mero_discoveryport", NULL},
		{ NULL,                NULL}
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
		if (strcmp(kv->val, "yes") == 0 ||
				strcmp(kv->val, "true") == 0 ||
				strcmp(kv->val, "1") == 0)
		{
			GDKfree(kv->val);
			kv->val = GDKstrdup("proxy");
		} else {
			GDKfree(kv->val);
			kv->val = GDKstrdup("redirect");
		}
		kv = findConfKey(ckv, "mero_discoveryport");
		kv->key = "shared";
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
