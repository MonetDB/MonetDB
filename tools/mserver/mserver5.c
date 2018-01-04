/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include <stdio.h>
#include <errno.h>
#include <string.h> /* strerror */
#include <locale.h>
#include "monet_options.h"
#include "mal.h"
#include "mal_session.h"
#include "mal_import.h"
#include "mal_client.h"
#include "mal_function.h"
#include "monet_version.h"
#include "mal_authorize.h"
#include "msabaoth.h"
#include "mutils.h"

#ifdef HAVE_LIBGEN_H
#include <libgen.h>
#endif

#ifndef HAVE_GETOPT_LONG
#  include "monet_getopt.h"
#else
# ifdef HAVE_GETOPT_H
#  include "getopt.h"
# endif
#endif

#ifdef _MSC_VER
#include <Psapi.h>      /* for GetModuleFileName */
#include <crtdbg.h>	/* for _CRT_ERROR, _CRT_ASSERT */
#endif

#ifdef _CRTDBG_MAP_ALLOC
/* Windows only:
   our definition of new and delete clashes with the one if
   _CRTDBG_MAP_ALLOC is defined.
 */
#undef _CRTDBG_MAP_ALLOC
#endif

#if defined(_MSC_VER) && _MSC_VER >= 1400
#define getcwd _getcwd
#endif

#ifdef HAVE_CONSOLE
static int monet_daemon;
#endif

/* NEEDED? */
#if defined(_MSC_VER) && defined(__cplusplus)
#include <eh.h>
void
mserver_abort()
{
	fprintf(stderr, "\n! mserver_abort() was called by terminate(). !\n");
	fflush(stderr);
	exit(0);
}
#endif

#ifdef _MSC_VER
static void
mserver_invalid_parameter_handler(
	const wchar_t *expression,
	const wchar_t *function,
	const wchar_t *file,
	unsigned int line,
	uintptr_t reserved)
{
	(void) expression;
	(void) function;
	(void) file;
	(void) line;
	(void) reserved;
	/* the essential bit of this function is that it returns:
	 * we don't want the server to quit when a function is called
	 * with an invalid parameter */
}
#endif

__declspec(noreturn) static void usage(char *prog, int xit)
	__attribute__((__noreturn__));

static void
usage(char *prog, int xit)
{
	fprintf(stderr, "Usage: %s [options] [scripts]\n", prog);
	fprintf(stderr, "    --dbpath=<directory>      Specify database location\n");
	fprintf(stderr, "    --dbextra=<directory>     Directory for transient BATs\n");
	fprintf(stderr, "    --dbinit=<stmt>           Execute statement at startup\n");
	fprintf(stderr, "    --config=<config_file>    Use config_file to read options from\n");
	fprintf(stderr, "    --daemon=yes|no           Do not read commands from standard input [no]\n");
	fprintf(stderr, "    --single-user             Allow only one user at a time\n");
	fprintf(stderr, "    --readonly                Safeguard database\n");
	fprintf(stderr, "    --set <option>=<value>    Set configuration option\n");
	fprintf(stderr, "    --help                    Print this list of options\n");
	fprintf(stderr, "    --version                 Print version and compile time info\n");

	fprintf(stderr, "The debug, testing & trace options:\n");
	fprintf(stderr, "     --threads\n");
	fprintf(stderr, "     --memory\n");
	fprintf(stderr, "     --io\n");
	fprintf(stderr, "     --heaps\n");
	fprintf(stderr, "     --properties\n");
	fprintf(stderr, "     --transactions\n");
	fprintf(stderr, "     --modules\n");
	fprintf(stderr, "     --algorithms\n");
	fprintf(stderr, "     --performance\n");
	fprintf(stderr, "     --optimizers\n");
	fprintf(stderr, "     --trace\n");
	fprintf(stderr, "     --forcemito\n");
	fprintf(stderr, "     --debug=<bitmask>\n");

	exit(xit);
}

/*
 * Collect some global system properties to relate performance results later
 */
static void
monet_hello(void)
{
	dbl sz_mem_h;
	char  *qc = " kMGTPE";
	int qi = 0;
	size_t len;

	monet_memory = MT_npages() * MT_pagesize();
	sz_mem_h = (dbl) monet_memory;
	while (sz_mem_h >= 1000.0 && qi < 6) {
		sz_mem_h /= 1024.0;
		qi++;
	}

	printf("# MonetDB 5 server v" VERSION);
	/* coverity[pointless_string_compare] */
	if (strcmp(MONETDB_RELEASE, "unreleased") == 0)
		printf("\n# This is an unreleased version");
	else
		printf(" \"%s\"", MONETDB_RELEASE);
	printf("\n# Serving database '%s', using %d thread%s\n",
			GDKgetenv("gdk_dbname"),
			GDKnr_threads, (GDKnr_threads != 1) ? "s" : "");
	printf("# Compiled for %s/" SZFMT "bit%s\n",
			HOST, sizeof(ptr) * 8,
#ifdef HAVE_HGE
			" with 128bit integers"
#else
			""
#endif
			);
	printf("# Found %.3f %ciB available main-memory.\n",
			sz_mem_h, qc[qi]);
#ifdef MONET_GLOBAL_DEBUG
	printf("# Database path:%s\n", GDKgetenv("gdk_dbpath"));
	printf("# Module path:%s\n", GDKgetenv("monet_mod_path"));
#endif
	printf("# Copyright (c) 1993 - July 2008 CWI.\n");
	printf("# Copyright (c) August 2008 - 2018 MonetDB B.V., all rights reserved\n");
	printf("# Visit https://www.monetdb.org/ for further information\n");

	// The properties shipped through the performance profiler
	len = snprintf(monet_characteristics, sizeof(monet_characteristics)-1, "{\n\"version\":\"%s\",\n", VERSION);
	len += snprintf(monet_characteristics + len, sizeof(monet_characteristics)-1-len, "\"release\":\"%s\",\n", MONETDB_RELEASE);
	len += snprintf(monet_characteristics + len, sizeof(monet_characteristics)-1-len, "\"host\":\"%s\",\n", HOST);
	len += snprintf(monet_characteristics + len, sizeof(monet_characteristics)-1-len, "\"threads\":\"%d\",\n", GDKnr_threads);
	len += snprintf(monet_characteristics + len, sizeof(monet_characteristics)-1-len, "\"memory\":\"%.3f %cB\",\n", sz_mem_h, qc[qi]);
	len += snprintf(monet_characteristics + len, sizeof(monet_characteristics)-1-len, "\"oid\":\""SZFMT"\",\n", sizeof(oid) *8);
	len += snprintf(monet_characteristics + len, sizeof(monet_characteristics)-1-len, "\"packages\":[");
	// add the compiled in package names
#ifdef HAVE_HGE
	len += snprintf(monet_characteristics + len, sizeof(monet_characteristics)-1-len, "\"%s\"","huge");
#endif
	len += snprintf(monet_characteristics + len, sizeof(monet_characteristics)-1-len, "]\n}");
}

static str
absolute_path(str s)
{
	if (!MT_path_absolute(s)) {
		str ret = (str) GDKmalloc(strlen(s) + strlen(monet_cwd) + 2);

		if (ret)
			sprintf(ret, "%s%c%s", monet_cwd, DIR_SEP, s);
		return ret;
	}
	return GDKstrdup(s);
}

#define BSIZE 8192

static int
monet_init(opt *set, int setlen)
{
	/* determine Monet's kernel settings */
	if (!GDKinit(set, setlen))
		return 0;

#ifdef HAVE_CONSOLE
	monet_daemon = 0;
	if (GDKgetenv_isyes("monet_daemon")) {
		monet_daemon = 1;
#ifdef HAVE_SETSID
		setsid();
#endif
	}
#endif
	monet_hello();
	return 1;
}

static void emergencyBreakpoint(void)
{
	/* just a handle to break after system initialization for GDB */
}

static void
handler(int sig)
{
	(void) sig;
	mal_exit();
}

int
main(int argc, char **av)
{
	char *prog = *av;
	opt *set = NULL;
	int i, grpdebug = 0, debug = 0, setlen = 0, listing = 0;
	str dbinit = NULL;
	str err = MAL_SUCCEED;
	char prmodpath[1024];
	char *modpath = NULL;
	char *binpath = NULL;
	str *monet_script;
	char *dbpath = NULL;
	char *dbextra = NULL;
	static struct option long_options[] = {
		{ "config", 1, 0, 'c' },
		{ "dbpath", 1, 0, 0 },
		{ "dbextra", 1, 0, 0 },
		{ "dbinit", 1, 0, 0 } ,
		{ "daemon", 1, 0, 0 },
		{ "debug", 2, 0, 'd' },
		{ "help", 0, 0, '?' },
		{ "version", 0, 0, 0 },
		{ "readonly", 0, 0, 'r' },
		{ "single-user", 0, 0, 0 },
		{ "set", 1, 0, 's' },
		{ "threads", 0, 0, 0 },
		{ "memory", 0, 0, 0 },
		{ "properties", 0, 0, 0 },
		{ "io", 0, 0, 0 },
		{ "transactions", 0, 0, 0 },
		{ "trace", 2, 0, 't' },
		{ "modules", 0, 0, 0 },
		{ "algorithms", 0, 0, 0 },
		{ "optimizers", 0, 0, 0 },
		{ "performance", 0, 0, 0 },
		{ "forcemito", 0, 0, 0 },
		{ "heaps", 0, 0, 0 },
		{ 0, 0, 0, 0 }
	};

#if defined(_MSC_VER) && defined(__cplusplus)
	set_terminate(mserver_abort);
#endif
#ifdef _MSC_VER
	_CrtSetReportMode(_CRT_ERROR, 0);
	_CrtSetReportMode(_CRT_ASSERT, 0);
	_set_invalid_parameter_handler(mserver_invalid_parameter_handler);
#ifdef _TWO_DIGIT_EXPONENT
	_set_output_format(_TWO_DIGIT_EXPONENT);
#endif
#endif
	if (setlocale(LC_CTYPE, "") == NULL) {
		GDKfatal("cannot set locale\n");
	}

	if (getcwd(monet_cwd, PATHLENGTH - 1) == NULL) {
		perror("pwd");
		fprintf(stderr,"monet_init: could not determine current directory\n");
		exit(-1);
	}

	/* retrieve binpath early (before monet_init) because some
	 * implementations require the working directory when the binary was
	 * called */
	binpath = get_bin_path();

	if (!(setlen = mo_builtin_settings(&set)))
		usage(prog, -1);

	for (;;) {
		int option_index = 0;

		int c = getopt_long(argc, av, "c:d::rs:t::?",
				long_options, &option_index);

		if (c == -1)
			break;

		switch (c) {
		case 0:
			if (strcmp(long_options[option_index].name, "dbpath") == 0) {
				size_t optarglen = strlen(optarg);
				/* remove trailing directory separator */
				while (optarglen > 0 &&
				       (optarg[optarglen - 1] == '/' ||
					optarg[optarglen - 1] == '\\'))
					optarg[--optarglen] = '\0';
				dbpath = absolute_path(optarg);
				if( dbpath == NULL)
					fprintf(stderr, "#error: can not allocate memory for dbpath\n");
				else
					setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_dbpath", dbpath);
				break;
			}
			if (strcmp(long_options[option_index].name, "dbextra") == 0) {
				if (dbextra)
					fprintf(stderr, "#warning: ignoring multiple --dbextra arguments\n");
				else
					dbextra = optarg;
				break;
			}
			if (strcmp(long_options[option_index].name, "dbinit") == 0) {
				if (dbinit)
					fprintf(stderr, "#warning: ignoring multiple --dbinit argument\n");
				else
					dbinit = optarg;
				break;
			}
#ifdef HAVE_CONSOLE
			if (strcmp(long_options[option_index].name, "daemon") == 0) {
				setlen = mo_add_option(&set, setlen, opt_cmdline, "monet_daemon", optarg);
				break;
			}
#endif
			if (strcmp(long_options[option_index].name, "single-user") == 0) {
				setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_single_user", "yes");
				break;
			}
			if (strcmp(long_options[option_index].name, "version") == 0) {
				monet_version();
				exit(0);
			}
			/* debugging options */
			if (strcmp(long_options[option_index].name, "properties") == 0) {
				grpdebug |= GRPproperties;
				break;
			}
			if (strcmp(long_options[option_index].name, "algorithms") == 0) {
				grpdebug |= GRPalgorithms;
				break;
			}
			if (strcmp(long_options[option_index].name, "optimizers") == 0) {
				grpdebug |= GRPoptimizers;
				break;
			}
			if (strcmp(long_options[option_index].name, "forcemito") == 0) {
				grpdebug |= GRPforcemito;
				break;
			}
			if (strcmp(long_options[option_index].name, "performance") == 0) {
				grpdebug |= GRPperformance;
				break;
			}
			if (strcmp(long_options[option_index].name, "io") == 0) {
				grpdebug |= GRPio;
				break;
			}
			if (strcmp(long_options[option_index].name, "memory") == 0) {
				grpdebug |= GRPmemory;
				break;
			}
			if (strcmp(long_options[option_index].name, "modules") == 0) {
				grpdebug |= GRPmodules;
				break;
			}
			if (strcmp(long_options[option_index].name, "transactions") == 0) {
				grpdebug |= GRPtransactions;
				break;
			}
			if (strcmp(long_options[option_index].name, "threads") == 0) {
				grpdebug |= GRPthreads;
				break;
			}
			if (strcmp(long_options[option_index].name, "trace") == 0) {
				mal_trace = 1;
				break;
			}
			if (strcmp(long_options[option_index].name, "heaps") == 0) {
				grpdebug |= GRPheaps;
				break;
			}
			usage(prog, -1);
		/* not reached */
		case 'c':
			/* coverity[var_deref_model] */
			setlen = mo_add_option(&set, setlen, opt_cmdline, "config", optarg);
			break;
		case 'd':
			if (optarg) {
				char *endarg;
				debug |= strtol(optarg, &endarg, 10);
				if (*endarg != '\0') {
					fprintf(stderr, "ERROR: wrong format for --debug=%s\n",
							optarg);
					usage(prog, -1);
				}
			} else {
				debug |= 1;
			}
			break;
		case 'r':
			setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_readonly", "yes");
			break;
		case 's': {
			/* should add option to a list */
			/* coverity[var_deref_model] */
			char *tmp = strchr(optarg, '=');

			if (tmp) {
				*tmp = '\0';
				setlen = mo_add_option(&set, setlen, opt_cmdline, optarg, tmp + 1);
			} else
				fprintf(stderr, "ERROR: wrong format %s\n", optarg);
			}
			break;
		case 't':
			mal_trace = 1;
			break;
		case '?':
			/* a bit of a hack: look at the option that the
			   current `c' is based on and see if we recognize
			   it: if -? or --help, exit with 0, else with -1 */
			usage(prog, strcmp(av[optind - 1], "-?") == 0 || strcmp(av[optind - 1], "--help") == 0 ? 0 : -1);
		default:
			fprintf(stderr, "ERROR: getopt returned character "
							"code '%c' 0%o\n", c, c);
			usage(prog, -1);
		}
	}

	if (!(setlen = mo_system_config(&set, setlen)))
		usage(prog, -1);

	if (debug || grpdebug) {
		char buf[16];
		char wasdebug = debug != 0;

		debug |= grpdebug;  /* add the algorithm tracers */
		snprintf(buf, sizeof(buf) - 1, "%d", debug);
		setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_debug", buf);
		if (wasdebug)
			mo_print_options(set, setlen);
	}

	monet_script = (str *) malloc(sizeof(str) * (argc + 1));
	if (monet_script == NULL) {
		fprintf(stderr, "!ERROR: cannot allocate memory for script \n");
		exit(1);
	}
	i = 0;
	while (optind < argc) {
		monet_script[i] = absolute_path(av[optind]);
		if (monet_script[i] == NULL) {
			fprintf(stderr, "!ERROR: cannot allocate memory for script \n");
			exit(1);
		}
		i++;
		optind++;
	}
	monet_script[i] = NULL;
	if (!dbpath) {
		dbpath = absolute_path(mo_find_option(set, setlen, "gdk_dbpath"));
		if (dbpath == NULL || GDKcreatedir(dbpath) != GDK_SUCCEED) {
			fprintf(stderr, "!ERROR: cannot allocate memory for database directory \n");
			exit(1);
		}
	}
	if (GDKcreatedir(dbpath) != GDK_SUCCEED) {
		fprintf(stderr, "!ERROR: cannot create directory for %s\n", dbpath);
		exit(1);
	}
	BBPaddfarm(dbpath, 1 << PERSISTENT);
	BBPaddfarm(dbextra ? dbextra : dbpath, 1 << TRANSIENT);
	GDKfree(dbpath);
	if (monet_init(set, setlen) == 0) {
		mo_free_options(set, setlen);
		return 0;
	}
	mo_free_options(set, setlen);

	if (GDKsetenv("monet_version", VERSION) != GDK_SUCCEED ||
	    GDKsetenv("monet_release", MONETDB_RELEASE) != GDK_SUCCEED) {
		fprintf(stderr, "!ERROR: GDKsetenv failed\n");
		exit(1);
	}

	if ((modpath = GDKgetenv("monet_mod_path")) == NULL) {
		/* start probing based on some heuristics given the binary
		 * location:
		 * bin/mserver5 -> ../
		 * libX/monetdb5/lib/
		 * probe libX = lib, lib32, lib64, lib/64 */
		char *libdirs[] = { "lib", "lib64", "lib/64", "lib32", NULL };
		struct stat sb;
		if (binpath != NULL) {
			char *p = strrchr(binpath, DIR_SEP);
			if (p != NULL)
				*p = '\0';
			p = strrchr(binpath, DIR_SEP);
			if (p != NULL) {
				*p = '\0';
				for (i = 0; libdirs[i] != NULL; i++) {
					snprintf(prmodpath, sizeof(prmodpath), "%s%c%s%cmonetdb5",
							binpath, DIR_SEP, libdirs[i], DIR_SEP);
					if (stat(prmodpath, &sb) == 0) {
						modpath = prmodpath;
						break;
					}
				}
			} else {
				printf("#warning: unusable binary location, "
					   "please use --set monet_mod_path=/path/to/... to "
					   "allow finding modules\n");
				fflush(NULL);
			}
		} else {
			printf("#warning: unable to determine binary location, "
				   "please use --set monet_mod_path=/path/to/... to "
				   "allow finding modules\n");
			fflush(NULL);
		}
		if (modpath != NULL &&
		    GDKsetenv("monet_mod_path", modpath) != GDK_SUCCEED) {
			fprintf(stderr, "!ERROR: GDKsetenv failed\n");
			exit(1);
		}
	}

	/* configure sabaoth to use the right dbpath and active database */
	msab_dbpathinit(GDKgetenv("gdk_dbpath"));
	/* wipe out all cruft, if left over */
	if ((err = msab_wildRetreat()) != NULL) {
		/* just swallow the error */
		free(err);
	}
	/* From this point, the server should exit cleanly.  Discussion:
	 * even earlier?  Sabaoth here registers the server is starting up. */
	if ((err = msab_registerStarting()) != NULL) {
		/* throw the error at the user, but don't die */
		fprintf(stderr, "!%s\n", err);
		free(err);
	}

#ifdef HAVE_SIGACTION
	{
		struct sigaction sa;

		sigemptyset(&sa.sa_mask);
		sa.sa_flags = 0;
		sa.sa_handler = handler;
		if (
				sigaction(SIGINT, &sa, NULL) == -1 ||
				sigaction(SIGQUIT, &sa, NULL) == -1 ||
				sigaction(SIGTERM, &sa, NULL) == -1)
		{
			fprintf(stderr, "!unable to create signal handlers\n");
		}
	}
#else
	signal(SIGINT, handler);
#ifdef SIGQUIT
	signal(SIGQUIT, handler);
#endif
	signal(SIGTERM, handler);
#endif

	{
		str lang = "mal";
		/* we inited mal before, so publish its existence */
		if ((err = msab_marchScenario(lang)) != NULL) {
			/* throw the error at the user, but don't die */
			fprintf(stderr, "!%s\n", err);
			free(err);
		}
	}

	{
		/* unlock the vault, first see if we can find the file which
		 * holds the secret */
		char secret[1024];
		char *secretp = secret;
		FILE *secretf;
		size_t len;

		if (GDKgetenv("monet_vault_key") == NULL) {
			/* use a default (hard coded, non safe) key */
			snprintf(secret, sizeof(secret), "%s", "Xas632jsi2whjds8");
		} else {
			if ((secretf = fopen(GDKgetenv("monet_vault_key"), "r")) == NULL) {
				snprintf(secret, sizeof(secret),
						"unable to open vault_key_file %s: %s",
						GDKgetenv("monet_vault_key"), strerror(errno));
				/* don't show this as a crash */
				msab_registerStop();
				GDKfatal("%s", secret);
			}
			len = fread(secret, 1, sizeof(secret), secretf);
			secret[len] = '\0';
			len = strlen(secret); /* secret can contain null-bytes */
			if (len == 0) {
				snprintf(secret, sizeof(secret), "vault key has zero-length!");
				/* don't show this as a crash */
				msab_registerStop();
				GDKfatal("%s", secret);
			} else if (len < 5) {
				fprintf(stderr, "#warning: your vault key is too short "
								"(" SZFMT "), enlarge your vault key!\n", len);
			}
			fclose(secretf);
		}
		if ((err = AUTHunlockVault(secretp)) != MAL_SUCCEED) {
			/* don't show this as a crash */
			msab_registerStop();
			GDKfatal("%s", err);
		}
	}
	/* make sure the authorisation BATs are loaded */
	if ((err = AUTHinitTables(NULL)) != MAL_SUCCEED) {
		/* don't show this as a crash */
		msab_registerStop();
		GDKfatal("%s", err);
	}
	if (mal_init()) {
		/* don't show this as a crash */
		msab_registerStop();
		return 0;
	}

	MSinitClientPrg(mal_clients, "user", "main");
	if (dbinit == NULL)
		dbinit = GDKgetenv("dbinit");
	if (dbinit)
		callString(mal_clients, dbinit, listing);

	emergencyBreakpoint();
	for (i = 0; monet_script[i]; i++) {
		str msg = evalFile(mal_clients, monet_script[i], listing);
		/* check for internal exception message to terminate */
		if (msg) {
			if (strcmp(msg, "MALException:client.quit:Server stopped.") == 0)
				mal_exit();
			fprintf(stderr, "#%s: %s\n", monet_script[i], msg);
			freeException(msg);
		}
		GDKfree(monet_script[i]);
		monet_script[i] = 0;
	}

	if ((err = msab_registerStarted()) != NULL) {
		/* throw the error at the user, but don't die */
		fprintf(stderr, "!%s\n", err);
		free(err);
	}

	free(monet_script);
#ifdef HAVE_CONSOLE
	if (!monet_daemon) {
		MSserveClient(mal_clients);
	} else
#endif
	while (1)
		MT_sleep_ms(5000);

	/* mal_exit calls MT_global_exit, so statements after this call will
	 * never get reached */
	mal_exit();

	return 0;
}
