/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
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

static _Noreturn void usage(char *prog, int xit);

static void
usage(char *prog, int xit)
{
	fprintf(stderr, "Usage: %s [options]\n", prog);
	fprintf(stderr, "    --dbpath=<directory>      Specify database location\n");
	fprintf(stderr, "    --dbextra=<directory>     Directory for transient BATs\n");
	fprintf(stderr, "    --dbtrace=<file>          File for produced traces\n");
	fprintf(stderr, "    --in-memory               Run database in-memory only\n");
	fprintf(stderr, "    --config=<config_file>    Use config_file to read options from\n");
	fprintf(stderr, "    --single-user             Allow only one user at a time\n");
	fprintf(stderr, "    --readonly                Safeguard database\n");
	fprintf(stderr, "    --set <option>=<value>    Set configuration option\n");
	fprintf(stderr, "    --help                    Print this list of options\n");
	fprintf(stderr, "    --version                 Print version and compile time info\n");

	fprintf(stderr, "The debug, testing & trace options:\n");
	fprintf(stderr, "     --algorithms\n");
	fprintf(stderr, "     --forcemito\n");
	fprintf(stderr, "     --heaps\n");
	fprintf(stderr, "     --io\n");
	fprintf(stderr, "     --memory\n");
	fprintf(stderr, "     --modules\n");
	fprintf(stderr, "     --performance\n");
	fprintf(stderr, "     --properties\n");
	fprintf(stderr, "     --threads\n");
	fprintf(stderr, "     --transactions\n");
	fprintf(stderr, "     --debug=<bitmask>\n");

	exit(xit);
}

/*
 * Collect some global system properties to relate performance results later
 */
static void
monet_hello(void)
{
	double sz_mem_h;
	char  *qc = " kMGTPE";
	int qi = 0;

	printf("# MonetDB 5 server v%s", GDKversion());
	{
#ifdef MONETDB_RELEASE
		printf(" (%s)", MONETDB_RELEASE);
#else
		const char *rev = mercurial_revision();
		if (strcmp(rev, "Unknown") != 0)
			printf(" (hg id: %s)", rev);
#endif
	}
#ifndef MONETDB_RELEASE
	printf("\n# This is an unreleased version");
#endif
	printf("\n# Serving database '%s', using %d thread%s\n",
			GDKgetenv("gdk_dbname"),
			GDKnr_threads, (GDKnr_threads != 1) ? "s" : "");
	printf("# Compiled for %s/%zubit%s\n",
			HOST, sizeof(ptr) * 8,
#ifdef HAVE_HGE
			" with 128bit integers"
#else
			""
#endif
			);
	sz_mem_h = (double) MT_npages() * MT_pagesize();
	while (sz_mem_h >= 1000.0 && qi < 6) {
		sz_mem_h /= 1024.0;
		qi++;
	}
	printf("# Found %.3f %ciB available main-memory", sz_mem_h, qc[qi]);
	sz_mem_h = (double) GDK_mem_maxsize;
	qi = 0;
	while (sz_mem_h >= 1000.0 && qi < 6) {
		sz_mem_h /= 1024.0;
		qi++;
	}
	printf(" of which we use %.3f %ciB\n", sz_mem_h, qc[qi]);
	if (GDK_vm_maxsize < GDK_VM_MAXSIZE) {
		sz_mem_h = (double) GDK_vm_maxsize;
		qi = 0;
		while (sz_mem_h >= 1000.0 && qi < 6) {
			sz_mem_h /= 1024.0;
			qi++;
		}
		printf("# Virtual memory usage limited to %.3f %ciB\n",
		       sz_mem_h, qc[qi]);
	}
#ifdef MONET_GLOBAL_DEBUG
	printf("# Database path:%s\n", GDKgetenv("gdk_dbpath"));
	printf("# Module path:%s\n", GDKgetenv("monet_mod_path"));
#endif
	printf("# Copyright (c) 1993 - July 2008 CWI.\n");
	printf("# Copyright (c) August 2008 - 2021 MonetDB B.V., all rights reserved\n");
	printf("# Visit https://www.monetdb.org/ for further information\n");

	// The properties shipped through the performance profiler
	(void) snprintf(monet_characteristics, sizeof(monet_characteristics),
			"{\n"
			"\"version\":\"%s\",\n"
			"\"release\":\"%s\",\n"
			"\"host\":\"%s\",\n"
			"\"threads\":\"%d\",\n"
			"\"memory\":\"%.3f %cB\",\n"
			"\"oid\":\"%zu\",\n"
			"\"packages\":["
#ifdef HAVE_HGE
			"\"huge\""
#endif
			"]\n}",
			GDKversion(),
#ifdef MONETDB_RELEASE
			MONETDB_RELEASE,
#else
			"unreleased",
#endif
			HOST, GDKnr_threads,
			sz_mem_h, qc[qi], sizeof(oid) * 8);
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
monet_init(opt *set, int setlen, bool embedded)
{
	/* check that library that we're linked against is compatible with
	 * the one we were compiled with */
	int maj, min, patch;
	const char *version = GDKlibversion();
	sscanf(version, "%d.%d.%d", &maj, &min, &patch);
	if (maj != GDK_VERSION_MAJOR || min < GDK_VERSION_MINOR) {
		fprintf(stderr, "Linked GDK library not compatible with the one this was compiled with\n");
		fprintf(stderr, "Linked version: %s, compiled version: %s\n",
				version, GDK_VERSION);
		return 0;
	}
	version = mal_version();
	sscanf(version, "%d.%d.%d", &maj, &min, &patch);
	if (maj != MONETDB5_VERSION_MAJOR || min < MONETDB5_VERSION_MINOR) {
		fprintf(stderr, "Linked MonetDB5 library not compatible with the one this was compiled with\n");
		fprintf(stderr, "Linked version: %s, compiled version: %s\n",
				version, MONETDB5_VERSION);
		return 0;
	}

	/* determine Monet's kernel settings */
	if (GDKinit(set, setlen, embedded) != GDK_SUCCEED)
		return 0;

#ifdef HAVE_SETSID
	setsid();
#endif
	monet_hello();
	return 1;
}

static void emergencyBreakpoint(void)
{
	/* just a handle to break after system initialization for GDB */
}

static volatile sig_atomic_t interrupted = 0;

#ifdef _MSC_VER
static BOOL WINAPI
winhandler(DWORD type)
{
	(void) type;
	interrupted = 1;
	return TRUE;
}
#else
static void
handler(int sig)
{
	(void) sig;
	interrupted = 1;
}
#endif

int
#ifdef _MSC_VER
wmain(int argc, wchar_t **argv)
#else
main(int argc, char **av)
#endif
{
#ifdef _MSC_VER
	char **av = malloc((argc + 1) * sizeof(char *));
	if (av == NULL) {
		fprintf(stderr, "cannot allocate memory for argument conversion\n");
		exit(1);
	}
	for (int i = 0; i < argc; i++) {
		if ((av[i] = wchartoutf8(argv[i])) == NULL) {
			fprintf(stderr, "cannot convert argument to UTF-8\n");
			exit(1);
		}
	}
	av[argc] = NULL;
#endif
	char *prog = *av;
	opt *set = NULL;
	int grpdebug = 0, debug = 0, setlen = 0;
	str err = MAL_SUCCEED;
	char prmodpath[FILENAME_MAX];
	const char *modpath = NULL;
	char *binpath = NULL;
	char *dbpath = NULL;
	char *dbextra = NULL;
	char *dbtrace = NULL;
	bool inmemory = false;
	bool readpwdxit = false;
	static struct option long_options[] = {
		{ "config", required_argument, NULL, 'c' },
		{ "dbextra", required_argument, NULL, 0 },
		{ "dbpath", required_argument, NULL, 0 },
		{ "dbtrace", required_argument, NULL, 0 },
		{ "debug", optional_argument, NULL, 'd' },
		{ "help", no_argument, NULL, '?' },
		{ "in-memory", no_argument, NULL, 0 },
		{ "readonly", no_argument, NULL, 'r' },
		{ "set", required_argument, NULL, 's' },
		{ "single-user", no_argument, NULL, 0 },
		{ "version", no_argument, NULL, 0 },

		{ "algorithms", no_argument, NULL, 0 },
		{ "forcemito", no_argument, NULL, 0 },
		{ "heaps", no_argument, NULL, 0 },
		{ "io", no_argument, NULL, 0 },
		{ "memory", no_argument, NULL, 0 },
		{ "modules", no_argument, NULL, 0 },
		{ "performance", no_argument, NULL, 0 },
		{ "properties", no_argument, NULL, 0 },
		{ "threads", no_argument, NULL, 0 },
		{ "transactions", no_argument, NULL, 0 },

		{ "read-password-initialize-and-exit", no_argument, NULL, 0 },
		{ "loadmodule", required_argument, NULL, 0 },

		{ NULL, 0, NULL, 0 }
	};

#define MAX_MODULES 32
	char *modules[MAX_MODULES+1];
	int mods = 0;

	modules[mods++] = "sql";
	modules[mods++] = "generator";
	modules[mods++] = "opt_sql_append";
#ifdef HAVE_GEOM
	modules[mods++] = "geom";
#endif
#ifdef HAVE_LIBR
	/* TODO check for used */
	modules[mods++] = "rapi";
#endif
#ifdef HAVE_LIBPY3
	/* TODO check for used */
	modules[mods++] = "pyapi3";
#endif
#ifdef HAVE_CUDF
	modules[mods++] = "capi";
#endif
	modules[mods++] = "udf";
#ifdef HAVE_FITS
	modules[mods++] = "fits";
#endif
#ifdef HAVE_NETCDF
	modules[mods++] = "netcdf";
#endif
#ifdef HAVE_SHP
	modules[mods++] = "shp";
#endif

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
		fprintf(stderr, "cannot set locale\n");
		exit(1);
	}

	if (MT_getcwd(monet_cwd, FILENAME_MAX - 1) == NULL) {
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

		int c = getopt_long(argc, av, "c:d::rs:t::v::?",
				long_options, &option_index);

		if (c == -1)
			break;

		switch (c) {
		case 0:
			if (strcmp(long_options[option_index].name, "in-memory") == 0) {
				inmemory = true;
				break;
			}
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

			if (strcmp(long_options[option_index].name, "dbtrace") == 0) {
				size_t optarglen = strlen(optarg);
				/* remove trailing directory separator */
				while (optarglen > 0 &&
				       (optarg[optarglen - 1] == '/' ||
					optarg[optarglen - 1] == '\\'))
					optarg[--optarglen] = '\0';
				dbtrace = absolute_path(optarg);
				if(dbtrace == NULL)
					fprintf(stderr, "#error: can not allocate memory for dbtrace\n");
				else
					setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_dbtrace", dbtrace);
				break;
			}

			if (strcmp(long_options[option_index].name, "single-user") == 0) {
				setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_single_user", "yes");
				break;
			}
			if (strcmp(long_options[option_index].name, "version") == 0) {
				monet_version();
				exit(0);
			}
			/* debugging options */
			if (strcmp(long_options[option_index].name, "algorithms") == 0) {
				grpdebug |= GRPalgorithms;
				break;
			}
			if (strcmp(long_options[option_index].name, "forcemito") == 0) {
				grpdebug |= GRPforcemito;
				break;
			}
			if (strcmp(long_options[option_index].name, "heaps") == 0) {
				grpdebug |= GRPheaps;
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
			if (strcmp(long_options[option_index].name, "performance") == 0) {
				grpdebug |= GRPperformance;
				break;
			}
			if (strcmp(long_options[option_index].name, "properties") == 0) {
				grpdebug |= GRPproperties;
				break;
			}
			if (strcmp(long_options[option_index].name, "threads") == 0) {
				grpdebug |= GRPthreads;
				break;
			}
			if (strcmp(long_options[option_index].name, "transactions") == 0) {
				grpdebug |= GRPtransactions;
				break;
			}
			if (strcmp(long_options[option_index].name, "read-password-initialize-and-exit") == 0) {
				readpwdxit = true;
				break;
			}
			if (strcmp(long_options[option_index].name, "loadmodule") == 0) {
				if (mods < MAX_MODULES)
					modules[mods++]=optarg;
				else
					fprintf(stderr, "ERROR: maximum number of modules reached\n");
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
		case '?':
			/* a bit of a hack: look at the option that the
			   current `c' is based on and see if we recognize
			   it: if -? or --help, exit with 0, else with -1 */
			usage(prog, strcmp(av[optind - 1], "-?") == 0 || strcmp(av[optind - 1], "--help") == 0 ? 0 : -1);
		default:
			fprintf(stderr, "ERROR: getopt returned character "
				"code '%c' 0%o\n", c, (unsigned) (uint8_t) c);
			usage(prog, -1);
		}
	}

	if (optind < argc)
		usage(prog, -1);

	if (!(setlen = mo_system_config(&set, setlen)))
		usage(prog, -1);

	if (debug)
		mo_print_options(set, setlen);

	if (dbpath && inmemory) {
		fprintf(stderr, "!ERROR: both dbpath and in-memory must not be set at the same time\n");
		exit(1);
	}

	if (inmemory && readpwdxit) {
		fprintf(stderr, "!ERROR: cannot have both in-memory and read-password-initialize-and-exit\n");
		exit(1);
	}

	if (!dbpath) {
		dbpath = absolute_path(mo_find_option(set, setlen, "gdk_dbpath"));
		if (!dbpath) {
			fprintf(stderr, "!ERROR: cannot allocate memory for database directory \n");
			exit(1);
		}
	}
	if (inmemory) {
		if (BBPaddfarm(NULL, (1U << PERSISTENT) | (1U << TRANSIENT), true) != GDK_SUCCEED) {
			fprintf(stderr, "!ERROR: cannot add in-memory farm\n");
			exit(1);
		}
	} else {
		if (BBPaddfarm(dbpath, 1U << PERSISTENT, true) != GDK_SUCCEED ||
		    BBPaddfarm(dbextra ? dbextra : dbpath, 1U << TRANSIENT, true) != GDK_SUCCEED) {
			fprintf(stderr, "!ERROR: cannot add farm\n");
			exit(1);
		}
		if (GDKcreatedir(dbpath) != GDK_SUCCEED) {
			fprintf(stderr, "!ERROR: cannot create directory for %s\n", dbpath);
			exit(1);
		}
	}
	GDKfree(dbpath);

	if (dbtrace) {
		/* GDKcreatedir makes sure that all parent directories of dbtrace exist */
		if (GDKcreatedir(dbtrace) != GDK_SUCCEED) {
			fprintf(stderr, "!ERROR: cannot create directory for %s\n", dbtrace);
			exit(1);
		}
		GDKfree(dbtrace);
	}

	if (monet_init(set, setlen, false) == 0) {
		mo_free_options(set, setlen);
		if (GDKerrbuf && *GDKerrbuf)
			fprintf(stderr, "%s\n", GDKerrbuf);
		exit(1);
	}
	GDKsetdebug(debug | grpdebug);  /* add the algorithm tracers */
	mo_free_options(set, setlen);

	if (GDKsetenv("monet_version", GDKversion()) != GDK_SUCCEED ||
	    GDKsetenv("monet_release",
#ifdef MONETDB_RELEASE
		      MONETDB_RELEASE
#else
		      "unreleased"
#endif
		    ) != GDK_SUCCEED) {
		fprintf(stderr, "!ERROR: GDKsetenv failed\n");
		exit(1);
	}

	if ((modpath = GDKgetenv("monet_mod_path")) == NULL) {
		/* start probing based on some heuristics given the binary
		 * location:
		 * bin/mserver5 -> ../
		 * libX/monetdb5/lib/
		 * probe libX = lib, lib32, lib64, lib/64 */
		size_t pref;
		/* "remove" common prefix of configured BIN and LIB
		 * directories from LIBDIR */
		for (pref = 0; LIBDIR[pref] != 0 && BINDIR[pref] == LIBDIR[pref]; pref++)
			;
		const char *libdirs[] = {
			&LIBDIR[pref],
			"lib",
			"lib64",
			"lib/64",
			"lib32",
			NULL,
		};
		struct stat sb;
		if (binpath != NULL) {
			char *p = strrchr(binpath, DIR_SEP);
			if (p != NULL)
				*p = '\0';
			p = strrchr(binpath, DIR_SEP);
			if (p != NULL) {
				*p = '\0';
				for (int i = 0; libdirs[i] != NULL; i++) {
					int len = snprintf(prmodpath, sizeof(prmodpath), "%s%c%s%cmonetdb5",
									   binpath, DIR_SEP, libdirs[i], DIR_SEP);
					if (len == -1 || len >= FILENAME_MAX)
						continue;
					if (MT_stat(prmodpath, &sb) == 0) {
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

	if (!GDKinmemory(0)) {
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
	}

#ifdef HAVE_SIGACTION
	{
		struct sigaction sa;

		(void) sigemptyset(&sa.sa_mask);
		sa.sa_flags = 0;
		sa.sa_handler = handler;
		if (sigaction(SIGINT, &sa, NULL) == -1 ||
		    sigaction(SIGQUIT, &sa, NULL) == -1 ||
		    sigaction(SIGTERM, &sa, NULL) == -1) {
			fprintf(stderr, "!unable to create signal handlers\n");
		}
	}
#else
#ifdef _MSC_VER
	if (!SetConsoleCtrlHandler(winhandler, TRUE))
		fprintf(stderr, "!unable to create console control handler\n");
#else
	if(signal(SIGINT, handler) == SIG_ERR)
		fprintf(stderr, "!unable to create signal handlers\n");
#ifdef SIGQUIT
	if(signal(SIGQUIT, handler) == SIG_ERR)
		fprintf(stderr, "!unable to create signal handlers\n");
#endif
	if(signal(SIGTERM, handler) == SIG_ERR)
		fprintf(stderr, "!unable to create signal handlers\n");
#endif
#endif

	if (!GDKinmemory(0)) {
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

		if (GDKinmemory(0) || GDKgetenv("monet_vault_key") == NULL) {
			/* use a default (hard coded, non safe) key */
			snprintf(secret, sizeof(secret), "%s", "Xas632jsi2whjds8");
		} else {
			if ((secretf = MT_fopen(GDKgetenv("monet_vault_key"), "r")) == NULL) {
				fprintf(stderr,
					"unable to open vault_key_file %s: %s\n",
					GDKgetenv("monet_vault_key"), strerror(errno));
				/* don't show this as a crash */
				msab_registerStop();
				exit(1);
			}
			len = fread(secret, 1, sizeof(secret), secretf);
			secret[len] = '\0';
			len = strlen(secret); /* secret can contain null-bytes */
			if (len == 0) {
				fprintf(stderr, "vault key has zero-length!\n");
				/* don't show this as a crash */
				msab_registerStop();
				exit(1);
			} else if (len < 5) {
				fprintf(stderr, "#warning: your vault key is too short "
								"(%zu), enlarge your vault key!\n", len);
			}
			fclose(secretf);
		}
		if ((err = AUTHunlockVault(secret)) != MAL_SUCCEED) {
			/* don't show this as a crash */
			if (!GDKinmemory(0))
				msab_registerStop();
			fprintf(stderr, "%s\n", err);
			freeException(err);
			exit(1);
		}
		if (readpwdxit) {
			if (fgets(secret, (int) sizeof(secret), stdin) == NULL) {
				fprintf(stderr, "!ERROR: no password read\n");
				exit(1);
			}
			if ((secretp = strchr(secret, '\n')) == NULL) {
				fprintf(stderr, "!ERROR: password too long\n");
				exit(1);
			}
			*secretp = '\0';
			err = AUTHinitTables(secret);
			msab_registerStop();
			if (err != MAL_SUCCEED) {
				fprintf(stderr, "%s\n", err);
				freeException(err);
				exit(1);
			}
			exit(0);
		}
	}

	modules[mods++] = 0;
	if (mal_init(modules, 0)) {
		/* don't show this as a crash */
		if (!GDKinmemory(0))
			msab_registerStop();
		return 1;
	}

	emergencyBreakpoint();

	if (!GDKinmemory(0) && (err = msab_registerStarted()) != NULL) {
		/* throw the error at the user, but don't die */
		fprintf(stderr, "!%s\n", err);
		free(err);
	}

#ifdef SIGHUP
	// Register signal to GDKtracer (logrotate)
	signal(SIGHUP, GDKtracer_reinit_basic);
#endif

#ifdef _MSC_VER
	printf("# MonetDB server is started. To stop server press Ctrl-C.\n");
#endif

	/* why busy wait ? */
	while (!interrupted && !GDKexiting()) {
		MT_sleep_ms(100);
	}

	/* mal_exit calls exit, so statements after this call will
	 * never get reached */
	mal_exit(0);

	return 0;
}
