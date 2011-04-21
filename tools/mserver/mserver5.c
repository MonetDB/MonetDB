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
#include <Psapi.h>		/* for GetModuleFileName */
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

static int malloc_init = 1;
/* NEEDED? */
#if defined(_MSC_VER) && defined(__cplusplus)
#include <eh.h>
void
mserver_abort()
{
	fprintf(stderr, "\n! mserver_abort() was called by terminate(). !\n");
	fflush(stderr);
	MT_global_exit(0);
}
#endif

void
usage(char *prog)
{
	fprintf(stderr, "Usage: %s [options] [scripts]\n", prog);
	fprintf(stderr, "    --dbname=<database_name>  Specify database name\n");
	fprintf(stderr, "    --dbfarm=<directory>      Specify database location\n");
	fprintf(stderr, "    --dbinit=<stmt>           Execute statement at startup\n");
	fprintf(stderr, "    --config=<config_file>    Use config_file to read options from\n");
	fprintf(stderr, "    --daemon=yes|no           Do not read commands from standard input [no]\n");
	fprintf(stderr, "    --single-user             Allow only one user at a time\n");
	fprintf(stderr, "    --readonly                Safeguard database\n");
	fprintf(stderr, "    --set <option>=<value>    Set configuration option\n");
	fprintf(stderr, "    --help                    Print this list of options\n");
	fprintf(stderr, "    --version                 Print version and compile time info\n");

	fprintf(stderr,"The debug, testing & trace options:\n");
	fprintf(stderr,"     --threads\n");
	fprintf(stderr,"     --memory\n");
	fprintf(stderr,"     --properties\n");
	fprintf(stderr,"     --io\n");
	fprintf(stderr,"     --transactions\n");
	fprintf(stderr,"     --modules\n");
	fprintf(stderr,"     --algorithms\n");
	fprintf(stderr,"     --xproperties\n");
	fprintf(stderr,"     --performance\n");
	fprintf(stderr,"     --optimizers\n");
	fprintf(stderr,"     --forcemito\n");
	fprintf(stderr,"     --debug=<bitmask>\n");
#ifndef NATIVE_WIN32
	fprintf(stderr,"(See `man mserver5.conf` for the documentation.)\n");
#endif

	exit(0);
}

static void
monet_hello(void)
{
#ifdef MONETDB_STATIC
	char *linkinfo = "statically";
#else
	char *linkinfo = "dynamically";
#endif

	dbl    sz_mem_h;
	char  *qc = " kMGTPE";
	int    qi = 0;

	monet_memory = MT_npages() * MT_pagesize();
	sz_mem_h = (dbl) monet_memory;
	while (sz_mem_h >= 1000.0 && qi < 6) {
		sz_mem_h /= 1024.0;
		qi++;
	}

	printf("# MonetDB 5 server v" VERSION);
	if (strcmp(MONETDB_RELEASE, "unreleased") == 0)
		printf("\n# This is an unreleased version");
	else
		printf(" \"%s\"", MONETDB_RELEASE);
	printf("\n# Serving database '%s', using %d thread%s\n",
			GDKgetenv("gdk_dbname"),
			GDKnr_threads, (GDKnr_threads != 1) ? "s" : "");
	printf("# Compiled for %s/" SZFMT "bit with " SZFMT "bit OIDs %s linked\n",
			HOST, sizeof(ptr) * 8, sizeof(oid) * 8, linkinfo);
	printf("# Found %.3f %ciB available main-memory.\n", 
			sz_mem_h, qc[qi]);
#ifdef MONET_GLOBAL_DEBUG
	printf("# Database farm:%s\n", GDKgetenv("gdk_dbfarm"));
	printf("# Module path:%s\n", GDKgetenv("monet_mod_path"));
#endif
	printf("# Copyright (c) 1993-July 2008 CWI.\n");
	printf("# Copyright (c) August 2008-2011 MonetDB B.V., all rights reserved\n");
	printf("# Visit http://monetdb.cwi.nl/ for further information\n");
}

str
absolute_path(str s)
{
	if (!MT_path_absolute(s)) {
		str ret = (str) GDKmalloc(strlen(s) + strlen(monet_cwd) + 2);

		if ( ret )
			sprintf(ret, "%s%c%s", monet_cwd, DIR_SEP, s);
		return ret;
	}
	return GDKstrdup(s);
}

#define BSIZE 8192

int
monet_init(opt *set, int setlen)
{
	/* determine Monet's kernel settings */
	if (!GDKinit(set, setlen))
		return 0;

#ifdef HAVE_CONSOLE
	monet_daemon = GDKembedded;
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

void emergencyBreakpoint(){
	/* just a handle to break after system initialization for GDB */
}

static void
handler(int sig)
{
	(void)sig;
	mal_exit();
}

int
main(int argc, char **av)
{
	char *prog = *av;
	opt *set = NULL;
	int idx = 0, grpdebug=0, debug = 0, setlen = 0, listing = 0, i = 0;
	str dbinit = NULL;
	str err = MAL_SUCCEED;
	char prmodpath[1024];
	char *modpath = NULL;

	static struct option long_options[] = {
		{"config", 1, 0, 'c'},
		{"dbname", 1, 0, 0},
		{"dbfarm", 1, 0, 0},
		{"dbinit", 1, 0, 0},
		{"daemon", 1, 0, 0},
		{"debug", 2, 0, 'd'},
		{"help", 0, 0, 'h'},
		{"version", 0, 0, 0},
		{"readonly", 0, 0, 'r'},
		{"single-user", 0, 0, 0},
		{"set", 1, 0, 's'},
		{"trace", 0, 0, 't'},
		{"threads",0,0,0},
		{"memory",0,0,0},
		{"properties",0,0,0},
		{"io",0,0,0},
		{"transaction",0,0,0},
		{"modules",0,0,0},
		{"algorithms",0,0,0},
		{"optimizers",0,0,0},
		{"performance",0,0,0},
		{"xproperties",0,0,0},
		{"forcemito",0,0,0},
		{0, 0, 0, 0}
	};

#if defined(_MSC_VER) && defined(__cplusplus)
	set_terminate(mserver_abort);
#endif
        if (setlocale(LC_CTYPE, "") == NULL) {
                GDKfatal("cannot set locale\n");
        }

#ifdef HAVE_MALLOPT
	if (malloc_init) {
/* for (Red Hat) Linux (6.2) unused and ignored at least as of glibc-2.1.3-15 */
/* for (Red Hat) Linux (8) used at least as of glibc-2.2.93-5 */
            if (mallopt(M_MXFAST, 192)) {
                fprintf(stderr, "!monet: mallopt(M_MXFAST,192) fails.\n");
            }
#ifdef M_BLKSZ
            if (mallopt(M_BLKSZ, 8*1024)) {
                fprintf(stderr, "!monet: mallopt(M_BLKSZ,8*1024) fails.\n");
            }
#endif
        }
	malloc_init=0;
#else
	(void) malloc_init;	/* still unused */
#endif

	if (getcwd(monet_cwd, PATHLENGTH - 1) == NULL) {
		perror("pwd");
		GDKfatal("monet_init: could not determine current directory\n");
	}

	if (!(setlen = mo_builtin_settings(&set)))
		usage(prog);

	for (;;) {
		int option_index = 0;

		int c = getopt_long(argc, av, "c:d::t:rh?s:m:i:a:e:x",
				    long_options, &option_index);

		if (c == -1)
			break;

		switch (c) {
		case 0:
			if (strcmp(long_options[option_index].name, "dbname") == 0) {
				setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_dbname", optarg);
				break;
			}
			if (strcmp(long_options[option_index].name, "dbfarm") == 0) {
				setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_dbfarm", optarg);
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
			if (strcmp(long_options[option_index].name, "xproperties") == 0) {
				grpdebug |= GRPxproperties;
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
			usage(prog);
			break;
		case 'c':
			setlen = mo_add_option(&set, setlen, opt_cmdline, "config", optarg);
			break;
		case 'd':
			if (optarg) {
				debug |= strtol(optarg, NULL, 10);
			} else {
				debug |= 1;
			}
			break;
		case 'r':
			setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_readonly", "yes");
			break;
		case 's':{
			/* should add option to a list */
			char *tmp = strchr(optarg, '=');

			if (tmp) {
				*tmp = '\0';
				setlen = mo_add_option(&set, setlen, opt_cmdline, optarg, tmp + 1);
			} else {
				fprintf(stderr, "ERROR: wrong format %s\n", optarg);
			}
		}
			break;
		case 't':	/* trace option, ignored to reduce testweb complaints
			fprintf(stderr, "#warning: trace option not yet supported\n");
			*/
			break;
		case 'h':
		case '?':
			usage(prog);
		default:
			fprintf(stderr, "ERROR: getopt returned character "
							"code '%c' 0%o\n",c, c);
			usage(prog);
		}
	}

	if (!(setlen = mo_system_config(&set, setlen)))
		usage(prog);

	if (debug || grpdebug) {
		long_str buf;

		if (debug)
			mo_print_options(set, setlen);
		debug |= grpdebug;	/* add the algorithm tracers */
		snprintf(buf, sizeof(long_str)-1, "%d", debug);
		setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_debug", buf);
	}

	monet_script = (str *) GDKmalloc(sizeof(str) * (argc + 1));
	if ( monet_script ){
		monet_script[idx] = NULL;
		while (optind < argc) {
			monet_script[idx] = absolute_path(av[optind]);
			monet_script[idx + 1] = NULL;
			optind++;
			idx++;
		}
	}

	if (monet_init(set, setlen) == 0 ) {
		mo_free_options(set, setlen);
		return 0;
	}
	mo_free_options(set, setlen);

	GDKsetenv("monet_version", VERSION);

	if ((modpath = GDKgetenv("monet_mod_path")) == NULL) {
		/* start probing based on some heuristics given the binary
		 * location:
		 * bin/mserver5 -> ../
		 * libX/monetdb5/lib/
		 * probe libX = lib, lib32, lib64, lib/64 */
		char *libdirs[] = { "lib", "lib64", "lib/64", "lib32", NULL };
		size_t i;
		struct stat sb;
		char *binpath = get_bin_path();
		if (binpath != NULL) {
			binpath = dirname(binpath);
			binpath = dirname(binpath);
			for (i = 0; libdirs[i] != NULL; i++) {
				snprintf(prmodpath, sizeof(prmodpath), "%s%c%s%cmonetdb5",
						binpath, DIR_SEP, libdirs[i], DIR_SEP);
				if (stat(prmodpath, &sb) == 0) {
					modpath = prmodpath;
					break;
				}
			}
		} else {
			printf("#warning: unable to determine binary location, "
					"please use --set monet_mod_path=/path/to/... to "
					"allow finding modules\n");
			fflush(NULL);
		}
		if (modpath != NULL)
			GDKsetenv("monet_mod_path", modpath);
	}

	/* configure sabaoth to use the right dbfarm and active database */
	msab_init(GDKgetenv("gdk_dbfarm"), GDKgetenv("gdk_dbname"));
	/* wipe out all cruft, if left over */
	if ((err = msab_wildRetreat()) != NULL) {
		/* just swallow the error */
		free(err);
	}
	/* From this point, the server should exit cleanly.  Discussion:
	 * even earlier?  Sabaoth here registers the server has started up. */
	if ((err = msab_registerStart()) != NULL) {
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
		char* secret = alloca(sizeof(char) * 1024);
		FILE* secretf;
		size_t len;
		
		if (GDKgetenv("monet_vault_key") == NULL) {
			/* use a default (hard coded, non safe) key */
			secret = "Xas632jsi2whjds8";
		} else {
			if ((secretf = fopen(GDKgetenv("monet_vault_key"), "r")) == NULL) {
				snprintf(secret, 1023, "unable to open vault_key_file %s: %s",
						GDKgetenv("monet_vault_key"), strerror(errno));
				secret[1023] = '\0';
				/* don't show this as a crash */
				msab_registerStop();
				GDKfatal(secret);
			}
			len = fread(secret, 1, 1023, secretf);
			secret[len] = '\0';
			len = strlen(secret); /* secret can contain null-bytes */
			if (len == 0) {
				snprintf(secret, 1023, "vault key has zero-length!");
				/* don't show this as a crash */
				msab_registerStop();
				GDKfatal(secret);
			} else if (len < 5) {
				fprintf(stderr, "#warning: your vault key is too short "
						"(" SZFMT "), enlarge your vault key!\n", len);
			}
			fclose(secretf);
		}
		if ((err = AUTHunlockVault(&secret)) != MAL_SUCCEED)
			GDKfatal(err);
	}
	/* make sure the authorisation BATs are loaded */
	if ((err = AUTHinitTables()) != MAL_SUCCEED)
		GDKfatal(err);
	if (mal_init())
		return 0;

	if (GDKgetenv("mal_listing"))
		sscanf(GDKgetenv("mal_listing"), "%d", &listing);

	MSinitClientPrg(mal_clients, "user", "main");
	if (dbinit == NULL)
		dbinit = GDKgetenv("dbinit");
	if (dbinit) 
		callString(mal_clients, dbinit, listing);

	emergencyBreakpoint();
	if (monet_script) for (i = 0; monet_script[i]; i++) {
		str msg = evalFile(mal_clients, monet_script[i], listing);
		/* check for internal exception message to terminate */
		if (msg && strcmp(msg, "MALException:client.quit:Server stopped.") == 0)
			mal_exit();
		if (msg) GDKfree(msg);
		GDKfree(monet_script[i]);
		monet_script[i] = 0;
	}
	if (monet_script)
		GDKfree(monet_script);
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
