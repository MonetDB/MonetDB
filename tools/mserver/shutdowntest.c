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
#include "mal_authorize.h"
#include "msabaoth.h"
#include "mutils.h"
#include "mal_linker.h"
#include "sql_execute.h"
#include "sql_scenario.h"

static char* dbdir = NULL;

static int monetdb_initialized = 0;

static void* monetdb_connect(void) {
	Client conn = NULL;
	if (!monetdb_initialized) {
		return NULL;
	}
	conn = MCinitClient(MAL_ADMIN, bstream_create(GDKstdin, 0), GDKstdout);
	if (!MCvalid(conn)) {
		return NULL;
	}
	conn->curmodule = conn->usermodule = userModule();
	str msg;
	if ((msg = SQLinitClient(conn)) != MAL_SUCCEED) {
		freeException(msg);
		return NULL;
	}
	((backend *) conn->sqlcontext)->mvc->session->auto_commit = 1;
	return conn;
}

static str monetdb_query(Client c, str query) {
	str retval;
	mvc* m = ((backend *) c->sqlcontext)->mvc;
	res_table* res = NULL;

	retval = SQLstatementIntern(c, query, "name", 1, 0, &res);
	if (retval == MAL_SUCCEED)
		retval = SQLautocommit(m);
	if (retval != MAL_SUCCEED) {
		printf("Failed to execute SQL query: %s\n", query);
		freeException(retval);
		exit(1);
		return MAL_SUCCEED;
	}
	if (res) {
		// print result columns
//		printf("%s (", res->cols->tn);
//		for(int i = 0; i < res->nr_cols; i++) {
//			printf("%s", res->cols[i].name);
//			printf(i + 1 == res->nr_cols ? ")\n" : ",");
//		}
		SQLdestroyResult(res);
	}
	return MAL_SUCCEED;
}

static void monetdb_disconnect(void* conn) {
	if (!MCvalid((Client) conn)) {
		return;
	}
	str msg = SQLexitClient((Client) conn);
	freeException(msg);
	MCcloseClient((Client) conn);
}

static str monetdb_initialize(void) {
	opt *set = NULL;
	volatile int setlen = 0; /* use volatile for setjmp */
	str retval = MAL_SUCCEED;
	char *err;
	char prmodpath[FILENAME_MAX];
	const char *modpath = NULL;
	char *binpath = NULL;

	if (monetdb_initialized) return MAL_SUCCEED;
	monetdb_initialized = 1;

	if (setlocale(LC_CTYPE, "") == NULL) {
		retval = GDKstrdup("setlocale() failed");
		goto cleanup;
	}

	GDKfataljumpenable = 1;
	if(setjmp(GDKfataljump) != 0) {
		retval = GDKfatalmsg;
		// we will get here if GDKfatal was called.
		if (retval == NULL) {
			retval = GDKstrdup("GDKfatal() with unspecified error?");
		}
		goto cleanup;
	}

	binpath = get_bin_path();

	setlen = mo_builtin_settings(&set);
	setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_dbpath", dbdir);

	if (BBPaddfarm(dbdir, (1U << PERSISTENT) | (1U << TRANSIENT), false) != GDK_SUCCEED) {
		retval = GDKstrdup("BBPaddfarm failed");
		goto cleanup;
	}
	if (GDKinit(set, setlen, true) != GDK_SUCCEED) {
		retval = GDKstrdup("GDKinit() failed");
		goto cleanup;
	}
	GDKdebug |= NOSYNCMASK;

	if (GDKsetenv("mapi_disable", "true") != GDK_SUCCEED) {
		retval = GDKstrdup("GDKsetenv failed");
		goto cleanup;
	}

	if ((modpath = GDKgetenv("monet_mod_path")) == NULL) {
		/* start probing based on some heuristics given the binary
		 * location:
		 * bin/mserver5 -> ../
		 * libX/monetdb5/lib/
		 * probe libX = lib, lib32, lib64, lib/64 */
		char *libdirs[] = { "lib", "lib64", "lib/64", "lib32", NULL };
		size_t i;
		struct stat sb;
		if (binpath != NULL) {
			char *p = strrchr(binpath, DIR_SEP);
			if (p != NULL)
				*p = '\0';
			p = strrchr(binpath, DIR_SEP);
			if (p != NULL) {
				*p = '\0';
				for (i = 0; libdirs[i] != NULL; i++) {
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
			retval = GDKstrdup("GDKsetenv failed");
			goto cleanup;
		}
	}

	/* configure sabaoth to use the right dbpath and active database */
	msab_dbpathinit(GDKgetenv("gdk_dbpath"));
	/* wipe out all cruft, if left over */
	if ((retval = msab_wildRetreat()) != NULL) {
		/* just swallow the error */
		free(retval);
	}
	/* From this point, the server should exit cleanly.  Discussion:
	 * even earlier?  Sabaoth here registers the server is starting up. */
	if ((retval = msab_registerStarting()) != NULL) {
		/* throw the error at the user, but don't die */
		fprintf(stderr, "!%s\n", retval);
		free(retval);
	}

	{
		str lang = "mal";
		/* we inited mal before, so publish its existence */
		if ((retval = msab_marchScenario(lang)) != NULL) {
			/* throw the error at the user, but don't die */
			fprintf(stderr, "!%s\n", retval);
			free(retval);
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
				fprintf(stderr,
					"unable to open vault_key_file %s: %s\n",
					GDKgetenv("monet_vault_key"), strerror(errno));
				/* don't show this as a crash */
				err = msab_registerStop();
				if (err)
					free(err);
				exit(1);
			}
			len = fread(secret, 1, sizeof(secret), secretf);
			secret[len] = '\0';
			len = strlen(secret); /* secret can contain null-bytes */
			if (len == 0) {
				fprintf(stderr, "vault key has zero-length!\n");
				/* don't show this as a crash */
				err = msab_registerStop();
				if (err)
					free(err);
				exit(1);
			} else if (len < 5) {
				fprintf(stderr, "#warning: your vault key is too short "
								"(%zu), enlarge your vault key!\n", len);
			}
			fclose(secretf);
		}
		if ((retval = AUTHunlockVault(secretp)) != MAL_SUCCEED) {
			/* don't show this as a crash */
			err = msab_registerStop();
			if (err)
				free(err);
			fprintf(stderr, "%s\n", retval);
			exit(1);
		}
	}
	/* make sure the authorisation BATs are loaded */
	if ((retval = AUTHinitTables(NULL)) != MAL_SUCCEED) {
		/* don't show this as a crash */
		err = msab_registerStop();
		if (err)
			free(err);
		fprintf(stderr, "%s\n", retval);
		exit(1);
	}

	char *modules[2];
	modules[0] = "sql";
	modules[1] = 0;
	if (mal_init(modules, 1) != 0) { // mal_init() does not return meaningful codes on failure
		retval = GDKstrdup("mal_init() failed");
		goto cleanup;
	}
	GDKfataljumpenable = 0;

	if (retval != MAL_SUCCEED) {
		printf("Failed to load SQL function: %s\n", retval);
		retval = GDKstrdup(retval);
		goto cleanup;
	}

	{
		Client c = (Client) monetdb_connect();
		char* query = "SELECT * FROM tables;";
		retval = monetdb_query(c, query);
		monetdb_disconnect(c);
	}

	mo_free_options(set, setlen);

	return MAL_SUCCEED;
cleanup:
	if (set)
		mo_free_options(set, setlen);
	monetdb_initialized = 0;
	return retval;
}

static void monetdb_shutdown(void) {
	if (monetdb_initialized) {
		mal_reset();
		monetdb_initialized = 0;
	}
}

int main(int argc, char **argv) {
	str retval;
	Client c;
	int i = 0;
	if (argc <= 1) {
		printf("Usage: shutdowntest [testdir]\n");
		return -1;
	}
	dbdir = argv[1];

	retval = monetdb_initialize();
	if (retval != MAL_SUCCEED) {
		printf("Failed first initialization: %s\n", retval);
		return -1;
	}
	c = (Client) monetdb_connect();
	monetdb_query(c, "CREATE TABLE temporary_table(i INTEGER);");
	monetdb_query(c, "INSERT INTO temporary_table VALUES (3), (4);");
	monetdb_disconnect(c);
//	printf("Successfully initialized MonetDB.\n");
	for(i = 0; i < 10; i++) {
		monetdb_shutdown();
//		printf("Successfully shutdown MonetDB.\n");
		retval = monetdb_initialize();
		if (retval != MAL_SUCCEED) {
			printf("Failed MonetDB restart: %s\n", retval);
			return -1;
		}
//		printf("Successfully restarted MonetDB.\n");
		c = (Client) monetdb_connect();
		monetdb_query(c, "SELECT * FROM temporary_table;");
		monetdb_query(c, "DROP TABLE temporary_table;");
		monetdb_query(c, "CREATE TABLE temporary_table(i INTEGER);");
		monetdb_query(c, "INSERT INTO temporary_table VALUES (3), (4);");
		monetdb_disconnect(c);
	}
	return 0;
}
