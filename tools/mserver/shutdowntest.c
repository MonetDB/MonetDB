
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
#include "mal_linker.h"
#include "sql_execute.h"
#include "sql_scenario.h"

static char* dbdir = NULL;

#define CREATE_SQL_FUNCTION_PTR(retval, fcnname)     \
   typedef retval (*fcnname##_ptr_tpe)();            \
   fcnname##_ptr_tpe fcnname##_ptr = NULL;

#define LOAD_SQL_FUNCTION_PTR(fcnname)                                             \
    fcnname##_ptr = (fcnname##_ptr_tpe) getAddress( #fcnname); \
    if (fcnname##_ptr == NULL) {                                                           \
        retval = GDKstrdup(#fcnname);  \
    }

CREATE_SQL_FUNCTION_PTR(int,SQLautocommit);
CREATE_SQL_FUNCTION_PTR(str,SQLexitClient);
CREATE_SQL_FUNCTION_PTR(str,SQLinitClient);
CREATE_SQL_FUNCTION_PTR(str,SQLstatementIntern);
CREATE_SQL_FUNCTION_PTR(void,SQLdestroyResult);

static int monetdb_initialized = 0;

static void* monetdb_connect(void) {
	Client conn = NULL;
	if (!monetdb_initialized) {
		return NULL;
	}
	conn = MCforkClient(&mal_clients[0]);
	if (!MCvalid(conn)) {
		return NULL;
	}
	if ((*SQLinitClient_ptr)(conn) != MAL_SUCCEED) {
		return NULL;
	}
	((backend *) conn->sqlcontext)->mvc->session->auto_commit = 1;
	return conn;
}

static str monetdb_query(Client c, str query) {
	str retval;
	mvc* m = ((backend *) c->sqlcontext)->mvc;
	res_table* res = NULL;
	int i;
	retval = (*SQLstatementIntern_ptr)(c, 
		&query, 
		"name", 
		1, 0, &res);
	(*SQLautocommit_ptr)(m);
	if (retval != MAL_SUCCEED) {
		printf("Failed to execute SQL query: %s\n", query);
		freeException(retval);
		exit(1);
		return MAL_SUCCEED;
	}
	if (res) {
		// print result columns
		printf("%s (", res->cols->tn);
		for(i = 0; i < res->nr_cols; i++) {
			printf("%s", res->cols[i].name);
			printf(i + 1 == res->nr_cols ? ")\n" : ",");
		}
		(*SQLdestroyResult_ptr)(res);
	}
	return MAL_SUCCEED;
}

static void monetdb_disconnect(void* conn) {
	if (!MCvalid((Client) conn)) {
		return;
	}
	(*SQLexitClient_ptr)((Client) conn);
	MCcloseClient((Client) conn);
}

static str monetdb_initialize(void) {
	opt *set = NULL;
	volatile int setlen = 0; /* use volatile for setjmp */
	str retval = MAL_SUCCEED;
	char prmodpath[1024];
	char *modpath = NULL;
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

	BBPaddfarm(dbdir, (1 << PERSISTENT) | (1 << TRANSIENT));
	if (GDKinit(set, setlen) == 0) {
		retval = GDKstrdup("GDKinit() failed");
		goto cleanup;
	}

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
		if ((retval = AUTHunlockVault(secretp)) != MAL_SUCCEED) {
			/* don't show this as a crash */
			msab_registerStop();
			GDKfatal("%s", retval);
		}
	}
	/* make sure the authorisation BATs are loaded */
	if ((retval = AUTHinitTables(NULL)) != MAL_SUCCEED) {
		/* don't show this as a crash */
		msab_registerStop();
		GDKfatal("%s", retval);
	}

	if (mal_init() != 0) { // mal_init() does not return meaningful codes on failure
		retval = GDKstrdup("mal_init() failed");
		goto cleanup;
	}
	GDKfataljumpenable = 0;

	LOAD_SQL_FUNCTION_PTR(SQLautocommit);
	LOAD_SQL_FUNCTION_PTR(SQLexitClient);
	LOAD_SQL_FUNCTION_PTR(SQLinitClient);
	LOAD_SQL_FUNCTION_PTR(SQLstatementIntern);
	LOAD_SQL_FUNCTION_PTR(SQLdestroyResult);

	if (retval != MAL_SUCCEED) {
		printf("Failed to load SQL function: %s\n", retval);
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
		mserver_reset(0);
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
	printf("Successfully initialized MonetDB.\n");
	for(i = 0; i < 10; i++) {
		monetdb_shutdown();
		printf("Successfully shutdown MonetDB.\n");
		retval = monetdb_initialize();
		if (retval != MAL_SUCCEED) {
			printf("Failed MonetDB restart: %s\n", retval);
			return -1;
		}
		printf("Successfully restarted MonetDB.\n");
		c = (Client) monetdb_connect();
		monetdb_query(c, "SELECT * FROM temporary_table;");
		monetdb_query(c, "DROP TABLE temporary_table;");
		monetdb_query(c, "CREATE TABLE temporary_table(i INTEGER);");
		monetdb_query(c, "INSERT INTO temporary_table VALUES (3), (4);");
		monetdb_disconnect(c);
	}
	return 0;
}
