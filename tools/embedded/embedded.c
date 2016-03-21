/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * H. Muehleisen, M. Raasveldt
 * Inverse RAPI
 */

#include "embedded.h"

#include "monetdb_config.h"
#include "monet_options.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_linker.h"
#include "msabaoth.h"
#include "sql_scenario.h"
#include "gdk_utils.h"
#include "sql_scenario.h"
#include "sql_execute.h"
#include "sql.h"
#include "sql_mvc.h"
#include "res_table.h"
#include "sql_scenario.h"

#include <locale.h>

int monetdb_embedded_initialized = 0;

FILE* embedded_stdout;
FILE* embedded_stderr;

void* monetdb_connect(void) {
	Client conn = NULL;
	if (!monetdb_embedded_initialized) {
		return NULL;
	}
	conn = MCforkClient(&mal_clients[0]);
	if (!MCvalid((Client) conn)) {
		return NULL;
	}
	if (SQLinitClient(conn) != MAL_SUCCEED) {
		return NULL;
	}
	((backend *) conn->sqlcontext)->mvc->session->auto_commit = 1;
	return conn;
}

void monetdb_disconnect(void* conn) {
	if (!MCvalid((Client) conn)) {
		return;
	}
	MCcloseClient((Client) conn);
}

#ifdef WIN32
#define NULLFILE "nul"
#else
#define NULLFILE "/dev/null"
#endif

char* monetdb_startup(char* dbdir, char silent, char sequential) {
	opt *set = NULL;
	volatile int setlen = 0;
	str retval = MAL_SUCCEED;
	char* sqres = NULL;
	void* res = NULL;
	void* c;

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

	if (monetdb_embedded_initialized) goto cleanup;

	embedded_stdout = fopen(NULLFILE, "w");
	embedded_stderr = fopen(NULLFILE, "w");

	setlen = mo_builtin_settings(&set);
	setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_dbpath", dbdir);

	BBPaddfarm(dbdir, (1 << PERSISTENT) | (1 << TRANSIENT));
	if (GDKinit(set, setlen) == 0) {
		retval = GDKstrdup("GDKinit() failed");
		goto cleanup;
	}
	GDKsetenv("monet_mod_path", "");
	GDKsetenv("mapi_disable", "true");
	if (sequential) {
		GDKsetenv("sql_optimizer", "sequential_pipe");
	}

	if (silent) THRdata[0] = stream_blackhole_create();
	msab_dbpathinit(dbdir);

	if (mal_init() != 0) { // mal_init() does not return meaningful codes on failure
		retval = GDKstrdup("mal_init() failed");
		goto cleanup;
	}

	if (silent) mal_clients[0].fdout = THRdata[0];

	monetdb_embedded_initialized = true;
	c = monetdb_connect();
	if (c == NULL) {
		monetdb_embedded_initialized = false;
		retval = GDKstrdup("Failed to initialize client");
		goto cleanup;
	}
	GDKfataljumpenable = 0;

	// we do not want to jump after this point, since we cannot do so between threads
	// sanity check, run a SQL query
	sqres = monetdb_query(c, "SELECT * FROM tables;", 1, &res);
	if (sqres != NULL) {
		monetdb_embedded_initialized = false;
		retval = sqres;
		goto cleanup;
	}
	monetdb_cleanup_result(c, res);
	monetdb_disconnect(c);
cleanup:
	mo_free_options(set, setlen);
	return retval;
}

char* monetdb_query(void* conn, char* query, char execute, void** result) {
	str res = MAL_SUCCEED;
	Client c = (Client) conn;
	mvc* m;
	if (!monetdb_embedded_initialized) {
		return GDKstrdup("Embedded MonetDB is not started");
	}
	if (!MCvalid((Client) conn)) {
		return GDKstrdup("Invalid connection");
	}
	m = ((backend *) c->sqlcontext)->mvc;

	while (*query == ' ' || *query == '\t') query++;
	if (strncasecmp(query, "START", 5) == 0) { // START TRANSACTION
		m->session->auto_commit = 0;
		m->session->status = 0;
	}
	else if (strncasecmp(query, "ROLLBACK", 8) == 0) {
		m->session->status = -1;
		m->session->auto_commit = 1;
	}
	else if (strncasecmp(query, "COMMIT", 6) == 0) {
		m->session->auto_commit = 1;
	}
	else if (strncasecmp(query, "SHIBBOLEET", 10) == 0) {
		res = GDKstrdup("\x46\x6f\x72\x20\x69\x6d\x6d\x65\x64\x69\x61\x74\x65\x20\x74\x65\x63\x68\x6e\x69\x63\x61\x6c\x20\x73\x75\x70\x70\x6f\x72\x74\x20\x63\x61\x6c\x6c\x20\x2b\x33\x31\x20\x32\x30\x20\x35\x39\x32\x20\x34\x30\x33\x39");
	}
	else if (m->session->status < 0 && m->session->auto_commit == 0){
		res = GDKstrdup("Current transaction is aborted (please ROLLBACK)");
	} else {
		res = SQLstatementIntern(c, &query, "name", execute, 0, (res_table **) result);
	}
	SQLautocommit(c, m);
	return res;
}

char* monetdb_append(void* conn, const char* schema, const char* table, append_data *data, int ncols) {
	int i;
	int nvar = 6; // variables we need to make up
	MalBlkRecord mb;
	MalStack*     stk = NULL;
	InstrRecord*  pci = NULL;
	str res = MAL_SUCCEED;
	VarRecord bat_varrec;
	Client c = (Client) conn;
	mvc* m;

	if (!monetdb_embedded_initialized) {
		return GDKstrdup("Embedded MonetDB is not started");
	}
	if(table == NULL || data == NULL || ncols < 1) {
		return GDKstrdup("Invalid parameters");
	}
	if (!MCvalid((Client) conn)) {
		return GDKstrdup("Invalid connection");
	}
	m = ((backend *) c->sqlcontext)->mvc;

	// very black MAL magic below
	mb.var = GDKmalloc(nvar * sizeof(VarRecord*));
	stk = GDKmalloc(sizeof(MalStack) + nvar * sizeof(ValRecord));
	pci = GDKmalloc(sizeof(InstrRecord) + nvar * sizeof(int));
	assert(mb.var != NULL && stk != NULL && pci != NULL); // cough, cough
	bat_varrec.type = TYPE_bat;
	for (i = 0; i < nvar; i++) {
		pci->argv[i] = i;
	}
	stk->stk[0].vtype = TYPE_int;
	stk->stk[2].val.sval = (str) schema;
	stk->stk[2].vtype = TYPE_str;
	stk->stk[3].val.sval = (str) table;
	stk->stk[3].vtype = TYPE_str;
	stk->stk[4].vtype = TYPE_str;
	stk->stk[5].vtype = TYPE_bat;
	mb.var[5] = &bat_varrec;
	if (!m->session->active) mvc_trans(m);
	for (i=0; i < ncols; i++) {
		append_data ad = data[i];
		stk->stk[4].val.sval = ad.colname;
		stk->stk[5].val.bval = ad.batid;

		res = mvc_append_wrap(c, &mb, stk, pci);
		if (res != NULL) {
			break;
		}
	}
	if (res == MAL_SUCCEED) {
		sqlcleanup(m, 0);
	}
	GDKfree(mb.var);
	GDKfree(stk);
	GDKfree(pci);
	return res;
}

void  monetdb_cleanup_result(void* conn, void* output) {
	(void) conn; // not needing conn here (but perhaps someday)
	res_table_destroy((res_table*) output);
}

str monetdb_get_columns(void* conn, const char* schema_name, const char *table_name, int *column_count, char ***column_names, int **column_types) {
	mvc *m;
	sql_schema *s;
	sql_table *t;
	char *msg = MAL_SUCCEED;
	int columns;
	node *n;
	Client c = (Client) conn;

	assert(column_count != NULL && column_names != NULL && column_types != NULL);

	if ((msg = getSQLContext(c, NULL, &m, NULL)) != NULL)
		return msg;

	s = mvc_bind_schema(m, schema_name);
	if (s == NULL)
		return createException(MAL, "embedded", "Missing schema!");
	t = mvc_bind_table(m, s, table_name);
	if (t == NULL)
		return createException(MAL, "embedded", "Could not find table %s", table_name);

	columns = t->columns.set->cnt;
	*column_count = columns;
	*column_names = GDKzalloc(sizeof(char*) * columns);
	*column_types = GDKzalloc(sizeof(int) * columns);

	if (*column_names == NULL || *column_types == NULL) {
		return MAL_MALLOC_FAIL;
	}

	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		(*column_names)[c->colnr] = c->base.name;
		(*column_types)[c->colnr] = c->type.type->localtype;
	}

	return msg;
}

// TODO: fix this, it is not working correctly
void monetdb_shutdown(void) {
	// kill SQL
	// SQLepilogue(NULL);
	// kill MAL & GDK
	// mal_exit();
	// monetdb_embedded_initialized = 0;
}
