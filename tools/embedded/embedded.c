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

typedef str (*SQLstatementIntern_ptr_tpe)(Client, str*, str, bit, bit, res_table**);
SQLstatementIntern_ptr_tpe SQLstatementIntern_ptr = NULL;
typedef str (*SQLautocommit_ptr_tpe)(Client, mvc*);
SQLautocommit_ptr_tpe SQLautocommit_ptr = NULL;
typedef str (*SQLinitClient_ptr_tpe)(Client);
SQLinitClient_ptr_tpe SQLinitClient_ptr = NULL;
typedef str (*getSQLContext_ptr_tpe)(Client, MalBlkPtr, mvc**, backend**);
getSQLContext_ptr_tpe getSQLContext_ptr = NULL;
typedef void (*res_table_destroy_ptr_tpe)(res_table *t);
res_table_destroy_ptr_tpe res_table_destroy_ptr = NULL;
typedef str (*mvc_append_wrap_ptr_tpe)(Client, MalBlkPtr, MalStkPtr, InstrPtr);
mvc_append_wrap_ptr_tpe mvc_append_wrap_ptr = NULL;
typedef sql_schema* (*mvc_bind_schema_ptr_tpe)(mvc*, const char*);
mvc_bind_schema_ptr_tpe mvc_bind_schema_ptr = NULL;
typedef sql_table* (*mvc_bind_table_ptr_tpe)(mvc*, sql_schema*, const char*);
mvc_bind_table_ptr_tpe mvc_bind_table_ptr = NULL;
typedef int (*sqlcleanup_ptr_tpe)(mvc*, int);
sqlcleanup_ptr_tpe sqlcleanup_ptr = NULL;
typedef void (*mvc_trans_ptr_tpe)(mvc*);
mvc_trans_ptr_tpe mvc_trans_ptr = NULL;

static MT_Lock monetdb_embedded_lock;
int monetdb_embedded_initialized = 0;

static void* lookup_function(char* func) {
	void *dl, *fun;
	dl = mdlopen("libmonetdb5", RTLD_NOW | RTLD_GLOBAL);
	if (dl == NULL) {
		return NULL;
	}
	fun = dlsym(dl, func);
	dlclose(dl);
	return fun;
}

void* monetdb_connect() {
	Client c = MCforkClient(&mal_clients[0]);
	if ((*SQLinitClient_ptr)(c) != MAL_SUCCEED) {
		return NULL;
	}
	((backend *) c->sqlcontext)->mvc->session->auto_commit = 1;
	return c;
}

void monetdb_disconnect(void* conn) {
	if (conn == NULL) {
		return;
	}
	MCcloseClient((Client) conn);
}

char* monetdb_startup(char* installdir, char* dbdir, char silent) {
	opt *set = NULL;
	int setlen = 0;
	str retval = MAL_SUCCEED;
	char* sqres = NULL;
	void* res = NULL;
	void* c;
	char mod_path[1000];
	GDKfataljumpenable = 1;
	if(setjmp(GDKfataljump) != 0) {
		retval = GDKfatalmsg;
		// we will get here if GDKfatal was called.
		if (retval != NULL) {
			retval = GDKstrdup("GDKfatal() with unspecified error?");
		}
		goto cleanup;
	}
	MT_lock_init(&monetdb_embedded_lock, "monetdb_embedded_lock");
	MT_lock_set(&monetdb_embedded_lock, "monetdb.startup");
	if (monetdb_embedded_initialized) goto cleanup;

	setlen = mo_builtin_settings(&set);
	setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_dbpath", dbdir);
	BBPaddfarm(dbdir, (1 << PERSISTENT) | (1 << TRANSIENT));

	if (GDKinit(set, setlen) == 0) {
		retval = GDKstrdup("GDKinit() failed");
		goto cleanup;
	}
	snprintf(mod_path, 1000, "%s/lib/monetdb5", installdir);
	GDKsetenv("monet_mod_path", mod_path);
	GDKsetenv("mapi_disable", "true");
	GDKsetenv("max_clients", "0");
	// TODO: SELECT * FROM table should not use mitosis in the first place (?).
	GDKsetenv("sql_optimizer", "sequential_pipe");

	if (silent) THRdata[0] = stream_blackhole_create();
	msab_dbpathinit(dbdir);

	if (mal_init() != 0) { // mal_init() does not return meaningful codes on failure
		retval = GDKstrdup("mal_init() failed");
		goto cleanup;
	}

	if (silent) mal_clients[0].fdout = THRdata[0];

	// This dynamically looks up functions, because the libraries containing them are loaded at runtime.
	SQLstatementIntern_ptr = (SQLstatementIntern_ptr_tpe) lookup_function("SQLstatementIntern");
	SQLautocommit_ptr      = (SQLautocommit_ptr_tpe)      lookup_function("SQLautocommit");
	SQLinitClient_ptr      = (SQLinitClient_ptr_tpe)      lookup_function("SQLinitClient");
	getSQLContext_ptr      = (getSQLContext_ptr_tpe)      lookup_function("getSQLContext");
	res_table_destroy_ptr  = (res_table_destroy_ptr_tpe)  lookup_function("res_table_destroy");
	mvc_append_wrap_ptr    = (mvc_append_wrap_ptr_tpe)    lookup_function("mvc_append_wrap");
	mvc_bind_schema_ptr    = (mvc_bind_schema_ptr_tpe)    lookup_function("mvc_bind_schema");
	mvc_bind_table_ptr     = (mvc_bind_table_ptr_tpe)     lookup_function("mvc_bind_table");
	sqlcleanup_ptr         = (sqlcleanup_ptr_tpe)         lookup_function("sqlcleanup");
	mvc_trans_ptr          = (mvc_trans_ptr_tpe)          lookup_function("mvc_trans");

	if (SQLstatementIntern_ptr == NULL || SQLautocommit_ptr == NULL ||
			SQLinitClient_ptr == NULL || getSQLContext_ptr == NULL ||
			res_table_destroy_ptr == NULL || mvc_append_wrap_ptr == NULL ||
			mvc_bind_schema_ptr == NULL || mvc_bind_table_ptr == NULL ||
			sqlcleanup_ptr == NULL || mvc_trans_ptr == NULL) {
		retval = GDKstrdup("Dynamic function lookup failed");
		goto cleanup;
	}

	c = monetdb_connect();
	if (c == NULL) {
		retval = GDKstrdup("Failed to initialize client");
		goto cleanup;
	}
	monetdb_embedded_initialized = true;
	// we do not want to jump after this point, since we cannot do so between threads
	GDKfataljumpenable = 0;
	// sanity check, run a SQL query
	sqres = monetdb_query(c, "SELECT * FROM tables;", &res);
	if (sqres != NULL) {
		monetdb_embedded_initialized = false;
		retval = sqres;
		goto cleanup;
	}
	monetdb_cleanup_result(c, res);
	monetdb_disconnect(c);
cleanup:
	mo_free_options(set, setlen);
	MT_lock_unset(&monetdb_embedded_lock, "monetdb.startup");
	return retval;
}

char* monetdb_query(void* conn, char* query, void** result) {
	str res = MAL_SUCCEED;
	Client c = (Client) conn;
	mvc* m = ((backend *) c->sqlcontext)->mvc;
	if (!monetdb_embedded_initialized) {
		return GDKstrdup("Embedded MonetDB is not started");
	}

	while (*query == ' ' || *query == '\t') query++;
	if (strncasecmp(query, "START", 5) == 0) { // START TRANSACTION
		m->session->auto_commit = 0;
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
	else if (m->session->status < 0 && m->session->auto_commit ==0){
		res = GDKstrdup("Current transaction is aborted (please ROLLBACK)");
	} else {
		res = (*SQLstatementIntern_ptr)(c, &query, "name", 1, 0, (res_table **) result);
	}

	(*SQLautocommit_ptr)(c, m);
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
	mvc* m = ((backend *) c->sqlcontext)->mvc;

	assert(table != NULL && data != NULL && ncols > 0);

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
	if (!m->session->active) (*mvc_trans_ptr)(m);
	for (i=0; i < ncols; i++) {
		append_data ad = data[i];
		stk->stk[4].val.sval = ad.colname;
		stk->stk[5].val.bval = ad.batid;

		res = (*mvc_append_wrap_ptr)(c, &mb, stk, pci);
		if (res != NULL) {
			break;
		}
	}
	if (res == MAL_SUCCEED) {
		(*sqlcleanup_ptr)(m, 0);
	}
	GDKfree(mb.var);
	GDKfree(stk);
	GDKfree(pci);
	return res;
}

void  monetdb_cleanup_result(void* conn, void* output) {
	(void) conn; // not needing conn here (but perhaps someday)
	(*res_table_destroy_ptr)((res_table*) output);
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

	if ((msg = (*getSQLContext_ptr)(c, NULL, &m, NULL)) != NULL)
		return msg;

	s = (*mvc_bind_schema_ptr)(m, schema_name);
	if (s == NULL)
		return createException(MAL, "embedded", "Missing schema!");
	t = (*mvc_bind_table_ptr)(m, s, table_name);
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
