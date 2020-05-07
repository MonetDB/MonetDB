/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 * H. Muehleisen, M. Raasveldt
 * Inverse RAPI
 */

#include "monetdb_config.h"

#include "monetdb_embedded.h"
#include "gdk.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_embedded.h"
#include "mtime.h"
#include "blob.h"
#include "sql_mvc.h"
#include "sql_catalog.h"
#include "sql_gencode.h"
#include "sql_semantic.h"
#include "sql_scenario.h"
#include "sql_optimizer.h"
#include "rel_exp.h"
#include "rel_rel.h"
#include "rel_updates.h"
#include "monet_options.h"
#include "msabaoth.h"

// Hack to get code to compile
#define MT_RWLock MT_Lock
#define MT_RWLOCK_INITIALIZER MT_LOCK_INITIALIZER
#define MT_rwlock_read_set MT_lock_unset
#define MT_rwlock_read_unset MT_lock_unset
#define MT_rwlock_write_set MT_lock_unset
// define MT_fprintf_silent 
//#define SQLisInitialized
#define MT_rwlock_write_unset MT_rwlock_write_set

#define UNUSED(x) (void)(x)

typedef struct {
	monetdb_result res;
	res_table *monetdb_resultset;
	monetdb_column **converted_columns;
} monetdb_result_internal;

static MT_RWLock embedded_lock = MT_RWLOCK_INITIALIZER("embedded_lock");
static bool monetdb_embedded_initialized = false;

bool
monetdb_is_initialized(void)
{
	MT_rwlock_read_set(&embedded_lock);
	bool res = monetdb_embedded_initialized;
	MT_rwlock_read_unset(&embedded_lock);
	return res;
}

static char*
commit_action(mvc* m, char* msg)
{
    char *commit_msg = SQLautocommit(m);
	if ((msg != MAL_SUCCEED || commit_msg != MAL_SUCCEED)) {
		if (msg == MAL_SUCCEED)
			msg = commit_msg;
		else if (commit_msg)
			freeException(commit_msg);
	}
	return msg;
}

static char*
validate_connection(monetdb_connection conn, const char* call) // Call this function always inside the embedded_lock
{
	if (!monetdb_embedded_initialized)
		return createException(MAL, call, "MonetDBe has not yet started");
	if (!MCvalid((Client) conn))
		return createException(MAL, call, "Invalid connection");
	return MAL_SUCCEED;
}

static void
monetdb_destroy_column(monetdb_column* column)
{
	size_t j;

	if (!column)
		return;

	if (column->type == monetdb_str) {
		// FIXME: clean up individual strings
		char** data = (char**)column->data;
		for(j = 0; j < column->count; j++) {
			if (data[j])
				GDKfree(data[j]);
		}
	} else if (column->type == monetdb_blob) {
		monetdb_data_blob* data = (monetdb_data_blob*)column->data;
		for(j = 0; j < column->count; j++) {
			if (data[j].data)
				GDKfree(data[j].data);
		}
	}
	GDKfree(column->data);
	GDKfree(column);
}

static char*
monetdb_cleanup_result_internal(monetdb_connection conn, monetdb_result* result)
{
	char* msg = MAL_SUCCEED;
	monetdb_result_internal* res = (monetdb_result_internal *) result;
	Client c = (Client) conn;

	mvc* m = NULL;

	if ((msg = validate_connection(conn, "embedded.monetdb_cleanup_result_internal")) != MAL_SUCCEED)
		return msg;
	if ((msg = getSQLContext(c, NULL, &m, NULL)) != MAL_SUCCEED)
		goto cleanup;
	if (!result) {
		msg = createException(MAL, "embedded.monetdb_cleanup_result_internal", "Parameter result is NULL");
		goto cleanup;
	}

	if (res->monetdb_resultset)
		res_tables_destroy(res->monetdb_resultset);

	if (res->converted_columns) {
		for (size_t i = 0; i < res->res.ncols; i++)
			monetdb_destroy_column(res->converted_columns[i]);
		GDKfree(res->converted_columns);
	}
	GDKfree(res);

cleanup:
	return commit_action(m, msg);
}

static char*
monetdb_query_internal(monetdb_connection conn, char* query, monetdb_result** result, lng* affected_rows,
					   int* prepare_id, char language)
{
	char* msg = MAL_SUCCEED, *commit_msg = MAL_SUCCEED, *nq = NULL;
	Client c = (Client) conn;
	mvc* m = NULL;
	backend *b;
	size_t query_len, input_query_len;
	buffer query_buf;
	stream *query_stream;
	monetdb_result_internal *res_internal = NULL;
	bstream *old_bstream = NULL;
	stream *fdout = c->fdout;

	if ((msg = validate_connection(conn, "embedded.monetdb_query_internal")) != MAL_SUCCEED)
		return msg;

	old_bstream = c->fdin;
	if ((msg = getSQLContext(c, NULL, &m, NULL)) != MAL_SUCCEED)
		goto cleanup;
	b = (backend *) c->sqlcontext;

	if (!query) {
		msg = createException(MAL, "embedded.monetdb_query_internal", "Query missing");
		goto cleanup;
	}
	if (!(query_stream = buffer_rastream(&query_buf, "sqlstatement"))) {
		msg = createException(MAL, "embedded.monetdb_query_internal", "Could not setup query stream");
		goto cleanup;
	}
	input_query_len = strlen(query);
	query_len = input_query_len + 3;
	if (!(nq = GDKmalloc(query_len))) {
		msg = createException(MAL, "embedded.monetdb_query_internal", "Could not setup query stream");
		goto cleanup;
	}
	strcpy(nq, query);
	strcpy(nq + input_query_len, "\n;");

	query_buf.pos = 0;
	query_buf.len = query_len;
	query_buf.buf = nq;

	if (!(c->fdin = bstream_create(query_stream, query_len))) {
		msg = createException(MAL, "embedded.monetdb_query_internal", "Could not setup query stream");
		goto cleanup;
	}
	if (bstream_next(c->fdin) < 0) {
		msg = createException(MAL, "embedded.monetdb_query_internal", "Internal error while starting the query");
		goto cleanup;
	}

	assert(language);
	b->language = language;
	b->output_format = OFMT_NONE;
	m->user_id = m->role_id = USER_MONETDB;
	m->errstr[0] = '\0';
	m->params = NULL;
	m->argc = 0;
	m->sym = NULL;
	m->label = 0;
	m->no_mitosis = 0;
	if (m->sa)
		m->sa = sa_reset(m->sa);
	m->scanner.mode = LINE_N;
	m->scanner.rs = c->fdin;
	scanner_query_processed(&(m->scanner));

	if ((msg = MSinitClientPrg(c, "user", "main")) != MAL_SUCCEED)
		goto cleanup;
	if ((msg = SQLparser(c)) != MAL_SUCCEED)
		goto cleanup;
	if (prepare_id && m->emode == m_prepare)
		*prepare_id = b->q->id;
	c->fdout = NULL;
	if ((msg = SQLengine(c)) != MAL_SUCCEED)
		goto cleanup;
	if (!m->results && m->rowcnt >= 0 && affected_rows)
		*affected_rows = m->rowcnt;

	if (result) {
		if (!(res_internal = GDKzalloc(sizeof(monetdb_result_internal)))) {
			msg = createException(MAL, "embedded.monetdb_query_internal", MAL_MALLOC_FAIL);
			goto cleanup;
		}
		if (m->emode == m_execute)
			res_internal->res.type = (m->results) ? Q_TABLE : Q_UPDATE;
		else if (m->emode & m_prepare)
			res_internal->res.type = Q_PREPARE;
		else
			res_internal->res.type = (m->results) ? m->results->query_type : m->type;
		res_internal->res.id = m->last_id;
		*result = (monetdb_result*) res_internal;
		m->reply_size = -2; /* do not clean up result tables */

		if (m->results) {
			res_internal->res.ncols = (size_t) m->results->nr_cols;
			if (m->results->nr_cols > 0 && m->results->order) {
				BAT* bb = BATdescriptor(m->results->order);
				if (!bb) {
					msg = createException(MAL, "embedded.monetdb_query_internal", RUNTIME_OBJECT_MISSING);
					goto cleanup;
				}
				res_internal->res.nrows = BATcount(bb);
				BBPunfix(bb->batCacheid);
			}
			res_internal->monetdb_resultset = m->results;
			res_internal->converted_columns = GDKzalloc(sizeof(monetdb_column*) * res_internal->res.ncols);
			if (!res_internal->converted_columns) {
				msg = createException(MAL, "embedded.monetdb_query_internal", MAL_MALLOC_FAIL);
				goto cleanup;
			}
			m->results = NULL;
		}
	}

cleanup:
	c->fdout = fdout;
	if (nq)
		GDKfree(nq);
	MSresetInstructions(c->curprg->def, 1);
	if (old_bstream) { //c->fdin was set
		bstream_destroy(c->fdin);
		c->fdin = old_bstream;
	}
	commit_msg = SQLautocommit(m); //need always to commit even if msg is set
	if ((msg != MAL_SUCCEED || commit_msg != MAL_SUCCEED)) {
		if (res_internal) {
			char* other = monetdb_cleanup_result_internal(conn, (monetdb_result*) res_internal);
			if (other)
				freeException(other);
		}
		if (result)
			*result = NULL;
		if (msg == MAL_SUCCEED) //if the error happened in the autocommit, set it as the returning error message
			msg = commit_msg;
		else if (commit_msg) //otherwise if msg is set, discard commit_msg
			freeException(commit_msg);
	}
	return msg;
}

static char*
monetdb_disconnect_internal(monetdb_connection conn)
{
	char* msg = MAL_SUCCEED;

	if ((msg = validate_connection(conn, "embedded.monetdb_disconnect_internal")) != MAL_SUCCEED)
		return msg;
	if ((msg = SQLexitClient((Client) conn)) != MAL_SUCCEED)
		return msg;
	MCcloseClient((Client) conn);
	return msg;
}

static char*
monetdb_connect_internal(monetdb_connection *conn)
{
	mvc *m;
	char* msg = MAL_SUCCEED;
	Client mc = NULL;

	if (!monetdb_embedded_initialized) {
		msg = createException(MAL, "embedded.monetdb_connect_internal", "Embedded MonetDB is not started");
		goto cleanup;
	}
	mc = MCinitClient((oid) 0, 0, 0);
	if (!MCvalid(mc)) {
		msg = createException(MAL, "embedded.monetdb_connect_internal", "Failed to initialize client");
		goto cleanup;
	}
	mc->curmodule = mc->usermodule = userModule();
	if (mc->usermodule == NULL) {
		msg = createException(MAL, "embedded.monetdb_connect_internal", "Failed to initialize client MAL module");
		goto cleanup;
	}
	if ((msg = SQLinitClient(mc)) != MAL_SUCCEED)
		goto cleanup;
	if ((msg = getSQLContext(mc, NULL, &m, NULL)) != MAL_SUCCEED)
		goto cleanup;
	m->session->auto_commit = 1;
	if (!m->sa)
		m->sa = sa_create();
	if (!m->sa) {
		msg = createException(SQL, "embedded.monetdb_connect_internal", MAL_MALLOC_FAIL);
		goto cleanup;
	}

cleanup:
	if (msg && mc) {
		char* other = monetdb_disconnect_internal(mc);
		if (other)
			freeException(other);
		*conn = NULL;
	} else if (conn)
		*conn = mc;
	return msg;
}

char*
monetdb_connect(monetdb_connection *conn)
{
	char* msg = MAL_SUCCEED;
	if (!conn)
		return createException(MAL, "embedded.monetdb_connect_internal", "monetdb_connection parameter is NULL");
	MT_rwlock_read_set(&embedded_lock);
	msg = monetdb_connect_internal(conn);
	MT_rwlock_read_unset(&embedded_lock);
	return msg;
}

char*
monetdb_disconnect(monetdb_connection conn)
{
	char* msg = MAL_SUCCEED;
	MT_rwlock_read_set(&embedded_lock);
	msg = monetdb_disconnect_internal(conn);
	MT_rwlock_read_unset(&embedded_lock);
	return msg;
}

static void
monetdb_shutdown_internal(void) // Call this function always inside the embedded_lock
{
	if (monetdb_embedded_initialized) {
            malEmbeddedReset();
		monetdb_embedded_initialized = false;
	}
}

char*
monetdb_startup(char* dbdir, bool sequential)
{
	char* msg = MAL_SUCCEED, *err;
	monetdb_result* res = NULL;
	void* c;
	opt *set = NULL;
	int setlen;
	gdk_return gdk_res;

	MT_rwlock_write_set(&embedded_lock);
	GDKfataljumpenable = 1;
	if(setjmp(GDKfataljump) != 0) {
		msg = GDKfatalmsg;
		// we will get here if GDKfatal was called.
		if (msg == NULL)
			msg = createException(MAL, "embedded.monetdb_startup", "GDKfatal() with unspecified error");
		goto cleanup;
	}

	if (monetdb_embedded_initialized) {
		msg = createException(MAL, "embedded.monetdb_startup", "MonetDBe is already initialized");
		goto done;
	}

        //hacked
	// MT_fprintf_silent(silent);

	if ((setlen = mo_builtin_settings(&set)) == 0) {
		msg = createException(MAL, "embedded.monetdb_startup", MAL_MALLOC_FAIL);
		goto cleanup;
	}
	if (dbdir && (setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_dbpath", dbdir)) == 0) {
		mo_free_options(set, setlen);
		msg = createException(MAL, "embedded.monetdb_startup", MAL_MALLOC_FAIL);
		goto cleanup;
	}
	if (sequential)
		setlen = mo_add_option(&set, setlen, opt_cmdline, "sql_optimizer", "sequential_pipe");
	else
		setlen = mo_add_option(&set, setlen, opt_cmdline, "sql_optimizer", "default_pipe");
	if (setlen == 0) {
		mo_free_options(set, setlen);
		msg = createException(MAL, "embedded.monetdb_startup", MAL_MALLOC_FAIL);
		goto cleanup;
	}
	if (!dbdir) { /* in-memory */
		if (BBPaddfarm(NULL, (1 << PERSISTENT) | (1 << TRANSIENT)) != GDK_SUCCEED) {
			mo_free_options(set, setlen);
			msg = createException(MAL, "embedded.monetdb_startup", "Cannot add in-memory farm");
			goto cleanup;
		}
	} else {
		if (BBPaddfarm(dbdir, 1 << PERSISTENT) != GDK_SUCCEED ||
			BBPaddfarm(/*dbextra ? dbextra : */dbdir, 1 << TRANSIENT) != GDK_SUCCEED) {
			mo_free_options(set, setlen);
			msg = createException(MAL, "embedded.monetdb_startup", "Cannot add farm %s", dbdir);
			goto cleanup;
		}
		if (GDKcreatedir(dbdir) != GDK_SUCCEED) {
			mo_free_options(set, setlen);
			msg = createException(MAL, "embedded.monetdb_startup", "Cannot create directory %s", dbdir);
			goto cleanup;
		}
		msab_dbpathinit(dbdir);
		if ((err = msab_wildRetreat()) != NULL) {
			mo_free_options(set, setlen);
			msg = createException(MAL, "embedded.monetdb_startup", "%s", err);
			free(err);
			goto cleanup;
		}
		if ((err = msab_registerStarting()) != NULL) {
			mo_free_options(set, setlen);
			msg = createException(MAL, "embedded.monetdb_startup", "%s", err);
			free(err);
			goto cleanup;
		}
	}
	gdk_res = GDKinit(set, setlen, 1);
	mo_free_options(set, setlen);
	if (gdk_res == GDK_FAIL) {
		msg = createException(MAL, "embedded.monetdb_startup", "GDKinit() failed");
		goto cleanup;
	}
	if ((msg = malEmbeddedBoot()) != MAL_SUCCEED)
		goto cleanup;
        //[FIX]: still exists in cmake monetdblite branch
	//if (!SQLisInitialized()) {
	//	msg = createException(MAL, "embedded.monetdb_startup", "SQL initialization failed");
	//	goto cleanup;
	//}

	monetdb_embedded_initialized = true;

	if ((msg = monetdb_connect_internal(&c)) != MAL_SUCCEED)
		goto cleanup;
	GDKfataljumpenable = 0;
	// we do not want to jump after this point, since we cannot do so between threads sanity check, run a SQL query
	if ((msg = monetdb_query_internal(c, "SELECT id FROM _tables LIMIT 1;", &res, NULL, NULL, 'S')) != MAL_SUCCEED)
		goto cleanup;
	if ((msg = monetdb_cleanup_result_internal(c, res)) != MAL_SUCCEED)
		goto cleanup;
	msg = monetdb_disconnect_internal(c);

cleanup:
	if (msg)
		monetdb_shutdown_internal();
done:
	MT_rwlock_write_unset(&embedded_lock);
	return msg;
}

char*
monetdb_clear_prepare(monetdb_connection conn, int id)
{
	char query[64], *msg;

	sprintf(query, "release %d", id); //no need to validate at this level
	MT_rwlock_read_set(&embedded_lock);
	msg = monetdb_query_internal(conn, query, NULL, NULL, NULL, 'X');
	MT_rwlock_read_unset(&embedded_lock);
	return msg;
}

char*
monetdb_send_close(monetdb_connection conn, int id)
{
	char query[64], *msg;

	sprintf(query, "close %d", id); //no need to validate at this level
	MT_rwlock_read_set(&embedded_lock);
	msg = monetdb_query_internal(conn, query, NULL, NULL, NULL, 'X');
	MT_rwlock_read_unset(&embedded_lock);
	return msg;
}

char*
monetdb_get_autocommit(monetdb_connection conn, int* result)
{
	mvc *m;
	char *msg = MAL_SUCCEED;
	Client connection = (Client) conn;

	MT_rwlock_read_set(&embedded_lock);
	if ((msg = validate_connection(conn, "embedded.monetdb_get_autocommit")) != MAL_SUCCEED) {
		MT_rwlock_read_unset(&embedded_lock);
		return msg;
	}

	if (!result) {
		msg = createException(MAL, "embedded.monetdb_get_autocommit", "Parameter result is NULL");
		goto cleanup;
	}

	m = ((backend *) connection->sqlcontext)->mvc;
	*result = m->session->auto_commit;
cleanup:
	MT_rwlock_read_unset(&embedded_lock);
	return msg;
}

char*
monetdb_set_autocommit(monetdb_connection conn, int value)
{
	char query[64], *msg;

	sprintf(query, "auto_commit %d", value); //no need to validate at this level
	MT_rwlock_read_set(&embedded_lock);
	msg = monetdb_query_internal(conn, query, NULL, NULL, NULL, 'X');
	MT_rwlock_read_unset(&embedded_lock);
	return msg;
}

char*
monetdb_query(monetdb_connection conn, char* query, monetdb_result** result, lng* affected_rows, int* prepare_id)
{
	char* msg;
	MT_rwlock_read_set(&embedded_lock);
	msg = monetdb_query_internal(conn, query, result, affected_rows, prepare_id, 'S');
	MT_rwlock_read_unset(&embedded_lock);
	return msg;
}

char*
monetdb_append(monetdb_connection conn, const char* schema, const char* table, bat *batids, size_t column_count)
{
	Client c = (Client) conn;
	mvc *m;
	char* msg = MAL_SUCCEED;

	MT_rwlock_read_set(&embedded_lock);
	if ((msg = validate_connection(conn, "embedded.monetdb_append")) != MAL_SUCCEED) {
		MT_rwlock_read_unset(&embedded_lock);
		return msg; //The connection is invalid, there is no transaction going
	}

	if ((msg = getSQLContext(c, NULL, &m, NULL)) != MAL_SUCCEED)
		goto cleanup;
        if ((msg = SQLtrans(m)) != MAL_SUCCEED)
		goto cleanup;
	if (table == NULL) {
		msg = createException(MAL, "embedded.monetdb_append", "table parameter is NULL");
		goto cleanup;
	}
	if (batids == NULL) {
		msg = createException(MAL, "embedded.monetdb_append", "batids parameter is NULL");
		goto cleanup;
	}
	if (column_count < 1) {
		msg = createException(MAL, "embedded.monetdb_append", "column_count must be higher than 0");
		goto cleanup;
	}
	if (!m->sa)
		m->sa = sa_create();
	if (!m->sa) {
		msg = createException(SQL, "embedded.monetdb_append", MAL_MALLOC_FAIL);
		goto cleanup;
	}
	{
		node *n;
		size_t i;
		sql_rel *rel;
                // [FIX]:
                UNUSED(rel);
		list *exps = sa_list(m->sa), *args = sa_list(m->sa), *col_types = sa_list(m->sa);
		sql_schema *s;
		sql_table *t;
		sql_subfunc *f = sql_find_func(m->sa, mvc_bind_schema(m, "sys"), "append", 1, F_UNION, NULL);

		assert(f);
		if (schema) {
			if (!(s = mvc_bind_schema(m, schema))) {
				msg = createException(MAL, "embedded.monetdb_append", "Schema missing %s", schema);
				goto cleanup;
			}
		} else {
			s = cur_schema(m);
		}
		if (!(t = mvc_bind_table(m, s, table))) {
			msg = createException(SQL, "embedded.monetdb_append", "Table missing %s.%s", schema, table);
			goto cleanup;
		}
		if (column_count != (size_t)list_length(t->columns.set)) {
			msg = createException(SQL, "embedded.monetdb_append", "Incorrect number of columns");
			goto cleanup;
		}
		for (i = 0, n = t->columns.set->h; i < column_count && n; i++, n = n->next) {
			sql_column *col = n->data;
			list_append(args, exp_atom_lng(m->sa, (lng) batids[i]));
			list_append(exps, exp_column(m->sa, t->base.name, col->base.name, &col->type, CARD_MULTI, col->null, 0));
			list_append(col_types, &col->type);
		}

		f->res = col_types;
		rel = rel_insert(m, rel_basetable(m, t, t->base.name), rel_table_func(m->sa, NULL, exp_op(m->sa,  args, f), exps, 1));
		assert(rel);
		m->scanner.rs = NULL;
		m->errstr[0] = '\0';
                // [FIX]:
		//if (backend_dumpstmt((backend *) c->sqlcontext, c->curprg->def, rel, 1, 1, "append") < 0) {
		//	msg = createException(SQL, "embedded.monetdb_append", "Append plan generation failure");
		//	goto cleanup;
		//}
                //[FIX]:
		//if ((msg = SQLoptimizeQuery(c, c->curprg->def)) != MAL_SUCCEED)
		//	goto cleanup;
		//if ((msg = SQLengine(c)) != MAL_SUCCEED)
		//	goto cleanup;
	}
cleanup:
	msg = commit_action(m, msg);
	MT_rwlock_read_unset(&embedded_lock);
	return msg;
}

char*
monetdb_cleanup_result(monetdb_connection conn, monetdb_result* result)
{
	char* msg = MAL_SUCCEED;
	MT_rwlock_read_set(&embedded_lock);
	msg = monetdb_cleanup_result_internal(conn, result);
	MT_rwlock_read_unset(&embedded_lock);
	return msg;
}

char*
monetdb_get_table(monetdb_connection conn, sql_table** table, const char* schema_name, const char* table_name)
{
	mvc *m;
	sql_schema *s;
	char *msg = MAL_SUCCEED;
	Client connection = (Client) conn;

	MT_rwlock_read_set(&embedded_lock);
	if ((msg = validate_connection(conn, "embedded.monetdb_get_table")) != MAL_SUCCEED) {
		MT_rwlock_read_unset(&embedded_lock);
		return msg;
	}

	if ((msg = getSQLContext(connection, NULL, &m, NULL)) != NULL)
		goto cleanup;
	if ((msg = SQLtrans(m)) != MAL_SUCCEED)
		goto cleanup;
	if (!table) {
		msg = createException(MAL, "embedded.monetdb_get_table", "Parameter table is NULL");
		goto cleanup;
	}
	if (schema_name) {
		if (!(s = mvc_bind_schema(m, schema_name))) {
			msg = createException(MAL, "embedded.monetdb_get_table", "Could not find schema %s", schema_name);
			goto cleanup;
		}
	} else {
		s = cur_schema(m);
	}
	if (!(*table = mvc_bind_table(m, s, table_name))) {
		msg = createException(MAL, "embedded.monetdb_get_table", "Could not find table %s", table_name);
		goto cleanup;
	}

cleanup:
	msg = commit_action(m, msg);
	MT_rwlock_read_unset(&embedded_lock);
	return msg;
}

char*
monetdb_get_columns(monetdb_connection conn, const char* schema_name, const char *table_name, size_t *column_count,
					char ***column_names, int **column_types)
{
	mvc *m;
	sql_schema *s;
	sql_table *t;
	char* msg = MAL_SUCCEED;
	int columns;
	node *n;
	Client c = (Client) conn;

	MT_rwlock_read_set(&embedded_lock);
	if ((msg = validate_connection(conn, "embedded.monetdb_get_columns")) != MAL_SUCCEED) {
		MT_rwlock_read_unset(&embedded_lock);
		return msg;
	}

	if ((msg = getSQLContext(c, NULL, &m, NULL)) != MAL_SUCCEED)
		goto cleanup;
	if ((msg = SQLtrans(m)) != MAL_SUCCEED)
		goto cleanup;
	if (!column_count) {
		msg = createException(MAL, "embedded.monetdb_get_columns", "Parameter column_count is NULL");
		goto cleanup;
	}
	if (!column_names) {
		msg = createException(MAL, "embedded.monetdb_get_columns", "Parameter column_names is NULL");
		goto cleanup;
	}
	if (!column_types) {
		msg = createException(MAL, "embedded.monetdb_get_columns", "Parameter column_types is NULL");
		goto cleanup;
	}
	if (!table_name) {
		msg = createException(MAL, "embedded.monetdb_get_columns", "Parameter table_name is NULL");
		goto cleanup;
	}
	if (schema_name) {
		if (!(s = mvc_bind_schema(m, schema_name))) {
			msg = createException(MAL, "embedded.monetdb_get_columns", "Could not find schema %s", schema_name);
			goto cleanup;
		}
	} else {
		s = cur_schema(m);
	}
	if (!(t = mvc_bind_table(m, s, table_name))) {
		msg = createException(MAL, "embedded.monetdb_get_columns", "Could not find table %s", table_name);
		goto cleanup;
	}

	columns = t->columns.set->cnt;
	*column_count = columns;
	*column_names = GDKzalloc(sizeof(char*) * columns);
	*column_types = GDKzalloc(sizeof(int) * columns);
	if (*column_names == NULL || *column_types == NULL) {
		if (*column_names) {
			GDKfree(*column_names);
			*column_names = NULL;
		}
		if (*column_types) {
			GDKfree(*column_types);
			*column_types = NULL;
		}
		msg = createException(MAL, "embedded.monetdb_get_columns", MAL_MALLOC_FAIL);
		goto cleanup;
	}

	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *col = n->data;
		(*column_names)[col->colnr] = col->base.name;
		(*column_types)[col->colnr] = col->type.type->localtype;
	}

cleanup:
	msg = commit_action(m, msg);
	MT_rwlock_read_unset(&embedded_lock);
	return msg;
}

char*
monetdb_shutdown(void)
{
	char* msg = MAL_SUCCEED;
	MT_rwlock_write_set(&embedded_lock);
	if (monetdb_embedded_initialized)
		monetdb_shutdown_internal();
	else
		msg = createException(MAL, "embedded.monetdb_shutdown", "MonetDBe has not yet started");
	MT_rwlock_write_unset(&embedded_lock);
	return msg;
}

#define GENERATE_BASE_HEADERS(type, tpename) \
	static int tpename##_is_null(type value)

#define GENERATE_BASE_FUNCTIONS(tpe, tpename, mname) \
	GENERATE_BASE_HEADERS(tpe, tpename); \
	static int tpename##_is_null(tpe value) { return value == mname##_nil; }

GENERATE_BASE_FUNCTIONS(int8_t, int8_t, bte)
GENERATE_BASE_FUNCTIONS(int16_t, int16_t, sht)
GENERATE_BASE_FUNCTIONS(int32_t, int32_t, int)
GENERATE_BASE_FUNCTIONS(int64_t, int64_t, lng)
GENERATE_BASE_FUNCTIONS(size_t, size_t, oid)

GENERATE_BASE_FUNCTIONS(float, float, flt)
GENERATE_BASE_FUNCTIONS(double, double, dbl)

GENERATE_BASE_HEADERS(char*, str);
GENERATE_BASE_HEADERS(monetdb_data_blob, blob);

GENERATE_BASE_HEADERS(monetdb_data_date, date);
GENERATE_BASE_HEADERS(monetdb_data_time, time);
GENERATE_BASE_HEADERS(monetdb_data_timestamp, timestamp);

#define GENERATE_BAT_INPUT_BASE(tpe)                                           \
	monetdb_column_##tpe *bat_data = GDKzalloc(sizeof(monetdb_column_##tpe));  \
	if (!bat_data) {                                                           \
		msg = createException(MAL, "embedded.monetdb_result_fetch", MAL_MALLOC_FAIL); \
		goto cleanup;                                                          \
	}                                                                          \
	bat_data->type = monetdb_##tpe;                                            \
	bat_data->is_null = tpe##_is_null;                                         \
	bat_data->scale = pow(10, sqltpe->scale);                                  \
	column_result = (monetdb_column*) bat_data;

#define GENERATE_BAT_INPUT(b, tpe, mtype)                                      \
	{                                                                          \
		GENERATE_BAT_INPUT_BASE(tpe);                                          \
		bat_data->count = BATcount(b);                                         \
		bat_data->null_value = mtype##_nil;                                    \
		bat_data->data = GDKzalloc(bat_data->count * sizeof(bat_data->null_value)); \
		if (!bat_data->data) {                                                 \
			msg = createException(MAL, "embedded.monetdb_result_fetch", MAL_MALLOC_FAIL); \
			goto cleanup;                                                      \
		}                                                                      \
		size_t it = 0;                                                         \
		mtype* val = (mtype*)Tloc(b, 0);                                       \
		/* bat is dense, materialize it */                                     \
		for (it = 0; it < bat_data->count; it++, val++)                        \
			bat_data->data[it] = (tpe) *val;                                   \
	}

static void data_from_date(date d, monetdb_data_date *ptr);
static void data_from_time(daytime d, monetdb_data_time *ptr);
static void data_from_timestamp(timestamp d, monetdb_data_timestamp *ptr);

char*
monetdb_result_fetch(monetdb_connection conn, monetdb_column** res, monetdb_result* mres, size_t column_index)
{
	BAT* b = NULL;
	int bat_type;
	mvc* m;
	char* msg = MAL_SUCCEED;
	monetdb_result_internal* result = (monetdb_result_internal*) mres;
	sql_subtype* sqltpe = NULL;
	monetdb_column* column_result = NULL;
	size_t j = 0;
	Client c = (Client) conn;

	MT_rwlock_read_set(&embedded_lock);
	if ((msg = validate_connection(conn, "embedded.monetdb_result_fetch")) != MAL_SUCCEED) {
		MT_rwlock_read_unset(&embedded_lock);
		return msg;
	}

	if ((msg = getSQLContext(c, NULL, &m, NULL)) != MAL_SUCCEED)
		goto cleanup;
	if ((msg = SQLtrans(m)) != MAL_SUCCEED)
		goto cleanup;
	if (!res) {
		msg = createException(MAL, "embedded.monetdb_result_fetch", "Parameter res is NULL");
		goto cleanup;
	}
	if (column_index >= mres->ncols) {
		msg = createException(MAL, "embedded.monetdb_result_fetch", "Index out of range");
		goto cleanup;
	}
	// check if we have the column converted already
	if (result->converted_columns[column_index]) {
		*res = result->converted_columns[column_index];
		MT_rwlock_read_unset(&embedded_lock);
		return MAL_SUCCEED;
	}

	// otherwise we have to convert the column
	b = BATdescriptor(result->monetdb_resultset->cols[column_index].b);
	if (!b) {
		msg = createException(MAL, "embedded.monetdb_result_fetch", RUNTIME_OBJECT_MISSING);
		goto cleanup;
	}
	bat_type = b->ttype;
	sqltpe = &result->monetdb_resultset->cols[column_index].type;

	if (bat_type == TYPE_bit || bat_type == TYPE_bte) {
		GENERATE_BAT_INPUT(b, int8_t, bte);
	} else if (bat_type == TYPE_sht) {
		GENERATE_BAT_INPUT(b, int16_t, sht);
	} else if (bat_type == TYPE_int) {
		GENERATE_BAT_INPUT(b, int32_t, int);
	} else if (bat_type == TYPE_oid) {
		GENERATE_BAT_INPUT(b, size_t, oid);
	} else if (bat_type == TYPE_lng) {
		GENERATE_BAT_INPUT(b, int64_t, lng);
	} else if (bat_type == TYPE_flt) {
		GENERATE_BAT_INPUT(b, float, flt);
	} else if (bat_type == TYPE_dbl) {
		GENERATE_BAT_INPUT(b, double, dbl);
	} else if (bat_type == TYPE_str) {
		BATiter li;
		BUN p = 0, q = 0;
		GENERATE_BAT_INPUT_BASE(str);
		bat_data->count = BATcount(b);
		bat_data->data = GDKzalloc(sizeof(char *) * bat_data->count);
		bat_data->null_value = NULL;
		if (!bat_data->data) {
			msg = createException(MAL, "embedded.monetdb_result_fetch", MAL_MALLOC_FAIL);
			goto cleanup;
		}

		j = 0;
		li = bat_iterator(b);
		BATloop(b, p, q)
		{
			char *t = (char *)BUNtail(li, p);
			if (strcmp(t, str_nil) == 0) {
				bat_data->data[j] = NULL;
			} else {
				bat_data->data[j] = GDKstrdup(t);
				if (!bat_data->data[j]) {
					goto cleanup;
				}
			}
			j++;
		}
	} else if (bat_type == TYPE_date) {
		date *baseptr;
		GENERATE_BAT_INPUT_BASE(date);
		bat_data->count = BATcount(b);
		bat_data->data = GDKmalloc(sizeof(bat_data->null_value) * bat_data->count);
		if (!bat_data->data) {
			msg = createException(MAL, "embedded.monetdb_result_fetch", MAL_MALLOC_FAIL);
			goto cleanup;
		}

		baseptr = (date *)Tloc(b, 0);
		for (j = 0; j < bat_data->count; j++)
			data_from_date(baseptr[j], bat_data->data + j);
		data_from_date(date_nil, &bat_data->null_value);
	} else if (bat_type == TYPE_daytime) {
		daytime *baseptr;
		GENERATE_BAT_INPUT_BASE(time);
		bat_data->count = BATcount(b);
		bat_data->data = GDKmalloc(sizeof(bat_data->null_value) * bat_data->count);
		if (!bat_data->data) {
			msg = createException(MAL, "embedded.monetdb_result_fetch", MAL_MALLOC_FAIL);
			goto cleanup;
		}

		baseptr = (daytime *)Tloc(b, 0);
		for (j = 0; j < bat_data->count; j++)
			data_from_time(baseptr[j], bat_data->data + j);
		data_from_time(daytime_nil, &bat_data->null_value);
	} else if (bat_type == TYPE_timestamp) {
		timestamp *baseptr;
		GENERATE_BAT_INPUT_BASE(timestamp);
		bat_data->count = BATcount(b);
		bat_data->data = GDKmalloc(sizeof(bat_data->null_value) * bat_data->count);
		if (!bat_data->data) {
			msg = createException(MAL, "embedded.monetdb_result_fetch", MAL_MALLOC_FAIL);
			goto cleanup;
		}

		baseptr = (timestamp *)Tloc(b, 0);
		for (j = 0; j < bat_data->count; j++)
			data_from_timestamp(baseptr[j], bat_data->data + j);
		data_from_timestamp(timestamp_nil, &bat_data->null_value);
	} else if (bat_type == TYPE_blob) {
		BATiter li;
		BUN p = 0, q = 0;
		GENERATE_BAT_INPUT_BASE(blob);
		bat_data->count = BATcount(b);
		bat_data->data = GDKmalloc(sizeof(monetdb_data_blob) * bat_data->count);
		if (!bat_data->data) {
			msg = createException(MAL, "embedded.monetdb_result_fetch", MAL_MALLOC_FAIL);
			goto cleanup;
		}
		j = 0;

		li = bat_iterator(b);
		BATloop(b, p, q)
		{
			blob *t = (blob *)BUNtail(li, p);
			if (t->nitems == ~(size_t)0) {
				bat_data->data[j].size = 0;
				bat_data->data[j].data = NULL;
			} else {
				bat_data->data[j].size = t->nitems;
				bat_data->data[j].data = GDKmalloc(t->nitems);
				if (!bat_data->data[j].data) {
					msg = createException(MAL, "embedded.monetdb_result_fetch", MAL_MALLOC_FAIL);
					goto cleanup;
				}
				memcpy(bat_data->data[j].data, t->data, t->nitems);
			}
			j++;
		}
		bat_data->null_value.size = 0;
		bat_data->null_value.data = NULL;
	} else {
		// unsupported type: convert to string
		BATiter li;
		BUN p = 0, q = 0;
		GENERATE_BAT_INPUT_BASE(str);
		bat_data->count = BATcount(b);
		bat_data->null_value = NULL;
		bat_data->data = GDKzalloc(sizeof(char *) * bat_data->count);
		if (!bat_data->data) {
			msg = createException(MAL, "embedded.monetdb_result_fetch", MAL_MALLOC_FAIL);
			goto cleanup;
		}
		j = 0;

		li = bat_iterator(b);
		BATloop(b, p, q)
		{
			void *t = BUNtail(li, p);
			if (BATatoms[bat_type].atomCmp(t, BATatoms[bat_type].atomNull) == 0) {
				bat_data->data[j] = NULL;
			} else {
				char *sresult = NULL;
				size_t length = 0;
				if (BATatoms[bat_type].atomToStr(&sresult, &length, t, true) == 0) {
					msg = createException(MAL, "embedded.monetdb_result_fetch", "Failed to convert element to string");
					goto cleanup;
				}
				bat_data->data[j] = sresult;
			}
			j++;
		}
	}
cleanup:
	if (b)
		BBPunfix(b->batCacheid);
	if (msg) {
		*res = NULL;
		monetdb_destroy_column(column_result);
	} else {
		result->converted_columns[column_index] = column_result;
		*res = result->converted_columns[column_index];
	}
	msg = commit_action(m, msg);
	MT_rwlock_read_unset(&embedded_lock);
	return msg;
}

char*
monetdb_result_fetch_rawcol(monetdb_connection conn, res_col** res, monetdb_result* mres, size_t column_index)
{
	char* msg = MAL_SUCCEED;
	monetdb_result_internal* result = (monetdb_result_internal*) mres;
	mvc* m;
	Client c = (Client) conn;

	MT_rwlock_read_set(&embedded_lock);
	if ((msg = validate_connection(conn, "embedded.monetdb_result_fetch_rawcol")) != MAL_SUCCEED) {
		MT_rwlock_read_unset(&embedded_lock);
		return msg;
	}

	if ((msg = getSQLContext(c, NULL, &m, NULL)) != MAL_SUCCEED)
		goto cleanup;
	if ((msg = SQLtrans(m)) != MAL_SUCCEED)
		goto cleanup;
	if (column_index >= mres->ncols) { // index out of range
		msg = createException(MAL, "embedded.monetdb_result_fetch_rawcol", "Index out of range");
		goto cleanup;
	}
cleanup:
	if (msg)
		*res = NULL;
	else
		*res = &(result->monetdb_resultset->cols[column_index]);
	msg = commit_action(m, msg);
	MT_rwlock_read_unset(&embedded_lock);
	return msg;
}

void
data_from_date(date d, monetdb_data_date *ptr)
{
	ptr->day = date_day(d);
	ptr->month = date_month(d);
	ptr->year = date_year(d);
}

void
data_from_time(daytime d, monetdb_data_time *ptr)
{
	ptr->hours = daytime_hour(d);
	ptr->minutes = daytime_min(d);
	ptr->seconds = daytime_sec(d);
	ptr->ms = daytime_usec(d) / 1000;
}

void
data_from_timestamp(timestamp d, monetdb_data_timestamp *ptr)
{
	daytime tm = timestamp_daytime(d);
	date dt = timestamp_date(d);

	ptr->date.day = date_day(dt);
	ptr->date.month = date_month(dt);
	ptr->date.year = date_year(dt);
	ptr->time.hours = daytime_hour(tm);
	ptr->time.minutes = daytime_min(tm);
	ptr->time.seconds = daytime_sec(tm);
	ptr->time.ms = daytime_usec(tm) / 1000;
}

static timestamp
timestamp_from_data(monetdb_data_timestamp *ptr)
{
	return timestamp_create(date_create(ptr->date.year,
										ptr->date.month,
										ptr->date.day),
							daytime_create(ptr->time.hours,
										   ptr->time.minutes,
										   ptr->time.seconds,
										   ptr->time.ms * 1000));
}

int
date_is_null(monetdb_data_date value)
{
	monetdb_data_date null_value;
	data_from_date(date_nil, &null_value);
	return value.year == null_value.year && value.month == null_value.month &&
		   value.day == null_value.day;
}

int
time_is_null(monetdb_data_time value)
{
	monetdb_data_time null_value;
	data_from_time(daytime_nil, &null_value);
	return value.hours == null_value.hours &&
		   value.minutes == null_value.minutes &&
		   value.seconds == null_value.seconds && value.ms == null_value.ms;
}

int
timestamp_is_null(monetdb_data_timestamp value)
{
	return is_timestamp_nil(timestamp_from_data(&value));
}

int
str_is_null(char *value)
{
	return value == NULL;
}

int
blob_is_null(monetdb_data_blob value)
{
	return value.data == NULL;
}
