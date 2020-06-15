/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "monetdbe.h"
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

#define UNUSED(x) (void)(x)

static int 
monetdb_type(monetdb_types t) {
	switch(t) {
	case monetdb_bool: return TYPE_bit;
	case monetdb_int8_t: return TYPE_bte;
	case monetdb_int16_t: return TYPE_sht;
	case monetdb_int32_t: return TYPE_int;
	case monetdb_int64_t: return TYPE_lng;
#ifdef HAVE_HGE
	case monetdb_int128_t: return TYPE_hge;
#endif 
	case monetdb_size_t: return TYPE_oid;
	case monetdb_float: return TYPE_flt;
	case monetdb_double: return TYPE_dbl;
	case monetdb_str: return TYPE_str;
	case monetdb_blob: return TYPE_blob;
	case monetdb_date: return TYPE_date;
	case monetdb_time: return TYPE_daytime;
	case monetdb_timestamp: return TYPE_timestamp;
	default:
		return -1;
	}	
}

static int 
embedded_type(int t) {
	switch(t) {
	case TYPE_bit: return monetdb_bool;
	case TYPE_bte: return monetdb_int8_t; 
	case TYPE_sht: return monetdb_int16_t;
	case TYPE_int: return monetdb_int32_t;
	case TYPE_lng: return monetdb_int64_t;
#ifdef HAVE_HGE
	case TYPE_hge: return monetdb_int128_t;
#endif 
	case TYPE_oid: return monetdb_size_t;
	case TYPE_flt: return monetdb_float;
	case TYPE_dbl: return monetdb_double;
	case TYPE_str: return monetdb_str;
	case TYPE_date: return monetdb_date;
	case TYPE_daytime: return monetdb_time;
	case TYPE_timestamp: return monetdb_timestamp;
	default:
	  	if (t==TYPE_blob)
			return monetdb_blob;
		return -1;
	}	
}

typedef struct monetdb_table_t {
	sql_table t;
} monetdb_table_t;

typedef struct {
	monetdb_result res;
	int type;
	res_table *monetdb_resultset;
	monetdb_column **converted_columns;
	monetdb_database dbhdl;
} monetdb_result_internal;

typedef struct {
	monetdb_statement res;
	ValRecord *data;
	ValPtr *args;	/* only used during calls */
	int retc;
	monetdb_database dbhdl;
	cq *q;
} monetdb_stmt_internal;

typedef struct {
	Client c;
} monetdb_database_internal;

static MT_Lock embedded_lock = MT_LOCK_INITIALIZER("embedded_lock");
static bool monetdb_embedded_initialized = false;
static char *monetdb_embedded_url = NULL;
static int open_dbs = 0;

static char* monetdb_cleanup_result_internal(monetdb_database dbhdl, monetdb_result* result);

static char*
commit_action(mvc* m, char* msg, monetdb_database dbhdl, monetdb_result **result, monetdb_result_internal *res_internal)
{
	/* handle autocommit */
    	char *commit_msg = SQLautocommit(m);
	if ((msg != MAL_SUCCEED || commit_msg != MAL_SUCCEED)) {
		if (res_internal) {
			char* other = monetdb_cleanup_result_internal(dbhdl, (monetdb_result*) res_internal);
			if (other)
				freeException(other);
		}
		if (result)
			*result = NULL;
		if (msg == MAL_SUCCEED)
			msg = commit_msg;
		else if (commit_msg)
			freeException(commit_msg);
	}
	return msg;
}

static int
validate_database_handle_noerror(monetdb_database dbhdl)
{
	if (!monetdb_embedded_initialized || !MCvalid((Client)dbhdl))
		return 0;
	return 1;
}

static char*
validate_database_handle(monetdb_database dbhdl, const char* call) // Call this function always inside the embedded_lock
{
	if (!monetdb_embedded_initialized)
		return createException(MAL, call, "MonetDBe has not yet started");
	if (!MCvalid((Client) dbhdl))
		return createException(MAL, call, "Invalid database handle");
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
monetdb_cleanup_result_internal(monetdb_database dbhdl, monetdb_result* result)
{
	char* msg = MAL_SUCCEED;
	monetdb_result_internal* res = (monetdb_result_internal *) result;
	Client c = (Client) dbhdl;

	mvc* m = NULL;

	assert(!res->dbhdl || res->dbhdl == dbhdl);
	if ((msg = validate_database_handle(dbhdl, "embedded.monetdb_cleanup_result_internal")) != MAL_SUCCEED)
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
	return commit_action(m, msg, NULL, NULL, NULL);
}

static char*
monetdb_query_internal(monetdb_database dbhdl, char* query, monetdb_result** result, monetdb_cnt* affected_rows, int *prepare_id, char language)
{
	char* msg = MAL_SUCCEED, *nq = NULL;
	Client c = (Client) dbhdl;
	mvc* m = NULL;
	backend *b;
	size_t query_len, input_query_len, prep_len = 0;
	buffer query_buf;
	stream *query_stream;
	monetdb_result_internal *res_internal = NULL;
	bstream *old_bstream = NULL;
	stream *fdout = c->fdout;

	if ((msg = validate_database_handle(dbhdl, "embedded.monetdb_query_internal")) != MAL_SUCCEED)
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
	if (prepare_id) {
		prep_len = sizeof("PREPARE ")-1;
		query_len += prep_len;
	}
	if (!(nq = GDKmalloc(query_len))) {
		msg = createException(MAL, "embedded.monetdb_query_internal", "Could not setup query stream");
		goto cleanup;
	}
	if (prepare_id) 
		strcpy(nq, "PREPARE ");
	strcpy(nq + prep_len, query);
	strcpy(nq + prep_len + input_query_len, "\n;");

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
	if (prepare_id)
		m->emode = m_prepare;
	if ((msg = SQLparser(c)) != MAL_SUCCEED)
		goto cleanup;
	if (m->emode == m_prepare)
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
			res_internal->type = (m->results) ? Q_TABLE : Q_UPDATE;
		else if (m->emode & m_prepare)
			res_internal->type = Q_PREPARE;
		else
			res_internal->type = (m->results) ? m->results->query_type : m->type;
		res_internal->res.last_id = m->last_id;
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
			res_internal->dbhdl = dbhdl;
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
	return commit_action(m, msg, dbhdl, result, res_internal);
}

static char*
monetdb_close_internal(monetdb_database dbhdl)
{
	char* msg = MAL_SUCCEED;

	if ((msg = validate_database_handle(dbhdl, "embedded.monetdb_close_internal")) != MAL_SUCCEED)
		return msg;
	if ((msg = SQLexitClient((Client) dbhdl)) != MAL_SUCCEED)
		return msg;
	MCcloseClient((Client) dbhdl);
	return msg;
}

static char*
monetdb_open_internal(monetdb_database *dbhdl)
{
	mvc *m;
	char* msg = MAL_SUCCEED;
	Client mc = NULL;

	if (!monetdb_embedded_initialized) {
		msg = createException(MAL, "embedded.monetdb_open_internal", "Embedded MonetDB is not started");
		goto cleanup;
	}
	mc = MCinitClient((oid) 0, 0, 0);
	if (!MCvalid(mc)) {
		msg = createException(MAL, "embedded.monetdb_open_internal", "Failed to initialize client");
		goto cleanup;
	}
	mc->curmodule = mc->usermodule = userModule();
	if (mc->usermodule == NULL) {
		msg = createException(MAL, "embedded.monetdb_open_internal", "Failed to initialize client MAL module");
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
		msg = createException(SQL, "embedded.monetdb_open_internal", MAL_MALLOC_FAIL);
		goto cleanup;
	}

cleanup:
	if (msg && mc) {
		char* other = monetdb_close_internal(mc);
		if (other)
			freeException(other);
		*dbhdl = NULL;
	} else if (dbhdl)
		*dbhdl = mc;
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

static char*
monetdb_startup(char* dbdir)
{
	char* msg = MAL_SUCCEED, *err;
	monetdb_result* res = NULL;
	void* c;
	opt *set = NULL;
	int setlen;
	gdk_return gdk_res;

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
	// define MT_fprintf_silent 
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
	/* get sequential and other flags from url
	if (sequential)
		setlen = mo_add_option(&set, setlen, opt_cmdline, "sql_optimizer", "sequential_pipe");
	else
	*/
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

	monetdb_embedded_initialized = true;
	monetdb_embedded_url = dbdir;

	if ((msg = monetdb_open_internal(&c)) != MAL_SUCCEED)
		goto cleanup;
	GDKfataljumpenable = 0;
	// we do not want to jump after this point, since we cannot do so between threads sanity check, run a SQL query
	if ((msg = monetdb_query_internal(c, "SELECT id FROM _tables LIMIT 1;", &res, NULL, NULL, 'S')) != MAL_SUCCEED)
		goto cleanup;
	if ((msg = monetdb_cleanup_result_internal(c, res)) != MAL_SUCCEED)
		goto cleanup;
	msg = monetdb_close_internal(c);

cleanup:
	if (msg)
		monetdb_shutdown_internal();
done:
	return msg;
}

char*
monetdb_open(monetdb_database *dbhdl, char *url)
{
	char* msg = MAL_SUCCEED;
	if (!dbhdl)
		return createException(MAL, "embedded.monetdb_open", "monetdb_open parameter is NULL");
	MT_lock_set(&embedded_lock);
	if (!monetdb_embedded_initialized) {
		/* later handle url !*/
		msg = monetdb_startup(url);
	} else { /* check uri */
		if ((monetdb_embedded_url && url && strcmp(monetdb_embedded_url, url) != 0) || (monetdb_embedded_url != url && (monetdb_embedded_url == NULL || url == NULL)))
			msg = createException(MAL, "embedded.monetdb_open", "monetdb_open currently only one active database is supported");
	}
	if (!msg)
		msg = monetdb_open_internal(dbhdl);
	open_dbs++;
	MT_lock_unset(&embedded_lock);
	return msg;
}

char*
monetdb_close(monetdb_database dbhdl)
{
	MT_lock_set(&embedded_lock);
	open_dbs--;
	char *msg = monetdb_close_internal(dbhdl);
	if (!open_dbs)
		monetdb_shutdown_internal();
	MT_lock_unset(&embedded_lock);
	return msg;
}

char*
monetdb_get_autocommit(monetdb_database dbhdl, int* result)
{
	char *msg = MAL_SUCCEED;

	MT_lock_set(&embedded_lock);
	if ((msg = validate_database_handle(dbhdl, "embedded.monetdb_get_autocommit")) != MAL_SUCCEED) {
		MT_lock_unset(&embedded_lock);
		return msg;
	}

	if (!result) {
		msg = createException(MAL, "embedded.monetdb_get_autocommit", "Parameter result is NULL");
	} else {
		Client db = (Client) dbhdl;
		mvc *m = ((backend *) db->sqlcontext)->mvc;
		*result = m->session->auto_commit;
	}
	MT_lock_unset(&embedded_lock);
	return msg;
}

char*
monetdb_set_autocommit(monetdb_database dbhdl, int value)
{
	char *msg = MAL_SUCCEED;

	MT_lock_set(&embedded_lock);
	if (!validate_database_handle_noerror(dbhdl)) {
		MT_lock_unset(&embedded_lock);
		return 0;
	}

	Client db = (Client) dbhdl;
	mvc *m = ((backend *) db->sqlcontext)->mvc;
	int commit = !m->session->auto_commit && value;

	m->session->auto_commit = value;
	m->session->ac_on_commit = m->session->auto_commit;
	if (m->session->tr->active) {
		if (commit) {
			msg = mvc_commit(m, 0, NULL, true);
		} else {
			msg = mvc_rollback(m, 0, NULL, true);
		}
	}
	MT_lock_unset(&embedded_lock);
	return msg;
}

int
monetdb_in_transaction(monetdb_database dbhdl)
{
	MT_lock_set(&embedded_lock);

	if (!validate_database_handle_noerror(dbhdl)) {
		MT_lock_unset(&embedded_lock);
		return 0;
	}

	Client db = (Client) dbhdl;
	mvc *m = ((backend *) db->sqlcontext)->mvc;
	int result = 0;

	if (m->session->tr)
		result = m->session->tr->active;
	MT_lock_unset(&embedded_lock);
	return result;
}

char*
monetdb_query(monetdb_database dbhdl, char* query, monetdb_result** result, monetdb_cnt* affected_rows)
{
	char* msg;
	MT_lock_set(&embedded_lock);
	msg = monetdb_query_internal(dbhdl, query, result, affected_rows, NULL, 'S');
	MT_lock_unset(&embedded_lock);
	return msg;
}

char*
monetdb_prepare(monetdb_database dbhdl, char* query, monetdb_statement **stmt)
{
	char* msg;
	int prepare_id = 0;

	MT_lock_set(&embedded_lock);
	if (!stmt)
		msg = createException(MAL, "embedded.monetdb_prepare", "Parameter stmt is NULL");
	else {
		msg = monetdb_query_internal(dbhdl, query, NULL, NULL, &prepare_id, 'S');
	}
	if (msg == MAL_SUCCEED) {
		Client db = (Client) dbhdl;
		mvc *m = ((backend *) db->sqlcontext)->mvc;
		monetdb_stmt_internal *stmt_internal = (monetdb_stmt_internal*)GDKmalloc(sizeof(monetdb_stmt_internal));
		cq *q = qc_find(m->qc, prepare_id);
		
		if (q && stmt_internal) {
			Symbol s = (Symbol)q->code;
			InstrPtr p = s->def->stmt[0];
			stmt_internal->dbhdl = db;
			stmt_internal->q = q;
			stmt_internal->retc = p->retc;
			stmt_internal->res.nparam = q->paramlen;
			stmt_internal->data = (ValRecord*)GDKzalloc(sizeof(ValRecord) * q->paramlen);
			stmt_internal->args = (ValPtr*)GDKmalloc(sizeof(ValPtr) * (q->paramlen + stmt_internal->retc));
			stmt_internal->res.type = (monetdb_types*)GDKmalloc(sizeof(monetdb_types)*q->paramlen);
			if (!stmt_internal->res.type || !stmt_internal->data || !stmt_internal->args) {
				if (stmt_internal->data)
					GDKfree(stmt_internal->data);
				msg = createException(MAL, "embedded.monetdb_prepare", "Could not setup prepared statement");
			} else {
				for (int i = 0; i<q->paramlen; i++) {
					stmt_internal->res.type[i] = embedded_type(q->params[i].type->localtype);
					stmt_internal->args[i+stmt_internal->retc] = &stmt_internal->data[i];
				}
			}
		}
		if (msg == MAL_SUCCEED)
			*stmt = (monetdb_statement*)stmt_internal;
		else if (stmt_internal)
			GDKfree(stmt_internal);
	}
	MT_lock_unset(&embedded_lock);
	return msg;
}

char*
monetdb_bind(monetdb_statement *stmt, void *data, size_t i)
{
	monetdb_stmt_internal *stmt_internal = (monetdb_stmt_internal*)stmt;

	/* TODO !data treat as NULL value (add nil mask) ? */
	if (i > stmt->nparam)
		return createException(MAL, "embedded.monetdb_bind", "Parameter %zu not bound to a value", i);
	stmt_internal->data[i].vtype = stmt_internal->q->params[i].type->localtype;
	/* TODO handle conversion from NULL and special types */
	VALset(&stmt_internal->data[i], stmt_internal->q->params[i].type->localtype, data);
	return MAL_SUCCEED;
}

char*
monetdb_execute(monetdb_statement *stmt, monetdb_result **result, monetdb_cnt *affected_rows)
{
	monetdb_result_internal *res_internal = NULL;
	monetdb_stmt_internal *stmt_internal = (monetdb_stmt_internal*)stmt;
	monetdb_database dbhdl = stmt_internal->dbhdl;
	Client c = (Client)dbhdl;
	mvc *m = ((backend *) c->sqlcontext)->mvc;
	cq *q = stmt_internal->q;
	str msg = MAL_SUCCEED;

        if ((msg = SQLtrans(m)) != MAL_SUCCEED)
		return msg;

	/* check if all inputs are bound */
	for(int i = 0; i<q->paramlen; i++){
		if (!stmt_internal->data[i].vtype)
			return createException(MAL, "embedded.monetdb_execute", "Parameter %d not bound to a value", i);
	}
	MalStkPtr glb = (MalStkPtr) (q->stk);
	Symbol s = (Symbol)q->code;
	msg = callMAL(c, s->def, &glb, stmt_internal->args, 0);

	if (!m->results && m->rowcnt >= 0 && affected_rows)
		*affected_rows = m->rowcnt;

	if (result) {
		if (!(res_internal = GDKzalloc(sizeof(monetdb_result_internal)))) {
			msg = createException(MAL, "embedded.monetdb_query_internal", MAL_MALLOC_FAIL);
			goto cleanup;
		}
		res_internal->type = (m->results) ? Q_TABLE : Q_UPDATE;
		res_internal->res.last_id = m->last_id;
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
			res_internal->dbhdl = dbhdl;
			if (!res_internal->converted_columns) {
				msg = createException(MAL, "embedded.monetdb_query_internal", MAL_MALLOC_FAIL);
				goto cleanup;
			}
			m->results = NULL;
		}
	}
cleanup:
	return commit_action(m, msg, dbhdl, result, res_internal);
}

char*
monetdb_cleanup_statement(monetdb_database dbhdl, monetdb_statement *stmt)
{
	(void)dbhdl;
	monetdb_stmt_internal *stmt_internal = (monetdb_stmt_internal*)stmt;
	Client c = (Client)dbhdl;
	mvc *m = ((backend *) c->sqlcontext)->mvc;
	cq *q = stmt_internal->q;

	assert(!stmt_internal->dbhdl || dbhdl == stmt_internal->dbhdl);
	GDKfree(stmt_internal->data);
	GDKfree(stmt_internal->args);
	GDKfree(stmt_internal->res.type);
	GDKfree(stmt_internal);

	if (q)
		qc_delete(m->qc, q);
	return MAL_SUCCEED;
}

char*
monetdb_append(monetdb_database dbhdl, const char* schema, const char* table, monetdb_column **input /*bat *batids*/, size_t column_count)
{
	Client c = (Client) dbhdl;
	mvc *m;
	char* msg = MAL_SUCCEED;

	MT_lock_set(&embedded_lock);
	if ((msg = validate_database_handle(dbhdl, "embedded.monetdb_append")) != MAL_SUCCEED) {
		MT_lock_unset(&embedded_lock);
		return msg; //The dbhdl is invalid, there is no transaction going
	}

	if ((msg = getSQLContext(c, NULL, &m, NULL)) != MAL_SUCCEED)
		goto cleanup;
        if ((msg = SQLtrans(m)) != MAL_SUCCEED)
		goto cleanup;

	if (schema == NULL) {
		msg = createException(MAL, "embedded.monetdb_append", "schema parameter is NULL");
		goto cleanup;
	}
	if (table == NULL) {
		msg = createException(MAL, "embedded.monetdb_append", "table parameter is NULL");
		goto cleanup;
	}
	if (input == NULL) {
		msg = createException(MAL, "embedded.monetdb_append", "input parameter is NULL");
		goto cleanup;
	}
	if (column_count < 1) {
		msg = createException(MAL, "embedded.monetdb_append", "column_count must be higher than 0");
		goto cleanup;
	}

	sql_schema *s;
	sql_table *t;

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
	
	/* for now no default values, ie user should supply all columns */

	if (column_count != (size_t)list_length(t->columns.set)) {
		msg = createException(SQL, "embedded.monetdb_append", "Incorrect number of columns");
		goto cleanup;
	}

	/* small number of rows */
	if (input[0]->count <= 16) {
		size_t i, cnt = input[0]->count;
		node *n;

		for (i = 0, n = t->columns.set->h; i < column_count && n; i++, n = n->next) {
			sql_column *c = n->data;
			int mtype = monetdb_type(input[i]->type);
			char *v = input[i]->data;
			int w = 1;

			if (mtype < 0) {
				msg = createException(SQL, "embedded.monetdb_append", "Cannot find type for column %zu", i);
				goto cleanup;
			}
			if (mtype >= TYPE_bit && mtype <= TYPE_dbl) {
				w = BATatoms[mtype].size;
				for (size_t j=0; j<cnt; j++, v+=w){
					if (store_funcs.append_col(m->session->tr, c, v, mtype) != 0) {
						msg = createException(SQL, "embedded.monetdb_append", "Cannot append values");
						goto cleanup;
					}
				}
			} else if (mtype == TYPE_str) {
				char **d = (char**)v;

				for (size_t j=0; j<cnt; j++){
					char *s = d[j];
					if (!s)
						s = (char*)str_nil;
					if (store_funcs.append_col(m->session->tr, c, s, mtype) != 0) {
						msg = createException(SQL, "embedded.monetdb_append", "Cannot append values");
						goto cleanup;
					}
				}
			}
			/* TODO blob, temperal */
		}
	} else { 
		msg = createException(SQL, "embedded.monetdb_append", "TODO bulk insert");
		goto cleanup;
	}
cleanup:
	msg = commit_action(m, msg, NULL, NULL, NULL);
	MT_lock_unset(&embedded_lock);
	return msg;
}

char*
monetdb_cleanup_result(monetdb_database dbhdl, monetdb_result* result)
{
	char* msg = MAL_SUCCEED;
	MT_lock_set(&embedded_lock);
	msg = monetdb_cleanup_result_internal(dbhdl, result);
	MT_lock_unset(&embedded_lock);
	return msg;
}

char*
monetdb_get_table(monetdb_database dbhdl, monetdb_table** table, const char* schema_name, const char* table_name)
{
	mvc *m;
	sql_schema *s;
	char *msg = MAL_SUCCEED;
	Client db = (Client) dbhdl;

	MT_lock_set(&embedded_lock);
	if ((msg = validate_database_handle(dbhdl, "embedded.monetdb_get_table")) != MAL_SUCCEED) {
		MT_lock_unset(&embedded_lock);
		return msg;
	}

	if ((msg = getSQLContext(db, NULL, &m, NULL)) != NULL)
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
	if (!(*(sql_table**)table = mvc_bind_table(m, s, table_name))) {
		msg = createException(MAL, "embedded.monetdb_get_table", "Could not find table %s", table_name);
		goto cleanup;
	}

cleanup:
	msg = commit_action(m, msg, NULL, NULL, NULL);
	MT_lock_unset(&embedded_lock);
	return msg;
}

char*
monetdb_get_columns(monetdb_database dbhdl, const char* schema_name, const char *table_name, size_t *column_count,
					char ***column_names, int **column_types)
{
	mvc *m;
	sql_schema *s;
	sql_table *t;
	char* msg = MAL_SUCCEED;
	int columns;
	node *n;
	Client c = (Client) dbhdl;

	MT_lock_set(&embedded_lock);
	if ((msg = validate_database_handle(dbhdl, "embedded.monetdb_get_columns")) != MAL_SUCCEED) {
		MT_lock_unset(&embedded_lock);
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
	msg = commit_action(m, msg, NULL, NULL, NULL);
	MT_lock_unset(&embedded_lock);
	return msg;
}

#define GENERATE_BASE_HEADERS(type, tpename) \
	static int tpename##_is_null(type value)

#define GENERATE_BASE_FUNCTIONS(tpe, tpename, mname) \
	GENERATE_BASE_HEADERS(tpe, tpename); \
	static int tpename##_is_null(tpe value) { return value == mname##_nil; }

#ifdef bool
#undef bool
#endif

GENERATE_BASE_FUNCTIONS(int8_t, bool, bit)
GENERATE_BASE_FUNCTIONS(int8_t, int8_t, bte)
GENERATE_BASE_FUNCTIONS(int16_t, int16_t, sht)
GENERATE_BASE_FUNCTIONS(int32_t, int32_t, int)
GENERATE_BASE_FUNCTIONS(int64_t, int64_t, lng)
#ifdef HAVE_HGE
GENERATE_BASE_FUNCTIONS(__int128, int128_t, hge)
#endif
GENERATE_BASE_FUNCTIONS(size_t, size_t, oid)

GENERATE_BASE_FUNCTIONS(float, float, flt)
GENERATE_BASE_FUNCTIONS(double, double, dbl)

GENERATE_BASE_HEADERS(char*, str);
GENERATE_BASE_HEADERS(monetdb_data_blob, blob);

GENERATE_BASE_HEADERS(monetdb_data_date, date);
GENERATE_BASE_HEADERS(monetdb_data_time, time);
GENERATE_BASE_HEADERS(monetdb_data_timestamp, timestamp);

#define GENERATE_BAT_INPUT_BASE(tpe)                                               \
	monetdb_column_##tpe *bat_data = GDKzalloc(sizeof(monetdb_column_##tpe));  \
	if (!bat_data) {                                                           \
		msg = createException(MAL, "embedded.monetdb_result_fetch", MAL_MALLOC_FAIL); \
		goto cleanup;                                                      \
	}                                                                          \
	bat_data->type = monetdb_##tpe;                                            \
	bat_data->is_null = tpe##_is_null;                                         \
	bat_data->scale = pow(10, sqltpe->scale);                                  \
	column_result = (monetdb_column*) bat_data;

#define GENERATE_BAT_INPUT(b, tpe, tpe_name, mtype)                                \
	{                                                                          \
		GENERATE_BAT_INPUT_BASE(tpe_name);                            	   \
		bat_data->count = BATcount(b);                                     \
		bat_data->null_value = mtype##_nil;                                \
		if (bat_data->count) {                                             \
			bat_data->data = GDKzalloc(bat_data->count * sizeof(bat_data->null_value)); \
			if (!bat_data->data) {                                     \
				msg = createException(MAL, "embedded.monetdb_result_fetch", MAL_MALLOC_FAIL); \
				goto cleanup;                                      \
			}                                                          \
		}                                                                  \
		size_t it = 0;                                                     \
		mtype* val = (mtype*)Tloc(b, 0);                                   \
		/* bat is dense, materialize it */                                 \
		for (it = 0; it < bat_data->count; it++, val++)                    \
			bat_data->data[it] = (tpe) *val;                           \
	}

static void data_from_date(date d, monetdb_data_date *ptr);
static void data_from_time(daytime d, monetdb_data_time *ptr);
static void data_from_timestamp(timestamp d, monetdb_data_timestamp *ptr);

char*
monetdb_result_fetch(monetdb_result* mres, monetdb_column** res, size_t column_index)
{
	BAT* b = NULL;
	int bat_type;
	mvc* m;
	char* msg = MAL_SUCCEED;
	monetdb_result_internal* result = (monetdb_result_internal*) mres;
	sql_subtype* sqltpe = NULL;
	monetdb_column* column_result = NULL;
	size_t j = 0;
	monetdb_database dbhdl = result->dbhdl;
	Client c = (Client) dbhdl;

	MT_lock_set(&embedded_lock);
	if ((msg = validate_database_handle(dbhdl, "embedded.monetdb_result_fetch")) != MAL_SUCCEED) {
		MT_lock_unset(&embedded_lock);
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
		MT_lock_unset(&embedded_lock);
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

	if (bat_type == TYPE_bit) {
		GENERATE_BAT_INPUT(b, int8_t, bool, bit);
	} else if (bat_type == TYPE_bte) {
		GENERATE_BAT_INPUT(b, int8_t, int8_t, bte);
	} else if (bat_type == TYPE_sht) {
		GENERATE_BAT_INPUT(b, int16_t, int16_t, sht);
	} else if (bat_type == TYPE_int) {
		GENERATE_BAT_INPUT(b, int32_t, int32_t, int);
	} else if (bat_type == TYPE_oid) {
		GENERATE_BAT_INPUT(b, size_t, size_t, oid);
	} else if (bat_type == TYPE_lng) {
		GENERATE_BAT_INPUT(b, int64_t, int64_t, lng);
#ifdef HAVE_HGE
	} else if (bat_type == TYPE_hge) {
		GENERATE_BAT_INPUT(b, __int128, int128_t, hge);
#endif
	} else if (bat_type == TYPE_flt) {
		GENERATE_BAT_INPUT(b, float, float, flt);
	} else if (bat_type == TYPE_dbl) {
		GENERATE_BAT_INPUT(b, double, double, dbl);
	} else if (bat_type == TYPE_str) {
		BATiter li;
		BUN p = 0, q = 0;
		GENERATE_BAT_INPUT_BASE(str);
		bat_data->count = BATcount(b);
		if (bat_data->count) {
			bat_data->data = GDKzalloc(sizeof(char *) * bat_data->count);
			bat_data->null_value = NULL;
			if (!bat_data->data) {
				msg = createException(MAL, "embedded.monetdb_result_fetch", MAL_MALLOC_FAIL);
				goto cleanup;
			}
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
		if (bat_data->count) {
			bat_data->data = GDKmalloc(sizeof(bat_data->null_value) * bat_data->count);
			if (!bat_data->data) {
				msg = createException(MAL, "embedded.monetdb_result_fetch", MAL_MALLOC_FAIL);
				goto cleanup;
			}
		}

		baseptr = (date *)Tloc(b, 0);
		for (j = 0; j < bat_data->count; j++)
			data_from_date(baseptr[j], bat_data->data + j);
		data_from_date(date_nil, &bat_data->null_value);
	} else if (bat_type == TYPE_daytime) {
		daytime *baseptr;
		GENERATE_BAT_INPUT_BASE(time);
		bat_data->count = BATcount(b);
		if (bat_data->count) {
			bat_data->data = GDKmalloc(sizeof(bat_data->null_value) * bat_data->count);
			if (!bat_data->data) {
				msg = createException(MAL, "embedded.monetdb_result_fetch", MAL_MALLOC_FAIL);
				goto cleanup;
			}
		}

		baseptr = (daytime *)Tloc(b, 0);
		for (j = 0; j < bat_data->count; j++)
			data_from_time(baseptr[j], bat_data->data + j);
		data_from_time(daytime_nil, &bat_data->null_value);
	} else if (bat_type == TYPE_timestamp) {
		timestamp *baseptr;
		GENERATE_BAT_INPUT_BASE(timestamp);
		bat_data->count = BATcount(b);
		if (bat_data->count) {
			bat_data->data = GDKmalloc(sizeof(bat_data->null_value) * bat_data->count);
			if (!bat_data->data) {
				msg = createException(MAL, "embedded.monetdb_result_fetch", MAL_MALLOC_FAIL);
				goto cleanup;
			}
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
		if (bat_data->count) {
			bat_data->data = GDKmalloc(sizeof(monetdb_data_blob) * bat_data->count);
			if (!bat_data->data) {
				msg = createException(MAL, "embedded.monetdb_result_fetch", MAL_MALLOC_FAIL);
				goto cleanup;
			}
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
		if (bat_data->count) {
			bat_data->null_value = NULL;
			bat_data->data = GDKzalloc(sizeof(char *) * bat_data->count);
			if (!bat_data->data) {
				msg = createException(MAL, "embedded.monetdb_result_fetch", MAL_MALLOC_FAIL);
				goto cleanup;
			}
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
	if (column_result)
		column_result->name = result->monetdb_resultset->cols[column_index].name;
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
	msg = commit_action(m, msg, NULL, NULL, NULL);
	MT_lock_unset(&embedded_lock);
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
