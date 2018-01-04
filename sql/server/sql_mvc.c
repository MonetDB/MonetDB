/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/* multi version catalog */

#include "monetdb_config.h"
#include "gdk.h"

#include "sql_mvc.h"
#include "sql_qc.h"
#include "sql_types.h"
#include "sql_env.h"
#include "sql_semantic.h"
#include "sql_privileges.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "gdk_logger.h"

static int mvc_debug = 0;

int
mvc_init(int debug, store_type store, int ro, int su, backend_stack stk)
{
	int first = 0;

	logger_settings log_settings;
	/* Set the default WAL directory. "sql_logs" by default */
	log_settings.logdir = "sql_logs";
	/* Get and pass on the WAL directory location, if set */
	if (GDKgetenv("gdk_logdir") != NULL) {
		log_settings.logdir = GDKgetenv("gdk_logdir");
	}
	/* Get and pass on the shared WAL directory location, if set */
	log_settings.shared_logdir = GDKgetenv("gdk_shared_logdir");
	/* Get and pass on the shared WAL drift threshold, if set.
	 * -1 by default, meaning it should be ignored, since it is not set */
	log_settings.shared_drift_threshold = GDKgetenv_int("gdk_shared_drift_threshold", -1);

	/* Get and pass on the flag how many WAL files should be preserved.
	 * 0 by default - keeps only the current WAL file. */
	log_settings.keep_persisted_log_files = GDKgetenv_int("gdk_keep_persisted_log_files", 0);

	mvc_debug = debug&4;
	if (mvc_debug) {
		fprintf(stderr, "#mvc_init logdir %s\n", log_settings.logdir);
		fprintf(stderr, "#mvc_init keep_persisted_log_files %d\n", log_settings.keep_persisted_log_files);
		if (log_settings.shared_logdir != NULL) {
			fprintf(stderr, "#mvc_init shared_logdir %s\n", log_settings.shared_logdir);
		}
		fprintf(stderr, "#mvc_init shared_drift_threshold %d\n", log_settings.shared_drift_threshold);
	}
	keyword_init();
	if(scanner_init_keywords() != 0) {
		fprintf(stderr, "!mvc_init: malloc failure\n");
		return -1;
	}

	if ((first = store_init(debug, store, ro, su, &log_settings, stk)) < 0) {
		fprintf(stderr, "!mvc_init: unable to create system tables\n");
		return -1;
	}
	if (first || catalog_version) {
		sql_schema *s;
		sql_table *t;
		sqlid tid = 0, ntid, cid = 0, ncid;
		mvc *m = mvc_create(0, stk, 0, NULL, NULL);
		if (!m) {
			fprintf(stderr, "!mvc_init: malloc failure\n");
			return -1;
		}

		m->sa = sa_create();
		if (!m->sa) {
			mvc_destroy(m);
			fprintf(stderr, "!mvc_init: malloc failure\n");
			return -1;
		}

		/* disable caching */
		m->caching = 0;
		/* disable history */
		m->history = 0;
		/* disable size header */
		m->sizeheader = 0;
		mvc_trans(m);
		s = m->session->schema = mvc_bind_schema(m, "sys");
		assert(m->session->schema != NULL);

		if (!first) {
			t = mvc_bind_table(m, s, "tables");
			tid = t->base.id;
			mvc_drop_table(m, s, t, 0);
			t = mvc_bind_table(m, s, "columns");
			cid = t->base.id;
			mvc_drop_table(m, s, t, 0);
		}

		t = mvc_create_view(m, s, "tables", SQL_PERSIST, "SELECT \"id\", \"name\", \"schema_id\", \"query\", CAST(CASE WHEN \"system\" THEN \"type\" + 10 /* system table/view */ ELSE (CASE WHEN \"commit_action\" = 0 THEN \"type\" /* table/view */ ELSE \"type\" + 20 /* global temp table */ END) END AS SMALLINT) AS \"type\", \"system\", \"commit_action\", \"access\", CASE WHEN (NOT \"system\" AND \"commit_action\" > 0) THEN 1 ELSE 0 END AS \"temporary\" FROM \"sys\".\"_tables\" WHERE \"type\" <> 2 UNION ALL SELECT \"id\", \"name\", \"schema_id\", \"query\", CAST(\"type\" + 30 /* local temp table */ AS SMALLINT) AS \"type\", \"system\", \"commit_action\", \"access\", 1 AS \"temporary\" FROM \"tmp\".\"_tables\";", 1);
		ntid = t->base.id;
		mvc_create_column_(m, t, "id", "int", 32);
		mvc_create_column_(m, t, "name", "varchar", 1024);
		mvc_create_column_(m, t, "schema_id", "int", 32);
		mvc_create_column_(m, t, "query", "varchar", 2048);
		mvc_create_column_(m, t, "type", "smallint", 16);
		mvc_create_column_(m, t, "system", "boolean", 1);
		mvc_create_column_(m, t, "commit_action", "smallint", 16);
		mvc_create_column_(m, t, "access", "smallint", 16);
		mvc_create_column_(m, t, "temporary", "smallint", 16);

		if (!first) {
			int pub = ROLE_PUBLIC;
			int p = PRIV_SELECT;
			int zero = 0;
			sql_table *privs = find_sql_table(s, "privileges");
			sql_table *deps = find_sql_table(s, "dependencies");
			sql_column *depids = find_sql_column(deps, "id");
			oid rid;

			table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
			while ((rid = table_funcs.column_find_row(m->session->tr, depids, &tid, NULL)) != oid_nil) {
				table_funcs.column_update_value(m->session->tr, depids, rid, &ntid);
			}
		}

		t = mvc_create_view(m, s, "columns", SQL_PERSIST, "SELECT * FROM (SELECT p.* FROM \"sys\".\"_columns\" AS p UNION ALL SELECT t.* FROM \"tmp\".\"_columns\" AS t) AS columns;", 1);
		ncid = t->base.id;
		mvc_create_column_(m, t, "id", "int", 32);
		mvc_create_column_(m, t, "name", "varchar", 1024);
		mvc_create_column_(m, t, "type", "varchar", 1024);
		mvc_create_column_(m, t, "type_digits", "int", 32);
		mvc_create_column_(m, t, "type_scale", "int", 32);
		mvc_create_column_(m, t, "table_id", "int", 32);
		mvc_create_column_(m, t, "default", "varchar", 2048);
		mvc_create_column_(m, t, "null", "boolean", 1);
		mvc_create_column_(m, t, "number", "int", 32);
		mvc_create_column_(m, t, "storage", "varchar", 2048);

		if (!first) {
			int pub = ROLE_PUBLIC;
			int p = PRIV_SELECT;
			int zero = 0;
			sql_table *privs = find_sql_table(s, "privileges");
			sql_table *deps = find_sql_table(s, "dependencies");
			sql_column *depids = find_sql_column(deps, "id");
			oid rid;

			table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
			while ((rid = table_funcs.column_find_row(m->session->tr, depids, &cid, NULL)) != oid_nil) {
				table_funcs.column_update_value(m->session->tr, depids, rid, &ncid);
			}
		} else { 
			sql_create_env(m, s);
			sql_create_privileges(m, s);
		}

		s = m->session->schema = mvc_bind_schema(m, "tmp");
		assert(m->session->schema != NULL);

		if (mvc_commit(m, 0, NULL) < 0) {
			fprintf(stderr, "!mvc_init: unable to commit system tables\n");
			return -1;
		}

		mvc_destroy(m);
	}
	return first;
}

void
mvc_exit(void)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_exit\n");

	store_exit();
	keyword_exit();
}

void
mvc_logmanager(void)
{
	Thread thr = THRnew("logmanager");

	store_manager();
	THRdel(thr);
}

void
mvc_idlemanager(void)
{
	Thread thr = THRnew("idlemanager");

	idle_manager();
	THRdel(thr);
}

int
mvc_status(mvc *m)
{
	int res = m->session->status;

	return res;
}

int
mvc_type(mvc *m)
{
	int res = m->type;

	m->type = Q_PARSE;
	return res;
}

int
mvc_debug_on(mvc *m, int flg)
{

	if (m->debug & flg)
		return 1;

	return 0;
}

void
mvc_trans(mvc *m)
{
	int schema_changed = 0, err = m->session->status;
	assert(!m->session->active);	/* can only start a new transaction */

	store_lock();
	schema_changed = sql_trans_begin(m->session);
	if (m->qc && (schema_changed || m->qc->nr > m->cache || err)){
		if (schema_changed || err) {
			int seqnr = m->qc->id;
			if (m->qc)
				qc_destroy(m->qc);
			m->qc = qc_create(m->clientid, seqnr);
		} else { /* clean all but the prepared statements */
			qc_clean(m->qc);
		}
	}
	store_unlock();
}

static sql_trans *
sql_trans_deref( sql_trans *tr ) 
{
	node *n, *m, *o;

	for ( n = tr->schemas.set->h; n; n = n->next) {
		sql_schema *s = n->data;

		if (s->tables.set)
		for ( m = s->tables.set->h; m; m = m->next) {
			sql_table *t = m->data;

			if (t->po) { 
				sql_table *p = t->po;

				t->po = t->po->po;
				table_destroy(p);
			}

			if (t->columns.set)
			for ( o = t->columns.set->h; o; o = o->next) {
				sql_column *c = o->data;

				if (c->po) {
					sql_column *p = c->po;

					c->po = c->po->po;
					column_destroy(p);
				}
			}
			if (t->idxs.set)
			for ( o = t->idxs.set->h; o; o = o->next) {
				sql_idx *i = o->data;

				if (i->po) {
					sql_idx *p = i->po;

					i->po = i->po->po;
					idx_destroy(p);
				}
			}
		}
	}
	return tr->parent;
}

int
mvc_commit(mvc *m, int chain, const char *name)
{
	sql_trans *cur, *tr = m->session->tr, *ctr;
	int ok = SQL_OK;//, wait = 0;

	assert(tr);
	assert(m->session->active);	/* only commit an active transaction */
	
	if (mvc_debug)
		fprintf(stderr, "#mvc_commit %s\n", (name) ? name : "");

	if (m->session->status < 0) {
		(void)sql_error(m, 010, "40000!COMMIT: transaction is aborted, will ROLLBACK instead");
		mvc_rollback(m, chain, name);
		return -1;
	}

	/* savepoint then simply make a copy of the current transaction */
	if (name && name[0] != '\0') {
		sql_trans *tr = m->session->tr;
		if (mvc_debug)
			fprintf(stderr, "#mvc_savepoint\n");
		store_lock();
		m->session->tr = sql_trans_create(m->session->stk, tr, name);
		store_unlock();
		m->type = Q_TRANS;
		if (m->qc) /* clean query cache, protect against concurrent access on the hash tables (when functions already exists, concurrent mal will
build up the hash (not copied in the trans dup)) */
			qc_clean(m->qc);
		m->session->schema = find_sql_schema(m->session->tr, m->session->schema_name);
		if (mvc_debug)
			fprintf(stderr, "#mvc_commit %s done\n", name);
		return 0;
	}

	/* first release all intermediate savepoints */
	ctr = cur = tr;
	tr = tr->parent;
	if (tr->parent) {
		store_lock();
		while (ctr->parent->parent != NULL && ok == SQL_OK) {
			/* first free references to tr objects, ie
			 * c->po = c->po->po etc
			 */
			ctr = sql_trans_deref(ctr);
		}
		while (tr->parent != NULL && ok == SQL_OK) 
			tr = sql_trans_destroy(tr);
		store_unlock();
	}
	cur -> parent = tr;
	tr = cur;

	store_lock();
	/* if there is nothing to commit reuse the current transaction */
	if (tr->wtime == 0) {
		if (!chain) 
			sql_trans_end(m->session);
		m->type = Q_TRANS;
		if (mvc_debug)
			fprintf(stderr, "#mvc_commit %s done\n", (name) ? name : "");
		store_unlock();
		return 0;
	}

	/*
	while (tr->schema_updates && store_nr_active > 1) {
		store_unlock();
		MT_sleep_ms(100);
		wait += 100;
		if (wait > 1000) {
			(void)sql_error(m, 010, "40000!COMMIT: transaction is aborted because of DDL concurrency conflicts, will ROLLBACK instead");
			mvc_rollback(m, chain, name);
			return -1;
		}
		store_lock();
	}
	 * */
	/* validation phase */
	if (sql_trans_validate(tr)) {
		if ((ok = sql_trans_commit(tr)) != SQL_OK) {
			char *msg = sql_message("40000!COMMIT: transaction commit failed (perhaps your disk is full?) exiting (kernel error: %s)", GDKerrbuf);
			GDKfatal("%s", msg);
			_DELETE(msg);
		}
	} else {
		store_unlock();
		(void)sql_error(m, 010, "40000!COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead");
		mvc_rollback(m, chain, name);
		return -1;
	}
	sql_trans_end(m->session);
	if (chain) 
		sql_trans_begin(m->session);
	store_unlock();
	m->type = Q_TRANS;
	if (mvc_debug)
		fprintf(stderr, "#mvc_commit %s done\n", (name) ? name : "");
	return ok;
}

int
mvc_rollback(mvc *m, int chain, const char *name)
{
	int res = 0;
	sql_trans *tr = m->session->tr;

	if (mvc_debug)
		fprintf(stderr, "#mvc_rollback %s\n", (name) ? name : "");

	assert(tr);
	assert(m->session->active);	/* only abort an active transaction */

	store_lock();
	if (m->qc) 
		qc_clean(m->qc);
	if (name && name[0] != '\0') {
		while (tr && (!tr->name || strcmp(tr->name, name) != 0))
			tr = tr->parent;
		if (!tr) {
			(void)sql_error(m, 010, "ROLLBACK: no such savepoint: '%s'", name);
			m->session->status = -1;
			store_unlock();
			return -1;
		}
		tr = m->session->tr;
		while (!tr->name || strcmp(tr->name, name) != 0) {
			/* make sure we do not reuse changed data */
			if (tr->wtime)
				tr->status = 1;
			tr = sql_trans_destroy(tr);
		}
		m->session->tr = tr;	/* restart at savepoint */
		m->session->status = tr->status;
		if (tr->name) 
			tr->name = NULL;
		m->session->schema = find_sql_schema(m->session->tr, m->session->schema_name);
	} else if (tr->parent) {
		/* first release all intermediate savepoints */
		while (tr->parent->parent != NULL) {
			tr = sql_trans_destroy(tr);
		}
		m->session-> tr = tr;
		/* make sure we do not reuse changed data */
		if (tr->wtime)
			tr->status = 1;
		sql_trans_end(m->session);
		if (chain) 
			sql_trans_begin(m->session);
	}
	store_unlock();
	m->type = Q_TRANS;
	if (mvc_debug)
		fprintf(stderr, "#mvc_rollback %s done\n", (name) ? name : "");
	return res;
}

/* release all savepoints up including the given named savepoint 
 * but keep the current changes.
 * */
int
mvc_release(mvc *m, const char *name)
{
	int ok = SQL_OK;
	int res = Q_TRANS;
	sql_trans *tr = m->session->tr;

	assert(tr);
	assert(m->session->active);	/* only release active transactions */

	if (mvc_debug)
		fprintf(stderr, "#mvc_release %s\n", (name) ? name : "");

	if (!name)
		mvc_rollback(m, 0, name);

	while (tr && (!tr->name || strcmp(tr->name, name) != 0))
		tr = tr->parent;
	if (!tr || !tr->name || strcmp(tr->name, name) != 0) {
		(void)sql_error(m, 010, "release savepoint %s doesn't exists", name);
		m->session->status = -1;
		return -1;
	}
	tr = m->session->tr;
	store_lock();
	while (ok == SQL_OK && (!tr->name || strcmp(tr->name, name) != 0)) {
		/* commit all intermediate savepoints */
		if (sql_trans_commit(tr) != SQL_OK)
			GDKfatal("release savepoints should not fail");
		tr = sql_trans_destroy(tr);
	}
	tr->name = NULL;
	store_unlock();
	m->session->tr = tr;
	m->session->schema = find_sql_schema(m->session->tr, m->session->schema_name);

	m->type = res;
	return res;
}

mvc *
mvc_create(int clientid, backend_stack stk, int debug, bstream *rs, stream *ws)
{
	int i;
	mvc *m;

 	m = ZNEW(mvc);
	if(!m) {
		return NULL;
	}
	if (mvc_debug)
		fprintf(stderr, "#mvc_create\n");

	m->errstr[0] = '\0';
	/* if an error exceeds the buffer we don't want garbage at the end */
	m->errstr[ERRSIZE-1] = '\0';

	m->qc = qc_create(clientid, 0);
	if(!m->qc) {
		_DELETE(m);
		return NULL;
	}
	m->sa = NULL;

	m->params = NULL;
	m->sizevars = MAXPARAMS;
	m->vars = NEW_ARRAY(sql_var, m->sizevars);

	m->topvars = 0;
	m->frame = 1;
	m->use_views = 0;
	m->argmax = MAXPARAMS;
	m->args = NEW_ARRAY(atom*,m->argmax);
	if(!m->vars || !m->args) {
		qc_destroy(m->qc);
		_DELETE(m->vars);
		_DELETE(m->args);
		_DELETE(m);
		return NULL;
	}
	m->argc = 0;
	m->sym = NULL;

	m->rowcnt = m->last_id = m->role_id = m->user_id = -1;
	m->timezone = 0;
	m->clientid = clientid;

	m->emode = m_normal;
	m->emod = mod_none;
	m->reply_size = 100;
	m->debug = debug;
	m->cache = DEFAULT_CACHESIZE;
	m->caching = m->cache;
	m->history = 0;

	m->label = 0;
	m->cascade_action = NULL;
	for(i=0;i<MAXSTATS;i++)
		m->opt_stats[i] = 0;

	store_lock();
	m->session = sql_session_create(stk, 1 /*autocommit on*/);
	store_unlock();
	if(!m->session) {
		qc_destroy(m->qc);
		_DELETE(m->vars);
		_DELETE(m->args);
		_DELETE(m);
		return NULL;
	}

	m->type = Q_PARSE;
	m->pushdown = 1;

	m->result_id = 0;
	m->results = NULL;

	scanner_init(&m->scanner, rs, ws);
	return m;
}

void
mvc_reset(mvc *m, bstream *rs, stream *ws, int debug, int globalvars)
{
	int i;
	sql_trans *tr;

	if (mvc_debug)
		fprintf(stderr, "#mvc_reset\n");
	tr = m->session->tr;
	if (tr && tr->parent) {
		assert(m->session->active == 0);
		store_lock();
		while (tr->parent->parent != NULL) 
			tr = sql_trans_destroy(tr);
		store_unlock();
	}
	if (tr)
		sql_session_reset(m->session, 1 /*autocommit on*/);

	if (m->sa)
		m->sa = sa_reset(m->sa);
	else 
		m->sa = sa_create();

	m->errstr[0] = '\0';

	m->params = NULL;
	/* reset topvars to the set of global variables */
	stack_pop_until(m, globalvars);
	m->frame = 1;
	m->argc = 0;
	m->sym = NULL;

	m->rowcnt = m->last_id = m->role_id = m->user_id = -1;
	m->emode = m_normal;
	m->emod = mod_none;
	if (m->reply_size != 100)
		stack_set_number(m, "reply_size", 100);
	m->reply_size = 100;
	if (m->timezone != 0)
		stack_set_number(m, "current_timezone", 0);
	m->timezone = 0;
	if (m->debug != debug)
		stack_set_number(m, "debug", debug);
	m->debug = debug;
	if (m->cache != DEFAULT_CACHESIZE)
		stack_set_number(m, "cache", DEFAULT_CACHESIZE);
	m->cache = DEFAULT_CACHESIZE;
	m->caching = m->cache;
	if (m->history != 0)
		stack_set_number(m, "history", 0);
	m->history = 0;

	m->label = 0;
	m->cascade_action = NULL;
	m->type = Q_PARSE;
	m->pushdown = 1;

	for(i=0;i<MAXSTATS;i++)
		m->opt_stats[i] = 0;

	m->result_id = 0;
	m->results = NULL;

	scanner_init(&m->scanner, rs, ws);
}

void
mvc_destroy(mvc *m)
{
	sql_trans *tr;

	if (mvc_debug)
		fprintf(stderr, "#mvc_destroy\n");
	tr = m->session->tr;
	if (tr) {
		store_lock();
		if (m->session->active)
			sql_trans_end(m->session);
		while (tr->parent)
			tr = sql_trans_destroy(tr);
		m->session->tr = NULL;
		store_unlock();
	}
	sql_session_destroy(m->session);

	stack_pop_until(m, 0);
	_DELETE(m->vars);

	if (m->scanner.log) /* close and destroy stream */
		close_stream(m->scanner.log);

	if (m->sa)
		sa_destroy(m->sa);
	m->sa = NULL;
	if (m->qc)
		qc_destroy(m->qc);
	m->qc = NULL;
	m->sqs = NULL;

	_DELETE(m->args);
	m->args = NULL;
	_DELETE(m);
}

sql_type *
mvc_bind_type(mvc *sql, const char *name)
{
	sql_type *t = sql_trans_bind_type(sql->session->tr, NULL, name);

	if (mvc_debug)
		fprintf(stderr, "#mvc_bind_type %s\n", name);
	return t;
}

sql_type *
schema_bind_type(mvc *sql, sql_schema *s, const char *name)
{
	sql_type *t = find_sql_type(s, name);

	(void) sql;
	if (!t)
		return NULL;
	if (mvc_debug)
		fprintf(stderr, "#schema_bind_type %s\n", name);
	return t;
}

sql_func *
mvc_bind_func(mvc *sql, const char *name)
{
	sql_func *t = sql_trans_bind_func(sql->session->tr, name);

	if (mvc_debug)
		fprintf(stderr, "#mvc_bind_func %s\n", name);
	return t;
}

list *
schema_bind_func(mvc *sql, sql_schema * s, const char *name, int type)
{
	list *func_list = find_all_sql_func(s, name, type);

	(void) sql;
	if (!func_list)
		return NULL;
	if (mvc_debug)
		fprintf(stderr, "#schema_bind_func %s\n", name);
	return func_list;
}

sql_schema *
mvc_bind_schema(mvc *m, const char *sname)
{
	sql_trans *tr = m->session->tr;
	sql_schema *s;

	if (!tr)
		return NULL;

	/* declared tables */
	if (strcmp(sname, str_nil) == 0)
		sname = dt_schema;
 	s = find_sql_schema(tr, sname);
	if (!s)
		return NULL;

	if (mvc_debug)
		fprintf(stderr, "#mvc_bind_schema %s\n", sname);
	return s;
}

sql_table *
mvc_bind_table(mvc *m, sql_schema *s, const char *tname)
{
	sql_table *t = NULL;

	if (!s) { /* Declared tables during query compilation have no schema */
		sql_table *tpe = stack_find_table(m, tname);
		if (tpe) {
			t = tpe;
		} else { /* during exection they are in the declared table schema */
			s = mvc_bind_schema(m, dt_schema);
			return mvc_bind_table(m, s, tname);
		}
	} else {
 		t = find_sql_table(s, tname);
	}
	if (!t)
		return NULL;
	if (mvc_debug)
		fprintf(stderr, "#mvc_bind_table %s.%s\n", s ? s->base.name : "<noschema>", tname);

	return t;
}

sql_column *
mvc_bind_column(mvc *m, sql_table *t, const char *cname)
{
	sql_column *c;

	(void)m;
	c = find_sql_column(t, cname);
	if (!c)
		return NULL;
	if (mvc_debug)
		fprintf(stderr, "#mvc_bind_column %s.%s\n", t->base.name, cname);
	return c;
}

static sql_column *
first_column(sql_table *t)
{
	node *n = cs_first_node(&t->columns);

	if (n)
		return n->data;
	return NULL;
}


sql_column *
mvc_first_column(mvc *m, sql_table *t)
{
	sql_column *c = first_column(t);

	(void) m;
	if (!c)
		return NULL;
	if (mvc_debug)
		fprintf(stderr, "#mvc_first_column %s.%s\n", t->base.name, c->base.name);

	return c;
}

sql_key *
mvc_bind_key(mvc *m, sql_schema *s, const char *kname)
{
	node *n = list_find_name(s->keys, kname);
	sql_key *k;

	(void) m;
	if (!n)
		return NULL;
	k = n->data;

	if (mvc_debug)
		fprintf(stderr, "#mvc_bind_key %s.%s\n", s->base.name, kname);

	return k;
}

sql_idx *
mvc_bind_idx(mvc *m, sql_schema *s, const char *iname)
{
	node *n = list_find_name(s->idxs, iname);
	sql_idx *i;

	(void) m;
	if (!n)
		return NULL;
	i = n->data;

	if (mvc_debug)
		fprintf(stderr, "#mvc_bind_idx %s.%s\n", s->base.name, iname);

	return i;
}

static int
uniqueKey(sql_key *k)
{
	return (k->type == pkey || k->type == ukey);
}

sql_key *
mvc_bind_ukey(sql_table *t, list *colnames)
{
	node *cn;
	node *cur;
	sql_key *res = NULL;
	int len = list_length(colnames);

	if (cs_size(&t->keys))
		for (cur = t->keys.set->h; cur; cur = cur->next) {
			node *cc;
			sql_key *k = cur->data;

			if (uniqueKey(k) && list_length(k->columns) == len) {
				res = k;
				for (cc = k->columns->h, cn = colnames->h; cc && cn; cc = cc->next, cn = cn->next) {
					sql_kc *c = cc->data;
					char *n = cn->data;

					if (strcmp(c->c->base.name, n) != 0) {
						res = NULL;
						break;
					}
				}
				if (res)
					break;
			}
		}
	return res;
}

sql_trigger *
mvc_bind_trigger(mvc *m, sql_schema *s, const char *tname)
{
	node *n = list_find_name(s->triggers, tname);
	sql_trigger *trigger;

	(void) m;
	if (!n)
		return NULL;
	trigger = n->data;

	if (mvc_debug)
		fprintf(stderr, "#mvc_bind_trigger %s.%s\n", s->base.name, tname);

	return trigger;
}

sql_type *
mvc_create_type(mvc *sql, sql_schema * s, const char *name, int digits, int scale, int radix, const char *impl)
{
	sql_type *t = NULL;

	if (mvc_debug)
		fprintf(stderr, "#mvc_create_type %s\n", name);

	t = sql_trans_create_type(sql->session->tr, s, name, digits, scale, radix, impl);
	return t;
}

int
mvc_drop_type(mvc *m, sql_schema *s, sql_type *t, int drop_action)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_drop_type %s %s\n", s->base.name, t->base.name);

	if (t)
		return sql_trans_drop_type(m->session->tr, s, t->base.id, drop_action);
	return 0;
}

sql_func *
mvc_create_func(mvc *sql, sql_allocator *sa, sql_schema * s, const char *name, list *args, list *res, int type, int lang, const char *mod, const char *impl, const char *query, bit varres, bit vararg)
{
	sql_func *f = NULL;

	if (mvc_debug)
		fprintf(stderr, "#mvc_create_func %s\n", name);
	if (sa) {
		f = create_sql_func(sa, name, args, res, type, lang, mod, impl, query, varres, vararg);
		f->s = s;
	} else 
		f = sql_trans_create_func(sql->session->tr, s, name, args, res, type, lang, mod, impl, query, varres, vararg);
	return f;
}

void
mvc_drop_func(mvc *m, sql_schema *s, sql_func *f, int drop_action)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_drop_func %s %s\n", s->base.name, f->base.name);

	sql_trans_drop_func(m->session->tr, s, f->base.id, drop_action ? DROP_CASCADE_START : DROP_RESTRICT);
}

void
mvc_drop_all_func(mvc *m, sql_schema *s, list *list_func, int drop_action)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_drop_all_func %s %s\n", s->base.name, ((sql_func *) list_func->h->data)->base.name);

	sql_trans_drop_all_func(m->session->tr, s, list_func, drop_action ? DROP_CASCADE_START : DROP_RESTRICT);
}

sql_schema *
mvc_create_schema(mvc *m, const char *name, int auth_id, int owner)
{
	sql_schema *s = NULL;

	if (mvc_debug)
		fprintf(stderr, "#mvc_create_schema %s %d %d\n", name, auth_id, owner);

	s = sql_trans_create_schema(m->session->tr, name, auth_id, owner);
	return s;
}

void
mvc_drop_schema(mvc *m, sql_schema * s, int drop_action)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_drop_schema %s\n", s->base.name);
	sql_trans_drop_schema(m->session->tr, s->base.id, drop_action ? DROP_CASCADE_START : DROP_RESTRICT);
}

sql_ukey *
mvc_create_ukey(mvc *m, sql_table *t, const char *name, key_type kt)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_create_ukey %s %u\n", t->base.name, kt);
	if (t->persistence == SQL_DECLARED_TABLE)
		return create_sql_ukey(m->sa, t, name, kt);	
	else
		return (sql_ukey*)sql_trans_create_ukey(m->session->tr, t, name, kt);
}

sql_key *
mvc_create_ukey_done(mvc *m, sql_key *k)
{
	if (k->t->persistence == SQL_DECLARED_TABLE)
		return key_create_done(m->sa, k);
	else
		return sql_trans_key_done(m->session->tr, k);
}

sql_fkey *
mvc_create_fkey(mvc *m, sql_table *t, const char *name, key_type kt, sql_key *rkey, int on_delete, int on_update)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_create_fkey %s %u " PTRFMT "\n", t->base.name, kt, PTRFMTCAST rkey);
	if (t->persistence == SQL_DECLARED_TABLE)
		return create_sql_fkey(m->sa, t, name, kt, rkey, on_delete, on_update);	
	else
		return sql_trans_create_fkey(m->session->tr, t, name, kt, rkey, on_delete, on_update);
}

sql_key *
mvc_create_kc(mvc *m, sql_key *k, sql_column *c)
{
	if (k->t->persistence == SQL_DECLARED_TABLE)
		return create_sql_kc(m->sa, k, c);
	else
		return sql_trans_create_kc(m->session->tr, k, c);
}

sql_fkey *
mvc_create_fkc(mvc *m, sql_fkey *fk, sql_column *c)
{
	sql_key *k = (sql_key*)fk;

	if (k->t->persistence == SQL_DECLARED_TABLE)
		return (sql_fkey*)create_sql_kc(m->sa, k, c);
	else
		return sql_trans_create_fkc(m->session->tr, fk, c);
}

void
mvc_drop_key(mvc *m, sql_schema *s, sql_key *k, int drop_action)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_drop_key %s %s\n", s->base.name, k->base.name);
	if (k->t->persistence == SQL_DECLARED_TABLE)
		drop_sql_key(k->t, k->base.id, drop_action);
	else
		sql_trans_drop_key(m->session->tr, s, k->base.id, drop_action ? DROP_CASCADE_START : DROP_RESTRICT);
}

sql_idx *
mvc_create_idx(mvc *m, sql_table *t, const char *name, idx_type it)
{
	sql_idx *i;

	if (mvc_debug)
		fprintf(stderr, "#mvc_create_idx %s %u\n", t->base.name, it);

	if (t->persistence == SQL_DECLARED_TABLE)
		/* declared tables should not end up in the catalog */
		return create_sql_idx(m->sa, t, name, it);
	else
		i = sql_trans_create_idx(m->session->tr, t, name, it);
	return i;
}

sql_idx *
mvc_create_ic(mvc *m, sql_idx * i, sql_column *c)
{
	if (i->t->persistence == SQL_DECLARED_TABLE)
		/* declared tables should not end up in the catalog */
		return create_sql_ic(m->sa, i, c);
	else
		return sql_trans_create_ic(m->session->tr, i, c);
}

void
mvc_drop_idx(mvc *m, sql_schema *s, sql_idx *i)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_drop_idx %s %s\n", s->base.name, i->base.name);

	if (i->t->persistence == SQL_DECLARED_TABLE)
		/* declared tables should not end up in the catalog */
		drop_sql_idx(i->t, i->base.id);
	else
		sql_trans_drop_idx(m->session->tr, s, i->base.id, DROP_RESTRICT);
}

sql_trigger * 
mvc_create_trigger(mvc *m, sql_table *t, const char *name, sht time, sht orientation, sht event, const char *old_name, const char *new_name, const char *condition, const char *statement )
{
	sql_trigger *i;

	if (mvc_debug)
		fprintf(stderr, "#mvc_create_trigger %s %d %d %d\n", t->base.name, time, orientation, event);

	i = sql_trans_create_trigger(m->session->tr, t, name, time, orientation, 
			event, old_name, new_name, condition, statement);
	return i;
}

sql_trigger *
mvc_create_tc(mvc *m, sql_trigger * i, sql_column *c /*, extra options such as trunc */ )
{
	sql_trans_create_tc(m->session->tr, i, c);
	return i;
}

void
mvc_drop_trigger(mvc *m, sql_schema *s, sql_trigger *tri)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_drop_trigger %s %s\n", s->base.name, tri->base.name);

	sql_trans_drop_trigger(m->session->tr, s, tri->base.id, DROP_RESTRICT);
}


sql_table *
mvc_create_table(mvc *m, sql_schema *s, const char *name, int tt, bit system, int persistence, int commit_action, int sz)
{
	sql_table *t = NULL;

	if (mvc_debug)
		fprintf(stderr, "#mvc_create_table %s %s %d %d %d %d\n", s->base.name, name, tt, system, persistence, commit_action);

	if (persistence == SQL_DECLARED_TABLE && (!s || strcmp(s->base.name, dt_schema))) {
		t = create_sql_table(m->sa, name, tt, system, persistence, commit_action);
		t->s = s;
	} else {
		t = sql_trans_create_table(m->session->tr, s, name, NULL, tt, system, persistence, commit_action, sz);
	}
	return t;
}

sql_table *
mvc_create_view(mvc *m, sql_schema *s, const char *name, int persistence, const char *sql, bit system)
{
	sql_table *t = NULL;

	if (mvc_debug)
		fprintf(stderr, "#mvc_create_view %s %s %s\n", s->base.name, name, sql);

	if (persistence == SQL_DECLARED_TABLE) {
		t = create_sql_table(m->sa, name, tt_view, system, persistence, 0);
		t->s = s;
		t->query = sa_strdup(m->sa, sql);
	} else {
		t = sql_trans_create_table(m->session->tr, s, name, sql, tt_view, system, SQL_PERSIST, 0, 0);
	}
	return t;
}

sql_table *
mvc_create_remote(mvc *m, sql_schema *s, const char *name, int persistence, const char *loc)
{
	sql_table *t = NULL;

	if (mvc_debug)
		fprintf(stderr, "#mvc_create_remote %s %s %s\n", s->base.name, name, loc);

	if (persistence == SQL_DECLARED_TABLE) {
		t = create_sql_table(m->sa, name, tt_remote, 0, persistence, 0);
		t->s = s;
		t->query = sa_strdup(m->sa, loc);
	} else {
		t = sql_trans_create_table(m->session->tr, s, name, loc, tt_remote, 0, SQL_REMOTE, 0, 0);
	}
	return t;
}

void
mvc_drop_table(mvc *m, sql_schema *s, sql_table *t, int drop_action)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_drop_table %s %s\n", s->base.name, t->base.name);

	sql_trans_drop_table(m->session->tr, s, t->base.id, drop_action ? DROP_CASCADE_START : DROP_RESTRICT);
}

BUN
mvc_clear_table(mvc *m, sql_table *t)
{
	return sql_trans_clear_table(m->session->tr, t);
}

sql_column *
mvc_create_column_(mvc *m, sql_table *t, const char *name, const char *type, int digits)
{
	sql_subtype tpe;

	if (!sql_find_subtype(&tpe, type, digits, 0))
		return NULL;

	return sql_trans_create_column(m->session->tr, t, name, &tpe);
}

sql_column *
mvc_create_column(mvc *m, sql_table *t, const char *name, sql_subtype *tpe)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_create_column %s %s %s\n", t->base.name, name, tpe->type->sqlname);
	if (t->persistence == SQL_DECLARED_TABLE && (!t->s || strcmp(t->s->base.name, dt_schema))) 
		/* declared tables should not end up in the catalog */
		return create_sql_column(m->sa, t, name, tpe);
	else
		return sql_trans_create_column(m->session->tr, t, name, tpe);
}

void
mvc_drop_column(mvc *m, sql_table *t, sql_column *col, int drop_action)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_drop_column %s %s\n", t->base.name, col->base.name);
	if (col->t->persistence == SQL_DECLARED_TABLE)
		drop_sql_column(t, col->base.id, drop_action);
	else
		sql_trans_drop_column(m->session->tr, t, col->base.id,  drop_action ? DROP_CASCADE_START : DROP_RESTRICT);
}

void
mvc_create_dependency(mvc *m, int id, int depend_id, int depend_type)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_create_dependency %d %d %d\n", id, depend_id, depend_type);
	if ( (id != depend_id) || (depend_type == BEDROPPED_DEPENDENCY) )
		sql_trans_create_dependency(m->session->tr, id, depend_id, depend_type);
	
}

void
mvc_create_dependencies(mvc *m, list *id_l, sqlid depend_id, int dep_type)
{
	node *n = id_l->h;
	int i;
	
	if (mvc_debug)
		fprintf(stderr, "#mvc_create_dependencies on %d of type %d\n", depend_id, dep_type);

	for (i = 0; i < list_length(id_l); i++)
	{
		mvc_create_dependency(m, *(int *) n->data, depend_id, dep_type);
		n = n->next;
	}

}

int
mvc_check_dependency(mvc * m, int id, int type, list *ignore_ids)
{
	list *dep_list = NULL;
	
	if (mvc_debug)
		fprintf(stderr, "#mvc_check_dependency on %d\n", id);


	switch(type) {
		case OWNER_DEPENDENCY : 
			dep_list = sql_trans_owner_schema_dependencies(m->session->tr, id);
			break;
		case SCHEMA_DEPENDENCY :
			dep_list = sql_trans_schema_user_dependencies(m->session->tr, id);
			break;
		case TABLE_DEPENDENCY : 
			dep_list = sql_trans_get_dependencies(m->session->tr, id, TABLE_DEPENDENCY, NULL);
			break;
		case VIEW_DEPENDENCY : 
			dep_list = sql_trans_get_dependencies(m->session->tr, id, TABLE_DEPENDENCY, NULL);
			break;
		case FUNC_DEPENDENCY : 
		case PROC_DEPENDENCY :
			dep_list = sql_trans_get_dependencies(m->session->tr, id, FUNC_DEPENDENCY, ignore_ids);
			break;
		default: 
			dep_list =  sql_trans_get_dependencies(m->session->tr, id, COLUMN_DEPENDENCY, NULL);
	}
	
	if ( list_length(dep_list) >= 2 ) {
		list_destroy(dep_list);
		return HAS_DEPENDENCY;
	}
	
	list_destroy(dep_list);
	return NO_DEPENDENCY;
}

sql_column *
mvc_null(mvc *m, sql_column *col, int isnull)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_null %s %d\n", col->base.name, isnull);

	if (col->t->persistence == SQL_DECLARED_TABLE) {
		col->null = isnull;
		return col;
	}
	return sql_trans_alter_null(m->session->tr, col, isnull);
}

sql_column *
mvc_default(mvc *m, sql_column *col, char *val)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_default %s %s\n", col->base.name, val);

	if (col->t->persistence == SQL_DECLARED_TABLE) {
		col->def = val?sa_strdup(m->sa, val):NULL;
		return col;
	} else {
		return sql_trans_alter_default(m->session->tr, col, val);
	}
}

sql_column *
mvc_drop_default(mvc *m, sql_column *col)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_drop_default %s\n", col->base.name);

	if (col->t->persistence == SQL_DECLARED_TABLE) {
		col->def = NULL;
		return col;
	} else {
		return sql_trans_alter_default(m->session->tr, col, NULL);
	}
}

sql_column *
mvc_storage(mvc *m, sql_column *col, char *storage)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_storage %s %s\n", col->base.name, storage);

	if (col->t->persistence == SQL_DECLARED_TABLE) {
		col->storage_type = storage?sa_strdup(m->sa, storage):NULL;
		return col;
	} else {
		return sql_trans_alter_storage(m->session->tr, col, storage);
	}
}

sql_table *
mvc_access(mvc *m, sql_table *t, sht access)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_access %s %d\n", t->base.name, access);

	if (t->persistence == SQL_DECLARED_TABLE) {
		t->access = access;
		return t;
	}
	return sql_trans_alter_access(m->session->tr, t, access);
}

int 
mvc_is_sorted(mvc *m, sql_column *col)
{
	if (mvc_debug)
		fprintf(stderr, "#mvc_is_sorted %s\n", col->base.name);

	return sql_trans_is_sorted(m->session->tr, col);
}

/* variable management */
static void
stack_set(mvc *sql, int var, const char *name, sql_subtype *type, sql_rel *rel, sql_table *t, int view, int frame)
{
	sql_var *v;
	if (var == sql->sizevars) {
		sql->sizevars <<= 1;
		sql->vars = RENEW_ARRAY(sql_var,sql->vars,sql->sizevars);
	}
	v = sql->vars+var;

	v->name = NULL;
	atom_init( &v->a );
	v->rel = rel;
	v->t = t;
	v->view = view;
	v->frame = frame;
	if (type) {
		int tpe = type->type->localtype;
		VALset(&sql->vars[var].a.data, tpe, (ptr) ATOMnilptr(tpe));
		v->a.tpe = *type;
	}
	if (name)
		v->name = _STRDUP(name);
}

void 
stack_push_var(mvc *sql, const char *name, sql_subtype *type)
{
	stack_set(sql, sql->topvars++, name, type, NULL, NULL, 0, 0);
}

void 
stack_push_rel_var(mvc *sql, const char *name, sql_rel *var, sql_subtype *type)
{
	stack_set(sql, sql->topvars++, name, type, var, NULL, 0, 0);
}

void 
stack_push_table(mvc *sql, const char *name, sql_rel *var, sql_table *t)
{
	stack_set(sql, sql->topvars++, name, NULL, var, t, 0, 0);
}


void 
stack_push_rel_view(mvc *sql, const char *name, sql_rel *var)
{
	stack_set(sql, sql->topvars++, name, NULL, var, NULL, 1, 0);
}


void
stack_set_var(mvc *sql, const char *name, ValRecord *v)
{
	int i;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (!sql->vars[i].frame && strcmp(sql->vars[i].name, name)==0) {
			VALclear(&sql->vars[i].a.data);
			VALcopy(&sql->vars[i].a.data, v);
			sql->vars[i].a.isnull = VALisnil(v);
			if (v->vtype == TYPE_flt)
				sql->vars[i].a.d = v->val.fval;
			else if (v->vtype == TYPE_dbl)
				sql->vars[i].a.d = v->val.dval;
		}
	}
}

atom *
stack_get_var(mvc *sql, const char *name)
{
	int i;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (!sql->vars[i].frame && strcmp(sql->vars[i].name, name)==0) {
			return &sql->vars[i].a;
		}
	}
	return NULL;
}

void 
stack_push_frame(mvc *sql, const char *name)
{
	stack_set(sql, sql->topvars++, name, NULL, NULL, NULL, 0, 1);
	sql->frame++;
}

void
stack_pop_until(mvc *sql, int top) 
{
	while(sql->topvars > top) {
		sql_var *v = &sql->vars[--sql->topvars];

		c_delete(v->name);
		VALclear(&v->a.data);
		v->a.data.vtype = 0;
	}
}

void 
stack_pop_frame(mvc *sql)
{
	while(!sql->vars[--sql->topvars].frame) {
		sql_var *v = &sql->vars[sql->topvars];

		c_delete(v->name);
		VALclear(&v->a.data);
		v->a.data.vtype = 0;
		if (v->t && v->view) 
			table_destroy(v->t);
		else if (v->rel)
			rel_destroy(v->rel);
	}
	if (sql->topvars && sql->vars[sql->topvars].name)  
		c_delete(sql->vars[sql->topvars].name);
	sql->frame--;
}

sql_subtype *
stack_find_type(mvc *sql, const char *name)
{
	int i;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (!sql->vars[i].frame && !sql->vars[i].view &&
			strcmp(sql->vars[i].name, name)==0)
			return &sql->vars[i].a.tpe;
	}
	return NULL;
}

sql_table *
stack_find_table(mvc *sql, const char *name)
{
	int i;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (!sql->vars[i].frame && !sql->vars[i].view &&
		    sql->vars[i].t && strcmp(sql->vars[i].name, name)==0)
			return sql->vars[i].t;
	}
	return NULL;
}

sql_rel *
stack_find_rel_view(mvc *sql, const char *name)
{
	int i;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (!sql->vars[i].frame && sql->vars[i].view &&
		    sql->vars[i].rel && strcmp(sql->vars[i].name, name)==0)
			return rel_dup(sql->vars[i].rel);
	}
	return NULL;
}

void 
stack_update_rel_view(mvc *sql, const char *name, sql_rel *view)
{
	int i;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (!sql->vars[i].frame && sql->vars[i].view &&
		    sql->vars[i].rel && strcmp(sql->vars[i].name, name)==0) {
			rel_destroy(sql->vars[i].rel);
			sql->vars[i].rel = view;
		}
	}
}

int 
stack_find_var(mvc *sql, const char *name)
{
	int i;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (!sql->vars[i].frame && !sql->vars[i].view &&
		    strcmp(sql->vars[i].name, name)==0)
			return 1;
	}
	return 0;
}

sql_rel *
stack_find_rel_var(mvc *sql, const char *name)
{
	int i;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (!sql->vars[i].frame && !sql->vars[i].view &&
		    sql->vars[i].rel && strcmp(sql->vars[i].name, name)==0)
			return rel_dup(sql->vars[i].rel);
	}
	return NULL;
}

int 
frame_find_var(mvc *sql, const char *name)
{
	int i;

	for (i = sql->topvars-1; i >= 0 && !sql->vars[i].frame; i--) {
		if (strcmp(sql->vars[i].name, name)==0)
			return 1;
	}
	return 0;
}

int
stack_find_frame(mvc *sql, const char *name)
{
	int i, frame = sql->frame;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (sql->vars[i].frame) 
			frame--;
		else if (sql->vars[i].name && strcmp(sql->vars[i].name, name)==0)
			return frame;
	}
	return 0;
}

int
stack_has_frame(mvc *sql, const char *name)
{
	int i;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (sql->vars[i].frame && sql->vars[i].name && 
		    strcmp(sql->vars[i].name, name)==0)
			return 1;
	}
	return 0;
}

int
stack_nr_of_declared_tables(mvc *sql)
{
	int i, dt = 0;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (sql->vars[i].rel && !sql->vars[i].view) {
			sql_var *v = &sql->vars[i];
			if (v->t) 
				dt++;
		}
	}
	return dt;
}

void
stack_set_string(mvc *sql, const char *name, const char *val)
{
	atom *a = stack_get_var(sql, name);

	if (a != NULL) {
		ValRecord *v = &a->data;

		if (v->val.sval)
			_DELETE(v->val.sval);
		v->val.sval = _STRDUP(val);
	}
}

str
stack_get_string(mvc *sql, const char *name)
{
	atom *a = stack_get_var(sql, name);

	if (!a || a->data.vtype != TYPE_str)
		return NULL;
	return a->data.val.sval;
}

void
#ifdef HAVE_HGE
stack_set_number(mvc *sql, const char *name, hge val)
#else
stack_set_number(mvc *sql, const char *name, lng val)
#endif
{
	atom *a = stack_get_var(sql, name);

	if (a != NULL) {
		ValRecord *v = &a->data;
#ifdef HAVE_HGE
		if (v->vtype == TYPE_hge) 
			v->val.hval = val;
#endif
		if (v->vtype == TYPE_lng) 
			v->val.lval = val;
		if (v->vtype == TYPE_int) 
			v->val.lval = (int) val;
		if (v->vtype == TYPE_sht) 
			v->val.lval = (sht) val;
		if (v->vtype == TYPE_bte) 
			v->val.lval = (bte) val;
		if (v->vtype == TYPE_bit) {
			if (val)
				v->val.btval = 1;
			else 
				v->val.btval = 0;
		}
	}
}

#ifdef HAVE_HGE
hge
#else
lng
#endif
val_get_number(ValRecord *v) 
{
	if (v != NULL) {
#ifdef HAVE_HGE
		if (v->vtype == TYPE_hge) 
			return v->val.hval;
#endif
		if (v->vtype == TYPE_lng) 
			return v->val.lval;
		if (v->vtype == TYPE_int) 
			return v->val.ival;
		if (v->vtype == TYPE_sht) 
			return v->val.shval;
		if (v->vtype == TYPE_bte) 
			return v->val.btval;
		if (v->vtype == TYPE_bit) 
			if (v->val.btval)
				return 1;
		return 0;
	}
	return 0;
}

#ifdef HAVE_HGE
hge
#else
lng
#endif
stack_get_number(mvc *sql, const char *name)
{
	atom *a = stack_get_var(sql, name);
	return val_get_number(a?&a->data:NULL);
}

sql_column *
mvc_copy_column( mvc *m, sql_table *t, sql_column *c)
{
	return sql_trans_copy_column(m->session->tr, t, c);
}

sql_key *
mvc_copy_key(mvc *m, sql_table *t, sql_key *k)
{
	return sql_trans_copy_key(m->session->tr, t, k);
}

sql_idx *
mvc_copy_idx(mvc *m, sql_table *t, sql_idx *i)
{
	return sql_trans_copy_idx(m->session->tr, t, i);
}

sql_rel *
mvc_push_subquery(mvc *m, const char *name, sql_rel *r)
{
	sql_rel *res = NULL;

	if (!m->sqs)
		m->sqs = sa_list(m->sa);
	if (m->sqs) {
		sql_var *v = SA_NEW(m->sa, sql_var);

		v->name = name;
		v->rel = r;
		list_append(m->sqs, v);
		res = r;
	}
	return res;
}

sql_rel *
mvc_find_subquery(mvc *m, const char *rname, const char *name) 
{
	node *n;

	if (!m->sqs)
		return NULL;
	for (n = m->sqs->h; n; n = n->next) {
		sql_var *v = n->data;

		if (strcmp(v->name, rname) == 0) {
			sql_exp *ne = exps_bind_column2(v->rel->exps, rname, name);

			if (ne)
				return v->rel;
		}
	}
	return NULL;
}

sql_exp *
mvc_find_subexp(mvc *m, const char *rname, const char *name) 
{
	node *n;

	if (!m->sqs)
		return NULL;
	for (n = m->sqs->h; n; n = n->next) {
		sql_var *v = n->data;

		if (strcmp(v->name, rname) == 0) {
			sql_exp *ne = exps_bind_column2(v->rel->exps, rname, name);

			if (ne)
				return ne;
		}
	}
	return NULL;
}
