/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

/* multi version catalog */

#include "monetdb_config.h"
#include "gdk.h"

#include "sql_mvc.h"
#include "sql_qc.h"
#include "sql_types.h"
#include "sql_env.h"
#include "sql_semantic.h"
#include "sql_partition.h"
#include "sql_privileges.h"
#include "mapi_querytype.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_semantic.h"
#include "rel_unnest.h"
#include "rel_optimizer.h"
#include "wlc.h"

#include "mal_authorize.h"

static void
sql_create_comments(mvc *m, sql_schema *s)
{
	sql_table *t = NULL;
	sql_column *c = NULL;
	sql_key *k = NULL;

	mvc_create_table(&t, m, s, "comments", tt_table, 1, SQL_PERSIST, 0, -1, 0);
	mvc_create_column_(&c, m, t, "id", "int", 32);
	sql_trans_create_ukey(&k, m->session->tr, t, "comments_id_pkey", pkey);
	sql_trans_create_kc(m->session->tr, k, c);
	sql_trans_key_done(m->session->tr, k);
	sql_trans_create_dependency(m->session->tr, c->base.id, k->idx->base.id, INDEX_DEPENDENCY);
	mvc_create_column_(&c, m, t, "remark", "varchar", 65000);
	sql_trans_alter_null(m->session->tr, c, 0);
}

sql_table *
mvc_init_create_view(mvc *m, sql_schema *s, const char *name, const char *query)
{
	sql_table *t = NULL;

	mvc_create_view(&t, m, s, name, SQL_PERSIST, query, 1);
	if (t) {
		char *buf;
		sql_rel *r = NULL;

		if (!(buf = sa_strdup(m->ta, t->query))) {
			(void) sql_error(m, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return NULL;
		}

		r = rel_parse(m, s, buf, m_deps);
		if (r)
			r = sql_processrelation(m, r, 0, 0, 0);
		if (r) {
			list *blist = rel_dependencies(m, r);
			if (mvc_create_dependencies(m, blist, t->base.id, VIEW_DEPENDENCY)) {
				sa_reset(m->ta);
				(void) sql_error(m, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				return NULL;
			}
		}
		sa_reset(m->ta);
		assert(r);
	}
	return t;
}

#define MVC_INIT_DROP_TABLE(SQLID, TNAME, VIEW, NCOL)			\
	do {								\
		str output;						\
		t = mvc_bind_table(m, s, TNAME);			\
		SQLID = t->base.id;					\
		for (int i = 0; i < NCOL; i++) {			\
			sql_column *col = mvc_bind_column(m, t, VIEW[i].name); \
			VIEW[i].oldid = col->base.id;			\
		}							\
		if ((output = mvc_drop_table(m, s, t, 0)) != MAL_SUCCEED) { \
			mvc_destroy(m);					\
			mvc_exit(store);	\
			TRC_CRITICAL(SQL_TRANS,				\
				     "Initialization error: %s\n", output);	\
			freeException(output);				\
			return NULL;					\
		}							\
	} while (0)

struct view_t {
	const char *name;
	const char *type;
	unsigned int digits;
	sqlid oldid;
	sqlid newid;
};

static void
mvc_fix_depend(mvc *m, sql_column *depids, struct view_t *v, int n)
{
	sqlstore *store = m->store;
	oid rid;
	rids *rs;

	for (int i = 0; i < n; i++) {
		rs = store->table_api.rids_select(m->session->tr, depids,
					     &v[i].oldid, &v[i].oldid, NULL);
		while ((rid = store->table_api.rids_next(rs)), !is_oid_nil(rid)) {
			store->table_api.column_update_value(m->session->tr, depids,
							rid, &v[i].newid);
		}
		store->table_api.rids_destroy(rs);
	}
}

sql_store
mvc_init(int debug, store_type store_tpe, int ro, int su)
{
	sqlstore *store = NULL;
	sql_schema *s;
	sql_table *t;
	mvc *m;
	str msg;

	TRC_DEBUG(SQL_TRANS, "Initialization\n");
	keyword_init();
	if (scanner_init_keywords() != 0) {
		TRC_CRITICAL(SQL_TRANS, "Malloc failure\n");
		return NULL;
	}

	if ((store = store_init(debug, store_tpe, ro, su)) == NULL) {
		keyword_exit();
		TRC_CRITICAL(SQL_TRANS, "Unable to create system tables\n");
		return NULL;
	}

	m = mvc_create((sql_store)store, store->sa, 0, 0, NULL, NULL);
	if (!m) {
		mvc_exit(store);
		TRC_CRITICAL(SQL_TRANS, "Malloc failure\n");
		return NULL;
	}

	assert(m->sa == NULL);
	m->sa = sa_create(m->pa);
	if (!m->sa) {
		mvc_destroy(m);
		mvc_exit(store);
		TRC_CRITICAL(SQL_TRANS, "Malloc failure\n");
		return NULL;
	}

	if (store->first || store->catalog_version) {
		sqlid tid = 0, cid = 0;
		struct view_t tview[10] = {
			{
				.name = "id",
				.type = "int",
				.digits = 32,
			},
			{
				.name = "name",
				.type = "varchar",
				.digits = 1024,
			},
			{
				.name = "schema_id",
				.type = "int",
				.digits = 32,
			},
			{
				.name = "query",
				.type = "varchar",
				.digits = 1 << 20,
			},
			{
				.name = "type",
				.type = "smallint",
				.digits = 16,
			},
			{
				.name = "system",
				.type = "boolean",
				.digits = 1,
			},
			{
				.name = "commit_action",
				.type = "smallint",
				.digits = 16,
			},
			{
				.name = "access",
				.type = "smallint",
				.digits = 16,
			},
			{
				.name = "temporary",
				.type = "smallint",
				.digits = 16,
			},
			{
				0
			},
		}, cview[11] = {
			{
				.name = "id",
				.type = "int",
				.digits = 32,
			},
			{
				.name = "name",
				.type = "varchar",
				.digits = 1024,
			},
			{
				.name = "type",
				.type = "varchar",
				.digits = 1024,
			},
			{
				.name = "type_digits",
				.type = "int",
				.digits = 32,
			},
			{
				.name = "type_scale",
				.type = "int",
				.digits = 32,
			},
			{
				.name = "table_id",
				.type = "int",
				.digits = 32,
			},
			{
				.name = "default",
				.type = "varchar",
				.digits = STORAGE_MAX_VALUE_LENGTH,
			},
			{
				.name = "null",
				.type = "boolean",
				.digits = 1,
			},
			{
				.name = "number",
				.type = "int",
				.digits = 32,
			},
			{
				.name = "storage",
				.type = "varchar",
				.digits = 2048,
			},
			{
				0
			},
		};
		if (mvc_trans(m) < 0) {
			mvc_destroy(m);
			mvc_exit(store);
			TRC_CRITICAL(SQL_TRANS, "Failed to start transaction\n");
			return NULL;
		}
		s = m->session->schema = mvc_bind_schema(m, "sys");
		assert(m->session->schema != NULL);

		if (!store->first) {
			MVC_INIT_DROP_TABLE(tid, "tables", tview, 9);
			MVC_INIT_DROP_TABLE(cid, "columns", cview, 10);
		}

		t = mvc_init_create_view(m, s, "tables", "SELECT \"id\", \"name\", \"schema_id\", \"query\", CAST(CASE WHEN \"system\" THEN \"type\" + 10 /* system table/view */ ELSE (CASE WHEN \"commit_action\" = 0 THEN \"type\" /* table/view */ ELSE \"type\" + 20 /* global temp table */ END) END AS SMALLINT) AS \"type\", \"system\", \"commit_action\", \"access\", CASE WHEN (NOT \"system\" AND \"commit_action\" > 0) THEN 1 ELSE 0 END AS \"temporary\" FROM \"sys\".\"_tables\" WHERE \"type\" <> 2 UNION ALL SELECT \"id\", \"name\", \"schema_id\", \"query\", CAST(\"type\" + 30 /* local temp table */ AS SMALLINT) AS \"type\", \"system\", \"commit_action\", \"access\", 1 AS \"temporary\" FROM \"tmp\".\"_tables\";");
		if (!t) {
			mvc_destroy(m);
			mvc_exit(store);
			TRC_CRITICAL(SQL_TRANS, "Failed to create 'tables' view\n");
			return NULL;
		}

		for (int i = 0; i < 9; i++) {
			sql_column *col = NULL;
			
			mvc_create_column_(&col, m, t, tview[i].name, tview[i].type, tview[i].digits);
			if (col == NULL) {
				mvc_destroy(m);
				mvc_exit(store);
				TRC_CRITICAL(SQL_TRANS,
					     "Initialization: creation of sys.tables column %s failed\n", tview[i].name);
				return NULL;
			}
			tview[i].newid = col->base.id;
		}

		if (!store->first) {
			int pub = ROLE_PUBLIC;
			int p = PRIV_SELECT;
			int zero = 0;
			sql_table *privs = find_sql_table(m->session->tr, s, "privileges");
			sql_table *deps = find_sql_table(m->session->tr, s, "dependencies");
			store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
			assert(tview[9].name == NULL);
			tview[9].oldid = tid;
			tview[9].newid = t->base.id;
			mvc_fix_depend(m, find_sql_column(deps, "id"), tview, 10);
			mvc_fix_depend(m, find_sql_column(deps, "depend_id"), tview, 10);
		}

		t = mvc_init_create_view(m, s, "columns", "SELECT * FROM (SELECT p.* FROM \"sys\".\"_columns\" AS p UNION ALL SELECT t.* FROM \"tmp\".\"_columns\" AS t) AS columns;");
		if (!t) {
			mvc_destroy(m);
			mvc_exit(store);
			TRC_CRITICAL(SQL_TRANS, "Failed to create 'columns' view\n");
			return NULL;
		}
		for (int i = 0; i < 10; i++) {
			sql_column *col = NULL;

			mvc_create_column_(&col, m, t, cview[i].name, cview[i].type, cview[i].digits);
			if (col == NULL) {
				mvc_destroy(m);
				mvc_exit(store);
				TRC_CRITICAL(SQL_TRANS,
					     "Initialization: creation of sys.tables column %s failed\n", cview[i].name);
				return NULL;
			}
			cview[i].newid = col->base.id;
		}

		if (!store->first) {
			int pub = ROLE_PUBLIC;
			int p = PRIV_SELECT;
			int zero = 0;
			sql_table *privs = find_sql_table(m->session->tr, s, "privileges");
			sql_table *deps = find_sql_table(m->session->tr, s, "dependencies");
			store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
			assert(cview[10].name == NULL);
			cview[10].oldid = cid;
			cview[10].newid = t->base.id;
			mvc_fix_depend(m, find_sql_column(deps, "id"), cview, 11);
			mvc_fix_depend(m, find_sql_column(deps, "depend_id"), cview, 11);
		} else {
			sql_create_env(m, s);
			sql_create_comments(m, s);
			sql_create_privileges(m, s);
		}

		if ((msg = mvc_commit(m, 0, NULL, false)) != MAL_SUCCEED) {
			TRC_CRITICAL(SQL_TRANS, "Unable to commit system tables: %s\n", (msg + 6));
			freeException(msg);
			mvc_destroy(m);
			mvc_exit(store);
			return NULL;
		}
	}

	if (mvc_trans(m) < 0) {
		mvc_destroy(m);
		mvc_exit(store);
		TRC_CRITICAL(SQL_TRANS, "Failed to start transaction\n");
		return NULL;
	}

	//as the sql_parser is not yet initialized in the storage, we determine the sql type of the sql_parts here

	struct os_iter si;
	os_iterator(&si, m->session->tr->cat->schemas, m->session->tr, NULL);
	for(sql_base *b = oi_next(&si); b; b = oi_next(&si)) {
		sql_schema *ss = (sql_schema*)b;
		struct os_iter oi;
		os_iterator(&oi, ss->tables, m->session->tr, NULL);
		for(sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
			sql_table *tt = (sql_table*)b;
			if (isPartitionedByColumnTable(tt) || isPartitionedByExpressionTable(tt)) {
				char *err;
				if ((err = initialize_sql_parts(m, tt)) != NULL) {
					TRC_CRITICAL(SQL_TRANS, "Unable to start partitioned table: %s.%s: %s\n", ss->base.name, tt->base.name, err);
					freeException(err);
					mvc_destroy(m);
					mvc_exit(store);
					return NULL;
				}
			}
		}
	}

	if ((msg = mvc_commit(m, 0, NULL, false)) != MAL_SUCCEED) {
		TRC_CRITICAL(SQL_TRANS, "Unable to commit system tables: %s\n", (msg + 6));
		freeException(msg);
		mvc_destroy(m);
		mvc_exit(store);
		return NULL;
	}

	mvc_destroy(m);
	return store;
}

void
mvc_exit(sql_store store)
{
	TRC_DEBUG(SQL_TRANS, "MVC exit\n");
	store_exit(store);
	keyword_exit();
}

void
mvc_logmanager(sql_store store)
{
	store_manager(store);
}

int
mvc_status(mvc *m)
{
	int res = m->session->status;

	return res;
}

int
mvc_error_retry(mvc *m)
{
	int res = m->session->status;

	if (!res || res == -ERR_AMBIGUOUS || res == -ERR_GROUPBY)
		return 0;
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
mvc_cancel_session(mvc *m)
{
	(void)sql_trans_end(m->session, SQL_ERR);
}

int
mvc_trans(mvc *m)
{
	int res = 0, err = m->session->status;
	assert(!m->session->tr->active);	/* can only start a new transaction */

	TRC_INFO(SQL_TRANS, "Starting transaction\n");
	res = sql_trans_begin(m->session);
	if (m->qc && (res || err)) {
		int seqnr = m->qc->id;
		if (m->qc)
			qc_destroy(m->qc);
		/* TODO Change into recreate all */
		if (!(m->qc = qc_create(m->pa, m->clientid, seqnr))) {
			if (m->session->tr->active)
				(void)sql_trans_end(m->session, SQL_ERR);
			return -1;
		}
	}
	return res;
}

str
mvc_commit(mvc *m, int chain, const char *name, bool enabling_auto_commit)
{
	sql_trans *tr = m->session->tr;
	int ok = SQL_OK;
	str msg = MAL_SUCCEED, other;
	char operation[BUFSIZ];

	assert(tr);
	assert(m->session->tr->active);	/* only commit an active transaction */
	TRC_DEBUG(SQL_TRANS,"Commit: %s\n", (name) ? name : "");
	if(enabling_auto_commit)
		snprintf(operation, BUFSIZ, "Commit failed while enabling auto_commit: ");
	else if(name)
		snprintf(operation, BUFSIZ, "SAVEPOINT: (%s)", name);
	else
		snprintf(operation, BUFSIZ, "COMMIT:");

	if (m->session->status < 0) {
		msg = createException(SQL, "sql.commit", SQLSTATE(40000) "%s transaction is aborted, will ROLLBACK instead", operation);
		if ((other = mvc_rollback(m, chain, name, false)) != MAL_SUCCEED)
			freeException(other);
		return msg;
	}

	/* savepoint, simply make a new sub transaction */
	if (name && name[0] != '\0') {
		sql_trans *tr = m->session->tr;
		TRC_DEBUG(SQL_TRANS, "Savepoint\n");
		if (!(m->session->tr = sql_trans_create(m->store, tr, name)))
			return createException(SQL, "sql.commit", SQLSTATE(HY013) "%s allocation failure while creating savepoint", operation);

		if (!(m->session->schema = find_sql_schema(m->session->tr, m->session->schema_name))) {
			m->session->tr = sql_trans_destroy(m->session->tr);
			return createException(SQL, "sql.commit", SQLSTATE(40000) "%s finished successfully, but the session's schema could not be found on the current transaction", operation);
		}
		m->type = Q_TRANS;
		TRC_INFO(SQL_TRANS, "Savepoint commit '%s' done\n", name);
		return msg;
	}

	if (!tr->parent && !name) {
		switch (sql_trans_end(m->session, ok)) {
			case SQL_ERR:
				GDKfatal("%s transaction commit failed; exiting (kernel error: %s)", operation, GDKerrbuf);
				break;
			case SQL_CONFLICT:
				/* transaction conflict */
				return createException(SQL, "sql.commit", SQLSTATE(40001) "%s transaction is aborted because of concurrency conflicts, will ROLLBACK instead", operation);
			default:
				break;
		}
		msg = WLCcommit(m->clientid);
		if (msg != MAL_SUCCEED) {
			if ((other = mvc_rollback(m, chain, name, false)) != MAL_SUCCEED)
				freeException(other);
			return msg;
		}
		if (chain) {
			if (sql_trans_begin(m->session) < 0)
				return createException(SQL, "sql.commit", SQLSTATE(40000) "%s finished successfully, but the session's schema could not be found while starting the next transaction", operation);
			m->session->auto_commit = 0; /* disable auto-commit while chaining */
		}
		m->type = Q_TRANS;
		TRC_INFO(SQL_TRANS,
			"Commit done\n");
		return msg;
	}

	/* save points only */
	assert(name || tr->parent);

	/* commit and cleanup nested transactions */
	if (tr->parent) {
		while (tr->parent != NULL && ok == SQL_OK) {
			if ((ok = sql_trans_commit(tr)) == SQL_ERR)
				GDKfatal("%s transaction commit failed; exiting (kernel error: %s)", operation, GDKerrbuf);
			m->session->tr = tr = sql_trans_destroy(tr);
		}
		while (tr->parent != NULL)
			m->session->tr = tr = sql_trans_destroy(tr);
		if (ok != SQL_OK)
			msg = createException(SQL, "sql.commit", SQLSTATE(40001) "%s transaction is aborted because of concurrency conflicts, will ROLLBACK instead", operation);
	}

	/* if there is nothing to commit reuse the current transaction */
	if (list_empty(tr->changes)) {
		if (!chain) {
			switch (sql_trans_end(m->session, ok)) {
				case SQL_ERR:
					GDKfatal("%s transaction commit failed; exiting (kernel error: %s)", operation, GDKerrbuf);
					break;
				case SQL_CONFLICT:
					if (!msg)
						msg = createException(SQL, "sql.commit", SQLSTATE(40001) "%s transaction is aborted because of concurrency conflicts, will ROLLBACK instead", operation);
					break;
				default:
					break;
			}
		}
		m->type = Q_TRANS;
		TRC_INFO(SQL_TRANS,
			"Commit done (no changes)\n");
		return msg;
	}

	switch (sql_trans_end(m->session, ok)) {
		case SQL_ERR:
			GDKfatal("%s transaction commit failed; exiting (kernel error: %s)", operation, GDKerrbuf);
			break;
		case SQL_CONFLICT:
			if (!msg)
				msg = createException(SQL, "sql.commit", SQLSTATE(40001) "%s transaction is aborted because of concurrency conflicts, will ROLLBACK instead", operation);
			return msg;
		default:
			break;
	}
	if (chain) {
		if (sql_trans_begin(m->session) < 0) {
			if (!msg)
				msg = createException(SQL, "sql.commit", SQLSTATE(40000) "%s finished successfully, but the session's schema could not be found while starting the next transaction", operation);
			return msg;
		}
		m->session->auto_commit = 0; /* disable auto-commit while chaining */
	}
	m->type = Q_TRANS;
	TRC_INFO(SQL_TRANS,
		"Commit done\n");
	return msg;
}

str
mvc_rollback(mvc *m, int chain, const char *name, bool disabling_auto_commit)
{
	str msg = MAL_SUCCEED;

	TRC_DEBUG(SQL_TRANS, "Rollback: %s\n", (name) ? name : "");
	(void) disabling_auto_commit;

	sql_trans *tr = m->session->tr;
	assert(m->session->tr && m->session->tr->active);	/* only abort an active transaction */
	if (name && name[0] != '\0') {
		while (tr && (!tr->name || strcmp(tr->name, name) != 0))
			tr = tr->parent;
		if (!tr || !tr->name || strcmp(tr->name, name) != 0) {
			msg = createException(SQL, "sql.rollback", SQLSTATE(42000) "ROLLBACK TO SAVEPOINT: no such savepoint: '%s'", name);
			m->session->status = -1;
			return msg;
		}
		tr = m->session->tr;
		while (!tr->name || strcmp(tr->name, name) != 0) {
			/* make sure we do not reuse changed data */
			if (!list_empty(tr->changes))
				tr->status = 1;
			m->session->tr = tr = sql_trans_destroy(tr);
		}
		/* start a new transaction after rolling back */
		if (!(m->session->tr = tr = sql_trans_create(m->store, tr, name))) {
			msg = createException(SQL, "sql.rollback", SQLSTATE(HY013) "ROLLBACK TO SAVEPOINT: allocation failure while restarting savepoint");
			m->session->status = -1;
			return msg;
		}
		m->session->status = tr->parent->status;
		if (!(m->session->schema = find_sql_schema(tr, m->session->schema_name))) {
			msg = createException(SQL, "sql.rollback", SQLSTATE(40000) "ROLLBACK TO SAVEPOINT: finished successfully, but the session's schema could not be found on the current transaction");
			m->session->status = -1;
			return msg;
		}
	} else {
		/* first release all intermediate savepoints */
		while (tr->parent != NULL)
			m->session-> tr = tr = sql_trans_destroy(tr);
		/* make sure we do not reuse changed data */
		if (!list_empty(tr->changes))
			tr->status = 1;
		(void)sql_trans_end(m->session, SQL_ERR);
		if (chain) {
			if (sql_trans_begin(m->session) < 0) {
				msg = createException(SQL, "sql.rollback", SQLSTATE(40000) "ROLLBACK: finished successfully, but the session's schema could not be found while starting the next transaction");
				m->session->status = -1;
				return msg;
			}
			m->session->auto_commit = 0; /* disable auto-commit while chaining */
		}
	}
	if (msg == MAL_SUCCEED)
		msg = WLCrollback(m->clientid);
	if (msg != MAL_SUCCEED) {
		m->session->status = -1;
		return msg;
	}
	m->type = Q_TRANS;
	TRC_INFO(SQL_TRANS,
		"Commit%s%s rolled back%s\n",
		name ? " " : "", name ? name : "",
		list_empty(tr->changes) ? " (no changes)" : "");
	return msg;
}

/* release all savepoints up including the given named savepoint
 * but keep the current changes.
 * */
str
mvc_release(mvc *m, const char *name)
{
	sql_trans *tr = m->session->tr;
	str msg = MAL_SUCCEED;

	assert(tr && tr->active);	/* only release active transactions */

	TRC_DEBUG(SQL_TRANS, "Release: %s\n", (name) ? name : "");

	if (!name && (msg = mvc_rollback(m, 0, name, false)) != MAL_SUCCEED) {
		m->session->status = -1;
		return msg;
	}

	while (tr && (!tr->name || strcmp(tr->name, name) != 0))
		tr = tr->parent;
	if (!tr || !tr->name || strcmp(tr->name, name) != 0) {
		msg = createException(SQL, "sql.release", SQLSTATE(42000) "RELEASE: no such savepoint: '%s'", name);
		m->session->status = -1;
		return msg;
	}
	tr = m->session->tr;
	while (!tr->name || strcmp(tr->name, name) != 0) {
		/* commit all intermediate savepoints */
		if (sql_trans_commit(tr) != SQL_OK)
			GDKfatal("release savepoints should not fail");
		m->session->tr = tr = sql_trans_destroy(tr);
	}
	_DELETE(m->session->tr->name); /* name will no longer be used */
	m->session->status = tr->status;
	if (!(m->session->schema = find_sql_schema(m->session->tr, m->session->schema_name))) {
		msg = createException(SQL, "sql.release", SQLSTATE(40000) "RELEASE: finished successfully, but the session's schema could not be found on the current transaction");
		m->session->status = -1;
		return msg;
	}

	m->type = Q_TRANS;
	return msg;
}

static void
_free(void *dummy, void *data)
{
	(void)dummy;
	GDKfree(data);
}

mvc *
mvc_create(sql_store *store, sql_allocator *pa, int clientid, int debug, bstream *rs, stream *ws)
{
	mvc *m;
	str sys_str = NULL;

	assert(pa);
 	m = SA_ZNEW(pa, mvc);
	if (!m)
		return NULL;

	TRC_DEBUG(SQL_TRANS, "MVC create\n");

	m->errstr[0] = '\0';
	/* if an error exceeds the buffer we don't want garbage at the end */
	m->errstr[ERRSIZE-1] = '\0';

	m->qc = qc_create(pa, clientid, 0);
	if (!m->qc) {
		return NULL;
	}
	m->pa = pa;
	m->sa = NULL;
	m->ta = sa_create(m->pa);
	m->sp = (uintptr_t)(&m);

	m->params = NULL;
	m->sizeframes = MAXPARAMS;
	m->frames = SA_NEW_ARRAY(pa, sql_frame*, m->sizeframes);
	m->topframes = 0;
	m->frame = 0;

	m->use_views = 0;
	if (!m->frames) {
		qc_destroy(m->qc);
		return NULL;
	}
	if (init_global_variables(m) < 0) {
		qc_destroy(m->qc);
		list_destroy(m->global_vars);
		return NULL;
	}
	m->sym = NULL;

	m->role_id = m->user_id = -1;
	m->timezone = 0;
	m->clientid = clientid;

	m->emode = m_normal;
	m->emod = mod_none;
	m->reply_size = 100;
	m->debug = debug;

	m->label = 0;
	m->cascade_action = NULL;

	if (!(m->schema_path = list_create((fdestroy)_free))) {
		qc_destroy(m->qc);
		list_destroy(m->global_vars);
		return NULL;
	}
	if (!(sys_str = _STRDUP("sys")) || !list_append(m->schema_path, sys_str)) {
		_DELETE(sys_str);
		qc_destroy(m->qc);
		list_destroy(m->global_vars);
		list_destroy(m->schema_path);
		return NULL;
	}
	m->schema_path_has_sys = 1;
	m->schema_path_has_tmp = 0;
	m->store = store;

	m->session = sql_session_create(m->store, m->pa, 1 /*autocommit on*/);
	if (!m->session) {
		qc_destroy(m->qc);
		list_destroy(m->global_vars);
		list_destroy(m->schema_path);
		return NULL;
	}

	m->type = Q_PARSE;

	scanner_init(&m->scanner, rs, ws);
	return m;
}

int
mvc_reset(mvc *m, bstream *rs, stream *ws, int debug)
{
	int res = 1, reset;
	sql_trans *tr;

	TRC_DEBUG(SQL_TRANS, "MVC reset\n");
	tr = m->session->tr;
	if (tr && tr->parent) {
		assert(m->session->tr->active == 0);
		while (tr->parent->parent != NULL)
			m->session->tr = tr = sql_trans_destroy(tr);
	}
	reset = sql_session_reset(m->session, 1 /*autocommit on*/);
	if (tr && !reset)
		res = 0;

	if (m->sa)
		m->sa = sa_reset(m->sa);
	else
		m->sa = sa_create(m->pa);
	if(!m->sa)
		res = 0;
	m->ta = sa_reset(m->ta);

	m->errstr[0] = '\0';

	m->params = NULL;
	/* reset frames to the set of global variables */
	stack_pop_until(m, 0);
	m->frame = 0;
	m->sym = NULL;

	m->role_id = m->user_id = -1;
	m->emode = m_normal;
	m->emod = mod_none;
	if (m->reply_size != 100)
		sqlvar_set_number(find_global_var(m, mvc_bind_schema(m, "sys"), "reply_size"), 100);
	m->reply_size = 100;
	if (m->timezone != 0)
		sqlvar_set_number(find_global_var(m, mvc_bind_schema(m, "sys"), "current_timezone"), 0);
	m->timezone = 0;
	if (m->debug != debug)
		sqlvar_set_number(find_global_var(m, mvc_bind_schema(m, "sys"), "debug"), debug);
	m->debug = debug;

	m->label = 0;
	m->cascade_action = NULL;
	m->type = Q_PARSE;

	scanner_init(&m->scanner, rs, ws);
	return res;
}

void
mvc_destroy(mvc *m)
{
	sql_trans *tr;

	TRC_DEBUG(SQL_TRANS, "MVC destroy\n");
	tr = m->session->tr;
	if (tr) {
		if (m->session->tr->active)
			(void)sql_trans_end(m->session, SQL_ERR);
		while (tr->parent)
			m->session->tr = tr = sql_trans_destroy(tr);
	}
	sql_session_destroy(m->session);

	list_destroy(m->global_vars);
	list_destroy(m->schema_path);
	stack_pop_until(m, 0);

	if (m->scanner.log) /* close and destroy stream */
		close_stream(m->scanner.log);

	m->sa = NULL;
	m->ta = NULL;
	if (m->qc)
		qc_destroy(m->qc);
	m->qc = NULL;
}

sql_type *
mvc_bind_type(mvc *sql, const char *name)
{
	sql_type *t = sql_trans_bind_type(sql->session->tr, NULL, name);
	TRC_DEBUG(SQL_TRANS, "Bind type: %s\n", name);
	return t;
}

sql_type *
schema_bind_type(mvc *sql, sql_schema *s, const char *name)
{
	sql_type *t = find_sql_type(sql->session->tr, s, name);

	(void) sql;
	if (!t)
		return NULL;
	TRC_DEBUG(SQL_TRANS, "Schema bind type: %s\n", name);
	return t;
}

sql_schema *
mvc_bind_schema(mvc *m, const char *sname)
{
	sql_trans *tr = m->session->tr;
	sql_schema *s;

	if (!tr)
		return NULL;

 	s = find_sql_schema(tr, sname);
	if (!s)
		return NULL;
	TRC_DEBUG(SQL_TRANS, "Bind schema: %s\n", sname);
	return s;
}

sql_table *
mvc_bind_table(mvc *m, sql_schema *s, const char *tname)
{
	sql_table *t = find_sql_table(m->session->tr, s, tname);

	(void) m;
	if (!t)
		return NULL;
	TRC_DEBUG(SQL_TRANS, "Bind table: %s.%s\n", s->base.name, tname);
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
	TRC_DEBUG(SQL_TRANS, "Bind column: %s.%s\n", t->base.name, cname);
	return c;
}

static sql_column *
first_column(sql_table *t)
{
	node *n = ol_first_node(t->columns);

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
	TRC_DEBUG(SQL_TRANS, "First column: %s.%s\n", t->base.name, c->base.name);
	return c;
}

sql_key *
mvc_bind_key(mvc *m, sql_schema *s, const char *kname)
{
	sql_base *b = os_find_name(s->keys, m->session->tr, kname);
	sql_key *k = (sql_key*)b;

	if (!b)
		return NULL;
	TRC_DEBUG(SQL_TRANS, "Bind key: %s.%s\n", s->base.name, kname);
	return k;
}

sql_idx *
mvc_bind_idx(mvc *m, sql_schema *s, const char *iname)
{
	sql_base *b = os_find_name(s->idxs, m->session->tr, iname);

	if (!b)
		return NULL;
	sql_idx *i = (sql_idx*)b;
	TRC_DEBUG(SQL_TRANS, "Bind index: %s.%s\n", s->base.name, iname);
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

	if (ol_length(t->keys))
		for (cur = ol_first_node(t->keys); cur; cur = cur->next) {
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
	sql_base *b = os_find_name(s->triggers, m->session->tr, tname);

	if (!b)
		return NULL;
	sql_trigger *trigger = (sql_trigger*)b;
	TRC_DEBUG(SQL_TRANS, "Bind trigger: %s.%s\n", s->base.name, tname);
	return trigger;
}

int
mvc_create_type(mvc *sql, sql_schema *s, const char *name, unsigned int digits, unsigned int scale, int radix, const char *impl)
{
	TRC_DEBUG(SQL_TRANS, "Create type: %s\n", name);
	return sql_trans_create_type(sql->session->tr, s, name, digits, scale, radix, impl);
}

int
mvc_drop_type(mvc *m, sql_schema *s, sql_type *t, int drop_action)
{
	TRC_DEBUG(SQL_TRANS, "Drop type: %s %s\n", s->base.name, t->base.name);
	if (t)
		return sql_trans_drop_type(m->session->tr, s, t->base.id, drop_action ? DROP_CASCADE_START : DROP_RESTRICT);
	return 0;
}

int
mvc_create_func(sql_func **f, mvc *m, sql_allocator *sa, sql_schema *s, const char *name, list *args, list *res, sql_ftype type, sql_flang lang,
				const char *mod, const char *impl, const char *query, bit varres, bit vararg, bit system, bit side_effect)
{
	int lres = LOG_OK;

	TRC_DEBUG(SQL_TRANS, "Create function: %s\n", name);
	if (sa) {
		*f = create_sql_func(m->store, sa, name, args, res, type, lang, mod, impl, query, varres, vararg, system, side_effect);
		(*f)->s = s;
	} else
		lres = sql_trans_create_func(f, m->session->tr, s, name, args, res, type, lang, mod, impl, query, varres, vararg, system, side_effect);
	return lres;
}

int
mvc_drop_func(mvc *m, sql_schema *s, sql_func *f, int drop_action)
{
	TRC_DEBUG(SQL_TRANS, "Drop function: %s %s\n", s->base.name, f->base.name);
	return sql_trans_drop_func(m->session->tr, s, f->base.id, drop_action ? DROP_CASCADE_START : DROP_RESTRICT);
}

int
mvc_drop_all_func(mvc *m, sql_schema *s, list *list_func, int drop_action)
{
	TRC_DEBUG(SQL_TRANS, "Drop all functions: %s %s\n", s->base.name, ((sql_func *) list_func->h->data)->base.name);
	return sql_trans_drop_all_func(m->session->tr, s, list_func, drop_action ? DROP_CASCADE_START : DROP_RESTRICT);
}

int
mvc_create_schema(mvc *m, const char *name, sqlid auth_id, sqlid owner)
{
	TRC_DEBUG(SQL_TRANS, "Create schema: %s %d %d\n", name, auth_id, owner);
	return sql_trans_create_schema(m->session->tr, name, auth_id, owner);
}

int
mvc_drop_schema(mvc *m, sql_schema * s, int drop_action)
{
	TRC_DEBUG(SQL_TRANS, "Drop schema: %s\n", s->base.name);
	return sql_trans_drop_schema(m->session->tr, s->base.id, drop_action ? DROP_CASCADE_START : DROP_RESTRICT);
}

int
mvc_create_ukey(sql_key **kres, mvc *m, sql_table *t, const char *name, key_type kt)
{
	int res = LOG_OK;

	TRC_DEBUG(SQL_TRANS, "Create ukey: %s %u\n", t->base.name, (unsigned) kt);
	if (t->persistence == SQL_DECLARED_TABLE)
		*kres = create_sql_ukey(m->store, m->sa, t, name, kt);
	else
		res = sql_trans_create_ukey(kres, m->session->tr, t, name, kt);
	return res;
}

int
mvc_create_key_done(mvc *m, sql_key *k)
{
	int res = LOG_OK;

	if (k->t->persistence == SQL_DECLARED_TABLE)
		key_create_done(m->session->tr, m->sa, k);
	else
		res = sql_trans_key_done(m->session->tr, k);
	return res;
}

int
mvc_create_fkey(sql_fkey **kres, mvc *m, sql_table *t, const char *name, key_type kt, sql_key *rkey, int on_delete, int on_update)
{
	int res = LOG_OK;

	TRC_DEBUG(SQL_TRANS, "Create fkey: %s %u %p\n", t->base.name, (unsigned) kt, rkey);
	if (t->persistence == SQL_DECLARED_TABLE)
		*kres = create_sql_fkey(m->store, m->sa, t, name, kt, rkey, on_delete, on_update);
	else
		res = sql_trans_create_fkey(kres, m->session->tr, t, name, kt, rkey, on_delete, on_update);
	return res;
}

int
mvc_create_kc(mvc *m, sql_key *k, sql_column *c)
{
	int res = LOG_OK;

	if (k->t->persistence == SQL_DECLARED_TABLE)
		create_sql_kc(m->store, m->sa, k, c);
	else
		res = sql_trans_create_kc(m->session->tr, k, c);
	return res;
}

int
mvc_create_fkc(mvc *m, sql_fkey *fk, sql_column *c)
{
	int res = LOG_OK;
	sql_key *k = (sql_key*)fk;

	if (k->t->persistence == SQL_DECLARED_TABLE)
		create_sql_kc(m->store, m->sa, k, c);
	else
		res = sql_trans_create_fkc(m->session->tr, fk, c);
	return res;
}

int
mvc_drop_key(mvc *m, sql_schema *s, sql_key *k, int drop_action)
{
	TRC_DEBUG(SQL_TRANS, "Drop key: %s %s\n", s->base.name, k->base.name);
	if (k->t->persistence == SQL_DECLARED_TABLE) {
		drop_sql_key(k->t, k->base.id, drop_action);
		return 0;
	} else
		return sql_trans_drop_key(m->session->tr, s, k->base.id, drop_action ? DROP_CASCADE_START : DROP_RESTRICT);
}

int
mvc_create_idx(sql_idx **i, mvc *m, sql_table *t, const char *name, idx_type it)
{
	int res = LOG_OK;

	TRC_DEBUG(SQL_TRANS, "Create index: %s %u\n", t->base.name, (unsigned) it);
	if (t->persistence == SQL_DECLARED_TABLE)
		/* declared tables should not end up in the catalog */
		*i = create_sql_idx(m->store, m->sa, t, name, it);
	else
		res = sql_trans_create_idx(i, m->session->tr, t, name, it);
	return res;
}

int
mvc_create_ic(mvc *m, sql_idx *i, sql_column *c)
{
	int res = LOG_OK;

	if (i->t->persistence == SQL_DECLARED_TABLE)
		/* declared tables should not end up in the catalog */
		create_sql_ic(m->store, m->sa, i, c);
	else
		res = sql_trans_create_ic(m->session->tr, i, c);
	return res;
}

int
mvc_create_idx_done(mvc *m, sql_idx *i)
{
	int res = LOG_OK;

	(void) m;
	(void) create_sql_idx_done(m->session->tr, i);
	return res;
}

int
mvc_drop_idx(mvc *m, sql_schema *s, sql_idx *i)
{
	TRC_DEBUG(SQL_TRANS, "Drop index: %s %s\n", s->base.name, i->base.name);
	if (i->t->persistence == SQL_DECLARED_TABLE) {
		/* declared tables should not end up in the catalog */
		drop_sql_idx(i->t, i->base.id);
		return 0;
	} else
		return sql_trans_drop_idx(m->session->tr, s, i->base.id, DROP_RESTRICT);
}

int
mvc_create_trigger(sql_trigger **tri, mvc *m, sql_table *t, const char *name, sht time, sht orientation, sht event, const char *old_name,
				   const char *new_name, const char *condition, const char *statement)
{
	TRC_DEBUG(SQL_TRANS, "Create trigger: %s %d %d %d\n", t->base.name, time, orientation, event);
	return sql_trans_create_trigger(tri, m->session->tr, t, name, time, orientation, event, old_name, new_name, condition, statement);
}

int
mvc_drop_trigger(mvc *m, sql_schema *s, sql_trigger *tri)
{
	TRC_DEBUG(SQL_TRANS, "Drop trigger: %s %s\n", s->base.name, tri->base.name);
	return sql_trans_drop_trigger(m->session->tr, s, tri->base.id, DROP_RESTRICT);
}

int
mvc_create_table(sql_table **t, mvc *m, sql_schema *s, const char *name, int tt, bit system, int persistence, int commit_action, int sz, bit properties)
{
	char *err = NULL;
	int res = LOG_OK;

	assert(s);
	TRC_DEBUG(SQL_TRANS, "Create table: %s %s %d %d %d %d %d\n", s->base.name, name, tt, system, persistence, commit_action, (int)properties);
	if (persistence == SQL_DECLARED_TABLE) {
		*t = create_sql_table(m->store, m->sa, name, tt, system, persistence, commit_action, properties);
		(*t)->s = s;
	} else {
		res = sql_trans_create_table(t, m->session->tr, s, name, NULL, tt, system, persistence, commit_action, sz, properties);
		if (res == LOG_OK && isPartitionedByExpressionTable(*t) && (err = bootstrap_partition_expression(m, *t, 1))) {
			(void) sql_error(m, 02, "%s", err);
			return -5;
		}
		if (res == LOG_OK)
			res = sql_trans_set_partition_table(m->session->tr, *t);
	}
	return res;
}

int
mvc_create_view(sql_table **t, mvc *m, sql_schema *s, const char *name, int persistence, const char *sql, bit system)
{
	int res = LOG_OK;

	TRC_DEBUG(SQL_TRANS, "Create view: %s %s %s\n", s->base.name, name, sql);
	if (persistence == SQL_DECLARED_TABLE) {
		*t = create_sql_table(m->store, m->sa, name, tt_view, system, persistence, 0, 0);
		(*t)->s = s;
		(*t)->query = sa_strdup(m->sa, sql);
	} else {
		res = sql_trans_create_table(t, m->session->tr, s, name, sql, tt_view, system, SQL_PERSIST, 0, 0, 0);
	}
	return res;
}

int
mvc_create_remote(sql_table **t, mvc *m, sql_schema *s, const char *name, int persistence, const char *loc)
{
	int res = LOG_OK;

	TRC_DEBUG(SQL_TRANS, "Create remote: %s %s %s\n", s->base.name, name, loc);
	if (persistence == SQL_DECLARED_TABLE) {
		*t = create_sql_table(m->store, m->sa, name, tt_remote, 0, persistence, 0, 0);
		(*t)->s = s;
		(*t)->query = sa_strdup(m->sa, loc);
	} else {
		res = sql_trans_create_table(t, m->session->tr, s, name, loc, tt_remote, 0, SQL_REMOTE, 0, 0, 0);
	}
	return res;
}

str
mvc_drop_table(mvc *m, sql_schema *s, sql_table *t, int drop_action)
{
	TRC_DEBUG(SQL_TRANS, "Drop table: %s %s\n", s->base.name, t->base.name);

	if (isRemote(t)) {
		str AUTHres;

		char *qualified_name = sa_strconcat(m->ta, sa_strconcat(m->ta, t->s->base.name, "."), t->base.name);
		if (!qualified_name)
			throw(SQL, "sql.mvc_drop_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		AUTHres = AUTHdeleteRemoteTableCredentials(qualified_name);
		sa_reset(m->ta);
		if(AUTHres != MAL_SUCCEED)
			return AUTHres;
	}

	switch (sql_trans_drop_table(m->session->tr, s, t->base.name, drop_action ? DROP_CASCADE_START : DROP_RESTRICT)) {
		case -1:
			throw(SQL,"sql.mvc_drop_table",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		case -2:
		case -3:
			throw(SQL, "sql.mvc_drop_table", SQLSTATE(42000) "Transaction conflict while dropping table %s.%s", s->base.name, t->base.name);
		default:
			break;
	}
	return MAL_SUCCEED;
}

BUN
mvc_clear_table(mvc *m, sql_table *t)
{
	return sql_trans_clear_table(m->session->tr, t);
}

int
mvc_create_column_(sql_column **col, mvc *m, sql_table *t, const char *name, const char *type, unsigned int digits)
{
	sql_subtype tpe;

	if (!sql_find_subtype(&tpe, type, digits, 0))
		return -1;

	return sql_trans_create_column(col, m->session->tr, t, name, &tpe);
}

int
mvc_create_column(sql_column **col, mvc *m, sql_table *t, const char *name, sql_subtype *tpe)
{
	int res = LOG_OK;

	TRC_DEBUG(SQL_TRANS, "Create column: %s %s %s\n", t->base.name, name, tpe->type->base.name);
	if (t->persistence == SQL_DECLARED_TABLE)
		/* declared tables should not end up in the catalog */
		*col = create_sql_column(m->store, m->sa, t, name, tpe);
	else
		res = sql_trans_create_column(col, m->session->tr, t, name, tpe);
	return res;
}

int
mvc_drop_column(mvc *m, sql_table *t, sql_column *col, int drop_action)
{
	TRC_DEBUG(SQL_TRANS, "Drop column: %s %s\n", t->base.name, col->base.name);
	if (col->t->persistence == SQL_DECLARED_TABLE) {
		drop_sql_column(t, col->base.id, drop_action);
		return 0;
	} else
		return sql_trans_drop_column(m->session->tr, t, col->base.id, drop_action ? DROP_CASCADE_START : DROP_RESTRICT);
}

int
mvc_create_dependency(mvc *m, sql_base *b, sqlid depend_id, sql_dependency depend_type)
{
	int res = LOG_OK;

	TRC_DEBUG(SQL_TRANS, "Create dependency: %d %d %d\n", b->id, depend_id, (int) depend_type);
	if ( (b->id != depend_id) || (depend_type == BEDROPPED_DEPENDENCY) ) {
		if (!b->new)
			res = sql_trans_add_dependency(m->session->tr, b->id, ddl);
		if (res == LOG_OK)
			res = sql_trans_create_dependency(m->session->tr, b->id, depend_id, depend_type);
	}
	return res;
}

int
mvc_create_dependencies(mvc *m, list *blist, sqlid depend_id, sql_dependency dep_type)
{
	int res = LOG_OK;

	TRC_DEBUG(SQL_TRANS, "Create dependencies on '%d' of type: %d\n", depend_id, (int) dep_type);
	if (!list_empty(blist)) {
		for (node *n = blist->h ; n && res == LOG_OK ; n = n->next) {
			sql_base *b = n->data;
			if (!b->new) /* only add old objects to the transaction dependency list */
				res = sql_trans_add_dependency(m->session->tr, b->id, ddl);
			if (res == LOG_OK)
				res = mvc_create_dependency(m, b, depend_id, dep_type);
		}
	}
	return res;
}

int
mvc_check_dependency(mvc *m, sqlid id, sql_dependency type, list *ignore_ids)
{
	list *dep_list = NULL;

	TRC_DEBUG(SQL_TRANS, "Check dependency on: %d\n", id);
	switch (type) {
		case OWNER_DEPENDENCY:
			dep_list = sql_trans_owner_schema_dependencies(m->session->tr, id);
			break;
		case SCHEMA_DEPENDENCY:
			dep_list = sql_trans_schema_user_dependencies(m->session->tr, id);
			break;
		case TABLE_DEPENDENCY:
			dep_list = sql_trans_get_dependencies(m->session->tr, id, TABLE_DEPENDENCY, NULL);
			break;
		case VIEW_DEPENDENCY:
			dep_list = sql_trans_get_dependencies(m->session->tr, id, TABLE_DEPENDENCY, NULL);
			break;
		case FUNC_DEPENDENCY:
		case PROC_DEPENDENCY:
			dep_list = sql_trans_get_dependencies(m->session->tr, id, FUNC_DEPENDENCY, ignore_ids);
			break;
		default:
			dep_list =  sql_trans_get_dependencies(m->session->tr, id, COLUMN_DEPENDENCY, NULL);
	}

	if (!dep_list)
		return DEPENDENCY_CHECK_ERROR;

	if (list_length(dep_list) >= 2) {
		list_destroy(dep_list);
		return HAS_DEPENDENCY;
	}

	list_destroy(dep_list);
	return NO_DEPENDENCY;
}

int
mvc_null(mvc *m, sql_column *col, int isnull)
{
	TRC_DEBUG(SQL_TRANS, "Null: %s %d\n", col->base.name, isnull);
	if (col->t->persistence == SQL_DECLARED_TABLE) {
		col->null = isnull;
		return 0;
	}
	return sql_trans_alter_null(m->session->tr, col, isnull);
}

int
mvc_default(mvc *m, sql_column *col, char *val)
{
	TRC_DEBUG(SQL_TRANS, "Default: %s %s\n", col->base.name, val);
	if (col->t->persistence == SQL_DECLARED_TABLE) {
		col->def = val?sa_strdup(m->sa, val):NULL;
		return 0;
	} else {
		return sql_trans_alter_default(m->session->tr, col, val);
	}
}

int
mvc_drop_default(mvc *m, sql_column *col)
{
	TRC_DEBUG(SQL_TRANS, "Drop default: %s\n", col->base.name);
	if (col->t->persistence == SQL_DECLARED_TABLE) {
		col->def = NULL;
		return 0;
	} else {
		return sql_trans_alter_default(m->session->tr, col, NULL);
	}
}

int
mvc_storage(mvc *m, sql_column *col, char *storage)
{
	TRC_DEBUG(SQL_TRANS, "Storage: %s %s\n", col->base.name, storage);
	if (col->t->persistence == SQL_DECLARED_TABLE) {
		col->storage_type = storage?sa_strdup(m->sa, storage):NULL;
		return 0;
	} else {
		return sql_trans_alter_storage(m->session->tr, col, storage);
	}
}

int
mvc_access(mvc *m, sql_table *t, sht access)
{
	TRC_DEBUG(SQL_TRANS, "Access: %s %d\n", t->base.name, access);
	if (t->persistence == SQL_DECLARED_TABLE) {
		t->access = access;
		return 0;
	}
	return sql_trans_alter_access(m->session->tr, t, access);
}

int
mvc_is_sorted(mvc *m, sql_column *col)
{
	TRC_DEBUG(SQL_TRANS, "Is sorted: %s\n", col->base.name);
	return sql_trans_is_sorted(m->session->tr, col);
}

int
mvc_is_unique(mvc *m, sql_column *col)
{
	TRC_DEBUG(SQL_TRANS, "Is unique: %s\n", col->base.name);
	return sql_trans_is_unique(m->session->tr, col);
}

int
mvc_is_duplicate_eliminated(mvc *m, sql_column *col)
{
	TRC_DEBUG(SQL_TRANS, "Is duplicate eliminated: %s\n", col->base.name);
	return sql_trans_is_duplicate_eliminated(m->session->tr, col);
}

int
mvc_copy_column(mvc *m, sql_table *t, sql_column *c, sql_column **cres)
{
	return sql_trans_copy_column(m->session->tr, t, c, cres);
}

int
mvc_copy_key(mvc *m, sql_table *t, sql_key *k, sql_key **kres)
{
	return sql_trans_copy_key(m->session->tr, t, k, kres);
}

int
mvc_copy_idx(mvc *m, sql_table *t, sql_idx *i, sql_idx **ires)
{
	return sql_trans_copy_idx(m->session->tr, t, i, ires);
}

int
mvc_copy_trigger(mvc *m, sql_table *t, sql_trigger *tr, sql_trigger **tres)
{
	return sql_trans_copy_trigger(m->session->tr, t, tr, tres);
}

sql_rel *
sql_processrelation(mvc *sql, sql_rel *rel, int instantiate, int value_based_opt, int storage_based_opt)
{
	if (rel)
		rel = rel_unnest(sql, rel);
	if (rel)
		rel = rel_optimizer(sql, rel, instantiate, value_based_opt, storage_based_opt);
	return rel;
}

static inline int dlist_cmp(mvc *sql, dlist *l1, dlist *l2);

static inline int
dnode_cmp(mvc *sql, dnode *d1, dnode *d2)
{
	if (d1 == d2)
		return 0;

	if (!d1 || !d2)
		return -1;

	if (d1->type == d2->type) {
		switch (d1->type) {
			case type_int:
				return (d1->data.i_val - d2->data.i_val);
			case type_lng: {
				lng c = d1->data.l_val - d2->data.l_val;
				assert((lng) GDK_int_min <= c && c <= (lng) GDK_int_max);
				return (int) c;
			}
			case type_string:
				if (d1->data.sval == d2->data.sval)
					return 0;
				if (!d1->data.sval || !d2->data.sval)
					return -1;
				return strcmp(d1->data.sval, d2->data.sval);
			case type_list:
				return dlist_cmp(sql, d1->data.lval, d2->data.lval);
			case type_symbol:
				return symbol_cmp(sql, d1->data.sym, d2->data.sym);
			case type_type:
				return subtype_cmp(&d1->data.typeval, &d2->data.typeval);
			default:
				assert(0);
		}
	}
	return -1;
}

static inline int
dlist_cmp(mvc *sql, dlist *l1, dlist *l2)
{
	int res = 0;
	dnode *d1, *d2;

	if (l1 == l2)
		return 0;

	if (!l1 || !l2 || dlist_length(l1) != dlist_length(l2))
		return -1;

	for (d1 = l1->h, d2 = l2->h; !res && d1; d1 = d1->next, d2 = d2->next) {
		res = dnode_cmp(sql, d1, d2);
	}
	return res;
}

static inline int
AtomNodeCmp(AtomNode *a1, AtomNode *a2)
{
	if (a1 == a2)
		return 0;
	if (!a1 || !a2)
		return -1;
	if (a1->a && a2->a)
		return atom_cmp(a1->a, a2->a);
	return -1;
}

static inline int
SelectNodeCmp(mvc *sql, SelectNode *s1, SelectNode *s2)
{
	if (s1 == s2)
		return 0;
	if (!s1 || !s2)
		return -1;

	if (symbol_cmp(sql, s1->limit, s2->limit) == 0 &&
		symbol_cmp(sql, s1->offset, s2->offset) == 0 &&
		symbol_cmp(sql, s1->sample, s2->sample) == 0 &&
		symbol_cmp(sql, s1->seed, s2->seed) == 0 &&
		s1->distinct == s2->distinct &&
		s1->lateral == s2->lateral &&
		symbol_cmp(sql, s1->name, s2->name) == 0 &&
		symbol_cmp(sql, s1->orderby, s2->orderby) == 0 &&
		symbol_cmp(sql, s1->having, s2->having) == 0 &&
		symbol_cmp(sql, s1->groupby, s2->groupby) == 0 &&
		symbol_cmp(sql, s1->where, s2->where) == 0 &&
		symbol_cmp(sql, s1->from, s2->from) == 0 &&
		symbol_cmp(sql, s1->window, s2->window) == 0 &&
		dlist_cmp(sql, s1->selection, s2->selection) == 0)
		return 0;
	return -1;
}

static inline int
_symbol_cmp(mvc *sql, symbol *s1, symbol *s2)
{
	if (s1 == s2)
		return 0;
	if (!s1 || !s2)
		return -1;
	if (s1->token != s2->token || s1->type != s2->type)
		return -1;
	switch (s1->type) {
		case type_int:
			return (s1->data.i_val - s2->data.i_val);
		case type_lng: {
			lng c = s1->data.l_val - s2->data.l_val;
			assert((lng) GDK_int_min <= c && c <= (lng) GDK_int_max);
			return (int) c;
		}
		case type_string:
			if (s1->data.sval == s2->data.sval)
				return 0;
			if (!s1->data.sval || !s2->data.sval)
				return -1;
			return strcmp(s1->data.sval, s2->data.sval);
		case type_list: {
				return dlist_cmp(sql, s1->data.lval, s2->data.lval);
		}
		case type_type:
			return subtype_cmp(&s1->data.typeval, &s2->data.typeval);
		case type_symbol:
			if (s1->token == SQL_SELECT) {
				if (s2->token != SQL_SELECT)
					return -1;
				return SelectNodeCmp(sql, (SelectNode *) s1, (SelectNode *) s2);
			} else if (s1->token == SQL_ATOM) {
				if (s2->token != SQL_ATOM)
					return -1;
				return AtomNodeCmp((AtomNode *) s1, (AtomNode *) s2);
			} else {
				return symbol_cmp(sql, s1->data.sym, s2->data.sym);
			}
		default:
			assert(0);
	}
	return 0;		/* never reached, just to pacify compilers */
}

int
symbol_cmp(mvc *sql, symbol *s1, symbol *s2)
{
	return _symbol_cmp(sql, s1, s2);
}
