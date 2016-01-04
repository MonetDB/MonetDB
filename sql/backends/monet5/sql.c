/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * authors M Kersten, N Nes
 * SQL support implementation
 * This module contains the wrappers around the SQL
 * multi-version-catalog and support routines copied
 * from the Version 4 code base.
 */
#include "monetdb_config.h"
#include "sql.h"
#include "streams.h"
#include "sql_result.h"
#include "sql_gencode.h"
#include <sql_storage.h>
#include <sql_scenario.h>
#include <store_sequence.h>
#include <sql_optimizer.h>
#include <sql_datetime.h>
#include <rel_optimizer.h>
#include <rel_distribute.h>
#include <rel_select.h>
#include <rel_exp.h>
#include <rel_dump.h>
#include <rel_bin.h>
#include <bbp.h>
#include <opt_pipes.h>
#include "clients.h"
#include "mal_instruction.h"

static int
rel_is_table(sql_rel *rel)
{
	if (!rel || is_base(rel->op))
		return 1;
	return 0;
}

static int
exp_is_point_select(sql_exp *e)
{
	if (!e)
		return 1;
	if (e->type == e_cmp && !e->f && e->flag == (int) cmp_equal) {
		sql_exp *r = e->r;
		sql_exp *l = e->l;

		if (!is_func(l->type) && r->card <= CARD_AGGR)
			return 1;
	}
	return 0;
}

static int
rel_is_point_query(sql_rel *rel)
{
	int is_point = 0;

	if (!rel)
		return 1;
	if (is_project(rel->op))
		return rel_is_point_query(rel->l);
	if (is_select(rel->op) && rel_is_table(rel->l) && rel->exps) {
		is_point = 0;
		/* just one point expression makes this a point query */
		if (rel->exps->h)
			if (exp_is_point_select(rel->exps->h->data))
				is_point = 1;
	}
	return is_point;
}

static int
rel_need_distinct_query(sql_rel *rel)
{
	int need_distinct = 0;

	while (!need_distinct && rel && is_project(rel->op) && !is_groupby(rel->op))
		rel = rel->l;
	if (!need_distinct && rel && is_groupby(rel->op) && rel->exps) {
		node *n, *m;
		for (n = rel->exps->h; n && !need_distinct; n = n->next) {
			sql_exp *e = n->data;
			if (e->type == e_aggr) {
				list *l = e->l;

				if (l)
					for (m = l->h; m && !need_distinct; m = m->next) {
						sql_exp *a = m->data;

						if (need_distinct(a))
							need_distinct = 1;
					}
			}
		}
	}
	return need_distinct;
}

sql_rel *
sql_symbol2relation(mvc *c, symbol *sym)
{
	sql_rel *r;

	r = rel_semantic(c, sym);
	if (!r)
		return NULL;
	if (r) {
		r = rel_optimizer(c, r);
		r = rel_distribute(c, r);
		if (rel_is_point_query(r) || rel_need_distinct_query(r))
			c->point_query = 1;
	}
	return r;
}

stmt *
sql_relation2stmt(mvc *c, sql_rel *r)
{
	stmt *s = NULL;

	if (!r) {
		return NULL;
	} else {
		if (c->emode == m_plan) {
			rel_print(c, r, 0);
		} else {
			s = output_rel_bin(c, r);
		}
	}
	return s;
}

/*
 * After the SQL statement has been executed, its data structures
 * should be garbage collected. For successful actions we have to finish
 * the transaction as well, e.g. commit or rollback.
 */
int
sqlcleanup(mvc *c, int err)
{
	sql_destroy_params(c);
	sql_destroy_args(c);

	if ((c->emod & mod_locked) == mod_locked) {
		/* here we should commit the transaction */
		if (!err) {
			sql_trans_commit(c->session->tr);
			/* write changes to disk */
			sql_trans_end(c->session);
			store_apply_deltas();
			sql_trans_begin(c->session);
		}
		store_unlock();
		c->emod = 0;
	}
	/* some statements dynamically disable caching */
	c->sym = NULL;
	if (c->sa)
		c->sa = sa_reset(c->sa);
	if (err >0)
		c->session->status = -err;
	if (err <0)
		c->session->status = err;
	c->label = 0;
	c->point_query = 0;
	scanner_query_processed(&(c->scanner));
	return err;
}

/*
 * The internal administration of the SQL compilation and execution state
 * is administered by a state descriptor accessible in each phase.
 * Failure to find the state descriptor aborts the session.
 */

str
checkSQLContext(Client cntxt)
{
	backend *be;

	if (cntxt == NULL)
		throw(SQL, "mvc", "No client record");
	if (cntxt->sqlcontext == NULL)
		throw(SQL, "mvc", "SQL module not initialized");
	be = (backend *) cntxt->sqlcontext;
	if (be->mvc == NULL)
		throw(SQL, "mvc", "SQL module not initialized, mvc struct missing");
	return MAL_SUCCEED;
}

str
getSQLContext(Client cntxt, MalBlkPtr mb, mvc **c, backend **b)
{
	backend *be;
	(void) mb;

	if (cntxt == NULL)
		throw(SQL, "mvc", "No client record");
	if (cntxt->sqlcontext == NULL)
		throw(SQL, "mvc", "SQL module not initialized");
	be = (backend *) cntxt->sqlcontext;
	if (be->mvc == NULL)
		throw(SQL, "mvc", "SQL module not initialized, mvc struct missing");
	if (c)
		*c = be->mvc;
	if (b)
		*b = be;
	return MAL_SUCCEED;
}

str
SQLmvc(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg;
	int *res = getArgReference_int(stk, pci, 0);

	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	*res = 0;
	return MAL_SUCCEED;
}

str
SQLtransaction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg;
	int type = *getArgReference_int(stk, pci, 1);
	int chain = *getArgReference_int(stk, pci, 2);
	str name = *getArgReference_str(stk, pci, 3);
	char buf[BUFSIZ];
	int ret = 0;

	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (name && strcmp(name, str_nil) == 0)
		name = NULL;

	switch (type) {
	case DDL_RELEASE:
		if (sql->session->auto_commit == 1)
			throw(SQL, "sql.trans", "3BM30!RELEASE SAVEPOINT: not allowed in auto commit mode");
		ret = mvc_release(sql, name);
		if (ret < 0) {
			snprintf(buf, BUFSIZ, "3B000!RELEASE SAVEPOINT: (%s) failed", name);
			throw(SQL, "sql.trans", "%s", buf);
		}
		break;
	case DDL_COMMIT:
		if (sql->session->auto_commit == 1) {
			if (name)
				throw(SQL, "sql.trans", "3BM30!SAVEPOINT: not allowed in auto commit mode");
			else
				throw(SQL, "sql.trans", "2DM30!COMMIT: not allowed in auto commit mode");
		}
		ret = mvc_commit(sql, chain, name);
		if (ret < 0 && !name)
			throw(SQL, "sql.trans", "2D000!COMMIT: failed");
		if (ret < 0 && name)
			throw(SQL, "sql.trans", "3B000!SAVEPOINT: (%s) failed", name);
		break;
	case DDL_ROLLBACK:
		if (sql->session->auto_commit == 1)
			throw(SQL, "sql.trans", "2DM30!ROLLBACK: not allowed in auto commit mode");
		RECYCLEdrop(cntxt);
		ret = mvc_rollback(sql, chain, name);
		if (ret < 0 && name) {
			snprintf(buf, BUFSIZ, "3B000!ROLLBACK TO SAVEPOINT: (%s) failed", name);
			throw(SQL, "sql.trans", "%s", buf);
		}
		break;
	case DDL_TRANS:
		if (sql->session->auto_commit == 0)
			throw(SQL, "sql.trans", "25001!START TRANSACTION: cannot start a transaction within a transaction");
		if (sql->session->active) {
			RECYCLEdrop(cntxt);
			mvc_rollback(sql, 0, NULL);
		}
		sql->session->auto_commit = 0;
		sql->session->ac_on_commit = 1;
		sql->session->level = chain;
		(void) mvc_trans(sql);
		break;
	default:
		throw(SQL, "sql.trans", "transaction unknown type");
	}
	return MAL_SUCCEED;
}

str
SQLcommit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int ret;
	mvc *sql = NULL;
	str msg;
	(void) stk;
	(void) pci;

	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if (sql->session->auto_commit != 0)
		throw(SQL, "sql.trans", "2DM30!COMMIT: not allowed in auto commit mode");
	ret = mvc_commit(sql, 0, 0);
	if (ret < 0)
		throw(SQL, "sql.trans", "2D000!COMMIT: failed");
	return msg;
}

str
SQLabort(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg;
	(void) stk;
	(void) pci;

	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if (sql->session->active) {
		RECYCLEdrop(cntxt);
		mvc_rollback(sql, 0, NULL);
	}
	return msg;
}

str
SQLshutdown_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg;

	if ((msg = CLTshutdown(cntxt, mb, stk, pci)) == MAL_SUCCEED) {
		// administer the shutdown
		mnstr_printf(GDKstdout, "#%s\n", *getArgReference_str(stk, pci, 0));
	}
	return msg;
}

str
SQLtransaction2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg;

	(void) stk;
	(void) pci;

	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (sql->session->auto_commit == 0)
		throw(SQL, "sql.trans", "25001!START TRANSACTION: cannot start a transaction within a transaction");
	if (sql->session->active) {
		RECYCLEdrop(cntxt);
		mvc_rollback(sql, 0, NULL);
	}
	sql->session->auto_commit = 0;
	sql->session->ac_on_commit = 1;
	sql->session->level = 0;
	(void) mvc_trans(sql);
	return msg;
}

static str
create_table_or_view(mvc *sql, char *sname, sql_table *t, int temp)
{
	sql_allocator *osa;
	sql_schema *s = mvc_bind_schema(sql, sname);
	sql_table *nt = NULL;
	node *n;

	if (STORE_READONLY)
		return sql_error(sql, 06, "25006!schema statements cannot be executed on a readonly database.");

	if (!s)
		return sql_message("3F000!CREATE %s: schema '%s' doesn't exist", (t->query) ? "TABLE" : "VIEW", sname);

	if (mvc_bind_table(sql, s, t->base.name)) {
		char *cd = (temp == SQL_DECLARED_TABLE) ? "DECLARE" : "CREATE";
		return sql_message("42S01!%s TABLE: name '%s' already in use", cd, t->base.name);
	} else if (temp != SQL_DECLARED_TABLE && (!mvc_schema_privs(sql, s) && !(isTempSchema(s) && temp == SQL_LOCAL_TEMP))) {
		return sql_message("42000!CREATE TABLE: insufficient privileges for user '%s' in schema '%s'", stack_get_string(sql, "current_user"), s->base.name);
	} else if (temp == SQL_DECLARED_TABLE && !list_empty(t->keys.set)) {
		return sql_message("42000!DECLARE TABLE: '%s' cannot have constraints", t->base.name);

	}

	osa = sql->sa;
	sql->sa = NULL;
	/* first check default values */
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;

		if (c->def) {
			char *buf;
			sql_rel *r = NULL;

			sql->sa = sa_create();
			buf = sa_alloc(sql->sa, strlen(c->def) + 8);
			snprintf(buf, BUFSIZ, "select %s;", c->def);
			r = rel_parse(sql, s, buf, m_deps);
			if (!r || !is_project(r->op) || !r->exps || list_length(r->exps) != 1 || rel_check_type(sql, &c->type, r->exps->h->data, type_equal) == NULL)
				throw(SQL, "sql.catalog", "%s", sql->errstr);
			rel_destroy(r);
			sa_destroy(sql->sa);
			sql->sa = NULL;
		}
	}

	nt = sql_trans_create_table(sql->session->tr, s, t->base.name, t->query, t->type, t->system, temp, t->commit_action, t->sz);

	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		if (mvc_copy_column(sql, nt, c) == NULL)
			throw(SQL, "sql.catalog", "CREATE TABLE: %s_%s_%s conflicts", s->base.name, t->base.name, c->base.name);

	}
	if (t->idxs.set) {
		for (n = t->idxs.set->h; n; n = n->next) {
			sql_idx *i = n->data;
			mvc_copy_idx(sql, nt, i);
		}
	}
	if (t->keys.set) {
		for (n = t->keys.set->h; n; n = n->next) {
			sql_key *k = n->data;

			mvc_copy_key(sql, nt, k);
		}
	}
	/* also create dependencies */
	if (nt->query && isView(nt)) {
		sql_rel *r = NULL;

		sql->sa = sa_create();
		r = rel_parse(sql, s, nt->query, m_deps);
		if (r)
			r = rel_optimizer(sql, r);
		if (r) {
			stmt *sqs = rel_bin(sql, r);
			list *view_id_l = stmt_list_dependencies(sql->sa, sqs, VIEW_DEPENDENCY);
			list *id_l = stmt_list_dependencies(sql->sa, sqs, COLUMN_DEPENDENCY);
			list *func_id_l = stmt_list_dependencies(sql->sa, sqs, FUNC_DEPENDENCY);

			mvc_create_dependencies(sql, id_l, nt->base.id, VIEW_DEPENDENCY);
			mvc_create_dependencies(sql, view_id_l, nt->base.id, VIEW_DEPENDENCY);
			mvc_create_dependencies(sql, func_id_l, nt->base.id, VIEW_DEPENDENCY);
		}
		sa_destroy(sql->sa);
	}
	sql->sa = osa;
	return MAL_SUCCEED;
}

static int
table_has_updates(sql_trans *tr, sql_table *t)
{
	node *n;
	int cnt = 0;

	for ( n = t->columns.set->h; !cnt && n; n = n->next) {
		sql_column *c = n->data;
		BAT *b = store_funcs.bind_col(tr, c, RD_UPD_ID);
		if ( b == 0)
			return -1;
		cnt |= BATcount(b) > 0;
		if (isTable(t) && t->access != TABLE_READONLY && (t->base.flag != TR_NEW /* alter */ ) &&
		    t->persistence == SQL_PERSIST && !t->commit_action)
			cnt |= store_funcs.count_col(tr, c, 0) > 0;
		BBPunfix(b->batCacheid);
	}
	return cnt;
}

static str
alter_table(mvc *sql, char *sname, sql_table *t)
{
	sql_schema *s = mvc_bind_schema(sql, sname);
	sql_table *nt = NULL;
	node *n;

	if (!s)
		return sql_message("3F000!ALTER TABLE: no such schema '%s'", sname);

	if ((nt = mvc_bind_table(sql, s, t->base.name)) == NULL) {
		return sql_message("42S02!ALTER TABLE: no such table '%s'", t->base.name);

	} else if (!mvc_schema_privs(sql, s) && !(isTempSchema(s) && t->persistence == SQL_LOCAL_TEMP)) {
		return sql_message("42000!ALTER TABLE: insufficient privileges for user '%s' in schema '%s'", stack_get_string(sql, "current_user"), s->base.name);
	}

	/* First check if all the changes are allowed */
	if (t->idxs.set) {
		/* only one pkey */
		if (nt->pkey) {
			for (n = t->idxs.nelm; n; n = n->next) {
				sql_idx *i = n->data;
				if (i->key && i->key->type == pkey)
					return sql_message("40000!CONSTRAINT PRIMARY KEY: a table can have only one PRIMARY KEY\n");
			}
		}
	}

	/* check for changes */
	if (t->columns.dset)
		for (n = t->columns.dset->h; n; n = n->next) {
			/* propagate alter table .. drop column */
			sql_column *c = n->data;
			sql_column *nc = mvc_bind_column(sql, nt, c->base.name);
			mvc_drop_column(sql, nt, nc, c->drop_action);
		}
	/* check for changes on current cols */
	for (n = t->columns.set->h; n != t->columns.nelm; n = n->next) {

		/* null or default value changes */
		sql_column *c = n->data;
		sql_column *nc = mvc_bind_column(sql, nt, c->base.name);

		if (c->null != nc->null && isTable(nt)) {
			mvc_null(sql, nc, c->null);
			/* for non empty check for nulls */
			if (c->null == 0) {
				void *nilptr = ATOMnilptr(c->type.type->localtype);
				rids *nils = table_funcs.rids_select(sql->session->tr, nc, nilptr, NULL, NULL);
				int has_nils = (table_funcs.rids_next(nils) != oid_nil);

				table_funcs.rids_destroy(nils);
				if (has_nils)
					return sql_message("40002!ALTER TABLE: NOT NULL constraint violated for column %s.%s", c->t->base.name, c->base.name);
			}
		}
		if (c->def != nc->def)
			mvc_default(sql, nc, c->def);

		if (c->storage_type != nc->storage_type) {
			if (c->t->access == TABLE_WRITABLE)
				return sql_message("40002!ALTER TABLE: SET STORAGE for column %s.%s only allowed on READ or INSERT ONLY tables", c->t->base.name, c->base.name);
			nc->base.rtime = nc->base.wtime = sql->session->tr->wtime;
			mvc_storage(sql, nc, c->storage_type);
		}
	}
	for (; n; n = n->next) {
		/* propagate alter table .. add column */
		sql_column *c = n->data;
		mvc_copy_column(sql, nt, c);
	}
	if (t->idxs.set) {
		/* alter drop index */
		if (t->idxs.dset)
			for (n = t->idxs.dset->h; n; n = n->next) {
				sql_idx *i = n->data;
				sql_idx *ni = mvc_bind_idx(sql, s, i->base.name);
				mvc_drop_idx(sql, s, ni);
			}
		/* alter add index */
		for (n = t->idxs.nelm; n; n = n->next) {
			sql_idx *i = n->data;
			mvc_copy_idx(sql, nt, i);
		}
	}
	if (t->keys.set) {
		/* alter drop key */
		if (t->keys.dset)
			for (n = t->keys.dset->h; n; n = n->next) {
				sql_key *k = n->data;
				sql_key *nk = mvc_bind_key(sql, s, k->base.name);
				if (nk)
					mvc_drop_key(sql, s, nk, k->drop_action);
			}
		/* alter add key */
		for (n = t->keys.nelm; n; n = n->next) {
			sql_key *k = n->data;
			mvc_copy_key(sql, nt, k);
		}
	}
	return MAL_SUCCEED;
}

static char *
drop_table(mvc *sql, char *sname, char *tname, int drop_action)
{
	sql_schema *s = NULL;
	sql_table *t = NULL;
	node *n;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_message("3F000!DROP TABLE: no such schema '%s'", sname);
	if (!s)
		s = cur_schema(sql);
	t = mvc_bind_table(sql, s, tname);
	if (!t && !sname) {
		s = tmp_schema(sql);
		t = mvc_bind_table(sql, s, tname);
	}
	if (!t) {
		return sql_message("42S02!DROP TABLE: no such table '%s'", tname);
	} else if (isView(t)) {
		return sql_message("42000!DROP TABLE: cannot drop VIEW '%s'", tname);
	} else if (t->system) {
		return sql_message("42000!DROP TABLE: cannot drop system table '%s'", tname);
	} else if (!mvc_schema_privs(sql, s) && !(isTempSchema(s) && t->persistence == SQL_LOCAL_TEMP)) {
		return sql_message("42000!DROP TABLE: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), s->base.name);
	}
	if (!drop_action && t->keys.set) {
		for (n = t->keys.set->h; n; n = n->next) {
			sql_key *k = n->data;

			if (k->type == ukey || k->type == pkey) {
				sql_ukey *uk = (sql_ukey *) k;

				if (uk->keys && list_length(uk->keys)) {
					node *l = uk->keys->h;

					for (; l; l = l->next) {
						k = l->data;
						/* make sure it is not a self referencing key */
						if (k->t != t)
							return sql_message("40000!DROP TABLE: FOREIGN KEY %s.%s depends on %s", k->t->base.name, k->base.name, tname);
					}
				}
			}
		}
	}

	if (!drop_action && mvc_check_dependency(sql, t->base.id, TABLE_DEPENDENCY, NULL))
		return sql_message("42000!DROP TABLE: unable to drop table %s (there are database objects which depend on it)\n", t->base.name);

	mvc_drop_table(sql, s, t, drop_action);
	return MAL_SUCCEED;
}

static char *
drop_view(mvc *sql, char *sname, char *tname, int drop_action)
{
	sql_table *t = NULL;
	sql_schema *ss = NULL;

	if (sname != NULL && (ss = mvc_bind_schema(sql, sname)) == NULL)
		return sql_message("3F000!DROP VIEW: no such schema '%s'", sname);

	if (ss == NULL)
		ss = cur_schema(sql);

	t = mvc_bind_table(sql, ss, tname);

	if (!mvc_schema_privs(sql, ss) && !(isTempSchema(ss) && t && t->persistence == SQL_LOCAL_TEMP)) {
		return sql_message("42000!DROP VIEW: access denied for %s to schema '%s'", stack_get_string(sql, "current_user"), ss->base.name);
	} else if (!t) {
		return sql_message("42S02!DROP VIEW: unknown view '%s'", tname);
	} else if (!isView(t)) {
		return sql_message("42000!DROP VIEW: unable to drop view '%s': is a table", tname);
	} else if (t->system) {
		return sql_message("42000!DROP VIEW: cannot drop system view '%s'", tname);
	} else if (!drop_action && mvc_check_dependency(sql, t->base.id, VIEW_DEPENDENCY, NULL)) {
		return sql_message("42000!DROP VIEW: cannot drop view '%s', there are database objects which depend on it", t->base.name);
	} else {
		mvc_drop_table(sql, ss, t, drop_action);
		return MAL_SUCCEED;
	}
}

static str
drop_key(mvc *sql, char *sname, char *kname, int drop_action)
{
	sql_key *key;
	sql_schema *ss = NULL;

	if (sname != NULL && (ss = mvc_bind_schema(sql, sname)) == NULL)
		return sql_message("3F000!ALTER TABLE: no such schema '%s'", sname);

	if (ss == NULL)
		ss = cur_schema(sql);

	if ((key = mvc_bind_key(sql, ss, kname)) == NULL)
		return sql_message("42000!ALTER TABLE: no such constraint '%s'", kname);
	if (!drop_action && mvc_check_dependency(sql, key->base.id, KEY_DEPENDENCY, NULL))
		return sql_message("42000!ALTER TABLE: cannot drop constraint '%s': there are database objects which depend on it", key->base.name);
	mvc_drop_key(sql, ss, key, drop_action);
	return MAL_SUCCEED;
}

static str
drop_index(mvc *sql, char *sname, char *iname)
{
	sql_schema *s = NULL;
	sql_idx *i = NULL;

	if (!(s = mvc_bind_schema(sql, sname)))
		return sql_message("3F000!DROP INDEX: no such schema '%s'", sname);
	i = mvc_bind_idx(sql, s, iname);
	if (!i) {
		return sql_message("42S12!DROP INDEX: no such index '%s'", iname);
	} else if (!mvc_schema_privs(sql, s)) {
		return sql_message("42000!DROP INDEX: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), s->base.name);
	} else {
		mvc_drop_idx(sql, s, i);
	}
	return NULL;
}

static str
create_seq(mvc *sql, char *sname, sql_sequence *seq)
{
	sql_schema *s = NULL;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_message("3F000!CREATE SEQUENCE: no such schema '%s'", sname);
	if (s == NULL)
		s = cur_schema(sql);
	if (find_sql_sequence(s, seq->base.name)) {
		return sql_message("42000!CREATE SEQUENCE: name '%s' already in use", seq->base.name);
	} else if (!mvc_schema_privs(sql, s)) {
		return sql_message("42000!CREATE SEQUENCE: insufficient privileges for '%s' in schema '%s'", stack_get_string(sql, "current_user"), s->base.name);
	}
	sql_trans_create_sequence(sql->session->tr, s, seq->base.name, seq->start, seq->minvalue, seq->maxvalue, seq->increment, seq->cacheinc, seq->cycle, seq->bedropped);
	return NULL;
}

static str
alter_seq(mvc *sql, char *sname, sql_sequence *seq, lng *val)
{
	sql_schema *s = NULL;
	sql_sequence *nseq = NULL;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_message("3F000!ALTER SEQUENCE: no such schema '%s'", sname);
	if (s == NULL)
		s = cur_schema(sql);
	if (!(nseq = find_sql_sequence(s, seq->base.name))) {
		return sql_message("42000!ALTER SEQUENCE: no such sequence '%s'", seq->base.name);
	} else if (!mvc_schema_privs(sql, s)) {
		return sql_message("42000!ALTER SEQUENCE: insufficient privileges for '%s' in schema '%s'", stack_get_string(sql, "current_user"), s->base.name);
	}

	/* first alter the known values */
	sql_trans_alter_sequence(sql->session->tr, nseq, seq->minvalue, seq->maxvalue, seq->increment, seq->cacheinc, seq->cycle);
	if (val)
		sql_trans_sequence_restart(sql->session->tr, nseq, *val);
	return MAL_SUCCEED;
}

static str
drop_seq(mvc *sql, char *sname, char *name)
{
	sql_schema *s = NULL;
	sql_sequence *seq = NULL;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_message("3F000!DROP SEQUENCE: no such schema '%s'", sname);
	if (!s)
		s = cur_schema(sql);
	if (!(seq = find_sql_sequence(s, name))) {
		return sql_message("42M35!DROP SEQUENCE: no such sequence '%s'", name);
	} else if (!mvc_schema_privs(sql, s)) {
		return sql_message("42000!DROP SEQUENCE: insufficient privileges for '%s' in schema '%s'", stack_get_string(sql, "current_user"), s->base.name);
	}
	if (mvc_check_dependency(sql, seq->base.id, BEDROPPED_DEPENDENCY, NULL))
		return sql_message("2B000!DROP SEQUENCE: unable to drop sequence %s (there are database objects which depend on it)\n", seq->base.name);

	sql_trans_drop_sequence(sql->session->tr, s, seq, 0);
	return NULL;
}

static str
drop_func(mvc *sql, char *sname, char *name, int fid, int type, int action)
{
	sql_schema *s = NULL;
	char is_aggr = (type == F_AGGR);
	char is_func = (type != F_PROC);
	char *F = is_aggr ? "AGGREGATE" : (is_func ? "FUNCTION" : "PROCEDURE");
	char *f = is_aggr ? "aggregate" : (is_func ? "function" : "procedure");
	char *KF = type == F_FILT ? "FILTER " : type == F_UNION ? "UNION " : "";
	char *kf = type == F_FILT ? "filter " : type == F_UNION ? "union " : "";

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_message("3F000!DROP %s%s: no such schema '%s'", KF, F, sname);
	if (!s)
		s = cur_schema(sql);
	if (fid >= 0) {
		node *n = find_sql_func_node(s, fid);
		if (n) {
			sql_func *func = n->data;

			if (!mvc_schema_privs(sql, s)) {
				return sql_message("DROP %s%s: access denied for %s to schema ;'%s'", KF, F, stack_get_string(sql, "current_user"), s->base.name);
			}
			if (!action && mvc_check_dependency(sql, func->base.id, !IS_PROC(func) ? FUNC_DEPENDENCY : PROC_DEPENDENCY, NULL))
				return sql_message("DROP %s%s: there are database objects dependent on %s%s %s;", KF, F, kf, f, func->base.name);

			mvc_drop_func(sql, s, func, action);
		}
	} else {
		node *n = NULL;
		list *list_func = schema_bind_func(sql, s, name, type);

		if (!mvc_schema_privs(sql, s)) {
			list_destroy(list_func);
			return sql_message("DROP %s%s: access denied for %s to schema ;'%s'", KF, F, stack_get_string(sql, "current_user"), s->base.name);
		}
		for (n = list_func->h; n; n = n->next) {
			sql_func *func = n->data;

			if (!action && mvc_check_dependency(sql, func->base.id, !IS_PROC(func) ? FUNC_DEPENDENCY : PROC_DEPENDENCY, list_func)) {
				list_destroy(list_func);
				return sql_message("DROP %s%s: there are database objects dependent on %s%s %s;", KF, F, kf, f, func->base.name);
			}
		}
		mvc_drop_all_func(sql, s, list_func, action);
		list_destroy(list_func);
	}
	return MAL_SUCCEED;
}

static char *
create_func(mvc *sql, char *sname, sql_func *f)
{
	sql_func *nf;
	sql_schema *s = NULL;
	char is_aggr = (f->type == F_AGGR);
	char is_func = (f->type != F_PROC);
	char *F = is_aggr ? "AGGREGATE" : (is_func ? "FUNCTION" : "PROCEDURE");
	char *KF = f->type == F_FILT ? "FILTER " : f->type == F_UNION ? "UNION " : "";

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_message("3F000!CREATE %s%s: no such schema '%s'", KF, F, sname);
	if (!s)
		s = cur_schema(sql);
	nf = mvc_create_func(sql, NULL, s, f->base.name, f->ops, f->res, f->type, f->lang, f->mod, f->imp, f->query, f->varres, f->vararg);
	if (nf && nf->query && nf->lang <= FUNC_LANG_SQL) {
		char *buf;
		sql_rel *r = NULL;
		sql_allocator *sa = sql->sa;

		sql->sa = sa_create();
		buf = sa_strdup(sql->sa, nf->query);
		r = rel_parse(sql, s, buf, m_deps);
		if (r)
			r = rel_optimizer(sql, r);
		if (r) {
			node *n;
			stmt *sb = rel_bin(sql, r);
			list *id_col_l = stmt_list_dependencies(sql->sa, sb, COLUMN_DEPENDENCY);
			list *id_func_l = stmt_list_dependencies(sql->sa, sb, FUNC_DEPENDENCY);
			list *view_id_l = stmt_list_dependencies(sql->sa, sb, VIEW_DEPENDENCY);

			if (!f->vararg && f->ops) {
				for (n = f->ops->h; n; n = n->next) {
					sql_arg *a = n->data;

					if (a->type.type->s) 
						mvc_create_dependency(sql, a->type.type->base.id, nf->base.id, TYPE_DEPENDENCY);
				}
			}
			if (!f->varres && f->res) {
				for (n = f->res->h; n; n = n->next) {
					sql_arg *a = n->data;

					if (a->type.type->s) 
						mvc_create_dependency(sql, a->type.type->base.id, nf->base.id, TYPE_DEPENDENCY);
				}
			}
			mvc_create_dependencies(sql, id_col_l, nf->base.id, !IS_PROC(f) ? FUNC_DEPENDENCY : PROC_DEPENDENCY);
			mvc_create_dependencies(sql, id_func_l, nf->base.id, !IS_PROC(f) ? FUNC_DEPENDENCY : PROC_DEPENDENCY);
			mvc_create_dependencies(sql, view_id_l, nf->base.id, !IS_PROC(f) ? FUNC_DEPENDENCY : PROC_DEPENDENCY);
		}
		sa_destroy(sql->sa);
		sql->sa = sa;
	} else if (nf->lang == FUNC_LANG_MAL) {
		if (!backend_resolve_function(sql, nf))
			return sql_message("3F000!CREATE %s%s: external name %s.%s not bound", KF, F, nf->mod, nf->base.name);
	}
	return MAL_SUCCEED;
}

str
UPGdrop_func(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg = MAL_SUCCEED;
	int id = *getArgReference_int(stk, pci, 1);
	sql_func *func;

	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	func = sql_trans_find_func(sql->session->tr, id);
	if (func)
		mvc_drop_func(sql, func->s, func, 0);
	return msg;
}

str
UPGcreate_func(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg = MAL_SUCCEED;
	str sname = *getArgReference_str(stk, pci, 1), osname;
	str func = *getArgReference_str(stk, pci, 2);
	stmt *s;

	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	osname = cur_schema(sql)->base.name;
	mvc_set_schema(sql, sname);
	s = sql_parse(sql, sa_create(), func, 0);
	if (s && s->type == st_catalog) {
		char *schema = ((stmt*)s->op1->op4.lval->h->data)->op4.aval->data.val.sval;
		sql_func *func = (sql_func*)((stmt*)s->op1->op4.lval->t->data)->op4.aval->data.val.pval;

		msg = create_func(sql, schema, func);
		mvc_set_schema(sql, osname);
	} else {
		mvc_set_schema(sql, osname);
		throw(SQL, "sql.catalog", "function creation failed '%s'", func);
	}
	return msg;
}

str
UPGcreate_view(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg = MAL_SUCCEED;
	str sname = *getArgReference_str(stk, pci, 1), osname;
	str view = *getArgReference_str(stk, pci, 2);
	stmt *s;

	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	osname = cur_schema(sql)->base.name;
	mvc_set_schema(sql, sname);
	s = sql_parse(sql, sa_create(), view, 0);
	if (s && s->type == st_catalog) {
		char *schema = ((stmt*)s->op1->op4.lval->h->data)->op4.aval->data.val.sval;
		sql_table *v = (sql_table*)((stmt*)s->op1->op4.lval->h->next->data)->op4.aval->data.val.pval;
		int temp = ((stmt*)s->op1->op4.lval->t->data)->op4.aval->data.val.ival;

		msg = create_table_or_view(sql, schema, v, temp);
		mvc_set_schema(sql, osname);
	} else {
		mvc_set_schema(sql, osname);
		throw(SQL, "sql.catalog", "view creation failed '%s'", view);
	}
	return msg;
}

static char *
create_trigger(mvc *sql, char *sname, char *tname, char *triggername, int time, int orientation, int event, char *old_name, char *new_name, char *condition, char *query)
{
	sql_trigger *tri = NULL;
	sql_schema *s = NULL;
	sql_table *t;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_message("3F000!CREATE TRIGGER: no such schema '%s'", sname);
	if (!s)
		s = cur_schema(sql);
	if (!mvc_schema_privs(sql, s))
		return sql_message("3F000!CREATE TRIGGER: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), s->base.name);
	if (mvc_bind_trigger(sql, s, triggername) != NULL)
		return sql_message("3F000!CREATE TRIGGER: name '%s' already in use", triggername);

	if (!(t = mvc_bind_table(sql, s, tname)))
		return sql_message("3F000!CREATE TRIGGER: unknown table '%s'", tname);

	if (isView(t))
		return sql_message("3F000!CREATE TRIGGER: cannot create trigger on view '%s'", tname);

	tri = mvc_create_trigger(sql, t, triggername, time, orientation, event, old_name, new_name, condition, query);
	if (tri) {
		char *buf;
		sql_rel *r = NULL;
		sql_allocator *sa = sql->sa;

		sql->sa = sa_create();
		buf = sa_strdup(sql->sa, query);
		r = rel_parse(sql, s, buf, m_deps);
		if (r)
			r = rel_optimizer(sql, r);
		/* TODO use relational part to find dependencies */
		if (r) {
			stmt *sqs = rel_bin(sql, r);
			list *col_l = stmt_list_dependencies(sql->sa, sqs, COLUMN_DEPENDENCY);
			list *func_l = stmt_list_dependencies(sql->sa, sqs, FUNC_DEPENDENCY);
			list *view_id_l = stmt_list_dependencies(sql->sa, sqs, VIEW_DEPENDENCY);

			mvc_create_dependencies(sql, col_l, tri->base.id, TRIGGER_DEPENDENCY);
			mvc_create_dependencies(sql, func_l, tri->base.id, TRIGGER_DEPENDENCY);
			mvc_create_dependencies(sql, view_id_l, tri->base.id, TRIGGER_DEPENDENCY);
		}
		sa_destroy(sql->sa);
		sql->sa = sa;
	}
	return MAL_SUCCEED;
}

static char *
drop_trigger(mvc *sql, char *sname, char *tname)
{
	sql_trigger *tri = NULL;
	sql_schema *s = NULL;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_message("3F000!DROP TRIGGER: no such schema '%s'", sname);
	if (!s)
		s = cur_schema(sql);
	assert(s);
	if (!mvc_schema_privs(sql, s))
		return sql_message("3F000!DROP TRIGGER: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), s->base.name);

	if ((tri = mvc_bind_trigger(sql, s, tname)) == NULL)
		return sql_message("3F000!DROP TRIGGER: unknown trigger %s\n", tname);
	mvc_drop_trigger(sql, s, tri);
	return MAL_SUCCEED;
}

static char *
rel_check_tables(sql_table *nt, sql_table *nnt)
{
	node *n, *m;

	if (cs_size(&nt->columns) != cs_size(&nnt->columns))
		return sql_message("3F000!ALTER MERGE TABLE: to be added table doesn't match MERGE TABLE definition");
	for (n = nt->columns.set->h, m = nnt->columns.set->h; n && m; n = n->next, m = m->next) {
		sql_column *nc = n->data;
		sql_column *mc = m->data;

		if (subtype_cmp(&nc->type, &mc->type) != 0)
			return sql_message("3F000!ALTER MERGE TABLE: to be added table column type doesn't match MERGE TABLE definition");
	}
	if (cs_size(&nt->idxs) != cs_size(&nnt->idxs))
		return sql_message("3F000!ALTER MERGE TABLE: to be added table index doesn't match MERGE TABLE definition");
	if (cs_size(&nt->idxs))
		for (n = nt->idxs.set->h, m = nnt->idxs.set->h; n && m; n = n->next, m = m->next) {
			sql_idx *ni = n->data;
			sql_idx *mi = m->data;

			if (ni->type != mi->type)
				return sql_message("3F000!ALTER MERGE TABLE: to be added table index type doesn't match MERGE TABLE definition");
		}
	return MAL_SUCCEED;
}

static char *
alter_table_add_table(mvc *sql, char *msname, char *mtname, char *psname, char *ptname)
{
	sql_schema *ms = mvc_bind_schema(sql, msname), *ps = mvc_bind_schema(sql, psname);
	sql_table *mt = NULL, *pt = NULL;

	if (ms)
		mt = mvc_bind_table(sql, ms, mtname);
	if (ps)
		pt = mvc_bind_table(sql, ps, ptname);
	if (mt && pt) {
		char *msg;
		node *n = cs_find_id(&mt->tables, pt->base.id);

		if (n)
			return sql_message("42S02!ALTER TABLE: table '%s.%s' is already part of the MERGE TABLE '%s.%s'", psname, ptname, msname, mtname);
		if ((msg = rel_check_tables(mt, pt)) != NULL)
			return msg;
		sql_trans_add_table(sql->session->tr, mt, pt);
	} else if (mt) {
		return sql_message("42S02!ALTER TABLE: no such table '%s' in schema '%s'", ptname, psname);
	} else {
		return sql_message("42S02!ALTER TABLE: no such table '%s' in schema '%s'", mtname, msname);
	}
	return MAL_SUCCEED;
}

static char *
alter_table_del_table(mvc *sql, char *msname, char *mtname, char *psname, char *ptname, int drop_action)
{
	sql_schema *ms = mvc_bind_schema(sql, msname), *ps = mvc_bind_schema(sql, psname);
	sql_table *mt = NULL, *pt = NULL;

	if (ms)
		mt = mvc_bind_table(sql, ms, mtname);
	if (ps)
		pt = mvc_bind_table(sql, ps, ptname);
	if (mt && pt) {
		node *n = NULL;

		if (!pt || (n = cs_find_id(&mt->tables, pt->base.id)) == NULL)
			return sql_message("42S02!ALTER TABLE: table '%s.%s' isn't part of the MERGE TABLE '%s.%s'", psname, ptname, msname, mtname);

		sql_trans_del_table(sql->session->tr, mt, pt, drop_action);
	} else if (mt) {
		return sql_message("42S02!ALTER TABLE: no such table '%s' in schema '%s'", ptname, psname);
	} else {
		return sql_message("42S02!ALTER TABLE: no such table '%s' in schema '%s'", mtname, msname);
	}
	return MAL_SUCCEED;
}

static char *
alter_table_set_access(mvc *sql, char *sname, char *tname, int access)
{
	sql_schema *s = mvc_bind_schema(sql, sname);
	sql_table *t = NULL;

	if (s)
		t = mvc_bind_table(sql, s, tname);
	if (t) {
		if (t->type == tt_merge_table)
			return sql_message("42S02!ALTER TABLE: read only MERGE TABLES are not supported");
		if (t->access != access) {
			if (access && table_has_updates(sql->session->tr, t))
				return sql_message("40000!ALTER TABLE: set READ or INSERT ONLY not possible with outstanding updates (wait until updates are flushed)\n");

			mvc_access(sql, t, access);
		}
	} else {
		return sql_message("42S02!ALTER TABLE: no such table '%s' in schema '%s'", tname, sname);
	}
	return MAL_SUCCEED;
}

static char *
SaveArgReference(MalStkPtr stk, InstrPtr pci, int arg)
{
	char *val = *getArgReference_str(stk, pci, arg);

	if (val && strcmp(val, str_nil) == 0)
		val = NULL;
	return val;
}

str
SQLcatalog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg;
	int type = *getArgReference_int(stk, pci, 1);
	str sname = *getArgReference_str(stk, pci, 2);

	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if (STORE_READONLY)
		return sql_message("25006!schema statements cannot be executed on a readonly database.");

	switch (type) {
	case DDL_CREATE_SEQ:
	{
		sql_sequence *s = *(sql_sequence **) getArgReference(stk, pci, 3);
		msg = create_seq(sql, sname, s);
		break;
	}
	case DDL_ALTER_SEQ:
	{
		lng *val = NULL;
		sql_sequence *s = *(sql_sequence **) getArgReference(stk, pci, 3);
		if (getArgType(mb, pci, 4) == TYPE_lng)
			val = getArgReference_lng(stk, pci, 4);
		if (val == NULL || *val == lng_nil)
			msg = sql_message("42M36!ALTER SEQUENCE: cannot (re)start with NULL");
		else
			msg = alter_seq(sql, sname, s, val);
		break;
	}
	case DDL_DROP_SEQ: {
		str name = *getArgReference_str(stk, pci, 3);

		msg = drop_seq(sql, sname, name);
		break;
	}
	case DDL_CREATE_SCHEMA:{
		str name = SaveArgReference(stk, pci, 3);
		int auth_id = sql->role_id;

		if (name && (auth_id = sql_find_auth(sql, name)) < 0) {
			msg = sql_message("42M32!CREATE SCHEMA: no such authorization '%s'", name);
		}
		if (sql->user_id != USER_MONETDB && sql->role_id != ROLE_SYSADMIN) {
			msg = sql_message("42000!CREATE SCHEMA: insufficient privileges for user '%s'", stack_get_string(sql, "current_user"));
		}
		if (mvc_bind_schema(sql, sname)) {
			msg = sql_message("3F000!CREATE SCHEMA: name '%s' already in use", sname);
		} else {
			(void) mvc_create_schema(sql, sname, auth_id, sql->user_id);
		}
		break;
	}
	case DDL_DROP_SCHEMA:{
		int action = *getArgReference_int(stk, pci, 4);
		sql_schema *s = mvc_bind_schema(sql, sname);

		if (!s) {
			msg = sql_message("3F000!DROP SCHEMA: name %s does not exist", sname);
		} else if (!mvc_schema_privs(sql, s)) {
			msg = sql_message("42000!DROP SCHEMA: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), s->base.name);
		} else if (s == cur_schema(sql)) {
			msg = sql_message("42000!DROP SCHEMA: cannot drop current schema");
		} else if (strcmp(sname, "sys") == 0 || strcmp(sname, "tmp") == 0) {
			msg = sql_message("42000!DROP SCHEMA: access denied for '%s'", sname);
		} else if (sql_schema_has_user(sql, s)) {
			msg = sql_message("2BM37!DROP SCHEMA: unable to drop schema '%s' (there are database objects which depend on it", sname);
		} else {
			mvc_drop_schema(sql, s, action);
		}
		break;
	}
	case DDL_CREATE_TABLE:
	case DDL_CREATE_VIEW:
	{
		sql_table *t = *(sql_table **) getArgReference(stk, pci, 3);
		int temp = *getArgReference_int(stk, pci, 4);

		msg = create_table_or_view(sql, sname, t, temp);
		break;
	}
	case DDL_DROP_TABLE:{
		int action = *getArgReference_int(stk, pci, 4);
		str name = *getArgReference_str(stk, pci, 3);

		msg = drop_table(sql, sname, name, action);
		break;
	}
	case DDL_DROP_VIEW:{
		int action = *getArgReference_int(stk, pci, 4);
		str name = *getArgReference_str(stk, pci, 3);

		msg = drop_view(sql, sname, name, action);
		break;
	}
	case DDL_DROP_CONSTRAINT:{
		int action = *getArgReference_int(stk, pci, 4);
		str name = *getArgReference_str(stk, pci, 3);

		msg = drop_key(sql, sname, name, action);
		break;
	}
	case DDL_ALTER_TABLE:{
		sql_table *t = *(sql_table **) getArgReference(stk, pci, 3);
		msg = alter_table(sql, sname, t);
		break;
	}
	case DDL_CREATE_TYPE:{
		char *name = *getArgReference_str(stk, pci, 3);
		char *impl = *getArgReference_str(stk, pci, 4);
		sql_schema *s = mvc_bind_schema(sql, sname);

		if (!mvc_schema_privs(sql, sql->session->schema)) 
			msg = sql_message("0D000!CREATE TYPE: not enough privileges to create type '%s'", sname);
		if (!mvc_create_type(sql, s, name, 0, 0, 0, impl))
			msg = sql_message("0D000!CREATE TYPE: unknown external type '%s'", impl);
		break;
	}
	case DDL_DROP_TYPE:{
		char *name = *getArgReference_str(stk, pci, 3);
		int drop_action = *getArgReference_int(stk, pci, 4);
		sql_schema *s = mvc_bind_schema(sql, sname);
		sql_type *t = schema_bind_type( sql, s, name);

		if (!t)
			msg = sql_message("0D000!DROP TYPE: type '%s' does not exist", sname);
		else if (!mvc_schema_privs(sql, sql->session->schema)) 
			msg = sql_message("0D000!DROP TYPE: not enough privileges to drop type '%s'", sname);
		else if (!drop_action && mvc_check_dependency(sql, t->base.id, TYPE_DEPENDENCY, NULL))
			return sql_message("42000!DROP TYPE: unable to drop type %s (there are database objects which depend on it)\n", sname);
		else if (!mvc_drop_type(sql, sql->session->schema, t, drop_action))
			msg = sql_message("0D000!DROP TYPE: failed to drop type '%s'", sname);
		break;
	}
	case DDL_GRANT_ROLES:{
		char *auth = SaveArgReference(stk, pci, 3);
		int grantor = *getArgReference_int(stk, pci, 4);
		int admin = *getArgReference_int(stk, pci, 5);

		msg = sql_grant_role(sql, sname /*grantee */ , auth, grantor, admin);
		break;
	}
	case DDL_REVOKE_ROLES:{
		char *auth = SaveArgReference(stk, pci, 3);
		int grantor = *getArgReference_int(stk, pci, 4);
		int admin = *getArgReference_int(stk, pci, 5);

		msg = sql_revoke_role(sql, sname /*grantee */ , auth, grantor, admin);
		break;
	}
	case DDL_GRANT:{
		char *tname = *getArgReference_str(stk, pci, 3);
		char *grantee = *getArgReference_str(stk, pci, 4);
		int privs = *getArgReference_int(stk, pci, 5);
		char *cname = SaveArgReference(stk, pci, 6);
		int grant = *getArgReference_int(stk, pci, 7);
		int grantor = *getArgReference_int(stk, pci, 8);
		if (!tname || strcmp(tname, str_nil) == 0) 
			msg = sql_grant_global_privs(sql, grantee, privs, grant, grantor);
		else
			msg = sql_grant_table_privs(sql, grantee, privs, sname, tname, cname, grant, grantor);
		break;
	}
	case DDL_REVOKE:{
		char *tname = *getArgReference_str(stk, pci, 3);
		char *grantee = *getArgReference_str(stk, pci, 4);
		int privs = *getArgReference_int(stk, pci, 5);
		char *cname = SaveArgReference(stk, pci, 6);
		int grant = *getArgReference_int(stk, pci, 7);
		int grantor = *getArgReference_int(stk, pci, 8);
		if (!tname || strcmp(tname, str_nil) == 0) 
			msg = sql_revoke_global_privs(sql, grantee, privs, grant, grantor);
		else
			msg = sql_revoke_table_privs(sql, grantee, privs, sname, tname, cname, grant, grantor);
		break;
	}
	case DDL_GRANT_FUNC:{
		int func_id = *getArgReference_int(stk, pci, 3);
		char *grantee = *getArgReference_str(stk, pci, 4);
		int privs = *getArgReference_int(stk, pci, 5);
		int grant = *getArgReference_int(stk, pci, 6);
		int grantor = *getArgReference_int(stk, pci, 7);
		msg = sql_grant_func_privs(sql, grantee, privs, sname, func_id, grant, grantor);
		break;
	}
	case DDL_REVOKE_FUNC:{
		int func_id = *getArgReference_int(stk, pci, 3);
		char *grantee = *getArgReference_str(stk, pci, 4);
		int privs = *getArgReference_int(stk, pci, 5);
		int grant = *getArgReference_int(stk, pci, 6);
		int grantor = *getArgReference_int(stk, pci, 7);
		msg = sql_revoke_func_privs(sql, grantee, privs, sname, func_id, grant, grantor);
		break;
	}
	case DDL_CREATE_USER:{
		char *passwd = *getArgReference_str(stk, pci, 3);
		int enc = *getArgReference_int(stk, pci, 4);
		char *schema = SaveArgReference(stk, pci, 5);
		char *fullname = SaveArgReference(stk, pci, 6);
		msg = sql_create_user(sql, sname, passwd, enc, fullname, schema);
		break;
	}
	case DDL_DROP_USER:{
		msg = sql_drop_user(sql, sname);
		break;
	}
	case DDL_ALTER_USER:{
		char *passwd = SaveArgReference(stk, pci, 3);
		int enc = *getArgReference_int(stk, pci, 4);
		char *schema = SaveArgReference(stk, pci, 5);
		char *oldpasswd = SaveArgReference(stk, pci, 6);
		msg = sql_alter_user(sql, sname, passwd, enc, schema, oldpasswd);
		break;
	}
	case DDL_RENAME_USER:{
		char *newuser = *getArgReference_str(stk, pci, 3);
		msg = sql_rename_user(sql, sname, newuser);
		break;
	}
	case DDL_CREATE_ROLE:{
		char *role = sname;
		int grantor = *getArgReference_int(stk, pci, 4);
		msg = sql_create_role(sql, role, grantor);
		break;
	}
	case DDL_DROP_ROLE:{
		char *role = sname;
		msg = sql_drop_role(sql, role);
		break;
	}
	case DDL_DROP_INDEX:{
		char *iname = *getArgReference_str(stk, pci, 3);
		msg = drop_index(sql, sname, iname);
		break;
	}
	case DDL_DROP_FUNCTION:{
		char *fname = *getArgReference_str(stk, pci, 3);
		int fid = *getArgReference_int(stk, pci, 4);
		int type = *getArgReference_int(stk, pci, 5);
		int action = *getArgReference_int(stk, pci, 6);
		msg = drop_func(sql, sname, fname, fid, type, action);
		break;
	}
	case DDL_CREATE_FUNCTION:{
		sql_func *f = *(sql_func **) getArgReference(stk, pci, 3);
		msg = create_func(sql, sname, f);
		break;
	}
	case DDL_CREATE_TRIGGER:{
		char *tname = *getArgReference_str(stk, pci, 3);
		char *triggername = *getArgReference_str(stk, pci, 4);
		int time = *getArgReference_int(stk, pci, 5);
		int orientation = *getArgReference_int(stk, pci, 6);
		int event = *getArgReference_int(stk, pci, 7);
		char *old_name = *getArgReference_str(stk, pci, 8);
		char *new_name = *getArgReference_str(stk, pci, 9);
		char *condition = *getArgReference_str(stk, pci, 10);
		char *query = *getArgReference_str(stk, pci, 11);

		msg = create_trigger(sql, sname, tname, triggername, time, orientation, event, old_name, new_name, condition, query);
		break;
	}
	case DDL_DROP_TRIGGER:{
		char *triggername = *getArgReference_str(stk, pci, 3);

		msg = drop_trigger(sql, sname, triggername);
		break;
	}
	case DDL_ALTER_TABLE_ADD_TABLE:{
		char *mtname = SaveArgReference(stk, pci, 3);
		char *psname = SaveArgReference(stk, pci, 4);
		char *ptname = SaveArgReference(stk, pci, 5);

		return alter_table_add_table(sql, sname, mtname, psname, ptname);
	}
	case DDL_ALTER_TABLE_DEL_TABLE:{
		char *mtname = SaveArgReference(stk, pci, 3);
		char *psname = SaveArgReference(stk, pci, 4);
		char *ptname = SaveArgReference(stk, pci, 5);
		int drop_action = *getArgReference_int(stk, pci, 6);

		return alter_table_del_table(sql, sname, mtname, psname, ptname, drop_action);
	}
	case DDL_ALTER_TABLE_SET_ACCESS:{
		char *tname = SaveArgReference(stk, pci, 3);
		int access = *getArgReference_int(stk, pci, 4);

		return alter_table_set_access(sql, sname, tname, access);
	}
	default:
		throw(SQL, "sql.catalog", "catalog unknown type");
	}
	if (msg)
		return msg;
	return MAL_SUCCEED;
}

/* setVariable(int *ret, str *name, any value) */
str
setVariable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res = getArgReference_int(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	str varname = *getArgReference_str(stk, pci, 2);
	int mtype = getArgType(mb, pci, 3);
	ValRecord *src;
	char buf[BUFSIZ];

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	*res = 0;
	if (mtype < 0 || mtype >= 255)
		throw(SQL, "sql.setVariable", "failed");
	if (strcmp("optimizer", varname) == 0) {
		str newopt = *getArgReference_str(stk, pci, 3);
		if (newopt) {
			if (!isOptimizerPipe(newopt) && strchr(newopt, (int) ';') == 0) {
				snprintf(buf, BUFSIZ, "optimizer '%s' unknown", newopt);
				throw(SQL, "sql.setVariable", "%s", buf);
			}
			snprintf(buf, BUFSIZ, "user_%d", cntxt->idx);
			if (!isOptimizerPipe(newopt) || strcmp(buf, newopt) == 0) {
				msg = addPipeDefinition(cntxt, buf, newopt);
				if (msg)
					return msg;
				if (stack_find_var(m, varname))
					stack_set_string(m, varname, buf);
			} else if (stack_find_var(m, varname))
				stack_set_string(m, varname, newopt);
		}
		return MAL_SUCCEED;
	}
	src = &stk->stk[getArg(pci, 3)];
	if (stack_find_var(m, varname)) {
		lng sgn = val_get_number(src);
		if ((msg = sql_update_var(m, varname, src->val.sval, sgn)) != NULL) {
			snprintf(buf, BUFSIZ, "%s", msg);
			_DELETE(msg);
			throw(SQL, "sql.setVariable", "%s", buf);
		}
		stack_set_var(m, varname, src);
	} else {
		snprintf(buf, BUFSIZ, "variable '%s' unknown", varname);
		throw(SQL, "sql.setVariable", "%s", buf);
	}
	return MAL_SUCCEED;
}

/* getVariable(int *ret, str *name) */
str
getVariable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int mtype = getArgType(mb, pci, 0);
	mvc *m = NULL;
	str msg;
	str varname = *getArgReference_str(stk, pci, 2);
	ValRecord *dst, *src;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (mtype < 0 || mtype >= 255)
		throw(SQL, "sql.getVariable", "failed");
	src = stack_get_var(m, varname);
	if (!src) {
		char buf[BUFSIZ];
		snprintf(buf, BUFSIZ, "variable '%s' unknown", varname);
		throw(SQL, "sql.getVariable", "%s", buf);
	}
	dst = &stk->stk[getArg(pci, 0)];
	VALcopy(dst, src);
	return MAL_SUCCEED;
}

str
sql_variables(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	mvc *m = NULL;
	BAT *vars;
	str msg;
	bat *res = getArgReference_bat(stk, pci, 0);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	vars = BATnew(TYPE_void, TYPE_str, m->topvars, TRANSIENT);
	if (vars == NULL)
		throw(SQL, "sql.variables", MAL_MALLOC_FAIL);
	BATseqbase(vars, 0);
	for (i = 0; i < m->topvars && !m->vars[i].frame; i++)
		BUNappend(vars, m->vars[i].name, FALSE);
	*res = vars->batCacheid;
	BBPkeepref(vars->batCacheid);
	return MAL_SUCCEED;
}

/* str mvc_logfile(int *d, str *filename); */
str
mvc_logfile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	str filename = *getArgReference_str(stk, pci, 1);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (m->scanner.log) {
		close_stream(m->scanner.log);
		m->scanner.log = NULL;
	}

	if (strcmp(filename, str_nil))
		m->scanner.log = open_wastream(filename);
	return MAL_SUCCEED;
}

/* str mvc_next_value(lng *res, str *sname, str *seqname); */
str
mvc_next_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	sql_schema *s;
	lng *res = getArgReference_lng(stk, pci, 0);
	str *sname = getArgReference_str(stk, pci, 1);
	str *seqname = getArgReference_str(stk, pci, 2);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, *sname);
	if (s) {
		sql_sequence *seq = find_sql_sequence(s, *seqname);

		if (seq && seq_next_value(seq, res)) {
			m->last_id = *res;
			stack_set_number(m, "last_id", m->last_id);
			return MAL_SUCCEED;
		}
	}
	throw(SQL, "sql.next_value", "error");
}

/* str mvc_bat_next_value(bat *res, int *sid, str *seqname); */
str
mvc_bat_next_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	BAT *b, *r;
	BUN p, q;
	sql_schema *s = NULL;
	sql_sequence *seq = NULL;
	seqbulk *sb = NULL;
	BATiter bi;
	bat *res = getArgReference_bat(stk, pci, 0);
	bat *sid = getArgReference_bat(stk, pci, 1);
	str *seqname = getArgReference_str(stk, pci, 2);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if ((b = BATdescriptor(*sid)) == NULL)
		throw(SQL, "sql.next_value", "Cannot access descriptor");

	r = BATnew(TYPE_void, TYPE_lng, BATcount(b), TRANSIENT);
	if (!r) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.next_value", "Cannot create bat");
	}
	BATseqbase(r, b->hseqbase);

	if (!BATcount(b)) {
		BBPunfix(b->batCacheid);
		BBPkeepref(r->batCacheid);
		*res = r->batCacheid;
		return MAL_SUCCEED;
	}

	bi = bat_iterator(b);
	BATloop(b, p, q) {
		str sname = BUNtail(bi, BUNfirst(b));
		lng l;

		if (!s || strcmp(s->base.name, sname) != 0) {
			if (sb)
				seqbulk_destroy(sb);
			s = mvc_bind_schema(m, sname);
			seq = NULL;
			if (!s || (seq = find_sql_sequence(s, *seqname)) == NULL || !(sb = seqbulk_create(seq, BATcount(b)))) {
				BBPunfix(b->batCacheid);
				BBPunfix(r->batCacheid);
				throw(SQL, "sql.next_value", "error");
			}
		}
		if (!seqbulk_next_value(sb, &l)) {
			BBPunfix(b->batCacheid);
			BBPunfix(r->batCacheid);
			seqbulk_destroy(sb);
			throw(SQL, "sql.next_value", "error");
		}
		BUNappend(r, &l, FALSE);
	}
	BATseqbase(r, b->hseqbase);
	if (sb)
		seqbulk_destroy(sb);
	BBPunfix(b->batCacheid);
	BBPkeepref(r->batCacheid);
	*res = r->batCacheid;
	return MAL_SUCCEED;
}

/* str mvc_get_value(lng *res, str *sname, str *seqname); */
str
mvc_get_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	sql_schema *s;
	lng *res = getArgReference_lng(stk, pci, 0);
	str *sname = getArgReference_str(stk, pci, 1);
	str *seqname = getArgReference_str(stk, pci, 2);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, *sname);
	if (s) {
		sql_sequence *seq = find_sql_sequence(s, *seqname);

		if (seq && seq_get_value(seq, res))
			return MAL_SUCCEED;
	}
	throw(SQL, "sql.get_value", "error");
}

str
mvc_getVersion(lng *version, const int *clientid)
{
	mvc *m = NULL;
	Client cntxt = MCgetClient(*clientid);
	str msg;

	if ((msg = getSQLContext(cntxt, NULL, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	*version = -1;
	if (m->session->tr)
		*version = m->session->tr->stime;
	return MAL_SUCCEED;
}

/* str mvc_restart_seq(lng *res, str *sname, str *seqname, lng *start); */
str
mvc_restart_seq(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	sql_schema *s;
	lng *res = getArgReference_lng(stk, pci, 0);
	str *sname = getArgReference_str(stk, pci, 1);
	str *seqname = getArgReference_str(stk, pci, 2);
	lng *start = getArgReference_lng(stk, pci, 3);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (*start == lng_nil)
		throw(SQL, "sql.restart", "cannot (re)start with NULL");
	s = mvc_bind_schema(m, *sname);
	if (s) {
		sql_sequence *seq = find_sql_sequence(s, *seqname);

		if (seq) {
			*res = sql_trans_sequence_restart(m->session->tr, seq, *start);
			return MAL_SUCCEED;
		}
	}
	throw(SQL, "sql.restart", "sequence %s not found", *sname);
}

static BAT *
mvc_bind(mvc *m, char *sname, char *tname, char *cname, int access)
{
	sql_trans *tr = m->session->tr;
	BAT *b = NULL;
	sql_schema *s = NULL;
	sql_table *t = NULL;
	sql_column *c = NULL;

	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		return NULL;
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		return NULL;
	c = mvc_bind_column(m, t, cname);
	if (c == NULL)
		return NULL;

	b = store_funcs.bind_col(tr, c, access);
	return b;
}

static BAT *
mvc_bind_dbat(mvc *m, char *sname, char *tname, int access)
{
	sql_trans *tr = m->session->tr;
	BAT *b = NULL;
	sql_schema *s = NULL;
	sql_table *t = NULL;

	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		return NULL;
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		return NULL;

	b = store_funcs.bind_del(tr, t, access);
	return b;
}

BAT *
mvc_bind_idxbat(mvc *m, const char *sname, const char *tname, const char *iname, int access)
{
	sql_trans *tr = m->session->tr;
	BAT *b = NULL;
	sql_schema *s = NULL;
	sql_idx *i = NULL;

	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		return NULL;
	i = mvc_bind_idx(m, s, iname);
	if (i == NULL)
		return NULL;

	(void) tname;
	b = store_funcs.bind_idx(tr, i, access);
	return b;
}

/* str mvc_bind_wrap(int *bid, str *sname, str *tname, str *cname, int *access); */
str
mvc_bind_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int upd = (pci->argc == 7 || pci->argc == 9);
	BAT *b = NULL, *bn;
	bat *bid = getArgReference_bat(stk, pci, 0);
	int coltype = getColumnType(getArgType(mb, pci, 0));
	mvc *m = NULL;
	str msg;
	str *sname = getArgReference_str(stk, pci, 2 + upd);
	str *tname = getArgReference_str(stk, pci, 3 + upd);
	str *cname = getArgReference_str(stk, pci, 4 + upd);
	int *access = getArgReference_int(stk, pci, 5 + upd);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = mvc_bind(m, *sname, *tname, *cname, *access);
	if (b && b->ttype != coltype)
		throw(SQL,"sql.bind","tail type mismatch");
	if (b) {
		if (pci->argc == (8 + upd) && getArgType(mb, pci, 6 + upd) == TYPE_int) {
			BUN cnt = BATcount(b), psz;
			/* partitioned access */
			int part_nr = *getArgReference_int(stk, pci, 6 + upd);
			int nr_parts = *getArgReference_int(stk, pci, 7 + upd);

			if (*access == 0) {
				psz = cnt ? (cnt / nr_parts) : 0;
				bn = BATslice(b, part_nr * psz, (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz));
				BATseqbase(bn, part_nr * psz);
			} else {
				/* BAT b holds the UPD_ID bat */
				oid l, h;
				BAT *c = mvc_bind(m, *sname, *tname, *cname, 0);
				if (c == NULL)
					throw(SQL,"sql.bind","Cannot access the update column");

				cnt = BATcount(c);
				psz = cnt ? (cnt / nr_parts) : 0;
				l = part_nr * psz;
				h = (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz);
				h--;
				bn = BATselect(b, NULL, &l, &h, 1, 1, 0);
				BBPunfix(c->batCacheid);
			}
			BBPunfix(b->batCacheid);
			b = bn;
		} else if (upd) {
			BAT *uv = mvc_bind(m, *sname, *tname, *cname, RD_UPD_VAL);
			bat *uvl = getArgReference_bat(stk, pci, 1);

			if (uv == NULL)
				throw(SQL,"sql.bind","Cannot access the update column");
			BBPkeepref(*bid = b->batCacheid);
			BBPkeepref(*uvl = uv->batCacheid);
			return MAL_SUCCEED;
		}
		if (upd) {
			bat *uvl = getArgReference_bat(stk, pci, 1);

			if (BATcount(b)) {
				BAT *uv = mvc_bind(m, *sname, *tname, *cname, RD_UPD_VAL);
				BAT *ui = mvc_bind(m, *sname, *tname, *cname, RD_UPD_ID);
				BAT *id;
				BAT *vl;
				if (ui == NULL)
					throw(SQL,"sql.bind","Cannot access the insert column");
				if (uv == NULL)
					throw(SQL,"sql.bind","Cannot access the update column");
				id = BATproject(b, ui);
				vl = BATproject(b, uv);
				assert(BATcount(id) == BATcount(vl));
				bat_destroy(ui);
				bat_destroy(uv);
				BBPkeepref(*bid = id->batCacheid);
				BBPkeepref(*uvl = vl->batCacheid);
			} else {
				sql_schema *s = mvc_bind_schema(m, *sname);
				sql_table *t = mvc_bind_table(m, s, *tname);
				sql_column *c = mvc_bind_column(m, t, *cname);

				*bid = e_bat(TYPE_oid);
				*uvl = e_bat(c->type.type->localtype);
			}
			BBPunfix(b->batCacheid);
		} else {
			BBPkeepref(*bid = b->batCacheid);
		}
		return MAL_SUCCEED;
	}
	if (*sname && strcmp(*sname, str_nil) != 0)
		throw(SQL, "sql.bind", "unable to find %s.%s(%s)", *sname, *tname, *cname);
	throw(SQL, "sql.bind", "unable to find %s(%s)", *tname, *cname);
}

/* str mvc_bind_idxbat_wrap(int *bid, str *sname, str *tname, str *iname, int *access); */
str
mvc_bind_idxbat_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int upd = (pci->argc == 7 || pci->argc == 9);
	BAT *b = NULL, *bn;
	bat *bid = getArgReference_bat(stk, pci, 0);
	int coltype = getColumnType(getArgType(mb, pci, 0));
	mvc *m = NULL;
	str msg;
	str *sname = getArgReference_str(stk, pci, 2 + upd);
	str *tname = getArgReference_str(stk, pci, 3 + upd);
	str *iname = getArgReference_str(stk, pci, 4 + upd);
	int *access = getArgReference_int(stk, pci, 5 + upd);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = mvc_bind_idxbat(m, *sname, *tname, *iname, *access);
	if (b && b->ttype != coltype)
		throw(SQL,"sql.bind","tail type mismatch");
	if (b) {
		if (pci->argc == (8 + upd) && getArgType(mb, pci, 6 + upd) == TYPE_int) {
			BUN cnt = BATcount(b), psz;
			/* partitioned access */
			int part_nr = *getArgReference_int(stk, pci, 6 + upd);
			int nr_parts = *getArgReference_int(stk, pci, 7 + upd);

			if (*access == 0) {
				psz = cnt ? (cnt / nr_parts) : 0;
				bn = BATslice(b, part_nr * psz, (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz));
				BATseqbase(bn, part_nr * psz);
			} else {
				/* BAT b holds the UPD_ID bat */
				oid l, h;
				BAT *c = mvc_bind_idxbat(m, *sname, *tname, *iname, 0);
				if ( c == NULL)
					throw(SQL,"sql.bindidx","can not access index column");
				cnt = BATcount(c);
				psz = cnt ? (cnt / nr_parts) : 0;
				l = part_nr * psz;
				h = (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz);
				h--;
				bn = BATselect(b, NULL, &l, &h, 1, 1, 0);
				BBPunfix(c->batCacheid);
			}
			BBPunfix(b->batCacheid);
			b = bn;
		} else if (upd) {
			BAT *uv = mvc_bind_idxbat(m, *sname, *tname, *iname, RD_UPD_VAL);
			bat *uvl = getArgReference_bat(stk, pci, 1);
			if ( uv == NULL)
				throw(SQL,"sql.bindidx","can not access index column");
			BBPkeepref(*bid = b->batCacheid);
			BBPkeepref(*uvl = uv->batCacheid);
			return MAL_SUCCEED;
		}
		if (upd) {
			bat *uvl = getArgReference_bat(stk, pci, 1);

			if (BATcount(b)) {
				BAT *uv = mvc_bind_idxbat(m, *sname, *tname, *iname, RD_UPD_VAL);
				BAT *ui = mvc_bind_idxbat(m, *sname, *tname, *iname, RD_UPD_ID);
				BAT *id, *vl;
				if ( ui == NULL)
					throw(SQL,"sql.bindidx","can not access index column");
				if ( uv == NULL)
					throw(SQL,"sql.bindidx","can not access index column");
				id = BATproject(b, ui);
				vl = BATproject(b, uv);
				assert(BATcount(id) == BATcount(vl));
				bat_destroy(ui);
				bat_destroy(uv);
				BBPkeepref(*bid = id->batCacheid);
				BBPkeepref(*uvl = vl->batCacheid);
			} else {
				sql_schema *s = mvc_bind_schema(m, *sname);
				sql_idx *i = mvc_bind_idx(m, s, *iname);

				*bid = e_bat(TYPE_oid);
				*uvl = e_bat((i->type==join_idx)?TYPE_oid:TYPE_wrd);
			}
			BBPunfix(b->batCacheid);
		} else {
			BBPkeepref(*bid = b->batCacheid);
		}
		return MAL_SUCCEED;
	}
	if (*sname)
		throw(SQL, "sql.idxbind", "unable to find index %s for %s.%s", *iname, *sname, *tname);
	throw(SQL, "sql.idxbind", "unable to find index %s for %s", *iname, *tname);
}

/*mvc_append_wrap(int *bid, str *sname, str *tname, str *cname, ptr d) */
str
mvc_append_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res = getArgReference_int(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 2);
	str tname = *getArgReference_str(stk, pci, 3);
	str cname = *getArgReference_str(stk, pci, 4);
	ptr ins = getArgReference(stk, pci, 5);
	int tpe = getArgType(mb, pci, 5);
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	BAT *b = 0;

	*res = 0;
	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (tpe > GDKatomcnt)
		tpe = TYPE_bat;
	if (tpe == TYPE_bat && (ins = BATdescriptor(*(int *) ins)) == NULL)
		throw(SQL, "sql.append", "Cannot access descriptor");
	if (ATOMextern(tpe))
		ins = *(ptr *) ins;
	if ( tpe == TYPE_bat)
		b =  (BAT*) ins;
	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		throw(SQL, "sql.append", "Schema missing");
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "sql.append", "Table missing");
	if( b && BATcount(b) > 4096 && b->batPersistence == PERSISTENT)
		BATmsync(b);
	if (cname[0] != '%' && (c = mvc_bind_column(m, t, cname)) != NULL) {
		store_funcs.append_col(m->session->tr, c, ins, tpe);
	} else if (cname[0] == '%') {
		sql_idx *i = mvc_bind_idx(m, s, cname + 1);
		if (i)
			store_funcs.append_idx(m->session->tr, i, ins, tpe);
	}
	if (tpe == TYPE_bat) {
		BBPunfix(((BAT *) ins)->batCacheid);
	}
	return MAL_SUCCEED;
}

/*mvc_update_wrap(int *bid, str *sname, str *tname, str *cname, ptr d) */
str
mvc_update_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res = getArgReference_int(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 2);
	str tname = *getArgReference_str(stk, pci, 3);
	str cname = *getArgReference_str(stk, pci, 4);
	bat Tids = *getArgReference_bat(stk, pci, 5);
	bat Upd = *getArgReference_bat(stk, pci, 6);
	BAT *tids, *upd;
	int tpe = getArgType(mb, pci, 6);
	sql_schema *s;
	sql_table *t;
	sql_column *c;

	*res = 0;
	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (tpe > TYPE_any)
		tpe = TYPE_bat;
	else
		assert(0);
	if (tpe != TYPE_bat)
		throw(SQL, "sql.update", "bat expected");
	if ((tids = BATdescriptor(Tids)) == NULL)
		throw(SQL, "sql.update", "Cannot access descriptor");
	if ((upd = BATdescriptor(Upd)) == NULL) {
		BBPunfix(tids->batCacheid);
		throw(SQL, "sql.update", "Cannot access descriptor");
	}
	s = mvc_bind_schema(m, sname);
	if (s == NULL) {
		BBPunfix(tids->batCacheid);
		BBPunfix(upd->batCacheid);
		throw(SQL, "sql.update", "Schema missing");
	}
	t = mvc_bind_table(m, s, tname);
	if (t == NULL) {
		BBPunfix(tids->batCacheid);
		BBPunfix(upd->batCacheid);
		throw(SQL, "sql.update", "Table missing");
	}
	if( upd && BATcount(upd) > 4096 && upd->batPersistence == PERSISTENT)
		BATmsync(upd);
	if( tids && BATcount(tids) > 4096 && tids->batPersistence == PERSISTENT)
		BATmsync(tids);
	if (cname[0] != '%' && (c = mvc_bind_column(m, t, cname)) != NULL) {
		store_funcs.update_col(m->session->tr, c, tids, upd, tpe);
	} else if (cname[0] == '%') {
		sql_idx *i = mvc_bind_idx(m, s, cname + 1);
		if (i)
			store_funcs.update_idx(m->session->tr, i, tids, upd, tpe);
	}
	BBPunfix(tids->batCacheid);
	BBPunfix(upd->batCacheid);
	return MAL_SUCCEED;
}

/* str mvc_clear_table_wrap(wrd *res, str *sname, str *tname); */
str
mvc_clear_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	sql_schema *s;
	sql_table *t;
	mvc *m = NULL;
	str msg;
	wrd *res = getArgReference_wrd(stk, pci, 0);
	str *sname = getArgReference_str(stk, pci, 1);
	str *tname = getArgReference_str(stk, pci, 2);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, *sname);
	if (s == NULL)
		throw(SQL, "sql.clear_table", "3F000!Schema missing");
	t = mvc_bind_table(m, s, *tname);
	if (t == NULL)
		throw(SQL, "sql.clear_table", "42S02!Table missing");
	*res = mvc_clear_table(m, t);
	return MAL_SUCCEED;
}

/*mvc_delete_wrap(int *d, str *sname, str *tname, ptr d) */
str
mvc_delete_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res = getArgReference_int(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	str sname = *getArgReference_str(stk, pci, 2);
	str tname = *getArgReference_str(stk, pci, 3);
	ptr ins = getArgReference(stk, pci, 4);
	int tpe = getArgType(mb, pci, 4);
	BAT *b = NULL;

	sql_schema *s;
	sql_table *t;

	*res = 0;
	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (tpe > TYPE_any)
		tpe = TYPE_bat;
	if (tpe == TYPE_bat && (b = BATdescriptor(*(int *) ins)) == NULL)
		throw(SQL, "sql.delete", "Cannot access descriptor");
	if (tpe != TYPE_bat || (b->ttype != TYPE_oid && b->ttype != TYPE_void))
		throw(SQL, "sql.delete", "Cannot access descriptor");
	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		throw(SQL, "sql.delete", "3F000!Schema missing");
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "sql.delete", "42S02!Table missing");
	if( b && BATcount(b) > 4096 && b->batPersistence == PERSISTENT)
		BATmsync(b);
	store_funcs.delete_tab(m->session->tr, t, b, tpe);
	if (tpe == TYPE_bat)
		BBPunfix(((BAT *) ins)->batCacheid);
	return MAL_SUCCEED;
}

static BAT *
setwritable(BAT *b)
{
	BAT *bn = b;

	if (BATsetaccess(b, BAT_WRITE) != GDK_SUCCEED) {
		if (b->batSharecnt) {
			bn = COLcopy(b, b->ttype, TRUE, TRANSIENT);
			if (bn != NULL)
				BATsetaccess(bn, BAT_WRITE);
		} else {
			bn = NULL;
		}
		BBPunfix(b->batCacheid);
	}
	return bn;
}

str
DELTAbat2(bat *result, const bat *col, const bat *uid, const bat *uval)
{
	return DELTAbat(result, col, uid, uval, NULL);
}

str
DELTAsub2(bat *result, const bat *col, const bat *cid, const bat *uid, const bat *uval)
{
	return DELTAsub(result, col, cid, uid, uval, NULL);
}

str
DELTAproject2(bat *result, const bat *sub, const bat *col, const bat *uid, const bat *uval)
{
	return DELTAproject(result, sub, col, uid, uval, NULL);
}

str
DELTAbat(bat *result, const bat *col, const bat *uid, const bat *uval, const bat *ins)
{
	BAT *c, *u_id, *u_val, *i = NULL, *res;

	if ((u_id = BBPquickdesc(abs(*uid), 0)) == NULL)
		throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
	if (ins && (i = BBPquickdesc(abs(*ins), 0)) == NULL)
		throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);

	/* no updates, no inserts */
	if (BATcount(u_id) == 0 && (!i || BATcount(i) == 0)) {
		BBPincref(*result = *col, TRUE);
		return MAL_SUCCEED;
	}

	if ((c = BBPquickdesc(abs(*col), 0)) == NULL)
		throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);

	/* bat may change */
	if (i && BATcount(c) == 0 && BATcount(u_id) == 0) {
		BBPincref(*result = *ins, TRUE);
		return MAL_SUCCEED;
	}

	c = BATdescriptor(*col);
	if (c == NULL)
		throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
	if ((res = COLcopy(c, c->ttype, TRUE, TRANSIENT)) == NULL) {
		BBPunfix(c->batCacheid);
		throw(MAL, "sql.delta", OPERATION_FAILED);
	}
	BBPunfix(c->batCacheid);

	if ((u_val = BATdescriptor(*uval)) == NULL)
		throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
	u_id = BATdescriptor(*uid);
	assert(BATcount(u_id) == BATcount(u_val));
	if (BATcount(u_id))
		BATreplace(res, u_id, u_val, TRUE);
	BBPunfix(u_id->batCacheid);
	BBPunfix(u_val->batCacheid);

	if (i && BATcount(i)) {
		i = BATdescriptor(*ins);
		BATappend(res, i, TRUE);
		BBPunfix(i->batCacheid);
	}

	if (!(res->batDirty&2)) BATsetaccess(res, BAT_READ);
	BBPkeepref(*result = res->batCacheid);
	return MAL_SUCCEED;
}

str
DELTAsub(bat *result, const bat *col, const bat *cid, const bat *uid, const bat *uval, const bat *ins)
{
	BAT *c, *cminu, *u_id, *u_val, *u, *i = NULL, *res;
	gdk_return ret;

	if ((u_id = BBPquickdesc(abs(*uid), 0)) == NULL)
		throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
	if (ins && (i = BBPquickdesc(abs(*ins), 0)) == NULL)
		throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);

	/* no updates, no inserts */
	if (BATcount(u_id) == 0 && (!i || BATcount(i) == 0)) {
		BBPincref(*result = *col, TRUE);
		return MAL_SUCCEED;
	}

	if ((c = BBPquickdesc(abs(*col), 0)) == NULL)
		throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);

	/* bat may change */
	if (i && BATcount(c) == 0 && BATcount(u_id) == 0) {
		BBPincref(*result = *ins, TRUE);
		return MAL_SUCCEED;
	}

	c = BATdescriptor(*col);
	res = c;
	if (BATcount(u_id)) {
		u_id = BATdescriptor(*uid);
		if (!u_id)
			throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
		cminu = BATdiff(c, u_id, NULL, NULL, 0, BUN_NONE);
		if (!cminu) {
			BBPunfix(u_id->batCacheid);
			throw(MAL, "sql.delta", MAL_MALLOC_FAIL " intermediate");
		}
		res = BATproject(cminu, c);
		BBPunfix(c->batCacheid);
		BBPunfix(cminu->batCacheid);
		if (!res) {
			BBPunfix(u_id->batCacheid);
			throw(MAL, "sql.delta", MAL_MALLOC_FAIL " intermediate" );
		}
		c = res;

		if ((u_val = BATdescriptor(*uval)) == NULL) {
			BBPunfix(c->batCacheid);
			BBPunfix(u_id->batCacheid);
			throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
		}
		u = BATproject(u_val, u_id);
		BBPunfix(u_val->batCacheid);
		BBPunfix(u_id->batCacheid);
		if (!u) {
			BBPunfix(c->batCacheid);
			throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
		}
		if (BATcount(u)) {	/* check selected updated values against candidates */
			BAT *c_ids = BATdescriptor(*cid);
			gdk_return rc;

			if (!c_ids){
				BBPunfix(c->batCacheid);
				BBPunfix(u->batCacheid);
				throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
			}
			rc = BATsemijoin(&cminu, NULL, u, c_ids, NULL, NULL, 0, BUN_NONE);
			BBPunfix(c_ids->batCacheid);
			if (rc != GDK_SUCCEED) {
				BBPunfix(u->batCacheid);
				throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
			}
			c_ids = BATproject(cminu, u);
			BBPunfix(cminu->batCacheid);
			BBPunfix(u->batCacheid);
			u = c_ids;
		}
		BATappend(res, u, TRUE);
		BBPunfix(u->batCacheid);

		ret = BATsort(&u, NULL, NULL, res, NULL, NULL, 0, 0);
		BBPunfix(res->batCacheid);
		if (ret != GDK_SUCCEED) {
			BBPunfix(c->batCacheid);
			throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
		}
		res = u;
	}

	if (i) {
		i = BATdescriptor(*ins);
		if (!i)
			throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
		if (BATcount(u_id)) {
			u_id = BATdescriptor(*uid);
			cminu = BATdiff(i, u_id, NULL, NULL, 0, BUN_NONE);
			BBPunfix(u_id->batCacheid);
			if (!cminu)
				throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
			u_id = BATproject(cminu, i);
			BBPunfix(cminu->batCacheid);
			BBPunfix(i->batCacheid);
			if (!u_id)
				throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
			i = u_id;
		}
		if (isVIEW(res)) {
			BAT *n = COLcopy(res, res->ttype, TRUE, TRANSIENT);
			BBPunfix(res->batCacheid);
			res = n;
			if (res == NULL) {
				BBPunfix(i->batCacheid);
				throw(MAL, "sql.delta", OPERATION_FAILED);
			}
		}
		BATappend(res, i, TRUE);
		BBPunfix(i->batCacheid);

		ret = BATsort(&u, NULL, NULL, res, NULL, NULL, 0, 0);
		BBPunfix(res->batCacheid);
		if (ret != GDK_SUCCEED)
			throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
		res = u;
	}
	BATkey(BATmirror(res), TRUE);
	if (!(res->batDirty&2)) BATsetaccess(res, BAT_READ);
	BBPkeepref(*result = res->batCacheid);
	return MAL_SUCCEED;
}

str
DELTAproject(bat *result, const bat *sub, const bat *col, const bat *uid, const bat *uval, const bat *ins)
{
	BAT *s, *c, *u_id, *u_val, *i = NULL, *res, *tres;

	if ((s = BATdescriptor(*sub)) == NULL)
		throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);

	if (ins && (i = BATdescriptor(*ins)) == NULL) {
		BBPunfix(s->batCacheid);
		throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
	}

	if (i && BATcount(s) == 0) {
		res = BATproject(s, i);
		BBPunfix(s->batCacheid);
		if (i)
			BBPunfix(i->batCacheid);

		BBPkeepref(*result = res->batCacheid);
		return MAL_SUCCEED;
	}

	if ((c = BATdescriptor(*col)) == NULL) {
		BBPunfix(s->batCacheid);
		if (i)
			BBPunfix(i->batCacheid);
		throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
	}

	/* projection(sub,col).union(projection(sub,i)) */
	res = c;
	if (i && BATcount(i)) {
		if (BATcount(c) == 0) {
			res = i;
			i = c;
		} else {
			if ((res = COLcopy(c, c->ttype, TRUE, TRANSIENT)) == NULL)
				throw(MAL, "sql.projectdelta", OPERATION_FAILED);
			BATappend(res, i, FALSE);
			BBPunfix(c->batCacheid);
		}
	}
	if (i)
		BBPunfix(i->batCacheid);

	tres = BATproject(s, res);
	assert(tres);
	BBPunfix(res->batCacheid);
	res = tres;

	if ((u_id = BATdescriptor(*uid)) == NULL) {
		BBPunfix(res->batCacheid);
		BBPunfix(s->batCacheid);
		throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
	}
	if (!BATcount(u_id)) {
		BBPunfix(u_id->batCacheid);
		BBPunfix(s->batCacheid);
		BBPkeepref(*result = res->batCacheid);
		return MAL_SUCCEED;
	}
	if ((u_val = BATdescriptor(*uval)) == NULL) {
		BBPunfix(u_id->batCacheid);
		BBPunfix(res->batCacheid);
		BBPunfix(s->batCacheid);
		throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
	}

	if (BATcount(u_val)) {
		BAT *o, *nu_id, *nu_val;
		/* create subsets of u_id and u_val where the tail
		 * values of u_id are also in s, and where those tail
		 * values occur as head value in res */
		if (BATsemijoin(&o, NULL, u_id, s, NULL, NULL, 0, BUN_NONE) != GDK_SUCCEED) {
			BBPunfix(s->batCacheid);
			BBPunfix(res->batCacheid);
			BBPunfix(u_id->batCacheid);
			BBPunfix(u_val->batCacheid);
			throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
		}
		nu_id = BATproject(o, u_id);
		nu_val = BATproject(o, u_val);
		BBPunfix(u_id->batCacheid);
		BBPunfix(u_val->batCacheid);
		BBPunfix(o->batCacheid);
		tres = BATdense(res->hseqbase, res->hseqbase, BATcount(res));
		if (nu_id == NULL ||
		    nu_val == NULL ||
		    tres == NULL ||
		    BATsemijoin(&o, NULL, nu_id, tres, NULL, NULL, 0, BUN_NONE) != GDK_SUCCEED) {
			BBPunfix(s->batCacheid);
			BBPunfix(res->batCacheid);
			BBPreclaim(nu_id);
			BBPreclaim(nu_val);
			BBPreclaim(tres);
			throw(MAL, "sql.delta", MAL_MALLOC_FAIL);
		}
		BBPunfix(tres->batCacheid);
		u_id = BATproject(o, nu_id);
		u_val = BATproject(o, nu_val);
		BBPunfix(nu_id->batCacheid);
		BBPunfix(nu_val->batCacheid);
		BBPunfix(o->batCacheid);
		if (u_id == NULL || u_val == NULL) {
			BBPunfix(s->batCacheid);
			BBPunfix(res->batCacheid);
			BBPreclaim(u_id);
			BBPreclaim(u_val);
			throw(MAL, "sql.delta", MAL_MALLOC_FAIL);
		}
		/* now update res with the subset of u_id and u_val we
		 * calculated */
		if ((res = setwritable(res)) == NULL ||
		    BATreplace(res, u_id, u_val, 0) != GDK_SUCCEED) {
			if (res)
				BBPunfix(res->batCacheid);
			BBPunfix(s->batCacheid);
			BBPunfix(u_id->batCacheid);
			BBPunfix(u_val->batCacheid);
			throw(MAL, "sql.delta", MAL_MALLOC_FAIL);
		}
	}
	BBPunfix(s->batCacheid);
	BBPunfix(u_id->batCacheid);
	BBPunfix(u_val->batCacheid);

	if (!(res->batDirty&2)) BATsetaccess(res, BAT_READ);
	BBPkeepref(*result = res->batCacheid);
	return MAL_SUCCEED;
}

str
BATleftproject(bat *Res, const bat *Col, const bat *L, const bat *R)
{
	BAT *c, *l, *r, *res;
	oid *p, *lp, *rp;
	BUN cnt = 0, i;

	c = BATdescriptor(*Col);
	if (c)
		cnt = BATcount(c);
	l = BATdescriptor(*L);
	r = BATdescriptor(*R);
	res = BATnew(TYPE_void, TYPE_oid, cnt, TRANSIENT);
	if (!c || !l || !r || !res) {
		if (c)
			BBPunfix(c->batCacheid);
		if (l)
			BBPunfix(l->batCacheid);
		if (r)
			BBPunfix(r->batCacheid);
		if (res)
			BBPunfix(res->batCacheid);
		throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
	}
	p = (oid*)Tloc(res,0);
	for(i=0;i<cnt; i++)
		*p++ = oid_nil;
	BATsetcount(res, cnt);

	cnt = BATcount(l);
	p = (oid*)Tloc(res, 0);
	lp = (oid*)Tloc(l, 0);
	rp = (oid*)Tloc(r, 0);
	if (l->ttype == TYPE_void) {
		oid lp = l->tseqbase;
		if (r->ttype == TYPE_void) {
			oid rp = r->tseqbase;
			for(i=0;i<cnt; i++, lp++, rp++) 
				p[lp] = rp;
		} else {
			for(i=0;i<cnt; i++, lp++) 
				p[lp] = rp[i];
		}
	}
	if (r->ttype == TYPE_void) {
		oid rp = r->tseqbase;
		for(i=0;i<cnt; i++, rp++) 
			p[lp[i]] = rp;
	} else {
		for(i=0;i<cnt; i++) 
			p[lp[i]] = rp[i];
	}
	BATseqbase(res, 0);
	res->T->sorted = 0;
	res->T->revsorted = 0;
	res->T->nil = 0;
	res->T->nonil = 0;
	res->T->key = 0;
	BBPunfix(c->batCacheid);
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	if (!(res->batDirty&2)) BATsetaccess(res, BAT_READ);
	BBPkeepref(*Res = res->batCacheid);
	return MAL_SUCCEED;
}

/* str SQLtid(bat *result, mvc *m, str *sname, str *tname) */
str
SQLtid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	sql_trans *tr;
	str sname = *getArgReference_str(stk, pci, 2);
	str tname = *getArgReference_str(stk, pci, 3);

	sql_schema *s;
	sql_table *t;
	sql_column *c;
	BAT *tids;
	size_t nr, inr = 0;
	oid sb = 0;

	*res = bat_nil;
	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	tr = m->session->tr;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		throw(SQL, "sql.tid", "3F000!Schema missing");
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "sql.tid", "42S02!Table missing");
	c = t->columns.set->h->data;

	nr = store_funcs.count_col(tr, c, 1);

	if (isTable(t) && t->access == TABLE_WRITABLE && (t->base.flag != TR_NEW /* alter */ ) &&
	    t->persistence == SQL_PERSIST && !t->commit_action)
		inr = store_funcs.count_col(tr, c, 0);
	nr -= inr;
	if (pci->argc == 6) {	/* partitioned version */
		size_t cnt = nr;
		int part_nr = *getArgReference_int(stk, pci, 4);
		int nr_parts = *getArgReference_int(stk, pci, 5);

		nr /= nr_parts;
		sb = (oid) (part_nr * nr);
		if (nr_parts == (part_nr + 1)) {	/* last part gets the inserts */
			nr = cnt - (part_nr * nr);	/* keep rest */
			nr += inr;
		}
	} else {
		nr += inr;
	}

	/* create void,void bat with length and oid's set */
	tids = BATnew(TYPE_void, TYPE_void, 0, TRANSIENT);
	if (tids == NULL)
		throw(SQL, "sql.tid", MAL_MALLOC_FAIL);
	BATsetcount(tids, (BUN) nr);
	BATseqbase(tids, sb);
	BATseqbase(BATmirror(tids), sb);

	if (store_funcs.count_del(tr, t)) {
		BAT *d = store_funcs.bind_del(tr, t, RD_INS);
		BAT *diff;
		if( d == NULL)
			throw(SQL,"sql.tid","Can not bind delete column");

		diff = BATdiff(tids, d, NULL, NULL, 0, BUN_NONE);
		BBPunfix(d->batCacheid);
		BBPunfix(tids->batCacheid);
		BATseqbase(diff, sb);
		tids = diff;
	}
	if (!(tids->batDirty&2)) BATsetaccess(tids, BAT_READ);
	BBPkeepref(*res = tids->batCacheid);
	return MAL_SUCCEED;
}

/* unsafe pattern resultSet(tbl:bat[:oid,:str], attr:bat[:oid,:str], tpe:bat[:oid,:str], len:bat[:oid,:int],scale:bat[:oid,:int], cols:bat[:oid,:any]...) :int */
/* New result set rendering infrastructure */

static str
mvc_result_set_wrap( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res_id =getArgReference_int(stk,pci,0);
	bat tblId= *getArgReference_bat(stk, pci,1);
	bat atrId= *getArgReference_bat(stk, pci,2);
	bat tpeId= *getArgReference_bat(stk, pci,3);
	bat lenId= *getArgReference_bat(stk, pci,4);
	bat scaleId= *getArgReference_bat(stk, pci,5);
	bat bid;
	int i,res;
	str tblname, colname, tpename, msg= MAL_SUCCEED;
	int *digits, *scaledigits;
	oid o = 0;
	BATiter itertbl,iteratr,itertpe;
	mvc *m = NULL;
	BAT *b, *tbl, *atr, *tpe,*len,*scale;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	bid = *getArgReference_bat(stk,pci,6);
	b = BATdescriptor(bid);
	if ( b == NULL)
		throw(MAL,"sql.resultset","Failed to access order column");
	res = *res_id = mvc_result_table(m, pci->argc - (pci->retc + 5), 1, b);
	if (res < 0)
		msg = createException(SQL, "sql.resultSet", "failed");
	BBPunfix(b->batCacheid);

	tbl = BATdescriptor(tblId);
	atr = BATdescriptor(atrId);
	tpe = BATdescriptor(tpeId);
	len = BATdescriptor(lenId);
	scale = BATdescriptor(scaleId);
	if( msg || tbl == NULL || atr == NULL || tpe == NULL || len == NULL || scale == NULL)
		goto wrapup_result_set;
	// mimick the old rsColumn approach;
	itertbl = bat_iterator(tbl);
	iteratr = bat_iterator(atr);
	itertpe = bat_iterator(tpe);
	digits = (int*) Tloc(len,BUNfirst(len));
	scaledigits = (int*) Tloc(scale,BUNfirst(scale));

	for( i = 6; msg == MAL_SUCCEED && i< pci->argc; i++, o++){
		bid = *getArgReference_bat(stk,pci,i);
		tblname = BUNtail(itertbl,o);
		colname = BUNtail(iteratr,o);
		tpename = BUNtail(itertpe,o);
		b = BATdescriptor(bid);
		if ( b == NULL)
			msg= createException(MAL,"sql.resultset","Failed to access result column");
		else if (mvc_result_column(m, tblname, colname, tpename, *digits++, *scaledigits++, b))
			msg = createException(SQL, "sql.resultset", "mvc_result_column failed");
		if( b)
			BBPunfix(bid);
	}
	// now sent it to the channel cntxt->fdout
	if (mvc_export_result(cntxt->sqlcontext, cntxt->fdout, res))
		msg = createException(SQL, "sql.resultset", "failed");
  wrapup_result_set:
	if( tbl) BBPunfix(tblId);
	if( atr) BBPunfix(atrId);
	if( tpe) BBPunfix(tpeId);
	if( len) BBPunfix(lenId);
	if( scale) BBPunfix(scaleId);
	return msg;
}







/* Copy the result set into a CSV file */
str
mvc_export_table_wrap( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res_id =getArgReference_int(stk,pci,0);
	str filename = *getArgReference_str(stk,pci,1);
	str format = *getArgReference_str(stk,pci,2);
	unsigned char *tsep = NULL, *rsep = NULL, *ssep = NULL, *ns = NULL;
	unsigned char **T = (unsigned char **) getArgReference_str(stk, pci, 3);
	unsigned char **R = (unsigned char **) getArgReference_str(stk, pci, 4);
	unsigned char **S = (unsigned char **) getArgReference_str(stk, pci, 5);
	unsigned char **N = (unsigned char **) getArgReference_str(stk, pci, 6);

	bat tblId= *getArgReference_bat(stk, pci,7);
	bat atrId= *getArgReference_bat(stk, pci,8);
	bat tpeId= *getArgReference_bat(stk, pci,9);
	bat lenId= *getArgReference_bat(stk, pci,10);
	bat scaleId= *getArgReference_bat(stk, pci,11);
	stream *s;
	bat bid;
	int i,res;
	size_t l;
	str tblname, colname, tpename, msg= MAL_SUCCEED;
	int *digits, *scaledigits;
	oid o = 0;
	BATiter itertbl,iteratr,itertpe;
	mvc *m = NULL;
	BAT *order = NULL, *b = NULL, *tbl = NULL, *atr = NULL, *tpe = NULL,*len = NULL,*scale = NULL;
	res_table *t = NULL;

	(void) format;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	bid = *getArgReference_bat(stk,pci,12);
	order = BATdescriptor(bid);
	if ( order == NULL)
		throw(MAL,"sql.resultset","Failed to access order column");
	res = *res_id = mvc_result_table(m, pci->argc - (pci->retc + 11), 1, order);
	t = m->results;
	if (res < 0){
		msg = createException(SQL, "sql.resultSet", "failed");
		goto wrapup_result_set1;
	}

	l = strlen((char *) (*T));
	GDKstrFromStr(tsep = GDKmalloc(l + 1), *T, l);
	l = 0;
	l = strlen((char *) (*R));
	GDKstrFromStr(rsep = GDKmalloc(l + 1), *R, l);
	l = 0;
	l = strlen((char *) (*S));
	GDKstrFromStr(ssep = GDKmalloc(l + 1), *S, l);
	l = 0;
	l = strlen((char *) (*N));
	GDKstrFromStr(ns = GDKmalloc(l + 1), *N, l);
	t->tsep = (char *) tsep;
	t->rsep = (char *) rsep;
	t->ssep = (char *) ssep;
	t->ns = (char *) ns;

	tbl = BATdescriptor(tblId);
	atr = BATdescriptor(atrId);
	tpe = BATdescriptor(tpeId);
	len = BATdescriptor(lenId);
	scale = BATdescriptor(scaleId);
	if( msg || tbl == NULL || atr == NULL || tpe == NULL || len == NULL || scale == NULL)
		goto wrapup_result_set1;
	// mimick the old rsColumn approach;
	itertbl = bat_iterator(tbl);
	iteratr = bat_iterator(atr);
	itertpe = bat_iterator(tpe);
	digits = (int*) Tloc(len,BUNfirst(len));
	scaledigits = (int*) Tloc(scale,BUNfirst(scale));

	for( i = 12; msg == MAL_SUCCEED && i< pci->argc; i++, o++){
		bid = *getArgReference_bat(stk,pci,i);
		tblname = BUNtail(itertbl,o);
		colname = BUNtail(iteratr,o);
		tpename = BUNtail(itertpe,o);
		b = BATdescriptor(bid);
		if ( b == NULL)
			msg= createException(MAL,"sql.resultset","Failed to access result column");
		else if (mvc_result_column(m, tblname, colname, tpename, *digits++, *scaledigits++, b))
			msg = createException(SQL, "sql.resultset", "mvc_result_column failed");
		if( b)
			BBPunfix(bid);
	}
	// now select the file channel
	if ( strcmp(filename,"stdout") == 0 )
		s= cntxt->fdout;
	else if ( (s = open_wastream(filename)) == NULL || mnstr_errnr(s)) {
		int errnr = mnstr_errnr(s);
		if (s)
			mnstr_destroy(s);
		msg=  createException(IO, "streams.open", "could not open file '%s': %s",
				      filename?filename:"stdout", strerror(errnr));
		goto wrapup_result_set1;
	}
	if (mvc_export_result(cntxt->sqlcontext, s, res))
		msg = createException(SQL, "sql.resultset", "failed");
	if( s != cntxt->fdout)
		mnstr_close(s);
  wrapup_result_set1:
	BBPunfix(order->batCacheid);
	if( tbl) BBPunfix(tblId);
	if( atr) BBPunfix(atrId);
	if( tpe) BBPunfix(tpeId);
	if( len) BBPunfix(lenId);
	if( scale) BBPunfix(scaleId);
	return msg;
}

/* unsafe pattern resultSet(tbl:bat[:oid,:str], attr:bat[:oid,:str], tpe:bat[:oid,:str], len:bat[:oid,:int],scale:bat[:oid,:int], cols:any...) :int */
str
mvc_row_result_wrap( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res_id= getArgReference_int(stk, pci,0);
	bat tblId= *getArgReference_bat(stk, pci,1);
	bat atrId= *getArgReference_bat(stk, pci,2);
	bat tpeId= *getArgReference_bat(stk, pci,3);
	bat lenId= *getArgReference_bat(stk, pci,4);
	bat scaleId= *getArgReference_bat(stk, pci,5);
	int i, res;
	str tblname, colname, tpename, msg= MAL_SUCCEED;
	int *digits, *scaledigits;
	oid o = 0;
	BATiter itertbl,iteratr,itertpe;
	mvc *m = NULL;
//	res_table *t= NULL;
	ptr v;
	int mtype;
	BAT  *tbl, *atr, *tpe,*len,*scale;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
//	m->results = t = res_table_create(m->session->tr, m->result_id++, pci->argc - (pci->retc+5), 1, m->results, NULL);
	res = *res_id = mvc_result_table(m, pci->argc - (pci->retc + 5), 1, NULL);

	tbl = BATdescriptor(tblId);
	atr = BATdescriptor(atrId);
	tpe = BATdescriptor(tpeId);
	len = BATdescriptor(lenId);
	scale = BATdescriptor(scaleId);
	if( tbl == NULL || atr == NULL || tpe == NULL || len == NULL || scale == NULL)
		goto wrapup_result_set;
	// mimick the old rsColumn approach;
	itertbl = bat_iterator(tbl);
	iteratr = bat_iterator(atr);
	itertpe = bat_iterator(tpe);
	digits = (int*) Tloc(len,BUNfirst(len));
	scaledigits = (int*) Tloc(scale,BUNfirst(scale));

	for( i = 6; msg == MAL_SUCCEED && i< pci->argc; i++, o++){
		tblname = BUNtail(itertbl,o);
		colname = BUNtail(iteratr,o);
		tpename = BUNtail(itertpe,o);

		v = getArgReference(stk, pci, i);
		mtype = getArgType(mb, pci, i);
		if (ATOMextern(mtype))
			v = *(ptr *) v;
		if (mvc_result_value(m, tblname, colname, tpename, *digits++, *scaledigits++, v, mtype))
			throw(SQL, "sql.rsColumn", "failed");
	}
//	*res_id = t->id;
	//if (*res_id < 0)
	//msg = createException(SQL, "sql.resultSet", "failed");
	if (mvc_export_result(cntxt->sqlcontext, cntxt->fdout, res))
		msg = createException(SQL, "sql.resultset", "failed");
  wrapup_result_set:
	if( tbl) BBPunfix(tblId);
	if( atr) BBPunfix(atrId);
	if( tpe) BBPunfix(tpeId);
	if( len) BBPunfix(lenId);
	if( scale) BBPunfix(scaleId);
	return msg;
}

str
mvc_export_row_wrap( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res_id= getArgReference_int(stk, pci,0);
	str filename = * getArgReference_str(stk,pci,1);
	str format = *getArgReference_str(stk,pci,2);
	unsigned char *tsep = NULL, *rsep = NULL, *ssep = NULL, *ns = NULL;
	unsigned char **T = (unsigned char **) getArgReference_str(stk, pci, 3);
	unsigned char **R = (unsigned char **) getArgReference_str(stk, pci, 4);
	unsigned char **S = (unsigned char **) getArgReference_str(stk, pci, 5);
	unsigned char **N = (unsigned char **) getArgReference_str(stk, pci, 6);

	bat tblId= *getArgReference_bat(stk, pci,7);
	bat atrId= *getArgReference_bat(stk, pci,8);
	bat tpeId= *getArgReference_bat(stk, pci,9);
	bat lenId= *getArgReference_bat(stk, pci,10);
	bat scaleId= *getArgReference_bat(stk, pci,11);

	size_t l;
	int i, res;
	stream *s;
	str tblname, colname, tpename, msg= MAL_SUCCEED;
	int *digits, *scaledigits;
	oid o = 0;
	BATiter itertbl,iteratr,itertpe;
	mvc *m = NULL;
	res_table *t = NULL;
	ptr v;
	int mtype;
	BAT  *tbl = NULL, *atr = NULL, *tpe = NULL,*len = NULL,*scale = NULL;

	(void) format;
	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	res = *res_id = mvc_result_table(m, pci->argc - (pci->retc + 11), 1, NULL);

	t = m->results;
	if (res < 0){
		msg = createException(SQL, "sql.resultSet", "failed");
		goto wrapup_result_set;
	}

	l = strlen((char *) (*T));
	GDKstrFromStr(tsep = GDKmalloc(l + 1), *T, l);
	l = 0;
	l = strlen((char *) (*R));
	GDKstrFromStr(rsep = GDKmalloc(l + 1), *R, l);
	l = 0;
	l = strlen((char *) (*S));
	GDKstrFromStr(ssep = GDKmalloc(l + 1), *S, l);
	l = 0;
	l = strlen((char *) (*N));
	GDKstrFromStr(ns = GDKmalloc(l + 1), *N, l);
	t->tsep = (char *) tsep;
	t->rsep = (char *) rsep;
	t->ssep = (char *) ssep;
	t->ns = (char *) ns;

	tbl = BATdescriptor(tblId);
	atr = BATdescriptor(atrId);
	tpe = BATdescriptor(tpeId);
	len = BATdescriptor(lenId);
	scale = BATdescriptor(scaleId);
	if( msg || tbl == NULL || atr == NULL || tpe == NULL || len == NULL || scale == NULL)
		goto wrapup_result_set;
	// mimick the old rsColumn approach;
	itertbl = bat_iterator(tbl);
	iteratr = bat_iterator(atr);
	itertpe = bat_iterator(tpe);
	digits = (int*) Tloc(len,BUNfirst(len));
	scaledigits = (int*) Tloc(scale,BUNfirst(scale));

	for( i = 12; msg == MAL_SUCCEED && i< pci->argc; i++, o++){
		tblname = BUNtail(itertbl,o);
		colname = BUNtail(iteratr,o);
		tpename = BUNtail(itertpe,o);

		v = getArgReference(stk, pci, i);
		mtype = getArgType(mb, pci, i);
		if (ATOMextern(mtype))
			v = *(ptr *) v;
		if (mvc_result_value(m, tblname, colname, tpename, *digits++, *scaledigits++, v, mtype))
			throw(SQL, "sql.rsColumn", "failed");
	}
	// now select the file channel
	if ( strcmp(filename,"stdout") == 0 )
		s= cntxt->fdout;
	else if ( (s = open_wastream(filename)) == NULL || mnstr_errnr(s)) {
		int errnr = mnstr_errnr(s);
		if (s)
			mnstr_destroy(s);
		msg=  createException(IO, "streams.open", "could not open file '%s': %s",
				      filename?filename:"stdout", strerror(errnr));
		goto wrapup_result_set;
	}
	if (mvc_export_result(cntxt->sqlcontext, s, res))
		msg = createException(SQL, "sql.resultset", "failed");
	if( s != cntxt->fdout)
		mnstr_close(s);
  wrapup_result_set:
	if( tbl) BBPunfix(tblId);
	if( atr) BBPunfix(atrId);
	if( tpe) BBPunfix(tpeId);
	if( len) BBPunfix(lenId);
	if( scale) BBPunfix(scaleId);
	return msg;
}

str
mvc_table_result_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str res = MAL_SUCCEED;
	BAT *order;
	mvc *m = NULL;
	str msg;
	int *res_id;
	int *nr_cols;
	int *qtype;
	bat *order_bid;

	if ( pci->argc > 6)
		return mvc_result_set_wrap(cntxt,mb,stk,pci);

	res_id = getArgReference_int(stk, pci, 0);
	nr_cols = getArgReference_int(stk, pci, 1);
	qtype = getArgReference_int(stk, pci, 2);
	order_bid = getArgReference_bat(stk, pci, 3);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if ((order = BATdescriptor(*order_bid)) == NULL) {
		throw(SQL, "sql.resultSet", "Cannot access descriptor");
	}
	*res_id = mvc_result_table(m, *nr_cols, *qtype, order);
	if (*res_id < 0)
		res = createException(SQL, "sql.resultSet", "failed");
	BBPunfix(order->batCacheid);
	return res;
}

/* str mvc_declared_table_wrap(int *res_id, str *name); */
str
mvc_declared_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	sql_schema *s = NULL;
	int *res_id = getArgReference_int(stk, pci, 0);
	str *name = getArgReference_str(stk, pci, 1);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, dt_schema);
	if (s == NULL)
		throw(SQL, "sql.declared_table", "3F000!Schema missing");
	(void) mvc_create_table(m, s, *name, tt_table, TRUE, SQL_DECLARED_TABLE, CA_DROP, 0);
	*res_id = 0;
	return MAL_SUCCEED;
}

/* str mvc_declared_table_column_wrap(int *ret, int *rs, str *tname, str *name, str *type, int *digits, int *scale); */
str
mvc_declared_table_column_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	sql_schema *s = NULL;
	sql_table *t = NULL;
	sql_type *type = NULL;
	sql_subtype tpe;
	int *rs = getArgReference_int(stk, pci, 1);
	str *tname = getArgReference_str(stk, pci, 2);
	str *name = getArgReference_str(stk, pci, 3);
	str *typename = getArgReference_str(stk, pci, 4);
	int *digits = getArgReference_int(stk, pci, 5);
	int *scale = getArgReference_int(stk, pci, 6);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (*rs != 0)
		throw(SQL, "sql.dtColumn", "Cannot access declared table");
	if (!sql_find_subtype(&tpe, *typename, *digits, *scale) && (type = mvc_bind_type(m, *typename)) == NULL)
		throw(SQL, "sql.dtColumn", "Cannot find column type");
	if (type)
		sql_init_subtype(&tpe, type, 0, 0);
	s = mvc_bind_schema(m, dt_schema);
	if (s == NULL)
		throw(SQL, "sql.declared_table_column", "3F000!Schema missing");
	t = mvc_bind_table(m, s, *tname);
	if (t == NULL)
		throw(SQL, "sql.declared_table_column", "42S02!Table missing");
	(void) mvc_create_column(m, t, *name, &tpe);
	return MAL_SUCCEED;
}

str
mvc_drop_declared_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str *name = getArgReference_str(stk, pci, 1);
	str msg;
	sql_schema *s = NULL;
	sql_table *t = NULL;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, dt_schema);
	if (s == NULL)
		throw(SQL, "sql.drop", "3F000!Schema missing");
	t = mvc_bind_table(m, s, *name);
	if (t == NULL)
		throw(SQL, "sql.drop", "42S02!Table missing");
	(void) mvc_drop_table(m, s, t, 0);
	return MAL_SUCCEED;
}

str
mvc_drop_declared_tables_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	int i = *getArgReference_int(stk, pci, 1);
	str msg;
	sql_schema *s = NULL;
	sql_table *t = NULL;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, dt_schema);
	if (s == NULL)
		throw(SQL, "sql.drop", "3F000!Schema missing");
	while (i && s->tables.set->t) {
		t = s->tables.set->t->data;
		(void) mvc_drop_table(m, s, t, 0);
		i--;
	}
	return MAL_SUCCEED;
}

/* str mvc_affected_rows_wrap(int *m, int m, wrd *nr, str *w); */
str
mvc_affected_rows_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *b = NULL;
	int *res = getArgReference_int(stk, pci, 0), error;
#ifndef NDEBUG
	int mtype = getArgType(mb, pci, 2);
#endif
	wrd nr;
	str msg;

	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	*res = 0;
	assert(mtype == TYPE_wrd);
	nr = *getArgReference_wrd(stk, pci, 2);
	b = cntxt->sqlcontext;
	error = mvc_export_affrows(b, b->out, nr, "");
	if (error)
		throw(SQL, "sql.affectedRows", "failed");
	return MAL_SUCCEED;
}

/* str mvc_export_head_wrap(int *ret, stream **s, int *res_id); */
str
mvc_export_head_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *b = NULL;
	stream **s = (stream **) getArgReference(stk, pci, 1);
	int *res_id = getArgReference_int(stk, pci, 2);
	str msg;

	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = cntxt->sqlcontext;
	if (mvc_export_head(b, *s, *res_id, FALSE))
		throw(SQL, "sql.exportHead", "failed");
	return MAL_SUCCEED;
}

/* str mvc_export_result_wrap(int *ret, stream **s, int *res_id); */
str
mvc_export_result_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *b = NULL;
	stream **s = (stream **) getArgReference(stk, pci, 1);
	int *res_id = getArgReference_int(stk, pci, 2);
	str msg;

	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = cntxt->sqlcontext;
	if( pci->argc > 5){
		res_id = getArgReference_int(stk, pci, 2);
		if (mvc_export_result(b, cntxt->fdout, *res_id))
			throw(SQL, "sql.exportResult", "failed");
	} else if (mvc_export_result(b, *s, *res_id))
		throw(SQL, "sql.exportResult", "failed");
	return MAL_SUCCEED;
}

/* str mvc_export_chunk_wrap(int *ret, stream **s, int *res_id, str *w); */
str
mvc_export_chunk_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *b = NULL;
	stream **s = (stream **) getArgReference(stk, pci, 1);
	int *res_id = getArgReference_int(stk, pci, 2);
	BUN offset = 0;
	BUN nr = 0;
	str msg;

	(void) mb;		/* NOT USED */
	if (pci->argc == 5) {
		offset = (BUN) *getArgReference_int(stk, pci, 3);
		nr = (BUN) *getArgReference_int(stk, pci, 4);
	}

	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = cntxt->sqlcontext;
	if (mvc_export_chunk(b, *s, *res_id, offset, nr))
		throw(SQL, "sql.exportChunk", "failed");
	return NULL;
}

/* str mvc_export_operation_wrap(int *ret, str *w); */
str
mvc_export_operation_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *b = NULL;
	str msg;

	(void) mb;		/* NOT USED */
	(void) stk;		/* NOT USED */
	(void) pci;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = cntxt->sqlcontext;
	if (mvc_export_operation(b, b->out, ""))
		throw(SQL, "sql.exportOperation", "failed");
	return NULL;
}

str
/*mvc_scalar_value_wrap(int *ret, int *qtype, str tn, str name, str type, int *digits, int *scale, int *eclass, ptr p, int mtype)*/
mvc_scalar_value_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *tn = getArgReference_str(stk, pci, 1);
	str *cn = getArgReference_str(stk, pci, 2);
	str *type = getArgReference_str(stk, pci, 3);
	int *digits = getArgReference_int(stk, pci, 4);
	int *scale = getArgReference_int(stk, pci, 5);
	int *eclass = getArgReference_int(stk, pci, 6);
	ptr p = getArgReference(stk, pci, 7);
	int mtype = getArgType(mb, pci, 7);
	str msg;
	backend *b = NULL;

	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = cntxt->sqlcontext;
	if (ATOMextern(mtype))
		p = *(ptr *) p;
	if (b->out == NULL || mvc_export_value(b, b->out, 1, *tn, *cn, *type, *digits, *scale, *eclass, p, mtype, "", "NULL") != SQL_OK)
		throw(SQL, "sql.exportValue", "failed");
	return MAL_SUCCEED;
}

static void
bat2return(MalStkPtr stk, InstrPtr pci, BAT **b)
{
	int i;

	for (i = 0; i < pci->retc; i++) {
		*getArgReference_bat(stk, pci, i) = b[i]->batCacheid;
		BBPkeepref(b[i]->batCacheid);
	}
}

#ifdef WIN32
static void
fix_windows_newline(unsigned char *s)
{
	char *p = NULL;
	int c = '\r';

	if (s && (p=strchr((char*)s, c)) != NULL && p[1] == '\n') {
		for(; p[1]; p++)
			p[0] = p[1];
		p[0] = 0;
	}
}
#endif

/* str mvc_import_table_wrap(int *res, str *sname, str *tname, unsigned char* *T, unsigned char* *R, unsigned char* *S, unsigned char* *N, str *fname, lng *sz, lng *offset); */
str
mvc_import_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *be;
	BAT **b = NULL;
	unsigned char *tsep = NULL, *rsep = NULL, *ssep = NULL, *ns = NULL;
	ssize_t len = 0;
	sql_table *t = *(sql_table **) getArgReference(stk, pci, pci->retc + 0);
	unsigned char **T = (unsigned char **) getArgReference_str(stk, pci, pci->retc + 1);
	unsigned char **R = (unsigned char **) getArgReference_str(stk, pci, pci->retc + 2);
	unsigned char **S = (unsigned char **) getArgReference_str(stk, pci, pci->retc + 3);
	unsigned char **N = (unsigned char **) getArgReference_str(stk, pci, pci->retc + 4);
	str *fname = getArgReference_str(stk, pci, pci->retc + 5);
	lng *sz = getArgReference_lng(stk, pci, pci->retc + 6);
	lng *offset = getArgReference_lng(stk, pci, pci->retc + 7);
	int *locked = getArgReference_int(stk, pci, pci->retc + 8);
	int *besteffort = getArgReference_int(stk, pci, pci->retc + 9);
	str msg = MAL_SUCCEED;
	bstream *s = NULL;
	stream *ss;

	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	be = cntxt->sqlcontext;
	len = strlen((char *) (*T));
	if ((tsep = GDKmalloc(len + 1)) == NULL)
		throw(MAL, "sql.copy_from", MAL_MALLOC_FAIL);
	GDKstrFromStr(tsep, *T, len);
	len = 0;
	len = strlen((char *) (*R));
	if ((rsep = GDKmalloc(len + 1)) == NULL) {
		GDKfree(tsep);
		throw(MAL, "sql.copy_from", MAL_MALLOC_FAIL);
	}
	GDKstrFromStr(rsep, *R, len);
	len = 0;
	if (*S && strcmp(str_nil, *(char **) S)) {
		len = strlen((char *) (*S));
		if ((ssep = GDKmalloc(len + 1)) == NULL) {
			GDKfree(tsep);
			GDKfree(rsep);
			throw(MAL, "sql.copy_from", MAL_MALLOC_FAIL);
		}
		GDKstrFromStr(ssep, *S, len);
		len = 0;
	}
	len = strlen((char *) (*N));
	if ((ns = GDKmalloc(len + 1)) == NULL) {
		GDKfree(tsep);
		GDKfree(rsep);
		GDKfree(ssep);
		throw(MAL, "sql.copy_from", MAL_MALLOC_FAIL);
	}
	GDKstrFromStr(ns, *N, len);
	len = 0;

	if (!*fname || strcmp(str_nil, *(char **) fname) == 0)
		fname = NULL;
	if (!fname) {
		msg = mvc_import_table(cntxt, &b, be->mvc, be->mvc->scanner.rs, t, (char *) tsep, (char *) rsep, (char *) ssep, (char *) ns, *sz, *offset, *locked, *besteffort);
	} else {
		ss = open_rastream(*fname);
		if (!ss || mnstr_errnr(ss)) {
			int errnr = mnstr_errnr(ss);
			if (ss)
				mnstr_destroy(ss);
			GDKfree(tsep);
			GDKfree(rsep);
			GDKfree(ssep);
			GDKfree(ns);
			msg = createException(IO, "sql.copy_from", "could not open file '%s': %s", *fname, strerror(errnr));
			return msg;
		}
#if SIZEOF_VOID_P == 4
		s = bstream_create(ss, 0x20000);
#else
		s = bstream_create(ss, 0x200000);
#endif
#ifdef WIN32
		fix_windows_newline(tsep);
		fix_windows_newline(rsep);
		fix_windows_newline(ssep);
#endif
		if (s != NULL) {
			msg = mvc_import_table(cntxt, &b, be->mvc, s, t, (char *) tsep, (char *) rsep, (char *) ssep, (char *) ns, *sz, *offset, *locked, *besteffort);
			bstream_destroy(s);
		}
	}
	GDKfree(tsep);
	GDKfree(rsep);
	if (ssep)
		GDKfree(ssep);
	GDKfree(ns);
	if (fname && s == NULL)
		throw(IO, "bstreams.create", "Failed to create block stream");
	if (b == NULL)
		throw(SQL, "importTable", "Failed to import table %s", be->mvc->errstr? be->mvc->errstr:"");
	bat2return(stk, pci, b);
	GDKfree(b);
	return msg;
}

/* str mvc_bin_import_table_wrap(.., str *sname, str *tname, str *fname..);
 * binary attachment only works for simple binary types.
 * Non-simple types require each line to contain a valid ascii representation
 * of the text terminate by a new-line. These strings are passed to the corresponding
 * atom conversion routines to fill the column.
 */
str
mvc_bin_import_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	BUN cnt = 0;
	int i;
	str sname = *getArgReference_str(stk, pci, 0 + pci->retc);
	str tname = *getArgReference_str(stk, pci, 1 + pci->retc);
	sql_schema *s;
	sql_table *t;
	node *n;
	FILE *f;
	char *buf;
	int bufsiz = 128 * BLOCK;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if ((s = mvc_bind_schema(m, sname)) == NULL)
		throw(SQL, "sql.drop", "3F000!Schema missing");
	t = mvc_bind_table(m, s, tname);
	if (!t)
		throw(SQL, "sql", "42S02!table %s not found", tname);
	if (list_length(t->columns.set) != (pci->argc - (2 + pci->retc)))
		throw(SQL, "sql", "Not enough columns in found");

	for (i = pci->retc + 2, n = t->columns.set->h; i < pci->argc && n; i++, n = n->next) {
		sql_column *col = n->data;

		if (ATOMvarsized(col->type.type->localtype) && col->type.type->localtype != TYPE_str)
			throw(SQL, "sql", "Failed to attach file %s", *getArgReference_str(stk, pci, i));
		f = fopen(*getArgReference_str(stk, pci, i), "r");
		if (f == NULL)
			throw(SQL, "sql", "Failed to open file %s", *getArgReference_str(stk, pci, i));
		fclose(f);
	}

	for (i = pci->retc + 2, n = t->columns.set->h; i < pci->argc && n; i++, n = n->next) {
		sql_column *col = n->data;
		BAT *c = NULL;
		int tpe = col->type.type->localtype;

		/* handle the various cases */
		if (tpe < TYPE_str || tpe == TYPE_date || tpe == TYPE_daytime || tpe == TYPE_timestamp) {
			c = BATattach(col->type.type->localtype, *getArgReference_str(stk, pci, i), PERSISTENT);
			if (c == NULL)
				throw(SQL, "sql", "Failed to attach file %s", *getArgReference_str(stk, pci, i));
			BATsetaccess(c, BAT_READ);
			BATderiveProps(c, 0);
		} else if (tpe == TYPE_str) {
			/* get the BAT and fill it with the strings */
			c = BATnew(TYPE_void, TYPE_str, 0, PERSISTENT);
			if (c == NULL)
				throw(SQL, "sql", MAL_MALLOC_FAIL);
			BATseqbase(c, 0);
			/* this code should be extended to deal with larger text strings. */
			f = fopen(*getArgReference_str(stk, pci, i), "r");
			if (f == NULL)
				throw(SQL, "sql", "Failed to re-open file %s", *getArgReference_str(stk, pci, i));

			buf = GDKmalloc(bufsiz);
			if (!buf) {
				fclose(f);
				throw(SQL, "sql", "Failed to create buffer");
			}
			while (fgets(buf, bufsiz, f) != NULL) {
				char *t = strrchr(buf, '\n');
				if (t)
					*t = 0;
				BUNappend(c, buf, FALSE);
			}
			fclose(f);
			GDKfree(buf);
		} else {
			throw(SQL, "sql", "Failed to attach file %s", *getArgReference_str(stk, pci, i));
		}
		if (i != (pci->retc + 2) && cnt != BATcount(c))
			throw(SQL, "sql", "binary files for table '%s' have inconsistent counts", tname);
		cnt = BATcount(c);
		*getArgReference_bat(stk, pci, i - (2 + pci->retc)) = c->batCacheid;
		BBPkeepref(c->batCacheid);
	}
	return MAL_SUCCEED;
}

str
zero_or_one(ptr ret, const bat *bid)
{
	BAT *b;
	BUN c, _s;
	ptr p;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "zero_or_one", "Cannot access descriptor");
	}
	c = BATcount(b);
	if (c == 0) {
		p = ATOMnilptr(b->ttype);
	} else if (c == 1) {
		BATiter bi = bat_iterator(b);
		p = BUNtail(bi, BUNfirst(b));
	} else {
		char buf[BUFSIZ];

		p = NULL;
		snprintf(buf, BUFSIZ, "21000!cardinality violation (" BUNFMT ">1)", c);
		throw(SQL, "zero_or_one", "%s", buf);
	}
	_s = ATOMsize(ATOMtype(b->ttype));
	if (ATOMextern(b->ttype)) {
		_s = ATOMlen(ATOMtype(b->ttype), p);
		memcpy(*(ptr *) ret = GDKmalloc(_s), p, _s);
	} else if (b->ttype == TYPE_bat) {
		bat bid = *(bat *) p;
		*(BAT **) ret = BATdescriptor(bid);
	} else if (_s == 4) {
		*(int *) ret = *(int *) p;
	} else if (_s == 1) {
		*(bte *) ret = *(bte *) p;
	} else if (_s == 2) {
		*(sht *) ret = *(sht *) p;
	} else if (_s == 8) {
		*(lng *) ret = *(lng *) p;
#ifdef HAVE_HGE
	} else if (_s == 16) {
		*(hge *) ret = *(hge *) p;
#endif
	} else {
		memcpy(ret, p, _s);
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
not_unique(bit *ret, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "not_unique", "Cannot access descriptor");
	}

	*ret = FALSE;
	if (BATtkey(b) || BATtdense(b) || BATcount(b) <= 1) {
		BBPunfix(b->batCacheid);
		return MAL_SUCCEED;
	} else if (b->tsorted) {
		BUN p, q;
		oid c = *(oid *) Tloc(b, BUNfirst(b));

		for (p = BUNfirst(b) + 1, q = BUNlast(b); p < q; p++) {
			oid v = *(oid *) Tloc(b, p);
			if (v <= c) {
				*ret = TRUE;
				break;
			}
			c = v;
		}
	} else {
		BBPunfix(b->batCacheid);
		throw(SQL, "not_unique", "input should be sorted");
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

/* row case */
str
SQLidentity(oid *ret, const void *i)
{
	(void)i;
	*ret = 0;
	return MAL_SUCCEED;
}

str
BATSQLidentity(bat *ret, const bat *bid)
{
	return BKCmirror(ret, bid);
}

str
PBATSQLidentity(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0);
	oid *ns = getArgReference_oid(stk, pci, 1);
	bat *bid = getArgReference_bat(stk, pci, 2);
	oid *s = getArgReference_oid(stk, pci, 3);
	BAT *b, *bn = NULL;

	(void) cntxt;
	(void) mb;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "batcalc.identity", RUNTIME_OBJECT_MISSING);
	}
	bn = BATdense(b->hseqbase, *s, BATcount(b));
	if (bn != NULL) {
		*ns = *s + BATcount(b);
		BBPunfix(b->batCacheid);
		BBPkeepref(*res = bn->batCacheid);
		return MAL_SUCCEED;
	}
	BBPunfix(b->batCacheid);
	throw(MAL, "batcalc.identity", GDK_EXCEPTION);

}

/*
 * The core modules of Monet provide just a limited set of
 * mathematical operators. The extensions required to support
 * SQL-99 are shown below. At some point they also should be
 * moved to module code base.
 */

str
daytime_2time_daytime(daytime *res, const daytime *v, const int *digits)
{
	int d = (*digits) ? *digits - 1 : 0;

	/* correct fraction */
	*res = *v;
	if (!daytime_isnil(*v) && d < 3) {
		*res = (daytime) (*res / scales[3 - d]);
		*res = (daytime) (*res * scales[3 - d]);
	}
	return MAL_SUCCEED;
}

str
second_interval_2_daytime(daytime *res, const lng *s, const int *digits)
{
	*res = (daytime) *s;
	return daytime_2time_daytime(res, res, digits);
}

str
nil_2time_daytime(daytime *res, const void *v, const int *digits)
{
	(void) digits;
	(void) v;
	*res = daytime_nil;
	return MAL_SUCCEED;
}

str
str_2time_daytimetz(daytime *res, const str *v, const int *digits, int *tz)
{
	int len = sizeof(daytime), pos;

	if (!*v || strcmp(str_nil, *v) == 0) {
		*res = daytime_nil;
		return MAL_SUCCEED;
	}
	if (*tz)
		pos = daytime_tz_fromstr(*v, &len, &res);
	else
		pos = daytime_fromstr(*v, &len, &res);
	if (!pos || pos < (int)strlen(*v))
		throw(SQL, "daytime", "22007!daytime (%s) has incorrect format", *v);
	return daytime_2time_daytime(res, res, digits);
}

str
str_2time_daytime(daytime *res, const str *v, const int *digits)
{
	int zero = 0;
	return str_2time_daytimetz(res, v, digits, &zero);
}

str
timestamp_2_daytime(daytime *res, const timestamp *v, const int *digits)
{
	int d = (*digits) ? *digits - 1 : 0;
	int msec = v->msecs;

	/* correct fraction */
	if (d < 3 && msec) {
		msec = (int) (msec / scales[3 - d]);
		msec = (int) (msec * scales[3 - d]);
	}
	*res = msec;
	return MAL_SUCCEED;
}

str
date_2_timestamp(timestamp *res, const date *v, const int *digits)
{
	(void) digits;		/* no precision needed */
	res->days = *v;
	res->msecs = 0;
	return MAL_SUCCEED;
}

str
timestamp_2time_timestamp(timestamp *res, const timestamp *v, const int *digits)
{
	int d = (*digits) ? *digits - 1 : 0;

	*res = *v;
	/* correct fraction */
	if (d < 3) {
		int msec = res->msecs;
		if (msec) {
			msec = (int) (msec / scales[3 - d]);
			msec = (int) (msec * scales[3 - d]);
		}
		res->msecs = msec;
	}
	return MAL_SUCCEED;
}

str
nil_2time_timestamp(timestamp *res, const void *v, const int *digits)
{
	(void) digits;
	(void) v;
	*res = *timestamp_nil;
	return MAL_SUCCEED;
}

str
str_2time_timestamptz(timestamp *res, const str *v, const int *digits, int *tz)
{
	int len = sizeof(timestamp), pos;

	if (!*v || strcmp(str_nil, *v) == 0) {
		*res = *timestamp_nil;
		return MAL_SUCCEED;
	}
	if (*tz)
		pos = timestamp_tz_fromstr(*v, &len, &res);
	else
		pos = timestamp_fromstr(*v, &len, &res);
	if (!pos || pos < (int)strlen(*v))
		throw(SQL, "timestamp", "22007!timestamp (%s) has incorrect format", *v);
	return timestamp_2time_timestamp(res, res, digits);
}

str
str_2time_timestamp(timestamp *res, const str *v, const int *digits)
{
	int zero = 0;
	return str_2time_timestamptz(res, v, digits, &zero);
}

str
SQLcst_alpha_cst(dbl *res, const dbl *decl, const dbl *theta)
{
	dbl s, c1, c2;
	char *msg = MAL_SUCCEED;
	if (*decl == dbl_nil || *theta == dbl_nil) {
		*res = dbl_nil;
	} else if (fabs(*decl) + *theta > 89.9) {
		*res = 180.0;
	} else {
		s = sin(radians(*theta));
		c1 = cos(radians(*decl - *theta));
		c2 = cos(radians(*decl + *theta));
		*res = degrees(fabs(atan(s / sqrt(fabs(c1 * c2)))));
	}
	return msg;
}

/*
  sql5_export str SQLcst_alpha_cst(dbl *res, dbl *decl, dbl *theta);
  sql5_export str SQLbat_alpha_cst(bat *res, bat *decl, dbl *theta);
  sql5_export str SQLcst_alpha_bat(bat *res, dbl *decl, bat *theta);
*/
str
SQLbat_alpha_cst(bat *res, const bat *decl, const dbl *theta)
{
	BAT *b, *bn;
	BATiter bi;
	BUN p, q;
	dbl s, c1, c2, r;
	char *msg = NULL;

	if (*theta == dbl_nil) {
		throw(SQL, "SQLbat_alpha", "Parameter theta should not be nil");
	}
	if ((b = BATdescriptor(*decl)) == NULL) {
		throw(SQL, "alpha", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	bn = BATnew(TYPE_void, TYPE_dbl, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.alpha", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	s = sin(radians(*theta));
	BATloop(b, p, q) {
		dbl d = *(dbl *) BUNtail(bi, p);
		if (d == dbl_nil)
			r = dbl_nil;
		else if (fabs(d) + *theta > 89.9)
			r = 180.0;
		else {
			c1 = cos(radians(d - *theta));
			c2 = cos(radians(d + *theta));
			r = degrees(fabs(atan(s / sqrt(fabs(c1 * c2)))));
		}
		BUNappend(bn, &r, FALSE);
	}
	BATseqbase(bn, b->hseqbase);
	*res = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
SQLcst_alpha_bat(bat *res, const dbl *decl, const bat *theta)
{
	BAT *b, *bn;
	BATiter bi;
	BUN p, q;
	dbl s, c1, c2, r;
	char *msg = NULL;

	if ((b = BATdescriptor(*theta)) == NULL) {
		throw(SQL, "alpha", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	bn = BATnew(TYPE_void, TYPE_dbl, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.alpha", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	BATloop(b, p, q) {
		dbl d = *decl;
		dbl *theta = (dbl *) BUNtail(bi, p);

		if (d == dbl_nil)
			r = dbl_nil;
		else if (fabs(d) + *theta > 89.9)
			r = (dbl) 180.0;
		else {
			s = sin(radians(*theta));
			c1 = cos(radians(d - *theta));
			c2 = cos(radians(d + *theta));
			r = degrees(fabs(atan(s / sqrt(fabs(c1 * c2)))));
		}
		BUNappend(bn, &r, FALSE);
	}
	BATseqbase(bn, b->hseqbase);
	BBPkeepref(*res = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
month_interval_str(int *ret, const str *s, const int *d, const int *sk)
{
	lng res;

	if (interval_from_str(*s, *d, *sk, &res) < 0)
		throw(SQL, "calc.month_interval", "wrong format (%s)", *s);
	assert((lng) GDK_int_min <= res && res <= (lng) GDK_int_max);
	*ret = (int) res;
	return MAL_SUCCEED;
}

str
second_interval_str(lng *res, const str *s, const int *d, const int *sk)
{
	if (interval_from_str(*s, *d, *sk, res) < 0)
		throw(SQL, "calc.second_interval", "wrong format (%s)", *s);
	return MAL_SUCCEED;
}

str
month_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = getArgReference_int(stk, pci, 0);
	int k = digits2ek(*getArgReference_int(stk, pci, 2));
	int r;

	(void) cntxt;
	(void) mb;
	switch (getArgType(mb, pci, 1)) {
	case TYPE_bte:
		r = stk->stk[getArg(pci, 1)].val.btval;
		break;
	case TYPE_sht:
		r = stk->stk[getArg(pci, 1)].val.shval;
		break;
	case TYPE_int:
		r = stk->stk[getArg(pci, 1)].val.ival;
		break;
	case TYPE_wrd:
		r = (int) stk->stk[getArg(pci, 1)].val.wval;
		break;
	case TYPE_lng:
		r = (int) stk->stk[getArg(pci, 1)].val.lval;
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		r = (int) stk->stk[getArg(pci, 1)].val.hval;
		break;
#endif
	default:
		throw(ILLARG, "calc.month_interval", "illegal argument");
	}
	switch (k) {
	case iyear:
		r *= 12;
		break;
	case imonth:
		break;
	default:
		throw(ILLARG, "calc.month_interval", "illegal argument");
	}
	*ret = r;
	return MAL_SUCCEED;
}

str
second_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	lng *ret = getArgReference_lng(stk, pci, 0), r;
	int k = digits2ek(*getArgReference_int(stk, pci, 2)), scale = 0;

	(void) cntxt;
	if (pci->argc > 3)
		scale = *getArgReference_int(stk, pci, 3);
	switch (getArgType(mb, pci, 1)) {
	case TYPE_bte:
		r = stk->stk[getArg(pci, 1)].val.btval;
		break;
	case TYPE_sht:
		r = stk->stk[getArg(pci, 1)].val.shval;
		break;
	case TYPE_int:
		r = stk->stk[getArg(pci, 1)].val.ival;
		break;
	case TYPE_wrd:
		r = stk->stk[getArg(pci, 1)].val.wval;
		break;
	case TYPE_lng:
		r = stk->stk[getArg(pci, 1)].val.lval;
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		r = (lng) stk->stk[getArg(pci, 1)].val.hval;
		break;
#endif
	default:
		throw(ILLARG, "calc.sec_interval", "illegal argument");
	}
	switch (k) {
	case iday:
		r *= 24;
		/* fall through */
	case ihour:
		r *= 60;
		/* fall through */
	case imin:
		r *= 60;
		/* fall through */
	case isec:
		r *= 1000;
		break;
	default:
		throw(ILLARG, "calc.sec_interval", "illegal argument");
	}
	if (scale)
		r /= scales[scale];
	*ret = r;
	return MAL_SUCCEED;
}

str
second_interval_daytime(lng *res, const daytime *s, const int *d, const int *sk)
{
	int k = digits2sk(*d);
	lng r = *(int *) s;

	(void) sk;
	if (daytime_isnil(*s)) {
		*res = lng_nil;
		return MAL_SUCCEED;
	}
	switch (k) {
	case isec:
		break;
	case imin:
		r /= 60000;
		r *= 60000;
		break;
	case ihour:
		r /= 3600000;
		r *= 3600000;
		break;
	case iday:
		r /= (24 * 3600000);
		r *= (24 * 3600000);
		break;
	default:
		throw(ILLARG, "calc.second_interval", "illegal argument");
	}
	*res = r;
	return MAL_SUCCEED;
}

str
SQLcurrent_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	daytime t, *res = getArgReference_TYPE(stk, pci, 0, daytime);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;

	if ((msg = MTIMEcurrent_time(&t)) == MAL_SUCCEED) {
		t += m->timezone;
		while (t < 0)
			t += 24*60*60*1000;
		while (t >= 24*60*60*1000)
			t -= 24*60*60*1000;
		*res = t;
	}
	return msg;
}

str
SQLcurrent_timestamp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	timestamp t, *res = getArgReference_TYPE(stk, pci, 0, timestamp);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;

	if ((msg = MTIMEcurrent_timestamp(&t)) == MAL_SUCCEED) {
		lng offset = m->timezone;
		return MTIMEtimestamp_add(res, &t, &offset);
	}
	return msg;
}

/* str dump_cache(int *r); */
str
dump_cache(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	int cnt;
	cq *q = NULL;
	BAT *query, *count;
	bat *rquery = getArgReference_bat(stk, pci, 0);
	bat *rcount = getArgReference_bat(stk, pci, 1);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	cnt = m->qc->id;
	query = BATnew(TYPE_void, TYPE_str, cnt, TRANSIENT);
	if (query == NULL)
		throw(SQL, "sql.dumpcache", MAL_MALLOC_FAIL);
	BATseqbase(query, 0);
	count = BATnew(TYPE_void, TYPE_int, cnt, TRANSIENT);
	if (count == NULL) {
		BBPunfix(query->batCacheid);
		throw(SQL, "sql.dumpcache", MAL_MALLOC_FAIL);
	}
	BATseqbase(count, 0);

	for (q = m->qc->q; q; q = q->next) {
		if (q->type != Q_PREPARE) {
			BUNappend(query, q->codestring, FALSE);
			BUNappend(count, &q->count, FALSE);
		}
	}
	*rquery = query->batCacheid;
	*rcount = count->batCacheid;
	BBPkeepref(*rquery);
	BBPkeepref(*rcount);
	return MAL_SUCCEED;
}

/* str dump_opt_stats(int *r); */
str
dump_opt_stats(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	int cnt;
	BAT *rewrite, *count;
	bat *rrewrite = getArgReference_bat(stk, pci, 0);
	bat *rcount = getArgReference_bat(stk, pci, 1);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	cnt = m->qc->id;
	rewrite = BATnew(TYPE_void, TYPE_str, cnt, TRANSIENT);
	if (rewrite == NULL)
		throw(SQL, "sql.optstats", MAL_MALLOC_FAIL);
	BATseqbase(rewrite, 0);
	count = BATnew(TYPE_void, TYPE_int, cnt, TRANSIENT);
	if (count == NULL)
		throw(SQL, "sql.optstats", MAL_MALLOC_FAIL);
	BATseqbase(count, 0);

	BUNappend(rewrite, "joinidx", FALSE);
	BUNappend(count, &m->opt_stats[0], FALSE);
	/* TODO add other rewrites */

	*rrewrite = rewrite->batCacheid;
	*rcount = count->batCacheid;
	BBPkeepref(*rrewrite);
	BBPkeepref(*rcount);
	return MAL_SUCCEED;
}

/* str dump_opt_stats(int *r); */
str
dump_trace(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	BAT *t[13];
	bat id;

	(void) cntxt;
	(void) mb;
	if (TRACEtable(t) != 13)
		throw(SQL, "sql.dump_trace", "3F000!Profiler not started");
	for(i=0; i< 13; i++){
		id = t[i]->batCacheid;
		*getArgReference_bat(stk, pci, i) = id;
		BBPkeepref(id);
	}
	return MAL_SUCCEED;
}

str
sql_sessions_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return CLTsessions(cntxt, mb, stk, pci);
}

str
sql_querylog_catalog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	BAT *t[8];

	(void) cntxt;
	(void) mb;
	QLOGcatalog(t);
	for (i = 0; i < 8; i++) {
		bat id = t[i]->batCacheid;

		*getArgReference_bat(stk, pci, i) = id;
		BBPkeepref(id);
	}
	return MAL_SUCCEED;
}

str
sql_querylog_calls(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	BAT *t[10];

	(void) cntxt;
	(void) mb;
	QLOGcalls(t);
	for (i = 0; i < 9; i++) {
		bat id = t[i]->batCacheid;

		*getArgReference_bat(stk, pci, i) = id;
		BBPkeepref(id);
	}
	return MAL_SUCCEED;
}

str
sql_querylog_empty(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	QLOGempty(NULL);
	return MAL_SUCCEED;
}

/* str sql_rowid(oid *rid, ptr v, str *sname, str *tname); */
str
sql_rowid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b;
	mvc *m = NULL;
	str msg;
	sql_schema *s = NULL;
	sql_table *t = NULL;
	sql_column *c = NULL;
	sql_delta *d;
	oid *rid = getArgReference_oid(stk, pci, 0);
	str *sname = getArgReference_str(stk, pci, 2);
	str *tname = getArgReference_str(stk, pci, 3);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, *sname);
	if (s == NULL)
		throw(SQL, "sql.rowid", "3F000!Schema missing");
	t = mvc_bind_table(m, s, *tname);
	if (s == NULL)
		throw(SQL, "sql.rowid", "42S02!Table missing");
	if (!s || !t || !t->columns.set->h)
		throw(SQL, "calc.rowid", "42S22!Cannot find column");
	c = t->columns.set->h->data;
	/* HACK, get insert bat */
	b = store_funcs.bind_col(m->session->tr, c, RD_INS);
	if( b == NULL)
		throw(SQL,"sql.rowid","Can not bind to column");
	/* UGH (move into storage backends!!) */
	d = c->data;
	*rid = d->ibase + BATcount(b);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
do_sql_rank_grp(bat *rid, const bat *bid, const bat *gid, int nrank, int dense, const char *name)
{
	BAT *r, *b, *g;
	BUN p, q;
	BATiter bi, gi;
	int (*ocmp) (const void *, const void *);
	int (*gcmp) (const void *, const void *);
	const void *oc, *gc, *on, *gn;
	int rank = 1;
	int c;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(SQL, name, "Cannot access descriptor");
	if ((g = BATdescriptor(*gid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, name, "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	gi = bat_iterator(g);
	ocmp = ATOMcompare(b->ttype);
	gcmp = ATOMcompare(g->ttype);
	oc = BUNtail(bi, BUNfirst(b));
	gc = BUNtail(gi, BUNfirst(g));
	if (!ALIGNsynced(b, g)) {
		BBPunfix(b->batCacheid);
		BBPunfix(g->batCacheid);
		throw(SQL, name, "bats not aligned");
	}
/*
  if (!BATtordered(b)) {
  BBPunfix(b->batCacheid);
  BBPunfix(g->batCacheid);
  throw(SQL, name, "bat not sorted");
  }
*/
	r = BATnew(TYPE_void, TYPE_int, BATcount(b), TRANSIENT);
	if (r == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(g->batCacheid);
		throw(SQL, name, "cannot allocate result bat");
	}
	BATloop(b, p, q) {
		on = BUNtail(bi, p);
		gn = BUNtail(gi, p);

		if ((c = ocmp(on, oc)) != 0)
			rank = nrank;
		if (gcmp(gn, gc) != 0)
			c = rank = nrank = 1;
		oc = on;
		gc = gn;
		BUNappend(r, &rank, FALSE);
		nrank += !dense || c;
	}
	BATseqbase(r, BAThdense(b) ? b->hseqbase : 0);
	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);
	BBPkeepref(*rid = r->batCacheid);
	return MAL_SUCCEED;
}

static str
do_sql_rank(bat *rid, const bat *bid, int nrank, int dense, const char *name)
{
	BAT *r, *b;
	BATiter bi;
	int (*cmp) (const void *, const void *);
	const void *cur, *n;
	BUN p, q;
	int rank = 1;
	int c;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(SQL, name, "Cannot access descriptor");
	if (!BATtordered(b) && !BATtrevordered(b))
		throw(SQL, name, "bat not sorted");

	bi = bat_iterator(b);
	cmp = ATOMcompare(b->ttype);
	cur = BUNtail(bi, BUNfirst(b));
	r = BATnew(TYPE_void, TYPE_int, BATcount(b), TRANSIENT);
	if (r == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, name, "cannot allocate result bat");
	}
	if (BATtdense(b)) {
		BATloop(b, p, q) {
			BUNappend(r, &rank, FALSE);
			rank++;
		}
	} else {
		BATloop(b, p, q) {
			n = BUNtail(bi, p);
			if ((c = cmp(n, cur)) != 0)
				rank = nrank;
			cur = n;
			BUNappend(r, &rank, FALSE);
			nrank += !dense || c;
		}
	}
	BATseqbase(r, BAThdense(b) ? b->hseqbase : 0);
	BBPunfix(b->batCacheid);
	BBPkeepref(*rid = r->batCacheid);
	return MAL_SUCCEED;
}

str
sql_rank_grp(bat *rid, const bat *bid, const bat *gid, const bat *gpe)
{
	(void) gpe;
	return do_sql_rank_grp(rid, bid, gid, 1, 0, "sql.rank_grp");
}

str
sql_dense_rank_grp(bat *rid, const bat *bid, const bat *gid, const bat *gpe)
{
	(void) gpe;
	return do_sql_rank_grp(rid, bid, gid, 2, 1, "sql.dense_rank_grp");
}

str
sql_rank(bat *rid, const bat *bid)
{
	return do_sql_rank(rid, bid, 1, 0, "sql.rank");
}

str
sql_dense_rank(bat *rid, const bat *bid)
{
	return do_sql_rank(rid, bid, 2, 1, "sql.dense_rank");
}

str
SQLargRecord(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str s, t, *ret;

	(void) cntxt;
	ret = getArgReference_str(stk, pci, 0);
	s = instruction2str(mb, stk, getInstrPtr(mb, 0), LIST_MAL_ALL);
	t = strchr(s, ' ');
	*ret = GDKstrdup(t ? t + 1 : s);
	GDKfree(s);
	return MAL_SUCCEED;
}

/*
 * Vacuum cleaning tables
 * Shrinking and re-using space to vacuum clean the holes in the relations.
 */
static str
vacuum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, str (*func) (bat *, const bat *, const bat *), const char *name)
{
	str *sch = getArgReference_str(stk, pci, 1);
	str *tbl = getArgReference_str(stk, pci, 2);
	sql_trans *tr;
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	mvc *m = NULL;
	str msg;
	bat bid;
	BAT *b, *del;
	node *o;
	int i, bids[2049];

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, *sch);
	if (s == NULL)
		throw(SQL, name, "3F000!Schema missing");
	t = mvc_bind_table(m, s, *tbl);
	if (t == NULL)
		throw(SQL, name, "42S02!Table missing");

	if (m->user_id != USER_MONETDB)
		throw(SQL, name, "42000!insufficient privileges");
	if ((!list_empty(t->idxs.set) || !list_empty(t->keys.set)))
		throw(SQL, name, "%s not allowed on tables with indices", name + 4);
	if (has_snapshots(m->session->tr))
		throw(SQL, name, "%s not allowed on snapshots", name + 4);
	if (!m->session->auto_commit)
		throw(SQL, name, "%s only allowed in auto commit mode", name + 4);

	tr = m->session->tr;

	/* get the deletions BAT */
	del = mvc_bind_dbat(m, *sch, *tbl, RD_INS);
	if (BATcount(del) == 0) {
		BBPunfix(del->batCacheid);
		return MAL_SUCCEED;
	}


	i = 0;
	bids[i] = 0;
	for (o = t->columns.set->h; o; o = o->next, i++) {
		c = o->data;
		b = store_funcs.bind_col(tr, c, RDONLY);
		if (b == NULL || (msg = (*func) (&bid, &b->batCacheid, &del->batCacheid)) != NULL) {
			for (i--; i >= 0; i--)
				BBPdecref(bids[i], TRUE);
			if (b)
				BBPunfix(b->batCacheid);
			BBPunfix(del->batCacheid);
			if (!msg)
				throw(SQL, name, "Can not access descriptor");
			return msg;
		}
		BBPunfix(b->batCacheid);
		if (i < 2048) {
			bids[i] = bid;
			bids[i + 1] = 0;
		}
	}
	if (i >= 2048) {
		for (i--; i >= 0; i--)
			BBPdecref(bids[i], TRUE);
		throw(SQL, name, "Too many columns to handle, use copy instead");
	}
	BBPunfix(del->batCacheid);

	mvc_clear_table(m, t);
	for (o = t->columns.set->h, i = 0; o; o = o->next, i++) {
		sql_column *c = o->data;
		BAT *ins = BATdescriptor(bids[i]);	/* use the insert bat */

		if( ins){
			store_funcs.append_col(tr, c, ins, TYPE_bat);
			BBPunfix(ins->batCacheid);
		}
		BBPdecref(bids[i], TRUE);
	}
	/* TODO indices */
	return MAL_SUCCEED;
}

str
SQLshrink(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return vacuum(cntxt, mb, stk, pci, BKCshrinkBAT, "sql.shrink");
}

str
SQLreuse(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return vacuum(cntxt, mb, stk, pci, BKCreuseBAT, "sql.reuse");
}

/*
 * The vacuum operation inspects the table for ordered properties and
 * will keep them.  To avoid expensive shuffles, the reorganisation is
 * balanced by the number of outstanding deletions.
 */
str
SQLvacuum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *sch = getArgReference_str(stk, pci, 1);
	str *tbl = getArgReference_str(stk, pci, 2);
	sql_trans *tr;
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	mvc *m = NULL;
	str msg;
	BAT *b, *del;
	node *o;
	int ordered = 0;
	BUN cnt = 0;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, *sch);
	if (s == NULL)
		throw(SQL, "sql.vacuum", "3F000!Schema missing");
	t = mvc_bind_table(m, s, *tbl);
	if (t == NULL)
		throw(SQL, "sql.vacuum", "42S02!Table missing");

	if (m->user_id != USER_MONETDB)
		throw(SQL, "sql.vacuum", "42000!insufficient privileges");
	if ((!list_empty(t->idxs.set) || !list_empty(t->keys.set)))
		throw(SQL, "sql.vacuum", "vacuum not allowed on tables with indices");
	if (has_snapshots(m->session->tr))
		throw(SQL, "sql.vacuum", "vacuum not allowed on snapshots");

	tr = m->session->tr;

	for (o = t->columns.set->h; o && ordered == 0; o = o->next) {
		c = o->data;
		b = store_funcs.bind_col(tr, c, RDONLY);
		if (b == NULL)
			throw(SQL, "sql.vacuum", "Can not access descriptor");
		ordered |= BATtordered(b);
		cnt = BATcount(b);
		BBPunfix(b->batCacheid);
	}

	/* get the deletions BAT */
	del = mvc_bind_dbat(m, *sch, *tbl, RD_INS);
	if( del == NULL)
		throw(SQL, "sql.vacuum", "Can not access deletion column");

	if (BATcount(del) > 0) {
		/* now decide on the algorithm */
		if (ordered) {
			if (BATcount(del) > cnt / 20)
				SQLshrink(cntxt, mb, stk, pci);
		} else {
			SQLreuse(cntxt, mb, stk, pci);
		}
	}
	BBPunfix(del->batCacheid);
	return MAL_SUCCEED;
}

/*
 * The drop_hash operation cleans up any hash indices on any of the tables columns.
 */
str
SQLdrop_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *sch = getArgReference_str(stk, pci, 1);
	str *tbl = getArgReference_str(stk, pci, 2);
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	mvc *m = NULL;
	str msg;
	BAT *b;
	node *o;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, *sch);
	if (s == NULL)
		throw(SQL, "sql.drop_hash", "3F000!Schema missing");
	t = mvc_bind_table(m, s, *tbl);
	if (t == NULL)
		throw(SQL, "sql.drop_hash", "42S02!Table missing");

	for (o = t->columns.set->h; o; o = o->next) {
		c = o->data;
		b = store_funcs.bind_col(m->session->tr, c, RDONLY);
		if (b == NULL)
			throw(SQL, "sql.drop_hash", "Can not access descriptor");
		HASHdestroy(b);
		BBPunfix(b->batCacheid);
	}
	return MAL_SUCCEED;
}


/* after an update on the optimizer catalog, we have to change
 * the internal optimizer pipe line administration
 * The minimal and default pipelines may not be changed.
 */
str
SQLoptimizersUpdate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	/* find the optimizer pipeline */
	(void) stk;
	(void) pci;
	throw(SQL, "updateOptimizer", PROGRAM_NYI);
}

/*
 * Inspection of the actual storage footprint is a recurring question of users.
 * This is modelled as a generic SQL table producing function.
 * create function storage()
 * returns table ("schema" string, "table" string, "column" string, "type" string, "mode" string, location string, "count" bigint, width int, columnsize bigint, heapsize bigint indices bigint, sorted int)
 * external name sql.storage;
 */
str
sql_storage(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *sch, *tab, *col, *type, *loc, *cnt, *atom, *size, *heap, *indices, *phash, *sort, *imprints, *mode;
	mvc *m = NULL;
	str msg;
	sql_trans *tr;
	node *nsch, *ntab, *ncol;
	int w;
	bit bitval;
	bat *rsch = getArgReference_bat(stk, pci, 0);
	bat *rtab = getArgReference_bat(stk, pci, 1);
	bat *rcol = getArgReference_bat(stk, pci, 2);
	bat *rtype = getArgReference_bat(stk, pci, 3);
	bat *rmode = getArgReference_bat(stk, pci, 4);
	bat *rloc = getArgReference_bat(stk, pci, 5);
	bat *rcnt = getArgReference_bat(stk, pci, 6);
	bat *ratom = getArgReference_bat(stk, pci, 7);
	bat *rsize = getArgReference_bat(stk, pci, 8);
	bat *rheap = getArgReference_bat(stk, pci, 9);
	bat *rindices = getArgReference_bat(stk, pci, 10);
	bat *rphash = getArgReference_bat(stk, pci, 11);
	bat *rimprints = getArgReference_bat(stk, pci, 12);
	bat *rsort = getArgReference_bat(stk, pci, 13);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	tr = m->session->tr;
	sch = BATnew(TYPE_void, TYPE_str, 0, TRANSIENT);
	BATseqbase(sch, 0);
	tab = BATnew(TYPE_void, TYPE_str, 0, TRANSIENT);
	BATseqbase(tab, 0);
	col = BATnew(TYPE_void, TYPE_str, 0, TRANSIENT);
	BATseqbase(col, 0);
	type = BATnew(TYPE_void, TYPE_str, 0, TRANSIENT);
	BATseqbase(type, 0);
	mode = BATnew(TYPE_void, TYPE_str, 0, TRANSIENT);
	BATseqbase(mode, 0);
	loc = BATnew(TYPE_void, TYPE_str, 0, TRANSIENT);
	BATseqbase(loc, 0);
	cnt = BATnew(TYPE_void, TYPE_lng, 0, TRANSIENT);
	BATseqbase(cnt, 0);
	atom = BATnew(TYPE_void, TYPE_int, 0, TRANSIENT);
	BATseqbase(atom, 0);
	size = BATnew(TYPE_void, TYPE_lng, 0, TRANSIENT);
	BATseqbase(size, 0);
	heap = BATnew(TYPE_void, TYPE_lng, 0, TRANSIENT);
	BATseqbase(heap, 0);
	indices = BATnew(TYPE_void, TYPE_lng, 0, TRANSIENT);
	BATseqbase(indices, 0);
	phash = BATnew(TYPE_void, TYPE_bit, 0, TRANSIENT);
	BATseqbase(phash, 0);
	imprints = BATnew(TYPE_void, TYPE_lng, 0, TRANSIENT);
	BATseqbase(imprints, 0);
	sort = BATnew(TYPE_void, TYPE_bit, 0, TRANSIENT);
	BATseqbase(sort, 0);


	if (sch == NULL || tab == NULL || col == NULL || type == NULL || mode == NULL || loc == NULL || imprints == NULL ||
	    sort == NULL || cnt == NULL || atom == NULL || size == NULL || heap == NULL || indices == NULL || phash == NULL) {
		if (sch)
			BBPunfix(sch->batCacheid);
		if (tab)
			BBPunfix(tab->batCacheid);
		if (col)
			BBPunfix(col->batCacheid);
		if (mode)
			BBPunfix(mode->batCacheid);
		if (loc)
			BBPunfix(loc->batCacheid);
		if (cnt)
			BBPunfix(cnt->batCacheid);
		if (type)
			BBPunfix(type->batCacheid);
		if (atom)
			BBPunfix(atom->batCacheid);
		if (size)
			BBPunfix(size->batCacheid);
		if (heap)
			BBPunfix(heap->batCacheid);
		if (indices)
			BBPunfix(indices->batCacheid);
		if (phash)
			BBPunfix(phash->batCacheid);
		if (imprints)
			BBPunfix(imprints->batCacheid);
		if (sort)
			BBPunfix(sort->batCacheid);
		throw(SQL, "sql.storage", MAL_MALLOC_FAIL);
	}
	for (nsch = tr->schemas.set->h; nsch; nsch = nsch->next) {
		sql_base *b = nsch->data;
		sql_schema *s = (sql_schema *) nsch->data;
		if (isalpha((int) b->name[0]))

			if (s->tables.set)
				for (ntab = (s)->tables.set->h; ntab; ntab = ntab->next) {
					sql_base *bt = ntab->data;
					sql_table *t = (sql_table *) bt;
					if (isTable(t))
						if (t->columns.set)
							for (ncol = (t)->columns.set->h; ncol; ncol = ncol->next) {
								sql_base *bc = ncol->data;
								sql_column *c = (sql_column *) ncol->data;
								BAT *bn = store_funcs.bind_col(tr, c, RDONLY);
								lng sz;

								if (bn == NULL)
									throw(SQL, "sql.storage", "Can not access column");

								/*printf("schema %s.%s.%s" , b->name, bt->name, bc->name); */
								BUNappend(sch, b->name, FALSE);
								BUNappend(tab, bt->name, FALSE);
								BUNappend(col, bc->name, FALSE);
								if (c->t->access == TABLE_WRITABLE)
									BUNappend(mode, "writable", FALSE);
								else if (c->t->access == TABLE_APPENDONLY)
									BUNappend(mode, "appendonly", FALSE);
								else if (c->t->access == TABLE_READONLY)
									BUNappend(mode, "readonly", FALSE);
								else
									BUNappend(mode, 0, FALSE);
								BUNappend(type, c->type.type->sqlname, FALSE);

								/*printf(" cnt "BUNFMT, BATcount(bn)); */
								sz = BATcount(bn);
								BUNappend(cnt, &sz, FALSE);

								/*printf(" loc %s", BBP_physical(bn->batCacheid)); */
								BUNappend(loc, BBP_physical(bn->batCacheid), FALSE);
								/*printf(" width %d", bn->T->width); */
								w = bn->T->width;
								if (bn->ttype == TYPE_str) {
									BUN p, q;
									double sum = 0;
									BATiter bi = bat_iterator(bn);
									lng cnt1, cnt2 = cnt1 = (lng) BATcount(bn);

									/* just take a sample */
									if (cnt1 > 512)
										cnt1 = cnt2 = 512;
									BATloop(bn, p, q) {
										str s = BUNtail(bi, p);
										if (s != NULL && strcmp(s, str_nil))
											sum += (int) strlen(s);
										if (--cnt1 <= 0)
											break;
									}
									if (cnt2)
										w = (int) (sum / cnt2);
								}
								BUNappend(atom, &w, FALSE);

								sz = tailsize(bn, BATcount(bn));
								sz += headsize(bn, BATcount(bn));
								BUNappend(size, &sz, FALSE);

								sz = bn->T->vheap ? bn->T->vheap->size : 0;
								sz += bn->H->vheap ? bn->H->vheap->size : 0;
								BUNappend(heap, &sz, FALSE);

								sz = bn->T->hash && bn->T->hash != (Hash *) 1 ? bn->T->hash->heap->size : 0; // HASHsize(bn)
								sz += bn->H->hash && bn->H->hash != (Hash *) 1 ? bn->H->hash->heap->size : 0; // HASHsize(bn)
								BUNappend(indices, &sz, FALSE);
								bitval = 0; // HASHispersistent(bn);
								BUNappend(phash, &bitval, FALSE);

								sz = IMPSimprintsize(bn);
								BUNappend(imprints, &sz, FALSE);
								/*printf(" indices "BUNFMT, bn->T->hash?bn->T->hash->heap->size:0); */
								/*printf("\n"); */

								w = BATtordered(bn);
								BUNappend(sort, &w, FALSE);
								BBPunfix(bn->batCacheid);
							}

					if (isTable(t))
						if (t->idxs.set)
							for (ncol = (t)->idxs.set->h; ncol; ncol = ncol->next) {
								sql_base *bc = ncol->data;
								sql_idx *c = (sql_idx *) ncol->data;
								if (idx_has_column(c->type)) {
									BAT *bn = store_funcs.bind_idx(tr, c, RDONLY);
									lng sz;

									if (bn == NULL)
										throw(SQL, "sql.storage", "Can not access column");
									/*printf("schema %s.%s.%s" , b->name, bt->name, bc->name); */
									BUNappend(sch, b->name, FALSE);
									BUNappend(tab, bt->name, FALSE);
									BUNappend(col, bc->name, FALSE);
									if (c->t->access == TABLE_WRITABLE)
										BUNappend(mode, "writable", FALSE);
									else if (c->t->access == TABLE_APPENDONLY)
										BUNappend(mode, "appendonly", FALSE);
									else if (c->t->access == TABLE_READONLY)
										BUNappend(mode, "readonly", FALSE);
									else
										BUNappend(mode, 0, FALSE);
									BUNappend(type, "oid", FALSE);

									/*printf(" cnt "BUNFMT, BATcount(bn)); */
									sz = BATcount(bn);
									BUNappend(cnt, &sz, FALSE);

									/*printf(" loc %s", BBP_physical(bn->batCacheid)); */
									BUNappend(loc, BBP_physical(bn->batCacheid), FALSE);
									/*printf(" width %d", bn->T->width); */
									w = bn->T->width;
									if (bn->ttype == TYPE_str) {
										BUN p, q;
										double sum = 0;
										BATiter bi = bat_iterator(bn);
										lng cnt1, cnt2 = cnt1 = BATcount(bn);

										/* just take a sample */
										if (cnt1 > 512)
											cnt1 = cnt2 = 512;
										BATloop(bn, p, q) {
											str s = BUNtail(bi, p);
											if (s != NULL && strcmp(s, str_nil))
												sum += (int) strlen(s);
											if (--cnt1 <= 0)
												break;
										}
										if (cnt2)
											w = (int) (sum / cnt2);
									}
									BUNappend(atom, &w, FALSE);
									/*printf(" size "BUNFMT, tailsize(bn,BATcount(bn)) + (bn->T->vheap? bn->T->vheap->size:0)); */
									sz = tailsize(bn, BATcount(bn));
									sz += headsize(bn, BATcount(bn));
									BUNappend(size, &sz, FALSE);

									sz = bn->T->vheap ? bn->T->vheap->size : 0;
									sz += bn->H->vheap ? bn->H->vheap->size : 0;
									BUNappend(heap, &sz, FALSE);

									sz = bn->T->hash && bn->T->hash != (Hash *) 1 ? bn->T->hash->heap->size : 0; // HASHsize()
									sz += bn->H->hash && bn->H->hash != (Hash *) 1 ? bn->H->hash->heap->size : 0; // HASHsize()
									BUNappend(indices, &sz, FALSE);
									bitval = 0; // HASHispersistent(bn);
									BUNappend(phash, &bitval, FALSE);

									sz = IMPSimprintsize(bn);
									BUNappend(imprints, &sz, FALSE);
									/*printf(" indices "BUNFMT, bn->T->hash?bn->T->hash->heap->size:0); */
									/*printf("\n"); */
									w = BATtordered(bn);
									BUNappend(sort, &w, FALSE);
									BBPunfix(bn->batCacheid);
								}
							}

				}
	}

	BBPkeepref(*rsch = sch->batCacheid);
	BBPkeepref(*rtab = tab->batCacheid);
	BBPkeepref(*rcol = col->batCacheid);
	BBPkeepref(*rmode = mode->batCacheid);
	BBPkeepref(*rloc = loc->batCacheid);
	BBPkeepref(*rtype = type->batCacheid);
	BBPkeepref(*rcnt = cnt->batCacheid);
	BBPkeepref(*ratom = atom->batCacheid);
	BBPkeepref(*rsize = size->batCacheid);
	BBPkeepref(*rheap = heap->batCacheid);
	BBPkeepref(*rindices = indices->batCacheid);
	BBPkeepref(*rphash = phash->batCacheid);
	BBPkeepref(*rimprints = imprints->batCacheid);
	BBPkeepref(*rsort = sort->batCacheid);
	return MAL_SUCCEED;
}

str
RAstatement(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int pos = 0;
	str *expr = getArgReference_str(stk, pci, 1);
	bit *opt = getArgReference_bit(stk, pci, 2);
	backend *b = NULL;
	mvc *m = NULL;
	str msg;
	sql_rel *rel;
	list *refs;

	if ((msg = getSQLContext(cntxt, mb, &m, &b)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (!m->sa)
		m->sa = sa_create();
	refs = sa_list(m->sa);
	rel = rel_read(m, *expr, &pos, refs);
	if (rel) {
		int oldvtop = cntxt->curprg->def->vtop;
		int oldstop = cntxt->curprg->def->stop;
		stmt *s;
		MalStkPtr oldglb = cntxt->glb;

		if (*opt)
			rel = rel_optimizer(m, rel);
		s = output_rel_bin(m, rel);
		rel_destroy(rel);

		MSinitClientPrg(cntxt, "user", "test");

		/* generate MAL code */
		backend_callinline(b, cntxt, s, 1);
		addQueryToCache(cntxt);

		msg = (str) runMAL(cntxt, cntxt->curprg->def, 0, 0);
		if (!msg) {
			resetMalBlk(cntxt->curprg->def, oldstop);
			freeVariables(cntxt, cntxt->curprg->def, NULL, oldvtop);
			if( !(cntxt->glb == 0 || cntxt->glb == oldglb))
				msg= createException(MAL,"sql","global stack leakage");	/* detect leak */
		}
		cntxt->glb = oldglb;
	}
	return msg;
}

str
RAstatement2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int pos = 0;
	str *mod = getArgReference_str(stk, pci, 1);
	str *nme = getArgReference_str(stk, pci, 2);
	str *expr = getArgReference_str(stk, pci, 3);
	str *sig = getArgReference_str(stk, pci, 4), c = *sig;
	backend *b = NULL;
	mvc *m = NULL;
	str msg;
	sql_rel *rel;
	list *refs, *ops;
	char buf[BUFSIZ];

	if ((msg = getSQLContext(cntxt, mb, &m, &b)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (!m->sa)
		m->sa = sa_create();

       	ops = sa_list(m->sa);
	//fprintf(stderr, "'%s' %s\n", *sig, *expr);
	//fflush(stderr);
	snprintf(buf, BUFSIZ, "%s %s", *sig, *expr);
	while (c && *c && !isspace(*c)) {
		char *vnme = c, *tnme; 
		char *p = strchr(++c, (int)' ');
		int d,s,nr;
		sql_subtype t;
		atom *a;

		*p++ = 0;
		vnme = sa_strdup(m->sa, vnme);
		nr = strtol(vnme+1, NULL, 10);
		tnme = p;
		p = strchr(p, (int)'(');
		*p++ = 0;
		tnme = sa_strdup(m->sa, tnme);

		d = strtol(p, &p, 10);
		p++; /* skip , */
		s = strtol(p, &p, 10);
		
		sql_find_subtype(&t, tnme, d, s);
		a = atom_general(m->sa, &t, NULL);
		/* the argument list may have holes and maybe out of order, ie
		 * done use sql_add_arg, but special numbered version
		 * sql_set_arg(m, a, nr);
		 * */
		sql_set_arg(m, nr, a);
		append(ops, stmt_alias(m->sa, stmt_varnr(m->sa, nr, &t), NULL, vnme));
		c = strchr(p, (int)',');
		if (c)
			c++;
	}
	//fprintf(stderr, "2: %d %s\n", list_length(ops), *expr);
	//fflush(stderr);
	refs = sa_list(m->sa);
	rel = rel_read(m, *expr, &pos, refs);
	//fprintf(stderr, "3: %d %s\n", list_length(ops), rel2str(m, rel));
	//fflush(stderr);
	if (!rel)
		throw(SQL, "sql.register", "Cannot register %s", buf);
	if (rel) {
		monet5_create_relational_function(m, *mod, *nme, rel, stmt_list(m->sa, ops), 0);
		rel_destroy(rel);
	}
	sqlcleanup(m, 0);
	return msg;
}


void
freeVariables(Client c, MalBlkPtr mb, MalStkPtr glb, int start)
{
	int i;

	for (i = start; i < mb->vtop;) {
		if (glb) {
			if (isVarCleanup(mb, i))
				garbageElement(c, &glb->stk[i]);
			/* clean stack entry */
			glb->stk[i].vtype = TYPE_int;
			glb->stk[i].val.ival = 0;
			glb->stk[i].len = 0;
		}
		clearVariable(mb, i);
		i++;
	}
	mb->vtop = start;
}

/* if at least (2*SIZEOF_BUN), also store length (heaps are then
 * incompatible) */
#define EXTRALEN ((SIZEOF_BUN + GDK_VARALIGN - 1) & ~(GDK_VARALIGN - 1))

str
STRindex_int(int *i, const str *src, const bit *u)
{
	(void)src; (void)u;
	*i = 0;
	return MAL_SUCCEED;
}

str
BATSTRindex_int(bat *res, const bat *src, const bit *u)
{
	BAT *s, *r;

	if ((s = BATdescriptor(*src)) == NULL)
		throw(SQL, "calc.index", "Cannot access descriptor");

	if (*u) {
		Heap *h = s->T->vheap;
		size_t pad, pos;
		const size_t extralen = h->hashash ? EXTRALEN : 0;
		int v;

		r = BATnew(TYPE_void, TYPE_int, 1024, TRANSIENT);
		if (r == NULL) {
			BBPunfix(s->batCacheid);
			throw(SQL, "calc.index", MAL_MALLOC_FAIL);
		}
		BATseqbase(r, 0);
		pos = GDK_STRHASHSIZE;
		while (pos < h->free) {
			const char *s;

			pad = GDK_VARALIGN - (pos & (GDK_VARALIGN - 1));
			if (pad < sizeof(stridx_t))
				pad += GDK_VARALIGN;
			pos += pad + extralen;
			s = h->base + pos;
			v = (int) (pos - GDK_STRHASHSIZE);
			BUNappend(r, &v, FALSE);
			pos += GDK_STRLEN(s);
		}
	} else {
		r = VIEWcreate(s->hseqbase, s);
		if (r == NULL) {
			BBPunfix(s->batCacheid);
			throw(SQL, "calc.index", MAL_MALLOC_FAIL);
		}
		r->ttype = TYPE_int;
		r->tvarsized = 0;
		r->T->vheap = NULL;
	}
	BBPunfix(s->batCacheid);
	BBPkeepref((*res = r->batCacheid));
	return MAL_SUCCEED;
}

str
STRindex_sht(sht *i, const str *src, const bit *u)
{
	(void)src; (void)u;
	*i = 0;
	return MAL_SUCCEED;
}

str
BATSTRindex_sht(bat *res, const bat *src, const bit *u)
{
	BAT *s, *r;

	if ((s = BATdescriptor(*src)) == NULL)
		throw(SQL, "calc.index", "Cannot access descriptor");

	if (*u) {
		Heap *h = s->T->vheap;
		size_t pad, pos;
		const size_t extralen = h->hashash ? EXTRALEN : 0;
		sht v;

		r = BATnew(TYPE_void, TYPE_sht, 1024, TRANSIENT);
		if (r == NULL) {
			BBPunfix(s->batCacheid);
			throw(SQL, "calc.index", MAL_MALLOC_FAIL);
		}
		BATseqbase(r, 0);
		pos = GDK_STRHASHSIZE;
		while (pos < h->free) {
			const char *s;

			pad = GDK_VARALIGN - (pos & (GDK_VARALIGN - 1));
			if (pad < sizeof(stridx_t))
				pad += GDK_VARALIGN;
			pos += pad + extralen;
			s = h->base + pos;
			v = (sht) (pos - GDK_STRHASHSIZE);
			BUNappend(r, &v, FALSE);
			pos += GDK_STRLEN(s);
		}
	} else {
		r = VIEWcreate(s->hseqbase, s);
		if (r == NULL) {
			BBPunfix(s->batCacheid);
			throw(SQL, "calc.index", MAL_MALLOC_FAIL);
		}
		r->ttype = TYPE_sht;
		r->tvarsized = 0;
		r->T->vheap = NULL;
	}
	BBPunfix(s->batCacheid);
	BBPkeepref((*res = r->batCacheid));
	return MAL_SUCCEED;
}

str
STRindex_bte(bte *i, const str *src, const bit *u)
{
	(void)src; (void)u;
	*i = 0;
	return MAL_SUCCEED;
}

str
BATSTRindex_bte(bat *res, const bat *src, const bit *u)
{
	BAT *s, *r;

	if ((s = BATdescriptor(*src)) == NULL)
		throw(SQL, "calc.index", "Cannot access descriptor");

	if (*u) {
		Heap *h = s->T->vheap;
		size_t pad, pos;
		const size_t extralen = h->hashash ? EXTRALEN : 0;
		bte v;

		r = BATnew(TYPE_void, TYPE_bte, 64, TRANSIENT);
		if (r == NULL) {
			BBPunfix(s->batCacheid);
			throw(SQL, "calc.index", MAL_MALLOC_FAIL);
		}
		BATseqbase(r, 0);
		pos = GDK_STRHASHSIZE;
		while (pos < h->free) {
			const char *s;

			pad = GDK_VARALIGN - (pos & (GDK_VARALIGN - 1));
			if (pad < sizeof(stridx_t))
				pad += GDK_VARALIGN;
			pos += pad + extralen;
			s = h->base + pos;
			v = (bte) (pos - GDK_STRHASHSIZE);
			BUNappend(r, &v, FALSE);
			pos += GDK_STRLEN(s);
		}
	} else {
		r = VIEWcreate(s->hseqbase, s);
		if (r == NULL) {
			BBPunfix(s->batCacheid);
			throw(SQL, "calc.index", MAL_MALLOC_FAIL);
		}
		r->ttype = TYPE_bte;
		r->tvarsized = 0;
		r->T->vheap = NULL;
	}
	BBPunfix(s->batCacheid);
	BBPkeepref((*res = r->batCacheid));
	return MAL_SUCCEED;
}

str
STRstrings(str *i, const str *src)
{
	(void)src;
	*i = 0;
	return MAL_SUCCEED;
}

str
BATSTRstrings(bat *res, const bat *src)
{
	BAT *s, *r;
	Heap *h;
	size_t pad, pos;
	size_t extralen;

	if ((s = BATdescriptor(*src)) == NULL)
		throw(SQL, "calc.strings", "Cannot access descriptor");

	h = s->T->vheap;
	extralen = h->hashash ? EXTRALEN : 0;
	r = BATnew(TYPE_void, TYPE_str, 1024, TRANSIENT);
	if (r == NULL) {
		BBPunfix(s->batCacheid);
		throw(SQL, "calc.strings", MAL_MALLOC_FAIL);
	}
	BATseqbase(r, 0);
	pos = GDK_STRHASHSIZE;
	while (pos < h->free) {
		const char *s;

		pad = GDK_VARALIGN - (pos & (GDK_VARALIGN - 1));
		if (pad < sizeof(stridx_t))
			pad += GDK_VARALIGN;
		pos += pad + extralen;
		s = h->base + pos;
		BUNappend(r, s, FALSE);
		pos += GDK_STRLEN(s);
	}
	BBPunfix(s->batCacheid);
	BBPkeepref((*res = r->batCacheid));
	return MAL_SUCCEED;
}

str 
SQLflush_log(void *ret)
{
	(void)ret;
	store_flush_log();
	return MAL_SUCCEED;
}
