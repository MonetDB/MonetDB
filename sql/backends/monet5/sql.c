/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
#include <cluster.h>
#include <opt_pipes.h>
#include "clients.h"
#ifdef HAVE_RAPTOR
# include <rdf.h>
#endif
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

		if (r->card <= CARD_AGGR)
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
/*
		node *n;
		is_point = 1;
		for (n=rel->exps->h; n && is_point; n = n->next) {
			if (!exp_is_point_select(n->data))
				is_point = 0;
		}
*/
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
	int *res = (int *) getArgReference(stk, pci, 0);

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
	int type = *(int *) getArgReference(stk, pci, 1);
	int chain = *(int *) getArgReference(stk, pci, 2);
	str name = *(str *) getArgReference(stk, pci, 3);
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
	str answ = *(str *) getArgReference(stk, pci, 0);

	CLTshutdown(cntxt, mb, stk, pci);

	// administer the shutdown
	mnstr_printf(GDKstdout, "#%s\n", answ);
	return MAL_SUCCEED;
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
	} else if (temp != SQL_DECLARED_TABLE && (!schema_privs(sql->role_id, s) && !(isTempSchema(s) && temp == SQL_LOCAL_TEMP))) {
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
			r = rel_parse(sql, buf, m_deps);
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
		mvc_copy_column(sql, nt, c);
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
		r = rel_parse(sql, nt->query, m_deps);
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
		BAT *b = store_funcs.bind_col(tr, c, RD_UPD);
		cnt |= BATcount(b) > 0;
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

	} else if (!schema_privs(sql->role_id, s) && !(isTempSchema(s) && t->persistence == SQL_LOCAL_TEMP)) {
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

	if (t->readonly != nt->readonly) {
		if (t->readonly && table_has_updates(sql->session->tr, nt)) 
			return sql_message("40000!ALTER TABLE: set READONLY not possible with outstanding updates (wait until updates are flushed)\n");
		mvc_readonly(sql, nt, t->readonly);
	}

	/* check for changes */
	if (t->tables.dset)
		for (n = t->tables.dset->h; n; n = n->next) {
			/* propagate alter table .. drop table */
			sql_table *at = n->data;
			sql_table *pt = mvc_bind_table(sql, nt->s, at->base.name);

			sql_trans_del_table(sql->session->tr, nt, pt, at->drop_action);
		}
	for (n = t->tables.nelm; n; n = n->next) {
		/* propagate alter table .. add table */
		sql_table *at = n->data;
		sql_table *pt = mvc_bind_table(sql, nt->s, at->base.name);

		sql_trans_add_table(sql->session->tr, nt, pt);
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

		if (c->null != nc->null) {
			mvc_null(sql, nc, c->null);
			/* for non empty check for nulls */
			if (c->null == 0) {
				void *nilptr = ATOMnilptr(c->type.type->localtype);
				if (table_funcs.column_find_row(sql->session->tr, nc, nilptr, NULL) != oid_nil)
					return sql_message("40002!ALTER TABLE: NOT NULL constraint violated for column %s.%s", c->t->base.name, c->base.name);
			}
		}
		if (c->def != nc->def)
			mvc_default(sql, nc, c->def);
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
	} else if (!schema_privs(sql->role_id, s) && !(isTempSchema(s) && t->persistence == SQL_LOCAL_TEMP)) {
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

	if (!schema_privs(sql->role_id, ss) && !(isTempSchema(ss) && t && t->persistence == SQL_LOCAL_TEMP)) {
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
	} else if (!schema_privs(sql->role_id, s)) {
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
	} else if (!schema_privs(sql->role_id, s)) {
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
	} else if (!schema_privs(sql->role_id, s)) {
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
	} else if (!schema_privs(sql->role_id, s)) {
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

			if (!schema_privs(sql->role_id, s)) {
				return sql_message("DROP %s%s: access denied for %s to schema ;'%s'", KF, F, stack_get_string(sql, "current_user"), s->base.name);
			}
			if (!action && mvc_check_dependency(sql, func->base.id, !IS_PROC(func) ? FUNC_DEPENDENCY : PROC_DEPENDENCY, NULL))
				 return sql_message("DROP %s%s: there are database objects dependent on %s%s %s;", KF, F, kf, f, func->base.name);

			mvc_drop_func(sql, s, func, action);
		}
	} else {
		node *n = NULL;
		list *list_func = schema_bind_func(sql, s, name, type);

		if (!schema_privs(sql->role_id, s)) {
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
	nf = mvc_create_func(sql, NULL, s, f->base.name, f->ops, f->res, f->type, f->mod, f->imp, f->query, f->varres, f->vararg);
	if (nf && nf->query) {
		char *buf;
		sql_rel *r = NULL;
		sql_allocator *sa = sql->sa;

		sql->sa = sa_create();
		buf = sa_strdup(sql->sa, nf->query);
		r = rel_parse(sql, buf, m_deps);
		if (r)
			r = rel_optimizer(sql, r);
		if (r) {
			stmt *sb = rel_bin(sql, r);
			list *id_col_l = stmt_list_dependencies(sql->sa, sb, COLUMN_DEPENDENCY);
			list *id_func_l = stmt_list_dependencies(sql->sa, sb, FUNC_DEPENDENCY);
			list *view_id_l = stmt_list_dependencies(sql->sa, sb, VIEW_DEPENDENCY);

			mvc_create_dependencies(sql, id_col_l, nf->base.id, !IS_PROC(f) ? FUNC_DEPENDENCY : PROC_DEPENDENCY);
			mvc_create_dependencies(sql, id_func_l, nf->base.id, !IS_PROC(f) ? FUNC_DEPENDENCY : PROC_DEPENDENCY);
			mvc_create_dependencies(sql, view_id_l, nf->base.id, !IS_PROC(f) ? FUNC_DEPENDENCY : PROC_DEPENDENCY);
		}
		sa_destroy(sql->sa);
		sql->sa = sa;
	} else {
		if (!backend_resolve_function(sql, nf))
			return sql_message("3F000!CREATE %s%s: external name %s.%s not bound", KF, F, nf->mod, nf->base.name);
	}
	return MAL_SUCCEED;
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
	if (!schema_privs(sql->role_id, s))
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
		r = rel_parse(sql, buf, m_deps);
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
	if (s && !schema_privs(sql->role_id, s))
		return sql_message("3F000!DROP TRIGGER: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), s->base.name);

	if ((tri = mvc_bind_trigger(sql, s, tname)) == NULL)
		return sql_message("3F000!DROP TRIGGER: unknown trigger %s\n", tname);
	mvc_drop_trigger(sql, s, tri);
	return MAL_SUCCEED;
}

static char *
SaveArgReference(MalStkPtr stk, InstrPtr pci, int arg)
{
	char *val = *(str *) getArgReference(stk, pci, arg);

	if (val && strcmp(val, str_nil) == 0)
		val = NULL;
	return val;
}

str
SQLcatalog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg;
	int type = *(int *) getArgReference(stk, pci, 1);
	str sname = *(str *) getArgReference(stk, pci, 2);

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
			val = getArgReference(stk, pci, 4);
		if (val == NULL || *val == lng_nil)
			msg = sql_message("42M36!ALTER SEQUENCE: cannot (re)start with NULL");
		else
			msg = alter_seq(sql, sname, s, val);
		break;
	}
	case DDL_DROP_SEQ: {
		str name = *(str *) getArgReference(stk, pci, 3);

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
		int action = *(int *) getArgReference(stk, pci, 4);
		sql_schema *s = mvc_bind_schema(sql, sname);

		if (!s) {
			msg = sql_message("3F000!DROP SCHEMA: name %s does not exist", sname);
		} else if (!schema_privs(sql->role_id, s)) {
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
		int temp = *(int *) getArgReference(stk, pci, 4);

		msg = create_table_or_view(sql, sname, t, temp);
		break;
	}
	case DDL_DROP_TABLE:{
		int action = *(int *) getArgReference(stk, pci, 4);
		str name = *(str *) getArgReference(stk, pci, 3);

		msg = drop_table(sql, sname, name, action);
		break;
	}
	case DDL_DROP_VIEW:{
		int action = *(int *) getArgReference(stk, pci, 4);
		str name = *(str *) getArgReference(stk, pci, 3);

		msg = drop_view(sql, sname, name, action);
		break;
	}
	case DDL_DROP_CONSTRAINT:{
		int action = *(int *) getArgReference(stk, pci, 4);
		str name = *(str *) getArgReference(stk, pci, 3);

		msg = drop_key(sql, sname, name, action);
		break;
	}
	case DDL_ALTER_TABLE:{
		sql_table *t = *(sql_table **) getArgReference(stk, pci, 3);
		msg = alter_table(sql, sname, t);
		break;
	}
	case DDL_CREATE_TYPE:{
		char *impl = *(str *) getArgReference(stk, pci, 3);
		if (!mvc_create_type(sql, sql->session->schema, sname, 0, 0, 0, impl))
			msg = sql_message("0D000!CREATE TYPE: unknown external type '%s'", impl);
		break;
	}
	case DDL_DROP_TYPE:{
		msg = sql_message("0A000!DROP TYPE: not implemented ('%s')", sname);
		break;
	}
	case DDL_GRANT_ROLES:{
		char *auth = SaveArgReference(stk, pci, 3);

		msg = sql_grant_role(sql, sname /*grantee */ , auth);
		break;
	}
	case DDL_REVOKE_ROLES:{
		char *auth = SaveArgReference(stk, pci, 3);

		msg = sql_revoke_role(sql, sname /*grantee */ , auth);
		break;
	}
	case DDL_GRANT:{
		char *tname = *(str *) getArgReference(stk, pci, 3);
		char *grantee = *(str *) getArgReference(stk, pci, 4);
		int privs = *(int *) getArgReference(stk, pci, 5);
		char *cname = SaveArgReference(stk, pci, 6);
		int grant = *(int *) getArgReference(stk, pci, 7);
		int grantor = *(int *) getArgReference(stk, pci, 8);
		msg = sql_grant_table_privs(sql, grantee, privs, sname, tname, cname, grant, grantor);
		break;
	}
	case DDL_REVOKE:{
		char *tname = *(str *) getArgReference(stk, pci, 3);
		char *grantee = *(str *) getArgReference(stk, pci, 4);
		int privs = *(int *) getArgReference(stk, pci, 5);
		char *cname = SaveArgReference(stk, pci, 6);
		int grant = *(int *) getArgReference(stk, pci, 7);
		int grantor = *(int *) getArgReference(stk, pci, 8);
		msg = sql_revoke_table_privs(sql, grantee, privs, sname, tname, cname, grant, grantor);
		break;
	}
	case DDL_CREATE_USER:{
		char *passwd = *(str *) getArgReference(stk, pci, 3);
		int enc = *(int *) getArgReference(stk, pci, 4);
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
		int enc = *(int *) getArgReference(stk, pci, 4);
		char *schema = SaveArgReference(stk, pci, 5);
		char *oldpasswd = SaveArgReference(stk, pci, 6);
		msg = sql_alter_user(sql, sname, passwd, enc, schema, oldpasswd);
		break;
	}
	case DDL_RENAME_USER:{
		char *newuser = *(str *) getArgReference(stk, pci, 3);
		msg = sql_rename_user(sql, sname, newuser);
		break;
	}
	case DDL_CREATE_ROLE:{
		char *role = sname;
		int grantor = *(int *) getArgReference(stk, pci, 4);
		msg = sql_create_role(sql, role, grantor);
		break;
	}
	case DDL_DROP_ROLE:{
		char *role = sname;
		msg = sql_drop_role(sql, role);
		break;
	}
	case DDL_DROP_INDEX:{
		char *iname = *(str *) getArgReference(stk, pci, 3);
		msg = drop_index(sql, sname, iname);
		break;
	}
	case DDL_DROP_FUNCTION:{
		char *fname = *(str *) getArgReference(stk, pci, 3);
		int fid = *(int *) getArgReference(stk, pci, 4);
		int type = *(int *) getArgReference(stk, pci, 5);
		int action = *(int *) getArgReference(stk, pci, 6);
		msg = drop_func(sql, sname, fname, fid, type, action);
		break;
	}
	case DDL_CREATE_FUNCTION:{
		sql_func *f = *(sql_func **) getArgReference(stk, pci, 3);
		msg = create_func(sql, sname, f);
		break;
	}
	case DDL_CREATE_TRIGGER:{
		char *tname = *(str *) getArgReference(stk, pci, 3);
		char *triggername = *(str *) getArgReference(stk, pci, 4);
		int time = *(int *) getArgReference(stk, pci, 5);
		int orientation = *(int *) getArgReference(stk, pci, 6);
		int event = *(int *) getArgReference(stk, pci, 7);
		char *old_name = *(str *) getArgReference(stk, pci, 8);
		char *new_name = *(str *) getArgReference(stk, pci, 9);
		char *condition = *(str *) getArgReference(stk, pci, 10);
		char *query = *(str *) getArgReference(stk, pci, 11);

		msg = create_trigger(sql, sname, tname, triggername, time, orientation, event, old_name, new_name, condition, query);
		break;
	}
	case DDL_DROP_TRIGGER:{
		char *triggername = *(str *) getArgReference(stk, pci, 3);

		msg = drop_trigger(sql, sname, triggername);
		break;
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
	int *res = (int *) getArgReference(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	str varname = *(str *) getArgReference(stk, pci, 2);
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
		str newopt = *(str *) getArgReference(stk, pci, 3);
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
	str varname = *(str *) getArgReference(stk, pci, 2);
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
	int *res = (int *) getArgReference(stk, pci, 0);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	vars = BATnew(TYPE_void, TYPE_str, m->topvars);
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
	int *res = (int *) getArgReference(stk, pci, 0);
	str filename = *(str *) getArgReference(stk, pci, 1);

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
	*res = 0;
	return MAL_SUCCEED;
}

/* str mvc_next_value(lng *res, str *sname, str *seqname); */
str
mvc_next_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	sql_schema *s;
	lng *res = (lng *) getArgReference(stk, pci, 0);
	str *sname = (str *) getArgReference(stk, pci, 1);
	str *seqname = (str *) getArgReference(stk, pci, 2);

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
	bat *res = (bat *) getArgReference(stk, pci, 0);
	int *sid = (int *) getArgReference(stk, pci, 1);
	str *seqname = (str *) getArgReference(stk, pci, 2);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if ((b = BATdescriptor(*sid)) == NULL)
		throw(SQL, "sql.next_value", "Cannot access descriptor");

	r = BATnew(b->htype, TYPE_lng, BATcount(b));
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
		BUNins(r, BUNhead(bi, p), &l, FALSE);
	}
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
	lng *res = (lng *) getArgReference(stk, pci, 0);
	str *sname = (str *) getArgReference(stk, pci, 1);
	str *seqname = (str *) getArgReference(stk, pci, 2);

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
mvc_getVersion(lng *version, int *clientid)
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
	lng *res = (lng *) getArgReference(stk, pci, 0);
	str *sname = (str *) getArgReference(stk, pci, 1);
	str *seqname = (str *) getArgReference(stk, pci, 2);
	lng *start = (lng *) getArgReference(stk, pci, 3);

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
mvc_bind_idxbat(mvc *m, char *sname, char *tname, char *iname, int access)
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
	int *bid = (int *) getArgReference(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	str *sname = (str *) getArgReference(stk, pci, 2 + upd);
	str *tname = (str *) getArgReference(stk, pci, 3 + upd);
	str *cname = (str *) getArgReference(stk, pci, 4 + upd);
	int *access = (int *) getArgReference(stk, pci, 5 + upd);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = mvc_bind(m, *sname, *tname, *cname, *access);
	if (b) {
		if (pci->argc == (8 + upd) && getArgType(mb, pci, 6 + upd) == TYPE_int) {
			BUN cnt = BATcount(b), psz;
			/* partitioned access */
			int part_nr = *(int *) getArgReference(stk, pci, 6 + upd);
			int nr_parts = *(int *) getArgReference(stk, pci, 7 + upd);

			if (*access == 0) {
				psz = cnt ? (cnt / nr_parts) : 0;
				bn = BATslice(b, part_nr * psz, (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz));
				BATseqbase(bn, part_nr * psz);
			} else {
				oid l, h;
				BAT *c = mvc_bind(m, *sname, *tname, *cname, 0);
				cnt = BATcount(c);
				psz = cnt ? (cnt / nr_parts) : 0;
				l = part_nr * psz;
				h = (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz);
				h--;
				bn = BATmirror(BATselect(BATmirror(b), &l, &h));
				BBPreleaseref(c->batCacheid);
			}
			BBPreleaseref(b->batCacheid);
			b = bn;
		}
		if (upd) {
			int *uvl = (int *) getArgReference(stk, pci, 1);

			if (BATcount(b)) {
				BAT *id = BATmirror(BATmark(b, 0));
				BAT *vl = BATmirror(BATmark(BATmirror(b), 0));
				BBPkeepref(*bid = id->batCacheid);
				BBPkeepref(*uvl = vl->batCacheid);
			} else {
				*bid = e_bat(TYPE_oid);
				*uvl = e_bat(b->T->type);
			}
			BBPreleaseref(b->batCacheid);
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
	int *bid = (int *) getArgReference(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	str *sname = (str *) getArgReference(stk, pci, 2 + upd);
	str *tname = (str *) getArgReference(stk, pci, 3 + upd);
	str *iname = (str *) getArgReference(stk, pci, 4 + upd);
	int *access = (int *) getArgReference(stk, pci, 5 + upd);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = mvc_bind_idxbat(m, *sname, *tname, *iname, *access);
	if (b) {
		if (pci->argc == (8 + upd) && getArgType(mb, pci, 6 + upd) == TYPE_int) {
			BUN cnt = BATcount(b), psz;
			/* partitioned access */
			int part_nr = *(int *) getArgReference(stk, pci, 6 + upd);
			int nr_parts = *(int *) getArgReference(stk, pci, 7 + upd);

			if (*access == 0) {
				psz = cnt ? (cnt / nr_parts) : 0;
				bn = BATslice(b, part_nr * psz, (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz));
				BATseqbase(bn, part_nr * psz);
			} else {
				oid l, h;
				BAT *c = mvc_bind_idxbat(m, *sname, *tname, *iname, 0);
				cnt = BATcount(c);
				psz = cnt ? (cnt / nr_parts) : 0;
				l = part_nr * psz;
				h = (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz);
				h--;
				bn = BATmirror(BATselect(BATmirror(b), &l, &h));
				BBPreleaseref(c->batCacheid);
			}
			BBPreleaseref(b->batCacheid);
			b = bn;
		}
		if (upd) {
			int *uvl = (int *) getArgReference(stk, pci, 1);

			if (BATcount(b)) {
				BAT *id = BATmirror(BATmark(b, 0));
				BAT *vl = BATmirror(BATmark(BATmirror(b), 0));
				BBPkeepref(*bid = id->batCacheid);
				BBPkeepref(*uvl = vl->batCacheid);
			} else {
				*bid = e_bat(TYPE_oid);
				*uvl = e_bat(b->T->type);
			}
			BBPreleaseref(b->batCacheid);
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
	int *res = (int *) getArgReference(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	str sname = *(str *) getArgReference(stk, pci, 2);
	str tname = *(str *) getArgReference(stk, pci, 3);
	str cname = *(str *) getArgReference(stk, pci, 4);
	ptr ins = (ptr) getArgReference(stk, pci, 5);
	int tpe = getArgType(mb, pci, 5);
	sql_schema *s;
	sql_table *t;
	sql_column *c;

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
	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		throw(SQL, "sql.append", "Schema missing");
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "sql.append", "Table missing");
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
	int *res = (int *) getArgReference(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	str sname = *(str *) getArgReference(stk, pci, 2);
	str tname = *(str *) getArgReference(stk, pci, 3);
	str cname = *(str *) getArgReference(stk, pci, 4);
	bat Tids = *(bat *) getArgReference(stk, pci, 5);
	bat Upd = *(bat *) getArgReference(stk, pci, 6);
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
	wrd *res = (wrd *) getArgReference(stk, pci, 0);
	str *sname = (str *) getArgReference(stk, pci, 1);
	str *tname = (str *) getArgReference(stk, pci, 2);

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
	int *res = (int *) getArgReference(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	str sname = *(str *) getArgReference(stk, pci, 2);
	str tname = *(str *) getArgReference(stk, pci, 3);
	ptr ins = (ptr) getArgReference(stk, pci, 4);
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
	store_funcs.delete_tab(m->session->tr, t, b, tpe);
	if (tpe == TYPE_bat)
		BBPunfix(((BAT *) ins)->batCacheid);
	return MAL_SUCCEED;
}

static BAT *
setwritable(BAT *b)
{
	BAT *bn;

	bn = BATsetaccess(b, BAT_WRITE);	/* can return NULL */
	if (b != bn)
		BBPunfix(b->batCacheid);
	return bn;
}

str
DELTAbat2(bat *result, bat *col, bat *uid, bat *uval)
{
	return DELTAbat(result, col, uid, uval, NULL);
}

str
DELTAsub2(bat *result, bat *col, bat *cid, bat *uid, bat *uval)
{
	return DELTAsub(result, col, cid, uid, uval, NULL);
}

str
DELTAproject2(bat *result, bat *sub, bat *col, bat *uid, bat *uval)
{
	return DELTAproject(result, sub, col, uid, uval, NULL);
}

str
DELTAbat(bat *result, bat *col, bat *uid, bat *uval, bat *ins)
{
	BAT *c, *u_id, *u_val, *u, *i = NULL, *res;

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
	if ((res = BATcopy(c, TYPE_void, c->ttype, TRUE)) == NULL){
		BBPunfix(c->batCacheid);
		throw(MAL, "sql.delta", OPERATION_FAILED);
	}
	BBPunfix(c->batCacheid);

	if ((u_val = BATdescriptor(*uval)) == NULL)
		throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
	u_id = BATdescriptor(*uid);
	u = BATleftfetchjoin(BATmirror(u_id), u_val, BATcount(u_val));
	BBPunfix(u_id->batCacheid);
	BBPunfix(u_val->batCacheid);
	if (BATcount(u))
		res = BATreplace(res, u, TRUE);
	BBPunfix(u->batCacheid);

	if (i && BATcount(i)) {
		i = BATdescriptor(*ins);
		res = BATappend(res, i, TRUE);
		BBPunfix(i->batCacheid);
	}

	BBPkeepref(*result = res->batCacheid);
	return MAL_SUCCEED;
}

str
DELTAsub(bat *result, bat *col, bat *cid, bat *uid, bat *uval, bat *ins)
{
	BAT *c, *cminu, *u_id, *u_val, *u, *i = NULL, *res;

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
		cminu = BATkdiff(BATmirror(c), BATmirror(u_id));
		BBPunfix(c->batCacheid);
		res = c = BATmirror(BATmark(cminu, 0));
		BBPunfix(cminu->batCacheid);

		if ((u_val = BATdescriptor(*uval)) == NULL) {
			BBPunfix(c->batCacheid);
			BBPunfix(u_id->batCacheid);
			throw(MAL, "sql.delta", RUNTIME_OBJECT_MISSING);
		}
		u = BATleftfetchjoin(u_val, u_id, BATcount(u_val));
		BBPunfix(u_val->batCacheid);
		BBPunfix(u_id->batCacheid);
		if (BATcount(u)) {	/* check selected updated values against candidates */
			BAT *c_ids = BATdescriptor(*cid);

			cminu = BATsemijoin(BATmirror(u), BATmirror(c_ids));
			BBPunfix(c_ids->batCacheid);
			BBPunfix(u->batCacheid);
			u = BATmirror(cminu);
		}
		res = BATappend(c, u, TRUE);
		BBPunfix(u->batCacheid);

		u = BATsort(BATmirror(res));
		BBPunfix(res->batCacheid);
		res = BATmirror(BATmark(u, 0));
		BBPunfix(u->batCacheid);
	}

	if (i) {
		i = BATdescriptor(*ins);
		if (BATcount(u_id)) {
			u_id = BATdescriptor(*uid);
			cminu = BATkdiff(BATmirror(i), BATmirror(u_id));
			BBPunfix(i->batCacheid);
			BBPunfix(u_id->batCacheid);
			i = BATmirror(BATmark(cminu, 0));
			BBPunfix(cminu->batCacheid);
		}
		res = BATappend(res, i, TRUE);
		BBPunfix(i->batCacheid);

		u = BATsort(BATmirror(res));
		BBPunfix(res->batCacheid);
		res = BATmirror(BATmark(u, 0));
		BBPunfix(u->batCacheid);
	}
	BATkey(BATmirror(res), TRUE);
	BBPkeepref(*result = res->batCacheid);
	return MAL_SUCCEED;
}

str
DELTAproject(bat *result, bat *sub, bat *col, bat *uid, bat *uval, bat *ins)
{
	BAT *s, *c, *u_id, *u_val, *u, *i = NULL, *res, *tres;

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

	/* leftfetchjoin(sub,col).union(leftfetchjoin(sub,i)) */
	res = c;
	if (i && BATcount(i)) {
		if (BATcount(c) == 0) {
			res = i;
			i = c;
		} else {
			if ((res = BATcopy(c, TYPE_void, c->ttype, TRUE)) == NULL)
				throw(MAL, "sql.projectdelta", OPERATION_FAILED);
			res = BATappend(res, i, FALSE);
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

	u = BATleftfetchjoin(BATmirror(u_id), u_val, BATcount(u_val));
	BBPunfix(u_id->batCacheid);
	BBPunfix(u_val->batCacheid);
	if (BATcount(u)) {
		BAT *nu = BATleftjoin(s, u, BATcount(u));
		res = setwritable(res);
		res = BATreplace(res, nu, 0);
		BBPunfix(nu->batCacheid);
	}
	BBPunfix(s->batCacheid);
	BBPunfix(u->batCacheid);

	BBPkeepref(*result = res->batCacheid);
	return MAL_SUCCEED;
}

/* str SQLtid(bat *result, mvc *m, str *sname, str *tname) */
str
SQLtid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res = (int *) getArgReference(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	sql_trans *tr;
	str sname = *(str *) getArgReference(stk, pci, 2);
	str tname = *(str *) getArgReference(stk, pci, 3);

	sql_schema *s;
	sql_table *t;
	sql_column *c;
	BAT *tids;
	size_t nr, inr = 0;
	oid sb = 0;

	*res = 0;
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

	if (isTable(t) && !t->readonly && (t->base.flag != TR_NEW /* alter */ ) &&
	    t->persistence == SQL_PERSIST && !t->commit_action)
		inr = store_funcs.count_col(tr, c, 0);
	nr -= inr;
	if (pci->argc == 6) {	/* partitioned version */
		size_t cnt = nr;
		int part_nr = *(int *) getArgReference(stk, pci, 4);
		int nr_parts = *(int *) getArgReference(stk, pci, 5);

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
	tids = BATnew(TYPE_void, TYPE_void, 0);
	tids->H->seq = sb;
	tids->T->seq = sb;
	BATsetcount(tids, (BUN) nr);
	tids->H->revsorted = 0;
	tids->T->revsorted = 0;

	if (store_funcs.count_del(tr, t)) {
		BAT *d = store_funcs.bind_del(tr, t, RD_INS);
		BAT *diff = BATkdiff(tids, BATmirror(d));

		BBPunfix(tids->batCacheid);
		tids = BATmirror(BATmark(diff, sb));
		BBPunfix(diff->batCacheid);
		BBPunfix(d->batCacheid);
	}
	BBPkeepref(*res = tids->batCacheid);
	return MAL_SUCCEED;
}

static int
mvc_result_row(mvc *m, int nr_cols, int qtype)
{
	m->results = res_table_create(m->session->tr, m->result_id++, nr_cols, qtype, m->results, NULL);
	return m->results->id;
}

/* str mvc_result_row_wrap(int *res_id, int *nr_cols, int *qtype, int *o); */
str
mvc_result_row_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	int *res_id = (int *) getArgReference(stk, pci, 0);
	int *nr_cols = (int *) getArgReference(stk, pci, 1);
	int *qtype = (int *) getArgReference(stk, pci, 2);
	int *o = (int *) getArgReference(stk, pci, 3);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	(void) o;		/* dummy order */
	*res_id = mvc_result_row(m, *nr_cols, *qtype);
	if (*res_id < 0)
		throw(SQL, "sql.resultSet", "failed");
	return MAL_SUCCEED;
}

/* str mvc_result_file_wrap(int *res_id, int *nr_cols, unsigned char* *T, unsigned char* *R, unsigned char* *S, unsigned char* *N, bat *order_bid); */
str
mvc_result_file_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str res = MAL_SUCCEED;
	BAT *order = NULL;
	mvc *m = NULL;
	str msg;
	res_table *t = NULL;
	unsigned char *tsep = NULL, *rsep = NULL, *ssep = NULL, *ns = NULL;
	ssize_t len;
	int *res_id = (int *) getArgReference(stk, pci, 0);
	int *nr_cols = (int *) getArgReference(stk, pci, 1);
	unsigned char **T = (unsigned char **) getArgReference(stk, pci, 2);
	unsigned char **R = (unsigned char **) getArgReference(stk, pci, 3);
	unsigned char **S = (unsigned char **) getArgReference(stk, pci, 4);
	unsigned char **N = (unsigned char **) getArgReference(stk, pci, 5);
	int mtype = getArgType(mb, pci, 6);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (isaBatType(mtype)) {
		bat *order_bid = (bat *) getArgReference(stk, pci, 6);
		if ((order = BATdescriptor(*order_bid)) == NULL) {
			throw(SQL, "sql.resultSet", "Cannot access descriptor");
		}
	}
	m->results = t = res_table_create(m->session->tr, m->result_id++, *nr_cols, Q_TABLE, m->results, order);
	len = strlen((char *) (*T));
	GDKstrFromStr(tsep = GDKmalloc(len + 1), *T, len);
	len = 0;
	len = strlen((char *) (*R));
	GDKstrFromStr(rsep = GDKmalloc(len + 1), *R, len);
	len = 0;
	len = strlen((char *) (*S));
	GDKstrFromStr(ssep = GDKmalloc(len + 1), *S, len);
	len = 0;
	len = strlen((char *) (*N));
	GDKstrFromStr(ns = GDKmalloc(len + 1), *N, len);
	len = 0;
	t->tsep = (char *) tsep;
	t->rsep = (char *) rsep;
	t->ssep = (char *) ssep;
	t->ns = (char *) ns;
	*res_id = t->id;
	if (*res_id < 0)
		res = createException(SQL, "sql.resultSet", "failed");
	if (order)
		BBPunfix(order->batCacheid);
	return res;
}

/* str mvc_result_table_wrap(int *res_id, int *nr_cols, int *qtype, bat *order_bid); */
str
mvc_result_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str res = MAL_SUCCEED;
	BAT *order;
	mvc *m = NULL;
	str msg;
	int *res_id = (int *) getArgReference(stk, pci, 0);
	int *nr_cols = (int *) getArgReference(stk, pci, 1);
	int *qtype = (int *) getArgReference(stk, pci, 2);
	bat *order_bid = (bat *) getArgReference(stk, pci, 3);

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

/* str mvc_result_column_wrap(int *ret, int *rs, str *tn, str *name, str *type, int *digits, int *scale, bat *bid); */
str
mvc_result_column_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str res = MAL_SUCCEED;
	BAT *b;
	mvc *m = NULL;
	str msg;
	int *ret = (int *) getArgReference(stk, pci, 0);
	str *tn = (str *) getArgReference(stk, pci, 2);
	str *name = (str *) getArgReference(stk, pci, 3);
	str *type = (str *) getArgReference(stk, pci, 4);
	int *digits = (int *) getArgReference(stk, pci, 5);
	int *scale = (int *) getArgReference(stk, pci, 6);
	bat *bid = (bat *) getArgReference(stk, pci, 7);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(SQL, "sql.rsColumn", "cannot access BAT descriptor");
	if (mvc_result_column(m, *tn, *name, *type, *digits, *scale, b))
		res = createException(SQL, "sql.rsColumn", "mvc_result_column failed");
	*ret = 0;
	BBPunfix(b->batCacheid);
	return res;
}

str
/*mvc_result_value_wrap(int *ret, int *rs, str *tn, str *name, str *type, int *digits, int *scale, ptr p, int mtype)*/
mvc_result_value_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int *) getArgReference(stk, pci, 0);
	str *tn = (str *) getArgReference(stk, pci, 2);
	str *cn = (str *) getArgReference(stk, pci, 3);
	str *type = (str *) getArgReference(stk, pci, 4);
	int *digits = (int *) getArgReference(stk, pci, 5);
	int *scale = (int *) getArgReference(stk, pci, 6);
	ptr p = (ptr) getArgReference(stk, pci, 7);
	int mtype = getArgType(mb, pci, 7);
	mvc *m = NULL;
	str msg;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (ATOMextern(mtype))
		p = *(ptr *) p;
	if (mvc_result_value(m, *tn, *cn, *type, *digits, *scale, p, mtype))
		throw(SQL, "sql.rsColumn", "failed");
	*ret = 0;
	return MAL_SUCCEED;
}

/* str mvc_declared_table_wrap(int *res_id, str *name); */
str
mvc_declared_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	sql_schema *s = NULL;
	int *res_id = (int *) getArgReference(stk, pci, 0);
	str *name = (str *) getArgReference(stk, pci, 1);

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
	int *ret = (int *) getArgReference(stk, pci, 0);
	int *rs = (int *) getArgReference(stk, pci, 1);
	str *tname = (str *) getArgReference(stk, pci, 2);
	str *name = (str *) getArgReference(stk, pci, 3);
	str *typename = (str *) getArgReference(stk, pci, 4);
	int *digits = (int *) getArgReference(stk, pci, 5);
	int *scale = (int *) getArgReference(stk, pci, 6);

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
	*ret = 0;
	return MAL_SUCCEED;
}

str
mvc_drop_declared_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str *name = (str *) getArgReference(stk, pci, 1);
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
	int i = *(int *) getArgReference(stk, pci, 1);
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
	int *res = (int *) getArgReference(stk, pci, 0);
#ifndef NDEBUG
	int mtype = getArgType(mb, pci, 2);
#endif
	wrd nr;
	str *w = (str *) getArgReference(stk, pci, 3), msg;

	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	*res = 0;
	assert(mtype == TYPE_wrd);
	nr = *(wrd *) getArgReference(stk, pci, 2);
	b = cntxt->sqlcontext;
	if (mvc_export_affrows(b, b->out, nr, *w))
		throw(SQL, "sql.affectedRows", "failed");
	return MAL_SUCCEED;
}

/* str mvc_export_head_wrap(int *ret, stream **s, int *res_id); */
str
mvc_export_head_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *b = NULL;
	stream **s = (stream **) getArgReference(stk, pci, 1);
	int *res_id = (int *) getArgReference(stk, pci, 2);
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
	int *res_id = (int *) getArgReference(stk, pci, 2);
	str msg;

	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = cntxt->sqlcontext;
	if (mvc_export_result(b, *s, *res_id))
		throw(SQL, "sql.exportResult", "failed");
	return NULL;
}

/* str mvc_export_chunk_wrap(int *ret, stream **s, int *res_id, str *w); */
str
mvc_export_chunk_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *b = NULL;
	stream **s = (stream **) getArgReference(stk, pci, 1);
	int *res_id = (int *) getArgReference(stk, pci, 2);
	BUN offset = 0;
	BUN nr = 0;
	str msg;

	(void) mb;		/* NOT USED */
	if (pci->argc == 5) {
		offset = *(BUN *) getArgReference(stk, pci, 3);
		nr = *(BUN *) getArgReference(stk, pci, 4);
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
	str *w = (str *) getArgReference(stk, pci, 1), msg;

	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = cntxt->sqlcontext;
	if (mvc_export_operation(b, b->out, *w))
		throw(SQL, "sql.exportOperation", "failed");
	return NULL;
}

str
/*mvc_export_value_wrap(int *ret, int *qtype, str tn, str name, str type, int *digits, int *scale, int *eclass, ptr p, int mtype)*/
mvc_export_value_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *qtype = (int *) getArgReference(stk, pci, 1);
	str *tn = (str *) getArgReference(stk, pci, 2);
	str *cn = (str *) getArgReference(stk, pci, 3);
	str *type = (str *) getArgReference(stk, pci, 4);
	int *digits = (int *) getArgReference(stk, pci, 5);
	int *scale = (int *) getArgReference(stk, pci, 6);
	int *eclass = (int *) getArgReference(stk, pci, 7);
	ptr p = (ptr) getArgReference(stk, pci, 8);
	int mtype = getArgType(mb, pci, 8);
	str *w = (str *) getArgReference(stk, pci, 9), msg;
	backend *b = NULL;

	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = cntxt->sqlcontext;
	if (ATOMextern(mtype))
		p = *(ptr *) p;
	if (b->out == NULL || mvc_export_value(b, b->out, *qtype, *tn, *cn, *type, *digits, *scale, *eclass, p, mtype, *w, "NULL") != SQL_OK)
		throw(SQL, "sql.exportValue", "failed");
	return MAL_SUCCEED;
}

static void
bat2return(MalStkPtr stk, InstrPtr pci, BAT **b)
{
	int i;

	for (i = 0; i < pci->retc; i++) {
		*(int *) getArgReference(stk, pci, i) = b[i]->batCacheid;
		BBPkeepref(b[i]->batCacheid);
	}
}

/* str mvc_import_table_wrap(int *res, str *sname, str *tname, unsigned char* *T, unsigned char* *R, unsigned char* *S, unsigned char* *N, str *fname, lng *sz, lng *offset); */
str
mvc_import_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *be;
	BAT **b = NULL;
	unsigned char *tsep = NULL, *rsep = NULL, *ssep = NULL, *ns = NULL;
	ssize_t len = 0;
	str filename, cs;
	str *sname = (str *) getArgReference(stk, pci, pci->retc + 0);
	str *tname = (str *) getArgReference(stk, pci, pci->retc + 1);
	unsigned char **T = (unsigned char **) getArgReference(stk, pci, pci->retc + 2);
	unsigned char **R = (unsigned char **) getArgReference(stk, pci, pci->retc + 3);
	unsigned char **S = (unsigned char **) getArgReference(stk, pci, pci->retc + 4);
	unsigned char **N = (unsigned char **) getArgReference(stk, pci, pci->retc + 5);
	str *fname = (str *) getArgReference(stk, pci, pci->retc + 6), msg;
	lng *sz = (lng *) getArgReference(stk, pci, pci->retc + 7);
	lng *offset = (lng *) getArgReference(stk, pci, pci->retc + 8);
	int *locked = (int *) getArgReference(stk, pci, pci->retc + 9);
	bstream *s;
	stream *ss;
	str utf8 = "UTF-8";

	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	be = cntxt->sqlcontext;
	len = strlen((char *) (*T));
	GDKstrFromStr(tsep = GDKmalloc(len + 1), *T, len);
	len = 0;
	len = strlen((char *) (*R));
	GDKstrFromStr(rsep = GDKmalloc(len + 1), *R, len);
	len = 0;
	if (*S && strcmp(str_nil, *(char **) S)) {
		len = strlen((char *) (*S));
		GDKstrFromStr(ssep = GDKmalloc(len + 1), *S, len);
		len = 0;
	}

	STRcodeset(&cs);
	STRIconv(&filename, fname, &utf8, &cs);
	GDKfree(cs);
	len = strlen((char *) (*N));
	GDKstrFromStr(ns = GDKmalloc(len + 1), *N, len);
	len = 0;
	ss = open_rastream(filename);
	if (!ss || mnstr_errnr(ss)) {
		int errnr = mnstr_errnr(ss);
		if (ss)
			mnstr_destroy(ss);
		throw(IO, "streams.open", "could not open file '%s': %s", filename, strerror(errnr));
	}
#if SIZEOF_VOID_P == 4
	s = bstream_create(ss, 0x20000);
#else
	s = bstream_create(ss, 0x2000000);
#endif
	if (s != NULL) {
		b = mvc_import_table(cntxt, be->mvc, s, *sname, *tname, (char *) tsep, (char *) rsep, (char *) ssep, (char *) ns, *sz, *offset, *locked);
		bstream_destroy(s);
	}
	GDKfree(filename);
	GDKfree(tsep);
	GDKfree(rsep);
	if (ssep)
		GDKfree(ssep);
	GDKfree(ns);
	if (s == NULL)
		throw(IO, "bstreams.create", "failed to create block stream");
	if (b == NULL)
		throw(SQL, "importTable", "%sfailed to import table", be->mvc->errstr);
	bat2return(stk, pci, b);
	GDKfree(b);
	return MAL_SUCCEED;
}

/* str mvc_import_table_stdin(int *res, str *sname, str *tname, unsigned char* *T, unsigned char* *R, unsigned char* *S, unsigned char* *N, lng *sz, lng *offset); */
str
mvc_import_table_stdin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT **b = NULL;
	mvc *m = NULL;
	str msg;
	unsigned char *tsep = NULL, *rsep = NULL, *ssep = NULL, *ns = NULL;
	ssize_t len = 0;
	str *sname = (str *) getArgReference(stk, pci, pci->retc + 0);
	str *tname = (str *) getArgReference(stk, pci, pci->retc + 1);
	unsigned char **T = (unsigned char **) getArgReference(stk, pci, pci->retc + 2);
	unsigned char **R = (unsigned char **) getArgReference(stk, pci, pci->retc + 3);
	unsigned char **S = (unsigned char **) getArgReference(stk, pci, pci->retc + 4);
	unsigned char **N = (unsigned char **) getArgReference(stk, pci, pci->retc + 5);
	lng *sz = (lng *) getArgReference(stk, pci, pci->retc + 6);
	lng *offset = (lng *) getArgReference(stk, pci, pci->retc + 7);
	int *locked = (int *) getArgReference(stk, pci, pci->retc + 8);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	len = strlen((char *) (*T));
	GDKstrFromStr(tsep = GDKmalloc(len + 1), *T, len);
	len = 0;
	len = strlen((char *) (*R));
	GDKstrFromStr(rsep = GDKmalloc(len + 1), *R, len);
	len = 0;
	if (*S && strcmp(str_nil, *(char **) S)) {
		len = strlen((char *) (*S));
		GDKstrFromStr(ssep = GDKmalloc(len + 1), *S, len);
		len = 0;
	}
	len = strlen((char *) (*N));
	GDKstrFromStr(ns = GDKmalloc(len + 1), *N, len);
	len = 0;
	b = mvc_import_table(cntxt, m, m->scanner.rs, *sname, *tname, (char *) tsep, (char *) rsep, (char *) ssep, (char *) ns, *sz, *offset, *locked);
	GDKfree(tsep);
	GDKfree(rsep);
	if (ssep)
		GDKfree(ssep);
	GDKfree(ns);
	if (!b)
		throw(SQL, "importTable", "%sfailed to import table", m->errstr);
	bat2return(stk, pci, b);
	GDKfree(b);
	return MAL_SUCCEED;
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
	str sname = *(str *) getArgReference(stk, pci, 0 + pci->retc);
	str tname = *(str *) getArgReference(stk, pci, 1 + pci->retc);
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
			throw(SQL, "sql", "failed to attach file %s", *(str *) getArgReference(stk, pci, i));
		f = fopen(*(str *) getArgReference(stk, pci, i), "r");
		if (f == NULL)
			throw(SQL, "sql", "failed to open file %s", *(str *) getArgReference(stk, pci, i));
		fclose(f);
	}

	for (i = pci->retc + 2, n = t->columns.set->h; i < pci->argc && n; i++, n = n->next) {
		sql_column *col = n->data;
		BAT *c = NULL;
		int tpe = col->type.type->localtype;

		/* handle the various cases */
		if (tpe < TYPE_str || tpe == TYPE_date || tpe == TYPE_daytime || tpe == TYPE_timestamp) {
			c = BATattach(col->type.type->localtype, *(str *) getArgReference(stk, pci, i));
			if (c == NULL)
				throw(SQL, "sql", "failed to attach file %s", *(str *) getArgReference(stk, pci, i));
			BATsetaccess(c, BAT_READ);
			BATderiveProps(c, 1);
		} else if (tpe == TYPE_str) {
			/* get the BAT and fill it with the strings */
			c = BATnew(TYPE_void, TYPE_str, 0);
			BATseqbase(c, 0);
			/* this code should be extended to deal with larger text strings. */
			f = fopen(*(str *) getArgReference(stk, pci, i), "r");
			if (f == NULL)
				throw(SQL, "sql", "failed to re-open file %s", *(str *) getArgReference(stk, pci, i));

			buf = GDKmalloc(bufsiz);
			while (fgets(buf, bufsiz, f) != NULL) {
				char *t = strrchr(buf, '\n');
				if (t)
					*t = 0;
				BUNappend(c, buf, FALSE);
			}
			fclose(f);
			GDKfree(buf);
		} else {
			throw(SQL, "sql", "failed to attach file %s", *(str *) getArgReference(stk, pci, i));
		}
		if (i != (pci->retc + 2) && cnt != BATcount(c))
			throw(SQL, "sql", "binary files for table '%s' have inconsistent counts", tname);
		cnt = BATcount(c);
		*(int *) getArgReference(stk, pci, i - (2 + pci->retc)) = c->batCacheid;
		BBPkeepref(c->batCacheid);
	}
	return MAL_SUCCEED;
}

str
zero_or_one(ptr ret, int *bid)
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
	} else {
		memcpy(ret, p, _s);
	}
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
not_unique(bit *ret, int *bid)
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

/* later we could optimize this to start from current BUN 
   And only search the from the first if second is not found.
 */
static inline int
HASHfndTwice(BAT *b, ptr v)
{
	BATiter bi = bat_iterator(b);
	BUN i = BUN_NONE;
	int first = 1;

	HASHloop(bi, b->H->hash, i, v) {
		if (!first)
			return 1;
		first = 0;
	}
	return 0;
}

str
not_unique_oids(bat *ret, bat *bid)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "not_uniques", "Cannot access descriptor");
	}
	if (b->ttype != TYPE_oid && b->ttype != TYPE_wrd) {
		throw(SQL, "not_uniques", "Wrong types");
	}

	assert(b->htype == TYPE_oid);
	if (BATtkey(b) || BATtdense(b) || BATcount(b) <= 1) {
		bn = BATnew(TYPE_void, TYPE_void, 0);
		if (bn == NULL) {
			BBPreleaseref(b->batCacheid);
			throw(SQL, "sql.not_uniques", MAL_MALLOC_FAIL);
		}
		BATseqbase(bn, 0);
		BATseqbase(BATmirror(bn), 0);
	} else if (b->tsorted) {	/* ugh handle both wrd and oid types */
		oid c = *(oid *) Tloc(b, BUNfirst(b)), *rf, *rh, *rt;
		oid *h = (oid *) Hloc(b, 0), *vp, *ve;
		int first = 1;

		bn = BATnew(TYPE_oid, TYPE_oid, BATcount(b));
		if (bn == NULL) {
			BBPreleaseref(b->batCacheid);
			throw(SQL, "sql.not_uniques", MAL_MALLOC_FAIL);
		}
		vp = (oid *) Tloc(b, BUNfirst(b));
		ve = vp + BATcount(b);
		rf = rh = (oid *) Hloc(bn, BUNfirst(bn));
		rt = (oid *) Tloc(bn, BUNfirst(bn));
		*rh++ = *h++;
		*rt++ = *vp;
		for (vp++; vp < ve; vp++, h++) {
			oid v = *vp;
			if (v == c) {
				first = 0;
				*rh++ = *h;
				*rt++ = v;
			} else if (!first) {
				first = 1;
				*rh++ = *h;
				*rt++ = v;
			} else {
				*rh = *h;
				*rt = v;
			}
			c = v;
		}
		if (first)
			rh--;
		BATsetcount(bn, (BUN) (rh - rf));
	} else {
		oid *rf, *rh, *rt;
		oid *h = (oid *) Hloc(b, 0), *vp, *ve;
		BAT *bm = BATmirror(b);

		if (BATprepareHash(bm))
			 throw(SQL, "not_uniques", "hash creation failed");
		bn = BATnew(TYPE_oid, TYPE_oid, BATcount(b));
		if (bn == NULL) {
			BBPreleaseref(b->batCacheid);
			throw(SQL, "sql.unique_oids", MAL_MALLOC_FAIL);
		}
		vp = (oid *) Tloc(b, BUNfirst(b));
		ve = vp + BATcount(b);
		rf = rh = (oid *) Hloc(bn, BUNfirst(bn));
		rt = (oid *) Tloc(bn, BUNfirst(bn));
		for (; vp < ve; vp++, h++) {
			/* try to find value twice */
			if (HASHfndTwice(bm, vp)) {
				*rh++ = *h;
				*rt++ = *vp;
			}
		}
		BATsetcount(bn, (BUN) (rh - rf));
	}
	BBPunfix(b->batCacheid);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

/* row case */
str
SQLidentity(bat *ret, bat *bid)
{
	BAT *bn, *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.identity", "Cannot access descriptor");
	}
	bn = VIEWhead(b);
	BBPunfix(b->batCacheid);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

str
BATSQLidentity(bat *ret, bat *bid)
{
	return BKCmirror(ret, bid);
}

str
PBATSQLidentity(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res = (int *) getArgReference(stk, pci, 0);
	oid *ns = (oid *) getArgReference(stk, pci, 1);
	int *bid = (int *) getArgReference(stk, pci, 2);
	oid *s = (oid *) getArgReference(stk, pci, 3);
	BAT *b, *bn = NULL;

	(void) cntxt;
	(void) mb;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "batcalc.identity", RUNTIME_OBJECT_MISSING);
	}
	bn = BATmark(b, *s);
	if (bn != NULL) {
		*ns = *s + BATcount(b);
		BBPreleaseref(b->batCacheid);
		BBPkeepref(*res = bn->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "batcalc.identity", GDK_EXCEPTION);

}

lng scales[20] = {
	LL_CONSTANT(1),
	LL_CONSTANT(10),
	LL_CONSTANT(100),
	LL_CONSTANT(1000),
	LL_CONSTANT(10000),
	LL_CONSTANT(100000),
	LL_CONSTANT(1000000),
	LL_CONSTANT(10000000),
	LL_CONSTANT(100000000),
	LL_CONSTANT(1000000000),
	LL_CONSTANT(10000000000),
	LL_CONSTANT(100000000000),
	LL_CONSTANT(1000000000000),
	LL_CONSTANT(10000000000000),
	LL_CONSTANT(100000000000000),
	LL_CONSTANT(1000000000000000),
	LL_CONSTANT(10000000000000000),
	LL_CONSTANT(100000000000000000),
	LL_CONSTANT(1000000000000000000)
};

/*
 * The core modules of Monet provide just a limited set of
 * mathematical operators. The extensions required to support
 * SQL-99 are shown below. At some point they also should be
 * moved to module code base.
 */

str
daytime_2time_daytime(daytime *res, daytime *v, int *digits)
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
second_interval_2_daytime(daytime *res, lng *s, int *digits)
{
	*res = (daytime) *s;
	return daytime_2time_daytime(res, res, digits);
}

str
nil_2time_daytime(daytime *res, void *v, int *digits)
{
	(void) digits;
	(void) v;
	*res = daytime_nil;
	return MAL_SUCCEED;
}

str
str_2time_daytime(daytime *res, str *v, int *digits)
{
	int len = sizeof(daytime), pos;

	if (!*v || strcmp(str_nil, *v) == 0) {
		*res = daytime_nil;
		return MAL_SUCCEED;
	}
	pos = daytime_fromstr(*v, &len, &res);
	if (!pos)
		throw(SQL, "daytime", "22007!daytime (%s) has incorrect format", *v);
	return daytime_2time_daytime(res, res, digits);
}

str
timestamp_2_daytime(daytime *res, timestamp *v, int *digits)
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
date_2_timestamp(timestamp *res, date *v, int *digits)
{
	(void) digits;		/* no precision needed */
	res->days = *v;
	res->msecs = 0;
	return MAL_SUCCEED;
}

str
timestamp_2time_timestamp(timestamp *res, timestamp *v, int *digits)
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
nil_2time_timestamp(timestamp *res, void *v, int *digits)
{
	(void) digits;
	(void) v;
	*res = *timestamp_nil;
	return MAL_SUCCEED;
}

str
str_2time_timestamp(timestamp *res, str *v, int *digits)
{
	int len = sizeof(timestamp), pos;

	if (!*v || strcmp(str_nil, *v) == 0) {
		*res = *timestamp_nil;
		return MAL_SUCCEED;
	}
	pos = timestamp_fromstr(*v, &len, &res);
	if (!pos)
		throw(SQL, "timestamp", "22007!timestamp (%s) has incorrect format", *v);
	return timestamp_2time_timestamp(res, res, digits);
}

str
SQLcst_alpha_cst(dbl *res, dbl *decl, dbl *theta)
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
SQLbat_alpha_cst(bat *res, bat *decl, dbl *theta)
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
	bn = BATnew(b->htype, TYPE_dbl, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
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
		BUNins(bn, BUNhead(bi, p), &r, FALSE);
	}
	*res = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
SQLcst_alpha_bat(bat *res, dbl *decl, bat *theta)
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
	bn = BATnew(b->htype, TYPE_dbl, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
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
		BUNins(bn, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
month_interval_str(int *ret, str *s, int *d, int *sk)
{
	lng res;

	if (interval_from_str(*s, *d, *sk, &res) < 0)
		throw(SQL, "calc.month_interval", "wrong format (%s)", *s);
	assert((lng) GDK_int_min <= res && res <= (lng) GDK_int_max);
	*ret = (int) res;
	return MAL_SUCCEED;
}

str
second_interval_str(lng *res, str *s, int *d, int *sk)
{
	if (interval_from_str(*s, *d, *sk, res) < 0)
		throw(SQL, "calc.second_interval", "wrong format (%s)", *s);
	return MAL_SUCCEED;
}

str
month_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int *) getArgReference(stk, pci, 0);
	int k = digits2ek(*(int *) getArgReference(stk, pci, 2));
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
	lng *ret = (lng *) getArgReference(stk, pci, 0), r;
	int k = digits2ek(*(int *) getArgReference(stk, pci, 2)), scale = 0;

	(void) cntxt;
	if (pci->argc > 3) 
		scale = *(int*) getArgReference(stk, pci, 3);
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
second_interval_daytime(lng *res, daytime *s, int *d, int *sk)
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
	daytime t, *res = (daytime *) getArgReference(stk, pci, 0);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;

	if ((msg = MTIMEcurrent_time(&t)) == MAL_SUCCEED)
		*res = t + m->timezone;
	return msg;
}

str
SQLcurrent_timestamp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	timestamp t, *res = (timestamp *) getArgReference(stk, pci, 0);

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
	int *rquery = (int *) getArgReference(stk, pci, 0);
	int *rcount = (int *) getArgReference(stk, pci, 1);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	cnt = m->qc->id;
	query = BATnew(TYPE_void, TYPE_str, cnt);
	if (query == NULL)
		throw(SQL, "sql.dumpcache", MAL_MALLOC_FAIL);
	BATseqbase(query, 0);
	count = BATnew(TYPE_void, TYPE_int, cnt);
	if (count == NULL) {
		BBPreleaseref(query->batCacheid);
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
	int *rrewrite = (int *) getArgReference(stk, pci, 0);
	int *rcount = (int *) getArgReference(stk, pci, 1);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	cnt = m->qc->id;
	rewrite = BATnew(TYPE_void, TYPE_str, cnt);
	if (rewrite == NULL)
		throw(SQL, "sql.optstats", MAL_MALLOC_FAIL);
	BATseqbase(rewrite, 0);
	count = BATnew(TYPE_void, TYPE_int, cnt);
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
	BAT *t[12];

	(void) cntxt;
	(void) mb;
	TRACEtable(t);
	for (i = 0; i < 12; i++) {
		int id = t[i]->batCacheid;

		*(int *) getArgReference(stk, pci, i) = id;
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
	BAT *t[7];

	(void) cntxt;
	(void) mb;
	QLOGcatalog(t);
	for (i = 0; i < 7; i++) {
		int id = t[i]->batCacheid;

		*(int *) getArgReference(stk, pci, i) = id;
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
	for (i = 0; i < 10; i++) {
		int id = t[i]->batCacheid;

		*(int *) getArgReference(stk, pci, i) = id;
		BBPkeepref(id);
	}
	return MAL_SUCCEED;
}

str
sql_querylog_empty(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int *) getArgReference(stk, pci, 0);
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	QLOGempty(ret);
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
	oid *rid = (oid *) getArgReference(stk, pci, 0);
	str *sname = (str *) getArgReference(stk, pci, 2);
	str *tname = (str *) getArgReference(stk, pci, 3);

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
	/* UGH (move into storage backends!!) */
	d = c->data;
	*rid = d->ibase + BATcount(b);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
do_sql_rank_grp(bat *rid, bat *bid, bat *gid, int nrank, int dense, const char *name)
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
		BBPreleaseref(b->batCacheid);
		throw(SQL, name, "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	gi = bat_iterator(g);
	ocmp = BATatoms[b->ttype].atomCmp;
	gcmp = BATatoms[g->ttype].atomCmp;
	oc = BUNtail(bi, BUNfirst(b));
	gc = BUNtail(gi, BUNfirst(g));
	if (!ALIGNsynced(b, g)) {
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(g->batCacheid);
		throw(SQL, name, "bats not aligned");
	}
/*
	if (!BATtordered(b)) {
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(g->batCacheid);
		throw(SQL, name, "bat not sorted");
	}
*/
	r = BATnew(TYPE_oid, TYPE_int, BATcount(b));
	if (r == NULL) {
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(g->batCacheid);
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
		BUNins(r, BUNhead(bi, p), &rank, FALSE);
		nrank += !dense || c;
	}
	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);
	BBPkeepref(*rid = r->batCacheid);
	return MAL_SUCCEED;
}

static str
do_sql_rank(bat *rid, bat *bid, int nrank, int dense, const char *name)
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
	cmp = BATatoms[b->ttype].atomCmp;
	cur = BUNtail(bi, BUNfirst(b));
	r = BATnew(TYPE_oid, TYPE_int, BATcount(b));
	if (r == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, name, "cannot allocate result bat");
	}
	if (BATtdense(b)) {
		BATloop(b, p, q) {
			BUNins(r, BUNhead(bi, p), &rank, FALSE);
			rank++;
		}
	} else {
		BATloop(b, p, q) {
			n = BUNtail(bi, p);
			if ((c = cmp(n, cur)) != 0)
				rank = nrank;
			cur = n;
			BUNins(r, BUNhead(bi, p), &rank, FALSE);
			nrank += !dense || c;
		}
	}
	BBPunfix(b->batCacheid);
	BBPkeepref(*rid = r->batCacheid);
	return MAL_SUCCEED;
}

str
sql_rank_grp(bat *rid, bat *bid, bat *gid, bat *gpe)
{
	(void) gpe;
	return do_sql_rank_grp(rid, bid, gid, 1, 0, "sql.rank_grp");
}

str
sql_dense_rank_grp(bat *rid, bat *bid, bat *gid, bat *gpe)
{
	(void) gpe;
	return do_sql_rank_grp(rid, bid, gid, 2, 1, "sql.dense_rank_grp");
}

str
sql_rank(bat *rid, bat *bid)
{
	return do_sql_rank(rid, bid, 1, 0, "sql.rank");
}

str
sql_dense_rank(bat *rid, bat *bid)
{
	return do_sql_rank(rid, bid, 2, 1, "sql.dense_rank");
}

str
SQLargRecord(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str s, t, *ret;

	(void) cntxt;
	ret = (str *) getArgReference(stk, pci, 0);
	s = instruction2str(mb, stk, getInstrPtr(mb, 0), LIST_MAL_DEBUG);
	t = strchr(s, ' ');
	*ret = GDKstrdup(t ? t + 1 : s);
	GDKfree(s);
	return MAL_SUCCEED;
}

/*
 * The table is searched for all columns and they are
 * re-clustered on the hash value over the  primary key.
 * Initially the first column
 */

str
SQLcluster1(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *sch = (str *) getArgReference(stk, pci, 1);
	str *tbl = (str *) getArgReference(stk, pci, 2);
	sql_trans *tr;
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	mvc *m = NULL;
	str msg;
	int first = 1;
	bat mid, hid, bid;
	BAT *map = NULL, *b;
	node *o;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, *sch);
	if (s == NULL)
		throw(SQL, "sql.cluster", "3F000!Schema missing");
	t = mvc_bind_table(m, s, *tbl);
	if (t == NULL)
		throw(SQL, "sql.cluster", "42S02!Table missing");
	tr = m->session->tr;
	t->base.wtime = s->base.wtime = tr->wtime = tr->wstime;
	t->base.rtime = s->base.rtime = tr->rtime = tr->stime;

	/* actually build the hash on the multi-column primary key */

	for (o = t->columns.set->h; o; o = o->next) {
		sql_delta *d;
		c = o->data;
		if (first) {
			first = 0;
			b = store_funcs.bind_col(tr, c, 0);
			msg = CLUSTER_key(&hid, &b->batCacheid);
			BBPreleaseref(b->batCacheid);
			if (msg)
				return msg;
			msg = CLUSTER_map(&mid, &hid);
			BBPdecref(hid, TRUE);
			if (msg)
				return msg;
			map = BATdescriptor(mid);
			if (map == NULL)
				throw(SQL, "sql.cluster", "Can not access descriptor");
		}

		b = store_funcs.bind_col(tr, c, 0);
		if (b == NULL)
			throw(SQL, "sql.cluster", "Can not access descriptor");
		msg = CLUSTER_apply(&bid, b, map);
		BBPreleaseref(b->batCacheid);
		if (msg) {
			BBPreleaseref(map->batCacheid);
			return msg;
		}
		d = c->data;
		if (d->bid)
			BBPdecref(d->bid, TRUE);
		if (d->ibid)
			BBPdecref(d->ibid, TRUE);
		d->bid = 0;
		d->ibase = 0;
		d->ibid = bid;	/* use the insert bat */
		c->base.wtime = tr->wstime;
		c->base.rtime = tr->stime;
	}
	/* bat was cleared */
	t->cleared = 1;
	if (map) {
		BBPreleaseref(map->batCacheid);
		BBPdecref(mid, TRUE);
	}
	return MAL_SUCCEED;
}

str
SQLcluster2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *sch = (str *) getArgReference(stk, pci, 1);
	str *tbl = (str *) getArgReference(stk, pci, 2);
	sql_trans *tr;
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	mvc *m = NULL;
	str msg;
	int first = 1;
	bat mid, hid, bid;
	BAT *b;
	node *o;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, *sch);
	if (s == NULL)
		throw(SQL, "sql.cluster", "3F000!Schema missing");
	t = mvc_bind_table(m, s, *tbl);
	if (t == NULL)
		throw(SQL, "sql.cluster", "42S02!Table missing");
	tr = m->session->tr;

	t->base.wtime = s->base.wtime = tr->wtime = tr->wstime;
	t->base.rtime = s->base.rtime = tr->rtime = tr->stime;
	for (o = t->columns.set->h; o; o = o->next) {
		sql_delta *d;
		c = o->data;
		if (first) {
			bat psum;
			unsigned int bits = 10, off = 0;
			first = 0;
			b = store_funcs.bind_col(tr, c, 0);
			msg = MKEYbathash(&hid, &b->batCacheid);
			BBPreleaseref(b->batCacheid);
			if (msg)
				return msg;
			msg = CLS_create_wrd(&psum, &mid, &hid, &bits, &off);
			BBPdecref(hid, TRUE);
			BBPdecref(psum, TRUE);
			if (msg)
				return msg;
		}

		b = store_funcs.bind_col(tr, c, 0);
		if (b == NULL)
			throw(SQL, "sql.cluster", "Can not access descriptor");
		msg = CLS_map(&bid, &mid, &b->batCacheid);
		BBPreleaseref(b->batCacheid);
		if (msg) {
			BBPreleaseref(bid);
			return msg;
		}

		d = c->data;
		if (d->bid)
			BBPdecref(d->bid, TRUE);
		if (d->ibid)
			BBPdecref(d->ibid, TRUE);
		d->bid = 0;
		d->ibase = 0;
		d->ibid = bid;	/* use the insert bat */

		c->base.wtime = tr->wstime;
		c->base.rtime = tr->stime;
	}
	/* bat was cleared */
	t->cleared = 1;
	return MAL_SUCCEED;
}

/*
 * Vacuum cleaning tables
 * Shrinking and re-using space to vacuum clean the holes in the relations.
 */
static str
vacuum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, str (*func) (int *, int *, int *), const char *name)
{
	str *sch = (str *) getArgReference(stk, pci, 1);
	str *tbl = (str *) getArgReference(stk, pci, 2);
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

	i = 0;
	bids[i] = 0;
	for (o = t->columns.set->h; o; o = o->next, i++) {
		c = o->data;
		b = store_funcs.bind_col(tr, c, 0);
		if (b == NULL || (msg = (*func) (&bid, &(b->batCacheid), &(del->batCacheid))) != NULL) {
			for (i--; i >= 0; i--)
				BBPdecref(bids[i], TRUE);
			if (b)
				BBPreleaseref(b->batCacheid);
			BBPreleaseref(del->batCacheid);
			if (!msg)
				throw(SQL, name, "Can not access descriptor");
			return msg;
		}
		BBPreleaseref(b->batCacheid);
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

	mvc_clear_table(m, t);
	for (o = t->columns.set->h, i = 0; o; o = o->next, i++) {
		sql_column *c = o->data;
		BAT *ins = BATdescriptor(bids[i]);	/* use the insert bat */

		if( ins){
			store_funcs.append_col(tr, c, ins, TYPE_bat);
			BBPreleaseref(ins->batCacheid);
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
	str *sch = (str *) getArgReference(stk, pci, 1);
	str *tbl = (str *) getArgReference(stk, pci, 2);
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
		b = store_funcs.bind_col(tr, c, 0);
		if (b == NULL)
			throw(SQL, "sql.vacuum", "Can not access descriptor");
		ordered |= BATtordered(b);
		cnt = BATcount(b);
		BBPreleaseref(b->batCacheid);
	}

	/* get the deletions BAT */
	del = mvc_bind_dbat(m, *sch, *tbl, RD_INS);

	/* now decide on the algorithm */
	if (ordered) {
		if (BATcount(del) > cnt / 20)
			SQLshrink(cntxt, mb, stk, pci);
	} else
		SQLreuse(cntxt, mb, stk, pci);

	BBPreleaseref(del->batCacheid);
	return MAL_SUCCEED;
}

/*
 * The drop_hash operation cleans up any hash indices on any of the tables columns.
 */
str
SQLdrop_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *sch = (str *) getArgReference(stk, pci, 1);
	str *tbl = (str *) getArgReference(stk, pci, 2);
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
		BBPreleaseref(b->batCacheid);
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
 * returns table ("schema" string, "table" string, "column" string, "type" string, location string, "count" bigint, width int, columnsize bigint, heapsize bigint indices bigint, sorted int)
 * external name sql.storage;
 */
str
sql_storage(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *sch, *tab, *col, *type, *loc, *cnt, *atom, *size, *heap, *indices, *sort, *imprints;
	mvc *m = NULL;
	str msg;
	sql_trans *tr;
	node *nsch, *ntab, *ncol;
	int w;
	int *rsch = (int *) getArgReference(stk, pci, 0);
	int *rtab = (int *) getArgReference(stk, pci, 1);
	int *rcol = (int *) getArgReference(stk, pci, 2);
	int *rtype = (int *) getArgReference(stk, pci, 3);
	int *rloc = (int *) getArgReference(stk, pci, 4);
	int *rcnt = (int *) getArgReference(stk, pci, 5);
	int *ratom = (int *) getArgReference(stk, pci, 6);
	int *rsize = (int *) getArgReference(stk, pci, 7);
	int *rheap = (int *) getArgReference(stk, pci, 8);
	int *rindices = (int *) getArgReference(stk, pci, 9);
	int *rimprints = (int *) getArgReference(stk, pci, 10);
	int *rsort = (int *) getArgReference(stk, pci, 11);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	tr = m->session->tr;
	sch = BATnew(TYPE_void, TYPE_str, 0);
	BATseqbase(sch, 0);
	tab = BATnew(TYPE_void, TYPE_str, 0);
	BATseqbase(tab, 0);
	col = BATnew(TYPE_void, TYPE_str, 0);
	BATseqbase(col, 0);
	type = BATnew(TYPE_void, TYPE_str, 0);
	BATseqbase(type, 0);
	loc = BATnew(TYPE_void, TYPE_str, 0);
	BATseqbase(loc, 0);
	cnt = BATnew(TYPE_void, TYPE_lng, 0);
	BATseqbase(cnt, 0);
	atom = BATnew(TYPE_void, TYPE_int, 0);
	BATseqbase(atom, 0);
	size = BATnew(TYPE_void, TYPE_lng, 0);
	BATseqbase(size, 0);
	heap = BATnew(TYPE_void, TYPE_lng, 0);
	BATseqbase(heap, 0);
	indices = BATnew(TYPE_void, TYPE_lng, 0);
	BATseqbase(indices, 0);
	imprints = BATnew(TYPE_void, TYPE_lng, 0);
	BATseqbase(imprints, 0);
	sort = BATnew(TYPE_void, TYPE_bit, 0);
	BATseqbase(sort, 0);
	if (sch == NULL || tab == NULL || col == NULL || type == NULL || loc == NULL || imprints == NULL || sort == NULL || cnt == NULL || atom == NULL || size == NULL || heap == NULL || indices == NULL) {
		if (sch)
			BBPreleaseref(sch->batCacheid);
		if (tab)
			BBPreleaseref(tab->batCacheid);
		if (col)
			BBPreleaseref(col->batCacheid);
		if (loc)
			BBPreleaseref(loc->batCacheid);
		if (cnt)
			BBPreleaseref(cnt->batCacheid);
		if (type)
			BBPreleaseref(type->batCacheid);
		if (atom)
			BBPreleaseref(atom->batCacheid);
		if (size)
			BBPreleaseref(size->batCacheid);
		if (heap)
			BBPreleaseref(heap->batCacheid);
		if (indices)
			BBPreleaseref(indices->batCacheid);
		if (imprints)
			BBPreleaseref(imprints->batCacheid);
		if (sort)
			BBPreleaseref(sort->batCacheid);
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
								BAT *bn = store_funcs.bind_col(tr, c, 0);
								lng sz;

								/*printf("schema %s.%s.%s" , b->name, bt->name, bc->name); */
								sch = BUNappend(sch, b->name, FALSE);
								tab = BUNappend(tab, bt->name, FALSE);
								col = BUNappend(col, bc->name, FALSE);
								type = BUNappend(type, c->type.type->sqlname, FALSE);

								/*printf(" cnt "BUNFMT, BATcount(bn)); */
								sz = BATcount(bn);
								cnt = BUNappend(cnt, &sz, FALSE);

								/*printf(" loc %s", BBP_physical(bn->batCacheid)); */
								loc = BUNappend(loc, BBP_physical(bn->batCacheid), FALSE);
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
								atom = BUNappend(atom, &w, FALSE);

								sz = tailsize(bn, BATcount(bn));
								sz += headsize(bn, BATcount(bn));
								size = BUNappend(size, &sz, FALSE);

								sz = bn->T->vheap ? bn->T->vheap->size : 0;
								sz += bn->H->vheap ? bn->H->vheap->size : 0;
								heap = BUNappend(heap, &sz, FALSE);

								sz = bn->T->hash ? bn->T->hash->heap->size : 0;
								sz += bn->H->hash ? bn->H->hash->heap->size : 0;
								indices = BUNappend(indices, &sz, FALSE);
								sz = IMPSimprintsize(bn);
								imprints = BUNappend(imprints, &sz, FALSE);
								/*printf(" indices "BUNFMT, bn->T->hash?bn->T->hash->heap->size:0); */
								/*printf("\n"); */

								w = BATtordered(bn);
								sort = BUNappend(sort, &w, FALSE);
								BBPunfix(bn->batCacheid);
							}

					if (isTable(t))
						if (t->idxs.set)
							for (ncol = (t)->idxs.set->h; ncol; ncol = ncol->next) {
								sql_base *bc = ncol->data;
								sql_idx *c = (sql_idx *) ncol->data;
								if (c->type != no_idx) {
									BAT *bn = store_funcs.bind_idx(tr, c, 0);
									lng sz;

									/*printf("schema %s.%s.%s" , b->name, bt->name, bc->name); */
									sch = BUNappend(sch, b->name, FALSE);
									tab = BUNappend(tab, bt->name, FALSE);
									col = BUNappend(col, bc->name, FALSE);
									type = BUNappend(type, "oid", FALSE);

									/*printf(" cnt "BUNFMT, BATcount(bn)); */
									sz = BATcount(bn);
									cnt = BUNappend(cnt, &sz, FALSE);

									/*printf(" loc %s", BBP_physical(bn->batCacheid)); */
									loc = BUNappend(loc, BBP_physical(bn->batCacheid), FALSE);
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
									atom = BUNappend(atom, &w, FALSE);
									/*printf(" size "BUNFMT, tailsize(bn,BATcount(bn)) + (bn->T->vheap? bn->T->vheap->size:0)); */
									sz = tailsize(bn, BATcount(bn));
									sz += headsize(bn, BATcount(bn));
									size = BUNappend(size, &sz, FALSE);

									sz = bn->T->vheap ? bn->T->vheap->size : 0;
									sz += bn->H->vheap ? bn->H->vheap->size : 0;
									heap = BUNappend(heap, &sz, FALSE);

									sz = bn->T->hash ? bn->T->hash->heap->size : 0;
									sz += bn->H->hash ? bn->H->hash->heap->size : 0;
									indices = BUNappend(indices, &sz, FALSE);
									sz = IMPSimprintsize(bn);
									imprints = BUNappend(imprints, &sz, FALSE);
									/*printf(" indices "BUNFMT, bn->T->hash?bn->T->hash->heap->size:0); */
									/*printf("\n"); */
									w = BATtordered(bn);
									sort = BUNappend(sort, &w, FALSE);
									BBPunfix(bn->batCacheid);
								}
							}

				}
	}

	BBPkeepref(*rsch = sch->batCacheid);
	BBPkeepref(*rtab = tab->batCacheid);
	BBPkeepref(*rcol = col->batCacheid);
	BBPkeepref(*rloc = loc->batCacheid);
	BBPkeepref(*rtype = type->batCacheid);
	BBPkeepref(*rcnt = cnt->batCacheid);
	BBPkeepref(*ratom = atom->batCacheid);
	BBPkeepref(*rsize = size->batCacheid);
	BBPkeepref(*rheap = heap->batCacheid);
	BBPkeepref(*rindices = indices->batCacheid);
	BBPkeepref(*rimprints = imprints->batCacheid);
	BBPkeepref(*rsort = sort->batCacheid);
	return MAL_SUCCEED;
}

str
RAstatement(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int pos = 0;
	str *expr = (str *) getArgReference(stk, pci, 1);
	bit *opt = (bit *) getArgReference(stk, pci, 2);
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
		char *msg;
		MalStkPtr oldglb = cntxt->glb;

		if (*opt)
			rel = rel_optimizer(m, rel);
		s = output_rel_bin(m, rel);
		rel_destroy(rel);

		MSinitClientPrg(cntxt, "user", "test");

		/* generate MAL code */
		backend_callinline(b, cntxt, s);
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

void
freeVariables(Client c, MalBlkPtr mb, MalStkPtr glb, int start)
{
	int i, j;

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
	for (i = j = 0; i < mb->ptop; i++) {
		if (mb->prps[i].var <start) {
			if (i > j)
				mb->prps[j] = mb->prps[i];
			j++;
		}
	}
	mb->ptop = j;
}

/* if at least (2*SIZEOF_BUN), also store length (heaps are then
 * incompatible) */
#define EXTRALEN ((SIZEOF_BUN + GDK_VARALIGN - 1) & ~(GDK_VARALIGN - 1))

str
STRindex_int(int *i, str src, bit *u)
{
	(void)src; (void)u;
	*i = 0;
	return MAL_SUCCEED;
}

str
BATSTRindex_int(bat *res, bat *src, bit *u)
{
	BAT *s, *r;

	if ((s = BATdescriptor(*src)) == NULL)
		throw(SQL, "calc.index", "Cannot access descriptor");
	
	if (*u) {
		Heap *h = s->T->vheap;
		size_t pad, pos;
		const size_t extralen = h->hashash ? EXTRALEN : 0;
		int v;

		r = BATnew(TYPE_void, TYPE_int, 1024);
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
		r = VIEWcreate(s, s);
		r->ttype = TYPE_int;
		r->tvarsized = 0;
		r->T->vheap = NULL;
	}
	BBPunfix(s->batCacheid);
	BBPkeepref((*res = r->batCacheid));
	return MAL_SUCCEED;
}

str
STRindex_sht(sht *i, str src, bit *u)
{
	(void)src; (void)u;
	*i = 0;
	return MAL_SUCCEED;
}

str
BATSTRindex_sht(bat *res, bat *src, bit *u)
{
	BAT *s, *r;

	if ((s = BATdescriptor(*src)) == NULL)
		throw(SQL, "calc.index", "Cannot access descriptor");
	
	if (*u) {
		Heap *h = s->T->vheap;
		size_t pad, pos;
		const size_t extralen = h->hashash ? EXTRALEN : 0;
		sht v;

		r = BATnew(TYPE_void, TYPE_sht, 1024);
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
		r = VIEWcreate(s, s);
		r->ttype = TYPE_sht;
		r->tvarsized = 0;
		r->T->vheap = NULL;
	}
	BBPunfix(s->batCacheid);
	BBPkeepref((*res = r->batCacheid));
	return MAL_SUCCEED;
}

str
STRindex_bte(bte *i, str src, bit *u)
{
	(void)src; (void)u;
	*i = 0;
	return MAL_SUCCEED;
}

str
BATSTRindex_bte(bat *res, bat *src, bit *u)
{
	BAT *s, *r;

	if ((s = BATdescriptor(*src)) == NULL)
		throw(SQL, "calc.index", "Cannot access descriptor");
	
	if (*u) {
		Heap *h = s->T->vheap;
		size_t pad, pos;
		const size_t extralen = h->hashash ? EXTRALEN : 0;
		bte v;

		r = BATnew(TYPE_void, TYPE_bte, 64);
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
		r = VIEWcreate(s, s);
		r->ttype = TYPE_bte;
		r->tvarsized = 0;
		r->T->vheap = NULL;
	}
	BBPunfix(s->batCacheid);
	BBPkeepref((*res = r->batCacheid));
	return MAL_SUCCEED;
}

str
STRstrings(str *i, str src)
{
	(void)src;
	*i = 0;
	return MAL_SUCCEED;
}

str
BATSTRstrings(bat *res, bat *src)
{
	BAT *s, *r;
	Heap *h;
	size_t pad, pos;
	size_t extralen;

	if ((s = BATdescriptor(*src)) == NULL)
		throw(SQL, "calc.strings", "Cannot access descriptor");
	
       	h = s->T->vheap;
       	extralen = h->hashash ? EXTRALEN : 0;
	r = BATnew(TYPE_void, TYPE_str, 1024);
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
