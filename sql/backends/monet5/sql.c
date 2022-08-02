/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
#include "mapi_prompt.h"
#include "sql_result.h"
#include "sql_gencode.h"
#include "sql_storage.h"
#include "sql_scenario.h"
#include "store_sequence.h"
#include "sql_optimizer.h"
#include "sql_datetime.h"
#include "sql_partition.h"
#include "rel_unnest.h"
#include "rel_optimizer.h"
#include "rel_partition.h"
#include "rel_distribute.h"
#include "rel_select.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_dump.h"
#include "rel_bin.h"
#include "bbp.h"
#include "opt_pipes.h"
#include "orderidx.h"
#include "clients.h"
#include "mal_instruction.h"
#include "mal_resource.h"
#include "mal_authorize.h"
#include "gdk_cand.h"

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
rel_no_mitosis(sql_rel *rel)
{
	int is_point = 0;

	if (!rel || is_basetable(rel->op))
		return 1;
	if (is_topn(rel->op) || is_sample(rel->op) || is_simple_project(rel->op))
		return rel_no_mitosis(rel->l);
	if (is_modify(rel->op) && rel->card <= CARD_AGGR) {
		if (is_delete(rel->op))
			return 1;
		return rel_no_mitosis(rel->r);
	}
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

	while (!need_distinct && rel && is_simple_project(rel->op))
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
sql_symbol2relation(mvc *sql, symbol *sym)
{
	sql_rel *rel;
	sql_query *query = query_create(sql);
	int top = sql->topvars;

	rel = rel_semantic(query, sym);
	if (rel)
		rel = sql_processrelation(sql, rel, 1);
	if (rel)
		rel = rel_distribute(sql, rel);
	if (rel)
		rel = rel_partition(sql, rel);
	if (rel && (rel_no_mitosis(rel) || rel_need_distinct_query(rel)))
		sql->no_mitosis = 1;

	/* On explain and plan modes, drop declared variables after generating the AST */
	if ((sql->emod & mod_explain) || (sql->emode != m_normal && sql->emode != m_execute))
		stack_pop_until(sql, top);
	return rel;
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
			sql_trans_end(c->session, 1);
			store_apply_deltas(true);
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
	c->no_mitosis = 0;
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
		throw(SQL, "mvc", SQLSTATE(42005) "No client record");
	if (cntxt->sqlcontext == NULL)
		throw(SQL, "mvc", SQLSTATE(42006) "SQL module not initialized");
	be = (backend *) cntxt->sqlcontext;
	if (be->mvc == NULL)
		throw(SQL, "mvc", SQLSTATE(42006) "SQL module not initialized, mvc struct missing");
	return MAL_SUCCEED;
}

str
getSQLContext(Client cntxt, MalBlkPtr mb, mvc **c, backend **b)
{
	backend *be;
	(void) mb;
	str msg;

	if ((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;
	be = (backend *) cntxt->sqlcontext;
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
SQLcommit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg;
	(void) stk;
	(void) pci;

	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if (sql->session->auto_commit != 0)
		throw(SQL, "sql.trans", SQLSTATE(2DM30) "COMMIT not allowed in auto commit mode");
	return mvc_commit(sql, 0, 0, false);
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

	if (sql->session->tr->active) {
		msg = mvc_rollback(sql, 0, NULL, false);
	}
	return msg;
}

str
SQLshutdown_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg;

	if ((msg = CLTshutdown(cntxt, mb, stk, pci)) == MAL_SUCCEED) {
		/* administer the shutdown in the system log */
		TRC_INFO(SQL_TRANS, "Shutdown: %s\n", *getArgReference_str(stk, pci, 0));
	}
	return msg;
}

str
create_table_or_view(mvc *sql, char* sname, char *tname, sql_table *t, int temp)
{
	sql_allocator *osa;
	sql_schema *s = mvc_bind_schema(sql, sname);
	sql_table *nt = NULL;
	node *n;
	int check = 0;

	if (STORE_READONLY)
		return sql_error(sql, 06, SQLSTATE(25006) "schema statements cannot be executed on a readonly database.");

	if (!s)
		return sql_message(SQLSTATE(3F000) "CREATE %s: schema '%s' doesn't exist", (t->query) ? "TABLE" : "VIEW", sname);
	if (mvc_bind_table(sql, s, t->base.name)) {
		char *cd = (temp == SQL_DECLARED_TABLE) ? "DECLARE" : "CREATE";
		return sql_message(SQLSTATE(42S01) "%s TABLE: name '%s' already in use", cd, t->base.name);
	} else if (temp != SQL_DECLARED_TABLE && (!mvc_schema_privs(sql, s) && !(isTempSchema(s) && temp == SQL_LOCAL_TEMP))) {
		return sql_message(SQLSTATE(42000) "CREATE TABLE: insufficient privileges for user '%s' in schema '%s'", stack_get_string(sql, "current_user"), s->base.name);
	} else if (temp == SQL_DECLARED_TABLE && !list_empty(t->keys.set)) {
		return sql_message(SQLSTATE(42000) "DECLARE TABLE: '%s' cannot have constraints", t->base.name);
	}

	osa = sql->sa;
	sql->sa = NULL;

	nt = sql_trans_create_table(sql->session->tr, s, tname, t->query, t->type, t->system, temp, t->commit_action,
								t->sz, t->properties);

	/* first check default values */
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;

		if (c->def) {
			char *buf, *typestr;
			sql_rel *r = NULL;
			list *id_l;

			sql->sa = sa_create();
			if (!sql->sa) {
				sql->sa = osa;
				throw(SQL, "sql.catalog",SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			buf = sa_alloc(sql->sa, strlen(c->def) + 8);
			if (!buf) {
				sa_destroy(sql->sa);
				sql->sa = osa;
				throw(SQL, "sql.catalog",SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			typestr = subtype2string2(&c->type);
			if (!typestr) {
				sa_destroy(sql->sa);
				sql->sa = osa;
				throw(SQL, "sql.catalog",SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			snprintf(buf, BUFSIZ, "select cast(%s as %s);", c->def, typestr);
			_DELETE(typestr);
			r = rel_parse(sql, s, buf, m_deps);
			if (!r || !is_project(r->op) || !r->exps || list_length(r->exps) != 1 ||
				exp_check_type(sql, &c->type, r, r->exps->h->data, type_equal) == NULL) {
				if (r)
					rel_destroy(r);
				sa_destroy(sql->sa);
				sql->sa = osa;
				if (strlen(sql->errstr) > 6 && sql->errstr[5] == '!')
					throw(SQL, "sql.catalog", "%s", sql->errstr);
				else
					throw(SQL, "sql.catalog", SQLSTATE(42000) "%s", sql->errstr);
			}
			id_l = rel_dependencies(sql, r);
			mvc_create_dependencies(sql, id_l, nt->base.id, FUNC_DEPENDENCY);
			rel_destroy(r);
			sa_destroy(sql->sa);
			sql->sa = NULL;
		}
	}

	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data, *copied = mvc_copy_column(sql, nt, c);

		if (copied == NULL) {
			sql->sa = osa;
			throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: %s_%s_%s conflicts", s->base.name, t->base.name, c->base.name);
		}
		if (isPartitionedByColumnTable(t) && c->base.id == t->part.pcol->base.id)
			nt->part.pcol = copied;
	}
	if (isPartitionedByExpressionTable(t)) {
		char *err = NULL;

		nt->part.pexp->exp = sa_strdup(sql->session->tr->sa, t->part.pexp->exp);

		sql->sa = sa_create();
		if (!sql->sa) {
			sql->sa = osa;
			throw(SQL, "sql.catalog",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}

		err = bootstrap_partition_expression(sql, sql->session->tr->sa, nt, 1);
		sa_destroy(sql->sa);
		sql->sa = NULL;
		if (err) {
			sql->sa = osa;
			return err;
		}
	}
	check = sql_trans_set_partition_table(sql->session->tr, nt);
	if (check == -1) {
		sql->sa = osa;
		throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: %s_%s: the partition's expression is too long", s->base.name, t->base.name);
	} else if (check) {
		sql->sa = osa;
		throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: %s_%s: an internal error occurred", s->base.name, t->base.name);
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
			char *err = NULL;

			sql->sa = sa_create();
			if(!sql->sa) {
				sql->sa = osa;
				throw(SQL, "sql.catalog",SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}

			err = sql_partition_validate_key(sql, nt, k, "CREATE");
			sa_destroy(sql->sa);
			sql->sa = NULL;
			if (err) {
				sql->sa = osa;
				return err;
			}
			mvc_copy_key(sql, nt, k);
		}
	}
	/*
	if (t->members) {
		for (n = t->members->h; n; n = n->next) {
			sql_part *pt = n->data;
			mvc_copy_part(sql, nt, pt);
		}
	}
	*/
	if (t->triggers.set) {
		for (n = t->triggers.set->h; n; n = n->next) {
			sql_trigger *tr = n->data;
			mvc_copy_trigger(sql, nt, tr);
		}
	}
	/* also create dependencies when not renaming */
	if (nt->query && isView(nt)) {
		sql_rel *r = NULL;

		sql->sa = sa_create();
		if (!sql->sa) {
			sql->sa = osa;
			throw(SQL, "sql.catalog",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		r = rel_parse(sql, s, nt->query, m_deps);
		if (r)
			r = sql_processrelation(sql, r, 0);
		if (r) {
			list *id_l = rel_dependencies(sql, r);
			mvc_create_dependencies(sql, id_l, nt->base.id, VIEW_DEPENDENCY);
		}
		sa_destroy(sql->sa);
		if (!r) {
			if (strlen(sql->errstr) > 6 && sql->errstr[5] == '!')
				throw(SQL, "sql.catalog", "%s", sql->errstr);
			else
				throw(SQL, "sql.catalog", SQLSTATE(42000) "%s", sql->errstr);
		}
	}
	sql->sa = osa;
	return MAL_SUCCEED;
}

str
create_table_from_emit(Client cntxt, char *sname, char *tname, sql_emit_col *columns, size_t ncols)
{
	size_t i;
	sql_table *t;
	sql_schema *s;
	mvc *sql = NULL;
	str msg = MAL_SUCCEED;

	if ((msg = getSQLContext(cntxt, NULL, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	/* for some reason we don't have an allocator here, so make one */
	if (!(sql->sa = sa_create())) {
		msg = sql_error(sql, 02, SQLSTATE(HY013) "CREATE TABLE: %s", MAL_MALLOC_FAIL);
		goto cleanup;
	}

	if (!sname)
		sname = "sys";
	if (!(s = mvc_bind_schema(sql, sname))) {
		msg = sql_error(sql, 02, SQLSTATE(3F000) "CREATE TABLE: no such schema '%s'", sname);
		goto cleanup;
	}
	if (!mvc_schema_privs(sql, s)) {
		msg = sql_error(sql, 02, SQLSTATE(42000) "CREATE TABLE: Access denied for %s to schema '%s'", stack_get_string(sql, "current_user"), s->base.name);
		goto cleanup;
	}
	if (!(t = mvc_create_table(sql, s, tname, tt_table, 0, SQL_DECLARED_TABLE, CA_COMMIT, -1, 0))) {
		msg = sql_error(sql, 02, SQLSTATE(3F000) "CREATE TABLE: could not create table '%s'", tname);
		goto cleanup;
	}

	for (i = 0; i < ncols; i++) {
		BAT *b = columns[i].b;
		str atoname = ATOMname(b->ttype);
		sql_subtype tpe;
		sql_column *col = NULL;

		if (!strcmp(atoname, "str"))
			sql_find_subtype(&tpe, "clob", 0, 0);
		else {
			sql_subtype *t = sql_bind_localtype(atoname);
			if (!t) {
				msg = sql_error(sql, 02, SQLSTATE(3F000) "CREATE TABLE: could not find type for column");
				goto cleanup;
			}
			tpe = *t;
		}

		if (columns[i].name && columns[i].name[0] == '%') {
			msg = sql_error(sql, 02, SQLSTATE(42000) "CREATE TABLE: generated labels not allowed in column names, use an alias instead");
			goto cleanup;
		} else if (!(col = mvc_create_column(sql, t, columns[i].name, &tpe))) {
			msg = sql_error(sql, 02, SQLSTATE(3F000) "CREATE TABLE: could not create column %s", columns[i].name);
			goto cleanup;
		}
	}
	if ((msg = create_table_or_view(sql, sname, t->base.name, t, 0)) != MAL_SUCCEED)
		goto cleanup;
	if (!(t = mvc_bind_table(sql, s, tname))) {
		msg = sql_error(sql, 02, SQLSTATE(3F000) "CREATE TABLE: could not bind table %s", tname);
		goto cleanup;
	}
	for (i = 0; i < ncols; i++) {
		BAT *b = columns[i].b;
		sql_column *col = NULL;

		if (!(col = mvc_bind_column(sql, t, columns[i].name))) {
			msg = sql_error(sql, 02, SQLSTATE(3F000) "CREATE TABLE: could not bind column %s", columns[i].name);
			goto cleanup;
		}
		if ((msg = mvc_append_column(sql->session->tr, col, b)) != MAL_SUCCEED)
			goto cleanup;
	}

cleanup:
	if(sql->sa) {
		sa_destroy(sql->sa);
		sql->sa = NULL;
	}
	return msg;
}

str
append_to_table_from_emit(Client cntxt, char *sname, char *tname, sql_emit_col *columns, size_t ncols)
{
	size_t i;
	sql_table *t;
	sql_schema *s;
	mvc *sql = NULL;
	str msg = MAL_SUCCEED;

	if ((msg = getSQLContext(cntxt, NULL, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	/* for some reason we don't have an allocator here, so make one */
	if (!(sql->sa = sa_create())) {
		msg = sql_error(sql, 02, SQLSTATE(HY013) "APPEND TABLE: %s", MAL_MALLOC_FAIL);
		goto cleanup;
	}

	if (!sname)
		sname = "sys";
	if (!(s = mvc_bind_schema(sql, sname))) {
		msg = sql_error(sql, 02, SQLSTATE(3F000) "APPEND TABLE: no such schema '%s'", sname);
		goto cleanup;
	}
	if (!(t = mvc_bind_table(sql, s, tname))) {
		msg = sql_error(sql, 02, SQLSTATE(3F000) "APPEND TABLE: could not bind table %s", tname);
		goto cleanup;
	}
	for (i = 0; i < ncols; i++) {
		BAT *b = columns[i].b;
		sql_column *col = NULL;

		if (!(col = mvc_bind_column(sql,t, columns[i].name))) {
			msg = sql_error(sql, 02, SQLSTATE(3F000) "APPEND TABLE: could not bind column %s", columns[i].name);
			goto cleanup;
		}
		if ((msg = mvc_append_column(sql->session->tr, col, b)) != MAL_SUCCEED)
			goto cleanup;
	}

cleanup:
	if(sql->sa) {
		sa_destroy(sql->sa);
		sql->sa = NULL;
	}
	return msg;
}

BAT *
mvc_bind(mvc *m, const char *sname, const char *tname, const char *cname, int access)
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

str
SQLcatalog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return sql_message(SQLSTATE(25006) "Deprecated statement");
}

/* setVariable(int *ret, str *name, any value) */
str
setVariable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res = getArgReference_int(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	const char *varname = *getArgReference_str(stk, pci, 2);
	int mtype = getArgType(mb, pci, 3);
	ValPtr ptr;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	*res = 0;
	if (mtype < 0 || mtype >= 255)
		throw(SQL, "sql.setVariable", SQLSTATE(42100) "Variable type error");
	if (strcmp("optimizer", varname) == 0) {
		const char *newopt = *getArgReference_str(stk, pci, 3);
		if (newopt) {
			char buf[BUFSIZ];

			if (strNil(newopt))
				throw(SQL, "sql.setVariable", SQLSTATE(42000) "optimizer cannot be NULL");
			if (!isOptimizerPipe(newopt) && strchr(newopt, (int) ';') == 0)
				throw(SQL, "sql.setVariable", SQLSTATE(42100) "optimizer '%s' unknown", newopt);
			snprintf(buf, BUFSIZ, "user_%d", cntxt->idx);
			if (!isOptimizerPipe(newopt) || strcmp(buf, newopt) == 0) {
				msg = addPipeDefinition(cntxt, buf, newopt);
				if (msg)
					return msg;
				if (stack_find_var(m, varname)) {
					if (!stack_set_string(m, varname, buf))
						throw(SQL, "sql.setVariable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			} else if (stack_find_var(m, varname)) {
				if (!stack_set_string(m, varname, newopt))
					throw(SQL, "sql.setVariable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
		return MAL_SUCCEED;
	}
	ptr = &stk->stk[getArg(pci, 3)];
	if (stack_find_var(m, varname)) {
		if ((msg = sql_update_var(m, varname, ptr)) != NULL)
			return msg;
		if (!stack_set_var(m, varname, ptr))
			throw(SQL, "sql.setVariable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	} else {
		throw(SQL, "sql.setVariable", SQLSTATE(42100) "variable '%s' unknown", varname);
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
	const char *varname = *getArgReference_str(stk, pci, 2);
	atom *a;
	ValRecord *dst, *src;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (mtype < 0 || mtype >= 255)
		throw(SQL, "sql.getVariable", SQLSTATE(42100) "Variable type error");
	if (!(a = stack_get_var(m, varname)))
		throw(SQL, "sql.getVariable", SQLSTATE(42100) "variable '%s' unknown", varname);
	src = &a->data;
	dst = &stk->stk[getArg(pci, 0)];
	if (VALcopy(dst, src) == NULL)
		throw(MAL, "sql.getVariable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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

	vars = COLnew(0, TYPE_str, m->topvars, TRANSIENT);
	if (vars == NULL)
		throw(SQL, "sql.variables", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	for (i = 0; i < m->topvars && !m->vars[i].frame; i++) {
		if (BUNappend(vars, m->vars[i].name, false) != GDK_SUCCEED) {
			BBPreclaim(vars);
			throw(SQL, "sql.variables", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
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
	const char *filename = *getArgReference_str(stk, pci, 1);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (m->scanner.log) {
		close_stream(m->scanner.log);
		m->scanner.log = NULL;
	}

	if (!strNil(filename)) {
		if((m->scanner.log = open_wastream(filename)) == NULL)
			throw(SQL, "sql.logfile", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	return MAL_SUCCEED;
}

/* str mvc_next_value(lng *res, str *sname, str *seqname); */
str
mvc_next_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	sql_schema *s;
	sql_sequence *seq;
	lng *res = getArgReference_lng(stk, pci, 0);
	const char *sname = *getArgReference_str(stk, pci, 1);
	const char *seqname = *getArgReference_str(stk, pci, 2);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (!(s = mvc_bind_schema(m, sname)))
		throw(SQL, "sql.next_value", SQLSTATE(3F000) "Cannot find the schema %s", sname);
	if (!mvc_schema_privs(m, s))
		throw(SQL, "sql.next_value", SQLSTATE(42000) "Access denied for %s to schema '%s'", stack_get_string(m, "current_user"), s->base.name);
	if (!(seq = find_sql_sequence(s, seqname)))
		throw(SQL, "sql.next_value", SQLSTATE(HY050) "Cannot find the sequence %s.%s", sname, seqname);

	if (seq_next_value(seq, res)) {
		m->last_id = *res;
		stack_set_number(m, "last_id", m->last_id);
		return MAL_SUCCEED;
	}
	throw(SQL, "sql.next_value", SQLSTATE(HY050) "Cannot generate next sequence value %s.%s", sname, seqname);
}

/* str mvc_get_value(lng *res, str *sname, str *seqname); */
str
mvc_get_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	sql_schema *s;
	sql_sequence *seq;
	lng *res = getArgReference_lng(stk, pci, 0);
	const char *sname = *getArgReference_str(stk, pci, 1);
	const char *seqname = *getArgReference_str(stk, pci, 2);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (!(s = mvc_bind_schema(m, sname)))
		throw(SQL, "sql.get_value", SQLSTATE(3F000) "Cannot find the schema %s", sname);
	if (!(seq = find_sql_sequence(s, seqname)))
		throw(SQL, "sql.get_value", SQLSTATE(HY050) "Cannot find the sequence %s.%s", sname, seqname);

	if (seq_get_value(seq, res))
		return MAL_SUCCEED;
	throw(SQL, "sql.get_value", SQLSTATE(HY050) "Cannot get sequence value %s.%s", sname, seqname);
}

static str
mvc_bat_next_get_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int (*bulk_func)(seqbulk *, lng *), const char *call, const char *action)
{
	mvc *m = NULL;
	str msg = MAL_SUCCEED, sname = NULL, seqname = NULL;
	BAT *b = NULL, *c = NULL, *r = NULL, *it;
	BUN p, q;
	sql_schema *s = NULL;
	sql_sequence *seq = NULL;
	seqbulk *sb = NULL;
	BATiter bi, ci;
	bat *res = getArgReference_bat(stk, pci, 0);
	bat schid = 0, seqid = 0;

	if (isaBatType(getArgType(mb, pci, 1)))
		schid = *getArgReference_bat(stk, pci, 1);
	else
		sname = *getArgReference_str(stk, pci, 1);
	if (isaBatType(getArgType(mb, pci, 2)))
		seqid = *getArgReference_bat(stk, pci, 2);
	else
		seqname = *getArgReference_str(stk, pci, 2);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if (schid && !(b = BATdescriptor(schid))) {
		msg = createException(SQL, call, SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	if (seqid && !(c = BATdescriptor(seqid))) {
		msg = createException(SQL, call, SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	assert(b || c);
	it = b ? b : c; /* Either b or c must be set */

	if (!(r = COLnew(it->hseqbase, TYPE_lng, BATcount(it), TRANSIENT))) {
		msg = createException(SQL, call, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	if (!BATcount(it))
		goto bailout; /* Success case */

	bi = bat_iterator(b);
	ci = bat_iterator(c);

	BATloop(it, p, q) {
		str nsname, nseqname;
		lng l;

		if (b)
			nsname = BUNtvar(bi, p);
		else
			nsname = sname;
		if (c)
			nseqname = BUNtvar(ci, p);
		else
			nseqname = seqname;

		if (!s || strcmp(s->base.name, nsname) != 0 || !seq || strcmp(seq->base.name, nseqname) != 0) {
			if (sb) {
				seqbulk_destroy(sb);
				sb = NULL;
			}
			seq = NULL;
			if ((!s || strcmp(s->base.name, nsname) != 0) && !(s = mvc_bind_schema(m, nsname))) {
				msg = createException(SQL, call, SQLSTATE(3F000) "Cannot find the schema %s", nsname);
				goto bailout;
			}
			if (bulk_func == seqbulk_next_value && !mvc_schema_privs(m, s)) {
				msg = createException(SQL, call, SQLSTATE(42000) "Access denied for %s to schema '%s'", stack_get_string(m, "current_user"), s->base.name);
				goto bailout;
			}
			if (!(seq = find_sql_sequence(s, nseqname)) || !(sb = seqbulk_create(seq, BATcount(it)))) {
				msg = createException(SQL, call, SQLSTATE(HY050) "Cannot find the sequence %s.%s", nsname, nseqname);
				goto bailout;
			}
		}
		if (!bulk_func(sb, &l)) {
			msg = createException(SQL, call, SQLSTATE(HY050) "Cannot %s sequence value %s.%s", action, nsname, nseqname);
			goto bailout;
		}
		if (BUNappend(r, &l, false) != GDK_SUCCEED) {
			msg = createException(SQL, call, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
	}

bailout:
	if (sb)
		seqbulk_destroy(sb);
	if (b)
		BBPunfix(b->batCacheid);
	if (c)
		BBPunfix(c->batCacheid);
	if (msg)
		BBPreclaim(r);
	else
		BBPkeepref(*res = r->batCacheid);
	return msg;
}

str
mvc_bat_next_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return mvc_bat_next_get_value(cntxt, mb, stk, pci, seqbulk_next_value, "sql.next_value", "generate next");
}

str
mvc_bat_get_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return mvc_bat_next_get_value(cntxt, mb, stk, pci, seqbulk_get_value, "sql.get_value", "get");
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
	sql_sequence *seq;
	lng *res = getArgReference_lng(stk, pci, 0);
	const char *sname = *getArgReference_str(stk, pci, 1);
	const char *seqname = *getArgReference_str(stk, pci, 2);
	lng start = *getArgReference_lng(stk, pci, 3);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (!(s = mvc_bind_schema(m, sname)))
		throw(SQL, "sql.restart", SQLSTATE(3F000) "Cannot find the schema %s", sname);
	if (!mvc_schema_privs(m, s))
		throw(SQL, "sql.restart", SQLSTATE(42000) "Access denied for %s to schema '%s'", stack_get_string(m, "current_user"), s->base.name);
	if (!(seq = find_sql_sequence(s, seqname)))
		throw(SQL, "sql.restart", SQLSTATE(HY050) "Failed to fetch sequence %s.%s", sname, seqname);
	if (is_lng_nil(start))
		throw(SQL, "sql.restart", SQLSTATE(HY050) "Cannot (re)start sequence %s.%s with NULL", sname, seqname);
	if (seq->minvalue && start < seq->minvalue)
		throw(SQL, "sql.restart", SQLSTATE(HY050) "Cannot set sequence %s.%s start to a value lesser than the minimum ("LLFMT" < "LLFMT")", sname, seqname, start, seq->minvalue);
	if (seq->maxvalue && start > seq->maxvalue)
		throw(SQL, "sql.restart", SQLSTATE(HY050) "Cannot set sequence %s.%s start to a value higher than the maximum ("LLFMT" > "LLFMT")", sname, seqname, start, seq->maxvalue);
	if (sql_trans_sequence_restart(m->session->tr, seq, start)) {
		*res = start;
		return MAL_SUCCEED;
	}
	throw(SQL, "sql.restart", SQLSTATE(HY050) "Cannot (re)start sequence %s.%s", sname, seqname);
}

str
mvc_bat_restart_seq(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = MAL_SUCCEED, sname = NULL, seqname = NULL;
	BAT *b = NULL, *c = NULL, *d = NULL, *r = NULL, *it;
	BUN p, q;
	sql_schema *s = NULL;
	sql_sequence *seq = NULL;
	seqbulk *sb = NULL;
	BATiter bi, ci;
	bat *res = getArgReference_bat(stk, pci, 0);
	bat schid = 0, seqid = 0, startid = 0;
	lng start = 0, *di = NULL;

	if (isaBatType(getArgType(mb, pci, 1)))
		schid = *getArgReference_bat(stk, pci, 1);
	else
		sname = *getArgReference_str(stk, pci, 1);
	if (isaBatType(getArgType(mb, pci, 2)))
		seqid = *getArgReference_bat(stk, pci, 2);
	else
		seqname = *getArgReference_str(stk, pci, 2);
	if (isaBatType(getArgType(mb, pci, 3)))
		startid = *getArgReference_bat(stk, pci, 3);
	else
		start = *getArgReference_lng(stk, pci, 3);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if (schid && !(b = BATdescriptor(schid))) {
		msg = createException(SQL, "sql.restart", SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	if (seqid && !(c = BATdescriptor(seqid))) {
		msg = createException(SQL, "sql.restart", SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	if (startid && !(d = BATdescriptor(startid))) {
		msg = createException(SQL, "sql.restart", SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	assert(b || c || d);
	it = b ? b : c ? c : d; /* Either b, c or d must be set */

	if (!(r = COLnew(it->hseqbase, TYPE_lng, BATcount(it), TRANSIENT))) {
		msg = createException(SQL, "sql.restart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	if (!BATcount(it))
		goto bailout; /* Success case */

	bi = bat_iterator(b);
	ci = bat_iterator(c);
	if (d)
		di = (lng *) Tloc(d, 0);

	BATloop(it, p, q) {
		str nsname, nseqname;
		lng nstart;

		if (b)
			nsname = BUNtvar(bi, p);
		else
			nsname = sname;
		if (c)
			nseqname = BUNtvar(ci, p);
		else
			nseqname = seqname;
		if (di)
			nstart = di[p];
		else
			nstart = start;

		if (!s || strcmp(s->base.name, nsname) != 0 || !seq || strcmp(seq->base.name, nseqname) != 0) {
			if (sb) {
				seqbulk_destroy(sb);
				sb = NULL;
			}
			seq = NULL;
			if ((!s || strcmp(s->base.name, nsname) != 0) && !(s = mvc_bind_schema(m, nsname))) {
				msg = createException(SQL, "sql.restart", SQLSTATE(3F000) "Cannot find the schema %s", nsname);
				goto bailout;
			}
			if (!mvc_schema_privs(m, s)) {
				msg = createException(SQL, "sql.restart", SQLSTATE(42000) "Access denied for %s to schema '%s'", stack_get_string(m, "current_user"), s->base.name);
				goto bailout;
			}
			if (!(seq = find_sql_sequence(s, nseqname)) || !(sb = seqbulk_create(seq, BATcount(it)))) {
				msg = createException(SQL, "sql.restart", SQLSTATE(HY050) "Cannot find the sequence %s.%s", nsname, nseqname);
				goto bailout;
			}
		}
		if (is_lng_nil(nstart)) {
			msg = createException(SQL, "sql.restart", SQLSTATE(HY050) "Cannot (re)start sequence %s.%s with NULL", sname, seqname);
			goto bailout;
		}
		if (seq->minvalue && nstart < seq->minvalue) {
			msg = createException(SQL, "sql.restart", SQLSTATE(HY050) "Cannot set sequence %s.%s start to a value lesser than the minimum ("LLFMT" < "LLFMT")", sname, seqname, start, seq->minvalue);
			goto bailout;
		}
		if (seq->maxvalue && nstart > seq->maxvalue) {
			msg = createException(SQL, "sql.restart", SQLSTATE(HY050) "Cannot set sequence %s.%s start to a value higher than the maximum ("LLFMT" > "LLFMT")", sname, seqname, start, seq->maxvalue);
			goto bailout;
		}
		if (!sql_trans_seqbulk_restart(m->session->tr, sb, nstart)) {
			msg = createException(SQL, "sql.restart", SQLSTATE(HY050) "Cannot restart sequence %s.%s", nsname, nseqname);
			goto bailout;
		}
		if (BUNappend(r, &nstart, false) != GDK_SUCCEED) {
			msg = createException(SQL, "sql.restart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
	}

bailout:
	if (sb)
		seqbulk_destroy(sb);
	if (b)
		BBPunfix(b->batCacheid);
	if (c)
		BBPunfix(c->batCacheid);
	if (d)
		BBPunfix(d->batCacheid);
	if (msg)
		BBPreclaim(r);
	else
		BBPkeepref(*res = r->batCacheid);
	return msg;
}

static BAT *
mvc_bind_dbat(mvc *m, const char *sname, const char *tname, int access)
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
	int coltype = getBatType(getArgType(mb, pci, 0));
	mvc *m = NULL;
	str msg;
	const char *sname = *getArgReference_str(stk, pci, 2 + upd);
	const char *tname = *getArgReference_str(stk, pci, 3 + upd);
	const char *cname = *getArgReference_str(stk, pci, 4 + upd);
	int access = *getArgReference_int(stk, pci, 5 + upd);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = mvc_bind(m, sname, tname, cname, access);
	if (b && b->ttype != coltype) {
		BBPunfix(b->batCacheid);
		throw(SQL,"sql.bind",SQLSTATE(42000) "Column type mismatch");
	}
	if (b) {
		if (pci->argc == (8 + upd) && getArgType(mb, pci, 6 + upd) == TYPE_int) {
			BUN cnt = BATcount(b), psz;
			/* partitioned access */
			int part_nr = *getArgReference_int(stk, pci, 6 + upd);
			int nr_parts = *getArgReference_int(stk, pci, 7 + upd);

			if (access == 0) {
				psz = cnt ? (cnt / nr_parts) : 0;
				bn = BATslice(b, part_nr * psz, (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz));
				if(bn == NULL) {
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.bind", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
				BAThseqbase(bn, part_nr * psz);
			} else {
				/* BAT b holds the UPD_ID bat */
				oid l, h;
				BAT *c = mvc_bind(m, sname, tname, cname, 0);
				if (c == NULL) {
					BBPunfix(b->batCacheid);
					throw(SQL,"sql.bind",SQLSTATE(HY005) "Cannot access the update column %s.%s.%s",
					      sname,tname,cname);
				}
				cnt = BATcount(c);
				psz = cnt ? (cnt / nr_parts) : 0;
				l = part_nr * psz;
				h = (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz);
				h--;
				bn = BATselect(b, NULL, &l, &h, true, true, false);
				BBPunfix(c->batCacheid);
				if(bn == NULL) {
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.bind", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			}
			BBPunfix(b->batCacheid);
			b = bn;
		} else if (upd) {
			BAT *uv = mvc_bind(m, sname, tname, cname, RD_UPD_VAL);
			bat *uvl = getArgReference_bat(stk, pci, 1);

			if (uv == NULL) {
				BBPunfix(b->batCacheid);
				throw(SQL,"sql.bind",SQLSTATE(HY005) "Cannot access the update column %s.%s.%s",
					sname,tname,cname);
			}
			BBPkeepref(*bid = b->batCacheid);
			BBPkeepref(*uvl = uv->batCacheid);
			return MAL_SUCCEED;
		}
		if (upd) {
			bat *uvl = getArgReference_bat(stk, pci, 1);

			if (BATcount(b)) {
				BAT *uv = mvc_bind(m, sname, tname, cname, RD_UPD_VAL);
				BAT *ui = mvc_bind(m, sname, tname, cname, RD_UPD_ID);
				BAT *id;
				BAT *vl;
				if (ui == NULL || uv == NULL) {
					bat_destroy(uv);
					bat_destroy(ui);
					BBPunfix(b->batCacheid);
					throw(SQL,"sql.bind",SQLSTATE(HY005) "Cannot access the insert column %s.%s.%s",
						sname, tname, cname);
				}
				id = BATproject(b, ui);
				vl = BATproject(b, uv);
				bat_destroy(ui);
				bat_destroy(uv);
				if (id == NULL || vl == NULL) {
					BBPunfix(b->batCacheid);
					bat_destroy(id);
					bat_destroy(vl);
					throw(SQL, "sql.bind", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
				if ( BATcount(id) != BATcount(vl)){
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.bind", SQLSTATE(0000) "Inconsistent BAT count");
				}
				BBPkeepref(*bid = id->batCacheid);
				BBPkeepref(*uvl = vl->batCacheid);
			} else {
				sql_schema *s = mvc_bind_schema(m, sname);
				sql_table *t = mvc_bind_table(m, s, tname);
				sql_column *c = mvc_bind_column(m, t, cname);

				*bid = e_bat(TYPE_oid);
				*uvl = e_bat(c->type.type->localtype);
				if(*bid == BID_NIL || *uvl == BID_NIL) {
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.bind", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			}
			BBPunfix(b->batCacheid);
		} else {
			BBPkeepref(*bid = b->batCacheid);
		}
		return MAL_SUCCEED;
	}
	if (!strNil(sname))
		throw(SQL, "sql.bind", SQLSTATE(42000) "unable to find %s.%s(%s)", sname, tname, cname);
	throw(SQL, "sql.bind", SQLSTATE(42000) "unable to find %s(%s)", tname, cname);
}

/* The output of this function are 7 columns:
 *  - The sqlid of the column
 *  - A flag indicating if the column's upper table is cleared or not.
 *  - Number of read-only values of the column (inherited from the previous transaction).
 *  - Number of inserted rows during the current transaction.
 *  - Number of updated rows during the current transaction.
 *  - Number of deletes of the column's table.
 *  - the number in the transaction chain (.i.e for each savepoint a new transaction is added in the chain)
 *  If the table is cleared, the values RDONLY, RD_INS and RD_UPD_ID and the number of deletes will be 0.
 */

static str
mvc_insert_delta_values(mvc *m, BAT *col1, BAT *col2, BAT *col3, BAT *col4, BAT *col5, BAT *col6, BAT *col7, sql_column *c, bit cleared, lng deletes)
{
	int level = 0;

	lng inserted = (lng) store_funcs.count_col(m->session->tr, c, 0);
	lng all = (lng) store_funcs.count_col(m->session->tr, c, 1);
	lng updates = (lng) store_funcs.count_col_upd(m->session->tr, c);
	lng readonly = all - inserted;

	assert(all >= inserted);

	if (BUNappend(col1, &c->base.id, false) != GDK_SUCCEED) {
		return createException(SQL,"sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (BUNappend(col2, &cleared, false) != GDK_SUCCEED) {
		return createException(SQL,"sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (BUNappend(col3, &readonly, false) != GDK_SUCCEED) {
		return createException(SQL,"sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (BUNappend(col4, &inserted, false) != GDK_SUCCEED) {
		return createException(SQL,"sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (BUNappend(col5, &updates, false) != GDK_SUCCEED) {
		return createException(SQL,"sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (BUNappend(col6, &deletes, false) != GDK_SUCCEED) {
		return createException(SQL,"sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	/* compute level using global transaction */
	if (gtrans) {
		sql_column *oc = tr_find_column(gtrans, c);

		if (oc) {
			for(sql_delta *d = oc->data; d; d = d->next)
				level++;
		}
	}
	if (BUNappend(col7, &level, false) != GDK_SUCCEED) {
		return createException(SQL,"sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	return MAL_SUCCEED;
}

str
mvc_delta_values(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	const char *sname = *getArgReference_str(stk, pci, 7),
			   *tname = (pci->argc > 8) ? *getArgReference_str(stk, pci, 8) : NULL,
			   *cname = (pci->argc > 9) ? *getArgReference_str(stk, pci, 9) : NULL;
	mvc *m;
	str msg = MAL_SUCCEED;
	BAT *col1 = NULL, *col2 = NULL, *col3 = NULL, *col4 = NULL, *col5 = NULL, *col6 = NULL, *col7 = NULL;
	bat *b1 = getArgReference_bat(stk, pci, 0),
		*b2 = getArgReference_bat(stk, pci, 1),
		*b3 = getArgReference_bat(stk, pci, 2),
		*b4 = getArgReference_bat(stk, pci, 3),
		*b5 = getArgReference_bat(stk, pci, 4),
		*b6 = getArgReference_bat(stk, pci, 5),
		*b7 = getArgReference_bat(stk, pci, 6);
	sql_schema *s = NULL;
	sql_table *t = NULL;
	sql_column *c = NULL;
	node *n;
	bit cleared;
	BUN nrows = 0;
	lng deletes;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		goto cleanup;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		goto cleanup;

	if (!(s = mvc_bind_schema(m, sname)))
		throw(SQL, "sql.delta", SQLSTATE(3F000) "No such schema '%s'", sname);

	if (tname) {
		if (!(t = mvc_bind_table(m, s, tname)))
			throw(SQL, "sql.delta", SQLSTATE(3F000) "No such table '%s' in schema '%s'", tname, s->base.name);
		if (isView(t))
			throw(SQL, "sql.delta", SQLSTATE(42000) "Views don't have delta values");
		if (isMergeTable(t))
			throw(SQL, "sql.delta", SQLSTATE(42000) "Merge tables don't have delta values");
		if (isStream(t))
			throw(SQL, "sql.delta", SQLSTATE(42000) "Stream tables don't have delta values");
		if (isRemote(t))
			throw(SQL, "sql.delta", SQLSTATE(42000) "Remote tables don't have delta values");
		if (isReplicaTable(t))
			throw(SQL, "sql.delta", SQLSTATE(42000) "Replica tables don't have delta values");
		if (cname) {
			if (!(c = mvc_bind_column(m, t, cname)))
				throw(SQL, "sql.delta", SQLSTATE(3F000) "No such column '%s' in table '%s'", cname, t->base.name);
			nrows = 1;
		} else {
			nrows = (BUN) t->columns.set->cnt;
		}
	} else if (s->tables.set) {
		for (n = s->tables.set->h; n ; n = n->next) {
			t = (sql_table *) n->data;
			if (!(isView(t) || isMergeTable(t) || isStream(t) || isRemote(t) || isReplicaTable(t)))
				nrows += t->columns.set->cnt;
		}
	}

	if ((col1 = COLnew(0, TYPE_int, nrows, TRANSIENT)) == NULL) {
		msg = createException(SQL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto cleanup;
	}
	if ((col2 = COLnew(0, TYPE_bit, nrows, TRANSIENT)) == NULL) {
		msg = createException(SQL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto cleanup;
	}
	if ((col3 = COLnew(0, TYPE_lng, nrows, TRANSIENT)) == NULL) {
		msg = createException(SQL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto cleanup;
	}
	if ((col4 = COLnew(0, TYPE_lng, nrows, TRANSIENT)) == NULL) {
		msg = createException(SQL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto cleanup;
	}
	if ((col5 = COLnew(0, TYPE_lng, nrows, TRANSIENT)) == NULL) {
		msg = createException(SQL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto cleanup;
	}
	if ((col6 = COLnew(0, TYPE_lng, nrows, TRANSIENT)) == NULL) {
		msg = createException(SQL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto cleanup;
	}
	if ((col7 = COLnew(0, TYPE_int, nrows, TRANSIENT)) == NULL) {
		msg = createException(SQL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto cleanup;
	}

	if (nrows) {
		if (tname) {
			cleared = (t->cleared != 0);
			deletes = (lng) store_funcs.count_del(m->session->tr, t);
			if (cname) {
				if ((msg=mvc_insert_delta_values(m, col1, col2, col3, col4, col5, col6, col7, c, cleared, deletes)) != NULL)
					goto cleanup;
			} else {
				for (n = t->columns.set->h; n ; n = n->next) {
					c = (sql_column*) n->data;
					if ((msg=mvc_insert_delta_values(m, col1, col2, col3, col4, col5, col6, col7, c, cleared, deletes)) != NULL)
						goto cleanup;
				}
			}
		} else if (s->tables.set) {
			for (n = s->tables.set->h; n ; n = n->next) {
				t = (sql_table *) n->data;
				if (!(isView(t) || isMergeTable(t) || isStream(t) || isRemote(t) || isReplicaTable(t))) {
					cleared = (t->cleared != 0);
					deletes = (lng) store_funcs.count_del(m->session->tr, t);

					for (node *nn = t->columns.set->h; nn ; nn = nn->next) {
						c = (sql_column*) nn->data;

						if ((msg=mvc_insert_delta_values(m, col1, col2, col3, col4, col5, col6, col7,
										 c, cleared, deletes)) != NULL)
							goto cleanup;
					}
				}
			}
		}
	}

cleanup:
	if (msg) {
		if (col1)
			BBPreclaim(col1);
		if (col2)
			BBPreclaim(col2);
		if (col3)
			BBPreclaim(col3);
		if (col4)
			BBPreclaim(col4);
		if (col5)
			BBPreclaim(col5);
		if (col6)
			BBPreclaim(col6);
		if (col7)
			BBPreclaim(col7);
	} else {
		BBPkeepref(*b1 = col1->batCacheid);
		BBPkeepref(*b2 = col2->batCacheid);
		BBPkeepref(*b3 = col3->batCacheid);
		BBPkeepref(*b4 = col4->batCacheid);
		BBPkeepref(*b5 = col5->batCacheid);
		BBPkeepref(*b6 = col6->batCacheid);
		BBPkeepref(*b7 = col7->batCacheid);
	}
	return msg;
}

/* str mvc_bind_idxbat_wrap(int *bid, str *sname, str *tname, str *iname, int *access); */
str
mvc_bind_idxbat_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int upd = (pci->argc == 7 || pci->argc == 9);
	BAT *b = NULL, *bn;
	bat *bid = getArgReference_bat(stk, pci, 0);
	int coltype = getBatType(getArgType(mb, pci, 0));
	mvc *m = NULL;
	str msg;
	const char *sname = *getArgReference_str(stk, pci, 2 + upd);
	const char *tname = *getArgReference_str(stk, pci, 3 + upd);
	const char *iname = *getArgReference_str(stk, pci, 4 + upd);
	int access = *getArgReference_int(stk, pci, 5 + upd);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = mvc_bind_idxbat(m, sname, tname, iname, access);
	if (b && b->ttype != coltype)
		throw(SQL,"sql.bind",SQLSTATE(42000) "Column type mismatch %s.%s.%s",sname,tname,iname);
	if (b) {
		if (pci->argc == (8 + upd) && getArgType(mb, pci, 6 + upd) == TYPE_int) {
			BUN cnt = BATcount(b), psz;
			/* partitioned access */
			int part_nr = *getArgReference_int(stk, pci, 6 + upd);
			int nr_parts = *getArgReference_int(stk, pci, 7 + upd);

			if (access == 0) {
				psz = cnt ? (cnt / nr_parts) : 0;
				bn = BATslice(b, part_nr * psz, (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz));
				if(bn == NULL)
					throw(SQL, "sql.bindidx", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				BAThseqbase(bn, part_nr * psz);
			} else {
				/* BAT b holds the UPD_ID bat */
				oid l, h;
				BAT *c = mvc_bind_idxbat(m, sname, tname, iname, 0);
				if ( c == NULL) {
					BBPunfix(b->batCacheid);
					throw(SQL,"sql.bindidx",SQLSTATE(42000) "Cannot access index column %s.%s.%s",sname,tname,iname);
				}
				cnt = BATcount(c);
				psz = cnt ? (cnt / nr_parts) : 0;
				l = part_nr * psz;
				h = (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz);
				h--;
				bn = BATselect(b, NULL, &l, &h, true, true, false);
				BBPunfix(c->batCacheid);
				if(bn == NULL) {
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.bindidx", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			}
			BBPunfix(b->batCacheid);
			b = bn;
		} else if (upd) {
			BAT *uv = mvc_bind_idxbat(m, sname, tname, iname, RD_UPD_VAL);
			bat *uvl = getArgReference_bat(stk, pci, 1);
			if ( uv == NULL)
				throw(SQL,"sql.bindidx",SQLSTATE(42000) "Cannot access index column %s.%s.%s",sname,tname,iname);
			BBPkeepref(*bid = b->batCacheid);
			BBPkeepref(*uvl = uv->batCacheid);
			return MAL_SUCCEED;
		}
		if (upd) {
			bat *uvl = getArgReference_bat(stk, pci, 1);

			if (BATcount(b)) {
				BAT *uv = mvc_bind_idxbat(m, sname, tname, iname, RD_UPD_VAL);
				BAT *ui = mvc_bind_idxbat(m, sname, tname, iname, RD_UPD_ID);
				BAT *id, *vl;
				if ( ui == NULL || uv == NULL) {
					bat_destroy(uv);
					bat_destroy(ui);
					throw(SQL,"sql.bindidx",SQLSTATE(42000) "Cannot access index column %s.%s.%s",sname,tname,iname);
				}
				id = BATproject(b, ui);
				vl = BATproject(b, uv);
				bat_destroy(ui);
				bat_destroy(uv);
				if (id == NULL || vl == NULL) {
					bat_destroy(id);
					bat_destroy(vl);
					throw(SQL, "sql.idxbind", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
				assert(BATcount(id) == BATcount(vl));
				BBPkeepref(*bid = id->batCacheid);
				BBPkeepref(*uvl = vl->batCacheid);
			} else {
				sql_schema *s = mvc_bind_schema(m, sname);
				sql_idx *i = mvc_bind_idx(m, s, iname);

				*bid = e_bat(TYPE_oid);
				*uvl = e_bat((i->type==join_idx)?TYPE_oid:TYPE_lng);
				if(*bid == BID_NIL || *uvl == BID_NIL)
					throw(SQL, "sql.idxbind", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			BBPunfix(b->batCacheid);
		} else {
			BBPkeepref(*bid = b->batCacheid);
		}
		return MAL_SUCCEED;
	}
	if (sname)
		throw(SQL, "sql.idxbind", SQLSTATE(HY005) "Cannot access column descriptor %s for %s.%s", iname, sname, tname);
	throw(SQL, "sql.idxbind", SQLSTATE(HY005) "Cannot access column descriptor %s for %s", iname, tname);
}

str
mvc_append_column(sql_trans *t, sql_column *c, BAT *ins)
{
	int res = store_funcs.append_col(t, c, ins, TYPE_bat);
	if (res != LOG_OK)
		throw(SQL, "sql.append", SQLSTATE(42000) "Cannot append values");
	return MAL_SUCCEED;
}

/*mvc_grow_wrap(int *bid, str *sname, str *tname, str *cname, ptr d) */
str
mvc_grow_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res = getArgReference_int(stk, pci, 0);
	bat Tid = *getArgReference_bat(stk, pci, 1);
	ptr Ins = getArgReference(stk, pci, 2);
	int tpe = getArgType(mb, pci, 2);
	BAT *tid = 0, *ins = 0;
	size_t cnt = 1;
	oid v = 0;

	(void)cntxt;
	*res = 0;
	if ((tid = BATdescriptor(Tid)) == NULL)
		throw(SQL, "sql.grow", SQLSTATE(HY005) "Cannot access descriptor");
	if (tpe > GDKatomcnt)
		tpe = TYPE_bat;
	if (tpe == TYPE_bat && (ins = BATdescriptor(*(bat *) Ins)) == NULL) {
		BBPunfix(Tid);
		throw(SQL, "sql.grow", SQLSTATE(HY005) "Cannot access descriptor");
	}
	if (ins) {
		cnt = BATcount(ins);
		BBPunfix(ins->batCacheid);
	}
	if (BATcount(tid)) {
		(void)BATmax(tid, &v);
		v++;
	}
	for(;cnt>0; cnt--, v++) {
		if (BUNappend(tid, &v, false) != GDK_SUCCEED) {
			BBPunfix(Tid);
			throw(SQL, "sql.grow", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	BBPunfix(Tid);
	return MAL_SUCCEED;
}

/*mvc_append_wrap(int *bid, str *sname, str *tname, str *cname, ptr d) */
str
mvc_append_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res = getArgReference_int(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	const char *sname = *getArgReference_str(stk, pci, 2);
	const char *tname = *getArgReference_str(stk, pci, 3);
	const char *cname = *getArgReference_str(stk, pci, 4);
	ptr ins = getArgReference(stk, pci, 5);
	int tpe = getArgType(mb, pci, 5), err = 0;
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
	if (tpe == TYPE_bat && (ins = BATdescriptor(*(bat *) ins)) == NULL)
		throw(SQL, "sql.append", SQLSTATE(HY005) "Cannot access column descriptor %s.%s.%s",
			sname,tname,cname);
	if (ATOMextern(tpe))
		ins = *(ptr *) ins;
	if ( tpe == TYPE_bat)
		b =  (BAT*) ins;
	s = mvc_bind_schema(m, sname);
	if (s == NULL) {
		if (b)
			BBPunfix(b->batCacheid);
		throw(SQL, "sql.append", SQLSTATE(3F000) "Schema missing %s",sname);
	}
	t = mvc_bind_table(m, s, tname);
	if (t == NULL) {
		if (b)
			BBPunfix(b->batCacheid);
		throw(SQL, "sql.append", SQLSTATE(42S02) "Table missing %s",tname);
	}
	if( b && BATcount(b) > 4096 && !b->batTransient)
		BATmsync(b);
	if (cname[0] != '%' && (c = mvc_bind_column(m, t, cname)) != NULL) {
		if (store_funcs.append_col(m->session->tr, c, ins, tpe) != LOG_OK)
			err = 1;
	} else if (cname[0] == '%') {
		sql_idx *i = mvc_bind_idx(m, s, cname + 1);
		if (i && store_funcs.append_idx(m->session->tr, i, ins, tpe) != LOG_OK)
			err = 1;
	}
	if (err)
		throw(SQL, "sql.append", SQLSTATE(42S02) "append failed");
	if (b) {
		BBPunfix(b->batCacheid);
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
	const char *sname = *getArgReference_str(stk, pci, 2);
	const char *tname = *getArgReference_str(stk, pci, 3);
	const char *cname = *getArgReference_str(stk, pci, 4);
	bat Tids = *getArgReference_bat(stk, pci, 5);
	bat Upd = *getArgReference_bat(stk, pci, 6);
	BAT *tids, *upd;
	int tpe = getArgType(mb, pci, 6), err = 0;
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
		throw(SQL, "sql.update", SQLSTATE(HY005) "Cannot access column descriptor %s.%s.%s",
		sname,tname,cname);
	if ((tids = BATdescriptor(Tids)) == NULL)
		throw(SQL, "sql.update", SQLSTATE(HY005) "Cannot access column descriptor %s.%s.%s",
			sname,tname,cname);
	if ((upd = BATdescriptor(Upd)) == NULL) {
		BBPunfix(tids->batCacheid);
		throw(SQL, "sql.update", SQLSTATE(HY005) "Cannot access column descriptor %s.%s.%s",
			sname,tname,cname);
	}
	s = mvc_bind_schema(m, sname);
	if (s == NULL) {
		BBPunfix(tids->batCacheid);
		BBPunfix(upd->batCacheid);
		throw(SQL, "sql.update", SQLSTATE(3F000) "Schema missing %s",sname);
	}
	t = mvc_bind_table(m, s, tname);
	if (t == NULL) {
		BBPunfix(tids->batCacheid);
		BBPunfix(upd->batCacheid);
		throw(SQL, "sql.update", SQLSTATE(42S02) "Table missing %s.%s",sname,tname);
	}
	if( upd && BATcount(upd) > 4096 && !upd->batTransient)
		BATmsync(upd);
	if( tids && BATcount(tids) > 4096 && !tids->batTransient)
		BATmsync(tids);
	if (cname[0] != '%' && (c = mvc_bind_column(m, t, cname)) != NULL) {
		if (store_funcs.update_col(m->session->tr, c, tids, upd, TYPE_bat) != LOG_OK)
			err = 1;
	} else if (cname[0] == '%') {
		sql_idx *i = mvc_bind_idx(m, s, cname + 1);
		if (i && store_funcs.update_idx(m->session->tr, i, tids, upd, TYPE_bat) != LOG_OK)
			err = 1;
	}
	BBPunfix(tids->batCacheid);
	BBPunfix(upd->batCacheid);
	if (err)
		throw(SQL, "sql.update", SQLSTATE(42S02) "update failed");
	return MAL_SUCCEED;
}

/* str mvc_clear_table_wrap(lng *res, str *sname, str *tname); */
str
mvc_clear_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	sql_schema *s;
	sql_table *t;
	mvc *m = NULL;
	str msg;
	lng *res = getArgReference_lng(stk, pci, 0);
	const char *sname = *getArgReference_str(stk, pci, 1);
	const char *tname = *getArgReference_str(stk, pci, 2);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		throw(SQL, "sql.clear_table", SQLSTATE(3F000) "Schema missing %s", sname);
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "sql.clear_table", SQLSTATE(42S02) "Table missing %s.%s", sname,tname);
	*res = mvc_clear_table(m, t);
	if (*res == BUN_NONE)
		throw(SQL, "sql.clear_table", SQLSTATE(42S02) "clear failed");
	return MAL_SUCCEED;
}

/*mvc_delete_wrap(int *d, str *sname, str *tname, ptr d) */
str
mvc_delete_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res = getArgReference_int(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	const char *sname = *getArgReference_str(stk, pci, 2);
	const char *tname = *getArgReference_str(stk, pci, 3);
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
	if (tpe == TYPE_bat && (b = BATdescriptor(*(bat *) ins)) == NULL)
		throw(SQL, "sql.delete", SQLSTATE(HY005) "Cannot access column descriptor");
	if (tpe != TYPE_bat || (b->ttype != TYPE_oid && b->ttype != TYPE_void)) {
		if (b)
			BBPunfix(b->batCacheid);
		throw(SQL, "sql.delete", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	s = mvc_bind_schema(m, sname);
	if (s == NULL) {
		if (b)
			BBPunfix(b->batCacheid);
		throw(SQL, "sql.delete", SQLSTATE(3F000) "Schema missing %s",sname);
	}
	t = mvc_bind_table(m, s, tname);
	if (t == NULL) {
		if (b)
			BBPunfix(b->batCacheid);
		throw(SQL, "sql.delete", SQLSTATE(42S02) "Table missing %s.%s",sname,tname);
	}
	if( b && BATcount(b) > 4096 && !b->batTransient)
		BATmsync(b);
	if (store_funcs.delete_tab(m->session->tr, t, b, tpe) != LOG_OK)
		throw(SQL, "sql.delete", SQLSTATE(3F000) "delete failed");
	if (b)
		BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static BAT *
setwritable(BAT *b)
{
	BAT *bn = b;

	if (BATsetaccess(b, BAT_WRITE) != GDK_SUCCEED) {
		if (b->batSharecnt) {
			bn = COLcopy(b, b->ttype, true, TRANSIENT);
			if (bn != NULL)
				if (BATsetaccess(bn, BAT_WRITE) != GDK_SUCCEED) {
					BBPreclaim(bn);
					bn = NULL;
				}
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

	if ((u_id = BBPquickdesc(*uid, false)) == NULL)
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (ins && (i = BBPquickdesc(*ins, false)) == NULL)
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	/* no updates, no inserts */
	if (BATcount(u_id) == 0 && (!i || BATcount(i) == 0)) {
		BBPretain(*result = *col);
		return MAL_SUCCEED;
	}

	if ((c = BBPquickdesc(*col, false)) == NULL)
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	/* bat may change */
	if (i && BATcount(c) == 0 && BATcount(u_id) == 0) {
		BBPretain(*result = *ins);
		return MAL_SUCCEED;
	}

	c = BATdescriptor(*col);
	if (c == NULL)
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if ((res = COLcopy(c, c->ttype, true, TRANSIENT)) == NULL) {
		BBPunfix(c->batCacheid);
		throw(MAL, "sql.delta", SQLSTATE(45002) "Cannot create copy of delta structure");
	}
	BBPunfix(c->batCacheid);

	if ((u_val = BATdescriptor(*uval)) == NULL) {
		BBPunfix(res->batCacheid);
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((u_id = BATdescriptor(*uid)) == NULL) {
		BBPunfix(u_val->batCacheid);
		BBPunfix(res->batCacheid);
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	assert(BATcount(u_id) == BATcount(u_val));
	if (BATcount(u_id) &&
	    BATreplace(res, u_id, u_val, true) != GDK_SUCCEED) {
		BBPunfix(u_id->batCacheid);
		BBPunfix(u_val->batCacheid);
		BBPunfix(res->batCacheid);
		throw(MAL, "sql.delta", SQLSTATE(45002) "Cannot access delta structure");
	}
	BBPunfix(u_id->batCacheid);
	BBPunfix(u_val->batCacheid);

	if (i && BATcount(i)) {
		if ((i = BATdescriptor(*ins)) == NULL) {
			BBPunfix(res->batCacheid);
			throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		if (BATappend(res, i, NULL, true) != GDK_SUCCEED) {
			BBPunfix(res->batCacheid);
			BBPunfix(i->batCacheid);
			throw(MAL, "sql.delta", SQLSTATE(45002) "Cannot access delta structure");
		}
		BBPunfix(i->batCacheid);
	}

	BBPkeepref(*result = res->batCacheid);
	return MAL_SUCCEED;
}

str
DELTAsub(bat *result, const bat *col, const bat *cid, const bat *uid, const bat *uval, const bat *ins)
{
	BAT *c, *cminu = NULL, *u_id, *u_val, *u, *i = NULL, *res;
	gdk_return ret;

	if ((u_id = BBPquickdesc(*uid, false)) == NULL)
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (ins && (i = BBPquickdesc(*ins, false)) == NULL)
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	/* no updates, no inserts */
	if (BATcount(u_id) == 0 && (!i || BATcount(i) == 0)) {
		BBPretain(*result = *col);
		return MAL_SUCCEED;
	}

	if ((c = BBPquickdesc(*col, false)) == NULL)
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	/* bat may change */
	if (i && BATcount(c) == 0 && BATcount(u_id) == 0) {
		BBPretain(*result = *ins);
		return MAL_SUCCEED;
	}

	c = BATdescriptor(*col);
	if (c == NULL)
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	res = c;
	if (BATcount(u_id)) {
		u_id = BATdescriptor(*uid);
		if (!u_id) {
			BBPunfix(c->batCacheid);
			throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		cminu = BATdiff(c, u_id, NULL, NULL, false, false, BUN_NONE);
		if (!cminu) {
			BBPunfix(c->batCacheid);
			BBPunfix(u_id->batCacheid);
			throw(MAL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL " intermediate");
		}
		res = BATproject(cminu, c);
		BBPunfix(c->batCacheid);
		BBPunfix(cminu->batCacheid);
		cminu = NULL;
		if (!res) {
			BBPunfix(u_id->batCacheid);
			throw(MAL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL " intermediate" );
		}
		c = res;

		if ((u_val = BATdescriptor(*uval)) == NULL) {
			BBPunfix(c->batCacheid);
			BBPunfix(u_id->batCacheid);
			throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		if (BATcount(u_val)) {
			u = BATproject(u_val, u_id);
			BBPunfix(u_val->batCacheid);
			BBPunfix(u_id->batCacheid);
			if (!u) {
				BBPunfix(c->batCacheid);
				throw(MAL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}

			/* check selected updated values against candidates */
			BAT *c_ids = BATdescriptor(*cid);

			if (!c_ids) {
				BBPunfix(c->batCacheid);
				BBPunfix(u->batCacheid);
				throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			}
			cminu = BATintersect(u, c_ids, NULL, NULL, false, false, BUN_NONE);
			BBPunfix(c_ids->batCacheid);
			if (cminu == NULL) {
				BBPunfix(c->batCacheid);
				BBPunfix(u->batCacheid);
				throw(MAL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			ret = BATappend(res, u, cminu, true);
			BBPunfix(u->batCacheid);
			if (cminu)
				BBPunfix(cminu->batCacheid);
			cminu = NULL;
			if (ret != GDK_SUCCEED) {
				BBPunfix(res->batCacheid);
				throw(MAL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}

			ret = BATsort(&u, NULL, NULL, res, NULL, NULL, false, false, false);
			BBPunfix(res->batCacheid);
			if (ret != GDK_SUCCEED) {
				throw(MAL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			res = u;
		} else {
			BBPunfix(u_val->batCacheid);
			BBPunfix(u_id->batCacheid);
		}
	}

	if (i) {
		i = BATdescriptor(*ins);
		if (!i) {
			BBPunfix(res->batCacheid);
			throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		if (BATcount(u_id)) {
			u_id = BATdescriptor(*uid);
			if (!u_id) {
				BBPunfix(res->batCacheid);
				BBPunfix(i->batCacheid);
				throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			}
			cminu = BATdiff(i, u_id, NULL, NULL, false, false, BUN_NONE);
			BBPunfix(u_id->batCacheid);
			if (!cminu) {
				BBPunfix(res->batCacheid);
				BBPunfix(i->batCacheid);
				throw(MAL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
		if (isVIEW(res)) {
			BAT *n = COLcopy(res, res->ttype, true, TRANSIENT);
			BBPunfix(res->batCacheid);
			res = n;
			if (res == NULL) {
				BBPunfix(i->batCacheid);
				if (cminu)
					BBPunfix(cminu->batCacheid);
				throw(MAL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
		ret = BATappend(res, i, cminu, true);
		BBPunfix(i->batCacheid);
		if (cminu)
			BBPunfix(cminu->batCacheid);
		if (ret != GDK_SUCCEED) {
			BBPunfix(res->batCacheid);
			throw(MAL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}

		ret = BATsort(&u, NULL, NULL, res, NULL, NULL, false, false, false);
		BBPunfix(res->batCacheid);
		if (ret != GDK_SUCCEED)
			throw(MAL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		res = u;
	}
	BATkey(res, true);
	BBPkeepref(*result = res->batCacheid);
	return MAL_SUCCEED;
}

str
DELTAproject(bat *result, const bat *sub, const bat *col, const bat *uid, const bat *uval, const bat *ins)
{
	BAT *s, *c, *u_id, *u_val, *i = NULL, *res, *tres;

	if ((s = BATdescriptor(*sub)) == NULL)
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (ins && (i = BATdescriptor(*ins)) == NULL) {
		BBPunfix(s->batCacheid);
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	if (i && BATcount(s) == 0) {
		res = BATproject(s, i);
		BBPunfix(s->batCacheid);
		BBPunfix(i->batCacheid);
		if (res == NULL)
			throw(MAL, "sql.projectdelta", SQLSTATE(HY013) MAL_MALLOC_FAIL);

		BBPkeepref(*result = res->batCacheid);
		return MAL_SUCCEED;
	}

	if ((c = BATdescriptor(*col)) == NULL) {
		BBPunfix(s->batCacheid);
		if (i)
			BBPunfix(i->batCacheid);
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	/* projection(sub,col).union(projection(sub,i)) */
	res = c;
	if (i && BATcount(i)) {
		if (BATcount(c) == 0) {
			res = i;
			i = c;
			tres = BATproject(s, res);
		} else {
			tres = BATproject2(s, c, i);
		}
	} else {
		tres = BATproject(s, res);
	}
	if (i)
		BBPunfix(i->batCacheid);
	BBPunfix(res->batCacheid);

	if (tres == NULL) {
		BBPunfix(s->batCacheid);
		throw(MAL, "sql.projectdelta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	res = tres;

	if ((u_id = BATdescriptor(*uid)) == NULL) {
		BBPunfix(res->batCacheid);
		BBPunfix(s->batCacheid);
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
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
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	if (BATcount(u_val)) {
		BAT *os, *ou;
		/* figure out the positions in res that we have to
		 * replace with values from u_val */
		if (BATsemijoin(&ou, &os, u_id, s, NULL, NULL, false, false, BUN_NONE) != GDK_SUCCEED) {
			BBPunfix(s->batCacheid);
			BBPunfix(res->batCacheid);
			BBPunfix(u_id->batCacheid);
			BBPunfix(u_val->batCacheid);
			throw(MAL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		/* BATcount(ou) == BATcount(os) */
		if (BATcount(ou) != 0) {
			/* ou contains the position in u_id/u_val that
			 * contain the new values */
			BAT *nu_val = BATproject(ou, u_val);
			BBPunfix(ou->batCacheid);
			/* os contains the corresponding positions in
			 * res that need to be replaced with those new
			 * values */
			if ((res = setwritable(res)) == NULL ||
			    BATreplace(res, os, nu_val, false) != GDK_SUCCEED) {
				if (res)
					BBPunfix(res->batCacheid);
				BBPunfix(os->batCacheid);
				BBPunfix(s->batCacheid);
				BBPunfix(u_id->batCacheid);
				BBPunfix(u_val->batCacheid);
				BBPunfix(nu_val->batCacheid);
				throw(MAL, "sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			BBPunfix(nu_val->batCacheid);
		} else {
			/* nothing to replace */
			BBPunfix(ou->batCacheid);
		}
		BBPunfix(os->batCacheid);
	}
	BBPunfix(s->batCacheid);
	BBPunfix(u_id->batCacheid);
	BBPunfix(u_val->batCacheid);

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
	res = COLnew(0, TYPE_oid, cnt, TRANSIENT);
	if (!c || !l || !r || !res) {
		if (c)
			BBPunfix(c->batCacheid);
		if (l)
			BBPunfix(l->batCacheid);
		if (r)
			BBPunfix(r->batCacheid);
		if (res)
			BBPunfix(res->batCacheid);
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
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
	res->tsorted = false;
	res->trevsorted = false;
	res->tnil = false;
	res->tnonil = false;
	res->tkey = false;
	BBPunfix(c->batCacheid);
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	BBPkeepref(*Res = res->batCacheid);
	return MAL_SUCCEED;
}

/* str SQLtid(bat *result, mvc *m, str *sname, str *tname) */
str
SQLtid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0);
	mvc *m = NULL;
	str msg = MAL_SUCCEED;
	sql_trans *tr;
	const char *sname = *getArgReference_str(stk, pci, 2);
	const char *tname = *getArgReference_str(stk, pci, 3);

	sql_schema *s;
	sql_table *t;
	sql_column *c;
	BAT *tids;
	size_t nr, inr = 0, dcnt;
	oid sb = 0;

	*res = bat_nil;
	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	tr = m->session->tr;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		throw(SQL, "sql.tid", SQLSTATE(3F000) "Schema missing %s",sname);
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "sql.tid", SQLSTATE(42S02) "Table missing %s.%s",sname,tname);
	c = t->columns.set->h->data;

	nr = store_funcs.count_col(tr, c, 1);

	if (isTable(t) && t->access == TABLE_WRITABLE && (!isNew(t) /* alter */ ) &&
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
	tids = BATdense(sb, sb, (BUN) nr);
	if (tids == NULL)
		throw(SQL, "sql.tid", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* V1 of the deleted list
	 * 1) in case of deletes, bind_del, order it, put into a heap(of the tids bat)
	 * 2) in mal recognize this type of bat.
	 * 3) if function can handle it pass along, else fall back to first diff.
	 * */
	if ((dcnt = store_funcs.count_del(tr, t)) > 0) {
		BAT *d = store_funcs.bind_del(tr, t, RD_INS);

		if (d == NULL) {
			BBPunfix(tids->batCacheid);
			throw(SQL,"sql.tid", SQLSTATE(45002) "Can not bind delete column");
		}

#if 1
		BAT *o;
		gdk_return ret = BATsort(&o, NULL, NULL, d, NULL, NULL, false, false, false);
		BBPunfix(d->batCacheid);
		if (ret != GDK_SUCCEED)
			throw(MAL, "sql.tids", SQLSTATE(HY013) MAL_MALLOC_FAIL);

		/* TODO handle dense o, ie full range out of the dense tids, could be at beginning or end (reduce range of tids)
		 * else materialize */
		/* copy into heap */
		ret = BATnegcands(tids, o);
		BBPunfix(o->batCacheid);
		if (ret != GDK_SUCCEED)
			throw(MAL, "sql.tids", SQLSTATE(45003) "TIDdeletes failed");
#else
		BAT *diff;
		diff = BATdiff(tids, d, NULL, NULL, false, false, BUN_NONE);
		assert(pci->argc == 6 || BATcount(diff) == (nr-dcnt));
		//if( !(pci->argc == 6 || BATcount(diff) == (nr-dcnt)) )
			//msg = createException(SQL, "sql.tid", SQLSTATE(00000) "Invalid sqltid state argc=%d diff=" BUNFMT ", nr=%zd, dcnt=%zd", pci->argc, BATcount(diff), nr, dcnt);
		BBPunfix(d->batCacheid);
		BBPunfix(tids->batCacheid);
		if (diff == NULL)
			throw(SQL,"sql.tid", SQLSTATE(45002) "Cannot subtract delete column");
		BAThseqbase(diff, sb);
		tids = diff;
#endif
	}
	BBPkeepref(*res = tids->batCacheid);
	return msg;
}

/* unsafe pattern resultSet(tbl:bat[:str], attr:bat[:str], tpe:bat[:str], len:bat[:int],scale:bat[:int], cols:bat[:any]...) :int */
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
		throw(MAL,"sql.resultset", SQLSTATE(HY005) "Cannot access column descriptor");
	if (isVIEW(b)) {
		BAT *bn = COLcopy(b, b->ttype, true, TRANSIENT);
		BBPunfix(b->batCacheid);
		if (bn == NULL)
			throw(MAL, "sql.resultset", GDK_EXCEPTION);
		b = bn;
		assert(!isVIEW(b));
	}
	res = *res_id = mvc_result_table(m, mb->tag, pci->argc - (pci->retc + 5), Q_TABLE, b);
	if (res < 0)
		msg = createException(SQL, "sql.resultSet", SQLSTATE(45000) "Result table construction failed");
	BBPunfix(b->batCacheid);

	tbl = BATdescriptor(tblId);
	atr = BATdescriptor(atrId);
	tpe = BATdescriptor(tpeId);
	len = BATdescriptor(lenId);
	scale = BATdescriptor(scaleId);
	if( msg || tbl == NULL || atr == NULL || tpe == NULL || len == NULL || scale == NULL)
		goto wrapup_result_set;
	/* mimick the old rsColumn approach; */
	itertbl = bat_iterator(tbl);
	iteratr = bat_iterator(atr);
	itertpe = bat_iterator(tpe);
	digits = (int*) Tloc(len,0);
	scaledigits = (int*) Tloc(scale,0);

	for( i = 6; msg == MAL_SUCCEED && i< pci->argc; i++, o++){
		bid = *getArgReference_bat(stk,pci,i);
		tblname = BUNtvar(itertbl,o);
		colname = BUNtvar(iteratr,o);
		tpename = BUNtvar(itertpe,o);
		b = BATdescriptor(bid);
		if ( b == NULL) {
			msg= createException(MAL,"sql.resultset",SQLSTATE(HY005) "Cannot access column descriptor ");
			break;
		}
		if (isVIEW(b)) {
			BAT *bn = COLcopy(b, b->ttype, true, TRANSIENT);
			BBPunfix(b->batCacheid);
			if (bn == NULL)
				throw(MAL, "sql.resultset", GDK_EXCEPTION);
			b = bn;
			assert(!isVIEW(b));
		}
		if (mvc_result_column(m, tblname, colname, tpename, *digits++, *scaledigits++, b))
			msg = createException(SQL, "sql.resultset", SQLSTATE(42000) "Cannot access column descriptor %s.%s",tblname,colname);
		BBPunfix(b->batCacheid);
	}
	/* now send it to the channel cntxt->fdout */
	if (mvc_export_result(cntxt->sqlcontext, cntxt->fdout, res, true, mb->starttime, mb->optimize))
		msg = createException(SQL, "sql.resultset", SQLSTATE(45000) "Result set construction failed");
	mb->starttime = 0;
	mb->optimize = 0;
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
	const char *filename = *getArgReference_str(stk,pci,1);
	const char *format = *getArgReference_str(stk,pci,2);
	const char *tsep = *getArgReference_str(stk, pci, 3);
	const char *rsep = *getArgReference_str(stk, pci, 4);
	const char *ssep = *getArgReference_str(stk, pci, 5);
	const char *ns = *getArgReference_str(stk, pci, 6);
	int onclient = *getArgReference_int(stk, pci, 7);

	bat tblId= *getArgReference_bat(stk, pci,8);
	bat atrId= *getArgReference_bat(stk, pci,9);
	bat tpeId= *getArgReference_bat(stk, pci,10);
	bat lenId= *getArgReference_bat(stk, pci,11);
	bat scaleId= *getArgReference_bat(stk, pci,12);
	stream *s;
	bat bid;
	int i,res;
	str tblname, colname, tpename, msg= MAL_SUCCEED;
	int *digits, *scaledigits;
	oid o = 0;
	BATiter itertbl,iteratr,itertpe;
	mvc *m = NULL;
	BAT *order = NULL, *b = NULL, *tbl = NULL, *atr = NULL, *tpe = NULL,*len = NULL,*scale = NULL;
	res_table *t = NULL;
	bool tostdout;
	char buf[80];
	ssize_t sz;

	(void) format;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;

	if (onclient && !cntxt->filetrans) {
		throw(MAL, "sql.resultSet", "cannot transfer files to client");
	}

	bid = *getArgReference_bat(stk,pci,13);
	order = BATdescriptor(bid);
	if ( order == NULL)
		throw(MAL,"sql.resultset", SQLSTATE(HY005) "Cannot access column descriptor");
	res = *res_id = mvc_result_table(m, mb->tag, pci->argc - (pci->retc + 12), Q_TABLE, order);
	t = m->results;
	if (res < 0){
		msg = createException(SQL, "sql.resultSet", SQLSTATE(45000) "Result set construction failed");
		goto wrapup_result_set1;
	}

	t->tsep = tsep;
	t->rsep = rsep;
	t->ssep = ssep;
	t->ns = ns;

	tbl = BATdescriptor(tblId);
	atr = BATdescriptor(atrId);
	tpe = BATdescriptor(tpeId);
	len = BATdescriptor(lenId);
	scale = BATdescriptor(scaleId);
	if( tbl == NULL || atr == NULL || tpe == NULL || len == NULL || scale == NULL)
		goto wrapup_result_set1;
	/* mimick the old rsColumn approach; */
	itertbl = bat_iterator(tbl);
	iteratr = bat_iterator(atr);
	itertpe = bat_iterator(tpe);
	digits = (int*) Tloc(len,0);
	scaledigits = (int*) Tloc(scale,0);

	for( i = 13; msg == MAL_SUCCEED && i< pci->argc; i++, o++){
		bid = *getArgReference_bat(stk,pci,i);
		tblname = BUNtvar(itertbl,o);
		colname = BUNtvar(iteratr,o);
		tpename = BUNtvar(itertpe,o);
		b = BATdescriptor(bid);
		if ( b == NULL)
			msg= createException(MAL,"sql.resultset",SQLSTATE(HY005) "Cannot access column descriptor");
		else if (mvc_result_column(m, tblname, colname, tpename, *digits++, *scaledigits++, b))
			msg = createException(SQL, "sql.resultset", SQLSTATE(42000) "Cannot access column descriptor %s.%s",tblname,colname);
		if( b)
			BBPunfix(bid);
	}
	if ( msg )
		goto wrapup_result_set1;

	/* now select the file channel */
	if ((tostdout = strcmp(filename,"stdout") == 0)) {
		s = cntxt->fdout;
	} else if (!onclient) {
		if ((s = open_wastream(filename)) == NULL || mnstr_errnr(s)) {
			msg=  createException(IO, "streams.open", SQLSTATE(42000) "could not open file '%s': %s",
					      filename?filename:"stdout", GDKstrerror(errno, (char[128]){0}, 128));
			close_stream(s);
			goto wrapup_result_set1;
		}
	} else {
		while (!m->scanner.rs->eof)
			bstream_next(m->scanner.rs);
		s = m->scanner.ws;
		mnstr_write(s, PROMPT3, sizeof(PROMPT3) - 1, 1);
		mnstr_printf(s, "w %s\n", filename);
		mnstr_flush(s);
		if ((sz = mnstr_readline(m->scanner.rs->s, buf, sizeof(buf))) > 1) {
			/* non-empty line indicates failure on client */
			msg = createException(IO, "streams.open", "%s", buf);
			/* deal with ridiculously long response from client */
			while (buf[sz - 1] != '\n' &&
			       (sz = mnstr_readline(m->scanner.rs->s, buf, sizeof(buf))) > 0)
				;
			goto wrapup_result_set1;
		}
	}
	if (mvc_export_result(cntxt->sqlcontext, s, res, tostdout, mb->starttime, mb->optimize))
		msg = createException(SQL, "sql.resultset", SQLSTATE(45000) "Result set construction failed");
	mb->starttime = 0;
	mb->optimize = 0;
	if (onclient) {
		mnstr_flush(s);
		if ((sz = mnstr_readline(m->scanner.rs->s, buf, sizeof(buf))) > 1) {
			msg = createException(IO, "streams.open", "%s", buf);
		}
		while (sz > 0)
			sz = mnstr_readline(m->scanner.rs->s, buf, sizeof(buf));
	} else if (!tostdout) {
		close_stream(s);
	}
  wrapup_result_set1:
	BBPunfix(order->batCacheid);
	if( tbl) BBPunfix(tblId);
	if( atr) BBPunfix(atrId);
	if( tpe) BBPunfix(tpeId);
	if( len) BBPunfix(lenId);
	if( scale) BBPunfix(scaleId);
	return msg;
}

/* unsafe pattern resultSet(tbl:bat[:str], attr:bat[:str], tpe:bat[:str], len:bat[:int],scale:bat[:int], cols:any...) :int */
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
	ptr v;
	int mtype;
	BAT  *tbl, *atr, *tpe,*len,*scale;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	res = *res_id = mvc_result_table(m, mb->tag, pci->argc - (pci->retc + 5), Q_TABLE, NULL);
	if (res < 0)
		throw(SQL, "sql.resultset", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	tbl = BATdescriptor(tblId);
	atr = BATdescriptor(atrId);
	tpe = BATdescriptor(tpeId);
	len = BATdescriptor(lenId);
	scale = BATdescriptor(scaleId);
	if( tbl == NULL || atr == NULL || tpe == NULL || len == NULL || scale == NULL)
		goto wrapup_result_set;
	/* mimick the old rsColumn approach; */
	itertbl = bat_iterator(tbl);
	iteratr = bat_iterator(atr);
	itertpe = bat_iterator(tpe);
	digits = (int*) Tloc(len,0);
	scaledigits = (int*) Tloc(scale,0);

	for( i = 6; msg == MAL_SUCCEED && i< pci->argc; i++, o++){
		tblname = BUNtvar(itertbl,o);
		colname = BUNtvar(iteratr,o);
		tpename = BUNtvar(itertpe,o);

		v = getArgReference(stk, pci, i);
		mtype = getArgType(mb, pci, i);
		if (ATOMextern(mtype))
			v = *(ptr *) v;
		if (mvc_result_value(m, tblname, colname, tpename, *digits++, *scaledigits++, v, mtype))
			throw(SQL, "sql.rsColumn", SQLSTATE(45000) "Result set construction failed");
	}
	if (mvc_export_result(cntxt->sqlcontext, cntxt->fdout, res, true, mb->starttime, mb->optimize))
		msg = createException(SQL, "sql.resultset", SQLSTATE(45000) "Result set construction failed");
	mb->starttime = 0;
	mb->optimize = 0;
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
	const char *format = *getArgReference_str(stk,pci,2);
	const char *tsep = *getArgReference_str(stk, pci, 3);
	const char *rsep = *getArgReference_str(stk, pci, 4);
	const char *ssep = *getArgReference_str(stk, pci, 5);
	const char *ns = *getArgReference_str(stk, pci, 6);
	int onclient = *getArgReference_int(stk, pci, 7);

	bat tblId= *getArgReference_bat(stk, pci,8);
	bat atrId= *getArgReference_bat(stk, pci,9);
	bat tpeId= *getArgReference_bat(stk, pci,10);
	bat lenId= *getArgReference_bat(stk, pci,11);
	bat scaleId= *getArgReference_bat(stk, pci,12);

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
	bool tostdout;
	char buf[80];
	ssize_t sz;

	(void) format;
	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (onclient && !cntxt->filetrans) {
		throw(MAL, "sql.resultSet", "cannot transfer files to client");
	}

	res = *res_id = mvc_result_table(m, mb->tag, pci->argc - (pci->retc + 12), Q_TABLE, NULL);

	t = m->results;
	if (res < 0){
		msg = createException(SQL, "sql.resultSet", SQLSTATE(45000) "Result set construction failed");
		goto wrapup_result_set;
	}

	t->tsep = tsep;
	t->rsep = rsep;
	t->ssep = ssep;
	t->ns = ns;

	tbl = BATdescriptor(tblId);
	atr = BATdescriptor(atrId);
	tpe = BATdescriptor(tpeId);
	len = BATdescriptor(lenId);
	scale = BATdescriptor(scaleId);
	if( msg || tbl == NULL || atr == NULL || tpe == NULL || len == NULL || scale == NULL)
		goto wrapup_result_set;
	/* mimick the old rsColumn approach; */
	itertbl = bat_iterator(tbl);
	iteratr = bat_iterator(atr);
	itertpe = bat_iterator(tpe);
	digits = (int*) Tloc(len,0);
	scaledigits = (int*) Tloc(scale,0);

	for( i = 13; msg == MAL_SUCCEED && i< pci->argc; i++, o++){
		tblname = BUNtvar(itertbl,o);
		colname = BUNtvar(iteratr,o);
		tpename = BUNtvar(itertpe,o);

		v = getArgReference(stk, pci, i);
		mtype = getArgType(mb, pci, i);
		if (ATOMextern(mtype))
			v = *(ptr *) v;
		if (mvc_result_value(m, tblname, colname, tpename, *digits++, *scaledigits++, v, mtype))
			throw(SQL, "sql.rsColumn", SQLSTATE(45000) "Result set construction failed");
	}
	/* now select the file channel */
	if ((tostdout = strcmp(filename,"stdout") == 0)) {
		s = cntxt->fdout;
	} else if (!onclient) {
		if ((s = open_wastream(filename)) == NULL || mnstr_errnr(s)) {
			msg=  createException(IO, "streams.open", SQLSTATE(42000) "could not open file '%s': %s",
					      filename?filename:"stdout", GDKstrerror(errno, (char[128]){0}, 128));
			close_stream(s);
			goto wrapup_result_set;
		}
	} else {
		while (!m->scanner.rs->eof)
			bstream_next(m->scanner.rs);
		s = m->scanner.ws;
		mnstr_write(s, PROMPT3, sizeof(PROMPT3) - 1, 1);
		mnstr_printf(s, "w %s\n", filename);
		mnstr_flush(s);
		if ((sz = mnstr_readline(m->scanner.rs->s, buf, sizeof(buf))) > 1) {
			/* non-empty line indicates failure on client */
			msg = createException(IO, "streams.open", "%s", buf);
			/* deal with ridiculously long response from client */
			while (buf[sz - 1] != '\n' &&
			       (sz = mnstr_readline(m->scanner.rs->s, buf, sizeof(buf))) > 0)
				;
			goto wrapup_result_set;
		}
	}
	if (mvc_export_result(cntxt->sqlcontext, s, res, strcmp(filename, "stdout") == 0, mb->starttime, mb->optimize)){
		msg = createException(SQL, "sql.resultset", SQLSTATE(45000) "Result set construction failed");
		goto wrapup_result_set;
	}
	mb->starttime = 0;
	mb->optimize = 0;
	if (onclient) {
		mnstr_flush(s);
		if ((sz = mnstr_readline(m->scanner.rs->s, buf, sizeof(buf))) > 1) {
			msg = createException(IO, "streams.open", "%s", buf);
		}
		while (sz > 0)
			sz = mnstr_readline(m->scanner.rs->s, buf, sizeof(buf));
	} else if (!tostdout) {
		close_stream(s);
	}
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
	int nr_cols;
	mapi_query_t qtype;
	bat order_bid;

	if ( pci->argc > 6)
		return mvc_result_set_wrap(cntxt,mb,stk,pci);

	res_id = getArgReference_int(stk, pci, 0);
	nr_cols = *getArgReference_int(stk, pci, 1);
	qtype = (mapi_query_t) *getArgReference_int(stk, pci, 2);
	order_bid = *getArgReference_bat(stk, pci, 3);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if ((order = BATdescriptor(order_bid)) == NULL) {
		throw(SQL, "sql.resultSet", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	*res_id = mvc_result_table(m, mb->tag, nr_cols, qtype, order);
	if (*res_id < 0)
		res = createException(SQL, "sql.resultSet", SQLSTATE(45000) "Result set construction failed");
	BBPunfix(order->batCacheid);
	return res;
}

/* str mvc_affected_rows_wrap(int *m, int m, lng *nr, str *w); */
str
mvc_affected_rows_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *b = NULL;
	int *res = getArgReference_int(stk, pci, 0), error;
#ifndef NDEBUG
	int mtype = getArgType(mb, pci, 2);
#endif
	lng nr;
	str msg;

	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	*res = 0;
	assert(mtype == TYPE_lng);
	nr = *getArgReference_lng(stk, pci, 2);
	b = cntxt->sqlcontext;
	error = mvc_export_affrows(b, b->out, nr, "", mb->tag, mb->starttime, mb->optimize);
	mb->starttime = 0;
	mb->optimize = 0;
	if (error)
		throw(SQL, "sql.affectedRows", SQLSTATE(45000) "Result set construction failed");
	return MAL_SUCCEED;
}

/* str mvc_export_head_wrap(int *ret, stream **s, int *res_id); */
str
mvc_export_head_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *b = NULL;
	stream **s = (stream **) getArgReference(stk, pci, 1);
	int res_id = *getArgReference_int(stk, pci, 2);
	str msg;

	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = cntxt->sqlcontext;
	if (mvc_export_head(b, *s, res_id, FALSE, TRUE, mb->starttime, mb->optimize))
		throw(SQL, "sql.exportHead", SQLSTATE(45000) "Result set construction failed");
	mb->starttime = 0;
	mb->optimize = 0;
	return MAL_SUCCEED;
}

/* str mvc_export_result_wrap(int *ret, stream **s, int *res_id); */
str
mvc_export_result_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *b = NULL;
	stream **s = (stream **) getArgReference(stk, pci, 1);
	int res_id = *getArgReference_int(stk, pci, 2);
	str msg;

	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = cntxt->sqlcontext;
	if( pci->argc > 5){
		res_id = *getArgReference_int(stk, pci, 2);
		if (mvc_export_result(b, cntxt->fdout, res_id, true, mb->starttime, mb->optimize))
			throw(SQL, "sql.exportResult", SQLSTATE(45000) "Result set construction failed");
	} else if (mvc_export_result(b, *s, res_id, false, mb->starttime, mb->optimize))
		throw(SQL, "sql.exportResult", SQLSTATE(45000) "Result set construction failed");
	mb->starttime = 0;
	mb->optimize = 0;
	return MAL_SUCCEED;
}

/* str mvc_export_chunk_wrap(int *ret, stream **s, int *res_id, str *w); */
str
mvc_export_chunk_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *b = NULL;
	stream **s = (stream **) getArgReference(stk, pci, 1);
	int res_id = *getArgReference_int(stk, pci, 2);
	BUN offset = 0;
	BUN nr = 0;
	str msg;

	(void) mb;		/* NOT USED */
	if (pci->argc == 5) {
		offset = (BUN) *getArgReference_int(stk, pci, 3);
		int cnt = *getArgReference_int(stk, pci, 4);
		nr = cnt < 0 ? BUN_NONE : (BUN) cnt;
	}

	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = cntxt->sqlcontext;
	if (mvc_export_chunk(b, *s, res_id, offset, nr))
		throw(SQL, "sql.exportChunk", SQLSTATE(45000) "Result set construction failed");
	return NULL;
}

/* str mvc_export_operation_wrap(int *ret, str *w); */
str
mvc_export_operation_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *b = NULL;
	str msg;

	(void) stk;		/* NOT USED */
	(void) pci;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = cntxt->sqlcontext;
	if (mvc_export_operation(b, b->out, "", mb->starttime, mb->optimize))
		throw(SQL, "sql.exportOperation", SQLSTATE(45000) "Result set construction failed");
	mb->starttime = 0;
	mb->optimize = 0;
	return NULL;
}

str
/*mvc_scalar_value_wrap(int *ret, int *qtype, str tn, str name, str type, int *digits, int *scale, int *eclass, ptr p, int mtype)*/
mvc_scalar_value_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	const char *tn = *getArgReference_str(stk, pci, 1);
	const char *cn = *getArgReference_str(stk, pci, 2);
	const char *type = *getArgReference_str(stk, pci, 3);
	int digits = *getArgReference_int(stk, pci, 4);
	int scale = *getArgReference_int(stk, pci, 5);
	ptr p = getArgReference(stk, pci, 7);
	int mtype = getArgType(mb, pci, 7);
	str msg;
	backend *b = NULL;
	int res_id;
	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = cntxt->sqlcontext;
	if (ATOMextern(mtype))
		p = *(ptr *) p;

	// scalar values are single-column result sets
	if ((res_id = mvc_result_table(b->mvc, mb->tag, 1, Q_TABLE, NULL)) < 0)
		throw(SQL, "sql.exportValue", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if (mvc_result_value(b->mvc, tn, cn, type, digits, scale, p, mtype))
		throw(SQL, "sql.exportValue", SQLSTATE(45000) "Result set construction failed");
	if (b->output_format == OFMT_NONE) {
		return MAL_SUCCEED;
	}
	if (mvc_export_result(b, b->out, res_id, true, mb->starttime, mb->optimize) < 0) {
		throw(SQL, "sql.exportValue", SQLSTATE(45000) "Result set construction failed");
	}
	mb->starttime = 0;
	mb->optimize = 0;
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

static char fwftsep[2] = {STREAM_FWF_FIELD_SEP, '\0'};
static char fwfrsep[2] = {STREAM_FWF_RECORD_SEP, '\0'};

/* str mvc_import_table_wrap(int *res, str *sname, str *tname, unsigned char* *T, unsigned char* *R, unsigned char* *S, unsigned char* *N, str *fname, lng *sz, lng *offset, int *locked, int *besteffort, str *fixed_width, int *onclient); */
str
mvc_import_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *be;
	BAT **b = NULL;
	ssize_t len = 0;
	sql_table *t = *(sql_table **) getArgReference(stk, pci, pci->retc + 0);
	const char *tsep = *getArgReference_str(stk, pci, pci->retc + 1);
	const char *rsep = *getArgReference_str(stk, pci, pci->retc + 2);
	const char *ssep = *getArgReference_str(stk, pci, pci->retc + 3);
	const char *ns = *getArgReference_str(stk, pci, pci->retc + 4);
	const char *fname = *getArgReference_str(stk, pci, pci->retc + 5);
	lng sz = *getArgReference_lng(stk, pci, pci->retc + 6);
	lng offset = *getArgReference_lng(stk, pci, pci->retc + 7);
	int locked = *getArgReference_int(stk, pci, pci->retc + 8);
	int besteffort = *getArgReference_int(stk, pci, pci->retc + 9);
	char *fixed_widths = *getArgReference_str(stk, pci, pci->retc + 10);
	int onclient = *getArgReference_int(stk, pci, pci->retc + 11);
	str msg = MAL_SUCCEED;
	bstream *s = NULL;
	stream *ss;

	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (onclient && !cntxt->filetrans) {
		throw(MAL, "sql.copy_from", "cannot transfer files from client");
	}

	be = cntxt->sqlcontext;
	/* The CSV parser expects ssep to have the value 0 if the user does not
	 * specify a quotation character
	 */
	if (*ssep == 0 || strNil(ssep))
		ssep = NULL;

	if (strNil(fname))
		fname = NULL;
	if (fname == NULL) {
		msg = mvc_import_table(cntxt, &b, be->mvc, be->mvc->scanner.rs, t, tsep, rsep, ssep, ns, sz, offset, locked, besteffort, true);
	} else {
		if (onclient) {
			mnstr_write(be->mvc->scanner.ws, PROMPT3, sizeof(PROMPT3)-1, 1);
			if (offset > 1 && rsep && rsep[0] == '\n' && rsep[1] == '\0') {
				/* only let client skip simple lines */
				mnstr_printf(be->mvc->scanner.ws, "r " LLFMT " %s\n",
					     offset, fname);
				offset = 0;
			} else {
				mnstr_printf(be->mvc->scanner.ws, "r 0 %s\n", fname);
			}
			msg = MAL_SUCCEED;
			mnstr_flush(be->mvc->scanner.ws);
			while (!be->mvc->scanner.rs->eof)
				bstream_next(be->mvc->scanner.rs);
			ss = be->mvc->scanner.rs->s;
			char buf[80];
			if ((len = mnstr_readline(ss, buf, sizeof(buf))) > 1) {
				if (buf[0] == '!' && buf[6] == '!')
					msg = createException(IO, "sql.copy_from", "%.7s%s: %s", buf, fname, buf+7);
				else
					msg = createException(IO, "sql.copy_from", "%s: %s", fname, buf);
				while (buf[len - 1] != '\n' &&
				       (len = mnstr_readline(ss, buf, sizeof(buf))) > 0)
					;
				/* read until flush marker */
				while (mnstr_read(ss, buf, 1, sizeof(buf)) > 0)
					;
				return msg;
			}
		} else {
			ss = open_rastream(fname);
			if (ss == NULL || mnstr_errnr(ss)) {
				msg = createException(IO, "sql.copy_from", SQLSTATE(42000) "Cannot open file '%s': %s", fname, GDKstrerror(errno, (char[128]){0}, 128));
				close_stream(ss);
				return msg;
			}
		}

		if (!strNil(fixed_widths)) {
			size_t ncol = 0, current_width_entry = 0, i;
			size_t *widths;
			char* val_start = fixed_widths;
			size_t width_len = strlen(fixed_widths);
			for (i = 0; i < width_len; i++) {
				if (fixed_widths[i] == '|') {
					ncol++;
				}
			}
			widths = malloc(sizeof(size_t) * ncol);
			if (!widths) {
				close_stream(ss);
				throw(MAL, "sql.copy_from", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			for (i = 0; i < width_len; i++) {
				if (fixed_widths[i] == STREAM_FWF_FIELD_SEP) {
					fixed_widths[i] = '\0';
					widths[current_width_entry++] = (size_t) strtoll(val_start, NULL, 10);
					val_start = fixed_widths + i + 1;
				}
			}
			/* overwrite other delimiters to the ones the FWF stream uses */
			tsep = fwftsep;
			rsep = fwfrsep;

			ss = stream_fwf_create(ss, ncol, widths, STREAM_FWF_FILLER);
		}
#if SIZEOF_VOID_P == 4
		s = bstream_create(ss, 0x20000);
#else
		s = bstream_create(ss, 0x200000);
#endif
		if (s != NULL) {
			msg = mvc_import_table(cntxt, &b, be->mvc, s, t, tsep, rsep, ssep, ns, sz, offset, locked, besteffort, false);
			if (onclient) {
				mnstr_write(be->mvc->scanner.ws, PROMPT3, sizeof(PROMPT3)-1, 1);
				mnstr_flush(be->mvc->scanner.ws);
				be->mvc->scanner.rs->eof = s->eof;
				s->s = NULL;
			}
			bstream_destroy(s);
		}
	}
	if (fname && s == NULL)
		throw(IO, "bstreams.create", SQLSTATE(42000) "Failed to create block stream");
	if (b == NULL)
		throw(SQL, "importTable", SQLSTATE(42000) "Failed to import table '%s', %s", t->base.name, be->mvc->errstr);
	bat2return(stk, pci, b);
	GDKfree(b);
	return msg;
}

static bool
read_more(bstream *in, stream *out)
{
	do {
		if (bstream_next(in) < 0)
			return false;
		if (in->eof) {
			if (mnstr_write(out, PROMPT2, sizeof(PROMPT2) - 1, 1) != 1
			    || mnstr_flush(out) < 0)
				return false;
			in->eof = false;
			if (bstream_next(in) <= 0)
				return false;
		}
	} while (in->len <= in->pos);
	return true;
}

static BAT *
BATattach_bstream(int tt, bstream *in, stream *out, BUN size)
{
	BAT *bn;
	size_t n;
	size_t asz = (size_t) ATOMsize(tt);

	bn = COLnew(0, tt, size, TRANSIENT);
	if (bn == NULL)
		return NULL;

	if (ATOMstorage(tt) < TYPE_str) {
		while (read_more(in, out)) {
			n = (in->len - in->pos) / asz;
			if (BATextend(bn, bn->batCount + n) != GDK_SUCCEED) {
				BBPreclaim(bn);
				return NULL;
			}
			memcpy(Tloc(bn, bn->batCount), in->buf + in->pos, n * asz);
			bn->batCount += (BUN) n;
			in->pos += n * asz;
		}
		BATsetcount(bn, bn->batCount);
		bn->tseqbase = oid_nil;
		bn->tnonil = bn->batCount == 0;
		bn->tnil = false;
		if (bn->batCount <= 1) {
			bn->tsorted = true;
			bn->trevsorted = true;
			bn->tkey = true;
		} else {
			bn->tsorted = false;
			bn->trevsorted = false;
			bn->tkey = false;
		}
	} else {
		assert(ATOMstorage(tt) == TYPE_str);
		while (read_more(in, out)) {
			int u;
			for (n = in->pos, u = 0; n < in->len; n++) {
				int c = in->buf[n];
				if (u) {
					if ((c & 0xC0) == 0x80)
						u--;
					else
						goto bailout;
				} else if ((c & 0xF8) == 0xF0) {
					u = 3;
				} else if ((c & 0xF0) == 0xE0) {
					u = 2;
				} else if ((c & 0xE0) == 0xC0) {
					u = 1;
				} else if ((c & 0xC0) == 0x80) {
					goto bailout;
				} else if (c == '\r') {
					if (n + 1 < in->len
					    && in->buf[n + 1] == '\n') {
						in->buf[n] = 0;
						if (BUNappend(bn, in->buf + in->pos, false) != GDK_SUCCEED)
							goto bailout;
						in->buf[n] = '\r';
						in->pos = n + 2;
						n++;
					}
				} else if (c == '\n' || c == '\0') {
					in->buf[n] = 0;
					if (BUNappend(bn, in->buf + in->pos, false) != GDK_SUCCEED)
						goto bailout;
					in->buf[n] = c;
					in->pos = n + 1;
				}
			}
		}
	}
	return bn;

  bailout:
	BBPreclaim(bn);
	return NULL;
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
	bool init = false;
	int i;
	const char *sname = *getArgReference_str(stk, pci, 0 + pci->retc);
	const char *tname = *getArgReference_str(stk, pci, 1 + pci->retc);
	int onclient = *getArgReference_int(stk, pci, 2 + pci->retc);
	sql_schema *s;
	sql_table *t;
	node *n;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if ((s = mvc_bind_schema(m, sname)) == NULL)
		throw(SQL, "sql.import_table", SQLSTATE(3F000) "Schema missing %s",sname);
	t = mvc_bind_table(m, s, tname);
	if (!t)
		throw(SQL, "sql", SQLSTATE(42S02) "Table missing %s", tname);
	if (list_length(t->columns.set) != (pci->argc - (3 + pci->retc)))
		throw(SQL, "sql", SQLSTATE(42000) "Not enough columns found in input file");
	if (2 * pci->retc + 3 != pci->argc)
		throw(SQL, "sql", SQLSTATE(42000) "Not enough output values");

	if (onclient && !cntxt->filetrans) {
		throw(MAL, "sql.copy_from", "cannot transfer files from client");
	}

	backend *be = cntxt->sqlcontext;

	for (i = 0; i < pci->retc; i++)
		*getArgReference_bat(stk, pci, i) = 0;

	for (i = pci->retc + 3, n = t->columns.set->h; i < pci->argc && n; i++, n = n->next) {
		sql_column *col = n->data;
		BAT *c = NULL;
		int tpe = col->type.type->localtype;
		const char *fname = *getArgReference_str(stk, pci, i);

		/* handle the various cases */
		if (strNil(fname)) {
			// no filename for this column, skip for now because we potentially don't know the count yet
			continue;
		}
		if (ATOMvarsized(tpe) && tpe != TYPE_str) {
			msg = createException(SQL, "sql", SQLSTATE(42000) "Failed to attach file %s", *getArgReference_str(stk, pci, i));
			goto bailout;
		}

		if (tpe <= TYPE_str || tpe == TYPE_date || tpe == TYPE_daytime || tpe == TYPE_timestamp) {
			if (onclient) {
				mnstr_write(be->mvc->scanner.ws, PROMPT3, sizeof(PROMPT3)-1, 1);
				mnstr_printf(be->mvc->scanner.ws, "rb %s\n", fname);
				msg = MAL_SUCCEED;
				mnstr_flush(be->mvc->scanner.ws);
				while (!be->mvc->scanner.rs->eof)
					bstream_next(be->mvc->scanner.rs);
				stream *ss = be->mvc->scanner.rs->s;
				char buf[80];
				if (mnstr_readline(ss, buf, sizeof(buf)) > 1) {
					msg = createException(IO, "sql.attach", "%s", buf);
					goto bailout;
				}
				bstream *s = bstream_create(ss, 1 << 20);
				if (!s) {
					msg = createException(SQL, "sql", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
				if (!(c = BATattach_bstream(col->type.type->localtype, s, be->mvc->scanner.ws, cnt))) {
					bstream_destroy(s);
					msg = createException(SQL, "sql", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
				mnstr_write(be->mvc->scanner.ws, PROMPT3, sizeof(PROMPT3)-1, 1);
				mnstr_flush(be->mvc->scanner.ws);
				be->mvc->scanner.rs->eof = s->eof;
				s->s = NULL;
				bstream_destroy(s);
			} else if (tpe == TYPE_str) {
				/* get the BAT and fill it with the strings */
				c = COLnew(0, TYPE_str, 0, TRANSIENT);
				if (c == NULL) {
					msg = createException(SQL, "sql", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
				/* this code should be extended to
				 * deal with larger text strings. */
				FILE *f = fopen(fname, "r");
				if (f == NULL) {
					BBPreclaim(c);
					msg = createException(SQL, "sql", SQLSTATE(42000) "Failed to re-open file %s", fname);
					goto bailout;
				}

#define bufsiz	(128 * BLOCK)
				char *buf = GDKmalloc(bufsiz);
				if (buf == NULL) {
					fclose(f);
					BBPreclaim(c);
					msg = createException(SQL, "sql", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
				while (fgets(buf, bufsiz, f) != NULL) {
					char *t = strrchr(buf, '\n');
					if (t)
						*t = 0;
					if (BUNappend(c, buf, false) != GDK_SUCCEED) {
						BBPreclaim(c);
						fclose(f);
						msg = createException(SQL, "sql", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
				}
#undef bufsiz
				fclose(f);
				GDKfree(buf);
			} else {
				c = BATattach(tpe, fname, TRANSIENT);
			}
			if (c == NULL) {
				msg = createException(SQL, "sql", SQLSTATE(42000) "Failed to attach file %s", fname);
				goto bailout;
			}
			if (BATsetaccess(c, BAT_READ) != GDK_SUCCEED) {
				BBPreclaim(c);
				msg = createException(SQL, "sql", SQLSTATE(42000) "Failed to set internal access while attaching file %s", fname);
				goto bailout;
			}
		} else {
			msg = createException(SQL, "sql", SQLSTATE(42000) "Failed to attach file %s", fname);
			goto bailout;
		}
		if (init && cnt != BATcount(c)) {
			BBPunfix(c->batCacheid);
			msg = createException(SQL, "sql", SQLSTATE(42000) "Binary files for table '%s' have inconsistent counts", tname);
			goto bailout;
		}
		cnt = BATcount(c);
		init = true;
		*getArgReference_bat(stk, pci, i - (3 + pci->retc)) = c->batCacheid;
		BBPkeepref(c->batCacheid);
	}
	if (init) {
		for (i = pci->retc + 3, n = t->columns.set->h; i < pci->argc && n; i++, n = n->next) {
			// now that we know the BAT count, we can fill in the columns for which no parameters were passed
			sql_column *col = n->data;
			BAT *c = NULL;
			int tpe = col->type.type->localtype;

			const char *fname = *getArgReference_str(stk, pci, i);
			if (strNil(fname)) {
				// fill the new BAT with NULL values
				c = BATconstant(0, tpe, ATOMnilptr(tpe), cnt, TRANSIENT);
				if (c == NULL) {
					msg = createException(SQL, "sql", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
				*getArgReference_bat(stk, pci, i - (3 + pci->retc)) = c->batCacheid;
				BBPkeepref(c->batCacheid);
			}
		}
	}
	return MAL_SUCCEED;
  bailout:
	for (i = 0; i < pci->retc; i++) {
		bat bid;
		if ((bid = *getArgReference_bat(stk, pci, i)) != 0) {
			BBPrelease(bid);
			*getArgReference_bat(stk, pci, i) = 0;
		}
	}
	return msg;
}

str
not_unique(bit *ret, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "not_unique", SQLSTATE(HY005) "Cannot access column descriptor");
	}

	*ret = FALSE;
	if (BATtkey(b) || BATtdense(b) || BATcount(b) <= 1) {
		BBPunfix(b->batCacheid);
		return MAL_SUCCEED;
	} else if (b->tsorted) {
		BUN p, q;
		oid c = *(oid *) Tloc(b, 0);

		for (p = 1, q = BUNlast(b); p < q; p++) {
			oid v = *(oid *) Tloc(b, p);
			if (v <= c) {
				*ret = TRUE;
				break;
			}
			c = v;
		}
	} else {
		BBPunfix(b->batCacheid);
		throw(SQL, "not_unique", SQLSTATE(42000) "Input column should be sorted");
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
	bat bid = *getArgReference_bat(stk, pci, 2);
	oid s = *getArgReference_oid(stk, pci, 3);
	BAT *b, *bn = NULL;

	(void) cntxt;
	(void) mb;
	if ((b = BATdescriptor(bid)) == NULL) {
		throw(MAL, "batcalc.identity", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn = BATdense(b->hseqbase, s, BATcount(b));
	if (bn != NULL) {
		*ns = s + BATcount(b);
		BBPunfix(b->batCacheid);
		BBPkeepref(*res = bn->batCacheid);
		return MAL_SUCCEED;
	}
	BBPunfix(b->batCacheid);
	throw(MAL, "batcalc.identity", SQLSTATE(45001) "Internal error");

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
	if (!is_daytime_nil(*v) && d < 6) {
#ifdef TRUNCATE_NUMBERS
		*res = (daytime) (*res / scales[6 - d]);
#else
		*res = (daytime) ((*res + scales[5 - d]*5) / scales[6 - d]);
#endif
		*res = (daytime) (*res * scales[6 - d]);
	}
	return MAL_SUCCEED;
}

str
second_interval_2_daytime(daytime *res, const lng *s, const int *digits)
{
	daytime d;

	if (*s == lng_nil) {
		*res = daytime_nil;
		return MAL_SUCCEED;
	}
	d = daytime_add_usec(daytime_create(0, 0, 0, 0), *s * 1000);
	return daytime_2time_daytime(res, &d, digits);
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
	size_t len = sizeof(daytime);
	ssize_t pos;

	if (strNil(*v)) {
		*res = daytime_nil;
		return MAL_SUCCEED;
	}
	if (*tz)
		pos = daytime_tz_fromstr(*v, &len, &res, false);
	else
		pos = daytime_fromstr(*v, &len, &res, false);
	if (pos < (ssize_t) strlen(*v) || /* includes pos < 0 */
	    ATOMcmp(TYPE_daytime, res, ATOMnilptr(TYPE_daytime)) == 0)
		throw(SQL, "daytime", SQLSTATE(22007) "Daytime (%s) has incorrect format", *v);
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
	daytime dt;

	dt = timestamp_daytime(*v);

	/* correct fraction */
	if (!is_daytime_nil(dt) && d < 6) {
#ifdef TRUNCATE_NUMBERS
		dt /= scales[6 - d];
#else
		dt = (dt + scales[5 - d]*5) / scales[6 - d];
#endif
		dt *= scales[6 - d];
	}
	*res = dt;
	return MAL_SUCCEED;
}

str
date_2_timestamp(timestamp *res, const date *v, const int *digits)
{
	(void) digits;		/* no precision needed */
	*res = timestamp_fromdate(*v);
	return MAL_SUCCEED;
}

str
timestamp_2time_timestamp(timestamp *res, const timestamp *v, const int *digits)
{
	int d = (*digits) ? *digits - 1 : 0;
	date dt;
	daytime tm;

	dt = timestamp_date(*v);
	tm = timestamp_daytime(*v);
	/* correct fraction */
	if (!is_daytime_nil(tm) && d < 6) {
#ifdef TRUNCATE_NUMBERS
		tm /= scales[6 - d];
#else
		tm = (tm + scales[5 - d]*5) / scales[6 - d];
#endif
		tm *= scales[6 - d];
	}
	*res = timestamp_create(dt, tm);
	return MAL_SUCCEED;
}

str
nil_2time_timestamp(timestamp *res, const void *v, const int *digits)
{
	(void) digits;
	(void) v;
	*res = timestamp_nil;
	return MAL_SUCCEED;
}

str
str_2time_timestamptz(timestamp *res, const str *v, const int *digits, int *tz)
{
	size_t len = sizeof(timestamp);
	ssize_t pos;

	if (strNil(*v)) {
		*res = timestamp_nil;
		return MAL_SUCCEED;
	}
	if (*tz)
		pos = timestamp_tz_fromstr(*v, &len, &res, false);
	else
		pos = timestamp_fromstr(*v, &len, &res, false);
	if (!pos || pos < (ssize_t) strlen(*v) || ATOMcmp(TYPE_timestamp, res, ATOMnilptr(TYPE_timestamp)) == 0)
		throw(SQL, "timestamp", SQLSTATE(22007) "Timestamp (%s) has incorrect format", *v);
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
	if (is_dbl_nil(*decl) || is_dbl_nil(*theta)) {
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
	BUN p, q;
	dbl s, c1, c2, r;
	char *msg = NULL;

	if (is_dbl_nil(*theta)) {
		throw(SQL, "SQLbat_alpha", SQLSTATE(42000) "Parameter theta should not be nil");
	}
	if ((b = BATdescriptor(*decl)) == NULL) {
		throw(SQL, "alpha", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	bn = COLnew(b->hseqbase, TYPE_dbl, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.alpha", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	s = sin(radians(*theta));
	const dbl *vals = (const dbl *) Tloc(b, 0);
	BATloop(b, p, q) {
		dbl d = vals[p];
		if (is_dbl_nil(d))
			r = dbl_nil;
		else if (fabs(d) + *theta > 89.9)
			r = 180.0;
		else {
			c1 = cos(radians(d - *theta));
			c2 = cos(radians(d + *theta));
			r = degrees(fabs(atan(s / sqrt(fabs(c1 * c2)))));
		}
		if (BUNappend(bn, &r, false) != GDK_SUCCEED) {
			BBPreclaim(bn);
			throw(SQL, "sql.alpha", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	*res = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
SQLcst_alpha_bat(bat *res, const dbl *decl, const bat *thetabid)
{
	BAT *b, *bn;
	BUN p, q;
	dbl s, c1, c2, r;
	char *msg = NULL;
	dbl *thetas;

	if ((b = BATdescriptor(*thetabid)) == NULL) {
		throw(SQL, "alpha", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	thetas = (dbl *) Tloc(b, 0);
	bn = COLnew(b->hseqbase, TYPE_dbl, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.alpha", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		dbl d = *decl;
		dbl theta = thetas[p];

		if (is_dbl_nil(d))
			r = dbl_nil;
		else if (fabs(d) + theta > 89.9)
			r = (dbl) 180.0;
		else {
			s = sin(radians(theta));
			c1 = cos(radians(d - theta));
			c2 = cos(radians(d + theta));
			r = degrees(fabs(atan(s / sqrt(fabs(c1 * c2)))));
		}
		if (BUNappend(bn, &r, false) != GDK_SUCCEED) {
			BBPreclaim(bn);
			throw(SQL, "sql.alpha", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
month_interval_str(int *ret, const str *s, const int *d, const int *sk)
{
	lng res;

	if (strNil(*s)) {
		*ret = int_nil;
	} else {
		if (interval_from_str(*s, *d, *sk, &res) < 0)
			throw(SQL, "calc.month_interval", SQLSTATE(42000) "Wrong format (%s)", *s);
		assert((lng) GDK_int_min <= res && res <= (lng) GDK_int_max);
		*ret = (int) res;
	}
	return MAL_SUCCEED;
}

str
second_interval_str(lng *res, const str *s, const int *d, const int *sk)
{
	if (strNil(*s)) {
		*res = lng_nil;
	} else {
		if (interval_from_str(*s, *d, *sk, res) < 0)
			throw(SQL, "calc.second_interval", SQLSTATE(42000) "Wrong format (%s)", *s);
	}
	return MAL_SUCCEED;
}

str
month_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = getArgReference_int(stk, pci, 0);
	int k = digits2ek(*getArgReference_int(stk, pci, 2)), r = 0, c;

	(void) cntxt;
	*ret = int_nil;
	switch (getArgType(mb, pci, 1)) {
	case TYPE_bte:
		if (is_bte_nil(stk->stk[getArg(pci, 1)].val.btval))
			return MAL_SUCCEED;
		r = stk->stk[getArg(pci, 1)].val.btval;
		break;
	case TYPE_sht:
		if (is_sht_nil(stk->stk[getArg(pci, 1)].val.shval))
			return MAL_SUCCEED;
		r = stk->stk[getArg(pci, 1)].val.shval;
		break;
	case TYPE_int:
		if (is_int_nil(stk->stk[getArg(pci, 1)].val.ival))
			return MAL_SUCCEED;
		r = stk->stk[getArg(pci, 1)].val.ival;
		break;
	case TYPE_lng: {
		lng l;
		if (is_lng_nil(stk->stk[getArg(pci, 1)].val.lval))
			return MAL_SUCCEED;
		l = stk->stk[getArg(pci, 1)].val.lval;
		if (l > GDK_int_max)
			throw(ILLARG, "calc.month_interval", SQLSTATE(22003) "Value " LLFMT " too large to fit at a month_interval", l);
		r = (int) l;
	} break;
#ifdef HAVE_HGE
	case TYPE_hge: {
		hge h;
		if (is_hge_nil(stk->stk[getArg(pci, 1)].val.hval))
			return MAL_SUCCEED;
		h = stk->stk[getArg(pci, 1)].val.hval;
		if (h > GDK_int_max)
			throw(ILLARG, "calc.month_interval", SQLSTATE(22003) "Value too large to fit at a month_interval");
		r = (int) h;
	} break;
#endif
	default:
		throw(ILLARG, "calc.month_interval", SQLSTATE(42000) "Illegal argument");
	}
	c = r;
	switch (k) {
	case iyear:
		c *= 12;
		break;
	case imonth:
		break;
	default:
		throw(ILLARG, "calc.month_interval", SQLSTATE(42000) "Illegal argument");
	}
	if (c < r)
		throw(ILLARG, "calc.month_interval", SQLSTATE(22003) "Overflow in convertion of %d to month_interval", r);
	*ret = c;
	return MAL_SUCCEED;
}

str
second_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	lng *ret = getArgReference_lng(stk, pci, 0), r, c;
	int k = digits2ek(*getArgReference_int(stk, pci, 2)), scale = 0;

	(void) cntxt;
	if (pci->argc > 3)
		scale = *getArgReference_int(stk, pci, 3);
	*ret = lng_nil;
	switch (getArgType(mb, pci, 1)) {
	case TYPE_bte:
		if (is_bte_nil(stk->stk[getArg(pci, 1)].val.btval))
			return MAL_SUCCEED;
		r = stk->stk[getArg(pci, 1)].val.btval;
		break;
	case TYPE_sht:
		if (is_sht_nil(stk->stk[getArg(pci, 1)].val.shval))
			return MAL_SUCCEED;
		r = stk->stk[getArg(pci, 1)].val.shval;
		break;
	case TYPE_int:
		if (is_int_nil(stk->stk[getArg(pci, 1)].val.ival))
			return MAL_SUCCEED;
		r = stk->stk[getArg(pci, 1)].val.ival;
		break;
	case TYPE_lng:
		if (is_lng_nil(stk->stk[getArg(pci, 1)].val.lval))
			return MAL_SUCCEED;
		r = stk->stk[getArg(pci, 1)].val.lval;
		break;
#ifdef HAVE_HGE
	case TYPE_hge: {
		hge h;
		if (is_hge_nil(stk->stk[getArg(pci, 1)].val.hval))
			return MAL_SUCCEED;
		h = stk->stk[getArg(pci, 1)].val.hval;
		if (h > GDK_lng_max)
			throw(ILLARG, "calc.sec_interval", SQLSTATE(22003) "Value too large to fit at a sec_interval");
		r = (lng) h;
	} break;
#endif
	default:
		throw(ILLARG, "calc.sec_interval", SQLSTATE(42000) "Illegal argument in second interval");
	}
	c = r;
	switch (k) {
	case iday:
		c *= 24;
		/* fall through */
	case ihour:
		c *= 60;
		/* fall through */
	case imin:
		c *= 60;
		/* fall through */
	case isec:
		c *= 1000;
		break;
	default:
		throw(ILLARG, "calc.sec_interval", SQLSTATE(42000) "Illegal argument in second interval");
	}
	if (scale) {
#ifndef TRUNCATE_NUMBERS
		c += 5*scales[scale-1];
#endif
		c /= scales[scale];
	}
	if (c < r)
		throw(ILLARG, "calc.sec_interval", SQLSTATE(22003) "Overflow in convertion of " LLFMT " to sec_interval", r);
	*ret = c;
	return MAL_SUCCEED;
}

str
second_interval_daytime(lng *res, const daytime *s, const int *d, const int *sk)
{
	int k = digits2sk(*d);
	lng r = *(int *) s;

	(void) sk;
	if (is_daytime_nil(*s)) {
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
		throw(ILLARG, "calc.second_interval", SQLSTATE(42000) "Illegal argument in daytime interval");
	}
	*res = r;
	return MAL_SUCCEED;
}

str
SQLcurrent_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	daytime *res = getArgReference_TYPE(stk, pci, 0, daytime);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;

	*res = timestamp_daytime(timestamp_add_usec(timestamp_current(),
						    m->timezone * LL_CONSTANT(1000)));
	return msg;
}

str
SQLcurrent_timestamp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	timestamp *res = getArgReference_TYPE(stk, pci, 0, timestamp);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;

	*res = timestamp_add_usec(timestamp_current(), m->timezone * LL_CONSTANT(1000));
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
	query = COLnew(0, TYPE_str, cnt, TRANSIENT);
	if (query == NULL)
		throw(SQL, "sql.dumpcache", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	count = COLnew(0, TYPE_int, cnt, TRANSIENT);
	if (count == NULL) {
		BBPunfix(query->batCacheid);
		throw(SQL, "sql.dumpcache", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	for (q = m->qc->q; q; q = q->next) {
		if (!q->prepared) {
			if (BUNappend(query, q->codestring, false) != GDK_SUCCEED ||
			    BUNappend(count, &q->count, false) != GDK_SUCCEED) {
				BBPunfix(query->batCacheid);
				BBPunfix(count->batCacheid);
				throw(SQL, "sql.dumpcache", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
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

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL ||
	    (msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	cnt = m->qc->id;
	rewrite = COLnew(0, TYPE_str, cnt, TRANSIENT);
	count = COLnew(0, TYPE_int, cnt, TRANSIENT);
	if (rewrite == NULL || count == NULL) {
		BBPreclaim(rewrite);
		BBPreclaim(count);
		throw(SQL, "sql.optstats", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	if (BUNappend(rewrite, "joinidx", false) != GDK_SUCCEED ||
	    BUNappend(count, &m->opt_stats[0], false) != GDK_SUCCEED) {
		BBPreclaim(rewrite);
		BBPreclaim(count);
		throw(SQL, "sql.optstats", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
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
	BAT *t[2];
	bat id;

	(void) cntxt;
	(void) mb;
	if (TRACEtable(cntxt, t) != 2)
		throw(SQL, "sql.dump_trace", SQLSTATE(3F000) "Profiler not started");
	for(i=0; i< 2; i++)
	if( t[i]){
		id = t[i]->batCacheid;
		*getArgReference_bat(stk, pci, i) = id;
		BBPkeepref(id);
	} else
		throw(SQL,"dump_trace", SQLSTATE(45000) "Missing trace BAT ");
	return MAL_SUCCEED;
}

str
sql_sessions_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return CLTsessions(cntxt, mb, stk, pci);
}

str
sql_rt_credentials_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *urib = NULL;
	BAT *unameb = NULL;
	BAT *hashb = NULL;
	bat *uri = getArgReference_bat(stk, pci, 0);
	bat *uname = getArgReference_bat(stk, pci, 1);
	bat *hash = getArgReference_bat(stk, pci, 2);
	str *table = getArgReference_str(stk, pci, 3);
	str uris;
	str unames;
	str hashs = NULL;
	str msg = MAL_SUCCEED;
	(void)mb;
	(void)cntxt;

	urib = COLnew(0, TYPE_str, 0, TRANSIENT);
	unameb = COLnew(0, TYPE_str, 0, TRANSIENT);
	hashb = COLnew(0, TYPE_str, 0, TRANSIENT);

	if (urib == NULL || unameb == NULL || hashb == NULL) {
		msg = createException(SQL, "sql.remote_table_credentials", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	if ((msg = AUTHgetRemoteTableCredentials(*table, &uris, &unames, &hashs)) != MAL_SUCCEED)
		goto bailout;

	MT_lock_set(&mal_contextLock);
	if (BUNappend(urib, uris, false) != GDK_SUCCEED)
		goto lbailout;
	if (BUNappend(unameb, unames, false) != GDK_SUCCEED)
		goto lbailout;
	if (BUNappend(hashb, hashs, false) != GDK_SUCCEED)
		goto lbailout;
	MT_lock_unset(&mal_contextLock);
	BBPkeepref(*uri = urib->batCacheid);
	BBPkeepref(*uname = unameb->batCacheid);
	BBPkeepref(*hash = hashb->batCacheid);

	if (hashs) GDKfree(hashs);
	return MAL_SUCCEED;

  lbailout:
	MT_lock_unset(&mal_contextLock);
	msg = createException(SQL, "sql.remote_table_credentials", SQLSTATE(HY013) MAL_MALLOC_FAIL);
  bailout:
	if (hashs) GDKfree(hashs);
	if (urib) BBPunfix(urib->batCacheid);
	if (unameb) BBPunfix(unameb->batCacheid);
	if (hashb) BBPunfix(hashb->batCacheid);
	return msg;
}

str
sql_querylog_catalog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	BAT *t[8];
	str msg;

	(void) cntxt;
	(void) mb;
	msg = QLOGcatalog(t);
	if( msg != MAL_SUCCEED)
		return msg;
	for (i = 0; i < 8; i++)
	if( t[i]){
		bat id = t[i]->batCacheid;

		*getArgReference_bat(stk, pci, i) = id;
		BBPkeepref(id);
	} else
		throw(SQL,"sql.querylog", SQLSTATE(45000) "Missing query catalog BAT");
	return MAL_SUCCEED;
}

str
sql_querylog_calls(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	BAT *t[10];
	str msg;

	(void) cntxt;
	(void) mb;
	msg = QLOGcalls(t);
	if( msg != MAL_SUCCEED)
		return msg;
	for (i = 0; i < 9; i++)
	if( t[i]){
		bat id = t[i]->batCacheid;

		*getArgReference_bat(stk, pci, i) = id;
		BBPkeepref(id);
	} else
		throw(SQL,"sql.querylog", SQLSTATE(45000) "Missing query call BAT");
	return MAL_SUCCEED;
}

str
sql_querylog_empty(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return QLOGempty(NULL);
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
	const char *sname = *getArgReference_str(stk, pci, 2);
	const char *tname = *getArgReference_str(stk, pci, 3);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		throw(SQL, "sql.rowid", SQLSTATE(3F000) "Schema missing %s", sname);
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "sql.rowid", SQLSTATE(42S02) "Table missing %s.%s",sname,tname);
	if (!s || !t || !t->columns.set->h)
		throw(SQL, "calc.rowid", SQLSTATE(42S22) "Column missing %s.%s",sname,tname);
	c = t->columns.set->h->data;
	/* HACK, get insert bat */
	b = store_funcs.bind_col(m->session->tr, c, RD_INS);
	if( b == NULL)
		throw(SQL,"sql.rowid", SQLSTATE(HY005) "Canot access column descriptor");
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
		throw(SQL, name, SQLSTATE(HY005) "Cannot access column descriptor");
	if ((g = BATdescriptor(*gid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, name, SQLSTATE(HY005) "Cannot access column descriptor");
	}
	bi = bat_iterator(b);
	gi = bat_iterator(g);
	ocmp = ATOMcompare(b->ttype);
	gcmp = ATOMcompare(g->ttype);
	oc = BUNtail(bi, 0);
	gc = BUNtail(gi, 0);
	if (!ALIGNsynced(b, g)) {
		BBPunfix(b->batCacheid);
		BBPunfix(g->batCacheid);
		throw(SQL, name, SQLSTATE(45000) "Internal error, columns not aligned");
	}
/*
  if (!BATtordered(b)) {
  BBPunfix(b->batCacheid);
  BBPunfix(g->batCacheid);
  throw(SQL, name, SQLSTATE(45000) "Internal error, columns not sorted");
  }
*/
	r = COLnew(b->hseqbase, TYPE_int, BATcount(b), TRANSIENT);
	if (r == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(g->batCacheid);
		throw(SQL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
		if (BUNappend(r, &rank, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPunfix(g->batCacheid);
			BBPunfix(r->batCacheid);
			throw(SQL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		nrank += !dense || c;
	}
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
		throw(SQL, name, SQLSTATE(HY005) "Cannot access column descriptor");
	if (!BATtordered(b) && !BATtrevordered(b)) {
		BBPunfix(b->batCacheid);
		throw(SQL, name, SQLSTATE(45000) "Internal error, columns not sorted");
	}

	bi = bat_iterator(b);
	cmp = ATOMcompare(b->ttype);
	cur = BUNtail(bi, 0);
	r = COLnew(b->hseqbase, TYPE_int, BATcount(b), TRANSIENT);
	if (r == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (BATtdense(b)) {
		BATloop(b, p, q) {
			if (BUNappend(r, &rank, false) != GDK_SUCCEED)
				goto bailout;
			rank++;
		}
	} else {
		BATloop(b, p, q) {
			n = BUNtail(bi, p);
			if ((c = cmp(n, cur)) != 0)
				rank = nrank;
			cur = n;
			if (BUNappend(r, &rank, false) != GDK_SUCCEED)
				goto bailout;
			nrank += !dense || c;
		}
	}
	BBPunfix(b->batCacheid);
	BBPkeepref(*rid = r->batCacheid);
	return MAL_SUCCEED;
  bailout:
	BBPunfix(b->batCacheid);
	BBPunfix(r->batCacheid);
	throw(SQL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
	s = instruction2str(mb, stk, getInstrPtr(mb, 0), LIST_MAL_CALL);
	if(s == NULL)
		throw(SQL, "sql.argRecord", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	t = strchr(s, ' ');
	if( ! t)
		t = strchr(s, '\t');
	*ret = GDKstrdup(t ? t + 1 : s);
	GDKfree(s);
	if(*ret == NULL)
		throw(SQL, "sql.argRecord", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/*
 * Vacuum cleaning tables
 * Shrinking and re-using space to vacuum clean the holes in the relations.
 */
static str
vacuum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, str (*func) (bat *, const bat *, const bat *), const char *name)
{
	const char *sch = *getArgReference_str(stk, pci, 1);
	const char *tbl = *getArgReference_str(stk, pci, 2);
	sql_trans *tr;
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	mvc *m = NULL;
	str msg;
	bat bid;
	BAT *b, *del;
	node *o;
	int i, bids[2049], err = 0;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, sch);
	if (s == NULL)
		throw(SQL, name, SQLSTATE(3F000) "Schema missing %s",sch);
	t = mvc_bind_table(m, s, tbl);
	if (t == NULL)
		throw(SQL, name, SQLSTATE(42S02) "Table missing %s.%s",sch,tbl);

	if (m->user_id != USER_MONETDB)
		throw(SQL, name, SQLSTATE(42000) "Insufficient privileges");
	if ((!list_empty(t->idxs.set) || !list_empty(t->keys.set)))
		throw(SQL, name, SQLSTATE(42000) "%s not allowed on tables with indices", name + 4);
	if (t->system)
		throw(SQL, name, SQLSTATE(42000) "%s not allowed on system tables", name + 4);

	if (has_snapshots(m->session->tr))
		throw(SQL, name, SQLSTATE(42000) "%s not allowed on snapshots", name + 4);
	if (!m->session->auto_commit)
		throw(SQL, name, SQLSTATE(42000) "%s only allowed in auto commit mode", name + 4);

	tr = m->session->tr;

	/* get the deletions BAT */
	del = mvc_bind_dbat(m, sch, tbl, RD_INS);
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
				BBPrelease(bids[i]);
			if (b)
				BBPunfix(b->batCacheid);
			BBPunfix(del->batCacheid);
			if (!msg)
				throw(SQL, name, SQLSTATE(HY005) "Cannot access column descriptor");
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
			BBPrelease(bids[i]);
		throw(SQL, name, SQLSTATE(42000) "Too many columns to handle, use copy instead");
	}
	BBPunfix(del->batCacheid);

	if (mvc_clear_table(m, t) == BUN_NONE)
		throw(SQL, name, SQLSTATE(42000) "vacumm: clear failed");
	for (o = t->columns.set->h, i = 0; o; o = o->next, i++) {
		sql_column *c = o->data;
		BAT *ins = BATdescriptor(bids[i]);	/* use the insert bat */

		if( ins){
			if (store_funcs.append_col(tr, c, ins, TYPE_bat) != LOG_OK)
				err = 1;
			BBPunfix(ins->batCacheid);
		}
		BBPrelease(bids[i]);
	}
	if (err)
		throw(SQL, name, SQLSTATE(42000) "vacuum: reappend failed");
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
	const char *sch = *getArgReference_str(stk, pci, 1);
	const char *tbl = *getArgReference_str(stk, pci, 2);
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
	BUN dcnt;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, sch);
	if (s == NULL)
		throw(SQL, "sql.vacuum", SQLSTATE(3F000) "Schema missing %s",sch);
	t = mvc_bind_table(m, s, tbl);
	if (t == NULL)
		throw(SQL, "sql.vacuum", SQLSTATE(42S02) "Table missing %s.%s",sch,tbl);

	if (m->user_id != USER_MONETDB)
		throw(SQL, "sql.vacuum", SQLSTATE(42000) "insufficient privileges");
	if ((!list_empty(t->idxs.set) || !list_empty(t->keys.set)))
		throw(SQL, "sql.vacuum", SQLSTATE(42000) "vacuum not allowed on tables with indices");
	if (t->system)
		throw(SQL, "sql.vacuum", SQLSTATE(42000) "vacuum not allowed on system tables");

	if (has_snapshots(m->session->tr))
		throw(SQL, "sql.vacuum", SQLSTATE(42000) "vacuum not allowed on snapshots");

	if (!m->session->auto_commit)
		throw(SQL, "sql.vacuum", SQLSTATE(42000) "vacuum only allowed in auto commit mode");
	tr = m->session->tr;

	for (o = t->columns.set->h; o && ordered == 0; o = o->next) {
		c = o->data;
		b = store_funcs.bind_col(tr, c, RDONLY);
		if (b == NULL)
			throw(SQL, "sql.vacuum", SQLSTATE(HY005) "Cannot access column descriptor");
		ordered |= BATtordered(b);
		cnt = BATcount(b);
		BBPunfix(b->batCacheid);
	}

	/* get the deletions BAT */
	del = mvc_bind_dbat(m, sch, tbl, RD_INS);
	if( del == NULL)
		throw(SQL, "sql.vacuum", SQLSTATE(HY005) "Cannot access deletion column");

	dcnt = BATcount(del);
	BBPunfix(del->batCacheid);
	if (dcnt > 0) {
		/* now decide on the algorithm */
		if (ordered) {
			if (dcnt > cnt / 20)
				return SQLshrink(cntxt, mb, stk, pci);
		} else {
			return SQLreuse(cntxt, mb, stk, pci);
		}
	}
	return MAL_SUCCEED;
}

/*
 * The drop_hash operation cleans up any hash indices on any of the tables columns.
 */
str
SQLdrop_hash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	const char *sch = *getArgReference_str(stk, pci, 1);
	const char *tbl = *getArgReference_str(stk, pci, 2);
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
	s = mvc_bind_schema(m, sch);
	if (s == NULL)
		throw(SQL, "sql.drop_hash", SQLSTATE(3F000) "Schema missing %s",sch);
	if (!mvc_schema_privs(m, s))
		throw(SQL, "sql.drop_hash", SQLSTATE(42000) "Access denied for %s to schema '%s'", stack_get_string(m, "current_user"), s->base.name);
	t = mvc_bind_table(m, s, tbl);
	if (t == NULL)
		throw(SQL, "sql.drop_hash", SQLSTATE(42S02) "Table missing %s.%s",sch, tbl);

	for (o = t->columns.set->h; o; o = o->next) {
		c = o->data;
		b = store_funcs.bind_col(m->session->tr, c, RDONLY);
		if (b == NULL)
			throw(SQL, "sql.drop_hash", SQLSTATE(HY005) "Cannot access column descriptor");
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
	throw(SQL, "updateOptimizer", SQLSTATE(0A000) PROGRAM_NYI);
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
	BAT *sch, *tab, *col, *type, *loc, *cnt, *atom, *size, *heap, *indices, *phash, *sort, *imprints, *mode, *revsort, *key, *oidx;
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
	bat *rrevsort = getArgReference_bat(stk, pci, 14);
	bat *rkey = getArgReference_bat(stk, pci, 15);
	bat *roidx = getArgReference_bat(stk, pci, 16);
	str sname = 0;
	str tname = 0;
	str cname = 0;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	tr = m->session->tr;
	sch = COLnew(0, TYPE_str, 0, TRANSIENT);
	tab = COLnew(0, TYPE_str, 0, TRANSIENT);
	col = COLnew(0, TYPE_str, 0, TRANSIENT);
	type = COLnew(0, TYPE_str, 0, TRANSIENT);
	mode = COLnew(0, TYPE_str, 0, TRANSIENT);
	loc = COLnew(0, TYPE_str, 0, TRANSIENT);
	cnt = COLnew(0, TYPE_lng, 0, TRANSIENT);
	atom = COLnew(0, TYPE_int, 0, TRANSIENT);
	size = COLnew(0, TYPE_lng, 0, TRANSIENT);
	heap = COLnew(0, TYPE_lng, 0, TRANSIENT);
	indices = COLnew(0, TYPE_lng, 0, TRANSIENT);
	phash = COLnew(0, TYPE_bit, 0, TRANSIENT);
	imprints = COLnew(0, TYPE_lng, 0, TRANSIENT);
	sort = COLnew(0, TYPE_bit, 0, TRANSIENT);
	revsort = COLnew(0, TYPE_bit, 0, TRANSIENT);
	key = COLnew(0, TYPE_bit, 0, TRANSIENT);
	oidx = COLnew(0, TYPE_lng, 0, TRANSIENT);

	if (sch == NULL || tab == NULL || col == NULL || type == NULL || mode == NULL || loc == NULL || imprints == NULL ||
	    sort == NULL || cnt == NULL || atom == NULL || size == NULL || heap == NULL || indices == NULL || phash == NULL ||
	    revsort == NULL || key == NULL || oidx == NULL) {
		goto bailout;
	}
	if( pci->argc - pci->retc >= 1)
		sname = *getArgReference_str(stk, pci, pci->retc);
	if( pci->argc - pci->retc >= 2)
		tname = *getArgReference_str(stk, pci, pci->retc + 1);
	if( pci->argc - pci->retc >= 3)
		cname = *getArgReference_str(stk, pci, pci->retc + 2);

	/* check for limited storage tables */
	for (nsch = tr->schemas.set->h; nsch; nsch = nsch->next) {
		sql_base *b = nsch->data;
		sql_schema *s = (sql_schema *) nsch->data;
		if( sname && strcmp(b->name, sname) )
			continue;
		if (isalpha((unsigned char) b->name[0]))
			if (s->tables.set)
				for (ntab = (s)->tables.set->h; ntab; ntab = ntab->next) {
					sql_base *bt = ntab->data;
					sql_table *t = (sql_table *) bt;
					if( tname && strcmp(bt->name, tname) )
						continue;
					if (isTable(t))
						if (t->columns.set)
							for (ncol = (t)->columns.set->h; ncol; ncol = ncol->next) {
								sql_base *bc = ncol->data;
								sql_column *c = (sql_column *) ncol->data;
								BAT *bn;
								lng sz;

								if( cname && strcmp(bc->name, cname) )
									continue;
								bn = store_funcs.bind_col(tr, c, RDONLY);
								if (bn == NULL)
									throw(SQL, "sql.storage", SQLSTATE(HY005) "Cannot access column descriptor");

								/*printf("schema %s.%s.%s" , b->name, bt->name, bc->name); */
								if (BUNappend(sch, b->name, false) != GDK_SUCCEED ||
								    BUNappend(tab, bt->name, false) != GDK_SUCCEED ||
								    BUNappend(col, bc->name, false) != GDK_SUCCEED)
									goto bailout;
								if (c->t->access == TABLE_WRITABLE) {
									if (BUNappend(mode, "writable", false) != GDK_SUCCEED)
										goto bailout;
								} else if (c->t->access == TABLE_APPENDONLY) {
									if (BUNappend(mode, "appendonly", false) != GDK_SUCCEED)
										goto bailout;
								} else if (c->t->access == TABLE_READONLY) {
									if (BUNappend(mode, "readonly", false) != GDK_SUCCEED)
										goto bailout;
								} else {
									if (BUNappend(mode, str_nil, false) != GDK_SUCCEED)
										goto bailout;
								}
								if (BUNappend(type, c->type.type->sqlname, false) != GDK_SUCCEED)
									goto bailout;

								/*printf(" cnt "BUNFMT, BATcount(bn)); */
								sz = BATcount(bn);
								if (BUNappend(cnt, &sz, false) != GDK_SUCCEED)
									goto bailout;

								/*printf(" loc %s", BBP_physical(bn->batCacheid)); */
								if (BUNappend(loc, BBP_physical(bn->batCacheid), false) != GDK_SUCCEED)
									goto bailout;
								/*printf(" width %d", bn->twidth); */
								w = bn->twidth;
								if (bn->ttype == TYPE_str) {
									BUN p, q;
									double sum = 0;
									BATiter bi = bat_iterator(bn);
									lng cnt1, cnt2 = cnt1 = (lng) BATcount(bn);

									/* just take a sample */
									if (cnt1 > 512)
										cnt1 = cnt2 = 512;
									BATloop(bn, p, q) {
										str s = BUNtvar(bi, p);
										if (!strNil(s))
											sum += strlen(s);
										if (--cnt1 <= 0)
											break;
									}
									if (cnt2)
										w = (int) (sum / cnt2);
								} else if (ATOMvarsized(bn->ttype)) {
									sz = BATcount(bn);
									if (sz > 0)
										w = (int) ((bn->tvheap->free + sz / 2) / sz);
									else
										w = 0;
								}
								if (BUNappend(atom, &w, false) != GDK_SUCCEED)
									goto bailout;

								sz = BATcount(bn) * bn->twidth;
								if (BUNappend(size, &sz, false) != GDK_SUCCEED)
									goto bailout;

								sz = heapinfo(bn->tvheap, bn->batCacheid);
								if (BUNappend(heap, &sz, false) != GDK_SUCCEED)
									goto bailout;

								sz = hashinfo(bn->thash, bn->batCacheid);
								if (BUNappend(indices, &sz, false) != GDK_SUCCEED)
									goto bailout;

								bitval = 0; /* HASHispersistent(bn); */
								if (BUNappend(phash, &bitval, false) != GDK_SUCCEED)
									goto bailout;

								sz = IMPSimprintsize(bn);
								if (BUNappend(imprints, &sz, false) != GDK_SUCCEED)
									goto bailout;
								/*printf(" indices "BUNFMT, bn->thash?bn->thash->heap.size:0); */
								/*printf("\n"); */

								bitval = BATtordered(bn);
								if (!bitval && bn->tnosorted == 0)
									bitval = bit_nil;
								if (BUNappend(sort, &bitval, false) != GDK_SUCCEED)
									goto bailout;

								bitval = BATtrevordered(bn);
								if (!bitval && bn->tnorevsorted == 0)
									bitval = bit_nil;
								if (BUNappend(revsort, &bitval, false) != GDK_SUCCEED)
									goto bailout;

								bitval = BATtkey(bn);
								if (!bitval && bn->tnokey[0] == 0 && bn->tnokey[1] == 0)
									bitval = bit_nil;
								if (BUNappend(key, &bitval, false) != GDK_SUCCEED)
									goto bailout;

								sz = bn->torderidx && bn->torderidx != (Heap *) 1 ? bn->torderidx->free : 0;
								if (BUNappend(oidx, &sz, false) != GDK_SUCCEED)
									goto bailout;
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
										throw(SQL, "sql.storage", SQLSTATE(HY005) "Cannot access column descriptor");
									if( cname && strcmp(bc->name, cname) )
										continue;
									/*printf("schema %s.%s.%s" , b->name, bt->name, bc->name); */
									if (BUNappend(sch, b->name, false) != GDK_SUCCEED ||
									    BUNappend(tab, bt->name, false) != GDK_SUCCEED ||
									    BUNappend(col, bc->name, false) != GDK_SUCCEED)
										goto bailout;
									if (c->t->access == TABLE_WRITABLE) {
										if (BUNappend(mode, "writable", false) != GDK_SUCCEED)
											goto bailout;
									} else if (c->t->access == TABLE_APPENDONLY) {
										if (BUNappend(mode, "appendonly", false) != GDK_SUCCEED)
											goto bailout;
									} else if (c->t->access == TABLE_READONLY) {
										if (BUNappend(mode, "readonly", false) != GDK_SUCCEED)
											goto bailout;
									} else {
										if (BUNappend(mode, str_nil, false) != GDK_SUCCEED)
											goto bailout;
									}
									if (BUNappend(type, "oid", false) != GDK_SUCCEED)
										goto bailout;

									/*printf(" cnt "BUNFMT, BATcount(bn)); */
									sz = BATcount(bn);
									if (BUNappend(cnt, &sz, false) != GDK_SUCCEED)
										goto bailout;

									/*printf(" loc %s", BBP_physical(bn->batCacheid)); */
									if (BUNappend(loc, BBP_physical(bn->batCacheid), false) != GDK_SUCCEED)
										goto bailout;
									/*printf(" width %d", bn->twidth); */
									w = bn->twidth;
									if (bn->ttype == TYPE_str) {
										BUN p, q;
										double sum = 0;
										BATiter bi = bat_iterator(bn);
										lng cnt1, cnt2 = cnt1 = BATcount(bn);

										/* just take a sample */
										if (cnt1 > 512)
											cnt1 = cnt2 = 512;
										BATloop(bn, p, q) {
											str s = BUNtvar(bi, p);
											if (!strNil(s))
												sum += strlen(s);
											if (--cnt1 <= 0)
												break;
										}
										if (cnt2)
											w = (int) (sum / cnt2);
									}
									if (BUNappend(atom, &w, false) != GDK_SUCCEED)
										goto bailout;
									/*printf(" size "BUNFMT, tailsize(bn,BATcount(bn)) + (bn->tvheap? bn->tvheap->size:0)); */
									sz = tailsize(bn, BATcount(bn));
									if (BUNappend(size, &sz, false) != GDK_SUCCEED)
										goto bailout;

									sz = bn->tvheap ? bn->tvheap->size : 0;
									if (BUNappend(heap, &sz, false) != GDK_SUCCEED)
										goto bailout;

									sz = bn->thash && bn->thash != (Hash *) 1 ? bn->thash->heaplink.size + bn->thash->heapbckt.size : 0; /* HASHsize() */
									if (BUNappend(indices, &sz, false) != GDK_SUCCEED)
										goto bailout;
									bitval = 0; /* HASHispersistent(bn); */
									if (BUNappend(phash, &bitval, false) != GDK_SUCCEED)
										goto bailout;

									sz = IMPSimprintsize(bn);
									if (BUNappend(imprints, &sz, false) != GDK_SUCCEED)
										goto bailout;
									/*printf(" indices "BUNFMT, bn->thash?bn->thash->heaplink.size+bn->thash->heapbckt.size:0); */
									/*printf("\n"); */
									bitval = BATtordered(bn);
									if (!bitval && bn->tnosorted == 0)
										bitval = bit_nil;
									if (BUNappend(sort, &bitval, false) != GDK_SUCCEED)
										goto bailout;
									bitval = BATtrevordered(bn);
									if (!bitval && bn->tnorevsorted == 0)
										bitval = bit_nil;
									if (BUNappend(revsort, &bitval, false) != GDK_SUCCEED)
										goto bailout;
									bitval = BATtkey(bn);
									if (!bitval && bn->tnokey[0] == 0 && bn->tnokey[1] == 0)
										bitval = bit_nil;
									if (BUNappend(key, &bitval, false) != GDK_SUCCEED)
										goto bailout;
									sz = bn->torderidx && bn->torderidx != (Heap *) 1 ? bn->torderidx->free : 0;
									if (BUNappend(oidx, &sz, false) != GDK_SUCCEED)
										goto bailout;
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
	BBPkeepref(*rrevsort = revsort->batCacheid);
	BBPkeepref(*rkey = key->batCacheid);
	BBPkeepref(*roidx = oidx->batCacheid);
	return MAL_SUCCEED;

  bailout:
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
	if (revsort)
		BBPunfix(revsort->batCacheid);
	if (key)
		BBPunfix(key->batCacheid);
	if (oidx)
		BBPunfix(oidx->batCacheid);
	throw(SQL, "sql.storage", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
		throw(SQL, "calc.index", SQLSTATE(HY005) "Cannot access column descriptor");

	if (*u) {
		Heap *h = s->tvheap;
		size_t pad, pos;
		const size_t extralen = h->hashash ? EXTRALEN : 0;
		int v;

		r = COLnew(0, TYPE_int, 1024, TRANSIENT);
		if (r == NULL) {
			BBPunfix(s->batCacheid);
			throw(SQL, "calc.index", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		pos = GDK_STRHASHSIZE;
		while (pos < h->free) {
			const char *p;

			pad = GDK_VARALIGN - (pos & (GDK_VARALIGN - 1));
			if (pad < sizeof(stridx_t))
				pad += GDK_VARALIGN;
			pos += pad + extralen;
			p = h->base + pos;
			v = (int) (pos - GDK_STRHASHSIZE);
			if (BUNappend(r, &v, false) != GDK_SUCCEED) {
				BBPreclaim(r);
				BBPunfix(s->batCacheid);
				throw(SQL, "calc.index", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			pos += strLen(p);
		}
	} else {
		r = VIEWcreate(s->hseqbase, s);
		if (r == NULL) {
			BBPunfix(s->batCacheid);
			throw(SQL, "calc.index", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		r->ttype = TYPE_int;
		r->tvarsized = false;
		r->tvheap = NULL;
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
		throw(SQL, "calc.index", SQLSTATE(HY005) "Cannot access column descriptor");

	if (*u) {
		Heap *h = s->tvheap;
		size_t pad, pos;
		const size_t extralen = h->hashash ? EXTRALEN : 0;
		sht v;

		r = COLnew(0, TYPE_sht, 1024, TRANSIENT);
		if (r == NULL) {
			BBPunfix(s->batCacheid);
			throw(SQL, "calc.index", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		pos = GDK_STRHASHSIZE;
		while (pos < h->free) {
			const char *s;

			pad = GDK_VARALIGN - (pos & (GDK_VARALIGN - 1));
			if (pad < sizeof(stridx_t))
				pad += GDK_VARALIGN;
			pos += pad + extralen;
			s = h->base + pos;
			v = (sht) (pos - GDK_STRHASHSIZE);
			if (BUNappend(r, &v, false) != GDK_SUCCEED) {
				BBPreclaim(r);
				throw(SQL, "calc.index", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			pos += strLen(s);
		}
	} else {
		r = VIEWcreate(s->hseqbase, s);
		if (r == NULL) {
			BBPunfix(s->batCacheid);
			throw(SQL, "calc.index", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		r->ttype = TYPE_sht;
		r->tvarsized = false;
		r->tvheap = NULL;
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
		throw(SQL, "calc.index", SQLSTATE(HY005) "Cannot access column descriptor");

	if (*u) {
		Heap *h = s->tvheap;
		size_t pad, pos;
		const size_t extralen = h->hashash ? EXTRALEN : 0;
		bte v;

		r = COLnew(0, TYPE_bte, 64, TRANSIENT);
		if (r == NULL) {
			BBPunfix(s->batCacheid);
			throw(SQL, "calc.index", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		pos = GDK_STRHASHSIZE;
		while (pos < h->free) {
			const char *p;

			pad = GDK_VARALIGN - (pos & (GDK_VARALIGN - 1));
			if (pad < sizeof(stridx_t))
				pad += GDK_VARALIGN;
			pos += pad + extralen;
			p = h->base + pos;
			v = (bte) (pos - GDK_STRHASHSIZE);
			if (BUNappend(r, &v, false) != GDK_SUCCEED) {
				BBPreclaim(r);
				BBPunfix(s->batCacheid);
				throw(SQL, "calc.index", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			pos += strLen(p);
		}
	} else {
		r = VIEWcreate(s->hseqbase, s);
		if (r == NULL) {
			BBPunfix(s->batCacheid);
			throw(SQL, "calc.index", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		r->ttype = TYPE_bte;
		r->tvarsized = false;
		r->tvheap = NULL;
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
		throw(SQL, "calc.strings", SQLSTATE(HY005) "Cannot access column descriptor");

	h = s->tvheap;
	extralen = h->hashash ? EXTRALEN : 0;
	r = COLnew(0, TYPE_str, 1024, TRANSIENT);
	if (r == NULL) {
		BBPunfix(s->batCacheid);
		throw(SQL, "calc.strings", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	pos = GDK_STRHASHSIZE;
	while (pos < h->free) {
		const char *p;

		pad = GDK_VARALIGN - (pos & (GDK_VARALIGN - 1));
		if (pad < sizeof(stridx_t))
			pad += GDK_VARALIGN;
		pos += pad + extralen;
		p = h->base + pos;
		if (BUNappend(r, p, false) != GDK_SUCCEED) {
			BBPreclaim(r);
			BBPunfix(s->batCacheid);
			throw(SQL, "calc.strings", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		pos += strLen(p);
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

str
SQLresume_log_flushing(void *ret)
{
	(void)ret;
	store_resume_log();
	return MAL_SUCCEED;
}

str
SQLsuspend_log_flushing(void *ret)
{
	(void)ret;
	store_suspend_log();
	return MAL_SUCCEED;
}

str
SQLhot_snapshot(void *ret, const str *tarfile_arg)
{
	(void)ret;
	char *tarfile = *tarfile_arg;
	lng result = store_hot_snapshot(tarfile);
	if (result)
		return MAL_SUCCEED;
	else
		throw(SQL, "sql.hot_snapshot", GDK_EXCEPTION);
}

str
SQLsession_prepared_statements(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *sessionid, *user, *statementid, *statement, *created;
	bat *sid = getArgReference_bat(stk,pci,0);
	bat *u = getArgReference_bat(stk,pci,1);
	bat *i = getArgReference_bat(stk,pci,2);
	bat *s = getArgReference_bat(stk,pci,3);
	bat *c = getArgReference_bat(stk,pci,4);
	str msg = MAL_SUCCEED, usr;
	mvc *sql = NULL;
	cq *q = NULL;

	(void) stk;
	(void) pci;
	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	assert(sql->qc);

	sessionid = COLnew(0, TYPE_int, 256, TRANSIENT);
	user = COLnew(0, TYPE_str, 256, TRANSIENT);
	statementid = COLnew(0, TYPE_int, 256, TRANSIENT);
	statement = COLnew(0, TYPE_str, 256, TRANSIENT);
	created = COLnew(0, TYPE_timestamp, 256, TRANSIENT);
	if (sessionid == NULL || user == NULL || statementid == NULL || statement == NULL || created == NULL) {
		msg = createException(SQL, "sql.session_prepared_statements", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	for (q = sql->qc->q; q; q = q->next) {
		if (q->prepared) {
			gdk_return bun_res;
			if (BUNappend(sessionid, &(cntxt->idx), false) != GDK_SUCCEED) {
				msg = createException(SQL, "sql.session_prepared_statements", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}

			msg = AUTHgetUsername(&usr, cntxt);
			if (msg != MAL_SUCCEED)
				goto bailout;
			bun_res = BUNappend(user, usr, false);
			GDKfree(usr);
			if (bun_res != GDK_SUCCEED) {
				msg = createException(SQL, "sql.session_prepared_statements", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}

			if (BUNappend(statementid, &(q->id), false) != GDK_SUCCEED) {
				msg = createException(SQL, "sql.session_prepared_statements", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			if (BUNappend(statement, q->codestring, false) != GDK_SUCCEED) {
				msg = createException(SQL, "sql.session_prepared_statements", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			if (BUNappend(created, &(q->created), false) != GDK_SUCCEED) {
				msg = createException(SQL, "sql.session_prepared_statements", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	if (msg) {
		BBPreclaim(sessionid);
		BBPreclaim(user);
		BBPreclaim(statementid);
		BBPreclaim(statement);
		BBPreclaim(created);
	} else {
		BBPkeepref(*sid = sessionid->batCacheid);
		BBPkeepref(*u = user->batCacheid);
		BBPkeepref(*i = statementid->batCacheid);
		BBPkeepref(*s = statement->batCacheid);
		BBPkeepref(*c = created->batCacheid);
	}
	return msg;
}

str
SQLsession_prepared_statements_args(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *statementid, *type, *digits, *isinout, *number, *scale, *schema, *table, *column;
	bat *sid = getArgReference_bat(stk,pci,0);
	bat *t = getArgReference_bat(stk,pci,1);
	bat *d = getArgReference_bat(stk,pci,2);
	bat *s = getArgReference_bat(stk,pci,3);
	bat *io = getArgReference_bat(stk,pci,4);
	bat *n = getArgReference_bat(stk,pci,5);
	bat *sch = getArgReference_bat(stk,pci,6);
	bat *tbl = getArgReference_bat(stk,pci,7);
	bat *col = getArgReference_bat(stk,pci,8);
	str msg = MAL_SUCCEED;
	mvc *sql = NULL;
	cq *q = NULL;

	(void) stk;
	(void) pci;
	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	assert(sql->qc);

	statementid = COLnew(0, TYPE_int, 256, TRANSIENT);
	type = COLnew(0, TYPE_str, 256, TRANSIENT);
	digits = COLnew(0, TYPE_int, 256, TRANSIENT);
	scale = COLnew(0, TYPE_int, 256, TRANSIENT);
	isinout = COLnew(0, TYPE_bte, 256, TRANSIENT);
	number = COLnew(0, TYPE_int, 256, TRANSIENT);
	schema = COLnew(0, TYPE_str, 256, TRANSIENT);
	table = COLnew(0, TYPE_str, 256, TRANSIENT);
	column = COLnew(0, TYPE_str, 256, TRANSIENT);
	if (!statementid || !type || !digits || !scale || !isinout || !number || !schema || !table || !column) {
		msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	for (q = sql->qc->q; q; q = q->next) {
		if (q->prepared) {
			sql_rel *r = q->rel;
			int arg_number = 0;
			bte inout = ARG_OUT;

			if (r && (is_topn(r->op) || is_sample(r->op)))
				r = r->l;

			if (r && is_project(r->op) && r->exps) {
				for (node *n = r->exps->h; n; n = n->next, arg_number++) {
					sql_exp *e = n->data;
					sql_subtype *t = exp_subtype(e);
					const char *name = exp_name(e), *rname = exp_relname(e), *rschema = ATOMnilptr(TYPE_str);

					if (!name && e->type == e_column && e->r)
						name = e->r;
					if (!name)
						name = ATOMnilptr(TYPE_str);
					if (!rname && e->type == e_column && e->l)
						rname = e->l;
					if (!rname)
						rname = ATOMnilptr(TYPE_str);

					if (BUNappend(statementid, &(q->id), false) != GDK_SUCCEED) {
						msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
					if (BUNappend(type, t->type->sqlname, false) != GDK_SUCCEED) {
						msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
					if (BUNappend(digits, &t->digits, false) != GDK_SUCCEED) {
						msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
					if (BUNappend(scale, &t->scale, false) != GDK_SUCCEED) {
						msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
					if (BUNappend(isinout, &inout, false) != GDK_SUCCEED) {
						msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
					if (BUNappend(number, &arg_number, false) != GDK_SUCCEED) {
						msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
					if (BUNappend(schema, rschema, false) != GDK_SUCCEED) {
						msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
					if (BUNappend(table, rname, false) != GDK_SUCCEED) {
						msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
					if (BUNappend(column, name, false) != GDK_SUCCEED) {
						msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
				}
			}

			if (q->params) {
				inout = ARG_IN;
				for (int i = 0; i < q->paramlen; i++, arg_number++) {
					sql_subtype t = q->params[i];

					if (BUNappend(statementid, &(q->id), false) != GDK_SUCCEED) {
						msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
					if (BUNappend(type, t.type->sqlname, false) != GDK_SUCCEED) {
						msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
					if (BUNappend(digits, &(t.digits), false) != GDK_SUCCEED) {
						msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
					if (BUNappend(scale, &(t.scale), false) != GDK_SUCCEED) {
						msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
					if (BUNappend(isinout, &inout, false) != GDK_SUCCEED) {
						msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
					if (BUNappend(number, &arg_number, false) != GDK_SUCCEED) {
						msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
					if (BUNappend(schema, ATOMnilptr(TYPE_str), false) != GDK_SUCCEED) {
						msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
					if (BUNappend(table, ATOMnilptr(TYPE_str), false) != GDK_SUCCEED) {
						msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
					if (BUNappend(column, ATOMnilptr(TYPE_str), false) != GDK_SUCCEED) {
						msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
				}
			}
		}
	}

bailout:
	if (msg) {
		BBPreclaim(statementid);
		BBPreclaim(type);
		BBPreclaim(digits);
		BBPreclaim(scale);
		BBPreclaim(isinout);
		BBPreclaim(number);
		BBPreclaim(schema);
		BBPreclaim(table);
		BBPreclaim(column);
	} else {
		BBPkeepref(*sid = statementid->batCacheid);
		BBPkeepref(*t = type->batCacheid);
		BBPkeepref(*d = digits->batCacheid);
		BBPkeepref(*s = scale->batCacheid);
		BBPkeepref(*io = isinout->batCacheid);
		BBPkeepref(*n = number->batCacheid);
		BBPkeepref(*sch = schema->batCacheid);
		BBPkeepref(*tbl = table->batCacheid);
		BBPkeepref(*col = column->batCacheid);
	}
	return msg;
}

/* input id,row-input-values
 * for each id call function(with row-input-values) return table
 * return for each id the table, ie id (*length of table) and table results
 */
str
SQLunionfunc(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int arg = pci->retc;
	str mod, fcn, ret = MAL_SUCCEED;
	InstrPtr npci;

	mod = *getArgReference_str(stk, pci, arg++);
	fcn = *getArgReference_str(stk, pci, arg++);
	npci = newStmt(mb, mod, fcn);

	for (int i = 1; i < pci->retc; i++) {
		int type = getArgType(mb, pci, i);

		if (i==1)
			getArg(npci, 0) = newTmpVariable(mb, type);
		else
			npci = pushReturn(mb, npci, newTmpVariable(mb, type));
	}
	for (int i = pci->retc+2+1; i < pci->argc; i++) {
		int type = getBatType(getArgType(mb, pci, i));

		npci = pushNil(mb, npci, type);
	}
	/* check program to get the proper malblk */
	if (chkInstruction(cntxt->usermodule, mb, npci)) {
		freeInstruction(npci);
		return createException(MAL, "sql.unionfunc", SQLSTATE(42000) PROGRAM_GENERAL);
	}

	if (npci) {
		BAT **res = NULL, **input = NULL;
		BATiter *bi = NULL;
		BUN cnt = 0;
		int nrinput = pci->argc - 2 - pci->retc;
		MalBlkPtr nmb = NULL;
		MalStkPtr env = NULL;
		InstrPtr q = NULL;

		if (!(input = GDKzalloc(sizeof(BAT*) * nrinput))) {
			ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto finalize;
		}
		if (!(bi = GDKmalloc(sizeof(BATiter) * nrinput))) {
			ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto finalize;
		}
		assert(nrinput == pci->retc);
		for (int i = 0, j = pci->retc+2; j < pci->argc; i++, j++) {
			bat *b = getArgReference_bat(stk, pci, j);
			if (!(input[i] = BATdescriptor(*b))) {
				ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY005) "Cannot access column descriptor");
				goto finalize;
			}
			bi[i] = bat_iterator(input[i]);
			cnt = BATcount(input[i]);
		}

		/* create result bats */
		if (!(res = GDKzalloc(sizeof(BAT*) * pci->retc))) {
			ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto finalize;
		}
		for (int i = 0; i<pci->retc; i++) {
			int type = getArgType(mb, pci, i);

			if (!(res[i] = COLnew(0, getBatType(type), cnt, TRANSIENT))) {
				ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto finalize;
			}
		}

		if (!(nmb = copyMalBlk(npci->blk))) {
			ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto finalize;
		}
		if (!(env = prepareMALstack(nmb, nmb->vsize))) { /* needed for result */
			ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto finalize;
		}

		q = getInstrPtr(nmb, 0);

		for (BUN cur = 0; cur<cnt && !ret; cur++ ) {
			MalStkPtr nstk = prepareMALstack(nmb, nmb->vsize);
			int i,ii;

			if (!nstk) { /* needed for result */
				ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			} else {
				/* copy (input) arguments onto destination stack, skipping rowid col */
				for (i = 1, ii = q->retc; ii < q->argc && !ret; ii++) {
					ValPtr lhs = &nstk->stk[q->argv[ii]];
					ptr rhs = (ptr)BUNtail(bi[i], cur);

					assert(lhs->vtype != TYPE_bat);
					if (VALset(lhs, input[i]->ttype, rhs) == NULL)
						ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
				if (!ret && ii == q->argc) {
					BAT *fres = NULL;
					ret = runMALsequence(cntxt, nmb, 1, nmb->stop, nstk, env /* copy result in nstk first instruction*/, q);

					if (!ret) {
						/* insert into result */
						if (!(fres = BATdescriptor(env->stk[q->argv[0]].val.bval)))
							ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY005) "Cannot access column descriptor");
						else {
							BAT *p = BATconstant(fres->hseqbase, res[0]->ttype, (ptr)BUNtail(bi[0], cur), BATcount(fres), TRANSIENT);

							if (p) {
								if (BATappend(res[0], p, NULL, FALSE) != GDK_SUCCEED)
									ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY013) MAL_MALLOC_FAIL);
								BBPunfix(p->batCacheid);
							} else {
								ret = createException(MAL, "sql.unionfunc", OPERATION_FAILED);
							}
							BBPunfix(fres->batCacheid);
						}
						i=1;
						for (ii = 0; i < pci->retc && !ret; i++) {
							BAT *b;

							if (!(b = BATdescriptor(env->stk[q->argv[ii]].val.bval)))
								ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY005) "Cannot access column descriptor");
							else if (BATappend(res[i], b, NULL, FALSE) != GDK_SUCCEED)
								ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY013) MAL_MALLOC_FAIL);
							if (b) {
								BBPrelease(b->batCacheid); /* release ref from env stack */
								BBPunfix(b->batCacheid);   /* free pointer */
							}
						}
					}
				}
				GDKfree(nstk);
			}
		}
finalize:
		GDKfree(env);
		if (nmb)
			freeMalBlk(nmb);
		if (res)
			for (int i = 0; i<pci->retc; i++) {
				bat *b = getArgReference_bat(stk, pci, i);
				if (res[i]) {
					*b = res[i]->batCacheid;
					if (ret)
						BBPunfix(*b);
					else
						BBPkeepref(*b);
				}
			}
		GDKfree(res);
		if (input)
			for (int i = 0; i<nrinput; i++) {
				if (input[i])
					BBPunfix(input[i]->batCacheid);
			}
		GDKfree(input);
		GDKfree(bi);
	}
	return ret;
}
