/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
#include "sql_storage.h"
#include "sql_scenario.h"
#include "store_sequence.h"
#include "sql_optimizer.h"
#include "sql_datetime.h"
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
	//if (is_topn(rel->op) || is_project(rel->op))
	if (is_topn(rel->op) || rel->op == op_project)
		return rel_no_mitosis(rel->l);
	if (is_modify(rel->op) && rel->card <= CARD_AGGR)
		return rel_no_mitosis(rel->r);
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
		r = rel_partition(c, r);
		if (rel_no_mitosis(r) || rel_need_distinct_query(r))
			c->no_mitosis = 1;
	}
	return r;
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
	c->sqs = NULL;

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

	if (cntxt == NULL)
		throw(SQL, "mvc", SQLSTATE(42005) "No client record");
	if (cntxt->sqlcontext == NULL)
		throw(SQL, "mvc", SQLSTATE(42006) "SQL module not initialized");
	be = (backend *) cntxt->sqlcontext;
	if (be->mvc == NULL)
		throw(SQL, "mvc", SQLSTATE(42006) "SQL module not initialized, mvc struct missing");
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
		throw(SQL, "sql.trans", SQLSTATE(2DM30) "COMMIT not allowed in auto commit mode");
	ret = mvc_commit(sql, 0, 0);
	if (ret < 0) {
		throw(SQL, "sql.trans", SQLSTATE(2D000) "transaction commit failed");
	}
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
		mvc_rollback(sql, 0, NULL);
	}
	return msg;
}

str
SQLshutdown_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg;

	if ((msg = CLTshutdown(cntxt, mb, stk, pci)) == MAL_SUCCEED) {
		/* administer the shutdown */
		mnstr_printf(GDKstdout, "#%s\n", *getArgReference_str(stk, pci, 0));
	}
	return msg;
}

str
create_table_or_view(mvc *sql, char *sname, char *tname, sql_table *t, int temp)
{
	sql_allocator *osa;
	sql_schema *s = mvc_bind_schema(sql, sname);
	sql_table *nt = NULL;
	node *n;

	(void)tname;
	if (STORE_READONLY)
		return sql_error(sql, 06, "25006!schema statements cannot be executed on a readonly database.");

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
	/* first check default values */
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;

		if (c->def) {
			char *buf;
			sql_rel *r = NULL;

			sql->sa = sa_create();
			if(!sql->sa)
				throw(SQL, "sql.catalog",SQLSTATE(HY001) MAL_MALLOC_FAIL);
			buf = sa_alloc(sql->sa, strlen(c->def) + 8);
			if(!buf)
				throw(SQL, "sql.catalog",SQLSTATE(HY001) MAL_MALLOC_FAIL);
			snprintf(buf, BUFSIZ, "select %s;", c->def);
			r = rel_parse(sql, s, buf, m_deps);
			if (!r || !is_project(r->op) || !r->exps || list_length(r->exps) != 1 || rel_check_type(sql, &c->type, r->exps->h->data, type_equal) == NULL)
				throw(SQL, "sql.catalog", SQLSTATE(42000) "%s", sql->errstr);
			rel_destroy(r);
			sa_destroy(sql->sa);
			sql->sa = NULL;
		}
	}

	nt = sql_trans_create_table(sql->session->tr, s, t->base.name, t->query, t->type, t->system, temp, t->commit_action, t->sz);

	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		if (mvc_copy_column(sql, nt, c) == NULL)
			throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: %s_%s_%s conflicts", s->base.name, t->base.name, c->base.name);

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
		if(!sql->sa)
			throw(SQL, "sql.catalog",SQLSTATE(HY001) MAL_MALLOC_FAIL);
		r = rel_parse(sql, s, nt->query, m_deps);
		if (r)
			r = rel_optimizer(sql, r);
		if (r) {
			list *id_l = rel_dependencies(sql->sa, r);

			mvc_create_dependencies(sql, id_l, nt->base.id, VIEW_DEPENDENCY);
		}
		sa_destroy(sql->sa);
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
	sql->sa = sa_create();
	if(!sql->sa)
		throw(SQL, "sql.catalog",SQLSTATE(HY001) MAL_MALLOC_FAIL);

	if (!sname)
		sname = "sys";
	if (!(s = mvc_bind_schema(sql, sname))) {
		msg = sql_error(sql, 02, "3F000!CREATE TABLE: no such schema '%s'", sname);
		goto cleanup;
	}
	if (!(t = mvc_create_table(sql, s, tname, tt_table, 0, SQL_DECLARED_TABLE, CA_COMMIT, -1))) {
		msg = sql_error(sql, 02, "3F000!CREATE TABLE: could not create table '%s'", tname);
		goto cleanup;
	}

	for(i = 0; i < ncols; i++) {
		BAT *b = columns[i].b;
		sql_subtype *tpe = sql_bind_localtype(ATOMname(b->ttype));
		sql_column *col = NULL;

		if (!tpe) {
			msg = sql_error(sql, 02, "3F000!CREATE TABLE: could not find type for column");
			goto cleanup;
		}

		col = mvc_create_column(sql, t, columns[i].name, tpe);
		if (!col) {
			msg = sql_error(sql, 02, "3F000!CREATE TABLE: could not create column %s", columns[i].name);
			goto cleanup;
		}
	}
	msg = create_table_or_view(sql, sname, t->base.name, t, 0);
	if (msg != MAL_SUCCEED) {
		goto cleanup;
	}
	t = mvc_bind_table(sql, s, tname);
	if (!t) {
		msg = sql_error(sql, 02, "3F000!CREATE TABLE: could not bind table %s", tname);
		goto cleanup;
	}
	for(i = 0; i < ncols; i++) {
		BAT *b = columns[i].b;
		sql_column *col = NULL;

		col = mvc_bind_column(sql,t, columns[i].name);
		if (!col) {
			msg = sql_error(sql, 02, "3F000!CREATE TABLE: could not bind column %s", columns[i].name);
			goto cleanup;
		}
		msg = mvc_append_column(sql->session->tr, col, b);
		if (msg != MAL_SUCCEED) {
			goto cleanup;
		}
	}

  cleanup:
	sa_destroy(sql->sa);
	sql->sa = NULL;
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
	sql->sa = sa_create();

	if (!sname) 
		sname = "sys";
	if (!(s = mvc_bind_schema(sql, sname))) {
		msg = sql_error(sql, 02, "3F000!CREATE TABLE: no such schema '%s'", sname);
		goto cleanup;
	}
	t = mvc_bind_table(sql, s, tname);
	if (!t) {
		msg = sql_error(sql, 02, "3F000!CREATE TABLE: could not bind table %s", tname);
		goto cleanup;
	}
	for(i = 0; i < ncols; i++) {
		BAT *b = columns[i].b;
		sql_column *col = NULL;

		col = mvc_bind_column(sql,t, columns[i].name);
		if (!col) {
			msg = sql_error(sql, 02, "3F000!CREATE TABLE: could not bind column %s", columns[i].name);
			goto cleanup;
		}
		msg = mvc_append_column(sql->session->tr, col, b);
		if (msg != MAL_SUCCEED) {
			goto cleanup;
		}
	}

  cleanup:
	sa_destroy(sql->sa);
	sql->sa = NULL;
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
	ValRecord *src;
	char buf[BUFSIZ];

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
			if (!isOptimizerPipe(newopt) && strchr(newopt, (int) ';') == 0) {
				throw(SQL, "sql.setVariable", SQLSTATE(42100) "optimizer '%s' unknown", newopt);
			}
			snprintf(buf, BUFSIZ, "user_%d", cntxt->idx);
			if (!isOptimizerPipe(newopt) || strcmp(buf, newopt) == 0) {
				msg = addPipeDefinition(cntxt, buf, newopt);
				if (msg)
					return msg;
				if (stack_find_var(m, varname)) {
					if(!stack_set_string(m, varname, buf))
						throw(SQL, "sql.setVariable", SQLSTATE(HY001) MAL_MALLOC_FAIL);
				}
			} else if (stack_find_var(m, varname)) {
				if(!stack_set_string(m, varname, newopt))
					throw(SQL, "sql.setVariable", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
		}
		return MAL_SUCCEED;
	}
	src = &stk->stk[getArg(pci, 3)];
	if (stack_find_var(m, varname)) {
		lng sgn = val_get_number(src);
		if ((msg = sql_update_var(m, varname, src->val.sval, sgn)) != NULL) {
			snprintf(buf, BUFSIZ, "%s", msg);
			if (strlen(msg) > 6 && msg[5] == '!')
				return msg;
			_DELETE(msg);
			throw(SQL, "sql.setVariable", SQLSTATE(42100) "%s", buf);
		}
		if(!stack_set_var(m, varname, src))
			throw(SQL, "sql.setVariable", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	} else {
		snprintf(buf, BUFSIZ, "variable '%s' unknown", varname);
		throw(SQL, "sql.setVariable", SQLSTATE(42100) "%s", buf);
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
	a = stack_get_var(m, varname);
	if (!a) {
		char buf[BUFSIZ];
		snprintf(buf, BUFSIZ, "variable '%s' unknown", varname);
		throw(SQL, "sql.getVariable", SQLSTATE(42100) "%s", buf);
	}
	src = &a->data;
	dst = &stk->stk[getArg(pci, 0)];
	if (VALcopy(dst, src) == NULL)
		throw(MAL, "sql.getVariable", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		throw(SQL, "sql.variables", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	for (i = 0; i < m->topvars && !m->vars[i].frame; i++) {
		if (BUNappend(vars, m->vars[i].name, FALSE) != GDK_SUCCEED) {
			BBPreclaim(vars);
			throw(SQL, "sql.variables", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
	const char *sname = *getArgReference_str(stk, pci, 1);
	const char *seqname = *getArgReference_str(stk, pci, 2);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, sname);
	if (s) {
		sql_sequence *seq = find_sql_sequence(s, seqname);

		if (seq && seq_next_value(seq, res)) {
			m->last_id = *res;
			stack_set_number(m, "last_id", m->last_id);
			return MAL_SUCCEED;
		}
	}
	throw(SQL, "sql.next_value", SQLSTATE(42000) "Error in fetching next value");
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
	bat sid = *getArgReference_bat(stk, pci, 1);
	const char *seqname = *getArgReference_str(stk, pci, 2);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if ((b = BATdescriptor(sid)) == NULL)
		throw(SQL, "sql.next_value", SQLSTATE(HY005) "Cannot access column descriptor");

	r = COLnew(b->hseqbase, TYPE_lng, BATcount(b), TRANSIENT);
	if (!r) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.next_value", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	if (!BATcount(b)) {
		BBPunfix(b->batCacheid);
		BBPkeepref(r->batCacheid);
		*res = r->batCacheid;
		return MAL_SUCCEED;
	}

	bi = bat_iterator(b);
	BATloop(b, p, q) {
		str sname = BUNtail(bi, 0);
		lng l;

		if (!s || strcmp(s->base.name, sname) != 0) {
			if (sb)
				seqbulk_destroy(sb);
			s = mvc_bind_schema(m, sname);
			seq = NULL;
			if (!s || (seq = find_sql_sequence(s, seqname)) == NULL || !(sb = seqbulk_create(seq, BATcount(b)))) {
				BBPunfix(b->batCacheid);
				BBPunfix(r->batCacheid);
				throw(SQL, "sql.next_value", SQLSTATE(HY050) "Cannot find the sequence %s.%s", sname,seqname);
			}
		}
		if (!seqbulk_next_value(sb, &l)) {
			BBPunfix(b->batCacheid);
			BBPunfix(r->batCacheid);
			seqbulk_destroy(sb);
			throw(SQL, "sql.next_value", SQLSTATE(HY050) "Cannot generate next seuqnce value %s.%s", sname, seqname);
		}
		if (BUNappend(r, &l, FALSE) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPunfix(r->batCacheid);
			seqbulk_destroy(sb);
			throw(SQL, "sql.next_value", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
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
	lng *res = getArgReference_lng(stk, pci, 0);
	const char *sname = *getArgReference_str(stk, pci, 1);
	const char *seqname = *getArgReference_str(stk, pci, 2);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	s = mvc_bind_schema(m, sname);
	if (s) {
		sql_sequence *seq = find_sql_sequence(s, seqname);

		if (seq && seq_get_value(seq, res))
			return MAL_SUCCEED;
	}
	throw(SQL, "sql.get_value", SQLSTATE(HY050) "Failed to fetch sequence %s.%s", sname, seqname);
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
	const char *sname = *getArgReference_str(stk, pci, 1);
	const char *seqname = *getArgReference_str(stk, pci, 2);
	lng start = *getArgReference_lng(stk, pci, 3);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (is_lng_nil(start))
		throw(SQL, "sql.restart", SQLSTATE(HY050) "Cannot (re)start sequence %s.%s with NULL",sname,seqname);
	s = mvc_bind_schema(m, sname);
	if (s) {
		sql_sequence *seq = find_sql_sequence(s, seqname);

		if (seq) {
			*res = sql_trans_sequence_restart(m->session->tr, seq, start);
			return MAL_SUCCEED;
		}
	}
	throw(SQL, "sql.restart", SQLSTATE(HY050) "Sequence %s.%s not found", sname, seqname);
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
	if (b && b->ttype != coltype)
		throw(SQL,"sql.bind",SQLSTATE(42000) "Column type mismatch");
	if (b) {
		if (pci->argc == (8 + upd) && getArgType(mb, pci, 6 + upd) == TYPE_int) {
			BUN cnt = BATcount(b), psz;
			/* partitioned access */
			int part_nr = *getArgReference_int(stk, pci, 6 + upd);
			int nr_parts = *getArgReference_int(stk, pci, 7 + upd);

			if (access == 0) {
				psz = cnt ? (cnt / nr_parts) : 0;
				bn = BATslice(b, part_nr * psz, (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz));
				BAThseqbase(bn, part_nr * psz);
			} else {
				/* BAT b holds the UPD_ID bat */
				oid l, h;
				BAT *c = mvc_bind(m, sname, tname, cname, 0);
				if (c == NULL)
					throw(SQL,"sql.bind",SQLSTATE(HY005) "Cannot access the update column %s.%s.%s",
					sname,tname,cname);

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
			BAT *uv = mvc_bind(m, sname, tname, cname, RD_UPD_VAL);
			bat *uvl = getArgReference_bat(stk, pci, 1);

			if (uv == NULL)
				throw(SQL,"sql.bind",SQLSTATE(HY005) "Cannot access the update column %s.%s.%s",
					sname,tname,cname);
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
				if (ui == NULL)
					throw(SQL,"sql.bind",SQLSTATE(HY005) "Cannot access the insert column %s.%s.%s",
						sname, tname, cname);
				if (uv == NULL)
					throw(SQL,"sql.bind",SQLSTATE(HY005) "Cannot access the update column %s.%s.%s",
						sname, tname, cname);
				id = BATproject(b, ui);
				vl = BATproject(b, uv);
				bat_destroy(ui);
				bat_destroy(uv);
				if (id == NULL || vl == NULL) {
					bat_destroy(id);
					bat_destroy(vl);
					throw(SQL, "sql.bind", SQLSTATE(HY001) MAL_MALLOC_FAIL);
				}
				assert(BATcount(id) == BATcount(vl));
				BBPkeepref(*bid = id->batCacheid);
				BBPkeepref(*uvl = vl->batCacheid);
			} else {
				sql_schema *s = mvc_bind_schema(m, sname);
				sql_table *t = mvc_bind_table(m, s, tname);
				sql_column *c = mvc_bind_column(m, t, cname);

				*bid = e_bat(TYPE_oid);
				*uvl = e_bat(c->type.type->localtype);
			}
			BBPunfix(b->batCacheid);
		} else {
			BBPkeepref(*bid = b->batCacheid);
		}
		return MAL_SUCCEED;
	}
	if (sname && strcmp(sname, str_nil) != 0)
		throw(SQL, "sql.bind", SQLSTATE(42000) "unable to find %s.%s(%s)", sname, tname, cname);
	throw(SQL, "sql.bind", SQLSTATE(42000) "unable to find %s(%s)", tname, cname);
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
				BAThseqbase(bn, part_nr * psz);
			} else {
				/* BAT b holds the UPD_ID bat */
				oid l, h;
				BAT *c = mvc_bind_idxbat(m, sname, tname, iname, 0);
				if ( c == NULL)
					throw(SQL,"sql.bindidx",SQLSTATE(42000) "Cannot access index column %s.%s.%s",sname,tname,iname);
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
				if ( ui == NULL)
					throw(SQL,"sql.bindidx",SQLSTATE(42000) "Cannot access index column %s.%s.%s",sname,tname,iname);
				if ( uv == NULL)
					throw(SQL,"sql.bindidx",SQLSTATE(42000) "Cannot access index column %s.%s.%s",sname,tname,iname);
				id = BATproject(b, ui);
				vl = BATproject(b, uv);
				bat_destroy(ui);
				bat_destroy(uv);
				if (id == NULL || vl == NULL) {
					bat_destroy(id);
					bat_destroy(vl);
					throw(SQL, "sql.idxbind", SQLSTATE(HY001) MAL_MALLOC_FAIL);
				}
				assert(BATcount(id) == BATcount(vl));
				BBPkeepref(*bid = id->batCacheid);
				BBPkeepref(*uvl = vl->batCacheid);
			} else {
				sql_schema *s = mvc_bind_schema(m, sname);
				sql_idx *i = mvc_bind_idx(m, s, iname);

				*bid = e_bat(TYPE_oid);
				*uvl = e_bat((i->type==join_idx)?TYPE_oid:TYPE_lng);
			}
			BBPunfix(b->batCacheid);
		} else {
			BBPkeepref(*bid = b->batCacheid);
		}
		return MAL_SUCCEED;
	}
	if (sname)
		throw(SQL, "sql.idxbind", SQLSTATE(HY005) "Cannot access column descriptor %s for %s.%s", iname, sname, tname);
	throw(SQL, "sql.idxbind", SQLSTATE(HY005) "Connot access column descriptor %s for %s", iname, tname);
}

str mvc_append_column(sql_trans *t, sql_column *c, BAT *ins) {
	int res = store_funcs.append_col(t, c, ins, TYPE_bat);
	if (res != 0) {
		throw(SQL, "sql.append", SQLSTATE(42000) "Cannot append values");
	}
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
		throw(SQL, "sql.grow", "Cannot access descriptor");
	if (tpe > GDKatomcnt)
		tpe = TYPE_bat;
	if (tpe == TYPE_bat && (ins = BATdescriptor(*(int *) Ins)) == NULL)
		throw(SQL, "sql.append", "Cannot access descriptor");
	if (ins) {
		cnt = BATcount(ins);
		BBPunfix(ins->batCacheid);
	}
	if (BATcount(tid)) {
		(void)BATmax(tid, &v);
		v++;
	}
	for(;cnt>0; cnt--, v++) {
		if (BUNappend(tid, &v, FALSE) != GDK_SUCCEED) {
			BBPunfix(Tid);
			throw(SQL, "sql", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
	if( b && BATcount(b) > 4096 && b->batPersistence == PERSISTENT)
		BATmsync(b);
	if (cname[0] != '%' && (c = mvc_bind_column(m, t, cname)) != NULL) {
		store_funcs.append_col(m->session->tr, c, ins, tpe);
	} else if (cname[0] == '%') {
		sql_idx *i = mvc_bind_idx(m, s, cname + 1);
		if (i)
			store_funcs.append_idx(m->session->tr, i, ins, tpe);
	}
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
		throw(SQL, "sql.update", SQLSTATE(HY005) "Cannot access column descriotor %s.%s.%s",
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
	if( upd && BATcount(upd) > 4096 && upd->batPersistence == PERSISTENT)
		BATmsync(upd);
	if( tids && BATcount(tids) > 4096 && tids->batPersistence == PERSISTENT)
		BATmsync(tids);
	if (cname[0] != '%' && (c = mvc_bind_column(m, t, cname)) != NULL) {
		store_funcs.update_col(m->session->tr, c, tids, upd, TYPE_bat);
	} else if (cname[0] == '%') {
		sql_idx *i = mvc_bind_idx(m, s, cname + 1);
		if (i)
			store_funcs.update_idx(m->session->tr, i, tids, upd, TYPE_bat);
	}
	BBPunfix(tids->batCacheid);
	BBPunfix(upd->batCacheid);
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
		throw(SQL, "sql.clear_table", "3F000!Schema missing %s", sname);
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "sql.clear_table", "42S02!Table missing %s.%s", sname,tname);
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
	if (tpe == TYPE_bat && (b = BATdescriptor(*(int *) ins)) == NULL)
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
	if( b && BATcount(b) > 4096 && b->batPersistence == PERSISTENT)
		BATmsync(b);
	store_funcs.delete_tab(m->session->tr, t, b, tpe);
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

	if ((u_id = BBPquickdesc(*uid, 0)) == NULL)
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (ins && (i = BBPquickdesc(*ins, 0)) == NULL)
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	/* no updates, no inserts */
	if (BATcount(u_id) == 0 && (!i || BATcount(i) == 0)) {
		BBPretain(*result = *col);
		return MAL_SUCCEED;
	}

	if ((c = BBPquickdesc(*col, 0)) == NULL)
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	/* bat may change */
	if (i && BATcount(c) == 0 && BATcount(u_id) == 0) {
		BBPretain(*result = *ins);
		return MAL_SUCCEED;
	}

	c = BATdescriptor(*col);
	if (c == NULL)
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if ((res = COLcopy(c, c->ttype, TRUE, TRANSIENT)) == NULL) {
		BBPunfix(c->batCacheid);
		throw(MAL, "sql.delta", SQLSTATE(45002) "Cannot create copy of delta structure");
	}
	BBPunfix(c->batCacheid);

	if ((u_val = BATdescriptor(*uval)) == NULL) {
		BBPunfix(res->batCacheid);
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	u_id = BATdescriptor(*uid);
	assert(BATcount(u_id) == BATcount(u_val));
	if (BATcount(u_id) &&
	    BATreplace(res, u_id, u_val, TRUE) != GDK_SUCCEED) {
		BBPunfix(u_id->batCacheid);
		BBPunfix(u_val->batCacheid);
		BBPunfix(res->batCacheid);
		throw(MAL, "sql.delta", SQLSTATE(45002) "Cannot access delta structure");
	}
	BBPunfix(u_id->batCacheid);
	BBPunfix(u_val->batCacheid);

	if (i && BATcount(i)) {
		i = BATdescriptor(*ins);
		if (BATappend(res, i, NULL, TRUE) != GDK_SUCCEED) {
			BBPunfix(res->batCacheid);
			BBPunfix(i->batCacheid);
			throw(MAL, "sql.delta", SQLSTATE(45002) "Cannot access delta structuren");
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

	if ((u_id = BBPquickdesc(*uid, 0)) == NULL)
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (ins && (i = BBPquickdesc(*ins, 0)) == NULL)
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	/* no updates, no inserts */
	if (BATcount(u_id) == 0 && (!i || BATcount(i) == 0)) {
		BBPretain(*result = *col);
		return MAL_SUCCEED;
	}

	if ((c = BBPquickdesc(*col, 0)) == NULL)
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
		cminu = BATdiff(c, u_id, NULL, NULL, 0, BUN_NONE);
		if (!cminu) {
			BBPunfix(c->batCacheid);
			BBPunfix(u_id->batCacheid);
			throw(MAL, "sql.delta", SQLSTATE(HY001) MAL_MALLOC_FAIL " intermediate");
		}
		res = BATproject(cminu, c);
		BBPunfix(c->batCacheid);
		BBPunfix(cminu->batCacheid);
		cminu = NULL;
		if (!res) {
			BBPunfix(u_id->batCacheid);
			throw(MAL, "sql.delta", SQLSTATE(HY001) MAL_MALLOC_FAIL " intermediate" );
		}
		c = res;

		if ((u_val = BATdescriptor(*uval)) == NULL) {
			BBPunfix(c->batCacheid);
			BBPunfix(u_id->batCacheid);
			throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		u = BATproject(u_val, u_id);
		BBPunfix(u_val->batCacheid);
		BBPunfix(u_id->batCacheid);
		if (!u) {
			BBPunfix(c->batCacheid);
			throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		if (BATcount(u)) {	/* check selected updated values against candidates */
			BAT *c_ids = BATdescriptor(*cid);
			gdk_return rc;

			if (!c_ids) {
				BBPunfix(c->batCacheid);
				BBPunfix(u->batCacheid);
				throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			}
			rc = BATsemijoin(&cminu, NULL, u, c_ids, NULL, NULL, 0, BUN_NONE);
			BBPunfix(c_ids->batCacheid);
			if (rc != GDK_SUCCEED) {
				BBPunfix(c->batCacheid);
				BBPunfix(u->batCacheid);
				throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			}
		}
		ret = BATappend(res, u, cminu, TRUE);
		BBPunfix(u->batCacheid);
		if (cminu)
			BBPunfix(cminu->batCacheid);
		cminu = NULL;
		if (ret != GDK_SUCCEED) {
			BBPunfix(res->batCacheid);
			throw(MAL, "sql.delta", SQLSTATE(45000) "Internal error in delta processing");
		}

		ret = BATsort(&u, NULL, NULL, res, NULL, NULL, 0, 0);
		BBPunfix(res->batCacheid);
		if (ret != GDK_SUCCEED) {
			throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		res = u;
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
			cminu = BATdiff(i, u_id, NULL, NULL, 0, BUN_NONE);
			BBPunfix(u_id->batCacheid);
			if (!cminu) {
				BBPunfix(res->batCacheid);
				BBPunfix(i->batCacheid);
				throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			}
		}
		if (isVIEW(res)) {
			BAT *n = COLcopy(res, res->ttype, TRUE, TRANSIENT);
			BBPunfix(res->batCacheid);
			res = n;
			if (res == NULL) {
				BBPunfix(i->batCacheid);
				if (cminu)
					BBPunfix(cminu->batCacheid);
				throw(MAL, "sql.delta", SQLSTATE(45000) "Internal error in delta processing");
			}
		}
		ret = BATappend(res, i, cminu, TRUE);
		BBPunfix(i->batCacheid);
		if (cminu)
			BBPunfix(cminu->batCacheid);
		if (ret != GDK_SUCCEED) {
			BBPunfix(res->batCacheid);
			throw(MAL, "sql.delta", SQLSTATE(45000) "Internal error in delta processing");
		}

		ret = BATsort(&u, NULL, NULL, res, NULL, NULL, 0, 0);
		BBPunfix(res->batCacheid);
		if (ret != GDK_SUCCEED)
			throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		res = u;
	}
	BATkey(res, TRUE);
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
			throw(MAL, "sql.projectdelta", SQLSTATE(45000) "Internal error in delta processing");

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
		} else {
			if ((res = COLcopy(c, c->ttype, TRUE, TRANSIENT)) == NULL) {
				BBPunfix(s->batCacheid);
				BBPunfix(i->batCacheid);
				BBPunfix(c->batCacheid);
				throw(MAL, "sql.projectdelta", SQLSTATE(45000) "Internal error in delta processing");
			}
			BBPunfix(c->batCacheid);
			if (BATappend(res, i, NULL, FALSE) != GDK_SUCCEED) {
				BBPunfix(s->batCacheid);
				BBPunfix(i->batCacheid);
				throw(MAL, "sql.projectdelta", SQLSTATE(45000) "Internal error in delta processing");
			}
		}
	}
	if (i)
		BBPunfix(i->batCacheid);

	tres = BATproject(s, res);
	BBPunfix(res->batCacheid);
	if (tres == NULL) {
		BBPunfix(s->batCacheid);
		throw(MAL, "sql.projectdelta", SQLSTATE(45000) "Internal error in delta processing");
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
		BAT *o, *nu_id, *nu_val;
		/* create subsets of u_id and u_val where the tail
		 * values of u_id are also in s, and where those tail
		 * values occur as head value in res */
		if (BATsemijoin(&o, NULL, u_id, s, NULL, NULL, 0, BUN_NONE) != GDK_SUCCEED) {
			BBPunfix(s->batCacheid);
			BBPunfix(res->batCacheid);
			BBPunfix(u_id->batCacheid);
			BBPunfix(u_val->batCacheid);
			throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
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
			throw(MAL, "sql.delta", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
			throw(MAL, "sql.delta", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
			throw(MAL, "sql.delta", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
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
	res->tsorted = 0;
	res->trevsorted = 0;
	res->tnil = 0;
	res->tnonil = 0;
	res->tkey = 0;
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
	str msg;
	sql_trans *tr;
	const char *sname = *getArgReference_str(stk, pci, 2);
	const char *tname = *getArgReference_str(stk, pci, 3);

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
		throw(SQL, "sql.tid", SQLSTATE(3F000) "Schema missing %s",sname);
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "sql.tid", SQLSTATE(42S02) "Table missing %s.%s",sname,tname);
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
	tids = BATdense(sb, sb, (BUN) nr);
	if (tids == NULL)
		throw(SQL, "sql.tid", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	if (store_funcs.count_del(tr, t)) {
		BAT *d = store_funcs.bind_del(tr, t, RD_INS);
		BAT *diff;
		if (d == NULL)
			throw(SQL,"sql.tid", SQLSTATE(45002) "Can not bind delete column");

		diff = BATdiff(tids, d, NULL, NULL, 0, BUN_NONE);
		BBPunfix(d->batCacheid);
		BBPunfix(tids->batCacheid);
		if (diff == NULL)
			throw(SQL,"sql.tid", SQLSTATE(45002) "Cannot subtract delete column");
		BAThseqbase(diff, sb);
		tids = diff;
	}
	BBPkeepref(*res = tids->batCacheid);
	return MAL_SUCCEED;
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
	res = *res_id = mvc_result_table(m, mb->tag, pci->argc - (pci->retc + 5), 1, b);
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
		tblname = BUNtail(itertbl,o);
		colname = BUNtail(iteratr,o);
		tpename = BUNtail(itertpe,o);
		b = BATdescriptor(bid);
		if ( b == NULL)
			msg= createException(MAL,"sql.resultset",SQLSTATE(HY005) "Cannot access column descriptor ");
		else if (mvc_result_column(m, tblname, colname, tpename, *digits++, *scaledigits++, b))
			msg = createException(SQL, "sql.resultset", SQLSTATE(42000) "Cannot access column descriptor %s.%s",tblname,colname);
		if( b)
			BBPunfix(bid);
	}
	/* now send it to the channel cntxt->fdout */
	if (mvc_export_result(cntxt->sqlcontext, cntxt->fdout, res, mb->starttime, mb->optimize))
		msg = createException(SQL, "sql.resultset", SQLSTATE(45000) "Result set construction failed");
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
	unsigned char *tsep = NULL, *rsep = NULL, *ssep = NULL, *ns = NULL;
	unsigned char *T = (unsigned char *) *getArgReference_str(stk, pci, 3);
	unsigned char *R = (unsigned char *) *getArgReference_str(stk, pci, 4);
	unsigned char *S = (unsigned char *) *getArgReference_str(stk, pci, 5);
	unsigned char *N = (unsigned char *) *getArgReference_str(stk, pci, 6);

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
		throw(MAL,"sql.resultset", SQLSTATE(HY005) "Cannot access column descriptor");
	res = *res_id = mvc_result_table(m, mb->tag, pci->argc - (pci->retc + 11), 1, order);
	t = m->results;
	if (res < 0){
		msg = createException(SQL, "sql.resultSet", SQLSTATE(45000) "Result set construction failed");
		goto wrapup_result_set1;
	}

	l = strlen((char *) T);
	tsep = GDKmalloc(l + 1);
	if(tsep == 0){
		msg = createException(SQL, "sql.resultSet", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto wrapup_result_set1;
	}
	GDKstrFromStr(tsep, T, l);
	l = 0;
	l = strlen((char *) R);
	rsep = GDKmalloc(l + 1);
	if(rsep == 0){
		msg = createException(SQL, "sql.resultSet", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto wrapup_result_set1;
	}
	GDKstrFromStr(rsep, R, l);
	l = 0;
	l = strlen((char *) S);
	ssep = GDKmalloc(l + 1);
	if(ssep == 0){
		msg = createException(SQL, "sql.resultSet", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto wrapup_result_set1;
	}
	GDKstrFromStr(ssep, S, l);
	l = 0;
	l = strlen((char *) N);
	ns = GDKmalloc(l + 1);
	if(ns == 0){
		msg = createException(SQL, "sql.resultSet", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto wrapup_result_set1;
	}
	GDKstrFromStr(ns, N, l);
	t->tsep = (char *) tsep;
	t->rsep = (char *) rsep;
	t->ssep = (char *) ssep;
	t->ns = (char *) ns;

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

	for( i = 12; msg == MAL_SUCCEED && i< pci->argc; i++, o++){
		bid = *getArgReference_bat(stk,pci,i);
		tblname = BUNtail(itertbl,o);
		colname = BUNtail(iteratr,o);
		tpename = BUNtail(itertpe,o);
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
	if ( strcmp(filename,"stdout") == 0 )
		s= cntxt->fdout;
	else if ( (s = open_wastream(filename)) == NULL || mnstr_errnr(s)) {
		int errnr = mnstr_errnr(s);
		if (s)
			mnstr_destroy(s);
		msg=  createException(IO, "streams.open", SQLSTATE(42000) "could not open file '%s': %s",
				      filename?filename:"stdout", strerror(errnr));
		goto wrapup_result_set1;
	}
	if (mvc_export_result(cntxt->sqlcontext, s, res, mb->starttime, mb->optimize))
		msg = createException(SQL, "sql.resultset", SQLSTATE(45000) "Result set construction failed");
	if( s != cntxt->fdout)
		close_stream(s);
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
	res = *res_id = mvc_result_table(m, mb->tag, pci->argc - (pci->retc + 5), 1, NULL);

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
		tblname = BUNtail(itertbl,o);
		colname = BUNtail(iteratr,o);
		tpename = BUNtail(itertpe,o);

		v = getArgReference(stk, pci, i);
		mtype = getArgType(mb, pci, i);
		if (ATOMextern(mtype))
			v = *(ptr *) v;
		if (mvc_result_value(m, tblname, colname, tpename, *digits++, *scaledigits++, v, mtype))
			throw(SQL, "sql.rsColumn", SQLSTATE(45000) "Result set construction failed");
	}
	if (mvc_export_result(cntxt->sqlcontext, cntxt->fdout, res, mb->starttime, mb->optimize))
		msg = createException(SQL, "sql.resultset", SQLSTATE(45000) "Result set construction failed");
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
	unsigned char *tsep = NULL, *rsep = NULL, *ssep = NULL, *ns = NULL;
	unsigned char *T = (unsigned char *) *getArgReference_str(stk, pci, 3);
	unsigned char *R = (unsigned char *) *getArgReference_str(stk, pci, 4);
	unsigned char *S = (unsigned char *) *getArgReference_str(stk, pci, 5);
	unsigned char *N = (unsigned char *) *getArgReference_str(stk, pci, 6);

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
	res = *res_id = mvc_result_table(m, mb->tag, pci->argc - (pci->retc + 11), 1, NULL);

	t = m->results;
	if (res < 0){
		msg = createException(SQL, "sql.resultSet", SQLSTATE(45000) "Result set construction failed");
		goto wrapup_result_set;
	}

	l = strlen((char *) T);
	tsep = GDKmalloc(l + 1);
	if(tsep == 0){
		msg = createException(SQL, "sql.resultSet", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto wrapup_result_set;
	}
	GDKstrFromStr(tsep, T, l);
	l = 0;
	l = strlen((char *) R);
	rsep = GDKmalloc(l + 1);
	if(rsep == 0){
		GDKfree(tsep);
		msg = createException(SQL, "sql.resultSet", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto wrapup_result_set;
	}
	GDKstrFromStr(rsep, R, l);
	l = 0;
	l = strlen((char *) S);
	ssep = GDKmalloc(l + 1);
	if(ssep == 0){
		GDKfree(tsep);
		GDKfree(rsep);
		msg = createException(SQL, "sql.resultSet", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto wrapup_result_set;
	}
	GDKstrFromStr(ssep, S, l);
	l = 0;
	l = strlen((char *) N);
	ns = GDKmalloc(l + 1);
	if(ns == 0){
		GDKfree(tsep);
		GDKfree(rsep);
		GDKfree(ssep);
		msg = createException(SQL, "sql.resultSet", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto wrapup_result_set;
	}
	GDKstrFromStr(ns, N, l);
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
	/* mimick the old rsColumn approach; */
	itertbl = bat_iterator(tbl);
	iteratr = bat_iterator(atr);
	itertpe = bat_iterator(tpe);
	digits = (int*) Tloc(len,0);
	scaledigits = (int*) Tloc(scale,0);

	for( i = 12; msg == MAL_SUCCEED && i< pci->argc; i++, o++){
		tblname = BUNtail(itertbl,o);
		colname = BUNtail(iteratr,o);
		tpename = BUNtail(itertpe,o);

		v = getArgReference(stk, pci, i);
		mtype = getArgType(mb, pci, i);
		if (ATOMextern(mtype))
			v = *(ptr *) v;
		if (mvc_result_value(m, tblname, colname, tpename, *digits++, *scaledigits++, v, mtype))
			throw(SQL, "sql.rsColumn", SQLSTATE(45000) "Result set construction failed");
	}
	/* now select the file channel */
	if ( strcmp(filename,"stdout") == 0 )
		s= cntxt->fdout;
	else if ( (s = open_wastream(filename)) == NULL || mnstr_errnr(s)) {
		int errnr = mnstr_errnr(s);
		if (s)
			mnstr_destroy(s);
		msg=  createException(IO, "streams.open", SQLSTATE(42000) "could not open file '%s': %s",
				      filename?filename:"stdout", strerror(errnr));
		goto wrapup_result_set;
	}
	if (mvc_export_result(cntxt->sqlcontext, s, res, mb->starttime, mb->optimize))
		msg = createException(SQL, "sql.resultset", SQLSTATE(45000) "Result set construction failed");
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
	int nr_cols;
	int qtype;
	bat order_bid;

	if ( pci->argc > 6)
		return mvc_result_set_wrap(cntxt,mb,stk,pci);

	res_id = getArgReference_int(stk, pci, 0);
	nr_cols = *getArgReference_int(stk, pci, 1);
	qtype = *getArgReference_int(stk, pci, 2);
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
		if (mvc_export_result(b, cntxt->fdout, res_id, mb->starttime, mb->optimize))
			throw(SQL, "sql.exportResult", SQLSTATE(45000) "Result set construction failed");
	} else if (mvc_export_result(b, *s, res_id, mb->starttime, mb->optimize))
		throw(SQL, "sql.exportResult", SQLSTATE(45000) "Result set construction failed");
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
		nr = (BUN) *getArgReference_int(stk, pci, 4);
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
	res_id = mvc_result_table(b->mvc, mb->tag, 1, 1, NULL);
	if (mvc_result_value(b->mvc, tn, cn, type, digits, scale, p, mtype))
		throw(SQL, "sql.exportValue", SQLSTATE(45000) "Result set construction failed");
	if (b->output_format == OFMT_NONE) {
		return MAL_SUCCEED;
	}
	if (mvc_export_result(b, b->out, res_id, mb->starttime, mb->optimize) < 0) {
		throw(SQL, "sql.exportValue", SQLSTATE(45000) "Result set construction failed");
	}
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
	unsigned char *tsep = NULL, *rsep = NULL, *ssep = NULL, *ns = NULL, *fn = NULL;
	ssize_t len = 0;
	sql_table *t = *(sql_table **) getArgReference(stk, pci, pci->retc + 0);
	unsigned char *T = (unsigned char *) *getArgReference_str(stk, pci, pci->retc + 1);
	unsigned char *R = (unsigned char *) *getArgReference_str(stk, pci, pci->retc + 2);
	unsigned char *S = (unsigned char *) *getArgReference_str(stk, pci, pci->retc + 3);
	unsigned char *N = (unsigned char *) *getArgReference_str(stk, pci, pci->retc + 4);
	const char *fname = *getArgReference_str(stk, pci, pci->retc + 5);
	lng sz = *getArgReference_lng(stk, pci, pci->retc + 6);
	lng offset = *getArgReference_lng(stk, pci, pci->retc + 7);
	int locked = *getArgReference_int(stk, pci, pci->retc + 8);
	int besteffort = *getArgReference_int(stk, pci, pci->retc + 9);
	char *fixed_widths = *getArgReference_str(stk, pci, pci->retc + 10);
	str msg = MAL_SUCCEED;
	bstream *s = NULL;
	stream *ss;

	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	be = cntxt->sqlcontext;
	len = strlen((char *) T);
	if ((tsep = GDKmalloc(len + 1)) == NULL)
		throw(MAL, "sql.copy_from", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	GDKstrFromStr(tsep, T, len);
	len = 0;
	len = strlen((char *) R);
	if ((rsep = GDKmalloc(len + 1)) == NULL) {
		GDKfree(tsep);
		throw(MAL, "sql.copy_from", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	GDKstrFromStr(rsep, R, len);
	len = 0;
	if (*S && strcmp(str_nil, (char *) S)) {
		len = strlen((char *) S);
		if ((ssep = GDKmalloc(len + 1)) == NULL) {
			GDKfree(tsep);
			GDKfree(rsep);
			throw(MAL, "sql.copy_from", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		GDKstrFromStr(ssep, S, len);
		len = 0;
	}
	len = strlen((char *) N);
	if ((ns = GDKmalloc(len + 1)) == NULL) {
		GDKfree(tsep);
		GDKfree(rsep);
		GDKfree(ssep);
		throw(MAL, "sql.copy_from", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	GDKstrFromStr(ns, N, len);
	len = 0;

	if (!fname || strcmp(str_nil, (char *) fname) == 0)
		fname = NULL;
	if (!fname) {
		msg = mvc_import_table(cntxt, &b, be->mvc, be->mvc->scanner.rs, t, (char *) tsep, (char *) rsep, (char *) ssep, (char *) ns, sz, offset, locked, besteffort);
	} else {
		len = strlen(fname);
		if ((fn = GDKmalloc(len + 1)) == NULL) {
			GDKfree(ns);
			GDKfree(tsep);
			GDKfree(rsep);
			GDKfree(ssep);
			throw(MAL, "sql.copy_from", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
#if defined(HAVE_EMBEDDED) && defined(WIN32)
		// fix single backslash file separator on windows
		strcpy(fn, fname);
#else
		GDKstrFromStr(fn, (unsigned char*)fname, len);
#endif
		ss = open_rastream((const char *) fn);
		if (!ss || mnstr_errnr(ss)) {
			int errnr = mnstr_errnr(ss);
			if (ss)
				mnstr_destroy(ss);
			GDKfree(tsep);
			GDKfree(rsep);
			GDKfree(ssep);
			GDKfree(ns);
			msg = createException(IO, "sql.copy_from", SQLSTATE(42000) "Cannot open file '%s': %s", fn, strerror(errnr));
			GDKfree(fn);
			return msg;
		}
		GDKfree(fn);
		if (fixed_widths && strcmp(fixed_widths, str_nil) != 0) {
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
				mnstr_destroy(ss);
				GDKfree(tsep);
				GDKfree(rsep);
				GDKfree(ssep);
				GDKfree(ns);
				throw(MAL, "sql.copy_from", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
			for (i = 0; i < width_len; i++) {
				if (fixed_widths[i] == STREAM_FWF_FIELD_SEP) {
					fixed_widths[i] = '\0';
					widths[current_width_entry++] = (size_t) strtoll(val_start, NULL, 10);
					val_start = fixed_widths + i + 1;
				}
			}
			/* overwrite other delimiters to the ones the FWF stream uses */
			sprintf((char*) tsep, "%c", STREAM_FWF_FIELD_SEP);
			sprintf((char*) rsep, "%c", STREAM_FWF_RECORD_SEP);
			if (!ssep) {
				ssep = GDKmalloc(2);
				if(ssep == NULL)
					throw(SQL, "sql.copy_from", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
			ssep[0] = 0;

			ss = stream_fwf_create(ss, ncol, widths, STREAM_FWF_FILLER);
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
			msg = mvc_import_table(cntxt, &b, be->mvc, s, t, (char *) tsep, (char *) rsep, (char *) ssep, (char *) ns, sz, offset, locked, besteffort);
			bstream_destroy(s);
		}
	}
	GDKfree(tsep);
	GDKfree(rsep);
	if (ssep)
		GDKfree(ssep);
	GDKfree(ns);
	if (fname && s == NULL)
		throw(IO, "bstreams.create", SQLSTATE(42000) "Failed to create block stream");
	if (b == NULL)
		throw(SQL, "importTable", SQLSTATE(42000) "Failed to import table '%s', %s", t->base.name, be->mvc->errstr);
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
	int init = 0;
	int i;
	const char *sname = *getArgReference_str(stk, pci, 0 + pci->retc);
	const char *tname = *getArgReference_str(stk, pci, 1 + pci->retc);
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
		throw(SQL, "sql.import_table", SQLSTATE(3F000) "Schema missing %s",sname);
	t = mvc_bind_table(m, s, tname);
	if (!t)
		throw(SQL, "sql", SQLSTATE(42S02) "Table missing %s", tname);
	if (list_length(t->columns.set) != (pci->argc - (2 + pci->retc)))
		throw(SQL, "sql", SQLSTATE(42000) "Not enough columns found in input file");

	for (i = pci->retc + 2, n = t->columns.set->h; i < pci->argc && n; i++, n = n->next) {
		sql_column *col = n->data;
		const char *fname = *getArgReference_str(stk, pci, i);
		size_t flen;
		char *fn;

		if (strcmp(fname, str_nil) == 0)  {
			// no file name passed for this column
			continue;
		}
		flen =  strlen(fname);

		if (ATOMvarsized(col->type.type->localtype) && col->type.type->localtype != TYPE_str)
			throw(SQL, "sql", SQLSTATE(42000) "Failed to attach file %s", *getArgReference_str(stk, pci, i));
		fn = GDKmalloc(flen + 1);
		if(fn == NULL)
			throw(SQL, "sql.attach", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		GDKstrFromStr((unsigned char *) fn, (const unsigned char *) fname, flen);
		if (fn == NULL)
			throw(SQL, "sql", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		f = fopen(fn, "r");
		if (f == NULL) {
			msg = createException(SQL, "sql", SQLSTATE(42000) "Failed to open file %s", fn);
			GDKfree(fn);
			return msg;
		}
		GDKfree(fn);
		fclose(f);
	}

	for (i = pci->retc + 2, n = t->columns.set->h; i < pci->argc && n; i++, n = n->next) {
		sql_column *col = n->data;
		BAT *c = NULL;
		int tpe = col->type.type->localtype;
		const char *fname = *getArgReference_str(stk, pci, i);

		/* handle the various cases */
		if (strcmp(fname, str_nil) == 0) {
			// no filename for this column, skip for now because we potentially don't know the count yet
			continue;
		} else if (tpe < TYPE_str || tpe == TYPE_date || tpe == TYPE_daytime || tpe == TYPE_timestamp) {
			c = BATattach(col->type.type->localtype, fname, TRANSIENT);
			if (c == NULL)
				throw(SQL, "sql", SQLSTATE(42000) "Failed to attach file %s", fname);
			BATsetaccess(c, BAT_READ);
		} else if (tpe == TYPE_str) {
			/* get the BAT and fill it with the strings */
			c = COLnew(0, TYPE_str, 0, TRANSIENT);
			if (c == NULL)
				throw(SQL, "sql", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			/* this code should be extended to deal with larger text strings. */
			f = fopen(*getArgReference_str(stk, pci, i), "r");
			if (f == NULL) {
				BBPreclaim(c);
				throw(SQL, "sql", SQLSTATE(42000) "Failed to re-open file %s", fname);
			}

			buf = GDKmalloc(bufsiz);
			if (!buf) {
				fclose(f);
				BBPreclaim(c);
				throw(SQL, "sql", SQLSTATE(42000) "Failed to create buffer");
			}
			while (fgets(buf, bufsiz, f) != NULL) {
				char *t = strrchr(buf, '\n');
				if (t)
					*t = 0;
				if (BUNappend(c, buf, FALSE) != GDK_SUCCEED) {
					BBPreclaim(c);
					fclose(f);
					throw(SQL, "sql", SQLSTATE(HY001) MAL_MALLOC_FAIL);
				}
			}
			fclose(f);
			GDKfree(buf);
		} else {
			throw(SQL, "sql", SQLSTATE(42000) "Failed to attach file %s", fname);
		}
		if (init && cnt != BATcount(c)) {
			BBPunfix(c->batCacheid);
			throw(SQL, "sql", SQLSTATE(42000) "Binary files for table '%s' have inconsistent counts", tname);
		}
		cnt = BATcount(c);
		init = 1;
		*getArgReference_bat(stk, pci, i - (2 + pci->retc)) = c->batCacheid;
		BBPkeepref(c->batCacheid);
	}
	if (init) {
		for (i = pci->retc + 2, n = t->columns.set->h; i < pci->argc && n; i++, n = n->next) {
			// now that we know the BAT count, we can fill in the columns for which no parameters were passed
			sql_column *col = n->data;
			BAT *c = NULL;
			int tpe = col->type.type->localtype;

			const char *fname = *getArgReference_str(stk, pci, i);
			if (strcmp(fname, str_nil) == 0) {
				BUN loop = 0;
				const void* nil = ATOMnilptr(tpe);
				// fill the new BAT with NULL values
				c = COLnew(0, tpe, cnt, TRANSIENT);
				if (c == NULL)
					throw(SQL, "sql", SQLSTATE(HY001) MAL_MALLOC_FAIL);
				for(loop = 0; loop < cnt; loop++) {
					if (BUNappend(c, nil, 0) != GDK_SUCCEED) {
						BBPreclaim(c);
						throw(SQL, "sql", SQLSTATE(HY001) MAL_MALLOC_FAIL);
					}
				}
				*getArgReference_bat(stk, pci, i - (2 + pci->retc)) = c->batCacheid;
				BBPkeepref(c->batCacheid);
			}
		}
	} 
	return MAL_SUCCEED;
}

str
zero_or_one(ptr ret, const bat *bid)
{
	BAT *b;
	BUN c;
	size_t _s;
	const void *p;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "zero_or_one", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	c = BATcount(b);
	if (c == 0) {
		p = ATOMnilptr(b->ttype);
	} else if (c == 1) {
		BATiter bi = bat_iterator(b);
		p = BUNtail(bi, 0);
	} else {
		p = NULL;
		BBPunfix(b->batCacheid);
		throw(SQL, "zero_or_one", SQLSTATE(21000) "Cardinality violation, scalar value expected");
	}
	_s = ATOMsize(ATOMtype(b->ttype));
	if (ATOMextern(b->ttype)) {
		_s = ATOMlen(ATOMtype(b->ttype), p);
		*(ptr *) ret = GDKmalloc(_s);
		if(*(ptr *) ret == NULL){
			BBPunfix(b->batCacheid);
			throw(SQL, "zero_or_one", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		memcpy(*(ptr *) ret, p, _s);
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
SQLall(ptr ret, const bat *bid)
{
	BAT *b;
	BUN c, _s;
	const void *p;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "all", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	c = BATcount(b);
	if (c == 0) {
		p = ATOMnilptr(b->ttype);
	} else {
		BUN q, r;
		int (*ocmp) (const void *, const void *);
		BATiter bi = bat_iterator(b);
		q = 0;
		r = BUNlast(b);
		p = BUNtail(bi, q);
		ocmp = ATOMcompare(b->ttype);
		for( ; (q+1) < r; q++) {
			const void *c = BUNtail(bi, q+1);
			if (ocmp(p, c) != 0) {
				p = ATOMnilptr(b->ttype);
				break;
			}
		}
	}
	_s = ATOMsize(ATOMtype(b->ttype));
	if (ATOMextern(b->ttype)) {
		_s = ATOMlen(ATOMtype(b->ttype), p);
		memcpy(*(ptr *) ret = GDKmalloc(_s), p, _s);
		if(ret == NULL){
			BBPunfix(b->batCacheid);
			throw(SQL, "SQLall", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
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
	size_t len = sizeof(daytime);
	ssize_t pos;

	if (!*v || strcmp(str_nil, *v) == 0) {
		*res = daytime_nil;
		return MAL_SUCCEED;
	}
	if (*tz)
		pos = daytime_tz_fromstr(*v, &len, &res);
	else
		pos = daytime_fromstr(*v, &len, &res);
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
	size_t len = sizeof(timestamp);
	ssize_t pos;

	if (!*v || strcmp(str_nil, *v) == 0) {
		*res = *timestamp_nil;
		return MAL_SUCCEED;
	}
	if (*tz)
		pos = timestamp_tz_fromstr(*v, &len, &res);
	else
		pos = timestamp_fromstr(*v, &len, &res);
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
	BATiter bi;
	BUN p, q;
	dbl s, c1, c2, r;
	char *msg = NULL;

	if (is_dbl_nil(*theta)) {
		throw(SQL, "SQLbat_alpha", SQLSTATE(42000) "Parameter theta should not be nil");
	}
	if ((b = BATdescriptor(*decl)) == NULL) {
		throw(SQL, "alpha", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	bi = bat_iterator(b);
	bn = COLnew(b->hseqbase, TYPE_dbl, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.alpha", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	s = sin(radians(*theta));
	BATloop(b, p, q) {
		dbl d = *(dbl *) BUNtail(bi, p);
		if (is_dbl_nil(d))
			r = dbl_nil;
		else if (fabs(d) + *theta > 89.9)
			r = 180.0;
		else {
			c1 = cos(radians(d - *theta));
			c2 = cos(radians(d + *theta));
			r = degrees(fabs(atan(s / sqrt(fabs(c1 * c2)))));
		}
		if (BUNappend(bn, &r, FALSE) != GDK_SUCCEED) {
			BBPreclaim(bn);
			throw(SQL, "sql.alpha", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	}
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
		throw(SQL, "alpha", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	bi = bat_iterator(b);
	bn = COLnew(b->hseqbase, TYPE_dbl, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.alpha", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		dbl d = *decl;
		dbl *theta = (dbl *) BUNtail(bi, p);

		if (is_dbl_nil(d))
			r = dbl_nil;
		else if (fabs(d) + *theta > 89.9)
			r = (dbl) 180.0;
		else {
			s = sin(radians(*theta));
			c1 = cos(radians(d - *theta));
			c2 = cos(radians(d + *theta));
			r = degrees(fabs(atan(s / sqrt(fabs(c1 * c2)))));
		}
		if (BUNappend(bn, &r, FALSE) != GDK_SUCCEED) {
			BBPreclaim(bn);
			throw(SQL, "sql.alpha", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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

	if (interval_from_str(*s, *d, *sk, &res) < 0)
		throw(SQL, "calc.month_interval", SQLSTATE(42000) "Wrong format (%s)", *s);
	assert((lng) GDK_int_min <= res && res <= (lng) GDK_int_max);
	*ret = (int) res;
	return MAL_SUCCEED;
}

str
second_interval_str(lng *res, const str *s, const int *d, const int *sk)
{
	if (interval_from_str(*s, *d, *sk, res) < 0)
		throw(SQL, "calc.second_interval", SQLSTATE(42000) "Wrong format (%s)", *s);
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
	case TYPE_lng:
		r = (int) stk->stk[getArg(pci, 1)].val.lval;
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		r = (int) stk->stk[getArg(pci, 1)].val.hval;
		break;
#endif
	default:
		throw(ILLARG, "calc.month_interval", SQLSTATE(42000) "Illegal argument");
	}
	switch (k) {
	case iyear:
		r *= 12;
		break;
	case imonth:
		break;
	default:
		throw(ILLARG, "calc.month_interval", SQLSTATE(42000) "Illegal argument");
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
	case TYPE_lng:
		r = stk->stk[getArg(pci, 1)].val.lval;
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		r = (lng) stk->stk[getArg(pci, 1)].val.hval;
		break;
#endif
	default:
		throw(ILLARG, "calc.sec_interval", SQLSTATE(42000) "Illegal argument in second interval");
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
		throw(ILLARG, "calc.sec_interval", SQLSTATE(42000) "Illegal argument in second interval");
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
	query = COLnew(0, TYPE_str, cnt, TRANSIENT);
	if (query == NULL)
		throw(SQL, "sql.dumpcache", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	count = COLnew(0, TYPE_int, cnt, TRANSIENT);
	if (count == NULL) {
		BBPunfix(query->batCacheid);
		throw(SQL, "sql.dumpcache", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	for (q = m->qc->q; q; q = q->next) {
		if (q->type != Q_PREPARE) {
			if (BUNappend(query, q->codestring, FALSE) != GDK_SUCCEED ||
			    BUNappend(count, &q->count, FALSE) != GDK_SUCCEED) {
				BBPunfix(query->batCacheid);
				BBPunfix(count->batCacheid);
				throw(SQL, "sql.dumpcache", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		throw(SQL, "sql.optstats", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	if (BUNappend(rewrite, "joinidx", FALSE) != GDK_SUCCEED ||
	    BUNappend(count, &m->opt_stats[0], FALSE) != GDK_SUCCEED) {
		BBPreclaim(rewrite);
		BBPreclaim(count);
		throw(SQL, "sql.optstats", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
	BAT *t[13];
	bat id;

	(void) cntxt;
	(void) mb;
	if (TRACEtable(t) != 13)
		throw(SQL, "sql.dump_trace", SQLSTATE(3F000) "Profiler not started");
	for(i=0; i< 13; i++)
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
		throw(SQL, name, SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		if (BUNappend(r, &rank, FALSE) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPunfix(g->batCacheid);
			BBPunfix(r->batCacheid);
			throw(SQL, name, SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		throw(SQL, name, SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	if (BATtdense(b)) {
		BATloop(b, p, q) {
			if (BUNappend(r, &rank, FALSE) != GDK_SUCCEED)
				goto bailout;
			rank++;
		}
	} else {
		BATloop(b, p, q) {
			n = BUNtail(bi, p);
			if ((c = cmp(n, cur)) != 0)
				rank = nrank;
			cur = n;
			if (BUNappend(r, &rank, FALSE) != GDK_SUCCEED)
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
	throw(SQL, name, SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
	int i, bids[2049];

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

	mvc_clear_table(m, t);
	for (o = t->columns.set->h, i = 0; o; o = o->next, i++) {
		sql_column *c = o->data;
		BAT *ins = BATdescriptor(bids[i]);	/* use the insert bat */

		if( ins){
			store_funcs.append_col(tr, c, ins, TYPE_bat);
			BBPunfix(ins->batCacheid);
		}
		BBPrelease(bids[i]);
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
	throw(SQL, "updateOptimizer", SQLSTATE(42000) PROGRAM_NYI);
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
								if (BUNappend(sch, b->name, FALSE) != GDK_SUCCEED ||
								    BUNappend(tab, bt->name, FALSE) != GDK_SUCCEED ||
								    BUNappend(col, bc->name, FALSE) != GDK_SUCCEED)
									goto bailout;
								if (c->t->access == TABLE_WRITABLE) {
									if (BUNappend(mode, "writable", FALSE) != GDK_SUCCEED)
										goto bailout;
								} else if (c->t->access == TABLE_APPENDONLY) {
									if (BUNappend(mode, "appendonly", FALSE) != GDK_SUCCEED)
										goto bailout;
								} else if (c->t->access == TABLE_READONLY) {
									if (BUNappend(mode, "readonly", FALSE) != GDK_SUCCEED)
										goto bailout;
								} else {
									if (BUNappend(mode, 0, FALSE) != GDK_SUCCEED)
										goto bailout;
								}
								if (BUNappend(type, c->type.type->sqlname, FALSE) != GDK_SUCCEED)
									goto bailout;

								/*printf(" cnt "BUNFMT, BATcount(bn)); */
								sz = BATcount(bn);
								if (BUNappend(cnt, &sz, FALSE) != GDK_SUCCEED)
									goto bailout;

								/*printf(" loc %s", BBP_physical(bn->batCacheid)); */
								if (BUNappend(loc, BBP_physical(bn->batCacheid), FALSE) != GDK_SUCCEED)
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
										str s = BUNtail(bi, p);
										if (s != NULL && strcmp(s, str_nil))
											sum += (int) strlen(s);
										if (--cnt1 <= 0)
											break;
									}
									if (cnt2)
										w = (int) (sum / cnt2);
								}
								if (BUNappend(atom, &w, FALSE) != GDK_SUCCEED)
									goto bailout;

								sz = BATcount(bn) * bn->twidth; 
								if (BUNappend(size, &sz, FALSE) != GDK_SUCCEED)
									goto bailout;

								sz = heapinfo(bn->tvheap, bn->batCacheid);
								if (BUNappend(heap, &sz, FALSE) != GDK_SUCCEED)
									goto bailout;

								sz = hashinfo(bn->thash, bn->batCacheid);
								if (BUNappend(indices, &sz, FALSE) != GDK_SUCCEED)
									goto bailout;

								bitval = 0; /* HASHispersistent(bn); */
								if (BUNappend(phash, &bitval, FALSE) != GDK_SUCCEED)
									goto bailout;

								sz = IMPSimprintsize(bn);
								if (BUNappend(imprints, &sz, FALSE) != GDK_SUCCEED)
									goto bailout;
								/*printf(" indices "BUNFMT, bn->thash?bn->thash->heap.size:0); */
								/*printf("\n"); */

								bitval = BATtordered(bn);
								if (!bitval && bn->tnosorted == 0)
									bitval = bit_nil;
								if (BUNappend(sort, &bitval, FALSE) != GDK_SUCCEED)
									goto bailout;

								bitval = BATtrevordered(bn);
								if (!bitval && bn->tnorevsorted == 0)
									bitval = bit_nil;
								if (BUNappend(revsort, &bitval, FALSE) != GDK_SUCCEED)
									goto bailout;

								bitval = BATtkey(bn);
								if (!bitval && bn->tnokey[0] == 0 && bn->tnokey[1] == 0)
									bitval = bit_nil;
								if (BUNappend(key, &bitval, FALSE) != GDK_SUCCEED)
									goto bailout;

								sz = bn->torderidx && bn->torderidx != (Heap *) 1 ? bn->torderidx->free : 0;
								if (BUNappend(oidx, &sz, FALSE) != GDK_SUCCEED)
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
									if (BUNappend(sch, b->name, FALSE) != GDK_SUCCEED ||
									    BUNappend(tab, bt->name, FALSE) != GDK_SUCCEED ||
									    BUNappend(col, bc->name, FALSE) != GDK_SUCCEED)
										goto bailout;
									if (c->t->access == TABLE_WRITABLE) {
										if (BUNappend(mode, "writable", FALSE) != GDK_SUCCEED)
											goto bailout;
									} else if (c->t->access == TABLE_APPENDONLY) {
										if (BUNappend(mode, "appendonly", FALSE) != GDK_SUCCEED)
											goto bailout;
									} else if (c->t->access == TABLE_READONLY) {
										if (BUNappend(mode, "readonly", FALSE) != GDK_SUCCEED)
											goto bailout;
									} else {
										if (BUNappend(mode, 0, FALSE) != GDK_SUCCEED)
											goto bailout;
									}
									if (BUNappend(type, "oid", FALSE) != GDK_SUCCEED)
										goto bailout;

									/*printf(" cnt "BUNFMT, BATcount(bn)); */
									sz = BATcount(bn);
									if (BUNappend(cnt, &sz, FALSE) != GDK_SUCCEED)
										goto bailout;

									/*printf(" loc %s", BBP_physical(bn->batCacheid)); */
									if (BUNappend(loc, BBP_physical(bn->batCacheid), FALSE) != GDK_SUCCEED)
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
											str s = BUNtail(bi, p);
											if (s != NULL && strcmp(s, str_nil))
												sum += (int) strlen(s);
											if (--cnt1 <= 0)
												break;
										}
										if (cnt2)
											w = (int) (sum / cnt2);
									}
									if (BUNappend(atom, &w, FALSE) != GDK_SUCCEED)
										goto bailout;
									/*printf(" size "BUNFMT, tailsize(bn,BATcount(bn)) + (bn->tvheap? bn->tvheap->size:0)); */
									sz = tailsize(bn, BATcount(bn));
									if (BUNappend(size, &sz, FALSE) != GDK_SUCCEED)
										goto bailout;

									sz = bn->tvheap ? bn->tvheap->size : 0;
									if (BUNappend(heap, &sz, FALSE) != GDK_SUCCEED)
										goto bailout;

									sz = bn->thash && bn->thash != (Hash *) 1 ? bn->thash->heap.size : 0; /* HASHsize() */
									if (BUNappend(indices, &sz, FALSE) != GDK_SUCCEED)
										goto bailout;
									bitval = 0; /* HASHispersistent(bn); */
									if (BUNappend(phash, &bitval, FALSE) != GDK_SUCCEED)
										goto bailout;

									sz = IMPSimprintsize(bn);
									if (BUNappend(imprints, &sz, FALSE) != GDK_SUCCEED)
										goto bailout;
									/*printf(" indices "BUNFMT, bn->thash?bn->thash->heap.size:0); */
									/*printf("\n"); */
									bitval = BATtordered(bn);
									if (!bitval && bn->tnosorted == 0)
										bitval = bit_nil;
									if (BUNappend(sort, &bitval, FALSE) != GDK_SUCCEED)
										goto bailout;
									bitval = BATtrevordered(bn);
									if (!bitval && bn->tnorevsorted == 0)
										bitval = bit_nil;
									if (BUNappend(revsort, &bitval, FALSE) != GDK_SUCCEED)
										goto bailout;
									bitval = BATtkey(bn);
									if (!bitval && bn->tnokey[0] == 0 && bn->tnokey[1] == 0)
										bitval = bit_nil;
									if (BUNappend(key, &bitval, FALSE) != GDK_SUCCEED)
										goto bailout;
									sz = bn->torderidx && bn->torderidx != (Heap *) 1 ? bn->torderidx->free : 0;
									if (BUNappend(oidx, &sz, FALSE) != GDK_SUCCEED)
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
	throw(SQL, "sql.storage", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
			throw(SQL, "calc.index", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
			if (BUNappend(r, &v, FALSE) != GDK_SUCCEED) {
				BBPreclaim(r);
				BBPunfix(s->batCacheid);
				throw(SQL, "calc.index", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
			pos += GDK_STRLEN(p);
		}
	} else {
		r = VIEWcreate(s->hseqbase, s);
		if (r == NULL) {
			BBPunfix(s->batCacheid);
			throw(SQL, "calc.index", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		r->ttype = TYPE_int;
		r->tvarsized = 0;
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
			throw(SQL, "calc.index", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
			if (BUNappend(r, &v, FALSE) != GDK_SUCCEED) {
				BBPreclaim(r);
				throw(SQL, "calc.index", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
			pos += GDK_STRLEN(s);
		}
	} else {
		r = VIEWcreate(s->hseqbase, s);
		if (r == NULL) {
			BBPunfix(s->batCacheid);
			throw(SQL, "calc.index", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		r->ttype = TYPE_sht;
		r->tvarsized = 0;
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
			throw(SQL, "calc.index", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
			if (BUNappend(r, &v, FALSE) != GDK_SUCCEED) {
				BBPreclaim(r);
				BBPunfix(s->batCacheid);
				throw(SQL, "calc.index", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
			pos += GDK_STRLEN(p);
		}
	} else {
		r = VIEWcreate(s->hseqbase, s);
		if (r == NULL) {
			BBPunfix(s->batCacheid);
			throw(SQL, "calc.index", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		r->ttype = TYPE_bte;
		r->tvarsized = 0;
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
		throw(SQL, "calc.strings", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	pos = GDK_STRHASHSIZE;
	while (pos < h->free) {
		const char *p;

		pad = GDK_VARALIGN - (pos & (GDK_VARALIGN - 1));
		if (pad < sizeof(stridx_t))
			pad += GDK_VARALIGN;
		pos += pad + extralen;
		p = h->base + pos;
		if (BUNappend(r, p, FALSE) != GDK_SUCCEED) {
			BBPreclaim(r);
			BBPunfix(s->batCacheid);
			throw(SQL, "calc.strings", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		pos += GDK_STRLEN(p);
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
SQLexist_val(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *res = getArgReference_bit(stk, pci, 0);
	ptr v = getArgReference(stk, pci, 1);
	int mtype = getArgType(mb, pci, 1);

	(void)cntxt;
	if (ATOMcmp(mtype, v, ATOMnilptr(mtype)) != 0)
		*res = TRUE;
	else
		*res = FALSE;
	return MAL_SUCCEED;
}

str
SQLexist(bit *res, bat *id)
{
	BAT *b;

	if ((b = BATdescriptor(*id)) == NULL)
		throw(SQL, "aggr.exist", SQLSTATE(HY005) "Cannot access column descriptor");
	*res = BATcount(b) != 0;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}
