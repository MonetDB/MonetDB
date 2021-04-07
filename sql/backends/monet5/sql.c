/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "mal.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_module.h"
#include "mal_session.h"
#include "mal_resolve.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_profiler.h"
#include "bat5.h"
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
	if (!need_distinct && rel && is_groupby(rel->op) && rel->exps && !rel->r) {
		for (node *n = rel->exps->h; n && !need_distinct; n = n->next) {
			sql_exp *e = n->data;
			if (e->type == e_aggr) {

				if (need_distinct(e))
					need_distinct = 1;
			}
		}
	}
	return need_distinct;
}

sql_rel *
sql_symbol2relation(backend *be, symbol *sym)
{
	sql_rel *rel;
	sql_query *query = query_create(be->mvc);
	lng Tbegin;
	int extra_opts = be->mvc->emode != m_prepare;

	rel = rel_semantic(query, sym);
	Tbegin = GDKusec();
	if (rel)
		rel = sql_processrelation(be->mvc, rel, extra_opts, extra_opts);
	if (rel)
		rel = rel_distribute(be->mvc, rel);
	if (rel)
		rel = rel_partition(be->mvc, rel);
	if (rel && (rel_no_mitosis(rel) || rel_need_distinct_query(rel)))
		be->no_mitosis = 1;
	be->reloptimizer = GDKusec() - Tbegin;
	return rel;
}

/*
 * After the SQL statement has been executed, its data structures
 * should be garbage collected. For successful actions we have to finish
 * the transaction as well, e.g. commit or rollback.
 */
int
sqlcleanup(backend *be, int err)
{
	sql_destroy_params(be->mvc);

	/* some statements dynamically disable caching */
	be->mvc->sym = NULL;
	if (be->mvc->ta)
		be->mvc->ta = sa_reset(be->mvc->ta);
	if (be->mvc->sa)
		be->mvc->sa = sa_reset(be->mvc->sa);
	if (err >0)
		be->mvc->session->status = -err;
	if (err <0)
		be->mvc->session->status = err;
	be->mvc->label = 0;
	be->no_mitosis = 0;
	scanner_query_processed(&(be->mvc->scanner));
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
getBackendContext(Client cntxt, backend **be)
{
	str msg;

	if ((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;
	*be = (backend *) cntxt->sqlcontext;
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

static str
SQLshutdown_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg;

	if ((msg = CLTshutdown(cntxt, mb, stk, pci)) == MAL_SUCCEED) {
		/* administer the shutdown in the system log */
		TRC_INFO(SQL_TRANS, "Shutdown: %s\n", *getArgReference_str(stk, pci, 0));
	}
	return msg;
}

static str
SQLset_protocol(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	const int protocol = *getArgReference_int(stk, pci, 1);

	(void) mb;
	(void) stk;

	if (!(
		protocol == PROTOCOL_AUTO ||
		protocol == PROTOCOL_9 ||
		protocol == PROTOCOL_COLUMNAR))
	{
		return createException(SQL, "sql.set_protocol", "unknown protocol: %d", protocol);
	}

	*getArgReference_int(stk, pci, 0) = (cntxt->protocol = (protocol_version) protocol);

	return MAL_SUCCEED;
}

str
create_table_or_view(mvc *sql, char* sname, char *tname, sql_table *t, int temp)
{
	sql_allocator *osa;
	sql_schema *s = mvc_bind_schema(sql, sname);
	sql_table *nt = NULL;
	node *n;
	int check = 0;
	const char *action = (temp == SQL_DECLARED_TABLE) ? "DECLARE" : "CREATE";

	if (store_readonly(sql->session->tr->store))
		return sql_error(sql, 06, SQLSTATE(25006) "schema statements cannot be executed on a readonly database.");

	if (!s)
		return sql_message(SQLSTATE(3F000) "%s %s: schema '%s' doesn't exist", action, (t->query) ? "TABLE" : "VIEW", sname);
	if (mvc_bind_table(sql, s, t->base.name)) {
		return sql_message(SQLSTATE(42S01) "%s TABLE: name '%s' already in use", action, t->base.name);
	} else if (temp != SQL_DECLARED_TABLE && (!mvc_schema_privs(sql, s) && !(isTempSchema(s) && temp == SQL_LOCAL_TEMP))) {
		return sql_message(SQLSTATE(42000) "%s TABLE: insufficient privileges for user '%s' in schema '%s'", action, get_string_global_var(sql, "current_user"), s->base.name);
	} else if (temp == SQL_DECLARED_TABLE && ol_length(t->keys)) {
		return sql_message(SQLSTATE(42000) "%s TABLE: '%s' cannot have constraints", action, t->base.name);
	}

	osa = sql->sa;
	sql->sa = sql->ta;

	nt = sql_trans_create_table(sql->session->tr, s, tname, t->query, t->type, t->system, temp, t->commit_action,
								t->sz, t->properties);
	if (!nt)
		return sql_message(SQLSTATE(42000) "%s TABLE: '%s' name conflicts", action, t->base.name);

	/* first check default values */
	for (n = ol_first_node(t->columns); n; n = n->next) {
		sql_column *c = n->data;

		if (c->def) {
			/* TODO please don't place an auto incremented sequence in the default value */
			const char *next_value_for = "next value for \"sys\".\"seq_";
			sql_rel *r = NULL;

			sql->sa = sql->ta;
			r = rel_parse(sql, s, sa_message(sql->ta, "select %s;", c->def), m_deps);
			if (!r || !is_project(r->op) || !r->exps || list_length(r->exps) != 1 ||
				exp_check_type(sql, &c->type, r, r->exps->h->data, type_equal) == NULL) {
				if (r)
					rel_destroy(r);
				sa_reset(sql->ta);
				sql->sa = osa;
				if (strlen(sql->errstr) > 6 && sql->errstr[5] == '!')
					throw(SQL, "sql.catalog", "%s", sql->errstr);
				else
					throw(SQL, "sql.catalog", SQLSTATE(42000) "%s", sql->errstr);
			}
			/* For a self incremented column, it's sequence will get a BEDROPPED_DEPENDENCY,
				so no additional dependencies are needed */
			if (strncmp(c->def, next_value_for, strlen(next_value_for)) != 0) {
				list *id_l = rel_dependencies(sql, r);
				mvc_create_dependencies(sql, id_l, nt->base.id, FUNC_DEPENDENCY);
			}
			rel_destroy(r);
			sa_reset(sql->sa);
		}
	}

	for (n = ol_first_node(t->columns); n; n = n->next) {
		sql_column *c = n->data, *copied = mvc_copy_column(sql, nt, c);

		if (copied == NULL) {
			sa_reset(sql->ta);
			sql->sa = osa;
			throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: %s_%s_%s conflicts", s->base.name, t->base.name, c->base.name);
		}
		if (isPartitionedByColumnTable(t) && c->base.id == t->part.pcol->base.id)
			nt->part.pcol = copied;
	}
	if (isPartitionedByExpressionTable(t)) {
		char *err = NULL;

		_DELETE(nt->part.pexp->exp);
		nt->part.pexp->exp = SA_STRDUP(sql->session->tr->sa, t->part.pexp->exp);
		err = bootstrap_partition_expression(sql, nt, 1);
		sa_reset(sql->ta);
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

	if (t->idxs) {
		for (n = ol_first_node(t->idxs); n; n = n->next) {
			sql_idx *i = n->data;
			if (!mvc_copy_idx(sql, nt, i)) {
				sql->sa = osa;
				throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: %s_%s_%s index conflicts", s->base.name, t->base.name, i->base.name);
			}
		}
	}
	if (t->keys) {
		for (n = ol_first_node(t->keys); n; n = n->next) {
			sql_key *k = n->data;
			char *err = NULL;

			err = sql_partition_validate_key(sql, nt, k, "CREATE");
			sa_reset(sql->ta);
			if (err) {
				sql->sa = osa;
				return err;
			}
			if (!mvc_copy_key(sql, nt, k)) {
				sql->sa = osa;
				throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: %s_%s_%s constraint conflicts", s->base.name, t->base.name, k->base.name);
			}
		}
	}
	if (t->triggers) {
		for (n = ol_first_node(t->triggers); n; n = n->next) {
			sql_trigger *tr = n->data;
			if (mvc_copy_trigger(sql, nt, tr)) {
				sql->sa = osa;
				throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: %s_%s_%s trigger conflicts", s->base.name, t->base.name, nt->base.name);
			}
		}
	}
	/* also create dependencies when not renaming */
	if (nt->query && isView(nt)) {
		sql_rel *r = NULL;

		r = rel_parse(sql, s, nt->query, m_deps);
		if (r)
			r = sql_processrelation(sql, r, 0, 0);
		if (r) {
			list *id_l = rel_dependencies(sql, r);
			mvc_create_dependencies(sql, id_l, nt->base.id, VIEW_DEPENDENCY);
		}
		sa_reset(sql->ta);
		if (!r) {
			sql->sa = osa;
			if (strlen(sql->errstr) > 6 && sql->errstr[5] == '!')
				throw(SQL, "sql.catalog", "%s", sql->errstr);
			else
				throw(SQL, "sql.catalog", SQLSTATE(42000) "%s", sql->errstr);
		}
	}
	sql->sa = osa;
	return MAL_SUCCEED;
}

static size_t
mvc_claim_slots(sql_trans *tr, sql_table *t, size_t cnt)
{
	sqlstore *store = tr->store;
	return store->storage_api.claim_tab(tr, t, cnt);
}

str
mvc_claim_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	lng *res = getArgReference_lng(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	const char *sname = *getArgReference_str(stk, pci, 2);
	const char *tname = *getArgReference_str(stk, pci, 3);
	lng cnt = *(lng*)getArgReference_lng(stk, pci, 4);

	sql_schema *s;
	sql_table *t;

	*res = 0;
	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		throw(SQL, "sql.claim", SQLSTATE(3F000) "Schema missing %s", sname);
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "sql.claim", SQLSTATE(42S02) "Table missing %s.%s", sname, tname);
	*res = mvc_claim_slots(m->session->tr, t, (size_t)cnt);
	return MAL_SUCCEED;
}

str
create_table_from_emit(Client cntxt, char *sname, char *tname, sql_emit_col *columns, size_t ncols)
{
	size_t i, pos = 0;
	sql_table *t;
	sql_schema *s;
	mvc *sql = NULL;
	str msg = MAL_SUCCEED;

	if ((msg = getSQLContext(cntxt, NULL, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if (!sname)
		sname = "sys";
	if (!(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, SQLSTATE(3F000) "CREATE TABLE: no such schema '%s'", sname);
	if (!mvc_schema_privs(sql, s))
		return sql_error(sql, 02, SQLSTATE(42000) "CREATE TABLE: Access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), s->base.name);
	if (!(t = mvc_create_table(sql, s, tname, tt_table, 0, SQL_DECLARED_TABLE, CA_COMMIT, -1, 0)))
		return sql_error(sql, 02, SQLSTATE(3F000) "CREATE TABLE: could not create table '%s'", tname);

	for (i = 0; i < ncols; i++) {
		BAT *b = columns[i].b;
		str atoname = ATOMname(b->ttype);
		sql_subtype tpe;
		sql_column *col = NULL;

		if (!strcmp(atoname, "str"))
			sql_find_subtype(&tpe, "clob", 0, 0);
		else {
			sql_subtype *t = sql_bind_localtype(atoname);
			if (!t)
				return sql_error(sql, 02, SQLSTATE(3F000) "CREATE TABLE: could not find type for column");
			tpe = *t;
		}

		if (columns[i].name && columns[i].name[0] == '%')
			return sql_error(sql, 02, SQLSTATE(42000) "CREATE TABLE: generated labels not allowed in column names, use an alias instead");
		if (!(col = mvc_create_column(sql, t, columns[i].name, &tpe)))
			return sql_error(sql, 02, SQLSTATE(3F000) "CREATE TABLE: could not create column %s", columns[i].name);
	}
	if ((msg = create_table_or_view(sql, sname, t->base.name, t, 0)) != MAL_SUCCEED)
		return msg;
	if (!(t = mvc_bind_table(sql, s, tname)))
		return sql_error(sql, ERR_NOTFOUND, SQLSTATE(3F000) "CREATE TABLE: could not bind table %s", tname);
	pos = mvc_claim_slots(sql->session->tr, t, BATcount(columns[0].b));
	for (i = 0; i < ncols; i++) {
		BAT *b = columns[i].b;
		sql_column *col = NULL;

		if (!(col = mvc_bind_column(sql, t, columns[i].name)))
			return sql_error(sql, ERR_NOTFOUND, SQLSTATE(3F000) "CREATE TABLE: could not bind column %s", columns[i].name);
		if ((msg = mvc_append_column(sql->session->tr, col, pos, b)) != MAL_SUCCEED)
			return msg;
	}

	return msg;
}

str
append_to_table_from_emit(Client cntxt, char *sname, char *tname, sql_emit_col *columns, size_t ncols)
{
	size_t i, pos = 0;
	sql_table *t;
	sql_schema *s;
	mvc *sql = NULL;
	str msg = MAL_SUCCEED;

	if ((msg = getSQLContext(cntxt, NULL, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if (!sname)
		sname = "sys";
	if (!(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, ERR_NOTFOUND, SQLSTATE(3F000) "APPEND TABLE: no such schema '%s'", sname);
	if (!(t = mvc_bind_table(sql, s, tname)))
		return sql_error(sql, ERR_NOTFOUND, SQLSTATE(3F000) "APPEND TABLE: could not bind table %s", tname);
	pos = mvc_claim_slots(sql->session->tr, t, BATcount(columns[0].b));
	for (i = 0; i < ncols; i++) {
		BAT *b = columns[i].b;
		sql_column *col = NULL;

		if (!(col = mvc_bind_column(sql, t, columns[i].name)))
			return sql_error(sql, ERR_NOTFOUND, SQLSTATE(3F000) "APPEND TABLE: could not bind column %s", columns[i].name);
		if ((msg = mvc_append_column(sql->session->tr, col, pos, b)) != MAL_SUCCEED)
			return msg;
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

	sqlstore *store = tr->store;
	b = store->storage_api.bind_col(tr, c, access);
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

/* setVariable(int *ret, str *sname, str *name, any value) */
str
setVariable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res = getArgReference_int(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	const char *sname = *getArgReference_str(stk, pci, 2);
	const char *varname = *getArgReference_str(stk, pci, 3);
	int mtype = getArgType(mb, pci, 4);
	sql_schema *s;
	sql_var *var;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if (!(s = mvc_bind_schema(m, sname)))
		throw(SQL, "sql.setVariable", SQLSTATE(3F000) "Cannot find the schema '%s'", sname);

	*res = 0;
	if (mtype < 0 || mtype >= 255)
		throw(SQL, "sql.setVariable", SQLSTATE(42100) "Variable type error");

	if ((var = find_global_var(m, s, varname))) {
		if (!strcmp("sys", s->base.name) && !strcmp("optimizer", varname)) {
			const char *newopt = *getArgReference_str(stk, pci, 4);
			char buf[18];

			if (strNil(newopt))
				throw(SQL, "sql.setVariable", SQLSTATE(42000) "Variable '%s.%s' cannot be NULL", sname, varname);
			if (!isOptimizerPipe(newopt) && strchr(newopt, (int) ';') == 0)
				throw(SQL, "sql.setVariable", SQLSTATE(42100) "optimizer '%s' unknown", newopt);
			(void) snprintf(buf, sizeof(buf), "user_%d", cntxt->idx); /* should always suffice */
			if (!isOptimizerPipe(newopt) || strcmp(buf, newopt) == 0) {
				if ((msg = addPipeDefinition(cntxt, buf, newopt)))
					return msg;
				if (!sqlvar_set_string(find_global_var(m, s, varname), buf))
					throw(SQL, "sql.setVariable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			} else if (!sqlvar_set_string(find_global_var(m, s, varname), newopt))
				throw(SQL, "sql.setVariable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		} else {
			ValPtr ptr = &stk->stk[getArg(pci, 4)];

			if ((msg = sql_update_var(m, s, varname, ptr)))
				return msg;
			if (!sqlvar_set(var, ptr))
				throw(SQL, "sql.setVariable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		return MAL_SUCCEED;
	}
	throw(SQL, "sql.setVariable", SQLSTATE(42100) "Variable '%s.%s' unknown", sname, varname);
}

/* getVariable(int *ret, str *name) */
str
getVariable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int mtype = getArgType(mb, pci, 0);
	mvc *m = NULL;
	str msg;
	const char *sname = *getArgReference_str(stk, pci, 2);
	const char *varname = *getArgReference_str(stk, pci, 3);
	ValRecord *dst, *src;
	sql_schema *s;
	sql_var *var;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if (!(s = mvc_bind_schema(m, sname)))
		throw(SQL, "sql.getVariable", SQLSTATE(3F000) "Cannot find the schema '%s'", sname);
	if (mtype < 0 || mtype >= 255)
		throw(SQL, "sql.getVariable", SQLSTATE(42100) "Variable type error");
	if (!(var = find_global_var(m, s, varname)))
		throw(SQL, "sql.getVariable", SQLSTATE(42100) "Variable '%s.%s' unknown", sname, varname);
	src = &(var->var.data);
	dst = &stk->stk[getArg(pci, 0)];
	if (VALcopy(dst, src) == NULL)
		throw(MAL, "sql.getVariable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str
sql_variables(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	BAT *schemas, *names, *types, *values;
	str msg = MAL_SUCCEED;
	bat *s = getArgReference_bat(stk,pci,0);
	bat *n = getArgReference_bat(stk,pci,1);
	bat *t = getArgReference_bat(stk,pci,2);
	bat *v = getArgReference_bat(stk,pci,3);
	int nvars;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	nvars = list_length(m->global_vars);
	schemas = COLnew(0, TYPE_str, nvars, TRANSIENT);
	names = COLnew(0, TYPE_str, nvars, TRANSIENT);
	types = COLnew(0, TYPE_str, nvars, TRANSIENT);
	values = COLnew(0, TYPE_str, nvars, TRANSIENT);
	if (!schemas || !names || !types || !values) {
		msg = createException(SQL, "sql.variables", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	if (m->global_vars) {
		for (node *n = m->global_vars->h; n ; n = n->next) {
			sql_var *var = (sql_var*) n->data;
			atom value = var->var;
			ValPtr myptr = &(value.data);
			ValRecord val = (ValRecord) {.vtype = TYPE_void,};
			gdk_return res;

			if (value.tpe.type->localtype != TYPE_str) {
				ptr ok = VALcopy(&val, myptr);
				if (ok)
					ok = VALconvert(TYPE_str, &val);
				if (!ok) {
					VALclear(&val);
					msg = createException(SQL, "sql.variables", SQLSTATE(HY013) "Failed to convert variable '%s.%s' into a string", var->sname, var->name);
					goto bailout;
				}
				myptr = &val;
			}
			res = BUNappend(values, VALget(myptr), false);
			VALclear(&val);
			if (res != GDK_SUCCEED) {
				msg = createException(SQL, "sql.variables", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			if (BUNappend(schemas, var->sname, false) != GDK_SUCCEED) {
				msg = createException(SQL, "sql.variables", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			if (BUNappend(names, var->name, false) != GDK_SUCCEED) {
				msg = createException(SQL, "sql.variables", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			if (BUNappend(types, value.tpe.type->sqlname, false) != GDK_SUCCEED) {
				msg = createException(SQL, "sql.variables", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	if (msg) {
		BBPreclaim(schemas);
		BBPreclaim(names);
		BBPreclaim(types);
		BBPreclaim(values);
	} else {
		BBPkeepref(*s = schemas->batCacheid);
		BBPkeepref(*n = names->batCacheid);
		BBPkeepref(*t = types->batCacheid);
		BBPkeepref(*v = values->batCacheid);
	}
	return msg;
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
			throw(SQL, "sql.logfile", SQLSTATE(HY013) "%s", mnstr_peek_error(NULL));
	}
	return MAL_SUCCEED;
}

/* str mvc_next_value(lng *res, str *sname, str *seqname); */
str
mvc_next_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *be = NULL;
	str msg;
	sql_schema *s;
	sql_sequence *seq;
	lng *res = getArgReference_lng(stk, pci, 0);
	const char *sname = *getArgReference_str(stk, pci, 1);
	const char *seqname = *getArgReference_str(stk, pci, 2);

	(void)mb;
	if ((msg = getBackendContext(cntxt, &be)) != NULL)
		return msg;
	if (!(s = mvc_bind_schema(be->mvc, sname)))
		throw(SQL, "sql.next_value", SQLSTATE(3F000) "Cannot find the schema %s", sname);
	if (!mvc_schema_privs(be->mvc, s))
		throw(SQL, "sql.next_value", SQLSTATE(42000) "Access denied for %s to schema '%s'", get_string_global_var(be->mvc, "current_user"), s->base.name);
	if (!(seq = find_sql_sequence(be->mvc->session->tr, s, seqname)))
		throw(SQL, "sql.next_value", SQLSTATE(HY050) "Cannot find the sequence %s.%s", sname, seqname);

	if (seq_next_value(be->mvc->session->tr->store, seq, res)) {
		be->last_id = *res;
		sqlvar_set_number(find_global_var(be->mvc, mvc_bind_schema(be->mvc, "sys"), "last_id"), be->last_id);
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
	if (!(seq = find_sql_sequence(m->session->tr, s, seqname)))
		throw(SQL, "sql.get_value", SQLSTATE(HY050) "Cannot find the sequence %s.%s", sname, seqname);

	if (seq_get_value(m->session->tr->store, seq, res))
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
	sqlstore *store = m->session->tr->store;

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

	if (b)
		bi = bat_iterator(b);
	if (c)
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
				seqbulk_destroy(store, sb);
				sb = NULL;
			}
			seq = NULL;
			if ((!s || strcmp(s->base.name, nsname) != 0) && !(s = mvc_bind_schema(m, nsname))) {
				msg = createException(SQL, call, SQLSTATE(3F000) "Cannot find the schema %s", nsname);
				goto bailout;
			}
			if (bulk_func == seqbulk_next_value && !mvc_schema_privs(m, s)) {
				msg = createException(SQL, call, SQLSTATE(42000) "Access denied for %s to schema '%s'", get_string_global_var(m, "current_user"), s->base.name);
				goto bailout;
			}
			if (!(seq = find_sql_sequence(m->session->tr, s, nseqname)) || !(sb = seqbulk_create(store, seq, BATcount(it)))) {
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
		seqbulk_destroy(store, sb);
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
		*version = (lng)m->session->tr->ts;
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
		throw(SQL, "sql.restart", SQLSTATE(42000) "Access denied for %s to schema '%s'", get_string_global_var(m, "current_user"), s->base.name);
	if (!(seq = find_sql_sequence(m->session->tr, s, seqname)))
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

	sqlstore *store = m->session->tr->store;
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

	if (b)
		bi = bat_iterator(b);
	if (c)
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
				seqbulk_destroy(store, sb);
				sb = NULL;
			}
			seq = NULL;
			if ((!s || strcmp(s->base.name, nsname) != 0) && !(s = mvc_bind_schema(m, nsname))) {
				msg = createException(SQL, "sql.restart", SQLSTATE(3F000) "Cannot find the schema %s", nsname);
				goto bailout;
			}
			if (!mvc_schema_privs(m, s)) {
				msg = createException(SQL, "sql.restart", SQLSTATE(42000) "Access denied for %s to schema '%s'", get_string_global_var(m, "current_user"), s->base.name);
				goto bailout;
			}
			if (!(seq = find_sql_sequence(m->session->tr, s, nseqname)) || !(sb = seqbulk_create(store, seq, BATcount(it)))) {
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
		seqbulk_destroy(store, sb);
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
	sqlstore *store = tr->store;
	b = store->storage_api.bind_idx(tr, i, access);
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
				BUN l, h;
				psz = cnt ? (cnt / nr_parts) : 0;
				l = part_nr * psz;
				if (l > cnt)
					l = cnt;
				h = (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz);
				if (h > cnt)
					h = cnt;
				bn = BATslice(b, l, h);
				if(bn == NULL) {
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.bind", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
				BAThseqbase(bn, l);
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
				if (l > cnt)
					l = cnt;
				h = (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz);
				if (h > cnt)
					h = cnt;
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
 *  If the table is cleared, the values RDONLY, and RD_UPD_ID and the number of deletes will be 0.
 */

static str
mvc_insert_delta_values(mvc *m, BAT *col1, BAT *col2, BAT *col3, BAT *col4, BAT *col5, BAT *col6, BAT *col7, sql_column *c, bit cleared, lng deletes)
{
	int level = 0;
	sqlstore *store = m->session->tr->store;

	lng inserted = (lng) store->storage_api.count_col(m->session->tr, c, 1);
	lng all = (lng) store->storage_api.count_col(m->session->tr, c, 0);
	lng updates = (lng) store->storage_api.count_col(m->session->tr, c, 2);
	lng readonly = all - inserted;

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
	if (c) {
		for(sql_delta *d = ATOMIC_PTR_GET(&c->data); d; d = d->next)
			level++;
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
		return msg;

	sqlstore *store = m->store;
	sql_trans *tr = m->session->tr;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		goto cleanup;

	if (!(s = mvc_bind_schema(m, sname)))
		throw(SQL, "sql.delta", SQLSTATE(3F000) "No such schema '%s'", sname);

	if (tname) {
		if (!(t = mvc_bind_table(m, s, tname)))
			throw(SQL, "sql.delta", SQLSTATE(3F000) "No such table '%s' in schema '%s'", tname, s->base.name);
		if (!isTable(t))
			throw(SQL, "sql.delta", SQLSTATE(42000) "%s don't have delta values", TABLE_TYPE_DESCRIPTION(t->type, t->properties));
		if (cname) {
			if (!(c = mvc_bind_column(m, t, cname)))
				throw(SQL, "sql.delta", SQLSTATE(3F000) "No such column '%s' in table '%s'", cname, t->base.name);
			nrows = 1;
		} else {
			nrows = (BUN) ol_length(t->columns);
		}
	} else if (s->tables) {
		struct os_iter oi;
		os_iterator(&oi, s->tables, tr, NULL);
		for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
			t = (sql_table *)b;
			if (isTable(t))
				nrows += (BUN) ol_length(t->columns);
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
			cleared = 0;//(t->cleared != 0);
			deletes = (lng) store->storage_api.count_del(m->session->tr, t, 0);
			if (cname) {
				if ((msg=mvc_insert_delta_values(m, col1, col2, col3, col4, col5, col6, col7, c, cleared, deletes)) != NULL)
					goto cleanup;
			} else {
				for (n = ol_first_node(t->columns); n ; n = n->next) {
					c = (sql_column*) n->data;
					if ((msg=mvc_insert_delta_values(m, col1, col2, col3, col4, col5, col6, col7, c, cleared, deletes)) != NULL)
						goto cleanup;
				}
			}
		} else if (s->tables) {
			struct os_iter oi;
			os_iterator(&oi, s->tables, tr, NULL);
			for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
				t = (sql_table *)b;
				if (isTable(t)) {
					cleared = 0;//(t->cleared != 0);
					deletes = (lng) store->storage_api.count_del(m->session->tr, t, 0);

					for (node *nn = ol_first_node(t->columns); nn ; nn = nn->next) {
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
				BUN l, h;
				psz = cnt ? (cnt / nr_parts) : 0;
				l = part_nr * psz;
				if (l > cnt)
					l = cnt;
				h = (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz);
				if (h > cnt)
					h = cnt;
				bn = BATslice(b, l, h);
				if(bn == NULL)
					throw(SQL, "sql.bindidx", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				BAThseqbase(bn, l);
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
				if (l > cnt)
					l = cnt;
				h = (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz);
				if (h > cnt)
					h = cnt;
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
mvc_append_column(sql_trans *t, sql_column *c, size_t pos, BAT *ins)
{
	sqlstore *store = t->store;
	int res = store->storage_api.append_col(t, c, pos, ins, TYPE_bat, 0);
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
	lng pos = *(lng*)getArgReference_lng(stk, pci, 5);
	ptr ins = getArgReference(stk, pci, 6);
	int tpe = getArgType(mb, pci, 6), err = 0;
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
	if (ATOMextern(tpe) && !ATOMvarsized(tpe))
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
	sqlstore *store = m->session->tr->store;
	if (cname[0] != '%' && (c = mvc_bind_column(m, t, cname)) != NULL) {
		if (store->storage_api.append_col(m->session->tr, c, (size_t)pos, ins, tpe, 1) != LOG_OK)
			err = 1;
	} else if (cname[0] == '%') {
		sql_idx *i = mvc_bind_idx(m, s, cname + 1);
		if (i && store->storage_api.append_idx(m->session->tr, i, (size_t)pos, ins, tpe, 1) != LOG_OK)
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
	sqlstore *store = m->session->tr->store;
	if (cname[0] != '%' && (c = mvc_bind_column(m, t, cname)) != NULL) {
		if (store->storage_api.update_col(m->session->tr, c, tids, upd, TYPE_bat) != LOG_OK)
			err = 1;
	} else if (cname[0] == '%') {
		sql_idx *i = mvc_bind_idx(m, s, cname + 1);
		if (i && store->storage_api.update_idx(m->session->tr, i, tids, upd, TYPE_bat) != LOG_OK)
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
	if (tpe != TYPE_bat || (b->ttype != TYPE_oid && b->ttype != TYPE_void && b->ttype != TYPE_msk)) {
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
	sqlstore *store = m->session->tr->store;
	if (store->storage_api.delete_tab(m->session->tr, t, b, tpe) != LOG_OK)
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
DELTAbat(bat *result, const bat *col, const bat *uid, const bat *uval)
{
	BAT *c, *u_id, *u_val, *res;

	if ((u_id = BBPquickdesc(*uid, false)) == NULL)
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	/* no updates */
	if (BATcount(u_id) == 0) {
		BBPretain(*result = *col);
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

	BBPkeepref(*result = res->batCacheid);
	return MAL_SUCCEED;
}

str
DELTAsub(bat *result, const bat *col, const bat *cid, const bat *uid, const bat *uval)
{
	BAT *c, *cminu = NULL, *u_id, *u_val, *u, *res;
	gdk_return ret;

	if ((u_id = BBPquickdesc(*uid, false)) == NULL)
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	/* no updates */
	if (BATcount(u_id) == 0) {
		BBPretain(*result = *col);
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

	BATkey(res, true);
	BBPkeepref(*result = res->batCacheid);
	return MAL_SUCCEED;
}

str
DELTAproject(bat *result, const bat *sub, const bat *col, const bat *uid, const bat *uval)
{
	BAT *s, *c, *u_id, *u_val, *res, *tres;

	if ((s = BATdescriptor(*sub)) == NULL)
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if ((c = BATdescriptor(*col)) == NULL) {
		BBPunfix(s->batCacheid);
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	/* projection(sub,col) */
	res = c;
	tres = BATproject(s, res);
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

	sqlstore *store = m->store;
	/* we have full table count, nr of deleted (unused rows) */
	int part_nr = 0;
	int nr_parts = 1;
	if (pci->argc == 6) {	/* partitioned version */
		part_nr = *getArgReference_int(stk, pci, 4);
		nr_parts = *getArgReference_int(stk, pci, 5);
	}
	BAT *b = store->storage_api.bind_cands(tr, t, nr_parts, part_nr);
	if (b) {
		*res = b->batCacheid;
		BBPkeepref(*res);
	}
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
	backend *be = NULL;
	BAT *b, *tbl, *atr, *tpe,*len,*scale;

	if ((msg = getBackendContext(cntxt, &be)) != NULL)
		return msg;
	bid = *getArgReference_bat(stk,pci,6);
	b = BATdescriptor(bid);
	if ( b == NULL)
		throw(MAL,"sql.resultset", SQLSTATE(HY005) "Cannot access column descriptor");
	res = *res_id = mvc_result_table(be, mb->tag, pci->argc - (pci->retc + 5), Q_TABLE, b);
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
		if ( b == NULL)
			msg= createException(MAL,"sql.resultset",SQLSTATE(HY005) "Cannot access column descriptor ");
		else if (mvc_result_column(be, tblname, colname, tpename, *digits++, *scaledigits++, b))
			msg = createException(SQL, "sql.resultset", SQLSTATE(42000) "Cannot access column descriptor %s.%s",tblname,colname);
		if( b)
			BBPunfix(bid);
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
	backend *be;
	mvc *m = NULL;
	BAT *order = NULL, *b = NULL, *tbl = NULL, *atr = NULL, *tpe = NULL,*len = NULL,*scale = NULL;
	res_table *t = NULL;
	bool tostdout;
	char buf[80];
	ssize_t sz;

	(void) format;

	if ((msg = getBackendContext(cntxt, &be)) != NULL)
		return msg;
	m = be->mvc;

	if (onclient && !cntxt->filetrans) {
		throw(MAL, "sql.resultSet", "cannot transfer files to client");
	}

	bid = *getArgReference_bat(stk,pci,13);
	order = BATdescriptor(bid);
	if ( order == NULL)
		throw(MAL,"sql.resultset", SQLSTATE(HY005) "Cannot access column descriptor");
	res = *res_id = mvc_result_table(be, mb->tag, pci->argc - (pci->retc + 12), Q_TABLE, order);
	t = be->results;
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
		else if (mvc_result_column(be, tblname, colname, tpename, *digits++, *scaledigits++, b))
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
			msg=  createException(IO, "streams.open", SQLSTATE(42000) "%s", mnstr_peek_error(NULL));
			close_stream(s);
			goto wrapup_result_set1;
		}
		be->output_format = OFMT_CSV;
	} else {
		while (!m->scanner.rs->eof)
			bstream_next(m->scanner.rs);
		s = m->scanner.ws;
		mnstr_write(s, PROMPT3, sizeof(PROMPT3) - 1, 1);
		mnstr_printf(s, "w %s\n", filename);
		mnstr_flush(s, MNSTR_FLUSH_DATA);
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
		mnstr_flush(s, MNSTR_FLUSH_DATA);
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
	backend *be = NULL;
	ptr v;
	int mtype;
	BAT  *tbl, *atr, *tpe,*len,*scale;

	if ((msg = getBackendContext(cntxt, &be)) != NULL)
		return msg;
	res = *res_id = mvc_result_table(be, mb->tag, pci->argc - (pci->retc + 5), Q_TABLE, NULL);
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
		if (mvc_result_value(be, tblname, colname, tpename, *digits++, *scaledigits++, v, mtype))
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
	backend *be;
	mvc *m = NULL;
	res_table *t = NULL;
	ptr v;
	int mtype;
	BAT  *tbl = NULL, *atr = NULL, *tpe = NULL,*len = NULL,*scale = NULL;
	bool tostdout;
	char buf[80];
	ssize_t sz;

	(void) format;
	if ((msg = getBackendContext(cntxt, &be)) != NULL)
		return msg;
	m = be->mvc;
	if (onclient && !cntxt->filetrans) {
		throw(MAL, "sql.resultSet", "cannot transfer files to client");
	}

	res = *res_id = mvc_result_table(be, mb->tag, pci->argc - (pci->retc + 12), Q_TABLE, NULL);

	t = be->results;
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
		if (mvc_result_value(be, tblname, colname, tpename, *digits++, *scaledigits++, v, mtype))
			throw(SQL, "sql.rsColumn", SQLSTATE(45000) "Result set construction failed");
	}
	/* now select the file channel */
	if ((tostdout = strcmp(filename,"stdout") == 0)) {
		s = cntxt->fdout;
	} else if (!onclient) {
		if ((s = open_wastream(filename)) == NULL || mnstr_errnr(s)) {
			msg=  createException(IO, "streams.open", SQLSTATE(42000) "%s", mnstr_peek_error(NULL));
			close_stream(s);
			goto wrapup_result_set;
		}
	} else {
		while (!m->scanner.rs->eof)
			bstream_next(m->scanner.rs);
		s = m->scanner.ws;
		mnstr_write(s, PROMPT3, sizeof(PROMPT3) - 1, 1);
		mnstr_printf(s, "w %s\n", filename);
		mnstr_flush(s, MNSTR_FLUSH_DATA);
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
		mnstr_flush(s, MNSTR_FLUSH_DATA);
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
	backend *be = NULL;
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

	if ((msg = getBackendContext(cntxt, &be)) != NULL)
		return msg;
	if ((order = BATdescriptor(order_bid)) == NULL) {
		throw(SQL, "sql.resultSet", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	*res_id = mvc_result_table(be, mb->tag, nr_cols, qtype, order);
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
	backend *be = NULL;
	int res_id;
	(void) mb;		/* NOT USED */
	if ((msg = getBackendContext(cntxt, &be)) != NULL)
		return msg;
	if (ATOMextern(mtype))
		p = *(ptr *) p;

	// scalar values are single-column result sets
	if ((res_id = mvc_result_table(be, mb->tag, 1, Q_TABLE, NULL)) < 0)
		throw(SQL, "sql.exportValue", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if (mvc_result_value(be, tn, cn, type, digits, scale, p, mtype))
		throw(SQL, "sql.exportValue", SQLSTATE(45000) "Result set construction failed");
	if (be->output_format == OFMT_NONE) {
		return MAL_SUCCEED;
	}
	if (mvc_export_result(be, be->out, res_id, true, mb->starttime, mb->optimize) < 0) {
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

static char fwftsep[2] = {STREAM_FWF_FIELD_SEP, '\0'};
static char fwfrsep[2] = {STREAM_FWF_RECORD_SEP, '\0'};

/* str mvc_import_table_wrap(int *res, sql_table **t, unsigned char* *T, unsigned char* *R, unsigned char* *S, unsigned char* *N, str *fname, lng *sz, lng *offset, int *besteffort, str *fixed_width, int *onclient, int *escape); */
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
	int besteffort = *getArgReference_int(stk, pci, pci->retc + 8);
	char *fixed_widths = *getArgReference_str(stk, pci, pci->retc + 9);
	int onclient = *getArgReference_int(stk, pci, pci->retc + 10);
	bool escape = *getArgReference_int(stk, pci, pci->retc + 11);
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
		msg = mvc_import_table(cntxt, &b, be->mvc, be->mvc->scanner.rs, t, tsep, rsep, ssep, ns, sz, offset, besteffort, true, escape);
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
			mnstr_flush(be->mvc->scanner.ws, MNSTR_FLUSH_DATA);
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
				msg = createException(IO, "sql.copy_from", SQLSTATE(42000) "%s", mnstr_peek_error(NULL));
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
			msg = mvc_import_table(cntxt, &b, be->mvc, s, t, tsep, rsep, ssep, ns, sz, offset, besteffort, false, escape);
			if (onclient) {
				mnstr_write(be->mvc->scanner.ws, PROMPT3, sizeof(PROMPT3)-1, 1);
				mnstr_flush(be->mvc->scanner.ws, MNSTR_FLUSH_DATA);
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
		if (BUNappend(query, q->f->query, false) != GDK_SUCCEED ||
		    BUNappend(count, &q->count, false) != GDK_SUCCEED) {
			BBPunfix(query->batCacheid);
			BBPunfix(count->batCacheid);
			throw(SQL, "sql.dumpcache", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
	backend *be;
	str msg;
	int cnt;
	BAT *rewrite, *count;
	bat *rrewrite = getArgReference_bat(stk, pci, 0);
	bat *rcount = getArgReference_bat(stk, pci, 1);

	(void)mb;
	if ((msg = getBackendContext(cntxt, &be)) != NULL)
		return msg;
	cnt = be->mvc->qc->id;
	rewrite = COLnew(0, TYPE_str, cnt, TRANSIENT);
	count = COLnew(0, TYPE_int, cnt, TRANSIENT);
	if (rewrite == NULL || count == NULL) {
		BBPreclaim(rewrite);
		BBPreclaim(count);
		throw(SQL, "sql.optstats", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	if (BUNappend(rewrite, "joinidx", false) != GDK_SUCCEED ||
	    BUNappend(count, &be->join_idx, false) != GDK_SUCCEED) {
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

static str
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
	str uris = NULL;
	str unames = NULL;
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
	if (BUNappend(urib, uris? uris: str_nil, false) != GDK_SUCCEED)
		goto lbailout;
	if (BUNappend(unameb, unames? unames: str_nil , false) != GDK_SUCCEED)
		goto lbailout;
	if (BUNappend(hashb, hashs? hashs: str_nil, false) != GDK_SUCCEED)
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
	if (!s || !t || !ol_first_node(t->columns))
		throw(SQL, "calc.rowid", SQLSTATE(42S22) "Column missing %s.%s",sname,tname);
	c = ol_first_node(t->columns)->data;
	/* HACK, get insert bat */
	sqlstore *store = m->session->tr->store;
	b = store->storage_api.bind_col(m->session->tr, c, RDONLY);
	if( b == NULL)
		throw(SQL,"sql.rowid", SQLSTATE(HY005) "Cannot access column descriptor");
	/* UGH (move into storage backends!!) */
	*rid = BATcount(b);
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
		throw(SQL, "sql.drop_hash", SQLSTATE(42000) "Access denied for %s to schema '%s'", get_string_global_var(m, "current_user"), s->base.name);
	t = mvc_bind_table(m, s, tbl);
	if (t == NULL)
		throw(SQL, "sql.drop_hash", SQLSTATE(42S02) "Table missing %s.%s",sch, tbl);
	if (!isTable(t))
		throw(SQL, "sql.drop_hash", SQLSTATE(42000) "%s '%s' is not persistent",
			  TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);

	sqlstore *store = m->session->tr->store;
	for (o = ol_first_node(t->columns); o; o = o->next) {
		c = o->data;
		b = store->storage_api.bind_col(m->session->tr, c, RDONLY);
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
	node *ncol;
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
	struct os_iter si = {0};

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	tr = m->session->tr;
	sqlstore *store = tr->store;
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
	os_iterator(&si, tr->cat->schemas, tr, NULL);
	for (sql_base *b = oi_next(&si); b; b = oi_next(&si)) {
		sql_schema *s = (sql_schema *) b;
		if( sname && strcmp(b->name, sname) )
			continue;
		if (isalpha((unsigned char) b->name[0]))
			if (s->tables) {
				struct os_iter oi;

				os_iterator(&oi, s->tables, tr, NULL);
				for (sql_base *bt = oi_next(&oi); bt; bt = oi_next(&oi)) {
					sql_table *t = (sql_table *) bt;
					if( tname && strcmp(bt->name, tname) )
						continue;
					if (isTable(t))
						if (ol_first_node(t->columns))
							for (ncol = ol_first_node((t)->columns); ncol; ncol = ncol->next) {
								sql_base *bc = ncol->data;
								sql_column *c = (sql_column *) ncol->data;
								BAT *bn;
								lng sz;

								if( cname && strcmp(bc->name, cname) )
									continue;
								bn = store->storage_api.bind_col(tr, c, RDONLY);
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
						if (t->idxs)
							for (ncol = ol_first_node((t)->idxs); ncol; ncol = ncol->next) {
								sql_base *bc = ncol->data;
								sql_idx *c = (sql_idx *) ncol->data;
								if (idx_has_column(c->type)) {
									BAT *bn = store->storage_api.bind_idx(tr, c, RDONLY);
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
freeVariables(Client c, MalBlkPtr mb, MalStkPtr glb, int oldvtop, int oldvid)
{
	for (int i = oldvtop; i < mb->vtop;) {
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
	mb->vtop = oldvtop;
	mb->vid = oldvid;
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
		HEAPdecref(r->tvheap, false);
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
		HEAPdecref(r->tvheap, false);
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
		HEAPdecref(r->tvheap, false);
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
SQLresume_log_flushing(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *mvc;

	(void)stk; (void)pci;
	char *msg = getSQLContext(cntxt, mb, &mvc, NULL);
	if (msg)
		return msg;
	store_resume_log(mvc->store);
	return MAL_SUCCEED;
}

str
SQLsuspend_log_flushing(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *mvc;

	(void)stk; (void)pci;
	char *msg = getSQLContext(cntxt, mb, &mvc, NULL);
	if (msg)
		return msg;
	store_suspend_log(mvc->store);
	return MAL_SUCCEED;
}

str
/*SQLhot_snapshot(void *ret, const str *tarfile_arg)*/
SQLhot_snapshot(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char *tarfile = *getArgReference_str(stk, pci, 1);
	mvc *mvc;

	char *msg = getSQLContext(cntxt, mb, &mvc, NULL);
	if (msg)
		return msg;
	lng result = store_hot_snapshot(mvc->session->tr->store, tarfile);
	if (result)
		return MAL_SUCCEED;
	else
		throw(SQL, "sql.hot_snapshot", GDK_EXCEPTION);
}

str
SQLhot_snapshot_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char *filename;
	bool onserver;
	char *msg = MAL_SUCCEED;
	char buf[80];
	mvc *mvc;
	ssize_t sz;
	stream *s;
	stream *cb = NULL;
	lng result;

	filename = *getArgReference_str(stk, pci, 1);
	onserver = *getArgReference_bit(stk, pci, 2);

	msg = getSQLContext(cntxt, mb, &mvc, NULL);
	if (msg)
		return msg;

	sqlstore *store = mvc->session->tr->store;
	if (onserver) {
		lng result = store_hot_snapshot(store, filename);
		if (result)
			return MAL_SUCCEED;
		else
			throw(SQL, "sql.hot_snapshot", GDK_EXCEPTION);
	}

	// sync with client, copy pasted from mvc_export_table_wrap
	while (!mvc->scanner.rs->eof)
		bstream_next(mvc->scanner.rs);

	// The snapshot code flushes from time to time.
	// Use a callback stream to suppress those.
	s = mvc->scanner.ws;
	cb = callback_stream(
		/* private */ s,
		/* read */    NULL,
		/* write */   (void*)mnstr_write,
		/* close */   NULL,
		/* destroy */ NULL,
		"snapshot-callback"
	);
	if (!cb)
		throw(SQL, "sql.hot_snapshot", GDK_EXCEPTION);

	// tell client to open file, copy pasted from mvc_export_table_wrap
	mnstr_write(s, PROMPT3, sizeof(PROMPT3) - 1, 1);
	mnstr_printf(s, "w %s\n", filename);
	mnstr_flush(s, MNSTR_FLUSH_DATA);
	if ((sz = mnstr_readline(mvc->scanner.rs->s, buf, sizeof(buf))) > 1) {
		/* non-empty line indicates failure on client */
		msg = createException(IO, "streams.open", "%s", buf);
		/* deal with ridiculously long response from client */
		while (buf[sz - 1] != '\n' &&
				(sz = mnstr_readline(mvc->scanner.rs->s, buf, sizeof(buf))) > 0)
			;
		goto end;
	}

	// client is waiting for data now, send it.
	result = store_hot_snapshot_to_stream(store, cb);
	if (result)
		msg = MAL_SUCCEED;
	else
		msg = createException(SQL, "sql.hot_snapshot", GDK_EXCEPTION);
	mnstr_destroy(cb);

	// tell client no more data, also copy pasted from mvc_export_table_wrap
	mnstr_flush(s, MNSTR_FLUSH_DATA);
	if ((sz = mnstr_readline(mvc->scanner.rs->s, buf, sizeof(buf))) > 1) {
		msg = createException(IO, "streams.open", "%s", buf);
	}
	while (sz > 0)
		sz = mnstr_readline(mvc->scanner.rs->s, buf, sizeof(buf));

end:
	return msg;
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
		if (BUNappend(statement, q->f->query, false) != GDK_SUCCEED) {
			msg = createException(SQL, "sql.session_prepared_statements", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		if (BUNappend(created, &(q->created), false) != GDK_SUCCEED) {
			msg = createException(SQL, "sql.session_prepared_statements", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
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

				if (BUNappend(statementid, &(q->id), false) != GDK_SUCCEED ||
					BUNappend(type, t->type->sqlname, false) != GDK_SUCCEED ||
					BUNappend(digits, &t->digits, false) != GDK_SUCCEED ||
					BUNappend(scale, &t->scale, false) != GDK_SUCCEED ||
					BUNappend(isinout, &inout, false) != GDK_SUCCEED ||
					BUNappend(number, &arg_number, false) != GDK_SUCCEED ||
					BUNappend(schema, rschema, false) != GDK_SUCCEED ||
					BUNappend(table, rname, false) != GDK_SUCCEED ||
					BUNappend(column, name, false) != GDK_SUCCEED) {
					msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
			}
		}

		if (q->f->ops) {
			inout = ARG_IN;
			for (node *n = q->f->ops->h; n; n=n->next, arg_number++) {
				sql_arg *a = n->data;
				sql_subtype *t = &a->type;

				if (BUNappend(statementid, &(q->id), false) != GDK_SUCCEED ||
					BUNappend(type, t->type->sqlname, false) != GDK_SUCCEED ||
					BUNappend(digits, &(t->digits), false) != GDK_SUCCEED ||
					BUNappend(scale, &(t->scale), false) != GDK_SUCCEED ||
					BUNappend(isinout, &inout, false) != GDK_SUCCEED ||
					BUNappend(number, &arg_number, false) != GDK_SUCCEED ||
					BUNappend(schema, ATOMnilptr(TYPE_str), false) != GDK_SUCCEED ||
					BUNappend(table, ATOMnilptr(TYPE_str), false) != GDK_SUCCEED ||
					BUNappend(column, ATOMnilptr(TYPE_str), false) != GDK_SUCCEED) {
					msg = createException(SQL, "sql.session_prepared_statements_args", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
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
	npci = newStmtArgs(mb, mod, fcn, pci->argc);

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

#include "wlr.h"
#include "sql_cat.h"
#include "sql_rank.h"
#include "sql_user.h"
#include "sql_assert.h"
#include "sql_execute.h"
#include "sql_orderidx.h"
#include "sql_subquery.h"
#include "sql_statistics.h"
#include "sql_transaction.h"
#include "mel.h"
static mel_func sql_init_funcs[] = {
 pattern("sql", "shutdown", SQLshutdown_wrap, false, "", args(1,3, arg("",str),arg("delay",bte),arg("force",bit))),
 pattern("sql", "shutdown", SQLshutdown_wrap, false, "", args(1,3, arg("",str),arg("delay",sht),arg("force",bit))),
 pattern("sql", "shutdown", SQLshutdown_wrap, false, "", args(1,3, arg("",str),arg("delay",int),arg("force",bit))),
 pattern("sql", "shutdown", SQLshutdown_wrap, false, "", args(1,2, arg("",str),arg("delay",bte))),
 pattern("sql", "shutdown", SQLshutdown_wrap, false, "", args(1,2, arg("",str),arg("delay",sht))),
 pattern("sql", "shutdown", SQLshutdown_wrap, false, "", args(1,2, arg("",str),arg("delay",int))),
 pattern("sql", "set_protocol", SQLset_protocol, true, "Configures the result set protocol", args(1,2, arg("",int), arg("protocol",int))),
 pattern("sql", "mvc", SQLmvc, false, "Get the multiversion catalog context. \nNeeded for correct statement dependencies\n(ie sql.update, should be after sql.bind in concurrent execution)", args(1,1, arg("",int))),
 pattern("sql", "transaction", SQLtransaction2, true, "Start an autocommit transaction", noargs),
 pattern("sql", "commit", SQLcommit, true, "Trigger the commit operation for a MAL block", noargs),
 pattern("sql", "abort", SQLabort, true, "Trigger the abort operation for a MAL block", noargs),
 pattern("sql", "eval", SQLstatement, false, "Compile and execute a single sql statement", args(1,2, arg("",void),arg("cmd",str))),
 pattern("sql", "eval", SQLstatement, false, "Compile and execute a single sql statement (and optionaly set the output to columnar format)", args(1,3, arg("",void),arg("cmd",str),arg("columnar",bit))),
 pattern("sql", "include", SQLinclude, false, "Compile and execute a sql statements on the file", args(1,2, arg("",void),arg("fname",str))),
 pattern("sql", "evalAlgebra", RAstatement, false, "Compile and execute a single 'relational algebra' statement", args(1,3, arg("",void),arg("cmd",str),arg("optimize",bit))),
 pattern("sql", "register", RAstatement2, false, "", args(1,5, arg("",int),arg("mod",str),arg("fname",str),arg("rel_stmt",str),arg("sig",str))),
 pattern("sql", "register", RAstatement2, false, "Compile the relational statement (rel_smt) and register it as mal function, mod.fname(signature)", args(1,6, arg("",int),arg("mod",str),arg("fname",str),arg("rel_stmt",str),arg("sig",str),arg("typ",str))),
 pattern("sql", "hot_snapshot", SQLhot_snapshot, true, "Write db snapshot to the given tar(.gz) file", args(1,2, arg("",void),arg("tarfile",str))),
 pattern("sql", "resume_log_flushing", SQLresume_log_flushing, true, "Resume WAL log flushing", args(1,1, arg("",void))),
 pattern("sql", "suspend_log_flushing", SQLsuspend_log_flushing, true, "Suspend WAL log flushing", args(1,1, arg("",void))),
 pattern("sql", "hot_snapshot", SQLhot_snapshot_wrap, true, "Write db snapshot to the given tar(.gz/.lz4/.bz/.xz) file on either server or client", args(1,3, arg("",void),arg("tarfile", str),arg("onserver",bit))),
 pattern("sql", "assert", SQLassert, false, "Generate an exception when b==true", args(1,3, arg("",void),arg("b",bit),arg("msg",str))),
 pattern("sql", "assert", SQLassertInt, false, "Generate an exception when b!=0", args(1,3, arg("",void),arg("b",int),arg("msg",str))),
 pattern("sql", "assert", SQLassertLng, false, "Generate an exception when b!=0", args(1,3, arg("",void),arg("b",lng),arg("msg",str))),
 pattern("sql", "setVariable", setVariable, true, "Set the value of a session variable", args(1,5, arg("",int),arg("mvc",int),arg("sname",str),arg("varname",str),argany("value",1))),
 pattern("sql", "getVariable", getVariable, false, "Get the value of a session variable", args(1,4, argany("",1),arg("mvc",int),arg("sname",str),arg("varname",str))),
 pattern("sql", "logfile", mvc_logfile, true, "Enable/disable saving the sql statement traces", args(1,2, arg("",void),arg("filename",str))),
 pattern("sql", "next_value", mvc_next_value, false, "return the next value of the sequence", args(1,3, arg("",lng),arg("sname",str),arg("sequence",str))),
 pattern("batsql", "next_value", mvc_bat_next_value, false, "return the next value of the sequence", args(1,3, batarg("",lng),batarg("sname",str),arg("sequence",str))),
 pattern("batsql", "next_value", mvc_bat_next_value, false, "return the next value of sequences", args(1,3, batarg("",lng),arg("sname",str),batarg("sequence",str))),
 pattern("batsql", "next_value", mvc_bat_next_value, false, "return the next value of sequences", args(1,3, batarg("",lng),batarg("sname",str),batarg("sequence",str))),
 pattern("sql", "get_value", mvc_get_value, false, "return the current value of the sequence", args(1,3, arg("",lng),arg("sname",str),arg("sequence",str))),
 pattern("batsql", "get_value", mvc_bat_get_value, false, "return the current value of the sequence", args(1,3, batarg("",lng),batarg("sname",str),arg("sequence",str))),
 pattern("batsql", "get_value", mvc_bat_get_value, false, "return the current value of sequences", args(1,3, batarg("",lng),arg("sname",str),batarg("sequence",str))),
 pattern("batsql", "get_value", mvc_bat_get_value, false, "return the current value of sequences", args(1,3, batarg("",lng),batarg("sname",str),batarg("sequence",str))),
 pattern("sql", "restart", mvc_restart_seq, true, "restart the sequence with value start", args(1,4, arg("",lng),arg("sname",str),arg("sequence",str),arg("start",lng))),
 pattern("batsql", "restart", mvc_bat_restart_seq, true, "restart the sequence with value start", args(1,4, batarg("",lng),batarg("sname",str),arg("sequence",str),arg("start",lng))),
 pattern("batsql", "restart", mvc_bat_restart_seq, true, "restart the sequence with value start", args(1,4, batarg("",lng),arg("sname",str),batarg("sequence",str),arg("start",lng))),
 pattern("batsql", "restart", mvc_bat_restart_seq, true, "restart the sequence with value start", args(1,4, batarg("",lng),arg("sname",str),arg("sequence",str),batarg("start",lng))),
 pattern("batsql", "restart", mvc_bat_restart_seq, true, "restart the sequence with value start", args(1,4, batarg("",lng),batarg("sname",str),batarg("sequence",str),arg("start",lng))),
 pattern("batsql", "restart", mvc_bat_restart_seq, true, "restart the sequence with value start", args(1,4, batarg("",lng),batarg("sname",str),arg("sequence",str),batarg("start",lng))),
 pattern("batsql", "restart", mvc_bat_restart_seq, true, "restart the sequence with value start", args(1,4, batarg("",lng),arg("sname",str),batarg("sequence",str),batarg("start",lng))),
 pattern("batsql", "restart", mvc_bat_restart_seq, true, "restart the sequence with value start", args(1,4, batarg("",lng),batarg("sname",str),batarg("sequence",str),batarg("start",lng))),
 pattern("sql", "deltas", mvc_delta_values, false, "Return the delta values sizes of all columns of the schema's tables, plus the current transaction level", args(7,8, batarg("ids",int),batarg("cleared",bit),batarg("readonly",lng),batarg("inserted",lng),batarg("updated",lng),batarg("deleted",lng),batarg("tr_level",int),arg("schema",str))),
 pattern("sql", "deltas", mvc_delta_values, false, "Return the delta values sizes from the table's columns, plus the current transaction level", args(7,9, batarg("ids",int),batarg("cleared",bit),batarg("readonly",lng),batarg("inserted",lng),batarg("updated",lng),batarg("deleted",lng),batarg("tr_level",int),arg("schema",str),arg("table",str))),
 pattern("sql", "deltas", mvc_delta_values, false, "Return the delta values sizes of a column, plus the current transaction level", args(7,10, batarg("ids",int),batarg("cleared",bit),batarg("readonly",lng),batarg("inserted",lng),batarg("updated",lng),batarg("deleted",lng),batarg("tr_level",int),arg("schema",str),arg("table",str),arg("column",str))),
 pattern("sql", "emptybindidx", mvc_bind_idxbat_wrap, false, "", args(1,6, batargany("",1),arg("mvc",int),arg("schema",str),arg("table",str),arg("index",str),arg("access",int))),
 pattern("sql", "bind_idxbat", mvc_bind_idxbat_wrap, false, "Bind the 'schema.table.index' BAT with access kind:\n0 - base table\n1 - inserts\n2 - updates", args(1,6, batargany("",1),arg("mvc",int),arg("schema",str),arg("table",str),arg("index",str),arg("access",int))),
 pattern("sql", "emptybindidx", mvc_bind_idxbat_wrap, false, "", args(2,7, batarg("uid",oid),batargany("uval",1),arg("mvc",int),arg("schema",str),arg("table",str),arg("index",str),arg("access",int))),
 pattern("sql", "bind_idxbat", mvc_bind_idxbat_wrap, false, "Bind the 'schema.table.index' BAT with access kind:\n0 - base table\n1 - inserts\n2 - updates", args(2,7, batarg("uid",oid),batargany("uval",1),arg("mvc",int),arg("schema",str),arg("table",str),arg("index",str),arg("access",int))),
 pattern("sql", "emptybindidx", mvc_bind_idxbat_wrap, false, "", args(1,8, batargany("",1),arg("mvc",int),arg("schema",str),arg("table",str),arg("index",str),arg("access",int),arg("part_nr",int),arg("nr_parts",int))),
 pattern("sql", "bind_idxbat", mvc_bind_idxbat_wrap, false, "Bind the 'schema.table.index' BAT with access kind:\n0 - base table\n1 - inserts\n2 - updates", args(1,8, batargany("",1),arg("mvc",int),arg("schema",str),arg("table",str),arg("index",str),arg("access",int),arg("part_nr",int),arg("nr_parts",int))),
 pattern("sql", "emptybindidx", mvc_bind_idxbat_wrap, false, "", args(2,9, batarg("uid",oid),batargany("uval",1),arg("mvc",int),arg("schema",str),arg("table",str),arg("index",str),arg("access",int),arg("part_nr",int),arg("nr_parts",int))),
 pattern("sql", "bind_idxbat", mvc_bind_idxbat_wrap, false, "Bind the 'schema.table.index' BAT with access kind:\n0 - base table\n1 - inserts\n2 - updates", args(2,9, batarg("uid",oid),batargany("uval",1),arg("mvc",int),arg("schema",str),arg("table",str),arg("index",str),arg("access",int),arg("part_nr",int),arg("nr_parts",int))),
 pattern("sql", "emptybind", mvc_bind_wrap, false, "", args(1,6, batargany("",1),arg("mvc",int),arg("schema",str),arg("table",str),arg("column",str),arg("access",int))),
 pattern("sql", "bind", mvc_bind_wrap, false, "Bind the 'schema.table.column' BAT with access kind:\n0 - base table\n1 - inserts\n2 - updates", args(1,6, batargany("",1),arg("mvc",int),arg("schema",str),arg("table",str),arg("column",str),arg("access",int))),
 pattern("sql", "emptybind", mvc_bind_wrap, false, "", args(2,7, batarg("uid",oid),batargany("uval",1),arg("mvc",int),arg("schema",str),arg("table",str),arg("column",str),arg("access",int))),
 pattern("sql", "bind", mvc_bind_wrap, false, "Bind the 'schema.table.column' BAT with access kind:\n0 - base table\n1 - inserts\n2 - updates", args(2,7, batarg("uid",oid),batargany("uval",1),arg("mvc",int),arg("schema",str),arg("table",str),arg("column",str),arg("access",int))),
 pattern("sql", "emptybind", mvc_bind_wrap, false, "", args(1,8, batargany("",1),arg("mvc",int),arg("schema",str),arg("table",str),arg("column",str),arg("access",int),arg("part_nr",int),arg("nr_parts",int))),
 pattern("sql", "bind", mvc_bind_wrap, false, "Bind the 'schema.table.column' BAT partition with access kind:\n0 - base table\n1 - inserts\n2 - updates", args(1,8, batargany("",1),arg("mvc",int),arg("schema",str),arg("table",str),arg("column",str),arg("access",int),arg("part_nr",int),arg("nr_parts",int))),
 pattern("sql", "emptybind", mvc_bind_wrap, false, "", args(2,9, batarg("uid",oid),batargany("uval",1),arg("mvc",int),arg("schema",str),arg("table",str),arg("column",str),arg("access",int),arg("part_nr",int),arg("nr_parts",int))),
 pattern("sql", "bind", mvc_bind_wrap, false, "Bind the 'schema.table.column' BAT with access kind:\n0 - base table\n1 - inserts\n2 - updates", args(2,9, batarg("uid",oid),batargany("uval",1),arg("mvc",int),arg("schema",str),arg("table",str),arg("column",str),arg("access",int),arg("part_nr",int),arg("nr_parts",int))),
 command("sql", "delta", DELTAbat, false, "Return column bat with delta's applied.", args(1,4, batargany("",3),batargany("col",3),batarg("uid",oid),batargany("uval",3))),
 command("sql", "projectdelta", DELTAproject, false, "Return column bat with delta's applied.", args(1,5, batargany("",3),batarg("select",oid),batargany("col",3),batarg("uid",oid),batargany("uval",3))),
 command("sql", "subdelta", DELTAsub, false, "Return a single bat of selected delta.", args(1,5, batarg("",oid),batarg("col",oid),batarg("cand",oid),batarg("uid",oid),batarg("uval",oid))),
 command("sql", "project", BATleftproject, false, "Last step of a left outer join, ie project the inner join (l,r) over the left input side (col)", args(1,4, batarg("",oid),batarg("col",oid),batarg("l",oid),batarg("r",oid))),
 command("sql", "getVersion", mvc_getVersion, false, "Return the database version identifier for a client.", args(1,2, arg("",lng),arg("clientid",int))),
 pattern("sql", "grow", mvc_grow_wrap, false, "Resize the tid column of a declared table.", args(1,3, arg("",int),batarg("tid",oid),argany("",1))),
 pattern("sql", "claim", mvc_claim_wrap, true, "Claims slots for appending rows.", args(1,5, arg("",lng),arg("mvc",int),arg("sname",str),arg("tname",str),arg("cnt",lng))),
 pattern("sql", "append", mvc_append_wrap, false, "Append to the column tname.cname (possibly optimized to replace the insert bat of tname.cname. Returns sequence number for order dependence.", args(1,7, arg("",int), arg("mvc",int),arg("sname",str),arg("tname",str),arg("cname",str),arg("offset",lng),argany("ins",0))),
 pattern("sql", "update", mvc_update_wrap, false, "Update the values of the column tname.cname. Returns sequence number for order dependence)", args(1,7, arg("",int), arg("mvc",int),arg("sname",str),arg("tname",str),arg("cname",str),argany("rids",0),argany("upd",0))), pattern("sql", "clear_table", mvc_clear_table_wrap, true, "Clear the table sname.tname.", args(1,3, arg("",lng),arg("sname",str),arg("tname",str))),
 pattern("sql", "tid", SQLtid, false, "Return a column with the valid tuple identifiers associated with the table sname.tname.", args(1,4, batarg("",oid),arg("mvc",int),arg("sname",str),arg("tname",str))),
 pattern("sql", "tid", SQLtid, false, "Return the tables tid column.", args(1,6, batarg("",oid),arg("mvc",int),arg("sname",str),arg("tname",str),arg("part_nr",int),arg("nr_parts",int))),
 pattern("sql", "delete", mvc_delete_wrap, true, "Delete a row from a table. Returns sequence number for order dependence.", args(1,5, arg("",int),arg("mvc",int),arg("sname",str),arg("tname",str),argany("b",0))),
 pattern("sql", "resultSet", mvc_scalar_value_wrap, true, "Prepare a table result set for the client front-end.", args(1,8, arg("",int),arg("tbl",str),arg("attr",str),arg("tpe",str),arg("len",int),arg("scale",int),arg("eclass",int),argany("val",0))),
 pattern("sql", "resultSet", mvc_row_result_wrap, true, "Prepare a table result set for the client front-end", args(1,7, arg("",int),batarg("tbl",str),batarg("attr",str),batarg("tpe",str),batarg("len",int),batarg("scale",int),varargany("cols",0))),
 pattern("sql", "resultSet", mvc_table_result_wrap, true, "Prepare a table result set for the client in default CSV format", args(1,7, arg("",int),batarg("tbl",str),batarg("attr",str),batarg("tpe",str),batarg("len",int),batarg("scale",int),batvarargany("cols",0))),
 pattern("sql", "export_table", mvc_export_row_wrap, true, "Prepare a table result set for the COPY INTO stream", args(1,14, arg("",int),arg("fname",str),arg("fmt",str),arg("colsep",str),arg("recsep",str),arg("qout",str),arg("nullrep",str),arg("onclient",int),batarg("tbl",str),batarg("attr",str),batarg("tpe",str),batarg("len",int),batarg("scale",int),varargany("cols",0))),
 pattern("sql", "export_table", mvc_export_table_wrap, true, "Prepare a table result set for the COPY INTO stream", args(1,14, arg("",int),arg("fname",str),arg("fmt",str),arg("colsep",str),arg("recsep",str),arg("qout",str),arg("nullrep",str),arg("onclient",int),batarg("tbl",str),batarg("attr",str),batarg("tpe",str),batarg("len",int),batarg("scale",int),batvarargany("cols",0))),
 pattern("sql", "exportHead", mvc_export_head_wrap, true, "Export a result (in order) to stream s", args(1,3, arg("",void),arg("s",streams),arg("res_id",int))),
 pattern("sql", "exportResult", mvc_export_result_wrap, true, "Export a result (in order) to stream s", args(1,3, arg("",void),arg("s",streams),arg("res_id",int))),
 pattern("sql", "exportChunk", mvc_export_chunk_wrap, true, "Export a chunk of the result set (in order) to stream s", args(1,3, arg("",void),arg("s",streams),arg("res_id",int))),
 pattern("sql", "exportChunk", mvc_export_chunk_wrap, true, "Export a chunk of the result set (in order) to stream s", args(1,5, arg("",void),arg("s",streams),arg("res_id",int),arg("offset",int),arg("nr",int))),
 pattern("sql", "exportOperation", mvc_export_operation_wrap, true, "Export result of schema/transaction queries", args(1,1, arg("",void))),
 pattern("sql", "affectedRows", mvc_affected_rows_wrap, true, "export the number of affected rows by the current query", args(1,3, arg("",int),arg("mvc",int),arg("nr",lng))),
 pattern("sql", "copy_from", mvc_import_table_wrap, true, "Import a table from bstream s with the \ngiven tuple and seperators (sep/rsep)", args(1,13, batvarargany("",0),arg("t",ptr),arg("sep",str),arg("rsep",str),arg("ssep",str),arg("ns",str),arg("fname",str),arg("nr",lng),arg("offset",lng),arg("best",int),arg("fwf",str),arg("onclient",int),arg("escape",int))),
 //we use bat.single now
 //pattern("sql", "single", CMDBATsingle, false, "", args(1,2, batargany("",2),argany("x",2))),
 pattern("sql", "importTable", mvc_bin_import_table_wrap, true, "Import a table from the files (fname)", args(1,6, batvarargany("",0),arg("sname",str),arg("tname",str),arg("onclient",int),arg("bswap",bit),vararg("fname",str))),
 pattern("sql", "importColumn", mvc_bin_import_column_wrap, false, "Import a column from the given file", args(2, 7, batargany("", 0),arg("", oid), arg("method",str),arg("bswap",bit),arg("path",str),arg("onclient",int),arg("nrows",oid))),
 command("aggr", "not_unique", not_unique, false, "check if the tail sorted bat b doesn't have unique tail values", args(1,2, arg("",bit),batarg("b",oid))),
 command("sql", "optimizers", getPipeCatalog, false, "", args(3,3, batarg("",str),batarg("",str),batarg("",str))),
 pattern("sql", "optimizer_updates", SQLoptimizersUpdate, false, "", noargs),
 pattern("sql", "argRecord", SQLargRecord, false, "Glue together the calling sequence", args(1,1, arg("",str))),
 pattern("sql", "argRecord", SQLargRecord, false, "Glue together the calling sequence", args(1,2, arg("",str),varargany("a",0))),
 pattern("sql", "sql_variables", sql_variables, false, "return the table with session variables", args(4,4, batarg("sname",str),batarg("name",str),batarg("type",str),batarg("value",str))),
 pattern("sql", "sessions", sql_sessions_wrap, false, "SQL export table of active sessions, their timeouts and idle status", args(9,9, batarg("id",int),batarg("user",str),batarg("start",timestamp),batarg("idle",timestamp),batarg("optmizer",str),batarg("stimeout",int),batarg("qtimeout",int),batarg("wlimit",int),batarg("mlimit",int))),
 pattern("sql", "db_users", db_users_wrap, false, "return table of users with sql scenario", args(1,1, batarg("",str))),
 pattern("sql", "password", db_password_wrap, false, "Return password hash of user", args(1,2, arg("",str),arg("user",str))),
 pattern("batsql", "password", db_password_wrap, false, "Return password hash of user", args(1,2, batarg("",str),batarg("user",str))),
 pattern("sql", "rt_credentials", sql_rt_credentials_wrap, false, "Return the remote table credentials for the given table", args(3,4, batarg("uri",str),batarg("username",str),batarg("hash",str),arg("tablename",str))),
 pattern("sql", "dump_cache", dump_cache, false, "dump the content of the query cache", args(2,2, batarg("query",str),batarg("count",int))),
 pattern("sql", "dump_opt_stats", dump_opt_stats, false, "dump the optimizer rewrite statistics", args(2,2, batarg("rewrite",str),batarg("count",int))),
 pattern("sql", "dump_trace", dump_trace, false, "dump the trace statistics", args(2,2, batarg("ticks",lng),batarg("stmt",str))),
 pattern("sql", "analyze", sql_analyze, true, "", args(1,3, arg("",void),arg("minmax",int),arg("sample",lng))),
 pattern("sql", "analyze", sql_analyze, true, "", args(1,4, arg("",void),arg("minmax",int),arg("sample",lng),arg("sch",str))),
 pattern("sql", "analyze", sql_analyze, true, "", args(1,5, arg("",void),arg("minmax",int),arg("sample",lng),arg("sch",str),arg("tbl",str))),
 pattern("sql", "analyze", sql_analyze, true, "Update the database statistics table", args(1,6, arg("",void),arg("minmax",int),arg("sample",lng),arg("sch",str),arg("tbl",str),arg("col",str))),
 pattern("sql", "storage", sql_storage, false, "return a table with storage information ", args(17,17, batarg("schema",str),batarg("table",str),batarg("column",str),batarg("type",str),batarg("mode",str),batarg("location",str),batarg("count",lng),batarg("atomwidth",int),batarg("columnsize",lng),batarg("heap",lng),batarg("hashes",lng),batarg("phash",bit),batarg("imprints",lng),batarg("sorted",bit),batarg("revsorted",bit),batarg("key",bit),batarg("orderidx",lng))),
 pattern("sql", "storage", sql_storage, false, "return a table with storage information for a particular schema ", args(17,18, batarg("schema",str),batarg("table",str),batarg("column",str),batarg("type",str),batarg("mode",str),batarg("location",str),batarg("count",lng),batarg("atomwidth",int),batarg("columnsize",lng),batarg("heap",lng),batarg("hashes",lng),batarg("phash",bit),batarg("imprints",lng),batarg("sorted",bit),batarg("revsorted",bit),batarg("key",bit),batarg("orderidx",lng),arg("sname",str))),
 pattern("sql", "storage", sql_storage, false, "return a table with storage information for a particular table", args(17,19, batarg("schema",str),batarg("table",str),batarg("column",str),batarg("type",str),batarg("mode",str),batarg("location",str),batarg("count",lng),batarg("atomwidth",int),batarg("columnsize",lng),batarg("heap",lng),batarg("hashes",lng),batarg("phash",bit),batarg("imprints",lng),batarg("sorted",bit),batarg("revsorted",bit),batarg("key",bit),batarg("orderidx",lng),arg("sname",str),arg("tname",str))),
 pattern("sql", "storage", sql_storage, false, "return a table with storage information for a particular column", args(17,20, batarg("schema",str),batarg("table",str),batarg("column",str),batarg("type",str),batarg("mode",str),batarg("location",str),batarg("count",lng),batarg("atomwidth",int),batarg("columnsize",lng),batarg("heap",lng),batarg("hashes",lng),batarg("phash",bit),batarg("imprints",lng),batarg("sorted",bit),batarg("revsorted",bit),batarg("key",bit),batarg("orderidx",lng),arg("sname",str),arg("tname",str),arg("cname",str))),
 pattern("sql", "createorderindex", sql_createorderindex, true, "Instantiate the order index on a column", args(0,3, arg("sch",str),arg("tbl",str),arg("col",str))),
 pattern("sql", "droporderindex", sql_droporderindex, true, "Drop the order index on a column", args(0,3, arg("sch",str),arg("tbl",str),arg("col",str))),
 command("calc", "identity", SQLidentity, false, "Returns a unique row identitfier.", args(1,2, arg("",oid),argany("",0))),
 command("batcalc", "identity", BATSQLidentity, false, "Returns the unique row identitfiers.", args(1,2, batarg("",oid),batargany("b",0))),
 pattern("batcalc", "identity", PBATSQLidentity, false, "Returns the unique row identitfiers.", args(2,4, batarg("resb",oid),arg("ns",oid),batargany("b",0),arg("s",oid))),
 pattern("sql", "querylog_catalog", sql_querylog_catalog, false, "Obtain the query log catalog", args(8,8, batarg("id",oid),batarg("user",str),batarg("defined",timestamp),batarg("query",str),batarg("pipe",str),batarg("plan",str),batarg("mal",int),batarg("optimize",lng))),
 pattern("sql", "querylog_calls", sql_querylog_calls, false, "Obtain the query log calls", args(9,9, batarg("id",oid),batarg("start",timestamp),batarg("stop",timestamp),batarg("arguments",str),batarg("tuples",lng),batarg("exec",lng),batarg("result",lng),batarg("cpuload",int),batarg("iowait",int))),
 pattern("sql", "querylog_empty", sql_querylog_empty, true, "", noargs),
 command("sql", "querylog_enable", QLOGenable, true, "", noargs),
 command("sql", "querylog_enable", QLOGenableThreshold, true, "", args(0,1, arg("thres",int))),
 command("sql", "querylog_disable", QLOGdisable, true, "", noargs),
 pattern("sql", "prepared_statements", SQLsession_prepared_statements, false, "Available prepared statements in the current session", args(5,5, batarg("sessionid",int),batarg("user",str),batarg("statementid",int),batarg("statement",str),batarg("created",timestamp))),
 pattern("sql", "prepared_statements_args", SQLsession_prepared_statements_args, false, "Available prepared statements' arguments in the current session", args(9,9, batarg("statementid",int),batarg("type",str),batarg("digits",int),batarg("scale",int),batarg("inout",bte),batarg("number",int),batarg("schema",str),batarg("table",str),batarg("column",str))),
 pattern("sql", "copy_rejects", COPYrejects, false, "", args(4,4, batarg("rowid",lng),batarg("fldid",int),batarg("msg",str),batarg("inp",str))),
 pattern("sql", "copy_rejects_clear", COPYrejects_clear, true, "", noargs),
 command("calc", "dec_round", bte_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,3, arg("",bte),arg("v",bte),arg("r",bte))),
 pattern("batcalc", "dec_round", bte_bat_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",bte),batarg("v",bte),arg("r",bte))),
 pattern("batcalc", "dec_round", bte_bat_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,4, batarg("",bte),batarg("v",bte),arg("r",bte),batarg("s",oid))),
 pattern("batcalc", "dec_round", bte_bat_dec_round_wrap_cst, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",bte),arg("v",bte),batarg("r",bte))),
 pattern("batcalc", "dec_round", bte_bat_dec_round_wrap_cst, false, "round off the value v to nearests multiple of r", args(1,4, batarg("",bte),arg("v",bte),batarg("r",bte),batarg("s",oid))),
 pattern("batcalc", "dec_round", bte_bat_dec_round_wrap_nocst, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",bte),batarg("v",bte),batarg("r",bte))),
 pattern("batcalc", "dec_round", bte_bat_dec_round_wrap_nocst, false, "round off the value v to nearests multiple of r", args(1,5, batarg("",bte),batarg("v",bte),batarg("r",bte),batarg("s1",oid),batarg("s2",oid))),
 command("calc", "round", bte_round_wrap, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, arg("",bte),arg("v",bte),arg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", bte_bat_round_wrap, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, batarg("",bte),batarg("v",bte),arg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", bte_bat_round_wrap, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,6, batarg("",bte),batarg("v",bte),arg("r",bte),batarg("s",oid),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", bte_bat_round_wrap_cst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, batarg("",bte),arg("v",bte),batarg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", bte_bat_round_wrap_cst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,6, batarg("",bte),arg("v",bte),batarg("r",bte),batarg("s",oid),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", bte_bat_round_wrap_nocst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, batarg("",bte),batarg("v",bte),batarg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", bte_bat_round_wrap_nocst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,7, batarg("",bte),batarg("v",bte),batarg("r",bte),batarg("s1",oid),batarg("s2",oid),arg("d",int),arg("s",int))),
 command("calc", "second_interval", bte_dec2second_interval, false, "cast bte decimal to a second_interval", args(1,5, arg("",lng),arg("sc",int),arg("v",bte),arg("ek",int),arg("sk",int))),
 pattern("batcalc", "second_interval", bte_batdec2second_interval, false, "cast bte decimal to a second_interval", args(1,6, batarg("",lng),arg("sc",int),batarg("v",bte),batarg("s",oid),arg("ek",int),arg("sk",int))),
 command("calc", "dec_round", sht_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,3, arg("",sht),arg("v",sht),arg("r",sht))),
 pattern("batcalc", "dec_round", sht_bat_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",sht),batarg("v",sht),arg("r",sht))),
 pattern("batcalc", "dec_round", sht_bat_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,4, batarg("",sht),batarg("v",sht),arg("r",sht),batarg("s",oid))),
 pattern("batcalc", "dec_round", sht_bat_dec_round_wrap_cst, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",sht),arg("v",sht),batarg("r",sht))),
 pattern("batcalc", "dec_round", sht_bat_dec_round_wrap_cst, false, "round off the value v to nearests multiple of r", args(1,4, batarg("",sht),arg("v",sht),batarg("r",sht),batarg("s",oid))),
 pattern("batcalc", "dec_round", sht_bat_dec_round_wrap_nocst, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",sht),batarg("v",sht),batarg("r",sht))),
 pattern("batcalc", "dec_round", sht_bat_dec_round_wrap_nocst, false, "round off the value v to nearests multiple of r", args(1,5, batarg("",sht),batarg("v",sht),batarg("r",sht),batarg("s1",oid),batarg("s2",oid))),
 command("calc", "round", sht_round_wrap, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, arg("",sht),arg("v",sht),arg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", sht_bat_round_wrap, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, batarg("",sht),batarg("v",sht),arg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", sht_bat_round_wrap, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,6, batarg("",sht),batarg("v",sht),arg("r",bte),batarg("s",oid),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", sht_bat_round_wrap_cst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, batarg("",sht),arg("v",sht),batarg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", sht_bat_round_wrap_cst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,6, batarg("",sht),arg("v",sht),batarg("r",bte),batarg("s",oid),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", sht_bat_round_wrap_nocst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, batarg("",sht),batarg("v",sht),batarg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", sht_bat_round_wrap_nocst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,7, batarg("",sht),batarg("v",sht),batarg("r",bte),batarg("s1",oid),batarg("s2",oid),arg("d",int),arg("s",int))),
 command("calc", "second_interval", sht_dec2second_interval, false, "cast sht decimal to a second_interval", args(1,5, arg("",lng),arg("sc",int),arg("v",sht),arg("ek",int),arg("sk",int))),
 pattern("batcalc", "second_interval", sht_batdec2second_interval, false, "cast sht decimal to a second_interval", args(1,6, batarg("",lng),arg("sc",int),batarg("v",sht),batarg("s",oid),arg("ek",int),arg("sk",int))),
 command("calc", "dec_round", int_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,3, arg("",int),arg("v",int),arg("r",int))),
 pattern("batcalc", "dec_round", int_bat_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",int),batarg("v",int),arg("r",int))),
 pattern("batcalc", "dec_round", int_bat_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,4, batarg("",int),batarg("v",int),arg("r",int),batarg("s",oid))),
 pattern("batcalc", "dec_round", int_bat_dec_round_wrap_cst, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",int),arg("v",int),batarg("r",int))),
 pattern("batcalc", "dec_round", int_bat_dec_round_wrap_cst, false, "round off the value v to nearests multiple of r", args(1,4, batarg("",int),arg("v",int),batarg("r",int),batarg("s",oid))),
 pattern("batcalc", "dec_round", int_bat_dec_round_wrap_nocst, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",int),batarg("v",int),batarg("r",int))),
 pattern("batcalc", "dec_round", int_bat_dec_round_wrap_nocst, false, "round off the value v to nearests multiple of r", args(1,5, batarg("",int),batarg("v",int),batarg("r",int),batarg("s1",oid),batarg("s2",oid))),
 command("calc", "round", int_round_wrap, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, arg("",int),arg("v",int),arg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", int_bat_round_wrap, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, batarg("",int),batarg("v",int),arg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", int_bat_round_wrap, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,6, batarg("",int),batarg("v",int),arg("r",bte),batarg("s",oid),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", int_bat_round_wrap_cst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, batarg("",int),arg("v",int),batarg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", int_bat_round_wrap_cst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,6, batarg("",int),arg("v",int),batarg("r",bte),batarg("s",oid),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", int_bat_round_wrap_nocst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, batarg("",int),batarg("v",int),batarg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", int_bat_round_wrap_nocst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,7, batarg("",int),batarg("v",int),batarg("r",bte),batarg("s1",oid),batarg("s2",oid),arg("d",int),arg("s",int))),
 command("calc", "second_interval", int_dec2second_interval, false, "cast int decimal to a second_interval", args(1,5, arg("",lng),arg("sc",int),arg("v",int),arg("ek",int),arg("sk",int))),
 pattern("batcalc", "second_interval", int_batdec2second_interval, false, "cast int decimal to a second_interval", args(1,6, batarg("",lng),arg("sc",int),batarg("v",int),batarg("s",oid),arg("ek",int),arg("sk",int))),
 command("calc", "dec_round", lng_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,3, arg("",lng),arg("v",lng),arg("r",lng))),
 pattern("batcalc", "dec_round", lng_bat_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",lng),batarg("v",lng),arg("r",lng))),
 pattern("batcalc", "dec_round", lng_bat_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,4, batarg("",lng),batarg("v",lng),arg("r",lng),batarg("s",oid))),
 pattern("batcalc", "dec_round", lng_bat_dec_round_wrap_cst, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",lng),arg("v",lng),batarg("r",lng))),
 pattern("batcalc", "dec_round", lng_bat_dec_round_wrap_cst, false, "round off the value v to nearests multiple of r", args(1,4, batarg("",lng),arg("v",lng),batarg("r",lng),batarg("s",oid))),
 pattern("batcalc", "dec_round", lng_bat_dec_round_wrap_nocst, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",lng),batarg("v",lng),batarg("r",lng))),
 pattern("batcalc", "dec_round", lng_bat_dec_round_wrap_nocst, false, "round off the value v to nearests multiple of r", args(1,5, batarg("",lng),batarg("v",lng),batarg("r",lng),batarg("s1",oid),batarg("s2",oid))),
 command("calc", "round", lng_round_wrap, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, arg("",lng),arg("v",lng),arg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", lng_bat_round_wrap, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, batarg("",lng),batarg("v",lng),arg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", lng_bat_round_wrap, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,6, batarg("",lng),batarg("v",lng),arg("r",bte),batarg("s",oid),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", lng_bat_round_wrap_cst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, batarg("",lng),arg("v",lng),batarg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", lng_bat_round_wrap_cst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,6, batarg("",lng),arg("v",lng),batarg("r",bte),batarg("s",oid),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", lng_bat_round_wrap_nocst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, batarg("",lng),batarg("v",lng),batarg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", lng_bat_round_wrap_nocst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,7, batarg("",lng),batarg("v",lng),batarg("r",bte),batarg("s1",oid),batarg("s2",oid),arg("d",int),arg("s",int))),
 command("calc", "second_interval", lng_dec2second_interval, false, "cast lng decimal to a second_interval", args(1,5, arg("",lng),arg("sc",int),arg("v",lng),arg("ek",int),arg("sk",int))),
 pattern("batcalc", "second_interval", lng_batdec2second_interval, false, "cast lng decimal to a second_interval", args(1,6, batarg("",lng),arg("sc",int),batarg("v",lng),batarg("s",oid),arg("ek",int),arg("sk",int))),
 command("calc", "dec_round", flt_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,3, arg("",flt),arg("v",flt),arg("r",flt))),
 pattern("batcalc", "dec_round", flt_bat_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",flt),batarg("v",flt),arg("r",flt))),
 pattern("batcalc", "dec_round", flt_bat_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,4, batarg("",flt),batarg("v",flt),arg("r",flt),batarg("s",oid))),
 pattern("batcalc", "dec_round", flt_bat_dec_round_wrap_cst, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",flt),arg("v",flt),batarg("r",flt))),
 pattern("batcalc", "dec_round", flt_bat_dec_round_wrap_cst, false, "round off the value v to nearests multiple of r", args(1,4, batarg("",flt),arg("v",flt),batarg("r",flt),batarg("s",oid))),
 pattern("batcalc", "dec_round", flt_bat_dec_round_wrap_nocst, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",flt),batarg("v",flt),batarg("r",flt))),
 pattern("batcalc", "dec_round", flt_bat_dec_round_wrap_nocst, false, "round off the value v to nearests multiple of r", args(1,5, batarg("",flt),batarg("v",flt),batarg("r",flt),batarg("s1",oid),batarg("s2",oid))),
 command("calc", "round", flt_round_wrap, false, "round off the floating point v to r digits behind the dot (if r < 0, before the dot)", args(1,3, arg("",flt),arg("v",flt),arg("r",bte))),
 pattern("batcalc", "round", flt_bat_round_wrap, false, "round off the floating point v to r digits behind the dot (if r < 0, before the dot)", args(1,3, batarg("",flt),batarg("v",flt),arg("r",bte))),
 pattern("batcalc", "round", flt_bat_round_wrap, false, "round off the floating point v to r digits behind the dot (if r < 0, before the dot)", args(1,4, batarg("",flt),batarg("v",flt),arg("r",bte),batarg("s",oid))),
 pattern("batcalc", "round", flt_bat_round_wrap_cst, false, "round off the floating point v to r digits behind the dot (if r < 0, before the dot)", args(1,3, batarg("",flt),arg("v",flt),batarg("r",bte))),
 pattern("batcalc", "round", flt_bat_round_wrap_cst, false, "round off the floating point v to r digits behind the dot (if r < 0, before the dot)", args(1,4, batarg("",flt),arg("v",flt),batarg("r",bte),batarg("s",oid))),
 pattern("batcalc", "round", flt_bat_round_wrap_nocst, false, "round off the floating point v to r digits behind the dot (if r < 0, before the dot)", args(1,3, batarg("",flt),batarg("v",flt),batarg("r",bte))),
 pattern("batcalc", "round", flt_bat_round_wrap_nocst, false, "round off the floating point v to r digits behind the dot (if r < 0, before the dot)", args(1,5, batarg("",flt),batarg("v",flt),batarg("r",bte),batarg("s1",oid),batarg("s2",oid))),
 command("sql", "ms_trunc", flt_trunc_wrap, false, "truncate the floating point v to r digits behind the dot (if r < 0, before the dot)", args(1,3, arg("",flt),arg("v",flt),arg("r",int))),
 command("calc", "dec_round", dbl_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,3, arg("",dbl),arg("v",dbl),arg("r",dbl))),
 pattern("batcalc", "dec_round", dbl_bat_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",dbl),batarg("v",dbl),arg("r",dbl))),
 pattern("batcalc", "dec_round", dbl_bat_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,4, batarg("",dbl),batarg("v",dbl),arg("r",dbl),batarg("s",oid))),
 pattern("batcalc", "dec_round", dbl_bat_dec_round_wrap_cst, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",dbl),arg("v",dbl),batarg("r",dbl))),
 pattern("batcalc", "dec_round", dbl_bat_dec_round_wrap_cst, false, "round off the value v to nearests multiple of r", args(1,4, batarg("",dbl),arg("v",dbl),batarg("r",dbl),batarg("s",oid))),
 pattern("batcalc", "dec_round", dbl_bat_dec_round_wrap_nocst, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",dbl),batarg("v",dbl),batarg("r",dbl))),
 pattern("batcalc", "dec_round", dbl_bat_dec_round_wrap_nocst, false, "round off the value v to nearests multiple of r", args(1,5, batarg("",dbl),batarg("v",dbl),batarg("r",dbl),batarg("s1",oid),batarg("s2",oid))),
 command("calc", "round", dbl_round_wrap, false, "round off the floating point v to r digits behind the dot (if r < 0, before the dot)", args(1,3, arg("",dbl),arg("v",dbl),arg("r",bte))),
 pattern("batcalc", "round", dbl_bat_round_wrap, false, "round off the floating point v to r digits behind the dot (if r < 0, before the dot)", args(1,3, batarg("",dbl),batarg("v",dbl),arg("r",bte))),
 pattern("batcalc", "round", dbl_bat_round_wrap, false, "round off the floating point v to r digits behind the dot (if r < 0, before the dot)", args(1,4, batarg("",dbl),batarg("v",dbl),arg("r",bte),batarg("s",oid))),
 pattern("batcalc", "round", dbl_bat_round_wrap_cst, false, "round off the floating point v to r digits behind the dot (if r < 0, before the dot)", args(1,3, batarg("",dbl),arg("v",dbl),batarg("r",bte))),
 pattern("batcalc", "round", dbl_bat_round_wrap_cst, false, "round off the floating point v to r digits behind the dot (if r < 0, before the dot)", args(1,4, batarg("",dbl),arg("v",dbl),batarg("r",bte),batarg("s",oid))),
 pattern("batcalc", "round", dbl_bat_round_wrap_nocst, false, "round off the floating point v to r digits behind the dot (if r < 0, before the dot)", args(1,3, batarg("",dbl),batarg("v",dbl),batarg("r",bte))),
 pattern("batcalc", "round", dbl_bat_round_wrap_nocst, false, "round off the floating point v to r digits behind the dot (if r < 0, before the dot)", args(1,5, batarg("",dbl),batarg("v",dbl),batarg("r",bte),batarg("s1",oid),batarg("s2",oid))),
 command("sql", "ms_trunc", dbl_trunc_wrap, false, "truncate the floating point v to r digits behind the dot (if r < 0, before the dot)", args(1,3, arg("",dbl),arg("v",dbl),arg("r",int))),
 command("sql", "alpha", SQLcst_alpha_cst, false, "Implementation of astronomy alpha function: expands the radius theta depending on the declination", args(1,3, arg("",dbl),arg("dec",dbl),arg("theta",dbl))),
 command("batsql", "alpha", SQLbat_alpha_cst, false, "BAT implementation of astronomy alpha function", args(1,3, batarg("",dbl),batarg("dec",dbl),arg("theta",dbl))),
 command("batsql", "alpha", SQLcst_alpha_bat, false, "BAT implementation of astronomy alpha function", args(1,3, batarg("",dbl),arg("dec",dbl),batarg("theta",dbl))),
 command("calc", "bte", nil_2dec_bte, false, "cast to dec(bte) and check for overflow", args(1,4, arg("",bte),arg("v",void),arg("digits",int),arg("scale",int))),
 command("batcalc", "bte", batnil_2dec_bte, false, "cast to dec(bte) and check for overflow", args(1,4, batarg("",bte),batarg("v",oid),arg("digits",int),arg("scale",int))),
 command("calc", "bte", str_2dec_bte, false, "cast to dec(bte) and check for overflow", args(1,4, arg("",bte),arg("v",str),arg("digits",int),arg("scale",int))),
 pattern("batcalc", "bte", batstr_2dec_bte, false, "cast to dec(bte) and check for overflow", args(1,5, batarg("",bte),batarg("v",str),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "sht", nil_2dec_sht, false, "cast to dec(sht) and check for overflow", args(1,4, arg("",sht),arg("v",void),arg("digits",int),arg("scale",int))),
 command("batcalc", "sht", batnil_2dec_sht, false, "cast to dec(sht) and check for overflow", args(1,4, batarg("",sht),batarg("v",oid),arg("digits",int),arg("scale",int))),
 command("calc", "sht", str_2dec_sht, false, "cast to dec(sht) and check for overflow", args(1,4, arg("",sht),arg("v",str),arg("digits",int),arg("scale",int))),
 pattern("batcalc", "sht", batstr_2dec_sht, false, "cast to dec(sht) and check for overflow", args(1,5, batarg("",sht),batarg("v",str),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "int", nil_2dec_int, false, "cast to dec(int) and check for overflow", args(1,4, arg("",int),arg("v",void),arg("digits",int),arg("scale",int))),
 command("batcalc", "int", batnil_2dec_int, false, "cast to dec(int) and check for overflow", args(1,4, batarg("",int),batarg("v",oid),arg("digits",int),arg("scale",int))),
 command("calc", "int", str_2dec_int, false, "cast to dec(int) and check for overflow", args(1,4, arg("",int),arg("v",str),arg("digits",int),arg("scale",int))),
 pattern("batcalc", "int", batstr_2dec_int, false, "cast to dec(int) and check for overflow", args(1,5, batarg("",int),batarg("v",str),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "lng", nil_2dec_lng, false, "cast to dec(lng) and check for overflow", args(1,4, arg("",lng),arg("v",void),arg("digits",int),arg("scale",int))),
 command("batcalc", "lng", batnil_2dec_lng, false, "cast to dec(lng) and check for overflow", args(1,4, batarg("",lng),batarg("v",oid),arg("digits",int),arg("scale",int))),
 command("calc", "lng", str_2dec_lng, false, "cast to dec(lng) and check for overflow", args(1,4, arg("",lng),arg("v",str),arg("digits",int),arg("scale",int))),
 pattern("batcalc", "lng", batstr_2dec_lng, false, "cast to dec(lng) and check for overflow", args(1,5, batarg("",lng),batarg("v",str),batarg("s",oid),arg("digits",int),arg("scale",int))),
 pattern("calc", "timestamp", nil_2time_timestamp, false, "cast to timestamp and check for overflow", args(1,3, arg("",timestamp),arg("v",void),arg("digits",int))),
 pattern("batcalc", "timestamp", nil_2time_timestamp, false, "cast to timestamp and check for overflow", args(1,3, batarg("",timestamp),batarg("v",oid),arg("digits",int))),
 pattern("batcalc", "timestamp", nil_2time_timestamp, false, "cast to timestamp and check for overflow", args(1,4, batarg("",timestamp),batarg("v",oid),arg("digits",int),batarg("r",bat))),
 pattern("calc", "timestamp", str_2time_timestamp, false, "cast to timestamp and check for overflow", args(1,3, arg("",timestamp),arg("v",str),arg("digits",int))),
 pattern("calc", "timestamp", str_2time_timestamptz, false, "cast to timestamp and check for overflow", args(1,4, arg("",timestamp),arg("v",str),arg("digits",int),arg("has_tz",int))),
 pattern("calc", "timestamp", timestamp_2time_timestamp, false, "cast timestamp to timestamp and check for overflow", args(1,3, arg("",timestamp),arg("v",timestamp),arg("digits",int))),
 command("batcalc", "timestamp", batstr_2time_timestamp, false, "cast to timestamp and check for overflow", args(1,4, batarg("",timestamp),batarg("v",str),batarg("s",oid),arg("digits",int))),
 command("batcalc", "timestamp", batstr_2time_timestamptz, false, "cast to timestamp and check for overflow", args(1,5, batarg("",timestamp),batarg("v",str),batarg("s",oid),arg("digits",int),arg("has_tz",int))),
 pattern("batcalc", "timestamp", timestamp_2time_timestamp, false, "cast timestamp to timestamp and check for overflow", args(1,4, batarg("",timestamp),batarg("v",timestamp),batarg("s",oid),arg("digits",int))),
 pattern("batcalc", "daytime", nil_2time_daytime, false, "cast to daytime and check for overflow", args(1,3, batarg("",daytime),batarg("v",oid),arg("digits",int))),
 pattern("calc", "daytime", str_2time_daytime, false, "cast to daytime and check for overflow", args(1,3, arg("",daytime),arg("v",str),arg("digits",int))),
 pattern("calc", "daytime", str_2time_daytimetz, false, "cast to daytime and check for overflow", args(1,4, arg("",daytime),arg("v",str),arg("digits",int),arg("has_tz",int))),
 pattern("calc", "daytime", daytime_2time_daytime, false, "cast daytime to daytime and check for overflow", args(1,3, arg("",daytime),arg("v",daytime),arg("digits",int))),
 command("batcalc", "daytime", batstr_2time_daytime, false, "cast to daytime and check for overflow", args(1,4, batarg("",daytime),batarg("v",str),batarg("s",oid),arg("digits",int))),
 pattern("batcalc", "daytime", str_2time_daytimetz, false, "cast daytime to daytime and check for overflow", args(1,5, batarg("",daytime),batarg("v",str),batarg("s",oid),arg("digits",int),arg("has_tz",int))),
 pattern("batcalc", "daytime", daytime_2time_daytime, false, "cast daytime to daytime and check for overflow", args(1,4, batarg("",daytime),batarg("v",daytime),batarg("s",oid),arg("digits",int))),
 command("sql", "date_trunc", bat_date_trunc, false, "Truncate a timestamp to (millennium, century,decade,year,quarter,month,week,day,hour,minute,second, milliseconds,microseconds)", args(1,3, batarg("",timestamp),arg("scale",str),batarg("v",timestamp))),
 command("sql", "date_trunc", date_trunc, false, "Truncate a timestamp to (millennium, century,decade,year,quarter,month,week,day,hour,minute,second, milliseconds,microseconds)", args(1,3, arg("",timestamp),arg("scale",str),arg("v",timestamp))),
 pattern("sql", "current_time", SQLcurrent_daytime, false, "Get the clients current daytime", args(1,1, arg("",daytime))),
 pattern("sql", "current_timestamp", SQLcurrent_timestamp, false, "Get the clients current timestamp", args(1,1, arg("",timestamp))),
 pattern("calc", "date", nil_2_date, false, "cast to date", args(1,2, arg("",date),arg("v",void))),
 pattern("batcalc", "date", nil_2_date, false, "cast to date", args(1,2, batarg("",date),batarg("v",oid))),
 pattern("calc", "date", str_2_date, false, "cast to date", args(1,2, arg("",date),arg("v",str))),
 command("batcalc", "date", batstr_2_date, false, "cast to date", args(1,3, batarg("",date),batarg("v",str),batarg("s",oid))),
 command("calc", "blob", str_2_blob, false, "cast to blob", args(1,2, arg("",blob),arg("v",str))),
 command("batcalc", "blob", batstr_2_blob, false, "cast to blob", args(1,3, batarg("",blob),batarg("v",str),batarg("s",oid))),
 pattern("calc", "str", SQLstr_cast, false, "cast to string and check for overflow", args(1,7, arg("",str),arg("eclass",int),arg("d1",int),arg("s1",int),arg("has_tz",int),argany("v",1),arg("digits",int))),
 pattern("batcalc", "str", SQLbatstr_cast, false, "cast to string and check for overflow", args(1,8, batarg("",str),arg("eclass",int),arg("d1",int),arg("s1",int),arg("has_tz",int),batargany("v",1),batarg("s",oid),arg("digits",int))),
 pattern("calc", "month_interval", month_interval_str, false, "cast str to a month_interval and check for overflow", args(1,4, arg("",int),arg("v",str),arg("ek",int),arg("sk",int))),
 pattern("batcalc", "month_interval", month_interval_str, false, "cast str to a month_interval and check for overflow", args(1,5, batarg("",int),batarg("v",str),batarg("s",oid),arg("ek",int),arg("sk",int))),
 pattern("calc", "second_interval", second_interval_str, false, "cast str to a second_interval and check for overflow", args(1,4, arg("",lng),arg("v",str),arg("ek",int),arg("sk",int))),
 pattern("batcalc", "second_interval", second_interval_str, false, "cast str to a second_interval and check for overflow", args(1,5, batarg("",lng),batarg("v",str),batarg("s",oid),arg("ek",int),arg("sk",int))),
 pattern("calc", "month_interval", month_interval, false, "cast bte to a month_interval and check for overflow", args(1,4, arg("",int),arg("v",bte),arg("ek",int),arg("sk",int))),
 pattern("batcalc", "month_interval", month_interval, false, "cast bte to a month_interval and check for overflow", args(1,5, batarg("",int),batarg("v",bte),batarg("s",oid),arg("ek",int),arg("sk",int))),
 pattern("calc", "second_interval", second_interval, false, "cast bte to a second_interval and check for overflow", args(1,4, arg("",lng),arg("v",bte),arg("ek",int),arg("sk",int))),
 pattern("batcalc", "second_interval", second_interval, false, "cast bte to a second_interval and check for overflow", args(1,5, batarg("",lng),batarg("v",bte),batarg("s",oid),arg("ek",int),arg("sk",int))),
 pattern("calc", "month_interval", month_interval, false, "cast sht to a month_interval and check for overflow", args(1,4, arg("",int),arg("v",sht),arg("ek",int),arg("sk",int))),
 pattern("batcalc", "month_interval", month_interval, false, "cast sht to a month_interval and check for overflow", args(1,5, batarg("",int),batarg("v",sht),batarg("s",oid),arg("ek",int),arg("sk",int))),
 pattern("calc", "second_interval", second_interval, false, "cast sht to a second_interval and check for overflow", args(1,4, arg("",lng),arg("v",sht),arg("ek",int),arg("sk",int))),
 pattern("batcalc", "second_interval", second_interval, false, "cast sht to a second_interval and check for overflow", args(1,5, batarg("",lng),batarg("v",sht),batarg("s",oid),arg("ek",int),arg("sk",int))),
 pattern("calc", "month_interval", month_interval, false, "cast int to a month_interval and check for overflow", args(1,4, arg("",int),arg("v",int),arg("ek",int),arg("sk",int))),
 pattern("batcalc", "month_interval", month_interval, false, "cast int to a month_interval and check for overflow", args(1,5, batarg("",int),batarg("v",int),batarg("s",oid),arg("ek",int),arg("sk",int))),
 pattern("calc", "second_interval", second_interval, false, "cast int to a second_interval and check for overflow", args(1,4, arg("",lng),arg("v",int),arg("ek",int),arg("sk",int))),
 pattern("batcalc", "second_interval", second_interval, false, "cast int to a second_interval and check for overflow", args(1,5, batarg("",lng),batarg("v",int),batarg("s",oid),arg("ek",int),arg("sk",int))),
 pattern("calc", "month_interval", month_interval, false, "cast lng to a month_interval and check for overflow", args(1,4, arg("",int),arg("v",lng),arg("ek",int),arg("sk",int))),
 pattern("batcalc", "month_interval", month_interval, false, "cast lng to a month_interval and check for overflow", args(1,5, batarg("",int),batarg("v",lng),batarg("s",oid),arg("ek",int),arg("sk",int))),
 pattern("calc", "second_interval", second_interval, false, "cast lng to a second_interval and check for overflow", args(1,4, arg("",lng),arg("v",lng),arg("ek",int),arg("sk",int))),
 pattern("batcalc", "second_interval", second_interval, false, "cast lng to a second_interval and check for overflow", args(1,5, batarg("",lng),batarg("v",lng),batarg("s",oid),arg("ek",int),arg("sk",int))),
 pattern("calc", "rowid", sql_rowid, false, "return the next rowid", args(1,4, arg("",oid),argany("v",1),arg("schema",str),arg("table",str))),
 pattern("sql", "drop_hash", SQLdrop_hash, true, "Drop hash indices for the given table", args(0,2, arg("sch",str),arg("tbl",str))),
 pattern("sql", "prelude", SQLprelude, false, "", noargs),
 command("sql", "epilogue", SQLepilogue, false, "", noargs),
 pattern("calc", "second_interval", second_interval_daytime, false, "cast daytime to a second_interval and check for overflow", args(1,4, arg("",lng),arg("v",daytime),arg("ek",int),arg("sk",int))),
 pattern("batcalc", "second_interval", second_interval_daytime, false, "cast daytime to a second_interval and check for overflow", args(1,5, batarg("",lng),batarg("v",daytime),batarg("s",oid),arg("ek",int),arg("sk",int))),
 pattern("calc", "daytime", second_interval_2_daytime, false, "cast second_interval to a daytime and check for overflow", args(1,3, arg("",daytime),arg("v",lng),arg("d",int))),
 pattern("batcalc", "daytime", second_interval_2_daytime, false, "cast second_interval to a daytime and check for overflow", args(1,4, batarg("",daytime),batarg("v",lng),batarg("s",oid),arg("d",int))),
 pattern("calc", "daytime", timestamp_2_daytime, false, "cast timestamp to a daytime and check for overflow", args(1,3, arg("",daytime),arg("v",timestamp),arg("d",int))),
 pattern("batcalc", "daytime", timestamp_2_daytime, false, "cast timestamp to a daytime and check for overflow", args(1,4, batarg("",daytime),batarg("v",timestamp),batarg("s",oid),arg("d",int))),
 pattern("calc", "timestamp", date_2_timestamp, false, "cast date to a timestamp and check for overflow", args(1,3, arg("",timestamp),arg("v",date),arg("d",int))),
 pattern("batcalc", "timestamp", date_2_timestamp, false, "cast date to a timestamp and check for overflow", args(1,4, batarg("",timestamp),batarg("v",date),batarg("s",oid),arg("d",int))),
 command("sql", "index", STRindex_bte, false, "Return the offsets as an index bat", args(1,3, arg("",bte),arg("v",str),arg("u",bit))), /* TODO add candidate list support? */
 command("batsql", "index", BATSTRindex_bte, false, "Return the offsets as an index bat", args(1,3, batarg("",bte),batarg("v",str),arg("u",bit))),
 command("sql", "index", STRindex_sht, false, "Return the offsets as an index bat", args(1,3, arg("",sht),arg("v",str),arg("u",bit))),
 command("batsql", "index", BATSTRindex_sht, false, "Return the offsets as an index bat", args(1,3, batarg("",sht),batarg("v",str),arg("u",bit))),
 command("sql", "index", STRindex_int, false, "Return the offsets as an index bat", args(1,3, arg("",int),arg("v",str),arg("u",bit))),
 command("batsql", "index", BATSTRindex_int, false, "Return the offsets as an index bat", args(1,3, batarg("",int),batarg("v",str),arg("u",bit))),
 command("sql", "strings", STRstrings, false, "Return the strings", args(1,2, arg("",str),arg("v",str))), /* TODO add candidate list support? */
 command("batsql", "strings", BATSTRstrings, false, "Return the strings", args(1,2, batarg("",str),batarg("v",str))),
 pattern("sql", "update_tables", SYSupdate_tables, true, "Procedure triggered on update of the sys._tables table", args(1,1, arg("",void))),
 pattern("sql", "update_schemas", SYSupdate_schemas, true, "Procedure triggered on update of the sys.schemas table", args(1,1, arg("",void))),
 pattern("sql", "unionfunc", SQLunionfunc, false, "", args(1,4, varargany("",0),arg("mod",str),arg("fcn",str),varargany("",0))),
 /* decimals */
 command("calc", "bte", flt_num2dec_bte, false, "cast number to decimal(bte) and check for overflow", args(1,4, arg("",bte),arg("v",flt),arg("digits",int),arg("scale",int))),
 command("batcalc", "bte", batflt_num2dec_bte, false, "cast number to decimal(bte) and check for overflow", args(1,5, batarg("",bte),batarg("v",flt),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "bte", dbl_num2dec_bte, false, "cast number to decimal(bte) and check for overflow", args(1,4, arg("",bte),arg("v",dbl),arg("digits",int),arg("scale",int))),
 command("batcalc", "bte", batdbl_num2dec_bte, false, "cast number to decimal(bte) and check for overflow", args(1,5, batarg("",bte),batarg("v",dbl),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "sht", flt_num2dec_sht, false, "cast number to decimal(sht) and check for overflow", args(1,4, arg("",sht),arg("v",flt),arg("digits",int),arg("scale",int))),
 command("batcalc", "sht", batflt_num2dec_sht, false, "cast number to decimal(sht) and check for overflow", args(1,5, batarg("",sht),batarg("v",flt),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "sht", dbl_num2dec_sht, false, "cast number to decimal(sht) and check for overflow", args(1,4, arg("",sht),arg("v",dbl),arg("digits",int),arg("scale",int))),
 command("batcalc", "sht", batdbl_num2dec_sht, false, "cast number to decimal(sht) and check for overflow", args(1,5, batarg("",sht),batarg("v",dbl),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "int", flt_num2dec_int, false, "cast number to decimal(int) and check for overflow", args(1,4, arg("",int),arg("v",flt),arg("digits",int),arg("scale",int))),
 command("batcalc", "int", batflt_num2dec_int, false, "cast number to decimal(int) and check for overflow", args(1,5, batarg("",int),batarg("v",flt),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "int", dbl_num2dec_int, false, "cast number to decimal(int) and check for overflow", args(1,4, arg("",int),arg("v",dbl),arg("digits",int),arg("scale",int))),
 command("batcalc", "int", batdbl_num2dec_int, false, "cast number to decimal(int) and check for overflow", args(1,5, batarg("",int),batarg("v",dbl),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "lng", flt_num2dec_lng, false, "cast number to decimal(lng) and check for overflow", args(1,4, arg("",lng),arg("v",flt),arg("digits",int),arg("scale",int))),
 command("batcalc", "lng", batflt_num2dec_lng, false, "cast number to decimal(lng) and check for overflow", args(1,5, batarg("",lng),batarg("v",flt),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "lng", dbl_num2dec_lng, false, "cast number to decimal(lng) and check for overflow", args(1,4, arg("",lng),arg("v",dbl),arg("digits",int),arg("scale",int))),
 command("batcalc", "lng", batdbl_num2dec_lng, false, "cast number to decimal(lng) and check for overflow", args(1,5, batarg("",lng),batarg("v",dbl),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "bte", bte_num2dec_bte, false, "cast number to decimal(bte) and check for overflow", args(1,4, arg("",bte),arg("v",bte),arg("digits",int),arg("scale",int))),
 command("batcalc", "bte", batbte_num2dec_bte, false, "cast number to decimal(bte) and check for overflow", args(1,5, batarg("",bte),batarg("v",bte),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "bte", bte_dec2_bte, false, "cast decimal(bte) to bte and check for overflow", args(1,3, arg("",bte),arg("s1",int),arg("v",bte))),
 command("calc", "bte", bte_dec2dec_bte, false, "cast decimal(bte) to decimal(bte) and check for overflow", args(1,5, arg("",bte),arg("s1",int),arg("v",bte),arg("d2",int),arg("s2",int))),
 command("batcalc", "bte", batbte_dec2_bte, false, "cast decimal(bte) to bte and check for overflow", args(1,4, batarg("",bte),arg("s1",int),batarg("v",bte),batarg("s",oid))),
 command("batcalc", "bte", batbte_dec2dec_bte, false, "cast decimal(bte) to decimal(bte) and check for overflow", args(1,6, batarg("",bte),arg("s1",int),batarg("v",bte),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "bte", sht_num2dec_bte, false, "cast number to decimal(bte) and check for overflow", args(1,4, arg("",bte),arg("v",sht),arg("digits",int),arg("scale",int))),
 command("batcalc", "bte", batsht_num2dec_bte, false, "cast number to decimal(bte) and check for overflow", args(1,5, batarg("",bte),batarg("v",sht),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "bte", sht_dec2_bte, false, "cast decimal(sht) to bte and check for overflow", args(1,3, arg("",bte),arg("s1",int),arg("v",sht))),
 command("calc", "bte", sht_dec2dec_bte, false, "cast decimal(sht) to decimal(bte) and check for overflow", args(1,5, arg("",bte),arg("s1",int),arg("v",sht),arg("d2",int),arg("s2",int))),
 command("batcalc", "bte", batsht_dec2_bte, false, "cast decimal(sht) to bte and check for overflow", args(1,4, batarg("",bte),arg("s1",int),batarg("v",sht),batarg("s",oid))),
 command("batcalc", "bte", batsht_dec2dec_bte, false, "cast decimal(sht) to decimal(bte) and check for overflow", args(1,6, batarg("",bte),arg("s1",int),batarg("v",sht),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "bte", int_num2dec_bte, false, "cast number to decimal(bte) and check for overflow", args(1,4, arg("",bte),arg("v",int),arg("digits",int),arg("scale",int))),
 command("batcalc", "bte", batint_num2dec_bte, false, "cast number to decimal(bte) and check for overflow", args(1,5, batarg("",bte),batarg("v",int),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "bte", int_dec2_bte, false, "cast decimal(int) to bte and check for overflow", args(1,3, arg("",bte),arg("s1",int),arg("v",int))),
 command("calc", "bte", int_dec2dec_bte, false, "cast decimal(int) to decimal(bte) and check for overflow", args(1,5, arg("",bte),arg("s1",int),arg("v",int),arg("d2",int),arg("s2",int))),
 command("batcalc", "bte", batint_dec2_bte, false, "cast decimal(int) to bte and check for overflow", args(1,4, batarg("",bte),arg("s1",int),batarg("v",int),batarg("s",oid))),
 command("batcalc", "bte", batint_dec2dec_bte, false, "cast decimal(int) to decimal(bte) and check for overflow", args(1,6, batarg("",bte),arg("s1",int),batarg("v",int),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "bte", lng_num2dec_bte, false, "cast number to decimal(bte) and check for overflow", args(1,4, arg("",bte),arg("v",lng),arg("digits",int),arg("scale",int))),
 command("batcalc", "bte", batlng_num2dec_bte, false, "cast number to decimal(bte) and check for overflow", args(1,5, batarg("",bte),batarg("v",lng),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "bte", lng_dec2_bte, false, "cast decimal(lng) to bte and check for overflow", args(1,3, arg("",bte),arg("s1",int),arg("v",lng))),
 command("calc", "bte", lng_dec2dec_bte, false, "cast decimal(lng) to decimal(bte) and check for overflow", args(1,5, arg("",bte),arg("s1",int),arg("v",lng),arg("d2",int),arg("s2",int))),
 command("batcalc", "bte", batlng_dec2_bte, false, "cast decimal(lng) to bte and check for overflow", args(1,4, batarg("",bte),arg("s1",int),batarg("v",lng),batarg("s",oid))),
 command("batcalc", "bte", batlng_dec2dec_bte, false, "cast decimal(lng) to decimal(bte) and check for overflow", args(1,6, batarg("",bte),arg("s1",int),batarg("v",lng),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "sht", bte_num2dec_sht, false, "cast number to decimal(sht) and check for overflow", args(1,4, arg("",sht),arg("v",bte),arg("digits",int),arg("scale",int))),
 command("batcalc", "sht", batbte_num2dec_sht, false, "cast number to decimal(sht) and check for overflow", args(1,5, batarg("",sht),batarg("v",bte),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "sht", bte_dec2_sht, false, "cast decimal(bte) to sht and check for overflow", args(1,3, arg("",sht),arg("s1",int),arg("v",bte))),
 command("calc", "sht", bte_dec2dec_sht, false, "cast decimal(bte) to decimal(sht) and check for overflow", args(1,5, arg("",sht),arg("s1",int),arg("v",bte),arg("d2",int),arg("s2",int))),
 command("batcalc", "sht", batbte_dec2_sht, false, "cast decimal(bte) to sht and check for overflow", args(1,4, batarg("",sht),arg("s1",int),batarg("v",bte),batarg("s",oid))),
 command("batcalc", "sht", batbte_dec2dec_sht, false, "cast decimal(bte) to decimal(sht) and check for overflow", args(1,6, batarg("",sht),arg("s1",int),batarg("v",bte),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "sht", sht_num2dec_sht, false, "cast number to decimal(sht) and check for overflow", args(1,4, arg("",sht),arg("v",sht),arg("digits",int),arg("scale",int))),
 command("batcalc", "sht", batsht_num2dec_sht, false, "cast number to decimal(sht) and check for overflow", args(1,5, batarg("",sht),batarg("v",sht),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "sht", sht_dec2_sht, false, "cast decimal(sht) to sht and check for overflow", args(1,3, arg("",sht),arg("s1",int),arg("v",sht))),
 command("calc", "sht", sht_dec2dec_sht, false, "cast decimal(sht) to decimal(sht) and check for overflow", args(1,5, arg("",sht),arg("s1",int),arg("v",sht),arg("d2",int),arg("s2",int))),
 command("batcalc", "sht", batsht_dec2_sht, false, "cast decimal(sht) to sht and check for overflow", args(1,4, batarg("",sht),arg("s1",int),batarg("v",sht),batarg("s",oid))),
 command("batcalc", "sht", batsht_dec2dec_sht, false, "cast decimal(sht) to decimal(sht) and check for overflow", args(1,6, batarg("",sht),arg("s1",int),batarg("v",sht),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "sht", int_num2dec_sht, false, "cast number to decimal(sht) and check for overflow", args(1,4, arg("",sht),arg("v",int),arg("digits",int),arg("scale",int))),
 command("batcalc", "sht", batint_num2dec_sht, false, "cast number to decimal(sht) and check for overflow", args(1,5, batarg("",sht),batarg("v",int),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "sht", int_dec2_sht, false, "cast decimal(int) to sht and check for overflow", args(1,3, arg("",sht),arg("s1",int),arg("v",int))),
 command("calc", "sht", int_dec2dec_sht, false, "cast decimal(int) to decimal(sht) and check for overflow", args(1,5, arg("",sht),arg("s1",int),arg("v",int),arg("d2",int),arg("s2",int))),
 command("batcalc", "sht", batint_dec2_sht, false, "cast decimal(int) to sht and check for overflow", args(1,4, batarg("",sht),arg("s1",int),batarg("v",int),batarg("s",oid))),
 command("batcalc", "sht", batint_dec2dec_sht, false, "cast decimal(int) to decimal(sht) and check for overflow", args(1,6, batarg("",sht),arg("s1",int),batarg("v",int),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "sht", lng_num2dec_sht, false, "cast number to decimal(sht) and check for overflow", args(1,4, arg("",sht),arg("v",lng),arg("digits",int),arg("scale",int))),
 command("batcalc", "sht", batlng_num2dec_sht, false, "cast number to decimal(sht) and check for overflow", args(1,5, batarg("",sht),batarg("v",lng),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "sht", lng_dec2_sht, false, "cast decimal(lng) to sht and check for overflow", args(1,3, arg("",sht),arg("s1",int),arg("v",lng))),
 command("calc", "sht", lng_dec2dec_sht, false, "cast decimal(lng) to decimal(sht) and check for overflow", args(1,5, arg("",sht),arg("s1",int),arg("v",lng),arg("d2",int),arg("s2",int))),
 command("batcalc", "sht", batlng_dec2_sht, false, "cast decimal(lng) to sht and check for overflow", args(1,4, batarg("",sht),arg("s1",int),batarg("v",lng),batarg("s",oid))),
 command("batcalc", "sht", batlng_dec2dec_sht, false, "cast decimal(lng) to decimal(sht) and check for overflow", args(1,6, batarg("",sht),arg("s1",int),batarg("v",lng),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "int", bte_num2dec_int, false, "cast number to decimal(int) and check for overflow", args(1,4, arg("",int),arg("v",bte),arg("digits",int),arg("scale",int))),
 command("batcalc", "int", batbte_num2dec_int, false, "cast number to decimal(int) and check for overflow", args(1,5, batarg("",int),batarg("v",bte),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "int", bte_dec2_int, false, "cast decimal(bte) to int and check for overflow", args(1,3, arg("",int),arg("s1",int),arg("v",bte))),
 command("calc", "int", bte_dec2dec_int, false, "cast decimal(bte) to decimal(int) and check for overflow", args(1,5, arg("",int),arg("s1",int),arg("v",bte),arg("d2",int),arg("s2",int))),
 command("batcalc", "int", batbte_dec2_int, false, "cast decimal(bte) to int and check for overflow", args(1,4, batarg("",int),arg("s1",int),batarg("v",bte),batarg("s",oid))),
 command("batcalc", "int", batbte_dec2dec_int, false, "cast decimal(bte) to decimal(int) and check for overflow", args(1,6, batarg("",int),arg("s1",int),batarg("v",bte),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "int", sht_num2dec_int, false, "cast number to decimal(int) and check for overflow", args(1,4, arg("",int),arg("v",sht),arg("digits",int),arg("scale",int))),
 command("batcalc", "int", batsht_num2dec_int, false, "cast number to decimal(int) and check for overflow", args(1,5, batarg("",int),batarg("v",sht),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "int", sht_dec2_int, false, "cast decimal(sht) to int and check for overflow", args(1,3, arg("",int),arg("s1",int),arg("v",sht))),
 command("calc", "int", sht_dec2dec_int, false, "cast decimal(sht) to decimal(int) and check for overflow", args(1,5, arg("",int),arg("s1",int),arg("v",sht),arg("d2",int),arg("s2",int))),
 command("batcalc", "int", batsht_dec2_int, false, "cast decimal(sht) to int and check for overflow", args(1,4, batarg("",int),arg("s1",int),batarg("v",sht),batarg("s",oid))),
 command("batcalc", "int", batsht_dec2dec_int, false, "cast decimal(sht) to decimal(int) and check for overflow", args(1,6, batarg("",int),arg("s1",int),batarg("v",sht),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "int", int_num2dec_int, false, "cast number to decimal(int) and check for overflow", args(1,4, arg("",int),arg("v",int),arg("digits",int),arg("scale",int))),
 command("batcalc", "int", batint_num2dec_int, false, "cast number to decimal(int) and check for overflow", args(1,5, batarg("",int),batarg("v",int),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "int", int_dec2_int, false, "cast decimal(int) to int and check for overflow", args(1,3, arg("",int),arg("s1",int),arg("v",int))),
 command("calc", "int", int_dec2dec_int, false, "cast decimal(int) to decimal(int) and check for overflow", args(1,5, arg("",int),arg("s1",int),arg("v",int),arg("d2",int),arg("s2",int))),
 command("batcalc", "int", batint_dec2_int, false, "cast decimal(int) to int and check for overflow", args(1,4, batarg("",int),arg("s1",int),batarg("v",int),batarg("s",oid))),
 command("batcalc", "int", batint_dec2dec_int, false, "cast decimal(int) to decimal(int) and check for overflow", args(1,6, batarg("",int),arg("s1",int),batarg("v",int),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "int", lng_num2dec_int, false, "cast number to decimal(int) and check for overflow", args(1,4, arg("",int),arg("v",lng),arg("digits",int),arg("scale",int))),
 command("batcalc", "int", batlng_num2dec_int, false, "cast number to decimal(int) and check for overflow", args(1,5, batarg("",int),batarg("v",lng),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "int", lng_dec2_int, false, "cast decimal(lng) to int and check for overflow", args(1,3, arg("",int),arg("s1",int),arg("v",lng))),
 command("calc", "int", lng_dec2dec_int, false, "cast decimal(lng) to decimal(int) and check for overflow", args(1,5, arg("",int),arg("s1",int),arg("v",lng),arg("d2",int),arg("s2",int))),
 command("batcalc", "int", batlng_dec2_int, false, "cast decimal(lng) to int and check for overflow", args(1,4, batarg("",int),arg("s1",int),batarg("v",lng),batarg("s",oid))),
 command("batcalc", "int", batlng_dec2dec_int, false, "cast decimal(lng) to decimal(int) and check for overflow", args(1,6, batarg("",int),arg("s1",int),batarg("v",lng),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "lng", bte_num2dec_lng, false, "cast number to decimal(lng) and check for overflow", args(1,4, arg("",lng),arg("v",bte),arg("digits",int),arg("scale",int))),
 command("batcalc", "lng", batbte_num2dec_lng, false, "cast number to decimal(lng) and check for overflow", args(1,5, batarg("",lng),batarg("v",bte),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "lng", bte_dec2_lng, false, "cast decimal(bte) to lng and check for overflow", args(1,3, arg("",lng),arg("s1",int),arg("v",bte))),
 command("calc", "lng", bte_dec2dec_lng, false, "cast decimal(bte) to decimal(lng) and check for overflow", args(1,5, arg("",lng),arg("s1",int),arg("v",bte),arg("d2",int),arg("s2",int))),
 command("batcalc", "lng", batbte_dec2_lng, false, "cast decimal(bte) to lng and check for overflow", args(1,4, batarg("",lng),arg("s1",int),batarg("v",bte),batarg("s",oid))),
 command("batcalc", "lng", batbte_dec2dec_lng, false, "cast decimal(bte) to decimal(lng) and check for overflow", args(1,6, batarg("",lng),arg("s1",int),batarg("v",bte),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "lng", sht_num2dec_lng, false, "cast number to decimal(lng) and check for overflow", args(1,4, arg("",lng),arg("v",sht),arg("digits",int),arg("scale",int))),
 command("batcalc", "lng", batsht_num2dec_lng, false, "cast number to decimal(lng) and check for overflow", args(1,5, batarg("",lng),batarg("v",sht),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "lng", sht_dec2_lng, false, "cast decimal(sht) to lng and check for overflow", args(1,3, arg("",lng),arg("s1",int),arg("v",sht))),
 command("calc", "lng", sht_dec2dec_lng, false, "cast decimal(sht) to decimal(lng) and check for overflow", args(1,5, arg("",lng),arg("s1",int),arg("v",sht),arg("d2",int),arg("s2",int))),
 command("batcalc", "lng", batsht_dec2_lng, false, "cast decimal(sht) to lng and check for overflow", args(1,4, batarg("",lng),arg("s1",int),batarg("v",sht),batarg("s",oid))),
 command("batcalc", "lng", batsht_dec2dec_lng, false, "cast decimal(sht) to decimal(lng) and check for overflow", args(1,6, batarg("",lng),arg("s1",int),batarg("v",sht),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "lng", int_num2dec_lng, false, "cast number to decimal(lng) and check for overflow", args(1,4, arg("",lng),arg("v",int),arg("digits",int),arg("scale",int))),
 command("batcalc", "lng", batint_num2dec_lng, false, "cast number to decimal(lng) and check for overflow", args(1,5, batarg("",lng),batarg("v",int),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "lng", int_dec2_lng, false, "cast decimal(int) to lng and check for overflow", args(1,3, arg("",lng),arg("s1",int),arg("v",int))),
 command("calc", "lng", int_dec2dec_lng, false, "cast decimal(int) to decimal(lng) and check for overflow", args(1,5, arg("",lng),arg("s1",int),arg("v",int),arg("d2",int),arg("s2",int))),
 command("batcalc", "lng", batint_dec2_lng, false, "cast decimal(int) to lng and check for overflow", args(1,4, batarg("",lng),arg("s1",int),batarg("v",int),batarg("s",oid))),
 command("batcalc", "lng", batint_dec2dec_lng, false, "cast decimal(int) to decimal(lng) and check for overflow", args(1,6, batarg("",lng),arg("s1",int),batarg("v",int),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "lng", lng_num2dec_lng, false, "cast number to decimal(lng) and check for overflow", args(1,4, arg("",lng),arg("v",lng),arg("digits",int),arg("scale",int))),
 command("batcalc", "lng", batlng_num2dec_lng, false, "cast number to decimal(lng) and check for overflow", args(1,5, batarg("",lng),batarg("v",lng),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "lng", lng_dec2_lng, false, "cast decimal(lng) to lng and check for overflow", args(1,3, arg("",lng),arg("s1",int),arg("v",lng))),
 command("calc", "lng", lng_dec2dec_lng, false, "cast decimal(lng) to decimal(lng) and check for overflow", args(1,5, arg("",lng),arg("s1",int),arg("v",lng),arg("d2",int),arg("s2",int))),
 command("batcalc", "lng", batlng_dec2_lng, false, "cast decimal(lng) to lng and check for overflow", args(1,4, batarg("",lng),arg("s1",int),batarg("v",lng),batarg("s",oid))),
 command("batcalc", "lng", batlng_dec2dec_lng, false, "cast decimal(lng) to decimal(lng) and check for overflow", args(1,6, batarg("",lng),arg("s1",int),batarg("v",lng),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "flt", bte_num2dec_flt, false, "cast number to decimal(flt) and check for overflow", args(1,4, arg("",flt),arg("v",bte),arg("digits",int),arg("scale",int))),
 command("batcalc", "flt", batbte_num2dec_flt, false, "cast number to decimal(flt) and check for overflow", args(1,5, batarg("",flt),batarg("v",bte),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "flt", bte_dec2_flt, false, "cast decimal(bte) to flt and check for overflow", args(1,3, arg("",flt),arg("s1",int),arg("v",bte))),
 command("calc", "flt", bte_dec2dec_flt, false, "cast decimal(bte) to decimal(flt) and check for overflow", args(1,5, arg("",flt),arg("s1",int),arg("v",bte),arg("d2",int),arg("s2",int))),
 command("batcalc", "flt", batbte_dec2_flt, false, "cast decimal(bte) to flt and check for overflow", args(1,4, batarg("",flt),arg("s1",int),batarg("v",bte),batarg("s",oid))),
 command("batcalc", "flt", batbte_dec2dec_flt, false, "cast decimal(bte) to decimal(flt) and check for overflow", args(1,6, batarg("",flt),arg("s1",int),batarg("v",bte),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "flt", sht_num2dec_flt, false, "cast number to decimal(flt) and check for overflow", args(1,4, arg("",flt),arg("v",sht),arg("digits",int),arg("scale",int))),
 command("batcalc", "flt", batsht_num2dec_flt, false, "cast number to decimal(flt) and check for overflow", args(1,5, batarg("",flt),batarg("v",sht),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "flt", sht_dec2_flt, false, "cast decimal(sht) to flt and check for overflow", args(1,3, arg("",flt),arg("s1",int),arg("v",sht))),
 command("calc", "flt", sht_dec2dec_flt, false, "cast decimal(sht) to decimal(flt) and check for overflow", args(1,5, arg("",flt),arg("s1",int),arg("v",sht),arg("d2",int),arg("s2",int))),
 command("batcalc", "flt", batsht_dec2_flt, false, "cast decimal(sht) to flt and check for overflow", args(1,4, batarg("",flt),arg("s1",int),batarg("v",sht),batarg("s",oid))),
 command("batcalc", "flt", batsht_dec2dec_flt, false, "cast decimal(sht) to decimal(flt) and check for overflow", args(1,6, batarg("",flt),arg("s1",int),batarg("v",sht),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "flt", int_num2dec_flt, false, "cast number to decimal(flt) and check for overflow", args(1,4, arg("",flt),arg("v",int),arg("digits",int),arg("scale",int))),
 command("batcalc", "flt", batint_num2dec_flt, false, "cast number to decimal(flt) and check for overflow", args(1,5, batarg("",flt),batarg("v",int),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "flt", int_dec2_flt, false, "cast decimal(int) to flt and check for overflow", args(1,3, arg("",flt),arg("s1",int),arg("v",int))),
 command("calc", "flt", int_dec2dec_flt, false, "cast decimal(int) to decimal(flt) and check for overflow", args(1,5, arg("",flt),arg("s1",int),arg("v",int),arg("d2",int),arg("s2",int))),
 command("batcalc", "flt", batint_dec2_flt, false, "cast decimal(int) to flt and check for overflow", args(1,4, batarg("",flt),arg("s1",int),batarg("v",int),batarg("s",oid))),
 command("batcalc", "flt", batint_dec2dec_flt, false, "cast decimal(int) to decimal(flt) and check for overflow", args(1,6, batarg("",flt),arg("s1",int),batarg("v",int),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "flt", lng_num2dec_flt, false, "cast number to decimal(flt) and check for overflow", args(1,4, arg("",flt),arg("v",lng),arg("digits",int),arg("scale",int))),
 command("batcalc", "flt", batlng_num2dec_flt, false, "cast number to decimal(flt) and check for overflow", args(1,5, batarg("",flt),batarg("v",lng),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "flt", lng_dec2_flt, false, "cast decimal(lng) to flt and check for overflow", args(1,3, arg("",flt),arg("s1",int),arg("v",lng))),
 command("calc", "flt", lng_dec2dec_flt, false, "cast decimal(lng) to decimal(flt) and check for overflow", args(1,5, arg("",flt),arg("s1",int),arg("v",lng),arg("d2",int),arg("s2",int))),
 command("batcalc", "flt", batlng_dec2_flt, false, "cast decimal(lng) to flt and check for overflow", args(1,4, batarg("",flt),arg("s1",int),batarg("v",lng),batarg("s",oid))),
 command("batcalc", "flt", batlng_dec2dec_flt, false, "cast decimal(lng) to decimal(flt) and check for overflow", args(1,6, batarg("",flt),arg("s1",int),batarg("v",lng),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "dbl", bte_num2dec_dbl, false, "cast number to decimal(dbl) and check for overflow", args(1,4, arg("",dbl),arg("v",bte),arg("digits",int),arg("scale",int))),
 command("batcalc", "dbl", batbte_num2dec_dbl, false, "cast number to decimal(dbl) and check for overflow", args(1,5, batarg("",dbl),batarg("v",bte),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "dbl", bte_dec2_dbl, false, "cast decimal(bte) to dbl and check for overflow", args(1,3, arg("",dbl),arg("s1",int),arg("v",bte))),
 command("calc", "dbl", bte_dec2dec_dbl, false, "cast decimal(bte) to decimal(dbl) and check for overflow", args(1,5, arg("",dbl),arg("s1",int),arg("v",bte),arg("d2",int),arg("s2",int))),
 command("batcalc", "dbl", batbte_dec2_dbl, false, "cast decimal(bte) to dbl and check for overflow", args(1,4, batarg("",dbl),arg("s1",int),batarg("v",bte),batarg("s",oid))),
 command("batcalc", "dbl", batbte_dec2dec_dbl, false, "cast decimal(bte) to decimal(dbl) and check for overflow", args(1,6, batarg("",dbl),arg("s1",int),batarg("v",bte),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "dbl", sht_num2dec_dbl, false, "cast number to decimal(dbl) and check for overflow", args(1,4, arg("",dbl),arg("v",sht),arg("digits",int),arg("scale",int))),
 command("batcalc", "dbl", batsht_num2dec_dbl, false, "cast number to decimal(dbl) and check for overflow", args(1,5, batarg("",dbl),batarg("v",sht),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "dbl", sht_dec2_dbl, false, "cast decimal(sht) to dbl and check for overflow", args(1,3, arg("",dbl),arg("s1",int),arg("v",sht))),
 command("calc", "dbl", sht_dec2dec_dbl, false, "cast decimal(sht) to decimal(dbl) and check for overflow", args(1,5, arg("",dbl),arg("s1",int),arg("v",sht),arg("d2",int),arg("s2",int))),
 command("batcalc", "dbl", batsht_dec2_dbl, false, "cast decimal(sht) to dbl and check for overflow", args(1,4, batarg("",dbl),arg("s1",int),batarg("v",sht),batarg("s",oid))),
 command("batcalc", "dbl", batsht_dec2dec_dbl, false, "cast decimal(sht) to decimal(dbl) and check for overflow", args(1,6, batarg("",dbl),arg("s1",int),batarg("v",sht),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "dbl", int_num2dec_dbl, false, "cast number to decimal(dbl) and check for overflow", args(1,4, arg("",dbl),arg("v",int),arg("digits",int),arg("scale",int))),
 command("batcalc", "dbl", batint_num2dec_dbl, false, "cast number to decimal(dbl) and check for overflow", args(1,5, batarg("",dbl),batarg("v",int),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "dbl", int_dec2_dbl, false, "cast decimal(int) to dbl and check for overflow", args(1,3, arg("",dbl),arg("s1",int),arg("v",int))),
 command("calc", "dbl", int_dec2dec_dbl, false, "cast decimal(int) to decimal(dbl) and check for overflow", args(1,5, arg("",dbl),arg("s1",int),arg("v",int),arg("d2",int),arg("s2",int))),
 command("batcalc", "dbl", batint_dec2_dbl, false, "cast decimal(int) to dbl and check for overflow", args(1,4, batarg("",dbl),arg("s1",int),batarg("v",int),batarg("s",oid))),
 command("batcalc", "dbl", batint_dec2dec_dbl, false, "cast decimal(int) to decimal(dbl) and check for overflow", args(1,6, batarg("",dbl),arg("s1",int),batarg("v",int),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "dbl", lng_num2dec_dbl, false, "cast number to decimal(dbl) and check for overflow", args(1,4, arg("",dbl),arg("v",lng),arg("digits",int),arg("scale",int))),
 command("batcalc", "dbl", batlng_num2dec_dbl, false, "cast number to decimal(dbl) and check for overflow", args(1,5, batarg("",dbl),batarg("v",lng),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "dbl", lng_dec2_dbl, false, "cast decimal(lng) to dbl and check for overflow", args(1,3, arg("",dbl),arg("s1",int),arg("v",lng))),
 command("calc", "dbl", lng_dec2dec_dbl, false, "cast decimal(lng) to decimal(dbl) and check for overflow", args(1,5, arg("",dbl),arg("s1",int),arg("v",lng),arg("d2",int),arg("s2",int))),
 command("batcalc", "dbl", batlng_dec2_dbl, false, "cast decimal(lng) to dbl and check for overflow", args(1,4, batarg("",dbl),arg("s1",int),batarg("v",lng),batarg("s",oid))),
 command("batcalc", "dbl", batlng_dec2dec_dbl, false, "cast decimal(lng) to decimal(dbl) and check for overflow", args(1,6, batarg("",dbl),arg("s1",int),batarg("v",lng),batarg("s",oid),arg("d2",int),arg("s2",int))),
 /* sql_rank */
 pattern("sql", "diff", SQLdiff, false, "return true if cur != prev row", args(1,2, arg("",bit),argany("b",1))),
 pattern("batsql", "diff", SQLdiff, false, "return true if cur != prev row", args(1,2, batarg("",bit),batargany("b",1))),
 pattern("sql", "diff", SQLdiff, false, "return true if cur != prev row", args(1,3, arg("",bit),arg("p",bit),argany("b",1))),
 pattern("batsql", "diff", SQLdiff, false, "return true if cur != prev row", args(1,3, batarg("",bit),arg("p",bit),batargany("b",1))),
 pattern("batsql", "diff", SQLdiff, false, "return true if cur != prev row", args(1,3, batarg("",bit),batarg("p",bit),argany("b",1))),
 pattern("batsql", "diff", SQLdiff, false, "return true if cur != prev row", args(1,3, batarg("",bit),batarg("p",bit),batargany("b",1))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, arg("",oid),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",bte))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",bte))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, arg("",oid),arg("p",bit),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",bte))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",bte))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),batarg("limit",bte))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),batarg("limit",bte))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, arg("",oid),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",sht))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",sht))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, arg("",oid),arg("p",bit),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",sht))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",sht))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),batarg("limit",sht))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),batarg("limit",sht))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, arg("",oid),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",int))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",int))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, arg("",oid),arg("p",bit),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",int))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",int))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),batarg("limit",int))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),batarg("limit",int))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, arg("",oid),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",lng))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",lng))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, arg("",oid),arg("p",bit),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",lng))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",lng))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),batarg("limit",lng))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),batarg("limit",lng))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, arg("",oid),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",flt))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",flt))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, arg("",oid),arg("p",bit),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",flt))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",flt))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),batarg("limit",flt))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),batarg("limit",flt))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, arg("",oid),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",dbl))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",dbl))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, arg("",oid),arg("p",bit),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",dbl))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",dbl))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),batarg("limit",dbl))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),batarg("limit",dbl))),
 pattern("sql", "row_number", SQLrow_number, false, "return the row_numer-ed groups", args(1,4, arg("",int),argany("b",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "row_number", SQLrow_number, false, "return the row_numer-ed groups", args(1,4, batarg("",int),batargany("b",1),argany("p",2),argany("o",3))),
 pattern("sql", "rank", SQLrank, false, "return the ranked groups", args(1,4, arg("",int),argany("b",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "rank", SQLrank, false, "return the ranked groups", args(1,4, batarg("",int),batargany("b",1),argany("p",2),argany("o",3))),
 pattern("sql", "dense_rank", SQLdense_rank, false, "return the densely ranked groups", args(1,4, arg("",int),argany("b",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "dense_rank", SQLdense_rank, false, "return the densely ranked groups", args(1,4, batarg("",int),batargany("b",1),argany("p",2),argany("o",3))),
 pattern("sql", "percent_rank", SQLpercent_rank, false, "return the percentage into the total number of groups for each row", args(1,4, arg("",dbl),argany("b",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "percent_rank", SQLpercent_rank, false, "return the percentage into the total number of groups for each row", args(1,4, batarg("",dbl),batargany("b",1),argany("p",2),argany("o",3))),
 pattern("sql", "cume_dist", SQLcume_dist, false, "return the accumulated distribution of the number of rows per group to the total number of partition rows", args(1,4, arg("",dbl),argany("b",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "cume_dist", SQLcume_dist, false, "return the accumulated distribution of the number of rows per group to the total number of partition rows", args(1,4, batarg("",dbl),batargany("b",1),argany("p",2),argany("o",3))),
 pattern("sql", "lag", SQLlag, false, "return the value in the previous row in the partition or NULL if non existent", args(1,4, argany("",1),argany("b",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "lag", SQLlag, false, "return the value in the previous row in the partition or NULL if non existent", args(1,4, batargany("",1),batargany("b",1),argany("p",2),argany("o",3))),
 pattern("sql", "lag", SQLlag, false, "return the value in the previous 'l' row in the partition or NULL if non existent", args(1,5, argany("",1),argany("b",1),argany("l",0),arg("p",bit),arg("o",bit))),
 pattern("batsql", "lag", SQLlag, false, "return the value in the previous 'l' row in the partition or NULL if non existent", args(1,5, batargany("",1),batargany("b",1),argany("l",0),argany("p",2),argany("o",3))),
 pattern("batsql", "lag", SQLlag, false, "return the value in the previous 'l' row in the partition or NULL if non existent", args(1,5, batargany("",1),argany("b",1),batargany("l",0),argany("p",2),argany("o",3))),
 pattern("batsql", "lag", SQLlag, false, "return the value in the previous 'l' row in the partition or NULL if non existent", args(1,5, batargany("",1),batargany("b",1),batargany("l",0),argany("p",2),argany("o",3))),
 pattern("sql", "lag", SQLlag, false, "return the value in the previous 'l' row in the partition or 'd' if non existent", args(1,6, argany("",1),argany("b",1),argany("l",0),argany("d",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "lag", SQLlag, false, "return the value in the previous 'l' row in the partition or 'd' if non existent", args(1,6, batargany("",1),batargany("b",1),argany("l",0),argany("d",1),argany("p",2),argany("o",3))),
 pattern("batsql", "lag", SQLlag, false, "return the value in the previous 'l' row in the partition or 'd' if non existent", args(1,6, batargany("",1),argany("b",1),batargany("l",0),argany("d",1),argany("p",2),argany("o",3))),
 pattern("batsql", "lag", SQLlag, false, "return the value in the previous 'l' row in the partition or 'd' if non existent", args(1,6, batargany("",1),batargany("b",1),batargany("l",0),argany("d",1),argany("p",2),argany("o",3))),
 pattern("batsql", "lag", SQLlag, false, "return the value in the previous 'l' row in the partition or 'd' if non existent", args(1,6, batargany("",1),argany("b",1),argany("l",0),batargany("d",1),argany("p",2),argany("o",3))),
 pattern("batsql", "lag", SQLlag, false, "return the value in the previous 'l' row in the partition or 'd' if non existent", args(1,6, batargany("",1),batargany("b",1),argany("l",0),batargany("d",1),argany("p",2),argany("o",3))),
 pattern("batsql", "lag", SQLlag, false, "return the value in the previous 'l' row in the partition or 'd' if non existent", args(1,6, batargany("",1),argany("b",1),batargany("l",0),batargany("d",1),argany("p",2),argany("o",3))),
 pattern("batsql", "lag", SQLlag, false, "return the value in the previous 'l' row in the partition or 'd' if non existent", args(1,6, batargany("",1),batargany("b",1),batargany("l",0),batargany("d",1),argany("p",2),argany("o",3))),
 pattern("sql", "lead", SQLlead, false, "return the value in the next row in the partition or NULL if non existent", args(1,4, argany("",1),argany("b",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "lead", SQLlead, false, "return the value in the next row in the partition or NULL if non existent", args(1,4, batargany("",1),batargany("b",1),argany("p",2),argany("o",3))),
 pattern("sql", "lead", SQLlead, false, "return the value in the next 'l' row in the partition or NULL if non existent", args(1,5, argany("",1),argany("b",1),argany("l",0),arg("p",bit),arg("o",bit))),
 pattern("batsql", "lead", SQLlead, false, "return the value in the next 'l' row in the partition or NULL if non existent", args(1,5, batargany("",1),batargany("b",1),argany("l",0),argany("p",2),argany("o",3))),
 pattern("batsql", "lead", SQLlead, false, "return the value in the next 'l' row in the partition or NULL if non existent", args(1,5, batargany("",1),argany("b",1),batargany("l",0),argany("p",2),argany("o",3))),
 pattern("batsql", "lead", SQLlead, false, "return the value in the next 'l' row in the partition or NULL if non existent", args(1,5, batargany("",1),batargany("b",1),batargany("l",0),argany("p",2),argany("o",3))),
 pattern("sql", "lead", SQLlead, false, "return the value in the next 'l' row in the partition or 'd' if non existent", args(1,6, argany("",1),argany("b",1),argany("l",0),argany("d",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "lead", SQLlead, false, "return the value in the next 'l' row in the partition or 'd' if non existent", args(1,6, batargany("",1),batargany("b",1),argany("l",0),argany("d",1),argany("p",2),argany("o",3))),
 pattern("batsql", "lead", SQLlead, false, "return the value in the next 'l' row in the partition or 'd' if non existent", args(1,6, batargany("",1),argany("b",1),batargany("l",0),argany("d",1),argany("p",2),argany("o",3))),
 pattern("batsql", "lead", SQLlead, false, "return the value in the next 'l' row in the partition or 'd' if non existent", args(1,6, batargany("",1),batargany("b",1),batargany("l",0),argany("d",1),argany("p",2),argany("o",3))),
 pattern("batsql", "lead", SQLlead, false, "return the value in the next 'l' row in the partition or 'd' if non existent", args(1,6, batargany("",1),argany("b",1),argany("l",0),batargany("d",1),argany("p",2),argany("o",3))),
 pattern("batsql", "lead", SQLlead, false, "return the value in the next 'l' row in the partition or 'd' if non existent", args(1,6, batargany("",1),batargany("b",1),argany("l",0),batargany("d",1),argany("p",2),argany("o",3))),
 pattern("batsql", "lead", SQLlead, false, "return the value in the next 'l' row in the partition or 'd' if non existent", args(1,6, batargany("",1),argany("b",1),batargany("l",0),batargany("d",1),argany("p",2),argany("o",3))),
 pattern("batsql", "lead", SQLlead, false, "return the value in the next 'l' row in the partition or 'd' if non existent", args(1,6, batargany("",1),batargany("b",1),batargany("l",0),batargany("d",1),argany("p",2),argany("o",3))),
 pattern("sql", "ntile", SQLntile, false, "return the groups divided as equally as possible", args(1,5, argany("",1),argany("b",0),argany("n",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "ntile", SQLntile, false, "return the groups divided as equally as possible", args(1,5, batargany("",1),batargany("b",0),argany("n",1),argany("p",2),argany("o",3))),
 pattern("batsql", "ntile", SQLntile, false, "return the groups divided as equally as possible", args(1,5, batargany("",1),argany("b",0),batargany("n",1),argany("p",2),argany("o",3))),
 pattern("batsql", "ntile", SQLntile, false, "return the groups divided as equally as possible", args(1,5, batargany("",1),batargany("b",0),batargany("n",1),argany("p",2),argany("o",3))),

 /* these window functions support frames */
 pattern("sql", "first_value", SQLfirst_value, false, "return the first value of groups", args(1,7, argany("",1),argany("b",1),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "first_value", SQLfirst_value, false, "return the first value of groups", args(1,7, batargany("",1),batargany("b",1),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "last_value", SQLlast_value, false, "return the last value of groups", args(1,7, argany("",1),argany("b",1),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "last_value", SQLlast_value, false, "return the last value of groups", args(1,7, batargany("",1),batargany("b",1),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "nth_value", SQLnth_value, false, "return the nth value of each group", args(1,8, argany("",1),argany("b",1),arg("n",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "nth_value", SQLnth_value, false, "return the nth value of each group", args(1,8, batargany("",1),batargany("b",1),arg("n",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "nth_value", SQLnth_value, false, "return the nth value of each group", args(1,8, batargany("",1),argany("b",1),batarg("n",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "nth_value", SQLnth_value, false, "return the nth value of each group", args(1,8, batargany("",1),batargany("b",1),batarg("n",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "min", SQLmin, false, "return the minimum of groups", args(1,7, argany("",1),argany("b",1),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "min", SQLmin, false, "return the minimum of groups", args(1,7, batargany("",1),batargany("b",1),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "max", SQLmax, false, "return the maximum of groups", args(1,7, argany("",1),argany("b",1),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "max", SQLmax, false, "return the maximum of groups",args(1,7, batargany("",1),batargany("b",1),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "count", SQLcount, false, "return count of groups", args(1,8, arg("",lng),argany("b",1),arg("ignils",bit),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "count", SQLcount, false,"return count of groups",args(1,8, batarg("",lng),batargany("b",1),arg("ignils",bit),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",lng),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",lng),batarg("b",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",lng),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",lng),batarg("b",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",lng),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",lng),batarg("b",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",lng),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",lng),batarg("b",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",flt),arg("b",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",flt),batarg("b",flt),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",dbl),arg("b",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",dbl),batarg("b",flt),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",dbl),arg("b",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",dbl),batarg("b",dbl),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",lng),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",lng),batarg("b",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",lng),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",lng),batarg("b",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",lng),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",lng),batarg("b",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",lng),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",lng),batarg("b",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",flt),arg("b",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",flt),batarg("b",flt),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",dbl),arg("b",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",dbl),batarg("b",flt),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",dbl),arg("b",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",dbl),batarg("b",dbl),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "avg", SQLavg, false, "return the average of groups", args(1,7, arg("",dbl),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavg, false, "return the average of groups", args(1,7, batarg("",dbl),batarg("b",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "avg", SQLavg, false, "return the average of groups", args(1,7, arg("",dbl),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavg, false, "return the average of groups", args(1,7, batarg("",dbl),batarg("b",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "avg", SQLavg, false, "return the average of groups", args(1,7, arg("",dbl),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavg, false, "return the average of groups", args(1,7, batarg("",dbl),batarg("b",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "avg", SQLavg, false, "return the average of groups", args(1,7, arg("",dbl),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavg, false, "return the average of groups", args(1,7, batarg("",dbl),batarg("b",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "avg", SQLavg, false, "return the average of groups", args(1,7, arg("",dbl),arg("b",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavg, false, "return the average of groups", args(1,7, batarg("",dbl),batarg("b",flt),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "avg", SQLavg, false, "return the average of groups", args(1,7, arg("",dbl),arg("b",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavg, false, "return the average of groups", args(1,7, batarg("",dbl),batarg("b",dbl),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, arg("",bte),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, batarg("",bte),batarg("b",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, arg("",sht),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, batarg("",sht),batarg("b",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, arg("",int),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, batarg("",int),batarg("b",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, arg("",lng),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, batarg("",lng),batarg("b",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, arg("",dbl),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, batarg("",dbl),batarg("b",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, arg("",dbl),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, batarg("",dbl),batarg("b",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, arg("",dbl),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, batarg("",dbl),batarg("b",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, arg("",dbl),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, batarg("",dbl),batarg("b",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, arg("",dbl),arg("b",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, batarg("",dbl),batarg("b",flt),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, arg("",dbl),arg("b",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, batarg("",dbl),batarg("b",dbl),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, arg("",dbl),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, batarg("",dbl),batarg("b",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, arg("",dbl),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, batarg("",dbl),batarg("b",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, arg("",dbl),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, batarg("",dbl),batarg("b",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, arg("",dbl),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, batarg("",dbl),batarg("b",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, arg("",dbl),arg("b",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, batarg("",dbl),batarg("b",flt),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, arg("",dbl),arg("b",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, batarg("",dbl),batarg("b",dbl),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, arg("",dbl),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, batarg("",dbl),batarg("b",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, arg("",dbl),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, batarg("",dbl),batarg("b",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, arg("",dbl),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, batarg("",dbl),batarg("b",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, arg("",dbl),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, batarg("",dbl),batarg("b",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, arg("",dbl),arg("b",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, batarg("",dbl),batarg("b",flt),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, arg("",dbl),arg("b",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, batarg("",dbl),batarg("b",dbl),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, arg("",dbl),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, batarg("",dbl),batarg("b",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, arg("",dbl),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, batarg("",dbl),batarg("b",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, arg("",dbl),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, batarg("",dbl),batarg("b",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, arg("",dbl),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, batarg("",dbl),batarg("b",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, arg("",dbl),arg("b",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, batarg("",dbl),batarg("b",flt),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, arg("",dbl),arg("b",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, batarg("",dbl),batarg("b",dbl),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, arg("",dbl),arg("b",bte),arg("c",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),arg("b",bte),batarg("c",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),batarg("b",bte),arg("c",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),batarg("b",bte),batarg("c",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, arg("",dbl),arg("b",sht),arg("c",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),arg("b",sht),batarg("c",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),batarg("b",sht),arg("c",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),batarg("b",sht),batarg("c",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, arg("",dbl),arg("b",int),arg("c",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),arg("b",int),batarg("c",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),batarg("b",int),arg("c",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),batarg("b",int),batarg("c",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, arg("",dbl),arg("b",lng),arg("c",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),arg("b",lng),batarg("c",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),batarg("b",lng),arg("c",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),batarg("b",lng),batarg("c",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, arg("",dbl),arg("b",flt),arg("c",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),arg("b",flt),batarg("c",flt),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),batarg("b",flt),arg("c",flt),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),batarg("b",flt),batarg("c",flt),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, arg("",dbl),arg("b",dbl),arg("c",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),arg("b",dbl),batarg("c",dbl),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),batarg("b",dbl),arg("c",dbl),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),batarg("b",dbl),batarg("c",dbl),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, arg("",dbl),arg("b",bte),arg("c",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),arg("b",bte),batarg("c",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),batarg("b",bte),arg("c",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),batarg("b",bte),batarg("c",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, arg("",dbl),arg("b",sht),arg("c",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),arg("b",sht),batarg("c",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),batarg("b",sht),arg("c",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),batarg("b",sht),batarg("c",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, arg("",dbl),arg("b",int),arg("c",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),arg("b",int),batarg("c",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),batarg("b",int),arg("c",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),batarg("b",int),batarg("c",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, arg("",dbl),arg("b",lng),arg("c",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),arg("b",lng),batarg("c",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),batarg("b",lng),arg("c",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),batarg("b",lng),batarg("c",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, arg("",dbl),arg("b",flt),arg("c",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),arg("b",flt),batarg("c",flt),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),batarg("b",flt),arg("c",flt),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),batarg("b",flt),batarg("c",flt),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, arg("",dbl),arg("b",dbl),arg("c",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),arg("b",dbl),batarg("c",dbl),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),batarg("b",dbl),arg("c",dbl),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),batarg("b",dbl),batarg("c",dbl),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, arg("",dbl),arg("b",bte),arg("c",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),arg("b",bte),batarg("c",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),batarg("b",bte),arg("c",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),batarg("b",bte),batarg("c",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, arg("",dbl),arg("b",sht),arg("c",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),arg("b",sht),batarg("c",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),batarg("b",sht),arg("c",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),batarg("b",sht),batarg("c",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, arg("",dbl),arg("b",int),arg("c",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),arg("b",int),batarg("c",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),batarg("b",int),arg("c",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),batarg("b",int),batarg("c",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, arg("",dbl),arg("b",lng),arg("c",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),arg("b",lng),batarg("c",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),batarg("b",lng),arg("c",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),batarg("b",lng),batarg("c",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, arg("",dbl),arg("b",flt),arg("c",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),arg("b",flt),batarg("c",flt),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),batarg("b",flt),arg("c",flt),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),batarg("b",flt),batarg("c",flt),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, arg("",dbl),arg("b",dbl),arg("c",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),arg("b",dbl),batarg("c",dbl),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),batarg("b",dbl),arg("c",dbl),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),batarg("b",dbl),batarg("c",dbl),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "str_group_concat", SQLstrgroup_concat, false, "return the string concatenation of groups", args(1,7, arg("",str),arg("b",str),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "str_group_concat", SQLstrgroup_concat, false, "return the string concatenation of groups", args(1,7, batarg("",str),batarg("b",str),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "str_group_concat", SQLstrgroup_concat, false, "return the string concatenation of groups with a custom separator", args(1,8, arg("",str),arg("b",str),arg("sep",str),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "str_group_concat", SQLstrgroup_concat, false, "return the string concatenation of groups with a custom separator", args(1,8, batarg("",str),arg("b",str),batarg("sep",str),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "str_group_concat", SQLstrgroup_concat, false, "return the string concatenation of groups with a custom separator", args(1,8, batarg("",str),batarg("b",str),arg("sep",str),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "str_group_concat", SQLstrgroup_concat, false, "return the string concatenation of groups with a custom separator", args(1,8, batarg("",str),batarg("b",str),batarg("sep",str),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 /* sql_subquery */
 command("aggr", "zero_or_one", zero_or_one, false, "if col contains exactly one value return this. Incase of more raise an exception else return nil", args(1,2, argany("",1),batargany("col",1))),
 command("aggr", "zero_or_one", zero_or_one_error, false, "if col contains exactly one value return this. Incase of more raise an exception if err is true else return nil", args(1,3, argany("",1),batargany("col",1),arg("err",bit))),
 command("aggr", "zero_or_one", zero_or_one_error_bat, false, "if col contains exactly one value return this. Incase of more raise an exception if err is true else return nil", args(1,3, argany("",1),batargany("col",1),batarg("err",bit))),
 command("aggr", "subzero_or_one", SQLsubzero_or_one, false, "", args(1,5, batargany("",1),batargany("b",1),batarg("g",oid),batarg("e",oid),arg("no_nil",bit))),
 command("aggr", "all", SQLall, false, "if all values in b are equal return this, else nil", args(1,2, argany("",1),batargany("b",1))),
 pattern("aggr", "suball", SQLall_grp, false, "if all values in l are equal (per group) return the value, else nil", args(1,5, batargany("",1),batargany("l",1),batarg("g",oid),batarg("e",oid),arg("no_nil",bit))),
 pattern("aggr", "suball", SQLall_grp, false, "if all values in l are equal (per group) return the value, else nil", args(1,6, batargany("",1),batargany("l",1),batarg("g",oid),batarg("e",oid),batarg("s",oid),arg("no_nil",bit))),
 command("aggr", "null", SQLnil, false, "if b has a nil return true, else false", args(1,2, arg("",bit),batargany("b",1))),
 pattern("aggr", "subnull", SQLnil_grp, false, "if any value in l is nil with in a group return true for that group, else false", args(1,5, batarg("",bit),batargany("l",1),batarg("g",oid),batarg("e",oid),arg("no_nil",bit))),
 pattern("aggr", "subnull", SQLnil_grp, false, "if any value in l is nil with in a group return true for that group, else false; with candidate list", args(1,6, batarg("",bit),batargany("l",1),batarg("g",oid),batarg("e",oid),batarg("s",oid),arg("no_nil",bit))),
 pattern("sql", "any", SQLany_cmp, false, "if cmp then true, (nl or nr) nil then nil, else false", args(1,4, arg("",bit),arg("cmp",bit),arg("nl",bit),arg("nr",bit))),
 pattern("batsql", "any", SQLany_cmp, false, "if cmp then true, (nl or nr) nil then nil, else false", args(1,4, batarg("",bit),batarg("cmp",bit),arg("nl",bit),arg("nr",bit))),
 pattern("batsql", "any", SQLany_cmp, false, "if cmp then true, (nl or nr) nil then nil, else false", args(1,4, batarg("",bit),arg("cmp",bit),batarg("nl",bit),arg("nr",bit))),
 pattern("batsql", "any", SQLany_cmp, false, "if cmp then true, (nl or nr) nil then nil, else false", args(1,4, batarg("",bit),arg("cmp",bit),arg("nl",bit),batarg("nr",bit))),
 pattern("batsql", "any", SQLany_cmp, false, "if cmp then true, (nl or nr) nil then nil, else false", args(1,4, batarg("",bit),arg("cmp",bit),batarg("nl",bit),batarg("nr",bit))),
 pattern("batsql", "any", SQLany_cmp, false, "if cmp then true, (nl or nr) nil then nil, else false", args(1,4, batarg("",bit),batarg("cmp",bit),arg("nl",bit),batarg("nr",bit))),
 pattern("batsql", "any", SQLany_cmp, false, "if cmp then true, (nl or nr) nil then nil, else false", args(1,4, batarg("",bit),batarg("cmp",bit),batarg("nl",bit),arg("nr",bit))),
 pattern("batsql", "any", SQLany_cmp, false, "if cmp then true, (nl or nr) nil then nil, else false", args(1,4, batarg("",bit),batarg("cmp",bit),batarg("nl",bit),batarg("nr",bit))),
 pattern("sql", "all", SQLall_cmp, false, "if !cmp then false, (nl or nr) then nil, else true", args(1,4, arg("",bit),arg("cmp",bit),arg("nl",bit),arg("nr",bit))),
 pattern("batsql", "all", SQLall_cmp, false, "if !cmp then false, (nl or nr) then nil, else true", args(1,4, batarg("",bit),batarg("cmp",bit),arg("nl",bit),arg("nr",bit))),
 pattern("batsql", "all", SQLall_cmp, false, "if !cmp then false, (nl or nr) then nil, else true", args(1,4, batarg("",bit),arg("cmp",bit),batarg("nl",bit),arg("nr",bit))),
 pattern("batsql", "all", SQLall_cmp, false, "if !cmp then false, (nl or nr) then nil, else true", args(1,4, batarg("",bit),arg("cmp",bit),arg("nl",bit),batarg("nr",bit))),
 pattern("batsql", "all", SQLall_cmp, false, "if !cmp then false, (nl or nr) then nil, else true", args(1,4, batarg("",bit),arg("cmp",bit),batarg("nl",bit),batarg("nr",bit))),
 pattern("batsql", "all", SQLall_cmp, false, "if !cmp then false, (nl or nr) then nil, else true", args(1,4, batarg("",bit),batarg("cmp",bit),arg("nl",bit),batarg("nr",bit))),
 pattern("batsql", "all", SQLall_cmp, false, "if !cmp then false, (nl or nr) then nil, else true", args(1,4, batarg("",bit),batarg("cmp",bit),batarg("nl",bit),arg("nr",bit))),
 pattern("batsql", "all", SQLall_cmp, false, "if !cmp then false, (nl or nr) then nil, else true", args(1,4, batarg("",bit),batarg("cmp",bit),batarg("nl",bit),batarg("nr",bit))),
 pattern("aggr", "anyequal", SQLanyequal, false, "if any value in r is equal to l return true, else if r has nil nil else false", args(1,3, arg("",bit),batargany("l",1),batargany("r",1))),
 pattern("bataggr", "anyequal", SQLanyequal, false, "", args(1,3, batarg("",bit),batargany("l",1),batargany("r",1))),
 pattern("aggr", "allnotequal", SQLallnotequal, false, "if all values in r are not equal to l return true, else if r has nil nil else false", args(1,3, arg("",bit),batargany("l",1),batargany("r",1))),
 pattern("bataggr", "allnotequal", SQLallnotequal, false, "", args(1,3, arg("",bit),batargany("l",1),batargany("r",1))),
 pattern("aggr", "subanyequal", SQLanyequal_grp, false, "if any value in r is equal to l return true, else if r has nil nil else false", args(1,6, batarg("",bit),batargany("l",1),batargany("r",1),batarg("g",oid),batarg("e",oid),arg("no_nil",bit))),
 pattern("aggr", "subanyequal", SQLanyequal_grp, false, "if any value in r is equal to l return true, else if r has nil nil else false; with candidate list", args(1,7, batarg("",bit),batargany("l",1),batargany("r",1),batarg("g",oid),batarg("e",oid),batarg("s",oid),arg("no_nil",bit))),
 pattern("aggr", "subanyequal", SQLanyequal_grp2, false, "if any value in r is equal to l return true, else if r has nil nil else false, except if rid is nil (ie empty) then false", args(1,7, batarg("",bit),batargany("l",1),batargany("r",1),batarg("rid",oid),batarg("g",oid),batarg("e",oid),arg("no_nil",bit))),
 pattern("aggr", "subanyequal", SQLanyequal_grp2, false, "if any value in r is equal to l return true, else if r has nil nil else false, except if rid is nil (ie empty) then false; with candidate list", args(1,8, batarg("",bit),batargany("l",1),batargany("r",1),batarg("rid",oid),batarg("g",oid),batarg("e",oid),batarg("s",oid),arg("no_nil",bit))),
 pattern("aggr", "suballnotequal", SQLallnotequal_grp, false, "if all values in r are not equal to l return true, else if r has nil nil else false", args(1,6, batarg("",bit),batargany("l",1),batargany("r",1),batarg("g",oid),batarg("e",oid),arg("no_nil",bit))),
 pattern("aggr", "suballnotequal", SQLallnotequal_grp, false, "if all values in r are not equal to l return true, else if r has nil nil else false; with candidate list", args(1,7, batarg("",bit),batargany("l",1),batargany("r",1),batarg("g",oid),batarg("e",oid),batarg("s",oid),arg("no_nil",bit))),
 pattern("aggr", "suballnotequal", SQLallnotequal_grp2, false, "if all values in r are not equal to l return true, else if r has nil nil else false, except if rid is nil (ie empty) then true", args(1,7, batarg("",bit),batargany("l",1),batargany("r",1),batarg("rid",oid),batarg("g",oid),batarg("e",oid),arg("no_nil",bit))),
 pattern("aggr", "suballnotequal", SQLallnotequal_grp2, false, "if all values in r are not equal to l return true, else if r has nil nil else false, except if rid is nil (ie empty) then true; with candidate list", args(1,8, batarg("",bit),batargany("l",1),batargany("r",1),batarg("rid",oid),batarg("g",oid),batarg("e",oid),batarg("s",oid),arg("no_nil",bit))),
 pattern("aggr", "exist", SQLexist, false, "", args(1,2, arg("",bit), argany("b",1))),
 pattern("bataggr", "exist", SQLexist, false, "", args(1,2, batarg("",bit), argany("b",1))),
 pattern("bataggr", "exist", SQLexist, false, "", args(1,2, arg("",bit), batargany("b",1))),
 pattern("bataggr", "exist", SQLexist, false, "", args(1,2, batarg("",bit), batargany("b",1))),
 pattern("aggr", "subexist", SQLsubexist, false, "", args(1,5, batarg("",bit),batargany("b",0),batarg("g",oid),batarg("e",oid),arg("no_nil",bit))),
 pattern("aggr", "subexist", SQLsubexist, false, "", args(1,6, batarg("",bit),batargany("b",0),batarg("g",oid),batarg("e",oid),batarg("s",oid),arg("no_nil",bit))),
 pattern("aggr", "not_exist", SQLnot_exist, false, "", args(1,2, arg("",bit), argany("b",1))),
 pattern("bataggr", "not_exist", SQLnot_exist, false, "", args(1,2, batarg("",bit), argany("b",1))),
 pattern("bataggr", "not_exist", SQLnot_exist, false, "", args(1,2, arg("",bit), batargany("b",1))),
 pattern("bataggr", "not_exist", SQLnot_exist, false, "", args(1,2, batarg("",bit), batargany("b",1))),
 pattern("aggr", "subnot_exist", SQLsubnot_exist, false, "", args(1,5, batarg("",bit),batargany("b",0),batarg("g",oid),batarg("e",oid),arg("no_nil",bit))),
 pattern("aggr", "subnot_exist", SQLsubnot_exist, false, "", args(1,6, batarg("",bit),batargany("b",0),batarg("g",oid),batarg("e",oid),batarg("s",oid),arg("no_nil",bit))),
 /* wlr */
 pattern("wlr", "master", WLRmaster, false, "Initialize the replicator thread", args(0,1, arg("dbname",str))),
 pattern("wlr", "stop", WLRstop, false, "Stop the replicator thread", noargs),
 pattern("wlr", "accept", WLRaccept, false, "Accept failing transaction", noargs),
 pattern("wlr", "replicate", WLRreplicate, false, "Continue to keep the replica in sink", noargs),
 pattern("wlr", "replicate", WLRreplicate, false, "Roll the snapshot forward to an up-to-date clone", args(0,1, arg("ts",timestamp))),
 pattern("wlr", "replicate", WLRreplicate, false, "Roll the snapshot forward to a specific transaction id", args(0,1, arg("id",bte))),
 pattern("wlr", "replicate", WLRreplicate, false, "Roll the snapshot forward to a specific transaction id", args(0,1, arg("id",sht))),
 pattern("wlr", "replicate", WLRreplicate, false, "Roll the snapshot forward to a specific transaction id", args(0,1, arg("id",int))),
 pattern("wlr", "replicate", WLRreplicate, false, "Roll the snapshot forward to a specific transaction id", args(0,1, arg("id",lng))),
 pattern("wlr", "getMaster", WLRgetmaster, false, "What is the current master database", args(1,1, arg("",str))),
 pattern("wlr", "setbeat", WLRsetbeat, false, "Threshold (in seconds) for re-running queries", args(0,1, arg("dur",int))),
 pattern("wlr", "getclock", WLRgetclock, false, "Timestamp of last replicated transaction.", args(1,1, arg("",str))),
 pattern("wlr", "gettick", WLRgettick, false, "Transaction identifier of the last replicated transaction.", args(1,1, arg("",lng))),
 pattern("wlr", "transaction", WLRtransaction, false, "Mark the beginning of the work unit which can be a compound transaction", args(0,3, arg("tid",lng),arg("started",str),arg("user",str))),
 pattern("wlr", "commit", WLRcommit, false, "Mark the end of the work unit", noargs),
 pattern("wlr", "rollback", WLRrollback, false, "Mark the end of the work unit", noargs),
 pattern("wlr", "catalog", WLRcatalog, false, "A catalog changing query", args(0,1, arg("q",str))),
 pattern("wlr", "action", WLRaction, false, "A query producing updates", args(0,1, arg("q",str))),
 pattern("wlr", "append", WLRappend, false, "Apply the insertions in the workload-capture-replay list", args(1,5, arg("",int),arg("sname",str),arg("tname",str),arg("cname",str),varargany("ins",0))),
 pattern("wlr", "update", WLRupdate, false, "Apply the update in the workload-capture-replay list", args(1,6, arg("",int),arg("sname",str),arg("tname",str),arg("cname",str),arg("tid",oid),argany("val",0))),
 pattern("wlr", "delete", WLRdelete, false, "Apply the deletions in the workload-capture-replay list", args(1,4, arg("",int),arg("sname",str),arg("tname",str),vararg("b",oid))),
 pattern("wlr", "clear_table", WLRclear_table, false, "Destroy the tuples in the table", args(1,3, arg("",int),arg("sname",str),arg("tname",str))),
 pattern("wlr", "create_seq", WLRgeneric, false, "Catalog operation create_seq", args(0,3, arg("sname",str),arg("seqname",str),arg("action",int))),
 pattern("wlr", "alter_seq", WLRgeneric, false, "Catalog operation alter_seq", args(0,3, arg("sname",str),arg("seqname",str),arg("val",lng))),
 pattern("wlr", "alter_seq", WLRgeneric, false, "Catalog operation alter_seq", args(0,4, arg("sname",str),arg("seqname",str),arg("seq",ptr),batarg("val",lng))),
 pattern("wlr", "drop_seq", WLRgeneric, false, "Catalog operation drop_seq", args(0,3, arg("sname",str),arg("nme",str),arg("action",int))),
 pattern("wlr", "create_schema", WLRgeneric, false, "Catalog operation create_schema", args(0,3, arg("sname",str),arg("auth",str),arg("action",int))),
 pattern("wlr", "drop_schema", WLRgeneric, false, "Catalog operation drop_schema", args(0,3, arg("sname",str),arg("ifexists",int),arg("action",int))),
 pattern("wlr", "create_table", WLRgeneric, false, "Catalog operation create_table", args(0,3, arg("sname",str),arg("tname",str),arg("temp",int))),
 pattern("wlr", "create_view", WLRgeneric, false, "Catalog operation create_view", args(0,3, arg("sname",str),arg("tname",str),arg("temp",int))),
 pattern("wlr", "drop_table", WLRgeneric, false, "Catalog operation drop_table", args(0,4, arg("sname",str),arg("name",str),arg("action",int),arg("ifexists",int))),
 pattern("wlr", "drop_view", WLRgeneric, false, "Catalog operation drop_view", args(0,4, arg("sname",str),arg("name",str),arg("action",int),arg("ifexists",int))),
 pattern("wlr", "drop_constraint", WLRgeneric, false, "Catalog operation drop_constraint", args(0,5, arg("sname",str),arg("tname",str),arg("name",str),arg("action",int),arg("ifexists",int))),
 pattern("wlr", "alter_table", WLRgeneric, false, "Catalog operation alter_table", args(0,3, arg("sname",str),arg("tname",str),arg("action",int))),
 pattern("wlr", "create_type", WLRgeneric, false, "Catalog operation create_type", args(0,3, arg("sname",str),arg("nme",str),arg("impl",str))),
 pattern("wlr", "drop_type", WLRgeneric, false, "Catalog operation drop_type", args(0,3, arg("sname",str),arg("nme",str),arg("action",int))),
 pattern("wlr", "grant_roles", WLRgeneric, false, "Catalog operation grant_roles", args(0,4, arg("sname",str),arg("auth",str),arg("grantor",int),arg("admin",int))),
 pattern("wlr", "revoke_roles", WLRgeneric, false, "Catalog operation revoke_roles", args(0,4, arg("sname",str),arg("auth",str),arg("grantor",int),arg("admin",int))),
 pattern("wlr", "grant", WLRgeneric, false, "Catalog operation grant", args(0,7, arg("sname",str),arg("tbl",str),arg("grantee",str),arg("privs",int),arg("cname",str),arg("gr",int),arg("grantor",int))),
 pattern("wlr", "revoke", WLRgeneric, false, "Catalog operation revoke", args(0,7, arg("sname",str),arg("tbl",str),arg("grantee",str),arg("privs",int),arg("cname",str),arg("grant",int),arg("grantor",int))),
 pattern("wlr", "grant_function", WLRgeneric, false, "Catalog operation grant_function", args(0,6, arg("sname",str),arg("fcnid",int),arg("grantee",str),arg("privs",int),arg("grant",int),arg("grantor",int))),
 pattern("wlr", "revoke_function", WLRgeneric, false, "Catalog operation revoke_function", args(0,6, arg("sname",str),arg("fcnid",int),arg("grantee",str),arg("privs",int),arg("grant",int),arg("grantor",int))),
 pattern("wlr", "create_user", WLRgeneric, false, "Catalog operation create_user", args(0,5, arg("sname",str),arg("passwrd",str),arg("enc",int),arg("schema",str),arg("fullname",str))),
 pattern("wlr", "drop_user", WLRgeneric, false, "Catalog operation drop_user", args(0,2, arg("sname",str),arg("action",int))),
 pattern("wlr", "drop_user", WLRgeneric, false, "Catalog operation drop_user", args(0,3, arg("sname",str),arg("auth",str),arg("action",int))),
 pattern("wlr", "alter_user", WLRgeneric, false, "Catalog operation alter_user", args(0,5, arg("sname",str),arg("passwrd",str),arg("enc",int),arg("schema",str),arg("oldpasswrd",str))),
 pattern("wlr", "rename_user", WLRgeneric, false, "Catalog operation rename_user", args(0,3, arg("sname",str),arg("newnme",str),arg("action",int))),
 pattern("wlr", "create_role", WLRgeneric, false, "Catalog operation create_role", args(0,3, arg("sname",str),arg("role",str),arg("grator",int))),
 pattern("wlr", "drop_role", WLRgeneric, false, "Catalog operation drop_role", args(0,3, arg("auth",str),arg("role",str),arg("action",int))),
 pattern("wlr", "drop_role", WLRgeneric, false, "Catalog operation drop_role", args(0,2, arg("role",str),arg("action",int))),
 pattern("wlr", "drop_index", WLRgeneric, false, "Catalog operation drop_index", args(0,3, arg("sname",str),arg("iname",str),arg("action",int))),
 pattern("wlr", "drop_function", WLRgeneric, false, "Catalog operation drop_function", args(0,5, arg("sname",str),arg("fname",str),arg("fid",int),arg("type",int),arg("action",int))),
 pattern("wlr", "create_function", WLRgeneric, false, "Catalog operation create_function", args(0,2, arg("sname",str),arg("fname",str))),
 pattern("wlr", "create_trigger", WLRgeneric, false, "Catalog operation create_trigger", args(0,10, arg("sname",str),arg("tname",str),arg("triggername",str),arg("time",int),arg("orientation",int),arg("event",int),arg("old",str),arg("new",str),arg("cond",str),arg("qry",str))),
 pattern("wlr", "drop_trigger", WLRgeneric, false, "Catalog operation drop_trigger", args(0,3, arg("sname",str),arg("nme",str),arg("ifexists",int))),
 pattern("wlr", "alter_add_table", WLRgeneric, false, "Catalog operation alter_add_table", args(0,5, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("action",int))),
 pattern("wlr", "alter_del_table", WLRgeneric, false, "Catalog operation alter_del_table", args(0,5, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("action",int))),
 pattern("wlr", "alter_set_table", WLRgeneric, false, "Catalog operation alter_set_table", args(0,3, arg("sname",str),arg("tnme",str),arg("access",int))),
 pattern("wlr", "alter_add_range_partition", WLRgeneric, false, "Catalog operation alter_add_range_partition", args(0,8, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("min",str),arg("max",str),arg("nills",bit),arg("update",int))),
 pattern("wlr", "comment_on", WLRgeneric, false, "Catalog operation comment_on", args(0,2, arg("objid",int),arg("remark",str))),
 pattern("wlr", "rename_schema", WLRgeneric, false, "Catalog operation rename_schema", args(0,2, arg("sname",str),arg("newnme",str))),
 pattern("wlr", "rename_table", WLRgeneric, false, "Catalog operation rename_table", args(0,4, arg("osname",str),arg("nsname",str),arg("otname",str),arg("ntname",str))),
 pattern("wlr", "rename_column", WLRgeneric, false, "Catalog operation rename_column", args(0,4, arg("sname",str),arg("tname",str),arg("cname",str),arg("newnme",str))),
 pattern("wlr", "transaction_release", WLRgeneric, false, "A transaction statement (type can be commit,release,rollback or start)", args(1,3, arg("",void),arg("chain",int),arg("name",str))),
 pattern("wlr", "transaction_commit", WLRgeneric, false, "A transaction statement (type can be commit,release,rollback or start)", args(1,3, arg("",void),arg("chain",int),arg("name",str))),
 pattern("wlr", "transaction_rollback", WLRgeneric, false, "A transaction statement (type can be commit,release,rollback or start)", args(1,3, arg("",void),arg("chain",int),arg("name",str))),
 pattern("wlr", "transaction_begin", WLRgeneric, false, "A transaction statement (type can be commit,release,rollback or start)", args(1,3, arg("",void),arg("chain",int),arg("name",str))),
 pattern("wlr", "transaction", WLRgeneric, true, "Start an autocommit transaction", noargs),
 pattern("wlr", "alter_add_value_partition", WLRgeneric, false, "Catalog operation alter_add_value_partition", args(0,6, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("nills",bit),arg("update",int))),
 pattern("wlr", "alter_add_value_partition", WLRgeneric, false, "Catalog operation alter_add_value_partition", args(0,7, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("nills",bit),arg("update",int),vararg("arg",str))),
 /* sqlcatalog */
 pattern("sqlcatalog", "create_seq", SQLcreate_seq, false, "Catalog operation create_seq", args(0,4, arg("sname",str),arg("seqname",str),arg("seq",ptr),arg("action",int))),
 pattern("sqlcatalog", "alter_seq", SQLalter_seq, false, "Catalog operation alter_seq", args(0,4, arg("sname",str),arg("seqname",str),arg("seq",ptr),arg("val",lng))),
 pattern("sqlcatalog", "alter_seq", SQLalter_seq, false, "Catalog operation alter_seq", args(0,4, arg("sname",str),arg("seqname",str),arg("seq",ptr),batarg("val",lng))),
 pattern("sqlcatalog", "drop_seq", SQLdrop_seq, false, "Catalog operation drop_seq", args(0,3, arg("sname",str),arg("nme",str),arg("action",int))),
 pattern("sqlcatalog", "create_schema", SQLcreate_schema, false, "Catalog operation create_schema", args(0,3, arg("sname",str),arg("auth",str),arg("action",int))),
 pattern("sqlcatalog", "drop_schema", SQLdrop_schema, false, "Catalog operation drop_schema", args(0,3, arg("sname",str),arg("ifexists",int),arg("action",int))),
 pattern("sqlcatalog", "create_table", SQLcreate_table, false, "Catalog operation create_table", args(0,4, arg("sname",str),arg("tname",str),arg("tbl",ptr),arg("temp",int))),
 pattern("sqlcatalog", "create_view", SQLcreate_view, false, "Catalog operation create_view", args(0,4, arg("sname",str),arg("vname",str),arg("tbl",ptr),arg("temp",int))),
 pattern("sqlcatalog", "drop_table", SQLdrop_table, false, "Catalog operation drop_table", args(0,4, arg("sname",str),arg("name",str),arg("action",int),arg("ifexists",int))),
 pattern("sqlcatalog", "drop_view", SQLdrop_view, false, "Catalog operation drop_view", args(0,4, arg("sname",str),arg("name",str),arg("action",int),arg("ifexists",int))),
 pattern("sqlcatalog", "drop_constraint", SQLdrop_constraint, false, "Catalog operation drop_constraint", args(0,5, arg("sname",str),arg("tname",str),arg("name",str),arg("action",int),arg("ifexists",int))),
 pattern("sqlcatalog", "alter_table", SQLalter_table, false, "Catalog operation alter_table", args(0,4, arg("sname",str),arg("tname",str),arg("tbl",ptr),arg("action",int))),
 pattern("sqlcatalog", "create_type", SQLcreate_type, false, "Catalog operation create_type", args(0,3, arg("sname",str),arg("nme",str),arg("impl",str))),
 pattern("sqlcatalog", "drop_type", SQLdrop_type, false, "Catalog operation drop_type", args(0,3, arg("sname",str),arg("nme",str),arg("action",int))),
 pattern("sqlcatalog", "grant_roles", SQLgrant_roles, false, "Catalog operation grant_roles", args(0,4, arg("sname",str),arg("auth",str),arg("grantor",int),arg("admin",int))),
 pattern("sqlcatalog", "revoke_roles", SQLrevoke_roles, false, "Catalog operation revoke_roles", args(0,4, arg("sname",str),arg("auth",str),arg("grantor",int),arg("admin",int))),
 pattern("sqlcatalog", "grant", SQLgrant, false, "Catalog operation grant", args(0,7, arg("sname",str),arg("tbl",str),arg("grantee",str),arg("privs",int),arg("cname",str),arg("gr",int),arg("grantor",int))),
 pattern("sqlcatalog", "revoke", SQLrevoke, false, "Catalog operation revoke", args(0,7, arg("sname",str),arg("tbl",str),arg("grantee",str),arg("privs",int),arg("cname",str),arg("grant",int),arg("grantor",int))),
 pattern("sqlcatalog", "grant_function", SQLgrant_function, false, "Catalog operation grant_function", args(0,6, arg("sname",str),arg("fcnid",int),arg("grantee",str),arg("privs",int),arg("grant",int),arg("grantor",int))),
 pattern("sqlcatalog", "revoke_function", SQLrevoke_function, false, "Catalog operation revoke_function", args(0,6, arg("sname",str),arg("fcnid",int),arg("grantee",str),arg("privs",int),arg("grant",int),arg("grantor",int))),
 pattern("sqlcatalog", "create_user", SQLcreate_user, false, "Catalog operation create_user", args(0,6, arg("sname",str),arg("passwrd",str),arg("enc",int),arg("schema",str),arg("schemapath",str),arg("fullname",str))),
 pattern("sqlcatalog", "drop_user", SQLdrop_user, false, "Catalog operation drop_user", args(0,2, arg("sname",str),arg("action",int))),
 pattern("sqlcatalog", "drop_user", SQLdrop_user, false, "Catalog operation drop_user", args(0,3, arg("sname",str),arg("auth",str),arg("action",int))),
 pattern("sqlcatalog", "alter_user", SQLalter_user, false, "Catalog operation alter_user", args(0,6, arg("sname",str),arg("passwrd",str),arg("enc",int),arg("schema",str),arg("schemapath",str),arg("oldpasswrd",str))),
 pattern("sqlcatalog", "rename_user", SQLrename_user, false, "Catalog operation rename_user", args(0,3, arg("sname",str),arg("newnme",str),arg("action",int))),
 pattern("sqlcatalog", "create_role", SQLcreate_role, false, "Catalog operation create_role", args(0,3, arg("sname",str),arg("role",str),arg("grator",int))),
 pattern("sqlcatalog", "drop_role", SQLdrop_role, false, "Catalog operation drop_role", args(0,3, arg("auth",str),arg("role",str),arg("action",int))),
 pattern("sqlcatalog", "drop_role", SQLdrop_role, false, "Catalog operation drop_role", args(0,2, arg("role",str),arg("action",int))),
 pattern("sqlcatalog", "drop_index", SQLdrop_index, false, "Catalog operation drop_index", args(0,3, arg("sname",str),arg("iname",str),arg("action",int))),
 pattern("sqlcatalog", "drop_function", SQLdrop_function, false, "Catalog operation drop_function", args(0,5, arg("sname",str),arg("fname",str),arg("fid",int),arg("type",int),arg("action",int))),
 pattern("sqlcatalog", "create_function", SQLcreate_function, false, "Catalog operation create_function", args(0,3, arg("sname",str),arg("fname",str),arg("fcn",ptr))),
 pattern("sqlcatalog", "create_trigger", SQLcreate_trigger, false, "Catalog operation create_trigger", args(0,10, arg("sname",str),arg("tname",str),arg("triggername",str),arg("time",int),arg("orientation",int),arg("event",int),arg("old",str),arg("new",str),arg("cond",str),arg("qry",str))),
 pattern("sqlcatalog", "drop_trigger", SQLdrop_trigger, false, "Catalog operation drop_trigger", args(0,3, arg("sname",str),arg("nme",str),arg("ifexists",int))),
 pattern("sqlcatalog", "alter_add_table", SQLalter_add_table, false, "Catalog operation alter_add_table", args(0,5, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("action",int))),
 pattern("sqlcatalog", "alter_del_table", SQLalter_del_table, false, "Catalog operation alter_del_table", args(0,5, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("action",int))),
 pattern("sqlcatalog", "alter_set_table", SQLalter_set_table, false, "Catalog operation alter_set_table", args(0,3, arg("sname",str),arg("tnme",str),arg("access",int))),
 pattern("sqlcatalog", "alter_add_range_partition", SQLalter_add_range_partition, false, "Catalog operation alter_add_range_partition", args(0,8, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),argany("min",1),argany("max",1),arg("nills",bit),arg("update",int))),
 pattern("sqlcatalog", "alter_add_value_partition", SQLalter_add_value_partition, false, "Catalog operation alter_add_value_partition", args(0,6, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("nills",bit),arg("update",int))),
 pattern("sqlcatalog", "alter_add_value_partition", SQLalter_add_value_partition, false, "Catalog operation alter_add_value_partition", args(0,7, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("nills",bit),arg("update",int),varargany("arg",0))),
 pattern("sqlcatalog", "comment_on", SQLcomment_on, false, "Catalog operation comment_on", args(0,2, arg("objid",int),arg("remark",str))),
 pattern("sqlcatalog", "rename_schema", SQLrename_schema, false, "Catalog operation rename_schema", args(0,2, arg("sname",str),arg("newnme",str))),
 pattern("sqlcatalog", "rename_table", SQLrename_table, false, "Catalog operation rename_table", args(0,4, arg("osname",str),arg("nsname",str),arg("otname",str),arg("ntname",str))),
 pattern("sqlcatalog", "rename_column", SQLrename_column, false, "Catalog operation rename_column", args(0,4, arg("sname",str),arg("tname",str),arg("cname",str),arg("newnme",str))),
 /* sql_transaction */
 pattern("sql", "transaction_release", SQLtransaction_release, true, "A transaction statement (type can be commit,release,rollback or start)", args(1,3, arg("",void),arg("chain",int),arg("name",str))),
 pattern("sql", "transaction_commit", SQLtransaction_commit, true, "A transaction statement (type can be commit,release,rollback or start)", args(1,3, arg("",void),arg("chain",int),arg("name",str))),
 pattern("sql", "transaction_rollback", SQLtransaction_rollback, true, "A transaction statement (type can be commit,release,rollback or start)", args(1,3, arg("",void),arg("chain",int),arg("name",str))),
 pattern("sql", "transaction_begin", SQLtransaction_begin, true, "A transaction statement (type can be commit,release,rollback or start)", args(1,3, arg("",void),arg("chain",int),arg("name",str))),
 pattern("sql", "transaction", SQLtransaction2, true, "Start an autocommit transaction", noargs),
#ifdef HAVE_HGE
 /* sql_hge */
 command("calc", "dec_round", hge_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,3, arg("",hge),arg("v",hge),arg("r",hge))),
 pattern("batcalc", "dec_round", hge_bat_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",hge),batarg("v",hge),arg("r",hge))),
 pattern("batcalc", "dec_round", hge_bat_dec_round_wrap, false, "round off the value v to nearests multiple of r", args(1,4, batarg("",hge),batarg("v",hge),arg("r",hge),batarg("s",oid))),
 pattern("batcalc", "dec_round", hge_bat_dec_round_wrap_cst, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",hge),arg("v",hge),batarg("r",hge))),
 pattern("batcalc", "dec_round", hge_bat_dec_round_wrap_cst, false, "round off the value v to nearests multiple of r", args(1,4, batarg("",hge),arg("v",hge),batarg("r",hge),batarg("s",oid))),
 pattern("batcalc", "dec_round", hge_bat_dec_round_wrap_nocst, false, "round off the value v to nearests multiple of r", args(1,3, batarg("",hge),batarg("v",hge),batarg("r",hge))),
 pattern("batcalc", "dec_round", hge_bat_dec_round_wrap_nocst, false, "round off the value v to nearests multiple of r", args(1,5, batarg("",hge),batarg("v",hge),batarg("r",hge),batarg("s1",oid),batarg("s2",oid))),
 command("calc", "round", hge_round_wrap, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, arg("",hge),arg("v",hge),arg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", hge_bat_round_wrap, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, batarg("",hge),batarg("v",hge),arg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", hge_bat_round_wrap, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,6, batarg("",hge),batarg("v",hge),arg("r",bte),batarg("s",oid),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", hge_bat_round_wrap_cst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, batarg("",hge),arg("v",hge),batarg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", hge_bat_round_wrap_cst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,6, batarg("",hge),arg("v",hge),batarg("r",bte),batarg("s",oid),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", hge_bat_round_wrap_nocst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,5, batarg("",hge),batarg("v",hge),batarg("r",bte),arg("d",int),arg("s",int))),
 pattern("batcalc", "round", hge_bat_round_wrap_nocst, false, "round off the decimal v(d,s) to r digits behind the dot (if r < 0, before the dot)", args(1,7, batarg("",hge),batarg("v",hge),batarg("r",bte),batarg("s1",oid),batarg("s2",oid),arg("d",int),arg("s",int))),
 command("calc", "second_interval", hge_dec2second_interval, false, "cast hge decimal to a second_interval", args(1,5, arg("",lng),arg("sc",int),arg("v",hge),arg("ek",int),arg("sk",int))),
 pattern("batcalc", "second_interval", hge_batdec2second_interval, false, "cast hge decimal to a second_interval", args(1,6, batarg("",lng),arg("sc",int),batarg("v",hge),batarg("s",oid),arg("ek",int),arg("sk",int))),
 command("calc", "hge", nil_2dec_hge, false, "cast to dec(hge) and check for overflow", args(1,4, arg("",hge),arg("v",void),arg("digits",int),arg("scale",int))),
 command("batcalc", "hge", batnil_2dec_hge, false, "cast to dec(hge) and check for overflow", args(1,4, batarg("",hge),batarg("v",void),arg("digits",int),arg("scale",int))),
 command("calc", "hge", str_2dec_hge, false, "cast to dec(hge) and check for overflow", args(1,4, arg("",hge),arg("v",str),arg("digits",int),arg("scale",int))),
 pattern("batcalc", "hge", batstr_2dec_hge, false, "cast to dec(hge) and check for overflow", args(1,5, batarg("",hge),batarg("v",str),batarg("s",oid),arg("digits",int),arg("scale",int))),
 pattern("calc", "month_interval", month_interval, false, "cast hge to a month_interval and check for overflow", args(1,4, arg("",int),arg("v",hge),arg("ek",int),arg("sk",int))),
 pattern("batcalc", "month_interval", month_interval, false, "cast hge to a month_interval and check for overflow", args(1,5, batarg("",int),batarg("v",hge),batarg("s",oid),arg("ek",int),arg("sk",int))),
 pattern("calc", "second_interval", second_interval, false, "cast hge to a second_interval and check for overflow", args(1,4, arg("",lng),arg("v",hge),arg("ek",int),arg("sk",int))),
 pattern("batcalc", "second_interval", second_interval, false, "cast hge to a second_interval and check for overflow", args(1,5, batarg("",lng),batarg("v",hge),batarg("s",oid),arg("ek",int),arg("sk",int))),
 /* sql_decimal_hge */
 command("calc", "hge", flt_num2dec_hge, false, "cast number to decimal(hge) and check for overflow", args(1,4, arg("",hge),arg("v",flt),arg("digits",int),arg("scale",int))),
 command("batcalc", "hge", batflt_num2dec_hge, false, "cast number to decimal(hge) and check for overflow", args(1,5, batarg("",hge),batarg("v",flt),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "hge", dbl_num2dec_hge, false, "cast number to decimal(hge) and check for overflow", args(1,4, arg("",hge),arg("v",dbl),arg("digits",int),arg("scale",int))),
 command("batcalc", "hge", batdbl_num2dec_hge, false, "cast number to decimal(hge) and check for overflow", args(1,5, batarg("",hge),batarg("v",dbl),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "hge", bte_num2dec_hge, false, "cast number to decimal(hge) and check for overflow", args(1,4, arg("",hge),arg("v",bte),arg("digits",int),arg("scale",int))),
 command("batcalc", "hge", batbte_num2dec_hge, false, "cast number to decimal(hge) and check for overflow", args(1,5, batarg("",hge),batarg("v",bte),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "hge", bte_dec2_hge, false, "cast decimal(bte) to hge and check for overflow", args(1,3, arg("",hge),arg("s1",int),arg("v",bte))),
 command("calc", "hge", bte_dec2dec_hge, false, "cast decimal(bte) to decimal(hge) and check for overflow", args(1,5, arg("",hge),arg("s1",int),arg("v",bte),arg("d2",int),arg("s2",int))),
 command("batcalc", "hge", batbte_dec2_hge, false, "cast decimal(bte) to hge and check for overflow", args(1,4, batarg("",hge),arg("s1",int),batarg("v",bte),batarg("s",oid))),
 command("batcalc", "hge", batbte_dec2dec_hge, false, "cast decimal(bte) to decimal(hge) and check for overflow", args(1,6, batarg("",hge),arg("s1",int),batarg("v",bte),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "hge", sht_num2dec_hge, false, "cast number to decimal(hge) and check for overflow", args(1,4, arg("",hge),arg("v",sht),arg("digits",int),arg("scale",int))),
 command("batcalc", "hge", batsht_num2dec_hge, false, "cast number to decimal(hge) and check for overflow", args(1,5, batarg("",hge),batarg("v",sht),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "hge", sht_dec2_hge, false, "cast decimal(sht) to hge and check for overflow", args(1,3, arg("",hge),arg("s1",int),arg("v",sht))),
 command("calc", "hge", sht_dec2dec_hge, false, "cast decimal(sht) to decimal(hge) and check for overflow", args(1,5, arg("",hge),arg("s1",int),arg("v",sht),arg("d2",int),arg("s2",int))),
 command("batcalc", "hge", batsht_dec2_hge, false, "cast decimal(sht) to hge and check for overflow", args(1,4, batarg("",hge),arg("s1",int),batarg("v",sht),batarg("s",oid))),
 command("batcalc", "hge", batsht_dec2dec_hge, false, "cast decimal(sht) to decimal(hge) and check for overflow", args(1,6, batarg("",hge),arg("s1",int),batarg("v",sht),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "hge", int_num2dec_hge, false, "cast number to decimal(hge) and check for overflow", args(1,4, arg("",hge),arg("v",int),arg("digits",int),arg("scale",int))),
 command("batcalc", "hge", batint_num2dec_hge, false, "cast number to decimal(hge) and check for overflow", args(1,5, batarg("",hge),batarg("v",int),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "hge", int_dec2_hge, false, "cast decimal(int) to hge and check for overflow", args(1,3, arg("",hge),arg("s1",int),arg("v",int))),
 command("calc", "hge", int_dec2dec_hge, false, "cast decimal(int) to decimal(hge) and check for overflow", args(1,5, arg("",hge),arg("s1",int),arg("v",int),arg("d2",int),arg("s2",int))),
 command("batcalc", "hge", batint_dec2_hge, false, "cast decimal(int) to hge and check for overflow", args(1,4, batarg("",hge),arg("s1",int),batarg("v",int),batarg("s",oid))),
 command("batcalc", "hge", batint_dec2dec_hge, false, "cast decimal(int) to decimal(hge) and check for overflow", args(1,6, batarg("",hge),arg("s1",int),batarg("v",int),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "hge", lng_num2dec_hge, false, "cast number to decimal(hge) and check for overflow", args(1,4, arg("",hge),arg("v",lng),arg("digits",int),arg("scale",int))),
 command("batcalc", "hge", batlng_num2dec_hge, false, "cast number to decimal(hge) and check for overflow", args(1,5, batarg("",hge),batarg("v",lng),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "hge", lng_dec2_hge, false, "cast decimal(lng) to hge and check for overflow", args(1,3, arg("",hge),arg("s1",int),arg("v",lng))),
 command("calc", "hge", lng_dec2dec_hge, false, "cast decimal(lng) to decimal(hge) and check for overflow", args(1,5, arg("",hge),arg("s1",int),arg("v",lng),arg("d2",int),arg("s2",int))),
 command("batcalc", "hge", batlng_dec2_hge, false, "cast decimal(lng) to hge and check for overflow", args(1,4, batarg("",hge),arg("s1",int),batarg("v",lng),batarg("s",oid))),
 command("batcalc", "hge", batlng_dec2dec_hge, false, "cast decimal(lng) to decimal(hge) and check for overflow", args(1,6, batarg("",hge),arg("s1",int),batarg("v",lng),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "hge", hge_num2dec_hge, false, "cast number to decimal(hge) and check for overflow", args(1,4, arg("",hge),arg("v",hge),arg("digits",int),arg("scale",int))),
 command("batcalc", "hge", bathge_num2dec_hge, false, "cast number to decimal(hge) and check for overflow", args(1,5, batarg("",hge),batarg("v",hge),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "hge", hge_dec2_hge, false, "cast decimal(hge) to hge and check for overflow", args(1,3, arg("",hge),arg("s1",int),arg("v",hge))),
 command("calc", "hge", hge_dec2dec_hge, false, "cast decimal(hge) to decimal(hge) and check for overflow", args(1,5, arg("",hge),arg("s1",int),arg("v",hge),arg("d2",int),arg("s2",int))),
 command("batcalc", "hge", bathge_dec2_hge, false, "cast decimal(hge) to hge and check for overflow", args(1,4, batarg("",hge),arg("s1",int),batarg("v",hge),batarg("s",oid))),
 command("batcalc", "hge", bathge_dec2dec_hge, false, "cast decimal(hge) to decimal(hge) and check for overflow", args(1,6, batarg("",hge),arg("s1",int),batarg("v",hge),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "bte", hge_num2dec_bte, false, "cast number to decimal(bte) and check for overflow", args(1,4, arg("",bte),arg("v",hge),arg("digits",int),arg("scale",int))),
 command("batcalc", "bte", bathge_num2dec_bte, false, "cast number to decimal(bte) and check for overflow", args(1,5, batarg("",bte),batarg("v",hge),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "bte", hge_dec2_bte, false, "cast decimal(hge) to bte and check for overflow", args(1,3, arg("",bte),arg("s1",int),arg("v",hge))),
 command("calc", "bte", hge_dec2dec_bte, false, "cast decimal(hge) to decimal(bte) and check for overflow", args(1,5, arg("",bte),arg("s1",int),arg("v",hge),arg("d2",int),arg("s2",int))),
 command("batcalc", "bte", bathge_dec2_bte, false, "cast decimal(hge) to bte and check for overflow", args(1,4, batarg("",bte),arg("s1",int),batarg("v",hge),batarg("s",oid))),
 command("batcalc", "bte", bathge_dec2dec_bte, false, "cast decimal(hge) to decimal(bte) and check for overflow", args(1,6, batarg("",bte),arg("s1",int),batarg("v",hge),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "sht", hge_num2dec_sht, false, "cast number to decimal(sht) and check for overflow", args(1,4, arg("",sht),arg("v",hge),arg("digits",int),arg("scale",int))),
 command("batcalc", "sht", bathge_num2dec_sht, false, "cast number to decimal(sht) and check for overflow", args(1,5, batarg("",sht),batarg("v",hge),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "sht", hge_dec2_sht, false, "cast decimal(hge) to sht and check for overflow", args(1,3, arg("",sht),arg("s1",int),arg("v",hge))),
 command("calc", "sht", hge_dec2dec_sht, false, "cast decimal(hge) to decimal(sht) and check for overflow", args(1,5, arg("",sht),arg("s1",int),arg("v",hge),arg("d2",int),arg("s2",int))),
 command("batcalc", "sht", bathge_dec2_sht, false, "cast decimal(hge) to sht and check for overflow", args(1,4, batarg("",sht),arg("s1",int),batarg("v",hge),batarg("s",oid))),
 command("batcalc", "sht", bathge_dec2dec_sht, false, "cast decimal(hge) to decimal(sht) and check for overflow", args(1,6, batarg("",sht),arg("s1",int),batarg("v",hge),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "int", hge_num2dec_int, false, "cast number to decimal(int) and check for overflow", args(1,4, arg("",int),arg("v",hge),arg("digits",int),arg("scale",int))),
 command("batcalc", "int", bathge_num2dec_int, false, "cast number to decimal(int) and check for overflow", args(1,5, batarg("",int),batarg("v",hge),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "int", hge_dec2_int, false, "cast decimal(hge) to int and check for overflow", args(1,3, arg("",int),arg("s1",int),arg("v",hge))),
 command("calc", "int", hge_dec2dec_int, false, "cast decimal(hge) to decimal(int) and check for overflow", args(1,5, arg("",int),arg("s1",int),arg("v",hge),arg("d2",int),arg("s2",int))),
 command("batcalc", "int", bathge_dec2_int, false, "cast decimal(hge) to int and check for overflow", args(1,4, batarg("",int),arg("s1",int),batarg("v",hge),batarg("s",oid))),
 command("batcalc", "int", bathge_dec2dec_int, false, "cast decimal(hge) to decimal(int) and check for overflow", args(1,6, batarg("",int),arg("s1",int),batarg("v",hge),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "lng", hge_num2dec_lng, false, "cast number to decimal(lng) and check for overflow", args(1,4, arg("",lng),arg("v",hge),arg("digits",int),arg("scale",int))),
 command("batcalc", "lng", bathge_num2dec_lng, false, "cast number to decimal(lng) and check for overflow", args(1,5, batarg("",lng),batarg("v",hge),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "lng", hge_dec2_lng, false, "cast decimal(hge) to lng and check for overflow", args(1,3, arg("",lng),arg("s1",int),arg("v",hge))),
 command("calc", "lng", hge_dec2dec_lng, false, "cast decimal(hge) to decimal(lng) and check for overflow", args(1,5, arg("",lng),arg("s1",int),arg("v",hge),arg("d2",int),arg("s2",int))),
 command("batcalc", "lng", bathge_dec2_lng, false, "cast decimal(hge) to lng and check for overflow", args(1,4, batarg("",lng),arg("s1",int),batarg("v",hge),batarg("s",oid))),
 command("batcalc", "lng", bathge_dec2dec_lng, false, "cast decimal(hge) to decimal(lng) and check for overflow", args(1,6, batarg("",lng),arg("s1",int),batarg("v",hge),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "flt", hge_num2dec_flt, false, "cast number to decimal(flt) and check for overflow", args(1,4, arg("",flt),arg("v",hge),arg("digits",int),arg("scale",int))),
 command("batcalc", "flt", bathge_num2dec_flt, false, "cast number to decimal(flt) and check for overflow", args(1,5, batarg("",flt),batarg("v",hge),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "flt", hge_dec2_flt, false, "cast decimal(hge) to flt and check for overflow", args(1,3, arg("",flt),arg("s1",int),arg("v",hge))),
 command("calc", "flt", hge_dec2dec_flt, false, "cast decimal(hge) to decimal(flt) and check for overflow", args(1,5, arg("",flt),arg("s1",int),arg("v",hge),arg("d2",int),arg("s2",int))),
 command("batcalc", "flt", bathge_dec2_flt, false, "cast decimal(hge) to flt and check for overflow", args(1,4, batarg("",flt),arg("s1",int),batarg("v",hge),batarg("s",oid))),
 command("batcalc", "flt", bathge_dec2dec_flt, false, "cast decimal(hge) to decimal(flt) and check for overflow", args(1,6, batarg("",flt),arg("s1",int),batarg("v",hge),batarg("s",oid),arg("d2",int),arg("s2",int))),
 command("calc", "dbl", hge_num2dec_dbl, false, "cast number to decimal(dbl) and check for overflow", args(1,4, arg("",dbl),arg("v",hge),arg("digits",int),arg("scale",int))),
 command("batcalc", "dbl", bathge_num2dec_dbl, false, "cast number to decimal(dbl) and check for overflow", args(1,5, batarg("",dbl),batarg("v",hge),batarg("s",oid),arg("digits",int),arg("scale",int))),
 command("calc", "dbl", hge_dec2_dbl, false, "cast decimal(hge) to dbl and check for overflow", args(1,3, arg("",dbl),arg("s1",int),arg("v",hge))),
 command("calc", "dbl", hge_dec2dec_dbl, false, "cast decimal(hge) to decimal(dbl) and check for overflow", args(1,5, arg("",dbl),arg("s1",int),arg("v",hge),arg("d2",int),arg("s2",int))),
 command("batcalc", "dbl", bathge_dec2_dbl, false, "cast decimal(hge) to dbl and check for overflow", args(1,4, batarg("",dbl),arg("s1",int),batarg("v",hge),batarg("s",oid))),
 command("batcalc", "dbl", bathge_dec2dec_dbl, false, "cast decimal(hge) to decimal(dbl) and check for overflow", args(1,6, batarg("",dbl),arg("s1",int),batarg("v",hge),batarg("s",oid),arg("d2",int),arg("s2",int))),
 /* sql_rank_hge */
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, arg("",oid),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("start",hge))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("start",hge))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, arg("",oid),arg("p",bit),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("start",hge))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("start",hge))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),batarg("start",hge))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),batarg("start",hge))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",hge),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",hge),batarg("b",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",hge),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",hge),batarg("b",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",hge),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",hge),batarg("b",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",hge),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",hge),batarg("b",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",hge),arg("b",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",hge),batarg("b",hge),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",hge),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",hge),batarg("b",bte),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",hge),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",hge),batarg("b",sht),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",hge),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",hge),batarg("b",int),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",hge),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",hge),batarg("b",lng),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",hge),arg("b",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",hge),batarg("b",hge),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "avg", SQLavg, false, "return the average of groups", args(1,7, arg("",dbl),arg("b",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavg, false, "return the average of groups", args(1,7, batarg("",dbl),batarg("b",hge),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, arg("",hge),arg("b",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, batarg("",hge),batarg("b",hge),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, arg("",dbl),arg("b",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, batarg("",dbl),batarg("b",hge),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, arg("",dbl),arg("b",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, batarg("",dbl),batarg("b",hge),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, arg("",dbl),arg("b",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, batarg("",dbl),batarg("b",hge),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, arg("",dbl),arg("b",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, batarg("",dbl),batarg("b",hge),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, arg("",dbl),arg("b",hge),arg("c",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),arg("b",hge),batarg("c",hge),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),batarg("b",hge),arg("c",hge),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),batarg("b",hge),batarg("c",hge),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, arg("",dbl),arg("b",hge),arg("c",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),arg("b",hge),batarg("c",hge),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),batarg("b",hge),arg("c",hge),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),batarg("b",hge),batarg("c",hge),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("sql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, arg("",dbl),arg("b",hge),arg("c",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),arg("b",hge),batarg("c",hge),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),batarg("b",hge),arg("c",hge),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),batarg("b",hge),batarg("c",hge),argany("p",0),argany("o",0),arg("t",int),argany("s",0),argany("e",0))),
#endif
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_sql_mal)
{ mal_module("sql", NULL, sql_init_funcs); }
