/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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
#include "sql_query.h"
#include "sql_storage.h"
#include "sql_scenario.h"
#include "store_sequence.h"
#include "sql_partition.h"
#include "rel_basetable.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_dump.h"
#include "rel_select.h"
#include "rel_physical.h"
#include "rel_remote.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_resolve.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_scenario.h"
#include "mal_profiler.h"
#include "bat5.h"
#include "opt_pipes.h"
#include "clients.h"
#include "mal_instruction.h"
#include "mal_resource.h"
#include "mal_authorize.h"

static inline void
BBPnreclaim(int nargs, ...)
{
	va_list valist;
	va_start(valist, nargs);
	for (int i = 0; i < nargs; i++) {
		BAT *b = va_arg(valist, BAT *);
		BBPreclaim(b);
	}
	va_end(valist);
}

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
rel_no_mitosis(mvc *sql, sql_rel *rel)
{
	if (mvc_highwater(sql))
		return 0;
	if (!rel || is_basetable(rel->op))
		return 1;
	/* use mitosis on order topn */
	if (is_topn(rel->op)) {
		sql_rel *l = rel->l;
		if (l && is_simple_project(l->op) && l->r)
			return 0;
	}
	if (is_topn(rel->op) || is_sample(rel->op) || is_simple_project(rel->op))
		return rel_no_mitosis(sql, rel->l);
	if (is_ddl(rel->op) && rel->flag == ddl_output) {
		// COPY SELECT ... INTO
		return rel_no_mitosis(sql, rel->l);
	}
	if ((is_delete(rel->op) || is_truncate(rel->op)) && rel->card <= CARD_AGGR)
		return 1;
	if ((is_insert(rel->op) || is_update(rel->op)) && rel->card <= CARD_AGGR)
		return rel_no_mitosis(sql, rel->r);
	if (is_select(rel->op) && rel_is_table(rel->l) && !list_empty(rel->exps)) {
		/* just one point expression makes this a point query */
		if (exp_is_point_select(rel->exps->h->data))
			return 1;
	}
	return 0;
}

static int
rel_need_distinct_query(sql_rel *rel)
{
	int need_distinct = 0;

	while (rel && is_simple_project(rel->op))
		rel = rel->l;
	if (rel && is_groupby(rel->op) && !list_empty(rel->exps) && list_empty(rel->r)) {
		for (node *n = rel->exps->h; n && !need_distinct; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_aggr && need_distinct(e))
				need_distinct = 1;
		}
	}
	return need_distinct;
}

sql_rel *
sql_symbol2relation(backend *be, symbol *sym)
{
	sql_rel *rel;
	sql_query *query = query_create(be->mvc);
	lng Tbegin, Tend;
	int value_based_opt = be->mvc->emode != m_prepare, storage_based_opt;
	int profile = be->mvc->emode == m_plan;
	Client c = be->client;

	Tbegin = GDKusec();
	rel = rel_semantic(query, sym);
	Tend = GDKusec();
	if(profilerStatus > 0 )
		profilerEvent(NULL,
					  &(struct NonMalEvent)
					  {SQL_TO_REL, c, Tend, NULL, NULL, rel?0:1, Tend-Tbegin});

	storage_based_opt = value_based_opt && rel && !is_ddl(rel->op);
	Tbegin = Tend;
	if (rel && !(rel->op == op_ddl && rel->card == CARD_ATOM && rel->flag == ddl_psm && (be->mvc->emod & mod_exec) != 0)) { /* no need to optimize exec */
		if (rel)
			rel = sql_processrelation(be->mvc, rel, profile, 1, value_based_opt, storage_based_opt);
		if (rel && (rel_no_mitosis(be->mvc, rel) || rel_need_distinct_query(rel)))
			be->no_mitosis = 1;
		if (rel)
			rel = rel_physical(be->mvc, rel);
	}
	Tend = GDKusec();
	be->reloptimizer = Tend - Tbegin;

	if(profilerStatus > 0)
		profilerEvent(NULL,
					  &(struct NonMalEvent)
					  {REL_OPT, c, Tend, NULL, NULL, rel?0:1, be->reloptimizer});
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
	be->mvc->runs = NULL;
	if (be->mvc->ta)
		be->mvc->ta = sa_reset(be->mvc->ta);
	if (be->mvc->sa)
		be->mvc->sa = sa_reset(be->mvc->sa);
	if (err >0)
		be->mvc->session->status = -err;
	if (err <0)
		be->mvc->session->status = err;
	be->mvc->label = 0;
	be->mvc->nid = 1;
	be->no_mitosis = 0;
	mvc_query_processed(be->mvc);
	return err;
}

/*
 * The internal administration of the MAL compiler and execution state
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
create_table_or_view(mvc *sql, char *sname, char *tname, sql_table *t, int temp, int replace)
{
	allocator *osa;
	sql_schema *s = mvc_bind_schema(sql, sname);
	sql_table *nt = NULL, *ot;
	node *n;
	int check = 0;
	const char *action = (temp == SQL_DECLARED_TABLE) ? "DECLARE" : (replace ? "CREATE OR REPLACE" : "CREATE");
	const char *obj = t->query ? "VIEW" : "TABLE";
	str msg = MAL_SUCCEED;

	if (store_readonly(sql->session->tr->store))
		throw(SQL, "sql.catalog", SQLSTATE(25006) "schema statements cannot be executed on a readonly database.");

	if (!s)
		throw(SQL, "sql.catalog", SQLSTATE(3F000) "%s %s: schema '%s' doesn't exist", action, obj, sname);
	if (temp != SQL_DECLARED_TABLE && (!mvc_schema_privs(sql, s) && !(isTempSchema(s) && temp == SQL_LOCAL_TEMP)))
		throw(SQL, "sql.catalog", SQLSTATE(42000) "%s %s: insufficient privileges for user '%s' in schema '%s'",
						 action, obj, get_string_global_var(sql, "current_user"), s->base.name);
	if ((ot = mvc_bind_table(sql, s, t->base.name))) {
		if (replace) {
			if (ot->type != t->type)
				throw(SQL, "sql.catalog", SQLSTATE(42000) "%s %s: unable to drop %s '%s': is a %s",
								 action, obj, obj, t->base.name, TABLE_TYPE_DESCRIPTION(ot->type, ot->properties));
			if (ot->system)
				throw(SQL, "sql.catalog", SQLSTATE(42000) "%s %s: cannot replace system %s '%s'", action, obj, obj, t->base.name);
			if (mvc_check_dependency(sql, ot->base.id, isView(ot) ? VIEW_DEPENDENCY : TABLE_DEPENDENCY, NULL))
				throw(SQL, "sql.catalog", SQLSTATE(42000) "%s %s: cannot replace %s '%s', there are database objects which depend on it",
								 action, obj, obj, t->base.name);
			if ((msg = mvc_drop_table(sql, s, ot, 0)) != MAL_SUCCEED)
				return msg;
		} else {
			throw(SQL, "sql.catalog", SQLSTATE(42S01) "%s %s: name '%s' already in use", action, obj, t->base.name);
		}
	}
	if (temp == SQL_DECLARED_TABLE && ol_length(t->keys))
		throw(SQL, "sql.catalog", SQLSTATE(42000) "%s %s: '%s' cannot have constraints", action, obj, t->base.name);

	switch (sql_trans_create_table(&nt, sql->session->tr, s, tname, t->query, t->type, t->system, temp, t->commit_action, t->sz, t->properties)) {
		case -1:
			throw(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		case -2:
		case -3:
			throw(SQL, "sql.catalog", SQLSTATE(42000) "%s %s: '%s' name conflicts", action, obj, t->base.name);
		default:
			break;
	}
	osa = sql->sa;
	allocator *nsa = sql->sa = sa_create(NULL);
	/* first check default values */
	for (n = ol_first_node(t->columns); n; n = n->next) {
		sql_column *c = n->data;

		if (c->def) {
			/* TODO please don't place an auto incremented sequence in the default value */
			const char next_value_for[] = "next value for \"sys\".\"seq_";
			sql_rel *r = NULL;

			sa_reset(nsa);
			sql->sa = nsa;
			r = rel_parse(sql, s, sa_message(sql->ta, "select %s;", c->def), m_deps);
			if (!r || !is_project(r->op) || !r->exps || list_length(r->exps) != 1 ||
				exp_check_type(sql, &c->type, r, r->exps->h->data, type_equal) == NULL) {
				if (r)
					rel_destroy(r);
				sa_destroy(nsa);
				sql->sa = osa;
				if (strlen(sql->errstr) > 6 && sql->errstr[5] == '!')
					throw(SQL, "sql.catalog", "%s", sql->errstr);
				else
					throw(SQL, "sql.catalog", SQLSTATE(42000) "%s", sql->errstr);
			}
			/* For a self incremented column, it's sequence will get a BEDROPPED_DEPENDENCY,
				so no additional dependencies are needed */
			if (strncmp(c->def, next_value_for, strlen(next_value_for)) != 0) {
				list *blist = rel_dependencies(sql, r);
				if (mvc_create_dependencies(sql, blist, nt->base.id, FUNC_DEPENDENCY)) {
					sa_destroy(nsa);
					sql->sa = osa;
					throw(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			}
			sa_reset(sql->sa);
		}
	}

	for (n = ol_first_node(t->columns); n; n = n->next) {
		sql_column *c = n->data, *copied = NULL;

		switch (mvc_copy_column(sql, nt, c, &copied)) {
			case -1:
				sa_destroy(nsa);
				sql->sa = osa;
				throw(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			case -2:
			case -3:
				sa_destroy(nsa);
				sql->sa = osa;
				throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: %s_%s_%s conflicts", s->base.name, t->base.name, c->base.name);
			default:
				break;
		}
		if (isPartitionedByColumnTable(t) && c->base.id == t->part.pcol->base.id)
			nt->part.pcol = copied;
	}
	if (isPartitionedByExpressionTable(t)) {
		char *err = NULL;

		_DELETE(nt->part.pexp->exp);
		nt->part.pexp->exp = _STRDUP(t->part.pexp->exp);
		err = bootstrap_partition_expression(sql, nt, 1);
		if (err) {
			sa_destroy(nsa);
			sql->sa = osa;
			return err;
		}
		sa_reset(nsa);
	}
	check = sql_trans_set_partition_table(sql->session->tr, nt);
	if (check == -4) {
		sa_destroy(nsa);
		sql->sa = osa;
		throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: %s_%s: the partition's expression is too long", s->base.name, t->base.name);
	} else if (check) {
		sa_destroy(nsa);
		sql->sa = osa;
		throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: %s_%s: an internal error occurred", s->base.name, t->base.name);
	}

	if (t->idxs) {
		for (n = ol_first_node(t->idxs); n; n = n->next) {
			sql_idx *i = n->data;

			switch (mvc_copy_idx(sql, nt, i, NULL)) {
				case -1:
					sa_destroy(nsa);
					sql->sa = osa;
					throw(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				case -2:
				case -3:
					sa_destroy(nsa);
					sql->sa = osa;
					throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: %s_%s_%s index conflicts", s->base.name, t->base.name, i->base.name);
				default:
					break;
			}
		}
	}
	if (t->keys) {
		for (n = ol_first_node(t->keys); n; n = n->next) {
			sql_key *k = n->data;
			char *err = NULL;

			err = sql_partition_validate_key(sql, nt, k, "CREATE");
			if (err) {
				sa_destroy(nsa);
				sql->sa = osa;
				return err;
			}
			sa_reset(sql->sa);
			switch (mvc_copy_key(sql, nt, k, NULL)) {
				case -1:
					sa_destroy(nsa);
					sql->sa = osa;
					throw(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				case -2:
				case -3:
					sa_destroy(nsa);
					sql->sa = osa;
					throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: %s_%s_%s constraint conflicts", s->base.name, t->base.name, k->base.name);
				default:
					break;
			}
			sa_reset(sql->sa);
		}
	}
	if (t->triggers) {
		for (n = ol_first_node(t->triggers); n; n = n->next) {
			sql_trigger *tr = n->data;

			switch (mvc_copy_trigger(sql, nt, tr, NULL)) {
				case -1:
					sa_destroy(nsa);
					sql->sa = osa;
					throw(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				case -2:
				case -3:
					sa_destroy(nsa);
					sql->sa = osa;
					throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: %s_%s_%s trigger conflicts", s->base.name, t->base.name, nt->base.name);
				default:
					break;
			}
		}
	}
	/* also create dependencies when not renaming */
	if (nt->query && isView(nt)) {
		sql_rel *r = NULL;

		sa_reset(nsa);
		r = rel_parse(sql, s, nt->query, m_deps);
		if (r)
			r = sql_processrelation(sql, r, 0, 0, 0, 0);
		if (r) {
			list *blist = rel_dependencies(sql, r);
			if (mvc_create_dependencies(sql, blist, nt->base.id, VIEW_DEPENDENCY)) {
				sa_destroy(nsa);
				sql->sa = osa;
				throw(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
		sql->sa = osa;
		if (!r) {
			sa_destroy(nsa);
			if (strlen(sql->errstr) > 6 && sql->errstr[5] == '!')
				throw(SQL, "sql.catalog", "%s", sql->errstr);
			else
				throw(SQL, "sql.catalog", SQLSTATE(42000) "%s", sql->errstr);
		}
	}
	sa_destroy(nsa);
	sql->sa = osa;
	return MAL_SUCCEED;
}

static int
mvc_claim_slots(sql_trans *tr, sql_table *t, size_t cnt, BUN *offset, BAT **pos)
{
	sqlstore *store = tr->store;
	return store->storage_api.claim_tab(tr, t, cnt, offset, pos);
}

str
mvc_claim_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BUN *offset = (BUN*)getArgReference_oid(stk, pci, 0);
	bat *res = getArgReference_bat(stk, pci, 1);
	mvc *m = NULL;
	str msg;
	const char *sname = *getArgReference_str(stk, pci, 3);
	const char *tname = *getArgReference_str(stk, pci, 4);
	lng cnt = *getArgReference_lng(stk, pci, 5);
	BAT *pos = NULL;
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
	if (!isTable(t))
		throw(SQL, "sql.claim", SQLSTATE(42000) "%s '%s' is not persistent", TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
	if (mvc_claim_slots(m->session->tr, t, (size_t)cnt, offset, &pos) == LOG_OK) {
		*res = bat_nil;
		if (pos) {
			*res = pos->batCacheid;
			BBPkeepref(pos);
		}
		return MAL_SUCCEED;
	}
	throw(SQL, "sql.claim", SQLSTATE(3F000) "Could not claim slots");
}

str
mvc_add_dependency_change(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg;
	mvc *m = NULL;
	const char *sname = *getArgReference_str(stk, pci, 1);
	const char *tname = *getArgReference_str(stk, pci, 2);
	lng cnt = *getArgReference_lng(stk, pci, 3);
	sql_schema *s;
	sql_table *t;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if ((s = mvc_bind_schema(m, sname)) == NULL)
		throw(SQL, "sql.dependency_change", SQLSTATE(3F000) "Schema missing %s", sname);
	if ((t = mvc_bind_table(m, s, tname)) == NULL)
		throw(SQL, "sql.dependency_change", SQLSTATE(42S02) "Table missing %s.%s", sname, tname);
	if (!isTable(t))
		throw(SQL, "sql.dependency_change", SQLSTATE(42000) "%s '%s' is not persistent", TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
	if (cnt > 0 && !isNew(t) && isGlobal(t) && !isGlobalTemp(t) && sql_trans_add_dependency_change(m->session->tr, t->base.id, dml) != LOG_OK)
		throw(SQL, "sql.dependency_change", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str
mvc_add_column_predicate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg;
	mvc *m = NULL;
	const char *sname = *getArgReference_str(stk, pci, 1);
	const char *tname = *getArgReference_str(stk, pci, 2);
	const char *cname = *getArgReference_str(stk, pci, 3);
	sql_schema *s;
	sql_table *t;
	sql_column *c;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if ((s = mvc_bind_schema(m, sname)) == NULL)
		throw(SQL, "sql.column_predicate", SQLSTATE(3F000) "Schema missing %s", sname);
	if ((t = mvc_bind_table(m, s, tname)) == NULL)
		throw(SQL, "sql.column_predicate", SQLSTATE(42S02) "Table missing %s.%s", sname, tname);
	if ((c = mvc_bind_column(m, t, cname)) == NULL)
		throw(SQL, "sql.column_predicate", SQLSTATE(42S22) "Column not found in %s.%s.%s",sname,tname,cname);

	if ((m->session->level & tr_snapshot) == tr_snapshot || isNew(c) || !isGlobal(c->t) || isGlobalTemp(c->t))
		return MAL_SUCCEED;
	if (sql_trans_add_predicate(m->session->tr, c, 0, NULL, NULL, false, false) != LOG_OK)
		throw(SQL, "sql.column_predicate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str
create_table_from_emit(Client cntxt, char *sname, char *tname, sql_emit_col *columns, size_t ncols)
{
	size_t i;
	sql_table *t = NULL;
	sql_schema *s = NULL;
	mvc *sql = NULL;
	str msg = MAL_SUCCEED;

	if ((msg = getSQLContext(cntxt, NULL, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if (!sname)
		sname = "sys";
	if (!(s = mvc_bind_schema(sql, sname)))
		throw(SQL, "sql.catalog", SQLSTATE(3F000) "CREATE TABLE: no such schema '%s'", sname);
	if (!mvc_schema_privs(sql, s))
		throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: Access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), s->base.name);
	switch (mvc_create_table(&t, sql, s, tname, tt_table, 0, SQL_DECLARED_TABLE, CA_COMMIT, -1, 0)) {
		case -1:
			throw(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		case -2:
		case -3:
			throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: transaction conflict detected");
		default:
			break;
	}

	for (i = 0; i < ncols; i++) {
		BAT *b = columns[i].b;
		const char *atomname = ATOMname(b->ttype);
		sql_subtype tpe;
		sql_column *col = NULL;

		if (!strcmp(atomname, "str"))
			sql_find_subtype(&tpe, "varchar", 0, 0);
		else {
			sql_subtype *t = sql_fetch_localtype(b->ttype);
			if (!t)
				throw(SQL, "sql.catalog", SQLSTATE(3F000) "CREATE TABLE: could not find type for column");
			tpe = *t;
		}

		if (columns[i].name && columns[i].name[0] == '%')
			throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: generated labels not allowed in column names, use an alias instead");
		if (tpe.type->eclass == EC_ANY)
			throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: any type (plain null value) not allowed as a column type, use an explicit cast");
		switch (mvc_create_column(&col, sql, t, columns[i].name, &tpe)) {
			case -1:
				throw(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			case -2:
			case -3:
				throw(SQL, "sql.catalog", SQLSTATE(42000) "CREATE TABLE: transaction conflict detected");
			default:
				break;
		}
	}
	if ((msg = create_table_or_view(sql, sname, t->base.name, t, 0, 0)) != MAL_SUCCEED)
		return msg;
	if (!(t = mvc_bind_table(sql, s, tname)))
		throw(SQL, "sql.catalog", SQLSTATE(3F000) "CREATE TABLE: could not bind table %s", tname);
	BUN offset;
	BAT *pos = NULL;
	if (mvc_claim_slots(sql->session->tr, t, BATcount(columns[0].b), &offset, &pos) != LOG_OK)
		throw(SQL, "sql.catalog", SQLSTATE(3F000) "CREATE TABLE: Could not insert data");
	for (i = 0; i < ncols; i++) {
		BAT *b = columns[i].b;
		sql_column *col = NULL;

		if (!(col = mvc_bind_column(sql, t, columns[i].name))) {
			bat_destroy(pos);
			throw(SQL, "sql.catalog", SQLSTATE(3F000) "CREATE TABLE: could not bind column %s", columns[i].name);
		}
		if ((msg = mvc_append_column(sql->session->tr, col, offset, pos, b)) != MAL_SUCCEED) {
			bat_destroy(pos);
			return msg;
		}
	}
	bat_destroy(pos);
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

	if (!sname)
		sname = "sys";
	if (!(s = mvc_bind_schema(sql, sname)))
		throw(SQL, "sql.catalog", SQLSTATE(3F000) "APPEND TABLE: no such schema '%s'", sname);
	if (!(t = mvc_bind_table(sql, s, tname)))
		throw(SQL, "sql.catalog", SQLSTATE(3F000) "APPEND TABLE: could not bind table %s", tname);
	if (!isTable(t))
		throw(SQL, "sql.catalog", SQLSTATE(42000) "APPEND TABLE: %s '%s' is not persistent", TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
	BUN offset;
	BAT *pos = NULL;
	if (mvc_claim_slots(sql->session->tr, t, BATcount(columns[0].b), &offset, &pos) != LOG_OK)
		throw(SQL, "sql.catalog", SQLSTATE(3F000) "APPEND TABLE: Could not append data");
	for (i = 0; i < ncols; i++) {
		BAT *b = columns[i].b;
		sql_column *col = NULL;

		if (!(col = mvc_bind_column(sql, t, columns[i].name))) {
			bat_destroy(pos);
			throw(SQL, "sql.catalog", SQLSTATE(3F000) "APPEND TABLE: could not bind column %s", columns[i].name);
		}
		if ((msg = mvc_append_column(sql->session->tr, col, offset, pos, b)) != MAL_SUCCEED) {
			bat_destroy(pos);
			return msg;
		}
	}
	bat_destroy(pos);
	if (BATcount(columns[0].b) > 0 && !isNew(t) && isGlobal(t) && !isGlobalTemp(t) &&
		sql_trans_add_dependency_change(sql->session->tr, t->base.id, dml) != LOG_OK)
		throw(SQL, "sql.catalog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
	if (t == NULL || !isTable(t))
		return NULL;
	c = mvc_bind_column(m, t, cname);
	if (c == NULL)
		return NULL;

	sqlstore *store = tr->store;
	b = store->storage_api.bind_col(tr, c, access);
	return b;
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
			if (BUNappend(types, value.tpe.type->base.name, false) != GDK_SUCCEED) {
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
		*s = schemas->batCacheid;
		BBPkeepref(schemas);
		*n = names->batCacheid;
		BBPkeepref(names);
		*t = types->batCacheid;
		BBPkeepref(types);
		*v = values->batCacheid;
		BBPkeepref(values);
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

str
mvc_next_value_bulk(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *be = NULL;
	str msg;
	sql_schema *s;
	sql_sequence *seq;
	bat *res = getArgReference_bat(stk, pci, 0);
	BUN card = (BUN)*getArgReference_lng(stk, pci, 1);
	const char *sname = *getArgReference_str(stk, pci, 2);
	const char *seqname = *getArgReference_str(stk, pci, 3);
	BAT *r = NULL;

	(void)mb;
	if ((msg = getBackendContext(cntxt, &be)) != NULL)
		return msg;
	if (!(s = mvc_bind_schema(be->mvc, sname)))
		throw(SQL, "sql.next_value", SQLSTATE(3F000) "Cannot find the schema %s", sname);
	if (!mvc_schema_privs(be->mvc, s))
		throw(SQL, "sql.next_value", SQLSTATE(42000) "Access denied for %s to schema '%s'", get_string_global_var(be->mvc, "current_user"), s->base.name);
	if (!(seq = find_sql_sequence(be->mvc->session->tr, s, seqname)))
		throw(SQL, "sql.next_value", SQLSTATE(HY050) "Cannot find the sequence %s.%s", sname, seqname);
	if (!(r = COLnew(0, TYPE_lng, card, TRANSIENT)))
		throw(SQL, "sql.next_value", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	lng *restrict rb = Tloc(r, 0);

	if (seqbulk_next_value(be->mvc->session->tr->store, seq, card, rb)) {
		be->last_id = rb[card-1];
		sqlvar_set_number(find_global_var(be->mvc, mvc_bind_schema(be->mvc, "sys"), "last_id"), be->last_id);
		BATsetcount(r, card);
		r->tnonil = true;
		r->tnil = false;
		/* TODO set the min/max, tsorted/trevsorted and tkey properties based on the sequence values */
		r->tsorted = r->trevsorted = r->tkey = BATcount(r) <= 1;
		*res = r->batCacheid;
		BBPkeepref(r);
		return MAL_SUCCEED;
	}
	BBPreclaim(r);
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

/* needed for msqldump and describe_sequences view */
static str
mvc_get_value_bulk(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	sql_schema *s;
	sql_sequence *seq;
	BATiter schi, seqi;
	BAT *bn = NULL, *scheb = NULL, *sches = NULL, *seqb = NULL, *seqs = NULL;
	lng *restrict vals;
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1), *r = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 5 ? getArgReference_bat(stk, pci, 3) : NULL, *sid2 = pci->argc == 5 ? getArgReference_bat(stk, pci, 4) : NULL;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (!(scheb = BATdescriptor(*l)) || !(seqb = BATdescriptor(*r))) {
		msg = createException(SQL, "sql.get_value", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(sches = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(seqs = BATdescriptor(*sid2)))) {
		msg = createException(SQL, "sql.get_value", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	canditer_init(&ci1, scheb, sches);
	canditer_init(&ci2, seqb, seqs);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq) {
		msg = createException(SQL, "sql.get_value", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_lng, ci1.ncand, TRANSIENT))) {
		msg = createException(SQL, "sql.get_value", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = scheb->hseqbase;
	off2 = seqb->hseqbase;
	schi = bat_iterator(scheb);
	seqi = bat_iterator(seqb);
	vals = Tloc(bn, 0);
	for (BUN i = 0; i < ci1.ncand; i++) {
		oid p1 = canditer_next(&ci1) - off1, p2 = canditer_next(&ci2) - off2;
		const char *sname = BUNtvar(schi, p1);
		const char *seqname = BUNtvar(seqi, p2);

		if (strNil(sname) || strNil(seqname)) {
			vals[i] = lng_nil;
			nils = true;
		} else {
			if (!(s = mvc_bind_schema(m, sname))) {
				msg = createException(SQL, "sql.get_value", SQLSTATE(3F000) "Cannot find the schema %s", sname);
				goto bailout1;
			}
			if (!(seq = find_sql_sequence(m->session->tr, s, seqname))) {
				msg = createException(SQL, "sql.get_value", SQLSTATE(HY050) "Cannot find the sequence %s.%s", sname, seqname);
				goto bailout1;
			}
			if (!seq_get_value(m->session->tr->store, seq, &(vals[i]))) {
				msg = createException(SQL, "sql.get_value", SQLSTATE(HY050) "Cannot get the next sequence value %s.%s", sname, seqname);
				goto bailout1;
			}
		}
	}

bailout1:
	bat_iterator_end(&schi);
	bat_iterator_end(&seqi);
bailout:
	BBPreclaim(scheb);
	BBPreclaim(sches);
	BBPreclaim(seqb);
	BBPreclaim(seqs);
	if (bn && !msg) {
		BATsetcount(bn, ci1.ncand);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		*res = bn->batCacheid;
		BBPkeepref(bn);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
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
	if (start < seq->minvalue)
		throw(SQL, "sql.restart", SQLSTATE(HY050) "Cannot set sequence %s.%s start to a value less than the minimum ("LLFMT" < "LLFMT")", sname, seqname, start, seq->minvalue);
	if (start > seq->maxvalue)
		throw(SQL, "sql.restart", SQLSTATE(HY050) "Cannot set sequence %s.%s start to a value higher than the maximum ("LLFMT" > "LLFMT")", sname, seqname, start, seq->maxvalue);
	switch (sql_trans_sequence_restart(m->session->tr, seq, start)) {
		case -1:
			throw(SQL,"sql.restart",SQLSTATE(HY013) MAL_MALLOC_FAIL);
		case -2:
		case -3:
			throw(SQL,"sql.restart",SQLSTATE(42000) "RESTART SEQUENCE: transaction conflict detected");
		case -4:
			throw(SQL,"sql.restart",SQLSTATE(HY050) "Cannot (re)start sequence %s.%s", sname, seqname);
		default:
			*res = start;
	}
	return MAL_SUCCEED;
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
	if (i == NULL || !isTable(i->t))
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
	BAT *b = NULL;
	bat *bid = getArgReference_bat(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	const char *sname	= *getArgReference_str(stk, pci, 2 + upd);
	const char *tname	= *getArgReference_str(stk, pci, 3 + upd);
	const char *cname	= *getArgReference_str(stk, pci, 4 + upd);
	const int	access	= *getArgReference_int(stk, pci, 5 + upd);

	const bool partitioned_access = pci->argc == (8 + upd) && getArgType(mb, pci, 6 + upd) == TYPE_int;

	/* This doesn't work with quick access for now... */
	assert(access != QUICK);
	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	sqlstore *store = m->store;
	sql_schema *s = mvc_bind_schema(m, sname);
	sql_table *t = mvc_bind_table(m, s, tname);
	if (t && !isTable(t))
		throw(SQL, "sql.bind", SQLSTATE(42000) "%s '%s' is not persistent",
			  TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
	sql_column *c = mvc_bind_column(m, t, cname);

	if (partitioned_access) {
		/* partitioned access */
		int part_nr = *getArgReference_int(stk, pci, 6 + upd);
		int nr_parts = *getArgReference_int(stk, pci, 7 + upd);
		BUN cnt = store->storage_api.count_col(m->session->tr, c, 0), psz;
		oid l, h;
		psz = cnt ? (cnt / nr_parts) : 0;
		l = part_nr * psz;
		if (l > cnt)
			l = cnt;
		h = (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz);
		if (h > cnt)
			h = cnt;

		if (upd) {
			BAT *ui = NULL, *uv = NULL;
			if (store->storage_api.bind_updates(m->session->tr, c, &ui, &uv) == LOG_ERR)
				throw(SQL,"sql.bind",SQLSTATE(HY005) "Cannot access the update columns");

			h--;
			BAT* bn = BATselect(ui, NULL, &l, &h, true, true, false, false);
			if(bn == NULL) {
				BBPunfix(ui->batCacheid);
				BBPunfix(uv->batCacheid);
				throw(SQL, "sql.bind", GDK_EXCEPTION);
			}

			bat *uvl = getArgReference_bat(stk, pci, 1);

			if (BATcount(bn)) {
				BAT *id;
				BAT *vl;
				if (ui == NULL || uv == NULL) {
					bat_destroy(uv);
					bat_destroy(ui);
					BBPunfix(bn->batCacheid);
					throw(SQL,"sql.bind",SQLSTATE(HY005) "Cannot access the insert column %s.%s.%s",
						sname, tname, cname);
				}
				assert(uv->batCount == ui->batCount);
				id = BATproject(bn, ui);
				vl = BATproject(bn, uv);
				bat_destroy(ui);
				bat_destroy(uv);
				if (id == NULL || vl == NULL) {
					BBPunfix(bn->batCacheid);
					bat_destroy(id);
					bat_destroy(vl);
					throw(SQL, "sql.bind", GDK_EXCEPTION);
				}
				if ( BATcount(id) != BATcount(vl)){
					BBPunfix(bn->batCacheid);
					bat_destroy(id);
					bat_destroy(vl);
					throw(SQL, "sql.bind", SQLSTATE(0000) "Inconsistent BAT count");
				}
				BBPkeepref(id);
				BBPkeepref(vl);
				*bid = id->batCacheid;
				*uvl = vl->batCacheid;
			} else {
				*bid = e_bat(TYPE_oid);
				*uvl = e_bat(c->type.type->localtype);
				if (*bid == BID_NIL || *uvl == BID_NIL) {
					if (*bid)
						BBPunfix(*bid);
					if (*uvl)
						BBPunfix(*uvl);
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.bind", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			}
		} else {
			int coltype = getBatType(getArgType(mb, pci, 0));
			b = store->storage_api.bind_col(m->session->tr, c, access);
			if (b == NULL)
				throw(SQL, "sql.bind", SQLSTATE(42000) "Cannot bind column %s.%s.%s", sname, tname, cname);

			if (b->ttype && b->ttype != coltype) {
				BBPunfix(b->batCacheid);
				throw(SQL,"sql.bind",SQLSTATE(42000) "Column type mismatch %s.%s.%s",sname,tname,cname);
			}

			BAT* bn = BATslice(b, l, h);
			if(bn == NULL) {
				BBPunfix(b->batCacheid);
				throw(SQL, "sql.bind", GDK_EXCEPTION);
			}
			BAThseqbase(bn, l);
			BBPunfix(b->batCacheid);
			BBPkeepref(bn);
			*bid = bn->batCacheid;
		}
	}
	else if (upd) { /*unpartitioned access to update bats*/
		BAT *ui = NULL, *uv = NULL;
		if (store->storage_api.bind_updates(m->session->tr, c, &ui, &uv) == LOG_ERR)
			throw(SQL,"sql.bind",SQLSTATE(HY005) "Cannot access the update columns");

		bat *uvl = getArgReference_bat(stk, pci, 1);
		BBPkeepref(ui);
		BBPkeepref(uv);
		*bid = ui->batCacheid;
		*uvl = uv->batCacheid;
	}
	else { /*unpartitioned access to base column*/
		int coltype = getBatType(getArgType(mb, pci, 0));
		b = store->storage_api.bind_col(m->session->tr, c, access);
		if (b == NULL)
			throw(SQL, "sql.bin", "Couldn't bind column");

		if (b->ttype && b->ttype != coltype) {
			BBPunfix(b->batCacheid);
			throw(SQL,"sql.bind",SQLSTATE(42000) "Column type mismatch %s.%s.%s",sname,tname,cname);
		}
		BBPkeepref(b);
		*bid = b->batCacheid;
	}
	return MAL_SUCCEED;
}

/* The output of this function are 7 columns:
 *  - The sqlid of the column
 *  - Number of values of the column.
 *  - Number of segments, indication of the fragmentation
 *  - Number of inserted rows during the current transaction.
 *  - Number of updated rows during the current transaction.
 *  - Number of deletes of the column's table.
 *  - the number in the transaction chain (.i.e for each savepoint a new transaction is added in the chain)
 */

static str
mvc_insert_delta_values(mvc *m, BAT *col1, BAT *col2, BAT *col3, BAT *col4, BAT *col5, BAT *col6, BAT *col7, sql_column *c, lng segments, lng deletes)
{
	int level = 0;
	sqlstore *store = m->session->tr->store;

	lng inserted = (lng) store->storage_api.count_col(m->session->tr, c, 1);
	lng all = (lng) store->storage_api.count_col(m->session->tr, c, 0);
	lng updates = (lng) store->storage_api.count_col(m->session->tr, c, 2);

	if (BUNappend(col1, &c->base.id, false) != GDK_SUCCEED) {
		return createException(SQL,"sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (BUNappend(col2, &segments, false) != GDK_SUCCEED) {
		return createException(SQL,"sql.delta", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (BUNappend(col3, &all, false) != GDK_SUCCEED) {
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
	BUN nrows = 0;
	lng deletes, segments;

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
			throw(SQL, "sql.delta", SQLSTATE(42000) "%s doesn't have delta values", TABLE_TYPE_DESCRIPTION(t->type, t->properties));
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
	if ((col2 = COLnew(0, TYPE_lng, nrows, TRANSIENT)) == NULL) {
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
			deletes = (lng) store->storage_api.count_del(m->session->tr, t, 0);
			segments = (lng) store->storage_api.count_del(m->session->tr, t, CNT_ACTIVE);
			if (cname) {
				if ((msg=mvc_insert_delta_values(m, col1, col2, col3, col4, col5, col6, col7, c, segments, deletes)) != NULL)
					goto cleanup;
			} else {
				for (n = ol_first_node(t->columns); n ; n = n->next) {
					c = (sql_column*) n->data;
					if ((msg=mvc_insert_delta_values(m, col1, col2, col3, col4, col5, col6, col7, c, segments, deletes)) != NULL)
						goto cleanup;
				}
			}
		} else if (s->tables) {
			struct os_iter oi;
			os_iterator(&oi, s->tables, tr, NULL);
			for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
				t = (sql_table *)b;
				if (isTable(t)) {
					deletes = (lng) store->storage_api.count_del(m->session->tr, t, 0);
					segments = (lng) store->storage_api.count_del(m->session->tr, t, CNT_ACTIVE);

					for (node *nn = ol_first_node(t->columns); nn ; nn = nn->next) {
						c = (sql_column*) nn->data;

						if ((msg=mvc_insert_delta_values(m, col1, col2, col3, col4, col5, col6, col7, c, segments, deletes)) != NULL)
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
		*b1 = col1->batCacheid;
		BBPkeepref(col1);
		*b2 = col2->batCacheid;
		BBPkeepref(col2);
		*b3 = col3->batCacheid;
		BBPkeepref(col3);
		*b4 = col4->batCacheid;
		BBPkeepref(col4);
		*b5 = col5->batCacheid;
		BBPkeepref(col5);
		*b6 = col6->batCacheid;
		BBPkeepref(col6);
		*b7 = col7->batCacheid;
		BBPkeepref(col7);
	}
	return msg;
}

/* str mvc_bind_idxbat_wrap(int *bid, str *sname, str *tname, str *iname, int *access); */
str
mvc_bind_idxbat_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int upd = (pci->argc == 7 || pci->argc == 9);
	BAT *b = NULL;
	bat *bid = getArgReference_bat(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	const char *sname	= *getArgReference_str(stk, pci, 2 + upd);
	const char *tname	= *getArgReference_str(stk, pci, 3 + upd);
	const char *iname	= *getArgReference_str(stk, pci, 4 + upd);
	const int	access	= *getArgReference_int(stk, pci, 5 + upd);

	const bool partitioned_access = pci->argc == (8 + upd) && getArgType(mb, pci, 6 + upd) == TYPE_int;

	/* This doesn't work with quick access for now... */
	assert(access != QUICK);
	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	sqlstore *store = m->store;
	sql_schema *s = mvc_bind_schema(m, sname);
	sql_table *t = mvc_bind_table(m, s, tname);
	if (t && !isTable(t))
		throw(SQL, "sql.bindidx", SQLSTATE(42000) "%s '%s' is not persistent",
			  TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
	sql_idx *i = mvc_bind_idx(m, s, iname);

	if (partitioned_access) {
		/* partitioned access */
		int part_nr = *getArgReference_int(stk, pci, 6 + upd);
		int nr_parts = *getArgReference_int(stk, pci, 7 + upd);
		BUN cnt = store->storage_api.count_idx(m->session->tr, i, 0), psz;
		oid l, h;
		psz = cnt ? (cnt / nr_parts) : 0;
		l = part_nr * psz;
		if (l > cnt)
			l = cnt;
		h = (part_nr + 1 == nr_parts) ? cnt : ((part_nr + 1) * psz);
		if (h > cnt)
			h = cnt;

		if (upd) {
			BAT *ui = NULL, *uv = NULL;
			if (store->storage_api.bind_updates_idx(m->session->tr, i, &ui, &uv) == LOG_ERR)
				throw(SQL,"sql.bindidx",SQLSTATE(HY005) "Cannot access the update columns");

			h--;
			BAT* bn = BATselect(ui, NULL, &l, &h, true, true, false, false);
			if(bn == NULL) {
				BBPunfix(ui->batCacheid);
				BBPunfix(uv->batCacheid);
				throw(SQL, "sql.bindidx", GDK_EXCEPTION);
			}

			bat *uvl = getArgReference_bat(stk, pci, 1);

			if (BATcount(bn)) {
				BAT *id;
				BAT *vl;
				if (ui == NULL || uv == NULL) {
					bat_destroy(uv);
					bat_destroy(ui);
					BBPunfix(bn->batCacheid);
					throw(SQL,"sql.bindidx",SQLSTATE(42000) "Cannot access index column %s.%s.%s",sname,tname,iname);
				}
				assert(uv->batCount == ui->batCount);
				id = BATproject(bn, ui);
				vl = BATproject(bn, uv);
				bat_destroy(ui);
				bat_destroy(uv);
				if (id == NULL || vl == NULL) {
					BBPunfix(bn->batCacheid);
					bat_destroy(id);
					bat_destroy(vl);
					throw(SQL, "sql.bindidx", GDK_EXCEPTION);
				}
				if ( BATcount(id) != BATcount(vl)){
					BBPunfix(bn->batCacheid);
					bat_destroy(id);
					bat_destroy(vl);
					throw(SQL, "sql.bindidx", SQLSTATE(0000) "Inconsistent BAT count");
				}
				BBPkeepref(id);
				BBPkeepref(vl);
				*bid = id->batCacheid;
				*uvl = vl->batCacheid;
			} else {
				*bid = e_bat(TYPE_oid);
				*uvl = e_bat((i->type==join_idx)?TYPE_oid:TYPE_lng);
				if (*bid == BID_NIL || *uvl == BID_NIL) {
					if (*bid)
						BBPunfix(*bid);
					if (*uvl)
						BBPunfix(*uvl);
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.bindidx", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			}
		} else {
			int idxtype = getBatType(getArgType(mb, pci, 0));
			b = store->storage_api.bind_idx(m->session->tr, i, access);

			if (b && b->ttype && b->ttype != idxtype) {
				BBPunfix(b->batCacheid);
				throw(SQL,"sql.bindidx",SQLSTATE(42000) "Index type mismatch %s.%s.%s",sname,tname,iname);
			}

			BAT* bn = BATslice(b, l, h);
			if(bn == NULL) {
				BBPunfix(b->batCacheid);
				throw(SQL, "sql.bindidx", GDK_EXCEPTION);
			}
			BAThseqbase(bn, l);
			BBPunfix(b->batCacheid);
			BBPkeepref(bn);
			*bid = bn->batCacheid;
		}
	}
	else if (upd) { /*unpartitioned access to update bats*/
		BAT *ui = NULL, *uv = NULL;
		if (store->storage_api.bind_updates_idx(m->session->tr, i, &ui, &uv) == LOG_ERR)
			throw(SQL,"sql.bindidx",SQLSTATE(HY005) "Cannot access the update columns");

		bat *uvl = getArgReference_bat(stk, pci, 1);
		BBPkeepref(ui);
		BBPkeepref(uv);
		*bid = ui->batCacheid;
		*uvl = uv->batCacheid;
	}
	else { /*unpartitioned access to base index*/
		int idxtype = getBatType(getArgType(mb, pci, 0));
		b = store->storage_api.bind_idx(m->session->tr, i, access);
		if (b == NULL)
			throw(SQL,"sql.bindidx", "Couldn't bind index");

		if (b->ttype && b->ttype != idxtype) {
			BBPunfix(b->batCacheid);
			throw(SQL,"sql.bindidx",SQLSTATE(42000) "Index type mismatch %s.%s.%s",sname,tname,iname);
		}
		BBPkeepref(b);
		*bid = b->batCacheid;
	}
	return MAL_SUCCEED;
}

str
mvc_append_column(sql_trans *t, sql_column *c, BUN offset, BAT *pos, BAT *ins)
{
	sqlstore *store = t->store;
	int res = store->storage_api.append_col(t, c, offset, pos, ins, BATcount(ins), true, ins->ttype);
	if (res != LOG_OK) /* the conflict case should never happen, but leave it here */
		throw(SQL, "sql.append", SQLSTATE(42000) "Append failed %s", res == LOG_CONFLICT ? "due to conflict with another transaction" : GDKerrbuf);
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
	bool isbat = false;
	BAT *tid = 0, *ins = 0;
	size_t cnt = 1;
	oid v = 0;

	(void)cntxt;
	*res = 0;
	if ((tid = BATdescriptor(Tid)) == NULL)
		throw(SQL, "sql.grow", SQLSTATE(HY005) "Cannot access descriptor");
	if (isaBatType(tpe))
		isbat = true;
	if (isbat && (ins = BATdescriptor(*(bat *) Ins)) == NULL) {
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
			throw(SQL, "sql.grow", GDK_EXCEPTION);
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
	BUN offset = (BUN)*getArgReference_oid(stk, pci, 5);
	bat Pos = *getArgReference_bat(stk, pci, 6);
	ptr ins = getArgReference(stk, pci, 7);
	int tpe = getArgType(mb, pci, 7), log_res = LOG_OK;
	bool isbat = false;
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	sql_idx *i;
	BAT *b = NULL, *pos = NULL;
	BUN cnt = 1;

	*res = 0;
	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (isaBatType(tpe)) {
		isbat = true;
		tpe = getBatType(tpe);
	}
	if (Pos != bat_nil && (pos = BATdescriptor(Pos)) == NULL)
		throw(SQL, "sql.append", SQLSTATE(HY005) "Cannot access append positions descriptor");
	if (isbat && (ins = BATdescriptor(*(bat *) ins)) == NULL) {
		bat_destroy(pos);
		throw(SQL, "sql.append", SQLSTATE(HY005) "Cannot access append values descriptor");
	}
	if (!isbat && ATOMextern(tpe) && !ATOMvarsized(tpe))
		ins = *(ptr *) ins;
	if (isbat) {
		b =  (BAT*) ins;
		if (VIEWtparent(b) || VIEWvtparent(b)) {
			/* note, b == (BAT*)ins */
			b = COLcopy(b, b->ttype, true, TRANSIENT);
			BBPreclaim(ins);
			ins = b;
			if (b == NULL)
				throw(SQL, "sql.append", GDK_EXCEPTION);
		}
	}
	s = mvc_bind_schema(m, sname);
	if (s == NULL) {
		bat_destroy(pos);
		bat_destroy(b);
		throw(SQL, "sql.append", SQLSTATE(3F000) "Schema missing %s",sname);
	}
	t = mvc_bind_table(m, s, tname);
	if (t == NULL) {
		bat_destroy(pos);
		bat_destroy(b);
		throw(SQL, "sql.append", SQLSTATE(42S02) "Table missing %s",tname);
	}
	if (!isTable(t)) {
		bat_destroy(pos);
		bat_destroy(b);
		throw(SQL, "sql.append", SQLSTATE(42000) "%s '%s' is not persistent", TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
	}
	if (b)
		cnt = BATcount(b);
	sqlstore *store = m->session->tr->store;
	if (cname[0] != '%' && (c = mvc_bind_column(m, t, cname)) != NULL) {
		log_res = store->storage_api.append_col(m->session->tr, c, offset, pos, ins, cnt, isbat, tpe);
	} else if (cname[0] == '%' && (i = mvc_bind_idx(m, s, cname + 1)) != NULL) {
		log_res = store->storage_api.append_idx(m->session->tr, i, offset, pos, ins, cnt, isbat, tpe);
	} else {
		bat_destroy(pos);
		bat_destroy(b);
		throw(SQL, "sql.append", SQLSTATE(38000) "Unable to find column or index %s.%s.%s",sname,tname,cname);
	}
	bat_destroy(pos);
	bat_destroy(b);
	if (log_res != LOG_OK) /* the conflict case should never happen, but leave it here */
		throw(SQL, "sql.append", SQLSTATE(42000) "Append failed %s", log_res == LOG_CONFLICT ? "due to conflict with another transaction" : GDKerrbuf);
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
	int tpe = getArgType(mb, pci, 6), log_res = LOG_OK;
	bool isbat = false;
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	sql_idx *i;

	*res = 0;
	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (isaBatType(tpe))
		isbat = true;
	else
		assert(0);
	if (!isbat)
		throw(SQL, "sql.update", SQLSTATE(HY005) "Update values is not a BAT input");
	if ((tids = BATdescriptor(Tids)) == NULL)
		throw(SQL, "sql.update", SQLSTATE(HY005) "Cannot access update positions descriptor");
	if ((upd = BATdescriptor(Upd)) == NULL) {
		BBPunfix(tids->batCacheid);
		throw(SQL, "sql.update", SQLSTATE(HY005) "Cannot access update values descriptor");
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
	if (!isTable(t)) {
		BBPunfix(tids->batCacheid);
		BBPunfix(upd->batCacheid);
		throw(SQL, "sql.update", SQLSTATE(42000) "%s '%s' is not persistent", TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
	}
	sqlstore *store = m->session->tr->store;
	if (cname[0] != '%' && (c = mvc_bind_column(m, t, cname)) != NULL) {
		log_res = store->storage_api.update_col(m->session->tr, c, tids, upd, isbat);
	} else if (cname[0] == '%' && (i = mvc_bind_idx(m, s, cname + 1)) != NULL) {
		log_res = store->storage_api.update_idx(m->session->tr, i, tids, upd, isbat);
	} else {
		BBPunfix(tids->batCacheid);
		BBPunfix(upd->batCacheid);
		throw(SQL, "sql.update", SQLSTATE(38000) "Unable to find column or index %s.%s.%s",sname,tname,cname);
	}
	BBPunfix(tids->batCacheid);
	BBPunfix(upd->batCacheid);
	if (log_res != LOG_OK)
		throw(SQL, "sql.update", SQLSTATE(42000) "Update failed%s", log_res == LOG_CONFLICT ? " due to conflict with another transaction" : "");
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
	BUN clear_res;
	lng *res = getArgReference_lng(stk, pci, 0);
	const char *sname = *getArgReference_str(stk, pci, 1);
	const char *tname = *getArgReference_str(stk, pci, 2);
	int restart_sequences = *getArgReference_int(stk, pci, 3);

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
	if (!isTable(t))
		throw(SQL, "sql.clear_table", SQLSTATE(42000) "%s '%s' is not persistent", TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
	clear_res = mvc_clear_table(m, t);
	if (clear_res >= BUN_NONE - 1)
		throw(SQL, "sql.clear_table", SQLSTATE(42000) "Table clear failed%s", clear_res == (BUN_NONE - 1) ? " due to conflict with another transaction" : "");
	if (restart_sequences) { /* restart the sequences if it's the case */
		sql_trans *tr = m->session->tr;
		const char next_value_for[] = "next value for ";

		for (node *n = ol_first_node(t->columns); n; n = n->next) {
			sql_column *col = n->data;

			if (col->def && !strncmp(col->def, next_value_for, strlen(next_value_for))) {
				sql_schema *seqs = NULL;
				sql_sequence *seq = NULL;
				char *schema = NULL, *seq_name = NULL;

				extract_schema_and_sequence_name(m->ta, col->def + strlen(next_value_for), &schema, &seq_name);
				if (!schema || !seq_name || !(seqs = find_sql_schema(tr, schema)))
					continue;

				/* TODO - At the moment the sequence may not be stored in the same schema as the table itself */
				if ((seq = find_sql_sequence(tr, seqs, seq_name))) {
					switch (sql_trans_sequence_restart(tr, seq, seq->start)) {
						case -1:
							throw(SQL, "sql.clear_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						case -2:
						case -3:
							throw(SQL, "sql.clear_table", SQLSTATE(HY005) "RESTART SEQUENCE: transaction conflict detected");
						case -4:
							throw(SQL, "sql.clear_table", SQLSTATE(HY005) "Could not restart sequence %s.%s", seqs->base.name, seq_name);
						default:
							break;
					}
				}
			}
		}
	}
	*res = (lng) clear_res;
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
	int tpe = getArgType(mb, pci, 4), log_res;
	bool isbat = false;
	BAT *b = NULL;
	sql_schema *s;
	sql_table *t;

	*res = 0;
	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (isaBatType(tpe))
		isbat = true;
	if (isbat && (b = BATdescriptor(*(bat *) ins)) == NULL)
		throw(SQL, "sql.delete", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (!isbat || (b->ttype != TYPE_oid && b->ttype != TYPE_void && b->ttype != TYPE_msk)) {
		BBPreclaim(b);
		throw(SQL, "sql.delete", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	s = mvc_bind_schema(m, sname);
	if (s == NULL) {
		BBPreclaim(b);
		throw(SQL, "sql.delete", SQLSTATE(3F000) "Schema missing %s",sname);
	}
	t = mvc_bind_table(m, s, tname);
	if (t == NULL) {
		BBPreclaim(b);
		throw(SQL, "sql.delete", SQLSTATE(42S02) "Table missing %s.%s",sname,tname);
	}
	if (!isTable(t)) {
		BBPreclaim(b);
		throw(SQL, "sql.delete", SQLSTATE(42000) "%s '%s' is not persistent", TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
	}
	sqlstore *store = m->session->tr->store;
	log_res = store->storage_api.delete_tab(m->session->tr, t, b, isbat);
	BBPreclaim(b);
	if (log_res != LOG_OK)
		throw(SQL, "sql.delete", SQLSTATE(42000) "Delete failed%s", log_res == LOG_CONFLICT ? " due to conflict with another transaction" : "");
	return MAL_SUCCEED;
}

static inline BAT *
setwritable(BAT *b)
{
	if (isVIEW(b)) {
		BAT *bn = COLcopy(b, b->ttype, true, TRANSIENT);
		BBPunfix(b->batCacheid);
		b = bn;
	}
	return b;
}

str
DELTAbat(bat *result, const bat *col, const bat *uid, const bat *uval)
{
	BAT *c, *u_id, *u_val, *res;

	if ((u_id = BBPquickdesc(*uid)) == NULL)
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
		throw(MAL, "sql.delta", GDK_EXCEPTION);
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
		throw(MAL, "sql.delta", GDK_EXCEPTION);
	}
	BBPunfix(u_id->batCacheid);
	BBPunfix(u_val->batCacheid);

	*result = res->batCacheid;
	BBPkeepref(res);
	return MAL_SUCCEED;
}

str
DELTAsub(bat *result, const bat *col, const bat *cid, const bat *uid, const bat *uval)
{
	BAT *c, *cminu = NULL, *u_id, *u_val, *u, *res;
	gdk_return ret;

	if ((u_id = BBPquickdesc(*uid)) == NULL)
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
			throw(MAL, "sql.delta", GDK_EXCEPTION);
		}
		res = BATproject(cminu, c);
		BBPunfix(c->batCacheid);
		BBPunfix(cminu->batCacheid);
		cminu = NULL;
		if (!res) {
			BBPunfix(u_id->batCacheid);
			throw(MAL, "sql.delta", GDK_EXCEPTION);
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
				throw(MAL, "sql.delta", GDK_EXCEPTION);
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
				throw(MAL, "sql.delta", GDK_EXCEPTION);
			}
			BAT *nres;
			if ((nres = COLcopy(res, res->ttype, true, TRANSIENT)) == NULL) {
				BBPunfix(res->batCacheid);
				BBPunfix(u->batCacheid);
				BBPunfix(cminu->batCacheid);
				throw(MAL, "sql.delta", GDK_EXCEPTION);
			}
			BBPunfix(res->batCacheid);
			res = nres;
			ret = BATappend(res, u, cminu, true);
			BBPunfix(u->batCacheid);
			BBPunfix(cminu->batCacheid);
			cminu = NULL;
			if (ret != GDK_SUCCEED) {
				BBPunfix(res->batCacheid);
				throw(MAL, "sql.delta", GDK_EXCEPTION);
			}

			ret = BATsort(&u, NULL, NULL, res, NULL, NULL, false, false, false);
			BBPunfix(res->batCacheid);
			if (ret != GDK_SUCCEED) {
				throw(MAL, "sql.delta", GDK_EXCEPTION);
			}
			res = u;
		} else {
			BBPunfix(u_val->batCacheid);
			BBPunfix(u_id->batCacheid);
		}
	}

	BATkey(res, true);
	*result = res->batCacheid;
	BBPkeepref(res);
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
		throw(MAL, "sql.projectdelta", GDK_EXCEPTION);
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
		*result = res->batCacheid;
		BBPkeepref(res);
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
			throw(MAL, "sql.delta", GDK_EXCEPTION);
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
			if (!nu_val || (res = setwritable(res)) == NULL ||
			    BATreplace(res, os, nu_val, false) != GDK_SUCCEED) {
				BBPreclaim(res);
				BBPunfix(os->batCacheid);
				BBPunfix(s->batCacheid);
				BBPunfix(u_id->batCacheid);
				BBPunfix(u_val->batCacheid);
				BBPreclaim(nu_val);
				throw(MAL, "sql.delta", GDK_EXCEPTION);
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

	*result = res->batCacheid;
	BBPkeepref(res);
	return MAL_SUCCEED;
}

str
BATleftproject(bat *Res, const bat *Col, const bat *L, const bat *R)
{
	BAT *c, *l, *r, *res;
	oid *p, *lp, *rp;
	BUN cnt = 0, i;
	BATiter li, ri;

	c = BATdescriptor(*Col);
	if (c)
		cnt = BATcount(c);
	l = BATdescriptor(*L);
	r = BATdescriptor(*R);
	res = COLnew(0, TYPE_oid, cnt, TRANSIENT);
	if (!c || !l || !r || !res) {
		BBPreclaim(c);
		BBPreclaim(l);
		BBPreclaim(r);
		BBPreclaim(res);
		throw(MAL, "sql.delta", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	p = (oid*)Tloc(res,0);
	for(i=0;i<cnt; i++)
		*p++ = oid_nil;
	BATsetcount(res, cnt);

	cnt = BATcount(l);
	p = (oid*)Tloc(res, 0);
	li = bat_iterator(l);
	ri = bat_iterator(r);
	lp = (oid*)li.base;
	rp = (oid*)ri.base;
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
	bat_iterator_end(&li);
	bat_iterator_end(&ri);
	res->tsorted = false;
	res->trevsorted = false;
	res->tnil = false;
	res->tnonil = false;
	res->tkey = false;
	BBPunfix(c->batCacheid);
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	*Res = res->batCacheid;
	BBPkeepref(res);
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
	if (!isTable(t))
		throw(SQL, "sql.tid", SQLSTATE(42000) "%s '%s' is not persistent",
			  TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);

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
		BBPkeepref(b);
	} else {
		msg = createException(SQL, "sql.tid", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
	int i, res, ok;
	const char *tblname, *colname, *tpename;
	str msg= MAL_SUCCEED;
	int *digits, *scaledigits;
	oid o = 0;
	BATiter itertbl,iteratr,itertpe,iterdig,iterscl;
	backend *be = NULL;
	BAT *b = NULL, *tbl = NULL, *atr = NULL, *tpe = NULL,*len = NULL,*scale = NULL;

	if ((msg = getBackendContext(cntxt, &be)) != NULL)
		return msg;
	bid = *getArgReference_bat(stk,pci,6);
	b = BATdescriptor(bid);
	if ( b == NULL) {
		msg = createException(SQL, "sql.resultSet", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto wrapup_result_set;
	}
	res = *res_id = mvc_result_table(be, mb->tag, pci->argc - (pci->retc + 5), Q_TABLE);
	BBPunfix(b->batCacheid);
	if (res < 0) {
		msg = createException(SQL, "sql.resultSet", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto wrapup_result_set;
	}

	tbl = BATdescriptor(tblId);
	atr = BATdescriptor(atrId);
	tpe = BATdescriptor(tpeId);
	len = BATdescriptor(lenId);
	scale = BATdescriptor(scaleId);
	if (tbl == NULL || atr == NULL || tpe == NULL || len == NULL || scale == NULL)
		goto wrapup_result_set;
	/* mimic the old rsColumn approach; */
	itertbl = bat_iterator(tbl);
	iteratr = bat_iterator(atr);
	itertpe = bat_iterator(tpe);
	iterdig = bat_iterator(len);
	iterscl = bat_iterator(scale);
	digits = (int*) iterdig.base;
	scaledigits = (int*) iterscl.base;

	for( i = 6; msg == MAL_SUCCEED && i< pci->argc; i++, o++){
		bid = *getArgReference_bat(stk,pci,i);
		tblname = BUNtvar(itertbl,o);
		colname = BUNtvar(iteratr,o);
		tpename = BUNtvar(itertpe,o);
		b = BATdescriptor(bid);
		if ( b == NULL)
			msg = createException(SQL, "sql.resultSet", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		else if (mvc_result_column(be, tblname, colname, tpename, *digits++, *scaledigits++, b))
			msg = createException(SQL, "sql.resultSet", SQLSTATE(42000) "Cannot access column descriptor %s.%s",tblname,colname);
		if( b)
			BBPunfix(bid);
	}
	bat_iterator_end(&itertbl);
	bat_iterator_end(&iteratr);
	bat_iterator_end(&itertpe);
	bat_iterator_end(&iterdig);
	bat_iterator_end(&iterscl);
	/* now send it to the channel cntxt->fdout */
	if (bstream_getoob(cntxt->fdin))
		msg = createException(SQL, "sql.resultSet", SQLSTATE(HY000) "Query aboted");
	else if (!msg && (ok = mvc_export_result(be, cntxt->fdout, res, true, cntxt->qryctx.starttime, mb->optimize)) < 0)
		msg = createException(SQL, "sql.resultSet", SQLSTATE(45000) "Result set construction failed: %s", mvc_export_error(be, cntxt->fdout, ok));
  wrapup_result_set:
	cntxt->qryctx.starttime = 0;
	cntxt->qryctx.endtime = 0;
	mb->optimize = 0;
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
	stream *s = NULL;
	bat bid;
	int i, res, ok;
	const char *tblname, *colname, *tpename;
	str msg= MAL_SUCCEED;
	int *digits, *scaledigits;
	oid o = 0;
	BATiter itertbl,iteratr,itertpe,iterdig,iterscl;
	backend *be;
	mvc *m = NULL;
	BAT *b = NULL, *tbl = NULL, *atr = NULL, *tpe = NULL,*len = NULL,*scale = NULL;
	res_table *t = NULL;
	bool tostdout;
	char buf[80];
	ssize_t sz;

	(void) format;

	if ((msg = getBackendContext(cntxt, &be)) != NULL)
		return msg;
	m = be->mvc;

	if (onclient && !cntxt->filetrans) {
		msg = createException(SQL, "sql.resultSet", SQLSTATE(42000) "Cannot transfer files to client");
		goto wrapup_result_set1;
	}

	bid = *getArgReference_bat(stk,pci,13);
	res = *res_id = mvc_result_table(be, mb->tag, pci->argc - (pci->retc + 12), Q_TABLE);
	t = be->results;
	if (res < 0) {
		msg = createException(SQL, "sql.resultSet", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
	/* mimic the old rsColumn approach; */
	itertbl = bat_iterator(tbl);
	iteratr = bat_iterator(atr);
	itertpe = bat_iterator(tpe);
	iterdig = bat_iterator(len);
	iterscl = bat_iterator(scale);
	digits = (int*) iterdig.base;
	scaledigits = (int*) iterscl.base;

	for( i = 13; msg == MAL_SUCCEED && i< pci->argc; i++, o++){
		bid = *getArgReference_bat(stk,pci,i);
		tblname = BUNtvar(itertbl,o);
		colname = BUNtvar(iteratr,o);
		tpename = BUNtvar(itertpe,o);
		b = BATdescriptor(bid);
		if ( b == NULL)
			msg = createException(SQL, "sql.resultSet", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		else if (mvc_result_column(be, tblname, colname, tpename, *digits++, *scaledigits++, b))
			msg = createException(SQL, "sql.resultSet", SQLSTATE(42000) "Cannot access column descriptor %s.%s",tblname,colname);
		if( b)
			BBPunfix(bid);
	}
	bat_iterator_end(&itertbl);
	bat_iterator_end(&iteratr);
	bat_iterator_end(&itertpe);
	bat_iterator_end(&iterdig);
	bat_iterator_end(&iterscl);
	if ( msg )
		goto wrapup_result_set1;

	/* now select the file channel */
	if ((tostdout = strcmp(filename,"stdout") == 0)) {
		s = cntxt->fdout;
	} else if (!onclient) {
		if ((s = open_wastream(filename)) == NULL || mnstr_errnr(s) != MNSTR_NO__ERROR) {
			msg=  createException(IO, "streams.open", SQLSTATE(42000) "%s", mnstr_peek_error(NULL));
			close_stream(s);
			goto wrapup_result_set1;
		}
		be->output_format = OFMT_CSV;
	} else {
		while (!m->scanner.rs->eof) {
			if (bstream_next(m->scanner.rs) < 0) {
				msg = createException(IO, "streams.open", "interrupted");
				goto wrapup_result_set1;
			}
		}
		s = m->scanner.ws;
		mnstr_write(s, PROMPT3, sizeof(PROMPT3) - 1, 1);
		mnstr_printf(s, "w %s\n", filename);
		mnstr_flush(s, MNSTR_FLUSH_DATA);
		if ((sz = mnstr_readline(m->scanner.rs->s, buf, sizeof(buf))) > 1) {
			/* non-empty line indicates failure on client */
			msg = createException(IO, "streams.open", "%s", buf);
			/* discard until client flushes */
			while (mnstr_read(m->scanner.rs->s, buf, 1, sizeof(buf)) > 0) {
				/* ignore remainder of error message */
			}
			goto wrapup_result_set1;
		}
	}
	if ((ok = mvc_export_result(cntxt->sqlcontext, s, res, tostdout, cntxt->qryctx.starttime, mb->optimize)) < 0) {
		msg = createException(SQL, "sql.resultSet", SQLSTATE(45000) "Result set construction failed: %s", mvc_export_error(cntxt->sqlcontext, s, ok));
		if (!onclient && !tostdout)
			close_stream(s);
		if (ok != -5)
			goto wrapup_result_set1;
	}
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
	cntxt->qryctx.starttime = 0;
	cntxt->qryctx.endtime = 0;
	mb->optimize = 0;
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
	int i, res, ok;
	const char *tblname, *colname, *tpename;
	str msg= MAL_SUCCEED;
	int *digits, *scaledigits;
	oid o = 0;
	BATiter itertbl,iteratr,itertpe,iterdig,iterscl;
	backend *be = NULL;
	ptr v;
	int mtype;
	BAT *tbl = NULL, *atr = NULL, *tpe = NULL, *len = NULL, *scale = NULL;

	if ((msg = getBackendContext(cntxt, &be)) != NULL)
		return msg;
	res = *res_id = mvc_result_table(be, mb->tag, pci->argc - (pci->retc + 5), Q_TABLE);
	if (res < 0) {
		msg = createException(SQL, "sql.resultSet", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto wrapup_result_set;
	}

	tbl = BATdescriptor(tblId);
	atr = BATdescriptor(atrId);
	tpe = BATdescriptor(tpeId);
	len = BATdescriptor(lenId);
	scale = BATdescriptor(scaleId);
	if( tbl == NULL || atr == NULL || tpe == NULL || len == NULL || scale == NULL)
		goto wrapup_result_set;
	/* mimic the old rsColumn approach; */
	itertbl = bat_iterator(tbl);
	iteratr = bat_iterator(atr);
	itertpe = bat_iterator(tpe);
	iterdig = bat_iterator(len);
	iterscl = bat_iterator(scale);
	digits = (int*) iterdig.base;
	scaledigits = (int*) iterscl.base;

	for( i = 6; msg == MAL_SUCCEED && i< pci->argc; i++, o++){
		tblname = BUNtvar(itertbl,o);
		colname = BUNtvar(iteratr,o);
		tpename = BUNtvar(itertpe,o);

		v = getArgReference(stk, pci, i);
		mtype = getArgType(mb, pci, i);
		if (ATOMextern(mtype))
			v = *(ptr *) v;
		if ((ok = mvc_result_value(be, tblname, colname, tpename, *digits++, *scaledigits++, v, mtype) < 0)) {
			msg = createException(SQL, "sql.rsColumn", SQLSTATE(45000) "Result set construction failed: %s", mvc_export_error(be, be->out, ok));
			bat_iterator_end(&itertbl);
			bat_iterator_end(&iteratr);
			bat_iterator_end(&itertpe);
			bat_iterator_end(&iterdig);
			bat_iterator_end(&iterscl);
			goto wrapup_result_set;
		}
	}
	bat_iterator_end(&itertbl);
	bat_iterator_end(&iteratr);
	bat_iterator_end(&itertpe);
	bat_iterator_end(&iterdig);
	bat_iterator_end(&iterscl);
	if (!msg && (ok = mvc_export_result(cntxt->sqlcontext, cntxt->fdout, res, true, cntxt->qryctx.starttime, mb->optimize)) < 0)
		msg = createException(SQL, "sql.resultSet", SQLSTATE(45000) "Result set construction failed: %s", mvc_export_error(cntxt->sqlcontext, cntxt->fdout, ok));
  wrapup_result_set:
	cntxt->qryctx.starttime = 0;
	cntxt->qryctx.endtime = 0;
	mb->optimize = 0;
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

	int i, res, ok;
	stream *s = NULL;
	const char *tblname, *colname, *tpename;
	str msg = MAL_SUCCEED;
	int *digits, *scaledigits;
	oid o = 0;
	BATiter itertbl,iteratr,itertpe,iterdig,iterscl;
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
		msg = createException(SQL, "sql.resultSet", SQLSTATE(42000) "Cannot transfer files to client");
		goto wrapup_result_set;
	}

	res = *res_id = mvc_result_table(be, mb->tag, pci->argc - (pci->retc + 12), Q_TABLE);

	t = be->results;
	if (res < 0){
		msg = createException(SQL, "sql.resultSet", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
	if (tbl == NULL || atr == NULL || tpe == NULL || len == NULL || scale == NULL)
		goto wrapup_result_set;
	/* mimic the old rsColumn approach; */
	itertbl = bat_iterator(tbl);
	iteratr = bat_iterator(atr);
	itertpe = bat_iterator(tpe);
	iterdig = bat_iterator(len);
	iterscl = bat_iterator(scale);
	digits = (int*) iterdig.base;
	scaledigits = (int*) iterscl.base;

	for( i = 13; msg == MAL_SUCCEED && i< pci->argc; i++, o++){
		tblname = BUNtvar(itertbl,o);
		colname = BUNtvar(iteratr,o);
		tpename = BUNtvar(itertpe,o);

		v = getArgReference(stk, pci, i);
		mtype = getArgType(mb, pci, i);
		if (ATOMextern(mtype))
			v = *(ptr *) v;
		if ((ok = mvc_result_value(be, tblname, colname, tpename, *digits++, *scaledigits++, v, mtype)) < 0) {
			msg = createException(SQL, "sql.rsColumn", SQLSTATE(45000) "Result set construction failed: %s", mvc_export_error(be, s, ok));
			bat_iterator_end(&itertbl);
			bat_iterator_end(&iteratr);
			bat_iterator_end(&itertpe);
			bat_iterator_end(&iterdig);
			bat_iterator_end(&iterscl);
			goto wrapup_result_set;
		}
	}
	bat_iterator_end(&itertbl);
	bat_iterator_end(&iteratr);
	bat_iterator_end(&itertpe);
	bat_iterator_end(&iterdig);
	bat_iterator_end(&iterscl);
	/* now select the file channel */
	if ((tostdout = strcmp(filename,"stdout") == 0)) {
		s = cntxt->fdout;
	} else if (!onclient) {
		if ((s = open_wastream(filename)) == NULL || mnstr_errnr(s) != MNSTR_NO__ERROR) {
			msg=  createException(IO, "streams.open", SQLSTATE(42000) "%s", mnstr_peek_error(NULL));
			close_stream(s);
			goto wrapup_result_set;
		}
	} else {
		while (!m->scanner.rs->eof) {
			if (bstream_next(m->scanner.rs) < 0) {
				msg = createException(IO, "streams.open", "interrupted");
				goto wrapup_result_set;
			}
		}
		s = m->scanner.ws;
		mnstr_write(s, PROMPT3, sizeof(PROMPT3) - 1, 1);
		mnstr_printf(s, "w %s\n", filename);
		mnstr_flush(s, MNSTR_FLUSH_DATA);
		if ((sz = mnstr_readline(m->scanner.rs->s, buf, sizeof(buf))) > 1) {
			/* non-empty line indicates failure on client */
			msg = createException(IO, "streams.open", "%s", buf);
			/* discard until client flushes */
			while (mnstr_read(m->scanner.rs->s, buf, 1, sizeof(buf)) > 0) {
				/* ignore remainder of error message */
			}
			goto wrapup_result_set;
		}
	}
	if ((ok = mvc_export_result(cntxt->sqlcontext, s, res, strcmp(filename, "stdout") == 0, cntxt->qryctx.starttime, mb->optimize)) < 0) {
		msg = createException(SQL, "sql.resultSet", SQLSTATE(45000) "Result set construction failed: %s", mvc_export_error(cntxt->sqlcontext, s, ok));
		if (!onclient && !tostdout)
			close_stream(s);
		goto wrapup_result_set;
	}
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
	cntxt->qryctx.starttime = 0;
	cntxt->qryctx.endtime = 0;
	mb->optimize = 0;
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
	backend *be = NULL;
	str msg;
	int *res_id;
	int nr_cols;
	mapi_query_t qtype;

	if ( pci->argc > 6)
		return mvc_result_set_wrap(cntxt,mb,stk,pci);

	assert(0);
	res_id = getArgReference_int(stk, pci, 0);
	nr_cols = *getArgReference_int(stk, pci, 1);
	qtype = (mapi_query_t) *getArgReference_int(stk, pci, 2);
	bat order_bid = *getArgReference_bat(stk, pci, 3);
	(void)order_bid;
	/* TODO remove use */

	if ((msg = getBackendContext(cntxt, &be)) != NULL)
		return msg;
	*res_id = mvc_result_table(be, mb->tag, nr_cols, qtype);
	if (*res_id < 0)
		res = createException(SQL, "sql.resultSet", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return res;
}

/* str mvc_affected_rows_wrap(int *m, int m, lng *nr, str *w); */
str
mvc_affected_rows_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *b = NULL;
	int *res = getArgReference_int(stk, pci, 0), ok;
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
	ok = mvc_export_affrows(b, b->out, nr, "", mb->tag, cntxt->qryctx.starttime, mb->optimize);
	cntxt->qryctx.starttime = 0;
	cntxt->qryctx.endtime = 0;
	mb->optimize = 0;
	if (ok < 0)
		throw(SQL, "sql.affectedRows", SQLSTATE(45000) "Result set construction failed: %s", mvc_export_error(b, b->out, ok));
	return MAL_SUCCEED;
}

/* str mvc_export_head_wrap(int *ret, stream **s, int *res_id); */
str
mvc_export_head_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *b = NULL;
	stream **s = (stream **) getArgReference(stk, pci, 1);
	int res_id = *getArgReference_int(stk, pci, 2), ok;
	str msg;

	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = cntxt->sqlcontext;
	ok = mvc_export_head(b, *s, res_id, FALSE, TRUE, cntxt->qryctx.starttime, mb->optimize);
	cntxt->qryctx.starttime = 0;
	cntxt->qryctx.endtime = 0;
	mb->optimize = 0;
	if (ok < 0)
		throw(SQL, "sql.exportHead", SQLSTATE(45000) "Result set construction failed: %s", mvc_export_error(b, *s, ok));
	return MAL_SUCCEED;
}

/* str mvc_export_result_wrap(int *ret, stream **s, int *res_id); */
str
mvc_export_result_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *b = NULL;
	stream **s = (stream **) getArgReference(stk, pci, 1), *sout;
	int res_id = *getArgReference_int(stk, pci, 2), ok;
	str msg;

	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = cntxt->sqlcontext;
	sout = pci->argc > 5 ? cntxt->fdout : *s;
	ok = mvc_export_result(b, sout, res_id, false, cntxt->qryctx.starttime, mb->optimize);
	cntxt->qryctx.starttime = 0;
	cntxt->qryctx.endtime = 0;
	mb->optimize = 0;
	if (ok < 0)
		throw(SQL, "sql.exportResult", SQLSTATE(45000) "Result set construction failed: %s", mvc_export_error(b, sout, ok));
	return MAL_SUCCEED;
}

/* str mvc_export_chunk_wrap(int *ret, stream **s, int *res_id, str *w); */
str
mvc_export_chunk_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *b = NULL;
	stream **s = (stream **) getArgReference(stk, pci, 1);
	int res_id = *getArgReference_int(stk, pci, 2), ok;
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
	if ((ok = mvc_export_chunk(b, *s, res_id, offset, nr)) < 0)
		throw(SQL, "sql.exportChunk", SQLSTATE(45000) "Result set construction failed: %s", mvc_export_error(b, *s, ok));
	return NULL;
}

/* str mvc_export_operation_wrap(int *ret, str *w); */
str
mvc_export_operation_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *b = NULL;
	str msg;
	int ok = 0;

	(void) stk;		/* NOT USED */
	(void) pci;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = cntxt->sqlcontext;
	if (b->out)
		ok = mvc_export_operation(b, b->out, "", cntxt->qryctx.starttime, mb->optimize);
	cntxt->qryctx.starttime = 0;
	cntxt->qryctx.endtime = 0;
	mb->optimize = 0;
	if (ok < 0)
		throw(SQL, "sql.exportOperation", SQLSTATE(45000) "Result set construction failed: %s", mvc_export_error(b, b->out, ok));
	return MAL_SUCCEED;
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
	int res_id, ok;
	(void) mb;		/* NOT USED */
	if ((msg = getBackendContext(cntxt, &be)) != NULL)
		return msg;
	if (ATOMextern(mtype))
		p = *(ptr *) p;

	// scalar values are single-column result sets
	if ((res_id = mvc_result_table(be, mb->tag, 1, Q_TABLE)) < 0) {
		cntxt->qryctx.starttime = 0;
		cntxt->qryctx.endtime = 0;
		mb->optimize = 0;
		throw(SQL, "sql.exportValue", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if ((ok = mvc_result_value(be, tn, cn, type, digits, scale, p, mtype)) < 0) {
		cntxt->qryctx.starttime = 0;
		cntxt->qryctx.endtime = 0;
		mb->optimize = 0;
		throw(SQL, "sql.exportValue", SQLSTATE(45000) "Result set construction failed: %s", mvc_export_error(be, be->out, ok));
	}
	if (be->output_format == OFMT_NONE) {
		cntxt->qryctx.starttime = 0;
		cntxt->qryctx.endtime = 0;
		mb->optimize = 0;
		return MAL_SUCCEED;
	}
	ok = mvc_export_result(be, be->out, res_id, true, cntxt->qryctx.starttime, mb->optimize);
	cntxt->qryctx.starttime = 0;
	cntxt->qryctx.endtime = 0;
	mb->optimize = 0;
	if (ok < 0)
		throw(SQL, "sql.exportValue", SQLSTATE(45000) "Result set construction failed: %s", mvc_export_error(be, be->out, ok));
	return MAL_SUCCEED;
}

static void
bat2return(MalStkPtr stk, InstrPtr pci, BAT **b)
{
	int i;

	for (i = 0; i < pci->retc; i++) {
		*getArgReference_bat(stk, pci, i) = b[i]->batCacheid;
		BBPkeepref(b[i]);
	}
}

static const char fwftsep[2] = {STREAM_FWF_FIELD_SEP, '\0'};
static const char fwfrsep[2] = {STREAM_FWF_RECORD_SEP, '\0'};

/* str mvc_import_table_wrap(int *res, sql_table **t, unsigned char* *T, unsigned char* *R, unsigned char* *S, unsigned char* *N, str *fname, lng *sz, lng *offset, int *besteffort, str *fixed_width, int *onclient, int *escape); */
str
mvc_import_table_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *be;
	BAT **b = NULL;
	sql_table *t = *(sql_table **) getArgReference(stk, pci, pci->retc + 0);
	const char *tsep = *getArgReference_str(stk, pci, pci->retc + 1);
	const char *rsep = *getArgReference_str(stk, pci, pci->retc + 2);
	const char *ssep = *getArgReference_str(stk, pci, pci->retc + 3);
	const char *ns = *getArgReference_str(stk, pci, pci->retc + 4);
	const char *fname = *getArgReference_str(stk, pci, pci->retc + 5);
	lng sz = *getArgReference_lng(stk, pci, pci->retc + 6);
	lng offset = *getArgReference_lng(stk, pci, pci->retc + 7);
	int besteffort = *getArgReference_int(stk, pci, pci->retc + 8);
	const char *fixed_widths = *getArgReference_str(stk, pci, pci->retc + 9);
	int onclient = *getArgReference_int(stk, pci, pci->retc + 10);
	bool escape = *getArgReference_int(stk, pci, pci->retc + 11);
	const char *decsep = *getArgReference_str(stk, pci, pci->retc + 12);
	const char *decskip = *getArgReference_str(stk, pci, pci->retc + 13);
	str msg = MAL_SUCCEED;
	bstream *s = NULL;
	stream *ss;

	(void) mb;		/* NOT USED */
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (onclient && !cntxt->filetrans)
		throw(MAL, "sql.copy_from", SQLSTATE(42000) "Cannot transfer files from client");
	if (strNil(decsep))
		throw(MAL, "sql.copy_from", SQLSTATE(42000) "decimal separator cannot be nil");
	if (strNil(decskip))
		decskip = NULL;

	be = cntxt->sqlcontext;
	/* The CSV parser expects ssep to have the value 0 if the user does not
	 * specify a quotation character
	 */
	if (*ssep == 0 || strNil(ssep))
		ssep = NULL;

	if (strNil(fname))
		fname = NULL;
	if (fname == NULL) {
		msg = mvc_import_table(cntxt, &b, be->mvc, be->mvc->scanner.rs, t, tsep, rsep, ssep, ns, sz, offset, besteffort, true, escape, decsep, decskip);
	} else {
		if (onclient) {
			ss = mapi_request_upload(fname, false, be->mvc->scanner.rs, be->mvc->scanner.ws);
		} else {
			ss = open_rastream(fname);
		}
		if (ss == NULL || mnstr_errnr(ss) != MNSTR_NO__ERROR) {
			msg = createException(IO, "sql.copy_from", SQLSTATE(42000) "%s", mnstr_peek_error(NULL));
			close_stream(ss);
			return msg;
		}

		if (!strNil(fixed_widths)) {
			size_t ncol = 0, current_width_entry = 0, i;
			size_t *widths;
			const char* val_start = fixed_widths;
			size_t width_len = strlen(fixed_widths);
			stream *ns;

			for (i = 0; i < width_len; i++) {
				if (fixed_widths[i] == STREAM_FWF_FIELD_SEP) {
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
					widths[current_width_entry++] = (size_t) strtoll(val_start, NULL, 10);
					val_start = fixed_widths + i + 1;
				}
			}
			/* overwrite other delimiters to the ones the FWF stream uses */
			tsep = fwftsep;
			rsep = fwfrsep;

			ns = stream_fwf_create(ss, ncol, widths, STREAM_FWF_FILLER);
			if (ns == NULL || mnstr_errnr(ns) != MNSTR_NO__ERROR) {
				msg = createException(IO, "sql.copy_from", SQLSTATE(42000) "%s", mnstr_peek_error(NULL));
				close_stream(ss);
				free(widths);
				return msg;
			}
			ss = ns;
		}
#if SIZEOF_VOID_P == 4
		s = bstream_create(ss, 0x20000);
#else
		s = bstream_create(ss, 0x200000);
#endif
		if (s == NULL) {
			close_stream(ss);
			throw(MAL, "sql.copy_from", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		msg = mvc_import_table(cntxt, &b, be->mvc, s, t, tsep, rsep, ssep, ns, sz, offset, besteffort, false, escape, decsep, decskip);
		// This also closes ss:
		bstream_destroy(s);
	}
	if (b && !msg)
		bat2return(stk, pci, b);
	GDKfree(b);
	return msg;
}

str
not_unique(bit *ret, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "not_unique", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	*ret = FALSE;
	BATiter bi = bat_iterator(b);
	if (bi.key || BATtdensebi(&bi) || bi.count <= 1) {
		bat_iterator_end(&bi);
		BBPunfix(b->batCacheid);
		return MAL_SUCCEED;
	} else if (bi.sorted) {
		BUN p;
		oid c = ((oid *) bi.base)[0];

		for (p = 1; p < bi.count; p++) {
			oid v = ((oid *) bi.base)[p];
			if (v <= c) {
				*ret = TRUE;
				break;
			}
			c = v;
		}
	} else {
		bat_iterator_end(&bi);
		BBPunfix(b->batCacheid);
		throw(SQL, "not_unique", SQLSTATE(42000) "Input column should be sorted");
	}
	bat_iterator_end(&bi);
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
	if (!(b = BBPquickdesc(bid)))
		throw(MAL, "batcalc.identity", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (!(bn = BATdense(b->hseqbase, s, BATcount(b))))
		throw(MAL, "batcalc.identity", GDK_EXCEPTION);
	*ns = s + BATcount(b);
	*res = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
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
		throw(SQL, "alpha", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn = COLnew(b->hseqbase, TYPE_dbl, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.alpha", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	s = sin(radians(*theta));
	BATiter bi = bat_iterator(b);
	const dbl *vals = (const dbl *) bi.base;
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
			bat_iterator_end(&bi);
			BBPunfix(b->batCacheid);
			throw(SQL, "sql.alpha", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	bat_iterator_end(&bi);
	*res = bn->batCacheid;
	BBPkeepref(bn);
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
		throw(SQL, "alpha", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn = COLnew(b->hseqbase, TYPE_dbl, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.alpha", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATiter bi = bat_iterator(b);
	thetas = (dbl *) bi.base;
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
			bat_iterator_end(&bi);
			BBPunfix(b->batCacheid);
			throw(SQL, "sql.alpha", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	bat_iterator_end(&bi);
	*res = bn->batCacheid;
	BBPkeepref(bn);
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
	BBPkeepref(query);
	BBPkeepref(count);
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
	BBPkeepref(rewrite);
	BBPkeepref(count);
	return MAL_SUCCEED;
}

/* str dump_opt_stats(int *r); */
str
dump_trace(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	BAT *t[3];

	(void) cntxt;
	(void) mb;
	if (TRACEtable(cntxt, t) != 3)
		throw(SQL, "sql.dump_trace", SQLSTATE(3F000) "Profiler not started");
	for (i = 0; i < 3; i++) {
		*getArgReference_bat(stk, pci, i) = t[i]->batCacheid;
		BBPkeepref(t[i]);
	}
	return MAL_SUCCEED;
}

static str
sql_unclosed_result_sets(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)mb;
	bat *ret_query_id = getArgReference_bat(stk, pci, 0);
	bat *ret_res_id = getArgReference_bat(stk, pci, 1);
	backend *be = cntxt->sqlcontext;

	BUN count = 0;
	for (res_table *p = be->results; p != NULL; p = p->next)
		count++;

	BAT *query_ids = COLnew(0, TYPE_oid, count, TRANSIENT);
	BAT *res_ids = COLnew(0, TYPE_int, count, TRANSIENT);

	if (query_ids == NULL || res_ids == NULL) {
		BBPreclaim(query_ids);
		BBPreclaim(res_ids);
		throw(SQL, "sql.sql_unclosed_result_sets", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	for (res_table *p = be->results; p != NULL; p = p->next) {
		if (BUNappend(query_ids, &p->query_id, false) != GDK_SUCCEED)
			goto bailout;
		if (BUNappend(res_ids, &p->id, false) != GDK_SUCCEED)
			goto bailout;
	}

	*ret_query_id = query_ids->batCacheid;
	BBPkeepref(query_ids);
	*ret_res_id = res_ids->batCacheid;
	BBPkeepref(res_ids);

	return MAL_SUCCEED;

bailout:
	BBPunfix(query_ids->batCacheid);
	BBPunfix(res_ids->batCacheid);
	throw(SQL, "sql.sql_unclosed_result_sets", SQLSTATE(42000)"failed to retrieve result tables");
}

static str
sql_sessions_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *id = NULL, *user = NULL, *login = NULL, *sessiontimeout = NULL,
		*querytimeout = NULL, *idle = NULL;
	BAT *opt = NULL, *wlimit = NULL, *mlimit = NULL;
	BAT *language = NULL, *peer = NULL, *hostname = NULL, *application = NULL, *client = NULL, *clientpid = NULL, *remark = NULL;
	bat *idId = getArgReference_bat(stk, pci, 0);
	bat *userId = getArgReference_bat(stk, pci, 1);
	bat *loginId = getArgReference_bat(stk, pci, 2);
	bat *idleId = getArgReference_bat(stk, pci, 3);
	bat *optId = getArgReference_bat(stk, pci, 4);
	bat *sessiontimeoutId = getArgReference_bat(stk, pci, 5);
	bat *querytimeoutId = getArgReference_bat(stk, pci, 6);
	bat *wlimitId = getArgReference_bat(stk, pci, 7);
	bat *mlimitId = getArgReference_bat(stk, pci, 8);
	bat *languageId = getArgReference_bat(stk, pci, 9);
	bat *peerId = getArgReference_bat(stk, pci, 10);
	bat *hostnameId = getArgReference_bat(stk, pci, 11);
	bat *applicationId = getArgReference_bat(stk, pci, 12);
	bat *clientId = getArgReference_bat(stk, pci, 13);
	bat *clientpidId = getArgReference_bat(stk, pci, 14);
	bat *remarkId = getArgReference_bat(stk, pci, 15);
	Client c;
	backend *be;
	sqlid user_id;
	sqlid role_id;
	bool admin;
	timestamp ts;
	lng pid;
	const char *s;
	int timeout;
	str msg = NULL;

	(void) cntxt;
	(void) mb;

	id = COLnew(0, TYPE_int, 0, TRANSIENT);
	user = COLnew(0, TYPE_str, 0, TRANSIENT);
	login = COLnew(0, TYPE_timestamp, 0, TRANSIENT);
	opt = COLnew(0, TYPE_str, 0, TRANSIENT);
	sessiontimeout = COLnew(0, TYPE_int, 0, TRANSIENT);
	querytimeout = COLnew(0, TYPE_int, 0, TRANSIENT);
	wlimit = COLnew(0, TYPE_int, 0, TRANSIENT);
	mlimit = COLnew(0, TYPE_int, 0, TRANSIENT);
	idle = COLnew(0, TYPE_timestamp, 0, TRANSIENT);
	language = COLnew(0, TYPE_str, 0, TRANSIENT);
	peer = COLnew(0, TYPE_str, 0, TRANSIENT);
	hostname = COLnew(0, TYPE_str, 0, TRANSIENT);
	application = COLnew(0, TYPE_str, 0, TRANSIENT);
	client = COLnew(0, TYPE_str, 0, TRANSIENT);
	clientpid = COLnew(0, TYPE_lng, 0, TRANSIENT);
	remark = COLnew(0, TYPE_str, 0, TRANSIENT);

	if (id == NULL || user == NULL || login == NULL || sessiontimeout == NULL
		|| idle == NULL || querytimeout == NULL || opt == NULL || wlimit == NULL
		|| mlimit == NULL || language == NULL || peer == NULL || hostname == NULL
		|| application == NULL || client == NULL || clientpid == NULL
		|| remark == NULL) {
		BBPreclaim(id);
		BBPreclaim(user);
		BBPreclaim(login);
		BBPreclaim(sessiontimeout);
		BBPreclaim(querytimeout);
		BBPreclaim(idle);
		BBPreclaim(opt);
		BBPreclaim(wlimit);
		BBPreclaim(mlimit);
		BBPreclaim(language);
		BBPreclaim(peer);
		BBPreclaim(hostname);
		BBPreclaim(application);
		BBPreclaim(client);
		BBPreclaim(clientpid);
		BBPreclaim(remark);

		throw(SQL, "sql.sessions", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	be = cntxt->sqlcontext;
	user_id = be->mvc->user_id;
	role_id = be->mvc->role_id;
	admin = user_id == USER_MONETDB || role_id == ROLE_SYSADMIN;

	MT_lock_set(&mal_contextLock);
	for (c = mal_clients; c < mal_clients + MAL_MAXCLIENTS; c++) {
		if (c->mode != RUNCLIENT)
			continue;

		backend *their_be = c->sqlcontext;
		bool allowed_to_see = admin || c == cntxt ||  their_be->mvc->user_id == user_id;
		// Note that their role_id is not checked. Just because we have
		// both been granted a ROLE does not mean you are allowed to see
		// my private details.
		if (!allowed_to_see)
			continue;

		const char *username = c->username;
		if (!username)
			username = str_nil;
		if (BUNappend(user, username, false) != GDK_SUCCEED)
			goto bailout;
		ts = timestamp_fromtime(c->login);
		if (is_timestamp_nil(ts)) {
			msg = createException(SQL, "sql.sessions",
									SQLSTATE(22003)
									"Failed to convert user logged time");
			goto bailout;
		}
		if (BUNappend(id, &c->idx, false) != GDK_SUCCEED)
				goto bailout;
		if (BUNappend(login, &ts, false) != GDK_SUCCEED)
			goto bailout;
		timeout = (int) (c->logical_sessiontimeout);
		if (BUNappend(sessiontimeout, &timeout, false) != GDK_SUCCEED)
			goto bailout;
		timeout = (int) (c->querytimeout / 1000000);
		if (BUNappend(querytimeout, &timeout, false) != GDK_SUCCEED)
			goto bailout;
		if (c->idle) {
			ts = timestamp_fromtime(c->idle);
			if (is_timestamp_nil(ts)) {
				msg = createException(SQL, "sql.sessions",
										SQLSTATE(22003)
										"Failed to convert user logged time");
				goto bailout;
			}
		} else
			ts = timestamp_nil;
		if (BUNappend(idle, &ts, false) != GDK_SUCCEED)
			goto bailout;
		if (BUNappend(opt, &c->optimizer, false) != GDK_SUCCEED)
				goto bailout;
		if (BUNappend(wlimit, &c->workerlimit, false) != GDK_SUCCEED)
			goto bailout;
		if (BUNappend(mlimit, &c->memorylimit, false) != GDK_SUCCEED)
			goto bailout;
		// If the scenario is NULL we assume we're in monetdbe/e which
		// is always SQL.
		s = c->scenario ? getScenarioLanguage(c) : "sql";
		if (BUNappend(language, s, false) != GDK_SUCCEED)
			goto bailout;
		s = c->peer ? c->peer : str_nil;
		if (BUNappend(peer, s, false) != GDK_SUCCEED)
			goto bailout;
		s = c->client_hostname ? c->client_hostname : str_nil;
		if (BUNappend(hostname, s, false) != GDK_SUCCEED)
			goto bailout;
		s = c->client_application ? c->client_application : str_nil;
		if (BUNappend(application, s, false) != GDK_SUCCEED)
			goto bailout;
		s = c->client_library ? c->client_library : str_nil;
		if (BUNappend(client, s, false) != GDK_SUCCEED)
			goto bailout;
		pid = c->client_pid;
		if (BUNappend(clientpid, pid ? &pid : &lng_nil, false) != GDK_SUCCEED)
			goto bailout;
		s = c->client_remark ? c->client_remark : str_nil;
		if (BUNappend(remark, s, false) != GDK_SUCCEED)
			goto bailout;
	}
	MT_lock_unset(&mal_contextLock);

	*idId = id->batCacheid;
	BBPkeepref(id);
	*userId = user->batCacheid;
	BBPkeepref(user);
	*loginId = login->batCacheid;
	BBPkeepref(login);
	*sessiontimeoutId = sessiontimeout->batCacheid;
	BBPkeepref(sessiontimeout);
	*querytimeoutId = querytimeout->batCacheid;
	BBPkeepref(querytimeout);
	*idleId = idle->batCacheid;
	BBPkeepref(idle);

	*optId = opt->batCacheid;
	BBPkeepref(opt);
	*wlimitId = wlimit->batCacheid;
	BBPkeepref(wlimit);
	*mlimitId = mlimit->batCacheid;
	BBPkeepref(mlimit);
	*languageId = language->batCacheid;
	BBPkeepref(language);
	*peerId = peer->batCacheid;
	BBPkeepref(peer);
	*hostnameId = hostname->batCacheid;
	BBPkeepref(hostname);
	*applicationId = application->batCacheid;
	BBPkeepref(application);
	*clientId = client->batCacheid;
	BBPkeepref(client);
	*clientpidId = clientpid->batCacheid;
	BBPkeepref(clientpid);
	*remarkId = remark->batCacheid;
	BBPkeepref(remark);

	return MAL_SUCCEED;

  bailout:
	MT_lock_unset(&mal_contextLock);
	BBPunfix(id->batCacheid);
	BBPunfix(user->batCacheid);
	BBPunfix(login->batCacheid);
	BBPunfix(sessiontimeout->batCacheid);
	BBPunfix(querytimeout->batCacheid);
	BBPunfix(idle->batCacheid);

	BBPunfix(opt->batCacheid);
	BBPunfix(wlimit->batCacheid);
	BBPunfix(mlimit->batCacheid);
	BBPunfix(language->batCacheid);
	BBPunfix(peer->batCacheid);
	BBPunfix(hostname->batCacheid);
	BBPunfix(application->batCacheid);
	BBPunfix(client->batCacheid);
	BBPunfix(clientpid->batCacheid);
	BBPunfix(remark->batCacheid);
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
		*getArgReference_bat(stk, pci, i) = t[i]->batCacheid;
		BBPkeepref(t[i]);
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
		*getArgReference_bat(stk, pci, i) = t[i]->batCacheid;
		BBPkeepref(t[i]);
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
		throw(SQL, "calc.rowid", SQLSTATE(3F000) "Schema missing %s", sname);
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "calc.rowid", SQLSTATE(42S02) "Table missing %s.%s",sname,tname);
	if (!isTable(t))
		throw(SQL, "calc.rowid", SQLSTATE(42000) "%s '%s' is not persistent",
			  TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
	if (!ol_first_node(t->columns))
		throw(SQL, "calc.rowid", SQLSTATE(42S22) "Column missing %s.%s",sname,tname);
	c = ol_first_node(t->columns)->data;
	/* HACK, get insert bat */
	sqlstore *store = m->session->tr->store;
	b = store->storage_api.bind_col(m->session->tr, c, QUICK);
	if( b == NULL)
		throw(SQL,"calc.rowid", SQLSTATE(HY005) "Cannot access column descriptor");
	/* UGH (move into storage backends!!) */
	*rid = BATcount(b);
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
		throw(SQL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if ((g = BATdescriptor(*gid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bi = bat_iterator(b);
	gi = bat_iterator(g);
	ocmp = ATOMcompare(b->ttype);
	gcmp = ATOMcompare(g->ttype);
	oc = BUNtail(bi, 0);
	gc = BUNtail(gi, 0);
	if (!ALIGNsynced(b, g)) {
		bat_iterator_end(&bi);
		bat_iterator_end(&gi);
		BBPunfix(b->batCacheid);
		BBPunfix(g->batCacheid);
		throw(SQL, name, SQLSTATE(45000) "Internal error, columns not aligned");
	}
/*
  if (!b->tsorted) {
  BBPunfix(b->batCacheid);
  BBPunfix(g->batCacheid);
  throw(SQL, name, SQLSTATE(45000) "Internal error, columns not sorted");
  }
*/
	r = COLnew(b->hseqbase, TYPE_int, BATcount(b), TRANSIENT);
	if (r == NULL) {
		bat_iterator_end(&bi);
		bat_iterator_end(&gi);
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
			bat_iterator_end(&bi);
			bat_iterator_end(&gi);
			BBPunfix(b->batCacheid);
			BBPunfix(g->batCacheid);
			BBPunfix(r->batCacheid);
			throw(SQL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		nrank += !dense || c;
	}
	bat_iterator_end(&bi);
	bat_iterator_end(&gi);
	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);
	*rid = r->batCacheid;
	BBPkeepref(r);
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
		throw(SQL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	bi = bat_iterator(b);
	if (!bi.sorted && !bi.revsorted) {
		bat_iterator_end(&bi);
		BBPunfix(b->batCacheid);
		throw(SQL, name, SQLSTATE(45000) "Internal error, columns not sorted");
	}

	cmp = ATOMcompare(bi.type);
	cur = BUNtail(bi, 0);
	r = COLnew(b->hseqbase, TYPE_int, BATcount(b), TRANSIENT);
	if (r == NULL) {
		bat_iterator_end(&bi);
		BBPunfix(b->batCacheid);
		throw(SQL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (BATtdensebi(&bi)) {
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
	bat_iterator_end(&bi);
	BBPunfix(b->batCacheid);
	*rid = r->batCacheid;
	BBPkeepref(r);
	return MAL_SUCCEED;
  bailout:
	bat_iterator_end(&bi);
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
	mvc *m = NULL;
	str msg;

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
	for (node *n = ol_first_node(t->columns); n; n = n->next) {
		sql_column *c = n->data;
		BAT *b = NULL, *nb = NULL;

		if (!(b = store->storage_api.bind_col(m->session->tr, c, RDONLY)))
			throw(SQL, "sql.drop_hash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		if (VIEWtparent(b) && (nb = BBP_desc(VIEWtparent(b)))) {
			BBPunfix(b->batCacheid);
			if (!(b = BATdescriptor(nb->batCacheid)))
				throw(SQL, "sql.drop_hash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
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

static str
sql_storage_appendrow(BAT *bs, const char *sname, const char *tname, const char *cname,
					  int access, const char *tpname,
					  BAT *sch, BAT *tab, BAT *col, BAT *type, BAT *loc,
					  BAT *cnt, BAT *atom, BAT *size, BAT *heap, BAT *indices,
					  BAT *phash, BAT *sort, BAT *imprints, BAT *mode,
					  BAT *revsort, BAT *key, BAT *oidx)
{
	BATiter bsi = bat_iterator(bs);
	lng sz;
	int w;
	bit bitval;

	if (BUNappend(sch, sname, false) != GDK_SUCCEED ||
		BUNappend(tab, tname, false) != GDK_SUCCEED ||
		BUNappend(col, cname, false) != GDK_SUCCEED)
		goto bailout1;
	if (access == TABLE_WRITABLE) {
		if (BUNappend(mode, "writable", false) != GDK_SUCCEED)
			goto bailout1;
	} else if (access == TABLE_APPENDONLY) {
		if (BUNappend(mode, "appendonly", false) != GDK_SUCCEED)
			goto bailout1;
	} else if (access == TABLE_READONLY) {
		if (BUNappend(mode, "readonly", false) != GDK_SUCCEED)
			goto bailout1;
	} else {
		if (BUNappend(mode, str_nil, false) != GDK_SUCCEED)
			goto bailout1;
	}
	if (BUNappend(type, tpname, false) != GDK_SUCCEED)
		goto bailout1;

	sz = bsi.count;
	if (BUNappend(cnt, &sz, false) != GDK_SUCCEED)
		goto bailout1;

	if (BUNappend(loc, BBP_physical(bs->batCacheid), false) != GDK_SUCCEED)
		goto bailout1;
	w = bsi.width;
	if (BUNappend(atom, &w, false) != GDK_SUCCEED)
		goto bailout1;

	sz = (lng) bsi.hfree;
	if (BUNappend(size, &sz, false) != GDK_SUCCEED)
		goto bailout1;

	sz = bsi.vhfree;
	if (BUNappend(heap, &sz, false) != GDK_SUCCEED)
		goto bailout1;

	sz = (lng) HASHsize(bs);
	if (BUNappend(indices, &sz, false) != GDK_SUCCEED)
		goto bailout1;

	bitval = sz > 0;
	if (BUNappend(phash, &bitval, false) != GDK_SUCCEED)
		goto bailout1;

	sz = 0;
	if (BUNappend(imprints, &sz, false) != GDK_SUCCEED)
		goto bailout1;
	bitval = bsi.sorted;
	if (!bitval && bsi.nosorted == 0)
		bitval = bit_nil;
	if (BUNappend(sort, &bitval, false) != GDK_SUCCEED)
		goto bailout1;

	bitval = bsi.revsorted;
	if (!bitval && bsi.norevsorted == 0)
		bitval = bit_nil;
	if (BUNappend(revsort, &bitval, false) != GDK_SUCCEED)
		goto bailout1;

	bitval = bsi.key;
	if (!bitval && bsi.nokey[0] == 0 && bsi.nokey[1] == 0)
		bitval = bit_nil;
	if (BUNappend(key, &bitval, false) != GDK_SUCCEED)
		goto bailout1;

	MT_lock_set(&bs->batIdxLock);
	sz = bs->torderidx && bs->torderidx != (Heap *) 1 ? bs->torderidx->free : 0;
	MT_lock_unset(&bs->batIdxLock);
	if (BUNappend(oidx, &sz, false) != GDK_SUCCEED)
		goto bailout1;
	bat_iterator_end(&bsi);
	return MAL_SUCCEED;
  bailout1:
	bat_iterator_end(&bsi);
	throw(SQL, "sql.storage", GDK_EXCEPTION);
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
	BAT *sch, *tab, *col, *type, *loc, *cnt, *atom, *size, *heap, *indices, *phash, *sort, *imprints, *mode, *revsort, *key, *oidx, *bs = NULL;
	mvc *m = NULL;
	str msg = MAL_SUCCEED;
	sql_trans *tr;
	node *ncol;
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

	if( pci->argc - pci->retc >= 1) {
		sname = *getArgReference_str(stk, pci, pci->retc);
		if (strNil(sname))
			throw(SQL, "sql.storage", SQLSTATE(42000) "Schema name cannot be NULL");
	}
	if( pci->argc - pci->retc >= 2) {
		tname = *getArgReference_str(stk, pci, pci->retc + 1);
		if (strNil(tname))
			throw(SQL, "sql.storage", SQLSTATE(42000) "Table name cannot be NULL");
	}
	if( pci->argc - pci->retc >= 3) {
		cname = *getArgReference_str(stk, pci, pci->retc + 2);
		if (strNil(cname))
			throw(SQL, "sql.storage", SQLSTATE(42000) "Column name cannot be NULL");
	}

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
		msg = createException(SQL, "sql.storage", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	/* check for limited storage tables */
	os_iterator(&si, tr->cat->schemas, tr, NULL);
	for (sql_base *b = oi_next(&si); b; b = oi_next(&si)) {
		sql_schema *s = (sql_schema *) b;
		if ((sname && strcmp(b->name, sname) ) || b->name[0] == '%')
			continue;
		if (s->tables) {
			struct os_iter oi;

			os_iterator(&oi, s->tables, tr, NULL);
			for (sql_base *bt = oi_next(&oi); bt; bt = oi_next(&oi)) {
				sql_table *t = (sql_table *) bt;
				if( tname && strcmp(bt->name, tname) )
					continue;
				if (isTable(t)) {
					if (ol_first_node(t->columns)) {
						for (ncol = ol_first_node((t)->columns); ncol; ncol = ncol->next) {
							sql_base *bc = ncol->data;
							sql_column *c = (sql_column *) ncol->data;

							if( cname && strcmp(bc->name, cname) )
								continue;
							bs = store->storage_api.bind_col(tr, c, QUICK);
							if (bs == NULL) {
								msg = createException(SQL, "sql.storage", SQLSTATE(HY005) "Cannot access column descriptor");
								goto bailout;
							}

							msg = sql_storage_appendrow(
								bs, b->name, bt->name, bc->name,
								c->t->access, c->type.type->base.name,
								sch, tab, col, type, loc, cnt, atom, size,
								heap, indices, phash, sort, imprints, mode,
								revsort, key, oidx);
							if (msg != MAL_SUCCEED)
								goto bailout;
						}
					}

					if (t->idxs) {
						for (ncol = ol_first_node((t)->idxs); ncol; ncol = ncol->next) {
							sql_base *bc = ncol->data;
							sql_idx *c = (sql_idx *) ncol->data;
							if (idx_has_column(c->type)) {
								bs = store->storage_api.bind_idx(tr, c, QUICK);

								if (bs == NULL) {
									msg = createException(SQL, "sql.storage", SQLSTATE(HY005) "Cannot access column descriptor");
									goto bailout;
								}
								if( cname && strcmp(bc->name, cname) )
									continue;
								msg = sql_storage_appendrow(
									bs, b->name, bt->name, bc->name,
									c->t->access, "oid",
									sch, tab, col, type, loc, cnt, atom, size,
									heap, indices, phash, sort, imprints, mode,
									revsort, key, oidx);
								if (msg != MAL_SUCCEED)
									goto bailout;
							}
						}
					}
				}
			}
		}
	}

	*rsch = sch->batCacheid;
	BBPkeepref(sch);
	*rtab = tab->batCacheid;
	BBPkeepref(tab);
	*rcol = col->batCacheid;
	BBPkeepref(col);
	*rmode = mode->batCacheid;
	BBPkeepref(mode);
	*rloc = loc->batCacheid;
	BBPkeepref(loc);
	*rtype = type->batCacheid;
	BBPkeepref(type);
	*rcnt = cnt->batCacheid;
	BBPkeepref(cnt);
	*ratom = atom->batCacheid;
	BBPkeepref(atom);
	*rsize = size->batCacheid;
	BBPkeepref(size);
	*rheap = heap->batCacheid;
	BBPkeepref(heap);
	*rindices = indices->batCacheid;
	BBPkeepref(indices);
	*rphash = phash->batCacheid;
	BBPkeepref(phash);
	*rimprints = imprints->batCacheid;
	BBPkeepref(imprints);
	*rsort = sort->batCacheid;
	BBPkeepref(sort);
	*rrevsort = revsort->batCacheid;
	BBPkeepref(revsort);
	*rkey = key->batCacheid;
	BBPkeepref(key);
	*roidx = oidx->batCacheid;
	BBPkeepref(oidx);
	return MAL_SUCCEED;

  bailout:
	BBPreclaim(sch);
	BBPreclaim(tab);
	BBPreclaim(col);
	BBPreclaim(mode);
	BBPreclaim(loc);
	BBPreclaim(cnt);
	BBPreclaim(type);
	BBPreclaim(atom);
	BBPreclaim(size);
	BBPreclaim(heap);
	BBPreclaim(indices);
	BBPreclaim(phash);
	BBPreclaim(imprints);
	BBPreclaim(sort);
	BBPreclaim(revsort);
	BBPreclaim(key);
	BBPreclaim(oidx);
	if (!msg)
		msg = createException(SQL, "sql.storage", GDK_EXCEPTION);
	return msg;
}

void
freeVariables(Client c, MalBlkPtr mb, MalStkPtr glb, int oldvtop)
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
	assert(oldvtop <= mb->vsize);
	mb->vtop = oldvtop;
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
/*SQLhot_snapshot(void *ret, const str *tarfile_arg [, bool onserver ])*/
SQLhot_snapshot(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char *filename;
	bool onserver = true;
	bool omitunlogged = false;
	str omitids = NULL;
	char *msg = MAL_SUCCEED;
	char buf[80];
	mvc *mvc;
	ssize_t sz;
	stream *s;
	stream *cb = NULL;
	lng result;

	filename = *getArgReference_str(stk, pci, 1);
	if (pci->argc > 2)
		onserver = *getArgReference_bit(stk, pci, 2);
	if (pci->argc > 3)
		omitunlogged = *getArgReference_bit(stk, pci, 3);
	if (pci->argc > 4) {
		omitids = *getArgReference_str(stk, pci, 4);
		if (strNil(omitids))
			omitids = NULL;
	}

	msg = getSQLContext(cntxt, mb, &mvc, NULL);
	if (msg)
		return msg;
	sql_trans *tr = mvc->session->tr;

	if (onserver) {
		lng result = store_hot_snapshot(tr, filename, omitunlogged, omitids);
		if (result)
			return MAL_SUCCEED;
		else
			throw(SQL, "sql.hot_snapshot", GDK_EXCEPTION);
	}

	// sync with client, copy pasted from mvc_export_table_wrap
	while (!mvc->scanner.rs->eof)
		if (bstream_next(mvc->scanner.rs) < 0)
			throw(SQL, "sql.hot_snapshot", "interrupted");

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
	mnstr_printf(s, "wb %s\n", filename);
	mnstr_flush(s, MNSTR_FLUSH_DATA);
	if ((sz = mnstr_readline(mvc->scanner.rs->s, buf, sizeof(buf))) > 1) {
		/* non-empty line indicates failure on client */
		msg = createException(IO, "streams.open", "%s", buf);
			/* discard until client flushes */
			while (mnstr_read(mvc->scanner.rs->s, buf, 1, sizeof(buf)) > 0) {
				/* ignore remainder of error message */
			}
		goto end;
	}

	// client is waiting for data now, send it.
	result = store_hot_snapshot_to_stream(tr, cb, omitunlogged, omitids);
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

MT_Lock lock_persist_unlogged = MT_LOCK_INITIALIZER(lock_persist_unlogged);

str
SQLpersist_unlogged(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)stk;
	(void)pci;

	assert(pci->argc == 5);

	bat *o0 = getArgReference_bat(stk, pci, 0),
		*o1 = getArgReference_bat(stk, pci, 1),
		*o2 = getArgReference_bat(stk, pci, 2);
	str sname = *getArgReference_str(stk, pci, 3),
		tname = *getArgReference_str(stk, pci, 4),
		msg = MAL_SUCCEED;

	mvc *m = NULL;
	msg = getSQLContext(cntxt, mb, &m, NULL);

	if (msg)
		return msg;

	sqlstore *store = store = m->session->tr->store;

	sql_schema *s = mvc_bind_schema(m, sname);
	if (s == NULL)
		throw(SQL, "sql.persist_unlogged", SQLSTATE(3F000) "Schema missing %s.", sname);

	if (!mvc_schema_privs(m, s))
		throw(SQL, "sql.persist_unlogged", SQLSTATE(42000) "Access denied for %s to schema '%s'.",
			  get_string_global_var(m, "current_user"), s->base.name);

	sql_table *t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		throw(SQL, "sql.persist_unlogged", SQLSTATE(42S02) "Table missing %s.%s", sname, tname);

	if (!isUnloggedTable(t) || t->access != TABLE_APPENDONLY)
		throw(SQL, "sql.persist_unlogged", "Unlogged and Insert Only mode combination required for table %s.%s", sname, tname);

	lng count = 0;

	sql_trans *tr = m->session->tr;
	storage *t_del = bind_del_data(tr, t, NULL);

	BAT *d = NULL;

	if (t_del)
		d = BATdescriptor(t_del->cs.bid);
	if (t_del == NULL || d == NULL)
		throw(SQL, "sql.persist_unlogged", "Cannot access %s column storage.", tname);

	MT_lock_set(&lock_persist_unlogged);
	BATiter d_bi = bat_iterator(d);

	if (BBP_status(d->batCacheid) & BBPEXISTING) {

		assert(d->batInserted <= d_bi.count);

		if (d->batInserted < d_bi.count) {
			int n = ol_length(t->columns);

			bat *commit_list = GDKzalloc(sizeof(bat) * (n + 2));
			BUN *sizes = GDKzalloc(sizeof(BUN) * (n + 2));

			if (commit_list == NULL || sizes == NULL) {
				bat_iterator_end(&d_bi);
				MT_lock_unset(&lock_persist_unlogged);
				GDKfree(commit_list);
				GDKfree(sizes);
				BBPreclaim(d);
				throw(SQL, "sql.persist_unlogged", SQLSTATE(HY001));
			}

			commit_list[0] = 0;
			sizes[0] = 0;
			int i = 1;

			for (node *ncol = ol_first_node(t->columns); ncol; ncol = ncol->next, i++) {

				sql_column *c = (sql_column *) ncol->data;
				BAT *b = store->storage_api.bind_col(tr, c, QUICK);

				if (b == NULL) {
					bat_iterator_end(&d_bi);
					MT_lock_unset(&lock_persist_unlogged);
					GDKfree(commit_list);
					GDKfree(sizes);
					BBPreclaim(d);
					throw(SQL, "sql.persist_unlogged", "Cannot access column descriptor.");
				}

				commit_list[i] = b->batCacheid;
				sizes[i] = d_bi.count;
			}

			assert(i<n+2);
			commit_list[i] = d->batCacheid;
			sizes[i] = d_bi.count;
			i++;

			if (TMsubcommit_list(commit_list, sizes, i, -1) != GDK_SUCCEED) {
				bat_iterator_end(&d_bi);
				MT_lock_unset(&lock_persist_unlogged);
				GDKfree(commit_list);
				GDKfree(sizes);
				BBPreclaim(d);
				throw(SQL, "sql.persist_unlogged", "Lower level commit operation failed");
			}

			GDKfree(commit_list);
			GDKfree(sizes);
		}
		count = d_bi.count;
	} else {
		/* special case of log_tstart: third arg == NULL with second arg
		 * true is request to rotate log file ASAP */
		store->logger_api.log_tstart(store, true, NULL);
		/* special case for sql->debug: if 1024 bit is set,
		 * store_manager doesn't wait for 30 seconds of idle time before
		 * attempting to rotate */
		MT_lock_set(&store->flush);
		store->debug |= 1024;
		MT_lock_unset(&store->flush);
	}

	bat_iterator_end(&d_bi);
	MT_lock_unset(&lock_persist_unlogged);
	BBPreclaim(d);

	BAT *table = COLnew(0, TYPE_str, 0, TRANSIENT),
		*tableid = COLnew(0, TYPE_int, 0, TRANSIENT),
		*rowcount = COLnew(0, TYPE_lng, 0, TRANSIENT);

	if (table == NULL || tableid == NULL || rowcount == NULL ||
		BUNappend(table, tname, false) != GDK_SUCCEED ||
		BUNappend(tableid, &(t->base.id), false) != GDK_SUCCEED ||
		BUNappend(rowcount, &count, false) != GDK_SUCCEED) {
		BBPnreclaim(3, table, tableid, rowcount);
		throw(SQL, "sql.persist_unlogged", SQLSTATE(HY001));
	}
	*o0 = table->batCacheid;
	*o1 = tableid->batCacheid;
	*o2 = rowcount->batCacheid;
	BBPkeepref(table);
	BBPkeepref(tableid);
	BBPkeepref(rowcount);
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

	sessionid = COLnew(0, TYPE_int, 256, TRANSIENT);
	user = COLnew(0, TYPE_str, 256, TRANSIENT);
	statementid = COLnew(0, TYPE_int, 256, TRANSIENT);
	statement = COLnew(0, TYPE_str, 256, TRANSIENT);
	created = COLnew(0, TYPE_timestamp, 256, TRANSIENT);
	if (sessionid == NULL || user == NULL || statementid == NULL || statement == NULL || created == NULL) {
		msg = createException(SQL, "sql.session_prepared_statements", GDK_EXCEPTION);
		goto bailout;
	}

	for (q = sql->qc->q; q; q = q->next) {
		gdk_return bun_res;
		if (BUNappend(sessionid, &(cntxt->idx), false) != GDK_SUCCEED) {
			msg = createException(SQL, "sql.session_prepared_statements", GDK_EXCEPTION);
			goto bailout;
		}

		if (msg != MAL_SUCCEED)
			goto bailout;
		bun_res = BUNappend(user, cntxt->username, false);
		if (bun_res != GDK_SUCCEED) {
			msg = createException(SQL, "sql.session_prepared_statements", GDK_EXCEPTION);
			goto bailout;
		}

		if (BUNappend(statementid, &(q->id), false) != GDK_SUCCEED) {
			msg = createException(SQL, "sql.session_prepared_statements", GDK_EXCEPTION);
			goto bailout;
		}
		if (BUNappend(statement, q->f->query, false) != GDK_SUCCEED) {
			msg = createException(SQL, "sql.session_prepared_statements", GDK_EXCEPTION);
			goto bailout;
		}
		if (BUNappend(created, &(q->created), false) != GDK_SUCCEED) {
			msg = createException(SQL, "sql.session_prepared_statements", GDK_EXCEPTION);
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
		*sid = sessionid->batCacheid;
		BBPkeepref(sessionid);
		*u = user->batCacheid;
		BBPkeepref(user);
		*i = statementid->batCacheid;
		BBPkeepref(statementid);
		*s = statement->batCacheid;
		BBPkeepref(statement);
		*c = created->batCacheid;
		BBPkeepref(created);
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
		msg = createException(SQL, "sql.session_prepared_statements_args", GDK_EXCEPTION);
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
					BUNappend(type, t->type->base.name, false) != GDK_SUCCEED ||
					BUNappend(digits, &t->digits, false) != GDK_SUCCEED ||
					BUNappend(scale, &t->scale, false) != GDK_SUCCEED ||
					BUNappend(isinout, &inout, false) != GDK_SUCCEED ||
					BUNappend(number, &arg_number, false) != GDK_SUCCEED ||
					BUNappend(schema, rschema, false) != GDK_SUCCEED ||
					BUNappend(table, rname, false) != GDK_SUCCEED ||
					BUNappend(column, name, false) != GDK_SUCCEED) {
					msg = createException(SQL, "sql.session_prepared_statements_args", GDK_EXCEPTION);
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
					BUNappend(type, t->type->base.name, false) != GDK_SUCCEED ||
					BUNappend(digits, &(t->digits), false) != GDK_SUCCEED ||
					BUNappend(scale, &(t->scale), false) != GDK_SUCCEED ||
					BUNappend(isinout, &inout, false) != GDK_SUCCEED ||
					BUNappend(number, &arg_number, false) != GDK_SUCCEED ||
					BUNappend(schema, ATOMnilptr(TYPE_str), false) != GDK_SUCCEED ||
					BUNappend(table, ATOMnilptr(TYPE_str), false) != GDK_SUCCEED ||
					BUNappend(column, ATOMnilptr(TYPE_str), false) != GDK_SUCCEED) {
					msg = createException(SQL, "sql.session_prepared_statements_args", GDK_EXCEPTION);
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
		*sid = statementid->batCacheid;
		BBPkeepref(statementid);
		*t = type->batCacheid;
		BBPkeepref(type);
		*d = digits->batCacheid;
		BBPkeepref(digits);
		*s = scale->batCacheid;
		BBPkeepref(scale);
		*io = isinout->batCacheid;
		BBPkeepref(isinout);
		*n = number->batCacheid;
		BBPkeepref(number);
		*sch = schema->batCacheid;
		BBPkeepref(schema);
		*tbl = table->batCacheid;
		BBPkeepref(table);
		*col = column->batCacheid;
		BBPkeepref(column);
	}
	return msg;
}

/* input id, row-input-values
 * for each id call function(with row-input-values) return table
 * return for each id the table, ie id (*length of table) and table results
 */
str
SQLunionfunc(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int arg = pci->retc;
	str mod, fcn, ret = MAL_SUCCEED;
	InstrPtr npci;
	MalBlkPtr nmb = newMalBlk(1), omb = NULL;

	if (!nmb)
		return createException(MAL, "sql.unionfunc", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	mod = *getArgReference_str(stk, pci, arg++);
	fcn = *getArgReference_str(stk, pci, arg++);
	npci = newStmtArgs(nmb, mod, fcn, pci->argc);
	if (npci == NULL) {
		freeMalBlk(nmb);
		return createException(MAL, "sql.unionfunc", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	for (int i = 1; i < pci->retc; i++) {
		int type = getArgType(mb, pci, i);

		if (i==1)
			getArg(npci, 0) = newTmpVariable(nmb, type);
		else
			npci = pushReturn(nmb, npci, newTmpVariable(nmb, type));
	}
	for (int i = pci->retc+2+1; i < pci->argc; i++) {
		int type = getBatType(getArgType(mb, pci, i));

		npci = pushNil(nmb, npci, type);
	}
	pushInstruction(nmb, npci);
	/* check program to get the proper malblk */
	if (chkInstruction(cntxt->usermodule, nmb, npci)) {
		freeMalBlk(nmb);
		return createException(MAL, "sql.unionfunc", SQLSTATE(42000) PROGRAM_GENERAL);
	}

	if (npci) {
		BAT **res = NULL, **input = NULL;
		BATiter *bi = NULL;
		BUN cnt = 0;
		int nrinput = pci->argc - 2 - pci->retc;
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
		assert(pci->retc + 2 + nrinput == pci->argc);
		for (int i = 0, j = pci->retc+2; j < pci->argc; i++, j++) {
			bat *b = getArgReference_bat(stk, pci, j);
			if (!(input[i] = BATdescriptor(*b))) {
				ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY005) "Cannot access column descriptor");
				while (i > 0) {
					i--;
					bat_iterator_end(&bi[i]);
					BBPunfix(input[i]->batCacheid);
				}
				GDKfree(input);
				input = NULL;
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
				ret = createException(MAL, "sql.unionfunc", GDK_EXCEPTION);
				goto finalize;
			}
		}

		if (npci->blk && npci->blk->stop > 1) {
			omb = nmb;
			if (!(nmb = copyMalBlk(npci->blk))) {
				ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto finalize;
			}
		}
		if (!(env = prepareMALstack(nmb, nmb->vsize))) { /* needed for result */
			ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto finalize;
		}
		q = getInstrPtr(nmb, 0);

		int start = 1;
		if (nmb->stop == 1 && (omb || !npci->fcn || npci->token != PATcall)) {
			InstrPtr *stmt = nmb->stmt;
			nmb->stmt = (InstrPtr*)GDKmalloc(sizeof(InstrPtr*)*3);
			nmb->stmt[0] = NULL; /* no main() */
			nmb->stmt[1] = NULL; /* no profiling */
			nmb->stmt[2] = stmt[0];
			nmb->stop = nmb->ssize = 3;
			GDKfree(stmt);
			start = 2;
		}
		for (BUN cur = 0; cur<cnt && !ret; cur++ ) {
			MalStkPtr nstk = prepareMALstack(nmb, nmb->vsize);
			int i,ii;

			if (!nstk) { /* needed for result */
				ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			} else {
				/* copy (input) arguments onto destination stack, skipping rowid col */
				for (i = 1, ii = q->retc; ii < q->argc && !ret; ii++, i++) {
					ValPtr lhs = &nstk->stk[q->argv[ii]];
					ptr rhs = (ptr)BUNtail(bi[i], cur);

					if (VALset(lhs, input[i]->ttype, rhs) == NULL)
						ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
				if (!ret && ii == q->argc) {
					BAT *fres = NULL;
					if (!omb && npci->fcn && npci->token == PATcall) /* pattern */
						ret = (*(str (*)(Client, MalBlkPtr, MalStkPtr, InstrPtr))npci->fcn)(cntxt, nmb, nstk, npci);
					else
						ret = runMALsequence(cntxt, nmb, start, nmb->stop, nstk, env /* copy result in nstk first instruction*/, q);

					if (!ret) {
						/* insert into result */
						if (!(fres = BBPquickdesc(omb?env->stk[q->argv[0]].val.bval:nstk->stk[q->argv[0]].val.bval))) {
							ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY005) "Cannot access column descriptor");
						} else {
							BAT *p = BATconstant(fres->hseqbase, res[0]->ttype, (ptr)BUNtail(bi[0], cur), BATcount(fres), TRANSIENT);

							if (p) {
								if (BATappend(res[0], p, NULL, FALSE) != GDK_SUCCEED)
									ret = createException(MAL, "sql.unionfunc", GDK_EXCEPTION);
								BBPunfix(p->batCacheid);
							} else {
								ret = createException(MAL, "sql.unionfunc", GDK_EXCEPTION);
							}
						}
						i=1;
						for (ii = 0; i < pci->retc && !ret; ii++, i++) {
							BAT *b;
							ValPtr vp = omb ? env->stk + q->argv[ii] : nstk->stk + q->argv[ii];

							if (!(b = BATdescriptor(vp->val.bval)))
								ret = createException(MAL, "sql.unionfunc", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
							else if (BATappend(res[i], b, NULL, FALSE) != GDK_SUCCEED)
								ret = createException(MAL, "sql.unionfunc", GDK_EXCEPTION);
							if (b) {
								BBPrelease(b->batCacheid); /* release ref from env stack */
								BBPunfix(b->batCacheid);   /* free pointer */
								VALempty(vp);
							}
						}
					}
				}
				freeStack(nstk);
			}
		}
finalize:
		freeStack(env);
		if (nmb)
			freeMalBlk(nmb);
		if (omb)
			freeMalBlk(omb);
		if (res)
			for (int i = 0; i<pci->retc; i++) {
				bat *b = getArgReference_bat(stk, pci, i);
				if (res[i]) {
					*b = res[i]->batCacheid;
					if (ret)
						BBPunfix(*b);
					else
						BBPkeepref(res[i]);
				}
			}
		GDKfree(res);
		if (input) {
			for (int i = 0; i<nrinput; i++) {
				if (input[i]) {
					bat_iterator_end(&bi[i]);
					BBPunfix(input[i]->batCacheid);
				}
			}
			GDKfree(input);
		}
		GDKfree(bi);
	}
	return ret;
}

static str
do_str_column_vacuum(sql_trans *tr, sql_column *c, bool force)
{
	if (ATOMvarsized(c->type.type->localtype)) {
		int res = 0;
		sqlstore *store = tr->store;

		if ((res = (int) store->storage_api.vacuum_col(tr, c, force)) != LOG_OK) {
			if (res == LOG_CONFLICT)
				throw(SQL, "do_str_column_vacuum", SQLSTATE(25S01) "TRANSACTION CONFLICT in storage_api.vacuum_col %s.%s.%s", c->t->s->base.name, c->t->base.name, c->base.name);
			if (res == LOG_ERR)
				throw(SQL, "do_str_column_vacuum", SQLSTATE(HY000) "LOG ERROR in storage_api.vacuum_col %s.%s.%s", c->t->s->base.name, c->t->base.name, c->base.name);
			throw(SQL, "do_str_column_vacuum", SQLSTATE(HY000) "ERROR in storage_api.vacuum_col %s.%s.%s", c->t->s->base.name, c->t->base.name, c->base.name);
		}
	}
	return MAL_SUCCEED;
}

static str
do_str_table_vacuum(sql_trans *tr, sql_table *t, bool force)
{
	int res = 0;
	sqlstore *store = tr->store;

	if ((res = (int) store->storage_api.vacuum_tab(tr, t, force)) != LOG_OK) {
		if (res == LOG_CONFLICT)
			throw(SQL, "do_str_table_vacuum", SQLSTATE(25S01) "TRANSACTION CONFLICT in storage_api.vacuum_col %s.%s", t->s->base.name, t->base.name);
		if (res == LOG_ERR)
			throw(SQL, "do_str_table_vacuum", SQLSTATE(HY000) "LOG ERROR in storage_api.vacuum_col %s.%s", t->s->base.name, t->base.name);
		throw(SQL, "do_str_table_vacuum", SQLSTATE(HY000) "ERROR in storage_api.vacuum_col %s.%s", t->s->base.name, t->base.name);
	}
	return MAL_SUCCEED;
}

static str
SQLstr_vacuum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = NULL;
	char *sname = *getArgReference_str(stk, pci, 1);
	char *tname = *getArgReference_str(stk, pci, 2);
	char *cname = NULL;
	if (pci->argc == 4)
		cname = *getArgReference_str(stk, pci, 3);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	sql_trans *tr = m->session->tr;
	sql_schema *s = NULL;
	sql_table *t = NULL;
	sql_column *c = NULL;

	if (strNil(sname))
		throw(SQL, "sql.str_vacuum", SQLSTATE(42000) "Schema name cannot be NULL");
	if (strNil(tname))
		throw(SQL, "sql.str_vacuum", SQLSTATE(42000) "Table name cannot be NULL");
	if (cname && strNil(cname))
		throw(SQL, "sql.str_vacuum", SQLSTATE(42000) "Column name cannot be NULL");
	if ((s = mvc_bind_schema(m, sname)) == NULL)
		throw(SQL, "sql.str_vacuum", SQLSTATE(3F000) "Invalid or missing schema %s",sname);
	if ((t = mvc_bind_table(m, s, tname)) == NULL)
		throw(SQL, "sql.str_vacuum", SQLSTATE(42S02) "Invalid or missing table %s.%s",sname,tname);
	if (!isTable(t))
		throw(SQL, "sql.str_vacuum", SQLSTATE(42000) "%s '%s' is not persistent",
			  TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
	if (isTempTable(t))
		throw(SQL, "sql.str_vacuum", SQLSTATE(42000) "Cannot vacuum column from temporary table");
	if (cname) {
		if ((c = mvc_bind_column(m, t, cname)) == NULL)
			throw(SQL, "sql.str_vacuum", SQLSTATE(42S22) "Column not found in %s.%s.%s",sname,tname,cname);
		if (c->storage_type)
			throw(SQL, "sql.str_vacuum", SQLSTATE(42000) "Cannot vacuum compressed column");
	}

	if (c)
		return do_str_column_vacuum(tr, c, true);
	else
		return do_str_table_vacuum(tr, t, true);
}


static gdk_return
str_vacuum_callback(int argc, void *argv[])
{
	sqlstore *store = (sqlstore *) argv[0];
	char *sname = (char *) argv[1];
	char *tname = (char *) argv[2];
	char *cname = (char *) argv[3];
	allocator *sa = NULL;
	sql_session *session = NULL;
	sql_schema *s = NULL;
	sql_table *t = NULL;
	sql_column *c = NULL;
	char *msg;
	gdk_return res = GDK_SUCCEED;

	(void) argc;

	if ((sa = sa_create(NULL)) == NULL) {
		TRC_ERROR(SQL_EXECUTION, "[str_vacuum_callback] -- Failed to create allocator!");
		return GDK_FAIL;
	}

	if ((session = sql_session_create(store, sa, 0)) == NULL) {
		TRC_ERROR(SQL_EXECUTION, "[str_vacuum_callback] -- Failed to create session!");
		sa_destroy(sa);
		return GDK_FAIL;
	}

	if (sql_trans_begin(session) < 0) {
		TRC_ERROR(SQL_EXECUTION, "[str_vacuum_callback] -- Failed to begin transaction!");
		sql_session_destroy(session);
		sa_destroy(sa);
		return GDK_FAIL;
	}

	do {
		if((s = find_sql_schema(session->tr, sname)) == NULL) {
			TRC_ERROR(SQL_EXECUTION, "[str_vacuum_callback] -- Invalid or missing schema %s!",sname);
			res = GDK_FAIL;
			break;
		}

		if((t = find_sql_table(session->tr, s, tname)) == NULL) {
			TRC_ERROR(SQL_EXECUTION, "[str_vacuum_callback] -- Invalid or missing table %s!", tname);
			res = GDK_FAIL;
			break;
		}
		if (cname) {
			if ((c = find_sql_column(t, cname)) == NULL) {
				TRC_ERROR(SQL_EXECUTION, "[str_vacuum_callback] -- Invalid or missing column %s!", cname);
				res = GDK_FAIL;
				break;
			}

			if((msg=do_str_column_vacuum(session->tr, c, false)) != MAL_SUCCEED) {
				TRC_ERROR(SQL_EXECUTION, "[str_vacuum_callback] -- %s", msg);
				res = GDK_FAIL;
			}
		} else {
			if((msg=do_str_table_vacuum(session->tr, t, false)) != MAL_SUCCEED) {
				TRC_ERROR(SQL_EXECUTION, "[str_vacuum_callback] -- %s", msg);
				res = GDK_FAIL;
			}
		}

	} while(0);

	if (res == GDK_SUCCEED) { /* everything is ok, do the commit route */
		switch (sql_trans_end(session, SQL_OK)) {
			case SQL_ERR:
				TRC_ERROR(SQL_EXECUTION, "[str_column_vacuum_callback] -- transaction commit failed (kernel error: %s)", GDKerrbuf);
				res = GDK_FAIL;
				break;
			case SQL_CONFLICT:
				TRC_ERROR(SQL_EXECUTION, "[str_column_vacuum_callback] -- transaction is aborted because of concurrency conflicts, will ROLLBACK instead");
				res = GDK_FAIL;
				break;
			default:
				break;
		}
	} else { /* an error triggered, rollback and ignore further errors */
		(void)sql_trans_end(session, SQL_ERR);
	}

	sql_session_destroy(session);
	sa_destroy(sa);
	return res;
}

static gdk_return
str_vacuum_callback_args_free(int argc, void *argv[])
{
	(void) argc;
	// free up sname, tname, cname. First pointer points to sqlstore so leave it.
	GDKfree(argv[1]); // sname
	GDKfree(argv[2]); // tname
	if (argv[3])
		GDKfree(argv[3]); // cname
	return GDK_SUCCEED;
}

static str
SQLstr_auto_vacuum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = NULL;
	char *sname = *getArgReference_str(stk, pci, 1);
	char *tname = *getArgReference_str(stk, pci, 2);
	char *cname = NULL;
	int iarg = 3;
	if (pci->argc == 5) {
		cname = *getArgReference_str(stk, pci, 3);
		iarg++;
	}
	int interval = *getArgReference_int(stk, pci, iarg); // in sec
	char *sname_copy = NULL, *tname_copy = NULL, *cname_copy = NULL;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	sql_schema *s = NULL;
	sql_table *t = NULL;
	sql_column *c = NULL;

	if (strNil(sname))
		throw(SQL, "sql.str_auto_vacuum", SQLSTATE(42000) "Schema name cannot be NULL");
	if (strNil(tname))
		throw(SQL, "sql.str_auto_vacuum", SQLSTATE(42000) "Table name cannot be NULL");
	if (strNil(cname))
		throw(SQL, "sql.str_auto_vacuum", SQLSTATE(42000) "Column name cannot be NULL");
	if ((s = mvc_bind_schema(m, sname)) == NULL)
		throw(SQL, "sql.str_auto_vacuum", SQLSTATE(3F000) "Invalid or missing schema %s",sname);
	if ((t = mvc_bind_table(m, s, tname)) == NULL)
		throw(SQL, "sql.str_auto_vacuum", SQLSTATE(42S02) "Invalid or missing table %s.%s",sname,tname);
	if (!isTable(t))
		throw(SQL, "sql.str_auto_vacuum", SQLSTATE(42000) "%s '%s' is not persistent",
			  TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
	if (isTempTable(t))
		throw(SQL, "sql.str_auto_vacuum", SQLSTATE(42000) "Cannot vacuum column from temporary table");
	if (cname && (c = mvc_bind_column(m, t, cname)) == NULL)
		throw(SQL, "sql.str_auto_vacuum", SQLSTATE(42S22) "Column not found in %s.%s.%s",sname,tname,cname);
	if (c && c->storage_type)
		throw(SQL, "sql.str_auto_vacuum", SQLSTATE(42000) "Cannot vacuum compressed column");

	if (!(sname_copy = GDKstrdup(sname)) || !(tname_copy = GDKstrdup(tname)) || (cname && !(cname_copy = GDKstrdup(cname)))) {
		GDKfree(sname_copy);
		GDKfree(tname_copy);
		GDKfree(cname_copy);
		throw(SQL, "sql.str_auto_vacuum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	void *argv[4] = {m->store, sname_copy, tname_copy, cname_copy};

	if (gdk_add_callback("str_vacuum", str_vacuum_callback, 4, argv, interval) != GDK_SUCCEED) {
		str_vacuum_callback_args_free(4, argv);
		throw(SQL, "sql.str_auto_vacuum", "adding vacuum callback failed!");
	}

	return MAL_SUCCEED;
}

static str
SQLstr_stop_vacuum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = NULL;
	char *sname = *getArgReference_str(stk, pci, 1);
	char *tname = *getArgReference_str(stk, pci, 2);
	char *cname = NULL;
	if (pci->argc == 4)
		cname = *getArgReference_str(stk, pci, 3);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	sql_schema *s = NULL;
	sql_table *t = NULL;
	sql_column *c = NULL;

	if (strNil(sname))
		throw(SQL, "sql.str_stop_vacuum", SQLSTATE(42000) "Schema name cannot be NULL");
	if (strNil(tname))
		throw(SQL, "sql.str_stop_vacuum", SQLSTATE(42000) "Table name cannot be NULL");
	if (cname && strNil(cname))
		throw(SQL, "sql.str_stop_vacuum", SQLSTATE(42000) "Column name cannot be NULL");
	if ((s = mvc_bind_schema(m, sname)) == NULL)
		throw(SQL, "sql.str_stop_vacuum", SQLSTATE(3F000) "Invalid or missing schema %s",sname);
	if ((t = mvc_bind_table(m, s, tname)) == NULL)
		throw(SQL, "sql.str_stop_vacuum", SQLSTATE(42S02) "Invalid or missing table %s.%s",sname,tname);
	if (!isTable(t))
		throw(SQL, "sql.str_stop_vacuum", SQLSTATE(42000) "%s '%s' is not persistent",
			  TABLE_TYPE_DESCRIPTION(t->type, t->properties), t->base.name);
	if (isTempTable(t))
		throw(SQL, "sql.str_stop_vacuum", SQLSTATE(42000) "Cannot vacuum column from temporary table");
	if (cname && (c = mvc_bind_column(m, t, cname)) == NULL)
		throw(SQL, "sql.str_stop_vacuum", SQLSTATE(42S22) "Column not found in %s.%s.%s",sname,tname,cname);

	if(gdk_remove_callback("str_vacuum", str_vacuum_callback_args_free) != GDK_SUCCEED)
		throw(SQL, "sql.str_stop_vacuum", "removing vacuum callback failed!");

	return MAL_SUCCEED;
}


#include "sql_cat.h"
#include "sql_rank.h"
#include "sql_user.h"
#include "sql_assert.h"
#include "sql_execute.h"
#include "sql_orderidx.h"
#include "sql_strimps.h"
#include "sql_subquery.h"
#include "sql_statistics.h"
#include "sql_transaction.h"
#include "for.h"
#include "dict.h"
#include "mel.h"


static str
SQLuser_password(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = NULL;
	str *password = getArgReference_str(stk, pci, 0);
	const char *username = *getArgReference_str(stk, pci, 1);

	(void) password;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (cntxt->username != username) {
		// only MAL_ADMIN and user himself can access password
		if ((msg = AUTHrequireAdmin(cntxt)) != MAL_SUCCEED)
			return msg;
	}
	*password = monet5_password_hash(m, username);
	if (!(*password))
		throw(SQL, "mvc", SQLSTATE(42000) "SELECT: Failed to retrieve password hash");
	return MAL_SUCCEED;
}

static str
SQLdecypher(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = NULL;
	str *pwhash = getArgReference_str(stk, pci, 0);
	const char *cypher = *getArgReference_str(stk, pci, 1);

	(void) pwhash;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	return AUTHdecypherValue(pwhash, cypher);
}

static str
SQLcheck(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = NULL;
	str *r = getArgReference_str(stk, pci, 0);
	const char *sname = *getArgReference_str(stk, pci, 1);
	const char *kname = *getArgReference_str(stk, pci, 2);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	(void)sname;
	sql_schema *s = mvc_bind_schema(m, sname);
	if (s) {
		sql_key *k = mvc_bind_key(m, s, kname);
		uintptr_t sp = m->sp;
#ifdef __has_builtin
#if __has_builtin(__builtin_frame_address)
		m->sp = (uintptr_t) __builtin_frame_address(0);
#define BUILTIN_USED
#endif
#endif
#ifndef BUILTIN_USED
		m->sp = (uintptr_t)(&m);
#endif
#undef BUILTIN_USED
		if (k && k->check) {
			int pos = 0;
			sql_rel *rel = rel_basetable(m, k->t, k->t->base.name);
			sql_exp *exp = NULL;
			if (rel) {
				rel_base_use_all(m, rel);
				exp = exp_read(m, rel, NULL, NULL, sa_strdup(m->sa, k->check), &pos, 0);
			}
			assert(exp);
			if (!exp)
				throw(SQL, "SQLcheck", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			if (exp->comment)
				*r = GDKstrdup(exp->comment);
			else
				*r = GDKstrdup(exp2sql(m, exp));
			if (*r == NULL)
				throw(SQL, "SQLcheck", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			m->sp = sp;
			return MAL_SUCCEED;
		}
		m->sp = sp;
	}
	if (!(*r = GDKstrdup(str_nil)))
		throw(SQL, "SQLcheck", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
SQLread_dump_rel(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = NULL;
	buffer *b = NULL;
	stream *s = NULL;
	char *res = NULL;
	str *r = getArgReference_str(stk, pci, 0);
	char *input = *getArgReference_str(stk, pci, 1);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	list *refs = sa_list(m->sa);
	if (refs == NULL)
		goto bailout;

	int pos = 0;
	sql_rel* rel = rel_read(m, input, &pos, refs);
	if (!rel)
		throw(SQL, "SQLread_dump_rel", SQLSTATE(42000) "failed to read relational plan");

	b = buffer_create(1024);
	if(b == NULL)
		goto bailout;
	s = buffer_wastream(b, "exp_dump");
	if(s == NULL)
		goto bailout;

	refs = sa_list(m->sa);
	if (refs == NULL)
		goto bailout;

	rel_print_refs(m, s, rel, 0, refs, 0);
	rel_print_(m, s, rel, 0, refs, 0);
	res = buffer_get_buf(b);

	if (res == NULL)
		goto bailout;
	if (!(*r = GDKstrdup(res)))
		goto bailout;

	free(res);
	close_stream(s);
	buffer_destroy(b);
	return MAL_SUCCEED;

bailout:
	if (res)
		free(res);
	if (s)
		mnstr_destroy(s);
	if (b)
		buffer_destroy(b);
	throw(SQL, "SQLread_dump_rel", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
SQLnormalize_monetdb_url(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)mb;
	str *ret = getArgReference_str(stk, pci, 0);
	str url = *getArgReference_str(stk, pci, 1);
	allocator *sa;
	backend *be = NULL;
	str msg;
	msettings_error err;
	str normalized;

	if (strNil(url))
		throw(MAL, "SQLnormalize_monetdb_url", SQLSTATE(42000) "url cannot be nil");

	if ((msg = getBackendContext(cntxt, &be)) != NULL)
		return msg;
	sa = be->mvc->sa;

	msettings *mp = sa_msettings_create(sa);
	if (mp == NULL)
		throw(SQL, "SQLnormalize_monetdb_url", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	err = msettings_parse_url(mp, url);
	if (err != NULL)
		throw(SQL, "SQLnormalize_monetdb_url", SQLSTATE(42000) "Invalid URL: %s", err);

	normalized = sa_msettings_to_string(mp, sa, strlen(url));
	if (normalized == NULL)
		throw(SQL, "SQLnormalize_monetdb_url", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	*ret = _STRDUP(normalized);

	return MAL_SUCCEED;
}




static mel_func sql_init_funcs[] = {
 pattern("sql", "shutdown", SQLshutdown_wrap, true, "", args(1,3, arg("",str),arg("delay",bte),arg("force",bit))),
 pattern("sql", "shutdown", SQLshutdown_wrap, true, "", args(1,3, arg("",str),arg("delay",sht),arg("force",bit))),
 pattern("sql", "shutdown", SQLshutdown_wrap, true, "", args(1,3, arg("",str),arg("delay",int),arg("force",bit))),
 pattern("sql", "shutdown", SQLshutdown_wrap, true, "", args(1,2, arg("",str),arg("delay",bte))),
 pattern("sql", "shutdown", SQLshutdown_wrap, true, "", args(1,2, arg("",str),arg("delay",sht))),
 pattern("sql", "shutdown", SQLshutdown_wrap, true, "", args(1,2, arg("",str),arg("delay",int))),
 pattern("sql", "set_protocol", SQLset_protocol, true, "Configures the result set protocol", args(1,2, arg("",int), arg("protocol",int))),
 pattern("sql", "mvc", SQLmvc, false, "Get the multiversion catalog context. \nNeeded for correct statement dependencies\n(ie sql.update, should be after sql.bind in concurrent execution)", args(1,1, arg("",int))),
 pattern("sql", "eval", SQLstatement, true, "Compile and execute a single sql statement", args(1,2, arg("",void),arg("cmd",str))),
 pattern("sql", "eval", SQLstatement, true, "Compile and execute a single sql statement (and optionally set the output to columnar format)", args(1,3, arg("",void),arg("cmd",str),arg("columnar",bit))),
 pattern("sql", "include", SQLinclude, true, "Compile and execute a sql statements on the file", args(1,2, arg("",void),arg("fname",str))),
 pattern("sql", "evalAlgebra", RAstatement, true, "Compile and execute a single 'relational algebra' statement", args(1,3, arg("",void),arg("cmd",str),arg("optimize",bit))),
 pattern("sql", "register", RAstatement2, true, "", args(1,5, arg("",int),arg("mod",str),arg("fname",str),arg("rel_stmt",str),arg("sig",str))),
 pattern("sql", "register", RAstatement2, true, "Compile the relational statement (rel_smt) and register it as mal function, mod.fname(signature)", args(1,6, arg("",int),arg("mod",str),arg("fname",str),arg("rel_stmt",str),arg("sig",str),arg("typ",str))),
 pattern("sql", "deregister", RAstatementEnd, true, "Finish running transaction", args(1,1, arg("",int))),
 pattern("sql", "hot_snapshot", SQLhot_snapshot, true, "Write db snapshot to the given tar(.gz) file", args(1,2, arg("",void),arg("tarfile",str))),
 pattern("sql", "resume_log_flushing", SQLresume_log_flushing, true, "Resume WAL log flushing", args(1,1, arg("",void))),
 pattern("sql", "suspend_log_flushing", SQLsuspend_log_flushing, true, "Suspend WAL log flushing", args(1,1, arg("",void))),
 pattern("sql", "hot_snapshot", SQLhot_snapshot, true, "Write db snapshot to the given tar(.gz/.lz4/.bz/.xz) file on either server or client", args(1,3, arg("",void),arg("tarfile", str),arg("onserver",bit))),
 pattern("sql", "persist_unlogged", SQLpersist_unlogged, true, "Persist deltas on append only table in schema s table t", args(3, 5, batarg("table", str), batarg("table_id", int), batarg("rowcount", lng), arg("s", str), arg("t", str))),
 pattern("sql", "hot_snapshot", SQLhot_snapshot, true, "Write db snapshot to the given tar(.gz/.lz4/.bz/.xz) file on either server or client, omitting some bats", args(1,4, arg("",void),arg("tarfile",str),arg("onserver",bit),arg("omitunlogged",bit))),
 pattern("sql", "hot_snapshot", SQLhot_snapshot, true, "Write db snapshot to the given tar(.gz/.lz4/.bz/.xz) file on either server or client, omitting some bats", args(1,5, arg("",void),arg("tarfile",str),arg("onserver",bit),arg("omitunlogged",bit),arg("omitids",str))),
 pattern("sql", "assert", SQLassert, false, "Generate an exception when b==true", args(1,3, arg("",void),arg("b",bit),arg("msg",str))),
 pattern("sql", "assert", SQLassertInt, false, "Generate an exception when b!=0", args(1,3, arg("",void),arg("b",int),arg("msg",str))),
 pattern("sql", "assert", SQLassertLng, false, "Generate an exception when b!=0", args(1,3, arg("",void),arg("b",lng),arg("msg",str))),
 pattern("sql", "setVariable", setVariable, true, "Set the value of a session variable", args(1,5, arg("",int),arg("mvc",int),arg("sname",str),arg("varname",str),argany("value",1))),
 pattern("sql", "getVariable", getVariable, false, "Get the value of a session variable", args(1,4, argany("",1),arg("mvc",int),arg("sname",str),arg("varname",str))),
 pattern("sql", "logfile", mvc_logfile, true, "Enable/disable saving the sql statement traces", args(1,2, arg("",void),arg("filename",str))),
 pattern("sql", "next_value", mvc_next_value, true, "return the next value of the sequence", args(1,3, arg("",lng),arg("sname",str),arg("sequence",str))),
 pattern("batsql", "next_value", mvc_next_value_bulk, true, "return the next value of the sequence", args(1,4, batarg("",lng),arg("card",lng), arg("sname",str),arg("sequence",str))),
 pattern("sql", "get_value", mvc_get_value, false, "return the current value of the sequence (ie the next to be used value)", args(1,3, arg("",lng),arg("sname",str),arg("sequence",str))),
 pattern("batsql", "get_value", mvc_get_value_bulk, false, "return the current value of the sequence (ie the next to be used value)", args(1,3, batarg("",lng),batarg("sname",str),batarg("sequence",str))),
 pattern("batsql", "get_value", mvc_get_value_bulk, false, "return the current value of the sequence (ie the next to be used value)", args(1,5, batarg("",lng),batarg("sname",str),batarg("sequence",str),batarg("s1",oid),batarg("s2",oid))),
 pattern("sql", "restart", mvc_restart_seq, true, "restart the sequence with value start", args(1,4, arg("",lng),arg("sname",str),arg("sequence",str),arg("start",lng))),
 pattern("sql", "deltas", mvc_delta_values, false, "Return the delta values sizes of all columns of the schema's tables, plus the current transaction level", args(7,8, batarg("ids",int),batarg("segments",lng),batarg("all",lng),batarg("inserted",lng),batarg("updated",lng),batarg("deleted",lng),batarg("tr_level",int),arg("schema",str))),
 pattern("sql", "deltas", mvc_delta_values, false, "Return the delta values sizes from the table's columns, plus the current transaction level", args(7,9, batarg("ids",int),batarg("segments",lng),batarg("all",lng),batarg("inserted",lng),batarg("updated",lng),batarg("deleted",lng),batarg("tr_level",int),arg("schema",str),arg("table",str))),
 pattern("sql", "deltas", mvc_delta_values, false, "Return the delta values sizes of a column, plus the current transaction level", args(7,10, batarg("ids",int),batarg("segments",lng),batarg("all",lng),batarg("inserted",lng),batarg("updated",lng),batarg("deleted",lng),batarg("tr_level",int),arg("schema",str),arg("table",str),arg("column",str))),
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
 command("sql", "delta", DELTAbat, false, "Return column bat with delta's applied.", args(1,4, batargany("",1),batargany("col",1),batarg("uid",oid),batargany("uval",1))),
 command("sql", "projectdelta", DELTAproject, false, "Return column bat with delta's applied.", args(1,5, batargany("",1),batarg("select",oid),batargany("col",1),batarg("uid",oid),batargany("uval",1))),
 command("sql", "subdelta", DELTAsub, false, "Return a single bat of selected delta.", args(1,5, batarg("",oid),batarg("col",oid),batarg("cand",oid),batarg("uid",oid),batarg("uval",oid))),
 command("sql", "project", BATleftproject, false, "Last step of a left outer join, ie project the inner join (l,r) over the left input side (col)", args(1,4, batarg("",oid),batarg("col",oid),batarg("l",oid),batarg("r",oid))),
 command("sql", "getVersion", mvc_getVersion, false, "Return the database version identifier for a client.", args(1,2, arg("",lng),arg("clientid",int))),
 pattern("sql", "grow", mvc_grow_wrap, false, "Resize the tid column of a declared table.", args(1,3, arg("",int),batarg("tid",oid),argany("",1))),
 pattern("sql", "claim", mvc_claim_wrap, true, "Claims slots for appending rows.", args(2,6, arg("",oid),batarg("",oid),arg("mvc",int),arg("sname",str),arg("tname",str),arg("cnt",lng))),
 pattern("sql", "depend", mvc_add_dependency_change, true, "Set dml dependency on current transaction for a table.", args(0,3, arg("sname",str),arg("tname",str),arg("cnt",lng))),
 pattern("sql", "predicate", mvc_add_column_predicate, true, "Add predicate on current transaction for a table column.", args(0,3, arg("sname",str),arg("tname",str),arg("cname",str))),
 pattern("sql", "append", mvc_append_wrap, false, "Append to the column tname.cname (possibly optimized to replace the insert bat of tname.cname. Returns sequence number for order dependence.", args(1,8, arg("",int), arg("mvc",int),arg("sname",str),arg("tname",str),arg("cname",str),arg("offset",oid),batarg("pos",oid),argany("ins",0))),
 pattern("sql", "update", mvc_update_wrap, false, "Update the values of the column tname.cname. Returns sequence number for order dependence)", args(1,7, arg("",int), arg("mvc",int),arg("sname",str),arg("tname",str),arg("cname",str),argany("rids",0),argany("upd",0))),
 pattern("sql", "clear_table", mvc_clear_table_wrap, true, "Clear the table sname.tname.", args(1,4, arg("",lng),arg("sname",str),arg("tname",str),arg("restart_sequences",int))),
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
 pattern("sql", "export_bin_column", mvc_bin_export_column_wrap, true, "export column as binary", args(1, 5, arg("", lng), batargany("col", 1), arg("byteswap", bit), arg("filename", str), arg("onclient", int))),
 pattern("sql", "export_bin_column", mvc_bin_export_column_wrap, true, "export column as binary", args(1, 5, arg("", lng), argany("val", 1), arg("byteswap", bit), arg("filename", str), arg("onclient", int))),
 pattern("sql", "affectedRows", mvc_affected_rows_wrap, true, "export the number of affected rows by the current query", args(1,3, arg("",int),arg("mvc",int),arg("nr",lng))),
 pattern("sql", "copy_from", mvc_import_table_wrap, true, "Import a table from bstream s with the \ngiven tuple and separators (sep/rsep)", args(1,15, batvarargany("",0),arg("t",ptr),arg("sep",str),arg("rsep",str),arg("ssep",str),arg("ns",str),arg("fname",str),arg("nr",lng),arg("offset",lng),arg("best",int),arg("fwf",str),arg("onclient",int),arg("escape",int),arg("decsep",str),arg("decskip",str))),
 //we use bat.single now
 //pattern("sql", "single", CMDBATsingle, false, "", args(1,2, batargany("",2),argany("x",2))),
 pattern("sql", "importColumn", mvc_bin_import_column_wrap, false, "Import a column from the given file", args(2, 8, batargany("", 0),arg("", oid), arg("method",str),arg("width",int),arg("bswap",bit),arg("path",str),arg("onclient",int),arg("nrows",oid))),
 command("aggr", "not_unique", not_unique, false, "check if the tail sorted bat b doesn't have unique tail values", args(1,2, arg("",bit),batarg("b",oid))),
 command("sql", "optimizers", getPipeCatalog, false, "", args(3,3, batarg("",str),batarg("",str),batarg("",str))),
 pattern("sql", "optimizer_updates", SQLoptimizersUpdate, false, "", noargs),
 pattern("sql", "argRecord", SQLargRecord, false, "Glue together the calling sequence", args(1,1, arg("",str))),
 pattern("sql", "argRecord", SQLargRecord, false, "Glue together the calling sequence", args(1,2, arg("",str),varargany("a",0))),
 pattern("sql", "sql_variables", sql_variables, false, "return the table with session variables", args(4,4, batarg("sname",str),batarg("name",str),batarg("type",str),batarg("value",str))),
 pattern("sql", "sessions", sql_sessions_wrap, false, "SQL export table of active sessions, their timeouts and idle status",args(16,16,batarg("id",int),batarg("user",str),batarg("start",timestamp),batarg("idle",timestamp),batarg("optimizer",str),batarg("stimeout",int),batarg("qtimeout",int),batarg("wlimit",int),batarg("mlimit",int),batarg("language", str),batarg("peer", str),batarg("hostname", str),batarg("application", str),batarg("client", str),batarg("clientpid", lng),batarg("remark", str),)),
 pattern("sql", "unclosed_result_sets", sql_unclosed_result_sets, false, "return query_id/res_id of unclosed result sets", args(2,2, batarg("query_id",oid),batarg("res_id", int))),
 pattern("sql", "password", SQLuser_password, false, "Return password hash of user", args(1,2, arg("",str),arg("user",str))),
 pattern("sql", "decypher", SQLdecypher, false, "Return decyphered password", args(1,2, arg("",str),arg("hash",str))),
 pattern("sql", "dump_cache", dump_cache, false, "dump the content of the query cache", args(2,2, batarg("query",str),batarg("count",int))),
 pattern("sql", "dump_opt_stats", dump_opt_stats, false, "dump the optimizer rewrite statistics", args(2,2, batarg("rewrite",str),batarg("count",int))),
 pattern("sql", "dump_trace", dump_trace, false, "dump the trace statistics", args(3,3, batarg("ticks",lng),batarg("stmt",str),batarg("stmt",str))),
 pattern("sql", "analyze", sql_analyze, true, "Update statistics for every column in the database", args(1,1, arg("",void))),
 pattern("sql", "analyze", sql_analyze, true, "Update statistics for schema", args(1,2, arg("",void),arg("sch",str))),
 pattern("sql", "analyze", sql_analyze, true, "Update statistics for table", args(1,3, arg("",void),arg("sch",str),arg("tbl",str))),
 pattern("sql", "analyze", sql_analyze, true, "Update statistics for column", args(1,4, arg("",void),arg("sch",str),arg("tbl",str),arg("col",str))),
 pattern("sql", "set_count_distinct", sql_set_count_distinct, true, "Set count distinct for column", args(1,5, arg("",void),arg("sch",str),arg("tbl",str),arg("col",str),arg("val",lng))),
 pattern("sql", "set_min", sql_set_min, true, "Set min for column", args(1,5, arg("",void),arg("sch",str),arg("tbl",str),arg("col",str),argany("val",1))),
 pattern("sql", "set_max", sql_set_max, true, "Set max for column", args(1,5, arg("",void),arg("sch",str),arg("tbl",str),arg("col",str),argany("val",1))),
 pattern("sql", "statistics", sql_statistics, false, "return a table with statistics information", args(13,13, batarg("columnid",int),batarg("schema",str),batarg("table",str),batarg("column",str),batarg("type",str),batarg("with",int),batarg("count",lng),batarg("unique",bit),batarg("nils",bit),batarg("minval",str),batarg("maxval",str),batarg("sorted",bit),batarg("revsorted",bit))),
 pattern("sql", "statistics", sql_statistics, false, "return a table with statistics information for a particular schema", args(13,14, batarg("columnid",int),batarg("schema",str),batarg("table",str),batarg("column",str),batarg("type",str),batarg("with",int),batarg("count",lng),batarg("unique",bit),batarg("nils",bit),batarg("minval",str),batarg("maxval",str),batarg("sorted",bit),batarg("revsorted",bit),arg("sname",str))),
 pattern("sql", "statistics", sql_statistics, false, "return a table with statistics information for a particular table", args(13,15, batarg("columnid",int),batarg("schema",str),batarg("table",str),batarg("column",str),batarg("type",str),batarg("with",int),batarg("count",lng),batarg("unique",bit),batarg("nils",bit),batarg("minval",str),batarg("maxval",str),batarg("sorted",bit),batarg("revsorted",bit),arg("sname",str),arg("tname",str))),
 pattern("sql", "statistics", sql_statistics, false, "return a table with statistics information for a particular column", args(13,16, batarg("columnid",int),batarg("schema",str),batarg("table",str),batarg("column",str),batarg("type",str),batarg("with",int),batarg("count",lng),batarg("unique",bit),batarg("nils",bit),batarg("minval",str),batarg("maxval",str),batarg("sorted",bit),batarg("revsorted",bit),arg("sname",str),arg("tname",str),arg("cname",str))),
 pattern("sql", "storage", sql_storage, false, "return a table with storage information ", args(17,17, batarg("schema",str),batarg("table",str),batarg("column",str),batarg("type",str),batarg("mode",str),batarg("location",str),batarg("count",lng),batarg("atomwidth",int),batarg("columnsize",lng),batarg("heap",lng),batarg("hashes",lng),batarg("phash",bit),batarg("imprints",lng),batarg("sorted",bit),batarg("revsorted",bit),batarg("key",bit),batarg("orderidx",lng))),
 pattern("sql", "storage", sql_storage, false, "return a table with storage information for a particular schema ", args(17,18, batarg("schema",str),batarg("table",str),batarg("column",str),batarg("type",str),batarg("mode",str),batarg("location",str),batarg("count",lng),batarg("atomwidth",int),batarg("columnsize",lng),batarg("heap",lng),batarg("hashes",lng),batarg("phash",bit),batarg("imprints",lng),batarg("sorted",bit),batarg("revsorted",bit),batarg("key",bit),batarg("orderidx",lng),arg("sname",str))),
 pattern("sql", "storage", sql_storage, false, "return a table with storage information for a particular table", args(17,19, batarg("schema",str),batarg("table",str),batarg("column",str),batarg("type",str),batarg("mode",str),batarg("location",str),batarg("count",lng),batarg("atomwidth",int),batarg("columnsize",lng),batarg("heap",lng),batarg("hashes",lng),batarg("phash",bit),batarg("imprints",lng),batarg("sorted",bit),batarg("revsorted",bit),batarg("key",bit),batarg("orderidx",lng),arg("sname",str),arg("tname",str))),
 pattern("sql", "storage", sql_storage, false, "return a table with storage information for a particular column", args(17,20, batarg("schema",str),batarg("table",str),batarg("column",str),batarg("type",str),batarg("mode",str),batarg("location",str),batarg("count",lng),batarg("atomwidth",int),batarg("columnsize",lng),batarg("heap",lng),batarg("hashes",lng),batarg("phash",bit),batarg("imprints",lng),batarg("sorted",bit),batarg("revsorted",bit),batarg("key",bit),batarg("orderidx",lng),arg("sname",str),arg("tname",str),arg("cname",str))),
 pattern("sql", "createorderindex", sql_createorderindex, true, "Instantiate the order index on a column", args(0,3, arg("sch",str),arg("tbl",str),arg("col",str))),
 pattern("sql", "droporderindex", sql_droporderindex, true, "Drop the order index on a column", args(0,3, arg("sch",str),arg("tbl",str),arg("col",str))),
 pattern("sql", "createstrimps", sql_createstrimps, true, "Instantiate the strimps index on a column", args(0,3, arg("sch",str),arg("tbl",str),arg("col",str))),
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
 pattern("for", "compress", FORcompress_col, false, "compress a sql column", args(0, 3, arg("schema", str), arg("table", str), arg("column", str))),
 pattern("for", "decompress", FORdecompress, false, "decompress a for compressed (sub)column", args(1, 3, batargany("", 1), batargany("o", 0), argany("minval", 1))),
 pattern("dict", "compress", DICTcompress, false, "dict compress a bat", args(2, 3, batargany("o", 0), batargany("v", 1), batargany("b", 1))),
 pattern("dict", "compress", DICTcompress_col, false, "compress a sql column", args(0, 3, arg("schema", str), arg("table", str), arg("column", str))),
 pattern("dict", "compress", DICTcompress_col, false, "compress a sql column", args(0, 4, arg("schema", str), arg("table", str), arg("column", str), arg("ordered", bit))),
 pattern("dict", "decompress", DICTdecompress, false, "decompress a dictionary compressed (sub)column", args(1, 3, batargany("", 1), batargany("o", 0), batargany("u", 1))),
 pattern("dict", "convert", DICTconvert, false, "convert candidate list into compressed offsets", args(1, 2, batargany("", 1), batargany("o", 0))),
 pattern("dict", "join", DICTjoin, false, "join 2 dictionaries", args(2, 10, batarg("r0", oid), batarg("r1", oid), batargany("lo", 0), batargany("lv", 1), batargany("ro", 0), batargany("rv", 1), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng))),
 pattern("dict", "thetaselect", DICTthetaselect, false, "thetaselect on a dictionary", args(1, 6, batarg("r0", oid), batargany("lo", 0), batarg("lc", oid), batargany("lv", 1), argany("val",1), arg("op", str))),
 pattern("dict", "renumber", DICTrenumber, false, "renumber offsets", args(1, 3, batargany("n", 1), batargany("o", 1), batargany("r", 1))),
 pattern("dict", "select", DICTselect, false, "value - range select on a dictionary", args(1, 10, batarg("r0", oid), batargany("lo", 0), batarg("lc", oid), batargany("lv", 1), argany("l", 1), argany("h", 1), arg("li", bit), arg("hi", bit), arg("anti", bit),  arg("unknown", bit))),
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
 pattern("calc", "timestamp", str_2time_timestamp, false, "cast to timestamp and check for overflow", args(1,3, arg("",timestamp),arg("v",str),arg("digits",int))),
 pattern("calc", "timestamptz", str_2time_timestamptz, false, "cast to timestamp and check for overflow", args(1,4, arg("",timestamp),arg("v",str),arg("digits",int),arg("tz_msec",lng))),
 pattern("calc", "timestamp", timestamp_2time_timestamp, false, "cast timestamp to timestamp and check for overflow", args(1,3, arg("",timestamp),arg("v",timestamp),arg("digits",int))),
 command("batcalc", "timestamp", batstr_2time_timestamp, false, "cast to timestamp and check for overflow", args(1,4, batarg("",timestamp),batarg("v",str),batarg("s",oid),arg("digits",int))),
 command("batcalc", "timestamptz", batstr_2time_timestamptz, false, "cast to timestamp and check for overflow", args(1,5, batarg("",timestamp),batarg("v",str),batarg("s",oid),arg("digits",int),arg("tz_msec",lng))),
 pattern("batcalc", "timestamp", timestamp_2time_timestamp, false, "cast timestamp to timestamp and check for overflow", args(1,4, batarg("",timestamp),batarg("v",timestamp),batarg("s",oid),arg("digits",int))),
 pattern("batcalc", "daytime", nil_2time_daytime, false, "cast to daytime and check for overflow", args(1,3, batarg("",daytime),batarg("v",oid),arg("digits",int))),
 pattern("calc", "daytime", str_2time_daytime, false, "cast to daytime and check for overflow", args(1,3, arg("",daytime),arg("v",str),arg("digits",int))),
 pattern("calc", "daytimetz", str_2time_daytimetz, false, "cast to daytime and check for overflow", args(1,4, arg("",daytime),arg("v",str),arg("digits",int),arg("tz_msec",lng))),
 pattern("calc", "daytime", daytime_2time_daytime, false, "cast daytime to daytime and check for overflow", args(1,3, arg("",daytime),arg("v",daytime),arg("digits",int))),
 command("batcalc", "daytime", batstr_2time_daytime, false, "cast to daytime and check for overflow", args(1,4, batarg("",daytime),batarg("v",str),batarg("s",oid),arg("digits",int))),
 pattern("batcalc", "daytimetz", str_2time_daytimetz, false, "cast daytime to daytime and check for overflow", args(1,5, batarg("",daytime),batarg("v",str),batarg("s",oid),arg("digits",int),arg("tz_msec",lng))),
 pattern("batcalc", "daytime", daytime_2time_daytime, false, "cast daytime to daytime and check for overflow", args(1,4, batarg("",daytime),batarg("v",daytime),batarg("s",oid),arg("digits",int))),
 command("sql", "date_trunc", bat_date_trunc, false, "Truncate a timestamp to (millennium, century,decade,year,quarter,month,week,day,hour,minute,second, milliseconds,microseconds)", args(1,3, batarg("",timestamp),arg("scale",str),batarg("v",timestamp))),
 command("sql", "date_trunc", date_trunc, false, "Truncate a timestamp to (millennium, century,decade,year,quarter,month,week,day,hour,minute,second, milliseconds,microseconds)", args(1,3, arg("",timestamp),arg("scale",str),arg("v",timestamp))),
 pattern("sql", "current_time", SQLcurrent_daytime, false, "Get the clients current daytime", args(1,1, arg("",daytime))),
 pattern("sql", "current_timestamp", SQLcurrent_timestamp, false, "Get the clients current timestamp", args(1,1, arg("",timestamp))),
 pattern("calc", "date", nil_2_date, false, "cast to date", args(1,2, arg("",date),arg("v",void))),
 pattern("batcalc", "date", nil_2_date, false, "cast to date", args(1,2, batarg("",date),batarg("v",oid))),
 pattern("calc", "str", SQLstr_cast, false, "cast to string and check for overflow", args(1,7, arg("",str),arg("eclass",int),arg("d1",int),arg("s1",int),arg("has_tz",int),argany("v",1),arg("digits",int))),
 pattern("batcalc", "str", SQLbatstr_cast, false, "cast to string and check for overflow, no candidate list", args(1,7, batarg("",str),arg("eclass",int),arg("d1",int),arg("s1",int),arg("has_tz",int),batargany("v",1),arg("digits",int))),
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
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),optbatarg("limit",bte))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, arg("",oid),arg("p",bit),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",bte))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),optbatarg("limit",bte))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, arg("",oid),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",sht))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),optbatarg("limit",sht))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, arg("",oid),arg("p",bit),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",sht))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),optbatarg("limit",sht))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, arg("",oid),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",int))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),optbatarg("limit",int))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, arg("",oid),arg("p",bit),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",int))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),optbatarg("limit",int))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, arg("",oid),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",lng))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),optbatarg("limit",lng))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, arg("",oid),arg("p",bit),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",lng))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),optbatarg("limit",lng))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, arg("",oid),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",flt))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),optbatarg("limit",flt))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, arg("",oid),arg("p",bit),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",flt))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),optbatarg("limit",flt))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, arg("",oid),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",dbl))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),optbatarg("limit",dbl))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, arg("",oid),arg("p",bit),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",dbl))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),optbatarg("limit",dbl))),
 pattern("sql", "row_number", SQLrow_number, false, "return the row_numer-ed groups", args(1,4, arg("",int),argany("b",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "row_number", SQLrow_number, false, "return the row_numer-ed groups", args(1,4, batarg("",int),batargany("b",1),optbatarg("p",bit),optbatarg("o",bit))),
 pattern("sql", "rank", SQLrank, false, "return the ranked groups", args(1,4, arg("",int),argany("b",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "rank", SQLrank, false, "return the ranked groups", args(1,4, batarg("",int),batargany("b",1),optbatarg("p",bit),optbatarg("o",bit))),
 pattern("sql", "dense_rank", SQLdense_rank, false, "return the densely ranked groups", args(1,4, arg("",int),argany("b",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "dense_rank", SQLdense_rank, false, "return the densely ranked groups", args(1,4, batarg("",int),batargany("b",1),optbatarg("p",bit),optbatarg("o",bit))),
 pattern("sql", "percent_rank", SQLpercent_rank, false, "return the percentage into the total number of groups for each row", args(1,4, arg("",dbl),argany("b",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "percent_rank", SQLpercent_rank, false, "return the percentage into the total number of groups for each row", args(1,4, batarg("",dbl),batargany("b",1),optbatarg("p",bit),optbatarg("o",bit))),
 pattern("sql", "cume_dist", SQLcume_dist, false, "return the accumulated distribution of the number of rows per group to the total number of partition rows", args(1,4, arg("",dbl),argany("b",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "cume_dist", SQLcume_dist, false, "return the accumulated distribution of the number of rows per group to the total number of partition rows", args(1,4, batarg("",dbl),batargany("b",1),optbatarg("p",bit),optbatarg("o",bit))),
 pattern("sql", "lag", SQLlag, false, "return the value in the previous row in the partition or NULL if non existent", args(1,4, argany("",1),argany("b",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "lag", SQLlag, false, "return the value in the previous row in the partition or NULL if non existent", args(1,4, batargany("",1),batargany("b",1),optbatarg("p",bit),optbatarg("o",bit))),
 pattern("sql", "lag", SQLlag, false, "return the value in the previous 'l' row in the partition or NULL if non existent", args(1,5, argany("",1),argany("b",1),argany("l",0),arg("p",bit),arg("o",bit))),
 pattern("batsql", "lag", SQLlag, false, "return the value in the previous 'l' row in the partition or NULL if non existent", args(1,5, batargany("",1),optbatargany("b",1),optbatargany("l",0),optbatarg("p",bit),optbatarg("o",bit))),
 pattern("sql", "lag", SQLlag, false, "return the value in the previous 'l' row in the partition or 'd' if non existent", args(1,6, argany("",1),argany("b",1),argany("l",0),argany("d",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "lag", SQLlag, false, "return the value in the previous 'l' row in the partition or 'd' if non existent", args(1,6, batargany("",1),optbatargany("b",1),optbatargany("l",0),optbatargany("d",1),optbatarg("p",bit),optbatarg("o",bit))),
 pattern("sql", "lead", SQLlead, false, "return the value in the next row in the partition or NULL if non existent", args(1,4, argany("",1),argany("b",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "lead", SQLlead, false, "return the value in the next row in the partition or NULL if non existent", args(1,4, batargany("",1),batargany("b",1),optbatarg("p",bit),optbatarg("o",bit))),
 pattern("sql", "lead", SQLlead, false, "return the value in the next 'l' row in the partition or NULL if non existent", args(1,5, argany("",1),argany("b",1),argany("l",0),arg("p",bit),arg("o",bit))),
 pattern("batsql", "lead", SQLlead, false, "return the value in the next 'l' row in the partition or NULL if non existent", args(1,5, batargany("",1),optbatargany("b",1),optbatargany("l",0),optbatarg("p",bit),optbatarg("o",bit))),
 pattern("sql", "lead", SQLlead, false, "return the value in the next 'l' row in the partition or 'd' if non existent", args(1,6, argany("",1),argany("b",1),argany("l",0),argany("d",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "lead", SQLlead, false, "return the value in the next 'l' row in the partition or 'd' if non existent", args(1,6, batargany("",1),optbatargany("b",1),optbatargany("l",0),optbatargany("d",1),optbatarg("p",bit),optbatarg("o",bit))),
 pattern("sql", "ntile", SQLntile, false, "return the groups divided as equally as possible", args(1,5, argany("",1),argany("b",0),argany("n",1),arg("p",bit),arg("o",bit))),
 pattern("batsql", "ntile", SQLntile, false, "return the groups divided as equally as possible", args(1,5, batargany("",1),optbatargany("b",0),optbatargany("n",1),optbatarg("p",bit),optbatarg("o",bit))),
 /* these window functions support frames */
 pattern("sql", "first_value", SQLfirst_value, false, "return the first value of groups", args(1,7, argany("",1),argany("b",1),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "first_value", SQLfirst_value, false, "return the first value of groups", args(1,7, batargany("",1),batargany("b",1),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "last_value", SQLlast_value, false, "return the last value of groups", args(1,7, argany("",1),argany("b",1),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "last_value", SQLlast_value, false, "return the last value of groups", args(1,7, batargany("",1),batargany("b",1),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "nth_value", SQLnth_value, false, "return the nth value of each group", args(1,8, argany("",1),argany("b",1),arg("n",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "nth_value", SQLnth_value, false, "return the nth value of each group", args(1,8, batargany("",1),optbatargany("b",1),optbatarg("n",lng),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "min", SQLmin, false, "return the minimum of groups", args(1,7, argany("",1),argany("b",1),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "min", SQLmin, false, "return the minimum of groups", args(1,7, batargany("",1),batargany("b",1),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "max", SQLmax, false, "return the maximum of groups", args(1,7, argany("",1),argany("b",1),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "max", SQLmax, false, "return the maximum of groups",args(1,7, batargany("",1),batargany("b",1),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "count", SQLbasecount, false, "return count of basetable", args(1,3, arg("",lng),arg("sname",str),arg("tname",str))),
 pattern("sql", "count", SQLcount, false, "return count of groups", args(1,8, arg("",lng),argany("b",1),arg("ignils",bit),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "count", SQLcount, false,"return count of groups",args(1,8, batarg("",lng),batargany("b",1),arg("ignils",bit),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",lng),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",lng),batarg("b",bte),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",lng),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",lng),batarg("b",sht),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",lng),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",lng),batarg("b",int),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",lng),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",lng),batarg("b",lng),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",flt),arg("b",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",flt),batarg("b",flt),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",dbl),arg("b",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",dbl),batarg("b",flt),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",dbl),arg("b",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",dbl),batarg("b",dbl),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 /* sql.sum for month intervals */
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",int),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",int),batarg("b",int),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",lng),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",lng),batarg("b",bte),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",lng),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",lng),batarg("b",sht),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",lng),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",lng),batarg("b",int),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",lng),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",lng),batarg("b",lng),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",flt),arg("b",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",flt),batarg("b",flt),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",dbl),arg("b",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",dbl),batarg("b",flt),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",dbl),arg("b",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",dbl),batarg("b",dbl),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "avg", SQLavg, false, "return the average of groups", args(1,7, arg("",dbl),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavg, false, "return the average of groups", args(1,7, batarg("",dbl),batarg("b",bte),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "avg", SQLavg, false, "return the average of groups", args(1,7, arg("",dbl),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavg, false, "return the average of groups", args(1,7, batarg("",dbl),batarg("b",sht),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "avg", SQLavg, false, "return the average of groups", args(1,7, arg("",dbl),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavg, false, "return the average of groups", args(1,7, batarg("",dbl),batarg("b",int),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "avg", SQLavg, false, "return the average of groups", args(1,7, arg("",dbl),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavg, false, "return the average of groups", args(1,7, batarg("",dbl),batarg("b",lng),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "avg", SQLavg, false, "return the average of groups", args(1,7, arg("",dbl),arg("b",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavg, false, "return the average of groups", args(1,7, batarg("",dbl),batarg("b",flt),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "avg", SQLavg, false, "return the average of groups", args(1,7, arg("",dbl),arg("b",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavg, false, "return the average of groups", args(1,7, batarg("",dbl),batarg("b",dbl),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, arg("",bte),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, batarg("",bte),batarg("b",bte),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, arg("",sht),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, batarg("",sht),batarg("b",sht),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, arg("",int),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, batarg("",int),batarg("b",int),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, arg("",lng),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, batarg("",lng),batarg("b",lng),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, arg("",dbl),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, batarg("",dbl),batarg("b",bte),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, arg("",dbl),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, batarg("",dbl),batarg("b",sht),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, arg("",dbl),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, batarg("",dbl),batarg("b",int),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, arg("",dbl),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, batarg("",dbl),batarg("b",lng),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, arg("",dbl),arg("b",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, batarg("",dbl),batarg("b",flt),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, arg("",dbl),arg("b",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, batarg("",dbl),batarg("b",dbl),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, arg("",dbl),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, batarg("",dbl),batarg("b",bte),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, arg("",dbl),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, batarg("",dbl),batarg("b",sht),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, arg("",dbl),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, batarg("",dbl),batarg("b",int),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, arg("",dbl),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, batarg("",dbl),batarg("b",lng),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, arg("",dbl),arg("b",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, batarg("",dbl),batarg("b",flt),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, arg("",dbl),arg("b",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, batarg("",dbl),batarg("b",dbl),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, arg("",dbl),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, batarg("",dbl),batarg("b",bte),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, arg("",dbl),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, batarg("",dbl),batarg("b",sht),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, arg("",dbl),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, batarg("",dbl),batarg("b",int),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, arg("",dbl),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, batarg("",dbl),batarg("b",lng),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, arg("",dbl),arg("b",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, batarg("",dbl),batarg("b",flt),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, arg("",dbl),arg("b",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, batarg("",dbl),batarg("b",dbl),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, arg("",dbl),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, batarg("",dbl),batarg("b",bte),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, arg("",dbl),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, batarg("",dbl),batarg("b",sht),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, arg("",dbl),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, batarg("",dbl),batarg("b",int),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, arg("",dbl),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, batarg("",dbl),batarg("b",lng),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, arg("",dbl),arg("b",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, batarg("",dbl),batarg("b",flt),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, arg("",dbl),arg("b",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, batarg("",dbl),batarg("b",dbl),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, arg("",dbl),arg("b",bte),arg("c",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),optbatarg("b",bte),optbatarg("c",bte),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, arg("",dbl),arg("b",sht),arg("c",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),optbatarg("b",sht),optbatarg("c",sht),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, arg("",dbl),arg("b",int),arg("c",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),optbatarg("b",int),optbatarg("c",int),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, arg("",dbl),arg("b",lng),arg("c",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),optbatarg("b",lng),optbatarg("c",lng),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, arg("",dbl),arg("b",flt),arg("c",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),optbatarg("b",flt),optbatarg("c",flt),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, arg("",dbl),arg("b",dbl),arg("c",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),optbatarg("b",dbl),optbatarg("c",dbl),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, arg("",dbl),arg("b",bte),arg("c",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),optbatarg("b",bte),optbatarg("c",bte),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, arg("",dbl),arg("b",sht),arg("c",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),optbatarg("b",sht),optbatarg("c",sht),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, arg("",dbl),arg("b",int),arg("c",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),optbatarg("b",int),optbatarg("c",int),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, arg("",dbl),arg("b",lng),arg("c",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),optbatarg("b",lng),optbatarg("c",lng),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, arg("",dbl),arg("b",flt),arg("c",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),optbatarg("b",flt),optbatarg("c",flt),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, arg("",dbl),arg("b",dbl),arg("c",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),optbatarg("b",dbl),optbatarg("c",dbl),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, arg("",dbl),arg("b",bte),arg("c",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),optbatarg("b",bte),optbatarg("c",bte),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, arg("",dbl),arg("b",sht),arg("c",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),optbatarg("b",sht),optbatarg("c",sht),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, arg("",dbl),arg("b",int),arg("c",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),optbatarg("b",int),optbatarg("c",int),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, arg("",dbl),arg("b",lng),arg("c",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),optbatarg("b",lng),optbatarg("c",lng),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, arg("",dbl),arg("b",flt),arg("c",flt),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),optbatarg("b",flt),optbatarg("c",flt),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, arg("",dbl),arg("b",dbl),arg("c",dbl),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),optbatarg("b",dbl),optbatarg("c",dbl),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "str_group_concat", SQLstrgroup_concat, false, "return the string concatenation of groups", args(1,7, arg("",str),arg("b",str),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "str_group_concat", SQLstrgroup_concat, false, "return the string concatenation of groups", args(1,7, batarg("",str),batarg("b",str),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "str_group_concat", SQLstrgroup_concat, false, "return the string concatenation of groups with a custom separator", args(1,8, arg("",str),arg("b",str),arg("sep",str),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "str_group_concat", SQLstrgroup_concat, false, "return the string concatenation of groups with a custom separator", args(1,8, batarg("",str),optbatarg("b",str),optbatarg("sep",str),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 /* sql_subquery */
 command("aggr", "zero_or_one", zero_or_one, false, "if col contains exactly one value return this. In case of more raise an exception else return nil", args(1,2, argany("",1),batargany("col",1))),
 command("aggr", "zero_or_one", zero_or_one_error, false, "if col contains exactly one value return this. In case of more raise an exception if err is true else return nil", args(1,3, argany("",1),batargany("col",1),arg("err",bit))),
 command("aggr", "zero_or_one", zero_or_one_error_bat, false, "if col contains exactly one value return this. In case of more raise an exception if err is true else return nil", args(1,3, argany("",1),batargany("col",1),batarg("err",bit))),
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
 pattern("aggr", "anyequal", SQLanyequal, false, "if any value in r is equal to l, return true, else if r has nil, return nil, else return false", args(1,3, arg("",bit),batargany("l",1),batargany("r",1))),
 pattern("bataggr", "anyequal", SQLanyequal, false, "if any value in r is equal to l, return true, else if r has nil, return nil, else return false", args(1,3, batarg("",bit),batargany("l",1),batargany("r",1))),
 pattern("aggr", "allnotequal", SQLallnotequal, false, "if all values in r are not equal to l, return true, else if r has nil, return nil, else return false", args(1,3, arg("",bit),batargany("l",1),batargany("r",1))),
 pattern("bataggr", "allnotequal", SQLallnotequal, false, "if all values in r are not equal to l, return true, else if r has nil, return nil, else return false", args(1,3, arg("",bit),batargany("l",1),batargany("r",1))),
 pattern("aggr", "subanyequal", SQLanyequal_grp, false, "if any value in r is equal to l, return true, else if r has nil, return nil, else return false", args(1,6, batarg("",bit),batargany("l",1),batargany("r",1),batarg("g",oid),batarg("e",oid),arg("no_nil",bit))),
// pattern("aggr", "subanyequal", SQLanyequal_grp, false, "if any value in r is equal to l, return true, else if r has nil, return nil, else return false; with candidate list", args(1,7, batarg("",bit),batargany("l",1),batargany("r",1),batarg("g",oid),batarg("e",oid),batarg("s",oid),arg("no_nil",bit))),
 pattern("aggr", "subanyequal", SQLanyequal_grp2, false, "if any value in r is equal to l, return true, else if r has nil, return nil, else return false, except if rid is nil (ie empty) then return false", args(1,7, batarg("",bit),batargany("l",1),batargany("r",1),batarg("rid",oid),batarg("g",oid),batarg("e",oid),arg("no_nil",bit))),
 pattern("aggr", "subanyequal", SQLanyequal_grp2, false, "if any value in r is equal to l, return true, else if r has nil, return nil, else return false, except if rid is nil (ie empty) then return false; with candidate list", args(1,8, batarg("",bit),batargany("l",1),batargany("r",1),batarg("rid",oid),batarg("g",oid),batarg("e",oid),batarg("s",oid),arg("no_nil",bit))),
 pattern("aggr", "suballnotequal", SQLallnotequal_grp, false, "if all values in r are not equal to l, return true, else if r has nil, return nil else return false", args(1,6, batarg("",bit),batargany("l",1),batargany("r",1),batarg("g",oid),batarg("e",oid),arg("no_nil",bit))),
// pattern("aggr", "suballnotequal", SQLallnotequal_grp, false, "if all values in r are not equal to l, return true, else if r has nil, return nil else return false; with candidate list", args(1,7, batarg("",bit),batargany("l",1),batargany("r",1),batarg("g",oid),batarg("e",oid),batarg("s",oid),arg("no_nil",bit))),
 pattern("aggr", "suballnotequal", SQLallnotequal_grp2, false, "if all values in r are not equal to l, return true, else if r has nil return nil, else return false, except if rid is nil (ie empty) then return true", args(1,7, batarg("",bit),batargany("l",1),batargany("r",1),batarg("rid",oid),batarg("g",oid),batarg("e",oid),arg("no_nil",bit))),
 pattern("aggr", "suballnotequal", SQLallnotequal_grp2, false, "if all values in r are not equal to l, return true, else if r has nil return nil, else return false, except if rid is nil (ie empty) then return true; with candidate list", args(1,8, batarg("",bit),batargany("l",1),batargany("r",1),batarg("rid",oid),batarg("g",oid),batarg("e",oid),batarg("s",oid),arg("no_nil",bit))),
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
 /* sqlcatalog */
 pattern("sqlcatalog", "create_seq", SQLcreate_seq, false, "Catalog operation create_seq", args(0,4, arg("sname",str),arg("seqname",str),arg("seq",ptr),arg("action",int))),
 pattern("sqlcatalog", "alter_seq", SQLalter_seq, false, "Catalog operation alter_seq", args(0,4, arg("sname",str),arg("seqname",str),arg("seq",ptr),arg("val",lng))),
 pattern("sqlcatalog", "alter_seq", SQLalter_seq, false, "Catalog operation alter_seq", args(0,4, arg("sname",str),arg("seqname",str),arg("seq",ptr),batarg("val",lng))),
 pattern("sqlcatalog", "drop_seq", SQLdrop_seq, false, "Catalog operation drop_seq", args(0,3, arg("sname",str),arg("nme",str),arg("action",int))),
 pattern("sqlcatalog", "create_schema", SQLcreate_schema, false, "Catalog operation create_schema", args(0,3, arg("sname",str),arg("auth",str),arg("action",int))),
 pattern("sqlcatalog", "drop_schema", SQLdrop_schema, false, "Catalog operation drop_schema", args(0,3, arg("sname",str),arg("ifexists",int),arg("action",int))),
 pattern("sqlcatalog", "create_table", SQLcreate_table, false, "Catalog operation create_table", args(0,4, arg("sname",str),arg("tname",str),arg("tbl",ptr),arg("temp",int))),
 pattern("sqlcatalog", "create_table", SQLcreate_table, false, "Catalog operation create_table", args(0,6, arg("sname",str),arg("tname",str),arg("tbl",ptr),arg("pw_encrypted",int),arg("username",str),arg("passwd",str))),
 pattern("sqlcatalog", "create_view", SQLcreate_view, false, "Catalog operation create_view", args(0,5, arg("sname",str),arg("vname",str),arg("tbl",ptr),arg("temp",int),arg("replace",int))),
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
 pattern("sqlcatalog", "create_user", SQLcreate_user, false, "Catalog operation create_user", args(0,10, arg("sname",str),arg("passwrd",str),arg("enc",int),arg("schema",str),arg("schemapath",str),arg("fullname",str), arg("max_memory", lng), arg("max_workers", int), arg("optimizer", str), arg("default_role", str))),
 pattern("sqlcatalog", "drop_user", SQLdrop_user, false, "Catalog operation drop_user", args(0,2, arg("sname",str),arg("action",int))),
 pattern("sqlcatalog", "drop_user", SQLdrop_user, false, "Catalog operation drop_user", args(0,3, arg("sname",str),arg("auth",str),arg("action",int))),
 pattern("sqlcatalog", "alter_user", SQLalter_user, false, "Catalog operation alter_user", args(0,9, arg("sname",str),arg("passwrd",str),arg("enc",int),arg("schema",str),arg("schemapath",str),arg("oldpasswrd",str),arg("role",str),arg("max_memory",lng),arg("max_workers",int))),
 pattern("sqlcatalog", "rename_user", SQLrename_user, false, "Catalog operation rename_user", args(0,3, arg("sname",str),arg("newnme",str),arg("action",int))),
 pattern("sqlcatalog", "create_role", SQLcreate_role, false, "Catalog operation create_role", args(0,3, arg("sname",str),arg("role",str),arg("grator",int))),
 pattern("sqlcatalog", "drop_role", SQLdrop_role, false, "Catalog operation drop_role", args(0,3, arg("auth",str),arg("role",str),arg("action",int))),
 pattern("sqlcatalog", "drop_role", SQLdrop_role, false, "Catalog operation drop_role", args(0,2, arg("role",str),arg("action",int))),
 pattern("sqlcatalog", "drop_index", SQLdrop_index, false, "Catalog operation drop_index", args(0,3, arg("sname",str),arg("iname",str),arg("action",int))),
 pattern("sqlcatalog", "drop_function", SQLdrop_function, false, "Catalog operation drop_function", args(0,5, arg("sname",str),arg("fname",str),arg("fid",int),arg("type",int),arg("action",int))),
 pattern("sqlcatalog", "create_function", SQLcreate_function, false, "Catalog operation create_function", args(0,4, arg("sname",str),arg("fname",str),arg("fcn",ptr),arg("replace",int))),
 pattern("sqlcatalog", "create_trigger", SQLcreate_trigger, false, "Catalog operation create_trigger", args(0,11, arg("sname",str),arg("tname",str),arg("triggername",str),arg("time",int),arg("orientation",int),arg("event",int),arg("old",str),arg("new",str),arg("cond",str),arg("qry",str),arg("replace",int))),
 pattern("sqlcatalog", "drop_trigger", SQLdrop_trigger, false, "Catalog operation drop_trigger", args(0,3, arg("sname",str),arg("nme",str),arg("ifexists",int))),
 pattern("sqlcatalog", "alter_add_table", SQLalter_add_table, false, "Catalog operation alter_add_table", args(0,5, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("action",int))),
 pattern("sqlcatalog", "alter_del_table", SQLalter_del_table, false, "Catalog operation alter_del_table", args(0,5, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("action",int))),
 pattern("sqlcatalog", "alter_set_table", SQLalter_set_table, false, "Catalog operation alter_set_table", args(0,3, arg("sname",str),arg("tnme",str),arg("access",int))),
 pattern("sqlcatalog", "alter_add_range_partition", SQLalter_add_range_partition, false, "Catalog operation alter_add_range_partition", args(0,9, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),argany("min",1),argany("max",1),arg("nills",bit),arg("update",int),arg("assert",lng))),
 pattern("sqlcatalog", "alter_add_range_partition", SQLalter_add_range_partition, false, "Catalog operation alter_add_range_partition", args(0,9, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),argany("min",1),argany("max",1),arg("nills",bit),arg("update",int),batarg("assert",lng))),
 pattern("sqlcatalog", "alter_add_value_partition", SQLalter_add_value_partition, false, "Catalog operation alter_add_value_partition", args(0,7, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("nills",bit),arg("update",int),arg("assert",lng))),
 pattern("sqlcatalog", "alter_add_value_partition", SQLalter_add_value_partition, false, "Catalog operation alter_add_value_partition", args(0,8, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("nills",bit),arg("update",int),arg("assert",lng), varargany("arg",0))),
 pattern("sqlcatalog", "alter_add_value_partition", SQLalter_add_value_partition, false, "Catalog operation alter_add_value_partition", args(0,7, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("nills",bit),arg("update",int),batarg("assert",lng))),
 pattern("sqlcatalog", "alter_add_value_partition", SQLalter_add_value_partition, false, "Catalog operation alter_add_value_partition", args(0,8, arg("sname",str),arg("mtnme",str),arg("psnme",str),arg("ptnme",str),arg("nills",bit),arg("update",int),batarg("assert",lng), varargany("arg",0))),
 pattern("sqlcatalog", "comment_on", SQLcomment_on, false, "Catalog operation comment_on", args(0,2, arg("objid",int),arg("remark",str))),
 pattern("sqlcatalog", "rename_schema", SQLrename_schema, false, "Catalog operation rename_schema", args(0,2, arg("sname",str),arg("newnme",str))),
 pattern("sqlcatalog", "rename_table", SQLrename_table, false, "Catalog operation rename_table", args(0,4, arg("osname",str),arg("nsname",str),arg("otname",str),arg("ntname",str))),
 pattern("sqlcatalog", "rename_column", SQLrename_column, false, "Catalog operation rename_column", args(0,4, arg("sname",str),arg("tname",str),arg("cname",str),arg("newnme",str))),
 /* sql_transaction */
 pattern("sql", "transaction_release", SQLtransaction_release, true, "A transaction statement (type can be commit,release,rollback or start)", args(1,3, arg("",void),arg("chain",int),arg("name",str))),
 pattern("sql", "transaction_commit", SQLtransaction_commit, true, "A transaction statement (type can be commit,release,rollback or start)", args(1,3, arg("",void),arg("chain",int),arg("name",str))),
 pattern("sql", "transaction_rollback", SQLtransaction_rollback, true, "A transaction statement (type can be commit,release,rollback or start)", args(1,3, arg("",void),arg("chain",int),arg("name",str))),
 pattern("sql", "transaction_begin", SQLtransaction_begin, true, "A transaction statement (type can be commit,release,rollback or start)", args(1,3, arg("",void),arg("chain",int),arg("name",str))),
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
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, arg("",oid),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",hge))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,6, batarg("",oid),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),optbatarg("limit",hge))),
 pattern("sql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, arg("",oid),arg("p",bit),argany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),arg("limit",hge))),
 pattern("batsql", "window_bound", SQLwindow_bound, false, "computes window ranges for each row", args(1,7, batarg("",oid),batarg("p",bit),batargany("b",1),arg("unit",int),arg("bound",int),arg("excl",int),optbatarg("limit",hge))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",hge),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",hge),batarg("b",bte),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",hge),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",hge),batarg("b",sht),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",hge),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",hge),batarg("b",int),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",hge),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",hge),batarg("b",lng),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "sum", SQLsum, false, "return the sum of groups", args(1,7, arg("",hge),arg("b",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "sum", SQLsum, false, "return the sum of groups", args(1,7, batarg("",hge),batarg("b",hge),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",hge),arg("b",bte),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",hge),batarg("b",bte),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",hge),arg("b",sht),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",hge),batarg("b",sht),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",hge),arg("b",int),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",hge),batarg("b",int),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",hge),arg("b",lng),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",hge),batarg("b",lng),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "prod", SQLprod, false, "return the product of groups", args(1,7, arg("",hge),arg("b",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "prod", SQLprod, false, "return the product of groups", args(1,7, batarg("",hge),batarg("b",hge),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "avg", SQLavg, false, "return the average of groups", args(1,7, arg("",dbl),arg("b",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavg, false, "return the average of groups", args(1,7, batarg("",dbl),batarg("b",hge),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, arg("",hge),arg("b",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "avg", SQLavginteger, false, "return the average of groups", args(1,7, batarg("",hge),batarg("b",hge),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, arg("",dbl),arg("b",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdev", SQLstddev_samp, false, "return the standard deviation sample of groups", args(1,7, batarg("",dbl),batarg("b",hge),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, arg("",dbl),arg("b",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "stdevp", SQLstddev_pop, false, "return the standard deviation population of groups", args(1,7, batarg("",dbl),batarg("b",hge),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, arg("",dbl),arg("b",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variance", SQLvar_samp, false, "return the variance sample of groups", args(1,7, batarg("",dbl),batarg("b",hge),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, arg("",dbl),arg("b",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "variancep", SQLvar_pop, false, "return the variance population of groups", args(1,7, batarg("",dbl),batarg("b",hge),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, arg("",dbl),arg("b",hge),arg("c",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariance", SQLcovar_samp, false, "return the covariance sample value of groups", args(1,8, batarg("",dbl),optbatarg("b",hge),optbatarg("c",hge),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, arg("",dbl),arg("b",hge),arg("c",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "covariancep", SQLcovar_pop, false, "return the covariance population value of groups", args(1,8, batarg("",dbl),optbatarg("b",hge),optbatarg("c",hge),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
 pattern("sql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, arg("",dbl),arg("b",hge),arg("c",hge),arg("p",bit),arg("o",bit),arg("t",int),arg("s",oid),arg("e",oid))),
 pattern("batsql", "corr", SQLcorr, false, "return the correlation value of groups", args(1,8, batarg("",dbl),optbatarg("b",hge),optbatarg("c",hge),optbatarg("p",bit),optbatarg("o",bit),arg("t",int),optbatarg("s",oid),optbatarg("e",oid))),
#endif
 pattern("sql", "vacuum", SQLstr_vacuum, true, "vacuum a string column", args(0,3, arg("sname",str),arg("tname",str),arg("cname",str))),
 pattern("sql", "vacuum", SQLstr_auto_vacuum, true, "auto vacuum string column with interval(sec)", args(0,4, arg("sname",str),arg("tname",str),arg("cname",str),arg("interval", int))),
 pattern("sql", "stop_vacuum", SQLstr_stop_vacuum, true, "stop auto vacuum", args(0,3, arg("sname",str),arg("tname",str),arg("cname",str))),
 pattern("sql", "vacuum", SQLstr_vacuum, true, "vacuum a string column", args(0,2, arg("sname",str),arg("tname",str))),
 pattern("sql", "vacuum", SQLstr_auto_vacuum, true, "auto vacuum string column of given table with interval(sec)", args(0,3, arg("sname",str),arg("tname",str),arg("interval", int))),
 pattern("sql", "stop_vacuum", SQLstr_stop_vacuum, true, "stop auto vacuum", args(0,2, arg("sname",str),arg("tname",str))),
 pattern("sql", "check", SQLcheck, false, "Return sql string of check constraint.", args(1,3, arg("sql",str), arg("sname", str), arg("name", str))),
 pattern("sql", "read_dump_rel", SQLread_dump_rel, false, "Reads sql_rel string into sql_rel object and then writes it to the return value", args(1,2, arg("sql",str), arg("sql_rel", str))),
 pattern("sql", "normalize_monetdb_url", SQLnormalize_monetdb_url, false, "Normalize mapi:monetdb://, monetdb:// or monetdbs:// URL", args(1,2, arg("",str),arg("u",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_sql_mal)
{ mal_module("sql", NULL, sql_init_funcs); }
