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

#include "monetdb_config.h"
#include "rel_updates.h"
#include "rel_semantic.h"
#include "rel_select.h"
#include "rel_rel.h"
#include "rel_basetable.h"
#include "rel_exp.h"
#include "rel_schema.h"
#include "sql_privileges.h"
#include "sql_partition.h"
#include "rel_dump.h"
#include "rel_psm.h"
#include "sql_symbol.h"
#include "rel_prop.h"
#include "sql_storage.h"

static sql_exp *
insert_value(sql_query *query, sql_column *c, sql_rel **r, symbol *s, const char* action)
{
	mvc *sql = query->sql;
	if (s->token == SQL_NULL) {
		return exp_atom(sql->sa, atom_general(sql->sa, &c->type, NULL, 0));
	} else if (s->token == SQL_DEFAULT) {
		if (c->def) {
			sql_exp *e = rel_parse_val(sql, c->t->s, c->def, &c->type, sql->emode, NULL);
			if (!e || (e = exp_check_type(sql, &c->type, r ? *r : NULL, e, type_equal)) == NULL)
				return NULL;
			return e;
		} else {
			return sql_error(sql, 02, SQLSTATE(42000) "%s: column '%s' has no valid default value", action, c->base.name);
		}
	} else {
		exp_kind ek = {type_value, card_value, FALSE};
		sql_exp *e = rel_value_exp2(query, r, s, sql_sel | sql_values, ek);

		if (!e)
			return(NULL);
		return exp_check_type(sql, &c->type, r ? *r : NULL, e, type_equal);
	}
}

static sql_exp **
insert_exp_array(mvc *sql, sql_table *t, int *Len)
{
	*Len = ol_length(t->columns);
	return SA_ZNEW_ARRAY(sql->sa, sql_exp*, *Len);
}

sql_table *
get_table(sql_rel *t)
{
	sql_table *tab = NULL;

	assert(is_updateble(t));
	if (t->op == op_basetable) { /* existing base table */
		tab = t->l;
	} else if (t->op == op_ddl &&
			   (t->flag == ddl_alter_table || t->flag == ddl_create_table || t->flag == ddl_create_view)) {
		return rel_ddl_table_get(t);
	}
	return tab;
}

static sql_rel *
get_basetable(sql_rel *t)
{
	if (is_simple_project(t->op) || is_select(t->op) || is_join(t->op) || is_semi(t->op)) {
		return get_basetable(t->l);
	} else if (t->op == op_basetable) { /* existing base table */
		return t;
	} else if (t->op == op_ddl &&
			   (t->flag == ddl_alter_table || t->flag == ddl_create_table || t->flag == ddl_create_view)) {
		return rel_ddl_basetable_get(t);
	}
	return t;
}

static sql_rel *
rel_insert_hash_idx(mvc *sql, const char* alias, sql_idx *i, sql_rel *inserts)
{
	char *iname = sa_strconcat( sql->sa, "%", i->base.name);
	node *m;
	sql_subtype *it, *lng;
	int bits = 1 + ((sizeof(lng)*8)-1)/(list_length(i->columns)+1);
	sql_exp *h = NULL;
	sql_rel *ins = inserts->r;

	assert(is_project(ins->op) || ins->op == op_table);
	if (list_length(i->columns) <= 1 || non_updatable_index(i->type)) {
		/* dummy append */
		list *exps = rel_projections(sql, ins, NULL, 1, 1);
		if (!exps)
			return NULL;
		inserts->r = ins = rel_project(sql->sa, ins, exps);
		if (!ins)
			return NULL;
		list_append(ins->exps, exp_label(sql->sa, exp_atom_lng(sql->sa, 0), ++sql->label));
		return inserts;
	}

	it = sql_fetch_localtype(TYPE_int);
	lng = sql_fetch_localtype(TYPE_lng);
	for (m = i->columns->h; m; m = m->next) {
		sql_kc *c = m->data;
		sql_exp *e = list_fetch(ins->exps, c->c->colnr);
		e = exp_ref(sql, e);

		if (h && i->type == hash_idx)  {
			list *exps = new_exp_list(sql->sa);
			sql_subfunc *xor = sql_bind_func_result(sql, "sys", "rotate_xor_hash", F_FUNC, true, lng, 3, lng, it, &c->c->type);

			append(exps, h);
			append(exps, exp_atom_int(sql->sa, bits));
			append(exps, e);
			h = exp_op(sql->sa, exps, xor);
		} else if (h)  { /* order preserving hash */
			sql_exp *h2;
			sql_subfunc *lsh = sql_bind_func_result(sql, "sys", "left_shift", F_FUNC, true, lng, 2, lng, it);
			sql_subfunc *lor = sql_bind_func_result(sql, "sys", "bit_or", F_FUNC, true, lng, 2, lng, lng);
			sql_subfunc *hf = sql_bind_func_result(sql, "sys", "hash", F_FUNC, true, lng, 1, &c->c->type);

			h = exp_binop(sql->sa, h, exp_atom_int(sql->sa, bits), lsh);
			h2 = exp_unop(sql->sa, e, hf);
			h = exp_binop(sql->sa, h, h2, lor);
		} else {
			sql_subfunc *hf = sql_bind_func_result(sql, "sys", "hash", F_FUNC, true, lng, 1, &c->c->type);
			h = exp_unop(sql->sa, e, hf);
			if (i->type == oph_idx)
				break;
		}
	}
	/* append inserts to hash */
	inserts->r = ins = rel_project(sql->sa, ins, rel_projections(sql, ins, NULL, 1, 1));
	exp_setname(sql, h, alias, iname);
	list_append(ins->exps, h);
	return inserts;
}

static sql_rel *
rel_insert_join_idx(mvc *sql, const char* alias, sql_idx *i, sql_rel *inserts)
{
	char *iname = sa_strconcat( sql->sa, "%", i->base.name);
	node *m, *o;
	sql_trans *tr = sql->session->tr;
	sql_key *rk = (sql_key*)os_find_id(tr->cat->objects, tr, ((sql_fkey*)i->key)->rkey);
	sql_rel *rt = rel_basetable(sql, rk->t, rk->t->base.name), *brt = rt;
	int selfref = (rk->t->base.id == i->t->base.id);
	if (selfref)
		TRC_DEBUG(SQL_TRANS, "Self-reference index\n");

	sql_rel *ins = inserts->r;
	sql_exp *e;
	list *join_exps = new_exp_list(sql->sa), *pexps;

	assert(is_project(ins->op) || ins->op == op_table);
	/* NULL and NOT NULL, for 'SIMPLE MATCH' semantics */
	/* AND joins expressions */

	/* left outer join idx.l1 = idx.r1 and .. idx.ln = idx.rn
	 *  then
	 *		for full match only include idx.l1 IS NULL and ... idx.ln IS NULL
	 *		for simple match include idx.l1 IS NULL or idx.ln IS NULL
	 *
	 *  for partial match the left join should include all partial matches, ie
	 *		(idx.l1 = idx.r1 or idx.l1 IS NULL) and
	 *
	 *	For the constraint it self to fail we need the counts from count(r) == count(idx updates).
	 */
	for (m = i->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *c = m->data;
		sql_kc *rc = o->data;
		sql_exp *_is = list_fetch(ins->exps, c->c->colnr), *je;

		if (rel_base_use(sql, brt, rc->c->colnr)) {
			/* TODO add access error */
			return NULL;
		}
		int unique = list_length(i->columns) == 1 && list_length(rk->columns) == 1 && is_column_unique(rc->c);
		sql_exp *rtc = exp_column(sql->sa, rel_name(rt), rc->c->base.name, &rc->c->type, CARD_MULTI, rc->c->null, unique, 0);
		rtc->alias.label = rel_base_nid(brt, rc->c);
		rtc->nid = rtc->alias.label;

		_is = exp_ref(sql, _is);
		if (rel_convert_types(sql, rt, ins, &rtc, &_is, 1, type_equal) < 0)
			return NULL;
		je = exp_compare(sql->sa, rtc, _is, cmp_equal);
		if (c->c->null)
			set_any(je);
		append(join_exps, je);
	}

	pexps = rel_projections(sql, ins, NULL, 1, 1);
	ins = rel_crossproduct(sql->sa, ins, rt, op_left);
	set_single(ins);
	ins->exps = join_exps;
	ins = rel_project(sql->sa, ins, pexps);
	/* add row numbers */
	e = exp_column(sql->sa, rel_name(rt), TID, sql_fetch_localtype(TYPE_oid), CARD_MULTI, 0, 1, 1);
	rel_base_use_tid(sql, brt);
	exp_setname(sql, e, alias, iname);
	e->nid = rel_base_nid(brt, NULL);
	append(ins->exps, e);
	set_processed(ins);

	inserts->r = ins;
	return inserts;
}

static sql_rel *
rel_insert_idxs(mvc *sql, sql_table *t, const char* alias, sql_rel *inserts)
{
	if (!ol_length(t->idxs))
		return inserts;

	inserts->r = rel_label(sql, inserts->r, 1);
	for (node *n = ol_first_node(t->idxs); n; n = n->next) {
		sql_idx *i = n->data;

		if (hash_index(i->type) || non_updatable_index(i->type)) {
			if (rel_insert_hash_idx(sql, alias, i, inserts) == NULL)
				return NULL;
		} else if (i->type == join_idx) {
			if (rel_insert_join_idx(sql, alias, i, inserts) == NULL)
				return NULL;
		}
	}
	return inserts;
}

sql_rel *
rel_insert(mvc *sql, sql_rel *t, sql_rel *inserts)
{
	sql_rel * r = rel_create(sql->sa);
	sql_table *tab = get_table(t);
	if(!r)
		return NULL;

	r->op = op_insert;
	r->l = t;
	r->r = inserts;
	r->card = inserts->card;
	/* insert indices */
	if (tab)
		r = rel_insert_idxs(sql, tab, rel_name(t), r);
	if (r) {
		set_processed(r);
		r->exps = rel_projections(sql, r->r, NULL, 1, 1);
		if (!list_empty(r->exps)) {
			for(node *n = r->exps->h, *m = ol_first_node(tab->columns); n && m; n = n->next, m = m->next) {
				sql_column *c = m->data;
				exp_setname(sql, n->data, c->t->base.name, c->base.name);
			}
			/* TODO what to do with indices ? */
		}
	}
	return r;
}

sql_rel *
rel_update_count(mvc *sql, sql_rel *rel)
{
	rel = rel_groupby(sql, rel, NULL);
	sql_subfunc *a = sql_bind_func(sql, "sys", "count", sql_fetch_localtype(TYPE_void), NULL, F_AGGR, true, true);
    sql_exp *e = exp_aggr(sql->sa, NULL, a, false, 0, CARD_ATOM, 0);
	set_intern(e);
	(void) rel_groupby_add_aggr(sql, rel, e);
	return rel;
}

static sql_rel *
rel_insert_table(sql_query *query, sql_table *t, char *name, sql_rel *inserts)
{
	sql_rel *rel = rel_basetable(query->sql, t, name);
	rel_base_use_all(query->sql, rel);
	rel = rewrite_basetable(query->sql, rel, false);
	return rel_insert(query->sql, rel, inserts);
}

static list *
check_table_columns(mvc *sql, sql_table *t, dlist *columns, const char *op, char *tname)
{
	list *collist;

	if (columns) {
		dnode *n;

		collist = sa_list(sql->sa);
		for (n = columns->h; n; n = n->next) {
			sql_column *c = mvc_bind_column(sql, t, n->data.sval);

			if (c) {
				list_append(collist, c);
			} else {
				return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42S22) "%s: no such column '%s.%s'", op, tname, n->data.sval);
			}
		}
	} else {
		collist = t->columns->l;
	}
	return collist;
}

static list *
rel_inserts(mvc *sql, sql_table *t, sql_rel *r, list *collist, size_t rowcount, int copy, const char* action)
{
	int len, i;
	sql_exp **inserts = insert_exp_array(sql, t, &len);
	list *exps = NULL;
	node *n, *m;
	bool has_rel = false, all_values = true;

	if (r->exps) {
		if (!copy) {
			for (n = r->exps->h, m = collist->h; n && m; n = n->next, m = m->next) {
				sql_column *c = m->data;
				sql_exp *e = n->data;

				if (inserts[c->colnr])
					return sql_error(sql, 02, SQLSTATE(42000) "%s: column '%s' specified more than once", action, c->base.name);
				if (!(inserts[c->colnr] = exp_check_type(sql, &c->type, r, e, type_equal)))
					return NULL;
				has_rel = (has_rel || exp_has_rel(e));
				all_values &= is_values(e);
			}
		} else {
			for (m = collist->h; m; m = m->next) {
				sql_column *c = m->data;
				sql_exp *e;

				e = exps_bind_column2(r->exps, c->t->base.name, c->base.name, NULL);
				if (e) {
					if (inserts[c->colnr])
						return sql_error(sql, 02, SQLSTATE(42000) "%s: column '%s' specified more than once", action, c->base.name);
					inserts[c->colnr] = exp_ref(sql, e);
					has_rel = has_rel || exp_has_rel(e);
					all_values &= is_values(e);
				}
			}
		}
	}
	for (m = ol_first_node(t->columns); m; m = m->next) {
		sql_column *c = m->data;
		sql_exp *exps = NULL;

		if (!inserts[c->colnr]) {
			for (size_t j = 0; j < rowcount; j++) {
				sql_exp *e = NULL;

				if (c->def) {
					e = rel_parse_val(sql, t->s, c->def, &c->type, sql->emode, NULL);
					if (!e || (e = exp_check_type(sql, &c->type, r, e, type_equal)) == NULL)
						return NULL;
				} else {
					atom *a = atom_general(sql->sa, &c->type, NULL, 0);
					e = exp_atom(sql->sa, a);
				}
				if (!e)
					return sql_error(sql, 02, SQLSTATE(42000) "%s: column '%s' has no valid default value", action, c->base.name);
				if (!exps && j+1 < rowcount) {
					exps = exp_values(sql->sa, sa_list(sql->sa));
					exps->tpe = c->type;
					exp_label(sql->sa, exps, ++sql->label);
				}
				if (exps) {
					list *vals_list = exps->f;

					assert(rowcount > 1);
					list_append(vals_list, e);
				}
				if (!exps)
					exps = e;
			}
			inserts[c->colnr] = exps;
			assert(inserts[c->colnr]);
		}
	}
	/* rewrite into unions */
	if (has_rel && rowcount && all_values) {
		sql_rel *c = NULL;
		for (size_t j = 0; j < rowcount; j++) {
			sql_rel *p = rel_project(sql->sa, NULL, sa_list(sql->sa));
			for (m = ol_first_node(t->columns); m; m = m->next) {
				sql_column *c = m->data;
				sql_exp *e = inserts[c->colnr];
				assert(is_values(e));
				list *vals = e->f;
				append(p->exps, list_fetch(vals, (int) j));
			}
			if (c) {
				sql_rel *ci = c;
				c = rel_setop_n_ary(sql->sa, append(append(sa_list(sql->sa), c), p), op_munion );
				rel_setop_n_ary_set_exps(sql, c, rel_projections(sql, ci, NULL, 1, 1), false);
				set_processed(c);
			} else
				c = p;
		}
		r->l = c;
		exps = rel_projections(sql, r->l, NULL, 1, 1);
	} else {
		/* now rewrite project exps in proper table order */
		exps = new_exp_list(sql->sa);
		for (i = 0; i<len; i++)
			list_append(exps, inserts[i]);
	}
	return exps;
}

static bool
has_complex_indexes(sql_table *t)
{
	for (node *n = ol_first_node(t->idxs); n; n = n->next) {
		sql_idx *i = n->data;

		if (hash_index(i->type) || oid_index(i->type) || i->type == no_idx)
			return true;
	}
	return false;
}

sql_table *
insert_allowed(mvc *sql, sql_table *t, char *tname, char *op, char *opname)
{
	list *mts = NULL;

	if (!t) {
		if (sql->session->status) /* if find_table_or_view_on_scope was already called, don't overwrite error message */
			return NULL;
		return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42S02) "%s: no such table '%s'", op, tname);
	} else if (isView(t)) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: cannot %s view '%s'", op, opname, tname);
	} else if (isNonPartitionedTable(t)) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: cannot %s merge table '%s'", op, opname, tname);
	} else if ((isRangePartitionTable(t) || isListPartitionTable(t)) && list_length(t->members)==0) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: %s partitioned table '%s' has no partitions set", op, isListPartitionTable(t)?"list":"range", tname);
	} else if ((isRangePartitionTable(t) || isListPartitionTable(t)) && has_complex_indexes(t)) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: not possible to insert into a partitioned table with complex indexes at the moment", op);
	} else if (isRemote(t)) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: cannot %s remote table '%s' from this server at the moment", op, opname, tname);
	} else if (isReplicaTable(t)) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: cannot %s replica table '%s'", op, opname, tname);
	} else if (t->access == TABLE_READONLY) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: cannot %s read only table '%s'", op, opname, tname);
	}
	if (t && !isTempTable(t) && store_readonly(sql->session->tr->store))
		return sql_error(sql, 02, SQLSTATE(42000) "%s: %s table '%s' not allowed in readonly mode", op, opname, tname);
	if (has_complex_indexes(t) && (mts = partition_find_mergetables(sql, t))) {
		for (node *n = mts->h ; n ; n = n->next) {
			sql_part *pt = n->data;

			if ((isRangePartitionTable(pt->t) || isListPartitionTable(pt->t)))
				return sql_error(sql, 02, SQLSTATE(42000) "%s: not possible to insert into a partitioned table with complex indexes at the moment", op);
		}
	}
	if (!table_privs(sql, t, PRIV_INSERT))
		return sql_error(sql, 02, SQLSTATE(42000) "%s: insufficient privileges for user '%s' to %s table '%s'", op, get_string_global_var(sql, "current_user"), opname, tname);
	return t;
}

static int
copy_allowed(mvc *sql, int from)
{
	if (!global_privs(sql, (from)?PRIV_COPYFROMFILE:PRIV_COPYINTOFILE))
		return 0;
	return 1;
}

sql_table *
update_allowed(mvc *sql, sql_table *t, char *tname, char *op, char *opname, int is_delete)
{
	list *mts = NULL;

	if (!t) {
		return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42S02) "%s: no such table '%s'", op, tname);
	} else if (isView(t)) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: cannot %s view '%s'", op, opname, tname);
	} else if (isNonPartitionedTable(t) && is_delete == 0) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: cannot %s merge table '%s'", op, opname, tname);
	} else if (isNonPartitionedTable(t) && is_delete != 0 && list_length(t->members)==0) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: cannot %s merge table '%s' has no partitions set", op, opname, tname);
	} else if ((isRangePartitionTable(t) || isListPartitionTable(t)) && list_length(t->members)==0) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: %s partitioned table '%s' has no partitions set", op, isListPartitionTable(t)?"list":"range", tname);
	} else if ((isRangePartitionTable(t) || isListPartitionTable(t)) && has_complex_indexes(t)) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: not possible to update a partitioned table with complex indexes at the moment", op);
	} else if (isRemote(t)) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: cannot %s remote table '%s' from this server at the moment", op, opname, tname);
	} else if (isReplicaTable(t)) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: cannot %s replica table '%s'", op, opname, tname);
	} else if (t->access == TABLE_READONLY || t->access == TABLE_APPENDONLY) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: cannot %s read or append only table '%s'", op, opname, tname);
	}
	if (t && !isTempTable(t) && store_readonly(sql->session->tr->store))
		return sql_error(sql, 02, SQLSTATE(42000) "%s: %s table '%s' not allowed in readonly mode", op, opname, tname);
	if (has_complex_indexes(t) && (mts = partition_find_mergetables(sql, t))) {
		for (node *n = mts->h ; n ; n = n->next) {
			sql_part *pt = n->data;

			if ((isRangePartitionTable(pt->t) || isListPartitionTable(pt->t)))
				return sql_error(sql, 02, SQLSTATE(42000) "%s: not possible to update a partitioned table with complex indexes at the moment", op);
		}
	}
	if ((is_delete == 1 && !table_privs(sql, t, PRIV_DELETE)) || (is_delete == 2 && !table_privs(sql, t, PRIV_TRUNCATE)))
		return sql_error(sql, 02, SQLSTATE(42000) "%s: insufficient privileges for user '%s' to %s table '%s'", op, get_string_global_var(sql, "current_user"), opname, tname);
	return t;
}

static sql_rel *
insert_generate_inserts(sql_query *query, sql_table *t, dlist *columns, symbol *val_or_q, const char* action)
{
	mvc *sql = query->sql;
	sql_rel *r = NULL;
	size_t rowcount = 0;
	bool is_subquery = false;
	list *collist = check_table_columns(sql, t, columns, action, t->base.name);
	if (!collist)
		return NULL;

	if (val_or_q->token == SQL_VALUES) {
		dlist *rowlist = val_or_q->data.lval;
		list *exps = new_exp_list(sql->sa);

		if (!rowlist->h) {
			r = rel_project(sql->sa, NULL, NULL);
			if (!columns)
				collist = NULL;
		}

		if (!rowlist->h) /* no values insert 1 row */
			rowcount++;
		for (dnode *o = rowlist->h; o; o = o->next, rowcount++) {
			dlist *values = o->data.lval;

			if (dlist_length(values) != list_length(collist)) {
				return sql_error(sql, 02, SQLSTATE(21S01) "%s: number of values doesn't match number of columns of table '%s'", action, t->base.name);
			} else {
				dnode *n;
				node *v, *m;

				if (o->next && list_empty(exps)) {
					for (n = values->h, m = collist->h; n && m; n = n->next, m = m->next) {
						sql_exp *vals = exp_values(sql->sa, sa_list(sql->sa));
						sql_column *c = m->data;

						vals->tpe = c->type;
						exp_label(sql->sa, vals, ++sql->label);
						list_append(exps, vals);
					}
				}
				if (!list_empty(exps)) {
					for (n = values->h, m = collist->h, v = exps->h; n && m && v; n = n->next, m = m->next, v = v->next) {
						sql_exp *vals = v->data;
						list *vals_list = vals->f;
						sql_column *c = m->data;
						sql_exp *ins = insert_value(query, c, &r, n->data.sym, action);

						if (!ins)
							return NULL;
						if (!exp_name(ins))
							exp_label(sql->sa, ins, ++sql->label);
						list_append(vals_list, ins);
					}
				} else {
					/* only allow correlation in a single row of values */
					for (n = values->h, m = collist->h; n && m; n = n->next, m = m->next) {
						sql_column *c = m->data;
						sql_exp *ins = insert_value(query, c, &r, n->data.sym, action);

						if (!ins)
							return NULL;
						if (!exp_name(ins))
							exp_label(sql->sa, ins, ++sql->label);
						list_append(exps, ins);
					}
				}
			}
		}
		if (collist)
			r = rel_project(sql->sa, r, exps);
	} else {
		exp_kind ek = {type_value, card_relation, TRUE};

		r = rel_subquery(query, val_or_q, ek);
		rowcount++;
		is_subquery = true;
	}
	if (!r)
		return NULL;

	/* For the subquery case a projection is always needed */
	if (is_subquery)
		r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 0));
	if ((r->exps && list_length(r->exps) != list_length(collist)) || (!r->exps && collist))
		return sql_error(sql, 02, SQLSTATE(21S01) "%s: query result doesn't match number of columns in table '%s'", action, t->base.name);
	if (is_subquery && !(r->exps = check_distinct_exp_names(sql, r->exps)))
		return sql_error(sql, 02, SQLSTATE(42000) "%s: duplicate column names in subquery column list", action);

	r->exps = rel_inserts(sql, t, r, collist, rowcount, 0, action);
	if(!r->exps)
		return NULL;
	return r;
}

static sql_rel *
merge_generate_inserts(sql_query *query, sql_table *t, sql_rel *r, dlist *columns, symbol *val_or_q)
{
	mvc *sql = query->sql;
	sql_rel *res = NULL;
	list *collist = check_table_columns(sql, t, columns, "MERGE", t->base.name);

	if (!collist)
		return NULL;

	if (val_or_q->token == SQL_VALUES) {
		list *exps = new_exp_list(sql->sa);
		dlist *rowlist = val_or_q->data.lval;

		if (!rowlist->h) {
			res = rel_project(sql->sa, NULL, NULL);
			if (!columns)
				collist = NULL;
		} else {
			node *m;
			dnode *n;
			dlist *inserts = rowlist->h->data.lval;

			if (dlist_length(rowlist) != 1)
				return sql_error(sql, 02, SQLSTATE(42000) "MERGE: number of insert rows must be exactly one in a merge statement");
			if (dlist_length(inserts) != list_length(collist))
				return sql_error(sql, 02, SQLSTATE(21S01) "MERGE: number of values doesn't match number of columns of table '%s'", t->base.name);

			for (n = inserts->h, m = collist->h; n && m; n = n->next, m = m->next) {
				sql_column *c = m->data;
				sql_exp *ins = insert_value(query, c, &r, n->data.sym, "MERGE");
				if (!ins)
					return NULL;
				if (!exp_name(ins))
					exp_label(sql->sa, ins, ++sql->label);
				list_append(exps, ins);
			}
		}
		if (collist)
			res = rel_project(sql->sa, r, exps);
	} else {
		return sql_error(sql, 02, SQLSTATE(42000) "MERGE: subqueries not supported in INSERT clauses inside MERGE statements");
	}
	if (!res)
		return NULL;
	if ((res->exps && list_length(res->exps) != list_length(collist)) || (!res->exps && collist))
		return sql_error(sql, 02, SQLSTATE(21S01) "MERGE: query result doesn't match number of columns in table '%s'", t->base.name);

	res->l = r;
	res->exps = rel_inserts(sql, t, res, collist, 1, 0, "MERGE");
	if(!res->exps)
		return NULL;
	return res;
}

static sql_rel *
insert_into(sql_query *query, dlist *qname, dlist *columns, symbol *val_or_q, dlist *opt_returning)
{
	char *sname = qname_schema(qname);
	char *tname = qname_schema_object(qname);
	sql_table *t = NULL;
	sql_rel *r = NULL;

	t = find_table_or_view_on_scope(query->sql, NULL, sname, tname, "INSERT INTO", false);
	if (insert_allowed(query->sql, t, tname, "INSERT INTO", "insert into") == NULL)
		return NULL;
	r = insert_generate_inserts(query, t, columns, val_or_q, "INSERT INTO");
	if(!r)
		return NULL;
	sql_rel* ins = rel_insert_table(query, t, t->base.name, r);

	if (opt_returning) {
		list *pexps = sa_list(query->sql->sa);
		for (dnode *n = opt_returning->h; n; n = n->next) {
			sql_exp *ce = rel_column_exp(query, &ins, n->data.sym, sql_sel | sql_no_subquery);
			if (ce == NULL)
				return NULL;
			pexps = append(pexps, ce);
		}
		ins = rel_project(query->sql->sa, ins, pexps);
		query->sql->type = Q_TABLE;
	} else if (ins) {
		ins = rel_update_count(query->sql, ins);
		query->sql->type = Q_UPDATE;
	}
	return ins;
}

static int
is_idx_updated(sql_idx * i, list *exps)
{
	int update = 0;
	node *m, *n;

	for (m = i->columns->h; m; m = m->next) {
		sql_kc *ic = m->data;

		for (n = exps->h; n; n = n->next) {
			sql_exp *ce = n->data;
			sql_column *c = find_sql_column(i->t, exp_name(ce));

			if (c && ic->c->colnr == c->colnr) {
				update = 1;
				break;
			}
		}
	}
	return update;
}

static sql_rel *
rel_update_hash_idx(mvc *sql, const char* alias, sql_idx *i, sql_rel *updates)
{
	char *iname = sa_strconcat( sql->sa, "%", i->base.name);
	node *m;
	sql_subtype *it, *lng = 0; /* is not set in first if below */
	int bits = 1 + ((sizeof(lng)*8)-1)/(list_length(i->columns)+1);
	sql_exp *h = NULL;
	sql_rel *ups = updates->r, *bt = get_basetable(updates->l);

	assert(is_project(ups->op) || ups->op == op_table);
	if (list_length(i->columns) <= 1 || non_updatable_index(i->type)) {
		h = exp_label(sql->sa, exp_atom_lng(sql->sa, 0), ++sql->label);
	} else {
		it = sql_fetch_localtype(TYPE_int);
		lng = sql_fetch_localtype(TYPE_lng);
		for (m = i->columns->h; m; m = m->next) {
			sql_kc *c = m->data;
			sql_exp *e = list_fetch(ups->exps, c->c->colnr+1);
			e = exp_ref(sql, e);

			if (h && i->type == hash_idx)  {
				list *exps = new_exp_list(sql->sa);
				sql_subfunc *xor = sql_bind_func_result(sql, "sys", "rotate_xor_hash", F_FUNC, true, lng, 3, lng, it, &c->c->type);

				append(exps, h);
				append(exps, exp_atom_int(sql->sa, bits));
				append(exps, e);
				h = exp_op(sql->sa, exps, xor);
			} else if (h)  { /* order preserving hash */
				sql_exp *h2;
				sql_subfunc *lsh = sql_bind_func_result(sql, "sys", "left_shift", F_FUNC, true, lng, 2, lng, it);
				sql_subfunc *lor = sql_bind_func_result(sql, "sys", "bit_or", F_FUNC, true, lng, 2, lng, lng);
				sql_subfunc *hf = sql_bind_func_result(sql, "sys", "hash", F_FUNC, true, lng, 1, &c->c->type);

				h = exp_binop(sql->sa, h, exp_atom_int(sql->sa, bits), lsh);
				h2 = exp_unop(sql->sa, e, hf);
				h = exp_binop(sql->sa, h, h2, lor);
			} else {
				sql_subfunc *hf = sql_bind_func_result(sql, "sys", "hash", F_FUNC, true, lng, 1, &c->c->type);
				h = exp_unop(sql->sa, e, hf);
				if (i->type == oph_idx)
					break;
			}
		}
	}
	/* append hash to updates */
	updates->r = ups = rel_project(sql->sa, ups, rel_projections(sql, ups, NULL, 1, 1));
	exp_setalias(h, rel_base_idx_nid(bt, i), alias, iname);
	list_append(ups->exps, h);

	if (!updates->exps)
		updates->exps = new_exp_list(sql->sa);
	append(updates->exps, h=exp_column(sql->sa, alias, iname, lng, CARD_MULTI, 0, 0, 0));
	h->alias.label = rel_base_idx_nid(bt, i);
	h->nid = h->alias.label;
	return updates;
}

/*
         A referential constraint is satisfied if one of the following con-
         ditions is true, depending on the <match option> specified in the
         <referential constraint definition>:

         -  If no <match type> was specified then, for each row R1 of the
            referencing table, either at least one of the values of the
            referencing columns in R1 shall be a null value, or the value of
            each referencing column in R1 shall be equal to the value of the
            corresponding referenced column in some row of the referenced
            table.

         -  If MATCH FULL was specified then, for each row R1 of the refer-
            encing table, either the value of every referencing column in R1
            shall be a null value, or the value of every referencing column
            in R1 shall not be null and there shall be some row R2 of the
            referenced table such that the value of each referencing col-
            umn in R1 is equal to the value of the corresponding referenced
            column in R2.

         -  If MATCH PARTIAL was specified then, for each row R1 of the
            referencing table, there shall be some row R2 of the refer-
            enced table such that the value of each referencing column in
            R1 is either null or is equal to the value of the corresponding
            referenced column in R2.
*/
static sql_rel *
rel_update_join_idx(mvc *sql, const char* alias, sql_idx *i, sql_rel *updates)
{
	int nr = ++sql->label;
	char name[16], *nme = number2name(name, sizeof(name), nr);
	char *iname = sa_strconcat( sql->sa, "%", i->base.name);

	node *m, *o;
	sql_trans *tr = sql->session->tr;
	sql_key *rk = (sql_key*)os_find_id(tr->cat->objects, tr, ((sql_fkey*)i->key)->rkey);
	sql_rel *rt = rel_basetable(sql, rk->t, sa_strdup(sql->sa, nme)), *brt = rt;

	sql_rel *ups = updates->r;
	sql_exp *e;
	list *join_exps = new_exp_list(sql->sa), *pexps;

	assert(is_project(ups->op) || ups->op == op_table);
	for (m = i->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *c = m->data;
		sql_kc *rc = o->data;
		sql_exp *upd = list_fetch(ups->exps, c->c->colnr + 1), *je;

		if (rel_base_use(sql, rt, rc->c->colnr)) {
			/* TODO add access error */
			return NULL;
		}
		int unique = list_length(i->columns) == 1 && list_length(rk->columns) == 1 && is_column_unique(rc->c);
		sql_exp *rtc = exp_column(sql->sa, rel_name(rt), rc->c->base.name, &rc->c->type, CARD_MULTI, rc->c->null, unique, 0);
		rtc->alias.label = rel_base_nid(brt, rc->c);
		rtc->nid = rtc->alias.label;

		/* FOR MATCH FULL/SIMPLE/PARTIAL see above */
		/* Currently only the default MATCH SIMPLE is supported */
		upd = exp_ref(sql, upd);
		if (rel_convert_types(sql, rt, updates, &rtc, &upd, 1, type_equal) < 0) {
			list_destroy(join_exps);
			return NULL;
		}
		je = exp_compare(sql->sa, rtc, upd, cmp_equal);
		if (c->c->null)
			set_any(je);
		append(join_exps, je);
	}

	pexps = rel_projections(sql, ups, NULL, 1, 1);
	ups = rel_crossproduct(sql->sa, ups, rt, op_left);
	set_single(ups);
	ups->exps = join_exps;
	ups = rel_project(sql->sa, ups, pexps);
	/* add row numbers */
	e = exp_column(sql->sa, rel_name(rt), TID, sql_fetch_localtype(TYPE_oid), CARD_MULTI, 0, 1, 1);
	rel_base_use_tid(sql, brt);
	exp_setname(sql, e, alias, iname);
	e->nid = rel_base_nid(brt, NULL);
	append(ups->exps, e);
	set_processed(ups);

	updates->r = ups;
	if (!updates->exps)
		updates->exps = new_exp_list(sql->sa);
	append(updates->exps, e = exp_column(sql->sa, alias, iname, sql_fetch_localtype(TYPE_oid), CARD_MULTI, 0, 0, 0));
	e->alias.label = rel_base_nid(brt, NULL);
	e->nid = e->alias.label;
	return updates;
}

/* for cascade of updates we change the 'relup' relations into
 * a ddl_list of update relations.
 */
static sql_rel *
rel_update_idxs(mvc *sql, const char *alias, sql_table *t, sql_rel *relup)
{
	if (!ol_length(t->idxs))
		return relup;

	for (node *n = ol_first_node(t->idxs); n; n = n->next) {
		sql_idx *i = n->data;

		/* check if update is needed,
		 * ie at least on of the idx columns is updated
		 */
		if (relup->exps && is_idx_updated(i, relup->exps) == 0)
			continue;

		/*
		 * relup->exps isn't set in case of alter statements!
		 * Ie todo check for new indices.
		 */

		if (hash_index(i->type) || non_updatable_index(i->type)) {
			rel_update_hash_idx(sql, alias, i, relup);
		} else if (i->type == join_idx) {
			rel_update_join_idx(sql, alias, i, relup);
		}
	}
	return relup;
}

sql_rel *
rel_update(mvc *sql, sql_rel *t, sql_rel *uprel, sql_exp **updates, list *exps)
{
	sql_rel *r = rel_create(sql->sa);
	sql_table *tab = get_table(t);
	sql_rel *bt = get_basetable(uprel);
	const char *alias = rel_name(t);
	node *m;

	if (!r)
		return NULL;

	/* todo only add column used by indices */
	if (tab && updates)
		for (m = ol_first_node(tab->columns); m; m = m->next) {
			sql_column *c = m->data;
			sql_exp *v = updates[c->colnr];

			if (!v && rel_base_use(sql, bt, c->colnr) < 0) /* not allowed */
				continue;
			if (ol_length(tab->idxs) && !v) {
				v = exp_column(sql->sa, alias, c->base.name, &c->type, CARD_MULTI, c->null, is_column_unique(c), 0);
				v->alias.label = rel_base_nid(bt, c);
				v->nid = v->alias.label;
			}
			if (v)
				v = rel_project_add_exp(sql, uprel, v);
		}

	r->op = op_update;
	r->l = t;
	r->r = uprel;
	r->card = uprel->card;
	r->exps = exps;
	/* update indices */
	if (tab)
		return rel_update_idxs(sql, alias, tab, r);
	return r;
}

sql_exp *
update_check_column(mvc *sql, sql_table *t, sql_column *c, sql_exp *v, sql_rel *r, char *cname, const char *action)
{
	if (!table_privs(sql, t, PRIV_UPDATE) && sql_privilege(sql, sql->user_id, c->base.id, PRIV_UPDATE) < 0)
		return sql_error(sql, 02, SQLSTATE(42000) "%s: insufficient privileges for user '%s' to update table '%s' on column '%s'", action, get_string_global_var(sql, "current_user"), t->base.name, cname);
	if (!v || (v = exp_check_type(sql, &c->type, r, v, type_equal)) == NULL)
		return NULL;
	return v;
}

static sql_rel *
update_generate_assignments(sql_query *query, sql_table *t, sql_rel *r, sql_rel *bt, dlist *assignmentlist, const char *action)
{
	mvc *sql = query->sql;
	sql_exp **updates = SA_ZNEW_ARRAY(sql->sa, sql_exp*, ol_length(t->columns)), *ne;
	list *exps, *mts = partition_find_mergetables(sql, t);
	dnode *n;
	const char *rname = NULL;

	if (!list_empty(mts)) {
		for (node *nn = mts->h; nn; ) { /* extract mergetable from the parts */
			node *next = nn->next;
			sql_part *pt = nn->data;

			if (isPartitionedByColumnTable(pt->t) || isPartitionedByExpressionTable(pt->t))
				nn->data = pt->t;
			else
				list_remove_node(mts, NULL, nn);
			nn = next;
		}
	}
	if (isPartitionedByColumnTable(t) || isPartitionedByExpressionTable(t)) { /* validate update on mergetable */
		if (!mts)
			mts = sa_list(sql->sa);
		list_append(mts, t);
	}

	/* first create the project */
	exps = list_append(new_exp_list(sql->sa), ne=exp_column(sql->sa, rname = rel_name(r), TID, sql_fetch_localtype(TYPE_oid), CARD_MULTI, 0, 1, 1));
	ne->alias.label = rel_base_nid(bt, NULL);
	ne->nid = ne->alias.label;

	for (n = assignmentlist->h; n; n = n->next) {
		symbol *a = NULL;
		sql_exp *v = NULL;
		sql_rel *rel_val = NULL;
		dlist *assignment = n->data.sym->data.lval;
		int single = (assignment->h->next->type == type_string), outer = 0;
		/* Single assignments have a name, multicolumn a list */

		a = assignment->h->data.sym;
		if (a) {
			exp_kind ek = { (single)?type_value:type_relation, card_column, FALSE};

			if (single && a->token == SQL_DEFAULT) {
				char *colname = assignment->h->next->data.sval;
				sql_column *c = mvc_bind_column(sql, t, colname);

				if (!c)
					return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42S22) "%s: no such column '%s.%s'", action, t->base.name, colname);
				if (c->def) {
					v = rel_parse_val(sql, t->s, c->def, &c->type, sql->emode, NULL);
				} else {
					return sql_error(sql, 02, SQLSTATE(42000) "%s: column '%s' has no valid default value", action, c->base.name);
				}
			} else if (single) {
				v = rel_value_exp(query, &r, a, sql_sel | sql_update_set, ek);
				outer = 1;
			} else {
				if (r)
					query_push_outer(query, r, sql_sel | sql_update_set);
				rel_val = rel_subquery(query, a, ek);
				if (r) {
					r = query_pop_outer(query);
					if (r && is_groupby(r->op))
						return sql_error(sql, 02, SQLSTATE(42000) "SELECT: aggregate functions not allowed in SET, WHILE, IF, ELSE, CASE, WHEN, RETURN, ANALYZE clauses");
				}
				outer = 1;
			}
			if ((single && !v) || (!single && !rel_val))
				return NULL;
			if (rel_val && outer) {
				if (single) {
					if (!exp_name(v))
						exp_label(sql->sa, v, ++sql->label);
					if (rel_val->op != op_project || is_processed(rel_val))
						rel_val = rel_project(sql->sa, rel_val, NULL);
					v = rel_project_add_exp(sql, rel_val, v);
					reset_processed(rel_val);
				}
				r = rel_crossproduct(sql->sa, r, rel_val, op_left);
				set_dependent(r);
				set_processed(r);
				if (single) {
					v = exp_column(sql->sa, NULL, exp_name(v), exp_subtype(v), v->card, has_nil(v), is_unique(v), is_intern(v));
					rel_val = NULL;
				}
			}
		}
		if (!single) {
			dlist *cols = assignment->h->next->data.lval;
			dnode *m;
			node *n;

			if (!rel_val)
				rel_val = r;
			if (!rel_val || !is_project(rel_val->op))
				return sql_error(sql, 02, SQLSTATE(42000) "%s: Invalid right side of the SET clause", action);
			if (dlist_length(cols) != list_length(rel_val->exps))
				return sql_error(sql, 02, SQLSTATE(42000) "%s: The number of specified columns between the SET clause and the right side don't match (%d != %d)", action, dlist_length(cols), list_length(rel_val->exps));
			for (n = rel_val->exps->h, m = cols->h; n && m; n = n->next, m = m->next) {
				char *cname = m->data.sval;
				sql_column *c = mvc_bind_column(sql, t, cname);
				sql_exp *v = n->data;

				if (!c)
					return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42S22) "%s: no such column '%s.%s'", action, t->base.name, cname);
				if (updates[c->colnr])
					return sql_error(sql, 02, SQLSTATE(42000) "%s: Multiple assignments to same column '%s'", action, c->base.name);
				if (!list_empty(mts)) {
					for (node *nn = mts->h; nn; nn = nn->next) {
						sql_table *mt = nn->data;

						if (isPartitionedByColumnTable(mt)) {
							if (mt->part.pcol->colnr == c->colnr)
								return sql_error(sql, 02, SQLSTATE(42000) "%s: Update on the partitioned column is not possible at the moment", action);
						} else if (isPartitionedByExpressionTable(mt)) {
							for (node *nnn = mt->part.pexp->cols->h ; nnn ; nnn = nnn->next) {
								int next = *(int*) nnn->data;
								if (next == c->colnr)
									return sql_error(sql, 02, SQLSTATE(42000) "%s: Update a column used by the partition's expression is not possible at the moment", action);
							}
						}
					}
				}
				if (!exp_name(v))
					exp_label(sql->sa, v, ++sql->label);
				if (!exp_is_atom(v) || outer)
					v = exp_ref(sql, v);
				if (!v) /* check for NULL */
					v = exp_atom(sql->sa, atom_general(sql->sa, &c->type, NULL, 0));
				if (!(v = update_check_column(sql, t, c, v, r, cname, action)))
					return NULL;
				list_append(exps, ne=exp_column(sql->sa, t->base.name, cname, &c->type, CARD_MULTI, 0, 0, 0));
				ne->alias.label = rel_base_nid(bt, c);
				ne->nid = ne->alias.label;
				exp_setname(sql, v, c->t->base.name, c->base.name);
				updates[c->colnr] = v;
				rel_base_use(sql, bt, c->colnr);
			}
		} else {
			char *cname = assignment->h->next->data.sval;
			sql_column *c = mvc_bind_column(sql, t, cname);

			if (!c)
				return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42S22) "%s: no such column '%s.%s'", action, t->base.name, cname);
			if (updates[c->colnr])
				return sql_error(sql, 02, SQLSTATE(42000) "%s: Multiple assignments to same column '%s'", action, c->base.name);
			if (!list_empty(mts)) {
				for (node *nn = mts->h; nn; nn = nn->next) {
					sql_table *mt = nn->data;

					if (isPartitionedByColumnTable(mt)) {
						if (mt->part.pcol->colnr == c->colnr)
							return sql_error(sql, 02, SQLSTATE(42000) "%s: Update on the partitioned column is not possible at the moment", action);
					} else if (isPartitionedByExpressionTable(mt)) {
						for (node *nnn = mt->part.pexp->cols->h ; nnn ; nnn = nnn->next) {
							int next = *(int*) nnn->data;
							if (next == c->colnr)
								return sql_error(sql, 02, SQLSTATE(42000) "%s: Update a column used by the partition's expression is not possible at the moment", action);
						}
					}
				}
			}
			if (!v)
				v = exp_atom(sql->sa, atom_general(sql->sa, &c->type, NULL, 0));
			if (!(v = update_check_column(sql, t, c, v, r, cname, action)))
				return NULL;
			list_append(exps, ne=exp_column(sql->sa, t->base.name, cname, &c->type, CARD_MULTI, 0, 0, 0));
			ne->alias.label = rel_base_nid(bt, c);
			ne->nid = ne->alias.label;
			exp_setname(sql, v, c->t->base.name, c->base.name);
			updates[c->colnr] = v;
			rel_base_use(sql, bt, c->colnr);
		}
	}
	sql_exp *v = exp_column(sql->sa, rname, TID, sql_fetch_localtype(TYPE_oid), CARD_MULTI, 0, 1, 1);
	if (!v)
		return NULL;
	v->alias.label = rel_base_nid(bt, NULL);
	v->nid = v->alias.label;
	r = rel_project(sql->sa, r, list_append(new_exp_list(sql->sa), v));
	reset_single(r); /* don't let single joins get propagated */
	r = rel_update(sql, bt, r, updates, exps);
	return r;
}

static sql_rel *
update_table(sql_query *query, dlist *qname, str alias, dlist *assignmentlist, symbol *opt_from, symbol *opt_where, dlist *opt_returning)
{
	mvc *sql = query->sql;
	char *sname = qname_schema(qname);
	char *tname = qname_schema_object(qname);
	sql_table *t = NULL;

	t = find_table_or_view_on_scope(sql, NULL, sname, tname, "UPDATE", false);
	if (update_allowed(sql, t, tname, "UPDATE", "update", 0) != NULL) {
		sql_rel *r = NULL, *res = rel_basetable(sql, t, alias ? alias : tname), *bt = rel_dup(res);

		/* We have always to reduce the column visibility because of the SET clause */
		if (!table_privs(sql, t, PRIV_SELECT)) {
			rel_base_disallow(res);
			if (rel_base_has_column_privileges(sql, res) == 0 && opt_where)
				return sql_error(sql, 02, SQLSTATE(42000) "UPDATE: insufficient privileges for user '%s' to update table '%s'",
								 get_string_global_var(sql, "current_user"), tname);
		}
		rel_base_use_tid(sql, res);
		if (opt_from) {
			dlist *fl = opt_from->data.lval;
			list *refs = list_append(new_exp_list(sql->sa), (char*) rel_name(res));
			sql_rel *tables = NULL;

			for (dnode *n = fl->h; n && res; n = n->next) {
				sql_rel *fnd = table_ref(query, n->data.sym, 0, refs);

				if (!fnd)
					return NULL;
				if (fnd && tables) {
					tables = rel_crossproduct(sql->sa, tables, fnd, op_join);
				} else {
					tables = fnd;
				}
			}
			if (!tables)
				return NULL;
			res = rel_crossproduct(sql->sa, res, tables, op_join);
			set_single(res);
		}
		if (opt_where) {
			if (!(r = rel_logical_exp(query, res, opt_where, sql_where)))
				return NULL;
			/* handle join */
			if (!opt_from && is_join(r->op))
				r->op = op_semi;
			else if (r->nrcols != res->nrcols)
				r = rel_project(sql->sa, r, rel_projections(sql, res, NULL, 1, 1));
		} else {	/* update all */
			r = res;
		}
		r = update_generate_assignments(query, t, r, bt, assignmentlist, "UPDATE");
		if (!r)
			return NULL;
		if (opt_returning) {
			/* we need to lookup all columns using the first column of the update result (row ids) */

			query_processed(query);
			set_processed(r);

			if (ol_first_node(t->columns)) {
				list *l = r->attr = sa_list(sql->sa);
				for (node *n = ol_first_node(t->columns); n; n = n->next) {
					sql_column *c = n->data;
					sql_exp *ne = NULL;

					append(l, ne = exp_column(sql->sa, t->base.name, c->base.name, &c->type, CARD_MULTI, c->null, is_column_unique(c), 0));
					rel_base_use(sql, bt, c->colnr);
					ne->nid = rel_base_nid(bt, c);
					ne->alias.label = ne->nid;
				}
			}
			list *pexps = sa_list(sql->sa);
			for (dnode *n = opt_returning->h; n; n = n->next) {
				sql_exp *ce = rel_column_exp(query, &r, n->data.sym, sql_sel | sql_no_subquery);
				if (ce == NULL)
					return NULL;
				pexps = append(pexps, ce);
			}
			r = rel_project(sql->sa, r, pexps);
			sql->type = Q_TABLE;
		} else {
			r = rel_update_count(sql, r);
			sql->type = Q_UPDATE;
		}
		return r;
	}
	return NULL;
}

sql_rel *
rel_delete(allocator *sa, sql_rel *t, sql_rel *deletes)
{
	sql_rel *r = rel_create(sa);
	if(!r)
		return NULL;

	r->op = op_delete;
	r->l = t;
	r->r = deletes;
	r->card = deletes ? deletes->card : CARD_MULTI;
	return r;
}

sql_rel *
rel_truncate(allocator *sa, sql_rel *t, int restart_sequences, int drop_action)
{
	sql_rel *r = rel_create(sa);
	list *exps = new_exp_list(sa);

	append(exps, exp_atom_int(sa, restart_sequences));
	append(exps, exp_atom_int(sa, drop_action));
	r->exps = exps;
	r->op = op_truncate;
	r->l = t;
	r->r = NULL;
	r->card = CARD_ATOM;
	return r;
}

static sql_rel *
delete_table(sql_query *query, dlist *qname, str alias, symbol *opt_where, dlist *opt_returning)
{
	mvc *sql = query->sql;
	char *sname = qname_schema(qname);
	char *tname = qname_schema_object(qname);
	sql_table *t = NULL;

	t = find_table_or_view_on_scope(sql, NULL, sname, tname, "DELETE FROM", false);
	if (update_allowed(sql, t, tname, "DELETE FROM", "delete from", 1) != NULL) {
		sql_rel *r = rel_basetable(sql, t, alias ? alias : tname), *bt = r;

		if (opt_where) {
			sql_exp *e;

			if (!table_privs(sql, t, PRIV_SELECT)) {
				rel_base_disallow(r);
				if (rel_base_has_column_privileges(sql, r) == 0)
					return sql_error(sql, 02, SQLSTATE(42000) "DELETE FROM: insufficient privileges for user '%s' to delete from table '%s'",
									 get_string_global_var(sql, "current_user"), tname);
			}
			rel_base_use_tid(sql, r);

			if (!(r = rel_logical_exp(query, r, opt_where, sql_where)))
				return NULL;
			e = exp_column(sql->sa, rel_name(r), TID, sql_fetch_localtype(TYPE_oid), CARD_MULTI, 0, 1, 1);
			e->nid = rel_base_nid(bt, NULL);
			e->alias.label = e->nid;
			r = rel_project(sql->sa, r, list_append(new_exp_list(sql->sa), e));
			r = rel_delete(sql->sa, /*rel_basetable(sql, t, alias ? alias : tname)*/rel_dup(bt), r);
		} else {	/* delete all */
			r = rel_delete(sql->sa, r, NULL);
		}
		r->exps = rel_projections(sql, r->r, NULL, 1, 1);
		set_processed(r);
		if (opt_returning) {
			query_processed(query);
			if (ol_first_node(t->columns)) {
				for (node *n = ol_first_node(t->columns); n; n = n->next) {
					sql_column *c = n->data;
					sql_exp *ne = NULL;

					append(r->exps, ne = exp_column(sql->sa, t->base.name, c->base.name, &c->type, CARD_MULTI, c->null, is_column_unique(c), 0));
					rel_base_use(sql, bt, c->colnr);
					ne->nid = rel_base_nid(bt, c);
					ne->alias.label = ne->nid;
				}
			}

			list *pexps = sa_list(sql->sa);
			for (dnode *n = opt_returning->h; n; n = n->next) {
				sql_exp *ce = rel_column_exp(query, &r, n->data.sym, sql_sel | sql_no_subquery);
				if (ce == NULL)
					return NULL;
				pexps = append(pexps, ce);
			}
			r = rel_project(sql->sa, r, pexps);
			sql->type = Q_TABLE;
		} else {
			r = rel_update_count(sql, r);
			sql->type = Q_UPDATE;
		}
		return r;
	}
	return NULL;
}

static sql_rel *
truncate_table(mvc *sql, dlist *qname, int restart_sequences, int drop_action)
{
	char *sname = qname_schema(qname);
	char *tname = qname_schema_object(qname);
	sql_table *t = NULL;

	t = find_table_or_view_on_scope(sql, NULL, sname, tname, "TRUNCATE", false);
	if (update_allowed(sql, t, tname, "TRUNCATE", "truncate", 2) != NULL)
		return rel_truncate(sql->sa, rel_basetable(sql, t, tname), restart_sequences, drop_action);
	return NULL;
}

static sql_rel *
rel_merge(mvc *sql, sql_rel *join, sql_rel *upd1, sql_rel *upd2)
{
	sql_rel *outer1 = upd1, *outer2 = upd2;
	if (upd2 && upd2->op == op_groupby)
		upd2 = upd2->l;
	if (upd1->op == op_groupby)
		upd1 = upd1->l;

	if (!upd2) { /* just insert or just update/delete ie just not match or match */
		if (upd1->op == op_groupby)
			upd1 = upd1->l;
		if (upd1->op == op_insert) {
			sql_rel *oj = upd1->l, *r = upd1->r;
			sql_exp *le = NULL;
			join->op = op_right;
			oj = rel_add_identity(sql, oj, &le);
			assert(oj == upd1->l);
			if (!le)
				return NULL;
			le = exp_ref(sql, le);
			set_has_nil(le);	/* full outer so possibly nulls */
			sql_exp *ce = exp_compare(sql->sa, le, exp_atom(sql->sa, atom_general(sql->sa, exp_subtype(le), NULL, 0)), cmp_equal );
			set_semantics(ce);
			while (r->l != join)
				r = r->l;
			r->l = join = rel_select( sql->sa, join, ce);
			return outer1;
		} else if (upd1->op == op_delete || upd1->op == op_update) { /* 2 cases (one for now, ie matched) ? */
			join->op = op_join;
			set_single(join);
			return outer1;
		}
	} else {
		sql_rel *oj = upd2->l, *r;
		sql_exp *le = NULL, *ce;
		join->op = op_right;
		oj = rel_add_identity(sql, oj, &le);
		assert(oj == upd2->l);
		if (!le)
			return NULL;
		le = exp_ref(sql, le);
		set_has_nil(le);
		ce = exp_compare(sql->sa, le, exp_atom(sql->sa, atom_general(sql->sa, exp_subtype(le), NULL, 0)), cmp_equal );
		set_semantics(ce);
		r = upd2->r;
		while (r->l != join)
			r = r->l;
		r->l = rel_select( sql->sa, join, ce);

		set_single(join);

		le = exp_ref(sql, le); /* matches ie select not null */
		set_has_nil(le);
		ce = exp_compare(sql->sa, le, exp_atom(sql->sa, atom_general(sql->sa, exp_subtype(le), NULL, 0)), cmp_notequal );
		set_semantics(ce);
		r = upd1->r;
		if (r->op == op_update) /* nested update */
			r = upd1->l;
		while (r->l != join)
			r = r->l;
		r->l = rel_select( sql->sa, join, ce);
		return rel_list(sql->sa, outer1, outer2);
	}
	assert (0);
	return NULL;
}

#define MERGE_UPDATE_DELETE 1
#define MERGE_INSERT        2

static sql_rel *
merge_into_table(sql_query *query, dlist *qname, str alias, symbol *tref, symbol *search_cond, dlist *merge_list)
{
	mvc *sql = query->sql;
	char *sname = qname_schema(qname), *tname = qname_schema_object(qname);
	sql_table *t = NULL;
	sql_rel *bt, *joined, *join_rel = NULL, *extra_project, *insert = NULL, *upd_del = NULL, *res = NULL;
	int processed = 0;
	const char *bt_name;

	assert(tref && search_cond && merge_list);

	if (!(t = find_table_or_view_on_scope(sql, NULL, sname, tname, "MERGE", false)))
		return NULL;
	if (isMergeTable(t))
		return sql_error(sql, 02, SQLSTATE(42000) "MERGE: merge statements not supported for merge tables");

	bt = rel_basetable(sql, t, alias ? alias : tname);
	if (!table_privs(sql, t, PRIV_SELECT)) {
		rel_base_disallow(bt);
		if (rel_base_has_column_privileges(sql, bt) == 0)
			return sql_error(sql, 02, SQLSTATE(42000) "MERGE: access denied for %s to table %s%s%s'%s'",
							 get_string_global_var(sql, "current_user"), t->s ? "'":"", t->s ? t->s->base.name : "", t->s ? "'.":"", tname);
	}
	joined = table_ref(query, tref, 0, NULL);
	if (!bt || !joined)
		return NULL;

	bt_name = rel_name(bt);
	if (rel_name(joined) && strcmp(bt_name, rel_name(joined)) == 0)
		return sql_error(sql, 02, SQLSTATE(42000) "MERGE: '%s' on both sides of the joining condition", bt_name);

	join_rel = rel_crossproduct(sql->sa, bt, joined, op_left);
	if (!(join_rel = rel_logical_exp(query, join_rel, search_cond, sql_where | sql_join | sql_merge)))
		return NULL;
	set_processed(join_rel);

	for (dnode *m = merge_list->h; m; m = m->next) {
		symbol *sym = m->data.sym, *opt_search, *action;
		tokens token = sym->token;
		dlist* dl = sym->data.lval, *sts;
		opt_search = dl->h->data.sym;
		action = dl->h->next->data.sym;
		sts = action->data.lval;
		sql_rel *sel_rel = NULL;

		if (token == SQL_MERGE_MATCH) {
			tokens uptdel = action->token;

			if ((processed & MERGE_UPDATE_DELETE) == MERGE_UPDATE_DELETE)
				return sql_error(sql, 02, SQLSTATE(42000) "MERGE: only one WHEN MATCHED clause is allowed");
			processed |= MERGE_UPDATE_DELETE;

			rel_base_use_tid(sql, bt);

			if ((processed & MERGE_INSERT) == MERGE_INSERT)
				join_rel = rel_dup(join_rel);

			if (uptdel == SQL_UPDATE) {
				if (!update_allowed(sql, t, tname, "MERGE", "update", 0))
					return NULL;
				sel_rel = join_rel;
				if (opt_search && !(sel_rel = rel_logical_exp(query, sel_rel, opt_search, sql_where | sql_merge)))
					return NULL;
				extra_project = rel_project(sql->sa, sel_rel, rel_projections(sql, join_rel, NULL, 1, 1));
				upd_del = update_generate_assignments(query, t, extra_project, rel_dup(bt)/*rel_basetable(sql, t, bt_name)*/, sts->h->data.lval, "MERGE");
			} else if (uptdel == SQL_DELETE) {
				if (!update_allowed(sql, t, tname, "MERGE", "delete", 1))
					return NULL;
				sql_exp *ne = exp_column(sql->sa, bt_name, TID, sql_fetch_localtype(TYPE_oid), CARD_MULTI, 0, 1, 1);
				ne->nid = rel_base_nid(bt, NULL);
				ne->alias.label = ne->nid;
				sel_rel = join_rel;
				if (opt_search && !(sel_rel = rel_logical_exp(query, sel_rel, opt_search, sql_where | sql_merge)))
					return NULL;
				extra_project = rel_project(sql->sa, sel_rel, list_append(new_exp_list(sql->sa), ne));
				upd_del = rel_delete(sql->sa, rel_dup(bt)/*rel_basetable(sql, t, bt_name)*/, extra_project);
			} else {
				assert(0);
			}
			if (!upd_del)
				return NULL;
			upd_del = rel_update_count(sql, upd_del);
		} else if (token == SQL_MERGE_NO_MATCH) {
			if ((processed & MERGE_INSERT) == MERGE_INSERT)
				return sql_error(sql, 02, SQLSTATE(42000) "MERGE: only one WHEN NOT MATCHED clause is allowed");
			processed |= MERGE_INSERT;

			if ((processed & MERGE_UPDATE_DELETE) == MERGE_UPDATE_DELETE)
				join_rel = rel_dup(join_rel);

			assert(action->token == SQL_INSERT);
			if (!insert_allowed(sql, t, tname, "MERGE", "insert"))
				return NULL;
			sel_rel = join_rel;
			if (opt_search && !(sel_rel = rel_logical_exp(query, sel_rel, opt_search, sql_where | sql_merge)))
				return NULL;
			extra_project = rel_project(sql->sa, sel_rel, rel_projections(sql, joined, NULL, 1, 0));
			if (!(insert = merge_generate_inserts(query, t, extra_project, sts->h->data.lval, sts->h->next->data.sym)))
				return NULL;
			sql_rel *ibt = rel_dup(bt);
			rel_base_use_all(query->sql, ibt);
			ibt = rewrite_basetable(query->sql, ibt, false);
			if (!(insert = rel_insert(query->sql, ibt, insert)))
				return NULL;
			insert = rel_update_count(sql, insert);
		} else {
			assert(0);
		}
	}

	if (!join_rel)
		return sql_error(sql, 02, SQLSTATE(42000) "MERGE: an insert or update or delete clause is required");
	if (processed == (MERGE_UPDATE_DELETE | MERGE_INSERT)) {
		res = rel_merge(sql, rel_dup(join_rel), upd_del, insert);
	} else if ((processed & MERGE_UPDATE_DELETE) == MERGE_UPDATE_DELETE) {
		res = rel_merge(sql, rel_dup(join_rel), upd_del, NULL);
	} else if ((processed & MERGE_INSERT) == MERGE_INSERT) {
		res = rel_merge(sql, rel_dup(join_rel), insert, NULL);
	} else {
		assert(0);
	}
	return res;
}

static list *
table_column_types(allocator *sa, sql_table *t)
{
	node *n;
	list *types = sa_list(sa);

	if (ol_first_node(t->columns)) for (n = ol_first_node(t->columns); n; n = n->next) {
		sql_column *c = n->data;
		if (c->base.name[0] != '%')
			append(types, &c->type);
	}
	return types;
}

static list *
table_column_names_and_defaults(allocator *sa, sql_table *t)
{
	node *n;
	list *types = sa_list(sa);

	if (ol_first_node(t->columns)) for (n = ol_first_node(t->columns); n; n = n->next) {
		sql_column *c = n->data;
		append(types, &c->base.name);
		append(types, c->def);
	}
	return types;
}

static sql_rel *
rel_import(mvc *sql, sql_table *t, const char *tsep, const char *rsep, const char *ssep, const char *ns, const char *filename, lng nr, lng offset, int best_effort, dlist *fwf_widths, int onclient, int escape, const char* decsep, const char *decskip)
{
	sql_rel *res;
	list *exps, *args;
	node *n;
	sql_subtype tpe;
	sql_exp *import;
	sql_subfunc *f = sql_find_func(sql, "sys", "copyfrom", 14, F_UNION, true, NULL);
	char *fwf_string = NULL;

	assert(f); /* we do expect copyfrom to be there */
	f->res = table_column_types(sql->sa, t);
 	sql_find_subtype(&tpe, "varchar", 0, 0);
	args = new_exp_list(sql->sa);
	append(args, exp_atom_ptr(sql->sa, t));
	append(args, exp_atom_str(sql->sa, tsep, &tpe));
	append(args, exp_atom_str(sql->sa, rsep, &tpe));
	append(args, exp_atom_str(sql->sa, ssep, &tpe));
	append(args, exp_atom_str(sql->sa, ns, &tpe));

	if (fwf_widths && dlist_length(fwf_widths) > 0) {
		dnode *dn;
		int ncol = 0;
		char *fwf_string_cur = fwf_string = sa_alloc(sql->sa, 20 * dlist_length(fwf_widths) + 1); /* a 64 bit int needs 19 characters in decimal representation plus the separator */

		if (!fwf_string)
			return NULL;
		for (dn = fwf_widths->h; dn; dn = dn->next) {
			fwf_string_cur += sprintf(fwf_string_cur, LLFMT"%c", dn->data.l_val, STREAM_FWF_FIELD_SEP);
			ncol++;
		}
		if (list_length(f->res) != ncol)
			return sql_error(sql, 02, SQLSTATE(3F000) "COPY INTO: fixed width import for %d columns but %d widths given.", list_length(f->res), ncol);
		*fwf_string_cur = '\0';
	}

	append(args, exp_atom_str(sql->sa, filename, &tpe));
	append(args, exp_atom_lng(sql->sa, nr));
	append(args, exp_atom_lng(sql->sa, offset));
	append(args, exp_atom_int(sql->sa, best_effort));
	append(args, exp_atom_str(sql->sa, fwf_string, &tpe));
	append(args, exp_atom_int(sql->sa, onclient));
	append(args, exp_atom_int(sql->sa, escape));
	append(args, exp_atom_str(sql->sa, decsep, &tpe));
	append(args, exp_atom_str(sql->sa, decskip, &tpe));

	import = exp_op(sql->sa, args, f);

	exps = new_exp_list(sql->sa);
	for (n = ol_first_node(t->columns); n; n = n->next) {
		sql_column *c = n->data;
		if (c->base.name[0] != '%') {
			sql_exp *e = exp_column(sql->sa, t->base.name, c->base.name, &c->type, CARD_MULTI, c->null, is_column_unique(c), 0);

			e->alias.label = -(sql->nid++);
			append(exps, e);
		}
	}
	res = rel_table_func(sql->sa, NULL, import, exps, TABLE_PROD_FUNC);
	return res;
}

static bool
valid_decsep(const char *s)
{
	if (strlen(s) != 1)
		return false;
	int c = s[0];
	if (c <= ' ' || c >= 127)
		return false;
	if (c == '-' || c == '+')
		return false;
	if (c >= '0' && c <= '9')
		return false;
	return true;
}

static sql_rel *
copyfrom(sql_query *query, CopyFromNode *copy)
{
	mvc *sql = query->sql;
	sql_rel *rel = NULL;
	char *sname = qname_schema(copy->qname);
	char *tname = qname_schema_object(copy->qname);
	sql_table *t = NULL, *nt = NULL;
	const char *tsep = copy->tsep;
	char *rsep = copy->rsep; /* not const, might need adjusting */
	const char *ssep = copy->ssep;
	const char *ns = copy->null_string ? copy->null_string : "null";
	lng nr = copy->nrows;
	lng offset = copy->offset;
	list *collist;
	int reorder = 0;
	dlist *headers = copy->header_list;

	if (strcmp(rsep, "\r\n") == 0) {
		/* silently fix it */
		rsep[0] = '\n';
		rsep[1] = '\0';
	} else if (strstr(rsep, "\r\n") != NULL) {
		return sql_error(sql, 02, SQLSTATE(42000)
				"COPY INTO: record separator contains '\\r\\n' but "
				"that will never match, use '\\n' instead");
	}

	if (
		strcmp(rsep, tsep) == 0
		|| (ssep && strcmp(rsep, ssep) == 0)
		|| (ssep && strcmp(tsep, ssep) == 0)
	) {
		return sql_error(sql, 02, SQLSTATE(42000)
				"COPY INTO: row separator, column separator and quote character must be distinct");
	}


	if (!valid_decsep(copy->decsep))
		return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO: invalid decimal separator");
	if (copy->decskip && !valid_decsep(copy->decskip))
		return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO: invalid thousands separator");
	if (copy->decskip && strcmp(copy->decsep, copy->decskip) == 0)
		return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO: decimal separator and thousands separator must be different");

	t = find_table_or_view_on_scope(sql, NULL, sname, tname, "COPY INTO", false);
	if (insert_allowed(sql, t, tname, "COPY INTO", "copy into") == NULL)
		return NULL;

	collist = check_table_columns(sql, t, copy->column_list, "COPY INTO", tname);
	if (!collist)
		return NULL;
	/* If we have a header specification use intermediate table, for
	 * column specification other then the default list we need to reorder
	 */
	nt = t;
	if (headers || collist != t->columns->l)
		reorder = 1;
	if (headers) {
		int has_formats = 0;

		switch (mvc_create_table(&nt, sql, t->s, tname, tt_table, 0, SQL_DECLARED_TABLE, CA_COMMIT, -1, 0)) {
			case -1:
				return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			case -2:
			case -3:
				return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO: transaction conflict detected");
			default:
				break;
		}
		for (dnode *n = headers->h; n; n = n->next) {
			dnode *dn = n->data.lval->h;
			char *cname = dn->data.sval;
			char *format = NULL;
			sql_column *cs = NULL;
			int res = LOG_OK;

			if (dn->next)
				format = dn->next->data.sval;
			if (!list_find_name(collist, cname)) {
				char *name;
				size_t len = strlen(cname) + 2;
				sql_subtype *ctype = sql_fetch_localtype(TYPE_oid);

				name = sa_alloc(sql->sa, len);
				snprintf(name, len, "%%cname");
				res = mvc_create_column(&cs, sql, nt, name, ctype);
			} else if (!format) {
				cs = find_sql_column(t, cname);
				res = mvc_create_column(&cs, sql, nt, cname, &cs->type);
			} else { /* load as string, parse later */
				sql_subtype *ctype = sql_fetch_localtype(TYPE_str);
				res = mvc_create_column(&cs, sql, nt, cname, ctype);
				has_formats = 1;
			}
			switch (res) {
				case -1:
					return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				case -2:
				case -3:
					return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO: transaction conflict detected");
				default:
					break;
			}
		}
		if (!has_formats)
			headers = NULL;
		reorder = 1;
	}
	if (copy->sources) {
		dnode *n = copy->sources->h;

		if (!copy->on_client && !copy_allowed(sql, 1)) {
			return sql_error(sql, 02, SQLSTATE(42000)
					 "COPY INTO: insufficient privileges: "
					 "COPY INTO from file(s) requires database administrator rights, "
					 "use 'COPY INTO \"%s\" FROM file ON CLIENT' instead", tname);
		}

		for (; n; n = n->next) {
			const char *fname = n->data.sval;
			sql_rel *nrel;

			if (!copy->on_client && fname && !MT_path_absolute(fname)) {
				char *fn = ATOMformat(TYPE_str, fname);
				sql_error(sql, 02, SQLSTATE(42000) "COPY INTO: filename must "
					  "have absolute path: %s", fn);
				GDKfree(fn);
				return NULL;
			}

			nrel = rel_import(sql, nt, tsep, rsep, ssep, ns, fname, nr, offset, copy->best_effort, copy->fwf_widths, copy->on_client, copy->escape, copy->decsep, copy->decskip);

			if (!rel)
				rel = nrel;
			else {
				sql_rel *orel = rel;
				rel = rel_setop_n_ary(sql->sa, append(append(sa_list(sql->sa), rel), nrel), op_munion );
				rel_setop_n_ary_set_exps(sql, rel, rel_projections(sql, orel, NULL, 0, 1), false);
				set_processed(rel);
			}
			if (!rel)
				return rel;
		}
	} else {
		assert(copy->on_client == 0);
		rel = rel_import(sql, nt, tsep, rsep, ssep, ns, NULL, nr, offset, copy->best_effort, NULL, copy->on_client, copy->escape, copy->decsep, copy->decskip);
	}
	if (headers) {
		dnode *n;
		node *m = rel->exps->h;
		list *nexps = sa_list(sql->sa);

		assert(is_project(rel->op) || is_base(rel->op));
		for (n = headers->h; n; n = n->next) {
			dnode *dn = n->data.lval->h;
			char *cname = dn->data.sval;
			sql_exp *e, *ne;

			if (!list_find_name(collist, cname))
				continue;
		       	e = m->data;
			if (dn->next) {
				char *format = dn->next->data.sval;
				sql_column *cs = find_sql_column(t, cname);
				sql_subtype st;
				sql_subfunc *f;
				list *args = sa_list(sql->sa);
				size_t l = strlen(cs->type.type->base.name);
				char *fname = sa_alloc(sql->sa, l+8);

				snprintf(fname, l+8, "str_to_%s", strcmp(cs->type.type->base.name, "timestamptz") == 0 ? "timestamp" : cs->type.type->base.name);
				sql_find_subtype(&st, "varchar", 0, 0);
				if (!(f = sql_bind_func_result(sql, "sys", fname, F_FUNC, true, &cs->type, 2, &st, &st)))
					return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO: '%s' missing for type %s", fname, cs->type.type->base.name);
				append(args, exp_ref(sql, e));
				append(args, exp_atom_clob(sql->sa, format));
				ne = exp_op(sql->sa, args, f);
				exp_setalias(ne, e->alias.label, exp_relname(e), exp_name(e));
			} else {
				ne = exp_ref(sql, e);
			}
			append(nexps, ne);
			m = m->next;
		}
		rel = rel_project(sql->sa, rel, nexps);
		reorder = 0;
	}

	if (!rel)
		return rel;
	if (reorder) {
		list *exps = rel_inserts(sql, t, rel, collist, 1, 1, "COPY INTO");
		if(!exps)
			return NULL;
		rel = rel_project(sql->sa, rel, exps);
	} else {
		rel->exps = rel_inserts(sql, t, rel, collist, 1, 0, "COPY INTO");
		if(!rel->exps)
			return NULL;
	}
	rel = rel_insert_table(query, t, tname, rel);
	if (rel)
		rel = rel_update_count(sql, rel);
	return rel;
}

static sql_rel *
bincopyfrom(sql_query *query, dlist *qname, dlist *columns, dlist *files, int onclient, endianness endian)
{
	mvc *sql = query->sql;
	char *sname = qname_schema(qname);
	char *tname = qname_schema_object(qname);
	sql_table *t = NULL;
	dnode *dn;
	node *n;
	sql_rel *res;
	list *exps, *args;
	sql_subtype strtpe;
	sql_exp *import;
	sql_subfunc *f = sql_find_func(sql, "sys", "copyfrombinary", 3, F_UNION, true, NULL);
	list *collist;
	list *typelist;

	assert(f);
	if (!copy_allowed(sql, 1))
		return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO: insufficient privileges: "
				"binary COPY INTO requires database administrator rights");

	t = find_table_or_view_on_scope(sql, NULL, sname, tname, "COPY INTO", false);
	if (insert_allowed(sql, t, tname, "COPY INTO", "copy into") == NULL)
		return NULL;
	if (files == NULL)
		return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO: must specify files");

	bool do_byteswap = (endian != endian_native && endian != OUR_ENDIANNESS);

	typelist = sa_list(sql->sa);
	collist = check_table_columns(sql, t, columns, "COPY BINARY INTO", tname);
	if (!collist || !typelist)
		return NULL;

	int column_count = list_length(collist);
	int file_count = dlist_length(files);
	if (column_count != file_count) {
		return sql_error(sql, 02, SQLSTATE(42000) "COPY BINARY INTO: "
			"number of files does not match number of columns: "
			"%d files, %d columns",
			file_count, column_count);
	}

	for (node *n = collist->h; n; n = n->next) {
		sql_column *c = n->data;
		sa_list_append(sql->sa, typelist, &c->type);
	}
	f->res = typelist;

 	sql_find_subtype(&strtpe, "varchar", 0, 0);
	args = append( append( append( append( new_exp_list(sql->sa),
		exp_atom_str(sql->sa, t->s?t->s->base.name:NULL, &strtpe)),
		exp_atom_str(sql->sa, t->base.name, &strtpe)),
		exp_atom_int(sql->sa, onclient)),
		exp_atom_bool(sql->sa, do_byteswap));

	for (dn = files->h; dn; dn = dn->next) {
		char *filename = dn->data.sval;
		append(args, exp_atom_str(sql->sa, filename, &strtpe));
	}

	import = exp_op(sql->sa, args, f);

	exps = new_exp_list(sql->sa);
	for (n = collist->h; n; n = n->next) {
		sql_column *c = n->data;
		sql_exp *e = exp_column(sql->sa, t->base.name, c->base.name, &c->type, CARD_MULTI, c->null, is_column_unique(c), 0);
		e->alias.label = -(sql->nid++);
		append(exps, e);
	}
	res = rel_table_func(sql->sa, NULL, import, exps, TABLE_PROD_FUNC);

	exps = rel_inserts(sql, t, res, collist, 1, 1, "COPY BINARY INTO");
	if(!exps)
		return NULL;
	res = rel_project(sql->sa, res, exps);

	res = rel_insert_table(query, t, t->base.name, res);
	if (res)
		res = rel_update_count(sql, res);
	return res;
}

static sql_rel *
copyfromloader(sql_query *query, dlist *qname, symbol *fcall)
{
	mvc *sql = query->sql;
	char *sname = qname_schema(qname);
	char *tname = qname_schema_object(qname);
	sql_subfunc *loader = NULL;
	sql_rel *rel = NULL;
	sql_table *t;
	list *mts;

	if (!copy_allowed(sql, 1))
		return sql_error(sql, 02, SQLSTATE(42000) "COPY LOADER INTO: insufficient privileges: "
				"COPY LOADER INTO requires database administrator rights");
	t = find_table_or_view_on_scope(sql, NULL, sname, tname, "COPY INTO", false);
	//TODO the COPY LOADER INTO should return an insert relation (instead of ddl) to handle partitioned tables properly
	if (insert_allowed(sql, t, tname, "COPY LOADER INTO", "copy loader into") == NULL)
		return NULL;
	if (isPartitionedByColumnTable(t) || isPartitionedByExpressionTable(t))
		return sql_error(sql, 02, SQLSTATE(42000) "COPY LOADER INTO: not possible for partitioned tables at the moment");
	if ((mts = partition_find_mergetables(sql, t))) {
		for (node *n = mts->h ; n ; n = n->next) {
			sql_part *pt = n->data;

			if ((isPartitionedByColumnTable(pt->t) || isPartitionedByExpressionTable(pt->t)))
				return sql_error(sql, 02, SQLSTATE(42000) "COPY LOADER INTO: not possible for tables child of partitioned tables at the moment");
		}
	}

	rel = rel_loader_function(query, fcall, new_exp_list(sql->sa), &loader);
	if (!rel || !loader)
		return NULL;

	loader->sname = t->s ? sa_strdup(sql->sa, t->s->base.name) : NULL;
	loader->tname = tname ? sa_strdup(sql->sa, tname) : NULL;
	loader->coltypes = table_column_types(sql->sa, t);
	loader->colnames = table_column_names_and_defaults(sql->sa, t);

	return rel;
}

static sql_rel *
copyto(sql_query *query, symbol *sq, const char *filename, dlist *seps, const char *null_string, int onclient)
{
	mvc *sql = query->sql;
	const char *tsep = seps->h->data.sval;
	const char *rsep = seps->h->next->data.sval;
	const char *ssep = (seps->h->next->next)?seps->h->next->next->data.sval:"\"";
	const char *ns = (null_string)?null_string:"null";
	sql_exp *tsep_e, *rsep_e, *ssep_e, *ns_e, *fname_e, *oncl_e;
	exp_kind ek = {type_value, card_relation, TRUE};
	sql_rel *r = rel_subquery(query, sq, ek);

	if (!r)
		return NULL;
	r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 0));
	if (!(r->exps = check_distinct_exp_names(sql, r->exps)))
		return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO: duplicate column names in subquery column list");

	tsep_e = exp_atom_clob(sql->sa, tsep);
	rsep_e = exp_atom_clob(sql->sa, rsep);
	ssep_e = exp_atom_clob(sql->sa, ssep);
	ns_e = exp_atom_clob(sql->sa, ns);
	oncl_e = exp_atom_int(sql->sa, onclient);
	fname_e = filename?exp_atom_clob(sql->sa, filename):NULL;

	if (!onclient && filename) {
		struct stat fs;
		if (!copy_allowed(sql, 0))
			return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO: insufficient privileges: "
					 "COPY INTO file requires database administrator rights, "
					 "use 'COPY ... INTO file ON CLIENT' instead");
		if (filename && !MT_path_absolute(filename))
			return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO ON SERVER: filename must "
					 "have absolute path: %s", filename);
		if (lstat(filename, &fs) == 0)
			return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO ON SERVER: file already "
					 "exists: %s", filename);
	}

	sql_rel *rel = rel_create(sql->sa);
	list *exps = new_exp_list(sql->sa);
	if(!rel || !exps)
		return NULL;

	/* With regular COPY INTO <file>, the first argument is a string.
	   With COPY INTO BINARY, it is an int. */
	append(exps, tsep_e);
	append(exps, rsep_e);
	append(exps, ssep_e);
	append(exps, ns_e);
	if (fname_e) {
		append(exps, fname_e);
		append(exps, oncl_e);
	}
	rel->l = r;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = ddl_output;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

static sql_rel *
bincopyto(sql_query *query, symbol *qry, endianness endian, dlist *filenames, int on_client)
{
	mvc *sql = query->sql;

	/* First emit code for the subquery.
	   Don't know what this is for, copy pasted it from copyto(): */
	exp_kind ek = { type_value, card_relation, TRUE};
	sql_rel *sub = rel_subquery(query, qry, ek);
	if (!sub)
		return NULL;
	/* Again, copy-pasted. copyto() uses this to check for duplicate column names
	   but we don't care about that here. */
	sub = rel_project(sql->sa, sub, rel_projections(sql, sub, NULL, 1, 0));

	int nrcolumns = sub->nrcols;
	int nrfilenames = filenames->cnt;
	if (nrcolumns != nrfilenames) {
		return sql_error(sql, 02, "COPY INTO BINARY: need %d file names, got %d",
			nrcolumns, nrfilenames);
	}

	sql_rel *rel = rel_create(sql->sa);
	list *exps = new_exp_list(sql->sa);
	if (!rel || !exps)
		return NULL;

	/* With regular COPY INTO <file>, the first argument is a string.
	   With COPY INTO BINARY, it is an int. */
	append(exps, exp_atom_int(sql->sa, endian));
	append(exps, exp_atom_int(sql->sa, on_client));

	for (dnode *n = filenames->h; n != NULL; n = n->next) {
		const char *filename = n->data.sval;
		/* Again, copied from copyto() */
		if (!on_client && filename) {
			struct stat fs;
			if (!copy_allowed(sql, 0))
				return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO: insufficient privileges: "
						"COPY INTO file requires database administrator rights, "
						"use 'COPY ... INTO file ON CLIENT' instead");
			if (filename && !MT_path_absolute(filename))
				return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO ON SERVER: filename must "
						"have absolute path: %s", filename);
			if (lstat(filename, &fs) == 0)
				return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO ON SERVER: file already "
						"exists: %s", filename);
		}
		append(exps, exp_atom_clob(sql->sa, filename));
	}

	rel->l = sub;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = ddl_output;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;

	return rel;
}

sql_exp *
rel_parse_val(mvc *m, sql_schema *sch, char *query, sql_subtype *tpe, char emode, sql_rel *from)
{
	sql_exp *e = NULL;
	buffer *b;
	char *n;
	size_t len = _strlen(query);
	exp_kind ek = {type_value, card_value, FALSE};
	stream *s;
	bstream *bs;

	b = malloc(sizeof(buffer));
	len += 8; /* add 'select ;' */
	n = malloc(len + 1 + 1);
	if(!b || !n) {
		free(b);
		free(n);
		return sql_error(m, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	snprintf(n, len + 2, "select %s;\n", query);
	len++;
	buffer_init(b, n, len);
	s = buffer_rastream(b, "sqlstatement");
	if(!s) {
		buffer_destroy(b);
		return sql_error(m, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	bs = bstream_create(s, b->len);
	if(bs == NULL) {
		buffer_destroy(b);
		return sql_error(m, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	mvc o = *m;
	scanner_init(&m->scanner, bs, NULL);
	m->scanner.mode = LINE_1;
	bstream_next(m->scanner.rs);

	m->qc = NULL;
	if (sch)
		m->session->schema = sch;
	m->emode = emode;
	m->params = NULL;
	m->sym = NULL;
	m->errstr[0] = '\0';
	m->session->status = 0;
	/* via views we give access to protected objects */
	m->user_id = USER_MONETDB;

	(void) sqlparse(m);

	/* get out the single value as we don't want an enclosing projection! */
	if (m->sym && m->sym->token == SQL_SELECT) {
		SelectNode *sn = (SelectNode *)m->sym;
		if (sn->selection->h->data.sym->token == SQL_COLUMN || sn->selection->h->data.sym->token == SQL_IDENT) {
			sql_rel *r = from;
			symbol* sq = sn->selection->h->data.sym->data.lval->h->data.sym;
			sql_query *query = query_create(m);
			e = rel_value_exp2(query, &r, sq, sql_sel | sql_values, ek);
			if (e && tpe)
				e = exp_check_type(m, tpe, from, e, type_cast);
		}
	}
	buffer_destroy(b);
	bstream_destroy(m->scanner.rs);

	m->sym = NULL;
	o.frames = m->frames;	/* may have been realloc'ed */
	o.sizeframes = m->sizeframes;
	if (m->session->status || m->errstr[0]) {
		int status = m->session->status;

		strcpy(o.errstr, m->errstr);
		*m = o;
		m->session->status = status;
	} else {
		unsigned int label = m->label;

		while (m->topframes > o.topframes)
			clear_frame(m, m->frames[--m->topframes]);
		*m = o;
		m->label = label;
	}
	return e;
}

sql_rel *
rel_updates(sql_query *query, symbol *s)
{
	mvc *sql = query->sql;
	sql_rel *ret = NULL;

	switch (s->token) {
	case SQL_COPYFROM:
	{
		CopyFromNode *copy = (CopyFromNode*)s;
		ret = copyfrom(query, copy);
		sql->type = Q_UPDATE;
	}
		break;
	case SQL_BINCOPYFROM:
	{
		dlist *l = s->data.lval;

		ret = bincopyfrom(query, l->h->data.lval, l->h->next->data.lval, l->h->next->next->data.lval, l->h->next->next->next->data.i_val, (endianness) l->h->next->next->next->next->data.i_val);
		sql->type = Q_UPDATE;
	}
		break;
	case SQL_COPYLOADER:
	{
		dlist *l = s->data.lval;
		dlist *qname = l->h->data.lval;
		symbol *sym = l->h->next->data.sym;
		sql_rel *rel = copyfromloader(query, qname, sym);

		if (rel)
			ret = rel_psm_stmt(sql->sa, exp_rel(sql, rel));
		sql->type = Q_SCHEMA;
	}
		break;
	case SQL_COPYINTO:
	{
		dlist *l = s->data.lval;

		ret = copyto(query, l->h->data.sym, l->h->next->data.sval, l->h->next->next->data.lval, l->h->next->next->next->data.sval, l->h->next->next->next->next->data.i_val);
		sql->type = Q_UPDATE;
	}
		break;
	case SQL_BINCOPYINTO:
	{
		dlist *l = s->data.lval;
		symbol *qry = l->h->data.sym;
		endianness endian = l->h->next->data.i_val;
		dlist *files = l->h->next->next->data.lval;
		int on_client = l->h->next->next->next->data.i_val;

		ret = bincopyto(query, qry, endian, files, on_client);
		sql->type = Q_UPDATE;
	}
		break;
	case SQL_INSERT:
	{
		dlist *l = s->data.lval;

		ret = insert_into(query, l->h->data.lval, l->h->next->data.lval, l->h->next->next->data.sym, l->h->next->next->next->data.lval);
	}
		break;
	case SQL_UPDATE:
	{
		dlist *l = s->data.lval;

		ret = update_table(query, l->h->data.lval, l->h->next->data.sval, l->h->next->next->data.lval,
						   l->h->next->next->next->data.sym, l->h->next->next->next->next->data.sym, l->h->next->next->next->next->next->data.lval);
	}
		break;
	case SQL_DELETE:
	{
		dlist *l = s->data.lval;

		ret = delete_table(query, l->h->data.lval, l->h->next->data.sval, l->h->next->next->data.sym, l->h->next->next->next->data.lval);
	}
		break;
	case SQL_TRUNCATE:
	{
		dlist *l = s->data.lval;

		int restart_sequences = l->h->next->data.i_val;
		int drop_action = l->h->next->next->data.i_val;
		ret = truncate_table(sql, l->h->data.lval, restart_sequences, drop_action);
		sql->type = Q_UPDATE;
	}
		break;
	case SQL_MERGE:
	{
		dlist *l = s->data.lval;

		ret = merge_into_table(query, l->h->data.lval, l->h->next->data.sval, l->h->next->next->data.sym,
							   l->h->next->next->next->data.sym, l->h->next->next->next->next->data.lval);
		sql->type = Q_UPDATE;
	} break;
	default:
		return sql_error(sql, 01, SQLSTATE(42000) "Updates statement unknown Symbol(%p)->token = %s", s, token2string(s->token));
	}
	query_processed(query);
	return ret;
}
