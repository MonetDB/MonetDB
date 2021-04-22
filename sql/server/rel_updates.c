/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "rel_unnest.h"
#include "rel_optimizer.h"
#include "rel_dump.h"
#include "rel_psm.h"
#include "sql_symbol.h"
#include "rel_prop.h"

static sql_exp *
insert_value(sql_query *query, sql_column *c, sql_rel **r, symbol *s, const char* action)
{
	mvc *sql = query->sql;
	if (s->token == SQL_NULL) {
		return exp_atom(sql->sa, atom_general(sql->sa, &c->type, NULL));
	} else if (s->token == SQL_DEFAULT) {
		if (c->def) {
			sql_exp *e = rel_parse_val(sql, c->t->s, c->def, &c->type, sql->emode, NULL);
			if (!e || (e = exp_check_type(sql, &c->type, r ? *r : NULL, e, type_equal)) == NULL)
				return sql_error(sql, 02, SQLSTATE(HY005) "%s: default expression could not be evaluated", action);
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
	if (list_length(i->columns) <= 1 || i->type == no_idx) {
		/* dummy append */
		inserts->r = ins = rel_project(sql->sa, ins, rel_projections(sql, ins, NULL, 1, 1));
		list_append(ins->exps, exp_label(sql->sa, exp_atom_lng(sql->sa, 0), ++sql->label));
		return inserts;
	}

	it = sql_bind_localtype("int");
	lng = sql_bind_localtype("lng");
	for (m = i->columns->h; m; m = m->next) {
		sql_kc *c = m->data;
		sql_exp *e = list_fetch(ins->exps, c->c->colnr);
		e = exp_ref(sql, e);

		if (h && i->type == hash_idx)  {
			list *exps = new_exp_list(sql->sa);
			sql_subfunc *xor = sql_bind_func_result(sql, "sys", "rotate_xor_hash", F_FUNC, lng, 3, lng, it, &c->c->type);

			append(exps, h);
			append(exps, exp_atom_int(sql->sa, bits));
			append(exps, e);
			h = exp_op(sql->sa, exps, xor);
		} else if (h)  { /* order preserving hash */
			sql_exp *h2;
			sql_subfunc *lsh = sql_bind_func_result(sql, "sys", "left_shift", F_FUNC, lng, 2, lng, it);
			sql_subfunc *lor = sql_bind_func_result(sql, "sys", "bit_or", F_FUNC, lng, 2, lng, lng);
			sql_subfunc *hf = sql_bind_func_result(sql, "sys", "hash", F_FUNC, lng, 1, &c->c->type);

			h = exp_binop(sql->sa, h, exp_atom_int(sql->sa, bits), lsh);
			h2 = exp_unop(sql->sa, e, hf);
			h = exp_binop(sql->sa, h, h2, lor);
		} else {
			sql_subfunc *hf = sql_bind_func_result(sql, "sys", "hash", F_FUNC, lng, 1, &c->c->type);
			h = exp_unop(sql->sa, e, hf);
			if (i->type == oph_idx)
				break;
		}
	}
	/* append inserts to hash */
	inserts->r = ins = rel_project(sql->sa, ins, rel_projections(sql, ins, NULL, 1, 1));
	list_append(ins->exps, h);
	exp_setname(sql->sa, h, alias, iname);
	return inserts;
}

static sql_rel *
rel_insert_join_idx(mvc *sql, const char* alias, sql_idx *i, sql_rel *inserts)
{
	char *iname = sa_strconcat( sql->sa, "%", i->base.name);
	int need_nulls = 0;
	node *m, *o;
	sql_trans *tr = sql->session->tr;
	sql_key *rk = (sql_key*)os_find_id(tr->cat->objects, tr, ((sql_fkey*)i->key)->rkey);
	sql_rel *rt = rel_basetable(sql, rk->t, rk->t->base.name);

	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *or = sql_bind_func_result(sql, "sys", "or", F_FUNC, bt, 2, bt, bt);

	sql_rel *_nlls = NULL, *nnlls, *ins = inserts->r;
	sql_exp *lnll_exps = NULL, *rnll_exps = NULL, *e;
	list *join_exps = new_exp_list(sql->sa), *pexps;

	assert(is_project(ins->op) || ins->op == op_table);
	for (m = i->columns->h; m; m = m->next) {
		sql_kc *c = m->data;

		if (c->c->null)
			need_nulls = 1;
	}
	/* NULL and NOT NULL, for 'SIMPLE MATCH' semantics */
	/* AND joins expressions */
	for (m = i->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *c = m->data;
		sql_kc *rc = o->data;
		sql_subfunc *isnil = sql_bind_func(sql, "sys", "isnull", &c->c->type, NULL, F_FUNC);
		sql_exp *_is = list_fetch(ins->exps, c->c->colnr), *lnl, *rnl, *je;
		if (rel_base_use(sql, rt, rc->c->colnr)) {
			/* TODO add access error */
			return NULL;
		}
		sql_exp *rtc = exp_column(sql->sa, rel_name(rt), rc->c->base.name, &rc->c->type, CARD_MULTI, rc->c->null, 0);

		_is = exp_ref(sql, _is);
		lnl = exp_unop(sql->sa, _is, isnil);
		set_has_no_nil(lnl);
		rnl = exp_unop(sql->sa, _is, isnil);
		set_has_no_nil(rnl);
		if (need_nulls) {
			if (lnll_exps) {
				lnll_exps = exp_binop(sql->sa, lnll_exps, lnl, or);
				rnll_exps = exp_binop(sql->sa, rnll_exps, rnl, or);
			} else {
				lnll_exps = lnl;
				rnll_exps = rnl;
			}
		}

		if (rel_convert_types(sql, rt, ins, &rtc, &_is, 1, type_equal) < 0)
			return NULL;
		je = exp_compare(sql->sa, rtc, _is, cmp_equal);
		append(join_exps, je);
	}
	if (need_nulls) {
		_nlls = rel_select( sql->sa, rel_dup(ins),
				exp_compare(sql->sa, lnll_exps, exp_atom_bool(sql->sa, 1), cmp_equal ));
		nnlls = rel_select( sql->sa, rel_dup(ins),
				exp_compare(sql->sa, rnll_exps, exp_atom_bool(sql->sa, 0), cmp_equal ));
		_nlls = rel_project(sql->sa, _nlls, rel_projections(sql, _nlls, NULL, 1, 1));
		/* add constant value for NULLS */
		e = exp_atom(sql->sa, atom_general(sql->sa, sql_bind_localtype("oid"), NULL));
		exp_setname(sql->sa, e, alias, iname);
		append(_nlls->exps, e);
	} else {
		nnlls = ins;
	}

	pexps = rel_projections(sql, nnlls, NULL, 1, 1);
	nnlls = rel_crossproduct(sql->sa, nnlls, rt, op_join);
	nnlls->exps = join_exps;
	nnlls = rel_project(sql->sa, nnlls, pexps);
	/* add row numbers */
	e = exp_column(sql->sa, rel_name(rt), TID, sql_bind_localtype("oid"), CARD_MULTI, 0, 1);
	rel_base_use_tid(sql, rt);
	exp_setname(sql->sa, e, alias, iname);
	append(nnlls->exps, e);

	if (need_nulls) {
		rel_destroy(ins);
		rt = inserts->r = rel_setop(sql->sa, _nlls, nnlls, op_union );
		rel_setop_set_exps(sql, rt, rel_projections(sql, nnlls, NULL, 1, 1));
		set_processed(rt);
	} else {
		inserts->r = nnlls;
	}
	return inserts;
}

static sql_rel *
rel_insert_idxs(mvc *sql, sql_table *t, const char* alias, sql_rel *inserts)
{
	sql_rel *p = inserts->r;

	if (!ol_length(t->idxs))
		return inserts;

	inserts->r = rel_label(sql, inserts->r, 1);
	for (node *n = ol_first_node(t->idxs); n; n = n->next) {
		sql_idx *i = n->data;

		if (hash_index(i->type) || i->type == no_idx) {
			rel_insert_hash_idx(sql, alias, i, inserts);
		} else if (i->type == join_idx) {
			rel_insert_join_idx(sql, alias, i, inserts);
		}
	}
	if (inserts->r != p) {
		sql_rel *r = rel_create(sql->sa);
		if(!r)
			return NULL;

		r->op = op_insert;
		r->l = rel_dup(p);
		r->r = inserts;
		r->card = inserts->card;
		r->flag |= UPD_COMP; /* mark as special update */
		return r;
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
		return rel_insert_idxs(sql, tab, rel_name(t), r);
	return r;
}

static sql_rel *
rel_insert_table(sql_query *query, sql_table *t, char *name, sql_rel *inserts)
{
	sql_rel *rel = rel_basetable(query->sql, t, name);
	rel_base_use_all(query->sql, rel);
	rel = rewrite_basetable(query->sql, rel);
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

	if (r->exps) {
		if (!copy) {
			for (n = r->exps->h, m = collist->h; n && m; n = n->next, m = m->next) {
				sql_column *c = m->data;
				sql_exp *e = n->data;

				if (inserts[c->colnr])
					return sql_error(sql, 02, SQLSTATE(42000) "%s: column '%s' specified more than once", action, c->base.name);
				if (!(inserts[c->colnr] = exp_check_type(sql, &c->type, r, e, type_equal)))
					return NULL;
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
						return sql_error(sql, 02, SQLSTATE(HY005) "%s: default expression could not be evaluated", action);
				} else {
					atom *a = atom_general(sql->sa, &c->type, NULL);
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
	/* now rewrite project exps in proper table order */
	exps = new_exp_list(sql->sa);
	for (i = 0; i<len; i++)
		list_append(exps, inserts[i]);
	return exps;
}

sql_table *
insert_allowed(mvc *sql, sql_table *t, char *tname, char *op, char *opname)
{
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
	} else if (isRemote(t)) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: cannot %s remote table '%s' from this server at the moment", op, opname, tname);
	} else if (isReplicaTable(t)) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: cannot %s replica table '%s'", op, opname, tname);
	} else if (t->access == TABLE_READONLY) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: cannot %s read only table '%s'", op, opname, tname);
	}
	if (t && !isTempTable(t) && store_readonly(sql->session->tr->store))
		return sql_error(sql, 02, SQLSTATE(42000) "%s: %s table '%s' not allowed in readonly mode", op, opname, tname);

	if (!table_privs(sql, t, PRIV_INSERT)) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: insufficient privileges for user '%s' to %s table '%s'", op, get_string_global_var(sql, "current_user"), opname, tname);
	}
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
	} else if (isRemote(t)) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: cannot %s remote table '%s' from this server at the moment", op, opname, tname);
	} else if (isReplicaTable(t)) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: cannot %s replica table '%s'", op, opname, tname);
	} else if (t->access == TABLE_READONLY || t->access == TABLE_APPENDONLY) {
		return sql_error(sql, 02, SQLSTATE(42000) "%s: cannot %s read or append only table '%s'", op, opname, tname);
	}
	if (t && !isTempTable(t) && store_readonly(sql->session->tr->store))
		return sql_error(sql, 02, SQLSTATE(42000) "%s: %s table '%s' not allowed in readonly mode", op, opname, tname);
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

		r = rel_subquery(query, NULL, val_or_q, ek);
		rowcount++;
	}
	if (!r)
		return NULL;

	/* In case of missing project, order by or distinct, we need to add
	   and projection */
	if (r->op != op_project || r->r || need_distinct(r))
		r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 0));
	if ((r->exps && list_length(r->exps) != list_length(collist)) ||
		(!r->exps && collist))
		return sql_error(sql, 02, SQLSTATE(21S01) "%s: query result doesn't match number of columns in table '%s'", action, t->base.name);

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
		return sql_error(sql, 02, SQLSTATE(42000) "MERGE: sub-queries not yet supported in INSERT clauses inside MERGE statements");
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
insert_into(sql_query *query, dlist *qname, dlist *columns, symbol *val_or_q)
{
	mvc *sql = query->sql;
	char *sname = qname_schema(qname);
	char *tname = qname_schema_object(qname);
	sql_table *t = NULL;
	sql_rel *r = NULL;

	t = find_table_or_view_on_scope(sql, NULL, sname, tname, "INSERT INTO", false);
	if (insert_allowed(sql, t, tname, "INSERT INTO", "insert into") == NULL)
		return NULL;
	r = insert_generate_inserts(query, t, columns, val_or_q, "INSERT INTO");
	if(!r)
		return NULL;
	return rel_insert_table(query, t, t->base.name, r);
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
	sql_rel *ups = updates->r;

	assert(is_project(ups->op) || ups->op == op_table);
	if (list_length(i->columns) <= 1 || i->type == no_idx) {
		h = exp_label(sql->sa, exp_atom_lng(sql->sa, 0), ++sql->label);
	} else {
		it = sql_bind_localtype("int");
		lng = sql_bind_localtype("lng");
		for (m = i->columns->h; m; m = m->next) {
			sql_kc *c = m->data;
			sql_exp *e = list_fetch(ups->exps, c->c->colnr+1);
			e = exp_ref(sql, e);

			if (h && i->type == hash_idx)  {
				list *exps = new_exp_list(sql->sa);
				sql_subfunc *xor = sql_bind_func_result(sql, "sys", "rotate_xor_hash", F_FUNC, lng, 3, lng, it, &c->c->type);

				append(exps, h);
				append(exps, exp_atom_int(sql->sa, bits));
				append(exps, e);
				h = exp_op(sql->sa, exps, xor);
			} else if (h)  { /* order preserving hash */
				sql_exp *h2;
				sql_subfunc *lsh = sql_bind_func_result(sql, "sys", "left_shift", F_FUNC, lng, 2, lng, it);
				sql_subfunc *lor = sql_bind_func_result(sql, "sys", "bit_or", F_FUNC, lng, 2, lng, lng);
				sql_subfunc *hf = sql_bind_func_result(sql, "sys", "hash", F_FUNC, lng, 1, &c->c->type);

				h = exp_binop(sql->sa, h, exp_atom_int(sql->sa, bits), lsh);
				h2 = exp_unop(sql->sa, e, hf);
				h = exp_binop(sql->sa, h, h2, lor);
			} else {
				sql_subfunc *hf = sql_bind_func_result(sql, "sys", "hash", F_FUNC, lng, 1, &c->c->type);
				h = exp_unop(sql->sa, e, hf);
				if (i->type == oph_idx)
					break;
			}
		}
	}
	/* append hash to updates */
	updates->r = ups = rel_project(sql->sa, ups, rel_projections(sql, ups, NULL, 1, 1));
	list_append(ups->exps, h);
	exp_setname(sql->sa, h, alias, iname);

	if (!updates->exps)
		updates->exps = new_exp_list(sql->sa);
	append(updates->exps, exp_column(sql->sa, alias, iname, lng, CARD_MULTI, 0, 0));
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

	int need_nulls = 0;
	node *m, *o;
	sql_trans *tr = sql->session->tr;
	sql_key *rk = (sql_key*)os_find_id(tr->cat->objects, tr, ((sql_fkey*)i->key)->rkey);
	sql_rel *rt = rel_basetable(sql, rk->t, sa_strdup(sql->sa, nme));

	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *or = sql_bind_func_result(sql, "sys", "or", F_FUNC, bt, 2, bt, bt);

	sql_rel *_nlls = NULL, *nnlls, *ups = updates->r;
	sql_exp *lnll_exps = NULL, *rnll_exps = NULL, *e;
	list *join_exps = new_exp_list(sql->sa), *pexps;

	assert(is_project(ups->op) || ups->op == op_table);
	for (m = i->columns->h; m; m = m->next) {
		sql_kc *c = m->data;

		if (c->c->null)
			need_nulls = 1;
	}
	for (m = i->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *c = m->data;
		sql_kc *rc = o->data;
		sql_subfunc *isnil = sql_bind_func(sql, "sys", "isnull", &c->c->type, NULL, F_FUNC);
		sql_exp *upd = list_fetch(ups->exps, c->c->colnr + 1), *lnl, *rnl, *je;
		if (rel_base_use(sql, rt, rc->c->colnr)) {
			/* TODO add access error */
			return NULL;
		}
		sql_exp *rtc = exp_column(sql->sa, rel_name(rt), rc->c->base.name, &rc->c->type, CARD_MULTI, rc->c->null, 0);

		/* FOR MATCH FULL/SIMPLE/PARTIAL see above */
		/* Currently only the default MATCH SIMPLE is supported */
		upd = exp_ref(sql, upd);
		lnl = exp_unop(sql->sa, upd, isnil);
		set_has_no_nil(lnl);
		rnl = exp_unop(sql->sa, upd, isnil);
		set_has_no_nil(rnl);
		if (need_nulls) {
			if (lnll_exps) {
				lnll_exps = exp_binop(sql->sa, lnll_exps, lnl, or);
				rnll_exps = exp_binop(sql->sa, rnll_exps, rnl, or);
			} else {
				lnll_exps = lnl;
				rnll_exps = rnl;
			}
		}
		if (rel_convert_types(sql, rt, updates, &rtc, &upd, 1, type_equal) < 0) {
			list_destroy(join_exps);
			return NULL;
		}
		je = exp_compare(sql->sa, rtc, upd, cmp_equal);
		append(join_exps, je);
	}
	if (need_nulls) {
		_nlls = rel_select( sql->sa, rel_dup(ups),
				exp_compare(sql->sa, lnll_exps, exp_atom_bool(sql->sa, 1), cmp_equal ));
		nnlls = rel_select( sql->sa, rel_dup(ups),
				exp_compare(sql->sa, rnll_exps, exp_atom_bool(sql->sa, 0), cmp_equal ));
		_nlls = rel_project(sql->sa, _nlls, rel_projections(sql, _nlls, NULL, 1, 1));
		/* add constant value for NULLS */
		e = exp_atom(sql->sa, atom_general(sql->sa, sql_bind_localtype("oid"), NULL));
		exp_setname(sql->sa, e, alias, iname);
		append(_nlls->exps, e);
	} else {
		nnlls = ups;
	}

	pexps = rel_projections(sql, nnlls, NULL, 1, 1);
	nnlls = rel_crossproduct(sql->sa, nnlls, rt, op_join);
	nnlls->exps = join_exps;
	nnlls->flag = LEFT_JOIN;
	nnlls = rel_project(sql->sa, nnlls, pexps);
	/* add row numbers */
	e = exp_column(sql->sa, rel_name(rt), TID, sql_bind_localtype("oid"), CARD_MULTI, 0, 1);
	rel_base_use_tid(sql, rt);
	exp_setname(sql->sa, e, alias, iname);
	append(nnlls->exps, e);

	if (need_nulls) {
		rel_destroy(ups);
		rt = updates->r = rel_setop(sql->sa, _nlls, nnlls, op_union );
		rel_setop_set_exps(sql, rt, rel_projections(sql, nnlls, NULL, 1, 1));
		set_processed(rt);
	} else {
		updates->r = nnlls;
	}
	if (!updates->exps)
		updates->exps = new_exp_list(sql->sa);
	append(updates->exps, exp_column(sql->sa, alias, iname, sql_bind_localtype("oid"), CARD_MULTI, 0, 0));
	return updates;
}

/* for cascade of updates we change the 'relup' relations into
 * a ddl_list of update relations.
 */
static sql_rel *
rel_update_idxs(mvc *sql, const char *alias, sql_table *t, sql_rel *relup)
{
	sql_rel *p = relup->r;

	if (!ol_length(t->idxs))
		return relup;

	for (node *n = ol_first_node(t->idxs); n; n = n->next) {
		sql_idx *i = n->data;

		/* check if update is needed,
		 * ie atleast on of the idx columns is updated
		 */
		if (relup->exps && is_idx_updated(i, relup->exps) == 0)
			continue;

		/*
		 * relup->exps isn't set in case of alter statements!
		 * Ie todo check for new indices.
		 */

		if (hash_index(i->type) || i->type == no_idx) {
			rel_update_hash_idx(sql, alias, i, relup);
		} else if (i->type == join_idx) {
			rel_update_join_idx(sql, alias, i, relup);
		}
	}
	if (relup->r != p) {
		sql_rel *r = rel_create(sql->sa);
		if(!r)
			return NULL;
		r->op = op_update;
		r->l = rel_dup(p);
		r->r = relup;
		r->card = relup->card;
		r->flag |= UPD_COMP; /* mark as special update */
		return r;
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
			if (ol_length(tab->idxs) && !v)
				v = exp_column(sql->sa, alias, c->base.name, &c->type, CARD_MULTI, c->null, 0);
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
	sql_table *mt = NULL;
	sql_exp **updates = SA_ZNEW_ARRAY(sql->sa, sql_exp*, ol_length(t->columns));
	list *exps, *pcols = NULL;
	dnode *n;
	const char *rname = NULL;

	if (isPartitionedByColumnTable(t) || isPartitionedByExpressionTable(t))
		mt = t;
	else if (partition_find_part(sql->session->tr, t, NULL))
		mt = partition_find_part(sql->session->tr, t, NULL)->t;

	if (mt && isPartitionedByColumnTable(mt)) {
		pcols = sa_list(sql->sa);
		int *nid = sa_alloc(sql->sa, sizeof(int));
		*nid = mt->part.pcol->colnr;
		list_append(pcols, nid);
	} else if (mt && isPartitionedByExpressionTable(mt)) {
		pcols = mt->part.pexp->cols;
	}
	/* first create the project */
	exps = list_append(new_exp_list(sql->sa), exp_column(sql->sa, rname = rel_name(r), TID, sql_bind_localtype("oid"), CARD_MULTI, 0, 1));

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
				rel_val = rel_subquery(query, NULL, a, ek);
				if (r)
					r = query_pop_outer(query);
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
				if (single) {
					v = exp_column(sql->sa, NULL, exp_name(v), exp_subtype(v), v->card, has_nil(v), is_intern(v));
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
				if (mt && pcols) {
					for (node *nn = pcols->h; nn; nn = n->next) {
						int next = *(int*) nn->data;
						if (next == c->colnr) {
							if (isPartitionedByColumnTable(mt)) {
								return sql_error(sql, 02, SQLSTATE(42000) "%s: Update on the partitioned column is not possible at the moment", action);
							} else if (isPartitionedByExpressionTable(mt)) {
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
					v = exp_atom(sql->sa, atom_general(sql->sa, &c->type, NULL));
				if (!(v = update_check_column(sql, t, c, v, r, cname, action)))
					return NULL;
				list_append(exps, exp_column(sql->sa, t->base.name, cname, &c->type, CARD_MULTI, 0, 0));
				exp_setname(sql->sa, v, c->t->base.name, c->base.name);
				updates[c->colnr] = v;
			}
		} else {
			char *cname = assignment->h->next->data.sval;
			sql_column *c = mvc_bind_column(sql, t, cname);

			if (!c)
				return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42S22) "%s: no such column '%s.%s'", action, t->base.name, cname);
			if (updates[c->colnr])
				return sql_error(sql, 02, SQLSTATE(42000) "%s: Multiple assignments to same column '%s'", action, c->base.name);
			if (mt && pcols) {
				for (node *nn = pcols->h; nn; nn = nn->next) {
					int next = *(int*) nn->data;
					if (next == c->colnr) {
						if (isPartitionedByColumnTable(mt)) {
							return sql_error(sql, 02, SQLSTATE(42000) "%s: Update on the partitioned column is not possible at the moment", action);
						} else if (isPartitionedByExpressionTable(mt)) {
							return sql_error(sql, 02, SQLSTATE(42000) "%s: Update a column used by the partition's expression is not possible at the moment", action);
						}
					}
				}
			}
			if (!v)
				v = exp_atom(sql->sa, atom_general(sql->sa, &c->type, NULL));
			if (!(v = update_check_column(sql, t, c, v, r, cname, action)))
				return NULL;
			list_append(exps, exp_column(sql->sa, t->base.name, cname, &c->type, CARD_MULTI, 0, 0));
			exp_setname(sql->sa, v, c->t->base.name, c->base.name);
			updates[c->colnr] = v;
		}
	}
	r = rel_project(sql->sa, r, list_append(new_exp_list(sql->sa), exp_column(sql->sa, rname, TID, sql_bind_localtype("oid"), CARD_MULTI, 0, 1)));
	reset_single(r); /* don't let single joins get propagated */
	r = rel_update(sql, bt, r, updates, exps);
	return r;
}

static sql_rel *
update_table(sql_query *query, dlist *qname, str alias, dlist *assignmentlist, symbol *opt_from, symbol *opt_where)
{
	mvc *sql = query->sql;
	char *sname = qname_schema(qname);
	char *tname = qname_schema_object(qname);
	sql_table *t = NULL;

	t = find_table_or_view_on_scope(sql, NULL, sname, tname, "UPDATE", false);
	if (update_allowed(sql, t, tname, "UPDATE", "update", 0) != NULL) {
		sql_rel *r = NULL, *res = rel_basetable(sql, t, alias ? alias : tname);

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
				sql_rel *fnd = table_ref(query, NULL, n->data.sym, 0, refs);

				if (!fnd)
					return NULL;
				if (fnd && tables)
					tables = rel_crossproduct(sql->sa, tables, fnd, op_join);
				else
					tables = fnd;
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
		return update_generate_assignments(query, t, r, rel_basetable(sql, t, alias ? alias : tname), assignmentlist, "UPDATE");
	}
	return NULL;
}

sql_rel *
rel_delete(sql_allocator *sa, sql_rel *t, sql_rel *deletes)
{
	sql_rel *r = rel_create(sa);
	if(!r)
		return NULL;

	r->op = op_delete;
	r->l = t;
	r->r = deletes;
	r->card = deletes ? deletes->card : CARD_ATOM;
	return r;
}

sql_rel *
rel_truncate(sql_allocator *sa, sql_rel *t, int restart_sequences, int drop_action)
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
delete_table(sql_query *query, dlist *qname, str alias, symbol *opt_where)
{
	mvc *sql = query->sql;
	char *sname = qname_schema(qname);
	char *tname = qname_schema_object(qname);
	sql_table *t = NULL;

	t = find_table_or_view_on_scope(sql, NULL, sname, tname, "DELETE FROM", false);
	if (update_allowed(sql, t, tname, "DELETE FROM", "delete from", 1) != NULL) {
		sql_rel *r = rel_basetable(sql, t, alias ? alias : tname);

		if (opt_where) {
			sql_exp *e;

			if (!table_privs(sql, t, PRIV_SELECT)) {
				rel_base_disallow(r);
				if (rel_base_has_column_privileges(sql, r) == 0)
					return sql_error(sql, 02, SQLSTATE(42000) "DELETE FROM: insufficient privileges for user '%s' to delete from table '%s'",
									 get_string_global_var(sql, "current_user"), tname);
				rel_base_use_tid(sql, r);
			}
			if (!(r = rel_logical_exp(query, r, opt_where, sql_where)))
				return NULL;
			e = exp_column(sql->sa, rel_name(r), TID, sql_bind_localtype("oid"), CARD_MULTI, 0, 1);
			r = rel_project(sql->sa, r, list_append(new_exp_list(sql->sa), e));
			r = rel_delete(sql->sa, rel_basetable(sql, t, alias ? alias : tname), r);
		} else {	/* delete all */
			r = rel_delete(sql->sa, r, NULL);
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

static sql_exp *
null_check_best_score(list *exps)
{
	int max = 0;
	sql_exp *res = exps->h->data;

	for (node *n = exps->h ; n ; n = n->next) {
		sql_exp *e = n->data;
		sql_subtype *t = exp_subtype(e);
		int score = 0;

		if (find_prop(e->p, PROP_HASHCOL)) /* distinct columns */
			score += 700;
		if (find_prop(e->p, PROP_SORTIDX)) /* has sort index */
			score += 400;
		if (find_prop(e->p, PROP_HASHIDX)) /* has hash index */
			score += 300;

		if (t) {
			switch (ATOMstorage(t->type->localtype)) {
				case TYPE_bte:
					score += 150 - 8;
					break;
				case TYPE_sht:
					score += 150 - 16;
					break;
				case TYPE_int:
					score += 150 - 32;
					break;
				case TYPE_void:
				case TYPE_lng:
					score += 150 - 64;
					break;
#ifdef HAVE_HGE
				case TYPE_hge:
					score += 150 - 128;
					break;
#endif
				case TYPE_flt:
					score += 75 - 24;
					break;
				case TYPE_dbl:
					score += 75 - 53;
					break;
				default:
					break;
			}
		}
		if (score > max) {
			max = score;
			res = e;
		}
	}
	return res;
}

#define MERGE_UPDATE_DELETE 1
#define MERGE_INSERT        2

static sql_rel *
validate_merge_update_delete(mvc *sql, sql_table *t, str alias, sql_rel *joined_table, tokens upd_token,
							 sql_rel *upd_del, sql_rel *join_rel, const char *bt_name)
{
	char buf[BUFSIZ];
	sql_exp *aggr1, *aggr2, *bigger, *ex;
	sql_subfunc *bf, *cf = sql_bind_func(sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR);
	list *exps = new_exp_list(sql->sa);
	sql_rel *groupby1, *groupby2, *res, *cross;
	const char *join_rel_name = rel_name(joined_table);
	exp_kind ek = {type_value, card_value, FALSE};

	assert(upd_token == SQL_UPDATE || upd_token == SQL_DELETE);

	groupby1 = rel_groupby(sql, rel_dup(join_rel), NULL); /* count the number of rows of the join */
	aggr1 = exp_aggr(sql->sa, NULL, cf, 0, 0, groupby1->card, 0);
	(void) rel_groupby_add_aggr(sql, groupby1, aggr1);
	exp_label(sql->sa, aggr1, ++sql->label);

	groupby2 = rel_groupby(sql, rel_basetable(sql, t, bt_name), NULL); /* the current count of the table */
	aggr2 = exp_aggr(sql->sa, NULL, cf, 0, 0, groupby2->card, 0);
	(void) rel_groupby_add_aggr(sql, groupby2, aggr2);
	exp_label(sql->sa, aggr2, ++sql->label);

	cross = rel_crossproduct(sql->sa, groupby1, groupby2, op_join); /* both should have 1 row */

	bf = sql_bind_func(sql, "sys", ">", exp_subtype(aggr1), exp_subtype(aggr2), F_FUNC);
	if (!bf)
		return sql_error(sql, 02, SQLSTATE(42000) "MERGE: function '>' not found");
	list_append(exps, exp_ref(sql, aggr1));
	list_append(exps, exp_ref(sql, aggr2));
	bigger = exp_op(sql->sa, exps, bf);
	exp_label(sql->sa, bigger, ++sql->label); /* the join cannot produce more rows than what the table has */

	res = rel_project(sql->sa, cross, list_append(sa_list(sql->sa), bigger));
	res = rel_return_zero_or_one(sql, res, ek);

	ex = exp_ref(sql, res->exps->h->data);
	snprintf(buf, BUFSIZ, "MERGE %s: Multiple rows in the input relation%s%s%s match the same row in the target %s '%s%s%s'",
			 (upd_token == SQL_DELETE) ? "DELETE" : "UPDATE",
			 join_rel_name ? " '" : "", join_rel_name ? join_rel_name : "", join_rel_name ? "'" : "",
			 alias ? "relation" : "table",
			 alias ? alias : t->s ? t->s->base.name : "", alias ? "" : ".", alias ? "" : t->base.name);
	ex = exp_exception(sql->sa, ex, buf);

	res = rel_exception(sql->sa, res, NULL, list_append(new_exp_list(sql->sa), ex));
	return rel_list(sql->sa, res, upd_del);
}

static sql_rel *
merge_into_table(sql_query *query, dlist *qname, str alias, symbol *tref, symbol *search_cond, dlist *merge_list)
{
	mvc *sql = query->sql;
	char *sname = qname_schema(qname), *tname = qname_schema_object(qname);
	sql_table *t = NULL;
	sql_rel *bt, *joined, *join_rel = NULL, *extra_project, *insert = NULL, *upd_del = NULL, *res = NULL, *no_tid = NULL;
	int processed = 0;
	const char *bt_name;

	assert(tref && search_cond && merge_list);

	if (!(t = find_table_or_view_on_scope(sql, NULL, sname, tname, "MERGE", false)))
		return NULL;
	if (isMergeTable(t))
		return sql_error(sql, 02, SQLSTATE(42000) "MERGE: merge statements not available for merge tables yet");

	bt = rel_basetable(sql, t, alias ? alias : tname);
	if (!table_privs(sql, t, PRIV_SELECT)) {
		rel_base_disallow(bt);
		if (rel_base_has_column_privileges(sql, bt) == 0)
			return sql_error(sql, 02, SQLSTATE(42000) "MERGE: access denied for %s to table %s%s%s'%s'",
							 get_string_global_var(sql, "current_user"), t->s ? "'":"", t->s ? t->s->base.name : "", t->s ? "'.":"", tname);
		rel_base_use_tid(sql, bt);
		list_append(bt->exps, exp_column(sql->sa, alias ? alias : tname, TID, sql_bind_localtype("oid"), CARD_MULTI, 0, 1));
	}
	joined = table_ref(query, NULL, tref, 0, NULL);
	if (!bt || !joined)
		return NULL;

	bt_name = rel_name(bt);
	if (rel_name(joined) && strcmp(bt_name, rel_name(joined)) == 0)
		return sql_error(sql, 02, SQLSTATE(42000) "MERGE: '%s' on both sides of the joining condition", bt_name);

	for (dnode *m = merge_list->h; m; m = m->next) {
		symbol *sym = m->data.sym, *opt_search, *action;
		tokens token = sym->token;
		dlist* dl = sym->data.lval, *sts;
		opt_search = dl->h->data.sym;
		action = dl->h->next->data.sym;
		sts = action->data.lval;

		if (opt_search)
			return sql_error(sql, 02, SQLSTATE(42000) "MERGE: search condition not yet supported");

		if (token == SQL_MERGE_MATCH) {
			tokens uptdel = action->token;

			if ((processed & MERGE_UPDATE_DELETE) == MERGE_UPDATE_DELETE)
				return sql_error(sql, 02, SQLSTATE(42000) "MERGE: only one WHEN MATCHED clause is allowed");
			processed |= MERGE_UPDATE_DELETE;

			if (uptdel == SQL_UPDATE) {
				if (!update_allowed(sql, t, tname, "MERGE", "update", 0))
					return NULL;
				if ((processed & MERGE_INSERT) == MERGE_INSERT) {
					join_rel = rel_dup(join_rel);
				} else {
					join_rel = rel_crossproduct(sql->sa, bt, joined, op_left);
					if (!(join_rel = rel_logical_exp(query, join_rel, search_cond, sql_where | sql_join)))
						return NULL;
					set_processed(join_rel);
				}

				sql_exp *extra = null_check_best_score(rel_projections(sql, joined, NULL, 1, 0));
				/* select values with correspondent on the table side, ie they are not null */
				sql_exp *le = exp_compare(sql->sa, exp_ref(sql, extra), exp_atom(sql->sa, atom_general(sql->sa, exp_subtype(extra), NULL)), cmp_equal);
				set_anti(le);
				set_has_no_nil(le);
				set_semantics(le);
				extra_project = rel_project(sql->sa, rel_select(sql->sa, join_rel, le), rel_projections(sql, join_rel, NULL, 1, 1));

				upd_del = update_generate_assignments(query, t, extra_project, rel_basetable(sql, t, bt_name), sts->h->data.lval, "MERGE");
			} else if (uptdel == SQL_DELETE) {
				if (!update_allowed(sql, t, tname, "MERGE", "delete", 1))
					return NULL;
				if ((processed & MERGE_INSERT) == MERGE_INSERT) {
					join_rel = rel_dup(join_rel);
				} else {
					join_rel = rel_crossproduct(sql->sa, bt, joined, op_left);
					if (!(join_rel = rel_logical_exp(query, join_rel, search_cond, sql_where | sql_join)))
						return NULL;
					set_processed(join_rel);
				}

				sql_exp *extra = null_check_best_score(rel_projections(sql, joined, NULL, 1, 0));
				/* select values with correspondent on the table side, ie they are not null */
				sql_exp *le = exp_compare(sql->sa, exp_ref(sql, extra), exp_atom(sql->sa, atom_general(sql->sa, exp_subtype(extra), NULL)), cmp_equal);
				set_anti(le);
				set_has_no_nil(le);
				set_semantics(le);
				extra_project = rel_project(sql->sa, rel_select(sql->sa, join_rel, le), list_append(new_exp_list(sql->sa), exp_column(sql->sa, bt_name, TID, sql_bind_localtype("oid"), CARD_MULTI, 0, 1)));

				upd_del = rel_delete(sql->sa, rel_basetable(sql, t, bt_name), extra_project);
			} else {
				assert(0);
			}
			if (!upd_del || !(upd_del = validate_merge_update_delete(sql, t, alias, joined, uptdel, upd_del, join_rel, bt_name)))
				return NULL;
		} else if (token == SQL_MERGE_NO_MATCH) {
			if ((processed & MERGE_INSERT) == MERGE_INSERT)
				return sql_error(sql, 02, SQLSTATE(42000) "MERGE: only one WHEN NOT MATCHED clause is allowed");
			processed |= MERGE_INSERT;

			assert(action->token == SQL_INSERT);
			if (!insert_allowed(sql, t, tname, "MERGE", "insert"))
				return NULL;
			if ((processed & MERGE_UPDATE_DELETE) == MERGE_UPDATE_DELETE) {
				join_rel = rel_dup(join_rel);
			} else {
				join_rel = rel_crossproduct(sql->sa, bt, joined, op_left);
				if (!(join_rel = rel_logical_exp(query, join_rel, search_cond, sql_where | sql_join)))
					return NULL;
				set_processed(join_rel);
			}

			//project joined values which didn't match on the join and insert them
			extra_project = rel_project(sql->sa, join_rel, rel_projections(sql, joined, NULL, 1, 0));
			no_tid = rel_project(sql->sa, rel_dup(joined), rel_projections(sql, joined, NULL, 1, 0));
			extra_project = rel_setop(sql->sa, no_tid, extra_project, op_except);
			rel_setop_set_exps(sql, extra_project, rel_projections(sql, extra_project, NULL, 1, 0));
			set_processed(extra_project);

			if (!(insert = merge_generate_inserts(query, t, extra_project, sts->h->data.lval, sts->h->next->data.sym)))
				return NULL;
			if (!(insert = rel_insert(query->sql, rel_basetable(sql, t, bt_name), insert)))
				return NULL;
		} else {
			assert(0);
		}
	}

	if (processed == (MERGE_UPDATE_DELETE | MERGE_INSERT)) {
		res = rel_list(sql->sa, insert, upd_del);
		res->p = prop_create(sql->sa, PROP_DISTRIBUTE, res->p);
	} else if ((processed & MERGE_UPDATE_DELETE) == MERGE_UPDATE_DELETE) {
		res = upd_del;
		res->p = prop_create(sql->sa, PROP_DISTRIBUTE, res->p);
	} else if ((processed & MERGE_INSERT) == MERGE_INSERT) {
		res = insert;
	} else {
		assert(0);
	}
	return res;
}

static list *
table_column_types(sql_allocator *sa, sql_table *t)
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
table_column_names_and_defaults(sql_allocator *sa, sql_table *t)
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
rel_import(mvc *sql, sql_table *t, const char *tsep, const char *rsep, const char *ssep, const char *ns, const char *filename, lng nr, lng offset, int best_effort, dlist *fwf_widths, int onclient, int escape)
{
	sql_rel *res;
	list *exps, *args;
	node *n;
	sql_subtype tpe;
	sql_exp *import;
	sql_subfunc *f = sql_find_func(sql, "sys", "copyfrom", 12, F_UNION, NULL);
	char *fwf_string = NULL;

	assert(f); /* we do expect copyfrom to be there */
	f->res = table_column_types(sql->sa, t);
 	sql_find_subtype(&tpe, "varchar", 0, 0);
	args = append( append( append( append( append( new_exp_list(sql->sa),
		exp_atom_ptr(sql->sa, t)),
		exp_atom_str(sql->sa, tsep, &tpe)),
		exp_atom_str(sql->sa, rsep, &tpe)),
		exp_atom_str(sql->sa, ssep, &tpe)),
		exp_atom_str(sql->sa, ns, &tpe));

	if (fwf_widths && dlist_length(fwf_widths) > 0) {
		dnode *dn;
		int ncol = 0;
		char *fwf_string_cur = fwf_string = sa_alloc(sql->sa, 20 * dlist_length(fwf_widths) + 1); // a 64 bit int needs 19 characters in decimal representation plus the separator

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

	append( args, exp_atom_str(sql->sa, filename, &tpe));
	import = exp_op(sql->sa,
					append(
						append(
							append(
								append(
									append(
										append(args,
											   exp_atom_lng(sql->sa, nr)),
										exp_atom_lng(sql->sa, offset)),
									exp_atom_int(sql->sa, best_effort)),
								exp_atom_str(sql->sa, fwf_string, &tpe)),
							exp_atom_int(sql->sa, onclient)),
						exp_atom_int(sql->sa, escape)), f);

	exps = new_exp_list(sql->sa);
	for (n = ol_first_node(t->columns); n; n = n->next) {
		sql_column *c = n->data;
		if (c->base.name[0] != '%')
			append(exps, exp_column(sql->sa, t->base.name, c->base.name, &c->type, CARD_MULTI, c->null, 0));
	}
	res = rel_table_func(sql->sa, NULL, import, exps, TABLE_PROD_FUNC);
	return res;
}

static sql_rel *
copyfrom(sql_query *query, dlist *qname, dlist *columns, dlist *files, dlist *headers, dlist *seps, dlist *nr_offset, str null_string, int best_effort, int constraint, dlist *fwf_widths, int onclient, int escape)
{
	mvc *sql = query->sql;
	sql_rel *rel = NULL;
	char *sname = qname_schema(qname);
	char *tname = qname_schema_object(qname);
	sql_table *t = NULL, *nt = NULL;
	const char *tsep = seps->h->data.sval;
	char *rsep = seps->h->next->data.sval; // not const, might need adjusting
	const char *ssep = (seps->h->next->next)?seps->h->next->next->data.sval:NULL;
	const char *ns = (null_string)?null_string:"null";
	lng nr = (nr_offset)?nr_offset->h->data.l_val:-1;
	lng offset = (nr_offset)?nr_offset->h->next->data.l_val:0;
	list *collist;
	int reorder = 0;
	assert(!nr_offset || nr_offset->h->type == type_lng);
	assert(!nr_offset || nr_offset->h->next->type == type_lng);

	if (strcmp(rsep, "\r\n") == 0) {
		// silently fix it
		rsep[0] = '\n';
		rsep[1] = '\0';
	} else if (strstr(rsep, "\r\n") != NULL) {
		return sql_error(sql, 02, SQLSTATE(42000)
				"COPY INTO: record separator contains '\\r\\n' but "
				"that will never match, use '\\n' instead");
	}

	t = find_table_or_view_on_scope(sql, NULL, sname, tname, "COPY INTO", false);
	if (insert_allowed(sql, t, tname, "COPY INTO", "copy into") == NULL)
		return NULL;

	collist = check_table_columns(sql, t, columns, "COPY INTO", tname);
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
		dnode *n;

		nt = mvc_create_table(sql, t->s, tname, tt_table, 0, SQL_DECLARED_TABLE, CA_COMMIT, -1, 0);
		for (n = headers->h; n; n = n->next) {
			dnode *dn = n->data.lval->h;
			char *cname = dn->data.sval;
			char *format = NULL;
			sql_column *cs = NULL;

			if (dn->next)
				format = dn->next->data.sval;
			if (!list_find_name(collist, cname)) {
				char *name;
				size_t len = strlen(cname) + 2;
				sql_subtype *ctype = sql_bind_localtype("oid");

				name = sa_alloc(sql->sa, len);
				snprintf(name, len, "%%cname");
				cs = mvc_create_column(sql, nt, name, ctype);
			} else if (!format) {
				cs = find_sql_column(t, cname);
				cs = mvc_create_column(sql, nt, cname, &cs->type);
			} else { /* load as string, parse later */
				sql_subtype *ctype = sql_bind_localtype("str");
				cs = mvc_create_column(sql, nt, cname, ctype);
				has_formats = 1;
			}
			(void)cs;
		}
		if (!has_formats)
			headers = NULL;
		reorder = 1;
	}
	if (files) {
		dnode *n = files->h;

		if (!onclient && !copy_allowed(sql, 1)) {
			return sql_error(sql, 02, SQLSTATE(42000)
					 "COPY INTO: insufficient privileges: "
					 "COPY INTO from file(s) requires database administrator rights, "
					 "use 'COPY INTO \"%s\" FROM file ON CLIENT' instead", tname);
		}

		for (; n; n = n->next) {
			const char *fname = n->data.sval;
			sql_rel *nrel;

			if (!onclient && fname && !MT_path_absolute(fname)) {
				char *fn = ATOMformat(TYPE_str, fname);
				sql_error(sql, 02, SQLSTATE(42000) "COPY INTO: filename must "
					  "have absolute path: %s", fn);
				GDKfree(fn);
				return NULL;
			}

			nrel = rel_import(sql, nt, tsep, rsep, ssep, ns, fname, nr, offset, best_effort, fwf_widths, onclient, escape);

			if (!rel)
				rel = nrel;
			else {
				rel = rel_setop(sql->sa, rel, nrel, op_union);
				set_processed(rel);
			}
			if (!rel)
				return rel;
		}
	} else {
		assert(onclient == 0);
		rel = rel_import(sql, nt, tsep, rsep, ssep, ns, NULL, nr, offset, best_effort, NULL, onclient, escape);
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
				size_t l = strlen(cs->type.type->sqlname);
				char *fname = sa_alloc(sql->sa, l+8);

				snprintf(fname, l+8, "str_to_%s", strcmp(cs->type.type->sqlname, "timestamptz") == 0 ? "timestamp" : cs->type.type->sqlname);
				sql_find_subtype(&st, "clob", 0, 0);
				if (!(f = sql_bind_func_result(sql, "sys", fname, F_FUNC, &cs->type, 2, &st, &st)))
					return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO: '%s' missing for type %s", fname, cs->type.type->sqlname);
				append(args, e);
				append(args, exp_atom_clob(sql->sa, format));
				ne = exp_op(sql->sa, args, f);
				if (exp_name(e))
					exp_prop_alias(sql->sa, ne, e);
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
	if (rel && !constraint)
		rel->flag |= UPD_NO_CONSTRAINT;
	return rel;
}

static sql_rel *
bincopyfrom(sql_query *query, dlist *qname, dlist *columns, dlist *files, int constraint, int onclient, endianness endian)
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
	sql_subfunc *f = sql_find_func(sql, "sys", "copyfrom", 3, F_UNION, NULL);
	list *collist;
	int i;

	assert(f);
	if (!copy_allowed(sql, 1))
		return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO: insufficient privileges: "
				"binary COPY INTO requires database administrator rights");

	t = find_table_or_view_on_scope(sql, NULL, sname, tname, "COPY INTO", false);
	if (insert_allowed(sql, t, tname, "COPY INTO", "copy into") == NULL)
		return NULL;
	if (files == NULL)
		return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO: must specify files");

	collist = check_table_columns(sql, t, columns, "COPY BINARY INTO", tname);
	if (!collist)
		return NULL;

	bool do_byteswap =
		#ifdef WORDS_BIGENDIAN
			endian == endian_little;
		#else
			endian == endian_big;
		#endif

	f->res = table_column_types(sql->sa, t);
 	sql_find_subtype(&strtpe, "varchar", 0, 0);
	args = append( append( append( append( new_exp_list(sql->sa),
		exp_atom_str(sql->sa, t->s?t->s->base.name:NULL, &strtpe)),
		exp_atom_str(sql->sa, t->base.name, &strtpe)),
		exp_atom_int(sql->sa, onclient)),
		exp_atom_bool(sql->sa, do_byteswap));

	// create the list of files that is passed to the function as parameter
	for (i = 0; i < ol_length(t->columns); i++) {
		// we have one file per column, however, because we have column selection that file might be NULL
		// first, check if this column number is present in the passed in the parameters
		int found = 0;
		dn = files->h;
		for (n = collist->h; n && dn; n = n->next, dn = dn->next) {
			sql_column *c = n->data;
			if (i == c->colnr) {
				// this column number was present in the input arguments; pass in the file name
				append(args, exp_atom_str(sql->sa, dn->data.sval, &strtpe));
				found = 1;
				break;
			}
		}
		if (!found) {
			// this column was not present in the input arguments; pass in NULL
			append(args, exp_atom_str(sql->sa, NULL, &strtpe));
		}
	}

	import = exp_op(sql->sa,  args, f);

	exps = new_exp_list(sql->sa);
	for (n = ol_first_node(t->columns); n; n = n->next) {
		sql_column *c = n->data;
		append(exps, exp_column(sql->sa, t->base.name, c->base.name, &c->type, CARD_MULTI, c->null, 0));
	}
	res = rel_table_func(sql->sa, NULL, import, exps, TABLE_PROD_FUNC);
	res = rel_insert_table(query, t, t->base.name, res);
	if (res && !constraint)
		res->flag |= UPD_NO_CONSTRAINT;
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
	sql_table* t;

	if (!copy_allowed(sql, 1))
		return sql_error(sql, 02, SQLSTATE(42000) "COPY INTO: insufficient privileges: "
				"binary COPY INTO requires database administrator rights");
	t = find_table_or_view_on_scope(sql, NULL, sname, tname, "COPY INTO", false);
	//TODO the COPY LOADER INTO should return an insert relation (instead of ddl) to handle partitioned tables properly
	if (insert_allowed(sql, t, tname, "COPY INTO", "copy into") == NULL)
		return NULL;
	else if (isPartitionedByColumnTable(t) || isPartitionedByExpressionTable(t))
		return sql_error(sql, 02, SQLSTATE(42000) "COPY LOADER INTO: not possible for partitioned tables at the moment");
	else if (partition_find_part(sql->session->tr, t, NULL)) {
		sql_part *mt = partition_find_part(sql->session->tr, t, NULL);
		if (mt && (isPartitionedByColumnTable(mt->t) || isPartitionedByExpressionTable(mt->t)))
			return sql_error(sql, 02, SQLSTATE(42000) "COPY LOADER INTO: not possible for tables child of partitioned tables at the moment");
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
rel_output(mvc *sql, sql_rel *l, sql_exp *sep, sql_exp *rsep, sql_exp *ssep, sql_exp *null_string, sql_exp *file, sql_exp *onclient)
{
	sql_rel *rel = rel_create(sql->sa);
	list *exps = new_exp_list(sql->sa);
	if(!rel || !exps)
		return NULL;

	append(exps, sep);
	append(exps, rsep);
	append(exps, ssep);
	append(exps, null_string);
	if (file) {
		append(exps, file);
		append(exps, onclient);
	}
	rel->l = l;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = ddl_output;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
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
	sql_rel *r = rel_subquery(query, NULL, sq, ek);

	if (!r)
		return NULL;

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

	return rel_output(sql, r, tsep_e, rsep_e, ssep_e, ns_e, fname_e, oncl_e);
}

sql_exp *
rel_parse_val(mvc *m, sql_schema *sch, char *query, sql_subtype *tpe, char emode, sql_rel *from)
{
	mvc o = *m;
	sql_exp *e = NULL;
	buffer *b;
	char *n;
	size_t len = _strlen(query);
	exp_kind ek = {type_value, card_value, FALSE};
	stream *s;
	bstream *bs;

	m->qc = NULL;

	if (sch)
		m->session->schema = sch;

	m->emode = emode;
	b = malloc(sizeof(buffer));
	len += 8; /* add 'select ;' */
	n = malloc(len + 1 + 1);
	if(!b || !n) {
		free(b);
		free(n);
		return NULL;
	}
	snprintf(n, len + 2, "select %s;\n", query);
	len++;
	buffer_init(b, n, len);
	s = buffer_rastream(b, "sqlstatement");
	if(!s) {
		buffer_destroy(b);
		return NULL;
	}
	bs = bstream_create(s, b->len);
	if(bs == NULL) {
		buffer_destroy(b);
		return NULL;
	}
	scanner_init(&m->scanner, bs, NULL);
	m->scanner.mode = LINE_1;
	bstream_next(m->scanner.rs);

	m->params = NULL;
	m->sym = NULL;
	m->errstr[0] = '\0';
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
		dlist *l = s->data.lval;

		ret = copyfrom(query,
				l->h->data.lval,
				l->h->next->data.lval,
				l->h->next->next->data.lval,
				l->h->next->next->next->data.lval,
				l->h->next->next->next->next->data.lval,
				l->h->next->next->next->next->next->data.lval,
				l->h->next->next->next->next->next->next->data.sval,
				l->h->next->next->next->next->next->next->next->data.i_val,
				l->h->next->next->next->next->next->next->next->next->data.i_val,
				l->h->next->next->next->next->next->next->next->next->next->data.lval,
				l->h->next->next->next->next->next->next->next->next->next->next->data.i_val,
				l->h->next->next->next->next->next->next->next->next->next->next->next->data.i_val);
		sql->type = Q_UPDATE;
	}
		break;
	case SQL_BINCOPYFROM:
	{
		dlist *l = s->data.lval;

		ret = bincopyfrom(query, l->h->data.lval, l->h->next->data.lval, l->h->next->next->data.lval, l->h->next->next->next->data.i_val, l->h->next->next->next->next->data.i_val, (endianness) l->h->next->next->next->next->next->data.i_val);
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
	case SQL_COPYTO:
	{
		dlist *l = s->data.lval;

		ret = copyto(query, l->h->data.sym, l->h->next->data.sval, l->h->next->next->data.lval, l->h->next->next->next->data.sval, l->h->next->next->next->next->data.i_val);
		sql->type = Q_UPDATE;
	}
		break;
	case SQL_INSERT:
	{
		dlist *l = s->data.lval;

		ret = insert_into(query, l->h->data.lval, l->h->next->data.lval, l->h->next->next->data.sym);
		sql->type = Q_UPDATE;
	}
		break;
	case SQL_UPDATE:
	{
		dlist *l = s->data.lval;

		ret = update_table(query, l->h->data.lval, l->h->next->data.sval, l->h->next->next->data.lval,
						   l->h->next->next->next->data.sym, l->h->next->next->next->next->data.sym);
		sql->type = Q_UPDATE;
	}
		break;
	case SQL_DELETE:
	{
		dlist *l = s->data.lval;

		ret = delete_table(query, l->h->data.lval, l->h->next->data.sval, l->h->next->next->data.sym);
		sql->type = Q_UPDATE;
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
	return ret;
}
