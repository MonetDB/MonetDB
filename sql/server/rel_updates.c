/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_updates.h"
#include "rel_semantic.h"
#include "rel_select.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "sql_privileges.h"
#include "rel_optimizer.h"
#include "rel_dump.h"
#include "rel_psm.h"
#include "sql_symbol.h"

static sql_exp *
insert_value(mvc *sql, sql_column *c, sql_rel **r, symbol *s)
{
	if (s->token == SQL_NULL) {
		return exp_atom(sql->sa, atom_general(sql->sa, &c->type, NULL));
	} else {
		int is_last = 0;
		exp_kind ek = {type_value, card_value, FALSE};
		sql_exp *e = rel_value_exp2(sql, r, s, sql_sel, ek, &is_last);

		if (!e)
			return(NULL);
		return rel_check_type(sql, &c->type, e, type_equal); 
	}
}

static sql_exp ** 
insert_exp_array(mvc *sql, sql_table *t, int *Len)
{
	sql_exp **inserts;
	int i, len = list_length(t->columns.set);
	node *m;

	*Len = len;
	inserts = SA_NEW_ARRAY(sql->sa, sql_exp *, len);
	for (m = t->columns.set->h, i = 0; m; m = m->next, i++) {
		sql_column *c = m->data;

		c->colnr = i;
		inserts[i] = NULL;
	}
	return inserts;
}

#define get_basetable(rel) rel->l

static sql_table *
get_table( sql_rel *t)
{
	sql_table *tab = NULL;

	assert(is_updateble(t)); 
	if (t->op == op_basetable) { /* existing base table */
		tab = get_basetable(t);
	} else if (t->op == op_ddl && (
			t->flag == DDL_ALTER_TABLE ||
			t->flag == DDL_CREATE_TABLE ||
			t->flag == DDL_CREATE_VIEW)) {
		return rel_ddl_table_get(t);
	}
	return tab;
}

static list *
get_inserts( sql_rel *ins )
{
	sql_rel *r = ins->r;

	assert(is_project(r->op) || r->op == op_table);
	return r->exps;
}

static sql_rel *
rel_insert_hash_idx(mvc *sql, sql_idx *i, sql_rel *inserts)
{
	char *iname = sa_strconcat( sql->sa, "%", i->base.name);
	node *m;
	sql_subtype *it, *lng;
	int bits = 1 + ((sizeof(lng)*8)-1)/(list_length(i->columns)+1);
	sql_exp *h = NULL;

	if (list_length(i->columns) <= 1 || i->type == no_idx) {
		/* dummy append */
		append(get_inserts(inserts), exp_label(sql->sa, exp_atom_lng(sql->sa, 0), ++sql->label));
		return inserts;
	}

	it = sql_bind_localtype("int");
	lng = sql_bind_localtype("lng");
	for (m = i->columns->h; m; m = m->next) {
		sql_kc *c = m->data;
		sql_exp *e = list_fetch(get_inserts(inserts), c->c->colnr);

		if (h && i->type == hash_idx)  { 
			list *exps = new_exp_list(sql->sa);
			sql_subfunc *xor = sql_bind_func_result3(sql->sa, sql->session->schema, "rotate_xor_hash", lng, it, &c->c->type, lng);

			append(exps, h);
			append(exps, exp_atom_int(sql->sa, bits));
			append(exps, e);
			h = exp_op(sql->sa, exps, xor);
		} else if (h)  { /* order preserving hash */
			sql_exp *h2;
			sql_subfunc *lsh = sql_bind_func_result(sql->sa, sql->session->schema, "left_shift", lng, it, lng);
			sql_subfunc *lor = sql_bind_func_result(sql->sa, sql->session->schema, "bit_or", lng, lng, lng);
			sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", &c->c->type, NULL, lng);

			h = exp_binop(sql->sa, h, exp_atom_int(sql->sa, bits), lsh); 
			h2 = exp_unop(sql->sa, e, hf);
			h = exp_binop(sql->sa, h, h2, lor);
		} else {
			sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", &c->c->type, NULL, lng);
			h = exp_unop(sql->sa, e, hf);
			if (i->type == oph_idx) 
				break;
		}
	}
	/* append inserts to hash */
	append(get_inserts(inserts), h);
	exp_setname(sql->sa, h, i->t->base.name, iname);
	return inserts;
}

static sql_rel *
rel_insert_join_idx(mvc *sql, sql_idx *i, sql_rel *inserts)
{
	char *iname = sa_strconcat( sql->sa, "%", i->base.name);
	int need_nulls = 0;
	node *m, *o;
	sql_key *rk = &((sql_fkey *) i->key)->rkey->k;
	sql_rel *rt = rel_basetable(sql, rk->t, rk->t->base.name);

	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *or = sql_bind_func_result(sql->sa, sql->session->schema, "or", bt, bt, bt);

	sql_rel *_nlls = NULL, *nnlls, *ins = inserts->r;
	sql_exp *lnll_exps = NULL, *rnll_exps = NULL, *e;
	list *join_exps = new_exp_list(sql->sa), *pexps;

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
		sql_subfunc *isnil = sql_bind_func(sql->sa, sql->session->schema, "isnull", &c->c->type, NULL, F_FUNC);
		sql_exp *_is = list_fetch(ins->exps, c->c->colnr), *lnl, *rnl, *je; 
		sql_exp *rtc = exp_column(sql->sa, rel_name(rt), rc->c->base.name, &rc->c->type, CARD_MULTI, rc->c->null, 0);
		const char *ename = exp_name(_is);

		if (!ename)
			exp_label(sql->sa, _is, ++sql->label);
		ename = exp_name(_is);
		_is = exp_column(sql->sa, exp_relname(_is), ename, exp_subtype(_is), _is->card, has_nil(_is), is_intern(_is));
		lnl = exp_unop(sql->sa, _is, isnil);
		rnl = exp_unop(sql->sa, _is, isnil);
		if (need_nulls) {
		    if (lnll_exps) {
			lnll_exps = exp_binop(sql->sa, lnll_exps, lnl, or);
			rnll_exps = exp_binop(sql->sa, rnll_exps, rnl, or);
		    } else {
			lnll_exps = lnl;
			rnll_exps = rnl;
		    }
		}

		if (rel_convert_types(sql, &rtc, &_is, 1, type_equal) < 0) 
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
		exp_setname(sql->sa, e, i->t->base.name, iname);
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
	exp_setname(sql->sa, e, i->t->base.name, iname);
	append(nnlls->exps, e);

	if (need_nulls) {
		rel_destroy(ins);
		rt = inserts->r = rel_setop(sql->sa, _nlls, nnlls, op_union );
		rt->exps = rel_projections(sql, nnlls, NULL, 1, 1);
		set_processed(rt);
	} else {
		inserts->r = nnlls;
	}
	return inserts;
}

static sql_rel *
rel_insert_idxs(mvc *sql, sql_table *t, sql_rel *inserts)
{
	sql_rel *p = inserts->r;
	node *n;

	if (!t->idxs.set)
		return inserts;

	inserts->r = rel_label(sql, inserts->r, 1); 
	for (n = t->idxs.set->h; n; n = n->next) {
		sql_idx *i = n->data;
		sql_rel *ins = inserts->r;

		if (ins->op == op_union) 
			inserts->r = rel_project(sql->sa, ins, rel_projections(sql, ins, NULL, 0, 1));
		if (hash_index(i->type) || i->type == no_idx) {
			rel_insert_hash_idx(sql, i, inserts);
		} else if (i->type == join_idx) {
			rel_insert_join_idx(sql, i, inserts);
		}
	}
	if (inserts->r != p) {
		sql_rel *r = rel_create(sql->sa);
		if(!r)
			return NULL;

		r->op = op_insert;
		r->l = rel_dup(p);
		r->r = inserts;
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
	/* insert indices */
	if (tab) 
		return rel_insert_idxs(sql, tab, r);
	return r;
}

static sql_rel *
rel_insert_table(mvc *sql, sql_table *t, char *name, sql_rel *inserts)
{
	return rel_insert(sql, rel_basetable(sql, t, name), inserts);
}


static list *
check_table_columns(mvc *sql, sql_table *t, dlist *columns, char *op, char *tname)
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
				return sql_error(sql, 02, "42S22!%s INTO: no such column '%s.%s'", op, tname, n->data.sval);
			}
		}
	} else {
		collist = t->columns.set;
	}
	return collist;
}

static list *
rel_inserts(mvc *sql, sql_table *t, sql_rel *r, list *collist, size_t rowcount, int copy)
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
		
				inserts[c->colnr] = rel_check_type(sql, &c->type, e, type_equal);
			}
		} else {
			for (m = collist->h; m; m = m->next) {
				sql_column *c = m->data;
				sql_exp *e;

				e = exps_bind_column2( r->exps, c->t->base.name, c->base.name);
				if (e)
					inserts[c->colnr] = exp_column(sql->sa, exp_relname(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
			}
		}
	}
	for (i = 0; i < len; i++) {
		if (!inserts[i]) {
			for (m = t->columns.set->h; m; m = m->next) {
				sql_column *c = m->data;

				if (c->colnr == i) {
					size_t j = 0;
					sql_exp *exps = NULL;

					for(j = 0; j < rowcount; j++) {
						sql_exp *e = NULL;

						if (c->def) {
							char *q = sa_message(sql->sa, "select %s;", c->def);
							e = rel_parse_val(sql, q, sql->emode);
							if (!e || (e = rel_check_type(sql, &c->type, e, type_equal)) == NULL)
								return NULL;
						} else {
							atom *a = atom_general(sql->sa, &c->type, NULL);
							e = exp_atom(sql->sa, a);
						}
						if (!e) 
							return sql_error(sql, 02, "INSERT INTO: column '%s' has no valid default value", c->base.name);
						if (exps) {
							list *vals_list = exps->f;
			
							list_append(vals_list, e);
						}
						if (!exps && j+1 < rowcount) {
							exps = exp_values(sql->sa, sa_list(sql->sa));
							exps->tpe = c->type;
							exp_label(sql->sa, exps, ++sql->label);
						}
						if (!exps)
							exps = e;
					}
					inserts[i] = exps;
				}
			}
			assert(inserts[i]);
		}
	}
	/* now rewrite project exps in proper table order */
	exps = new_exp_list(sql->sa);
	for (i = 0; i<len; i++) 
		list_append(exps, inserts[i]);
	return exps;
}


static sql_table *
insert_allowed(mvc *sql, sql_table *t, char *tname, char *op, char *opname)
{
	if (!t) {
		return sql_error(sql, 02, "42S02!%s: no such table '%s'", op, tname);
	} else if (isView(t)) {
		return sql_error(sql, 02, "%s: cannot %s view '%s'", op, opname, tname);
	} else if (isMergeTable(t)) {
		return sql_error(sql, 02, "%s: cannot %s merge table '%s'", op, opname, tname);
	} else if (isStream(t)) {
		return sql_error(sql, 02, "%s: cannot %s stream '%s'", op, opname, tname);
	} else if (t->access == TABLE_READONLY) {
		return sql_error(sql, 02, "%s: cannot %s read only table '%s'", op, opname, tname);
	}
	if (t && !isTempTable(t) && STORE_READONLY)
		return sql_error(sql, 02, "%s: %s table '%s' not allowed in readonly mode", op, opname, tname);

	if (!table_privs(sql, t, PRIV_INSERT)) {
		return sql_error(sql, 02, "%s: insufficient privileges for user '%s' to %s table '%s'", op, stack_get_string(sql, "current_user"), opname, tname);
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

static sql_table *
update_allowed(mvc *sql, sql_table *t, char *tname, char *op, char *opname, int is_delete)
{
	if (!t) {
		return sql_error(sql, 02, "42S02!%s: no such table '%s'", op, tname);
	} else if (isView(t)) {
		return sql_error(sql, 02, "%s: cannot %s view '%s'", op, opname, tname);
	} else if (isMergeTable(t)) {
		return sql_error(sql, 02, "%s: cannot %s merge table '%s'", op, opname, tname);
	} else if (isStream(t)) {
		return sql_error(sql, 02, "%s: cannot %s stream '%s'", op, opname, tname);
	} else if (t->access == TABLE_READONLY || t->access == TABLE_APPENDONLY) {
		return sql_error(sql, 02, "%s: cannot %s read or append only table '%s'", op, opname, tname);
	}
	if (t && !isTempTable(t) && STORE_READONLY)
		return sql_error(sql, 02, "%s: %s table '%s' not allowed in readonly mode", op, opname, tname);
	if (is_delete && !table_privs(sql, t, PRIV_DELETE)) 
		return sql_error(sql, 02, "%s: insufficient privileges for user '%s' to %s table '%s'", op, stack_get_string(sql, "current_user"), opname, tname);
	return t;
}

static sql_rel *
insert_into(mvc *sql, dlist *qname, dlist *columns, symbol *val_or_q)
{
	size_t rowcount = 1;
	char *sname = qname_schema(qname);
	char *tname = qname_table(qname);
	sql_schema *s = NULL;
	sql_table *t = NULL;
	list *collist = NULL;
	sql_rel *r = NULL;

	if (sname && !(s=mvc_bind_schema(sql, sname))) {
		(void) sql_error(sql, 02, "3F000!INSERT INTO: no such schema '%s'", sname);
		return NULL;
	}
	if (!s)
		s = cur_schema(sql);
	t = mvc_bind_table(sql, s, tname);
	if (!t && !sname) {
		s = tmp_schema(sql);
		t = mvc_bind_table(sql, s, tname);
		if (!t) 
			t = mvc_bind_table(sql, NULL, tname);
	}
	if (insert_allowed(sql, t, tname, "INSERT INTO", "insert into") == NULL) 
		return NULL;
	collist = check_table_columns(sql, t, columns, "INSERT", tname);
	if (!collist)
		return NULL;
	if (val_or_q->token == SQL_VALUES) {
		dlist *rowlist = val_or_q->data.lval;
		dlist *values;
		dnode *o;
		list *exps = new_exp_list(sql->sa);
		sql_rel *inner = NULL;

		if (!rowlist->h) {
			r = rel_project(sql->sa, NULL, NULL);
			if (!columns)
				collist = NULL;
		}

		for (o = rowlist->h; o; o = o->next, rowcount++) {
			values = o->data.lval;

			if (dlist_length(values) != list_length(collist)) {
				return sql_error(sql, 02, "21S01!INSERT INTO: number of values doesn't match number of columns of table '%s'", tname);
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
						sql_rel *r = NULL;
						sql_exp *ins = insert_value(sql, c, &r, n->data.sym);
						if (!ins) 
							return NULL;
						if (r && inner)
							inner = rel_crossproduct(sql->sa, inner, r, op_join);
						else if (r) 
							inner = r;
						if (inner && !ins->name && !is_atom(ins->type)) {
							exp_label(sql->sa, ins, ++sql->label);
							ins = exp_column(sql->sa, exp_relname(ins), exp_name(ins), exp_subtype(ins), ins->card, has_nil(ins), is_intern(ins));
						}
						list_append(vals_list, ins);
					}
				} else {
					/* only allow correlation in a single row of values */
					for (n = values->h, m = collist->h; n && m; n = n->next, m = m->next) {
						sql_column *c = m->data;
						sql_rel *r = NULL;
						sql_exp *ins = insert_value(sql, c, &r, n->data.sym);
						if (!ins)
							return NULL;
						if (r && inner)
							inner = rel_crossproduct(sql->sa, inner, r, op_join);
						else if (r) 
							inner = r;
						if (!ins->name)
							exp_label(sql->sa, ins, ++sql->label);
						list_append(exps, ins);
					}
				}
			}
		}
		if (collist)
			r = rel_project(sql->sa, inner, exps);
	} else {
		exp_kind ek = {type_value, card_relation, TRUE};

		r = rel_subquery(sql, NULL, val_or_q, ek, APPLY_JOIN);
	}
	if (!r) 
		return NULL;

	/* In case of missing project, order by or distinct, we need to add	
	   and projection */
	if (r->op != op_project || r->r || need_distinct(r))
		r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 0));
	if ((r->exps && list_length(r->exps) != list_length(collist)) ||
	   (!r->exps && collist)) 
		return sql_error(sql, 02, "21S01!INSERT INTO: query result doesn't match number of columns in table '%s'", tname);

	r->exps = rel_inserts(sql, t, r, collist, rowcount, 0);
	return rel_insert_table(sql, t, tname, r);
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
			sql_column *c = find_sql_column(i->t, ce->name);

			if (c && ic->c->colnr == c->colnr) {
				update = 1;
				break;
			}
		}
	}
	return update;
}

static sql_rel *
rel_update_hash_idx(mvc *sql, sql_idx *i, sql_rel *updates)
{
	char *iname = sa_strconcat( sql->sa, "%", i->base.name);
	node *m;
	sql_subtype *it, *lng = 0; /* is not set in first if below */
	int bits = 1 + ((sizeof(lng)*8)-1)/(list_length(i->columns)+1);
	sql_exp *h = NULL;

	if (list_length(i->columns) <= 1 || i->type == no_idx) {
		h = exp_label(sql->sa, exp_atom_lng(sql->sa, 0), ++sql->label);
	} else {
		it = sql_bind_localtype("int");
		lng = sql_bind_localtype("lng");
		for (m = i->columns->h; m; m = m->next) {
			sql_kc *c = m->data;
			sql_exp *e;

	       		e = list_fetch(get_inserts(updates), c->c->colnr+1);
			
			if (h && i->type == hash_idx)  { 
				list *exps = new_exp_list(sql->sa);
				sql_subfunc *xor = sql_bind_func_result3(sql->sa, sql->session->schema, "rotate_xor_hash", lng, it, &c->c->type, lng);
	
				append(exps, h);
				append(exps, exp_atom_int(sql->sa, bits));
				append(exps, e);
				h = exp_op(sql->sa, exps, xor);
			} else if (h)  { /* order preserving hash */
				sql_exp *h2;
				sql_subfunc *lsh = sql_bind_func_result(sql->sa, sql->session->schema, "left_shift", lng, it, lng);
				sql_subfunc *lor = sql_bind_func_result(sql->sa, sql->session->schema, "bit_or", lng, lng, lng);
				sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", &c->c->type, NULL, lng);
	
				h = exp_binop(sql->sa, h, exp_atom_int(sql->sa, bits), lsh); 
				h2 = exp_unop(sql->sa, e, hf);
				h = exp_binop(sql->sa, h, h2, lor);
			} else {
				sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", &c->c->type, NULL, lng);
				h = exp_unop(sql->sa, e, hf);
				if (i->type == oph_idx) 
					break;
			}
		}
	}
	/* append hash to updates */
	append(get_inserts(updates), h);
	exp_setname(sql->sa, h, i->t->base.name, iname);

	if (!updates->exps)
		updates->exps = new_exp_list(sql->sa);
	append(updates->exps, exp_column(sql->sa, i->t->base.name, iname, lng, CARD_MULTI, 0, 0));
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
rel_update_join_idx(mvc *sql, sql_idx *i, sql_rel *updates)
{
	int nr = ++sql->label;
	char name[16], *nme = number2name(name, 16, nr);
	char *iname = sa_strconcat( sql->sa, "%", i->base.name);

	int need_nulls = 0;
	node *m, *o;
	sql_key *rk = &((sql_fkey *) i->key)->rkey->k;
	sql_rel *rt = rel_basetable(sql, rk->t, sa_strdup(sql->sa, nme));

	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *or = sql_bind_func_result(sql->sa, sql->session->schema, "or", bt, bt, bt);

	sql_rel *_nlls = NULL, *nnlls, *ups = updates->r;
	sql_exp *lnll_exps = NULL, *rnll_exps = NULL, *e;
	list *join_exps = new_exp_list(sql->sa), *pexps;

	for (m = i->columns->h; m; m = m->next) {
		sql_kc *c = m->data;

		if (c->c->null) 
			need_nulls = 1;
	}
	for (m = i->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *c = m->data;
		sql_kc *rc = o->data;
		sql_subfunc *isnil = sql_bind_func(sql->sa, sql->session->schema, "isnull", &c->c->type, NULL, F_FUNC);
		sql_exp *upd = list_fetch(get_inserts(updates), c->c->colnr + 1), *lnl, *rnl, *je;
		sql_exp *rtc = exp_column(sql->sa, rel_name(rt), rc->c->base.name, &rc->c->type, CARD_MULTI, rc->c->null, 0);


		/* FOR MATCH FULL/SIMPLE/PARTIAL see above */
		/* Currently only the default MATCH SIMPLE is supported */
		upd = exp_column(sql->sa, exp_relname(upd), exp_name(upd), exp_subtype(upd), upd->card, has_nil(upd), is_intern(upd));
		lnl = exp_unop(sql->sa, upd, isnil);
		rnl = exp_unop(sql->sa, upd, isnil);
		if (need_nulls) {
		    if (lnll_exps) {
			lnll_exps = exp_binop(sql->sa, lnll_exps, lnl, or);
			rnll_exps = exp_binop(sql->sa, rnll_exps, rnl, or);
		    } else {
			lnll_exps = lnl;
			rnll_exps = rnl;
		    }
		}
		if (rel_convert_types(sql, &rtc, &upd, 1, type_equal) < 0) {
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
		exp_setname(sql->sa, e, i->t->base.name, iname);
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
	exp_setname(sql->sa, e, i->t->base.name, iname);
	append(nnlls->exps, e);

	if (need_nulls) {
		rel_destroy(ups);
		rt = updates->r = rel_setop(sql->sa, _nlls, nnlls, op_union );
		rt->exps = rel_projections(sql, nnlls, NULL, 1, 1);
		set_processed(rt);
	} else {
		updates->r = nnlls;
	}
	if (!updates->exps)
		updates->exps = new_exp_list(sql->sa);
	append(updates->exps, exp_column(sql->sa, i->t->base.name, iname, sql_bind_localtype("oid"), CARD_MULTI, 0, 0));
	return updates;
}

/* for cascade of updates we change the 'relup' relations into
 * a DDL_LIST of update relations.
 */
static sql_rel *
rel_update_idxs(mvc *sql, sql_table *t, sql_rel *relup)
{
	sql_rel *p = relup->r;
	node *n;

	if (!t->idxs.set)
		return relup;

	for (n = t->idxs.set->h; n; n = n->next) {
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
			rel_update_hash_idx(sql, i, relup);
		} else if (i->type == join_idx) {
			rel_update_join_idx(sql, i, relup);
		}
	}
	if (relup->r != p) {
		sql_rel *r = rel_create(sql->sa);
		if(!r)
			return NULL;
		r->op = op_update;
		r->l = rel_dup(p);
		r->r = relup;
		r->flag |= UPD_COMP; /* mark as special update */
		return r;
	}
	return relup;
}

sql_exp ** 
table_update_array(mvc *sql, sql_table *t)
{
	sql_exp **updates;
	int i, len = list_length(t->columns.set);
	node *m;

	updates = SA_NEW_ARRAY(sql->sa, sql_exp *, len);
	for (m = t->columns.set->h, i = 0; m; m = m->next, i++) {
		sql_column *c = m->data;

		/* update the column number, for correct array access */
		c->colnr = i;
		updates[i] = NULL;
	}
	return updates;
}

sql_rel *
rel_update(mvc *sql, sql_rel *t, sql_rel *uprel, sql_exp **updates, list *exps)
{
	sql_rel *r = rel_create(sql->sa);
	sql_table *tab = get_table(t);
	node *m;
	if(!r)
		return NULL;

	if (tab)
	for (m = tab->columns.set->h; m; m = m->next) {
		sql_column *c = m->data;
		sql_exp *v = updates[c->colnr];

		if (tab->idxs.set && !v) 
			v = exp_column(sql->sa, tab->base.name, c->base.name, &c->type, CARD_MULTI, c->null, 0);
		if (v)
			rel_project_add_exp(sql, uprel, v);
	}

	r->op = op_update;
	r->l = t;
	r->r = uprel;
	r->exps = exps;
	/* update indices */
	if (tab)
		return rel_update_idxs(sql, tab, r);
	return r;
}


static sql_exp *
update_check_column(mvc *sql, sql_table *t, sql_column *c, sql_exp *v, sql_rel *r, char *cname)
{
	if (!c) {
		rel_destroy(r);
		return sql_error(sql, 02, "42S22!UPDATE: no such column '%s.%s'", t->base.name, cname);
	}
	if (!table_privs(sql, t, PRIV_UPDATE) && !sql_privilege(sql, sql->user_id, c->base.id, PRIV_UPDATE, 0)) 
		return sql_error(sql, 02, "UPDATE: insufficient privileges for user '%s' to update table '%s' on column '%s'", stack_get_string(sql, "current_user"), t->base.name, cname);
	if (!v || (v = rel_check_type(sql, &c->type, v, type_equal)) == NULL) {
		rel_destroy(r);
		return NULL;
	}
	return v;
}

static sql_rel *
update_table(mvc *sql, dlist *qname, dlist *assignmentlist, symbol *opt_from, symbol *opt_where)
{
	char *sname = qname_schema(qname);
	char *tname = qname_table(qname);
	sql_schema *s = NULL;
	sql_table *t = NULL;

	if (sname && !(s=mvc_bind_schema(sql,sname))) {
		(void) sql_error(sql, 02, "3F000!UPDATE: no such schema '%s'", sname);
		return NULL;
	}
	if (!s)
		s = cur_schema(sql);
	t = mvc_bind_table(sql, s, tname);
	if (!t && !sname) {
		s = tmp_schema(sql);
		t = mvc_bind_table(sql, s, tname);
		if (!t) 
			t = mvc_bind_table(sql, NULL, tname);
		if (!t) 
			t = stack_find_table(sql, tname);
	}
	if (update_allowed(sql, t, tname, "UPDATE", "update", 0) != NULL) {
		sql_exp *e = NULL, **updates;
		sql_rel *r = NULL;
		list *exps;
		dnode *n;
		const char *rname = NULL;
		sql_rel *res = NULL, *bt = rel_basetable(sql, t, t->base.name);

		res = bt;
#if 0
			dlist *selection = dlist_create(sql->sa);
			dlist *from_list = dlist_create(sql->sa);
			symbol *sym;
			sql_rel *sq;

			dlist_append_list(sql->sa, from_list, qname);
			dlist_append_symbol(sql->sa, from_list, NULL);
			sym = symbol_create_list(sql->sa, SQL_NAME, from_list);
			from_list = dlist_create(sql->sa);
			dlist_append_symbol(sql->sa, from_list, sym);

			{
				dlist *l = dlist_create(sql->sa);


				dlist_append_string(sql->sa, l, tname);
				dlist_append_string(sql->sa, l, TID);
				sym = symbol_create_list(sql->sa, SQL_COLUMN, l);

				l = dlist_create(sql->sa);
				dlist_append_symbol(sql->sa, l, sym);
				dlist_append_string(sql->sa, l, TID);
				dlist_append_symbol(sql->sa, selection, 
				  symbol_create_list(sql->sa, SQL_COLUMN, l));
			}
			for (n = assignmentlist->h; n; n = n->next) {
				dlist *assignment = n->data.sym->data.lval, *l;
				int single = (assignment->h->next->type == type_string);
				symbol *a = assignment->h->data.sym;

				l = dlist_create(sql->sa);
				dlist_append_symbol(sql->sa, l, a);
				dlist_append_string(sql->sa, l, (single)?assignment->h->next->data.sval:NULL);
				a = symbol_create_list(sql->sa, SQL_COLUMN, l);
				dlist_append_symbol(sql->sa, selection, a);
			}
		       
			sym = newSelectNode(sql->sa, 0, selection, NULL, symbol_create_list(sql->sa, SQL_FROM, from_list), opt_where, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
			sq = rel_selects(sql, sym);
			if (sq)
				sq = rel_optimizer(sql, sq);
		}
#endif

		if (opt_from) {
			dlist *fl = opt_from->data.lval;
			dnode *n = NULL;
			sql_rel *fnd = NULL;

			for (n = fl->h; n && res; n = n->next) {
				fnd = table_ref(sql, NULL, n->data.sym, 0);
				if (fnd)
					res = rel_crossproduct(sql->sa, res, fnd, op_join);
				else
					res = fnd;
			}
			if (!res) 
				return NULL;
		}
		if (opt_where) {
			int status = sql->session->status;
	
			if (!table_privs(sql, t, PRIV_SELECT)) 
				return sql_error(sql, 02, "UPDATE: insufficient privileges for user '%s' to update table '%s'", stack_get_string(sql, "current_user"), tname);
			r = rel_logical_exp(sql, NULL, opt_where, sql_where);
			if (r) { /* simple predicate which is not using the to 
				    be updated table. We add a select all */
				r = rel_crossproduct(sql->sa, res, r, op_semi);
			} else {
				sql->errstr[0] = 0;
				sql->session->status = status;
				r = rel_logical_exp(sql, res, opt_where, sql_where);
				if (!opt_from && r && is_join(r->op))
					r->op = op_semi;
			}
			if (!r) 
				return NULL;
		} else {	/* update all */
			r = res;
		}
	
		/* first create the project */
		e = exp_column(sql->sa, rname = rel_name(r), TID, sql_bind_localtype("oid"), CARD_MULTI, 0, 1);
		exps = new_exp_list(sql->sa);
		append(exps, e);
		updates = table_update_array(sql, t);
		for (n = assignmentlist->h; n; n = n->next) {
			symbol *a = NULL;
			sql_exp *v = NULL;
			sql_rel *rel_val = NULL;
			dlist *assignment = n->data.sym->data.lval;
			int single = (assignment->h->next->type == type_string);
			/* Single assignments have a name, multicolumn a list */

			a = assignment->h->data.sym;
			if (a) {
				int status = sql->session->status;
				exp_kind ek = {type_value, (single)?card_column:card_relation, FALSE};

				if (single) 
					v = rel_value_exp(sql, &rel_val, a, sql_sel, ek);
				else
					rel_val = rel_subquery(sql, NULL, a, ek, APPLY_JOIN);

				if (!v) {
					sql->errstr[0] = 0;
					sql->session->status = status;
					if (single) {
						rel_val = NULL;
						v = rel_value_exp(sql, &r, a, sql_sel, ek);
					} else if (!rel_val && r) {
						r = rel_subquery(sql, r, a, ek, APPLY_LOJ);
						if (r) {
							list *val_exps = rel_projections(sql, r->r, NULL, 0, 1);

							r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 1));
							if (r)
								list_merge(r->exps, val_exps, (fdup)NULL);
							reset_processed(r);
						}
					}
				}
				if ((single && !v) || (!single && !r)) {
					rel_destroy(r);
					return NULL;
				}
				if (rel_val) {
					if (single) {
						if (!exp_name(v))
							exp_label(sql->sa, v, ++sql->label);
						rel_val = rel_project(sql->sa, rel_val, rel_projections(sql, rel_val, NULL, 0, 1));
						rel_project_add_exp(sql, rel_val, v);
						reset_processed(rel_val);
					}
					r = rel_crossproduct(sql->sa, r, rel_val, op_left);
					if (single) 
						v = exp_column(sql->sa, NULL, exp_name(v), exp_subtype(v), v->card, has_nil(v), is_intern(v));
				}
			}
			if (!single) {
				dlist *cols = assignment->h->next->data.lval;
				dnode *m;
				node *n;
				int nr;

				if (!rel_val)
					rel_val = r;
				if (!rel_val || !is_project(rel_val->op) ||
				    dlist_length(cols) > list_length(rel_val->exps)) {
					rel_destroy(r);
					return sql_error(sql, 02, "UPDATE: too many columns specified");
				}
				nr = (list_length(rel_val->exps)-dlist_length(cols));
				for(n=rel_val->exps->h; nr; nr--, n = n->next)
					; 
				for(m = cols->h; n && m; n = n->next, m = m->next) {
					char *cname = m->data.sval;
					sql_column *c = mvc_bind_column(sql, t, cname);
					sql_exp *v = n->data;

					if (!exp_name(v))
						exp_label(sql->sa, v, ++sql->label);
					v = exp_column(sql->sa, exp_relname(v), exp_name(v), exp_subtype(v), v->card, has_nil(v), is_intern(v));
					if (!v) { /* check for NULL */
						v = exp_atom(sql->sa, atom_general(sql->sa, &c->type, NULL));
					} else if ((v = update_check_column(sql, t, c, v, r, cname)) == NULL) {
						return NULL;
					}
					list_append(exps, exp_column(sql->sa, t->base.name, cname, &c->type, CARD_MULTI, 0, 0));
					assert(!updates[c->colnr]);
					exp_setname(sql->sa, v, c->t->base.name, c->base.name);
					updates[c->colnr] = v;
				}
			} else {
				char *cname = assignment->h->next->data.sval;
				sql_column *c = mvc_bind_column(sql, t, cname);

				if (!v) {
					v = exp_atom(sql->sa, atom_general(sql->sa, &c->type, NULL));
				} else if ((v = update_check_column(sql, t, c, v, r, cname)) == NULL) {
					return NULL;
				}
				list_append(exps, exp_column(sql->sa, t->base.name, cname, &c->type, CARD_MULTI, 0, 0));
				exp_setname(sql->sa, v, c->t->base.name, c->base.name);
				updates[c->colnr] = v;
			}
		}
		e = exp_column(sql->sa, rname, TID, sql_bind_localtype("oid"), CARD_MULTI, 0, 1);
		r = rel_project(sql->sa, r, append(new_exp_list(sql->sa),e));
		r = rel_update(sql, bt, r, updates, exps);
		return r;
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
	return r;
}

static sql_rel *
delete_table(mvc *sql, dlist *qname, symbol *opt_where)
{
	char *sname = qname_schema(qname);
	char *tname = qname_table(qname);
	sql_schema *schema = NULL;
	sql_table *t = NULL;

	if (sname && !(schema=mvc_bind_schema(sql, sname))) {
		(void) sql_error(sql, 02, "3F000!DELETE FROM: no such schema '%s'", sname);
		return NULL;
	}
	if (!schema)
		schema = cur_schema(sql);
	t = mvc_bind_table(sql, schema, tname);
	if (!t && !sname) {
		schema = tmp_schema(sql);
		t = mvc_bind_table(sql, schema, tname);
		if (!t) 
			t = mvc_bind_table(sql, NULL, tname);
		if (!t) 
			t = stack_find_table(sql, tname);
	}
	if (update_allowed(sql, t, tname, "DELETE FROM", "delete from", 1) != NULL) {
		sql_rel *r = NULL;

		if (opt_where) {
			int status = sql->session->status;

			if (!table_privs(sql, t, PRIV_SELECT)) 
				return sql_error(sql, 02, "DELETE FROM: insufficient privileges for user '%s' to delete from table '%s'", stack_get_string(sql, "current_user"), tname);

			r = rel_logical_exp(sql, NULL, opt_where, sql_where);
			if (r) { /* simple predicate which is not using the to 
		    		    be updated table. We add a select all */
				sql_rel *l = rel_basetable(sql, t, t->base.name );
				r = rel_crossproduct(sql->sa, l, r, op_join);
			} else {
				sql->errstr[0] = 0;
				sql->session->status = status;
				r = rel_basetable(sql, t, t->base.name );
				r = rel_logical_exp(sql, r, opt_where, sql_where);
			}
			if (!r) {
				return NULL;
			} else {
				sql_exp *e = exp_column(sql->sa, rel_name(r), TID, sql_bind_localtype("oid"), CARD_MULTI, 0, 1);

				r = rel_project(sql->sa, r, append(new_exp_list(sql->sa), e));

			}
			r = rel_delete(sql->sa, rel_basetable(sql, t, tname), r);
		} else {	/* delete all */
			r = rel_delete(sql->sa, rel_basetable(sql, t, tname), NULL);
		}
		return r;
	}
	return NULL;
}

static list *
table_column_types(sql_allocator *sa, sql_table *t)
{
	node *n;
	list *types = sa_list(sa);

	if (t->columns.set) for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		if (c->base.name[0] != '%')
			append(types, &c->type);
	}
	return types;
}

static list *
table_column_names(sql_allocator *sa, sql_table *t)
{
	node *n;
	list *types = sa_list(sa);

	if (t->columns.set) for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		append(types, &c->base.name);
	}
	return types;
}

static sql_rel *
rel_import(mvc *sql, sql_table *t, char *tsep, char *rsep, char *ssep, char *ns, char *filename, lng nr, lng offset, int locked, int best_effort, dlist *fwf_widths)
{
	sql_rel *res;
	list *exps, *args;
	node *n;
	sql_subtype tpe;
	sql_exp *import;
	sql_schema *sys = mvc_bind_schema(sql, "sys");
	sql_subfunc *f = sql_find_func(sql->sa, sys, "copyfrom", 11, F_UNION, NULL);
	char *fwf_string = NULL;
	
	if (!f) /* we do expect copyfrom to be there */
		return NULL;
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
		if(list_length(f->res) != ncol) {
			(void) sql_error(sql, 02, "3F000!COPY INTO: fixed width import for %d columns but %d widths given.", list_length(f->res), ncol);
			return NULL;
		}
		*fwf_string_cur = '\0';
	}

	append( args, exp_atom_str(sql->sa, filename, &tpe)); 
	import = exp_op(sql->sa,  
	append(
		append(
			append( 
				append(
					append( args,
						exp_atom_lng(sql->sa, nr)),
						exp_atom_lng(sql->sa, offset)),
						exp_atom_int(sql->sa, locked)),
						exp_atom_int(sql->sa, best_effort)),
						exp_atom_str(sql->sa, fwf_string, &tpe)), f);
	
	exps = new_exp_list(sql->sa);
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		if (c->base.name[0] != '%')
			append(exps, exp_column(sql->sa, t->base.name, c->base.name, &c->type, CARD_MULTI, c->null, 0));
	}
	res = rel_table_func(sql->sa, NULL, import, exps, 1);
	return res;
}

static sql_rel *
copyfrom(mvc *sql, dlist *qname, dlist *columns, dlist *files, dlist *headers, dlist *seps, dlist *nr_offset, str null_string, int locked, int best_effort, int constraint, dlist *fwf_widths)
{
	sql_rel *rel = NULL;
	char *sname = qname_schema(qname);
	char *tname = qname_table(qname);
	sql_schema *s = NULL;
	sql_table *t = NULL, *nt = NULL;
	char *tsep = seps->h->data.sval;
	char *rsep = seps->h->next->data.sval;
	char *ssep = (seps->h->next->next)?seps->h->next->next->data.sval:NULL;
	char *ns = (null_string)?null_string:"null";
	lng nr = (nr_offset)?nr_offset->h->data.l_val:-1;
	lng offset = (nr_offset)?nr_offset->h->next->data.l_val:0;
	list *collist;
	int reorder = 0;
	assert(!nr_offset || nr_offset->h->type == type_lng);
	assert(!nr_offset || nr_offset->h->next->type == type_lng);
	if (sname && !(s=mvc_bind_schema(sql, sname))) {
		(void) sql_error(sql, 02, "3F000!COPY INTO: no such schema '%s'", sname);
		return NULL;
	}
	if (!s)
		s = cur_schema(sql);
	t = mvc_bind_table(sql, s, tname);
	if (!t && !sname) {
		s = tmp_schema(sql);
		t = mvc_bind_table(sql, s, tname);
		if (!t) 
			t = stack_find_table(sql, tname);
	}
	if (insert_allowed(sql, t, tname, "COPY INTO", "copy into") == NULL) 
		return NULL;
	/* Only the MONETDB user is allowed copy into with 
	   a lock and only on tables without idx */
	if (locked && !copy_allowed(sql, 1)) {
		return sql_error(sql, 02, "COPY INTO: insufficient privileges: "
		    "COPY INTO from .. LOCKED requires database administrator rights");
	}
	if (locked && (!list_empty(t->idxs.set) || !list_empty(t->keys.set))) {
		return sql_error(sql, 02, "COPY INTO: insufficient privileges: "
		    "COPY INTO from .. LOCKED requires tables without indices");
	}
	if (locked && has_snapshots(sql->session->tr)) {
		return sql_error(sql, 02, "COPY INTO .. LOCKED: not allowed on snapshots");
	}
	if (locked && !sql->session->auto_commit) {
		return sql_error(sql, 02, "COPY INTO .. LOCKED: only allowed in auto commit mode");
	}
	/* lock the store, for single user/transaction */
	if (locked) { 
		if (headers)
			return sql_error(sql, 02, "COPY INTO .. LOCKED: not allowed with column lists");
		store_lock();
		while (store_nr_active > 1) {
			store_unlock();
			MT_sleep_ms(100);
			store_lock();
		}
		sql->emod |= mod_locked;
		sql->caching = 0; 	/* do not cache this query */
	}
		 
	collist = check_table_columns(sql, t, columns, "COPY", tname);
	if (!collist)
		return NULL;
	/* If we have a header specification use intermediate table, for
	 * column specification other then the default list we need to reorder
	 */
	nt = t;
	if (headers || collist != t->columns.set) 
		reorder = 1;
	if (headers) {
		int has_formats = 0;
		dnode *n;

		nt = mvc_create_table(sql, s, tname, tt_table, 0, SQL_DECLARED_TABLE, CA_COMMIT, -1);
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

		if (!copy_allowed(sql, 1))
			return sql_error(sql, 02, "COPY INTO: insufficient privileges: "
					"COPY INTO from file(s) requires database administrator rights, "
					"use 'COPY INTO \"%s\" FROM STDIN' instead", tname);

		for (; n; n = n->next) {
			char *fname = n->data.sval;
			sql_rel *nrel;

			if (fname && !MT_path_absolute(fname))
				return sql_error(sql, 02, "COPY INTO: filename must "
						"have absolute path: %s", fname);

			nrel = rel_import(sql, nt, tsep, rsep, ssep, ns, fname, nr, offset, locked, best_effort, fwf_widths);

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
		rel = rel_import(sql, nt, tsep, rsep, ssep, ns, NULL, nr, offset, locked, best_effort, NULL);
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
				sql_schema *sys = mvc_bind_schema(sql, "sys");
				sql_subtype st;
				sql_subfunc *f;
				list *args = sa_list(sql->sa);
				size_t l = strlen(cs->type.type->sqlname);
				char *fname = sa_alloc(sql->sa, l+8);

				snprintf(fname, l+8, "str_to_%s", cs->type.type->sqlname);
				sql_find_subtype(&st, "clob", 0, 0);
				f = sql_bind_func_result(sql->sa, sys, fname, &st, &st, &cs->type); 
				if (!f)
					return sql_error(sql, 02, "COPY INTO: '%s' missing for type %s", fname, cs->type.type->sqlname);
				append(args, e);
				append(args, exp_atom_clob(sql->sa, format));
				ne = exp_op(sql->sa, args, f);
				exp_setname(sql->sa, ne, exp_relname(e), exp_name(e));
				append(nexps, ne);
			} else {
				append(nexps, e);
			}
			m = m->next;
		}
		rel = rel_project(sql->sa, rel, nexps);
		reorder = 0;
	}
	
	if (!rel)
		return rel;
	if (reorder) 
		rel = rel_project(sql->sa, rel, rel_inserts(sql, t, rel, collist, 1, 1));
	else
		rel->exps = rel_inserts(sql, t, rel, collist, 1, 0);
	rel = rel_insert_table(sql, t, tname, rel);
	if (rel && locked)
		rel->flag |= UPD_LOCKED;
	if (rel && !constraint)
		rel->flag |= UPD_NO_CONSTRAINT;
	return rel;
}

static sql_rel *
bincopyfrom(mvc *sql, dlist *qname, dlist *columns, dlist *files, int constraint)
{
	char *sname = qname_schema(qname);
	char *tname = qname_table(qname);
	sql_schema *s = NULL;
	sql_table *t = NULL;

	dnode *dn;
	node *n;
	sql_rel *res;
	list *exps, *args;
	sql_subtype strtpe;
	sql_exp *import;
	sql_schema *sys = mvc_bind_schema(sql, "sys");
	sql_subfunc *f = sql_find_func(sql->sa, sys, "copyfrom", 2, F_UNION, NULL); 
	list *collist;
	int i;

	assert(f);
	if (!copy_allowed(sql, 1)) {
		(void) sql_error(sql, 02, "COPY INTO: insufficient privileges: "
				"binary COPY INTO requires database administrator rights");
		return NULL;
	}

	if (sname && !(s=mvc_bind_schema(sql, sname))) {
		(void) sql_error(sql, 02, "3F000!COPY INTO: no such schema '%s'", sname);
		return NULL;
	}
	if (!s)
		s = cur_schema(sql);
	t = mvc_bind_table(sql, s, tname);
	if (!t && !sname) {
		s = tmp_schema(sql);
		t = mvc_bind_table(sql, s, tname);
		if (!t) 
			t = stack_find_table(sql, tname);
	}
	if (insert_allowed(sql, t, tname, "COPY INTO", "copy into") == NULL) 
		return NULL;
	if (files == NULL)
		return sql_error(sql, 02, "COPY INTO: must specify files");

	collist = check_table_columns(sql, t, columns, "COPY BINARY", tname);
	if (!collist)
		return NULL;

	f->res = table_column_types(sql->sa, t);
 	sql_find_subtype(&strtpe, "varchar", 0, 0);
	args = append( append( new_exp_list(sql->sa),
		exp_atom_str(sql->sa, t->s?t->s->base.name:NULL, &strtpe)), 
		exp_atom_str(sql->sa, t->base.name, &strtpe));

	// create the list of files that is passed to the function as parameter
	for(i = 0; i < list_length(t->columns.set); i++) {
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
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		append(exps, exp_column(sql->sa, t->base.name, c->base.name, &c->type, CARD_MULTI, c->null, 0));
	}
	res = rel_table_func(sql->sa, NULL, import, exps, 1);
	res = rel_insert_table(sql, t, t->base.name, res);
	if (res && !constraint)
		res->flag |= UPD_NO_CONSTRAINT;
	return res;
}


static sql_rel *
copyfromloader(mvc *sql, dlist *qname, symbol *fcall)
{
	char *sname = qname_schema(qname);
	char *tname = qname_table(qname);

	sql_schema *s = NULL;
	sql_table *t = NULL;

	node *n;
	sql_rel res_obj ;
	sql_rel *res = &res_obj;
	list *exps = new_exp_list(sql->sa); //, *args = NULL;
	sql_exp *import;
	exp_kind ek = {type_value, card_loader, FALSE};

	if (!copy_allowed(sql, 1)) {
		(void) sql_error(sql, 02, "COPY INTO: insufficient privileges: "
				"binary COPY INTO requires database administrator rights");
		return NULL;
	}

	if (sname && !(s=mvc_bind_schema(sql, sname))) {
		(void) sql_error(sql, 02, "3F000!COPY INTO: no such schema '%s'", sname);
		return NULL;
	}
	if (!s)
		s = cur_schema(sql);
	t = mvc_bind_table(sql, s, tname);
	if (!t && !sname) {
		s = tmp_schema(sql);
		t = mvc_bind_table(sql, s, tname);
		if (!t)
			t = stack_find_table(sql, tname);
	}
	if (insert_allowed(sql, t, tname, "COPY INTO", "copy into") == NULL) {
		return NULL;
	}

	import = rel_value_exp(sql, &res, fcall, sql_sel, ek);
	if (!import) {
		return NULL;
	}
	((sql_subfunc*) import->f)->res = table_column_types(sql->sa, t);
	((sql_subfunc*) import->f)->colnames = table_column_names(sql->sa, t);

	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		append(exps, exp_column(sql->sa, t->base.name, c->base.name, &c->type, CARD_MULTI, c->null, 0));
	}

	res = rel_table_func(sql->sa, NULL, import, exps, 1);
	return  rel_insert_table(sql, t, t->base.name, res);
}


static sql_rel *
rel_output(mvc *sql, sql_rel *l, sql_exp *sep, sql_exp *rsep, sql_exp *ssep, sql_exp *null_string, sql_exp *file) 
{
	sql_rel *rel = rel_create(sql->sa);
	list *exps = new_exp_list(sql->sa);
	if(!rel || !exps)
		return NULL;

	append(exps, sep);
	append(exps, rsep);
	append(exps, ssep);
	append(exps, null_string);
	if (file)
		append(exps, file);
	rel->l = l;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = DDL_OUTPUT;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

static sql_rel *
copyto(mvc *sql, symbol *sq, str filename, dlist *seps, str null_string)
{
	char *tsep = seps->h->data.sval;
	char *rsep = seps->h->next->data.sval;
	char *ssep = (seps->h->next->next)?seps->h->next->next->data.sval:"\"";
	char *ns = (null_string)?null_string:"null";
	sql_exp *tsep_e, *rsep_e, *ssep_e, *ns_e, *fname_e;
	exp_kind ek = {type_value, card_relation, TRUE};
	sql_rel *r = rel_subquery(sql, NULL, sq, ek, APPLY_JOIN);

	if (!r) 
		return NULL;

	tsep_e = exp_atom_clob(sql->sa, tsep);
	rsep_e = exp_atom_clob(sql->sa, rsep);
	ssep_e = exp_atom_clob(sql->sa, ssep);
	ns_e = exp_atom_clob(sql->sa, ns);
	fname_e = filename?exp_atom_clob(sql->sa, filename):NULL;

	if (filename) {
		struct stat fs;
		if (!copy_allowed(sql, 0)) 
			return sql_error(sql, 02, "COPY INTO: insufficient privileges: "
					"COPY INTO file requires database administrator rights, "
					"use 'COPY ... INTO STDOUT' instead");
		if (filename && !MT_path_absolute(filename))
			return sql_error(sql, 02, "COPY INTO: filename must "
					"have absolute path: %s", filename);
		if (lstat(filename, &fs) == 0)
			return sql_error(sql, 02, "COPY INTO: file already "
					"exists: %s", filename);
	}

	return rel_output(sql, r, tsep_e, rsep_e, ssep_e, ns_e, fname_e);
}

sql_exp *
rel_parse_val(mvc *m, char *query, char emode)
{
	mvc o = *m;
	sql_exp *e = NULL;
	buffer *b;
	char *n;
	int len = _strlen(query);
	exp_kind ek = {type_value, card_value, FALSE};
	stream *s;
	bstream *bs;

	m->qc = NULL;

	m->caching = 0;
	m->emode = emode;
	b = (buffer*)GDKmalloc(sizeof(buffer));
	n = GDKmalloc(len + 1 + 1);
	if(!b || !n) {
		GDKfree(b);
		GDKfree(n);
		return NULL;
	}
	strncpy(n, query, len);
	query = n;
	query[len] = '\n';
	query[len+1] = 0;
	len++;
	buffer_init(b, query, len);
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
	/*m->args = NULL;*/
	m->argc = 0;
	m->sym = NULL;
	m->errstr[0] = '\0';
	/* via views we give access to protected objects */
	m->user_id = USER_MONETDB;

	(void) sqlparse(m);	
	
	/* get out the single value as we don't want an enclosing projection! */
	if (m->sym && m->sym->token == SQL_SELECT) {
		SelectNode *sn = (SelectNode *)m->sym;
		if (sn->selection->h->data.sym->token == SQL_COLUMN) {
			int is_last = 0;
			sql_rel *r = NULL;
			symbol* sq = sn->selection->h->data.sym->data.lval->h->data.sym;
			e = rel_value_exp2(m, &r, sq, sql_sel, ek, &is_last);
		}
	}
	GDKfree(query);
	GDKfree(b);
	bstream_destroy(m->scanner.rs);

	m->sym = NULL;
	if (m->session->status || m->errstr[0]) {
		int status = m->session->status;
		char errstr[ERRSIZE];

		strcpy(errstr, m->errstr);
		*m = o;
		m->session->status = status;
		strcpy(m->errstr, errstr);
	} else {
		*m = o;
	}
	return e;
}

sql_rel *
rel_updates(mvc *sql, symbol *s)
{
	sql_rel *ret = NULL;
	int old = sql->use_views;

	sql->use_views = 1;
	switch (s->token) {
	case SQL_COPYFROM:
	{
		dlist *l = s->data.lval;

		ret = copyfrom(sql, 
				l->h->data.lval, 
				l->h->next->data.lval, 
				l->h->next->next->data.lval, 
				l->h->next->next->next->data.lval, 
				l->h->next->next->next->next->data.lval, 
				l->h->next->next->next->next->next->data.lval, 
				l->h->next->next->next->next->next->next->data.sval, 
				l->h->next->next->next->next->next->next->next->data.i_val, 
				l->h->next->next->next->next->next->next->next->next->data.i_val, 
				l->h->next->next->next->next->next->next->next->next->next->data.i_val,
				l->h->next->next->next->next->next->next->next->next->next->next->data.lval);
		sql->type = Q_UPDATE;
	}
		break;
	case SQL_BINCOPYFROM:
	{
		dlist *l = s->data.lval;

		ret = bincopyfrom(sql, l->h->data.lval, l->h->next->data.lval, l->h->next->next->data.lval, l->h->next->next->next->data.i_val);
		sql->type = Q_UPDATE;
	}
		break;
	case SQL_COPYLOADER:
	{
		dlist *l = s->data.lval;

		ret = copyfromloader(sql, l->h->data.lval, l->h->next->data.sym);
		sql->type = Q_UPDATE;
	}
		break;
	case SQL_COPYTO:
	{
		dlist *l = s->data.lval;

		ret = copyto(sql, l->h->data.sym, l->h->next->data.sval, l->h->next->next->data.lval, l->h->next->next->next->data.sval);
		sql->type = Q_UPDATE;
	}
		break;
	case SQL_INSERT:
	{
		dlist *l = s->data.lval;

		ret = insert_into(sql, l->h->data.lval, l->h->next->data.lval, l->h->next->next->data.sym);
		sql->type = Q_UPDATE;
	}
		break;
	case SQL_UPDATE:
	{
		dlist *l = s->data.lval;

		ret = update_table(sql, l->h->data.lval, l->h->next->data.lval, l->h->next->next->data.sym, l->h->next->next->next->data.sym);
		sql->type = Q_UPDATE;
	}
		break;
	case SQL_DELETE:
	{
		dlist *l = s->data.lval;

		ret = delete_table(sql, l->h->data.lval, l->h->next->data.sym);
		sql->type = Q_UPDATE;
	}
		break;
	default:
		sql->use_views = old;
		return sql_error(sql, 01, "Updates statement unknown Symbol(" PTRFMT ")->token = %s", PTRFMTCAST s, token2string(s->token));
	}
	sql->use_views = old;
	return ret;
}
