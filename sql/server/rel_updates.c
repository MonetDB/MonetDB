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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */


#include "monetdb_config.h"
#include "rel_updates.h"
#include "rel_semantic.h"
#include "rel_select.h"
#include "rel_exp.h"
#include "rel_subquery.h"
#include "sql_privileges.h"

static sql_exp *
nth( list *l, int n)
{
	int i;
	node *m;

	for (i=0, m = l->h; i<n && m; i++, m = m->next) ; 
	if (m)
		return m->data;
	return NULL;
}

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
insert_exp_array(sql_table *t, int *Len)
{
	sql_exp **inserts;
	int i, len = list_length(t->columns.set);
	node *m;

	*Len = len;
	inserts = NEW_ARRAY(sql_exp *, len);
	for (m = t->columns.set->h, i = 0; m; m = m->next, i++) {
		sql_column *c = m->data;

		c->colnr = i;
		inserts[i] = NULL;
	}
	return inserts;
}

#define get_basetable(rel) rel->l

sql_table *
rel_ddl_table_get(sql_rel *r)
{
	if (r->flag == DDL_ALTER_TABLE || r->flag == DDL_CREATE_TABLE || r->flag == DDL_CREATE_VIEW) {
		sql_exp *e = r->exps->t->data;
		atom *a = e->l;

		return a->data.val.pval;
	}
	return NULL;
}

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
	node *m;
	sql_subtype *it, *wrd;
	int bits = 1 + ((sizeof(wrd)*8)-1)/(list_length(i->columns)+1);
	sql_exp *h = NULL;

	if (list_length(i->columns) <= 1 || i->type == no_idx) {
		/* dummy append */
		append(get_inserts(inserts), exp_label(sql->sa, exp_atom_wrd(sql->sa, 0), ++sql->label));
		return inserts;
	}

	it = sql_bind_localtype("int");
	wrd = sql_bind_localtype("wrd");
	for (m = i->columns->h; m; m = m->next) {
		sql_kc *c = m->data;
		sql_exp *e = nth(get_inserts(inserts), c->c->colnr);

		if (h && i->type == hash_idx)  { 
			list *exps = new_exp_list(sql->sa);
			sql_subfunc *xor = sql_bind_func_result3(sql->sa, sql->session->schema, "rotate_xor_hash", wrd, it, &c->c->type, wrd);

			append(exps, h);
			append(exps, exp_atom_int(sql->sa, bits));
			append(exps, e);
			h = exp_op(sql->sa, exps, xor);
		} else if (h)  { /* order preserving hash */
			sql_exp *h2;
			sql_subfunc *lsh = sql_bind_func_result(sql->sa, sql->session->schema, "left_shift", wrd, it, wrd);
			sql_subfunc *lor = sql_bind_func_result(sql->sa, sql->session->schema, "bit_or", wrd, wrd, wrd);
			sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", &c->c->type, NULL, wrd);

			h = exp_binop(sql->sa, h, exp_atom_int(sql->sa, bits), lsh); 
			h2 = exp_unop(sql->sa, e, hf);
			h = exp_binop(sql->sa, h, h2, lor);
		} else {
			sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", &c->c->type, NULL, wrd);
			h = exp_unop(sql->sa, e, hf);
			if (i->type == oph_idx) 
				break;
		}
	}
	/* append inserts to hash */
	append(get_inserts(inserts), h);
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
	sql_exp *nll_exps = NULL, *e;
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
		sql_subfunc *isnil = sql_bind_func(sql->sa, sql->session->schema, "isnull", &c->c->type, NULL);
		sql_exp *_is = nth(ins->exps, c->c->colnr), *nl, *je; 
		sql_exp *rtc = exp_column(sql->sa, rel_name(rt), rc->c->base.name, &rc->c->type, CARD_MULTI, rc->c->null, 0);
		char *ename = exp_name(_is);

		if (!ename)
			exp_label(sql->sa, _is, ++sql->label);
		ename = exp_name(_is);
		_is = exp_column(sql->sa, exp_relname(_is), ename, exp_subtype(_is), _is->card, has_nil(_is), is_intern(_is));
		nl = exp_unop(sql->sa, _is, isnil);
		if (need_nulls) {
		    if (nll_exps) {
			nll_exps = exp_binop(sql->sa, nll_exps, nl, or);
		    } else {
			nll_exps = nl;
		    }
		}

		if (rel_convert_types(sql, &rtc, &_is, 1, type_equal) < 0) 
			return NULL;
		je = exp_compare(sql->sa, rtc, _is, cmp_equal);
		append(join_exps, je);
	}
	if (need_nulls) {
       		_nlls = rel_select( sql->sa, rel_dup(ins), 
				exp_compare(sql->sa, nll_exps, exp_atom_bool(sql->sa, 1), cmp_equal ));
        	nnlls = rel_select( sql->sa, rel_dup(ins), 
				exp_compare(sql->sa, nll_exps, exp_atom_bool(sql->sa, 0), cmp_equal ));
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
	e = exp_column(sql->sa, rel_name(rt), "%TID%", sql_bind_localtype("oid"), CARD_MULTI, 0, 1);
	exp_setname(sql->sa, e, i->t->base.name, iname);
	append(nnlls->exps, e);

	if (need_nulls) {
		rel_destroy(ins);
		rt = inserts->r = rel_setop(sql->sa, _nlls, nnlls, op_union );
		rt->exps = rel_projections(sql, nnlls, NULL, 1, 1);
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

	inserts->r = rel_label(sql, inserts->r); 
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

static sql_rel *
insert_into(mvc *sql, dlist *qname, dlist *columns, symbol *val_or_q)
{
	int i, len = 0;
	char *sname = qname_schema(qname);
	char *tname = qname_table(qname);
	sql_schema *s = NULL;
	sql_table *t = NULL;
	list *collist = NULL, *exps;
	sql_rel *r = NULL;
	sql_exp **inserts;
	node *n, *m;

	if (sname && !(s=mvc_bind_schema(sql, sname))) {
		(void) sql_error(sql, 02, "INSERT INTO: no such schema '%s'", sname);
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
	if (!t) {
		return sql_error(sql, 02, "INSERT INTO: no such table '%s'", tname);
	} else if (isView(t)) {
		return sql_error(sql, 02, "INSERT INTO: cannot insert into view '%s'", tname);
	} else if (t->readonly) {
		return sql_error(sql, 02, "INSERT INTO: cannot insert into read only table '%s'", tname);
	}
	if (t && !isTempTable(t) && STORE_READONLY(active_store_type))
		return sql_error(sql, 02, "INSERT INTO: insert into table '%s' not allowed in readonly mode", tname);

	if (!table_privs(sql, t, PRIV_INSERT)) {
		return sql_error(sql, 02, "INSERT INTO: insufficient privileges for user '%s' to insert into table '%s'", stack_get_string(sql, "current_user"), tname);
	}
	if (columns) {
		dnode *n;

		collist = list_new(sql->sa);
		for (n = columns->h; n; n = n->next) {
			sql_column *c = mvc_bind_column(sql, t, n->data.sval);

			if (c) {
				list_append(collist, c);
			} else {
				return sql_error(sql, 02, "INSERT INTO: no such column '%s.%s'", tname, n->data.sval);
			}
		}
	} else {
		collist = t->columns.set;
	}

	if (val_or_q->token == SQL_VALUES) {
		dlist *rowlist = val_or_q->data.lval;
		dlist *values;
		dnode *o;

		if (!rowlist->h) {
			r = rel_project(sql->sa, NULL, NULL);
			if (!columns)
				collist = NULL;
		}

		for (o = rowlist->h; o; o = o->next) {
			values = o->data.lval;

			if (dlist_length(values) != list_length(collist)) {
				return sql_error(sql, 02, "INSERT INTO: number of values doesn't match number of columns of table '%s'", tname);
			} else {
				sql_rel *inner = NULL;
				sql_rel *i = NULL;
				list *exps = new_exp_list(sql->sa);
				dnode *n;

				for (n = values->h, m = collist->h; n && m; n = n->next, m = m->next) {
					sql_column *c = m->data;
					sql_rel *r = NULL;
					sql_exp *ins = insert_value(sql, c, &r, n->data.sym);
					if (!ins)
						return NULL;
					if (r && inner)
						inner = rel_crossproduct(sql->sa, inner,r, op_join);
					else if (r) 
						inner = r;
					if (!ins->name)
						exp_label(sql->sa, ins, ++sql->label);
					list_append(exps, ins);
				}
				i = rel_project(sql->sa, inner, exps);
				if (r) {
					r = rel_setop(sql->sa, r, i, op_union);
				} else {
					r = i;
				}
			}
		}
	} else {
		exp_kind ek = {type_value, card_relation, TRUE};

		r = rel_subquery(sql, NULL, val_or_q, ek);
	}
	if (!r) 
		return NULL;

	/* In case of missing project, order by or distinct, we need to add	
	   and projection */
	if (r->op != op_project || r->r || need_distinct(r))
		r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 0, 0));
	if ((r->exps && list_length(r->exps) != list_length(collist)) ||
	   (!r->exps && collist)) 
		return sql_error(sql, 02, "INSERT INTO: query result doesn't match number of columns in table '%s'", tname);

	inserts = insert_exp_array(t, &len);

	if (r->exps) {
		for (n = r->exps->h, m = collist->h; n && m; n = n->next, m = m->next) {
			sql_column *c = m->data;
			sql_exp *e = n->data;
	
			inserts[c->colnr] = rel_check_type(sql, &c->type, e, type_equal);
		}
	}

	for (i = 0; i < len; i++) {
		if (!inserts[i]) {
			for (m = t->columns.set->h; m; m = m->next) {
				sql_column *c = m->data;

				if (c->colnr == i) {
					sql_exp *e = NULL;

					if (c->def) {
						char *q = sql_message( "select %s;", c->def);
						e = rel_parse_val(sql, q, sql->emode);
						_DELETE(q);
						if (!e || (e = rel_check_type(sql, &c->type, e, type_equal)) == NULL)
							return NULL;
					} else {
						atom *a = atom_general(sql->sa, &c->type, NULL);
						e = exp_atom(sql->sa, a);
					}
					if (!e) 
						return sql_error(sql, 02, "INSERT INTO: column '%s' has no valid default value", c->base.name);
					inserts[i] = e;
				}
			}
			assert(inserts[i]);
		}
	}
	/* now rewrite project exps in proper table order */
	exps = new_exp_list(sql->sa);
	for (i = 0; i<len; i++) 
		list_append(exps, inserts[i]);
	_DELETE(inserts);
	list_destroy(r->exps);
	r->exps = exps;
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
	sql_subtype *it, *wrd = 0; /* is not set in first if below */
	int bits = 1 + ((sizeof(wrd)*8)-1)/(list_length(i->columns)+1);
	sql_exp *h = NULL;

	if (list_length(i->columns) <= 1 || i->type == no_idx) {
		h = exp_label(sql->sa, exp_atom_wrd(sql->sa, 0), ++sql->label);
	} else {
		it = sql_bind_localtype("int");
		wrd = sql_bind_localtype("wrd");
		for (m = i->columns->h; m; m = m->next) {
			sql_kc *c = m->data;
			sql_exp *e;

	       		e = nth(get_inserts(updates), c->c->colnr+1);
			
			if (h && i->type == hash_idx)  { 
				list *exps = new_exp_list(sql->sa);
				sql_subfunc *xor = sql_bind_func_result3(sql->sa, sql->session->schema, "rotate_xor_hash", wrd, it, &c->c->type, wrd);
	
				append(exps, h);
				append(exps, exp_atom_int(sql->sa, bits));
				append(exps, e);
				h = exp_op(sql->sa, exps, xor);
			} else if (h)  { /* order preserving hash */
				sql_exp *h2;
				sql_subfunc *lsh = sql_bind_func_result(sql->sa, sql->session->schema, "left_shift", wrd, it, wrd);
				sql_subfunc *lor = sql_bind_func_result(sql->sa, sql->session->schema, "bit_or", wrd, wrd, wrd);
				sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", &c->c->type, NULL, wrd);
	
				h = exp_binop(sql->sa, h, exp_atom_int(sql->sa, bits), lsh); 
				h2 = exp_unop(sql->sa, e, hf);
				h = exp_binop(sql->sa, h, h2, lor);
			} else {
				sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", &c->c->type, NULL, wrd);
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
	append(updates->exps, exp_column(sql->sa, i->t->base.name, iname, wrd, CARD_MULTI, 0, 0));
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
	sql_rel *rt = rel_basetable(sql, rk->t, nme);

	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *or = sql_bind_func_result(sql->sa, sql->session->schema, "or", bt, bt, bt);

	sql_rel *_nlls = NULL, *nnlls, *ups = updates->r;
	sql_exp *nll_exps = NULL, *e;
	list *join_exps = new_exp_list(sql->sa);

	for (m = i->columns->h; m; m = m->next) {
		sql_kc *c = m->data;

		if (c->c->null) 
			need_nulls = 1;
	}
	for (m = i->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *c = m->data;
		sql_kc *rc = o->data;
		sql_subfunc *isnil = sql_bind_func(sql->sa, sql->session->schema, "isnull", &c->c->type, NULL);
		sql_exp *upd = nth(get_inserts(updates), c->c->colnr + 1), *nl, *je;
		sql_exp *rtc = exp_column(sql->sa, rel_name(rt), rc->c->base.name, &rc->c->type, CARD_MULTI, rc->c->null, 0);


		/* FOR MATCH FULL/SIMPLE/PARTIAL see above */
		/* Currently only the default MATCH SIMPLE is supported */
		upd = exp_column(sql->sa, exp_relname(upd), exp_name(upd), exp_subtype(upd), upd->card, has_nil(upd), is_intern(upd));
		nl = exp_unop(sql->sa, upd, isnil);
		if (need_nulls) {
		    if (nll_exps) {
			nll_exps = exp_binop(sql->sa, nll_exps, nl, or);
		    } else {
			nll_exps = nl;
		    }
		}
		if (rel_convert_types(sql, &rtc, &upd, 1, type_equal) < 0) 
			return NULL;
		je = exp_compare(sql->sa, rtc, upd, cmp_equal);
		append(join_exps, je);
	}
	if (need_nulls) {
       		_nlls = rel_select( sql->sa, rel_dup(ups), 
				exp_compare(sql->sa, nll_exps, exp_atom_bool(sql->sa, 1), cmp_equal ));
        	nnlls = rel_select( sql->sa, rel_dup(ups), 
				exp_compare(sql->sa, nll_exps, exp_atom_bool(sql->sa, 0), cmp_equal ));
		_nlls = rel_project(sql->sa, _nlls, rel_projections(sql, _nlls, NULL, 1, 1));
		/* add constant value for NULLS */
		e = exp_atom(sql->sa, atom_general(sql->sa, sql_bind_localtype("oid"), NULL));
		exp_setname(sql->sa, e, i->t->base.name, iname);
		append(_nlls->exps, e);
	} else {
		nnlls = ups;
	}

	nnlls = rel_crossproduct(sql->sa, nnlls, rt, op_join);
	nnlls->exps = join_exps;
	nnlls = rel_project(sql->sa, nnlls, rel_projections(sql, nnlls->l, NULL, 1, 1));
	/* add row numbers */
	e = exp_column(sql->sa, rel_name(rt), "%TID%", sql_bind_localtype("oid"), CARD_MULTI, 0, 1);
	exp_setname(sql->sa, e, i->t->base.name, iname);
	append(nnlls->exps, e);

	if (need_nulls) {
		rel_destroy(ups);
		rt = updates->r = rel_setop(sql->sa, _nlls, nnlls, op_union );
		rt->exps = rel_projections(sql, nnlls, NULL, 1, 1);
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

	for (m = tab->columns.set->h; m; m = m->next) {
		sql_column *c = m->data;
		sql_exp *v = updates[c->colnr];

		if (!v) 
			v = exp_column(sql->sa, tab->base.name, c->base.name, &c->type, CARD_MULTI, c->null, 0);
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

static sql_rel *
update_table(mvc *sql, dlist *qname, dlist *assignmentlist, symbol *opt_where)
{
	char *sname = qname_schema(qname);
	char *tname = qname_table(qname);
	sql_schema *s = NULL;
	sql_table *t = NULL;

	if (sname && !(s=mvc_bind_schema(sql,sname))) {
		(void) sql_error(sql, 02, "UPDATE: no such schema '%s'", sname);
		return NULL;
	}
	if (!s)
		s = cur_schema(sql);
	t = mvc_bind_table(sql, s, tname);
	if (!t && !sname) 
		s = tmp_schema(sql);

	t = mvc_bind_table(sql, s, tname);
	if (!t && !s) {
		sql_subtype *tpe;

		if ((tpe = stack_find_type(sql, tname)) != NULL) 
			t = tpe->comp_type;
	}
	if (!t) {
		return sql_error(sql, 02, "UPDATE: no such table '%s'", tname);
	} else if (isView(t)) {
		return sql_error(sql, 02, "UPDATE: cannot update view '%s'", tname);
	} else if (t->readonly) {
		return sql_error(sql, 02, "UPDATE: cannot update read only table '%s'", tname);
	} else {
		sql_exp *e = NULL, **updates;
		sql_rel *r = NULL;
		list *exps = new_exp_list(sql->sa), *pexps;
		dnode *n;

		if (t && !isTempTable(t) && STORE_READONLY(active_store_type))
			return sql_error(sql, 02, "UPDATE: update table '%s' not allowed in readonly mode", tname);

		if (opt_where) {
			int status = sql->session->status;
	
			r = rel_logical_exp(sql, NULL, opt_where, sql_where);
			if (r) { /* simple predicate which is not using the to 
				    be updated table. We add a select all */

				sql_rel *l = rel_basetable(sql, t, t->base.name );
				r = rel_crossproduct(sql->sa, l, r, op_semi);
			} else {
				sql->errstr[0] = 0;
				sql->session->status = status;
				r = rel_basetable(sql, t, t->base.name );
				r = rel_logical_exp(sql, r, opt_where, sql_where);
				if (r && is_join(r->op))
					r->op = op_semi;
			}
			if (!r) 
				return NULL;
		} else {	/* update all */
			r = rel_basetable(sql, t, t->base.name );
		}
	
		pexps = rel_projections(sql, r, NULL, 1, 0);
		/* We simply create a relation %TID%, updates */

		/* first create the project */
		e = exp_column(sql->sa, rel_name(r), "%TID%", sql_bind_localtype("oid"), CARD_MULTI, 0, 1);
		r = rel_project(sql->sa, r, append(new_exp_list(sql->sa),e));
		e = exp_column(sql->sa, rel_name(r), "%TID%", sql_bind_localtype("oid"), CARD_MULTI, 0, 1);
		append(exps, e);
		updates = table_update_array(sql, t);
		for (n = assignmentlist->h; n; n = n->next) {
			symbol *a = NULL;
			sql_exp *v = NULL;
			dlist *assignment = n->data.sym->data.lval;
			char *cname = assignment->h->next->data.sval;
			sql_column *c = mvc_bind_column(sql, t, cname);

			if (!c) {
				rel_destroy(r);
				return sql_error(sql, 02, "UPDATE: no such column '%s.%s'", t->base.name, cname);
			}
			a = assignment->h->data.sym;
			if (a) {
				int status = sql->session->status;
				sql_rel *rel_val = NULL;
				exp_kind ek = {type_value, card_column, FALSE};

				v = rel_value_exp(sql, &rel_val, a, sql_sel, ek);

				if (!v) {
					symbol *s = n->data.sym;
					sql->errstr[0] = 0;
					sql->session->status = status;
					/*v = rel_value_exp(sql, &r, a, sql_sel, ek);*/
					s->token = SQL_COLUMN;
					v = rel_column_exp(sql, &r, s, sql_sel);

					if (v && r && r->op == op_project) {
						sql_rel *rl = r->l;

						if (rl && rl->op == op_project)
							list_merge(rl->exps, pexps, (fdup)NULL);
					}
				}
				if (!v || (v = rel_check_type(sql, &c->type, v, type_equal)) == NULL) {
					rel_destroy(r);
					return NULL;
				}
				if (rel_val) {
					sql_rel *nr;
					list *exps;

					if (!exp_name(v))
						exp_label(sql->sa, v, ++sql->label);
					rel_val = rel_project(sql->sa, rel_val, rel_projections(sql, rel_val, NULL, 0, 1));
					rel_project_add_exp(sql, rel_val, v);
					exps = rel_projections(sql, r, NULL, 0, 1);
					nr = rel_project(sql->sa, rel_crossproduct(sql->sa, rel_dup(r->l), rel_val, op_join), exps);
					rel_destroy(r);
					r = nr;
					v = exp_column(sql->sa, NULL, exp_name(v), exp_subtype(v), v->card, has_nil(v), is_intern(v));
				}		
			} else {
				v = exp_atom(sql->sa, atom_general(sql->sa, &c->type, NULL));
			}

			if (!v) {
				rel_destroy(r);
				return NULL;
			}
			list_append(exps, exp_column(sql->sa, t->base.name, cname, &c->type, CARD_MULTI, 0, 0));
			assert(!updates[c->colnr]);
			exp_setname(sql->sa, v, c->t->base.name, c->base.name);
			updates[c->colnr] = v;
		}
		r = rel_update(sql, rel_basetable(sql, t, tname), r, updates, exps);
		return r;
	}
}

sql_rel *
rel_delete(sql_allocator *sa, sql_rel *t, sql_rel *deletes)
{
	sql_rel *r = rel_create(sa);

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
		(void) sql_error(sql, 02, "DELETE FROM: no such schema '%s'", sname);
		return NULL;
	}
	if (!schema)
		schema = cur_schema(sql);
	t = mvc_bind_table(sql, schema, tname);
	if (!t && !sname) {
		schema = tmp_schema(sql);
		t = mvc_bind_table(sql, schema, tname);
		if (!t) {
			sql_subtype *tpe = stack_find_type(sql, tname);
			if (tpe)
				t = tpe->comp_type;
		}
	}
	if (!t) {
		return sql_error(sql, 02, "DELETE FROM: no such table '%s'", tname);
	} else if (isView(t)) {
		return sql_error(sql, 02, "DELETE FROM: cannot delete from view '%s'", tname);
	} else if (t->readonly) {
		return sql_error(sql, 02, "DELETE FROM: cannot delete from read only table '%s'", tname);
	}
	if (t && !isTempTable(t) && STORE_READONLY(active_store_type))
		return sql_error(sql, 02, "DELETE FROM: delete from table '%s' not allowed in readonly mode", tname);
	if (!table_privs(sql, t, PRIV_DELETE)) {
		return sql_error(sql, 02, "DELETE FROM: insufficient privileges for user '%s' to delete from table '%s'", stack_get_string(sql, "current_user"), tname);
	} else {
		sql_rel *r = NULL;

		if (opt_where) {
			int status = sql->session->status;

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
				sql_exp *e = exp_column(sql->sa, rel_name(r), "%TID%", sql_bind_localtype("oid"), CARD_MULTI, 0, 1);

				r = rel_project(sql->sa, r, append(new_exp_list(sql->sa), e));

			}
			r = rel_delete(sql->sa, rel_basetable(sql, t, tname), r);
		} else {	/* delete all */
			r = rel_delete(sql->sa, rel_basetable(sql, t, tname), NULL);
		}
		return r;
	}
}

static sql_rel *
rel_import(mvc *sql, sql_table *t, char *tsep, char *rsep, char *ssep, char *ns, char *filename, lng nr, lng offset, int locked)
{
	sql_rel *res;
	list *exps, *args;
	node *n;
	sql_subtype tpe;
	sql_exp *import;
	sql_schema *sys = mvc_bind_schema(sql, "sys");
	int len = 7 + (filename?1:0);
	sql_subfunc *f = sql_find_func(sql->sa, sys, "copyfrom", len); 
	
	f->res.comp_type = t;
 	sql_find_subtype(&tpe, "varchar", 0, 0);
	args = append( append( append( append( append( append( new_exp_list(sql->sa), 
		exp_atom_str(sql->sa, t->s?t->s->base.name:NULL, &tpe)), 
		exp_atom_str(sql->sa, t->base.name, &tpe)), 
		exp_atom_str(sql->sa, tsep, &tpe)), 
		exp_atom_str(sql->sa, rsep, &tpe)), 
		exp_atom_str(sql->sa, ssep, &tpe)), 
		exp_atom_str(sql->sa, ns, &tpe));

	if (filename)
		append( args, exp_atom_str(sql->sa, filename, &tpe)); 
	import = exp_op(sql->sa,  
		append(
			append( 
				append( args, 
					exp_atom_lng(sql->sa, nr)), 
					exp_atom_lng(sql->sa, offset)), 
					exp_atom_int(sql->sa, locked)), f); 
	
	exps = new_exp_list(sql->sa);
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		append(exps, exp_column(sql->sa, t->base.name, c->base.name, &c->type, CARD_MULTI, c->null, 0));
	}
	res = rel_table_func(sql->sa, NULL, import, exps);
	return res;
}

static sql_rel *
copyfrom(mvc *sql, dlist *qname, dlist *files, dlist *seps, dlist *nr_offset, str null_string, int locked)
{
	sql_rel *rel = NULL;
	char *sname = qname_schema(qname);
	char *tname = qname_table(qname);
	sql_schema *s = NULL;
	sql_table *t = NULL;
	char *tsep = seps->h->data.sval;
	char *rsep = seps->h->next->data.sval;
	char *ssep = (seps->h->next->next)?seps->h->next->next->data.sval:NULL;
	char *ns = (null_string)?null_string:"null";
	lng nr = (nr_offset)?nr_offset->h->data.l_val:-1;
	lng offset = (nr_offset)?nr_offset->h->next->data.l_val:0;

	assert(!nr_offset || nr_offset->h->type == type_lng);
	assert(!nr_offset || nr_offset->h->next->type == type_lng);
	if (sname && !(s=mvc_bind_schema(sql, sname))) {
		(void) sql_error(sql, 02, "COPY INTO: no such schema '%s'", sname);
		return NULL;
	}
	if (!s)
		s = cur_schema(sql);
	t = mvc_bind_table(sql, s, tname);
	if (!t && !sname) {
		s = tmp_schema(sql);
		t = mvc_bind_table(sql, s, tname);
		if (!t) {
			sql_subtype *tpe = stack_find_type(sql, tname);
			if (tpe)
				t = tpe->comp_type;
		}
	}
	if (!t) 
		return sql_error(sql, 02, "COPY INTO: no such table '%s'", tname);
	if (t->readonly) 
		return sql_error(sql, 02, "COPY INTO: cannot copy into read only table '%s'", tname);
	if (t && !isTempTable(t) && STORE_READONLY(active_store_type))
		return sql_error(sql, 02, "COPY INTO: copy into table '%s' not allowed in readonly mode", tname);

	/* Only the MONETDB user is allowed copy into with 
	   a lock and only on tables without idx */
	if (locked && sql->user_id != USER_MONETDB) {
		return sql_error(sql, 02, "COPY INTO: insufficient privileges: "
		    "COPY INTO from .. LOCKED requires administrator rights");
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
		store_lock();
		while (store_nr_active > 1) {
			store_unlock();
			MT_sleep_ms(100);
			store_lock();
		}
		sql->emod |= mod_locked;
		sql->caching = 0; 	/* do not cache this query */
	}
		 
	if (files) {
		dnode *n = files->h;

		if (sql->user_id != USER_MONETDB)
			return sql_error(sql, 02, "COPY INTO: insufficient privileges: "
					"COPY INTO from file(s) requires administrator rights, "
					"use 'COPY INTO \"%s\" FROM STDIN' instead", tname);


		for (; n; n = n->next) {
			char *fname = n->data.sval;
			sql_rel *nrel;

			if (fname && !MT_path_absolute(fname))
				return sql_error(sql, 02, "COPY INTO: filename must "
						"have absolute path: %s", fname);

			nrel = rel_import(sql, t, tsep, rsep, ssep, ns, fname, nr, offset, locked);

			if (!rel)
				rel = nrel;
			else
				rel = rel_setop(sql->sa, rel, nrel, op_union);
			if (!rel)
				return rel;
		}
	} else {
		rel = rel_import(sql, t, tsep, rsep, ssep, ns, NULL, nr, offset, locked);
	}
	if (!rel)
		return rel;
	rel = rel_insert_table(sql, t, t->base.name, rel);
	if (rel && locked)
		rel->flag |= UPD_LOCKED;
	return rel;
}

static sql_rel *
bincopyfrom(mvc *sql, dlist *qname, dlist *files)
{
	char *sname = qname_schema(qname);
	char *tname = qname_table(qname);
	sql_schema *s = NULL;
	sql_table *t = NULL;

	dnode *dn;
	node *n;
	sql_rel *res;
	list *exps, *args;
	sql_subtype tpe;
	sql_exp *import;
	sql_schema *sys = mvc_bind_schema(sql, "sys");
	sql_subfunc *f = sql_find_func(sql->sa, sys, "copyfrom", 2); 

	if (sql->user_id != USER_MONETDB) {
		(void) sql_error(sql, 02, "COPY INTO: insufficient privileges: "
				"binary COPY INTO requires administrator rights");
		return NULL;
	}

	if (sname && !(s=mvc_bind_schema(sql, sname))) {
		(void) sql_error(sql, 02, "COPY INTO: no such schema '%s'", sname);
		return NULL;
	}
	if (!s)
		s = cur_schema(sql);
	t = mvc_bind_table(sql, s, tname);
	if (!t && !sname) {
		s = tmp_schema(sql);
		t = mvc_bind_table(sql, s, tname);
		if (!t) {
			sql_subtype *tpe = stack_find_type(sql, tname);
			if (tpe)
				t = tpe->comp_type;
		}
	}
	if (!t) 
		return sql_error(sql, 02, "COPY INTO: no such table '%s'", tname);
	if (t->readonly) 
		return sql_error(sql, 02, "COPY INTO: cannot copy into read only table '%s'", tname);
	if (t && !isTempTable(t) && STORE_READONLY(active_store_type))
		return sql_error(sql, 02, "COPY INTO: copy into table '%s' not allowed in readonly mode", tname);
	if (files == NULL)
		return sql_error(sql, 02, "COPY INTO: must specify files");

	f->res.comp_type = t;
 	sql_find_subtype(&tpe, "varchar", 0, 0);
	args = append( append( new_exp_list(sql->sa), 
		exp_atom_str(sql->sa, t->s?t->s->base.name:NULL, &tpe)), 
		exp_atom_str(sql->sa, t->base.name, &tpe));

	for (dn = files->h; dn; dn = dn->next) {
		append(args, exp_atom_str(sql->sa, dn->data.sval, &tpe)); 

		/* extend the bincopyfrom, with extra args and types */
	}
	
	import = exp_op(sql->sa,  args, f); 

	exps = new_exp_list(sql->sa);
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		append(exps, exp_column(sql->sa, t->base.name, c->base.name, &c->type, CARD_MULTI, c->null, 0));
	}
	res = rel_table_func(sql->sa, NULL, import, exps);
	return rel_insert_table(sql, t, t->base.name, res);
}

static sql_rel *
rel_output(mvc *sql, sql_rel *l, sql_exp *sep, sql_exp *rsep, sql_exp *ssep, sql_exp *null_string, sql_exp *file) 
{
	sql_rel *rel = rel_create(sql->sa);
	list *exps = new_exp_list(sql->sa);

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
	sql_rel *r = rel_subquery(sql, NULL, sq, ek);

	if (!r) 
		return NULL;

	tsep_e = exp_atom_clob(sql->sa, tsep);
	rsep_e = exp_atom_clob(sql->sa, rsep);
	ssep_e = exp_atom_clob(sql->sa, ssep);
	ns_e = exp_atom_clob(sql->sa, ns);
	fname_e = filename?exp_atom_clob(sql->sa, filename):NULL;

	if (filename) {
		struct stat fs;
		if (sql->user_id != USER_MONETDB)
			return sql_error(sql, 02, "COPY INTO: insufficient privileges: "
					"COPY INTO file requires administrator rights, "
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


sql_rel *
rel_updates(mvc *sql, symbol *s)
{
	sql_rel *ret = NULL;

	switch (s->token) {
	case SQL_COPYFROM:
	{
		dlist *l = s->data.lval;

		ret = copyfrom(sql, l->h->data.lval, l->h->next->data.lval, l->h->next->next->data.lval, l->h->next->next->next->data.lval, l->h->next->next->next->next->data.sval, l->h->next->next->next->next->next->data.i_val);
		sql->type = Q_UPDATE;
	}
		break;
	case SQL_BINCOPYFROM:
	{
		dlist *l = s->data.lval;

		ret = bincopyfrom(sql, l->h->data.lval, l->h->next->data.lval);
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

		ret = update_table(sql, l->h->data.lval, l->h->next->data.lval, l->h->next->next->data.sym);
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
		return sql_error(sql, 01, "Updates statement unknown Symbol(" PTRFMT ")->token = %s", PTRFMTCAST s, token2string(s->token));
	}
	return ret;
}
