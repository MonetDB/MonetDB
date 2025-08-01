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
#include "sql_relation.h"
#include "sql_semantic.h"
#include "sql_decimal.h"
#include "rel_exp.h"
#include "rel_rel.h"
#include "rel_basetable.h"
#include "rel_prop.h"

comp_type
compare_str2type(const char *compare_op)
{
	comp_type type = cmp_filter;

	if (compare_op[0] == '=') {
		type = cmp_equal;
	} else if (compare_op[0] == '<') {
		type = cmp_lt;
		if (compare_op[1] == '>')
			type = cmp_notequal;
		else if (compare_op[1] == '=')
			type = cmp_lte;
	} else if (compare_op[0] == '>') {
		type = cmp_gt;
		if (compare_op[1] == '=')
			type = cmp_gte;
	}
	return type;
}

comp_type
swap_compare( comp_type t )
{
	switch(t) {
	case cmp_equal:
		return cmp_equal;
	case cmp_lt:
		return cmp_gt;
	case cmp_lte:
		return cmp_gte;
	case cmp_gte:
		return cmp_lte;
	case cmp_gt:
		return cmp_lt;
	case cmp_notequal:
		return cmp_notequal;
	default:
		return cmp_equal;
	}
}

comp_type
negate_compare( comp_type t )
{
	switch(t) {
	case cmp_equal:
		return cmp_notequal;
	case cmp_notequal:
		return cmp_equal;
	case cmp_lt:
		return cmp_gte;
	case cmp_lte:
		return cmp_gt;
	case cmp_gte:
		return cmp_lt;
	case cmp_gt:
		return cmp_lte;

	case cmp_in:
		return cmp_notin;
	case cmp_notin:
		return cmp_in;

	default:
		return t;
	}
}

comp_type
range2lcompare( int r )
{
	if (r&1) {
		return cmp_gte;
	} else {
		return cmp_gt;
	}
}

comp_type
range2rcompare( int r )
{
	if (r&2) {
		return cmp_lte;
	} else {
		return cmp_lt;
	}
}

int
compare2range( int l, int r )
{
	if (l == cmp_gt) {
		if (r == cmp_lt)
			return 0;
		else if (r == cmp_lte)
			return 2;
	} else if (l == cmp_gte) {
		if (r == cmp_lt)
			return 1;
		else if (r == cmp_lte)
			return 3;
	}
	return -1;
}

int
compare_funcs2range(const char *l_op, const char *r_op)
{
	assert(l_op[0] == '>' && r_op[0] == '<');
	if (!l_op[1] && !r_op[1])
		return 0;
	if (!l_op[1] && r_op[1] == '=')
		return 2;
	if (l_op[1] == '=' && !r_op[1])
		return 1;
	if (l_op[1] == '=' && r_op[1] == '=')
		return 3;
	assert(0);
	return 0;
}

static sql_exp *
exp_create(allocator *sa, int type)
{
	sql_exp *e = SA_NEW(sa, sql_exp);

	if (!e)
		return NULL;
	*e = (sql_exp) {
		.type = (expression_type) type,
	};
	return e;
}

sql_exp *
exp_compare(allocator *sa, sql_exp *l, sql_exp *r, int cmptype)
{
	sql_exp *e = exp_create(sa, e_cmp);
	if (e == NULL)
		return NULL;
	e->card = MAX(l->card,r->card);
	e->l = l;
	e->r = r;
	e->flag = cmptype;
	if (!has_nil(l) && !has_nil(r))
		set_has_no_nil(e);
	return e;
}

sql_exp *
exp_compare2(allocator *sa, sql_exp *l, sql_exp *r, sql_exp *f, int cmptype, int symmetric)
{
	sql_exp *e = exp_create(sa, e_cmp);
	if (e == NULL)
		return NULL;
	assert(f);
	e->card = MAX(MAX(l->card,r->card),f->card);
	e->l = l;
	e->r = r;
	e->f = f;
	e->flag = cmptype;
	if (symmetric)
		set_symmetric(e);
	if (!has_nil(l) && !has_nil(r) && !has_nil(f))
		set_has_no_nil(e);
	return e;
}

sql_exp *
exp_filter(allocator *sa, list *l, list *r, sql_subfunc *f, int anti)
{
	sql_exp *e = exp_create(sa, e_cmp);

	if (e == NULL)
		return NULL;
	e->card = MAX(exps_card(l),exps_card(r));
	if (!r) { /* split l */
		list *nl = sa_list(sa), *nr = sa_list(sa);
		node *n = l->h;
		append(nl, n->data); /* sofar only first is left */
		for(n = n->next; n; n = n->next)
			append(nr, n->data);
		l = nl;
		r = nr;
	}
	e->l = l;
	e->r = r;
	e->f = f;
	e->flag = cmp_filter;
	if (anti)
		set_anti(e);
	if (!have_nil(l) && !have_nil(r))
		set_has_no_nil(e);
	return e;
}

sql_exp *
exp_in(allocator *sa, sql_exp *l, list *r, int cmptype)
{
	sql_exp *e = exp_create(sa, e_cmp);
	unsigned int exps_card = CARD_ATOM;

	if (e == NULL)
		return NULL;

	/* ignore the cardinalites of sub-relations */
	for (node *n = r->h; n ; n = n->next) {
		sql_exp *next = n->data;

		if (!exp_is_rel(next) && exps_card < next->card)
			exps_card = next->card;
	}
	e->card = MAX(l->card, exps_card);
	e->l = l;
	e->r = r;
	assert( cmptype == cmp_in || cmptype == cmp_notin);
	e->flag = cmptype;
	if (!has_nil(l) && !have_nil(r))
		set_has_no_nil(e);
	return e;
}

sql_exp *
exp_in_func(mvc *sql, sql_exp *le, sql_exp *vals, int anyequal, int is_tuple)
{
	sql_subfunc *a_func = NULL;
	sql_exp *e = le;

	if (is_tuple) {
		list *l = exp_get_values(e);
		e = l->h->data;
	}
	if (!(a_func = sql_bind_func(sql, "sys", anyequal ? "sql_anyequal" : "sql_not_anyequal", exp_subtype(e), exp_subtype(e), F_FUNC, true, true)))
		return sql_error(sql, 02, SQLSTATE(42000) "(NOT) IN operator on type %s missing", exp_subtype(e) ? exp_subtype(e)->type->base.name : "unknown");
	e = exp_binop(sql->sa, le, vals, a_func);
	if (e) {
		unsigned int exps_card = CARD_ATOM;

		/* ignore the cardinalites of sub-relations */
		if (vals->type == e_atom && vals->f) {
			for (node *n = ((list*)vals->f)->h ; n ; n = n->next) {
				sql_exp *next = n->data;

				if (!exp_is_rel(next) && exps_card < next->card)
					exps_card = next->card;
			}
		} else if (!exp_is_rel(vals))
			exps_card = vals->card;

		e->card = MAX(le->card, exps_card);
		if (!has_nil(le) && !has_nil(vals))
			set_has_no_nil(e);
	}
	return e;
}

sql_exp *
exp_in_aggr(mvc *sql, sql_exp *le, sql_exp *vals, int anyequal, int is_tuple)
{
	sql_subfunc *a_func = NULL;
	sql_exp *e = le;

	if (is_tuple) {
		list *l = exp_get_values(e);
		e = l->h->data;
	}
	if (!(a_func = sql_bind_func(sql, "sys", anyequal ? "anyequal" : "allnotequal", exp_subtype(e), exp_subtype(e), F_AGGR, true, true)))
		return sql_error(sql, 02, SQLSTATE(42000) "(NOT) IN operator on type %s missing", exp_subtype(e) ? exp_subtype(e)->type->base.name : "unknown");
	e = exp_aggr2(sql->sa, le, vals, a_func, need_distinct(e), need_no_nil(e), e->card, has_nil(e));
	if (e) {
		unsigned int exps_card = CARD_ATOM;

		/* ignore the cardinalites of sub-relations */
		if (vals->type == e_atom && vals->f) {
			for (node *n = ((list*)vals->f)->h ; n ; n = n->next) {
				sql_exp *next = n->data;

				if (!exp_is_rel(next) && exps_card < next->card)
					exps_card = next->card;
			}
		} else if (!exp_is_rel(vals))
			exps_card = vals->card;

		e->card = MAX(le->card, exps_card);
		if (!has_nil(le) && !has_nil(vals))
			set_has_no_nil(e);
	}
	return e;
}

sql_exp *
exp_compare_func(mvc *sql, sql_exp *le, sql_exp *re, const char *compareop, int quantifier)
{
	sql_subfunc *cmp_func = sql_bind_func(sql, "sys", compareop, exp_subtype(le), exp_subtype(le), F_FUNC, true, true);
	sql_exp *e = NULL;

	if (cmp_func == NULL)
		return NULL;

	e = exp_binop(sql->sa, le, re, cmp_func);
	if (e) {
		e->flag = quantifier;
		/* At ANY and ALL operators, the cardinality on the right side is ignored if it is a sub-relation */
		e->card = quantifier && exp_is_rel(re) ? le->card : MAX(le->card, re->card);
		if (!has_nil(le) && !has_nil(re))
			set_has_no_nil(e);
	}
	return e;
}

sql_exp *
exp_conjunctive(allocator *sa, list *exps)
{
	sql_exp *e = exp_create(sa, e_cmp);

	if (e == NULL)
		return NULL;

	e->card = exps_card(exps);
	e->l = exps;
	e->flag = cmp_con;
	return e;
}

sql_exp *
exp_disjunctive(allocator *sa, list *exps)
{
	sql_exp *e = exp_create(sa, e_cmp);

	if (e == NULL)
		return NULL;

	e->card = exps_card(exps);
	e->l = exps;
	e->flag = cmp_dis;
	return e;
}

sql_exp *
exp_disjunctive2(allocator *sa, sql_exp *e1, sql_exp *e2)
{
	if (e1->type == e_cmp && e1->flag == cmp_dis && !is_anti(e1)) {
		append(e1->l, e2);
		return e1;
	}
	return exp_disjunctive(sa, append(append(sa_list(sa), e1), e2));
}

static sql_subtype*
dup_subtype(allocator *sa, sql_subtype *st)
{
	sql_subtype *res = SA_NEW(sa, sql_subtype);

	if (res == NULL)
		return NULL;
	*res = *st;
	return res;
}

sql_exp *
exp_convert(mvc *sql, sql_exp *exp, sql_subtype *fromtype, sql_subtype *totype )
{
	sql_exp *e = exp_create(sql->sa, e_convert);
	if (e == NULL)
		return NULL;
	e->card = exp->card;
	e->l = exp;
	totype = dup_subtype(sql->sa, totype);
	e->r = append(append(sa_list(sql->sa), dup_subtype(sql->sa, fromtype)),totype);
	e->tpe = *totype;
	e->alias = exp->alias;
	if (e->alias.label)
		e->alias.label = -(sql->nid++);
	if (!has_nil(exp))
		set_has_no_nil(e);
	return e;
}

sql_exp *
exp_op( allocator *sa, list *l, sql_subfunc *f )
{
	if (f->func->type == F_FILT)
		return exp_filter(sa, l, NULL, f, false);
	sql_exp *e = exp_create(sa, e_func);
	if (e == NULL)
		return NULL;
	e->card = exps_card(l);
	e->l = l;
	e->f = f;
	e->semantics = f->func->semantics;
	if (!is_semantics(e) && !is_any(e) && l && !have_nil(l))
		set_has_no_nil(e);
	return e;
}

sql_exp *
exp_rank_op( allocator *sa, list *l, list *gbe, list *obe, sql_subfunc *f )
{
	sql_exp *e = exp_create(sa, e_func);
	if (e == NULL)
		return NULL;
	e->card = list_empty(l)?CARD_MULTI:exps_card(l);
	e->l = l;
	e->r = append(append(sa_list(sa), gbe), obe);
	e->f = f;
	if (!f->func->s && strcmp(f->func->base.name, "count") == 0)
		set_has_no_nil(e);
	e->semantics = f->func->semantics;
	return e;
}

sql_exp *
exp_aggr( allocator *sa, list *l, sql_subfunc *a, int distinct, int no_nils, unsigned int card, int has_nils )
{
	sql_exp *e = exp_create(sa, e_aggr);
	if (e == NULL)
		return NULL;
	e->card = card;
	e->l = l;
	e->f = a;
	e->semantics = a->func->semantics;
	if (distinct)
		set_distinct(e);
	if (no_nils)
		set_no_nil(e);
	if ((!a->func->semantics && !has_nils) || (!a->func->s && strcmp(a->func->base.name, "count") == 0))
		set_has_no_nil(e);
	return e;
}

sql_exp *
exp_atom(allocator *sa, atom *a)
{
	sql_exp *e = exp_create(sa, e_atom);
	if (e == NULL)
		return NULL;
	e->card = CARD_ATOM;
	e->tpe = a->tpe;
	e->l = a;
	if (!a->isnull)
		set_has_no_nil(e);
	return e;
}

sql_exp *
exp_atom_max(allocator *sa, sql_subtype *tpe)
{
	if (tpe->type->localtype == TYPE_bte) {
		return exp_atom_bte(sa, GDK_bte_max);
	} else if (tpe->type->localtype == TYPE_sht) {
		return exp_atom_sht(sa, GDK_sht_max);
	} else if (tpe->type->localtype == TYPE_int) {
		return exp_atom_int(sa, GDK_int_max);
	} else if (tpe->type->localtype == TYPE_lng) {
		return exp_atom_lng(sa, GDK_lng_max);
#ifdef HAVE_HGE
	} else if (tpe->type->localtype == TYPE_hge) {
		return exp_atom_hge(sa, GDK_hge_max);
#endif
	}
	return NULL;
}

sql_exp *
exp_atom_bool(allocator *sa, int b)
{
	sql_subtype bt;

	sql_find_subtype(&bt, "boolean", 0, 0);
	if (b)
		return exp_atom(sa, atom_bool(sa, &bt, TRUE ));
	else
		return exp_atom(sa, atom_bool(sa, &bt, FALSE ));
}

sql_exp *
exp_atom_bte(allocator *sa, bte i)
{
	sql_subtype it;

	sql_find_subtype(&it, "tinyint", 3, 0);
	return exp_atom(sa, atom_int(sa, &it, i ));
}

sql_exp *
exp_atom_sht(allocator *sa, sht i)
{
	sql_subtype it;

	sql_find_subtype(&it, "smallint", 5, 0);
	return exp_atom(sa, atom_int(sa, &it, i ));
}

sql_exp *
exp_atom_int(allocator *sa, int i)
{
	sql_subtype it;

	sql_find_subtype(&it, "int", 9, 0);
	return exp_atom(sa, atom_int(sa, &it, i ));
}

sql_exp *
exp_atom_lng(allocator *sa, lng i)
{
	sql_subtype it;

#ifdef HAVE_HGE
	sql_find_subtype(&it, "bigint", 18, 0);
#else
	sql_find_subtype(&it, "bigint", 19, 0);
#endif
	return exp_atom(sa, atom_int(sa, &it, i ));
}

sql_exp *
exp_atom_oid(allocator *sa, oid i)
{
	sql_subtype it;

#if SIZEOF_OID == SIZEOF_INT
	sql_find_subtype(&it, "oid", 31, 0);
#else
	sql_find_subtype(&it, "oid", 63, 0);
#endif
	return exp_atom(sa, atom_int(sa, &it, i ));
}

#ifdef HAVE_HGE
sql_exp *
exp_atom_hge(allocator *sa, hge i)
{
	sql_subtype it;

	sql_find_subtype(&it, "hugeint", 39, 0);
	return exp_atom(sa, atom_int(sa, &it, i ));
}
#endif

sql_exp *
exp_atom_flt(allocator *sa, flt f)
{
	sql_subtype it;

	sql_find_subtype(&it, "real", 24, 0);
	return exp_atom(sa, atom_float(sa, &it, (dbl)f ));
}

sql_exp *
exp_atom_dbl(allocator *sa, dbl f)
{
	sql_subtype it;

	sql_find_subtype(&it, "double", 53, 0);
	return exp_atom(sa, atom_float(sa, &it, (dbl)f ));
}

sql_exp *
exp_atom_str(allocator *sa, const char *s, sql_subtype *st)
{
	return exp_atom(sa, atom_string(sa, st, s?sa_strdup(sa, s):NULL));
}

sql_exp *
exp_atom_clob(allocator *sa, const char *s)
{
	sql_subtype clob;

	sql_find_subtype(&clob, "varchar", 0, 0);
	return exp_atom(sa, atom_string(sa, &clob, s?sa_strdup(sa, s):NULL));
}

sql_exp *
exp_atom_ptr(allocator *sa, void *s)
{
	sql_subtype *t = sql_fetch_localtype(TYPE_ptr);
	return exp_atom(sa, atom_ptr(sa, t, s));
}

sql_exp *
exp_atom_ref(allocator *sa, int i, sql_subtype *tpe)
{
	sql_exp *e = exp_create(sa, e_atom);
	if (e == NULL)
		return NULL;
	e->card = CARD_ATOM;
	e->flag = i;
	if (tpe)
		e->tpe = *tpe;
	return e;
}

sql_exp *
exp_null(allocator *sa, sql_subtype *tpe)
{
	atom *a = atom_general(sa, tpe, NULL, 0);
	return exp_atom(sa, a);
}

sql_exp *
exp_zero(allocator *sa, sql_subtype *tpe)
{
	atom *a = atom_zero_value(sa, tpe);
	return exp_atom(sa, a);
}

atom *
exp_value(mvc *sql, sql_exp *e)
{
	if (!e || e->type != e_atom)
		return NULL;
	if (e->l) { /* literal */
		return e->l;
	} else if (e->r) { /* param (ie not set) */
		sql_var_name *vname = (sql_var_name*) e->r;

		assert(e->flag != 0 || vname->sname); /* global variables must have a schema */
		sql_var *var = e->flag == 0 ? find_global_var(sql, mvc_bind_schema(sql, vname->sname), vname->name) :
									  stack_find_var_at_level(sql, vname->name, e->flag);
		if (var)
			return &(var->var);
	}
	return NULL;
}

sql_exp *
exp_param_or_declared(allocator *sa, const char *sname, const char *name, sql_subtype *tpe, int frame)
{
	sql_var_name *vname;
	sql_exp *e = exp_create(sa, e_atom);
	if (e == NULL)
		return NULL;

	e->r = sa_alloc(sa, sizeof(sql_var_name));
	vname = (sql_var_name*) e->r;
	vname->sname = sname;
	vname->name = name;
	e->card = CARD_ATOM;
	e->flag = frame;
	if (tpe)
		e->tpe = *tpe;
	return e;
}

sql_exp *
exp_values(allocator *sa, list *exps)
{
	sql_exp *e = exp_create(sa, e_atom);
	if (e == NULL)
		return NULL;
	e->card = exps_card(exps);
	e->f = exps;
	return e;
}

list *
exp_get_values(sql_exp *e)
{
	if (is_atom(e->type) && e->f)
		return e->f;
	return NULL;
}

list *
exp_types(allocator *sa, list *exps)
{
	list *l = sa_list(sa);

	if (exps)
		for (node *n = exps->h; n; n = n->next)
			list_append(l, exp_subtype(n->data));
	return l;
}

int
have_nil(list *exps)
{
	int has_nil = 0;

	if (exps)
		for (node *n = exps->h; n && !has_nil; n = n->next) {
			sql_exp *e = n->data;
			has_nil |= has_nil(e);
		}
	return has_nil;
}

int
have_semantics(list *exps)
{
	int has_semantics = 0;

	if (exps)
		for (node *n = exps->h; n && !has_semantics; n = n->next) {
			sql_exp *e = n->data;
			has_semantics |= is_compare(e->type) && is_semantics(e);
		}
	return has_semantics;
}

sql_exp *
exp_column(allocator *sa, const char *rname, const char *cname, sql_subtype *t, unsigned int card, int has_nils, int unique, int intern)
{
	sql_exp *e = exp_create(sa, e_column);

	if (e == NULL)
		return NULL;
	assert(cname);
	e->card = card;
	e->alias.name = cname;
	e->alias.rname = rname;
	e->r = (char*)e->alias.name;
	e->l = (char*)e->alias.rname;
	if (t)
		e->tpe = *t;
	if (!has_nils)
		set_has_no_nil(e);
	if (unique)
		set_unique(e);
	if (intern)
		set_intern(e);
	return e;
}

sql_exp *
exp_propagate(allocator *sa, sql_exp *ne, sql_exp *oe)
{
	if (has_label(oe) &&
	   (oe->alias.rname == ne->alias.rname || (oe->alias.rname && ne->alias.rname && strcmp(oe->alias.rname, ne->alias.rname) == 0)) &&
	   (oe->alias.name == ne->alias.name || (oe->alias.name && ne->alias.name && strcmp(oe->alias.name, ne->alias.name) == 0)))
		ne->alias.label = oe->alias.label;
	if (is_intern(oe))
		set_intern(ne);
	if (is_anti(oe))
		set_anti(ne);
	if (is_semantics(oe))
		set_semantics(ne);
	if (is_any(oe))
		set_any(ne);
	if (is_symmetric(oe))
		set_symmetric(ne);
	if (is_partitioning(oe))
		set_partitioning(ne);
	if (is_ascending(oe))
		set_ascending(ne);
	if (nulls_last(oe))
		set_nulls_last(ne);
	if (need_distinct(oe))
		set_distinct(ne);
	if (zero_if_empty(oe))
		set_zero_if_empty(ne);
	if (need_no_nil(oe))
		set_no_nil(ne);
	if (!has_nil(oe))
		set_has_no_nil(ne);
	if (has_nil(oe))
		set_has_nil(ne);
	/*
	if (is_unique(oe))
		set_unique(ne);
		*/
	if (is_basecol(oe))
		set_basecol(ne);
	ne->p = prop_copy(sa, oe->p);
	return ne;
}

static sql_exp *
exp_ref_by_label(allocator *sa, sql_exp *o)
{
	sql_exp *e = exp_create(sa, e_column);

	if (e == NULL)
		return NULL;
	e->card = o->card;
	e->alias = o->alias;
	assert(e->alias.label);
	e->r = (char*)e->alias.name;
	e->l = (char*)e->alias.rname;
	e->nid = o->alias.label;
	assert(e->nid);
	sql_subtype *t = exp_subtype(o);
	if (t)
		e->tpe = *t;
	if (!has_nil(o))
		set_has_no_nil(e);
	if (has_nil(o))
		set_has_nil(e);
	/*
	if (is_unique(o))
		set_unique(e);
		*/
	if (is_intern(o))
		set_intern(e);
	return exp_propagate(sa, e, o);
}

sql_exp *
exp_ref(mvc *sql, sql_exp *e)
{
	if (!has_label(e) && !exp_name(e))
		exp_label(sql->sa, e, ++sql->label);
	if (e->alias.label)
		return exp_ref_by_label(sql->sa, e);
	sql_exp *ne = exp_propagate(sql->sa, exp_column(sql->sa, exp_relname(e), exp_name(e), exp_subtype(e), exp_card(e), has_nil(e), is_unique(e), is_intern(e)), e);
	if (ne) {
		ne->nid = e->alias.label;
		ne->alias.label = ne->nid;
	}
	return ne;
}

sql_exp *
exp_ref_save(mvc *sql, sql_exp *e)
{
	if (e->type == e_column)
		return e;
	if (is_atom(e->type))
		return exp_copy(sql, e);
	if (!e->alias.label || !exp_name(e))
		exp_label(sql->sa, e, ++sql->label);
	if (e->type != e_column) /* ref as referenced within the (same) rank expression */
		e->ref = 1;
	sql_exp *ne = exp_ref(sql, e);
	if (ne && is_freevar(e))
		set_freevar(ne, is_freevar(e)-1);
	return ne;
}

sql_exp *
exp_alias(mvc *sql, const char *arname, const char *acname, const char *org_rname, const char *org_cname, sql_subtype *t, unsigned int card, int has_nils, int unique, int intern)
{
	sql_exp *e = exp_column(sql->sa, org_rname, org_cname, t, card, has_nils, unique, intern);

	if (e == NULL)
		return NULL;
	assert(acname && org_cname);
	exp_setname(sql, e, (arname)?arname:org_rname, acname);
	return e;
}

sql_exp *
exp_alias_ref(mvc *sql, sql_exp *e)
{
	const char *tname = exp_relname(e);
	const char *cname = exp_name(e);

	if (!has_label(e))
		exp_label(sql->sa, e, ++sql->label);
	sql_exp *ne = exp_ref(sql, e);
	if (ne == NULL)
		return NULL;
	exp_setname(sql, ne, tname, cname);
	return ne;
}

sql_exp *
exp_set(allocator *sa, const char *sname, const char *name, sql_exp *val, int level)
{
	sql_exp *e = exp_create(sa, e_psm);

	if (e == NULL)
		return NULL;
	e->alias.rname = sname;
	e->alias.name = name;
	e->l = val;
	e->flag = PSM_SET + SET_PSM_LEVEL(level);
	return e;
}

sql_exp *
exp_var(allocator *sa, const char *sname, const char *name, sql_subtype *type, int level)
{
	sql_exp *e = exp_create(sa, e_psm);

	if (e == NULL)
		return NULL;
	e->alias.rname = sname;
	e->alias.name = name;
	e->tpe = *type;
	e->flag = PSM_VAR + SET_PSM_LEVEL(level);
	return e;
}

sql_exp *
exp_table(allocator *sa, const char *name, sql_table *t, int level)
{
	sql_exp *e = exp_create(sa, e_psm);

	if (e == NULL)
		return NULL;
	e->alias.rname = NULL;
	e->alias.name = name;
	e->f = t;
	e->flag = PSM_VAR + SET_PSM_LEVEL(level);
	return e;
}

sql_exp *
exp_return(allocator *sa, sql_exp *val, int level)
{
	sql_exp *e = exp_create(sa, e_psm);

	if (e == NULL)
		return NULL;
	e->l = val;
	e->flag = PSM_RETURN + SET_PSM_LEVEL(level);
	return e;
}

sql_exp *
exp_while(allocator *sa, sql_exp *cond, list *stmts)
{
	sql_exp *e = exp_create(sa, e_psm);

	if (e == NULL)
		return NULL;
	e->l = cond;
	e->r = stmts;
	e->flag = PSM_WHILE;
	return e;
}

sql_exp *
exp_if(allocator *sa, sql_exp *cond, list *if_stmts, list *else_stmts)
{
	sql_exp *e = exp_create(sa, e_psm);

	if (e == NULL)
		return NULL;
	e->l = cond;
	e->r = if_stmts;
	e->f = else_stmts;
	e->flag = PSM_IF;
	return e;
}

sql_exp *
exp_rel(mvc *sql, sql_rel *rel)
{
	sql_exp *e = exp_create(sql->sa, e_psm);

	if (e == NULL)
		return NULL;
	e->l = rel;
	e->flag = PSM_REL;
	e->card = is_single(rel)?CARD_ATOM:rel->card;
	assert(rel);
	if (is_topn(rel->op))
		rel = rel->l;
	if (is_project(rel->op)) {
		sql_exp *last = rel->exps->t->data;
		sql_subtype *t = exp_subtype(last);
		e->tpe = t ? *t : (sql_subtype) {0};
	}
	return e;
}

sql_exp *
exp_exception(allocator *sa, sql_exp *cond, const char *error_message)
{
	sql_exp *e = exp_create(sa, e_psm);

	if (e == NULL)
		return NULL;
	e->l = cond;
	e->r = sa_strdup(sa, error_message);
	e->flag = PSM_EXCEPTION;
	return e;
}

/* Set a name (alias) for the expression, such that we can refer
   to this expression by this simple name.
 */
void
exp_setname(mvc *sql, sql_exp *e, const char *rname, const char *name )
{
	assert(name || rname);
	e->alias.label = -(sql->nid++);
	//e->alias.label = 0;
	if (name)
		e->alias.name = name;
	e->alias.rname = (rname);
}

void
noninternexp_setname(mvc *sql, sql_exp *e, const char *rname, const char *name )
{
	if (!is_intern(e))
		exp_setname(sql, e, rname, name);
}

void
exp_setalias(sql_exp *e, int label, const char *rname, const char *name )
{
	e->alias.label = label;
	assert(e->alias.label);
	e->alias.name = name;
	e->alias.rname = rname;
}

void
exp_prop_alias(allocator *sa, sql_exp *e, sql_exp *oe )
{
	e->ref = oe->ref;
	if (oe->alias.name == NULL && exp_has_rel(oe)) {
		sql_rel *r = exp_rel_get_rel(sa, oe);
		if (!is_project(r->op))
			return ;
		oe = r->exps->t->data;
	}
	e->alias = oe->alias;
}

str
number2name(str s, int len, int i)
{
	s[--len] = 0;
	while(i>0) {
		s[--len] = '0' + (i & 7);
		i >>= 3;
	}
	s[--len] = '%';
	return s + len;
}

void
exp_setrelname(allocator *sa, sql_exp *e, int nr)
{
	char name[16], *nme;

	nme = number2name(name, sizeof(name), nr);
	e->alias.label = 0;
	e->alias.rname = sa_strdup(sa, nme);
}

char *
make_label(allocator *sa, int nr)
{
	char name[16], *nme;

	nme = number2name(name, sizeof(name), nr);
	return sa_strdup(sa, nme);
}

sql_exp*
exp_label(allocator *sa, sql_exp *e, int nr)
{
	assert(nr > 0);
	//assert (e->alias.label == 0);
	e->alias.label = nr;
	e->alias.rname = e->alias.name = make_label(sa, nr);
	return e;
}

list*
exps_label(mvc *sql, list *exps)
{
	if (!exps)
		return NULL;

	int nr = sql->label+1;
	sql->label += list_length(exps);
	for (node *n = exps->h; n; n = n->next)
		n->data = exp_label(sql->sa, n->data, nr++);
	list_hash_clear(exps);
	return exps;
}

void
exp_swap( sql_exp *e )
{
	sql_exp *s = e->l;

	e->l = e->r;
	e->r = s;
	e->flag = swap_compare((comp_type)e->flag);
	assert(!e->f);
}

sql_subtype *
exp_subtype( sql_exp *e )
{
	switch(e->type) {
	case e_atom: {
		if (e->l) {
			atom *a = e->l;
			return atom_type(a);
		} else if (e->tpe.type) { /* atom reference */
			return &e->tpe;
		} else if (e->f) {
			list *vals = exp_get_values(e);
			if (!list_empty(vals))
				return exp_subtype(vals->h->data);
		}
		break;
	}
	case e_convert:
	case e_column:
		if (e->tpe.type)
			return &e->tpe;
		break;
	case e_aggr:
	case e_func: {
		if (e->f) {
			sql_subfunc *f = e->f;
			if (f->res && list_length(f->res) == 1)
				return f->res->h->data;
		}
		return NULL;
	}
	case e_cmp:
		return sql_fetch_localtype(TYPE_bit);
	case e_psm:
		if (e->tpe.type)
			return &e->tpe;
		/* fall through */
	default:
		return NULL;
	}
	return NULL;
}

const char *
exp_name( sql_exp *e )
{
	if (e->alias.name)
		return e->alias.name;
	if (e->type == e_convert && e->l)
		return exp_name(e->l);
	if (e->type == e_psm && e->l) { /* subquery return name of last expression */
		sql_rel *r = e->l;
		if (is_project(r->op))
			return exp_name(r->exps->t->data);
	}
	return NULL;
}

const char *
exp_relname( sql_exp *e )
{
	if (e->alias.rname)
		return e->alias.rname;
	if (!e->alias.name && e->type == e_convert && e->l)
		return exp_relname(e->l);
	if (!e->alias.name && e->type == e_psm && e->l) { /* subquery return name of last expression */
		sql_rel *r = e->l;
		if (is_project(r->op))
			return exp_relname(r->exps->t->data);
	}
	return NULL;
}

const char *
exp_find_rel_name(sql_exp *e)
{
	if (e->alias.rname)
		return e->alias.rname;
	switch(e->type) {
	case e_column:
		break;
	case e_convert:
		return exp_find_rel_name(e->l);
	default:
		return NULL;
	}
	return NULL;
}

unsigned int
exp_card( sql_exp *e )
{
	return e->card;
}

unsigned int
exp_get_label( sql_exp *e )
{
	if (e->alias.label)
		return e->alias.label;
	if (e->type == e_convert && e->l)
		return exp_get_label(e->l);
	if (e->type == e_psm && e->l) { /* subquery return name of last expression */
		sql_rel *r = e->l;
		if (is_project(r->op))
			return exp_get_label(r->exps->t->data);
	}
	return 0;
}


const char *
exp_func_name( sql_exp *e )
{
	if (e->type == e_func && e->f) {
		sql_subfunc *f = e->f;
		return f->func->base.name;
	}
	if (e->alias.name)
		return e->alias.name;
	if (e->type == e_convert && e->l)
		return exp_name(e->l);
	return NULL;
}

int
exp_cmp( sql_exp *e1, sql_exp *e2)
{
	return (e1 == e2)?0:-1;
}

int
exp_equal( sql_exp *e1, sql_exp *e2)
{
	if (e1 == e2)
		return 0;
	if (e1->alias.label && e1->alias.label == e2->alias.label)
		return 0;
	return -1;
}

int
is_conflict( sql_exp *e1, sql_exp *e2)
{
	if (e1->alias.label && e1->alias.label == e2->alias.label &&
		e1->nid && e1->nid != e2->nid)
		return 0;
	return -1;
}

int
exp_match( sql_exp *e1, sql_exp *e2)
{
	if (exp_cmp(e1, e2) == 0)
		return 1;
	if (e1->type == e2->type && e1->type == e_column) {
		if (e1->nid && e1->nid == e2->nid)
			return 1;
		if (e1->alias.label != e2->alias.label || !e1->alias.label || !e2->alias.label)
			return 0;
		return 1;
	}
	if (e1->type == e2->type && e1->type == e_func) {
		if (is_identity(e1, NULL) && is_identity(e2, NULL)) {
			list *args1 = e1->l;
			list *args2 = e2->l;

			if (list_length(args1) == list_length(args2) && list_length(args1) == 1) {
				sql_exp *ne1 = args1->h->data;
				sql_exp *ne2 = args2->h->data;

				if (exp_match(ne1,ne2))
					return 1;
			}
		}
	}
	return 0;
}

/* list already contains matching expression */
sql_exp*
exps_find_exp( list *l, sql_exp *e)
{
	node *n;

	if (!l || !l->h)
		return NULL;

	for(n=l->h; n; n = n->next) {
		if (exp_match(n->data, e) || exp_refers(n->data, e))
			return n->data;
	}
	return NULL;
}

/* c refers to the parent p */
int
exp_refers( sql_exp *p, sql_exp *c)
{
	if (c->type == e_column && c->nid)
		return c->nid == p->alias.label;
	return 0;
}

sql_exp*
exps_refers(sql_exp *p, list *l)
{
	node *n;

	if (!l || !l->h)
		return NULL;

	for(n=l->h; n; n = n->next) {
		if (exp_refers(p, n->data))
			return n->data;
	}
	return NULL;
}

int
exp_match_col_exps( sql_exp *e, list *l)
{
	node *n;

	for(n=l->h; n; n = n->next) {
		sql_exp *re = n->data;
		sql_exp *re_r = re->r;

		/* TODO handle nested conjunctive/disjunctive */
		if (re->type != e_cmp || !re_r || re_r->card != 1 || !exp_match_exp(e, re->l))
			return 0;
	}
	return 1;
}

int
exps_match_col_exps( sql_exp *e1, sql_exp *e2)
{
	sql_exp *e1_r = e1->r;
	sql_exp *e2_r = e2->r;

	if (e1->type != e_cmp || e2->type != e_cmp)
		return 0;

	if (!is_complex_exp(e1->flag) && e1_r && e1_r->card == CARD_ATOM &&
	    !is_complex_exp(e2->flag) && e2_r && e2_r->card == CARD_ATOM)
		return exp_match_exp(e1->l, e2->l);

	if (!is_complex_exp(e1->flag) && e1_r && e1_r->card == CARD_ATOM &&
	    (e2->flag == cmp_in || e2->flag == cmp_notin))
		return exp_match_exp(e1->l, e2->l);
	if ((e1->flag == cmp_in || e1->flag == cmp_notin) &&
	    !is_complex_exp(e2->flag) && e2_r && e2_r->card == CARD_ATOM)
		return exp_match_exp(e1->l, e2->l);

	if ((e1->flag == cmp_in || e1->flag == cmp_notin) &&
	    (e2->flag == cmp_in || e2->flag == cmp_notin))
		return exp_match_exp(e1->l, e2->l);
	return 0;
}

int
exp_match_list( list *l, list *r)
{
	node *n, *m;
	char *lu, *ru;
	int lc = 0, rc = 0, match = 0;

	if (!l || !r)
		return l == r;
	if (list_length(l) != list_length(r) || list_length(l) == 0 || list_length(r) == 0)
		return 0;
	if (list_length(l) > 10 || list_length(r) > 10)
		return 0;/* to expensive */

	lu = ZNEW_ARRAY(char, list_length(l));
	ru = ZNEW_ARRAY(char, list_length(r));
	if (!lu || !ru) {
		_DELETE(lu);
		_DELETE(ru);
		return 0;
	}
	for (n = l->h, lc = 0; n; n = n->next, lc++) {
		sql_exp *le = n->data;

		for ( m = r->h, rc = 0; m; m = m->next, rc++) {
			sql_exp *re = m->data;

			if (!ru[rc] && exp_match_exp(le,re)) {
				lu[lc] = 1;
				ru[rc] = 1;
				match = 1;
			}
		}
	}
	for (n = l->h, lc = 0; n && match; n = n->next, lc++)
		if (!lu[lc])
			match = 0;
	for (n = r->h, rc = 0; n && match; n = n->next, rc++)
		if (!ru[rc])
			match = 0;
	_DELETE(lu);
	_DELETE(ru);
	return match;
}

static int
exps_equal( list *l, list *r)
{
	node *n, *m;

	if (!l || !r)
		return l == r;
	if (list_length(l) != list_length(r))
		return 0;
	for (n = l->h, m = r->h; n && m; n = n->next, m = m->next) {
		sql_exp *le = n->data, *re = m->data;

		if (!exp_match_exp(le,re))
			return 0;
	}
	return 1;
}

int
exp_match_exp_semantics( sql_exp *e1, sql_exp *e2, bool semantics)
{
	if (exp_match(e1, e2))
		return 1;

	if (is_ascending(e1) != is_ascending(e2) ||
		nulls_last(e1) != nulls_last(e2) ||
		zero_if_empty(e1) != zero_if_empty(e2) ||
		need_no_nil(e1) != need_no_nil(e2) ||
		is_anti(e1) != is_anti(e2) ||
		(semantics && is_semantics(e1) != is_semantics(e2)) ||
		(semantics && is_any(e1) != is_any(e2)) ||
		is_symmetric(e1) != is_symmetric(e2) ||
		is_unique(e1) != is_unique(e2) ||
		need_distinct(e1) != need_distinct(e2))
		return 0;

	if (e1->type == e2->type) {
		switch(e1->type) {
		case e_cmp:
			if (e1->flag == e2->flag && !is_complex_exp(e1->flag) &&
			    exp_match_exp(e1->l, e2->l) && exp_match_exp(e1->r, e2->r) &&
			    ((!e1->f && !e2->f) || (e1->f && e2->f && exp_match_exp(e1->f, e2->f))))
				return 1;
			else if (e1->flag == e2->flag &&
				(e1->flag == cmp_con || e1->flag == cmp_dis) &&
			    exp_match_list(e1->l, e2->l))
				return 1;
			else if (e1->flag == e2->flag &&
				(e1->flag == cmp_in || e1->flag == cmp_notin) &&
			    exp_match_exp(e1->l, e2->l) && exp_match_list(e1->r, e2->r))
				return 1;
			else if (e1->flag == e2->flag && (e1->flag == cmp_equal || e1->flag == cmp_notequal) &&
				exp_match_exp(e1->l, e2->r) && exp_match_exp(e1->r, e2->l))
				return 1; /* = and <> operations are reflective, so exp_match_exp can be called crossed */
			break;
		case e_convert:
			if (!subtype_cmp(exp_totype(e1), exp_totype(e2)) &&
			    !subtype_cmp(exp_fromtype(e1), exp_fromtype(e2)) &&
			    exp_match_exp(e1->l, e2->l))
				return 1;
			break;
		case e_aggr:
			if (!subfunc_cmp(e1->f, e2->f) && /* equal aggregation*/
			    exps_equal(e1->l, e2->l))
				return 1;
			break;
		case e_func: {
			sql_subfunc *e1f = (sql_subfunc*) e1->f;
			const char *sname = e1f->func->s ? e1f->func->s->base.name : NULL;
			int (*comp)(list*, list*) = is_commutative(sname, e1f->func->base.name) ? exp_match_list : exps_equal;

			if (!e1f->func->side_effect &&
				!subfunc_cmp(e1f, e2->f) && /* equal functions */
				comp(e1->l, e2->l) &&
				/* optional order by expressions */
				exps_equal(e1->r, e2->r))
					return 1;
			} break;
		case e_atom:
			if (e1->l && e2->l && !atom_cmp(e1->l, e2->l))
				return 1;
			if (e1->f && e2->f && exps_equal(e1->f, e2->f))
				return 1;
			if (e1->r && e2->r && e1->flag == e2->flag && !subtype_cmp(&e1->tpe, &e2->tpe)) {
				sql_var_name *v1 = (sql_var_name*) e1->r, *v2 = (sql_var_name*) e2->r;
				if (((!v1->sname && !v2->sname) || (v1->sname && v2->sname && strcmp(v1->sname, v2->sname) == 0)) &&
					((!v1->name && !v2->name) || (v1->name && v2->name && strcmp(v1->name, v2->name) == 0)))
					return 1;
			}
			if (!e1->l && !e1->r && !e1->f && !e2->l && !e2->r && !e2->f && e1->flag == e2->flag && !subtype_cmp(&e1->tpe, &e2->tpe))
				return 1;
			break;
		default:
			break;
		}
	}
	return 0;
}

int
exp_match_exp( sql_exp *e1, sql_exp *e2)
{
	return exp_match_exp_semantics( e1, e2, true);
}

sql_exp *
exps_any_match(list *l, sql_exp *e)
{
	if (!l)
		return NULL;
	for (node *n = l->h; n ; n = n->next) {
		sql_exp *ne = (sql_exp *) n->data;
		if (exp_match_exp(ne, e))
			return ne;
	}
	return NULL;
}

int
exp_is_join_exp(sql_exp *e)
{
	if (exp_is_join(e, NULL) == 0)
		return 0;
	return -1;
}

static int
exp_is_complex_select( sql_exp *e )
{
	switch (e->type) {
	case e_atom: {
		if (e->f) {
			int r = (e->card == CARD_ATOM);
			list *l = e->f;

			if (r)
				for (node *n = l->h; n && !r; n = n->next)
					r |= exp_is_complex_select(n->data);
			return r;
		}
		return 0;
	}
	case e_convert:
		return exp_is_complex_select(e->l);
	case e_func:
	case e_aggr:
	{
		int r = (e->card == CARD_ATOM);
		list *l = e->l;

		if (r && l)
			for (node *n = l->h; n && !r; n = n->next)
				r |= exp_is_complex_select(n->data);
		return r;
	}
	case e_psm:
		return 1;
	case e_column:
	case e_cmp:
	default:
		return 0;
	}
}

static int
complex_select(sql_exp *e)
{
	sql_exp *l = e->l, *r = e->r;

	if (exp_is_complex_select(l) || exp_is_complex_select(r))
		return 1;
	return 0;
}

static int
distinct_rel(sql_exp *e, const char **rname)
{
	const char *e_rname = NULL;

	switch(e->type) {
	case e_column:
		e_rname = exp_relname(e);

		if (*rname && e_rname && strcmp(*rname, e_rname) == 0)
			return 1;
		if (!*rname) {
			*rname = e_rname;
			return 1;
		}
		break;
	case e_aggr:
	case e_func:
		if (e->l) {
			int m = 1;
			list *l = e->l;
			node *n;

			for(n=l->h; n && m; n = n->next) {
				sql_exp *ae = n->data;

				m = distinct_rel(ae, rname);
			}
			return m;
		}
		return 0;
	case e_atom:
		return 1;
	case e_convert:
		return distinct_rel(e->l, rname);
	default:
		return 0;
	}
	return 0;
}

int
rel_has_exp(sql_rel *rel, sql_exp *e, bool subexp)
{
	if (rel_find_exp_and_corresponding_rel(rel, e, subexp, NULL, NULL))
		return 0;
	return -1;
}

int
rel_has_exps(sql_rel *rel, list *exps, bool subexp)
{
	if (list_empty(exps))
		return 0;
	for (node *n = exps->h; n; n = n->next)
		if (rel_has_exp(rel, n->data, subexp) >= 0)
			return 0;
	return -1;
}

int
rel_has_all_exps(sql_rel *rel, list *exps)
{
	if (list_empty(exps))
		return 1;
	for (node *n = exps->h; n; n = n->next)
		if (rel_has_exp(rel, n->data, false) < 0)
			return 0;
	return 1;
}

static int
rel_has_exp2(sql_rel *rel, sql_exp *e)
{
	return rel_has_exp(rel, e, false);
}

sql_rel *
find_rel(list *rels, sql_exp *e)
{
	node *n = list_find(rels, e, (fcmp)&rel_has_exp2);
	if (n)
		return n->data;
	return NULL;
}

sql_rel *
find_one_rel(list *rels, sql_exp *e)
{
	node *n;
	sql_rel *fnd = NULL;

	for(n = rels->h; n; n = n->next) {
		if (rel_has_exp(n->data, e, false) == 0) {
			if (fnd)
				return NULL;
			fnd = n->data;
		}
	}
	return fnd;
}

static int
exp_is_rangejoin(sql_exp *e, list *rels)
{
	/* assume e is a e_cmp with 3 args
	 * Need to check e->r and e->f only touch one table.
	 */
	const char *rname = 0;

	if (distinct_rel(e->r, &rname) && distinct_rel(e->f, &rname))
		return 0;
	if (rels) {
		sql_rel *r = find_rel(rels, e->r);
		sql_rel *f = find_rel(rels, e->f);
		if (r && f && r == f)
			return 0;
	}
	return -1;
}

int
exp_is_join(sql_exp *e, list *rels)
{
	/* only simple compare expressions, ie not or lists
		or range expressions (e->f)
	 */
	if (e->type == e_cmp && !is_complex_exp(e->flag) && e->l && e->r && !e->f && e->card >= CARD_AGGR && !complex_select(e))
		return 0;
	if (e->type == e_cmp && e->flag == cmp_filter && e->l && e->r && e->card >= CARD_AGGR)
		return 0;
	/* range expression */
	if (e->type == e_cmp && !is_complex_exp(e->flag) && e->l && e->r && e->f && e->card >= CARD_AGGR && !complex_select(e))
		return exp_is_rangejoin(e, rels);
	return -1;
}

int
exp_is_eqjoin(sql_exp *e)
{
	if (e->flag == cmp_equal) {
		sql_exp *l = e->l;
		sql_exp *r = e->r;

		if (!is_func(l->type) && !is_func(r->type))
			return 0;
	}
	return -1;
}

sql_exp *
exps_find_prop(list *exps, rel_prop kind)
{
	if (list_empty(exps))
		return NULL;
	for (node *n = exps->h ; n ; n = n->next) {
		sql_exp *e = n->data;

		if (find_prop(e->p, kind))
			return e;
	}
	return NULL;
}

/* check is one of the exps can be found in this relation */
static sql_exp* rel_find_exp_and_corresponding_rel_(sql_rel *rel, sql_exp *e, bool subexp, sql_rel **res);

static bool
rel_find_exps_and_corresponding_rel_(sql_rel *rel, list *l, bool subexp, sql_rel **res)
{
	int all = 1;

	if (list_empty(l))
		return true;
	for(node *n = l->h; n && (subexp || all); n = n->next) {
		sql_exp *ne = rel_find_exp_and_corresponding_rel_(rel, n->data, subexp, res);
		if (subexp && ne)
			return true;
		all &= (ne?1:0);
	}
	if (all)
		return true;
	return false;
}

static sql_exp *
rel_find_exp_and_corresponding_rel_(sql_rel *rel, sql_exp *e, bool subexp, sql_rel **res)
{
	sql_exp *ne = NULL;

	if (!rel)
		return NULL;
	switch(e->type) {
	case e_column:
		if (is_basetable(rel->op) && !rel->exps) {
			assert(e->nid);
			if (rel_base_has_nid(rel, e->nid))
				ne = e;
		} else if ((!list_empty(rel->exps) && (is_project(rel->op) || is_base(rel->op))) ||
					(!list_empty(rel->attr) && is_join(rel->op))) {
			list *l = rel->attr ? rel->attr : rel->exps;
			assert(e->nid);
			ne = exps_bind_nid(l, e->nid);
		}
		if (ne && res)
			*res = rel;
		return ne;
	case e_convert:
		return rel_find_exp_and_corresponding_rel_(rel, e->l, subexp, res);
	case e_aggr:
	case e_func:
		if (e->l)
			if (rel_find_exps_and_corresponding_rel_(rel, e->l, subexp, res))
				return e;
		return NULL;
	case e_cmp:
		if (!subexp)
			return NULL;

		if (e->flag == cmp_filter) {
			if (rel_find_exps_and_corresponding_rel_(rel, e->l, subexp, res) ||
				rel_find_exps_and_corresponding_rel_(rel, e->r, subexp, res))
				return e;
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			if (rel_find_exps_and_corresponding_rel_(rel, e->l, subexp, res))
				return e;
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			if (rel_find_exp_and_corresponding_rel_(rel, e->l, subexp, res) ||
				rel_find_exps_and_corresponding_rel_(rel, e->r, subexp, res))
				return e;
		} else if (rel_find_exp_and_corresponding_rel_(rel, e->l, subexp, res) ||
			    rel_find_exp_and_corresponding_rel_(rel, e->r, subexp, res) ||
			    (!e->f || rel_find_exp_and_corresponding_rel_(rel, e->f, subexp, res))) {
				return e;
		}
		return NULL;
	case e_psm:
		return NULL;
	case e_atom:
		if (e->f) { /* values */
			list *l = e->f;
			node *n = l->h;

			ne = n->data;
			while ((subexp || ne != NULL) && n != NULL) {
				ne = rel_find_exp_and_corresponding_rel_(rel, n->data, subexp, res);
				if (subexp && ne)
					break;
				n = n->next;
			}
			return ne;
		}
		return e;
	}
	return ne;
}

sql_exp *
rel_find_exp_and_corresponding_rel(sql_rel *rel, sql_exp *e, bool subexp, sql_rel **res, bool *under_join)
{
	sql_exp *ne = rel_find_exp_and_corresponding_rel_(rel, e, subexp, res);

	if (rel && !ne) {
		switch(rel->op) {
		case op_left:
		case op_right:
		case op_full:
		case op_join:
		case op_semi:
		case op_anti:
			ne = rel_find_exp_and_corresponding_rel(rel->l, e, subexp, res, under_join);
			if (!ne && is_join(rel->op))
				ne = rel_find_exp_and_corresponding_rel(rel->r, e, subexp, res, under_join);
			break;
		case op_table:
		case op_basetable:
			break;
		case op_munion:
			for (node* n = ((list*)rel->l)->h; n && !ne; n = n->next)
				ne = rel_find_exp_and_corresponding_rel(n->data, e, subexp, res, under_join);
			break;
		default:
			if (!is_project(rel->op) && rel->l)
				ne = rel_find_exp_and_corresponding_rel(rel->l, e, subexp, res, under_join);
		}
	}
	if (ne && under_join && is_join(rel->op))
		*under_join = true;
	return ne;
}

sql_exp *
rel_find_exp(sql_rel *rel, sql_exp *e)
{
	return rel_find_exp_and_corresponding_rel(rel, e, false, NULL, NULL);
}

bool
rel_find_nid(sql_rel *rel, int nid)
{
	if (rel) {
		switch(rel->op) {
		case op_left:
		case op_right:
		case op_full:
		case op_join:
		case op_semi:
		case op_anti:
			if (rel_find_nid(rel->l, nid))
				return true;
			if (is_join(rel->op))
				return rel_find_nid(rel->r, nid);
			break;
		case op_table:
		case op_basetable:
		case op_munion:
		case op_inter:
		case op_except:
		case op_project:
		case op_groupby:
		case op_insert:
		case op_delete:
		case op_update:
			if (rel->exps) {
				if (exps_bind_nid(rel->exps, nid))
					return true;
			} else if (rel->op == op_basetable)
				return rel_base_has_nid(rel, nid);
			break;
		case op_select:
		case op_topn:
		case op_sample:
			if (rel_find_nid(rel->l, nid))
				return true;
			break;
		case op_ddl:
		case op_truncate:
			return false;

		}
	}
	return false;
}

int
exp_is_true(sql_exp *e)
{
	if (e->type == e_atom && e->l)
		return atom_is_true(e->l);
	if (e->type == e_cmp && e->flag == cmp_equal)
		return (exp_is_true(e->l) && exp_is_true(e->r) && exp_match_exp(e->l, e->r));
	return 0;
}

static inline bool
exp_is_cmp_exp_is_false(sql_exp* e)
{
	sql_exp *l = e->l;
	sql_exp *r = e->r;
	assert(e->type == e_cmp && e->f == NULL && l && r);

	/* Handle 'v is x' and 'v is not x' expressions.
	* Other cases in is-semantics are unspecified.
	*/
	if (e->flag != cmp_equal && e->flag != cmp_notequal)
		return false;
	if (e->flag == cmp_equal && !is_anti(e))
		return ((exp_is_null(l) && exp_is_not_null(r)) || (exp_is_not_null(l) && exp_is_null(r)));
	if ((e->flag == cmp_notequal && !is_anti(e)) || (e->flag == cmp_equal && is_anti(e)))
		return exp_is_null(l) && exp_is_null(r);
	return false;
}

static inline bool
exp_single_bound_cmp_exp_is_false(sql_exp* e)
{
    assert(e->type == e_cmp);
    sql_exp* l = e->l;
    sql_exp* r = e->r;
    assert(e->f == NULL);
    assert (l && r);

    return exp_is_null(l) || exp_is_null(r);
}

static inline bool
exp_two_sided_bound_cmp_exp_is_false(sql_exp* e)
{
    assert(e->type == e_cmp);
    sql_exp* v = e->l;
    sql_exp* l = e->r;
    sql_exp* h = e->f;
    assert (v && l && h);

    return is_anti(e) ? exp_is_null(v) || (exp_is_null(l) && exp_is_null(h)) : false;
}

static inline bool
exp_regular_cmp_exp_is_false(sql_exp* e)
{
    assert(e->type == e_cmp);

    if (is_semantics(e) && !is_any(e)) return exp_is_cmp_exp_is_false(e);
	if (is_any(e)) return false;
    if (e -> f)         return exp_two_sided_bound_cmp_exp_is_false(e);
    else                return exp_single_bound_cmp_exp_is_false(e);
}

static inline bool
exp_con_exp_is_false(sql_exp* e)
{
    assert(e->type == e_cmp && e->flag == cmp_con);
	if (is_anti(e))
		return false;
	list* exps = e->l;
	for(node* n = exps->h; n; n=n->next) {
		if (exp_is_false(n->data))
			return true;
	}
	return false;
}

static inline bool
exp_dis_exp_is_false(sql_exp* e)
{
    assert(e->type == e_cmp && e->flag == cmp_dis);
	list* exps = e->l;
	if (is_anti(e))
		return false;
	for(node* n = exps->h; n; n=n->next) {
		if (!exp_is_false(n->data))
			return false;
	}
	return true;
}

static inline bool
exp_cmp_exp_is_false(sql_exp* e)
{
    assert(e->type == e_cmp);

    switch (e->flag) {
    case cmp_gt:
    case cmp_gte:
    case cmp_lte:
    case cmp_lt:
    case cmp_equal:
    case cmp_notequal:
		return exp_regular_cmp_exp_is_false(e);
    case cmp_con:
		return exp_con_exp_is_false(e);
    case cmp_dis:
		return exp_dis_exp_is_false(e);
    default:
		return false;
	}
}

int
exp_is_false(sql_exp *e)
{
	if (e->type == e_atom && e->l)
		return atom_is_false(e->l);
	else if (e->type == e_cmp)
		return exp_cmp_exp_is_false(e);
	return 0;
}

int
exp_is_zero(sql_exp *e)
{
	if (e->type == e_atom && e->l)
		return atom_is_zero(e->l);
	return 0;
}

int
exp_is_not_null(sql_exp *e)
{
	if (!has_nil(e))
		return true;

	switch (e->type) {
	case e_atom:
		if (e->f) /* values list */
			return false;
		if (e->l)
			return !(atom_null(e->l));
		return false;
	case e_convert:
		return exp_is_not_null(e->l);
	case e_func:
		if (!is_semantics(e) && e->l) {
			list *l = e->l;
			for (node *n = l->h; n; n=n->next) {
				sql_exp *p = n->data;
				if (!exp_is_not_null(p))
					return false;
			}
			return true;
		}
		return false;
	case e_aggr:
	case e_column:
	case e_cmp:
	case e_psm:
		return false;
	}
	return false;
}

static int
exps_have_null(list *l)
{
	if (!l)
		return false;
	for(node *n = l->h; n; n = n->next)
		if (exp_is_null(n->data))
			return true;
	return false;
}

int
exp_is_null(sql_exp *e )
{
	if (!has_nil(e))
		return false;

	switch (e->type) {
	case e_atom:
		if (e->f) /* values list */
			return 0;
		if (e->l)
			return (atom_null(e->l));
		return 0;
	case e_convert:
		return exp_is_null(e->l);
	case e_func:
		if (!is_semantics(e) && e->l) {
			/* This is a call to a function with no-nil semantics.
			 * If one of the parameters is null the expression itself is null
			 */
			list* l = e->l;
			for(node* n = l->h; n; n=n->next) {
				sql_exp* p = n->data;
				if (exp_is_null(p)) {
					return true;
				}
			}
		}
		return 0;
	case e_cmp:
		if (!is_semantics(e)) {
			if (e->flag == cmp_filter) {
				return (exps_have_null(e->l) && exps_have_null(e->r));
			} else if (e->flag == cmp_con || e->flag == cmp_dis) {
				return false;
			} else if (e->flag == cmp_in || e->flag == cmp_notin) {
				return ((e->flag == cmp_in && exp_is_null(e->l)) ||
						(e->flag == cmp_notin && (exp_is_null(e->l) || exps_have_null(e->r))));
			} else if (e->f) {
				return exp_is_null(e->l) && exp_is_null(e->r) && exp_is_null(e->f);
			} else {
				return exp_is_null(e->l) || exp_is_null(e->r);
			}
		}
		return 0;
	case e_aggr:
	case e_column:
	case e_psm:
		return 0;
	}
	return 0;
}

int
exp_is_rel( sql_exp *e )
{
	if (e) {
		switch(e->type){
		case e_convert:
			return exp_is_rel(e->l);
		case e_psm:
			return e->flag == PSM_REL && e->l;
		default:
			return 0;
		}
	}
	return 0;
}

int
exps_one_is_rel(list *exps)
{
	if (list_empty(exps))
		return 0;
	for(node *n = exps->h ; n ; n = n->next)
		if (exp_is_rel(n->data))
			return 1;
	return 0;
}

int
exp_is_atom( sql_exp *e )
{
	switch (e->type) {
	case e_atom:
		if (e->f) /* values list */
			return exps_are_atoms(e->f);
		return 1;
	case e_convert:
		return exp_is_atom(e->l);
	case e_func:
	case e_aggr:
		return e->card == CARD_ATOM && exps_are_atoms(e->l);
	case e_cmp:
		if (e->card != CARD_ATOM)
			return 0;
		if (e->flag == cmp_filter)
			return exps_are_atoms(e->l) && exps_are_atoms(e->r);
		if (e->flag == cmp_con || e->flag == cmp_dis)
			return exps_are_atoms(e->l);
		if (e->flag == cmp_in || e->flag == cmp_notin)
			return exp_is_atom(e->l) && exps_are_atoms(e->r);
		return exp_is_atom(e->l) && exp_is_atom(e->r) && (!e->f || exp_is_atom(e->f));
	case e_column:
	case e_psm:
		return 0;
	}
	return 0;
}

static int
exps_are_aggr(sql_rel *r, list *exps)
{
	int aggr = 1;
	if (!list_empty(exps))
		for(node *n=exps->h; n && aggr; n=n->next)
			aggr &= exp_is_aggr(r, n->data);
	return aggr;
}

/* is expression e an aggregated result of r */
int
exp_is_aggr(sql_rel *r, sql_exp *e)
{
	sql_exp *ne = NULL;

	switch (e->type) {
	case e_atom:
		return true;
	case e_convert:
		return exp_is_aggr(r, e->l);
	case e_func:
		return exps_are_aggr(r, e->l);
	case e_aggr:
		return true;
	case e_cmp:
		if (e->card != CARD_ATOM)
			return false;
		if (e->flag == cmp_filter)
			return exps_are_aggr(r, e->l) && exps_are_aggr(r, e->r);
		if (e->flag == cmp_con || e->flag == cmp_dis)
			return exps_are_aggr(r, e->l);
		if (e->flag == cmp_in || e->flag == cmp_notin)
			return exp_is_aggr(r, e->l) && exps_are_aggr(r, e->r);
		return exp_is_aggr(r, e->l) && exp_is_aggr(r, e->r) && (!e->f || exp_is_aggr(r, e->f));
	case e_column:
		if (e->freevar)
			return true;
		ne = rel_find_exp(r, e);
		if (ne) /* found local */
			return true;
		else
			return false;
	case e_psm:
		return false;
	}
	return false;
}

static int
exps_have_aggr(sql_rel *r, list *exps)
{
	int aggr = 0;
	if (!list_empty(exps))
		for(node *n=exps->h; n && !aggr; n=n->next)
			aggr |= exp_has_aggr(r, n->data);
	return aggr;
}

int
exp_has_aggr(sql_rel *r, sql_exp *e )
{
	sql_exp *ne = NULL;

	switch (e->type) {
	case e_atom:
		return false;
	case e_convert:
		return exp_has_aggr(r, e->l);
	case e_func:
		return exps_have_aggr(r, e->l);
	case e_aggr:
		return true;
	case e_cmp:
		if (e->card != CARD_ATOM)
			return false;
		if (e->flag == cmp_filter)
			return exps_have_aggr(r, e->l) && exps_have_aggr(r, e->r);
		if (e->flag == cmp_con || e->flag == cmp_dis)
			return exps_have_aggr(r, e->l);
		if (e->flag == cmp_in || e->flag == cmp_notin)
			return exp_has_aggr(r, e->l) && exps_have_aggr(r, e->r);
		return exp_has_aggr(r, e->l) && exp_has_aggr(r, e->r) && (!e->f || exp_has_aggr(r, e->f));
	case e_column:
		if (e->freevar)
			return false;
		ne = rel_find_exp(r->l, e);
		if (ne) /* found lower */
			return false;
		else
			return true;
	case e_psm:
		return false;
	}
	return false;
}

int
exp_has_rel( sql_exp *e )
{
	if (!e)
		return 0;
	switch(e->type){
	case e_func:
	case e_aggr:
		return exps_have_rel_exp(e->l);
	case e_cmp:
		if (e->flag == cmp_filter) {
			return (exps_have_rel_exp(e->l) || exps_have_rel_exp(e->r));
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			return exps_have_rel_exp(e->l);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			return (exp_has_rel(e->l) || exps_have_rel_exp(e->r));
		} else {
			return (exp_has_rel(e->l) || exp_has_rel(e->r) || (e->f && exp_has_rel(e->f)));
		}
	case e_convert:
		return exp_has_rel(e->l);
	case e_psm:
		return exp_is_rel(e);
	case e_atom:
		return (e->f && exps_have_rel_exp(e->f));
	case e_column:
		return 0;
	}
	return 0;
}

int
exps_have_rel_exp( list *exps)
{
	if (list_empty(exps))
		return 0;
	for(node *n=exps->h; n; n=n->next) {
		sql_exp *e = n->data;

		if (exp_has_rel(e))
			return 1;
	}
	return 0;
}

static sql_rel *
exps_rel_get_rel(allocator *sa, list *exps )
{
	sql_rel *r = NULL, *xp = NULL;

	if (list_empty(exps))
		return NULL;
	for (node *n = exps->h; n; n=n->next){
		sql_exp *e = n->data;

		if (exp_has_rel(e)) {
			if (!(r = exp_rel_get_rel(sa, e)))
				return NULL;
			if (xp) {
				xp = rel_crossproduct(sa, xp, r, op_full);
				set_processed(xp);
			} else {
				xp = r;
			}
		}
	}
	return xp;
}

int
exp_rel_depth(sql_exp *e)
{
	if (!e)
		return 0;
	switch(e->type){
	case e_func:
	case e_aggr:
	case e_cmp:
		return 1;
	case e_convert:
		return 0;
	case e_psm:
		if (exp_is_rel(e))
			return 0;
		return 1;
	case e_atom:
	case e_column:
		return 0;
	}
	return 0;
}

sql_rel *
exp_rel_get_rel(allocator *sa, sql_exp *e)
{
	if (!e)
		return NULL;

	switch(e->type){
	case e_func:
	case e_aggr:
		return exps_rel_get_rel(sa, e->l);
	case e_cmp: {
		sql_rel *r = NULL, *xp = NULL;

		if (e->flag == cmp_filter) {
			if (exps_have_rel_exp(e->l))
				xp = exps_rel_get_rel(sa, e->l);
			if (exps_have_rel_exp(e->r)) {
				if (!(r = exps_rel_get_rel(sa, e->r)))
					return NULL;
				if (xp) {
					xp = rel_crossproduct(sa, xp, r, op_join);
					set_processed(xp);
				} else {
					xp = r;
				}
			}
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			if (exps_have_rel_exp(e->l)) {
				if (!(r = exps_rel_get_rel(sa, e->l)))
					return NULL;
				if (xp) {
					xp = rel_crossproduct(sa, xp, r, op_join);
					set_processed(xp);
				} else {
					xp = r;
				}
			}
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			if (exp_has_rel(e->l))
				xp = exp_rel_get_rel(sa, e->l);
			if (exps_have_rel_exp(e->r)) {
				if (!(r = exps_rel_get_rel(sa, e->r)))
					return NULL;
				if (xp) {
					xp = rel_crossproduct(sa, xp, r, op_join);
					set_processed(xp);
				} else {
					xp = r;
				}
			}
		} else {
			if (exp_has_rel(e->l))
				xp = exp_rel_get_rel(sa, e->l);
			if (exp_has_rel(e->r)) {
				if (!(r = exp_rel_get_rel(sa, e->r)))
					return NULL;
				if (xp) {
					xp = rel_crossproduct(sa, xp, r, op_join);
					set_processed(xp);
				} else {
					xp = r;
				}
			}
			if (e->f && exp_has_rel(e->f)) {
				if (!(r = exp_rel_get_rel(sa, e->f)))
					return NULL;
				if (xp) {
					xp = rel_crossproduct(sa, xp, r, op_join);
					set_processed(xp);
				} else {
					xp = r;
				}
			}
		}
		return xp;
	}
	case e_convert:
		return exp_rel_get_rel(sa, e->l);
	case e_psm:
		if (exp_is_rel(e))
			return e->l;
		return NULL;
	case e_atom:
		if (e->f && exps_have_rel_exp(e->f))
			return exps_rel_get_rel(sa, e->f);
		return NULL;
	case e_column:
		return NULL;
	}
	return NULL;
}

static void exp_rel_update_set_freevar(sql_exp *e);

static void
exps_rel_update_set_freevar(list *exps)
{
	if (!list_empty(exps))
		for (node *n=exps->h; n ; n=n->next)
			exp_rel_update_set_freevar(n->data);
}

static void
exp_rel_update_set_freevar(sql_exp *e)
{
	if (!e)
		return ;

	switch(e->type){
	case e_func:
	case e_aggr:
		exps_rel_update_set_freevar(e->l);
		break;
	case e_cmp:
		if (e->flag == cmp_filter) {
			exps_rel_update_set_freevar(e->l);
			exps_rel_update_set_freevar(e->r);
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			exps_rel_update_set_freevar(e->l);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			exp_rel_update_set_freevar(e->l);
			exps_rel_update_set_freevar(e->r);
		} else {
			exp_rel_update_set_freevar(e->l);
			exp_rel_update_set_freevar(e->r);
			if (e->f)
				exp_rel_update_set_freevar(e->f);
		}
		break;
	case e_convert:
		exp_rel_update_set_freevar(e->l);
		break;
	case e_atom:
		if (e->f)
			exps_rel_update_set_freevar(e->f);
		break;
	case e_column:
		set_freevar(e, 1);
		break;
	case e_psm:
		break;
	}
}

static list *
exp_rel_update_exps(mvc *sql, list *exps, bool up)
{
	if (list_empty(exps))
		return exps;
	for (node *n = exps->h; n; n=n->next){
		sql_exp *e = n->data;

		if (exp_has_rel(e))
			n->data = exp_rel_update_exp(sql, e, up);
		else if (!exp_is_atom(e) && !up)
			exp_rel_update_set_freevar(e);
	}
	return exps;
}

static sql_exp *
exp_rel_update_exp_(mvc *sql, sql_exp *e, bool up)
{
	if (exp_has_rel(e))
		e = exp_rel_update_exp(sql, e, up);
	else if (!exp_is_atom(e) && !up)
		exp_rel_update_set_freevar(e);
	return e;
}

sql_exp *
exp_rel_update_exp(mvc *sql, sql_exp *e, bool up)
{
	if (!e)
		return NULL;

	switch(e->type){
	case e_func:
	case e_aggr:
		if (exps_have_rel_exp(e->l))
			e->l = exp_rel_update_exps(sql, e->l, up);
		return e;
	case e_cmp:
		if (e->flag == cmp_filter) {
			if (exps_have_rel_exp(e->l))
				e->l = exp_rel_update_exps(sql, e->l, up);
			if (exps_have_rel_exp(e->r))
				e->r = exp_rel_update_exps(sql, e->r, up);
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			if (exps_have_rel_exp(e->l))
				e->l = exp_rel_update_exps(sql, e->l, up);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			if (exp_has_rel(e->l))
				e->l = exp_rel_update_exp(sql, e->l, up);
			if (exps_have_rel_exp(e->r))
				e->r = exp_rel_update_exps(sql, e->r, up);
		} else {
			//if (exp_has_rel(e->l))
				e->l = exp_rel_update_exp_(sql, e->l, up);
			//if (exp_has_rel(e->r))
				e->r = exp_rel_update_exp_(sql, e->r, up);
			if (e->f /*&& exp_has_rel(e->f)*/)
				e->f = exp_rel_update_exp_(sql, e->f, up);
		}
		return e;
	case e_convert:
		if (exp_has_rel(e->l))
			e->l = exp_rel_update_exp(sql, e->l, up);
		return e;
	case e_psm:
		if (exp_is_rel(e)) {
			sql_rel *r = exp_rel_get_rel(sql->sa, e), *nr = r;
			if (is_topn(r->op)) {
				nr = r->l;
				if (nr && !is_project(nr->op))
					r->l = nr = rel_project(sql->sa, nr, rel_projections(sql, nr, NULL, 1, 0));
			}
			e = nr->exps->t->data;
			e = exp_ref(sql, e);
			if (up)
				set_freevar(e, 1);
			return e;
		}
		return e;
	case e_atom:
		if (e->f && exps_have_rel_exp(e->f))
			e->f = exp_rel_update_exps(sql, e->f, up);
		return e;
	case e_column:
		return e;
	}
	return e;
}

sql_exp *
exp_rel_label(mvc *sql, sql_exp *e)
{
	if (exp_is_rel(e))
		e->l = rel_label(sql, e->l, 1);
	return e;
}

int
exps_are_atoms( list *exps)
{
	int atoms = 1;
	if (!list_empty(exps))
		for(node *n=exps->h; n && atoms; n=n->next)
			atoms &= exp_is_atom(n->data);
	return atoms;
}

int
exps_have_func(list *exps)
{
	if (list_empty(exps))
		return 0;
	for(node *n=exps->h; n; n=n->next) {
		sql_exp *e = n->data;

		if (exp_has_func(e))
			return 1;
	}
	return 0;
}

static int exp_has_func_or_cmp(sql_exp *e, bool cmp);

static int
exps_have_func_or_cmp(list *exps, bool cmp)
{
	if (list_empty(exps))
		return 0;
	for(node *n=exps->h; n; n=n->next) {
		sql_exp *e = n->data;

		if (exp_has_func_or_cmp(e, cmp))
			return 1;
	}
	return 0;
}

static int
exp_has_func_or_cmp(sql_exp *e, bool cmp)
{
	if (!e)
		return 0;
	switch (e->type) {
	case e_atom:
		if (e->f)
			return exps_have_func_or_cmp(e->f, true);
		return 0;
	case e_convert:
		{
			sql_subtype *t = exp_totype(e);
			sql_subtype *f = exp_fromtype(e);
			if (t->type->eclass == EC_FLT && (f->type->eclass == EC_DEC || f->type->eclass == EC_NUM))
				return exp_has_func_or_cmp(e->l, cmp);
			if (f->type->localtype > t->type->localtype)
				return true;
		}
		return exp_has_func_or_cmp(e->l, cmp);
	case e_func:
		return 1;
	case e_aggr:
		if (e->l)
			return exps_have_func_or_cmp(e->l, true);
		return 0;
	case e_cmp:
		if (cmp)
			return 1;
		if (e->flag == cmp_filter) {
			return (exps_have_func_or_cmp(e->l, true) || exps_have_func_or_cmp(e->r, true));
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			return exps_have_func_or_cmp(e->l, true);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			return (exp_has_func_or_cmp(e->l, true) || exps_have_func_or_cmp(e->r, true));
		} else {
			return (exp_has_func_or_cmp(e->l, true) || exp_has_func_or_cmp(e->r, true) ||
					(e->f && exp_has_func_or_cmp(e->f, true)));
		}
	case e_column:
	case e_psm:
		return 0;
	}
	return 0;
}

int
exp_has_func(sql_exp *e)
{
	return exp_has_func_or_cmp(e, false);
}

static int
exps_have_sideeffect( list *exps)
{
	node *n;
	int has_sideeffect = 0;

	for(n=exps->h; n && !has_sideeffect; n=n->next)
		has_sideeffect |= exp_has_sideeffect(n->data);
	return has_sideeffect;
}

int
exp_has_sideeffect( sql_exp *e )
{
	switch (e->type) {
	case e_convert:
		return exp_has_sideeffect(e->l);
	case e_func:
		{
			sql_subfunc *f = e->f;

			if (f->func->side_effect)
				return 1;
			if (e->l)
				return exps_have_sideeffect(e->l);
			return 0;
		}
	case e_atom:
		if (e->f)
			return exps_have_sideeffect(e->f);
		return 0;
	case e_aggr:
	case e_cmp:
	case e_column:
	case e_psm:
		return 0;
	}
	return 0;
}

bool
exps_have_unsafe(list *exps, bool allow_identity, bool card)
{
	int unsafe = 0;

	if (list_empty(exps))
		return 0;
	for (node *n = exps->h; n && !unsafe; n = n->next)
		unsafe |= exp_unsafe(n->data, allow_identity, card);
	return unsafe;
}

bool
exp_unsafe(sql_exp *e, bool allow_identity, bool card)
{
	switch (e->type) {
	case e_convert:
		if (card) {
			sql_subtype *t = exp_totype(e);
			sql_subtype *f = exp_fromtype(e);
			if (t->type->eclass == EC_FLT && (f->type->eclass == EC_DEC || f->type->eclass == EC_NUM))
				return false;
			if (f->type->localtype > t->type->localtype)
				return true;
			return false;
		}
		return exp_unsafe(e->l, allow_identity, card);
	case e_aggr:
	case e_func: {
		sql_subfunc *f = e->f;

		if (IS_ANALYTIC(f->func) || !LANG_INT_OR_MAL(f->func->lang) || f->func->side_effect || (!allow_identity && is_identity(e, NULL)))
			return 1;
		return exps_have_unsafe(e->l, allow_identity, card);
	} break;
	case e_cmp: {
		if (e->flag == cmp_con || e->flag == cmp_dis) {
			return exps_have_unsafe(e->l, allow_identity, card);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			return exp_unsafe(e->l, allow_identity, card) || exps_have_unsafe(e->r, allow_identity, card);
		} else if (e->flag == cmp_filter) {
			return exps_have_unsafe(e->l, allow_identity, card) || exps_have_unsafe(e->r, allow_identity, card);
		} else {
			return exp_unsafe(e->l, allow_identity, card) || exp_unsafe(e->r, allow_identity, card) || (e->f && exp_unsafe(e->f, allow_identity, card));
		}
	} break;
	case e_atom: {
		if (e->f)
			return exps_have_unsafe(e->f, allow_identity, card);
		return 0;
	} break;
	case e_column:
	case e_psm:
		return 0;
	}
	return 0;
}

bool
exps_have_fallible(list *exps)
{
	if (!exps)
		return false;
	for (node *n = exps->h; n; n = n->next)
		if (exp_is_fallible(n->data))
			return true;
	return false;
}

bool
exp_is_fallible(sql_exp *e)
{
	switch (e->type) {
	case e_convert:
		{
			sql_subtype *t = exp_totype(e);
			sql_subtype *f = exp_fromtype(e);
			if (t->type->eclass == EC_FLT && (f->type->eclass == EC_DEC || f->type->eclass == EC_NUM))
				return exp_is_fallible(e->l);
			if (f->type->localtype > t->type->localtype)
				return true;
			/* TODO for types with digits check if t->digits is large enough
			 * for types with scale etc */
			return exp_is_fallible(e->l);
		}
	case e_aggr:
	case e_func: {
		sql_subfunc *f = e->f;

		if (IS_ANALYTIC(f->func) || !LANG_INT_OR_MAL(f->func->lang) || f->func->side_effect)
			return true;
		return exps_have_fallible(e->l);
	} break;
	case e_cmp: {
		if (e->flag == cmp_con || e->flag == cmp_dis) {
			return exps_have_fallible(e->l);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			return exp_is_fallible(e->l) || exps_have_fallible(e->r);
		} else if (e->flag == cmp_filter) {
			return exps_have_fallible(e->l) || exps_have_fallible(e->r);
		} else {
			return exp_is_fallible(e->l) || exp_is_fallible(e->r) || (e->f && exp_is_fallible(e->f));
		}
	} break;
	case e_atom:
	case e_column:
	case e_psm:
		return false;
	}
	return false;
}

static inline int
exp_key( sql_exp *e )
{
	if (e->alias.name)
		return hash_key(e->alias.name);
	return 0;
}

sql_exp *
exps_uses_nid(list *exps, int nid)
{
	if (exps) {
		for (node *en = exps->h; en; en = en->next ) {
			sql_exp *e = en->data;

			if (e->nid == nid)
				return e;
		}
	}
	return NULL;
}

sql_exp *
exps_bind_nid(list *exps, int nid)
{
	if (exps) {
		for (node *en = exps->h; en; en = en->next ) {
			sql_exp *e = en->data;

			if (e->alias.label == nid)
				return e;
		}
	}
	return NULL;
}

sql_exp *
exps_bind_column(list *exps, const char *cname, int *ambiguous, int *multiple, int no_tname)
{
	sql_exp *res = NULL;

	if (exps && cname) {
		node *en;

		if (exps) {
			if (!exps->ht && list_length(exps) > HASH_MIN_SIZE) {
				exps->ht = hash_new(exps->sa, list_length(exps), (fkeyvalue)&exp_key);
				if (exps->ht == NULL)
					return NULL;
				for (en = exps->h; en; en = en->next ) {
					sql_exp *e = en->data;
					if (e->alias.name) {
						int key = exp_key(e);

						if (hash_add(exps->ht, key, e) == NULL)
							return NULL;
					}
				}
			}
			if (exps->ht) {
				int key = hash_key(cname);
				sql_hash_e *he = exps->ht->buckets[key&(exps->ht->size-1)];

				for (; he; he = he->chain) {
					sql_exp *e = he->value;

					if (e->alias.name && strcmp(e->alias.name, cname) == 0 && (!no_tname || !e->alias.rname)) {
						if (res && multiple)
							*multiple = 1;
						if (!res)
							res = e;

						if (res && res != e && e->alias.rname && res->alias.rname && strcmp(e->alias.rname, res->alias.rname) != 0 ) {
							if (ambiguous)
								*ambiguous = 1;
							return NULL;
						}
						res = e;
					}
				}
				return res;
			}
		}
		for (en = exps->h; en; en = en->next ) {
			sql_exp *e = en->data;

			if (e->alias.name && strcmp(e->alias.name, cname) == 0 && (!no_tname || !e->alias.rname)) {
				if (res && multiple)
					*multiple = 1;
				if (!res)
					res = e;

				if (res && res != e && e->alias.rname && res->alias.rname && strcmp(e->alias.rname, res->alias.rname) != 0 ) {
					if (ambiguous)
						*ambiguous = 1;
					return NULL;
				}
				res = e;
			}
		}
	}
	return res;
}

sql_exp *
exps_bind_column2(list *exps, const char *rname, const char *cname, int *multiple)
{
	sql_exp *res = NULL;

	if (exps) {
		node *en;

		if (!exps->ht && list_length(exps) > HASH_MIN_SIZE) {
			exps->ht = hash_new(exps->sa, list_length(exps), (fkeyvalue)&exp_key);
			if (exps->ht == NULL)
				return res;

			for (en = exps->h; en; en = en->next ) {
				sql_exp *e = en->data;
				if (e->alias.name) {
					int key = exp_key(e);

					if (hash_add(exps->ht, key, e) == NULL)
						return res;
				}
			}
		}
		if (exps->ht) {
			int key = hash_key(cname);
			sql_hash_e *he = exps->ht->buckets[key&(exps->ht->size-1)];

			for (; he; he = he->chain) {
				sql_exp *e = he->value;

				if (e && e->alias.name && e->alias.rname && strcmp(e->alias.name, cname) == 0 && strcmp(e->alias.rname, rname) == 0) {
					if (res && multiple)
						*multiple = 1;
					if (!res)
						res = e;
					if (res && has_label(res)) /* aliases maybe used multiple times without problems */
						return res;
				}
			}
			return res;
		}
		for (en = exps->h; en; en = en->next ) {
			sql_exp *e = en->data;

			if (e && e->alias.name && e->alias.rname && strcmp(e->alias.name, cname) == 0 && strcmp(e->alias.rname, rname) == 0) {
				if (res && multiple)
					*multiple = 1;
				if (!res)
					res = e;
				if (res && has_label(res)) /* aliases maybe used multiple times without problems */
					return res;
			}
		}
	}
	return res;
}

/* find an column based on the original name, not the alias it got */
sql_exp *
exps_bind_alias( list *exps, const char *rname, const char *cname )
{
	if (exps) {
		node *en;

		for (en = exps->h; en; en = en->next ) {
			sql_exp *e = en->data;

			if (e && is_column(e->type) && !rname && e->r && strcmp(e->r, cname) == 0)
			{
				assert(0);
				return e;
			}
			if (e && e->type == e_column && rname && e->l && e->r && strcmp(e->r, cname) == 0 && strcmp(e->l, rname) == 0) {
				assert(0);
				return e;
			}
		}
	}
	return NULL;
}

unsigned int
exps_card( list *l )
{
	node *n;
	unsigned int card = CARD_ATOM;

	if (l) for(n = l->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e && card < e->card)
			card = e->card;
	}
	return card;
}

void
exps_fix_card( list *exps, unsigned int card)
{
	if (exps)
		for (node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e && e->card > card)
			e->card = card;
	}
}

void
exps_setcard( list *exps, unsigned int card)
{
	if (exps)
		for (node *n = exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (e && e->card != CARD_ATOM)
				e->card = card;
		}
}

int
exps_intern(list *exps)
{
	if (exps)
		for (node *n=exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (is_intern(e))
				return 1;
		}
	return 0;
}

sql_exp *
exps_find_one_multi_exp(list *exps)
{
	sql_exp *l = NULL;
	int skip = 0;

	/* Find one and only 1 expression with card > CARD_ATOM */
	if (!list_empty(exps)) {
		for (node *m = exps->h ; m && !skip ; m = m->next) {
			sql_exp *e = m->data;

			if (e->card > CARD_ATOM) {
				skip |= l != NULL;
				l = e;
			}
		}
	}
	if (skip)
		l = NULL;
	return l;
}

const char *
compare_func( comp_type t, int anti )
{
	switch(t) {
	case cmp_equal:
		return anti?"<>":"=";
	case cmp_lt:
		return anti?">":"<";
	case cmp_lte:
		return anti?">=":"<=";
	case cmp_gte:
		return anti?"<=":">=";
	case cmp_gt:
		return anti?"<":">";
	case cmp_notequal:
		return anti?"=":"<>";
	default:
		return NULL;
	}
}

int
is_identity( sql_exp *e, sql_rel *r)
{
	switch(e->type) {
	case e_column:
		if (r && is_project(r->op) && !is_set(r->op) && !is_munion(r->op)) {
			sql_exp *re = NULL;
			assert(e->nid);
			re = exps_bind_nid(r->exps, e->nid);
			if (re)
				return is_identity(re, r->l);
		}
		return 0;
	case e_func: {
		sql_subfunc *f = e->f;
		return !f->func->s && strcmp(f->func->base.name, "identity") == 0;
	}
	default:
		return 0;
	}
}

list *
exps_alias(mvc *sql, list *exps)
{
	list *nl = new_exp_list(sql->sa);

	if (exps)
		for (node *n = exps->h; n; n = n->next) {
			sql_exp *e = n->data, *ne;

			assert(exp_name(e));
			ne = exp_ref(sql, e);
			append(nl, ne);
		}
	return nl;
}

list *
exps_copy(mvc *sql, list *exps)
{
	list *nl;

	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (!exps)
		return NULL;
	nl = new_exp_list(sql->sa);
	for (node *n = exps->h; n; n = n->next) {
		sql_exp *arg = n->data;

		arg = exp_copy(sql, arg);
		if (!arg)
			return NULL;
		append(nl, arg);
	}
	return nl;
}

sql_exp *
exp_copy(mvc *sql, sql_exp * e)
{
	sql_exp *l, *r, *r2, *ne = NULL;

	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (!e)
		return NULL;
	switch(e->type){
	case e_column:
		ne = exp_column(sql->sa, e->l, e->r, exp_subtype(e), e->card, has_nil(e), is_unique(e), is_intern(e));
		ne->flag = e->flag;
		ne->alias.label = e->alias.label;
		ne->nid = e->nid;
		break;
	case e_cmp:
		if (e->flag == cmp_filter) {
			list *l = exps_copy(sql, e->l);
			list *r = exps_copy(sql, e->r);

			ne = exp_filter(sql->sa, l, r, e->f, is_anti(e));
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			list *l = exps_copy(sql, e->l);

			if (e->flag == cmp_con)
				return exp_conjunctive(sql->sa, l);
			else
				return exp_disjunctive(sql->sa, l);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			sql_exp *l = exp_copy(sql, e->l);
			list *r = exps_copy(sql, e->r);

			ne = exp_in(sql->sa, l, r, e->flag);
		} else {
			l = exp_copy(sql, e->l);
			r = exp_copy(sql, e->r);

			if (e->f) {
				r2 = exp_copy(sql, e->f);
				ne = exp_compare2(sql->sa, l, r, r2, e->flag, is_symmetric(e));
			} else {
				ne = exp_compare(sql->sa, l, r, e->flag);
			}
		}
		break;
	case e_convert:
		ne = exp_convert(sql, exp_copy(sql, e->l), exp_fromtype(e), exp_totype(e));
		break;
	case e_aggr:
	case e_func: {
		list *l = exps_copy(sql, e->l);

		if (e->type == e_func)
			ne = exp_op(sql->sa, l, e->f);
		else
			ne = exp_aggr(sql->sa, l, e->f, need_distinct(e), need_no_nil(e), e->card, has_nil(e));
		if (e->r) { /* copy obe and gbe lists */
			list *er = (list*) e->r;
			assert(list_length(er) <= 2);
			if (list_length(er) == 2)
				ne->r = list_append(list_append(sa_list(sql->sa), exps_copy(sql, er->h->data)), exps_copy(sql, er->h->next->data));
			else
				ne->r = list_append(sa_list(sql->sa), exps_copy(sql, er->h->data));
		}
		break;
	}
	case e_atom:
		if (e->l)
			ne = exp_atom(sql->sa, e->l);
		else if (e->r) {
			sql_var_name *vname = (sql_var_name*) e->r;
			ne = exp_param_or_declared(sql->sa, vname->sname, vname->name, &e->tpe, e->flag);
		} else if (e->f)
			ne = exp_values(sql->sa, exps_copy(sql, e->f));
		else
			ne = exp_atom_ref(sql->sa, e->flag, &e->tpe);
		break;
	case e_psm:
		if (e->flag & PSM_SET) {
			ne = exp_set(sql->sa, e->alias.rname, e->alias.name, exp_copy(sql, e->l), GET_PSM_LEVEL(e->flag));
		} else if (e->flag & PSM_VAR) {
			if (e->f)
				ne = exp_table(sql->sa, e->alias.name, e->f, GET_PSM_LEVEL(e->flag));
			else
				ne = exp_var(sql->sa, e->alias.rname, e->alias.name, &e->tpe, GET_PSM_LEVEL(e->flag));
		} else if (e->flag & PSM_RETURN) {
			ne = exp_return(sql->sa, exp_copy(sql, e->l), GET_PSM_LEVEL(e->flag));
		} else if (e->flag & PSM_WHILE) {
			ne = exp_while(sql->sa, exp_copy(sql, e->l), exps_copy(sql, e->r));
		} else if (e->flag & PSM_IF) {
			ne = exp_if(sql->sa, exp_copy(sql, e->l), exps_copy(sql, e->r), exps_copy(sql, e->f));
		} else if (e->flag & PSM_REL) {
			if (!e->alias.label)
				exp_label(sql->sa, e, ++sql->label);
			return exp_ref(sql, e);
		} else if (e->flag & PSM_EXCEPTION) {
			ne = exp_exception(sql->sa, exp_copy(sql, e->l), (const char *) e->r);
		}
		break;
	}
	if (!ne)
		return ne;
	if (e->alias.name)
		exp_prop_alias(sql->sa, ne, e);
	ne = exp_propagate(sql->sa, ne, e);
	if (is_freevar(e))
		set_freevar(ne, is_freevar(e)-1);
	return ne;
}

/* scaling for the division operator */
static sql_exp *
exp_scale_algebra(mvc *sql, sql_subfunc *f, sql_rel *rel, sql_exp *l, sql_exp *r)
{
	sql_subtype *lt = exp_subtype(l);
	sql_subtype *rt = exp_subtype(r);

	if (!EC_INTERVAL(lt->type->eclass) && lt->type->scale == SCALE_FIX &&
		(lt->scale || rt->scale) && strcmp(sql_func_imp(f->func), "/") == 0) {
		sql_subtype *res = f->res->h->data;
		unsigned int scale, digits, digL, scaleL;
		sql_subtype nlt;

		/* scale fixing may require a larger type ! */
		scaleL = (lt->scale < sql->div_min_scale) ? sql->div_min_scale : lt->scale;
		scaleL += (scaleL < rt->scale) ? rt->scale - scaleL : 0;
		scale = scaleL;
		scaleL += rt->scale;
		digL = lt->digits + (scaleL - lt->scale);
		digits = (digL > rt->digits) ? digL : rt->digits;

		/* HACK alert: digits should be less than max */
#ifdef HAVE_HGE
		if (res->type->radix == 10 && digits > 38)
			digits = 38;
		if (res->type->radix == 2 && digits > 127)
			digits = 127;
#else
		if (res->type->radix == 10 && digits > 18)
			digits = 18;
		if (res->type->radix == 2 && digits > 63)
			digits = 63;
#endif

		sql_find_subtype(&nlt, lt->type->base.name, digL, scaleL);
		if (nlt.digits < scaleL)
			return sql_error(sql, 01, SQLSTATE(42000) "Scale (%d) overflows type", scaleL);
		l = exp_check_type(sql, &nlt, rel, l, type_equal);

		sql_find_subtype(res, lt->type->base.name, digits, scale);
	} else if (lt->type->scale == SCALE_FIX) {
		sql_subtype *res = f->res->h->data;
		if (res->type->eclass == EC_NUM)
			res->digits = MAX(lt->digits, rt->digits);
	}
	return l;
}

sql_exp *
exps_scale_algebra(mvc *sql, sql_subfunc *f, sql_rel *rel, list *exps)
{
	if (list_length(exps) != 2)
		return NULL;
	sql_exp *e = exp_scale_algebra(sql, f, rel, exps->h->data, exps->h->next->data);
	if (e)
		exps->h->data = e;
	return e;
}

void
exps_digits_add(sql_subfunc *f, list *exps)
{
	/* concat and friends need larger results */
	if (!f->func->res)
		return;
	int digits = 0;
	for(node *n = exps->h; n; n = n->next) {
		sql_subtype *t = exp_subtype(n->data);

		if (!t->digits) {
			digits = 0;
			break;
		}
		digits += t->digits;
	}
	sql_subtype *res = f->res->h->data;
	res->digits = digits;
}

void
exps_sum_scales(sql_subfunc *f, list *exps)
{
	/* sum scales and digits for multiply operation */
	sql_arg *ares = f->func->res->h->data;

	if (ares->type.type->scale == SCALE_FIX && strcmp(sql_func_imp(f->func), "*") == 0) {
		unsigned int digits = 0, scale = 0;
		sql_type *largesttype = ares->type.type;

		for(node *n = exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_subtype *t = exp_subtype(e);

			scale += t->scale;
			digits += t->digits;
			if (largesttype->localtype < t->type->localtype)
				largesttype = t->type;
		}
		sql_subtype *res = f->res->h->data;

		res->scale = scale;
		res->digits = digits;

		/* HACK alert: digits should be less than max */
#ifdef HAVE_HGE
		if (ares->type.type->radix == 10 && res->digits > 38) {
			res->digits = 38;
			res->scale = MIN(res->scale, res->digits - 1);
		}
		if (ares->type.type->radix == 2 && res->digits > 127) {
			res->digits = 127;
			res->scale = MIN(res->scale, res->digits - 1);
		}
#else
		if (ares->type.type->radix == 10 && res->digits > 18) {
			res->digits = 18;
			res->scale = MIN(res->scale, res->digits - 1);
		}
		if (ares->type.type->radix == 2 && res->digits > 63) {
			res->digits = 63;
			res->scale = MIN(res->scale, res->digits - 1);
		}
#endif

		sql_subtype t;
		/* numeric types are fixed length */
		if (ares->type.type->eclass == EC_NUM) {
#ifdef HAVE_HGE
			if (ares->type.type->localtype == TYPE_hge && res->digits == 127)
				t = *sql_fetch_localtype(TYPE_hge);
			else
#endif
			if (ares->type.type->localtype == TYPE_lng && res->digits == 63)
				t = *sql_fetch_localtype(TYPE_lng);
			else if (res->type->digits >= res->digits)
				t = *res; /* we cannot reduce types! */
			else
				sql_find_numeric(&t, ares->type.type->localtype, res->digits);
		} else {
			if (res->digits > largesttype->digits)
				sql_find_subtype(&t, largesttype->base.name, res->digits, res->scale);
			else
				sql_init_subtype(&t, largesttype, res->digits, res->scale);
		}
		*res = t;
	}
}

void
exps_max_bits(sql_subfunc *f, list *exps)
{
	/* + and - have max_bits + 1 */
	if (!f->func->res)
		return;
	unsigned int digits = 0;
	for(node *n = exps->h; n; n = n->next) {
		sql_subtype *t = exp_subtype(n->data);

		if (!t)
			continue;
		if (digits < t->digits)
			digits = t->digits;
	}
	/* + and - (because of negative numbers) could need one extra bit (or digit) */
	digits += 1;
	sql_subtype *res = f->res->h->data;
	if (digits > res->type->digits)
		res = sql_find_numeric(res, res->type->localtype, digits);
	else
		res->digits = digits;
}

void
exps_inout(sql_subfunc *f, list *exps)
{
	/* output == first input */
	if (!f->func->res)
		return;
	sql_subtype *res = f->res->h->data;
	bool is_decimal = (res->type->eclass == EC_DEC);
	unsigned int digits = 0, scale = 0;
	sql_type *largesttype = NULL;
	for(node *n = exps->h; n; n = n->next) {
		sql_subtype *t = exp_subtype(n->data);

		if (!t)
			continue;

		if (is_decimal && t->type->eclass == EC_DEC && (!largesttype || largesttype->localtype < t->type->localtype))
			largesttype = t->type;
		if (is_decimal && t->type->eclass == EC_NUM) {
			unsigned int d = bits2digits(t->digits);
			digits = d>digits?d:digits;
		} else if (digits < t->digits)
			digits = t->digits;
		if (scale < t->scale)
			scale = t->scale;
		break;
	}
	if (digits > res->digits || scale > res->scale) {
		if (largesttype)
			sql_init_subtype(res, largesttype, digits, scale);
		else
			sql_find_subtype(res, res->type->base.name, digits, scale);
	} else
		res->digits = digits;
}

/* for aggregates we can reduce the result types size based on real digits/bits used number of known input rows */
void
exps_largest_int(sql_subfunc *f, list *exps, lng cnt)
{
	if (!f->func->res || cnt == 0)
		return;
	sql_subtype *res = f->res->h->data;
	if (res->type->eclass != EC_DEC && res->type->eclass != EC_NUM)
		return;
	bool is_decimal = (res->type->eclass == EC_DEC);
	unsigned int digits = 0, scale = 0, mdigits = is_decimal ? decimal_digits(cnt) : number_bits(cnt);
	sql_type *largesttype = NULL;
	for(node *n = exps->h; n; n = n->next) {
		sql_subtype *t = exp_subtype(n->data);

		if (!t)
			continue;

		largesttype = t->type;
		if (is_decimal && t->type->eclass == EC_NUM) {
			unsigned int d = bits2digits(t->digits);
			digits = d>digits?d:digits;
		} else if (digits < t->digits)
			digits = t->digits;
		if (scale < t->scale)
			scale = t->scale;
		break;
	}
	digits += mdigits;
	if (largesttype && digits <= largesttype->digits)
		sql_init_subtype(res, largesttype, digits, scale);
	else if (is_decimal)
		sql_find_subtype(res, res->type->base.name, digits, scale);
	else
		sql_find_numeric(res, 1, digits);
}

#define is_addition(fname) (strcmp(fname, "sql_add") == 0)
#define is_subtraction(fname) (strcmp(fname, "sql_sub") == 0)
void
exps_scale_fix(sql_subfunc *f, list *exps, sql_subtype *atp)
{
	if (!f->func->res)
		return;

	sql_subtype *res = f->res->h->data;
	if (res->type->eclass != EC_ANY && res->type->eclass != EC_DEC)
		return;

	unsigned int digits = 0, scale = 0;
	sql_type *largesttype = NULL;
	for(node *n = exps->h; n; n = n->next) {
		sql_subtype *t = exp_subtype(n->data);

		if (!t)
			continue;
		if (digits < t->digits)
			digits = t->digits;
		if (scale < t->scale)
			scale = t->scale;
		if (t->type->eclass == EC_DEC && (!largesttype || largesttype->localtype < t->type->localtype))
			largesttype = t->type;
	}
	res->scale = scale;
	if (res->type->eclass == EC_DEC)
		digits += (is_addition(f->func->base.name) || is_subtraction(f->func->base.name));
	if (digits > res->type->digits) {
		if (largesttype && largesttype->localtype > res->type->localtype)
			sql_init_subtype(res, largesttype, digits, scale);
		else
			sql_find_subtype(res, res->type->localtype?res->type->base.name:atp->type->base.name, digits, scale);
	} else if (res->type->eclass == EC_DEC || res->type->eclass == EC_NUM)
		res->digits = digits;
}

int
exp_aggr_is_count(sql_exp *e)
{
	if (e->type == e_aggr && !((sql_subfunc *)e->f)->func->s && strcmp(((sql_subfunc *)e->f)->func->base.name, "count") == 0)
		return 1;
	return 0;
}

list *
check_distinct_exp_names(mvc *sql, list *exps)
{
	list *distinct_exps = NULL;
	bool duplicates = false;

	if (list_length(exps) < 2) {
		return exps; /* always true */
	} else if (list_length(exps) < 5) {
		distinct_exps = list_distinct(exps, (fcmp) exp_equal, (fdup) NULL);
	} else { /* for longer lists, use hashing */
		sql_hash *ht = hash_new(sql->ta, list_length(exps), (fkeyvalue)&exp_key);

		for (node *n = exps->h; n && !duplicates; n = n->next) {
			sql_exp *e = n->data;
			int key = ht->key(e);
			sql_hash_e *he = ht->buckets[key&(ht->size-1)];

			for (; he && !duplicates; he = he->chain) {
				sql_exp *f = he->value;

				if (!exp_equal(e, f))
					duplicates = true;
			}
			hash_add(ht, key, e);
		}
	}
	if ((distinct_exps && list_length(distinct_exps) != list_length(exps)) || duplicates)
		return NULL;
	return exps;
}

static int rel_find_parameter(mvc *sql, sql_subtype *type, sql_rel *rel, int nid, const char *relname, const char *expname);

static int
set_exp_type(mvc *sql, sql_subtype *type, sql_rel *rel, sql_exp *e)
{
	if (mvc_highwater(sql)) {
		(void) sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return -1;
	}
	if (e->type == e_column) {
		const char *nrname = (const char*) e->l, *nename = (const char*) e->r;
		/* find all the column references and set the type */
		e->tpe = *type;
		assert(e->nid);
		return rel_find_parameter(sql, type, rel, e->nid, nrname, nename);
	} else if (e->type == e_atom && e->f) {
		list *atoms = e->f;
		if (!list_empty(atoms))
			for (node *n = atoms->h; n; n = n->next)
				if (set_exp_type(sql, type, rel, n->data) < 0) /* set recursively */
					return -1;
		e->tpe = *type;
		return 1; /* on a list of atoms, everything should be found */
	} else if (e->type == e_atom && !e->l && !e->r && !e->f) {
		e->tpe = *type;
		return set_type_param(sql, type, e->flag) == 0 ? 1 : 0;
	} else if (exp_is_rel(e)) { /* for relation expressions, restart cycle */
		rel = (sql_rel*) e->l;
		/* limiting to these cases */
		if (!is_project(rel->op) || list_length(rel->exps) != 1)
			return 0;
		sql_exp *re = rel->exps->h->data;

		e->tpe = *type;
		return set_exp_type(sql, type, rel, re); /* set recursively */
	}
	return 0;
}

int
rel_set_type_param(mvc *sql, sql_subtype *type, sql_rel *rel, sql_exp *exp, int upcast)
{
	if (!type || !exp || (exp->type != e_atom && exp->type != e_column && !exp_is_rel(exp)))
		return -1;

	/* use largest numeric types */
	if (upcast && type->type->eclass == EC_NUM)
#ifdef HAVE_HGE
		type = sql_fetch_localtype(TYPE_hge);
#else
		type = sql_fetch_localtype(TYPE_lng);
#endif
	else if (upcast && type->type->eclass == EC_FLT)
		type = sql_fetch_localtype(TYPE_dbl);

	/* TODO we could use the sql_query* struct to set parameters used as freevars,
	   but it requires to change a lot of interfaces */
	/* if (is_freevar(exp))
		rel = query_fetch_outer(query, is_freevar(exp)-1); */
	return set_exp_type(sql, type, rel, exp);
}

/* try to do an in-place conversion
 *
 * in-place conversion is only possible if the exp is a variable.
 * This is only done to be able to map more cached queries onto the same
 * interface.
 */
static sql_exp *
exp_convert_inplace(sql_subtype *t, sql_exp *exp)
{
	atom *a, *na;

	/* exclude named variables and variable lists */
	if (exp->type != e_atom || exp->r /* named */ || exp->f /* list */ || !exp->l /* not direct atom */)
		return NULL;

	a = exp->l;
	if (!a->isnull && t->scale && t->type->eclass != EC_FLT)
		return NULL;

	if ((na = atom_cast_inplace(a, t))) {
		exp->l = na;
		return exp;
	}
	return NULL;
}

sql_exp *
exp_numeric_supertype(mvc *sql, sql_exp *e )
{
	sql_subtype *tp = exp_subtype(e);

	if (tp->type->eclass == EC_DEC) {
		sql_subtype *dtp = sql_fetch_localtype(TYPE_dbl);

		return exp_check_type(sql, dtp, NULL, e, type_cast);
	}
	if (tp->type->eclass == EC_NUM) {
#ifdef HAVE_HGE
		sql_subtype *ltp = sql_fetch_localtype(TYPE_hge);
#else
		sql_subtype *ltp = sql_fetch_localtype(TYPE_lng);
#endif

		return exp_check_type(sql, ltp, NULL, e, type_cast);
	}
	return e;
}

sql_exp *
exp_check_type(mvc *sql, sql_subtype *t, sql_rel *rel, sql_exp *exp, check_type tpe)
{
	int c, err = 0;
	sql_exp* nexp = NULL;
	sql_subtype *fromtype = exp_subtype(exp);

	if ((!fromtype || !fromtype->type) && rel_set_type_param(sql, t, rel, exp, 0) == 0)
		return exp;

	/* first try cheap internal (in-place) conversions ! */
	if ((nexp = exp_convert_inplace(t, exp)) != NULL)
		return nexp;

	if (fromtype && subtype_cmp(t, fromtype) != 0) {
		if (EC_INTERVAL(fromtype->type->eclass) && (t->type->eclass == EC_NUM || t->type->eclass == EC_POS) && t->digits < fromtype->digits) {
			err = 1; /* conversion from interval to num depends on the number of digits */
		} else {
			c = sql_type_convert(fromtype->type->eclass, t->type->eclass);
			if (!c || (c == 2 && tpe == type_set) || (c == 3 && tpe != type_cast)) {
				err = 1;
			} else {
				exp = exp_convert(sql, exp, fromtype, t);
			}
		}
	}
	if (err) {
		const char *name = (exp->type == e_column && !has_label(exp)) ? exp_name(exp) : "%";
		sql_exp *res = sql_error( sql, 03, SQLSTATE(42000) "types %s(%u,%u) and %s(%u,%u) are not equal%s%s%s",
			fromtype->type->base.name,
			fromtype->digits,
			fromtype->scale,
			t->type->base.name,
			t->digits,
			t->scale,
			(name[0] != '%' ? " for column '" : ""),
			(name[0] != '%' ? name : ""),
			(name[0] != '%' ? "'" : "")
		);
		return res;
	}
	return exp;
}

list*
exps_check_type(mvc *sql, sql_subtype *t, list *exps)
{
	if (list_empty(exps))
		return exps;
	for(node *n = exps->h; n; n = n->next) {
		n->data = exp_check_type(sql, t, NULL, n->data, type_equal);
		if (!n->data)
			return NULL;
	}
	return exps;
}

sql_exp *
exp_values_set_supertype(mvc *sql, sql_exp *values, sql_subtype *opt_super)
{
	assert(is_values(values));
	list *vals = exp_get_values(values), *nexps;
	sql_subtype *tpe = opt_super?opt_super:exp_subtype(vals->h->data);

	if (!opt_super && tpe)
		values->tpe = *tpe;

	for (node *m = vals->h; m; m = m->next) {
		sql_exp *e = m->data;
		sql_subtype super, *ttpe;

		/* if the expression is a parameter set its type */
		if (tpe && e->type == e_atom && !e->l && !e->r && !e->f && !e->tpe.type) {
			if (set_type_param(sql, tpe, e->flag) == 0)
				e->tpe = *tpe;
			else
				return NULL;
		}
		ttpe = exp_subtype(e);
		if (tpe && ttpe) {
			supertype(&super, ttpe, tpe);
			values->tpe = super;
			tpe = &values->tpe;
		} else {
			tpe = ttpe;
		}
	}

	if (tpe) {
		/* if the expression is a parameter set its type */
		for (node *m = vals->h; m; m = m->next) {
			sql_exp *e = m->data;
			if (e->type == e_atom && !e->l && !e->r && !e->f && !e->tpe.type) {
				if (set_type_param(sql, tpe, e->flag) == 0)
					e->tpe = *tpe;
				else
					return NULL;
			}
		}
		values->tpe = *tpe;
		nexps = sa_list(sql->sa);
		for (node *m = vals->h; m; m = m->next) {
			sql_exp *e = m->data;
			e = exp_check_type(sql, &values->tpe, NULL, e, type_equal);
			if (!e)
				return NULL;
			exp_label(sql->sa, e, ++sql->label);
			append(nexps, e);
		}
		values->f = nexps;
	}
	return values;
}

/* return -1 on error, 0 not found, 1 found */
static int
rel_find_parameter(mvc *sql, sql_subtype *type, sql_rel *rel, int nid, const char *relname, const char *expname)
{
	int res = 0;

	if (mvc_highwater(sql)) {
		(void) sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return -1;
	}
	if (!rel)
		return 0;

	const char *nrname = relname, *nename = expname;
	if (is_project(rel->op) && !list_empty(rel->exps)) {
		sql_exp *e = NULL;

		assert(nid);
		e = exps_bind_nid(rel->exps, nid);
		if (!e)
			return 0; /* not found */
		if (is_mset(rel->op)) { /* TODO for set relations this needs further improvement */
			(void) sql_error(sql, 10, SQLSTATE(42000) "Cannot set parameter types under set relations at the moment");
			return -1;
		}
		/* set order by column types */
		if (is_simple_project(rel->op) && !list_empty(rel->r)) {
			sql_exp *ordere = NULL;
			ordere = exps_bind_nid(rel->r, nid);
			if (ordere && ordere->type == e_column)
				ordere->tpe = *type;
		}
		if (e->type == e_column) {
			nid = e->nid;
			nrname = (const char*) e->l;
			nename = (const char*) e->r;
			e->tpe = *type;
			res = 1; /* found */
		} else if ((e->type == e_atom || exp_is_rel(e)) && (res = set_exp_type(sql, type, rel, e)) <= 0) {
			return res; /* don't search further */
		}
		/* group by columns can have aliases! */
		if (is_groupby(rel->op) && !list_empty(rel->r)) {
			e = exps_bind_nid(rel->r, nid);
			if (!e)
				return res; /* don't search further */
			if (e->type == e_column) {
				nid = e->nid;
				nrname = (const char*) e->l;
				nename = (const char*) e->r;
				e->tpe = *type;
			} else if ((e->type == e_atom || exp_is_rel(e)) && (res = set_exp_type(sql, type, rel, e)) <= 0) {
				return res; /* don't search further */
			}
		}
		if (e->type != e_column)
			return res; /* don't search further */
	}

	switch (rel->op) {
		case op_join:
		case op_left:
		case op_right:
		case op_full:
			if (rel->l)
				res = rel_find_parameter(sql, type, rel->l, nid, nrname, nename);
			if (rel->r && res <= 0) { /* try other relation if not found */
				int err = sql->session->status, lres = res;
				char buf[ERRSIZE];

				strcpy(buf, sql->errstr); /* keep error found and try other join relation */
				sql->session->status = 0;
				sql->errstr[0] = '\0';
				res = rel_find_parameter(sql, type, rel->r, nid, nrname, nename);
				if (res == 0) { /* parameter wasn't found, set error */
					res = lres;
					sql->session->status = err;
					strcpy(sql->errstr, buf);
				}
			}
			break;
		case op_semi:
		case op_anti:
		case op_groupby:
		case op_project:
		case op_select:
		case op_topn:
		case op_sample:
			if (rel->l)
				res = rel_find_parameter(sql, type, rel->l, nid, nrname, nename);
			break;
		case op_inter:
		case op_except:
		case op_munion:	{
			(void) sql_error(sql, 10, SQLSTATE(42000) "Cannot set parameter types under set relations at the moment");
			return -1;
		}
		default: /* For table returning functions, the type must be set when the relation is created */
			return 0;
	}
	return res;
}

sql_exp *
list_find_exp( list *exps, sql_exp *e)
{
	if (e->type != e_column)
		return NULL;
	return exps_bind_nid(exps, e->nid);
}
