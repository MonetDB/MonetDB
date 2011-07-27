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

#include <monetdb_config.h>
#include "sql_semantic.h"
#include "rel_semantic.h"
#include "rel_exp.h"

static sql_exp * 
exp_create(sql_allocator *sa, int type ) 
{
	sql_exp *e = SA_NEW(sa, sql_exp);

	e->name = NULL;
	e->rname = NULL;
	e->card = 0;
	e->flag = 0;
	e->l = e->r = NULL;
	e->type = (expression_type)type;
	e->f = NULL;
	e->p = NULL;
	e->used = 0;
	e->tpe.type = NULL;
	e->tpe.comp_type = NULL;
	e->tpe.digits = e->tpe.scale = 0;
	return e;
}

sql_exp * 
exp_compare(sql_allocator *sa, sql_exp *l, sql_exp *r, int cmptype) 
{
	sql_exp *e = exp_create(sa, e_cmp);
	e->card = l->card;
	e->l = l;
	e->r = r;
	e->flag = cmptype;
	return e;
}

sql_exp * 
exp_compare2(sql_allocator *sa, sql_exp *l, sql_exp *r, sql_exp *h, int cmptype) 
{
	sql_exp *e = exp_create(sa, e_cmp);
	e->card = l->card;
	e->l = l;
	e->r = r;
	if (h)
		e->f = h;
	e->flag = cmptype;
	return e;
}

sql_exp *
exp_or(sql_allocator *sa, list *l, list *r)
{
	sql_exp *f = NULL;
	sql_exp *e = exp_create(sa, e_cmp);
	
	f = l->h?l->h->data:r->h?r->h->data:NULL;
	e->card = l->h?exps_card(l):exps_card(r);
	e->l = l;
	e->r = r;
	assert(f);
	e->f = f;
	e->flag = cmp_or;
	return e;
}

sql_exp *
exp_in(sql_allocator *sa, sql_exp *l, list *r, int cmptype)
{
	sql_exp *e = exp_create(sa, e_cmp);
	
	e->card = l->card;
	e->l = l;
	e->r = r;
	assert( cmptype == cmp_in || cmptype == cmp_notin);
	e->flag = cmptype;
	return e;
}


static sql_subtype*
dup_subtype(sql_allocator *sa, sql_subtype *st)
{
	sql_subtype *res = SA_NEW(sa, sql_subtype);

	*res = *st;
	return res;
}

sql_exp * 
exp_convert(sql_allocator *sa, sql_exp *exp, sql_subtype *fromtype, sql_subtype *totype )
{
	sql_exp *e = exp_create(sa, e_convert);
	e->card = exp->card;
	e->l = exp;
	totype = dup_subtype(sa, totype);
	e->r = append(append(list_new(sa), dup_subtype(sa, fromtype)),totype);
	e->tpe = *totype; 
	if (exp->name)
		e->name = sa_strdup(sa, exp->name);
	if (exp->rname)
		e->rname = sa_strdup(sa, exp->rname);
	return e;
}

sql_exp * 
exp_op( sql_allocator *sa, list *l, sql_subfunc *f )
{
	sql_exp *e = exp_create(sa, e_func);
	e->card = exps_card(l);
	if (!l || list_length(l) == 0)
		e->card = CARD_ATOM; /* unop returns a single atom */
	e->l = l;
	e->f = f; 
	return e;
}

sql_exp * 
exp_aggr( sql_allocator *sa, list *l, sql_subaggr *a, int distinct, int no_nils, int card, int has_nils )
{
	sql_exp *e = exp_create(sa, e_aggr);
	e->card = card;
	e->l = l;
	e->f = a; 
	if (distinct)
		set_distinct(e);
	if (no_nils)
		set_no_nil(e);
	if (!has_nils)
		set_has_no_nil(e);
	return e;
}

sql_exp * 
exp_atom(sql_allocator *sa, atom *a) 
{
	sql_exp *e = exp_create(sa, e_atom);
	e->card = CARD_ATOM;
	e->l = a;
	return e;
}

sql_exp *
exp_atom_bool(sql_allocator *sa, int b) 
{
	sql_subtype bt; 

	sql_find_subtype(&bt, "boolean", 0, 0);
	if (b) 
		return exp_atom(sa, atom_bool(sa, &bt, TRUE ));
	else
		return exp_atom(sa, atom_bool(sa, &bt, FALSE ));
}

sql_exp *
exp_atom_int(sql_allocator *sa, int i) 
{
	sql_subtype it; 

	sql_find_subtype(&it, "int", 9, 0);
	return exp_atom(sa, atom_int(sa, &it, i ));
}

sql_exp *
exp_atom_lng(sql_allocator *sa, lng i) 
{
	sql_subtype it; 

	sql_find_subtype(&it, "bigint", 19, 0);
	return exp_atom(sa, atom_int(sa, &it, (lng)i ));
}

sql_exp *
exp_atom_wrd(sql_allocator *sa, wrd w) 
{
	sql_subtype it; 

	sql_find_subtype(&it, "wrd", 19, 0);
	return exp_atom(sa, atom_int(sa, &it, (lng)w ));
}

sql_exp *
exp_atom_str(sql_allocator *sa, str s, sql_subtype *st) 
{
	return exp_atom(sa, atom_string(sa, st, s?sa_strdup(sa, s):NULL));
}

sql_exp *
exp_atom_clob(sql_allocator *sa, str s) 
{
	sql_subtype clob;

	sql_find_subtype(&clob, "clob", 0, 0);
	return exp_atom(sa, atom_string(sa, &clob, s?sa_strdup(sa, s):NULL));
}

sql_exp *
exp_atom_ptr(sql_allocator *sa, void *s) 
{
	sql_subtype *t = sql_bind_localtype("ptr");
	return exp_atom(sa, atom_ptr(sa, t, s));
}

sql_exp * 
exp_atom_ref(sql_allocator *sa, int i, sql_subtype *tpe) 
{
	sql_exp *e = exp_create(sa, e_atom);
	e->card = CARD_ATOM;
	e->flag = i;
	if (tpe)
		e->tpe = *tpe;
	return e;
}

atom *
exp_value(sql_exp *e, atom **args, int maxarg)
{
	if (!e || e->type != e_atom)
		return NULL; 
	if (e->l) {	   /* literal */
		return e->l;
	} else if (e->r) { /* param (ie not set) */
		return NULL; 
	} else if (e->flag < maxarg) {
		return args[e->flag]; 
	}
	return NULL; 
}

sql_exp * 
exp_param(sql_allocator *sa, char *name, sql_subtype *tpe, int frame) 
{
	sql_exp *e = exp_create(sa, e_atom);
	e->r = sa_strdup(sa, name);
	e->card = CARD_ATOM;
	e->flag = frame;
	if (tpe)
		e->tpe = *tpe;
	return e;
}

sql_exp * 
exp_values(sql_allocator *sa, list *exps) 
{
	sql_exp *e = exp_create(sa, e_atom);
	e->card = CARD_MULTI;
	e->f = exps;
	return e;
}

sql_exp * 
exp_alias(sql_allocator *sa, char *arname, char *acname, char *org_rname, char *org_cname, sql_subtype *t, int card, int has_nils, int intern) 
{
	sql_exp *e = exp_create(sa, e_column);

	assert(acname && org_cname);
	e->card = card;
	e->rname = (arname)?sa_strdup(sa, arname):(org_rname)?sa_strdup(sa, org_rname):NULL;
	e->name = sa_strdup(sa, acname);
	e->l = (org_rname)?sa_strdup(sa, org_rname):NULL;
	e->r = sa_strdup(sa, org_cname);
	if (t)
		e->tpe = *t;
	if (!has_nils)
		set_has_no_nil(e);
	if (intern)
		set_intern(e);
	return e;
}

sql_exp * 
exp_column(sql_allocator *sa, char *rname, char *cname, sql_subtype *t, int card, int has_nils, int intern) 
{
	sql_exp *e = exp_create(sa, e_column);

	assert(cname);
	e->card = card;
	e->name = sa_strdup(sa, cname);
	e->l = (rname)?sa_strdup(sa, rname):NULL;
	e->r = sa_strdup(sa, cname);
	if (t)
		e->tpe = *t;
	if (!has_nils)
		set_has_no_nil(e);
	if (intern)
		set_intern(e);
	return e;
}

/* Set a name (alias) for the expression, such that we can refer 
   to this expression by this simple name.
 */
void 
exp_setname(sql_allocator *sa, sql_exp *e, char *rname, char *name )
{
	if (name) 
		e->name = sa_strdup(sa, name);
	e->rname = (rname)?sa_strdup(sa, rname):NULL;
}

str
number2name(str s, int len, int i)
{
	s[--len] = 0;
	while(i>0) {
		s[--len] = '0' + (i & 7);
		i >>= 3;
	}
	s[--len] = 'L';
	return s + len;
}

sql_exp*
exp_label(sql_allocator *sa, sql_exp *e, int nr)
{
	char name[16], *nme;

	nme = number2name(name, 16, nr);
	assert(e->name == NULL);
	e->name = sa_strdup(sa, nme);
	return e;
}

void
exp_swap( sql_exp *e ) 
{
	sql_exp *s = e->l;

	e->l = e->r;
	e->r = s;
	e->flag = swap_compare((comp_type)e->flag);
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
		}
		break;
	}
	case e_convert:
	case e_column:
		if (e->tpe.type)
			return &e->tpe;
		break;
	case e_aggr: {
		sql_subaggr *a = e->f;
		return &a->res;
	}
	case e_func: {
		if (e->f) {
			sql_subfunc *f = e->f;
			return &f->res;
		}
		return NULL;
	}
	case e_cmp:
		/* return bit */
	default:
		return NULL;
	}
	return NULL;
}

char *
exp_name( sql_exp *e )
{
	if (e->name)
		return e->name;
	if (e->type == e_convert && e->l)
		return exp_name(e->l);
	return NULL;
}

char *
exp_relname( sql_exp *e )
{
	if (e->rname)
		return e->rname;
	if (e->type == e_column && e->l)
		return e->l;
	return NULL;
}

char *
exp_func_name( sql_exp *e )
{
	if (e->type == e_func && e->f) {
		sql_subfunc *f = e->f;
		return f->func->base.name;
	}
	if (e->name)
		return e->name;
	if (e->type == e_convert && e->l)
		return exp_name(e->l);
	return NULL;
}


char *
exp_find_rel_name(sql_exp *e)
{
	switch(e->type) {
	case e_column:
		if (e->l)
			return e->l;
		break;
	case e_convert:
		return exp_find_rel_name(e->l);
	default:
		return NULL;
	}
	return NULL;
}

int 
exp_cmp( sql_exp *e1, sql_exp *e2)
{
	return (e1 == e2)?0:-1;
}

int 
exp_match( sql_exp *e1, sql_exp *e2)
{
	if (exp_cmp(e1, e2) == 0)
		return 1;
	if (e1->type == e2->type && e1->type == e_column) {
		if (!e1->name || !e2->name || strcmp(e1->name, e2->name) != 0)
			return 0;
		if (!e1->l || !e2->l || strcmp(e1->l, e2->l) != 0)
			return 0;
		/* e1->r */
		return 1;
	}
	return 0;
}

int
exp_match_col_exps( sql_exp *e, list *l)
{
	node *n;

	for(n=l->h; n; n = n->next) {
		sql_exp *re = n->data;
		sql_exp *re_r = re->r;
	
		if (re->type == e_cmp && re->flag == cmp_or)
			return exp_match_col_exps(e, re->l) &&
			       exp_match_col_exps(e, re->r); 

		if (re->type != e_cmp || /*re->flag != cmp_equal ||*/ !re_r || re_r->card != 1 || !exp_match_exp(e, re->l)) 
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

	if (!is_complex_exp(e1->flag) && e1_r && e1_r->card == 1 &&
	    !is_complex_exp(e2->flag) && e2_r && e2_r->card == 1)
		return exp_match_exp(e1->l, e2->l);

	if (!is_complex_exp(e1->flag) && e1_r && e1_r->card == 1 &&
	    e2->flag == cmp_or)
 		return exp_match_col_exps(e1->l, e2->l) &&
 		       exp_match_col_exps(e1->l, e2->r); 

	if (e1->flag == cmp_or &&
	    !is_complex_exp(e2->flag) && e2_r && e2_r->card == 1)
 		return exp_match_col_exps(e2->l, e1->l) &&
 		       exp_match_col_exps(e2->l, e1->r); 

	if (e1->flag == cmp_or && e2->flag == cmp_or) {
		list *l = e1->l, *r = e1->r;	
		sql_exp *el = l->h->data;
		sql_exp *er = r->h->data;

		return list_length(l) == 1 && list_length(r) == 1 &&
		       exps_match_col_exps(el, e2) &&
		       exps_match_col_exps(er, e2);
	}
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
	if (list_length(l) != list_length(r))
		return 0;
	lu = alloca(list_length(l));
	ru = alloca(list_length(r));
	memset(lu, 0, list_length(l));
	memset(ru, 0, list_length(r));
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
	return match;
}

int 
exp_match_exp( sql_exp *e1, sql_exp *e2)
{
	if (exp_match(e1, e2))
		return 1;
	if (e1->type == e2->type) { 
		switch(e1->type) {
		case e_cmp:
			if (e1->flag == e2->flag && !is_complex_exp(e1->flag) &&
		            exp_match_exp(e1->l, e2->l) && 
			    exp_match_exp(e1->r, e2->r) && 
			    ((!e1->f && !e2->f) || exp_match_exp(e1->f, e2->f)))
				return 1;
			else if (e1->flag == e2->flag && e1->flag == cmp_or &&
		            exp_match_list(e1->l, e2->l) && 
			    exp_match_list(e1->r, e2->r))
				return 1;
			break;
		case e_convert:
			if (!subtype_cmp(exp_totype(e1), exp_totype(e2)) &&
			    !subtype_cmp(exp_fromtype(e1), exp_fromtype(e2)) &&
			    exp_match_exp(e1->l, e2->l))
				return 1;
			break;
		case e_aggr:
			if (!subaggr_cmp(e1->f, e2->f) && /* equal aggregation*/
			    exp_match_list(e1->l, e2->l) && 
			    e1->flag == e2->flag)
				return 1;
			break;
		case e_func:
			if (!subfunc_cmp(e1->f, e2->f) && /* equal functions */
			    exp_match_list(e1->l, e2->l))
				return 1;
			break;
		case e_atom:
			if (e1->l && e2->l && !atom_cmp(e1->l, e2->l))
				return 1;
			break;
		default:
			break;
		}
	}
	return 0;
}

static int
exps_are_joins( list *l )
{
	node *n;

	for (n = l->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (exp_is_join_exp(e))
			return -1;
	}
	return 0;
}

int
exp_is_join_exp(sql_exp *e)
{
	if (exp_is_join(e) == 0)
		return 0;
	if (e->type == e_cmp && e->flag == cmp_or && e->card >= CARD_AGGR)
		if (exps_are_joins(e->l) == 0 && exps_are_joins(e->r) == 0)
			return 0;
	return -1;
}

static int
exp_is_complex_select( sql_exp *e )
{
	switch (e->type) {
	case e_atom:
		return 0;
	case e_convert:
		return exp_is_complex_select(e->l);
	case e_func:
	case e_aggr:
	{	
		int r = (e->card == CARD_ATOM);
		node *n;
		list *l = e->l;

		if (r && l)
			for (n = l->h; n; n = n->next) 
				r |= exp_is_complex_select(n->data);
		return r;
	}
	case e_column:
	case e_cmp:
		return 0;
	}
	return 0;
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
distinct_rel(sql_exp *e, char **rname)
{
	char *e_rname = NULL;

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
exp_is_rangejoin(sql_exp *e)
{
	/* assume e is a e_cmp with 3 args 
	 * Need to check e->r and e->f only touch one table.
	 */
	char *rname = 0;

	if (distinct_rel(e->r, &rname) && distinct_rel(e->f, &rname))
		return 0;
	return -1;
}

int
exp_is_join(sql_exp *e)
{
	/* only simple compare expressions, ie not or lists
		or range expressions (e->f)
	 */ 
	if (e->type == e_cmp && !is_complex_exp(e->flag) && e->l && e->r && !e->f && e->card >= CARD_AGGR && !complex_select(e))
		return 0;
	/* range expression */
	if (e->type == e_cmp && !is_complex_exp(e->flag) && e->l && e->r && e->f && e->card >= CARD_AGGR && !complex_select(e)) 
		return exp_is_rangejoin(e);
	return -1;
}

int
exp_is_eqjoin(sql_exp *e)
{
	if (e->flag == cmp_equal)
		return 0;
	return -1; 
}

static sql_exp *
rel_find_exp_( sql_rel *rel, sql_exp *e) 
{
	sql_exp *ne = NULL;

	switch(e->type) {
	case e_column:
		if (e->l) {
			ne = exps_bind_column2(rel->exps, e->l, e->r);
		} else {
			ne = exps_bind_column(rel->exps, e->r, NULL);
		}
		return ne;
	case e_convert:
		return rel_find_exp_(rel, e->l);
	case e_aggr:
	case e_func: 
		if (e->l) {
			list *l = e->l;
			node *n = l->h;
	
			ne = n->data;
			while (ne != NULL && n != NULL) {
				ne = rel_find_exp_(rel, n->data);
				n = n->next;
			}
			return ne;
		}
	case e_cmp:	
		return NULL;
	case e_atom:
		return e;
	}
	return ne;
}

sql_exp *
rel_find_exp( sql_rel *rel, sql_exp *e)
{
	sql_exp *ne = rel_find_exp_(rel, e);
	if (!ne) {
		switch(rel->op) {
		case op_left:
		case op_right:
		case op_full:
		case op_join:
			ne = rel_find_exp(rel->l, e);
			if (!ne) 
				ne = rel_find_exp(rel->r, e);
			break;
		case op_table:
			if (rel->exps && e->type == e_column && e->l && exps_bind_column2(rel->exps, e->l, e->r)) 
				ne = e;
			break;
		case op_union:
		case op_except:
		case op_inter:
		{
			if (rel->l)
				ne = rel_find_exp(rel->l, e);
			else if (rel->exps && e->l)
				ne = exps_bind_column2(rel->exps, e->l, e->r);
			else if (rel->exps)
				ne = exps_bind_column(rel->exps, e->r, NULL);
		}
		break;
		case op_basetable: 
			if (rel->exps && e->type == e_column && e->l) 
				ne = exps_bind_column2(rel->exps, e->l, e->r);
			break;
		default:
			if (!is_project(rel->op) && rel->l)
				ne = rel_find_exp(rel->l, e);
		}
	}
	return ne;
}

int 
exp_is_correlation(sql_exp *e, sql_rel *r )
{
	if (e->type == e_cmp && !is_complex_exp(e->flag)) {
		sql_exp *le = rel_find_exp(r->l, e->l);
		sql_exp *re = rel_find_exp(r->r, e->r);

		if (le && re)
			return 0;
		le = rel_find_exp(r->r, e->l);
		re = rel_find_exp(r->l, e->r);
		if (le && re) {
			/* for future processing we depend on 
			   the correct order of the expression, ie swap here */
			exp_swap(e);
			return 0;
		}
	}
	return -1;
}

int
exp_is_atom( sql_exp *e )
{
	switch (e->type) {
	case e_atom:
		return 1;
	case e_convert:
		return exp_is_atom(e->l);
	case e_func:
	case e_aggr:
	{	
		int r = (e->card == CARD_ATOM);
		node *n;
		list *l = e->l;

		if (r && l)
			for (n = l->h; n; n = n->next) 
				r &= exp_is_atom(n->data);
		return r;
	}
	case e_column:
	case e_cmp:
		return 0;
	}
	return 0;
}

sql_exp *
exps_bind_column( list *exps, char *cname, int *ambiguous ) 
{
	sql_exp *e = NULL;

	if (exps && cname) {
		node *en;

		for (en = exps->h; en; en = en->next ) {
			sql_exp *ce = en->data;
			if (ce->name && strcmp(ce->name, cname) == 0) {
				if (e) {
					if (ambiguous)
						*ambiguous = 1;
					return NULL;
				}
				e = ce;
			}
		}
	}
	return e;
}

sql_exp *
exps_bind_column2( list *exps, char *rname, char *cname ) 
{
	if (exps) {
		node *en;

		for (en = exps->h; en; en = en->next ) {
			sql_exp *e = en->data;
		
			if (e && is_column(e->type) && e->name && e->rname && strcmp(e->name, cname) == 0 && strcmp(e->rname, rname) == 0)
				return e;
			if (e && e->type == e_column && e->name && !e->rname && e->l && strcmp(e->name, cname) == 0 && strcmp(e->l, rname) == 0)
				return e;
			if (e && e->type == e_column && !e->name && !e->rname && e->l && e->r && strcmp(e->r, cname) == 0 && strcmp(e->l, rname) == 0)
				return e;
		}
	}
	return NULL;
}

int
exps_card( list *l ) 
{
	node *n;
	int card = 0;

	if (l) for(n = l->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (card < e->card)
			card = e->card;
	}
	return card;
}
	
void
exps_fix_card( list *exps, int card)
{
	node *n;

	for (n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->card > card)
			e->card = card;
	}
}

int
exps_intern(list *exps)
{
	node *n;
			
	for (n=exps->h; n; n = n->next) {
		sql_exp *e = n->data;		

		if (is_intern(e))
			return 1;
	}
	return 0;
}

char *
compare_func( comp_type t )
{
	switch(t) {
	case cmp_equal:
		return "=";
	case cmp_lt:
		return "<";
	case cmp_lte:
		return "<=";
	case cmp_gte:
		return ">=";
	case cmp_gt:
		return ">";
	case cmp_notequal:
		return "<>";
	default:
		return NULL;
	}
}

