/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_optimizer_private.h"
#include "rel_select.h"
#include "rel_exp.h"
#include "rel_rewriter.h"

static inline int
str_ends_with(const char *s, const char *suffix)
{
	size_t slen = strlen(s), suflen = strlen(suffix);
	if (suflen > slen)
		return 1;
	return strncmp(s + slen - suflen, suffix, suflen);
}

static sql_exp *
exp_simplify_math( mvc *sql, sql_exp *e, int *changes)
{
	if (e->type == e_func || e->type == e_aggr) {
		list *l = e->l;
		sql_subfunc *f = e->f;
		node *n;
		sql_exp *le;

		if (list_length(l) < 1)
			return e;

		/* if the function has no null semantics we can return NULL if one of the arguments is NULL */
		if (!f->func->semantics && f->func->type != F_PROC) {
			for (node *n = l->h ; n ; n = n->next) {
				sql_exp *arg = n->data;

				if (exp_is_atom(arg) && exp_is_null(arg)) {
					sql_exp *ne = exp_null(sql->sa, exp_subtype(e));
					(*changes)++;
					if (exp_name(e))
						exp_prop_alias(sql->sa, ne, e);
					return ne;
				}
			}
		}
		if (!f->func->s && list_length(l) == 2 && str_ends_with(sql_func_imp(f->func), "_no_nil") == 0) {
			sql_exp *le = l->h->data;
			sql_exp *re = l->h->next->data;

			/* if "_no_nil" is in the name of the
			 * implementation function (currently either
			 * min_no_nil or max_no_nil), in which case we
			 * ignore the NULL and return the other value */

			if (exp_is_atom(le) && exp_is_null(le)) {
				(*changes)++;
				if (exp_name(e))
					exp_prop_alias(sql->sa, re, e);
				return re;
			}
			if (exp_is_atom(re) && exp_is_null(re)) {
				(*changes)++;
				if (exp_name(e))
					exp_prop_alias(sql->sa, le, e);
				return le;
			}
		}

		le = l->h->data;
		if (!EC_COMPUTE(exp_subtype(le)->type->eclass) && exp_subtype(le)->type->eclass != EC_DEC)
			return e;

		if (!f->func->s && !strcmp(f->func->base.name, "sql_mul") && list_length(l) == 2) {
			sql_exp *le = l->h->data;
			sql_exp *re = l->h->next->data;
			sql_subtype *et = exp_subtype(e);

			/* 0*a = 0 */
			if (exp_is_atom(le) && exp_is_zero(le) && exp_is_atom(re) && !has_nil(re)) {
				(*changes)++;
				le = exp_zero(sql->sa, et);
				if (subtype_cmp(exp_subtype(e), exp_subtype(le)) != 0)
					le = exp_convert(sql->sa, le, exp_subtype(le), exp_subtype(e));
				if (exp_name(e))
					exp_prop_alias(sql->sa, le, e);
				return le;
			}
			/* a*0 = 0 */
			if (exp_is_atom(re) && exp_is_zero(re) && exp_is_atom(le) && !has_nil(le)) {
				(*changes)++;
				re = exp_zero(sql->sa, et);
				if (subtype_cmp(exp_subtype(e), exp_subtype(re)) != 0)
					re = exp_convert(sql->sa, re, exp_subtype(re), exp_subtype(e));
				if (exp_name(e))
					exp_prop_alias(sql->sa, re, e);
				return re;
			}
			if (exp_is_atom(le) && exp_is_atom(re)) {
				atom *la = exp_flatten(sql, true, le);
				atom *ra = exp_flatten(sql, true, re);

				if (la && ra && subtype_cmp(atom_type(la), atom_type(ra)) == 0 && subtype_cmp(atom_type(la), exp_subtype(e)) == 0) {
					atom *a = atom_mul(sql->sa, la, ra);

					if (a && (a = atom_cast(sql->sa, a, exp_subtype(e)))) {
						sql_exp *ne = exp_atom(sql->sa, a);
						if (subtype_cmp(exp_subtype(e), exp_subtype(ne)) != 0)
							ne = exp_convert(sql->sa, ne, exp_subtype(ne), exp_subtype(e));
						(*changes)++;
						if (exp_name(e))
							exp_prop_alias(sql->sa, ne, e);
						return ne;
					}
				}
			}
			/* change a*pow(a,n) or pow(a,n)*a into pow(a,n+1) */
			if (is_func(le->type)) {
				list *l = le->l;
				sql_subfunc *f = le->f;

				if (!f->func->s && !strcmp(f->func->base.name, "power") && list_length(l) == 2) {
					sql_exp *lle = l->h->data;
					sql_exp *lre = l->h->next->data;
					if (exp_equal(re, lle)==0) {
						atom *a = exp_value(sql, lre);
						if (a && (a = atom_inc(sql->sa, a))) {
							lre->l = a;
							lre->r = NULL;
							if (subtype_cmp(exp_subtype(e), exp_subtype(le)) != 0)
								le = exp_convert(sql->sa, le, exp_subtype(le), exp_subtype(e));
							(*changes)++;
							if (exp_name(e))
								exp_prop_alias(sql->sa, le, e);
							return le;
						}
					}
				}
				if (!f->func->s && !strcmp(f->func->base.name, "sql_mul") && list_length(l) == 2) {
					sql_exp *lle = l->h->data;
					sql_exp *lre = l->h->next->data;
					if (!exp_is_atom(lle) && exp_is_atom(lre) && exp_is_atom(re)) {
						/* (x*c1)*c2 -> x * (c1*c2) */
						sql_exp *ne = NULL;

						if (!(le = rel_binop_(sql, NULL, lre, re, "sys", "sql_mul", card_value))) {
							sql->session->status = 0;
							sql->errstr[0] = '\0';
							return e; /* error, fallback to original expression */
						}
						if (!(ne = rel_binop_(sql, NULL, lle, le, "sys", "sql_mul", card_value))) {
							sql->session->status = 0;
							sql->errstr[0] = '\0';
							return e; /* error, fallback to original expression */
						}
						if (subtype_cmp(exp_subtype(e), exp_subtype(ne)) != 0)
							ne = exp_convert(sql->sa, ne, exp_subtype(ne), exp_subtype(e));
						(*changes)++;
						if (exp_name(e))
							exp_prop_alias(sql->sa, ne, e);
						return ne;
					}
				}
			}
		}
		if (!f->func->s && !strcmp(f->func->base.name, "sql_add") && list_length(l) == 2) {
			sql_exp *le = l->h->data;
			sql_exp *re = l->h->next->data;
			if (exp_is_atom(le) && exp_is_zero(le)) {
				if (subtype_cmp(exp_subtype(e), exp_subtype(re)) != 0)
					re = exp_convert(sql->sa, re, exp_subtype(re), exp_subtype(e));
				(*changes)++;
				if (exp_name(e))
					exp_prop_alias(sql->sa, re, e);
				return re;
			}
			if (exp_is_atom(re) && exp_is_zero(re)) {
				if (subtype_cmp(exp_subtype(e), exp_subtype(le)) != 0)
					le = exp_convert(sql->sa, le, exp_subtype(le), exp_subtype(e));
				(*changes)++;
				if (exp_name(e))
					exp_prop_alias(sql->sa, le, e);
				return le;
			}
			if (exp_is_atom(le) && exp_is_atom(re)) {
				atom *la = exp_flatten(sql, true, le);
				atom *ra = exp_flatten(sql, true, re);

				if (la && ra) {
					atom *a = atom_add(sql->sa, la, ra);

					if (a) {
						sql_exp *ne = exp_atom(sql->sa, a);
						if (subtype_cmp(exp_subtype(e), exp_subtype(ne)) != 0)
							ne = exp_convert(sql->sa, ne, exp_subtype(ne), exp_subtype(e));
						(*changes)++;
						if (exp_name(e))
							exp_prop_alias(sql->sa, ne, e);
						return ne;
					}
				}
			}
			if (is_func(le->type)) {
				list *ll = le->l;
				sql_subfunc *f = le->f;
				if (!f->func->s && !strcmp(f->func->base.name, "sql_add") && list_length(ll) == 2) {
					sql_exp *lle = ll->h->data;
					sql_exp *lre = ll->h->next->data;

					if (exp_is_atom(lle) && exp_is_atom(lre))
						return e;
					if (!exp_is_atom(re) && exp_is_atom(lre)) {
						/* (x+c1)+y -> (x+y) + c1 */
						ll->h->next->data = re;
						l->h->next->data = lre;
						if (!(l->h->data = exp_simplify_math(sql, le, changes)))
							return NULL;
						(*changes)++;
						return e;
					}
					if (exp_is_atom(re) && exp_is_atom(lre)) {
						/* (x+c1)+c2 -> (c2+c1) + x */
						ll->h->data = re;
						l->h->next->data = lle;
						if (!(l->h->data = exp_simplify_math(sql, le, changes)))
							return NULL;
						(*changes)++;
						return e;
					}
				}
			}
		}
		if (!f->func->s && !strcmp(f->func->base.name, "sql_sub") && list_length(l) == 2) {
			sql_exp *le = l->h->data;
			sql_exp *re = l->h->next->data;

			if (exp_is_atom(le) && exp_is_atom(re)) {
				atom *la = exp_flatten(sql, true, le);
				atom *ra = exp_flatten(sql, true, re);

				if (la && ra) {
					atom *a = atom_sub(sql->sa, la, ra);

					if (a) {
						sql_exp *ne = exp_atom(sql->sa, a);
						if (subtype_cmp(exp_subtype(e), exp_subtype(ne)) != 0)
							ne = exp_convert(sql->sa, ne, exp_subtype(ne), exp_subtype(e));
						(*changes)++;
						if (exp_name(e))
							exp_prop_alias(sql->sa, ne, e);
						return ne;
					}
				}
			}
			if (!has_nil(le) && !has_nil(re) && exp_equal(le,re) == 0) { /* a - a = 0 */
				atom *a;
				sql_exp *ne;

				if (exp_subtype(le)->type->eclass == EC_NUM) {
					a = atom_int(sql->sa, exp_subtype(le), 0);
				} else if (exp_subtype(le)->type->eclass == EC_FLT) {
					a = atom_float(sql->sa, exp_subtype(le), 0);
				} else {
					return e;
				}
				ne = exp_atom(sql->sa, a);
				if (subtype_cmp(exp_subtype(e), exp_subtype(ne)) != 0)
					ne = exp_convert(sql->sa, ne, exp_subtype(ne), exp_subtype(e));
				(*changes)++;
				if (exp_name(e))
					exp_prop_alias(sql->sa, ne, e);
				return ne;
			}
			if (is_func(le->type)) {
				list *ll = le->l;
				sql_subfunc *f = le->f;
				if (!f->func->s && !strcmp(f->func->base.name, "sql_add") && list_length(ll) == 2) {
					sql_exp *lle = ll->h->data;
					sql_exp *lre = ll->h->next->data;
					if (exp_equal(re, lre) == 0) {
						/* (x+a)-a = x*/
						if (subtype_cmp(exp_subtype(e), exp_subtype(lle)) != 0)
							lle = exp_convert(sql->sa, lle, exp_subtype(lle), exp_subtype(e));
						if (exp_name(e))
							exp_prop_alias(sql->sa, lle, e);
						(*changes)++;
						return lle;
					}
					if (exp_is_atom(lle) && exp_is_atom(lre))
						return e;
					if (!exp_is_atom(re) && exp_is_atom(lre)) {
						/* (x+c1)-y -> (x-y) + c1 */
						ll->h->next->data = re;
						l->h->next->data = lre;
						le->f = e->f;
						e->f = f;
						if (!(l->h->data = exp_simplify_math(sql, le, changes)))
							return NULL;
						(*changes)++;
						return e;
					}
					if (exp_is_atom(re) && exp_is_atom(lre)) {
						/* (x+c1)-c2 -> (c1-c2) + x */
						ll->h->data = lre;
						ll->h->next->data = re;
						l->h->next->data = lle;
						le->f = e->f;
						e->f = f;
						if (!(l->h->data = exp_simplify_math(sql, le, changes)))
							return NULL;
						(*changes)++;
						return e;
					}
				}
			}
		}
		if (l)
			for (n = l->h; n; n = n->next)
				if (!(n->data = exp_simplify_math(sql, n->data, changes)))
					return NULL;
	}
	if (e->type == e_convert)
		if (!(e->l = exp_simplify_math(sql, e->l, changes)))
			return NULL;
	return e;
}

static inline sql_rel *
rel_simplify_math_(visitor *v, sql_rel *rel)
{
	if ((is_simple_project(rel->op) || (rel->op == op_ddl && rel->flag == ddl_psm)) && rel->exps) {
		int needed = 0, ochanges = 0;

		for (node *n = rel->exps->h; n && !needed; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_func || e->type == e_convert || e->type == e_aggr || e->type == e_psm)
				needed = 1;
		}
		if (!needed)
			return rel;

		for (node *n = rel->exps->h; n; n = n->next) {
			sql_exp *ne = exp_simplify_math(v->sql, n->data, &ochanges);

			if (!ne)
				return NULL;
			n->data = ne;
		}
		v->changes += ochanges;
	}
	return rel;
}

static sql_rel *
rel_simplify_math(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_visitor_bottomup(v, rel, &rel_simplify_math_);
}

run_optimizer
bind_simplify_math(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_cycle == 0 && gp->opt_level == 1 && v->value_based_opt && (gp->cnt[op_project]
		   || gp->cnt[op_ddl] || gp->cnt[ddl_psm]) && (flag & simplify_math) ? rel_simplify_math : NULL;
}


/*
 * Casting decimal values on both sides of a compare expression is expensive,
 * both in preformance (cpu cost) and memory requirements (need for large
 * types).
 */

#define reduce_scale_tpe(tpe, uval) \
	do { \
		tpe v = uval; \
		if (v != 0) { \
			while( (v/10)*10 == v ) { \
				i++; \
				v /= 10; \
			} \
			nval = v; \
		} \
	} while (0)

atom *
reduce_scale(mvc *sql, atom *a)
{
	int i = 0;
	atom *na = a;
#ifdef HAVE_HGE
	hge nval = 0;
#else
	lng nval = 0;
#endif

#ifdef HAVE_HGE
	if (a->data.vtype == TYPE_hge) {
		reduce_scale_tpe(hge, a->data.val.hval);
	} else
#endif
	if (a->data.vtype == TYPE_lng) {
		reduce_scale_tpe(lng, a->data.val.lval);
	} else if (a->data.vtype == TYPE_int) {
		reduce_scale_tpe(int, a->data.val.ival);
	} else if (a->data.vtype == TYPE_sht) {
		reduce_scale_tpe(sht, a->data.val.shval);
	} else if (a->data.vtype == TYPE_bte) {
		reduce_scale_tpe(bte, a->data.val.btval);
	}
	if (i) {
		na = atom_int(sql->sa, &a->tpe, nval);
		if (na->tpe.scale)
			na->tpe.scale -= i;
	}
	return na;
}

static inline sql_exp *
rel_simplify_predicates(visitor *v, sql_rel *rel, sql_exp *e)
{
	if (is_func(e->type) && list_length(e->l) == 3 && is_case_func((sql_subfunc*)e->f) /*is_ifthenelse_func((sql_subfunc*)e->f)*/) {
		list *args = e->l;
		sql_exp *ie = args->h->data;

		if (exp_is_true(ie)) { /* ifthenelse(true, x, y) -> x */
			sql_exp *res = args->h->next->data;
			if (exp_name(e))
				exp_prop_alias(v->sql->sa, res, e);
			v->changes++;
			return res;
		} else if (exp_is_false(ie) || exp_is_null(ie)) { /* ifthenelse(false or null, x, y) -> y */
			sql_exp *res = args->h->next->next->data;
			if (exp_name(e))
				exp_prop_alias(v->sql->sa, res, e);
			v->changes++;
			return res;
		}
	}
	if (is_select(rel->op) || is_join(rel->op) || is_semi(rel->op)) {
		/* simplify like expressions */
		if (is_compare(e->type) && e->flag == cmp_filter && !((sql_subfunc*)e->f)->func->s && strcmp(((sql_subfunc*)e->f)->func->base.name, "like") == 0 &&
			list_length((list *)e->l) == 1 && list_length((list *)e->r) == 3) {
			list *r = e->r;
			sql_exp *fmt = r->h->data;
			sql_exp *esc = r->h->next->data;
			sql_exp *isen = r->h->next->next->data;
			int rewrite = 0, isnull = 0;

			if (fmt->type == e_convert)
				fmt = fmt->l;
			/* check for simple like expression */
			if (exp_is_null(fmt)) {
				isnull = 1;
			} else if (is_atom(fmt->type)) {
				atom *fa = NULL;

				if (fmt->l)
					fa = fmt->l;
				if (fa && fa->data.vtype == TYPE_str && !strchr(fa->data.val.sval, '%') && !strchr(fa->data.val.sval, '_'))
					rewrite = 1;
			}
			if (rewrite && !isnull) { /* check escape flag */
				if (exp_is_null(esc)) {
					isnull = 1;
				} else {
					atom *ea = esc->l;

					if (!is_atom(esc->type) || !ea)
						rewrite = 0;
					else if (ea->data.vtype != TYPE_str || strlen(ea->data.val.sval) != 0)
						rewrite = 0;
				}
			}
			if (rewrite && !isnull) { /* check insensitive flag */
				if (exp_is_null(isen)) {
					isnull = 1;
				} else {
					atom *ia = isen->l;

					if (!is_atom(isen->type) || !ia)
						rewrite = 0;
					else if (ia->data.vtype != TYPE_bit || ia->data.val.btval == 1)
						rewrite = 0;
				}
			}
			if (isnull) {
				e = exp_null(v->sql->sa, sql_bind_localtype("bit"));
				v->changes++;
				return e;
			} else if (rewrite) { /* rewrite to cmp_equal ! */
				list *l = e->l;
				list *r = e->r;
				e = exp_compare(v->sql->sa, l->h->data, r->h->data, is_anti(e) ? cmp_notequal : cmp_equal);
				v->changes++;
			}
		}
		/* rewrite e if left or right is a cast */
		if (is_compare(e->type) && !e->f && is_theta_exp(e->flag) && (((sql_exp*)e->l)->type == e_convert || ((sql_exp*)e->r)->type == e_convert)) {
			sql_rel *r = rel->r;
			sql_exp *le = e->l, *re = e->r;

			/* if convert on left then find mul or div on right which increased scale! */
			if (le->type == e_convert && re->type == e_column && (e->flag == cmp_lt || e->flag == cmp_gt) && r && is_project(r->op)) {
				sql_exp *nre = rel_find_exp(r, re);
				sql_subtype *tt = exp_totype(le), *ft = exp_fromtype(le);

				if (nre && nre->type == e_func) {
					sql_subfunc *f = nre->f;

					if (!f->func->s && !strcmp(f->func->base.name, "sql_mul")) {
						list *args = nre->l;
						sql_exp *ce = args->t->data;
						sql_subtype *fst = exp_subtype(args->h->data);

						if (fst->scale && fst->scale == ft->scale && is_atom(ce->type) && ce->l) {
							atom *a = ce->l;
							int anti = is_anti(e);
							sql_exp *arg1, *arg2;
							sql_subfunc *f;
#ifdef HAVE_HGE
							hge val = 1;
#else
							lng val = 1;
#endif
							/* multiply with smallest value, then scale and (round) */
							int scale = (int) tt->scale - (int) ft->scale, rs = 0;
							atom *na = reduce_scale(v->sql, a);

							if (na != a) {
								rs = a->tpe.scale - na->tpe.scale;
								ce->l = na;
							}
							scale -= rs;

							while (scale > 0) {
								scale--;
								val *= 10;
							}
							arg1 = re;
#ifdef HAVE_HGE
							arg2 = exp_atom_hge(v->sql->sa, val);
#else
							arg2 = exp_atom_lng(v->sql->sa, val);
#endif
							if ((f = sql_bind_func(v->sql, "sys", "scale_down", exp_subtype(arg1), exp_subtype(arg2), F_FUNC, true))) {
								e = exp_compare(v->sql->sa, le->l, exp_binop(v->sql->sa, arg1, arg2, f), e->flag);
								if (anti) set_anti(e);
								v->changes++;
							} else {
								v->sql->session->status = 0;
								v->sql->errstr[0] = '\0';
							}
						}
					}
				}
			}
		}
		if (is_compare(e->type) && is_semantics(e) && (e->flag == cmp_equal || e->flag == cmp_notequal) && exp_is_null(e->r)) {
			/* simplify 'is null' predicates on constants */
			if (exp_is_null(e->l)) {
				int nval = e->flag == cmp_equal;
				if (is_anti(e)) nval = !nval;
				e = exp_atom_bool(v->sql->sa, nval);
				v->changes++;
				return e;
			} else if (exp_is_not_null(e->l)) {
				int nval = e->flag == cmp_notequal;
				if (is_anti(e)) nval = !nval;
				e = exp_atom_bool(v->sql->sa, nval);
				v->changes++;
				return e;
			}
		}
		if (is_atom(e->type) && ((!e->l && !e->r && !e->f) || e->r)) /* prepared statement parameter or argument */
			return e;
		if (is_atom(e->type) && e->l) { /* direct literal */
			atom *a = e->l;
			int flag = a->data.val.bval;

			/* remove simple select true expressions */
			if (flag)
				return e;
		}
		if (is_compare(e->type) && is_theta_exp(e->flag)) {
			sql_exp *l = e->l;
			sql_exp *r = e->r;

			if (is_func(l->type) && (e->flag == cmp_equal || e->flag == cmp_notequal)) {
				sql_subfunc *f = l->f;

				/* rewrite isnull(x) = TRUE/FALSE => x =/<> NULL */
				if (!f->func->s && is_isnull_func(f)) {
					list *args = l->l;
					sql_exp *ie = args->h->data;

					if (!has_nil(ie) || exp_is_not_null(ie)) { /* is null on something that is never null, is always false */
						ie = exp_atom_bool(v->sql->sa, 0);
						v->changes++;
						e->l = ie;
					} else if (exp_is_null(ie)) { /* is null on something that is always null, is always true */
						ie = exp_atom_bool(v->sql->sa, 1);
						v->changes++;
						e->l = ie;
					} else if (is_atom(r->type) && r->l) { /* direct literal */
						atom *a = r->l;

						if (a->isnull) {
							if (is_semantics(e)) { /* isnull(x) = NULL -> false, isnull(x) <> NULL -> true */
								int flag = e->flag == cmp_notequal;
								if (is_anti(e))
									flag = !flag;
								e = exp_atom_bool(v->sql->sa, flag);
							} else /* always NULL */
								e = exp_null(v->sql->sa, sql_bind_localtype("bit"));
							v->changes++;
						} else {
							int flag = a->data.val.bval;

							assert(list_length(args) == 1);
							l = args->h->data;
							if (exp_subtype(l)) {
								r = exp_atom(v->sql->sa, atom_general(v->sql->sa, exp_subtype(l), NULL));
								e = exp_compare(v->sql->sa, l, r, e->flag);
								if (e && !flag)
									set_anti(e);
								if (e)
									set_semantics(e);
								v->changes++;
							}
						}
					}
				} else if (!f->func->s && is_not_func(f)) {
					if (is_atom(r->type) && r->l) { /* direct literal */
						atom *a = r->l;
						list *args = l->l;
						sql_exp *inner = args->h->data;
						sql_subfunc *inf = inner->f;

						assert(list_length(args) == 1);

						/* not(not(x)) = TRUE/FALSE => x = TRUE/FALSE */
						if (is_func(inner->type) &&
							!inf->func->s &&
							is_not_func(inf)) {
							int anti = is_anti(e), is_semantics = is_semantics(e);

							args = inner->l;
							assert(list_length(args) == 1);
							l = args->h->data;
							e = exp_compare(v->sql->sa, l, r, e->flag);
							if (anti) set_anti(e);
							if (is_semantics) set_semantics(e);
							v->changes++;
						/* rewrite not(=/<>(a,b)) = TRUE/FALSE => a=b / a<>b */
						} else if (is_func(inner->type) &&
							!inf->func->s &&
							(!strcmp(inf->func->base.name, "=") ||
							 !strcmp(inf->func->base.name, "<>"))) {
							int flag = a->data.val.bval;
							sql_exp *ne;
							args = inner->l;

							if (!strcmp(inf->func->base.name, "<>"))
								flag = !flag;
							if (e->flag == cmp_notequal)
								flag = !flag;
							assert(list_length(args) == 2);
							l = args->h->data;
							r = args->h->next->data;
							ne = exp_compare(v->sql->sa, l, r, (!flag)?cmp_equal:cmp_notequal);
							if (a->isnull)
								e->l = ne;
							else
								e = ne;
							v->changes++;
						} else if (a && a->data.vtype == TYPE_bit) {
							int anti = is_anti(e), is_semantics = is_semantics(e);

							/* change atom's value on right */
							l = args->h->data;
							if (!a->isnull)
								r = exp_atom_bool(v->sql->sa, !a->data.val.bval);
							e = exp_compare(v->sql->sa, l, r, e->flag);
							if (anti) set_anti(e);
							if (is_semantics) set_semantics(e);
							v->changes++;
						}
					}
				}
			} else if (is_atom(l->type) && is_atom(r->type) && !is_semantics(e) && !e->f) {
				/* compute comparisons on atoms */
				if (exp_is_null(l) || exp_is_null(r)) {
					e = exp_null(v->sql->sa, sql_bind_localtype("bit"));
					v->changes++;
				} else if (l->l && r->l) {
					int res = atom_cmp(l->l, r->l);
					bool flag = !is_anti(e);

					if (res == 0)
						e = exp_atom_bool(v->sql->sa, (e->flag == cmp_equal || e->flag == cmp_gte || e->flag == cmp_lte) ? flag : !flag);
					else if (res > 0)
						e = exp_atom_bool(v->sql->sa, (e->flag == cmp_gt || e->flag == cmp_gte || e->flag == cmp_notequal) ? flag : !flag);
					else
						e = exp_atom_bool(v->sql->sa, (e->flag == cmp_lt || e->flag == cmp_lte || e->flag == cmp_notequal) ? flag : !flag);
					v->changes++;
				}
			}
		}
	}
	return e;
}

static inline sql_exp *
rel_merge_project_rse(visitor *v, sql_rel *rel, sql_exp *e)
{
	if (is_simple_project(rel->op) && is_func(e->type) && e->l) {
		list *fexps = e->l;
		sql_subfunc *f = e->f;

		/* is and function */
		if (!f->func->s && strcmp(f->func->base.name, "and") == 0 && list_length(fexps) == 2) {
			sql_exp *l = list_fetch(fexps, 0), *r = list_fetch(fexps, 1);

			/* check merge into single between */
			if (is_func(l->type) && is_func(r->type)) {
				list *lfexps = l->l, *rfexps = r->l;
				sql_subfunc *lff = l->f, *rff = r->f;

				if (((strcmp(lff->func->base.name, ">=") == 0 || strcmp(lff->func->base.name, ">") == 0) && list_length(lfexps) == 2) &&
					((strcmp(rff->func->base.name, "<=") == 0 || strcmp(rff->func->base.name, "<") == 0) && list_length(rfexps) == 2)) {
					sql_exp *le = list_fetch(lfexps, 0), *lf = list_fetch(rfexps, 0);
					int c_le = is_numeric_upcast(le), c_lf = is_numeric_upcast(lf);

					if (exp_equal(c_le?le->l:le, c_lf?lf->l:lf) == 0) {
						sql_exp *re = list_fetch(lfexps, 1), *rf = list_fetch(rfexps, 1), *ne = NULL;
						sql_subtype super;

						supertype(&super, exp_subtype(le), exp_subtype(lf)); /* le/re and lf/rf must have the same type */
						if (!(le = exp_check_type(v->sql, &super, rel, le, type_equal)) ||
							!(re = exp_check_type(v->sql, &super, rel, re, type_equal)) ||
							!(rf = exp_check_type(v->sql, &super, rel, rf, type_equal))) {
								v->sql->session->status = 0;
								v->sql->errstr[0] = 0;
								return e;
							}
						if ((ne = exp_compare2(v->sql->sa, le, re, rf, compare_funcs2range(lff->func->base.name, rff->func->base.name), 0))) {
							if (exp_name(e))
								exp_prop_alias(v->sql->sa, ne, e);
							e = ne;
							v->changes++;
						}
					}
				}
			}
		}
	}
	return e;
}

static sql_exp *
rel_optimize_exps_(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	(void) depth;
	if (v->value_based_opt)
		e = rel_simplify_predicates(v, rel, e);
	e = rel_merge_project_rse(v, rel, e);
	return e;
}

static sql_rel *
rel_optimize_exps(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_exp_visitor_bottomup(v, rel, &rel_optimize_exps_, false);
}

run_optimizer
bind_optimize_exps(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_cycle < 2 && gp->opt_level == 1 && (gp->cnt[op_project] || gp->cnt[op_join]
		   || gp->cnt[op_left] || gp->cnt[op_right] || gp->cnt[op_full] || gp->cnt[op_semi]
		   || gp->cnt[op_anti] || gp->cnt[op_select]) && (flag & optimize_exps) ? rel_optimize_exps : NULL;
}
