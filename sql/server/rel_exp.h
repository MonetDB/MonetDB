/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _REL_EXP_H_
#define _REL_EXP_H_

#include "sql_relation.h"
#include "sql_mvc.h"
#include "sql_atom.h"
#include "sql_semantic.h"
#include "rel_prop.h"

#define LANG_INT_OR_MAL(l)  ((l)==FUNC_LANG_INT || (l)==FUNC_LANG_MAL)

#define new_exp_list(sa) sa_list(sa)
#define exp2list(sa,e)   append(sa_list(sa),e)

#define is_compare_func(sf) (!sf->func->s && \
	(strcmp(sf->func->base.name, "<") == 0 || strcmp(sf->func->base.name, "<=") == 0 || \
 	 strcmp(sf->func->base.name, "=") == 0 || strcmp(sf->func->base.name, "<>") == 0 || \
 	 strcmp(sf->func->base.name, ">") == 0 || strcmp(sf->func->base.name, ">=") == 0) )

#define is_notequal_func(sf) (!sf->func->s && strcmp(sf->func->base.name, "<>") == 0)

extern comp_type compare_str2type(const char *compare_op);
extern comp_type swap_compare( comp_type t );
extern comp_type negate_compare( comp_type t );
extern comp_type range2lcompare( int r );
extern comp_type range2rcompare( int r );
extern int compare2range( int l, int r );
extern int compare_funcs2range(const char *l, const char *r);

extern sql_exp *exp_compare(allocator *sa, sql_exp *l, sql_exp *r, int cmptype);
extern sql_exp *exp_compare2(allocator *sa, sql_exp *l, sql_exp *r, sql_exp *f, int cmptype, int symmetric);
extern sql_exp *exp_filter(allocator *sa, list *l, list *r, sql_subfunc *f, int anti);
extern sql_exp *exp_or(allocator *sa, list *l, list *r, int anti);
extern sql_exp *exp_in(allocator *sa, sql_exp *l, list *r, int cmptype);
extern sql_exp *exp_in_func(mvc *sql, sql_exp *le, sql_exp *vals, int anyequal, int is_tuple);
extern sql_exp *exp_in_aggr(mvc *sql, sql_exp *le, sql_exp *vals, int anyequal, int is_tuple);
extern sql_exp *exp_compare_func(mvc *sql, sql_exp *le, sql_exp *re, const char *compareop, int quantifier);

#define exp_fromtype(e)	((list*)e->r)->h->data
#define exp_totype(e)	((list*)e->r)->h->next->data
extern sql_exp *exp_convert(mvc *sql, sql_exp *exp, sql_subtype *fromtype, sql_subtype *totype );
sql_export str number2name(str s, int len, int i);
sql_export sql_exp *exp_op(allocator *sa, list *l, sql_subfunc *f );
extern sql_exp *exp_rank_op(allocator *sa, list *largs, list *gbe, list *obe, sql_subfunc *f );

#define append(l,v) list_append(l,v)
#define exp_unop(sa,l,f) \
	exp_op(sa, append(new_exp_list(sa),l), f)
#define exp_binop(sa,l,r,f) \
	exp_op(sa, append(append(new_exp_list(sa),l),r), f)
#define exp_op3(sa,l,r,r2,f) \
	exp_op(sa, append(append(append(new_exp_list(sa),l),r),r2), f)
#define exp_op4(sa,l,r,r2,r3,f) \
	exp_op(sa, append(append(append(append(new_exp_list(sa),l),r),r2),r3), f)
extern sql_exp *exp_aggr(allocator *sa, list *l, sql_subfunc *a, int distinct, int no_nils, unsigned int card, int has_nil );
#define exp_aggr1(sa, e, a, d, n, c, hn) \
	exp_aggr(sa, append(new_exp_list(sa), e), a, d, n, c, hn)
#define exp_aggr2(sa, e1, e2, a, d, n, c, hn) \
	exp_aggr(sa, append(append(new_exp_list(sa),e1),e2), a, d, n, c, hn)
#define exp_aggr3(sa, e1, e2, e3, a, d, n, c, hn) \
	exp_aggr(sa, append(append(append(new_exp_list(sa),e1),e2),e3), a, d, n, c, hn)
extern sql_exp * exp_atom(allocator *sa, atom *a);
extern sql_exp * exp_atom_max(allocator *sa, sql_subtype *tpe);
extern sql_exp * exp_atom_bool(allocator *sa, int b);
extern sql_exp * exp_atom_bte(allocator *sa, bte i);
extern sql_exp * exp_atom_sht(allocator *sa, sht i);
sql_export sql_exp * exp_atom_int(allocator *sa, int i);
sql_export sql_exp * exp_atom_lng(allocator *sa, lng l);
extern sql_exp *exp_atom_oid(allocator *sa, oid i);
#ifdef HAVE_HGE
extern sql_exp * exp_atom_hge(allocator *sa, hge l);
#endif
extern sql_exp * exp_atom_flt(allocator *sa, flt f);
extern sql_exp * exp_atom_dbl(allocator *sa, dbl d);
sql_export sql_exp * exp_atom_str(allocator *sa, const char *s, sql_subtype *st);
extern sql_exp * exp_atom_clob(allocator *sa, const char *s);
sql_export sql_exp * exp_atom_ptr(allocator *sa, void *s);
extern sql_exp * exp_atom_ref(allocator *sa, int i, sql_subtype *tpe);
extern sql_exp * exp_null(allocator *sa, sql_subtype *tpe);
extern sql_exp * exp_zero(allocator *sa, sql_subtype *tpe); /* Apply it to numeric types only obviously */
extern sql_exp * exp_param_or_declared(allocator *sa, const char *sname, const char *name, sql_subtype *tpe, int frame);
extern atom * exp_value(mvc *sql, sql_exp *e);
extern sql_exp * exp_values(allocator *sa, list *exps);
extern list * exp_get_values(sql_exp *e); /* get expression list from the values expression */
extern list * exp_types(allocator *sa, list *exps);
extern int have_nil(list *exps);
extern int have_semantics(list *exps);

sql_export sql_exp * exp_column(allocator *sa, const char *rname, const char *name, sql_subtype *t, unsigned int card, int has_nils, int unique, int intern);
extern sql_exp * exp_propagate(allocator *sa, sql_exp *ne, sql_exp *oe);
extern sql_exp * exp_ref(mvc *sql, sql_exp *e);
extern sql_exp * exp_ref_save(mvc *sql, sql_exp *e); /* if needed mark the input expression as a referenced expression, return reference to e */
extern sql_exp * exp_alias(mvc *sql, const char *arname, const char *acname, const char *org_rname, const char *org_cname, sql_subtype *t, unsigned int card, int has_nils, int unique, int intern);
extern sql_exp * exp_alias_ref(mvc *sql, sql_exp *e);
extern sql_exp * exp_set(allocator *sa, const char *sname, const char *name, sql_exp *val, int level);
extern sql_exp * exp_var(allocator *sa, const char *sname, const char *name, sql_subtype *type, int level);
extern sql_exp * exp_table(allocator *sa, const char *name, sql_table *t, int level);
extern sql_exp * exp_return(allocator *sa, sql_exp *val, int level);
extern sql_exp * exp_while(allocator *sa, sql_exp *cond, list *stmts);
extern sql_exp * exp_exception(allocator *sa, sql_exp *cond, const char *error_message);
extern sql_exp * exp_if(allocator *sa, sql_exp *cond, list *if_stmts, list *else_stmts);
extern sql_exp * exp_rel(mvc *sql, sql_rel * r);

extern void exp_setname(mvc *sql, sql_exp *e, const char *rname, const char *name );
extern void exp_setrelname(allocator *sa, sql_exp *e, int nr );
extern void exp_setalias(sql_exp *e, int label, const char *rname, const char *name);
extern void exp_prop_alias(allocator *sa, sql_exp *e, sql_exp *oe);

extern void noninternexp_setname(mvc *sql, sql_exp *e, const char *rname, const char *name );
extern char* make_label(allocator *sa, int nr);
extern sql_exp* exp_label(allocator *sa, sql_exp *e, int nr);
extern list* exps_label(mvc *sql, list *exps);

extern sql_exp * exp_copy( mvc *sql, sql_exp *e);
extern list * exps_copy( mvc *sql, list *exps);
extern list * exps_alias( mvc *sql, list *exps);

extern void exp_swap( sql_exp *e );

extern sql_subtype * exp_subtype( sql_exp *e );
extern const char * exp_name( sql_exp *e );
extern const char * exp_relname( sql_exp *e );
extern const char * exp_func_name( sql_exp *e );
extern unsigned int exp_card(sql_exp *e);
extern unsigned int exp_get_label(sql_exp *e);

extern const char *exp_find_rel_name(sql_exp *e);

extern sql_exp *rel_find_exp(sql_rel *rel, sql_exp *e);
extern sql_exp *rel_find_exp_and_corresponding_rel(sql_rel *rel, sql_exp *e, bool subexp, sql_rel **res, bool *under_join);
extern bool rel_find_nid(sql_rel *rel, int nid);

extern int exp_cmp( sql_exp *e1, sql_exp *e2);
extern int exp_equal( sql_exp *e1, sql_exp *e2);
extern int exp_refers( sql_exp *p, sql_exp *c);
extern sql_exp *exps_refers( sql_exp *p, list *exps);
extern int exp_match( sql_exp *e1, sql_exp *e2);
extern sql_exp* exps_find_exp( list *l, sql_exp *e);
extern int exp_match_exp( sql_exp *e1, sql_exp *e2);
extern int exp_match_exp_semantics( sql_exp *e1, sql_exp *e2, bool semantics);
extern sql_exp* exps_any_match(list *l, sql_exp *e);
/* match just the column (cmp equality) expressions */
extern int exp_match_col_exps( sql_exp *e, list *l);
extern int exps_match_col_exps( sql_exp *e1, sql_exp *e2);
/* todo rename */
extern int exp_match_list( list *l, list *r);
extern int exp_is_join(sql_exp *e, list *rels);
extern int exp_is_eqjoin(sql_exp *e);
extern int exp_is_join_exp(sql_exp *e);
extern int exp_is_atom(sql_exp *e);
/* exp_is_true/false etc return true if the expression is true, on unknown etc false is returned */
extern int exp_is_true(sql_exp *e);
extern int exp_is_false(sql_exp *e);
extern int exp_is_zero(sql_exp *e);
extern int exp_is_not_null(sql_exp *e);
extern int exp_is_null(sql_exp *e);
extern int exp_is_rel(sql_exp *e);
extern int exps_one_is_rel(list *exps);
extern int exp_is_aggr(sql_rel *r, sql_exp *e); /* check if e is aggregation result of r */
extern int exp_has_aggr(sql_rel *r, sql_exp *e); /* check if group by expression has some aggregate function from r */
extern int exp_has_rel(sql_exp *e);
extern int exps_have_rel_exp(list *exps);
extern int exps_have_func(list *exps);
extern sql_rel *exp_rel_get_rel(allocator *sa, sql_exp *e);
extern sql_exp *exp_rel_update_exp(mvc *sql, sql_exp *e, bool up);
extern sql_exp *exp_rel_label(mvc *sql, sql_exp *e);
extern int exp_rel_depth(sql_exp *e);
extern int exps_are_atoms(list *exps);
extern int exp_has_func(sql_exp *e);
extern bool exps_have_unsafe(list *exps, bool allow_identity, bool card /* on true check for possible cardinality related
																		  unsafeness (conversions for example) */);
extern bool exp_unsafe(sql_exp *e, bool allow_identity, bool card);
extern int exp_has_sideeffect(sql_exp *e);

extern sql_exp *exps_find_prop(list *exps, rel_prop kind);

/* returns 0 when the relation contain the passed expression (or sub expressions if subexp is set) else < 0 */
extern int rel_has_exp(sql_rel *rel, sql_exp *e, bool subexp);
/* return 0 when the relation contain at least one of the passed expressions (or sub expressions if subexp is set) else < 0 */
extern int rel_has_exps(sql_rel *rel, list *e, bool subexp);
/* return 1 when the relation contains all of the passed expressions else 0 */
extern int rel_has_all_exps(sql_rel *rel, list *e);

extern sql_rel *find_rel(list *rels, sql_exp *e);
extern sql_rel *find_one_rel(list *rels, sql_exp *e);

extern sql_exp *exps_bind_nid(list *exps, int nid); /* get first expression to which this nid points */
extern sql_exp *exps_uses_nid(list *exps, int nid); /* get first expression which references back to nid */
extern sql_exp *exps_bind_column(list *exps, const char *cname, int *ambiguous, int *multiple, int no_tname /* set if expressions should be without a tname */);
extern sql_exp *exps_bind_column2(list *exps, const char *rname, const char *cname, int *multiple);
extern sql_exp *exps_bind_alias(list *exps, const char *rname, const char *cname);
extern sql_exp * list_find_exp( list *exps, sql_exp *e);

extern unsigned int exps_card( list *l );
extern void exps_fix_card( list *exps, unsigned int card);
extern void exps_setcard( list *exps, unsigned int card);
extern int exps_intern(list *exps);
extern sql_exp *exps_find_one_multi_exp(list *exps);

extern const char *compare_func( comp_type t, int anti );
extern int is_identity( sql_exp *e, sql_rel *r);

extern void exps_scale_fix(sql_subfunc *f, list *exps, sql_subtype *atp);
extern void exps_max_bits(sql_subfunc *f, list *exps);
extern void exps_sum_scales(sql_subfunc *f, list *exps);
extern sql_exp *exps_scale_algebra(mvc *sql, sql_subfunc *f, sql_rel *rel, list *exps);
extern void exps_digits_add(sql_subfunc *f, list *exps);
extern void exps_inout(sql_subfunc *f, list *exps);
extern void exps_largest_int(sql_subfunc *f, list *exps, lng cnt);

extern int exp_aggr_is_count(sql_exp *e);
extern list *check_distinct_exp_names(mvc *sql, list *exps);

extern sql_exp *exp_check_type(mvc *sql, sql_subtype *t, sql_rel *rel, sql_exp *exp, check_type tpe);
extern int rel_set_type_param(mvc *sql, sql_subtype *type, sql_rel *rel, sql_exp *rel_exp, int upcast);
extern sql_exp *exp_convert_inplace(mvc *sql, sql_subtype *t, sql_exp *exp);
extern sql_exp *exp_numeric_supertype(mvc *sql, sql_exp *e);
extern sql_exp *exp_values_set_supertype(mvc *sql, sql_exp *values, sql_subtype *opt_super);

#endif /* _REL_EXP_H_ */
