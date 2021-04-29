/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _REL_EXP_H_
#define _REL_EXP_H_

#include "sql_relation.h"
#include "sql_mvc.h"
#include "sql_atom.h"
#include "sql_semantic.h"
#include "rel_prop.h"

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

extern sql_exp *exp_compare(sql_allocator *sa, sql_exp *l, sql_exp *r, int cmptype);
extern sql_exp *exp_compare2(sql_allocator *sa, sql_exp *l, sql_exp *r, sql_exp *f, int cmptype);
extern sql_exp *exp_filter(sql_allocator *sa, list *l, list *r, sql_subfunc *f, int anti);
extern sql_exp *exp_or(sql_allocator *sa, list *l, list *r, int anti);
extern sql_exp *exp_in(sql_allocator *sa, sql_exp *l, list *r, int cmptype);
extern sql_exp *exp_in_func(mvc *sql, sql_exp *le, sql_exp *vals, int anyequal, int is_tuple);
extern sql_exp *exp_compare_func(mvc *sql, sql_exp *le, sql_exp *re, const char *compareop, int quantifier);

#define exp_fromtype(e)	((list*)e->r)->h->data
#define exp_totype(e)	((list*)e->r)->h->next->data
extern sql_exp *exp_convert(sql_allocator *sa, sql_exp *exp, sql_subtype *fromtype, sql_subtype *totype );
sql_export str number2name(str s, int len, int i);
extern sql_exp *exp_op(sql_allocator *sa, list *l, sql_subfunc *f );
extern sql_exp *exp_rank_op(sql_allocator *sa, list *largs, list *gbe, list *obe, sql_subfunc *f );

#define append(l,v) list_append(l,v)
#define exp_unop(sa,l,f) \
	exp_op(sa, append(new_exp_list(sa),l), f)
#define exp_binop(sa,l,r,f) \
	exp_op(sa, append(append(new_exp_list(sa),l),r), f)
#define exp_op3(sa,l,r,r2,f) \
	exp_op(sa, append(append(append(new_exp_list(sa),l),r),r2), f)
#define exp_op4(sa,l,r,r2,r3,f) \
	exp_op(sa, append(append(append(append(new_exp_list(sa),l),r),r2),r3), f)
extern sql_exp *exp_aggr(sql_allocator *sa, list *l, sql_subfunc *a, int distinct, int no_nils, unsigned int card, int has_nil );
#define exp_aggr1(sa, e, a, d, n, c, hn) \
	exp_aggr(sa, append(new_exp_list(sa), e), a, d, n, c, hn)
extern sql_exp * exp_atom(sql_allocator *sa, atom *a);
extern sql_exp * exp_atom_max(sql_allocator *sa, sql_subtype *tpe);
extern sql_exp * exp_atom_bool(sql_allocator *sa, int b);
extern sql_exp * exp_atom_bte(sql_allocator *sa, bte i);
extern sql_exp * exp_atom_sht(sql_allocator *sa, sht i);
extern sql_exp * exp_atom_int(sql_allocator *sa, int i);
extern sql_exp * exp_atom_lng(sql_allocator *sa, lng l);
extern sql_exp *exp_atom_oid(sql_allocator *sa, oid i);
#ifdef HAVE_HGE
extern sql_exp * exp_atom_hge(sql_allocator *sa, hge l);
#endif
extern sql_exp * exp_atom_flt(sql_allocator *sa, flt f);
extern sql_exp * exp_atom_dbl(sql_allocator *sa, dbl d);
extern sql_exp * exp_atom_str(sql_allocator *sa, const char *s, sql_subtype *st);
extern sql_exp * exp_atom_clob(sql_allocator *sa, const char *s);
extern sql_exp * exp_atom_ptr(sql_allocator *sa, void *s);
extern sql_exp * exp_atom_ref(sql_allocator *sa, int i, sql_subtype *tpe);
extern sql_exp * exp_null(sql_allocator *sa, sql_subtype *tpe);
extern sql_exp * exp_zero(sql_allocator *sa, sql_subtype *tpe); /* Apply it to numeric types only obviously */
extern sql_exp * exp_param_or_declared(sql_allocator *sa, const char *sname, const char *name, sql_subtype *tpe, int frame);
extern atom * exp_value(mvc *sql, sql_exp *e);
extern sql_exp * exp_values(sql_allocator *sa, list *exps);
extern list * exp_get_values(sql_exp *e); /* get expression list from the values expression */
extern list * exp_types(sql_allocator *sa, list *exps);
extern int have_nil(list *exps);

sql_export sql_exp * exp_column(sql_allocator *sa, const char *rname, const char *name, sql_subtype *t, unsigned int card, int has_nils, int intern);
extern sql_exp * exp_propagate(sql_allocator *sa, sql_exp *ne, sql_exp *oe);
extern sql_exp * exp_ref(mvc *sql, sql_exp *e);
extern sql_exp * exp_ref_save(mvc *sql, sql_exp *e); /* if needed mark the input expression as a referenced expression, return reference to e */
extern sql_exp * exp_alias(sql_allocator *sa, const char *arname, const char *acname, const char *org_rname, const char *org_cname, sql_subtype *t, unsigned int card, int has_nils, int intern);
extern sql_exp * exp_alias_or_copy( mvc *sql, const char *tname, const char *cname, sql_rel *orel, sql_exp *old);
extern sql_exp * exp_alias_ref(mvc *sql, sql_exp *e);
extern sql_exp * exp_set(sql_allocator *sa, const char *sname, const char *name, sql_exp *val, int level);
extern sql_exp * exp_var(sql_allocator *sa, const char *sname, const char *name, sql_subtype *type, int level);
extern sql_exp * exp_table(sql_allocator *sa, const char *name, sql_table *t, int level);
extern sql_exp * exp_return(sql_allocator *sa, sql_exp *val, int level);
extern sql_exp * exp_while(sql_allocator *sa, sql_exp *cond, list *stmts);
extern sql_exp * exp_exception(sql_allocator *sa, sql_exp *cond, const char *error_message);
extern sql_exp * exp_if(sql_allocator *sa, sql_exp *cond, list *if_stmts, list *else_stmts);
extern sql_exp * exp_rel(mvc *sql, sql_rel * r);

extern void exp_setname(sql_allocator *sa, sql_exp *e, const char *rname, const char *name );
extern void exp_setrelname(sql_allocator *sa, sql_exp *e, int nr );
extern void exp_setalias(sql_exp *e, const char *rname, const char *name);
extern void exp_prop_alias(sql_allocator *sa, sql_exp *e, sql_exp *oe);

extern void noninternexp_setname(sql_allocator *sa, sql_exp *e, const char *rname, const char *name );
extern char* make_label(sql_allocator *sa, int nr);
extern sql_exp* exp_label(sql_allocator *sa, sql_exp *e, int nr);
extern sql_exp* exp_label_table(sql_allocator *sa, sql_exp *e, int nr);
extern list* exps_label(sql_allocator *sa, list *exps, int nr);

extern sql_exp * exp_copy( mvc *sql, sql_exp *e);
extern list * exps_copy( mvc *sql, list *exps);
extern list * exps_alias( mvc *sql, list *exps);

extern void exp_swap( sql_exp *e );

extern sql_subtype * exp_subtype( sql_exp *e );
extern const char * exp_name( sql_exp *e );
extern const char * exp_relname( sql_exp *e );
extern const char * exp_func_name( sql_exp *e );
extern unsigned int exp_card(sql_exp *e);

extern const char *exp_find_rel_name(sql_exp *e);

extern sql_exp *rel_find_exp(sql_rel *rel, sql_exp *e);
extern sql_exp *rel_find_exp_and_corresponding_rel(sql_rel *rel, sql_exp *e, sql_rel **res, bool *under_join);

extern int exp_cmp( sql_exp *e1, sql_exp *e2);
extern int exp_equal( sql_exp *e1, sql_exp *e2);
extern int exp_refers( sql_exp *p, sql_exp *c);
extern int exp_match( sql_exp *e1, sql_exp *e2);
extern sql_exp* exps_find_exp( list *l, sql_exp *e);
extern int exp_match_exp( sql_exp *e1, sql_exp *e2);
extern sql_exp* exps_any_match(list *l, sql_exp *e);
extern sql_exp *exps_any_match_same_or_no_alias(list *l, sql_exp *e);
/* match just the column (cmp equality) expressions */
extern int exp_match_col_exps( sql_exp *e, list *l);
extern int exps_match_col_exps( sql_exp *e1, sql_exp *e2);
/* todo rename */
extern int exp_match_list( list *l, list *r);
extern int exp_is_join(sql_exp *e, list *rels);
extern int exp_is_eqjoin(sql_exp *e);
extern int exp_is_join_exp(sql_exp *e);
extern int exp_is_atom(sql_exp *e);
extern int exp_is_true(sql_exp *e);
extern int exp_is_false(sql_exp *e);
extern int exp_is_zero(sql_exp *e);
extern int exp_is_not_null(sql_exp *e);
extern int exp_is_null(sql_exp *e);
extern int exp_is_rel(sql_exp *e);
extern int exps_one_is_rel(list *exps);
extern int exp_has_rel(sql_exp *e);
extern int exps_have_rel_exp(list *exps);
extern int exps_have_func(list *exps);
extern sql_rel *exp_rel_get_rel(sql_allocator *sa, sql_exp *e);
extern sql_exp *exp_rel_update_exp(mvc *sql, sql_exp *e);
extern sql_exp *exp_rel_label(mvc *sql, sql_exp *e);
extern int exps_are_atoms(list *exps);
extern int exp_has_func(sql_exp *e);
extern int exps_have_unsafe(list *exps, int allow_identity);
extern int exp_unsafe(sql_exp *e, int allow_identity);
extern int exp_has_sideeffect(sql_exp *e);

extern sql_exp *exps_find_prop(list *exps, rel_prop kind);

/* returns 0 when the relation contain the passed expression else < 0 */
extern int rel_has_exp(sql_rel *rel, sql_exp *e);
/* return 0 when the relation contain atleast one of the passed expressions else < 0 */
extern int rel_has_exps(sql_rel *rel, list *e);
/* return 1 when the relation contains all of the passed expressions else 0 */
extern int rel_has_all_exps(sql_rel *rel, list *e);

extern sql_rel *find_rel(list *rels, sql_exp *e);
extern sql_rel *find_one_rel(list *rels, sql_exp *e);

extern sql_exp *exps_bind_column(list *exps, const char *cname, int *ambiguous, int *multiple, int no_tname /* set if expressions should be without a tname */);
extern sql_exp *exps_bind_column2(list *exps, const char *rname, const char *cname, int *multiple);
extern sql_exp *exps_bind_alias(list *exps, const char *rname, const char *cname);

extern unsigned int exps_card( list *l );
extern void exps_fix_card( list *exps, unsigned int card);
extern void exps_setcard( list *exps, unsigned int card);
extern int exps_intern(list *exps);

extern const char *compare_func( comp_type t, int anti );
extern int is_identity( sql_exp *e, sql_rel *r);

extern atom *exp_flatten(mvc *sql, sql_exp *e);

extern void exp_sum_scales(sql_subfunc *f, sql_exp *l, sql_exp *r);

extern int exp_aggr_is_count(sql_exp *e);

extern void exps_reset_freevar(list *exps);

extern sql_exp *exp_check_type(mvc *sql, sql_subtype *t, sql_rel *rel, sql_exp *exp, check_type tpe);
extern int rel_set_type_param(mvc *sql, sql_subtype *type, sql_rel *rel, sql_exp *rel_exp, int upcast);
extern sql_exp *exp_convert_inplace(mvc *sql, sql_subtype *t, sql_exp *exp);
extern sql_exp *exp_numeric_supertype(mvc *sql, sql_exp *e);
extern sql_exp *exp_values_set_supertype(mvc *sql, sql_exp *values, sql_subtype *opt_super);

extern int rel_set_type_recurse(mvc *sql, sql_subtype *type, sql_rel *rel, const char **relname, const char **expname);
#endif /* _REL_EXP_H_ */
