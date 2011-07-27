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

#ifndef _REL_EXP_H_
#define _REL_EXP_H_

#define new_exp_list(sa) list_new(sa)
#define exp2list(sa,e)   append(list_new(sa),e)

extern sql_exp *exp_compare(sql_allocator *sa, sql_exp *l, sql_exp *r, int cmptype);
extern sql_exp *exp_compare2(sql_allocator *sa, sql_exp *l, sql_exp *r, sql_exp *h, int cmptype);
extern sql_exp *exp_or(sql_allocator *sa, list *l, list *r);
extern sql_exp *exp_in(sql_allocator *sa, sql_exp *l, list *r, int cmptype);

#define exp_fromtype(e)	((list*)e->r)->h->data
#define exp_totype(e)	((list*)e->r)->h->next->data
extern sql_exp *exp_convert(sql_allocator *sa, sql_exp *exp, sql_subtype *fromtype, sql_subtype *totype );
extern str number2name(str s, int len, int i);
extern sql_exp *exp_op(sql_allocator *sa, list *l, sql_subfunc *f );

#define append(l,v) list_append(l,v) 
#define exp_unop(sa,l,f) \
	exp_op(sa, append(new_exp_list(sa),l), f)
#define exp_binop(sa,l,r,f) \
	exp_op(sa, append(append(new_exp_list(sa),l),r), f)
#define exp_op3(sa,l,r,r2,f) \
	exp_op(sa, append(append(append(new_exp_list(sa),l),r),r2), f)
#define exp_op4(sa,l,r,r2,r3,f) \
	exp_op(sa, append(append(append(append(new_exp_list(sa),l),r),r2),r3), f)
extern sql_exp *exp_aggr(sql_allocator *sa, list *l, sql_subaggr *a, int distinct, int no_nils, int card, int has_nil );
#define exp_aggr1(sa, e, a, d, n, c, hn) \
	exp_aggr(sa, append(new_exp_list(sa), e), a, d, n, c, hn)
extern sql_exp * exp_atom(sql_allocator *sa, atom *a);
extern sql_exp * exp_atom_bool(sql_allocator *sa, int b); 
extern sql_exp * exp_atom_int(sql_allocator *sa, int i);
extern sql_exp * exp_atom_lng(sql_allocator *sa, lng l);
extern sql_exp * exp_atom_wrd(sql_allocator *sa, wrd w);
extern sql_exp * exp_atom_str(sql_allocator *sa, str s, sql_subtype *st);
extern sql_exp * exp_atom_clob(sql_allocator *sa, str s);
extern sql_exp * exp_atom_ptr(sql_allocator *sa, void *s);
extern sql_exp * exp_atom_ref(sql_allocator *sa, int i, sql_subtype *tpe);
extern sql_exp * exp_param(sql_allocator *sa, char *name, sql_subtype *tpe, int frame);
extern atom * exp_value(sql_exp *e, atom **args, int maxarg);
extern sql_exp * exp_values(sql_allocator *sa, list *exps);

extern sql_exp * exp_column(sql_allocator *sa, char *rname, char *name, sql_subtype *t, int card, int has_nils, int intern);
extern sql_exp * exp_alias(sql_allocator *sa, char *arname, char *acname, char *org_rname, char *org_cname, sql_subtype *t, int card, int has_nils, int intern);
extern void exp_setname(sql_allocator *sa, sql_exp *e, char *rname, char *name );
extern sql_exp* exp_label(sql_allocator *sa, sql_exp *e, int nr);

extern void exp_swap( sql_exp *e );

extern sql_subtype * exp_subtype( sql_exp *e );
extern char * exp_name( sql_exp *e );
extern char * exp_relname( sql_exp *e );
extern char * exp_func_name( sql_exp *e );

extern char *exp_find_rel_name(sql_exp *e);

extern sql_exp *rel_find_exp( sql_rel *rel, sql_exp *e);

extern int exp_cmp( sql_exp *e1, sql_exp *e2);
extern int exp_match( sql_exp *e1, sql_exp *e2);
extern int exp_match_exp( sql_exp *e1, sql_exp *e2);
/* match just the column (cmp equality) expressions */
extern int exp_match_col_exps( sql_exp *e, list *l);
extern int exps_match_col_exps( sql_exp *e1, sql_exp *e2);
extern int exp_is_join(sql_exp *e);
extern int exp_is_eqjoin(sql_exp *e);
extern int exp_is_correlation(sql_exp *e, sql_rel *r );
extern int exp_is_join_exp(sql_exp *e);
extern int exp_is_atom(sql_exp *e);

extern sql_exp *exps_bind_column( list *exps, char *cname, int *ambiguous);
extern sql_exp *exps_bind_column2( list *exps, char *rname, char *cname);

extern int exps_card( list *l );
extern void exps_fix_card( list *exps, int card);
extern int exps_intern(list *exps);

extern char *compare_func( comp_type t );

#endif /* _REL_EXP_H_ */
