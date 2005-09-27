/**
 * @file
 *
 * Mnemonic abbreviations for physical algebra constructors.
 *
 * $Id$
 */

/* Also import generic algebra stuff */
#include "algebra_mnemonic.h"

/** a sort specification list is just another attribute list */
/* FIXME */
#define sortby(...)     PFalg_attlist (__VA_ARGS__)

/** literal table construction */
#define lit_tbl(a,b,c)    PFpa_lit_tbl ((a), (b), (c))

/** empty table construction */
#define empty_tbl(atts)   PFpa_empty_tbl (atts)

#define append_union(a,b) PFpa_append_union ((a), (b))
#define merge_union(a,b,c) PFpa_merge_union ((a), (b), (c))

#define intersect(a,b)    PFpa_intersect ((a), (b))

#define difference(a,b)   PFpa_difference ((a), (b))

/** cartesian product */
#define cross(a,b)        PFpa_cross ((a),(b))

/** ColumnAttach */
#define attach(a,b,c)     PFpa_attach ((a), (b), (c))

/** HashDistinct */
#define sort_distinct(a,b) PFpa_sort_distinct ((a), (b))

/** projection operator */
#define project(a,b,c)    PFpa_project ((a), (b), (c))

#define eqjoin(a,b,c,d)   PFpa_eqjoin ((a), (b), (c), (d))
#define leftjoin(a,b,c,d) PFpa_leftjoin ((a), (b), (c), (d))

#if 0
/** NestedLoopJoin */
#define nljoin(a,b,c,d)   PFpa_nljoin ((a), (b), (c), (d))

/** MergeJoin */
#define merge_join(a,b,c,d) PFpa_merge_join ((a), (b), (c), (d))
#endif

/** StandardSort */
#define std_sort(a,b)     PFpa_std_sort ((a), (b))

/** RefineSort */
#define refine_sort(a,b,c) PFpa_refine_sort ((a), (b), (c))

/** HashRowNumber */
#define hash_rownum(a,b,c) PFpa_hash_rownum ((a), (b), (c))

/** MergeRowNumber */
#define merge_rownum(a,b,c) PFpa_merge_rownum ((a), (b), (c))

#define num_add(a,b,c,d)   PFpa_num_add ((a), (b), (c), (d))
#define num_sub(a,b,c,d)   PFpa_num_sub ((a), (b), (c), (d))
#define num_mult(a,b,c,d)  PFpa_num_mult ((a), (b), (c), (d))
#define num_div(a,b,c,d)   PFpa_num_div ((a), (b), (c), (d))
#define num_mod(a,b,c,d)   PFpa_num_mod ((a), (b), (c), (d))

#define num_add_atom(a,b,c,d)   PFpa_num_add_atom ((a), (b), (c), (d))
#define num_sub_atom(a,b,c,d)   PFpa_num_sub_atom ((a), (b), (c), (d))
#define num_mult_atom(a,b,c,d)  PFpa_num_mult_atom ((a), (b), (c), (d))
#define num_div_atom(a,b,c,d)   PFpa_num_div_atom ((a), (b), (c), (d))
#define num_mod_atom(a,b,c,d)   PFpa_num_mod_atom ((a), (b), (c), (d))

#define num_neg(a,b,c)  PFpa_num_neg ((a), (b), (c))
#define bool_not(a,b,c) PFpa_bool_not ((a), (b), (c))

#define cast(a,b,c) PFpa_cast ((a), (b), (c))

#define select_(a,b) PFpa_select ((a), (b))

#define hash_count(a,b,c) PFpa_hash_count ((a), (b), (c))

/** StaircaseJoin */
#define llscj_anc(a,b,c,d,e) PFpa_llscj_anc ((a), (b), (c), (d), (e))
#define llscj_anc_self(a,b,c,d,e) PFpa_llscj_anc_self ((a), (b), (c), (d), (e))
#define llscj_attr(a,b,c,d,e) PFpa_llscj_attr ((a), (b), (c), (d), (e))
#define llscj_child(a,b,c,d,e) PFpa_llscj_child ((a), (b), (c), (d), (e))
#define llscj_desc(a,b,c,d,e) PFpa_llscj_desc ((a), (b), (c), (d), (e))
#define llscj_desc_self(a,b,c,d,e) PFpa_llscj_desc_self ((a),(b), (c), (d), (e))
#define llscj_foll(a,b,c,d,e) PFpa_llscj_foll ((a), (b), (c), (d), (e))
#define llscj_foll_self(a,b,c,d,e) PFpa_llscj_foll_self ((a),(b), (c), (d), (e))
#define llscj_parent(a,b,c,d,e) PFpa_llscj_parent ((a), (b), (c), (d), (e))
#define llscj_prec(a,b,c,d,e) PFpa_llscj_prec ((a), (b), (c), (d), (e))
#define llscj_prec_self(a,b,c,d,e) PFpa_llscj_prec_self ((a),(b), (c), (d), (e))

#define doc_tbl(a)        PFpa_doc_tbl (a)

/** empty fragment list */
#define empty_frag()      PFpa_empty_frag ()

/** roots() operator */
#define roots(a)          PFpa_roots (a)

#define fragment(a)       PFpa_fragment (a)
#define frag_union(a,b)   PFpa_frag_union ((a), (b))

#define doc_access(a,b,c,d) PFpa_doc_access ((a), (b), (c), (d))
#define string_join(a,b)  PFpa_string_join ((a), (b))

#define serialize(a,b)    PFpa_serialize ((a), (b))

/* vim:set shiftwidth=4 expandtab: */
