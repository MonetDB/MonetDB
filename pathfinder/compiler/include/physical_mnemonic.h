/**
 * @file
 *
 * Mnemonic abbreviations for physical algebra constructors.
 *
 * Copyright Notice:
 * -----------------
 *
 * The contents of this file are subject to the Pathfinder Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/PathfinderLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the Pathfinder system.
 *
 * The Original Code has initially been developed by the Database &
 * Information Systems Group at the University of Konstanz, Germany and
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

/* Also import generic algebra stuff */
#include "algebra_mnemonic.h"

#define serialize(a,b,c)     PFpa_serialize ((a), (b), (c))

/** literal table construction */
#define lit_tbl(a,b,c)       PFpa_lit_tbl ((a), (b), (c))

/** empty table construction */
#define empty_tbl(atts)      PFpa_empty_tbl (atts)

/** ColumnAttach */
#define attach(a,b,c)        PFpa_attach ((a), (b), (c))

/** cartesian product */
#define cross(a,b)           PFpa_cross ((a),(b))

/** join that preserves the order of the first argument */
#define leftjoin(a,b,c,d)    PFpa_leftjoin ((a), (b), (c), (d))

#if 0
/** NestedLoopJoin */
#define nljoin(a,b,c,d)      PFpa_nljoin ((a), (b), (c), (d))

/** MergeJoin */
#define merge_join(a,b,c,d)  PFpa_merge_join ((a), (b), (c), (d))
#endif

/** standard join operator */
#define eqjoin(a,b,c,d)      PFpa_eqjoin ((a), (b), (c), (d))

/** semijoin operator */
#define semijoin(a,b,c,d)    PFpa_semijoin ((a), (b), (c), (d))

/** thetajoin operator */
#define thetajoin(a,b,c,d)   PFpa_thetajoin ((a), (b), (c), (d))

/** thetajoin operator */
#define unq2_tjoin(a,b,c,d,e,f,g) PFpa_unq2_thetajoin((a), (b), (c), \
                                                 (d), (e), (f), (g))

/** thetajoin operator */
#define unq1_tjoin(a,b,c,d,e,f,g) PFpa_unq1_thetajoin((a), (b), (c), \
                                                 (d), (e), (f), (g))

/** projection operator */
#define project(a,b,c)       PFpa_project ((a), (b), (c))

#define select_(a,b)         PFpa_select ((a), (b))
#define val_select(a,b,c)    PFpa_value_select ((a), (b), (c))

#define append_union(a,b)    PFpa_append_union ((a), (b))
#define merge_union(a,b,c)   PFpa_merge_union ((a), (b), (c))

#define intersect(a,b)       PFpa_intersect ((a), (b))
#define difference(a,b)      PFpa_difference ((a), (b))
/** HashDistinct */
#define sort_distinct(a,b)   PFpa_sort_distinct ((a), (b))
/** StandardSort */
#define std_sort(a,b)        PFpa_std_sort ((a), (b))
/** RefineSort */
#define refine_sort(a,b,c)   PFpa_refine_sort ((a), (b), (c))

#define fun_1to1(a,b,c,d)    PFpa_fun_1to1 ((a), (b), (c), (d))

#define bool_not(a,b,c)      PFpa_bool_not ((a), (b), (c))

#define hash_count(a,b,c)    PFpa_hash_count ((a), (b), (c))

#define aggr(a,b,c,d, e)     PFpa_aggr ((a), (b), (c), (d), (e))

/** a sort specification list is just another attribute list */
#define sortby(...)          PFord_order_intro (__VA_ARGS__)

/** Numbering operators */
#define mark(a,b)            PFpa_mark ((a), (b))
#define rank(a,b,c)          PFpa_rank ((a), (b), (c))
#define mark_grp(a,b,c)      PFpa_mark_grp ((a), (b), (c))

#define type(a,b,c,d)        PFpa_type ((a), (b), (c), (d))
#define type_assert(a,b,c)   PFpa_type_assert ((a), (b), (c))
#define cast(a,b,c,d)        PFpa_cast ((a), (b), (c), (d))

/** StaircaseJoin */
#define llscj_anc(a,b,c,d,e,f,g) PFpa_llscj_anc ((a), (b), (c), \
        (d), (e), (f), (g))
#define llscj_anc_self(a,b,c,d,e,f,g) PFpa_llscj_anc_self ((a), (b), (c), \
        (d), (e), (f), (g))
#define llscj_attr(a,b,c,d,e,f,g) PFpa_llscj_attr ((a), (b), (c), \
        (d), (e), (f), (g))
#define llscj_child(a,b,c,d,e,f,g) PFpa_llscj_child ((a), (b), (c), \
        (d), (e), (f), (g))
#define llscj_desc(a,b,c,d,e,f,g) PFpa_llscj_desc ((a), (b), (c), \
        (d), (e), (f), (g))
#define llscj_desc_self(a,b,c,d,e,f,g) PFpa_llscj_desc_self ((a),(b), (c), \
        (d), (e), (f), (g))
#define llscj_foll(a,b,c,d,e,f,g) PFpa_llscj_foll ((a), (b), (c), \
        (d), (e), (f), (g))
#define llscj_foll_self(a,b,c,d,e,f,g) PFpa_llscj_foll_self ((a),(b), (c), \
        (d), (e), (f), (g))
#define llscj_parent(a,b,c,d,e,f,g) PFpa_llscj_parent ((a), (b), (c), \
        (d), (e), (f), (g))
#define llscj_prec(a,b,c,d,e,f,g) PFpa_llscj_prec ((a), (b), (c), \
        (d), (e), (f), (g))
#define llscj_prec_self(a,b,c,d,e,f,g) PFpa_llscj_prec_self ((a),(b), (c), \
        (d), (e), (f), (g))

#define doc_tbl(a,b,c)       PFpa_doc_tbl ((a), (b), (c))
#define doc_access(a,b,c,d,e) PFpa_doc_access ((a), (b), (c), (d), (e))

/* twig root operator */
#define twig(a,b,c)          PFpa_twig ((a),(b),(c))

/* twig constructor sequence */
#define fcns(a,b)            PFpa_fcns ((a),(b))

/* document node-constructing operator */
#define docnode(a,b,c)       PFpa_docnode ((a),(b),(c))

/* element-constructing operator */
#define element(a,b,c,d)     PFpa_element ((a),(b),(c),(d))

/* attribute-constructing operator */
#define attribute(a,b,c,d)   PFpa_attribute ((a),(b),(c),(d))

/* text node-constructing operator */
#define textnode(a,b,c)      PFpa_textnode ((a),(b),(c))

/* comment-constructing operator */
#define comment(a,b,c)       PFpa_comment ((a),(b),(c))

/* processing instruction-constructing operator */
#define processi(a,b,c,d)    PFpa_processi ((a),(b),(c),(d))

/* constructor content operator (elem|doc) */
#define content(a,b,c,d)     PFpa_content ((a),(b),(c),(d))

#define merge_adjacent(a,b,c,d,e) PFpa_merge_adjacent ((a),(b),(c),(d),(e))

/** roots() operator */
#define roots(a)             PFpa_roots (a)

#define fragment(a)          PFpa_fragment (a)
#define frag_extract(a,b)    PFpa_frag_extract ((a),(b))
#define frag_union(a,b)      PFpa_frag_union ((a), (b))

/** empty fragment list */
#define empty_frag()         PFpa_empty_frag ()

#define error(a,b,c)         PFpa_error ((a), (b), (c))
#define cond_err(a,b,c,d)    PFpa_cond_err ((a), (b), (c), (d))
#define nil()                PFpa_nil ()
#define trace(a,b,c,d)       PFpa_trace ((a),(b),(c),(d))
#define trace_msg(a,b,c,d)   PFpa_trace_msg ((a), (b), (c), (d))
#define trace_map(a,b,c,d)   PFpa_trace_map ((a), (b), (c), (d))

/* recursion operators */
#define rec_fix(a,b)         PFpa_rec_fix ((a),(b))
#define rec_param(a,b)       PFpa_rec_param ((a),(b))
#define rec_arg(a,b,c)       PFpa_rec_arg ((a),(b),(c))
#define rec_base(a,b)        PFpa_rec_base ((a),(b))
#define rec_border(a)        PFpa_rec_border (a)

/* function application */
#define fun_call(a,b,c,d,e,f,g,h) \
        PFpa_fun_call ((a),(b),(c),(d),(e),(f),(g),(h))
#define fun_param(a,b,c)  PFpa_fun_param ((a),(b),(c))  
#define fun_frag_param(a,b,c) PFpa_fun_frag_param ((a),(b),(c))  

#define fn_concat(a,b,c,d)   PFpa_fn_concat ((a), (b), (c), (d))
#define fn_contains(a,b,c,d) PFpa_fn_contains ((a), (b), (c), (d))
#define string_join(a,b,c,d) PFpa_string_join ((a),(b),(c),(d))

/* vim:set shiftwidth=4 expandtab: */
