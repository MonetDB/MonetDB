/**
 * @file
 *
 * Mnemonic abbreviations for logical algebra constructors.
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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

/* Also import generic algebra stuff */
#include "algebra_mnemonic.h"

/** dummy operator */
#define dummy(a) PFla_dummy (a)

/** serialization */
#define serialize(a,b,c,d) PFla_serialize ((a),(b),(c),(d))

/** literal table construction */
#define lit_tbl(...)      PFla_lit_tbl (__VA_ARGS__)

/** empty table construction */
#define empty_tbl(atts)   PFla_empty_tbl (atts)

/** ColumnAttach operator */
#define attach(a,b,c)   PFla_attach ((a),(b),(c))

/** cartesian product */
#define cross(a,b)        PFla_cross ((a),(b))

/** equi-join */
#define eqjoin(a,b,c,d)   PFla_eqjoin ((a),(b),(c),(d))

/** semi-join */
#define semijoin(a,b,c,d) PFla_semijoin ((a),(b),(c),(d))

/** projection operator */
#define project(...)      PFla_project (__VA_ARGS__)

/* selection operator */
#define select_(a,b)      PFla_select ((a),(b))

/** disjoint union (where both argument must have the same schema) */
#define disjunion(a,b)    PFla_disjunion ((a),(b))

/** intersection (where both argument must have the same schema) */
#define intersect(a,b)    PFla_intersect ((a),(b))

/** difference (where both argument must have the same schema) */
#define difference(a,b)   PFla_difference ((a),(b))

/* duplicate elimination operator */
#define distinct(a)       PFla_distinct ((a))

/* addition operator */
#define add(a,b,c,d)      PFla_add ((a),(b),(c),(d))

/* subtraction operator */
#define subtract(a,b,c,d) PFla_subtract ((a),(b),(c),(d))

/* multiplication operator */
#define multiply(a,b,c,d) PFla_multiply ((a),(b),(c),(d))

/* division operator */
#define divide(a,b,c,d)   PFla_divide ((a),(b),(c),(d))

/* modulo operator */
#define modulo(a,b,c,d)   PFla_modulo ((a),(b),(c),(d))

/* numeric equal operator */
#define eq(a,b,c,d)       PFla_eq ((a),(b),(c),(d))

/* numeric greater-than operator */
#define gt(a,b,c,d)       PFla_gt ((a),(b),(c),(d))

/* numeric negation operator */
#define neg(a,b,c)        PFla_neg ((a),(b),(c))

/* boolean AND operator */
#define and(a,b,c,d)      PFla_and ((a),(b),(c),(d))

/* boolean OR operator */
#define or(a,b,c,d)       PFla_or ((a),(b),(c),(d))

/* boolean NOT operator */
#define not(a,b,c)        PFla_not ((a),(b),(c))

/* operator applying a (partitioned) aggregation function on a column */
#define aggr(a,b,c,d,e)      PFla_aggr ((a),(b),(c),(d),(e))

/* (partitioned) row counting operator */
#define count(a,b,c)      PFla_count ((a),(b),(c))

/** rownum operator */
#define rownum(a,b,c,d)   PFla_rownum ((a),(b),(c),(d))

/** number operator */
#define number(a,b,c)     PFla_number ((a),(b),(c))

/** type test operator */
#define type(a,b,c,d)     PFla_type ((a),(b),(c),(d))

/** type restriction operators */
#define type_assert_pos(a,b,c)   PFla_type_assert ((a),(b),(c),(true))
#define type_assert_neg(a,b,c)   PFla_type_assert ((a),(b),(c),(false))

/* type cast operator */
#define cast(a,b,c,d)     PFla_cast ((a),(b),(c),(d))

/* algebra seqty1 operator (see PFla_seqty1()) */
#define seqty1(a,b,c,d)   PFla_seqty1((a), (b), (c), (d))

/* all operator (see PFla_all()) */
#define all(a,b,c,d)      PFla_all((a), (b), (c), (d))

/** staircase join */
#define scjoin(a,b,c,d,e,f,g) PFla_scjoin ((a),(b),(c),(d),(e),(f),(g))
#define dup_scjoin(a,b,c,d,e,f) PFla_dup_scjoin ((a),(b),(c),(d),(e),(f))

/** document table */
#define doc_tbl(a,b,c,d)    PFla_doc_tbl((a),(b),(c),(d))

/** document content access */
#define doc_access(a,b,c,d,e) PFla_doc_access ((a), (b), (c), (d), (e))

/* element-constructing operator */
#define element(a,b,c,d,e,f,g,h,i,j) \
        PFla_element ((a),(b),(c),(d),(e),(f),(g),(h),(i),(j))

/* attribute-constructing operator */
#define attribute(a,b,c,d) PFla_attribute ((a),(b),(c),(d))

/* text node-constructing operator */
#define textnode(a,b,c)   PFla_textnode ((a),(b),(c))

/* document node-constructing operator */
#define docnode(a,b)      PFla_docnode ((a),(b))

/* comment-constructing operator */
#define comment(a)        PFla_comment ((a))

/* processing instruction-constructing operator */
#define processi(a)       PFla_processi ((a))

/* constructor for fs:item-sequence-to-node-sequence() functionality */
#define pos_merge_str(a)  PFla_pos_merge_str ((a))

/* constructor for pf:merge-adjacent-text-nodes() functionality */
#define merge_adjacent(a,b,c,d,e,f,g,h) \
        PFla_pf_merge_adjacent_text_nodes ((a),(b),(c),(d),(e),(f),(g),(h))

/** constructor for algebraic representation of newly ceated xml nodes */
#define roots(a)          PFla_roots ((a))

/** constructor for a new fragment, containing newly ceated xml nodes */
#define fragment(a)       PFla_fragment ((a))

/** constructor for an empty fragment */
#define empty_frag()      PFla_empty_frag ()

/* conditional error operator */
#define cond_err(a,b,c,d) PFla_cond_err ((a),(b),(c),(d))

/* recursion operators */
#define rec_fix(a,b) PFla_rec_fix ((a),(b))
#define rec_param(a,b) PFla_rec_param ((a),(b))
#define rec_nil() PFla_rec_nil ()
#define rec_arg(a,b,c) PFla_rec_arg ((a),(b),(c))
#define rec_base(a) PFla_rec_base (a)

/* constructors for built-in functions */
#define fn_concat(a,b,c,d)  PFla_fn_concat ((a), (b), (c), (d))
#define fn_contains(a,b,c,d)  PFla_fn_contains ((a), (b), (c), (d))
#define fn_string_join(a,b,c,d,e,f,g,h,i) \
        PFla_fn_string_join ((a),(b),(c),(d),(e),(f),(g),(h),(i))

/** a sort specification list is just another attribute list */
#define sortby(...)     PFord_order_intro (__VA_ARGS__)

/* vim:set shiftwidth=4 expandtab: */
