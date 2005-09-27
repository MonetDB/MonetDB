/**
 * @file
 *
 * Mnemonic MIL constructor names
 *
 * This introduces mnemonic abbreviations for PFmil_... constructors
 * in mil/mil.c
 *
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
 * The Initial Developer of the Original Code is the Database &
 * Information Systems Group at the University of Konstanz, Germany.
 * Portions created by the University of Konstanz are Copyright (C)
 * 2000-2005 University of Konstanz.  All Rights Reserved.
 *
 * $Id$
 */

/** literal integers */
#define lit_int(i) PFmil_lit_int (i)

/** literal strings */
#define lit_str(i) PFmil_lit_str (i)

/** literal oids */
#define lit_oid(i) PFmil_lit_oid (i)

/** literal dbls */
#define lit_dbl(i) PFmil_lit_dbl (i)

/** literal bits */
#define lit_bit(i) PFmil_lit_bit (i)

/** MIL variables */
#define var(v) PFmil_var(v)

/** MIL types */
#define type(t) PFmil_type(t)

/** `no operation' */
#define nop() PFmil_nop ()

/** `nil' */
#define nil() PFmil_nil ()

/** shortcut for MIL variable `unused' */
#define unused() PFmil_unused ()

#define if_(a,b,c) PFmil_if ((a), (b), (c))

/** assignment statement, combined with variable declaration */
#define assgn(a,b) PFmil_assgn ((a),(b))

/** assignment statement */
#define reassgn(a,b) PFmil_reassgn ((a),(b))

/** construct new BAT */
#define new(a,b) PFmil_new ((a),(b))

/** sequence of MIL statements */
#define seq(...) PFmil_seq (__VA_ARGS__)

/** seqbase() operator */
#define seqbase(a,b) PFmil_seqbase((a), (b))

/** key() function */
#define key(a,b) PFmil_key((a), (b))

/** order() operator (destructively re-orders a BAT by its head) */
#define order(a) PFmil_order((a))

/** select() operator */
#define select_(a,b) PFmil_select((a), (b))

/** select() operator */
#define select2(a,b,c) PFmil_select2((a), (b), (c))

/** uselect() operator */
#define uselect(a,b) PFmil_uselect((a), (b))

/** project() operator */
#define project(a,b) PFmil_project((a), (b))

/** mark() operator */
#define mark(a,b) PFmil_mark((a), (b))

/** mark_grp() operator */
#define mark_grp(a,b) PFmil_mark_grp((a), (b))

/** fetch() function */
#define fetch(a,b) PFmil_fetch((a), (b))

/** insert() function to insert a single BUN (3 arguments) */
#define insert(a,b,c) PFmil_insert((a), (b), (c))

/** insert() function to insert a BAT at once (2 arguments) */
#define binsert(a,b) PFmil_binsert((a), (b))

/** append() function */
#define bappend(a,b) PFmil_bappend ((a), (b))

/** set access restrictions to a BAT */
#define access(a,b) PFmil_access((a), (b))

/** cross() operator */
#define cross(a,b) PFmil_cross((a), (b))

/** join() operator */
#define join(a,b) PFmil_join((a), (b))

/** leftjoin() operator */
#define leftjoin(a,b) PFmil_leftjoin((a), (b))

/** reverse() operator */
#define reverse(a) PFmil_reverse(a)

/** mirror() operator */
#define mirror(a) PFmil_mirror(a)

/** kunique() operator */
#define kunique(a) PFmil_kunique(a)

/** kunion() operator */
#define kunion(a,b) PFmil_kunion((a),(b))

/** kdiff() operator */
#define kdiff(a,b) PFmil_kdiff((a),(b))

/** merged_union() function */
#define merged_union(a) PFmil_merged_union (a)

/** build argument list for MIL variable argument list functions */
#define arg(a,b) PFmil_arg ((a), (b))

/** copy() operator */
#define copy(a) PFmil_copy(a)

/** sort() function */
#define sort(a) PFmil_sort(a)

/** ctgroup() function */
#define ctgroup(a) PFmil_ctgroup (a)

/** ctmap() function */
#define ctmap(a) PFmil_ctmap (a)

/** ctextend() function */
#define ctextend(a) PFmil_ctextend (a)

/** ctrefine() function */
#define ctrefine(a,b) PFmil_ctrefine(a,b)

/** ctderive() function */
#define ctderive(a,b) PFmil_ctderive(a,b)

/** max() operator */
#ifdef max
#undef max
#endif
#define max(a) PFmil_max(a)

/** count() operator and grouped count */
#define count(a) PFmil_count(a)
#define gcount(a) PFmil_gcount(a)

/** type cast */
#define cast(type,e) PFmil_cast ((type), (e))

/** multiplexed type cast */
#define mcast(type,e) PFmil_mcast ((type), (e))

/** arithmetic add */
#define add(a,b) PFmil_add ((a), (b))

/** multiplexed arithmetic add */
#define madd(a,b) PFmil_madd ((a), (b))

/** multiplexed arithmetic subtract */
#define msub(a,b) PFmil_msub ((a), (b))

/** multiplexed arithmetic multiply */
#define mmult(a,b) PFmil_mmult ((a), (b))

/** multiplexed arithmetic divide */
#define mdiv(a,b) PFmil_mdiv ((a), (b))

/** multiplexed arithmetic modulo */
#define mmod(a,b) PFmil_mmod ((a), (b))

/** multiplexed comparison (greater than) */
#define mgt(a,b) PFmil_mgt ((a), (b))

/** multiplexed comparison (equality) */
#define meq(a,b) PFmil_meq ((a), (b))

/** multiplexed boolean negation */
#define mnot(a) PFmil_mnot (a)

/** multiplexed isnil() operator `[isnil]()' */
#define misnil(a) PFmil_misnil (a)

/** multiplexed ifthenelse() operator `[ifthenelse]()' */
#define mifthenelse(a,b,c) PFmil_mifthenelse ((a), (b), (c))

/** create new (empty) working set */
#define new_ws() PFmil_new_ws ()

/** positional multijoin with a working set `mposjoin (a, b, c)' */
#define mposjoin(a,b,c) PFmil_mposjoin ((a), (b), (c))

/** multijoin with a working set `mvaljoin (a, b, c)' */
#define mvaljoin(a,b,c) PFmil_mvaljoin ((a), (b), (c))

/** MonetDB bat() function */
#define bat(a) PFmil_bat (a)

#define doc_tbl(a,b) PFmil_doc_tbl ((a), (b))

/* staircase join variants (ancestor axis) */
#define llscj_anc(a,b,c,d,e) \
    PFmil_llscj_anc ((a), (b), (c), (d), (e))
#define llscj_anc_elem(a,b,c,d,e) \
    PFmil_llscj_anc_elem ((a), (b), (c), (d), (e))
#define llscj_anc_text(a,b,c,d,e) \
    PFmil_llscj_anc_text ((a), (b), (c), (d), (e))
#define llscj_anc_comm(a,b,c,d,e) \
    PFmil_llscj_anc_comm ((a), (b), (c), (d), (e))
#define llscj_anc_pi(a,b,c,d,e) \
    PFmil_llscj_anc_pi ((a), (b), (c), (d), (e))
#define llscj_anc_elem_nsloc(a,b,c,d,e,f,g) \
    PFmil_llscj_anc_elem_nsloc ((a), (b), (c), (d), (e), (f), (g))
#define llscj_anc_elem_loc(a,b,c,d,e,f) \
    PFmil_llscj_anc_elem_loc ((a), (b), (c), (d), (e), (f))
#define llscj_anc_elem_ns(a,b,c,d,e,f) \
    PFmil_llscj_anc_elem_ns ((a), (b), (c), (d), (e), (f))
#define llscj_anc_pi_targ(a,b,c,d,e,f) \
    PFmil_llscj_anc_pi_targ ((a), (b), (c), (d), (e), (f))

/* staircase join variants (ancestor-or-self axis) */
#define llscj_anc_self(a,b,c,d,e) \
    PFmil_llscj_anc_self ((a), (b), (c), (d), (e))
#define llscj_anc_self_elem(a,b,c,d,e) \
    PFmil_llscj_anc_self_elem ((a), (b), (c), (d), (e))
#define llscj_anc_self_text(a,b,c,d,e) \
    PFmil_llscj_anc_self_text ((a), (b), (c), (d), (e))
#define llscj_anc_self_comm(a,b,c,d,e) \
    PFmil_llscj_anc_self_comm ((a), (b), (c), (d), (e))
#define llscj_anc_self_pi(a,b,c,d,e) \
    PFmil_llscj_anc_self_pi ((a), (b), (c), (d), (e))
#define llscj_anc_self_elem_nsloc(a,b,c,d,e,f,g) \
    PFmil_llscj_anc_self_elem_nsloc ((a), (b), (c), (d), (e), (f), (g))
#define llscj_anc_self_elem_loc(a,b,c,d,e,f) \
    PFmil_llscj_anc_self_elem_loc ((a), (b), (c), (d), (e), (f))
#define llscj_anc_self_elem_ns(a,b,c,d,e,f) \
    PFmil_llscj_anc_self_elem_ns ((a), (b), (c), (d), (e), (f))
#define llscj_anc_self_pi_targ(a,b,c,d,e,f) \
    PFmil_llscj_anc_self_pi_targ ((a), (b), (c), (d), (e), (f))

/* staircase join variants (child axis) */
#define llscj_child(a,b,c,d,e) \
    PFmil_llscj_child ((a), (b), (c), (d), (e))
#define llscj_child_elem(a,b,c,d,e) \
    PFmil_llscj_child_elem ((a), (b), (c), (d), (e))
#define llscj_child_text(a,b,c,d,e) \
    PFmil_llscj_child_text ((a), (b), (c), (d), (e))
#define llscj_child_comm(a,b,c,d,e) \
    PFmil_llscj_child_comm ((a), (b), (c), (d), (e))
#define llscj_child_pi(a,b,c,d,e) \
    PFmil_llscj_child_pi ((a), (b), (c), (d), (e))
#define llscj_child_elem_nsloc(a,b,c,d,e,f,g) \
    PFmil_llscj_child_elem_nsloc ((a), (b), (c), (d), (e), (f), (g))
#define llscj_child_elem_loc(a,b,c,d,e,f) \
    PFmil_llscj_child_elem_loc ((a), (b), (c), (d), (e), (f))
#define llscj_child_elem_ns(a,b,c,d,e,f) \
    PFmil_llscj_child_elem_ns ((a), (b), (c), (d), (e), (f))
#define llscj_child_pi_targ(a,b,c,d,e,f) \
    PFmil_llscj_child_pi_targ ((a), (b), (c), (d), (e), (f))

/* staircase join variants (descendant axis) */
#define llscj_desc(a,b,c,d,e) \
    PFmil_llscj_desc ((a), (b), (c), (d), (e))
#define llscj_desc_elem(a,b,c,d,e) \
    PFmil_llscj_desc_elem ((a), (b), (c), (d), (e))
#define llscj_desc_text(a,b,c,d,e) \
    PFmil_llscj_desc_text ((a), (b), (c), (d), (e))
#define llscj_desc_comm(a,b,c,d,e) \
    PFmil_llscj_desc_comm ((a), (b), (c), (d), (e))
#define llscj_desc_pi(a,b,c,d,e) \
    PFmil_llscj_desc_pi ((a), (b), (c), (d), (e))
#define llscj_desc_elem_nsloc(a,b,c,d,e,f,g) \
    PFmil_llscj_desc_elem_nsloc ((a), (b), (c), (d), (e), (f), (g))
#define llscj_desc_elem_loc(a,b,c,d,e,f) \
    PFmil_llscj_desc_elem_loc ((a), (b), (c), (d), (e), (f))
#define llscj_desc_elem_ns(a,b,c,d,e,f) \
    PFmil_llscj_desc_elem_ns ((a), (b), (c), (d), (e), (f))
#define llscj_desc_pi_targ(a,b,c,d,e,f) \
    PFmil_llscj_desc_pi_targ ((a), (b), (c), (d), (e), (f))

/* staircase join variants (descendant-or-self axis) */
#define llscj_desc_self(a,b,c,d,e) \
    PFmil_llscj_desc_self ((a), (b), (c), (d), (e))
#define llscj_desc_self_elem(a,b,c,d,e) \
    PFmil_llscj_desc_self_elem ((a), (b), (c), (d), (e))
#define llscj_desc_self_text(a,b,c,d,e) \
    PFmil_llscj_desc_self_text ((a), (b), (c), (d), (e))
#define llscj_desc_self_comm(a,b,c,d,e) \
    PFmil_llscj_desc_self_comm ((a), (b), (c), (d), (e))
#define llscj_desc_self_pi(a,b,c,d,e) \
    PFmil_llscj_desc_self_pi ((a), (b), (c), (d), (e))
#define llscj_desc_self_elem_nsloc(a,b,c,d,e,f,g) \
    PFmil_llscj_desc_self_elem_nsloc ((a), (b), (c), (d), (e), (f), (g))
#define llscj_desc_self_elem_loc(a,b,c,d,e,f) \
    PFmil_llscj_desc_self_elem_loc ((a), (b), (c), (d), (e), (f))
#define llscj_desc_self_elem_ns(a,b,c,d,e,f) \
    PFmil_llscj_desc_self_elem_ns ((a), (b), (c), (d), (e), (f))
#define llscj_desc_self_pi_targ(a,b,c,d,e,f) \
    PFmil_llscj_desc_self_pi_targ ((a), (b), (c), (d), (e), (f))

/* staircase join variants (following axis) */
#define llscj_foll(a,b,c,d,e) \
    PFmil_llscj_foll ((a), (b), (c), (d), (e))
#define llscj_foll_elem(a,b,c,d,e) \
    PFmil_llscj_foll_elem ((a), (b), (c), (d), (e))
#define llscj_foll_text(a,b,c,d,e) \
    PFmil_llscj_foll_text ((a), (b), (c), (d), (e))
#define llscj_foll_comm(a,b,c,d,e) \
    PFmil_llscj_foll_comm ((a), (b), (c), (d), (e))
#define llscj_foll_pi(a,b,c,d,e) \
    PFmil_llscj_foll_pi ((a), (b), (c), (d), (e))
#define llscj_foll_elem_nsloc(a,b,c,d,e,f,g) \
    PFmil_llscj_foll_elem_nsloc ((a), (b), (c), (d), (e), (f), (g))
#define llscj_foll_elem_loc(a,b,c,d,e,f) \
    PFmil_llscj_foll_elem_loc ((a), (b), (c), (d), (e), (f))
#define llscj_foll_elem_ns(a,b,c,d,e,f) \
    PFmil_llscj_foll_elem_ns ((a), (b), (c), (d), (e), (f))
#define llscj_foll_pi_targ(a,b,c,d,e,f) \
    PFmil_llscj_foll_pi_targ ((a), (b), (c), (d), (e), (f))

/* staircase join variants (following-sibling axis) */
#define llscj_foll_sibl(a,b,c,d,e) \
    PFmil_llscj_foll_sibl ((a), (b), (c), (d), (e))
#define llscj_foll_sibl_elem(a,b,c,d,e) \
    PFmil_llscj_foll_sibl_elem ((a), (b), (c), (d), (e))
#define llscj_foll_sibl_text(a,b,c,d,e) \
    PFmil_llscj_foll_sibl_text ((a), (b), (c), (d), (e))
#define llscj_foll_sibl_comm(a,b,c,d,e) \
    PFmil_llscj_foll_sibl_comm ((a), (b), (c), (d), (e))
#define llscj_foll_sibl_pi(a,b,c,d,e) \
    PFmil_llscj_foll_sibl_pi ((a), (b), (c), (d), (e))
#define llscj_foll_sibl_elem_nsloc(a,b,c,d,e,f,g) \
    PFmil_llscj_foll_sibl_elem_nsloc ((a), (b), (c), (d), (e), (f), (g))
#define llscj_foll_sibl_elem_loc(a,b,c,d,e,f) \
    PFmil_llscj_foll_sibl_elem_loc ((a), (b), (c), (d), (e), (f))
#define llscj_foll_sibl_elem_ns(a,b,c,d,e,f) \
    PFmil_llscj_foll_sibl_elem_ns ((a), (b), (c), (d), (e), (f))
#define llscj_foll_sibl_pi_targ(a,b,c,d,e,f) \
    PFmil_llscj_foll_sibl_pi_targ ((a), (b), (c), (d), (e), (f))

/* staircase join variants (parent axis) */
#define llscj_parent(a,b,c,d,e) \
    PFmil_llscj_parent ((a), (b), (c), (d), (e))
#define llscj_parent_elem(a,b,c,d,e) \
    PFmil_llscj_parent_elem ((a), (b), (c), (d), (e))
#define llscj_parent_text(a,b,c,d,e) \
    PFmil_llscj_parent_text ((a), (b), (c), (d), (e))
#define llscj_parent_comm(a,b,c,d,e) \
    PFmil_llscj_parent_comm ((a), (b), (c), (d), (e))
#define llscj_parent_pi(a,b,c,d,e) \
    PFmil_llscj_parent_pi ((a), (b), (c), (d), (e))
#define llscj_parent_elem_nsloc(a,b,c,d,e,f,g) \
    PFmil_llscj_parent_elem_nsloc ((a), (b), (c), (d), (e), (f), (g))
#define llscj_parent_elem_loc(a,b,c,d,e,f) \
    PFmil_llscj_parent_elem_loc ((a), (b), (c), (d), (e), (f))
#define llscj_parent_elem_ns(a,b,c,d,e,f) \
    PFmil_llscj_parent_elem_ns ((a), (b), (c), (d), (e), (f))
#define llscj_parent_pi_targ(a,b,c,d,e,f) \
    PFmil_llscj_parent_pi_targ ((a), (b), (c), (d), (e), (f))

/* staircase join variants (preceding axis) */
#define llscj_prec(a,b,c,d,e) \
    PFmil_llscj_prec ((a), (b), (c), (d), (e))
#define llscj_prec_elem(a,b,c,d,e) \
    PFmil_llscj_prec_elem ((a), (b), (c), (d), (e))
#define llscj_prec_text(a,b,c,d,e) \
    PFmil_llscj_prec_text ((a), (b), (c), (d), (e))
#define llscj_prec_comm(a,b,c,d,e) \
    PFmil_llscj_prec_comm ((a), (b), (c), (d), (e))
#define llscj_prec_pi(a,b,c,d,e) \
    PFmil_llscj_prec_pi ((a), (b), (c), (d), (e))
#define llscj_prec_elem_nsloc(a,b,c,d,e,f,g) \
    PFmil_llscj_prec_elem_nsloc ((a), (b), (c), (d), (e), (f), (g))
#define llscj_prec_elem_loc(a,b,c,d,e,f) \
    PFmil_llscj_prec_elem_loc ((a), (b), (c), (d), (e), (f))
#define llscj_prec_elem_ns(a,b,c,d,e,f) \
    PFmil_llscj_prec_elem_ns ((a), (b), (c), (d), (e), (f))
#define llscj_prec_pi_targ(a,b,c,d,e,f) \
    PFmil_llscj_prec_pi_targ ((a), (b), (c), (d), (e), (f))

/* staircase join variants (preceding-sibling axis) */
#define llscj_prec_sibl(a,b,c,d,e) \
    PFmil_llscj_prec_sibl ((a), (b), (c), (d), (e))
#define llscj_prec_sibl_elem(a,b,c,d,e) \
    PFmil_llscj_prec_sibl_elem ((a), (b), (c), (d), (e))
#define llscj_prec_sibl_text(a,b,c,d,e) \
    PFmil_llscj_prec_sibl_text ((a), (b), (c), (d), (e))
#define llscj_prec_sibl_comm(a,b,c,d,e) \
    PFmil_llscj_prec_sibl_comm ((a), (b), (c), (d), (e))
#define llscj_prec_sibl_pi(a,b,c,d,e) \
    PFmil_llscj_prec_sibl_pi ((a), (b), (c), (d), (e))
#define llscj_prec_sibl_elem_nsloc(a,b,c,d,e,f,g) \
    PFmil_llscj_prec_sibl_elem_nsloc ((a), (b), (c), (d), (e), (f), (g))
#define llscj_prec_sibl_elem_loc(a,b,c,d,e,f) \
    PFmil_llscj_prec_sibl_elem_loc ((a), (b), (c), (d), (e), (f))
#define llscj_prec_sibl_elem_ns(a,b,c,d,e,f) \
    PFmil_llscj_prec_sibl_elem_ns ((a), (b), (c), (d), (e), (f))
#define llscj_prec_sibl_pi_targ(a,b,c,d,e,f) \
    PFmil_llscj_prec_sibl_pi_targ ((a), (b), (c), (d), (e), (f))

#define string_join(a,b) PFmil_string_join ((a), (b))

#define get_fragment(a) PFmil_get_fragment (a)
#define set_kind(a,b)   PFmil_set_kind ((a), (b))
#define is_fake_project(a) PFmil_is_fake_project (a)
#define chk_order(a)    PFmil_chk_order (a)

/** variable declaration */
#define declare(a) PFmil_declare (a)

/** serialization function */
#define serialize(a) PFmil_ser (a)
#define print(a) PFmil_print (a)
#define col_name(a,b) PFmil_col_name ((a), (b))

/* vim:set shiftwidth=4 expandtab: */
