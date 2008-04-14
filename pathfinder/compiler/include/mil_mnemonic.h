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

/** literal integers */
#define lit_int(i) PFmil_lit_int (i)

/** literal long integers */
#define lit_lng(i) PFmil_lit_lng (i)

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
#define while_(a,b) PFmil_while ((a), (b))

/** assignment statement, combined with variable declaration */
#define assgn(a,b) PFmil_assgn ((a),(b))

/** assignment statement */
#define reassgn(a,b) PFmil_reassgn ((a),(b))

/** construct new BAT */
#define new(a,b) PFmil_new ((a),(b))

/** sequence of MIL statements */
#define seq(...) PFmil_seq (__VA_ARGS__)

/** seqbase() operator */
#define seqbase_lookup(a) PFmil_seqbase_lookup((a))

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

/** exist() operator */
#define exist(a,b) PFmil_exist((a), (b))

/** project() operator */
#define project(a,b) PFmil_project((a), (b))

/** mark() operator */
#define mark(a,b) PFmil_mark((a), (b))

/** hmark() operator */
#define hmark(a,b) PFmil_hmark((a), (b))

/** tmark() operator */
#define tmark(a,b) PFmil_tmark((a), (b))

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
#define append(a,b) PFmil_bappend ((a), (b))

/** set access restrictions to a BAT */
#define access(a,b) PFmil_access((a), (b))

/** cross() operator */
#define cross(a,b) PFmil_cross((a), (b))

/** join() operator */
#define join(a,b) PFmil_join((a), (b))

/** leftjoin() operator */
#define leftjoin(a,b) PFmil_leftjoin((a), (b))

/** leftfetchjoin() operator */
#define leftfetchjoin(a,b) PFmil_leftfetchjoin((a), (b))

/** thetajoin() operator */
#define tjoin(a,b,c,d) PFmil_thetajoin((a), (b), (c), (d))

/** htordered_unique_thetajoin PROC */
#define unq2_tjoin(a,b,c) PFmil_unq2_thetajoin((a), (b), (c))

/** ll_htordered_unique_thetajoin PROC */
#define unq1_tjoin(a,b,c,d,e) PFmil_unq1_thetajoin ((a), (b), (c), (d), (e))

/** combine_node_info PROC */
#define zip_nodes(a,b,c,d,e,f) PFmil_zip_nodes ((a), (b), (c), (d), (e), (f))

/** reverse() operator */
#define reverse(a) PFmil_reverse(a)

/** mirror() operator */
#define mirror(a) PFmil_mirror(a)

/** kunique() operator */
#define kunique(a) PFmil_kunique(a)

/** tunique() operator */
#define tunique(a) PFmil_tunique(a)

/** kunion() operator */
#define kunion(a,b) PFmil_kunion((a),(b))

/** kdiff() operator */
#define kdiff(a,b) PFmil_kdiff((a),(b))

/** kintersect() operator */
#define kintersect(a,b) PFmil_kintersect((a),(b))

/** sintersect() operator */
#define sintersect(a,b) PFmil_sintersect((a),(b))

/** multi column intersection */
#define mc_intersect(a) PFmil_mc_intersect(a)

/** merged_union() function */
#define merged_union(a) PFmil_merged_union (a)

/** multi_merged_union() function */
#define multi_merged_union(a) PFmil_multi_merged_union (a)

/** build argument list for MIL variable argument list functions */
#define arg(a,b) PFmil_arg ((a), (b))

/** copy() operator */
#define copy(a) PFmil_copy(a)

/** sort() function */
#define sort(a,b) PFmil_sort((a),(b))

/** CTgroup() function */
#define ctgroup(a) PFmil_ctgroup (a)

/** CTmap() function */
#define ctmap(a) PFmil_ctmap (a)

/** CTextend() function */
#define ctextend(a) PFmil_ctextend (a)

/** CTrefine() function */
#define ctrefine(a,b,c) PFmil_ctrefine((a),(b),(c))

/** CTderive() function */
#define ctderive(a,b) PFmil_ctderive(a,b)

/** max() operator */
#ifdef max
#undef max
#endif
#define max(a) PFmil_max(a)

#define enumerate(a,b) PFmil_enumerate((a),(b))

/** count() operator and grouped count */
#define count(a) PFmil_count(a)
#define gcount(a) PFmil_gcount(a)
#define egcount(a,b) PFmil_egcount((a),(b))

/** sum() operator and grouped sum */
#define sum(a) PFmil_sum(a)
#define gsum(a) PFmil_gsum(a)
#define egsum(a,b) PFmil_egsum((a),(b))

/** type cast */
#define cast(type,e) PFmil_cast ((type), (e))

/** multiplexed type cast */
#define mcast(type,e) PFmil_mcast ((type), (e))

/** arithmetic add */
#define add(a,b) PFmil_add ((a), (b))

/** multiplexed arithmetic add */
#define madd(a,b) PFmil_madd ((a), (b))

/** arithmetic subtract */
#define sub(a,b) PFmil_sub ((a), (b))

/** multiplexed arithmetic subtract */
#define msub(a,b) PFmil_msub ((a), (b))

/** multiplexed arithmetic multiply */
#define mmult(a,b) PFmil_mmult ((a), (b))

/** arithmetic divide */
#define div_(a,b) PFmil_div ((a), (b))

/** multiplexed arithmetic divide */
#define mdiv(a,b) PFmil_mdiv ((a), (b))

/** multiplexed arithmetic modulo */
#define mmod(a,b) PFmil_mmod ((a), (b))

/** multiplexed arithmetic maximum */
#define mmax(a,b) PFmil_mmax ((a), (b))

/** greater than */
#define gt(a,b) PFmil_gt ((a), (b))

/** equal */
#define eq(a,b) PFmil_eq ((a), (b))

/** multiplexed comparison (greater than) */
#define mgt(a,b) PFmil_mgt ((a), (b))

/** multiplexed comparison (equality) */
#define meq(a,b) PFmil_meq ((a), (b))

/** multiplexed and */
#define mand(a,b) PFmil_mand ((a), (b))

/** multiplexed or */
#define mor(a,b) PFmil_mor ((a), (b))

/** boolean negation */
#define not(a) PFmil_not (a)

/** multiplexed boolean negation */
#define mnot(a) PFmil_mnot (a)

/** multiplexed roundup function */
#define mround_up(a) PFmil_mround_up (a)

/** operator `isnil()' */
#define isnil(a) PFmil_isnil (a)

/** multiplexed isnil() operator `[isnil]()' */
#define misnil(a) PFmil_misnil (a)

/** multiplexed ifthenelse() operator `[ifthenelse]()' */
#define mifthenelse(a,b,c) PFmil_mifthenelse ((a), (b), (c))

/** get the time */
#define usec() PFmil_usec ()

/** create new (empty) working set */
#define new_ws() PFmil_new_ws ()

/** Free an existing working set */
#define destroy_ws(ws) PFmil_destroy_ws (ws)

/** positional multijoin with a working set `mposjoin (a, b, c)' */
#define mposjoin(a,b,c) PFmil_mposjoin ((a), (b), (c))

/** multijoin with a working set `mvaljoin (a, b, c)' */
#define mvaljoin(a,b,c) PFmil_mvaljoin ((a), (b), (c))

/** MonetDB bat() function */
#define bat(a) PFmil_bat (a)

/** MonetDB catch function + assignment 'a0000 := CATCH()' */
#define catch_(a,b) PFmil_catch ((a),(b))

/** MonetDB ERROR() function */
#define error(a) PFmil_error (a)

/* procs in the pathfinder module */
#define doc_tbl(a,b) PFmil_doc_tbl ((a), (b))
#define attribute(a,b,c) PFmil_attribute ((a), (b), (c))
#define element(a,b,c,d,e,f,g,h) PFmil_element ((a),(b),(c),(d),(e),(f),(g),(h))
#define empty_element(a,b) PFmil_empty_element ((a), (b))
#define textnode(a,b) PFmil_textnode ((a), (b))
#define add_qname(a,b,c,d) PFmil_add_qname ((a), (b), (c), (d))
#define add_qnames(a,b,c,d) PFmil_add_qnames ((a), (b), (c), (d))
#define add_content(a,b,c)  PFmil_add_content ((a), (b), (c))
#define check_qnames(a) PFmil_check_qnames ((a))

/** Multiplexed search() function `[search](a,b)' */
#define msearch(a,b) PFmil_msearch ((a), (b))

/** Multiplexed string() function `[string](a,b)' */
#define mstring(a,b) PFmil_mstring ((a), (b))

/** Multiplexed string() function `[string](a,b,c)' */
#define mstring2(a,b,c) PFmil_mstring2 ((a), (b), (c))

/** Multiplexed startsWith() function `[startsWith](a,b)' */
#define mstarts_with(a,b) PFmil_mstarts_with ((a), (b))

/** Multiplexed endsWith() function `[endsWith](a,b)' */
#define mends_with(a,b) PFmil_mends_with ((a), (b))

/** Multiplexed length() function `[length](a)' */
#define mlength(a)  PFmil_mlength ((a))

/** Multiplexed toUpper() function `[toUpper](a)' */
#define mtoUpper(a) PFmil_mtoUpper ((a))

/** Multiplexed toLower() function `[toLower](a)' */
#define mtoLower(a) PFmil_mtoLower ((a))

/** Multiplexed translate() function `[translate](a,b,c)' */
#define mtranslate(a,b,c) PFmil_mtranslate ((a),(b),(c))

/** Multiplexed normSpace() function `[normSpace](a)' */
#define mnorm_space(a) PFmil_mnorm_space ((a))

/** Multiplexed pcre_match() function `[pcre_match](a,b)' */
#define mpcre_match(a,b) PFmil_mpcre_match ((a),(b))

/** Multiplexed pcre_match() function `[pcre_match](a,b,c)' */
#define mpcre_match_flag(a,b,c) PFmil_mpcre_match_flag ((a), (b), (c))

/** Multiplexed pcre_replace() function `[pcre_replace](a,b,c,d)' */
#define mpcre_replace(a,b,c,d) PFmil_mpcre_replace ((a),(b),(c),(d))

/** general purpose staircase join */
#define step(a,b,c,d,e,f,g,h,i,j,k,l) \
    PFmil_step ((a), (b), (c), (d), (e), (f), (g), (h), (i), (j), (k), (l))

#define merge_adjacent(a,b,c,d) PFmil_merge_adjacent ((a), (b), (c), (d))
#define string_join(a,b) PFmil_string_join ((a), (b))

#define get_fragment(a) PFmil_get_fragment (a)
#define set_kind(a,b)   PFmil_set_kind ((a), (b))
#define materialize(a,b) PFmil_materialize ((a),(b))
#define assert_order(a) PFmil_assert_order (a)
#define chk_order(a)    PFmil_chk_order (a)

/** variable declaration */
#define declare(a) PFmil_declare (a)

/** serialization function */
#define serialize(a) PFmil_ser (a)
#define trace(a) PFmil_trace (a)
#define print(a) PFmil_print (a)
#define col_name(a,b) PFmil_col_name ((a), (b))
#define comment(...) PFmil_comment (__VA_ARGS__)

/** module loading */
#define module(a) PFmil_module (a)

/** play update tape function */
#define update_tape(a) PFmil_upd (a)

/** play docmgmt tape function */
#define docmgmt_tape(a) PFmil_docmgmt (a)

/** mil document functions */
#define ws_collection_root(a,b) PFmil_ws_collection_root ((a), (b))
#define ws_documents(a,b) PFmil_ws_documents ((a), (b))
#define ws_documents_str(a,b,c) PFmil_ws_documents ((a), (b), (c))
#define ws_docname(a,b,c,d) PFmil_ws_docname ((a), (b), (c), (d))
#define ws_collections(a,b) PFmil_ws_collections ((a), (b))

#ifdef HAVE_PFTIJAH

/** pftijah main query handler */
#define tj_query_handler(a,b,c,d,e,f,g) PFmil_tj_query_handler ((a), (b), (c), (d), (e), (f), (g))

/** pftijah computes nodes from id's */
#define tj_query_nodes(a,b,c)   PFmil_tj_query_nodes ((a), (b), (c) )

/** pftijah computes score from id's and nodes */
#define tj_query_score(a,b,c,d) PFmil_tj_query_score ((a), (b), (c), (d))

/** pftijah helper fun to pack algebra operands */
#define tj_pfop(a,b,c,d) PFmil_tj_pfop ((a), (b), (c), (d) )

/** pftijah function  to handle documents and ft-index modifications at the
 *  then end of a transaction */
#define tj_docmgmt_tape(a,b,c,d,e,f) PFmil_tj_docmgmt_tape ((a), (b), (c), (d), (e), (f))

/** add an ft-index event to the tape to be handled later by tj_docmgmt_tape function */
#define tj_add_fti_tape(a,b,c,d,e,f) PFmil_tj_add_fti_tape ((a), (b), (c), (d), (e), (f))

#define tj_tokenize(a) PFmil_tj_tokenize ((a))

#endif

/* vim:set shiftwidth=4 expandtab: */
