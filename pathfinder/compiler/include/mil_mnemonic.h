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
 *  The contents of this file are subject to the MonetDB Public
 *  License Version 1.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 *
 *  The Original Code is the ``Pathfinder'' system. The Initial
 *  Developer of the Original Code is the Database & Information
 *  Systems Group at the University of Konstanz, Germany. Portions
 *  created by U Konstanz are Copyright (C) 2000-2004 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
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

/** order() operator (destructively re-orders a BAT by its head) */
#define order(a) PFmil_order((a))

/** select() operator */
#define select(a,b) PFmil_select((a), (b))

/** project() operator */
#define project(a,b) PFmil_project((a), (b))

/** mark() operator */
#define mark(a,b) PFmil_mark((a), (b))

/** mark_grp() operator */
#define mark_grp(a,b) PFmil_mark_grp((a), (b))

/** insert() function to insert a single BUN (3 arguments) */
#define insert(a,b,c) PFmil_insert((a), (b), (c))

/** insert() function to insert a BAT at once (2 arguments) */
#define binsert(a,b) PFmil_binsert((a), (b))

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

/** copy() operator */
#define copy(a) PFmil_copy(a)

/** sort() function */
#define sort(a) PFmil_sort(a)

/** ctrefine() function */
#define ctrefine(a,b) PFmil_ctrefine(a,b)

/** max() operator */
#define max(a) PFmil_max(a)

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

/** multiplexed comparison (greater than) */
#define mgt(a,b) PFmil_mgt ((a), (b))

/** multiplexed comparison (equality) */
#define meq(a,b) PFmil_meq ((a), (b))

/** multiplexed boolean negation */
#define mnot(a) PFmil_mnot (a)

/** serialization function */
#define serialize(prefix, a, b, c, d, e, f, g) \
    PFmil_ser ((prefix), (a), (b), (c), (d), (e), (f), (g))
