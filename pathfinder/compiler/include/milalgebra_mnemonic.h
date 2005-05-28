/**
 * @file
 *
 * Mnemonic MIL algebra constructor names
 *
 * This introduces mnemonic abbreviations for PFma_... constructors
 * in mil/milalgebra.c
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

#ifndef MILALGEBRA_MNEMONIC_H
#define MILALGEBRA_MNEMONIC_H

#define lit_oid(o)     PFma_lit_oid (o)
#define lit_int(i)     PFma_lit_int (i)
#define lit_str(s)     PFma_lit_str (s)
#define lit_bit(b)     PFma_lit_bit (b)
#define lit_dbl(d)     PFma_lit_dbl (d)

#define serialize(p,i1,i2,i3,i4,i5,i6) \
    PFma_serialize ((p), (i1), (i2), (i3), (i4), (i5), (i6))

#define new(h,t)       PFma_new ((h), (t))
#define insert(n,h,t)  PFma_insert ((n), (h), (t))
#define seqbase(n,s)   PFma_seqbase ((n), (s))
#define project(n,v)   PFma_project ((n), (v))
#define reverse(n)     PFma_reverse (n)
#define sort(n)        PFma_sort (n)
#define ctrefine(a,b)  PFma_ctrefine ((a), (b))
#define join(a,b)      PFma_join ((a), (b))
#define leftjoin(a,b)  PFma_leftjoin ((a), (b))
#define cross(a,b)     PFma_cross ((a), (b))
#define mirror(a)      PFma_mirror (a)
#define kunique(a)     PFma_kunique (a)
#define mark_grp(b,g)  PFma_mark_grp ((b), (g))
#define mark(a,b)      PFma_mark ((a), (b))
#define count(a)       PFma_count (a)
#define append(a,b)    PFma_append ((a), (b))
#define oid(a)         PFma_oid (a)
#define moid(a)        PFma_moid (a)
#define mint(a)        PFma_mint (a)
#define mstr(a)        PFma_mstr (a)
#define mdbl(a)        PFma_mdbl (a)
#define mbit(a)        PFma_mbit (a)
#define madd(a,b)      PFma_madd ((a), (b))
#define msub(a,b)      PFma_msub ((a), (b))
#define mmult(a,b)     PFma_mmult ((a), (b))
#define mdiv(a,b)      PFma_mdiv ((a), (b))

#endif  /* MILALGEBRA_MNEMONIC_H */

/* vim:set shiftwidth=4 expandtab: */

