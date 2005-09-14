/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Pathfinder's implemenation of 
 *
 *  - the XQuery subtyping relation `<:',
 *  - the XQuery ``can be promoted to'' relation,
 *  - the disjointness `||' for two types.
 *
 * These relationship for XQuery types is decided using a variant of
 * Valentin Antimirov's regular inequalities [1] (a similar approach
 * has been investigated by the XOBE project [2]).
 *
 *
 * References
 *
 * [1] Valentin Antimirov, Rewriting Regular Inequalities, LNCS 965
 *     (Fundamentals of Computation), pages 116--125, 1994.
 *
 * [2] Martin Kempa, Volker Linnemann, Type-Checking in XOBE, 
 *     pages 227--246, BTW 2003.
 *
 * [3] Haruo Hosoya, Jerome Vouillon, Benjamin C. Pierce,
 *     Regular Expression Types for XML, pages 11--122, ACM SIGPLAN, 2000
 *
 * The routines in this module rely on two well-formedness constraints
 * for recursive types (these constraints ensure regularity of the
 * system of all defined types):
 *
 * (1) recursive types may appear in tail positions only,
 * (2) recursive types must be preceded by at least one non-nullable type
 *
 * - Constraint (1) rules out  t = int, t | int, t, str | t
 *                                               ^        ^
 *                                               non-tail positions
 *   
 * - Constraint (2) rules out  t = (), t | int*, t | int, t
 *                                     ^         ^ 
 *                                     not preceded by non-nullable type
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

#include "pathfinder.h"

/* CHAR_BIT */
#include <limits.h>
#include <assert.h>

#include "subtyping.h"

/* PFns_pf */
#include "ns.h"
/* PFarray_t */
#include "array.h"
#include "oops.h"
#include "mem.h"

/**
 * We try to account for the algebraic nature of Antimirov's subtyping
 * algorithm by using a functional (non-destructive) style of programming
 * in this module:
 *
 * -  A function f transforming a type t into a type t' does not alter
 *    type t and instead constructs a new type t'.  Types t and t', however,
 *    may share parts, e.g.:
 *
 *    @verbatim
                          t = c1    c1' = t'  
                             /  \  /  \
                            c2   c3   c2' 
                           / \   |     |
                          .   .  .     .
                          .   .  .     .
@endverbatim
 *
 * -  Arrays/sets of types (QNames, ...) contain pointers to their elements.
 *    Inserting, removing an element does not alter the element itself.
 *
 * -  Chains of applications of binary type constructors ((|), (&),
 *    (,)) are expected to be left-deep (to facilitate the 
 *    analysis/simplifcation of such chains).  
 */


/**
 * Encoding of ``trivial'' <:, ||, `can be promoted to' relationships (see
 * include/types.h):
 *
 * - to decide <:, @a trivial encodes the subtyping @a hierarchy
 * - to decide ||, @a trivial tells if two types trivially @a intersect
 * - to decide `can be promoted to', 
 *             @a trivial tells if two types are promotable
 */
static char (*trivial)[ty_types][ty_types];

/** 
 * Sets of types.  
 * @note These sets store pointers to their element types.
 */
typedef PFarray_t *ty_set_t;
#define ty_set()          ((ty_set_t) PFarray (sizeof (PFty_t *)))
#define ty_set_card(s)    (PFarray_last (s))
#define ty_set_elem(s, n) (*(PFty_t **) PFarray_at ((s), (n)))
#define ty_set_ins(s, t)  ((*(PFty_t **) PFarray_add (s)) = (t))

/**
 * A pair of (pointers to) types, used to represent 
 * - monomials (see #lf ())
 * - partial derivatives of a type (see #pd_ty ()), and
 * - regular inequalities.
 */
struct ty_pair_t {
    PFty_t fst;
    PFty_t snd;
};

typedef struct ty_pair_t ty_pair_t;

/**
 * Sets of pairs of types.
 * @note These sets store pointers to their element pairs.
 */
typedef PFarray_t *ty_pair_set_t;
#define ty_pair_set()          ((ty_pair_set_t )PFarray (sizeof (ty_pair_t *)))
#define ty_pair_set_card(s)    (PFarray_last (s))
#define ty_pair_set_elem(s, n) (*(ty_pair_t **) PFarray_at ((s), (n)))
#define ty_pair_set_ins(s, p)  ((*(ty_pair_t **) PFarray_add (s)) = (p))

/**
 * A partial derivative of an regular inequality, used in
 * - #pd_ineq ()
 * (see Antimirov, Definition 6).
 */
struct pd_ineq_t {
    ty_pair_t    *p;
    ty_pair_set_t pds;
};

typedef struct pd_ineq_t pd_ineq_t;

/**
 * Sets of partial derivatives of regular inequalities.
 * @note These sets store pointers to their partial derivatives.
 */
typedef PFarray_t *pd_ineq_set_t;
#define pd_ineq_set()          ((pd_ineq_set_t) PFarray (sizeof (pd_ineq_t *)))
#define pd_ineq_set_card(s)    (PFarray_last (s))
#define pd_ineq_set_elem(s, n) (*(pd_ineq_t **) PFarray_at ((s), (n)))
#define pd_ineq_set_ins(s, p)  ((*(pd_ineq_t **) PFarray_add (s)) = (p))


/*
 * Utility routines working on types, arrays.
 *
 * - perms:    enumerate all permutations of a set of types
 * - foldr1:   fold a binary type constructor over a non-empty set of types
 * - nub:      generic duplicate elimnation
 */

static void perms (ty_set_t, void (*) (ty_set_t));
static PFty_t foldr1 (ty_set_t, PFty_t (*) (PFty_t, PFty_t));
static PFarray_t *nub (PFarray_t *, bool (*) (void *, void *));

/**
 * Bit-wise encoding of sets (used in #set_theory ())
 */
typedef unsigned long long bitset_t;

/** 
 * For non-empty ts = { t1, t2, t3, ..., tn } and binary type constructor #,
 * compute the right-deep type t1 # (t2 # (t3 # (... # tn)...))
 *
 * @param ts non-empty set of types
 * @param op binary type constructor
 * @return type obtained by folding op over ts
 */
static PFty_t
foldr1 (ty_set_t ts, PFty_t (op) (PFty_t, PFty_t))
{
    PFty_t t;
    int n;

    /* ts is a non-empty set */
    assert (ty_set_card (ts) > 0);

    /* t = tn */
    t = *(ty_set_elem (ts, ty_set_card (ts) - 1));

    for (n = ty_set_card (ts) - 2; n >= 0; n--)
        t = op (*(ty_set_elem (ts, n)), t);

    return t;
}

/**
 * Generic duplicate elimination.  Constructs a new array with all
 * duplicate array entries removed.  
 *
 * NB. array entries are assumed to be pointers to the actual array
 *     elements
 *
 * @param a     array, potentially with duplicates
 * @param equal predicate to decide equality of two array entries
 * @return new array of unique entries
 */
static PFarray_t *
nub (PFarray_t *a, bool (equal) (void *, void *))
{
    PFarray_t *s;
    unsigned n, e;

    assert (a);

    s = PFarray (sizeof (void *));

    if (PFarray_last (a)) 
        for (n = 0; n < PFarray_last (a); n++) {
            for (e = n + 1; e < PFarray_last (a); e++)
                if (equal (*(void **) PFarray_at (a, n), 
                           *(void **) PFarray_at (a, e)))
                    goto dupe;
      
            /* insert unique entry into result ``set'' s */
            (*(void **) PFarray_add (s)) = (*(void **) PFarray_at (a, n)); 
        dupe:
            /* duplicate found */
            ;
        }

    return s;
}

/**
 * Worker for perms ().
 */
static unsigned int
_perm (ty_set_t s, ty_set_t p, unsigned int card, 
       unsigned int k, unsigned int lv,
       void (*f) (ty_set_t))
{
    unsigned int n;

    if (lv > 0)
        ty_set_elem (p, k) = ty_set_elem (s, lv - 1);

    if (lv == card)
        f (p);
    else
        for (n = 0; n < card; n++)
            if (! ty_set_elem (p, n))
                lv = _perm (s, p, card, n, lv + 1, f);

    ty_set_elem (p, k) = 0;

    return lv - 1;
}

/**
 * Compute all 2^n permutations of the set of types s (cardinality n).
 * For each permutation, invoke callback f.
 *
 * @param s set of types 
 * @param f callback invoked for each permutation generated
 */
static void
perms (ty_set_t s, void (*f) (ty_set_t))
{
    ty_set_t p;
    unsigned int card;
    unsigned int n;

    p = ty_set ();
    card = ty_set_card (s);

    for (n = 0; n < card; n++)
        ty_set_elem (p, n) = 0;

    ty_set_card (p) = card;

    (void) _perm (s, p, card, 0, 0, f);
}



/*
 * Auxiliary routines to implement Antimirov's algorithm.
 *
 * - nullable:     epsilon inclusion test
 * - sigma:        { t1, t2, ..., tn }  -->  (...(t1 | t2) | ... | tn)
 * - leadingnames: collect leading type/element/attribute names 
 * - lf:           linear form of a type
 * - pd_ty:        partial derivatives of a type
 * - pd_ineq:      partial derivatives of a regular inequality
 * - set_theory:   set-theoretic observation (subset test for cartesian prod)
 */

static bool nullable (PFty_t);
static PFty_t sigma (ty_set_t);
static ty_set_t leadingnames (PFty_t);
static ty_pair_set_t lf (PFty_t);
static ty_pair_set_t pd_ty (PFty_t *, PFty_t);
static pd_ineq_set_t pd_ineq (PFty_t *, PFty_t, PFty_t);
static ty_pair_set_t set_theory (pd_ineq_t *);


/**
 * Worker for #PFty_regularity ().  Test if recursive occurences of 
 * named types are preceded by a non-nullable type.
 *
 * - non_null_head (b, named (n)) = b
 * - non_null_head (b, s?)        = non_null_head (b, s)
 * - non_null_head (b, s*)        = non_null_head (b, s)
 * - non_null_head (b, s+)        = non_null_head (b, s)
 * - non_null_head (b, (s1,s2))   = non_null_head (b, s1) /\
 *                                  non_null_head (b \/ not(nullable (s1)), s2)
 * - non_null_head (b, (s1|s2))   = non_null_head (b, s1) /\
 *                                  non_null_head (b, s2)
 * - non_null_head (b, s)         = true
 *
 * @param b true, if recursive occurrence of type is OK in type @a t
 * @param t (pointer to) type to test
 * @return true, if all recursive type occurrences in @a t are preceded
 *   by a non-nullable type
 */
static bool
non_null_head (bool b, PFty_t *t)
{
    assert (t);

    switch (t->type) {
    case ty_named:
        /* since we have unfolded the type before, this name must
         * indicate a recursive type
         */
        return b;

    case ty_opt:
    case ty_star:
    case ty_plus:
        return non_null_head (b, t->child[0]);
    
    case ty_seq:
        return non_null_head (b, t->child[0]) &&
            non_null_head (b || (! nullable (*(t->child[0]))), t->child[1]);

    case ty_choice:
        return non_null_head (b, t->child[0]) && non_null_head (b, t->child[1]);

    default:
        return true;
    }
}

/**
 * Worker for #PFty_regularity ().  Test if recursive occurences of 
 * named type @a n occurs in @a t in tail positions only.
 *
 * - rec_tail (t, b, named (n)) = if (t = n) then b else true
 * - rec_tail (t, b, s?)        = rec_tail (t, b, s)
 * - rec_tail (t, b, s*)        = rec_tail (t, false, s)
 * - rec_tail (t, b, s+)        = rec_tail (t, false, s)
 * - rec_tail (t, b, (s1,s2))   = rec_tail (t, false, s1) /\ 
 *                                rec_tail (t, b,     s2)
 * - rec_tail (t, b, (s1|s2))   = rec_tail (t, b, s1) /\ 
 *                                rec_tail (t, b, s2)
 * - rec_tail (t, b, s)         = true
 *
 * @param n named type to test
 * @param b true, if a recursive occurrence would be ok in @a t, 
 *   false otherwise
 * @param t type to test
 * @return true, if @a t has only tail occurrences of @a n, false otherwise
 */
static bool
rec_tail (PFty_t n, bool b, PFty_t *t)
{
    assert (t);

    switch (t->type) {
    case ty_named:
        /* since we have unfolded the type before, this name must
         * indicate a recursive type
         */
        if (PFty_eq (n, *t))
            return b;
    
        return true;
    case ty_opt:
        return rec_tail (n, b, t->child[0]);

    case ty_star:
    case ty_plus:
        return rec_tail (n, false, t->child[0]);

    case ty_seq:
        return rec_tail (n, false, t->child[0]) && rec_tail (n, b, t->child[1]);

    case ty_choice:
        return rec_tail (n, b, t->child[0]) && rec_tail (n, b, t->child[1]);
    
    default:
        return true;
    }
}

/**
 * Test for the well-formedness (regularity) of recursive types
 */
bool
PFty_regularity (PFty_t t)
{
    PFty_t *s;

    /* an unnamed type is well-formed: only naming may lead to recursion */
    if (t.type != ty_named)
        return true;

    s = PFty_simplify (PFty_defn (t));
    assert (s);

    /* enforce both well-formedness constraints to ensure regularity:
     * (1) rec_tail:      recursive occurrences of types in 
     *                    tail positions, only
     * (2) non_null_head: recursive occurrences of types preceded by
     *                    a non-nullable head type
     */
    return rec_tail (t, true, s) && non_null_head (false, s);
}




/**
 * Worker for #nullable ().
 */
static bool
_nullable (PFty_t t)
{
    switch (t.type) {

    case ty_plus:
        return _nullable (*(t.child[0]));

    case ty_seq:
        return _nullable (*(t.child[0])) && _nullable (*(t.child[1]));

    case ty_choice:
        return _nullable (*(t.child[0])) || _nullable (*(t.child[1]));

    case ty_all:
        return _nullable (*(PFty_simplify (t)));

    case ty_empty:
    case ty_opt:
    case ty_star:
        return true;

    default:
        return false;
    }
}

/**
 * For (the regular expression) type t, test if () is in L(t).
 * 
 * @param t type t
 * @return true, if () is in L(t), false otherwise
 */
static bool
nullable (PFty_t t)
{
    return _nullable (PFty_defn (t));
}

/**
 * For non-empty ts = { t1, t2, t3, ..., tn }, compute the right-deep type
 * t1 | (t2 | (t3 | (... | tn)...))
 *
 * @param ts non-empty set of types
 * @return choice of all types in ts
 */
static PFty_t
sigma (ty_set_t ts)
{
    return foldr1 (ts, PFty_choice);
}

/**
 * Shallow (and name-based) equality for types.  Usage:
 *
 * - (1) detect duplicate leading names in #leadingnames ()
 * - (2) deriving @a t2 by @a t1 in #pd_ty ()
 *
 * @param t1 (pointer to) first type
 * @param t2 (pointer to) second type
 * @return true, if both types are shallow equal, false otherwise
 */
static bool
name_eq (PFty_t *t1, PFty_t *t2)
{
    assert (t1 && t2);

    assert (trivial);
    if ((*trivial)[t1->type][t2->type] == 1)
        return true;

    if (t1->type != t2->type)
        return false;

    switch (t1->type) {
    case ty_elem:
    case ty_attr:
    case ty_pi:
        /* a match against a wildcards always succeeds */
        if (PFty_wildcard (*t2))
            return true;

        /* otherwise compare the element/attribute names */
        return (PFqname_eq (t1->name, t2->name) == 0);

    case ty_named:
        /* compare symbol spaces and type names */
        return PFty_eq (*t1, *t2);
    
    default:
        return true;
    }
}

/**
 * Worker for leadingnames ().
 */
static ty_set_t
_ln (PFty_t *t, PFarray_t *lns)
{
    assert (t);

    switch (t->type) {
    case ty_empty:
    case ty_none:
        return lns;

    case ty_named:
        /* this must be the recursive occurrence of a named type
         * reference, no new leading names can be found by looking
         * at the referenced type
         */
        return lns;

    case ty_opt:
    case ty_star:
    case ty_plus:
        return _ln (t->child[0], lns);

    case ty_seq:
        lns = _ln (t->child[0], lns);
        if (nullable (*(t->child[0])))
            lns = _ln (t->child[1], lns);
        return lns;

    case ty_choice:
        return _ln (t->child[1], _ln (t->child[0], lns));

    case ty_all:
        PFoops (OOPS_FATAL, 
                "encountered a `&' constructor in a simplified type");

    default:
        ty_set_ins (lns, t);
        return lns;
    }
}

/**
 * The set of leading type and element (names) of a regular expression type.
 *
 * @attention N.B. Make sure apply #PFty_defn () to @a t first, otherwise
 *                 the result may not be correct for recursive types.
 *
 * - leadingnames (())      = {}
 * - leadingnames (empty)   = {}
 * - leadingnames (named n) = {}
 * - leadingnames (t?)      = leadingnames (t)
 * - leadingnames (t*)      = leadingnames (t)
 * - leadingnames (t+)      = leadingnames (t)
 * - leadingnames (t1, t2)  
 *      = / leadingnames (t1) U leadingnames (t2) , if nullable (t1)
 *        \ leadingnames (t1)                     , otherwise
 * - leadingnames (t1 | t2) = leadingnames (t1) U leadingnames (t2)
 * - leadingnames (t)       = { t }
 *
 * @param t type 
 * @return the set of leading types in type @a t
 */
static ty_set_t
leadingnames (PFty_t t)
{
    return nub (_ln (PFty_simplify (t), ty_set ()),
                (bool (*) (void *, void *)) name_eq);
}


/**
 * Compare two types t1, t2 for structural equality.
 *
 * @param t1 (pointer to) type t1
 * @param t2 (pointer to) type t2
 * @return true if t1, t2 are structurally equal, false otherwise
 *
 * FIXME: optimization for speed:
 *        may use pointer equality at some places to prevent deep recursion
 */
static bool
ty_eq (PFty_t *t1, PFty_t *t2)
{
    assert (t1 && t2);

    if (t1->type != t2->type)
	return false;

    switch (t1->type) {
    case ty_named:
        /* both types need to be in the same symbol space and
         * have identical names
         */
	return (t1->sym_space == t2->sym_space) &&
               (PFqname_eq (t1->name, t2->name) == 0);

    case ty_opt:
    case ty_plus:
    case ty_star:
    case ty_doc:
	return ty_eq (t1->child[0], t2->child[0]);

    case ty_seq:
    case ty_choice:
    case ty_all:
	return ty_eq (t1->child[0], t2->child[0])
               && ty_eq (t1->child[1], t2->child[1]);

    case ty_elem:
    case ty_attr:
	if (PFty_wildcard (*t1) && PFty_wildcard (*t2))
	    return ty_eq (t1->child[0], t2->child[0]);
	else
	    return ty_eq (t1->child[0], t2->child[0]) &&
		(PFqname_eq (t1->name, t2->name) == 0);

    case ty_pi:
        if (PFty_wildcard (*t1) && PFty_wildcard (*t2))
            return true;
        else
            return (PFqname_eq (t1->name, t2->name) == 0);

    default:
	return true;
    }
}

/**
 * Component-wise equality of two pairs of types.
 *
 * @param p1 (pointer to) first pair
 * @param p2 (pointer to) second pair
 * @return true if @a p1 and @a p2 are equal, false otherwise
 */
static bool
ty_pair_eq (ty_pair_t *p1, ty_pair_t *p2)
{
    return ty_eq (&(p1->fst), &(p2->fst)) && 
           ty_eq (&(p1->snd), &(p2->snd));
}

/**
 * The (.) extended concatenation of linear forms 
 * (see Antimirov, Definition 1).
 *
 * - l               (.) none  = {}
 * - {}              (.) t     = {}
 * - l               (.) empty = l
 * - { <x, none> }   (.) t     = { <x, none> }
 * - { <x, empty> }  (.) t     = { <x, t> }
 * - { <x, p> }      (.) t     = { <x, (p,t)> }
 * - { t1, ..., tn } (.) t     = { t1 } (.) t U ... U { tn } (.) t
 *
 * @param l array (set) to which type @a t is concatenated
 * @param t type to concatenate
 * @return extended concatenation of @a l and @a t
 */
static ty_pair_set_t
odot (ty_pair_set_t l, PFty_t t)
{
    ty_pair_set_t monos;

    monos = ty_pair_set ();
    assert (monos);

    /* {} (.) t = {} */
    if (PFarray_empty (l))
        return monos;

    switch (t.type) {
    case ty_empty:
        /* l (.) empty = l (monos is still empty here) */
        return PFarray_concat (monos, l);

    case ty_none:
        /* l (.) none = {} */
        return monos;

    default: {
        /* iterate over all monomials found so far (this iteration
         * replaces the original recursive formulation of (.)
         * found in Antimirov's paper)
         */
        unsigned int n; 
        PFty_t x, p;
        ty_pair_t *mono;

        for (n = 0; n < PFarray_last (l); n++) {
            /* access the current monomial <x, p> */
            x = (ty_pair_set_elem (l, n))->fst;
            p = (ty_pair_set_elem (l, n))->snd;

            mono = (ty_pair_t *) PFmalloc (sizeof (ty_pair_t));

            switch (p.type) {
            case ty_none:
                /* { <x, none> } (.) t = { <x, none> } */
                mono->fst = x;
                mono->snd = PFty_none ();
                break;

            case ty_empty:
                /* { <x, empty> (.) t = { <x, t> } */
                mono->fst = x;
                mono->snd = t;
                break;

            default:
                /* { <x, p> (.) t = { <x, (p,t)> } */
                mono->fst = x;
                mono->snd = PFty_seq (p, t);
            }

            /* add the monomial to the result */
            ty_pair_set_ins (monos, mono);
        }

        return monos;
    }
    }
}

/**
 * Worker for #lf ().
 */
static ty_pair_set_t
_lf (PFty_t t)
{
    ty_pair_set_t monos;
  
    monos = ty_pair_set ();
    assert (monos);
  
    switch (t.type) {
    case ty_empty:
    case ty_none:
        return monos;

    case ty_named: {
        /* unfold the definition of the named type; the non-nullable head
         * well-formedness constraints ensures that we do not run into
         * an infinite recursion 
         */
        PFty_t *s;

        if (! (s = PFty_schema (t)))
            PFoops (OOPS_TYPENOTDEF, "unknown schema type `%s'",
                    PFqname_str (t.name));

        return _lf (*s);
    }

    case ty_opt: 
        /* _lf (t?) = _lf (t | empty) */
        return _lf (PFty_choice (*(t.child[0]), PFty_empty ()));
    
    case ty_star:
        return odot (_lf (*(t.child[0])), t);
    
    case ty_plus:
        /* _lf (t+) = _lf (t, t*) */
        return _lf (PFty_seq (*(t.child[0]), PFty_star (*(t.child[0]))));
  
    case ty_seq:
        if (nullable (*(t.child[0])))
            return PFarray_concat (odot (_lf (*(t.child[0])), *(t.child[1])),
                                   _lf (*(t.child[1])));
        else
            return odot (_lf (*(t.child[0])), *(t.child[1])); 

    case ty_choice:
        return PFarray_concat (_lf (*(t.child[0])), _lf (*(t.child[1])));

    case ty_all:
        PFoops (OOPS_FATAL, 
                "encountered a `&' constructor in a simplified type");

    default: {
        ty_pair_t *mono;
    
        mono = (ty_pair_t *) PFmalloc (sizeof (ty_pair_t));
    
        /* build the monomial <t, empty> */
        mono->fst = t;
        mono->snd = PFty_empty ();
    
        ty_pair_set_ins (monos, mono);
    
        return monos;
    }
    }
}

/**
 * Compute the linear form of a given type @a t
 * (see Antimirov, Definition 1).
 *
 * - lf (none)    = {}
 * - lf (empty)   = {}
 * - lf (t?)      = lf (t | empty)
 * - lf (t*)      = lf (t) (.) t*
 * - lf (t+)      = lf (t, t*)
 * - lf (t1, t2)  =
 *      = / lf (t1) (.) U lf (t2)  , if nullable (t1)
 *        \ lf (t1) (.) t2         , otherwise
 * - lf (t1 | t2) = lf (t1) U lf (t2)
 * - lf (t)       - { <t, empty> }
 *
 * @param t type to derive linear form for
 * @return array (set) of monomials for type @a t (a monomial is a pair of
 *   types <x, p>)
 */
static ty_pair_set_t
lf (PFty_t t)
{
    return nub (_lf (*PFty_simplify (t)),
                (bool (*) (void *, void *)) ty_pair_eq);
}

/**
 * The content (type) of a given type @a t.  This adds the
 * child-dimension to the sibling-dimension explored by #lf ().
 *
 * content (elem qn { t }) = t
 * content (attr qn { t }) = t
 * content (doc { t })     = t
 * content (item)          = item*
 * content (node)          = item*
 * content (t)             = empty
 * 
 * @param t type whose contents is needed
 * @return content type of @a t or @a empty if @a t has no content type
 */
static PFty_t
content (PFty_t t)
{
    switch (t.type) {
    case ty_elem:
    case ty_attr:
    case ty_doc:
        assert (t.child[0]);
        return *(t.child[0]);

    case ty_item:
    case ty_node:
        return PFty_star (PFty_item ());

    default: 
        return PFty_empty ();
    }
}

/**
 * Compute the partial derivatives of a given type @a t with respect
 * to type @a x (see Antimirov, Definition 2 [ð_x (t)] and the XOBE work).
 *
 * @param x (pointer to) type to derive by
 * @param t type to compute the partial derivatives for
 * @return array of pairs @a pd = <@a d, @a c>: 
 *
 * @a d is a partial derivative of @a t, @a c is the content of @a d
 * (@a c = empty if @a d has no content).
 */
static ty_pair_set_t
pd_ty (PFty_t *x, PFty_t t)
{
    ty_pair_set_t pds;
    ty_pair_t    *pd;
    PFty_t        p, t_;

    ty_pair_set_t lin_form;

    unsigned int n;

#ifdef DEBUG_TYPES
    PFlog ("subtyping: pd_ty (%s; %s)",
           PFty_str (*x), PFty_str (t));
#endif /* DEBUG_TYPES */

    pds = ty_pair_set ();
    assert (pds);

    lin_form = lf (t);

    /* pds = { <content (t_), p> | <t_, p> <- lin_form /\ 
     *                             name_eq (x, t_,) /\ p != none }
     */
    for (n = 0; n < PFarray_last (lin_form); n++) {
        t_ = (ty_pair_set_elem (lin_form, n))->fst;
        p  = (ty_pair_set_elem (lin_form, n))->snd;

        if ((p.type != ty_none) && name_eq (x, &t_)) {
            pd = (ty_pair_t *) PFmalloc (sizeof (ty_pair_t));

            pd->fst = content (t_);
            pd->snd = p;

            ty_pair_set_ins (pds, pd);

#ifdef DEBUG_TYPES
            PFlog ("subtyping:  (%s; %s)", PFty_str (pd->fst), PFty_str (pd->snd));
#endif /* DEBUG_TYPES */
        }
    }

    return pds;
}

/**
 * Partial derivatives of the regular inequality @a a <: @a b with
 * respect to type @a w (see Antimirov, Definition 6)
 *
 * @param w (pointer to) type to derive by
 * @param a type on lhs of regular inequality
 * @param b type on rhs of regular inequality
 * @return array of @a pd_ineq_t:
 *
 * @note: This returns the set of partial derivatives for @a a <:b @a b,
 * in the form (the p are types of pairs (ty_pair_t)):
 *
 *                       { (p, { p, p, ... }),
 *                         (p, { p, p, ... }),
 *                         ...
 *                       }
 */
static pd_ineq_set_t
pd_ineq (PFty_t *w, PFty_t a, PFty_t b)
{
    pd_ineq_set_t pds;
    pd_ineq_t    *pd;

    ty_pair_set_t pds_a; 
    ty_pair_t    *p;

    unsigned int n;

    pds = pd_ineq_set ();
    assert (pds);

    /* compute ð_w (a) */
    pds_a = pd_ty (w, a);

    /* pds = { (p, ð_w (b)) | p <- ð_w (a) }
     */
    for (n = 0; n < PFarray_last (pds_a); n++) {
        p = ty_pair_set_elem (pds_a, n);
        assert (p);

        pd = (pd_ineq_t *) PFmalloc (sizeof (pd_ineq_t));

        /* build partial derivative (p, ð_w (b)) */
        pd->p   = p;
        pd->pds = pd_ty (w, b);
        assert (pd->pds);

        /* insert (p, ð_w (b)) into result */
        pd_ineq_set_ins (pds, pd);
    }

    return pds;
}


/** 
 * Worker for #set_theory ().
 *
 * Build the inequality
 *
 * @verbatim
                     @a t <: @a t0 | @a t1 |... | @a tn
@endverbatim
 *
 * where the ti are taken from @a pds (@a subset and @a left indicate
 * which ti to take).
 *
 * @param t left-hand side type t of inequality
 * @param pds set of pairs of types to pick for right-hand side
 * @param left pick left-hand or right-hand side of type pair?
 * @param s number of elements encoded in @a subset
 * @param subset bit-encoding of subset of types to pick from @a pds
 */
static ty_pair_t *
subset_ineq (PFty_t t, ty_pair_set_t pds, bool left, 
             unsigned int s, bitset_t subset)
{
    ty_pair_t *ineq;
    unsigned int i;

    ineq = (ty_pair_t *) PFmalloc (sizeof (ty_pair_t));

    ineq->fst = t;
    ineq->snd = PFty_none ();

    for (i = 0; i < s; i++) {
        if (subset & 1)
            ineq->snd = PFty_choice (ineq->snd,
                                     left ? (ty_pair_set_elem (pds, i))->fst
                                     : (ty_pair_set_elem (pds, i))->snd);

        subset >>= 1;
    }

    ineq->snd = *PFty_simplify (ineq->snd);

#ifdef DEBUG_TYPES
    PFlog ("subtyping: set_theory() generated subgoal %s <: %s",
           PFty_str (ineq->fst), PFty_str (ineq->snd));
#endif /* DEBUG_TYPES */

    return ineq;
}

/**
 * Implementation of the set-theoretic observation of
 * Hosoya, Vouillon, Pierce [3].
 *
 * Turn the partial derivative of an inequality 
 *
 *                       ( (t,t'), { (a0,b0), (a1,b1) })
 *
 * into the flat set of type pairs
 *
 *                 { (t, none) , (t', b0|b1),
 *                   (t, a0)   , (t', b1)   ,
 *                   (t, a1)   , (t', b0)   ,
 *                   (t, a0|a1), (t', none)
 *                 }
 * 
 * whose interpretation by the subtype checker then is
 *
 *                   (t <: none  \/ t' <: b0|b1) /\
 *                   (t <: a0    \/ t' <: b1)    /\
 *                   (t <: a1    \/ t' <: b0)    /\
 *                   (t <: a0|a1 \/ t' <: none) 
 *
 * @param pd partial derivative of an inequality
 * @return a set of type pairs or 0 if the set of partial derivatives of
 *   the rhs is empty
 */
static ty_pair_set_t
set_theory (pd_ineq_t *pd)
{
    /* bit encoding of subsets of pd->pds (1 = element present in subset) */
    bitset_t subset;
    ty_pair_set_t ineqs;

    unsigned int s;
  
    assert (pd && pd->pds);
  
    s = ty_pair_set_card (pd->pds);
    if (s == 0)
        return 0;

    /* make sure we can enumerate the complete powerset of pd->pds */
    assert (s <= sizeof (bitset_t) * CHAR_BIT);

    ineqs = ty_pair_set ();
    assert (ineqs);
  
    /* enumerate the powerset of pd->pds */
    for (subset = 0ULL; subset < (1ULL << s); subset++) {
        ty_pair_set_ins (ineqs, subset_ineq (pd->p->fst, pd->pds, true,  
                                             s, subset));
        ty_pair_set_ins (ineqs, subset_ineq (pd->p->snd, pd->pds, false, 
                                             s, ~subset));
    }
  
    return ineqs;
}

/**
 * Encoding of the XQuery type hierarchy and ``trivial'' <: relationships
 * (see include/types.h).
 *
 * An entry for @a hierarchy[t1][t2] of
 * - 0 indicates not (t1 <: t2)
 * - 1 indicates t1 <: t2
 * - _ indicates <: must be decided using Antimirov's algorithm.
 */
#define _ -1

static
char hierarchy[ty_types][ty_types] = { 
    /*  
                                                  u
                                                  n
                                                  t
                                              u   y
                                              n   p
                                              t   e
                                              y   d n i d     b 
                                              p a A u n e d s o 
                            e n               e t t m t c o t o 
                          n m a             i d o o e e i u r l n e a   t   c
                          o p m             t A m m r g m b i e o l t d x   o
                          n t e             e n i i i e a l n a d e t o e p m
                          e y d ? + * , | & m y c c c r l e g n e m r c t i m*/
     [ty_none   ]      ={ 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1 }
    ,[ty_empty  ]      ={ 0,1,_,_,_,_,_,_,_,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_named  ]      ={ _,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_opt    ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_plus   ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_star   ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_seq    ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_choice ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_all    ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_item   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_untypedAny]   ={ 0,0,_,_,_,_,_,_,_,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_atomic ]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_untypedAtomic]={ 0,0,_,_,_,_,_,_,_,1,0,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_numeric]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,1,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_integer]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,1,1,1,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_decimal]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,1,0,1,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_double ]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,1,0,0,1,0,0,0,0,0,0,0,0,0 }
    ,[ty_string ]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,0,0,0,0,1,0,0,0,0,0,0,0,0 }
    ,[ty_boolean]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,0,0,0,0,0,1,0,0,0,0,0,0,0 }
    ,[ty_node   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0 }
    ,[ty_elem   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,_,0,0,0,0,0 }
    ,[ty_attr   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,0,_,0,0,0,0 }
    ,[ty_doc    ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,0,0,_,0,0,0 }
    ,[ty_text   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,0,0,0,1,0,0 }
    ,[ty_pi     ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,_,0 }
    ,[ty_comm   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,1 }
};

/**
 * Encoding of the XQuery type equality relationships
 *
 * An entry for @a equality[t1][t2] of
 * - 0 indicates not (t1 <: t2)
 * - 1 indicates t1 <: t2
 * - _ indicates <: must be decided using Antimirov's algorithm.
 */
#define _ -1

static
char equality[ty_types][ty_types] = { 
    /*  
                                                  u
                                                  n
                                                  t
                                              u   y
                                              n   p
                                              t   e
                                              y   d n i d     b 
                                              p a A u n e d s o 
                            e n               e t t m t c o t o 
                          n m a             i d o o e e i u r l n e a   t   c
                          o p m             t A m m r g m b i e o l t d x   o
                          n t e             e n i i i e a l n a d e t o e p m
                          e y d ? + * , | & m y c c c r l e g n e m r c t i m*/
     [ty_none   ]      ={ 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1 }
    ,[ty_empty  ]      ={ 0,1,_,_,_,_,_,_,_,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_named  ]      ={ _,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_opt    ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_plus   ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_star   ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_seq    ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_choice ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_all    ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_item   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_untypedAny]   ={ 0,0,_,_,_,_,_,_,_,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_atomic ]      ={ 0,0,_,_,_,_,_,_,_,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_untypedAtomic]={ 0,0,_,_,_,_,_,_,_,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_numeric]      ={ 0,0,_,_,_,_,_,_,_,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_integer]      ={ 0,0,_,_,_,_,_,_,_,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_decimal]      ={ 0,0,_,_,_,_,_,_,_,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_double ]      ={ 0,0,_,_,_,_,_,_,_,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0 }
    ,[ty_string ]      ={ 0,0,_,_,_,_,_,_,_,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0 }
    ,[ty_boolean]      ={ 0,0,_,_,_,_,_,_,_,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0 }
    ,[ty_node   ]      ={ 0,0,_,_,_,_,_,_,_,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0 }
    ,[ty_elem   ]      ={ 0,0,_,_,_,_,_,_,_,0,0,0,0,0,0,0,0,0,0,0,_,0,0,0,0,0 }
    ,[ty_attr   ]      ={ 0,0,_,_,_,_,_,_,_,0,0,0,0,0,0,0,0,0,0,0,0,_,0,0,0,0 }
    ,[ty_doc    ]      ={ 0,0,_,_,_,_,_,_,_,0,0,0,0,0,0,0,0,0,0,0,0,0,_,0,0,0 }
    ,[ty_text   ]      ={ 0,0,_,_,_,_,_,_,_,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0 }
    ,[ty_pi     ]      ={ 0,0,_,_,_,_,_,_,_,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,_,0 }
    ,[ty_comm   ]      ={ 0,0,_,_,_,_,_,_,_,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1 }
};

/**
 * Encoding of the XQuery `can be promoted to' relationship.
 * (see include/types.h).
 *
 * An entry for @a promotable[t1][t2] of
 * - 0 indicates t1 cannot be promoted to t2
 * - 1 indicates t1 can be promoted to t2
 * - _ indicates promotoion must be decided using Antimirov's algorithm.
 */
static
char promotable[ty_types][ty_types] = { 
    /*  
                                                  u
                                                  n
                                                  t
                                              u   y
                                              n   p
                                              t   e
                                              y   d n i d     b 
                                              p a A u n e d s o 
                          e n                 e t t m t c o t o 
                          n m a             i d o o e e i u r l n e a   t   c
                          o p m             t A m m r g m b i e o l t d x   o
                          n t e             e n i i i e a l n a d e t o e p m
                          e y d ? + * , | & m y c c c r l e g n e m r c t i m*/
     [ty_none   ]      ={ 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1 }
    ,[ty_empty  ]      ={ 0,1,_,_,_,_,_,_,_,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_named  ]      ={ _,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_opt    ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_plus   ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_star   ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_seq    ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_choice ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_all    ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_item   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_untypedAny]   ={ 0,0,_,_,_,_,_,_,_,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_atomic ]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_untypedAtomic]={ 0,0,_,_,_,_,_,_,_,1,0,1,0,0,0,0,1,0,0,0,0,0,0,0,0,0 }
    ,[ty_numeric]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,1,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_integer]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,1,1,1,1,0,0,0,0,0,0,0,0,0 }
    ,[ty_decimal]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,1,0,1,1,0,0,0,0,0,0,0,0,0 }
    ,[ty_double ]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,1,0,0,1,0,0,0,0,0,0,0,0,0 }
    ,[ty_string ]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,0,0,0,0,1,0,0,0,0,0,0,0,0 }
    ,[ty_boolean]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,0,0,0,0,0,1,0,0,0,0,0,0,0 }
    ,[ty_node   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0 }
    ,[ty_elem   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,_,0,0,0,0,0 }
    ,[ty_attr   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,0,_,0,0,0,0 }
    ,[ty_doc    ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,0,0,_,0,0,0 }
    ,[ty_text   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,0,0,0,1,0,0 }
    ,[ty_pi     ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,_,0 }
    ,[ty_comm   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,1 }
};

/**
 * Encoding of trivial XQuery type intersections
 * (see include/types.h).
 *
 * An entry for @a intersect[t1][t2] of
 * - 1 indicates not (t1 || t2)
 * - 0 indicates t1 || t2
 * - _ indicates || must be decided using Antimirov's algorithm.
 */
static
char intersect[ty_types][ty_types] = { 
    /*
                                                  u
                                                  n
                                                  t
                                              u   y
                                              n   p
                                              t   e
                                              y   d n i d     b 
                                              p a A u n e d s o 
                            e n               e t t m t c o t o 
                          n m a             i d o o e e i u r l n e a   t   c
                          o p m             t A m m r g m b i e o l t d x   o
                          n t e             e n i i i e a l n a d e t o e p m
                          e y d ? + * , | & m y c c c r l e g n e m r c t i m*/
     [ty_none   ]      ={ 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_empty  ]      ={ 0,1,_,1,_,1,_,_,_,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_named  ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_opt    ]      ={ 0,1,_,_,_,1,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_plus   ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_star   ]      ={ 0,1,_,1,_,1,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_seq    ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_choice ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_all    ]      ={ 0,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_ }
    ,[ty_item   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1 }
    ,[ty_untypedAny]   ={ 0,0,_,_,_,_,_,_,_,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_atomic ]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,1,1,1,1,1,1,1,0,0,0,0,0,0,0 }
    ,[ty_untypedAtomic]={ 0,0,_,_,_,_,_,_,_,1,0,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_numeric]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,1,1,1,1,0,0,0,0,0,0,0,0,0 }
    ,[ty_integer]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,1,1,0,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_decimal]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,1,0,1,0,0,0,0,0,0,0,0,0,0 }
    ,[ty_double ]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,1,0,0,1,0,0,0,0,0,0,0,0,0 }
    ,[ty_string ]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,0,0,0,0,1,0,0,0,0,0,0,0,0 }
    ,[ty_boolean]      ={ 0,0,_,_,_,_,_,_,_,1,0,1,0,0,0,0,0,0,1,0,0,0,0,0,0,0 }
    ,[ty_node   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,1,1,1,1,1,1 }
    ,[ty_elem   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,_,0,0,0,0,0 }
    ,[ty_attr   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,0,_,1,1,1,1 }
    ,[ty_doc    ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,0,0,_,0,0,0 }
    ,[ty_text   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,0,0,0,1,0,0 }
    ,[ty_pi     ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,_,0 }
    ,[ty_comm   ]      ={ 0,0,_,_,_,_,_,_,_,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,1 }
};


/**
 * Is the inequality @a t1 <: @a t2 in the set of assumptions @a a?
 *
 * @param t1 first type
 * @param t2 second type
 * @param a assumption set
 * @return true, if @a t1 <: @a t2 is element of @a, false otherwise
 */
static bool
assumption (PFty_t t1, PFty_t t2, ty_pair_set_t a)
{
    unsigned int n;

    for (n = 0; n < ty_pair_set_card (a); n++)
        if (ty_eq (&t1, &(ty_pair_set_elem (a, n)->fst)) &&
            ty_eq (&t2, &(ty_pair_set_elem (a, n)->snd)))
            return true;

    return false;
}

/**
 * Worker for #PFty_subtype () and #PFty_promotable.  
 * Test, whether @a t1 <: @a t2 (or @a t1 can be promoted to @a t2).
 *
 * @param t1 first type
 * @param t2 second type
 * @param a set of assumptions about already proven <: relationships
 * @return true if @a t1 <: @a t2 (or @a t1 can be promoted to @a t2), 
 *         false otherwise.
 */
static bool
subtype (PFty_t t1, PFty_t t2, ty_pair_set_t a)
{
    ty_set_t      ns;
    ty_pair_t    *ineq;
    pd_ineq_set_t pd;
    ty_pair_set_t ineqs;

    unsigned int n, i, k;

    bool proof;

#ifdef DEBUG_TYPES
    PFlog ("subtyping: (0) start proof [%s <: %s]", 
           PFty_str (t1), PFty_str (t2));

    PFlog ("subtyping: assuming the following:");
    if (ty_pair_set_card (a))
        for (n = 0; n < ty_pair_set_card (a); n++)
            PFlog ("subtyping:  %s <: %s",
                   PFty_str (ty_pair_set_elem (a, n)->fst),
                   PFty_str (ty_pair_set_elem (a, n)->snd));
    else
        PFlog ("subtyping:  [nothing]");
#endif /* DEBUG_TYPES */

    t1 = *PFty_simplify (PFty_defn (t1));
    t2 = *PFty_simplify (PFty_defn (t2));

#ifdef DEBUG_TYPES
    PFlog ("subtyping: (1) simplify inequality [%s <: %s]", 
           PFty_str (t1), PFty_str (t2));
#endif /* DEBUG_TYPES */

    /* consult the XQuery type hierarchy first */
    assert (trivial);
    switch ((*trivial)[t1.type][t2.type]) {
    case 0:
#ifdef DEBUG_TYPES
        PFlog ("subtyping: NO PROOF (trivial/type hierarchy)");
#endif /* DEBUG_TYPES */
        return false;

    case 1:
#ifdef DEBUG_TYPES
        PFlog ("subtyping: PROOF (trivial/type hierarchy)");
#endif /* DEBUG_TYPES */
        return true;

    default:
        /* <: still undecided ... */
        break;
    }

    /* t <: t */
    if (ty_eq (&t1, &t2)) {
#ifdef DEBUG_TYPES
        PFlog ("subtyping: PROOF (types equal)");
#endif /* DEBUG_TYPES */
        return true;
    }

#ifdef DEBUG_TYPES
    PFlog ("subtyping: (2) need Antimirov");
#endif /* DEBUG_TYPES */

    /* Antimirov: trivial inconsistency */
    if (nullable (t1) && ! nullable (t2)) {
#ifdef DEBUG_TYPES
        PFlog ("subtyping: NO PROOF (trivially inconsistent)"); 
#endif /* DEBUG_TYPES */
        return false;
    }

#ifdef DEBUG_TYPES
    PFlog ("subtyping: (3) not trivially inconsistent"); 
#endif /* DEBUG_TYPES */

    /* if s1 <: s2 is found in the set of assumptions, 
     * the proof is successful
     */
    if (assumption (t1, t2, a)) {
#ifdef DEBUG_TYPES
        PFlog ("subtyping: PROOF (found in assumptions)");
#endif /* DEBUG_TYPES */
        return true;
    }

#ifdef DEBUG_TYPES
    PFlog ("subtyping: (4) not found in assumptions, added");
#endif /* DEBUG_TYPES */
  
    /* Antimirov's/XOBE algorithm below */

    proof = true;

    /* add t1 <: t2 to the set of assumptions a */
    ineq = (ty_pair_t *) PFmalloc (sizeof (ty_pair_t));
    
    ineq->fst = t1;
    ineq->snd = t2;
    ty_pair_set_ins (a, ineq);
    
    ns = leadingnames (t1);
    assert (ns);

    for (n = 0; n < ty_set_card (ns); n++) {

#ifdef DEBUG_TYPES
        PFlog ("subtyping: (5) deriving by leadingname %s",
               PFty_str (*ty_set_elem (ns, n)));
#endif /* DEBUG_TYPES */

        pd = pd_ineq (ty_set_elem (ns, n), t1, t2);
        assert (pd);
    
        for (i = 0; i < pd_ineq_set_card (pd); i++) {
            ineqs = set_theory (pd_ineq_set_elem (pd, i));

            if (! ineqs) {
#ifdef DEBUG_TYPES
                PFlog ("subtyping: NO PROOF (no partial derivatives)");
#endif /* DEBUG_TYPES */
                return false;
            }
            
            /* make sure we received an even number of inequalities */
            assert (~(ty_pair_set_card (ineqs)) & 1);

            for (k = 0; k < ty_pair_set_card (ineqs); k += 2)
                /* for two adjacent inequalities t1 <: t2, s1 <: s2, invoke
                 * the proof procedure recursively:
                 *        proof = proof /\ ((t1 <: t2) \/ (s1 <: s2))
                 *
                 * (the semantics of &&/|| ensure that we bail out quickly
                 * if the proof will finally fail)
                 */
                proof = proof && 
                        (subtype (ty_pair_set_elem (ineqs, k)->fst, 
                                  ty_pair_set_elem (ineqs, k)->snd, a)
                         || 
                         subtype (ty_pair_set_elem (ineqs, k + 1)->fst,
                                  ty_pair_set_elem (ineqs, k + 1)->snd, a));
        }
    }
  
    return proof;
}

/**
 * The <: subtyping relationship.  Test whether @a t1 <: @a t2.
 *
 * @param t1 first type
 * @param t2 second type
 * @return true if @a t1 <: @a t2, false otherwise.
 */
bool 
PFty_subtype (PFty_t t1, PFty_t t2) 
{
    /* trivial subtyping relationships encoded in hierarchy table */
    trivial = &hierarchy;

    return subtype (t1, t2, ty_pair_set ()); 
}

/**
 * The equality relationship.  Test whether @a t1 = @a t2.
 *
 * @param t1 first type
 * @param t2 second type
 * @return true if @a t1 = @a t2, false otherwise.
 */
bool 
PFty_equality (PFty_t t1, PFty_t t2) 
{
    /* trivial subtyping relationships encoded in equality table */
    trivial = &equality;

    return subtype (t1, t2, ty_pair_set ()) &&
           subtype (t2, t1, ty_pair_set ()); 
}

/**
 * The `can be promoted to' relationship.  
 * Test whether @a t1 can be promoted to @a t2.
 *
 * @param t1 first type
 * @param t2 second type
 * @return true if @a t1 can be promoted to @a t2, false otherwise.
 */
bool 
PFty_promotable (PFty_t t1, PFty_t t2) 
{
    /* trivial `can be promoted to' relationships encoded in promotable
     * table 
     */
    trivial = &promotable;

    return subtype (t1, t2, ty_pair_set ()); 
}

/**
 * Worker for #PFty_disjoint ().  Test, whether @a t1 || @a t2
 * (@a t1 and @a t2 have an empty intersection).
 *
 * @param t1 first type
 * @param t2 second type
 * @param a set of assumptions about already proven || relationships
 * @return true if @a t1 || @a t2, false otherwise.
 */
static bool
disjoint (PFty_t t1, PFty_t t2, ty_pair_set_t a)
{
    ty_set_t      ns;
    ty_pair_t    *ineq;
    pd_ineq_set_t pd;
    ty_pair_set_t ineqs;

    unsigned int n, i, k;

    bool proof;

#ifdef DEBUG_TYPES
    PFlog ("disjoint: (0) start proof [%s || %s]", 
           PFty_str (t1), PFty_str (t2));

    PFlog ("disjoint: assuming the following:");
    if (ty_pair_set_card (a))
        for (n = 0; n < ty_pair_set_card (a); n++)
            PFlog ("disjoint:  %s || %s",
                   PFty_str (ty_pair_set_elem (a, n)->fst),
                   PFty_str (ty_pair_set_elem (a, n)->snd));
    else
        PFlog ("disjoint:  [nothing]");
#endif /* DEBUG_TYPES */

    t1 = *PFty_simplify (PFty_defn (t1));
    t2 = *PFty_simplify (PFty_defn (t2));

#ifdef DEBUG_TYPES
    PFlog ("disjoint: (1) simplify types [%s || %s]", 
           PFty_str (t1), PFty_str (t2));
#endif /* DEBUG_TYPES */

    /* consult the XQuery type hierarchy first */
    assert (trivial);
    switch ((*trivial)[t1.type][t2.type]) {
    case 0:
#ifdef DEBUG_TYPES
        PFlog ("disjoint: PROOF (trivial/type hierarchy)");
#endif /* DEBUG_TYPES */
        return true;

    case 1:
#ifdef DEBUG_TYPES
        PFlog ("disjoint: NO PROOF (trivial/type hierarchy)");
#endif /* DEBUG_TYPES */
        return false;

    default:
        /* || still undecided ... */
        break;
    }

    /* t || t */
    if (ty_eq (&t1, &t2)) {
#ifdef DEBUG_TYPES
        PFlog ("disjoint: NO PROOF (types equal)");
#endif /* DEBUG_TYPES */
        return false;
    }

#ifdef DEBUG_TYPES
    PFlog ("disjoint: (2) need Antimirov");
#endif /* DEBUG_TYPES */

    /* Antimirov: trivial inconsistency */
    if (nullable (t1) && nullable (t2)) {
#ifdef DEBUG_TYPES
        PFlog ("disjoint: NO PROOF (trivial intersection: ())"); 
#endif /* DEBUG_TYPES */
        return false;
    }

#ifdef DEBUG_TYPES
    PFlog ("disjoint: (3) no trivial intersection"); 
#endif /* DEBUG_TYPES */

    /* if s1 || s2 is found in the set of assumptions, 
     * the proof is successful
     */
    if (assumption (t1, t2, a)) {
#ifdef DEBUG_TYPES
        PFlog ("disjoint: PROOF (found in assumptions)");
#endif /* DEBUG_TYPES */
        return true;
    }

#ifdef DEBUG_TYPES
    PFlog ("disjoint: (4) not found in assumptions, added");
#endif /* DEBUG_TYPES */
  
    /* Antimirov's/XOBE algorithm below */

    proof = true;

    /* add t1 || t2 to the set of assumptions a */
    ineq = (ty_pair_t *) PFmalloc (sizeof (ty_pair_t));
    
    ineq->fst = t1;
    ineq->snd = t2;
    ty_pair_set_ins (a, ineq);
    
    ns = leadingnames (t1);
    assert (ns);

    for (n = 0; n < ty_set_card (ns); n++) {

#ifdef DEBUG_TYPES
        PFlog ("disjoint: (5) deriving by leadingname %s",
               PFty_str (*ty_set_elem (ns, n)));
#endif /* DEBUG_TYPES */

        pd = pd_ineq (ty_set_elem (ns, n), t1, t2);
        assert (pd);
    
        for (i = 0; i < pd_ineq_set_card (pd); i++) {
            ineqs = set_theory (pd_ineq_set_elem (pd, i));

            if (! ineqs) {
#ifdef DEBUG_TYPES
                PFlog ("disjoint: continuing (no partial derivatives)");
#endif /* DEBUG_TYPES */
                continue;
            }
            
            /* make sure we received an even number of disjointness
             * proofs
             */
            assert (~(ty_pair_set_card (ineqs)) & 1);

            proof = false;

            for (k = 0; k < ty_pair_set_card (ineqs); k += 2)
                /* for two disjointness proofs t1 || t2, s1 || s2, invoke
                 * the proof procedure recursively:
                 *        proof = proof \/ ((t1 || t2) /\ (s1 || s2))
                 *
                 * (the semantics of &&/|| ensure that we bail out quickly
                 * if the proof will finally fail)
                 */
                proof = proof ||
                        (disjoint (ty_pair_set_elem (ineqs, k)->fst, 
                                   ty_pair_set_elem (ineqs, k)->snd, a)
                         &&
                         disjoint (ty_pair_set_elem (ineqs, k + 1)->fst,
                                   ty_pair_set_elem (ineqs, k + 1)->snd, a));
        }
    }
  
    return proof;
}

/**
 * The || disjointness relationship.  Test whether @a t1 || @a t2
 * (the intersection of @a t1 and @a t2 is empty).
 *
 * @param t1 first type
 * @param t2 second type
 * @return true if @a t1 || @a t2, false otherwise.
 */
bool 
PFty_disjoint (PFty_t t1, PFty_t t2) 
{
    /* trivial non-empty intersections encoded in hierarchy table */
    trivial = &intersect;

    return disjoint (t1, t2, ty_pair_set ()); 
}


/**
 * Compare two types t1, t2 for structural equality.
 *
 * @param t1 type t1
 * @param t2 type t2
 * @return true if t1, t2 are structurally equal, false otherwise
 */
bool
PFty_eq (PFty_t t1, PFty_t t2)
{
    return ty_eq (&t1, &t2);
}



/**
 * Transform an arbitrarily nested type built using the binary type
 * constructor # such that we get a right-deep chain of
 * #-applications.
 *
 *                       #               #
 *                      / \             / \
 *  right_deep (#,     #   t3 )   =   t0   #
 *                    / \                 / \
 *                   #   t2             t1   #
 *                  / \                     / \
 *                 t0  t1                 t2   t3
 *
 * @param k kind of binary type constructor
 * @param t type to transform
 * @return (pointer to) right-deep transformed type
 */
static PFty_t *
right_deep (PFtytype_t k, PFty_t *t)
{
    PFty_t *s0, *s1;

    if (t->type == k && t->child[0]->type == k) {
        s0 = (PFty_t *) PFmalloc (sizeof (PFty_t));

        s0->type = k;
        s0->child[0] = t->child[0]->child[1];
        s0->child[1] = right_deep (k, t->child[1]);

        s1 = (PFty_t *) PFmalloc (sizeof (PFty_t));    
    
        s1->type = k;
        s1->child[1] = right_deep (k, s0);
        s1->child[0] = right_deep (k, t->child[0]->child[0]);
    
        return right_deep (k, s1);
    }
  
    return t;
}

/**
 * Compute the prime type for a given (recursive) type @a t (see W3C
 * XQuery, 3.5).  
 *
 * @attention N.B. Make sure apply #PFty_defn () to @a t first, otherwise
 *                 the result may not be correct for recursive types.
 *
 *
 * W3C XQuery, 3.5:
 *
 * - prime (())      = none
 * - prime (none)    = none
 * - prime (t1, t2)  = prime (t1) | prime (t2)
 * - prime (t1 & t2) = prime (t1) | prime (t2)
 * - prime (t1 | t2) = prime (t1) | prime (t2)
 * - prime (t?)      = prime (t)
 * - prime (t*)      = prime (t)
 * - prime (t+)      = prime (t)
 * - prime (named n) = none           
 * - prime (t)       = t
 *
 * @param t type whose prime type is sought
 * @return prime type for @a t
 */
PFty_t
PFty_prime (PFty_t t)
{
    switch (t.type) {
    case ty_empty:
    case ty_none:
        return PFty_none ();

    case ty_seq:
    case ty_choice:
    case ty_all: {
        PFty_t c = PFty_choice (PFty_prime (*(t.child[0])),
                                PFty_prime (*(t.child[1])));
        return *right_deep (ty_choice, &c);
    }
    
    case ty_opt:
    case ty_star:
    case ty_plus:
        return PFty_prime (*(t.child[0]));

    case ty_named:
        /* this must be the occurrence of a recursive type reference, so
         * nothing new can be learned from looking at the referenced type
         */
        return PFty_none ();

    default:
        return t;
    }
}

/**
 * Worker for #PFty_data_on().
 */
static PFty_t
data_on (PFty_t t)
{
    switch (t.type) {

        case ty_none:
            return PFty_none ();

        case ty_empty:
            return PFty_empty ();

        case ty_seq:
        case ty_choice:
        case ty_all:
            {
                PFty_t c = PFty_choice (PFty_data_on (*(t.child[0])),
                                        PFty_data_on (*(t.child[1])));
                return *right_deep (ty_choice, &c);
            }
    
        case ty_opt:
        case ty_star:
            return PFty_opt (PFty_data_on (*(t.child[0])));

        case ty_plus:
            return PFty_data_on (*(t.child[0]));

        case ty_named:
            /* this must be the occurrence of a recursive type
             * reference, so nothing new can be learned from
             * looking at the referenced type
             */
            return PFty_none ();

        default:
            if (PFty_subtype (t, PFty_atomic ()))
                return t;
            else if (PFty_subtype (t, PFty_choice (PFty_comm (),
                                                   PFty_pi (NULL))))
                return PFty_xs_string ();
            else if (PFty_subtype (t, PFty_xs_anyElement ())) {
                PFty_t t1 = PFty_data_on (content (t));
                if (PFty_subtype (t1, PFty_xs_anySimpleType ()))
                    return t1;
                else
                    return PFty_untypedAtomic ();
            }
            /* Disregard attributes in element content */
            else if (PFty_subtype (t, PFty_xs_anyAttribute ()))
                return PFty_none ();
            else
                return PFty_untypedAtomic ();
    }
}

/**
 * Implementation of the "data on" judgement (W3C Formal Semantics 6.2.3)
 * that describes the special typing rules for the fn:data() function.
 *
 *  - data on (none)    = none
 *  - data on (empty)   = empty
 *  - data on (t1 , t2) = data on (t1) | data on (t2)
 *  - data on (t1 | t2) = data on (t1) | data on (t2)
 *  - data on (t1 & t2) = data on (t1) | data on (t2)
 *  - data on (t1?)     = data on (t1) | empty
 *  - data on (t1*)     = data on (t1) | empty
 *  - data on (t1+)     = data on (t1)
 *  - data on (t)       = t                  if t <: atomic
 *  - data on (t)       = xs:string          if t <: comment | processing-instr
 *  - data on (t)       = data on (t')       if t <: elem * { t' }
 *                                               or t <: attr * { t' }  [1]
 *  - data on (t)       = xdt:untypedAtomic  if t <: text | doc
 *
 *  - data on (named n) = none
 *
 * [1] Remark: We only look into attributes on top-level. We disregard
 *     attributes that are within the content of some element.
 */
PFty_t
PFty_data_on (PFty_t t)
{
    if (t.type == ty_attr) {
        PFty_t t1 = data_on (content (t));
        if (PFty_subtype (t1, PFty_xs_anySimpleType ()))
            return t1;
        else
            return PFty_untypedAtomic ();
    }
    else
        return data_on (t);
}

/**
 * Replaces all atomic types by text()
 *  - is2ns (t1 , t2) = is2ns (t1) , is2ns (t2)
 *  - is2ns (t1 | t2) = is2ns (t1) | is2ns (t2)
 *  - is2ns (t1 & t2) = is2ns (t1) | is2ns (t2)
 *  - is2ns (t1?)     = is2ns (t1)?
 *  - is2ns (t1*)     = is2ns (t1)*
 *  - is2ns (t1+)     = is2ns (t1)+
 *  - is2ns (named n) = none
 *  - is2ns (t)       = text()         if t <: atomic
 *  - is2ns (t)       = t              if t <: node
 *  - is2ns (t)       = node           otherwise
 */
PFty_t
PFty_is2ns (PFty_t t)
{
    switch (t.type) {

        case ty_seq:
            {
                PFty_t c = PFty_seq (PFty_is2ns (*(t.child[0])),
                                     PFty_is2ns (*(t.child[1])));
                return *right_deep (ty_seq, &c);
            }
        case ty_choice:
        case ty_all:
            {
                PFty_t c = PFty_choice (PFty_is2ns (*(t.child[0])),
                                        PFty_is2ns (*(t.child[1])));
                return *right_deep (ty_choice, &c);
            }
    
        case ty_opt:
            return PFty_opt (PFty_is2ns (*(t.child[0])));
        case ty_star:
            return PFty_star (PFty_is2ns (*(t.child[0])));

        case ty_plus:
            return PFty_plus (PFty_is2ns (*(t.child[0])));

        case ty_named:
            /* this must be the occurrence of a recursive type
             * reference, so nothing new can be learned from
             * looking at the referenced type
             */
            return PFty_none ();

        default:
            if (PFty_subtype (t, PFty_atomic ()))
                return PFty_text();
            else if (PFty_subtype (t, PFty_xs_anyNode ()))
                return t;
            else
                /*
                 * We've broken up the input type as much as possible.
                 * Still, the type is neither a subtype of `atom', nor
                 * of `node' (probably we ended up with `item'). So we
                 * fall back to the most general case and return `node'.
                 */
                return PFty_xs_anyNode ();
    }
}

enum quantifier {
    none = 0      /**<  0  */
  , one           /**<  1  */
  , opt           /**<  ?  */
  , plus          /**<  +  */
  , star          /**<  *  */
};


/**
 * Quantifier sum (see W3C XQuery, 3.5).
 */
static enum quantifier
sum[5][5] = {
              /*  0     1     ?     +     *   */
    /*  0  */  { none, one,  opt,  plus, star }
    /*  1  */ ,{ one,  plus, plus, plus, plus }
    /*  ?  */ ,{ opt,  plus, star, plus, star }
    /*  +  */ ,{ plus, plus, plus, plus, plus }
    /*  *  */ ,{ star, plus, star, plus, star }
};
  
/**
 * Quantifier choice (see W3C XQuery, 3.5).
 */
static enum quantifier
choice[5][5] = {
              /*  0     1     ?     +     *   */
    /*  0  */  { none, opt,  opt,  star, star }
    /*  1  */ ,{ opt,  one,  opt,  plus, star }
    /*  ?  */ ,{ opt,  opt,  opt,  star, star }
    /*  +  */ ,{ star, plus, star, plus, star }
    /*  *  */ ,{ star, star, star, star, star }
};
  
/**
 * Quantifier product (see W3C XQuery, 3.5).
 */
static enum quantifier
product[5][5] = {
              /*  0     1     ?     +     *   */
    /*  0  */  { none, none, none, none, none }
    /*  1  */ ,{ none, one,  opt,  plus, star }
    /*  ?  */ ,{ none, opt,  opt,  star, star }
    /*  +  */ ,{ none, plus, star, plus, star }
    /*  *  */ ,{ none, star, star, star, star }
};

/** 
 * Implements the 0 quantifier: t . 0 = ()
 */
static PFty_t
fn_none (PFty_t unused)
{
    (void) unused;
    return PFty_empty ();
}

/**
 * Implements the 1 quantifier: t . 1 = t 
 */
static PFty_t
fn_one (PFty_t t)
{
    return t;
}

/**
 * Functions implementing the quantifers.
 */
static PFty_t (*quantifier_fn[5]) (PFty_t) = {
    fn_none
    , fn_one
    , PFty_opt
    , PFty_plus
    , PFty_star
};

/**
 * W3C XQuery, 3.5:
 *
 * - quantifier (())      = 0
 * - quantifier (none)    = 0
 * - quantifier (t1, t2)  = quantifier (t1) , quantifier (t2)
 * - quantifier (t1 & t2) = quantifier (t1) , quantifier (t2)
 * - quantifier (t1 | t2) = quantifier (t1) | quantifier (t2)
 * - quantifier (t?)      = quantifier (t) . ?
 * - quantifier (t*)      = quantifier (t) . *
 * - quantifier (t+)      = quantifier (t) . +
 * - quantifier (named n) = 1
 * - quantifier (t)       = 1
 */
static enum quantifier
quantifier (PFty_t t)
{
    switch (t.type) {
    case ty_empty:
    case ty_none:
        return none;

    case ty_seq:
    case ty_all:
        return sum [quantifier (*(t.child[0]))][quantifier (*(t.child[1]))];
    
    case ty_choice:
        return choice [quantifier (*(t.child[0]))][quantifier (*(t.child[1]))];

    case ty_opt:
        return product [quantifier (*(t.child[0]))][opt];

    case ty_star:
        return product [quantifier (*(t.child[0]))][star];

    case ty_plus:
        return product [quantifier (*(t.child[0]))][plus];

    case ty_named:
        /* this is a recursive occurence of a named type */
        return one;

    default:
        return one;
    };
}

/** 
 * The quantifier of a given (recursive) type @a t (see W3C XQuery, 3.5).
 *
 * This returns a (pointer to a) function @a q representing the quantifier:
 * when applied to a type @a t, @a q adds the quantifier to @a t
 * as described in W3C XQuery, 3.5 (Semantics):
 *
 *                             q (t) = t . q
 *
 * @attention N.B. Make sure apply #PFty_defn () to @a t first, otherwise
 *                 the result may not be correct for recursive types.
 *
 * @param t type whose quantifier is sought
 * @return (pointer to) function representing the quantifier of @a t
 */
PFty_t (*PFty_quantifier (PFty_t t)) (PFty_t)
{
    return quantifier_fn[quantifier (t)];
}


/**
 * Callback invoked whenever a new permutation ts = [ t1, t2, ..., tn ]
 * of the n types of an all group t1 & t2 & ... & tn has been computed.
 * & semantics: add the sequence t1, t2, ..., tn to the alternatives alts
 * computed so far:
 *                   alts = (t1, t2, ..., tn) | alts
 */
static PFty_t alts;

static void
_alt (ty_set_t ts)
{
    alts = PFty_choice (foldr1 (ts, PFty_seq), alts);
}


/** 
 * Simplify a given type using a list of given type simplfication rules.
 *
 * @param t type to simplify
 * @return (pointer to) simplified type
 */
PFty_t *
PFty_simplify (PFty_t t)
{
    PFty_t *s;
    PFty_t fix;

    s = (PFty_t *) PFmalloc (sizeof (PFty_t));

    *s = t;

    /* iteratively simplify s, 
     * stop when simplification reached a fixpoint
     */
    do {
        fix = *s;

        switch (s->type) {

        case ty_opt:
            /* s*? = s* */
            /* s?? = s? */
            s->child[0] = PFty_simplify (*(s->child[0]));

            if (s->child[0]->type == ty_star)
                s = s->child[0];

            if (s->child[0]->type == ty_opt)
                s = s->child[0];
            break;

        case ty_plus:
            /* ()+ = () */
            /* s++ = s+ */
            s->child[0] = PFty_simplify (*(s->child[0]));

            switch (s->child[0]->type) {
            case ty_empty:
                *s = PFty_empty ();
                break;

            case ty_plus:
                s = s->child[0];

            default:
                break;
            }

            break;

        case ty_star:
            /* ()*   = () 
             * none* = ()
             * s**   = s*
             * s?*   = s*
             */
            s->child[0] = PFty_simplify (*(s->child[0]));

            switch (s->child[0]->type) {
            case ty_empty:
            case ty_none:
                *s = PFty_empty ();
                break;

            case ty_star:
                s = s->child[0];
                break;

            case ty_opt:
                s->child[0] = s->child[0]->child[0];
                break;

            default:
                break;
            }

            break;

        case ty_seq:
            /* (), s    = s
             * s, ()    = s 
             * s, none  = none
             * none, s  = none
             */
            s->child[0] = PFty_simplify (*(s->child[0]));
            s->child[1] = PFty_simplify (*(s->child[1]));

            if (s->child[0]->type == ty_empty)
                s = s->child[1];
            else if (s->child[1]->type == ty_empty)
                s = s->child[0];
            else if (s->child[1]->type == ty_none)
                *s = PFty_none ();
            else if (s->child[0]->type == ty_none)
                *s = PFty_none ();
            break;

        case ty_choice:
            /* none | s                  = s
             * s | none                  = s 
             * s1 | (s2 | (... | sn)...) = sigma (nub { s1, s2, ..., sn })
             */
            s->child[0] = PFty_simplify (*(s->child[0]));
            s->child[1] = PFty_simplify (*(s->child[1]));

            if (s->child[0]->type == ty_none)
                s = s->child[1];
            else if (s->child[1]->type == ty_none)
                s = s->child[0];
            else {
                ty_set_t ts = ty_set ();
                PFty_t *cg  = right_deep (ty_choice, s);

                /* construct ts = { sn, ..., s2, s1 } from the choice group
                 * s1 | (s2 | (... | sn)...)
                 */
                do {
                    ty_set_ins (ts, PFty_simplify (*(cg->child[0])));
                    cg = cg->child[1];
                } while (cg->type == ty_choice);
                ty_set_ins (ts, PFty_simplify (*cg));

                *s = sigma (nub (ts, (bool (*) (void *, void *)) ty_eq));
            }
            break;

        case ty_all:
            /* s1 & s2 & ... & sn  =
             *   (s1,s2,...,sn) | (s2,s1,...,sn) | ... | (sn,...,s2,s1) 
             */
            {   /* replace an n-ary &-group by its equivalent: & really is an
                 * n-ary operator; we assume a right-deep chain of depth (n-1)
                 * of & applications to represent the &-group;
                 * let s = s1 & (s2 & (s3 & (... & sn)...))
                 */
                ty_set_t ts = ty_set ();
                PFty_t  *ag = right_deep (ty_all, s);

                /* construct ts = { sn, ..., s2, s1 } */
                do {
                    ty_set_ins (ts, ag->child[0]);
                    ag = ag->child[1];
                } while (ag->type == ty_all);
                ty_set_ins (ts, ag);

                /* & semantics:
                 * compute all permutation sequences of ts, and combine these
                 * via `|'; we may start out with `none', since: none | s = s.
                 */
                alts = PFty_none ();
                perms (ts, _alt);

                /* s = (s1,s2,...,sn) | (s2,s1,...,sn) | ... | (sn,...,s2,s1) */
                *s = alts;
                break;
            }

        case ty_elem:
        case ty_attr:
        case ty_doc:
            s->child[0] = PFty_simplify (*(s->child[0]));
            break;

        default:
            /* failed to simplify type s */
            break;
        }
    }
    while (! ty_eq (&fix, s));

    return s;
}


/* vim:set shiftwidth=4 expandtab: */
