/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Pathfinder's implementation of the XQuery subtyping relation `<:'
 *
 * $Id$
 */

#ifndef SUBTYPING_H
#define SUBTYPING_H

#include "types.h"

/** 
 * Compare two types for _structural_ equality.
 *
 * Note that this equality test is quite ``dumb''. It does not consider
 * equivalences like `t1 | t2' == `t2 | t1'.
 */
bool PFty_eq (PFty_t, PFty_t);

/** The <: subtype relationship */
bool PFty_subtype (PFty_t, PFty_t);

/** The || (disjointness) relationship */
bool PFty_disjoint (PFty_t, PFty_t);

/** Perform well-formedness (regularity) test for recursive types */
bool PFty_regularity (PFty_t);

/** Simplify a given type using a list of given type simplfication rules. */
PFty_t *PFty_simplify (PFty_t);

/** The prime type of a given type (apply #PFty_defn () first!). */
PFty_t PFty_prime (PFty_t);

/** 
 * The quantifier of a given type (apply #PFty_defn () first!). 
 * This returns a (pointer to a) function @a q representing the quantifier:
 * when applied to a type @a t, @a q adds the quantifier to @a t
 * as described in W3C XQuery, 3.5 (Semantics):
 *
 *                             q (t) = t . q
 */
PFty_t (*PFty_quantifier (PFty_t)) (PFty_t);

/** FIXME: type system debugging, to be removed eventually */
#ifdef DEBUG_TYPES 
void ty_FIXME ();
#endif


#endif /* SUBTYPING_H */

/* vim:set shiftwidth=4 expandtab: */
