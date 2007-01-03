/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Pathfinder's implementation of the XQuery subtyping relation `<:'
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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#ifndef SUBTYPING_H
#define SUBTYPING_H

#include "pathfinder.h"

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

/** The equality relationship */
bool PFty_equality (PFty_t, PFty_t);

/** The || (disjointness) relationship */
bool PFty_disjoint (PFty_t, PFty_t);

/** The `can be promoted to' relationship */
bool PFty_promotable (PFty_t, PFty_t);

/** Perform well-formedness (regularity) test for recursive types */
bool PFty_regularity (PFty_t);

/** Simplify a given type using a list of given type simplfication rules. */
PFty_t *PFty_simplify (PFty_t);

/** The prime type of a given type (apply #PFty_defn () first!). */
PFty_t PFty_prime (PFty_t);

/** Normalize type choices. */
PFty_t PFty_normalize_choice (PFty_t t);

/** The `data on' judgement as in W3C Formal Semantics 6.2.3. */
PFty_t PFty_data_on (PFty_t t);

/** The `is2ns' replaces all atomic types by text() */
PFty_t PFty_is2ns (PFty_t t);

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
void ty_FIXME (void);
#endif

#ifndef NDEBUG

/**
 * Perform a number of internal tests with respect to the subtyping
 * algorithms.  Accessible via command line option --debug.
 * Facilitates testing the algorithms with Mtest.
 */
void PFty_debug_subtyping (void);

#endif

#endif /* SUBTYPING_H */

/* vim:set shiftwidth=4 expandtab: */
