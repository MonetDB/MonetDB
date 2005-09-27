/**
 * @file
 *
 * Deal with table orderings in physical algebra plans.
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
 * $Id$
 */

/* Always include pathfinder.h first! */
#include "pathfinder.h"

#include <assert.h>

#include "ordering.h"

#include "array.h"

PFord_ordering_t
PFordering (void)
{
    return PFarray (sizeof (PFalg_att_t));
}

PFord_set_t
PFord_set (void)
{
    return PFarray (sizeof (PFord_ordering_t));
}

PFord_ordering_t
PFord_refine (const PFord_ordering_t ordering, const PFalg_att_t attribute)
{
    /* construct new PFord_ordering_t object */
    PFord_ordering_t ret = PFordering ();

    /* copy existing orderings to return value */
    for (unsigned int i = 0; i < PFarray_last (ordering); i++)
        *((PFalg_att_t *) PFarray_add (ret))
            = *((PFalg_att_t *) PFarray_at (ordering, i));

    *((PFalg_att_t *) PFarray_add (ret)) = attribute;

    return ret;
}

unsigned int
PFord_count (const PFord_ordering_t ordering)
{
    return PFarray_last (ordering);
}

PFalg_att_t
PFord_order_at (const PFord_ordering_t ordering, unsigned int index)
{
    assert (index < PFord_count (ordering));

    return *((PFalg_att_t *) PFarray_at (ordering, index));
}

bool
PFord_implies (const PFord_ordering_t a, const PFord_ordering_t b)
{
    /* For a to imply b, it must at least have as many attributes. */
    if (PFord_count (a) < PFord_count (b))
        return false;

    /* Attribute list of b must be a prefix of attribute list of a. */
    for (unsigned int i = 0; i < PFord_count (b); i++)
        if (PFord_order_at (a, i) != PFord_order_at (b, i))
            return false;

    return true;
}

bool
PFord_common_prefix (const PFord_ordering_t a, const PFord_ordering_t b)
{
    return PFord_count (a) && PFord_count (b)
        && PFord_order_at (a, 0) == PFord_order_at (b, 0);
}

char *
PFord_str (const PFord_ordering_t o)
{
    PFarray_t *a = PFarray (sizeof (char));

    PFarray_printf (a, "<");

    for (unsigned int i = 0; i < PFord_count (o); i++)
        PFarray_printf (a, "%s%s", i ? "," : "", 
                        PFatt_print (PFord_order_at (o, i)));

    PFarray_printf (a, ">");

    return (char *) a->base;
}

unsigned int
PFord_set_count (const PFord_set_t set)
{
    return PFarray_last (set);
}

PFord_set_t
PFord_set_add (PFord_set_t set, const PFord_ordering_t ord)
{
    *(PFord_ordering_t *) PFarray_add (set) = ord;

    return set;
}

PFord_ordering_t
PFord_set_at (const PFord_set_t set, unsigned int i)
{
    return *(PFord_ordering_t *) PFarray_at (set, i);
}

PFord_set_t
PFord_intersect (const PFord_set_t a, const PFord_set_t b)
{
    PFord_set_t ret = PFord_set ();

    /*
     * For each ordering in a search for the longest prefix with
     * any ordering in b.
     */
    for(unsigned int i = 0; i < PFord_set_count (a); i++) {

        PFord_ordering_t ai          = PFord_set_at (a, i);
        PFord_ordering_t best_prefix = PFordering ();

        for (unsigned int j = 0; j < PFord_set_count (b); j++) {

            PFord_ordering_t bj     = PFord_set_at (b, j);
            PFord_ordering_t prefix = PFordering ();

            /* compute the longest prefix of the current ordering in a and b */
            for (unsigned int k = 0;
                 k < PFord_count (ai) && k < PFord_count (bj)
                    && PFord_order_at (ai, k) == PFord_order_at (bj, k);
                 k++)
                prefix = PFord_refine (prefix, PFord_order_at (ai, k));

            /* keep it if it is better than what we have so far */
            if (PFord_count (prefix) > PFord_count (best_prefix))
                best_prefix = prefix;

            /* if we already meet the full ordering of our current
             * ordering from a, we're done (for this i).
             */
            if (PFord_count (best_prefix) == PFord_count (ai))
                break;
        }

        /* If we actually found a prefix, add it to the result */
        if (PFord_count (best_prefix) > 0)
            PFord_set_add (ret, best_prefix);
    }

    /* remove duplicates or prefixes that are implied by some other prefix */
    return  (ret);
}

/**
 * Compute duplicate-free equivalent of the list of orderings @a a.
 * This also eliminates orderings that are implied by some more
 * specific ordering.
 *
 * We proceed in two phases:
 *  - Scan the input list forwardly.  Copy all those orderings
 *    to an intermediate result that are not already implied by
 *    some ordering in there.
 *  - Scan this intermediate result backwardly.  Again, copy only
 *    those orderings to the result for which we don't already
 *    have better orderings.
 */
PFord_set_t
PFord_unique (const PFord_set_t a)
{
    PFord_set_t tmp = PFord_set ();
    PFord_set_t ret = PFord_set ();
    unsigned int i;
    unsigned int j;

    /* process input list forward */
    for (i = 0; i < PFord_set_count (a); i++) {
        for (j = 0; j < PFord_set_count (tmp); j++)
            if (PFord_implies (PFord_set_at (tmp, j), PFord_set_at (a, i)))
                break;
        if (j == PFord_set_count (tmp))
            PFord_set_add (tmp, PFord_set_at (a, i));
    }
    /* and backward */
    for (i = 0; i < PFord_set_count (tmp); i++) {
        for (j = 0; j < PFord_set_count (ret); j++)
            if (PFord_implies (PFord_set_at (ret, j), PFord_set_at (tmp, i)))
                break;
        if (j == PFord_set_count (ret))
            PFord_set_add (ret, PFord_set_at (tmp, i));
    }

    return ret;
}

/**
 * Given a list of orderings @a a, compute all prefixes of the
 * orderings in @a a.
 *
 * This ``expands'' the current list of orderings by all those
 * orderings that the original list implies.  Does not produce
 * 1:1 duplicates in the result.
 */
PFord_set_t
PFord_prefixes (PFord_set_t a)
{
    PFord_set_t ret = PFord_set ();

    for (unsigned int i = 0; i < PFord_set_count (a); i++) {

        PFord_ordering_t ai     = PFord_set_at (a, i);
        PFord_ordering_t prefix = PFordering ();

        for (unsigned int j = 0; j < PFord_count (ai); j++) {

            prefix = PFord_refine (prefix, PFord_order_at (ai, j));

            /* make sure we do not already have that prefix in the list */
            unsigned int k;
            for (k = 0; k < PFord_set_count (ret); k++)
                if (PFord_implies (prefix, PFord_set_at (ret, k))
                    && PFord_implies (PFord_set_at (ret, k), prefix))
                    break;

            if (k == PFord_set_count (ret))
                PFord_set_add (ret, prefix);
        }
    }

    return ret;
}

/**
 * Helper function: Compute all permutations of ordering @a ordering
 * and return them as a #PFord_set_t set of #PFord_ordering_t's.
 *
 * (Literal tables, e.g., follow all possible orderings if they
 * contain exactly one tuple.)
 *
 * @param ordering Start ordering from which we compute all possible
 *                 permutations.
 * @return Set (#PFord_set_t) of orderings (#PFord_ordering_t).
 *         Contains all possible permutations of @a ordering.
 */
PFord_set_t
PFord_permutations (const PFord_ordering_t ordering)
{
    PFord_set_t ret = PFord_set ();

    /*
     * End of recursion: If we get an empty ordering, return a
     * list that just contains an empty ordering.
     */
    if (PFord_count (ordering) == 0)
        return PFord_set_add (ret, PFordering ());
    
    for (unsigned int i = 0; i < PFord_count (ordering); i++) {

        /* Compute all permutations with attribute at position i removed. */
        PFord_ordering_t  removed = PFordering ();
        PFord_set_t       subperms;

        for (unsigned int j = 0; j < PFord_count (ordering); j++)
            if (j != i)
                removed = PFord_refine (removed, PFord_order_at (ordering, j));

        subperms = PFord_permutations (removed);

        /* Refine all this permutations with the attribute at position i. */
        for (unsigned int j = 0; j < PFarray_last (subperms); j++)
            PFord_set_add (ret,
                           PFord_refine (PFord_set_at (subperms, j),
                                         PFord_order_at (ordering, i)));
    }

    return ret;
}

/* vim:set shiftwidth=4 expandtab: */
