/**
 * @file
 *
 * Introduce the borders of recursion bodies.
 * (This enables the MIL generation to detect expressions
 *  that are invariant to the recursion body.)
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

/* always include pathfinder.h first! */
#include "pathfinder.h"

#include "intro_rec_border.h"
#include "alg_dag.h"

#include <assert.h>

/* short-hands */
#define L(p) ((p)->child[0])
#define R(p) ((p)->child[1])
#define LR(p) (R(L(p)))

#define SEEN(n) n->bit_dag
#define pfIN(n) n->bit_in

/**
 * Worker that introduces border operators and 'in' flags.
 * Nodes are marked 'inside' as soon as one child reports
 * that one of its descendants is a base operator belonging
 * to the current recursion.
 */
static bool
introduce_rec_borders_worker (PFpa_op_t *n, PFarray_t *bases)
{
    unsigned int i;
    bool base_path = false;

    /* short-cut in case n is already determined as 'inside' */
    if (pfIN(n))
        return true;

    switch (n->kind)
    {
        /* make sure we don't follow fragment */
        case pa_frag_union:
        case pa_empty_frag:
            return false;

        /* ignore nested recursions and only collect the path */
        case pa_rec_fix:
            base_path = introduce_rec_borders_worker (L(n), bases);
            break;
        case pa_rec_param:
            base_path = introduce_rec_borders_worker (L(n), bases);
            base_path = introduce_rec_borders_worker (R(n), bases)
                        || base_path;
            break;
        case pa_nil:
            break;
        case pa_rec_arg:
            base_path = introduce_rec_borders_worker (L(n), bases);
            break;
            
        /* if the base operator belongs to the currently searched
           recursion mark the node as inside the recursion */
        case pa_rec_base:
            for (i = 0; i < PFarray_last (bases); i++)
                if (n == *(PFpa_op_t **) PFarray_at (bases, i)) {
                    base_path = true;
                    break;
                }
            break;

        case pa_fcns:
            /* this also skips the introduction of a rec_border
               operator for the content of an empty elements:
               elem (fcns (nil, nil)). */
            if (R(n)->kind == pa_nil)
                break;
            /* else fall through */
        default:
            /* follow the children until a base or a leaf is reached */
            for (unsigned int i = 0; i < PFPA_OP_MAXCHILD && n->child[i]; i++)
                base_path = introduce_rec_borders_worker (n->child[i], bases)
                            || base_path;

            /* Introduce border if the current node is 'inside'
               the recursion while its left child is not.
               Make sure that no borders are introduced along the
               fragment information edge. */
            if (base_path && L(n) && !pfIN(L(n)) && 
                L(n)->kind != pa_frag_union && 
                L(n)->kind != pa_empty_frag) {
                L(n) = PFpa_rec_border (L(n));
                L(n)->prop = L(L(n))->prop;
            }
            /* Introduce border if the current node is 'inside'
               the recursion while its right child is not. */
            if (base_path && R(n) && !pfIN(R(n)) &&
                R(n)->kind != pa_fcns) {
                R(n) = PFpa_rec_border (R(n));
                R(n)->prop = L(R(n))->prop;
            }
            break;
    }
    if (base_path)
        pfIN(n) = true;

    return base_path;
}

/**
 * reset the 'in' bits.
 * (We know that these 'in' bits are set for the seed
 *  and all lie on a path starting from the seed.)
 */
static void
in_reset (PFpa_op_t *n)
{
    unsigned int i;

    if (!pfIN(n))
        return;
    else
        pfIN(n) = false;
    
    for (i = 0; i < PFPA_OP_MAXCHILD && n->child[i]; i++)
        in_reset (n->child[i]);
}

/**
 * Walk down the DAG and for each recursion operator 
 * introduce border operators.
 *
 * We mark all operators that lie on the path from the
 * result (or the recursion arguments) to the base 
 * operators of the recursion as inside the recursion body.
 *
 * A border is introduced between nodes (a) and (b) where
 * (a) is the parent of (b), (a) lies in the set of nodes marked
 * as inside, and (b) lies in the set of nodes marked as outside
 * of the recursion body.
 */
static void
introduce_rec_borders (PFpa_op_t *n)
{
    if (SEEN(n))
        return;
    else
        SEEN(n) = true;

    switch (n->kind)
    {
        case pa_rec_fix:
        {
            PFarray_t *bases = PFarray (sizeof (PFpa_op_t *));
            PFpa_op_t *cur;

            /* collect base operators */
            cur = L(n);
            while (cur->kind != pa_nil) {
                assert (cur->kind == pa_rec_param && 
                        L(cur)->kind == pa_rec_arg);
                *(PFpa_op_t **) PFarray_add (bases) = L(cur)->sem.rec_arg.base;
                cur = R(cur);
            }

            /* call the path traversal worker, that introduces 
               the border operator and marks all 'inside' nodes, 
               for all recursion arguments as well as the result */
            cur = L(n);
            while (cur->kind != pa_nil) {
                introduce_rec_borders_worker (LR(cur), bases);
                cur = R(cur);
            }
            introduce_rec_borders_worker (R(n), bases);

            /* Remove the 'in' flag for all nodes */
            cur = L(n);
            while (cur->kind != pa_nil) {
                in_reset (LR(cur));
                cur = R(cur);
            }
            in_reset (R(n));
        } break;

        default:
            for (unsigned int i = 0; i < PFPA_OP_MAXCHILD && n->child[i]; i++)
                introduce_rec_borders (n->child[i]);
            break;
    }
}

/**
 * Introduce boundary operators for every recursion
 * such that the MIL generation detects expressions
 * that are invariant to the recursion body.
 */
PFpa_op_t *
PFpa_intro_rec_borders (PFpa_op_t *n)
{
    introduce_rec_borders (n);
    PFpa_dag_reset (n);
    return n;
}
