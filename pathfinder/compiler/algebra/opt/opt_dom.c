/**
 * @file
 *
 * Optimize relational algebra expression DAG
 * based on the domain property.
 * (This requires no burg pattern matching as we
 *  apply optimizations in a peep-hole style on
 *  single nodes only.)
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

/* always include pathfinder.h first! */
#include "pathfinder.h"
#include <assert.h>
#include <stdio.h>

#include "algopt.h"
#include "properties.h"
#include "alg_dag.h"
#include "mem.h"          /* PFmalloc() */

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define SEEN(p) ((p)->bit_dag)

/* worker for PFalgopt_dom */
static void
opt_dom (PFla_op_t *p)
{
    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply domain optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_dom (p->child[i]);

    /* action code */
    switch (p->kind) {
        case la_difference:
        {   /**
             * If the domains of the first relation are all subdomains
             * of the corresponding domains in the second argument
             * the result of the difference operation will be empty.
             */
            unsigned int all_subdom = 0;
            for (unsigned int i = 0; i < L(p)->schema.count; i++)
                for (unsigned int j = 0; j < R(p)->schema.count; j++)
                    if (L(p)->schema.items[i].name ==
                        R(p)->schema.items[j].name &&
                        PFprop_subdom (
                            p->prop,
                            PFprop_dom_left (p->prop,
                                             L(p)->schema.items[i].name),
                            PFprop_dom_right (p->prop,
                                              R(p)->schema.items[j].name))) {
                        all_subdom++;
                        break;
                    }

            if (all_subdom == p->schema.count) {
                *p = *PFla_empty_tbl_ (p->schema);
                SEEN(p) = true;
                break;
            }
        }   break;

        case la_semijoin:
            /* if the semijoin operator does not prune a row
               because the domains are identical we can safely
               remove it. */
            if (PFprop_subdom (
                    p->prop,
                    PFprop_dom_left (p->prop,
                                     p->sem.eqjoin.att1),
                    PFprop_dom_right (p->prop,
                                      p->sem.eqjoin.att2)) &&
                PFprop_subdom (
                    p->prop,
                    PFprop_dom_right (p->prop,
                                      p->sem.eqjoin.att2),
                    PFprop_dom_left (p->prop,
                                     p->sem.eqjoin.att1))) {
                *p = *L(p);
                break;
            }
            break;

        default:
            break;
    }
}

/**
 * Invoke algebra optimization.
 */
PFla_op_t *
PFalgopt_dom (PFla_op_t *root)
{
    /* Infer domain properties first */
    PFprop_infer_dom (root);

    /* Optimize algebra tree */
    opt_dom (root);
    PFla_dag_reset (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
