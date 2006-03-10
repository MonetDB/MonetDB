/**
 * @file
 *
 * Optimize relational algebra expression DAG
 * based on multiple properties.
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
 * The Initial Developer of the Original Code is the Database &
 * Information Systems Group at the University of Konstanz, Germany.
 * Portions created by the University of Konstanz are Copyright (C)
 * 2000-2005 University of Konstanz.  All Rights Reserved.
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

/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])

#define SEEN(p) ((p)->bit_dag)

/* worker for PFalgopt_complex */
static void
opt_complex (PFla_op_t *p)
{
    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply complex optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_complex (p->child[i]);

    /* action code */
    switch (p->kind) {
        case la_eqjoin:
            /**
             * if we have a key join (key property) on a 
             * domain-subdomain relationship (domain property)
             * where the columns of the argument marked as 'domain'
             * are not required (icol property) we can skip the join
             * completely.
             */
            if (PFprop_key (p->prop, p->sem.eqjoin.att1) &&
                PFprop_key (p->prop, p->sem.eqjoin.att2)) {
                /* we can use the schema information of the children
                   as no rewrite adds more columns to that subtree. */
                bool left_arg_req = false;
                bool right_arg_req = false;
                for (unsigned int i = 0; i < L(p)->schema.count; i++) {
                    left_arg_req = left_arg_req ||
                                   PFprop_icol (
                                       p->prop, 
                                       L(p)->schema.items[i].name);
                }
                if (PFprop_subdom (p->prop, 
                                   PFprop_dom_right (p->prop,
                                                     p->sem.eqjoin.att2),
                                   PFprop_dom_left (p->prop,
                                                    p->sem.eqjoin.att1)) &&
                    !left_arg_req) {
                    *p = *(R(p));
                    break;
                }
                
                for (unsigned int i = 0; i < R(p)->schema.count; i++) {
                    right_arg_req = right_arg_req ||
                                    PFprop_icol (
                                        p->prop, 
                                        R(p)->schema.items[i].name);
                }
                if (PFprop_subdom (p->prop, 
                                   PFprop_dom_left (p->prop,
                                                    p->sem.eqjoin.att1),
                                   PFprop_dom_right (p->prop,
                                                     p->sem.eqjoin.att2)) &&
                    !right_arg_req) {
                    *p = *(L(p));
                    break;
                }
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
PFalgopt_complex (PFla_op_t *root)
{
    /* Infer key, icols, and domain properties first */
    PFprop_infer_key (root);
    PFprop_infer_icol (root);
    PFprop_infer_dom (root);

    /* Optimize algebra tree */
    opt_complex (root);
    PFla_dag_reset (root);

    /* Infer ocol properties as the rewrite
       rules introduced inconsistencies */
    PFprop_infer_ocol (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
