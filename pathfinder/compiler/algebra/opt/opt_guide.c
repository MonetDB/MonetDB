/**
 * @file
 *
 * Optimize relational algebra expression DAG 
 *  based on guide nodes.
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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#include "pathfinder.h"
#include <assert.h>

#include "algopt.h"
#include "properties.h"
#include "alg_dag.h"

/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])

#define SEEN(p) ((p)->bit_dag)

/* worker for PFalgopt_guide */
static void
opt_guide (PFla_op_t *p)
{
    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply guide-related optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_guide (p->child[i]);

    /* action code */
    switch (p->kind) {
        case la_step:
            if (PFprop_guide (p->prop, p->sem.step.item_res)) {
                if (PFprop_guide_count (p->prop, p->sem.step.item_res))
                    *p = *PFla_guide_step (
                              L(p),
                              R(p),
                              p->sem.step.axis,
                              p->sem.step.ty,
                              PFprop_guide_count (
                                  p->prop,
                                  p->sem.step.item_res),
                              PFprop_guide_elements (
                                  p->prop,
                                  p->sem.step.item_res),
                              p->sem.step.level,
                              p->sem.step.iter,
                              p->sem.step.item,
                              p->sem.step.item_res);
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
PFla_op_t* 
PFalgopt_guide(PFla_op_t *root, PFguide_tree_t *guide)
{

    PFprop_infer_guide(root, guide);

    /* Optimize algebra tree */
    opt_guide(root);
    PFla_dag_reset(root);

    return root;
}
