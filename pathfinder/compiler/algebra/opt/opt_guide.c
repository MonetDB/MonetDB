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
#include <stdio.h> 

#include "algopt.h"
#include "properties.h"
#include "alg_dag.h"

#define SEEN(n) ((n)->bit_dag)  
/* prop of n */
#define PROP(n) ((n)->prop) 
/* axis of n, n must be a step */
#define AXIS(n) ((n)->sem.step.axis)
/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])
/** starting from p, make two steps right */
#define RR(p) (((p)->child[1])->child[1])

/* Merge 2 guide_steps if it is possible */
static void merge_guide_steps(PFla_op_t *n);

/* worker for PFalgopt_guide */
static void opt_guide(PFla_op_t *n);


/* Merge 2 guide_steps if it is possible */
static void
merge_guide_steps(PFla_op_t *n)
{
    assert(n);

    /* apply chances for step operators */
    if(n->kind == la_guide_step) {
        assert(PROP(n));
        assert(R(n));
        /* right child has guide_step -> delete it */
        if(R(n)->kind == la_guide_step) {

            bool merge_steps = false;
            PFalg_axis_t new_axis;

            assert(RR(n));

            /* check if axis can be merged */
            if(!((AXIS(n) == alg_self || AXIS(n) == alg_chld || 
                    AXIS(n) == alg_desc || AXIS(n) == alg_desc_s) &&
                (AXIS(R(n)) == alg_self || AXIS(R(n)) == alg_chld || 
                    AXIS(R(n)) == alg_desc || AXIS(R(n)) == alg_desc_s))) 
         
                if(!((AXIS(n) == alg_self || AXIS(n) == alg_par || 
                        AXIS(n) == alg_anc || AXIS(n) == alg_anc_s) &&
                    (AXIS(R(n)) == alg_self || AXIS(R(n)) == alg_par || 
                        AXIS(R(n)) == alg_anc || AXIS(R(n)) == alg_anc_s))) 
                    return;
            
            /* self axis */
            if(AXIS(n) == alg_self) {
                switch(AXIS(R(n))) {
                    case alg_self:
                    case alg_chld:
                    case alg_desc:
                    case alg_desc_s:
                    case alg_par:
                    case alg_anc:
                    case alg_anc_s:
                        new_axis = AXIS(R(n));
                        merge_steps = true;
                        break;
                    default:
                        return;
                }
            } else
            if(AXIS(R(n)) == alg_self) {
                switch(AXIS(n)) {
                    case alg_self:
                    case alg_chld:
                    case alg_desc:
                    case alg_desc_s:
                    case alg_par:
                    case alg_anc:
                    case alg_anc_s:
                        new_axis = AXIS(n);
                        merge_steps = true;
                        break;
                    default:
                        return;
                }
            } else           
            /* child and desc axis -> new_axis = desc */
            if(AXIS(n) == alg_chld || AXIS(R(n)) == alg_chld || 
                    AXIS(n) == alg_desc || AXIS(R(n)) == alg_desc) {
                new_axis = alg_desc;
                merge_steps = true;
            } else
            /* parent and anc axis -> new axis = desc */
            if(AXIS(n) == alg_par || AXIS(R(n)) == alg_par || 
                    AXIS(n) == alg_anc || AXIS(R(n)) == alg_anc) { 
                new_axis = alg_anc;
                merge_steps = true;
            } else
            /* if both axis are equal */
            if(AXIS(n) == AXIS(R(n))) {
                new_axis = AXIS(n);
                merge_steps = true;
            }

            if(merge_steps == false)
                return;

            AXIS(n) = new_axis;
            R(n) = RR(n);
            return;
        }
    }
    return;
}

/* worker for PFalgopt_guide */
static void 
opt_guide(PFla_op_t *n)
{
    assert(n);

    /* rewrite each node only once */
    if(SEEN(n))
        return;
    else
        SEEN(n) = true;

    /* apply guide-related optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        opt_guide (n->child[i]);

    /* apply chances for step operators */
    switch (n->kind) {
        case la_step:
            assert(PROP(n));
            assert(L(n));
            assert(R(n));
    
            PFla_op_t *ret = NULL;  /* new guide_step operator */
            unsigned int count = 0; /* # of guide nodes */
            PFguide_tree_t** guides = NULL; /* array of guide nodes */
            PFalg_att_t column = n->sem.step.item_res;
    
            /* look if operator has guide nodes */
            if(PFprop_guide(PROP(n), column) == false)
                break; 
    
            /* # of guide nodes */
            count = PFprop_guide_count(PROP(n), column);
            /* guide list is empty -> create empty table*/
            if(count == 0) {
                ret = PFla_empty_tbl_ (n->schema);
            } else {
                /* get guide nodes */
                guides = PFprop_guide_elements(PROP(n), column);
                /* create new step operator */
                ret = PFla_guide_step(L(n), R(n), n->sem.step.axis, 
                            n->sem.step.ty, count, guides, n->sem.step.level, 
                            n->sem.step.iter, n->sem.step.item, 
                            n->sem.step.item_res);
    
            }
        
            *n = *ret;
            SEEN(n) = true;
            break;
        default:
            break;
    }
    
    
    /* merge 2 guide_step operator if possible */
    merge_guide_steps(n);

    return;
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
