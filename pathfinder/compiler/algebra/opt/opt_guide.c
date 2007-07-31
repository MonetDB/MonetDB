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
static void
merge_guide_steps (PFla_op_t *n)
{
    PFla_op_t *step1, *step2, *proj = NULL;
    PFalg_att_t item;
    PFalg_axis_t new_axis;

    assert(n);
    assert (n->kind == la_step ||
            n->kind == la_step_join ||
            n->kind == la_guide_step ||
            n->kind == la_guide_step_join);
    
    step1 = n;
    step2 = R(n);
    item = n->sem.step.item;

    if (step2->kind == la_project) {
        proj = step2;
        step2 = L(proj);
        for (unsigned int i = 0; i < proj->sem.proj.count; i++)
            if (proj->sem.proj.items[i].new == item) {
                item = proj->sem.proj.items[i].old;
                break;
            }
    }
        
    /* do not merge if we have no adjacent steps */
    if (step2->kind != la_step &&
        step2->kind != la_step_join &&
        step2->kind != la_guide_step &&
        step2->kind != la_guide_step_join)
        return;

    /* do not merge if step1 does not work
       on the result of step2 */
    if (item != step2->sem.step.item_res)
        return;

    /* check if axes can be merged */
    if (!((AXIS(step1) == alg_self || AXIS(step1) == alg_chld || 
           AXIS(step1) == alg_desc || AXIS(step1) == alg_desc_s) &&
          (AXIS(step2) == alg_self || AXIS(step2) == alg_chld || 
           AXIS(step2) == alg_desc || AXIS(step2) == alg_desc_s))) 
 
        if (!((AXIS(step1) == alg_self || AXIS(step1) == alg_par || 
               AXIS(step1) == alg_anc || AXIS(step1) == alg_anc_s) &&
              (AXIS(step2) == alg_self || AXIS(step2) == alg_par || 
               AXIS(step2) == alg_anc || AXIS(step2) == alg_anc_s))) 
            return;
    
    /* try to merge the axes */
    
    /* self axis */
    if (AXIS(step1) == alg_self)
        switch (AXIS(step2)) {
            case alg_self:
            case alg_chld:
            case alg_desc:
            case alg_desc_s:
            case alg_par:
            case alg_anc:
            case alg_anc_s:
                new_axis = AXIS(step2);
                break;
            default:
                return;
        }
    else if (AXIS(step2) == alg_self)
        switch(AXIS(step1)) {
            case alg_self:
            case alg_chld:
            case alg_desc:
            case alg_desc_s:
            case alg_par:
            case alg_anc:
            case alg_anc_s:
                new_axis = AXIS(step1);
                break;
            default:
                return;
        }
    else if (AXIS(step1) == alg_chld || AXIS(step2) == alg_chld ||
             AXIS(step1) == alg_desc || AXIS(step2) == alg_desc)
        /* child and desc axis -> new_axis = desc */
        new_axis = alg_desc;
    else if (AXIS(step1) == alg_par || AXIS(step2) == alg_par || 
             AXIS(step1) == alg_anc || AXIS(step2) == alg_anc)
        /* parent and anc axis -> new axis = desc */
        new_axis = alg_anc;
    else if (AXIS(step1) == AXIS(step2))
        /* if both axis are equal */
        new_axis = AXIS(step1);
    else
        return;

    if (proj) {
        unsigned int count = 0;
        PFalg_proj_t *proj_list;

        proj_list = PFmalloc (proj->sem.proj.count *
                              sizeof (*(proj_list)));
                              
        for (unsigned int i = 0; i < proj->sem.proj.count; i++)
            if (proj->sem.proj.items[i].old == 
                step2->sem.step.item_res) {
                proj_list[i] = PFalg_proj (
                                   proj->sem.proj.items[i].new,
                                   step2->sem.step.item);
                count++;
            } else
                proj_list[i] = proj->sem.proj.items[i];

        /* do not rewrite if the result of step2
           is referenced multiple times */
        if (count > 1) return;
        
        R(step1) = PFla_project_ (R(step2), proj->sem.proj.count, proj_list);
    } else if (step2->kind == la_step_join ||
               step2->kind == la_guide_step_join) {
        PFalg_proj_t *proj_list;

        proj_list = PFmalloc (step2->schema.count *
                              sizeof (*(proj_list)));
                              
        for (unsigned int i = 0; i < step2->schema.count; i++)
            if (step2->schema.items[i].name == 
                step2->sem.step.item_res)
                proj_list[i] = PFalg_proj (
                                   step2->sem.step.item_res,
                                   step2->sem.step.item);
            else
                proj_list[i] = PFalg_proj (
                                   step2->schema.items[i].name,
                                   step2->schema.items[i].name);

        R(step1) = PFla_project_ (R(step2), step2->schema.count, proj_list);
    } else
        R(step1) = R(step2);
    
    AXIS(step1) = new_axis;
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
        case la_step_join:
	{
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
            if (count == 0) {
                ret = PFla_empty_tbl_ (n->schema);
            } else {
                /* get guide nodes */
                guides = PFprop_guide_elements(PROP(n), column);
                /* create new step operator */
                if (n->kind == la_step) {
                    ret = PFla_guide_step (
                              L(n), R(n), n->sem.step.axis, 
                              n->sem.step.ty, count, guides,
                              n->sem.step.level, 
                              n->sem.step.iter, n->sem.step.item, 
                              n->sem.step.item_res);

                    merge_guide_steps(ret);
                } else {
                    ret = PFla_guide_step_join (
                              L(n), R(n), n->sem.step.axis,
                              n->sem.step.ty, count, guides,
                              n->sem.step.level,
                              n->sem.step.item, 
                              n->sem.step.item_res);

                    if (PFprop_set (n->prop) &&
                        !PFprop_icol (n->prop, n->sem.step.item))
                        merge_guide_steps(ret);
                }
            }
        
            *n = *ret;
            SEEN(n) = true;
        }   break;

        case la_guide_step:
            merge_guide_steps (n);
            break;
            
        case la_guide_step_join:
            if (PFprop_set (n->prop) &&
                !PFprop_icol (n->prop, n->sem.step.item))
                merge_guide_steps (n);
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
    assert(guide);

    PFprop_infer_set (root);
    PFprop_infer_icol (root);
    PFprop_infer_guide (root, guide);

    /* Optimize algebra tree */
    opt_guide (root);
    PFla_dag_reset (root);

    return root;
}
