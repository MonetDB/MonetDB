/**
 * @file
 *
 * This file contains optimizations that make the detection
 * of join graphs much easier. The idea is to use less but more generic
 * distinct operators (that e.g., remove duplicates of multiple nested
 * path steps in one go instead of a single distinct after each step).
 * Furthermore we try to replace all step operators by step_join operators
 * that can handle more input columns (than just iter|item) which should
 * ultimatively lead to less mapping joins and blocking number operators.
 *
 * Because the optimizations introduced here might hurt the efficiency
 * of the generated MIL code we used a separate optimization phase that
 * only the SQL generation (which can handle join graphs) calls by default.
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
#define LL(p) (L(L(p)))
#define RL(p) (L(R(p)))
#define LR(p) (R(L(p)))

#define SEEN(p) ((p)->bit_dag)

/* worker for PFalgopt_join_graph */
static void
opt_join_graph (PFla_op_t *p)
{
    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply join-graph specific optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_join_graph (p->child[i]);

    /* action code */
    switch (p->kind) {
        /* Replace semijoin operators by normal equi-joins. (A following
           optimization phase might then try to remove the equi-joins.) */
        case la_semijoin:
            if (PFprop_set (p->prop)) {
                /* we need to additional projections:
                   top    to remove the right join argument from the result list
                   right  to avoid name conflicts with the left input columns
                          and to ensure that distinct is applied 
                          only on the join column */
                PFalg_proj_t *top   = PFmalloc (p->schema.count *
                                                sizeof (PFalg_proj_t));
                PFalg_att_t   used_cols = 0,
                              new_col = p->sem.eqjoin.att2,
                              cur_col;

                /* fill the 'top' projection and collect all used columns
                   of the left input relation (equals the semijoin output
                   schema) */
                for (unsigned int i = 0; i < p->schema.count; i++) {
                    cur_col = p->schema.items[i].name;
                    
                    top[i] = PFalg_proj (cur_col, cur_col);
                    used_cols |= cur_col;
                }
                /* rename the join argument in case a name conflict occurs */
                if (used_cols & new_col)
                    new_col = PFalg_ori_name (
                                  PFalg_unq_name (new_col, 0),
                                  ~used_cols);
                
                /* replace the semijoin */
                *p = *PFla_project_ (
                          PFla_eqjoin (L(p),
                                       /* project away all columns except
                                          for the join column */
                                       PFla_project (
                                           R(p),
                                           PFalg_proj (
                                               new_col,
                                               p->sem.eqjoin.att2)),
                                       p->sem.eqjoin.att1,
                                       new_col),
                          p->schema.count,
                          top);
            }
            break;

        /* Remove unnecessary distinct operators and narrow the schema
           of a distinct operator if some columns are not required 
           while the remaining columns still provide a composite key. */
        case la_distinct:
            if (PFprop_set (p->prop))
                *p = *PFla_dummy (L(p));
            else {
                PFalg_schema_t schema;
                schema.count = 0;
                schema.items = PFmalloc (p->schema.count *
                                         sizeof (PFalg_schema_t));
                
                for (unsigned int i = 0; i < p->schema.count; i++)
                    if (PFprop_icol (p->prop, p->schema.items[i].name))
                        schema.items[schema.count++].name
                            = p->schema.items[i].name;

                /* with compound keys we can now test whether we change
                   the semantics if we introduce a projection that narrows
                   the schema. */
                if (PFprop_ckey (p->prop, schema)) {
                    PFalg_proj_t *proj_list;

                    /* create projection list */
                    proj_list = PFmalloc (schema.count *
                                          sizeof (*(proj_list)));
                                          
                    for (unsigned int i = 0; i < schema.count; i++)
                        proj_list[i] = PFalg_proj (schema.items[i].name,
                                                   schema.items[i].name);

                    *p = *PFla_distinct (
                              PFla_project_ (
                                  L(p),
                                  schema.count,
                                  proj_list));
                }
            }
            break;
        
        /* Use a rank operator with multiple sort criteria as the place
           where a new distinct operator based on the composite key
           property is introduced. (Rank operators with multiple sort
           criteria are only introduced during back-mapping -- after
           a for-loop body has been processed. Nested for-loops that
           return path steps thus will always benefit from a new distinct
           operator as nesting and the correct duplicates are treated
           by the new distinct operator.) */
        case la_rank:
            if (PFord_count (p->sem.rank.sortby) > 1 &&
                !PFprop_set (p->prop) && /* ignore nested rank operators */
                L(p)->kind != la_distinct) /* introduce distinct only once */ {
                PFalg_att_t col;
                PFalg_schema_t schema;
                schema.count = 0;
                schema.items = PFmalloc ((p->schema.count - 1) *
                                         sizeof (PFalg_schema_t));
                /* Create a new schema without result column... */
                for (unsigned int i = 0; i < p->schema.count; i++) {
                    col = p->schema.items[i].name;
                    if (col != p->sem.rank.res)
                        schema.items[schema.count++].name = col;
                }
                /* ... and use it to check if it contains a composite key. */
                if (PFprop_ckey (p->prop, schema) > 1) 
                    L(p) = PFla_distinct (L(p));
            }
            break;
            
        /* Replace number generated columns by already existing
           key columns.
           !!!This means that iter columns with type aat_nat
              are replaced by columns of arbitrary type!!! */
        case la_number:
        {    
            PFalg_att_t key = att_NULL;
            
            for (unsigned int i = 0; i < p->schema.count; i++)
                if (p->sem.number.res != p->schema.items[i].name &&
                    PFprop_key (p->prop, p->schema.items[i].name))
                    key = p->schema.items[i].name;
           
            if (key) {
                PFalg_proj_t *proj_list;

                /* create projection list */
                proj_list = PFmalloc (p->schema.count *
                                      sizeof (*(proj_list)));
                                      
                for (unsigned int i = 0; i < p->schema.count; i++)
                    if (p->sem.number.res != p->schema.items[i].name)
                        proj_list[i] = PFalg_proj (p->schema.items[i].name,
                                                   p->schema.items[i].name);
                    else
                        proj_list[i] = PFalg_proj (p->sem.number.res, key);

                *p = *PFla_project_ (L(p), p->schema.count, proj_list);
                break;
            }
        }   break;
            
        /* Replace all step operators by step_join operators
           to allow a following optimization phase to get rid
           of unnecessary eqjoin and number operators */
        case la_step:
            if ((PFprop_set (p->prop)) ||
                    
                (PFprop_key_right (p->prop, p->sem.step.item) &&
                 PFprop_level_right (p->prop, p->sem.step.item) >= 0 &&
                 (p->sem.step.axis == alg_attr ||
                  p->sem.step.axis == alg_chld ||
                  p->sem.step.axis == alg_self ||
                  p->sem.step.axis == alg_desc ||
                  p->sem.step.axis == alg_desc_s))) {
                
                PFalg_att_t item_res;
                item_res = PFalg_ori_name (
                               PFalg_unq_name (att_item, 0),
                               ~(p->sem.step.item |
                                 p->sem.step.iter));

                *p = *PFla_project (
                             PFla_step_join (
                                 L(p),
                                 R(p),
                                 p->sem.step.axis,
                                 p->sem.step.ty,
                                 p->sem.step.level,
                                 p->sem.step.item,
                                 item_res),
                             PFalg_proj (p->sem.step.iter,
                                         p->sem.step.iter),
                             PFalg_proj (p->sem.step.item_res,
                                         item_res));
                break;
            }
            break;
            
        /* Replace all guide_step operators by guide_step_join
           operators to allow a following optimization phase
           to get rid of unnecessary eqjoin and number operators */
        case la_guide_step:
            if ((PFprop_set (p->prop)) ||
                    
                (PFprop_key_right (p->prop, p->sem.step.item) &&
                 PFprop_level_right (p->prop, p->sem.step.item) >= 0 &&
                 (p->sem.step.axis == alg_attr ||
                  p->sem.step.axis == alg_chld ||
                  p->sem.step.axis == alg_self ||
                  p->sem.step.axis == alg_desc ||
                  p->sem.step.axis == alg_desc_s))) {
                
                PFalg_att_t item_res;
                item_res = PFalg_ori_name (
                               PFalg_unq_name (att_item, 0),
                               ~(p->sem.step.item |
                                 p->sem.step.iter));
                
                *p = *PFla_project (
                             PFla_guide_step_join (
                                 L(p),
                                 R(p),
                                 p->sem.step.axis,
                                 p->sem.step.ty,
                                 p->sem.step.guide_count,
                                 p->sem.step.guides,
                                 p->sem.step.level,
                                 p->sem.step.item,
                                 item_res),
                             PFalg_proj (p->sem.step.iter,
                                         p->sem.step.iter),
                             PFalg_proj (p->sem.step.item_res,
                                         item_res));
                break;
            }
            break;
            
        /* throw away proxy nodes and thus avoid calling option '-o {'
           which would remove all step_joins as well. */
        case la_proxy:
        case la_proxy_base:
            *p = *PFla_dummy (L(p));
            break;

        default:
            break;
    }
}

/**
 * Invoke algebra optimization.
 */
PFla_op_t *
PFalgopt_join_graph (PFla_op_t *root)
{
    /* Infer key, icols, set, and composite key
       properties first */
    PFprop_infer_composite_key (root);
    /* PFprop_infer_key (root); */
    PFprop_infer_icol (root);
    PFprop_infer_set (root);
    PFprop_infer_key (root);
    /* level is already inferred by key */
    /* PFprop_infer_level (root); */

    /* Optimize algebra tree */
    opt_join_graph (root);
    PFla_dag_reset (root);

    /* In addition optimize the resulting DAG using the icols property 
       to remove inconsistencies introduced by changing the types 
       of unreferenced columns (rule distinct). The icols optimization
       will ensure that these columns are 'really' never used. */
    root = PFalgopt_icol (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
