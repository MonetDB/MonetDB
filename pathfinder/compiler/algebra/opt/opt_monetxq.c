/**
 * @file
 *
 * Modifications to the relational algebra expression DAG
 * that produce a better code generation for MonetDB/XQuery.
 *
 * These modifications include adding distinct operators to
 * minimize intermediate result sizes as well as path step
 * rewrites that are benefitial for MonetDB/XQuery.
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
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2010 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id$
 */

/* always include pf_config.h first! */
#include "pf_config.h"
#include "pathfinder.h"
#include <assert.h>
#include <stdio.h>

#include "algopt.h"
#include "properties.h"
#include "alg_dag.h"
#include "mem.h"          /* PFmalloc() */
#include "oops.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

/** mnemonic algebra constructors */
#include "logical_mnemonic.h"

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

#define SEEN(p) ((p)->bit_dag)

/* worker for PFopt_monetxq */
static void
opt_monetxq (PFla_op_t *p)
{
    assert(p);

    /* nothing to do if we already visited that node */
    if (SEEN(p))
        return;
    /* otherwise mark the node */
    else
        SEEN(p) = true;

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_monetxq (p->child[i]);

    /* action code */
    switch (p->kind) {
        case la_project:
            /* merge adjacent projection operators */
            if (L(p)->kind == la_project)
                *p = *PFla_project_ (LL(p),
                                     p->schema.count,
                                     PFalg_proj_merge (
                                         p->sem.proj.items,
                                         p->sem.proj.count,
                                         L(p)->sem.proj.items,
                                         L(p)->sem.proj.count));
            break;

        case la_step_join:
            /* switch the order of steps as MonetDB/XQuery
               (as Peter says:) always benefits from it
            
               This rewrite would not be benficial if the
               document lookup appears inside a for loop.
               The witdth of the projection column enforces
               that the result of the first step is the only
               visible column and thus that the pattern is
               not nested in a for loop. */
            if (PFprop_set (p->prop) &&
                PFprop_not_icol (p->prop, p->sem.step.item_res) &&
                p->sem.step.spec.axis == alg_chld &&
                p->sem.step.spec.kind != node_kind_node &&
                p->sem.step.spec.kind != node_kind_doc) {
                PFla_op_t  *op = R(p);
                PFalg_col_t item = p->sem.step.item;

                /* check for a possible projection in between */
                if (op->kind == la_project &&
                    op->schema.count == 1 &&
                    op->sem.proj.items[0].new == item) {
                    item = op->sem.proj.items[0].old;
                    op = L(op);
                }

                /* check for a '//element(*)' step */
                if (op->kind == la_step_join &&
                    op->sem.step.item_res == item &&
                    (op->sem.step.spec.axis == alg_desc ||
                     op->sem.step.spec.axis == alg_desc_s) &&
                    ((op->sem.step.spec.kind == node_kind_elem &&
                      PFqname_ns_wildcard (op->sem.step.spec.qname) &&
                      PFqname_loc_wildcard (op->sem.step.spec.qname)) ||
                     op->sem.step.spec.kind == node_kind_node) &&
                    R(op)->kind == la_roots &&
                    RL(op)->kind == la_doc_tbl &&
                    op->sem.step.item == RL(op)->sem.doc_tbl.res) {
                    /* copy the step specification and change the axes */
                    PFalg_step_spec_t spec_desc = p->sem.step.spec,
                                      spec_par  = op->sem.step.spec;
                    spec_desc.axis = alg_desc;
                    spec_par.axis  = alg_par;

                    if (R(p)->kind == la_project) {
                        /* new name for the lower path step
                           to avoid name conflicts */
                        PFalg_col_t col = PFcol_new (p->sem.step.item_res);
                        *p = *step_join (L(p),
                                         project (
                                             step_join (L(p),
                                                        R(op),
                                                        spec_desc,
                                                        p->sem.step.level,
                                                        RL(op)->sem.doc_tbl.res,
                                                        col),
                                             proj (p->sem.step.item_res, col)),
                                         spec_par,
                                         op->sem.step.level,
                                         p->sem.step.item_res,
                                         p->sem.step.item);
                    }
                    else
                        *p = *step_join (L(p),
                                         step_join (L(p),
                                                    R(op),
                                                    spec_desc,
                                                    p->sem.step.level,
                                                    RL(op)->sem.doc_tbl.res,
                                                    p->sem.step.item_res),
                                         spec_par,
                                         op->sem.step.level,
                                         p->sem.step.item_res,
                                         p->sem.step.item);
                    break;
                }
            }

            /* only introduce a distinct on top of the path step
               if the result is not used anymore */
            if (PFprop_icol (p->prop, p->sem.step.item_res))
                break;
            /* fall through */
        case la_thetajoin:
        case la_select:
        /* case la_pos_select: */
        case la_doc_index_join:
            /**
             * For all operators that prune rows we try to add a
             * distinct operator on top that removes duplicates.
             * This rewrite hopefully keeps intermediate result
             * sizes small.
             *
             * An operator p gets copied and two projections
             * and a distinct operator are placed on top:
             *
             *         |
             *      project_(full schema with dummy entries)
             *         |
             *      distinct
             *         |
             *      project_(icols)
             *         |
             *         p
             *         |
             */
            if (PFprop_set (p->prop) && PFprop_icols_count(p->prop)) {
                /* look up required columns (icols) */
                PFalg_collist_t *icols = PFprop_icols_to_collist (p->prop);
                PFalg_proj_t    *proj1 = PFmalloc (clsize (icols) *
                                                   sizeof (PFalg_proj_t)),
                                *proj2 = PFmalloc (p->schema.count *
                                                   sizeof (PFalg_proj_t));

                /* fill the projection list of the lower projection (proj1) */
                for (unsigned int i = 0; i < clsize (icols); i++)
                    proj1[i] = PFalg_proj (clat (icols, i), clat (icols, i));

                /* fill the projection list of the upper projection (proj2) */
                for (unsigned int i = 0; i < p->schema.count; i++) {
                    PFalg_col_t cur = p->schema.items[i].name;
                    if (PFprop_icol (p->prop, cur))
                        proj2[i] = PFalg_proj (cur, cur);
                    else
                        /* replace missing column by dummy reference */
                        proj2[i] = PFalg_proj (cur, clat (icols, 0));
                }

                /* Place a distinct operator on top of the operator.
                   The projection below the distinct operator removes
                   unreferenced columns and the distinct on top fills
                   the missing column references with a dummy column. */
                *p = *PFla_project_ (
                          distinct (
                              PFla_project_ (
                                  PFla_op_duplicate (p, L(p), R(p)),
                                  clsize (icols),
                                  proj1)),
                          p->schema.count,
                          proj2);
            }
            break;

        case la_difference:
            /* remove distinct operators again whose output
               is only consumed by the right side of a difference
               operator */
            if (p->schema.count == 1 &&
                /* PROJECTION IN PATTERN */
                R(p)->kind == la_project &&
                RL(p)->kind == la_distinct)
                RL(p) = RLL(p);
            break;

        case la_attach:
            /* If the attach column is the only result it might
               be benefitial to return only a single row
               (the attach column without duplicates). */
            if (PFprop_set (p->prop) &&
                PFprop_icols_count (p->prop) == 1 &&
                PFprop_icol (p->prop, p->sem.attach.res)) {
                PFalg_col_t   col  = p->sem.attach.res;
                PFalg_proj_t *proj = PFmalloc (p->schema.count *
                                               sizeof (PFalg_proj_t));

                /* fill the projection list of the upper projection (proj) */
                for (unsigned int i = 0; i < p->schema.count; i++)
                    proj[i] = PFalg_proj (p->schema.items[i].name, col);

                *p = *PFla_project_ (
                          PFla_distinct (
                              PFla_project (
                                  PFla_attach (L(p), col, p->sem.attach.value),
                                  PFalg_proj (col, col))),
                          p->schema.count,
                          proj);
            }
            break;

        default:
            break;
    }
}

/**
 * Introduce additional projection operators
 * (based in the available icols information)
 * thus reducing the schema width.
 */
static void 
opt_monetprojections (PFla_op_t *p)
{
    assert(p);
    
    /* nothing to do if we already visited that node */
    if (SEEN(p))
        return;
    /* otherwise mark the node */
    else
        SEEN(p) = true;

     /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_monetprojections (p->child[i]);

     /* action code */
    switch (p->kind) {
    
        /* adding projection_(icols) over operator p to restrict the 
         * schema width
         *                   |
         *     |          project_(icols)
         *     p   -->       |
         *     |             p
         *                   |
         */
        case la_attach:
        case la_cross:
        case la_eqjoin:
        case la_thetajoin:
        case la_select:
        case la_pos_select:
        case la_fun_1to1:
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_bool_not:
        case la_to:
        case la_rownum:
        case la_rowrank:
        case la_rank:
        case la_rowid:
        case la_type: 
        case la_cast:
        case la_step_join:
        case la_guide_step_join:
        case la_doc_index_join:
        case la_doc_access:
        case la_roots:
        
            /* add projection only if there are icols for the current 
               operator  */
            if (PFprop_icols_count(p->prop)) {
                /* get the icols as collist */
                PFalg_collist_t *icols = PFprop_icols_to_collist (p->prop);
                /* allocate projection list with size of icol collist */
                PFalg_proj_t    *proj = PFmalloc (clsize (icols) *
                                                    sizeof (PFalg_proj_t));
                                                    
                /* fill the projection list with icols */
                for (unsigned int i = 0; i < clsize (icols); i++)
                    proj[i] = PFalg_proj (clat (icols, i), clat (icols, i));
            
                /* Place new projection operator on top of current operator
                   duplicating current opperator */
                *p = *PFla_project_ (
                            PFla_op_duplicate (p, L(p), R(p)),
                            clsize (icols),
                            proj);
                /* mark the new projection node as SEEN */
                SEEN(p) = true;
            }
            break;
        
        default:
            break;
    }
}

/**
 * MonetDB/XQuery specific optimizations:
 *  - Introduce additional distinct operators.
 *  - Rewrite //_[child::a] steps into //a/parent::_ steps
 *  - Introduce additional projection operators (to reduce the schema width).
 */
PFla_op_t *
PFalgopt_monetxq (PFla_op_t *root)
{
    /* infer set and icols properties */
    PFprop_infer_set (root);
    PFprop_infer_icol (root);

    opt_monetxq (root);

    PFla_dag_reset (root);

    /* In addition optimize the resulting DAG using the icols property
       to remove inconsistencies introduced by changing the types
       of unreferenced columns (rule eqjoin). The icols optimization
       will ensure that these columns are 'really' never used. */
    root = PFalgopt_icol (root);
    
    PFprop_infer_icol (root);

    /* In a second traversal introduce projections where possible
       to restrict the schema width. (This leads to better physical
       path step planning and reduces the complexity of the enumeration
       process in the planner.) */
    opt_monetprojections (root);

    PFla_dag_reset (root);
    
    /* In addition optimize the resulting DAG using the icols property
       to remove schema inconsistencies.  The icols optimization
       will ensure that unreferenced columns are 'really' never used. */
    root = PFalgopt_icol (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
