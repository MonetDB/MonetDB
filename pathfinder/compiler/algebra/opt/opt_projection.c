/**
 * @file
 *
 * This optimization phase removes as many projections as possible
 * from the relational algebra expression DAG by pushing projections
 * up in the plan and merging adjacent projections.
 *
 * 2009/05/09 (JR):
 * This optimization is currently used to simplify the query 
 * plans to detect more common subexpressions.
 *
 * It might affect rewrites in algebra/opt/opt_complex.c,
 * algebra/opt/opt_monetxq.c, and algebra/opt/opt_thetajoin.c.
 * The affected places are marked with 'PROJECTION IN PATTERN'.
 *
 * Note that algebra/opt/opt_thetajoin.c relies on the projection
 * operators introduced by rewrite phase } (proxy introduction).
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

#define new_name(c) PFcol_new((c))

/**
 * Look up the column name before renaming.
 */
static PFalg_col_t
get_old_name (PFla_op_t *op, PFalg_col_t col)
{
    assert (op->kind == la_project);

    for (unsigned int i = 0; i < op->sem.proj.count; i++ )
        /* return the 'old' name */
        if (col == op->sem.proj.items[i].new)
            return op->sem.proj.items[i].old;

    assert (0);
    return col;
}

/**
 * Create a new column list with 'old' column names.
 */
static PFalg_collist_t *
rename_col_in_collist (PFla_op_t *op, PFalg_collist_t *collist)
{
    PFalg_collist_t *cl = PFalg_collist_copy (collist);

    /* check for each column in the columnlist */
    for (unsigned int i = 0; i < clsize(cl); i++)
        clat (cl, i) = get_old_name (op, clat (cl, i));

    return cl;
}

/**
 * Create a new ordering list with 'old' column names.
 */
static PFord_ordering_t
rename_col_in_ordering (PFla_op_t *op, PFord_ordering_t ord)
{
    /* allocate new order list */
    PFord_ordering_t order = PFarray_copy (ord);

    /* check for each column in the ordering */
    for (unsigned int i = 0; i < PFord_count(order); i++)
        PFord_set_order_col_at (
            order,
            i,
            get_old_name (op, PFord_order_col_at (order, i)));

    return order;
}

/**
 * Create a new predicate list with 'old' column names.
 */
static PFalg_sel_t *
rename_col_in_pred (PFalg_sel_t *pred,
                    unsigned int pred_count,
                    PFla_op_t *lop,
                    PFla_op_t *rop)
{
    /* allocate new predicate array */
    PFalg_sel_t *predicates = PFmalloc (pred_count * sizeof (PFalg_sel_t));

    for (unsigned int i = 0; i < pred_count; i++) {
        /* duplicate predicate */
        predicates[i] = pred[i];

        /* left predicate check */
        if (lop)
            predicates[i].left = get_old_name (lop, predicates[i].left);

        /* right predicate check */
        if (rop)
            predicates[i].right = get_old_name (rop, predicates[i].right);
    }

    return predicates;
}

/**
 * Extend a projection list with another column.
 */
static PFalg_proj_t *
extend_proj1 (PFla_op_t *proj_op, PFalg_proj_t proj_item)
{
    assert (proj_op->kind == la_project);

    /* allocate new projection array */
    PFalg_proj_t *proj = PFmalloc ((proj_op->sem.proj.count + 1) *
                                   sizeof (PFalg_proj_t));

    for (unsigned int i = 0; i < proj_op->sem.proj.count; i++)
        proj[i] = proj_op->sem.proj.items[i];

    proj[proj_op->sem.proj.count] = proj_item;

    return proj;
}

/**
 * Extend a projection list with another list of columns
 * (from a schema).
 *
 * This function is called when a projection is pushed through
 * a join. To get the correct schema we need to add the columns
 * of the unaffected join partner to the overall projection.
 */
static PFalg_proj_t *
extend_proj (PFla_op_t *op, PFalg_schema_t schema)
{
    assert (op->kind == la_project);

    /* allocate new projection array */
    PFalg_proj_t *proj = PFmalloc ((op->sem.proj.count + schema.count) *
                                   sizeof (PFalg_proj_t));

    /* copying old projections */
    unsigned int i;
    for (i = 0; i < op->sem.proj.count; i++)
        proj[i] = op->sem.proj.items[i];

    /* adding new projections */
    for (unsigned int j = 0; j < schema.count; j++) {
        proj[i+j].old = schema.items[j].name;
        proj[i+j].new = schema.items[j].name;
    }

    return proj;
}

/**
 * Concatenate two projection lists.
 *
 * This function is called when two projections are pushed through
 * a join (from both sides).
 */
static PFalg_proj_t *
merge_projs (PFla_op_t *lop, PFla_op_t *rop)
{
    assert (lop->kind == la_project && rop->kind == la_project);

    /* allocate new projection array */
    PFalg_proj_t *proj = PFmalloc ((lop->sem.proj.count +
                                    rop->sem.proj.count) *
                                   sizeof (PFalg_proj_t));

    /* copying left projections */
    unsigned int i;
    for (i = 0; i < lop->sem.proj.count; i++)
        proj[i] = lop->sem.proj.items[i];

    /* adding right projections */
    for (unsigned int j = 0; j < rop->sem.proj.count; j++) {
        proj[i+j] = rop->sem.proj.items[j];
    }

    return proj;
}

/**
 * Worker for PFalgopt_projection
 */
static void
opt_projection (PFla_op_t *p)
{
    assert(p);

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply projection push up for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_projection (p->child[i]);

    /* action code */

    /* swap projection and current roots operator p followed by a doc_tbl op
     *
     *      |                  |
     *   roots (p)          project
     *      |                  |
     *   doc_tbl    -->     roots (p)
     *      |                  |
     *   project            doc_tbl
     *      |                  |
     *
     */
    if (p->kind == la_roots &&
        L(p)->kind == la_doc_tbl &&
        LL(p)->kind == la_project) {
        PFla_op_t *proj_op = LL(p);

        /* make column name unique */
        PFalg_col_t res     = L(p)->sem.doc_tbl.res,
                    old_res = new_name (L(p)->sem.doc_tbl.res);

        /* we need to make sure that both FRAG and ROOTS see the same changes */
        *L(p) = *doc_tbl (
                     LLL(p),
                     old_res,
                     get_old_name (proj_op, L(p)->sem.doc_tbl.col),
                     L(p)->sem.doc_tbl.kind);

        *p = *PFla_project_ (
                roots (L(p)),
                p->schema.count,
                /* create a new projection list combining the entries
                   of the old projection and the result column */
                extend_proj1 (proj_op, PFalg_proj (res, old_res)));
    }

    /* Most of the rewrites swap projection and current operator p
     *
     *      |                  |
     *     op (p)           project
     *      |        -->       |
     *   project              op (p)
     *      |                  |
     *
     * if the projection renames columns, the columns have to
     * be renamed before swapping the projection and operator p
     */
    if ((L(p) && L(p)->kind == la_project) ||
        (R(p) && R(p)->kind == la_project)) {
        switch (p->kind) {
            case la_attach:
            {
               /* make column name unique */
               PFalg_col_t res = new_name(p->sem.attach.res);

               /* swap projection and current operator */
               *p = *PFla_project_ (
                         attach (LL(p), res, p->sem.attach.value),
                         p->schema.count,
                         /* create a new projection list combining the entries
                            of the old projection and the result column */
                         extend_proj1 (L(p),
                                       PFalg_proj (p->sem.attach.res, res)));
            }   break;

            case la_cross:
            case la_eqjoin:
            case la_thetajoin:
            {
                bool pi_l    = L(p)->kind == la_project,
                     pi_r    = R(p)->kind == la_project,
                     push_l  = true,
                     push_r  = true,
                     push_lr = true;

                PFalg_col_t lcol, rcol;

                /* check if column name conflicts arise */
                if (pi_l && pi_r)
                    for (unsigned int i = 0; i < LL(p)->schema.count; i++) {
                        lcol = LL(p)->schema.items[i].name;
                        for (unsigned int j = 0; j < RL(p)->schema.count; j++) {
                            rcol = RL(p)->schema.items[j].name;
                            push_lr &= lcol != rcol;
                        }
                    }
                if (pi_l)
                    for (unsigned int i = 0; i < LL(p)->schema.count; i++)
                        push_l &= !PFprop_ocol (R(p),
                                                LL(p)->schema.items[i].name);
                if (pi_r)
                    for (unsigned int i = 0; i < RL(p)->schema.count; i++)
                        push_r &= !PFprop_ocol (L(p),
                                                RL(p)->schema.items[i].name);

                /* if push_lr == true then the left and right
                 * projection can pass through the join
                 *
                 *       |                      |
                 *       X (p)             pi_r & pi_l
                 *     /   \         -->        |
                 *  pi_l    pi_r                X (p)
                 *    |      |                /   \
                 *
                 */
                if (pi_l && pi_r && push_lr) {
                    switch (p->kind) {
                        case la_cross:
                            *p = *PFla_project_ (
                                       cross (
                                           LL(p),
                                           RL(p)),
                                       p->schema.count,
                                       merge_projs (L(p), R(p)));
                            break;
                        case la_eqjoin:
                            *p = *PFla_project_ (
                                       eqjoin (
                                           LL(p),
                                           RL(p),
                                           get_old_name (L(p),
                                                         p->sem.eqjoin.col1),
                                           get_old_name (R(p),
                                                         p->sem.eqjoin.col2)),
                                       p->schema.count,
                                       merge_projs (L(p), R(p)));
                            break;
                        case la_thetajoin:
                            *p = *PFla_project_ (
                                       thetajoin (
                                           LL(p),
                                           RL(p),
                                           p->sem.thetajoin.count,
                                           rename_col_in_pred (
                                               p->sem.thetajoin.pred,
                                               p->sem.thetajoin.count,
                                               L(p),
                                               R(p))),
                                       p->schema.count,
                                       merge_projs (L(p), R(p)));
                            break;
                        default:
                            break;
                    }
                    break;
                }

                 /* if push_l == true then only the left
                 * projection can pass through the join
                 *
                 *       |                      |
                 *       X (p)                pi_l++
                 *     /   \         -->        |
                 *  pi_l    (*)                 X (p)
                 *    |                       /   \
                 *   (*)                    (*)    (*)
                 *                                  |
                 */
                else if (pi_l && push_l) {
                    switch (p->kind) {
                        case la_cross:
                            *p = *PFla_project_ (
                                      cross (
                                          LL(p),
                                          R(p)),
                                      p->schema.count,
                                      extend_proj (L(p), R(p)->schema));
                            break;
                        case la_eqjoin:
                            *p = *PFla_project_ (
                                      eqjoin (
                                          LL(p),
                                          R(p),
                                          get_old_name (L(p),
                                                        p->sem.eqjoin.col1),
                                          p->sem.eqjoin.col2),
                                      p->schema.count,
                                      extend_proj (L(p), R(p)->schema));
                            break;
                        case la_thetajoin:
                            *p = *PFla_project_ (
                                      thetajoin (
                                          LL(p),
                                          R(p),
                                          p->sem.thetajoin.count,
                                          rename_col_in_pred (
                                              p->sem.thetajoin.pred,
                                              p->sem.thetajoin.count,
                                              L(p),
                                              NULL)),
                                      p->schema.count,
                                      extend_proj (L(p), R(p)->schema));
                            break;
                        default:
                            break;
                    }
                    break;
                }

                /* if push_r == true then only the right
                 * projection can pass through the join
                 *
                 *       |                      |
                 *       X (p)                pi_r++
                 *     /   \         -->        |
                 *   (*)    pi_r                X (p)
                 *           |                /   \
                 *          (*)             (*)   (*)
                 */
                else if (pi_r && push_r) {
                    switch (p->kind) {
                        case la_cross:
                            *p = *PFla_project_ (
                                      cross (
                                          L(p),
                                          RL(p)),
                                      p->schema.count,
                                      extend_proj (R(p), L(p)->schema));
                            break;
                        case la_eqjoin:
                            *p = *PFla_project_ (
                                      eqjoin (
                                          L(p),
                                          RL(p),
                                          p->sem.eqjoin.col1,
                                          get_old_name (R(p),
                                                        p->sem.eqjoin.col2)),
                                      p->schema.count,
                                      extend_proj (R(p), L(p)->schema));
                            break;
                        case la_thetajoin:
                            *p = *PFla_project_ (
                                      thetajoin (
                                          L(p),
                                          RL(p),
                                          p->sem.thetajoin.count,
                                          rename_col_in_pred (
                                              p->sem.thetajoin.pred,
                                              p->sem.thetajoin.count,
                                              NULL,
                                              R(p))),
                                      p->schema.count,
                                      extend_proj (R(p), L(p)->schema));
                            break;
                        default:
                            break;
                    }
                    break;
                }
            }   break;

            /* is a projection followed by a projection, then merge them
             *
             *      |
             *   project (p)           |
             *      |        -->    project (p)
             *   project               |
             *      |
             */
            case la_project:
                /* merge adjacent projection operators */
                *p = *PFla_project_ (
                           LL(p),
                           p->schema.count,
                           PFalg_proj_merge (
                               p->sem.proj.items,
                               p->sem.proj.count,
                               L(p)->sem.proj.items,
                               L(p)->sem.proj.count));
                break;

            case la_select:
                *p = *PFla_project_ (
                          select_ (
                              LL(p),
                              get_old_name (L(p), p->sem.select.col)),
                          L(p)->schema.count,
                          L(p)->sem.proj.items);
                break;

            case la_pos_select:
                *p = *PFla_project_ (
                          pos_select (
                              LL(p),
                              p->sem.pos_sel.pos,
                              rename_col_in_ordering (L(p),
                                                      p->sem.pos_sel.sortby),
                              (p->sem.pos_sel.part == col_NULL)
                              ? col_NULL
                              : get_old_name (L(p), p->sem.pos_sel.part)),
                        L(p)->schema.count,
                        L(p)->sem.proj.items);
                break;

            case la_fun_1to1:
            {
               /* make column name unique */
               PFalg_col_t res = new_name(p->sem.fun_1to1.res);

               /* swap projection and current operator */
               *p = *PFla_project_ (
                         fun_1to1 (LL(p),
                                   p->sem.fun_1to1.kind,
                                   res,
                                   rename_col_in_collist (
                                       L(p),
                                       p->sem.fun_1to1.refs)),
                         p->schema.count,
                         /* create a new projection list combining the entries
                            of the old projection and the result column */
                         extend_proj1 (L(p),
                                       PFalg_proj (p->sem.fun_1to1.res, res)));
            }   break;

            case la_num_eq:
            case la_num_gt:
            case la_bool_and:
            case la_bool_or:
            case la_to:
            {
               /* make column name unique */
               PFalg_col_t res     = p->sem.binary.res,
                           old_res = new_name (p->sem.binary.res);

               /* patch the semantic information of p
                  (as PFla_op_duplicate will use this information) */
               p->sem.binary.res  = old_res;
               p->sem.binary.col1 = get_old_name (L(p), p->sem.binary.col1);
               p->sem.binary.col2 = get_old_name (L(p), p->sem.binary.col2);

               /* swap projection and current operator */
               *p = *PFla_project_ (
                         PFla_op_duplicate (p, LL(p), NULL),
                         p->schema.count,
                         /* create a new projection list combining the entries
                            of the old projection and the result column */
                         extend_proj1 (L(p), PFalg_proj (res, old_res)));
            }  break;

            case la_bool_not:
            {
               /* make column name unique */
               PFalg_col_t res = new_name(p->sem.unary.res);

               /* swap projection and current operator */
               *p = *PFla_project_ (
                         not (LL(p),
                              res,
                              get_old_name (L(p), p->sem.unary.col)),
                         p->schema.count,
                         /* create a new projection list combining the entries
                            of the old projection and the result column */
                         extend_proj1 (L(p),
                                       PFalg_proj (p->sem.unary.res, res)));
            }   break;

            case la_aggr:
                /* integrate all column renaming in the aggregate inputs */
                for (unsigned int i = 0; i < p->sem.aggr.count; i++)
                    if (p->sem.aggr.aggr[i].col)
                        for (unsigned int j = 0; j < L(p)->sem.proj.count; j++)
                            if (p->sem.aggr.aggr[i].col ==
                                L(p)->sem.proj.items[j].new) {
                                p->sem.aggr.aggr[i].col = 
                                    L(p)->sem.proj.items[j].old;
                                break;
                            }
                /* If partition column is renamed add a renaming projection
                   on top of it that adjusts the partition column. */
                if (p->sem.aggr.part) {
                    PFalg_col_t old_part = col_NULL;
                    for (unsigned int i = 0; i < L(p)->sem.proj.count; i++)
                        if (p->sem.aggr.part == L(p)->sem.proj.items[i].new) {
                            old_part = L(p)->sem.proj.items[i].old;
                            break;
                        }
                    if (p->sem.aggr.part != old_part) {
                        PFalg_proj_t *proj = PFmalloc (p->schema.count *
                                                       sizeof (PFalg_proj_t));
                        for (unsigned int i = 0; i < p->sem.aggr.count; i++)
                            proj[i] = PFalg_proj (p->sem.aggr.aggr[i].res,
                                                  p->sem.aggr.aggr[i].res);
                        proj[p->sem.aggr.count] = PFalg_proj (p->sem.aggr.part,
                                                              old_part);

                        *p = *PFla_project_ (aggr (LL(p),
                                                   old_part,
                                                   p->sem.aggr.count,
                                                   p->sem.aggr.aggr),
                                             p->schema.count,
                                             proj);
                        break;
                    }
                }
                /* remove the projection */
                L(p) = LL(p);
                break;

            case la_rownum:
            case la_rowrank:
            case la_rank:
            {
               /* make column name unique */
               PFalg_col_t res     = p->sem.sort.res,
                           old_res = new_name (p->sem.sort.res);

               /* patch the semantic information of p
                  (as PFla_op_duplicate will use this information) */
               p->sem.sort.res    = old_res;
               p->sem.sort.sortby = rename_col_in_ordering (
                                        L(p),
                                        p->sem.sort.sortby);
               if (p->sem.sort.part)
                   p->sem.sort.part = get_old_name (L(p), p->sem.sort.part);

               /* swap projection and current operator */
               *p = *PFla_project_ (
                         PFla_op_duplicate (p, LL(p), NULL),
                         p->schema.count,
                         /* create a new projection list combining the entries
                            of the old projection and the result column */
                         extend_proj1 (L(p), PFalg_proj (res, old_res)));
            }  break;

            case la_rowid:
            {
               /* make column name unique */
               PFalg_col_t res = new_name(p->sem.rowid.res);

               /* swap projection and current operator */
               *p = *PFla_project_ (
                         rowid (LL(p), res),
                         p->schema.count,
                         /* create a new projection list combining the entries
                            of the old projection and the result column */
                         extend_proj1 (L(p),
                                       PFalg_proj (p->sem.rowid.res, res)));
            }   break;

            case la_type:
            case la_cast:
            {
               /* make column name unique */
               PFalg_col_t res     = p->sem.type.res,
                           old_res = new_name (p->sem.type.res);

               /* patch the semantic information of p
                  (as PFla_op_duplicate will use this information) */
               p->sem.type.res = old_res;
               p->sem.type.col = get_old_name (L(p), p->sem.type.col);

               /* swap projection and current operator */
               *p = *PFla_project_ (
                         PFla_op_duplicate (p, LL(p), NULL),
                         p->schema.count,
                         /* create a new projection list combining the entries
                            of the old projection and the result column */
                         extend_proj1 (L(p), PFalg_proj (res, old_res)));
            }  break;

            case la_type_assert:
               /* swap projection and current operator */
               *p = *PFla_project_ (
                         type_assert_pos (LL(p), 
                                          get_old_name (L(p), p->sem.type.col),
                                          p->sem.type.ty),
                         p->schema.count,
                         L(p)->sem.proj.items);
               break;

            /* swap projection and current operator p
             *
             *      |                    |
             *   step_join (p)        project
             *     / \                   |
             *    /   \               step_join (p)
             *   |     |      -->       / \
             *  doc  project           /   \
             *   |     |             doc   RL(p)
             *
             * if the projection renames columns, the columns have to
             * be renamed before swapping the projection and operator p
             */
            case la_step_join:
            case la_guide_step_join:
            {
               /* make column name unique */
               PFalg_col_t res     = p->sem.step.item_res,
                           old_res = new_name (p->sem.step.item_res);

               /* patch the semantic information of p
                  (as PFla_op_duplicate will use this information) */
               p->sem.step.item_res = old_res;
               p->sem.step.item     = get_old_name (R(p), p->sem.step.item);

               /* swap projection and current operator */
               *p = *PFla_project_ (
                         PFla_op_duplicate (p, L(p), RL(p)),
                         p->schema.count,
                         /* create a new projection list combining the entries
                            of the old projection and the result column */
                         extend_proj1 (R(p), PFalg_proj (res, old_res)));
            }  break;

            /* swap projection and current operator p
             *
             *      |                        |
             *  doc_indx_join (p)         project
             *     / \                       |
             *    /   \                  doc_index_join (p)
             *   |     |        -->         / \
             *  doc  project               /   \
             *   |     |                 doc   RL(p)
             *
             * if the projection renames columns, the columns have to
             * be renamed before swapping the projection and operator p
             */
            case la_doc_index_join:
            {
               /* make column name unique */
               PFalg_col_t res     = p->sem.doc_join.item_res,
                           old_res = new_name (p->sem.doc_join.item_res);

               /* patch the semantic information of p
                  (as PFla_op_duplicate will use this information) */
               p->sem.doc_join.item_res = old_res;
               p->sem.doc_join.item     = get_old_name (
                                              R(p),
                                              p->sem.doc_join.item);
               p->sem.doc_join.item_doc = get_old_name (
                                              R(p),
                                              p->sem.doc_join.item_doc);

               /* swap projection and current operator */
               *p = *PFla_project_ (
                         PFla_op_duplicate (p, L(p), RL(p)),
                         p->schema.count,
                         /* create a new projection list combining the entries
                            of the old projection and the result column */
                         extend_proj1 (R(p), PFalg_proj (res, old_res)));
            }  break;

            /* swap projection and current operator p
             *
             *      |                        |
             *  doc_access (p)            project
             *     / \                       |
             *    /   \                  doc_access (p)
             *   |     |        -->         / \
             *  doc  project               /   \
             *   |     |                 doc   RL(p)
             *
             * if the projection renames columns, the columns have to
             * be renamed before swapping the projection and operator p
             */
            case la_doc_access:
            {
               /* make column name unique */
               PFalg_col_t res     = p->sem.doc_access.res,
                           old_res = new_name (p->sem.doc_access.res);

               /* patch the semantic information of p
                  (as PFla_op_duplicate will use this information) */
               p->sem.doc_access.res = old_res;
               p->sem.doc_access.col = get_old_name (R(p),
                                                     p->sem.doc_access.col);

               /* swap projection and current operator */
               *p = *PFla_project_ (
                         PFla_op_duplicate (p, L(p), RL(p)),
                         p->schema.count,
                         /* create a new projection list combining the entries
                            of the old projection and the result column */
                         extend_proj1 (R(p), PFalg_proj (res, old_res)));
            }  break;

            case la_dummy:
            case la_proxy:
            case la_proxy_base:
                /* swap projection and current operator */
                *p = *PFla_project_ (
                        PFla_op_duplicate (p, LL(p), R(p)),
                        p->schema.count,
                        L(p)->sem.proj.items);
                break;

            /* do nothing */
            default:
                break;
        }
    }
}

/**
 * Invoke projection push-up.
 */
PFla_op_t *
PFalgopt_projection (PFla_op_t *root)
{
    /* Optimize algebra tree */
    opt_projection (root);

    PFla_dag_reset (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
