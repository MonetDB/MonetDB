/**
 * @file
 *
 * Optimize relational algebra expression DAG
 * based on multi valued dependencies. (Cross
 * products and attach operators are moved up
 * along the DAG structure.)
 *
 * We add a new cross operator that can cope with identical
 * columns (la_internal_op). This allows us to push down operators
 * into both operands of the cross product whenever we do not
 * know which operand really requires the operators.
 * A cleaning phase then replaces the clone column aware cross products
 * by normal ones and adds additional project operators if name
 * conflicts would arise.
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

/* apply cse before rewriting */
#include "algebra_cse.h"

/* mnemonic algebra constructors */
#include "logical_mnemonic.h"

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define SEEN(p) ((p)->bit_dag)

/**
 * Use cross product implementation that copes
 * with 'c'loned 'a'ttribute 'n'ames.
 */
#define cross_can(a,b) PFla_cross_opt_internal ((a),(b))

/* Keep track of the ineffective cross product - cross product
   rewrites (cross_changes) and stop if max_cross_changes is reached. */
static unsigned int cross_changes;
static unsigned int max_cross_changes;

/* Keep track of proxy references to avoid accidental rewrites */
static PFarray_t *refs;
/* helper to check for a proxy reference */
static bool is_ref (PFla_op_t *p);

static bool
is_cross (PFla_op_t *p)
{
    return (p->kind == la_cross || p->kind == la_internal_op);
}

/* check if @a col appears in the schema of operator @a p */
static bool
col_present (PFla_op_t *p, PFalg_col_t col)
{
    for (unsigned int i = 0; i < p->schema.count; i++)
        if (p->schema.items[i].name == col)
            return true;

    return false;
}

static PFla_op_t *
internal_op_to_cross (PFla_op_t *p)
{
    assert (p->kind == la_internal_op);

    PFalg_proj_t *proj_list;
    unsigned int i,
                 j,
                 count = 0,
                 dup_count = 0;

    /* create projection list */
    proj_list = PFmalloc ((L(p)->schema.count)
                          * sizeof (*(proj_list)));

    /* create projection list */
    for (i = 0; i < L(p)->schema.count; i++) {
        for (j = 0; j < R(p)->schema.count; j++)
            if (L(p)->schema.items[i].name == R(p)->schema.items[j].name) {
                dup_count++;
                break;
            }

        if (j == R(p)->schema.count)
            proj_list[count++] = proj (L(p)->schema.items[i].name,
                                       L(p)->schema.items[i].name);
    }

    /* ensure that we do not generate empty projection lists */
    if (!count) {
        /* we can throw away the cross product if no column
           is required and the cross product does not change
           the cardinality */
        if (PFprop_card (L(p)->prop) == 1 ||
            /* check current op in case L(p) is a newly
               generated cross product */
            PFprop_card (p->prop) == 1)
            return dummy (R(p));
        else {
            /* The left side does not add anything to the result
               (except for the cardinality). */

            /* new dummy column */
            PFalg_col_t dummy = PFcol_new (col_iter);

            return PFla_project_ (
                       cross (
                           project (
                               attach (L(p), dummy, lit_nat (42)), 
                               proj (dummy, dummy)),
                           R(p)),
                       p->schema.count,
                       PFalg_proj_create (p->schema));
        }
    }
    else {
        if (dup_count)
            return cross (PFla_project_ (L(p), count, proj_list), R(p));
        else
            return cross (L(p), R(p));
    }
}

/* worker for binary operators */
static bool
modify_binary_op (PFla_op_t *p,
                  PFla_op_t * (* op) (const PFla_op_t *,
                                      PFalg_col_t,
                                      PFalg_col_t,
                                      PFalg_col_t))
{
    bool modified = false;

    if (is_cross (L(p))) {
        bool switch_left = col_present (LL(p), p->sem.binary.col1) &&
                           col_present (LL(p), p->sem.binary.col2);
        bool switch_right = col_present (LR(p), p->sem.binary.col1) &&
                            col_present (LR(p), p->sem.binary.col2);

        /* Pushing down the operator twice is only allowed
           if it doesn't affect the cardinality. */
        if (switch_left && switch_right) {
            *p = *(cross_can (op (LL(p),
                                  p->sem.binary.res,
                                  p->sem.binary.col1,
                                  p->sem.binary.col2),
                              op (LR(p),
                                  p->sem.binary.res,
                                  p->sem.binary.col1,
                                  p->sem.binary.col2)));
            modified = true;
        }
        else if (switch_left) {
            *p = *(cross_can (op (LL(p),
                                  p->sem.binary.res,
                                  p->sem.binary.col1,
                                  p->sem.binary.col2),
                              LR(p)));
            modified = true;
        }
        else if (switch_right) {
            *p = *(cross_can (LL(p),
                              op (LR(p),
                                  p->sem.binary.res,
                                  p->sem.binary.col1,
                                  p->sem.binary.col2)));
            modified = true;
        }
    }
    return modified;
}

/* worker for unary operators */
static bool
modify_unary_op (PFla_op_t *p,
                  PFla_op_t * (* op) (const PFla_op_t *,
                                      PFalg_col_t,
                                      PFalg_col_t))
{
    bool modified = false;

    if (is_cross (L(p))) {
        bool switch_left = col_present (LL(p), p->sem.unary.col);
        bool switch_right = col_present (LR(p), p->sem.unary.col);

        /* Pushing down the operator twice is only allowed
           if it doesn't affect the cardinality. */
        if (switch_left && switch_right) {
            *p = *(cross_can (op (LL(p),
                                  p->sem.unary.res,
                                  p->sem.unary.col),
                              op (LR(p),
                                  p->sem.unary.res,
                                  p->sem.unary.col)));
            modified = true;
        }
        else if (switch_left) {
            *p = *(cross_can (op (LL(p),
                                  p->sem.unary.res,
                                  p->sem.unary.col),
                              LR(p)));
            modified = true;
        }
        else if (switch_right) {
            *p = *(cross_can (LL(p),
                              op (LR(p),
                                  p->sem.unary.res,
                                  p->sem.unary.col)));
            modified = true;
        }
    }
    return modified;
}

/* worker for aggregate operators */
static bool
modify_aggr (PFla_op_t *p)
{
    bool modified = false;

    assert (p->kind == la_aggr);

    /* An expression that contains the partitioning column is
       independent of the aggregate. The translation thus moves the
       expression above the aggregate operator and removes its partitioning
       column. */
    if (is_cross (L(p)) &&
        p->sem.aggr.part) {
        PFla_op_t *lp        = NULL,
                  *rp        = NULL;
        bool       all_left  = true,
                   all_right = true;

        /* check if all aggregate columns reside in one cross product
           argument */
        for (unsigned int i = 0; i < p->sem.aggr.count; i++) {
            /* Some aggregates (such as count, sum, ...) are affected
               by the cardinality of the input. For these aggregates
               we would need to ensure the keyness of the part column.
               As we don't have that information at hand we skip the
               rewrite. */
            switch (p->sem.aggr.aggr[i].kind) {
                case alg_aggr_min:
                case alg_aggr_max:
                case alg_aggr_avg:
                case alg_aggr_all:
                    break;
                default:
                    return modified;
            }

            if (p->sem.aggr.aggr[i].kind != alg_aggr_count) {
                if (!col_present (LL(p), p->sem.aggr.aggr[i].col))
                    all_left &= false;
                else if (!col_present (LR(p), p->sem.aggr.aggr[i].col))
                    all_right &= false;
            }
        }

        if (col_present (LL(p), p->sem.aggr.part) &&
            !col_present (LR(p), p->sem.aggr.part) &&
            all_right) {
            lp = LL(p);
            rp = LR(p);
        }
        /* if not present check the right operand */
        else if (col_present (LR(p), p->sem.aggr.part) &&
                 !col_present (LL(p), p->sem.aggr.part) &&
                 all_left) {
            lp = LR(p);
            rp = LL(p);
        }

        if (lp && rp) {
            /* Invent a new constant partition column as we need
               to ensure that aggregates on empty inputs return
               an empty input. (The unpartitioned aggregate will
               *always* return a single row.) */
            PFalg_col_t const_part = PFcol_new (col_iter);

            *p = *PFla_project_ (
                      cross_can (
                          distinct (
                              project (lp, proj (p->sem.aggr.part,
                                                 p->sem.aggr.part))),
                          aggr (attach (rp, const_part, lit_nat (21)),
                                const_part,
                                p->sem.aggr.count,
                                p->sem.aggr.aggr)),
                      p->schema.count,
                      PFalg_proj_create (p->schema));
            modified = true;
        }
    }
    return modified;
}

/* check if the semantical information
   of two columns matches */
static bool
project_identical (PFla_op_t *a, PFla_op_t *b)
{
    if (a->sem.proj.count != b->sem.proj.count)
        return false;

    for (unsigned int i = 0; i < a->sem.proj.count; i++)
        if (a->sem.proj.items[i].new != b->sem.proj.items[i].new
            || a->sem.proj.items[i].old != b->sem.proj.items[i].old)
            return false;

    return true;
}

/**
 * worker for PFalgopt_mvd
 *
 * opt_mvd looks up an independent expression and
 * tries to move it up the DAG as much as possible.
 */
static bool do_opt_mvd (PFla_op_t *p, bool modified);

static bool
opt_mvd (PFla_op_t *p)
{
    bool modified = false;

    assert (p);

    PFrecursion_fence ();

    /* rewrite each node only once */
    if (SEEN(p))
        return false;
    else
        SEEN(p) = true;

    /* apply complex optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        modified = opt_mvd (p->child[i]) || modified ;

    return do_opt_mvd (p, modified);
}

static bool
do_opt_mvd (PFla_op_t *p, bool modified)
{
    bool cross_cross = false;
    unsigned int i, j;

    /**
     * In the following action code we try to propagate cross
     * products up the DAG as far as possible. To avoid possible
     * 'deadlocks' we discard all independent expressions that
     * are constant thus reducing the number of rivalling cross
     * product operators.
     *
     * Exceptions are the attach and the union operator where
     * the nested attach operator introduces new independencies.
     * (For intersect and difference we assume that constant
     *  optimization already removed the attach operators.)
     */

    /* look if there is an occurrence of two nested cross products */
    if (!is_cross (p) &&
        ((L(p) &&
          is_cross (L(p)) &&
          (is_cross (LL(p)) || is_cross (LR(p))))
         ||
         (R(p) &&
          is_cross (R(p)) &&
          (is_cross (RL(p)) || is_cross (RR(p))))))
        cross_cross = true;

    /* action code */
    switch (p->kind) {

    case la_serialize_seq:
    case la_serialize_rel:
    case la_side_effects:
        break;

    case la_lit_tbl:
    case la_empty_tbl:
    case la_ref_tbl:
        /* nothing to do -- everything is constant */
        break;

    case la_attach:
        /* Replace two consecutive attach operators by a cross
           product of the input of the nested attach operator and
           a base table. The idea is to move possibly independent
           expression up the DAG. */
        if (L(p)->kind == la_attach) {
            *p = *(cross_can (
                      LL(p),
                      attach (
                          lit_tbl (collist (L(p)->sem.attach.res),
                                   tuple (L(p)->sem.attach.value)),
                          p->sem.attach.res,
                          p->sem.attach.value)));
            modified = true;
        }
        else if (is_cross (L(p))) {
            /* push attach into both cross product operands */
            *p = *(cross_can (
                       attach (LL(p),
                               p->sem.attach.res,
                               p->sem.attach.value),
                       attach (LR(p),
                               p->sem.attach.res,
                               p->sem.attach.value)));
            modified = true;
        }
        break;

    case la_cross:
    case la_internal_op:
        /* If there are two nested cross products make sure that
           each operand has the chance to be propagated up the DAG:

                 x            x              x          x
                / \          / \            / \        / \
               1   x    =>  2   x   and    x   3  =>  1   x
                  / \          / \        / \            / \
                 2   3        3   1      1   2          2   3

           The only problem is that this might cause an endless
           loop if the above operator introduces a dependency.
           Therefore we introduce a check at the end of this function
           that sets the 'modified' bit to false if multiple times
           two adjacent nested cross products did not cause a rewrite.
          */

        if (is_cross (L(p))) {
            *p = *(cross_can (LL(p),
                              cross_can (LR(p),
                                         R(p))));
            modified = true;
        }
        else if (is_cross (R(p))) {
            *p = *(cross_can (RL(p),
                              cross_can (RR(p),
                                         L(p))));
            modified = true;
        }
        break;

    case la_eqjoin:
        /* Move the independent expression (the one without
           join column) up the DAG. */
        if (is_cross (L(p))) {
            if (col_present (LL(p), p->sem.eqjoin.col1))
                *p = *(cross_can (LR(p), eqjoin (LL(p), R(p),
                                                 p->sem.eqjoin.col1,
                                                 p->sem.eqjoin.col2)));
            else
                *p = *(cross_can (LL(p), eqjoin (LR(p), R(p),
                                                 p->sem.eqjoin.col1,
                                                 p->sem.eqjoin.col2)));

            modified = true;
            break;

        }
        if (is_cross (R(p))) {
            if (col_present (RL(p), p->sem.eqjoin.col2))
                *p = *(cross_can (RR(p), eqjoin (L(p), RL(p),
                                                 p->sem.eqjoin.col1,
                                                 p->sem.eqjoin.col2)));
            else
                *p = *(cross_can (RL(p), eqjoin (L(p), RR(p),
                                                 p->sem.eqjoin.col1,
                                                 p->sem.eqjoin.col2)));

            modified = true;
            break;
        }
        break;

    case la_semijoin:
        /* Move the independent expression (the one without
           join column) up the DAG. */
        if (is_cross (L(p))) {
            if (col_present (LL(p), p->sem.eqjoin.col1))
                *p = *(cross_can (LR(p), semijoin (LL(p), R(p),
                                                   p->sem.eqjoin.col1,
                                                   p->sem.eqjoin.col2)));
            else
                *p = *(cross_can (LL(p), semijoin (LR(p), R(p),
                                                   p->sem.eqjoin.col1,
                                                   p->sem.eqjoin.col2)));

            modified = true;
            break;

        }
        break;

    case la_thetajoin:
        /* Move the independent expression (the one without any
           join columns) up the DAG. */
        if (is_cross (L(p))) {
            bool all_left = true,
                 all_right = true;

            for (i = 0; i < p->sem.thetajoin.count; i++) {
                all_left  = all_left &&
                            col_present (LL(p), p->sem.thetajoin.pred[i].left);
                all_right = all_right &&
                            col_present (LR(p), p->sem.thetajoin.pred[i].left);
            }

            if (all_left)
                *p = *(cross_can (LR(p), thetajoin (LL(p), R(p),
                                                    p->sem.thetajoin.count,
                                                    p->sem.thetajoin.pred)));
            else if (all_right)
                *p = *(cross_can (LL(p), thetajoin (LR(p), R(p),
                                                    p->sem.thetajoin.count,
                                                    p->sem.thetajoin.pred)));
            else {
                PFalg_sel_t *pred1,
                            *pred2;
                unsigned int pred1_count = 0,
                             pred2_count = 0;
                PFla_op_t   *lp = L(p);
                if (lp->kind != la_cross) {
                    lp = internal_op_to_cross (lp);
                    if (lp->kind != la_cross)
                        break;
                }
                /* We have to split up the thetajoin predicates according
                   to the cross product arguments and apply two thetajoins. */
                pred1 = PFmalloc ((p->sem.thetajoin.count - 1) *
                                  sizeof (PFalg_sel_t)),
                pred2 = PFmalloc ((p->sem.thetajoin.count - 1) *
                                  sizeof (PFalg_sel_t));
                for (i = 0; i < p->sem.thetajoin.count; i++) {
                    if (col_present (L(lp), p->sem.thetajoin.pred[i].left))
                        pred1[pred1_count++] = p->sem.thetajoin.pred[i];
                    if (col_present (R(lp), p->sem.thetajoin.pred[i].left))
                        pred2[pred2_count++] = p->sem.thetajoin.pred[i];
                }
                assert (pred1_count + pred2_count == p->sem.thetajoin.count);

                *p = *(thetajoin (R(lp),
                                  thetajoin (L(lp), R(p), pred1_count, pred1),
                                  pred2_count,
                                  pred2));
            }

            modified = true;
            break;
        }
        if (is_cross (R(p))) {
            bool all_left = true,
                 all_right = true;

            for (i = 0; i < p->sem.thetajoin.count; i++) {
                all_left  = all_left &&
                            col_present (RL(p), p->sem.thetajoin.pred[i].right);
                all_right = all_right &&
                            col_present (RR(p), p->sem.thetajoin.pred[i].right);
            }

            if (all_left)
                *p = *(cross_can (RR(p), thetajoin (L(p), RL(p),
                                                    p->sem.thetajoin.count,
                                                    p->sem.thetajoin.pred)));
            else if (all_right)
                *p = *(cross_can (RL(p), thetajoin (L(p), RR(p),
                                                    p->sem.thetajoin.count,
                                                    p->sem.thetajoin.pred)));
            else {
                PFalg_sel_t *pred1,
                            *pred2;
                unsigned int pred1_count = 0,
                             pred2_count = 0;
                PFla_op_t   *rp = R(p);
                if (rp->kind != la_cross) {
                    rp = internal_op_to_cross (rp);
                    if (rp->kind != la_cross)
                        break;
                }
                /* We have to split up the thetajoin predicates according
                   to the cross product arguments and apply two thetajoins. */
                pred1 = PFmalloc ((p->sem.thetajoin.count - 1) *
                                  sizeof (PFalg_sel_t)),
                pred2 = PFmalloc ((p->sem.thetajoin.count - 1) *
                                  sizeof (PFalg_sel_t));
                for (i = 0; i < p->sem.thetajoin.count; i++) {
                    if (col_present (L(rp), p->sem.thetajoin.pred[i].right))
                        pred1[pred1_count++] = p->sem.thetajoin.pred[i];
                    if (col_present (R(rp), p->sem.thetajoin.pred[i].right))
                        pred2[pred2_count++] = p->sem.thetajoin.pred[i];
                }
                assert (pred1_count + pred2_count == p->sem.thetajoin.count);

                *p = *(thetajoin (thetajoin (L(p), L(rp), pred1_count, pred1),
                                  R(rp),
                                  pred2_count,
                                  pred2));
            }

            modified = true;
            break;
        }
        break;

    case la_project:
        /* Split project operator and push it beyond the cross product. */
        if (is_cross (L(p))) {
            PFalg_proj_t *proj_list1,
                         *proj_list2;
            unsigned int  count1 = 0,
                          count2 = 0;

            /* create first projection list */
            proj_list1 = PFmalloc (p->schema.count *
                                   sizeof (*(proj_list1)));

            for (i = 0; i < LL(p)->schema.count; i++)
                for (j = 0; j < p->sem.proj.count; j++)
                    if (LL(p)->schema.items[i].name
                        == p->sem.proj.items[j].old) {
                        proj_list1[count1++] = p->sem.proj.items[j];
                    }

            /* create second projection list */
            proj_list2 = PFmalloc (p->schema.count *
                                   sizeof (*(proj_list1)));

            for (i = 0; i < LR(p)->schema.count; i++)
                for (j = 0; j < p->sem.proj.count; j++)
                    if (LR(p)->schema.items[i].name
                        == p->sem.proj.items[j].old) {
                        proj_list2[count2++] = p->sem.proj.items[j];
                    }

            /* Ensure that both arguments add at least one column to
               the result. */
            if (count1 && count2) {
                PFla_op_t *lp,
                          *rp;

                /* Merge adjacent projections to detect more rewrites
                   (e.g., for union operators). Be careful however
                   to avoid destroying the proxy pattern as otherwise
                   a subsequent thetajoin optimization may create
                   inconsistencies. */
                if (LL(p)->kind == la_project && !is_ref(LL(p)))
                    lp = PFla_project_ (LLL(p),
                                        count1,
                                        PFalg_proj_merge (
                                            proj_list1,
                                            count1,
                                            LL(p)->sem.proj.items,
                                            LL(p)->sem.proj.count));
                else
                    lp = PFla_project_ (LL(p), count1, proj_list1);

                if (LR(p)->kind == la_project && !is_ref(LR(p)))
                    rp = PFla_project_ (LRL(p),
                                        count2,
                                        PFalg_proj_merge (
                                            proj_list2,
                                            count2,
                                            LR(p)->sem.proj.items,
                                            LR(p)->sem.proj.count));
                else
                    rp = PFla_project_ (LR(p), count2, proj_list2);

                *p = *(cross_can (lp, rp));
                modified = true;
            }
        }
        break;

    case la_select:
        if (is_cross (L(p))) {
            if (col_present (LL(p), p->sem.select.col))
                *p = *(cross_can (LR(p), select_ (LL(p),
                                                  p->sem.select.col)));
            else
                *p = *(cross_can (LL(p), select_ (LR(p),
                                                  p->sem.select.col)));

            modified = true;
        }
        break;

    case la_pos_select:
        /* An expression that does not contain any sorting column
           required by the positional select operator, but contains
           the partitioning column is independent of the positional
           select. The translation thus moves the expression above
           the positional selection and removes its partitioning column. */
        if (is_cross (L(p)) &&
            p->sem.pos_sel.part) {
            bool part, sortby;

            /* first check the dependencies of the left cross product input */
            part = false;
            sortby = false;
            for (i = 0; i < LL(p)->schema.count; i++) {
                if (LL(p)->schema.items[i].name == p->sem.pos_sel.part)
                    part = true;
                for (j = 0;
                     j < PFord_count (p->sem.pos_sel.sortby);
                     j++)
                    if (LL(p)->schema.items[i].name
                        == PFord_order_col_at (
                               p->sem.pos_sel.sortby,
                               j)) {
                        sortby = true;
                        break;
                    }
            }
            if (part && !sortby) {
                *p = *(cross_can (
                          LL(p),
                          pos_select (
                              LR(p),
                              p->sem.pos_sel.pos,
                              p->sem.pos_sel.sortby,
                              col_NULL)));
                modified = true;
                break;
            }

            /* then check the dependencies of the right cross product input */
            part = false;
            sortby = false;
            for (i = 0; i < LR(p)->schema.count; i++) {
                if (LR(p)->schema.items[i].name == p->sem.pos_sel.part)
                    part = true;
                for (j = 0;
                     j < PFord_count (p->sem.pos_sel.sortby);
                     j++)
                    if (LR(p)->schema.items[i].name
                        == PFord_order_col_at (
                               p->sem.pos_sel.sortby,
                               j)) {
                        sortby = true;
                        break;
                    }
            }
            if (part && !sortby) {
                *p = *(cross_can (
                          LR(p),
                          pos_select (
                              LL(p),
                              p->sem.pos_sel.pos,
                              p->sem.pos_sel.sortby,
                              col_NULL)));
                modified = true;
                break;
            }
        }
        break;

    case la_disjunion:
        /* If the children of the union operator are independent
           expressions (both attach operator or cross product) which
           reference the same subexpression, we move this expression
           above the union. */
        if (L(p)->kind == la_attach &&
            R(p)->kind == la_attach &&
            LL(p) == RL(p)) {
            *p = *(cross_can (
                      LL(p),
                      disjunion (
                          lit_tbl (collist (L(p)->sem.attach.res),
                                   tuple (L(p)->sem.attach.value)),
                          lit_tbl (collist (R(p)->sem.attach.res),
                                   tuple (R(p)->sem.attach.value)))));
            modified = true;
            break;
        }
        else if (L(p)->kind == la_attach &&
                 is_cross (R(p))) {
            if (LL(p) == RL(p)) {
                *p = *(cross_can (
                           LL(p),
                           disjunion (
                               lit_tbl (collist (L(p)->sem.attach.res),
                                        tuple (L(p)->sem.attach.value)),
                               RR(p))));
                modified = true;
                break;
            }
            else if (LL(p) == RR(p)) {
                *p = *(cross_can (
                           LL(p),
                           disjunion (
                               lit_tbl (collist (L(p)->sem.attach.res),
                                        tuple (L(p)->sem.attach.value)),
                               RL(p))));
                modified = true;
                break;
            }
        }
        else if (is_cross (L(p)) &&
                 R(p)->kind == la_attach) {
            if (LL(p) == RL(p)) {
                *p = *(cross_can (
                           RL(p),
                           disjunion (
                               lit_tbl (collist (R(p)->sem.attach.res),
                                        tuple (R(p)->sem.attach.value)),
                               LR(p))));
                modified = true;
                break;
            }
            else if (LR(p) == RL(p)) {
                *p = *(cross_can (
                           RL(p),
                           disjunion (
                               lit_tbl (collist (R(p)->sem.attach.res),
                                        tuple (R(p)->sem.attach.value)),
                               LL(p))));
                modified = true;
                break;
            }
        }
        else if (is_cross (L(p)) &&
                 is_cross (R(p))) {
            if (LL(p) == RL(p)) {
                *p = *(cross_can (LL(p), disjunion (LR(p), RR(p))));
                modified = true;
                break;
            }
            else if (LR(p) == RL(p)) {
                *p = *(cross_can (LR(p), disjunion (LL(p), RR(p))));
                modified = true;
                break;
            }
            else if (LL(p) == RR(p)) {
                *p = *(cross_can (LL(p), disjunion (LR(p), RL(p))));
                modified = true;
                break;
            }
            else if (LR(p) == RR(p)) {
                *p = *(cross_can (LR(p), disjunion (LL(p), RL(p))));
                modified = true;
                break;
            }
            else if (LL(p)->kind == la_project &&
                     RL(p)->kind == la_project &&
                     LLL(p) == RLL(p) &&
                     project_identical (LL(p), RL(p))) {
                *p = *(cross_can (LL(p), disjunion (LR(p), RR(p))));
                modified = true;
                break;
            }
            else if (LR(p)->kind == la_project &&
                     RL(p)->kind == la_project &&
                     LRL(p) == RLL(p) &&
                     project_identical (LR(p), RL(p))) {
                *p = *(cross_can (LR(p), disjunion (LL(p), RR(p))));
                modified = true;
                break;
            }
            else if (LL(p)->kind == la_project &&
                     RR(p)->kind == la_project &&
                     LLL(p) == RRL(p) &&
                     project_identical (LL(p), RR(p))) {
                *p = *(cross_can (LL(p), disjunion (LR(p), RL(p))));
                modified = true;
                break;
            }
            else if (LR(p)->kind == la_project &&
                     RR(p)->kind == la_project &&
                     LRL(p) == RRL(p) &&
                     project_identical (LR(p), RR(p))) {
                *p = *(cross_can (LR(p), disjunion (LL(p), RL(p))));
                modified = true;
                break;
            }
        }
        break;

    case la_intersect:
        /* If the children of the intersect operator are independent
           expressions (cross product) which reference the same
           subexpression, we move this expression above the intersect. */
        if (is_cross (L(p)) &&
            is_cross (R(p))) {
            if (LL(p) == RL(p)) {
                *p = *(cross_can (LL(p), intersect (LR(p), RR(p))));
                modified = true;
                break;
            }
            else if (LR(p) == RL(p)) {
                *p = *(cross_can (LR(p), intersect (LL(p), RR(p))));
                modified = true;
                break;
            }
            else if (LL(p) == RR(p)) {
                *p = *(cross_can (LL(p), intersect (LR(p), RL(p))));
                modified = true;
                break;
            }
            else if (LR(p) == RR(p)) {
                *p = *(cross_can (LR(p), intersect (LL(p), RL(p))));
                modified = true;
                break;
            }
        }
        break;

    case la_difference:
        /* If the children of the difference operator are independent
           expressions (cross product) which reference the same
           subexpression, we move this expression above the difference. */
        if (is_cross (L(p)) &&
            is_cross (R(p))) {
            if (LL(p) == RL(p)) {
                *p = *(cross_can (LL(p), difference (LR(p), RR(p))));
                modified = true;
                break;
            }
            else if (LR(p) == RL(p)) {
                *p = *(cross_can (LR(p), difference (LL(p), RR(p))));
                modified = true;
                break;
            }
            else if (LL(p) == RR(p)) {
                *p = *(cross_can (LL(p), difference (LR(p), RL(p))));
                modified = true;
                break;
            }
            else if (LR(p) == RR(p)) {
                *p = *(cross_can (LR(p), difference (LL(p), RL(p))));
                modified = true;
                break;
            }
        }
        break;

    case la_distinct:
        /* Push distinct into both cross product operands. */
        if (is_cross (L(p))) {
            *p = *(cross_can (distinct (LL(p)), distinct (LR(p))));
            modified = true;
        }
        break;

    case la_fun_1to1:
        if (is_cross (L(p))) {
            bool switch_left = true;
            bool switch_right = true;

            for (i = 0; i < clsize (p->sem.fun_1to1.refs); i++) {
                switch_left  = switch_left &&
                               col_present (LL(p),
                                            clat (p->sem.fun_1to1.refs, i));
                switch_right = switch_right &&
                               col_present (LR(p),
                                            clat (p->sem.fun_1to1.refs, i));
            }

            /* Pushing down the operator twice is only allowed
               if it doesn't affect the cardinality. */
            if (switch_left && switch_right) {
                *p = *(cross_can (fun_1to1 (LL(p),
                                            p->sem.fun_1to1.kind,
                                            p->sem.fun_1to1.res,
                                            p->sem.fun_1to1.refs),
                                  fun_1to1 (LR(p),
                                            p->sem.fun_1to1.kind,
                                            p->sem.fun_1to1.res,
                                            p->sem.fun_1to1.refs)));
                modified = true;
            }
            else if (switch_left) {
                *p = *(cross_can (fun_1to1 (LL(p),
                                            p->sem.fun_1to1.kind,
                                            p->sem.fun_1to1.res,
                                            p->sem.fun_1to1.refs),
                                  LR(p)));
                modified = true;
            }
            else if (switch_right) {
                *p = *(cross_can (LL(p),
                                  fun_1to1 (LR(p),
                                            p->sem.fun_1to1.kind,
                                            p->sem.fun_1to1.res,
                                            p->sem.fun_1to1.refs)));
                modified = true;
            }
        }
        break;

    case la_num_eq:
        modified = modify_binary_op (p, PFla_eq) || modified;
        break;
    case la_num_gt:
        modified = modify_binary_op (p, PFla_gt) || modified;
        break;
    case la_bool_and:
        modified = modify_binary_op (p, PFla_and) || modified;
        break;
    case la_bool_or:
        modified = modify_binary_op (p, PFla_or) || modified;
        break;
    case la_bool_not:
        modified = modify_unary_op (p, PFla_not) || modified;
        break;

    case la_to:
        if (is_cross (L(p))) {
            bool switch_left = col_present (LL(p), p->sem.binary.col1) &&
                               col_present (LL(p), p->sem.binary.col2);
            bool switch_right = col_present (LR(p), p->sem.binary.col1) &&
                                col_present (LR(p), p->sem.binary.col2);

            if (switch_left) {
                *p = *(cross_can (to (LL(p),
                                      p->sem.binary.res,
                                      p->sem.binary.col1,
                                      p->sem.binary.col2),
                                  LR(p)));
                modified = true;
            }
            else if (switch_right) {
                *p = *(cross_can (LL(p),
                                  to (LR(p),
                                      p->sem.binary.res,
                                      p->sem.binary.col1,
                                      p->sem.binary.col2)));
                modified = true;
            }
        }
        break;

    case la_aggr:
        modified = modify_aggr (p) || modified;
        break;

    case la_rownum:
        /* An expression that does not contain any sorting column
           required by the rownum operator, but contains the partitioning
           column is independent of the rownum. The translation thus
           moves the expression above the rownum and removes its partitioning
           column. */
        if (is_cross (L(p)) &&
            p->sem.sort.part) {
            bool part, sortby;

            /* first check the dependencies of the left cross product input */
            part = false;
            sortby = false;
            for (i = 0; i < LL(p)->schema.count; i++) {
                if (LL(p)->schema.items[i].name == p->sem.sort.part)
                    part = true;
                for (j = 0;
                     j < PFord_count (p->sem.sort.sortby);
                     j++)
                    if (LL(p)->schema.items[i].name
                        == PFord_order_col_at (
                               p->sem.sort.sortby,
                               j)) {
                        sortby = true;
                        break;
                    }
            }
            if (part && !sortby) {
                *p = *(cross_can (
                          LL(p),
                          rownum (
                              LR(p),
                              p->sem.sort.res,
                              p->sem.sort.sortby,
                              col_NULL)));
                modified = true;
                break;
            }

            /* then check the dependencies of the right cross product input */
            part = false;
            sortby = false;
            for (i = 0; i < LR(p)->schema.count; i++) {
                if (LR(p)->schema.items[i].name == p->sem.sort.part)
                    part = true;
                for (j = 0;
                     j < PFord_count (p->sem.sort.sortby);
                     j++)
                    if (LR(p)->schema.items[i].name
                        == PFord_order_col_at (
                               p->sem.sort.sortby,
                               j)) {
                        sortby = true;
                        break;
                    }
            }
            if (part && !sortby) {
                *p = *(cross_can (
                          LR(p),
                          rownum (
                              LL(p),
                              p->sem.sort.res,
                              p->sem.sort.sortby,
                              col_NULL)));
                modified = true;
                break;
            }
        }
        break;

    case la_rowrank:
    case la_rank:
        /* An expression that does not contain any sorting column
           required by the rank operator, is independent of the rank.
           The translation thus moves the expression above the rank. */
        if (is_cross (L(p))) {
            bool sortby;
            PFla_op_t *(* op) (const PFla_op_t *,
                               PFalg_col_t,
                               PFord_ordering_t);

            op = p->kind == la_rowrank ? PFla_rowrank : PFla_rank;

            /* first check the dependencies of the left cross product input */
            sortby = false;
            for (i = 0; i < LL(p)->schema.count; i++)
                for (j = 0;
                     j < PFord_count (p->sem.sort.sortby);
                     j++)
                    if (LL(p)->schema.items[i].name
                        == PFord_order_col_at (
                               p->sem.sort.sortby,
                               j)) {
                        sortby = true;
                        break;
                    }
            if (!sortby) {
                *p = *(cross_can (
                          LL(p),
                          op (LR(p),
                              p->sem.sort.res,
                              p->sem.sort.sortby)));
                modified = true;
                break;
            }

            /* then check the dependencies of the right cross product input */
            sortby = false;
            for (i = 0; i < LR(p)->schema.count; i++)
                for (j = 0;
                     j < PFord_count (p->sem.sort.sortby);
                     j++)
                    if (LR(p)->schema.items[i].name
                        == PFord_order_col_at (
                               p->sem.sort.sortby,
                               j)) {
                        sortby = true;
                        break;
                    }
            if (!sortby) {
                *p = *(cross_can (
                          LR(p),
                          op (LL(p),
                              p->sem.sort.res,
                              p->sem.sort.sortby)));
                modified = true;
                break;
            }
        }
        break;

    case la_rowid:
        break;

    case la_type:
        if (is_cross (L(p))) {
            bool switch_left = col_present (LL(p), p->sem.type.col);
            bool switch_right = col_present (LR(p), p->sem.type.col);

            /* Pushing down the operator twice is only allowed
               if it doesn't affect the cardinality. */
            if (switch_left && switch_right) {
                *p = *(cross_can (type (LL(p),
                                        p->sem.type.res,
                                        p->sem.type.col,
                                        p->sem.type.ty),
                                  type (LR(p),
                                        p->sem.type.res,
                                        p->sem.type.col,
                                        p->sem.type.ty)));
                modified = true;
            }
            else if (switch_left) {
                *p = *(cross_can (type (LL(p),
                                        p->sem.type.res,
                                        p->sem.type.col,
                                        p->sem.type.ty),
                                  LR(p)));
                modified = true;
            }
            else if (switch_right) {
                *p = *(cross_can (LL(p),
                                  type (LR(p),
                                        p->sem.type.res,
                                        p->sem.type.col,
                                        p->sem.type.ty)));
                modified = true;
            }
        }
        break;

    case la_type_assert:
        if (is_cross (L(p))) {
            bool switch_left = col_present (LL(p), p->sem.type.col);
            bool switch_right = col_present (LR(p), p->sem.type.col);

            /* Pushing down the operator twice is only allowed
               if it doesn't affect the cardinality. */
            if (switch_left && switch_right) {
                *p = *(cross_can (type_assert_pos (
                                      LL(p),
                                      p->sem.type.col,
                                      p->sem.type.ty),
                                  type_assert_pos (
                                      LR(p),
                                      p->sem.type.col,
                                      p->sem.type.ty)));
                modified = true;
            }
            else if (switch_left) {
                *p = *(cross_can (type_assert_pos (
                                      LL(p),
                                      p->sem.type.col,
                                      p->sem.type.ty),
                                  LR(p)));
                modified = true;
            }
            else if (switch_right) {
                *p = *(cross_can (LL(p),
                                  type_assert_pos (
                                      LR(p),
                                      p->sem.type.col,
                                      p->sem.type.ty)));
                modified = true;
            }
        }
        break;

    case la_cast:
        if (is_cross (L(p))) {
            bool switch_left = col_present (LL(p), p->sem.type.col);
            bool switch_right = col_present (LR(p), p->sem.type.col);

            /* Pushing down the operator twice is only allowed
               if it doesn't affect the cardinality. */
            if (switch_left && switch_right) {
                *p = *(cross_can (cast (LL(p),
                                        p->sem.type.res,
                                        p->sem.type.col,
                                        p->sem.type.ty),
                                  cast (LR(p),
                                        p->sem.type.res,
                                        p->sem.type.col,
                                        p->sem.type.ty)));
                modified = true;
            }
            else if (switch_left) {
                *p = *(cross_can (cast (LL(p),
                                        p->sem.type.res,
                                        p->sem.type.col,
                                        p->sem.type.ty),
                                  LR(p)));
                modified = true;
            }
            else if (switch_right) {
                *p = *(cross_can (LL(p),
                                  cast (LR(p),
                                        p->sem.type.res,
                                        p->sem.type.col,
                                        p->sem.type.ty)));
                modified = true;
            }
        }
        break;

    case la_step:
        if (is_cross (R(p))) {
            if (col_present (RL(p), p->sem.step.item)) {
                *p = *(cross_can (
                           RR(p),
                           project (step (L(p),
                                            attach (RL(p),
                                                    p->sem.step.iter,
                                                    lit_nat(1)),
                                            p->sem.step.spec,
                                            p->sem.step.level,
                                            p->sem.step.iter,
                                            p->sem.step.item,
                                            p->sem.step.item_res),
                                    proj (p->sem.step.item_res,
                                          p->sem.step.item_res))));
            } else {
                *p = *(cross_can (
                           RL(p),
                           project (step (L(p),
                                            attach (RR(p),
                                                    p->sem.step.iter,
                                                    lit_nat(1)),
                                            p->sem.step.spec,
                                            p->sem.step.level,
                                            p->sem.step.iter,
                                            p->sem.step.item,
                                            p->sem.step.item_res),
                                    proj (p->sem.step.item_res,
                                          p->sem.step.item_res))));
            }
            modified = true;
        }
        break;

    case la_step_join:
        if (is_cross (R(p))) {
            bool switch_left = col_present (RL(p), p->sem.step.item);
            bool switch_right = col_present (RR(p), p->sem.step.item);

            if (switch_left) {
                *p = *(cross_can (step_join (
                                        L(p), RL(p),
                                        p->sem.step.spec,
                                        p->sem.step.level,
                                        p->sem.step.item,
                                        p->sem.step.item_res),
                                  RR(p)));
                modified = true;
            }
            else if (switch_right) {
                *p = *(cross_can (RL(p),
                                  step_join (
                                        L(p),
                                        RR(p),
                                        p->sem.step.spec,
                                        p->sem.step.level,
                                        p->sem.step.item,
                                        p->sem.step.item_res)));
                modified = true;
            }
        }
        break;

    case la_guide_step:
        if (is_cross (R(p))) {
            if (col_present (RL(p), p->sem.step.item)) {
                *p = *(cross_can (
                           RR(p),
                           project (guide_step (
                                        L(p),
                                        attach (RL(p),
                                                p->sem.step.iter,
                                                lit_nat(1)),
                                        p->sem.step.spec,
                                        p->sem.step.guide_count,
                                        p->sem.step.guides,
                                        p->sem.step.level,
                                        p->sem.step.iter,
                                        p->sem.step.item,
                                        p->sem.step.item_res),
                                    proj (p->sem.step.item_res,
                                          p->sem.step.item_res))));
            } else {
                *p = *(cross_can (
                           RL(p),
                           project (guide_step (
                                        L(p),
                                        attach (RR(p),
                                                p->sem.step.iter,
                                                lit_nat(1)),
                                        p->sem.step.spec,
                                        p->sem.step.guide_count,
                                        p->sem.step.guides,
                                        p->sem.step.level,
                                        p->sem.step.iter,
                                        p->sem.step.item,
                                        p->sem.step.item_res),
                                    proj (p->sem.step.item_res,
                                          p->sem.step.item_res))));
            }
            modified = true;
        }
        break;

    case la_guide_step_join:
        if (is_cross (R(p))) {
            bool switch_left = col_present (RL(p), p->sem.step.item);
            bool switch_right = col_present (RR(p), p->sem.step.item);

            if (switch_left) {
                *p = *(cross_can (guide_step_join (
                                        L(p), RL(p),
                                        p->sem.step.spec,
                                        p->sem.step.guide_count,
                                        p->sem.step.guides,
                                        p->sem.step.level,
                                        p->sem.step.item,
                                        p->sem.step.item_res),
                                  RR(p)));
                modified = true;
            }
            else if (switch_right) {
                *p = *(cross_can (RL(p),
                                  guide_step_join (
                                        L(p),
                                        RR(p),
                                        p->sem.step.spec,
                                        p->sem.step.guide_count,
                                        p->sem.step.guides,
                                        p->sem.step.level,
                                        p->sem.step.item,
                                        p->sem.step.item_res)));
                modified = true;
            }
        }
        break;

    case la_doc_index_join:
        if (is_cross (R(p))) {
            bool switch_left = col_present (RL(p), p->sem.doc_join.item) &&
                               col_present (RL(p), p->sem.doc_join.item_doc);
            bool switch_right = col_present (RR(p), p->sem.doc_join.item) &&
                                col_present (RR(p), p->sem.doc_join.item_doc);

            if (switch_left) {
                *p = *(cross_can (doc_index_join (
                                        L(p), RL(p),
                                        p->sem.doc_join.kind,
                                        p->sem.doc_join.item,
                                        p->sem.doc_join.item_res,
                                        p->sem.doc_join.item_doc,
                                        p->sem.doc_join.ns1,
                                        p->sem.doc_join.loc1,
                                        p->sem.doc_join.ns2,
                                        p->sem.doc_join.loc2),
                                  RR(p)));
                modified = true;
            }
            else if (switch_right) {
                *p = *(cross_can (RL(p),
                                  doc_index_join (
                                        L(p),
                                        RR(p),
                                        p->sem.doc_join.kind,
                                        p->sem.doc_join.item,
                                        p->sem.doc_join.item_res,
                                        p->sem.doc_join.item_doc,
                                        p->sem.doc_join.ns1,
                                        p->sem.doc_join.loc1,
                                        p->sem.doc_join.ns2,
                                        p->sem.doc_join.loc2))),
                modified = true;
            }
        }
        break;

    case la_doc_tbl:
        /* should not appear as roots already
           translates the doc_tbl operator. */
        break;

    case la_doc_access:
        if (is_cross (R(p))) {
            bool switch_left = col_present (RL(p), p->sem.doc_access.col);
            bool switch_right = col_present (RR(p), p->sem.doc_access.col);

            if (switch_left) {
                *p = *(cross_can (doc_access (L(p), RL(p),
                                        p->sem.doc_access.res,
                                        p->sem.doc_access.col,
                                        p->sem.doc_access.doc_col),
                                  RR(p)));
                modified = true;
            }
            else if (switch_right) {
                *p = *(cross_can (RL(p),
                                  doc_access (L(p), RR(p),
                                        p->sem.doc_access.res,
                                        p->sem.doc_access.col,
                                        p->sem.doc_access.doc_col)));
                modified = true;
            }
        }
        break;

    case la_twig:
    case la_fcns:
    case la_docnode:
    case la_element:
    case la_attribute:
    case la_textnode:
    case la_comment:
    case la_processi:
    case la_content:
    case la_merge_adjacent:
        /* constructors introduce like the unpartitioned
           rowid or rownum operators a dependency. */
        break;

    case la_roots:
        /* modify the only pattern starting in roots
           that is no constructor: roots-doc_tbl */
        if (L(p)->kind == la_doc_tbl) {
            if (is_cross (LL(p))) {
                if (col_present (L(LL(p)), L(p)->sem.doc_tbl.col)) {
                    PFla_op_t *other_side = R(LL(p));
                    /* overwrite doc_tbl node to update
                       both roots and frag operators */
                    *(L(p)) = *(doc_tbl (L(LL(p)),
                                         L(p)->sem.doc_tbl.res,
                                         L(p)->sem.doc_tbl.col,
                                         L(p)->sem.doc_tbl.kind));
                    /* push roots + doc_tbl through the cross product */
                    *p = *(cross_can (other_side, roots (L(p))));
                }
                else {
                    PFla_op_t *other_side = L(LL(p));
                    /* overwrite doc_tbl node to update
                       both roots and frag operators */
                    *(L(p)) = *(doc_tbl (R(LL(p)),
                                         L(p)->sem.doc_tbl.res,
                                         L(p)->sem.doc_tbl.col,
                                         L(p)->sem.doc_tbl.kind));
                    /* push roots + doc_tbl through the cross product */
                    *p = *(cross_can (other_side, roots (L(p))));
                }
                modified = true;
            }
            else if (LL(p)->kind == la_attach &&
                     LL(p)->sem.attach.res == L(p)->sem.doc_tbl.col) {
                /* save input relation */
                PFla_op_t *input = L(LL(p));

                /* create base table and overwrite doc_tbl
                   node to update both roots and frag operators */
                *(L(p)) = *(doc_tbl (lit_tbl (
                                         collist (L(p)->sem.doc_tbl.col),
                                         tuple (LL(p)->sem.attach.value)),
                                     L(p)->sem.doc_tbl.res,
                                     L(p)->sem.doc_tbl.col,
                                     L(p)->sem.doc_tbl.kind));

                /* push roots + doc_tbl through the cross product */
                *p = *(cross_can (input, roots (L(p))));
                modified = true;
            }
        }
        break;

    case la_fragment:
    case la_frag_extract:
    case la_frag_union:
    case la_empty_frag:
        break;

    case la_error: /* don't rewrite runtime errors */
    case la_nil:
    case la_cache: /* don't rewrite side effects */
    case la_trace: /* don't rewrite side effects */
        break;

    case la_trace_items:
    case la_trace_msg:
    case la_trace_map:
        /* we may not modify the cardinality */
    case la_rec_fix:
    case la_rec_param:
    case la_rec_arg:
    case la_rec_base:
        /* do not rewrite anything that has to do with recursion */
        break;

    case la_fun_call:
    case la_fun_param:
    case la_fun_frag_param:
        /* do not rewrite anything that has to do with function application */
        break;

    case la_proxy:
        /**
         * ATTENTION: The proxies (especially the kind=1 version) are
         * generated in such a way, that the following rewrite does not
         * introduce inconsistencies.
         * The following rewrite would break if the proxy contains operators
         * that are themselves not allowed to be rewritten by the multi-value
         * dependency optimization phase. We may not transform expressions that
         * rely on the cardinality of their inputs.
         *
         * In the current situation these operators are:
         * - aggregates (sum and count)
         * - rownum and rowid operators
         * - constructors (element, attribute, and textnode)
         * - fn:string-join
         *
         * By exploiting the semantics of our generated plans (at the creation
         * date of this rule) we ensure that none of the problematic operators
         * appear in the plans.
         *
         *               !!! BEWARE THIS IS NOT CHECKED !!!
         *
         * (A future redefinition of the translation scheme might break it.)
         *
         * Here are the reasons why it currently works without problems:
         * - aggregates (sum and count):
         *    The aggregates are only a problem if used without partitioning
         *    or if the partition criterion is not inferred from the rowid
         *    operator at the proxy exit. As an aggregate always uses the
         *    current scope (ensured during generation) and all scopes are
         *    properly nested, we have a functional dependency between the
         *    partitioning column of an aggregate inside this proxy
         *    (which is also equivalent to a scope) and the proxy exit rowid
         *    operator.
         *
         * - rownum and rowid operators:
         *    With a similar explanation as the one for aggregates we can be
         *    sure that every rownum and rowid operator inside the proxy is
         *    not used outside the proxy pattern. The generation process
         *    ensures that these are not used outside the scope or if used
         *    outside are partitioned by the numbers of the rowid operator at
         *    the proxy exit.
         *
         * - constructors (element, attribute, and textnode):
         *    Constructors are never allowed inside the proxy of kind=1.
         *    This is ensured by the fragment information that introduces
         *    conflicting references at the constructors -- they are linked
         *    from outside (via la_fragment) and inside (via la_roots) the
         *    proxy -- which cannot be resolved. This leads to the abortion
         *    of the proxy generation whenever a constructor appears inside
         *    the proxy.
         *
         * - fn:string-join
         *    The same scope arguments as for the aggregates apply.
         *
         */

        /**
         * We are looking for a proxy (of kind 1) followed by a cross product.
         * We check whether the proxy is independent of tree t1. If that's
         * the case we rewrite the DAG in Figure (1) into the one of Figure (2):
         *
         *                proxy (kind=1)              pi_above
         *                  |                          |
         *                 / \                         X
         *                /___\                    ___/ \___
         *                  |                     /         \
         *                  X                    pi_lcross  pi_rcross
         *                 / \                    |          |
         *                t1 t2                   t1       proxy (kind=1)
         *                                                   |
         *                ( 1 )                             / \
         *                                                 /___\
         *                                                   |
         *                                                  pi_below
         *                                                   |
         *                                                   t2
         *
         *                                            ( 2 )
         */
        if (is_cross (L(p->sem.proxy.base1)) &&
            p->sem.proxy.kind == 1) {
            PFla_op_t *cross = L(p->sem.proxy.base1);
            PFla_op_t *lcross, *rcross;
            unsigned int count = 0;
            bool rewrite = false;

            /* first check the dependencies of the left cross product input */
            for (i = 0; i < L(cross)->schema.count; i++)
                for (j = 0; j < clsize (p->sem.proxy.req_cols); j++)
                    if (L(cross)->schema.items[i].name
                        == clat (p->sem.proxy.req_cols, j)) {
                        count++;
                        break;
                    }
            if (clsize (p->sem.proxy.req_cols) == count) {
                lcross = L(cross);
                rcross = R(cross);
                rewrite = true;
            }
            else {
                count = 0;
                /* then check the dependencies of the right cross product
                   input */
                for (i = 0; i < R(cross)->schema.count; i++)
                    for (j = 0; j < clsize (p->sem.proxy.req_cols); j++)
                        if (R(cross)->schema.items[i].name
                            == clat (p->sem.proxy.req_cols, j)) {
                            count++;
                            break;
                        }
                if (clsize (p->sem.proxy.req_cols) == count) {
                    lcross = R(cross);
                    rcross = L(cross);
                    rewrite = true;
                }
            }

            if (rewrite) {
                unsigned int  lcount    = 0,
                              rcount    = 0;
                PFalg_col_t   dummy_col = lcross->schema.items[0].name,
                              cur;
                PFalg_proj_t *proj_below = PFmalloc (cross->schema.count *
                                                     sizeof (PFalg_proj_t));
                PFalg_proj_t *proj_above = PFmalloc (p->schema.count *
                                                     sizeof (PFalg_proj_t));
                PFalg_proj_t *proj_lcross = PFmalloc (p->schema.count *
                                                      sizeof (PFalg_proj_t));
                PFalg_proj_t *proj_rcross = PFmalloc (p->schema.count *
                                                      sizeof (PFalg_proj_t));

                /* Replace the right columns of the Cartesian product
                   by dummy columns (at the bottom of the proxy). */
                for (i = 0; i < cross->schema.count; i++) {
                    cur = cross->schema.items[i].name;
                    for (j = 0; j < lcross->schema.count; j++)
                        if (cur == lcross->schema.items[j].name) {
                            proj_below[i] = proj (cur, cur);
                            break;
                        }
                    if (j == lcross->schema.count)
                        proj_below[i] = proj (cur, dummy_col);
                }
                /* Replace the Cartesian product by a projection that
                   assigns the right cross product columns dummy columns
                   (to ensure that the plan stays consistent). */
                L(p->sem.proxy.base1) = PFla_project_ (lcross,
                                                       cross->schema.count,
                                                       proj_below);

                /* Split up the columns into a distinct list of left
                   and right cross product columns. If a column appears
                   in both children we keep the column in the right child.

                   Futhermore create a projection list that throws
                   away additional columns (introduced by the fallback
                   for a possibly empty projection list). */
                for (i = 0; i < p->schema.count; i++) {
                    cur = p->schema.items[i].name;
                    /* prefer right columns over left columns */
                    for (j = 0; j < rcross->schema.count; j++)
                        if (cur == rcross->schema.items[j].name) {
                            proj_rcross[rcount++] = proj (cur, cur);
                            break;
                        }
                    if (j == rcross->schema.count)
                        proj_lcross[lcount++] = proj (cur, cur);

                    proj_above[i] = proj (cur, cur);

                }
                /* Create a fallback projection list with a new name
                   to ensure that at least one column remains as input
                   for the cross product. */
                if (!lcount) {
                    cur = PFcol_new (col_iter);
                    proj_lcross[0] = proj (cur, p->schema.items[0].name);
                    lcount++;
                } else if (!rcount) {
                    cur = PFcol_new (col_iter);
                    proj_rcross[0] = proj (cur, rcross->schema.items[0].name);
                    rcount++;
                }

                /* Place the cross product on top of the proxy operator
                   and adjust all column names by additional projection. */
                *p = *(PFla_project_ (
                            cross_can (
                                PFla_project_ (
                                    /* make a copy of p */
                                    PFla_proxy (
                                        L(p),
                                        p->sem.proxy.kind,
                                        p->sem.proxy.ref,
                                        p->sem.proxy.base1,
                                        p->sem.proxy.req_cols,
                                        p->sem.proxy.new_cols),
                                    lcount, proj_lcross),
                                PFla_project_ (rcross, rcount, proj_rcross)),
                            p->schema.count, proj_above));

                modified = true;
                break;
            }
        }
        /* Remove proxy if a cross product wants to escape its focus. */
        else if (is_cross (L(p)) && p->sem.proxy.kind == 1) {
            *(p->sem.proxy.base1) = *dummy (L(p->sem.proxy.base1));
            *p = *(cross_can (LL(p), LR(p)));
            modified = true;
            break;
        }
        break;

    case la_proxy_base:
    case la_string_join:
        break;

    case la_dummy:
        if (is_cross (L(p))) {
            *p = *(cross_can (LL(p), LR(p)));
            modified = true;
            break;
        }
        break;
    }

    /* update cross_changes counter if
       cross product couldn't be propagated up the tree */
    if (cross_cross && !is_cross (p)) {
        cross_changes++;
        modified = true;
    } else if (cross_cross)
        cross_changes = 0;

    /* discard the subtree with directly following cross products */
    if (cross_changes >= max_cross_changes)
        modified = false;

    return modified;
}

/**
 * clean_up_cross replaces all clone column aware cross
 * product operators by normal cross products
 * and introduces additional project operators
 * whenever there are duplicate columns.
 */
static void
clean_up_cross (PFla_op_t *p)
{
    unsigned int i;

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply complex optimization for children */
    for (i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        clean_up_cross (p->child[i]);

    if (p->kind == la_internal_op)
        *p = *internal_op_to_cross (p);
}

/**
 * Re-route operators that reference rowid operators
 * (but effectively ignore them).
 */
static void
rowid_removal (PFla_op_t *p)
{
    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        rowid_removal (p->child[i]);

    /* re-route operators in case the underlying
       rowid operator is ignored by the DAG-branch. */
    if (L(p) && L(p)->kind == la_rowid &&
        !PFprop_icol_left (p->prop, L(p)->sem.rowid.res)) {
        L(p) = PFla_attach (LL(p), L(p)->sem.rowid.res, PFalg_lit_nat(1));
    }
    if (R(p) && R(p)->kind == la_rowid &&
        !PFprop_icol_right (p->prop, R(p)->sem.rowid.res)) {
        R(p) = PFla_attach (RL(p), R(p)->sem.rowid.res, PFalg_lit_nat(1));
    }
}

/**
 * Check if an operator is a proxy reference.
 */
static bool
is_ref (PFla_op_t *p)
{
    assert (refs);
    for (unsigned int i = 0; i < PFarray_last (refs); i++)
        if (*(PFla_op_t **) PFarray_at (refs, i) == p)
            return true;

    return false;
}

/**
 * Collect all proxy references.
 */
static void
collect_refs (PFla_op_t *p)
{
    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        collect_refs (p->child[i]);

    if (p->kind == la_proxy)
        *(PFla_op_t **) PFarray_add (refs) = p->sem.proxy.ref;
}

/**
 * Invoke algebra optimization.
 */
PFla_op_t *
PFalgopt_mvd (PFla_op_t *root, unsigned int noneffective_tries)
{
    /* Move rowid operators out of the way (to detect more patterns)
       if possible. */
    PFprop_infer_icol (root);
    rowid_removal (root);
    PFla_dag_reset (root);

    /* remove all common subexpressions to
       detect more patterns */
    PFla_cse (root);

    /* create a new reference list ... */
    refs = PFarray (sizeof (PFla_op_t *), 5);
    /* ... and fill it */
    collect_refs (root);
    PFla_dag_reset (root);

    /* Keep track of the ineffective
       cross product - cross product
       rewrites and stop if
       max_cross_changes is reached. */
    cross_changes = 0;
    max_cross_changes = noneffective_tries;

    /* Optimize algebra tree */
    while (opt_mvd (root))
        PFla_dag_reset (root);

    PFla_dag_reset (root);

    /* Infer cardinality property first */
    PFprop_infer_card (root);

    /* replace clone column aware cross
       products by normal ones */
    clean_up_cross (root);
    PFla_dag_reset (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
