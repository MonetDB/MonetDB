/**
 * @file
 *
 * Optimize relational algebra expression DAG
 * based on the constant property.
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
#include <string.h>

#include "algopt.h"
#include "properties.h"
#include "alg_dag.h"
#include "mem.h"          /* PFmalloc() */

/** mnemonic algebra constructors */
#include "logical_mnemonic.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

#define SEEN(p) ((p)->bit_dag)

/**
 * Adds a superfluous attach node. An additional projection
 * operator ensures that the attached column is projected away
 * before. In combination with other optimizations this rewrite
 * ensures that constants are introduced at the latest point in
 * the plan (previous unreferenced appearances will be removed).
 */
static PFla_op_t *
add_attach (PFla_op_t *p, PFalg_col_t col, PFalg_atom_t value)
{
    PFla_op_t *rel;
    unsigned int count = 0;
    PFalg_proj_t *cols = PFmalloc ((p->schema.count - 1)
                                    * sizeof (PFalg_proj_t));

    /* avoid attach if @a col is the only column */
    if (p->schema.count == 1) return p;

    for (unsigned int i = 0; i < p->schema.count; i++)
        if (p->schema.items[i].name != col)
            cols[count++] = PFalg_proj (p->schema.items[i].name,
                                        p->schema.items[i].name);

    rel = PFla_attach (PFla_project_ (p, count, cols), col, value);

    return rel;
}

/* worker for PFalgopt_const */
static void
opt_const_attach (PFla_op_t *p)
{
    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply constant optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_const_attach (p->child[i]);

    /* action code */
    switch (p->kind) {
        case la_serialize_seq:
            /* introduce attach if necessary */
            if (PFprop_const_right (p->prop, p->sem.ser_seq.pos)) {
                R(p) = add_attach (R(p), p->sem.ser_seq.pos,
                                   PFprop_const_val_right (
                                       p->prop,
                                       p->sem.ser_seq.pos));
            }
            if (PFprop_const_right (p->prop, p->sem.ser_seq.item)) {
                R(p) = add_attach (R(p), p->sem.ser_seq.item,
                                   PFprop_const_val_right (
                                       p->prop,
                                       p->sem.ser_seq.item));
            }
            break;


        case la_serialize_rel:
        {
            /* Introduce (superfluous) attach-ops for constant columns.
               This rewrite ensures, that constants are introduced at
               the latest possible point in the plan.
            */

            /* Rewrite for the iter-column. */
            if (PFprop_const_right (p->prop, p->sem.ser_rel.iter)) {
                R(p) = add_attach (R(p), p->sem.ser_rel.iter,
                                   PFprop_const_val_right (
                                       p->prop,
                                       p->sem.ser_rel.iter));

            }

            /* Rewrite for the pos-column. */
            if (PFprop_const_right (p->prop, p->sem.ser_rel.pos)) {
                R(p) = add_attach (R(p), p->sem.ser_rel.pos,
                                   PFprop_const_val_right (
                                       p->prop,
                                       p->sem.ser_rel.pos));
            }

            /* Rewrite for the item-columns. */
            PFalg_collist_t *items = p->sem.ser_rel.items;
            for (unsigned int i = 0; i < clsize (items); i++)
            {
                PFalg_col_t item = clat (items, i);
                if (PFprop_const_right (p->prop, item)) {
                    R(p) = add_attach (R(p), item,
                                       PFprop_const_val_right (
                                           p->prop,
                                           item));
                }
            }
        }   break;

        case la_fun_1to1:
            /* introduce attach if necessary */
            for (unsigned int i = 0; i < clsize (p->sem.fun_1to1.refs); i++)
                if (PFprop_const (p->prop, clat (p->sem.fun_1to1.refs, i)))
                    L(p) = add_attach (L(p), clat (p->sem.fun_1to1.refs, i),
                                       PFprop_const_val (
                                           p->prop,
                                           clat (p->sem.fun_1to1.refs, i)));
            break;

        case la_num_eq:
        case la_num_gt:   /* possible extensions for 'and' and 'or': */
        case la_bool_and: /* if one arg is const && false replace by project */
        case la_bool_or:  /* if one arg is const && true replace by project */
            /* introduce attach if necessary */
            if (PFprop_const (p->prop, p->sem.binary.col1)) {
                L(p) = add_attach (L(p), p->sem.binary.col1,
                                   PFprop_const_val (p->prop,
                                                     p->sem.binary.col1));
            }
            if (PFprop_const (p->prop, p->sem.binary.col2)) {
                L(p) = add_attach (L(p), p->sem.binary.col2,
                                   PFprop_const_val (p->prop,
                                                     p->sem.binary.col2));
            }
            break;

        case la_bool_not:
            /* introduce attach if necessary */
            if (PFprop_const (p->prop, p->sem.unary.col)) {
                L(p) = add_attach (L(p), p->sem.unary.col,
                                   PFprop_const_val (p->prop,
                                                     p->sem.unary.col));
            }
            break;

        case la_aggr:
            /* introduce attach if necessary */
            if (p->sem.aggr.part &&
                PFprop_const_left (p->prop, p->sem.aggr.part)) {
                L(p) = add_attach (L(p), p->sem.aggr.part,
                                   PFprop_const_val_left (p->prop,
                                                          p->sem.aggr.part));
            }
            /* introduce attach if necessary */
            for (unsigned int i = 0; i < p->sem.aggr.count; i++)
                if (p->sem.aggr.aggr[i].col &&
                    PFprop_const_left (p->prop, p->sem.aggr.aggr[i].col))
                    L(p) = add_attach (L(p),
                                       p->sem.aggr.aggr[i].col,
                                       PFprop_const_val_left (
                                           p->prop,
                                           p->sem.aggr.aggr[i].col));
            break;

        case la_type:
        case la_cast:
            /* introduce attach if necessary */
            if (PFprop_const_left (p->prop, p->sem.type.col)) {
                L(p) = add_attach (L(p), p->sem.type.col,
                                   PFprop_const_val_left (
                                       p->prop,
                                       p->sem.type.col));
            }
            break;

        default:
            break;
    }
}

/* worker for PFalgopt_const */
static void
opt_const (PFla_op_t *p)
{
    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply constant optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_const (p->child[i]);

    /* action code */
    switch (p->kind) {
        case la_eqjoin:
        {   /**
             * If both join columns are constant
             * we can replace the eqjoin by one of the
             * following expressions:
             *
             * (a) if the comparison of col1 and col2
             *     is stable regarding different implementations
             *     (e.g. comparisons on integer, boolean, or
             *      native numbers)
             *
             *      case true:         case false:
             *
             *          X               empty_tbl
             *         / \
             *      Rel1 Rel2
             *
             * (b) otherwise      X
             *                  /   \
             *           ------/     \------
             *          /                   \
             * project(schema(Rel1)) project(schema(Rel2))
             *         |                     |
             *     select (new)          select (new)
             *         |                     |
             *  = new:<col1,col2>    = new:<col2,col1>
             *         |                     |
             *      @ col2                @ col1
             *         |                     |
             *       Rel1                  Rel2
             */
            PFalg_col_t col1, col2;
            col1 = p->sem.eqjoin.col1;
            col2 = p->sem.eqjoin.col2;

            if (PFprop_const_left (p->prop, col1) &&
                PFprop_const_right (p->prop, col2)) {
                PFla_op_t *ret, *left, *right;
                PFalg_proj_t *left_cols, *right_cols;
                PFalg_col_t res = PFcol_new (col_item);
                PFalg_simple_type_t ty;

                ty = (PFprop_const_val (p->prop, col1)).type;

                /* (a) the comparison between col1 and col2 is stable */
                if (ty == aat_nat || ty == aat_int ||
                    ty == aat_bln || ty == aat_qname) {

                    if (!PFalg_atom_cmp (
                             PFprop_const_val (p->prop, col1),
                             PFprop_const_val (p->prop, col2))) {
                        /* replace join by cross product */
                        ret = PFla_cross (L(p), R(p));
                    } else {
                        ret = PFla_empty_tbl_ (p->schema);
                    }
                    *p = *ret;
                    SEEN(p) = true;
                    break;
                }
                /* (b) otherwise: */

                /* prepare projection list for left subtree */
                left_cols = PFmalloc (L(p)->schema.count
                                      * sizeof (PFalg_proj_t));

                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    left_cols[i] = PFalg_proj (L(p)->schema.items[i].name,
                                               L(p)->schema.items[i].name);

                /* generate modified left subtree */
                left = PFla_project_ (
                           PFla_select (
                               PFla_eq (
                                   PFla_attach (
                                       L(p), col2,
                                       PFprop_const_val_right (
                                           p->prop, col2)),
                                   res, col1, col2),
                               res),
                           L(p)->schema.count,
                           left_cols);

                /* prepare projection list for right subtree */
                right_cols = PFmalloc (L(p)->schema.count
                                       * sizeof (PFalg_proj_t));

                for (unsigned int i = 0; i < R(p)->schema.count; i++)
                    right_cols[i] = PFalg_proj (R(p)->schema.items[i].name,
                                                R(p)->schema.items[i].name);

                /* generate modified right subtree */
                right = PFla_project_ (
                            PFla_select (
                                PFla_eq (
                                    PFla_attach (
                                        R(p), col1,
                                        PFprop_const_val_left (
                                            p->prop, col1)),
                                    res, col1, col2),
                                res),
                            R(p)->schema.count,
                            right_cols);

                /* combine both subtrees */
                ret = PFla_cross (left, right);

                *p = *ret;
                SEEN(p) = true;
            }
            /**
             * If one join column (here col1) is constant
             * we can replace the eqjoin by the following expression:
             *
             *               X
             *             /   \
             *            /     \
             *         Rel1   project (schema(Rel2)
             *                   |
             *                select (new)
             *                   |
             *                   = new:<col2,col1>
             *                   |
             *                   @ col1
             *                   |
             *                 Rel2
             */
            else if (PFprop_const_left (p->prop, col1)) {
                PFla_op_t *right;
                PFalg_proj_t *right_cols;
                PFalg_col_t res = PFcol_new (col_item);

                /* prepare projection list for right subtree */
                right_cols = PFmalloc (R(p)->schema.count
                                       * sizeof (PFalg_proj_t));

                for (unsigned int i = 0; i < R(p)->schema.count; i++)
                    right_cols[i] = PFalg_proj (R(p)->schema.items[i].name,
                                                R(p)->schema.items[i].name);

                /* generate modified right subtree */
                right = PFla_project_ (
                            PFla_select (
                                PFla_eq (
                                    PFla_attach (
                                        R(p), col1,
                                        PFprop_const_val_left (
                                            p->prop, col1)),
                                    res, col1, col2),
                                res),
                            R(p)->schema.count,
                            right_cols);

                /* combine both subtrees */
                *p = *PFla_cross (L(p), right);

                SEEN(p) = true;
            }
            else if (PFprop_const_right (p->prop, col2)) {
                PFla_op_t *left;
                PFalg_proj_t *left_cols;
                PFalg_col_t res = PFcol_new (col_item);

                /* prepare projection list for left subtree */
                left_cols = PFmalloc (L(p)->schema.count
                                      * sizeof (PFalg_proj_t));

                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    left_cols[i] = PFalg_proj (L(p)->schema.items[i].name,
                                               L(p)->schema.items[i].name);

                /* generate modified left subtree */
                left = PFla_project_ (
                           PFla_select (
                               PFla_eq (
                                   PFla_attach (
                                       L(p), col2,
                                       PFprop_const_val_right (
                                           p->prop, col2)),
                                   res, col1, col2),
                               res),
                           L(p)->schema.count,
                           left_cols);

                /* combine both subtrees */
                *p = *PFla_cross (left, R(p));

                SEEN(p) = true;
            }
        }   break;

        case la_thetajoin:
            /* simple variant that can be extended much more if necessary */
            if (p->sem.thetajoin.count > 1) {
                PFalg_sel_t *pred = p->sem.thetajoin.pred;
                PFalg_col_t  l,
                             r;
                for (unsigned int i = 0; i < p->sem.thetajoin.count; i++) {
                    l = pred[i].left;
                    r = pred[i].right;
                    /* check if the predicate is an equality predicate
                       on constant and matching columns */
                    if (pred[i].comp == alg_comp_eq &&
                        PFprop_const_left (p->prop, l) &&
                        PFprop_const_right (p->prop, r) &&
                        PFalg_atom_comparable (
                            PFprop_const_val_left (p->prop, l),
                            PFprop_const_val_right (p->prop, r)) &&
                        !PFalg_atom_cmp (
                            PFprop_const_val_left (p->prop, l),
                            PFprop_const_val_right (p->prop, r)) &&
                        p->sem.thetajoin.count > 1) {
                        /* remove the current predicate
                           (by overwriting it with the last predicate) */
                        pred[i] = pred[p->sem.thetajoin.count-1];
                        i--;
                        p->sem.thetajoin.count--;
                    }
                }
            }
            break;

        case la_select:
            /**
             * if select criterium is constant we have two options:
             * - true : remove select
             * - false: replace select by an empty_tbl
             */
            if (PFprop_const_left (p->prop, p->sem.select.col)) {
                if (PFprop_const_val_left (p->prop,
                                           p->sem.select.col).val.bln) {
                    *p = *PFla_dummy (L(p));
                    break;
                } else {
                    *p = *PFla_empty_tbl_ (p->schema);
                    SEEN(p) = true;
                    break;
                }
            }
            break;

        case la_disjunion:
            /* match unpartitioned count pattern
               (where the partitioning criterion is constant)
                             |
                             U
                     _______/ \_______
                    /                 \
                   pi_item           pi_item
                    |                 |
                    |                 @item:0
                    |                 |
                    |                diff
                    |               /   \
                    |             loop  pi_iter
                    |                    |
                    \_______   _________/
                            \ /
                           count_item:/iter
                             |

               and replace it by an unpartitioned count */
            if (p->schema.count == 1 &&
                L(p)->kind == la_project &&
                LL(p)->kind == la_aggr &&
                LL(p)->sem.aggr.count == 1 &&
                LL(p)->sem.aggr.aggr[0].kind == alg_aggr_count &&
                LL(p)->sem.aggr.part &&
                PFprop_const (LL(p)->prop, LL(p)->sem.aggr.part) &&
                L(p)->sem.proj.items[0].old == LL(p)->sem.aggr.aggr[0].res &&
                R(p)->kind == la_project &&
                RL(p)->kind == la_attach &&
                R(p)->sem.proj.items[0].old == RL(p)->sem.attach.res &&
                RL(p)->sem.attach.value.type == aat_int &&
                RL(p)->sem.attach.value.val.int_ == 0 &&
                RLL(p)->kind == la_difference &&
                RLL(p)->schema.count == 1 &&
                RLLR(p)->kind == la_project &&
                RLLR(p)->sem.proj.items[0].old ==
                LL(p)->sem.aggr.part &&
                L(RLLR(p)) == LL(p)) {
                PFalg_aggr_t aggr = PFalg_aggr (alg_aggr_count,
                                                R(p)->sem.proj.items[0].old,
                                                col_NULL);

                /* check that the values in the loop are constant
                   and provide the same value */
                assert (PFprop_const (RLLL(p)->prop,
                                      RLLR(p)->sem.proj.items[0].new));
                assert (
                    PFalg_atom_comparable (
                        PFprop_const_val (RLLL(p)->prop,
                                          RLLR(p)->sem.proj.items[0].new),
                        PFprop_const_val (LL(p)->prop,
                                          LL(p)->sem.aggr.part)) &&
                    !PFalg_atom_cmp (
                        PFprop_const_val (RLLL(p)->prop,
                                          RLLR(p)->sem.proj.items[0].new),
                        PFprop_const_val (LL(p)->prop,
                                          LL(p)->sem.aggr.part)));

                /* To avoid creating more results than allowed (1 instead
                   of 0) we have to make sure that we take the cardinality
                   of the loop relation into account. */
                *p = *project (cross (RLLL(p), /* loop */
                                      PFla_aggr (LLL(p),
                                                 col_NULL,
                                                 1,
                                                 &aggr)),
                               proj (R(p)->sem.proj.items[0].new,
                                     R(p)->sem.proj.items[0].old));
                break;
            }
            break;

/* as this rule does not cope with empty sequences correctly it is disabled */
#if 0
        case la_difference:
        {   /**
             * difference can be pruned if all columns of both operands
             * are constant and the comparison between the single
             * columns are stable (in respect to the implementations)
             * e.g. type integer or native.
             *
             * If all columns contain the same values the difference
             * can be replaced by an empty literal table. Otherwise
             * no rows in the left argument get pruned and the
             * difference can be discarded.
             */
            PFalg_col_t col;
            bool all_const = true;
            bool all_match = true;

            for (unsigned int i = 0; i < p->schema.count; i++) {
                col = p->schema.items[i].name;
                all_const = all_const &&
                            PFprop_const_left (p->prop, col) &&
                            PFprop_const_right (p->prop, col) &&
                            (PFprop_const_val_left (p->prop,
                                                    col)).type == aat_nat &&
                            (PFprop_const_val_right (p->prop,
                                                     col)).type == aat_nat;
                if (all_const)
                    all_match = all_match &&
                                !PFalg_atom_cmp (
                                     PFprop_const_val_left (p->prop, col),
                                     PFprop_const_val_right (p->prop, col));
            }

            if (all_const) {
                if (all_match) {
                    /* for each tuple in the left argument there
                       exists one in the right argument - thus
                       return empty_tbl */
                    *p = *PFla_empty_tbl_ (p->schema);
                    SEEN(p) = true;
                } else {
                    /* we have no matches -- thus the left argument
                       remains unchanged and the difference is superfluous */
                    *p = *PFla_dummy (L(p));
                }
            }
        }   break;
#endif

        case la_distinct:
        {
            unsigned int i, count;

            /* check number of non-constant columns */
            for (count = 0, i = 0; i < p->schema.count; i++)
                if (!PFprop_const (p->prop, p->schema.items[i].name))
                    count++;

            if (count && count < p->schema.count) {
                /* to avoid distinct on constant columns remove all constant
                columns before applying distinct and attach them afterwards
                again. */
                PFla_op_t *ret;
                PFalg_proj_t *cols = PFmalloc (count * sizeof (PFalg_proj_t));

                for (count = 0, i = 0; i < p->schema.count; i++)
                    if (!PFprop_const (p->prop, p->schema.items[i].name))
                        cols[count++] = PFalg_proj (p->schema.items[i].name,
                                                    p->schema.items[i].name);

                /* introduce project before distinct */
                ret = PFla_distinct (PFla_project_ (L(p), count, cols));

                /* attach constant columns afterwards */
                for (i = 0; i < p->schema.count; i++)
                    if (PFprop_const (p->prop, p->schema.items[i].name))
                        ret = PFla_attach (ret,
                                           p->schema.items[i].name,
                                           PFprop_const_val (
                                               p->prop,
                                               p->schema.items[i].name));

                *p = *ret;
                SEEN(p) = true;
            }
/* as this rule does not cope with empty sequences correctly it is disabled */
#if 0
            else if (!count) {
                /* as all columns are constant, the distinct operator
                   can be replaced by lit_tbl of the current schema
                   whose single row contains the constant values */
                PFalg_collist_t *collist = PFalg_collist (p->schema.count);
                PFalg_atom_t *atoms = PFmalloc (p->schema.count *
                                                sizeof (PFalg_atom_t));

                for (i = 0; i < p->schema.count; i++) {
                    cladd (collist) = p->schema.items[i].name;
                    atoms[i] = PFprop_const_val (p->prop,
                                                 p->schema.items[i].name);
                }
                *p = *PFla_lit_tbl (collist, PFalg_tuple_ (i, atoms));

                SEEN(p) = true;
            }
#endif
        } break;

        case la_fun_1to1:
            /* rewrites for fn:contains ($arg1, $arg2) */
            /* If the value of $arg2 is the zero-length string,
               then the function returns true. */
            if (p->sem.fun_1to1.kind == alg_fun_fn_contains &&
                PFprop_const (p->prop, clat (p->sem.fun_1to1.refs, 1)) &&
                !strcmp (PFprop_const_val (
                             p->prop,
                             clat (p->sem.fun_1to1.refs, 1)).val.str, "")) {
                *p = *PFla_attach (L(p), p->sem.fun_1to1.res,
                                   PFalg_lit_bln (true));
                break;
            }
            /* If the value of $arg1 is the zero-length string,
               then the function returns false. */
            if (p->sem.fun_1to1.kind == alg_fun_fn_contains &&
                PFprop_const (p->prop, clat (p->sem.fun_1to1.refs, 0)) &&
                !strcmp (PFprop_const_val (
                             p->prop,
                             clat (p->sem.fun_1to1.refs, 0)).val.str, "") &&
                PFprop_const (p->prop, clat (p->sem.fun_1to1.refs, 1)) &&
                strcmp (PFprop_const_val (
                            p->prop,
                            clat (p->sem.fun_1to1.refs, 1)).val.str, "")) {
                *p = *PFla_attach (L(p), p->sem.fun_1to1.res,
                                   PFalg_lit_bln (false));
                break;
            }
            /* concatenate two constant strings */
            if (p->sem.fun_1to1.kind == alg_fun_fn_concat &&
                PFprop_const (p->prop, clat (p->sem.fun_1to1.refs, 0)) &&
                PFprop_const (p->prop, clat (p->sem.fun_1to1.refs, 1))) {
                char *str0 = PFprop_const_val (
                                 p->prop,
                                 clat (p->sem.fun_1to1.refs, 0)).val.str,
                     *str1 = PFprop_const_val (
                                 p->prop,
                                 clat (p->sem.fun_1to1.refs, 1)).val.str,
                     *s;
                unsigned int count = strlen (str0) + strlen (str1) + 1;

                /* try to avoid constructing two long strings */
                if (count < 2048) {
                    s = (char *) PFmalloc (count);
               
                    strcat (strcpy (s, str0), str1);

                    *p = *PFla_attach (L(p), p->sem.fun_1to1.res,
                                       PFalg_lit_str (s));
                    break;
                }
            }
            break;

        case la_num_eq:
            if (PFprop_const (p->prop, p->sem.binary.col1) &&
                PFprop_const (p->prop, p->sem.binary.col2)) {
                PFalg_atom_t val1, val2;
                val1 = PFprop_const_val (p->prop, p->sem.binary.col1);
                val2 = PFprop_const_val (p->prop, p->sem.binary.col2);
                if (val1.type == val2.type && val1.type == aat_int) {
                    *p = *PFla_attach (L(p), p->sem.binary.res,
                                       PFalg_lit_bln (
                                           val1.val.int_ == val2.val.int_));
                    break;
                }
            }
            break;

        case la_aggr:
            for (unsigned int i = 0; i < p->sem.aggr.count; i++)
                if (PFprop_const_left (p->prop, p->sem.aggr.aggr[i].col))
                    switch (p->sem.aggr.aggr[i].kind) {
                        case alg_aggr_avg:
                        case alg_aggr_max:
                        case alg_aggr_min:
                        case alg_aggr_all:
                            p->sem.aggr.aggr[i].kind = alg_aggr_dist;
                            break;

                        default:
                            break;
                    }
            break;

        case la_rownum:
        {
            /* discard all sort criterions that are constant */
            PFord_ordering_t sortby = PFordering ();

            for (unsigned int i = 0;
                 i < PFord_count (p->sem.sort.sortby);
                 i++)
                if (!PFprop_const (p->prop,
                                   PFord_order_col_at (
                                       p->sem.sort.sortby, i)))
                    sortby = PFord_refine (
                                sortby,
                                PFord_order_col_at (
                                    p->sem.sort.sortby,
                                    i),
                                PFord_order_dir_at (
                                    p->sem.sort.sortby,
                                    i));

            p->sem.sort.sortby = sortby;

            /* only include partitioning column if it is not constant */
            if (p->sem.sort.part &&
                PFprop_const (p->prop, p->sem.sort.part))
                p->sem.sort.part = col_NULL;

        }   break;

        case la_rowrank:
        case la_rank:
        {
            /* discard all sort criterions that are constant */
            PFord_ordering_t sortby = PFordering ();

            for (unsigned int i = 0;
                 i < PFord_count (p->sem.sort.sortby);
                 i++)
                if (!PFprop_const (p->prop,
                                   PFord_order_col_at (
                                       p->sem.sort.sortby, i)))
                    sortby = PFord_refine (
                                sortby,
                                PFord_order_col_at (
                                    p->sem.sort.sortby,
                                    i),
                                PFord_order_dir_at (
                                    p->sem.sort.sortby,
                                    i));

            p->sem.sort.sortby = sortby;

            /* replace rank operator if it has no rank criterion anymore */
            if (PFord_count (sortby) == 0)
                *p = *PFla_attach (L(p), p->sem.sort.res, PFalg_lit_int (1));
        }   break;

        default:
            break;
    }
}

/**
 * Invoke algebra optimization.
 */
PFla_op_t *
PFalgopt_const (PFla_op_t *root, bool no_attach)
{
    /* Infer constant properties first */
    PFprop_infer_const (root);

    if (!no_attach) {
        /* Introduce attach operators */
        opt_const_attach (root);
        PFla_dag_reset (root);
    }

    /* Optimize algebra tree */
    opt_const (root);
    PFla_dag_reset (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
/* vim:set foldmarker=#if,#endif foldmethod=marker foldopen-=search: */
