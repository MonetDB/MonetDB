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

#define SEEN(p) ((p)->bit_dag)

/**
 * Adds a superfluous attach node. An additional projection
 * operator ensures that the attached attribute is projected away
 * before. In combination with other optimizations this rewrite
 * ensures that constants are introduced at the latest point in
 * the plan (previous unreferenced appearances will be removed).
 */
static PFla_op_t *
add_attach (PFla_op_t *p, PFalg_att_t att, PFalg_atom_t value)
{
    PFla_op_t *rel;
    unsigned int count = 0;
    PFalg_proj_t *atts = PFmalloc ((p->schema.count - 1)
                                    * sizeof (PFalg_proj_t));

    /* avoid attach if @a att is the only column */
    if (p->schema.count == 1) return p;

    for (unsigned int i = 0; i < p->schema.count; i++)
        if (p->schema.items[i].name != att)
            atts[count++] = PFalg_proj (p->schema.items[i].name,
                                        p->schema.items[i].name);

    rel = PFla_attach (PFla_project_ (p, count, atts), att, value);

    return rel;
}

/* worker for PFalgopt_const */
static void
opt_const (PFla_op_t *p, bool no_attach)
{
    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply constant optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_const (p->child[i], no_attach);

    /* avoid applying rules that only introduce new attach
       nodes (e.g. during second constant optimzation run)*/
    if (no_attach)
        switch (p->kind) {
            case la_eqjoin:
            case la_select:
            case la_disjunion:
            case la_difference:
            case la_distinct:
            case la_num_eq:
            case la_avg:
            case la_max:
            case la_min:
            case la_rownum:
            case la_rank:
            case la_error:
            case la_cond_err:
                /* these rules apply a 'real rewrite'
                   and therefore continue */
                break;
            default:
                return;
        }

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

        case la_eqjoin:
        {   /**
             * If both join columns are constant
             * we can replace the eqjoin by one of the
             * following expressions:
             *
             * (a) if the comparison of att1 and att2
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
             *  = new:<att1,att2>    = new:<att2,att1>
             *         |                     |
             *      @ att2                @ att1
             *         |                     |
             *       Rel1                  Rel2
             */
            PFalg_att_t att1, att2;
            att1 = p->sem.eqjoin.att1;
            att2 = p->sem.eqjoin.att2;

            if (PFprop_const_left (p->prop, att1) &&
                PFprop_const_right (p->prop, att2)) {
                PFla_op_t *ret, *left, *right;
                PFalg_proj_t *left_atts, *right_atts;
                PFalg_att_t res;
                PFalg_simple_type_t ty;

                ty = (PFprop_const_val (p->prop, att1)).type;

                /* (a) the comparison between att1 and att2 is stable */
                if (ty == aat_nat || ty == aat_int ||
                    ty == aat_bln || ty == aat_qname) {

                    if (!PFalg_atom_cmp (
                             PFprop_const_val (p->prop, att1),
                             PFprop_const_val (p->prop, att2))) {
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

                /* find unused attribute */
                for (res = 1 << 30; res > 0; res >>= 1)
                    if (!PFprop_ocol (p, res)) break;

                /* prepare projection list for left subtree */
                left_atts = PFmalloc (L(p)->schema.count
                                      * sizeof (PFalg_proj_t));

                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    left_atts[i] = PFalg_proj (L(p)->schema.items[i].name,
                                               L(p)->schema.items[i].name);

                /* generate modified left subtree */
                left = PFla_project_ (
                           PFla_select (
                               PFla_eq (
                                   PFla_attach (
                                       L(p), att2,
                                       PFprop_const_val_right (
                                           p->prop, att2)),
                                   res, att1, att2),
                               res),
                           L(p)->schema.count,
                           left_atts);

                /* prepare projection list for right subtree */
                right_atts = PFmalloc (L(p)->schema.count
                                       * sizeof (PFalg_proj_t));

                for (unsigned int i = 0; i < R(p)->schema.count; i++)
                    right_atts[i] = PFalg_proj (R(p)->schema.items[i].name,
                                                R(p)->schema.items[i].name);

                /* generate modified right subtree */
                right = PFla_project_ (
                            PFla_select (
                                PFla_eq (
                                    PFla_attach (
                                        R(p), att1,
                                        PFprop_const_val_left (
                                            p->prop, att1)),
                                    res, att1, att2),
                                res),
                            R(p)->schema.count,
                            right_atts);

                /* combine both subtrees */
                ret = PFla_cross (left, right);

                *p = *ret;
                SEEN(p) = true;
            }
            /**
             * If one join column (here att1) is constant
             * we can replace the eqjoin by the following expression:
             *
             *               X
             *             /   \
             *            /     \
             *         Rel1   project (schema(Rel2)
             *                   |
             *                select (new)
             *                   |
             *                   = new:<att2,att1>
             *                   |
             *                   @ att1
             *                   |
             *                 Rel2
             */
            else if (PFprop_const_left (p->prop, att1)) {
                PFla_op_t *right;
                PFalg_proj_t *right_atts;
                PFalg_att_t res;

                /* find unused attribute */
                for (res = 1 << 30; res > 0; res >>= 1)
                    if (!PFprop_ocol (p, res)) break;

                /* prepare projection list for right subtree */
                right_atts = PFmalloc (R(p)->schema.count
                                       * sizeof (PFalg_proj_t));

                for (unsigned int i = 0; i < R(p)->schema.count; i++)
                    right_atts[i] = PFalg_proj (R(p)->schema.items[i].name,
                                                R(p)->schema.items[i].name);

                /* generate modified right subtree */
                right = PFla_project_ (
                            PFla_select (
                                PFla_eq (
                                    PFla_attach (
                                        R(p), att1,
                                        PFprop_const_val_left (
                                            p->prop, att1)),
                                    res, att1, att2),
                                res),
                            R(p)->schema.count,
                            right_atts);

                /* combine both subtrees */
                *p = *PFla_cross (L(p), right);

                SEEN(p) = true;
            }
            else if (PFprop_const_right (p->prop, att2)) {
                PFla_op_t *left;
                PFalg_proj_t *left_atts;
                PFalg_att_t res;

                /* find unused attribute */
                for (res = 1 << 30; res > 0; res >>= 1)
                    if (!PFprop_ocol (p, res)) break;

                /* prepare projection list for left subtree */
                left_atts = PFmalloc (L(p)->schema.count
                                      * sizeof (PFalg_proj_t));

                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    left_atts[i] = PFalg_proj (L(p)->schema.items[i].name,
                                               L(p)->schema.items[i].name);

                /* generate modified left subtree */
                left = PFla_project_ (
                           PFla_select (
                               PFla_eq (
                                   PFla_attach (
                                       L(p), att2,
                                       PFprop_const_val_right (
                                           p->prop, att2)),
                                   res, att1, att2),
                               res),
                           L(p)->schema.count,
                           left_atts);

                /* combine both subtrees */
                *p = *PFla_cross (left, R(p));

                SEEN(p) = true;
            }
        }   break;

        case la_select:
            /**
             * if select criterium is constant we have two options:
             * - true : remove select
             * - false: replace select by an empty_tbl
             */
            if (PFprop_const_left (p->prop, p->sem.select.att)) {
                if (PFprop_const_val_left (p->prop,
                                           p->sem.select.att).val.bln) {
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
                L(L(p))->kind == la_count &&
                PFprop_const (L(L(p))->prop, L(L(p))->sem.aggr.part) &&
                R(p)->kind == la_project &&
                L(R(p))->kind == la_attach &&
                L(R(p))->sem.attach.value.type == aat_int &&
                L(R(p))->sem.attach.value.val.int_ == 0 &&
                L(L(R(p)))->kind == la_difference &&
                R(L(L(R(p))))->kind == la_project &&
                R(L(L(R(p))))->sem.proj.items[0].new ==
                L(L(p))->sem.aggr.part &&
                L(R(L(L(R(p))))) == L(L(p))) {

                /* check that the values in the loop are constant
                   and provide the same value */
                assert (PFprop_const (L(L(L(R(p))))->prop,
                                      L(L(p))->sem.aggr.part));
                assert (
                    PFalg_atom_comparable (
                        PFprop_const_val (L(L(L(R(p))))->prop,
                                          L(L(p))->sem.aggr.part),
                        PFprop_const_val (L(L(p))->prop,
                                          L(L(p))->sem.aggr.part)) &&
                    !PFalg_atom_cmp (
                        PFprop_const_val (L(L(L(R(p))))->prop,
                                          L(L(p))->sem.aggr.part),
                        PFprop_const_val (L(L(p))->prop,
                                          L(L(p))->sem.aggr.part)));

                *p = *PFla_count (L(L(L(p))),
                                  p->schema.items[0].name,
                                  att_NULL);
                break;
            }
            break;

        case la_difference:
            /* as this rule does not cope with empty sequences
               correctly it is disabled */
            break;
#if 0
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
            PFalg_att_t att;
            bool all_const = true;
            bool all_match = true;

            for (unsigned int i = 0; i < p->schema.count; i++) {
                att = p->schema.items[i].name;
                all_const = all_const &&
                            PFprop_const_left (p->prop, att) &&
                            PFprop_const_right (p->prop, att) &&
                            (PFprop_const_val_left (p->prop,
                                                    att)).type == aat_nat &&
                            (PFprop_const_val_right (p->prop,
                                                     att)).type == aat_nat;
                if (all_const)
                    all_match = all_match &&
                                !PFalg_atom_cmp (
                                     PFprop_const_val_left (p->prop, att),
                                     PFprop_const_val_right (p->prop, att));
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
                PFalg_proj_t *atts = PFmalloc (count * sizeof (PFalg_proj_t));

                for (count = 0, i = 0; i < p->schema.count; i++)
                    if (!PFprop_const (p->prop, p->schema.items[i].name))
                        atts[count++] = PFalg_proj (p->schema.items[i].name,
                                                    p->schema.items[i].name);

                /* introduce project before distinct */
                ret = PFla_distinct (PFla_project_ (L(p), count, atts));

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
            } else if (!count) {
                /* as this rule does not cope with empty sequences
                   correctly it is disabled */
                break;

#if 0
                /* as all columns are constant, the distinct operator
                   can be replaced by lit_tbl of the current schema
                   whose single row contains the constant values */
                PFalg_att_t *atts = PFmalloc (p->schema.count *
                                              sizeof (PFalg_att_t));
                PFalg_atom_t *atoms = PFmalloc (p->schema.count *
                                                sizeof (PFalg_atom_t));

                for (i = 0; i < p->schema.count; i++) {
                    atts[i] = p->schema.items[i].name;
                    atoms[i] = PFprop_const_val (p->prop,
                                                 p->schema.items[i].name);
                }
                *p = *PFla_lit_tbl (PFalg_attlist_ (i, atts),
                                    PFalg_tuple_ (i, atoms));

                SEEN(p) = true;
#endif
            }
        } break;

        case la_fun_1to1:
            /* introduce attach if necessary */
            for (unsigned int i = 0; i < p->sem.fun_1to1.refs.count; i++)
                if (PFprop_const (p->prop, p->sem.fun_1to1.refs.atts[i]))
                    L(p) = add_attach (L(p), p->sem.fun_1to1.refs.atts[i],
                                       PFprop_const_val (
                                           p->prop,
                                           p->sem.fun_1to1.refs.atts[i]));
            break;

        case la_num_eq:
            if (PFprop_const (p->prop, p->sem.binary.att1) &&
                PFprop_const (p->prop, p->sem.binary.att2)) {
                PFalg_atom_t val1, val2;
                val1 = PFprop_const_val (p->prop, p->sem.binary.att1);
                val2 = PFprop_const_val (p->prop, p->sem.binary.att2);
                if (val1.type == val2.type && val1.type == aat_int) {
                    *p = *PFla_attach (L(p), p->sem.binary.res,
                                       PFalg_lit_bln (
                                           val1.val.int_ == val2.val.int_));
                    break;
                }
            }
            if (no_attach) break; /* else continue */
        case la_num_gt:   /* possible extensions for 'and' and 'or': */
        case la_bool_and: /* if one arg is const && false replace by project */
        case la_bool_or:  /* if one arg is const && true replace by project */
            /* introduce attach if necessary */
            if (PFprop_const (p->prop, p->sem.binary.att1)) {
                L(p) = add_attach (L(p), p->sem.binary.att1,
                                   PFprop_const_val (p->prop,
                                                     p->sem.binary.att1));
            }
            if (PFprop_const (p->prop, p->sem.binary.att2)) {
                L(p) = add_attach (L(p), p->sem.binary.att2,
                                   PFprop_const_val (p->prop,
                                                     p->sem.binary.att2));
            }
            break;

        case la_bool_not:
            /* introduce attach if necessary */
            if (PFprop_const (p->prop, p->sem.unary.att)) {
                L(p) = add_attach (L(p), p->sem.unary.att,
                                   PFprop_const_val (p->prop,
                                                     p->sem.unary.att));
            }
            break;

        case la_avg:
        case la_max:
        case la_min:
            /* some optimization opportunities for
               aggregate operators arise if 'att' is constant */
            if (PFprop_const_left (p->prop, p->sem.aggr.att)) {
                /* if partitioning column is constant as well
                   replace aggregate by a new literal table
                   with one row containing 'att' and 'part' */
                if (p->sem.aggr.part &&
                    PFprop_const_left (p->prop, p->sem.aggr.part))
                    *p = *PFla_lit_tbl (
                              PFalg_attlist (p->sem.aggr.res,
                                             p->sem.aggr.part),
                              PFalg_tuple (PFprop_const_val_left (
                                               p->prop,
                                               p->sem.aggr.att),
                                           PFprop_const_val_left (
                                               p->prop,
                                               p->sem.aggr.part)));
                /* if the partitioning column is present but not
                   constant we can replace the aggregate by a
                   distinct operator (value in 'att' stays the same). */
                else if (p->sem.aggr.part)
                    *p = *PFla_distinct (
                              PFla_project (
                                  L(p),
                                  PFalg_proj (p->sem.aggr.res,
                                              p->sem.aggr.att),
                                  PFalg_proj (p->sem.aggr.part,
                                              p->sem.aggr.part)));
                /* replace aggregate by a new literal table
                   containining a single record with the result of
                   aggregate operator. */
                else
                    *p = *PFla_lit_tbl (PFalg_attlist (p->sem.aggr.res),
                                        PFalg_tuple (
                                            PFprop_const_val_left (
                                                p->prop,
                                                p->sem.aggr.att)));
            }
            break;

        case la_sum:
            /* introduce attach if necessary */
            if (PFprop_const_left (p->prop, p->sem.aggr.att)) {
                L(p) = add_attach (L(p), p->sem.aggr.att,
                                   PFprop_const_val_left (p->prop,
                                                          p->sem.aggr.att));
            }

            /* if partitiong attribute is constant remove it
               and attach it after the operator */
            if (p->sem.aggr.part &&
                PFprop_const_left (p->prop, p->sem.aggr.part)) {
                PFla_op_t *sum = PFla_aggr (p->kind,
                                           L(p),
                                           p->sem.aggr.res,
                                           p->sem.aggr.att,
                                           p->sem.aggr.part);
                PFla_op_t *ret = add_attach (sum, p->sem.aggr.part,
                                             PFprop_const_val_left (
                                                 p->prop,
                                                 p->sem.aggr.part));
                sum->sem.aggr.part = att_NULL;
                *p = *ret;
                SEEN(p) = true;
            }
            break;

        case la_count:
            /* if partitiong attribute is constant remove it
               and attach it after the operator */
            if (p->sem.aggr.part &&
                PFprop_const_left (p->prop, p->sem.aggr.part)) {
                PFla_op_t *count = PFla_count (L(p),
                                               p->sem.aggr.res,
                                               p->sem.aggr.part);
                PFla_op_t *ret = add_attach (count, p->sem.aggr.part,
                                             PFprop_const_val_left (
                                                 p->prop,
                                                 p->sem.aggr.part));
                count->sem.aggr.part = att_NULL;
                *p = *ret;
                SEEN(p) = true;
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

            /* only include partitioning attribute if it is not constant */
            if (p->sem.sort.part &&
                PFprop_const (p->prop, p->sem.sort.part))
                p->sem.sort.part = att_NULL;

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

        case la_type:
        case la_cast:
            /* introduce attach if necessary */
            if (PFprop_const_left (p->prop, p->sem.type.att)) {
                L(p) = add_attach (L(p), p->sem.type.att,
                                   PFprop_const_val_left (
                                       p->prop,
                                       p->sem.type.att));
            }
            break;

        case la_seqty1:
        case la_all:
            /* introduce attach if necessary */
            if (PFprop_const_left (p->prop, p->sem.aggr.att)) {
                L(p) = add_attach (L(p), p->sem.aggr.att,
                                   PFprop_const_val_left (p->prop,
                                                          p->sem.aggr.att));
            }

            /* if partitiong attribute is constant remove it
               and attach it after the operator */
            if (p->sem.aggr.part &&
                PFprop_const_left (p->prop, p->sem.aggr.part)) {
                PFla_op_t *op = p->kind == la_all
                                ? PFla_all (L(p),
                                            p->sem.aggr.res,
                                            p->sem.aggr.att,
                                            p->sem.aggr.part)
                                : PFla_seqty1 (L(p),
                                               p->sem.aggr.res,
                                               p->sem.aggr.att,
                                               p->sem.aggr.part);

                PFla_op_t *ret = add_attach (op, p->sem.aggr.part,
                                             PFprop_const_val_left (
                                                 p->prop,
                                                 p->sem.aggr.part));
                op->sem.aggr.part = att_NULL;
                *p = *ret;
                SEEN(p) = true;
            }
            break;

        case la_cond_err:
            if (PFprop_const_right (p->prop, p->sem.err.att) &&
                PFprop_type_of (R(p), p->sem.err.att) == aat_bln &&
                PFprop_const_val_right (p->prop, p->sem.err.att).val.bln) {
                *p = *PFla_dummy (L(p));
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
PFalgopt_const (PFla_op_t *root, bool no_attach)
{
    /* Infer constant properties first */
    PFprop_infer_const (root);

    /* Optimize algebra tree */
    opt_const (root, no_attach);
    PFla_dag_reset (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
