/**
 * @file
 *
 * Optimize relational algebra expression DAG based on multivalued
 * dependencies. We make use of the fact that a large number of operators
 * does not rely on both sides of a thetajoin operator. We push down
 * as many operators underneath the thetajoin operator and thus replace
 * multivalued dependencies by an explicit join operator.
 *
 * We replace all thetajoin operators by an internal variant that
 * can cope with identical columns, can hide its input columns, and
 * is able to represent partial predicates (comparisons where a
 * selection has not been applied yet). With this enhanced thetajoin
 * operator we can push down operators into both operands whenever we
 * do not know which operand really requires the operators. Furthermore
 * we can collect the predicates step by step without looking at a bigger
 * pattern.
 * A cleaning phase then replaces the internal thetajoin operators by
 * normal ones and adds additional operators for all partial predicates
 * as well as projections to avoid name conflicts.
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

/* mnemonic algebra constructors */
#include "logical_mnemonic.h"

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define SEEN(p) ((p)->bit_dag)

struct pred_struct {
    PFalg_comp_t comp;      /* comparison */
    PFalg_col_t  left;      /* left selection column */
    PFalg_col_t  right;     /* right selection column */
    PFalg_col_t  res;       /* boolean result column of the comparison */
    bool         persist;   /* marker if predicate is persistent */
    bool         left_vis;  /* indicator if the left column is visible */
    bool         right_vis; /* indicator if the right column is visible */
    bool         res_vis;   /* indicator if the res column is visible */
};
typedef struct pred_struct pred_struct;

/* mnemonic for a value lookup in the predicate struct */
#define COMP_AT(a,i) ((*(pred_struct *) PFarray_at ((a), (i))).comp)
#define PERS_AT(a,i) ((*(pred_struct *) PFarray_at ((a), (i))).persist)
#define LEFT_AT(a,i) ((*(pred_struct *) PFarray_at ((a), (i))).left)
#define RIGHT_AT(a,i) ((*(pred_struct *) PFarray_at ((a), (i))).right)
#define RES_AT(a,i) ((*(pred_struct *) PFarray_at ((a), (i))).res)
#define LEFT_VIS_AT(a,i) ((*(pred_struct *) PFarray_at ((a), (i))).left_vis)
#define RIGHT_VIS_AT(a,i) ((*(pred_struct *) PFarray_at ((a), (i))).right_vis)
#define RES_VIS_AT(a,i) ((*(pred_struct *) PFarray_at ((a), (i))).res_vis)

/**
 * Thetajoin constructor for thetajoins used during optimization
 * (special variant).
 */
static PFla_op_t *
thetajoin_opt (PFla_op_t *n1, PFla_op_t *n2, PFarray_t *pred)
{
    PFla_op_t *ret = PFla_thetajoin_opt_internal (n1, n2, pred);
    unsigned int i, j, count, max_schema_count;

    /* check for consistency of all columns
       and fill in the schema */
    max_schema_count = n1->schema.count /* left */
                     + n2->schema.count /* right */
                     + PFarray_last (pred); /* new columns */

    /* allocate memory for the result schema (schema(n1) + schema(n2)) */
    ret->schema.items
        = PFmalloc (max_schema_count * sizeof (*(ret->schema.items)));

    count = 0;

    /* copy schema from argument 'n1' */
    for (i = 0; i < n1->schema.count; i++) {
        for (j = 0; j < PFarray_last (pred); j++)
            if (LEFT_AT(pred, j) == n1->schema.items[i].name) {
                if (LEFT_VIS_AT(pred, j))
                    ret->schema.items[count++] = n1->schema.items[i];

                break;
            }

        if (j == PFarray_last (pred))
            ret->schema.items[count++] = n1->schema.items[i];
    }

    /* copy schema from argument 'n2', check for duplicate column names */
    for (i = 0; i < n2->schema.count; i++) {
        /* ignore duplicate columns (introduced during rewrites)
           in the schema */
        if (PFprop_ocol (n1, n2->schema.items[i].name))
            continue;

        for (j = 0; j < PFarray_last (pred); j++)
            if (RIGHT_AT(pred, j) == n2->schema.items[i].name) {
                if (RIGHT_VIS_AT(pred, j))
                    ret->schema.items[count++] = n2->schema.items[i];

                break;
            }

        if (j == PFarray_last (pred))
            ret->schema.items[count++] = n2->schema.items[i];
    }

    /* copy all new columns generated by the thetajoin operator */
    for (i = 0; i < PFarray_last (pred); i++) {
        if (RES_AT(pred, i) && RES_VIS_AT(pred, i)) {
            ret->schema.items[count].name = RES_AT(pred, i);
            ret->schema.items[count].type = aat_bln;
            count++;

#ifndef NDEBUG
            /* make sure that the new column
               does not appear in the children */
            if (PFprop_ocol (n1, RES_AT(pred, i)))
               PFoops (OOPS_FATAL,
                       "duplicate column '%s' in thetajoin",
                       PFcol_str (RES_AT(pred, i)));

            if (PFprop_ocol (n2, RES_AT(pred, i)))
               PFoops (OOPS_FATAL,
                       "duplicate column '%s' in thetajoin",
                       PFcol_str (RES_AT(pred, i)));
#endif
        }

#ifndef NDEBUG
        /* make sure that all input columns (to the thetajoin are available */
        if (!PFprop_ocol (n1, LEFT_AT(pred, i)))
           PFoops (OOPS_FATAL,
                   "column '%s' not found in thetajoin",
                   PFcol_str (LEFT_AT(pred, i)));

        if (!PFprop_ocol (n2, RIGHT_AT(pred, i)))
           PFoops (OOPS_FATAL,
                   "column '%s' not found in thetajoin",
                   PFcol_str (RIGHT_AT(pred, i)));
#endif
    }

    /* fix the schema count */
    ret->schema.count = count;

    return ret;
}

/**
 * check for a thetajoin operator
 */
static bool
is_tj (PFla_op_t *p)
{
    return (p->kind == la_internal_op);
}

/**
 * check for a name conflict
 *
 * For operators with two inputs (join & cross) it may happen
 * that a thetajoin (with an invisible column) was already pushed
 * through. As only the invisible columns may conflict we
 * check here (and in most cases discard the rewrite).
 */
static bool
name_conflict (PFla_op_t *n1, PFla_op_t *n2)
{
    for (unsigned int i = 0; i < n1->schema.count; i++)
        for (unsigned int j = 0; j < n2->schema.count; j++)
            if (n1->schema.items[i].name ==
                n2->schema.items[j].name)
                return true;
    return false;
}

/**
 * thetajoin_identical checks if the semantical
 * information of two thetajoin operators is the same.
 */
static bool
thetajoin_identical (PFla_op_t *a, PFla_op_t *b)
{
    PFarray_t *pred1, *pred2;

    assert (a->kind == la_internal_op &&
            b->kind == la_internal_op);

    pred1 = a->sem.thetajoin_opt.pred;
    pred2 = b->sem.thetajoin_opt.pred;

    if (PFarray_last (pred1) != PFarray_last (pred2))
        return false;

    for (unsigned int i = 0; i < PFarray_last (pred1); i++)
        if (COMP_AT (pred1, i)      != COMP_AT (pred2, i)      ||
            PERS_AT (pred1, i)      != PERS_AT (pred2, i)      ||
            LEFT_AT (pred1, i)      != LEFT_AT (pred2, i)      ||
            RIGHT_AT (pred1, i)     != RIGHT_AT (pred2, i)     ||
            RES_AT (pred1, i)       != RES_AT (pred2, i)       ||
            LEFT_VIS_AT (pred1, i)  != LEFT_VIS_AT (pred2, i)  ||
            RIGHT_VIS_AT (pred1, i) != RIGHT_VIS_AT (pred2, i) ||
            RES_VIS_AT (pred1, i)   != RES_VIS_AT (pred2, i))
            return false;

    return true;
}

/**
 * thetajoin_stable_col_count checks if the thetajoin
 * operator prunes the input schema (by marking input
 * columns as invisible).
 */
static bool
thetajoin_stable_col_count (PFla_op_t *p)
{
    PFarray_t *pred;

    assert (p->kind == la_internal_op);

    pred = p->sem.thetajoin_opt.pred;
    assert (pred);

    for (unsigned int i = 0; i < PFarray_last (pred); i++)
        if (!LEFT_VIS_AT (pred, i)  || !RIGHT_VIS_AT (pred, i))
            return false;

    return true;
}

/**
 * project_identical checks if the semantical
 * information of two projections is the same.
 */
static bool
project_identical (PFla_op_t *a, PFla_op_t *b)
{
    assert (a->kind == la_project &&
            b->kind == la_project);

    if (a->sem.proj.count != b->sem.proj.count)
        return false;

    for (unsigned int i = 0; i < a->sem.proj.count; i++)
        if (a->sem.proj.items[i].new != b->sem.proj.items[i].new
            || a->sem.proj.items[i].old != b->sem.proj.items[i].old)
            return false;

    return true;
}

/**
 * Worker for binary operators that pushes the binary
 * operator underneath the thetajoin if both arguments
 * are provided by the same child operator. If the boolean
 * flag @a integrate is true and if both arguments to the binary
 * operation come from different relations we integrate
 * the comparison as a possible new predicate in the thetajoin.
 */
static bool
modify_binary_op (PFla_op_t *p,
                  PFla_op_t * (* op) (const PFla_op_t *,
                                      PFalg_col_t,
                                      PFalg_col_t,
                                      PFalg_col_t),
                  bool integrate,
                  PFalg_comp_t comp)
{
    PFalg_col_t res, col1, col2;
    bool modified = false,
         match    = false;

    if (is_tj (L(p))) {
        bool switch_left = PFprop_ocol (LL(p), p->sem.binary.col1) &&
                           PFprop_ocol (LL(p), p->sem.binary.col2);
        bool switch_right = PFprop_ocol (LR(p), p->sem.binary.col1) &&
                            PFprop_ocol (LR(p), p->sem.binary.col2);

        /* Pushing down the operator twice is only allowed
           if it doesn't affect the cardinality. */
        if (switch_left && switch_right) {
            *p = *(thetajoin_opt (
                       op (LL(p),
                           p->sem.binary.res,
                           p->sem.binary.col1,
                           p->sem.binary.col2),
                       op (LR(p),
                           p->sem.binary.res,
                           p->sem.binary.col1,
                           p->sem.binary.col2),
                       L(p)->sem.thetajoin_opt.pred));
            modified = true;
        }
        else if (switch_left) {
            *p = *(thetajoin_opt (
                       op (LL(p),
                           p->sem.binary.res,
                           p->sem.binary.col1,
                           p->sem.binary.col2),
                       LR(p),
                       L(p)->sem.thetajoin_opt.pred));
            modified = true;
        }
        else if (switch_right) {
            *p = *(thetajoin_opt (
                       LL(p),
                       op (LR(p),
                           p->sem.binary.res,
                           p->sem.binary.col1,
                           p->sem.binary.col2),
                       L(p)->sem.thetajoin_opt.pred));
            modified = true;
        }
        else if (integrate &&
            PFprop_ocol (LL(p), p->sem.binary.col1) &&
            PFprop_ocol (LR(p), p->sem.binary.col2)) {
            res  = p->sem.binary.res;
            col1 = p->sem.binary.col1;
            col2 = p->sem.binary.col2;
            match = true;
        }
        else if (integrate &&
            PFprop_ocol (LL(p), p->sem.binary.col2) &&
            PFprop_ocol (LR(p), p->sem.binary.col1)) {
            /* switch direction of inequality predicates */
            if (comp == alg_comp_gt)
                comp = alg_comp_lt;
            else if (comp == alg_comp_lt)
                comp = alg_comp_gt;
            res  = p->sem.binary.res;
            col1 = p->sem.binary.col2;
            col2 = p->sem.binary.col1;
            match = true;
        }

        /* integrate the comparison as a possible new predicate
           in the thetajoin operator */
        if (match) {
            PFarray_t *pred = PFarray_copy (L(p)->sem.thetajoin_opt.pred);

            /* add a new predicate ... */
            *(pred_struct *) PFarray_add (pred) =
                (pred_struct) {
                    .comp      = comp,
                    .left      = col1,
                    .right     = col2,
                    .res       = res,
                    .persist   = false,
                    .left_vis  = true,
                    .right_vis = true,
                    .res_vis   = true
                };

            /* ... and create a new thetajoin operator */
            *p = *(thetajoin_opt (LL(p), LR(p), pred));

            modified = true;
        }
    }
    return modified;
}

/**
 * worker for PFalgopt_mvd
 *
 * opt_mvd looks up op-thetajoin operator pairs and
 * tries to move the thetajoin operator up in the DAG
 * as far as possible. During this rewrites the thetajoin
 * operator eventually collects other predicates and thus
 * becomes more selective.
 */
static bool do_opt_mvd (PFla_op_t *p, bool modified);

static bool
opt_mvd (PFla_op_t *p)
{
    bool modified = false;

    PFrecursion_fence ();

    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return modified;
    else
        SEEN(p) = true;

    /* apply optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        modified = opt_mvd (p->child[i]) || modified;

    return do_opt_mvd (p, modified);
}

static bool
do_opt_mvd (PFla_op_t *p, bool modified)
{
    unsigned i, j;

    /**
     * In the following action code we try to propagate thetajoin
     * operators up the DAG as far as possible.
     */

    /* action code */
    switch (p->kind) {
        case la_serialize_seq:
        case la_serialize_rel:
        case la_side_effects:
            break;

        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
            /* nothing to do -- no thetajoin may sit underneath */
            break;

        case la_attach:
            if (is_tj (L(p))) {
                /* push attach into both thetajoin operands */
                *p = *(thetajoin_opt (
                           attach (LL(p),
                                   p->sem.attach.res,
                                   p->sem.attach.value),
                           attach (LR(p),
                                   p->sem.attach.res,
                                   p->sem.attach.value),
                           L(p)->sem.thetajoin_opt.pred));
                modified = true;
            }
            break;

        case la_cross:
            if (is_tj (L(p))) {
                if (name_conflict (LR(p), R(p))) break;
                *p = *(thetajoin_opt (LL(p),
                                  cross (LR(p), R(p)),
                                  L(p)->sem.thetajoin_opt.pred));
                modified = true;
            }
            else if (is_tj (R(p))) {
                if (name_conflict (RR(p), L(p))) break;
                *p = *(thetajoin_opt (RL(p),
                                  cross (RR(p), L(p)),
                                  R(p)->sem.thetajoin_opt.pred));
                modified = true;
            }
            break;

        case la_eqjoin:
            /* Move the independent expression (the one without
               join column) up the DAG. */
            if (is_tj (L(p))) {
                if (PFprop_ocol (LL(p), p->sem.eqjoin.col1)) {
                    if (name_conflict (LL(p), R(p))) break;
                    *p = *(thetajoin_opt (eqjoin (LL(p),
                                                  R(p),
                                                  p->sem.eqjoin.col1,
                                                  p->sem.eqjoin.col2),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));
                } else {
                    if (name_conflict (LR(p), R(p))) break;
                    *p = *(thetajoin_opt (LL(p),
                                          eqjoin (LR(p),
                                                  R(p),
                                                  p->sem.eqjoin.col1,
                                                  p->sem.eqjoin.col2),
                                          L(p)->sem.thetajoin_opt.pred));
                }

                modified = true;
                break;

            }
            if (is_tj (R(p))) {
                if (PFprop_ocol (RL(p), p->sem.eqjoin.col2)) {
                    if (name_conflict (L(p), RL(p))) break;
                    *p = *(thetajoin_opt (eqjoin (L(p),
                                                  RL(p),
                                                  p->sem.eqjoin.col1,
                                                  p->sem.eqjoin.col2),
                                          RR(p),
                                          R(p)->sem.thetajoin_opt.pred));
                } else {
                    if (name_conflict (L(p), RR(p))) break;
                    *p = *(thetajoin_opt (RL(p),
                                          eqjoin (L(p),
                                                  RR(p),
                                                  p->sem.eqjoin.col1,
                                                  p->sem.eqjoin.col2),
                                          R(p)->sem.thetajoin_opt.pred));
                }

                modified = true;
                break;
            }
            break;

        case la_semijoin:
            /* Move the independent expression (the one without
               join column) up the DAG. */
            if (is_tj (L(p))) {
                if (PFprop_ocol (LL(p), p->sem.eqjoin.col1))
                    *p = *(thetajoin_opt (semijoin (LL(p),
                                                    R(p),
                                                    p->sem.eqjoin.col1,
                                                    p->sem.eqjoin.col2),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));
                else
                    *p = *(thetajoin_opt (LL(p),
                                          semijoin (LR(p),
                                                    R(p),
                                                    p->sem.eqjoin.col1,
                                                    p->sem.eqjoin.col2),
                                          L(p)->sem.thetajoin_opt.pred));

                modified = true;
                break;

            }
            break;

        case la_thetajoin:
        case la_internal_op:
            /* ignore adjacent thetajoins for now */
            break;

        case la_project:
            /* Split project operator and push it beyond the thetajoin. */
            if (is_tj (L(p))) {
                PFarray_t    *pred = PFarray_copy (L(p)->sem.thetajoin_opt.pred);
                unsigned int  count1 = 0,
                              count2 = 0;
                PFalg_proj_t *proj_list1,
                             *proj_list2;
                PFalg_col_t   new_name;
                bool          conflict = false;

                /* create projection lists */
                proj_list1 = PFmalloc ((p->schema.count + PFarray_last (pred)) *
                                       sizeof (PFalg_proj_t));
                proj_list2 = PFmalloc ((p->schema.count + PFarray_last (pred)) *
                                       sizeof (PFalg_proj_t));

                for (i = 0; i < LL(p)->schema.count; i++)
                    for (j = 0; j < p->sem.proj.count; j++)
                        if (LL(p)->schema.items[i].name
                            == p->sem.proj.items[j].old) {
                            proj_list1[count1++] = p->sem.proj.items[j];
                        }

                for (i = 0; i < LR(p)->schema.count; i++)
                    for (j = 0; j < p->sem.proj.count; j++)
                        if (LR(p)->schema.items[i].name
                            == p->sem.proj.items[j].old) {
                            proj_list2[count2++] = p->sem.proj.items[j];
                        }

                /* The following for loop does multiple things:
                   1.) removes all invisible result columns
                   2.) prunes non persistent predicates whose result column
                       is invisible
                   3.) updates the names of all visible result columns
                       (based on the projection list)
                   4.) marks join columns missing in the projection list
                       as invisible
                   5.) updates the names of all visible join columns
                       (based on the projection list)
                   6.) counts the number of invisible join columns
                   7.) collects the names of all join columns
                       (the only 'new' names are the ones from the invisible
                       join columns) */
                for (i = 0; i < PFarray_last (pred); i++) {
                    /* clean up the unreferenced results of comparisons
                       and throw away uneffective comparisons. */
                    if (RES_VIS_AT (pred, i)) {
                        for (j = 0; j < p->sem.proj.count; j++)
                            if (RES_AT (pred, i) == p->sem.proj.items[j].old)
                                break;
                        if (j == p->sem.proj.count) {
                            /* throw away non visible result column */
                            RES_VIS_AT (pred, i) = false;
                            RES_AT (pred, i) = col_NULL;

                            /* throw away non persistent predicate completely */
                            if (!PERS_AT (pred, i)) {
                                /* copy last predicate to the current position,
                                   remove the last predicate, and reevaluate
                                   the predicate at the current position */
                                *(pred_struct *) PFarray_at (pred, i)
                                    = *(pred_struct *) PFarray_top (pred);
                                PFarray_del (pred);
                                i--;
                                continue;
                            }
                        } else {
                            /* check if the result is used with two different names */
                            for (; j < p->sem.proj.count; j++)
                                conflict |= (RES_AT (pred, i) ==
                                             p->sem.proj.items[j].old);

                            /* update the column name of the result column */
                            RES_AT (pred, i) = p->sem.proj.items[j].new;
                        }
                    }

                    if (LEFT_VIS_AT (pred, i)) {
                        for (j = 0; j < p->sem.proj.count; j++)
                            if (LEFT_AT (pred, i) == p->sem.proj.items[j].old)
                                break;

                        if (j == p->sem.proj.count) {
                            /* create a new unique column name */
                            new_name = PFcol_new (LEFT_AT (pred, i));
                            /* mark unreferenced join inputs invisible */
                            LEFT_VIS_AT (pred, i) = false;
                            /* rename from old to new unique name */
                            proj_list1[count1++]
                                = PFalg_proj (new_name, LEFT_AT (pred, i));
                            /* and assign input the new name */
                            LEFT_AT (pred, i) = new_name;
                        }
                        else
                            /* update the column name of all referenced
                               join columns */
                            LEFT_AT (pred, i) = p->sem.proj.items[j].new;
                    }
                    else {
                        for (j = 0; j < count1; j++)
                            if (LEFT_AT (pred, i) == proj_list1[j].new)
                                break;
                        /* only add column once */
                        if (j == count1)
                            proj_list1[count1++]
                                = PFalg_proj (LEFT_AT (pred, i),
                                              LEFT_AT (pred, i));
                    }

                    if (RIGHT_VIS_AT (pred, i)) {
                        for (j = 0; j < p->sem.proj.count; j++)
                            if (RIGHT_AT (pred, i) == p->sem.proj.items[j].old)
                                break;

                        if (j == p->sem.proj.count) {
                            /* create a new unique column name */
                            new_name = PFcol_new (RIGHT_AT (pred, i));
                            /* mark unreferenced join inputs invisible */
                            RIGHT_VIS_AT (pred, i) = false;
                            /* rename from old to new unique name */
                            proj_list2[count2++]
                                = PFalg_proj (new_name, RIGHT_AT (pred, i));
                            /* and assign input the new name */
                            RIGHT_AT (pred, i) = new_name;
                        }
                        else
                            /* update the column name of all referenced
                               join columns */
                            RIGHT_AT (pred, i) = p->sem.proj.items[j].new;
                    }
                    else {
                        for (j = 0; j < count2; j++)
                            if (RIGHT_AT (pred, i) == proj_list2[j].new)
                                break;
                        /* only add column once */
                        if (j == count2)
                            proj_list2[count2++]
                                = PFalg_proj (RIGHT_AT (pred, i),
                                              RIGHT_AT (pred, i));
                    }
                }

                for (i = 0; i < count1; i++)
                    for (j = 0; j < count2; j++)
                        conflict |= (proj_list1[i].new == proj_list2[j].new);
                /* Ensure that both arguments add at least one column to
                   the result. */
                if (conflict || !count1 || !count2)
                    break;

                *p = *(thetajoin_opt (
                           PFla_project_ (LL(p), count1, proj_list1),
                           PFla_project_ (LR(p), count2, proj_list2),
                           pred));
                modified = true;
            }
            break;

        case la_select:
            if (is_tj (L(p))) {
                if (PFprop_ocol (LL(p), p->sem.select.col)) {
                    *p = *(thetajoin_opt (select_ (LL(p),
                                                   p->sem.select.col),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));
                } else if (PFprop_ocol (LR(p), p->sem.select.col)) {
                    *p = *(thetajoin_opt (LL(p),
                                          select_ (LR(p),
                                                   p->sem.select.col),
                                          L(p)->sem.thetajoin_opt.pred));
                } else {
                    /* col is referenced in a result column of the thetajoin */
                    PFalg_col_t sel_col = p->sem.select.col;
                    PFarray_t  *pred;

                    /* create a new thetajoin operator ... */
                    *p = *(thetajoin_opt (LL(p),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));

                    /* ... and update all the predicates
                       that are affected by the selection */
                    pred = p->sem.thetajoin_opt.pred;
                    for (i = 0; i < PFarray_last (pred); i++)
                        if (RES_VIS_AT (pred, i) && RES_AT (pred, i) == sel_col)
                            /* make the join condition persistent */
                            PERS_AT (pred, i) = true;
                }

                modified = true;
            }
            break;

        case la_pos_select:
            /* the following rewrite is incorrect as the thetajoin arguments
               filter out some rows from the cross product -- Not every
               iteration has the same number of rows anymore and thus row-
               numbering inside the pos_select needs to take the result
               of the thetajoin into account. */
#if 0
            /* An expression that does not contain any sorting column
               required by the positional select operator, but contains
               the partitioning column is independent of the positional
               select. The translation thus moves the expression above
               the positional selection and removes its partitioning column. */
            if (is_tj (L(p)) &&
                p->sem.pos_sel.part) {
                bool lpart = false,
                     rpart = false;
                unsigned int lsortby = 0,
                             rsortby = 0;

                /* first check for the required columns
                   in the left thetajoin input */
                for (i = 0; i < LL(p)->schema.count; i++) {
                    if (LL(p)->schema.items[i].name == p->sem.pos_sel.part)
                        lpart = true;
                    for (j = 0;
                         j < PFord_count (p->sem.pos_sel.sortby);
                         j++)
                        if (LL(p)->schema.items[i].name
                            == PFord_order_col_at (
                                   p->sem.pos_sel.sortby,
                                   j)) {
                            lsortby++;
                            break;
                        }
                }
                /* and then check for the required columns
                   in the right thetajoin input */
                for (i = 0; i < LR(p)->schema.count; i++) {
                    if (LR(p)->schema.items[i].name == p->sem.pos_sel.part)
                        rpart = true;
                    for (j = 0;
                         j < PFord_count (p->sem.pos_sel.sortby);
                         j++)
                        if (LR(p)->schema.items[i].name
                            == PFord_order_col_at (
                                   p->sem.pos_sel.sortby,
                                   j)) {
                            rsortby++;
                            break;
                        }
                }

                if (lpart && rsortby == PFord_count (p->sem.pos_sel.sortby)) {
                    *p = *(thetajoin_opt (
                              LL(p),
                              pos_select (
                                  LR(p),
                                  p->sem.pos_sel.pos,
                                  p->sem.pos_sel.sortby,
                                  col_NULL),
                              L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }

                if (rpart && lsortby == PFord_count (p->sem.pos_sel.sortby)) {
                    *p = *(thetajoin_opt (
                              pos_select (
                                  LL(p),
                                  p->sem.pos_sel.pos,
                                  p->sem.pos_sel.sortby,
                                  col_NULL),
                              LR(p),
                              L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }
            }
#endif
            break;

        case la_disjunion:
            /* If the children of the union operator are both thetajoin
               operators which reference the same subexpression, we move
               them above the union. */
            if (is_tj (L(p)) && is_tj (R(p)) &&
                thetajoin_identical (L(p), R(p))) {
                if (LL(p) == RL(p)) {
                    *p = *(thetajoin_opt (LL(p),
                                          disjunion (LR(p), RR(p)),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }
                else if (LR(p) == RR(p)) {
                    *p = *(thetajoin_opt (disjunion (LL(p), RL(p)),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }
                else if (LL(p)->kind == la_project &&
                         RL(p)->kind == la_project &&
                         LLL(p) == RLL(p) &&
                         project_identical (LL(p), RL(p))) {
                    *p = *(thetajoin_opt (LL(p),
                                          disjunion (LR(p), RR(p)),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }
                else if (LR(p)->kind == la_project &&
                         RR(p)->kind == la_project &&
                         LRL(p) == RRL(p) &&
                         project_identical (LR(p), RR(p))) {
                    *p = *(thetajoin_opt (disjunion (LL(p), RL(p)),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }
            }
            break;

        case la_intersect:
            /* If the children of the intersect operator are both thetajoin
               operators which reference the same subexpression, we move
               them above the intersect. */
            if (is_tj (L(p)) && is_tj (R(p)) &&
                thetajoin_identical (L(p), R(p)) &&
                thetajoin_stable_col_count (L(p))) {
                if (LL(p) == RL(p)) {
                    *p = *(thetajoin_opt (LL(p),
                                          intersect (LR(p), RR(p)),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }
                else if (LR(p) == RR(p)) {
                    *p = *(thetajoin_opt (intersect (LL(p), RL(p)),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }
            }
            break;

        case la_difference:
            /* If the children of the difference operator are both thetajoin
               operators which reference the same subexpression, we move
               them above the difference. */
            if (is_tj (L(p)) && is_tj (R(p)) &&
                thetajoin_identical (L(p), R(p)) &&
                thetajoin_stable_col_count (L(p))) {
                if (LL(p) == RL(p)) {
                    *p = *(thetajoin_opt (LL(p),
                                          difference (LR(p), RR(p)),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }
                else if (LR(p) == RR(p)) {
                    *p = *(thetajoin_opt (difference (LL(p), RL(p)),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }
            }
            break;

        case la_distinct:
            /* Push distinct into both thetajoin operands
               if the thetajoin operator does not hide a
               join column. */
            if (is_tj (L(p)) &&
                thetajoin_stable_col_count (L(p))) {
                *p = *(thetajoin_opt (distinct (LL(p)),
                                      distinct (LR(p)),
                                      L(p)->sem.thetajoin_opt.pred));
                modified = true;
            }
            break;

        case la_fun_1to1:
            if (is_tj (L(p))) {
                bool switch_left = true;
                bool switch_right = true;

                for (i = 0; i < clsize (p->sem.fun_1to1.refs); i++) {
                    switch_left  = switch_left &&
                                   PFprop_ocol (LL(p),
                                                clat (p->sem.fun_1to1.refs, i));
                    switch_right = switch_right &&
                                   PFprop_ocol (LR(p),
                                                clat (p->sem.fun_1to1.refs, i));
                }

                /* Pushing down the operator twice is only allowed
                   if it doesn't affect the cardinality. */
                if (switch_left && switch_right) {
                    *p = *(thetajoin_opt (
                               fun_1to1 (LL(p),
                                         p->sem.fun_1to1.kind,
                                         p->sem.fun_1to1.res,
                                         p->sem.fun_1to1.refs),
                               fun_1to1 (LR(p),
                                         p->sem.fun_1to1.kind,
                                         p->sem.fun_1to1.res,
                                         p->sem.fun_1to1.refs),
                               L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_left) {
                    *p = *(thetajoin_opt (
                               fun_1to1 (LL(p),
                                         p->sem.fun_1to1.kind,
                                         p->sem.fun_1to1.res,
                                         p->sem.fun_1to1.refs),
                               LR(p),
                               L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_right) {
                    *p = *(thetajoin_opt (
                               LL(p),
                               fun_1to1 (LR(p),
                                         p->sem.fun_1to1.kind,
                                         p->sem.fun_1to1.res,
                                         p->sem.fun_1to1.refs),
                               L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
            }
            break;

        case la_num_eq:
            modified = modify_binary_op (p, PFla_eq, true, alg_comp_eq);
            break;
        case la_num_gt:
            modified = modify_binary_op (p, PFla_gt, true, alg_comp_gt);
            break;
        case la_bool_and:
            modified = modify_binary_op (p, PFla_and, false,
                           alg_comp_eq /* pacify picky compiler */);
            break;
        case la_bool_or:
            modified = modify_binary_op (p, PFla_or, false,
                           alg_comp_eq /* pacify picky compiler */);
            break;

        case la_bool_not:
            if (is_tj (L(p))) {
                bool switch_left = PFprop_ocol (LL(p), p->sem.unary.col);
                bool switch_right = PFprop_ocol (LR(p), p->sem.unary.col);

                /* Pushing down the operator twice is only allowed
                   if it doesn't affect the cardinality. */
                if (switch_left && switch_right) {
                    *p = *(thetajoin_opt (
                               PFla_not (LL(p),
                                         p->sem.unary.res,
                                         p->sem.unary.col),
                               PFla_not (LR(p),
                                         p->sem.unary.res,
                                         p->sem.unary.col),
                               L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_left) {
                    *p = *(thetajoin_opt (
                               PFla_not (LL(p),
                                         p->sem.unary.res,
                                         p->sem.unary.col),
                               LR(p),
                               L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_right) {
                    *p = *(thetajoin_opt (
                               LL(p),
                               PFla_not (LR(p),
                                         p->sem.unary.res,
                                         p->sem.unary.col),
                               L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else {
                    /* col is referenced in a result column of the thetajoin */
                    PFalg_col_t res = p->sem.unary.res,
                                col = p->sem.unary.col;
                    PFarray_t  *pred;
                    PFalg_comp_t comp = alg_comp_eq;

                    /* copy the predicates ... */
                    pred = PFarray_copy (L(p)->sem.thetajoin_opt.pred);

                    /* ... and introduce a new predicate that makes
                       the result column visible (as we do not know
                       the comparison kind as well as the arguments
                       we choose dummy ones)  */
                    *(pred_struct *) PFarray_add (pred) =
                        (pred_struct) {
                            .comp      = comp,                   /* dummy */
                            .left      = LEFT_AT (pred, 0),      /* dummy */
                            .right     = RIGHT_AT (pred, 0),     /* dummy */
                            .res       = res,
                            .persist   = false,
                            .left_vis  = LEFT_VIS_AT (pred, 0),  /* dummy */
                            .right_vis = RIGHT_VIS_AT (pred, 0), /* dummy */
                            .res_vis   = true
                        };

                    /* create a new thetajoin operator ... */
                    *p = *(thetajoin_opt (LL(p), LR(p), pred));

                    /* get the current predicate list,
                       find the result column ... */
                    pred = p->sem.thetajoin_opt.pred;
                    for (i = 0; i < PFarray_last (pred); i++)
                        if (RES_VIS_AT (pred, i) && RES_AT (pred, i) == col)
                            break;

                    assert (i != PFarray_last (pred));

                    switch (COMP_AT (pred, i)) {
                        case alg_comp_eq:
                            comp = alg_comp_ne;
                            break;

                        case alg_comp_gt:
                            comp = alg_comp_le;
                            break;

                        case alg_comp_ge:
                            comp = alg_comp_lt;
                            break;

                        case alg_comp_lt:
                            comp = alg_comp_ge;
                            break;

                        case alg_comp_le:
                            comp = alg_comp_gt;
                            break;

                        case alg_comp_ne:
                            comp = alg_comp_eq;
                            break;
                    }

                    /* ... and update the new predicate based
                       on the information of the already existing one */
                    *(pred_struct *) PFarray_top (pred) =
                        (pred_struct) {
                            .comp      = comp,
                            .left      = LEFT_AT (pred, i),
                            .right     = RIGHT_AT (pred, i),
                            .res       = res,
                            .persist   = false,
                            .left_vis  = LEFT_VIS_AT (pred, i),
                            .right_vis = RIGHT_VIS_AT (pred, i),
                            .res_vis   = true
                        };

                    modified = true;
                }
            }
            break;

        case la_to:
            if (is_tj (L(p))) {
                bool switch_left = PFprop_ocol (LL(p), p->sem.binary.col1) &&
                                   PFprop_ocol (LL(p), p->sem.binary.col2);
                bool switch_right = PFprop_ocol (LR(p), p->sem.binary.col1) &&
                                    PFprop_ocol (LR(p), p->sem.binary.col2);

                if (switch_left) {
                    *p = *(thetajoin_opt (
                               to (LL(p),
                                   p->sem.binary.res,
                                   p->sem.binary.col1,
                                   p->sem.binary.col2),
                               LR(p),
                               L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_right) {
                    *p = *(thetajoin_opt (
                               LL(p),
                               to (LR(p),
                                   p->sem.binary.res,
                                   p->sem.binary.col1,
                                   p->sem.binary.col2),
                               L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
            }
            break;

        case la_aggr:
            break;

        case la_rownum:
            /* An expression that does not contain any sorting column
               required by the rownum operator, but contains the partitioning
               column is independent of the rownum. The translation thus
               moves the expression above the rownum and removes its partitioning
               column. */
            if (is_tj (L(p)) &&
                p->sem.sort.part) {
                bool lpart = false,
                     rpart = false;
                unsigned int lsortby = 0,
                             rsortby = 0;

                /******************** thetajoin check ************************/

                unsigned int count = 0;
                PFarray_t   *pred = L(p)->sem.thetajoin_opt.pred;
                
                /* The following rewrite is incorrect if we have a real
                   thetajoin as the thetajoin arguments filter out some
                   rows from the cross product -- Not every iteration
                   has the same number of rows anymore and thus row-numbering
                   needs to take the result of the thetajoin into account. */

                /* Check if we have a cross product without selection. */
                for (i = 0; i < PFarray_last (pred); i++)
                    if (PERS_AT(pred, i)) count++;
                /* skip rewrite (see explanation above) */
                if (count) break;

                /******************** thetajoin check ************************/

                /* first check for the required columns
                   in the left thetajoin input */
                for (i = 0; i < LL(p)->schema.count; i++) {
                    if (LL(p)->schema.items[i].name == p->sem.sort.part)
                        lpart = true;
                    for (j = 0;
                         j < PFord_count (p->sem.sort.sortby);
                         j++)
                        if (LL(p)->schema.items[i].name
                            == PFord_order_col_at (
                                   p->sem.sort.sortby,
                                   j)) {
                            lsortby++;
                            break;
                        }
                }
                /* and then check for the required columns
                   in the right thetajoin input */
                for (i = 0; i < LR(p)->schema.count; i++) {
                    if (LR(p)->schema.items[i].name == p->sem.sort.part)
                        rpart = true;
                    for (j = 0;
                         j < PFord_count (p->sem.sort.sortby);
                         j++)
                        if (LR(p)->schema.items[i].name
                            == PFord_order_col_at (
                                   p->sem.sort.sortby,
                                   j)) {
                            rsortby++;
                            break;
                        }
                }

                if (lpart && rsortby == PFord_count (p->sem.sort.sortby)) {
                    *p = *(thetajoin_opt (
                              LL(p),
                              rownum (
                                  LR(p),
                                  p->sem.sort.res,
                                  p->sem.sort.sortby,
                                  col_NULL),
                              L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }

                if (rpart && lsortby == PFord_count (p->sem.sort.sortby)) {
                    *p = *(thetajoin_opt (
                              rownum (
                                  LL(p),
                                  p->sem.sort.res,
                                  p->sem.sort.sortby,
                                  col_NULL),
                              LR(p),
                              L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }
            }
            break;

        case la_rowrank:
            if (is_tj (L(p))) {
                /******************** thetajoin check ************************/

                unsigned int count = 0;
                PFarray_t   *pred = L(p)->sem.thetajoin_opt.pred;
                
                /* The following rewrite is incorrect if we have a real
                   thetajoin as the thetajoin arguments filter out some
                   rows from the cross product -- Not every group exists
                   anymore and thus row-ranking needs to take the result
                   of the thetajoin into account. */

                /* Check if we have a cross product without selection. */
                for (i = 0; i < PFarray_last (pred); i++)
                    if (PERS_AT(pred, i)) count++;
                /* skip rewrite (see explanation above) */
                if (count) break;

                /******************** thetajoin check ************************/
            }
            /* fall through */

        case la_rank:
            /* An expression that does not contain any sorting column
               required by the rank operator is independent of the rank.
               The translation thus moves the expression above the rank
               column. */
            if (is_tj (L(p))) {
                unsigned int lsortby = 0,
                             rsortby = 0;
                PFla_op_t *(* op) (const PFla_op_t *,
                                   PFalg_col_t,
                                   PFord_ordering_t);

                op = p->kind == la_rowrank ? PFla_rowrank : PFla_rank;

                /* first check for the required columns
                   in the left thetajoin input */
                for (i = 0; i < LL(p)->schema.count; i++)
                    for (j = 0;
                         j < PFord_count (p->sem.sort.sortby);
                         j++)
                        if (LL(p)->schema.items[i].name
                            == PFord_order_col_at (
                                   p->sem.sort.sortby,
                                   j)) {
                            lsortby++;
                            break;
                        }

                /* and then check for the required columns
                   in the right thetajoin input */
                for (i = 0; i < LR(p)->schema.count; i++)
                    for (j = 0;
                         j < PFord_count (p->sem.sort.sortby);
                         j++)
                        if (LR(p)->schema.items[i].name
                            == PFord_order_col_at (
                                   p->sem.sort.sortby,
                                   j)) {
                            rsortby++;
                            break;
                        }

                if (!lsortby && rsortby == PFord_count (p->sem.sort.sortby)) {
                    *p = *(thetajoin_opt (
                              LL(p),
                              op (LR(p),
                                  p->sem.sort.res,
                                  p->sem.sort.sortby),
                              L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }

                if (lsortby == PFord_count (p->sem.sort.sortby) && !rsortby) {
                    *p = *(thetajoin_opt (
                              op (LL(p),
                                  p->sem.sort.res,
                                  p->sem.sort.sortby),
                              LR(p),
                              L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }
            }
            break;

        case la_rowid:
            break;

        case la_type:
            if (is_tj (L(p))) {
                bool switch_left = PFprop_ocol (LL(p), p->sem.type.col);
                bool switch_right = PFprop_ocol (LR(p), p->sem.type.col);

                /* Pushing down the operator twice is only allowed
                   if it doesn't affect the cardinality. */
                if (switch_left && switch_right) {
                    *p = *(thetajoin_opt (type (LL(p),
                                                p->sem.type.res,
                                                p->sem.type.col,
                                                p->sem.type.ty),
                                          type (LR(p),
                                                p->sem.type.res,
                                                p->sem.type.col,
                                                p->sem.type.ty),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_left) {
                    *p = *(thetajoin_opt (type (LL(p),
                                                p->sem.type.res,
                                                p->sem.type.col,
                                                p->sem.type.ty),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_right) {
                    *p = *(thetajoin_opt (LL(p),
                                          type (LR(p),
                                                p->sem.type.res,
                                                p->sem.type.col,
                                                p->sem.type.ty),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
            }
            break;

        case la_type_assert:
            if (is_tj (L(p))) {
                bool switch_left = PFprop_ocol (LL(p), p->sem.type.col);
                bool switch_right = PFprop_ocol (LR(p), p->sem.type.col);

                /* Pushing down the operator twice is only allowed
                   if it doesn't affect the cardinality. */
                if (switch_left && switch_right) {
                    *p = *(thetajoin_opt (type_assert_pos (
                                              LL(p),
                                              p->sem.type.col,
                                              p->sem.type.ty),
                                          type_assert_pos (
                                              LR(p),
                                              p->sem.type.col,
                                              p->sem.type.ty),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_left) {
                    *p = *(thetajoin_opt (type_assert_pos (
                                              LL(p),
                                              p->sem.type.col,
                                              p->sem.type.ty),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_right) {
                    *p = *(thetajoin_opt (LL(p),
                                          type_assert_pos (
                                              LR(p),
                                              p->sem.type.col,
                                              p->sem.type.ty),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
            }
            break;

        case la_cast:
            if (is_tj (L(p))) {
                bool switch_left = PFprop_ocol (LL(p), p->sem.type.col);
                bool switch_right = PFprop_ocol (LR(p), p->sem.type.col);

                /* Pushing down the operator twice is only allowed
                   if it doesn't affect the cardinality. */
                if (switch_left && switch_right) {
                    *p = *(thetajoin_opt (cast (LL(p),
                                                p->sem.type.res,
                                                p->sem.type.col,
                                                p->sem.type.ty),
                                          cast (LR(p),
                                                p->sem.type.res,
                                                p->sem.type.col,
                                                p->sem.type.ty),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_left) {
                    *p = *(thetajoin_opt (cast (LL(p),
                                                p->sem.type.res,
                                                p->sem.type.col,
                                                p->sem.type.ty),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_right) {
                    *p = *(thetajoin_opt (LL(p),
                                          cast (LR(p),
                                                p->sem.type.res,
                                                p->sem.type.col,
                                                p->sem.type.ty),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
            }
            break;

        case la_step:
        case la_guide_step:
            break;

        case la_step_join:
            if (is_tj (R(p))) {
                bool switch_left = PFprop_ocol (RL(p), p->sem.step.item);
                bool switch_right = PFprop_ocol (RR(p), p->sem.step.item);

                if (switch_left) {
                    *p = *(thetajoin_opt (step_join (
                                                L(p), RL(p),
                                                p->sem.step.spec,
                                                p->sem.step.level,
                                                p->sem.step.item,
                                                p->sem.step.item_res),
                                          RR(p),
                                          R(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_right) {
                    *p = *(thetajoin_opt (RL(p),
                                          step_join (
                                                L(p),
                                                RR(p),
                                                p->sem.step.spec,
                                                p->sem.step.level,
                                                p->sem.step.item,
                                                p->sem.step.item_res),
                                          R(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
            }
            break;

        case la_guide_step_join:
            if (is_tj (R(p))) {
                bool switch_left = PFprop_ocol (RL(p), p->sem.step.item);
                bool switch_right = PFprop_ocol (RR(p), p->sem.step.item);

                if (switch_left) {
                    *p = *(thetajoin_opt (guide_step_join (
                                                L(p), RL(p),
                                                p->sem.step.spec,
                                                p->sem.step.guide_count,
                                                p->sem.step.guides,
                                                p->sem.step.level,
                                                p->sem.step.item,
                                                p->sem.step.item_res),
                                          RR(p),
                                          R(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_right) {
                    *p = *(thetajoin_opt (RL(p),
                                          guide_step_join (
                                                L(p),
                                                RR(p),
                                                p->sem.step.spec,
                                                p->sem.step.guide_count,
                                                p->sem.step.guides,
                                                p->sem.step.level,
                                                p->sem.step.item,
                                                p->sem.step.item_res),
                                          R(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
            }
            break;

        case la_doc_index_join:
            if (is_tj (R(p))) {
                bool switch_left, switch_right;
                switch_left = PFprop_ocol (RL(p), p->sem.doc_join.item) &&
                              PFprop_ocol (RL(p), p->sem.doc_join.item_doc);
                switch_right = PFprop_ocol (RR(p), p->sem.doc_join.item) &&
                               PFprop_ocol (RR(p), p->sem.doc_join.item_doc);

                if (switch_left) {
                    *p = *(thetajoin_opt (doc_index_join (
                                                L(p), RL(p),
                                                p->sem.doc_join.kind,
                                                p->sem.doc_join.item,
                                                p->sem.doc_join.item_res,
                                                p->sem.doc_join.item_doc,
                                                p->sem.doc_join.ns1,
                                                p->sem.doc_join.loc1,
                                                p->sem.doc_join.ns2,
                                                p->sem.doc_join.loc2),
                                          RR(p),
                                          R(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_right) {
                    *p = *(thetajoin_opt (RL(p),
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
                                                p->sem.doc_join.loc2),
                                          R(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
            }
            break;

        case la_doc_tbl:
            /* should not appear as roots already
               translates the doc_tbl operator. */
            break;

        case la_doc_access:
            if (is_tj (R(p))) {
                bool switch_left = PFprop_ocol (RL(p), p->sem.doc_access.col);
                bool switch_right = PFprop_ocol (RR(p), p->sem.doc_access.col);

                if (switch_left) {
                    *p = *(thetajoin_opt (doc_access (L(p), RL(p),
                                                p->sem.doc_access.res,
                                                p->sem.doc_access.col,
                                                p->sem.doc_access.doc_col),
                                          RR(p),
                                          R(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_right) {
                    *p = *(thetajoin_opt (RL(p),
                                          doc_access (L(p), RR(p),
                                                p->sem.doc_access.res,
                                                p->sem.doc_access.col,
                                                p->sem.doc_access.doc_col),
                                          R(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
            }
            break;

        case la_roots:
            /* modify the only pattern starting in roots
               that is no constructor: roots-doc_tbl */
            if (L(p)->kind == la_doc_tbl &&
                is_tj (LL(p))) {
                    if (PFprop_ocol (L(LL(p)), p->sem.doc_tbl.col)) {
                        PFarray_t *pred = LL(p)->sem.thetajoin_opt.pred;
                        PFla_op_t *other_side = R(LL(p));
                        /* overwrite doc_tbl node to update
                           both roots and frag operators */
                        *(L(p)) = *(doc_tbl (L(LL(p)),
                                             L(p)->sem.doc_tbl.res,
                                             L(p)->sem.doc_tbl.col,
                                             L(p)->sem.doc_tbl.kind));
                        /* push roots + doc_tbl through the thetajoin */
                        *p = *(thetajoin_opt (roots (L(p)), other_side, pred));
                    }
                    else {
                        PFarray_t *pred = LL(p)->sem.thetajoin_opt.pred;
                        PFla_op_t *other_side = L(LL(p));
                        /* overwrite doc_tbl node to update
                           both roots and frag operators */
                        *(L(p)) = *(doc_tbl (R(LL(p)),
                                             L(p)->sem.doc_tbl.res,
                                             L(p)->sem.doc_tbl.col,
                                             L(p)->sem.doc_tbl.kind));
                        /* push roots + doc_tbl through the thetajoin */
                        *p = *(thetajoin_opt (other_side, roots (L(p)), pred));
                    }
                    modified = true;
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
            /* do not rewrite anything
               that has to do with function application */
            break;

        case la_proxy:
            /**
             * ATTENTION: The proxies (especially the kind=1 version) are
             * generated in such a way, that the following rewrite does not
             * introduce inconsistencies.
             * The following rewrite would break if the proxy contains operators
             * that are themselves not allowed to be rewritten by the optimization
             * phase. We may not transform expressions that rely on the
             * cardinality of their inputs.
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
             * We are looking for a proxy (of kind 1) followed by a thetajoin.
             * Thus we first ensure that the proxy still has the correct shape
             * (see Figure (1)) and then check whether the proxy is independent
             * of tree t1. If that's the case we rewrite the DAG in Figure (1)
             * into the one of Figure (2).
             *
             *                proxy (kind=1)                |X|
             *      __________/ |  \___                     / \
             *     /(sem.base1) pi_1   \ (sem.ref)        t1   proxy (kind=1)
             *     |            |       |            __________/ |  \___
             *     |           |X|      |           /(sem.base1) pi_1'  \ (sem.ref)
             *     |          /   \     |           |            |       |
             *     |         |     pi_2 |           |           |X|      |
             *     |         |     |    |           |          /   \     |
             *     |         |    |X|   |           |         |     pi_2 |
             *     |        pi_3  / \   |           |         |     |    |
             *     |         |   /___\  |           |         |    |X|   |
             *     |         |     | __/            |        pi_3' / \   |
             *     |         |     |/               |         |   /___\  |
             *     |         |     pi_4             |         |     | __/
             *     |         |     |                |         |     |/
             *     |          \   /                 |         |     pi_4'
             *     |           \ /                  |         |     |
             *     \______      #                   |          \   /
             *            \    /                    |           \ /
             *          proxy_base                  \______      #
             *              |                              \    /
             *             |X|                           proxy_base
             *             / \                               |
             *           t1   t2                             t2
             *
             *            ( 1 )                             ( 2 )
             *
             *
             * The changes happen at the 3 projections: pi_1, pi_3, and pi_4.
             * - All columns of t1 in pi_1 are projected out (resulting in pi_1').
             *   The invisible join columns of t2 have to be added.
             * - All columns of t1 in pi_3 and pi_4 are replaced by a dummy
             *   column (the first column of t2) thus resulting in the modified
             *   projections pi_3' and pi_4', respectively. We don't throw out
             *   the columns of t1 in the proxy as this would require a bigger
             *   rewrite, which is done eventually by the following icols
             *   optimization.
             */
            if (is_tj (L(p->sem.proxy.base1)) &&
                p->sem.proxy.kind == 1 &&
                /* check consistency */
                /* PROJECTION IN PATTERN */
                L(p)->kind == la_project &&
                LL(p)->kind == la_eqjoin &&
                /* PROJECTION IN PATTERN */
                L(LL(p))->kind == la_project &&
                /* PROJECTION IN PATTERN */
                R(LL(p))->kind == la_project &&
                RL(LL(p))->kind == la_eqjoin &&
                LL(LL(p))->kind == la_rowid &&
                L(LL(LL(p))) == p->sem.proxy.base1 &&
                /* PROJECTION IN PATTERN */
                p->sem.proxy.ref->kind == la_project &&
                L(p->sem.proxy.ref) == LL(LL(p))) {

                PFla_op_t *thetajoin = L(p->sem.proxy.base1);
                PFla_op_t *lthetajoin, *rthetajoin;
                PFla_op_t *ref = p->sem.proxy.ref;
                unsigned int count = 0;
                bool rewrite = false;
                bool t1_left = false;
                PFarray_t *pred = thetajoin->sem.thetajoin_opt.pred;

                /* first check the dependencies of the left thetajoin input */
                for (i = 0; i < L(thetajoin)->schema.count; i++)
                    for (j = 0; j < clsize (p->sem.proxy.req_cols); j++)
                        if (L(thetajoin)->schema.items[i].name
                            == clat (p->sem.proxy.req_cols, j)) {
                            count++;
                            break;
                        }
                /* left side of the thetajoin corresponds to t2
                   (rthetajoin in the following) */
                if (clsize (p->sem.proxy.req_cols) == count) {
                    rewrite = true;
                    t1_left = false;

                    /* collect the number of additional columns
                       that have to be mapped (either invisible
                       thetajoin columns or thetajoin columns
                       that are not visible anymore at the proxy
                       top */
                    count = 0;
                    for (i = 0; i < PFarray_last (pred); i++)
                        if (!LEFT_VIS_AT (pred, i) ||
                            !PFprop_ocol (p, LEFT_VIS_AT (pred, i)))
                            count++;
                }
                else {
                    count = 0;
                    /* then check the dependencies of the right thetajoin
                       input */
                    for (i = 0; i < R(thetajoin)->schema.count; i++)
                        for (j = 0; j < clsize (p->sem.proxy.req_cols); j++)
                            if (R(thetajoin)->schema.items[i].name
                                == clat (p->sem.proxy.req_cols, j)) {
                                count++;
                                break;
                            }
                    /* right side of the thetajoin corresponds to t2
                       (rthetajoin in the following) */
                    if (clsize (p->sem.proxy.req_cols) == count) {
                        rewrite = true;
                        t1_left = true;

                        /* collect the number of additional columns
                           that have to be mapped (either invisible
                           thetajoin columns or thetajoin columns
                           that are not visible anymore at the proxy
                           top */
                        count = 0;
                        for (i = 0; i < PFarray_last (pred); i++)
                            if (!RIGHT_VIS_AT (pred, i) ||
                                !PFprop_ocol (p, RIGHT_VIS_AT (pred, i)))
                                count++;
                    }
                }

                if (rewrite) {
                    PFalg_proj_t *proj_proxy, *proj_left, *proj_exit;
                    PFalg_col_t   dummy_col;
                    unsigned int  invisible_col_count = count;
                    bool          conflict = false;

                    /* pi_1' */
                    proj_proxy = PFmalloc ((L(p)->schema.count + count) *
                                           sizeof (PFalg_proj_t));
                    /* pi_3' */
                    proj_left = PFmalloc ((L(LL(p))->schema.count + count) *
                                          sizeof (PFalg_proj_t));
                    /* pi_4' */
                    proj_exit = PFmalloc (ref->schema.count *
                                          sizeof (PFalg_proj_t));

                    /* first ensure that no invisible columns conflict either
                       with the variable introduced by the rowid operator
                       or with the columns introduced by the proxy */
                    for (i = 0; i < PFarray_last (pred); i++) {
                        if (L(ref)->sem.rowid.res == (LEFT_AT (pred, i)) ||
                            L(ref)->sem.rowid.res == (RIGHT_AT (pred, i))) {
                            conflict = true;
                            break;
                        }
                        for (j = 0; j < clsize (p->sem.proxy.new_cols); j++)
                            if (clat (p->sem.proxy.new_cols, j) == (LEFT_AT (pred, i)) ||
                                clat (p->sem.proxy.new_cols, j) == (RIGHT_AT (pred, i))) {
                                conflict = true;
                                break;
                            }
                        if (conflict)
                            break;
                    }
                    if (conflict)
                        break;

                    /* Fill in the invisible column names at the beginning
                       of the mapping projections. We do not have to cope
                       with column name conflicts as invisible columns are
                       newly generated ones -- see case la_project.

                       And fill in the visible predicate columns that get
                       pruned inside the proxy. Use new names to avoid
                       any column name conflicts.  */
                    count = 0;
                    if (t1_left) {
                        for (i = 0; i < PFarray_last (pred); i++) {
                            if (!RIGHT_VIS_AT (pred, i)) {
                                PFalg_col_t cur_col = RIGHT_AT (pred, i);
                                proj_proxy[count] = PFalg_proj (cur_col, cur_col);
                                proj_left[count] = PFalg_proj (cur_col, cur_col);
                                count++;
                            }
                            else if (!PFprop_ocol (p, RIGHT_VIS_AT (pred, i))) {
                                PFalg_col_t cur_col = RIGHT_AT (pred, i);
                                PFalg_col_t new_col = PFcol_new (cur_col);
                                proj_proxy[count] = PFalg_proj (cur_col, new_col);
                                proj_left[count] = PFalg_proj (new_col, cur_col);
                                count++;
                            }
                        }
                    } else {
                        for (i = 0; i < PFarray_last (pred); i++) {
                            if (!LEFT_VIS_AT (pred, i)) {
                                PFalg_col_t cur_col = LEFT_AT (pred, i);
                                proj_proxy[count] = PFalg_proj (cur_col, cur_col);
                                proj_left[count] = PFalg_proj (cur_col, cur_col);
                                count++;
                            }
                            else if (!PFprop_ocol (p, LEFT_VIS_AT (pred, i))) {
                                PFalg_col_t cur_col = LEFT_AT (pred, i);
                                PFalg_col_t new_col = PFcol_new (cur_col);
                                proj_proxy[count] = PFalg_proj (cur_col, new_col);
                                proj_left[count] = PFalg_proj (new_col, cur_col);
                                count++;
                            }
                        }
                    }

                    /* get the children of the thetajoin (in a normalized way) */
                    if (t1_left) {
                        lthetajoin = L(thetajoin);
                        rthetajoin = R(thetajoin);
                    } else {
                        rthetajoin = L(thetajoin);
                        lthetajoin = R(thetajoin);
                    }

                    dummy_col = rthetajoin->schema.items[0].name;

                    /* replace the columns of the right argument
                       of the thetajoin by a dummy column
                       of the left argument */
                    count = invisible_col_count;
                    for (i = 0; i < L(LL(p))->sem.proj.count; i++) {
                        for (j = 0; j < lthetajoin->schema.count; j++)
                            if (L(LL(p))->sem.proj.items[i].old ==
                                lthetajoin->schema.items[j].name)
                                break;
                        if (j == lthetajoin->schema.count)
                            proj_left[count++] = L(LL(p))->sem.proj.items[i];
                        else
                            proj_left[count++] =
                                PFalg_proj (L(LL(p))->sem.proj.items[i].new,
                                            dummy_col);
                    }

                    /* prune the columns of the right argument
                       of the thetajoin */
                    count = invisible_col_count;
                    for (i = 0; i < L(p)->sem.proj.count; i++) {
                        for (j = 0; j < lthetajoin->schema.count; j++)
                            if (L(p)->sem.proj.items[i].new ==
                                lthetajoin->schema.items[j].name)
                                break;
                        if (j == lthetajoin->schema.count)
                            proj_proxy[count++] = L(p)->sem.proj.items[i];
                    }

                    /* replace the columns of the right argument
                       of the Cartesian product by a dummy column
                       of the left argument */
                    for (i = 0; i < ref->sem.proj.count; i++) {
                        for (j = 0; j < lthetajoin->schema.count; j++)
                            if (ref->sem.proj.items[i].old ==
                                lthetajoin->schema.items[j].name)
                                break;
                        if (j == lthetajoin->schema.count)
                            proj_exit[i] = ref->sem.proj.items[i];
                        else
                            proj_exit[i] = PFalg_proj (
                                               ref->sem.proj.items[i].new,
                                               dummy_col);
                    }

                    PFla_op_t *new_rowid = PFla_rowid (
                                               PFla_proxy_base (rthetajoin),
                                               L(ref)->sem.rowid.res);

                    *ref = *PFla_project_ (new_rowid,
                                           ref->schema.count,
                                           proj_exit);

                    rthetajoin = PFla_proxy (
                                     PFla_project_ (
                                         PFla_eqjoin (
                                             PFla_project_ (
                                                 new_rowid,
                                                 L(LL(p))->schema.count
                                                 + invisible_col_count,
                                                 proj_left),
                                             R(LL(p)),
                                             LL(p)->sem.eqjoin.col1,
                                             LL(p)->sem.eqjoin.col2),
                                         count, proj_proxy),
                                     1,
                                     ref,
                                     L(new_rowid),
                                     p->sem.proxy.new_cols,
                                     p->sem.proxy.req_cols);

                    if (t1_left)
                        *p = *(thetajoin_opt (
                                   lthetajoin,
                                   rthetajoin,
                                   pred));
                    else
                        *p = *(thetajoin_opt (
                                   rthetajoin,
                                   lthetajoin,
                                   pred));

                    modified = true;
                    break;
                }
            }
            break;

        case la_proxy_base:
        case la_string_join:
        case la_dummy:
            break;
    }

    return modified;
}

/**
 * intro_internal_thetajoin replaces all thetajoin
 * operator by an intermediate internal representation
 * that is able to cope with partial predicates.
 */
static void
intro_internal_thetajoin (PFla_op_t *p)
{
    unsigned int i;

    /* rewrite each node only once */
    if (SEEN(p))
        return;

    /* apply complex optimization for children */
    for (i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        intro_internal_thetajoin (p->child[i]);

    /* replace original cross operators by the internal
       variant */
    if (p->kind == la_cross) {
        PFarray_t *pred = PFarray (sizeof (pred_struct), 5);
        *p = *thetajoin_opt (L(p), R(p), pred);
    }
    /* replace original thetajoin operators by the internal
       variant */
    else if (p->kind == la_thetajoin) {
        PFarray_t *pred = PFarray (sizeof (pred_struct), 5);

        for (i = 0; i < p->sem.thetajoin.count; i++)
            *(pred_struct *) PFarray_add (pred) =
                (pred_struct) {
                    .comp      = p->sem.thetajoin.pred[i].comp,
                    .left      = p->sem.thetajoin.pred[i].left,
                    .right     = p->sem.thetajoin.pred[i].right,
                    .res       = col_NULL,
                    .persist   = true,
                    .left_vis  = true,
                    .right_vis = true,
                    .res_vis   = false
                };

        *p = *thetajoin_opt (L(p), R(p), pred);
    }

    SEEN(p) = true;
}

/**
 * remove_thetajoin_opt replaces all intermediate
 * thetajoin operators by normal ones. For every
 * comparison whose result is still visible a comparison
 * operator is introduced and a pruning projection on
 * top of the operator chain ensures that only the
 * expected column names are visible.
 */
static void
remove_thetajoin_opt (PFla_op_t *p)
{
    unsigned int i;

    /* rewrite each node only once */
    if (SEEN(p))
        return;

    /* apply complex optimization for children */
    for (i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        remove_thetajoin_opt (p->child[i]);

    /* replace the intermediate representation of the thetajoin */
    if (p->kind == la_internal_op) {
        unsigned int  count = 0;
        PFalg_sel_t  *pred_new;
        PFarray_t    *pred = p->sem.thetajoin_opt.pred;
        PFla_op_t    *thetajoin;
        PFalg_col_t   cur_col;
        PFalg_proj_t *proj, *lproj;
        unsigned int  lcount = 0;

        /* add a pruning projection underneath the thetajoin operator to
           get rid of duplicate columns. */
        lproj = PFmalloc (L(p)->schema.count * sizeof (PFalg_proj_t));

        /* fill pruning projection list */
        for (unsigned j = 0; j < L(p)->schema.count; j++) {
            cur_col = L(p)->schema.items[j].name;
            for (i = 0; i < R(p)->schema.count; i++)
                if (cur_col == R(p)->schema.items[i].name)
                    break;
            if (i == R(p)->schema.count)
                lproj[lcount++] = PFalg_proj (cur_col, cur_col);
        }
        /* add projection if necessary */
        if (lcount < L(p)->schema.count)
            L(p) = PFla_project_ (L(p), lcount, lproj);

        /* collect the number of persistent predicates */
        for (i = 0; i < PFarray_last (pred); i++)
            if (PERS_AT(pred, i)) count++;

        if (count) {
            /* copy the list of persistent predicates */
            pred_new = PFmalloc (count * sizeof (PFalg_sel_t));
            count = 0;
            for (i = 0; i < PFarray_last (pred); i++)
                if (PERS_AT(pred, i)) {
                    pred_new[count++] = PFalg_sel (COMP_AT (pred, i),
                                                   LEFT_AT (pred, i),
                                                   RIGHT_AT (pred, i));
                }

            /* generate a new thetajoin operator
               (with persistent predicates only) */
            thetajoin = PFla_thetajoin (L(p), R(p), count, pred_new);
        }
        else
            thetajoin = PFla_cross (L(p), R(p));

        SEEN(thetajoin) = true;


        /* add an operator for every result column that is generated */
        for (i = 0; i < PFarray_last (pred); i++)
            if (RES_VIS_AT(pred, i)) {
                switch (COMP_AT(pred, i)) {
                    case alg_comp_eq:
                        thetajoin = PFla_eq (thetajoin,
                                             RES_AT (pred, i),
                                             LEFT_AT (pred, i),
                                             RIGHT_AT (pred, i));
                        break;

                    case alg_comp_gt:
                        thetajoin = PFla_gt (thetajoin,
                                             RES_AT (pred, i),
                                             LEFT_AT (pred, i),
                                             RIGHT_AT (pred, i));
                        break;

                    case alg_comp_ge:
                    {
                        /* create a new column name to split up
                           this predicate into two logical operators */
                        PFalg_col_t new_col;
                        /* get a new column name */
                        new_col = PFcol_new (RES_AT(pred, i));

                        thetajoin = PFla_not (
                                        PFla_gt (thetajoin,
                                                 new_col,
                                                 RIGHT_AT (pred, i),
                                                 LEFT_AT (pred, i)),
                                        RES_AT (pred, i),
                                        new_col);
                    } break;

                    case alg_comp_lt:
                        thetajoin = PFla_gt (thetajoin,
                                             RES_AT (pred, i),
                                             RIGHT_AT (pred, i),
                                             LEFT_AT (pred, i));
                        break;

                    case alg_comp_le:
                    {
                        /* create a new column name to split up
                           this predicate into two logical operators */
                        PFalg_col_t new_col;
                        /* get a new column name */
                        new_col = PFcol_new (RES_AT(pred, i));

                        thetajoin = PFla_not (
                                        PFla_gt (thetajoin,
                                                 new_col,
                                                 LEFT_AT (pred, i),
                                                 RIGHT_AT (pred, i)),
                                        RES_AT (pred, i),
                                        new_col);
                    } break;

                    case alg_comp_ne:
                    {
                        /* create a new column name to split up
                           this predicate into two logical operators */
                        PFalg_col_t new_col;
                        /* get a new column name */
                        new_col = PFcol_new (RES_AT(pred, i));

                        thetajoin = PFla_not (
                                        PFla_eq (thetajoin,
                                                 new_col,
                                                 LEFT_AT (pred, i),
                                                 RIGHT_AT (pred, i)),
                                        RES_AT (pred, i),
                                        new_col);
                    } break;
                }
                SEEN(thetajoin) = true;
            }

        /* add a pruning projection on top of the thetajoin operator to
           throw away all unused columns. */
        proj = PFmalloc (p->schema.count * sizeof (PFalg_proj_t));

        /* fill pruning projection list */
        for (i = 0; i < p->schema.count; i++) {
            cur_col = p->schema.items[i].name;
            proj[i] = PFalg_proj (cur_col, cur_col);
        }

        /* place a renaming projection on top of the thetajoin */
        *p = *PFla_project_ (thetajoin, p->schema.count, proj);
    }

    SEEN(p) = true;
}

/**
 * Invoke thetajoin optimization. We try to move thetajoin operators
 * as high in the plans as possible. If applicable we furthermore extend
 * the list of predicates in the thetajoin.
 */
PFla_op_t *
PFalgopt_thetajoin (PFla_op_t *root)
{
    /* replace thetajoin operator by the internal thetajoin
       representation needed for this optimization phase */
    intro_internal_thetajoin (root);
    PFla_dag_reset (root);

    /* Traverse the DAG bottom up and look for op-thetajoin
       operator pairs. As long as we find a rewrite we start
       a new traversal. */
    while (opt_mvd (root))
        PFla_dag_reset (root);
    PFla_dag_reset (root);

    /* replace the internal thetajoin representation by
       normal thetajoins and generate operators for all
       operators that could not be integrated in the normal
       thetajoins. */
    remove_thetajoin_opt (root);
    PFla_dag_reset (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
