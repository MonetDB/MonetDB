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
#include "oops.h"

/* apply cse before rewriting */
#include "algebra_cse.h"
/* mnemonic algebra constructors */
#include "logical_mnemonic.h"

/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])
/** starting from p, make two steps left */
#define LL(p) L(L(p))
/** starting from p, make a step left, then a step right */
#define LR(p) R(L(p))
/** starting from p, make a step right, then a step left */
#define RL(p) L(R(p))
/** starting from p, make two steps right */
#define RR(p) R(R(p))
/** and so on ... */
#define LLL(p) L(L(L(p)))
#define LRL(p) L(R(L(p)))
#define RLL(p) L(L(R(p)))
#define RRL(p) L(R(R(p)))

#define SEEN(p) ((p)->bit_dag)

struct pred_struct {
    PFalg_comp_t comp;      /* comparison */
    PFalg_att_t  left;      /* left selection column */
    PFalg_att_t  right;     /* right selection column */
    PFalg_att_t  res;       /* boolean result column of the comparison */
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

    /* copy schema from argument 'n2', check for duplicate attribute names */
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
                       "duplicate attribute '%s' in thetajoin",
                       PFatt_str (RES_AT(pred, i)));

            if (PFprop_ocol (n2, RES_AT(pred, i)))
               PFoops (OOPS_FATAL,
                       "duplicate attribute '%s' in thetajoin",
                       PFatt_str (RES_AT(pred, i)));
#endif
        }

#ifndef NDEBUG
        /* make sure that all input columns (to the thetajoin are available */
        if (!PFprop_ocol (n1, LEFT_AT(pred, i)))
           PFoops (OOPS_FATAL,
                   "attribute '%s' not found in thetajoin",
                   PFatt_str (LEFT_AT(pred, i)));

        if (!PFprop_ocol (n2, RIGHT_AT(pred, i)))
           PFoops (OOPS_FATAL,
                   "attribute '%s' not found in thetajoin",
                   PFatt_str (RIGHT_AT(pred, i)));
#endif
    }

    /* fix the schema count */
    ret->schema.count = count;

    return ret;
}

/**
 * resolve_name_conflict renames a column of the thetajoin
 * predicates if it conflicts with a newly introduced column
 * name @a att. (This can of course only happen if the
 * conflicting column names are not visible anymore.)
 */
static void
resolve_name_conflict (PFla_op_t *n, PFalg_att_t att)
{
    PFalg_att_t used_cols = 0;
    bool conflict_left = false,
         conflict_right = false;
    PFarray_t *pred;
    unsigned int i;

    assert (n->kind == la_thetajoin);

    pred = n->sem.thetajoin_opt.pred;

    /* collect all the names in use */
    used_cols = att;
    for (i = 0; i < n->schema.count; i++)
        used_cols = used_cols | n->schema.items[i].name;

    /* check for conflicts */
    for (i = 0; i < PFarray_last (pred); i++) {
        if (LEFT_AT(pred, i) == att) {
            /* If the input to the predicate is used above
               while it is still visible a thinking
               error has sneaked in. */
            assert (LEFT_VIS_AT (pred, i) == false);
            conflict_left = true;
        }
        if (RIGHT_AT(pred, i) == att) {
            /* If the input to the predicate is used above
               while it is still visible a thinking
               error has sneaked in. */
            assert (RIGHT_VIS_AT (pred, i) == false);
            conflict_right = true;
        }

        /* collect all the names in use */
        used_cols = used_cols | LEFT_AT(pred, i);
        used_cols = used_cols | RIGHT_AT(pred, i);
    }

    /* solve conflicts by generating new column name that
       replaces the conflicting one */
    if (conflict_left) {
        PFalg_proj_t *proj = PFmalloc (L(n)->schema.count *
                                       sizeof (PFalg_proj_t));
        PFalg_att_t new_col, cur_col;

        /* generate new column name */
        new_col = PFalg_ori_name (PFalg_unq_name (att, 0),
                                  ~used_cols);
        used_cols = used_cols | new_col;

        /* fill renaming projection list */
        for (i = 0; i < L(n)->schema.count; i++) {
            cur_col = L(n)->schema.items[i].name;

            if (cur_col != att)
                proj[i] = PFalg_proj (cur_col, cur_col);
            else
                proj[i] = PFalg_proj (new_col, att);
        }

        /* place a renaming projection underneath the thetajoin */
        L(n) = PFla_project_ (L(n), L(n)->schema.count, proj);

        /* update all the references in the predicate list */
        for (i = 0; i < PFarray_last (pred); i++)
            if (LEFT_AT(pred, i) == att)
                LEFT_AT(pred, i) = new_col;
    }

    /* solve conflicts by generating new column name that
       replaces the conflicting one */
    if (conflict_right) {
        PFalg_proj_t *proj = PFmalloc (R(n)->schema.count *
                                       sizeof (PFalg_proj_t));
        PFalg_att_t new_col, cur_col;

        /* generate new column name */
        new_col = PFalg_ori_name (PFalg_unq_name (att, 0),
                                  ~used_cols);
        used_cols = used_cols | new_col;

        /* fill renaming projection list */
        for (i = 0; i < R(n)->schema.count; i++) {
            cur_col = R(n)->schema.items[i].name;

            if (cur_col != att)
                proj[i] = PFalg_proj (cur_col, cur_col);
            else
                proj[i] = PFalg_proj (new_col, att);
        }

        /* place a renaming projection underneath the thetajoin */
        R(n) = PFla_project_ (R(n), R(n)->schema.count, proj);

        /* update all the references in the predicate list */
        for (i = 0; i < PFarray_last (pred); i++)
            if (RIGHT_AT(pred, i) == att)
                RIGHT_AT(pred, i) = new_col;
    }
}

/**
 * resolve_name_conflicts renames columns of the thetajoin
 * predicates if it conflicts with a set of newly introduced
 * column names in a schema (@a schema). (This can of course
 * only happen if the conflicting column names are not visible
 * anymore.)
 */
static void
resolve_name_conflicts (PFla_op_t *n, PFalg_schema_t schema)
{
    PFalg_att_t used_cols = 0, conf_cols = 0;
    bool conflict_left = false,
         conflict_right = false;
    PFarray_t *pred;
    unsigned int i, j;

    assert (n->kind == la_thetajoin);

    pred = n->sem.thetajoin_opt.pred;

    /* collect all the names in use */
    for (i = 0; i < schema.count; i++)
        conf_cols = conf_cols | schema.items[i].name;
    used_cols = conf_cols;
    for (i = 0; i < n->schema.count; i++)
        used_cols = used_cols | n->schema.items[i].name;
    
    /* check for conflicts */
    for (i = 0; i < PFarray_last (pred); i++) {
        if (LEFT_AT(pred, i) & conf_cols) {
            /* If the input to the predicate is used above
               while it is still visible a thinking
               error has sneaked in. */
            assert (LEFT_VIS_AT (pred, i) == false);
            conflict_left = true;
        }
        if (RIGHT_AT(pred, i) & conf_cols) {
            /* If the input to the predicate is used above
               while it is still visible a thinking
               error has sneaked in. */
            assert (RIGHT_VIS_AT (pred, i) == false);
            conflict_right = true;
        }

        /* collect all the names in use */
        used_cols = used_cols | LEFT_AT(pred, i);
        used_cols = used_cols | RIGHT_AT(pred, i);
    }

    /* solve conflicts by generating new column name that
       replaces the conflicting one */
    if (conflict_left) {
        PFalg_proj_t *proj = PFmalloc (L(n)->schema.count *
                                       sizeof (PFalg_proj_t));
        PFalg_att_t new_col, cur_col;

        /* fill renaming projection list */
        for (i = 0; i < L(n)->schema.count; i++) {
            cur_col = L(n)->schema.items[i].name;

            if (cur_col & conf_cols) {
                /* generate new column name */
                new_col = PFalg_ori_name (PFalg_unq_name (cur_col, 0),
                                          ~used_cols);
                used_cols = used_cols | new_col;

                proj[i] = PFalg_proj (new_col, cur_col);
                
                /* update all the references in the predicate list */
                for (j = 0; j < PFarray_last (pred); j++)
                    if (LEFT_AT(pred, j) == cur_col)
                        LEFT_AT(pred, j) = new_col;
            }
            else
                proj[i] = PFalg_proj (cur_col, cur_col);
        }

        /* place a renaming projection underneath the thetajoin */
        L(n) = PFla_project_ (L(n), L(n)->schema.count, proj);
    }

    /* solve conflicts by generating new column name that
       replaces the conflicting one */
    if (conflict_right) {
        PFalg_proj_t *proj = PFmalloc (R(n)->schema.count *
                                       sizeof (PFalg_proj_t));
        PFalg_att_t new_col, cur_col;

        /* fill renaming projection list */
        for (i = 0; i < R(n)->schema.count; i++) {
            cur_col = R(n)->schema.items[i].name;

            if (cur_col & conf_cols) {
                /* generate new column name */
                new_col = PFalg_ori_name (PFalg_unq_name (cur_col, 0),
                                          ~used_cols);
                used_cols = used_cols | new_col;

                proj[i] = PFalg_proj (new_col, cur_col);
                
                /* update all the references in the predicate list */
                for (j = 0; j < PFarray_last (pred); j++)
                    if (RIGHT_AT(pred, j) == cur_col)
                        RIGHT_AT(pred, j) = new_col;
            }
            else
                proj[i] = PFalg_proj (cur_col, cur_col);
        }

        /* place a renaming projection underneath the thetajoin */
        R(n) = PFla_project_ (R(n), R(n)->schema.count, proj);
    }
}

/**
 * check for a thetajoin operator
 */
static bool
is_tj (PFla_op_t *p)
{
    return (p->kind == la_thetajoin);
}

/**
 * thetajoin_identical checks if the semantical
 * information of two thetajoin operators is the same.
 */
static bool
thetajoin_identical (PFla_op_t *a, PFla_op_t *b)
{
    PFarray_t *pred1, *pred2;

    assert (a->kind == la_thetajoin &&
            b->kind == la_thetajoin);

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

    assert (p->kind == la_thetajoin);

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
                                      PFalg_att_t,
                                      PFalg_att_t,
                                      PFalg_att_t),
                  bool integrate,
                  PFalg_comp_t comp)
{
    PFalg_att_t res, att1, att2;
    bool modified = false,
         match    = false;

    if (is_tj (L(p))) {
        bool switch_left = PFprop_ocol (LL(p), p->sem.binary.att1) &&
                           PFprop_ocol (LL(p), p->sem.binary.att2);
        bool switch_right = PFprop_ocol (LR(p), p->sem.binary.att1) &&
                            PFprop_ocol (LR(p), p->sem.binary.att2);

        if (switch_left && switch_right) {
            resolve_name_conflict (L(p), p->sem.binary.res);
            *p = *(thetajoin_opt (
                       op (LL(p),
                           p->sem.binary.res,
                           p->sem.binary.att1,
                           p->sem.binary.att2),
                       op (LR(p),
                           p->sem.binary.res,
                           p->sem.binary.att1,
                           p->sem.binary.att2),
                       L(p)->sem.thetajoin_opt.pred));
            modified = true;
        }
        else if (switch_left) {
            resolve_name_conflict (L(p), p->sem.binary.res);
            *p = *(thetajoin_opt (
                       op (LL(p),
                           p->sem.binary.res,
                           p->sem.binary.att1,
                           p->sem.binary.att2),
                       LR(p),
                       L(p)->sem.thetajoin_opt.pred));
            modified = true;
        }
        else if (switch_right) {
            resolve_name_conflict (L(p), p->sem.binary.res);
            *p = *(thetajoin_opt (
                       LL(p),
                       op (LR(p),
                           p->sem.binary.res,
                           p->sem.binary.att1,
                           p->sem.binary.att2),
                       L(p)->sem.thetajoin_opt.pred));
            modified = true;
        }
        else if (integrate &&
            PFprop_ocol (LL(p), p->sem.binary.att1) &&
            PFprop_ocol (LR(p), p->sem.binary.att2)) {
            res  = p->sem.binary.res;
            att1 = p->sem.binary.att1;
            att2 = p->sem.binary.att2;
            match = true;
        }
        else if (integrate &&
            PFprop_ocol (LL(p), p->sem.binary.att2) &&
            PFprop_ocol (LR(p), p->sem.binary.att1)) {
            comp = alg_comp_gt ? alg_comp_lt : comp;
            res  = p->sem.binary.res;
            att1 = p->sem.binary.att2;
            att2 = p->sem.binary.att1;
            match = true;
        }

        /* integrate the comparison as a possible new predicate
           in the thetajoin operator */
        if (match) {
            /* create a new thetajoin operator ... */
            *p = *(thetajoin_opt (LL(p),
                                  LR(p),
                                  L(p)->sem.thetajoin_opt.pred));

            /* ... and add a new predicate */
            *(pred_struct *) PFarray_add (p->sem.thetajoin_opt.pred) =
                (pred_struct) {
                    .comp      = comp,
                    .left      = att1,
                    .right     = att2,
                    .res       = res,
                    .persist   = false,
                    .left_vis  = true,
                    .right_vis = true,
                    .res_vis   = true
                };
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
static bool
opt_mvd (PFla_op_t *p)
{
    bool modified = false;
    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return modified;
    else
        SEEN(p) = true;

    /* apply optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        modified = opt_mvd (p->child[i]) || modified;

    /**
     * In the following action code we try to propagate thetajoin
     * operators up the DAG as far as possible.
     */

    /* action code */
    switch (p->kind) {
        case la_serialize_seq:
        case la_serialize_rel:
            break;

        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
            /* nothing to do -- no thetajoin may sit underneath */
            break;

        case la_attach:
            if (is_tj (L(p))) {
                resolve_name_conflict (L(p), p->sem.attach.res);

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
        case la_cross_mvd:
            if (is_tj (L(p))) {
                resolve_name_conflicts (L(p), R(p)->schema);
                *p = *(thetajoin_opt (LL(p),
                                  cross (LR(p), R(p)),
                                  L(p)->sem.thetajoin_opt.pred));
                modified = true;
            }
            else if (is_tj (R(p))) {
                resolve_name_conflicts (R(p), L(p)->schema);
                *p = *(thetajoin_opt (RL(p),
                                  cross (RR(p), L(p)),
                                  R(p)->sem.thetajoin_opt.pred));
                modified = true;
            }
            break;

        case la_eqjoin_unq:
            break;

        case la_eqjoin:
            /* Move the independent expression (the one without
               join attribute) up the DAG. */
            if (is_tj (L(p))) {
                resolve_name_conflicts (L(p), R(p)->schema);
                if (PFprop_ocol (LL(p), p->sem.eqjoin.att1))
                    *p = *(thetajoin_opt (eqjoin (LL(p),
                                                  R(p),
                                                  p->sem.eqjoin.att1,
                                                  p->sem.eqjoin.att2),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));
                else
                    *p = *(thetajoin_opt (LL(p),
                                          eqjoin (LR(p),
                                                  R(p),
                                                  p->sem.eqjoin.att1,
                                                  p->sem.eqjoin.att2),
                                          L(p)->sem.thetajoin_opt.pred));

                modified = true;
                break;

            }
            if (is_tj (R(p))) {
                resolve_name_conflicts (R(p), L(p)->schema);
                if (PFprop_ocol (RL(p), p->sem.eqjoin.att2))
                    *p = *(thetajoin_opt (eqjoin (L(p),
                                                  RL(p),
                                                  p->sem.eqjoin.att1,
                                                  p->sem.eqjoin.att2),
                                          RR(p),
                                          R(p)->sem.thetajoin_opt.pred));
                else
                    *p = *(thetajoin_opt (RL(p),
                                          eqjoin (L(p),
                                                  RR(p),
                                                  p->sem.eqjoin.att1,
                                                  p->sem.eqjoin.att2),
                                          R(p)->sem.thetajoin_opt.pred));

                modified = true;
                break;
            }
            break;

        case la_semijoin:
            /* Move the independent expression (the one without
               join attribute) up the DAG. */
            if (is_tj (L(p))) {
                if (PFprop_ocol (LL(p), p->sem.eqjoin.att1))
                    *p = *(thetajoin_opt (semijoin (LL(p),
                                                    R(p),
                                                    p->sem.eqjoin.att1,
                                                    p->sem.eqjoin.att2),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));
                else
                    *p = *(thetajoin_opt (LL(p),
                                          semijoin (LR(p),
                                                    R(p),
                                                    p->sem.eqjoin.att1,
                                                    p->sem.eqjoin.att2),
                                          L(p)->sem.thetajoin_opt.pred));

                modified = true;
                break;

            }
            break;

        case la_thetajoin:
            /* ignore adjacent thetajoins for now */
            break;

        case la_project:
            /* Split project operator and push it beyond the thetajoin. */
            if (is_tj (L(p))) {
                PFarray_t    *pred = PFarray_copy (L(p)->sem.thetajoin_opt.pred);
                unsigned int  i,
                              j,
                              count1 = 0,
                              count2 = 0;
                PFalg_proj_t *proj_list1,
                             *proj_list2;
                PFalg_att_t   used_cols = 0;

                /* collect all the column names that are already in use */
                for (i = 0; i < p->sem.proj.count; i++)
                    used_cols = used_cols | p->sem.proj.items[i].new;

                /* The following for loop does multiple things:
                   1.) removes all invisible result columns
                   2.) prunes non persistent predicates whose result column
                       is invisible
                   3.) updates the names of all visible result columns
                       (based on the projection list)
                   4.) marks join attributes missing in the projection list
                       as invisible
                   5.) updates the names of all visible join attributes
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
                            RES_AT (pred, i) = att_NULL;

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
                        } else
                            /* update the column name of the result attribute */
                            RES_AT (pred, i) = p->sem.proj.items[j].new;
                    }

                    if (LEFT_VIS_AT (pred, i)) {
                        for (j = 0; j < p->sem.proj.count; j++)
                            if (LEFT_AT (pred, i) == p->sem.proj.items[j].old)
                                break;

                        if (j == p->sem.proj.count)
                            /* mark unreferenced join inputs invisible */
                            LEFT_VIS_AT (pred, i) = false;
                        else
                            /* update the column name of all referenced
                               join attributes */
                            LEFT_AT (pred, i) = p->sem.proj.items[j].new;
                    }
                    if (!LEFT_VIS_AT (pred, i))
                        count1++;

                    /* collect all the column names that are already in use */
                    used_cols = used_cols | LEFT_AT (pred, i);

                    if (RIGHT_VIS_AT (pred, i)) {
                        for (j = 0; j < p->sem.proj.count; j++)
                            if (RIGHT_AT (pred, i) == p->sem.proj.items[j].old)
                                break;

                        if (j == p->sem.proj.count)
                            /* mark unreferenced join inputs invisible */
                            RIGHT_VIS_AT (pred, i) = false;
                        else
                            /* update the column name of all referenced
                               join attributes */
                            RIGHT_AT (pred, i) = p->sem.proj.items[j].new;
                    }
                    if (!RIGHT_VIS_AT (pred, i))
                        count2++;

                    /* collect all the column names that are already in use */
                    used_cols = used_cols | RIGHT_AT (pred, i);
                }

                /* create first projection list */
                proj_list1 = PFmalloc ((p->schema.count + count1) *
                                       sizeof (PFalg_proj_t));
                count1 = 0;

                for (unsigned int i = 0; i < LL(p)->schema.count; i++)
                    for (unsigned int j = 0; j < p->sem.proj.count; j++)
                        if (LL(p)->schema.items[i].name
                            == p->sem.proj.items[j].old) {
                            proj_list1[count1++] = p->sem.proj.items[j];
                        }

                /* create second projection list */
                proj_list2 = PFmalloc ((p->schema.count + count2) *
                                       sizeof (PFalg_proj_t));
                count2 = 0;

                for (unsigned int i = 0; i < LR(p)->schema.count; i++)
                    for (unsigned int j = 0; j < p->sem.proj.count; j++)
                        if (LR(p)->schema.items[i].name
                            == p->sem.proj.items[j].old) {
                            proj_list2[count2++] = p->sem.proj.items[j];
                        }

                /* used_cols now contains all the new column names
                   of the projection list and all the old column names
                   of the join columns. A mapping for all the remaining
                   invisible join columns thus does not use a column name
                   that may be used lateron. In consequence the renaming
                   of the invisible join columns is kept at a minimum. */

                for (i = 0; i < PFarray_last (pred); i++) {
                    if (!LEFT_VIS_AT (pred, i)) {
                        /* try to find a matching slot in the projection list */
                        for (j = 0; j < count1; j++)
                            if (LEFT_AT (pred, i) == proj_list1[j].old)
                                break;

                        if (j == count1) {
                            /* introduce a new column name ... */
                            PFalg_att_t new_col;
                            new_col = PFalg_ori_name (
                                          PFalg_unq_name (LEFT_AT(pred, i), 0),
                                          ~used_cols);
                            used_cols = used_cols | new_col;

                            /* ... and add the mapping
                               to the left projection list */
                            proj_list1[count1++] = PFalg_proj (new_col,
                                                               LEFT_AT(pred, i));
                        }
                        /* update the column name of the referenced
                           join attributes */
                        LEFT_AT (pred, i) = proj_list1[j].new;
                    }

                    if (!RIGHT_VIS_AT (pred, i)) {
                        /* try to find a matching slot in the projection list */
                        for (j = 0; j < count2; j++)
                            if (RIGHT_AT (pred, i) == proj_list2[j].old)
                                break;

                        if (j == count2) {
                            /* introduce a new column name ... */
                            PFalg_att_t new_col;
                            new_col = PFalg_ori_name (
                                          PFalg_unq_name (RIGHT_AT(pred, i), 0),
                                          ~used_cols);
                            used_cols = used_cols | new_col;

                            /* ... and add the mapping
                               to the right projection list */
                            proj_list2[count2++] = PFalg_proj (new_col,
                                                               RIGHT_AT(pred, i));
                        }
                        /* update the column name of the referenced
                           join attributes */
                        RIGHT_AT (pred, i) = proj_list2[j].new;
                    }
                }

                /* Ensure that both arguments add at least one column to
                   the result. */
                assert (count1 && count2);

                *p = *(thetajoin_opt (
                           PFla_project_ (LL(p), count1, proj_list1),
                           PFla_project_ (LR(p), count2, proj_list2),
                           pred));
                modified = true;
            }
            break;

        case la_select:
            if (is_tj (L(p))) {
                if (PFprop_ocol (LL(p), p->sem.select.att)) {
                    *p = *(thetajoin_opt (select_ (LL(p),
                                                   p->sem.select.att),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));
                } else if (PFprop_ocol (LR(p), p->sem.select.att)) {
                    *p = *(thetajoin_opt (LL(p),
                                          select_ (LR(p),
                                                   p->sem.select.att),
                                          L(p)->sem.thetajoin_opt.pred));
                } else {
                    /* att is referenced in a result column of the thetajoin */
                    PFalg_att_t sel_att = p->sem.select.att;
                    PFarray_t  *pred;

                    /* create a new thetajoin operator ... */
                    *p = *(thetajoin_opt (LL(p),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));

                    /* ... and update all the predicates
                       that are affected by the selection */
                    pred = p->sem.thetajoin_opt.pred;
                    for (unsigned int i = 0; i < PFarray_last (pred); i++)
                        if (RES_VIS_AT (pred, i) && RES_AT (pred, i) == sel_att)
                            /* make the join condition persistent */
                            PERS_AT (pred, i) = true;
                }

                modified = true;
            }
            break;

        case la_pos_select:
            /* An expression that does not contain any sorting column
               required by the positional select operator, but contains
               the partitioning attribute is independent of the positional
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
                for (unsigned int i = 0; i < LL(p)->schema.count; i++) {
                    if (LL(p)->schema.items[i].name == p->sem.pos_sel.part)
                        lpart = true;
                    for (unsigned int j = 0;
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
                for (unsigned int i = 0; i < LR(p)->schema.count; i++) {
                    if (LR(p)->schema.items[i].name == p->sem.pos_sel.part)
                        rpart = true;
                    for (unsigned int j = 0;
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
                                  att_NULL),
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
                                  att_NULL),
                              LR(p),
                              L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }
            }
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

                for (unsigned int i = 0; i < p->sem.fun_1to1.refs.count; i++) {
                    switch_left  = switch_left &&
                                   PFprop_ocol (LL(p),
                                                p->sem.fun_1to1.refs.atts[i]);
                    switch_right = switch_right &&
                                   PFprop_ocol (LR(p),
                                                p->sem.fun_1to1.refs.atts[i]);
                }

                if (switch_left && switch_right) {
                    resolve_name_conflict (L(p), p->sem.fun_1to1.res);
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
                    resolve_name_conflict (L(p), p->sem.fun_1to1.res);
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
                    resolve_name_conflict (L(p), p->sem.fun_1to1.res);
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
                bool switch_left = PFprop_ocol (LL(p), p->sem.unary.att);
                bool switch_right = PFprop_ocol (LR(p), p->sem.unary.att);

                if (switch_left && switch_right) {
                    resolve_name_conflict (L(p), p->sem.unary.res);
                    *p = *(thetajoin_opt (
                               PFla_not (LL(p),
                                         p->sem.unary.res,
                                         p->sem.unary.att),
                               PFla_not (LR(p),
                                         p->sem.unary.res,
                                         p->sem.unary.att),
                               L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_left) {
                    resolve_name_conflict (L(p), p->sem.unary.res);
                    *p = *(thetajoin_opt (
                               PFla_not (LL(p),
                                         p->sem.unary.res,
                                         p->sem.unary.att),
                               LR(p),
                               L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_right) {
                    resolve_name_conflict (L(p), p->sem.unary.res);
                    *p = *(thetajoin_opt (
                               LL(p),
                               PFla_not (LR(p),
                                         p->sem.unary.res,
                                         p->sem.unary.att),
                               L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else {
                    /* att is referenced in a result column of the thetajoin */
                    PFalg_att_t res = p->sem.unary.res,
                                att = p->sem.unary.att;
                    PFarray_t  *pred;
                    unsigned int i;
                    PFalg_comp_t comp;

                    /* pacify picky compilers: one does not like no value (gcc)
                       and the other one does not like a dummy value (icc) */
                    comp = alg_comp_eq;

                    /* make sure that column res is not used as join argument */
                    resolve_name_conflict (L(p), res);
                    /* create a new thetajoin operator ... */
                    *p = *(thetajoin_opt (LL(p),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));

                    /* find the result column ... */
                    pred = p->sem.thetajoin_opt.pred;
                    for (i = 0; i < PFarray_last (pred); i++)
                        if (RES_VIS_AT (pred, i) && RES_AT (pred, i) == att)
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

                    /* ... and introduce a new predicate
                       based on the already existing one */
                    *(pred_struct *) PFarray_add (pred) =
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
        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_count:
        case la_seqty1:
        case la_all:
            break;

        case la_rownum:
            /* An expression that does not contain any sorting column
               required by the rownum operator, but contains the partitioning
               attribute is independent of the rownum. The translation thus
               moves the expression above the rownum and removes its partitioning
               column. */
            if (is_tj (L(p)) &&
                p->sem.rownum.part) {
                bool lpart = false,
                     rpart = false;
                unsigned int lsortby = 0,
                             rsortby = 0;

                /* first check for the required columns
                   in the left thetajoin input */
                for (unsigned int i = 0; i < LL(p)->schema.count; i++) {
                    if (LL(p)->schema.items[i].name == p->sem.rownum.part)
                        lpart = true;
                    for (unsigned int j = 0;
                         j < PFord_count (p->sem.rownum.sortby);
                         j++)
                        if (LL(p)->schema.items[i].name
                            == PFord_order_col_at (
                                   p->sem.rownum.sortby,
                                   j)) {
                            lsortby++;
                            break;
                        }
                }
                /* and then check for the required columns
                   in the right thetajoin input */
                for (unsigned int i = 0; i < LR(p)->schema.count; i++) {
                    if (LR(p)->schema.items[i].name == p->sem.rownum.part)
                        rpart = true;
                    for (unsigned int j = 0;
                         j < PFord_count (p->sem.rownum.sortby);
                         j++)
                        if (LR(p)->schema.items[i].name
                            == PFord_order_col_at (
                                   p->sem.rownum.sortby,
                                   j)) {
                            rsortby++;
                            break;
                        }
                }

                if (lpart && rsortby == PFord_count (p->sem.rownum.sortby)) {
                    resolve_name_conflict (L(p), p->sem.rownum.res);
                    *p = *(thetajoin_opt (
                              LL(p),
                              rownum (
                                  LR(p),
                                  p->sem.rownum.res,
                                  p->sem.rownum.sortby,
                                  att_NULL),
                              L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }

                if (rpart && lsortby == PFord_count (p->sem.rownum.sortby)) {
                    resolve_name_conflict (L(p), p->sem.rownum.res);
                    *p = *(thetajoin_opt (
                              rownum (
                                  LL(p),
                                  p->sem.rownum.res,
                                  p->sem.rownum.sortby,
                                  att_NULL),
                              LR(p),
                              L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }
            }
            break;

        case la_rank:
            /* An expression that does not contain any sorting column
               required by the rank operator is independent of the rank.
               The translation thus moves the expression above the rank
               column. */
            if (is_tj (L(p))) {
                unsigned int lsortby = 0,
                             rsortby = 0;

                /* first check for the required columns
                   in the left thetajoin input */
                for (unsigned int i = 0; i < LL(p)->schema.count; i++)
                    for (unsigned int j = 0;
                         j < PFord_count (p->sem.rank.sortby);
                         j++)
                        if (LL(p)->schema.items[i].name
                            == PFord_order_col_at (
                                   p->sem.rank.sortby,
                                   j)) {
                            lsortby++;
                            break;
                        }

                /* and then check for the required columns
                   in the right thetajoin input */
                for (unsigned int i = 0; i < LR(p)->schema.count; i++)
                    for (unsigned int j = 0;
                         j < PFord_count (p->sem.rank.sortby);
                         j++)
                        if (LR(p)->schema.items[i].name
                            == PFord_order_col_at (
                                   p->sem.rank.sortby,
                                   j)) {
                            rsortby++;
                            break;
                        }

                if (!lsortby && rsortby == PFord_count (p->sem.rank.sortby)) {
                    resolve_name_conflict (L(p), p->sem.rank.res);
                    *p = *(thetajoin_opt (
                              LL(p),
                              rank (
                                  LR(p),
                                  p->sem.rank.res,
                                  p->sem.rank.sortby),
                              L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }

                if (lsortby == PFord_count (p->sem.rank.sortby) && !rsortby) {
                    resolve_name_conflict (L(p), p->sem.rank.res);
                    *p = *(thetajoin_opt (
                              rank (
                                  LL(p),
                                  p->sem.rank.res,
                                  p->sem.rank.sortby),
                              LR(p),
                              L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                    break;
                }
            }
            break;

        case la_number:
            break;

        case la_type:
            if (is_tj (L(p))) {
                bool switch_left = PFprop_ocol (LL(p), p->sem.type.att);
                bool switch_right = PFprop_ocol (LR(p), p->sem.type.att);

                if (switch_left && switch_right) {
                    resolve_name_conflict (L(p), p->sem.type.res);
                    *p = *(thetajoin_opt (type (LL(p),
                                                p->sem.type.res,
                                                p->sem.type.att,
                                                p->sem.type.ty),
                                          type (LR(p),
                                                p->sem.type.res,
                                                p->sem.type.att,
                                                p->sem.type.ty),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_left) {
                    resolve_name_conflict (L(p), p->sem.type.res);
                    *p = *(thetajoin_opt (type (LL(p),
                                                p->sem.type.res,
                                                p->sem.type.att,
                                                p->sem.type.ty),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_right) {
                    resolve_name_conflict (L(p), p->sem.type.res);
                    *p = *(thetajoin_opt (LL(p),
                                          type (LR(p),
                                                p->sem.type.res,
                                                p->sem.type.att,
                                                p->sem.type.ty),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
            }
            break;

        case la_type_assert:
            if (is_tj (L(p))) {
                bool switch_left = PFprop_ocol (LL(p), p->sem.type.att);
                bool switch_right = PFprop_ocol (LR(p), p->sem.type.att);

                if (switch_left && switch_right) {
                    *p = *(thetajoin_opt (type_assert_pos (
                                              LL(p),
                                              p->sem.type.att,
                                              p->sem.type.ty),
                                          type_assert_pos (
                                              LR(p),
                                              p->sem.type.att,
                                              p->sem.type.ty),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_left) {
                    *p = *(thetajoin_opt (type_assert_pos (
                                              LL(p),
                                              p->sem.type.att,
                                              p->sem.type.ty),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_right) {
                    *p = *(thetajoin_opt (LL(p),
                                          type_assert_pos (
                                              LR(p),
                                              p->sem.type.att,
                                              p->sem.type.ty),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
            }
            break;

        case la_cast:
            if (is_tj (L(p))) {
                bool switch_left = PFprop_ocol (LL(p), p->sem.type.att);
                bool switch_right = PFprop_ocol (LR(p), p->sem.type.att);

                if (switch_left && switch_right) {
                    resolve_name_conflict (L(p), p->sem.type.res);
                    *p = *(thetajoin_opt (cast (LL(p),
                                                p->sem.type.res,
                                                p->sem.type.att,
                                                p->sem.type.ty),
                                          cast (LR(p),
                                                p->sem.type.res,
                                                p->sem.type.att,
                                                p->sem.type.ty),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_left) {
                    resolve_name_conflict (L(p), p->sem.type.res);
                    *p = *(thetajoin_opt (cast (LL(p),
                                                p->sem.type.res,
                                                p->sem.type.att,
                                                p->sem.type.ty),
                                          LR(p),
                                          L(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_right) {
                    resolve_name_conflict (L(p), p->sem.type.res);
                    *p = *(thetajoin_opt (LL(p),
                                          cast (LR(p),
                                                p->sem.type.res,
                                                p->sem.type.att,
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

                if (switch_left && switch_right) {
                    resolve_name_conflict (R(p), p->sem.step.item_res);
                    *p = *(thetajoin_opt (step_join (
                                                L(p),
                                                RL(p),
                                                p->sem.step.axis,
                                                p->sem.step.ty,
                                                p->sem.step.level,
                                                p->sem.step.item,
                                                p->sem.step.item_res),
                                          step_join (
                                                L(p),
                                                RR(p),
                                                p->sem.step.axis,
                                                p->sem.step.ty,
                                                p->sem.step.level,
                                                p->sem.step.item,
                                                p->sem.step.item_res),
                                          R(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_left) {
                    resolve_name_conflict (R(p), p->sem.step.item_res);
                    *p = *(thetajoin_opt (step_join (
                                                L(p), RL(p),
                                                p->sem.step.axis,
                                                p->sem.step.ty,
                                                p->sem.step.level,
                                                p->sem.step.item,
                                                p->sem.step.item_res),
                                          RR(p),
                                          R(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_right) {
                    resolve_name_conflict (R(p), p->sem.step.item_res);
                    *p = *(thetajoin_opt (RL(p),
                                          step_join (
                                                L(p),
                                                RR(p),
                                                p->sem.step.axis,
                                                p->sem.step.ty,
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

                if (switch_left && switch_right) {
                    resolve_name_conflict (R(p), p->sem.step.item_res);
                    *p = *(thetajoin_opt (guide_step_join (
                                                L(p),
                                                RL(p),
                                                p->sem.step.axis,
                                                p->sem.step.ty,
                                                p->sem.step.guide_count,
                                                p->sem.step.guides,
                                                p->sem.step.level,
                                                p->sem.step.item,
                                                p->sem.step.item_res),
                                          guide_step_join (
                                                L(p),
                                                RR(p),
                                                p->sem.step.axis,
                                                p->sem.step.ty,
                                                p->sem.step.guide_count,
                                                p->sem.step.guides,
                                                p->sem.step.level,
                                                p->sem.step.item,
                                                p->sem.step.item_res),
                                          R(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_left) {
                    resolve_name_conflict (R(p), p->sem.step.item_res);
                    *p = *(thetajoin_opt (guide_step_join (
                                                L(p), RL(p),
                                                p->sem.step.axis,
                                                p->sem.step.ty,
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
                    resolve_name_conflict (R(p), p->sem.step.item_res);
                    *p = *(thetajoin_opt (RL(p),
                                          guide_step_join (
                                                L(p),
                                                RR(p),
                                                p->sem.step.axis,
                                                p->sem.step.ty,
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

                if (switch_left && switch_right) {
                    resolve_name_conflict (R(p), p->sem.doc_join.item_res);
                    *p = *(thetajoin_opt (doc_index_join (
                                                L(p),
                                                RL(p),
                                                p->sem.doc_join.kind,
                                                p->sem.doc_join.item,
                                                p->sem.doc_join.item_res,
                                                p->sem.doc_join.item_doc),
                                          doc_index_join (
                                                L(p),
                                                RR(p),
                                                p->sem.doc_join.kind,
                                                p->sem.doc_join.item,
                                                p->sem.doc_join.item_res,
                                                p->sem.doc_join.item_doc),
                                          R(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_left) {
                    resolve_name_conflict (R(p), p->sem.doc_join.item_res);
                    *p = *(thetajoin_opt (doc_index_join (
                                                L(p), RL(p),
                                                p->sem.doc_join.kind,
                                                p->sem.doc_join.item,
                                                p->sem.doc_join.item_res,
                                                p->sem.doc_join.item_doc),
                                          RR(p),
                                          R(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_right) {
                    resolve_name_conflict (R(p), p->sem.doc_join.item_res);
                    *p = *(thetajoin_opt (RL(p),
                                          doc_index_join (
                                                L(p),
                                                RR(p),
                                                p->sem.doc_join.kind,
                                                p->sem.doc_join.item,
                                                p->sem.doc_join.item_res,
                                                p->sem.doc_join.item_doc),
                                          R(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
            }
            break;

        case la_doc_tbl:
            break;

        case la_doc_access:
            if (is_tj (R(p))) {
                bool switch_left = PFprop_ocol (RL(p), p->sem.doc_access.att);
                bool switch_right = PFprop_ocol (RR(p), p->sem.doc_access.att);

                if (switch_left && switch_right) {
                    resolve_name_conflict (R(p), p->sem.doc_access.res);
                    *p = *(thetajoin_opt (doc_access (L(p), RL(p),
                                                p->sem.doc_access.res,
                                                p->sem.doc_access.att,
                                                p->sem.doc_access.doc_col),
                                          doc_access (L(p), RR(p),
                                                p->sem.doc_access.res,
                                                p->sem.doc_access.att,
                                                p->sem.doc_access.doc_col),
                                          R(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_left) {
                    resolve_name_conflict (R(p), p->sem.doc_access.res);
                    *p = *(thetajoin_opt (doc_access (L(p), RL(p),
                                                p->sem.doc_access.res,
                                                p->sem.doc_access.att,
                                                p->sem.doc_access.doc_col),
                                          RR(p),
                                          R(p)->sem.thetajoin_opt.pred));
                    modified = true;
                }
                else if (switch_right) {
                    resolve_name_conflict (R(p), p->sem.doc_access.res);
                    *p = *(thetajoin_opt (RL(p),
                                          doc_access (L(p), RR(p),
                                                p->sem.doc_access.res,
                                                p->sem.doc_access.att,
                                                p->sem.doc_access.doc_col),
                                          R(p)->sem.thetajoin_opt.pred));
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
        case la_roots:
            /* constructors introduce like the unpartitioned
               number or rownum operators a dependency. */
            break;

        case la_fragment:
            break;
        case la_frag_union:
            break;
        case la_empty_frag:
            break;

        case la_cond_err:
            /* We push the error operator into the left input
               as we do not know whether it relates to the left or
               to the right input. */
            if (is_tj (L(p))) {
                *p = *(thetajoin_opt (cond_err (LL(p), R(p),
                                                p->sem.err.att,
                                                p->sem.err.str),
                                      LR(p),
                                      L(p)->sem.thetajoin_opt.pred));
                modified = true;
            }
            break;

        case la_nil:
        case la_trace:
        case la_trace_msg:
        case la_trace_map:
            /* we may not modify the cardinality */
        case la_rec_fix:
        case la_rec_param:
        case la_rec_arg:
        case la_rec_base:
            /* do not rewrite anything that has to do with recursion */
            break;

        case la_proxy:
            /**
             * ATTENTION: The proxies (especially the kind=1 version) are
             * generated in such a way, that the following rewrite does not
             * introduce inconsistencies.
             * The following rewrite would break if the proxy contains operators
             * that are themselves not allowed to be rewritten by thistimization
             * phase. We may not transform expressions that rely on the
             * cardinality of their inputs.
             *
             * In the current situation these operators are:
             * - aggregates (sum and count)
             * - rownum and number operators
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
             *    or if the partition criterion is not inferred from the number
             *    operator at the proxy exit. As an aggregate always uses the
             *    current scope (ensured during generation) and all scopes are
             *    properly nested, we have a functional dependency between the
             *    partitioning attribute of an aggregate inside this proxy
             *    (which is also equivalent to a scope) and the proxy exit number
             *    operator.
             *
             * - rownum and number operators:
             *    With a similar explanation as the one for aggregates we can be
             *    sure that every rownum and number operator inside the proxy is
             *    not used outside the proxy pattern. The generation process
             *    ensures that these are not used outside the scope or if used
             *    outside are partitioned by the number of the number operator at
             *    theproxy exit.
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
                L(p)->kind == la_project &&
                LL(p)->kind == la_eqjoin &&
                L(LL(p))->kind == la_project &&
                R(LL(p))->kind == la_project &&
                RL(LL(p))->kind == la_eqjoin &&
                LL(LL(p))->kind == la_number &&
                L(LL(LL(p))) == p->sem.proxy.base1 &&
                p->sem.proxy.ref->kind == la_project &&
                L(p->sem.proxy.ref) == LL(LL(p))) {

                PFla_op_t *thetajoin = L(p->sem.proxy.base1);
                PFla_op_t *lthetajoin, *rthetajoin;
                PFla_op_t *ref = p->sem.proxy.ref;
                unsigned int i, j, count = 0;
                bool rewrite = false;
                bool t1_left = false;
                PFarray_t *pred = thetajoin->sem.thetajoin_opt.pred;

                /* first check the dependencies of the left thetajoin input */
                for (i = 0; i < L(thetajoin)->schema.count; i++)
                    for (j = 0; j < p->sem.proxy.req_cols.count; j++)
                        if (L(thetajoin)->schema.items[i].name
                            == p->sem.proxy.req_cols.atts[j]) {
                            count++;
                            break;
                        }
                /* left side of the thetajoin corresponds to t2
                   (rthetajoin in the following) */
                if (p->sem.proxy.req_cols.count == count) {
                    rewrite = true;
                    t1_left = false;

                    /* collect the number of additional columns
                       that have to be mapped */
                    count = 0;
                    for (j = 0; j < PFarray_last (pred); j++)
                        if (!LEFT_VIS_AT (pred, i)) count++;
                }
                else {
                    count = 0;
                    /* then check the dependencies of the right thetajoin
                       input */
                    for (i = 0; i < R(thetajoin)->schema.count; i++)
                        for (j = 0; j < p->sem.proxy.req_cols.count; j++)
                            if (R(thetajoin)->schema.items[i].name
                                == p->sem.proxy.req_cols.atts[j]) {
                                count++;
                                break;
                            }
                    /* right side of the thetajoin corresponds to t2
                       (rthetajoin in the following) */
                    if (p->sem.proxy.req_cols.count == count) {
                        rewrite = true;
                        t1_left = true;

                        /* collect the number of additional columns
                           that have to be mapped */
                        count = 0;
                        for (j = 0; j < PFarray_last (pred); j++)
                            if (!RIGHT_VIS_AT (pred, i)) count++;
                    }
                }

                if (rewrite) {
                    PFalg_proj_t *proj_proxy, *proj_left, *proj_exit;
                    PFalg_att_t   dummy_col;
                    PFalg_att_t   used_cols;
                    PFalg_att_t   lconf_list, rconf_list;
                    unsigned int  invisible_col_count = count;

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
                       with the variable introduced by the number operator
                       or with the columns introduced by the proxy */
                    /* get rest of possible conflicting columns */
                    used_cols = L(ref)->sem.number.res;
                    for (i = 0; i < p->sem.proxy.new_cols.count; i++)
                        used_cols |= p->sem.proxy.new_cols.atts[i];

                    /* check for conflicts */
                    lconf_list = 0;
                    rconf_list = 0;
                    for (i = 0; i < PFarray_last (pred); i++) {
                        lconf_list |= (LEFT_AT (pred, i) & used_cols);
                        rconf_list |= (RIGHT_AT (pred, i) & used_cols);
                    }
                    /* if conflicts where found complete the list
                       of column names that may not be used as a
                       replacement */
                    if (lconf_list || rconf_list) {
                        for (i = 0; i < thetajoin->schema.count; i++)
                            used_cols |= thetajoin->schema.items[i].name;
                        for (i = 0; i < PFarray_last (pred); i++) {
                            used_cols |= LEFT_AT (pred, i);
                            used_cols |= RIGHT_AT (pred, i);
                        }
                    }
                    /* solve conflicts for the left thetajoin argument */
                    if (lconf_list) {
                        PFalg_proj_t *proj_list = PFmalloc (
                                                      L(thetajoin)->schema.count *
                                                      sizeof (PFalg_proj_t));
                        PFalg_att_t cur_col;
                        for (i = 0; i < L(thetajoin)->schema.count; i++) {
                            cur_col = L(thetajoin)->schema.items[i].name;
                            if (lconf_list & cur_col) {
                                PFalg_att_t new_col;
                                /* get a new column name */
                                new_col = PFalg_ori_name (
                                              PFalg_unq_name (cur_col, 0),
                                              ~used_cols);
                                used_cols = used_cols | new_col;
                                /* introduce a renaming */
                                proj_list[i] = PFalg_proj (new_col, cur_col);
                                /* update the predicate list */
                                for (j = 0; j < PFarray_last (pred); j++)
                                    if (LEFT_AT (pred, j) == cur_col)
                                        LEFT_AT (pred, j) = new_col;
                            } else
                                proj_list[i] = PFalg_proj (cur_col, cur_col);
                        }
                        L(thetajoin) = PFla_project_ (L(thetajoin), i, proj_list);
                    }
                    /* solve conflicts for the right thetajoin argument */
                    if (rconf_list) {
                        PFalg_proj_t *proj_list = PFmalloc (
                                                      R(thetajoin)->schema.count *
                                                      sizeof (PFalg_proj_t));
                        PFalg_att_t cur_col;
                        for (i = 0; i < R(thetajoin)->schema.count; i++) {
                            cur_col = R(thetajoin)->schema.items[i].name;
                            if (rconf_list & cur_col) {
                                PFalg_att_t new_col;
                                /* get a new column name */
                                new_col = PFalg_ori_name (
                                              PFalg_unq_name (cur_col, 0),
                                              ~used_cols);
                                used_cols = used_cols | new_col;
                                /* introduce a renaming */
                                proj_list[i] = PFalg_proj (new_col, cur_col);
                                /* update the predicate list */
                                for (j = 0; j < PFarray_last (pred); j++)
                                    if (RIGHT_AT (pred, j) == cur_col)
                                        RIGHT_AT (pred, j) = new_col;
                            } else
                                proj_list[i] = PFalg_proj (cur_col, cur_col);
                        }
                        R(thetajoin) = PFla_project_ (R(thetajoin), i, proj_list);
                    }

                    /* collect all the column names that are already in use
                       (on the way from the proxy base to the proxy root) */
                    used_cols = 0;
                    /* between pi_1 and pi_3 */
                    for (i = 0; i < LL(p)->schema.count; i++)
                        used_cols = used_cols | LL(p)->schema.items[i].name;

                    /* fill in the invisible column names at the beginning
                       of the mapping projections. We only have to cope with
                       column name conflicts between pi_1 and pi_3 as at
                       the beginning and at the end of the proxy operator
                       already cleared all conflicting names. */
                    count = 0;
                    if (t1_left) {
                        PFalg_att_t used_invisible_cols = 0;
                        /* ensure that do not introduce name
                           conflicts between invisible columns */
                        for (i = 0; i < PFarray_last (pred); i++)
                            if (!RIGHT_VIS_AT (pred, i))
                                used_invisible_cols |= RIGHT_AT (pred, i);

                        for (i = 0; i < PFarray_last (pred); i++)
                            if (!RIGHT_VIS_AT (pred, i)) {
                                PFalg_att_t cur_col = RIGHT_AT (pred, i);
                                PFalg_att_t new_col;
                                if (cur_col & used_cols) {
                                    /* get a new column name */
                                    new_col = PFalg_ori_name (
                                                  PFalg_unq_name (cur_col, 0),
                                                  ~(used_cols |
                                                    used_invisible_cols));
                                    used_cols = used_cols | new_col;
                                } else
                                    new_col = cur_col;

                                proj_proxy[count] = PFalg_proj (cur_col, new_col);
                                proj_left[count] = PFalg_proj (new_col, cur_col);
                                count++;
                            }
                    } else {
                        PFalg_att_t used_invisible_cols = 0;
                        /* ensure that do not introduce name
                           conflicts between invisible columns */
                        for (i = 0; i < PFarray_last (pred); i++)
                            if (!LEFT_VIS_AT (pred, i))
                                used_invisible_cols |= LEFT_AT (pred, i);

                        for (i = 0; i < PFarray_last (pred); i++)
                            if (!LEFT_VIS_AT (pred, i)) {
                                PFalg_att_t cur_col = LEFT_AT (pred, i);
                                PFalg_att_t new_col;
                                if (cur_col & used_cols) {
                                    /* get a new column name */
                                    new_col = PFalg_ori_name (
                                                  PFalg_unq_name (cur_col, 0),
                                                  ~(used_cols |
                                                    used_invisible_cols));
                                    used_cols = used_cols | new_col;
                                } else
                                    new_col = cur_col;

                                proj_proxy[count] = PFalg_proj (cur_col, new_col);
                                proj_left[count] = PFalg_proj (new_col, cur_col);
                                count++;
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

                    PFla_op_t *new_number = PFla_number (
                                                PFla_proxy_base (rthetajoin),
                                                L(ref)->sem.number.res);

                    *ref = *PFla_project_ (new_number,
                                           ref->schema.count,
                                           proj_exit);

                    rthetajoin = PFla_proxy (
                                     PFla_project_ (
                                         PFla_eqjoin (
                                             PFla_project_ (
                                                 new_number,
                                                 L(LL(p))->schema.count
                                                 + invisible_col_count,
                                                 proj_left),
                                             R(LL(p)),
                                             LL(p)->sem.eqjoin.att1,
                                             LL(p)->sem.eqjoin.att2),
                                         count, proj_proxy),
                                     1,
                                     ref,
                                     L(new_number),
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

    /* replace original thetajoin operators by the internal
       variant */
    if (p->kind == la_thetajoin) {
        PFarray_t *pred = PFarray (sizeof (pred_struct));

        for (i = 0; i < p->sem.thetajoin.count; i++)
            *(pred_struct *) PFarray_add (pred) =
                (pred_struct) {
                    .comp      = p->sem.thetajoin.pred[i].comp,
                    .left      = p->sem.thetajoin.pred[i].left,
                    .right     = p->sem.thetajoin.pred[i].right,
                    .res       = att_NULL,
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
    if (p->kind == la_thetajoin) {
        unsigned int  count = 0;
        PFalg_sel_t  *pred_new;
        PFarray_t    *pred = p->sem.thetajoin_opt.pred;
        PFla_op_t    *thetajoin;
        PFalg_att_t   used_cols, cur_col;
        PFalg_proj_t *proj, *lproj;
        unsigned int  lcount = 0;

        /* collect the number of persistent predicates */
        for (i = 0; i < PFarray_last (pred); i++)
            if (PERS_AT(pred, i)) count++;

        /* copy the list of persistent predicates */
        pred_new = PFmalloc (count * sizeof (PFalg_sel_t));
        count = 0;
        for (i = 0; i < PFarray_last (pred); i++)
            if (PERS_AT(pred, i)) {
                pred_new[count++] = PFalg_sel (COMP_AT (pred, i),
                                               LEFT_AT (pred, i),
                                               RIGHT_AT (pred, i));
            }

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

        /* generate a new thetajoin operator
           (with persistent predicates only) */
        thetajoin = PFla_thetajoin (L(p), R(p), count, pred_new);
        SEEN(thetajoin) = true;

        used_cols = 0;
        /* collect the columns in use */
        for (i = 0; i < p->schema.count; i++)
            used_cols = used_cols | p->schema.items[i].name;

        for (i = 0; i < PFarray_last (pred); i++) {
            used_cols = used_cols | LEFT_AT(pred, i);
            used_cols = used_cols | RIGHT_AT(pred, i);
        }

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
                        PFalg_att_t new_col;
                        /* get a new column name */
                        new_col = PFalg_ori_name (
                                      PFalg_unq_name (RES_AT(pred, i), 0),
                                      ~used_cols);
                        used_cols = used_cols | new_col;

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
                        PFalg_att_t new_col;
                        /* get a new column name */
                        new_col = PFalg_ori_name (
                                      PFalg_unq_name (RES_AT(pred, i), 0),
                                      ~used_cols);
                        used_cols = used_cols | new_col;

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
                        PFalg_att_t new_col;
                        /* get a new column name */
                        new_col = PFalg_ori_name (
                                      PFalg_unq_name (RES_AT(pred, i), 0),
                                      ~used_cols);
                        used_cols = used_cols | new_col;

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
       operators that cound not be integrated in the normal
       thetajoins. */
    remove_thetajoin_opt (root);
    PFla_dag_reset (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
