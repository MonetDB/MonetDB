/**
 * @file
 *
 * PFalgopt_rank():
 * Optimize relational algebra expression DAG by pushing rank operators
 * up in the DAG and merging them whenever possible. This leads to wider,
 * yet less rank operators ensuring the order. In certain scenarios ('join
 * graph queries') this leads to a single rank operator implementing all
 * order constraints of a query.
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

/* mnemonic algebra constructors */
#include "logical_mnemonic.h"

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define SEEN(p) ((p)->bit_dag)

struct sort_struct {
    PFalg_col_t  col; /* sort column */
    bool         dir; /* sort order direction */
    bool         vis; /* indicator if the sort column is visible */
};
typedef struct sort_struct sort_struct;

/* mnemonic for a value lookup in the sort struct */
#define COL_AT(a,i) ((*(sort_struct *) PFarray_at ((a), (i))).col)
#define DIR_AT(a,i) ((*(sort_struct *) PFarray_at ((a), (i))).dir)
#define VIS_AT(a,i) ((*(sort_struct *) PFarray_at ((a), (i))).vis)

/**
 * Rank constructor for rank and split operators used during optimization
 * (special variant).
 */
#ifndef NDEBUG
#define rank_opt(n,r,s) rank_opt_ (n,r,s,__LINE__)
static PFla_op_t *
rank_opt_ (PFla_op_t *n, PFalg_col_t res, PFarray_t *sortby, const int line)
#else
static PFla_op_t *
rank_opt (PFla_op_t *n, PFalg_col_t res, PFarray_t *sortby)
#endif
{
    PFla_op_t *ret = PFla_rank_opt_internal (n, res, sortby);
    PFarray_t *new_sortby;
    unsigned int i, j, count;

    /* allocate memory for the result schema (schema(n) + 1) */
    ret->schema.items = PFmalloc ((n->schema.count + 1) *
                                  sizeof (*(ret->schema.items)));

    count = 0;

    /* copy schema from argument 'n' */
    for (i = 0; i < n->schema.count; i++) {
        for (j = 0; j < PFarray_last (sortby); j++)
            if (COL_AT(sortby, j) == n->schema.items[i].name) {
                if (VIS_AT(sortby, j))
                    ret->schema.items[count++] = n->schema.items[i];

                break;
            }

        if (j == PFarray_last (sortby))
            ret->schema.items[count++] = n->schema.items[i];
    }

    /* remove duplicate rank entries */
    new_sortby = ret->sem.rank_opt.sortby;
    PFarray_last (new_sortby) = 0;
    for (i = 0; i < PFarray_last (sortby); i++) {
        /* Throw away all identical order criteria
           (after the first appearance of the sort criterion). */
        for (j = 0; j < i; j++)
            if (COL_AT(sortby, i) == COL_AT(sortby, j) &&
                DIR_AT(sortby, i) == DIR_AT(sortby, j) &&
                VIS_AT(sortby, i) == VIS_AT(sortby, j))
                break;
        if (i == j)
            *(sort_struct *) PFarray_add (new_sortby) =
                *(sort_struct *) PFarray_at (sortby, (i));
    }

#ifndef NDEBUG
    for (i = 0; i < PFarray_last (sortby); i++) {
        /* make sure that all input columns (to the rank are available) */
        if (!PFprop_ocol (n, COL_AT(sortby, i))) {
            fprintf (stderr, "input consists of: %s",
                     PFcol_str (n->schema.items[0].name));
            for (j = 1; j < n->schema.count; j++)
                fprintf (stderr, ", %s",
                         PFcol_str (n->schema.items[j].name));
            fprintf (stderr, "\nsortby list consists of: %s",
                     PFcol_str (COL_AT(sortby, 0)));
            for (j = 1; j < PFarray_last (sortby); j++)
                fprintf (stderr, ", %s",
                         PFcol_str (COL_AT(sortby, j)));
            fprintf (stderr, "\n");

            PFoops (OOPS_FATAL,
                    "column '%s' not found in rank (build in line %i)",
                    PFcol_str (COL_AT(sortby, i)), line);
        }
    }
    if (PFprop_ocol (n, res))
       PFoops (OOPS_FATAL,
               "result column '%s' found in rank input (build in line %i)",
               PFcol_str (res), line);
#endif

    /* add the result column */
    ret->schema.items[count].name = res;
    ret->schema.items[count].type = aat_nat;
    count++;
    
    /* fix the schema count */
    ret->schema.count = count;

    return ret;
}

/**
 * check for a rank operator
 */
static bool
is_rr (PFla_op_t *p)
{
    return (p->kind == la_internal_op);
}

/**
 * rank_identical checks if the semantical
 * information of two rank operators is the same.
 */
static bool
rank_identical (PFla_op_t *a, PFla_op_t *b)
{
    PFarray_t *sortby1, *sortby2;

    assert (a->kind == la_internal_op &&
            b->kind == la_internal_op);

    sortby1 = a->sem.rank_opt.sortby;
    sortby2 = b->sem.rank_opt.sortby;

    if (PFarray_last (sortby1) != PFarray_last (sortby2))
        return false;

    for (unsigned int i = 0; i < PFarray_last (sortby1); i++)
        if (COL_AT (sortby1, i) != COL_AT (sortby2, i) ||
            DIR_AT (sortby1, i) != DIR_AT (sortby2, i) ||
            VIS_AT (sortby1, i) != VIS_AT (sortby2, i))
            return false;

    return true;
}

/**
 * rank_stable_col_count checks if the rank
 * operator prunes the input schema (by marking input
 * columns as invisible).
 */
static bool
rank_stable_col_count (PFla_op_t *p)
{
    PFarray_t *sortby;

    assert (p->kind == la_internal_op);

    sortby = p->sem.rank_opt.sortby;
    assert (sortby);

    for (unsigned int i = 0; i < PFarray_last (sortby); i++)
        if (!VIS_AT (sortby, i))
            return false;

    return true;
}

/**
 * Worker for binary operators that pushes the binary
 * operator underneath the rank if both arguments
 * are provided by the same child operator.
 */
static bool
modify_binary_op (PFla_op_t *p,
                  PFla_op_t * (* op) (const PFla_op_t *,
                                      PFalg_col_t,
                                      PFalg_col_t,
                                      PFalg_col_t))
{
    bool modified = false;

    if (is_rr (L(p)) &&
        p->sem.binary.col1 != L(p)->sem.rank_opt.res &&
        p->sem.binary.col2 != L(p)->sem.rank_opt.res) {

        *p = *(rank_opt (
                   op (LL(p),
                       p->sem.binary.res,
                       p->sem.binary.col1,
                       p->sem.binary.col2),
                   L(p)->sem.rank_opt.res,
                   L(p)->sem.rank_opt.sortby));
        modified = true;
    }
    return modified;
}

/**
 * Worker that merges two adjacent order lists.
 *
 * The entry in the upper list (@a sortby) at position @a pos
 * is replaced by the lower list (@a lsortby).
 */
static PFord_ordering_t
merge_order_lists (PFord_ordering_t sortby,
                   PFarray_t *lsortby,
                   unsigned int pos)
{
    /* replace result column in the above order list
       by the list of sort criteria in the below order list */
    PFord_ordering_t new_sortby = PFordering ();
    unsigned int     count      = PFord_count (sortby),
                     lcount     = PFarray_last (lsortby);
    bool             dir_pos    = PFord_order_dir_at (sortby, pos);

    /* keep the first sort criteria */
    for (unsigned int i = 0; i < pos; i++)
        new_sortby = PFord_refine (
                         new_sortby,
                         PFord_order_col_at (sortby, i),
                         PFord_order_dir_at (sortby, i));

    /* replace entry pos by the list of sort criteria
       it represent */
    for (unsigned int i = 0; i < lcount; i++)
        new_sortby = PFord_refine (
                         new_sortby,
                         COL_AT (lsortby, i),
                         /* ensure that we maintain the direction */
                         dir_pos == DIR_ASC
                         ? DIR_AT (lsortby, i)
                         : !DIR_AT (lsortby, i));
    
    /* keep the remaining sort criteria */
    for (unsigned int i = pos+1; i < count; i++)
        new_sortby = PFord_refine (
                         new_sortby,
                         PFord_order_col_at (sortby, i),
                         PFord_order_dir_at (sortby, i));

    return new_sortby;
}

/* Rename 'invisible' rank columns. */
static PFla_op_t *
avoid_conflict (PFla_op_t *p)
{
    PFalg_proj_t *proj;
    PFarray_t    *sortby;

    assert (p->kind == la_internal_op);

    sortby = PFarray_copy (p->sem.rank_opt.sortby);
    /* create projection list */
    proj = PFmalloc (L(p)->schema.count * sizeof (PFalg_proj_t));
    /* fill projection list */
    for (unsigned int i = 0; i < L(p)->schema.count; i++)
        proj[i] = PFalg_proj (L(p)->schema.items[i].name,
                              L(p)->schema.items[i].name);

    /* rename 'invisible' rank columns */
    for (unsigned int i = 0; i < PFarray_last (sortby); i++)
        if (!VIS_AT(sortby, i)) 
            for (unsigned int j = 0; j < L(p)->schema.count; j++)
                if (COL_AT(sortby, i) == proj[j].old) {
                    PFalg_col_t new = PFcol_new (COL_AT(sortby, i));
                    COL_AT(sortby, i) = new;
                    proj[j].new = new;
                    break;
                }
    
    /* return a modified rank operator */
    return rank_opt (
               PFla_project_ (
                   L(p),
                   L(p)->schema.count,
                   proj),
               p->sem.rank_opt.res,
               sortby);
}

/* Check if an 'invisible' rank column appears in a schema of @a p. */
static bool
name_conflict (PFla_op_t *p, PFarray_t *sortby)
{
    for (unsigned int i = 0; i < PFarray_last (sortby); i++)
        if (!VIS_AT(sortby, i))
            for (unsigned int j = 0; j < p->schema.count; j++)
                if (COL_AT(sortby, i) == p->schema.items[j].name)
                    return true;
    return false;
}

/* define the different operation modes
   of opt_rank and intro_internal_rank */
#define RANK           1
#define ROWRANK_SINGLE 2
#define ROWRANK_MULTI  3

/**
 * worker for PFalgopt_rank
 *
 * opt_rank looks up op-rank operator pairs and
 * tries to move the rank operator up in the DAG
 * as far as possible.
 */
static bool
opt_rank (PFla_op_t *p, unsigned char mode)
{
    PFla_op_t    *lp        = NULL;
    unsigned int  lcount    = 0;
    PFalg_proj_t *lproj     = NULL;
    bool          lmodified = false,
                  modified  = false;

    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return modified;
    else
        SEEN(p) = true;

    /* apply optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        modified = opt_rank (p->child[i], mode) || modified;

    if (mode == ROWRANK_SINGLE) {
        /* In case we are trying to push up rowranks
           we don't want to split them and thus allow
           only pushing whenever no split may occur. */
        if (L(p) && is_rr (L(p)) && PFprop_refctr (p) != 1)
            return modified;
        if (R(p) && is_rr (R(p)) && PFprop_refctr (p) != 1)
            return modified;

        /* For rowrank operators we are not allowed to push
           the operator through a selection as missing rows
           lead to a different numbering. */
        switch (p->kind) {
            case la_eqjoin:
            case la_thetajoin:
            case la_semijoin:
            case la_select:
            case la_pos_select:
            case la_intersect:
            case la_difference:
            case la_step_join:
            case la_guide_step_join:
            case la_doc_index_join:
                return modified;

            default:
                break;
        }
    }
    else if (mode == ROWRANK_MULTI) {
        /* only push up a rowrank if the cardinality does not change */
        switch (p->kind) {
            case la_attach:
            case la_fun_1to1:
            case la_num_eq:
            case la_num_gt:
            case la_bool_and:
            case la_bool_or:
            case la_bool_not:
            case la_rownum:
            case la_rowrank:
            case la_rank:
            case la_internal_op:
            case la_rowid:
            case la_type:
            case la_type_assert:
            case la_cast:
                if (!is_rr (L(p)))
                    return modified;

                /* prepare for compensation actions (that avoid
                   splitting up the rowrank operator) */
                lp        = L(p);
                lcount    = L(p)->schema.count;
                lproj     = PFalg_proj_create (L(p)->schema);
                lmodified = modified;
                modified  = false;
                break;

            default:
                return modified;
        }
    }

    /**
     * In the following action code we try to propagate rank
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
            /* nothing to do -- no rank may sit underneath */
            break;

        case la_attach:
            if (is_rr (L(p))) {
                *p = *(rank_opt (
                           attach (LL(p),
                                   p->sem.attach.res,
                                   p->sem.attach.value),
                           L(p)->sem.rank_opt.res,
                           L(p)->sem.rank_opt.sortby));
                modified = true;
                break;
            }
            break;

        case la_cross:
            if (is_rr (L(p))) {
                /* Add check and rewrite that avoids name conflicts
                   for 'invisible' rank columns.
                   This situation may arise if the same rank was already
                   pushed through on the other side. */
                if (name_conflict (R(p), L(p)->sem.rank_opt.sortby))
                    L(p) = avoid_conflict (L(p));

                *p = *(rank_opt (cross (LL(p), R(p)),
                                 L(p)->sem.rank_opt.res,
                                 L(p)->sem.rank_opt.sortby));
                modified = true;
                break;
            }
            else if (is_rr (R(p))) {
                /* Add check and rewrite that avoids name conflicts
                   for 'invisible' rank columns.
                   This situation may arise if the same rank was already
                   pushed through on the other side. */
                if (name_conflict (L(p), R(p)->sem.rank_opt.sortby))
                    R(p) = avoid_conflict (R(p));

                *p = *(rank_opt (cross (L(p), RL(p)),
                                 R(p)->sem.rank_opt.res,
                                 R(p)->sem.rank_opt.sortby));
                modified = true;
                break;
            }
            break;

        case la_eqjoin:
            if (is_rr (L(p)) &&
                p->sem.eqjoin.col1 != L(p)->sem.rank_opt.res) {
                /* Add check and rewrite that avoids name conflicts
                   for 'invisible' rank columns.
                   This situation may arise if the same rank was already
                   pushed through on the other side. */
                if (name_conflict (R(p), L(p)->sem.rank_opt.sortby))
                    L(p) = avoid_conflict (L(p));

                *p = *(rank_opt (eqjoin (LL(p),
                                         R(p),
                                         p->sem.eqjoin.col1,
                                         p->sem.eqjoin.col2),
                                 L(p)->sem.rank_opt.res,
                                 L(p)->sem.rank_opt.sortby));
                modified = true;
                break;

            }
            if (is_rr (R(p)) &&
                p->sem.eqjoin.col2 != R(p)->sem.rank_opt.res) {
                /* Add check and rewrite that avoids name conflicts
                   for 'invisible' rank columns.
                   This situation may arise if the same rank was already
                   pushed through on the other side. */
                if (name_conflict (L(p), R(p)->sem.rank_opt.sortby))
                    R(p) = avoid_conflict (R(p));

                *p = *(rank_opt (eqjoin (L(p),
                                         RL(p),
                                         p->sem.eqjoin.col1,
                                         p->sem.eqjoin.col2),
                                 R(p)->sem.rank_opt.res,
                                 R(p)->sem.rank_opt.sortby));
                modified = true;
                break;
            }
            break;

        case la_semijoin:
            if (is_rr (L(p)) &&
                p->sem.eqjoin.col1 != L(p)->sem.rank_opt.res) {
                *p = *(rank_opt (semijoin (LL(p),
                                           R(p),
                                           p->sem.eqjoin.col1,
                                           p->sem.eqjoin.col2),
                                 L(p)->sem.rank_opt.res,
                                 L(p)->sem.rank_opt.sortby));
                modified = true;
                break;
            }
            break;

        case la_thetajoin:
            if (is_rr (L(p))) {
                bool res_used = false;

                for (unsigned int i = 0; i < p->sem.thetajoin.count; i++)
                    if (p->sem.thetajoin.pred[i].left ==
                        L(p)->sem.rank_opt.res) {
                        res_used = true;
                        break;
                    }

                if (!res_used) {
                    /* Add check and rewrite that avoids name conflicts
                       for 'invisible' rank columns.
                       This situation may arise if the same rank was already
                       pushed through on the other side. */
                    if (name_conflict (R(p), L(p)->sem.rank_opt.sortby))
                        L(p) = avoid_conflict (L(p));

                    *p = *(rank_opt (thetajoin (LL(p),
                                                R(p),
                                                p->sem.thetajoin.count,
                                                p->sem.thetajoin.pred),
                                     L(p)->sem.rank_opt.res,
                                     L(p)->sem.rank_opt.sortby));
                    modified = true;
                    break;
                }
            }
            if (is_rr (R(p))) {
                bool res_used = false;

                for (unsigned int i = 0; i < p->sem.thetajoin.count; i++)
                    if (p->sem.thetajoin.pred[i].right ==
                        R(p)->sem.rank_opt.res) {
                        res_used = true;
                        break;
                    }

                if (!res_used) {
                    /* Add check and rewrite that avoids name conflicts
                       for 'invisible' rank columns.
                       This situation may arise if the same rank was already
                       pushed through on the other side. */
                    if (name_conflict (L(p), R(p)->sem.rank_opt.sortby))
                        R(p) = avoid_conflict (R(p));

                    *p = *(rank_opt (thetajoin (L(p),
                                                RL(p),
                                                p->sem.thetajoin.count,
                                                p->sem.thetajoin.pred),
                                     R(p)->sem.rank_opt.res,
                                     R(p)->sem.rank_opt.sortby));
                    modified = true;
                    break;
                }
            }
            break;

        case la_project:
            /* Push project operator beyond the rank. */
            if (is_rr (L(p))) {
                PFalg_col_t   res     = L(p)->sem.rank_opt.res,
                              new_res = col_NULL,
                              new_name;
                PFarray_t    *sortby  = PFarray_copy (L(p)->sem.rank_opt.sortby);
                unsigned int  i,
                              j,
                              count = 0,
                              res_used = 0;
                PFalg_proj_t *proj;

                /* prune the rank operator if it is not used anymore */
                for (i = 0; i < p->sem.proj.count; i++)
                    if (p->sem.proj.items[i].old == res) {
                        new_res = p->sem.proj.items[i].new; 
                        res_used++;
                    }
                if (!res_used) {
                    L(p) = LL(p);
                    modified = true;
                    break;
                }
                /* we cannot split the rank operator */
                else if (res_used > 1)
                    break;

                /* create projection list */
                proj = PFmalloc ((p->schema.count + PFarray_last (sortby)) *
                                 sizeof (PFalg_proj_t));
                count = 0;

                for (unsigned int i = 0; i < LL(p)->schema.count; i++)
                    for (unsigned int j = 0; j < p->sem.proj.count; j++)
                        if (LL(p)->schema.items[i].name
                            == p->sem.proj.items[j].old) {
                            proj[count++] = p->sem.proj.items[j];
                        }

                /* The following for loop does multiple things:
                   1.) marks order columns now missing in the projection list
                       as invisible and creates new unique names for them
                   2.) updates the names of all visible order columns
                       (based on the projection list)
                   3.) adds invisible order columns to the projection list */
                for (i = 0; i < PFarray_last (sortby); i++) {
                    if (VIS_AT (sortby, i)) {
                        for (j = 0; j < p->sem.proj.count; j++)
                            if (COL_AT (sortby, i) == p->sem.proj.items[j].old)
                                break;

                        if (j == p->sem.proj.count) {
                            /* create a new unique column name */
                            new_name = PFcol_new (COL_AT (sortby, i));
                            /* mark unreferenced join inputs invisible */
                            VIS_AT (sortby, i) = false;
                            /* rename from old to new unique name */
                            proj[count++] = PFalg_proj (new_name,
                                                        COL_AT (sortby, i));
                            /* and assign input the new name */
                            COL_AT (sortby, i) = new_name;
                        }
                        else
                            /* update the column name of all referenced
                               join columns */
                            COL_AT (sortby, i) = p->sem.proj.items[j].new;
                    }
                    else {
                        proj[count++] = PFalg_proj (COL_AT (sortby, i),
                                                    COL_AT (sortby, i));
                    }
                }

                /* Ensure that the argument adds at least one column to
                   the result. */
                assert (count);

                *p = *(rank_opt (
                           PFla_project_ (LL(p), count, proj),
                           new_res,
                           sortby));
                modified = true;
                break;
            }
            break;

        case la_select:
            if (is_rr (L(p)) &&
                p->sem.select.col != L(p)->sem.rank_opt.res) {
                *p = *(rank_opt (select_ (LL(p),
                                          p->sem.select.col),
                                 L(p)->sem.rank_opt.res,
                                 L(p)->sem.rank_opt.sortby));
                modified = true;
            }
            break;

        case la_pos_select:
            if (is_rr (L(p)) &&
                p->sem.pos_sel.part != L(p)->sem.rank_opt.res) {
                unsigned int i;
                bool         res_used = false;

                for (i = 0; i < PFord_count (p->sem.pos_sel.sortby); i++)
                    if (PFord_order_col_at (p->sem.pos_sel.sortby, i) ==
                        L(p)->sem.rank_opt.res) {
                        res_used = true;
                        break;
                    }

                if (!res_used) {
                    *p = *(rank_opt (
                              pos_select (
                                  LL(p),
                                  p->sem.pos_sel.pos,
                                  p->sem.pos_sel.sortby,
                                  p->sem.pos_sel.part),
                              L(p)->sem.rank_opt.res,
                              L(p)->sem.rank_opt.sortby));
                    modified = true;
                    break;
                }
                else {
                    /* push up the internal rank operator */
                    *p = *(rank_opt (
                              pos_select (LL(p),
                                          p->sem.pos_sel.pos,
                                          /* replace result column in the above
                                             order list by the list of sort
                                             criteria in the below order list */
                                          merge_order_lists (
                                              p->sem.pos_sel.sortby,
                                              L(p)->sem.rank_opt.sortby,
                                              i),
                                          p->sem.pos_sel.part),
                              L(p)->sem.rank_opt.res,
                              L(p)->sem.rank_opt.sortby));
                    modified = true;
                    break;
                }
            }
            break;

        case la_disjunion:
            /* If the children of the union operator are both rank
               operators which reference the same subexpression, we move
               them above the union. */
            if (is_rr (L(p)) && is_rr (R(p)) &&
                rank_identical (L(p), R(p))) {
                *p = *(rank_opt (disjunion (LL(p), RL(p)),
                                 L(p)->sem.rank_opt.res,
                                 L(p)->sem.rank_opt.sortby));
                modified = true;
                break;
            }
            break;

        case la_intersect:
            /* If the children of the intersect operator are both rank
               operators which reference the same subexpression, we move
               them above the intersect. */
            if (is_rr (L(p)) && is_rr (R(p)) &&
                rank_identical (L(p), R(p)) &&
                rank_stable_col_count (L(p))) {
                *p = *(rank_opt (intersect (LL(p), RL(p)),
                                 L(p)->sem.rank_opt.res,
                                 L(p)->sem.rank_opt.sortby));
                modified = true;
                break;
            }
            break;

        case la_difference:
            /* If the children of the difference operator are both rank
               operators which reference the same subexpression, we move
               them above the difference. */
            if (is_rr (L(p)) && is_rr (R(p)) &&
                rank_identical (L(p), R(p)) &&
                rank_stable_col_count (L(p))) {
                *p = *(rank_opt (difference (LL(p), RL(p)),
                                 L(p)->sem.rank_opt.res,
                                 L(p)->sem.rank_opt.sortby));
                modified = true;
                break;
            }
            break;

        case la_distinct:
            /* Push distinct into the rank operand
               as the additional columns don't change
               the cardinality of the distinct result. */
            if (is_rr (L(p))) {
                *p = *(rank_opt (distinct (LL(p)),
                                 L(p)->sem.rank_opt.res,
                                 L(p)->sem.rank_opt.sortby));
                modified = true;
            }
            break;

        case la_fun_1to1:
            if (is_rr (L(p))) {
                bool res_used = false;

                for (unsigned int i = 0; i < clsize (p->sem.fun_1to1.refs); i++)
                    if (clat (p->sem.fun_1to1.refs, i) ==
                        L(p)->sem.rank_opt.res) {
                        res_used = true;
                        break;
                    }

                if (!res_used) {
                    *p = *(rank_opt (
                               fun_1to1 (LL(p),
                                         p->sem.fun_1to1.kind,
                                         p->sem.fun_1to1.res,
                                         p->sem.fun_1to1.refs),
                               L(p)->sem.rank_opt.res,
                               L(p)->sem.rank_opt.sortby));
                    modified = true;
                }
            }
            break;

        case la_num_eq:
            modified = modify_binary_op (p, PFla_eq);
            break;
        case la_num_gt:
            modified = modify_binary_op (p, PFla_gt);
            break;
        case la_bool_and:
            modified = modify_binary_op (p, PFla_and);
            break;
        case la_bool_or:
            modified = modify_binary_op (p, PFla_or);
            break;

        case la_bool_not:
            if (is_rr (L(p)) &&
                p->sem.unary.col != L(p)->sem.rank_opt.res) {
                *p = *(rank_opt (
                           PFla_not (LL(p),
                                     p->sem.unary.res,
                                     p->sem.unary.col),
                           L(p)->sem.rank_opt.res,
                           L(p)->sem.rank_opt.sortby));
                modified = true;
            }
            break;

        case la_to:
            modified = modify_binary_op (p, PFla_to);
            break;

        case la_aggr:
            if (is_rr (L(p)) &&
                p->sem.aggr.part == L(p)->sem.rank_opt.res) {
                /* Split up the ranking operator into an operator that
                   can be evaluated after the aggregate and a rank operator
                   that provides the correct partitions for the aggregate. */

                PFarray_t       *lsortby    = L(p)->sem.rank_opt.sortby,
                                *new_sortby;
                PFord_ordering_t old_sortby = PFordering ();
                unsigned int     lcount     = PFarray_last (lsortby),
                                 count      = 0,
                                 i,
                                 j;
                PFalg_col_t      col;
                bool             vis,
                                 dir;
                PFalg_aggr_t    *aggr;
                PFalg_proj_t    *proj;

                aggr       = PFmalloc ((lcount + p->sem.aggr.count) *
                                       sizeof (PFalg_aggr_t));
                new_sortby = PFarray (sizeof (sort_struct), lcount);

                /* copy all aggregates */
                for (i = 0; i < p->sem.aggr.count; i++)
                    aggr[count++] = p->sem.aggr.aggr[i];

                /* Extend aggregate list in case we miss some order columns. */
                for (i = 0; i < PFarray_last (lsortby); i++) {
                    col = COL_AT (lsortby, i);
                    dir = DIR_AT (lsortby, i);
                    vis = false;

                    /* Add order column to the lower rank operator. */
                    old_sortby = PFord_refine (old_sortby, col, dir);

                    /* Try to find the order criterion in the aggregate list. */
                    for (j = 0; j < count; j++)
                        if (aggr[j].col == col &&
                            aggr[j].kind == alg_aggr_dist) {
                            /* We found a matching aggregate and change the
                               name and visibility of the ordering entry. */
                            col = aggr[j].res;
                            vis = true;
                            break;
                        }

                    /* We haven't found a matching aggregate and generate
                       a new one. */
                    if (j == count)
                        aggr[count++] = PFalg_aggr (alg_aggr_dist, col, col);

                    /* Add order column to the upper rank operator. */
                    *(sort_struct *) PFarray_add (new_sortby) =
                        (sort_struct) { .col = col, .dir = dir, .vis = vis };

                }

                /* we need to prune away the partition (aka result) column */
                proj = PFmalloc (count * sizeof (PFalg_proj_t));
                for (i = 0; i < count; i++)
                    proj[i] = PFalg_proj (aggr[i].res, aggr[i].res);

                *p = *(rank_opt (
                           PFla_project_ (
                               aggr (
                                   rank (
                                       LL(p),
                                       p->sem.aggr.part,
                                       old_sortby),
                                   p->sem.aggr.part,
                                   count,
                                   aggr),
                               count,
                               proj),
                           p->sem.aggr.part,
                           new_sortby));

                modified = true;
                break;
            }
            break;

        case la_rownum:
            if (is_rr (L(p)) &&
                p->sem.sort.part != L(p)->sem.rank_opt.res) {
                unsigned int i;
                bool         res_used = false;

                for (i = 0; i < PFord_count (p->sem.sort.sortby); i++)
                    if (PFord_order_col_at (p->sem.sort.sortby, i) ==
                        L(p)->sem.rank_opt.res) {
                        res_used = true;
                        break;
                    }

                if (!res_used) {
                    *p = *(rank_opt (
                              rownum (
                                  LL(p),
                                  p->sem.sort.res,
                                  p->sem.sort.sortby,
                                  p->sem.sort.part),
                              L(p)->sem.rank_opt.res,
                              L(p)->sem.rank_opt.sortby));
                    modified = true;
                    break;
                }
                else {
                    /* push up the internal rank operator */
                    *p = *(rank_opt (
                              rownum (LL(p),
                                      p->sem.sort.res,
                                      /* replace result column in the above
                                         order list by the list of sort
                                         criteria in the below order list */
                                      merge_order_lists (
                                          p->sem.sort.sortby,
                                          L(p)->sem.rank_opt.sortby,
                                          i),
                                      p->sem.sort.part),
                              L(p)->sem.rank_opt.res,
                              L(p)->sem.rank_opt.sortby));
                    modified = true;
                    break;
                }
            }
            break;

        case la_rowrank:
        case la_rank:
            if (is_rr (L(p))) {
                PFla_op_t *(* op) (const PFla_op_t *,
                                   PFalg_col_t,
                                   PFord_ordering_t);
                unsigned int i;
                bool         res_used = false;

                op = p->kind == la_rowrank ? PFla_rowrank : PFla_rank;
                
                for (i = 0; i < PFord_count (p->sem.sort.sortby); i++)
                    if (PFord_order_col_at (p->sem.sort.sortby, i) ==
                        L(p)->sem.rank_opt.res) {
                        res_used = true;
                        break;
                    }

                if (!res_used) {
                    *p = *(rank_opt (
                              op (
                                  LL(p),
                                  p->sem.sort.res,
                                  p->sem.sort.sortby),
                              L(p)->sem.rank_opt.res,
                              L(p)->sem.rank_opt.sortby));
                    modified = true;
                    break;
                }
                else {
                    /* push up the internal rank operator */
                    *p = *(rank_opt (
                              op (LL(p),
                                  p->sem.sort.res,
                                  /* replace result column in the above
                                     order list by the list of sort
                                     criteria in the below order list */
                                  merge_order_lists (
                                      p->sem.sort.sortby,
                                      L(p)->sem.rank_opt.sortby,
                                      i)),
                              L(p)->sem.rank_opt.res,
                              L(p)->sem.rank_opt.sortby));
                    modified = true;
                    break;
                }
            }
            break;

        case la_internal_op:
            if (is_rr (L(p))) {
                unsigned int i;
                bool         res_used = false;

                for (i = 0; i < PFarray_last (p->sem.rank_opt.sortby); i++)
                    if (COL_AT (p->sem.rank_opt.sortby, i) ==
                        L(p)->sem.rank_opt.res) {
                        res_used = true;
                        break;
                    }

                if (!res_used) {
                    /* we have to maintain the visibility information for
                       columns that are used in both operators */
                    PFarray_t   *sortby, *lsortby;
                    unsigned int count, lcount,
                                 matches = 0,
                                 lmatches = 0;

                    sortby  = PFarray_copy (p->sem.rank_opt.sortby);
                    lsortby = PFarray_copy (L(p)->sem.rank_opt.sortby);
                    count   = PFarray_last (sortby);
                    lcount  = PFarray_last (lsortby);

                    for (unsigned int i = 0; i < count; i++) {
                        matches = 0;
                        for (unsigned int j = 0; j < lcount; j++) {
                            lmatches = 0;
                            if (COL_AT (sortby, i) == COL_AT (lsortby, j)) {
                                /* maintain visibility over both operators */
                                VIS_AT (lsortby, j) = VIS_AT (sortby, i);
                                VIS_AT (sortby, i) = true;
                                matches++;
                                lmatches++;
                            }
                            if (lmatches > 1)
                                break; /* do not rewrite */
                        }
                        if (matches > 1 || lmatches > 1)
                            break; /* do not rewrite */
                    }

                    if (matches > 1 || lmatches > 1)
                        break;

                    *p = *(rank_opt (
                              rank_opt (
                                  LL(p),
                                  p->sem.rank_opt.res,
                                  sortby),
                              L(p)->sem.rank_opt.res,
                              lsortby));

                    /* don't mark plan as modified to avoid endless loops */
                    break;
                }
                else {
                    /* merge adjacent rank operators where one rank operator
                       consumes the result of the other */
                    bool         vis     = VIS_AT (p->sem.rank_opt.sortby, i);
                    PFarray_t   *sortby  = p->sem.rank_opt.sortby,
                                *lsortby = L(p)->sem.rank_opt.sortby,
                                *new_sortby;
                    unsigned int count   = PFarray_last (sortby),
                                 lcount  = PFarray_last (lsortby);
                    PFla_op_t   *lrank;
                    bool         dir_i   = DIR_AT (sortby, i);

                    new_sortby = PFarray (sizeof (sort_struct), count + lcount);

                    lsortby = L(p)->sem.rank_opt.sortby;
                    
                    /* keep the first sort criteria */
                    for (unsigned int j = 0; j < i; j++)
                        *(sort_struct *) PFarray_add (new_sortby) =
                            (sort_struct) {
                                .col = COL_AT (sortby, j),
                                .dir = DIR_AT (sortby, j),
                                .vis = VIS_AT (sortby, j)
                            };
                    /* replace entry i by the list of sort criteria
                       it represent */
                    for (unsigned int j = 0; j < lcount; j++) {
                        *(sort_struct *) PFarray_add (new_sortby) =
                            (sort_struct) {
                                .col = COL_AT (lsortby, j),
                                 /* ensure that we maintain the direction */
                                .dir = dir_i == DIR_ASC
                                       ? DIR_AT (lsortby, j)
                                       : !DIR_AT (lsortby, j),
                                .vis = PFprop_ocol (p, COL_AT(lsortby, j))
                            };
                        VIS_AT (lsortby, j) = true;
                    }
                    /* keep the remaining sort criteria */
                    for (unsigned int j = i+1; j < count; j++)
                        *(sort_struct *) PFarray_add (new_sortby) =
                            (sort_struct) {
                                .col = COL_AT (sortby, j),
                                .dir = DIR_AT (sortby, j),
                                .vis = VIS_AT (sortby, j)
                            };

                    if (vis)
                        lrank = rank_opt (
                                    LL(p),
                                    L(p)->sem.rank_opt.res,
                                    lsortby);
                    else
                        lrank = LL(p);

                    /* adjust the schema of the rank operators */
                    *p = *(rank_opt (
                               lrank,
                               p->sem.rank_opt.res,
                               new_sortby));

                    /* don't mark plan as modified to avoid endless loops */
                    break;
                }
            }
            break;

        case la_rowid:
            if (is_rr (L(p))) {
                *p = *(rank_opt (
                          rowid (
                              LL(p),
                              p->sem.rowid.res),
                          L(p)->sem.rank_opt.res,
                          L(p)->sem.rank_opt.sortby));
                modified = true;
            }
            break;

        case la_type:
            if (is_rr (L(p)) &&
                p->sem.type.col != L(p)->sem.rank_opt.res) {
                *p = *(rank_opt (
                           type (LL(p),
                                 p->sem.type.res,
                                 p->sem.type.col,
                                 p->sem.type.ty),
                           L(p)->sem.rank_opt.res,
                           L(p)->sem.rank_opt.sortby));
                modified = true;
            }
            break;

        case la_type_assert:
            if (is_rr (L(p)) &&
                p->sem.type.col != L(p)->sem.rank_opt.res) {
                *p = *(rank_opt (type_assert_pos (
                                     LL(p),
                                     p->sem.type.col,
                                     p->sem.type.ty),
                                 L(p)->sem.rank_opt.res,
                                 L(p)->sem.rank_opt.sortby));
                modified = true;
            }
            break;

        case la_cast:
            if (is_rr (L(p)) &&
                p->sem.type.col != L(p)->sem.rank_opt.res) {
                *p = *(rank_opt (
                           cast (LL(p),
                                 p->sem.type.res,
                                 p->sem.type.col,
                                 p->sem.type.ty),
                           L(p)->sem.rank_opt.res,
                           L(p)->sem.rank_opt.sortby));
                modified = true;
            }
            break;

        case la_step:
        case la_guide_step:
            break;

        case la_step_join:
            if (is_rr (R(p)) &&
                p->sem.step.item != R(p)->sem.rank_opt.res) {
                *p = *(rank_opt (
                           step_join (L(p), RL(p),
                                      p->sem.step.spec,
                                      p->sem.step.level,
                                      p->sem.step.item,
                                      p->sem.step.item_res),
                           R(p)->sem.rank_opt.res,
                           R(p)->sem.rank_opt.sortby));
                modified = true;
            }
            break;

        case la_guide_step_join:
            if (is_rr (R(p)) &&
                p->sem.step.item != R(p)->sem.rank_opt.res) {
                *p = *(rank_opt (
                           guide_step_join (L(p), RL(p),
                                            p->sem.step.spec,
                                            p->sem.step.guide_count,
                                            p->sem.step.guides,
                                            p->sem.step.level,
                                            p->sem.step.item,
                                            p->sem.step.item_res),
                           R(p)->sem.rank_opt.res,
                           R(p)->sem.rank_opt.sortby));
                modified = true;
            }
            break;

        case la_doc_index_join:
            if (is_rr (R(p)) &&
                p->sem.doc_join.item != R(p)->sem.rank_opt.res &&
                p->sem.doc_join.item_doc != R(p)->sem.rank_opt.res) {
                *p = *(rank_opt (
                           doc_index_join (L(p), RL(p),
                                           p->sem.doc_join.kind,
                                           p->sem.doc_join.item,
                                           p->sem.doc_join.item_res,
                                           p->sem.doc_join.item_doc,
                                           p->sem.doc_join.ns1,
                                           p->sem.doc_join.loc1,
                                           p->sem.doc_join.ns2,
                                           p->sem.doc_join.loc2),
                           R(p)->sem.rank_opt.res,
                           R(p)->sem.rank_opt.sortby));
                modified = true;
            }
            break;

        case la_doc_tbl:
            /* should not appear as roots already
               translates the doc_tbl operator. */
            break;

        case la_doc_access:
            if (is_rr (R(p)) &&
                p->sem.doc_access.col != R(p)->sem.rank_opt.res) {
                *p = *(rank_opt (
                           doc_access (L(p), RL(p),
                                       p->sem.doc_access.res,
                                       p->sem.doc_access.col,
                                       p->sem.doc_access.doc_col),
                           R(p)->sem.rank_opt.res,
                           R(p)->sem.rank_opt.sortby));
                modified = true;
            }
            break;

        case la_roots:
            /* modify the only pattern starting in roots
               that is no constructor: roots-doc_tbl */
            if (L(p)->kind == la_doc_tbl &&
                is_rr (LL(p)) &&
                p->sem.doc_tbl.col != LL(p)->sem.rank_opt.res) {

                PFalg_col_t res    = LL(p)->sem.rank_opt.res;
                PFarray_t  *sortby = LL(p)->sem.rank_opt.sortby;

                /* overwrite doc_tbl node to update
                   both roots and frag operators */
                *(L(p)) = *(doc_tbl (L(LL(p)),
                                     L(p)->sem.doc_tbl.res,
                                     L(p)->sem.doc_tbl.col,
                                     L(p)->sem.doc_tbl.kind));
                /* push roots + doc_tbl through the rank */
                *p = *(rank_opt (roots (L(p)), res, sortby));
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
        case la_proxy_base:
            /* remove proxy operators as they might
               lead to inconsistencies */
            *p = *(dummy (L(p)));
            modified = true;
            break;

        case la_string_join:
            break;

        case la_dummy:
            *p = *(rank_opt (LL(p),
                             L(p)->sem.rank_opt.res,
                             L(p)->sem.rank_opt.sortby));
            break;
    }

    if (mode == ROWRANK_MULTI) {
        assert (lp && lproj && lcount != 0);
        /* Add compensation action that links the references
           to the old rowrank operator to the new one (to avoid
           multiple rowrank operators). */
        if (modified)
            *lp = *PFla_project_ (p, lcount, lproj);

        /* combine modified flags */
        modified |= lmodified;
    }

    return modified;
}

/**
 * intro_internal_rank replaces all rank
 * operator by an intermediate internal representation
 * that is able to cope with invisible orderings.
 */
static void
intro_internal_rank (PFla_op_t *p, unsigned char mode)
{
    PFla_op_kind_t rank_kind = (mode == RANK) ? la_rank : la_rowrank;
    unsigned int i;

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* traverse children */
    for (i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        intro_internal_rank (p->child[i], mode);

    /* In case we are trying to push up rowranks
       we don't want to split them and thus allow
       only pushing whenever no split may occur. */
    if (mode == ROWRANK_SINGLE && PFprop_refctr(p) != 1)
        return;

    /* replace original rank operators by the internal
       variant */
    if (p->kind == rank_kind) {
        PFarray_t *sortby = PFarray (sizeof (sort_struct),
                                     PFord_count (p->sem.sort.sortby));

        for (i = 0; i < PFord_count (p->sem.sort.sortby); i++)
            *(sort_struct *) PFarray_add (sortby) =
                (sort_struct) {
                    .col = PFord_order_col_at (p->sem.sort.sortby, i),
                    .dir = PFord_order_dir_at (p->sem.sort.sortby, i),
                    .vis = true
                };

        *p = *rank_opt (L(p), p->sem.sort.res, sortby);
    }
}

/**
 * remove_internal_rank replaces all intermediate
 * rank operators by normal ones. A pruning projection
 * on top of the operator chain ensures that only the
 * expected column names are visible.
 */
static void
remove_internal_rank (PFla_op_t *p,
                      PFla_op_t * (*rank_op) (const PFla_op_t *,
                                              PFalg_col_t,
                                              PFord_ordering_t))
{
    PFalg_col_t  cur_col;
    unsigned int i;

    /* rewrite each node only once */
    if (SEEN(p))
        return;

    /* traverse children */
    for (i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        remove_internal_rank (p->child[i], rank_op);

    /* replace the intermediate representation of the rank */
    if (p->kind == la_internal_op) {
        PFarray_t       *sort_list = p->sem.rank_opt.sortby;
        PFalg_proj_t    *proj      = PFmalloc (p->schema.count *
                                               sizeof (PFalg_proj_t));
        PFord_ordering_t sortby    = PFordering ();

        /* transform internal sort list into a normal sortby list */
        for (i = 0; i < PFarray_last (sort_list); i++)
            sortby = PFord_refine (sortby,
                                   COL_AT (sort_list, i),
                                   DIR_AT (sort_list, i));

        /* Create a pruning projection list that is placed on top
           of the rank to discard all invisible columns. */
        for (i = 0; i < p->schema.count; i++) {
            cur_col = p->schema.items[i].name;
            proj[i] = PFalg_proj (cur_col, cur_col);
        }

        *p = *PFla_project_ (rank_op (L(p), p->sem.rank_opt.res, sortby),
                             p->schema.count,
                             proj);
    }

    SEEN(p) = true;
}

/**
 * Invoke rank optimization that moves rank
 * operators as high in the plans as possible.
 */
PFla_op_t *
PFalgopt_rank (PFla_op_t *root)
{
    /* rowrank single reference */

    /* collect the number of parent references
       to avoid splitting up rowrank operators */
    PFprop_infer_refctr (root);

    /* Replace rowrank operator by the internal rank
       representation needed for this optimization phase. */
    intro_internal_rank (root, ROWRANK_SINGLE);
    PFla_dag_reset (root);

    /* Traverse the DAG bottom up and look for op-rowrank
       operator pairs. As long as we find a rewrite we start
       a new traversal. */
    while (opt_rank (root, ROWRANK_SINGLE))
        PFla_dag_reset (root);
    PFla_dag_reset (root);

    /* Replace the internal rank representation by
       normal rowranks. */
    remove_internal_rank (root, PFla_rowrank);
    PFla_dag_reset (root);


    /* rowrank multiple references */

    /* get projections out of the way */
    PFalgopt_projection (root);

    /* Replace rowrank operator by the internal rank
       representation needed for this optimization phase. */
    intro_internal_rank (root, ROWRANK_MULTI);
    PFla_dag_reset (root);

    /* Traverse the DAG bottom up and look for op-rowrank
       operator pairs. As long as we find a rewrite we start
       a new traversal. */
    while (opt_rank (root, ROWRANK_MULTI)) {
        PFla_dag_reset (root);
        PFalgopt_projection (root);
    }
    PFla_dag_reset (root);

    /* Replace the internal rank representation by
       normal rowranks. */
    remove_internal_rank (root, PFla_rowrank);
    PFla_dag_reset (root);


    /* rank */

    /* Replace rank operator by the internal rank
       representation needed for this optimization phase. */
    intro_internal_rank (root, RANK);
    PFla_dag_reset (root);

    /* Traverse the DAG bottom up and look for op-rank
       operator pairs. As long as we find a rewrite we start
       a new traversal. */
    while (opt_rank (root, RANK))
        PFla_dag_reset (root);
    PFla_dag_reset (root);

    /* Replace the internal rank representation by
       normal ranks. */
    remove_internal_rank (root, PFla_rank);
    PFla_dag_reset (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
