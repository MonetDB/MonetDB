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
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
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
 * worker for PFalgopt_rank
 *
 * opt_rank looks up op-rank operator pairs and
 * tries to move the rank operator up in the DAG
 * as far as possible.
 */
static bool
opt_rank (PFla_op_t *p)
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
        modified = opt_rank (p->child[i]) || modified;

    /**
     * In the following action code we try to propagate rank
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
                *p = *(rank_opt (cross (LL(p), R(p)),
                                 L(p)->sem.rank_opt.res,
                                 L(p)->sem.rank_opt.sortby));
                modified = true;
                break;
            }
            else if (is_rr (R(p))) {
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
                            new_name = PFalg_new_name (COL_AT (sortby, i));
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
                    /* replace result column in the above order list
                       by the list of sort criteria in the below order list */
                    PFord_ordering_t sortby     = p->sem.pos_sel.sortby,
                                     new_sortby = PFordering ();
                    PFarray_t       *lsortby    = L(p)->sem.rank_opt.sortby;
                    unsigned int     count      = PFord_count (sortby),
                                     lcount     = PFarray_last (lsortby);

                    /* keep the first sort criteria */
                    for (unsigned int j = 0; j < i; j++)
                        new_sortby = PFord_refine (
                                         new_sortby,
                                         PFord_order_col_at (sortby, j),
                                         PFord_order_dir_at (sortby, j));

                    /* replace entry i by the list of sort criteria
                       it represent */
                    for (unsigned int j = 0; j < lcount; j++)
                        new_sortby = PFord_refine (
                                         new_sortby,
                                         COL_AT (lsortby, j),
                                         DIR_AT (lsortby, j));
                    
                    /* keep the remaining sort criteria */
                    for (unsigned int j = i+1; j < count; j++)
                        new_sortby = PFord_refine (
                                         new_sortby,
                                         PFord_order_col_at (sortby, j),
                                         PFord_order_dir_at (sortby, j));

                    /* push up the internal rank operator */
                    *p = *(rank_opt (
                              pos_select (LL(p),
                                          p->sem.pos_sel.pos,
                                          new_sortby,
                                          p->sem.pos_sel.part),
                              L(p)->sem.rank_opt.res,
                              lsortby));
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

        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_count:
        case la_seqty1:
        case la_all:
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
                    /* replace result column in the above order list
                       by the list of sort criteria in the below order list */
                    PFord_ordering_t sortby     = p->sem.sort.sortby,
                                     new_sortby = PFordering ();
                    PFarray_t       *lsortby    = L(p)->sem.rank_opt.sortby;
                    unsigned int     count      = PFord_count (sortby),
                                     lcount     = PFarray_last (lsortby);

                    lsortby = L(p)->sem.rank_opt.sortby;
                    
                    /* keep the first sort criteria */
                    for (unsigned int j = 0; j < i; j++)
                        new_sortby = PFord_refine (
                                         new_sortby,
                                         PFord_order_col_at (sortby, j),
                                         PFord_order_dir_at (sortby, j));

                    /* replace entry i by the list of sort criteria
                       it represent */
                    for (unsigned int j = 0; j < lcount; j++)
                        new_sortby = PFord_refine (
                                         new_sortby,
                                         COL_AT (lsortby, j),
                                         DIR_AT (lsortby, j));
                    
                    /* keep the remaining sort criteria */
                    for (unsigned int j = i+1; j < count; j++)
                        new_sortby = PFord_refine (
                                         new_sortby,
                                         PFord_order_col_at (sortby, j),
                                         PFord_order_dir_at (sortby, j));

                    /* push up the internal rank operator */
                    *p = *(rank_opt (
                              rownum (LL(p),
                                      p->sem.sort.res,
                                      new_sortby,
                                      p->sem.sort.part),
                              L(p)->sem.rank_opt.res,
                              lsortby));
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
                    /* replace result column in the above order list
                       by the list of sort criteria in the below order list */
                    PFord_ordering_t sortby     = p->sem.sort.sortby,
                                     new_sortby = PFordering ();
                    PFarray_t       *lsortby    = L(p)->sem.rank_opt.sortby;
                    unsigned int     count      = PFord_count (sortby),
                                     lcount     = PFarray_last (lsortby);

                    lsortby = L(p)->sem.rank_opt.sortby;
                    
                    /* keep the first sort criteria */
                    for (unsigned int j = 0; j < i; j++)
                        new_sortby = PFord_refine (
                                         new_sortby,
                                         PFord_order_col_at (sortby, j),
                                         PFord_order_dir_at (sortby, j));

                    /* replace entry i by the list of sort criteria
                       it represent */
                    for (unsigned int j = 0; j < lcount; j++)
                        new_sortby = PFord_refine (
                                         new_sortby,
                                         COL_AT (lsortby, j),
                                         DIR_AT (lsortby, j));
                    
                    /* keep the remaining sort criteria */
                    for (unsigned int j = i+1; j < count; j++)
                        new_sortby = PFord_refine (
                                         new_sortby,
                                         PFord_order_col_at (sortby, j),
                                         PFord_order_dir_at (sortby, j));

                    /* push up the internal rank operator */
                    *p = *(rank_opt (
                              op (LL(p),
                                  p->sem.sort.res,
                                  new_sortby),
                              L(p)->sem.rank_opt.res,
                              lsortby));
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
                    unsigned int count, lcount, matches, lmatches;

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
                        if (matches > 1)
                            break; /* do not rewrite */
                    }

                    *p = *(rank_opt (
                              rank_opt (
                                  LL(p),
                                  p->sem.rank_opt.res,
                                  sortby),
                              L(p)->sem.rank_opt.res,
                              lsortby));
                    modified = true;
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
                                .dir = DIR_AT (lsortby, j),
                                .vis = vis && VIS_AT (lsortby, j)
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
                    modified = true;
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
                                           p->sem.doc_join.item_doc),
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

        case la_error: /* don't rewrite errors */
            break;

        case la_cond_err:
            if (is_rr (L(p))) {
                *p = *(rank_opt (cond_err (LL(p), R(p),
                                           p->sem.err.col,
                                           p->sem.err.str),
                                 L(p)->sem.rank_opt.res,
                                 L(p)->sem.rank_opt.sortby));
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

    return modified;
}

/**
 * intro_internal_rank replaces all rank
 * operator by an intermediate internal representation
 * that is able to cope with invisible orderings.
 */
static void
intro_internal_rank (PFla_op_t *p)
{
    unsigned int i;

    /* rewrite each node only once */
    if (SEEN(p))
        return;

    /* traverse children */
    for (i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        intro_internal_rank (p->child[i]);

    /* replace original rank operators by the internal
       variant */
    if (p->kind == la_rank) {
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

    SEEN(p) = true;
}

/**
 * remove_internal_rank replaces all intermediate
 * rank operators by normal ones. A pruning projection
 * on top of the operator chain ensures that only the
 * expected column names are visible.
 */
static void
remove_internal_rank (PFla_op_t *p)
{
    PFalg_col_t  cur_col;
    unsigned int i;

    /* rewrite each node only once */
    if (SEEN(p))
        return;

    /* traverse children */
    for (i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        remove_internal_rank (p->child[i]);

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

        *p = *PFla_project_ (rank (L(p), p->sem.rank_opt.res, sortby),
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
    /* Replace rank operator by the internal rank
       representation needed for this optimization phase. */
    intro_internal_rank (root);
    PFla_dag_reset (root);

    /* Traverse the DAG bottom up and look for op-rank
       operator pairs. As long as we find a rewrite we start
       a new traversal. */
    while (opt_rank (root))
        PFla_dag_reset (root);
    PFla_dag_reset (root);

    /* Replace the internal rank representation by
       normal ranks. */
    remove_internal_rank (root);
    PFla_dag_reset (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
