/**
 * @file
 *
 * Push down equi-join operators as much as possible.
 * This optimization removes a number of mapping joins.
 *
 * The patterns of the transformation rules consist of an equi-join
 * and one child operator. The child operators can be splitted into
 * seven groups:
 * 1.) operators that do not support equi-join pushdown (e.g. the ones
 *     with a fixed schema or leaf operators)
 * 2.) operators that always support the transformation (select,
 *     roots, and type assertion)
 * 3.) operators that support the transformation if the do not create
 *     a join column (e.g. comparisons and calculations)
 * 4.) operators that fit into the 4. category and additionally require
 *     the cardinality to stay the same (rownum, rowid, attribute, and
 *     textnode constructor)
 * 5.) cross product and thetajoin operators
 * 6.) equi-join operator
 * 7.) project operator
 * The last four groups (4.-7.) require special treatment (for details
 * on the transformation please look into the code).
 *
 * In addition a transformation rule that combines adjacent projections
 * is copied from opt_general.brg to keep the plans small. One more
 * transformation rule removes equi-joins that join the same relation
 * on the same column whose values are unique.
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

#include "oops.h"
#include "algopt.h"
#include "properties.h"
#include "alg_dag.h"
#include "mem.h"          /* PFmalloc() */
#include "map_names.h"

/* mnemonic algebra constructors */
#include "logical_mnemonic.h"

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define SEEN(p) ((p)->bit_dag)
#define LEFT(p) ((p)->bit_in)
#define RIGHT(p) ((p)->bit_out)

/**
 * Use equi-join implementation that replaces
 * both join argument by a result column.
 */
#define eqjoin_opt(a,b,c,d) PFla_eqjoin_opt_internal ((a),(b),(c),(d))

#define proj_at(l,i) (*(PFalg_proj_t *) PFarray_at ((l),(i)))
#define proj_add(l)  (*(PFalg_proj_t *) PFarray_add ((l)))
#define lproj(p) ((p)->sem.eqjoin_opt.lproj)
#define rproj(p) ((p)->sem.eqjoin_opt.rproj)
#define lcol(p)  (proj_at(lproj(p),0).old)
#define rcol(p)  (proj_at(rproj(p),0).old)
#define res(p)   (proj_at(lproj(p),0).new)

/* abbreviation for column dependency test */
static bool
is_join_col (PFla_op_t *p, PFalg_col_t col)
{
    assert (p && p->kind == la_internal_op);

    return (lcol(p) == (col) || rcol(p) == (col));
}

/* for an eqjoin_opt operator map an 'old' column name to a 'new' one */
static PFalg_col_t
map_proj_name (PFla_op_t *p, bool leftside, PFalg_col_t col)
{
    assert (p && p->kind == la_internal_op);

    PFarray_t *proj = leftside ? lproj(p) : rproj(p);

    for (unsigned int i = 0; i < PFarray_last (proj); i++)
        if (proj_at (proj, i).old == col)
            return proj_at (proj, i).new;
   
    PFoops (OOPS_FATAL,
            "Could not find column name in projection list.");

    return col_NULL;
}
#define map_col(col) (map_proj_name (p, left, (col)))

/* worker for binary operators */
static PFla_op_t *
modify_binary_op (PFla_op_t *p,
                  PFla_op_t *lp,
                  PFla_op_t *rp,
                  PFla_op_t * (* op) (const PFla_op_t *,
                                      PFalg_col_t,
                                      PFalg_col_t,
                                      PFalg_col_t))
{
    PFarray_t  *lproj, *rproj;
    bool        left;
    PFla_op_t  *next_join = NULL;

    /* parametrize optimization to use the same code
       for the left and the right operand of the equi-join */
    if (L(p) == lp) {
        lproj = lproj(p);
        rproj = rproj(p);
        left  = true;
    } else {
        lproj = rproj(p);
        rproj = lproj(p);
        left  = false;
    }

    if (!is_join_col (p, lp->sem.binary.res)) {
        *p = *(op (eqjoin_opt (L(lp), rp, lproj, rproj),
                   map_col (lp->sem.binary.res),
                   map_col (lp->sem.binary.col1),
                   map_col (lp->sem.binary.col2)));
        next_join = L(p);
    }
    return next_join;
}

/* worker for unary operators */
static PFla_op_t *
modify_unary_op (PFla_op_t *p,
                 PFla_op_t *lp,
                 PFla_op_t *rp,
                 PFla_op_t * (* op) (const PFla_op_t *,
                                     PFalg_col_t,
                                     PFalg_col_t))
{
    PFarray_t  *lproj, *rproj;
    bool        left;
    PFla_op_t  *next_join = NULL;

    /* parametrize optimization to use the same code
       for the left and the right operand of the equi-join */
    if (L(p) == lp) {
        lproj = lproj(p);
        rproj = rproj(p);
        left  = true;
    } else {
        lproj = rproj(p);
        rproj = lproj(p);
        left  = false;
    }

    if (!is_join_col (p, lp->sem.unary.res)) {
        *p = *(op (eqjoin_opt (L(lp), rp, lproj, rproj),
                   map_col (lp->sem.unary.res),
                   map_col (lp->sem.unary.col)));
        next_join = L(p);
    }
    return next_join;
}

/**
 * top-down worker for join_pushdown
 *
 * join_pushdown_worker takes an equi-join and
 * tries to push it down the DAG as much as possible.
 */
static bool
join_pushdown_worker (PFla_op_t *p, PFarray_t *clean_up_list)
{
    PFla_op_t   *lp,
                *rp;
    PFarray_t   *lproj,
                *rproj;
    PFalg_col_t  lcol,
                 rcol;
    PFla_op_t   *next_join = NULL;
    bool         modified  = false,
                 left;

    assert (p);

    /* only process equi-joins */
    assert (p->kind == la_internal_op);

    /* make sure that we clean up all the references afterwards */
    *(PFla_op_t **) PFarray_add (clean_up_list) = L(p);
    *(PFla_op_t **) PFarray_add (clean_up_list) = R(p);

    /* action code that tries to push an equi-join underneath
       its operands */
    for (unsigned int c = 0; c < 2; c++) {
        /* only process equi-joins */
        if (p->kind != la_internal_op)
            break;

        /* remove unnecessary joins
           (where both children references point to the same node) */
        if (L(p) == R(p) &&
            lcol(p) == rcol(p) &&
            PFprop_key (L(p)->prop, lcol(p))) {
            /* the join does nothing -- it only applies a key-join
               with itself and the schema stays the same.
               Thus replace it by a dummy projection */
            PFalg_proj_t *proj_list = PFmalloc (p->schema.count *
                                                sizeof (PFalg_proj_t));
            unsigned int  count = 0;

            /* copy the left projection list, ... */
            for (unsigned int i = 0; i < PFarray_last (lproj(p)); i++)
                proj_list[count++] = proj_at (lproj(p), i);
            /* copy the right projection list, ...
               ... and keep the result only once (discard the right join arg) */
            for (unsigned int i = 1; i < PFarray_last (rproj(p)); i++)
                proj_list[count++] = proj_at (rproj(p), i);

            assert (count == p->schema.count);

            *p = *(PFla_project_ (L(p), p->schema.count, proj_list));
            return true;
        }

        /* parametrize optimization to use the same code
           for the left and the right operand of the equi-join */
        if (c) {
            lp    = L(p);
            rp    = R(p);
            lproj = lproj(p);
            rproj = rproj(p);
            lcol  = lcol(p);
            rcol  = rcol(p);
            left  = true;
        } else {
            lp    = R(p);
            rp    = L(p);
            lproj = rproj(p);
            rproj = lproj(p);
            lcol  = rcol(p);
            rcol  = lcol(p);
            left  = false;
        }

        /* skip rewrite if the join might reference itself
           afterwards */
        if (LEFT(lp) && RIGHT(lp))
            continue;

        switch (lp->kind) {
            case la_serialize_seq:
            case la_serialize_rel:
            case la_side_effects:
            case la_lit_tbl:
            case la_empty_tbl:
            case la_ref_tbl:
            case la_intersect:
            case la_difference:
            case la_aggr:
            case la_step:
            case la_guide_step:
            case la_doc_tbl:
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
            case la_fragment:
            case la_frag_extract:
            case la_frag_union:
            case la_empty_frag:
            case la_string_join:
            case la_dummy:
                /* nothing to do -- can't push down the equi-join */
                break;

            case la_attach:
                if (!is_join_col (p, lp->sem.attach.res)) {
                    map_col (lp->sem.attach.res);
                    eqjoin_opt (L(lp), rp, lproj, rproj);
                    *p = *(attach (eqjoin_opt (L(lp), rp, lproj, rproj),
                                   map_col (lp->sem.attach.res),
                                   lp->sem.attach.value));
                    next_join = L(p);
                }
                break;

            case la_cross:
                /* We can push the equi-join operator only in one branch
                   of the cross product. Note however, that the renaming
                   projection of the equi-join operator also has to applied
                   to the other cross product argument as otherwise name
                   mismatches may occur. */
                if (PFprop_ocol (L(lp), lcol)) {
                    /* create projection list */
                    PFalg_proj_t *proj_list;
                    unsigned int  count = 0;
                    PFalg_col_t   cur;
                    proj_list = PFmalloc (R(lp)->schema.count *
                                          sizeof (*(proj_list)));
                    for (unsigned int i = 0; i < R(lp)->schema.count; i++) {
                        cur = map_col (R(lp)->schema.items[i].name);
                        proj_list[count++] = proj (cur,
                                                   R(lp)->schema.items[i].name);
                    }

                    /* push join below the left side */
                    *p = *(cross (eqjoin_opt (L(lp), rp, lproj, rproj),
                                  PFla_project_ (R(lp), count, proj_list)));
                } else {
                    /* create projection list */
                    PFalg_proj_t *proj_list;
                    unsigned int  count = 0;
                    PFalg_col_t   cur;
                    proj_list = PFmalloc (L(lp)->schema.count *
                                          sizeof (*(proj_list)));
                    for (unsigned int i = 0; i < L(lp)->schema.count; i++) {
                        cur = map_col (L(lp)->schema.items[i].name);
                        proj_list[count++] = proj (cur,
                                                   L(lp)->schema.items[i].name);
                    }

                    /* push join below the right side */
                    *p = *(cross (eqjoin_opt (R(lp), rp, lproj, rproj),
                                  PFla_project_ (L(lp), count, proj_list)));
                }

                next_join = L(p);
                *(PFla_op_t **) PFarray_add (clean_up_list) = R(p);
                break;

            case la_eqjoin:
                PFoops (OOPS_FATAL,
                        "clone column unaware eqjoin operator is "
                        "only allowed with original column names!");

            case la_internal_op:
            {
                PFla_op_t   *llp,
                            *lrp;
                PFarray_t   *llproj,
                            *lrproj,
                            *new_lproj,
                            *new_llproj,
                            *new_lrproj;
                PFalg_col_t  cur;
                unsigned int i;
                bool         lleft = false;

                /* find the operand the join column comes from */
                for (i = 0; i < PFarray_last (lp->sem.eqjoin_opt.lproj); i++)
                    if (proj_at (lp->sem.eqjoin_opt.lproj, i).new == lcol) {
                        lleft = true;
                        break;
                    }

                /* find branch to switch */
                if (lleft) {
                    llp    = L(lp);
                    lrp    = R(lp);
                    llproj = lp->sem.eqjoin_opt.lproj;
                    lrproj = lp->sem.eqjoin_opt.rproj;
                } else {
                    llp    = R(lp);
                    lrp    = L(lp);
                    llproj = lp->sem.eqjoin_opt.rproj;
                    lrproj = lp->sem.eqjoin_opt.lproj;
                }
                /* only rewrite if we hope to push the join further done */
                if (/* PFprop_key (rp->prop, rcol) && *//* be less restrictive */
                    lcol != proj_at (llproj, 0).new)
                    ;
                else if (rp->kind == la_rowid &&
                         lcol == proj_at (llproj, 0).new &&
                         llp->kind == la_rowid &&
                         rp == llp &&
                         llp->sem.rowid.res == proj_at (rproj, 0).old &&
                         llp->sem.rowid.res == proj_at (llproj, 0).old)
                    ;
                else if (rp->kind == la_rowid &&
                         lcol == proj_at (lrproj, 0).new &&
                         lrp->kind == la_rowid &&
                         rp == lrp &&
                         lrp->sem.rowid.res == proj_at (rproj, 0).old &&
                         lrp->sem.rowid.res == proj_at (llproj, 0).old) {
                    PFla_op_t *tmp_op;
                    PFarray_t *tmp_proj;
                   
                    tmp_op = llp;
                    llp    = lrp;
                    lrp    = tmp_op;
                    
                    tmp_proj = llproj;
                    llproj   = lrproj;
                    lrproj   = tmp_proj;
                }
                else
                    break;
                    
                new_llproj = PFarray (sizeof (PFalg_proj_t),
                                      PFarray_last (llproj));
                new_lrproj = PFarray (sizeof (PFalg_proj_t),
                                      PFarray_last (lrproj));
                new_lproj  = PFarray (sizeof (PFalg_proj_t),
                                      PFarray_last (llproj) +
                                      PFarray_last (rproj) - 1);

                /* Push down renaming of lproj ... */
                                    
                /* ... into the now top right plan ... */
                for (i = 0; i < PFarray_last (lrproj); i++) {
                    proj_add (new_lrproj)
                        = proj (map_col (proj_at (lrproj, i).new),
                                proj_at (lrproj, i).old);
                }
                /* ... and into the new bottom left plan
                   where the join column of lproj is the join
                   column (the first entry in new_llproj) ... */
                for (i = 0; i < PFarray_last (llproj); i++) {
                    if (proj_at (llproj, i).new == lcol) {
                        proj_add (new_llproj)
                            = proj (map_col (proj_at (llproj, i).new),
                                    proj_at (llproj, i).old);
                        break;
                    }
                }
                /* ... and all other columns follow. */
                for (i = 0; i < PFarray_last (llproj); i++) {
                    if (proj_at (llproj, i).new != lcol)
                        proj_add (new_llproj)
                            = proj (map_col (proj_at (llproj, i).new),
                                    proj_at (llproj, i).old);
                }
                
                /* Create new lproj list (top left plan)
                   where the join column of llproj is the join
                   column (the first entry in new_lproj). */
                for (i = 0; i < PFarray_last (llproj); i++) {
                    cur = map_col (proj_at (llproj, i).new);
                    proj_add (new_lproj) = proj (cur, cur);
                }
                for (i = 1; i < PFarray_last (rproj); i++) {
                    cur = proj_at (rproj, i).new;
                    proj_add (new_lproj) = proj (cur, cur);
                }
                
                /* build up operators */
                *p = *eqjoin_opt (eqjoin_opt (llp,
                                              rp,
                                              new_llproj,
                                              rproj),
                                  lrp,
                                  new_lproj,
                                  new_lrproj);
                
                next_join = L(p);
                *(PFla_op_t **) PFarray_add (clean_up_list) = R(p);
                
                /* Make sure that this rewrite only reports
                   a modification if this rewrite wasn't
                   the only one. Otherwise we might end up
                   in an infinite loop. */
                modified = join_pushdown_worker (next_join,
                                                 clean_up_list);
                next_join = NULL;

            }   break;

            case la_semijoin:
                /* push join below the left side */
                *p = *(semijoin (eqjoin_opt (L(lp), rp, lproj, rproj),
                                 R(lp),
                                 map_col (lp->sem.eqjoin.col1),
                                 lp->sem.eqjoin.col2));

                next_join = L(p);
                *(PFla_op_t **) PFarray_add (clean_up_list) = R(p);
                break;

            case la_thetajoin:
                /* We can push the equi-join operator only in one branch
                   of the theta-join. Note however, that the renaming
                   projection of the equi-join operator also has to applied
                   to the other theta-join argument as otherwise name
                   mismatches may occur. */
            {
                PFalg_sel_t *pred = PFmalloc (lp->sem.thetajoin.count *
                                              sizeof (PFalg_sel_t));
                for (unsigned int i = 0; i < lp->sem.thetajoin.count; i++) {
                    pred[i] = lp->sem.thetajoin.pred[i];
                    pred[i].left  = map_col (pred[i].left);
                    pred[i].right = map_col (pred[i].right);
                }
                
                /* choose the correct operand of the theta-join
                   (the one with the join argument) to push down
                   the equi-join */
                if (PFprop_ocol (L(lp), lcol)) {
                    /* create projection list */
                    PFalg_proj_t *proj_list;
                    unsigned int  count = 0;
                    PFalg_col_t   cur;
                    proj_list = PFmalloc (R(lp)->schema.count *
                                          sizeof (*(proj_list)));
                    for (unsigned int i = 0; i < R(lp)->schema.count; i++) {
                        cur = map_col (R(lp)->schema.items[i].name);
                        proj_list[count++] = proj (cur,
                                                   R(lp)->schema.items[i].name);
                    }

                    /* push join below the left side */
                    *p = *(thetajoin (eqjoin_opt (L(lp), rp, lproj, rproj),
                                      PFla_project_ (R(lp), count, proj_list),
                                      lp->sem.thetajoin.count,
                                      pred));

                    next_join = L(p);
                    *(PFla_op_t **) PFarray_add (clean_up_list) = R(p);
                } else {
                    /* create projection list */
                    PFalg_proj_t *proj_list;
                    unsigned int  count = 0;
                    PFalg_col_t   cur;
                    proj_list = PFmalloc (L(lp)->schema.count *
                                          sizeof (*(proj_list)));
                    for (unsigned int i = 0; i < L(lp)->schema.count; i++) {
                        cur = map_col (L(lp)->schema.items[i].name);
                        proj_list[count++] = proj (cur,
                                                   L(lp)->schema.items[i].name);
                    }

                    /* push join below the right side */
                    *p = *(thetajoin (PFla_project_ (L(lp), count, proj_list),
                                      eqjoin_opt (R(lp), rp, lproj, rproj),
                                      lp->sem.thetajoin.count,
                                      pred));

                    next_join = R(p);
                    *(PFla_op_t **) PFarray_add (clean_up_list) = L(p);
                }
            }   break;

            case la_project:
                /* Arbitrary projections are pushed through the join operator
                   by:
                   1.) merging the projection lists of the projection
                       and the join operator
                   2.) preparing a projection list that provides the original
                       schema and correctly undoes the next step
                   3.) removing duplicate entries in the join operator
                       projection list
                   4.) extending the join operator projection list with
                       the input column that did not appear in the list
                       so far (new names).

                   The rewrite ensures that the projection list in the join
                   operator does not provide a column split (e.g., a:a, b:a).
                   This way the column name mapping stays unambiguous in the
                   other rewrite rules. */
            {
                PFarray_t    *new_lproj = PFarray (sizeof (PFalg_proj_t),
                                                   PFarray_last (lproj));
                PFalg_proj_t *proj_list = PFmalloc (p->schema.count *
                                                    sizeof (PFalg_proj_t));
                PFalg_col_t   cur;
                unsigned int  i,
                              j,
                              count = 0,
                              lproj_old_size;

                /* merge projection list of operand (lp)
                   and join operator (lproj) */
                for (i = 0; i < PFarray_last (lproj); i++) {
                    for (j = 0; j < lp->sem.proj.count; j++) 
                        if (proj_at (lproj, i).old ==
                            lp->sem.proj.items[j].new) {
                            proj_at (lproj, i).old = lp->sem.proj.items[j].old;
                            break;
                        }
                    if (j == lp->sem.proj.count)
                        PFoops (OOPS_FATAL,
                                "did not find matching column in projection");
                }
                
                /* collect projection list */ 
                
                /* for the left input add renaming projections for columns
                   that are referenced multiple times (use map_col() to look up
                   the same new name multiple times) */
                for (i = 0; i < PFarray_last (lproj); i++)
                    proj_list[count++] = proj (proj_at (lproj, i).new,
                                               map_col (proj_at (lproj, i).old));
                /* for the right input keep the projection list
                   but add the join result only once */
                for (i = 1; i < PFarray_last (rproj); i++)
                    proj_list[count++] = proj (proj_at (rproj, i).new,
                                               proj_at (rproj, i).new);

                /* remove all duplicate entries in lproj */

                /* Iterate over the projection list (lproj) and copy the
                   first occurrence of each 'old' name.
                   Note that the following code has to be aligned to map_col() */
                for (i = 0; i < PFarray_last (lproj); i++) {
                    cur = proj_at (lproj, i).old;
                    for (j = 0; j < PFarray_last (new_lproj); j++)
                        if (cur == proj_at (new_lproj, j).old)
                            break;
                    /* only map the first occurrence
                       of the (*:cur) projection pair */
                    if (j == PFarray_last (new_lproj))
                        proj_add (new_lproj) = proj_at (lproj, i);
                }
                /* replace projection list (lproj) by the pruned one */
                lproj = new_lproj;
                
                /* extend the internal projection list of the join operator
                   with all visible columns of the unfiltered input */
                lproj_old_size = PFarray_last (lproj);
                for (i = 0; i < L(lp)->schema.count; i++) {
                    cur = L(lp)->schema.items[i].name;
                    for (j = 0; j < lproj_old_size; j++)
                        if (cur == proj_at (lproj, j).old)
                            break;
                    /* any new column will be added
                       with an unused column name */
                    if (j == lproj_old_size)
                        proj_add (lproj) = proj (PFcol_new (cur), cur);
                }

                *p = *(PFla_project_ (eqjoin_opt (L(lp), rp, lproj, rproj),
                                      count,
                                      proj_list));
                next_join = L(p);

                /* Make sure that this rewrite only reports
                   a modification if this rewrite wasn't
                   the only one. Otherwise we might end up
                   in an infinite loop. */
                modified = join_pushdown_worker (next_join,
                                                 clean_up_list);
                next_join = NULL;
            }   break;

            case la_select:
                *p = *(select_ (eqjoin_opt (L(lp), rp, lproj, rproj),
                                map_col (lp->sem.select.col)));
                next_join = L(p);
                break;

            case la_pos_select:
                if (!PFprop_key (rp->prop, rcol) ||
                    !PFprop_subdom (rp->prop,
                                    PFprop_dom (lp->prop, lcol),
                                    PFprop_dom (rp->prop, rcol)))
                    /* Ensure that the values of the left join argument
                       are a subset of the values of the right join argument
                       and that the right join argument is keyed. These
                       two tests make sure that we have exactly one match per
                       tuple in the left relation and thus the result of the
                       positional select operator stays stable. */
                    break;

                {
                    PFord_ordering_t sortby;

                    /* create projection list */
                    PFalg_proj_t *proj_list;
                    unsigned int  count = 0;
                    PFalg_col_t   cur;
                    proj_list = PFmalloc (lp->schema.count *
                                          sizeof (*(proj_list)));
                    for (unsigned int i = 0; i < lp->schema.count; i++) {
                        cur = map_col (lp->schema.items[i].name);
                        proj_list[count++] = proj (lp->schema.items[i].name,
                                                   cur);
                    }

                    /* copy sortby criteria and change name of
                       sort column to equi-join result if it is
                       also a join argument */
                    sortby = PFordering ();

                    for (unsigned int i = 0;
                         i < PFord_count (lp->sem.pos_sel.sortby);
                         i++) {
                        sortby = PFord_refine (
                                     sortby,
                                     map_col (PFord_order_col_at (
                                                  lp->sem.pos_sel.sortby,
                                                  i)),
                                     PFord_order_dir_at (
                                         lp->sem.pos_sel.sortby,
                                         i));
                    }

                    /* make sure that the other operators see the correct columns */
                    *p = *(pos_select (eqjoin_opt (L(lp), rp, lproj, rproj),
                                       lp->sem.pos_sel.pos,
                                       sortby,
                                       lp->sem.pos_sel.part
                                           ? map_col (lp->sem.pos_sel.part)
                                           : col_NULL));

                    /* the schema of the new positional selection operator
                       has to be pruned to maintain the schema
                       of the original positional selection operator
                       -- its pointer is replaced */
                    *lp = *(PFla_project_ (p, count, proj_list));

                    next_join = L(p);
                    break;
                }
                break;

            case la_disjunion:
                /* disable the following rewrite as it leads to a plan
                   explosion for more complex queries (see bug #1991738) */
                break;
#if 0
            {
                /* In situations where we apply actions on the
                   empty sequence and append the generated rows with a union
                   a join pushdown does not help (-- it will be stopped at
                   the difference operator anyway).
                   Thus we avoid to rewrite any union that uses the outcome
                   of a difference operation. */
                PFla_op_t *cur;
                bool diff;

                /* check for difference in left subtree */
                cur = L(lp);
                diff = false;
                while (cur) {
                    switch (cur->kind)
                    {
                        case la_project:
                        case la_attach:
                            cur = L(cur);
                            break;
                        case la_difference:
                            diff = true;
                            /* continue */
                        default:
                            cur = NULL; /* stop processing */
                            break;
                    }
                }
                /* stop rewriting if difference was discovered */
                if (diff)
                    break;

                /* check for difference in left subtree */
                cur = R(lp);
                diff = false;
                while (cur) {
                    switch (cur->kind)
                    {
                        case la_project:
                        case la_attach:
                            cur = L(cur);
                            break;
                        case la_difference:
                            diff = true;
                            /* continue */
                        default:
                            cur = NULL; /* stop processing */
                            break;
                    }
                }
                /* stop rewriting if difference was discovered */
                if (diff)
                    break;

                /* This rewrite is only correct if the union operator
                   is implemented as a union all operation. */
                *p = *disjunion (eqjoin_opt (L(lp), rp, lproj, rproj),
                                 eqjoin_opt (R(lp), rp, lproj, rproj));

                modified = true;

                join_pushdown_worker (L(p), clean_up_list);
                join_pushdown_worker (R(p), clean_up_list);
            } break;
#endif

            case la_distinct:
                if (!PFprop_key (rp->prop, rcol) ||
                    !PFprop_subdom (rp->prop,
                                    PFprop_dom (lp->prop, lcol),
                                    PFprop_dom (rp->prop, rcol)))
                    /* Ensure that the values of the left join argument
                       are a subset of the values of the right join argument
                       and that the right join argument is keyed. These
                       two tests make sure that we have exactly one match per
                       tuple in the left relation and thus that all other
                       columns in the right join partner are functionally
                       dependent on the join column. */
                    break;
                
                *p = *(distinct (eqjoin_opt (L(lp), rp, lproj, rproj)));
                next_join = L(p);
                break;
            
            case la_fun_1to1:
                if (!is_join_col (p, lp->sem.fun_1to1.res)) {
                    PFalg_collist_t *refs;
                    refs = PFalg_collist_copy (lp->sem.fun_1to1.refs);

                    for (unsigned int i = 0; i < clsize (refs); i++)
                        clat (refs, i) = map_col (clat (refs, i));

                    *p = *(fun_1to1 (eqjoin_opt (L(lp), rp, lproj, rproj),
                                     lp->sem.fun_1to1.kind,
                                     map_col (lp->sem.fun_1to1.res),
                                     refs));
                    next_join = L(p);
                }
                break;

            case la_num_eq:
                next_join = modify_binary_op (p, lp, rp, PFla_eq);
                break;
            case la_num_gt:
                next_join = modify_binary_op (p, lp, rp, PFla_gt);
                break;
            case la_bool_and:
                next_join = modify_binary_op (p, lp, rp, PFla_and);
                break;
            case la_bool_or:
                next_join = modify_binary_op (p, lp, rp, PFla_or);
                break;
            case la_bool_not:
                next_join = modify_unary_op (p, lp, rp, PFla_not);
                break;
            case la_to:
                next_join = modify_binary_op (p, lp, rp, PFla_to);
                break;

            case la_rownum:
                if (!PFprop_key (rp->prop, rcol) ||
                    !PFprop_subdom (rp->prop,
                                    PFprop_dom (lp->prop, lcol),
                                    PFprop_dom (rp->prop, rcol)))
                    /* Ensure that the values of the left join argument
                       are a subset of the values of the right join argument
                       and that the right join argument is keyed. These
                       two tests make sure that we have exactly one match per
                       tuple in the left relation and thus the result of the
                       rownum operator stays stable. */
                    break;

            case la_rowrank:
            case la_rank:
                if (!is_join_col (p, lp->sem.sort.res)) {
                    PFord_ordering_t sortby;
                    PFla_op_t *eqjoin;

                    /* create projection list */
                    PFalg_proj_t *proj_list;
                    unsigned int  count = 0;
                    PFalg_col_t   cur;
                    proj_list = PFmalloc (lp->schema.count *
                                          sizeof (*(proj_list)));
                    for (unsigned int i = 0; i < lp->schema.count; i++) {
                        cur = map_col (lp->schema.items[i].name);
                        proj_list[count++] = proj (lp->schema.items[i].name,
                                                   cur);
                    }

                    /* copy sortby criteria and change name of
                       sort column to equi-join result if it is
                       also a join argument */
                    sortby = PFordering ();

                    for (unsigned int i = 0;
                         i < PFord_count (lp->sem.sort.sortby);
                         i++) {
                        sortby = PFord_refine (
                                     sortby,
                                     map_col (PFord_order_col_at (
                                                  lp->sem.sort.sortby,
                                                  i)),
                                     PFord_order_dir_at (
                                         lp->sem.sort.sortby,
                                         i));
                    }

                    eqjoin = eqjoin_opt (L(lp), rp, lproj, rproj);

                    /* make sure that the other operators see the correct columns */
                    if (lp->kind == la_rownum) {
                        *p = *(rownum (eqjoin,
                                       map_col (lp->sem.sort.res),
                                       sortby,
                                       lp->sem.sort.part
                                           ? map_col (lp->sem.sort.part)
                                           : col_NULL));

                        /* the schema of the new operator has to be pruned
                           to maintain the schema of the original rownum,
                           operator -- its pointer is replaced */
                        *lp = *(PFla_project_ (p, count, proj_list));
                    }
                    else if (lp->kind == la_rowrank) {
                        *p = *(rowrank (eqjoin,
                                        map_col (lp->sem.sort.res),
                                        sortby));

                        /* If there is more than this single reference
                           to the rowrank operator we check whether the values
                           of the left join argument are a subset of the values
                           of the right join argument and that the right join
                           argument is keyed. These two tests make sure that we
                           have exactly one match per tuple in the left relation
                           and thus the result of the rowrank operator can be
                           reused. */
                        if (PFprop_refctr (lp) > 1 &&
                            PFprop_key (rp->prop, rcol) &&
                            PFprop_subdom (rp->prop,
                                           PFprop_dom (lp->prop, lcol),
                                           PFprop_dom (rp->prop, rcol)))
                            *lp = *(PFla_project_ (p, count, proj_list));
                    }
                    else if (lp->kind == la_rank) {
                        *p = *(rank (eqjoin,
                                     map_col (lp->sem.sort.res),
                                     sortby));
                    
                        /* If there is more than this single reference
                           to the rank operator we check whether the values
                           of the left join argument are a subset of the values
                           of the right join argument and that the right join
                           argument is keyed. These two tests make sure that we
                           have exactly one match per tuple in the left relation
                           and thus the result of the rank operator can be reused. */
                        if (PFprop_refctr (lp) > 1 &&
                            PFprop_key (rp->prop, rcol) &&
                            PFprop_subdom (rp->prop,
                                           PFprop_dom (lp->prop, lcol),
                                           PFprop_dom (rp->prop, rcol)))
                            *lp = *(PFla_project_ (p, count, proj_list));
                    }

                    next_join = L(p);
                    break;
                }
                break;

            case la_rowid:
                if (!PFprop_key (rp->prop, rcol) ||
                    !PFprop_subdom (rp->prop,
                                    PFprop_dom (lp->prop, lcol),
                                    PFprop_dom (rp->prop, rcol)))
                    /* Ensure that the values of the left join argument
                       are a subset of the values of the right join argument
                       and that the right join argument is keyed. These
                       two tests make sure that we have exactly one match per
                       tuple in the left relation and thus the result of the
                       rowid operator stays stable. */
                    break;

                if (!is_join_col (p, lp->sem.rowid.res)) {
                    /* create projection list */
                    PFalg_proj_t *proj_list;
                    unsigned int  count = 0;
                    PFalg_col_t   cur;
                    proj_list = PFmalloc (lp->schema.count *
                                          sizeof (*(proj_list)));
                    for (unsigned int i = 0; i < lp->schema.count; i++) {
                        cur = map_col (lp->schema.items[i].name);
                        proj_list[count++] = proj (lp->schema.items[i].name,
                                                   cur);
                    }

                    /* make sure that frag and roots see
                       the new column node */
                    *p = *(rowid (eqjoin_opt (L(lp), rp, lproj, rproj),
                                  map_col (lp->sem.rowid.res)));

                    /* the schema of the new rowid operator has to
                       be pruned to maintain the schema of the original
                       rowid operator -- its pointer is replaced */
                    *lp = *(PFla_project_ (p, count, proj_list));

                    next_join = L(p);
                    break;
                }
                break;

            case la_type:
                if (!is_join_col (p, lp->sem.type.res)) {
                    *p = *(type (eqjoin_opt (L(lp), rp, lproj, rproj),
                                 map_col (lp->sem.type.res),
                                 map_col (lp->sem.type.col),
                                 lp->sem.type.ty));
                    next_join = L(p);
                }
                break;

            case la_type_assert:
                if (!is_join_col (p, lp->sem.type.col)) {
                    *p = *(type_assert_pos (eqjoin_opt (L(lp), rp,
                                                        lproj, rproj),
                                            map_col (lp->sem.type.col),
                                            lp->sem.type.ty));
                    next_join = L(p);
                }
                break;

            case la_cast:
                if (!is_join_col (p, lp->sem.type.res)) {
                    *p = *(cast (eqjoin_opt (L(lp), rp, lproj, rproj),
                                 map_col (lp->sem.type.res),
                                 map_col (lp->sem.type.col),
                                 lp->sem.type.ty));
                    next_join = L(p);
                }
                break;

            case la_step_join:
                if (!is_join_col (p, lp->sem.step.item_res)) {
                    *p = *(step_join (
                               L(lp),
                               eqjoin_opt (R(lp), rp, lproj, rproj),
                               lp->sem.step.spec,
                               lp->sem.step.level,
                               map_col (lp->sem.step.item),
                               map_col (lp->sem.step.item_res)));
                    next_join = R(p);
                }
                break;

            case la_guide_step_join:
                if (!is_join_col (p, lp->sem.step.item_res)) {
                    *p = *(guide_step_join (
                               L(lp),
                               eqjoin_opt (R(lp), rp, lproj, rproj),
                               lp->sem.step.spec,
                               lp->sem.step.guide_count,
                               lp->sem.step.guides,
                               lp->sem.step.level,
                               map_col (lp->sem.step.item),
                               map_col (lp->sem.step.item_res)));
                    next_join = R(p);
                }
                break;

            case la_doc_index_join:
                if (!is_join_col (p, lp->sem.doc_join.item_res)) {
                    *p = *(doc_index_join (
                               L(lp),
                               eqjoin_opt (R(lp), rp, lproj, rproj),
                               lp->sem.doc_join.kind,
                               map_col (lp->sem.doc_join.item),
                               map_col (lp->sem.doc_join.item_res),
                               map_col (lp->sem.doc_join.item_doc),
                               lp->sem.doc_join.ns1,
                               lp->sem.doc_join.loc1,
                               lp->sem.doc_join.ns2,
                               lp->sem.doc_join.loc2));
                    next_join = R(p);
                }
                break;

            case la_roots:
                if ((!PFprop_key (rp->prop, rcol) ||
                     !PFprop_subdom (rp->prop,
                                     PFprop_dom (lp->prop, lcol),
                                     PFprop_dom (rp->prop, rcol))) &&
                     PFprop_refctr (lp) != 1)
                    /* Ensure that the values of the left join argument
                       are a subset of the values of the right join argument
                       and that the right join argument is keyed. These
                       two tests make sure that we have exactly one match per
                       tuple in the left relation and thus the result of the
                       roots operator stays stable. */
                    break;

                if (L(lp)->kind == la_doc_tbl &&
                    !is_join_col (p, L(lp)->sem.doc_tbl.res)) {
                    /* create projection list */
                    PFalg_proj_t *proj_list;
                    unsigned int  count = 0;
                    PFalg_col_t   cur;
                    proj_list = PFmalloc (lp->schema.count *
                                          sizeof (*(proj_list)));
                    for (unsigned int i = 0; i < lp->schema.count; i++) {
                        cur = map_col (lp->schema.items[i].name);
                        proj_list[count++] = proj (lp->schema.items[i].name,
                                                   cur);
                    }

                    *L(lp) = *doc_tbl (eqjoin_opt (LL(lp), rp, lproj, rproj),
                                       map_col (L(lp)->sem.doc_tbl.res),
                                       map_col (L(lp)->sem.doc_tbl.col),
                                       L(lp)->sem.doc_tbl.kind);
                    *p = *roots (L(lp));

                    /* the schema of the new roots operator has to
                       be pruned to maintain the schema of the original
                       roots operator -- its pointer is replaced */
                    *lp = *(PFla_project_ (p, count, proj_list));

                    next_join = LL(p);
                }
                break;

            case la_doc_access:
                if (!is_join_col (p, lp->sem.doc_access.res)) {
                    *p = *(doc_access (L(lp),
                                       eqjoin_opt (R(lp), rp, lproj, rproj),
                                       map_col (lp->sem.doc_access.res),
                                       map_col (lp->sem.doc_access.col),
                                       lp->sem.doc_access.doc_col));
                    next_join = R(p);
                }
                break;

            case la_error:
            case la_nil:
            case la_cache:
            case la_trace:
                break;

            case la_trace_items:
                /* this operator will always be referenced
                   by a la_trace operator */
                break;

            case la_trace_msg:
                /* this operator will always be referenced
                   by a la_trace_items operator */
                break;

            case la_trace_map:
                /* this operator will always be referenced
                   by a la_trace_msg operator */
                break;

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
                PFoops (OOPS_FATAL,
                        "cannot cope with proxy nodes");
                break;
        }

        if (next_join)
            /* we already rewrote the join and
               thus do not need the second iteration */
            break;
    }

    if (next_join) {
        /* If we have rewritten a join continue with
           the new instance of that join (next_join). */
        modified = true;
        join_pushdown_worker (next_join, clean_up_list);
    }

    return modified;
}

/**
 * map_name looks up the (possibly) modified name of a column.
 * It returns the invalid column name col_NULL if the mapping
 * did not result in a (new) name.
 */
static PFalg_col_t
map_name (PFla_op_t *p, PFalg_col_t col)
{
    switch (p->kind) {
        case la_serialize_seq:
        case la_serialize_rel:
        case la_side_effects:
        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
        case la_eqjoin:
        case la_intersect:
        case la_difference:
        case la_distinct:
        case la_to:
        case la_aggr:
        case la_step:
        case la_guide_step:
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
        case la_string_join:
        case la_error:
        case la_nil:
        case la_cache:
        case la_trace:
        case la_trace_items:
        case la_trace_msg:
        case la_trace_map:
        case la_rec_fix:
        case la_rec_param:
        case la_rec_arg:
        case la_rec_base:
        case la_fun_call:
        case la_fun_param:
        case la_fun_frag_param:
        case la_proxy:
        case la_proxy_base:
        case la_dummy:
            /* join can't be pushed down anyway */
            return col_NULL;

        case la_cross:
        case la_select:
        case la_pos_select:
        case la_disjunion:
        case la_semijoin:
        case la_thetajoin:
            /* name does not change */
            break;

        case la_internal_op:
            for (unsigned int i = 0; i < PFarray_last (lproj(p)); i++)
                if (proj_at (lproj(p), i).new == col)
                    return proj_at (lproj(p), i).old;
            for (unsigned int i = 0; i < PFarray_last (rproj(p)); i++)
                if (proj_at (rproj(p), i).new == col)
                    return proj_at (rproj(p), i).old;
            break;
            
        case la_fragment:
        case la_frag_extract:
        case la_frag_union:
        case la_empty_frag:
            PFoops (OOPS_FATAL,
                    "subtree marking should not reach this operator");
            break;

        case la_project:
            for (unsigned int i = 0; i < p->sem.proj.count; i++)
                if (p->sem.proj.items[i].new == col)
                    return p->sem.proj.items[i].old;

        case la_attach:
            if (col == p->sem.attach.res) return col_NULL;
            break;
        case la_fun_1to1:
            if (col == p->sem.fun_1to1.res) return col_NULL;
            break;
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
            if (col == p->sem.binary.res) return col_NULL;
            break;
        case la_bool_not:
            if (col == p->sem.unary.res) return col_NULL;
            break;
        case la_rownum:
        case la_rowrank:
        case la_rank:
            if (col == p->sem.sort.res) return col_NULL;
            break;
        case la_rowid:
            if (col == p->sem.rowid.res) return col_NULL;
            break;
        case la_type:
        case la_cast:
        case la_type_assert:
            if (col == p->sem.type.res) return col_NULL;
            break;
        case la_step_join:
        case la_guide_step_join:
            if (col == p->sem.step.item_res) return col_NULL;
            break;
        case la_doc_index_join:
            if (col == p->sem.doc_join.item_res) return col_NULL;
            break;
        case la_doc_tbl:
            if (col == p->sem.doc_tbl.res) return col_NULL;
            break;
        case la_doc_access:
            if (col == p->sem.doc_access.res) return col_NULL;
            break;
    }
    return col;
}

/**
 * mark_left_subdag marks all operators in the DAG
 * underneath p as being LEFT children of the join.
 */
static void
mark_left_subdag (PFla_op_t *p)
{
    assert (p);

    if (LEFT(p))
       return;

    if (p->kind == la_frag_union ||
        p->kind == la_empty_frag)
        return;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        mark_left_subdag (p->child[i]);

    LEFT(p) = true;
}

/**
 * mark_right_subdag marks all operators in the DAG
 * underneath p as being RIGHT children of the join.
 */
static void
mark_right_subdag (PFla_op_t *p)
{
    assert (p);

    if (RIGHT(p))
       return;

    if (p->kind == la_frag_union ||
        p->kind == la_empty_frag)
        return;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        mark_right_subdag (p->child[i]);

    RIGHT(p) = true;
}

/**
 * mark_left_path follows a path based on an initial
 * column name and marks all operators on the path as
 * being LEFT children of the join.
 */
static void
mark_left_path (PFla_op_t *p, PFalg_col_t col)
{
    assert (p);

    if (LEFT(p))
       return;

    if (p->kind == la_frag_union ||
        p->kind == la_empty_frag)
        return;
    else if (p->kind == la_internal_op &&
             col == res(p)) {
        mark_left_path (L(p), lcol(p));
        mark_left_path (R(p), rcol(p));
        LEFT(p) = true;
        return;
    }
    else if (p->kind == la_disjunion ||
             p->kind == la_intersect ||
             p->kind == la_difference) {
        /* the name column is split into multiple columns
           so we fall back to the primitive variant */
        mark_left_subdag (p);
        return;
    }

    col = map_name (p, col);
    if (!col) {
        /* we could not follow the names and
           have to fall back to the primitive variant */
        mark_left_subdag (p);
        return;
    }

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        for (unsigned int j = 0; j < p->child[i]->schema.count; j++)
            if (col == p->child[i]->schema.items[j].name) {
                mark_left_path (p->child[i], col);
                break;
            }

    LEFT(p) = true;
}

/**
 * mark_right_path follows a path based on an initial
 * column name and marks all operators on the path as
 * being RIGHT children of the join.
 */
static void
mark_right_path (PFla_op_t *p, PFalg_col_t col)
{
    assert (p);

    if (RIGHT(p))
       return;

    if (p->kind == la_frag_union ||
        p->kind == la_empty_frag)
        return;
    else if (p->kind == la_internal_op &&
             col == res(p)) {
        mark_right_path (L(p), lcol(p));
        mark_right_path (R(p), rcol(p));
        LEFT(p) = true;
        return;
    }
    else if (p->kind == la_disjunion ||
             p->kind == la_intersect ||
             p->kind == la_difference) {
        /* the name column is split into multiple columns
           so we fall back to the primitive variant */
        mark_right_subdag (p);
        return;
    }

    col = map_name (p, col);

    if (!col) {
        /* we could not follow the names and
           have to fall back to the primitive variant */
        mark_right_subdag (p);
        return;
    }

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        for (unsigned int j = 0; j < p->child[i]->schema.count; j++)
            if (col == p->child[i]->schema.items[j].name) {
                mark_right_path (p->child[i], col);
                break;
            }

    RIGHT(p) = true;
}

/**
 * remove_marks reset both LEFT and RIGHT marks.
 * The processing traverses the subDAG until a node
 * without mark is reached.
 */
static void
remove_marks (PFla_op_t *p)
{
    assert (p);

    if (LEFT(p) || RIGHT (p))
        for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
            remove_marks (p->child[i]);

    LEFT(p) = false;
    RIGHT(p) = false;
}

/**
 * top-down worker for PFalgopt_join_pd
 *
 * join_pushdown looks up an equi-join and
 * tries to push it down the DAG as much as possible.
 */
static bool
join_pushdown (PFla_op_t *p, PFarray_t *clean_up_list)
{
    bool modified = false;

    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return false;

    /* apply join_pushdown for the children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        modified = join_pushdown (p->child[i], clean_up_list) || modified;

    /* combine multiple projections */
    if (p->kind == la_project &&
        L(p)->kind == la_project) {
        /* combine two projections */
        *p = *PFla_project_ (LL(p),
                             p->schema.count,
                             PFalg_proj_merge (
                                 p->sem.proj.items,
                                 p->sem.proj.count,
                                 L(p)->sem.proj.items,
                                 L(p)->sem.proj.count));

        /* do not mark phase modified as this might
           result in an infinite loop */
    }

    /* remove unnecessary joins
       (where both children references point to the same node) */
    if (p->kind == la_internal_op &&
        L(p) == R(p) &&
        lcol(p) == rcol(p) &&
        PFprop_key (L(p)->prop, lcol(p))) {
        /* the join does nothing -- it only applies a key-join
           with itself and the schema stays the same.
           Thus replace it by a dummy projection */
        PFalg_proj_t *proj_list = PFmalloc (p->schema.count *
                                            sizeof (PFalg_proj_t));
        unsigned int  count = 0;

        /* copy the left projection list, ... */
        for (unsigned int i = 0; i < PFarray_last (lproj(p)); i++)
            proj_list[count++] = proj_at (lproj(p), i);
        /* copy the right projection list, ...
           ... and keep the result only once (discard the right join arg) */
        for (unsigned int i = 1; i < PFarray_last (rproj(p)); i++)
            proj_list[count++] = proj_at (rproj(p), i);

        assert (count == p->schema.count);

        *p = *(PFla_project_ (L(p), p->schema.count, proj_list));
        modified = true;
    }

    /* We need to make sure that the eqjoin operator does not reference
       itself after the rewrite. Thus we make sure that no forbidden rewrite
       happens by marking all reachable nodes in the LEFT and the
       RIGHT subtree and prohibit any rewrite for algebra nodes that are
       marked LEFT and RIGHT. */
    if (p->kind == la_internal_op) {
        /* mark nodes in the left child as 'LEFT' */
        if (PFprop_key (L(p)->prop, lcol(p)) &&
            PFprop_subdom (p->prop,
                           PFprop_dom (R(p)->prop, rcol(p)),
                           PFprop_dom (L(p)->prop, lcol(p))))
            /* if the left join argument is key and a super domain
               of the right one we can be sure that the left side will
               never rewrite any node in the right subtree except for
               the nodes on the path of the join argument. */
            mark_left_path (L(p), lcol(p));
        else
            mark_left_subdag (L(p));

        /* mark nodes in the right child as 'RIGHT' */
        if (PFprop_key (R(p)->prop, rcol(p)) &&
            PFprop_subdom (p->prop,
                           PFprop_dom (L(p)->prop, lcol(p)),
                           PFprop_dom (R(p)->prop, rcol(p))))
            /* if the right join argument is key and a super domain
               of the left one we can be sure that the right side will
               never rewrite any node in the left subtree except for
               the nodes on the path of the join argument. */
            mark_right_path (R(p), rcol(p));
        else
            mark_right_subdag (R(p));

        /* start pushing down the join operator */
        modified = join_pushdown_worker (p, clean_up_list) || modified;

        /* clean up the remaining 'LEFT' and 'RIGHT' markers */
        for (unsigned int i = 0; i < PFarray_last (clean_up_list); i++)
            remove_marks (*(PFla_op_t **) PFarray_at (clean_up_list, i));
        PFarray_last (clean_up_list) = 0;

    }

    SEEN(p) = true;

    return modified;
}

/**
 * Introduce the special eqjoin operator with implicit
 * projection list that is pushed down in the optimization
 * phase.
 */
static void
introduce_eqjoin_opt (PFla_op_t *p)
{
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;
    
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        introduce_eqjoin_opt (p->child[i]);

    /* introduce the special eqjoin operator */
    if (p->kind == la_eqjoin) {
        unsigned int  lcount = L(p)->schema.count,
                      rcount = R(p)->schema.count,
                      i;
        PFarray_t    *lproj  = PFarray (sizeof (PFalg_proj_t), lcount),
                     *rproj  = PFarray (sizeof (PFalg_proj_t), rcount);
        PFalg_proj_t *projlist;
        PFalg_col_t   col1   = p->sem.eqjoin.col1,
                      col2   = p->sem.eqjoin.col2,
                      res    = col1 < col2 ? col1 : col2,
                      col;
        
        projlist  = PFmalloc (p->schema.count * sizeof (PFalg_proj_t));
        
        /* add the join columns as first arguments
           to the projection lists */
        proj_add (lproj) = proj (res, col1);
        proj_add (rproj) = proj (res, col2);
        
        /* fill the projection lists */
        for (i = 0; i < lcount; i++) {
            col = L(p)->schema.items[i].name;
            if (col == col1) {
                projlist[i] = proj (col1, res);
            }
            else {
                projlist[i] = proj (col, col);
                proj_add (lproj) = proj (col, col);
            }
        }
        for (i = 0; i < rcount; i++) {
            col = R(p)->schema.items[i].name;
            if (col == col2) {
                projlist[i+lcount] = proj (col2, res);
            }
            else {
                projlist[i+lcount] = proj (col, col);
                proj_add (rproj) = proj (col, col);
            }
        }
            
        *p = *PFla_project_ (
                  eqjoin_opt (L(p), R(p), lproj, rproj),
                  p->schema.count, projlist);
    }
}

/**
 * Replace the special eqjoin operator with implicit
 * projection list with the normal eqjoin operator.
 */
static void
remove_eqjoin_opt (PFla_op_t *p)
{
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;
    
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        remove_eqjoin_opt (p->child[i]);

    /* remove the special eqjoin operator */
    if (p->kind == la_internal_op) {
        PFarray_t    *lproj  = p->sem.eqjoin_opt.lproj,
                     *rproj  = p->sem.eqjoin_opt.rproj;
        PFalg_proj_t *projlist,
                     *lprojlist,
                     *rprojlist;
        unsigned int  lcount = PFarray_last (lproj),
                      rcount = PFarray_last (rproj),
                      i;
        PFalg_col_t   col1_new,
                      col2_old,
                      col2_new,
                      col;
        
        /* look up the join column names */
        col1_new = proj_at (lproj, 0).new;
        col2_old = proj_at (rproj, 0).old;
        /* get a new column name */
        col2_new = PFcol_new (col2_old);

        projlist  = PFmalloc (p->schema.count * sizeof (PFalg_proj_t));
        lprojlist = PFmalloc (lcount * sizeof (PFalg_proj_t));
        rprojlist = PFmalloc (rcount * sizeof (PFalg_proj_t));
        
        /* create the projection list for the left operand */
        for (i = 0; i < PFarray_last (lproj); i++)
            lprojlist[i] = proj_at (lproj, i);

        /* create the projection list for the right operand */
        rprojlist[0] = proj (col2_new, col2_old);
        for (i = 1; i < PFarray_last (rproj); i++)
            rprojlist[i] = proj_at (rproj, i);

        /* As some operators rely on the schema of its operands
           we introduce a projection that removes the second join
           column thus maintaining the schema of the duplicate
           aware eqjoin operator. */
        for (unsigned int i = 0; i < p->schema.count; i++) {
            col = p->schema.items[i].name;
            projlist[i] = proj (col, col);
        }
        
        *p = *PFla_project_ (
                  eqjoin (PFla_project_ (L(p), lcount, lprojlist),
                          PFla_project_ (R(p), rcount, rprojlist),
                          col1_new, col2_new),
                  p->schema.count, projlist);
    }
}

/**
 * Invoke algebra optimization.
 */
PFla_op_t *
PFalgopt_join_pd (PFla_op_t *root)
{
    PFarray_t *clean_up_list = PFarray (sizeof (PFla_op_t *), 40);
    unsigned int tries = 0, max_tries = 1;
    bool modified = true;

    /* Optimize algebra tree */
    while (modified || tries <= max_tries) {
        /* replace la_eqjoin by la_internal_op operators */
        introduce_eqjoin_opt (root);
        PFla_dag_reset (root);

        PFprop_infer_refctr (root);
        PFprop_infer_key (root);
        /* key property inference already requires
           the domain property inference for native
           types. Thus we can skip it:
        PFprop_infer_nat_dom (root);
        */

        modified = join_pushdown (root, clean_up_list);
        PFla_dag_reset (root);
        if (!modified) tries++;

        /* replace la_internal_op by la_eqjoin operators */
        remove_eqjoin_opt (root);
        PFla_dag_reset (root);

        /* The eqjoin removal might result in very wide relations.
           To speed up the unique name mapping we remove the superfluos
           columns. */
        root = PFalgopt_icol (root);

        /* make sure that column name splitting is removed */
        root = PFmap_unq_names (root);
    }

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
