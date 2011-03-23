/**
 * @file
 *
 * Optimize relational algebra expression DAG
 * based on multiple properties.
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
 * 2008-2011 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id$
 */

/* always include monetdb_config.h first! */
#include "monetdb_config.h"
#include "pathfinder.h"
#include <assert.h>
#include <stdio.h>

#include "algopt.h"
#include "properties.h"
#include "alg_dag.h"
#include "mem.h"          /* PFmalloc() */

/** mnemonic algebra constructors */
#include "logical_mnemonic.h"

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#include "oops.h"

#define SEEN(p) ((p)->bit_dag)

/* lookup the input name of an output column
   of a projection */
static PFalg_col_t
find_old_name (PFla_op_t *op, PFalg_col_t col)
{
    assert (op->kind == la_project);

    for (unsigned int i = 0; i < op->sem.proj.count; i++)
         if (op->sem.proj.items[i].new == col)
             return op->sem.proj.items[i].old;

    return col_NULL;
}

/**
 * Find a equi-join -- aggregate pattern and turn it into
 * an aggregate operator with multiple aggregates.
 *
 * We look for the following pattern, where we ensure that
 * all columns functionally depend on the join columns, the
 * aggregates partition their input by the join columns, and
 * at least operator agg_l or operator agg_r is present.
 *
 *          |X|
 *     _____/ \_______
 *    /               \
 * (pi_l1)?         (pi_r1)?
 *   |                |
 * (agg_l)?         (agg_r)?
 *   |                |
 * (pi_l2)?         (pi_r2)?
 *   |                |
 *   \______   _______/
 *          \ /
 *           op
 */
PFla_op_t *
aggregate_pattern (PFla_op_t *p)
{
    PFalg_col_t   lcol,
                  rcol;
    PFla_op_t    *lp,
                 *rp,
                 *lproj1 = NULL,
                 *laggr  = NULL,
                 *lproj2 = NULL,
                 *rproj1 = NULL,
                 *raggr  = NULL,
                 *rproj2 = NULL,
                 *res    = NULL;
    unsigned int  i,
                  j,
                  acount = 0,
                  pcount = 0;
    PFalg_aggr_t *aggr;
    PFalg_proj_t *proj;

    /* make sure that we start with an equi-join */
    if (p->kind != la_eqjoin) return NULL;

    /* make sure that all columns depend on the join argument */
    for (i = 0; i < p->schema.count; i++) {
        /* In case the column is not referenced we don't have to
           test its fd relationship. In case there exist columns
           that are not in the icols list they may be incorrectly
           turned into distinct aggregates. Subsequent icols
           optimization however deletes these aggregates. */
        if (PFprop_not_icol (p->prop, p->schema.items[i].name))
           continue;
        if (!PFprop_fd (p->prop,
                        p->sem.eqjoin.col1,
                        p->schema.items[i].name))
            return NULL;
    }

    /* make sure that the result may return the join argument
       as a key column */
    if (!PFprop_key (p->prop, p->sem.eqjoin.col1) &&
        !PFprop_set (p->prop))
        return NULL;

    /* checks for the left join side */

    lp   = L(p);
    lcol = p->sem.eqjoin.col1;
    /* record projection information */
    if (lp->kind == la_project) {
        lproj1 = lp;
        lp     = L(lp);
        lcol   = find_old_name (lproj1, lcol);
    }

    /* check for a matching aggregate */
    if (lp->kind == la_aggr &&
        lp->sem.aggr.part == lcol) {
        laggr = lp;
        lp = L(lp);
    }

    /* record projection information */
    if (lp->kind == la_project) {
        lproj2 = lp;
        lp     = L(lp);
        lcol   = find_old_name (lproj2, lcol);
    }

    /* checks for the right join side */

    rp   = R(p);
    rcol = p->sem.eqjoin.col2;
    /* record projection information */
    if (rp->kind == la_project) {
        rproj1 = rp;
        rp     = L(rp);
        rcol   = find_old_name (rproj1, rcol);
    }

    /* check for a matching aggregate */
    if (rp->kind == la_aggr &&
        rp->sem.aggr.part == rcol) {
        raggr = rp;
        rp = L(rp);
    }

    /* record projection information */
    if (rp->kind == la_project) {
        rproj2 = rp;
        rp     = L(rp);
        rcol   = find_old_name (rproj2, rcol);
    }

    /* check for the remainder of the pattern */
    if (lp != rp ||
        lcol != rcol ||
        (!laggr && !raggr))
        return NULL;

    /* We have to create for all column but the partition columns
       an aggregate. */
    aggr = PFmalloc ((p->schema.count - 2) * sizeof (PFalg_aggr_t));

    /* In case we have an aggregate we have to copy the aggregates
       to the result list. */
    if (laggr) {
        /* Merge the upper projection with the aggregate. For every
           result column (except for the partition column) an aggregate
           is generated (possibly leading to duplicate aggregates). */
        if (lproj1)
            for (i = 0; i < lproj1->sem.proj.count; i++) {
                PFalg_col_t new = lproj1->sem.proj.items[i].new,
                            old = lproj1->sem.proj.items[i].old;
                if (new != p->sem.eqjoin.col1) {
                    /* find matching index */
                    for (j = 0; j < laggr->sem.aggr.count; j++)
                        if (old == laggr->sem.aggr.aggr[j].res)
                            break;
                    /* index not found -- bail out */
                    if (j == laggr->sem.aggr.count)
                        return NULL;

                    aggr[acount++] = PFalg_aggr (laggr->sem.aggr.aggr[j].kind,
                                                 new,
                                                 laggr->sem.aggr.aggr[j].col);
                }
            }
        else
            /* copy the aggregate list */
            for (j = 0; j < laggr->sem.aggr.count; j++)
                aggr[acount++] = laggr->sem.aggr.aggr[j];

        /* Merge the lower projection with the aggregate by renaming
           the input columns of the aggregate. */
        if (lproj2)
            for (j = 0; j < acount; j++) {
                /* don't rename missing input columns */
                if (!aggr[j].col)
                    continue;
                for (i = 0; i < lproj2->sem.proj.count; i++)
                    if (aggr[j].col == lproj2->sem.proj.items[i].new) {
                        aggr[j].col = lproj2->sem.proj.items[i].old;
                        break;
                    }
                assert (i < lproj2->sem.proj.count);
            }
    }
    /* In case we have no aggregate we need to create for all columns
       (except for the join column) distinct aggregates. */
    else {
        /* we don't know how to handle two adjacent projections here */
        if (lproj2)
           return NULL;

        /* Turn renaming projections into aggregates with differently
           named input and output columns. */
        if (lproj1)
            for (i = 0; i < lproj1->sem.proj.count; i++) {
                PFalg_col_t new = lproj1->sem.proj.items[i].new,
                            old = lproj1->sem.proj.items[i].old;
                if (new != p->sem.eqjoin.col1)
                    aggr[acount++] = PFalg_aggr (alg_aggr_dist, new, old);
            }
        else
            for (i = 0; i < lp->schema.count; i++) {
                PFalg_col_t col = lp->schema.items[i].name;
                if (col != p->sem.eqjoin.col1)
                    aggr[acount++] = PFalg_aggr (alg_aggr_dist, col, col);
            }
    }

    /* In case we have an aggregate we have to copy the aggregates
       to the result list. */
    if (raggr) {
        /* Store the current offset to avoid renaming the aggregates
           of the left side. */
        unsigned int offset = acount;

        /* Merge the upper projection with the aggregate. For every
           result column (except for the partition column) an aggregate
           is generated (possibly leading to duplicate aggregates). */
        if (rproj1)
            for (i = 0; i < rproj1->sem.proj.count; i++) {
                PFalg_col_t new = rproj1->sem.proj.items[i].new,
                            old = rproj1->sem.proj.items[i].old;
                if (new != p->sem.eqjoin.col2) {
                    /* find matching index */
                    for (j = 0; j < raggr->sem.aggr.count; j++)
                        if (old == raggr->sem.aggr.aggr[j].res)
                            break;
                    /* index not found -- bail out */
                    if (j == raggr->sem.aggr.count)
                        return NULL;

                    aggr[acount++] = PFalg_aggr (raggr->sem.aggr.aggr[j].kind,
                                                 new,
                                                 raggr->sem.aggr.aggr[j].col);
                }
            }
        else
            /* copy the aggregate list */
            for (j = 0; j < raggr->sem.aggr.count; j++)
                aggr[acount++] = raggr->sem.aggr.aggr[j];

        /* Merge the lower projection with the aggregate by renaming
           the input columns of the aggregate. */
        if (rproj2)
            for (j = offset; j < acount; j++) {
                /* don't rename missing input columns */
                if (!aggr[j].col)
                    continue;
                for (i = 0; i < rproj2->sem.proj.count; i++)
                    if (aggr[j].col == rproj2->sem.proj.items[i].new) {
                        aggr[j].col = rproj2->sem.proj.items[i].old;
                        break;
                    }
                assert (i < rproj2->sem.proj.count);
            }
    }
    /* In case we have no aggregate we need to create for all columns
       (except for the join column) distinct aggregates. */
    else {
        /* we don't know how to handle two adjacent projections here */
        if (rproj2)
           return NULL;

        /* Turn renaming projections into aggregates with differently
           named input and output columns. */
        if (rproj1)
            for (i = 0; i < rproj1->sem.proj.count; i++) {
                PFalg_col_t new = rproj1->sem.proj.items[i].new,
                            old = rproj1->sem.proj.items[i].old;
                if (new != p->sem.eqjoin.col2)
                    aggr[acount++] = PFalg_aggr (alg_aggr_dist, new, old);
            }
        else
            for (i = 0; i < rp->schema.count; i++) {
                PFalg_col_t col = rp->schema.items[i].name;
                if (col != p->sem.eqjoin.col2)
                    aggr[acount++] = PFalg_aggr (alg_aggr_dist, col, col);
            }
    }

    /* create a projection list to correctly reflect the output schema */
    proj = PFmalloc (p->schema.count * sizeof (PFalg_proj_t));

    /* fill the projection list and remove duplicate aggregates */
    i = 0;
    while (i < acount) {
        /* check if we saw the same aggregate already */
        for (j = 0; j < i; j++)
            if (aggr[i].kind == aggr[j].kind &&
                aggr[i].col  == aggr[j].col) {
                break;
            }
        /* We have found the same aggregate and replace it by
           the last aggregate in the aggregate list. The projection
           ensures that the output references the found aggregate. */
        if (i != j) {
            proj[pcount++] = PFalg_proj (aggr[i].res, aggr[j].res);
            acount--;
            aggr[i] = aggr[acount];
        }
        /* We found a new aggregate and add its output to the projection
           list. */
        else {
            proj[pcount++] = PFalg_proj (aggr[i].res, aggr[i].res);
            i++;
        }
    }

    /* add the join (aka partition) columns to the projection list */
    proj[pcount++] = PFalg_proj (p->sem.eqjoin.col1, lcol);
    proj[pcount++] = PFalg_proj (p->sem.eqjoin.col2, lcol);

    assert (pcount == p->schema.count);

    /* build the new aggregate */
    res = PFla_project_ (aggr (lp, lcol, acount, aggr), pcount, proj);

    return res;
}

/**
 * Replace a thetajoin with a one-row literal table by a selection.
 */
static PFla_op_t *
replace_thetajoin (PFla_op_t *p, bool replace_left)
{
    PFla_op_t *lp  = replace_left ? L(p) : R(p),
              *res = replace_left ? R(p) : L(p);

    assert (p->kind == la_thetajoin &&
            PFprop_card (lp->prop) &&
            (lp->kind == la_lit_tbl ||
             (lp->kind == la_project && L(lp)->kind == la_lit_tbl)));

    /* attach all columns of the to-be-replaced side
       to the other side */
    for (unsigned int i = 0; i < lp->schema.count; i++) {
        assert (PFprop_const (p->prop,
                              lp->schema.items[i].name));
        res = attach (res,
                      lp->schema.items[i].name,
                      PFprop_const_val (
                          p->prop,
                          lp->schema.items[i].name));
    }

    /* evaluate selections on the relation */
    for (unsigned int i = 0; i < p->sem.thetajoin.count; i++) {
        PFalg_col_t col1 = p->sem.thetajoin.pred[i].left,
                    col2 = p->sem.thetajoin.pred[i].right,
                    new  = PFcol_new (col1),
                    tmp;
        switch (p->sem.thetajoin.pred[i].comp) {
            case alg_comp_eq:
                res = eq (res, new, col1, col2);
                break;
            case alg_comp_gt:
                res = gt (res, new, col1, col2);
                break;
            case alg_comp_ge:
                tmp = PFcol_new (col1);
                res = not (gt (res, tmp, col2, col1), new, tmp);
                break;
            case alg_comp_lt:
                res = gt (res, new, col2, col1);
                break;
            case alg_comp_le:
                tmp = PFcol_new (col1);
                res = not (gt (res, tmp, col1, col2), new, tmp);
                break;
            case alg_comp_ne:
                tmp = PFcol_new (col1);
                res = not (eq (res, tmp, col1, col2), new, tmp);
                break;
        }
        res = select_ (res, new);
    }
    /* remove additional columns */
    return PFla_project_ (res,
                          p->schema.count,
                          PFalg_proj_create (p->schema));
}

/**
 * This function checks for the following bigger pattern:
 *
 *                    |
 *                   sel_d
 *                    |
 *                   eq_d:<pos1,pos2>
 *                    |
 *                   pi*
 *                    |
 *                   |X|_<iter1,iter2>
 *         __________/ \_________
 *        /                      \
 *       pi*                     pi*
 *       |                        |
 *      row#_pos1:<col1>/iter1   row#_pos2:<col2>/iter2
 *       |                        |
 *       pi*                     pi*
 *        \__________   _________/
 *                   \ /
 *                    op
 *
 * where at operator op the following conditions need to
 * be fulfilled: col1 = col2 and iter1 = iter2.
 *
 * The pattern describes a split of two columns that
 * are merged back together (zip) based on their iter and pos
 * values. (The aligned row# operators ensure that iter+pos
 * provide a comparable key.)
 *
 * The result pattern avoids the splitting and correctly
 * reflects the renaming of the various projection operators
 * (in pi_...):
 *
 *                    |
 *                   pi_pos1:pos',pos2:pos',d:d',...
 *                    |
 *                    @_d':true
 *                    |
 *                   row#_pos':<col1>/iter1
 *                    |
 *                    op
 *
 * As the pattern checking is more complicated than the
 * replacement we marked all code snippets that prepare
 * the transformation as ACTION code to make the pattern
 * detection code more readible.
 */
static bool
zip_alignment (PFla_op_t *p)
{
    PFalg_col_t   pos1,
                  pos2,
                  iter1,
                  iter2,
                  sort1,
                  sort2;
    PFla_op_t    *op = p,
                 *lop,
                 *rop;
    /* ACTION: local variables needed for the transformation */
    unsigned int  count  = p->schema.count,
                  lcount = 0,
                  rcount = 0;
    PFalg_proj_t *proj,
                 *lproj,
                 *rproj;
    PFalg_col_t   new_rownum,
                  new_eq,
                  rownum1,
                  rownum2,
                  eq;
    /* end of action code part */

    /* check for pattern 'sel (eq (_))' where the selection
       consumes the output of comparison */
    if (op->kind != la_select ||
        L(op)->kind != la_num_eq ||
        op->sem.select.col != L(op)->sem.binary.res)
        return false;

    /* ACTION: initialize column name mapping
               to correctly rename all columns */
    proj = PFmalloc (count * sizeof (PFalg_proj_t));
    for (unsigned int i = 0; i < count; i++)
        proj[i] = PFalg_proj (p->schema.items[i].name,
                              p->schema.items[i].name);
    /* end of action code part */

    pos1 = L(op)->sem.binary.col1;
    pos2 = L(op)->sem.binary.col2;

    op = LL(op);

    /* update the important column names
       for all projection operators on the way */
    while (op->kind == la_project) {
        pos1 = find_old_name (op, pos1); if (!pos1) return false;
        pos2 = find_old_name (op, pos2); if (!pos2) return false;
        /* ACTION: update column name mapping */
        for (unsigned int i = 0; i < count; i++)
            proj[i].old = find_old_name (op, proj[i].old);
        /* end of action code part */
        op   = L(op);
    }

    /* check for join */
    if (op->kind != la_eqjoin)
        return false;

    iter1 = op->sem.eqjoin.col1;
    iter2 = op->sem.eqjoin.col2;

    lop = L(op);
    rop = R(op);

    /* ACTION: split up column name mapping into
               a left and a right mapping */
    lproj = PFmalloc (lop->schema.count * sizeof (PFalg_proj_t));
    rproj = PFmalloc (rop->schema.count * sizeof (PFalg_proj_t));
    for (unsigned int i = 0; i < count; i++) {
        if (PFprop_ocol (lop, proj[i].old))
            lproj[lcount++] = proj[i];
        else
            rproj[rcount++] = proj[i];
    }
    /* end of action code part */

    /* update the important column names
       for all projection operators on the way */
    while (lop->kind == la_project) {
        iter1 = find_old_name (lop, iter1); if (!iter1) return false;
        pos1  = find_old_name (lop, pos1);  if (!pos1)  return false;
        /* ACTION: update left column name mapping */
        for (unsigned int i = 0; i < lcount; i++)
            lproj[i].old = find_old_name (lop, lproj[i].old);
        /* end of action code part */
        lop   = L(lop);
    }
    while (rop->kind == la_project) {
        iter2 = find_old_name (rop, iter2); if (!iter2) return false;
        pos2  = find_old_name (rop, pos2);  if (!pos2)  return false;
        /* ACTION: update right column name mapping */
        for (unsigned int i = 0; i < rcount; i++)
            rproj[i].old = find_old_name (rop, rproj[i].old);
        /* end of action code part */
        rop   = L(rop);
    }

    /* check for the correct rownumber usage in the left
       and the right side of the equi-join */
    if (lop->kind != la_rownum ||
        rop->kind != la_rownum ||
        lop->sem.sort.res != pos1 ||
        rop->sem.sort.res != pos2 ||
        lop->sem.sort.part != iter1 ||
        rop->sem.sort.part != iter2 ||
        PFord_count (lop->sem.sort.sortby) != 1 ||
        PFord_count (rop->sem.sort.sortby) != 1 ||
        PFord_order_dir_at (lop->sem.sort.sortby, 0) != DIR_ASC ||
        PFord_order_dir_at (rop->sem.sort.sortby, 0) != DIR_ASC)
        return false;

    sort1 = PFord_order_col_at (lop->sem.sort.sortby, 0);
    sort2 = PFord_order_col_at (rop->sem.sort.sortby, 0);

    lop = L(lop);
    rop = L(rop);

    /* update the important column names
       for all projection operators on the way */
    while (lop->kind == la_project) {
        iter1 = find_old_name (lop, iter1); if (!iter1) return false;
        sort1 = find_old_name (lop, sort1); if (!sort1) return false;
        /* ACTION: update left column name mapping */
        for (unsigned int i = 0; i < lcount; i++)
            lproj[i].old = find_old_name (lop, lproj[i].old);
        /* end of action code part */
        lop   = L(lop);
    }
    while (rop->kind == la_project) {
        iter2 = find_old_name (rop, iter2); if (!iter2) return false;
        sort2 = find_old_name (rop, sort2); if (!sort2) return false;
        /* ACTION: update right column name mapping */
        for (unsigned int i = 0; i < rcount; i++)
            rproj[i].old = find_old_name (rop, rproj[i].old);
        /* end of action code part */
        rop   = L(rop);
    }

    /* check if we have a common operator
       and if all column names match */
    if (lop != rop ||
        iter1 != iter2 ||
        sort1 != sort2)
        return false;

    /* consistency check - we have to find all updated column names  */
    for (unsigned int i = 0; i < lcount; i++)
        if (lproj[i].old == col_NULL &&
            lproj[i].new != L(p)->sem.binary.col1)
            return false;

    /* consistency check - we have to find all updated column names  */
    for (unsigned int i = 0; i < rcount; i++)
        if (rproj[i].old == col_NULL &&
            rproj[i].new != L(p)->sem.binary.col2 &&
            rproj[i].new != L(p)->sem.binary.res)
            return false;

    /* ACTION: */
    /* create two new column names to avoid name conflicts */
    new_rownum = PFcol_new (col_pos);
    new_eq     = PFcol_new (col_item);
    rownum1    = L(p)->sem.binary.col1;
    rownum2    = L(p)->sem.binary.col2;
    eq         = L(p)->sem.binary.res;

    /* merge back the modified column names of the left
       name mapping into the initial list */
    for (unsigned int i = 0; i < lcount; i++)
        for (unsigned int j = 0; j < count; j++)
            if (lproj[i].new == proj[j].new) {
                proj[j].old = lproj[i].old;
                break;
            }
    /* merge back the modified column names of the right
       name mapping into the initial list */
    for (unsigned int i = 0; i < rcount; i++)
        for (unsigned int j = 0; j < count; j++)
            if (rproj[i].new == proj[j].new) {
                proj[j].old = rproj[i].old;
                break;
            }
    /* Adjust the column names of the three columns
       generated in the pattern (comparison and rownumber
       operators). */
    for (unsigned int j = 0; j < count; j++) {
        if (rownum1 == proj[j].new ||
            rownum2 == proj[j].new)
            proj[j].old = new_rownum;
        else if (eq == proj[j].new)
            proj[j].old = new_eq;
    }

    /* link the base only once and thus ignore the aligning
       join on columns iter and pos */
    *p = *PFla_project_ (
              attach (rownum (lop, new_rownum, sortby (sort1), iter1),
                      new_eq,
                      PFalg_lit_bln (true)),
              count,
              proj);
    /* end of action code part */

    /* we have rewritten the query plan
       based on the pattern */
    return true;
}


/**
 * find_last_base checks for the following
 * small DAG fragment:
 *
 *     /   pi*  \*
 *    |    |     |
 *     \  |X|   /     Note: All joins |X| are mapping joins
 *        / \               (the right join column is key).
 *      |X|  \
 *      / \
 *     |   \
 *    pi*  pi*
 *     |    |
 *     |   count_last/iter
 *     |    |
 *     |   pi*
 *     \   /
 *     (...)
 */
static bool
find_last_base (PFla_op_t *op,
                PFalg_col_t item,
                PFalg_col_t iter,
                PFalg_col_t last)
{
    PFla_op_t  *base = NULL;
    PFalg_col_t base_iter = col_NULL,
                base_last = col_NULL;

    /* ignore projections */
    while (op->kind == la_project) {
        iter = find_old_name (op, iter); if (!iter) return false;
        item = find_old_name (op, item); if (!item) return false;
        last = find_old_name (op, last); if (!last) return false;
        op = L(op);
    }

    if (op->kind != la_eqjoin ||
        (op->sem.eqjoin.col1 != iter &&
         op->sem.eqjoin.col2 != iter))
        return false;

    /* Ensure that the join is a mapping join (and decide
       based on column item where to look for the aggregate). */
    if (PFprop_ocol (L(op), item) &&
        PFprop_key_right (op->prop, op->sem.eqjoin.col2) &&
        PFprop_subdom (op->prop,
                       PFprop_dom_left (op->prop,
                                        op->sem.eqjoin.col1),
                       PFprop_dom_right (op->prop,
                                         op->sem.eqjoin.col2))) {
        base_iter = op->sem.eqjoin.col1;
        base_last = last;
        iter = op->sem.eqjoin.col2;
        base = L(op);
        op   = R(op);
    }
    else if (PFprop_ocol (R(op), item) &&
             PFprop_key_left (op->prop, op->sem.eqjoin.col1) &&
             PFprop_subdom (op->prop,
                            PFprop_dom_right (op->prop,
                                              op->sem.eqjoin.col2),
                            PFprop_dom_left (op->prop,
                                             op->sem.eqjoin.col1))) {
        base_iter = op->sem.eqjoin.col2;
        base_last = last;
        iter = op->sem.eqjoin.col1;
        base = R(op);
        op   = L(op);
    }
    else
        return false;

    /* ignore projections */
    while (op->kind == la_project) {
        iter = find_old_name (op, iter); if (!iter) break;
        last = find_old_name (op, last); if (!last) break;
        op = L(op);
    }

    /* check for count aggregate */
    if (op->kind == la_aggr &&
        op->sem.aggr.count == 1 &&
        op->sem.aggr.aggr[0].kind == alg_aggr_count &&
        op->sem.aggr.part &&
        op->sem.aggr.part == iter &&
        op->sem.aggr.aggr[0].res == last) {
        /* follow the projection list to the base operator */
        while (base->kind == la_project) {
            base_iter = find_old_name (base, base_iter);
            if (!base_iter) return false;
            base = L(base);
        }
        op = L(op);

        /* follow the projections underneath the count operator */
        while (op->kind == la_project) {
            iter = find_old_name (op, iter); if (!iter) return false;
            op = L(op);
        }

        /* ensure that the count operator stems from the same input
           as the order column (and refers to the same iterations) */
        return op == base && iter == base_iter;
    }
    else
        /* recursively call find_last_base() to ignore projections
           AND mapping joins */
        return find_last_base (base, item, base_iter, base_last);
}

/**
 * replace_pos_predicate checks for the following
 * small DAG fragment:
 *
 *        pi_1        This fragment has to fulfill some additional
 *         |          conditions:
 *        sel         o sel.item is not used above
 *         |          o sel.item == =.res (sel)
 *        pi*_2       o =.att1 is not used above
 *         |          o =.att2 is not used above
 *         =          o =.col[1|2] == @.res (optional match)
 *         |          o =.col[1|2] == cast.res
 *        pi*_3       o cast.type == aat_int
 *         |          o cast.res is only used in =
 *         @?         o cast.col == row#.res
 *         |          o row#.res == cast.col
 *        pi*_4       o row#.res is only used in cast
 *         |
 *        cast        Furthermore the operator underneath the rownum (..)
 *         |          forms the input to the new pos_select operator that
 *        pi*_5       replaces pi_1. The correct mapping of names is stored
 *         |          in variable map. If there was no attach operator (@?)
 *        row#        a further check for the positional predicate 'last()'
 *         |          is issued.
 *        (..)
 */
static bool
replace_pos_predicate (PFla_op_t *p)
{
    PFalg_col_t sel, eq1, eq2, eq = col_NULL, cast,
                last = col_NULL, item, part;
    PFla_op_t *op, *base;
    long long int pos = 0;
    unsigned int count = p->schema.count;
    PFalg_proj_t *map = PFmalloc (count * sizeof (PFalg_proj_t));

    /* pi_1 && sel */
    if (p->kind != la_project ||
        L(p)->kind != la_select ||
        PFprop_icol (L(p)->prop, L(p)->sem.select.col))
        return false;

    for (unsigned int i = 0; i < count; i++)
        map[i] = p->sem.proj.items[i];

    sel = L(p)->sem.select.col;
    op = LL(p);

    /* pi*_2 */
    while (op->kind == la_project) {
        for (unsigned int i = 0; i < count; i++) {
            map[i].old = find_old_name (op, map[i].old);
            if (!map[i].old) return false;
        }
        sel  = find_old_name (op, sel); if (!sel) return false;
        op = L(op);
    }

    /* = */
    if (op->kind != la_num_eq ||
        op->sem.binary.res != sel ||
        PFprop_icol (op->prop, op->sem.binary.col1) ||
        PFprop_icol (op->prop, op->sem.binary.col2))
        return false;

    eq1 = op->sem.binary.col1;
    eq2 = op->sem.binary.col2;

    op = L(op);

    /* pi*_3 && @? && pi*_4 */
    while (op->kind == la_project || op->kind == la_attach) {
        if (op->kind == la_attach) {
            if (pos) return false;

            if (op->sem.attach.res == eq1 &&
                op->sem.attach.value.type == aat_int) {
                pos = op->sem.attach.value.val.int_;
                eq  = eq2;
            }
            else if (op->sem.attach.res == eq2 &&
                     op->sem.attach.value.type == aat_int) {
                pos = op->sem.attach.value.val.int_;
                eq  = eq1;
            }

            if (pos <= 0) return false;

            op = L(op);

        } else {
            for (unsigned int i = 0; i < count; i++) {
                map[i].old = find_old_name (op, map[i].old);
                if (!map[i].old) return false;
            }

            if (pos) {
                eq = find_old_name (op, eq);
                if (!eq) return false;
            } else {
                eq1 = find_old_name (op, eq1);
                eq2 = find_old_name (op, eq2);
                if (!eq1 || !eq2) return false;
            }

            op = L(op);
        }
    }

    /* cast */
    if (op->kind != la_cast ||
        op->sem.type.ty != aat_int ||
        (op->sem.type.res != eq1 && op->sem.type.res != eq2) ||
        PFprop_icol (op->prop, op->sem.type.col))
        return false;

    if (!pos) last = op->sem.type.res == eq1 ? eq2 : eq1;
    cast = op->sem.type.col;

    op = L(op);

    /* pi*_5 */
    while (op->kind == la_project) {
        for (unsigned int i = 0; i < count; i++) {
            map[i].old = find_old_name (op, map[i].old);
            if (!map[i].old) return false;
        }

        cast = find_old_name (op, cast); if (!cast) return false;

        if (!pos) {
            last = find_old_name (op, last);
            if (!last) return false;
        }

        op = L(op);
    }

    /* row# */
    if (op->kind != la_rownum ||
        op->sem.sort.res != cast ||
        PFord_count (op->sem.sort.sortby) != 1 ||
        PFord_order_dir_at (op->sem.sort.sortby, 0) != DIR_ASC)
        return false;

    /* check that the result of the rownum operator is not used above */
    for (unsigned int i = 0; i < count; i++)
        if (map[i].old == op->sem.sort.res)
            return false;

    base = L(op);
    part = op->sem.sort.part;
    item = PFord_order_col_at (op->sem.sort.sortby, 0);

    if (!pos) {
        if (!part ||
            !find_last_base (base, item, part, last)) return false;
        pos = -1;
    }

    *p = *PFla_project_ (
              PFla_pos_select (
                  base,
                  pos,
                  PFord_refine (PFordering (), item, DIR_ASC),
                  part),
              count,
              map);
    /* replace pattern here:
    fprintf(stderr,"pos %lli;", pos);
    fprintf(stderr," sort by %s; partition by %s;",
            PFcol_str(item),
            PFcol_str(part));
    for (unsigned int i = 0; i < count; i++)
        fprintf(stderr," map %s to %s%s",
                PFcol_str(map[i].new),
                PFcol_str(map[i].old),
                i == count - 1 ?"\n":";");
    */
    return true;
}

/**
 * Starting from an operator @a p we check if a column @a desc_col
 * provides values in the same order as a column @a origin_col.
 *
 * We can ensure the correct order by following the lineage information
 * of column @a desc_col and checking whether the generated values are
 * in the same order as the values in the input column. If we reach
 * the origin of column @a origin_col and see that column @a desc_col
 * was created by downward path steps starting fom column @a origin_col
 * we can be sure that column @a desc_col describes also the order of
 * column @a origin_col.
 */
static bool
check_order (PFla_op_t *p, PFalg_col_t desc_col, PFalg_col_t origin_col)
{
    PFla_op_t *origin, *desc, *new_desc;

    /* check for node types */
    if (PFprop_type_of (p, desc_col) & ~aat_node ||
        PFprop_type_of (p, origin_col) & ~aat_node)
        return false;

    desc   = PFprop_lineage (p->prop, desc_col);
    origin = PFprop_lineage (p->prop, origin_col);

    /* ensure that we have lineage information */
    if (!origin || !desc)
        return false;

    desc_col   = PFprop_lineage_col (p->prop, desc_col);
    origin_col = PFprop_lineage_col (p->prop, origin_col);

    /* follow the 'assumed' descendant link
       until we reach the origin operator */
    while (desc != origin) {
        /* for downward steps (without overlap) we
           know that the resulting node references
           are in the same order as the mapped context
           nodes */
        if ((desc->kind != la_step_join &&
             desc->kind != la_guide_step_join) ||
            desc_col != desc->sem.step.item_res ||
            !((desc->sem.step.spec.axis == alg_attr ||
               desc->sem.step.spec.axis == alg_chld ||
               desc->sem.step.spec.axis == alg_self ||
               desc->sem.step.spec.axis == alg_desc_s ||
               desc->sem.step.spec.axis == alg_desc) &&
              LEVEL_KNOWN(PFprop_level (desc->prop, desc->sem.step.item))))
            return false;

        /* jump to the operator that provided the step input column */
        desc_col = desc->sem.step.item;
        new_desc = PFprop_lineage (desc->prop, desc_col);
        /* ensure that we still have lineage information */
        if (!new_desc)
            return false;
        /* adjust the column name to the next operator */
        desc_col = PFprop_lineage_col (desc->prop, desc_col);
        desc     = new_desc;
    }

    /* we ensured that (desc == origin) and now
       check that they stem from the same column */
    return desc_col == origin_col;
}

/**
 * For operators with order criteria we try to
 * remove columns from the order list if they are
 * functionally dependent and the correct order is
 * ensured by the next order criterion in the order list.
 */
static bool
shrink_order_list (PFla_op_t *p)
{
    PFord_ordering_t  sortby,
                      res      = PFordering ();
    bool              modified = false;
    PFalg_col_t       cur,
                      next_cur;

    /* allow only operators with order criteria */
    if (p->kind == la_rank ||
        p->kind == la_rowrank ||
        p->kind == la_rownum)
        sortby = p->sem.sort.sortby;
    else if (p->kind == la_pos_select)
        sortby = p->sem.pos_sel.sortby;
    else
        return false;

    /* we have to find a column to prune */
    if (PFord_count (sortby) < 2)
        return false;

    /* compare adjacent order criteria */
    for (unsigned int i = 1; i < PFord_count (sortby); i++) {
        cur      = PFord_order_col_at (sortby, i-1);
        next_cur = PFord_order_col_at (sortby, i);

        /* and skip the former criterion if it is
           functionally dependent from the current
           and the order is the same */
        if (PFord_order_dir_at (sortby, i-1) == DIR_ASC &&
            PFord_order_dir_at (sortby, i) == DIR_ASC &&
            PFprop_fd (p->prop, next_cur, cur) &&
            check_order (p, next_cur, cur)) {
            modified = true;
            continue;
        }
        
        /* Check if the ranking list already describes the new order (as the
           new criterion functionally depends on the last added entry). */
        if (!(PFord_count (res) &&
              PFord_order_dir_at (res, PFord_count (res) - 1) == DIR_ASC &&
              PFord_order_dir_at (sortby, i-1) == DIR_ASC &&
              PFprop_fd (p->prop, 
                         PFord_order_col_at (res, PFord_count (res) - 1),
                         cur)))
            /* otherwise keep former criterion */
            res = PFord_refine (res, cur, PFord_order_dir_at (sortby, i-1));
    }
    /* Check if the ranking list already describes the new order (as the
       last criterion functionally depends on the last added entry). */
    if (PFord_count (res) &&
        PFord_order_dir_at (res, PFord_count (res) - 1) == DIR_ASC &&
        PFord_order_dir_at (sortby, PFord_count (sortby) - 1) == DIR_ASC &&
        PFprop_fd (p->prop, 
                   PFord_order_col_at (res, PFord_count (res) - 1),
                   PFord_order_col_at (sortby, PFord_count (sortby) - 1)))
        modified = true;
    else
        /* add last criterion */
        res = PFord_refine (
                  res,
                  PFord_order_col_at (sortby, PFord_count (sortby) - 1),
                  PFord_order_dir_at (sortby, PFord_count (sortby) - 1));

    /* modify operator in case we have removed one or more order criteria */
    if (modified) {
        if (p->kind == la_pos_select)
            p->sem.pos_sel.sortby = res;
        else
            p->sem.sort.sortby = res;
    }
    return modified;
}

/**
 * Check if two comparison operators work on the same input.
 */
static bool
check_comp_semantics (PFla_op_t *op1, PFla_op_t *op2)
{
    assert (op1->kind == la_num_eq || op1->kind == la_num_gt);
    assert (op2->kind == la_num_eq || op2->kind == la_num_gt);

    if (op1->sem.binary.col1 != op2->sem.binary.col1) {
        if (!PFprop_const (op1->prop, op1->sem.binary.col1) ||
            !PFprop_const (op2->prop, op2->sem.binary.col1) ||
            !PFalg_atom_comparable (
                 PFprop_const_val (op1->prop, op1->sem.binary.col1),
                 PFprop_const_val (op2->prop, op2->sem.binary.col1)) ||
            PFalg_atom_cmp (
                 PFprop_const_val (op1->prop, op1->sem.binary.col1),
                 PFprop_const_val (op2->prop, op2->sem.binary.col1)))
            return false;
        /* else columns match */
    }
    /* else columns match */

    if (op1->sem.binary.col2 != op2->sem.binary.col2) {
        if (!PFprop_const (op1->prop, op1->sem.binary.col2) ||
            !PFprop_const (op2->prop, op2->sem.binary.col2) ||
            !PFalg_atom_comparable (
                 PFprop_const_val (op1->prop, op1->sem.binary.col2),
                 PFprop_const_val (op2->prop, op2->sem.binary.col2)) ||
            PFalg_atom_cmp (
                 PFprop_const_val (op1->prop, op1->sem.binary.col2),
                 PFprop_const_val (op2->prop, op2->sem.binary.col2)))
            return false;
        /* else columns match */
    }
    /* else columns match */

    return true;
}

/* worker for PFalgopt_complex */
static bool
opt_complex (PFla_op_t *p)
{
    bool modified = false;

    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return false;
    else
        SEEN(p) = true;

    PFrecursion_fence();

    /* apply complex optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        modified = opt_complex (p->child[i]) || modified;

    /* action code */
    switch (p->kind) {
        case la_serialize_seq:
            if (PFprop_card (p->prop) == 1 &&
                p->sem.ser_seq.pos != p->sem.ser_seq.item) {
                R(p) = PFla_attach (
                           PFla_project (
                               R(p),
                               PFalg_proj (p->sem.ser_seq.item,
                                           p->sem.ser_seq.item)),
                           p->sem.ser_seq.pos,
                           PFalg_lit_nat (1));

                /* do not mark modified as this will apply
                   the rewrite over and over again */
            }
            break;

        case la_attach:
            /**
             * if an attach column is the only required column
             * and we know its exact cardinality we can replace
             * the complete subtree by a literal table.
             */
            if (PFprop_icols_count (p->prop) == 1 &&
                PFprop_icol (p->prop, p->sem.attach.res) &&
                PFprop_card (p->prop) >= 1) {

                PFla_op_t *res;
                unsigned int count = PFprop_card (p->prop);
                /* create projection list to avoid missing columns */
                PFalg_proj_t *proj = PFmalloc (p->schema.count *
                                               sizeof (PFalg_proj_t));

                /* create list of tuples each containing a list of atoms */
                PFalg_tuple_t *tuples = PFmalloc (count *
                                                  sizeof (*(tuples)));;

                for (unsigned int i = 0; i < count; i++) {
                    tuples[i].atoms = PFmalloc (sizeof (*(tuples[i].atoms)));
                    tuples[i].atoms[0] = p->sem.attach.value;
                    tuples[i].count = 1;
                }

                res = PFla_lit_tbl_ (collist (p->sem.attach.res),
                                     count, tuples);

                /* Every column of the relation will point
                   to the attach argument to avoid missing
                   references. (Columns that are not required
                   may be still referenced by the following
                   operators.) */
                for (unsigned int i = 0; i < p->schema.count; i++)
                    proj[i] = PFalg_proj (p->schema.items[i].name,
                                          p->sem.attach.res);

                *p = *PFla_project_ (res, p->schema.count, proj);
                modified = true;
            }
/* ineffective without step operators */
            /* prune unnecessary attach-project operators */
            if (L(p)->kind == la_project &&
                L(p)->schema.count == 1 &&
                (LL(p)->kind == la_step || LL(p)->kind == la_guide_step) &&
                p->sem.attach.res == LL(p)->sem.step.iter &&
                PFprop_const (LL(p)->prop, LL(p)->sem.step.iter) &&
                PFalg_atom_comparable (
                    p->sem.attach.value,
                    PFprop_const_val (LL(p)->prop, LL(p)->sem.step.iter)) &&
                !PFalg_atom_cmp (
                    p->sem.attach.value,
                    PFprop_const_val (LL(p)->prop, LL(p)->sem.step.iter)) &&
                L(p)->sem.proj.items[0].new == LL(p)->sem.step.item_res) {
                *p = *PFla_dummy (LL(p));
                modified = true;
                break;
            }
            /* prune unnecessary attach-project operators */
            if (L(p)->kind == la_project &&
                L(p)->schema.count == 1 &&
                (LL(p)->kind == la_step || LL(p)->kind == la_guide_step) &&
                PFprop_const (LL(p)->prop, LL(p)->sem.step.iter) &&
                PFalg_atom_comparable (
                    p->sem.attach.value,
                    PFprop_const_val (LL(p)->prop, LL(p)->sem.step.iter)) &&
                !PFalg_atom_cmp (
                    p->sem.attach.value,
                    PFprop_const_val (LL(p)->prop, LL(p)->sem.step.iter)) &&
                L(p)->sem.proj.items[0].old == LL(p)->sem.step.item_res) {
                *p = *PFla_project (PFla_dummy (LL(p)),
                                    PFalg_proj (p->sem.attach.res,
                                                LL(p)->sem.step.iter),
                                    L(p)->sem.proj.items[0]);
                modified = true;
                break;
/* end of: ineffective without step operators */
            }

            break;

        case la_project:
            /* PROJECTION IN PATTERN */
            if (replace_pos_predicate (p))
                modified = true;
            break;

        case la_pos_select:
            /* try to remove order criteria based on functional
               dependencies and lineage information */
            modified |= shrink_order_list (p);
            break;

        case la_eqjoin:
            /**
             * if we have a key join (key property) on a
             * domain-subdomain relationship (domain property)
             * where the columns of the argument marked as 'domain'
             * are not required (icol property) we can skip the join
             * completely.
             */
        {
            /* we can use the schema information of the children
               as no rewrite adds more columns to that subtree. */
            bool left_arg_req = false;
            bool right_arg_req = false;

            /* discard join columns as one of them always remains */
            for (unsigned int i = 0; i < L(p)->schema.count; i++) {
                left_arg_req = left_arg_req ||
                               (!PFprop_subdom (
                                     p->prop,
                                     PFprop_dom_left (
                                         p->prop,
                                         p->sem.eqjoin.col1),
                                     PFprop_dom_left (
                                         p->prop,
                                         L(p)->schema.items[i].name)) &&
                                PFprop_icol (
                                   p->prop,
                                   L(p)->schema.items[i].name));
            }
            if ((PFprop_key_left (p->prop, p->sem.eqjoin.col1) ||
                 /* set ok as rewrite can only result in equal or less rows */
                 PFprop_set (p->prop)) &&
                PFprop_subdom (p->prop,
                               PFprop_dom_right (p->prop,
                                                 p->sem.eqjoin.col2),
                               PFprop_dom_left (p->prop,
                                                p->sem.eqjoin.col1)) &&
                !left_arg_req) {
                /* Every column of the left argument will point
                   to the join argument of the right argument to
                   avoid missing references. (Columns that are not
                   required may be still referenced by the following
                   operators.) */
                PFalg_proj_t *proj = PFmalloc (p->schema.count *
                                               sizeof (PFalg_proj_t));
                unsigned int count = 0;

                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        L(p)->schema.items[i].name,
                                        p->sem.eqjoin.col2);

                for (unsigned int i = 0; i < R(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        R(p)->schema.items[i].name,
                                        R(p)->schema.items[i].name);

                *p = *PFla_project_ (R(p), count, proj);
                modified = true;
                break;
            }

            /* discard join columns as one of them always remains */
            for (unsigned int i = 0; i < R(p)->schema.count; i++) {
                right_arg_req = right_arg_req ||
                                (!PFprop_subdom (
                                      p->prop,
                                      PFprop_dom_right (
                                          p->prop,
                                          p->sem.eqjoin.col2),
                                      PFprop_dom_right (
                                          p->prop,
                                          R(p)->schema.items[i].name)) &&
                                 PFprop_icol (
                                     p->prop,
                                     R(p)->schema.items[i].name));
            }
            if ((PFprop_key_right (p->prop, p->sem.eqjoin.col2) ||
                 /* set ok as rewrite can only result in equal or less rows */
                 PFprop_set (p->prop)) &&
                PFprop_subdom (p->prop,
                               PFprop_dom_left (p->prop,
                                                p->sem.eqjoin.col1),
                               PFprop_dom_right (p->prop,
                                                 p->sem.eqjoin.col2)) &&
                !right_arg_req) {
                /* Every column of the right argument will point
                   to the join argument of the left argument to
                   avoid missing references. (Columns that are not
                   required may be still referenced by the following
                   operators.) */
                PFalg_proj_t *proj = PFmalloc (p->schema.count *
                                               sizeof (PFalg_proj_t));
                unsigned int count = 0;

                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        L(p)->schema.items[i].name,
                                        L(p)->schema.items[i].name);

                for (unsigned int i = 0; i < R(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        R(p)->schema.items[i].name,
                                        p->sem.eqjoin.col1);

                *p = *PFla_project_ (L(p), count, proj);
                modified = true;
                break;
            }

            /* this code makes the recognition of last()
               predicates easier */
            if (PFprop_subdom (p->prop,
                               PFprop_dom_left (p->prop,
                                                p->sem.eqjoin.col1),
                               PFprop_dom_right (p->prop,
                                                 p->sem.eqjoin.col2)) &&
                /* PROJECTION IN PATTERN */
                R(p)->kind == la_project &&
                RL(p)->kind == la_disjunion) {
                PFalg_proj_t *proj     = R(p)->sem.proj.items;
                unsigned int  count    = R(p)->schema.count;
                PFalg_col_t   col1     = p->sem.eqjoin.col1,
                              col2     = p->sem.eqjoin.col2;
                for (unsigned int i = 0; i < count; i++)
                    if (proj[i].new == col2) {
                        col2 = proj[i].old;
                        break;
                    }

                if (PFprop_disjdom (p->prop,
                                    PFprop_dom_left (RL(p)->prop, col2),
                                    PFprop_dom_left (p->prop, col1))) {
                    R(p) = PFla_project_ (RLR(p), count, proj);
                    modified = true;
                }
                else if (PFprop_disjdom (p->prop,
                                         PFprop_dom_right (RL(p)->prop, col2),
                                         PFprop_dom_left (p->prop, col1))) {
                    R(p) = PFla_project_ (RLL(p), count, proj);
                    modified = true;
                }
            }
            if (PFprop_subdom (p->prop,
                               PFprop_dom_right (p->prop,
                                                p->sem.eqjoin.col2),
                               PFprop_dom_left (p->prop,
                                                 p->sem.eqjoin.col1)) &&
                /* PROJECTION IN PATTERN */
                L(p)->kind == la_project &&
                LL(p)->kind == la_disjunion) {
                PFalg_proj_t *proj     = L(p)->sem.proj.items;
                unsigned int  count    = L(p)->schema.count;
                PFalg_col_t   col1     = p->sem.eqjoin.col1,
                              col2     = p->sem.eqjoin.col2;
                for (unsigned int i = 0; i < count; i++)
                    if (proj[i].new == col1) {
                        col1 = proj[i].old;
                        break;
                    }

                if (PFprop_disjdom (p->prop,
                                    PFprop_dom_left (LL(p)->prop, col1),
                                    PFprop_dom_right (p->prop, col2))) {
                    L(p) = PFla_project_ (LLR(p), count, proj);
                    modified = true;
                }
                else if (PFprop_disjdom (p->prop,
                                         PFprop_dom_right (LL(p)->prop, col1),
                                         PFprop_dom_right (p->prop, col2))) {
                    L(p) = PFla_project_ (LLL(p), count, proj);
                    modified = true;
                }
            }
            /* An equi-join on equal columns referencing the same input
               twice can be avoided in case all columns in one join
               argument depend fully on the join column. In this situation
               no new row combinations may be generated and the equi-join
               thus only introduces (superfluous) duplicate rows. */
             /* set ok as rewrite can only result in equal or less rows */
            if ((PFprop_set (p->prop) ||
                 PFprop_key (p->prop, p->sem.eqjoin.col1)) &&
                PFprop_subdom (
                    p->prop,
                    PFprop_dom_left (p->prop,
                                     p->sem.eqjoin.col1),
                    PFprop_dom_right (p->prop,
                                      p->sem.eqjoin.col2)) &&
                PFprop_subdom (
                    p->prop,
                    PFprop_dom_right (p->prop,
                                      p->sem.eqjoin.col2),
                    PFprop_dom_left (p->prop,
                                     p->sem.eqjoin.col1))) {
                unsigned int i,
                             j,
                             k = 0;
                PFla_op_t   *lp = L(p),
                            *rp = R(p),
                            *op;
                bool         fd;

                if (lp->kind == la_project)
                    lp = L(lp);
                if (rp->kind == la_project)
                    rp = L(rp);

                /* check for the same operator */
                while (lp == rp && k <= 1) {
                    op = p->child[k];
                    k++; /* switch from left child to right child */
                    fd = true;

                    /* check for the functional dependency */
                    for (unsigned int i = 0; i < op->schema.count; i++)
                        if (p->sem.eqjoin.col1 != op->schema.items[i].name &&
                            PFprop_icol (p->prop, op->schema.items[i].name))
                            fd &= PFprop_fd (p->prop,
                                             p->sem.eqjoin.col1,
                                             op->schema.items[i].name);
                    if (fd) {
                        /* All columns in one join argument depend on the join
                           column and thus can also be placed in the other
                           argument. */
                        PFalg_proj_t *proj = PFmalloc (p->schema.count *
                                                       sizeof (PFalg_proj_t));
                        /* concatenate the two projection lists */
                        if (L(p)->kind == la_project)
                            for (i = 0; i < L(p)->schema.count; i++)
                                proj[i] = L(p)->sem.proj.items[i];
                        else
                            for (i = 0; i < L(p)->schema.count; i++)
                                proj[i] = PFalg_proj (
                                              L(p)->schema.items[i].name,
                                              L(p)->schema.items[i].name);

                        if (R(p)->kind == la_project)
                            for (j = 0; j < R(p)->schema.count; j++)
                                proj[i+j] = R(p)->sem.proj.items[j];
                        else
                            for (j = 0; j < L(p)->schema.count; j++)
                                proj[i+j] = PFalg_proj (
                                                L(p)->schema.items[j].name,
                                                L(p)->schema.items[j].name);

                        *p = *PFla_project_ (lp, p->schema.count, proj);
                        modified = true;
                        break;
                    }
                }
                if (modified) break;
            }
            if (L(p)->kind == la_distinct &&
                PFprop_ckey (p->prop, p->schema)) {
                *p = *distinct (eqjoin (LL(p),
                                        R(p),
                                        p->sem.eqjoin.col1,
                                        p->sem.eqjoin.col2));
                modified = true;
                break;
            }
            if (R(p)->kind == la_distinct &&
                PFprop_ckey (p->prop, p->schema)) {
                *p = *distinct (eqjoin (L(p),
                                        RL(p),
                                        p->sem.eqjoin.col1,
                                        p->sem.eqjoin.col2));
                modified = true;
                break;
            }

            /* aggregate patterns */
            PFla_op_t *res = aggregate_pattern (p);
            if (res) {
                *p = *res;
                modified = true;
                break;
            }

#if 0 /* disable join -> semijoin rewrites */
            /* introduce semi-join operator if possible */
            if (!left_arg_req &&
                (PFprop_key_left (p->prop, p->sem.eqjoin.col1) ||
                 /* set ok as rewrite can only result in equal or less rows */
                 PFprop_set (p->prop))) {
                /* Every column of the left argument will point
                   to the join argument of the right argument to
                   avoid missing references. (Columns that are not
                   required may be still referenced by the following
                   operators.) */
                PFalg_proj_t *proj = PFmalloc (p->schema.count *
                                               sizeof (PFalg_proj_t));
                unsigned int count = 0;
                PFla_op_t *semijoin;
                PFla_op_t *left = L(p);
                PFla_op_t *right = R(p);
                PFalg_col_t lcol = p->sem.eqjoin.col1;
                PFalg_col_t rcol = p->sem.eqjoin.col2;

                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        L(p)->schema.items[i].name,
                                        p->sem.eqjoin.col2);

                for (unsigned int i = 0; i < R(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        R(p)->schema.items[i].name,
                                        R(p)->schema.items[i].name);

                while (left->kind == la_project) {
                    for (unsigned int i = 0; i < left->sem.proj.count; i++)
                        if (lcol == left->sem.proj.items[i].new) {
                            lcol = left->sem.proj.items[i].old;
                            break;
                        }
                    left = L(left);
                }
                while (right->kind == la_project) {
                    for (unsigned int i = 0; i < right->sem.proj.count; i++)
                        if (rcol == right->sem.proj.items[i].new) {
                            rcol = right->sem.proj.items[i].old;
                            break;
                        }
                    right = L(right);
                }

                if (lcol == rcol && left == right)
                    semijoin = R(p);
                else
                    semijoin = PFla_semijoin (
                                   R(p),
                                   L(p),
                                   p->sem.eqjoin.col2,
                                   p->sem.eqjoin.col1);

                *p = *PFla_project_ (semijoin, count, proj);
                break;
            }

            /* introduce semi-join operator if possible */
            if (!right_arg_req &&
                (PFprop_key_right (p->prop, p->sem.eqjoin.col2) ||
                 /* set ok as rewrite can only result in equal or less rows */
                 PFprop_set (p->prop))) {
                /* Every column of the right argument will point
                   to the join argument of the left argument to
                   avoid missing references. (Columns that are not
                   required may be still referenced by the following
                   operators.) */
                PFalg_proj_t *proj = PFmalloc (p->schema.count *
                                               sizeof (PFalg_proj_t));
                unsigned int count = 0;
                PFla_op_t *semijoin;
                PFla_op_t *left = L(p);
                PFla_op_t *right = R(p);
                PFalg_col_t lcol = p->sem.eqjoin.col1;
                PFalg_col_t rcol = p->sem.eqjoin.col2;

                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        L(p)->schema.items[i].name,
                                        L(p)->schema.items[i].name);

                for (unsigned int i = 0; i < R(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        R(p)->schema.items[i].name,
                                        p->sem.eqjoin.col1);

                while (left->kind == la_project) {
                    for (unsigned int i = 0; i < left->sem.proj.count; i++)
                        if (lcol == left->sem.proj.items[i].new) {
                            lcol = left->sem.proj.items[i].old;
                            break;
                        }
                    left = L(left);
                }
                while (right->kind == la_project) {
                    for (unsigned int i = 0; i < right->sem.proj.count; i++)
                        if (rcol == right->sem.proj.items[i].new) {
                            rcol = right->sem.proj.items[i].old;
                            break;
                        }
                    right = L(right);
                }

                if (lcol == rcol && left == right)
                    semijoin = L(p);
                else
                    semijoin = PFla_semijoin (
                                   L(p),
                                   R(p),
                                   p->sem.eqjoin.col1,
                                   p->sem.eqjoin.col2);

                *p = *PFla_project_ (semijoin, count, proj);
                break;
            }
#endif
        }   break;

        case la_semijoin:
            /* if the semijoin operator does not prune a row
               because the domains are identical and the left
               side does not contain duplicates we can safely
               remove it. */
            if (PFprop_ckey (p->prop, p->schema) &&
                PFprop_subdom (
                    p->prop,
                    PFprop_dom_left (p->prop,
                                     p->sem.eqjoin.col1),
                    PFprop_dom_right (p->prop,
                                      p->sem.eqjoin.col2)) &&
                PFprop_subdom (
                    p->prop,
                    PFprop_dom_right (p->prop,
                                      p->sem.eqjoin.col2),
                    PFprop_dom_left (p->prop,
                                     p->sem.eqjoin.col1))) {
                *p = *dummy (L(p));
                modified = true;
                break;
            }
            if (L(p)->kind == la_difference &&
                (L(p)->schema.count == 1 ||
                 PFprop_key (p->prop, p->sem.eqjoin.col1)) &&
                PFprop_subdom (
                    p->prop,
                    PFprop_dom_right (p->prop,
                                      p->sem.eqjoin.col2),
                    PFprop_dom_right (L(p)->prop,
                                      p->sem.eqjoin.col1))) {
                *p = *PFla_empty_tbl_ (p->schema);
                modified = true;
                break;
            }

            if (!PFprop_key_left (p->prop, p->sem.eqjoin.col1) ||
                !PFprop_subdom (p->prop,
                                PFprop_dom_right (p->prop,
                                                  p->sem.eqjoin.col2),
                                PFprop_dom_left (p->prop,
                                                 p->sem.eqjoin.col1)))
                break;

            /* remove the distinct operator and redirect the
               references to the semijoin operator */
            if (R(p)->kind == la_distinct) {
                PFla_op_t *distinct = R(p);
                R(p) = L(distinct);
                *distinct = *PFla_project (p, PFalg_proj (p->sem.eqjoin.col2,
                                                          p->sem.eqjoin.col1));
            }
            else if (R(p)->kind == la_project &&
                     RL(p)->kind == la_distinct &&
                     R(p)->schema.count == 1 &&
                     RL(p)->schema.count == 1) {
                PFla_op_t *project = R(p),
                          *distinct = RL(p);
                R(p) = L(distinct);
                *project = *PFla_project (
                                p,
                                PFalg_proj (p->sem.eqjoin.col2,
                                            p->sem.eqjoin.col1));
                *distinct = *PFla_project (
                                 p,
                                 PFalg_proj (distinct->schema.items[0].name,
                                             p->sem.eqjoin.col1));

                /* we need to adjust the semijoin argument as well */
                p->sem.eqjoin.col2 = R(p)->schema.items[0].name;
            }
            break;

        case la_thetajoin:
            /* rewrite thetajoin into equi-join if it seems to be a mapping join */
            if (p->sem.thetajoin.count == 1 &&
                p->sem.thetajoin.pred[0].comp == alg_comp_eq &&
                ((PFprop_key_left (p->prop, p->sem.thetajoin.pred[0].left) &&
                  PFprop_subdom (p->prop,
                                 PFprop_dom_right (p->prop,
                                                   p->sem.thetajoin.pred[0].right),
                                 PFprop_dom_left (p->prop,
                                                  p->sem.thetajoin.pred[0].left))) ||
                 (PFprop_key_right (p->prop, p->sem.thetajoin.pred[0].right) &&
                  PFprop_subdom (p->prop,
                                 PFprop_dom_left (p->prop,
                                                   p->sem.thetajoin.pred[0].left),
                                 PFprop_dom_right (p->prop,
                                                  p->sem.thetajoin.pred[0].right))))) {
                *p = *PFla_eqjoin (L(p), R(p),
                                   p->sem.thetajoin.pred[0].left,
                                   p->sem.thetajoin.pred[0].right);
                modified = true;
            }
            /* unfold a thetajoin with a one-row literal table */
            else if (PFprop_card (L(p)->prop) == 1 &&
                     (L(p)->kind == la_lit_tbl ||
                      (L(p)->kind == la_project &&
                       LL(p)->kind == la_lit_tbl))) {
                *p = *replace_thetajoin (p, true);
                modified = true;
            }
            /* unfold a thetajoin with a one-row literal table */
            else if (PFprop_card (R(p)->prop) == 1 &&
                     (R(p)->kind == la_lit_tbl ||
                      (R(p)->kind == la_project &&
                       RL(p)->kind == la_lit_tbl))) {
                *p = *replace_thetajoin (p, false);
                modified = true;
            }
            break;

        case la_cross:
            /* PFprop_icols_count () == 0 is also true
               for nodes without inferred properties
               (newly created nodes). The cardinality
               constraint however ensures that the
               properties are available. */
            if (PFprop_card (L(p)->prop) == 1 &&
                PFprop_icols_count (L(p)->prop) == 0) {
                *p = *PFla_dummy (R(p));
                modified = true;
                break;
            }
            if (PFprop_card (R(p)->prop) == 1 &&
                PFprop_icols_count (R(p)->prop) == 0) {
                *p = *PFla_dummy (L(p));
                modified = true;
                break;
            }
            break;

        /* Remove unnecessary distinct operators */
        case la_distinct:
            if (PFprop_ckey (L(p)->prop, p->schema)) {
                *p = *PFla_dummy (L(p));
                modified = true;
                break;
            }
            /* optimization based on functional dependencies */
            if (PFprop_icols_count (p->prop) != p->schema.count) {
                PFalg_proj_t *proj1 = PFmalloc (p->schema.count *
                                                sizeof (PFalg_proj_t));
                PFalg_proj_t *proj2 = PFmalloc (p->schema.count *
                                                sizeof (PFalg_proj_t));
                unsigned int  count = 0,
                              i,
                              j;
                for (i = 0; i < p->schema.count; i++) {
                    proj2[i] = PFalg_proj (p->schema.items[i].name,
                                           p->schema.items[i].name);
                    if (PFprop_not_icol (p->prop, p->schema.items[i].name)) {
                        for (j = 0; j < p->schema.count; j++)
                            if (i != j &&
                                PFprop_icol (p->prop, p->schema.items[j].name) &&
                                PFprop_fd (p->prop,
                                           p->schema.items[j].name,
                                           p->schema.items[i].name)) {
                                /* replace dependent column by icols column */
                                proj2[i].old = p->schema.items[j].name;
                                /* throw away column */
                                break;
                            }
                        if (j == p->schema.count)
                            /* keep column */
                            proj1[count++] = proj2[i];
                    }
                    else
                        /* keep column */
                        proj1[count++] = proj2[i];
                }
                /* If a column was thrown away we prune the schema
                   but re-align it above the distinct operator again.
                   (Dependent columns are replaced by their describing part.) */
                if (count < p->schema.count) {
                    *p = *PFla_project_ (
                              distinct (PFla_project_ (L(p), count, proj1)),
                              p->schema.count,
                              proj2);
                    modified = true;
                }
                break;
            }
            break;

        case la_select:
            /* check for the alignment of columns produced
               by the zip operator in the ferryc compiler */
            if (zip_alignment (p)) {
                modified = true;
                break;
            }
        /**
         * Rewrite the pattern (1) into expression (2):
         *
         *          select_(col1) [icols:col2]          pi_(col2,...:col2)
         *            |                                  |
         *         ( pi_(col1,col2) )                 distinct
         *            |                                  |
         *           or_(col1:col3,col4)               union
         *            |                              ____/\____
         *         ( pi_(col2,col3,col4) )          /          \
         *            |                            pi_(col2)   pi_(col2:col5)
         *           |X|_(col2,col5)              /              \
         *        __/   \__                    select_(col3)   select_(col4)
         *       /         \                     |                |
         *      /1\       /2\                   /1\              /2\
         *     /___\     /___\                 /___\            /___\
         * (col2,col3) (col5,col4)            (col2,col3)      (col5,col4)
         *
         *           (1)                                 (2)
         */
        {
            unsigned int i;
            PFalg_col_t col_sel,
                        col_join1, col_join2,
                        col_sel_in1, col_sel_in2;
            PFla_op_t *cur, *left, *right;
            PFalg_proj_t *lproj, *rproj, *top_proj;

            if (p->schema.count != 2 ||
                PFprop_icols_count (p->prop) != 1 ||
                PFprop_icol (p->prop, p->sem.select.col))
                break;

            col_sel = p->sem.select.col;
            col_join1 = p->schema.items[0].name != col_sel
                        ? p->schema.items[0].name
                        : p->schema.items[1].name;
            cur = L(p);

            /* cope with intermediate projections */
            if (cur->kind == la_project) {
                for (i = 0; i < cur->sem.proj.count; i++)
                    if (L(p)->sem.proj.items[i].new == col_sel)
                        col_sel = L(p)->sem.proj.items[i].old;
                    else if (L(p)->sem.proj.items[i].new == col_join1)
                        col_join1 = L(p)->sem.proj.items[i].old;
                cur = L(cur);
            }

            if (cur->kind != la_bool_or ||
                col_sel != cur->sem.binary.res)
                break;

            col_sel_in1 = cur->sem.binary.col1;
            col_sel_in2 = cur->sem.binary.col2;

            cur = L(cur);

            /* cope with intermediate projections */
            if (cur->kind == la_project) {
                for (i = 0; i < cur->sem.proj.count; i++)
                    if (L(p)->sem.proj.items[i].new == col_join1)
                        col_join1 = L(p)->sem.proj.items[i].old;
                    else if (L(p)->sem.proj.items[i].new == col_sel_in1)
                        col_sel_in1 = L(p)->sem.proj.items[i].old;
                    else if (L(p)->sem.proj.items[i].new == col_sel_in2)
                        col_sel_in2 = L(p)->sem.proj.items[i].old;
                cur = L(cur);
            }

            if (cur->kind != la_eqjoin ||
                (col_join1 != cur->sem.eqjoin.col1 &&
                 col_join1 != cur->sem.eqjoin.col2) ||
                /* Make sure that both join arguments are key as
                   otherwise placing the distinct operator above
                   the plan fragment is incorrect. */
                !PFprop_key_left (cur->prop, cur->sem.eqjoin.col1) ||
                !PFprop_key_right (cur->prop, cur->sem.eqjoin.col2))
                break;

            if (PFprop_ocol (L(cur), col_sel_in1) &&
                PFprop_ocol (R(cur), col_sel_in2)) {
                col_join1 = cur->sem.eqjoin.col1;
                col_join2 = cur->sem.eqjoin.col2;
                left = L(cur);
                right = R(cur);
            }
            else if (PFprop_ocol (L(cur), col_sel_in2) &&
                    PFprop_ocol (R(cur), col_sel_in1)) {
                col_join1 = cur->sem.eqjoin.col2;
                col_join2 = cur->sem.eqjoin.col1;
                left = R(cur);
                right = L(cur);
            }
            else
                break;

            /* pattern (1) is now ensured: create pattern (2) */
            lproj = PFmalloc (sizeof (PFalg_proj_t));
            rproj = PFmalloc (sizeof (PFalg_proj_t));
            top_proj = PFmalloc (2 * sizeof (PFalg_proj_t));

            lproj[0] = PFalg_proj (col_join1, col_join1);
            rproj[0] = PFalg_proj (col_join1, col_join2);
            top_proj[0] = PFalg_proj (p->schema.items[0].name, col_join1);
            top_proj[1] = PFalg_proj (p->schema.items[1].name, col_join1);

            *p = *PFla_project_ (
                      PFla_distinct (
                          PFla_disjunion (
                              PFla_project_ (
                                  PFla_select (left, col_sel_in1),
                                  1, lproj),
                              PFla_project_ (
                                  PFla_select (right, col_sel_in2),
                                  1, rproj))),
                      2, top_proj);
            modified = true;
        }   break;

        case la_difference:
        {   /**
             * If the domains of the first relation are all subdomains
             * of the corresponding domains in the second argument
             * the result of the difference operation will be empty.
             */
            unsigned int all_subdom = 0;
            for (unsigned int i = 0; i < L(p)->schema.count; i++)
                for (unsigned int j = 0; j < R(p)->schema.count; j++)
                    if (L(p)->schema.items[i].name ==
                        R(p)->schema.items[j].name &&
                        PFprop_subdom (
                            p->prop,
                            PFprop_dom_left (p->prop,
                                             L(p)->schema.items[i].name),
                            PFprop_dom_right (p->prop,
                                              R(p)->schema.items[j].name))) {
                        all_subdom++;
                        break;
                    }

            if (all_subdom == p->schema.count &&
                /* we have to make sure that the left side
                   does not contain duplicates */
                PFprop_ckey (p->prop, p->schema)) {
                *p = *PFla_empty_tbl_ (p->schema);
                modified = true;
                SEEN(p) = true;
                break;
            }
        }   break;

        case la_num_eq:
        case la_num_gt:
            /* Merge adjacent identical comparison operators.
               (These duplicate operators are a side-product
                of pushing down equi-joins.) */
            if (L(p)->kind == p->kind &&
                check_comp_semantics (p, L(p))) {
                PFalg_proj_t *proj = PFalg_proj_create (p->schema);
                /* replace result column by the result column
                   of the lower comparison operator */
                for (unsigned int i = 0; i < p->schema.count; i++)
                    if (proj[i].new == p->sem.binary.res) {
                        proj[i].old = L(p)->sem.binary.res;
                        break;
                    }
                *p = *PFla_project_ (L(p), p->schema.count, proj);
            }
            else if ((L(p)->kind == la_select ||
                      L(p)->kind == la_attach) &&
                     LL(p)->kind == p->kind &&
                     check_comp_semantics (p, LL(p))) {
                PFalg_proj_t *proj = PFalg_proj_create (p->schema);
                /* replace result column by the result column
                   of the lower comparison operator */
                for (unsigned int i = 0; i < p->schema.count; i++)
                    if (proj[i].new == p->sem.binary.res) {
                        proj[i].old = LL(p)->sem.binary.res;
                        break;
                    }
                *p = *PFla_project_ (L(p), p->schema.count, proj);
            }
            break;

        /* to get rid of the operator 'and' and to split up
           different conditions we can introduce additional
           select operators above comparisons whose required
           value is true. */
        case la_bool_and:
            if (PFprop_req_bool_val (p->prop, p->sem.binary.res) &&
                PFprop_req_bool_val_val (p->prop, p->sem.binary.res)) {
                *p = *PFla_attach (
                          PFla_select (
                              PFla_select (
                                  L(p),
                                  p->sem.binary.col1),
                              p->sem.binary.col2),
                          p->sem.binary.res,
                          PFalg_lit_bln (true));
                modified = true;
            }
            break;

        case la_aggr:
            /* In case the aggregated value is the same for all rows in a group
               we can replace the aggregate by a duplicate elimination. */
            if (p->sem.aggr.part)
                for (unsigned int i = 0; i < p->sem.aggr.count; i++)
                    if (p->sem.aggr.aggr[i].col &&
                        PFprop_fd (L(p)->prop,
                                   p->sem.aggr.part,
                                   p->sem.aggr.aggr[i].col) &&
                        (p->sem.aggr.aggr[i].kind == alg_aggr_min ||
                         p->sem.aggr.aggr[i].kind == alg_aggr_max ||
                         p->sem.aggr.aggr[i].kind == alg_aggr_avg ||
                         p->sem.aggr.aggr[i].kind == alg_aggr_all))
                        p->sem.aggr.aggr[i].kind = alg_aggr_dist;
            /* A further rewrite in opt_general.brg will introduce distinct
               operators if no other aggregates exist. */
            break;

        case la_rownum:
            /* try to remove order criteria based on functional
               dependencies and lineage information */
            modified |= shrink_order_list (p);

            /* Replace the rownumber operator by a rowrank operator
               if it is only used to provide the correct link to
               the outer relation in a ferry setting. */
            if (PFprop_req_link_col (p->prop, p->sem.sort.res)) {
                PFalg_schema_t schema;
                schema.count = PFord_count (p->sem.sort.sortby);
                schema.items = PFmalloc (schema.count *
                                         sizeof (PFalg_schema_t));
                for (unsigned int i = 0; i < schema.count; i++)
                    schema.items[i].name =
                        PFord_order_col_at (p->sem.sort.sortby, i);
                if (PFprop_ckey (p->prop, schema)) {
                    *p = *PFla_rowrank (
                              L(p),
                              p->sem.sort.res,
                              p->sem.sort.sortby);
                   modified = true;
                   break;
                }
            }

            /* Replace the rownumber operator by a projection
               if only its value distribution (keys) are required
               instead of its real values. */
            if (!PFprop_req_value_col (p->prop, p->sem.sort.res) &&
		!p->sem.sort.part &&
                PFord_count (p->sem.sort.sortby) == 1 &&
                PFprop_key (p->prop,
                            PFord_order_col_at (p->sem.sort.sortby, 0))) {
                /* create projection list */
                PFalg_proj_t *proj_list = PFmalloc ((L(p)->schema.count + 1)
                                                    * sizeof (*(proj_list)));

                /* We cannot rewrite if we require the correct order
                   and the rownum operator changes it from descending
                   to ascending. */
                if (PFord_order_dir_at (p->sem.sort.sortby, 0) == DIR_DESC &&
                    PFprop_req_order_col (p->prop, p->sem.sort.res))
                    break;

                /* copy the child schema (as we cannot be sure that
                   the schema of the rownum operator is still valid) ...*/
                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    proj_list[i] = PFalg_proj (
                                       L(p)->schema.items[i].name,
                                       L(p)->schema.items[i].name);

                /* ... and extend it with the sort
                   criterion as new rownum column */
                proj_list[L(p)->schema.count] = PFalg_proj (
                                                    p->sem.sort.res,
                                                    PFord_order_col_at (
                                                        p->sem.sort.sortby, 0));

                *p = *PFla_project_ (L(p), L(p)->schema.count + 1, proj_list);
                modified = true;
            }
            break;

        case la_rowrank:
            /* try to remove order criteria based on functional
               dependencies and lineage information */
            modified |= shrink_order_list (p);
            break;

        case la_rank:
            /* try to remove order criteria based on functional
               dependencies and lineage information */
            modified |= shrink_order_list (p);

            /* first of all try to replace a rank with a single
               ascending order criterion by a projection match
               match the pattern rank - (project -) rank and
               try to merge both rank operators if the nested
               one only prepares some columns for the outer rank.
                 As most operators are separated by a projection
               we also support projections that do not rename. */
        {
            PFla_op_t *rank;
            bool proj = false, renamed = false;
            unsigned int i;

            /* Replace a rank operator with a single ascending order
               criterion by a projection that links the output column
               to the order criterion. */
            if (PFord_count (p->sem.sort.sortby) == 1 &&
                PFord_order_dir_at (p->sem.sort.sortby, 0) == DIR_ASC) {
                PFalg_proj_t *proj_list;
                unsigned int count = 0;

                /* create projection list */
                proj_list = PFmalloc (p->schema.count *
                                      sizeof (*(proj_list)));

                /* adjust column name of the rank operator */
                proj_list[count++] = PFalg_proj (
                                         p->sem.sort.res,
                                         PFord_order_col_at (
                                             p->sem.sort.sortby,
                                             0));

                for (unsigned int i = 0; i < p->schema.count; i++)
                    if (p->schema.items[i].name != p->sem.sort.res)
                        proj_list[count++] = PFalg_proj (
                                                 p->schema.items[i].name,
                                                 p->schema.items[i].name);

                *p = *PFla_project_ (L(p), count, proj_list);
                modified = true;
                SEEN(p) = false;
                break;
            }

            /* check for a projection */
            if (L(p)->kind == la_project) {
                proj = true;
                for (i = 0; i < L(p)->sem.proj.count; i++)
                    renamed = renamed || (L(p)->sem.proj.items[i].new !=
                                          L(p)->sem.proj.items[i].old);
                rank = LL(p);
            }
            else
                rank = L(p);

            /* don't handle patterns with renaming projections */
            if (renamed) break;

            /* check the remaining part of the pattern (nested rank)
               and ensure that the column generated by the nested
               row number operator is not used above the outer rank. */
            if (rank->kind == la_rank &&
                PFprop_not_icol (p->prop, rank->sem.sort.res)) {

                PFord_ordering_t sortby;
                PFalg_proj_t *proj_list;
                PFalg_col_t inner_col = rank->sem.sort.res;
                unsigned int pos_col = 0, count = 0;

                /* lookup position of the inner rank column in
                   the list of sort criteria of the outer rank */
                for (i = 0; i < PFord_count (p->sem.sort.sortby); i++)
                    if (PFord_order_col_at (p->sem.sort.sortby, i)
                            == inner_col &&
                        /* make sure the order is the same */
                        PFord_order_dir_at (p->sem.sort.sortby, i)
                            == DIR_ASC) {
                        pos_col = i;
                        break;
                    }

                /* inner rank column is not used in the outer rank
                   (thus the inner rank is probably superfluous
                    -- let the icols optimization remove the operator) */
                if (i == PFord_count (p->sem.sort.sortby)) break;

                sortby = PFordering ();

                /* create new sort list where the sort criteria of the
                   inner rank substitute the inner rank column */
                for (i = 0; i < pos_col; i++)
                    sortby = PFord_refine (sortby,
                                           PFord_order_col_at (
                                               p->sem.sort.sortby,
                                               i),
                                           PFord_order_dir_at (
                                               p->sem.sort.sortby,
                                               i));

                for (i = 0; i < PFord_count (rank->sem.sort.sortby); i++)
                    sortby = PFord_refine (sortby,
                                           PFord_order_col_at (
                                               rank->sem.sort.sortby,
                                               i),
                                           PFord_order_dir_at (
                                               rank->sem.sort.sortby,
                                               i));

                for (i = pos_col + 1;
                     i < PFord_count (p->sem.sort.sortby);
                     i++)
                    sortby = PFord_refine (sortby,
                                           PFord_order_col_at (
                                               p->sem.sort.sortby,
                                               i),
                                           PFord_order_dir_at (
                                               p->sem.sort.sortby,
                                               i));

                if (proj) {
                    /* Introduce the projection above the new rank
                       operator to maintain the correct result schema.
                       As the result column name of the old outer rank
                       may collide with the column name of one of the
                       inner ranks sort criteria, we use the column name
                       of the inner rank as resulting column name
                       and adjust the name in the new projection. */

                    count = 0;

                    /* create projection list */
                    proj_list = PFmalloc (p->schema.count *
                                          sizeof (*(proj_list)));

                    /* adjust column name of the rank operator */
                    proj_list[count++] = PFalg_proj (
                                             p->sem.sort.res,
                                             rank->sem.sort.res);

                    for (i = 0; i < p->schema.count; i++)
                        if (p->schema.items[i].name !=
                            p->sem.sort.res)
                            proj_list[count++] = PFalg_proj (
                                                     p->schema.items[i].name,
                                                     p->schema.items[i].name);

                    *p = *PFla_project_ (PFla_rank (L(rank),
                                                    rank->sem.sort.res,
                                                    sortby),
                                         count, proj_list);
                }
                else
                    *p = *PFla_rank (rank, p->sem.sort.res, sortby);

                modified = true;
                break;
            }

        }   break;

        case la_rowid:
            if (PFprop_card (p->prop) == 1) {
                *p = *PFla_attach (L(p), p->sem.rowid.res, PFalg_lit_nat (1));
                modified = true;
            }
            /* Get rid of a rowid operator that is only used to maintain
               the correct cardinality or the correct order (in an unordered
               scenario). In case other columns provide a compound key we
               replace the rowid operator by a rank operator consuming the
               compound key. */
            else if (PFprop_req_unique_col (p->prop, p->sem.rowid.res) ||
                     PFprop_req_order_col (p->prop, p->sem.rowid.res)) {
                PFalg_collist_t *collist;
                unsigned int     count = PFprop_ckeys_count (p->prop),
                                 i,
                                 j;
                for (i = 0; i < count; i++) {
                    collist = PFprop_ckey_at (p->prop, i);
                    /* make sure the result is not part of the compound key
                       and all columns of the new key are already used
                       elsewhere. */
                    for (j = 0; j < clsize (collist); j++)
                        if (clat (collist, j) == p->sem.rowid.res ||
                            PFprop_not_icol (p->prop, clat (collist, j)))
                            break;
                    if (j == clsize (collist)) {
                        PFord_ordering_t sortby = PFordering ();
                        for (j = 0; j < clsize (collist); j++)
                            sortby = PFord_refine (sortby,
                                                   clat (collist, j),
                                                   DIR_ASC);
                        *p = *PFla_rank (L(p), p->sem.rowid.res, sortby);
                        modified = true;
                        break;
                    }
                }
                if (L(p)->kind == la_cross) {
                    PFalg_col_t new_col1 = PFcol_new (p->sem.rowid.res),
                                new_col2 = PFcol_new (p->sem.rowid.res);

                    *p = *PFla_rank (
                              cross (
                                  rowid (LL(p), new_col1),
                                  rowid (LR(p), new_col2)),
                              p->sem.rowid.res,
                              sortby (new_col1, new_col2));
                    modified = true;
                    break;
                }
                /* Push a rowid operator underneath the adjacent
                   projection thus hoping to find a composite key
                   that allows to replace the rowid operator by
                   a rank operator. */
                else if (L(p)->kind == la_project) {
                    PFalg_proj_t *proj = PFmalloc (p->schema.count *
                                                   sizeof (PFalg_proj_t));
                    PFalg_col_t   col  = PFcol_new (p->sem.rowid.res);
                    for (unsigned int i = 0; i < L(p)->sem.proj.count; i++)
                        proj[i] = L(p)->sem.proj.items[i];
                    proj[L(p)->schema.count] = PFalg_proj (p->sem.rowid.res,
                                                           col);
                    *p = *PFla_project_ (rowid (LL(p), col), p->schema.count, proj);
                    modified = true;
                }
            }
            break;

        case la_cast:
            if (PFprop_req_order_col (p->prop, p->sem.type.res) &&
                p->sem.type.ty == aat_int &&
                PFprop_type_of (p, p->sem.type.col) == aat_nat) {
                PFalg_proj_t *proj = PFmalloc (p->schema.count *
                                               sizeof (PFalg_proj_t));
                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    proj[i] = PFalg_proj (L(p)->schema.items[i].name,
                                          L(p)->schema.items[i].name);
                proj[L(p)->schema.count] = PFalg_proj (p->sem.type.res,
                                                       p->sem.type.col);
                *p = *PFla_project_ (L(p), p->schema.count, proj);
                modified = true;
            }
            break;

/* ineffective without step operators */
        case la_step:
            if (!LEVEL_KNOWN(p->sem.step.level))
                p->sem.step.level = PFprop_level (p->prop,
                                                  p->sem.step.item_res);

            if ((p->sem.step.spec.axis == alg_desc ||
                 p->sem.step.spec.axis == alg_desc_s) &&
                p->sem.step.level >= 1 &&
                p->sem.step.level - 1 == PFprop_level (R(p)->prop,
                                                       p->sem.step.item))
                p->sem.step.spec.axis = alg_chld;

            if (R(p)->kind == la_project &&
                RL(p)->kind == la_step) {
                if ((p->sem.step.item ==
                     R(p)->sem.proj.items[0].new &&
                     RL(p)->sem.step.item_res ==
                     R(p)->sem.proj.items[0].old &&
                     p->sem.step.iter ==
                     R(p)->sem.proj.items[1].new &&
                     RL(p)->sem.step.iter ==
                     R(p)->sem.proj.items[1].old) ||
                    (p->sem.step.item ==
                     R(p)->sem.proj.items[1].new &&
                     RL(p)->sem.step.item_res ==
                     R(p)->sem.proj.items[1].old &&
                     p->sem.step.iter ==
                     R(p)->sem.proj.items[0].new &&
                     RL(p)->sem.step.iter ==
                     R(p)->sem.proj.items[0].old)) {
                    *p = *PFla_project (PFla_step (
                                            L(p),
                                            RL(p),
                                            p->sem.step.spec,
                                            p->sem.step.level,
                                            RL(p)->sem.step.iter,
                                            RL(p)->sem.step.item,
                                            RL(p)->sem.step.item_res),
                                        R(p)->sem.proj.items[0],
                                        R(p)->sem.proj.items[1]);
                    modified = true;
                }
                break;
            }
            else if (R(p)->kind == la_rowid &&
                     p->sem.step.spec.axis == alg_chld &&
                     p->sem.step.iter == R(p)->sem.rowid.res &&
                     PFprop_not_icol (p->prop, p->sem.step.iter) &&
                     PFprop_key (p->prop, p->sem.step.item)) {
                R(p) = PFla_attach (RL(p),
                                    R(p)->sem.rowid.res,
                                    PFalg_lit_nat (1));
                modified = true;
                break;
            }
            break;
/* end of: ineffective without step operators */

        case la_guide_step:
        case la_guide_step_join:
            if (!LEVEL_KNOWN(p->sem.step.level) && p->sem.step.guide_count) {
                int level = p->sem.step.guides[0]->level;
                for (unsigned int i = 1; i < p->sem.step.guide_count; i++)
                    if (level != p->sem.step.guides[i]->level)
                        break;
                p->sem.step.level = level;
                modified = true;
            }

            if ((p->sem.step.spec.axis == alg_desc ||
                 p->sem.step.spec.axis == alg_desc_s) &&
                p->sem.step.level >= 1 &&
                p->sem.step.level - 1 == PFprop_level (R(p)->prop,
                                                       p->sem.step.item)) {
                p->sem.step.spec.axis = alg_chld;
                modified = true;
            }
            break;

        case la_step_join:
            if (!LEVEL_KNOWN(p->sem.step.level))
                p->sem.step.level = PFprop_level (p->prop,
                                                  p->sem.step.item_res);

            if ((p->sem.step.spec.axis == alg_desc ||
                 p->sem.step.spec.axis == alg_desc_s) &&
                p->sem.step.level >= 1 &&
                p->sem.step.level - 1 == PFprop_level (R(p)->prop,
                                                       p->sem.step.item)) {
                p->sem.step.spec.axis = alg_chld;
                modified = true;
            }

            /* combine steps if they are of the form:
               ``/descandent-or-self::node()/child::element()'' */
                /* PROJECTION IN PATTERN */
            if (R(p)->kind == la_project &&
                RL(p)->kind == la_step_join &&
                /* check for the different step combinations */
                ((/* descendant-or-self::node()/child:: */
                  p->sem.step.spec.axis == alg_chld &&
                  RL(p)->sem.step.spec.axis == alg_desc_s) ||
                 (/* child::node()/descendant-or-self:: */
                  p->sem.step.spec.axis == alg_desc_s &&
                  RL(p)->sem.step.spec.axis == alg_chld) ||
                 (/* coll_node/descendant::node()/child:: */
                  /* Works correctly as the collection node is not
                     reachable in the query and thus descendant behaves
                     like a descendant-or-self step starting from a
                     document node. */
                  p->sem.step.spec.axis == alg_chld &&
                  PFprop_level (RL(p)->prop, RL(p)->sem.step.item) == -1 &&
                  RL(p)->sem.step.spec.axis == alg_desc &&
                  /* check for node kind to avoid that this pattern
                     is triggered multiple times */
                  p->sem.step.spec.kind != node_kind_node) ||
                 (/* coll_node/child::node()/descendant:: */
                  /* Works correctly as the descendant step filter
                     discards the document nodes. */
                  p->sem.step.spec.axis == alg_desc &&
                  PFprop_level (RL(p)->prop, RL(p)->sem.step.item) == -1 &&
                  RL(p)->sem.step.spec.axis == alg_chld &&
                  p->sem.step.spec.kind != node_kind_node &&
                  p->sem.step.spec.kind != node_kind_doc)) &&
                RL(p)->sem.step.spec.kind == node_kind_node &&
                PFprop_not_icol (p->prop, p->sem.step.item) &&
                 /* set ok as rewrite can only result in equal or less rows */
                (PFprop_set (p->prop) ||
                 PFprop_ckey (p->prop, p->schema) ||
                 PFprop_key (p->prop, p->sem.step.item_res))) {

                bool          item_link_correct = false;
                PFalg_col_t   item_res          = p->sem.step.item_res,
                              item_in           = p->sem.step.item,
                              old_item_res      = RL(p)->sem.step.item_res,
                              old_item_in       = RL(p)->sem.step.item;
                PFalg_proj_t *proj = PFmalloc (R(p)->schema.count *
                                               sizeof (PFalg_proj_t));

                for (unsigned int i = 0; i < R(p)->sem.proj.count; i++) {
                    PFalg_proj_t proj_item = R(p)->sem.proj.items[i];

                    if (proj_item.new == item_in &&
                        proj_item.old == old_item_res) {
                        item_link_correct = true;
                        proj[i] = PFalg_proj (item_in, old_item_in);
                    }
                    else if (proj_item.old == old_item_in)
                        /* the old input item may not appear in the result */
                        break;
                    else
                        proj[i] = proj_item;
                }
                if (item_link_correct) {
                    PFalg_step_spec_t spec = p->sem.step.spec;
                    spec.axis = alg_desc;
                    /* rewrite child into descendant
                       and discard descendant-or-self step */
                    *p = *PFla_step_join_simple (
                              L(p),
                              PFla_project_ (RLR(p), R(p)->schema.count, proj),
                              spec, item_in, item_res);
                    modified = true;
                    break;
                }
            }

            /* combine steps if they are of the form:
               ``/descandent-or-self::node()/child::element()'' */
            if (R(p)->kind == la_step_join &&
                p->sem.step.item == R(p)->sem.step.item_res &&
                /* check for the different step combinations */
                ((/* descendant-or-self::node()/child:: */
                  p->sem.step.spec.axis == alg_chld &&
                  R(p)->sem.step.spec.axis == alg_desc_s) ||
                 (/* child::node()/descendant-or-self:: */
                  p->sem.step.spec.axis == alg_desc_s &&
                  R(p)->sem.step.spec.axis == alg_chld) ||
                 (/* coll_node/descendant::node()/child:: */
                  /* Works correctly as the collection node is not
                     reachable in the query and thus descendant behaves
                     like a descendant-or-self step starting from a
                     document node. */
                  p->sem.step.spec.axis == alg_chld &&
                  PFprop_level (R(p)->prop, R(p)->sem.step.item) == -1 &&
                  R(p)->sem.step.spec.axis == alg_desc &&
                  /* check for node kind to avoid that this pattern
                     is triggered multiple times */
                  p->sem.step.spec.kind != node_kind_node) ||
                 (/* coll_node/child::node()/descendant:: */
                  /* Works correctly as the descendant step filter
                     discards the document nodes. */
                  p->sem.step.spec.axis == alg_desc &&
                  PFprop_level (R(p)->prop, R(p)->sem.step.item) == -1 &&
                  R(p)->sem.step.spec.axis == alg_chld &&
                  p->sem.step.spec.kind != node_kind_node &&
                  p->sem.step.spec.kind != node_kind_doc)) &&
                R(p)->sem.step.spec.kind == node_kind_node &&
                PFprop_not_icol (p->prop, p->sem.step.item) &&
                 /* set ok as rewrite can only result in equal or less rows */
                (PFprop_set (p->prop) ||
                 PFprop_ckey (p->prop, p->schema) ||
                 PFprop_key (p->prop, p->sem.step.item_res))) {

                PFalg_proj_t *proj = PFmalloc (R(p)->schema.count *
                                               sizeof (PFalg_proj_t));

                for (unsigned int i = 0; i < R(p)->schema.count; i++) {
                    proj[i] = PFalg_proj (R(p)->schema.items[i].name,
                                          R(p)->schema.items[i].name);

                    if (proj[i].new == R(p)->sem.step.item_res)
                        proj[i].old = R(p)->sem.step.item;
                }

                PFalg_step_spec_t spec = p->sem.step.spec;
                spec.axis = alg_desc;
                /* rewrite child into descendant
                   and discard descendant-or-self step */
                *p = *PFla_step_join_simple (
                          L(p),
                          PFla_project_ (RR(p), R(p)->schema.count, proj),
                          spec,
                          R(p)->sem.step.item,
                          p->sem.step.item_res);
                modified = true;
                break;
            }
            {   /* Try to link the first step directly to the document lookup */
                PFla_op_t *doc_tbl = PFprop_lineage (p->prop, p->sem.step.item);
                if (R(p)->kind != la_roots &&
                    doc_tbl &&
                    doc_tbl->kind == la_doc_tbl &&
                    PFprop_lineage_col (p->prop, p->sem.step.item) ==
                    doc_tbl->sem.doc_tbl.res &&
                    PFprop_card (doc_tbl->prop) == 1) {
                    /* we have to adjust the result of the step to the input
                       cardinality */
                    PFalg_col_t new_col = PFcol_new (p->sem.step.item_res);
                    *p = *cross (R(p),
                                 project (
                                     step_join (L(p),
                                                roots (doc_tbl),
                                                p->sem.step.spec,
                                                p->sem.step.level,
                                                doc_tbl->sem.doc_tbl.res,
                                                new_col),
                                     proj (p->sem.step.item_res, new_col)));
                    modified = true;
                    break;
                }
            }
            break;

        case la_element:
        {
            PFla_op_t *fcns = R(p);

            /* Traverse all children and try to merge more nodes into the twig
               (as long as the input provides for every iteration a node). */
            while (fcns->kind == la_fcns) {
                /**
                 * match the following pattern
                 *              _ _ _ _ _
                 *            |          \
                 *         content        |
                 *         /     \
                 *     frag_U    attach   |
                 *     /   \       |
                 *  empty  frag  roots    |
                 *  frag      \   /
                 *            twig        |
                 *              | _ _ _ _/
                 *
                 * and throw it away ( - - - )
                 */
                if (L(fcns)->kind == la_content &&
                    /* Make sure that all iterations of the parent
                       are present (no subdomain relationship). */
                    (PFprop_subdom (
                         p->prop,
                         PFprop_dom (p->prop,
                                     p->sem.iter_item.iter),
                         PFprop_dom (L(fcns)->prop,
                                     L(fcns)->sem.iter_pos_item.iter)) ||
                     (PFprop_card (p->prop) == 1 &&
                      PFprop_card (L(fcns)->prop) == 1 &&
                      PFprop_const (p->prop, p->sem.iter_item.iter) &&
                      PFprop_const (L(fcns)->prop,
                                    L(fcns)->sem.iter_pos_item.iter) &&
                      PFalg_atom_comparable (
                          PFprop_const_val (p->prop, p->sem.iter_item.iter),
                          PFprop_const_val (L(fcns)->prop,
                                            L(fcns)->sem.iter_pos_item.iter)) &&
                      !PFalg_atom_cmp (
                          PFprop_const_val (p->prop, p->sem.iter_item.iter),
                          PFprop_const_val (L(fcns)->prop,
                                            L(fcns)->sem.iter_pos_item.iter)))) &&
                    LR(fcns)->kind == la_attach &&
                    LRL(fcns)->kind == la_roots &&
                    LRLL(fcns)->kind == la_twig &&
                    LL(fcns)->kind == la_frag_union &&
                    LLL(fcns)->kind == la_empty_frag &&
                    LLR(fcns)->kind == la_fragment &&
                    LLRL(fcns) == LRLL(fcns) &&
                    /* input columns match the output
                       columns of the underlying twig */
                    L(fcns)->sem.iter_pos_item.iter ==
                    LRLL(fcns)->sem.iter_item.iter &&
                    L(fcns)->sem.iter_pos_item.item ==
                    LRLL(fcns)->sem.iter_item.item &&
                    /* input twig is referenced only once */
                    PFprop_refctr (LR(fcns)) == 1 &&
                    PFprop_refctr (LRL(fcns)) == 1) {
                    L(fcns) = L(LRLL(fcns));
                    modified = true;
                }
                fcns = R(fcns);
            }
        }   break;

        case la_string_join:
            if (PFprop_key_left (p->prop, p->sem.string_join.iter) &&
                PFprop_subdom (p->prop,
                               PFprop_dom_right (p->prop,
                                                 p->sem.string_join.iter_sep),
                               PFprop_dom_left (p->prop,
                                                p->sem.string_join.iter))) {
                *p = *PFla_project (L(p),
                                    PFalg_proj (p->sem.string_join.iter_res,
                                                p->sem.string_join.iter),
                                    PFalg_proj (p->sem.string_join.item_res,
                                                p->sem.string_join.item));
                modified = true;
                break;
            }
            break;

        case la_roots:
            if (L(p)->kind == la_merge_adjacent &&
                PFprop_key_right (L(p)->prop,
                                  L(p)->sem.merge_adjacent.iter_in)) {
                *p = *PFla_project (
                          LR(p),
                          PFalg_proj (L(p)->sem.merge_adjacent.iter_res,
                                      L(p)->sem.merge_adjacent.iter_in),
                          PFalg_proj (L(p)->sem.merge_adjacent.pos_res,
                                      L(p)->sem.merge_adjacent.pos_in),
                          PFalg_proj (L(p)->sem.merge_adjacent.item_res,
                                      L(p)->sem.merge_adjacent.item_in));
                modified = true;
                break;
            }
            break;

        case la_frag_union:
            if (L(p)->kind == la_fragment &&
                LL(p)->kind == la_merge_adjacent &&
                PFprop_key_right (LL(p)->prop,
                                  LL(p)->sem.merge_adjacent.iter_in)) {
                *p = *PFla_dummy (R(p));
                modified = true;
            }
            else if (R(p)->kind == la_fragment &&
                RL(p)->kind == la_merge_adjacent &&
                PFprop_key_right (RL(p)->prop,
                                  RL(p)->sem.merge_adjacent.iter_in)) {
                *p = *PFla_dummy (L(p));
                modified = true;
            }
            /* remove unreferenced twig constructors */
            else if (L(p)->kind == la_fragment &&
                     LL(p)->kind == la_twig &&
                     PFprop_refctr (LL(p)) == 1) {
                *p = *PFla_dummy (R(p));
                modified = true;
            }
            else if (R(p)->kind == la_fragment &&
                     RL(p)->kind == la_twig &&
                     PFprop_refctr (RL(p)) == 1) {
                *p = *PFla_dummy (L(p));
                modified = true;
            }
            break;

        default:
            break;
    }
    return modified;
}

/**
 * Worker for split_rowid_for_side_effects that
 * searches and splits rowid operators lying on
 * the border between side effects and query body.
 */
static void
split_rowid_for_side_effects_worker (PFla_op_t *p)
{
    if (SEEN(p))
        return;

    /* We have found a rowid operator
       that is used at most once by a given
       side effect (see the reference counter
       in cases: la_error, la_cache and la_trace). */
    if (L(p) &&
        SEEN(L(p)) &&
        L(p)->kind == la_rowid &&
        /* ensure that `p' is the only reference */
        PFprop_refctr (L(p)) == 1) {
        L(p) = PFla_op_duplicate (L(p), LL(p), NULL);
        return;
    }
    if (R(p) &&
        SEEN(R(p)) &&
        R(p)->kind == la_rowid &&
        /* ensure that `p' is the only reference */
        PFprop_refctr (R(p)) == 1) {
        R(p) = PFla_op_duplicate (R(p), RL(p), NULL);
        return;
    }

    /* recursive traversal */
    switch (p->kind) {
        case la_serialize_seq:
        case la_serialize_rel:
        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
        case la_rownum:
        case la_rowrank:
        case la_rowid:
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
        case la_nil:
        case la_rec_fix:
        case la_rec_param:
        case la_rec_arg:
        case la_rec_base:
        case la_fun_call:
        case la_fun_param:
        case la_fun_frag_param:
        case la_proxy:
        case la_proxy_base:
        case la_internal_op:
            break;

        case la_side_effects:
        case la_cross:
        case la_eqjoin:
        case la_semijoin:
        case la_thetajoin:
        case la_disjunion:
        case la_intersect:
        case la_difference:
        case la_step:
        case la_step_join:
        case la_guide_step:
        case la_guide_step_join:
        case la_doc_index_join:
        case la_doc_access:
        case la_trace_items:
        case la_trace_msg:
        case la_trace_map:
        case la_string_join:
            split_rowid_for_side_effects_worker (L(p));
            split_rowid_for_side_effects_worker (R(p));
            break;

        case la_attach:
        case la_project:
        case la_select:
        case la_pos_select:
        case la_distinct:
        case la_fun_1to1:
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_bool_not:
        case la_to:
        case la_aggr:
        case la_rank:
        case la_type:
        case la_type_assert:
        case la_cast:
        case la_doc_tbl:
        case la_roots:
        case la_dummy:
            split_rowid_for_side_effects_worker (L(p));
            break;

        case la_error:
        case la_cache:
        case la_trace:
            split_rowid_for_side_effects_worker (L(p));
            PFprop_infer_refctr (R(p));
            split_rowid_for_side_effects_worker (R(p));
            break;
    }
}

/* worker for split_rowid_for_side_effects */
static void
mark_body (PFla_op_t *p)
{
    assert (p);

    /* nothing to do if we already visited that node */
    if (SEEN(p))
        return;
    /* otherwise mark the node */
    else
        SEEN(p) = true;
    /* and traverse the children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        mark_body (p->child[i]);
}

/**
 * Disable usage conflicts for rowid operators
 * that are also referenced by the side effects.
 *
 * In case the query body refers to rowid operator
 * only to ensure the correct cardinality whereas
 * the side effects use the rowid column as partition
 * criterion we split up the rowid operator. This
 * allows the subsequent rewrites to e.g. replace
 * the rowid operator by a rank operator.
 */
static void
split_rowid_for_side_effects (PFla_op_t *root)
{
    /* collect the number of direct
       references for the side effects */
    PFprop_infer_refctr (L(root));
    /* mark the nodes of the query body */
    mark_body (R(root));
    /* split up a rowid operator that lies
       on the edge of the side effects and the
       query body */
    split_rowid_for_side_effects_worker (L(root));
    /* clean up the marks */
    PFla_dag_reset (R(root));
}

/**
 * Invoke algebra optimization.
 */
PFla_op_t *
PFalgopt_complex (PFla_op_t *root)
{
    bool modified = true;

    /* Tell the optimizer that the same rowid
       operator is used in multiple settings
       (by splitting it up). */
    split_rowid_for_side_effects (root);

    while (modified) {
        /* Infer key, const, icols, domain, reqval,
           and refctr properties first */
        PFprop_infer_lineage (root);
        PFprop_infer_composite_key (root);
        /* card property inferred by key */
        /* level property inferred by key */
        PFprop_infer_key_and_fd (root);
        PFprop_infer_const (root);
        PFprop_infer_icol (root);
        /* Exploiting set information together with key information
           is only correct if we assure that the rewrite does not
           produce more duplicates afterward. (If more duplicates
           due to a set-based rewrite were introduced the key
           information might get inconsistent.) */
        PFprop_infer_set (root);
        PFprop_infer_reqval (root);
        PFprop_infer_dom (root);
        PFprop_infer_refctr (root);

        /* Optimize algebra tree */
        modified = opt_complex (root);
        PFla_dag_reset (root);

        /* In addition optimize the resulting DAG using the icols property
           to remove inconsistencies introduced by changing the types
           of unreferenced columns (rule eqjoin). The icols optimization
           will ensure that these columns are 'really' never used. */
        root = PFalgopt_icol (root);
    }

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
/* vim:set foldmarker=#if,#endif foldmethod=marker foldopen-=search: */
