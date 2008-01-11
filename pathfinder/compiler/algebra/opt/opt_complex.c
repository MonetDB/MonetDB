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

/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])
#define LL(p) (L(L(p)))
#define RL(p) (L(R(p)))
#define LR(p) (R(L(p)))
#define LLL(p) (LL(L(p)))
#define LLR(p) (R(LL(p)))
#define LRL(p) (L(LR(p)))
#define LRLL(p) (LL(LR(p)))
#define LLRL(p) (RL(LL(p)))

#define SEEN(p) ((p)->bit_dag)

/* lookup the input name of an output column
   of a projection */
static PFalg_att_t
find_old_name (PFla_op_t *op, PFalg_att_t att)
{
    assert (op->kind == la_project);

    for (unsigned int i = 0; i < op->sem.proj.count; i++)
         if (op->sem.proj.items[i].new == att)
             return op->sem.proj.items[i].old;

    return att_NULL;
}

/**
 * find_last_base checks for the following
 * small DAG fragment:
 *
 *         pi*_1
 *         |
 *        |X|_1
 *        /  \
 *    count  pi*_2    This fragment has to fulfill some additional
 *      |     |       conditions:
 *     pi*_3  |       o |X|_1.att1 == #.res (part)
 *      |     |       o |X|_2.att2 == #.res (num)
 *     |X|_2  |       o count.res == last
 *     / \    |       o count.part == #.res (part)
 *    |  pi*_4|       o |X|_2.att1 == iter
 *    |    \ /        o |X|_2.att2 == iter
 *   pi*_5  #
 *     \    |         Furthermore the rowid operator (#) forms
 *      \  pi*_6      the output and the variables (iter, item,
 *       \  /         and map) are updated to refer to the correct
 *       (..)         columns at op #.
 *
 */
static PFla_op_t *
find_last_base (PFla_op_t *op,
                PFalg_att_t *item,
                PFalg_att_t *iter,
                PFalg_att_t last,
                PFalg_proj_t *map,
                unsigned int map_count)
{
    PFla_op_t  *count     = NULL,
               *rowid     = NULL,
               *eqjoin    = NULL,
               *base      = NULL;
    PFalg_att_t part      = att_NULL,
                iter1     = att_NULL,
                iter2     = att_NULL,
                num       = att_NULL,
                part_backup;
    bool        base_left = true /* dummy value */;

    /* pi*_1: update the column names */
    while (op->kind == la_project) {
        for (unsigned int i = 0; i < map_count; i++) {
            map[i].old = find_old_name (op, map[i].old);
            if (!map[i].old) return NULL;
        }
        *iter = find_old_name (op, *iter); if (!*iter) return NULL;
        *item = find_old_name (op, *item); if (!*item) return NULL;
        last = find_old_name (op, last); if (!last) return NULL;
        op = L(op);
    }

    /* |X|_1 */
    if (op->kind != la_eqjoin)
        return NULL;

    /* count */
    if (L(op)->kind == la_count) {
        count  = L(op);
        rowid  = R(op);
        part   = op->sem.eqjoin.att1;
        num    = op->sem.eqjoin.att2;
    } else if (R(op)->kind == la_count) {
        count  = R(op);
        rowid  = L(op);
        part   = op->sem.eqjoin.att2;
        num    = op->sem.eqjoin.att1;
    } else
        return NULL;

    if (!count->sem.aggr.part ||
        count->sem.aggr.part != part ||
        count->sem.aggr.res != last)
        return NULL;

    /* ensure that last does not appear
       in the list of output variables */
    for (unsigned int i = 0; i < map_count; i++)
        if (map[i].old == last)
            return NULL;

    /* align the name of the join attribute we don't follow with map */
    for (unsigned int i = 0; i < map_count; i++)
        if (map[i].old == part)
            map[i].old = num;

    /* pi*_2: update the column names */
    op = rowid;
    while (op->kind == la_project) {
        for (unsigned int i = 0; i < map_count; i++) {
            map[i].old = find_old_name (op, map[i].old);
            if (!map[i].old) return NULL;
        }
        *iter = find_old_name (op, *iter); if (!*iter) return NULL;
        *item = find_old_name (op, *item); if (!*item) return NULL;
        num  = find_old_name (op, num);  if (!num)  return NULL;
        op = L(op);
    }
    rowid = op;

    /* # */
    if (rowid->kind != la_rowid || rowid->sem.rowid.res != num)
        return NULL;

    op = L(op);
    iter1 = *iter;

    /* pi*_6: update a copy of the iter column name */
    while (op->kind == la_project) {
        iter1 = find_old_name (op, iter1); if (!iter1) return NULL;
        op = L(op);
    }
    base = op;

    /* pi*_3: update a copy of the |X|_1 join column name */
    op = L(count);
    while (op->kind == la_project) {
        part = find_old_name (op, part); if (!part) return NULL;
        op = L(op);
    }

    /* |X|_2 */
    if (op->kind != la_eqjoin)
        return NULL;
    eqjoin = op;

    /* pi*_[4|5]: update a copies of the |X|_[1|2] join column names */
    /* make sure the left side matches */
    op = L(eqjoin);
    iter2 = eqjoin->sem.eqjoin.att1;
    part_backup = part;
    while (op->kind == la_project) {
        part  = find_old_name (op, part);
        iter2 = find_old_name (op, iter2); if (!iter2) return NULL;
        op = L(op);
    }
    /* check for the correct operator: # or (..) */
    if (op == base) {
        if (iter2 != iter1) return NULL;
        else base_left = true;
    }
    else if (op == rowid) {
        if (num != part || *iter != iter2) return NULL;
        else base_left = false;
    }
    else if (op != base && op != rowid)
        return NULL;

    /* pi*_[4|5]: update a copies of the |X|_[1|2] join column names */
    /* make sure the right side matches */
    op = R(eqjoin);
    iter2 = eqjoin->sem.eqjoin.att2;
    part = part_backup;
    while (op->kind == la_project) {
        part  = find_old_name (op, part);
        iter2 = find_old_name (op, iter2); if (!iter2) return NULL;
        op = L(op);
    }
    /* check for the correct 'other' operator: # or (..) */
    if (op == base) {
        if (iter2 != iter1) return NULL;
        if (base_left == true) return NULL;
    }
    else if (op == rowid) {
        if (num != part || *iter != iter2) return NULL;
        if (base_left == false) return NULL;
    }
    else if (op != base && op != rowid)
        return NULL;

    return rowid;
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
 *         =          o =.att[1|2] == @.res (optional match)
 *         |          o =.att[1|2] == cast.res
 *        pi*_3       o cast.type == aat_int
 *         |          o cast.res is only used in =
 *         @?         o cast.att == row#.res
 *         |          o row#.res == cast.att
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
static void
replace_pos_predicate (PFla_op_t *p)
{
    PFalg_att_t sel, eq1, eq2, eq = att_NULL, cast,
                last = att_NULL, item, part;
    PFla_op_t *op, *base;
    long long int pos = 0;
    unsigned int count = p->schema.count;
    PFalg_proj_t *map = PFmalloc (count * sizeof (PFalg_proj_t));

    /* pi_1 && sel */
    if (p->kind != la_project ||
        L(p)->kind != la_select ||
        PFprop_icol (L(p)->prop, L(p)->sem.select.att))
        return;

    for (unsigned int i = 0; i < count; i++)
        map[i] = p->sem.proj.items[i];

    sel = L(p)->sem.select.att;
    op = LL(p);

    /* pi*_2 */
    while (op->kind == la_project) {
        for (unsigned int i = 0; i < count; i++) {
            map[i].old = find_old_name (op, map[i].old);
            if (!map[i].old) return;
        }
        sel  = find_old_name (op, sel); if (!sel) return;
        op = L(op);
    }

    /* = */
    if (op->kind != la_num_eq ||
        op->sem.binary.res != sel ||
        PFprop_icol (op->prop, op->sem.binary.att1) ||
        PFprop_icol (op->prop, op->sem.binary.att2))
        return;

    eq1 = op->sem.binary.att1;
    eq2 = op->sem.binary.att2;

    op = L(op);

    /* pi*_3 && @? && pi*_4 */
    while (op->kind == la_project || op->kind == la_attach) {
        if (op->kind == la_attach) {
            if (pos) return;

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

            if (pos <= 0) return;

            op = L(op);

        } else {
            for (unsigned int i = 0; i < count; i++) {
                map[i].old = find_old_name (op, map[i].old);
                if (!map[i].old) return;
            }

            if (pos) {
                eq = find_old_name (op, eq);
                if (!eq) return;
            } else {
                eq1 = find_old_name (op, eq1);
                eq2 = find_old_name (op, eq2);
                if (!eq1 || !eq2) return;
            }

            op = L(op);
        }
    }

    /* cast */
    if (op->kind != la_cast ||
        op->sem.type.ty != aat_int ||
        (op->sem.type.res != eq1 && op->sem.type.res != eq2) ||
        PFprop_icol (op->prop, op->sem.type.att))
        return;

    if (!pos) last = op->sem.type.res == eq1 ? eq2 : eq1;
    cast = op->sem.type.att;

    op = L(op);

    /* pi*_5 */
    while (op->kind == la_project) {
        for (unsigned int i = 0; i < count; i++) {
            map[i].old = find_old_name (op, map[i].old);
            if (!map[i].old) return;
        }

        cast = find_old_name (op, cast); if (!cast) return;

        if (!pos) {
            last = find_old_name (op, last);
            if (!last) return;
        }

        op = L(op);
    }

    /* row# */
    if (op->kind != la_rownum ||
        op->sem.sort.res != cast ||
        PFord_count (op->sem.sort.sortby) != 1 ||
        PFord_order_dir_at (op->sem.sort.sortby, 0) != DIR_ASC)
        return;

    /* check that the result of the rownum operator is not used above */
    for (unsigned int i = 0; i < count; i++)
        if (map[i].old == op->sem.sort.res)
            return;

    base = L(op);
    part = op->sem.sort.part;
    item = PFord_order_col_at (op->sem.sort.sortby, 0);

    if (!pos) {
        if (!part) return;

        /* find the base operator and update the columns in
           variables: item, part and map */
        base = find_last_base (base, &item, &part, last, map, count);
        if (!base) return;
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
            PFatt_str(item),
            PFatt_str(part));
    for (unsigned int i = 0; i < count; i++)
        fprintf(stderr," map %s to %s%s",
                PFatt_str(map[i].new),
                PFatt_str(map[i].old),
                i == count - 1 ?"\n":";");
    */
}

/* worker for PFalgopt_complex */
static void
opt_complex (PFla_op_t *p)
{
    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply complex optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_complex (p->child[i]);

    /* action code */
    switch (p->kind) {
        case la_serialize_seq:
            if (PFprop_card (p->prop) == 1) {
                R(p) = PFla_attach (
                           PFla_project (
                               R(p),
                               PFalg_proj (p->sem.ser_seq.item,
                                           p->sem.ser_seq.item)),
                           p->sem.ser_seq.pos,
                           PFalg_lit_nat (1));
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
                /* create projection list to avoid missing attributes */
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

                res = PFla_lit_tbl_ (PFalg_attlist (p->sem.attach.res),
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
            }
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
                break;
            }
            /* prune unnecessary attach-project operators */
            if (L(p)->kind == la_project &&
                L(p)->schema.count == 1 &&
                LL(p)->kind == la_roots &&
                LLL(p)->kind == la_doc_tbl &&
                p->sem.attach.res == LLL(p)->sem.doc_tbl.iter &&
                PFprop_const (LLL(p)->prop, LLL(p)->sem.doc_tbl.iter) &&
                PFalg_atom_comparable (
                    p->sem.attach.value,
                    PFprop_const_val (LLL(p)->prop,
                                      LLL(p)->sem.doc_tbl.iter)) &&
                !PFalg_atom_cmp (
                    p->sem.attach.value,
                    PFprop_const_val (LLL(p)->prop,
                                      LLL(p)->sem.doc_tbl.iter)) &&
                L(p)->sem.proj.items[0].new == LLL(p)->sem.doc_tbl.item_res) {
                *p = *PFla_dummy (LL(p));
                break;
            }

            break;

        case la_project:
            replace_pos_predicate (p);
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

            /* discard join attributes as one of them always remains */
            for (unsigned int i = 0; i < L(p)->schema.count; i++) {
                left_arg_req = left_arg_req ||
                               (PFprop_unq_name (
                                    L(p)->prop,
                                    L(p)->schema.items[i].name) !=
                                PFprop_unq_name (
                                    p->prop,
                                    p->sem.eqjoin.att1) &&
                                PFprop_icol (
                                   p->prop,
                                   L(p)->schema.items[i].name));
            }
            if ((PFprop_key_left (p->prop, p->sem.eqjoin.att1) ||
                 PFprop_set (p->prop)) &&
                PFprop_subdom (p->prop,
                               PFprop_dom_right (p->prop,
                                                 p->sem.eqjoin.att2),
                               PFprop_dom_left (p->prop,
                                                p->sem.eqjoin.att1)) &&
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
                                        p->sem.eqjoin.att2);

                for (unsigned int i = 0; i < R(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        R(p)->schema.items[i].name,
                                        R(p)->schema.items[i].name);

                *p = *PFla_project_ (R(p), count, proj);
                break;
            }

            /* discard join attributes as one of them always remains */
            for (unsigned int i = 0; i < R(p)->schema.count; i++) {
                right_arg_req = right_arg_req ||
                                (PFprop_unq_name (
                                     R(p)->prop,
                                     R(p)->schema.items[i].name) !=
                                 PFprop_unq_name (
                                     p->prop,
                                     p->sem.eqjoin.att2) &&
                                 PFprop_icol (
                                     p->prop,
                                     R(p)->schema.items[i].name));
            }
            if ((PFprop_key_right (p->prop, p->sem.eqjoin.att2) ||
                 PFprop_set (p->prop)) &&
                PFprop_subdom (p->prop,
                               PFprop_dom_left (p->prop,
                                                p->sem.eqjoin.att1),
                               PFprop_dom_right (p->prop,
                                                 p->sem.eqjoin.att2)) &&
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
                                        p->sem.eqjoin.att1);

                *p = *PFla_project_ (L(p), count, proj);
                break;
            }

            /* introduce semi-join operator if possible */
            if (!left_arg_req &&
                (PFprop_key_left (p->prop, p->sem.eqjoin.att1) ||
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
                PFalg_att_t latt = p->sem.eqjoin.att1;
                PFalg_att_t ratt = p->sem.eqjoin.att2;

                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        L(p)->schema.items[i].name,
                                        p->sem.eqjoin.att2);

                for (unsigned int i = 0; i < R(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        R(p)->schema.items[i].name,
                                        R(p)->schema.items[i].name);

                while (left->kind == la_project) {
                    for (unsigned int i = 0; i < left->sem.proj.count; i++)
                        if (latt == left->sem.proj.items[i].new) {
                            latt = left->sem.proj.items[i].old;
                            break;
                        }
                    left = L(left);
                }
                while (right->kind == la_project) {
                    for (unsigned int i = 0; i < right->sem.proj.count; i++)
                        if (ratt == right->sem.proj.items[i].new) {
                            ratt = right->sem.proj.items[i].old;
                            break;
                        }
                    right = L(right);
                }

                if (latt == ratt && left == right)
                    semijoin = R(p);
                else
                    semijoin = PFla_semijoin (
                                   R(p),
                                   L(p),
                                   p->sem.eqjoin.att2,
                                   p->sem.eqjoin.att1);

                *p = *PFla_project_ (semijoin, count, proj);
                break;
            }

            /* introduce semi-join operator if possible */
            if (!right_arg_req &&
                (PFprop_key_right (p->prop, p->sem.eqjoin.att2) ||
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
                PFalg_att_t latt = p->sem.eqjoin.att1;
                PFalg_att_t ratt = p->sem.eqjoin.att2;

                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        L(p)->schema.items[i].name,
                                        L(p)->schema.items[i].name);

                for (unsigned int i = 0; i < R(p)->schema.count; i++)
                    proj[count++] = PFalg_proj (
                                        R(p)->schema.items[i].name,
                                        p->sem.eqjoin.att1);

                while (left->kind == la_project) {
                    for (unsigned int i = 0; i < left->sem.proj.count; i++)
                        if (latt == left->sem.proj.items[i].new) {
                            latt = left->sem.proj.items[i].old;
                            break;
                        }
                    left = L(left);
                }
                while (right->kind == la_project) {
                    for (unsigned int i = 0; i < right->sem.proj.count; i++)
                        if (ratt == right->sem.proj.items[i].new) {
                            ratt = right->sem.proj.items[i].old;
                            break;
                        }
                    right = L(right);
                }

                if (latt == ratt && left == right)
                    semijoin = L(p);
                else
                    semijoin = PFla_semijoin (
                                   L(p),
                                   R(p),
                                   p->sem.eqjoin.att1,
                                   p->sem.eqjoin.att2);

                *p = *PFla_project_ (semijoin, count, proj);
                break;
            }
        }   break;

        case la_semijoin:
            /* the following if statement is a copy of code in opt_dom.c */
            /* if the semijoin operator does not prune a row
               because the domains are identical we can safely
               remove it. */
            if (PFprop_subdom (
                    p->prop,
                    PFprop_dom_left (p->prop,
                                     p->sem.eqjoin.att1),
                    PFprop_dom_right (p->prop,
                                      p->sem.eqjoin.att2)) &&
                PFprop_subdom (
                    p->prop,
                    PFprop_dom_right (p->prop,
                                      p->sem.eqjoin.att2),
                    PFprop_dom_left (p->prop,
                                     p->sem.eqjoin.att1))) {
                *p = *L(p);
                break;
            }
            if (L(p)->kind == la_difference &&
                (L(p)->schema.count == 1 ||
                 PFprop_key (p->prop, p->sem.eqjoin.att1)) &&
                PFprop_subdom (
                    p->prop,
                    PFprop_dom_right (p->prop,
                                      p->sem.eqjoin.att2),
                    PFprop_dom_right (L(p)->prop,
                                      p->sem.eqjoin.att1))) {
                *p = *PFla_empty_tbl_ (p->schema);
                break;
            }

            if (!PFprop_key_left (p->prop, p->sem.eqjoin.att1) ||
                !PFprop_subdom (p->prop,
                                PFprop_dom_right (p->prop,
                                                  p->sem.eqjoin.att2),
                                PFprop_dom_left (p->prop,
                                                 p->sem.eqjoin.att1)))
                break;

            /* remove the distinct operator and redirect the
               references to the semijoin operator */
            if (R(p)->kind == la_distinct) {
                PFla_op_t *distinct = R(p);
                R(p) = L(distinct);
                *distinct = *PFla_project (p, PFalg_proj (p->sem.eqjoin.att2,
                                                          p->sem.eqjoin.att1));
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
                                PFalg_proj (p->sem.eqjoin.att2,
                                            p->sem.eqjoin.att1));
                *distinct = *PFla_project (
                                 p,
                                 PFalg_proj (distinct->schema.items[0].name,
                                             p->sem.eqjoin.att1));

                /* we need to adjust the semijoin argument as well */
                p->sem.eqjoin.att2 = R(p)->schema.items[0].name;
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
                break;
            }
            if (PFprop_card (R(p)->prop) == 1 &&
                PFprop_icols_count (R(p)->prop) == 0) {
                *p = *PFla_dummy (L(p));
                break;
            }
            break;

        case la_select:
        /**
         * Rewrite the pattern (1) into expression (2):
         *
         *          select_(att1) [icols:att2]          pi_(att2,...:att2)
         *            |                                  |
         *         ( pi_(att1,att2) )                 distinct
         *            |                                  |
         *           or_(att1:att3,att4)               union
         *            |                              ____/\____
         *         ( pi_(att2,att3,att4) )          /          \
         *            |                            pi_(att2)   pi_(att2:att5)
         *           |X|_(att2,att5)              /              \
         *        __/   \__                    select_(att3)   select_(att4)
         *       /         \                     |                |
         *      /1\       /2\                   /1\              /2\
         *     /___\     /___\                 /___\            /___\
         * (att2,att3) (att5,att4)            (att2,att3)      (att5,att4)
         *
         *           (1)                                 (2)
         */
        {
            unsigned int i;
            PFalg_att_t att_sel,
                        att_join1, att_join2,
                        att_sel_in1, att_sel_in2;
            PFla_op_t *cur, *left, *right;
            PFalg_proj_t *lproj, *rproj, *top_proj;

            if (p->schema.count != 2 ||
                PFprop_icols_count (p->prop) != 1 ||
                PFprop_icol (p->prop, p->sem.select.att))
                break;

            att_sel = p->sem.select.att;
            att_join1 = p->schema.items[0].name != att_sel
                        ? p->schema.items[0].name
                        : p->schema.items[1].name;
            cur = L(p);

            /* cope with intermediate projections */
            if (cur->kind == la_project) {
                for (i = 0; i < cur->sem.proj.count; i++)
                    if (L(p)->sem.proj.items[i].new == att_sel)
                        att_sel = L(p)->sem.proj.items[i].old;
                    else if (L(p)->sem.proj.items[i].new == att_join1)
                        att_join1 = L(p)->sem.proj.items[i].old;
                cur = L(cur);
            }

            if (cur->kind != la_bool_or ||
                att_sel != cur->sem.binary.res)
                break;

            att_sel_in1 = cur->sem.binary.att1;
            att_sel_in2 = cur->sem.binary.att2;

            cur = L(cur);

            /* cope with intermediate projections */
            if (cur->kind == la_project) {
                for (i = 0; i < cur->sem.proj.count; i++)
                    if (L(p)->sem.proj.items[i].new == att_join1)
                        att_join1 = L(p)->sem.proj.items[i].old;
                    else if (L(p)->sem.proj.items[i].new == att_sel_in1)
                        att_sel_in1 = L(p)->sem.proj.items[i].old;
                    else if (L(p)->sem.proj.items[i].new == att_sel_in2)
                        att_sel_in2 = L(p)->sem.proj.items[i].old;
                cur = L(cur);
            }

            if (cur->kind != la_eqjoin ||
                (att_join1 != cur->sem.eqjoin.att1 &&
                 att_join1 != cur->sem.eqjoin.att2))
                break;

            if (PFprop_ocol (L(cur), att_sel_in1) &&
                PFprop_ocol (R(cur), att_sel_in2)) {
                att_join1 = cur->sem.eqjoin.att1;
                att_join2 = cur->sem.eqjoin.att2;
                left = L(cur);
                right = R(cur);
            }
            else if (PFprop_ocol (L(cur), att_sel_in2) &&
                    PFprop_ocol (R(cur), att_sel_in1)) {
                att_join1 = cur->sem.eqjoin.att2;
                att_join2 = cur->sem.eqjoin.att1;
                left = R(cur);
                right = L(cur);
            }
            else
                break;

            /* pattern (1) is now ensured: create pattern (2) */
            lproj = PFmalloc (sizeof (PFalg_proj_t));
            rproj = PFmalloc (sizeof (PFalg_proj_t));
            top_proj = PFmalloc (2 * sizeof (PFalg_proj_t));

            lproj[0] = PFalg_proj (att_join1, att_join1);
            rproj[0] = PFalg_proj (att_join1, att_join2);
            top_proj[0] = PFalg_proj (p->schema.items[0].name, att_join1);
            top_proj[1] = PFalg_proj (p->schema.items[1].name, att_join1);

            *p = *PFla_project_ (
                      PFla_distinct (
                          PFla_disjunion (
                              PFla_project_ (
                                  PFla_select (left, att_sel_in1),
                                  1, lproj),
                              PFla_project_ (
                                  PFla_select (right, att_sel_in2),
                                  1, rproj))),
                      2, top_proj);
        }   break;

        case la_difference: /* this case is a copy of code in opt_dom.c */
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

            if (all_subdom == p->schema.count) {
                *p = *PFla_empty_tbl_ (p->schema);
                SEEN(p) = true;
                break;
            }
        }   break;

        /* to get rid of the operator 'and' and to split up
           different conditions we can introduce additional
           select operators above comparisons whose required
           value is true. */
        case la_bool_and:
            if (PFprop_reqval (p->prop, p->sem.binary.res) &&
                PFprop_reqval_val (p->prop, p->sem.binary.res) &&
                PFprop_set (p->prop)) {
                *p = *PFla_attach (
                          PFla_select (
                              PFla_select (
                                  L(p),
                                  p->sem.binary.att1),
                              p->sem.binary.att2),
                          p->sem.binary.res,
                          PFalg_lit_bln (true));
            }
            break;

        case la_rank:
            /* match the pattern rank - (project -) rank and
               try to merge both rank operators if the nested
               one only prepares some columns for the outer rank.
                 As most operators are separated by a projection
               we also support projections that do not rename. */
        {
            PFla_op_t *rank;
            bool proj = false, renamed = false;
            unsigned int i;

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
                !PFprop_icol (p->prop, rank->sem.sort.res)) {

                PFord_ordering_t sortby;
                PFalg_proj_t *proj_list;
                PFalg_att_t inner_att = rank->sem.sort.res;
                unsigned int pos_att = 0, count = 0;

                /* lookup position of the inner rank column in
                   the list of sort criteria of the outer rank */
                for (i = 0; i < PFord_count (p->sem.sort.sortby); i++)
                    if (PFord_order_col_at (p->sem.sort.sortby, i)
                            == inner_att &&
                        /* make sure the order is the same */
                        PFord_order_dir_at (p->sem.sort.sortby, i)
                            == DIR_ASC) {
                        pos_att = i;
                        break;
                    }

                /* inner rank column is not used in the outer rank
                   (thus the inner rank is probably superfluous
                    -- let the icols optimization remove the operator) */
                if (i == PFord_count (p->sem.sort.sortby)) break;

                sortby = PFordering ();

                /* create new sort list where the sort criteria of the
                   inner rank substitute the inner rank column */
                for (i = 0; i < pos_att; i++)
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

                for (i = pos_att + 1;
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
                       may collide with the attribute name of one of the
                       inner ranks sort criteria, we use the column name
                       of the inner rank as resulting attribute name
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

                break;
            }

        }   break;

        case la_rowid:
            if (PFprop_card (p->prop) == 1) {
                *p = *PFla_attach (L(p), p->sem.rowid.res, PFalg_lit_nat (1));
            }
            break;

        case la_step:
            if (p->sem.step.level < 0)
                p->sem.step.level = PFprop_level (p->prop,
                                                  p->sem.step.item_res);

            if ((p->sem.step.axis == alg_desc ||
                 p->sem.step.axis == alg_desc_s) &&
                p->sem.step.level >= 1 &&
                p->sem.step.level - 1 == PFprop_level (R(p)->prop,
                                                       p->sem.step.item))
                p->sem.step.axis = alg_chld;

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
                     R(p)->sem.proj.items[0].old))
                    *p = *PFla_project (PFla_step (
                                            L(p),
                                            RL(p),
                                            p->sem.step.axis,
                                            p->sem.step.ty,
                                            p->sem.step.level,
                                            RL(p)->sem.step.iter,
                                            RL(p)->sem.step.item,
                                            RL(p)->sem.step.item_res),
                                        R(p)->sem.proj.items[0],
                                        R(p)->sem.proj.items[1]);
                break;
            }
            else if (R(p)->kind == la_rowid &&
                     p->sem.step.axis == alg_chld &&
                     p->sem.step.iter == R(p)->sem.rowid.res &&
                     !PFprop_icol (p->prop, p->sem.step.iter) &&
                     PFprop_key (p->prop, p->sem.step.item)) {
                R(p) = PFla_attach (RL(p),
                                    R(p)->sem.rowid.res,
                                    PFalg_lit_nat (1));
                break;
            }
            break;

        case la_guide_step:
        case la_guide_step_join:
            if (p->sem.step.level < 0 && p->sem.step.guide_count) {
                int level = p->sem.step.guides[0]->level;
                for (unsigned int i = 1; i < p->sem.step.guide_count; i++)
                    if (level != p->sem.step.guides[i]->level)
                        break;
                p->sem.step.level = level;
            }

            if ((p->sem.step.axis == alg_desc ||
                 p->sem.step.axis == alg_desc_s) &&
                p->sem.step.level >= 1 &&
                p->sem.step.level - 1 == PFprop_level (R(p)->prop,
                                                       p->sem.step.item))
                p->sem.step.axis = alg_chld;
            break;

        case la_step_join:
            if (p->sem.step.level < 0)
                p->sem.step.level = PFprop_level (p->prop,
                                                  p->sem.step.item_res);

            if ((p->sem.step.axis == alg_desc ||
                 p->sem.step.axis == alg_desc_s) &&
                p->sem.step.level >= 1 &&
                p->sem.step.level - 1 == PFprop_level (R(p)->prop,
                                                       p->sem.step.item))
                p->sem.step.axis = alg_chld;
            break;

        case la_fcns:
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
            if (L(p)->kind == la_content &&
                LR(p)->kind == la_attach &&
                LRL(p)->kind == la_roots &&
                LRLL(p)->kind == la_twig &&
                LL(p)->kind == la_frag_union &&
                LLL(p)->kind == la_empty_frag &&
                LLR(p)->kind == la_fragment &&
                LLRL(p) == LRLL(p) &&
                /* input columns match the output
                   columns of the underlying twig */
                L(p)->sem.iter_pos_item.iter ==
                LRLL(p)->sem.iter_item.iter &&
                L(p)->sem.iter_pos_item.item ==
                LRLL(p)->sem.iter_item.item &&
                /* input twig is referenced only once */
                PFprop_refctr (LR(p)) == 1 &&
                PFprop_refctr (LRL(p)) == 1) {
                L(p) = L(LRLL(p));
            }
            break;

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
                break;
            }
            break;

        case la_frag_union:
            if (L(p)->kind == la_fragment &&
                LL(p)->kind == la_merge_adjacent &&
                PFprop_key_right (LL(p)->prop,
                                  LL(p)->sem.merge_adjacent.iter_in))
                *p = *PFla_dummy (R(p));
            else if (R(p)->kind == la_fragment &&
                RL(p)->kind == la_merge_adjacent &&
                PFprop_key_right (RL(p)->prop,
                                  RL(p)->sem.merge_adjacent.iter_in))
                *p = *PFla_dummy (L(p));
            break;

        default:
            break;
    }
}

/**
 * Invoke algebra optimization.
 */
PFla_op_t *
PFalgopt_complex (PFla_op_t *root)
{
    /* Infer key, icols, domain, and unique names
       properties first */
    PFprop_infer_unq_names (root);
    /* already inferred by PFprop_infer_unq_names
    PFprop_infer_key (root);
    */
    PFprop_infer_level (root);
    PFprop_infer_icol (root);
    PFprop_infer_dom (root);
    PFprop_infer_set (root);
    PFprop_infer_reqval (root);
    PFprop_infer_refctr (root);

    /* Optimize algebra tree */
    opt_complex (root);
    PFla_dag_reset (root);

    /* In addition optimize the resulting DAG using the icols property
       to remove inconsistencies introduced by changing the types
       of unreferenced columns (rule eqjoin). The icols optimization
       will ensure that these columns are 'really' never used. */
    root = PFalgopt_icol (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
