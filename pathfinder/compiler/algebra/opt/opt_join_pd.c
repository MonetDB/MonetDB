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
 *     conditional error, roots, and type assertion)
 * 3.) operators that support the transformation if the do not create
 *     a join column (e.g. comparisons and calculations)
 * 4.) operators that fit into the 4. category and additionally require
 *     the cardinality to stay the same (rownum, rowid, attribute, and
 *     textnode constructor)
 * 5.) cross product operator
 * 6.) equi-join operator
 * 7.) project operator
 * The last four groups (4.-7.) require special treatment (for details
 * on the transformation please look into the code).
 *
 * In addition a transformation rule that combines adjacent projections
 * is copied from opt_general.brg to keep the plans small. One more
 * transformation rule removes equi-joins that join the same relation
 * on the same attribute whose values are unique.
 *
 * As pushing down equi-joins may result in an equi-join referencing the
 * same operator twice the equi-join operator has to cope with duplicate
 * columns that would appear in its schema. Because no operator except the
 * eqjoin_unq can cope with duplicate columns this operator prunes all
 * these (based on the name) and returns a schema with distinct column
 * names.
 * We can be sure that these column are always the same as the mapping
 * from original names to unique names introduced renaming projections
 * whenever the same unique names would have been chosen for a column.
 * In addition we ensure that during this phase no such projection is
 * ignored and the duplicate unique columns are indeed the same column.
 *
 * An cleaning phase in the end removes all the duplicate columns by
 * introducing projections whenever necessary.
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

#include "oops.h"
#include "algopt.h"
#include "properties.h"
#include "alg_dag.h"
#include "mem.h"          /* PFmalloc() */

/* mnemonic algebra constructors */
#include "logical_mnemonic.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define SEEN(p) ((p)->bit_dag)
#define LEFT(p) ((p)->bit_in)
#define RIGHT(p) ((p)->bit_out)

/**
 * Use equi-join implementation that replaces
 * both join argument by a result column.
 */
#define eqjoin_unq(a,b,c,d,e) PFla_eqjoin_clone ((a),(b),(c),(d),(e))

/* abbreviation for attribute dependency test */
static bool
is_join_att (PFla_op_t *p, PFalg_att_t att)
{
    assert (p && p->kind == la_eqjoin_unq);

    return (p->sem.eqjoin_unq.att1 == (att) ||
            p->sem.eqjoin_unq.att2 == (att));
}

/* worker for binary operators */
static PFla_op_t *
modify_binary_op (PFla_op_t *p,
                  PFla_op_t *lp,
                  PFla_op_t *rp,
                  PFla_op_t * (* op) (const PFla_op_t *,
                                      PFalg_att_t,
                                      PFalg_att_t,
                                      PFalg_att_t))
{
    PFalg_att_t latt, ratt;
    PFla_op_t  *next_join = NULL;

    /* parametrize optimization to use the same code
       for the left and the right operand of the equi-join */
    if (L(p) == lp) {
        latt = p->sem.eqjoin_unq.att1;
        ratt = p->sem.eqjoin_unq.att2;
    } else {
        latt = p->sem.eqjoin_unq.att2;
        ratt = p->sem.eqjoin_unq.att1;
    }

    if (!is_join_att (p, lp->sem.binary.res)) {
        *p = *(op (eqjoin_unq (L(lp), rp, latt, ratt,
                               p->sem.eqjoin_unq.res),
                   lp->sem.binary.res,
                   is_join_att (p, lp->sem.binary.att1)
                      ? p->sem.eqjoin_unq.res
                      : lp->sem.binary.att1,
                   is_join_att (p, lp->sem.binary.att2)
                      ? p->sem.eqjoin_unq.res
                      : lp->sem.binary.att2));
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
                                     PFalg_att_t,
                                     PFalg_att_t))
{
    PFalg_att_t latt, ratt;
    PFla_op_t  *next_join = NULL;

    /* parametrize optimization to use the same code
       for the left and the right operand of the equi-join */
    if (L(p) == lp) {
        latt = p->sem.eqjoin_unq.att1;
        ratt = p->sem.eqjoin_unq.att2;
    } else {
        latt = p->sem.eqjoin_unq.att2;
        ratt = p->sem.eqjoin_unq.att1;
    }

    if (!is_join_att (p, lp->sem.unary.res)) {
        *p = *(op (eqjoin_unq (L(lp), rp, latt, ratt,
                               p->sem.eqjoin_unq.res),
                   lp->sem.unary.res,
                   is_join_att (p, lp->sem.unary.att)
                      ? p->sem.eqjoin_unq.res
                      : lp->sem.unary.att));
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
    PFalg_att_t  latt,
                 ratt;
    unsigned int lp_child;
    PFla_op_t   *next_join = NULL;
    bool         modified  = false;

    assert (p);

#define LP (p->child[lp_child])
/* check whether a column att is already generated */
#define GEN(att) (!is_join_att (p, (att)) && PFprop_ocol (rp, (att)))

    /* only process equi-joins */
    assert (p->kind == la_eqjoin_unq);

    /* make sure that we clean up all the references afterwards */
    *(PFla_op_t **) PFarray_add (clean_up_list) = L(p);
    *(PFla_op_t **) PFarray_add (clean_up_list) = R(p);

    /* action code that tries to push an equi-join underneath
       its operands */
    for (unsigned int c = 0; c < 2; c++) {
        /* only process equi-joins */
        if (p->kind != la_eqjoin_unq)
            break;

        /* in case we have pushed a join down to its origin (where
           both children references point to the same node) we may
           safely remove the join */
        if (L(p) == R(p) &&
            p->sem.eqjoin_unq.att1 == p->sem.eqjoin_unq.att2 &&
            PFprop_key (L(p)->prop, p->sem.eqjoin_unq.att1)) {
            /* the join does nothing -- it only applies a key-join
               with itself and the schema stays the same.
               Thus replace it by a dummy projection */
            PFalg_proj_t *proj_list = PFmalloc (p->schema.count *
                                                sizeof (PFalg_proj_t));

            for (unsigned int i = 0; i < p->schema.count; i++)
                proj_list[i] = proj (p->schema.items[i].name,
                                     p->schema.items[i].name);

            *p = *(PFla_project_ (L(p), p->schema.count, proj_list));
            return true;
        }

        /* parametrize optimization to use the same code
           for the left and the right operand of the equi-join */
        if (c) {
            lp = L(p);
            rp = R(p);
            latt = p->sem.eqjoin_unq.att1;
            ratt = p->sem.eqjoin_unq.att2;
            lp_child = 0;
        } else {
            lp = R(p);
            rp = L(p);
            latt = p->sem.eqjoin_unq.att2;
            ratt = p->sem.eqjoin_unq.att1;
            lp_child = 1;
        }

        /* In case the join does nothing we may safely discard it. */
        if (lp->schema.count == 1 &&
            PFprop_key (lp->prop, latt) &&
            PFprop_subdom (lp->prop,
                           PFprop_dom (rp->prop, ratt),
                           PFprop_dom (lp->prop, latt)) &&
            ratt == p->sem.eqjoin_unq.res) {
            *p = *dummy (rp);
            return true;
        }

        /* skip rewrite if the join might reference itself
           afterwards */
        if (LEFT(lp) && RIGHT(lp))
            continue;

        /* Throw away lp whenever lp creates a column
           that is also in the schema of rp. In this case
           the new column is also created in the subtree
           of rp. (Pushing lp above the eqjoin would result
           in conflicting columns otherwise.) */
        switch (lp->kind) {
            case la_serialize_seq:
            case la_serialize_rel:
            case la_lit_tbl:
            case la_empty_tbl:
            case la_ref_tbl:
            case la_cross:
            case la_eqjoin:
            case la_eqjoin_unq:
            case la_semijoin:
            case la_thetajoin:
            case la_project:
            case la_select:
            case la_pos_select:
            case la_disjunion:
            case la_intersect:
            case la_difference:
            case la_distinct:
            case la_to:
            case la_avg:
            case la_max:
            case la_min:
            case la_sum:
            case la_count:
            case la_type_assert:
            case la_seqty1:
            case la_all:
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
            case la_fragment:
            case la_frag_extract:
            case la_frag_union:
            case la_empty_frag:
            case la_error:
            case la_cond_err:
            case la_nil:
            case la_trace:
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
            case la_cross_mvd:
            case la_string_join:
                /* nothing to do -- can't throw away lp */
                break;

            case la_attach:
                if ((modified = GEN(lp->sem.attach.res))) LP = L(lp);
                break;

            case la_fun_1to1:
                if ((modified = GEN(lp->sem.fun_1to1.res))) LP = L(lp);
                break;

            case la_num_eq:
            case la_num_gt:
            case la_bool_and:
            case la_bool_or:
                if ((modified = GEN(lp->sem.binary.res))) LP = L(lp);
                break;

            case la_bool_not:
                if ((modified = GEN(lp->sem.unary.res))) LP = L(lp);
                break;

            case la_rownum:
                if (!PFprop_key (rp->prop, ratt) ||
                    !PFprop_subdom (rp->prop,
                                    PFprop_dom (lp->prop, latt),
                                    PFprop_dom (rp->prop, ratt)))
                    /* Ensure that the values of the left join argument
                       are a subset of the values of the right join argument
                       and that the right join argument is keyed. These
                       two tests make sure that we have exactly one match per
                       tuple in the left relation and thus the result of the
                       rownum operator stays stable. */
                    break;

                if ((modified = GEN(lp->sem.sort.res))) LP = L(lp);
                break;

            case la_rowrank:
            case la_rank:
                if ((modified = GEN(lp->sem.sort.res))) LP = L(lp);
                break;

            case la_rowid:
                if (!PFprop_key (rp->prop, ratt) ||
                    !PFprop_subdom (rp->prop,
                                    PFprop_dom (lp->prop, latt),
                                    PFprop_dom (rp->prop, ratt)))
                    /* Ensure that the values of the left join argument
                       are a subset of the values of the right join argument
                       and that the right join argument is keyed. These
                       two tests make sure that we have exactly one match per
                       tuple in the left relation and thus the result of the
                       rowid operator stays stable. */
                    break;

                if ((modified = GEN(lp->sem.rowid.res))) LP = L(lp);
                break;

            case la_type:
            case la_cast:
                if ((modified = GEN(lp->sem.type.res))) LP = L(lp);
                break;

            case la_step_join:
            case la_guide_step_join:
                if ((modified = GEN(lp->sem.step.item_res))) LP = R(lp);
                break;

            case la_doc_index_join:
                if ((modified = GEN(lp->sem.doc_join.item_res))) LP = R(lp);
                break;

            case la_doc_tbl:
                if ((modified = GEN(lp->sem.doc_tbl.res))) LP = L(lp);
                break;

            case la_doc_access:
                if ((modified = GEN(lp->sem.doc_access.res))) LP = R(lp);
                break;

            case la_dummy:
                LP = L(lp);
                next_join = p;
                break;
        }
        /* If a child of p was replaced ... */
        if (modified) {
            /* ... start another round with p. */
            join_pushdown_worker (p, clean_up_list);
            return true;
        }

        switch (lp->kind) {
            case la_serialize_seq:
            case la_serialize_rel:
            case la_lit_tbl:
            case la_empty_tbl:
            case la_ref_tbl:
            case la_intersect:
            case la_difference:
            case la_distinct:
            case la_avg:
            case la_max:
            case la_min:
            case la_sum:
            case la_count:
            case la_seqty1:
            case la_all:
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
            case la_fragment:
            case la_frag_extract:
            case la_frag_union:
            case la_empty_frag:
            case la_string_join:
            case la_dummy:
                /* nothing to do -- can't push down the equi-join */
                break;

            case la_attach:
                if (!is_join_att (p, lp->sem.attach.res)) {
                    *p = *(attach (eqjoin_unq (L(lp), rp, latt, ratt,
                                               p->sem.eqjoin_unq.res),
                                   lp->sem.attach.res,
                                   lp->sem.attach.value));
                    next_join = L(p);
                }
                break;

            case la_cross:
            {
                /* choose the correct operand of the cross product
                   (the one with the join argument) to push down
                   the equi-join */
                bool in_left = false;
                for (unsigned int i = 0; i < L(lp)->schema.count; i++)
                    if (L(lp)->schema.items[i].name == latt) {
                        in_left = true;
                        break;
                    }

                if (in_left)
                    *p = *(cross (eqjoin_unq (L(lp), rp, latt, ratt,
                                              p->sem.eqjoin_unq.res),
                                  R(lp)));
                else
                    *p = *(cross (eqjoin_unq (R(lp), rp, latt, ratt,
                                              p->sem.eqjoin_unq.res),
                                  L(lp)));

                next_join = L(p);
                *(PFla_op_t **) PFarray_add (clean_up_list) = R(p);
            }   break;

            case la_cross_mvd:
                PFoops (OOPS_FATAL,
                        "clone column aware cross product operator is "
                        "only allowed inside mvd optimization!");
                break;

            case la_eqjoin:
                PFoops (OOPS_FATAL,
                        "clone column unaware eqjoin operator is "
                        "only allowed with original attribute names!");

            case la_eqjoin_unq:
                /* Rewriting joins is only effective if we push
                   joins further down that have different join columns.
                   The correct choice of the branch is however not
                   directly clear.

                   We thus only optimze only the patterns
                         |X|_1          |X|_1           |X|
                        /   \          /   \           /   \
                       |X|   1        |X|   1         #     2
                      /   \    and   /   \     into   |
                      #    2        2     #          |X|_1
                      |                   |         /   \
                      3                   3        3     1

                   whenever possible (e.g., only if no columns references
                   are missing).

                   In addition we remove mapping joins directly visible
                   (more explanation see below).

                        |X|
                        / \
                       |X| \          |X|
                       / \  \   ->    / \
                        (pi) |           #
                          \ /
                           #

                   For all other directly following equi-joins
                   we discard any rewriting. */

                /* make sure that we have a key join
                   (a rewrite thus does not change the cardinality) */
                if (!PFprop_key (rp->prop, ratt)
                    ||
                    !PFprop_subdom (lp->prop,
                                    PFprop_dom (lp->prop, latt),
                                    PFprop_dom (rp->prop, ratt)))
                    break;

                if (L(lp)->kind == la_rowid &&
                    L(lp)->sem.rowid.res == lp->sem.eqjoin_unq.att1 &&
                    lp->sem.eqjoin_unq.res != latt) {
                    unsigned int  i;
                    PFla_op_t    *new_rowid, *new_p;
                    PFalg_proj_t *proj,
                                 *top_proj = NULL;
                    PFalg_att_t   cur;

                    /* ensure that column of the upper eqjoin
                       is not created on the other side */
                    for (i = 0; i < L(lp)->schema.count; i++)
                        if (L(lp)->schema.items[i].name == latt)
                            break;
                    if (i == L(lp)->schema.count)
                        break;

                    /* replace the rowid operator by a projection
                       that holds the same schema as the old rowid
                       operator */
                    proj = PFmalloc (L(lp)->schema.count *
                                     sizeof (PFalg_proj_t));
                    for (i = 0; i < L(lp)->schema.count; i++) {
                        cur = L(lp)->schema.items[i].name;
                        if (cur == latt)
                            /* make sure the column names stay the same */
                            proj[i] = PFalg_proj (latt, p->sem.eqjoin_unq.res);
                        else
                            proj[i] = PFalg_proj (cur, cur);
                    }

                    /* push down upper join underneath the rowid
                       operator */
                    new_rowid = rowid (
                                    eqjoin_unq (LL(lp), rp,
                                                latt, ratt,
                                                p->sem.eqjoin_unq.res),
                                    L(lp)->sem.rowid.res);
                    /* remember a reference to that join */
                    next_join = L(new_rowid);

                    /* replace the old rowid operator by a projection
                       that refers to the new rowid operator */
                    *L(lp) = *PFla_project_ (new_rowid,
                                             L(lp)->schema.count,
                                             proj);

                    /* rebuild the originally lower join operator */
                    new_p = eqjoin_unq (new_rowid, R(lp),
                                        lp->sem.eqjoin_unq.att1,
                                        lp->sem.eqjoin_unq.att2,
                                        lp->sem.eqjoin_unq.res);

                    /* remember to clean up the parts that cannot be
                       visited anymore by the rewritten join */
                    *(PFla_op_t **) PFarray_add (clean_up_list) = R(lp);

                    /* As the new projection (that replaced the rowid
                       operator) refers to the 'latt' column (and this
                       column may only appear above the pattern if it
                       corresponds to the result of the originally
                       upper join) we need to make sure that it is
                       removed if it would introduce a new column name */
                    if (latt != p->sem.eqjoin_unq.res) {
                        top_proj = PFmalloc (p->schema.count *
                                             sizeof (PFalg_proj_t));
                        for (i = 0; i < p->schema.count; i++) {
                            cur = p->schema.items[i].name;
                             top_proj[i] = PFalg_proj (cur, cur);
                        }
                        new_p = PFla_project_ (new_p,
                                               p->schema.count,
                                               top_proj);
                    }

                    *p = *new_p;
                    break;
                }
                if (R(lp)->kind == la_rowid &&
                    R(lp)->sem.rowid.res == lp->sem.eqjoin_unq.att1 &&
                    lp->sem.eqjoin_unq.res != latt) {
                    unsigned int  i;
                    PFla_op_t    *new_rowid, *new_p;
                    PFalg_proj_t *proj,
                                 *top_proj = NULL;
                    PFalg_att_t   cur;

                    /* ensure that column of the upper eqjoin
                       is not created on the other side */
                    for (i = 0; i < R(lp)->schema.count; i++)
                        if (R(lp)->schema.items[i].name == latt)
                            break;
                    if (i == R(lp)->schema.count)
                        break;

                    /* replace the rowid operator by a projection
                       that holds the same schema as the old rowid
                       operator */
                    proj = PFmalloc (R(lp)->schema.count *
                                     sizeof (PFalg_proj_t));
                    for (i = 0; i < R(lp)->schema.count; i++) {
                        cur = R(lp)->schema.items[i].name;
                        if (cur == latt)
                            /* make sure the column names stay the same */
                            proj[i] = PFalg_proj (latt, p->sem.eqjoin_unq.res);
                        else
                            proj[i] = PFalg_proj (cur, cur);
                    }

                    /* push down upper join underneath the rowid
                       operator */
                    new_rowid = rowid (
                                    eqjoin_unq (RL(lp), rp,
                                                latt, ratt,
                                                p->sem.eqjoin_unq.res),
                                    R(lp)->sem.rowid.res);
                    /* remember a reference to that join */
                    next_join = L(new_rowid);

                    /* replace the old rowid operator by a projection
                       that refers to the new rowid operator */
                    *R(lp) = *PFla_project_ (new_rowid,
                                             R(lp)->schema.count,
                                             proj);

                    /* rebuild the originally lower join operator */
                    new_p = eqjoin_unq (new_rowid, L(lp),
                                        lp->sem.eqjoin_unq.att1,
                                        lp->sem.eqjoin_unq.att2,
                                        lp->sem.eqjoin_unq.res);

                    /* remember to clean up the parts that cannot be
                       visited anymore by the rewritten join */
                    *(PFla_op_t **) PFarray_add (clean_up_list) = L(lp);

                    /* As the new projection (that replaced the rowid
                       operator) refers to the 'latt' column (and this
                       column may only appear above the pattern if it
                       corresponds to the result of the originally
                       upper join) we need to make sure that it is
                       removed if it would introduce a new column name */
                    if (latt != p->sem.eqjoin_unq.res) {
                        top_proj = PFmalloc (p->schema.count *
                                             sizeof (PFalg_proj_t));
                        for (i = 0; i < p->schema.count; i++) {
                            cur = p->schema.items[i].name;
                             top_proj[i] = PFalg_proj (cur, cur);
                        }
                        new_p = PFla_project_ (new_p,
                                               p->schema.count,
                                               top_proj);
                    }

                    *p = *new_p;
                    break;
                }

                /* check for the following pattern where the join
                   condition of p originates from the rowid operator
                   in rp and remove the join.

                        |X|
                        / \
                       |X| \          |X|
                       / \  \   ->    / \
                        (pi) |           #
                          \ /
                           #
                 */
                if (rp->kind == la_rowid &&
                    (L(lp) == rp || R(lp) == rp ||
                     (L(lp)->kind == la_project && LL(lp) == rp) ||
                     (R(lp)->kind == la_project && RL(lp) == rp)) &&
                    rp->sem.rowid.res == ratt) {
                    PFla_op_t    *proj,
                                 *left,
                                 *join;
                    PFalg_att_t   l_join_col,
                                  r_join_col;
                    PFalg_proj_t *proj_list;
                    unsigned int  count;

                    /* align pattern */
                    if (L(lp) == rp) {
                        proj = NULL;
                        left = R(lp);
                        l_join_col = lp->sem.eqjoin_unq.att2;
                        r_join_col = lp->sem.eqjoin_unq.att1;
                    } else if (R(lp) == rp) {
                        proj = NULL;
                        left = L(lp);
                        l_join_col = lp->sem.eqjoin_unq.att1;
                        r_join_col = lp->sem.eqjoin_unq.att2;
                    } else if (L(lp)->kind == la_project && LL(lp) == rp) {
                        proj = L(lp);
                        left = R(lp);
                        l_join_col = lp->sem.eqjoin_unq.att2;
                        r_join_col = lp->sem.eqjoin_unq.att1;
                    } else {
                        proj = R(lp);
                        left = L(lp);
                        l_join_col = lp->sem.eqjoin_unq.att1;
                        r_join_col = lp->sem.eqjoin_unq.att2;
                    }

                    /* allow only non-renaming projections */
                    if (proj) {
                        bool renamed = false;

                        for (unsigned int i = 0; i < proj->sem.proj.count; i++)
                            renamed = renamed ||
                                      (proj->sem.proj.items[i].new
                                       != proj->sem.proj.items[i].old);
                        if (renamed)
                            break;
                    }

                    /* make sure that the column generated in the rowid
                       operator is only used as join column in the above
                       join operation */
                    if (r_join_col == ratt ||
                        latt != ratt ||
                        ratt != p->sem.eqjoin_unq.res)
                        break;

                    /* create a copy of the below join operator */
                    join = eqjoin_unq (left, rp,
                                       l_join_col,
                                       r_join_col,
                                       lp->sem.eqjoin_unq.res);

                    /* if both join columns of the remaining join
                       operator are identical we only have to
                       replace the node p */
                    if (l_join_col == r_join_col &&
                        l_join_col == lp->sem.eqjoin_unq.res) {
                        *p = *join;
                        modified = true;
                        *(PFla_op_t **) PFarray_add (clean_up_list) = left;
                        *(PFla_op_t **) PFarray_add (clean_up_list) = rp;
                        break;
                    }

                    /* otherwise we have to make sure that both
                       join column are visible afterwards */
                    proj_list = PFmalloc ((join->schema.count + 1)
                                          * sizeof (*(proj_list)));
                    count = 0;

                    for (unsigned int i = 0; i < join->schema.count; i++) {
                        PFalg_att_t cur = join->schema.items[i].name;
                        if (cur == lp->sem.eqjoin_unq.res) {
                            proj_list[count++] = PFalg_proj (
                                                     r_join_col,
                                                     lp->sem.eqjoin_unq.res);
                            proj_list[count++] = PFalg_proj (
                                                     l_join_col,
                                                     lp->sem.eqjoin_unq.res);
                        } else
                            proj_list[count++] = PFalg_proj (cur, cur);
                    }

                    *p = *PFla_project_ (join, count, proj_list);
                    modified = true;
                    *(PFla_op_t **) PFarray_add (clean_up_list) = left;
                    *(PFla_op_t **) PFarray_add (clean_up_list) = rp;
                    break;
                }

                /* Key joins that use different join columns than
                   the join underneath have some potential and are
                   thus pushed down */
                if (lp->sem.eqjoin_unq.res != latt) {
                    PFla_op_t *res = NULL;
                    /* there is only one way to move this join */
                    if (PFprop_ocol (L(lp), latt) &&
                        !PFprop_ocol (R(lp), latt)) {
                        res = eqjoin_unq (
                                  eqjoin_unq (
                                      L(lp),
                                      rp,
                                      latt,
                                      ratt,
                                      p->sem.eqjoin_unq.res),
                                  R(lp),
                                  lp->sem.eqjoin_unq.att1,
                                  lp->sem.eqjoin_unq.att2,
                                  lp->sem.eqjoin_unq.res);
                        next_join = L(res);
                    }
                    /* there is only one way to move this join */
                    else if (!PFprop_ocol (L(lp), latt) &&
                             PFprop_ocol (R(lp), latt)) {
                        res = eqjoin_unq (
                                  L(lp),
                                  eqjoin_unq (
                                      R(lp),
                                      rp,
                                      latt,
                                      ratt,
                                      p->sem.eqjoin_unq.res),
                                  lp->sem.eqjoin_unq.att1,
                                  lp->sem.eqjoin_unq.att2,
                                  lp->sem.eqjoin_unq.res);
                        next_join = R(res);
                    }
                    else
                        break;

                    assert (res);

                    /* In case both join columns of the lower join
                       are visible on top of it we have to introduce
                       a projection that makes the second join column
                       visible again after the join has pruned it. */
                    if ((PFprop_ocol (rp, lp->sem.eqjoin_unq.att1) &&
                         lp->sem.eqjoin_unq.att1 !=
                         lp->sem.eqjoin_unq.res) ||
                        (PFprop_ocol (rp, lp->sem.eqjoin_unq.att2) &&
                         lp->sem.eqjoin_unq.att2 !=
                         lp->sem.eqjoin_unq.res)) {
                        PFalg_proj_t *proj = PFmalloc (
                                                 (res->schema.count + 1) *
                                                 sizeof (PFalg_proj_t));
                        unsigned int count = 0;
                        for (unsigned int i = 0; i < res->schema.count; i++) {
                            PFalg_att_t cur = res->schema.items[i].name;
                            if (cur == lp->sem.eqjoin_unq.res) {
                                proj[count++] = PFalg_proj (
                                                    lp->sem.eqjoin_unq.att1,
                                                    lp->sem.eqjoin_unq.res);
                                proj[count++] = PFalg_proj (
                                                    lp->sem.eqjoin_unq.att2,
                                                    lp->sem.eqjoin_unq.res);
                            } else
                                proj[count++] = PFalg_proj (cur, cur);
                        }
                        *p = *PFla_project_ (res, count, proj);
                    }
                    else
                        *p = *res;

                    /* Make sure that this rewrite only reports
                       a modification if this rewrite wasn't
                       the only one. Otherwise we might end up
                       in an infinite loop. */
                    modified = join_pushdown_worker (next_join,
                                                     clean_up_list);
                    next_join = NULL;

                }
                break;

            case la_semijoin:
                /* push join below the left side */
                *p = *(semijoin (eqjoin_unq (L(lp), rp, latt, ratt,
                                             p->sem.eqjoin_unq.res),
                                 R(lp),
                                 lp->sem.eqjoin.att1 == latt
                                 ? p->sem.eqjoin_unq.res
                                 : lp->sem.eqjoin.att1,
                                 lp->sem.eqjoin.att2));

                next_join = L(p);
                *(PFla_op_t **) PFarray_add (clean_up_list) = R(p);
                break;

            case la_thetajoin:
                /* choose the correct operand of the theta-join
                   (the one with the join argument) to push down
                   the equi-join */
                if (PFprop_ocol (L(lp), latt)) {
                    PFalg_sel_t *pred = PFmalloc (lp->sem.thetajoin.count *
                                                  sizeof (PFalg_sel_t));
                    /* push join below the left side */
                    for (unsigned int i = 0; i < lp->sem.thetajoin.count; i++) {
                        pred[i] = lp->sem.thetajoin.pred[i];
                        if (pred[i].left == latt)
                            pred[i].left = p->sem.eqjoin_unq.res;
                    }

                    *p = *(thetajoin (eqjoin_unq (L(lp), rp, latt, ratt,
                                                  p->sem.eqjoin_unq.res),
                                      R(lp),
                                      lp->sem.thetajoin.count,
                                      pred));

                    next_join = L(p);
                    *(PFla_op_t **) PFarray_add (clean_up_list) = R(p);
                } else {
                    PFalg_sel_t *pred = PFmalloc (lp->sem.thetajoin.count *
                                                  sizeof (PFalg_sel_t));
                    /* push join below the right side */
                    for (unsigned int i = 0; i < lp->sem.thetajoin.count; i++) {
                        pred[i] = lp->sem.thetajoin.pred[i];
                        if (pred[i].right == latt)
                            pred[i].right = p->sem.eqjoin_unq.res;
                    }

                    *p = *(thetajoin (L(lp),
                                      eqjoin_unq (R(lp), rp, latt, ratt,
                                                  p->sem.eqjoin_unq.res),
                                      lp->sem.thetajoin.count,
                                      pred));

                    next_join = R(p);
                    *(PFla_op_t **) PFarray_add (clean_up_list) = L(p);
                }
                break;

            case la_project:
                /* Here we apply transformations in two different cases.
                   First we rewrite equi-joins followed by a project that
                   do not apply renaming on the arguments. This ensures
                   that equally named columns are indeed identical columns.
                   This is furthermore enforced by the fact that we only
                   transform key joins.
                   The second case applys a transformation of an equi-join
                   followed by a renaming projection with the additional
                   constraints that the second join relation and the
                   projection both reference the same operator and join
                   on the same (possibly renamed) column.

                   A possible third alternative would be an equi-join
                   together with a renaming projection where the old as
                   well as the new column names do not conflict with the
                   column names of the right join operand. For a similar
                   reason as the missing transformation of join-join
                   patterns we skip this alternative here. */
            {
                bool renamed = false;

                /* check for renaming */
                for (unsigned int i = 0; i < lp->sem.proj.count; i++)
                    renamed = renamed ||
                              (lp->sem.proj.items[i].new
                               != lp->sem.proj.items[i].old);

                if (!renamed) {
                    PFalg_att_t cur;
                    PFalg_proj_t *proj_list;
                    unsigned int count = 0;

                    /* create projection list */
                    proj_list = PFmalloc (p->schema.count *
                                          sizeof (*(proj_list)));

                    /* add result column of equi-join */
                    proj_list[count++] = proj (p->sem.eqjoin_unq.res,
                                               p->sem.eqjoin_unq.res);

                    /* add all columns of the left argument
                       (without the join arguments) */
                    for (unsigned int i = 0; i < lp->schema.count; i++) {
                        cur = lp->schema.items[i].name;
                        if (!is_join_att (p, cur))
                            proj_list[count++] = proj (cur, cur);
                    }
                    /* add all columns of the right argument
                       (without duplicates and join arguments) */
                    for (unsigned int i = 0; i < rp->schema.count; i++) {
                        cur = rp->schema.items[i].name;
                        if (!is_join_att (p, cur)) {
                            unsigned int j;
                            for (j = 0; j < count; j++)
                                if (proj_list[j].new == cur)
                                    break;
                            /* skip columns with duplicate names */
                            if (j == count)
                                proj_list[count++] = proj (cur, cur);
                        }
                    }
                    assert (count <= p->schema.count);

                    *p = *(PFla_project_ (eqjoin_unq (L(lp), rp, latt, ratt,
                                                      p->sem.eqjoin_unq.res),
                                          count,
                                          proj_list));
                    next_join = L(p);
                }
                else if (L(lp) == rp && PFprop_key (rp->prop, ratt)) {
                    unsigned int i;
                    PFalg_proj_t proj;
                    bool same_join_col = false;

                    for (i = 0; i < lp->sem.proj.count; i++) {
                        proj = lp->sem.proj.items[i];
                        if (proj.new == latt && proj.old == ratt) {
                            same_join_col = true;
                            break;
                        }
                    }
                    /* match the pattern:   |X|
                                           /   \
                                          |  project
                                          |     |
                                           \   /
                                             op

                       where both join attributes refer to the same
                       column (which contains only unique values -
                       see check above) */
                    if (same_join_col) {
                        PFalg_att_t cur;
                        PFalg_proj_t *proj_list;
                        unsigned int count = 0;

                        /* create projection list */
                        proj_list = PFmalloc (p->schema.count *
                                              sizeof (*(proj_list)));

                        /* add result column of equi-join */
                        proj_list[count++] = proj (p->sem.eqjoin_unq.res,
                                                   p->sem.eqjoin_unq.res);

                        /* add all columns of the right argument
                           (without the join arguments) */
                        for (i = 0; i < rp->schema.count; i++) {
                            cur = rp->schema.items[i].name;
                            if (!is_join_att (p, cur))
                                proj_list[count++] = proj (cur, cur);
                        }

                        /* add all columns of the left argument
                           (without the join arguments and duplicates) */
                        for (i = 0; i < lp->sem.proj.count; i++) {
                            proj = lp->sem.proj.items[i];
                            if (!is_join_att (p, proj.new)) {
                                unsigned int j;
                                for (j = 0; j < count; j++)
                                    if (proj_list[j].new == proj.new)
                                        break;

                                if (j == count)
                                    proj_list[count++] = proj;
                            }
                        }
                        assert (count <= p->schema.count);

                        *p = *(PFla_project_ (L(lp), count, proj_list));
                        modified = true;
                        *(PFla_op_t **) PFarray_add (clean_up_list) = rp;
                    }
                    break;
                }
            }   break;

            case la_select:
                *p = *(select_ (eqjoin_unq (L(lp), rp, latt, ratt,
                                            p->sem.eqjoin_unq.res),
                                is_join_att(p, lp->sem.select.att)
                                   ? p->sem.eqjoin_unq.res
                                   : lp->sem.select.att));
                next_join = L(p);
                break;

            case la_pos_select:
                if (!PFprop_key (rp->prop, ratt) ||
                    !PFprop_subdom (rp->prop,
                                    PFprop_dom (lp->prop, latt),
                                    PFprop_dom (rp->prop, ratt)))
                    /* Ensure that the values of the left join argument
                       are a subset of the values of the right join argument
                       and that the right join argument is keyed. These
                       two tests make sure that we have exactly one match per
                       tuple in the left relation and thus the result of the
                       positional select operator stays stable. */
                    break;

                {
                    PFalg_proj_t *proj_list;
                    PFord_ordering_t sortby;
                    PFalg_att_t cur;
                    unsigned int count = 0;
                    /* create projection list */
                    proj_list = PFmalloc (lp->schema.count *
                                          sizeof (*(proj_list)));

                    for (unsigned int i = 0; i < lp->schema.count; i++) {
                        cur = lp->schema.items[i].name;
                        if (cur != latt)
                            proj_list[count++] = proj (cur, cur);
                    }
                    /* add join column with original name
                       the projection list */
                    proj_list[count++] = proj (latt,
                                               p->sem.eqjoin_unq.res);

                    /* copy sortby criteria and change name of
                       sort column to equi-join result if it is
                       also a join argument */
                    sortby = PFordering ();

                    for (unsigned int i = 0;
                         i < PFord_count (lp->sem.pos_sel.sortby);
                         i++) {
                        cur = PFord_order_col_at (
                                  lp->sem.pos_sel.sortby,
                                  i);
                        if (cur == latt)
                            cur = p->sem.eqjoin_unq.res;

                        sortby = PFord_refine (
                                     sortby,
                                     cur,
                                     PFord_order_dir_at (
                                         lp->sem.pos_sel.sortby,
                                         i));
                    }

                    /* make sure that frag and roots see
                       the new attribute node */
                    *p = *(pos_select (eqjoin_unq (L(lp), rp, latt, ratt,
                                                   p->sem.eqjoin_unq.res),
                                       lp->sem.pos_sel.pos,
                                       sortby,
                                       lp->sem.pos_sel.part
                                           ? is_join_att(p,
                                                         lp->sem.pos_sel.part)
                                                 ? p->sem.eqjoin_unq.res
                                                 : lp->sem.pos_sel.part
                                           : att_NULL));
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

                /* stop if this consistency check fails */
                if (p->sem.eqjoin_unq.res != latt &&
                    p->sem.eqjoin_unq.res != ratt)
                    break;

                /* We have to look at the union operator in conjunction
                   with its renaming projection operators underneath as
                   we otherwise state that the input is the same -- the
                   cleanup phase only looks at the column names to decide
                   how to resolve conflicts. */
                if (L(lp)->kind == la_project && R(lp)->kind == la_project) {
                    PFalg_att_t   latt1 = att_NULL,
                                  latt2 = att_NULL,
                                  res1,
                                  res2;
                    PFalg_proj_t *proj_list1,
                                 *proj_list2;
                    PFalg_att_t   cur;
                    unsigned int  count = 0;

                    /* create projection list */
                    proj_list1 = PFmalloc (p->schema.count *
                                           sizeof (*(proj_list1)));
                    proj_list2 = PFmalloc (p->schema.count *
                                           sizeof (*(proj_list2)));

                    /* Copy the projection list except for the join column. */
                    for (unsigned int i = 0; i < lp->schema.count; i++) {
                        cur = lp->schema.items[i].name;
                        /* consistency check to ensure that the projections
                           and the union operator are aligned */
                        assert (cur == L(lp)->sem.proj.items[i].new &&
                                cur == R(lp)->sem.proj.items[i].new);

                        if (cur == latt) {
                            /* remember old join names */
                            latt1 = L(lp)->sem.proj.items[i].old;
                            latt2 = R(lp)->sem.proj.items[i].old;
                        }
                        else {
                            proj_list1[count] = L(lp)->sem.proj.items[i];
                            proj_list2[count] = R(lp)->sem.proj.items[i];
                            count++;
                        }
                    }

                    /* Extend the projection list with the columns of
                       the right join input (again discarding the join
                       column. */
                    for (unsigned int i = 0; i < rp->schema.count; i++) {
                        cur = rp->schema.items[i].name;
                        if (cur != ratt) {
                            proj_list1[count] = proj (cur, cur);
                            proj_list2[count] = proj (cur, cur);
                            count++;
                        }
                        /* Make sure that we do not write more
                           projection than we allocated space for it. */
                        if (count == p->schema.count)
                            break;
                    }

                    /* Skip rewrite if we have duplicate columns. */
                    if (count != p->schema.count - 1)
                        break;

                    assert (latt1);
                    assert (latt2);

                    /* find out the correct join result columns */
                    res1 = latt1 < ratt ? latt1 : ratt;
                    res2 = latt2 < ratt ? latt2 : ratt;

                    /* fix the output schema by adjusting the result name */
                    proj_list1[count] = proj (p->sem.eqjoin_unq.res, res1);
                    proj_list2[count] = proj (p->sem.eqjoin_unq.res, res2);
                    count++;

                    /* This rewrite is only correct if the union operator
                       is implemented as a union all operation. */
                    *p = *disjunion (PFla_project_ (
                                         eqjoin_unq (LL(lp), rp, latt1, ratt,
                                                     res1),
                                         count, proj_list1),
                                     PFla_project_ (
                                         eqjoin_unq (RL(lp), rp, latt2, ratt,
                                                     res2),
                                         count, proj_list2));
                    modified = true;

                    join_pushdown_worker (LL(p), clean_up_list);
                    join_pushdown_worker (RL(p), clean_up_list);
                }
            } break;

            case la_fun_1to1:
                if (!is_join_att (p, lp->sem.fun_1to1.res)) {
                    PFalg_attlist_t refs;

                    refs.count = lp->sem.fun_1to1.refs.count;
                    refs.atts  = PFmalloc (refs.count *
                                           sizeof (*(refs.atts)));

                    for (unsigned int i = 0; i < refs.count; i++)
                        refs.atts[i] = is_join_att (
                                           p,
                                           lp->sem.fun_1to1.refs.atts[i])
                                       ? p->sem.eqjoin_unq.res
                                       : lp->sem.fun_1to1.refs.atts[i];

                    *p = *(fun_1to1 (eqjoin_unq (L(lp), rp, latt, ratt,
                                                 p->sem.eqjoin_unq.res),
                                     lp->sem.fun_1to1.kind,
                                     lp->sem.fun_1to1.res,
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
                if (!PFprop_key (rp->prop, ratt) ||
                    !PFprop_subdom (rp->prop,
                                    PFprop_dom (lp->prop, latt),
                                    PFprop_dom (rp->prop, ratt)))
                    /* Ensure that the values of the left join argument
                       are a subset of the values of the right join argument
                       and that the right join argument is keyed. These
                       two tests make sure that we have exactly one match per
                       tuple in the left relation and thus the result of the
                       rownum operator stays stable. */
                    break;

            case la_rowrank:
            case la_rank:
                if (!is_join_att (p, lp->sem.sort.res)) {
                    PFalg_proj_t *proj_list;
                    PFord_ordering_t sortby;
                    PFalg_att_t cur;
                    unsigned int count = 0;
                    PFla_op_t *eqjoin;
                    /* create projection list */
                    proj_list = PFmalloc (lp->schema.count *
                                          sizeof (*(proj_list)));

                    for (unsigned int i = 0; i < lp->schema.count; i++) {
                        cur = lp->schema.items[i].name;
                        if (cur != latt)
                            proj_list[count++] = proj (cur, cur);
                    }
                    /* add join column with original name
                       the projection list */
                    proj_list[count++] = proj (latt,
                                               p->sem.eqjoin_unq.res);

                    /* copy sortby criteria and change name of
                       sort column to equi-join result if it is
                       also a join argument */
                    sortby = PFordering ();

                    for (unsigned int i = 0;
                         i < PFord_count (lp->sem.sort.sortby);
                         i++) {
                        cur = PFord_order_col_at (
                                  lp->sem.sort.sortby,
                                  i);
                        if (cur == latt)
                            cur = p->sem.eqjoin_unq.res;

                        sortby = PFord_refine (
                                     sortby,
                                     cur,
                                     PFord_order_dir_at (
                                         lp->sem.sort.sortby,
                                         i));
                    }

                    eqjoin = eqjoin_unq (L(lp), rp, latt, ratt,
                                         p->sem.eqjoin_unq.res);

                    /* make sure that frag and roots see
                       the new attribute node */
                    if (lp->kind == la_rownum)
                        *p = *(rownum (eqjoin,
                                       lp->sem.sort.res,
                                       sortby,
                                       lp->sem.sort.part
                                           ? is_join_att(p,
                                                         lp->sem.sort.part)
                                                 ? p->sem.eqjoin_unq.res
                                                 : lp->sem.sort.part
                                           : att_NULL));
                    else if (lp->kind == la_rowrank)
                        *p = *(rowrank (eqjoin, lp->sem.sort.res, sortby));
                    else if (lp->kind == la_rank)
                        *p = *(rank (eqjoin, lp->sem.sort.res, sortby));

                    /* the schema of the new operator has to be pruned
                       to maintain the schema of the original rownum, rowrank,
                       or rank operator -- its pointer is replaced */
                    *lp = *(PFla_project_ (p, count, proj_list));

                    next_join = L(p);
                    break;
                }

                break;

                if (!PFprop_key (rp->prop, ratt) ||
                    !PFprop_subdom (rp->prop,
                                    PFprop_dom (lp->prop, latt),
                                    PFprop_dom (rp->prop, ratt)))
                    /* Ensure that the values of the left join argument
                       are a subset of the values of the right join argument
                       and that the right join argument is keyed. These
                       two tests make sure that we have exactly one match per
                       tuple in the left relation and thus the result of the
                       rank operator stays stable. */
                    break;

                if (!is_join_att (p, lp->sem.sort.res)) {
                    PFalg_proj_t *proj_list;
                    PFord_ordering_t sortby;
                    PFalg_att_t cur;
                    unsigned int count = 0;
                    /* create projection list */
                    proj_list = PFmalloc (lp->schema.count *
                                          sizeof (*(proj_list)));

                    for (unsigned int i = 0; i < lp->schema.count; i++) {
                        cur = lp->schema.items[i].name;
                        if (cur != latt)
                            proj_list[count++] = proj (cur, cur);
                    }
                    /* add join column with original name
                       the projection list */
                    proj_list[count++] = proj (latt,
                                               p->sem.eqjoin_unq.res);

                    /* copy sortby criteria and change name of
                       sort column to equi-join result if it is
                       also a join argument */
                    sortby = PFordering ();

                    for (unsigned int i = 0;
                         i < PFord_count (lp->sem.sort.sortby);
                         i++) {
                        cur = PFord_order_col_at (
                                  lp->sem.sort.sortby,
                                  i);
                        if (cur == latt)
                            cur = p->sem.eqjoin_unq.res;

                        sortby = PFord_refine (
                                     sortby,
                                     cur,
                                     PFord_order_dir_at (
                                         lp->sem.sort.sortby,
                                         i));
                    }

                    /* make sure that frag and roots see
                       the new attribute node */
                    *p = *(rank (eqjoin_unq (L(lp), rp, latt, ratt,
                                               p->sem.eqjoin_unq.res),
                                   lp->sem.sort.res,
                                   sortby));
                    /* the schema of the new rank operator has to
                       be pruned to maintain the schema of the original
                       rank operator -- its pointer is replaced */
                    *lp = *(PFla_project_ (p, count, proj_list));

                    next_join = L(p);
                    break;
                }

                break;

            case la_rowid:
                if (!PFprop_key (rp->prop, ratt) ||
                    !PFprop_subdom (rp->prop,
                                    PFprop_dom (lp->prop, latt),
                                    PFprop_dom (rp->prop, ratt)))
                    /* Ensure that the values of the left join argument
                       are a subset of the values of the right join argument
                       and that the right join argument is keyed. These
                       two tests make sure that we have exactly one match per
                       tuple in the left relation and thus the result of the
                       rowid operator stays stable. */
                    break;

                if (!is_join_att (p, lp->sem.rowid.res)) {
                    PFalg_proj_t *proj_list;
                    PFalg_att_t cur;
                    unsigned int count = 0;
                    /* create projection list */
                    proj_list = PFmalloc (lp->schema.count *
                                          sizeof (*(proj_list)));

                    for (unsigned int i = 0; i < lp->schema.count; i++) {
                        cur = lp->schema.items[i].name;
                        if (cur != latt)
                            proj_list[count++] = proj (cur, cur);
                    }
                    /* add join column with original name
                       the projection list */
                    proj_list[count++] = proj (latt,
                                               p->sem.eqjoin_unq.res);

                    /* make sure that frag and roots see
                       the new attribute node */
                    *p = *(rowid (eqjoin_unq (L(lp), rp, latt, ratt,
                                               p->sem.eqjoin_unq.res),
                                   lp->sem.rowid.res));
                    /* the schema of the new rowid operator has to
                       be pruned to maintain the schema of the original
                       rowid operator -- its pointer is replaced */
                    *lp = *(PFla_project_ (p, count, proj_list));

                    next_join = L(p);
                    break;
                }

                break;

            case la_type:
                if (!is_join_att (p, lp->sem.type.res)) {
                    *p = *(type (eqjoin_unq (L(lp), rp, latt, ratt,
                                             p->sem.eqjoin_unq.res),
                                 lp->sem.type.res,
                                 is_join_att(p, lp->sem.type.att)
                                    ? p->sem.eqjoin_unq.res
                                    : lp->sem.type.att,
                                 lp->sem.type.ty));
                    next_join = L(p);
                }
                break;

            case la_type_assert:
                if (!is_join_att (p, lp->sem.type.att)) {
                    *p = *(type_assert_pos (eqjoin_unq (L(lp), rp, latt, ratt,
                                                        p->sem.eqjoin_unq.res),
                                            lp->sem.type.att,
                                            lp->sem.type.ty));
                    next_join = L(p);
                }
                break;

            case la_cast:
                if (!is_join_att (p, lp->sem.type.res)) {
                    *p = *(cast (eqjoin_unq (L(lp), rp, latt, ratt,
                                             p->sem.eqjoin_unq.res),
                                 lp->sem.type.res,
                                 is_join_att(p, lp->sem.type.att)
                                    ? p->sem.eqjoin_unq.res
                                    : lp->sem.type.att,
                                 lp->sem.type.ty));
                    next_join = L(p);
                }
                break;

            case la_step_join:
                if (!is_join_att (p, lp->sem.step.item_res)) {
                    *p = *(step_join (
                               L(lp),
                               eqjoin_unq (R(lp), rp, latt, ratt,
                                           p->sem.eqjoin_unq.res),
                               lp->sem.step.spec,
                               lp->sem.step.level,
                               is_join_att(p, lp->sem.step.item)
                                  ? p->sem.eqjoin_unq.res
                                  : lp->sem.step.item,
                               lp->sem.step.item_res));
                    next_join = R(p);
                }
                break;

            case la_guide_step_join:
                if (!is_join_att (p, lp->sem.step.item_res)) {
                    *p = *(guide_step_join (
                               L(lp),
                               eqjoin_unq (R(lp), rp, latt, ratt,
                                           p->sem.eqjoin_unq.res),
                               lp->sem.step.spec,
                               lp->sem.step.guide_count,
                               lp->sem.step.guides,
                               lp->sem.step.level,
                               is_join_att(p, lp->sem.step.item)
                                  ? p->sem.eqjoin_unq.res
                                  : lp->sem.step.item,
                               lp->sem.step.item_res));
                    next_join = R(p);
                }
                break;

            case la_doc_index_join:
                if (!is_join_att (p, lp->sem.doc_join.item_res)) {
                    *p = *(doc_index_join (
                               L(lp),
                               eqjoin_unq (R(lp), rp, latt, ratt,
                                           p->sem.eqjoin_unq.res),
                               lp->sem.doc_join.kind,
                               is_join_att(p, lp->sem.doc_join.item)
                                  ? p->sem.eqjoin_unq.res
                                  : lp->sem.doc_join.item,
                               lp->sem.doc_join.item_res,
                               is_join_att(p, lp->sem.doc_join.item_doc)
                                  ? p->sem.eqjoin_unq.res
                                  : lp->sem.doc_join.item_doc));
                    next_join = R(p);
                }
                break;

            case la_doc_tbl:
                if (!is_join_att (p, lp->sem.doc_tbl.res)) {
                    *p = *(doc_tbl (eqjoin_unq (L(lp), rp, latt, ratt,
                                                p->sem.eqjoin_unq.res),
                                    lp->sem.doc_tbl.res,
                                    is_join_att(p, lp->sem.doc_tbl.att)
                                       ? p->sem.eqjoin_unq.res
                                       : lp->sem.doc_tbl.att));
                    next_join = L(p);
                }
                break;

            case la_doc_access:
                if (!is_join_att (p, lp->sem.doc_access.res)) {
                    *p = *(doc_access (L(lp),
                                       eqjoin_unq (R(lp), rp, latt, ratt,
                                                   p->sem.eqjoin_unq.res),
                                       lp->sem.doc_access.res,
                                       is_join_att(p, lp->sem.doc_access.att)
                                          ? p->sem.eqjoin_unq.res
                                          : lp->sem.doc_access.att,
                                       lp->sem.doc_access.doc_col));
                    next_join = R(p);
                }
                break;

            case la_error:
                if (!PFprop_key (rp->prop, ratt) ||
                    !PFprop_subdom (rp->prop,
                                    PFprop_dom (lp->prop, latt),
                                    PFprop_dom (rp->prop, ratt)))
                    /* Ensure that the values of the left join argument
                       are a subset of the values of the right join argument
                       and that the right join argument is keyed. These
                       two tests make sure that we have exactly one match per
                       tuple in the left relation and thus the result of the
                       error operator stays stable. */
                    break;

                if (!is_join_att (p, lp->sem.err.att)) {
                    *p = *(PFla_error_ (eqjoin_unq (L(lp), rp, latt, ratt,
                                                    p->sem.eqjoin_unq.res),
                                        lp->sem.err.att,
                                        PFprop_type_of (lp, lp->sem.err.att)));
                    next_join = L(p);
                    break;
                }
                break;

            case la_cond_err:
                /* this breaks proxy generation - thus don't
                   rewrite conditional errors */
                /*
                *p = *(cond_err (eqjoin_unq (L(lp), rp, latt, ratt,
                                             p->sem.eqjoin_unq.res),
                                 R(lp),
                                 lp->sem.err.att,
                                 lp->sem.err.str));
                next_join = L(p);
                */
                break;

            case la_nil:
                break;

            case la_trace:
                if (!PFprop_key (rp->prop, ratt) ||
                    !PFprop_subdom (rp->prop,
                                    PFprop_dom (lp->prop, latt),
                                    PFprop_dom (rp->prop, ratt)))
                    /* Ensure that the values of the left join argument
                       are a subset of the values of the right join argument
                       and that the right join argument is keyed. These
                       two tests make sure that we have exactly one match per
                       tuple in the left relation and thus the result of the
                       trace operator stays stable. */
                    break;

            {
                PFalg_proj_t *proj_list;
                PFalg_att_t cur;
                unsigned int count = 0;
                /* create projection list */
                proj_list = PFmalloc (lp->schema.count *
                                      sizeof (*(proj_list)));

                for (unsigned int i = 0; i < lp->schema.count; i++) {
                    cur = lp->schema.items[i].name;
                    if (cur != latt)
                        proj_list[count++] = proj (cur, cur);
                }
                /* add join column with its original name
                   to the projection list */
                proj_list[count++] = proj (latt,
                                           p->sem.eqjoin_unq.res);

                *p = *(trace (eqjoin_unq (L(lp), rp, latt, ratt,
                                          p->sem.eqjoin_unq.res),
                              R(lp),
                              is_join_att(p, lp->sem.iter_pos_item.iter)
                                  ? p->sem.eqjoin_unq.res
                                  : lp->sem.iter_pos_item.iter,
                              is_join_att(p, lp->sem.iter_pos_item.pos)
                                  ? p->sem.eqjoin_unq.res
                                  : lp->sem.iter_pos_item.pos,
                              is_join_att(p, lp->sem.iter_pos_item.item)
                                  ? p->sem.eqjoin_unq.res
                                  : lp->sem.iter_pos_item.item));

                /* the schema of the new trace operator has to
                   be pruned to maintain the schema of the original
                   trace operator -- its pointer is replaced */
                *lp = *(PFla_project_ (p, count, proj_list));

                next_join = L(p);
            }   break;

            case la_trace_msg:
                /* this operator will always be referenced
                   by a la_trace operator */
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
 * It returns the invalid column name att_NULL if the mapping
 * did not result in a (new) name.
 */
static PFalg_att_t
map_name (PFla_op_t *p, PFalg_att_t att)
{
    switch (p->kind) {
        case la_serialize_seq:
        case la_serialize_rel:
        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
        case la_eqjoin:
        case la_intersect:
        case la_difference:
        case la_distinct:
        case la_to:
        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_count:
        case la_seqty1:
        case la_all:
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
        case la_fragment:
        case la_frag_extract:
        case la_frag_union:
        case la_empty_frag:
        case la_string_join:
        case la_nil:
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
        case la_cross_mvd:
            /* join can't be pushed down anyway */
            return att_NULL;

        case la_cross:
        case la_select:
        case la_pos_select:
        case la_disjunion:
        case la_eqjoin_unq:
        case la_semijoin:
        case la_thetajoin:
        case la_trace:
        case la_error:
        case la_cond_err:
            /* name does not change */
            break;

        case la_project:
            for (unsigned int i = 0; i < p->sem.proj.count; i++)
                if (p->sem.proj.items[i].new == att)
                    return p->sem.proj.items[i].old;

        case la_attach:
            if (att == p->sem.attach.res) return att_NULL;
            break;
        case la_fun_1to1:
            if (att == p->sem.fun_1to1.res) return att_NULL;
            break;
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
            if (att == p->sem.binary.res) return att_NULL;
            break;
        case la_bool_not:
            if (att == p->sem.unary.res) return att_NULL;
            break;
        case la_rownum:
        case la_rowrank:
        case la_rank:
            if (att == p->sem.sort.res) return att_NULL;
            break;
        case la_rowid:
            if (att == p->sem.rowid.res) return att_NULL;
            break;
        case la_type:
        case la_cast:
        case la_type_assert:
            if (att == p->sem.type.res) return att_NULL;
            break;
        case la_step_join:
        case la_guide_step_join:
            if (att == p->sem.step.item_res) return att_NULL;
            break;
        case la_doc_index_join:
            if (att == p->sem.doc_join.item_res) return att_NULL;
            break;
        case la_doc_tbl:
            if (att == p->sem.doc_tbl.res) return att_NULL;
            break;
        case la_doc_access:
            if (att == p->sem.doc_access.res) return att_NULL;
            break;
    }
    return att;
}

/**
 * mark_left_path follows a path based on an initial
 * column name and marks all operators on the path as
 * being LEFT children of the join.
 */
static void
mark_left_path (PFla_op_t *p, PFalg_att_t att)
{
    assert (p);

    if (LEFT(p))
       return;

    if (p->kind == la_frag_union ||
        p->kind == la_empty_frag)
        return;
    else if (p->kind == la_eqjoin_unq &&
             att == p->sem.eqjoin_unq.res) {
        mark_left_path (L(p), p->sem.eqjoin_unq.att1);
        mark_left_path (R(p), p->sem.eqjoin_unq.att2);
        LEFT(p) = true;
        return;
    }
    else if (p->kind == la_cond_err) {
        mark_left_path (L(p), att);
        LEFT(p) = true;
        return;
    }

    att = map_name (p, att);
    if (!att) return;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        for (unsigned int j = 0; j < p->child[i]->schema.count; j++)
            if (att == p->child[i]->schema.items[j].name) {
                mark_left_path (p->child[i], att);
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
mark_right_path (PFla_op_t *p, PFalg_att_t att)
{
    assert (p);

    if (RIGHT(p))
       return;

    if (p->kind == la_frag_union ||
        p->kind == la_empty_frag)
        return;
    else if (p->kind == la_eqjoin_unq &&
             att == p->sem.eqjoin_unq.res) {
        mark_right_path (L(p), p->sem.eqjoin_unq.att1);
        mark_right_path (R(p), p->sem.eqjoin_unq.att2);
        LEFT(p) = true;
        return;
    }
    else if (p->kind == la_cond_err) {
        mark_right_path (L(p), att);
        LEFT(p) = true;
        return;
    }

    att = map_name (p, att);

    if (!att) return;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        for (unsigned int j = 0; j < p->child[i]->schema.count; j++)
            if (att == p->child[i]->schema.items[j].name) {
                mark_right_path (p->child[i], att);
                break;
            }

    RIGHT(p) = true;
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
        /* copy of code located in algebra/opt/opt_general.brg */
        PFalg_proj_t *proj = PFmalloc (p->schema.count *
                                           sizeof (PFalg_proj_t));

        unsigned int i, j, count = 0;
        for (i = 0; i < p->schema.count; i++)
            for (j = 0; j < L(p)->schema.count; j++)
                if (p->sem.proj.items[i].old ==
                    L(p)->sem.proj.items[j].new) {
                    proj[count++] = PFalg_proj (
                                        p->sem.proj.items[i].new,
                                        L(p)->sem.proj.items[j].old);
                    break;
                }

        /* ensure that at least one column remains! */
        if (!count)
            for (j = 0; j < L(p)->schema.count; j++)
                if (p->sem.proj.items[0].old ==
                    L(p)->sem.proj.items[j].new) {
                    proj[count++] = PFalg_proj (
                                        p->sem.proj.items[0].new,
                                        L(p)->sem.proj.items[j].old);
                    break;
                }

        *p = *(PFla_project_ (LL(p), count, proj));
        modified = true;
    }

    /* remove unnecessary joins
       (where both children references point to the same node) */
    if (p->kind == la_eqjoin_unq &&
        L(p) == R(p) &&
        p->sem.eqjoin_unq.att1 == p->sem.eqjoin_unq.att2 &&
        PFprop_key (L(p)->prop, p->sem.eqjoin_unq.att1)) {
        /* the join does nothing -- it only applies a key-join
           with itself and the schema stays the same.
           Thus replace it by a dummy projection */
        PFalg_proj_t *proj_list = PFmalloc (p->schema.count *
                                            sizeof (PFalg_proj_t));

        for (unsigned int i = 0; i < p->schema.count; i++)
            proj_list[i] = proj (p->schema.items[i].name,
                                 p->schema.items[i].name);

        *p = *(PFla_project_ (L(p), p->schema.count, proj_list));
        modified = true;
    }

    /* We need to make sure that the eqjoin operator does not reference
       itself after the rewrite. Thus we make sure that no forbidden rewrite
       happens by marking all reachable nodes in the LEFT and the
       RIGHT subtree and prohibit any rewrite for algebra nodes that are
       marked LEFT and RIGHT. */
    if (p->kind == la_eqjoin_unq) {
        /* mark nodes in the left child as 'LEFT' */
        if (PFprop_key (p->prop, p->sem.eqjoin.att1) &&
            PFprop_subdom (p->prop,
                           PFprop_dom (R(p)->prop, p->sem.eqjoin.att2),
                           PFprop_dom (L(p)->prop, p->sem.eqjoin.att1)))
            /* if the left join argument is key and a super domain
               of the right one we can be sure that the left side will
               never rewrite any node in the right subtree except for
               the nodes on the path of the join argument. */
            mark_left_path (L(p), p->sem.eqjoin.att1);
        else
            mark_left_subdag (L(p));

        /* mark nodes in the right child as 'RIGHT' */
        if (PFprop_key (p->prop, p->sem.eqjoin.att2) &&
            PFprop_subdom (p->prop,
                           PFprop_dom (L(p)->prop, p->sem.eqjoin.att1),
                           PFprop_dom (R(p)->prop, p->sem.eqjoin.att2)))
            /* if the right join argument is key and a super domain
               of the left one we can be sure that the right side will
               never rewrite any node in the left subtree except for
               the nodes on the path of the join argument. */
            mark_right_path (R(p), p->sem.eqjoin.att2);
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
 * clean_up_eqjoin introduces additional project operators
 * whenever there are duplicate columns that are not join
 * attributes.
 */
static void
clean_up_eqjoin (PFla_op_t *p)
{
    unsigned int i;

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* clean up equ-joins in the subtrees */
    for (i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        clean_up_eqjoin (p->child[i]);

    if (p->kind == la_eqjoin_unq) {
        PFla_op_t *left, *right;
        PFalg_proj_t *proj_list;
        PFalg_att_t cur;
        unsigned int j, count;
        bool prune;

        /* as the equi-join may be pushed down that far that it
           references the same relation we have to remove
           every duplicate except the first join column from
           the first operand and project away the first join column
           from the second operand. */

        /* first operand */

        /* check for duplicate columns */
        prune = false;
        for (i = 0; i < L(p)->schema.count; i++) {
            cur = L(p)->schema.items[i].name;
            if (cur != p->sem.eqjoin_unq.att1)
                for (j = 0; j < R(p)->schema.count; j++)
                    if (R(p)->schema.items[j].name == cur) {
                        prune = true;
                        break;
                    }
            if (prune)
                break;
        }

        /* if duplicate columns appeared add a projection */
        if (prune) {
            count = 0;
            /* create projection list (at maximum it has the same size) */
            proj_list = PFmalloc ((L(p)->schema.count)
                                  * sizeof (*(proj_list)));

            /* keep first join attribute */
            proj_list[count++] = proj (p->sem.eqjoin_unq.att1,
                                       p->sem.eqjoin_unq.att1);

            /* create projection list */
            for (i = 0; i < L(p)->schema.count; i++) {
                cur = L(p)->schema.items[i].name;
                for (j = 0; j < R(p)->schema.count; j++)
                    if (R(p)->schema.items[j].name == cur)
                        /* we detected a cloned column */ break;

                /* No clone column found ... */
                if (j == R(p)->schema.count &&
                    /* ... and it is not a join argument ... */
                    !is_join_att (p, cur))
                    /* ... then add it to the projection list. */
                    proj_list[count++] = proj (cur, cur);
            }
            left = PFla_project_ (L(p), count, proj_list);
        }
        else
            left = L(p);

        /* second operand */

        /* check for duplicate columns */
        prune = false;
        if (p->sem.eqjoin_unq.att1 != p->sem.eqjoin_unq.att2)
            for (i = 0; i < R(p)->schema.count; i++)
                /* The first join column appeared - we have to introduce
                   the projection. */
                if (R(p)->schema.items[i].name == p->sem.eqjoin_unq.att1) {
                    prune = true;
                    break;
                }

        /* if duplicate columns appeared add a projection */
        if (prune) {
            count = 0;
            /* create projection list (at maximum it has the same size) */
            proj_list = PFmalloc ((R(p)->schema.count)
                                  * sizeof (*(proj_list)));

            /* keep second join attribute */
            proj_list[count++] = proj (p->sem.eqjoin_unq.att2,
                                       p->sem.eqjoin_unq.att2);

            for (i = 0; i < R(p)->schema.count; i++) {
                cur = R(p)->schema.items[i].name;
                /* Throw out join columns we already added it
                   separately */
                if (!is_join_att (p, cur))
                    proj_list[count++] = proj (cur, cur);
            }
            right = PFla_project_ (R(p), count, proj_list);
        }
        else
            right = R(p);

        /* replace eqjoin if a projection was introduced */
        if (left != L(p) || right != R(p))
            *p = *(eqjoin_unq (left, right,
                               p->sem.eqjoin_unq.att1,
                               p->sem.eqjoin_unq.att2,
                               p->sem.eqjoin_unq.res));
    }
}

/**
 * Invoke algebra optimization.
 */
PFla_op_t *
PFalgopt_join_pd (PFla_op_t *root)
{
    PFarray_t *clean_up_list = PFarray (sizeof (PFla_op_t *), 40);
    unsigned int tries = 0, max_tries = 0;
    bool modified = true;

    /* Optimize algebra tree */
    while (modified || tries < max_tries) {
        PFprop_infer_key (root);
        /* key property inference already requires
           the domain property inference for native
           types. Thus we can skip it:
        PFprop_infer_nat_dom (root);
        */

        modified = join_pushdown (root, clean_up_list);
        PFla_dag_reset (root);
        if (!modified) tries++;
    }
    /* remove duplicate columns introduced
       during rewriting */
    clean_up_eqjoin (root);
    PFla_dag_reset (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
