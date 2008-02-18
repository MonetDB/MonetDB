/**
 * @file
 *
 * Introduce thetajoin operators.
 *
 * In this phase we transform equi-join operators into theta-joins.
 * While both operators initally do the same the new theta-join operator
 * allows us to create a new join rewrite strategy. Instead of pushing
 * down theta-joins (like equi-joins) we move them as far up as possible.
 *
 * To choose which equi-join is transformed into a theta-join we start
 * with the goal to transform a selection into a join predicate. Starting
 * from selection operators we first find the corresponding comparison
 * operator(s) (if any). These comparison operators than provide a left
 * and a right join argument which we follow until the origin of the
 * left and the right comparison argument is unified in a single column.
 *    At this place we have found the first common scope of both comparison
 * sides. We now only have to transform the first equi-join operator on our
 * way back to the root of the traversal (the select operator) into a
 * theta-join operator.
 *    In a following rewrite phase the new theta-join operator will
 * eventually become a real theta-join that has to evaluate a conjunctive
 * join.
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

#include "la_thetajoin.h"
#include "properties.h"
#include "mem.h"          /* PFmalloc() */
#include "oops.h"
#include "alg_dag.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define SEEN(n)       (n)->bit_dag
#define EDGE(n)       (n)->state_label

#define BOOL_COLS(n)  (n)->prop->icols
#define LEFT_COLS(n)  (n)->prop->l_icols
#define RIGHT_COLS(n) (n)->prop->r_icols

/**
 * Returns the intersection of an column list @a cols and a schema
 * @a schema_ocols
 */
static PFalg_att_t
intersect_ocol (PFalg_att_t cols, PFalg_schema_t schema_ocols)
{
    PFalg_att_t ocols = 0;

    /* intersect attributes */
    for (unsigned int j = 0; j < schema_ocols.count; j++)
        ocols |= schema_ocols.items[j].name;

    return cols & ocols;
}

/**
 * Returns union of two column lists
 */
static PFalg_att_t
union_ (PFalg_att_t a, PFalg_att_t b)
{
    return a | b;
}

/**
 * Returns the difference of two column lists
 */
static PFalg_att_t
diff (PFalg_att_t a, PFalg_att_t b)
{
    return a & (~b);
}

/**
 * Worker function for find_join (). It traverses
 * the DAG starting from a select operator and infers
 * three different informations:
 *  * bool_cols: The boolean column name(s) consumed
 *    by the select operator. If a boolean marked column
 *    hits a comparison operator new left and right
 *    columns are introduced.
 *  * left_cols: Column (names) that are necessary to
 *    provide the left input of a comparison.
 *  * right_cols: Column (names) that are necessary to
 *    provide the right input of a comparison.
 *
 * This worker ends if the left and the right column lists
 * overlap. In this case we have found something similar
 * to the lowest common scope (where a split appeared).
 */
static bool
find_join_worker (PFla_op_t *n,
                  PFla_op_t **join,
                  PFalg_att_t bool_cols,
                  PFalg_att_t left_cols,
                  PFalg_att_t right_cols)
{
    assert (n);

    /* collect all input columns */
    BOOL_COLS(n)  = union_ (BOOL_COLS(n), bool_cols);
    LEFT_COLS(n)  = union_ (LEFT_COLS(n), left_cols);
    RIGHT_COLS(n) = union_ (RIGHT_COLS(n), right_cols);

    EDGE(n)++;
    /* nothing to do if we haven't collected
       all incoming column lists of that node */
    if (EDGE(n) < PFprop_refctr (n))
        return false;

    /* remove all unnecessary columns from our structure */
    BOOL_COLS(n)  = intersect_ocol (BOOL_COLS(n), n->schema);
    LEFT_COLS(n)  = intersect_ocol (LEFT_COLS(n), n->schema);
    RIGHT_COLS(n) = intersect_ocol (RIGHT_COLS(n), n->schema);

    /* If both the left side and the right side of a comparison
       reference the same column in a rowid operator
       we have (hopefully) reached our goal. */
    if (n->kind == la_rowid &&
        LEFT_COLS(n) && RIGHT_COLS(n) &&
        ((LEFT_COLS(n) & RIGHT_COLS(n)) == LEFT_COLS(n) ||
         (LEFT_COLS(n) & RIGHT_COLS(n)) == RIGHT_COLS(n)))
        return true;

    switch (n->kind) {
        case la_serialize_seq:
        case la_serialize_rel:
            assert (!"this operator should never occur");
            break;

        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
            break;

        case la_attach:
            BOOL_COLS(n)  = diff (BOOL_COLS(n),  n->sem.attach.res);
            LEFT_COLS(n)  = diff (LEFT_COLS(n),  n->sem.attach.res);
            RIGHT_COLS(n) = diff (RIGHT_COLS(n), n->sem.attach.res);
            break;

        case la_cross:
        case la_semijoin:
        case la_select:
        case la_pos_select:
        case la_disjunion:
        case la_intersect:
        case la_difference:
        case la_distinct:
            break;

        case la_eqjoin:
        case la_thetajoin:
            /* make sure to remember the last join node where we split
               up the left and the right cols */
            if ((intersect_ocol (LEFT_COLS(n), L(n)->schema) ^
                 intersect_ocol (LEFT_COLS(n), R(n)->schema))
                &&
                (intersect_ocol (RIGHT_COLS(n), L(n)->schema) ^
                 intersect_ocol (RIGHT_COLS(n), R(n)->schema))
                &&
                ((intersect_ocol (LEFT_COLS(n), L(n)->schema) &&
                  intersect_ocol (RIGHT_COLS(n), R(n)->schema))
                 ^
                 (intersect_ocol (RIGHT_COLS(n), L(n)->schema) &&
                  intersect_ocol (LEFT_COLS(n), R(n)->schema))))
                *join = n;
            break;

        case la_project:
        {
            PFalg_att_t bool_cols  = 0,
                        left_cols  = 0,
                        right_cols = 0;

            /* rename columns from new to old */
            for (unsigned int i = 0; i < n->sem.proj.count; i++) {
                if (BOOL_COLS(n) & n->sem.proj.items[i].new)
                    bool_cols = union_ (bool_cols, n->sem.proj.items[i].old);
                if (LEFT_COLS(n) & n->sem.proj.items[i].new)
                    left_cols = union_ (left_cols, n->sem.proj.items[i].old);
                if (RIGHT_COLS(n) & n->sem.proj.items[i].new)
                    right_cols = union_ (right_cols, n->sem.proj.items[i].old);
            }
            BOOL_COLS(n)  = bool_cols;
            LEFT_COLS(n)  = left_cols;
            RIGHT_COLS(n) = right_cols;
        }   break;

        case la_fun_1to1:
            BOOL_COLS(n) = diff (BOOL_COLS(n), n->sem.fun_1to1.res);
            /* ignore fn:contains() here as we do not know
               how to implement it in a theta-join */

            if (LEFT_COLS(n) & n->sem.fun_1to1.res) {
                LEFT_COLS(n) = diff (LEFT_COLS(n), n->sem.fun_1to1.res);
                for (unsigned int i = 0; i < n->sem.fun_1to1.refs.count; i++)
                    LEFT_COLS(n) = union_ (LEFT_COLS(n),
                                           n->sem.fun_1to1.refs.atts[i]);
            }

            if (RIGHT_COLS(n) & n->sem.fun_1to1.res) {
                RIGHT_COLS(n) = diff (RIGHT_COLS(n), n->sem.fun_1to1.res);
                for (unsigned int i = 0; i < n->sem.fun_1to1.refs.count; i++)
                    RIGHT_COLS(n) = union_ (RIGHT_COLS(n),
                                            n->sem.fun_1to1.refs.atts[i]);
            }
            break;

        case la_num_eq:
        case la_num_gt:
            if (BOOL_COLS(n) & n->sem.binary.res) {
                BOOL_COLS(n)  = diff   (BOOL_COLS(n),  n->sem.binary.res);
                LEFT_COLS(n)  = union_ (LEFT_COLS(n),  n->sem.binary.att1);
                RIGHT_COLS(n) = union_ (RIGHT_COLS(n), n->sem.binary.att2);
            }

            if (LEFT_COLS(n) & n->sem.binary.res) {
                LEFT_COLS(n) = diff   (LEFT_COLS(n), n->sem.binary.res);
                LEFT_COLS(n) = union_ (LEFT_COLS(n), n->sem.binary.att1);
                LEFT_COLS(n) = union_ (LEFT_COLS(n), n->sem.binary.att2);
            }

            if (RIGHT_COLS(n) & n->sem.binary.res) {
                RIGHT_COLS(n) = diff   (RIGHT_COLS(n), n->sem.binary.res);
                RIGHT_COLS(n) = union_ (RIGHT_COLS(n), n->sem.binary.att1);
                RIGHT_COLS(n) = union_ (RIGHT_COLS(n), n->sem.binary.att2);
            }
            break;

        case la_bool_and:
            if (BOOL_COLS(n) & n->sem.binary.res) {
                BOOL_COLS(n) = diff   (BOOL_COLS(n), n->sem.binary.res);
                BOOL_COLS(n) = union_ (BOOL_COLS(n), n->sem.binary.att1);
                BOOL_COLS(n) = union_ (BOOL_COLS(n), n->sem.binary.att2);
            }

            if (LEFT_COLS(n) & n->sem.binary.res) {
                LEFT_COLS(n) = diff   (LEFT_COLS(n), n->sem.binary.res);
                LEFT_COLS(n) = union_ (LEFT_COLS(n), n->sem.binary.att1);
                LEFT_COLS(n) = union_ (LEFT_COLS(n), n->sem.binary.att2);
            }

            if (RIGHT_COLS(n) & n->sem.binary.res) {
                RIGHT_COLS(n) = diff   (RIGHT_COLS(n), n->sem.binary.res);
                RIGHT_COLS(n) = union_ (RIGHT_COLS(n), n->sem.binary.att1);
                RIGHT_COLS(n) = union_ (RIGHT_COLS(n), n->sem.binary.att2);
            }
            break;

        case la_bool_or:
            BOOL_COLS(n) = diff (BOOL_COLS(n), n->sem.binary.res);

            if (LEFT_COLS(n) & n->sem.binary.res) {
                LEFT_COLS(n) = diff   (LEFT_COLS(n), n->sem.binary.res);
                LEFT_COLS(n) = union_ (LEFT_COLS(n), n->sem.binary.att1);
                LEFT_COLS(n) = union_ (LEFT_COLS(n), n->sem.binary.att2);
            }

            if (RIGHT_COLS(n) & n->sem.binary.res) {
                RIGHT_COLS(n) = diff   (RIGHT_COLS(n), n->sem.binary.res);
                RIGHT_COLS(n) = union_ (RIGHT_COLS(n), n->sem.binary.att1);
                RIGHT_COLS(n) = union_ (RIGHT_COLS(n), n->sem.binary.att2);
            }
            break;

        case la_bool_not:
            if (BOOL_COLS(n) & n->sem.unary.res) {
                BOOL_COLS(n) = diff   (BOOL_COLS(n), n->sem.unary.res);
                BOOL_COLS(n) = union_ (BOOL_COLS(n), n->sem.unary.att);
            }

            if (LEFT_COLS(n) & n->sem.unary.res) {
                LEFT_COLS(n) = diff   (LEFT_COLS(n), n->sem.unary.res);
                LEFT_COLS(n) = union_ (LEFT_COLS(n), n->sem.unary.att);
            }

            if (RIGHT_COLS(n) & n->sem.unary.res) {
                RIGHT_COLS(n) = diff   (RIGHT_COLS(n), n->sem.unary.res);
                RIGHT_COLS(n) = union_ (RIGHT_COLS(n), n->sem.unary.att);
            }
            break;

        case la_to:
            if (LEFT_COLS(n) & n->sem.binary.res) {
                LEFT_COLS(n) = diff (LEFT_COLS(n), n->sem.binary.res);
            }
            if (RIGHT_COLS(n) & n->sem.binary.res) {
                RIGHT_COLS(n) = diff (RIGHT_COLS(n), n->sem.binary.res);
            }
            break;

        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_seqty1:
        case la_all:
            BOOL_COLS(n)  = diff (BOOL_COLS(n),  n->sem.aggr.res);

            if (LEFT_COLS(n) & n->sem.aggr.res)
                LEFT_COLS(n) = union_ (LEFT_COLS(n), n->sem.aggr.att);
            if (RIGHT_COLS(n) & n->sem.aggr.res)
                RIGHT_COLS(n) = union_ (RIGHT_COLS(n), n->sem.aggr.att);
            LEFT_COLS(n)  = diff (LEFT_COLS(n),  n->sem.aggr.res);
            RIGHT_COLS(n) = diff (RIGHT_COLS(n), n->sem.aggr.res);

        case la_count:
            /* keep partition columns for LEFT_COLS and RIGHT_COLS */

            /* BOOL_COLS can never occur as partitioning as there
               are no boolean partition criteria */
            break;

        case la_rownum:
        case la_rowrank:
        case la_rank:
            if (LEFT_COLS(n) & n->sem.sort.res) {
                LEFT_COLS(n) = diff   (LEFT_COLS(n), n->sem.sort.res);

                for (unsigned int i = 0;
                     i < PFord_count (n->sem.sort.sortby);
                     i++)
                    LEFT_COLS(n) = union_ (LEFT_COLS(n),
                                           PFord_order_col_at (
                                               n->sem.sort.sortby, i));

                /* only infer part if available */
                if (n->sem.sort.part != att_NULL)
                    LEFT_COLS(n) = union_ (LEFT_COLS(n),
                                           n->sem.sort.part);
            }

            if (RIGHT_COLS(n) & n->sem.sort.res) {
                RIGHT_COLS(n) = diff   (RIGHT_COLS(n), n->sem.sort.res);

                for (unsigned int i = 0;
                     i < PFord_count (n->sem.sort.sortby);
                     i++)
                    RIGHT_COLS(n) = union_ (RIGHT_COLS(n),
                                            PFord_order_col_at (
                                                n->sem.sort.sortby, i));

                /* only infer part if available */
                if (n->sem.sort.part != att_NULL)
                    RIGHT_COLS(n) = union_ (RIGHT_COLS(n),
                                            n->sem.sort.part);
            }
            break;

        case la_rowid:
            LEFT_COLS(n)  = diff   (LEFT_COLS(n), n->sem.rowid.res);
            RIGHT_COLS(n) = diff   (RIGHT_COLS(n), n->sem.rowid.res);
            break;

        case la_type:
        case la_cast:
            BOOL_COLS(n) = diff (BOOL_COLS(n), n->sem.type.res);

            if (LEFT_COLS(n) & n->sem.type.res) {
                LEFT_COLS(n) = diff   (LEFT_COLS(n), n->sem.type.res);
                LEFT_COLS(n) = union_ (LEFT_COLS(n), n->sem.type.att);
            }

            if (RIGHT_COLS(n) & n->sem.type.res) {
                RIGHT_COLS(n) = diff   (RIGHT_COLS(n), n->sem.type.res);
                RIGHT_COLS(n) = union_ (RIGHT_COLS(n), n->sem.type.att);
            }
            break;

        case la_type_assert:
            break;

        case la_step:
        case la_step_join:
        case la_guide_step:
        case la_guide_step_join:
            if (LEFT_COLS(n) & n->sem.step.item_res) {
                LEFT_COLS(n) = diff   (LEFT_COLS(n), n->sem.step.item_res);
                LEFT_COLS(n) = union_ (LEFT_COLS(n), n->sem.step.item);
            }
            if (RIGHT_COLS(n) & n->sem.step.item_res) {
                RIGHT_COLS(n) = diff   (RIGHT_COLS(n), n->sem.step.item_res);
                RIGHT_COLS(n) = union_ (RIGHT_COLS(n), n->sem.step.item);
            }
            break;

        case la_doc_index_join:
            if (LEFT_COLS(n) & n->sem.doc_join.item_res) {
                LEFT_COLS(n) = diff   (LEFT_COLS(n), n->sem.doc_join.item_res);
                LEFT_COLS(n) = union_ (LEFT_COLS(n), n->sem.doc_join.item);
                LEFT_COLS(n) = union_ (LEFT_COLS(n), n->sem.doc_join.item_doc);
            }
            if (RIGHT_COLS(n) & n->sem.doc_join.item_res) {
                RIGHT_COLS(n) = diff   (RIGHT_COLS(n),
                                        n->sem.doc_join.item_res);
                RIGHT_COLS(n) = union_ (RIGHT_COLS(n), n->sem.doc_join.item);
                RIGHT_COLS(n) = union_ (RIGHT_COLS(n),
                                        n->sem.doc_join.item_doc);
            }
            break;

        case la_doc_tbl:
            if (LEFT_COLS(n) & n->sem.doc_tbl.res) {
                LEFT_COLS(n) = diff   (LEFT_COLS(n), n->sem.doc_tbl.res);
                LEFT_COLS(n) = union_ (LEFT_COLS(n), n->sem.doc_tbl.att);
            }
            if (RIGHT_COLS(n) & n->sem.doc_tbl.res) {
                RIGHT_COLS(n) = diff   (RIGHT_COLS(n), n->sem.doc_tbl.res);
                RIGHT_COLS(n) = union_ (RIGHT_COLS(n), n->sem.doc_tbl.att);
            }
            break;

        case la_doc_access:
            if (LEFT_COLS(n) & n->sem.doc_access.res) {
                LEFT_COLS(n) = diff   (LEFT_COLS(n), n->sem.doc_access.res);
                LEFT_COLS(n) = union_ (LEFT_COLS(n), n->sem.doc_access.att);
            }
            if (RIGHT_COLS(n) & n->sem.doc_access.res) {
                RIGHT_COLS(n) = diff   (RIGHT_COLS(n), n->sem.doc_access.res);
                RIGHT_COLS(n) = union_ (RIGHT_COLS(n), n->sem.doc_access.att);
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
            /* FIXME: for now we assume that a theta-join
               cannot be pushed through a constructor */
            LEFT_COLS(n)  = att_NULL;
            RIGHT_COLS(n) = att_NULL;
            break;

        case la_merge_adjacent:
            /* FIXME: for now we assume that a theta-join
               cannot be pushed through an element constructor */
            LEFT_COLS(n)  = att_NULL;
            RIGHT_COLS(n) = att_NULL;
            break;

        case la_roots:
            break;

        case la_frag_union:
        case la_fragment:
        case la_frag_extract:
        case la_empty_frag:
            /* for the fragment information we do not need to introduce
               column names as the thetajoin can never be moved along
               its edges. */
            LEFT_COLS(n)  = att_NULL;
            RIGHT_COLS(n) = att_NULL;
            break;

        case la_error:
            /* FIXME: for now we assume that a theta-join
               cannot be pushed through an error */
            LEFT_COLS(n)  = att_NULL;
            RIGHT_COLS(n) = att_NULL;
            break;

        case la_cond_err:
        case la_trace:
            /* propagate input columns to the left child ... */
            if (find_join_worker (
                    L(n),
                    join,
                    BOOL_COLS(n),
                    LEFT_COLS(n),
                    RIGHT_COLS(n)))
                return true;

            /* ... and propagate nothing to the right child */
            if (find_join_worker (
                    R(n),
                    join,
                    0, 0, 0))
                return true;

            return false;
            break;

        case la_nil:
            break;

        case la_trace_msg:
            /* this operator cannot be reached */
            break;

        case la_trace_map:
            /* this operator cannot be reached */
            break;

        case la_rec_fix:
            /* FIXME: for now we assume that a theta-join
               cannot be pushed through a recursion operator */
            LEFT_COLS(n)  = att_NULL;
            RIGHT_COLS(n) = att_NULL;
            break;

        case la_rec_param:
        case la_rec_arg:
        case la_rec_base:
            break;

        case la_fun_call:
        case la_fun_param:
        case la_fun_frag_param:
            /* FIXME: for now we assume that a theta-join
               cannot be pushed through a function application */
            LEFT_COLS(n)  = att_NULL;
            RIGHT_COLS(n) = att_NULL;
            break;
            
        case la_proxy:
        case la_proxy_base:
            break;

        case la_string_join:
            /* propagate updated item column to the left child ... */
            if (find_join_worker (
                    L(n),
                    join,
                    BOOL_COLS(n),
                    LEFT_COLS(n) ? n->sem.string_join.item : 0,
                    RIGHT_COLS(n) ? n->sem.string_join.item : 0))
                return true;

            /* ... and propagate the item separator to the right child */
            if (find_join_worker (
                    R(n),
                    join,
                    BOOL_COLS(n),
                    LEFT_COLS(n) ? n->sem.string_join.item_sep : 0,
                    RIGHT_COLS(n) ? n->sem.string_join.item_sep : 0))
                return true;

            return false;
            break;

        case la_eqjoin_unq:
            PFoops (OOPS_FATAL,
                    "clone column aware equi-join operator is "
                    "only allowed with unique names!");

        case la_cross_mvd:
            PFoops (OOPS_FATAL,
                    "clone column aware cross product operator is "
                    "only allowed inside mvd optimization!");

        case la_dummy:
            break;
    }

    /* infer properties for children */
    if (L(n) && find_join_worker (L(n),
                                  join,
                                  BOOL_COLS(n),
                                  LEFT_COLS(n),
                                  RIGHT_COLS(n)))
        return true;

    if (R(n) && find_join_worker (R(n),
                                  join,
                                  BOOL_COLS(n),
                                  LEFT_COLS(n),
                                  RIGHT_COLS(n)))
        return true;

    return false;
}

/**
 * Reset the property information.
 */
static void
reset_fun (PFla_op_t *n)
{
    EDGE(n) = 0;

    /* reset the referenced column information */
    BOOL_COLS(n)  = 0;
    LEFT_COLS(n)  = 0;
    RIGHT_COLS(n) = 0;
}

/**
 * For each select node we reset the property information
 * and let a worker function find a join operator that can
 * be rewritten.
 */
static PFla_op_t *
find_join (PFla_op_t *select)
{
    PFla_op_t   *join = NULL;

    assert (select && select->kind == la_select);

    /* collect number of incoming edges (parents) */
    PFprop_infer_refctr (select);

    /* reset the old property information */
    PFprop_reset (select, reset_fun);

    if (find_join_worker (L(select),
                          &join,
                          select->sem.select.att,
                          0,
                          0) &&
        join &&
        join->kind == la_eqjoin)
        return join;
    else
        return NULL;
}

/**
 * Rewrite an equi-join operator into a theta-join operator.
 */
static void
transform_join (PFla_op_t *join)
{
    PFalg_sel_t *pred = PFmalloc (sizeof (PFalg_sel_t));

    assert (join && join->kind == la_eqjoin);

    pred[0] = PFalg_sel (alg_comp_eq,
                         join->sem.eqjoin.att1,
                         join->sem.eqjoin.att2);

    *join = *PFla_thetajoin (L(join), R(join), 1, pred);
}

/**
 * Collect all select nodes in our logical plan. (Starting
 * from these nodes we then try to find places where we
 * can introduce theta-join operators that (after optimization)
 * might hopefully merge with the select nodes.)
 */
static void
collect_select_nodes (PFla_op_t *n, PFarray_t *select_nodes)
{
    assert (n);

    /* nothing to do if we already visited that node */
    if (SEEN(n))
        return;

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        collect_select_nodes (n->child[i], select_nodes);

    if (n->kind == la_select)
        *(PFla_op_t **) PFarray_add (select_nodes) = n;

    SEEN(n) = true;
}

/**
 * Introduce theta-join operators.
 */
PFla_op_t *
PFintro_thetajoins (PFla_op_t *root)
{
    PFarray_t *select_nodes = PFarray (sizeof (PFla_op_t *));
    PFla_op_t *select, *join;

    /* collect the select nodes */
    collect_select_nodes (root, select_nodes);
    PFla_dag_reset (root);

    for (unsigned int i = 0; i < PFarray_last (select_nodes); i++) {
        /* for each select node we try to find a corresponding join */
        select = *(PFla_op_t **) PFarray_at (select_nodes, i);

        assert (select->kind == la_select);

        /* start the property inference (and find
           as a side effect a matching join operator) */
        if ((join = find_join (select)))
            /* transform the equi-join into a theta-join */
            transform_join (join);
    }

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
