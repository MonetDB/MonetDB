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

#include "la_thetajoin.h"
#include "properties.h"
#include "mem.h"          /* PFmalloc() */
#include "oops.h"
#include "alg_dag.h"

/** mnemonic algebra constructors */
#include "logical_mnemonic.h"

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define SEEN(n)       (n)->bit_dag

#define BOOL_COLS(n)  (n)->prop->icols
#define LEFT_COLS(n)  (n)->prop->l_icols
#define RIGHT_COLS(n) (n)->prop->r_icols

/**
 * Check if a column @a b appears in list @a a.
 */
static bool
in (PFalg_collist_t *a, PFalg_col_t b)
{
    if (!a) return false;

    for (unsigned int i = 0; i < clsize (a); i++)
        if (b == clat (a, i))
            return true;

    return false;
}

/**
 * Returns union of two icols lists
 */
static void
union_ (PFalg_collist_t *a, PFalg_col_t b)
{
    assert (a);

    if (!in (a, b))
        cladd (a) = b;
}

/**
 * Extends icol list @a with all the items
 * of the icol list @a b that are not in @a a/
 */
static void
union_list (PFalg_collist_t *a, PFalg_collist_t *b)
{
    PFalg_col_t cur;

    assert (a);
    if (!b) return;

    for (unsigned int i = 0; i < clsize (b); i++) {
        cur = clat (b, i);
        if (!in (a, cur))
            cladd (a) = cur;
    }
}

/**
 * Returns difference of two icols lists
 */
static void
diff (PFalg_collist_t *a, PFalg_col_t b)
{
    unsigned int i = 0;
    while (i < clsize (a)) {
        if (clat (a, i) != b)
            i++;
        else { /* b found -- remove the column */
            clat (a, i) = cltop (a);
            clsize (a)--;
            return;
        }
    }
}

/**
 * Returns the intersection of an column list @a collist and a schema
 * @a schema_ocols
 */
static PFalg_collist_t *
intersect_ocol (PFalg_collist_t *collist, PFalg_schema_t schema_ocols)
{
    PFalg_collist_t *new_collist = PFalg_collist (clsize (collist));

    /* intersect columns */
    for (unsigned int i = 0; i < schema_ocols.count; i++)
        if (in (collist, schema_ocols.items[i].name))
            cladd (new_collist) = schema_ocols.items[i].name;

    return new_collist;
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
find_join_worker (PFla_op_t       *n,
                  PFla_op_t      **join,
                  PFalg_collist_t *bool_cols,
                  PFalg_collist_t *left_cols,
                  PFalg_collist_t *right_cols)
{
    assert (n);

    /* collect all input columns */
    union_list (BOOL_COLS(n), bool_cols);
    union_list (LEFT_COLS(n), left_cols);
    union_list (RIGHT_COLS(n), right_cols);

    PFprop_refctr (n) = PFprop_refctr (n) - 1;
    /* nothing to do if we haven't collected
       all incoming column lists of that node */
    if (PFprop_refctr (n) > 0)
        return false;

    /* remove all unnecessary columns from our structure */
    BOOL_COLS(n)  = intersect_ocol (BOOL_COLS(n), n->schema);
    LEFT_COLS(n)  = intersect_ocol (LEFT_COLS(n), n->schema);
    RIGHT_COLS(n) = intersect_ocol (RIGHT_COLS(n), n->schema);

    /* If both the left side and the right side of a comparison
       reference the same column in a rowid operator
       we have (hopefully) reached our goal. */
    if (n->kind == la_rowid &&
        clsize (LEFT_COLS(n)) && clsize (RIGHT_COLS(n))) {
        bool flag = false;
        for (unsigned int i = 0; i < clsize (LEFT_COLS(n)); i++) {
            flag = in (RIGHT_COLS(n), clat (LEFT_COLS(n), i));
            if (!flag)
                break;
        }
        if (flag) return true;
        flag = false;
        for (unsigned int i = 0; i < clsize (RIGHT_COLS(n)); i++) {
            flag = in (LEFT_COLS(n), clat (RIGHT_COLS(n), i));
            if (!flag)
                break;
        }
        if (flag) return true;
    }

    switch (n->kind) {
        case la_serialize_seq:
        case la_serialize_rel:
            assert (!"this operator should never occur");
            break;

        case la_side_effects:
            /* a thetajoin cannot be pushed through
               a side effect list operator */
            BOOL_COLS(n)  = NULL;
            LEFT_COLS(n)  = NULL;
            RIGHT_COLS(n) = NULL;
            break;

        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
            break;

        case la_attach:
            diff (BOOL_COLS(n),  n->sem.attach.res);
            diff (LEFT_COLS(n),  n->sem.attach.res);
            diff (RIGHT_COLS(n), n->sem.attach.res);
            break;

        case la_cross:
        case la_semijoin:
        case la_select:
        case la_disjunion:
        case la_intersect:
        case la_difference:
        case la_distinct:
            break;

        case la_eqjoin:
        case la_thetajoin:
            /* make sure to remember the last join node where we split
               up the left and the right cols */
            if (clsize (LEFT_COLS(n)) &&
                clsize (RIGHT_COLS(n))) {
                /* find out which side LEFT_COLS refers to */
                bool left_to_left = false;
                for (unsigned int i = 0; i < L(n)->schema.count; i++)
                    if (L(n)->schema.items[i].name == clat (LEFT_COLS(n), 0)) {
                        left_to_left = true;
                        break;
                    }

                /* the required columns need to be strictly separated */
                if (left_to_left) {
                    if (clsize (intersect_ocol (LEFT_COLS(n), L(n)->schema))
                            == clsize (LEFT_COLS(n)) &&
                        clsize (intersect_ocol (LEFT_COLS(n), R(n)->schema))
                            == 0 &&
                        clsize (intersect_ocol (RIGHT_COLS(n), R(n)->schema))
                            == clsize (RIGHT_COLS(n)) &&
                        clsize (intersect_ocol (RIGHT_COLS(n), L(n)->schema))
                            == 0)
                        *join = n;
                }
                else {
                    if (clsize (intersect_ocol (LEFT_COLS(n), R(n)->schema))
                            == clsize (LEFT_COLS(n)) &&
                        clsize (intersect_ocol (LEFT_COLS(n), L(n)->schema))
                            == 0 &&
                        clsize (intersect_ocol (RIGHT_COLS(n), L(n)->schema))
                            == clsize (RIGHT_COLS(n)) &&
                        clsize (intersect_ocol (RIGHT_COLS(n), R(n)->schema))
                            == 0)
                        *join = n;
                }
            }
            break;

        case la_project:
        {
            PFalg_collist_t *bool_cols  = PFalg_collist (n->sem.proj.count),
                            *left_cols  = PFalg_collist (n->sem.proj.count),
                            *right_cols = PFalg_collist (n->sem.proj.count);

            /* rename columns from new to old */
            for (unsigned int i = 0; i < n->sem.proj.count; i++) {
                if (in (BOOL_COLS(n), n->sem.proj.items[i].new))
                    union_ (bool_cols, n->sem.proj.items[i].old);
                if (in (LEFT_COLS(n), n->sem.proj.items[i].new))
                    union_ (left_cols, n->sem.proj.items[i].old);
                if (in (RIGHT_COLS(n), n->sem.proj.items[i].new))
                    union_ (right_cols, n->sem.proj.items[i].old);
            }
            BOOL_COLS(n)  = bool_cols;
            LEFT_COLS(n)  = left_cols;
            RIGHT_COLS(n) = right_cols;
        }   break;

        case la_pos_select:
            BOOL_COLS(n) = NULL;
            LEFT_COLS(n) = NULL;
            break;

        case la_fun_1to1:
            diff (BOOL_COLS(n), n->sem.fun_1to1.res);
            /* ignore fn:contains() here as we do not know
               how to implement it in a theta-join */

            if (in (LEFT_COLS(n), n->sem.fun_1to1.res)) {
                diff (LEFT_COLS(n), n->sem.fun_1to1.res);
                for (unsigned int i = 0; i < clsize (n->sem.fun_1to1.refs); i++)
                    union_ (LEFT_COLS(n), clat (n->sem.fun_1to1.refs, i));
            }

            if (in (RIGHT_COLS(n), n->sem.fun_1to1.res)) {
                diff (RIGHT_COLS(n), n->sem.fun_1to1.res);
                for (unsigned int i = 0; i < clsize (n->sem.fun_1to1.refs); i++)
                    union_ (RIGHT_COLS(n), clat (n->sem.fun_1to1.refs, i));
            }
            break;

        case la_num_eq:
        case la_num_gt:
            if (in (BOOL_COLS(n), n->sem.binary.res)) {
                diff (BOOL_COLS(n),  n->sem.binary.res);
                union_ (LEFT_COLS(n),  n->sem.binary.col1);
                union_ (RIGHT_COLS(n), n->sem.binary.col2);
            }

            if (in (LEFT_COLS(n), n->sem.binary.res)) {
                diff (LEFT_COLS(n), n->sem.binary.res);
                union_ (LEFT_COLS(n), n->sem.binary.col1);
                union_ (LEFT_COLS(n), n->sem.binary.col2);
            }

            if (in (RIGHT_COLS(n), n->sem.binary.res)) {
                diff (RIGHT_COLS(n), n->sem.binary.res);
                union_ (RIGHT_COLS(n), n->sem.binary.col1);
                union_ (RIGHT_COLS(n), n->sem.binary.col2);
            }
            break;

        case la_bool_and:
            if (in (BOOL_COLS(n), n->sem.binary.res)) {
                diff (BOOL_COLS(n), n->sem.binary.res);
                union_ (BOOL_COLS(n), n->sem.binary.col1);
                union_ (BOOL_COLS(n), n->sem.binary.col2);
            }

            if (in (LEFT_COLS(n), n->sem.binary.res)) {
                diff (LEFT_COLS(n), n->sem.binary.res);
                union_ (LEFT_COLS(n), n->sem.binary.col1);
                union_ (LEFT_COLS(n), n->sem.binary.col2);
            }

            if (in (RIGHT_COLS(n), n->sem.binary.res)) {
                diff (RIGHT_COLS(n), n->sem.binary.res);
                union_ (RIGHT_COLS(n), n->sem.binary.col1);
                union_ (RIGHT_COLS(n), n->sem.binary.col2);
            }
            break;

        case la_bool_or:
            diff (BOOL_COLS(n), n->sem.binary.res);

            if (in (LEFT_COLS(n), n->sem.binary.res)) {
                diff (LEFT_COLS(n), n->sem.binary.res);
                union_ (LEFT_COLS(n), n->sem.binary.col1);
                union_ (LEFT_COLS(n), n->sem.binary.col2);
            }

            if (in (RIGHT_COLS(n), n->sem.binary.res)) {
                diff (RIGHT_COLS(n), n->sem.binary.res);
                union_ (RIGHT_COLS(n), n->sem.binary.col1);
                union_ (RIGHT_COLS(n), n->sem.binary.col2);
            }
            break;

        case la_bool_not:
            if (in (BOOL_COLS(n), n->sem.unary.res)) {
                diff (BOOL_COLS(n), n->sem.unary.res);
                union_ (BOOL_COLS(n), n->sem.unary.col);
            }

            if (in (LEFT_COLS(n), n->sem.unary.res)) {
                diff (LEFT_COLS(n), n->sem.unary.res);
                union_ (LEFT_COLS(n), n->sem.unary.col);
            }

            if (in (RIGHT_COLS(n), n->sem.unary.res)) {
                diff (RIGHT_COLS(n), n->sem.unary.res);
                union_ (RIGHT_COLS(n), n->sem.unary.col);
            }
            break;

        case la_to:
            if (in (LEFT_COLS(n), n->sem.binary.res)) {
                diff (LEFT_COLS(n), n->sem.binary.res);
            }
            if (in (RIGHT_COLS(n), n->sem.binary.res)) {
                diff (RIGHT_COLS(n), n->sem.binary.res);
            }
            break;

        case la_aggr:
            /* a theta-join cannot be pushed through an aggregate */
            BOOL_COLS(n)  = NULL;
            LEFT_COLS(n)  = NULL;
            RIGHT_COLS(n) = NULL;
            break;

        case la_rownum:
        case la_rowrank:
        case la_rank:
            if (in (LEFT_COLS(n), n->sem.sort.res)) {
                diff (LEFT_COLS(n), n->sem.sort.res);

                for (unsigned int i = 0;
                     i < PFord_count (n->sem.sort.sortby);
                     i++)
                    union_ (LEFT_COLS(n), PFord_order_col_at (
                                               n->sem.sort.sortby, i));

                /* only infer part if available */
                if (n->sem.sort.part != col_NULL)
                    union_ (LEFT_COLS(n), n->sem.sort.part);
            }

            if (in (RIGHT_COLS(n), n->sem.sort.res)) {
                diff (RIGHT_COLS(n), n->sem.sort.res);

                for (unsigned int i = 0;
                     i < PFord_count (n->sem.sort.sortby);
                     i++)
                    union_ (RIGHT_COLS(n), PFord_order_col_at (
                                                n->sem.sort.sortby, i));

                /* only infer part if available */
                if (n->sem.sort.part != col_NULL)
                    union_ (RIGHT_COLS(n), n->sem.sort.part);
            }
            break;

        case la_rowid:
            diff (LEFT_COLS(n), n->sem.rowid.res);
            diff (RIGHT_COLS(n), n->sem.rowid.res);
            break;

        case la_type:
        case la_cast:
            diff (BOOL_COLS(n), n->sem.type.res);

            if (in (LEFT_COLS(n), n->sem.type.res)) {
                diff (LEFT_COLS(n), n->sem.type.res);
                union_ (LEFT_COLS(n), n->sem.type.col);
            }

            if (in (RIGHT_COLS(n), n->sem.type.res)) {
                diff (RIGHT_COLS(n), n->sem.type.res);
                union_ (RIGHT_COLS(n), n->sem.type.col);
            }
            break;

        case la_type_assert:
            break;

        case la_step:
        case la_step_join:
        case la_guide_step:
        case la_guide_step_join:
            if (in (LEFT_COLS(n), n->sem.step.item_res)) {
                diff (LEFT_COLS(n), n->sem.step.item_res);
                union_ (LEFT_COLS(n), n->sem.step.item);
            }
            if (in (RIGHT_COLS(n), n->sem.step.item_res)) {
                diff (RIGHT_COLS(n), n->sem.step.item_res);
                union_ (RIGHT_COLS(n), n->sem.step.item);
            }
            break;

        case la_doc_index_join:
            if (in (LEFT_COLS(n), n->sem.doc_join.item_res)) {
                diff (LEFT_COLS(n), n->sem.doc_join.item_res);
                union_ (LEFT_COLS(n), n->sem.doc_join.item);
                union_ (LEFT_COLS(n), n->sem.doc_join.item_doc);
            }
            if (in (RIGHT_COLS(n), n->sem.doc_join.item_res)) {
                diff (RIGHT_COLS(n), n->sem.doc_join.item_res);
                union_ (RIGHT_COLS(n), n->sem.doc_join.item);
                union_ (RIGHT_COLS(n), n->sem.doc_join.item_doc);
            }
            break;

        case la_doc_tbl:
            if (in (LEFT_COLS(n), n->sem.doc_tbl.res)) {
                diff (LEFT_COLS(n), n->sem.doc_tbl.res);
                union_ (LEFT_COLS(n), n->sem.doc_tbl.col);
            }
            if (in (RIGHT_COLS(n), n->sem.doc_tbl.res)) {
                diff (RIGHT_COLS(n), n->sem.doc_tbl.res);
                union_ (RIGHT_COLS(n), n->sem.doc_tbl.col);
            }
            break;

        case la_doc_access:
            if (in (LEFT_COLS(n), n->sem.doc_access.res)) {
                diff (LEFT_COLS(n), n->sem.doc_access.res);
                union_ (LEFT_COLS(n), n->sem.doc_access.col);
            }
            if (in (RIGHT_COLS(n), n->sem.doc_access.res)) {
                diff (RIGHT_COLS(n), n->sem.doc_access.res);
                union_ (RIGHT_COLS(n), n->sem.doc_access.col);
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
            BOOL_COLS(n)  = NULL;
            LEFT_COLS(n)  = NULL;
            RIGHT_COLS(n) = NULL;
            break;

        case la_merge_adjacent:
            /* FIXME: for now we assume that a theta-join
               cannot be pushed through an element constructor */
            BOOL_COLS(n)  = NULL;
            LEFT_COLS(n)  = NULL;
            RIGHT_COLS(n) = NULL;
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
            BOOL_COLS(n)  = NULL;
            LEFT_COLS(n)  = NULL;
            RIGHT_COLS(n) = NULL;
            break;

        case la_error:
        case la_cache:
        case la_trace:
        case la_trace_items:
        case la_trace_msg:
        case la_trace_map:
            /* a theta-join cannot be pushed through */
            BOOL_COLS(n)  = NULL;
            LEFT_COLS(n)  = NULL;
            RIGHT_COLS(n) = NULL;
            break;

        case la_nil:
            break;

        case la_rec_fix:
            /* FIXME: for now we assume that a theta-join
               cannot be pushed through a recursion operator */
            BOOL_COLS(n)  = NULL;
            LEFT_COLS(n)  = NULL;
            RIGHT_COLS(n) = NULL;
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
            BOOL_COLS(n)  = NULL;
            LEFT_COLS(n)  = NULL;
            RIGHT_COLS(n) = NULL;
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
                    LEFT_COLS(n) ? collist (n->sem.string_join.item) : NULL,
                    RIGHT_COLS(n) ? collist (n->sem.string_join.item) : NULL))
                return true;

            /* ... and propagate the item separator to the right child */
            if (find_join_worker (
                    R(n),
                    join,
                    BOOL_COLS(n),
                    LEFT_COLS(n) ? collist (n->sem.string_join.item_sep) : NULL,
                    RIGHT_COLS(n) ? collist (n->sem.string_join.item_sep) : NULL))
                return true;

            return false;
            break;

        case la_internal_op:
            PFoops (OOPS_FATAL,
                    "internal optimization operator is not allowed here");

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
    /* reset property
       (reuse already existing lists if already available
        as this increases the performance of the compiler a lot) */
    if (BOOL_COLS(n))
        clsize (BOOL_COLS(n)) = 0;
    else
        BOOL_COLS(n) = PFalg_collist (10);

    if (LEFT_COLS(n))
        clsize (LEFT_COLS(n)) = 0;
    else
        LEFT_COLS(n) = PFalg_collist (10);

    if (RIGHT_COLS(n))
        clsize (RIGHT_COLS(n)) = 0;
    else
        RIGHT_COLS(n) = PFalg_collist (10);
}

/**
 * For each select node we reset the property information
 * and let a worker function find a join operator that can
 * be rewritten.
 */
static PFla_op_t *
find_join (PFla_op_t *select)
{
    PFla_op_t *join = NULL;

    assert (select && select->kind == la_select);

    /* collect number of incoming edges (parents) */
    PFprop_infer_refctr (select);

    /* reset the old property information */
    PFprop_reset (select, reset_fun);

    if (find_join_worker (L(select),
                          &join,
                          collist (select->sem.select.col),
                          NULL,
                          NULL) &&
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
                         join->sem.eqjoin.col1,
                         join->sem.eqjoin.col2);

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
    PFarray_t *select_nodes = PFarray (sizeof (PFla_op_t *), 20);
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
