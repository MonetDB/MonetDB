/**
 * @file
 *
 * Inference of unique column names in logical algebra expressions.
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

#include "properties.h"
#include "alg_dag.h"
#include "oops.h"
#include "mem.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define ARRAY_SIZE(n) ((n)->schema.count > 10 ? (n)->schema.count : 10)

/* worker for PFprop_unq_name* */
static PFalg_col_t
find_unq_name (PFarray_t *np_list, PFalg_col_t col)
{
    if (!np_list) return 0;

    for (unsigned int i = 0; i < PFarray_last (np_list); i++)
        if (col == ((name_pair_t *) PFarray_at (np_list, i))->ori)
            return ((name_pair_t *) PFarray_at (np_list, i))->unq;

    return 0;
}

/**
 * Return unique name of column @a col stored
 * in property container @a prop.
 */
PFalg_col_t
PFprop_unq_name (const PFprop_t *prop, PFalg_col_t col)
{
    assert (prop);
    return find_unq_name (prop->name_pairs, col);
}

/**
 * Return unique name of column @a col stored
 * in the left name mapping field of property container @a prop.
 */
PFalg_col_t
PFprop_unq_name_left (const PFprop_t *prop, PFalg_col_t col)
{
    assert (prop);
    return  find_unq_name (prop->l_name_pairs, col);
}

/**
 * Return unique name of column @a col stored
 * in the right name mapping field of property container @a prop.
 */
PFalg_col_t
PFprop_unq_name_right (const PFprop_t *prop, PFalg_col_t col)
{
    assert (prop);
    return  find_unq_name (prop->r_name_pairs, col);
}

/**
 * Add a new original name/unique name pair to the list of name pairs
 * (@a np_list).
 */
static void
add_name_pair (PFarray_t *np_list, PFalg_col_t ori, PFalg_col_t unq)
{
    assert (np_list);

    *(name_pair_t *) PFarray_add (np_list)
        = (name_pair_t) { .ori = ori, .unq = unq };
}

/**
 * Create new unique name and add it together with the original name
 * to the name pair list (@a np_list).
 */
static void
new_name_pair (PFarray_t *np_list, PFalg_col_t ori)
{
    add_name_pair (np_list, ori, PFcol_new (ori));
}

/**
 * Add all original name/unique name pairs of the node @a child to
 * the list of name pairs (@a np_list).
 */
static void
bulk_add_name_pairs (PFarray_t *np_list, PFla_op_t *child)
{
    assert (np_list);
    assert (child);
    assert (child->prop);

    for (unsigned int i = 0; i < child->schema.count; i++) {
        add_name_pair (np_list,
                       child->schema.items[i].name,
                       PFprop_unq_name (child->prop,
                                        child->schema.items[i].name));
    }
}

/**
 * Infer unique name properties; worker for prop_infer().
 */
static void
infer_unq_names (PFla_op_t *n)
{
    PFarray_t *np_list = n->prop->name_pairs;

    switch (n->kind) {
        case la_serialize_seq:
        case la_serialize_rel:
            bulk_add_name_pairs (np_list, R(n));
            break;

        case la_side_effects:
            break;

        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
            /* create new unique names for all columns */
            for (unsigned int i = 0; i < n->schema.count; i++)
                new_name_pair (np_list, n->schema.items[i].name);
            break;

        case la_attach:
            bulk_add_name_pairs (np_list, L(n));
            new_name_pair (np_list, n->sem.attach.res);
            break;

        case la_cross:
        case la_thetajoin: /* for theta-join operators we could find
         a better solution that does not introduce renaming projections.
         It is however unclear whether this would benefit any rewrites. */
        {
            /* To avoid name collisions that arise from applying
               a cross product on input relations with identical
               names we create new column names.
               The correspondence between the unique names of the
               operands and the new unique names is stored in
               a list of name pairs for each operand. */
            PFalg_col_t ori, ori_prev, unq, child_unq;
            PFarray_t *left_np_list, *right_np_list;

            /* initialize left and right name pair list */
            n->prop->l_name_pairs = PFarray (sizeof (name_pair_t),
                                             ARRAY_SIZE(L(n)));
            n->prop->r_name_pairs = PFarray (sizeof (name_pair_t),
                                             ARRAY_SIZE(R(n)));

            left_np_list  = n->prop->l_name_pairs;
            right_np_list = n->prop->r_name_pairs;

            for (unsigned int i = 0; i < L(n)->schema.count; i++) {
                ori = L(n)->schema.items[i].name;
                child_unq = PFprop_unq_name (L(n)->prop, ori);

                /* Check for multiple occurrences of the same unique
                   column name. (Use check function of variable
                   backmapping as it works on the same array) */
                if ((ori_prev = PFprop_ori_name_left (n->prop, child_unq)))
                    /* we already have mapped this unique name
                       - so look up new unique name */
                    unq = find_unq_name (np_list, ori_prev);
                else
                    /* no match */
                    unq = PFcol_new (ori);

                add_name_pair (np_list, ori, unq);
                add_name_pair (left_np_list, ori, child_unq);
            }
            for (unsigned int i = 0; i < R(n)->schema.count; i++) {
                ori = R(n)->schema.items[i].name;
                child_unq = PFprop_unq_name (R(n)->prop, ori);

                /* Check for multiple occurrences of the same unique
                   column name. (Use check function of variable
                   backmapping as it works on the same array) */
                if ((ori_prev = PFprop_ori_name_right (n->prop, child_unq)))
                    /* we already have mapped this unique name
                       - so look up new unique name */
                    unq = find_unq_name (np_list, ori_prev);
                else
                    /* no match */
                    unq = PFcol_new (ori);

                add_name_pair (np_list, ori, unq);
                add_name_pair (right_np_list, ori, child_unq);
            }
        }   break;

        case la_eqjoin:
        {
            PFalg_col_t ori, join_unq,
                        col1_unq, col2_unq,
                        child_unq,
                        col1_conflict, col2_conflict;
            PFarray_t *left_np_list, *right_np_list;
            bool proj_left = PFprop_key (L(n)->prop, n->sem.eqjoin.col1);

            col1_unq = PFprop_unq_name (L(n)->prop, n->sem.eqjoin.col1);
            col2_unq = PFprop_unq_name (R(n)->prop, n->sem.eqjoin.col2);
            /* always use smaller (hopefully original) unique name
               as name of the both join arguments */
            if (col1_unq <= col2_unq)
                join_unq = col1_unq;
            else
                join_unq = col2_unq;

            /* initialize left and right name pair list */
            n->prop->l_name_pairs = PFarray (sizeof (name_pair_t),
                                             ARRAY_SIZE(L(n)));
            n->prop->r_name_pairs = PFarray (sizeof (name_pair_t),
                                             ARRAY_SIZE(R(n)));

            left_np_list  = n->prop->l_name_pairs;
            right_np_list = n->prop->r_name_pairs;

            /* make sure to have stable column names for columns
               conflicting with the join columns */
            col1_conflict = PFcol_new (col1_unq);
            col2_conflict = PFcol_new (col2_unq);

            /* Based on the boolean proj_left we decide whether
               we solve name conflicts at the left or the right side.
                  All column names that are identical to the join
               argument are replaced by the overall join column name
               and all columns that are also referenced on the other
               side will be renamed by introducing a new unique name.
               All other columns stay unchanged. */
            for (unsigned int i = 0; i < L(n)->schema.count; i++) {
                ori = L(n)->schema.items[i].name;
                child_unq = PFprop_unq_name (L(n)->prop, ori);

                if (child_unq == col1_unq)
                    add_name_pair (np_list, ori, join_unq);
                else if (proj_left &&
                         PFprop_ori_name (R(n)->prop, child_unq)) {
                    PFalg_col_t ori_prev, unq;

                    /* like in the cross product case we map equally
                       named conflicting names to the same replacement */
                    if ((ori_prev = PFprop_ori_name_left (n->prop,
                                                           child_unq)))
                        /* we already have mapped this unique name
                           - so look up new unique name */
                        unq = find_unq_name (np_list, ori_prev);
                    else
                        /* no match */
                        unq = PFcol_new (ori);

                    add_name_pair (np_list, ori, unq);
                }
                else if (child_unq == col2_unq)
                    add_name_pair (np_list, ori, col1_conflict);
                else
                    add_name_pair (np_list, ori, child_unq);

                add_name_pair (left_np_list, ori, child_unq);
            }

            for (unsigned int i = 0; i < R(n)->schema.count; i++) {
                ori = R(n)->schema.items[i].name;
                child_unq = PFprop_unq_name (R(n)->prop, ori);

                if (child_unq == col2_unq)
                    add_name_pair (np_list, ori, join_unq);
                else if (!proj_left &&
                         PFprop_ori_name (L(n)->prop, child_unq)) {
                    PFalg_col_t ori_prev, unq;

                    /* like in the cross product case we map equally
                       named conflicting names to the same replacement */
                    if ((ori_prev = PFprop_ori_name_right (n->prop,
                                                           child_unq)))
                        /* we already have mapped this unique name
                           - so look up new unique name */
                        unq = find_unq_name (np_list, ori_prev);
                    else
                        /* no match */
                        unq = PFcol_new (ori);

                    add_name_pair (np_list, ori, unq);
                }
                else if (child_unq == col1_unq)
                    add_name_pair (np_list, ori, col2_conflict);
                else
                    add_name_pair (np_list, ori, child_unq);

                add_name_pair (right_np_list, ori, child_unq);
            }
        }   break;

        case la_semijoin:
            bulk_add_name_pairs (np_list, L(n));

            n->prop->l_name_pairs = PFarray (sizeof (name_pair_t),
                                             ARRAY_SIZE(L(n)));
            n->prop->r_name_pairs = PFarray (sizeof (name_pair_t),
                                             ARRAY_SIZE(R(n)));
            /* make sure to know the names of the inputs */
            bulk_add_name_pairs (n->prop->l_name_pairs, L(n));

            add_name_pair (n->prop->r_name_pairs,
                           n->sem.eqjoin.col2,
                           PFprop_unq_name (R(n)->prop,
                                            n->sem.eqjoin.col2));
            break;

        case la_project:
            /* bind all existing unique names to the possibly new names */
            for (unsigned int i = 0; i < n->schema.count; i++)
                add_name_pair (np_list,
                               n->sem.proj.items[i].new,
                               PFprop_unq_name (L(n)->prop,
                                                n->sem.proj.items[i].old));
            break;

        case la_select:
        case la_pos_select:
        case la_distinct:
            bulk_add_name_pairs (np_list, L(n));
            break;

        case la_disjunion:
        case la_intersect:
        case la_difference:
        {
            /* To avoid name collisions that arise from applying
               a set operation on input relations with identical
               names we create new column names.
               The correspondence between the unique names of the
               operands and the new unique names is stored in
               a list of name pairs for each operand. */
            PFalg_col_t ori, unq, l_unq, r_unq;

            n->prop->l_name_pairs = PFarray (sizeof (name_pair_t),
                                             ARRAY_SIZE(L(n)));
            n->prop->r_name_pairs = PFarray (sizeof (name_pair_t),
                                             ARRAY_SIZE(R(n)));

            for (unsigned int i = 0; i < n->schema.count; i++) {
                ori = n->schema.items[i].name;
                l_unq = PFprop_unq_name (L(n)->prop, ori);
                r_unq = PFprop_unq_name (R(n)->prop, ori);

                /* maintain unique name if it the same for both operands */
                /*
                if (n->kind == la_disjunion && l_unq == r_unq)
                    unq = l_unq;
                else
                */
                    unq = PFcol_new (ori);

                add_name_pair (np_list, ori, unq);
                add_name_pair (n->prop->l_name_pairs, ori, l_unq);
                add_name_pair (n->prop->r_name_pairs, ori, r_unq);
            }
        }   break;

        case la_fun_1to1:
            bulk_add_name_pairs (np_list, L(n));
            new_name_pair (np_list, n->sem.fun_1to1.res);
            break;

        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_to:
            bulk_add_name_pairs (np_list, L(n));
            new_name_pair (np_list, n->sem.binary.res);
            break;

        case la_bool_not:
            bulk_add_name_pairs (np_list, L(n));
            new_name_pair (np_list, n->sem.unary.res);
            break;

        case la_aggr:
            for (unsigned int i = 0; i < n->sem.aggr.count; i++)
                new_name_pair (np_list, n->sem.aggr.aggr[i].res);
            if (n->sem.aggr.part)
                add_name_pair (np_list,
                               n->sem.aggr.part,
                               PFprop_unq_name (L(n)->prop,
                                                n->sem.aggr.part));
            break;

        case la_rownum:
        case la_rowrank:
        case la_rank:
            bulk_add_name_pairs (np_list, L(n));
            new_name_pair (np_list, n->sem.sort.res);
            break;

        case la_rowid:
            bulk_add_name_pairs (np_list, L(n));
            new_name_pair (np_list, n->sem.rowid.res);
            break;

        case la_type:
        case la_cast:
            bulk_add_name_pairs (np_list, L(n));
            new_name_pair (np_list, n->sem.type.res);
            break;

        case la_type_assert:
            bulk_add_name_pairs (np_list, L(n));
            break;

        case la_step:
        case la_guide_step:
            add_name_pair (np_list,
                           n->sem.step.iter,
                           PFprop_unq_name (R(n)->prop,
                                            n->sem.step.iter));
            new_name_pair (np_list, n->sem.step.item_res);
            break;

        case la_step_join:
        case la_guide_step_join:
            bulk_add_name_pairs (np_list, R(n));
            new_name_pair (np_list, n->sem.step.item_res);
            break;

        case la_doc_index_join:
            bulk_add_name_pairs (np_list, R(n));
            new_name_pair (np_list, n->sem.doc_join.item_res);
            break;

        case la_doc_tbl:
            bulk_add_name_pairs (np_list, L(n));
            new_name_pair (np_list, n->sem.doc_tbl.res);
            break;

        case la_doc_access:
            bulk_add_name_pairs (np_list, R(n));
            new_name_pair (np_list, n->sem.doc_access.res);
            break;

        case la_twig:
            switch (L(n)->kind) {
                case la_docnode:
                    add_name_pair (np_list,
                                   n->sem.iter_item.iter,
                                   PFprop_unq_name (LL(n)->prop,
                                                    L(n)->sem.docnode.iter));
                    break;

                case la_element:
                case la_textnode:
                case la_comment:
                    add_name_pair (np_list,
                                   n->sem.iter_item.iter,
                                   PFprop_unq_name (LL(n)->prop,
                                                    L(n)->sem.iter_item.iter));
                    break;

                case la_attribute:
                case la_processi:
                    add_name_pair (np_list,
                                   n->sem.iter_item.iter,
                                   PFprop_unq_name (
                                       LL(n)->prop,
                                       L(n)->sem.iter_item1_item2.iter));
                    break;

                case la_content:
                    add_name_pair (np_list,
                                   n->sem.iter_item.iter,
                                   PFprop_unq_name (
                                       LR(n)->prop,
                                       L(n)->sem.iter_pos_item.iter));
                    break;

                default:
                    break;
            }
            new_name_pair (np_list, n->sem.iter_item.item);
            break;

        case la_fcns:
        case la_docnode:
        case la_element:
        case la_attribute:
        case la_textnode:
        case la_comment:
        case la_processi:
        case la_content:
            break;

        case la_merge_adjacent:
            add_name_pair (np_list,
                           n->sem.merge_adjacent.iter_res,
                           PFprop_unq_name (R(n)->prop,
                                            n->sem.merge_adjacent.iter_in));
            add_name_pair (np_list,
                           n->sem.merge_adjacent.pos_res,
                           PFprop_unq_name (R(n)->prop,
                                            n->sem.merge_adjacent.pos_in));
            new_name_pair (np_list, n->sem.merge_adjacent.item_res);
            break;

        case la_roots:
            bulk_add_name_pairs (np_list, L(n));
            break;

        case la_fragment:
        case la_frag_extract:
        case la_frag_union:
        case la_empty_frag:
        case la_fun_frag_param:
            break;

        case la_error:
        case la_cache:
            bulk_add_name_pairs (np_list, R(n));
            break;

        case la_trace:
            break;

        case la_trace_items:
        case la_trace_msg:
        case la_trace_map:
            bulk_add_name_pairs (np_list, L(n));
            break;

        case la_nil:
            break;

        case la_rec_fix:
            /* get the unique names of the overall result */
            bulk_add_name_pairs (np_list, R(n));
            break;

        case la_rec_param:
            /* recursion parameters do not have properties */
            break;

        case la_rec_arg:
            /* The both inputs (seed and recursion) do not use
               the same column names. Thus we live with inconsistent
               unique names and introduce renaming projections
               (Schema R -> new and Schema L -> new) during the name
               mapping. */

            /* create new unique names for all columns */
            for (unsigned int i = 0; i < n->schema.count; i++)
                new_name_pair (np_list, n->schema.items[i].name);
            break;

        case la_rec_base:
            /* properties are already assigned */
            break;

        case la_fun_call:
            /* create new unique names for all columns */
            for (unsigned int i = 0; i < n->schema.count; i++)
                new_name_pair (np_list, n->schema.items[i].name);
            break;

        case la_fun_param:
            bulk_add_name_pairs (np_list, L(n));
            break;

        case la_proxy:
        case la_proxy_base:
            bulk_add_name_pairs (np_list, L(n));
            break;

        case la_string_join:
            add_name_pair (np_list,
                           n->sem.string_join.iter_res,
                           PFprop_unq_name (R(n)->prop,
                                            n->sem.string_join.iter_sep));
            new_name_pair (np_list, n->sem.string_join.item_res);
            break;

        case la_internal_op:
            PFoops (OOPS_FATAL,
                    "internal optimization operator is not allowed here");

        case la_dummy:
            bulk_add_name_pairs (np_list, L(n));
            break;
    }
}

static void
reset_property (PFla_op_t *n)
{
    /* reset the unique name information */
    if (n->prop->name_pairs)
        PFarray_last (n->prop->name_pairs) = 0;
    else
        n->prop->name_pairs = PFarray (sizeof (name_pair_t),
                                       ARRAY_SIZE(n));

    n->prop->l_name_pairs = NULL;
    n->prop->r_name_pairs = NULL;
}

/* forward declaration */
static void
prop_infer (PFla_op_t *n);

/* Helper function that walks through a recursion paramter list
   and only calls the property inference for the seed expressions. */
static void
prop_infer_rec_seed (PFla_op_t *n)
{
    switch (n->kind)
    {
        case la_rec_param:
            /* infer the unique names of the arguments */
            prop_infer_rec_seed (L(n));
            prop_infer_rec_seed (R(n));

            /* recursion parameters do not have properties */
            reset_property (n);
            break;

        case la_rec_arg:
            /* infer the unique names of the seed */
            prop_infer (L(n));

            reset_property (n);

            /* infer unique name columns */
            infer_unq_names (n);

            n->sem.rec_arg.base->bit_dag = true;
            reset_property (n->sem.rec_arg.base);

            /* copy the mapping of the unique column names of the recursion
               argument to its base. */
            bulk_add_name_pairs (n->sem.rec_arg.base->prop->name_pairs, n);

            /* The both inputs (seed and recursion) do not use
               the same column names. Thus we live with inconsistent
               unique names and introduce renaming projections
               (Schema R -> new and Schema L -> new) during the name
               mapping. */
            break;

        case la_nil:
            break;

        default:
            PFoops (OOPS_FATAL,
                    "unexpected node kind %i",
                    n->kind);
            break;
    }
}

/* Helper function that walks through a recursion paramter list
   and only calls the property inference for the recursion body. */
static void
prop_infer_rec_body (PFla_op_t *n)
{
    switch (n->kind)
    {
        case la_rec_param:
            /* infer the unique names of the arguments */
            prop_infer_rec_body (L(n));
            prop_infer_rec_body (R(n));
            break;

        case la_rec_arg:
            /* infer the unique names of the recursion body */
            prop_infer (R(n));

            /* The both inputs (seed and recursion) do not use
               the same column names. Thus we live with inconsistent
               unique names and introduce renaming projections
               (Schema R -> new and Schema L -> new) during the name
               mapping. */
            break;

        case la_nil:
            break;

        default:
            PFoops (OOPS_FATAL,
                    "unexpected node kind %i",
                    n->kind);
            break;
    }

    n->bit_dag = true;
}

/* worker for PFprop_infer_unq_names */
static void
prop_infer (PFla_op_t *n)
{
    bool bottom_up = true;

    assert (n);

    /* nothing to do if we already visited that node */
    if (n->bit_dag)
        return;

    /* Make sure to first collect all seeds and adjust
       the rec_base properties before inferring the properties
       for the body and result expression. */
    switch (n->kind)
    {
        case la_rec_fix:
            /* infer the unique names of the arguments */
            prop_infer_rec_seed (LR(n));
            reset_property (L(n));
            prop_infer (LL(n));
            prop_infer_rec_body (LR(n));
            prop_infer (R(n));
            bottom_up = false;
            break;

        default:
            break;
    }

    if (bottom_up)
        /* infer properties for children bottom-up (ensure that
           the fragment information is translated after the value part) */
        for (unsigned int i = PFLA_OP_MAXCHILD; i > 0; i--)
            if (n->child[i - 1])
                prop_infer (n->child[i - 1]);

    n->bit_dag = true;
    reset_property (n);

    /* infer unique name columns */
    infer_unq_names (n);
}

/**
 * Infer unique names for a DAG rooted in root
 */
void
PFprop_infer_unq_names (PFla_op_t *root)
{
    /* get the key information for
       a more useful eqjoin inference */
    PFprop_infer_key (root);

    prop_infer (root);
    PFla_dag_reset (root);
}

/* vim:set shiftwidth=4 expandtab: */
