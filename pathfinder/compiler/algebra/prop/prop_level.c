/**
 * @file
 *
 * Inference of the level of a node.
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

#include "properties.h"
#include "alg_dag.h"
#include "oops.h"
#include "mem.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

static int
level_lookup (PFarray_t *level_mapping, PFalg_col_t col)
{
    if (!level_mapping)
        return UNKNOWN_LEVEL;

    for (unsigned int i = 0; i < PFarray_last (level_mapping); i++)
        if (col == ((level_t *) PFarray_at (level_mapping, i))->col)
            return ((level_t *) PFarray_at (level_mapping, i))->level;

    return UNKNOWN_LEVEL;
}

/**
 * Return level stored in property container @a prop.
 */
int
PFprop_level (const PFprop_t *prop, PFalg_col_t col)
{
    assert (prop);
    return level_lookup (prop->level_mapping, col);
}

/**
 * Return the level of nodes stored in column @a col
 * in the left level mapping filed of property container @a prop.
 */
int
PFprop_level_left (const PFprop_t *prop, PFalg_col_t col)
{
    assert (prop);
    return level_lookup (prop->l_level_mapping, col);
}

/**
 * Return the level of nodes stored in column @a col
 * in the right level mapping filed of property container @a prop.
 */
int
PFprop_level_right (const PFprop_t *prop, PFalg_col_t col)
{
    assert (prop);
    return level_lookup (prop->r_level_mapping, col);
}

static void
copy_level_info (PFla_op_t *n, PFla_op_t *child)
{
    if (!child->prop->level_mapping)
        n->prop->level_mapping = NULL;
    else
        n->prop->level_mapping = PFarray_copy (child->prop->level_mapping);
}

#define insert_level_info(lvl_mapping,child)                      \
{                                                                 \
    assert (child && child->prop);                                \
    if (!lvl_mapping &&                                           \
        child->prop->level_mapping)                               \
        lvl_mapping = PFarray_copy (child->prop->level_mapping);  \
    else if (lvl_mapping &&                                       \
             child->prop->level_mapping) {                        \
        PFarray_t *content = child->prop->level_mapping;          \
                                                                  \
        for (unsigned int i = 0; i < PFarray_last (content); i++) \
            *(level_t *) PFarray_add (lvl_mapping) =              \
                *(level_t *) PFarray_at (content, i);             \
    }                                                             \
}

static void
mark_level (PFprop_t *prop, PFalg_col_t col, int level)
{
    assert (prop);

    if (!prop->level_mapping)
        prop->level_mapping = PFarray (sizeof (level_t), 5);

    *(level_t *) PFarray_add (prop->level_mapping)
        = (level_t) { .col = col, .level = level };
}

/**
 * Infer properties about level; worker for prop_infer().
 */
static void
infer_level (PFla_op_t *n)
{
    /* copy key properties of children into current node */
    if (L(n)) insert_level_info (n->prop->l_level_mapping, L(n));
    if (R(n)) insert_level_info (n->prop->r_level_mapping, R(n));

    switch (n->kind) {
        case la_serialize_seq:
        case la_serialize_rel:
        case la_doc_index_join:
        case la_doc_access:
        case la_error:
        case la_cache:
            /* level stays the same */
            copy_level_info (n, R(n));
            break;

        case la_side_effects:
        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
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
        case la_trace:
        case la_rec_fix:
        case la_rec_param:
        case la_rec_arg:
        case la_rec_base:
        case la_fun_call:
        case la_fun_param:
        case la_fun_frag_param:
        case la_string_join:
            n->prop->level_mapping = NULL;
            break;

        case la_attach:
        case la_semijoin:
        case la_select:
        case la_pos_select:
        case la_difference:
        case la_distinct:
        case la_fun_1to1:
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_bool_not:
        case la_to:
        case la_rownum:
        case la_rowrank:
        case la_rank:
        case la_rowid:
        case la_type:
        case la_type_assert:
        case la_cast:
        case la_roots:
        case la_trace_items:
        case la_trace_msg:
        case la_trace_map:
        case la_proxy:
        case la_proxy_base:
        case la_dummy:
            /* level stays the same */
            copy_level_info (n, L(n));
            break;

        case la_cross:
        case la_eqjoin:
        case la_thetajoin:
        case la_internal_op: /* interpret this operator as internal join */
            copy_level_info (n, L(n));
            if (R(n)) /* avoid the segfault for la_internal_op != eqjoin */
                insert_level_info (n->prop->level_mapping, R(n));
            break;

        case la_project:
            for (unsigned int i = 0; i < n->sem.proj.count; i++) {
                int level = PFprop_level (L(n)->prop,
                                          n->sem.proj.items[i].old);
                if (LEVEL_KNOWN(level))
                    mark_level (n->prop,
                                n->sem.proj.items[i].new,
                                level);
            }
            break;

        case la_disjunion:
        case la_intersect:
            /* if the level for intersect does not match
               we could theoretically return an empty table */
            for (unsigned int i = 0; i < L(n)->schema.count; i++)
                for (unsigned int j = 0; j < R(n)->schema.count; j++)
                    if (L(n)->schema.items[i].name ==
                        R(n)->schema.items[j].name) {
                        PFalg_col_t col = L(n)->schema.items[i].name;
                        int l_level, r_level;

                        l_level = PFprop_level (L(n)->prop, col);
                        r_level = PFprop_level (R(n)->prop, col);

                        if (LEVEL_KNOWN(l_level) && l_level == r_level)
                            mark_level (n->prop, col, l_level);
                        break;
                    }
            break;

        case la_aggr:
            for (unsigned int i = 0; i < n->sem.aggr.count; i++)
                if (n->sem.aggr.aggr[i].kind == alg_aggr_dist &&
                    LEVEL_KNOWN (PFprop_level (L(n)->prop,
                                               n->sem.aggr.aggr[i].col)))
                    mark_level (n->prop,
                                n->sem.aggr.aggr[i].res,
                                PFprop_level (L(n)->prop,
                                              n->sem.aggr.aggr[i].col));
            break;

        case la_step_join:
        case la_guide_step_join:
            copy_level_info (n, R(n));
        case la_step:
        case la_guide_step:
            if (LEVEL_KNOWN(n->sem.step.level))
                mark_level (n->prop, n->sem.step.item_res, n->sem.step.level);
            else {
                PFalg_col_t item_res = n->sem.step.item_res;
                int level = PFprop_level (R(n)->prop, n->sem.step.item);
                if (LEVEL_KNOWN(level))
                    switch (n->sem.step.spec.axis) {
                        case alg_attr:
                        case alg_chld:
                            mark_level (n->prop, item_res, level+1);
                            break;

                        case alg_par:
                            mark_level (n->prop, item_res, level-1);
                            break;

                        case alg_fol_s:
                        case alg_prec_s:
                        case alg_self:
                            mark_level (n->prop, item_res, level);
                            break;

                        default:
                            break;
                    }
            }
            if (PFprop_level (R(n)->prop, n->sem.step.iter) !=
                PFprop_level (n->prop, n->sem.step.iter))
                mark_level (n->prop,
                            n->sem.step.iter,
                            PFprop_level (R(n)->prop, n->sem.step.iter));
            break;

        case la_twig:
            mark_level (n->prop, n->sem.iter_item.item, 0);
            break;

        case la_doc_tbl:
            /* level stays the same */
            copy_level_info (n, L(n));

            if (n->sem.doc_tbl.kind == alg_dt_doc) 
                mark_level (n->prop, n->sem.doc_tbl.res, 0);
            else
                mark_level (n->prop, n->sem.doc_tbl.res, -1);
            break;
    }
}

/* worker for PFprop_infer_level */
static void
prop_infer (PFla_op_t *n)
{
    assert (n);

    /* nothing to do if we already visited that node */
    if (n->bit_dag)
        return;

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        prop_infer (n->child[i]);

    n->bit_dag = true;

    /* reset level property */
    if (n->prop->level_mapping)
        PFarray_last (n->prop->level_mapping) = 0;

    if (n->prop->l_level_mapping)
        PFarray_last (n->prop->l_level_mapping) = 0;
    if (n->prop->r_level_mapping)
        PFarray_last (n->prop->r_level_mapping) = 0;

    /* infer level information */
    infer_level (n);
}

/**
 * Infer level property for a DAG rooted in @a root
 */
void
PFprop_infer_level (PFla_op_t *root) {
    prop_infer (root);
    PFla_dag_reset (root);
}

/* vim:set shiftwidth=4 expandtab: */
