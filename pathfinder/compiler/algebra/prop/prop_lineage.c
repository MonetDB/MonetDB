/**
 * @file
 *
 * Inference of column lineage (the operator a column stems from).
 * This property attaches to each column the information about the
 * operator it was created by.
 *
 * Binary operator where columns are merged (e.g. union or equi-join
 * columns) and its input columns do not originate in the same operator
 * provide a new lineage for these column.
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
#include <stdio.h>

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define SEEN(n) (n)->bit_dag

/**
 * Look up the lineage of column @a col in the property container @a prop.
 */
PFla_op_t *
PFprop_lineage (const PFprop_t *prop, PFalg_col_t col)
{
    if (!prop || !prop->lineage)
        return NULL;

    for (unsigned int i = 0; i < PFarray_last (prop->lineage); i++)
        if (col == ((lineage_t *) PFarray_at (prop->lineage, i))->col)
            return ((lineage_t *) PFarray_at (prop->lineage, i))->op;

    return NULL;
}

/**
 * Look up the original column name of column @a col
 * in the property container @a prop.
 */
PFalg_col_t
PFprop_lineage_col (const PFprop_t *prop, PFalg_col_t col)
{
    /* this function should be only called after PFprop_lineage was checked */
    assert (prop && prop->lineage);

    for (unsigned int i = 0; i < PFarray_last (prop->lineage); i++)
        if (col == ((lineage_t *) PFarray_at (prop->lineage, i))->col)
            return ((lineage_t *) PFarray_at (prop->lineage, i))->ori_col;

    assert (0);
    return col_NULL;
}

/* add a new column lineage */
#define add_lineage(c,o,oc)                                         \
        *(lineage_t *) PFarray_add (n->prop->lineage)               \
            = (lineage_t) { .col = (c), .op = o , .ori_col = (oc) }
#define add_new_lineage(col) add_lineage ((col), n, (col))
#define map_lineage(out_col,child,in_col)                           \
        add_lineage ((out_col),                                     \
                     PFprop_lineage ((child)->prop, (in_col)),      \
                     PFprop_lineage_col ((child)->prop, (in_col)))

/* copy a list of lineage mappings */
static void
copy (PFarray_t *base, PFarray_t *content)
{
    for (unsigned int i = 0; i < PFarray_last (content); i++)
        *(lineage_t *) PFarray_add (base) =
            *(lineage_t *) PFarray_at (content, i);
}
#define add_all(child) copy (n->prop->lineage, (child)->prop->lineage)

/**
 * Infer properties about lineage; worker for prop_infer().
 *
 * For every new column a new lineage is created (add_new_lineage())
 * and all other columns are propagated bottom-up.
 */
static void
infer_lineage (PFla_op_t *n)
{
    switch (n->kind) {
        case la_serialize_seq:
        case la_serialize_rel:
        case la_content:
        case la_error:
        case la_cache:
            add_all (R(n));
            break;

        case la_side_effects:
        case la_fcns:
        case la_fragment:
        case la_frag_extract:
        case la_frag_union:
        case la_empty_frag:
        case la_nil:
        case la_trace:
        case la_rec_param:
        case la_rec_arg:
        case la_fun_frag_param:
            break;

        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
        case la_rec_fix:
        case la_rec_base:
        case la_fun_call:
        case la_internal_op:
            for (unsigned int i = 0; i < n->schema.count; i++)
                add_new_lineage (n->schema.items[i].name);
            break;

        case la_attach:
            add_all (L(n));
            add_new_lineage (n->sem.attach.res);
            break;

        case la_cross:
        case la_eqjoin:
        case la_thetajoin:
            add_all (L(n));
            add_all (R(n));
            break;

        case la_semijoin:
        case la_select:
        case la_pos_select:
        case la_difference:
        case la_distinct:
        case la_docnode:
        case la_element:
        case la_attribute:
        case la_textnode:
        case la_comment:
        case la_processi:
        case la_roots:
        case la_trace_items:
        case la_trace_msg:
        case la_trace_map:
        case la_fun_param:
        case la_proxy:
        case la_proxy_base:
        case la_dummy:
            add_all (L(n));
            break;

        case la_project:
            for (unsigned int i = 0; i < n->sem.proj.count; i++) {
                assert (PFprop_lineage (L(n)->prop, n->sem.proj.items[i].old));
                map_lineage (n->sem.proj.items[i].new,
                             L(n),
                             n->sem.proj.items[i].old);
            }
            break;

        case la_disjunion:
        case la_intersect:
            /* Keep lineage if both columns stem from the same origin
               and introduce new lineage. */
            for (unsigned int i = 0; i < n->schema.count; i++) {
                PFalg_col_t cur_col = n->schema.items[i].name,
                            lcol,
                            rcol;
                PFla_op_t  *lop = PFprop_lineage (L(n)->prop, cur_col),
                           *rop = PFprop_lineage (R(n)->prop, cur_col);

                assert (lop && rop);
                lcol = PFprop_lineage_col (L(n)->prop, cur_col);
                rcol = PFprop_lineage_col (R(n)->prop, cur_col);

                if (lop == rop && lcol == rcol)
                    add_lineage (cur_col, lop, lcol);
                else
                    add_new_lineage (cur_col);
            }
            break;

        case la_fun_1to1:
            add_all (L(n));
            add_new_lineage (n->sem.fun_1to1.res);
            break;

        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_to:
            add_all (L(n));
            add_new_lineage (n->sem.binary.res);
            break;

        case la_bool_not:
            add_all (L(n));
            add_new_lineage (n->sem.unary.res);
            break;

        case la_aggr:
            if (n->sem.aggr.part)
                map_lineage (n->sem.aggr.part, L(n), n->sem.aggr.part);

            for (unsigned int i = 0; i < n->sem.aggr.count; i++)
                if (n->sem.aggr.aggr[i].kind == alg_aggr_dist)
                    map_lineage (n->sem.aggr.aggr[i].res,
                                 L(n),
                                 n->sem.aggr.aggr[i].col);
                else
                    add_new_lineage (n->sem.aggr.aggr[i].res);
            break;

        case la_rownum:
        case la_rowrank:
        case la_rank:
            add_all (L(n));
            add_new_lineage (n->sem.sort.res);
            break;

        case la_rowid:
            add_all (L(n));
            add_new_lineage (n->sem.rowid.res);
            break;

        case la_type:
        case la_type_assert:
        case la_cast:
            add_all (L(n));
            add_new_lineage (n->sem.type.res);
            break;

        case la_step:
        case la_guide_step:
            map_lineage (n->sem.step.iter, R(n), n->sem.step.iter);
            add_new_lineage (n->sem.step.item_res);
            break;

        case la_step_join:
        case la_guide_step_join:
            add_all (R(n));
            add_new_lineage (n->sem.step.item_res);
            break;

        case la_doc_index_join:
            add_all (R(n));
            add_new_lineage (n->sem.doc_join.item_res);
            break;

        case la_doc_tbl:
            add_all (L(n));
            add_new_lineage (n->sem.doc_tbl.res);
            break;

        case la_doc_access:
            add_all (R(n));
            add_new_lineage (n->sem.doc_access.res);
            break;

        case la_twig:
        {
            PFalg_col_t iter_col = col_NULL;
            /* look up iter lineage in the first child */
            switch (L(n)->kind) {
                case la_docnode:
                    iter_col = L(n)->sem.docnode.iter;
                    break;
                case la_element:
                case la_textnode:
                case la_comment:
                    iter_col = L(n)->sem.iter_item.iter;
                    break;
                case la_attribute:
                case la_processi:
                    iter_col = L(n)->sem.iter_item1_item2.iter;
                    break;
                case la_content:
                    iter_col = L(n)->sem.iter_pos_item.iter;
                    break;
                default:
                    break;
            }
            assert (iter_col);
            map_lineage (n->sem.iter_item.iter, LL(n), iter_col);
            add_new_lineage (n->sem.iter_item.item);
        }   break;

        case la_merge_adjacent:
            map_lineage (n->sem.merge_adjacent.iter_res,
                         R(n),
                         n->sem.merge_adjacent.iter_in);
            add_new_lineage (n->sem.merge_adjacent.pos_res);
            add_new_lineage (n->sem.merge_adjacent.item_res);
            break;

        case la_string_join:
            map_lineage (n->sem.string_join.iter_res,
                         R(n),
                         n->sem.string_join.iter_sep);
            add_new_lineage (n->sem.string_join.item_res);
            break;
    }
}

/* worker for PFprop_infer_lineage */
static void
prop_infer (PFla_op_t *n)
{
    assert (n);

    /* nothing to do if we already visited that node */
    if (SEEN(n))
        return;
    else
        SEEN(n) = true;

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        prop_infer (n->child[i]);

    /* reset lineage property
       (reuse already existing lists if already available
        as this increases the performance of the compiler a lot) */
    if (n->prop->lineage)
        PFarray_last (n->prop->lineage) = 0;
    else
        /* prepare the property for all columns */
        n->prop->lineage = PFarray (sizeof (lineage_t), n->schema.count);

    /* infer lineage information */
    infer_lineage (n);
}

/**
 * Infer lineage property for a DAG rooted in @a root.
 */
void
PFprop_infer_lineage (PFla_op_t *root) {
    prop_infer (root);
    PFla_dag_reset (root);
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
