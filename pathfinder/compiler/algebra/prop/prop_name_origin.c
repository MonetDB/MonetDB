/**
 * @file
 *
 * Inference of name origin properties of logical algebra expressions.
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
 * 2008-2009 Eberhard Karls Universitaet Tuebingen, respectively.  All
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
#include <string.h>

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

/**
 * Return the original name of column @a col (in container @a prop).
 */
char *
PFprop_name_origin (const PFprop_t *prop, PFalg_col_t col)
{
    assert (prop);

    if (!prop->name_origin)
        return NULL;

    for (unsigned int i = 0; i < PFarray_last (prop->name_origin); i++)
        if (col == ((name_origin_t *) PFarray_at (prop->name_origin, i))->col)
            return ((name_origin_t *) PFarray_at (prop->name_origin, i))->name;

    return NULL;
}

/**
 * Add for a column @a col the original name @a name.
 */
static void
add_name (PFprop_t *prop, PFalg_col_t col, char *name)
{
    assert (prop);
    assert (prop->name_origin);


#ifndef NDEBUG
    if (PFprop_name_origin (prop, col))
        PFoops (OOPS_FATAL,
                "name for column `%s' already declared (%s)",
                PFcol_str (col),
                PFprop_name_origin (prop, col));
#endif

    *(name_origin_t *) PFarray_add (prop->name_origin)
        = (name_origin_t) { .col = col, .name = name };
}

static void
copy (PFarray_t *base, PFarray_t *content)
{
    for (unsigned int i = 0; i < PFarray_last (content); i++)
        *(name_origin_t *) PFarray_add (base) =
            *(name_origin_t *) PFarray_at (content, i);
}

/**
 * Infer properties about the original names; worker for prop_infer().
 */
static void
infer_name_origin (PFla_op_t *n)
{
    /*
     * Several operators (at least) propagate original
     * to their output 1:1.
     */
    switch (n->kind) {

        case la_serialize_seq:
        case la_serialize_rel:
        case la_attach:
        case la_cross:
        case la_eqjoin:
        case la_thetajoin:
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
        case la_rownum:
        case la_rowrank:
        case la_rank:
        case la_rowid:
        case la_type:
        case la_type_assert:
        case la_cast:
        case la_step_join:
        case la_guide_step_join:
        case la_doc_index_join:
        case la_doc_tbl:
        case la_doc_access:
        case la_docnode:
        case la_element:
        case la_attribute:
        case la_textnode:
        case la_comment:
        case la_processi:
        case la_content:
        case la_roots:
        case la_error:
        case la_trace_items:
        case la_proxy:
        case la_proxy_base:
        case la_dummy:

            /* propagate information from both input operators */
            for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
                copy (n->prop->name_origin, n->child[i]->prop->name_origin);
            break;

        default:
            break;
    }

    /*
     * Now consider more specific stuff from various rules.
     */
    switch (n->kind) {
        case la_ref_tbl:
            {
                PFarray_t   *fullname = PFarray (sizeof (char), 64);
                unsigned int len      = strlen (n->sem.ref_tbl.name) + 1;
                PFalg_col_t  col;
                char        *name;

                /* add the table name */
                PFarray_printf (fullname, "%s.", n->sem.ref_tbl.name);

                for (unsigned int i = 0; i < n->schema.count; i++) {
                    col  = n->schema.items[i].name;
                    name = *((char**) PFarray_at (n->sem.ref_tbl.tcols, i));

                    /* add the column name to the table name */
                    PFarray_printf (fullname, "%s", name);

                    add_name (n->prop, col, PFstrdup (fullname->base));

                    /* reset the column name */
                    PFarray_last (fullname) = len;
                }
            } break;

        case la_project:
            /*
             * projection does not affect properties, except for the
             * column name change.
             */
            for (unsigned int i = 0; i < n->sem.proj.count; i++) {
                char *name = PFprop_name_origin (L(n)->prop,
                                                 n->sem.proj.items[i].old);
                if (name)
                    add_name (n->prop, n->sem.proj.items[i].new, name);
            }
            break;

        case la_disjunion:
        case la_intersect:
            /*
             * add all columns that are constant in both input relations
             * and additionally both contain the same value
             */
            for (unsigned int i = 0; i < n->schema.count; i++) {
                PFalg_col_t col   = n->schema.items[i].name;
                char       *lname = PFprop_name_origin (L(n)->prop, col),
                           *rname = PFprop_name_origin (R(n)->prop, col);
                
                if (lname && rname && !strcmp (lname, rname))
                    add_name (n->prop, col, lname);
            }
            break;

        case la_semijoin:
        case la_difference:
            copy (n->prop->name_origin, L(n)->prop->name_origin);
            break;
            
        case la_aggr:
            for (unsigned int i = 0; i < n->sem.aggr.count; i++)
                if (n->sem.aggr.aggr[i].kind == alg_aggr_dist &&
                    PFprop_name_origin (L(n)->prop, n->sem.aggr.aggr[i].col))
                    add_name (n->prop,
                              n->sem.aggr.aggr[i].col,
                              PFprop_name_origin (L(n)->prop, 
                                                  n->sem.aggr.aggr[i].col));

            if (n->sem.aggr.part &&
                PFprop_name_origin (L(n)->prop, n->sem.aggr.part))
                add_name (
                        n->prop,
                        n->sem.aggr.part,
                        PFprop_name_origin (L(n)->prop, n->sem.aggr.part));
            break;

        case la_step:
        case la_guide_step:
            if (PFprop_name_origin (R(n)->prop, n->sem.step.iter))
                add_name (
                        n->prop,
                        n->sem.step.iter,
                        PFprop_name_origin (R(n)->prop, n->sem.step.iter));
            /* fall through */
        case la_step_join:
        case la_guide_step_join:
            {
                PFarray_t *name = PFarray (sizeof (char), 64);
                PFarray_printf (name, "%s(%s)",
                                PFalg_node_kind_str (n->sem.step.spec.kind),
                                (n->sem.step.spec.kind == node_kind_elem ||
                                 n->sem.step.spec.kind == node_kind_attr)
                                ?  PFqname_str (n->sem.step.spec.qname)
                                : "");
                                 
                add_name (n->prop,
                          n->sem.step.item_res,
                          PFstrdup (name->base));
            } break;

        case la_doc_index_join:
            add_name (n->prop, n->sem.doc_join.item_res, "node-id(index)");
            break;

        case la_doc_tbl:
            add_name (n->prop, n->sem.doc_tbl.res, "document-node()");
            break;

        case la_twig:
            switch (L(n)->kind) {
                case la_docnode:
                    if (PFprop_name_origin (L(n)->prop, L(n)->sem.docnode.iter))
                        add_name (
                                n->prop,
                                n->sem.iter_item.iter,
                                PFprop_name_origin (L(n)->prop,
                                                    L(n)->sem.docnode.iter));
                    break;

                case la_element:
                case la_textnode:
                case la_comment:
                    if (PFprop_name_origin (L(n)->prop, L(n)->sem.iter_item.iter))
                        add_name (
                                n->prop,
                                n->sem.iter_item.iter,
                                PFprop_name_origin (L(n)->prop,
                                                    L(n)->sem.iter_item.iter));
                    break;

                case la_attribute:
                case la_processi:
                    if (PFprop_name_origin (L(n)->prop,
                                      L(n)->sem.iter_item1_item2.iter))
                        add_name (n->prop,
                                  n->sem.iter_item.iter,
                                  PFprop_name_origin (
                                      L(n)->prop,
                                      L(n)->sem.iter_item1_item2.iter));
                    break;

                case la_content:
                    if (PFprop_name_origin (L(n)->prop,
                                      L(n)->sem.iter_pos_item.iter))
                        add_name (n->prop,
                                  n->sem.iter_item.iter,
                                  PFprop_name_origin (
                                      L(n)->prop,
                                      L(n)->sem.iter_pos_item.iter));
                    break;

                default:
                    break;
            }
            add_name (n->prop, n->sem.step.item, "node-id(transient)");
            break;

        case la_error:
        case la_nil:
        case la_cache:
        case la_trace:
        case la_trace_items:
        case la_trace_msg:
        case la_trace_map:
            /* we have no have properties */
            break;

        case la_rec_fix:
            /* get the constants of the overall result */
            copy (n->prop->constants, R(n)->prop->constants);
            break;

        case la_rec_param:
            /* recursion parameters do not have properties */
            break;

        case la_rec_arg:
            copy (n->prop->constants, R(n)->prop->constants);
            break;

        case la_rec_base:
            /* infer no properties of the seed */
            break;

        case la_fun_param:
            copy (n->prop->constants, L(n)->prop->constants);
            break;

        case la_serialize_seq:
        case la_serialize_rel:
        case la_side_effects:
        case la_lit_tbl:
        case la_empty_tbl:
        case la_attach:
        case la_cross:
        case la_eqjoin:
        case la_thetajoin:
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
        case la_rownum:
        case la_rowrank:
        case la_rank:
        case la_rowid:
        case la_type:
        case la_type_assert:
        case la_cast:
        case la_doc_access:
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
        case la_fun_call:
        case la_fun_frag_param:
        case la_proxy:
        case la_proxy_base:
        case la_string_join:
        case la_dummy:

            break;

        case la_internal_op:
            PFoops (OOPS_FATAL,
                    "internal optimization operator is not allowed here");
    }
}

/* worker for PFprop_infer_name_origin */
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

    /* reset name origin property
       (reuse already existing lists if already available
        as this increases the performance of the compiler a lot) */
    if (n->prop->name_origin)
        PFarray_last (n->prop->name_origin) = 0;
    else
        /* prepare the property for 10 names */
        n->prop->name_origin = PFarray (sizeof (const_t), 10);

    /* infer information on original column names */
    infer_name_origin (n);
}

/**
 * Infer original name property for a DAG rooted in root
 */
void
PFprop_infer_name_origin (PFla_op_t *root) {
    prop_infer (root);
    PFla_dag_reset (root);
}

/* vim:set shiftwidth=4 expandtab: */
