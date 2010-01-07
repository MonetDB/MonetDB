/**
 * @file
 *
 * Inference of functional dependencies in logical algebra expressions.
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

/* worker for PFprop_fd */
static bool
find_fd (PFarray_t *fds, PFalg_col_t col1, PFalg_col_t col2)
{
    if (!fds) return false;

    if (col1 == col2) return true;

    for (unsigned int i = 0; i < PFarray_last (fds); i++)
        if (col1 == ((fd_t *) PFarray_at (fds, i))->col1 &&
            col2 == ((fd_t *) PFarray_at (fds, i))->col2)
            return true;

    return false;
}

/**
 * Test if a column @a dependent functionally depends on column @a describing.
 * in the list of functional dependencies in container @a prop
 */
bool
PFprop_fd (const PFprop_t *prop, PFalg_col_t describing, PFalg_col_t dependent)
{
    if (!prop) return false;

    return find_fd (prop->fds, describing, dependent);
}

/**
 * Add a new functional dependency to the list of FDs (@a fds).
 */
static void
add_fd (PFarray_t *fds, PFalg_col_t describing, PFalg_col_t dependent)
{
    assert (fds);

    if (!find_fd (fds, describing, dependent))
        *(fd_t *) PFarray_add (fds)
            = (fd_t) { .col1 = describing, .col2 = dependent };

    /* and build transitive closure */
    for (unsigned int i = 0; i < PFarray_last (fds); i++)
        if (dependent == ((fd_t *) PFarray_at (fds, i))->col1 &&
            !find_fd (fds, describing, ((fd_t *) PFarray_at (fds, i))->col2))
            *(fd_t *) PFarray_add (fds)
                = (fd_t) { .col1 = describing,
                           .col2 = ((fd_t *) PFarray_at (fds, i))->col2 };
}

/**
 * Add all input functional dependencies of node @a child to
 * the list of FDs (@a fds).
 */
static void
bulk_add_fds (PFarray_t *fds, PFla_op_t *child)
{
    assert (fds);
    assert (child);
    assert (child->prop);
    assert (child->prop->fds);

    for (unsigned int i = 0; i < PFarray_last (child->prop->fds); i++)
        *(fd_t *) PFarray_add (fds) =
            *(fd_t *) PFarray_at (child->prop->fds, i);
}


/**
 * Infer functional dependency properties; worker for prop_infer().
 */
static void
infer_functional_dependencies (PFla_op_t *n)
{
    PFarray_t *fds = n->prop->fds;

    switch (n->kind) {
        /* copy only right FDs */
        case la_serialize_seq:
        case la_serialize_rel:
        case la_doc_index_join:
        case la_doc_access:
            bulk_add_fds (fds, R(n));
            break;
			
        /* do nothing */
        case la_side_effects:
        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
        case la_disjunion:
        case la_rownum:
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
        case la_fragment:
        case la_frag_extract:
        case la_frag_union:
        case la_empty_frag:
        case la_error:
        case la_nil:
        case la_cache:
        case la_trace:
        case la_rec_fix:
        case la_rec_param:
        case la_rec_arg:
        case la_rec_base:
        case la_fun_call:
        case la_fun_param:
        case la_fun_frag_param:
        case la_internal_op:
        case la_string_join:
            break;

        case la_attach:
            bulk_add_fds (fds, L(n));
            /* add for all input columns a FD (col -> res) */
            for (unsigned int i = 0; i <  L(n)->schema.count; i++)
                add_fd (fds, L(n)->schema.items[i].name, n->sem.attach.res);
            break;

        /* copy left and right FDs */
        case la_cross:
        case la_thetajoin:
        case la_intersect:
            bulk_add_fds (fds, L(n));
            bulk_add_fds (fds, R(n));
            break;

        case la_eqjoin:
        {
            PFarray_t  *lfds = L(n)->prop->fds,
                       *rfds = R(n)->prop->fds;
            PFalg_col_t cur_key;

            /* copy left and right FDs */
            bulk_add_fds (fds, L(n));
            bulk_add_fds (fds, R(n));

            /* everything that functionally depends on a join column
               also depends on the other join column */
            for (unsigned int i = 0; i < L(n)->schema.count; i++)
                if (find_fd (fds, n->sem.eqjoin.col1, L(n)->schema.items[i].name))
                    add_fd (fds, n->sem.eqjoin.col2, L(n)->schema.items[i].name);

            for (unsigned int i = 0; i < R(n)->schema.count; i++)
                if (find_fd (fds, n->sem.eqjoin.col2, R(n)->schema.items[i].name))
                    add_fd (fds, n->sem.eqjoin.col1, R(n)->schema.items[i].name);

            /* build up transitive FDs for join columns */
            if (PFprop_key_left (n->prop, n->sem.eqjoin.col1)) {
                for (unsigned int i = 0; i < PFarray_last (rfds); i++)
                    if (((fd_t *) PFarray_at (rfds, i))->col1 ==
                        n->sem.eqjoin.col2)
                        for (unsigned int j = 0; j < L(n)->schema.count; j++) {
                            cur_key = L(n)->schema.items[j].name;
                            if (cur_key != n->sem.eqjoin.col1 &&
                                PFprop_key_left (n->prop, cur_key))
                                add_fd (fds,
                                        cur_key,
                                        ((fd_t *) PFarray_at (rfds, i))->col2);
                        }
            }
            if (PFprop_key_right (n->prop, n->sem.eqjoin.col2)) {
                for (unsigned int i = 0; i < PFarray_last (lfds); i++)
                    if (((fd_t *) PFarray_at (lfds, i))->col1 ==
                        n->sem.eqjoin.col1)
                        for (unsigned int j = 0; j < R(n)->schema.count; j++) {
                            cur_key = R(n)->schema.items[j].name;
                            if (cur_key != n->sem.eqjoin.col2 &&
                                PFprop_key_right (n->prop, cur_key))
                                add_fd (fds,
                                        cur_key,
                                        ((fd_t *) PFarray_at (lfds, i))->col2);
                        }
            }
        }   break;

        /* copy only left FDs */
        case la_semijoin:
        case la_select:
        case la_pos_select:
        case la_difference:
        case la_distinct:
        case la_fun_1to1:
        case la_rowid:
        case la_type:
        case la_type_assert:
        case la_roots:
        case la_trace_items:
        case la_trace_msg:
        case la_trace_map:
        case la_proxy:
        case la_proxy_base:
        case la_dummy:
            bulk_add_fds (fds, L(n));
            break;

        case la_aggr:
            if (n->sem.aggr.part)
                for (unsigned int i = 0; i < n->sem.aggr.count; i++)
                    add_fd (fds, n->sem.aggr.part, n->sem.aggr.aggr[i].col);
            break;

        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_to:
            bulk_add_fds (fds, L(n));

            /* if a column can describe the inputs
               it can also describe the output */
            for (unsigned int i = 0; i < L(n)->schema.count; i++)
                if (find_fd (fds,
                             L(n)->schema.items[i].name,
                             n->sem.binary.col1) &&
                    find_fd (fds,
                             L(n)->schema.items[i].name,
                             n->sem.binary.col2))
                    add_fd (fds,
                            L(n)->schema.items[i].name,
                            n->sem.binary.res);
            break;

        case la_project:
        {
            PFarray_t  *lfds = L(n)->prop->fds;
            PFalg_col_t col1,
                        col2;

            /* Keep FDs where both arguments are still visible
               after the projection. */
            for (unsigned int ci = 0; ci < PFarray_last (lfds); ci++) {
                col1 = ((fd_t *) PFarray_at (lfds, ci))->col1;
                col2 = ((fd_t *) PFarray_at (lfds, ci))->col2;
                for (unsigned int i = 0; i <  n->sem.proj.count; i++)
                    /* check for the visibility of the first fd column */
                    if (col1 == n->sem.proj.items[i].old) {
                        for (unsigned int j = 0; j <  n->sem.proj.count; j++)
                            /* check for visibility of the second fd column */
                            if (col2 == n->sem.proj.items[j].old) {
                                /* add the (possibly renamed) fd */
                                add_fd (fds,
                                        n->sem.proj.items[i].new,
                                        n->sem.proj.items[j].new);
                                break;
                            }
                        break;
                    }
            }
        }   break;

        /* copy left FDs and
           add FD (input -> res) and FD (res -> intput) */
        case la_bool_not:
            bulk_add_fds (fds, L(n));
            add_fd (fds, n->sem.unary.col, n->sem.unary.res);
            add_fd (fds, n->sem.unary.res, n->sem.unary.col);
            break;

        /* copy left FDs and
           add all FDs (res -> sort) */
        case la_rowrank:
        case la_rank:
            bulk_add_fds (fds, L(n));
            for (unsigned int i = 0; i < PFord_count (n->sem.sort.sortby); i++)
                add_fd (fds,
                        n->sem.sort.res,
                        PFord_order_col_at (n->sem.sort.sortby, i));
            break;

        /* copy left FDs and
           add FD (input -> res) */
        case la_cast:
            bulk_add_fds(fds, L(n));
            add_fd (fds, n->sem.type.col, n->sem.type.res);
            break;


        /* copy right FDs and
           add FD (res -> input) for some special conditions */
        case la_step_join:
        case la_guide_step_join:
            bulk_add_fds (fds, R(n));
            /* Each attribute result node has exactly one owner,
               each result node along a child axis has exactly one
               parent, and each result of a self step is the node
               itself. The context thus depends functionally on the
               result.
               For descendant and descendant-or-self step we can
               guarantee the functional dependency by the knowledge
               that the step starts from a given level and thus the
               results may not overlap. */
            if (n->sem.step.spec.axis == alg_attr ||
                n->sem.step.spec.axis == alg_chld ||
                n->sem.step.spec.axis == alg_self ||
                ((n->sem.step.spec.axis == alg_desc_s ||
                  n->sem.step.spec.axis == alg_desc) &&
                 LEVEL_KNOWN(PFprop_level (n->prop, n->sem.step.item))))
                add_fd (fds, n->sem.step.item_res, n->sem.step.item);
            break;

        /* copy left FDs and
           add FD (input -> res) and FD (res -> intput) */
        case la_doc_tbl:
            bulk_add_fds (fds, L(n));
            add_fd (fds, n->sem.doc_tbl.col, n->sem.doc_tbl.res);
            add_fd (fds, n->sem.doc_tbl.res, n->sem.doc_tbl.col);
            break;
    }

    /* Combine all key columns with all other columns to form FDs. */
    for (unsigned int i = 0; i < n->schema.count; i++)
        if (PFprop_key (n->prop, n->schema.items[i].name)) {
            PFalg_col_t key_col = n->schema.items[i].name;
            for (unsigned int j = 0; j < n->schema.count; j++)
                if (j != i) add_fd (fds, key_col, n->schema.items[j].name);
        }
}

static void
reset_property (PFla_op_t *n)
{
    /* reset the functional dependency information */
    if (n->prop->fds)
        PFarray_last (n->prop->fds) = 0;
    else
        n->prop->fds = PFarray (sizeof (fd_t), ARRAY_SIZE(n));
}

/* worker for PFprop_infer_functional_dependencies */
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
    reset_property (n);

    /* infer functional dependencies */
    infer_functional_dependencies (n);
}

/**
 * Infer functional dependencies for a DAG rooted in root
 */
void
PFprop_infer_functional_dependencies (PFla_op_t *root)
{
    /* get the key information for a more useful FD inference */
    PFprop_infer_key (root);

    prop_infer (root);
    PFla_dag_reset (root);
}

/* vim:set shiftwidth=4 expandtab: */
