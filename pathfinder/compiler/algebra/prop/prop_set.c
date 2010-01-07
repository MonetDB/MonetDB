/**
 * @file
 *
 * Inference of set property of logical algebra expressions. This property
 * marks all operators that may return their result without duplicates.
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

#define SEEN(n)       (n)->bit_dag
#define EDGE(n)       (n)->refctr

/**
 * Test if an operator referenced via its container @a prop
 * may result a set of tuples instead of a bag of tuples.
 */
bool
PFprop_set (const PFprop_t *prop)
{
    return prop->set && prop->set_col == col_NULL;
}

/**
 * worker for PFprop_infer_set
 * infers the set property during the second run
 * (uses edge counter stored in EDGE(n) from the first run)
 */
static void
prop_infer_set (PFla_op_t *n, bool set)
{
    bool l_set = false,
         r_set = false;

    assert (n);

    /* collect the set properties of all parents */
    n->prop->set = n->prop->set && set;

    /* nothing to do if we haven't collected
       all incoming set properties of the parents */
    if (EDGE(n) > 1) {
        EDGE(n)--;
        return;
    }

    /* infer set property for children */
    switch (n->kind) {
        case la_serialize_seq:
        case la_serialize_rel:
        case la_side_effects:
        case la_cache:
        case la_trace:
        case la_trace_items:
        case la_trace_msg:
        case la_trace_map:
            l_set = false;
            r_set = false;
            break;

        case la_pos_select:
        case la_rownum:
        case la_rowid:
        case la_twig:
        case la_docnode:
        case la_attribute:
        case la_textnode:
        case la_comment:
        case la_processi:
            l_set = false;
            break;

        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
            break;

        case la_attach:
        case la_project:
        case la_select:
        case la_fun_1to1:
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_bool_not:
        case la_to:
        case la_rowrank:
        case la_rank:
        case la_type:
        case la_cast:
        case la_type_assert:
        case la_roots:
        case la_doc_tbl:
            l_set = n->prop->set;
            break;

        case la_cross:
        case la_eqjoin:
        case la_thetajoin:
        case la_disjunion:
        case la_intersect:
            l_set = n->prop->set;
            r_set = n->prop->set;
            break;

        case la_semijoin:
            l_set = n->prop->set;
            r_set = true;
            break;

        case la_difference:
            /* the left argument may not prune duplicates
               (as this might lead to more result tuples) */
            l_set = false;
            r_set = true;
            break;

        case la_distinct:
            l_set = true;
            break;

        case la_aggr:
            l_set = true;
            for (unsigned int i = 0; i < n->sem.aggr.count; i++)
                switch (n->sem.aggr.aggr[i].kind) {
                    case alg_aggr_dist:
                    case alg_aggr_min:
                    case alg_aggr_max:
                    case alg_aggr_all:
                        break;

                    case alg_aggr_count:
                    case alg_aggr_avg:
                    case alg_aggr_sum:
                    case alg_aggr_seqty1:
                    case alg_aggr_prod:
                        l_set = false;
                        break;
                }
            break;

        case la_step:
        case la_guide_step:
            l_set = false;
            r_set = true;
            break;

        case la_step_join:
        case la_guide_step_join:
        case la_doc_index_join:
            l_set = false;
            r_set = n->prop->set;
            break;

        case la_doc_access:
            l_set = false;
            r_set = n->prop->set;
            break;

        case la_fcns:
        case la_element:
        case la_content:
        case la_merge_adjacent:
        case la_string_join:
            l_set = false;
            r_set = false;
            break;

        case la_frag_union:
            r_set = false;
        case la_fragment:
        case la_frag_extract:
        case la_empty_frag:
            l_set = false;
            break;

        case la_error:
            l_set = false;
            r_set = true;
            break;

        /* For proxies we overwrite inferred property at its
           base by the input property of the proxy. */
        case la_proxy:
            prop_infer_set (L(n), n->prop->set);
            if (n->sem.proxy.kind == 1)
                n->sem.proxy.base1->prop->set = n->prop->set;

            /* start the property inference for the base operator */
            prop_infer_set (L(n->sem.proxy.base1),
                            n->sem.proxy.base1->prop->set);
            return;

        case la_proxy_base:
            /* avoid the inference as the proxy node
               may overwrite the property after traversal */
            return;

        case la_nil:
            break;

        /* ignore this property for the recursion */
        case la_rec_fix:
        case la_rec_param:
        case la_rec_arg:
            l_set = false;
            r_set = false;
        case la_rec_base:
            break;

        /* ignore this property for the function application */
        case la_fun_call:
        case la_fun_param:
        case la_fun_frag_param:
            l_set = false;
            r_set = false;
            break;

        case la_internal_op:
            PFoops (OOPS_FATAL,
                    "internal optimization operator is not allowed here");

        case la_dummy:
            l_set = n->prop->set;
            break;
    }

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        prop_infer_set (n->child[i], i==0?l_set:r_set);
}

/**
 * worker for PFprop_infer_set_extended
 * infers the set property during the second run
 * (uses edge counter stored in EDGE(n) from the first run)
 */
static void
prop_infer_set_extended (PFla_op_t *n, bool set, PFalg_col_t col)
{
    PFalg_col_t l_col,
                r_col;
    bool l_set,
         r_set;

    assert (n);

    /* collect the set properties of all parents */
    if (!n->prop->set || !set)
        n->prop->set = false;
    else if (n->prop->set_col && col &&
             n->prop->set_col != col)
        n->prop->set = false;
    else if (!n->prop->set_col && col)
        n->prop->set_col = col;

    /* nothing to do if we haven't collected
       all incoming set properties of the parents */
    if (EDGE(n) > 1) {
        EDGE(n)--;
        return;
    }

    /* in case we MAYBE have the set property set
       and the column does not appear anymore than
       we remove any set information */
    if (n->prop->set_col &&
        !PFprop_ocol (n, n->prop->set_col)) {
        n->prop->set = false;
        n->prop->set_col = col_NULL;
    }

    /* keep input properties */
    l_set = n->prop->set;
    r_set = n->prop->set;
    l_col = n->prop->set_col;
    r_col = n->prop->set_col;

    /* infer set property for children */
    switch (n->kind) {
        case la_serialize_seq:
        case la_serialize_rel:
        case la_side_effects:
        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
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
        case la_cache:
        case la_trace:
        case la_trace_items:
        case la_trace_msg:
        case la_trace_map:
        case la_rec_fix:
        case la_rec_param:
        case la_rec_arg:
        case la_rec_base:
        case la_fun_call:
        case la_fun_param:
        case la_fun_frag_param:
        case la_string_join:
            /* do not infer set information to the children */
            l_set = false;
            r_set = false;
            break;

        case la_attach:
        case la_cross:
        case la_eqjoin:
        case la_thetajoin:
        case la_select:
        case la_disjunion:
        case la_intersect:
        case la_fun_1to1:
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_bool_not:
        case la_to:
        case la_rowrank:
        case la_rank:
        case la_type:
        case la_cast:
        case la_type_assert:
        case la_doc_tbl:
        case la_roots:
        case la_dummy:
            /* adopt set information for the children */
            break;

        case la_semijoin:
            /* adopt set information for left argument
               and allow duplicates in the right argument */
            r_col = col_NULL;
            r_set = true;
            break;

        case la_project:
            /* maintain the set column name */
            if (n->prop->set_col) {
                unsigned int i;
                for (i = 0; i < n->sem.proj.count; i++)
                    if (n->prop->set_col == n->sem.proj.items[i].new) {
                        l_col = n->sem.proj.items[i].old;
                        break;
                    }
                assert (i < n->sem.proj.count);
            }
            break;

        case la_pos_select:
        case la_rownum:
            /* Switch the set property from TRUE to MAYBE (TRUE+col)
               if there is a partition column col and keep the MAYBE
               information if it stores the same column name. */
            if (n->prop->set && n->sem.sort.part) {
                if ((n->prop->set_col && n->prop->set_col == n->sem.sort.part) ||
                    !n->prop->set_col) {
                    l_col = n->sem.sort.part;
                    l_set = true;
                    break;
                }
            }
            l_set = false;
            break;

        case la_difference:
            /* the left argument may not prune duplicates
               (as this might lead to more result tuples) */
            l_set = false;
            /* and allow duplicates in the right argument */
            r_col = col_NULL;
            r_set = true;
            break;

        case la_distinct:
            /* allow duplicates for the argument */
            l_col = col_NULL;
            l_set = true;
            break;

        case la_aggr:
            l_col = col_NULL;
            l_set = true;

            for (unsigned int i = 0; i < n->sem.aggr.count; i++) {
                switch (n->sem.aggr.aggr[i].kind) {
                    case alg_aggr_dist:
                    case alg_aggr_min:
                    case alg_aggr_max:
                    case alg_aggr_all:
                        break;

                    case alg_aggr_count:
                    case alg_aggr_avg:
                    case alg_aggr_sum:
                    case alg_aggr_seqty1:
                    case alg_aggr_prod:
                        /* Switch the set property from TRUE to MAYBE (TRUE+col)
                           if there is a partition column col and keep the MAYBE
                           information if it stores the same column name. */
                        if (n->prop->set &&
                            n->sem.aggr.part &&
                            ((n->prop->set_col &&
                              n->prop->set_col == n->sem.aggr.part) ||
                             !n->prop->set_col)) {
                            l_col = n->sem.aggr.part;
                        }
                        else {
                            l_set = false;
                        }
                        break;
                }
                if (!l_set) {
                    l_col = col_NULL;
                    break;
                }
            }
            break;

        case la_rowid:
            /* Introduce a set property for the children if
               duplicates are MAYBE (TRUE+col) allowed and
               column col is generated by the rowid operator.

               Furthermore ensure that the real values of the 
               generated column are never used. */
            if (n->prop->set && n->prop->set_col &&
                n->prop->set_col == n->sem.rowid.res &&
                PFprop_req_bijective_col (n->prop, n->prop->set_col)) {
                l_col = col_NULL;
                l_set = true;
                break;
            }
            l_set = false;
            break;

        case la_step:
        case la_guide_step:
            /* ignore fragment information */
            l_set = false;
            r_col = col_NULL;
            r_set = true;
            break;

        case la_step_join:
        case la_guide_step_join:
        case la_doc_index_join:
            /* ignore fragment information */
            l_set = false;
            r_set = n->prop->set;
            break;

        case la_doc_access:
            /* ignore fragment information */
            l_set = false;
            r_set = n->prop->set;
            break;

        case la_error:
            /* ignore side_effect information */
            l_set = false;
            /* allow duplicates for runtime errors */
            r_col = col_NULL;
            r_set = true;
            break;

        /* For proxies we overwrite inferred property at its
           base by the input property of the proxy. */
        case la_proxy:
            prop_infer_set_extended (L(n), n->prop->set, l_col);
            if (n->sem.proxy.kind == 1)
                n->sem.proxy.base1->prop->set = n->prop->set;

            /* start the property inference for the base operator */
            prop_infer_set_extended (L(n->sem.proxy.base1),
                            n->sem.proxy.base1->prop->set,
                            l_col);
            return;

        case la_proxy_base:
            /* avoid the inference as the proxy node
               may overwrite the property after traversal */
            return;

        case la_internal_op:
            PFoops (OOPS_FATAL,
                    "internal optimization operator is not allowed here");
    }

    if (!l_set) l_col = col_NULL;
    if (!r_set) r_col = col_NULL;

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        prop_infer_set_extended (n->child[i], i==0?l_set:r_set, i==0?l_col:r_col);
}

/* worker for PFprop_infer_set */
static void
prop_infer (PFla_op_t *n)
{
    assert (n);

    /* count number of incoming edges
       (during first run) */
    EDGE(n)++;

    /* nothing to do if we already visited that node */
    if (SEEN(n))
        return;
    /* otherwise initialize edge counter (first occurrence) */
    else
        EDGE(n) = 1;

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        prop_infer (n->child[i]);

    SEEN(n) = true;

    /* reset set property */
    n->prop->set = true;
    n->prop->set_col = col_NULL;
}

/**
 * Infer set property (duplicates can be thrown away)
 * for a DAG rooted in @a root
 */
void
PFprop_infer_set (PFla_op_t *root) {
    /* collect number of incoming edges (parents) */
    prop_infer (root);
    PFla_dag_reset (root);

    /* second run infers set property */
    prop_infer_set (root, false);
}

/**
 * Infer set property (duplicates can be thrown away)
 * for a DAG rooted in @a root
 */
void
PFprop_infer_set_extended (PFla_op_t *root) {
    PFprop_infer_reqval (root);
    /* collect number of incoming edges (parents) */
    prop_infer (root);
    PFla_dag_reset (root);

    /* second run infers set property */
    prop_infer_set_extended (root, false, col_NULL);
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
