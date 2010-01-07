/**
 * @file
 *
 * Inference of cardinality property of logical algebra expressions.
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

/**
 * Return cardinality stored in property container @a prop.
 */
unsigned int
PFprop_card (const PFprop_t *prop)
{
    assert (prop);

    return prop->card;
}


static void prop_infer (PFla_op_t *n);

/**
 * Infer properties about cardinalities; worker for prop_infer().
 */
static void
infer_card (PFla_op_t *n)
{
    switch (n->kind) {
        case la_serialize_seq:
        case la_serialize_rel:
        case la_doc_access:
        case la_content:
            /* cardinality stays the same */
            n->prop->card = R(n)->prop->card;
            break;

        case la_side_effects:
        case la_eqjoin:
        case la_semijoin:
        case la_thetajoin:
        case la_select:
        case la_pos_select:
        case la_intersect:
        case la_difference:
        case la_step:
        case la_step_join:
        case la_doc_index_join:
        case la_fcns:
        case la_textnode:
        case la_merge_adjacent:
        case la_fragment:
        case la_frag_extract:
        case la_frag_union:
        case la_empty_frag:
        case la_fun_frag_param:
        case la_string_join:
            /* can't say something specific about cardinality */
            n->prop->card = 0;
            break;

        case la_attach:
        case la_project:
        case la_fun_1to1:
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_bool_not:
        case la_rownum:
        case la_rowrank:
        case la_rank:
        case la_rowid:
        case la_type:
        case la_type_assert:
        case la_cast:
        case la_doc_tbl:
        case la_twig:
        case la_docnode:
        case la_element:
        case la_attribute:
        case la_comment:
        case la_processi:
        case la_roots:
        case la_fun_param:
        case la_proxy:
        case la_proxy_base:
        case la_dummy:
            /* cardinality stays the same */
            n->prop->card = L(n)->prop->card;
            break;

        case la_lit_tbl:
            /* number of tuples */
            n->prop->card = n->sem.lit_tbl.count;
            break;

        case la_empty_tbl:
        case la_ref_tbl:
            /* zero tuples */
            n->prop->card = 0;
            break;

        case la_cross:
            /* multiply both children cardinalities */
            n->prop->card = L(n)->prop->card * R(n)->prop->card;
            break;

        case la_disjunion:
            /* add cardinalities of both children if we know
               both of them */
            n->prop->card = L(n)->prop->card && R(n)->prop->card ?
                            L(n)->prop->card + R(n)->prop->card : 0;
            break;

        case la_distinct:
            if (L(n)->prop->card == 1)
                n->prop->card = 1;
            else
                /* can't say something specific about cardinality */
                n->prop->card = 0;
            break;

        case la_to:
            /* with constant information
               we could infer the cardinality better */
            n->prop->card = 0;
            break;

        case la_aggr:
            /* if part is not present the
               aggregation yields only one tuple */
            n->prop->card = n->sem.aggr.part ? 0 : 1;
            break;

        case la_guide_step:
        case la_guide_step_join:
            if (R(n)->prop->card == 1 &&
                n->sem.step.guide_count == 1 &&
                n->sem.step.guides[0]->count == 1)
                n->prop->card = 1;
            else
                n->prop->card = 0;
            break;

        case la_error:
        case la_nil:
        case la_cache:
        case la_trace:
        case la_trace_items:
        case la_trace_msg:
        case la_trace_map:
            /* there is no property to infer */
            break;

        case la_rec_fix:
            /* get the cardinality of the overall result */
            n->prop->card = R(n)->prop->card;
            break;

        case la_rec_param:
            /* recursion parameters do not have properties */
            break;

        case la_rec_arg: {
            PFla_op_t *base = n->sem.rec_arg.base;
            /* infer the cardinality of the left child */
            prop_infer (L(n));

            /* set the base to the just inferred cardinality */
            base->prop->card = L(n)->prop->card;

            /* infer the cardinality of the body under the
             * new circumstances */
            prop_infer (R(n));

            /* if the inferred cardinality is the same as
             * the cardinality inferred for the base,
             * we are sure that the recursion will return
             * a result in any case */
            if ((R(n)->prop) &&
                (R(n)->prop->card == base->prop->card)) {
                n->prop->card = R(n)->prop->card;
                break;
            }

            /* in every other case we infer the cardinality
             * like before: we infer nothing about the cardinality
             * of the seed operator and infer the cardinality of
             * the recursion body again */
            base->prop->card = 0;
            prop_infer (R(n));
            n->prop->card = R(n)->prop->card;
        }   break;

        case la_rec_base:
            /* do nothing, magic happens in rec_arg */
            break;

        case la_fun_call:
            if (n->sem.fun_call.occ_ind == alg_occ_exactly_one)
                n->prop->card = L(n)->prop->card;
            else
                n->prop->card = 0;
            break;

        case la_internal_op:
            /* interpret this operator as cross product */
            if (n->sem.eqjoin_opt.kind == la_cross)
                /* multiply both children cardinalities */
                n->prop->card = L(n)->prop->card * R(n)->prop->card;
            else
                n->prop->card = 0;
            break;
    }
}

/* worker for PFprop_infer_card */
static void
prop_infer (PFla_op_t *n)
{
    bool topdown;
    assert (n);

    /* nothing to do if we already visited that node */
    if (n->bit_dag)
        return;

    switch (n->kind) {
        case la_rec_arg:
            topdown = true;
        default:
            topdown = false;
    }

    /* infer properties for children */
    if (!topdown)
        for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
            prop_infer (n->child[i]);

    n->bit_dag = true;

    /* reset cardinality property */
    n->prop->card = 0;

    /* infer cardinality information */
    infer_card (n);
}

/**
 * Infer cardinality property for a DAG rooted in @a root
 */
void
PFprop_infer_card (PFla_op_t *root) {
    prop_infer (root);
    PFla_dag_reset (root);
}

/* vim:set shiftwidth=4 expandtab: */
