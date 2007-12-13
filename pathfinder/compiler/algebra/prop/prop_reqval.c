/**
 * @file
 *
 * Inference of required value property of logical algebra expressions.
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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"

#include <assert.h>

#include "properties.h"

#include "alg_dag.h"
#include "oops.h"
#include "mem.h"


/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])

#define SEEN(p) ((p)->bit_dag)

/**
 * Test if @a attr is in the list of required value columns
 * in container @a prop
 */
bool
PFprop_reqval (const PFprop_t *prop, PFalg_att_t attr)
{
    return prop->reqvals.name & attr;
}

/**
 * Looking up required value of column @a attr
 * in container @a prop
 */
bool
PFprop_reqval_val (const PFprop_t *prop, PFalg_att_t attr)
{
    return prop->reqvals.val & attr;
}

/**
 * Returns union of two reqvals lists
 */
static PFalg_att_t
union_ (PFalg_att_t a, PFalg_att_t b)
{
    return a | b;
}

/**
 * Returns difference of two reqvals lists
 */
static PFalg_att_t
diff (PFalg_att_t a, PFalg_att_t b)
{
    return a & (~b);
}

/**
 * worker for PFprop_infer_reqval
 * infers the required values property during the second run
 * (uses edge counter stored in n->state_label from the first run)
 */
static void
prop_infer_reqvals (PFla_op_t *n, reqval_t reqvals)
{
    reqval_t rv;
    assert (n);

    /* make sure that relations that require all values
       overrule any decision */
    if (SEEN(n) && !reqvals.name)
        n->prop->reqvals = (reqval_t) { .name = 0, .val = 0 };

    /* in all calls (except the first) we ensure that
       we already have required value columns */
    if (SEEN(n) && n->prop->reqvals.name) {
        /* Check if both parents look for required values
           in the same columns and then resolve the possible
           conflicts */
        if (n->prop->reqvals.name == reqvals.name) {
            PFalg_att_t  overlap   = reqvals.name;
            unsigned int bit_shift = 1;

            while (bit_shift <= overlap) {
                /* if the values of column that is required by both
                   parents do not match remove this column from the
                   list of required value columns */
                if (bit_shift & overlap &&
                    ((bit_shift & reqvals.val) !=
                     (bit_shift & n->prop->reqvals.val))) {
                    /* remove entry from the list */
                    n->prop->reqvals.name = diff (n->prop->reqvals.name,
                                                  bit_shift);
                    n->prop->reqvals.val = diff (n->prop->reqvals.val,
                                                 bit_shift);
                }
                bit_shift <<= 1;
            }
        } else
            n->prop->reqvals = (reqval_t) { .name = 0, .val = 0 };
    }

    /* in the first call we use the required values of the caller */
    if (!SEEN(n)) {
        n->prop->reqvals = reqvals;
        SEEN(n) = true;
    }

    /* nothing to do if we haven't collected
       all incoming required values lists of that node */
    if (n->state_label > 1) {
        n->state_label--;
        return;
    }

    /* copy current required value list */
    rv = n->prop->reqvals;

    /**
     * Infer required values property for n's children:
     *
     * - 'select' introduces new required value columns
     * - 'not' extends required values list with a new column
     *   if the result also is one.
     * - all other operators either ignore the required value columns
     *   or infer them (partially) to their children
     */
    switch (n->kind) {
        case la_serialize_seq:
            rv.name = 0;
            rv.val = 0;
            prop_infer_reqvals (L(n), rv);
            prop_infer_reqvals (R(n), n->prop->reqvals);
            break;

        case la_serialize_rel:
            prop_infer_reqvals (L(n), n->prop->reqvals);
            break;

        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
        case la_empty_frag:
            break;

        case la_attach:
            rv.name = diff (rv.name, n->sem.attach.res);
            rv.val = diff (rv.val, n->sem.attach.res);
            prop_infer_reqvals (L(n), rv);
            break;

        case la_cross:
        case la_eqjoin:
        case la_thetajoin:
        case la_disjunion:
        case la_intersect:
            prop_infer_reqvals (L(n), rv);
            prop_infer_reqvals (R(n), rv);
            break;

        case la_project:
            rv.name = 0;
            rv.val = 0;
            /* rename reqvals columns from new to old */
            for (unsigned int i = 0; i < n->sem.proj.count; i++)
                if (n->prop->reqvals.name & n->sem.proj.items[i].new) {
                    rv.name = union_ (rv.name, n->sem.proj.items[i].old);
                    /* keep values but map them to the old column name */
                    if (n->prop->reqvals.val & n->sem.proj.items[i].new)
                        rv.val = union_ (rv.val, n->sem.proj.items[i].old);
                }
            prop_infer_reqvals (L(n), rv);
            break;

        case la_select:
            /* introduce new required value column */
            n->prop->reqvals.name = union_ (n->prop->reqvals.name,
                                            n->sem.select.att);
            n->prop->reqvals.val = union_ (n->prop->reqvals.val,
                                           n->sem.select.att);
            rv.name = union_ (rv.name, n->sem.select.att);
            rv.val = union_ (rv.val, n->sem.select.att);
            prop_infer_reqvals (L(n), rv);
            break;

        case la_pos_select:
        case la_distinct:
        case la_rownum:  /* for rownum, rowrank, rank, and rowid */
        case la_rowrank: /* type of res is != boolean and */
        case la_rank:    /* therefore never needs to be removed */
        case la_rowid:
        case la_type_assert:
        case la_roots:
        case la_proxy:
        case la_proxy_base:
        case la_dummy:
        case la_error:
            /* propagate required values list to left subtree */
            prop_infer_reqvals (L(n), rv);
            break;

        case la_semijoin:
        case la_difference:
        case la_cond_err:
            prop_infer_reqvals (L(n), rv);

            rv.name = 0;
            rv.val = 0;
            prop_infer_reqvals (R(n), rv);
            break;

        case la_fun_1to1:
            rv.name = diff (rv.name, n->sem.fun_1to1.res);
            rv.val = diff (rv.val, n->sem.fun_1to1.res);
            prop_infer_reqvals (L(n), rv);
            break;

        case la_num_eq:
        case la_num_gt:
            rv.name = diff (rv.name, n->sem.binary.res);
            rv.val = diff (rv.val, n->sem.binary.res);
            prop_infer_reqvals (L(n), rv);
            break;

        case la_bool_and:
            if (PFprop_reqval (n->prop, n->sem.binary.res) &&
                PFprop_reqval_val (n->prop, n->sem.binary.res)) {
                rv.name = union_ (rv.name, n->sem.binary.att1);
                rv.val = union_ (rv.val, n->sem.binary.att1);
                rv.name = union_ (rv.name, n->sem.binary.att2);
                rv.val = union_ (rv.val, n->sem.binary.att2);
            }
            prop_infer_reqvals (L(n), rv);
            break;

        case la_bool_or:
            if (PFprop_reqval (n->prop, n->sem.binary.res) &&
                !PFprop_reqval_val (n->prop, n->sem.binary.res)) {
                rv.name = union_ (rv.name, n->sem.binary.att1);
                rv.val = diff (rv.val, n->sem.binary.att1);
                rv.name = union_ (rv.name, n->sem.binary.att2);
                rv.val = diff (rv.val, n->sem.binary.att2);
            }
            prop_infer_reqvals (L(n), rv);
            break;

        case la_bool_not:
            /* if res is a required value column also add att
               with the switched boolean value */
            if (PFprop_reqval (n->prop, n->sem.unary.res)) {
                rv.name = union_ (rv.name, n->sem.unary.att);
                /* add positive value if res is wrong otherwise
                   value stays false (default) */
                if (!PFprop_reqval_val (n->prop, n->sem.unary.res))
                    rv.val = union_ (rv.val, n->sem.unary.att);
            }
            rv.name = diff (rv.name, n->sem.unary.res);
            rv.val = diff (rv.val, n->sem.unary.res);
            prop_infer_reqvals (L(n), rv);
            break;

        case la_to:
        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_count:
        case la_seqty1:
        case la_all:
        case la_doc_tbl:
        case la_twig:
        case la_docnode:
        case la_attribute:
        case la_textnode:
        case la_comment:
        case la_processi:
        case la_fragment:
        case la_frag_extract:
            rv.name = 0;
            rv.val = 0;
            prop_infer_reqvals (L(n), rv);
            break;

        case la_cast:
            if (n->sem.type.ty == aat_bln) {
                bool att_bln = false;
                for (unsigned int i = 0; i < n->schema.count; i++)
                    if (n->sem.type.att == n->schema.items[i].name) {
                        att_bln = (n->schema.items[i].type == aat_bln);
                        break;
                    }

                if (att_bln && PFprop_reqval (n->prop, n->sem.type.res)) {
                    rv.name = union_ (rv.name, n->sem.type.att);
                    if (PFprop_reqval_val (n->prop, n->sem.type.res))
                        rv.val = union_ (rv.val, n->sem.type.att);
                }
            }
            /* fall through */
        case la_type:
            rv.name = diff (rv.name, n->sem.type.res);
            rv.val = diff (rv.val, n->sem.type.res);
            prop_infer_reqvals (L(n), rv);
            break;

        case la_step:
        case la_guide_step:
        case la_fcns:
        case la_element:
        case la_content:
        case la_merge_adjacent:
        case la_frag_union:
        case la_string_join:
            rv.name = 0;
            rv.val = 0;
            prop_infer_reqvals (L(n), rv);
            prop_infer_reqvals (R(n), rv);
            break;

        case la_step_join:
        case la_guide_step_join:
            rv.name = 0;
            rv.val = 0;
            prop_infer_reqvals (L(n), rv);

            rv.name = diff (n->prop->reqvals.name, n->sem.step.item_res);
            rv.val = diff (n->prop->reqvals.val, n->sem.step.item_res);
            prop_infer_reqvals (R(n), rv);
            break;

        case la_doc_index_join:
            rv.name = 0;
            rv.val = 0;
            prop_infer_reqvals (L(n), rv);

            rv.name = diff (n->prop->reqvals.name, n->sem.doc_join.item_res);
            rv.val = diff (n->prop->reqvals.val, n->sem.doc_join.item_res);
            prop_infer_reqvals (R(n), rv);
            break;

        case la_doc_access:
            rv.name = 0;
            rv.val = 0;
            prop_infer_reqvals (L(n), rv);

            rv.name = diff (n->prop->reqvals.name, n->sem.doc_access.res);
            rv.val = diff (n->prop->reqvals.val, n->sem.doc_access.res);
            prop_infer_reqvals (R(n), rv);
            break;

        case la_nil:
            break;

        case la_trace:
            prop_infer_reqvals (L(n), rv);

            rv.name = 0;
            rv.val = 0;
            prop_infer_reqvals (R(n), rv);
            break;

        case la_trace_msg:
        case la_trace_map:
            rv.name = 0;
            rv.val = 0;
            prop_infer_reqvals (L(n), rv);
            prop_infer_reqvals (R(n), rv);
            break;

        case la_rec_fix:
            /* infer no required values */
            rv.name = 0;
            rv.val = 0;
            prop_infer_reqvals (L(n), rv);
            prop_infer_reqvals (R(n), rv);
            break;

        case la_rec_param:
        case la_rec_arg:
            /* infer no required values */
            rv.name = 0;
            rv.val = 0;
            prop_infer_reqvals (L(n), rv);
            prop_infer_reqvals (R(n), rv);
            break;

        case la_rec_base:
            break;
            
        case la_fun_call:
        case la_fun_param:
        case la_fun_frag_param:
            /* infer no required values */
            rv.name = 0;
            rv.val = 0;
            prop_infer_reqvals (L(n), rv);
            prop_infer_reqvals (R(n), rv);
            break;

        case la_cross_mvd:
            PFoops (OOPS_FATAL,
                    "clone column aware cross product operator is "
                    "only allowed inside mvd optimization!");

        case la_eqjoin_unq:
            PFoops (OOPS_FATAL,
                    "clone column aware equi-join operator is "
                    "only allowed with unique attribute names!");
    }
}

/* worker for PFprop_infer_reqval */
static void
prop_infer (PFla_op_t *n)
{
    assert (n);

    /* count number of incoming edges
       (during first run) */
    n->state_label++;

    /* nothing to do if we already visited that node */
    if (SEEN(n))
        return;
    /* otherwise initialize edge counter (first occurrence) */
    else
        n->state_label = 1;

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        prop_infer (n->child[i]);

    SEEN(n) = true;

    /* reset reqvals property */
    n->prop->reqvals.name = 0;
    n->prop->reqvals.val = 0;
}

/**
 * Infer required values property for a DAG rooted in @a root
 */
void
PFprop_infer_reqval (PFla_op_t *root) {
    /* initial empty list of required values */
    reqval_t init = { .name = 0, .val = 0 };

    /* collect number of incoming edges (parents) */
    prop_infer (root);
    PFla_dag_reset (root);

    /* second run infers reqvals property */
    prop_infer_reqvals (root, init);
    PFla_dag_reset (root);
}


