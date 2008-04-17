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
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"

#include <assert.h>
#include <stdio.h>

#include "properties.h"

#include "alg_dag.h"
#include "oops.h"
#include "mem.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define SEEN(p) ((p)->bit_dag)

/* store the number of incoming edges for each operator
   in the refctr field */
#define EDGE(n) ((n)->refctr)

#define empty_list 0
#define in(a,b) ((a) & (b) ? true : false)

/**
 * Test if @a attr is in the list of required value columns
 * in container @a prop
 */
bool
PFprop_req_bool_val (const PFprop_t *prop, PFalg_att_t attr)
{
    return in (prop->req_bool_vals.name, attr);
}

/**
 * Looking up required value of column @a attr
 * in container @a prop
 */
bool
PFprop_req_bool_val_val (const PFprop_t *prop, PFalg_att_t attr)
{
    return in (prop->req_bool_vals.val, attr);
}

/**
 * Test if @a attr is in the list of order columns
 * in container @a prop
 */
bool
PFprop_req_order_col (const PFprop_t *prop, PFalg_att_t attr)
{
    return in (prop->req_order_cols, attr) &&
          !in (prop->req_value_cols, attr);
}

/**
 * Test if @a attr is in the list of bijective columns
 * in container @a prop
 */
bool
PFprop_req_bijective_col (const PFprop_t *prop, PFalg_att_t attr)
{
    return in (prop->req_bijective_cols, attr) &&
          !in (prop->req_value_cols, attr);
}

/**
 * Test if @a attr may be represented by multiple columns
 */
bool
PFprop_req_multi_col_col (const PFprop_t *prop, PFalg_att_t attr)
{
    return in (prop->req_multi_col_cols, attr) &&
          !in (prop->req_value_cols, attr);
}

/**
 * Test if @a attr is in the list of filter columns
 * in container @a prop
 */
bool
PFprop_req_filter_col (const PFprop_t *prop, PFalg_att_t attr)
{
    return in (prop->req_filter_cols, attr);
}

/**
 * Test if @a attr is in the list of value columns
 * in container @a prop
 */
bool
PFprop_req_value_col (const PFprop_t *prop, PFalg_att_t attr)
{
    return in (prop->req_value_cols, attr);
}

/**
 * Transform a schema into a bit-encoded column list.
 */
static PFalg_att_t
schema2collist (PFla_op_t *n)
{
    PFalg_att_t res = empty_list;

    for (unsigned int i = 0; i < n->schema.count; i++) {
        res |= n->schema.items[i].name;
    }
    return res;
}

/**
 * Returns union of two lists
 */
static PFalg_att_t
intersect (PFalg_att_t a, PFalg_att_t b)
{
    return a & b;
}

/**
 * Returns union of two lists
 */
static PFalg_att_t
union_ (PFalg_att_t a, PFalg_att_t b)
{
    return a | b;
}

/**
 * Returns difference of two lists
 */
static PFalg_att_t
diff (PFalg_att_t a, PFalg_att_t b)
{
    return a & (~b);
}

/* short version to descend to fragment information */
#define prop_infer_reqvals_empty(n)                                \
        prop_infer_reqvals((n),                                    \
                           (req_bool_val_t) { .name = empty_list,  \
                                              .val = empty_list }, \
                           empty_list, empty_list, empty_list,     \
                           empty_list, empty_list, empty_list)

/**
 * worker for PFprop_infer_req_bool_val
 * infers the required values property during the second run
 * (uses edge counter from the first run)
 */
static void
prop_infer_reqvals (PFla_op_t *n,
                    req_bool_val_t req_bool_vals,
                    PFalg_att_t    cols,
                    PFalg_att_t    order_cols,
                    PFalg_att_t    bijective_cols,
                    PFalg_att_t    multi_col_cols,
                    PFalg_att_t    filter_cols,
                    PFalg_att_t    value_cols)
{
    req_bool_val_t rv; /* list of required boolean values */
    PFalg_att_t    oc, /* list of order columns */
                   bc, /* list of bijective columns */
                   mc, /* list of multi column columns */
                   fc, /* list of filter columns */
                   vc; /* list of value columns */
    assert (n);

    /* make sure that relations that require all boolean
       values overrule any decision */
    if (SEEN(n) && !req_bool_vals.name)
        n->prop->req_bool_vals = (req_bool_val_t) { .name = empty_list,
                                                    .val = empty_list };

    /* in all calls (except the first) we ensure that
       we already have required boolean value columns */
    if (SEEN(n) && n->prop->req_bool_vals.name) {
        /* Check if both parents look for required values
           in the same columns and then resolve the possible
           conflicts */
        if (n->prop->req_bool_vals.name == req_bool_vals.name) {
            PFalg_att_t  overlap   = req_bool_vals.name;
            unsigned int bit_shift = 1;

            while (bit_shift <= overlap) {
                /* if the values of column that is required by both
                   parents do not match remove this column from the
                   list of required value columns */
                if (bit_shift & overlap &&
                    ((bit_shift & req_bool_vals.val) !=
                     (bit_shift & n->prop->req_bool_vals.val))) {
                    /* remove entry from the list */
                    n->prop->req_bool_vals.name
                        = diff (n->prop->req_bool_vals.name, bit_shift);
                    n->prop->req_bool_vals.val
                        = diff (n->prop->req_bool_vals.val, bit_shift);
                }
                bit_shift <<= 1;
            }
        } else
            n->prop->req_bool_vals = (req_bool_val_t) { .name = empty_list,
                                                        .val = empty_list };
    }

    /* in the first call we use the required values of the caller */
    if (!SEEN(n)) {
        n->prop->req_bool_vals = req_bool_vals;

        oc = intersect (cols, order_cols);
        bc = intersect (cols, bijective_cols);
        mc = intersect (cols, multi_col_cols);
        fc = intersect (cols, filter_cols);
        vc = intersect (cols, value_cols);
        SEEN(n) = true;
    } else {
        oc = n->prop->req_order_cols;
        bc = n->prop->req_bijective_cols;
        mc = n->prop->req_multi_col_cols;
        fc = n->prop->req_filter_cols;
        vc = n->prop->req_value_cols;

        /* collect all columns whose order is necessary */
        oc = union_ (oc, intersect (cols, order_cols));
        /* collect all columns whose bijective value mapping is necessary */
        bc = union_ (bc, intersect (cols, bijective_cols));
        /* Keep only non-conflicting columns by matching the columns
           from the new input and the exisiting ones thus keeping the
           columns that are not referenced (not in cols).
           (comparison is done on common columns only) */
        mc = union_ (intersect (intersect (mc, cols), multi_col_cols),
                     diff (mc, cols));
        /* Keep only non-conflicting columns by matching the columns
           from the new input and the exisiting ones thus keeping the
           columns that are not referenced (not in cols).
           (comparison is done on common columns only) */
        fc = union_ (intersect (intersect (fc, cols), filter_cols),
                     diff (fc, cols));
        /* collect all columns whose real value is necessary */
        vc = union_ (vc, intersect (cols, value_cols));
    }
    /* store the combined lists */
    n->prop->req_order_cols     = oc;
    n->prop->req_bijective_cols = bc;
    n->prop->req_multi_col_cols = mc;
    n->prop->req_filter_cols    = fc;
    n->prop->req_value_cols     = vc;

    /* nothing to do if we haven't collected
       all incoming required values lists of that node */
    if (EDGE(n) > 1) {
        EDGE(n)--;
        return;
    }

    /* copy current required value list */
    rv  = n->prop->req_bool_vals;

    /* get the current schema */
    cols = schema2collist (n);

    /**
     * Infer REQUIRED boolean values property for n's children:
     *
     * - 'select' introduces new required boolean value columns.
     * - 'not' extends required boolean values list with a new column
     *   if the result also is one.
     * - All other operators either ignore the required value columns
     *   or infer them (partially) to their children.
     *
     * Infer REQUIRED order value columns for n's children:
     *
     * - Marks a column if it is used only in orderings.
     *
     * Infer REQUIRED bijective value columns for n's children:
     *
     * - Marks a column if it is used only in joins, orderings, and
     *   partitionings (where only the values need to fullfil a
     *   bijective mapping).
     *
     * Infer POSSIBLE multi column columns for n's children:
     *
     * - Marks a column if it is used only in joins and orderings
     *   where a column can be splitted into two.
     *
     * Infer REQUIRED real value columns for n's children:
     *
     * - Marks all columns that are input to operators (except for
     *   the operators that infer order, bijective and/or multi column
     *   columns).
     */
    switch (n->kind) {
        case la_serialize_seq:
            oc = union_ (empty_list, n->sem.ser_seq.pos);
            bc = empty_list;
            mc = diff (diff (cols, n->sem.ser_seq.pos), n->sem.ser_seq.item);
            fc = empty_list;
            vc = union_ (vc, n->sem.ser_seq.item);

            prop_infer_reqvals_empty (L(n)); /* fragments */
            prop_infer_reqvals (R(n), rv, cols, oc, bc, mc, fc, vc);
            return; /* only infer once */

        case la_serialize_rel:
            oc = union_ (empty_list, n->sem.ser_rel.pos);
            bc = empty_list;
            mc = diff (diff (cols, n->sem.ser_rel.iter), n->sem.ser_rel.pos);
            fc = empty_list;
            vc = union_ (vc, n->sem.ser_rel.iter);

            for (unsigned int i = 0; i < n->sem.ser_rel.items.count; i++) {
                mc = diff (mc, n->sem.ser_rel.items.atts[i]);
                vc = union_ (vc, n->sem.ser_rel.items.atts[i]);
            }
            break;

        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
        case la_empty_frag:
        case la_nil:
            /* leafs */
            break;

        case la_attach:
            rv.name = diff (rv.name, n->sem.attach.res);
            rv.val = diff (rv.val, n->sem.attach.res);

            /* make the new column invisible for the children */
            cols = diff (cols, n->sem.attach.res);
            break;

        case la_cross:
            prop_infer_reqvals (L(n), rv, schema2collist(L(n)),
                                oc, bc, mc, fc, vc);
            prop_infer_reqvals (R(n), rv, schema2collist(R(n)),
                                oc, bc, mc, fc, vc);
            return; /* only infer once */

        case la_eqjoin:
            /* Check whether the join columns can be marked as bijective value
               columns and multi column columns or if they have to produce
               the exact values. */
            if ((PFprop_subdom (n->prop,
                                PFprop_dom_right (n->prop,
                                                  n->sem.eqjoin.att2),
                                PFprop_dom_left (n->prop,
                                                 n->sem.eqjoin.att1)) ||
                 PFprop_subdom (n->prop,
                                PFprop_dom_left (n->prop,
                                                 n->sem.eqjoin.att1),
                                PFprop_dom_right (n->prop,
                                                  n->sem.eqjoin.att2))) &&
                !in (vc, n->sem.eqjoin.att1) &&
                !in (vc, n->sem.eqjoin.att2)) {
                /* We know that the columns are from the same origin (subdomain
                   relationship) and there values are not needed.
                   We thus mark that we only need the bijectivity of the values
                   to correctly implement the operator. */
                bc = union_ (union_ (bc, n->sem.eqjoin.att1),
                             n->sem.eqjoin.att2);
                /* We know that the columns are from the same origin (subdomain
                   relationship) and there values are not needed.
                   We thus mark that we could replace the comparison
                   by a one with two condititions (using a theta-join). */
                mc = union_ (union_ (mc, n->sem.eqjoin.att1),
                              n->sem.eqjoin.att2);
            } else {
                vc = union_ (union_ (vc, n->sem.eqjoin.att1),
                             n->sem.eqjoin.att2);
            }

            prop_infer_reqvals (L(n), rv, schema2collist(L(n)),
                                oc, bc, mc, fc, vc);
            prop_infer_reqvals (R(n), rv, schema2collist(R(n)),
                                oc, bc, mc, fc, vc);
            return; /* only infer once */

        case la_semijoin:
        {
            PFalg_att_t oc_right = empty_list,
                        bc_right = empty_list,
                        mc_right = empty_list,
                        fc_right = empty_list,
                        vc_right = empty_list;

            /* Check whether the join columns can be marked as bijective value
               columns or if they have to produce the exact values. */
            if ((PFprop_subdom (n->prop,
                                PFprop_dom_right (n->prop,
                                                  n->sem.eqjoin.att2),
                                PFprop_dom_left (n->prop,
                                                 n->sem.eqjoin.att1)) ||
                 PFprop_subdom (n->prop,
                                PFprop_dom_left (n->prop,
                                                 n->sem.eqjoin.att1),
                                PFprop_dom_right (n->prop,
                                                  n->sem.eqjoin.att2))) &&
                !in (vc, n->sem.eqjoin.att1)) {
                /* We know that the columns are from the same origin (subdomain
                   relationship) and there values are not needed.
                   We thus mark that we only need the bijectivity of the values
                   to correctly implement the operator. */
                bc = union_ (bc, n->sem.eqjoin.att1);
                bc_right = union_ (empty_list, n->sem.eqjoin.att2);
            } else {
                vc = union_ (vc, n->sem.eqjoin.att1);
                vc_right = union_ (empty_list, n->sem.eqjoin.att2);
            }

            /* remove columns as we cannot split up the join column */
            mc = diff (mc, n->sem.eqjoin.att1);

            prop_infer_reqvals (L(n), rv, cols, oc, bc, mc, fc, vc);

            cols = union_ (empty_list, n->sem.eqjoin.att2);
            rv.name = empty_list;
            rv.val = empty_list;

            prop_infer_reqvals (R(n), rv, cols, oc_right,
                                bc_right, mc_right, fc_right, vc_right);
        }   return; /* only infer once */

        case la_thetajoin:
            /* add all join columns to the inferred icols */
            for (unsigned int i = 0; i < n->sem.thetajoin.count; i++) {
                PFalg_att_t left  = n->sem.thetajoin.pred[i].left,
                            right = n->sem.thetajoin.pred[i].right;

                if (n->sem.thetajoin.pred[i].comp == alg_comp_eq &&
                    (PFprop_subdom (n->prop,
                                    PFprop_dom_right (n->prop,
                                                      right),
                                    PFprop_dom_left (n->prop,
                                                     left)) ||
                     PFprop_subdom (n->prop,
                                    PFprop_dom_left (n->prop,
                                                     left),
                                    PFprop_dom_right (n->prop,
                                                      right))) &&
                    !in (vc, left) &&
                    !in (vc, right)) {
                    /* We know that the columns are from the same origin
                       (subdomain relationship) and there values are not needed.
                       We thus mark that we only need the bijectivity
                       of the values to correctly implement the operator. */
                    bc = union_ (union_ (bc, left), right);
                    /* We know that the columns are from the same origin
                       (subdomain relationship) and there values are not needed.
                       We thus mark that we could replace the comparison
                       by a one with two condititions (using a theta-join). */
                    mc = union_ (union_ (mc, left), right);
                } else {
                    if (n->sem.thetajoin.pred[i].comp == alg_comp_eq)
                        fc = union_ (union_ (fc, left), right);
                    vc  = union_ (union_ (vc, left), right);
                }
            }

            prop_infer_reqvals (L(n), rv, schema2collist(L(n)),
                                oc, bc, mc, fc, vc);
            prop_infer_reqvals (R(n), rv, schema2collist(R(n)),
                                oc, bc, mc, fc, vc);
            return; /* only infer once */

        case la_project:
            /* discard the copies */
            rv.name = empty_list;
            rv.val = empty_list;
            oc = empty_list;
            bc = empty_list;
            mc = empty_list;
            fc = empty_list;
            vc = empty_list;
            cols = empty_list;
            /* and fill them again based on the original stored values */

            /* rename reqvals columns from new to old */
            for (unsigned int i = 0; i < n->sem.proj.count; i++) {
                if (in (n->prop->req_bool_vals.name,
                        n->sem.proj.items[i].new)) {
                    rv.name = union_ (rv.name, n->sem.proj.items[i].old);
                    /* keep values but map them to the old column name */
                    if (in (n->prop->req_bool_vals.val,
                            n->sem.proj.items[i].new))
                        rv.val = union_ (rv.val, n->sem.proj.items[i].old);
                }
                if (in (n->prop->req_order_cols, n->sem.proj.items[i].new))
                    oc = union_ (oc, n->sem.proj.items[i].old);
                if (in (n->prop->req_bijective_cols, n->sem.proj.items[i].new))
                    bc = union_ (bc, n->sem.proj.items[i].old);
                if (in (n->prop->req_multi_col_cols, n->sem.proj.items[i].new))
                    mc = union_ (mc, n->sem.proj.items[i].old);
                if (in (n->prop->req_filter_cols, n->sem.proj.items[i].new))
                    fc = union_ (fc, n->sem.proj.items[i].old);
                if (in (n->prop->req_value_cols, n->sem.proj.items[i].new))
                    vc = union_ (vc, n->sem.proj.items[i].old);
                cols = union_ (cols, n->sem.proj.items[i].old);
            }
            break;

        case la_select:
            /* introduce new required value column */
            n->prop->req_bool_vals.name = union_ (n->prop->req_bool_vals.name,
                                                  n->sem.select.att);
            n->prop->req_bool_vals.val = union_ (n->prop->req_bool_vals.val,
                                                 n->sem.select.att);
            rv.name = union_ (rv.name, n->sem.select.att);
            rv.val = union_ (rv.val, n->sem.select.att);

            /* we have a real value column here */
            oc = diff (oc, n->sem.select.att);
            bc = diff (bc, n->sem.select.att);
            mc = diff (mc, n->sem.select.att);
            vc = union_ (vc, n->sem.select.att);
            break;

        case la_pos_select:
        {
            PFord_ordering_t sortby = n->sem.pos_sel.sortby;
            for (unsigned int i = 0; i < PFord_count (sortby); i++) {
                 /* for the order criteria we only have to ensure
                    the correct order */
                 oc = union_ (oc, PFord_order_col_at (sortby, i));
                 mc = union_ (mc, PFord_order_col_at (sortby, i));
            }

            if (n->sem.pos_sel.part) {
                /* we only have to provide the same groups */
                bc = union_ (bc, n->sem.pos_sel.part);
                /* we cannot split up a partition column */
                mc = diff (mc, n->sem.pos_sel.part);
            }
        }   break;

        case la_disjunion:
        case la_intersect:
            /* We have to apply a natural join here which means
               the values should be comparable. (Including domain
               information might improve this inference.) */
            oc = empty_list;
            bc = empty_list;
            mc = empty_list;

            /* Keep order columns, bijective value columns and multi
               column columns if they stem from the same operator.
               For all other columns we require the real values. */
            for (unsigned int i = 0; i < n->schema.count; i++) {
                PFalg_att_t cur = n->schema.items[i].name;
                if ((PFprop_subdom (n->prop,
                                    PFprop_dom_right (n->prop, cur),
                                    PFprop_dom_left (n->prop, cur)) ||
                     PFprop_subdom (n->prop,
                                    PFprop_dom_left (n->prop, cur),
                                    PFprop_dom_right (n->prop, cur))) &&
                    !in (vc, cur)) {
                    if (in (n->prop->req_order_cols, cur))
                        oc = union_ (oc, cur);
                    if (in (n->prop->req_bijective_cols, cur))
                        bc = union_ (bc, cur);
                    if (in (n->prop->req_multi_col_cols, cur))
                        mc = union_ (mc, cur);

                    if (!in (oc, cur) && !in (bc, cur) && !in (mc, cur))
                        vc  = union_ (vc,  cur);
                } else
                    vc  = union_ (vc,  cur);
            }
            break;

        case la_difference:
            /* We have to apply a natural join here which means
               the values should be comparable. (Including domain
               information might improve this inference.) */
            oc = empty_list;
            bc = empty_list;
            mc = empty_list;

            for (unsigned int i = 0; i < n->schema.count; i++) {
                PFalg_att_t cur = n->schema.items[i].name;
                if ((PFprop_subdom (n->prop,
                                    PFprop_dom_right (n->prop, cur),
                                    PFprop_dom_left (n->prop, cur)) ||
                     PFprop_subdom (n->prop,
                                    PFprop_dom_left (n->prop, cur),
                                    PFprop_dom_right (n->prop, cur))) &&
                    !in (vc, cur)) {
                    if (in (n->prop->req_order_cols, cur))
                        oc = union_ (oc, cur);
                    if (in (n->prop->req_bijective_cols, cur))
                        bc = union_ (bc, cur);
                    if (in (n->prop->req_multi_col_cols, cur))
                        mc = union_ (mc, cur);

                    if (!in (oc, cur) && !in (bc, cur) && !in (mc, cur))
                        vc  = union_ (vc,  cur);
                } else
                    vc  = union_ (vc,  cur);
            }

            prop_infer_reqvals (L(n), rv, cols, oc, bc, mc, fc, vc);

            rv.name = empty_list;
            rv.val = empty_list;
            prop_infer_reqvals (R(n), rv, cols, oc, bc, mc, fc, vc);
            return; /* only infer once */

        case la_distinct:
            /* for all columns where we do not need the real values
               we need at least the bijectivity of the values */
            bc = diff (cols, vc);
            mc = diff (cols, vc);
            break;

        case la_fun_1to1:
            rv.name = diff (rv.name, n->sem.fun_1to1.res);
            rv.val = diff (rv.val, n->sem.fun_1to1.res);

            /* mark the input columns as value columns */
            for (unsigned int i = 0; i < n->sem.fun_1to1.refs.count; i++)
                vc = union_ (vc, n->sem.fun_1to1.refs.atts[i]);

            /* make the new column invisible for the children */
            cols = diff (cols, n->sem.fun_1to1.res);
            break;

        case la_num_eq:
            if (in (rv.name, n->sem.binary.res))
                fc = union_ (union_ (fc, n->sem.binary.att1),
                             n->sem.binary.att2);
        case la_num_gt:
        case la_to:
            rv.name = diff (rv.name, n->sem.binary.res);
            rv.val = diff (rv.val, n->sem.binary.res);

            /* mark the input columns as value columns */
            vc = union_ (union_ (vc, n->sem.binary.att1), n->sem.binary.att2);

            /* make the new column invisible for the children */
            cols = diff (cols, n->sem.binary.res);
            break;

        case la_bool_and:
            if (PFprop_req_bool_val (n->prop, n->sem.binary.res) &&
                PFprop_req_bool_val_val (n->prop, n->sem.binary.res)) {
                rv.name = union_ (rv.name, n->sem.binary.att1);
                rv.val = union_ (rv.val, n->sem.binary.att1);
                rv.name = union_ (rv.name, n->sem.binary.att2);
                rv.val = union_ (rv.val, n->sem.binary.att2);
            }

            /* mark the input columns as value columns */
            vc = union_ (union_ (vc, n->sem.binary.att1), n->sem.binary.att2);

            /* make the new column invisible for the children */
            cols = diff (cols, n->sem.binary.res);
            break;

        case la_bool_or:
            if (PFprop_req_bool_val (n->prop, n->sem.binary.res) &&
                !PFprop_req_bool_val_val (n->prop, n->sem.binary.res)) {
                rv.name = union_ (rv.name, n->sem.binary.att1);
                rv.val = diff (rv.val, n->sem.binary.att1);
                rv.name = union_ (rv.name, n->sem.binary.att2);
                rv.val = diff (rv.val, n->sem.binary.att2);
            }

            /* mark the input columns as value columns */
            vc = union_ (union_ (vc, n->sem.binary.att1), n->sem.binary.att2);

            /* make the new column invisible for the children */
            cols = diff (cols, n->sem.binary.res);
            break;

        case la_bool_not:
            /* if res is a required value column also add att
               with the switched boolean value */
            if (PFprop_req_bool_val (n->prop, n->sem.unary.res)) {
                rv.name = union_ (rv.name, n->sem.unary.att);
                /* add positive value if res is wrong otherwise
                   value stays false (default) */
                if (!PFprop_req_bool_val_val (n->prop, n->sem.unary.res))
                    rv.val = union_ (rv.val, n->sem.unary.att);
            }
            rv.name = diff (rv.name, n->sem.unary.res);
            rv.val = diff (rv.val, n->sem.unary.res);

            /* mark the input columns as value columns */
            vc = union_ (vc, n->sem.unary.att);

            /* make the new column invisible for the children */
            cols = diff (cols, n->sem.unary.res);
            break;

        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_count:
        case la_seqty1:
        case la_all:
            rv.name = empty_list;
            rv.val = empty_list;

            /* mark the input columns as value columns */
            vc = union_ (diff (vc, n->sem.aggr.res), n->sem.aggr.att);

            if (n->sem.aggr.part) {
                /* we only have to provide the same groups */
                bc = union_ (bc, n->sem.aggr.part);
                /* we cannot split up a partition column */
                mc = diff (mc, n->sem.aggr.part);
            }

            /* make the new column invisible for the children */
            cols = diff (cols, n->sem.aggr.res);

            if (n->sem.aggr.att)
                /* to make up for the schema change
                   we add the input columns by hand */
                cols = union_ (cols, n->sem.aggr.att);
            break;

        case la_rownum:  /* for rownum, rowrank, rank, and rowid */
        case la_rowrank: /* type of res is != boolean and */
        case la_rank:    /* therefore never needs to be removed */
        {
            PFord_ordering_t sortby = n->sem.sort.sortby;
            for (unsigned int i = 0; i < PFord_count (sortby); i++) {
                 /* for the order criteria we only have to ensure
                    the correct order */
                 oc = union_ (oc , PFord_order_col_at (sortby, i));
                 mc = union_ (mc, PFord_order_col_at (sortby, i));
            }

            if (n->sem.sort.part) {
                /* we only have to provide the same groups */
                bc = union_ (bc, n->sem.sort.part);
                /* we cannot split up a partition column */
                mc = diff (mc, n->sem.sort.part);
            }
            /* make the new column invisible for the children */
            cols = diff (cols, n->sem.sort.res);
        }   break;

        case la_rowid:
            /* make the new column invisible for the children */
            cols = diff (cols, n->sem.rowid.res);
            break;

        case la_cast:
            if (n->sem.type.ty == aat_bln) {
                bool att_bln = false;
                for (unsigned int i = 0; i < n->schema.count; i++)
                    if (n->sem.type.att == n->schema.items[i].name) {
                        att_bln = (n->schema.items[i].type == aat_bln);
                        break;
                    }

                if (att_bln && PFprop_req_bool_val (n->prop, n->sem.type.res)) {
                    rv.name = union_ (rv.name, n->sem.type.att);
                    if (PFprop_req_bool_val_val (n->prop, n->sem.type.res))
                        rv.val = union_ (rv.val, n->sem.type.att);
                }
            }
            rv.name = diff (rv.name, n->sem.type.res);
            rv.val = diff (rv.val, n->sem.type.res);

            if (!in (vc, n->sem.type.res) &&
                !in (bc, n->sem.type.res) &&
                in (oc, n->sem.type.res))
                /* mark the input column as order column if
                   not used differently */
                oc = union_ (oc, n->sem.type.att);
            else
                /* mark the input column as value columns */
                vc = union_ (vc, n->sem.type.att);

            /* make the new column invisible for the children */
            cols = diff (cols, n->sem.type.res);
            break;

        case la_type:
            rv.name = diff (rv.name, n->sem.type.res);
            rv.val = diff (rv.val, n->sem.type.res);

            /* mark the input columns as value columns */
            vc = union_ (vc, n->sem.type.att);

            /* make the new column invisible for the children */
            cols = diff (cols, n->sem.type.res);
            break;

        case la_type_assert:
            /* propagate required values list to left subtree */

            /* mark the input columns as value columns */
            vc = union_ (vc, n->sem.type.att);
            break;

        case la_step:
        case la_guide_step:
            rv.name = empty_list;
            rv.val = empty_list;

            /* to make up for the schema change
               we add the input columns by hand */
            cols = union_ (union_ (empty_list, n->sem.step.iter),
                           n->sem.step.item);
            oc   = empty_list;
            bc   = empty_list;
            mc   = empty_list;
            fc   = empty_list;
            vc   = cols;

            prop_infer_reqvals_empty (L(n)); /* fragments */
            prop_infer_reqvals (R(n), rv, cols, oc, bc, mc, fc, vc);
            return; /* only infer once */

        case la_step_join:
        case la_guide_step_join:
            rv.name = diff (rv.name, n->sem.step.item_res);
            rv.val = diff (rv.val, n->sem.step.item_res);

            /* mark the input columns as value columns */
            vc = union_ (vc, n->sem.step.item);

            /* make the new column invisible for the children */
            cols = diff (cols, n->sem.step.item_res);

            prop_infer_reqvals_empty (L(n)); /* fragments */
            prop_infer_reqvals (R(n), rv, cols, oc, bc, mc, fc, vc);
            return; /* only infer once */

        case la_doc_index_join:
            rv.name = diff (rv.name, n->sem.doc_join.item_res);
            rv.val = diff (rv.val, n->sem.doc_join.item_res);

            /* mark the input columns as value columns */
            vc = union_ (union_ (vc, n->sem.doc_join.item),
                         n->sem.doc_join.item_doc);

            /* make the new column invisible for the children */
            cols = diff (cols, n->sem.doc_join.item_res);

            prop_infer_reqvals_empty (L(n)); /* fragments */
            prop_infer_reqvals (R(n), rv, cols, oc, bc, mc, fc, vc);
            return; /* only infer once */

        case la_doc_tbl:
            rv.name = diff (rv.name, n->sem.doc_tbl.res);
            rv.val = diff (rv.val, n->sem.doc_tbl.res);

            /* mark the input columns as value columns */
            vc = union_ (vc, n->sem.doc_tbl.att);

            /* make the new column invisible for the children */
            cols = diff (cols, n->sem.doc_tbl.res);
            break;

        case la_doc_access:
            rv.name = diff (rv.name, n->sem.doc_access.res);
            rv.val = diff (rv.val, n->sem.doc_access.res);

            /* mark the input columns as value columns */
            vc = union_ (vc, n->sem.doc_access.att);

            /* make the new column invisible for the children */
            cols = diff (cols, n->sem.doc_access.res);

            prop_infer_reqvals_empty (L(n)); /* fragments */
            prop_infer_reqvals (R(n), rv, cols, oc, bc, mc, fc, vc);
            return; /* only infer once */

        case la_twig:
        case la_fcns:
            rv.name = empty_list;
            rv.val = empty_list;

            cols = empty_list;
            break;

        case la_docnode:
            rv.name = empty_list;
            rv.val = empty_list;

            /* to make up for the schema change
               we add the input columns by hand */
            cols = union_ (empty_list, n->sem.docnode.iter);
            oc   = empty_list;
            bc   = empty_list;
            mc   = empty_list;
            fc   = empty_list;
            vc   = cols;

            prop_infer_reqvals (L(n), rv, cols, oc, bc, mc, fc, vc);
            prop_infer_reqvals_empty (R(n)); /* constructor */
            return; /* only infer once */

        case la_element:
            rv.name = empty_list;
            rv.val = empty_list;

            /* to make up for the schema change
               we add the input columns by hand */
            cols = union_ (union_ (empty_list, n->sem.iter_item.iter),
                           n->sem.iter_item.item);
            oc   = empty_list;
            bc   = empty_list;
            mc   = empty_list;
            fc   = empty_list;
            vc   = cols;

            prop_infer_reqvals (L(n), rv, cols, oc, bc, mc, fc, vc);
            prop_infer_reqvals_empty (R(n)); /* constructor */
            return; /* only infer once */

        case la_textnode:
        case la_comment:
            rv.name = empty_list;
            rv.val = empty_list;

            /* to make up for the schema change
               we add the input columns by hand */
            cols = union_ (union_ (empty_list, n->sem.iter_item.iter),
                           n->sem.iter_item.item);
            oc   = empty_list;
            bc   = empty_list;
            mc   = empty_list;
            fc   = empty_list;
            vc   = cols;
            break;

        case la_attribute:
        case la_processi:
            rv.name = empty_list;
            rv.val = empty_list;

            /* to make up for the schema change
               we add the input columns by hand */
            cols = union_ (union_ (union_ (empty_list,
                                           n->sem.iter_item1_item2.iter),
                                   n->sem.iter_item1_item2.item1),
                           n->sem.iter_item1_item2.item2);
            oc   = empty_list;
            bc   = empty_list;
            mc   = empty_list;
            fc   = empty_list;
            vc   = cols;
            break;

        case la_content:
            rv.name = empty_list;
            rv.val = empty_list;

            /* to make up for the schema change
               we add the input columns by hand */
            cols = union_ (union_ (union_ (empty_list,
                                           n->sem.iter_pos_item.iter),
                                   n->sem.iter_pos_item.pos),
                           n->sem.iter_pos_item.item);
            oc   = empty_list;
            bc   = empty_list;
            mc   = empty_list;
            fc   = empty_list;
            vc   = cols;

            prop_infer_reqvals_empty (L(n)); /* fragments */
            prop_infer_reqvals (R(n), rv, cols, oc, bc, mc, fc, vc);
            return; /* only infer once */

        case la_merge_adjacent:
            rv.name = empty_list;
            rv.val = empty_list;

            /* to make up for the schema change
               we add the input columns by hand */
            cols = union_ (union_ (union_ (empty_list,
                                           n->sem.merge_adjacent.iter_in),
                                   n->sem.merge_adjacent.pos_in),
                           n->sem.merge_adjacent.item_in);
            oc   = empty_list;
            bc   = empty_list;
            mc   = empty_list;
            fc   = empty_list;
            vc   = cols;

            prop_infer_reqvals_empty (L(n)); /* fragments */
            prop_infer_reqvals (R(n), rv, cols, oc, bc, mc, fc, vc);
            return; /* only infer once */

        case la_roots:
        case la_proxy:
        case la_proxy_base:
        case la_dummy:
        case la_error:
            /* propagate required values list to left subtree */
            break;

        case la_fragment:
        case la_frag_extract:
            prop_infer_reqvals_empty (L(n)); /* fragments */
            return; /* only infer once */

        case la_frag_union:
            prop_infer_reqvals_empty (L(n)); /* fragments */
            prop_infer_reqvals_empty (R(n)); /* fragments */
            return; /* only infer once */

        case la_cond_err:
            prop_infer_reqvals (L(n), rv, cols, oc, bc, mc, fc, vc);

            rv.name = empty_list;
            rv.val = empty_list;

            /* to make up for the schema change
               we add the input columns by hand */
            cols = union_ (empty_list, n->sem.err.att);
            oc   = empty_list;
            bc   = empty_list;
            mc   = empty_list;
            fc   = empty_list;
            vc   = cols;

            prop_infer_reqvals (R(n), rv, cols, oc, bc, mc, fc, vc);
            return; /* only infer once */

        case la_trace:
            rv.name = empty_list;
            rv.val = empty_list;

            /* to make up for the schema change
               we add the input columns by hand */
            cols = union_ (union_ (union_ (empty_list,
                                           n->sem.iter_pos_item.iter),
                                   n->sem.iter_pos_item.pos),
                           n->sem.iter_pos_item.item);
            oc   = empty_list;
            bc   = empty_list;
            mc   = empty_list;
            fc   = empty_list;
            vc   = cols;

            prop_infer_reqvals (L(n), rv, cols, oc, bc, mc, fc, vc);
            prop_infer_reqvals_empty (R(n)); /* trace */
            return; /* only infer once */

        case la_trace_msg:
            rv.name = empty_list;
            rv.val = empty_list;

            /* to make up for the schema change
               we add the input columns by hand */
            cols = union_ (union_ (empty_list, n->sem.iter_item.iter),
                           n->sem.iter_item.item);
            oc   = empty_list;
            bc   = empty_list;
            mc   = empty_list;
            fc   = empty_list;
            vc   = cols;

            prop_infer_reqvals (L(n), rv, cols, oc, bc, mc, fc, vc);
            prop_infer_reqvals_empty (R(n)); /* trace */
            return; /* only infer once */

        case la_trace_map:
            rv.name = empty_list;
            rv.val = empty_list;

            /* to make up for the schema change
               we add the input columns by hand */
            cols = union_ (union_ (empty_list, n->sem.trace_map.inner),
                           n->sem.trace_map.outer);
            oc   = empty_list;
            bc   = empty_list;
            mc   = empty_list;
            fc   = empty_list;
            vc   = cols;

            prop_infer_reqvals (L(n), rv, cols, oc, bc, mc, fc, vc);
            prop_infer_reqvals_empty (R(n)); /* trace */
            return; /* only infer once */

        case la_rec_fix:
            /* infer no required values */
            rv.name = empty_list;
            rv.val = empty_list;

            oc   = empty_list;
            bc   = empty_list;
            mc   = empty_list;
            fc   = empty_list;
            vc  = cols;

            prop_infer_reqvals_empty (L(n)); /* recursion param */
            prop_infer_reqvals (R(n), rv, cols, oc, bc, mc, fc, vc);
            return; /* only infer once */

        case la_rec_param:
            prop_infer_reqvals_empty (L(n)); /* recursion arg */
            prop_infer_reqvals_empty (R(n)); /* recursion param */
            return; /* only infer once */

        case la_rec_arg:
            /* infer no required values */
            rv.name = empty_list;
            rv.val = empty_list;

            oc   = empty_list;
            bc   = empty_list;
            mc   = empty_list;
            fc   = empty_list;
            vc  = cols;

            prop_infer_reqvals (L(n), rv, cols, oc, bc, mc, fc, vc);
            prop_infer_reqvals (R(n), rv, cols, oc, bc, mc, fc, vc);
            return; /* only infer once */

        case la_rec_base:
            break;

        case la_fun_call:
            /* infer no required values */
            rv.name = empty_list;
            rv.val = empty_list;

            /* to make up for the schema change
               we add the input columns by hand */
            oc   = empty_list;
            bc   = empty_list;
            mc   = empty_list;
            fc   = empty_list;
            cols = union_ (empty_list, n->sem.fun_call.iter);
            vc   = cols;

            prop_infer_reqvals (L(n), rv, cols, oc, bc, mc, fc, vc);
            prop_infer_reqvals_empty (R(n)); /* function param */
            return; /* only infer once */

        case la_fun_param:
            /* infer no required values */
            rv.name = empty_list;
            rv.val = empty_list;

            oc   = empty_list;
            bc   = empty_list;
            mc   = empty_list;
            fc   = empty_list;
            vc   = cols;

            prop_infer_reqvals (L(n), rv, cols, oc, bc, mc, fc, vc);
            prop_infer_reqvals_empty (R(n)); /* function param */
            return; /* only infer once */

        case la_fun_frag_param:
            prop_infer_reqvals_empty (L(n)); /* fragments */
            prop_infer_reqvals_empty (R(n)); /* function param */
            return; /* only infer once */

        case la_cross_mvd:
            PFoops (OOPS_FATAL,
                    "clone column aware cross product operator is "
                    "only allowed inside mvd optimization!");

        case la_eqjoin_unq:
            PFoops (OOPS_FATAL,
                    "clone column aware equi-join operator is "
                    "only allowed with unique attribute names!");

        case la_string_join:
            rv.name = empty_list;
            rv.val = empty_list;

            /* to make up for the schema change
               we add the input columns by hand */
            cols = union_ (union_ (union_ (empty_list,
                                           n->sem.string_join.iter),
                                   n->sem.string_join.pos),
                           n->sem.string_join.item);
            oc   = empty_list;
            bc   = empty_list;
            mc   = empty_list;
            fc   = empty_list;
            vc   = cols;

            prop_infer_reqvals (L(n), rv, cols, oc, bc, mc, fc, vc);

            /* to make up for the schema change
               we add the input columns by hand */
            cols = union_ (union_ (empty_list, n->sem.string_join.iter_sep),
                           n->sem.string_join.item_sep);
            vc   = cols;

            prop_infer_reqvals (R(n), rv, cols, oc, bc, mc, fc, vc);
            return; /* only infer once */
    }

    if (L(n)) prop_infer_reqvals (L(n), rv, cols, oc, bc, mc, fc, vc);
    if (R(n)) prop_infer_reqvals (R(n), rv, cols, oc, bc, mc, fc, vc);
}

/* worker for PFprop_infer_reqval */
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

    /* reset required values properties */
    n->prop->req_bool_vals.name = empty_list;
    n->prop->req_bool_vals.val  = empty_list;
    n->prop->req_order_cols     = empty_list;
    n->prop->req_bijective_cols = empty_list;
    n->prop->req_multi_col_cols = empty_list;
    n->prop->req_value_cols     = empty_list;
}

/**
 * Infer required values properties for a DAG rooted in @a root
 */
void
PFprop_infer_reqval (PFla_op_t *root) {
    /* initial empty list of required values */
    req_bool_val_t init = { .name = empty_list, .val = empty_list };

    /* We need the domain property to detect more bijective
       columns (in operators with two children). */
       PFprop_infer_nat_dom (root);

    /* collect number of incoming edges (parents) */
    prop_infer (root);
    PFla_dag_reset (root);

    /* second run infers required values properties */
    prop_infer_reqvals (root, init, schema2collist(root),
                        empty_list, empty_list, empty_list,
                        empty_list, empty_list);
    PFla_dag_reset (root);
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
