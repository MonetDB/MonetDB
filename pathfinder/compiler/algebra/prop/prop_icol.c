/**
 * @file
 *
 * Inference of icols property of logical algebra expressions.
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

#include "properties.h"

#include "alg_dag.h"
#include "oops.h"
#include "mem.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define SEEN(n)       (n)->bit_dag
#define EDGE(n)       (n)->refctr

/**
 * Test if @a attr is in the list of icol columns in container @a prop
 */
bool
PFprop_icol (const PFprop_t *prop, PFalg_att_t attr)
{
    return prop->icols & attr ? true : false;
}

/**
 * Test if @a attr is in the list of icol columns of the left child
 * (information is stored in property container @a prop)
 */
bool
PFprop_icol_left (const PFprop_t *prop, PFalg_att_t attr)
{
    return prop->l_icols & attr ? true : false;
}

/**
 * Test if @a attr is in the list of icol columns of the right child
 * (information is stored in property container @a prop)
 */
bool
PFprop_icol_right (const PFprop_t *prop, PFalg_att_t attr)
{
    return prop->r_icols & attr ? true : false;
}

/**
 * worker for PFprop_icols_count and PFprop_icols_to_attlist
 */
static unsigned int
icols_count (const PFprop_t *prop)
{
    PFalg_att_t icols = prop->icols;
    unsigned int counter = 0;

    while (icols) {
        counter += icols & 1;
        icols >>= 1;
    }
    return counter;
}

/*
 * count number of icols attributes
 */
unsigned int
PFprop_icols_count (const PFprop_t *prop)
{
    return icols_count (prop);
}

/**
 * Return icols attributes in an attlist.
 */
PFalg_attlist_t
PFprop_icols_to_attlist (const PFprop_t *prop)
{
    PFalg_attlist_t new_list;
    PFalg_att_t icols = prop->icols;
    unsigned int counter = 0, bit_shift = 1;

    new_list.count = icols_count (prop);
    new_list.atts = PFmalloc (new_list.count * sizeof (*(new_list.atts)));

    /* unfold icols into a list of attributes */
    while (icols && counter < new_list.count) {
        new_list.atts[counter] = prop->icols & bit_shift;
        bit_shift <<= 1;

        counter += icols & 1;
        icols >>= 1;
    }
    return new_list;
}

/**
 * Returns the intersection of an icols list @a icols and a schema
 * @a schema_ocols
 */
static PFalg_att_t
intersect_ocol (PFalg_att_t icols, PFalg_schema_t schema_ocols)
{
    PFalg_att_t ocols = 0;

    /* intersect attributes */
    for (unsigned int j = 0; j < schema_ocols.count; j++)
        ocols |= schema_ocols.items[j].name;

    return icols & ocols;
}

/**
 * Returns union of two icols lists
 */
static PFalg_att_t
union_ (PFalg_att_t a, PFalg_att_t b)
{
    return a | b;
}

/**
 * Returns difference of two icols lists
 */
static PFalg_att_t
diff (PFalg_att_t a, PFalg_att_t b)
{
    return a & (~b);
}

/* forward declaration */
static void prop_infer_icols (PFla_op_t *, PFalg_att_t);

/**
 * Alternative traversal of the icols that is started
 * only for the recursion operator rec_fix. This traversal
 * ensures that the required columns of all operators in the
 * recursion body are inferred before the icols property
 * of the seeds.
 */
static void
prop_infer_icols_rec_body (PFla_op_t *n)
{
    if (n->kind == la_rec_param) {
        prop_infer_icols_rec_body (L(n));
        prop_infer_icols_rec_body (R(n));
    }
    else if (n->kind == la_nil)
        return;
    else if (n->kind == la_rec_arg) {
        /* the required columns of the body are all the
           columns of the schema */
        for (unsigned int i = 0; i < n->schema.count; i++)
            n->prop->r_icols = union_ (n->prop->r_icols,
                                       n->schema.items[i].name);
        /* infer the icols property of the operators in the
           recursion body */
        prop_infer_icols (R(n), n->prop->r_icols);
    }
    else PFoops (OOPS_FATAL,
                 "only recursion operators expected!");
}

static bool twig_needed;
static bool first_twig_child;

/**
 * worker for PFprop_infer
 * infers the icols property during the second run
 * (uses edge counter stored in EDGE(n) from the first run)
 */
static void
prop_infer_icols (PFla_op_t *n, PFalg_att_t icols)
{
    /* for element construction we need a special translation
       and therefore skip the default inference of the children */
    bool skip_children = false;

    assert (n);

    /* collect the icols properties of all parents*/
    n->prop->icols = union_ (n->prop->icols, icols);

    /* nothing to do if we haven't collected
       all incoming icols lists of that node */
    if (EDGE(n) > 1) {
        EDGE(n)--;
        return;
    }

    /* remove all unnecessary columns from icols */
    n->prop->icols = intersect_ocol (n->prop->icols, n->schema);

    /* infer icols property for children */
    switch (n->kind) {
        case la_serialize_seq:
            /* infer empty list for fragments */
            n->prop->l_icols = 0;
            n->prop->r_icols = union_ (n->sem.ser_seq.pos,
                                       n->sem.ser_seq.item);
            break;

        case la_serialize_rel:
            n->prop->l_icols = union_ (n->sem.ser_rel.iter,
                                       n->sem.ser_rel.pos);
            for (unsigned int i = 0; i < n->sem.ser_rel.items.count; i++)
                n->prop->l_icols = union_ (n->prop->l_icols,
                                           n->sem.ser_rel.items.atts[i]);
            break;

        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
            break;

        case la_attach:
            n->prop->l_icols = diff (n->prop->icols,
                                     n->sem.attach.res);
            break;

        case la_cross:
            n->prop->l_icols = n->prop->icols;
            n->prop->r_icols = n->prop->icols;
            break;

        case la_eqjoin:
            /* add both join columns to the inferred icols */
            n->prop->l_icols = union_ (n->prop->icols, n->sem.eqjoin.att1);
            n->prop->r_icols = union_ (n->prop->icols, n->sem.eqjoin.att2);
            break;

        case la_semijoin:
            /* add both join columns to the inferred icols */
            n->prop->l_icols = union_ (n->prop->icols, n->sem.eqjoin.att1);
            n->prop->r_icols = n->sem.eqjoin.att2;
            break;

        case la_thetajoin:
            n->prop->l_icols = n->prop->icols;
            n->prop->r_icols = n->prop->icols;

            /* add all join columns to the inferred icols */
            for (unsigned int i = 0; i < n->sem.thetajoin.count; i++) {
                n->prop->l_icols = union_ (n->prop->l_icols,
                                           n->sem.thetajoin.pred[i].left);
                n->prop->r_icols = union_ (n->prop->r_icols,
                                           n->sem.thetajoin.pred[i].right);
            }
            break;

        case la_project:
            n->prop->l_icols = 0;
            /* rename icols columns from new to old */
            for (unsigned int i = 0; i < n->sem.proj.count; i++)
                if (n->prop->icols & n->sem.proj.items[i].new)
                    n->prop->l_icols = union_ (n->prop->l_icols,
                                               n->sem.proj.items[i].old);
            break;

        case la_select:
            /* add selected column to the inferred icols */
            n->prop->l_icols = union_ (n->prop->icols, n->sem.select.att);
            break;

        case la_pos_select:
            n->prop->l_icols = n->prop->icols;

            for (unsigned int i = 0;
                 i < PFord_count (n->sem.pos_sel.sortby);
                 i++)
                n->prop->l_icols = union_ (n->prop->l_icols,
                                           PFord_order_col_at (
                                               n->sem.pos_sel.sortby, i));

            /* only infer part if available */
            if (n->sem.pos_sel.part != att_NULL)
                n->prop->l_icols = union_ (n->prop->l_icols,
                                           n->sem.pos_sel.part);
            break;

        case la_disjunion:
            n->prop->l_icols = n->prop->icols;

            /* ensure that we have at least one icols column
               otherwise we get into trouble as icols optimization
               might infer non-matching columns */
            if (!n->prop->l_icols) {
                /* try to use a constant as it might get pruned later */
                for (unsigned int i = 0; i < n->schema.count; i++)
                    if (PFprop_const (n->prop, n->schema.items[i].name)) {
                        n->prop->l_icols = n->schema.items[i].name;
                        break;
                    }
                if (!n->prop->l_icols)
                    n->prop->l_icols = n->schema.items[0].name;
            }

            /* disjoint union also works with less columns */
            n->prop->r_icols = n->prop->l_icols;
            break;

        case la_intersect:
            /* add both intersect columns to the inferred icols */
        case la_difference:
            /* to support both scenarios where difference is
               used ((a) missing iterations and (b) except)
               extend the icols with all ocols */
        case la_distinct:
            /* to support both scenarios where distinct is
               used ((a) distinct and (b) unique iterations)
               extend the icols with all ocols */
            n->prop->l_icols = n->prop->icols;

            for (unsigned int i = 0; i < n->schema.count; i++)
                n->prop->l_icols = union_ (n->prop->l_icols,
                                           n->schema.items[i].name);

            n->prop->r_icols = n->prop->l_icols;
            break;

        case la_fun_1to1:
            n->prop->l_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            if (!(n->prop->icols & n->sem.fun_1to1.res))
                break;

            n->prop->l_icols = diff (n->prop->l_icols, n->sem.fun_1to1.res);
            for (unsigned int i = 0; i < n->sem.fun_1to1.refs.count; i++)
                n->prop->l_icols = union_ (n->prop->l_icols,
                                           n->sem.fun_1to1.refs.atts[i]);
            break;

        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_to:
            n->prop->l_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            if (!(n->prop->icols & n->sem.binary.res))
                break;

            n->prop->l_icols = diff (n->prop->l_icols, n->sem.binary.res);
            n->prop->l_icols = union_ (n->prop->l_icols, n->sem.binary.att1);
            n->prop->l_icols = union_ (n->prop->l_icols, n->sem.binary.att2);
            break;

        case la_bool_not:
            n->prop->l_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            if (!(n->prop->icols & n->sem.unary.res))
                break;

            n->prop->l_icols = diff (n->prop->l_icols, n->sem.unary.res);
            n->prop->l_icols = union_ (n->prop->l_icols, n->sem.unary.att);
            break;

        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_seqty1:
        case la_all:
            n->prop->l_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            if (!(n->prop->icols & n->sem.aggr.res))
                break;

            n->prop->l_icols = diff (n->prop->l_icols, n->sem.aggr.res);
            n->prop->l_icols = union_ (n->prop->l_icols, n->sem.aggr.att);
            /* only infer part if available */
            if (n->sem.aggr.part != att_NULL)
                n->prop->l_icols = union_ (n->prop->l_icols, n->sem.aggr.part);
            break;

        case la_count:
            n->prop->l_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            if (!(n->prop->icols & n->sem.aggr.res))
                break;

            n->prop->l_icols = diff (n->prop->l_icols, n->sem.aggr.res);
            /* only infer part if available */
            if (n->sem.aggr.part != att_NULL)
                n->prop->l_icols = union_ (n->prop->l_icols, n->sem.aggr.part);
            break;

        case la_rownum:
        case la_rowrank:
        case la_rank:
            n->prop->l_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            if (!(n->prop->icols & n->sem.sort.res))
                break;

            n->prop->l_icols = diff (n->prop->l_icols, n->sem.sort.res);

            for (unsigned int i = 0; i < PFord_count (n->sem.sort.sortby); i++)
                n->prop->l_icols = union_ (n->prop->l_icols,
                                           PFord_order_col_at (
                                               n->sem.sort.sortby, i));

            /* only infer part if available */
            if (n->sem.sort.part != att_NULL)
                n->prop->l_icols = union_ (n->prop->l_icols,
                                           n->sem.sort.part);
            break;

        case la_rowid:
            n->prop->l_icols = n->prop->icols;
            n->prop->l_icols = diff (n->prop->l_icols, n->sem.rowid.res);
            break;

        case la_type:
        case la_cast:
            n->prop->l_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            if (!(n->prop->icols & n->sem.type.res))
                break;

            n->prop->l_icols = diff (n->prop->l_icols, n->sem.type.res);
            n->prop->l_icols = union_ (n->prop->l_icols, n->sem.type.att);
            break;

        case la_type_assert:
            /* if n->sem.type.att is not present this operator
               has to be pruned -- therefore we do not care about
               inferrring this column in icols. */
            n->prop->l_icols = n->prop->icols;
            break;

        case la_step:
        case la_guide_step:
            /* infer empty list for fragments */
            n->prop->l_icols = 0;
            /* infer iter|item schema for input relation */
            n->prop->r_icols = union_ (n->sem.step.iter,
                                       n->sem.step.item);
            break;

        case la_step_join:
        case la_guide_step_join:
            /* infer empty list for fragments */
            n->prop->l_icols = 0;
            n->prop->r_icols = n->prop->icols;
            n->prop->r_icols = diff (n->prop->r_icols, n->sem.step.item_res);
            n->prop->r_icols = union_ (n->prop->r_icols, n->sem.step.item);
            break;

        case la_doc_index_join:
            /* infer empty list for fragments */
            n->prop->l_icols = 0;
            n->prop->r_icols = n->prop->icols;
            n->prop->r_icols = diff (n->prop->r_icols,
                                     n->sem.doc_join.item_res);
            n->prop->r_icols = union_ (n->prop->r_icols,
                                       n->sem.doc_join.item);
            n->prop->r_icols = union_ (n->prop->r_icols,
                                       n->sem.doc_join.item_doc);
            break;

        case la_doc_tbl:
            n->prop->l_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            if (!(n->prop->icols & n->sem.doc_tbl.res))
                break;

            n->prop->l_icols = diff (n->prop->l_icols, n->sem.doc_tbl.res);
            n->prop->l_icols = union_ (n->prop->l_icols, n->sem.doc_tbl.att);
            break;

        case la_doc_access:
            n->prop->l_icols = 0;
            n->prop->r_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            if (!(n->prop->icols & n->sem.doc_access.res))
                break;

            n->prop->r_icols = diff (n->prop->r_icols, n->sem.doc_access.res);
            n->prop->r_icols = union_ (n->prop->r_icols, n->sem.doc_access.att);
            break;

        case la_twig:
            /* make sure the underlying constructors
               propagate the correct information */
            if ((n->prop->icols & n->sem.iter_item.item)) {
                twig_needed = true;
            } else {
                twig_needed = false;
                first_twig_child = true;
            }
            prop_infer_icols (L(n), 0);
            skip_children = true;
            break;

        case la_fcns:
            break;

        case la_docnode:
            /* whenever the constructor itself is not needed column item
               is missing and therefore the name expression can be
               used as replacement */
            if (twig_needed) {
                n->prop->icols = n->sem.docnode.iter;
                twig_needed = false;
                prop_infer_icols (L(n), n->prop->icols);
                twig_needed = true;
                prop_infer_icols (R(n), 0);
                twig_needed = true;
                prop_infer_icols (R(n), 0);
            } else if (first_twig_child) {
                first_twig_child = false;
                n->prop->icols = n->sem.docnode.iter;
                prop_infer_icols (L(n), n->prop->icols);
                prop_infer_icols (R(n), 0);
            } else {
                prop_infer_icols (L(n), 0);
                prop_infer_icols (R(n), 0);
            }
            skip_children = true;
            break;

        case la_element:
            /* whenever the constructor itself is not needed column item
               is missing and therefore the name expression can be
               used as replacement */
            if (twig_needed) {
                n->prop->icols = union_ (n->sem.iter_item.iter,
                                         n->sem.iter_item.item);
                twig_needed = false;
                prop_infer_icols (L(n), n->prop->icols);
                twig_needed = true;
                prop_infer_icols (R(n), 0);
            } else if (first_twig_child) {
                first_twig_child = false;
                n->prop->icols = n->sem.iter_item.iter;
                prop_infer_icols (L(n), n->prop->icols);
                prop_infer_icols (R(n), 0);
            } else {
                prop_infer_icols (L(n), 0);
                prop_infer_icols (R(n), 0);
            }
            skip_children = true;
            break;

        case la_textnode:
        case la_comment:
            /* whenever the constructor itself is not needed column item
               is missing and therefore the name expression can be
               used as replacement */
            if (twig_needed) {
                n->prop->icols = union_ (n->sem.iter_item.iter,
                                         n->sem.iter_item.item);
                twig_needed = false;
                prop_infer_icols (L(n), n->prop->icols);
                twig_needed = true;
            } else if (first_twig_child) {
                first_twig_child = false;
                n->prop->icols = n->sem.iter_item.iter;
                prop_infer_icols (L(n), n->prop->icols);
            } else {
                prop_infer_icols (L(n), 0);
            }
            skip_children = true;
            break;

        case la_attribute:
        case la_processi:
            /* whenever the constructor itself is not needed column item
               is missing and therefore the name expression can be
               used as replacement */
            if (twig_needed) {
                n->prop->icols = union_ (
                                     n->sem.iter_item1_item2.iter,
                                     union_ (n->sem.iter_item1_item2.item1,
                                             n->sem.iter_item1_item2.item2));
                twig_needed = false;
                prop_infer_icols (L(n), n->prop->icols);
                twig_needed = true;
            } else if (first_twig_child) {
                first_twig_child = false;
                n->prop->icols = n->sem.iter_item1_item2.iter;
                prop_infer_icols (L(n), n->prop->icols);
            } else {
                prop_infer_icols (L(n), 0);
            }
            skip_children = true;
            break;

        case la_content:
            /* whenever the constructor itself is not needed column item
               is missing and therefore the name expression can be
               used as replacement */
            if (twig_needed) {
                n->prop->icols = union_ (n->sem.iter_pos_item.iter,
                                         union_ (n->sem.iter_pos_item.pos,
                                                 n->sem.iter_pos_item.item));
                twig_needed = false;
                prop_infer_icols (L(n), 0);
                prop_infer_icols (R(n), n->prop->icols);
                twig_needed = true;
            } else if (first_twig_child) {
                n->prop->icols = n->sem.iter_pos_item.iter;
                first_twig_child = false;
                prop_infer_icols (L(n), 0);
                prop_infer_icols (R(n), n->prop->icols);
            } else {
                prop_infer_icols (L(n), 0);
                prop_infer_icols (R(n), 0);
            }
            skip_children = true;
            break;

        case la_merge_adjacent:
            /* infer empty list for fragments */
            n->prop->l_icols = 0;
            /* infer iter|pos|item schema for element content relation */
            n->prop->r_icols = union_ (union_ (n->sem.merge_adjacent.iter_in,
                                               n->sem.merge_adjacent.item_in),
                                       n->sem.merge_adjacent.pos_in);
            break;

        case la_roots:
            /* infer incoming icols for input relation */
            n->prop->l_icols = n->prop->icols;
            break;

        case la_frag_union:
            n->prop->r_icols = 0;
        case la_fragment:
        case la_frag_extract:
        case la_empty_frag:
        case la_fun_frag_param:
            /* infer empty list for fragments */
            n->prop->l_icols = 0;
            break;

        case la_error:
            n->prop->l_icols = n->prop->icols;
            n->prop->l_icols = union_ (n->prop->l_icols, n->sem.err.att);
            break;
            
        case la_cond_err:
            /* infer incoming icols for input relation */
            n->prop->l_icols = n->prop->icols;
            /* infer attribute that triggers error generation
               for error checking relation  */
            n->prop->r_icols = n->sem.err.att;
            break;

        case la_nil:
            break;

        case la_trace:
            n->prop->l_icols = n->prop->icols;
            n->prop->l_icols = union_ (n->prop->l_icols,
                                       n->sem.iter_pos_item.iter);
            n->prop->l_icols = union_ (n->prop->l_icols,
                                       n->sem.iter_pos_item.pos);
            n->prop->l_icols = union_ (n->prop->l_icols,
                                       n->sem.iter_pos_item.item);
            break;

        case la_trace_msg:
            n->prop->l_icols = union_ (n->sem.iter_item.iter,
                                       n->sem.iter_item.item);
            break;

        case la_trace_map:
            n->prop->l_icols = union_ (n->sem.trace_map.inner,
                                       n->sem.trace_map.outer);
            break;

        case la_rec_fix:
            /* infer the required columns of the result */
            n->prop->r_icols = n->prop->icols;
            prop_infer_icols (R(n), n->prop->r_icols);

            /* start an alternative traversal of the recursion
               nodes to ensure that the body of the recursion
               and thus the base operators all contain icols
               properties before the properties of the seeds are
               inferred. */
            prop_infer_icols_rec_body (L(n));

            /* infer empty list for parameter list */
            n->prop->l_icols = 0;

            prop_infer_icols (L(n), n->prop->l_icols);

            skip_children = true;
            break;

        case la_rec_param:
            /* infer empty list for parameter list */
            n->prop->l_icols = 0;
            n->prop->r_icols = 0;
            break;

        case la_rec_arg:
            /* the properties of the body are already inferred
               (see prop_infer_icols_rec_body in la_rec_fix) */

            /* the icols of the seed are the resulting icols of the body */
            n->prop->l_icols = n->sem.rec_arg.base->prop->icols;

            /* The above only works if the icols are already inferred.
               If that's not the case choose the full schema as seed
               for L(n) */
            if (!n->prop->l_icols) {
                n->prop->l_icols = n->prop->r_icols;
                PFlog ("icols property inference of the recursion "
                       "does not work as expected.");
            }

            prop_infer_icols (L(n), n->prop->l_icols);

            skip_children = true;
            break;

        case la_rec_base:
            break;

        case la_fun_call:
            n->prop->l_icols = n->sem.fun_call.iter;
            break;
            
        case la_fun_param:
            for (unsigned int i = 0; i < n->schema.count; i++)
                n->prop->l_icols = union_ (n->prop->l_icols,
                                           n->schema.items[i].name);
            break;
            
        case la_proxy:
        case la_proxy_base:
            /* infer incoming icols for input relation */
            n->prop->l_icols = n->prop->icols;
            break;

        case la_string_join:
            /* infer iter|pos|item schema for first input relation */
            n->prop->l_icols = union_ (union_ (n->sem.string_join.iter,
                                               n->sem.string_join.item),
                                       n->sem.string_join.pos);
            /* infer iter|item schema for second input relation */
            n->prop->r_icols = union_ (n->sem.string_join.iter_sep,
                                       n->sem.string_join.item_sep);
            break;

        case la_eqjoin_unq:
            PFoops (OOPS_FATAL,
                    "clone column aware equi-join operator is "
                    "only allowed with unique names!");

        case la_cross_mvd:
            PFoops (OOPS_FATAL,
                    "clone column aware cross product operator is "
                    "only allowed inside mvd optimization!");

        case la_dummy:
            /* infer incoming icols for input relation */
            n->prop->l_icols = n->prop->icols;
            break;
    }

    if (!skip_children)
        /* infer properties for children */
        for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
            prop_infer_icols (n->child[i],
                              /* infer the respective icols property */
                              i==0?n->prop->l_icols:n->prop->r_icols);
}

/* worker for PFprop_infer_icol */
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

    /* reset icols property */
    n->prop->icols = 0;
    n->prop->l_icols = 0;
    n->prop->r_icols = 0;
}

/**
 * Infer icols property for a DAG rooted in @a root starting
 * with the icols collected in @a icols.
 */
void
PFprop_infer_icol_specific (PFla_op_t *root, PFalg_att_t icols) {
    /* collect number of incoming edges (parents) */
    prop_infer (root);
    PFla_dag_reset (root);

    /* second run infers icols property */
    prop_infer_icols (root, icols);
}

/**
 * Infer icols property for a DAG rooted in @a root
 */
void
PFprop_infer_icol (PFla_op_t *root) {
    PFprop_infer_icol_specific (root, 0);
}


