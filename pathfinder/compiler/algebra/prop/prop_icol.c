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

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define SEEN(n)       (n)->bit_dag
#define EDGE(n)       (n)->refctr

/**
 * worker for PFprop_icol;
 * Test if @a col is in the list of icol columns in array @a icols
 */
static bool
icol_worker (PFalg_collist_t *icols, PFalg_col_t col)
{
    if (!icols) return false;

    for (unsigned int i = 0; i < clsize (icols); i++)
        if (col == clat (icols, i))
            return true;

    return false;
}

/**
 * Test if @a col is in the list of icol columns in container @a prop
 */
bool
PFprop_icol (const PFprop_t *prop, PFalg_col_t col)
{
    return icol_worker (prop->icols, col);
}

/**
 * Test if @a col is in the list of icol columns of the left child
 * (information is stored in property container @a prop)
 */
bool
PFprop_icol_left (const PFprop_t *prop, PFalg_col_t col)
{
    return icol_worker (prop->l_icols, col);
}

/**
 * Test if @a col is in the list of icol columns of the right child
 * (information is stored in property container @a prop)
 */
bool
PFprop_icol_right (const PFprop_t *prop, PFalg_col_t col)
{
    return icol_worker (prop->r_icols, col);
}

/*
 * count number of icols columns
 */
unsigned int
PFprop_icols_count (const PFprop_t *prop)
{
    if (!prop->icols)
        return 0;
    else
        return clsize (prop->icols);
}

/**
 * Return icols columns in an collist.
 */
PFalg_collist_t *
PFprop_icols_to_collist (const PFprop_t *prop)
{
    if (!prop->icols)
        return PFalg_collist (0);
    else
        return PFalg_collist_copy (prop->icols);
}

/**
 * Returns the intersection of an icols list @a icols and a schema
 * @a schema_ocols
 */
static void
intersect_ocol (PFalg_collist_t *icols, PFla_op_t *n)
{
    unsigned int i = 0;
    assert (icols);
    while (i < clsize (icols)) {
        if (PFprop_ocol (n, clat (icols, i)))
            i++;
        else { /* no match -- remove the column */
            clat (icols, i) = cltop (icols);
            clsize (icols)--;
        }
    }
}

/**
 * Returns union of two icols lists
 */
static void
union_ (PFalg_collist_t *a, PFalg_col_t b)
{
    assert (a);

    if (!icol_worker (a, b))
        cladd (a) = b;
}

/**
 * Extends icol list @a with all the items
 * of the icol list @a b that are not in @a a/
 */
static void
union_list (PFalg_collist_t *a, PFalg_collist_t *b)
{
    PFalg_col_t cur;

    assert (a);
    if (!b) return;

    for (unsigned int i = 0; i < clsize (b); i++) {
        cur = clat (b, i);
        if (!icol_worker (a, cur))
            cladd (a) = cur;
    }
}

/**
 * Returns difference of two icols lists
 */
static void
diff (PFalg_collist_t *a, PFalg_col_t b)
{
    unsigned int i = 0;
    while (i < clsize (a)) {
        if (clat (a, i) != b)
            i++;
        else { /* b found -- remove the column */
            clat (a, i) = cltop (a);
            clsize (a)--;
            return;
        }
    }
}

/**
 * Check if a column @a b appears in list @a a.
 */
static bool
in (PFalg_collist_t *a, PFalg_col_t b)
{
    return (icol_worker (a, b));
}

/**
 * Check if a list @a a is empty.
 */
static bool
empty (PFalg_collist_t *a)
{
    return clsize (a) == 0;
}

static void
copy (PFalg_collist_t *base, PFalg_collist_t *content)
{
    for (unsigned int i = 0; i < clsize (content); i++)
        cladd (base) = clat (content, i);
}

/* forward declaration */
static void prop_infer_icols (PFla_op_t *, PFalg_collist_t *);

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
    if (n->kind == la_side_effects) {
        prop_infer_icols (L(n), n->prop->l_icols);
        prop_infer_icols_rec_body (R(n));
    }
    else if (n->kind == la_rec_param) {
        prop_infer_icols_rec_body (L(n));
        prop_infer_icols_rec_body (R(n));
    }
    else if (n->kind == la_nil)
        return;
    else if (n->kind == la_rec_arg) {
        /* the required columns of the body are all the
           columns of the schema */
        for (unsigned int i = 0; i < n->schema.count; i++)
            union_ (n->prop->r_icols, n->schema.items[i].name);
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
prop_infer_icols (PFla_op_t *n, PFalg_collist_t *icols)
{
    /* for element construction we need a special translation
       and therefore skip the default inference of the children */
    bool skip_children = false;

    assert (n);

    /* collect the icols properties of all parents*/
    union_list (n->prop->icols, icols);

    /* nothing to do if we haven't collected
       all incoming icols lists of that node */
    if (EDGE(n) > 1) {
        EDGE(n)--;
        return;
    }

    /* remove all unnecessary columns from icols */
    intersect_ocol (n->prop->icols, n);

    /* infer icols property for children */
    switch (n->kind) {
        case la_serialize_seq:
            /* infer empty list for fragments */
            union_ (n->prop->r_icols, n->sem.ser_seq.pos);
            union_ (n->prop->r_icols, n->sem.ser_seq.item);
            break;

        case la_serialize_rel:
            union_ (n->prop->r_icols, n->sem.ser_rel.iter);
            union_ (n->prop->r_icols, n->sem.ser_rel.pos);
            for (unsigned int i = 0; i < clsize (n->sem.ser_rel.items); i++)
                union_ (n->prop->r_icols, clat (n->sem.ser_rel.items, i));
            break;

        case la_side_effects:
            /* infer empty list for side effects */
            n->prop->r_icols = NULL;
            n->prop->l_icols = NULL;
            break;

        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
            break;

        case la_attach:
            copy (n->prop->l_icols, n->prop->icols);
            diff (n->prop->l_icols, n->sem.attach.res);
            break;

        case la_cross:
            copy (n->prop->l_icols, n->prop->icols);
            copy (n->prop->r_icols, n->prop->icols);
            break;

        case la_eqjoin:
            copy (n->prop->l_icols, n->prop->icols);
            copy (n->prop->r_icols, n->prop->icols);
            /* add both join columns to the inferred icols */
            union_ (n->prop->l_icols, n->sem.eqjoin.col1);
            union_ (n->prop->r_icols, n->sem.eqjoin.col2);
            break;

        case la_semijoin:
            copy (n->prop->l_icols, n->prop->icols);
            /* add both join columns to the inferred icols */
            union_ (n->prop->l_icols, n->sem.eqjoin.col1);
            union_ (n->prop->r_icols, n->sem.eqjoin.col2);
            break;

        case la_thetajoin:
            copy (n->prop->l_icols, n->prop->icols);
            copy (n->prop->r_icols, n->prop->icols);

            /* add all join columns to the inferred icols */
            for (unsigned int i = 0; i < n->sem.thetajoin.count; i++) {
                union_ (n->prop->l_icols, n->sem.thetajoin.pred[i].left);
                union_ (n->prop->r_icols, n->sem.thetajoin.pred[i].right);
            }
            break;

        case la_project:
            /* rename icols columns from new to old */
            for (unsigned int i = 0; i < n->sem.proj.count; i++)
                if (in (n->prop->icols, n->sem.proj.items[i].new))
                    union_ (n->prop->l_icols, n->sem.proj.items[i].old);
            break;

        case la_select:
            copy (n->prop->l_icols, n->prop->icols);
            /* add selected column to the inferred icols */
            union_ (n->prop->l_icols, n->sem.select.col);
            break;

        case la_pos_select:
            copy (n->prop->l_icols, n->prop->icols);

            for (unsigned int i = 0;
                 i < PFord_count (n->sem.pos_sel.sortby);
                 i++)
                union_ (n->prop->l_icols,
                        PFord_order_col_at (n->sem.pos_sel.sortby, i));

            /* only infer part if available */
            if (n->sem.pos_sel.part != col_NULL)
                union_ (n->prop->l_icols, n->sem.pos_sel.part);
            break;

        case la_disjunion:
            copy (n->prop->l_icols, n->prop->icols);

            /* ensure that we have at least one icols column
               otherwise we get into trouble as icols optimization
               might infer non-matching columns */
            if (empty (n->prop->l_icols)) {
                /* try to use a constant as it might get pruned later */
                for (unsigned int i = 0; i < n->schema.count; i++)
                    if (PFprop_const (n->prop, n->schema.items[i].name)) {
                        union_ (n->prop->l_icols, n->schema.items[i].name);
                        break;
                    }
                if (empty (n->prop->l_icols))
                    union_ (n->prop->l_icols, n->schema.items[0].name);
            }

            /* disjoint union also works with less columns */
            copy (n->prop->r_icols, n->prop->l_icols);
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
            copy (n->prop->l_icols, n->prop->icols);

            for (unsigned int i = 0; i < n->schema.count; i++)
                union_ (n->prop->l_icols, n->schema.items[i].name);

            if (R(n))
                copy (n->prop->r_icols, n->prop->l_icols);
            break;

        case la_fun_1to1:
            copy (n->prop->l_icols, n->prop->icols);

            /* do not infer input columns if operator is not required */
            if (!in (n->prop->icols, n->sem.fun_1to1.res))
                break;

            diff (n->prop->l_icols, n->sem.fun_1to1.res);
            for (unsigned int i = 0; i < clsize (n->sem.fun_1to1.refs); i++)
                union_ (n->prop->l_icols, clat (n->sem.fun_1to1.refs, i));
            break;

        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_to:
            copy (n->prop->l_icols, n->prop->icols);

            /* do not infer input columns if operator is not required */
            if (!in (n->prop->icols, n->sem.binary.res))
                break;

            diff (n->prop->l_icols, n->sem.binary.res);
            union_ (n->prop->l_icols, n->sem.binary.col1);
            union_ (n->prop->l_icols, n->sem.binary.col2);
            break;

        case la_bool_not:
            copy (n->prop->l_icols, n->prop->icols);

            /* do not infer input columns if operator is not required */
            if (!in (n->prop->icols, n->sem.unary.res))
                break;

            diff (n->prop->l_icols, n->sem.unary.res);
            union_ (n->prop->l_icols, n->sem.unary.col);
            break;

        case la_aggr:
            if (n->sem.aggr.part)
                union_ (n->prop->l_icols, n->sem.aggr.part);
            /* only infer the columns that are necessary for the output */
            for (unsigned int i = 0; i < n->sem.aggr.count; i++)
                if (n->sem.aggr.aggr[i].kind != alg_aggr_count &&
                    in (n->prop->icols, n->sem.aggr.aggr[i].res))
                    union_ (n->prop->l_icols, n->sem.aggr.aggr[i].col);
            break;

        case la_rownum:
        case la_rowrank:
        case la_rank:
            copy (n->prop->l_icols, n->prop->icols);

            /* do not infer input columns if operator is not required */
            if (!in (n->prop->icols, n->sem.sort.res))
                break;

            diff (n->prop->l_icols, n->sem.sort.res);

            for (unsigned int i = 0; i < PFord_count (n->sem.sort.sortby); i++)
                union_ (n->prop->l_icols,
                        PFord_order_col_at (n->sem.sort.sortby, i));

            /* only infer part if available */
            if (n->sem.sort.part != col_NULL)
                union_ (n->prop->l_icols, n->sem.sort.part);
            break;

        case la_rowid:
            copy (n->prop->l_icols, n->prop->icols);
            diff (n->prop->l_icols, n->sem.rowid.res);
            break;

        case la_type:
        case la_cast:
            copy (n->prop->l_icols, n->prop->icols);

            /* do not infer input columns if operator is not required */
            if (!in (n->prop->icols, n->sem.type.res))
                break;

            diff (n->prop->l_icols, n->sem.type.res);
            union_ (n->prop->l_icols, n->sem.type.col);
            break;

        case la_type_assert:
            /* if n->sem.type.col is not present this operator
               has to be pruned -- therefore we do not care about
               inferrring this column in icols. */
            copy (n->prop->l_icols, n->prop->icols);
            break;

        case la_step:
        case la_guide_step:
            /* infer empty list for fragments */
            /* infer iter|item schema for input relation */
            union_ (n->prop->r_icols, n->sem.step.iter);
            union_ (n->prop->r_icols, n->sem.step.item);
            break;

        case la_step_join:
        case la_guide_step_join:
            /* infer empty list for fragments */
            copy (n->prop->r_icols, n->prop->icols);
            diff (n->prop->r_icols, n->sem.step.item_res);
            union_ (n->prop->r_icols, n->sem.step.item);
            break;

        case la_doc_index_join:
            /* infer empty list for fragments */
            copy (n->prop->r_icols, n->prop->icols);
            diff (n->prop->r_icols, n->sem.doc_join.item_res);
            union_ (n->prop->r_icols, n->sem.doc_join.item);
            union_ (n->prop->r_icols, n->sem.doc_join.item_doc);
            break;

        case la_doc_tbl:
            copy (n->prop->l_icols, n->prop->icols);

            /* do not infer input columns if operator is not required */
            if (!in (n->prop->icols, n->sem.doc_tbl.res))
                break;

            diff (n->prop->l_icols, n->sem.doc_tbl.res);
            union_ (n->prop->l_icols, n->sem.doc_tbl.col);
            break;

        case la_doc_access:
            copy (n->prop->r_icols, n->prop->icols);

            /* do not infer input columns if operator is not required */
            if (!in (n->prop->icols, n->sem.doc_access.res))
                break;

            diff (n->prop->r_icols, n->sem.doc_access.res);
            union_ (n->prop->r_icols, n->sem.doc_access.col);
            break;

        case la_twig:
            /* make sure the underlying constructors
               propagate the correct information */
            if (in (n->prop->icols, n->sem.iter_item.item)) {
                twig_needed = true;
            } else {
                twig_needed = false;
                first_twig_child = true;
            }
            prop_infer_icols (L(n), NULL);
            skip_children = true;
            break;

        case la_fcns:
            break;

        case la_docnode:
            /* whenever the constructor itself is not needed column item
               is missing and therefore the name expression can be
               used as replacement */
            if (twig_needed) {
                union_ (n->prop->icols, n->sem.docnode.iter);
                twig_needed = false;
                copy (n->prop->l_icols, n->prop->icols);
                prop_infer_icols (L(n), n->prop->icols);
                twig_needed = true;
                prop_infer_icols (R(n), NULL);
            } else if (first_twig_child) {
                first_twig_child = false;
                union_ (n->prop->icols, n->sem.docnode.iter);
                copy (n->prop->l_icols, n->prop->icols);
                prop_infer_icols (L(n), n->prop->icols);
                prop_infer_icols (R(n), NULL);
            } else {
                prop_infer_icols (L(n), NULL);
                prop_infer_icols (R(n), NULL);
            }
            skip_children = true;
            break;

        case la_element:
            /* whenever the constructor itself is not needed column item
               is missing and therefore the name expression can be
               used as replacement */
            if (twig_needed) {
                union_ (n->prop->icols, n->sem.iter_item.iter);
                union_ (n->prop->icols, n->sem.iter_item.item);
                twig_needed = false;
                copy (n->prop->l_icols, n->prop->icols);
                prop_infer_icols (L(n), n->prop->icols);
                twig_needed = true;
                prop_infer_icols (R(n), NULL);
            } else if (first_twig_child) {
                first_twig_child = false;
                union_ (n->prop->icols, n->sem.iter_item.iter);
                copy (n->prop->l_icols, n->prop->icols);
                prop_infer_icols (L(n), n->prop->icols);
                prop_infer_icols (R(n), NULL);
            } else {
                prop_infer_icols (L(n), NULL);
                prop_infer_icols (R(n), NULL);
            }
            skip_children = true;
            break;

        case la_textnode:
        case la_comment:
            /* whenever the constructor itself is not needed column item
               is missing and therefore the name expression can be
               used as replacement */
            if (twig_needed) {
                union_ (n->prop->icols, n->sem.iter_item.iter);
                union_ (n->prop->icols, n->sem.iter_item.item);
                twig_needed = false;
                copy (n->prop->l_icols, n->prop->icols);
                prop_infer_icols (L(n), n->prop->icols);
                twig_needed = true;
            } else if (first_twig_child) {
                first_twig_child = false;
                union_ (n->prop->icols, n->sem.iter_item.iter);

                /* special textnode treating as we do not prune
                   a single (stand-alone) textnode constructor */
                if (n->kind == la_textnode)
                    union_ (n->prop->icols, n->sem.iter_item.item);

                copy (n->prop->l_icols, n->prop->icols);
                prop_infer_icols (L(n), n->prop->icols);
            } else {
                prop_infer_icols (L(n), NULL);
            }
            skip_children = true;
            break;

        case la_attribute:
        case la_processi:
            /* whenever the constructor itself is not needed column item
               is missing and therefore the name expression can be
               used as replacement */
            if (twig_needed) {
                union_ (n->prop->icols, n->sem.iter_item1_item2.iter);
                union_ (n->prop->icols, n->sem.iter_item1_item2.item1);
                union_ (n->prop->icols, n->sem.iter_item1_item2.item2);
                twig_needed = false;
                copy (n->prop->l_icols, n->prop->icols);
                prop_infer_icols (L(n), n->prop->icols);
                twig_needed = true;
            } else if (first_twig_child) {
                first_twig_child = false;
                union_ (n->prop->icols, n->sem.iter_item1_item2.iter);
                copy (n->prop->l_icols, n->prop->icols);
                prop_infer_icols (L(n), n->prop->icols);
            } else {
                prop_infer_icols (L(n), NULL);
            }
            skip_children = true;
            break;

        case la_content:
            /* whenever the constructor itself is not needed column item
               is missing and therefore the name expression can be
               used as replacement */
            if (twig_needed) {
                union_ (n->prop->icols, n->sem.iter_pos_item.iter);
                union_ (n->prop->icols, n->sem.iter_pos_item.pos);
                union_ (n->prop->icols, n->sem.iter_pos_item.item);
                twig_needed = false;
                prop_infer_icols (L(n), NULL);
                copy (n->prop->r_icols, n->prop->icols);
                prop_infer_icols (R(n), n->prop->icols);
                twig_needed = true;
            } else if (first_twig_child) {
                union_ (n->prop->icols, n->sem.iter_pos_item.iter);
                first_twig_child = false;
                prop_infer_icols (L(n), NULL);
                copy (n->prop->r_icols, n->prop->icols);
                prop_infer_icols (R(n), n->prop->icols);
            } else {
                prop_infer_icols (L(n), NULL);
                prop_infer_icols (R(n), NULL);
            }
            skip_children = true;
            break;

        case la_merge_adjacent:
            /* infer empty list for fragments */
            /* infer iter|pos|item schema for element content relation */
            union_ (n->prop->r_icols, n->sem.merge_adjacent.iter_in);
            union_ (n->prop->r_icols, n->sem.merge_adjacent.pos_in);
            union_ (n->prop->r_icols, n->sem.merge_adjacent.item_in);
            break;

        case la_roots:
            /* infer incoming icols for input relation */
            copy (n->prop->l_icols, n->prop->icols);
            break;

        case la_frag_union:
            n->prop->r_icols = NULL;
        case la_fragment:
        case la_frag_extract:
        case la_empty_frag:
        case la_fun_frag_param:
            /* infer empty list for fragments */
            n->prop->l_icols = NULL;
            break;

        case la_error:
            /* infer empty list for other side effects */
            n->prop->l_icols = NULL;
            /* infer column that triggers error generation
               for error checking relation  */
            union_ (n->prop->r_icols, n->sem.err.col);
            break;

        case la_nil:
            break;

        case la_cache:
            /* infer empty list for other side effects */
            n->prop->l_icols = NULL;
            /* infer columns that are needed for caching */
            union_ (n->prop->r_icols, n->sem.cache.pos);
            union_ (n->prop->r_icols, n->sem.cache.item);
            break;

        case la_trace:
            /* infer empty list for other side effects */
            n->prop->l_icols = NULL;
            /* infer empty list for traces */
            n->prop->l_icols = NULL;
            break;

        case la_trace_items:
            union_ (n->prop->l_icols, n->sem.iter_pos_item.iter);
            union_ (n->prop->l_icols, n->sem.iter_pos_item.pos);
            union_ (n->prop->l_icols, n->sem.iter_pos_item.item);
            break;

        case la_trace_msg:
            union_ (n->prop->l_icols, n->sem.iter_item.iter);
            union_ (n->prop->l_icols, n->sem.iter_item.item);
            break;

        case la_trace_map:
            union_ (n->prop->l_icols, n->sem.trace_map.inner);
            union_ (n->prop->l_icols, n->sem.trace_map.outer);
            break;

        case la_rec_fix:
            /* infer the required columns of the result */
            copy (n->prop->r_icols, n->prop->icols);
            prop_infer_icols (R(n), n->prop->r_icols);

            /* start an alternative traversal of the recursion
               nodes to ensure that the body of the recursion
               and thus the base operators all contain icols
               properties before the properties of the seeds are
               inferred. */
            prop_infer_icols_rec_body (L(n));

            /* infer empty list for parameter list */
            n->prop->l_icols = NULL;

            prop_infer_icols (L(n), n->prop->l_icols);

            skip_children = true;
            break;

        case la_rec_param:
            /* infer empty list for parameter list */
            n->prop->l_icols = NULL;
            n->prop->r_icols = NULL;
            break;

        case la_rec_arg:
            /* the properties of the body are already inferred
               (see prop_infer_icols_rec_body in la_rec_fix) */

            /* the icols of the seed are the resulting icols of the body */
            union_list (n->prop->l_icols, n->sem.rec_arg.base->prop->icols);

            /* The above only works if the icols are already inferred.
               If that's not the case choose the full schema as seed
               for L(n) */
            if (empty (n->prop->l_icols)) {
                copy (n->prop->l_icols, n->prop->r_icols);
                PFlog ("icols property inference of the recursion "
                       "does not work as expected.");
            }

            prop_infer_icols (L(n), n->prop->l_icols);

            skip_children = true;
            break;

        case la_rec_base:
            break;

        case la_fun_call:
            union_ (n->prop->l_icols, n->sem.fun_call.iter);
            break;

        case la_fun_param:
            for (unsigned int i = 0; i < n->schema.count; i++)
                union_ (n->prop->l_icols, n->schema.items[i].name);
            break;

        case la_proxy:
        case la_proxy_base:
            /* infer incoming icols for input relation */
            copy (n->prop->l_icols, n->prop->icols);
            break;

        case la_string_join:
            /* infer iter|pos|item schema for first input relation */
            union_ (n->prop->l_icols, n->sem.string_join.iter);
            union_ (n->prop->l_icols, n->sem.string_join.pos);
            union_ (n->prop->l_icols, n->sem.string_join.item);
            /* infer iter|item schema for second input relation */
            union_ (n->prop->r_icols, n->sem.string_join.iter_sep);
            union_ (n->prop->r_icols, n->sem.string_join.item_sep);
            break;

        case la_internal_op:
            /* interpret this operator as internal join */
            if (n->sem.eqjoin_opt.kind == la_eqjoin) {
#define proj_at(l,i) (*(PFalg_proj_t *) PFarray_at ((l),(i)))
                PFarray_t  *lproj = n->sem.eqjoin_opt.lproj,
                           *rproj = n->sem.eqjoin_opt.rproj;

                /* add both join columns to the inferred icols */
                union_ (n->prop->l_icols, proj_at(lproj, 0).old);
                union_ (n->prop->r_icols, proj_at(rproj, 0).old);

                /* rename icols columns from new to old */
                for (unsigned int i = 1; i < PFarray_last (lproj); i++)
                    if (in (n->prop->icols, proj_at(lproj, i).new))
                        union_ (n->prop->l_icols, proj_at(lproj, i).old);

                /* rename icols columns from new to old */
                for (unsigned int i = 1; i < PFarray_last (rproj); i++)
                    if (in (n->prop->icols, proj_at(rproj, i).new))
                        union_ (n->prop->r_icols, proj_at(rproj, i).old);
            }
            else
                PFoops (OOPS_FATAL,
                        "internal optimization operator is not allowed here");
            break;

        case la_dummy:
            /* infer incoming icols for input relation */
            copy (n->prop->l_icols, n->prop->icols);
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

    /* reset icols property
       (reuse already existing lists if already available
        as this increases the performance of the compiler a lot) */
    if (n->prop->icols)
        clsize (n->prop->icols) = 0;
    else
        n->prop->icols = PFalg_collist (10);

    if (L(n)) {
        if (n->prop->l_icols)
            clsize (n->prop->l_icols) = 0;
        else
            n->prop->l_icols = PFalg_collist (10);
    }

    if (R(n)) {
        if (n->prop->r_icols)
            clsize (n->prop->r_icols) = 0;
        else
            n->prop->r_icols = PFalg_collist (10);
    }
}

/**
 * Infer icols property for a DAG rooted in @a root starting
 * with the icols collected in @a icols.
 */
void
PFprop_infer_icol_specific (PFla_op_t *root, PFalg_collist_t *icols)
{
    /* collect number of incoming edges (parents) */
    prop_infer (root);
    PFla_dag_reset (root);

    /* second run infers icols property */
    prop_infer_icols (root, PFalg_collist_copy (icols));
}

/**
 * Infer icols property for a DAG rooted in @a root
 */
void
PFprop_infer_icol (PFla_op_t *root) {
    /* collect number of incoming edges (parents) */
    prop_infer (root);
    PFla_dag_reset (root);

    prop_infer_icols (root, NULL);
}

/* vim:set shiftwidth=4 expandtab: */
