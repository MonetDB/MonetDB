/**
 * @file
 *
 * Inference of constant properties of logical algebra expressions.
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

/**
 * Test if @a col is marked constant in property container @a prop.
 */
bool
PFprop_const (const PFprop_t *prop, PFalg_col_t col)
{
    assert (prop);
    if (!prop->constants) return false;

    for (unsigned int i = 0; i < PFarray_last (prop->constants); i++)
        if (col == ((const_t *) PFarray_at (prop->constants, i))->col)
            return true;

    return false;
}

/**
 * Test if @a col is marked constant in the left child
 * (information is stored in property container @a prop)
 */
bool
PFprop_const_left (const PFprop_t *prop, PFalg_col_t col)
{
    assert (prop);
    if (!prop->l_constants) return false;

    for (unsigned int i = 0; i < PFarray_last (prop->l_constants); i++)
        if (col == ((const_t *) PFarray_at (prop->l_constants, i))->col)
            return true;

    return false;
}

/**
 * Test if @a col is marked constant in the left child
 * (information is stored in property container @a prop)
 */
bool
PFprop_const_right (const PFprop_t *prop, PFalg_col_t col)
{
    assert (prop);
    if (!prop->r_constants) return false;

    for (unsigned int i = 0; i < PFarray_last (prop->r_constants); i++)
        if (col == ((const_t *) PFarray_at (prop->r_constants, i))->col)
            return true;

    return false;
}

/* worker for PFprop_const_val(_left|_right)? */
static PFalg_atom_t
const_val (PFarray_t *constants, PFalg_col_t col)
{
    assert (constants);

    for (unsigned int i = 0; i < PFarray_last (constants); i++)
        if (col == ((const_t *) PFarray_at (constants, i))->col)
            return ((const_t *) PFarray_at (constants, i))->value;

    PFoops (OOPS_FATAL,
            "could not find column that is supposed to be constant: `%s'",
            PFcol_str (col));

    assert (0); /* never reached due to "exit" in PFoops */
    return PFalg_lit_int (0); /* pacify picky compilers */
}

/**
 * Lookup value of @a col in property container @a prop.  Attribute
 * @a col must be marked constant, otherwise the function will fail.
 */
PFalg_atom_t
PFprop_const_val (const PFprop_t *prop, PFalg_col_t col)
{
    assert (prop);

    return const_val (prop->constants, col);
}

/**
 * Lookup value of @a col in the list of constants of the left
 * child. (Information resides in property container @a prop.)
 * Attribute @a col must be marked constant, otherwise
 * the function will fail.
 */
PFalg_atom_t
PFprop_const_val_left (const PFprop_t *prop, PFalg_col_t col)
{
    assert (prop);
    assert (prop->l_constants);

    return const_val (prop->l_constants, col);
}

/**
 * Lookup value of @a col in the list of constants of the right
 * child. (Information resides in property container @a prop.)
 * Attribute @a col must be marked constant, otherwise
 * the function will fail.
 */
PFalg_atom_t
PFprop_const_val_right (const PFprop_t *prop, PFalg_col_t col)
{
    assert (prop);
    assert (prop->r_constants);

    return const_val (prop->r_constants, col);
}

/* the following 3 functions are used for debug printing */
/**
 * Return number of columns marked const.
 */
unsigned int
PFprop_const_count (const PFprop_t *prop)
{
    assert (prop);
    if (!prop->constants) return 0;

    return PFarray_last (prop->constants);
}

/**
 * Return name of constant column number @a i (in container @a prop).
 * (Needed, e.g., to iterate over constant columns.)
 */
PFalg_col_t
PFprop_const_at (const PFprop_t *prop, unsigned int i)
{
    assert (prop);
    assert (prop->constants);

    return ((const_t *) PFarray_at (prop->constants, i))->col;
}

/**
 * Return value of constant column number @a i (in container @a prop).
 * (Needed, e.g., to iterate over constant columns.)
 */
PFalg_atom_t
PFprop_const_val_at (const PFprop_t *prop, unsigned int i)
{
    assert (prop);
    assert (prop->constants);

    return ((const_t *) PFarray_at (prop->constants, i))->value;
}

/**
 * Mark @a col as constant with value @a value in node @a n.
 */
static void
PFprop_mark_const (PFprop_t *prop, PFalg_col_t col, PFalg_atom_t value)
{
    assert (prop);
    assert (prop->constants);


#ifndef NDEBUG
    if (PFprop_const (prop, col))
        PFoops (OOPS_FATAL,
                "column `%s' already declared constant",
                PFcol_str (col));
#endif

    *(const_t *) PFarray_add (prop->constants)
        = (const_t) { .col = col, .value = value };
}

static void
copy (PFarray_t *base, PFarray_t *content)
{
    for (unsigned int i = 0; i < PFarray_last (content); i++)
        *(const_t *) PFarray_add (base) =
            *(const_t *) PFarray_at (content, i);
}

/**
 * Infer properties about constant columns; worker for prop_infer().
 */
static void
infer_const (PFla_op_t *n)
{
    /* first get the properties of the children */
    if (L(n)) copy (n->prop->l_constants, L(n)->prop->constants);
    if (R(n)) copy (n->prop->r_constants, R(n)->prop->constants);

    /*
     * Several operates (at least) propagate constant columns
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
        case la_dummy:

            /* propagate information from both input operators */
            for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
                for (unsigned int j = 0;
                        j < PFprop_const_count (n->child[i]->prop); j++)
                    if (!PFprop_const (n->prop,
                                       PFprop_const_at (n->child[i]->prop, j)))
                        PFprop_mark_const (
                                n->prop,
                                PFprop_const_at (n->child[i]->prop, j),
                                PFprop_const_val_at (n->child[i]->prop, j));
            break;

        default:
            break;
    }

    /*
     * Now consider more specific stuff from various rules.
     */
    switch (n->kind) {

        case la_lit_tbl:

            /* check for constant columns */
            for (unsigned int col = 0; col < n->schema.count; col++) {

                bool          constant = true;
                PFalg_atom_t  val;

                for (unsigned int row = 0; row < n->sem.lit_tbl.count; row++)
                    if (row == 0)
                        val = n->sem.lit_tbl.tuples[row].atoms[col];
                    else
                        if (!PFalg_atom_comparable (
                                 val,
                                 n->sem.lit_tbl.tuples[row].atoms[col]) ||
                            PFalg_atom_cmp (
                                val,
                                n->sem.lit_tbl.tuples[row].atoms[col])) {
                            constant = false;
                            break;
                        }

                if (constant)
                    PFprop_mark_const (n->prop, n->schema.items[col].name, val);
            }
            break;

        case la_attach:
            /* attached column is always constant */
            if (!PFprop_const (n->prop, n->sem.attach.res))
                PFprop_mark_const (n->prop,
                                   n->sem.attach.res,
                                   n->sem.attach.value);
            break;

        case la_project:
            /*
             * projection does not affect properties, except for the
             * column name change.
             */
            for (unsigned int i = 0; i < n->sem.proj.count; i++)
                if (PFprop_const (L(n)->prop, n->sem.proj.items[i].old))
                    PFprop_mark_const (n->prop,
                                       n->sem.proj.items[i].new,
                                       PFprop_const_val (
                                           L(n)->prop,
                                           n->sem.proj.items[i].old));
            break;

        case la_select:
            /* the selection criterion itself will now also be const */
            if (!PFprop_const (n->prop, n->sem.select.col))
                PFprop_mark_const (
                        n->prop, n->sem.select.col, PFalg_lit_bln (true));
            break;

        case la_disjunion:
        case la_intersect:
            /*
             * add all columns that are constant in both input relations
             * and additionally both contain the same value
             */
            for (unsigned int i = 0; i < PFprop_const_count (L(n)->prop); i++)
                for (unsigned int j = 0;
                        j < PFprop_const_count (R(n)->prop); j++)
                    if (PFprop_const_at (L(n)->prop, i) ==
                        PFprop_const_at (R(n)->prop, j) &&
                        PFalg_atom_comparable (
                            PFprop_const_val_at (L(n)->prop, i),
                            PFprop_const_val_at (R(n)->prop, j)) &&
                        !PFalg_atom_cmp (
                            PFprop_const_val_at (L(n)->prop, i),
                            PFprop_const_val_at (R(n)->prop, j))) {
                        PFprop_mark_const (
                                n->prop,
                                PFprop_const_at (L(n)->prop, i),
                                PFprop_const_val_at (L(n)->prop, i));
                        break;
                    }
            break;

        case la_semijoin:
        case la_difference:
            /* propagate information from the first input operator */
                for (unsigned int j = 0;
                        j < PFprop_const_count (L(n)->prop); j++)
                    if (!PFprop_const (n->prop,
                                       PFprop_const_at (L(n)->prop, j)))
                        PFprop_mark_const (
                                n->prop,
                                PFprop_const_at (L(n)->prop, j),
                                PFprop_const_val_at (L(n)->prop, j));
            break;
            
        case la_fun_1to1:
            if (n->sem.fun_1to1.kind == alg_fun_num_add) {
                PFalg_col_t col1 = clat (n->sem.fun_1to1.refs, 0),
                            col2 = clat (n->sem.fun_1to1.refs, 1);
                if (PFprop_const (L(n)->prop, col1) &&
                    PFprop_const (L(n)->prop, col2) &&
                    PFprop_type_of (n, n->sem.fun_1to1.res) == aat_int)
                    PFprop_mark_const (
                        n->prop,
                        n->sem.fun_1to1.res,
                        PFalg_lit_int (
                            (PFprop_const_val (L(n)->prop, col1).val.int_ +
                             PFprop_const_val (L(n)->prop, col2).val.int_)));
            }
            break;

        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
            /* if both involved columns are constant and
               we can be sure that the result is the same on all
               plattforms we mark the result as constant and calculate
               its value. (Note: Avoid inferring values that are
               ambiguous e.g. +(dbl, dbl) as the runtime might
               calculate a differing result.) */
            if (PFprop_const (L(n)->prop, n->sem.binary.col1) &&
                PFprop_const (L(n)->prop, n->sem.binary.col2)) {
                PFalg_col_t col1, col2;
                PFalg_simple_type_t ty;

                col1 = n->sem.binary.col1;
                col2 = n->sem.binary.col2;
                ty = 0;

                for (unsigned int i = 0; i < n->schema.count; i++)
                    if (n->schema.items[i].name == n->sem.binary.col1) {
                        ty = n->schema.items[i].type;
                        break;
                    }

                if (n->kind == la_num_eq &&
                    (ty == aat_nat || ty == aat_int ||
                     ty == aat_bln || ty == aat_qname)) {

                    !PFalg_atom_cmp (PFprop_const_val (L(n)->prop, col1),
                                     PFprop_const_val (L(n)->prop, col2))
                    ?
                    PFprop_mark_const (
                        n->prop,
                        n->sem.binary.res,
                        PFalg_lit_bln (true))
                    :
                    PFprop_mark_const (
                        n->prop,
                        n->sem.binary.res,
                        PFalg_lit_bln (false));
                }
                else if (n->kind == la_num_gt &&
                         (ty == aat_nat || ty == aat_int)) {

                    (PFalg_atom_cmp (PFprop_const_val (L(n)->prop, col1),
                                     PFprop_const_val (L(n)->prop, col2))
                    > 0)
                    ?
                    PFprop_mark_const (
                        n->prop,
                        n->sem.binary.res,
                        PFalg_lit_bln (true))
                    :
                    PFprop_mark_const (
                        n->prop,
                        n->sem.binary.res,
                        PFalg_lit_bln (false));
                }
                else if (n->kind == la_bool_and && ty == aat_bln) {
                    PFprop_mark_const (
                        n->prop,
                        n->sem.binary.res,
                        PFalg_lit_bln (
                            (PFprop_const_val (L(n)->prop, col1)).val.bln &&
                            (PFprop_const_val (L(n)->prop, col2)).val.bln));
                }
                else if (n->kind == la_bool_or && ty == aat_bln) {
                    PFprop_mark_const (
                        n->prop,
                        n->sem.binary.res,
                        PFalg_lit_bln (
                            (PFprop_const_val (L(n)->prop, col1)).val.bln ||
                            (PFprop_const_val (L(n)->prop, col2)).val.bln));
                }
            }
            /* if one argument of the and operator has constant value
               false the result will be false as well */
            else if (n->kind == la_bool_and &&
                     PFprop_const (L(n)->prop, n->sem.binary.col1) &&
                     !(PFprop_const_val (L(n)->prop,
                                         n->sem.binary.col1)).val.bln)
                PFprop_mark_const (
                    n->prop,
                    n->sem.binary.res,
                    PFalg_lit_bln (false));
            /* if one argument of the or operator has constant value
               true the result will be true as well */
            else if (n->kind == la_bool_or &&
                     PFprop_const (L(n)->prop, n->sem.binary.col1) &&
                     (PFprop_const_val (L(n)->prop,
                                        n->sem.binary.col1)).val.bln)
                PFprop_mark_const (
                    n->prop,
                    n->sem.binary.res,
                    PFalg_lit_bln (true));
            break;

        case la_bool_not:
            /* if input is constant, output is constant as
               well with the switched value */
            if (PFprop_const (n->prop, n->sem.unary.col))
                PFprop_mark_const (
                    n->prop,
                    n->sem.unary.res,
                    PFalg_lit_bln (!PFprop_const_val (
                                        n->prop,
                                        n->sem.unary.col).val.bln));
            break;

        case la_to:
            if (PFprop_const (L(n)->prop, n->sem.binary.col1) &&
                PFprop_const (L(n)->prop, n->sem.binary.col2) &&
                PFalg_atom_comparable (
                    PFprop_const_val (L(n)->prop, n->sem.binary.col1),
                    PFprop_const_val (L(n)->prop, n->sem.binary.col2)) &&
                !PFalg_atom_cmp (
                    PFprop_const_val (L(n)->prop, n->sem.binary.col1),
                    PFprop_const_val (L(n)->prop, n->sem.binary.col2)))
                PFprop_mark_const (
                    n->prop,
                    n->sem.binary.res,
                    PFprop_const_val (L(n)->prop, n->sem.binary.col1));
            break;

        case la_aggr:
            for (unsigned int i = 0; i < n->sem.aggr.count; i++)
                if (PFprop_const (L(n)->prop, n->sem.aggr.aggr[i].col))
                    switch (n->sem.aggr.aggr[i].kind) {
                        case alg_aggr_dist:
                        case alg_aggr_min:
                        case alg_aggr_max:
                        case alg_aggr_avg:
                        case alg_aggr_all:
                            PFprop_mark_const (
                                    n->prop,
                                    n->sem.aggr.aggr[i].res,
                                    PFprop_const_val (L(n)->prop,
                                                      n->sem.aggr.aggr[i].col));
                            break;

                        default:
                            break;
                    }
            if (n->sem.aggr.part &&
                PFprop_const (L(n)->prop, n->sem.aggr.part))
                PFprop_mark_const (
                        n->prop,
                        n->sem.aggr.part,
                        PFprop_const_val (L(n)->prop, n->sem.aggr.part));
            break;

        case la_cast:
            /* Inference of the constant result columns
               is not possible as a cast is required. */

            /* if the cast does not change the type res equals col */
            if (PFprop_const (L(n)->prop, n->sem.type.col) &&
                (PFprop_const_val (L(n)->prop,
                                   n->sem.type.col)).type == n->sem.type.ty)
                PFprop_mark_const (
                        n->prop,
                        n->sem.type.res,
                        PFprop_const_val (L(n)->prop, n->sem.type.col));
            /* In special cases a stable cast (in respect to different
               implementations) is possible (see e.g. from str to uA). */
            else if (PFprop_const (L(n)->prop, n->sem.type.col) &&
                     (PFprop_const_val (L(n)->prop,
                                        n->sem.type.col)).type == aat_str &&
                     n->sem.type.ty == aat_uA)
                PFprop_mark_const (
                        n->prop,
                        n->sem.type.res,
                        PFalg_lit_uA ((PFprop_const_val (
                                           L(n)->prop,
                                           n->sem.type.col)).val.str));
            /* In special cases a stable cast (in respect to different
               implementations) is possible (see e.g. from int to dbl). */
            else if (PFprop_const (L(n)->prop, n->sem.type.col) &&
                     (PFprop_const_val (L(n)->prop,
                                        n->sem.type.col)).type == aat_int &&
                     n->sem.type.ty == aat_dbl)
                PFprop_mark_const (
                        n->prop,
                        n->sem.type.res,
                        PFalg_lit_dbl ((PFprop_const_val (
                                            L(n)->prop,
                                            n->sem.type.col)).val.int_));
            /* In special cases a stable cast (in respect to different
               implementations) is possible (see e.g. from nat to int). */
            else if (PFprop_const (L(n)->prop, n->sem.type.col) &&
                     (PFprop_const_val (L(n)->prop,
                                        n->sem.type.col)).type == aat_nat &&
                     n->sem.type.ty == aat_int)
                PFprop_mark_const (
                        n->prop,
                        n->sem.type.res,
                        PFalg_lit_int ((PFprop_const_val (
                                            L(n)->prop,
                                            n->sem.type.col)).val.nat_));
            break;

        case la_type:
            for (unsigned int i = 0; i < n->schema.count; i++) {
                if (n->sem.type.col == n->schema.items[i].name) {
                    if (n->sem.type.ty == n->schema.items[i].type)
                        PFprop_mark_const (
                                n->prop,
                                n->sem.type.res,
                                PFalg_lit_bln (true));
                    /* a node type test with only one node kind as input
                       is correct too */
                    else if (n->sem.type.ty == aat_node &&
                             !(~aat_node & n->schema.items[i].type))
                        PFprop_mark_const (
                                n->prop,
                                n->sem.type.res,
                                PFalg_lit_bln (true));
                    else if (!(n->sem.type.ty & n->schema.items[i].type))
                        PFprop_mark_const (
                                n->prop,
                                n->sem.type.res,
                                PFalg_lit_bln (false));
                    break;
                }
            }
            break;

        case la_step:
            if (PFprop_const (R(n)->prop, n->sem.step.iter))
                PFprop_mark_const (
                        n->prop,
                        n->sem.step.iter,
                        PFprop_const_val (R(n)->prop, n->sem.step.iter));
            break;

        case la_guide_step:
            if (PFprop_const (R(n)->prop, n->sem.step.iter))
                PFprop_mark_const (
                        n->prop,
                        n->sem.step.iter,
                        PFprop_const_val (R(n)->prop, n->sem.step.iter));
            break;

        case la_twig:
            switch (L(n)->kind) {
                case la_docnode:
                    if (PFprop_const (L(n)->prop, L(n)->sem.docnode.iter))
                        PFprop_mark_const (
                                n->prop,
                                n->sem.iter_item.iter,
                                PFprop_const_val (L(n)->prop,
                                                  L(n)->sem.docnode.iter));
                    break;

                case la_element:
                case la_textnode:
                case la_comment:
                    if (PFprop_const (L(n)->prop, L(n)->sem.iter_item.iter))
                        PFprop_mark_const (
                                n->prop,
                                n->sem.iter_item.iter,
                                PFprop_const_val (L(n)->prop,
                                                  L(n)->sem.iter_item.iter));
                    break;

                case la_attribute:
                case la_processi:
                    if (PFprop_const (L(n)->prop,
                                      L(n)->sem.iter_item1_item2.iter))
                        PFprop_mark_const (
                                n->prop,
                                n->sem.iter_item.iter,
                                PFprop_const_val (
                                    L(n)->prop,
                                    L(n)->sem.iter_item1_item2.iter));
                    break;

                case la_content:
                    if (PFprop_const (L(n)->prop,
                                      L(n)->sem.iter_pos_item.iter))
                        PFprop_mark_const (
                                n->prop,
                                n->sem.iter_item.iter,
                                PFprop_const_val (
                                    L(n)->prop,
                                    L(n)->sem.iter_pos_item.iter));
                    break;

                default:
                    break;
            }
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

        case la_fun_call:
            /* All the following operators store an iter column
               in the first column which may not escape the domain
               of the LOOP relation and thus stays constant if the
               LOOP relation is constant. */
            if ((n->sem.fun_call.kind == alg_fun_call_xrpc ||
                 n->sem.fun_call.kind == alg_fun_call_pf_documents ||
                 n->sem.fun_call.kind == alg_fun_call_pf_documents_unsafe ||
                 n->sem.fun_call.kind == alg_fun_call_pf_documents_str ||
                 n->sem.fun_call.kind == alg_fun_call_pf_documents_str_unsafe ||
                 n->sem.fun_call.kind == alg_fun_call_pf_collections ||
                 n->sem.fun_call.kind == alg_fun_call_pf_collections_unsafe ||
                 n->sem.fun_call.kind == alg_fun_call_xrpc ||
                 n->sem.fun_call.kind == alg_fun_call_xrpc_helpers ||
                 n->sem.fun_call.kind == alg_fun_call_tijah ||
                 n->sem.fun_call.kind == alg_fun_call_cache) &&
                PFprop_const (L(n)->prop, n->sem.fun_call.iter))
                PFprop_mark_const (
                        n->prop,
                        n->schema.items[0].name,
                        PFprop_const_val (L(n)->prop, n->sem.fun_call.iter));
            break;

        case la_fun_param:
            copy (n->prop->constants, L(n)->prop->constants);
            break;

        case la_proxy:
        case la_proxy_base:
            copy (n->prop->constants, L(n)->prop->constants);
            break;

        case la_string_join:
            if (PFprop_const (L(n)->prop, n->sem.string_join.iter))
                PFprop_mark_const (
                        n->prop,
                        n->sem.string_join.iter_res,
                        PFprop_const_val (L(n)->prop, n->sem.string_join.iter));
            break;


        case la_serialize_seq:
        case la_serialize_rel:
        case la_side_effects:
        case la_empty_tbl:
        case la_ref_tbl:
        case la_cross:
        case la_eqjoin:
        case la_thetajoin:
        case la_pos_select:
        case la_distinct:
        case la_rownum:
        case la_rowrank:
        case la_rank:
        case la_rowid:
        case la_type_assert:
        case la_step_join:
        case la_guide_step_join:
        case la_doc_index_join:
        case la_doc_tbl:
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
        case la_fun_frag_param:
        case la_dummy:
            break;

        case la_internal_op:
            PFoops (OOPS_FATAL,
                    "internal optimization operator is not allowed here");
    }
}

/* worker for PFprop_infer_const */
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

    /* reset constant property
       (reuse already existing lists if already available
        as this increases the performance of the compiler a lot) */
    if (n->prop->constants)
        PFarray_last (n->prop->constants) = 0;
    else
        /* prepare the property for 10 constants */
        n->prop->constants   = PFarray (sizeof (const_t), 10);

    if (L(n)) {
        if (n->prop->l_constants)
            PFarray_last (n->prop->l_constants) = 0;
        else
            /* prepare the property for 10 constants */
            n->prop->l_constants = PFarray (sizeof (const_t), 10);
    }

    if (R(n)) {
        if (n->prop->r_constants)
            PFarray_last (n->prop->r_constants) = 0;
        else
            /* prepare the property for 10 constants */
            n->prop->r_constants = PFarray (sizeof (const_t), 10);
    }

    /* infer information on constant columns */
    infer_const (n);
}

/**
 * Infer constant property for a DAG rooted in root
 */
void
PFprop_infer_const (PFla_op_t *root) {
    prop_infer (root);
    PFla_dag_reset (root);
}

/* vim:set shiftwidth=4 expandtab: */
