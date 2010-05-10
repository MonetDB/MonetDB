/**
 * @file
 *
 * Functions related to algebra tree optimization.
 * (Generic stuff for logical algebra.)
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

#include "pf_config.h"
#include "pathfinder.h"
#include <assert.h>

#include "alg_dag.h"

/* !!! */
/** abbreviation for expression list constructor */
#define el(s)       PFmsa_exprlist((s))
/** abbreviation for expression list accessors */
#define elat(el,i)  PFmsa_exprlist_at((el),(i))
#define eltop(el)   PFmsa_exprlist_top((el))
#define eladd(el)   PFmsa_exprlist_add((el))
#define elsize(el)  PFmsa_exprlist_size((el))

/* helper function that prepares the DAG bit reset */
static void
la_prepare_reset (PFla_op_t *n)
{
    assert (n);
    if (n->bit_reset)
        return;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        la_prepare_reset (n->child[i]);

    n->bit_reset = true;
}

/* helper function that prepares the DAG bit reset */
static void
pa_prepare_reset (PFpa_op_t *n)
{
    assert (n);
    if (n->bit_reset)
        return;

    for (unsigned int i = 0; i < PFPA_OP_MAXCHILD && n->child[i]; i++)
        pa_prepare_reset (n->child[i]);

    n->bit_reset = true;
}

/* stub declaration */
static void
msa_prepare_reset_exprs_in_list(PFmsa_exprlist_t *list);

/* helper function that prepares the
   DAG bit reset for expression nodes */
static void
msa_prepare_reset_expr(PFmsa_expr_t *n)
{
    assert (n);
    
    if (n->bit_reset)
        return;
    
    /* In some expressions, exression lists have to be traversed as well */
    switch (n->kind) {
        case msa_expr_num_gen:
            msa_prepare_reset_exprs_in_list(n->sem.num_gen.sort_cols);
            msa_prepare_reset_exprs_in_list(n->sem.num_gen.part_cols);
            break;
        default:
            break;
    }
    
    for (unsigned int i = 0; i < PFMSA_EXPR_MAXCHILD && n->child[i]; i++)
        msa_prepare_reset_expr (n->child[i]);
    
    n->bit_reset = true;
}

/* helper function prepares the DAG bit reset
   in expressions in a expression list */
static void
msa_prepare_reset_exprs_in_list(PFmsa_exprlist_t *list)
{
    unsigned int i;
    for (i = 0; i < elsize(list); i++) {
        PFmsa_expr_t *curr_expr = elat(list, i);
        msa_prepare_reset_expr(curr_expr);
    }
}

/* helper function that prepares the DAG bit reset */
static void
msa_prepare_reset (PFmsa_op_t *n)
{
    assert (n);
    
    if (n->bit_reset)
        return;

    /* In some operators, exressions have to be traversed as well */
    switch (n->kind) {
        case msa_op_project:
            msa_prepare_reset_exprs_in_list(n->sem.proj.expr_list);
            break;
        case msa_op_select:
            msa_prepare_reset_exprs_in_list(n->sem.select.expr_list);
            break;
        case msa_op_table:
            msa_prepare_reset_exprs_in_list(n->sem.table.expr_list);
            msa_prepare_reset_exprs_in_list(n->sem.table.col_names);
            break;
        case msa_op_groupby:
            msa_prepare_reset_exprs_in_list(n->sem.groupby.grp_list);
            msa_prepare_reset_exprs_in_list(n->sem.groupby.prj_list);
            break;
        case msa_op_join:
        case msa_op_semijoin:
            msa_prepare_reset_exprs_in_list(n->sem.join.expr_list);
            break;
        default:
            break;
    }
    
    for (unsigned int i = 0; i < PFMSA_OP_MAXCHILD && n->child[i]; i++)
        msa_prepare_reset (n->child[i]);

    n->bit_reset = true;
}

/* helper function that reset the DAG bit
   to allow another DAG traversal */
static void
la_dag_bit_reset (PFla_op_t *n)
{
    assert (n);
    if (!n->bit_reset)
        return;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        la_dag_bit_reset (n->child[i]);

    n->bit_reset = false;
    n->bit_dag = false;
}

/* helper function that reset the DAG bit
   to allow another DAG traversal */
static void
pa_dag_bit_reset (PFpa_op_t *n)
{
    assert (n);
    if (!n->bit_reset)
        return;

    for (unsigned int i = 0; i < PFPA_OP_MAXCHILD && n->child[i]; i++)
        pa_dag_bit_reset (n->child[i]);

    n->bit_reset = false;
    n->bit_dag = false;
}

/* stub declaration */
static void
msa_dag_bit_reset_exprs_in_list (PFmsa_exprlist_t *list);

/* helper function to reset the DAG bit in expressions */
static void
msa_dag_bit_reset_expr (PFmsa_expr_t *n)
{
    assert (n);
    if (!n->bit_reset)
        return;
    
    /* In some expressions, exression lists have to be traversed as well */
    switch (n->kind) {
        case msa_expr_num_gen:
            msa_dag_bit_reset_exprs_in_list(n->sem.num_gen.sort_cols);
            msa_dag_bit_reset_exprs_in_list(n->sem.num_gen.part_cols);
            break;
        default:
            break;
    }
    
    for (unsigned int i = 0; i < PFMSA_EXPR_MAXCHILD && n->child[i]; i++)
        msa_dag_bit_reset_expr (n->child[i]);
    
    n->bit_reset = false;
    n->bit_dag = false;
}

/* helper function to reset all expressions in expression list */
static void
msa_dag_bit_reset_exprs_in_list (PFmsa_exprlist_t *list)
{
    unsigned int i;
    for (i = 0; i < elsize(list); i++) {
        PFmsa_expr_t *curr_expr = elat(list, i);
        msa_dag_bit_reset_expr(curr_expr);
    }
}

/* helper function that reset the DAG bit
   to allow another DAG traversal */
static void
msa_dag_bit_reset (PFmsa_op_t *n)
{
    assert (n);
    if (!n->bit_reset)
        return;

    for (unsigned int i = 0; i < PFMSA_OP_MAXCHILD && n->child[i]; i++)
        msa_dag_bit_reset (n->child[i]);
    
    /* In some operators, exressions have to be traversed as well */
    switch (n->kind) {
        case msa_op_project:
            msa_dag_bit_reset_exprs_in_list(n->sem.proj.expr_list);
            break;
        case msa_op_select:
            msa_dag_bit_reset_exprs_in_list(n->sem.select.expr_list);
            break;
        case msa_op_table:
            msa_dag_bit_reset_exprs_in_list(n->sem.table.expr_list);
            msa_dag_bit_reset_exprs_in_list(n->sem.table.col_names);
            break;
        case msa_op_groupby:
            msa_dag_bit_reset_exprs_in_list(n->sem.groupby.grp_list);
            msa_dag_bit_reset_exprs_in_list(n->sem.groupby.prj_list);
            break;
        case msa_op_join:
        case msa_op_semijoin:
            msa_dag_bit_reset_exprs_in_list(n->sem.join.expr_list);
            break;
        default:
            break;
    }
    
    n->bit_reset = false;
    n->bit_dag = false;
}

/* helper function that reset the IN bit
   to allow another proxy search */
static void
in_out_bit_reset (PFla_op_t *n)
{
    assert (n);
    if (!n->bit_reset)
        return;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        in_out_bit_reset (n->child[i]);

    n->bit_reset = false;
    n->bit_in = false;
    n->bit_out = false;
}

/* helper function that resets only the IN bit */
static void
in_bit_reset (PFla_op_t *n)
{
    assert (n);
    if (!n->bit_reset)
        return;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        in_bit_reset (n->child[i]);

    n->bit_reset = false;
    n->bit_in = false;
}

/* helper function that resets only the OUT bit */
static void
out_bit_reset (PFla_op_t *n)
{
    assert (n);
    if (!n->bit_reset)
        return;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        out_bit_reset (n->child[i]);

    n->bit_reset = false;
    n->bit_out = false;
}

/*
 * Mark the DAG bit of the logical algebra tree.
 * (it requires a clean dag bit to traverse
 *  the logical tree as DAG.)
 */
void
PFla_dag_mark (PFla_op_t *n)
{
    assert (n);
    if (n->bit_dag)
        return;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        PFla_dag_mark (n->child[i]);

    n->bit_dag = true;
}

/*
 * Reset the DAG bit of the logical algebra tree.
 * (it requires a clean reset bit to traverse
 *  the logical tree as DAG.)
 */
void
PFla_dag_reset (PFla_op_t *n) {
    la_prepare_reset (n);
    la_dag_bit_reset (n);
}

/*
 * Reset the DAG bit of the physical algebra tree.
 * (it requires a clean reset bit to traverse
 *  the physical tree as DAG.)
 */
void
PFpa_dag_reset (PFpa_op_t *n) {
    pa_prepare_reset (n);
    pa_dag_bit_reset (n);
}

/*
 * Reset the DAG bit of the SQL algebra tree.
 * (it requires a clean reset bit to traverse
 *  the SQL algebra tree as DAG.)
 */
void
PFmsa_dag_reset (PFmsa_op_t *n) {
    msa_prepare_reset (n);
    msa_dag_bit_reset (n);
}

/*
 * Reset the IN and OUT bits of the logical algebra tree.
 * (it requires a clean reset bit to traverse
 *  the logical tree as DAG.)
 */
void
PFla_in_out_reset (PFla_op_t *n) {
    la_prepare_reset (n);
    in_out_bit_reset (n);
}

/*
 * Reset the IN bit of the logical algebra tree.
 * (it requires a clean reset bit to traverse
 *  the logical tree as DAG.)
 */
void
PFla_in_reset (PFla_op_t *n) {
    la_prepare_reset (n);
    in_bit_reset (n);
}

/*
 * Reset the OUT bit of the logical algebra tree.
 * (it requires a clean reset bit to traverse
 *  the logical tree as DAG.)
 */
void
PFla_out_reset (PFla_op_t *n) {
    la_prepare_reset (n);
    out_bit_reset (n);
}

/* vim:set shiftwidth=4 expandtab: */
