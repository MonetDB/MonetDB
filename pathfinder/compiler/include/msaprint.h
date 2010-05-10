/**
 * @file
 *
 * Convert the internal representation of the M5 SQL algebra into
 * the AT&T dot format.
 *
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
 * $Id: $
 */

#ifndef MSAPRINT_H
#define MSAPRINT_H

/* FILE, ... */
#include <stdio.h>

#include "msa.h"
#include "plan_bundle.h"

void PFmsa_dot (FILE *, PFmsa_op_t *);


/* FIXME: NOT TESTED YET - DRAFT OF NEW DAG-TRAVERSAL */

/* ------------------------------------------------------------------------------------ */
/* Functions to traverse the DAG */

static unsigned int
traverse_op (PFmsa_op_t *n, unsigned int node_id,
             unsigned int (*g) (PFmsa_op_t *, unsigned int));
static unsigned int
f2_prepare_reset_op(PFmsa_op_t *n, unsigned int node_id);
static unsigned int
g2_prepare_reset_expr(PFmsa_expr_t *n, unsigned int node_id);
static unsigned int
f1_reset_op (PFmsa_op_t *n, unsigned int node_id);
static unsigned int
g1_reset_expr (PFmsa_expr_t *n, unsigned int node_id);
static unsigned int
f3_create_node_id_op (PFmsa_op_t *n, unsigned int node_id);
static unsigned int
g3_create_node_id_expr (PFmsa_expr_t *n, unsigned int node_id);



static unsigned int
traverse_op (PFmsa_op_t *n, unsigned int node_id,
             unsigned int (*f) (PFmsa_op_t *, unsigned int))
{
    unsigned int i;
    
    /* do something with g */
    node_id = f(n, node_id);
    
    for (i = 0; i < PFMSA_OP_MAXCHILD && n->child[i]; i++)
        traverse_op (n->child[i], node_id, f);
    
    return node_id;
}

static unsigned int
traverse_expr (PFmsa_expr_t *n, unsigned int node_id,
               unsigned int (*f) (PFmsa_expr_t *, unsigned int))
{
    unsigned int i;
    
    /* do something with g */
    node_id = f(n, node_id);
    
    for (i = 0; i < PFMSA_EXPR_MAXCHILD && n->child[i]; i++)
        traverse_expr (n->child[i], node_id, f);
    
    return node_id;
}

static unsigned int
traverse_expr_list (PFmsa_exprlist_t *list, unsigned int node_id,
                    unsigned int (*f) (PFmsa_expr_t *, unsigned int))
{
    unsigned int i;
    PFmsa_expr_t *curr_expr;
    
    for (i = 0; i < elsize(list); i++) {
        curr_expr = elat(list, i);
        node_id = traverse_expr(curr_expr, node_id, f);
    }
    
    return node_id;
}

/* ------------------------------------------------------------------------------------ */
/* Functions to perform the work */

/* helper function that prepares the
 DAG bit reset for expression nodes */
static unsigned int
f2_prepare_reset_op(PFmsa_op_t *n, unsigned int node_id)
{
    assert (n);
    
    /* no node_id used while preparing reset of bit_dag */
    (void) node_id;
    
    if (n->bit_reset)
        return 0;
    
    /* In some operators, exressions have to be traversed as well */
    switch (n->kind) {
        case msa_op_project:
            traverse_expr_list(n->sem.proj.expr_list, 0, g2_prepare_reset_expr);
            break;
        case msa_op_select:
            traverse_expr_list(n->sem.select.expr_list, 0, g2_prepare_reset_expr);
            break;
        case msa_op_table:
            traverse_expr_list(n->sem.table.expr_list, 0, g2_prepare_reset_expr);
            traverse_expr_list(n->sem.table.col_names, 0, g2_prepare_reset_expr);
            break;
        case msa_op_groupby:
            traverse_expr_list(n->sem.groupby.grp_list, 0, g2_prepare_reset_expr);
            traverse_expr_list(n->sem.groupby.prj_list, 0, g2_prepare_reset_expr);
            break;
        case msa_op_join:
        case msa_op_semijoin:
            traverse_expr_list(n->sem.join.expr_list, 0, g2_prepare_reset_expr);
            break;
        default:
            break;
    }
    
    n->bit_reset = true;
    return 0;
}

/* helper function that prepares the
 DAG bit reset for expression nodes */
static unsigned int
g2_prepare_reset_expr(PFmsa_expr_t *n, unsigned int node_id)
{
    assert (n);
    
    /* no node_id used while preparing reset of bit_dag */
    (void) node_id;
    
    if (n->bit_reset)
        return 0;
    
    /* In some expressions, exression lists have to be traversed as well */
    switch (n->kind) {
        case msa_expr_num_gen:
            traverse_expr_list(n->sem.num_gen.sort_cols, 0, g2_prepare_reset_expr);
            traverse_expr_list(n->sem.num_gen.part_cols, 0, g2_prepare_reset_expr);
            break;
        default:
            break;
    }
    
    n->bit_reset = true;
    return 0;
}

/* helper function that reset the DAG bit
 to allow another DAG traversal */
static unsigned int
f1_reset_op (PFmsa_op_t *n, unsigned int node_id)
{
    assert (n);
    
    /* Suppress warnings */
    PFmsa_op_t *root = NULL;
    (void) traverse_op(root, 0, f2_prepare_reset_op);
    (void) traverse_op(root, 0, f1_reset_op);
    (void) traverse_op (n, 1, f3_create_node_id_op);
    
    /* no node_id used while resetting bit_dag */
    (void) node_id;
    
    if (!n->bit_reset)
        return 0;;
    
    /* In some operators, exressions have to be traversed as well */
    switch (n->kind) {
        case msa_op_project:
            traverse_expr_list(n->sem.proj.expr_list, 0, g1_reset_expr);
            break;
        case msa_op_select:
            traverse_expr_list(n->sem.select.expr_list, 0, g1_reset_expr);
            break;
        case msa_op_table:
            traverse_expr_list(n->sem.table.expr_list, 0, g1_reset_expr);
            traverse_expr_list(n->sem.table.col_names, 0, g1_reset_expr);
            break;
        case msa_op_groupby:
            traverse_expr_list(n->sem.groupby.grp_list, 0, g1_reset_expr);
            traverse_expr_list(n->sem.groupby.prj_list, 0, g1_reset_expr);
            break;
        case msa_op_join:
        case msa_op_semijoin:
            traverse_expr_list(n->sem.join.expr_list, 0, g1_reset_expr);
            break;
        default:
            break;
    }
    
    n->bit_reset = false;
    n->bit_dag = false;
    return 0;
}

/* helper function to reset the DAG bit in expressions */
static unsigned int
g1_reset_expr (PFmsa_expr_t *n, unsigned int node_id)
{
    assert (n);
    
    /* no node_id used while resetting bit_dag */
    (void) node_id;
    
    if (!n->bit_reset)
        return 0;
    
    /* In some expressions, exression lists have to be traversed as well */
    switch (n->kind) {
        case msa_expr_num_gen:
            traverse_expr_list(n->sem.num_gen.sort_cols, 0, g1_reset_expr);
            traverse_expr_list(n->sem.num_gen.part_cols, 0, g1_reset_expr);
            break;
        default:
            break;
    }
    
    n->bit_reset = false;
    n->bit_dag = false;
    return 0;
}

static unsigned int
f3_create_node_id_op (PFmsa_op_t *n, unsigned int node_id)
{
    if (n->bit_dag)
        return node_id;
    else
        n->bit_dag = true;
    
    n->node_id = node_id++;
    
    /* If op has expression list, set node id for expressions as well */
    switch (n->kind) {
        case msa_op_project:
            node_id = traverse_expr_list(n->sem.proj.expr_list, node_id, g3_create_node_id_expr);
            break;
        case msa_op_select:
            node_id = traverse_expr_list(n->sem.select.expr_list, node_id, g3_create_node_id_expr);
            break;
        case msa_op_table:
            node_id = traverse_expr_list(n->sem.table.expr_list, node_id, g3_create_node_id_expr);
            node_id = traverse_expr_list(n->sem.table.col_names, node_id, g3_create_node_id_expr);
            break;
        case msa_op_groupby:
            node_id = traverse_expr_list(n->sem.groupby.grp_list, node_id, g3_create_node_id_expr);
            node_id = traverse_expr_list(n->sem.groupby.prj_list, node_id, g3_create_node_id_expr);
            break;
        case msa_op_join:
        case msa_op_semijoin:
            node_id = traverse_expr_list(n->sem.join.expr_list, node_id, g3_create_node_id_expr);
            break;
        default:
            break;
    }
    
    return node_id;
}

static unsigned int
g3_create_node_id_expr (PFmsa_expr_t *n, unsigned int node_id)
{
    if (n->bit_dag)
        return node_id;
    else
        n->bit_dag = true;
    
    n->node_id = node_id++;
    
    /* If expression has expression list, set node id for expressions as well */
    switch (n->kind) {
        case msa_expr_num_gen:
            node_id = traverse_expr_list(n->sem.num_gen.sort_cols, node_id, g3_create_node_id_expr);
            node_id = traverse_expr_list(n->sem.num_gen.part_cols, node_id, g3_create_node_id_expr);
            break;
        default:
            break;
    }
    
    return node_id;
}

#endif /* MSAPRINT_H */

/* vim:set shiftwidth=4 expandtab: */
