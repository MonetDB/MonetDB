/**
 * @file
 *
 * Functions related to M5 SQL algebra plan construction.
 *
 * This file mainly contains the constructor functions to create an
 * internal representation of the intermediate M5 SQL algebra.
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

/** handling of variable argument lists */
#include <stdarg.h>
/** strcpy, strlen, ... */
#include <string.h>
#include <stdio.h>
/** assert() */
#include <assert.h>

#include "oops.h"
#include "mem.h"
#include "array.h"

#include "msa.h"

#include "msa_mnemonic.h"

/* --------------- Expression nodes --------------- */

/**
 * Create an expression (leaf) node.
 */
static PFmsa_expr_t *
msa_expr_leaf (PFmsa_expr_kind_t kind)
{
    unsigned int i;
    
    PFmsa_expr_t *ret = PFmalloc (sizeof (PFmsa_expr_t));
    
    ret->kind = kind;
    
    for (i = 0; i < PFMSA_EXPR_MAXCHILD; i++)
    {
        ret->child[i] = NULL;
    }
    
    ret->refctr  = 0;
    ret->bit_reset = false;
    ret->bit_dag = false;
    ret->node_id = 0;
    
    ret->sortorder = DIR_ASC;     /* default sortorder ascending */
    
    return ret;
}

/**
 * Create an expression node with one child.
 * Similar to #msa_expr_leaf(), but additionally wires one child.
 */
static PFmsa_expr_t *
msa_expr_wire1 (PFmsa_expr_kind_t kind, const PFmsa_expr_t *n)
{
    PFmsa_expr_t *ret = msa_expr_leaf (kind);
    
    assert (n);
    
    ret->child[0] = (PFmsa_expr_t *) n;
    
    
    return ret;
}

/**
 * Create an expression node with two children.
 * Similar to #msa_expr_wire1(), but additionally wires another child.
 */
static PFmsa_expr_t *
msa_expr_wire2 (PFmsa_expr_kind_t kind, const PFmsa_expr_t *n1, const PFmsa_expr_t *n2)
{
    PFmsa_expr_t *ret = msa_expr_wire1 (kind, n1);
    
    assert (n2);
    
    ret->child[1] = (PFmsa_expr_t *) n2;
    
    return ret;
}

/* --------------- Operator nodes --------------- */

/**
 * Create a logical algebra operator (leaf) node.
 *
 * Allocates memory for an algebra operator leaf node
 * and initializes all its fields. The node will have the
 * kind @a kind.
 */
static PFmsa_op_t *
msa_op_leaf (PFmsa_op_kind_t kind)
{
    PFmsa_op_t *ret = PFmalloc (sizeof (PFmsa_op_t));
    unsigned int i;
    
    ret->kind = kind;
    for (i = 0; i < PFMSA_OP_MAXCHILD; i++)
    {
        ret->child[i] = NULL;
    }
    
    ret->distinct = false;

    ret->refctr  = 0;
    ret->bit_reset = false;
    ret->bit_dag = false;
    ret->node_id = 0;
    
    return ret;
}

/**
 * Create an algebra operator node with one child.
 * Similar to #la_op_leaf(), but additionally wires one child.
 */
static PFmsa_op_t *
msa_op_wire1 (PFmsa_op_kind_t kind, const PFmsa_op_t *n)
{
    PFmsa_op_t *ret = msa_op_leaf (kind);

    assert (n);

    ret->child[0] = (PFmsa_op_t *) n;

    return ret;
}


/**
 * Create an algebra operator node with two children.
 * Similar to #la_op_wire1(), but additionally wires another child.
 */
static PFmsa_op_t *
msa_op_wire2 (PFmsa_op_kind_t kind, const PFmsa_op_t *n1, const PFmsa_op_t *n2)
{
    PFmsa_op_t *ret = msa_op_wire1 (kind, n1);

    assert (n2);

    ret->child[1] = (PFmsa_op_t *) n2;

    return ret;
}

/* -------------------- Auxiliary functions --------------------*/

#ifndef NDEBUG
/* Auxiliary function to determine if all
 columns that are referenced in expr are columns of schema
 */
static bool
expr_cols_in_schema(PFmsa_expr_t *expr, PFalg_schema_t schema)
{
    unsigned int i;
    
    if (expr->kind == msa_expr_column) {
        /* Loop to check if original expression name is in schema */
        
        for (i = 0; i < schema.count; i++) {
            if (expr->sem.col.old == schema.items[i].name)
                return true;
        }
        /* column name not in schema */
        printf("/* a column was found (%s, old: %s), but not the right one */\n", 
               PFcol_str(expr->col), 
               PFcol_str(expr->sem.col.old));
        return false;
    }
    else {
        /* traverse the children expressions */
        for (i = 0; i < PFMSA_EXPR_MAXCHILD && expr->child[i]; i++){
            
            /* propagate missing match */
            if (!expr_cols_in_schema(expr->child[i], schema))
                return false;
        }
        /* no problem detected */
        return true;
    }
}
#endif

/* Auxiliary function to get schema from a list of expressions */
PFalg_schema_t
schema_from_exprlist(PFmsa_exprlist_t *expr_list)
{
    PFalg_schema_t *ret = PFmalloc (sizeof(PFalg_schema_t));
    unsigned int i;
    
    ret->count = elsize(expr_list);
    ret->items = PFmalloc(ret->count * sizeof(*(ret->items)));
    
    /* The new schema consists of columns in expr_list  */
    for (i = 0; i < elsize(expr_list); i++) {
        PFmsa_expr_t *curr_expr = elat(expr_list, i);
        ret->items[i].name = curr_expr->col;
        ret->items[i].type = curr_expr->type;
    }
    
    return *ret;
}

/* Auxiliary function to get schema from an operator node */
static PFalg_schema_t
schema_from_operator(PFmsa_op_t *op)
{
    PFalg_schema_t *ret = PFmalloc (sizeof(PFalg_schema_t));
    unsigned int i;
    
    /* allocate memory for result schema which is the same as the schema of op */
    ret->count = op->schema.count;
    ret->items = PFmalloc(ret->count * sizeof(*(ret->items)));
    
    /* copy schema */
    for (i = 0; i < op->schema.count; i++) {
        ret->items[i] = op->schema.items[i];
    }
    
    return *ret;
}

/* --------------- Constructors for expressions --------------- */

/* Contructor for function expression */
PFmsa_expr_t * 
PFmsa_expr_func (PFmsa_expr_func_name name, PFmsa_exprlist_t *child_list,
                 PFalg_col_t res_col)
{
    PFmsa_expr_t *ret = NULL;
    PFmsa_expr_t *n1 = NULL;
    PFmsa_expr_t *n2 = NULL;
    
    /* Common cases */
    if (elsize(child_list) == 0) 
    {
        ret = msa_expr_leaf (msa_expr_func);
    } 
    else if (elsize(child_list) == 1) 
    {
        n1 = elat(child_list, 0);
        ret = msa_expr_wire1 (msa_expr_func, n1);
    } 
    else if (elsize(child_list) == 2) 
    {
        n1 = elat(child_list, 0);
        n2 = elat(child_list, 1);
        ret = msa_expr_wire2 (msa_expr_func, n1, n2);
    } 
    else if (elsize(child_list) > PFMSA_EXPR_MAXCHILD) 
    {
        /* show an error message */
        PFoops (OOPS_FATAL,
                "MSA expression function: expression cannot have more "
                "than %i children", PFMSA_EXPR_MAXCHILD);
    }
    
    ret->sem.func.name = name;
    
    ret->col = res_col;
    if (n1) ret->type = n1->type;       /* Assuming that child type will be okay */
        else ret->type = aat_uA;
    
    return ret;
}

/* Contructor for number generating function expression */
PFmsa_expr_t * PFmsa_expr_num_gen (PFmsa_num_gen_kind_t kind, PFmsa_exprlist_t *child_list,
                                   PFalg_col_t res_col)
{
    PFmsa_expr_t *ret = NULL;
    PFmsa_expr_t *n1 = NULL;
    PFmsa_expr_t *n2 = NULL;
    
    /* Common cases */
    if (elsize(child_list) == 0) 
    {
        ret = msa_expr_leaf (msa_expr_num_gen);
    } 
    else if (elsize(child_list) == 1) 
    {
        n1 = elat(child_list, 0);
        ret = msa_expr_wire1 (msa_expr_num_gen, n1);
    } 
    else if (elsize(child_list) == 2) 
    {
        n1 = elat(child_list, 0);
        n2 = elat(child_list, 1);
        ret = msa_expr_wire2 (msa_expr_num_gen, n1, n2);
    } 
    else if (elsize(child_list) > PFMSA_EXPR_MAXCHILD) 
    {
        /* FIXME: special cases (expressions with more that 2 children) */
        
        /* but for now, only show an error message */
        PFoops (OOPS_FATAL,
                "MSA number generating expression function: expression cannot have more "
                "than %i children", PFMSA_EXPR_MAXCHILD);
    }
    
    ret->sem.num_gen.kind = kind;
    ret->sem.num_gen.sort_cols = el(1);
    ret->sem.num_gen.part_cols = el(1);
    
    ret->col = res_col;
    if (n1) ret->type = n1->type;       /* Assuming that child type will be okay */
        else ret->type = aat_uA;
            
            return ret;
}

/* Contructor for comparison expression */
PFmsa_expr_t *
PFmsa_expr_comp (PFmsa_expr_t *n1, PFmsa_expr_t *n2, PFmsa_comp_kind_t kind, 
                 PFalg_col_t res_col)
{
    PFmsa_expr_t *ret = msa_expr_wire2 (msa_expr_comp, n1, n2);
    
    ret->sem.comp.kind = kind;
    
    ret->col = res_col;
    ret->type = aat_bln;
    
    return ret;
}

/* Contructor for aggr expression */
PFmsa_expr_t * 
PFmsa_expr_aggr (PFmsa_expr_t *n, PFmsa_aggr_kind_t aggr_kind, 
            PFalg_col_t res_col)
{
    PFmsa_expr_t *ret = msa_expr_wire1 (msa_expr_aggr, n);
    
    ret->sem.aggr.kind = aggr_kind;
    ret->col = res_col;
    ret->type = n->type;         /* Later: Check cases, e.g. PFmsa_agg_kind_t is average, so 
                                  result type is double. Or: PFmsa_agg_kind_t is count, so
                                  result type is integer */
    
    return ret;
}

/* Contructor for convert expression */
PFmsa_expr_t * 
PFmsa_expr_convert (PFmsa_expr_t *n, PFalg_simple_type_t type,
                    PFalg_col_t res_col)
{
    PFmsa_expr_t *ret = msa_expr_wire1 (msa_expr_convert, n);
    
    ret->col = res_col;
    ret->type = type;
    
    return ret;
}

/* Contructor for column expression */
PFmsa_expr_t * 
PFmsa_expr_column (PFalg_col_t col, PFalg_simple_type_t type)
{
    PFmsa_expr_t *ret = msa_expr_leaf (msa_expr_column);
    
    ret->sem.col.old = col;
    ret->col = col;
    ret->type = type;
    
    return ret;
}

/* Contructor for atom expression */
PFmsa_expr_t * 
PFmsa_expr_atom (PFalg_col_t res_col, PFalg_atom_t atom)
{
    PFmsa_expr_t *ret = msa_expr_leaf (msa_expr_atom);
    
    ret->sem.atom.atom = atom;
    ret->col = res_col;
    ret->type = atom.type;    
    
    return ret;
}


/* --------------- Constructors for operators --------------- */

/* Constructor for nil node operator */
PFmsa_op_t * 
PFmsa_op_nil_node ()
{
    PFmsa_op_t *ret = msa_op_leaf(msa_op_nil_node);
    return ret;
}

/* Contructor for serialize operator */
PFmsa_op_t *
PFmsa_op_serialize_rel (PFmsa_op_t *DAG, PFmsa_op_t *side_effects,
                     PFalg_col_t iter, PFalg_col_t pos,
                     PFalg_collist_t *items)
{
    PFmsa_op_t *ret = msa_op_wire2 (msa_op_serialize_rel, side_effects, DAG);
    
    ret->schema = schema_from_operator(DAG);
    
    ret->sem.ser_rel.iter  = iter;
    ret->sem.ser_rel.pos   = pos;
    ret->sem.ser_rel.items = items;
    
    return ret;
}

/* Contructor for project operator */
PFmsa_op_t *
PFmsa_op_project (PFmsa_op_t *n, bool distinct_flag, PFmsa_exprlist_t *expr_list)
{
    PFmsa_op_t *ret = msa_op_wire1 (msa_op_project, n);
#ifndef NDEBUG
    unsigned int i;
#endif
    
    /* Get schema from colum names in expr_list */
    ret->schema = schema_from_exprlist(expr_list);
    
    ret->distinct = distinct_flag;
    ret->sem.proj.expr_list = expr_list;
    
#ifndef NDEBUG
    /* Check if expr_list only contains expressions with columns from n->schema */
    for (i = 0; i < elsize(expr_list); i++) {
        PFmsa_expr_t *curr_expr = elat(expr_list, i);
        
        if (!expr_cols_in_schema (curr_expr, n->schema)) {
            PFoops (OOPS_FATAL,
                    "MSA project: expression references unknown column %s.",
                    PFcol_str (curr_expr->col));
        }
    }
#endif
    
    return ret;
}

/* Contructor for select operator */
PFmsa_op_t *
PFmsa_op_select (PFmsa_op_t *n, PFmsa_exprlist_t *expr_list)
{
    PFmsa_op_t *ret = msa_op_wire1 (msa_op_select, n);
#ifndef NDEBUG
    unsigned int i;
#endif
    
    ret->schema = schema_from_operator(n);
    
    ret->sem.select.expr_list = expr_list;
    
#ifndef NDEBUG
    /* check which type of expressions reside in expr_list */
    for (i = 0; i < elsize(expr_list); i++) {
        PFmsa_expr_t *curr_expr = elat(expr_list, i);
        if (curr_expr->type != aat_bln) {
            PFoops (OOPS_FATAL,
                    "MSA select: only expressions with type boolean can be in conjunction list");
        }
    }
    
    /* Check if expr_list only contains expressions with columns from n->schema */
    for (i = 0; i < elsize(expr_list); i++) {
        PFmsa_expr_t *curr_expr = elat(expr_list, i);
        
        if (!expr_cols_in_schema (curr_expr, n->schema)) {
            PFoops (OOPS_FATAL,
                    "MSA select: expression references unknown column %s.",
                    PFcol_str (curr_expr->col));
        }
    }
#endif
    
    return ret;
}

/* Contructor for literal table operator */
PFmsa_op_t *
PFmsa_op_literal_table (PFalg_schema_t names,
                     unsigned int tuples_count, PFalg_tuple_t *tuples)
{
    PFmsa_op_t *ret = msa_op_leaf (msa_op_literal_table);
    unsigned int i;  
    
    /* copy the passed schema */
    ret->schema.count = names.count;
    ret->schema.items = PFmalloc (ret->schema.count * sizeof(*(ret->schema.items)));
    
    for (i = 0; i < names.count; i++) {
        ret->schema.items[i] = names.items[i];
    }
    
    ret->sem.literal_table.tuples_count = tuples_count;
    ret->sem.literal_table.tuples = tuples;
    
#ifndef NDEBUG
    /* check if all tuples have same schema as the given schema "names" */
    for (i = 0; i < tuples_count; i++) {
        /* same number of columns? */
        if (names.count != tuples[i].count) {
            PFoops (OOPS_FATAL,
                    "MSA literal table: tuple list contains tuple with too few/many values");
        }
    }
#endif
    
    return ret;
}

/* Contructor for table operator */
PFmsa_op_t *
PFmsa_op_table (char* name, PFalg_schema_t schema, PFmsa_exprlist_t *expr_list,
                PFmsa_exprlist_t *col_names)
{
    assert(name);
    
    PFmsa_op_t *ret = msa_op_leaf (msa_op_table);
    
    ret->schema = schema;
    
    ret->sem.table.name = name;
    ret->sem.table.expr_list = expr_list;
    ret->sem.table.col_names = col_names;
    
    return ret;
}

/* Contructor for union operator */
PFmsa_op_t *
PFmsa_op_union (PFmsa_op_t *n1, PFmsa_op_t *n2)
{
    PFmsa_op_t *ret = msa_op_wire2 (msa_op_union, n1, n2);
#ifndef NDEBUG
    unsigned int i;
#endif
    
    ret->schema = schema_from_operator(n1);
    
#ifndef NDEBUG
    /* union requires schemata of same size */
    if (n1->schema.count != n2->schema.count) {
        PFoops (OOPS_FATAL,
                "MSA union: column counts do not match (%i and %i)",
                n1->schema.count, n2->schema.count);
    }
    
    /* union requires schemata of same names */
    for (i = 0; i < n1->schema.count; i++) {
        if (n1->schema.items[i].name != n2->schema.items[i].name) {
            PFoops (OOPS_FATAL,
                "MSA union: incompatible columns %s and %s",
                    PFcol_str(n1->schema.items[i].name),
                    PFcol_str(n2->schema.items[i].name));
        }
    }
#endif
    
    return ret;
}

/* Contructor for except operator */
PFmsa_op_t *
PFmsa_op_except (PFmsa_op_t *n1, PFmsa_op_t *n2)
{
    PFmsa_op_t *ret = msa_op_wire2 (msa_op_except, n1, n2);
#ifndef NDEBUG
    unsigned int i;
#endif
    
    ret->schema = schema_from_operator(n1);
    
#ifndef NDEBUG
    /* except requires schemata of same size */
    if (n1->schema.count != n2->schema.count) {
        PFoops (OOPS_FATAL,
                "MSA except: column counts do not match (%i and %i)",
                n1->schema.count, n2->schema.count);
    }
    
    /* except requires schemata of same names */
    for (i = 0; i < n1->schema.count; i++) {
        if (n1->schema.items[i].name != n2->schema.items[i].name) {
            PFoops (OOPS_FATAL,
                    "MSA except: incopatible columns %s and %s",
                    PFcol_str(n1->schema.items[i].name),
                    PFcol_str(n2->schema.items[i].name));
        }
    }
#endif
    
    return ret;
}

/* Contructor for groupby operator */
PFmsa_op_t *
PFmsa_op_groupby (PFmsa_op_t *n, PFmsa_exprlist_t *grp_list, PFmsa_exprlist_t *prj_list)
{
    PFmsa_op_t *ret = msa_op_wire1 (msa_op_groupby, n);
#ifndef NDEBUG
    unsigned int i;
    unsigned int j;
#endif
    
    /* Get schema from colum names in prj_list */
    ret->schema = schema_from_exprlist(prj_list);
    
    ret->sem.groupby.grp_list = grp_list;
    ret->sem.groupby.prj_list = prj_list;
    
#ifndef NDEBUG
    /* check if every column in prj_list is also in grp_list*/
    for (i = 0; i < elsize(prj_list); i++) {
        PFmsa_expr_t *curr_prj = elat(prj_list, i);
        
        if (curr_prj->kind == msa_expr_aggr) {
            /* Aggregate functions do not need a correspondent in grp_list */
            break;
        }
        
        for (j = 0; j < elsize(grp_list); j++) {
            PFmsa_expr_t *curr_grp = elat(grp_list, j);
            if (curr_prj->col == curr_grp->col) {
                break;
            }
        }
        
        if (j >= elsize(grp_list)) {
            /* Error, projection list contains column that is non group by column */
            PFoops (OOPS_FATAL,
                    "MSA group by: cannot project on non-groupby column %s ",
                    PFcol_str(curr_prj->col));
        }
        
    }
    
    /* Check if prj_list only contains expressions with columns from ret->schema */
    for (i = 0; i < elsize(prj_list); i++) {
        PFmsa_expr_t *curr_prj = elat(prj_list, i);
        
        if (!expr_cols_in_schema (curr_prj, n->schema)) {
            PFoops (OOPS_FATAL,
                    "MSA group by: expression references unknown column %s.",
                    PFcol_str (curr_prj->col));
        }
    }
#endif
    
    return ret;
}

/* Contructor for join operator */
PFmsa_op_t *
PFmsa_op_join (PFmsa_op_t *n1, PFmsa_op_t *n2, PFmsa_exprlist_t *expr_list)
{
    PFmsa_op_t *ret = msa_op_wire2 (msa_op_join, n1, n2);
    unsigned int i;
    unsigned int j;
    
    ret->schema.count = n1->schema.count + n2->schema.count;
    ret->schema.items = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));
    
    /* copy schema */
    for (i = 0; i < n1->schema.count; i++) {
        ret->schema.items[i] = n1->schema.items[i];
    }
    
    for (j = 0; j < n2->schema.count; j++) {
        ret->schema.items[i + j] = n2->schema.items[j];
    }
    
    ret->sem.join.expr_list = expr_list;

#ifndef NDEBUG
    /* check which type of expressions reside in expr_list */
    for (i = 0; i < elsize(expr_list); i++) {
        PFmsa_expr_t *curr_expr = elat(expr_list, i);
        if (curr_expr->type != aat_bln) {
            /* Error: Only booleans can be in conjunction list */
            PFoops (OOPS_FATAL,
                    "MSA join: only booleans can be in conjunction list");
        }
    }
    
    /* Check if expr_list only contains expressions with columns from ret->schema.
     Use of ret->schema because expr_list can contain columns from both children
     and ret->schema represents all these columns */
    for (i = 0; i < elsize(expr_list); i++) {
        PFmsa_expr_t *curr_expr = elat(expr_list, i);
        
        if (!expr_cols_in_schema (curr_expr, ret->schema)) {
            PFoops (OOPS_FATAL,
                    "MSA join: expression references unknown column %s.",
                    PFcol_str (curr_expr->col));
            
        }
    }
#endif
    
    return ret;
}

/* Contructor for semijoin operator */
PFmsa_op_t *
PFmsa_op_semijoin (PFmsa_op_t *n1, PFmsa_op_t *n2, PFmsa_exprlist_t *expr_list)
{
    PFmsa_op_t *ret = msa_op_wire2 (msa_op_semijoin, n1, n2);
#ifndef NDEBUG
    unsigned int i;
#endif

    ret->schema = schema_from_operator(n1);
    
    ret->sem.join.expr_list = expr_list;
    
#ifndef NDEBUG
    /* check which type of expressions reside in expr_list */
    for (i = 0; i < elsize(expr_list); i++) {
        PFmsa_expr_t *curr_expr = elat(expr_list, i);
        if (curr_expr->type != aat_bln) {
            /* Error: Only booleans can be in conjunction list */
            PFoops (OOPS_FATAL,
                    "MSA semijoin: only booleans can be in conjunction list");
        }
    }
    
    /* Check if expr_list only contains expressions with columns from n1->schema
      or n2->schema. */
    for (i = 0; i < elsize(expr_list); i++) {
        PFmsa_expr_t *curr_expr = elat(expr_list, i);
        
        if (!expr_cols_in_schema (curr_expr, n1->schema) &&
            !expr_cols_in_schema (curr_expr, n2->schema)) {
            PFoops (OOPS_FATAL,
                    "MSA semijoin: expression references unknown column %s.",
                    PFcol_str (curr_expr->col));
            
        }
    }
#endif
    
    return ret;
}

/* Contructor for cross operator */
PFmsa_op_t *
PFmsa_op_cross (PFmsa_op_t *n1, PFmsa_op_t *n2)
{
    PFmsa_op_t *ret = msa_op_wire2 (msa_op_cross, n1, n2);
    unsigned int i;
    unsigned int j;
    
    ret->schema.count = n1->schema.count + n2->schema.count;
    ret->schema.items = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));
    
    /* copy schema */
    for (i = 0; i < n1->schema.count; i++) {
        ret->schema.items[i] = n1->schema.items[i];
    }
    
    for (j = 0; j < n2->schema.count; j++) {
        ret->schema.items[i + j] = n2->schema.items[j];
    }
    
    return ret;
}

/* --------------- Set distinct flag in operator --------------- */
PFmsa_op_t *
PFmsa_distinct(PFmsa_op_t *op)
{
    PFmsa_op_t *ret = PFmalloc(sizeof(PFmsa_op_t));
    memcpy(ret, op, sizeof(PFmsa_op_t));
    
    ret->distinct = true;
    return ret;
}

/* vim:set shiftwidth=4 expandtab: */
