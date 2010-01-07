/**
 * @file
 *
 * Optimize relational algebra expression DAG
 * based on the required values property.
 * (This requires no burg pattern matching as we
 *  apply optimizations in a peep-hole style on
 *  single nodes only.)
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
#include <stdio.h>
#include "logdebug.h"

#include "algopt.h"
#include "properties.h"
#include "alg_dag.h"
#include "mem.h"          /* PFmalloc() */

/* Easily access subtree-parts */
#include "child_mnemonic.h"

/** mnemonic algebra constructors */
#include "logical_mnemonic.h"

/* indicator to monitor the schema changes */
static bool schema_dirty;

/**
 * Worker for map_fun creating copies for project, not, attach,
 * and union operators.
 */
static PFla_op_t *
plan_copy (PFla_op_t *p)
{
    if (p->kind == la_project ||
        p->kind == la_bool_not ||
        p->kind == la_attach)
        return duplicate (p, plan_copy (L(p)), NULL);
    else if (p->kind == la_disjunion)
        return duplicate (p, plan_copy (L(p)), plan_copy (R(p)));
    else return p;
}

/**
 * Copy parts of the subtree of a select operator
 * to split up references in an if-then-else setting.
 *
 * In case the plan splitting is of no use in opt_reqvals() the
 * following CSE phase will merge splitted nodes again.
 */
static void
map_fun (PFla_op_t *p)
{
    if (p->kind == la_select)
        L(p) = plan_copy (L(p));
}

/* worker for PFalgopt_reqval */
static void
opt_reqvals (PFla_op_t *p)
{
    assert (p);

    /* nothing to do if we already visited that node */
    if (p->bit_dag)
        return;

    /* Using the required values property as well as the constant
       property we can replace every expression, where at least one
       column has a constant value that differs its required value,
       by an empty table. */
    for (unsigned int i = 0; i < p->schema.count; i++) {
        PFalg_col_t cur_col = p->schema.items[i].name;

        if (PFprop_req_bool_val (p->prop, cur_col) &&
            PFprop_const (p->prop, cur_col) &&
            (PFprop_const_val (p->prop, cur_col)).val.bln !=
             PFprop_req_bool_val_val (p->prop, cur_col)) {
            /* create an empty table instead */
            *p = *PFla_empty_tbl_ (p->schema);
            return;
        }
    }

    /* Replace rowrank operators whose real values
       are not needed by rank operators.
       Note that we do not need to check for the order
       constraint (PFprop_req_order_col()) as this rewrite
       does not harm it.  */
    if (p->kind == la_rowrank &&
        PFprop_req_rank_col (p->prop, p->sem.sort.res))
        *p = *rank (L(p), p->sem.sort.res, p->sem.sort.sortby);
    else if (p->kind == la_rowrank &&
             PFprop_req_multi_col_col (p->prop, p->sem.sort.res) &&
             /* single ascending order criterion */
             PFord_count (p->sem.sort.sortby) == 1 &&
             PFord_order_dir_at (p->sem.sort.sortby, 0) == DIR_ASC &&
             /* sort criterion should not stem from a rank operator */
             PFprop_type_of (p, PFord_order_col_at (p->sem.sort.sortby, 0))
             != aat_nat) {
        PFalg_proj_t *proj = PFmalloc (p->schema.count *
                                       sizeof (PFalg_proj_t));

        for (unsigned int i = 0; i < L(p)->schema.count; i++)
            proj[i] = PFalg_proj (L(p)->schema.items[i].name,
                                  L(p)->schema.items[i].name);

        proj[L(p)->schema.count]
            = PFalg_proj (p->sem.sort.res,
                          PFord_order_col_at (p->sem.sort.sortby, 0));

        *p = *PFla_project_ (L(p), p->schema.count, proj);
        /* This rewrite changes the type of the schema column
           which (without further care) triggers problems
           for the following rewrite phases. */
        schema_dirty = true;
    }
    
    /* Replace rownumber operators without partitioning criterion
       that are used for sorting only by rank operators. */
    if (p->kind == la_rownum &&
        !p->sem.sort.part &&
        PFprop_req_order_col (p->prop, p->sem.sort.res)) {
        if (PFord_count (p->sem.sort.sortby) > 0)
            *p = *rank (L(p), p->sem.sort.res, p->sem.sort.sortby);
        else
            *p = *rowid (L(p), p->sem.sort.res);
    }
    
    if (p->kind == la_bool_and &&
        PFprop_req_bool_val (p->prop, p->sem.binary.res) &&
        PFprop_req_bool_val_val (p->prop, p->sem.binary.res)) {
        *p = *PFla_attach (
                  PFla_select (PFla_select (L(p), p->sem.binary.col1),
                               p->sem.binary.col2),
                  p->sem.binary.res, PFalg_lit_bln (true));
    }

    /* if the resulting value of fn:number is only used in a predicate
       we can use the lax variant that ignores NaN values */
    if (p->kind == la_fun_1to1 &&
        p->sem.fun_1to1.kind == alg_fun_fn_number &&
        PFprop_req_filter_col (p->prop, p->sem.fun_1to1.res))
        /* Note: the debug printing does not show this difference */
        p->sem.fun_1to1.kind = alg_fun_fn_number_lax;
    
    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_reqvals (p->child[i]);

    /* mark node as visited */
    p->bit_dag = true;
}

/**
 * Invoke algebra optimization.
 */
PFla_op_t *
PFalgopt_reqval (PFla_op_t *root)
{
    /* Split up plans where two select operators (then + else setting)
       reference the same call to fn:boolean (@col:false U @col:true) */
    PFla_map_fun (root, map_fun);

    /* Infer reqval properties first */
    PFprop_infer_reqval (root);
    /* infer constants to simplify required value analysis */
    PFprop_infer_const (root);

    schema_dirty = false;

    /* Optimize algebra tree */
    opt_reqvals (root);
    PFla_dag_reset (root);

    /* A rewrite changed the type of a column without updating
       the schema of the ancestor operators and we have to clean
       it up. */
    if (schema_dirty)
        PFprop_infer_ocol (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
