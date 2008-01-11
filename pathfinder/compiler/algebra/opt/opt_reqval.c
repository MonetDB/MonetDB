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

#include "algopt.h"
#include "properties.h"
#include "alg_dag.h"
#include "mem.h"          /* PFmalloc() */

/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])
#define LL(p) (L(L(p)))
#define RL(p) (L(R(p)))
#define LR(p) (R(L(p)))
#define RR(p) (R(R(p)))

/* worker for PFalgopt_reqval */
static void
opt_reqvals (PFla_op_t *p)
{
    unsigned int count = 0;
    PFalg_att_t  att = 0;
    bool         val = false;

    assert (p);

    /* nothing to do if we already visited that node */
    if (p->bit_dag)
        return;

    /* Using the required values property as well as the constant
       property we can replace every expression, where at least one
       column has a constant value that differs its required value,
       by an empty table. */
    for (unsigned int i = 0; i < p->schema.count; i++) {
        PFalg_att_t cur_att = p->schema.items[i].name;

        if (PFprop_reqval (p->prop, cur_att) &&
            PFprop_const (p->prop, cur_att) &&
            (PFprop_const_val (p->prop, cur_att)).val.bln !=
             PFprop_reqval_val (p->prop, cur_att)) {
            /* create an empty table instead */
            *p = *PFla_empty_tbl_ (p->schema);
            return;
        }

        if (PFprop_reqval (p->prop, cur_att)) {
            count++;
            att = cur_att;
            val = PFprop_reqval_val (p->prop, cur_att);
        }

    }

    /* if we look for exactly one required value and
       a child of the current operator is a union operator
       that produces two different constant values in
       the required value column in its children then
       we can directly link to the operator that produces
       the required value. */
    if (count == 1) {
        if (p->kind == la_project)
            for (unsigned int i = 0; i < p->sem.proj.count; i++)
                if (att == p->sem.proj.items[i].new) {
                    att = p->sem.proj.items[i].old;
                    break;
                }

        if (L(p) && L(p)->kind == la_disjunion &&
            PFprop_const (LL(p)->prop, att) &&
            PFprop_const (LR(p)->prop, att) &&
            (PFprop_const_val (LL(p)->prop, att)).val.bln !=
            (PFprop_const_val (LR(p)->prop, att)).val.bln) {
            if ((PFprop_const_val (LL(p)->prop, att)).val.bln == val)
                L(p) = LL(p);
            else
                L(p) = LR(p);
        }

        if (R(p) && R(p)->kind == la_disjunion &&
            PFprop_const (RL(p)->prop, att) &&
            PFprop_const (RR(p)->prop, att) &&
            (PFprop_const_val (RL(p)->prop, att)).val.bln !=
            (PFprop_const_val (RR(p)->prop, att)).val.bln) {
            if ((PFprop_const_val (RL(p)->prop, att)).val.bln == val)
                R(p) = RL(p);
            else
                R(p) = RR(p);
        }
    }

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
    /* Infer reqval properties first */
    PFprop_infer_reqval (root);
    /* infer constants to simplify required value analysis */
    PFprop_infer_const (root);

    /* Optimize algebra tree */
    opt_reqvals (root);
    PFla_dag_reset (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
