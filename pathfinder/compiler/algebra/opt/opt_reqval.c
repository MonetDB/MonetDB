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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
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
        PFalg_att_t att = p->schema.items[i].name;

        if (PFprop_reqval (p->prop, att) && PFprop_const (p->prop, att) &&
            (PFprop_const_val (p->prop, att)).val.bln !=
            PFprop_reqval_val (p->prop, att)) {
            /* create an empty table instead */
            *p = *PFla_empty_tbl_ (p->schema);
            return;
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
