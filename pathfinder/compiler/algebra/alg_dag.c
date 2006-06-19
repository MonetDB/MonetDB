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
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2006 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#include "pathfinder.h"
#include <assert.h>

#include "alg_dag.h"

/* helper function that prepares the DAG bit reset */
static void
prepare_reset (PFla_op_t *n)
{
    assert (n);
    if (n->bit_reset)
        return;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        prepare_reset (n->child[i]);

    n->bit_reset = true;
}

/* helper function that reset the DAG bit
   to allow another DAG traversal */
static void
bit_reset (PFla_op_t *n)
{
    assert (n);
    if (!n->bit_reset)
        return;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        bit_reset (n->child[i]);

    n->bit_reset = false;
    n->bit_dag = false;
}

/*
 * Reset the DAG bit of the logical algebra tree.
 * (it requires a clean reset bit to traverse
 *  the logical tree as DAG.)
 */
void
PFla_dag_reset (PFla_op_t *n) {
    prepare_reset (n);
    bit_reset (n);
}

/* vim:set shiftwidth=4 expandtab: */
