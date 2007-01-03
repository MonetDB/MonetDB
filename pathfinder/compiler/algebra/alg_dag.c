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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#include "pathfinder.h"
#include <assert.h>

#include "alg_dag.h"

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

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        pa_dag_bit_reset (n->child[i]);

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
