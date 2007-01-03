/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Access and helper functions for abstract syntax tree
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
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#include "pathfinder.h"

#include <string.h>

#include "abssyn.h"

#include "mem.h"

/** 
 * Allocates a new parse tree leaf and initializes 
 * its type.
 *
 * @param  kind kind of new leaf
 * @param  loc  source location of new node
 * @return new parse tree lead
 */
PFpnode_t * 
p_leaf (PFptype_t kind, PFloc_t loc)
{
    PFpnode_t *n;
    int c;

    n = (PFpnode_t *) PFmalloc (sizeof (PFpnode_t));

    for (c = 0; c < PFPNODE_MAXCHILD; c++)
        n->child[c] = 0;
    n->kind = kind;
    n->loc = loc;
    n->state_label = 0;

    /* core code for this node will be assigned by the
     * XQuery Formal Semantics phase
     */
    n->core = 0;

    return n;
}

/**
 * Allocates and initializes a new parse tree node @a n with given @a type,
 * then wires single child node @a n1.
 *
 * @param  type type of new node
 * @param  loc  source location of new node
 * @param  n1   pointer to child
 * @return new parse tree node
 */
PFpnode_t *
p_wire1 (PFptype_t type, PFloc_t loc, PFpnode_t *n1) 
{
    PFpnode_t *n;

    n = p_leaf (type, loc);
    n->child[0] = n1;

    return n;
}

/**
 * Allocates and initializes a new parse tree node @a n with given @a type,
 * then wires two child nodes @a n1 and @a n2.
 *
 * @param  type type of new node
 * @param  loc  source location of new node
 * @param  n1   pointer to first child
 * @param  n2   pointer to second child
 * @return new parse tree node
 */
PFpnode_t *
p_wire2 (PFptype_t type, PFloc_t loc, PFpnode_t *n1, PFpnode_t *n2) 
{
    PFpnode_t *n;

    n = p_wire1 (type, loc, n1);
    n->child[1] = n2;

    return n;
} 


/* vim:set shiftwidth=4 expandtab: */
