/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Access and helper functions for abstract syntax tree
 *
 * $Id$
 */

#include <string.h>

#include "abssyn.h"


/** 
 * Allocates a new parse tree leaf and initializes 
 * its type.
 *
 * @param  kind kind of new leaf
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

/**
 * Allocates and initializes a new parse tree node @a n with given @a type,
 * then wires three child nodes @a n1, @a n2, and @a n3.
 *
 * @param  type type of new node
 * @param  loc  source location of new node
 * @param  n1   pointer to first child
 * @param  n2   pointer to second child
 * @param  n3   pointer to third child
 * @return new parse tree node
 */
PFpnode_t *
p_wire3 (PFptype_t type, PFloc_t loc,
         PFpnode_t *n1, PFpnode_t *n2, PFpnode_t *n3) 
{
    PFpnode_t *n;

    n = p_wire2 (type, loc, n1, n2);
    n->child[2] = n3;

    return n;
} 

/**
 * Allocates and initializes a new parse tree node @a n with given @a type,
 * then wires four child nodes @a n1, @a n2, @a n3, and @a n4.
 *
 * @param  type type of new node
 * @param  loc  source location of new node
 * @param  n1   pointer to first child
 * @param  n2   pointer to second child
 * @param  n3   pointer to third child
 * @param  n4   pointer to fourth child
 * @return new parse tree node
 */
PFpnode_t *
p_wire4 (PFptype_t type, PFloc_t loc,
       PFpnode_t *n1, PFpnode_t *n2, PFpnode_t *n3, PFpnode_t *n4)
{
    PFpnode_t *n;

    n = p_wire3 (type, loc, n1, n2, n3);
    n->child[3] = n4;

    return n;
} 


/* vim:set shiftwidth=4 expandtab: */
