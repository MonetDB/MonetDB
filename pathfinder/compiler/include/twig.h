/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Pathfinder specific setups for `twig'.  
 *
 * This provides the necessary interface functionality to make Pathfinder's
 * abstract syntax/core/... trees accessible for `twig'.
 *
 * This file assumes the following:
 *
 * - macro @a TWIG_NODE denotes the tree node struct type @a t
 * - @a t has a field @a int @a kind, indicating the node's kind
 * - @a t has an array field @a TWIG_NODE @a *child[0..], holding pointers to
 *   the node's children
 * - macro @a TWIG_MAXCHILD indicates the maximum number of possible 
 *   child nodes
 * - array @a int @a TWIG_ID[] maps @a t node kinds to twig node identifiers
 *
 *
 * Copyright Notice:
 * -----------------
 *
 *  The contents of this file are subject to the MonetDB Public
 *  License Version 1.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 *
 *  The Original Code is the ``Pathfinder'' system. The Initial
 *  Developer of the Original Code is the Database & Information
 *  Systems Group at the University of Konstanz, Germany. Portions
 *  created by U Konstanz are Copyright (C) 2000-2003 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

#ifndef TWIG_H
#define TWIG_H

/** sanity checks: TWIG_NODE and TWIG_MAXCHILD defined? */
#ifndef TWIG_NODE
#error TWIG_NODE is undefined
#endif
#ifndef TWIG_MAXCHILD
#error TWIG_MAXCHILD is undefined
#endif

typedef TWIG_NODE *NODEPTR;

/** cost of a rule */
#define COST int

/** infinite cost */
#define INFINITY INT_MAX

/** default cost */
#define ZEROCOST 1
#define ADDCOST(a,b) ((a) += (b))


/** 
 * (Optional) support for optimization phases:
 *
 * In the cost part of a twig rule use (usually as the first instruction)
 *
 *                          PHASE (foo);
 *
 * to have the rule activated iff we are in optimization
 * phase `foo'.
 */
#define PHASE(p) if ((p) & twig_phase__) cost = 0; else ABORT
static int twig_phase__;

/**
 * Run the twig pattern matcher (in phases).
 */
static NODEPTR rewrite (NODEPTR, int *) __attribute__ ((unused));


/** compare costs @a a and @a b */
static int
COSTLESS (COST a, COST b)
{
    return (a < b);
}

/** 
 * root of tree while twig operates
 */
static NODEPTR twig_root__ = 0;

/**
 * Return @a n-th child of @a r (or the root).
 *
 * @param r parent node
 * @param n number of child (1..)
 * @return @a n-th child of @a r
 */
static NODEPTR
mtGetNodes (NODEPTR r, int n)
{
    if (r == 0 && n == 1)
        return twig_root__;

    if (n >= 1 && n <= TWIG_MAXCHILD && r)
        return r->child[n - 1];

    return 0;
}

/**
 * Replace the @a n-th child of @a r (or set the root).
 *
 * @param r parent node
 * @param n number of child (1..)
 * @param c new child node
 */
static void
mtSetNodes (NODEPTR r, int n, NODEPTR c)
{
    assert (n >= 1 && n <= TWIG_MAXCHILD);

    if (r == 0 && n == 1)
        twig_root__ = c;
    else
        r->child[n - 1] = c;
}

/**
 * Return node type of tree node @a n (or the root node).
 *
 * @param n tree node
 * @return node type of node @a n
 */
static int
mtValue (NODEPTR n)
{
    if (n)
        return TWIG_ID[n->kind];

    return mtValue (mtGetNodes (0, 1));
}

#include "walker_template"

/**
 * Run the twig pattern matcher (in phases) on tree @a r.
 *
 * @param r root of tree to match over
 * @param phases array of match phases (terminated by -1), or 0 (single match)
 * @return root of tree rewritten by twig
 */
static NODEPTR
rewrite (NODEPTR r, int *phases) 
{
    assert (r);

    /* set twig's root node */
    mtSetNodes (0, 1, r);

    /* intialize the matcher */
    _matchinit ();

    /* no phase announced (yet) */
    twig_phase__ = -1;

    if (phases)
        while (*phases >= 0) {
            /* announce next phase */
            twig_phase__ = *phases++;

            /* intiate the pattern match */
            _match ();

            /* root node of matched tree might have changed */
            mtSetNodes (0, 1, mtGetNodes (0, 1));
        }
    else
        _match ();

    /* return root node of rewritten tree */
    return mtGetNodes (0, 1);
} 

#endif /* TWIG_H */

/* vim:set shiftwidth=4 expandtab: */
