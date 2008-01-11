/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * 
 * Debugging: dump compiled MIL tree in AY&T dot format or human readable
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

#include "pathfinder.h"

#include <stdlib.h>
#include <string.h>

#include "mildebug.h"

#include "mil.h"
#include "mem.h"
#include "pfstrings.h"
#include "prettyp.h"

/** Node names to print out for all the MIL tree nodes. */
char *m_id[]  = {
    [m_assgn]                 "assgn"
  , [m_comm_seq]              "comm_seq"
  , [m_print]                 "print"
  , [m_new]                   "new"
  , [m_seqbase]               "seqbase"
  , [m_tail]                  "tail"

  , [m_var]                   "var"
  , [m_lit_int]               "lit_int"
  , [m_lit_bit]               "lit_bit"
  , [m_lit_str]               "lit_str"
  , [m_lit_dbl]               "lit_dbl"
  , [m_lit_oid]               "lit_oid"
  , [m_type]                  "type"

  , [m_cast]                  "cast"
  , [m_fcast]                 "fcast"

  , [m_batloop]               "batloop"
  , [m_ifthenelse]            "if-then-else"
  , [m_insert]                "insert"
  , [m_nil]                   "nil"
};

/** string representation of MIL simple types */
static char *miltypes[] = {
    [mty_void]  "void"
  , [mty_int]   "int"
  , [mty_str]   "str"
  , [mty_bit]   "bit"
  , [mty_dbl]   "dbl"
  , [mty_item]  "item"
};

/** Current node id */
static unsigned no = 0;
/** Temporary variable to allocate mem for node names */
static char    *child;
/** Temporary variable for node labels in dot tree */
static char     label[32];

/** Print node with no content */
#define L(t)           snprintf (label, 32, t)
/** Print node with single content */
#define L2(l1, l2)     snprintf (label, 32, "%s [%s]",    l1, l2)
/** Print node with two content parts */
#define L3(l1, l2, l3) snprintf (label, 32, "%s [%s,%s]", l1, l2, l3)

/**
 * Print MIL tree in AT&T dot notation.
 * @param f File pointer for the output (usually @c stdout)
 * @param n The current node to print (function is recursive)
 * @param node Name of the parent node.
 */
static void 
mil_dot (FILE *f, PFmnode_t *n, char *node)
{
    int c;
    char s[sizeof ("4294967285")];

    switch (n->kind)
    {
        case m_var:       L2 (m_id[n->kind], PFqname_str (n->sem.var->qname));
                          break;
        case m_lit_int:   snprintf (s, sizeof (s), LLFMT, n->sem.num);
                          L2 (m_id[n->kind], s);
                          break;
        case m_lit_str:   snprintf (s, sizeof (s), "%s", n->sem.str);
                          L2 (m_id[n->kind], s);
                          break;
        case m_lit_dbl:   snprintf (s, sizeof (s), "%g", n->sem.dbl);
                          L2 (m_id[n->kind], s);
                          break;
        case m_lit_bit:   snprintf (s, sizeof (s),
                                  "%s", n->sem.tru ? "true" : "false");
                          L2 (m_id[n->kind], s);
                          break;
        case m_type:      snprintf (s, sizeof (s), "%s", miltypes[n->mty.ty]);
                          L2 (m_id[n->kind], s);
                          break;

        default:          L (m_id[n->kind]);
    }

    fprintf (f, "%s [label=\"%s\"];\n", node, label);

    for (c = 0; c < PFMNODE_MAXCHILD && n->child[c] != 0; c++) {      
        child = (char *) PFmalloc (sizeof ("node4294967296"));

        sprintf (child, "node%u", ++no);
        fprintf (f, "%s -> %s;\n", node, child);

        mil_dot (f, n->child[c], child);
    }
}


/**
 * Dump MIL tree in AT&T dot format
 * (pipe the output through `dot -Tps' to produce a Postscript file).
 *
 * @param f file to dump into
 * @param t root of abstract syntax tree
 */
void
PFmil_dot (FILE *f, PFmnode_t *root)
{
    if (root) {
        fprintf (f, "digraph XQueryMIL {\n");
        mil_dot (f, root, "node0");
        fprintf (f, "}\n");
    }
}

/**
 * Recursively walk the MIL tree @a n and prettyprint
 * the query it represents.
 *
 * @param n MIL tree to prettyprint
 */
static void
mil_pretty (PFmnode_t *n)
{
    int c;
    bool comma;

    if (!n)
        return;

    PFprettyprintf ("%s (%c", m_id[n->kind], START_BLOCK);

    comma = true;

    switch (n->kind)
    {
        case m_var:      PFprettyprintf ("%s", PFqname_str (n->sem.var->qname));
                         break;
        case m_lit_int:  PFprettyprintf (LLFMT, n->sem.num);
                         break;
        case m_lit_bit:  PFprettyprintf ("%s", n->sem.tru ? "true" : "false");
                         break;
        case m_lit_str:  PFprettyprintf ("%s", n->sem.str);
                         break;
        case m_lit_dbl:  PFprettyprintf ("%g", n->sem.dbl);
                         break;
        case m_type:     PFprettyprintf ("%s", miltypes[n->mty.ty]);
                         break;

        default:         comma = false;
    }

    for (c = 0;
            c < PFMNODE_MAXCHILD && n->child[c] != 0;
            c++) {
        if (comma)
            PFprettyprintf (",%c %c", END_BLOCK, START_BLOCK);
        comma = true;

        mil_pretty (n->child[c]);
    }

    PFprettyprintf ("%c)", END_BLOCK);
}

/**
 * Dump MIL tree @a t in pretty-printed form into file @a f.
 *
 * @param f file to dump into
 * @param t root of MIL tree
 */
void
PFmil_pretty (FILE *f, PFmnode_t *t)
{
    PFprettyprintf ("%c", START_BLOCK);
    mil_pretty (t);
    PFprettyprintf ("%c", END_BLOCK);

    (void) PFprettyp (f);

    fputc ('\n', f);
}


/* vim:set shiftwidth=4 expandtab: */
