/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * 
 * Debugging: dump algebra tree in AY&T dot format or human readable
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
 *  created by U Konstanz are Copyright (C) 2000-2004 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "pathfinder.h"
#include "algdebug.h"

#include "mem.h"
/* #include "pfstrings.h" */
#include "prettyp.h"
#include "oops.h"

/** Node names to print out for all the Algebra tree nodes. */
char *a_id[]  = {
      [aop_lit_tbl]    "TBL"
    , [aop_disjunion]  "U"
    , [aop_cross]      "×"
    , [aop_project]    "¶"
    , [aop_rownum]     "ROW#"
    , [aop_serialize]  "SERIALIZE"
};

/** string representation of algebra atomic types */
static char *atomtype[] = {
      [aat_int]   "int"
    , [aat_str]   "str"
    , [aat_node]  "node"
};

/** Current node id */
/* static unsigned no = 0; */
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
 * Print algebra tree in AT&T dot notation.
 * @param f File pointer for the output (usually @c stdout)
 * @param n The current node to print (function is recursive)
 * @param node Name of the parent node.
 */
static void 
alg_dot (FILE *f, PFalg_op_t *n, char *node)
{
    int c;
    char s[sizeof ("4294967285")];

    switch (n->kind)
    {
        case aop_lit_tbl: snprintf (s, sizeof (s),
                                    "%i / %i",
                                    n->schema.count, n->sem.lit_tbl.count);
                          L2 (a_id[n->kind], s);
                          break;
                                    
        case aop_project: snprintf (s, sizeof (s), "%i", n->schema.count);
                          L2 (a_id[n->kind], s);
                          break;
        default:          L (a_id[n->kind]);
    }

    fprintf (f, "%s [label=\"%s\"];\n", node, label);

    for (c = 0; c < PFALG_OP_MAXCHILD && n->child[c] != 0; c++) {      
        child = (char *) PFmalloc (sizeof ("node4294967296"));

        sprintf (child, "node%x", (unsigned int) n->child[c]);
        fprintf (f, "%s -> %s;\n", node, child);

        alg_dot (f, n->child[c], child);
    }
}


/**
 * Dump algebra tree in AT&T dot format
 * (pipe the output through `dot -Tps' to produce a Postscript file).
 *
 * @param f file to dump into
 * @param t root of abstract syntax tree
 */
void
PFalg_dot (FILE *f, PFalg_op_t *root)
{
    if (root) {
        fprintf (f, "digraph XQueryAlgebra {\n");
        alg_dot (f, root, "node0");
        fprintf (f, "}\n");
    }
}

static void
print_tuple (PFalg_tuple_t t)
{
    int i;

    PFprettyprintf ("%c[", START_BLOCK);

    for (i = 0; i < t.count; i++) {

        if (i != 0)
            PFprettyprintf (",");

        switch (t.atoms[i].type) {
            case aat_nat:   PFprettyprintf ("#%i", t.atoms[i].val.nat);
                            break;
            case aat_int:   PFprettyprintf ("%i", t.atoms[i].val.int_);
                            break;
            case aat_str:   PFprettyprintf ("\"%s\"", t.atoms[i].val.str);
                            break;
            case aat_node:  PFprettyprintf ("@%i", t.atoms[i].val.node);
                            break;
        }
    }
    PFprettyprintf ("]%c", END_BLOCK);
}

/**
 * Print all the types in @a t, separated by a comma. Types are
 * listed in the array #atomtype.
 */
static void
print_type (PFalg_type_t t)
{
    int count = 0;
    PFalg_type_t i;

    for (i = 1; i; i <<= 1)    /* shift bit through bit-vector */
        if (t & i)
            PFprettyprintf ("%s%s", count++ ? "," : "", atomtype[i]);
}


/**
 * Recursively walk the algebra tree @a n and prettyprint
 * the query it represents.
 *
 * @param n algebra tree to prettyprint
 */
static void
alg_pretty (PFalg_op_t *n)
{
    int c;
    int i;

    if (!n)
        return;

    PFprettyprintf ("%s", a_id[n->kind]);

    switch (n->kind)
    {
        case aop_lit_tbl:
            PFprettyprintf (" <");
            for (i = 0; i < n->schema.count; i++) {
                PFprettyprintf ("(\"%s\":", n->schema.items[i].name);
                print_type (n->schema.items[i].type);
                PFprettyprintf (")%c", i < n->schema.count-1 ? ',' : '>');
            }
            PFprettyprintf ("%c[", START_BLOCK);
            for (i = 0; i < n->sem.lit_tbl.count; i++) {
                if (i)
                    PFprettyprintf (",");
                print_tuple (n->sem.lit_tbl.tuples[i]);
            }
            PFprettyprintf ("]%c", END_BLOCK);

            break;

        case aop_cross:
        case aop_disjunion:
            break;

        case aop_project:
            PFprettyprintf (" (");
            for (i = 0; i < n->sem.proj.count; i++)
                if (strcmp (n->sem.proj.items[i].new, n->sem.proj.items[i].old))
                    PFprettyprintf ("%s:%s%c", 
                                    n->sem.proj.items[i].new,
                                    n->sem.proj.items[i].old,
                                    i < n->sem.proj.count-1 ? ',' : ')');
                else
                    PFprettyprintf ("%s%c", 
                                    n->sem.proj.items[i].old,
                                    i < n->sem.proj.count-1 ? ',' : ')');
            break;

        case aop_rownum:
            PFprettyprintf (" (%s:<", n->sem.rownum.attname);
            for (i = 0; i < n->sem.rownum.sortby.count; i++)
                PFprettyprintf ("%s%c",
                                n->sem.rownum.sortby.atts[i],
                                i < n->sem.rownum.sortby.count-1 ? ',' : '>');
            if (n->sem.rownum.part)
                PFprettyprintf ("/%s", n->sem.rownum.part);
            PFprettyprintf (")");
            break;

        case aop_serialize:
            break;
    }

    for (c = 0; c < PFALG_OP_MAXCHILD && n->child[c] != 0; c++) {
        PFprettyprintf ("%c[", START_BLOCK);
        alg_pretty (n->child[c]);
        PFprettyprintf ("]%c", END_BLOCK);
    }
}

/**
 * Dump algebra tree @a t in pretty-printed form into file @a f.
 *
 * @param f file to dump into
 * @param t root of algebra tree
 */
void
PFalg_pretty (FILE *f, PFalg_op_t *t)
{
    PFprettyprintf ("%c", START_BLOCK);
    alg_pretty (t);
    PFprettyprintf ("%c", END_BLOCK);

    (void) PFprettyp (f);

    fputc ('\n', f);
}


/* vim:set shiftwidth=4 expandtab: */
