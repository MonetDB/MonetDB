/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * 
 * Debugging: dump MIL algebra tree in AT&T dot format
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
#include "ma_debug.h"

#include "mem.h"
#include "array.h"
#include "oops.h"

/** Node names to print out for all the Algebra tree nodes. */
static char *ma_id[] = {

      [ma_lit_oid]      = "lit_oid"
    , [ma_lit_int]      = "lit_int"
    , [ma_lit_dbl]      = "lit_dbl"
    , [ma_lit_str]      = "lit_str"
    , [ma_lit_bit]      = "lit_bit"
                                    
    , [ma_new]          = "new"
    , [ma_insert]       = "insert"
    , [ma_bun]          = "bun"
    , [ma_seqbase]      = "seqbase"
    , [ma_project]      = "project"
    , [ma_reverse]      = "reverse"
    , [ma_sort]         = "sort"
    , [ma_ctrefine]     = "ctrefine"
    , [ma_join]         = "join"
    , [ma_leftjoin]     = "leftjoin"
    , [ma_cross]        = "cross"
    , [ma_mirror]       = "mirror"
    , [ma_kunique]      = "kunique"
    , [ma_mark_grp]     = "mark_grp"
    , [ma_mark]         = "mark"
    , [ma_append]       = "append"
    , [ma_count]        = "count"
    , [ma_oid]          = "oid"
    , [ma_moid]         = "[oid]"
    , [ma_mint]         = "[int]"
    , [ma_mstr]         = "[str]"
    , [ma_mdbl]         = "[dbl]"
    , [ma_mbit]         = "[bit]"
    , [ma_madd]         = "[+]"
    , [ma_msub]         = "[-]"
    , [ma_mmult]        = "[*]"
    , [ma_mdiv]         = "[/]"
    , [ma_serialize]    = "serialize"
    , [ma_ser_args]     = "ser_args"

};

static char *ty[] = {
      [m_void]          = "void"
    , [m_oid]           = "oid"
    , [m_int]           = "int"
    , [m_str]           = "str"
    , [m_dbl]           = "dbl"
    , [m_bit]           = "bit"
};

static char *color[] = {

      [ma_lit_oid]      = "grey"
    , [ma_lit_int]      = "grey"
    , [ma_lit_dbl]      = "grey"
    , [ma_lit_str]      = "grey"
    , [ma_lit_bit]      = "grey"
                                    
    , [ma_new]          = "grey"
    , [ma_insert]       = "grey"
    , [ma_bun]          = "grey"
    , [ma_seqbase]      = "grey"
    , [ma_project]      = "grey"
    , [ma_reverse]      = "grey"
    , [ma_sort]         = "red"
    , [ma_ctrefine]     = "grey"
    , [ma_join]         = "grey"
    , [ma_leftjoin]     = "grey"
    , [ma_cross]        = "grey"
    , [ma_mirror]       = "grey"
    , [ma_kunique]      = "grey"
    , [ma_mark_grp]     = "grey"
    , [ma_mark]         = "grey"
    , [ma_append]       = "grey"
    , [ma_count]        = "grey"
    , [ma_oid]          = "grey"
    , [ma_moid]         = "grey"
    , [ma_mint]         = "grey"
    , [ma_mstr]         = "grey"
    , [ma_mdbl]         = "grey"
    , [ma_mbit]         = "grey"
    , [ma_madd]         = "grey"
    , [ma_msub]         = "grey"
    , [ma_mmult]        = "grey"
    , [ma_mdiv]         = "grey"

    , [ma_serialize]    = "grey"
    , [ma_ser_args]     = "grey"

};

/** Temporary variable to allocate mem for node names */
static char    *child;

/**
 * Print MIL algebra tree in AT&T dot notation.
 * @param dot Array into which we print
 * @param n The current node to print (function is recursive)
 * @param node Name of the parent node.
 */
static void 
ma_dot (PFarray_t *dot, PFma_op_t *n, char *node)
{
    int c;
    static unsigned int node_id = 1;

    n->node_id = node_id++;

    /* open up label */
    PFarray_printf (dot, "%s [label=\"", node);

    /* create label */
    switch (n->kind) {

        case ma_new:
            PFarray_printf (dot, "%s (%s,%s)", ma_id[n->kind],
                                               ty[n->sem.new.htype],
                                               ty[n->sem.new.ttype]);
            break;

        case ma_lit_oid:
            PFarray_printf (dot, "%s (%u@0)", ma_id[n->kind],
                                              n->sem.lit_val.val.o);
            break;

        case ma_lit_int:
            PFarray_printf (dot, "%s (%i)", ma_id[n->kind],
                                            n->sem.lit_val.val.i);
            break;

        case ma_lit_dbl:
            PFarray_printf (dot, "%s (%d)", ma_id[n->kind],
                                            n->sem.lit_val.val.d);
            break;

        case ma_lit_str:
            PFarray_printf (dot, "%s (\\\"%16s\\\")", ma_id[n->kind],
                                                      n->sem.lit_val.val.s);
            break;

        case ma_lit_bit:
            PFarray_printf (dot, "%s (%s)",
                                 ma_id[n->kind],
                                 n->sem.lit_val.val.b ? "true" : "false");
            break;

        default:
            PFarray_printf (dot, "%s", ma_id[n->kind]);
            break;
    }

    /*
     * print expression type after newline
     */
    PFarray_printf (dot, "\\n");

    switch (n->type.kind) {

        case type_bat:
            PFarray_printf (dot, "[%s, %s]", ty[n->type.ty.bat.htype],
                                              ty[n->type.ty.bat.ttype]);
            break;

        case type_atom:
            PFarray_printf (dot, "%s", ty[n->type.ty.atom]);
            break;

        case type_none:
            break;
    }

    /* Print MIL variable name if available */
    if (n->varname)
        PFarray_printf (dot, "\\n%s", n->varname);

    /* close up label */
    PFarray_printf (dot, "\", color=%s ];\n", color[n->kind]);

    for (c = 0; c < MILALGEBRA_MAXCHILD && n->child[c] != 0; c++) {      
        child = (char *) PFmalloc (sizeof ("node4294967296"));

        /*
         * Label for child node has already been built, such that
         * only the edge between parent and child must be created
         */
        if (n->child[c]->node_id != 0) {
            sprintf (child, "node%i", n->child[c]->node_id);
            PFarray_printf (dot, "%s -> %s;\n", node, child);
        }
        else {
            sprintf (child, "node%i", node_id);
            PFarray_printf (dot, "%s -> %s;\n", node, child);

            ma_dot (dot, n->child[c], child);
        }
    }
}

static void
clear_node_ids (PFma_op_t *n)
{
    unsigned int i;

    n->node_id = 0;
    for (i = 0; i < MILALGEBRA_MAXCHILD && n->child[i]; i++)
        clear_node_ids (n->child[i]);
}

/**
 * Dump algebra tree in AT&T dot format
 * (pipe the output through `dot -Tps' to produce a Postscript file).
 *
 * @param f file to dump into
 * @param root root of abstract syntax tree
 */
void
PFma_dot (FILE *f, PFma_op_t *root)
{
    clear_node_ids (root);

    if (root) {
        /* initialize array to hold dot output */
        PFarray_t *dot = PFarray (sizeof (char));

        PFarray_printf (dot, "digraph MilAlgebra {\n"
                             "ordering=out;\n"
                             "node [shape=box];\n"
                             "node [height=0.1];\n"
                             "node [width=0.2];\n"
                             "node [style=filled];\n"
                             "node [color=grey];\n"
                             "node [fontsize=10];\n");

        ma_dot (dot, root, "node1");
        /* put content of array into file */
        PFarray_printf (dot, "}\n");
        fprintf (f, "%s", dot->base);
    }
}



/* vim:set shiftwidth=4 expandtab: */
