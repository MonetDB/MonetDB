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
      [aop_lit_tbl]      = "TBL"
    , [aop_disjunion]    = "U"
    , [aop_difference]   = "DIFF"
      /* note: dot does not like the sequence "×\nfoo", so we put spaces
       * around the cross symbol.
       */
    , [aop_cross]        = " × "              /* yellow */
    , [aop_eqjoin]       = "|X|"              /* green */
    , [aop_scjoin]       = "/|"               /* light blue */
    , [aop_doc_tbl]      = "DOC"
    , [aop_select]       = "SEL"
    , [aop_negate]       = "NOT"
    , [aop_type]         = "TYPE"
    , [aop_cast]         = "CAST"
    , [aop_num_add]      = "num-add"
    , [aop_num_subtract] = "num_subtr"
    , [aop_num_multiply] = "num-mult"
    , [aop_num_divide]   = "num-div"
    , [aop_project]      = "¶"
    , [aop_rownum]       = "ROW#"
    , [aop_serialize]    = "SERIALIZE"
};

/** string representation of algebra atomic types */
static char *atomtype[] = {
      [aat_int]  = "int"
    , [aat_str]  = "str"
    , [aat_node] = "node"
    , [aat_dec]  = "dec"
    , [aat_dbl]  = "dbl"
    , [aat_bln]  = "bool"
};

/** Current node id */
/* static unsigned no = 0; */
/** Temporary variable to allocate mem for node names */
static char    *child;
/** Temporary variable for node labels in dot tree */
/*static char     label[32];*/

#if 0
/** Print node with no content */
#define L(t)           snprintf (label, 32, "%s", t)
/** Print node with single content */
#define L2(l1, l2)     snprintf (label, 32, "%s\\n%s",  l1, l2)
/** Print node with two content parts */
#define L3(l1, l2, l3) snprintf (label, 32, "%s %s %s", l1, l2, l3)
#endif

/**
 * Print algebra tree in AT&T dot notation.
 * @param dot Array into which we print
 * @param n The current node to print (function is recursive)
 * @param node Name of the parent node.
 */
static void 
alg_dot (PFarray_t *dot, PFalg_op_t *n, char *node)
{
    int c;
    static int node_id = 1;

    static char *color[] = {
        [aop_lit_tbl]      = "grey",
        [aop_disjunion]    = "grey",
        [aop_difference]   = "grey",
        [aop_cross]        = "yellow",
        [aop_eqjoin]       = "green",
        [aop_scjoin]       = "lightblue",
        [aop_doc_tbl]      = "grey",
        [aop_select]       = "grey",
        [aop_negate]       = "grey",
        [aop_type]         = "grey",
        [aop_cast]         = "grey",
        [aop_project]      = "grey",
        [aop_rownum]       = "red",
        [aop_serialize]    = "grey",
        [aop_num_add]      = "grey",
        [aop_num_subtract] = "grey",
        [aop_num_multiply] = "grey",
        [aop_num_divide]   = "grey"
    };

    n->node_id = node_id;
    node_id++;

    /* open up label */
    PFarray_printf (dot, "%s [label=\"", node);

    /* create label */
    switch (n->kind)
    {
        case aop_lit_tbl:
            /* list the attributes of this table */
            PFarray_printf (dot, "%s: <%s", a_id[n->kind],
                            n->schema.items[0].name);

            for (c = 1; c < n->schema.count;c++)
                PFarray_printf (dot, " | %s", n->schema.items[c].name);

            PFarray_printf (dot, ">");

            /* print out first tuple in table, if table is not empty */
            if (n->sem.lit_tbl.count > 0) {
                PFarray_printf (dot, "\\n[");

                for (c = 0; c < n->sem.lit_tbl.tuples[0].count; c++) {
                    if (c != 0)
                        PFarray_printf (dot, ",");

                    if (n->sem.lit_tbl.tuples[0].atoms[c].type == aat_nat)
                        PFarray_printf (dot, "%i",
                                n->sem.lit_tbl.tuples[0].atoms[c].val.nat);
                    else if (n->sem.lit_tbl.tuples[0].atoms[c].type == aat_int)
                        PFarray_printf (dot, "%i",
                                n->sem.lit_tbl.tuples[0].atoms[c].val.int_);
                    else if (n->sem.lit_tbl.tuples[0].atoms[c].type == aat_str)
                        PFarray_printf (dot, "%s",
                                n->sem.lit_tbl.tuples[0].atoms[c].val.str);
                    else if (n->sem.lit_tbl.tuples[0].atoms[c].type == aat_node)
                        PFarray_printf (dot, "%i",
                                n->sem.lit_tbl.tuples[0].atoms[c].val.node);
                    else if (n->sem.lit_tbl.tuples[0].atoms[c].type == aat_dec)
                        PFarray_printf (dot, "%g",
                                n->sem.lit_tbl.tuples[0].atoms[c].val.dec);
                    else if (n->sem.lit_tbl.tuples[0].atoms[c].type == aat_dbl)
                        PFarray_printf (dot, "%g",
                                n->sem.lit_tbl.tuples[0].atoms[c].val.dbl);
                    else if (n->sem.lit_tbl.tuples[0].atoms[c].type == aat_bln)
                        PFarray_printf (dot, "%s",
                                n->sem.lit_tbl.tuples[0].atoms[c].val.bln ?
                                        "t" : "f");
                }

                PFarray_printf (dot, "]");

                /* if there is more than one tuple in the table, mark
                 * the dot node accordingly
                 */
                if (n->sem.lit_tbl.count > 1)
                    PFarray_printf (dot, "...");
            }
            break;

        case aop_cross:
            PFarray_printf (dot, "%s", a_id[n->kind]);
            break;

        case aop_eqjoin:
            PFarray_printf (dot, "%s (%s:%s)",
                            a_id[n->kind], n->sem.eqjoin.att1,
                            n->sem.eqjoin.att2);
            break;

        case aop_scjoin:
            PFarray_printf (dot, "%s ", a_id[n->kind]);
                
            /* print out XPath axis */
            switch (n->sem.scjoin.axis)
            {
                case aop_anc:
                    PFarray_printf (dot, "ancestor::");
                    break;
                case aop_anc_s:
                    PFarray_printf (dot, "anc-or-self::");
                    break;
                case aop_attr:
                    PFarray_printf (dot, "attribute::");
                    break;
                case aop_chld:
                    PFarray_printf (dot, "child::");
                    break;
                case aop_desc:
                    PFarray_printf (dot, "descendant::");
                    break;
                case aop_desc_s:
                    PFarray_printf (dot, "desc-or-self::");
                    break;
                case aop_fol:
                    PFarray_printf (dot, "following::");
                    break;
                case aop_fol_s:
                    PFarray_printf (dot, "fol-sibling::");
                    break;
                case aop_par:
                    PFarray_printf (dot, "parent::");
                    break;
                case aop_prec:
                    PFarray_printf (dot, "preceding::");
                    break;
                case aop_prec_s:
                    PFarray_printf (dot, "prec-sibling::");
                    break;
                case aop_self:
                    PFarray_printf (dot, "self::");
                    break;
                default: PFoops (OOPS_FATAL,
                        "unknown XPath axis in dot output");
            }

            /* print out kind test */
            switch (n->sem.scjoin.test)
            {
                case aop_name:
                    PFarray_printf (dot, "%s",
                                    n->sem.scjoin.str.qname.loc);
                    break;
                case aop_node:
                    PFarray_printf (dot, "node()");
                    break;
                case aop_comm:
                    PFarray_printf (dot, "comment()");
                    break;
                case aop_text:
                    PFarray_printf (dot, "text()");
                    break;
                case aop_pi:
                    PFarray_printf (dot, "pi()");
                    break;
                case aop_pi_tar:
                    PFarray_printf (dot, "pi(%s)", n->sem.scjoin.str.target);
                    break;
                case aop_doc:
                    PFarray_printf (dot, "doc()");
                    break;
                case aop_elem:
                    PFarray_printf (dot, "*");
                    break;
                case aop_at_tst:
                    PFarray_printf (dot, "attribute");
                    break;
                default: PFoops (OOPS_FATAL,
                        "unknown kind test in dot output");
            }
            break;

        case aop_select:
            PFarray_printf (dot, "%s (%s)", a_id[n->kind],
                            n->sem.select.att);
            break;

        case aop_negate:
            PFarray_printf (dot, "%s (%s:%s)", a_id[n->kind],
                            n->sem.negate.res, n->sem.negate.att);
            break;

        case aop_type:
            PFarray_printf (dot, "%s (%s:%s,%s)", a_id[n->kind],
                            n->sem.type.res, n->sem.type.att,
                            PFty_str (n->sem.type.ty));
            break;

        case aop_cast:
            PFarray_printf (dot, "%s (%s,%s)", a_id[n->kind],
                            n->sem.cast.att, atomtype[n->sem.cast.ty]);
            break;

        case aop_num_add:
        case aop_num_subtract:
        case aop_num_multiply:
        case aop_num_divide:
            PFarray_printf (dot, "%s (%s:%s, %s)", a_id[n->kind],
                            n->sem.arithm.res, n->sem.arithm.att1,
                            n->sem.arithm.att2);
            break;

        case aop_project:
            if (strcmp(n->sem.proj.items[0].new, n->sem.proj.items[0].old))
                PFarray_printf (dot, "%s (%s:%s", a_id[n->kind],
                                n->sem.proj.items[0].new,
                                n->sem.proj.items[0].old);
            else
                PFarray_printf (dot, "%s (%s", a_id[n->kind],
                                n->sem.proj.items[0].old);

            for (c = 1; c < n->sem.proj.count; c++)
                if (strcmp(n->sem.proj.items[c].new, n->sem.proj.items[c].old))
                    PFarray_printf (dot, ",%s:%s",
                                    n->sem.proj.items[c].new,
                                    n->sem.proj.items[c].old);
                else
                    PFarray_printf (dot, ",%s", n->sem.proj.items[c].old);

            PFarray_printf (dot, ")");
            break;

        case aop_rownum:
            PFarray_printf (dot, "%s (%s:<%s", a_id[n->kind],
                            n->sem.rownum.attname,
                            n->sem.rownum.sortby.atts[0]);

            for (c = 1; c < n->sem.rownum.sortby.count; c++)
                PFarray_printf (dot, ",%s", n->sem.rownum.sortby.atts[c]);

            PFarray_printf (dot, ">");

            if (n->sem.rownum.part)
                PFarray_printf (dot, "/%s", n->sem.rownum.part);

            PFarray_printf (dot, ")");
            break;

        default:          PFarray_printf (dot, "%s", a_id[n->kind]);
    }

    /*
     * print variable name used in the MIL program to represent this
     * expression if available
     */
    if (n->bat_prefix)
        PFarray_printf (dot, "\\n%s", n->bat_prefix);

    /* close up label */
    PFarray_printf (dot, "\", color=%s ];\n", color[n->kind]);

    for (c = 0; c < PFALG_OP_MAXCHILD && n->child[c] != 0; c++) {      
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

            alg_dot (dot, n->child[c], child);
        }
    }
}


/**
 * Dump algebra tree in AT&T dot format
 * (pipe the output through `dot -Tps' to produce a Postscript file).
 *
 * @param f file to dump into
 * @param root root of abstract syntax tree
 */
void
PFalg_dot (FILE *f, PFalg_op_t *root)
{
    if (root) {
        /* initialize array to hold dot output */
        PFarray_t *dot = PFarray (sizeof (char));

        PFarray_printf (dot, "digraph XQueryAlgebra {\n"
                             "ordering=out;\n"
                             "node [shape=box];\n"
                             "node [height=0.1];\n"
                             "node [width=0.2];\n"
                             "node [style=filled];\n"
                             "node [color=grey];\n"
                             "node [fontsize=10];\n");

        alg_dot (dot, root, "node1");
        /* put content of array into file */
        PFarray_printf (dot, "}\n");
        fprintf (f, "%s", dot->base);
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
            case aat_dec:   PFprettyprintf ("%g", t.atoms[i].val.dec);
                            break;
            case aat_dbl:   PFprettyprintf ("%g", t.atoms[i].val.dbl);
                            break;
            case aat_bln:   PFprettyprintf ("%s", t.atoms[i].val.bln ? "true"
                                            : "false");
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

        case aop_doc_tbl:
            PFprettyprintf (" <");
            for (i = 0; i < n->schema.count; i++) {
                PFprettyprintf ("(\"%s\":", n->schema.items[i].name);
                print_type (n->schema.items[i].type);
                PFprettyprintf (")%c", i < n->schema.count-1 ? ',' : '>');
            }
            break;

        case aop_cross:
        case aop_disjunion:
            break;

        case aop_difference:
            break;

        case aop_eqjoin:
            break;

        case aop_scjoin:
            break;

        case aop_select:
            break;

        case aop_negate:
            break;

        case aop_type:
            break;

        case aop_cast:
            break;

        case aop_num_add:
            break;

        case aop_num_subtract:
            break;

        case aop_num_multiply:
            break;

        case aop_num_divide:
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
