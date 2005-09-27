/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * 
 * Debugging: dump logical algebra tree in AY&T dot format.
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
 *  created by U Konstanz are Copyright (C) 2000-2005 University
 *  of Konstanz. All Rights Reserved.
 *
 * $Id$
 */

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "pathfinder.h"
#include "logdebug.h"

#include "mem.h"
/* #include "pfstrings.h" */
#include "prettyp.h"
#include "oops.h"

/** Node names to print out for all the Algebra tree nodes. */
static char *a_id[]  = {
      [la_lit_tbl]          = "TBL"
    , [la_empty_tbl]        = "EMPTY_TBL"
    , [la_disjunion]        = "U"
    , [la_intersect]        = "n"
    , [la_difference]       = "DIFF"             /* \"#FFA500\" */
      /* note: dot does not like the sequence "×\nfoo", so we put spaces
       * around the cross symbol.
       */
    , [la_cross]            = " × "              /* \"#00FFFF\" */
    , [la_eqjoin]           = "|X|"              /* \"#00FF00\" */
    , [la_scjoin]           = "/|"               /* light blue */
    , [la_doc_tbl]          = "DOC"
    , [la_select]           = "SEL"
    , [la_type]             = "TYPE"
    , [la_cast]             = "CAST"
    , [la_num_add]          = "num-add"
    , [la_num_subtract]     = "num_subtr"
    , [la_num_multiply]     = "num-mult"
    , [la_num_divide]       = "num-div"
    , [la_num_modulo]       = "num-mod"
    , [la_project]          = "¶"
    , [la_rownum]           = "ROW#"              /* \"#FF0000\" */
    , [la_serialize]        = "SERIALIZE"
    , [la_num_eq]           = "="
    , [la_num_gt]           = ">"
    , [la_num_neg]          = "-"
    , [la_bool_and ]        = "AND"
    , [la_bool_or ]         = "OR"
    , [la_bool_not]         = "NOT"
    , [la_sum]              = "SUM"
    , [la_count]            = "COUNT"
    , [la_distinct]         = "DISTINCT"         /* indian \"#FF0000\" */
    , [la_element]          = "ELEM"             /* lawn \"#00FF00\" */
    , [la_element_tag]      = "ELEM_TAG"         /* lawn \"#00FF00\" */
    , [la_attribute]        = "ATTR"             /* lawn \"#00FF00\" */
    , [la_textnode]         = "TEXT"             /* lawn \"#00FF00\" */
    , [la_docnode]          = "DOC"              /* lawn \"#00FF00\" */
    , [la_comment]          = "COMMENT"          /* lawn \"#00FF00\" */
    , [la_processi]         = "PI"               /* lawn \"#00FF00\" */
    , [la_concat]           = "strconcat"
    , [la_merge_adjacent]   = "merge-adjacent-text-nodes"
    , [la_doc_access]       = "doc_access"
    , [la_string_join]      = "string_join"
    , [la_seqty1]           = "SEQTY1"
    , [la_all]              = "ALL"
    , [la_roots]            = "ROOTS"
    , [la_fragment]         = "FRAGs"
    , [la_frag_union]       = "FRAG_UNION"
    , [la_empty_frag]       = "EMPTY_FRAG"
};

/** string representation of algebra atomic types */
static char *atomtype[] = {
      [aat_int]   = "int"
    , [aat_str]   = "str"
    , [aat_node]  = "node"
    , [aat_anode] = "attr"
    , [aat_pnode] = "pnode"
    , [aat_dec]   = "dec"
    , [aat_dbl]   = "dbl"
    , [aat_bln]   = "bool"
};

/** Current node id */
/* static unsigned no = 0; */
/** Temporary variable to allocate mem for node names */
static char    *child;
/** Temporary variable for node labels in dot tree */
/*static char     label[32];*/

/**
 * Print algebra tree in AT&T dot notation.
 * @param dot Array into which we print
 * @param n The current node to print (function is recursive)
 * @param node Name of the parent node.
 */
static void 
la_dot (PFarray_t *dot, PFla_op_t *n, char *node)
{
    unsigned int c;
    static int node_id = 1;

    static char *color[] = {
          [la_lit_tbl]        = "\"#C0C0C0\""
        , [la_empty_tbl]      = "\"#C0C0C0\""
        , [la_disjunion]      = "\"#C0C0C0\""
        , [la_intersect]      = "\"#C0C0C0\""
        , [la_difference]     = "\"#FFA500\""
        , [la_cross]          = "\"#00FFFF\""
        , [la_eqjoin]         = "\"#00FF00\""
        , [la_scjoin]         = "\"#1E90FF\""
        , [la_doc_tbl]        = "\"#C0C0C0\""
        , [la_select]         = "\"#C0C0C0\""
        , [la_type]           = "\"#C0C0C0\""
        , [la_cast]           = "\"#C0C0C0\""
        , [la_project]        = "\"#C0C0C0\""
        , [la_rownum]         = "\"#FF0000\""
        , [la_serialize]      = "\"#C0C0C0\""
        , [la_num_add]        = "\"#C0C0C0\""
        , [la_num_subtract]   = "\"#C0C0C0\""
        , [la_num_multiply]   = "\"#C0C0C0\""
        , [la_num_divide]     = "\"#C0C0C0\""
        , [la_num_modulo]     = "\"#C0C0C0\""
        , [la_num_eq]         = "\"#C0C0C0\""
        , [la_num_gt]         = "\"#C0C0C0\""
        , [la_num_neg]        = "\"#C0C0C0\""
        , [la_bool_and ]      = "\"#C0C0C0\""
        , [la_bool_or ]       = "\"#C0C0C0\""
        , [la_bool_not]       = "\"#C0C0C0\""
        , [la_sum]            = "\"#C0C0C0\""
        , [la_count]          = "\"#C0C0C0\""
        , [la_distinct]       = "\"#CD5C5C\""
        , [la_element]        = "\"#00FC59\""
        , [la_element_tag]    = "\"#00FC59\""
        , [la_attribute]      = "\"#00FC59\""
        , [la_textnode]       = "\"#00FC59\""
        , [la_docnode]        = "\"#00FC59\""
        , [la_comment]        = "\"#00FC59\""
        , [la_processi]       = "\"#00FC59\""
        , [la_concat]         = "\"#C0C0C0\""
        , [la_merge_adjacent] = "\"#C0C0C0\""
        , [la_doc_access]     = "\"#C0C0C0\""
        , [la_string_join]    = "\"#C0C0C0\""
        , [la_seqty1]         = "\"#C0C0C0\""
        , [la_all]            = "\"#C0C0C0\""
        , [la_roots]          = "\"#C0C0C0\""
        , [la_fragment]       = "\"#C0C0C0\""
        , [la_frag_union]     = "\"#C0C0C0\""
        , [la_empty_frag]     = "\"#C0C0C0\""
    };

    n->node_id = node_id;
    node_id++;

    /* open up label */
    PFarray_printf (dot, "%s [label=\"", node);

    /* create label */
    switch (n->kind)
    {
        case la_lit_tbl:
            /* list the attributes of this table */
            PFarray_printf (dot, "%s: <%s", a_id[n->kind],
                            PFatt_print (n->schema.items[0].name));

            for (c = 1; c < n->schema.count;c++)
                PFarray_printf (dot, " | %s",
                                PFatt_print (n->schema.items[c].name));

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
                    else
                        PFarray_printf (dot, "<NODE>");
                }

                PFarray_printf (dot, "]");

                /* if there is more than one tuple in the table, mark
                 * the dot node accordingly
                 */
                if (n->sem.lit_tbl.count > 1)
                    PFarray_printf (dot, "...");
            }
            break;

        case la_empty_tbl:
            /* list the attributes of this table */
            PFarray_printf (dot, "%s: <%s", a_id[n->kind],
                            PFatt_print (n->schema.items[0].name));

            for (c = 1; c < n->schema.count;c++)
                PFarray_printf (dot, " | %s", 
                                PFatt_print (n->schema.items[c].name));

            PFarray_printf (dot, ">");
            break;

        case la_eqjoin:
            PFarray_printf (dot, "%s (%s=%s)",
                            a_id[n->kind], 
                            PFatt_print (n->sem.eqjoin.att1),
                            PFatt_print (n->sem.eqjoin.att2));
            break;

        case la_scjoin:
            PFarray_printf (dot, "%s ", a_id[n->kind]);
                
            /* print out XPath axis */
            switch (n->sem.scjoin.axis)
            {
                case alg_anc:
                    PFarray_printf (dot, "ancestor::");
                    break;
                case alg_anc_s:
                    PFarray_printf (dot, "anc-or-self::");
                    break;
                case alg_attr:
                    PFarray_printf (dot, "attribute::");
                    break;
                case alg_chld:
                    PFarray_printf (dot, "child::");
                    break;
                case alg_desc:
                    PFarray_printf (dot, "descendant::");
                    break;
                case alg_desc_s:
                    PFarray_printf (dot, "desc-or-self::");
                    break;
                case alg_fol:
                    PFarray_printf (dot, "following::");
                    break;
                case alg_fol_s:
                    PFarray_printf (dot, "fol-sibling::");
                    break;
                case alg_par:
                    PFarray_printf (dot, "parent::");
                    break;
                case alg_prec:
                    PFarray_printf (dot, "preceding::");
                    break;
                case alg_prec_s:
                    PFarray_printf (dot, "prec-sibling::");
                    break;
                case alg_self:
                    PFarray_printf (dot, "self::");
                    break;
                default: PFoops (OOPS_FATAL,
                        "unknown XPath axis in dot output");
            }

            PFarray_printf (dot, "%s", PFty_str (n->sem.scjoin.ty));

            break;

        case la_select:
            PFarray_printf (dot, "%s (%s)", a_id[n->kind],
                            PFatt_print (n->sem.select.att));
            break;

        case la_type:
            PFarray_printf (dot, "%s (%s:%s,%s)", a_id[n->kind],
                            PFatt_print (n->sem.type.res),
                            PFatt_print (n->sem.type.att),
                            atomtype[n->sem.type.ty]);
            break;

        case la_cast:
            PFarray_printf (dot, "%s (%s,%s)", a_id[n->kind],
                            PFatt_print (n->sem.cast.att),
                            atomtype[n->sem.cast.ty]);
            break;

        case la_num_add:
        case la_num_subtract:
        case la_num_multiply:
        case la_num_divide:
        case la_num_modulo:
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
            PFarray_printf (dot, "%s %s:(%s, %s)", a_id[n->kind],
                            PFatt_print (n->sem.binary.res),
                            PFatt_print (n->sem.binary.att1),
                            PFatt_print (n->sem.binary.att2));
            break;

        case la_num_neg:
        case la_bool_not:
            PFarray_printf (dot, "%s %s:(%s)", a_id[n->kind],
                            PFatt_print (n->sem.unary.res),
                            PFatt_print (n->sem.unary.att));
	    break;

        case la_sum:
            if (n->sem.sum.part == aat_NULL)
                PFarray_printf (dot, "%s %s:(%s)", a_id[n->kind],
                                PFatt_print (n->sem.sum.res),
                                PFatt_print (n->sem.sum.att));
            else
                PFarray_printf (dot, "%s %s:(%s)/%s", a_id[n->kind],
                                PFatt_print (n->sem.sum.res),
                                PFatt_print (n->sem.sum.att),
                                PFatt_print (n->sem.sum.part));
            break;

        case la_count:
            if (n->sem.count.part == aat_NULL)
                PFarray_printf (dot, "%s %s", a_id[n->kind],
                                PFatt_print (n->sem.count.res));
            else
                PFarray_printf (dot, "%s %s/%s", a_id[n->kind],
                                PFatt_print (n->sem.count.res),
                                PFatt_print (n->sem.count.part));
            break;

        case la_project:
            if (n->sem.proj.items[0].new != n->sem.proj.items[0].old)
                PFarray_printf (dot, "%s (%s:%s", a_id[n->kind],
                                PFatt_print (n->sem.proj.items[0].new),
                                PFatt_print (n->sem.proj.items[0].old));
            else
                PFarray_printf (dot, "%s (%s", a_id[n->kind],
                                PFatt_print (n->sem.proj.items[0].old));

            for (c = 1; c < n->sem.proj.count; c++)
                if (n->sem.proj.items[c].new != n->sem.proj.items[c].old)
                    PFarray_printf (dot, ",%s:%s",
                                    PFatt_print (n->sem.proj.items[c].new),
                                    PFatt_print (n->sem.proj.items[c].old));
                else
                    PFarray_printf (dot, ",%s", 
                                    PFatt_print (n->sem.proj.items[c].old));

            PFarray_printf (dot, ")");
            break;

        case la_rownum:
            PFarray_printf (dot, "%s (%s:<%s", a_id[n->kind],
                            PFatt_print (n->sem.rownum.attname),
                            PFatt_print (n->sem.rownum.sortby.atts[0]));

            for (c = 1; c < n->sem.rownum.sortby.count; c++)
                PFarray_printf (dot, ",%s", 
                                PFatt_print (n->sem.rownum.sortby.atts[c]));

            PFarray_printf (dot, ">");

            if (n->sem.rownum.part != aat_NULL)
                PFarray_printf (dot, "/%s", 
                                PFatt_print (n->sem.rownum.part));

            PFarray_printf (dot, ")");
            break;

        case la_seqty1:
        case la_all:
            PFarray_printf (dot, "%s (%s:%s/%s)", a_id[n->kind],
                            PFatt_print (n->sem.blngroup.res),
                            PFatt_print (n->sem.blngroup.att),
                            PFatt_print (n->sem.blngroup.part));
            break;

        case la_cross:
        case la_disjunion:
        case la_intersect:
        case la_difference:
        case la_serialize:
        case la_distinct:
        case la_element:
        case la_element_tag:
        case la_attribute:
        case la_textnode:
        case la_docnode:
        case la_comment:
        case la_processi:
        case la_concat:
        case la_merge_adjacent:
        case la_doc_access:
        case la_string_join:
        case la_roots:
        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
        case la_doc_tbl:
        case la_dummy:
            PFarray_printf (dot, "%s", a_id[n->kind]);
            break;
    }

    if (PFstate.format) {

        char *fmt = PFstate.format;

        while (*fmt) {
            switch (*fmt) {
                /* list attributes marked const if requested */
                case 'c':
                    if (n->prop)
                        for (unsigned int i = 0;
                                i < PFprop_const_count (n->prop); i++)
                            PFarray_printf (dot, i ? ", %s" : "\\nconst: %s",
                                            PFatt_print (
                                                PFprop_const_at (n->prop, i)));
                    break;
            }
            fmt++;
        }
    }

    /* close up label */
    PFarray_printf (dot, "\", color=%s ];\n", color[n->kind]);

    for (c = 0; c < PFLA_OP_MAXCHILD && n->child[c] != 0; c++) {      
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

            la_dot (dot, n->child[c], child);
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
PFla_dot (FILE *f, PFla_op_t *root)
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
                             "node [color=\"#C0C0C0\"];\n"
                             "node [fontsize=10];\n");

        la_dot (dot, root, "node1");
        /* put content of array into file */
        PFarray_printf (dot, "}\n");
        fprintf (f, "%s", (char *) dot->base);
    }
}

static void
print_tuple (PFalg_tuple_t t)
{
    unsigned int i;

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
            case aat_dec:   PFprettyprintf ("%g", t.atoms[i].val.dec);
                            break;
            case aat_dbl:   PFprettyprintf ("%g", t.atoms[i].val.dbl);
                            break;
            case aat_bln:   PFprettyprintf ("%s", t.atoms[i].val.bln ? "true"
                                            : "false");
                            break;
            case aat_qname: PFprettyprintf ("%s",
                                            PFqname_str (t.atoms[i].val.qname));
                            break;
            case aat_node:  /* PFprettyprintf ("@%i", t.atoms[i].val.node); */
            case aat_pnode:
            case aat_anode:
            case aat_pre:
            case aat_attr:
            case aat_pfrag:
            case aat_afrag:
                            PFprettyprintf ("<NODE>");
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
la_pretty (PFla_op_t *n)
{
    int c;
    unsigned int i;

    if (!n)
        return;

    PFprettyprintf ("%s", a_id[n->kind]);

    switch (n->kind)
    {
        case la_lit_tbl:
            PFprettyprintf (" <");
            for (i = 0; i < n->schema.count; i++) {
                PFprettyprintf ("(\"%s\":", 
                                PFatt_print (n->schema.items[i].name));
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

        case la_doc_tbl:
            PFprettyprintf (" <");
            for (i = 0; i < n->schema.count; i++) {
                PFprettyprintf ("(\"%s\":",
                                PFatt_print (n->schema.items[i].name));
                print_type (n->schema.items[i].type);
                PFprettyprintf (")%c", i < n->schema.count-1 ? ',' : '>');
            }
            break;

        case la_project:
            PFprettyprintf (" (");
            for (i = 0; i < n->sem.proj.count; i++)
                if (n->sem.proj.items[i].new != n->sem.proj.items[i].old)
                    PFprettyprintf ("%s:%s%c", 
                                    PFatt_print (n->sem.proj.items[i].new),
                                    PFatt_print (n->sem.proj.items[i].old),
                                    i < n->sem.proj.count-1 ? ',' : ')');
                else
                    PFprettyprintf ("%s%c", 
                                    PFatt_print (n->sem.proj.items[i].old),
                                    i < n->sem.proj.count-1 ? ',' : ')');
            break;

        case la_rownum:
            PFprettyprintf (" (%s:<", PFatt_print (n->sem.rownum.attname));
            for (i = 0; i < n->sem.rownum.sortby.count; i++)
                PFprettyprintf ("%s%c",
                                PFatt_print (n->sem.rownum.sortby.atts[i]),
                                i < n->sem.rownum.sortby.count-1 ? ',' : '>');
            if (n->sem.rownum.part != aat_NULL)
                PFprettyprintf ("/%s", PFatt_print (n->sem.rownum.part));
            PFprettyprintf (")");
            break;

        case la_seqty1:
        case la_all:
            PFprettyprintf (" (%s:%s/%s)", 
                            PFatt_print (n->sem.blngroup.res),
                            PFatt_print (n->sem.blngroup.att),
                            PFatt_print (n->sem.blngroup.part));
            break;

        case la_empty_tbl:
        case la_serialize:
        case la_cross:
        case la_disjunion:
        case la_intersect:
        case la_difference:
        case la_eqjoin:
        case la_scjoin:
        case la_select:
        case la_type:
        case la_cast:
        case la_num_add:
        case la_num_subtract:
        case la_num_multiply:
        case la_num_divide:
        case la_num_modulo:
        case la_num_eq:
        case la_num_gt:
        case la_num_neg:
        case la_bool_and:
        case la_bool_or:
        case la_bool_not:
        case la_sum:
        case la_count:
        case la_distinct:
        case la_element:
        case la_element_tag:
        case la_attribute:
        case la_textnode:
        case la_docnode:
        case la_comment:
        case la_processi:
        case la_concat:
        case la_merge_adjacent:
        case la_doc_access:
        case la_string_join:
        case la_roots:
        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
        case la_dummy:
            break;

    }

    for (c = 0; c < PFLA_OP_MAXCHILD && n->child[c] != 0; c++) {
        PFprettyprintf ("%c[", START_BLOCK);
        la_pretty (n->child[c]);
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
PFla_pretty (FILE *f, PFla_op_t *t)
{
    PFprettyprintf ("%c", START_BLOCK);
    la_pretty (t);
    PFprettyprintf ("%c", END_BLOCK);

    (void) PFprettyp (f);

    fputc ('\n', f);
}


/* vim:set shiftwidth=4 expandtab: */
