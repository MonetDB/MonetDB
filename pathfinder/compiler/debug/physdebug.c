/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * 
 * Debugging: dump physical algebra tree in AY&T dot format.
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
#include "physdebug.h"

#include "mem.h"
/* #include "pfstrings.h" */
#include "prettyp.h"
#include "oops.h"

#include "properties.h"

/** Node names to print out for all the Algebra tree nodes. */
static char *a_id[]  = {
      [pa_lit_tbl]         = "lit_tbl"
    , [pa_empty_tbl]       = "empty_tbl"
    , [pa_append_union]    = "append_union"
    , [pa_merge_union]     = "merge_union"
    , [pa_intersect]       = " \\ "
    , [pa_difference]      = "DIFF"
    , [pa_cross]           = " × "
    , [pa_attach]          = " @ "
    , [pa_project]         = "¶"
    , [pa_eqjoin]          = "EqJoin"
    , [pa_leftjoin]        = "LeftJoin"
    , [pa_sort_distinct]   = "sort_distinct"
    , [pa_std_sort]        = "std_sort"
    , [pa_refine_sort]     = "refine_sort"
    , [pa_hash_rownum]     = "HROW#"
    , [pa_merge_rownum]    = "MROW#"
    , [pa_num_add]         = "NumAdd"
    , [pa_num_add_atom]    = "NumAddAtom"
    , [pa_num_sub]         = "NumSub"
    , [pa_num_sub_atom]    = "NumSubAtom"
    , [pa_num_mult]        = "NumMult"
    , [pa_num_mult_atom]   = "NumMultAtom"
    , [pa_num_div]         = "NumDiv"
    , [pa_num_div_atom]    = "NumDivAtom"
    , [pa_num_mod]         = "NumMod"
    , [pa_num_mod_atom]    = "NumModAtom"
    , [pa_eq]              = "Eq"
    , [pa_eq_atom]         = "EqAtom"
    , [pa_gt]              = "Gt"
    , [pa_gt_atom]         = "GtAtom"
    , [pa_num_neg]         = "NumNeg"
    , [pa_bool_not]        = "BoolNeg"
    , [pa_bool_and]        = "And"
    , [pa_bool_or]         = "Or"
    , [pa_cast]            = "Cast"
    , [pa_select]          = "Select"
    , [pa_hash_count]      = "HashCount"
    , [pa_llscj_anc]       = "//| ancestor"
    , [pa_llscj_anc_self]  = "//| anc-self"
    , [pa_llscj_attr]      = "//| attr"
    , [pa_llscj_child]     = "//| child"
    , [pa_llscj_desc]      = "//| descendant"
    , [pa_llscj_desc_self] = "//| desc-self"
    , [pa_llscj_foll]      = "//| following"
    , [pa_llscj_foll_sibl] = "//| foll-sibl"
    , [pa_llscj_parent]    = "//| parent"
    , [pa_llscj_prec]      = "//| preceding"
    , [pa_llscj_prec_sibl] = "//| prec-sibl"
    , [pa_doc_tbl]         = "doc_tbl"
    , [pa_doc_access]      = "doc_access"
    , [pa_string_join]     = "string_join"
    , [pa_serialize]       = "serialize"
    , [pa_roots]           = "roots"
    , [pa_fragment]        = "fragment"
    , [pa_frag_union]      = "frag_union"
    , [pa_empty_frag]      = "empty_frag"
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
    , [aat_qname] = "qname"
};

/** Current node id */
/* static unsigned no = 0; */
/** Temporary variable to allocate mem for node names */
static char    *child;
/** Temporary variable for node labels in dot tree */
/*static char     label[32];*/

static char * literal (PFalg_atom_t a);

/**
 * Print algebra tree in AT&T dot notation.
 * @param dot Array into which we print
 * @param n The current node to print (function is recursive)
 * @param node Name of the parent node.
 */
static void 
pa_dot (PFarray_t *dot, PFpa_op_t *n, char *node)
{
    unsigned int c;
    static int node_id = 1;

    static char *color[] = {
          [pa_lit_tbl]         = "\"#C0C0C0\""
        , [pa_empty_tbl]       = "\"#C0C0C0\""
        , [pa_append_union]    = "\"#C0C0C0\""
        , [pa_merge_union]     = "\"#C0C0C0\""
        , [pa_intersect]       = "\"#C0C0C0\""
        , [pa_difference]      = "\"#C0C0C0\""
        , [pa_cross]           = "blue"
        , [pa_attach]          = "\"#C0C0C0\""
        , [pa_project]         = "\"#C0C0C0\""
        , [pa_eqjoin]          = "blue"
        , [pa_leftjoin]        = "blue"
        , [pa_sort_distinct]   = "\"#C0C0C0\""
        , [pa_std_sort]        = "red"
        , [pa_refine_sort]     = "red"
        , [pa_hash_rownum]     = "\"#C0C0C0\""
        , [pa_merge_rownum]    = "\"#C0C0C0\""
        , [pa_num_add]         = "\"#C0C0C0\""
        , [pa_num_add_atom]    = "\"#C0C0C0\""
        , [pa_num_sub]         = "\"#C0C0C0\""
        , [pa_num_sub_atom]    = "\"#C0C0C0\""
        , [pa_num_mult]        = "\"#C0C0C0\""
        , [pa_num_mult_atom]   = "\"#C0C0C0\""
        , [pa_num_div]         = "\"#C0C0C0\""
        , [pa_num_div_atom]    = "\"#C0C0C0\""
        , [pa_num_mod]         = "\"#C0C0C0\""
        , [pa_num_mod_atom]    = "\"#C0C0C0\""
        , [pa_eq]              = "\"#C0C0C0\""
        , [pa_eq_atom]         = "\"#C0C0C0\""
        , [pa_gt]              = "\"#C0C0C0\""
        , [pa_gt_atom]         = "\"#C0C0C0\""
        , [pa_num_neg]         = "\"#C0C0C0\""
        , [pa_bool_not]        = "\"#C0C0C0\""
        , [pa_bool_and]        = "\"#C0C0C0\""
        , [pa_bool_or]         = "\"#C0C0C0\""
        , [pa_cast]            = "\"#C0C0C0\""
        , [pa_select]          = "\"#C0C0C0\""
        , [pa_hash_count]      = "\"#C0C0C0\""
        , [pa_llscj_anc]       = "\"#C0C0C0\""
        , [pa_llscj_anc_self]  = "\"#C0C0C0\""
        , [pa_llscj_attr]      = "\"#C0C0C0\""
        , [pa_llscj_child]     = "\"#C0C0C0\""
        , [pa_llscj_desc]      = "\"#C0C0C0\""
        , [pa_llscj_desc_self] = "\"#C0C0C0\""
        , [pa_llscj_foll]      = "\"#C0C0C0\""
        , [pa_llscj_foll_sibl] = "\"#C0C0C0\""
        , [pa_llscj_parent]    = "\"#C0C0C0\""
        , [pa_llscj_prec]      = "\"#C0C0C0\""
        , [pa_llscj_prec_sibl] = "\"#C0C0C0\""
        , [pa_doc_tbl]         = "\"#C0C0C0\""
        , [pa_doc_access]      = "\"#C0C0C0\""
        , [pa_string_join]     = "\"#C0C0C0\""
        , [pa_serialize]       = "\"#C0C0C0\""
        , [pa_roots]           = "\"#C0C0C0\""
        , [pa_fragment]        = "\"#C0C0C0\""
        , [pa_frag_union]      = "\"#C0C0C0\""
        , [pa_empty_frag]      = "\"#C0C0C0\""
    };

    n->node_id = node_id;
    node_id++;

    /* open up label */
    PFarray_printf (dot, "%s [label=\"", node);

    /* create label */
    switch (n->kind)
    {
        case pa_lit_tbl:
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
                    PFarray_printf (
                            dot, "%s%s",
                            c == 0 ? "" : ",",
                            literal (n->sem.lit_tbl.tuples[0].atoms[c]));
                }

                PFarray_printf (dot, "]");

                /* if there is more than one tuple in the table, mark
                 * the dot node accordingly
                 */
                if (n->sem.lit_tbl.count > 1)
                    PFarray_printf (dot, "...");
            }
            break;

        case pa_empty_tbl:
            /* list the attributes of this table */
            PFarray_printf (dot, "%s: <%s", a_id[n->kind],
                            PFatt_print (n->schema.items[0].name));

            for (c = 1; c < n->schema.count;c++)
                PFarray_printf (dot, " | %s", 
                                PFatt_print (n->schema.items[c].name));

            PFarray_printf (dot, ">");
            break;

        case pa_merge_union:
            PFarray_printf (dot, "%s: %s", a_id[n->kind],
                            PFord_str (n->sem.merge_union.ord));
            break;

        case pa_attach:
            PFarray_printf (dot, "%s: <%s,%s>", a_id[n->kind],
                            PFatt_print (n->sem.attach.attname),
                            literal (n->sem.attach.value));
            break;

        case pa_num_add:
        case pa_num_sub:
        case pa_num_mult:
        case pa_num_div:
        case pa_num_mod:
        case pa_eq:
        case pa_gt:
            PFarray_printf (dot, "%s\\n%s:(%s,%s)", a_id[n->kind],
                            PFatt_print (n->sem.binary.res),
                            PFatt_print (n->sem.binary.att1),
                            PFatt_print (n->sem.binary.att2));
            break;

        case pa_num_add_atom:
        case pa_num_sub_atom:
        case pa_num_mult_atom:
        case pa_num_div_atom:
        case pa_num_mod_atom:
        case pa_eq_atom:
        case pa_gt_atom:
        case pa_bool_and:
        case pa_bool_or:
            PFarray_printf (dot, "%s\\n%s:(%s,%s)", a_id[n->kind],
                            PFatt_print (n->sem.bin_atom.res),
                            PFatt_print (n->sem.bin_atom.att1),
                            literal (n->sem.bin_atom.att2));
            break;

        case pa_num_neg:
        case pa_bool_not:
            PFarray_printf (dot, "%s\\n%s:%s", a_id[n->kind],
                            PFatt_print (n->sem.unary.res),
                            PFatt_print (n->sem.unary.att));
            break;

        case pa_cast:
            PFarray_printf (dot, "%s\\n%s -> %s", a_id[n->kind],
                            PFatt_print (n->sem.cast.att),
                            atomtype[n->sem.cast.ty]);
            break;

        case pa_hash_count:
            if (n->sem.count.part != aat_NULL)
                PFarray_printf (dot, "%s\\n%s/%s", a_id[n->kind],
                                PFatt_print (n->sem.count.res),
                                PFatt_print (n->sem.count.part));
            else
                PFarray_printf (dot, "%s\\n%s", a_id[n->kind],
                                PFatt_print (n->sem.count.res));
            break;

        case pa_append_union:
        case pa_intersect:
        case pa_difference:
        case pa_cross:
        case pa_project:
        case pa_eqjoin:
        case pa_leftjoin:
        case pa_sort_distinct:
        case pa_std_sort:
        case pa_refine_sort:
        case pa_hash_rownum:
        case pa_merge_rownum:
        case pa_llscj_anc:
        case pa_llscj_anc_self:
        case pa_llscj_attr:
        case pa_llscj_child:
        case pa_llscj_desc:
        case pa_llscj_desc_self:
        case pa_llscj_foll:
        case pa_llscj_foll_sibl:
        case pa_llscj_parent:
        case pa_llscj_prec:
        case pa_llscj_prec_sibl:
        case pa_doc_tbl:
        case pa_doc_access:
        case pa_string_join:
        case pa_serialize:
        case pa_roots:
        case pa_fragment:
        case pa_frag_union:
        case pa_empty_frag:
        case pa_select:
            PFarray_printf (dot, "%s", a_id[n->kind]);
            break;
    }

    if (PFstate.format) {

        char *fmt = PFstate.format;

        while (*fmt) {
            switch (*fmt) {

                /* list costs if requested */
                case 'C':
                    PFarray_printf (dot, "\\ncost: %lu", n->cost);
                    break;

                /* list attributes marked const if requested */
                case 'c':
                    if (n->prop)
                        for (unsigned int i = 0;
                                i < PFprop_const_count (n->prop); i++)
                            PFarray_printf (dot, i ? ", %s" : "\\nconst: %s",
                                            PFatt_print (
                                                PFprop_const_at (n->prop, i)));
                    break;

                /* list orderings if requested */
                case 'o':
                    PFarray_printf (dot, "\\norderings:");
                    for (unsigned int i = 0;
                            i < PFarray_last (n->orderings); i++)
                        PFarray_printf (
                                dot, "\\n%s",
                                PFord_str (
                                    *(PFord_ordering_t *)
                                            PFarray_at (n->orderings,i)));
                    break;
            }
            fmt++;
        }
    }

    /* close up label */
    PFarray_printf (dot, "\", color=%s ];\n", color[n->kind]);

    for (c = 0; c < PFPA_OP_MAXCHILD && n->child[c] != 0; c++) {      
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

            pa_dot (dot, n->child[c], child);
        }
    }
}

static char *
literal (PFalg_atom_t a)
{
    PFarray_t *s = PFarray (sizeof (char));

    switch (a.type) {

        case aat_nat:
            PFarray_printf (s, "#%u", a.val.nat);
            break;

        case aat_int:
            PFarray_printf (s, "%i", a.val.int_);
            break;
            
        case aat_str:
            PFarray_printf (s, "\\\"%s\\\"", a.val.str);
            break;

        case aat_dec:
            PFarray_printf (s, "%g", a.val.dec);
            break;

        case aat_dbl:
            PFarray_printf (s, "%g", a.val.dbl);
            break;

        case aat_bln:
            PFarray_printf (s, a.val.bln ? "true" : "false");
            break;

        default:
            PFarray_printf (s, "?");
            break;
    }

    return (char *) s->base;
}


/**
 * Dump physical algebra tree in AT&T dot format
 * (pipe the output through `dot -Tps' to produce a Postscript file).
 *
 * @param f file to dump into
 * @param root root of abstract syntax tree
 */
void
PFpa_dot (FILE *f, PFpa_op_t *root)
{
    if (root) {
        /* initialize array to hold dot output */
        PFarray_t *dot = PFarray (sizeof (char));

        PFarray_printf (dot, "digraph XQueryPhysicalAlgebra {\n"
                             "ordering=out;\n"
                             "node [shape=box];\n"
                             "node [height=0.1];\n"
                             "node [width=0.2];\n"
                             "node [style=filled];\n"
                             "node [color=\"#C0C0C0\"];\n"
                             "node [fontsize=10];\n");

        pa_dot (dot, root, "node1");
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
            case aat_node:
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



/* vim:set shiftwidth=4 expandtab: */
