/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * 
 * Debugging: dump logical algebra tree in AY&T dot format.
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
 * 2000-2005 University of Konstanz and (C) 2005-2006 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "pathfinder.h"
#include "logdebug.h"

#include "mem.h"
#include "prettyp.h"
#include "oops.h"

/** Node names to print out for all the Algebra tree nodes. */
static char *a_id[]  = {
      [la_serialize]        = "SERIALIZE"
    , [la_lit_tbl]          = "TBL"
    , [la_empty_tbl]        = "EMPTY_TBL"
      /* note: dot does not like the sequence "×\nfoo", so we put spaces
       * around the cross symbol.
       */
    , [la_cross]            = " × "              /* \"#00FFFF\" */
    , [la_eqjoin]           = "|X|"              /* \"#00FF00\" */
    , [la_project]          = "¶"
    , [la_select]           = "SEL"
    , [la_disjunion]        = "U"
    , [la_intersect]        = "n"
    , [la_difference]       = "DIFF"             /* \"#FFA500\" */
    , [la_distinct]         = "DISTINCT"         /* indian \"#FF0000\" */
    , [la_num_add]          = "num-add"
    , [la_num_subtract]     = "num_subtr"
    , [la_num_multiply]     = "num-mult"
    , [la_num_divide]       = "num-div"
    , [la_num_modulo]       = "num-mod"
    , [la_num_eq]           = "="
    , [la_num_gt]           = ">"
    , [la_num_neg]          = "-"
    , [la_bool_and ]        = "AND"
    , [la_bool_or ]         = "OR"
    , [la_bool_not]         = "NOT"
    , [la_sum]              = "SUM"
    , [la_count]            = "COUNT"
    , [la_rownum]           = "ROW#"              /* \"#FF0000\" */
    , [la_number]           = "NUMBER"            /* \"#FF0000\" */
    , [la_type]             = "TYPE"
    , [la_type_assert]      = "type assertion"
    , [la_cast]             = "CAST"
    , [la_seqty1]           = "SEQTY1"
    , [la_all]              = "ALL"
    , [la_scjoin]           = "/|"               /* light blue */
    , [la_doc_tbl]          = "DOC"
    , [la_doc_access]       = "access"
    , [la_element]          = "ELEM"             /* lawn \"#00FF00\" */
    , [la_element_tag]      = "ELEM_TAG"         /* lawn \"#00FF00\" */
    , [la_attribute]        = "ATTR"             /* lawn \"#00FF00\" */
    , [la_textnode]         = "TEXT"             /* lawn \"#00FF00\" */
    , [la_docnode]          = "DOCNODE"          /* lawn \"#00FF00\" */
    , [la_comment]          = "COMMENT"          /* lawn \"#00FF00\" */
    , [la_processi]         = "PI"               /* lawn \"#00FF00\" */
    , [la_merge_adjacent]   = "#pf:merge-adjacent-text-nodes"
    , [la_roots]            = "ROOTS"
    , [la_fragment]         = "FRAGs"
    , [la_frag_union]       = "FRAG_UNION"
    , [la_empty_frag]       = "EMPTY_FRAG"
    , [la_cond_err]         = "!ERROR"
    , [la_concat]           = "fn:concat"
    , [la_contains]         = "fn:contains"
    , [la_string_join]      = "fn:string_join"
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
          [la_serialize]      = "\"#C0C0C0\""
        , [la_lit_tbl]        = "\"#C0C0C0\""
        , [la_empty_tbl]      = "\"#C0C0C0\""
        , [la_cross]          = "\"#00FFFF\""
        , [la_eqjoin]         = "\"#00FF00\""
        , [la_project]        = "\"#D0D0D0\""
        , [la_select]         = "\"#C0C0C0\""
        , [la_disjunion]      = "\"#C0C0C0\""
        , [la_intersect]      = "\"#C0C0C0\""
        , [la_difference]     = "\"#FFA500\""
        , [la_distinct]       = "\"#CD5C5C\""
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
        , [la_rownum]         = "\"#FF0000\""
        , [la_number]         = "\"#FF0000\""
        , [la_type]           = "\"#C0C0C0\""
        , [la_type_assert]    = "\"#C0C0C0\""
        , [la_cast]           = "\"#C0C0C0\""
        , [la_seqty1]         = "\"#C0C0C0\""
        , [la_all]            = "\"#C0C0C0\""
        , [la_scjoin]         = "\"#1E90FF\""
        , [la_doc_tbl]        = "\"#C0C0C0\""
        , [la_doc_access]     = "\"#C0C0C0\""
        , [la_element]        = "\"#00FC59\""
        , [la_element_tag]    = "\"#00FC59\""
        , [la_attribute]      = "\"#00FC59\""
        , [la_textnode]       = "\"#00FC59\""
        , [la_docnode]        = "\"#00FC59\""
        , [la_comment]        = "\"#00FC59\""
        , [la_processi]       = "\"#00FC59\""
        , [la_merge_adjacent] = "\"#00D000\""
        , [la_roots]          = "\"#E0E0E0\""
        , [la_fragment]       = "\"#E0E0E0\""
        , [la_frag_union]     = "\"#E0E0E0\""
        , [la_empty_frag]     = "\"#E0E0E0\""
        , [la_cond_err]       = "\"#C0C0C0\""
        , [la_concat]         = "\"#C0C0C0\""
        , [la_contains]       = "\"#C0C0C0\""
        , [la_string_join]    = "\"#C0C0C0\""
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
                            PFatt_str (n->schema.items[0].name));

            for (c = 1; c < n->schema.count;c++)
                PFarray_printf (dot, " | %s",
                                PFatt_str (n->schema.items[c].name));

            PFarray_printf (dot, ">");

            /* print out tuples in table, if table is not empty */
            for (unsigned int i = 0; i < n->sem.lit_tbl.count; i++) {
                PFarray_printf (dot, "\\n[");

                for (c = 0; c < n->sem.lit_tbl.tuples[i].count; c++) {
                    if (c != 0)
                        PFarray_printf (dot, ",");

                    if (n->sem.lit_tbl.tuples[i].atoms[c].special == amm_min)
                        PFarray_printf (dot, "MIN");
                    else if (n->sem.lit_tbl.tuples[i].atoms[c].special ==
                             amm_max)
                        PFarray_printf (dot, "MAX");
                    else if (n->sem.lit_tbl.tuples[i].atoms[c].type == aat_nat)
                        PFarray_printf (dot, "#%u",
                                n->sem.lit_tbl.tuples[i].atoms[c].val.nat);
                    else if (n->sem.lit_tbl.tuples[i].atoms[c].type == aat_int)
                        PFarray_printf (dot, "%i",
                                n->sem.lit_tbl.tuples[i].atoms[c].val.int_);
                    else if (n->sem.lit_tbl.tuples[i].atoms[c].type == aat_str)
                        PFarray_printf (dot, "\\\"%s\\\"",
                                n->sem.lit_tbl.tuples[i].atoms[c].val.str);
                    else if (n->sem.lit_tbl.tuples[i].atoms[c].type == aat_dec)
                        PFarray_printf (dot, "%g",
                                n->sem.lit_tbl.tuples[i].atoms[c].val.dec);
                    else if (n->sem.lit_tbl.tuples[i].atoms[c].type == aat_dbl)
                        PFarray_printf (dot, "%g",
                                n->sem.lit_tbl.tuples[i].atoms[c].val.dbl);
                    else if (n->sem.lit_tbl.tuples[i].atoms[c].type == aat_bln)
                        PFarray_printf (dot, "%s",
                                n->sem.lit_tbl.tuples[i].atoms[c].val.bln ?
                                        "true" : "false");
                    else if (n->sem.lit_tbl.tuples[i].atoms[c].type 
                             == aat_qname)
                        PFarray_printf (dot, "%s",
                                PFqname_str (n->sem.lit_tbl.tuples[i]
                                                    .atoms[c].val.qname));
                    else
                        PFarray_printf (dot, "<NODE>");
                }

                PFarray_printf (dot, "]");
            }
            break;

        case la_empty_tbl:
            /* list the attributes of this table */
            PFarray_printf (dot, "%s: <%s", a_id[n->kind],
                            PFatt_str (n->schema.items[0].name));

            for (c = 1; c < n->schema.count;c++)
                PFarray_printf (dot, " | %s", 
                                PFatt_str (n->schema.items[c].name));

            PFarray_printf (dot, ">");
            break;

        case la_eqjoin:
            PFarray_printf (dot, "%s (%s=%s)",
                            a_id[n->kind], 
                            PFatt_str (n->sem.eqjoin.att1),
                            PFatt_str (n->sem.eqjoin.att2));
            break;

        case la_project:
            if (n->sem.proj.items[0].new != n->sem.proj.items[0].old)
                PFarray_printf (dot, "%s (%s:%s", a_id[n->kind],
                                PFatt_str (n->sem.proj.items[0].new),
                                PFatt_str (n->sem.proj.items[0].old));
            else
                PFarray_printf (dot, "%s (%s", a_id[n->kind],
                                PFatt_str (n->sem.proj.items[0].old));

            for (c = 1; c < n->sem.proj.count; c++)
                if (n->sem.proj.items[c].new != n->sem.proj.items[c].old)
                    PFarray_printf (dot, ",%s:%s",
                                    PFatt_str (n->sem.proj.items[c].new),
                                    PFatt_str (n->sem.proj.items[c].old));
                else
                    PFarray_printf (dot, ",%s", 
                                    PFatt_str (n->sem.proj.items[c].old));

            PFarray_printf (dot, ")");
            break;

        case la_select:
            PFarray_printf (dot, "%s (%s)", a_id[n->kind],
                            PFatt_str (n->sem.select.att));
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
        case la_concat:
        case la_contains:
            PFarray_printf (dot, "%s %s:(%s, %s)", a_id[n->kind],
                            PFatt_str (n->sem.binary.res),
                            PFatt_str (n->sem.binary.att1),
                            PFatt_str (n->sem.binary.att2));
            break;

        case la_num_neg:
        case la_bool_not:
            PFarray_printf (dot, "%s %s:(%s)", a_id[n->kind],
                            PFatt_str (n->sem.unary.res),
                            PFatt_str (n->sem.unary.att));
	    break;

        case la_sum:
            if (n->sem.sum.part == att_NULL)
                PFarray_printf (dot, "%s %s:(%s)", a_id[n->kind],
                                PFatt_str (n->sem.sum.res),
                                PFatt_str (n->sem.sum.att));
            else
                PFarray_printf (dot, "%s %s:(%s)/%s", a_id[n->kind],
                                PFatt_str (n->sem.sum.res),
                                PFatt_str (n->sem.sum.att),
                                PFatt_str (n->sem.sum.part));
            break;

        case la_count:
            if (n->sem.count.part == att_NULL)
                PFarray_printf (dot, "%s %s", a_id[n->kind],
                                PFatt_str (n->sem.count.res));
            else
                PFarray_printf (dot, "%s %s/%s", a_id[n->kind],
                                PFatt_str (n->sem.count.res),
                                PFatt_str (n->sem.count.part));
            break;

        case la_rownum:
            PFarray_printf (dot, "%s (%s:<%s", a_id[n->kind],
                            PFatt_str (n->sem.rownum.attname),
                            PFatt_str (n->sem.rownum.sortby.atts[0]));

            for (c = 1; c < n->sem.rownum.sortby.count; c++)
                PFarray_printf (dot, ",%s", 
                                PFatt_str (n->sem.rownum.sortby.atts[c]));

            PFarray_printf (dot, ">");

            if (n->sem.rownum.part != att_NULL)
                PFarray_printf (dot, "/%s", 
                                PFatt_str (n->sem.rownum.part));

            PFarray_printf (dot, ")");
            break;

        case la_number:
            PFarray_printf (dot, "%s (%s", a_id[n->kind],
                            PFatt_str (n->sem.number.attname));
            if (n->sem.number.part != att_NULL)
                PFarray_printf (dot, "/%s", 
                                PFatt_str (n->sem.number.part));

            PFarray_printf (dot, ")");
            break;

        case la_type:
            if (atomtype[n->sem.type.ty])
                PFarray_printf (dot, "%s (%s:%s,%s)", a_id[n->kind],
                                PFatt_str (n->sem.type.res),
                                PFatt_str (n->sem.type.att),
                                atomtype[n->sem.type.ty]);
            else
                PFarray_printf (dot, "%s (%s:%s,%i)", a_id[n->kind],
                                PFatt_str (n->sem.type.res),
                                PFatt_str (n->sem.type.att),
                                n->sem.type.ty);
            break;

        case la_type_assert:
            if (atomtype[n->sem.type_a.ty])
                PFarray_printf (dot, "%s (%s,%s)", a_id[n->kind],
                                PFatt_str (n->sem.type_a.att),
                                atomtype[n->sem.type_a.ty]);
            else
                PFarray_printf (dot, "%s (%s,%i)", a_id[n->kind],
                                PFatt_str (n->sem.type_a.att),
                                n->sem.type_a.ty);
                
            break;

        case la_cast:
            PFarray_printf (dot, "%s (%s%s%s,%s)", a_id[n->kind],
                            n->sem.cast.res?PFatt_str(n->sem.cast.res):"",
                            n->sem.cast.res?":":"",
                            PFatt_str (n->sem.cast.att),
                            atomtype[n->sem.cast.ty]);
            break;

        case la_seqty1:
        case la_all:
            PFarray_printf (dot, "%s (%s:%s/%s)", a_id[n->kind],
                            PFatt_str (n->sem.blngroup.res),
                            PFatt_str (n->sem.blngroup.att),
                            PFatt_str (n->sem.blngroup.part));
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

        case la_doc_access:
            PFarray_printf (dot, "%s ", a_id[n->kind]);

            switch (n->sem.doc_access.doc_col)
            {
                case doc_atext:
                    PFarray_printf (dot, "attribute value");
                    break;
                case doc_text:
                    PFarray_printf (dot, "textnode content");
                    break;
                case doc_comm:
                    PFarray_printf (dot, "comment text");
                    break;
                case doc_pi_text:
                    PFarray_printf (dot, "processing instruction");
                    break;
                default: PFoops (OOPS_FATAL,
                        "unknown document access in dot output");
            }

            PFarray_printf (dot, "\\%s (%s)",
                            PFatt_str (n->sem.doc_access.res),
                            PFatt_str (n->sem.doc_access.att));
            break;

        case la_attribute:
            PFarray_printf (dot, "%s (%s:%s,%s)", a_id[n->kind],
                            PFatt_str (n->sem.attr.res),
                            PFatt_str (n->sem.attr.qn),
                            PFatt_str (n->sem.attr.val));
            break;

        case la_textnode:
            PFarray_printf (dot, "%s (%s:%s)", a_id[n->kind],
                            PFatt_str (n->sem.textnode.res),
                            PFatt_str (n->sem.textnode.item));
            break;

        case la_cond_err:
            PFarray_printf (dot, "%s (%s)\\n%s ...", a_id[n->kind],
                            PFatt_str (n->sem.err.att),
                            PFstrndup (n->sem.err.str, 16));
            break;

        case la_serialize:
        case la_cross:
        case la_disjunion:
        case la_intersect:
        case la_difference:
        case la_distinct:
        case la_doc_tbl:
        case la_element:
        case la_element_tag:
        case la_docnode:
        case la_comment:
        case la_processi:
        case la_merge_adjacent:
        case la_roots:
        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
        case la_string_join:
            PFarray_printf (dot, "%s", a_id[n->kind]);
            break;
    }

    if (PFstate.format) {

        char *fmt = PFstate.format;
        bool all = false;

        while (*fmt) { 
            if (*fmt == '+')
            {
                if (n->prop)
                {
                    PFalg_attlist_t icols = PFprop_icols_to_attlist (n->prop);

                    /* list attributes marked const */
                    for (unsigned int i = 0;
                            i < PFprop_const_count (n->prop); i++)
                        PFarray_printf (dot, i ? ", %s" : "\\nconst: %s",
                                        PFatt_str (
                                            PFprop_const_at (n->prop, i)));
                    /* list icols attributes */
                    for (unsigned int i = 0; i < icols.count; i++)
                        PFarray_printf (dot, i ? ", %s" : "\\nicols: %s",
                                        PFatt_str (icols.atts[i]));
                }
                all = true;
            }
            fmt++;
        }
        fmt = PFstate.format;

        while (!all && *fmt) {
            switch (*fmt) {
                /* list attributes marked const if requested */
                case 'c':
                    if (n->prop)
                        for (unsigned int i = 0;
                                i < PFprop_const_count (n->prop); i++)
                            PFarray_printf (dot, i ? ", %s" : "\\nconst: %s",
                                            PFatt_str (
                                                PFprop_const_at (n->prop, i)));
                    break;
                /* list icols attributes if requested */
                case 'i':
                    if (n->prop) {
                        PFalg_attlist_t icols = 
                                        PFprop_icols_to_attlist (n->prop);
                        for (unsigned int i = 0; i < icols.count; i++)
                            PFarray_printf (dot, i ? ", %s" : "\\nicols: %s",
                                            PFatt_str (icols.atts[i]));
                    }
                    break;
            }
            fmt++;
        }
    }

    /* close up label */
    PFarray_printf (dot, "\", color=%s ];\n", color[n->kind]);

    for (c = 0; c < PFLA_OP_MAXCHILD && n->child[c] != 0; c++) {      
        char *child = (char *) PFmalloc (sizeof ("node4294967296"));

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

/* vim:set shiftwidth=4 expandtab: */
