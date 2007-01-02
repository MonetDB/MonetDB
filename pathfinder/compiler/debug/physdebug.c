/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * 
 * Debugging: dump physical algebra tree in AY&T dot format.
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
#include "physdebug.h"

#include "alg_dag.h"
#include "mem.h"
/* #include "pfstrings.h" */
#include "prettyp.h"
#include "oops.h"
#include "pfstrings.h"

/** Node names to print out for all the Algebra tree nodes. */
static char *a_id[]  = {
      [pa_serialize]       = "SERIALIZE"
    , [pa_lit_tbl]         = "TBL"
    , [pa_empty_tbl]       = "EMPTY_TBL"
    , [pa_attach]          = "@"
      /* note: dot does not like the sequence "×\nfoo", so we put spaces
       * around the cross symbol.
       */
    , [pa_cross]           = " × "              /* \"#00FFFF\" */
    , [pa_leftjoin]        = "LEFTJOIN"         /* \"#00FF00\" */
    , [pa_eqjoin]          = "EQJOIN"           /* \"#00FF00\" */
    , [pa_semijoin]        = "SEMIJOIN"         /* \"#00FF00\" */
    , [pa_project]         = "¶ "
    , [pa_select]          = "SEL"
    , [pa_append_union]    = "APPEND_UNION"
    , [pa_merge_union]     = "MERGE_UNION"
    , [pa_intersect]       = "INTERSECT"        /* \"#FFA500\" */
    , [pa_difference]      = "DIFF"             /* \"#FFA500\" */
    , [pa_sort_distinct]   = "SORT_DISTINCT"
    , [pa_std_sort]        = "SORT"
    , [pa_refine_sort]     = "refine_sort"
    , [pa_num_add]         = "num-add"
    , [pa_num_add_atom]    = "num-add (atom)"
    , [pa_num_sub]         = "num-sub"
    , [pa_num_sub_atom]    = "num-sub (atom)"
    , [pa_num_mult]        = "num-mult"
    , [pa_num_mult_atom]   = "num-mult (atom)"
    , [pa_num_div]         = "num-div"
    , [pa_num_div_atom]    = "num-div (atom)"
    , [pa_num_mod]         = "num-mod"
    , [pa_num_mod_atom]    = "num-mod (atom)"
    , [pa_eq]              = "="
    , [pa_eq_atom]         = "= (atom)"
    , [pa_gt]              = ">"
    , [pa_gt_atom]         = "> (atom)"
    , [pa_num_neg]         = "-"
    , [pa_bool_and]        = "AND"
    , [pa_bool_or]         = "OR"
    , [pa_bool_not]        = "NOT"
    , [pa_bool_and_atom]   = "AND (atom)"
    , [pa_bool_or_atom]    = "OR (atom)"
    , [pa_avg]             = "AVG"
    , [pa_min]             = "MAX"
    , [pa_max]             = "MIN"
    , [pa_sum]             = "SUM"
    , [pa_hash_count]      = "HASH_COUNT"
    , [pa_number]          = "NUMBER"            /* \"#FF0000\" */
    , [pa_type]            = "TYPE"
    , [pa_type_assert]     = "type assertion"
    , [pa_cast]            = "CAST"
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
    , [pa_doc_tbl]         = "DOC"
    , [pa_doc_access]      = "access"
    , [pa_element]         = "ELEM"             /* lawn \"#00FF00\" */
    , [pa_element_tag]     = "ELEM_TAG"         /* lawn \"#00FF00\" */
    , [pa_attribute]       = "ATTR"             /* lawn \"#00FF00\" */
    , [pa_textnode]        = "TEXT"             /* lawn \"#00FF00\" */
    , [pa_docnode]         = "DOC"              /* lawn \"#00FF00\" */
    , [pa_comment]         = "COMMENT"          /* lawn \"#00FF00\" */
    , [pa_processi]        = "PI"               /* lawn \"#00FF00\" */
    , [pa_merge_adjacent]  = "#pf:merge-adjacent-text-nodes"
    , [pa_roots]           = "ROOTS"
    , [pa_fragment]        = "FRAGs"
    , [pa_frag_union]      = "FRAG_UNION"
    , [pa_empty_frag]      = "EMPTY_FRAG"
    , [pa_cond_err]        = "!ERROR"
    , [pa_rec_fix]         = "rec fix"
    , [pa_rec_param]       = "rec param"
    , [pa_rec_nil]         = "rec nil"
    , [pa_rec_arg]         = "rec arg"
    , [pa_rec_base]        = "rec base"
    , [pa_rec_border]      = "rec border"
    , [pa_concat]          = "fn:concat"
    , [pa_contains]        = "fn:contains"
    , [pa_string_join]     = "fn:string-join"
};

/** Node names to print out for all the Algebra tree nodes. */
static char *xml_id[]  = {
      [pa_serialize]       = "serialize"
    , [pa_lit_tbl]         = "tbl"
    , [pa_empty_tbl]       = "empty_tbl"
    , [pa_attach]          = "attach"
    , [pa_cross]           = "cross"
    , [pa_leftjoin]        = "leftjoin"
    , [pa_eqjoin]          = "eqjoin"
    , [pa_semijoin]        = "semijoin"
    , [pa_project]         = "project"
    , [pa_select]          = "select"
    , [pa_append_union]    = "append_union"
    , [pa_merge_union]     = "merge_union"
    , [pa_intersect]       = "intersect"
    , [pa_difference]      = "difference"
    , [pa_sort_distinct]   = "sort_distinct"
    , [pa_std_sort]        = "sort"
    , [pa_refine_sort]     = "refine_sort"
    , [pa_num_add]         = "num-add"
    , [pa_num_add_atom]    = "num-add (atom)"
    , [pa_num_sub]         = "num-sub"
    , [pa_num_sub_atom]    = "num-sub (atom)"
    , [pa_num_mult]        = "num-mult"
    , [pa_num_mult_atom]   = "num-mult (atom)"
    , [pa_num_div]         = "num-div"
    , [pa_num_div_atom]    = "num-div (atom)"
    , [pa_num_mod]         = "num-mod"
    , [pa_num_mod_atom]    = "num-mod (atom)"
    , [pa_eq]              = "eq"
    , [pa_eq_atom]         = "eq (atom)"
    , [pa_gt]              = "gt"
    , [pa_gt_atom]         = "gt (atom)"
    , [pa_num_neg]         = "neg"
    , [pa_bool_and]        = "and"
    , [pa_bool_or]         = "or"
    , [pa_bool_not]        = "not"
    , [pa_bool_and_atom]   = "and (atom)"
    , [pa_bool_or_atom]    = "or (atom)"
    , [pa_avg]             = "avg"
    , [pa_min]             = "max"
    , [pa_max]             = "min"
    , [pa_sum]             = "sum"
    , [pa_hash_count]      = "hash_count"
    , [pa_number]          = "number"
    , [pa_type]            = "type"
    , [pa_type_assert]     = "type assertion"
    , [pa_cast]            = "cast"
    , [pa_llscj_anc]       = "scjoin"
    , [pa_llscj_anc_self]  = "scjoin"
    , [pa_llscj_attr]      = "scjoin"
    , [pa_llscj_child]     = "scjoin"
    , [pa_llscj_desc]      = "scjoin"
    , [pa_llscj_desc_self] = "scjoin"
    , [pa_llscj_foll]      = "scjoin"
    , [pa_llscj_foll_sibl] = "scjoin"
    , [pa_llscj_parent]    = "scjoin"
    , [pa_llscj_prec]      = "scjoin"
    , [pa_llscj_prec_sibl] = "scjoin"
    , [pa_doc_tbl]         = "fn:doc"
    , [pa_doc_access]      = "access"
    , [pa_element]         = "elem"
    , [pa_element_tag]     = "elem_tag"
    , [pa_attribute]       = "attr"
    , [pa_textnode]        = "text"
    , [pa_docnode]         = "doc"
    , [pa_comment]         = "comment"
    , [pa_processi]        = "pi"
    , [pa_merge_adjacent]  = "#pf:merge-adjacent-text-nodes"
    , [pa_roots]           = "roots"
    , [pa_fragment]        = "frags"
    , [pa_frag_union]      = "frag_union"
    , [pa_empty_frag]      = "empty_frag"
    , [pa_cond_err]        = "!ERROR"
    , [pa_rec_fix]         = "rec_fix"
    , [pa_rec_param]       = "rec_param"
    , [pa_rec_nil]         = "rec_nil"
    , [pa_rec_arg]         = "rec_arg"
    , [pa_rec_base]        = "rec_base"
    , [pa_rec_border]      = "rec_border"
    , [pa_concat]          = "fn:concat"
    , [pa_contains]        = "fn:contains"
    , [pa_string_join]     = "fn:string-join"
};

/** string representation of algebra atomic types */
static char *atomtype[] = {
      [aat_nat]   = "nat"
    , [aat_int]   = "int"
    , [aat_str]   = "str"
    , [aat_uA]    = "uA"
    , [aat_node]  = "node"
    , [aat_anode] = "attr"
    , [aat_pnode] = "pnode"
    , [aat_dec]   = "dec"
    , [aat_dbl]   = "dbl"
    , [aat_bln]   = "bool"
    , [aat_qname] = "qname"
};

static char *
literal (PFalg_atom_t a)
{
    PFarray_t *s = PFarray (sizeof (char));

    switch (a.type) {

        case aat_nat:
            PFarray_printf (s, "#%u", a.val.nat_);
            break;

        case aat_int:
            PFarray_printf (s, "%lld", a.val.int_);
            break;
            
        case aat_str:
        case aat_uA:
            PFarray_printf (s, "\\\"%s\\\"", PFesc_string (a.val.str));
            break;

        case aat_dec:
            PFarray_printf (s, "%g", a.val.dec_);
            break;

        case aat_dbl:
            PFarray_printf (s, "%g", a.val.dbl);
            break;

        case aat_bln:
            PFarray_printf (s, a.val.bln ? "true" : "false");
            break;

        case aat_qname:
            PFarray_printf (s, "%s", PFqname_str (a.val.qname));
            break;

        default:
            PFarray_printf (s, "<node/>");
            break;
    }

    return (char *) s->base;
}

static char *
xml_literal (PFalg_atom_t a)
{
    PFarray_t *s = PFarray (sizeof (char));

    if (a.type == aat_nat)
        PFarray_printf (
           s, "<value type=\"%s\">%u</value>",
           atomtype[a.type],
           a.val.nat_);
    else if (a.type == aat_int)
        PFarray_printf (
           s, "<value type=\"%s\">%lld</value>",
           atomtype[a.type],
           a.val.int_);
    else if (a.type == aat_str || a.type == aat_uA)
        PFarray_printf (
           s, "<value type=\"%s\">%s</value>",
           atomtype[a.type],
           PFesc_string (a.val.str));
    else if (a.type == aat_dec)
        PFarray_printf (
           s, "<value type=\"%s\">%g</value>",
           atomtype[a.type],
           a.val.dec_);
    else if (a.type == aat_dbl)
        PFarray_printf (
           s, "<value type=\"%s\">%g</value>",
           atomtype[a.type],
           a.val.dbl);
    else if (a.type == aat_bln)
        PFarray_printf (
           s, "<value type=\"%s\">%s</value>",
           atomtype[a.type],
           a.val.bln ?
               "true" : "false");
    else if (a.type == aat_qname)
        PFarray_printf (s, "%s",
                PFqname_str (a.val.qname));
    else
        PFarray_printf (s, "<value type=\"node\"/>");

    return (char *) s->base;
}

/**
 * Print algebra tree in AT&T dot notation.
 * @param dot Array into which we print
 * @param n The current node to print (function is recursive)
 * @param node_id the next available node id.
 */
static unsigned int 
pa_dot (PFarray_t *dot, PFpa_op_t *n, unsigned int node_id)
{
    unsigned int c;
    assert(n->node_id);

    static char *color[] = {
          [pa_serialize]       = "\"#C0C0C0\""
        , [pa_lit_tbl]         = "\"#C0C0C0\""
        , [pa_empty_tbl]       = "\"#C0C0C0\""
        , [pa_attach]          = "\"#EEEEEE\""
        , [pa_cross]           = "\"#990000\""
        , [pa_leftjoin]        = "\"#00FF00\""
        , [pa_eqjoin]          = "\"#00FF00\""
        , [pa_semijoin]        = "\"#00FF00\""
        , [pa_project]         = "\"#EEEEEE\""
        , [pa_select]          = "\"#00DDDD\""
        , [pa_append_union]    = "\"#909090\""
        , [pa_merge_union]     = "\"#909090\""
        , [pa_intersect]       = "\"#FFA500\""
        , [pa_difference]      = "\"#FFA500\""
        , [pa_sort_distinct]   = "\"#FFA500\""
        , [pa_std_sort]        = "red"
        , [pa_refine_sort]     = "red"
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
        , [pa_eq]              = "\"#00DDDD\""
        , [pa_eq_atom]         = "\"#00DDDD\""
        , [pa_gt]              = "\"#00DDDD\""
        , [pa_gt_atom]         = "\"#00DDDD\""
        , [pa_num_neg]         = "\"#C0C0C0\""
        , [pa_bool_not]        = "\"#C0C0C0\""
        , [pa_bool_and]        = "\"#C0C0C0\""
        , [pa_bool_or]         = "\"#C0C0C0\""
        , [pa_bool_and_atom]   = "\"#C0C0C0\""
        , [pa_bool_or_atom]    = "\"#C0C0C0\""
        , [pa_avg]             = "\"#A0A0A0\""
        , [pa_max]             = "\"#A0A0A0\""
        , [pa_min]             = "\"#A0A0A0\""
        , [pa_sum]             = "\"#A0A0A0\""
        , [pa_hash_count]      = "\"#A0A0A0\""
        , [pa_number]          = "\"#FFBBBB\""
        , [pa_type]            = "\"#C0C0C0\""
        , [pa_type_assert]     = "\"#C0C0C0\""
        , [pa_cast]            = "\"#C0C0C0\""
        , [pa_llscj_anc]       = "\"#1E90FF\""
        , [pa_llscj_anc_self]  = "\"#1E90FF\""
        , [pa_llscj_attr]      = "\"#1E90FF\""
        , [pa_llscj_child]     = "\"#1E90FF\""
        , [pa_llscj_desc]      = "\"#1E90FF\""
        , [pa_llscj_desc_self] = "\"#1E90FF\""
        , [pa_llscj_foll]      = "\"#1E90FF\""
        , [pa_llscj_foll_sibl] = "\"#1E90FF\""
        , [pa_llscj_parent]    = "\"#1E90FF\""
        , [pa_llscj_prec]      = "\"#1E90FF\""
        , [pa_llscj_prec_sibl] = "\"#1E90FF\""
        , [pa_doc_tbl]         = "\"#C0C0C0\""
        , [pa_doc_access]      = "\"#CCCCFF\""
        , [pa_element]         = "\"#00FC59\""
        , [pa_element_tag]     = "\"#00FC59\""
        , [pa_attribute]       = "\"#00FC59\""
        , [pa_textnode]        = "\"#00FC59\""
        , [pa_docnode]         = "\"#00FC59\""
        , [pa_comment]         = "\"#00FC59\""
        , [pa_processi]        = "\"#00FC59\""
        , [pa_merge_adjacent]  = "\"#00D000\""
        , [pa_roots]           = "\"#E0E0E0\""
        , [pa_fragment]        = "\"#E0E0E0\""
        , [pa_frag_union]      = "\"#E0E0E0\""
        , [pa_empty_frag]      = "\"#E0E0E0\""
        , [pa_cond_err]        = "\"#C0C0C0\""
        , [pa_rec_fix]         = "\"#FF00FF\""
        , [pa_rec_param]       = "\"#FF00FF\""
        , [pa_rec_nil]         = "\"#FF00FF\""
        , [pa_rec_arg]         = "\"#BB00BB\""
        , [pa_rec_base]        = "\"#BB00BB\""
        , [pa_rec_border]      = "\"#BB00BB\""
        , [pa_concat]          = "\"#C0C0C0\""
        , [pa_contains]        = "\"#C0C0C0\""
        , [pa_string_join]     = "\"#C0C0C0\""
    };

    /* open up label */
    PFarray_printf (dot, "node%i [label=\"", n->node_id);

    /* the following line enables id printing to simplify comparison with
       generated XML plans */
    /* PFarray_printf (dot, "id: %i\\n", n->node_id); */

    /* create label */
    switch (n->kind)
    {
        case pa_lit_tbl:
            /* list the attributes of this table */
            PFarray_printf (dot, "%s: (%s", a_id[n->kind],
                            PFatt_str (n->schema.items[0].name));

            for (c = 1; c < n->schema.count;c++)
                PFarray_printf (dot, " | %s", 
                                PFatt_str (n->schema.items[c].name));

            PFarray_printf (dot, ")");

            /* print out tuples in table, if table is not empty */
            for (unsigned int d = 0; d < n->sem.lit_tbl.count; d++) {
                PFarray_printf (dot, "\\n[");
                for (c = 0; c < n->sem.lit_tbl.tuples[d].count; c++) {
                    PFarray_printf (
                            dot, "%s%s",
                            c == 0 ? "" : ",",
                            literal (n->sem.lit_tbl.tuples[d].atoms[c]));
                }
                PFarray_printf (dot, "]");
            }
            break;

        case pa_empty_tbl:
            /* list the attributes of this table */
            PFarray_printf (dot, "%s: (%s", a_id[n->kind],
                            PFatt_str (n->schema.items[0].name));

            for (c = 1; c < n->schema.count;c++)
                PFarray_printf (dot, " | %s", 
                                PFatt_str (n->schema.items[c].name));

            PFarray_printf (dot, ")");
            break;

        case pa_attach:
            PFarray_printf (dot, "%s (%s), val: %s", a_id[n->kind],
                            PFatt_str (n->sem.attach.attname),
                            literal (n->sem.attach.value));
            break;

        case pa_leftjoin:
        case pa_eqjoin:
        case pa_semijoin:
            PFarray_printf (dot, "%s: (%s= %s)", a_id[n->kind],
                            PFatt_str (n->sem.eqjoin.att1),
                            PFatt_str (n->sem.eqjoin.att2));
            break;
            
        case pa_project:
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

        case pa_select:
            PFarray_printf (dot, "%s (%s)", a_id[n->kind],
                            PFatt_str (n->sem.select.att));
            break;

        case pa_merge_union:
            PFarray_printf (dot, "%s: (%s)", a_id[n->kind],
                            PFord_str (n->sem.merge_union.ord));
            break;

        case pa_sort_distinct:
            PFarray_printf (dot, "%s: (%s)", a_id[n->kind],
                            PFord_str (n->sem.sort_distinct.ord));
            break;

        case pa_std_sort:
        case pa_refine_sort:
            PFarray_printf (dot, "%s: (%s)", a_id[n->kind],
                            PFord_str (n->sem.sortby.required));
            break;

        case pa_num_add:
        case pa_num_sub:
        case pa_num_mult:
        case pa_num_div:
        case pa_num_mod:
        case pa_eq:
        case pa_gt:
        case pa_bool_and:
        case pa_bool_or:
        case pa_concat:
        case pa_contains:
            PFarray_printf (dot, "%s (%s:<%s, %s>)", a_id[n->kind],
                            PFatt_str (n->sem.binary.res),
                            PFatt_str (n->sem.binary.att1),
                            PFatt_str (n->sem.binary.att2));
            break;

        case pa_num_add_atom:
        case pa_num_sub_atom:
        case pa_num_mult_atom:
        case pa_num_div_atom:
        case pa_num_mod_atom:
        case pa_eq_atom:
        case pa_gt_atom:
        case pa_bool_and_atom:
        case pa_bool_or_atom:
            PFarray_printf (dot, "%s (%s:<%s, %s>)", a_id[n->kind],
                            PFatt_str (n->sem.bin_atom.res),
                            PFatt_str (n->sem.bin_atom.att1),
                            literal (n->sem.bin_atom.att2));
            break;

        case pa_num_neg:
        case pa_bool_not:
            PFarray_printf (dot, "%s (%s:<%s>)", a_id[n->kind],
                            PFatt_str (n->sem.unary.res),
                            PFatt_str (n->sem.unary.att));
            break;

        case pa_hash_count:
            if (n->sem.count.part == att_NULL)
                PFarray_printf (dot, "%s (%s)", a_id[n->kind],
                                PFatt_str (n->sem.count.res));
            else
                PFarray_printf (dot, "%s (%s:/%s)", a_id[n->kind],
                                PFatt_str (n->sem.count.res),
                                PFatt_str (n->sem.count.part));
            break;

        case pa_avg:
        case pa_max:
        case pa_min:
        case pa_sum:
            if (n->sem.aggr.part == att_NULL)
                PFarray_printf (dot, "%s (%s:<%s>)", a_id[n->kind],
                                PFatt_str (n->sem.aggr.res),
                                PFatt_str (n->sem.aggr.att));
            else
                PFarray_printf (dot, "%s (%s:<%s>/%s)", a_id[n->kind],
                                PFatt_str (n->sem.aggr.res),
                                PFatt_str (n->sem.aggr.att),
                                PFatt_str (n->sem.aggr.part));
            break;

        case pa_number:
            PFarray_printf (dot, "%s (%s", a_id[n->kind],
                            PFatt_str (n->sem.number.attname));
            if (n->sem.number.part != att_NULL)
                PFarray_printf (dot, "/%s", 
                                PFatt_str (n->sem.number.part));

            PFarray_printf (dot, ")");
            break;

        case pa_type:
            if (atomtype[n->sem.type.ty])
                PFarray_printf (dot, "%s (%s:<%s>), type: %s", a_id[n->kind],
                                PFatt_str (n->sem.type.res),
                                PFatt_str (n->sem.type.att),
                                atomtype[n->sem.type.ty]);
            else
                PFarray_printf (dot, "%s (%s:<%s>), type: %i", a_id[n->kind],
                                PFatt_str (n->sem.type.res),
                                PFatt_str (n->sem.type.att),
                                n->sem.type.ty);
            break;

        case pa_type_assert:
            if (atomtype[n->sem.type_a.ty])
                PFarray_printf (dot, "%s (%s), type: %s", a_id[n->kind],
                                PFatt_str (n->sem.type_a.att),
                                atomtype[n->sem.type_a.ty]);
            else
                PFarray_printf (dot, "%s (%s), type: %i", a_id[n->kind],
                                PFatt_str (n->sem.type_a.att),
                                n->sem.type_a.ty);
                
            break;

        case pa_cast:
            PFarray_printf (dot, "%s (%s%s%s%s), type: %s", a_id[n->kind],
                            n->sem.cast.res?PFatt_str(n->sem.cast.res):"",
                            n->sem.cast.res?":<":"",
                            PFatt_str (n->sem.cast.att),
                            n->sem.cast.res?">":"",
                            atomtype[n->sem.cast.ty]);
            break;

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
            PFarray_printf (dot, "%s", a_id[n->kind]);
            PFarray_printf (dot, "::%s", PFty_str (n->sem.scjoin.ty));
            break;

        case pa_doc_access:
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

            PFarray_printf (dot, " (%s:<%s>)",
                            PFatt_str (n->sem.doc_access.res),
                            PFatt_str (n->sem.doc_access.att));
            break;

        case pa_attribute:
            PFarray_printf (dot, "%s (%s:<%s, %s>)", a_id[n->kind],
                            PFatt_str (n->sem.attr.res),
                            PFatt_str (n->sem.attr.qn),
                            PFatt_str (n->sem.attr.val));
            break;

        case pa_textnode:
            PFarray_printf (dot, "%s (%s:<%s>)", a_id[n->kind],
                            PFatt_str (n->sem.textnode.res),
                            PFatt_str (n->sem.textnode.item));
            break;

        case pa_cond_err:
            PFarray_printf (dot, "%s (%s)\\n%s ...", a_id[n->kind],
                            PFatt_str (n->sem.err.att),
                            PFstrndup (n->sem.err.str, 16));
            break;

        case pa_serialize:
        case pa_cross:
        case pa_append_union:
        case pa_intersect:
        case pa_difference:
        case pa_doc_tbl:
        case pa_element:
        case pa_element_tag:
        case pa_docnode:
        case pa_comment:
        case pa_processi:
        case pa_merge_adjacent:
        case pa_roots:
        case pa_fragment:
        case pa_frag_union:
        case pa_empty_frag:
        case pa_rec_fix:
        case pa_rec_param:
        case pa_rec_nil:
        case pa_rec_arg:
        case pa_rec_base:
        case pa_rec_border:
        case pa_string_join:
            PFarray_printf (dot, "%s", a_id[n->kind]);
            break;
    }

    if (PFstate.format) {

        char *fmt = PFstate.format;
        bool all = false;

        while (*fmt) { 
            if (*fmt == '+')
            {
                PFalg_attlist_t icols = PFprop_icols_to_attlist (n->prop);
                PFalg_attlist_t keys = PFprop_keys_to_attlist (n->prop);

                /* list costs if requested */
                PFarray_printf (dot, "\\ncost: %lu", n->cost);

                /* if present print cardinality */
                if (PFprop_card (n->prop))
                    PFarray_printf (dot, "\\ncard: %i", PFprop_card (n->prop));

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

                /* list keys attributes */
                for (unsigned int i = 0; i < keys.count; i++)
                    PFarray_printf (dot, i ? ", %s" : "\\nkeys: %s",
                                    PFatt_str (keys.atts[i]));

                /* list required value columns and their values */
                for (unsigned int pre = 0, i = 0; i < n->schema.count; i++) {
                    PFalg_att_t att = n->schema.items[i].name;
                    if (PFprop_reqval (n->prop, att))
                        PFarray_printf (
                            dot, 
                            pre++ ? ", %s=%s " : "\\nreq. val: %s=%s ",
                            PFatt_str (att),
                            PFprop_reqval_val (n->prop, att)?"true":"false");
                }

                /* list attributes and their corresponding domains */
                for (unsigned int i = 0; i < n->schema.count; i++)
                    if (PFprop_dom (n->prop, n->schema.items[i].name)) {
                        PFarray_printf (dot, i ? ", %s " : "\\ndom: %s ",
                                        PFatt_str (n->schema.items[i].name));
                        PFprop_write_domain (
                            dot, 
                            PFprop_dom (n->prop, n->schema.items[i].name));
                    }

                /* list orderings if requested */
                PFarray_printf (dot, "\\norderings:");
                for (unsigned int i = 0;
                        i < PFarray_last (n->orderings); i++)
                    PFarray_printf (
                            dot, "\\n%s",
                            PFord_str (
                                *(PFord_ordering_t *)
                                        PFarray_at (n->orderings,i)));

                all = true;
            }
            fmt++;
        }
        fmt = PFstate.format;

        while (!all && *fmt) {
            switch (*fmt) {

                /* list costs if requested */
                case 'C':
                    PFarray_printf (dot, "\\ncost: %lu", n->cost);
                    break;

                /* list attributes marked const if requested */
                case 'c':
                    for (unsigned int i = 0;
                            i < PFprop_const_count (n->prop); i++)
                        PFarray_printf (dot, i ? ", %s" : "\\nconst: %s",
                                        PFatt_str (
                                            PFprop_const_at (n->prop, i)));
                    break;

                /* list icols attributes if requested */
                case 'i':
                {
                    PFalg_attlist_t icols =
                                    PFprop_icols_to_attlist (n->prop);
                    for (unsigned int i = 0;
                            i < icols.count; i++)
                        PFarray_printf (dot, i ? ", %s" : "\\nicols: %s",
                                        PFatt_str (icols.atts[i]));
                } break;

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

    for (c = 0; c < PFPA_OP_MAXCHILD && n->child[c]; c++) {      

        /*
         * Label for child node has already been built, such that
         * only the edge between parent and child must be created
         */
        if (n->child[c]->node_id == 0)
            n->child[c]->node_id =  node_id++;

        PFarray_printf (dot, "node%i -> node%i;\n",
                        n->node_id, n->child[c]->node_id);
    }

    /* create soft links */
    switch (n->kind)
    {
        case pa_rec_arg:
            if (n->sem.rec_arg.base) {
                if (n->sem.rec_arg.base->node_id == 0)
                    n->sem.rec_arg.base->node_id = node_id++;
                
                PFarray_printf (dot, 
                                "node%i -> node%i "
                                "[style=dashed label=seed dir=back];\n",
                                n->sem.rec_arg.base->node_id,
                                n->child[0]->node_id);
                PFarray_printf (dot, 
                                "node%i -> node%i "
                                "[style=dashed label=recurse];\n",
                                n->child[1]->node_id,
                                n->sem.rec_arg.base->node_id);
            }
            break;

        default:
            break;
    }

    /* mark node visited */
    n->bit_dag = true;

    for (c = 0; c < PFPA_OP_MAXCHILD && n->child[c]; c++) {
        if (!n->child[c]->bit_dag)
            node_id = pa_dot (dot, n->child[c], node_id);
    }

    return node_id;
    /* close up label */
}

/**
 * Print physical algebra tree in XML notation.
 * @param xml Array into which we print
 * @param n The current node to print (function is recursive)
 * @param node_id the next available node id.
 */
static unsigned int 
pa_xml (PFarray_t *xml, PFpa_op_t *n, unsigned int node_id)
{
    unsigned int c;
    assert(n->node_id);

    /* open up label */
    PFarray_printf (xml,
                    "  <node id=\"%i\" kind=\"%s\">\n",
                    n->node_id,
                    xml_id[n->kind]);

    /* create label */
    switch (n->kind)
    {
        case pa_lit_tbl:
            /* list the attributes of this table */
            PFarray_printf (xml, "    <content>\n"); 

            for (c = 0; c < n->schema.count;c++) {
                PFarray_printf (xml, 
                                "      <column name=\"%s\" new=\"true\">\n",
                                PFatt_str (n->schema.items[c].name));
                /* print out tuples in table, if table is not empty */
                for (unsigned int i = 0; i < n->sem.lit_tbl.count; i++)
                    PFarray_printf (xml,
                                    "          %s\n",
                                    xml_literal (n->sem.lit_tbl
                                                       .tuples[i].atoms[c]));
                PFarray_printf (xml, "      </column>\n");
            }

            PFarray_printf (xml, "    </content>\n");
            break;

        case pa_empty_tbl:
            PFarray_printf (xml, "    <content>\n"); 
            /* list the attributes of this table */
            for (c = 0; c < n->schema.count;c++)
                PFarray_printf (xml, 
                                "      <column name=\"%s\" new=\"true\"/>\n",
                                PFatt_str (n->schema.items[c].name));

            PFarray_printf (xml, "    </content>\n");
            break;

        case pa_attach:
            PFarray_printf (xml, 
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\">\n"
                            "        %s\n"
                            "      </column>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.attach.attname),
                            xml_literal (n->sem.attach.value));
            break;

        case pa_leftjoin:
        case pa_eqjoin:
        case pa_semijoin:
            PFarray_printf (xml, 
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"false\">\n"
                            "        <annotation>first join argument"
                                    "</annotation>\n"
                            "      </column>\n"
                            "      <column name=\"%s\" new=\"false\">\n"
                            "        <annotation>second join argument"
                                    "</annotation>\n"
                            "      </column>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.eqjoin.att1),
                            PFatt_str (n->sem.eqjoin.att2));
            break;

        case pa_project:
            PFarray_printf (xml, "    <content>\n");
            for (c = 0; c < n->sem.proj.count; c++)
                if (n->sem.proj.items[c].new != n->sem.proj.items[c].old)
                    PFarray_printf (
                        xml, 
                        "      <column name=\"%s\" "
                                      "old_name=\"%s\" "
                                      "new=\"true\"/>\n",
                        PFatt_str (n->sem.proj.items[c].new),
                        PFatt_str (n->sem.proj.items[c].old));
                else
                    PFarray_printf (
                        xml, 
                        "      <column name=\"%s\" new=\"false\"/>\n",
                        PFatt_str (n->sem.proj.items[c].old));

            PFarray_printf (xml, "    </content>\n");
            break;

        case pa_select:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"false\">\n"
                            "        <annotation>select on column %s results "
                                    "in smaller relation</annotation>\n"
                            "      </column>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.select.att),
                            PFatt_str (n->sem.select.att));
            break;

        case pa_std_sort:
        case pa_refine_sort:
        {
            unsigned int i;

            PFarray_printf (xml, "    <content>\n");

            for (c = 0; c < PFord_count (n->sem.sortby.existing); c++)
                PFarray_printf (xml, 
                                "      <column name=\"%s\" direction=\"%s\""
                                        "function=\"existing sort\""
                                        " position=\"%u\" new=\"false\">\n"
                                "        <annotation>%u. existing sort "
                                        "criterion</annotation>\n"
                                "      </column>\n",
                                PFatt_str (PFord_order_col_at (
                                               n->sem.sortby.existing,
                                               c)),
                                PFord_order_dir_at (
                                    n->sem.sortby.required,
                                    c) == DIR_ASC
                                ? "ascending"
                                : "descending",
                                c+1, c+1);

            i = c;
            for (c = 0; c < PFord_count (n->sem.sortby.required); c++)
                PFarray_printf (xml, 
                                "      <column name=\"%s\" direction=\"%s\""
                                        "function=\"new sort\""
                                        " position=\"%u\" new=\"false\">\n"
                                "        <annotation>%u. sort argument"
                                        "</annotation>\n"
                                "      </column>\n",
                                PFatt_str (PFord_order_col_at (
                                               n->sem.sortby.required,
                                               c)),
                                PFord_order_dir_at (
                                    n->sem.sortby.required,
                                    c) == DIR_ASC
                                ? "ascending"
                                : "descending",
                                i+c+1, i+c+1);

            PFarray_printf (xml, "    </content>\n");
        }   break;

        case pa_num_add:
        case pa_num_sub:
        case pa_num_mult:
        case pa_num_div:
        case pa_num_mod:
        case pa_eq:
        case pa_gt:
        case pa_bool_and:
        case pa_bool_or:
        case pa_concat:
        case pa_contains:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\">\n"
                            "        <annotation>result of the operation"
                                    "</annotation>\n"
                            "      </column>\n"
                            "      <column name=\"%s\" new=\"false\">\n"
                            "        <annotation>first argument"
                                    "</annotation>\n"
                            "      </column>\n"
                            "      <column name=\"%s\" new=\"false\">\n"
                            "        <annotation>second argument"
                                    "</annotation>\n"
                            "      </column>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.binary.res),
                            PFatt_str (n->sem.binary.att1),
                            PFatt_str (n->sem.binary.att2));
            break;

        case pa_num_add_atom:
        case pa_num_sub_atom:
        case pa_num_mult_atom:
        case pa_num_div_atom:
        case pa_num_mod_atom:
        case pa_eq_atom:
        case pa_gt_atom:
        case pa_bool_and_atom:
        case pa_bool_or_atom:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\">\n"
                            "        <annotation>result of the operation"
                                    "</annotation>\n"
                            "      </column>\n"
                            "      <column name=\"%s\" new=\"false\">\n"
                            "        <annotation>first argument"
                                    "</annotation>\n"
                            "      </column>\n"
                            "      %s\n"
                            "    </content>\n",
                            PFatt_str (n->sem.bin_atom.res),
                            PFatt_str (n->sem.bin_atom.att1),
                            xml_literal (n->sem.bin_atom.att2));
            break;

        case pa_num_neg:
        case pa_bool_not:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\">\n"
                            "        <annotation>result of the operation"
                                    "</annotation>\n"
                            "      </column>\n"
                            "      <column name=\"%s\" new=\"false\">\n"
                            "        <annotation>argument"
                                    "</annotation>\n"
                            "      </column>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.unary.res),
                            PFatt_str (n->sem.unary.att));
            break;

        case pa_hash_count:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\">\n"
                            "        <annotation>result of the count operator"
                                    "</annotation>\n"
                            "      </column>\n",
                            PFatt_str (n->sem.count.res));
            if (n->sem.count.part != att_NULL)
                PFarray_printf (xml,
                            "      <column name=\"%s\" function=\"partition\""
                                    " new=\"false\">\n"
                            "        <annotation>partitioning argument"
                                    "</annotation>\n"
                            "      </column>\n",
                            PFatt_str (n->sem.count.part));
            PFarray_printf (xml, "    </content>\n");
            break;

        case pa_avg:
        case pa_max:
        case pa_min:
        case pa_sum:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\">\n"
                            "        <annotation>result of the %s operator"
                                    "</annotation>\n"
                            "      </column>\n"
                            "      <column name=\"%s\" new=\"false\">\n"
                            "        <annotation>argument for %s"
                                    "</annotation>\n"
                            "      </column>\n",
                            PFatt_str (n->sem.aggr.res),
                            xml_id[n->kind],
                            PFatt_str (n->sem.aggr.att),
                            xml_id[n->kind]);
            if (n->sem.aggr.part != att_NULL)
                PFarray_printf (xml,
                            "      <column name=\"%s\" function=\"partition\""
                                    " new=\"false\">\n"
                            "        <annotation>partitioning argument"
                                    "</annotation>\n"
                            "      </column>\n",
                            PFatt_str (n->sem.aggr.part));
            PFarray_printf (xml, "    </content>\n");
            break;

        case pa_number:
            PFarray_printf (xml, 
                            "    <content>\n" 
                            "      <column name=\"%s\" new=\"true\">\n"
                            "        <annotation>new number column"
                                    "</annotation>\n"
                            "      </column>\n",
                            PFatt_str (n->sem.number.attname));

            if (n->sem.number.part != att_NULL)
                PFarray_printf (xml,
                                "      <column name=\"%s\" function=\"partition\""
                                        " new=\"false\">\n"
                                "        <annotation>partitioning argument"
                                        "</annotation>\n"
                                "      </column>\n",
                                PFatt_str (n->sem.number.part));

            PFarray_printf (xml, "    </content>\n");
            break;

        case pa_type:
            PFarray_printf (xml, 
                            "    <content>\n" 
                            "      <column name=\"%s\" new=\"true\">\n"
                            "        <annotation>result of the type operator"
                                    "</annotation>\n"
                            "      </column>\n"
                            "      <column name=\"%s\" new=\"false\">\n"
                            "        <annotation>argument"
                                    "</annotation>\n"
                            "      </column>\n",
                            PFatt_str (n->sem.type.res),
                            PFatt_str (n->sem.type.att));

            if (atomtype[n->sem.type.ty])
                PFarray_printf (xml, 
                                "      <type name=\"%s\">\n"
                                "        <annotation>type to check"
                                        "</annotation>\n"
                                "      </type>\n",
                                atomtype[n->sem.type.ty]);
            else
                PFarray_printf (xml, 
                                "      <type name=\"%i\">\n"
                                "        <annotation>type to check"
                                        "</annotation>\n"
                                "      </type>\n",
                                n->sem.type.ty);

            PFarray_printf (xml, "    </content>\n");
            break;

        case pa_type_assert:
            PFarray_printf (xml, 
                            "    <content>\n" 
                            "      <column name=\"%s\" new=\"false\">\n"
                            "        <annotation>column to assign "
                                    "more explicit type</annotation>\n"
                            "      </column>\n",
                            PFatt_str (n->sem.type.att));

            if (atomtype[n->sem.type.ty])
                PFarray_printf (xml, 
                                "      <type name=\"%s\">\n"
                                "        <annotation>type to assign"
                                        "</annotation>\n"
                                "      </type>\n",
                                atomtype[n->sem.type.ty]);
            else
                PFarray_printf (xml, 
                                "      <type name=\"%i\">\n"
                                "        <annotation>type to assign"
                                        "</annotation>\n"
                                "      </type>\n",
                                n->sem.type.ty);

            PFarray_printf (xml, "    </content>\n");
            break;

        case pa_cast:
            PFarray_printf (xml, 
                            "    <content>\n" 
                            "      <column name=\"%s\" new=\"true\">\n"
                            "        <annotation>result of the cast operator"
                                    "</annotation>\n"
                            "      </column>\n"
                            "      <column name=\"%s\" new=\"false\">\n"
                            "        <annotation>argument"
                                    "</annotation>\n"
                            "      </column>\n"
                            "      <type name=\"%s\">\n"
                            "        <annotation>type to cast"
                                    "</annotation>\n"
                            "      </type>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.type.res),
                            PFatt_str (n->sem.type.att),
                            atomtype[n->sem.type.ty]);
            break;

        case pa_llscj_anc:
            PFarray_printf (xml, "    <content>\n      <step axis=\"");
            PFarray_printf (xml, "ancestor");
            PFarray_printf (xml,
                            "\" type=\"%s\"/>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFty_str (n->sem.scjoin.ty),
                            PFatt_str (n->sem.scjoin.iter),
                            PFatt_str (n->sem.scjoin.item));
            break;

        case pa_llscj_anc_self:
            PFarray_printf (xml, "    <content>\n      <step axis=\"");
            PFarray_printf (xml, "anc-or-self");
            PFarray_printf (xml,
                            "\" type=\"%s\"/>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFty_str (n->sem.scjoin.ty),
                            PFatt_str (n->sem.scjoin.iter),
                            PFatt_str (n->sem.scjoin.item));
            break;

        case pa_llscj_attr:
            PFarray_printf (xml, "    <content>\n      <step axis=\"");
            PFarray_printf (xml, "attribute");
            PFarray_printf (xml,
                            "\" type=\"%s\"/>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFty_str (n->sem.scjoin.ty),
                            PFatt_str (n->sem.scjoin.iter),
                            PFatt_str (n->sem.scjoin.item));
            break;

        case pa_llscj_child:
            PFarray_printf (xml, "    <content>\n      <step axis=\"");
            PFarray_printf (xml, "child");
            PFarray_printf (xml,
                            "\" type=\"%s\"/>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFty_str (n->sem.scjoin.ty),
                            PFatt_str (n->sem.scjoin.iter),
                            PFatt_str (n->sem.scjoin.item));
            break;

        case pa_llscj_desc:
            PFarray_printf (xml, "    <content>\n      <step axis=\"");
            PFarray_printf (xml, "descendant");
            PFarray_printf (xml,
                            "\" type=\"%s\"/>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFty_str (n->sem.scjoin.ty),
                            PFatt_str (n->sem.scjoin.iter),
                            PFatt_str (n->sem.scjoin.item));
            break;

        case pa_llscj_desc_self:
            PFarray_printf (xml, "    <content>\n      <step axis=\"");
            PFarray_printf (xml, "desc-or-self");
            PFarray_printf (xml,
                            "\" type=\"%s\"/>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFty_str (n->sem.scjoin.ty),
                            PFatt_str (n->sem.scjoin.iter),
                            PFatt_str (n->sem.scjoin.item));
            break;

        case pa_llscj_foll:
            PFarray_printf (xml, "    <content>\n      <step axis=\"");
            PFarray_printf (xml, "following");
            PFarray_printf (xml,
                            "\" type=\"%s\"/>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFty_str (n->sem.scjoin.ty),
                            PFatt_str (n->sem.scjoin.iter),
                            PFatt_str (n->sem.scjoin.item));
            break;

        case pa_llscj_foll_sibl:
            PFarray_printf (xml, "    <content>\n      <step axis=\"");
            PFarray_printf (xml, "fol-sibling");
            PFarray_printf (xml,
                            "\" type=\"%s\"/>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFty_str (n->sem.scjoin.ty),
                            PFatt_str (n->sem.scjoin.iter),
                            PFatt_str (n->sem.scjoin.item));
            break;

        case pa_llscj_parent:
            PFarray_printf (xml, "    <content>\n      <step axis=\"");
            PFarray_printf (xml, "parent");
            PFarray_printf (xml,
                            "\" type=\"%s\"/>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFty_str (n->sem.scjoin.ty),
                            PFatt_str (n->sem.scjoin.iter),
                            PFatt_str (n->sem.scjoin.item));
            break;

        case pa_llscj_prec:
            PFarray_printf (xml, "    <content>\n      <step axis=\"");
            PFarray_printf (xml, "preceding");
            PFarray_printf (xml,
                            "\" type=\"%s\"/>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFty_str (n->sem.scjoin.ty),
                            PFatt_str (n->sem.scjoin.iter),
                            PFatt_str (n->sem.scjoin.item));
            break;

        case pa_llscj_prec_sibl:
            PFarray_printf (xml, "    <content>\n      <step axis=\"");
            PFarray_printf (xml, "prec-sibling");
            PFarray_printf (xml,
                            "\" type=\"%s\"/>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFty_str (n->sem.scjoin.ty),
                            PFatt_str (n->sem.scjoin.iter),
                            PFatt_str (n->sem.scjoin.item));
            break;

        case pa_doc_tbl:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.ii.iter),
                            PFatt_str (n->sem.ii.item));
            break;

        case pa_doc_access:
            PFarray_printf (xml, 
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\">\n"
                            "        <annotation>result of the document access"
                                    "</annotation>\n"
                            "      </column>\n"
                            "      <column name=\"%s\" new=\"false\">\n"
                            "        <annotation>references to the document"
                                    " relation</annotation>\n"
                            "      </column>\n"
                            "      <column name=\"",
                            PFatt_str (n->sem.doc_access.res),
                            PFatt_str (n->sem.doc_access.att));

            switch (n->sem.doc_access.doc_col)
            {
                case doc_atext:
                    PFarray_printf (xml, "doc.attribute");
                    break;
                case doc_text:
                    PFarray_printf (xml, "doc.textnode");
                    break;
                case doc_comm:
                    PFarray_printf (xml, "doc.comment");
                    break;
                case doc_pi_text:
                    PFarray_printf (xml, "doc.pi");
                    break;
                default: PFoops (OOPS_FATAL,
                        "unknown document access in dot output");
            }
            PFarray_printf (xml, 
                            "\" new=\"false\">\n"
                            "        <annotation>document relation"
                                    "</annotation>\n"
                            "      </column>\n"
                            "    </content>\n");
            break;

        case pa_element:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.ii.iter),
                            PFatt_str (n->sem.ii.item));
            break;
        
        case pa_attribute:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\">\n"
                            "        <annotation>result of the attribute "
                                    "construction</annotation>\n"
                            "      </column>\n"
                            "      <column name=\"%s\" function=\"qname item\""
                                    " new=\"false\">\n"
                            "        <annotation>qname argument"
                                    "</annotation>\n"
                            "      </column>\n"
                            "      <column name=\"%s\" function=\"content item"
                                    "\" new=\"false\">\n"
                            "        <annotation>value argument"
                                    "</annotation>\n"
                            "      </column>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.attr.res),
                            PFatt_str (n->sem.attr.qn),
                            PFatt_str (n->sem.attr.val));
            break;

        case pa_textnode:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\">\n"
                            "        <annotation>result of the textnode "
                                    "construction</annotation>\n"
                            "      </column>\n"
                            "      <column name=\"%s\" new=\"false\">\n"
                            "        <annotation>value argument"
                                    "</annotation>\n"
                            "      </column>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.textnode.res),
                            PFatt_str (n->sem.textnode.item));
            break;

        case pa_merge_adjacent:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.ii.iter),
                            PFatt_str (n->sem.ii.item));
            break;
        
        case pa_cond_err:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"false\">\n"
                            "        <error>%s</error>\n"
                            "        <annotation>argument that triggers"
                                    " error message</annotation>\n"
                            "      </column>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.err.att),
                            PFstrdup (n->sem.err.str));
            break;

        case pa_string_join:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.ii.iter),
                            PFatt_str (n->sem.ii.item));
            break;
        
        default:
            break;
    }

    for (c = 0; c < PFLA_OP_MAXCHILD && n->child[c] != 0; c++) {      

        /*
         * Label for child node has already been built, such that
         * only the edge between parent and child must be created
         */
        if (n->child[c]->node_id == 0)
            n->child[c]->node_id =  node_id++;

        PFarray_printf (xml,
                        "    <edge to=\"%i\"/>\n", 
                        n->child[c]->node_id);
    }

    /* close up label */
    PFarray_printf (xml, "  </node>\n");

    /* mark node visited */
    n->bit_dag = true;

    for (c = 0; c < PFPA_OP_MAXCHILD && n->child[c]; c++) {
        if (!n->child[c]->bit_dag)
            node_id = pa_xml (xml, n->child[c], node_id);
    }

    return node_id;
    /* close up label */
}


static void
reset_node_id (PFpa_op_t *n)
{
    if (n->bit_dag)
        return;
    else
        n->bit_dag = true;

    n->node_id = 0;

    for (unsigned int c = 0; c < PFLA_OP_MAXCHILD && n->child[c]; c++)
        reset_node_id (n->child[c]);
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

        root->node_id = 1;
        pa_dot (dot, root, root->node_id + 1);
        PFpa_dag_reset (root);
        reset_node_id (root);
        PFpa_dag_reset (root);

        /* add domain subdomain relationships if required */
        if (PFstate.format) {
            char *fmt = PFstate.format;
            while (*fmt) { 
                if (*fmt == '+') {
                        PFprop_write_dom_rel_dot (dot, root->prop);
                        break;
                }
                fmt++;
            }
        }

        /* put content of array into file */
        PFarray_printf (dot, "}\n");
        fprintf (f, "%s", (char *) dot->base);
    }
}

/**
 * Dump physical algebra tree in XML format
 *
 * @param f file to dump into
 * @param root root of physical algebra tree
 */
void
PFpa_xml (FILE *f, PFpa_op_t *root)
{
    if (root) {

        /* initialize array to hold dot output */
        PFarray_t *xml = PFarray (sizeof (char));

        PFarray_printf (xml, "<physical_query_plan>\n");

        root->node_id = 1;
        pa_xml (xml, root, root->node_id + 1);
        PFpa_dag_reset (root);
        reset_node_id (root);
        PFpa_dag_reset (root);

        PFarray_printf (xml, "</physical_query_plan>\n");
        /* put content of array into file */
        fprintf (f, "%s", (char *) xml->base);
    }
}

/* vim:set shiftwidth=4 expandtab: */
