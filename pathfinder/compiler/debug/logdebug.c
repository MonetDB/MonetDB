/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * 
 * Debugging: dump logical algebra tree in AT&T dot format.
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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "logdebug.h"

#include "alg_dag.h"
#include "mem.h"
#include "prettyp.h"
#include "oops.h"
#include "pfstrings.h"

/** Node names to print out for all the Algebra tree nodes. */
static char *a_id[]  = {
      [la_serialize_seq]    = "SERIALIZE"
    , [la_serialize_rel]    = "REL SERIALIZE"
    , [la_lit_tbl]          = "TBL"
    , [la_empty_tbl]        = "EMPTY_TBL"
    , [la_attach]           = "Attach"
    , [la_cross]            = "Cross"
    , [la_eqjoin]           = "Join"
    , [la_eqjoin_unq]       = "Join"
    , [la_semijoin]         = "SemiJoin"
    , [la_thetajoin]        = "ThetaJoin"
    , [la_project]          = "Project"
    , [la_select]           = "Select"
    , [la_pos_select]       = "PosSelect"
    , [la_disjunion]        = "UNION"
    , [la_intersect]        = "INTERSECT"
    , [la_difference]       = "DIFF"
    , [la_distinct]         = "DISTINCT"
    , [la_fun_1to1]         = "1:1 fun"
    , [la_num_eq]           = "="
    , [la_num_gt]           = ">"
    , [la_bool_and ]        = "AND"
    , [la_bool_or ]         = "OR"
    , [la_bool_not]         = "NOT"
    , [la_to]               = "op:to"
    , [la_avg]              = "AVG"
    , [la_max]              = "MAX"
    , [la_min]              = "MIN"
    , [la_sum]              = "SUM"
    , [la_count]            = "COUNT"
    , [la_rownum]           = "ROW#"
    , [la_rank]             = "RANK"
    , [la_number]           = "NUMBER"
    , [la_type]             = "TYPE"
    , [la_type_assert]      = "type assertion"
    , [la_cast]             = "CAST"
    , [la_seqty1]           = "SEQTY1"
    , [la_all]              = "ALL"
    , [la_step]             = "/|"
    , [la_step_join]        = "/|+"
    , [la_guide_step]       = "/| (guide)"
    , [la_guide_step_join]  = "/|+ (guide)"
    , [la_doc_index_join]   = "DocJoin"
    , [la_doc_tbl]          = "DOC"
    , [la_doc_access]       = "access"
    , [la_twig]             = "twig"
    , [la_fcns]             = "fcns"
    , [la_docnode]          = "DOCNODE"
    , [la_element]          = "ELEM"
    , [la_attribute]        = "ATTR"
    , [la_textnode]         = "TEXT"
    , [la_comment]          = "COMMENT"
    , [la_processi]         = "PI"
    , [la_content]          = "CONTENT"
    , [la_merge_adjacent]   = "#pf:merge-adjacent-text-nodes"
    , [la_roots]            = "ROOTS"
    , [la_fragment]         = "FRAGs"
    , [la_frag_union]       = "FRAG_UNION"
    , [la_empty_frag]       = "EMPTY_FRAG"
    , [la_cond_err]         = "!ERROR"
    , [la_nil]              = "nil"
    , [la_trace]            = "trace"
    , [la_trace_msg]        = "trace_msg"
    , [la_trace_map]        = "trace_map"
    , [la_rec_fix]          = "rec fix"
    , [la_rec_param]        = "rec param"
    , [la_rec_arg]          = "rec arg"
    , [la_rec_base]         = "rec base"
    , [la_proxy]            = "PROXY"
    , [la_proxy_base]       = "PROXY_BASE"
    , [la_string_join]      = "fn:string_join"
    , [la_dummy]            = "DUMMY"
};

/* XML node names to print out for all kinds */
static char *xml_id[]  = {
      [la_serialize_seq]    = "serialize sequence"
    , [la_serialize_rel]    = "serialize relation"
    , [la_lit_tbl]          = "table"
    , [la_empty_tbl]        = "empty_tbl"
    , [la_attach]           = "attach"
    , [la_cross]            = "cross"
    , [la_eqjoin]           = "eqjoin"
    , [la_eqjoin_unq]       = "eqjoin_unq"
    , [la_semijoin]         = "semijoin"
    , [la_thetajoin]        = "thetajoin"
    , [la_project]          = "project"
    , [la_select]           = "select"
    , [la_pos_select]       = "pos_select"
    , [la_disjunion]        = "union"
    , [la_intersect]        = "intersect"
    , [la_difference]       = "difference"
    , [la_distinct]         = "distinct"
    , [la_fun_1to1]         = "fun"
    , [la_num_eq]           = "eq"
    , [la_num_gt]           = "gt"
    , [la_bool_and ]        = "and"
    , [la_bool_or ]         = "or"
    , [la_bool_not]         = "not"
    , [la_to]               = "op:to"
    , [la_avg]              = "avg"
    , [la_max]              = "max"
    , [la_min]              = "min"
    , [la_sum]              = "sum"
    , [la_count]            = "count"
    , [la_rownum]           = "rownum"
    , [la_rank]             = "rank"
    , [la_number]           = "number"
    , [la_type]             = "type"
    , [la_type_assert]      = "type assertion"
    , [la_cast]             = "cast"
    , [la_seqty1]           = "seqty1"
    , [la_all]              = "all"
    , [la_step]             = "XPath step"
    , [la_step_join]        = "path step join"
    , [la_guide_step]       = "XPath step (with guide information)"
    , [la_guide_step_join]  = "path step join (with guide information)"
    , [la_doc_index_join]   = "document index join"
    , [la_doc_tbl]          = "fn:doc"
    , [la_doc_access]       = "#pf:string-value"
    , [la_twig]             = "twig_construction"
    , [la_fcns]             = "constructor_sequence_(fcns)"
    , [la_docnode]          = "documentnode_construction"
    , [la_element]          = "element_construction"
    , [la_attribute]        = "attribute_construction"
    , [la_textnode]         = "textnode_construction"
    , [la_comment]          = "comment_construction"
    , [la_processi]         = "pi_construction"
    , [la_content]          = "constructor_content"
    , [la_merge_adjacent]   = "#pf:merge-adjacent-text-nodes"
    , [la_roots]            = "ROOTS"
    , [la_fragment]         = "FRAG"
    , [la_frag_union]       = "FRAG_UNION"
    , [la_empty_frag]       = "EMPTY_FRAG"
    , [la_cond_err]         = "error"
    , [la_nil]              = "nil"
    , [la_trace]            = "trace"
    , [la_trace_msg]        = "trace msg"
    , [la_trace_map]        = "trace map"
    , [la_rec_fix]          = "recursion fix"
    , [la_rec_param]        = "recursion param"
    , [la_rec_arg]          = "recursion arg"
    , [la_rec_base]         = "recursion base"
    , [la_proxy]            = "proxy"
    , [la_proxy_base]       = "proxy base"
    , [la_string_join]      = "fn:string-join"
    , [la_dummy]            = "dummy"
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
            PFarray_printf (s, LLFMT, a.val.int_);
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
           PFalg_simple_type_str (a.type),
           a.val.nat_);
    else if (a.type == aat_int)
        PFarray_printf (
           s, "<value type=\"%s\">" LLFMT "</value>",
           PFalg_simple_type_str (a.type),
           a.val.int_);
    else if (a.type == aat_str || a.type == aat_uA)
        PFarray_printf (
           s, "<value type=\"%s\">%s</value>",
           PFalg_simple_type_str (a.type),
           PFesc_string (a.val.str));
    else if (a.type == aat_dec)
        PFarray_printf (
           s, "<value type=\"%s\">%g</value>",
           PFalg_simple_type_str (a.type),
           a.val.dec_);
    else if (a.type == aat_dbl)
        PFarray_printf (
           s, "<value type=\"%s\">%g</value>",
           PFalg_simple_type_str (a.type),
           a.val.dbl);
    else if (a.type == aat_bln)
        PFarray_printf (
           s, "<value type=\"%s\">%s</value>",
           PFalg_simple_type_str (a.type),
           a.val.bln ?
               "true" : "false");
    else if (a.type == aat_qname)
        PFarray_printf (
           s, "<value type=\"%s\">%s</value>",
           PFalg_simple_type_str (a.type),
           PFqname_str (a.val.qname));
    else
        PFarray_printf (s, "<value type=\"node\"/>");

    return (char *) s->base;
}

static char *
comp_str (PFalg_comp_t comp) {
    switch (comp) {
        case alg_comp_eq: return "eq";
        case alg_comp_gt: return "gt";
        case alg_comp_ge: return "ge";
        case alg_comp_lt: return "lt";
        case alg_comp_le: return "le";
        case alg_comp_ne: return "ne";
    }
    assert (0);
    return NULL;
}

/**
 * Print algebra tree in AT&T dot notation.
 * @param dot Array into which we print
 * @param n The current node to print (function is recursive)
 */
static void
la_dot (PFarray_t *dot, PFla_op_t *n)
{
    unsigned int c;
    assert(n->node_id);

    static char *color[] = {
          [la_serialize_seq]   = "#C0C0C0"
        , [la_serialize_rel]   = "#C0C0C0"
        , [la_lit_tbl]         = "#C0C0C0"
        , [la_empty_tbl]       = "#C0C0C0"
        , [la_attach]          = "#EEEEEE"
        , [la_cross]           = "#990000"
        , [la_eqjoin]          = "#00FF00"
        , [la_eqjoin_unq]      = "#00CC00"
        , [la_semijoin]        = "#009900"
        , [la_thetajoin]       = "#00AA00"
        , [la_project]         = "#EEEEEE"
        , [la_select]          = "#00DDDD"
        , [la_pos_select]      = "#CC2222"
        , [la_disjunion]       = "#909090"
        , [la_intersect]       = "#FFA500"
        , [la_difference]      = "#FFA500"
        , [la_distinct]        = "#FFA500"
        , [la_fun_1to1]        = "#C0C0C0"
        , [la_num_eq]          = "#00DDDD"
        , [la_num_gt]          = "#00DDDD"
        , [la_bool_and ]       = "#C0C0C0"
        , [la_bool_or ]        = "#C0C0C0"
        , [la_bool_not]        = "#C0C0C0"
        , [la_to]              = "#C0C0C0"
        , [la_avg]             = "#A0A0A0"
        , [la_max]             = "#A0A0A0"
        , [la_min]             = "#A0A0A0"
        , [la_sum]             = "#A0A0A0"
        , [la_count]           = "#A0A0A0"
        , [la_rownum]          = "#FF0000"
        , [la_rank]            = "#FF3333"
        , [la_number]          = "#FF9999"
        , [la_type]            = "#C0C0C0"
        , [la_type_assert]     = "#C0C0C0"
        , [la_cast]            = "#C0C0C0"
        , [la_seqty1]          = "#C0C0C0"
        , [la_all]             = "#C0C0C0"
        , [la_step]            = "#1E90FF"
        , [la_step_join]       = "#1E9099"
        , [la_guide_step]      = "#007AE0"
        , [la_guide_step_join] = "#007A7A"
        , [la_doc_index_join]  = "#1E9099"
        , [la_doc_tbl]         = "#C0C0C0"
        , [la_doc_access]      = "#CCCCFF"
        , [la_twig]            = "#00AA44"
        , [la_fcns]            = "#009959"
        , [la_docnode]         = "#00CC59"
        , [la_element]         = "#00CC59"
        , [la_attribute]       = "#00CC59"
        , [la_textnode]        = "#00CC59"
        , [la_comment]         = "#00CC59"
        , [la_processi]        = "#00CC59"
        , [la_content]         = "#00CC59"
        , [la_merge_adjacent]  = "#00D000"
        , [la_roots]           = "#E0E0E0"
        , [la_fragment]        = "#E0E0E0"
        , [la_frag_union]      = "#E0E0E0"
        , [la_empty_frag]      = "#E0E0E0"
        , [la_cond_err]        = "#C0C0C0"
        , [la_nil]             = "#FFFFFF"
        , [la_trace]           = "#FF5500"
        , [la_trace_msg]       = "#FF5500"
        , [la_trace_map]       = "#FF5500"
        , [la_rec_fix]         = "#FF00FF"
        , [la_rec_param]       = "#FF00FF"
        , [la_rec_arg]         = "#BB00BB"
        , [la_rec_base]        = "#BB00BB"
        , [la_proxy]           = "#DFFFFF"
        , [la_proxy_base]      = "#DFFFFF"
        , [la_string_join]     = "#C0C0C0"
        , [la_dummy]           = "#FFFFFF"
    };

    /* open up label */
    PFarray_printf (dot, "node%i [label=\"", n->node_id);

    /* the following line enables id printing to simplify comparison with
       generated XML plans */
    /* PFarray_printf (dot, "id: %i\\n", n->node_id); */

    /* create label */
    switch (n->kind)
    {
        case la_serialize_seq:
            PFarray_printf (dot, "%s (%s) order by (%s)",
                            a_id[n->kind], 
                            PFatt_str (n->sem.ser_seq.item),
                            PFatt_str (n->sem.ser_seq.pos));
            break;
            
        case la_serialize_rel:
            PFarray_printf (dot, "%s (%s",
                            a_id[n->kind],
                            PFatt_str (n->sem.ser_rel.items.atts[0]));
            for (c = 1; c < n->sem.ser_rel.items.count; c++)
                PFarray_printf (dot, ", %s",
                                PFatt_str (n->sem.ser_rel.items.atts[c])); 
            PFarray_printf (dot, ")\\norder by (%s) partition by (%s)",
                            PFatt_str (n->sem.ser_rel.pos),
                            PFatt_str (n->sem.ser_rel.iter));
            break;
            
        case la_lit_tbl:
            /* list the attributes of this table */
            PFarray_printf (dot, "%s: (%s", a_id[n->kind],
                            PFatt_str (n->schema.items[0].name));

            for (c = 1; c < n->schema.count;c++)
                PFarray_printf (dot, " | %s",
                                PFatt_str (n->schema.items[c].name));

            PFarray_printf (dot, ")");

            /* print out tuples in table, if table is not empty */
            for (unsigned int i = 0; i < n->sem.lit_tbl.count; i++) {
                PFarray_printf (dot, "\\n[");

                for (c = 0; c < n->sem.lit_tbl.tuples[i].count; c++) {
                    if (c != 0)
                        PFarray_printf (dot, ",");
                    PFarray_printf (dot, "%s", 
                                    literal (n->sem.lit_tbl
                                                   .tuples[i].atoms[c]));
                }

                PFarray_printf (dot, "]");
            }
            break;

        case la_empty_tbl:
            /* list the attributes of this table */
            PFarray_printf (dot, "%s: (%s", a_id[n->kind],
                            PFatt_str (n->schema.items[0].name));

            for (c = 1; c < n->schema.count;c++)
                PFarray_printf (dot, " | %s", 
                                PFatt_str (n->schema.items[c].name));

            PFarray_printf (dot, ")");
            break;

        case la_attach:
            PFarray_printf (dot, "%s (%s), val: %s", a_id[n->kind],
                            PFatt_str (n->sem.attach.res),
                            literal (n->sem.attach.value));
            break;

        case la_eqjoin:
        case la_semijoin:
            PFarray_printf (dot, "%s (%s = %s)",
                            a_id[n->kind], 
                            PFatt_str (n->sem.eqjoin.att1),
                            PFatt_str (n->sem.eqjoin.att2));
            break;

        case la_thetajoin:
            /* overwrite standard node layout */
            PFarray_printf (dot, "\", shape=polygon peripheries=2, label=\"");
            
            PFarray_printf (dot, "%s", a_id[n->kind]);

            for (c = 0; c < n->sem.thetajoin.count; c++)
                PFarray_printf (dot, "\\n(%s %s %s)",
                                PFatt_str (n->sem.thetajoin.pred[c].left),
                                comp_str (n->sem.thetajoin.pred[c].comp),
                                PFatt_str (n->sem.thetajoin.pred[c].right));
            break;
            
        case la_eqjoin_unq:
            PFarray_printf (dot, "%s (%s:%s = %s)",
                            a_id[n->kind], 
                            PFatt_str (n->sem.eqjoin_unq.res),
                            PFatt_str (n->sem.eqjoin_unq.att1),
                            PFatt_str (n->sem.eqjoin_unq.att2));
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
                    PFarray_printf (dot, ", %s:%s",
                                    PFatt_str (n->sem.proj.items[c].new),
                                    PFatt_str (n->sem.proj.items[c].old));
                else
                    PFarray_printf (dot, ", %s", 
                                    PFatt_str (n->sem.proj.items[c].old));

            PFarray_printf (dot, ")");
            break;

        case la_select:
            PFarray_printf (dot, "%s (%s)", a_id[n->kind],
                            PFatt_str (n->sem.select.att));
            break;

        case la_pos_select:
            PFarray_printf (dot, "%s (%i, <", a_id[n->kind],
                            n->sem.pos_sel.pos);

            if (PFord_count (n->sem.pos_sel.sortby))
                PFarray_printf (dot, "%s%s", 
                                PFatt_str (
                                    PFord_order_col_at (
                                        n->sem.pos_sel.sortby, 0)),
                                PFord_order_dir_at (
                                    n->sem.pos_sel.sortby, 0) == DIR_ASC
                                ? "" : " (desc)");

            for (c = 1; c < PFord_count (n->sem.pos_sel.sortby); c++)
                PFarray_printf (dot, ", %s%s", 
                                PFatt_str (
                                    PFord_order_col_at (
                                        n->sem.pos_sel.sortby, c)),
                                PFord_order_dir_at (
                                    n->sem.pos_sel.sortby, c) == DIR_ASC
                                ? "" : " (desc)");

            PFarray_printf (dot, ">");

            if (n->sem.pos_sel.part != att_NULL)
                PFarray_printf (dot, "/%s", 
                                PFatt_str (n->sem.pos_sel.part));

            PFarray_printf (dot, ")");
            break;

            break;

        case la_fun_1to1:
            PFarray_printf (dot, "%s [%s] (%s:<", a_id[n->kind],
                            PFalg_fun_str (n->sem.fun_1to1.kind),
                            PFatt_str (n->sem.fun_1to1.res));
            for (c = 0; c < n->sem.fun_1to1.refs.count;c++)
                PFarray_printf (dot, "%s%s", 
                                c ? ", " : "",
                                PFatt_str (n->sem.fun_1to1.refs.atts[c]));
            PFarray_printf (dot, ">)");
            break;
            
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
            PFarray_printf (dot, "%s (%s:<%s, %s>)", a_id[n->kind],
                            PFatt_str (n->sem.binary.res),
                            PFatt_str (n->sem.binary.att1),
                            PFatt_str (n->sem.binary.att2));
            break;

        case la_bool_not:
            PFarray_printf (dot, "%s (%s:<%s>)", a_id[n->kind],
                            PFatt_str (n->sem.unary.res),
                            PFatt_str (n->sem.unary.att));
	    break;

        case la_to:
            PFarray_printf (dot, "%s (%s:<%s,%s>/%s)",
                            a_id[n->kind],
                            PFatt_str (n->sem.to.res),
                            PFatt_str (n->sem.to.att1),
                            PFatt_str (n->sem.to.att2),
                            PFatt_str (n->sem.to.part));
	    break;

        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_seqty1:
        case la_all:
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

        case la_count:
            if (n->sem.aggr.part == att_NULL)
                PFarray_printf (dot, "%s (%s)", a_id[n->kind],
                                PFatt_str (n->sem.aggr.res));
            else
                PFarray_printf (dot, "%s (%s:/%s)", a_id[n->kind],
                                PFatt_str (n->sem.aggr.res),
                                PFatt_str (n->sem.aggr.part));
            break;

        case la_rownum:
            PFarray_printf (dot, "%s (%s:<", a_id[n->kind],
                            PFatt_str (n->sem.rownum.res));

            if (PFord_count (n->sem.rownum.sortby))
                PFarray_printf (dot, "%s%s", 
                                PFatt_str (
                                    PFord_order_col_at (
                                        n->sem.rownum.sortby, 0)),
                                PFord_order_dir_at (
                                    n->sem.rownum.sortby, 0) == DIR_ASC
                                ? "" : " (desc)");

            for (c = 1; c < PFord_count (n->sem.rownum.sortby); c++)
                PFarray_printf (dot, ", %s%s", 
                                PFatt_str (
                                    PFord_order_col_at (
                                        n->sem.rownum.sortby, c)),
                                PFord_order_dir_at (
                                    n->sem.rownum.sortby, c) == DIR_ASC
                                ? "" : " (desc)");

            PFarray_printf (dot, ">");

            if (n->sem.rownum.part != att_NULL)
                PFarray_printf (dot, "/%s", 
                                PFatt_str (n->sem.rownum.part));

            PFarray_printf (dot, ")");
            break;

        case la_rank:
            PFarray_printf (dot, "%s (%s:<", a_id[n->kind],
                            PFatt_str (n->sem.rank.res));

            if (PFord_count (n->sem.rank.sortby))
                PFarray_printf (dot, "%s%s", 
                                PFatt_str (
                                    PFord_order_col_at (
                                        n->sem.rank.sortby, 0)),
                                PFord_order_dir_at (
                                    n->sem.rank.sortby, 0) == DIR_ASC
                                ? "" : " (desc)");

            for (c = 1; c < PFord_count (n->sem.rank.sortby); c++)
                PFarray_printf (dot, ", %s%s", 
                                PFatt_str (
                                    PFord_order_col_at (
                                        n->sem.rank.sortby, c)),
                                PFord_order_dir_at (
                                    n->sem.rank.sortby, c) == DIR_ASC
                                ? "" : " (desc)");

            PFarray_printf (dot, ">)");
            break;

        case la_number:
            PFarray_printf (dot, "%s (%s)", a_id[n->kind],
                            PFatt_str (n->sem.number.res));
            break;

        case la_type:
            PFarray_printf (dot, "%s (%s:<%s>), type: %s", a_id[n->kind],
                            PFatt_str (n->sem.type.res),
                            PFatt_str (n->sem.type.att),
                            PFalg_simple_type_str (n->sem.type.ty));
            break;

        case la_type_assert:
            PFarray_printf (dot, "%s (%s), type: %s", a_id[n->kind],
                            PFatt_str (n->sem.type.att),
                            PFalg_simple_type_str (n->sem.type.ty));
            break;

        case la_cast:
            PFarray_printf (dot, "%s (%s%s%s%s), type: %s", a_id[n->kind],
                            n->sem.type.res?PFatt_str(n->sem.type.res):"",
                            n->sem.type.res?":<":"",
                            PFatt_str (n->sem.type.att),
                            n->sem.type.res?">":"",
                            PFalg_simple_type_str (n->sem.type.ty));
            break;

        case la_guide_step:
        case la_guide_step_join:
            /* overwrite standard node layout */
            PFarray_printf (dot, "\", fontcolor=\"#FFFFFF\", label=\"");
        case la_step:
        case la_step_join:
            PFarray_printf (dot, "%s ", a_id[n->kind]);
                
            /* print out XPath axis */
            switch (n->sem.step.axis)
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
            PFarray_printf (dot, "%s ", PFty_str (n->sem.step.ty));
            
            /* print guide info */
            if (n->kind == la_guide_step ||
                n->kind == la_guide_step_join) {
                bool first = true;
                PFarray_printf (dot, "- (");
                
                for (unsigned int i = 0; i < n->sem.step.guide_count; i++) {
                    PFarray_printf (dot, "%s%i", first ? "" : ", ",
                                    n->sem.step.guides[i]->guide);
                    first = false;
                }
                PFarray_printf (dot, ") ");
            }
            
            if (n->kind == la_step || n->kind == la_guide_step)
                PFarray_printf (dot, "(%s, %s%s%s)", 
                                PFatt_str (n->sem.step.iter),
                                PFatt_str (n->sem.step.item_res),
                                n->sem.step.item_res != n->sem.step.item
                                ? ":" : "",
                                n->sem.step.item_res != n->sem.step.item
                                ? PFatt_str (n->sem.step.item) : "");
            else
                PFarray_printf (dot, "(%s:%s)", 
                                PFatt_str (n->sem.step.item_res),
                                PFatt_str (n->sem.step.item));

            if (n->sem.step.level >= 0)
                PFarray_printf (dot, "\\nlevel=%i", n->sem.step.level);
            break;

        case la_doc_index_join:
        {
            char *name = NULL;
            
            switch (n->sem.doc_join.kind) {
                case la_dj_id:    name = "fn:id";    break;
                case la_dj_idref: name = "fn:idref"; break;
                case la_dj_text:  name = "pf:text";  break;
                case la_dj_attr:  name = "pf:attr";  break;
            }
            PFarray_printf (dot, "%s (%s:<%s, %s>)",
                            name,
                            PFatt_str (n->sem.doc_join.item_res),
                            PFatt_str (n->sem.doc_join.item),
                            PFatt_str (n->sem.doc_join.item_doc));
        }   break;

        case la_doc_tbl:
            PFarray_printf (dot, "%s (%s, %s%s%s)", 
                            a_id[n->kind],
                            PFatt_str (n->sem.doc_tbl.iter),
                            PFatt_str (n->sem.doc_tbl.item_res),
                            n->sem.doc_tbl.item_res != n->sem.doc_tbl.item
                            ? ":" : "",
                            n->sem.doc_tbl.item_res != n->sem.doc_tbl.item
                            ? PFatt_str (n->sem.doc_tbl.item) : "");
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

            PFarray_printf (dot, " (%s:<%s>)",
                            PFatt_str (n->sem.doc_access.res),
                            PFatt_str (n->sem.doc_access.att));
            break;

        case la_twig:
        case la_element:
        case la_textnode:
        case la_comment:
        case la_trace_msg:
            PFarray_printf (dot, "%s (%s, %s)",
                            a_id[n->kind],
                            PFatt_str (n->sem.iter_item.iter),
                            PFatt_str (n->sem.iter_item.item));
            break;
        
        case la_docnode:
            PFarray_printf (dot, "%s (%s)",
                            a_id[n->kind],
                            PFatt_str (n->sem.docnode.iter));
            break;
        
        case la_attribute:
        case la_processi:
            PFarray_printf (dot, "%s (%s, %s, %s)", a_id[n->kind],
                            PFatt_str (n->sem.iter_item1_item2.iter),
                            PFatt_str (n->sem.iter_item1_item2.item1),
                            PFatt_str (n->sem.iter_item1_item2.item2));
            break;

        case la_content:
        case la_trace:
            PFarray_printf (dot,
                            "%s (%s, %s, %s)",
                            a_id[n->kind],
                            PFatt_str (n->sem.iter_pos_item.iter),
                            PFatt_str (n->sem.iter_pos_item.pos),
                            PFatt_str (n->sem.iter_pos_item.item));
            break;
        
        case la_cond_err:
            PFarray_printf (dot, "%s (%s)\\n%s ...", a_id[n->kind],
                            PFatt_str (n->sem.err.att),
                            PFstrndup (n->sem.err.str, 16));
            break;
            
        case la_trace_map:
            PFarray_printf (dot,
                            "%s (%s, %s)",
                            a_id[n->kind],
                            PFatt_str (n->sem.trace_map.inner),
                            PFatt_str (n->sem.trace_map.outer));
            break;
        
        case la_proxy:
            PFarray_printf (dot, "%s %i (", a_id[n->kind], n->sem.proxy.kind);

            if (n->sem.proxy.new_cols.count)
                PFarray_printf (dot, "%s", 
                                PFatt_str (n->sem.proxy.new_cols.atts[0]));

            for (c = 1; c < n->sem.proxy.new_cols.count; c++)
                PFarray_printf (dot, ", %s", 
                                PFatt_str (n->sem.proxy.new_cols.atts[c]));

            if (n->sem.proxy.req_cols.count)
                PFarray_printf (dot, ")\\n(req cols: %s", 
                                PFatt_str (n->sem.proxy.req_cols.atts[0]));

            for (c = 1; c < n->sem.proxy.req_cols.count; c++)
                PFarray_printf (dot, ", %s", 
                                PFatt_str (n->sem.proxy.req_cols.atts[c]));

            PFarray_printf (dot, ")");
            break;

        case la_cross:
        case la_disjunion:
        case la_intersect:
        case la_difference:
        case la_distinct:
        case la_fcns:
        case la_merge_adjacent:
        case la_roots:
        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
        case la_nil:
        case la_rec_fix:
        case la_rec_param:
        case la_rec_arg:
        case la_rec_base:
        case la_proxy_base:
        case la_string_join:
        case la_dummy:
            PFarray_printf (dot, "%s", a_id[n->kind]);
            break;

        case la_cross_mvd:
            PFoops (OOPS_FATAL,
                    "clone column aware cross product operator is "
                    "only allowed inside mvd optimization!");
    }

    if (PFstate.format) {

        char *fmt = PFstate.format;
        /* format character '+' overwrites all others */
        bool all = false;
        while (*fmt) {
            if (*fmt == '+') {
                all = true;
                break;
            }
            fmt++;
        }
        /* iterate over all format characters 
           if we haven't found a '+' character */
        if (!all)
            fmt = PFstate.format;

        while (*fmt) {
            if (*fmt == '+' || *fmt == 'A') {
                /* if present print cardinality */
                if (PFprop_card (n->prop))
                    PFarray_printf (dot, "\\ncard: %i", PFprop_card (n->prop));
            }
            if (*fmt == '+' || *fmt == 'O') {
                /* list attributes marked const */
                for (unsigned int i = 0;
                        i < PFprop_const_count (n->prop); i++)
                    PFarray_printf (dot, i ? ", %s" : "\\nconst: %s",
                                    PFatt_str (
                                        PFprop_const_at (n->prop, i)));
            }
            if (*fmt == '+' || *fmt == 'I') {
                PFalg_attlist_t icols = PFprop_icols_to_attlist (n->prop);

                /* list icols attributes */
                for (unsigned int i = 0; i < icols.count; i++)
                    PFarray_printf (dot, i ? ", %s" : "\\nicols: %s",
                                    PFatt_str (icols.atts[i]));
            }
            if (*fmt == '+' || *fmt == 'K') {
                PFalg_attlist_t keys = PFprop_keys_to_attlist (n->prop);

                /* list keys attributes */
                for (unsigned int i = 0; i < keys.count; i++)
                    PFarray_printf (dot, i ? ", %s" : "\\nkeys: %s",
                                    PFatt_str (keys.atts[i]));
            }
            if (*fmt == '+' || *fmt == 'V') {
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
            }
            if (*fmt == '+' || *fmt == 'D') {
                /* list attributes and their corresponding domains */
                for (unsigned int i = 0; i < n->schema.count; i++)
                    if (PFprop_dom (n->prop, n->schema.items[i].name)) {
                        PFarray_printf (dot, i ? ", %s " : "\\ndom: %s ",
                                        PFatt_str (n->schema.items[i].name));
                        PFprop_write_domain (
                            dot, 
                            PFprop_dom (n->prop, n->schema.items[i].name));
                    }
            }
            if (*fmt == '+' || *fmt == '[') {
                /* list attributes and their unique names */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_att_t ori = n->schema.items[i].name;
                    PFalg_att_t unq = PFprop_unq_name (n->prop, ori);
                    if (unq) {
                        PFalg_att_t l_unq, r_unq;
                        PFarray_printf (
                            dot,
                            i ? " , %s=%s" : "\\nO->U names: %s=%s",
                            PFatt_str (ori), PFatt_str (unq));

                        l_unq = PFprop_unq_name_left (n->prop, ori);
                        r_unq = PFprop_unq_name_right (n->prop, ori);

                        if (l_unq && l_unq != unq && r_unq && r_unq != unq)
                            PFarray_printf (dot,
                                            " [%s|%s]", 
                                            PFatt_str(l_unq),
                                            PFatt_str(r_unq));
                        else if (l_unq && l_unq != unq)
                            PFarray_printf (dot, " [%s|", PFatt_str(l_unq));
                        else if (r_unq && r_unq != unq)
                            PFarray_printf (dot, " |%s]", PFatt_str(r_unq));
                    }
                }
            }
            if (*fmt == '+' || *fmt == ']') {
                /* list attributes and their original names */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_att_t unq = n->schema.items[i].name;
                    PFalg_att_t ori = PFprop_ori_name (n->prop, unq);
                    if (ori) {
                        PFalg_att_t l_ori, r_ori;
                        PFarray_printf (
                            dot,
                            i ? " , %s=%s" : "\\nU->O names: %s=%s",
                            PFatt_str (unq), PFatt_str (ori));

                        l_ori = PFprop_ori_name_left (n->prop, unq);
                        r_ori = PFprop_ori_name_right (n->prop, unq);

                        if (l_ori && l_ori != ori && r_ori && r_ori != ori)
                            PFarray_printf (dot,
                                            " [%s|%s]", 
                                            PFatt_str(l_ori),
                                            PFatt_str(r_ori));
                        else if (l_ori && l_ori != ori)
                            PFarray_printf (dot, " [%s|", PFatt_str(l_ori));
                        else if (r_ori && r_ori != ori)
                            PFarray_printf (dot, " |%s]", PFatt_str(r_ori));
                    }
                }
            }
            if (*fmt == '+' || *fmt == 'S') {
                /* print whether columns do have to respect duplicates */
                if (PFprop_set (n->prop))
                    PFarray_printf (dot, "\\nset"); 
            }
            if (*fmt == '+' || *fmt == 'L') {
                /* print columns that have a level information attached */
                bool first = true;
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_att_t att = n->schema.items[i].name;
                    int level = PFprop_level (n->prop, att);
                    if (level >= 0) {
                        PFarray_printf (
                            dot,
                            "%s %s=%i",
                            first ? "\\nlevel:" : ",",
                            PFatt_str (att),
                            level);
                        first = false;
                    }
                }
            }
            if (*fmt == '+' || *fmt == 'U') {
                PFguide_tree_t **guides;
                PFalg_att_t att;
                unsigned int i, j, count;
                bool first = true;
                
                for (i = 0; i < n->schema.count; i++) {
                    att = n->schema.items[i].name;
                    if (PFprop_guide (n->prop, att)) {
                    
                        PFarray_printf (dot, "%s %s:", 
                                        first ? "\\nGUIDE:" : ",",
                                        PFatt_str(att));
                        first = false;
                        
                        /* print guides */
                        count  = PFprop_guide_count (n->prop, att);
                        guides = PFprop_guide_elements (n->prop, att);
                        for (j = 0; j < count; j++)
                            PFarray_printf (dot, " %i", guides[j]->guide);
                    }
                }
            }
            if (*fmt == '+' || *fmt == 'Y') {
                if (PFprop_ckeys_count (n->prop)) {
                    PFalg_attlist_t list;
                    unsigned int i, j;
                    bool first = true;
                    
                    PFarray_printf (dot, "\\ncomposite keys:");

                    for (i = 0; i < PFprop_ckeys_count (n->prop); i++) {
                        list = PFprop_ckey_at (n->prop, i);
                        first = true;
                        for (j = 0; j < list.count; j++) {
                            PFarray_printf (dot, "%s%s",
                                            first ? "\\n<" : ", ",
                                            PFatt_str(list.atts[j]));
                            first = false;
                        }
                        PFarray_printf (dot, ">");
                    }
                } 
            }

            /* stop after all properties have been printed */
            if (*fmt == '+')
                break;
            else
                fmt++;
        }
    }

    /* close up label */
    PFarray_printf (dot, "\", color=\"%s\" ];\n", color[n->kind]);

    for (c = 0; c < PFLA_OP_MAXCHILD && n->child[c]; c++) {      
        PFarray_printf (dot, "node%i -> node%i;\n",
                        n->node_id, n->child[c]->node_id);
    }

    /* create soft links */
    switch (n->kind)
    {
        case la_rec_arg:
            if (n->sem.rec_arg.base) {
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

        case la_proxy:
            if (n->sem.proxy.base1)
                PFarray_printf (dot, "node%i -> node%i [style=dashed];\n",
                                n->node_id, n->sem.proxy.base1->node_id);
            
            if (n->sem.proxy.base2)
                PFarray_printf (dot, "node%i -> node%i [style=dashed];\n",
                                n->node_id, n->sem.proxy.base2->node_id);
            
            if (n->sem.proxy.ref)
                PFarray_printf (dot, 
                                "node%i -> node%i "
                                "[style=dashed label=ref];\n",
                                n->node_id, n->sem.proxy.ref->node_id);
            break;
            
        default:
            break;
    }
    
    /* mark node visited */
    n->bit_dag = true;

    for (c = 0; c < PFLA_OP_MAXCHILD && n->child[c]; c++) {
        if (!n->child[c]->bit_dag)
            la_dot (dot, n->child[c]);
    }
}

/**
 * Print algebra tree in XML notation.
 * @param xml Array into which we print
 * @param n The current node to print (function is recursive)
 */
static void 
la_xml (PFarray_t *xml, PFla_op_t *n)
{
    unsigned int c;

    assert(n->node_id);

    for (c = 0; c < PFLA_OP_MAXCHILD && n->child[c] != 0; c++)
        if (!n->child[c]->bit_dag)
            la_xml (xml, n->child[c]);

    /* open up label */
    PFarray_printf (xml,
                    "  <node id=\"%i\" kind=\"%s\">\n",
                    n->node_id,
                    xml_id[n->kind]);

    /*
     * Print schema information for the current algebra expression
     *
     * Format:
     *
     *   <schema>
     *     <column name='iter' type='nat'/>
     *     <column name='item' type='int str'/>
     *     ...
     *   </schema>
     */
    PFarray_printf (xml, "    <schema>\n");
    for (unsigned int i = 0; i < n->schema.count; i++) {
        bool first = true;

        PFarray_printf (xml, "      <col name=\"%s\" types=\"",
                        PFatt_str (n->schema.items[i].name));
        for (PFalg_simple_type_t t = 1; t; t <<= 1) {
            if (t & n->schema.items[i].type) {
                /* hide fragment information */
                switch (t) {
                    case aat_afrag:
                    case aat_pfrag:
                        continue;
                    default:
                        break;
                }
                
                /* start printing spaces only after the first type */
                if (!first)
                    PFarray_printf (xml, " ");
                else
                    first = false;
                
                /* print the different types */
                switch (t) {
                    case aat_nat:    PFarray_printf (xml, "nat");   break;
                    case aat_int:    PFarray_printf (xml, "int");   break;
                    case aat_str:    PFarray_printf (xml, "str");   break;
                    case aat_dec:    PFarray_printf (xml, "dec");   break;
                    case aat_dbl:    PFarray_printf (xml, "dbl");   break;
                    case aat_bln:    PFarray_printf (xml, "bln");   break;
                    case aat_qname:  PFarray_printf (xml, "qname"); break;
                    case aat_uA:     PFarray_printf (xml, "uA");    break;
                    case aat_attr:   PFarray_printf (xml, "attr");  break;
                    case aat_pre:    PFarray_printf (xml, "node");  break;
                    default:                                        break;
                }
            }
        }
        PFarray_printf (xml, "\"/>\n");
    }
    PFarray_printf (xml, "    </schema>\n");

    if (PFstate.format) {

        char *fmt = PFstate.format;
        /* format character '+' overwrites all others */
        bool all = false;
        while (*fmt) {
            if (*fmt == '+') {
                all = true;
                break;
            }
            fmt++;
        }

        /* iterate over all format characters 
           if we haven't found a '+' character */
        if (!all)
            fmt = PFstate.format;

        PFarray_printf (xml, "    <properties>\n");
        while (*fmt) {
            if (*fmt == '+' || *fmt == 'A') {
                /* if present print cardinality */
                if (PFprop_card (n->prop))
                    PFarray_printf (xml, "      <card value=\"%i\"/>\n",
                                    PFprop_card (n->prop));
            }
            if (*fmt == '+' || *fmt == 'O') {
                /* list attributes marked const */
                for (unsigned int i = 0;
                        i < PFprop_const_count (n->prop); i++)
                    PFarray_printf (xml,
                                    "      <const column=\"%s\">\n"
                                    "        %s\n"
                                    "      </const>\n",
                                    PFatt_str (
                                        PFprop_const_at (n->prop, i)),
                                    xml_literal (
                                        PFprop_const_val_at (n->prop, i)));
            }
            if (*fmt == '+' || *fmt == 'I') {
                PFalg_attlist_t icols = PFprop_icols_to_attlist (n->prop);

                /* list icols attributes */
                for (unsigned int i = 0; i < icols.count; i++)
                    PFarray_printf (xml, "      <icols column=\"%s\"/>\n",
                                    PFatt_str (icols.atts[i]));
            }
            if (*fmt == '+' || *fmt == 'K') {
                PFalg_attlist_t keys = PFprop_keys_to_attlist (n->prop);

                /* list keys attributes */
                for (unsigned int i = 0; i < keys.count; i++)
                    PFarray_printf (xml, "      <keys column=\"%s\"/>\n",
                                    PFatt_str (keys.atts[i]));
            }
            if (*fmt == '+' || *fmt == 'V') {
                /* list required value columns and their values */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_att_t att = n->schema.items[i].name;
                    if (PFprop_reqval (n->prop, att))
                        PFarray_printf (
                            xml, 
                            "      <required attr=\"%s\" value=\"%s\"/>\n",
                            PFatt_str (att),
                            PFprop_reqval_val (n->prop, att)?"true":"false");
                }
            }
            if (*fmt == '+' || *fmt == 'D') {
                /* list attributes and their corresponding domains */
                for (unsigned int i = 0; i < n->schema.count; i++)
                    if (PFprop_dom (n->prop, n->schema.items[i].name)) {
                        PFarray_printf (xml, "      <domain attr=\"%s\" "
                                        "value=\"",
                                        PFatt_str (n->schema.items[i].name));
                        PFprop_write_domain (
                            xml, 
                            PFprop_dom (n->prop, n->schema.items[i].name));
                        PFarray_printf (xml, "\"/>\n");
                    }
            }
            if (*fmt == '+' || *fmt == 'S') {
                /* print whether columns do have to respect duplicates */
                if (PFprop_set (n->prop))
                    PFarray_printf (xml, "      <duplicates allowed=\"yes\"/>\n");
            }
            if (*fmt == '+') {
                /* print the number of referenced columns */
                if (PFprop_refctr (n))
                    PFarray_printf (xml, "      <references count=\"%i\"/>\n",
                                    PFprop_refctr (n));
            }

            /* stop after all properties have been printed */
            if (*fmt == '+')
                break;
            else
                fmt++;
        }
        PFarray_printf (xml, "    </properties>\n");
    }

    /* create label */
    switch (n->kind)
    {
        case la_serialize_seq:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"false\""
                                         " function=\"pos\"/>\n"
                            "      <column name=\"%s\" new=\"false\""
                                         " function=\"item\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.ser_seq.pos),
                            PFatt_str (n->sem.ser_seq.item));
            break;

        case la_serialize_rel:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"false\""
                                         " function=\"iter\"/>\n"
                            "      <column name=\"%s\" new=\"false\""
                                         " function=\"pos\"/>\n",
                            PFatt_str (n->sem.ser_rel.iter),
                            PFatt_str (n->sem.ser_rel.pos));

            for (c = 0; c < n->sem.ser_rel.items.count; c++)
                PFarray_printf (xml,
                                "      <column name=\"%s\" new=\"false\""
                                             " function=\"item\""
                                             " position=\"%i\"/>\n",
                                PFatt_str (n->sem.ser_rel.items.atts[c]),
                                c);
            
            PFarray_printf (xml, "    </content>\n");
            break;

        case la_lit_tbl:
            /* list the attributes of this table */
            PFarray_printf (xml, "    <content>\n"); 

            for (c = 0; c < n->schema.count;c++) {
                PFarray_printf (xml, 
                                "      <column name=\"%s\" new=\"true\">\n",
                                PFatt_str (n->schema.items[c].name));
                /* print out tuples in table, if table is not empty */
                for (unsigned int i = 0; i < n->sem.lit_tbl.count; i++)
                    PFarray_printf (xml,
                                    "        %s\n",
                                    xml_literal (n->sem.lit_tbl
                                                       .tuples[i].atoms[c]));
                PFarray_printf (xml, "      </column>\n");
            }

            PFarray_printf (xml, "    </content>\n");
            break;

        case la_empty_tbl:
            PFarray_printf (xml, "    <content>\n"); 
            /* list the attributes of this table */
            for (c = 0; c < n->schema.count;c++)
                PFarray_printf (xml, 
                                "      <column name=\"%s\" new=\"true\"/>\n",
                                PFatt_str (n->schema.items[c].name));

            PFarray_printf (xml, "    </content>\n");
            break;

        case la_attach:
            PFarray_printf (xml, 
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\">\n"
                            "        %s\n"
                            "      </column>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.attach.res),
                            xml_literal (n->sem.attach.value));
            break;

        case la_eqjoin:
        case la_semijoin:
            PFarray_printf (xml, 
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"false\""
                                         " position=\"1\"/>\n"
                            "      <column name=\"%s\" new=\"false\""
                                         " position=\"2\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.eqjoin.att1),
                            PFatt_str (n->sem.eqjoin.att2));
            break;

        case la_thetajoin:
            PFarray_printf (xml, "    <content>\n");
            for (c = 0; c < n->sem.thetajoin.count; c++)
                PFarray_printf (xml,
                                "      <comparison kind=\"%s\">\n"
                                "        <column name=\"%s\" new=\"false\""
                                               " position=\"1\"/>\n"
                                "        <column name=\"%s\" new=\"false\""
                                               " position=\"2\"/>\n"
                                "      </comparison>\n",
                                comp_str (n->sem.thetajoin.pred[c].comp),
                                PFatt_str (n->sem.thetajoin.pred[c].left),
                                PFatt_str (n->sem.thetajoin.pred[c].right));
            PFarray_printf (xml, "    </content>\n");
            break;

        case la_eqjoin_unq:
            PFarray_printf (xml, 
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"false\""
                                         " keep=\"%s\" position=\"1\"/>\n"
                            "      <column name=\"%s\" new=\"false\""
                                         " keep=\"%s\" position=\"2\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.eqjoin_unq.att1),
                            n->sem.eqjoin_unq.att1 == n->sem.eqjoin_unq.res
                            ? "true" : "false",
                            PFatt_str (n->sem.eqjoin_unq.att2),
                            n->sem.eqjoin_unq.att2 == n->sem.eqjoin_unq.res
                            ? "true" : "false");
            break;

        case la_project:
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

        case la_select:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"false\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.select.att));
            break;
            
        case la_pos_select:
            PFarray_printf (xml, 
                            "    <content>\n" 
                            "      <position>%i</position>\n",
                            n->sem.pos_sel.pos);

            for (c = 0; c < PFord_count (n->sem.pos_sel.sortby); c++)
                PFarray_printf (xml, 
                                "      <column name=\"%s\" function=\"sort\""
                                        " position=\"%u\" direction=\"%s\""
                                        " new=\"false\"/>\n",
                                PFatt_str (
                                    PFord_order_col_at (
                                        n->sem.pos_sel.sortby, c)),
                                c+1,
                                PFord_order_dir_at (
                                    n->sem.pos_sel.sortby, c) == DIR_ASC
                                ? "ascending" : "descending");

            if (n->sem.pos_sel.part != att_NULL)
                PFarray_printf (xml,
                                "      <column name=\"%s\" function=\"partition\""
                                        " new=\"false\"/>\n",
                                PFatt_str (n->sem.pos_sel.part));

            PFarray_printf (xml, "    </content>\n");
            break;

        case la_fun_1to1:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <kind name=\"%s\"/>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n",
                            PFalg_fun_str (n->sem.fun_1to1.kind),
                            PFatt_str (n->sem.fun_1to1.res));
            
            for (c = 0; c < n->sem.fun_1to1.refs.count; c++)
                PFarray_printf (xml,
                                "      <column name=\"%s\" new=\"false\""
                                             " position=\"%i\"/>\n",
                                PFatt_str (n->sem.fun_1to1.refs.atts[c]),
                                c + 1);
                        
            PFarray_printf (xml, "    </content>\n");
            break;

        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n"
                            "      <column name=\"%s\" new=\"false\""
                                         " position=\"1\"/>\n"
                            "      <column name=\"%s\" new=\"false\""
                                         " position=\"2\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.binary.res),
                            PFatt_str (n->sem.binary.att1),
                            PFatt_str (n->sem.binary.att2));
            break;

        case la_bool_not:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n"
                            "      <column name=\"%s\" new=\"false\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.unary.res),
                            PFatt_str (n->sem.unary.att));
            break;

        case la_to:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n"
                            "      <column name=\"%s\" new=\"false\""
                                         " function=\"start\"/>\n"
                            "      <column name=\"%s\" new=\"false\""
                                         " function=\"end\"/>\n"
                            "      <column name=\"%s\" new=\"false\""
                                         " function=\"partition\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.to.res),
                            PFatt_str (n->sem.to.att1),
                            PFatt_str (n->sem.to.att2),
                            PFatt_str (n->sem.to.part));
            break;

        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_seqty1:
        case la_all:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n"
                            "      <column name=\"%s\" new=\"false\""
                                         " function=\"item\"/>\n",
                            PFatt_str (n->sem.aggr.res),
                            PFatt_str (n->sem.aggr.att));
            if (n->sem.aggr.part != att_NULL)
                PFarray_printf (xml,
                            "      <column name=\"%s\" function=\"partition\""
                                    " new=\"false\"/>\n",
                            PFatt_str (n->sem.aggr.part));
            PFarray_printf (xml, "    </content>\n");

            break;

        case la_count:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n",
                            PFatt_str (n->sem.aggr.res));
            if (n->sem.aggr.part != att_NULL)
                PFarray_printf (xml,
                            "      <column name=\"%s\" function=\"partition\""
                                    " new=\"false\"/>\n",
                            PFatt_str (n->sem.aggr.part));
            PFarray_printf (xml, "    </content>\n");
            break;

        case la_rownum:
            PFarray_printf (xml, 
                            "    <content>\n" 
                            "      <column name=\"%s\" new=\"true\"/>\n",
                            PFatt_str (n->sem.rownum.res));

            for (c = 0; c < PFord_count (n->sem.rownum.sortby); c++)
                PFarray_printf (xml, 
                                "      <column name=\"%s\" function=\"sort\""
                                        " position=\"%u\" direction=\"%s\""
                                        " new=\"false\"/>\n",
                                PFatt_str (
                                    PFord_order_col_at (
                                        n->sem.rownum.sortby, c)),
                                c+1,
                                PFord_order_dir_at (
                                    n->sem.rownum.sortby, c) == DIR_ASC
                                ? "ascending" : "descending");

            if (n->sem.rownum.part != att_NULL)
                PFarray_printf (xml,
                                "      <column name=\"%s\" function=\"partition\""
                                        " new=\"false\"/>\n",
                                PFatt_str (n->sem.rownum.part));

            PFarray_printf (xml, "    </content>\n");
            break;

        case la_rank:
            PFarray_printf (xml, 
                            "    <content>\n" 
                            "      <column name=\"%s\" new=\"true\"/>\n",
                            PFatt_str (n->sem.rank.res));

            for (c = 0; c < PFord_count (n->sem.rank.sortby); c++)
                PFarray_printf (xml, 
                                "      <column name=\"%s\" function=\"sort\""
                                        " position=\"%u\" direction=\"%s\""
                                        " new=\"false\"/>\n",
                                PFatt_str (
                                    PFord_order_col_at (
                                        n->sem.rank.sortby, c)),
                                c+1,
                                PFord_order_dir_at (
                                    n->sem.rank.sortby, c) == DIR_ASC
                                ? "ascending" : "descending");

            PFarray_printf (xml, "    </content>\n");
            break;

        case la_number:
            PFarray_printf (xml, 
                            "    <content>\n" 
                            "      <column name=\"%s\" new=\"true\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.number.res));
            break;

        case la_type:
            PFarray_printf (xml, 
                            "    <content>\n" 
                            "      <column name=\"%s\" new=\"true\"/>\n"
                            "      <column name=\"%s\" new=\"false\"/>\n"
                            "      <type name=\"%s\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.type.res),
                            PFatt_str (n->sem.type.att),
                            PFalg_simple_type_str (n->sem.type.ty));
            break;

        case la_type_assert:
            PFarray_printf (xml, 
                            "    <content>\n" 
                            "      <column name=\"%s\" new=\"false\"/>\n"
                            "      <type name=\"%s\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.type.att),
                            PFalg_simple_type_str (n->sem.type.ty));
            break;

        case la_cast:
            PFarray_printf (xml, 
                            "    <content>\n" 
                            "      <column name=\"%s\" new=\"true\"/>\n"
                            "      <column name=\"%s\" new=\"false\"/>\n"
                            "      <type name=\"%s\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.type.res),
                            PFatt_str (n->sem.type.att),
                            PFalg_simple_type_str (n->sem.type.ty));
            break;

        case la_step:
        case la_step_join:
        case la_guide_step:
        case la_guide_step_join:
            PFarray_printf (xml, "    <content>\n      <step axis=\"");
                
            /* print out XPath axis */
            switch (n->sem.step.axis)
            {
                case alg_anc:
                    PFarray_printf (xml, "ancestor");
                    break;
                case alg_anc_s:
                    PFarray_printf (xml, "anc-or-self");
                    break;
                case alg_attr:
                    PFarray_printf (xml, "attribute");
                    break;
                case alg_chld:
                    PFarray_printf (xml, "child");
                    break;
                case alg_desc:
                    PFarray_printf (xml, "descendant");
                    break;
                case alg_desc_s:
                    PFarray_printf (xml, "desc-or-self");
                    break;
                case alg_fol:
                    PFarray_printf (xml, "following");
                    break;
                case alg_fol_s:
                    PFarray_printf (xml, "fol-sibling");
                    break;
                case alg_par:
                    PFarray_printf (xml, "parent");
                    break;
                case alg_prec:
                    PFarray_printf (xml, "preceding");
                    break;
                case alg_prec_s:
                    PFarray_printf (xml, "prec-sibling");
                    break;
                case alg_self:
                    PFarray_printf (xml, "self");
                    break;
                default: PFoops (OOPS_FATAL,
                        "unknown XPath axis in dot output");
            }

            PFarray_printf (xml, "\" type=\"%s\"", PFty_str (n->sem.step.ty));
            
            if (n->kind == la_guide_step || n->kind == la_guide_step_join) {
                bool first = true;
                PFarray_printf (xml, " guide=\"");
                for (unsigned int i = 0; i < n->sem.step.guide_count; i++) {
                    PFarray_printf (xml, "%s%i", first ? "" : " ",
                                    n->sem.step.guides[i]->guide);
                    first = false;
                }
                PFarray_printf (xml, "\"");
            }
            if (n->sem.step.level >= 0)
                PFarray_printf (xml, " level=\"%i\"", n->sem.step.level);
            
            if (n->kind == la_step || n->kind == la_guide_step)
                PFarray_printf (xml,
                                "/>\n"
                                "      <column name=\"%s\" function=\"iter\"/>\n"
                                "      <column name=\"%s\" function=\"item\"/>\n"
                                "    </content>\n",
                                PFatt_str (n->sem.step.iter),
                                PFatt_str (n->sem.step.item));
            else 
                PFarray_printf (xml,
                                "/>\n"
                                "      <column name=\"%s\" new=\"true\"/>\n"
                                "      <column name=\"%s\" function=\"item\"/>\n"
                                "    </content>\n",
                                PFatt_str (n->sem.step.item_res),
                                PFatt_str (n->sem.step.item));
            break;

        case la_doc_index_join:
        {    
            char *name = NULL;
            
            switch (n->sem.doc_join.kind) {
                case la_dj_id:    name = "fn:id";    break;
                case la_dj_idref: name = "fn:idref"; break;
                case la_dj_text:  name = "pf:text";  break;
                case la_dj_attr:  name = "pf:attr";  break;
            }
            
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <kind name=\"%s\"/>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "      <column name=\"%s\" function=\"docnode\"/>\n"
                            "    </content>\n",
                            name,
                            PFatt_str (n->sem.doc_join.item_res),
                            PFatt_str (n->sem.doc_join.item),
                            PFatt_str (n->sem.doc_join.item_doc));
        }   break;

        case la_doc_tbl:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.doc_tbl.item_res),
                            PFatt_str (n->sem.doc_tbl.iter),
                            PFatt_str (n->sem.doc_tbl.item));
            break;

        case la_doc_access:
            PFarray_printf (xml, 
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n"
                            "      <column name=\"%s\" new=\"false\"/>\n"
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
                            "\" new=\"false\" function=\"document column\"/>\n"
                            "    </content>\n");
            break;

        case la_twig:
        case la_element:
        case la_textnode:
        case la_comment:
        case la_trace_msg:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.iter_item.iter),
                            PFatt_str (n->sem.iter_item.item));
            break;
        
        case la_docnode:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.docnode.iter));
            break;
        
        case la_attribute:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"qname item\""
                                    "/>\n"
                            "      <column name=\"%s\" function=\"content item\""
                                    "/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.iter_item1_item2.iter),
                            PFatt_str (n->sem.iter_item1_item2.item1),
                            PFatt_str (n->sem.iter_item1_item2.item2));
            break;

        case la_processi:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"target item\""
                                    "/>\n"
                            "      <column name=\"%s\" function=\"value item\""
                                    "/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.iter_item1_item2.iter),
                            PFatt_str (n->sem.iter_item1_item2.item1),
                            PFatt_str (n->sem.iter_item1_item2.item2));
            break;

        case la_content:
        case la_trace:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"pos\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.iter_pos_item.iter),
                            PFatt_str (n->sem.iter_pos_item.pos),
                            PFatt_str (n->sem.iter_pos_item.item));
            break;
        
        case la_merge_adjacent:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"pos\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.merge_adjacent.iter_res),
                            PFatt_str (n->sem.merge_adjacent.pos_res),
                            PFatt_str (n->sem.merge_adjacent.item_res));
            break;
        
        case la_cond_err:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"false\"/>\n"
                            "      <error>%s</error>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.err.att),
                            PFstrdup (n->sem.err.str));
            break;

        case la_trace_map:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" function=\"inner\"/>\n"
                            "      <column name=\"%s\" function=\"outer\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.trace_map.inner),
                            PFatt_str (n->sem.trace_map.outer));
            break;
        
        case la_string_join:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"pos\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.string_join.iter),
                            PFatt_str (n->sem.string_join.pos),
                            PFatt_str (n->sem.string_join.item));
            break;
        
        default:
            break;
    }

    for (c = 0; c < PFLA_OP_MAXCHILD && n->child[c] != 0; c++)
        PFarray_printf (xml,
                        "    <edge to=\"%i\"/>\n", 
                        n->child[c]->node_id);

    /* close up label */
    PFarray_printf (xml, "  </node>\n");

    /* mark node visited */
    n->bit_dag = true;
}

static unsigned int
create_node_id_worker (PFla_op_t *n, unsigned int i)
{
    if (n->bit_dag)
        return i;
    else
        n->bit_dag = true;

    n->node_id = i++;

    for (unsigned int c = 0; c < PFLA_OP_MAXCHILD && n->child[c]; c++)
        i = create_node_id_worker (n->child[c], i);

    return i;
}

static void
create_node_id (PFla_op_t *n)
{
    (void) create_node_id_worker (n, 1);
    PFla_dag_reset (n);
}

static void
reset_node_id_worker (PFla_op_t *n)
{
    if (n->bit_dag)
        return;
    else
        n->bit_dag = true;

    n->node_id = 0;

    for (unsigned int c = 0; c < PFLA_OP_MAXCHILD && n->child[c]; c++)
        reset_node_id_worker (n->child[c]);
}

static void
reset_node_id (PFla_op_t *n)
{
    reset_node_id_worker (n);
    PFla_dag_reset (n);
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
                             "node [fontsize=10];\n"
                             "edge [fontsize=9];\n");

        create_node_id (root);
        la_dot (dot, root);
        PFla_dag_reset (root);
        reset_node_id (root);

        /* add domain subdomain relationships if required */
        if (PFstate.format) {
            char *fmt = PFstate.format;
            while (*fmt) { 
                if (*fmt == '+' || *fmt == 'D') {
                        PFprop_write_dom_rel_dot (dot, root->prop);
                        break;
                }
                fmt++;
            }
        }

        PFarray_printf (dot, "}\n");

        /* put content of array into file */
        fprintf (f, "%s", (char *) dot->base);
    }
}


/**
 * Dump algebra tree in XML format
 *
 * @param f file to dump into
 * @param root root of logical algebra tree
 */
void
PFla_xml (FILE *f, PFla_op_t *root)
{
    if (root) {

        /* initialize array to hold dot output */
        PFarray_t *xml = PFarray (sizeof (char));


        
        PFarray_printf (xml, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        PFarray_printf (xml, "<logical_query_plan unique_names=\"%s\">\n",
                        (PFalg_is_unq_name(root->schema.items[0].name) ? "true" : "false"));
        /* add domain subdomain relationships if required */
        if (PFstate.format) {
            char *fmt = PFstate.format;
            while (*fmt) { 
                if (*fmt == '+' || *fmt == 'D') {
                        PFprop_write_dom_rel_xml (xml, root->prop);
                        break;
                }
                fmt++;
            }
        }

        create_node_id (root);
        la_xml (xml, root);
        PFla_dag_reset (root);
        reset_node_id (root);
        PFla_dag_reset (root);

        PFarray_printf (xml, "</logical_query_plan>\n");
        /* put content of array into file */
        fprintf (f, "%s", (char *) xml->base);
    }
}

/* vim:set shiftwidth=4 expandtab: */
