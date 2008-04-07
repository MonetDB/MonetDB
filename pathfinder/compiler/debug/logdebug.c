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
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
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
    , [la_ref_tbl ]         = "REF_TBL"
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
    , [la_rownum]           = "ROWNUM"
    , [la_rowrank]          = "ROWRANK"
    , [la_rank]             = "RANK"
    , [la_rowid]            = "ROWID"
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
    , [la_frag_extract]     = "FRAG EXTRACT"
    , [la_frag_union]       = "FRAG_UNION"
    , [la_empty_frag]       = "EMPTY_FRAG"
    , [la_error]            = "!ERROR"
    , [la_cond_err]         = "!ERROR"
    , [la_nil]              = "nil"
    , [la_trace]            = "trace"
    , [la_trace_msg]        = "trace_msg"
    , [la_trace_map]        = "trace_map"
    , [la_rec_fix]          = "rec fix"
    , [la_rec_param]        = "rec param"
    , [la_rec_arg]          = "rec arg"
    , [la_rec_base]         = "rec base"
    , [la_fun_call]         = "fun call"
    , [la_fun_param]        = "fun param"
    , [la_fun_frag_param]   = "fun frag param"
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
    , [la_ref_tbl ]         = "ref_tbl"
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
    , [la_rowrank]          = "rowrank"
    , [la_rank]             = "rank"
    , [la_rowid]            = "rowid"
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
    , [la_frag_extract]     = "FRAG EXTRACT"
    , [la_frag_union]       = "FRAG_UNION"
    , [la_empty_frag]       = "EMPTY_FRAG"
    , [la_error]            = "error"
    , [la_cond_err]         = "error"
    , [la_nil]              = "nil"
    , [la_trace]            = "trace"
    , [la_trace_msg]        = "trace msg"
    , [la_trace_map]        = "trace map"
    , [la_rec_fix]          = "recursion fix"
    , [la_rec_param]        = "recursion param"
    , [la_rec_arg]          = "recursion arg"
    , [la_rec_base]         = "recursion base"
    , [la_fun_call]         = "function call"
    , [la_fun_param]        = "function call parameter"
    , [la_fun_frag_param]   = "function call fragment parameter"
    , [la_proxy]            = "proxy"
    , [la_proxy_base]       = "proxy base"
    , [la_string_join]      = "fn:string-join"
    , [la_dummy]            = "dummy"
};

static char *
literal (PFalg_atom_t a)
{
    PFarray_t *s = PFarray (sizeof (char), 50);

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
            PFarray_printf (s, "%s", PFesc_string (PFqname_str (a.val.qname)));
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
    PFarray_t *s = PFarray (sizeof (char), 50);

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
           s, "<value type=\"%s\">"
              "<qname prefix=\"%s\" uri=\"%s\" local=\"%s\"/>"
              "</value>",
           PFalg_simple_type_str (a.type),
           PFqname_prefix (a.val.qname),
           PFqname_uri (a.val.qname),
           PFqname_loc (a.val.qname));
    else
        PFarray_printf (s, "<value type=\"node\"/>");

    return (char *) s->base;
}

static char *
xml_literal_list (PFalg_simple_type_t ty)
{
    bool first = true;
    PFarray_t *s = PFarray (sizeof (char), 50);

    if (ty & aat_update)
        PFarray_printf (s, "update");
    else if (ty & aat_docmgmt)
        PFarray_printf (s, "docmgmt");
    else
        for (PFalg_simple_type_t t = 1; t; t <<= 1) {
            if (t & ty) {
                /* hide fragment information */
                switch (t) {
                    case aat_pre:
                    case aat_frag:
                    case aat_qname_loc:
                        continue;
                    default:
                        break;
                }

                /* start printing spaces only after the first type */
                if (!first)
                    PFarray_printf (s, " ");
                else
                    first = false;

                /* print the different types */
                switch (t) {
                    case aat_nat:      PFarray_printf (s, "nat");   break;
                    case aat_int:      PFarray_printf (s, "int");   break;
                    case aat_str:      PFarray_printf (s, "str");   break;
                    case aat_dec:      PFarray_printf (s, "dec");   break;
                    case aat_dbl:      PFarray_printf (s, "dbl");   break;
                    case aat_bln:      PFarray_printf (s, "bool");  break;
                    case aat_uA:       PFarray_printf (s, "uA");    break;
                    case aat_qname_id: PFarray_printf (s, "qname"); break;
                    case aat_attr:     PFarray_printf (s, "attr");  break;
                    case aat_nkind:    PFarray_printf (s, "pnode"); break;
                    default:                                        break;
                }
            }
        }

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
la_dot (PFarray_t *dot, PFla_op_t *n, bool print_frag_info, char *prop_args)
{
    unsigned int c;
    assert(n->node_id);

    static char *color[] = {
          [la_serialize_seq]   = "#C0C0C0"
        , [la_serialize_rel]   = "#C0C0C0"
        , [la_lit_tbl]         = "#C0C0C0"
        , [la_empty_tbl]       = "#C0C0C0"
        , [la_ref_tbl]         = "#C0C0C0"
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
        , [la_rowrank]         = "#FF0000"
        , [la_rank]            = "#FF3333"
        , [la_rowid]           = "#FF9999"
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
        , [la_frag_extract]    = "#DD22DD"
        , [la_frag_union]      = "#E0E0E0"
        , [la_empty_frag]      = "#E0E0E0"
        , [la_error]           = "#C0C0C0"
        , [la_cond_err]        = "#C0C0C0"
        , [la_nil]             = "#FFFFFF"
        , [la_trace]           = "#FF5500"
        , [la_trace_msg]       = "#FF5500"
        , [la_trace_map]       = "#FF5500"
        , [la_rec_fix]         = "#FF00FF"
        , [la_rec_param]       = "#FF00FF"
        , [la_rec_arg]         = "#BB00BB"
        , [la_rec_base]        = "#BB00BB"
        , [la_fun_call]        = "#BB00BB"
        , [la_fun_param]       = "#BB00BB"
        , [la_fun_frag_param]  = "#BB00BB"
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

        case la_ref_tbl :
            PFarray_printf (dot, "%s: ", a_id[n->kind]);
            for (c = 0; c < n->schema.count;c++)
                PFarray_printf (dot, "%s%s", c ? " | " : "(",
                                PFatt_str (n->schema.items[c].name));
            PFarray_printf (dot, ")\\ncolumn name ");
            for (c = 0; c < n->schema.count;c++)
                PFarray_printf (dot, "%s%s", c ? " | " : "(",
                                *((char**) PFarray_at (n->sem.ref_tbl.tatts,
                                                       c)));
            PFarray_printf (dot, ")\\ntype ");
            for (c = 0; c < n->schema.count;c++)
                PFarray_printf (dot, "%s%s", c ? " | " : "(",
                                PFalg_simple_type_str (
                                    n->schema.items[c].type));
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
        case la_to:
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
        case la_rowrank:
        case la_rank:
            PFarray_printf (dot, "%s (%s:<", a_id[n->kind],
                            PFatt_str (n->sem.sort.res));

            if (PFord_count (n->sem.sort.sortby))
                PFarray_printf (dot, "%s%s",
                                PFatt_str (
                                    PFord_order_col_at (
                                        n->sem.sort.sortby, 0)),
                                PFord_order_dir_at (
                                    n->sem.sort.sortby, 0) == DIR_ASC
                                ? "" : " (desc)");

            for (c = 1; c < PFord_count (n->sem.sort.sortby); c++)
                PFarray_printf (dot, ", %s%s",
                                PFatt_str (
                                    PFord_order_col_at (
                                        n->sem.sort.sortby, c)),
                                PFord_order_dir_at (
                                    n->sem.sort.sortby, c) == DIR_ASC
                                ? "" : " (desc)");

            PFarray_printf (dot, ">");

            if (n->sem.sort.part != att_NULL)
                PFarray_printf (dot, "/%s",
                                PFatt_str (n->sem.sort.part));

            PFarray_printf (dot, ")");
            break;

        case la_rowid:
            PFarray_printf (dot, "%s (%s)", a_id[n->kind],
                            PFatt_str (n->sem.rowid.res));
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
            PFarray_printf (dot, "%s %s::%s",
                            a_id[n->kind],
                            PFalg_axis_str (n->sem.step.spec.axis),
                            PFalg_node_kind_str (n->sem.step.spec.kind));

            if (n->sem.step.spec.kind == node_kind_elem ||
                n->sem.step.spec.kind == node_kind_attr)
                PFarray_printf (dot, "(%s)",
                                PFqname_str (n->sem.step.spec.qname));
            else if (n->sem.step.spec.kind == node_kind_pi &&
                     PFqname_loc (n->sem.step.spec.qname))
                PFarray_printf (dot, "(%s)", PFqname_loc (n->sem.step.spec.qname));
            else
                PFarray_printf (dot, "()");

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
            PFarray_printf (dot, "%s (%s:<%s>)",
                            a_id[n->kind],
                            PFatt_str (n->sem.doc_tbl.res),
                            PFatt_str (n->sem.doc_tbl.att));
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

        case la_fun_call:
            PFarray_printf (dot,
                            "%s function \\\"%s\\\" (",
                            PFalg_fun_call_kind_str (n->sem.fun_call.kind),
                            PFqname_uri_str (n->sem.fun_call.qname));
            for (unsigned int i = 0; i < n->schema.count; i++)
                PFarray_printf (dot, "%s%s",
                                i?", ":"",
                                PFatt_str (n->schema.items[i].name));
            PFarray_printf (dot,
                            ")\\n(loop: %s)",
                            PFatt_str (n->sem.fun_call.iter));
            break;

        case la_fun_param:
            PFarray_printf (dot, "%s (", a_id[n->kind]);
            for (unsigned int i = 0; i < n->schema.count; i++)
                PFarray_printf (dot, "%s%s",
                                i?", ":"",
                                PFatt_str (n->schema.items[i].name));
            PFarray_printf (dot, ")");
            break;

        case la_frag_extract:
        case la_fun_frag_param:
            PFarray_printf (dot, "%s (referencing column %i)",
                            a_id[n->kind], n->sem.col_ref.pos);
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
        case la_error:
            PFarray_printf (dot, "%s", a_id[n->kind]);
            break;

        case la_cross_mvd:
            PFoops (OOPS_FATAL,
                    "clone column aware cross product operator is "
                    "only allowed inside mvd optimization!");
    }

    if (prop_args) {

        char *fmt = prop_args;
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
            fmt = prop_args;

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
                bool fst;
                fst = true;
                /* list required value columns and their values */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_att_t att = n->schema.items[i].name;
                    if (PFprop_req_bool_val (n->prop, att)) {
                        PFarray_printf (
                            dot,
                            fst ? "\\nreq. val: %s=%s " : ", %s=%s ",
                            PFatt_str (att),
                            PFprop_req_bool_val_val (n->prop, att)
                            ?"true":"false");
                        fst = false;
                    }
                }
                fst = true;
                /* list order columns */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_att_t att = n->schema.items[i].name;
                    if (PFprop_req_order_col (n->prop, att)) {
                        PFarray_printf (
                            dot,
                            fst ? "\\norder col: %s" : ", %s",
                            PFatt_str (att));
                        fst = false;
                    }
                }
                fst = true;
                /* list bijective columns */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_att_t att = n->schema.items[i].name;
                    if (PFprop_req_bijective_col (n->prop, att)) {
                        PFarray_printf (
                            dot,
                            fst ? "\\nbijective col: %s" : ", %s",
                            PFatt_str (att));
                        fst = false;
                    }
                }
                fst = true;
                /* list multi-col columns */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_att_t att = n->schema.items[i].name;
                    if (PFprop_req_multi_col_col (n->prop, att)) {
                        PFarray_printf (
                            dot,
                            fst ? "\\nmulti-col col: %s" : ", %s",
                            PFatt_str (att));
                        fst = false;
                    }
                }
                fst = true;
                /* list filter columns */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_att_t att = n->schema.items[i].name;
                    if (PFprop_req_filter_col (n->prop, att)) {
                        PFarray_printf (
                            dot,
                            fst ? "\\nfilter col: %s" : ", %s",
                            PFatt_str (att));
                        fst = false;
                    }
                }
                fst = true;
                /* list value columns */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_att_t att = n->schema.items[i].name;
                    if (PFprop_req_value_col (n->prop, att)) {
                        PFarray_printf (
                            dot,
                            fst ? "\\nvalue col: %s" : ", %s",
                            PFatt_str (att));
                        fst = false;
                    }
                }

                /* node properties */

                fst = true;
                /* node content queried */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_att_t att = n->schema.items[i].name;
                    if (PFprop_node_property (n->prop, att) &&
                        PFprop_node_content_queried (n->prop, att)) {
                        PFarray_printf (
                            dot,
                            fst ? "\\nnode content queried: %s" : ", %s",
                            PFatt_str (att));
                        fst = false;
                    }
                }
                fst = true;
                /* node and its subtree serialized */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_att_t att = n->schema.items[i].name;
                    if (PFprop_node_property (n->prop, att) &&
                        PFprop_node_serialize (n->prop, att)) {
                        PFarray_printf (
                            dot,
                            fst ? "\\nnode serialized: %s" : ", %s",
                            PFatt_str (att));
                        fst = false;
                    }
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
        /* Avoid printing the fragment info to make the graphs
           more readable. */
        if (print_frag_info &&
            (n->child[c]->kind == la_frag_union ||
             n->child[c]->kind == la_empty_frag ||
             (n->kind == la_fcns && c == 1 && n->child[c]->kind == la_nil)))
            continue;
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
        /* Avoid printing the fragment info to make the graphs
           more readable. */
        if (print_frag_info &&
            (n->child[c]->kind == la_frag_union ||
             n->child[c]->kind == la_empty_frag ||
             (n->kind == la_fcns && c == 1 && n->child[c]->kind == la_nil)))
            continue;
        if (!n->child[c]->bit_dag)
            la_dot (dot, n->child[c], print_frag_info, prop_args);
    }
}

/**
 * Print algebra tree in XML notation.
 * @param xml Array into which we print
 * @param n The current node to print (function is recursive)
 */
static void
la_xml (PFarray_t *xml, PFla_op_t *n, char *prop_args)
{
    unsigned int c;

    assert(n->node_id);

    for (c = 0; c < PFLA_OP_MAXCHILD && n->child[c] != 0; c++)
        if (!n->child[c]->bit_dag)
            la_xml (xml, n->child[c], prop_args);

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
    for (unsigned int i = 0; i < n->schema.count; i++)
        PFarray_printf (xml, "      <col name=\"%s\" types=\"%s\"/>\n",
                        PFatt_str (n->schema.items[i].name),
                        xml_literal_list (n->schema.items[i].type));
    PFarray_printf (xml, "    </schema>\n");

    if (prop_args) {

        char *fmt = prop_args;
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
            fmt = prop_args;

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
                    if (PFprop_req_bool_val (n->prop, att))
                        PFarray_printf (
                            xml,
                            "      <required attr=\"%s\" value=\"%s\"/>\n",
                            PFatt_str (att),
                            PFprop_req_bool_val_val (n->prop, att)
                            ?"true":"false");
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
                    PFarray_printf (xml, "      <duplicates"
                                                " allowed=\"yes\"/>\n");
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
            for (unsigned int i = 0; i < n->schema.count; i++)
                PFarray_printf (xml,
                                "      <column name=\"%s\" type=\"%s\""
                                             " new=\"true\"/>\n",
                                PFatt_str (n->schema.items[i].name),
                                xml_literal_list (n->schema.items[i].type));

            PFarray_printf (xml, "    </content>\n");
            break;


        case la_ref_tbl :

            /* todo: only print the properties here, if the
               "general property output" is not enabled (e.g. via -f<format>)

               NB: the keys-property is an inherent information
                   for the la_ref_tbl-op
            */
            PFarray_printf (xml, "    <properties>\n");
            PFarray_printf (xml, "      <keys>\n");
            /* list the keys of this table */
            for (unsigned int c = 0;
                 c < PFarray_last (n->sem.ref_tbl.keys);
                 c++)
            {

                int keyPos = *((int*) PFarray_at (n->sem.ref_tbl.keys, c));
                PFalg_schm_item_t schemaItem = n->schema.items[keyPos];
                PFalg_att_t keyName = schemaItem.name;

                PFarray_printf (xml, "    <key>\n");
                PFarray_printf (xml,
                                 "          <column name=\"%s\""
                                            " position=\"%i\"/>\n",
                                PFatt_str(keyName),
                                 1);
                PFarray_printf (xml, "    </key>\n");
            }
            PFarray_printf (xml, "      </keys>\n");
            PFarray_printf (xml, "    </properties>\n");


            PFarray_printf (xml, "    <content>\n");
            PFarray_printf (xml, "      <table name=\"%s\">\n",
                                 n->sem.ref_tbl.name);
            /* list the attributes of this table */
            for (c = 0; c < n->schema.count;c++)
            {
                PFarray_printf (xml,
                                "        <column name=\"%s\""
                                         " tname=\"%s\" type=\"%s\"/>\n",
                                PFatt_str (n->schema.items[c].name),
                                *((char**) PFarray_at (n->sem.ref_tbl.tatts,
                                                       c)),
                                PFalg_simple_type_str (
                                    n->schema.items[c].type));
            }
            PFarray_printf (xml, "      </table>\n");
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
                                "      <column name=\"%s\""
                                       " function=\"partition\""
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
        case la_to:
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
        case la_rowrank:
        case la_rank:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n",
                            PFatt_str (n->sem.sort.res));

            for (c = 0; c < PFord_count (n->sem.sort.sortby); c++)
                PFarray_printf (xml,
                                "      <column name=\"%s\" function=\"sort\""
                                        " position=\"%u\" direction=\"%s\""
                                        " new=\"false\"/>\n",
                                PFatt_str (
                                    PFord_order_col_at (
                                        n->sem.sort.sortby, c)),
                                c+1,
                                PFord_order_dir_at (
                                    n->sem.sort.sortby, c) == DIR_ASC
                                ? "ascending" : "descending");

            if (n->sem.sort.part != att_NULL)
                PFarray_printf (xml,
                                "      <column name=\"%s\""
                                        " function=\"partition\""
                                        " new=\"false\"/>\n",
                                PFatt_str (n->sem.sort.part));

            PFarray_printf (xml, "    </content>\n");
            break;

        case la_rowid:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.rowid.res));
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
        {
            char  *axis   = PFalg_axis_str (n->sem.step.spec.axis),
                  *kind   = PFalg_node_kind_str (n->sem.step.spec.kind),
                  *prefix = NULL,
                  *uri    = NULL,
                  *local  = NULL;

            if (n->sem.step.spec.kind == node_kind_elem ||
                n->sem.step.spec.kind == node_kind_attr) {
                prefix = PFqname_prefix (n->sem.step.spec.qname);
                uri    = PFqname_uri (n->sem.step.spec.qname);
                local  = PFqname_loc (n->sem.step.spec.qname);
            } else if (n->sem.step.spec.kind == node_kind_pi)
                local  = PFqname_loc (n->sem.step.spec.qname);

            PFarray_printf (xml,
                            "    <content>\n"
                            "      <step axis=\"%s\" kind=\"%s\"",
                            axis, kind);
            if (prefix) {
                PFarray_printf (xml, " prefix=\"%s\"", prefix);
                PFarray_printf (xml, " uri=\"%s\"", uri);
            }
            if (local) {
                PFarray_printf (xml, " name=\"%s\"", local);
            }

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
                                "      <column name=\"%s\""
                                       " function=\"iter\"/>\n"
                                "      <column name=\"%s\""
                                       " function=\"item\"/>\n"
                                "    </content>\n",
                                PFatt_str (n->sem.step.iter),
                                PFatt_str (n->sem.step.item));
            else
                PFarray_printf (xml,
                                "/>\n"
                                "      <column name=\"%s\" new=\"true\"/>\n"
                                "      <column name=\"%s\""
                                       " function=\"item\"/>\n"
                                "    </content>\n",
                                PFatt_str (n->sem.step.item_res),
                                PFatt_str (n->sem.step.item));
        }   break;

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
                            "      <column name=\"%s\" new=\"false\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.doc_tbl.res),
                            PFatt_str (n->sem.doc_tbl.att));
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
                            "      <column name=\"%s\""
                                   " function=\"content item\"/>\n"
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

        case la_error:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"false\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.iter_pos_item.item));

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

        case la_fun_call:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <function uri=\"%s\" name=\"%s\"/>\n"
                            "      <kind name=\"%s\"/>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "    </content>\n",
                            PFqname_uri (n->sem.fun_call.qname),
                            PFqname_loc (n->sem.fun_call.qname),
                            PFalg_fun_call_kind_str (n->sem.fun_call.kind),
                            PFatt_str (n->sem.fun_call.iter));
            break;

        case la_fun_param:
            PFarray_printf (xml, "    <content>\n");
            for (c = 0; c < n->schema.count; c++)
                PFarray_printf (xml,
                                "      <column name=\"%s\" position=\"%u\"/>\n",
                                PFatt_str (n->schema.items[c].name), c);
            PFarray_printf (xml, "    </content>\n");
            break;

        case la_frag_extract:
        case la_fun_frag_param:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column reference=\"%i\"/>\n"
                            "    </content>\n",
                            n->sem.col_ref.pos);
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
PFla_dot (FILE *f, PFla_op_t *root, char *prop_args)
{
    if (root) {
        /* initialize array to hold dot output */
        PFarray_t *dot = PFarray (sizeof (char), 32000);

        PFarray_printf (dot, "digraph XQueryAlgebra {\n"
                             "ordering=out;\n"
                             "node [shape=box];\n"
                             "node [height=0.1];\n"
                             "node [width=0.2];\n"
                             "node [style=filled];\n"
                             "node [color=\"#C0C0C0\"];\n"
                             "node [fontsize=10];\n"
                             "edge [fontsize=9];\n"
                             "edge [dir=back];\n");

        create_node_id (root);
        la_dot (dot, root, getenv("PF_DEBUG_PRINT_FRAG") != NULL, prop_args);
        PFla_dag_reset (root);
        reset_node_id (root);

        /* add domain subdomain relationships if required */
        if (prop_args) {
            char *fmt = prop_args;
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
PFla_xml (FILE *f, PFla_op_t *root, char *prop_args)
{
    if (root) {

        /* initialize array to hold dot output */
        PFarray_t *xml = PFarray (sizeof (char), 64000);

        PFarray_printf (xml, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        PFarray_printf (xml, "<logical_query_plan unique_names=\"%s\">\n",
                        PFalg_is_unq_name (root->schema.items[0].name)
                        ? "true" : "false");

        /* add domain subdomain relationships if required */
        if (prop_args) {
            char *fmt = prop_args;
            while (*fmt) {
                if (*fmt == '+' || *fmt == 'D') {
                        PFprop_write_dom_rel_xml (xml, root->prop);
                        break;
                }
                fmt++;
            }
        }

        create_node_id (root);
        la_xml (xml, root, prop_args);
        PFla_dag_reset (root);
        reset_node_id (root);
        PFla_dag_reset (root);

        PFarray_printf (xml, "</logical_query_plan>\n");
        /* put content of array into file */
        fprintf (f, "%s", (char *) xml->base);
    }
}

/* vim:set shiftwidth=4 expandtab: */
