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
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2010 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id$
 */

/* always include pf_config.h first! */
#include "pf_config.h"
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

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/** Node names to print out for all the Algebra tree nodes. */
static char *a_id[]  = {
      [la_serialize_seq]    = "SERIALIZE"
    , [la_serialize_rel]    = "REL SERIALIZE"
    , [la_side_effects]     = "SIDE EFFECTS"
    , [la_lit_tbl]          = "TBL"
    , [la_empty_tbl]        = "EMPTY_TBL"
    , [la_ref_tbl ]         = "table"
    , [la_attach]           = "Attach"
    , [la_cross]            = "Cross"
    , [la_eqjoin]           = "Join"
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
    , [la_aggr]             = "AGGR"
    , [la_rownum]           = "ROWNUM"
    , [la_rowrank]          = "ROWRANK"
    , [la_rank]             = "RANK"
    , [la_rowid]            = "ROWID"
    , [la_type]             = "TYPE"
    , [la_type_assert]      = "type assertion"
    , [la_cast]             = "CAST"
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
    , [la_nil]              = "nil"
    , [la_cache]            = "cache"
    , [la_trace]            = "trace"
    , [la_trace_items]      = "trace_items"
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
    , [la_internal_op]      = "!!!INTERNAL OP!!!"
    , [la_string_join]      = "fn:string_join"
    , [la_dummy]            = "DUMMY"
};

/* XML node names to print out for all kinds */
static char *xml_id[]  = {
      [la_serialize_seq]    = "serialize sequence"
    , [la_serialize_rel]    = "serialize relation"
    , [la_side_effects]     = "observe side effects"
    , [la_lit_tbl]          = "table"
    , [la_empty_tbl]        = "empty_tbl"
    , [la_ref_tbl ]         = "ref_tbl"
    , [la_attach]           = "attach"
    , [la_cross]            = "cross"
    , [la_eqjoin]           = "eqjoin"
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
    , [la_aggr]             = "aggr"
    , [la_rownum]           = "rownum"
    , [la_rowrank]          = "rowrank"
    , [la_rank]             = "rank"
    , [la_rowid]            = "rowid"
    , [la_type]             = "type"
    , [la_type_assert]      = "type assertion"
    , [la_cast]             = "cast"
    , [la_step]             = "XPath step"
    , [la_step_join]        = "path step join"
    , [la_guide_step]       = "XPath step (with guide information)"
    , [la_guide_step_join]  = "path step join (with guide information)"
    , [la_doc_index_join]   = "document index join"
    , [la_doc_tbl]          = "document table access"
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
    , [la_nil]              = "nil"
    , [la_cache]            = "cache"
    , [la_trace]            = "trace"
    , [la_trace_items]      = "trace items"
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
        {
            unsigned int split = 29;
            if (strlen(a.val.str) > split)
                PFarray_printf (s, "\\\"%s\\n%s\\\"",
                                PFesc_string (PFstrndup (a.val.str, split)),
                                PFesc_string (a.val.str+split));
            else
                PFarray_printf (s, "\\\"%s\\\"", PFesc_string (a.val.str));
        }   break;

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
la_dot (PFarray_t *dot, PFarray_t *side_effects,
        PFla_op_t *n, bool print_frag_info, char *prop_args, int id)
{
#define DOT (n->bit_in ? dot : side_effects)
    unsigned int c;
    assert(n->node_id);

    static char *color[] = {
          [la_serialize_seq]   = "#C0C0C0"
        , [la_serialize_rel]   = "#C0C0C0"
        , [la_side_effects]    = "#C0C0C0"
        , [la_lit_tbl]         = "#C0C0C0"
        , [la_empty_tbl]       = "#C0C0C0"
        , [la_ref_tbl]         = "#C0C0C0"
        , [la_attach]          = "#EEEEEE"
        , [la_cross]           = "#990000"
        , [la_eqjoin]          = "#00FF00"
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
        , [la_aggr]            = "#A0A0A0"
        , [la_rownum]          = "#FF0000"
        , [la_rowrank]         = "#FF0000"
        , [la_rank]            = "#FF3333"
        , [la_rowid]           = "#FF9999"
        , [la_type]            = "#C0C0C0"
        , [la_type_assert]     = "#C0C0C0"
        , [la_cast]            = "#C0C0C0"
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
        , [la_nil]             = "#FFFFFF"
        , [la_cache]           = "#FF5500"
        , [la_trace]           = "#FF5500"
        , [la_trace_items]     = "#FF5500"
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
        , [la_internal_op]     = "#FF0000"
        , [la_dummy]           = "#FFFFFF"
    };

    /* open up label */
    PFarray_printf (DOT, "node%i_%i [label=\"", id, n->node_id);

    /* the following line enables id printing to simplify comparison with
       generated XML plans */
    PFarray_printf (DOT, "id: %i\\n", n->node_id);

    /* create label */
    switch (n->kind)
    {
        case la_serialize_seq:
            PFarray_printf (DOT, "%s (%s) order by (%s)",
                            a_id[n->kind],
                            PFcol_str (n->sem.ser_seq.item),
                            PFcol_str (n->sem.ser_seq.pos));
            break;

        case la_serialize_rel:
            PFarray_printf (DOT, "%s (%s",
                            a_id[n->kind],
                            PFcol_str (clat (n->sem.ser_rel.items, 0)));
            for (c = 1; c < clsize (n->sem.ser_rel.items); c++)
                PFarray_printf (DOT, ", %s",
                                PFcol_str (clat (n->sem.ser_rel.items, c)));
            PFarray_printf (DOT, ")\\norder by (%s) partition by (%s)",
                            PFcol_str (n->sem.ser_rel.pos),
                            PFcol_str (n->sem.ser_rel.iter));
            break;

        case la_lit_tbl:
            /* list the columns of this table */
            PFarray_printf (DOT, "%s: (%s", a_id[n->kind],
                            PFcol_str (n->schema.items[0].name));

            for (c = 1; c < n->schema.count;c++)
                PFarray_printf (DOT, " | %s",
                                PFcol_str (n->schema.items[c].name));

            PFarray_printf (DOT, ")");

            /* print out tuples in table, if table is not empty */
            for (unsigned int i = 0; i < n->sem.lit_tbl.count; i++) {
                PFarray_printf (DOT, "\\n[");

                for (c = 0; c < n->sem.lit_tbl.tuples[i].count; c++) {
                    if (c != 0)
                        PFarray_printf (DOT, ",");
                    PFarray_printf (DOT, "%s",
                                    literal (n->sem.lit_tbl
                                                   .tuples[i].atoms[c]));
                }

                PFarray_printf (DOT, "]");
            }
            break;

        case la_empty_tbl:
            /* list the columns of this table */
            PFarray_printf (DOT, "%s: (%s", a_id[n->kind],
                            PFcol_str (n->schema.items[0].name));

            for (c = 1; c < n->schema.count;c++)
                PFarray_printf (DOT, " | %s",
                                PFcol_str (n->schema.items[c].name));

            PFarray_printf (DOT, ")");
            break;

        case la_ref_tbl :
            PFarray_printf (DOT, "%s %s: ",
                            a_id[n->kind],
                            n->sem.ref_tbl.name);
            for (c = 0; c < n->schema.count;c++)
                PFarray_printf (DOT, "%s%s%s:%s [%s]",
                                c ? "," : "(",
                                (c % 3) == 2 ? "\\n" : (c ? " " : ""),
                                PFcol_str (n->schema.items[c].name),
                                *((char**) PFarray_at (n->sem.ref_tbl.tcols,
                                                       c)),
                                PFalg_simple_type_str (
                                    n->schema.items[c].type));
            PFarray_printf (DOT, ")");
            break;

        case la_attach:
            PFarray_printf (DOT, "%s (%s), val: %s", a_id[n->kind],
                            PFcol_str (n->sem.attach.res),
                            literal (n->sem.attach.value));
            break;

        case la_eqjoin:
        case la_semijoin:
            PFarray_printf (DOT, "%s (%s = %s)",
                            a_id[n->kind],
                            PFcol_str (n->sem.eqjoin.col1),
                            PFcol_str (n->sem.eqjoin.col2));
            break;

        case la_thetajoin:
            /* overwrite standard node layout */
            PFarray_printf (DOT, "\", shape=polygon peripheries=2, label=\"");

            PFarray_printf (DOT, "%s", a_id[n->kind]);

            for (c = 0; c < n->sem.thetajoin.count; c++)
                PFarray_printf (DOT, "\\n(%s %s %s)",
                                PFcol_str (n->sem.thetajoin.pred[c].left),
                                comp_str (n->sem.thetajoin.pred[c].comp),
                                PFcol_str (n->sem.thetajoin.pred[c].right));
            break;

        case la_internal_op:
            /* interpret this operator as internal join */
            if (n->sem.eqjoin_opt.kind == la_eqjoin) {
#define proj_at(l,i) (*(PFalg_proj_t *) PFarray_at ((l),(i)))
                PFarray_printf (
                    DOT,
                    "%s (%s:%s = %s)",
                    a_id[n->kind],
                    PFcol_str (proj_at(n->sem.eqjoin_opt.lproj,0).new),
                    PFcol_str (proj_at(n->sem.eqjoin_opt.lproj,0).old),
                    PFcol_str (proj_at(n->sem.eqjoin_opt.rproj,0).old));
                PFarray_printf (DOT, "\\nleft proj: (");
                for (unsigned int i = 1;
                     i < PFarray_last (n->sem.eqjoin_opt.lproj);
                     i++)
                    PFarray_printf (
                        DOT,
                        "%s:%s%s",
                        PFcol_str (proj_at(n->sem.eqjoin_opt.lproj,i).new),
                        PFcol_str (proj_at(n->sem.eqjoin_opt.lproj,i).old),
                        i+1 == PFarray_last (n->sem.eqjoin_opt.lproj)
                        ? "" : ", ");
                PFarray_printf (DOT, ")");
                PFarray_printf (DOT, "\\nright proj: (");
                for (unsigned int i = 1;
                     i < PFarray_last (n->sem.eqjoin_opt.rproj);
                     i++)
                    PFarray_printf (
                        DOT,
                        "%s:%s%s",
                        PFcol_str (proj_at(n->sem.eqjoin_opt.rproj,i).new),
                        PFcol_str (proj_at(n->sem.eqjoin_opt.rproj,i).old),
                        i+1 == PFarray_last (n->sem.eqjoin_opt.rproj)
                        ? "" : ", ");
                PFarray_printf (DOT, ")");
            }
            /* interpret this operator as internal cross product */
            else if (n->sem.eqjoin_opt.kind == la_cross)
                PFarray_printf (DOT, "%s", a_id[n->kind]);
            /* interpret this operator as internal rank operator */
            else if (n->sem.eqjoin_opt.kind == la_rank) {
                PFarray_printf (
                    DOT,
                    "%s (%s<length %i>)",
                    a_id[n->kind],
                    PFcol_str (n->sem.rank_opt.res),
                    PFarray_last (n->sem.rank_opt.sortby));
            }
            break;

        case la_project:
            {
                int startpos = PFarray_last (DOT),
                    curpos;
                char *sep,
                     *space    = ", ",
                     *newline  = ",\\n";
                /* print first column */
                if (n->sem.proj.items[0].new != n->sem.proj.items[0].old)
                    PFarray_printf (DOT, "%s (%s:%s", a_id[n->kind],
                                    PFcol_str (n->sem.proj.items[0].new),
                                    PFcol_str (n->sem.proj.items[0].old));
                else
                    PFarray_printf (DOT, "%s (%s", a_id[n->kind],
                                    PFcol_str (n->sem.proj.items[0].old));

                for (c = 1; c < n->sem.proj.count; c++) {
                    curpos = PFarray_last (DOT);
                    if (curpos - startpos > 42) {
                        sep = newline;
                        startpos = curpos;
                    }
                    else
                        sep = space;

                    if (n->sem.proj.items[c].new != n->sem.proj.items[c].old)
                        PFarray_printf (DOT, "%s%s:%s",
                                        sep,
                                        PFcol_str (n->sem.proj.items[c].new),
                                        PFcol_str (n->sem.proj.items[c].old));
                    else
                        PFarray_printf (DOT, "%s%s",
                                        sep,
                                        PFcol_str (n->sem.proj.items[c].old));
                }

                PFarray_printf (DOT, ")");
            } break;

        case la_select:
            PFarray_printf (DOT, "%s (%s)", a_id[n->kind],
                            PFcol_str (n->sem.select.col));
            break;

        case la_pos_select:
            PFarray_printf (DOT, "%s (%i, <", a_id[n->kind],
                            n->sem.pos_sel.pos);

            if (PFord_count (n->sem.pos_sel.sortby))
                PFarray_printf (DOT, "%s%s",
                                PFcol_str (
                                    PFord_order_col_at (
                                        n->sem.pos_sel.sortby, 0)),
                                PFord_order_dir_at (
                                    n->sem.pos_sel.sortby, 0) == DIR_ASC
                                ? "" : " (desc)");

            for (c = 1; c < PFord_count (n->sem.pos_sel.sortby); c++)
                PFarray_printf (DOT, ", %s%s",
                                PFcol_str (
                                    PFord_order_col_at (
                                        n->sem.pos_sel.sortby, c)),
                                PFord_order_dir_at (
                                    n->sem.pos_sel.sortby, c) == DIR_ASC
                                ? "" : " (desc)");

            PFarray_printf (DOT, ">");

            if (n->sem.pos_sel.part != col_NULL)
                PFarray_printf (DOT, "/%s",
                                PFcol_str (n->sem.pos_sel.part));

            PFarray_printf (DOT, ")");
            break;

            break;

        case la_fun_1to1:
            PFarray_printf (DOT, "%s [%s] (%s:<", a_id[n->kind],
                            PFalg_fun_str (n->sem.fun_1to1.kind),
                            PFcol_str (n->sem.fun_1to1.res));
            for (c = 0; c < clsize (n->sem.fun_1to1.refs);c++)
                PFarray_printf (DOT, "%s%s",
                                c ? ", " : "",
                                PFcol_str (clat (n->sem.fun_1to1.refs, c)));
            PFarray_printf (DOT, ">)");
            break;

        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_to:
            PFarray_printf (DOT, "%s (%s:<%s, %s>)", a_id[n->kind],
                            PFcol_str (n->sem.binary.res),
                            PFcol_str (n->sem.binary.col1),
                            PFcol_str (n->sem.binary.col2));
            break;

        case la_bool_not:
            PFarray_printf (DOT, "%s (%s:<%s>)", a_id[n->kind],
                            PFcol_str (n->sem.unary.res),
                            PFcol_str (n->sem.unary.col));
            break;

        case la_aggr:
            /* overwrite standard node layout */
            PFarray_printf (DOT, "\", shape=polygon peripheries=2, label=\"");

            PFarray_printf (DOT, "%s", a_id[n->kind]);
            if (n->sem.aggr.part != col_NULL)
                PFarray_printf (DOT, " / %s",
                                PFcol_str (n->sem.aggr.part));

            for (c = 0; c < n->sem.aggr.count; c++)
                PFarray_printf (DOT, "\\n%s = %s (%s)",
                                PFcol_str (n->sem.aggr.aggr[c].res),
                                PFalg_aggr_kind_str (n->sem.aggr.aggr[c].kind),
                                n->sem.aggr.aggr[c].col
                                ? PFcol_str (n->sem.aggr.aggr[c].col) : "");
            break;

        case la_rownum:
        case la_rowrank:
        case la_rank:
            PFarray_printf (DOT, "%s (%s:<", a_id[n->kind],
                            PFcol_str (n->sem.sort.res));

            if (PFord_count (n->sem.sort.sortby))
                PFarray_printf (DOT, "%s%s",
                                PFcol_str (
                                    PFord_order_col_at (
                                        n->sem.sort.sortby, 0)),
                                PFord_order_dir_at (
                                    n->sem.sort.sortby, 0) == DIR_ASC
                                ? "" : " (desc)");

            for (c = 1; c < PFord_count (n->sem.sort.sortby); c++)
                PFarray_printf (DOT, ", %s%s",
                                PFcol_str (
                                    PFord_order_col_at (
                                        n->sem.sort.sortby, c)),
                                PFord_order_dir_at (
                                    n->sem.sort.sortby, c) == DIR_ASC
                                ? "" : " (desc)");

            PFarray_printf (DOT, ">");

            if (n->sem.sort.part != col_NULL)
                PFarray_printf (DOT, "/%s",
                                PFcol_str (n->sem.sort.part));

            PFarray_printf (DOT, ")");
            break;

        case la_rowid:
            PFarray_printf (DOT, "%s (%s)", a_id[n->kind],
                            PFcol_str (n->sem.rowid.res));
            break;

        case la_type:
            PFarray_printf (DOT, "%s (%s:<%s>), type: %s", a_id[n->kind],
                            PFcol_str (n->sem.type.res),
                            PFcol_str (n->sem.type.col),
                            PFalg_simple_type_str (n->sem.type.ty));
            break;

        case la_type_assert:
            PFarray_printf (DOT, "%s (%s), type: %i", a_id[n->kind],
                            PFcol_str (n->sem.type.col),
                            n->sem.type.ty);
            break;

        case la_cast:
            PFarray_printf (DOT, "%s (%s%s%s%s), type: %s", a_id[n->kind],
                            n->sem.type.res?PFcol_str(n->sem.type.res):"",
                            n->sem.type.res?":<":"",
                            PFcol_str (n->sem.type.col),
                            n->sem.type.res?">":"",
                            PFalg_simple_type_str (n->sem.type.ty));
            break;

        case la_guide_step:
        case la_guide_step_join:
            /* overwrite standard node layout */
            PFarray_printf (DOT, "\", fontcolor=\"#FFFFFF\", label=\"");
        case la_step:
        case la_step_join:
            PFarray_printf (DOT, "%s %s::%s",
                            a_id[n->kind],
                            PFalg_axis_str (n->sem.step.spec.axis),
                            PFalg_node_kind_str (n->sem.step.spec.kind));

            if (n->sem.step.spec.kind == node_kind_elem ||
                n->sem.step.spec.kind == node_kind_attr)
                PFarray_printf (DOT, "(%s)",
                                PFqname_str (n->sem.step.spec.qname));
            else if (n->sem.step.spec.kind == node_kind_pi &&
                     PFqname_loc (n->sem.step.spec.qname))
                PFarray_printf (DOT, "(%s)", PFqname_loc (n->sem.step.spec.qname));
            else
                PFarray_printf (DOT, "()");

            /* print guide info */
            if (n->kind == la_guide_step ||
                n->kind == la_guide_step_join) {
                bool first = true;
                PFarray_printf (DOT, " - (");

                for (unsigned int i = 0; i < n->sem.step.guide_count; i++) {
                    PFarray_printf (DOT, "%s%i", first ? "" : ", ",
                                    n->sem.step.guides[i]->guide);
                    first = false;
                }
                PFarray_printf (DOT, ") ");
            }

            if (n->kind == la_step || n->kind == la_guide_step)
                PFarray_printf (DOT, "(%s, %s%s%s)",
                                PFcol_str (n->sem.step.iter),
                                PFcol_str (n->sem.step.item_res),
                                n->sem.step.item_res != n->sem.step.item
                                ? ":" : "",
                                n->sem.step.item_res != n->sem.step.item
                                ? PFcol_str (n->sem.step.item) : "");
            else
                PFarray_printf (DOT, "(%s:%s)",
                                PFcol_str (n->sem.step.item_res),
                                PFcol_str (n->sem.step.item));

            if (LEVEL_KNOWN(n->sem.step.level))
                PFarray_printf (DOT, "\\nlevel=%i", n->sem.step.level);
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
            PFarray_printf (DOT, "%s (%s:<%s, %s>)",
                            name,
                            PFcol_str (n->sem.doc_join.item_res),
                            PFcol_str (n->sem.doc_join.item),
                            PFcol_str (n->sem.doc_join.item_doc));
        }   break;

        case la_doc_tbl:
        {
            char *name = NULL;

            switch (n->sem.doc_tbl.kind) {
                case alg_dt_doc: name = "fn:doc";        break;
                case alg_dt_col: name = "fn:collection"; break;
            }
            PFarray_printf (DOT, "%s (%s:<%s>)",
                            name,
                            PFcol_str (n->sem.doc_tbl.res),
                            PFcol_str (n->sem.doc_tbl.col));
        }   break;

        case la_doc_access:
            PFarray_printf (DOT, "%s ", a_id[n->kind]);

            switch (n->sem.doc_access.doc_col)
            {
                case doc_atext:
                    PFarray_printf (DOT, "attribute value");
                    break;
                case doc_text:
                    PFarray_printf (DOT, "textnode content");
                    break;
                case doc_comm:
                    PFarray_printf (DOT, "comment text");
                    break;
                case doc_pi_text:
                    PFarray_printf (DOT, "processing instruction");
                    break;
                case doc_qname:
                    PFarray_printf (DOT, "qname");
                    break;
                case doc_atomize:
                    PFarray_printf (DOT, "atomize");
                    break;
                default: PFoops (OOPS_FATAL,
                        "unknown document access in DOT output");
            }

            PFarray_printf (DOT, " (%s:<%s>)",
                            PFcol_str (n->sem.doc_access.res),
                            PFcol_str (n->sem.doc_access.col));
            break;

        case la_twig:
        case la_element:
        case la_textnode:
        case la_comment:
        case la_trace_msg:
            PFarray_printf (DOT, "%s (%s, %s)",
                            a_id[n->kind],
                            PFcol_str (n->sem.iter_item.iter),
                            PFcol_str (n->sem.iter_item.item));
            break;

        case la_docnode:
            PFarray_printf (DOT, "%s (%s)",
                            a_id[n->kind],
                            PFcol_str (n->sem.docnode.iter));
            break;

        case la_attribute:
        case la_processi:
            PFarray_printf (DOT, "%s (%s, %s, %s)", a_id[n->kind],
                            PFcol_str (n->sem.iter_item1_item2.iter),
                            PFcol_str (n->sem.iter_item1_item2.item1),
                            PFcol_str (n->sem.iter_item1_item2.item2));
            break;

        case la_content:
        case la_trace_items:
            PFarray_printf (DOT,
                            "%s (%s, %s, %s)",
                            a_id[n->kind],
                            PFcol_str (n->sem.iter_pos_item.iter),
                            PFcol_str (n->sem.iter_pos_item.pos),
                            PFcol_str (n->sem.iter_pos_item.item));
            break;

        case la_error:
            PFarray_printf (DOT, "%s (%s)", a_id[n->kind],
                            PFcol_str (n->sem.err.col));
            break;

        case la_cache:
            PFarray_printf (DOT, "%s %s (%s, %s)", a_id[n->kind],
                            n->sem.cache.id,
                            PFcol_str (n->sem.cache.pos),
                            PFcol_str (n->sem.cache.item));
            break;

        case la_trace_map:
            PFarray_printf (DOT,
                            "%s (%s, %s)",
                            a_id[n->kind],
                            PFcol_str (n->sem.trace_map.inner),
                            PFcol_str (n->sem.trace_map.outer));
            break;

        case la_fun_call:
            if (PFqname_loc (n->sem.fun_call.qname))
                PFarray_printf (DOT,
                                "%s function \\\"%s\\\" (",
                                PFalg_fun_call_kind_str (n->sem.fun_call.kind),
                                PFqname_uri_str (n->sem.fun_call.qname));
            else
                PFarray_printf (DOT,
                                "%s function (",
                                PFalg_fun_call_kind_str (n->sem.fun_call.kind));
            for (unsigned int i = 0; i < n->schema.count; i++)
                PFarray_printf (DOT, "%s%s",
                                i?", ":"",
                                PFcol_str (n->schema.items[i].name));
            PFarray_printf (DOT,
                            ")\\n(loop: %s)",
                            PFcol_str (n->sem.fun_call.iter));
            break;

        case la_fun_param:
            PFarray_printf (DOT, "%s (", a_id[n->kind]);
            for (unsigned int i = 0; i < n->schema.count; i++)
                PFarray_printf (DOT, "%s%s",
                                i?", ":"",
                                PFcol_str (n->schema.items[i].name));
            PFarray_printf (DOT, ")");
            break;

        case la_frag_extract:
        case la_fun_frag_param:
            PFarray_printf (DOT, "%s (referencing column %i)",
                            a_id[n->kind], n->sem.col_ref.pos);
            break;

        case la_proxy:
            PFarray_printf (DOT, "%s %i (", a_id[n->kind], n->sem.proxy.kind);

            if (clsize (n->sem.proxy.new_cols))
                PFarray_printf (DOT, "%s",
                                PFcol_str (clat (n->sem.proxy.new_cols, 0)));

            for (c = 1; c < clsize (n->sem.proxy.new_cols); c++)
                PFarray_printf (DOT, ", %s",
                                PFcol_str (clat (n->sem.proxy.new_cols, c)));

            if (clsize (n->sem.proxy.req_cols))
                PFarray_printf (DOT, ")\\n(req cols: %s",
                                PFcol_str (clat (n->sem.proxy.req_cols, 0)));

            for (c = 1; c < clsize (n->sem.proxy.req_cols); c++)
                PFarray_printf (DOT, ", %s",
                                PFcol_str (clat (n->sem.proxy.req_cols, c)));

            PFarray_printf (DOT, ")");
            break;

        case la_string_join:
            PFarray_printf (DOT,
                            "%s\\n"
                            "%s <%s, %s>,\\n"
                            "<%s>,\\n"
                            "%s <%s, %s>",
                            a_id[n->kind],
                            PFcol_str (n->sem.string_join.iter_res),
                            PFcol_str (n->sem.string_join.iter),
                            PFcol_str (n->sem.string_join.iter_sep),
                            PFcol_str (n->sem.string_join.pos),
                            PFcol_str (n->sem.string_join.item_res),
                            PFcol_str (n->sem.string_join.item),
                            PFcol_str (n->sem.string_join.item_sep));
            break;

        case la_side_effects:
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
        case la_trace:
        case la_rec_fix:
        case la_rec_param:
        case la_rec_arg:
        case la_rec_base:
        case la_proxy_base:
        case la_dummy:
            PFarray_printf (DOT, "%s", a_id[n->kind]);
            break;
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
            if (*fmt == '+' || *fmt == 'N') {
                bool  started  = false;
                int   startpos = PFarray_last (DOT),
                      curpos;
                char *sep,
                     *space    = ", ",
                     *newline  = ",\\n";
                /* list columns marked const */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_col_t col  = n->schema.items[i].name;
                    char       *name = PFprop_name_origin (n->prop, col);
                    if (name) {
                        curpos = PFarray_last (DOT);
                        if (!started) {
                            started = true;
                            sep = "\\nname origin: ";
                        }
                        else if (curpos - startpos > 42) {
                            sep = newline;
                            startpos = curpos;
                        }
                        else
                            sep = space;

                        PFarray_printf (DOT, 
                                        "%s%s=%s",
                                        sep,
                                        PFcol_str (col),
                                        name);
                    }
                }
            }
            if (*fmt == '+' || *fmt == 'A') {
                /* if present print cardinality */
                if (PFprop_card (n->prop))
                    PFarray_printf (DOT, "\\ncard: %i", PFprop_card (n->prop));
            }
            if (*fmt == '+' || *fmt == 'O') {
                /* list columns marked const */
                for (unsigned int i = 0;
                        i < PFprop_const_count (n->prop); i++)
                    PFarray_printf (DOT, i ? ", %s" : "\\nconst: %s",
                                    PFcol_str (
                                        PFprop_const_at (n->prop, i)));
            }
            if (*fmt == '+' || *fmt == 'I') {
                PFalg_collist_t *icols = PFprop_icols_to_collist (n->prop);

                /* list icols columns */
                for (unsigned int i = 0; i < clsize (icols); i++)
                    PFarray_printf (DOT, i ? ", %s" : "\\nicols: %s",
                                    PFcol_str (clat (icols, i)));
            }
            if (*fmt == '+' || *fmt == 'K') {
                PFalg_collist_t *keys = PFprop_keys_to_collist (n->prop);

                /* list keys columns */
                for (unsigned int i = 0; i < clsize (keys); i++)
                    PFarray_printf (DOT, i ? ", %s" : "\\nkeys: %s",
                                    PFcol_str (clat (keys, i)));
            }
            if (*fmt == '+' || *fmt == 'V') {
                bool fst;
                fst = true;
                /* list required value columns and their values */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_col_t col = n->schema.items[i].name;
                    if (PFprop_req_bool_val (n->prop, col)) {
                        PFarray_printf (
                            DOT,
                            fst ? "\\nreq. val: %s=%s " : ", %s=%s ",
                            PFcol_str (col),
                            PFprop_req_bool_val_val (n->prop, col)
                            ?"true":"false");
                        fst = false;
                    }
                }
                fst = true;
                /* list order columns */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_col_t col = n->schema.items[i].name;
                    if (PFprop_req_order_col (n->prop, col)) {
                        PFarray_printf (
                            DOT,
                            fst ? "\\norder col: %s" : ", %s",
                            PFcol_str (col));
                        fst = false;
                    }
                }
                fst = true;
                /* list bijective columns */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_col_t col = n->schema.items[i].name;
                    if (PFprop_req_bijective_col (n->prop, col)) {
                        PFarray_printf (
                            DOT,
                            fst ? "\\nbijective col: %s" : ", %s",
                            PFcol_str (col));
                        fst = false;
                    }
                }
                fst = true;
                /* list rank columns */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_col_t col = n->schema.items[i].name;
                    if (PFprop_req_rank_col (n->prop, col)) {
                        PFarray_printf (
                            DOT,
                            fst ? "\\nrank col: %s" : ", %s",
                            PFcol_str (col));
                        fst = false;
                    }
                }
                fst = true;
                /* list multi-col columns */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_col_t col = n->schema.items[i].name;
                    if (PFprop_req_multi_col_col (n->prop, col)) {
                        PFarray_printf (
                            DOT,
                            fst ? "\\nmulti-col col: %s" : ", %s",
                            PFcol_str (col));
                        fst = false;
                    }
                }
                fst = true;
                /* list filter columns */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_col_t col = n->schema.items[i].name;
                    if (PFprop_req_filter_col (n->prop, col)) {
                        PFarray_printf (
                            DOT,
                            fst ? "\\nfilter col: %s" : ", %s",
                            PFcol_str (col));
                        fst = false;
                    }
                }
                fst = true;
                /* list link columns */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_col_t col = n->schema.items[i].name;
                    if (PFprop_req_link_col (n->prop, col)) {
                        PFarray_printf (
                            DOT,
                            fst ? "\\nlink col: %s" : ", %s",
                            PFcol_str (col));
                        fst = false;
                    }
                }
                fst = true;
                /* list unique columns */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_col_t col = n->schema.items[i].name;
                    if (PFprop_req_unique_col (n->prop, col)) {
                        PFarray_printf (
                            DOT,
                            fst ? "\\nunique col: %s" : ", %s",
                            PFcol_str (col));
                        fst = false;
                    }
                }
                fst = true;
                /* list value columns */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_col_t col = n->schema.items[i].name;
                    if (PFprop_req_value_col (n->prop, col)) {
                        PFarray_printf (
                            DOT,
                            fst ? "\\nvalue col: %s" : ", %s",
                            PFcol_str (col));
                        fst = false;
                    }
                }

                /* node properties */

                fst = true;
                /* node content queried */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_col_t col = n->schema.items[i].name;
                    if (PFprop_node_property (n->prop, col) &&
                        PFprop_node_content_queried (n->prop, col)) {
                        PFarray_printf (
                            DOT,
                            fst ? "\\nnode content queried: %s" : ", %s",
                            PFcol_str (col));
                        fst = false;
                    }
                }
                fst = true;
                /* node and its subtree serialized */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_col_t col = n->schema.items[i].name;
                    if (PFprop_node_property (n->prop, col) &&
                        PFprop_node_serialize (n->prop, col)) {
                        PFarray_printf (
                            DOT,
                            fst ? "\\nnode serialized: %s" : ", %s",
                            PFcol_str (col));
                        fst = false;
                    }
                }
            }
            if (*fmt == '+' || *fmt == 'D') {
                /* list columns and their corresponding domains */
                for (unsigned int i = 0; i < n->schema.count; i++)
                    if (PFprop_dom (n->prop, n->schema.items[i].name)) {
                        PFarray_printf (DOT, i ? ", %s " : "\\ndom: %s ",
                                        PFcol_str (n->schema.items[i].name));
                        PFprop_write_domain (
                            DOT,
                            PFprop_dom (n->prop, n->schema.items[i].name));
                    }
                /* list columns and their corresponding lineage */
                for (unsigned int i = 0; i < n->schema.count; i++)
                    if (PFprop_lineage (n->prop, n->schema.items[i].name))
                        PFarray_printf (DOT, 
                                        i
                                        ? ", %s (%i.%s)"
                                        : "\\nlineage: %s (%i.%s)",
                                        PFcol_str (n->schema.items[i].name),
                                        PFprop_lineage (
                                            n->prop,
                                            n->schema.items[i].name)->node_id,
                                        PFcol_str (
                                            PFprop_lineage_col (
                                                n->prop,
                                                n->schema.items[i].name)));
            }
            if (*fmt == '+' || *fmt == 'F') {
                unsigned int fd_count = 0;
                /* list all functional depencies */
                for (unsigned int i = 0; i < n->schema.count; i++)
                    for (unsigned int j = 0; j < n->schema.count; j++)
                        if (i != j &&
                            PFprop_fd (n->prop,
                                       n->schema.items[i].name,
                                       n->schema.items[j].name)) {
                            if (fd_count == 0)
                                PFarray_printf (
                                    DOT,
                                    "\\nfunctional dependencies:");
                            else
                                PFarray_printf (DOT, ", ");
                            if (fd_count % 3 == 0)
                                PFarray_printf (DOT, "\\n");
                            fd_count++;
                            PFarray_printf (
                                DOT,
                                "%s => %s",
                                PFcol_str (n->schema.items[i].name),
                                PFcol_str (n->schema.items[j].name));
                        }
            }
            if (*fmt == '+' || *fmt == '[') {
                /* list columns and their unique names */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_col_t ori = n->schema.items[i].name;
                    PFalg_col_t unq = PFprop_unq_name (n->prop, ori);
                    if (unq) {
                        PFalg_col_t l_unq, r_unq;
                        PFarray_printf (
                            DOT,
                            i ? " , %s=%s" : "\\nO->U names: %s=%s",
                            PFcol_str (ori), PFcol_str (unq));

                        l_unq = PFprop_unq_name_left (n->prop, ori);
                        r_unq = PFprop_unq_name_right (n->prop, ori);

                        if (l_unq && l_unq != unq && r_unq && r_unq != unq)
                            PFarray_printf (DOT,
                                            " [%s|%s]",
                                            PFcol_str(l_unq),
                                            PFcol_str(r_unq));
                        else if (l_unq && l_unq != unq)
                            PFarray_printf (DOT, " [%s|", PFcol_str(l_unq));
                        else if (r_unq && r_unq != unq)
                            PFarray_printf (DOT, " |%s]", PFcol_str(r_unq));
                    }
                }
            }
            if (*fmt == '+' || *fmt == ']') {
                /* list columns and their original names */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_col_t unq = n->schema.items[i].name;
                    PFalg_col_t ori = PFprop_ori_name (n->prop, unq);
                    if (ori) {
                        PFalg_col_t l_ori, r_ori;
                        PFarray_printf (
                            DOT,
                            i ? " , %s=%s" : "\\nU->O names: %s=%s",
                            PFcol_str (unq), PFcol_str (ori));

                        l_ori = PFprop_ori_name_left (n->prop, unq);
                        r_ori = PFprop_ori_name_right (n->prop, unq);

                        if (l_ori && l_ori != ori && r_ori && r_ori != ori)
                            PFarray_printf (DOT,
                                            " [%s|%s]",
                                            PFcol_str(l_ori),
                                            PFcol_str(r_ori));
                        else if (l_ori && l_ori != ori)
                            PFarray_printf (DOT, " [%s|", PFcol_str(l_ori));
                        else if (r_ori && r_ori != ori)
                            PFarray_printf (DOT, " |%s]", PFcol_str(r_ori));
                    }
                }
            }
            if (*fmt == '+' || *fmt == 'S') {
                /* print whether columns do have to respect duplicates */
                if (PFprop_set (n->prop))
                    PFarray_printf (DOT, "\\nset");
            }
            if (*fmt == '+' || *fmt == 'L') {
                /* print columns that have a level information attached */
                bool first = true;
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_col_t col = n->schema.items[i].name;
                    int level = PFprop_level (n->prop, col);
                    if (LEVEL_KNOWN(level)) {
                        PFarray_printf (
                            DOT,
                            "%s %s=%i",
                            first ? "\\nlevel:" : ",",
                            PFcol_str (col),
                            level);
                        first = false;
                    }
                }
            }
            if (*fmt == '+' || *fmt == 'U') {
                PFguide_tree_t **guides;
                PFalg_col_t col;
                unsigned int i, j, count;
                bool first = true;

                for (i = 0; i < n->schema.count; i++) {
                    col = n->schema.items[i].name;
                    if (PFprop_guide (n->prop, col)) {

                        PFarray_printf (DOT, "%s %s:",
                                        first ? "\\nGUIDE:" : ",",
                                        PFcol_str(col));
                        first = false;

                        /* print guides */
                        count  = PFprop_guide_count (n->prop, col);
                        guides = PFprop_guide_elements (n->prop, col);
                        for (j = 0; j < count; j++)
                            PFarray_printf (DOT, " %i", guides[j]->guide);
                    }
                }
            }
            if (*fmt == '+' || *fmt == 'Y') {
                if (PFprop_ckeys_count (n->prop)) {
                    PFalg_collist_t *list;
                    unsigned int i, j;
                    bool first = true;

                    PFarray_printf (DOT, "\\ncomposite keys:");

                    for (i = 0; i < PFprop_ckeys_count (n->prop); i++) {
                        list = PFprop_ckey_at (n->prop, i);
                        first = true;
                        for (j = 0; j < clsize (list); j++) {
                            PFarray_printf (DOT, "%s%s",
                                            first ? "\\n<" : ", ",
                                            PFcol_str(clat (list, j)));
                            first = false;
                        }
                        PFarray_printf (DOT, ">");
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
    PFarray_printf (DOT, "\", color=\"%s\" ];\n", color[n->kind]);

    for (c = 0; c < PFLA_OP_MAXCHILD && n->child[c]; c++) {
        /* Avoid printing the fragment info to make the graphs
           more readable. */
        if (print_frag_info &&
            (n->child[c]->kind == la_frag_union ||
             n->child[c]->kind == la_empty_frag))
            continue;
        /* do not print the last list item (nil) */
        if (((n->kind == la_serialize_rel ||
              n->kind == la_side_effects ||
              n->kind == la_error ||
              n->kind == la_cache ||
              n->kind == la_trace) &&
             c == 0 &&
             n->child[c]->kind == la_nil) ||
            (n->kind == la_fcns &&
             c == 1 &&
             n->child[c]->kind == la_nil))
            continue;
        /* do not print empty side effects */
        if (n->kind == la_serialize_seq &&
            c == 0 &&
            n->child[c]->kind == la_side_effects &&
            n->child[c]->child[0]->kind == la_nil)
            continue;
        PFarray_printf (n->bit_in || n->child[c]->bit_in ? dot : side_effects,
                        "node%i_%i -> node%i_%i;\n",
                        id, n->node_id, id, n->child[c]->node_id);
    }

    /* create soft links */
    switch (n->kind)
    {
        case la_rec_arg:
            if (n->sem.rec_arg.base) {
                PFarray_printf (DOT,
                                "node%i_%i -> node%i_%i "
                                "[style=dashed label=seed dir=back];\n",
                                id,
                                n->sem.rec_arg.base->node_id,
                                id,
                                n->child[0]->node_id);
                PFarray_printf (DOT,
                                "node%i_%i -> node%i_%i "
                                "[style=dashed label=recurse];\n",
                                id,
                                n->child[1]->node_id,
                                id,
                                n->sem.rec_arg.base->node_id);
            }
            break;

        case la_proxy:
            if (n->sem.proxy.base1)
                PFarray_printf (DOT, "node%i_%i -> node%i_%i [style=dashed];\n",
                                id, n->node_id, id, n->sem.proxy.base1->node_id);

            if (n->sem.proxy.base2)
                PFarray_printf (DOT, "node%i_%i -> node%i_%i [style=dashed];\n",
                                id, n->node_id, id, n->sem.proxy.base2->node_id);

            if (n->sem.proxy.ref)
                PFarray_printf (DOT,
                                "node%i_%i -> node%i_%i "
                                "[style=dashed label=ref];\n",
                                id, n->node_id, id, n->sem.proxy.ref->node_id);
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
             n->child[c]->kind == la_empty_frag))
            continue;
        /* do not print the last list item (nil) */
        if (((n->kind == la_serialize_rel ||
              n->kind == la_side_effects ||
              n->kind == la_error ||
              n->kind == la_cache ||
              n->kind == la_trace) &&
             c == 0 &&
             n->child[c]->kind == la_nil) ||
            (n->kind == la_fcns &&
             c == 1 &&
             n->child[c]->kind == la_nil))
            continue;
        /* do not print empty side effects */
        if (n->kind == la_serialize_seq &&
            c == 0 &&
            n->child[c]->kind == la_side_effects &&
            n->child[c]->child[0]->kind == la_nil)
            continue;
        if (!n->child[c]->bit_dag)
            la_dot (dot,
                    side_effects,
                    n->child[c],
                    print_frag_info,
                    prop_args,
                    id);
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
                        PFcol_str (n->schema.items[i].name),
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
                /* list columns marked const */
                for (unsigned int i = 0;
                        i < PFprop_const_count (n->prop); i++)
                    PFarray_printf (xml,
                                    "      <const column=\"%s\">\n"
                                    "        %s\n"
                                    "      </const>\n",
                                    PFcol_str (
                                        PFprop_const_at (n->prop, i)),
                                    xml_literal (
                                        PFprop_const_val_at (n->prop, i)));
            }
            if (*fmt == '+' || *fmt == 'I') {
                PFalg_collist_t *icols = PFprop_icols_to_collist (n->prop);

                /* list icols columns */
                for (unsigned int i = 0; i < clsize (icols); i++)
                    PFarray_printf (xml, "      <icols column=\"%s\"/>\n",
                                    PFcol_str (clat (icols, i)));
            }
            if (*fmt == '+' || *fmt == 'K') {
                PFalg_collist_t *keys = PFprop_keys_to_collist (n->prop);

                /* list keys columns */
                for (unsigned int i = 0; i < clsize (keys); i++)
                    PFarray_printf (xml, "      <keys column=\"%s\"/>\n",
                                    PFcol_str (clat (keys, i)));
            }
            if (*fmt == '+' || *fmt == 'V') {
                /* list required value columns and their values */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    PFalg_col_t col = n->schema.items[i].name;
                    if (PFprop_req_bool_val (n->prop, col))
                        PFarray_printf (
                            xml,
                            "      <required attr=\"%s\" value=\"%s\"/>\n",
                            PFcol_str (col),
                            PFprop_req_bool_val_val (n->prop, col)
                            ?"true":"false");
                }
            }
            if (*fmt == '+' || *fmt == 'D') {
                /* list columns and their corresponding domains */
                for (unsigned int i = 0; i < n->schema.count; i++)
                    if (PFprop_dom (n->prop, n->schema.items[i].name)) {
                        PFarray_printf (xml, "      <domain attr=\"%s\" "
                                        "value=\"",
                                        PFcol_str (n->schema.items[i].name));
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
                            PFcol_str (n->sem.ser_seq.pos),
                            PFcol_str (n->sem.ser_seq.item));
            break;

        case la_serialize_rel:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"false\""
                                         " function=\"iter\"/>\n"
                            "      <column name=\"%s\" new=\"false\""
                                         " function=\"pos\"/>\n",
                            PFcol_str (n->sem.ser_rel.iter),
                            PFcol_str (n->sem.ser_rel.pos));

            for (c = 0; c < clsize (n->sem.ser_rel.items); c++)
                PFarray_printf (xml,
                                "      <column name=\"%s\" new=\"false\""
                                             " function=\"item\""
                                             " position=\"%i\"/>\n",
                                PFcol_str (clat (n->sem.ser_rel.items, c)),
                                c);

            PFarray_printf (xml, "    </content>\n");
            break;

        case la_lit_tbl:
            /* list the columns of this table */
            PFarray_printf (xml, "    <content>\n");

            for (c = 0; c < n->schema.count;c++) {
                PFarray_printf (xml,
                                "      <column name=\"%s\" new=\"true\">\n",
                                PFcol_str (n->schema.items[c].name));
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

            /* list the columns of this table */
            for (unsigned int i = 0; i < n->schema.count; i++)
                PFarray_printf (xml,
                                "      <column name=\"%s\" type=\"%s\""
                                             " new=\"true\"/>\n",
                                PFcol_str (n->schema.items[i].name),
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
            for (unsigned int key = 0;
                 key < PFarray_last (n->sem.ref_tbl.keys);
                 key++)
            {
                
                PFarray_printf (xml, "    <key>\n");

                PFarray_t * keyPositions = *((PFarray_t**) PFarray_at (n->sem.ref_tbl.keys, key));

                for (unsigned int i = 0;
                    i < PFarray_last (keyPositions);
                    i++)
                {
                    int keyPos = *((int*) PFarray_at (keyPositions, i));
                    PFalg_schm_item_t schemaItem = n->schema.items[keyPos];
                    PFalg_col_t keyName = schemaItem.name;

                    PFarray_printf (xml,
                                     "          <column name=\"%s\""
                                                " position=\"%i\"/>\n",
                                    PFcol_str(keyName),
                                    i);
                }
                PFarray_printf (xml, "    </key>\n");
            }
            PFarray_printf (xml, "      </keys>\n");
            PFarray_printf (xml, "    </properties>\n");


            PFarray_printf (xml, "    <content>\n");
            PFarray_printf (xml, "      <table name=\"%s\">\n",
                                 n->sem.ref_tbl.name);
            /* list the columns of this table */
            for (c = 0; c < n->schema.count;c++)
            {
                PFarray_printf (xml,
                                "        <column name=\"%s\""
                                         " tname=\"%s\" type=\"%s\"/>\n",
                                PFcol_str (n->schema.items[c].name),
                                *((char**) PFarray_at (n->sem.ref_tbl.tcols,
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
                            PFcol_str (n->sem.attach.res),
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
                            PFcol_str (n->sem.eqjoin.col1),
                            PFcol_str (n->sem.eqjoin.col2));
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
                                PFcol_str (n->sem.thetajoin.pred[c].left),
                                PFcol_str (n->sem.thetajoin.pred[c].right));
            PFarray_printf (xml, "    </content>\n");
            break;

        case la_internal_op:
#if 0
        /* this code only works for printing an internal eqjoin */
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"false\""
                                         " keep=\"%s\" position=\"1\"/>\n"
                            "      <column name=\"%s\" new=\"false\""
                                         " keep=\"%s\" position=\"2\"/>\n",
                            PFcol_str (proj_at(n->sem.eqjoin_opt.lproj,0).old),
                            proj_at(n->sem.eqjoin_opt.lproj,0).new ==
                            proj_at(n->sem.eqjoin_opt.lproj,0).old
                            ? "true" : "false",
                            PFcol_str (proj_at(n->sem.eqjoin_opt.rproj,0).old),
                            proj_at(n->sem.eqjoin_opt.rproj,0).new ==
                            proj_at(n->sem.eqjoin_opt.rproj,0).old
                            ? "true" : "false");
            for (c = 1; c < PFarray_last (n->sem.eqjoin_opt.lproj); c++) {
                PFalg_proj_t proj = proj_at(n->sem.eqjoin_opt.lproj,c);
                if (proj.new != proj.old)
                    PFarray_printf (
                        xml,
                        "      <column name=\"%s\" "
                                      "old_name=\"%s\" "
                                      "new=\"true\" function=\"left\"/>\n",
                        PFcol_str (proj.new),
                        PFcol_str (proj.old));
                else
                    PFarray_printf (
                        xml,
                        "      <column name=\"%s\" "
                                       "new=\"false\" function=\"left\"/>\n",
                        PFcol_str (proj.new));
            }
            for (c = 1; c < PFarray_last (n->sem.eqjoin_opt.rproj); c++) {
                PFalg_proj_t proj = proj_at(n->sem.eqjoin_opt.rproj,c);
                if (proj.new != proj.old)
                    PFarray_printf (
                        xml,
                        "      <column name=\"%s\" "
                                      "old_name=\"%s\" "
                                      "new=\"true\" function=\"right\"/>\n",
                        PFcol_str (proj.new),
                        PFcol_str (proj.old));
                else
                    PFarray_printf (
                        xml,
                        "      <column name=\"%s\" "
                                       "new=\"false\" function=\"right\"/>\n",
                        PFcol_str (proj.new));
            }
            PFarray_printf (xml, "    </content>\n");
#endif
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
                        PFcol_str (n->sem.proj.items[c].new),
                        PFcol_str (n->sem.proj.items[c].old));
                else
                    PFarray_printf (
                        xml,
                        "      <column name=\"%s\" new=\"false\"/>\n",
                        PFcol_str (n->sem.proj.items[c].old));

            PFarray_printf (xml, "    </content>\n");
            break;

        case la_select:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"false\"/>\n"
                            "    </content>\n",
                            PFcol_str (n->sem.select.col));
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
                                PFcol_str (
                                    PFord_order_col_at (
                                        n->sem.pos_sel.sortby, c)),
                                c+1,
                                PFord_order_dir_at (
                                    n->sem.pos_sel.sortby, c) == DIR_ASC
                                ? "ascending" : "descending");

            if (n->sem.pos_sel.part != col_NULL)
                PFarray_printf (xml,
                                "      <column name=\"%s\""
                                       " function=\"partition\""
                                       " new=\"false\"/>\n",
                                PFcol_str (n->sem.pos_sel.part));

            PFarray_printf (xml, "    </content>\n");
            break;

        case la_fun_1to1:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <kind name=\"%s\"/>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n",
                            PFalg_fun_str (n->sem.fun_1to1.kind),
                            PFcol_str (n->sem.fun_1to1.res));

            for (c = 0; c < clsize (n->sem.fun_1to1.refs); c++)
                PFarray_printf (xml,
                                "      <column name=\"%s\" new=\"false\""
                                             " position=\"%i\"/>\n",
                                PFcol_str (clat (n->sem.fun_1to1.refs, c)),
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
                            PFcol_str (n->sem.binary.res),
                            PFcol_str (n->sem.binary.col1),
                            PFcol_str (n->sem.binary.col2));
            break;

        case la_bool_not:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n"
                            "      <column name=\"%s\" new=\"false\"/>\n"
                            "    </content>\n",
                            PFcol_str (n->sem.unary.res),
                            PFcol_str (n->sem.unary.col));
            break;

        case la_aggr:
            PFarray_printf (xml, "    <content>\n");
            if (n->sem.aggr.part != col_NULL)
                PFarray_printf (xml,
                            "      <column name=\"%s\" function=\"partition\""
                                    " new=\"false\"/>\n",
                            PFcol_str (n->sem.aggr.part));
            for (c = 0; c < n->sem.aggr.count; c++) {
                PFarray_printf (xml,
                                "      <aggregate kind=\"%s\">\n"
                                "        <column name=\"%s\" new=\"true\"/>\n",
                                PFalg_aggr_kind_str (n->sem.aggr.aggr[c].kind),
                                PFcol_str (n->sem.aggr.aggr[c].res));
                if (n->sem.aggr.aggr[c].col)
                    PFarray_printf (xml,
                                    "        <column name=\"%s\" new=\"false\""
                                                   " function=\"item\"/>\n",
                                    PFcol_str (n->sem.aggr.aggr[c].col));
                PFarray_printf (xml, "      </aggregate>\n");
            }
            PFarray_printf (xml, "    </content>\n");
            break;

        case la_rownum:
        case la_rowrank:
        case la_rank:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n",
                            PFcol_str (n->sem.sort.res));

            for (c = 0; c < PFord_count (n->sem.sort.sortby); c++)
                PFarray_printf (xml,
                                "      <column name=\"%s\" function=\"sort\""
                                        " position=\"%u\" direction=\"%s\""
                                        " new=\"false\"/>\n",
                                PFcol_str (
                                    PFord_order_col_at (
                                        n->sem.sort.sortby, c)),
                                c+1,
                                PFord_order_dir_at (
                                    n->sem.sort.sortby, c) == DIR_ASC
                                ? "ascending" : "descending");

            if (n->sem.sort.part != col_NULL)
                PFarray_printf (xml,
                                "      <column name=\"%s\""
                                        " function=\"partition\""
                                        " new=\"false\"/>\n",
                                PFcol_str (n->sem.sort.part));

            PFarray_printf (xml, "    </content>\n");
            break;

        case la_rowid:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n"
                            "    </content>\n",
                            PFcol_str (n->sem.rowid.res));
            break;

        case la_type:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n"
                            "      <column name=\"%s\" new=\"false\"/>\n"
                            "      <type name=\"%s\"/>\n"
                            "    </content>\n",
                            PFcol_str (n->sem.type.res),
                            PFcol_str (n->sem.type.col),
                            PFalg_simple_type_str (n->sem.type.ty));
            break;

        case la_type_assert:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"false\"/>\n"
                            "      <type name=\"%s\"/>\n"
                            "    </content>\n",
                            PFcol_str (n->sem.type.col),
                            PFalg_simple_type_str (n->sem.type.ty));
            break;

        case la_cast:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n"
                            "      <column name=\"%s\" new=\"false\"/>\n"
                            "      <type name=\"%s\"/>\n"
                            "    </content>\n",
                            PFcol_str (n->sem.type.res),
                            PFcol_str (n->sem.type.col),
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
            if (LEVEL_KNOWN(n->sem.step.level))
                PFarray_printf (xml, " level=\"%i\"", n->sem.step.level);

            if (n->kind == la_step || n->kind == la_guide_step)
                PFarray_printf (xml,
                                "/>\n"
                                "      <column name=\"%s\""
                                       " function=\"iter\"/>\n"
                                "      <column name=\"%s\""
                                       " function=\"item\"/>\n"
                                "    </content>\n",
                                PFcol_str (n->sem.step.iter),
                                PFcol_str (n->sem.step.item));
            else
                PFarray_printf (xml,
                                "/>\n"
                                "      <column name=\"%s\" new=\"true\"/>\n"
                                "      <column name=\"%s\""
                                       " function=\"item\"/>\n"
                                "    </content>\n",
                                PFcol_str (n->sem.step.item_res),
                                PFcol_str (n->sem.step.item));
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
                            PFcol_str (n->sem.doc_join.item_res),
                            PFcol_str (n->sem.doc_join.item),
                            PFcol_str (n->sem.doc_join.item_doc));
        }   break;

        case la_doc_tbl:
        {

            char *name = NULL;

            switch (n->sem.doc_tbl.kind) {
                case alg_dt_doc:    name = "fn:doc";        break;
                case alg_dt_col:    name = "fn:collection"; break;
            }

            PFarray_printf (xml,
                            "    <content>\n"
                            "      <kind name=\"%s\"/>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n"
                            "      <column name=\"%s\" new=\"false\"/>\n"
                            "    </content>\n",
                            name,
                            PFcol_str (n->sem.doc_tbl.res),
                            PFcol_str (n->sem.doc_tbl.col));
        }   break;

        case la_doc_access:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\"/>\n"
                            "      <column name=\"%s\" new=\"false\"/>\n"
                            "      <column name=\"",
                            PFcol_str (n->sem.doc_access.res),
                            PFcol_str (n->sem.doc_access.col));

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
                case doc_qname:
                    PFarray_printf (xml, "qname");
                    break;
                case doc_atomize:
                    PFarray_printf (xml, "atomize");
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
                            PFcol_str (n->sem.iter_item.iter),
                            PFcol_str (n->sem.iter_item.item));
            break;

        case la_docnode:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "    </content>\n",
                            PFcol_str (n->sem.docnode.iter));
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
                            PFcol_str (n->sem.iter_item1_item2.iter),
                            PFcol_str (n->sem.iter_item1_item2.item1),
                            PFcol_str (n->sem.iter_item1_item2.item2));
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
                            PFcol_str (n->sem.iter_item1_item2.iter),
                            PFcol_str (n->sem.iter_item1_item2.item1),
                            PFcol_str (n->sem.iter_item1_item2.item2));
            break;

        case la_content:
        case la_trace_items:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"pos\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFcol_str (n->sem.iter_pos_item.iter),
                            PFcol_str (n->sem.iter_pos_item.pos),
                            PFcol_str (n->sem.iter_pos_item.item));
            break;

        case la_merge_adjacent:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"pos\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFcol_str (n->sem.merge_adjacent.iter_res),
                            PFcol_str (n->sem.merge_adjacent.pos_res),
                            PFcol_str (n->sem.merge_adjacent.item_res));
            break;

        case la_error:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"false\"/>\n"
                            "    </content>\n",
                            PFcol_str (n->sem.err.col));
            break;

        case la_cache:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <id>%s</id>\n"
                            "      <column name=\"%s\" function=\"pos\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            n->sem.cache.id,
                            PFcol_str (n->sem.cache.pos),
                            PFcol_str (n->sem.cache.item));
            break;

        case la_trace_map:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" function=\"inner\"/>\n"
                            "      <column name=\"%s\" function=\"outer\"/>\n"
                            "    </content>\n",
                            PFcol_str (n->sem.trace_map.inner),
                            PFcol_str (n->sem.trace_map.outer));
            break;

        case la_fun_call:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <function");
            if (PFqname_uri (n->sem.fun_call.qname))
                PFarray_printf (xml,
                                " uri=\"%s\"",
                                PFqname_uri (n->sem.fun_call.qname));
            PFarray_printf (xml,
                                       " name=\"%s\"/>\n"
                            "      <kind name=\"%s\"/>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <output min=\"%i\" max=\"%i\">\n",
                            PFqname_loc (n->sem.fun_call.qname),
                            PFalg_fun_call_kind_str (n->sem.fun_call.kind),
                            PFcol_str (n->sem.fun_call.iter),
                            n->sem.fun_call.occ_ind == alg_occ_exactly_one ||
                            n->sem.fun_call.occ_ind == alg_occ_one_or_more
                            ? 1 : 0,
                            n->sem.fun_call.occ_ind == alg_occ_zero_or_one ||
                            n->sem.fun_call.occ_ind == alg_occ_exactly_one
                            ? 1 : 2);
            for (c = 0; c < n->schema.count; c++)
                PFarray_printf (xml,
                                "        <column name=\"%s\""
                                               " type=\"%s\""
                                               " position=\"%u\"/>\n",
                                PFcol_str (n->schema.items[c].name),
                                PFalg_simple_type_str (n->schema.items[c].type),
                                c);
            PFarray_printf (xml,
                            "      </output>\n"
                            "    </content>\n");
            break;

        case la_fun_param:
            PFarray_printf (xml, "    <content>\n");
            for (c = 0; c < n->schema.count; c++)
                PFarray_printf (xml,
                                "      <column name=\"%s\""
                                             " type=\"%s\""
                                             " position=\"%u\"/>\n",
                                PFcol_str (n->schema.items[c].name),
                                PFalg_simple_type_str (n->schema.items[c].type),
                                c);
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
                            "      <column name=\"%s\" function=\"iter sep\"/>\n"
                            "      <column name=\"%s\" function=\"item sep\"/>\n"
                            "    </content>\n",
                            PFcol_str (n->sem.string_join.iter),
                            PFcol_str (n->sem.string_join.pos),
                            PFcol_str (n->sem.string_join.item),
                            PFcol_str (n->sem.string_join.iter_sep),
                            PFcol_str (n->sem.string_join.item_sep));
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

static void
mark_body_worker (PFla_op_t *n)
{
    if (n->bit_dag)
        return;
    else
        n->bit_dag = true;

    /* do not mark side effects ... */ 
    if (n->kind == la_side_effects) 
        mark_body_worker (n->child[1]);
    else if (n->kind == la_serialize_rel) {
        n->bit_in = true;
        mark_body_worker (n->child[1]);
    }
    /* ... but mark the rest of the body */
    else {
        n->bit_in = true;

        for (unsigned int c = 0; c < PFLA_OP_MAXCHILD && n->child[c]; c++)
            mark_body_worker (n->child[c]);
    }
}

static void
mark_body (PFla_op_t *n)
{
    mark_body_worker (n);
    PFla_dag_reset (n);
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
 * Dump algebra tree initialization in AT&T dot format
 */
static void
la_dot_init (FILE *f)
{
    fprintf (f, "digraph XQueryAlgebra {\n"
                "ordering=out;\n"
                "node [shape=box];\n"
                "node [height=0.1];\n"
                "node [width=0.2];\n"
                "node [style=filled];\n"
                "node [color=\"#C0C0C0\"];\n"
                "node [fontsize=10];\n"
                "edge [fontsize=9];\n"
                "edge [dir=back];\n");
}

/**
 * Worker for PFla_dot and PFla_dot_bundle
 */
static unsigned int
la_dot_internal (FILE *f, PFla_op_t *root, char *prop_args, int id)
{
    /* initialize array to hold dot output */
    PFarray_t *dot          = PFarray (sizeof (char), 32000),
              *side_effects = PFarray (sizeof (char),  4000);
    unsigned int root_id;

    /* inside debugging we need to reset the dag bits first */
    PFla_dag_reset (root);
    create_node_id (root);
    /* store root node id */
    root_id = root->node_id;
    mark_body (root);
    la_dot (dot,
            side_effects,
            root,
            getenv("PF_DEBUG_PRINT_FRAG") != NULL,
            prop_args,
            id);
    PFla_dag_reset (root);
    reset_node_id (root);

    /* add domain subdomain relationships if required */
    if (prop_args) {
        char *fmt = prop_args;
        while (*fmt) {
            if (*fmt == '+' || *fmt == 'D') {
                    PFprop_write_dom_rel_dot (dot, root->prop, id);
                    break;
            }
            fmt++;
        }
    }

    /* put content of array into file */
    fprintf (f,
             "subgraph clusterSideEffects%i {\n"
             "label=\"SIDE EFFECTS\";\n"
             "fontsize=10;\n"
             "fontcolor=\"#808080\";\n"
             "color=\"#C0C0C0\";\n"
             "fillcolor=\"#F7F7F7\";\n"
             "style=filled;\n"
             "%s"
             "}\n"
             "%s"
             "}\n", 
             id,
             (char *) side_effects->base,
             (char *) dot->base);

    return root_id;
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
    assert (root);
    la_dot_init (f);
    la_dot_internal (f, root, prop_args, 0);
}

/**
 * Dump algebra plan bundle in AT&T dot format
 * (pipe the output through `dot -Tps' to produce a Postscript file).
 *
 * @param f file to dump into
 * @param root root of abstract syntax tree
 */
void
PFla_dot_bundle (FILE *f, PFla_pb_t *lapb, char *prop_args)
{
    PFla_op_t *root;
    int        id, idref, colref, root_id;
    char      *color = "blue";

    la_dot_init (f);

    for (unsigned int i = 0; i < PFla_pb_size(lapb); i++) {
        root   = PFla_pb_op_at (lapb, i);
        id     = PFla_pb_id_at (lapb, i);
        idref  = PFla_pb_idref_at (lapb, i);
        colref = PFla_pb_colref_at (lapb, i);
        
        assert (root && root->kind == la_serialize_rel);

        /* print header box */
        fprintf (f, "planHeader%i [shape=record, style=solid, "
                    "color=%s, fontsize=11, fontcolor=%s, "
                    "label=\"{<q> Q%i} | {<iter%i> %s | %s | {",
                id, color, color, id, id, 
                PFcol_str (root->sem.ser_rel.iter),
                PFcol_str (root->sem.ser_rel.pos));
        for (unsigned int j = 0; j < clsize (root->sem.ser_rel.items); j++)
            fprintf (f, "%s <item%u> %s ",
                     (j ? "|" : ""),
                     j + 1,
                     PFcol_str (clat (root->sem.ser_rel.items, j)));
        fprintf (f, "}}}\"];\n"
                    "subgraph clusterPlan%i {\n", id);

        /* print query plan */
        root_id = la_dot_internal (f, root, prop_args, id);

        /* link header box to plan */
        fprintf (f, "planHeader%i:q -> node%i_%i"
                    " [color=%s];\n",
                 id, id, root_id, color);

        if (idref != -1) {
            /* link header box to the surrogate column */
            assert (colref != -1);
            fprintf (f, "planHeader%i:item%i -> planHeader%i:iter%i"
                    " [color=%s];\n",
                     idref, colref, id, id, color);
        }
    }

    fprintf (f, "}\n");
}


/**
 * Worker for PFla_xml and PFla_xml_bundle
 */
static void
la_xml_internal (FILE *f, PFla_op_t *root, char *prop_args)
{
    /* initialize array to hold dot output */
    PFarray_t *xml = PFarray (sizeof (char), 64000);

    PFarray_printf (xml, "<logical_query_plan unique_names=\"%s\">\n",
                    PFcol_is_name_unq (root->schema.items[0].name)
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

/**
 * Dump algebra tree in XML format
 *
 * @param f file to dump into
 * @param root root of logical algebra tree
 */
void
PFla_xml (FILE *f, PFla_op_t *root, char *prop_args)
{
    assert (root);
    fprintf (f, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    la_xml_internal (f, root, prop_args);
}


void
PFla_xml_qp_property (
	FILE *f, PFla_pb_item_property_t property, unsigned int nest)
{

	char *nestSpaces;
	nestSpaces = (char *) PFmalloc (nest + 1);
	for(unsigned int i = 0; i < nest; i++)
	{
		nestSpaces[i] = ' ';
	}
	nestSpaces[nest] = '\0';

	fprintf (f, "%s<property name=\"%s\"",
			nestSpaces, property.name);
	if (property.value)
	{
	    fprintf (f, " value=\"%s\"",
			property.value);
	}
	if (property.properties)
	{
		fprintf (f, ">\n");
		for (unsigned int i = 0;
			 i < PFarray_last (property.properties);
	         i++)
		{
			PFla_pb_item_property_t subProperty =
				*((PFla_pb_item_property_t*) PFarray_at (
												property.properties, i));
			PFla_xml_qp_property (f, subProperty, nest+2);
		}
		fprintf (f, "%s</property>\n", nestSpaces);
	}
	else
	{
		fprintf (f, "/>\n");
	}

}

/**
 * Dump algebra plan bundle in XML format
 *
 * @param f file to dump into
 * @param root root of logical algebra tree
 */
void
PFla_xml_bundle (FILE *f, PFla_pb_t *lapb, char *prop_args)
{
    PFla_op_t *root;
    int        id, idref, colref;
    PFarray_t *properties;

    fprintf (f, 
             "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
             "<query_plan_bundle>\n");

    for (unsigned int i = 0; i < PFla_pb_size(lapb); i++) {
        root       = PFla_pb_op_at (lapb, i);
        id         = PFla_pb_id_at (lapb, i);
        idref      = PFla_pb_idref_at (lapb, i);
        colref     = PFla_pb_colref_at (lapb, i);
        properties = PFla_pb_properties_at (lapb, i);
        
        fprintf (f, "<query_plan id=\"%i\"", id);
        if (idref != -1)
            fprintf (f, " idref=\"%i\" colref=\"%i\"", idref, colref);
        fprintf (f, ">\n");

        if (properties)
        {
        	fprintf (f, "  <properties>\n");
        	for (unsigned int i = 0;
        					i < PFarray_last (properties);
        		            i++)
			{
				PFla_pb_item_property_t property =
					*((PFla_pb_item_property_t*) PFarray_at (properties, i));
				PFla_xml_qp_property (f, property, 4);
			}


        	fprintf (f, "  </properties>\n");
        }

        assert (root);
        la_xml_internal (f, root, prop_args);

        fprintf (f, "</query_plan>\n");
    }

    fprintf (f, "</query_plan_bundle>\n");
}

/* vim:set shiftwidth=4 expandtab: */
