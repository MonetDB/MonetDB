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

#include "physdebug.h"

#include "alg_dag.h"
#include "mem.h"
/* #include "pfstrings.h" */
#include "oops.h"
#include "pfstrings.h"

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/** Node names to print out for all the Algebra tree nodes. */
static char *a_id[]  = {
      [pa_serialize]       = "SERIALIZE"
    , [pa_lit_tbl]         = "TBL"
    , [pa_empty_tbl]       = "EMPTY_TBL"
    , [pa_attach]          = "@"
      /* note: dot does not like the sequence "×\nfoo", so we put spaces
       * around the cross symbol.
       */
    , [pa_cross]           = "CROSS"            /* \"#00FFFF\" */
    , [pa_dep_cross]       = "DEP CROSS"        /* \"#00FFFF\" */
    , [pa_dep_border]      = "dep border"
    , [pa_leftjoin]        = "LEFTJOIN"         /* \"#00FF00\" */
    , [pa_eqjoin]          = "EQJOIN"           /* \"#00FF00\" */
    , [pa_semijoin]        = "SEMIJOIN"         /* \"#00FF00\" */
    , [pa_thetajoin]       = "thetajoin"
    , [pa_unq2_thetajoin]  = "unique_thetajoin"
    , [pa_unq1_thetajoin]  = "dep_unique_thetajoin"
    , [pa_project]         = "PROJECT"
    , [pa_slice]           = "SLICE"
    , [pa_select]          = "SEL"
    , [pa_val_select]      = "VAL SEL"
    , [pa_append_union]    = "APPEND_UNION"
    , [pa_merge_union]     = "MERGE_UNION"
    , [pa_intersect]       = "INTERSECT"        /* \"#FFA500\" */
    , [pa_difference]      = "DIFF"             /* \"#FFA500\" */
    , [pa_sort_distinct]   = "SORT_DISTINCT"
    , [pa_std_sort]        = "SORT"
    , [pa_refine_sort]     = "refine_sort"
    , [pa_fun_1to1]        = "1:1 fun"
    , [pa_eq]              = "="
    , [pa_gt]              = ">"
    , [pa_bool_and]        = "AND"
    , [pa_bool_or]         = "OR"
    , [pa_bool_not]        = "NOT"
    , [pa_to]              = "op:to"
    , [pa_aggr]            = "AGGR"
    , [pa_count_ext]       = "{COUNT}"
    , [pa_mark]            = "mark"
    , [pa_rank]            = "rank"
    , [pa_mark_grp]        = "mark_grp"
    , [pa_type]            = "TYPE"
    , [pa_type_assert]     = "type assertion"
    , [pa_cast]            = "CAST"
    , [pa_llscjoin]        = "//|"
    , [pa_llscjoin_dup]    = "//|+"
    , [pa_doc_tbl]         = "DOC"
    , [pa_doc_access]      = "access"
    , [pa_twig]            = "TWIG"             /* lawn \"#00FF00\" */
    , [pa_fcns]            = "FCNS"             /* lawn \"#00FF00\" */
    , [pa_docnode]         = "DOC"              /* lawn \"#00FF00\" */
    , [pa_element]         = "ELEM"             /* lawn \"#00FF00\" */
    , [pa_attribute]       = "ATTR"             /* lawn \"#00FF00\" */
    , [pa_textnode]        = "TEXT"             /* lawn \"#00FF00\" */
    , [pa_comment]         = "COMMENT"          /* lawn \"#00FF00\" */
    , [pa_processi]        = "PI"               /* lawn \"#00FF00\" */
    , [pa_content]         = "CONTENT"          /* lawn \"#00FF00\" */
    , [pa_slim_content]    = "slim content"     /* lawn \"#00FF00\" */
    , [pa_merge_adjacent]  = "#pf:merge-adjacent-text-nodes"
    , [pa_error]           = "!ERROR"
    , [pa_nil]             = "nil"
    , [pa_cache]           = "cache"
    , [pa_cache_border]    = "cache border"
    , [pa_trace]           = "trace"
    , [pa_trace_items]     = "trace_items"
    , [pa_trace_msg]       = "trace_msg"
    , [pa_trace_map]       = "trace_map"
    , [pa_rec_fix]         = "rec fix"
    , [pa_side_effects]    = "rec side effects"
    , [pa_rec_param]       = "rec param"
    , [pa_rec_arg]         = "rec arg"
    , [pa_rec_base]        = "rec base"
    , [pa_rec_border]      = "rec border"
    , [pa_fun_call]        = "fun call"
    , [pa_fun_param]       = "fun param"
    , [pa_string_join]     = "fn:string-join"
    , [pa_findnodes]       = "fn:id/idref"
    , [pa_vx_lookup]       = "pf:text/attribute"
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
            PFarray_printf (s, "%s", PFqname_str (a.val.qname));
            break;

        default:
            PFarray_printf (s, "<node/>");
            break;
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
 * @param node_id the next available node id.
 */
static unsigned int
pa_dot (PFarray_t *dot, PFpa_op_t *n, unsigned int node_id, char *prop_args)
{
    unsigned int c;
    assert(n->node_id);

    static char *color[] = {
          [pa_serialize]       = "\"#C0C0C0\""
        , [pa_lit_tbl]         = "\"#C0C0C0\""
        , [pa_empty_tbl]       = "\"#C0C0C0\""
        , [pa_attach]          = "\"#EEEEEE\""
        , [pa_cross]           = "\"#990000\""
        , [pa_dep_cross]       = "\"#990000\""
        , [pa_dep_border]      = "\"#990000\""
        , [pa_leftjoin]        = "\"#00FF00\""
        , [pa_eqjoin]          = "\"#00FF00\""
        , [pa_semijoin]        = "\"#00FF00\""
        , [pa_thetajoin]       = "\"#00FF00\""
        , [pa_unq2_thetajoin]  = "\"#00FF00\""
        , [pa_unq1_thetajoin]  = "\"#00FF00\""
        , [pa_project]         = "\"#EEEEEE\""
        , [pa_slice]           = "\"#00BBFF\""
        , [pa_select]          = "\"#00DDDD\""
        , [pa_val_select]      = "\"#00BBBB\""
        , [pa_append_union]    = "\"#909090\""
        , [pa_merge_union]     = "\"#909090\""
        , [pa_intersect]       = "\"#FFA500\""
        , [pa_difference]      = "\"#FFA500\""
        , [pa_sort_distinct]   = "\"#FFA500\""
        , [pa_std_sort]        = "red"
        , [pa_refine_sort]     = "red"
        , [pa_fun_1to1]        = "\"#C0C0C0\""
        , [pa_eq]              = "\"#00DDDD\""
        , [pa_gt]              = "\"#00DDDD\""
        , [pa_bool_not]        = "\"#C0C0C0\""
        , [pa_bool_and]        = "\"#C0C0C0\""
        , [pa_bool_or]         = "\"#C0C0C0\""
        , [pa_to]              = "\"#C0C0C0\""
        , [pa_aggr]            = "\"#A0A0A0\""
        , [pa_count_ext]       = "\"#A0A0A0\""
        , [pa_mark]            = "\"#FFBBBB\""
        , [pa_rank]            = "\"#FFBBBB\""
        , [pa_mark_grp]        = "\"#FFBBBB\""
        , [pa_type]            = "\"#C0C0C0\""
        , [pa_type_assert]     = "\"#C0C0C0\""
        , [pa_cast]            = "\"#C0C0C0\""
        , [pa_llscjoin]        = "\"#1E90FF\""
        , [pa_llscjoin_dup]    = "\"#1E9099\""
        , [pa_doc_tbl]         = "\"#C0C0C0\""
        , [pa_doc_access]      = "\"#CCCCFF\""
        , [pa_twig]            = "\"#00FC59\""
        , [pa_fcns]            = "\"#00FC59\""
        , [pa_docnode]         = "\"#00FC59\""
        , [pa_element]         = "\"#00FC59\""
        , [pa_attribute]       = "\"#00FC59\""
        , [pa_textnode]        = "\"#00FC59\""
        , [pa_comment]         = "\"#00FC59\""
        , [pa_processi]        = "\"#00FC59\""
        , [pa_content]         = "\"#00FC59\""
        , [pa_slim_content]    = "\"#AAFC59\""
        , [pa_merge_adjacent]  = "\"#00D000\""
        , [pa_error]           = "\"#C0C0C0\""
        , [pa_nil]             = "\"#FFFFFF\""
        , [pa_cache]           = "\"#FF5500\""
        , [pa_cache_border]    = "\"#FF5500\""
        , [pa_trace]           = "\"#FF5500\""
        , [pa_trace_items]     = "\"#FF5500\""
        , [pa_trace_msg]       = "\"#FF5500\""
        , [pa_trace_map]       = "\"#FF5500\""
        , [pa_rec_fix]         = "\"#FF00FF\""
        , [pa_side_effects]    = "\"#FF00FF\""
        , [pa_rec_param]       = "\"#FF00FF\""
        , [pa_rec_arg]         = "\"#BB00BB\""
        , [pa_rec_base]        = "\"#BB00BB\""
        , [pa_rec_border]      = "\"#BB00BB\""
        , [pa_fun_call]        = "\"#BB00BB\""
        , [pa_fun_param]       = "\"#BB00BB\""
        , [pa_string_join]     = "\"#C0C0C0\""
        , [pa_findnodes]       = "\"#CCCCFF\""
        , [pa_vx_lookup]       = "\"#CCCCFF\""
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
            /* list the columns of this table */
            PFarray_printf (dot, "%s: (%s", a_id[n->kind],
                            PFcol_str (n->schema.items[0].name));

            for (c = 1; c < n->schema.count;c++)
                PFarray_printf (dot, " | %s",
                                PFcol_str (n->schema.items[c].name));

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
            /* list the columns of this table */
            PFarray_printf (dot, "%s: (%s", a_id[n->kind],
                            PFcol_str (n->schema.items[0].name));

            for (c = 1; c < n->schema.count;c++)
                PFarray_printf (dot, " | %s",
                                PFcol_str (n->schema.items[c].name));

            PFarray_printf (dot, ")");
            break;

        case pa_attach:
        case pa_val_select:
            PFarray_printf (dot, "%s (%s), val: %s", a_id[n->kind],
                            PFcol_str (n->sem.attach.col),
                            literal (n->sem.attach.value));
            break;

        case pa_leftjoin:
        case pa_eqjoin:
        case pa_semijoin:
            PFarray_printf (dot, "%s: (%s= %s)", a_id[n->kind],
                            PFcol_str (n->sem.eqjoin.col1),
                            PFcol_str (n->sem.eqjoin.col2));
            break;

        case pa_thetajoin:
            PFarray_printf (dot, "%s", a_id[n->kind]);

            for (c = 0; c < n->sem.thetajoin.count; c++)
                PFarray_printf (dot, "\\n(%s %s %s)",
                                PFcol_str (n->sem.thetajoin.pred[c].left),
                                comp_str (n->sem.thetajoin.pred[c].comp),
                                PFcol_str (n->sem.thetajoin.pred[c].right));
            break;

        case pa_unq2_thetajoin:
            PFarray_printf (dot, "%s:\\n(%s %s %s)\\ndist (%s, %s)",
                            a_id[n->kind],
                            PFcol_str (n->sem.unq_thetajoin.left),
                            comp_str (n->sem.unq_thetajoin.comp),
                            PFcol_str (n->sem.unq_thetajoin.right),
                            PFcol_str (n->sem.unq_thetajoin.ldist),
                            PFcol_str (n->sem.unq_thetajoin.rdist));
            break;

        case pa_unq1_thetajoin:
            PFarray_printf (dot, "%s:\\n(%s = %s)\\n"
                            "(%s %s %s)\\ndist (%s)",
                            a_id[n->kind],
                            PFcol_str (n->sem.unq_thetajoin.ldist),
                            PFcol_str (n->sem.unq_thetajoin.rdist),
                            PFcol_str (n->sem.unq_thetajoin.left),
                            comp_str (n->sem.unq_thetajoin.comp),
                            PFcol_str (n->sem.unq_thetajoin.right),
                            PFcol_str (n->sem.unq_thetajoin.ldist));
            break;

        case pa_project:
            if (n->sem.proj.items[0].new != n->sem.proj.items[0].old)
                PFarray_printf (dot, "%s (%s:%s", a_id[n->kind],
                                PFcol_str (n->sem.proj.items[0].new),
                                PFcol_str (n->sem.proj.items[0].old));
            else
                PFarray_printf (dot, "%s (%s", a_id[n->kind],
                                PFcol_str (n->sem.proj.items[0].old));

            for (c = 1; c < n->sem.proj.count; c++)
                if (n->sem.proj.items[c].new != n->sem.proj.items[c].old)
                    PFarray_printf (dot, ",%s:%s",
                                    PFcol_str (n->sem.proj.items[c].new),
                                    PFcol_str (n->sem.proj.items[c].old));
                else
                    PFarray_printf (dot, ",%s",
                                    PFcol_str (n->sem.proj.items[c].old));

            PFarray_printf (dot, ")");
            break;

        case pa_slice:
            PFarray_printf (dot, "%s (%i, %i)", a_id[n->kind],
                            n->sem.slice.low,
                            n->sem.slice.high);
            break;

        case pa_select:
            PFarray_printf (dot, "%s (%s)", a_id[n->kind],
                            PFcol_str (n->sem.select.col));
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
            PFarray_printf (dot, "%s: (%s)", a_id[n->kind],
                            PFord_str (n->sem.sortby.required));
            break;

        case pa_refine_sort:
            PFarray_printf (dot, "%s: (%s)\\nprovided (%s)",
                            a_id[n->kind],
                            PFord_str (n->sem.sortby.required),
                            PFord_str (n->sem.sortby.existing));
            break;

        case pa_fun_1to1:
            PFarray_printf (dot, "%s [%s] (%s:<", a_id[n->kind],
                            PFalg_fun_str (n->sem.fun_1to1.kind),
                            PFcol_str (n->sem.fun_1to1.res));
            for (c = 0; c < clsize (n->sem.fun_1to1.refs);c++)
                PFarray_printf (dot, "%s%s",
                                c ? ", " : "",
                                PFcol_str (clat (n->sem.fun_1to1.refs, c)));
            PFarray_printf (dot, ">)");
            break;

        case pa_eq:
        case pa_gt:
        case pa_bool_and:
        case pa_bool_or:
        case pa_to:
            PFarray_printf (dot, "%s (%s:<%s, %s>)", a_id[n->kind],
                            PFcol_str (n->sem.binary.res),
                            PFcol_str (n->sem.binary.col1),
                            PFcol_str (n->sem.binary.col2));
            break;

        case pa_bool_not:
            PFarray_printf (dot, "%s (%s:<%s>)", a_id[n->kind],
                            PFcol_str (n->sem.unary.res),
                            PFcol_str (n->sem.unary.col));
            break;

        case pa_count_ext:
            if (n->sem.count.part == col_NULL)
                PFarray_printf (dot, "%s (%s)", a_id[n->kind],
                                PFcol_str (n->sem.count.res));
            else
                PFarray_printf (dot, "%s (%s:/%s)", a_id[n->kind],
                                PFcol_str (n->sem.count.res),
                                PFcol_str (n->sem.count.part));
            if (n->sem.count.loop != col_NULL)
                PFarray_printf (dot, " (%s)",
                                PFcol_str (n->sem.count.loop));
            break;

        case pa_aggr:
            /* overwrite standard node layout */
            PFarray_printf (dot, "\", shape=polygon peripheries=2, label=\"");

            PFarray_printf (dot, "%s", a_id[n->kind]);
            if (n->sem.aggr.part != col_NULL)
                PFarray_printf (dot, " / %s",
                                PFcol_str (n->sem.aggr.part));

            for (c = 0; c < n->sem.aggr.count; c++)
                PFarray_printf (dot, "\\n%s = %s (%s)",
                                PFcol_str (n->sem.aggr.aggr[c].res),
                                PFalg_aggr_kind_str (n->sem.aggr.aggr[c].kind),
                                n->sem.aggr.aggr[c].col
                                ? PFcol_str (n->sem.aggr.aggr[c].col) : "");
            break;

        case pa_mark:
        case pa_mark_grp:
            PFarray_printf (dot, "%s (%s", a_id[n->kind],
                            PFcol_str (n->sem.mark.res));
            if (n->sem.mark.part != col_NULL)
                PFarray_printf (dot, "/%s",
                                PFcol_str (n->sem.mark.part));

            PFarray_printf (dot, ")");
            break;

        case pa_rank:
            PFarray_printf (dot, "%s (%s)", a_id[n->kind],
                            PFcol_str (n->sem.rank.res));
            break;

        case pa_type:
            PFarray_printf (dot, "%s (%s:<%s>), type: %s", a_id[n->kind],
                            PFcol_str (n->sem.type.res),
                            PFcol_str (n->sem.type.col),
                            PFalg_simple_type_str (n->sem.type.ty));
            break;

        case pa_type_assert:
            PFarray_printf (dot, "%s (%s), type: %s", a_id[n->kind],
                            PFcol_str (n->sem.type_a.col),
                            PFalg_simple_type_str (n->sem.type_a.ty));
            break;

        case pa_cast:
            PFarray_printf (dot, "%s (%s%s%s%s), type: %s", a_id[n->kind],
                            n->sem.cast.res?PFcol_str(n->sem.cast.res):"",
                            n->sem.cast.res?":<":"",
                            PFcol_str (n->sem.cast.col),
                            n->sem.cast.res?">":"",
                            PFalg_simple_type_str (n->sem.cast.ty));
            break;

        case pa_llscjoin:
        case pa_llscjoin_dup:
            PFarray_printf (dot, "%s %s::%s",
                            a_id[n->kind],
                            PFalg_axis_str (n->sem.scjoin.spec.axis),
                            PFalg_node_kind_str (n->sem.scjoin.spec.kind));

            if (n->sem.scjoin.spec.kind == node_kind_elem ||
                n->sem.scjoin.spec.kind == node_kind_attr)
                PFarray_printf (dot, "(%s)",
                                PFqname_str (n->sem.scjoin.spec.qname));
            else if (n->sem.scjoin.spec.kind == node_kind_pi &&
                     PFqname_loc (n->sem.scjoin.spec.qname))
                PFarray_printf (dot, "(%s)",
                                PFqname_loc (n->sem.scjoin.spec.qname));
            else
                PFarray_printf (dot, "()");

                PFarray_printf (dot, " (%s, %s)",
                                PFcol_str (n->sem.scjoin.iter),
                                PFcol_str (n->sem.scjoin.item));
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
                case doc_qname:
                    PFarray_printf (dot, "qname");
                    break;
                case doc_atomize:
                    PFarray_printf (dot, "atomize");
                    break;
                default: PFoops (OOPS_FATAL,
                        "unknown document access in dot output");
            }

            PFarray_printf (dot, " (%s:<%s>)",
                            PFcol_str (n->sem.doc_access.res),
                            PFcol_str (n->sem.doc_access.col));
            break;

        case pa_twig:
        case pa_element:
        case pa_textnode:
        case pa_comment:
        case pa_content:
        case pa_slim_content:
        case pa_trace_items:
        case pa_trace_msg:
            PFarray_printf (dot, "%s (%s, %s)",
                            a_id[n->kind],
                            PFcol_str (n->sem.ii.iter),
                            PFcol_str (n->sem.ii.item));
            break;

        case pa_docnode:
            PFarray_printf (dot, "%s (%s)",
                            a_id[n->kind],
                            PFcol_str (n->sem.ii.iter));
            break;

        case pa_attribute:
        case pa_processi:
            PFarray_printf (dot, "%s (%s:<%s, %s>)", a_id[n->kind],
                            PFcol_str (n->sem.iter_item1_item2.iter),
                            PFcol_str (n->sem.iter_item1_item2.item1),
                            PFcol_str (n->sem.iter_item1_item2.item2));
            break;

        case pa_error:
            PFarray_printf (dot, "%s: (%s)", a_id[n->kind],
                            PFcol_str (n->sem.err.col));
            break;

        case pa_cache:
            PFarray_printf (dot, "%s %s (%s)", a_id[n->kind],
                            n->sem.cache.id,
                            PFcol_str (n->sem.cache.item));
            break;

        case pa_trace_map:
            PFarray_printf (dot,
                            "%s (%s, %s)",
                            a_id[n->kind],
                            PFcol_str (n->sem.trace_map.inner),
                            PFcol_str (n->sem.trace_map.outer));
            break;

        case pa_fun_call:
            if (PFqname_loc (n->sem.fun_call.qname))
                PFarray_printf (dot,
                                "%s function \\\"%s\\\" (",
                                PFalg_fun_call_kind_str (n->sem.fun_call.kind),
                                PFqname_uri_str (n->sem.fun_call.qname));
            else
                PFarray_printf (dot,
                                "%s function (",
                                PFalg_fun_call_kind_str (n->sem.fun_call.kind));
            for (unsigned int i = 0; i < n->schema.count; i++)
                PFarray_printf (dot, "%s%s",
                                i?", ":"",
                                PFcol_str (n->schema.items[i].name));
            PFarray_printf (dot,
                            ")\\n(loop: %s)",
                            PFcol_str (n->sem.fun_call.iter));
            break;

        case pa_fun_param:
            PFarray_printf (dot, "%s (", a_id[n->kind]);
            for (unsigned int i = 0; i < n->schema.count; i++)
                PFarray_printf (dot, "%s%s",
                                i?", ":"",
                                PFcol_str (n->schema.items[i].name));
            PFarray_printf (dot, ")");
            break;

        case pa_serialize:
        case pa_cross:
        case pa_dep_cross:
        case pa_dep_border:
        case pa_append_union:
        case pa_intersect:
        case pa_difference:
        case pa_doc_tbl:
        case pa_fcns:
        case pa_merge_adjacent:
        case pa_nil:
        case pa_rec_fix:
        case pa_side_effects:
        case pa_rec_param:
        case pa_rec_arg:
        case pa_rec_base:
        case pa_rec_border:
        case pa_cache_border:
        case pa_trace:
        case pa_string_join:
        case pa_findnodes:
        case pa_vx_lookup:
            PFarray_printf (dot, "%s", a_id[n->kind]);
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
            if (*fmt == '+' || *fmt == 'c') {
                PFarray_printf (dot, "\\ncost: %lu", n->cost);
            }
            if (*fmt == '+' || *fmt == 'o') {
                PFarray_printf (dot, "\\norderings:");
                for (unsigned int i = 0;
                        i < PFarray_last (n->orderings); i++)
                    PFarray_printf (
                            dot, "\\n%s",
                            PFord_str (
                                *(PFord_ordering_t *)
                                        PFarray_at (n->orderings,i)));
            }

            /* stop after all properties have been printed */
            if (*fmt == '+')
                break;
            else
                fmt++;
        }
    }
    if (prop_args && n->prop) {

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
                /* list columns marked const */
                for (unsigned int i = 0;
                        i < PFprop_const_count (n->prop); i++)
                    PFarray_printf (dot, i ? ", %s" : "\\nconst: %s",
                                    PFcol_str (
                                        PFprop_const_at (n->prop, i)));
            }
            if (*fmt == '+' || *fmt == 'I') {
                PFalg_collist_t *icols = PFprop_icols_to_collist (n->prop);

                /* list icols columns */
                for (unsigned int i = 0; i < clsize (icols); i++)
                    PFarray_printf (dot, i ? ", %s" : "\\nicols: %s",
                                    PFcol_str (clat (icols, i)));
            }
            if (*fmt == '+' || *fmt == 'K') {
                PFalg_collist_t *keys = PFprop_keys_to_collist (n->prop);

                /* list keys columns */
                for (unsigned int i = 0; i < clsize (keys); i++)
                    PFarray_printf (dot, i ? ", %s" : "\\nkeys: %s",
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
                            dot,
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
                            dot,
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
                            dot,
                            fst ? "\\nbijective col: %s" : ", %s",
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
                            dot,
                            fst ? "\\nmulti-col col: %s" : ", %s",
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
                            dot,
                            fst ? "\\nvalue col: %s" : ", %s",
                            PFcol_str (col));
                        fst = false;
                    }
                }
            }
            if (*fmt == '+' || *fmt == 'D') {
                /* list columns and their corresponding domains */
                for (unsigned int i = 0; i < n->schema.count; i++)
                    if (PFprop_dom (n->prop, n->schema.items[i].name)) {
                        PFarray_printf (dot, i ? ", %s " : "\\ndom: %s ",
                                        PFcol_str (n->schema.items[i].name));
                        PFprop_write_domain (
                            dot,
                            PFprop_dom (n->prop, n->schema.items[i].name));
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
                            dot,
                            i ? " , %s=%s" : "\\nO->U names: %s=%s",
                            PFcol_str (ori), PFcol_str (unq));

                        l_unq = PFprop_unq_name_left (n->prop, ori);
                        r_unq = PFprop_unq_name_right (n->prop, ori);

                        if (l_unq && l_unq != unq && r_unq && r_unq != unq)
                            PFarray_printf (dot,
                                            " [%s|%s]",
                                            PFcol_str(l_unq),
                                            PFcol_str(r_unq));
                        else if (l_unq && l_unq != unq)
                            PFarray_printf (dot, " [%s|", PFcol_str(l_unq));
                        else if (r_unq && r_unq != unq)
                            PFarray_printf (dot, " |%s]", PFcol_str(r_unq));
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
                            dot,
                            i ? " , %s=%s" : "\\nU->O names: %s=%s",
                            PFcol_str (unq), PFcol_str (ori));

                        l_ori = PFprop_ori_name_left (n->prop, unq);
                        r_ori = PFprop_ori_name_right (n->prop, unq);

                        if (l_ori && l_ori != ori && r_ori && r_ori != ori)
                            PFarray_printf (dot,
                                            " [%s|%s]",
                                            PFcol_str(l_ori),
                                            PFcol_str(r_ori));
                        else if (l_ori && l_ori != ori)
                            PFarray_printf (dot, " [%s|", PFcol_str(l_ori));
                        else if (r_ori && r_ori != ori)
                            PFarray_printf (dot, " |%s]", PFcol_str(r_ori));
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
                    PFalg_col_t col = n->schema.items[i].name;
                    int level = PFprop_level (n->prop, col);
                    if (LEVEL_KNOWN(level)) {
                        PFarray_printf (
                            dot,
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

                        PFarray_printf (dot, "%s %s:",
                                        first ? "\\nGUIDE:" : ",",
                                        PFcol_str(col));
                        first = false;

                        /* print guides */
                        count  = PFprop_guide_count (n->prop, col);
                        guides = PFprop_guide_elements (n->prop, col);
                        for (j = 0; j < count; j++)
                            PFarray_printf (dot, " %i", guides[j]->guide);
                    }
                }
            }
            if (*fmt == '+' || *fmt == 'Y') {
                if (PFprop_ckeys_count (n->prop)) {
                    PFalg_collist_t *list;
                    unsigned int i, j;
                    bool first = true;

                    PFarray_printf (dot, "\\ncomposite keys:");

                    for (i = 0; i < PFprop_ckeys_count (n->prop); i++) {
                        list = PFprop_ckey_at (n->prop, i);
                        first = true;
                        for (j = 0; j < clsize (list); j++) {
                            PFarray_printf (dot, "%s%s",
                                            first ? "\\n<" : ", ",
                                            PFcol_str(clat (list, j)));
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
            node_id = pa_dot (dot, n->child[c], node_id, prop_args);
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
PFpa_dot (FILE *f, PFpa_op_t *root, char *prop_args)
{
    if (root) {
        /* initialize array to hold dot output */
        PFarray_t *dot = PFarray (sizeof (char), 32000);

        PFarray_printf (dot, "digraph XQueryPhysicalAlgebra {\n"
                             "ordering=out;\n"
                             "node [shape=box];\n"
                             "node [height=0.1];\n"
                             "node [width=0.2];\n"
                             "node [style=filled];\n"
                             "node [color=\"#C0C0C0\"];\n"
                             "node [fontsize=10];\n");

        root->node_id = 1;
        pa_dot (dot, root, root->node_id + 1, prop_args);
        PFpa_dag_reset (root);
        reset_node_id (root);
        PFpa_dag_reset (root);

        /* add domain subdomain relationships if required */
        if (prop_args) {
            char *fmt = prop_args;
            while (*fmt) {
                if (*fmt == '+') {
                        PFprop_write_dom_rel_dot (dot, root->prop, 0);
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

/* vim:set shiftwidth=4 expandtab: */
