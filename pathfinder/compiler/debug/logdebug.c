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

#include "alg_dag.h"
#include "mem.h"
#include "prettyp.h"
#include "oops.h"
#include "pfstrings.h"

/** Node names to print out for all the Algebra tree nodes. */
static char *a_id[]  = {
      [la_serialize]        = "SERIALIZE"
    , [la_lit_tbl]          = "TBL"
    , [la_empty_tbl]        = "EMPTY_TBL"
    , [la_attach]           = "Attach"
      /* note: dot does not like the sequence "×\nfoo", so we put spaces
       * around the cross symbol.
       */
    , [la_cross]            = "Cross"             /* \"#00FFFF\" */
    , [la_eqjoin]           = "Join"              /* \"#00FF00\" */
    , [la_eqjoin_unq]       = "Join"              /* \"#00FF00\" */
    , [la_semijoin]         = "SemiJoin"          /* \"#00FF00\" */
    , [la_project]          = "Project"
    , [la_select]           = "Select"
    , [la_disjunion]        = "UNION"
    , [la_intersect]        = "INTERSECT"
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
    , [la_avg]              = "AVG"
    , [la_max]              = "MAX"
    , [la_min]              = "MIN"
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
    , [la_dup_scjoin]       = "/|+"               /* light blue */
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
    , [la_rec_fix]          = "rec fix"
    , [la_rec_param]        = "rec param"
    , [la_rec_nil]          = "rec nil"
    , [la_rec_arg]          = "rec arg"
    , [la_rec_base]         = "rec base"
    , [la_proxy]            = "PROXY"
    , [la_proxy_base]       = "PROXY_BASE"
    , [la_concat]           = "fn:concat"
    , [la_contains]         = "fn:contains"
    , [la_string_join]      = "fn:string_join"
    , [la_dummy]            = "DUMMY"
};

/* XML node names to print out for all kinds */
static char *xml_id[]  = {
      [la_serialize]        = "serialize"
    , [la_lit_tbl]          = "table"
    , [la_empty_tbl]        = "empty_tbl"
    , [la_attach]           = "attach"
    , [la_cross]            = "cross"
    , [la_eqjoin]           = "eqjoin"
    , [la_semijoin]         = "semijoin"
    , [la_project]          = "project"
    , [la_select]           = "select"
    , [la_disjunion]        = "union"
    , [la_intersect]        = "intersect"
    , [la_difference]       = "difference"
    , [la_distinct]         = "distinct"
    , [la_num_add]          = "num-add"
    , [la_num_subtract]     = "num_subtr"
    , [la_num_multiply]     = "num-mult"
    , [la_num_divide]       = "num-div"
    , [la_num_modulo]       = "num-mod"
    , [la_num_eq]           = "eq"
    , [la_num_gt]           = "gt"
    , [la_num_neg]          = "negate"
    , [la_bool_and ]        = "and"
    , [la_bool_or ]         = "or"
    , [la_bool_not]         = "not"
    , [la_avg]              = "avg"
    , [la_max]              = "max"
    , [la_min]              = "min"
    , [la_sum]              = "sum"
    , [la_count]            = "count"
    , [la_rownum]           = "rownum"
    , [la_number]           = "number"
    , [la_type]             = "type"
    , [la_type_assert]      = "type_assertion"
    , [la_cast]             = "cast"
    , [la_seqty1]           = "seqty1"
    , [la_all]              = "all"
    , [la_scjoin]           = "staircase_join"
    , [la_dup_scjoin]       = "staircase_join_with_duplicates"
    , [la_doc_tbl]          = "fn:doc"
    , [la_doc_access]       = "document_access"
    , [la_element]          = "element_construction"
    , [la_element_tag]      = "element_tagname"
    , [la_attribute]        = "attribute_construction"
    , [la_textnode]         = "textnode_construction"
    , [la_docnode]          = "documentnode_construction"
    , [la_comment]          = "comment_construction"
    , [la_processi]         = "pi_construction"
    , [la_merge_adjacent]   = "#pf:merge-adjacent-text-nodes"
    , [la_roots]            = "ROOTS"
    , [la_fragment]         = "FRAG"
    , [la_frag_union]       = "FRAG_UNION"
    , [la_empty_frag]       = "EMPTY_FRAG"
    , [la_cond_err]         = "!ERROR"
    , [la_rec_fix]          = "rec_fix"
    , [la_rec_param]        = "rec_param"
    , [la_rec_nil]          = "rec_nil"
    , [la_rec_arg]          = "rec_arg"
    , [la_rec_base]         = "rec_base"
    , [la_proxy]            = "PROXY"
    , [la_proxy_base]       = "PROXY_BASE"
    , [la_concat]           = "fn:concat"
    , [la_contains]         = "fn:contains"
    , [la_string_join]      = "fn:string_join"
    , [la_dummy]            = "DUMMY"
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

    if (a.special == amm_min)
        return "MIN";
    else if (a.special == amm_max)
        return "MAX";

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

    if (a.special == amm_min)
        PFarray_printf (
           s, "<value type=\"%s\">MIN</value>",
           atomtype[a.type]);
    else if (a.special == amm_max)
        PFarray_printf (
           s, "<value type=\"%s\">MAX</value>",
           atomtype[a.type]);
    else if (a.type == aat_nat)
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
        PFarray_printf (
           s, "<value type=\"%s\">%s</value>",
           atomtype[a.type],
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
la_dot (PFarray_t *dot, PFla_op_t *n, unsigned int node_id)
{
    unsigned int c;
    assert(n->node_id);

    static char *color[] = {
          [la_serialize]      = "\"#C0C0C0\""
        , [la_lit_tbl]        = "\"#C0C0C0\""
        , [la_empty_tbl]      = "\"#C0C0C0\""
        , [la_attach]         = "\"#EEEEEE\""
        , [la_cross]          = "\"#990000\""
        , [la_eqjoin]         = "\"#00FF00\""
        , [la_eqjoin_unq]     = "\"#00CC00\""
        , [la_semijoin]       = "\"#009900\""
        , [la_project]        = "\"#EEEEEE\""
        , [la_select]         = "\"#00DDDD\""
        , [la_disjunion]      = "\"#909090\""
        , [la_intersect]      = "\"#FFA500\""
        , [la_difference]     = "\"#FFA500\""
        , [la_distinct]       = "\"#FFA500\""
        , [la_num_add]        = "\"#C0C0C0\""
        , [la_num_subtract]   = "\"#C0C0C0\""
        , [la_num_multiply]   = "\"#C0C0C0\""
        , [la_num_divide]     = "\"#C0C0C0\""
        , [la_num_modulo]     = "\"#C0C0C0\""
        , [la_num_eq]         = "\"#00DDDD\""
        , [la_num_gt]         = "\"#00DDDD\""
        , [la_num_neg]        = "\"#C0C0C0\""
        , [la_bool_and ]      = "\"#C0C0C0\""
        , [la_bool_or ]       = "\"#C0C0C0\""
        , [la_bool_not]       = "\"#C0C0C0\""
        , [la_avg]            = "\"#A0A0A0\""
        , [la_max]            = "\"#A0A0A0\""
        , [la_min]            = "\"#A0A0A0\""
        , [la_sum]            = "\"#A0A0A0\""
        , [la_count]          = "\"#A0A0A0\""
        , [la_rownum]         = "\"#FF0000\""
        , [la_number]         = "\"#FF9999\""
        , [la_type]           = "\"#C0C0C0\""
        , [la_type_assert]    = "\"#C0C0C0\""
        , [la_cast]           = "\"#C0C0C0\""
        , [la_seqty1]         = "\"#C0C0C0\""
        , [la_all]            = "\"#C0C0C0\""
        , [la_scjoin]         = "\"#1E90FF\""
        , [la_dup_scjoin]     = "\"#1E9099\""
        , [la_doc_tbl]        = "\"#C0C0C0\""
        , [la_doc_access]     = "\"#CCCCFF\""
        , [la_element]        = "\"#00CC59\""
        , [la_element_tag]    = "\"#00CC59\""
        , [la_attribute]      = "\"#00CC59\""
        , [la_textnode]       = "\"#00CC59\""
        , [la_docnode]        = "\"#00CC59\""
        , [la_comment]        = "\"#00CC59\""
        , [la_processi]       = "\"#00CC59\""
        , [la_merge_adjacent] = "\"#00D000\""
        , [la_roots]          = "\"#E0E0E0\""
        , [la_fragment]       = "\"#E0E0E0\""
        , [la_frag_union]     = "\"#E0E0E0\""
        , [la_empty_frag]     = "\"#E0E0E0\""
        , [la_cond_err]       = "\"#C0C0C0\""
        , [la_rec_fix]        = "\"#FF00FF\""
        , [la_rec_param]      = "\"#FF00FF\""
        , [la_rec_nil]        = "\"#FF00FF\""
        , [la_rec_arg]        = "\"#BB00BB\""
        , [la_rec_base]       = "\"#BB00BB\""
        , [la_proxy]          = "\"#DFFFFF\""
        , [la_proxy_base]     = "\"#DFFFFF\""
        , [la_concat]         = "\"#C0C0C0\""
        , [la_contains]       = "\"#C0C0C0\""
        , [la_string_join]    = "\"#C0C0C0\""
        , [la_dummy]          = "\"#FFFFFF\""
    };

    /* open up label */
    PFarray_printf (dot, "node%i [label=\"", n->node_id);

    /* the following line enables id printing to simplify comparison with
       generated XML plans */
    /* PFarray_printf (dot, "id: %i\\n", n->node_id); */

    /* create label */
    switch (n->kind)
    {
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
                            PFatt_str (n->sem.attach.attname),
                            literal (n->sem.attach.value));
            break;

        case la_eqjoin:
        case la_semijoin:
            PFarray_printf (dot, "%s (%s = %s)",
                            a_id[n->kind], 
                            PFatt_str (n->sem.eqjoin.att1),
                            PFatt_str (n->sem.eqjoin.att2));
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
            PFarray_printf (dot, "%s (%s:<%s, %s>)", a_id[n->kind],
                            PFatt_str (n->sem.binary.res),
                            PFatt_str (n->sem.binary.att1),
                            PFatt_str (n->sem.binary.att2));
            break;

        case la_num_neg:
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
            PFarray_printf (dot, "%s (%s:<", a_id[n->kind],
                            PFatt_str (n->sem.rownum.attname));

            if (n->sem.rownum.sortby.count)
                PFarray_printf (dot, "%s", 
                                PFatt_str (n->sem.rownum.sortby.atts[0]));

            for (c = 1; c < n->sem.rownum.sortby.count; c++)
                PFarray_printf (dot, ", %s", 
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

        case la_type_assert:
            if (atomtype[n->sem.type.ty])
                PFarray_printf (dot, "%s (%s), type: %s", a_id[n->kind],
                                PFatt_str (n->sem.type.att),
                                atomtype[n->sem.type.ty]);
            else
                PFarray_printf (dot, "%s (%s), type: %i", a_id[n->kind],
                                PFatt_str (n->sem.type.att),
                                n->sem.type.ty);
                
            break;

        case la_cast:
            PFarray_printf (dot, "%s (%s%s%s%s), type: %s", a_id[n->kind],
                            n->sem.type.res?PFatt_str(n->sem.type.res):"",
                            n->sem.type.res?":<":"",
                            PFatt_str (n->sem.type.att),
                            n->sem.type.res?">":"",
                            atomtype[n->sem.type.ty]);
            break;

        case la_scjoin:
        case la_dup_scjoin:
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

            if (n->kind == la_scjoin)
                PFarray_printf (dot, "%s (%s, %s)", 
                                PFty_str (n->sem.scjoin.ty),
                                PFatt_str (n->sem.scjoin.iter),
                                PFatt_str (n->sem.scjoin.item));
            else
                PFarray_printf (dot, "%s (%s:%s)", 
                                PFty_str (n->sem.scjoin.ty),
                                PFatt_str (n->sem.scjoin.item_res),
                                PFatt_str (n->sem.scjoin.item));

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

        case la_element:
            PFarray_printf (dot, "%s (%s, %s:<%s, %s><%s, %s, %s>)",
                            a_id[n->kind],
                            PFatt_str (n->sem.elem.iter_res),
                            PFatt_str (n->sem.elem.item_res),
                            PFatt_str (n->sem.elem.iter_qn),
                            PFatt_str (n->sem.elem.item_qn),
                            PFatt_str (n->sem.elem.iter_val),
                            PFatt_str (n->sem.elem.pos_val),
                            PFatt_str (n->sem.elem.item_val));
            break;
        
        case la_attribute:
            PFarray_printf (dot, "%s (%s:<%s, %s>)", a_id[n->kind],
                            PFatt_str (n->sem.attr.res),
                            PFatt_str (n->sem.attr.qn),
                            PFatt_str (n->sem.attr.val));
            break;

        case la_textnode:
            PFarray_printf (dot, "%s (%s:<%s>)", a_id[n->kind],
                            PFatt_str (n->sem.textnode.res),
                            PFatt_str (n->sem.textnode.item));
            break;

        case la_cond_err:
            PFarray_printf (dot, "%s (%s)\\n%s ...", a_id[n->kind],
                            PFatt_str (n->sem.err.att),
                            PFstrndup (n->sem.err.str, 16));
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

        case la_serialize:
        case la_cross:
        case la_disjunion:
        case la_intersect:
        case la_difference:
        case la_distinct:
        case la_doc_tbl:
        case la_element_tag:
        case la_docnode:
        case la_comment:
        case la_processi:
        case la_merge_adjacent:
        case la_roots:
        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
        case la_rec_fix:
        case la_rec_param:
        case la_rec_nil:
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
        bool all = false;

        while (*fmt) { 
            if (*fmt == '+')
            {
                PFalg_attlist_t icols = PFprop_icols_to_attlist (n->prop);
                PFalg_attlist_t keys = PFprop_keys_to_attlist (n->prop);

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
                
                /* print whether columns do have to respect duplicates */
                if (PFprop_set (n->prop))
                    PFarray_printf (dot, "\\nset"); 

                all = true;
            }
            fmt++;
        }
        fmt = PFstate.format;

        while (!all && *fmt) {
            switch (*fmt) {
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
                    for (unsigned int i = 0; i < icols.count; i++)
                        PFarray_printf (dot, i ? ", %s" : "\\nicols: %s",
                                        PFatt_str (icols.atts[i]));
                } break;
            }
            fmt++;
        }
    }

    /* close up label */
    PFarray_printf (dot, "\", color=%s ];\n", color[n->kind]);

    for (c = 0; c < PFLA_OP_MAXCHILD && n->child[c]; c++) {      
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
        case la_rec_arg:
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
                if (!n->sem.rec_arg.base->bit_dag)
                    node_id = la_dot (dot, n->sem.rec_arg.base, node_id);
            }
            break;

        case la_proxy:
            if (n->sem.proxy.base1) {
                if (n->sem.proxy.base1->node_id == 0)
                    n->sem.proxy.base1->node_id = node_id++;
                
                PFarray_printf (dot, "node%i -> node%i [style=dashed];\n",
                                n->node_id, n->sem.proxy.base1->node_id);
                if (!n->sem.proxy.base1->bit_dag)
                    node_id = la_dot (dot, n->sem.proxy.base1, node_id);
            }
            
            if (n->sem.proxy.base2) {
                if (n->sem.proxy.base2->node_id == 0)
                    n->sem.proxy.base2->node_id = node_id++;
                
                PFarray_printf (dot, "node%i -> node%i [style=dashed];\n",
                                n->node_id, n->sem.proxy.base2->node_id);
                if (!n->sem.proxy.base2->bit_dag)
                    node_id = la_dot (dot, n->sem.proxy.base2, node_id);
            }
            
            if (n->sem.proxy.ref) {
                if (n->sem.proxy.ref->node_id == 0)
                    n->sem.proxy.ref->node_id = node_id++;
                
                PFarray_printf (dot, 
                                "node%i -> node%i "
                                "[style=dashed label=ref];\n",
                                n->node_id, n->sem.proxy.ref->node_id);
                if (!n->sem.proxy.ref->bit_dag)
                    node_id = la_dot (dot, n->sem.proxy.ref, node_id);
            }
            break;
            
        default:
            break;
    }
    
    /* mark node visited */
    n->bit_dag = true;

    for (c = 0; c < PFLA_OP_MAXCHILD && n->child[c]; c++) {
        if (!n->child[c]->bit_dag)
            node_id = la_dot (dot, n->child[c], node_id);
    }

    return node_id;
}

/**
 * Print algebra tree in XML notation.
 * @param xml Array into which we print
 * @param n The current node to print (function is recursive)
 * @param node_id the next available node id.
 */
static unsigned int 
la_xml (PFarray_t *xml, PFla_op_t *n, unsigned int node_id)
{
    unsigned int c;

    assert(n->node_id);

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
        PFarray_printf (xml, "      <col name='%s' types='",
                        PFatt_str (n->schema.items[i].name));
        for (PFalg_simple_type_t t = 1; t; t <<= 1) {
            if (t & n->schema.items[i].type) {
                switch (t) {
                    case aat_nat:    PFarray_printf (xml, "nat");    break;
                    case aat_int:    PFarray_printf (xml, "int");    break;
                    case aat_str:    PFarray_printf (xml, "str");    break;
                    case aat_dec:    PFarray_printf (xml, "dec");    break;
                    case aat_dbl:    PFarray_printf (xml, "dbl");    break;
                    case aat_bln:    PFarray_printf (xml, "bln");    break;
                    case aat_qname:  PFarray_printf (xml, "qname");  break;
                    case aat_uA:     PFarray_printf (xml, "uA");     break;
                    case aat_node:   PFarray_printf (xml, "node");   break;
                    case aat_anode:  PFarray_printf (xml, "anode");  break;
                    case aat_attr:   PFarray_printf (xml, "attr");   break;
                    case aat_afrag:  PFarray_printf (xml, "afrag");  break;
                    case aat_pnode:  PFarray_printf (xml, "pnode");  break;
                    case aat_pre:    PFarray_printf (xml, "pre");    break;
                    case aat_pfrag:  PFarray_printf (xml, "pfrag");  break;
                }
                PFarray_printf (xml, " ");
            }
        }
        PFarray_printf (xml, "'/>\n");
    }
    PFarray_printf (xml, "    </schema>\n");

    if (PFstate.format) {

        char *fmt = PFstate.format;
        bool all = false;

        PFarray_printf (xml, "    <properties>\n");
        while (*fmt) { 
            if (*fmt == '+')
            {
                PFalg_attlist_t icols = PFprop_icols_to_attlist (n->prop);
                PFalg_attlist_t keys = PFprop_keys_to_attlist (n->prop);

                /* if present print cardinality */
                if (PFprop_card (n->prop))
                    PFarray_printf (xml, "      <card value=\"%i\"/>\n",
                                    PFprop_card (n->prop));

                /* list attributes marked const */
                for (unsigned int i = 0;
                        i < PFprop_const_count (n->prop); i++)
                    PFarray_printf (xml, "      <const column=\"%s\"/>\n",
                                    PFatt_str (
                                        PFprop_const_at (n->prop, i)));

                /* list icols attributes */
                for (unsigned int i = 0; i < icols.count; i++)
                    PFarray_printf (xml, "      <icols column=\"%s\"/>\n",
                                    PFatt_str (icols.atts[i]));

                /* list keys attributes */
                for (unsigned int i = 0; i < keys.count; i++)
                    PFarray_printf (xml, "      <keys column=\"%s\"/>\n",
                                    PFatt_str (keys.atts[i]));

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

                if (PFprop_set (n->prop))
                    PFarray_printf (xml, "      <duplicates allowed=\"yes\"/>\n");

                all = true;
            }
            fmt++;
        }
        fmt = PFstate.format;

        while (!all && *fmt) {
            switch (*fmt) {
                /* list attributes marked const if requested */
                case 'c':
                    for (unsigned int i = 0;
                            i < PFprop_const_count (n->prop); i++)
                        PFarray_printf (xml, 
                                        "      <const column=\"%s\"/>\n",
                                        PFatt_str (
                                            PFprop_const_at (n->prop, i)));
                    break;
                /* list icols attributes if requested */
                case 'i':
                {
                    PFalg_attlist_t icols = 
                                    PFprop_icols_to_attlist (n->prop);
                    for (unsigned int i = 0; i < icols.count; i++)
                        PFarray_printf (xml, 
                                        "      <icols column=\"%s\"/>\n",
                                        PFatt_str (icols.atts[i]));
                } break;
            }
            fmt++;
        }
        PFarray_printf (xml, "    </properties>\n");
    }

    /* create label */
    switch (n->kind)
    {
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
                                    "          %s\n",
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
                            PFatt_str (n->sem.attach.attname),
                            xml_literal (n->sem.attach.value));
            break;

        case la_eqjoin:
        case la_semijoin:
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
                            "      <column name=\"%s\" new=\"false\">\n"
                            "        <annotation>select on column %s results "
                                    "in smaller relation</annotation>\n"
                            "      </column>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.select.att),
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

        case la_num_neg:
        case la_bool_not:
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

        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_seqty1:
        case la_all:
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

        case la_count:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" new=\"true\">\n"
                            "        <annotation>result of the count operator"
                                    "</annotation>\n"
                            "      </column>\n",
                            PFatt_str (n->sem.aggr.res));
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

        case la_rownum:
            PFarray_printf (xml, 
                            "    <content>\n" 
                            "      <column name=\"%s\" new=\"true\">\n"
                            "        <annotation>new rownum column"
                                    "</annotation>\n"
                            "      </column>\n",
                            PFatt_str (n->sem.rownum.attname));

            for (c = 0; c < n->sem.rownum.sortby.count; c++)
                PFarray_printf (xml, 
                                "      <column name=\"%s\" function=\"sort\""
                                        " position=\"%u\" new=\"false\">\n"
                                "        <annotation>%u. sort argument"
                                        "</annotation>\n"
                                "      </column>\n",
                                PFatt_str (n->sem.rownum.sortby.atts[c]),
                                c+1, c+1);

            if (n->sem.rownum.part != att_NULL)
                PFarray_printf (xml,
                                "      <column name=\"%s\" function=\"partition\""
                                        " new=\"false\">\n"
                                "        <annotation>partitioning argument"
                                        "</annotation>\n"
                                "      </column>\n",
                                PFatt_str (n->sem.rownum.part));

            PFarray_printf (xml, "    </content>\n");
            break;

        case la_number:
            PFarray_printf (xml, 
                            "    <content>\n" 
                            "      <column name=\"%s\" new=\"true\">\n"
                            "        <annotation>result of the count operator"
                                    "</annotation>\n"
                            "      </column>\n",
                            PFatt_str (n->sem.number.attname));

            if (n->sem.rownum.part != att_NULL)
                PFarray_printf (xml,
                                "      <column name=\"%s\" function=\"partition\""
                                        " new=\"false\">\n"
                                "        <annotation>partitioning argument"
                                        "</annotation>\n"
                                "      </column>\n",
                                PFatt_str (n->sem.number.part));

            PFarray_printf (xml, "    </content>\n");
            break;

        case la_type:
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

        case la_type_assert:
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

        case la_cast:
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

        case la_scjoin:
        case la_dup_scjoin:
            PFarray_printf (xml, "    <content>\n      <step axis=\"");
                
            /* print out XPath axis */
            switch (n->sem.scjoin.axis)
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

            if (n->kind == la_scjoin)
                PFarray_printf (xml,
                                "\" type=\"%s\"/>\n"
                                "      <column name=\"%s\" function=\"iter\"/>\n"
                                "      <column name=\"%s\" function=\"item\"/>\n"
                                "    </content>\n",
                                PFty_str (n->sem.scjoin.ty),
                                PFatt_str (n->sem.scjoin.iter),
                                PFatt_str (n->sem.scjoin.item));
            else
                PFarray_printf (xml,
                                "\" type=\"%s\"/>\n"
                                "      <column name=\"%s\" new=\"true\"/>\n"
                                "      <column name=\"%s\" function=\"item\"/>\n"
                                "    </content>\n",
                                PFty_str (n->sem.scjoin.ty),
                                PFatt_str (n->sem.scjoin.item_res),
                                PFatt_str (n->sem.scjoin.item));
            break;

        case la_doc_tbl:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.doc_tbl.iter),
                            PFatt_str (n->sem.doc_tbl.item));
            break;

        case la_doc_access:
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

        case la_element:
            PFarray_printf (xml,
                            "    <content>\n"
                            "      <column name=\"%s\" function=\"iter\"/>\n"
                            "      <column name=\"%s\" function=\"pos\"/>\n"
                            "      <column name=\"%s\" function=\"item\"/>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.elem.iter_val),
                            PFatt_str (n->sem.elem.pos_val),
                            PFatt_str (n->sem.elem.item_val));
            break;
        
        case la_attribute:
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

        case la_textnode:
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
                            "      <column name=\"%s\" new=\"false\">\n"
                            "        <error>%s</error>\n"
                            "        <annotation>argument that triggers"
                                    " error message</annotation>\n"
                            "      </column>\n"
                            "    </content>\n",
                            PFatt_str (n->sem.err.att),
                            PFstrdup (n->sem.err.str));
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

    for (c = 0; c < PFLA_OP_MAXCHILD && n->child[c] != 0; c++) {      
        if (!n->child[c]->bit_dag) {
            node_id = la_xml (xml, n->child[c], node_id);
        }
    }
    return node_id;
}

static void
reset_node_id (PFla_op_t *n)
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

        root->node_id = 1;
        la_dot (dot, root, root->node_id + 1);
        PFla_dag_reset (root);
        reset_node_id (root);
        PFla_dag_reset (root);

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

        PFarray_printf (xml, "<logical_query_plan>\n");
        /* add domain subdomain relationships if required */
        if (PFstate.format) {
            char *fmt = PFstate.format;
            while (*fmt) { 
                if (*fmt == '+') {
                        PFprop_write_dom_rel_xml (xml, root->prop);
                        break;
                }
                fmt++;
            }
        }


        root->node_id = 1;
        la_xml (xml, root, root->node_id + 1);
        PFla_dag_reset (root);
        reset_node_id (root);
        PFla_dag_reset (root);

        PFarray_printf (xml, "</logical_query_plan>\n");
        /* put content of array into file */
        fprintf (f, "%s", (char *) xml->base);
    }
}

/* vim:set shiftwidth=4 expandtab: */
