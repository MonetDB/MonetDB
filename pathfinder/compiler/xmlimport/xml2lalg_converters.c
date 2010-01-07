/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * 
 * Converting data (strings) of XML-serialized logical Algebra 
 * Plans to the corresponding PF data types
 *
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
 * $Id: xml2lalg_converters.c,v 1.0 2007/10/31 22:00:00 ts-tum 
 * Exp $ 
 */

#include "pf_config.h"
#include "pathfinder.h"
#include "xml2lalg_converters.h"

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

#include "oops.h"
#include "logical_mnemonic.h"
#include "string_utils.h"


PFla_op_kind_t 
PFxml2la_conv_2PFLA_OpKind (const char* s)
{
    if (false) return la_dummy; /* discard first case */
#define mapto(op,str) else if (strcmp (s, (str)) == 0) return (op);
    /* the name mapping was copied from logdebug.c 
       (and should stay aligned) */
    mapto (la_serialize_seq,   "serialize sequence")
    mapto (la_serialize_rel,   "serialize relation")
    mapto (la_side_effects,    "observe side effects")
    mapto (la_lit_tbl,         "table")
    mapto (la_empty_tbl,       "empty_tbl")
    mapto (la_ref_tbl ,        "ref_tbl")
    mapto (la_attach,          "attach")
    mapto (la_cross,           "cross")
    mapto (la_eqjoin,          "eqjoin")
    mapto (la_semijoin,        "semijoin")
    mapto (la_thetajoin,       "thetajoin")
    mapto (la_project,         "project")
    mapto (la_select,          "select")
    mapto (la_pos_select,      "pos_select")
    mapto (la_disjunion,       "union")
    mapto (la_intersect,       "intersect")
    mapto (la_difference,      "difference")
    mapto (la_distinct,        "distinct")
    mapto (la_fun_1to1,        "fun")
    mapto (la_num_eq,          "eq")
    mapto (la_num_gt,          "gt")
    mapto (la_bool_and ,       "and")
    mapto (la_bool_or ,        "or")
    mapto (la_bool_not,        "not")
    mapto (la_to,              "op:to")
    mapto (la_aggr,            "aggr")
    mapto (la_aggr,            "avg")
    mapto (la_aggr,            "max")
    mapto (la_aggr,            "min")
    mapto (la_aggr,            "sum")
    mapto (la_aggr,            "count")
    mapto (la_aggr,            "prod")
    mapto (la_rownum,          "rownum")
    mapto (la_rowrank,         "rowrank")
    mapto (la_rank,            "rank")
    mapto (la_rowid,           "rowid")
    mapto (la_type,            "type")
    mapto (la_type_assert,     "type assertion")
    mapto (la_cast,            "cast")
    mapto (la_aggr,            "seqty1")
    mapto (la_aggr,            "all")
    mapto (la_step,            "XPath step")
    mapto (la_step_join,       "path step join")
    mapto (la_guide_step,      "XPath step (with guide information)")
    mapto (la_guide_step_join, "path step join (with guide information)")
    mapto (la_doc_index_join,  "document index join")
    mapto (la_doc_tbl,         "document table access")
    mapto (la_doc_access,      "#pf:string-value")
    mapto (la_twig,            "twig_construction")
    mapto (la_fcns,            "constructor_sequence_(fcns)")
    mapto (la_docnode,         "documentnode_construction")
    mapto (la_element,         "element_construction")
    mapto (la_attribute,       "attribute_construction")
    mapto (la_textnode,        "textnode_construction")
    mapto (la_comment,         "comment_construction")
    mapto (la_processi,        "pi_construction")
    mapto (la_content,         "constructor_content")
    mapto (la_merge_adjacent,  "#pf:merge-adjacent-text-nodes")
    mapto (la_roots,           "ROOTS")
    mapto (la_fragment,        "FRAG")
    mapto (la_frag_extract,    "FRAG EXTRACT")
    mapto (la_frag_union,      "FRAG_UNION")
    mapto (la_empty_frag,      "EMPTY_FRAG")
    mapto (la_error,           "error")
    mapto (la_nil,             "nil")
    mapto (la_cache,           "cache")
    mapto (la_trace,           "trace")
    mapto (la_trace_items,     "trace items")
    mapto (la_trace_msg,       "trace msg")
    mapto (la_trace_map,       "trace map")
    mapto (la_rec_fix,         "recursion fix")
    mapto (la_rec_param,       "recursion param")
    mapto (la_rec_arg,         "recursion arg")
    mapto (la_rec_base,        "recursion base")
    mapto (la_fun_call,        "function call")
    mapto (la_fun_param,       "function call parameter")
    mapto (la_fun_frag_param,  "function call fragment parameter")
    mapto (la_proxy,           "proxy")
    mapto (la_proxy_base,      "proxy base")
    mapto (la_string_join,     "fn:string-join")
    mapto (la_dummy,           "dummy")

    PFoops (OOPS_FATAL, "unknown operator kind (%s)", s);
    /* pacify picky compilers */
    return la_dummy;
}

PFalg_col_t 
PFxml2la_conv_2PFLA_attributeName (const char* s)
{
         if (strcmp(s, "(NULL)") == 0) return col_NULL;
    else if (strcmp(s, "iter"  ) == 0) return col_iter;
    else if (strcmp(s, "item"  ) == 0) return col_item;
    else if (strcmp(s, "pos"   ) == 0) return col_pos;
    else if (strcmp(s, "iter1" ) == 0) return col_iter1;
    else if (strcmp(s, "item1" ) == 0) return col_item1;
    else if (strcmp(s, "pos1"  ) == 0) return col_pos1;
    else if (strcmp(s, "inner" ) == 0) return col_inner;
    else if (strcmp(s, "outer" ) == 0) return col_outer;
    else if (strcmp(s, "sort"  ) == 0) return col_sort;
    else if (strcmp(s, "sort1" ) == 0) return col_sort1;
    else if (strcmp(s, "sort2" ) == 0) return col_sort2;
    else if (strcmp(s, "sort3" ) == 0) return col_sort3;
    else if (strcmp(s, "sort4" ) == 0) return col_sort4;
    else if (strcmp(s, "sort5" ) == 0) return col_sort5;
    else if (strcmp(s, "sort6" ) == 0) return col_sort6;
    else if (strcmp(s, "sort7" ) == 0) return col_sort7;
    else if (strcmp(s, "ord"   ) == 0) return col_ord;
    else if (strcmp(s, "iter2" ) == 0) return col_iter2;
    else if (strcmp(s, "iter3" ) == 0) return col_iter3;
    else if (strcmp(s, "iter4" ) == 0) return col_iter4;
    else if (strcmp(s, "iter5" ) == 0) return col_iter5;
    else if (strcmp(s, "iter6" ) == 0) return col_iter6;
    else if (strcmp(s, "res"   ) == 0) return col_res;
    else if (strcmp(s, "res1"  ) == 0) return col_res1;
    else if (strcmp(s, "cast"  ) == 0) return col_cast;
    else if (strcmp(s, "item2" ) == 0) return col_item2;
    else if (strcmp(s, "item3" ) == 0) return col_item3;
    else if (strcmp(s, "item4" ) == 0) return col_subty;
    else if (strcmp(s, "item5" ) == 0) return col_itemty;
    else if (strcmp(s, "item6" ) == 0) return col_notsub;
    else if (strcmp(s, "score1") == 0) return col_score1;
    else if (strcmp(s, "score2") == 0) return col_score2;

    PFoops (OOPS_FATAL, "unknown attribute name (%s)", s);
    return -1; /* pacify picky compilers */
}

PFalg_col_t 
PFxml2la_conv_2PFLA_attributeName_unq (const char* s)
{
    char        *idString = NULL;
    PFalg_col_t  ori      = col_NULL;
    unsigned int length   = 0;
    PFalg_col_t  rv;

    if (PFstrUtils_beginsWith(s, "iter")) {
        ori = col_iter;
        length = strlen("iter");
    }
    else if (PFstrUtils_beginsWith(s, "pos")) {
        ori = col_pos;
        length = strlen("pos");
    }
    else if (PFstrUtils_beginsWith(s, "item")) {
        ori = col_item;
        length = strlen("item");
    }
    else if (PFstrUtils_beginsWith(s, "score")) {
        ori = col_score1;
        length = strlen("score");
    }
    else if (!strcmp (s, "(NULL)"))
        return col_NULL;
    else
        PFoops (OOPS_FATAL, "don't know what to do with (%s)", s);

    assert (ori);

    idString = PFstrUtils_substring (s + length, s + strlen(s));

    if (idString[0] == '0')
        rv = PFcol_new_fixed (ori, 0);
    else
        rv = PFcol_new_fixed (ori, atoi (idString));
    free(idString);
    return rv;
}

/**
 * convert simple type name string to PFalg_simple_type_t
 */
PFalg_simple_type_t 
PFxml2la_conv_2PFLA_atomType (char* typeString)
{
         if (strcmp(typeString, "nat") == 0)	return aat_nat;
    else if (strcmp(typeString, "int") == 0)	return aat_int;
    else if (strcmp(typeString, "str") == 0)	return aat_str;
    else if (strcmp(typeString, "dec") == 0)	return aat_dec;
    else if (strcmp(typeString, "dbl") == 0)	return aat_dbl;
    else if (strcmp(typeString, "bool") == 0)	return aat_bln;
    else if (strcmp(typeString, "uA") == 0)	return aat_uA;
    else if (strcmp(typeString, "qname") == 0)	return aat_qname;
    else if (strcmp(typeString, "node")== 0)	return aat_node;
    else if (strcmp(typeString, "attr")== 0)	return aat_anode;
    else if (strcmp(typeString, "pnode") == 0)	return aat_pnode;

    PFoops (OOPS_FATAL, "unknown attribute simple type (%s)", typeString);
    return -1; /* pacify picky compilers */
}

PFalg_atom_t 
PFxml2la_conv_2PFLA_atom (PFalg_simple_type_t type,
                          char *prefix, char *uri, char *val)
{
    switch (type) {
        case aat_nat:
            return lit_nat (atoi (val));
        case aat_int:
            return lit_int (atoi (val));
        case aat_dec:
            return lit_dec (atof (val));
        case aat_dbl:
            return lit_dbl (atof (val));
        case aat_str:
            return lit_str (val);
        case aat_uA:
            return lit_uA (val);
        case aat_bln:
            return lit_bln (strcmp(val, "true") == 0 ? true : false);
        case aat_qname:
            return lit_qname (PFqname ((PFns_t) { .prefix=prefix, .uri=uri },
                              val));
        default:       
            PFoops (OOPS_FATAL, "don't know what to do (%s, %s)", 
                    PFalg_simple_type_str (type), val);
            /* pacify picky compilers */
            return (PFalg_atom_t) { .type = 0, .val = { .nat_ = 0 } }; 
    }
}

PFalg_comp_t 
PFxml2la_conv_2PFLA_comparisonType (char* s)
{
         if (strcmp(s, "eq") == 0) return alg_comp_eq;
    else if (strcmp(s, "gt") == 0) return alg_comp_gt;
    else if (strcmp(s, "ge") == 0) return alg_comp_ge;
    else if (strcmp(s, "lt") == 0) return alg_comp_lt;
    else if (strcmp(s, "le") == 0) return alg_comp_le;
    else if (strcmp(s, "ne") == 0) return alg_comp_ne;

    PFoops (OOPS_FATAL, "don't know what to do (%s)", s);
    /* pacify picky compilers */
    return alg_comp_eq; 
}

PFalg_aggr_kind_t 
PFxml2la_conv_2PFLA_aggregateType (char* s)
{
    if (false) return alg_aggr_count; /* discard first case */
#define mapto_aggr_kind(kind)                              \
    else if (strcmp (s, PFalg_aggr_kind_str((kind))) == 0) \
        return (kind);
    /* the kind was copied from algebra.c:PFalg_axis_str()
       (and should stay aligned) */
    mapto_aggr_kind (alg_aggr_dist)
    mapto_aggr_kind (alg_aggr_count)
    mapto_aggr_kind (alg_aggr_min)
    mapto_aggr_kind (alg_aggr_max)
    mapto_aggr_kind (alg_aggr_avg)
    mapto_aggr_kind (alg_aggr_sum)
    mapto_aggr_kind (alg_aggr_seqty1)
    mapto_aggr_kind (alg_aggr_all)
    mapto_aggr_kind (alg_aggr_prod)

    PFoops (OOPS_FATAL, "don't know what to do (%s)", s);
    /* pacify picky compilers */
    return alg_aggr_count;
}

PFalg_fun_t 
PFxml2la_conv_2PFLA_functionType (char* s)
{
    if (false) return alg_fun_num_add; /* discard first case */
#define mapto_fun_kind(kind) else if (strcmp (s, PFalg_fun_str((kind))) == 0) \
                                 return (kind);
    /* the kind was copied from algebra.c:PFalg_fun_str()
       (and should stay aligned) */
    mapto_fun_kind (alg_fun_num_add)
    mapto_fun_kind (alg_fun_num_subtract)
    mapto_fun_kind (alg_fun_num_multiply)
    mapto_fun_kind (alg_fun_num_divide)
    mapto_fun_kind (alg_fun_num_modulo)
    mapto_fun_kind (alg_fun_fn_abs)
    mapto_fun_kind (alg_fun_fn_ceiling)
    mapto_fun_kind (alg_fun_fn_floor)
    mapto_fun_kind (alg_fun_fn_round)
    mapto_fun_kind (alg_fun_fn_concat)
    mapto_fun_kind (alg_fun_fn_substring)
    mapto_fun_kind (alg_fun_fn_substring_dbl)
    mapto_fun_kind (alg_fun_fn_string_length)
    mapto_fun_kind (alg_fun_fn_normalize_space)
    mapto_fun_kind (alg_fun_fn_upper_case)
    mapto_fun_kind (alg_fun_fn_lower_case)
    mapto_fun_kind (alg_fun_fn_translate)
    mapto_fun_kind (alg_fun_fn_contains)
    mapto_fun_kind (alg_fun_fn_starts_with)
    mapto_fun_kind (alg_fun_fn_ends_with)
    mapto_fun_kind (alg_fun_fn_substring_before)
    mapto_fun_kind (alg_fun_fn_substring_after)
    mapto_fun_kind (alg_fun_fn_matches)
    mapto_fun_kind (alg_fun_fn_matches_flag)
    mapto_fun_kind (alg_fun_fn_replace)
    mapto_fun_kind (alg_fun_fn_replace_flag)
    mapto_fun_kind (alg_fun_fn_name)
    mapto_fun_kind (alg_fun_fn_local_name)
    mapto_fun_kind (alg_fun_fn_namespace_uri)
    mapto_fun_kind (alg_fun_fn_number)
    mapto_fun_kind (alg_fun_fn_number_lax)
    mapto_fun_kind (alg_fun_fn_qname)
    mapto_fun_kind (alg_fun_fn_doc_available)
    mapto_fun_kind (alg_fun_pf_fragment)
    mapto_fun_kind (alg_fun_pf_supernode)
    mapto_fun_kind (alg_fun_pf_add_doc_str)
    mapto_fun_kind (alg_fun_pf_add_doc_str_int)
    mapto_fun_kind (alg_fun_pf_del_doc)
    mapto_fun_kind (alg_fun_pf_nid)
    mapto_fun_kind (alg_fun_pf_docname)
    mapto_fun_kind (alg_fun_upd_rename)
    mapto_fun_kind (alg_fun_upd_delete)
    mapto_fun_kind (alg_fun_upd_insert_into_as_first)
    mapto_fun_kind (alg_fun_upd_insert_into_as_last)
    mapto_fun_kind (alg_fun_upd_insert_before)
    mapto_fun_kind (alg_fun_upd_insert_after)
    mapto_fun_kind (alg_fun_upd_replace_value_att)
    mapto_fun_kind (alg_fun_upd_replace_value)
    mapto_fun_kind (alg_fun_upd_replace_element)
    mapto_fun_kind (alg_fun_upd_replace_node)
    mapto_fun_kind (alg_fun_fn_year_from_datetime)
    mapto_fun_kind (alg_fun_fn_month_from_datetime)
    mapto_fun_kind (alg_fun_fn_day_from_datetime)
    mapto_fun_kind (alg_fun_fn_hours_from_datetime)
    mapto_fun_kind (alg_fun_fn_minutes_from_datetime)
    mapto_fun_kind (alg_fun_fn_seconds_from_datetime)
    mapto_fun_kind (alg_fun_fn_year_from_date)
    mapto_fun_kind (alg_fun_fn_month_from_date)
    mapto_fun_kind (alg_fun_fn_day_from_date)
    mapto_fun_kind (alg_fun_fn_hours_from_time)
    mapto_fun_kind (alg_fun_fn_minutes_from_time)
    mapto_fun_kind (alg_fun_fn_seconds_from_time)

    PFoops (OOPS_FATAL, "don't know what to do (%s)", s);
    /* pacify picky compilers */
    return alg_fun_num_add; 
}

bool 
PFxml2la_conv_2PFLA_orderingDirection (char* s) 
{
    if (strcmp(s, "ascending") == 0)
        return DIR_ASC;
    else if (strcmp(s, "descending") == 0)
        return DIR_DESC;
    else
        PFoops (OOPS_FATAL, "don't know what to do (%s)", s);

    /* pacify picky compilers */
    return DIR_ASC;
}

PFalg_axis_t 
PFxml2la_conv_2PFLA_xpathaxis(char* s) 
{
    if (false) return alg_anc; /* discard first case */
#define mapto_axis_kind(kind)                         \
    else if (strcmp (s, PFalg_axis_str((kind))) == 0) \
        return (kind);
    /* the kind was copied from algebra.c:PFalg_axis_str()
       (and should stay aligned) */
    mapto_axis_kind (alg_anc)
    mapto_axis_kind (alg_anc_s)
    mapto_axis_kind (alg_attr)
    mapto_axis_kind (alg_chld)
    mapto_axis_kind (alg_desc)
    mapto_axis_kind (alg_desc_s)
    mapto_axis_kind (alg_fol)
    mapto_axis_kind (alg_fol_s)
    mapto_axis_kind (alg_par)
    mapto_axis_kind (alg_prec)
    mapto_axis_kind (alg_prec_s)
    mapto_axis_kind (alg_self)

    PFoops (OOPS_FATAL, "don't know what to do (%s)", s);
    /* pacify picky compilers */
    return alg_anc;
}

PFalg_node_kind_t 
PFxml2la_conv_2PFLA_nodekind(char* s) 
{
    if (false) return node_kind_node; /* discard first case */
#define mapto_kind_kind(kind)                              \
    else if (strcmp (s, PFalg_node_kind_str((kind))) == 0) \
        return (kind);
    /* the kind was copied from algebra.c:PFalg_node_kind_str()
       (and should stay aligned) */
    mapto_kind_kind (node_kind_elem)
    mapto_kind_kind (node_kind_attr)
    mapto_kind_kind (node_kind_text)
    mapto_kind_kind (node_kind_pi)
    mapto_kind_kind (node_kind_comm)
    mapto_kind_kind (node_kind_doc)
    mapto_kind_kind (node_kind_node)

    PFoops (OOPS_FATAL, "don't know what to do (%s)", s);
    /* pacify picky compilers */
    return node_kind_node;
}

PFalg_fun_call_t 
PFxml2la_conv_2PFLA_fun_callkind(char* s) 
{
    if (false) return alg_fun_call_pf_documents; /* discard first case */
#define mapto_fun_call_kind(kind)                              \
    else if (strcmp (s, PFalg_fun_call_kind_str((kind))) == 0) \
        return (kind);
    /* the kind was copied from algebra.c:PFalg_node_kind_str()
       (and should stay aligned) */
    mapto_fun_call_kind (alg_fun_call_pf_documents)
    mapto_fun_call_kind (alg_fun_call_pf_documents_unsafe)
    mapto_fun_call_kind (alg_fun_call_pf_documents_str)
    mapto_fun_call_kind (alg_fun_call_pf_documents_str_unsafe)
    mapto_fun_call_kind (alg_fun_call_pf_collections)
    mapto_fun_call_kind (alg_fun_call_pf_collections_unsafe)
    mapto_fun_call_kind (alg_fun_call_xrpc)
    mapto_fun_call_kind (alg_fun_call_xrpc_helpers)
    mapto_fun_call_kind (alg_fun_call_tijah)
    mapto_fun_call_kind (alg_fun_call_cache)

    PFoops (OOPS_FATAL, "don't know what to do (%s)", s);
    /* pacify picky compilers */
    return alg_fun_call_pf_documents;
}

PFalg_doc_t 
PFxml2la_conv_2PFLA_docType(char* s) 
{
         if (strcmp(s, "doc.attribute") == 0) return doc_atext;
    else if (strcmp(s, "doc.textnode") == 0)  return doc_text;
    else if (strcmp(s, "doc.comment") == 0)   return doc_comm;
    else if (strcmp(s, "doc.pi") == 0)        return doc_pi_text;
    else if (strcmp(s, "qname") == 0)         return doc_qname;
    else if (strcmp(s, "atomize") == 0)       return doc_atomize;

    PFoops (OOPS_FATAL, "don't know what to do (%s)", s);
    /* pacify picky compilers */
    return doc_atext;
}

PFalg_doc_tbl_kind_t 
PFxml2la_conv_2PFLA_doctblType (char* s) 
{
         if (strcmp(s, "fn:doc") == 0)        return alg_dt_doc;
    else if (strcmp(s, "fn:collection") == 0) return alg_dt_col;

    PFoops (OOPS_FATAL, "don't know what to do (%s)", s);
    /* pacify picky compilers */
    return alg_dt_doc;
}
