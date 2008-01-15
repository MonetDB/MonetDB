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
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id: xml2lalg_converters.c,v 1.0 2007/10/31 22:00:00 ts-tum 
 * Exp $ 
 */


#include "pathfinder.h"

#include <stdio.h>
#ifdef HAVE_STDBOOL_H
    #include <stdbool.h>
#endif

#include <string.h>
#include <assert.h>
#include <stdlib.h>






#include "oops.h"


#include "algebra.h"
#include "logical.h"
#include "logical_mnemonic.h"

#include "string_utils.h"

#include "xml2lalg_converters.h"





/******************************************************************************/
/******************************************************************************/


nat 
PFxml2la_conv_2PFLA_atomValue_nat(char* s)
{
    return atoi(s);
}

/******************************************************************************/
/******************************************************************************/

long long int 
PFxml2la_conv_2PFLA_atomValue_int(char* s)
{
    return atoi(s);
}

/******************************************************************************/
/******************************************************************************/

char* 
PFxml2la_conv_2PFLA_atomValue_str(char* s)
{
    return s;
}

/******************************************************************************/
/******************************************************************************/


double 
PFxml2la_conv_2PFLA_atomValue_dec(char* s)
{
    return atof(s);
}
/******************************************************************************/
/******************************************************************************/

double 
PFxml2la_conv_2PFLA_atomValue_dbl(char* s)
{
    return atof(s);
}

/******************************************************************************/
/******************************************************************************/

bool 
PFxml2la_conv_2PFLA_atomValue_bln(char* s)
{
   
    if(strcmp(s, "true") == 0)
    {
        return true;
    } 
    else if(strcmp(s, "false") == 0)
    {
        return false;
    } 
    else 
    {
        PFoops (OOPS_FATAL, "don't know what to do with (%s)", s);
        /* pacify picky compilers */
        return -1;
    }
}





/******************************************************************************/
/******************************************************************************/

PFla_op_kind_t 
PFxml2la_conv_2PFLA_OpKind(const char* s)
{
    if (strcmp(s, "serialize sequence") == 0)
    {
        return la_serialize_seq;
    }
    else if (strcmp(s, "serialize relation") == 0)
    {
        return la_serialize_rel;
    }
    else if (strcmp(s, "table") == 0)
    {
        return la_lit_tbl;
    }
    else if (strcmp(s, "empty_tbl") == 0)
    {
        return la_empty_tbl;
    }
    else if (strcmp(s, "ref_tbl") == 0)
    {
        return la_ref_tbl;
    }
    else if (strcmp(s, "attach") == 0)
    {
        return la_attach;
    }
    else if (strcmp(s, "cross") == 0)
    {
        return la_cross;
    }
    else if (strcmp(s, "eqjoin") == 0)
    {
        return la_eqjoin;
    }
    else if (strcmp(s, "semijoin") == 0)
    {
        return la_semijoin;
    }
    else if (strcmp(s, "thetajoin") == 0)
    {
        return la_thetajoin;
    }
    else if (strcmp(s, "project") == 0)
    {
        return la_project;
    }
    else if (strcmp(s, "select") == 0)
    {
        return la_select;
    }
    else if (strcmp(s, "pos_select") == 0)
    {
        return la_pos_select;
    }
    else if (strcmp(s, "union") == 0)
    {
        return la_disjunion;
    }
    else if (strcmp(s, "intersect") == 0)
    {
        return la_intersect;
    }
    else if (strcmp(s, "difference") == 0)
    {
        return la_difference;
    }
    else if (strcmp(s, "distinct") == 0)
    {
        return la_distinct;
    }
    else if (strcmp(s, "fun") == 0)
    {
        return la_fun_1to1;
    }
    else if (strcmp(s, "eq") == 0)
    {
        return la_num_eq;
    }
    else if (strcmp(s, "gt") == 0)
    {
        return la_num_gt;
    }
    else if (strcmp(s, "and") == 0)
    {
        return la_bool_and;
    }
    else if (strcmp(s, "or") == 0)
    {
        return la_bool_or;
    }
    else if (strcmp(s, "not") == 0)
    {
        return la_bool_not;
    }
    else if (strcmp(s, "op:to") == 0)
    {
        return la_to;
    }
    else if (strcmp(s, "avg") == 0)
    {
        return la_avg;
    }
    else if (strcmp(s, "max") == 0)
    {
        return la_max;
    }
    else if (strcmp(s, "min") == 0)
    {
        return la_min;
    }
    else if (strcmp(s, "sum") == 0)
    {
        return la_sum;
    }
    else if (strcmp(s, "count") == 0)
    {
        return la_count;
    }
    else if (strcmp(s, "rownum") == 0)
    {
        return la_rownum;
    }
    else if (strcmp(s, "rowrank") == 0)
    {
        return la_rowrank;
    }
    else if (strcmp(s, "rank") == 0)
    {
        return la_rank;
    }
    else if (strcmp(s, "rowid") == 0)
    {
        return la_rowid;
    }
    else if (strcmp(s, "type") == 0)
    {
        return la_type;
    }
    else if (strcmp(s, "type assertion") == 0)
    {
        return la_type_assert;
    }
    else if (strcmp(s, "cast") == 0)
    {
        return la_cast;
    }
    else if (strcmp(s, "seqty1") == 0)
    {
        return la_seqty1;
    }
    else if (strcmp(s, "all") == 0)
    {
        return la_all;
    }
    else if (strcmp(s, "XPath step") == 0)
    {
        return la_step;
    }
    else if (strcmp(s, "path step join") == 0)
    {
        return la_step_join;
    }
    else if (strcmp(s, "XPath step (with guide information)") == 0)
    {
        return la_guide_step;
    }
    else if (strcmp(s, "path step join (with guide information)") == 0)
    {
        return la_guide_step_join;
    }
//     else if (strcmp(s, "???") == 0)
//     {
//         return la_doc_index_join;
//     }
    else if (strcmp(s, "fn:doc") == 0)
    {
        return la_doc_tbl;
    }
    else if (strcmp(s, "#pf:string-value") == 0)
    {
        return la_doc_access;
    }
    else if (strcmp(s, "twig_construction") == 0)
    {
        return la_twig;
    }
    else if (strcmp(s, "constructor_sequence_(fcns)") == 0)
    {
        return la_fcns;
    }
    else if (strcmp(s, "documentnode_construction") == 0)
    {
        return la_docnode;
    }
    else if (strcmp(s, "element_construction") == 0)
    {
        return la_element;
    }
    else if (strcmp(s, "attribute_construction") == 0)
    {
        return la_attribute;
    }
    else if (strcmp(s, "textnode_construction") == 0)
    {
        return la_textnode;
    }
    else if (strcmp(s, "comment_construction") == 0)
    {
        return la_comment;
    }
    else if (strcmp(s, "pi_construction") == 0)
    {
        return la_processi;
    }
    else if (strcmp(s, "constructor_content") == 0)
    {
        return la_content;
    }
    else if (strcmp(s, "#pf:merge-adjacent-text-nodeStore") == 0)
    {
        return la_merge_adjacent;
    }
    else if (strcmp(s, "ROOTS") == 0)
    {
        return la_roots;
    }
    else if (strcmp(s, "FRAG") == 0)
    {
        return la_fragment;
    }
    else if (strcmp(s, "FRAG_UNION") == 0)
    {
        return la_frag_union;
    }
    else if (strcmp(s, "EMPTY_FRAG") == 0)
    {
        return la_empty_frag;
    }
    else if (strcmp(s, "error") == 0)
    {
        return la_cond_err;
    }
    else if (strcmp(s, "nil") == 0)
    {
        return la_nil;
    }
    else if (strcmp(s, "trace") == 0)
    {
        return la_trace;
    }
    else if (strcmp(s, "trace msg") == 0)
    {
        return la_trace_msg;
    }
    else if (strcmp(s, "trace map") == 0)
    {
        return la_trace_map;
    }
    else if (strcmp(s, "recursion fix") == 0)
    {
        return la_rec_fix;
    }
    else if (strcmp(s, "recursion param") == 0)
    {
        return la_rec_param;
    }
    else if (strcmp(s, "recursion arg") == 0)
    {
        return la_rec_arg;
    }
    else if (strcmp(s, "recursion base") == 0)
    {
        return la_rec_base;
    }
    else if (strcmp(s, "function call") == 0)
    {
        return la_fun_call;
    }
    else if (strcmp(s, "function call parameter") == 0)
    {
        return la_fun_param;
    }
    else if (strcmp(s, "proxy") == 0)
    {
        return la_proxy;
    }
    else if (strcmp(s, "proxy base") == 0)
    {
        return la_proxy_base;
    }
    //  else if (strcmp(s, "???") == 0) {
    //      return la_cross_mvd 
    // }
    else if (strcmp(s, "eqjoin_unq") == 0)
    {
        return la_eqjoin_unq;
    }
    else if (strcmp(s, "fn:string-join") == 0)
    {
        return la_string_join;
    }
    else if (strcmp(s, "dummy") == 0)
    {
        return la_dummy;
    }
    else
    {

        PFoops (OOPS_FATAL, "unknown operator kind (%s)", s);
        /* pacify picky compilers */
        return la_serialize_seq;

    }



}


/******************************************************************************/
/******************************************************************************/

PFalg_att_t 
PFxml2la_conv_2PFLA_attributeName(const char* s)
{


    if (strcmp(s, "(NULL)" ) == 0)
    {
        return att_NULL;
    }
    else if (strcmp(s, "iter"   ) == 0)
    {
        return att_iter;
    }
    else if (strcmp(s, "item"   ) == 0)
    {
        return att_item;
    }
    else if (strcmp(s, "pos"    ) == 0)
    {
        return att_pos;
    }
    else if (strcmp(s, "iter1"  ) == 0)
    {
        return att_iter1;
    }
    else if (strcmp(s, "item1"  ) == 0)
    {
        return att_item1;    
    }
    else if (strcmp(s, "pos1"   ) == 0)
    {
        return att_pos1;
    }
    else if (strcmp(s, "inner"  ) == 0)
    {
        return att_inner;
    }
    else if (strcmp(s, "outer"  ) == 0)
    {
        return att_outer;
    }
    else if (strcmp(s, "sort"   ) == 0)
    {
        return att_sort;
    }
    else if (strcmp(s, "sort1"  ) == 0)
    {
        return att_sort1;
    }
    else if (strcmp(s, "sort2"  ) == 0)
    {
        return att_sort2;
    }
    else if (strcmp(s, "sort3"  ) == 0)
    {
        return att_sort3;
    }
    else if (strcmp(s, "sort4"  ) == 0)
    {
        return att_sort4;
    }
    else if (strcmp(s, "sort5"  ) == 0)
    {
        return att_sort5;
    }
    else if (strcmp(s, "sort6"  ) == 0)
    {
        return att_sort6;
    }
    else if (strcmp(s, "sort7"  ) == 0)
    {
        return att_sort7;
    }
    else if (strcmp(s, "ord"    ) == 0)
    {
        return att_ord;
    }
    else if (strcmp(s, "iter2"  ) == 0)
    {
        return att_iter2;
    }
    else if (strcmp(s, "iter3"  ) == 0)
    {
        return att_iter3;
    }
    else if (strcmp(s, "iter4"  ) == 0)
    {
        return att_iter4;
    }
    else if (strcmp(s, "iter5"  ) == 0)
    {
        return att_iter5;
    }
    else if (strcmp(s, "iter6"  ) == 0)
    {
        return att_iter6;
    }
    else if (strcmp(s, "res"    ) == 0)
    {
        return att_res;
    }
    else if (strcmp(s, "res1"   ) == 0)
    {
        return att_res1;
    }
    else if (strcmp(s, "cast"   ) == 0)
    {
        return att_cast;
    }
    else if (strcmp(s, "item2"  ) == 0)
    {
        return att_item2;
    }
    else if (strcmp(s, "item3"  ) == 0)
    {
        return att_subty;
    }
    else if (strcmp(s, "item4"  ) == 0)
    {
        return att_itemty;
    }
    else if (strcmp(s, "item5"  ) == 0)
    {
        return att_notsub;
    }
    else if (strcmp(s, "item6"  ) == 0)
    {
        return att_isint;
    }
    else if (strcmp(s, "item7"  ) == 0)
    {
        return att_isdec;
    }
    else
    {

        PFoops (OOPS_FATAL, "unknown attribute name (%s)", s);
        return -1; /* pacify picky compilers */

    }
}

/******************************************************************************/
/******************************************************************************/

PFalg_att_t 
PFxml2la_conv_2PFLA_attributeName_unq(const char* s)
{



   
    PFalg_att_t ori;
    int id;

    if (PFstrUtils_beginsWith(s, "iter"))
    {

        ori = att_iter;
       if(strcmp(s, "iter"  ) == 0)
       {
           id = 0;
       }
       else
       {
           char* idString = PFstrUtils_substring (
               s + strlen("iter"), s + strlen(s));
           id = atoi(idString);
       }
    }
    else if (PFstrUtils_beginsWith(s, "pos"))
    {
       ori = att_pos;
       if(strcmp(s, "pos"  ) == 0)
       {
           id = 0;
       }
       else
       {
           char* idString = PFstrUtils_substring (
               s + strlen("pos"), s + strlen(s));
           id = atoi(idString);
       }

    }
    else if (PFstrUtils_beginsWith(s, "item"))
    {
       ori = att_item;
       if(strcmp(s, "item"  ) == 0)
       {
           id = 0;
       }
       else
       {
           char* idString = 
               PFstrUtils_substring (s + strlen("item"), s + strlen(s));
           id = atoi(idString);
       }

    }
    else {


        PFoops (OOPS_FATAL, "don't know what to do with (%s)", s);
        
    }


    return PFalg_unq_name(ori, id);
}


/******************************************************************************/
/******************************************************************************/


/**
 * convert simple type name string to PFalg_simple_type_t
 */
PFalg_simple_type_t 
PFxml2la_conv_2PFLA_atomType(char* typeString)
{


    if (strcmp(typeString, "nat") == 0)
    {
        return aat_nat;
    }
    else if (strcmp(typeString, "int") == 0)
    {
        return aat_int;
    }
    else if (strcmp(typeString, "str") == 0)
    {
        return aat_str;
    }
    else if (strcmp(typeString, "dec") == 0)
    {
        return aat_dec;
    }
    else if (strcmp(typeString, "dbl") == 0)
    {
        return aat_dbl;
    }
    else if (strcmp(typeString, "bool") == 0)
    {
        return aat_bln;
    }
    else if (strcmp(typeString, "uA") == 0)
    {
        return aat_uA;
    }
    else if (strcmp(typeString, "qname") == 0)
    {
        return aat_qname;
    }
    else if (strcmp(typeString, "node")== 0)
    {
        return aat_node;
    }
    else if (strcmp(typeString, "attr")== 0)
    {
        return aat_anode;
    }
    else if (strcmp(typeString, "attrID") == 0)
    {
        return aat_attr;
    }
    else if (strcmp(typeString, "afrag") == 0)
    {
        return aat_afrag;
    }
    else if (strcmp(typeString, "pnode") == 0)
    {
        return aat_pnode;
    }
    else if (strcmp(typeString, "pre") == 0)
    {
        return aat_pre;
    }
    else if (strcmp(typeString, "pfrag") == 0)
    {
        return aat_pfrag;
    }
    else
    {

        PFoops (OOPS_FATAL, "unknown attribute simple type (%s)", typeString);
        return -1; /* pacify picky compilers */

    }
}




/******************************************************************************/
/******************************************************************************/



PFalg_atom_t 
PFxml2la_conv_2PFLA_atom(char* typeString, char* valueString)
{

    PFalg_simple_type_t type = PFxml2la_conv_2PFLA_atomType(typeString);


    switch (type)
    {
    case aat_nat:   
        {

            return lit_nat(PFxml2la_conv_2PFLA_atomValue_nat(valueString));
            
        }
    case aat_int:    
        {
            return lit_int(PFxml2la_conv_2PFLA_atomValue_int(valueString));
            
        }
    case aat_str:    
        {
            return lit_str(PFxml2la_conv_2PFLA_atomValue_str(valueString));
            
        }
    case aat_dec:    
        {
            return lit_dec(PFxml2la_conv_2PFLA_atomValue_dec(valueString));
            
        }
    case aat_dbl:    
        {
            return lit_dbl(PFxml2la_conv_2PFLA_atomValue_dbl(valueString));
            
        }
    case aat_bln:    
        {
            return lit_bln(PFxml2la_conv_2PFLA_atomValue_bln(valueString));
        }
    case aat_uA:      
        {
            return lit_str(PFxml2la_conv_2PFLA_atomValue_str(valueString));
            
        }
    case aat_qname:  //todo: main: pfqname init() aufrufen
        {

            return lit_qname(PFxml2la_conv_2PFLA_atomValue_int(valueString));
            
        }
    case aat_node:   
    case aat_anode:   
    case aat_attr:   
    case aat_afrag:  
    case aat_pnode:  
    case aat_pre:    
    case aat_pfrag:  
    case aat_charseq:
    default:       
        {
            PFoops (OOPS_FATAL, "don't know what to do (%s, %s)", typeString, valueString);
            /* pacify picky compilers */
            return (PFalg_atom_t) { .type = 0, .val = { .nat_ = 0 } }; 

        }



    }

}


/******************************************************************************/
/******************************************************************************/



PFalg_comp_t 
PFxml2la_conv_2PFLA_comparisonType(char* s)
{

    

    if (strcmp(s, "eq") == 0)
    {
        return alg_comp_eq;
    }
    else if (strcmp(s, "gt") == 0)
    {
        return alg_comp_gt;
    }
    else if (strcmp(s, "ge") == 0)
    {
        return alg_comp_ge;
    }
    else if (strcmp(s, "lt") == 0)
    {
        return alg_comp_lt;
    }
    else if (strcmp(s, "le") == 0)
    {
        return alg_comp_le;
    }
    else if (strcmp(s, "ne") == 0)
    {
        return alg_comp_ne;
    }
    else
    {
        PFoops (OOPS_FATAL, "don't know what to do (%s)", s);
        /* pacify picky compilers */
        return alg_comp_eq; 


    }
}


/******************************************************************************/
/******************************************************************************/


PFalg_fun_t 
PFxml2la_conv_2PFLA_functionType(char* s)
{

    if (strcmp(s, "add") == 0)
    {
        return alg_fun_num_add;
    }
    else if (strcmp(s, "subtract") == 0)
    {
        return alg_fun_num_subtract;
    }
    else if (strcmp(s, "multiply") == 0)
    {
        return alg_fun_num_multiply;
    }
    else if (strcmp(s, "divide") == 0)
    {
        return alg_fun_num_divide;
    }
    else if (strcmp(s, "modulo") == 0)
    {
        return alg_fun_num_modulo;
    }
    else if (strcmp(s, "fn:abs") == 0)
    {
        return alg_fun_fn_abs;
    }
    else if (strcmp(s, "fn:ceiling") == 0)
    {
        return alg_fun_fn_ceiling;
    }
    else if (strcmp(s, "fn:floor") == 0)
    {
        return alg_fun_fn_floor;
    }
    else if (strcmp(s, "fn:round") == 0)
    {
        return alg_fun_fn_round;
    }
    else if (strcmp(s, "fn:concat") == 0)
    {
        return alg_fun_fn_concat;
    }
    else if (strcmp(s, "fn:contains") == 0)
    {
        return alg_fun_fn_contains;
    }
    else if (strcmp(s, "fn:number") == 0)
    {
        return alg_fun_fn_number;
    }


    else
    {
        PFoops (OOPS_FATAL, "don't know what to do (%s)", s);
        /* pacify picky compilers */
        return alg_fun_num_add; 

    }
}


/******************************************************************************/
/******************************************************************************/
bool 
PFxml2la_conv_2PFLA_orderingDirection(char* s) 
{
    if (strcmp(s, "ascending") == 0)
    {
        return DIR_ASC;
    }
    else if (strcmp(s, "descending") == 0)
    {
        return DIR_DESC;
    }
    else
    {
        PFoops (OOPS_FATAL, "don't know what to do (%s)", s);
        /* pacify picky compilers */
        return DIR_ASC;
    }

}


/******************************************************************************/
/******************************************************************************/
PFalg_axis_t 
PFxml2la_conv_2PFLA_xpathaxis(char* s) 
{
    if (strcmp(s, "ancestor::") == 0)
    {
        return alg_anc;
    }
    else if (strcmp(s, "anc-or-self::") == 0)
    {
        return alg_anc_s;
    }
    else if (strcmp(s, "attribute::") == 0)
    {
        return alg_attr;
    }
    else if (strcmp(s, "child::") == 0)
    {
        return alg_chld;
    }
    else if (strcmp(s, "descendant::") == 0)
    {
        return alg_desc;
    }
    else if (strcmp(s, "desc-or-self::") == 0)
    {
        return alg_desc_s;
    }
    else if (strcmp(s, "following::") == 0)
    {
        return alg_fol;
    }
    else if (strcmp(s, "fol-sibling::") == 0)
    {
        return alg_fol_s;
    }
    else if (strcmp(s, "parent::") == 0)
    {
        return alg_par;
    }
    else if (strcmp(s, "preceding::") == 0)
    {
        return alg_prec;
    }
    else if (strcmp(s, "prec-sibling::") == 0)
    {
        return alg_prec_s;
    }
    else if (strcmp(s, "self::") == 0)
    {
        return alg_self;
    }
    else
    {
        PFoops (OOPS_FATAL, "don't know what to do (%s)", s);
        /* pacify picky compilers */
        return alg_anc;

    }

}






/******************************************************************************/
/******************************************************************************/

/*todo: implement me */
PFty_t 
PFxml2la_conv_2PFLA_sequenceType(char* s)
{


    PFty_t type = PFty_none();

    


    if (PFstrUtils_beginsWith(s, "item"))
    {
        type.type = ty_item;

    }
    else if (PFstrUtils_beginsWith(s, "node"))
    {

    }
    else if (PFstrUtils_beginsWith(s, "element"))
    {

    }
    else if (PFstrUtils_beginsWith(s, "attribute"))
    {
    }
    else if (PFstrUtils_beginsWith(s, "text"))
    {

    }


    else
    {
        PFoops (OOPS_FATAL, "don't know what to do (%s)", s);
        /* pacify picky compilers */
        return type;


    }


    return type;

}


/******************************************************************************/
/******************************************************************************/

PFalg_doc_t 
PFxml2la_conv_2PFLA_docType(char* s) 
{
    if (strcmp(s, "doc.attribute") == 0)
    {
        return doc_atext;
    }
    else if (strcmp(s, "doc.textnode") == 0)
    {
        return doc_text;
    }
    else if (strcmp(s, "doc.comment") == 0)
    {
        return doc_comm;
    }
    else if (strcmp(s, "doc.pi") == 0)
    {
        return doc_pi_text;
    }
    else
    {
        PFoops (OOPS_FATAL, "don't know what to do (%s)", s);
        /* pacify picky compilers */
        return doc_atext;


    }

}







