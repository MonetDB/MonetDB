/**
 * @file
 *
 * Serialize MIL tree.
 *
 * Serialization is done with the help of a simplified MIL grammar:
 *
 * @verbatim
 
   statements    : statements statements                    <m_seq>
                 | 'if (' Expr ') {' stmts '} else {' stmts '}' <m_if>
                 | 'while (' Expr ') {' stmts '}'           <m_while>
                 | <nothing>                                <m_nop>
                 | '#' c                                    <m_comment> 
                 | statement ';'                            <otherwise>

   statement     : Variable ':=' expression                 <m_assgn>
                 | 'module (' literal ')'                   <m_module>
                 | Variable ':= CATCH ({' statements '})'   <m_catch>
                 | expr '.insert (' expr ',' expr ')'       <m_insert>
                 | expression '.append (' expression ')'    <m_bappend>
                 | expression '.access (' restriction ')'   <m_access>
                 | 'serialize (...)'                        <m_serialize>
                 | 'var' Variable                           <m_declare>
                 | 'ERROR (' expression ')'                 <m_error>
                 | 'print (' args ')'                       <m_print>
                 | 'col_name (' expr ',' expr ')'           <m_col_name>
                 | 'destroy_ws (' expression ')'            <m_destroy_ws>

   expression    : Variable                                 <m_var>
                 | literal                                  <m_lit_*, m_nil>
                 | 'new (' Type ',' Type ')'                <m_new>
                 | expression '.seqbase (' expression ')'   <m_seqbase>
                 | expression '.select (' expression ')'    <m_select>
                 | expression '.exist (' expression ')'     <m_exist>
                 | expression '.project (' expression ')'   <m_project>
                 | expression '.mark (' expression ')'      <m_mark>
                 | expression '.mark_grp (' expression ')'  <m_mark_grp>
                 | expression '.cross (' expression ')'     <m_cross>
                 | expression '.join (' expression ')'      <m_join>
                 | expression '.leftjoin (' expression ')'  <m_leftjoin>
                 | expression '.kunion (' expression ')'    <m_kunion>
                 | expression '.kdiff (' expression ')'     <m_kdiff>
                 | expression '.sintersect (' expression ')'<m_sintersect>
                 | expression '.CTrefine (' expression ')'  <m_ctrefine>
                 | expression '.CTderive (' expression ')'  <m_ctderive>
                 | expression '.insert (' expression ')'    <m_binsert>
                 | expression '.append (' expression ')'    <m_bappend>
                 | expression '.fetch (' expression ')'     <m_fetch>
                 | expression '.set_kind (' expression ')'  <m_set_kind>
                 | expression '.kunique ()'                 <m_kunique>
                 | expression '.tunique ()'                 <m_tunique>
                 | expression '.reverse ()'                 <m_reverse>
                 | expression '.mirror ()'                  <m_mirror>
                 | expression '.copy ()'                    <m_copy>
                 | expression '.sort ()'                    <m_sort>
                 | expression '.count ()'                   <m_count>
                 | expression '.avg ()'                     <m_avg>
                 | expression '.max ()'                     <m_max>
                 | expression '.min ()'                     <m_min>
                 | expression '.sum ()'                     <m_sum>
                 | expression 'bat ()'                      <m_bat>
                 | expression '.CTgroup ()'                 <m_ctgroup>
                 | expression '.CTmap ()'                   <m_ctmap>
                 | expression '.CTextend ()'                <m_ctextend>
                 | expression '.get_fragment ()'            <m_get_fragment>
                 | expression '.materialize (' exp ')'      <m_materialize>
                 | expression '.assert_order ()'            <m_assert_order>
                 | expression '.chk_order ()'               <m_chk_order>
                 | expression '.access (' restriction ')'   <m_access>
                 | expression '.key (' bool ')'             <m_key>
                 | expr '.insert (' expr ',' expr ')'       <m_insert>
                 | expr '.select (' expr ',' expr ')'       <m_select2>
                 | Type '(' expression ')'                  <m_cast>
                 | '[' Type '](' expression ')'             <m_mcast>
                 | '+(' expression ',' expression ')'       <m_add>
                 | '[+](' expression ',' expression ')'     <m_madd>
                 | '-(' expression ',' expression ')'       <m_sub>
                 | '[-](' expression ',' expression ')'     <m_msub>
                 | '[*](' expression ',' expression ')'     <m_mmult>
                 | '/(' expression ',' expression ')'       <m_div>
                 | '[/](' expression ',' expression ')'     <m_mdiv>
                 | '[%](' expression ',' expression ')'     <m_mmod>
                 | '>(' expression ',' expression ')'       <m_gt>
                 | '[>](' expression ',' expression ')'     <m_mgt>
                 | '[=](' expression ',' expression ')'     <m_meq>
                 | 'not(' expression ')'                    <m_not>
                 | '[not](' expression ')'                  <m_mnot>
                 | '[-](' expression ')'                    <m_mneg>
                 | 'isnil(' expression ')'                  <m_isnil>
                 | '[isnil](' expression ')'                <m_misnil>
                 | '[and](' expression ',' expression ')'   <m_mand>
                 | '[or](' expression ',' expression ')'    <m_mor>
                 | '[ifthenelse](' exp ',' exp ',' exp ')'  <m_ifthenelse>
                 | '[search](' expression ',' expression ')'<m_msearch>
                 | '[string](' expression ',' expression ')'<m_mstring>
                 | '[string](' exp ',' exp ',' exp ')'      <m_mstring2>
                 | '{count}(' expression ')'                <m_gcount>
                 | '{count}(' expression ',' expression ')' <m_egcount>
                 | '{avg}(' expression ')'                  <m_gavg>
                 | '{max}(' expression ')'                  <m_gmax>
                 | '{min}(' expression ')'                  <m_gmin>
                 | '{sum}(' expression ')'                  <m_gsum>
                 | 'usec ()'                                <m_usec>
                 | 'new_ws ()'                              <m_new_ws>
                 | 'mposjoin (' exp ',' exp ',' exp ')'     <m_mposjoin>
                 | 'mvaljoin (' exp ',' exp ',' exp ')'     <m_mvaljoin>
                 | 'doc_tbl (' expr ',' expr ')'            <m_doc_tbl>
                 | 'attr_constr (' exp ',' exp ',' exp ')'  <m_attr_constr>
                 | 'elem_constr (' e ',' e ',' e ',' e ','
                                   e ',' e ',' e ',' e ')'  <m_elem_constr>
                 | 'elem_constr_empty (' expr ',' expr ')'  <m_elem_constr_e>
                 | 'text_constr (' expr ',' expr ')'        <m_text_constr>
                 | 'add_qname (' ex ',' ex ',' ex ',' ex ')'<m_add_qname>
                 | 'add_qnames(' ex ',' ex ',' ex ',' ex ')'<m_add_qnames>
                 | 'check_qnames (' expression ')'          <m_chk_qnames>
                 | 'sc_desc (' ex ',' ex ',' ex ',' ex ')'  <m_sc_desc>
                 | 'merged_adjacent_text_nodes
                             (' ex ',' ex ',' ex ',' ex ')' <m_merge_adjacent>
                 | 'string_join (' expr ',' expr ')'        <m_string_join>
                 | 'merged_union (' args ')'                <m_merged_union>

                 | 'llscj_child (' a ',' b ',' c ',' d ')'  <m_llscj_child>
                 | ... more staircase join variants ...

   args          : args ',' args                            <m_arg>
                 | expression                               <otherwise>

   literal       : IntegerLiteral                           <m_lit_int>
                 | LongIntegerLiteral                       <m_lit_lng>
                 | StringLiteral                            <m_lit_str>
                 | OidLiteral                               <m_lit_oid>
                 | MinLiteral                               <m_lit_min>
                 | MaxLiteral                               <m_lit_max>
                 | 'nil'                                    <m_nil>
@endverbatim
 *
 * Grammar rules are reflected by @c print_* functions in this file.
 * Depending on the current MIL tree node kind (see enum values
 * in brackets above), the corresponding sub-rule is chosen (i.e.
 * the corresponding sub-routine is called).
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
 * 2000-2005 University of Konstanz and (C) 2005-2006 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */
#include <stdio.h>
#include <assert.h>

#include "pathfinder.h"
#include "milprint.h"

#include "oops.h"
#include "pfstrings.h"

static char *ID[] = {

      [m_new]          = "new"
    , [m_seqbase]      = "seqbase"
    , [m_key]          = "key"
    , [m_order]        = "order"
    , [m_select]       = "select"
    , [m_uselect]      = "ord_uselect"
    , [m_select2]      = "select"
    , [m_exist]        = "exist"
    , [m_insert]       = "insert"
    , [m_binsert]      = "insert"
    , [m_bappend]      = "append"
    , [m_fetch]        = "fetch"
    , [m_project]      = "project"
    , [m_mark]         = "mark"
    , [m_mark_grp]     = "mark_grp"
    , [m_access]       = "access"
    , [m_cross]        = "cross"
    , [m_join]         = "join"
    , [m_leftjoin]     = "leftjoin"
    , [m_reverse]      = "reverse"
    , [m_mirror]       = "mirror"
    , [m_copy]         = "copy"
    , [m_kunique]      = "kunique"
    , [m_tunique]      = "tunique"
    , [m_kunion]       = "kunion"
    , [m_kdiff]        = "kdiff"
    , [m_sintersect]   = "sintersect"
    , [m_merged_union] = "merged_union"
    , [m_var]          = "var"

    , [m_sort]         = "sort"
    , [m_ctgroup]      = "CTgroup"
    , [m_ctmap]        = "CTmap"
    , [m_ctextend]     = "CTextend"
    , [m_ctrefine]     = "CTrefine"
    , [m_ctderive]     = "CTderive"

    , [m_add]          = "+"
    , [m_madd]         = "[+]"
    , [m_sub]          = "-"
    , [m_msub]         = "[-]"
    , [m_mmult]        = "[*]"
    , [m_div]          = "/"
    , [m_mdiv]         = "[/]"
    , [m_mmod]         = "[%]"
    , [m_gt]           = ">"
    , [m_mgt]          = "[>]"
    , [m_meq]          = "[=]"
    , [m_not]          = "not"
    , [m_mnot]         = "[not]"
    , [m_mneg]         = "[-]"
    , [m_mand]         = "[and]"
    , [m_mor]          = "[or]"
    , [m_mifthenelse]  = "[ifthenelse]"
    , [m_msearch]      = "[search]"
    , [m_mstring]      = "[string]"
    , [m_mstring2]     = "[string]"
    , [m_isnil]        = "isnil"
    , [m_misnil]       = "[isnil]"
    , [m_usec]         = "usec"
    , [m_new_ws]       = "ws_create"
    , [m_destroy_ws]   = "ws_destroy"
    , [m_mposjoin]     = "mposjoin"
    , [m_mvaljoin]     = "mvaljoin"
    , [m_doc_tbl]      = "doc_tbl"
    , [m_attr_constr]  = "attr_constr"
    , [m_elem_constr]  = "elem_constr"
    , [m_elem_constr_e]= "elem_constr_empty"
    , [m_text_constr]  = "text_constr"
    , [m_add_qname]    = "add_qname"
    , [m_add_qnames]   = "add_qnames"
    , [m_chk_qnames]   = "invalid_qname"

    , [m_llscj_anc]              = "loop_lifted_ancestor_step"
    , [m_llscj_anc_elem]         = "loop_lifted_ancestor_step_with_kind_test"
    , [m_llscj_anc_text]         = "loop_lifted_ancestor_step_with_kind_test"
    , [m_llscj_anc_comm]         = "loop_lifted_ancestor_step_with_kind_test"
    , [m_llscj_anc_pi]           = "loop_lifted_ancestor_step_with_kind_test"
    , [m_llscj_anc_elem_nsloc]   = "loop_lifted_ancestor_step_with_nsloc_test"
    , [m_llscj_anc_elem_loc]     = "loop_lifted_ancestor_step_with_loc_test"
    , [m_llscj_anc_elem_ns]      = "loop_lifted_ancestor_step_with_ns_test"
    , [m_llscj_anc_pi_targ]      = "loop_lifted_ancestor_step_with_target_test"

    , [m_llscj_anc_self]         
        = "loop_lifted_ancestor_or_self_step"
    , [m_llscj_anc_self_elem]
        = "loop_lifted_ancestor_or_self_step_with_kind_test"
    , [m_llscj_anc_self_text]
        = "loop_lifted_ancestor_or_self_step_with_kind_test"
    , [m_llscj_anc_self_comm]
        = "loop_lifted_ancestor_or_self_step_with_kind_test"
    , [m_llscj_anc_self_pi]
        = "loop_lifted_ancestor_or_self_step_with_kind_test"
    , [m_llscj_anc_self_elem_nsloc]
        = "loop_lifted_ancestor_or_self_step_with_nsloc_test"
    , [m_llscj_anc_self_elem_loc]
        = "loop_lifted_ancestor_or_self_step_with_loc_test"
    , [m_llscj_anc_self_elem_ns]
        = "loop_lifted_ancestor_or_self_step_with_ns_test"
    , [m_llscj_anc_self_pi_targ]
        = "loop_lifted_ancestor_or_self_step_with_target_test"

    , [m_llscj_child]            = "loop_lifted_child_step"
    , [m_llscj_child_elem]       = "loop_lifted_child_step_with_kind_test"
    , [m_llscj_child_text]       = "loop_lifted_child_step_with_kind_test"
    , [m_llscj_child_comm]       = "loop_lifted_child_step_with_kind_test"
    , [m_llscj_child_pi]         = "loop_lifted_child_step_with_kind_test"
    , [m_llscj_child_elem_nsloc] = "loop_lifted_child_step_with_nsloc_test"
    , [m_llscj_child_elem_loc]   = "loop_lifted_child_step_with_loc_test"
    , [m_llscj_child_elem_ns]    = "loop_lifted_child_step_with_ns_test"
    , [m_llscj_child_pi_targ]    = "loop_lifted_child_step_with_target_test"

    , [m_llscj_desc]            = "loop_lifted_descendant_step"
    , [m_llscj_desc_elem]       = "loop_lifted_descendant_step_with_kind_test"
    , [m_llscj_desc_text]       = "loop_lifted_descendant_step_with_kind_test"
    , [m_llscj_desc_comm]       = "loop_lifted_descendant_step_with_kind_test"
    , [m_llscj_desc_pi]         = "loop_lifted_descendant_step_with_kind_test"
    , [m_llscj_desc_elem_nsloc] = "loop_lifted_descendant_step_with_nsloc_test"
    , [m_llscj_desc_elem_loc]   = "loop_lifted_descendant_step_with_loc_test"
    , [m_llscj_desc_elem_ns]    = "loop_lifted_descendant_step_with_ns_test"
    , [m_llscj_desc_pi_targ]    = "loop_lifted_descendant_step_with_target_test"

    , [m_llscj_desc_self]
        = "loop_lifted_descendant_or_self_step"
    , [m_llscj_desc_self_elem]
        = "loop_lifted_descendant_or_self_step_with_kind_test"
    , [m_llscj_desc_self_text]
        = "loop_lifted_descendant_or_self_step_with_kind_test"
    , [m_llscj_desc_self_comm]
        = "loop_lifted_descendant_or_self_step_with_kind_test"
    , [m_llscj_desc_self_pi]
        = "loop_lifted_descendant_or_self_step_with_kind_test"
    , [m_llscj_desc_self_elem_nsloc]
        = "loop_lifted_descendant_or_self_step_with_nsloc_test"
    , [m_llscj_desc_self_elem_loc]
        = "loop_lifted_descendant_or_self_step_with_loc_test"
    , [m_llscj_desc_self_elem_ns]
        = "loop_lifted_descendant_or_self_step_with_ns_test"
    , [m_llscj_desc_self_pi_targ]
        = "loop_lifted_descendant_or_self_step_with_target_test"

    , [m_llscj_foll]            = "loop_lifted_following_step"
    , [m_llscj_foll_elem]       = "loop_lifted_following_step_with_kind_test"
    , [m_llscj_foll_text]       = "loop_lifted_following_step_with_kind_test"
    , [m_llscj_foll_comm]       = "loop_lifted_following_step_with_kind_test"
    , [m_llscj_foll_pi]         = "loop_lifted_following_step_with_kind_test"
    , [m_llscj_foll_elem_nsloc] = "loop_lifted_following_step_with_nsloc_test"
    , [m_llscj_foll_elem_loc]   = "loop_lifted_following_step_with_loc_test"
    , [m_llscj_foll_elem_ns]    = "loop_lifted_following_step_with_ns_test"
    , [m_llscj_foll_pi_targ]    = "loop_lifted_following_step_with_target_test"

    , [m_llscj_foll_sibl]
        = "loop_lifted_following_sibling_step"
    , [m_llscj_foll_sibl_elem]
        = "loop_lifted_following_sibling_step_with_kind_test"
    , [m_llscj_foll_sibl_text]
        = "loop_lifted_following_sibling_step_with_kind_test"
    , [m_llscj_foll_sibl_comm]
        = "loop_lifted_following_sibling_step_with_kind_test"
    , [m_llscj_foll_sibl_pi]
        = "loop_lifted_following_sibling_step_with_kind_test"
    , [m_llscj_foll_sibl_elem_nsloc]
        = "loop_lifted_following_sibling_step_with_nsloc_test"
    , [m_llscj_foll_sibl_elem_loc]
        = "loop_lifted_following_sibling_step_with_loc_test"
    , [m_llscj_foll_sibl_elem_ns]
        = "loop_lifted_following_sibling_step_with_ns_test"
    , [m_llscj_foll_sibl_pi_targ]
        = "loop_lifted_following_sibling_step_with_target_test"

    , [m_llscj_parent]            = "loop_lifted_parent_step"
    , [m_llscj_parent_elem]       = "loop_lifted_parent_step_with_kind_test"
    , [m_llscj_parent_text]       = "loop_lifted_parent_step_with_kind_test"
    , [m_llscj_parent_comm]       = "loop_lifted_parent_step_with_kind_test"
    , [m_llscj_parent_pi]         = "loop_lifted_parent_step_with_kind_test"
    , [m_llscj_parent_elem_nsloc] = "loop_lifted_parent_step_with_nsloc_test"
    , [m_llscj_parent_elem_loc]   = "loop_lifted_parent_step_with_loc_test"
    , [m_llscj_parent_elem_ns]    = "loop_lifted_parent_step_with_ns_test"
    , [m_llscj_parent_pi_targ]    = "loop_lifted_parent_step_with_target_test"

    , [m_llscj_prec]            = "loop_lifted_preceding_step"
    , [m_llscj_prec_elem]       = "loop_lifted_preceding_step_with_kind_test"
    , [m_llscj_prec_text]       = "loop_lifted_preceding_step_with_kind_test"
    , [m_llscj_prec_comm]       = "loop_lifted_preceding_step_with_kind_test"
    , [m_llscj_prec_pi]         = "loop_lifted_preceding_step_with_kind_test"
    , [m_llscj_prec_elem_nsloc] = "loop_lifted_preceding_step_with_nsloc_test"
    , [m_llscj_prec_elem_loc]   = "loop_lifted_preceding_step_with_loc_test"
    , [m_llscj_prec_elem_ns]    = "loop_lifted_preceding_step_with_ns_test"
    , [m_llscj_prec_pi_targ]    = "loop_lifted_preceding_step_with_target_test"

    , [m_llscj_prec_sibl]
        = "loop_lifted_preceding_sibling_step"
    , [m_llscj_prec_sibl_elem]
        = "loop_lifted_preceding_sibling_step_with_kind_test"
    , [m_llscj_prec_sibl_text]
        = "loop_lifted_preceding_sibling_step_with_kind_test"
    , [m_llscj_prec_sibl_comm]
        = "loop_lifted_preceding_sibling_step_with_kind_test"
    , [m_llscj_prec_sibl_pi]
        = "loop_lifted_preceding_sibling_step_with_kind_test"
    , [m_llscj_prec_sibl_elem_nsloc]
        = "loop_lifted_preceding_sibling_step_with_nsloc_test"
    , [m_llscj_prec_sibl_elem_loc]
        = "loop_lifted_preceding_sibling_step_with_loc_test"
    , [m_llscj_prec_sibl_elem_ns]
        = "loop_lifted_preceding_sibling_step_with_ns_test"
    , [m_llscj_prec_sibl_pi_targ]
        = "loop_lifted_preceding_sibling_step_with_target_test"

    , [m_merge_adjacent]   = "merge_adjacent_text_nodes"
    , [m_string_join]      = "string_join"

    , [m_get_fragment]    = "get_fragment"
    , [m_set_kind]        = "set_kind"
    , [m_materialize]     = "materialize"
    , [m_assert_order]    = "assert_order"
    , [m_chk_order]       = "chk_order"

    , [m_sc_desc]  = "sc_desc"

    , [m_count]    = "count"
    , [m_gcount]   = "{count}"
    , [m_egcount]  = "{count}"
    , [m_avg]      = "avg"
    , [m_gavg]     = "{avg}"
    , [m_max]      = "max"
    , [m_gmax]     = "{max}"
    , [m_min]      = "min"
    , [m_gmin]     = "{min}"
    , [m_sum]      = "sum"
    , [m_gsum]     = "{sum}"
    , [m_bat]      = "bat"
    , [m_catch]    = "CATCH"
    , [m_error]    = "ERROR"
    , [m_col_name] = "col_name"

};

/** The string we print to */
static PFarray_t *out = NULL;

/* Wrapper to print stuff */
static void milprintf (char *, ...)
    __attribute__ ((format (printf, 1, 2)));

/* forward declarations for left sides of grammar rules */
static void print_statements (PFmil_t *);
static void print_statement (PFmil_t *);
static void print_variable (PFmil_t *);
static void print_expression (PFmil_t *);
static void print_min (PFmil_t *);
static void print_max (PFmil_t *);
static void print_literal (PFmil_t *);
static void print_type (PFmil_t *);
static void print_args (PFmil_t *);

#ifdef NDEBUG
#define debug_output
#else
/**
 * In our debug versions we want to have meaningful error messages when
 * generating MIL output failed. So we print the MIL script as far as
 * we already generated it.
 */
#define debug_output \
  PFinfo (OOPS_FATAL, "I encountered problems while generating MIL output."); \
  PFinfo (OOPS_FATAL, "This is possibly due to an illegal MIL tree, "         \
                      "not conforming to the grammar in milprint.c");         \
  PFinfo (OOPS_FATAL, "This is how far I was able to generate the script:");  \
  fprintf (stderr, "%s", (char *) out->base);
#endif

/**
 * Implementation of the grammar rules for `statements'.
 *
 * @param n MIL tree node
 */
static void
print_statements (PFmil_t * n)
{
    switch (n->kind) {

        /* statements : statements statements */
        case m_seq:
            print_statements (n->child[0]);
            print_statements (n->child[1]);
            break;

        case m_if:
            milprintf ("if (");
            print_expression (n->child[0]);
            milprintf (") {\n");
            print_statements (n->child[1]);
            milprintf ("} else {\n");
            print_statements (n->child[2]);
            milprintf ("}\n");
            break;

        case m_while:
            milprintf ("while (");
            print_expression (n->child[0]);
            milprintf (") {\n");
            print_statements (n->child[1]);        
            milprintf ("}\n");
            break;

        case m_nop:
            break;

        case m_comment:
	    milprintf ("# ");
	    /* FIXME: What if c contains \n's? */
	    milprintf ("%s\n", n->sem.s);
	    break;

        /* statements : statement ';' */
        default:
            print_statement (n);
            milprintf (";\n");
            break;
    }
}

/**
 * Implementation of the grammar rules for `statement'.
 *
 * @param n MIL tree node
 */
static void
print_statement (PFmil_t * n)
{
    switch (n->kind) {

        /* statement : 'module (' Literal ')' */
        case m_module:
            milprintf ("module (");
            print_literal (n->child[0]);
            milprintf (")");
            break;
            
        /* statement : variable ':=' expression */
        case m_assgn:
            print_variable (n->child[0]);
            milprintf (" := ");
            print_expression (n->child[1]);
            break;

        /* statement : variable ':= CATCH ({' statements '})' */
        case m_catch:
            print_variable (n->child[0]);
            milprintf (" := CATCH ({\n");
            print_statements (n->child[1]);
            milprintf ("})");
            break;

        /* statement : 'var' Variable */
        case m_declare:
            milprintf ("var ");
            print_variable (n->child[0]);
            break;

        /* expr '.insert (' expr ',' expr ')' */
        case m_insert:
            print_expression (n->child[0]);
            milprintf (".%s (", ID[n->kind]);
            print_expression (n->child[1]);
            milprintf (", ");
            print_expression (n->child[2]);
            milprintf (")");
            break;

        /* expression '.insert (' expression ')' */
        case m_binsert:
        /* expression '.append (' expression ')' */
        case m_bappend:
            print_expression (n->child[0]);
            milprintf (".%s (", ID[n->kind]);
            print_expression (n->child[1]);
            milprintf (")");
            break;

        /* expression '.access (' restriction ')' */
        case m_access:
            print_expression (n->child[0]);
            switch (n->sem.access) {
                case BAT_READ:   milprintf (".access (BAT_READ)"); break;
                case BAT_APPEND: milprintf (".access (BAT_APPEND)"); break;
                case BAT_WRITE:  milprintf (".access (BAT_WRITE)"); break;
            }
            break;

        case m_order:
            print_expression (n->child[0]);
            milprintf (".%s", ID[n->kind]);
            break;

        /* `nop' nodes (`no operation') may be produced during compilation */
        /*
        case m_nop:
            break;
        */

        /* statement: 'destroy_ws (' expression ')' */
        case m_destroy_ws:
        /* statement: 'ERROR(' expression ')' */
        case m_error:
            milprintf ("%s (", ID[n->kind]);
            print_args (n->child[0]);
            milprintf (")");
            break;

        /* statement: 'print (' expression ')' */
        case m_print:
            milprintf ("print (");
            print_args (n->child[0]);
            milprintf (")");
            break;

        case m_col_name:
            print_expression (n->child[0]);
            milprintf (".%s (", ID[n->kind]);
            print_expression (n->child[1]);
            milprintf (")");
            break;

        case m_serialize:
            milprintf ("print_result (");
            print_args (n->child[0]);
            milprintf (")");
            break;

        default:
            debug_output;     /* Print MIL code so far when in debug mode. */
#ifndef NDEBUG
            PFinfo (OOPS_NOTICE, "node: %s", ID[n->kind]);
#endif
            PFoops (OOPS_FATAL,
                    "Illegal MIL tree. MIL printer screwed up (kind: %u).",
                    n->kind);
    }
}

static void
print_args (PFmil_t *n)
{
    switch (n->kind) {

        case m_arg:   print_args (n->child[0]);
                      milprintf (", ");
                      print_args (n->child[1]);
                      break;

        default:      print_expression (n);
                      break;
    }
}

/**
 * Implementation of the grammar rules for `expression'.
 *
 * @param n MIL tree node
 */
static void
print_expression (PFmil_t * n)
{
    assert(n);
    switch (n->kind) {

        /* expression : Variable */
        case m_var:
            print_variable (n);
            break;

        /* expression : 'new (' Type ',' Type ')' */
        case m_new:
            milprintf ("new (");
            print_type (n->child[0]);
            milprintf (", ");
            print_type (n->child[1]);
            milprintf (")");
            break;

        /* expression : expression '.materialize (' expression ')' */
        case m_materialize:
        /* expression : expression '.seqbase (' expression ')' */
        case m_seqbase:
        /* expression : expression '.select (' expression ')' */
        case m_select:
        /* expression : expression '.ord_uselect (' expression ')' */
        case m_uselect:
        /* expression : expression '.exist (' expression ')' */
        case m_exist:
        /* expression : expression '.project (' expression ')' */
        case m_project:
        /* expression : expression '.mark (' expression ')' */
        case m_mark:
        /* expression : expression '.mark_grp (' expression ')' */
        case m_mark_grp:
        /* expression : expression '.cross (' expression ')' */
        case m_cross:
        /* expression : expression '.join (' expression ')' */
        case m_join:
        /* expression : expression '.leftjoin (' expression ')' */
        case m_leftjoin:
        /* expression : expression '.CTrefine (' expression ')' */
        case m_ctrefine:
        /* expression : expression '.CTderive (' expression ')' */
        case m_ctderive:
        /* expression : expression '.insert (' expression ')' */
        case m_binsert:
        /* expression : expression '.append (' expression ')' */
        case m_bappend:
        /* expression : expression '.fetch (' expression ')' */
        case m_fetch:
        /* expression : expression '.kunion (' expression ')' */
        case m_kunion:
        /* expression : expression '.kdiff (' expression ')' */
        case m_kdiff:
        /* expression : expression '.sintersect (' expression ')' */
        case m_sintersect:
        /* expression : expression '.set_kind (' expression ')' */
        case m_set_kind:
            print_expression (n->child[0]);
            milprintf (".%s (", ID[n->kind]);
            print_expression (n->child[1]);
            milprintf (")");
            break;

        /* expression : expression '.reverse' */
        case m_reverse:
        /* expression : expression '.mirror' */
        case m_mirror:
        /* expression : expression '.kunique' */
        case m_kunique:
        /* expression : expression '.tunique' */
        case m_tunique:
        /* expression : expression '.copy' */
        case m_copy:
        /* expression : expression '.sort' */
        case m_sort:
         /* expression : expression '.count' */
        case m_count:
        /* expression : expression '.avg' */
        case m_avg:
        /* expression : expression '.max' */
        case m_max:
	/* expression : expression '.min' */
        case m_min:
	/* expression : expression '.sum' */
        case m_sum:
        /* expression : expression 'bat()' */
        case m_bat:
        /* expression '.CTgroup ()' */
        case m_ctgroup:
        /* expression '.CTmap ()' */
        case m_ctmap:
        /* expression '.CTextend ()' */
        case m_ctextend:
        /* expression '.get_fragment ()' */
        case m_get_fragment:
        /* 'invalid_qname (' expression ')' */
        case m_chk_qnames:
        /* expression '.assert_order ()' */
        case m_assert_order:
        /* expression '.chk_order ()' */
        case m_chk_order:
            print_expression (n->child[0]);
            milprintf (".%s ()", ID[n->kind]);
            break;

        case m_access:
            print_expression (n->child[0]);
            switch (n->sem.access) {
                case BAT_READ:   milprintf (".access (BAT_READ)"); break;
                case BAT_APPEND: milprintf (".access (BAT_APPEND)"); break;
                case BAT_WRITE:  milprintf (".access (BAT_WRITE)"); break;
            }
            break;

        /* expression '.key (' bool ')' */
        case m_key:
            print_expression (n->child[0]);
            milprintf (".key (%s)", n->sem.b ? "true" : "false");
            break;

        /* expression : Type '(' expression ')' */
        case m_cast:
            print_type (n->child[0]);
            milprintf ("(");
            print_expression (n->child[1]);
            milprintf (")");
            break;

        /* expression : '[' Type '](' expression ')' */
        case m_mcast:
            milprintf ("[");
            print_type (n->child[0]);
            milprintf ("](");
            print_expression (n->child[1]);
            milprintf (")");
            break;

        /* expression : 'string_join(' exp ',' exp)' */
        case m_string_join:
        /* expression : '[search](' exp ',' exp)' */
        case m_msearch:
        /* expression : '[string](' exp ',' exp)' */
        case m_mstring:
        /* expression : '+(' expression ',' expression ')' */
        case m_add:
        /* expression : '[+](' expression ',' expression ')' */
        case m_madd:
        /* expression : '-(' expression ',' expression ')' */
        case m_sub:
        /* expression : '[-](' expression ',' expression ')' */
        case m_msub:
        /* expression : '[*](' expression ',' expression ')' */
        case m_mmult:
        /* expression : '/(' expression ',' expression ')' */
        case m_div:
        /* expression : '[/](' expression ',' expression ')' */
        case m_mdiv:
        /* expression : '[%](' expression ',' expression ')' */
        case m_mmod:
        /* expression : '>(' expression ',' expression ')' */
        case m_gt:
        /* expression : '[>](' expression ',' expression ')' */
        case m_mgt:
        /* expression : '[=](' expression ',' expression ')' */
        case m_meq:
        /* expression : '[and](' expression ',' expression ')' */
        case m_mand:
        /* expression : '[or](' expression ',' expression ')' */
        case m_mor:
            milprintf ("%s(", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (", ");
            print_expression (n->child[1]);
            milprintf (")");
            break;

        /* expression : '[string](' exp ',' exp ',' exp)' */
        case m_mstring2:
        /* expression : '[ifthenelse](' expr ',' expr ',' expr ')' */
        case m_mifthenelse:
            milprintf ("%s(", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (", ");
            print_expression (n->child[1]);
            milprintf (", ");
            print_expression (n->child[2]);
            milprintf (")");
            break;

        /* expression: '{count}(' expression ')' */
        case m_gcount:
        /* expression: '{avg}(' expression ')' */
        case m_gavg:
        /* expression: '{max}(' expression ')' */
        case m_gmax:
        /* expression: '{min}(' expression ')' */
        case m_gmin:
        /* expression: '{sum}(' expression ')' */
        case m_gsum:
            milprintf ("%s(", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (")");
            break;

        /* expr '.select (' expr ',' expr ')' */
        case m_select2:
            print_expression (n->child[0]);
            milprintf (".%s (", ID[n->kind]);
            print_expression (n->child[1]);
            milprintf (", ");
            print_expression (n->child[2]);
            milprintf (")");
            break;

        /* expression : 'not(' expression ')' */
        case m_not:
        /* expression : '[not](' expression ')' */
        case m_mnot:
        /* expression : '[-](' expression ')' */
        case m_mneg:
        /* expression : 'isnil(' expression ')' */
        case m_isnil:
        /* expression : '[isnil](' expression ')' */
        case m_misnil:
            milprintf ("%s(", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (")");
            break;

        /* expression : 'usec ()' */
        case m_usec:
        /* expression : 'new_ws ()' */
        case m_new_ws:
            milprintf ("%s ()", ID[n->kind]);
            break;

        /* expression : 'attr_constr (' expr ',' expr ',' expr ')' */
        case m_attr_constr:
        /* expression : 'mposjoin (' exp ',' exp ',' exp ')' */
        case m_mposjoin:
        /* expression : 'mvaljoin (' exp ',' exp ',' exp ')' */
        case m_mvaljoin:
            milprintf ("%s (", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (", ");
            print_expression (n->child[1]);
            milprintf (", ");
            print_expression (n->child[2]);
            milprintf (")");
            break;

        /* expression: '{count}(' expression ',' expression ')' */
        case m_egcount:
        /* expression : 'doc_tbl (' expr ',' expr ')' */
        case m_doc_tbl:
        /* expression : 'elem_constr_empty (' expr ',' expr ')' */
        case m_elem_constr_e:
        /* expression : 'text_constr (' expr ',' expr ')' */
        case m_text_constr:
            milprintf ("%s (", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (", ");
            print_expression (n->child[1]);
            milprintf (")");
            break;


        /* expression : 'merged_adjacent_text_nodes
                             (' ex ',' ex ',' ex ',' ex ')' */
        case m_merge_adjacent:
        /* expression : 'add_qname (' expr ',' expr ',' expr ',' expr ')' */
        case m_add_qname:
        /* expression : 'add_qnames (' expr ',' expr ',' expr ',' expr ')' */
        case m_add_qnames:
        /* expression : 'sc_desc (' expr ',' expr ',' expr ',' expr ')' */
        case m_sc_desc:
            milprintf ("%s (", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (", ");
            print_expression (n->child[1]);
            milprintf (", ");
            print_expression (n->child[2]);
            milprintf (", ");
            print_expression (n->child[3]);
            milprintf (")");
            break;

        /* expression : 'elem_constr (' e ',' e ',' e ',' e ','
                                        e ',' e ',' e ',' e ')' */
        case m_elem_constr:
            milprintf ("%s (", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (", ");
            print_expression (n->child[1]);
            milprintf (", ");
            print_expression (n->child[2]);
            milprintf (", ");
            print_expression (n->child[3]);
            milprintf (", ");
            print_expression (n->child[4]);
            milprintf (", ");
            print_expression (n->child[5]);
            milprintf (", ");
            print_expression (n->child[6]);
            milprintf (", ");
            print_expression (n->child[7]);
            milprintf (")");
            break;

        case m_merged_union:
            milprintf ("%s (", ID[n->kind]);
            print_args (n->child[0]);
            milprintf (")");
            break;

        case m_lit_min:
            print_min (n);
            break;

        case m_lit_max:
            print_max (n);
            break;

        /* expression : literal */
        case m_lit_int:
        case m_lit_lng:
        case m_lit_str:
        case m_lit_oid:
        case m_lit_dbl:
        case m_lit_bit:
        case m_nil:
            print_literal (n);
            break;

        case m_insert:
            print_expression (n->child[0]);
            milprintf (".%s (", ID[n->kind]);
            print_expression (n->child[1]);
            milprintf (", ");
            print_expression (n->child[2]);
            milprintf (")");
            break;

        /* staircase join variants */
        case m_llscj_anc:
        case m_llscj_anc_self:
        case m_llscj_child:
        case m_llscj_desc:
        case m_llscj_desc_self:
        case m_llscj_foll:
        case m_llscj_foll_sibl:
        case m_llscj_parent:
        case m_llscj_prec:
        case m_llscj_prec_sibl:
            milprintf ("%s (", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (", ");
            print_expression (n->child[1]);
            milprintf (", ");
            print_expression (n->child[2]);
            milprintf (", ");
            print_expression (n->child[3]);
            milprintf (", ");
            print_expression (n->child[4]);
            milprintf (")");
            break;

        case m_llscj_anc_elem:
        case m_llscj_anc_self_elem:
        case m_llscj_child_elem:
        case m_llscj_desc_elem:
        case m_llscj_desc_self_elem:
        case m_llscj_foll_elem:
        case m_llscj_foll_sibl_elem:
        case m_llscj_parent_elem:
        case m_llscj_prec_elem:
        case m_llscj_prec_sibl_elem:
            milprintf ("%s (", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (", ");
            print_expression (n->child[1]);
            milprintf (", ");
            print_expression (n->child[2]);
            milprintf (", ");
            print_expression (n->child[3]);
            milprintf (", ");
            print_expression (n->child[4]);
            milprintf (", ELEMENT)");
            break;

        case m_llscj_anc_text:
        case m_llscj_anc_self_text:
        case m_llscj_child_text:
        case m_llscj_desc_text:
        case m_llscj_desc_self_text:
        case m_llscj_foll_text:
        case m_llscj_foll_sibl_text:
        case m_llscj_parent_text:
        case m_llscj_prec_text:
        case m_llscj_prec_sibl_text:
            milprintf ("%s (", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (", ");
            print_expression (n->child[1]);
            milprintf (", ");
            print_expression (n->child[2]);
            milprintf (", ");
            print_expression (n->child[3]);
            milprintf (", ");
            print_expression (n->child[4]);
            milprintf (", TEXT)");
            break;

        case m_llscj_anc_comm:
        case m_llscj_anc_self_comm:
        case m_llscj_child_comm:
        case m_llscj_desc_comm:
        case m_llscj_desc_self_comm:
        case m_llscj_foll_comm:
        case m_llscj_foll_sibl_comm:
        case m_llscj_parent_comm:
        case m_llscj_prec_comm:
        case m_llscj_prec_sibl_comm:
            milprintf ("%s (", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (", ");
            print_expression (n->child[1]);
            milprintf (", ");
            print_expression (n->child[2]);
            milprintf (", ");
            print_expression (n->child[3]);
            milprintf (", ");
            print_expression (n->child[4]);
            milprintf (", COMMENT)");
            break;

        case m_llscj_anc_pi:
        case m_llscj_anc_self_pi:
        case m_llscj_child_pi:
        case m_llscj_desc_pi:
        case m_llscj_desc_self_pi:
        case m_llscj_foll_pi:
        case m_llscj_foll_sibl_pi:
        case m_llscj_parent_pi:
        case m_llscj_prec_pi:
        case m_llscj_prec_sibl_pi:
            milprintf ("%s (", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (", ");
            print_expression (n->child[1]);
            milprintf (", ");
            print_expression (n->child[2]);
            milprintf (", ");
            print_expression (n->child[3]);
            milprintf (", ");
            print_expression (n->child[4]);
            milprintf (", PI)");
            break;

        case m_llscj_anc_elem_nsloc:
        case m_llscj_anc_self_elem_nsloc:
        case m_llscj_child_elem_nsloc:
        case m_llscj_desc_elem_nsloc:
        case m_llscj_desc_self_elem_nsloc:
        case m_llscj_foll_elem_nsloc:
        case m_llscj_foll_sibl_elem_nsloc:
        case m_llscj_parent_elem_nsloc:
        case m_llscj_prec_elem_nsloc:
        case m_llscj_prec_sibl_elem_nsloc:
            milprintf ("%s (", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (", ");
            print_expression (n->child[1]);
            milprintf (", ");
            print_expression (n->child[2]);
            milprintf (", ");
            print_expression (n->child[3]);
            milprintf (", ");
            print_expression (n->child[4]);
            milprintf (", ");
            print_expression (n->child[5]);
            milprintf (", ");
            print_expression (n->child[6]);
            milprintf (")");
            break;

        case m_llscj_anc_elem_loc:
        case m_llscj_anc_self_elem_loc:
        case m_llscj_child_elem_loc:
        case m_llscj_desc_elem_loc:
        case m_llscj_desc_self_elem_loc:
        case m_llscj_foll_elem_loc:
        case m_llscj_foll_sibl_elem_loc:
        case m_llscj_parent_elem_loc:
        case m_llscj_prec_elem_loc:
        case m_llscj_prec_sibl_elem_loc:
            milprintf ("%s (", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (", ");
            print_expression (n->child[1]);
            milprintf (", ");
            print_expression (n->child[2]);
            milprintf (", ");
            print_expression (n->child[3]);
            milprintf (", ");
            print_expression (n->child[4]);
            milprintf (", ");
            print_expression (n->child[5]);
            milprintf (")");
            break;

        case m_llscj_anc_elem_ns:
        case m_llscj_anc_self_elem_ns:
        case m_llscj_child_elem_ns:
        case m_llscj_desc_elem_ns:
        case m_llscj_desc_self_elem_ns:
        case m_llscj_foll_elem_ns:
        case m_llscj_foll_sibl_elem_ns:
        case m_llscj_parent_elem_ns:
        case m_llscj_prec_elem_ns:
        case m_llscj_prec_sibl_elem_ns:
            milprintf ("%s (", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (", ");
            print_expression (n->child[1]);
            milprintf (", ");
            print_expression (n->child[2]);
            milprintf (", ");
            print_expression (n->child[3]);
            milprintf (", ");
            print_expression (n->child[4]);
            milprintf (", ");
            print_expression (n->child[5]);
            milprintf (")");
            break;

        case m_llscj_anc_pi_targ:
        case m_llscj_anc_self_pi_targ:
        case m_llscj_child_pi_targ:
        case m_llscj_desc_pi_targ:
        case m_llscj_desc_self_pi_targ:
        case m_llscj_foll_pi_targ:
        case m_llscj_foll_sibl_pi_targ:
        case m_llscj_parent_pi_targ:
        case m_llscj_prec_pi_targ:
        case m_llscj_prec_sibl_pi_targ:
            milprintf ("%s (", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (", ");
            print_expression (n->child[1]);
            milprintf (", ");
            print_expression (n->child[2]);
            milprintf (", ");
            print_expression (n->child[3]);
            milprintf (", ");
            print_expression (n->child[4]);
            milprintf (", ");
            print_expression (n->child[5]);
            milprintf (")");
            break;



        default:
            debug_output;     /* Print MIL code so far when in debug mode. */
#ifndef NDEBUG
            PFinfo (OOPS_NOTICE, "node: %s", ID[n->kind]);
#endif
            PFoops (OOPS_FATAL, "Illegal MIL tree. MIL printer screwed up.");
    }
}


/**
 * Create MIL script output for variables.
 *
 * @param n MIL tree node of type #m_var.
 */
static void
print_variable (PFmil_t * n)
{
    assert (n->kind == m_var);

    milprintf ("%s", PFmil_var_str (n->sem.ident));
}

/**
 * Implementation of the grammar rules for minimal `literal'.
 *
 * @param n MIL tree node
 */
static void
print_min (PFmil_t * n)
{
    char *min[] = {
          [m_lng]   = "LNG_MIN"
        , [m_dbl]   = "dbl(nil)"
        , [m_str]   = "str(nil)"
        , [m_bit]   = "bit(nil)"
    };

    if (n->kind != m_lit_min) {
        debug_output;     /* Print MIL code so far when in debug mode. */
#ifndef NDEBUG
        PFinfo (OOPS_NOTICE, "node: %s", ID[n->kind]);
#endif
        PFoops (OOPS_FATAL, "Illegal MIL tree, type expected. "
                            "MIL printer screwed up.");
    }

    milprintf (min[n->sem.t]);
}

/**
 * Implementation of the grammar rules for maximal `literal'.
 *
 * @param n MIL tree node
 */
static void
print_max (PFmil_t * n)
{
    char *max[] = {
          [m_lng]   = "LNG_MAX"
        /* FIXME: this is the minimum value */
        , [m_dbl]   = "dbl(nil)"
        , [m_str]   = "str(nil)"
        , [m_bit]   = "bit(nil)"
    };

    if (n->kind != m_lit_max) {
        debug_output;     /* Print MIL code so far when in debug mode. */
#ifndef NDEBUG
        PFinfo (OOPS_NOTICE, "node: %s", ID[n->kind]);
#endif
        PFoops (OOPS_FATAL, "Illegal MIL tree, type expected. "
                            "MIL printer screwed up.");
    }

    milprintf (max[n->sem.t]);
}

/**
 * Implementation of the grammar rules for `literal'.
 *
 * @param n MIL tree node
 */
static void
print_literal (PFmil_t * n)
{
    switch (n->kind) {

        /* literal : IntegerLiteral */
        case m_lit_int:
            milprintf ("%i", n->sem.i);
            break;

        /* literal : LongIntegerLiteral */
        case m_lit_lng:
            milprintf ("%lldLL", n->sem.l);
            break;

        /* literal : StringLiteral */
        case m_lit_str:
            assert (n->sem.s);
            milprintf ("\"%s\"", PFesc_string (n->sem.s));
            break;

        /* literal : OidLiteral */
        case m_lit_oid:
            milprintf ("%u@0", n->sem.o);
            break;

        /* literal : DblLiteral */
        case m_lit_dbl:
            milprintf ("dbl(%gLL)", n->sem.d);
            break;

        /* literal : BitLiteral */
        case m_lit_bit:
            milprintf (n->sem.b ? "true" : "false");
            break;

        /* literal : 'nil' */
        case m_nil:
            milprintf ("nil");
            break;

        default:
            debug_output;     /* Print MIL code so far when in debug mode. */
#ifndef NDEBUG
            PFinfo (OOPS_NOTICE, "node: %s", ID[n->kind]);
#endif
            PFoops (OOPS_FATAL, "Illegal MIL tree, literal expected. "
                                "MIL printer screwed up.");
    }
}

static void
print_type (PFmil_t *n)
{
    char *types[] = {
          [m_oid]   = "oid"
        , [m_void]  = "void"
        , [m_int]   = "int"
        , [m_str]   = "str"
        , [m_lng]   = "lng"
        , [m_dbl]   = "dbl"
        , [m_bit]   = "bit"
        , [m_chr]   = "chr"
    };

    if (n->kind != m_type) {
        debug_output;     /* Print MIL code so far when in debug mode. */
#ifndef NDEBUG
        PFinfo (OOPS_NOTICE, "node: %s", ID[n->kind]);
#endif
        PFoops (OOPS_FATAL, "Illegal MIL tree, type expected. "
                            "MIL printer screwed up.");
    }

    milprintf (types[n->sem.t]);
}

/**
 * output a single chunk of MIL code to the output character
 * array @a out. Uses @c printf style syntax.
 * @param fmt printf style format string, followed by an arbitrary
 *            number of arguments, according to format string
 */
static void
milprintf (char * fmt, ...)
{
    va_list args;

    assert (out);

    /* print string */
    va_start (args, fmt);

    if (PFarray_vprintf (out, fmt, args) == -1) 
        PFoops (OOPS_FATAL, "unable to print MIL output");

    va_end (args);
}

/**
 * Serialize the internal representation of a MIL program into a
 * string representation that can serve as an input to Monet.
 * 
 * @param m   The MIL tree to print
 * @return Dynamic (character) array holding the generated MIL script.
 */
PFarray_t *
PFmil_serialize (PFmil_t * m)
{
    out = PFarray (sizeof (char));

    /* `statements' is the top rule of our grammar */
    print_statements (m);

    return out;
}

/**
 * Print the generated MIL script in @a milprg to the output stream
 * @a stream, while indenting it nicely.
 *
 * Most characters of the MIL script will be output 1:1 (using fputc).
 * If we encounter a newline, we add spaces according to our current
 * indentation level. If we see curly braces, we increase or decrease
 * the indentation level. Spaces are not printed immediately, but
 * `buffered' (we only increment the counter @c spaces for that).
 * If we see an opening curly brace, we can `redo' some of these
 * spaces to make the opening curly brace be indented less than the
 * block it surrounds.
 *
 * @param stream The output stream to print to (usually @c stdout)
 * @param milprg The dynamic (character) array holding the MIL script.
 */
void
PFmilprint (FILE *stream, PFarray_t * milprg)
{
    char         c;              /* the current character  */
    unsigned int pos;            /* current position in input array */
    unsigned int spaces = 0;     /* spaces accumulated in our buffer */
    unsigned int indent = 0;     /* current indentation level */

    for (pos = 0; (c = *((char *) PFarray_at (milprg, pos))) != '\0'; pos++) {

        switch (c) {

            case '\n':                     /* print newline and spaces       */
                fputc ('\n', stream);      /* according to indentation level */
                spaces = indent;
                break;

            case ' ':                      /* buffer spaces                  */
                spaces++;
                break;

            case '}':                      /* `undo' some spaces when we see */
                                           /* an opening curly brace         */
                spaces = spaces > INDENT_WIDTH ? spaces - INDENT_WIDTH : 0;
                indent -= 2 * INDENT_WIDTH;
                /* Double indentation, as we will reduce indentation when
                 * we fall through next. */

            case '{':
                indent += INDENT_WIDTH;
                /* fall through */

            default:
                while (spaces > 0) {
                    spaces--;
                    fputc (' ', stream);
                }
                fputc (c, stream);
                break;
        }
    }
}

/* vim:set shiftwidth=4 expandtab: */
