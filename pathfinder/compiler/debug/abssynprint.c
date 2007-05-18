/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * 
 * Debugging: dump XQuery abstract syntax tree in
 * AY&T dot format or human readable
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

#include "pathfinder.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "abssynprint.h"

#include "mem.h"
#include "oops.h"
#include "abssyn.h"
#include "prettyp.h"
#include "pfstrings.h"
#include "functions.h"

/** Node names to print out for all the abstract syntax tree nodes. */
char *p_id[]  = {
      [p_main_mod]          = "main_mod"
    , [p_lib_mod]           = "lib_mod"
    , [p_mod_ns]            = "mod_ns"
    , [p_ordering_mode]     = "ordering_mode"
    , [p_def_order]         = "def_order"
    , [p_copy_ns]           = "copy_ns"
    , [p_base_uri]          = "base_uri"
    , [p_schm_ats]          = "schm_ats"
    , [p_mod_imp]           = "mod_imp"
    , [p_var_decl]          = "var_decl"
    , [p_external]          = "external"
    , [p_constr_decl]       = "constr_decl"
    , [p_fun_sig]           = "fun_sig"
    , [p_where]             = "where"
    , [p_ord_ret]           = "ord_ret"
    , [p_orderby]           = "orderby"
    , [p_vars]              = "vars"
    , [p_var_type]          = "var_type"
    , [p_orderspecs]        = "orderspecs"
    , [p_default]           = "default"
    , [p_schm_elem]         = "schm_elem"
    , [p_schm_attr]         = "schm_attr"
    , [p_then_else]         = "then_else"
    , [p_ordered]           = "ordered"
    , [p_unordered]         = "unordered"

    , [p_plus]              = "plus"                                       
    , [p_minus]             = "minus"                                      
    , [p_mult]              = "mult"                                       
    , [p_div]               = "div"                                        
    , [p_idiv]              = "idiv"                                        
    , [p_mod]               = "mod"                                        
    , [p_and]               = "and"                                        
    , [p_or]                = "or"                                         
    , [p_lt]                = "lt"                                         
    , [p_le]                = "le"                                         
    , [p_gt]                = "gt"                                         
    , [p_ge]                = "ge"                                         
    , [p_eq]                = "eq"                                         
    , [p_ne]                = "ne"                                         
    , [p_val_lt]            = "val_lt"                                     
    , [p_val_le]            = "val_le"                                     
    , [p_val_gt]            = "val_gt"                                     
    , [p_val_ge]            = "val_ge"                                     
    , [p_val_eq]            = "val_eq"                                     
    , [p_val_ne]            = "val_ne"                                     
    , [p_uplus]             = "uplus"                                      
    , [p_uminus]            = "uminus"                                     
    , [p_lit_int]           = "lit_int"
    , [p_lit_dec]           = "lit_dec"
    , [p_lit_dbl]           = "lit_dbl"
    , [p_lit_str]           = "lit_str"
    , [p_is]                = "is"                                         
    , [p_step]              = "step"
    , [p_varref]            = "varref" 
    , [p_var]               = "var"
    , [p_locpath]           = "locpath"                                    
    , [p_root]              = "root"                                       
    , [p_dot]               = "dot"                                        
    , [p_ltlt]              = "ltlt"                                       
    , [p_gtgt]              = "gtgt"                                       
    , [p_flwr]              = "flwr"                                       
    , [p_binds]             = "binds"                                      
    , [p_nil]               = "nil"                                        
    , [p_empty_seq]         = "empty-seq"                                        
    , [p_bind]              = "bind"                                       
    , [p_let]               = "let"                                        
    , [p_exprseq]           = "exprseq"                                    
    , [p_range]             = "range"                                      
    , [p_union]             = "union"                                      
    , [p_intersect]         = "intersect"                                  
    , [p_except]            = "except"                                     
    , [p_pred]              = "pred"                                       
    , [p_if]                = "if"                                         
    , [p_some]              = "some"                                       
    , [p_every]             = "every"                                      
    , [p_instof]            = "instof"                                     
    , [p_seq_ty]            = "seq_ty"
    , [p_empty_ty]          = "empty_ty"                                   
    , [p_node_ty]           = "node_ty"
    , [p_item_ty]           = "item_ty"                                     
    , [p_atom_ty]           = "atom_ty"
    , [p_named_ty]          = "named_ty"
    , [p_req_ty]            = "req_ty"                                     
    , [p_req_name]          = "req_name"
    , [p_typeswitch]        = "typeswitch"                                 
    , [p_cases]             = "cases"                                      
    , [p_case]              = "case"           
    , [p_castable]          = "castable"
    , [p_cast]              = "cast"                                       
    , [p_treat]             = "treat"                                      
    , [p_validate]          = "validate"
    , [p_apply]             = "apply"                                   
    , [p_fun_ref]           = "fun_ref"
    , [p_args]              = "args"                                       
    , [p_doc]               = "doc"                                       
    , [p_elem]              = "elem"
    , [p_attr]              = "attr"
    , [p_text]              = "text"
    , [p_tag]               = "tag"
    , [p_pi]                = "pi"
    , [p_comment]           = "comment"                                    
    , [p_contseq]           = "contseq"
    , [p_decl_imps]         = "decl_imps"                                  
    , [p_boundspc_decl]     = "boundspc_decl"
    , [p_coll_decl]         = "coll_decl"
    , [p_ns_decl]           = "ns_decl"
    , [p_option]            = "option"
    , [p_fun_decl]          = "fun_decl"
    , [p_fun]               = "fun"
    , [p_ens_decl]          = "ens_decl"                                   
    , [p_fns_decl]          = "fns_decl"                                   
    , [p_schm_imp]          = "schm_imp"                                   
    , [p_params]            = "params"                                     
    , [p_param]             = "param"    
    , [p_ext_expr]          = "ext_expr"
    , [p_pragma]            = "pragma"
    , [p_pragmas]           = "pragmas"
    , [p_revalid]           = "revalid"
    , [p_insert]            = "insert"
    , [p_delete]            = "delete"
    , [p_replace]           = "replace"
    , [p_rename]            = "rename"
    , [p_transform]         = "transform"
    , [p_modify]            = "modify"
    , [p_transbinds]        = "transbinds"
    , [p_stmt_ty]           = "stmt_ty"

    /* Pathfinder extension: recursion */
    , [p_recursion]         = "recursion"
    , [p_seed]              = "seed"
    
    /* Pathfinder extension: XRPC */
    , [p_xrpc]              = "xrpc"

    /* docmgmt type (for user-defined doc mgmt functions) */
    , [p_docmgmt_ty]        = "docmgmt_ty"
};

/** Names of XPath axes */
static char *axis[] = {
      [p_ancestor]           = "ancestor"
    , [p_ancestor_or_self]   = "ancestor-or-self"
    , [p_attribute]          = "attribute"
    , [p_child]              = "child"
    , [p_descendant]         = "descendant"
    , [p_descendant_or_self] = "descendant-or-self"
    , [p_following]          = "following"
    , [p_following_sibling]  = "following-sibling"
    , [p_parent]             = "parent"
    , [p_preceding]          = "preceding"
    , [p_preceding_sibling]  = "preceding-sibling"
    , [p_self]               = "self"
/* [STANDOFF] */
    , [p_select_narrow]      = "select-narrow"
    , [p_select_wide]        = "select-wide"
    , [p_reject_narrow]      = "reject-narrow"
    , [p_reject_wide]        = "reject-wide"
/* [/STANDOFF] */
};

/** Names of node type tests */
static char *kind[] = {
      [p_kind_node]          = "node"
    , [p_kind_comment]       = "comment"
    , [p_kind_text]          = "text"
    , [p_kind_pi]            = "pi"
    , [p_kind_doc]           = "document"
    , [p_kind_elem]          = "element"
    , [p_kind_attr]          = "attribute"
};

/** Names of occurence indicators */
static char *oci[] = {
      [p_one]                = "1"
    , [p_zero_or_one]        = "?"
    , [p_zero_or_more]       = "*"
    , [p_one_or_more]        = "+"
};

/** Name of sort order (ascending/descending) */
static char *dir[] = {
      [p_asc]                = "asc"
    , [p_desc]               = "desc"
};

/** Name of sort modifiers */
static char *empty[] = {
      [p_greatest]           = "empty greatest"
    , [p_least]              = "empty least"
};

/** Current node id */
static unsigned no = 0;
/** Temporary variable to allocate mem for node names */
static char    *child;
#define DOT_LABELS 64
/** Temporary variable for node labels in dot tree */
static char     label[DOT_LABELS];

/** Print node with no content */
#define L(t, loc)           snprintf (label, DOT_LABELS,                    \
				      "%s\\n(%u,%u-%u,%u)\\r",              \
                                      (t), (loc).first_row, (loc).first_col,\
                                           (loc).last_row, (loc).last_col)

/** Print node with single content */
#define L2(l1, l2, loc)     snprintf (label, DOT_LABELS,                    \
				      "%s [%s]\\n(%u,%u-%u,%u)\\r",         \
                                      (l1), (l2),                           \
                                      (loc).first_row, (loc).first_col,     \
                                      (loc).last_row, (loc).last_col)

/** Print node with two content parts */
#define L3(l1, l2, l3, loc) snprintf (label, DOT_LABELS,                    \
                                      "%s [%s,%s]\\n(%u,%u-%u,%u)\\r",      \
                                      (l1), (l2), (l3),                     \
                                      (loc).first_row, (loc).first_col,     \
                                      (loc).last_row, (loc).last_col)

/** Print node with two content parts */
#define L4(l1, l2, l3, l4, loc) snprintf (label, DOT_LABELS,                \
                                      "%s [%s,%s,%s]\\n(%u,%u-%u,%u)\\r",   \
                                      (l1), (l2), (l3), (l4),               \
                                      (loc).first_row, (loc).first_col,     \
                                      (loc).last_row, (loc).last_col)

/**
 * Print abstract syntax tree in AT&T dot notation.
 * @param f File pointer for the output (usually @c stdout)
 * @param n The current node to print (function is recursive)
 * @param node Name of the parent node.
 */
static void 
abssyn_dot (FILE *f, PFpnode_t *n, char *node, bool qnames_resolved)
{
    int c;
    char s[sizeof ("4294967285")];

    switch (n->kind) {
        case p_lit_int:
            snprintf (s, sizeof (s), LLFMT, n->sem.num);
            L2 (p_id[n->kind], s, n->loc);
            break;
        case p_lit_dec:
            snprintf (s, sizeof (s), "%.5g", n->sem.dec);
            L2 (p_id[n->kind], s, n->loc);
            break;
        case p_lit_dbl:
            snprintf (s, sizeof (s), "%.5g", n->sem.dbl);
            L2 (p_id[n->kind], s, n->loc);
            break;
        case p_lit_str:
            L2 (p_id[n->kind], PFesc_string (n->sem.str), n->loc);
            break;
        case p_step:
            L2 (p_id[n->kind], axis[n->sem.axis], n->loc);
            break;
        case p_var:
            L2 (p_id[n->kind], PFqname_str (n->sem.var->qname), n->loc);
            break;
        case p_orderby:
            L2 (p_id[n->kind], n->sem.tru ? "stable" : "unstable", n->loc);
            break;
        case p_orderspecs:
            L4 (p_id[n->kind], dir[n->sem.mode.dir],
                               empty[n->sem.mode.empty],
                               n->sem.mode.coll ? n->sem.mode.coll : "default",
                               n->loc);
            break;
        case p_seq_ty:
            L2 (p_id[n->kind], oci[n->sem.oci], n->loc);
            break;
        case p_node_ty:
            L2 (p_id[n->kind], kind[n->sem.kind], n->loc);
            break;

        case p_validate:
            L2 (p_id[n->kind], n->sem.tru ? "strict" : "lax", n->loc);
            break;

        case p_varref:
        case p_atom_ty:
        case p_named_ty:
        case p_req_name:
        case p_schm_elem:
        case p_schm_attr:
        case p_fun_ref:
        case p_fun_decl:
        case p_tag:
        case p_option:
            L2 (p_id[n->kind], qnames_resolved
                               ? PFqname_str (n->sem.qname)
                               : PFqname_raw_str (n->sem.qname_raw), n->loc);
            break;

        case p_apply:
        case p_fun:
            L2 (p_id[n->kind], PFqname_str (n->sem.fun->qname), n->loc);
            break;

        case p_boundspc_decl:
            L2 (p_id[n->kind], n->sem.tru ? "preserve" : "strip", n->loc);
            break;
        case p_ns_decl:
            L2 (p_id[n->kind], n->sem.str, n->loc);
            break;
        case p_lib_mod:
            L2 (p_id[n->kind], n->sem.str ? n->sem.str : "<NULL>", n->loc);
            break;

        case p_revalid:
            L2 (p_id[n->kind], n->sem.revalid == revalid_strict
                               ? "strict"
                               : (n->sem.revalid == revalid_lax
                                  ? "lax"
                                  : "skip"),
                               n->loc);
            break;

        case p_insert:
            {
                char *s[] = { [p_first_into] = "as first into",
                              [p_last_into]  = "as last into",
                              [p_into]       = "into",
                              [p_after]      = "after",
                              [p_before]     = "before" };
                assert (n->sem.insert < (sizeof (s) / sizeof (*s)));
                L2 (p_id[n->kind], s[n->sem.insert], n->loc);
            } break;

        case p_replace:
            L2 (p_id[n->kind], n->sem.tru ? "" : "value of", n->loc);
            break;

        default:
            L (p_id[n->kind], n->loc);
    }

    fprintf (f, "%s [label=\"%s\"];\n", node, label);

    for (c = 0; c < PFPNODE_MAXCHILD && n->child[c] != 0; c++) {   
        child = (char *) PFmalloc (sizeof ("node4294967296"));

        sprintf (child, "node%u", ++no);
        fprintf (f, "%s -> %s;\n", node, child);

        abssyn_dot (f, n->child[c], child, qnames_resolved);
    }
}


/**
 * Dump XQuery abstract syntax tree in AT&T dot format
 * (pipe the output through `dot -Tps' to produce a Postscript file).
 *
 * @param f file to dump into
 * @param root root of abstract syntax tree
 */
void
PFabssyn_dot (FILE *f, PFpnode_t *root, bool qnames_resolved)
{
    if (root) {
        fprintf (f, "digraph XQuery {\n");
        fprintf (f, "node [shape=box];\n");
        abssyn_dot (f, root, "node0", qnames_resolved);
        fprintf (f, "}\n");
    }
}

/**
 * Recursively walk the abstract syntax tree @a n and prettyprint
 * the query it represents.
 *
 * @param n abstract syntax tree to prettyprint
 */
static void
abssyn_pretty (PFpnode_t *n, bool qnames_resolved)
{
    int c;
    bool comma;

    if (!n)
        return;

    PFprettyprintf ("%s (%c", p_id[n->kind], START_BLOCK);

    comma = true;

    switch (n->kind) {
        case p_lit_int:
            PFprettyprintf (LLFMT, n->sem.num);
            break;
        case p_lit_dec:
            PFprettyprintf ("%.5g", n->sem.dec);
            break;
        case p_lit_dbl:
            PFprettyprintf ("%.5g", n->sem.dbl);
            break;
        case p_lit_str:
            PFprettyprintf ("\"%s\"", n->sem.str);
            break;
        case p_step:
            PFprettyprintf ("%s", axis[n->sem.axis]);
            break;
        case p_varref:
            PFprettyprintf ("$%s", qnames_resolved
                                   ? PFqname_str (n->sem.qname)
                                   : PFqname_raw_str (n->sem.qname_raw));
            break;
        case p_var:
            PFprettyprintf ("$%s", PFqname_str (n->sem.var->qname));
            break;
        case p_orderby:
            PFprettyprintf ("%s", n->sem.tru ? "stable" : "unstable");
            break;
        case p_orderspecs:
            PFprettyprintf ("%s, %s, %s", dir[n->sem.mode.dir],
                                          empty[n->sem.mode.empty],
                                          n->sem.mode.coll ? n->sem.mode.coll
                                                           : "default");
            break;
        case p_seq_ty:
            PFprettyprintf ("%s", oci[n->sem.oci]);
            break;
        case p_node_ty:
            PFprettyprintf ("%s", kind[n->sem.kind]);
            break;
        case p_atom_ty:
        case p_named_ty:
        case p_req_name:
        case p_fun_ref:
        case p_tag:
        case p_option:
            PFprettyprintf ("%s", qnames_resolved
                                  ? PFqname_str (n->sem.qname)
                                  : PFqname_raw_str (n->sem.qname_raw));
            break;
        case p_apply:
            PFprettyprintf ("%s", PFqname_str (n->sem.fun->qname));
            break;
        case p_pi:
            PFprettyprintf ("%s", n->sem.str);
            break;    
        case p_boundspc_decl:
            PFprettyprintf ("%s", n->sem.tru ? "preserve" : "strip");
            break;
        case p_ns_decl:
            PFprettyprintf ("%s", n->sem.str);
            break;
        case p_fun_decl:
            PFprettyprintf ("%s", qnames_resolved 
                                  ? PFqname_str (n->sem.qname)
                                  : PFqname_raw_str (n->sem.qname_raw));
            break;
        case p_fun:
            PFprettyprintf ("%s", PFqname_str (n->sem.fun->qname));
            break;
        case p_lib_mod:
            PFprettyprintf ("\"%s\"", n->sem.str ? n->sem.str : "<NULL>");
            break;
        case p_revalid:
            PFprettyprintf ("%s", n->sem.revalid == revalid_strict
                                  ? "strict"
                                  : (n->sem.revalid == revalid_lax
                                     ? "lax"
                                     : "skip"));
            break;
        case p_insert:
            {
                char *s[] = { [p_first_into] = "as first into",
                              [p_last_into]  = "as last into",
                              [p_into]       = "into",
                              [p_after]      = "after",
                              [p_before]     = "before" };
                assert (n->sem.insert < (sizeof (s) / sizeof (*s)));
                PFprettyprintf ("%s", s[n->sem.insert]);
            } break;

        case p_replace:
            PFprettyprintf (n->sem.tru ? "" : "value of");
            break;

        default:
            comma = false;
    }

    for (c = 0; c < PFPNODE_MAXCHILD && n->child[c] != 0; c++) {
        if (comma)
            PFprettyprintf (",%c %c", END_BLOCK, START_BLOCK);
        comma = true;

        abssyn_pretty (n->child[c], qnames_resolved);
    }

    PFprettyprintf ("%c)", END_BLOCK);
}

/**
 * Dump XQuery abstract syntax tree @a t in pretty-printed form
 * into file @a f.
 *
 * @param f file to dump into
 * @param t root of abstract syntax tree
 */
void
PFabssyn_pretty (FILE *f, PFpnode_t *t, bool qnames_resolved)
{
    PFprettyprintf ("%c", START_BLOCK);
    abssyn_pretty (t, qnames_resolved);
    PFprettyprintf ("%c", END_BLOCK);

    (void) PFprettyp (f);

    fputc ('\n', f);
}


/* vim:set shiftwidth=4 expandtab: */
