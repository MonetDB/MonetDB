/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Debugging: dump XQuery core language tree in
 * AT&T dot format or human readable
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

#include "pathfinder.h"

#include <stdlib.h>
#include <string.h>

#include "coreprint.h"

#include "mem.h"
/* PFcnode_t */
#include "core.h"
/* PFty_str */
#include "types.h"
/* PFesc_string */
#include "pfstrings.h"

#include "prettyp.h"
#include <assert.h>

/* Easily access subtree-parts */
#include "child_mnemonic.h"

/** Node names to print out for all the abstract syntax tree nodes. */
char *c_id[]  = {

    [c_var]                = "var"
  , [c_lit_str]            = "lit_str"
  , [c_lit_int]            = "lit_int"
  , [c_lit_dec]            = "lit_dec"
  , [c_lit_dbl]            = "lit_dbl"
  , [c_nil]                = "nil"

  , [c_seq]                = "seq"
  , [c_twig_seq]           = "twig_seq"
  , [c_ordered]            = "ordered"
  , [c_unordered]          = "unordered"

  , [c_flwr]               = "flwr"
  , [c_let]                = "let"
  , [c_letbind]            = "letbind"
  , [c_for]                = "for"
  , [c_forbind]            = "forbind"
  , [c_forvars]            = "forvars"

  , [c_orderby]            = "orderby"
  , [c_orderspecs]         = "orderspecs"

  , [c_apply]              = "apply"
  , [c_arg]                = "arg"

  , [c_typesw]             = "typesw"
  , [c_cases]              = "cases"
  , [c_case]               = "case"
  , [c_default]            = "default"
  , [c_seqtype]            = "seqtype"
  , [c_seqcast]            = "seqcast"
  , [c_proof]              = "proof <:"
  , [c_stattype]           = "stattype"

  , [c_if]                 = "if"
  , [c_then_else]          = "then_else"

  , [c_locsteps]           = "locsteps"

  , [c_ancestor]           = "ancestor"
  , [c_ancestor_or_self]   = "ancestor-or-self"
  , [c_attribute]          = "attribute"
  , [c_child]              = "child"
  , [c_descendant]         = "descendant"
  , [c_descendant_or_self] = "descendant-or-self"
  , [c_following]          = "following"
  , [c_following_sibling]  = "following-sibling"
  , [c_parent]             = "parent"
  , [c_preceding]          = "preceding"
  , [c_preceding_sibling]  = "preceding-sibling"
  , [c_self]               = "self"
/* [STANDOFF] */
  , [c_select_narrow]      = "select-narrow"
  , [c_select_wide]        = "select-wide"
  , [c_reject_narrow]      = "reject-narrow"
  , [c_reject_wide]        = "reject-wide"
/* [/STANDOFF] */

  , [c_elem]               = "elem"
  , [c_attr]               = "attr"
  , [c_text]               = "text"
  , [c_doc]                = "doc"
  , [c_comment]            = "comment"
  , [c_pi]                 = "pi"
  , [c_tag]                = "tag"

  , [c_true]               = "true"
  , [c_false]              = "false"
  , [c_empty]              = "empty"

  , [c_main]               = "main"
  , [c_fun_decls]          = "fun_decls"
  , [c_fun_decl]           = "fun_decl"
  , [c_params]             = "params"
  , [c_param]              = "param"
  , [c_cast]               = "cast"

  /* Pathfinder extensions: recursion */
  , [c_recursion]          = "recursion"
  , [c_seed]               = "seed"

  /* Pathfinder extension: XRPC */
  , [c_xrpc]               = "xrpc"

};

/** Current node id */
static unsigned no;
/** Temporary variable for node labels in dot tree */
static char     label[32];

/** Print node with no content */
#define L0(t)           snprintf (label, sizeof(label), (t))
/** Print node with single content */
#define L2(l1, l2)     snprintf (label, sizeof(label), "%s [%s]",    (l1), (l2))
/** Print node with two content parts */
#define L3(l1, l2, l3) snprintf (label, sizeof(label), "%s [%s,%s]", (l1), (l2), (l3))

/**
 * Print core language tree in AT&T dot notation.
 * @param f File pointer for the output (usually @c stdout)
 * @param n The current node to print (function is recursive)
 * @param node Name of the parent node.
 */
static void
core_dot (FILE *f, PFcnode_t *n, char *node)
{
    int c;
    char s[sizeof ("4294967285")];

    switch (n->kind) {
    case c_var:
        L2 (c_id[n->kind], PFqname_str (n->sem.var->qname));
        break;
    case c_lit_str:
        snprintf (s, sizeof (s), "%s", PFesc_string (n->sem.str));
        s[sizeof(s) - 1] = 0;
        L2 (c_id[n->kind], s);
        break;
    case c_lit_int:
        snprintf (s, sizeof (s), LLFMT, n->sem.num);
        s[sizeof(s) - 1] = 0;
        L2 (c_id[n->kind], s);
        break;
    case c_lit_dec:
        snprintf (s, sizeof (s), "%.5g", n->sem.dec);
        s[sizeof(s) - 1] = 0;
        L2 (c_id[n->kind], s);
        break;
    case c_lit_dbl:
        snprintf (s, sizeof (s), "%.5g", n->sem.dbl);
        s[sizeof(s) - 1] = 0;
        L2 (c_id[n->kind], s);
        break;
    case c_apply:
    case c_fun_decl:
        L2 (c_id[n->kind], PFqname_str (n->sem.fun->qname));
        break;
    case c_tag:
        L2 (c_id[n->kind], PFqname_str (n->sem.qname));
        break;
    case c_seqtype:
        L2 (c_id[n->kind], PFty_str (n->sem.type));
        break;

    case c_orderby:
        L2 (c_id[n->kind], n->sem.tru ? "stable" : "");
        break;

    case c_orderspecs:
        L2 (c_id[n->kind],
            n->sem.mode.dir == p_desc ? "descending" :"ascending");
        break;

    default:
        L0 (c_id[n->kind]);
        break;
    }
    label[sizeof(label) - 1] = 0;

    fprintf (f, "%s [label=\"%s\"];\n", node, label);

    for (c = 0; c < PFCNODE_MAXCHILD && n->child[c] != 0; c++) {
        /* Temporary variable to allocate mem for node names */
        char *child = (char *) PFmalloc (sizeof ("node4294967296"));

        sprintf (child, "node%u", ++no);
        fprintf (f, "%s -> %s;\n", node, child);

        core_dot (f, n->child[c], child);
    }
}


/**
 * Dump XQuery core language tree in AT&T dot format
 * (pipe the output through `dot -Tps' to produce a Postscript file).
 *
 * @param f file to dump into
 * @param root root of abstract syntax tree
 */
void
PFcore_dot (FILE *f, PFcnode_t *root)
{
    if (root) {
        no = 0;
        fprintf (f, "digraph XQueryCore {\n");
        core_dot (f, root, "node0");
        fprintf (f, "}\n");
    }
}

/**
 * Break the current line and indent the next line
 * by @a ind characters.
 *
 * @param f file to print to
 * @param ind indentation level
 */
static void
indent (FILE *f, int ind, bool nl)
{
    if (!nl) return;

    fputc ('\n', f);

    while (ind-- > 0)
        fputc (' ', f);
}

/**
 * Recursively walk the core language tree @a n and prettyprint
 * the query it represents.
 *
 * @param n core language tree to prettyprint
 */
static void
core_pretty (FILE *f, PFcnode_t *n, int i, bool nl, bool print_types)
{
    int c;
    bool topdown = false;
    bool comma;

    if (!n)
        return;

    switch (n->kind) {
        case c_for:
            indent (f, i - 5, nl);
            fprintf (f, "%s (", c_id[n->kind]);
            break;

        case c_let:
            indent (f, i - 5, nl);
            fprintf (f, "%s (", c_id[n->kind]);
            break;

        case c_typesw:
            indent (f, i, nl);
            fprintf (f, "%s  (", c_id[n->kind]);
            break;

        case c_case:
            indent (f, i - 4, nl);
            fprintf (f, "%s (", c_id[n->kind]);
            break;

        case c_default:
            indent (f, i - 7, nl);
            fprintf (f, "%s (", c_id[n->kind]);
            break;

        default:
            indent (f, i, nl);
            fprintf (f, "%s (", c_id[n->kind]);
            break;
    }

    comma = true;

    switch (n->kind) {
    case c_var:
        fprintf (f, "%s", PFqname_str (n->sem.var->qname));
        break;
    case c_lit_str:
        fprintf (f, "\"%s\"", PFesc_string (n->sem.str));
        break;
    case c_lit_int:
        fprintf (f, LLFMT, n->sem.num);
        break;
    case c_lit_dec:
        fprintf (f, "%.5g", n->sem.dec);
        break;
    case c_lit_dbl:
        fprintf (f, "%.5g", n->sem.dbl);
        break;
    case c_apply:
        fprintf (f, "%s", PFqname_str (n->sem.fun->qname));
        break;
    case c_seqtype:
        fprintf (f, "%s", PFty_str (n->sem.type));
        break;
    case c_tag:
        fprintf (f, "%s", PFqname_str (n->sem.qname));
        break;
    case c_orderby:
        fprintf (f, "%s", n->sem.tru ? "stable" : "unstable");
        break;
    case c_orderspecs:
        fprintf (f, "%s,%c %c%s", n->sem.mode.dir == p_desc ?
                                      "descending" : "ascending",
                                      END_BLOCK, START_BLOCK,
                                      n->sem.mode.empty == p_greatest ?
                                      "greatest" : "least");
        break;

    case c_flwr:
        core_pretty (f, n->child[0], i+6, L(n)->kind != c_nil, print_types);
        fprintf (f, ",");
        core_pretty (f, n->child[1], i+6, true, print_types);
        topdown = true;
        break;

    case c_let:
        core_pretty (f, L(n), i, false, print_types);
        fprintf (f, ",");
        core_pretty (f, R(n), i, true, print_types);
        topdown = true;
        break;

    case c_letbind:
        core_pretty (f, L(n), i+2, true, print_types);
        fprintf (f, ",");
        core_pretty (f, R(n), i+2, true, print_types);
        topdown = true;
        break;

    case c_for:
        core_pretty (f, L(n), i, false, print_types);
        fprintf (f, ",");
        core_pretty (f, R(n), i, true, print_types);
        topdown = true;
        break;

    case c_forbind:
        core_pretty (f, L(n), i+2, true, print_types);
        fprintf (f, ",");
        core_pretty (f, R(n), i+2, true, print_types);
        topdown = true;
        break;

    case c_forvars:
        core_pretty (f, L(n), i, false, print_types);
        fprintf (f, ", ");
        core_pretty (f, R(n), i, false, print_types);
        topdown = true;
        break;

    case c_ancestor:
    case c_ancestor_or_self:
    case c_attribute:
    case c_child:
    case c_descendant:
    case c_descendant_or_self:
    case c_following:
    case c_following_sibling:
    case c_parent:
    case c_preceding:
    case c_preceding_sibling:
    case c_self:
    case c_select_narrow:
    case c_select_wide:
    case c_reject_narrow:
    case c_reject_wide:
        core_pretty (f, L(n), i, false, print_types);
        topdown = true;
        break;

    case c_arg:
        core_pretty (f, n->child[0], i+5, false, print_types);
        fprintf (f, ",");
        core_pretty (f, n->child[1], i, true, print_types);
        topdown = true;
        break;

    case c_typesw:
        core_pretty (f, L(n), i, false, print_types);
        fprintf (f, ",");
        core_pretty (f, R(n), i+2, true, print_types);
        topdown = true;
        break;

    case c_cases:
        core_pretty (f, n->child[0], i+5, true, print_types);
        fprintf (f, ",");
        core_pretty (f, n->child[1], i+5, true, print_types);
        topdown = true;
        break;

    case c_case:
        core_pretty (f, L(n), i+2, false, print_types);
        fprintf (f, ",");
        core_pretty (f, R(n), i+2, true, print_types);
        topdown = true;
        break;

    case c_default:
        if (L(n)->kind == c_typesw)
            core_pretty (f, L(n), i-7, true, print_types);
        else
            core_pretty (f, L(n), i+2, false, print_types);
        topdown = true;
        break;

    default:
        comma = false;
        break;
    }

    if (!topdown)
        for (c = 0;
             c < PFCNODE_MAXCHILD && n->child[c] != 0;
             c++) {
            if (comma)
                fprintf (f, ",");
            comma = true;

            core_pretty (f, n->child[c], i+2, true, print_types);
        }

    if (print_types)
        fprintf (f, ") {%s}", PFty_str (n->type));
    else
        fprintf (f, ")");
}

/**
 * Dump XQuery core language tree @a t in pretty-printed form
 * into file @a f.
 *
 * @param f file to dump into
 * @param t root of core language tree
 */
void
PFcore_pretty (FILE *f, PFcnode_t *t, bool print_types)
{
    core_pretty (f, t, 0, false, print_types);
}

void
PFcore_stdout (PFcnode_t *t)
{
    PFcore_pretty(stdout, t, true);
}

/* vim:set shiftwidth=4 expandtab: */
