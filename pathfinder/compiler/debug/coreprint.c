/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * 
 * Debugging: dump XQuery core language tree in
 * AY&T dot format or human readable
 *
 * $Id$
 */

#include <stdlib.h>
#include <string.h>

/* PFstate */
#include "pathfinder.h"
#include "core.h"
/* PFty_str */
#include "types.h"
/* PFesc_string */
#include "pfstrings.h"

#include "coreprint.h"
#include "prettyp.h"

/** Node names to print out for all the abstract syntax tree nodes. */
char *c_id[]  = {
    
    [c_var]                 "var"
  , [c_lit_str]             "lit_str"
  , [c_lit_int]             "lit_int"
  , [c_lit_dec]             "lit_dec"
  , [c_lit_dbl]             "lit_dbl"
  , [c_nil]                 "nil"

  , [c_seq]                 "seq"

  , [c_let]                 "let"
  , [c_for]                 "for"

  , [c_apply]               "apply"
  , [c_arg]                 "arg"

  , [c_typesw]              "typesw"
  , [c_cases]               "cases"
  , [c_case]                "case"
  , [c_seqtype]             "seqtype"
  , [c_seqcast]             "seqcast"
  , [c_proof]               "proof <:"

  , [c_ifthenelse]          "if-then-else"

  , [c_locsteps]            "locsteps"

  , [c_ancestor]            "ancestor"
  , [c_ancestor_or_self]    "ancestor-or-self"
  , [c_attribute]           "attribute"
  , [c_child]               "child"
  , [c_descendant]          "descendant"
  , [c_descendant_or_self]  "descendant-or-self"
  , [c_following]           "following"
  , [c_following_sibling]   "following-sibling"
  , [c_parent]              "parent"
  , [c_preceding]           "preceding"
  , [c_preceding_sibling]   "preceding-sibling"
  , [c_self]                "self"

  , [c_kind_node]           "kind-node"
  , [c_kind_comment]        "kind-comment"
  , [c_kind_text]           "kind-text"
  , [c_kind_pi]             "kind-pi"
  , [c_kind_doc]            "kind-doc"
  , [c_kind_elem]           "kind-elem"
  , [c_kind_attr]           "kind-attr"

  , [c_namet]               "namet"

  , [c_true]                "true"
  , [c_false]               "false"
  , [c_error]               "error"
  , [c_root]                "root"
  , [c_empty]               "empty"

  , [c_int_eq]              "int-eq"
};

/** Current node id */
static unsigned no = 0;
/** Temporary variable to allocate mem for node names */
static char    *child;
/** Temporary variable for node labels in dot tree */
static char     label[32];

/** Print node with no content */
#define L(t)           snprintf (label, 32, (t))
/** Print node with single content */
#define L2(l1, l2)     snprintf (label, 32, "%s [%s]",    (l1), (l2))
/** Print node with two content parts */
#define L3(l1, l2, l3) snprintf (label, 32, "%s [%s,%s]", (l1), (l2), (l3))

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
        L2 (c_id[n->kind], s);
        break;
    case c_lit_int:   
        snprintf (s, sizeof (s), "%u", n->sem.num);
        L2 (c_id[n->kind], s);                  
        break;
    case c_lit_dec:   
        snprintf (s, sizeof (s), "%.5g", n->sem.dec);
        L2 (c_id[n->kind], s);                
        break;
    case c_lit_dbl:   
        snprintf (s, sizeof (s), "%.5g", n->sem.dbl);
        L2 (c_id[n->kind], s);                
        break;
    case c_namet:     
        L2 (c_id[n->kind], PFqname_str (n->sem.qname));
        break;
    case c_apply:     
        L2 (c_id[n->kind], PFqname_str (n->sem.fun->qname));
        break;
    case c_seqtype:      
        L2 (c_id[n->kind], PFty_str (n->sem.type));
        break;
    case c_error:    
        snprintf (s, sizeof (s), "%s", PFesc_string (n->sem.str));
        L2 (c_id[n->kind], s);
        break;
        
    default:          
        L (c_id[n->kind]);
        break;
    }
    
    fprintf (f, "%s [label=\"%s\"];\n", node, label);
    
    for (c = 0;
         c < PFCNODE_MAXCHILD && n->child[c] != 0;
         c++) {      
        child = (char *) PFmalloc (sizeof ("node4294967296"));
        
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
 * @param t root of abstract syntax tree
 */
void
PFcore_dot (FILE *f, PFcnode_t *root)
{
    if (root) {
        fprintf (f, "digraph XQueryCore {\n");
        core_dot (f, root, "node0");
        fprintf (f, "}\n");
    }    
}

/**
 * Recursively walk the core language tree @a n and prettyprint
 * the query it represents.
 *
 * @param n core language tree to prettyprint
 */
static void
core_pretty (PFcnode_t *n)
{
    int c;
    bool comma;
    
    if (!n)
        return;
    
    PFprettyprintf ("%s (%c", c_id[n->kind], START_BLOCK);
    
    comma = true;
    
    switch (n->kind) {
    case c_var:         
        PFprettyprintf ("%s", PFqname_str (n->sem.var->qname));
        break;
    case c_lit_str:     
        PFprettyprintf ("%s", n->sem.str);
        break;
    case c_lit_int:     
        PFprettyprintf ("%i", n->sem.num);
        break;
    case c_lit_dec:     
        PFprettyprintf ("%.5g", n->sem.dec);
        break;
    case c_lit_dbl:     
        PFprettyprintf ("%.5g", n->sem.dbl);
        break;
    case c_namet:       
        PFprettyprintf ("%s", PFqname_str (n->sem.qname));
        break;
    case c_apply:       
        PFprettyprintf ("%s", PFqname_str (n->sem.fun->qname));
        break;
    case c_seqtype:        
        PFprettyprintf ("%s", PFty_str (n->sem.type));
        break;
    case c_error:       
        PFprettyprintf ("\"%s\"", n->sem.str);
        break;
        
    default:            
        comma = false;
        break;
    }
    
    for (c = 0;
         c < PFCNODE_MAXCHILD && n->child[c] != 0;
         c++) {
        if (comma)
            PFprettyprintf (",%c %c", END_BLOCK, START_BLOCK);
        comma = true;
        
        core_pretty (n->child[c]);
    }
    
    if (PFstate.print_types)
        PFprettyprintf ("%c) {%s}", END_BLOCK, PFty_str (n->type));
    else
        PFprettyprintf ("%c)", END_BLOCK);
}

/**
 * Dump XQuery core language tree @a t in pretty-printed form
 * into file @a f.
 *
 * @param f file to dump into
 * @param t root of core language tree
 */
void
PFcore_pretty (FILE *f, PFcnode_t *t)
{
    PFprettyprintf ("%c", START_BLOCK);
    core_pretty (t);
    PFprettyprintf ("%c", END_BLOCK);
    
    (void) PFprettyp (f);
    
    fputc ('\n', f);
}


/* vim:set shiftwidth=4 expandtab: */
