/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Implementations for functions required in normalizer.mt
 *
 * Doxygen cannot correctly parse twig input files. When we place
 * functions, etc. in this separate file, we can process it with
 * doxygen and include the normalization stuff into the documentation.
 * This file is included by normalizer.mt. It cannot be compiled on
 * its own but only makes sense included into normalizer.mt
 *
 *
 * Copyright Notice:
 * -----------------
 *
 *  The contents of this file are subject to the MonetDB Public
 *  License Version 1.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 *
 *  The Original Code is the ``Pathfinder'' system. The Initial
 *  Developer of the Original Code is the Database & Information
 *  Systems Group at the University of Konstanz, Germany. Portions
 *  created by U Konstanz are Copyright (C) 2000-2003 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

#include <limits.h>
#include <assert.h>

#include "pathfinder.h"
#include "abssyn.h"

/** twig-generated tokens (used for pattern matching) */
#include "normalize.symbols.h"

/** twig: type of pointer to abstract syntax tree node */
#define TWIG_NODE PFpnode_t

/** twig: max number of children under an abstract syntax tree node */
#define TWIG_MAXCHILD PFPNODE_MAXCHILD

static int TWIG_ID[] = {
    [p_plus]         plus,         /* binary + */
    [p_minus]        minus,        /* binary - */
    [p_mult]         mult,         /* * (multiplication) */
    [p_div]          div_,         /* div (division) */
    [p_idiv]         idiv,         /* idiv (integer division) */
    [p_mod]          mod,          /* mod */
    [p_and]          and,          /* and */
    [p_or]           or,           /* or */
    [p_lt]           lt,           /* < (less than) */
    [p_le]           le,           /* <= (less than or equal) */
    [p_gt]           gt,           /* > (greater than) */
    [p_ge]           ge,           /* >= (greater than or equal) */
    [p_eq]           eq,           /* = (equality) */
    [p_ne]           ne,           /* != (inequality) */
    [p_val_lt]       val_lt,       /* lt (value less than) */
    [p_val_le]       val_le,       /* le (value less than or equal) */
    [p_val_gt]       val_gt,       /* gt (value greater than) */
    [p_val_ge]       val_ge,       /* ge (value greter than or equal) */
    [p_val_eq]       val_eq,       /* eq (value equality) */
    [p_val_ne]       val_ne,       /* ne (value inequality) */
    [p_uplus]        uplus,        /* unary + */
    [p_uminus]       uminus,       /* unary - */
    [p_lit_int]      lit_int,      /* integer literal */
    [p_lit_dec]      lit_dec,      /* decimal literal */
    [p_lit_dbl]      lit_dbl,      /* double literal */
    [p_lit_str]      lit_str,      /* string literal */
    [p_is]           is,           /* is (node identity) */
    [p_nis]          nis,          /* isnot (negated node identity) *grin* */
    [p_step]         step,         /* axis step */
    [p_varref]       varref,       /* variable reference (unscoped) */
    [p_var]          var,          /* ``real'' scoped variable */
    [p_namet]        namet,        /* name test */
    [p_kindt]        kindt,        /* kind test */
    [p_locpath]      locpath,      /* location path */
    [p_root]         root_,        /* / (document root) */
    [p_dot]          dot,          /* current context node */
    [p_ltlt]         ltlt,         /* << (less than in doc order) */
    [p_gtgt]         gtgt,         /* >> (greater in doc order) */
    [p_flwr]         flwr,         /* for-let-where-return */
    [p_binds]        binds,        /* sequence of variable bindings */
    [p_nil]          nil,          /* end-of-sequence marker */
    [p_empty_seq]    empty_seq,    /* end-of-sequence marker */
    [p_bind]         bind,         /* for/some/every variable binding */
    [p_let]          let,          /* let binding */
    [p_exprseq]      exprseq,      /* e1, e2 (expression sequence) */
    [p_range]        range,        /* to (range) */
    [p_union]        union_,       /* union */
    [p_intersect]    intersect,    /* intersect */
    [p_except]       except,       /* except */
    [p_pred]         pred,         /* e1[e2] (predicate) */
    [p_if]           if_,          /* if-then-else */
    [p_some]         some,         /* some (existential quantifier) */
    [p_every]        every,        /* every (universal quantifier) */
    [p_orderby]      orderby,      /* order by */
    [p_orderspecs]   orderspecs,   /* order criteria */
    [p_instof]       instof,       /* instance of */
    [p_seq_ty]       seq_ty,       /* sequence type */
    [p_empty_ty]     empty_ty,     /* empty type */
    [p_node_ty]      node_ty,      /* node type */
    [p_item_ty]      item_ty,      /* item type */
    [p_atom_ty]      atom_ty,      /* named atomic type */
    [p_untyped_ty]   untyped_ty,   /* untyped type */
    [p_atomval_ty]   atomval_ty,   /* atomic value type */
    [p_named_ty]     named_ty,     /* named type */ 
    [p_req_ty]       req_ty,       /* required type */
    [p_req_name]     req_name,     /* required name */
    [p_typeswitch]   typeswitch,   /* typeswitch */
    [p_cases]        cases,        /* list of case branches */
    [p_case]         case_,        /* a case branch */
    [p_schm_path]    schm_path,    /* path of schema context steps */
    [p_schm_step]    schm_step,    /* schema context step */
    [p_glob_schm]    glob_schm,    /* global schema */
    [p_glob_schm_ty] glob_schm_ty, /* global schema type */
    [p_castable]     castable,     /* castable */
    [p_cast]         cast,         /* cast as */
    [p_treat]        treat,        /* treat as */
    [p_validate]     validate,     /* validate */
    [p_fun_ref]      fun_ref,      /* e1 (e2, ...) (function application) */
    [p_args]         args,         /* function argument list (actuals) */
    [p_char]         char_,        /* character content */
    [p_doc]          doc,          /* document constructor (document { }) */
    [p_elem]         elem,         /* XML element constructor */
    [p_attr]         attr,         /* XML attribute constructor */
    [p_text]         text,         /* XML text node constructor */
    [p_tag]          tag,          /* (fixed) tag name */
    [p_pi]           pi,           /* <?...?> content */
    [p_comment]      comment,      /* <!--...--> content */
    [p_xquery]       xquery,       /* root of the query parse tree */
    [p_prolog]       prolog,       /* query prolog */
    [p_decl_imps]    decl_imps,    /* list of declarations and imports */
    [p_xmls_decl]    xmls_decl,    /* xmlspace declaration */
    [p_coll_decl]    coll_decl,    /* default collation declaration */
    [p_ns_decl]      ns_decl,      /* namespace declaration */
    [p_fun_decls]    fun_decls,    /* list of function declarations */
    [p_fun_decl]     fun_decl,     /* function declaration */
    [p_ens_decl]     ens_decl,     /* default element namespace declaration */
    [p_fns_decl]     fns_decl,     /* default function namespace decl */
    [p_schm_imp]     schm_imp,     /* schema import */
    [p_params]       params,       /* list of (formal) function parameters */
    [p_param]        param         /* (formal) function parameter */
};

/** twig: setup twig */
#include "twig.h"

/* we define a function varref() later and don't want it to be
 * overridden by the twig definition for the node type.
 */
#undef varref

/**
 * Normalize abstract syntax tree.
 *
 * Remove ambiguities in abstract syntax tree and bring it into
 * normal form. This is done with twig based pattern matching that
 * rewrites the abstract syntax tree until no patterns can be found
 * that are not in normalized form.
 * @param r root of the abstract syntax tree (note that this
 *   might change during normalization)
 * @return the normalized abstract syntax tree
 */
PFpnode_t *
PFnormalize_abssyn (PFpnode_t *r)
{
    /* normalize (rewrite) tree */
    return rewrite (r, 0);
}

/**
 * Generate a parse tree node for a variable reference
 * @param q   QName for the variable
 * @param loc location information as #PFloc_t struct
 * @return    a parse tree node representing the variable
 */
static PFpnode_t *
varref (PFqname_t q, PFloc_t loc)
{
    PFpnode_t * n = p_leaf (p_varref, loc);
    
    n->sem.qname = q;
    
    return n;
}

/**
 * Generate a new (unique) variable name. This unique name is created
 * by incrementing a number that is appended to the variable name.
 * A prefix may be given that may not be longer than 3 characters
 * (the length of 'dot'). If no prefix is given (by passing NULL),
 * the default 'v' is used. The namespace for the newly created
 * variable is the internal Pathfinder namespace `pf:...' (PFns_pf).
 *
 * @param prefix The prefix to use. The variable name will be
 *   "[prefix]_[num]", where [prefix] is the prefix and [num] is
 *   a three digit, zero-padded integer number
 * @return A QName describing the new variable name
 */
static PFqname_t
new_varname (char *prefix)
{
    static unsigned int varcount = 0;
    char                varname[sizeof("dot_000")];
    PFqname_t           qn;
    
    /* construct new variable name */
    if (prefix)
        snprintf (varname, sizeof ("dot_000"), "%3s_%03u", prefix, varcount++);
    else
        snprintf (varname, sizeof ("dot_000"), "v_%03u", varcount++);
    
    /* assign internal Pathfinder namespace `pf:...' */
    qn.ns  = PFns_pf;
    qn.loc = PFstrdup (varname);
    
    return qn;
}


/**
 * Helper function to turn a left-deep into a right-deep chain of
 * `op's (the chain ends in a `nil' node).
 *
 * If @a a is a (chain of) `op' node(s), the trailing @c nil
 * node is replaced by @a b. Otherwise a new `op' node is returned
 * that concatenates @a a and @a b.
 *
 * @param op chain-building node type
 * @param a  first expression
 * @param b  second expression
 * @return   concatenation of @a a and @a b.
 */
static PFpnode_t *
concat (PFptype_t op, PFpnode_t *a, PFpnode_t *b)
{
    PFloc_t   loc;
    PFptype_t terminal;   /* node type that terminates chain */
    
    switch (op) {
    case p_exprseq:   terminal = p_empty_seq; break;
    default:          terminal = p_nil; break;
    }
    
    if (a->kind == terminal)
        return b;
    
    if (a->kind == op) {
        a->child[1] = concat (op, a->child[1], b);
        return a;
    }
    
    /* New location is maximum range from both operands */
    if (a->loc.first_row == b->loc.first_row) {
        loc.first_row = a->loc.first_row;
        if (a->loc.first_col <= b->loc.first_col) {
            loc.first_col = a->loc.first_col;
            loc.last_row = b->loc.last_row;
            loc.last_col = b->loc.last_col;
        }
        else {
            loc.first_col = b->loc.first_col;
            loc.last_row = a->loc.last_row;
            loc.last_col = a->loc.last_col;
        }
    }
    else if (a->loc.first_row < b->loc.first_row) {
        loc.first_row = a->loc.first_row;
        loc.first_col = a->loc.first_col;
        loc.last_row = b->loc.last_row;
        loc.last_col = b->loc.last_col;
    }
    else if (a->loc.first_row > b->loc.first_row) {
        loc.first_row = b->loc.first_row;
        loc.first_col = b->loc.first_col;
        loc.last_row = a->loc.last_row;
        loc.last_col = a->loc.last_col;
    }
    
    return p_wire2 (op, loc, a, b);
}

/* vim:set shiftwidth=4 expandtab: */
