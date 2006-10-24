/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Declarations for core language tree.
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

#ifndef CORE_H
#define CORE_H

/* PFvar_t */
#include "variable.h"

/* PFty_t */
#include "types.h"

typedef struct PFcnode_t PFcnode_t;

/* global for core_new_var (TODO REMOVE) */
extern unsigned int core_vars;

/* PFfun_t */
#include "functions.h"

/* PFapply_t */
#include "apply.h"

/* PFla_pair_t */
#include "logical.h"

/** Maximum number of children of a core tree node */
#define PFCNODE_MAXCHILD 2

/**
 * Core tree node type indicators
 *
 * @warning
 *   This enumeration appears in various files within the project
 *   (primarily in switch() statements or array initializers).
 *   If you make modifications to this enum, make sure you also
 *   adapt
 *    - core/simplify.brg
 *    - semantics/typecheck.brg
 *    - core/coreopt.brg
 *    - debug/coreprint.c
 *    - algebra/core2alg.brg
 *    - mil/milprint_summer.c
 */
enum PFctype_t {
    c_var                =  1 /**< variable */
  , c_lit_str            =  2 /**< string literal */
  , c_lit_int            =  3 /**< integer literal */
  , c_lit_dec            =  4 /**< decimal literal */
  , c_lit_dbl            =  5 /**< double literal */
  , c_nil                =  6 /**< end-of-sequence marker */

  , c_seq                =  7 /**< sequence construction */
  , c_ordered            =  8
  , c_unordered          =  9

  , c_flwr               = 10 /**< flwr expression */
  , c_let                = 11 /**< let expression */
  , c_letbind            = 12 /**< binding part of a let expression */
  , c_for                = 13 /**< for expression */
  , c_forbind            = 14 /**< binding part of a for expression */
  , c_forvars            = 15 /**< variable pair (var + pos. var) of a for */

  , c_orderby            = 16 /**< orderby clause */
  , c_orderspecs         = 17 /**< list of order specs */

  , c_apply              = 18 /**< function application */
  , c_arg                = 19 /**< function argument (list) */

  , c_typesw             = 20 /**< typeswitch clause */
  , c_cases              = 21 /**< case concatenation for typeswitch */
  , c_case               = 22 /**< single case for typeswitch */
  , c_default            = 23 /**< default branch in typeswitch */
  , c_seqtype            = 24 /**< a SequenceType */
  , c_seqcast            = 25 /**< cast along <: */
  , c_proof              = 26 /**< typechecker only: prove <: relationship */
  , c_subty              = 27 /**< subtype condition for proof() */
  , c_stattype           = 28 /**< static type of expression. Required for
                                fs:convert-operand. Will be removed during
                                typechecking and replaced by seqtype, as
                                soon as types are known. */

  , c_if                 = 29 /**< if-then-else conditional */
  , c_then_else          = 30 /**< then- and else-branches of an
                                if-then-else conditional */

  , c_locsteps           = 31 /**< path of location steps only */

  /* XPath axes */
  , c_ancestor           = 32 /**< the parent, the parent's parent,... */
  , c_ancestor_or_self   = 33 /**< the parent, the parent's parent,... + self */
  , c_attribute          = 34 /**< attributes of the context node */
  , c_child              = 35 /**< children of the context node */
  , c_descendant         = 36 /**< children, children's children,... + self */
  , c_descendant_or_self = 37 /**< children, children's children,... */
  , c_following          = 38 /**< nodes after current node (document order) */
  , c_following_sibling  = 39 /**< all following nodes with same parent */
  , c_parent             = 40 /**< parent node (exactly one or none) */
  , c_preceding          = 41 /**< nodes before context node (document order) */
  , c_preceding_sibling  = 42 /**< all preceding nodes with same parent */
  , c_self               = 43 /**< the context node itself */
/* [STANDOFF] */
  , c_select_narrow      = 100 /**< regions contained in a context-node */
  , c_select_wide        = 101 /**< regions overlapping a context-node */
  , c_reject_narrow      = 102 /**< all regions except the contained regions */
  , c_reject_wide        = 103 /**< all regions except the overlapping regions */
/* [/STANDOFF] */

  /* Constructor Nodes */
  , c_elem               = 44 /**< the element constructor */
  , c_attr               = 45 /**< the attribute constructor */
  , c_text               = 46 /**< the text constructor */
  , c_doc                = 47 /**< the document constructor */
  , c_comment            = 48 /**< the comment constructor */
  , c_pi                 = 49 /**< the processing-instruction constructor */
  , c_tag                = 50 /**< the tagname for elem. and attr. constr. */

  , c_true               = 51 /**< built-in function `fn:true ()' */
  , c_false              = 52 /**< built-in function `fn:false ()' */
  , c_empty              = 53 /**< built-in function `empty' */

  , c_main               = 54 /**< tree root.
                                   Separates function declarations from
                                   the query body. */
  , c_fun_decls          = 55 /**< list of function declarations */
  , c_fun_decl           = 56 /**< function declaration */
  , c_params             = 57 /**< function declaration parameter list */
  , c_param              = 58 /**< function declaration parameter */
  , c_cast               = 59 /**< function declaration parameter */

  /* Pathfinder extension: recursion */
  , c_recursion          = 60 /**< "with $v seeded by..." expression */
  , c_seed               = 61

  /* Pathfinder extension: XRPC */
  , c_xrpc               = 62 /**< XRPC calls: "execute at" */
};

/** Core tree node type indicators */
typedef enum PFctype_t PFctype_t;

/** Semantic node content of core tree node */
union PFcsem_t {
    long long int  num;        /**< integer value */
    double         dec;        /**< decimal value */
    double         dbl;        /**< double value */
    bool           tru;        /**< truth value (boolean) */
    char          *str;        /**< string value */
    char           chr;        /**< character value */
    PFqname_t      qname;      /**< qualified name */
    PFvar_t       *var;        /**< variable information */
    PFty_t         type;       /**< used with c_type */
    PFsort_t       mode;       /**< sort modifier */
    PFapply_t      apply;      /**< function application */

    /* semantic content for flwr subexpressions (let/for) */
    struct {
        PFty_t (*quantifier) (PFty_t); 
                           /**< quantifier for flwor return expression */
        int    fid;        /**< for loop id (used in milprint_summer.c) */
    } flwr;
};

/** Semantic node content of core tree node */
typedef union PFcsem_t PFcsem_t;

/** struct representing a core tree node */
struct PFcnode_t {
    PFctype_t   kind;                    /**< node kind indicator */
    PFcsem_t    sem;                     /**< semantic node information */
    PFcnode_t  *child[PFCNODE_MAXCHILD]; /**< child nodes */
    PFty_t      type;                    /**< static type */
    struct PFla_pair_t alg;
    short       state_label;             /**< for BURG pattern matcher */
};


/**
 * We call everything an atom that is
 * - a constant
 * - a variable
 * - the literal empty sequence.
 * Use this macro only with core tree nodes as an argument!
 */
#define IS_ATOM(n) ((n)                                 \
                    && ((n)->kind == c_lit_str          \
                        || (n)->kind == c_lit_int       \
                        || (n)->kind == c_lit_dec       \
                        || (n)->kind == c_lit_dbl       \
                        || (n)->kind == c_true          \
                        || (n)->kind == c_false         \
                        || (n)->kind == c_empty         \
                        || (n)->kind == c_var))


/* PFp..._t */
#include "abssyn.h"

/** 
 * Core constructor functions below.
 */
PFcnode_t *PFcore_leaf (PFctype_t);
PFcnode_t *PFcore_wire1 (PFctype_t, 
                         const PFcnode_t *);
PFcnode_t *PFcore_wire2 (PFctype_t, 
                         const PFcnode_t *, const PFcnode_t *);

PFcnode_t *PFcore_nil (void);

PFvar_t *PFcore_new_var (char *);
PFcnode_t *PFcore_var (PFvar_t *);

PFcnode_t *PFcore_num (long long int);
PFcnode_t *PFcore_dec (double);
PFcnode_t *PFcore_dbl (double);
PFcnode_t *PFcore_str (char *);

PFcnode_t *PFcore_main (const PFcnode_t *, const PFcnode_t *);

PFcnode_t *PFcore_seqtype (PFty_t);
PFcnode_t *PFcore_seqcast (const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_proof (const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_subty (const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_stattype (const PFcnode_t *);
PFcnode_t *PFcore_typeswitch (const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_case (const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_cases (const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_default (const PFcnode_t *);

PFcnode_t *PFcore_if (const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_then_else (const PFcnode_t *, const PFcnode_t *);

PFcnode_t *PFcore_flwr (const PFcnode_t *, const PFcnode_t *);

PFcnode_t *PFcore_for (const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_forbind (const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_forvars (const PFcnode_t *, const PFcnode_t *);

PFcnode_t *PFcore_let (const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_letbind (const PFcnode_t *, const PFcnode_t *);

PFcnode_t *PFcore_orderby (bool, const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_orderspecs (PFsort_t, const PFcnode_t *, const PFcnode_t *);

PFcnode_t *PFcore_seq (const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_ordered (const PFcnode_t *);
PFcnode_t *PFcore_unordered (const PFcnode_t *);
PFcnode_t *PFcore_empty (void);

PFcnode_t *PFcore_true (void);
PFcnode_t *PFcore_false (void);

PFcnode_t *PFcore_locsteps (const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_step (PFpaxis_t, const PFcnode_t *);

PFcnode_t *PFcore_constr_elem (const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_constr_attr (const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_constr_pi (const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_constr (PFptype_t, const PFcnode_t *);
PFcnode_t *PFcore_tag (PFqname_t);
PFcnode_t *PFcore_fun_decl (PFfun_t *fun, const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_fun_decls (const PFcnode_t *fun, const PFcnode_t *funs);
PFcnode_t *PFcore_params (const PFcnode_t *param, const PFcnode_t *params);
PFcnode_t *PFcore_param (const PFcnode_t *type, const PFcnode_t *var);
PFcnode_t *PFcore_cast (const PFcnode_t *type, const PFcnode_t *expr);

PFcnode_t *PFcore_recursion (const PFcnode_t *var,
                             const PFcnode_t *seed_recurse);
PFcnode_t *PFcore_seed (const PFcnode_t *seed, const PFcnode_t *recurse);

PFcnode_t *PFcore_xrpc (const PFcnode_t *uri, const PFcnode_t *fun);

PFfun_t *PFcore_function (PFqname_t);
PFcnode_t *PFcore_apply (PFapply_t *, const PFcnode_t *);
PFcnode_t *PFcore_arg (const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_apply_ (PFapply_t *, ...);

/**
 * Expansion functions for Calculations
 */
PFcnode_t *PFcore_fn_data (const PFcnode_t *);
PFcnode_t *PFcore_fs_convert_op_by_type (const PFcnode_t *, PFty_t);
PFcnode_t *PFcore_fs_convert_op_by_expr (const PFcnode_t *, const PFcnode_t *);
PFcnode_t *PFcore_some (const PFcnode_t *, const PFcnode_t *, const PFcnode_t *);

/**
 * Wrapper for #apply_.
 */
#define APPLY(fn,...) PFcore_apply_ (PFapply(fn), __VA_ARGS__, 0)

PFcnode_t *PFcore_ebv (const PFcnode_t *);

PFcnode_t *PFcore_error (const char *, ...);
PFcnode_t *PFcore_error_loc (PFloc_t, const char *, ...);

#endif   /* CORE_H */

/* vim:set shiftwidth=4 expandtab: */
