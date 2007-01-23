/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Access and helper functions for abstract syntax tree (declarations)
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

#ifndef ABSSYN_H
#define ABSSYN_H

/* PFqname_t */
#include "qname.h"    

/* PFns_map_t */
#include "ns.h"

/* PFvar_t */
#include "variable.h"    

/** no type of parse tree node will need more than
 *  this many child nodes 
 */
#define PFPNODE_MAXCHILD 2

/** parse tree node type indicators */
enum PFptype_t {
      p_and              =   1  /**< and */
    , p_apply            =   2  /**< function application (``scoped'') */
    , p_args             =   3  /**< function argument list (actuals) */
    , p_atom_ty          =   4  /**< named atomic type */
    , p_attr             =   5  /**< XML attribute constructor */
    , p_base_uri         =   6  /**< `declare base-uri' */
    , p_bind             =   7  /**< for/some/every variable binding */
    , p_binds            =   8  /**< sequence of variable bindings */
    , p_case             =   9  /**< a case branch */
    , p_cases            =  10  /**< list of case branches */
    , p_cast             =  11  /**< cast as */
    , p_castable         =  12  /**< castable */
    , p_coll_decl        =  13  /**< default collation declaration */
    , p_comment          =  14  /**< <!--...--> content */
    , p_constr_decl      =  15  /**< `declare construction preserve' */
    , p_contseq          =  16  /**< content sequence (in constructors) */
    , p_decl_imps        =  17  /**< list of declarations and imports */
    , p_def_order        =  18  /**< default order (empty greatest/least) */
    , p_default          =  19  /**< `default' clause in typeswitches */
    , p_div              =  20  /**< div (division) */
    , p_doc              =  21  /**< document constructor (document { }) */
    , p_dot              =  22  /**< current context node */
    , p_elem             =  23  /**< XML element constructor */
    , p_empty_seq        =  24  /**< the empty sequence */
    , p_empty_ty         =  25  /**< empty type */
    , p_ens_decl         =  26  /**< default element namespace declaration */
    , p_eq               =  27  /**< = (equality) */
    , p_every            =  28  /**< every (universal quantifier) */
    , p_except           =  29  /**< except */
    , p_exprseq          =  30  /**< e1, e2 (expression sequence) */
    , p_external         =  31  /**< keyword external: var/fun def'd external */
    , p_ext_expr         =  32  /**< extension expression (pragma) */
    , p_flwr             =  33  /**< for-let-where-return */
    , p_fns_decl         =  34  /**< default function namespace declaration */
    , p_fun              =  35  /**< function decl. (after fun. ``scoping'') */
    , p_fun_decl         =  36  /**< function declaration (yet ``unscoped'') */
    , p_fun_ref          =  37  /**< function appl. (not ``scoped'' yet) */
    , p_fun_sig          =  38  /**< function signature (parameters and type) */
    , p_ge               =  39  /**< >= (greater than or equal) */
    , p_gt               =  40  /**< > (greater than) */
    , p_gtgt             =  41  /**< >> (greater in doc order) */
    , p_idiv             =  42  /**< idiv (integer division) */
    , p_if               =  43  /**< if-then-else */
    /* FIXME , p_inherit_ns       =  43  **< inherit-namespaces */
    , p_copy_ns          =  44  /**< copy-namespaces */
    , p_instof           =  45  /**< instance of */
    , p_intersect        =  46  /**< intersect */
    , p_is               =  47  /**< is (node identity) */
    , p_item_ty          =  48  /**< item type */
    , p_le               =  49  /**< <= (less than or equal) */
    , p_let              =  50  /**< let binding */
    , p_lib_mod          =  51  /**< library module */
    , p_lit_dbl          =  52  /**< double literal */
    , p_lit_dec          =  53  /**< decimal literal */
    , p_lit_int          =  54  /**< integer literal */
    , p_lit_str          =  55  /**< string literal */
    , p_locpath          =  56  /**< location path */
    , p_lt               =  57  /**< < (less than) */
    , p_ltlt             =  58  /**< << (less than in doc order) */
    , p_main_mod         =  59  /**< main module */
    , p_minus            =  60  /**< binary - */
    , p_mod              =  61  /**< mod */
    , p_mod_imp          =  62  /**< Module import */
    , p_mod_ns           =  63  /**< module namespace */
    , p_mult             =  64  /**< * (multiplication) */
    , p_named_ty         =  65  /**< named type */ 
    , p_ne               =  66  /**< != (inequality) */
    , p_nil              =  67  /**< end-of-sequence marker */
    , p_node_ty          =  68  /**< node type */
    , p_ns_decl          =  69  /**< namespace declaration */
    , p_or               =  70  /**< or */
    , p_ord_ret          =  71  /**< `order by'/`return' in FLWOR clauses */
    , p_orderby          =  72  /**< FLWOR `orderby' clause */
    , p_ordered          =  73  /**< keyword `ordered {...}' */
    , p_ordering_mode    =  74  /**< ordering mode declaration */
    , p_orderspecs       =  75  /**< list of order specifiers (in FLWORs) */
    , p_param            =  76  /**< (formal) function parameter */
    , p_params           =  77  /**< list of (formal) function parameters */
    , p_pi               =  78  /**< <?...?> content */
    , p_plus             =  79  /**< binary + */
    , p_pragma           =  80  /**< pragma */
    , p_pragmas          =  81  /**< list of pragmas */
    , p_pred             =  82  /**< e1[e2] (predicate) */
    , p_range            =  83  /**< to (range) */
    , p_req_name         =  84  /**< required name */
    , p_req_ty           =  85  /**< required type */
    , p_root             =  86  /**< / (document root) */
    , p_schm_ats         =  87  /**< list of `at StringLit' in schema imp. */
    , p_schm_attr        =  88  /**< `schema-attribute()' test */
    , p_schm_elem        =  89  /**< `schema-element()' test */
    , p_schm_imp         =  90  /**< schema import */
    , p_seq_ty           =  91  /**< sequence type */
    , p_some             =  92  /**< some (existential quantifier) */
    , p_step             =  93  /**< axis step */
    , p_tag              =  94  /**< (fixed) tag name */
    , p_text             =  95  /**< XML text node constructor */
    , p_then_else        =  96  /**< `then' and `else' in if-then-else */
    , p_treat            =  97  /**< treat as */
    , p_typeswitch       =  98  /**< typeswitch */
    , p_uminus           =  99  /**< unary - */
    , p_union            = 100  /**< union */
    , p_unordered        = 101  /**< keyword `unordered {...}' */
    , p_uplus            = 102  /**< unary + */
    , p_val_eq           = 103  /**< eq (value equality) */
    , p_val_ge           = 104  /**< ge (value greter than or equal) */
    , p_val_gt           = 105  /**< gt (value greater than) */
    , p_val_le           = 106  /**< le (value less than or equal) */
    , p_val_lt           = 107  /**< lt (value less than) */
    , p_val_ne           = 108  /**< ne (value inequality) */
    , p_validate         = 109  /**< validate */
    , p_var              = 110  /**< ``real'' scoped variable */
    , p_var_decl         = 111  /**< variable declaration */
    , p_var_type         = 112  /**< variable/type combination */
    , p_varref           = 113  /**< variable reference (no scoping yet) */
    , p_vars             = 114  /**< parent of two variables in FLWORs */
    , p_where            = 115  /**< FLWOR `where' clause */
    , p_boundspc_decl    = 116  /**< xmlspace declaration */
    , p_revalid          = 117  /**< revalidation declaration (XQ Updt) */
    , p_insert           = 118  /**< "do insert" expression (XQ Updt) */
    , p_delete           = 119  /**< "do delete" expression (XQ Updt) */
    , p_replace          = 120  /**< "do replace" expression (XQ Updt) */
    , p_rename           = 121  /**< "do rename" expression (XQ Updt) */
    , p_transform        = 122  /**< "transform copy $" expressions (XQ Updt) */
    , p_modify           = 123  /**< modify and return parts of transform */
    , p_transbinds       = 124  /**< variable bindings in a "transform" */
    , p_stmt_ty          = 125  /**< "item" type `stmt' (funct. decls.) */

    /* Pathfinder extension: recursion */
    , p_recursion        = 126  /**< "with $xx seeded by..." expression */
    , p_seed             = 127  /**< seed and return expr of a recursive expr */

    /* Pathfinder extension: XRPC */
    , p_xrpc             = 128  /**< XRPC: "execute at" */
};

typedef enum PFptype_t PFptype_t;

/** XQuery (XPath) axes */
enum PFpaxis_t {
    p_ancestor,           /**< the parent, the parent's parent,... */
    p_ancestor_or_self,   /**< the parent, the parent's parent,... + self */
    p_attribute,          /**< attributes of the context node */
    p_child,              /**< children of the context node */
    p_descendant,         /**< children, children's children,... + self */
    p_descendant_or_self, /**< children, children's children,... */
    p_following,          /**< nodes after current node (document order) */
    p_following_sibling,  /**< all following nodes with same parent */
    p_parent,             /**< parent node (exactly one or none) */
    p_preceding,          /**< nodes before context node (document order) */
    p_preceding_sibling,  /**< all preceding nodes with same parent */
    p_self                /**< the context node itself */
/* [STANDOFF] */
,
    p_select_narrow,      /**< regions contained in a context-node */
    p_select_wide,        /**< regions overlapping a context-node */
    p_reject_narrow,      /**< all regions except the contained regions */
    p_reject_wide         /**< all regions except the overlapping regions */
/* [/STANDOFF] */
};

typedef enum PFpaxis_t PFpaxis_t;

/** XML node kinds */
enum PFpkind_t {
    p_kind_node,
    p_kind_comment,
    p_kind_text,
    p_kind_pi,
    p_kind_doc,
    p_kind_elem,
    p_kind_attr
};

/** XML node kinds */
typedef enum PFpkind_t PFpkind_t;

/** XQuery sequence type occurrence indicator (see W3C XQuery, 2.1.3.2) */
enum PFpoci_t {
  p_one,           /**< exactly one (no indicator) */
  p_zero_or_one,   /**< ? */
  p_zero_or_more,  /**< * */
  p_one_or_more    /**< + */
};

typedef enum PFpoci_t PFpoci_t;

/** target for an "do insert" update statement */
enum PFinsertmod_t {
      p_first_into
    , p_last_into
    , p_into
    , p_after
    , p_before
};
typedef enum PFinsertmod_t PFinsertmod_t;

/** XQuery parse tree node
 */
typedef struct PFpnode_t PFpnode_t;

/** semantic node information
 */
typedef union PFpsem_t PFpsem_t;

union PFpsem_t {
    long long int    num;        /**< integer value */
    double           dec;        /**< decimal value */
    double           dbl;        /**< double value */
    bool             tru;        /**< truth value (boolean) */
    char            *str;        /**< string value */
    char             chr;        /**< character value */
    PFqname_raw_t    qname_raw;  /**< qualified name (before ns resolution) */
    PFqname_t        qname;      /**< qualified name (after ns resolution) */
    PFpaxis_t        axis;       /**< XPath axis */
    PFpkind_t        kind;       /**< node kind */
    PFsort_t         mode;       /**< sort modifier */
    PFpoci_t         oci;        /**< occurrence indicator */
    struct {
        bool   preserve;   /**< copy-namespace preserve/no-preserve */
        bool   inherit;    /**< copy-namespace inherit/no-inherit */
    } copy_ns;             /**< "declare copy-namespaces" setting */
    PFempty_order_t  empty; /**< empty ordering declaration */

    PFvar_t         *var;        /**< variable information
                                      (used after var scoping) */

    struct PFfun_t  *fun;      /**< function application information
                                      (used after fun checks) */
    PFpragma_t       pragma;     /**< information given in a pragma
                                      (# qn cont #) */
    PFrevalidation_t revalid;    /**< revalidation declaration
                                      (XQ Update Facility) */
    PFinsertmod_t    insert;     /**< insertion target for "do insert"
                                      (XQ Update Facility) */
}; 


/* interfaces to parse construction routines 
 */
PFpnode_t *
p_leaf  (PFptype_t type, PFloc_t loc);

PFpnode_t *
p_wire1 (PFptype_t type, PFloc_t loc,
	 PFpnode_t *n1);

PFpnode_t *
p_wire2 (PFptype_t type, PFloc_t loc,
	 PFpnode_t *n1, PFpnode_t *n2);

PFpnode_t *
p_wire3 (PFptype_t type, PFloc_t loc,
	 PFpnode_t *n1, PFpnode_t *n2, PFpnode_t *n3);

PFpnode_t *
p_wire4 (PFptype_t type, PFloc_t loc,
	 PFpnode_t *n1, PFpnode_t *n2, PFpnode_t *n3, PFpnode_t *n4);


struct PFpnode_t {
  PFptype_t         kind;              /**< node kind */
  PFpsem_t          sem;               /**< semantic node information */
  PFpnode_t        *child[PFPNODE_MAXCHILD];  /**< child node list */
  PFloc_t           loc;               /**< textual location of this node */
  struct PFcnode_t *core;              /**< pointer to core representation */
  short             state_label;       /**< for BURG pattern matcher */
};

/*
 * In several cases, the semantic actions of a grammar rule cannot
 * construct a complete abstract syntax tree, e.g., consider the
 * generation of a right-deep abstract syntax tree from a left-recursive
 * grammar rule.
 *
 * Whenever such a situation arises, we let the semantic action
 * construct as much of the tree as possible with parts of the tree
 * unspecified.  The semantic action then returns the ROOT of this
 * tree as well as pointer to the node under which the yet unspecified
 * tree part will reside; this node is subsequently referred to as the
 * `hole'.
 */
struct phole_t {
    PFpnode_t *root;
    PFpnode_t *hole;
};

#endif  /* ABSSYN_H */

/* vim:set shiftwidth=4 expandtab: */
