/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Access and helper functions for abstract syntax tree (declarations)
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
 *  created by U Konstanz are Copyright (C) 2000-2004 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

#ifndef ABSSYN_H
#define ABSSYN_H

/* PFqname_t */
#include "qname.h"    

/* PFvar_t */
#include "variable.h"    

/* PFfun_t */
/*
#include "functions.h"
*/

/** no type of parse tree node will need more than
 *  this many child nodes 
 */
#define PFPNODE_MAXCHILD 4

/** parse tree node type indicators */
enum PFptype_t {
  p_plus,         /**< binary + */
  p_minus,        /**< binary - */
  p_mult,         /**< * (multiplication) */
  p_div,          /**< div (division) */
  p_idiv,         /**< idiv (integer division) */
  p_mod,          /**< mod */
  p_and,          /**< and */
  p_or,           /**< or */
  p_lt,           /**< < (less than) */
  p_le,           /**< <= (less than or equal) */
  p_gt,           /**< > (greater than) */
  p_ge,           /**< >= (greater than or equal) */
  p_eq,           /**< = (equality) */
  p_ne,           /**< != (inequality) */
  p_val_lt,       /**< lt (value less than) */
  p_val_le,       /**< le (value less than or equal) */
  p_val_gt,       /**< gt (value greater than) */
  p_val_ge,       /**< ge (value greter than or equal) */
  p_val_eq,       /**< eq (value equality) */
  p_val_ne,       /**< ne (value inequality) */
  p_uplus,        /**< unary + */
  p_uminus,       /**< unary - */
  p_lit_int,      /**< integer literal */
  p_lit_dec,      /**< decimal literal */
  p_lit_dbl,      /**< double literal */
  p_lit_str,      /**< string literal */
  p_is,           /**< is (node identity) */
  p_nis,          /**< isnot (negated node identity) *grin* */
  p_step,         /**< axis step */
  p_varref,       /**< variable reference (no scoping yet) */
  p_var,          /**< ``real'' scoped variable */
  p_namet,        /**< name test */
  p_kindt,        /**< kind test */
  p_locpath,      /**< location path */
  p_root,         /**< / (document root) */
  p_dot,          /**< current context node */
  p_ltlt,         /**< << (less than in doc order) */
  p_gtgt,         /**< >> (greater in doc order) */
  p_flwr,         /**< for-let-where-return */
  p_binds,        /**< sequence of variable bindings */
  p_nil,          /**< end-of-sequence marker (*NOT* the empty sequence) */
  p_bind,         /**< for/some/every variable binding */
  p_empty_seq,    /**< the empty sequence */
  p_let,          /**< let binding */
  p_exprseq,      /**< e1, e2 (expression sequence) */
  p_range,        /**< to (range) */
  p_union,        /**< union */
  p_intersect,    /**< intersect */
  p_except,       /**< except */
  p_pred,         /**< e1[e2] (predicate) */
  p_if,           /**< if-then-else */
  p_some,         /**< some (existential quantifier) */
  p_every,        /**< every (universal quantifier) */
  p_orderby,      /**< order by */
  p_orderspecs,   /**< order criteria */
  p_instof,       /**< instance of */
  p_seq_ty,       /**< sequence type */
  p_empty_ty,     /**< empty type */
  p_node_ty,      /**< node type */
  p_item_ty,      /**< item type */
  p_atom_ty,      /**< named atomic type */
  p_atomval_ty,   /**< atomic value type */
  p_named_ty,     /**< named type */ 
  p_req_ty,       /**< required type */
  p_req_name,     /**< required name */
  p_typeswitch,   /**< typeswitch */
  p_cases,        /**< list of case branches */
  p_case,         /**< a case branch */
  p_schm_path,    /**< path of schema context steps */
  p_schm_step,    /**< schema context step */
  p_glob_schm,    /**< global schema */
  p_glob_schm_ty, /**< global schema type */
  p_castable,     /**< castable */
  p_cast,         /**< cast as */
  p_treat,        /**< treat as */
  p_validate,     /**< validate */
  p_char,         /**< character content */
  p_doc,          /**< document constructor (document { }) */
  p_elem,         /**< XML element constructor */
  p_attr,         /**< XML attribute constructor */
  p_text,         /**< XML text node constructor */
  p_tag,          /**< (fixed) tag name */
  p_pi,           /**< <?...?> content */
  p_comment,      /**< <!--...--> content */
  p_contseq,      /**< content sequence (in constructors) */
  p_xquery,       /**< root of the query parse tree */
  p_prolog,       /**< query prolog */
  p_decl_imps,    /**< list of declarations and imports */
  p_xmls_decl,    /**< xmlspace declaration */
  p_coll_decl,    /**< default collation declaration */
  p_ns_decl,      /**< namespace declaration */
  p_fun_ref,      /**< function application (not ``scoped'' yet) */
  p_apply,        /**< function application (``scoped'') */
  p_args,         /**< function argument list (actuals) */
  p_fun_decls,    /**< list of function declarations */
  p_fun_decl,     /**< function declaration (yet ``unscoped'') */
  p_fun,          /**< function declaration (after function ``scoping'') */
  p_ens_decl,     /**< default element namespace declaration */
  p_fns_decl,     /**< default function namespace declaration */
  p_schm_imp,     /**< schema import */
  p_params,       /**< list of (formal) function parameters */
  p_param         /**< (formal) function parameter */
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

/** XQuery `order by' modifier (see W3C XQuery, 3.8.3) */
typedef struct PFpsort_t PFpsort_t;

struct PFpsort_t {
  enum { p_asc, p_desc }       dir;     /**< ascending/descending */
  enum { p_greatest, p_least } empty;   /**< empty greatest/empty least */
  char                        *coll;    /**< collation (may be 0) */
};


/** XQuery parse tree node
 */
typedef struct PFpnode_t PFpnode_t;

/** semantic node information
 */
typedef union PFpsem_t PFpsem_t;

union PFpsem_t {
  int        num;        /**< integer value */
  double     dec;        /**< decimal value */
  double     dbl;        /**< double value */
  bool       tru;        /**< truth value (boolean) */
  char      *str;        /**< string value */
  char       chr;        /**< character value */
  PFqname_t  qname;      /**< qualified name */
  PFpaxis_t  axis;       /**< XPath axis */
  PFpkind_t  kind;       /**< node kind */
  PFpsort_t  mode;       /**< sort modifier */
  PFpoci_t   oci;        /**< occurrence indicator */

  PFvar_t   *var;        /**< variable information (used after var scoping) */

  struct PFfun_t *fun;   /**< function information (used after fun checks) */
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
  PFptype_t kind;                      /**< node kind */
  PFpsem_t  sem;                       /**< semantic node information */
  PFpnode_t *child[PFPNODE_MAXCHILD];  /**< child node list */
  PFloc_t   loc;                       /**< textual location of this node */
  struct PFcnode_t *core;              /**< pointer to core representation */
};

#endif  /* ABSSYN_H */

/* vim:set shiftwidth=4 expandtab: */
