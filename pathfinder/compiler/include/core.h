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

/*
 * NOTE (Revision Information):
 *
 * Changes in the Core2MIL_Summer2004 branch have been merged into
 * this file on July 15, 2004. I have tagged this file in the
 * Core2MIL_Summer2004 branch with `merged-into-main-15-07-2004'.
 *
 * For later merges from the Core2MIL_Summer2004, please only merge
 * the changes since this tag.
 *
 * Jens
 */

#ifndef CORE_H
#define CORE_H

/* PFvar_t */
#include "variable.h"

/* PFty_t */
#include "types.h"

typedef struct PFcnode_t PFcnode_t;

/* PFfun_t */
#include "functions.h"

/** Maximum number of children of a core tree node */
#define PFCNODE_MAXCHILD 4

/**
 * Core tree node type indicators
 *
 * @warning
 *   This enumeration appears in various files within the project
 *   (primarily in switch() statements or array initializers).
 *   If you make modifications to this enum, make sure you also
 *   adapt
 *    - the twig grammar rules in core/simplify.mt
 *    - the twig grammar rules in semantics/typecheck.mt
 *    - #c_id in debug/coreprint.c
 *    - #core2mil() in mil/core2mil.c
 */
enum PFctype_t {
    c_var                     /**< variable */
  , c_lit_str                 /**< string literal */
  , c_lit_int                 /**< integer literal */
  , c_lit_dec                 /**< decimal literal */
  , c_lit_dbl                 /**< double literal */
  , c_nil                     /**< end-of-sequence marker */

  , c_seq                     /**< sequence construction */

  , c_let                     /**< let binding */
  , c_for                     /**< for binding */

  , c_apply                   /**< function application */
  , c_arg                     /**< function argument (list) */

  , c_typesw                  /**< typeswitch clause */
  , c_cases                   /**< case concatenation for typeswitch */
  , c_case                    /**< single case for typeswitch */
  , c_seqtype                 /**< a SequenceType */
  , c_seqcast                 /**< cast along <: */
  , c_proof                   /**< typechecker only: prove <: relationship */
  , c_stattype                /**< static type of expression. Required for
                                   fs:convert-operand. Will be removed during
                                   typechecking and replaced by seqtype, as
                                   soon as types are known. */

  , c_ifthenelse              /**< if-then-else conditional */

  , c_locsteps                /**< path of location steps only */

  /* XPath axes */
  , c_ancestor              /**< the parent, the parent's parent,... */
  , c_ancestor_or_self      /**< the parent, the parent's parent,... + self */
  , c_attribute             /**< attributes of the context node */
  , c_child                 /**< children of the context node */
  , c_descendant            /**< children, children's children,... + self */
  , c_descendant_or_self    /**< children, children's children,... */
  , c_following             /**< nodes after current node (document order) */
  , c_following_sibling     /**< all following nodes with same parent */
  , c_parent                /**< parent node (exactly one or none) */
  , c_preceding             /**< nodes before context node (document order) */
  , c_preceding_sibling     /**< all preceding nodes with same parent */
  , c_self                  /**< the context node itself */

  , c_kind_node
  , c_kind_comment
  , c_kind_text
  , c_kind_pi
  , c_kind_doc
  , c_kind_elem
  , c_kind_attr

  , c_namet                   /**< name test */

  /* Constructor Nodes */
  , c_elem                    /**< the element constructor */
  , c_attr                    /**< the attribute constructor */
  , c_text                    /**< the text constructor */
  , c_doc                     /**< the document constructor */
  , c_comment                 /**< the comment constructor */
  , c_pi                      /**< the processing-instruction constructor */
  , c_tag                     /**< the tagname for element and attribute constructors */

  , c_true                    /**< built-in function `fn:true ()' */
  , c_false                   /**< built-in function `fn:false ()' */
  , c_error                   /**< built-in function `error' */
  , c_root                    /**< built-in function `root' */
  , c_empty                   /**< built-in function `empty' */
};

/** Core tree node type indicators */
typedef enum PFctype_t PFctype_t;

/** Semantic node content of core tree node */
union PFcsem_t {
  int        num;        /**< integer value */
  double     dec;        /**< decimal value */
  double     dbl;        /**< double value */
  bool       tru;        /**< truth value (boolean) */
  char      *str;        /**< string value */
  char       chr;        /**< character value */
  PFqname_t  qname;      /**< qualified name */
  PFvar_t   *var;        /**< variable information */
  PFty_t     type;       /**< used with c_type */
  struct PFfun_t *fun;   /**< function reference */
};

/** Semantic node content of core tree node */
typedef union PFcsem_t PFcsem_t;

/** struct representing a core tree node */
struct PFcnode_t {
    PFctype_t   kind;                    /**< node kind indicator */
    PFcsem_t    sem;                     /**< semantic node information */
    PFcnode_t  *child[PFCNODE_MAXCHILD]; /**< child nodes */
    PFty_t      type;                    /**< static type */
    struct PFalg_pair_t alg;
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
                         PFcnode_t *);
PFcnode_t *PFcore_wire2 (PFctype_t, 
                         PFcnode_t *, PFcnode_t *);
PFcnode_t *PFcore_wire3 (PFctype_t,
                         PFcnode_t *, PFcnode_t *, PFcnode_t *);
PFcnode_t *PFcore_wire4 (PFctype_t, 
                         PFcnode_t *, PFcnode_t *, PFcnode_t *, PFcnode_t *);

PFcnode_t *PFcore_nil (void);

PFvar_t *PFcore_new_var (char *);
PFcnode_t *PFcore_var (PFvar_t *);

PFcnode_t *PFcore_num (int);
PFcnode_t *PFcore_dec (double);
PFcnode_t *PFcore_dbl (double);
PFcnode_t *PFcore_str (char *);

PFcnode_t *PFcore_seqtype (PFty_t);
PFcnode_t *PFcore_seqcast (PFcnode_t *, PFcnode_t *);
PFcnode_t *PFcore_proof (PFcnode_t *, PFcnode_t *, PFcnode_t *);
PFcnode_t *PFcore_stattype (PFcnode_t *);
PFcnode_t *PFcore_typeswitch (PFcnode_t *, PFcnode_t *, PFcnode_t *);
PFcnode_t *PFcore_case (PFcnode_t *, PFcnode_t *);
PFcnode_t *PFcore_cases (PFcnode_t *, PFcnode_t *);

PFcnode_t *PFcore_ifthenelse (PFcnode_t *, PFcnode_t *, PFcnode_t *);

PFcnode_t *PFcore_for (PFcnode_t *, PFcnode_t *, PFcnode_t *, PFcnode_t *);
PFcnode_t *PFcore_let (PFcnode_t *, PFcnode_t *, PFcnode_t *);

PFcnode_t *PFcore_seq (PFcnode_t *, PFcnode_t *);
PFcnode_t *PFcore_empty (void);

PFcnode_t *PFcore_true (void);
PFcnode_t *PFcore_false (void);

PFcnode_t *PFcore_locsteps (PFcnode_t *, PFcnode_t *);
PFcnode_t *PFcore_step (PFpaxis_t, PFcnode_t *);
PFcnode_t *PFcore_kindt (PFpkind_t, PFcnode_t *);
PFcnode_t *PFcore_namet (PFqname_t );

PFcnode_t *PFcore_constr_elem (PFcnode_t *, PFcnode_t *);
PFcnode_t *PFcore_constr_attr (PFcnode_t *, PFcnode_t *);
PFcnode_t *PFcore_constr (PFptype_t, PFcnode_t *);
PFcnode_t *PFcore_tag (PFqname_t);

PFfun_t *PFcore_function (PFqname_t);
PFcnode_t *PFcore_apply (PFfun_t *, PFcnode_t *);
PFcnode_t *PFcore_arg (PFcnode_t *, PFcnode_t *);
PFcnode_t *PFcore_apply_ (PFfun_t *, ...);

/**
 * Expansion functions for Calculations
 */
PFcnode_t *PFcore_fn_data (PFcnode_t *);
PFcnode_t *PFcore_fs_convert_op_by_type (PFcnode_t *, PFty_t);
PFcnode_t *PFcore_fs_convert_op_by_expr (PFcnode_t *, PFcnode_t *);
PFcnode_t *PFcore_some (PFcnode_t *, PFcnode_t *, PFcnode_t *);

/**
 * Wrapper for #apply_.
 */
#define APPLY(fn,...) PFcore_apply_ ((fn), __VA_ARGS__, 0)

PFcnode_t *PFcore_ebv (PFcnode_t *);

PFcnode_t *PFcore_error (const char *, ...);
PFcnode_t *PFcore_error_loc (PFloc_t, const char *, ...);

#endif   /* CORE_H */

/* vim:set shiftwidth=4 expandtab: */
