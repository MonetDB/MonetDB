/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Type declarations for MIL program.
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

#ifndef MIL_H
#define MIL_H

#include "pathfinder.h"
#include "variable.h"
#include "milty.h"
#include "functions.h"

/** maximum number of children for a MIL tree node */
#define PFMNODE_MAXCHILD 4

/** MIL tree node type indicators */
typedef enum PFmtype_t PFmtype_t;

/** MIL tree node type indicators */
enum PFmtype_t {
      m_assgn         /**< a := b */
    , m_comm_seq      /**< command sequence: a ; b */
    , m_print         /**< print(a) */
    , m_new           /**< new(a,b) */
    , m_seqbase       /**< seqbase(a,b) */
    , m_tail          /**< the MIL special variable $t, specifying the tail
                           value of the current BUN in a batloop */

    , m_var           /**< variable */
    , m_lit_int       /**< literal integer */
    , m_lit_bit       /**< literal boolean */
    , m_lit_str       /**< literal string */
    , m_lit_dbl       /**< literal double (currently also used for decimals) */
    , m_lit_oid       /**< literal oid (for setting sequence bases) */
    , m_type          /**< a MIL type (used e.g. in new() statements) */

    , m_cast          /**< cast a simple value; semantic value contains MIL
                           type to cast to (quantifier will be disregarded) */
    , m_fcast         /**< a cast folded into a BAT; semantic value contains MIL
                           type to cast to (quantifier will be disregarded) */

    , m_batloop       /**< MIL batloop */
    , m_fetch         /**< Monet's fetch() function */
    , m_count         /**< Monet's count() function */
    , m_ifthenelse    /**< if-then-else */
    , m_ifthenelse_   /**< Monet's ifthenelse(,,) function */

    , m_plus          /**< Monet '+' operator */
    , m_not           /**< MIL not function */
    , m_isnil         /**< MIL isnil function */
    , m_equals        /**< Monet's equal operator `=' */
    , m_or            /**< MIL `or'/`||' operator */

    , m_apply         /**< Monet function application */
    , m_args          /**< list of function arguments */
    , m_arg           /**< single function argument */

    , m_insert        /**< insert statement */
    , m_error         /**< Monet's error() function */
    , m_nil
};

/** corresponds to the node type in MIL (implemented as a Monet oid) */
typedef unsigned int node;

/** corresponds to the oid type in MIL */
typedef unsigned int oid;

/** semantic node information variants */
typedef union PFmsem_t PFmsem_t;

/** semantic node information variants */
union PFmsem_t {
    char     * str;    /**< pointer to string type content */
    int        num;    /**< inlined content if type is int */
    bool       tru;    /**< inlined content if type is bool */
    double     dbl;    /**< inlined content if type is dbl */
    node       n;      /**< inlined content if type is Monet type node */
    oid        o;      /**< inlined content if type is Monet type oid */
    PFvar_t *  var;    /**< pointer to variable */
    PFmty_t    mty;    /**< MIL type specifier (e.g. for casts) */
    PFfun_t *  fun;    /**< function to call */
};

/** a MIL tree node */
typedef struct PFmnode_t PFmnode_t;

/** a MIL tree node */
struct PFmnode_t {
    PFmtype_t     kind;                     /**< node kind indicator */
    PFmsem_t      sem;                      /**< semantic node information */
    PFmty_t       mty;                      /**< implementation type of
                                             *   this subexpression */
    PFmnode_t    *child[PFMNODE_MAXCHILD];  /**< child nodes */
};

#endif

/* vim:set shiftwidth=4 expandtab: */
