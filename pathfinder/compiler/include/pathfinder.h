/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Global definitions for Pathfinder compiler.
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

#ifndef PATHFINDER_H
#define PATHFINDER_H

#if HAVE_CONFIG_H
#include <pf_config.h>
#endif

#ifndef NULL
/** Make sure we have NULL available */
#define NULL 0
#endif

/* boolean type `bool' and constants `true', `false' */
#ifdef HAVE_STDBOOL_H
#include <stdbool.h>
#elif !defined(HAVE_BOOL)
#define bool    char
#define true    (char)1
#define false   (char)0
#endif

/** Compilation phases */
enum PFphases_t {
      phas_parse     = 1   /**< Parsing and abstract syntax tree generation */
    , phas_semantics       /**< Semantics checks (varscope, ns, functions) */
    , phas_fs              /**< Compilation to core language */
    , phas_simpl           /**< Simplification/normalization of core language */
    , phas_alg             /**< Algebra tree generation */
    , phas_algopt          /**< Algebra tree rewrite/optimization */
    , phas_mil             /**< MIL code generation */
    , phas_mil_summer      /**< temporary MIL code generation and print */
   
    , phas_all             /**< Do all processing phases */
};

typedef enum PFphases_t PFphases_t;

/** global state of the compiler  */
typedef struct PFstate_t PFstate_t;

#define PF_GEN_ORG      0
#define PF_GEN_XML      1
#define PF_GEN_SAX      2

/** componentes of global compiler state */
struct PFstate_t {
    bool quiet;               /**< command line switch: -q */
    bool timing;              /**< command line switch: -T */
    bool print_dot;           /**< command line switch: -D */
    bool print_pretty;        /**< command line switch: -P */
    unsigned int stop_after;  /**< processing phase to stop after */
    bool print_types;         /**< command line switch: -t */
    unsigned int optimize;    /**< command line switch: -O */
    bool print_parse_tree;    /**< command line switch: -p */
    bool print_core_tree;     /**< command line switch: -c */
    bool print_algebra_tree;  /**< command line switch: -a */
    bool print_ma_tree;       /**< command line switch: -m */
    bool summer_branch;       /**< command line switch: -M */
    bool parse_hsk;           /**< command line switch: -H */
    unsigned int genType;     /* kind of output */
};

/** global state of the compiler */
extern PFstate_t PFstate;

/** information on textual location of XQuery parse tree node */
typedef struct PFloc_t PFloc_t;

/** information on textual location of XQuery parse tree node */
struct PFloc_t {
    unsigned int first_row;    /**< row number in which location starts. */
    unsigned int first_col;    /**< column number in which location starts. */
    unsigned int last_row;     /**< row number in which location ends. */
    unsigned int last_col;     /**< column number in which location ends. */
};

#endif

/* vim:set shiftwidth=4 expandtab: */
