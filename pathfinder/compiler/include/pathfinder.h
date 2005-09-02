/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Global definitions for Pathfinder compiler.
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
 * The Initial Developer of the Original Code is the Database &
 * Information Systems Group at the University of Konstanz, Germany.
 * Portions created by the University of Konstanz are Copyright (C)
 * 2000-2005 University of Konstanz.  All Rights Reserved.
 *
 * $Id$
 */

#ifndef PATHFINDER_H
#define PATHFINDER_H

#include <pf_config.h>
#include <setjmp.h>

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

/**
 * Has the Pathfinder compiler been invoked on the command line,
 * or from within MonetDB?
 */
enum PFinvocation_t {
      invoke_cmdline
    , invoke_monetdb
};

/** componentes of global compiler state */
struct PFstate_t {
    unsigned int debug;       /**< command line switch: -d */
    bool timing;              /**< command line switch: -T */
    bool print_dot;           /**< command line switch: -D */
    bool print_pretty;        /**< command line switch: -P */
    unsigned int stop_after;  /**< processing phase to stop after */
    bool print_types;         /**< command line switch: -t */
    unsigned int optimize;    /**< command line switch: -O */
    bool print_parse_tree;    /**< command line switch: -p */
    bool print_core_tree;     /**< command line switch: -c */
    bool print_la_tree;       /**< command line switch: -l */
    bool print_pa_tree;       /**< command line switch: -a */
    bool summer_branch;       /**< command line switch: -M */

    char *format;             /**< dot output format (command line switch -f) */

    char* genType;     /* kind of output */
    enum PFinvocation_t invocation;
};


/** global state of the compiler (regrettably not the *only* global state..) */
extern PFstate_t PFstate;

/** fatalities now lead to a longjump instead of exit() */
extern jmp_buf PFexitPoint;       

enum PFempty_order_t {
      greatest
    , least
    , undefined
};
typedef enum PFempty_order_t PFempty_order_t;

/**
 * Declarations given in the input query (encoding, ordering mode, etc.)
 */
struct PFquery_t {
    char           *version;        /**< XQuery version in query */
    char           *encoding;       /**< Encoding specified in query */
    bool            ordering;       /**< ordering declaration in query */
    PFempty_order_t empty_order;    /**< `declare default order' */
    bool            inherit_ns;
};
typedef struct PFquery_t PFquery_t;

extern PFquery_t PFquery;

/** information on textual location of XQuery parse tree node */
typedef struct PFloc_t PFloc_t;

/** information on textual location of XQuery parse tree node */
struct PFloc_t {
    unsigned int first_row;    /**< row number in which location starts. */
    unsigned int first_col;    /**< column number in which location starts. */
    unsigned int last_row;     /**< row number in which location ends. */
    unsigned int last_col;     /**< column number in which location ends. */
};

/**
 * We currently do not really implement the XQuery type xs:decimal.
 * For now, it is implemented as a C float (which actually way off
 * the XQuery specification).
 *
 * @warning Only few compiler phases actually use this typedef here!
 *          Most phases explicitly use float, so you have to change
 *          those first before you dare to change this typedef!
 */
typedef float dec;

/** XQuery `order by' modifier (see W3C XQuery, 3.8.3) */
typedef struct PFsort_t PFsort_t;

struct PFsort_t {
  enum { p_asc, p_desc }       dir;     /**< ascending/descending */
  enum { p_greatest, p_least } empty;   /**< empty greatest/empty least */
  char                        *coll;    /**< collation (may be 0) */
};

#endif  /* PATHFINDER_H */

/* vim:set shiftwidth=4 expandtab: */
