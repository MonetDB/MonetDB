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

#ifndef PATHFINDER_H
#define PATHFINDER_H

#include <pf_config.h>

/* AIX requires this to be the first thing in the file.  */
#ifdef HAVE_ALLOCA_H
#  include <alloca.h>
#else
#  ifdef _AIX
#pragma alloca
#  else
#   ifndef alloca       /* predefined by HP cc +Olibcalls */
void *alloca(size_t);
#   endif
#  endif
#endif

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

enum PFoutput_format_t {
      PFoutput_format_not_specified
    , PFoutput_format_milprint_summer
    , PFoutput_format_mil
    , PFoutput_format_sql
    , PFoutput_format_xml
};

/** componentes of global compiler state */
struct PFstate_t {
    bool quiet;               /**< command line switch: -q */
    bool timing;              /**< command line switch: -T */
    bool print_dot;           /**< command line switch: -D */
    bool print_xml;           /**< command line switch: -X */
    bool import_xml;          /**< command line switch: -I */
    char* import_xml_filename;/**< the filename of the import-file */
    bool print_pretty;        /**< command line switch: -P */
    unsigned int stop_after;  /**< processing phase to stop after */
    bool print_types;         /**< command line switch: -t */
    unsigned int optimize;    /**< command line switch: -O */
    bool print_parse_tree;    /**< command line switch: -p */
    bool print_core_tree;     /**< command line switch: -c */
    bool print_la_tree;       /**< command line switch: -l */
    bool print_pa_tree;       /**< command line switch: -a */
    enum PFoutput_format_t output_format; /**< command line switches:
                                               -A, -M, and -S */
    bool dead_code_el;        /**< command line switch: -e */

    bool standoff_axis_steps; /**< command line switch: -b */

    char *opt_alg;            /**< list of algebraic optimizations 
                                   -- specific for MIL compilation
                                   (command line switch -o) */
    char *opt_sql;            /**< list of algebraic optimizations 
                                   -- specific for SQL compilation
                                   (command line switch -o) */
    char *format;             /**< dot output format (command line switch -f) */

    char* genType;     /* kind of output */
    enum PFinvocation_t invocation;

#ifndef NDEBUG
    struct {
        bool   subtyping;     /**< invoke PFty_debug_subtyping()?
                                   (command line option `-d subtyping') */
    } debug;
#endif
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

enum PFrevalidation_t {
      revalid_strict
    , revalid_lax
    , revalid_skip
};
typedef enum PFrevalidation_t PFrevalidation_t;

/**
 * Declarations given in the input query (encoding, ordering mode, etc.)
 */
struct PFquery_t {
    char           *version;             /**< XQuery version in query */
    char           *encoding;            /**< Encoding specified in query */
    bool            ordering;            /**< ordering declaration in query */
    PFempty_order_t empty_order;         /**< `declare default order' */
    bool            inherit_ns;
    bool            pres_boundary_space; /**< perserve boundary space? */
    PFrevalidation_t revalid;
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
 * For now, it is implemented as a C double (which actually way off
 * the XQuery specification).
 *
 * @warning Only few compiler phases actually use this typedef here!
 *          Most phases explicitly use double, so you have to change
 *          those first before you dare to change this typedef!
 */
typedef double dec;

/** XQuery `order by' modifier (see W3C XQuery, 3.8.3) */
typedef struct PFsort_t PFsort_t;

struct PFsort_t {
  enum { p_asc, p_desc }       dir;     /**< ascending/descending */
  enum { p_greatest, p_least } empty;   /**< empty greatest/empty least */
  char                        *coll;    /**< collation (may be 0) */
};

#include "qname.h"

/**
 * Information in a Pragma:
 *   (# qname content #)
 */
struct PFpragma_t {
    union {
        PFqname_raw_t qname_raw;
        PFqname_t     qname;
    } qn;
    char       *content;
};
typedef struct PFpragma_t PFpragma_t;

/**
 * We still require the "milprint_summer" code, but started to
 * wrap it into ENABLE_MILPRINT_SUMMER conditions.
 */
#define ENABLE_MILPRINT_SUMMER 1

#endif  /* PATHFINDER_H */

/* vim:set shiftwidth=4 expandtab: */
