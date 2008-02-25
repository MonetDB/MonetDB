/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Compiler Driver definitions for Pathfinder compiler.
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
 */

#ifndef COMPILE_H
#define COMPILE_H

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
/** global state of the compiler  */
typedef struct PFstate_t PFstate_t;

void PFstate_init (PFstate_t *status);

/** The main compiler driver function in compiler/compiler.c */
int PFcompile (char *url, FILE* pfout, PFstate_t *status);

#endif

/* vim:set shiftwidth=4 expandtab: */
