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

/*
 * Include all the information we got from the configure script
 */
#include <pf_config.h>


#ifndef PATHFINDER_H
#define PATHFINDER_H

#ifndef NULL
/** Make sure we have NULL available */
#define NULL 0
#endif

/* FILE */
#include <stdio.h>

/* boolean type `bool' and constants `true', `false' */
#include <stdbool.h>

/** Compilation phases */
typedef enum PFphases_t PFphases_t;

/** Compilation phases */
enum PFphases_t {
    phas_parse     = 1   /**< Parsing and abstract syntax tree generation */
  , phas_semantics       /**< Semantics checks (varscope, ns, functions) */
  , phas_fs              /**< Compilation to core language */
  , phas_simpl           /**< Simplification/normalization of core language */
  , phas_mil             /**< MIL code generation */

  , phas_all             /**< Do all processing phases */
};

/** Output types */
typedef enum PFoutput_t PFoutput_t;

/** Output types */
enum PFoutput_t {
      output_monet       /**< Output to feed into Monet (w/o markup) */
    , output_xterm       /**< Output for screen-view (colored) */
    , output_html        /**< HTML-Output (colored) */
};

/** global state of the compiler  */
typedef struct PFstate_t PFstate_t;

/** componentes of global compiler state */
struct PFstate_t {
  bool quiet;               /**< command line switch: -q */
  bool daemon;              /**< command line switch: -d */
  bool timing;              /**< command line switch: -T */
  bool print_dot;           /**< command line switch: -D */
  bool print_pretty;        /**< command line switch: -P */
  PFphases_t stop_after;    /**< processing phase to stop after */
  bool print_types;         /**< command line switch: -t */
  PFoutput_t output_type;   /**< command line switch: -o */
  PFoutput_t optimize;      /**< command line switch: -O */
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

/* stack-based error handling */
#include "oops.h"

/* garbage collected memory handling */
#include "mem.h"

#endif

/* vim:set shiftwidth=4 expandtab: */
