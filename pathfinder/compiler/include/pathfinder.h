/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Global definitions for Pathfinder compiler.
 *
 * $Id$
 */

/*
 * Include all the information we got from the configure script
 */
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif


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
