/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Declarations for semantics/functions.c; Data structures and access
 * functions for XQuery function calls and definitions.
 *
 * $Id$
 */

#ifndef FUNCTIONS_H
#define FUNCTIONS_H

#include "pathfinder.h"

/* PFty_t */
#include "types.h"

#include "env.h"

/** Data structure to hold information about XQuery functions.  */
typedef struct PFfun_t PFfun_t;

/**
 * Data structure to hold information about XQuery functions.
 * Functions can be either predefined (builtin, XQuery F&O) functions or
 * user-defined.
 *
 * @note
 * As of the <a href="http://www.w3.org/TR/2002/WD-xquery-20020816/">Aug
 * 2002 W3C draft</a> (sec. 4.5), XML Query does not allow function
 * overloading for user-defined functions. Recursion, however, is
 * allowed.
 */
struct PFfun_t {
  PFqname_t    qname;      /**< function name */
  unsigned int arity;      /**< number of arguments */
  bool         builtin;    /**< is this a builtin (XQuery F&O) function? */
  PFty_t      *par_ty;     /**< builtin: array of formal parameter types */
  PFty_t       ret_ty;     /**< builtin: return type */
};

/**
 * Environment of functions known to Pathfinder.
 */
extern PFenv_t *PFfun_env;

/** allocate a new struct to describe a (built-in or user) function */
PFfun_t *PFfun_new (PFqname_t, unsigned int, bool, PFty_t *, PFty_t *);

#endif   /* FUNCTIONS_H */

/* vim:set shiftwidth=4 expandtab: */
