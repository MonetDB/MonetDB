/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Declarations for semantics/functions.c; Data structures and access
 * functions for XQuery function calls and definitions.
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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#ifndef FUNCTIONS_H

typedef struct PFfun_t PFfun_t;

#define FUNCTIONS_H

#include "pathfinder.h"
/* PFty_t */
#include "types.h"
/* PFenv_t */
#include "env.h"

/* PFvar_t */
#include "variable.h"

/* PFcnode_t */
#include "core.h"

#include "core2alg.h"

/** Data structure to hold information about XQuery functions.  */
/* typedef struct PFfun_t PFfun_t; */

/**
 * Data structure to hold signature information.
 * (builtin functions).
 */
struct PFfun_sig_t {
    PFty_t   *par_ty; /**< builtin: array of formal parameter types */
    PFty_t    ret_ty; /**< builtin: return type */
};

typedef struct PFfun_sig_t PFfun_sig_t;

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
    PFqname_t      qname;      /**< function name */
    unsigned int   arity;      /**< number of arguments */
    bool           builtin;    /**< is this a builtin (XQuery F&O) function? */
    unsigned int sig_count;    /**< number of signatures, >1 for dynamically
                                    overloaded functions. */
    PFfun_sig_t   *sigs;       /**< signatures */
    struct PFla_pair_t (*alg) (const struct PFla_op_t *, bool, struct PFla_pair_t *);
    PFvar_t      **params;     /**< list of parameter variables */
    PFcnode_t     *core;
    int            fid;        /**< id for variable environment mapping
                                 (summer_branch) */
    char          *sig;        /**< milprint_summer: full signature
                                 converted to single identifier */	
    char          *atURI;      /**< URI given by the "at"-hint of the
                                 module to which this function belongs,
                                 if any */
};

/**
 * Environment of functions known to Pathfinder.
 */
extern PFenv_t *PFfun_env;

/** allocate a new struct to describe a (built-in or user) function */
PFfun_t *PFfun_new (PFqname_t, unsigned int, bool, unsigned int, PFfun_sig_t *,
                    struct PFla_pair_t (*alg) (const struct PFla_op_t *,
                                               bool,
                                               struct PFla_pair_t *),
                    PFvar_t **params, char *atURI);

#endif   /* FUNCTIONS_H */

/* vim:set shiftwidth=4 expandtab: */
