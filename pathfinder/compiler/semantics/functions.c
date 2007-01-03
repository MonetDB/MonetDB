/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Data structures for XML Query function definition and calls,
 * access functions for them and tree-walker to check for correct
 * function referencing.
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

#include "pathfinder.h"

#include <stdlib.h>
/* variable argument list for func_add_var() */
#include <stdarg.h>  
#include <string.h>
#include <assert.h>

#include "functions.h"

#include "func_chk.h"
#include "types.h"
#include "oops.h"
#include "mem.h"

/* "at"-hint of the module that we are currently checking */
static char *current_atURI = NULL;

/* add a single user-defined function definition to the list */
static void add_ufun (PFpnode_t *n);

/* register a user-defined function */
static void fun_add_user (PFqname_t qname, unsigned int arity,
                          PFvar_t **params);


/**
 * Environment of functions known to Pathfinder.
 */
PFenv_t *PFfun_env = NULL;

/* activate debugging code */
/* #define DEBUG_FUNCTIONS */

/* Print out all registered functions for debugging purposes */
#ifdef DEBUG_FUNCTIONS
#include <stdio.h>

static void print_functions (void);
#endif


/**
 * Count number of formal arguments to a user-defined function
 * (defined in abstract syntax tree node @a n).
 * 
 * Parse tree structure:
 *
 *                  fun_decl
 *                 /        \
 *            fun_sig        e
 *           /       \
 *        params      t
 *       /     \                        n is one of the params nodes
 *      p1     params
 *             /    \
 *            p2    ...
 *                    \
 *                    nil
 *
 * @param n The current @c p_params node; when called from outside,
 *   this is the topmost @c p_params node below the function
 *   declaration. (Can also be a @c p_nil node if no parameters are
 *   specified or the bottom is reached during recursion.)
 * @return number of formal arguments
 */
static unsigned int
formal_args (PFpnode_t *n)
{
    switch (n->kind) {
        case p_nil:
            return 0;
        case p_params:
            return 1 + formal_args (n->child[1]);
        default:
            PFoops_loc (OOPS_FATAL, n->loc,
                        "illegal node kind (expecting nil/params)");
    }

    /* just to pacify picky compilers; never reached due to "exit" in PFoops */
    return 0;
}

/**
 * Count the number of actual arguments for the function call
 * (or declaration) in abstract syntax tree node @a n.
 *
 * @param n The current @c p_args (or @c p_params) node; when
 *   called from outside, this is the topmost @c p_args (@c p_params)
 *   node below the function call (declaration). (Can also be a
 *   @c p_nil node if no parameters are specified or the bottom
 *   is reached during recursion.)
 * @return number of actual arguments
 */
static unsigned int
actual_args (PFpnode_t *n)
{
    switch (n->kind) {
        case p_fun_sig:
            return actual_args (n->child[0]);

        case p_nil:  
            return 0;

        case p_args:
        case p_params:
            return 1 + actual_args (n->child[1]);

        default:     
            PFoops_loc (OOPS_FATAL, n->loc,
                        "illegal node kind (expecting nil/args/params)");
    }

    /* just to pacify picky compilers; never reached due to "exit" in PFoops */
    return 0;
}

/*
 * Parse tree structure:
 *
 *                                  fun_decl
 *                                 /        \
 *                            fun_sig        e
 *                           /       \
 *                        params      t
 *                       /      \
 *                  param        params
 *                /   |          /    \
 *          seq_ty   var      param    ...
 *            |              /   |       \
 *           ...        seq_ty  var      nil
 *                       |
 *                      ...
 *
 * n is one of the params nodes.
 */
static void
fill_paramlist (PFvar_t **params, PFpnode_t *n)
{
    assert (n);

    switch (n->kind) {
        case p_nil:
            break;

        case p_params:
            assert (n->child[0]); assert (n->child[0]->kind == p_param);
            assert (n->child[0]->child[1]
                    && n->child[0]->child[1]->kind == p_var);
            *params = n->child[0]->child[1]->sem.var;
            fill_paramlist (params + 1, n->child[1]);
            break;

        default:     
            PFoops_loc (OOPS_FATAL, n->loc,
                        "illegal node kind (expecting nil/params)");
    }
}


/**
 * Register a user-defined function.
 * 
 * Parse tree structure:
 *
 *                fun_decl     <--- n
 *               /        \
 *          fun_sig        e
 *         /       \
 *      params      t
 *     /     \
 *    p1     params
 *           /    \
 *          p2    ...
 *                  \
 *                  nil
 */
static void
add_ufun (PFpnode_t *n)
{
    unsigned int   arity;
    PFvar_t      **params;

    assert (n->kind == p_fun_decl);
    assert (n->child[0] && n->child[0]->child[0]);

    /* count formal function arguments */
    arity = formal_args (n->child[0]->child[0]);

    params = PFmalloc (arity * sizeof (*params));
    fill_paramlist (params, n->child[0]->child[0]);

#ifdef DEBUG_FUNCTIONS
    fprintf (stderr, "registering %s (%i)\n",
             PFqname_str (n->sem.qname), arity);
#endif

    fun_add_user (n->sem.qname, arity, params);
}

/**
 * Register all functions in the abstract syntax tree.
 *
 * Recursively walks down the function declarations and registers all
 * functions using #add_ufun.
 */
static void
add_ufuns (PFpnode_t *n)
{
    if (n->kind == p_lib_mod)
        current_atURI = n->sem.str;

    switch (n->kind) {
        case p_fun_decl:
            /* add this function */
            add_ufun (n);
            return;

        default:
            /* recurse */
            for (unsigned int i = 0;
                    (i < PFPNODE_MAXCHILD) && (n->child[i]); i++)
                add_ufuns (n->child[i]);
    }
}


/**
 * Traverse the whole abstract syntax tree and look for #p_fun_ref
 * nodes.  For each of them, determine the number of actual arguments,
 * lookup the function in the function environment #PFfun_env and see if
 * everything's ok. This function is recursive.
 *
 * @param n The current abstract syntax tree node.
 */
static void
check_fun_usage (PFpnode_t * n)
{
    unsigned int i;
    PFarray_t *funs;
    PFfun_t *fun;
    unsigned int arity;

    assert (n);

    /* process child nodes */
    for (i = 0; (i < PFPNODE_MAXCHILD) && (n->child[i]); i++)
        check_fun_usage (n->child[i]);
    
    switch (n->kind) {

    case p_fun_ref:
    case p_fun_decl:
        
        funs = PFenv_lookup (PFfun_env, n->sem.qname);
        
        if (! funs)
            PFoops_loc (OOPS_APPLYERROR, n->loc,
                        "reference to undefined function `%s'", 
                        PFqname_str (n->sem.qname));

        /* Determine number of actual arguments */
        arity = actual_args (n->child[0]);
       
        /* avoid warning about uninitialized variable 'fun' */ 
        fun = *((PFfun_t **) PFarray_at (funs, 0));

        /*
         * There's exactly one function defined in XQuery that has
         * an arbitrary number (>= 2) of arguments: fn:concat().
         * (see http://www.w3.org/TR/xpath-functions/#func-concat)
         */
        if (PFqname_eq (n->sem.qname, PFqname (PFns_fn, "concat")) == 0) {

            if (arity < 2)
                PFoops_loc (OOPS_APPLYERROR, n->loc,
                            "fn:concat expects at least two arguments "
                            "(got %u)", arity);

            n->sem.fun = fun;
            n->kind = n->kind == p_fun_ref ? p_apply : p_fun;
            break;
        }
        
        /*
         * Unset the function reference field in the parse tree
         * node. Note that this invalidates the sem.qname field!
         * (sem is a union type).
         */
        n->sem.fun = NULL;

        /*
         * For all the other functions, we search for the last
         * variant with the correct number of arguments.
         *
         * We may overload user-defined functions, as soon as they
         * have a different number of parameters. For built-ins
         * we may overload even with same number of parameters.
         * In that case, we pick the last variant, as this is the
         * most generic. (For many built-ins we provide several
         * implementations that are optimized to certain type.
         * We list them with the most specific variants first
         * in xquery_fo.c.) Picking the last (and most generic
         * variant) makes sure that we apply the function conversion
         * rules (W3C XQuery 3.1.5) correctly.
         */
        for (i = 0; i < PFarray_last (funs); i++) { 
            fun = *((PFfun_t **) PFarray_at (funs, i));
            if (arity == fun->arity) {
                n->sem.fun = fun;
            }
        }

        /* see if number of actual argument matches function declaration */
        if (! n->sem.fun)
            PFoops_loc (OOPS_APPLYERROR, n->loc,
                        "wrong number of arguments for function `%s' "
                        "(expected %u, got %u)",
                        PFqname_str (fun->qname), fun->arity, arity);
        
        /*
         * Replace semantic value of abstract syntax tree node
         * with pointer to #PFfun_t struct. Tree node is now a
         * ``real'' function application (declaration).
         */
        n->kind = n->kind == p_fun_ref ? p_apply : p_fun;
        
        break;
        
    default:
        /* for all other cases, do nothing */
        break;
    }
}


#ifdef DEBUG_FUNCTIONS

/**
 * Print information about a single function for debugging purposes
 */
static void
print_fun (PFfun_t ** funs)
{
    PFfun_t *fun = *funs;
    unsigned int i = 0;

    fprintf (stderr, "function name: %s\n", PFqname_str (fun->qname));
    if (fun->builtin) {
        for (unsigned int s = 0; s < fun->sig_count; s++) {
            PFfun_sig_t *sig = fun->sigs + s;
            fprintf (stderr, "\treturn type  : %s\n", PFty_str (sig->ret_ty));
  
            for (i = 0; i < fun->arity; i++)
                fprintf (stderr, "\t%2i. parameter: %s\n", 
                         i + 1, 
                         PFty_str (*(sig->par_ty + i)));
        }
    }
}

/**
 * Print list of registered functions for debugging purposes
 */
static void
print_functions (void)
{
    PFenv_iterate (PFfun_env, (void (*) (void *)) print_fun);
}
#endif   /* DEBUG_FUNCTIONS */

/**
 * Clear the list of available XQuery functions
 */
void
PFfun_clear (void)
{
    PFfun_env = NULL;
}

/**
 * Register an XQuery function with the function environment #PFfun_env
 * 
 * @param  qn function name
 * @param  arity number of function arguments
 * @return status code
 */
static void
fun_add_user (PFqname_t      qn,
              unsigned int   arity,
              PFvar_t      **params)
{
    PFfun_t      *fun = PFfun_new (qn, arity, false, 1, NULL, NULL, params, current_atURI);
    PFarray_t    *funs = NULL;
    unsigned int  i;

    /*
     * fn:concat() is a very special built-in function: It allows
     * an arbitrary number of arguments. That's why we could not
     * detect when fn:concat() is overloaded by a user-defined
     * function. So we check that here explicitly.
     */
    if (PFqname_eq (qn, PFqname (PFns_fn, "concat")) == 0)
        PFoops (OOPS_FUNCREDEF, "`%s'", PFqname_str (qn));

    /* insert new entry into function list */
    if ((funs = PFenv_bind (PFfun_env, qn, fun))) {

        /*
         * There is already a binding for a function with that
         * name. Functions with same name are only allowed, if
         * they have a different number of arguments.
         *
         * (Note that PFenv_bind() has already added to the list
         * of bindings. So we must not look at the last binding,
         * it will always have same arity as fun.)
         */
        for (i = 0; i < (PFarray_last (funs) - 1); i++) {
            if ((*((PFfun_t **) PFarray_at (funs, i)))->arity == arity)
                PFoops (OOPS_FUNCREDEF, "`%s'", PFqname_str (qn));
        }
    }
}

/**
 * Creates a new datastructure of type #PFfun_t that describes a
 * (user or built-in) function. Allocates memory for the function
 * and initializes the struct with the given values.
 * @param qn      qualified name of the function
 * @param arity   number of arguments
 * @param builtin Is this a built-in function or a user function?
 * @param par_tys array of formal parameter types. If parameter types
 *                are not known yet, pass @c NULL.
 * @param ret_ty  Pointer to return type. If return type is not known
 *                yet, pass @c NULL.
 * @param overload Informations for dynamic function overloading.
 * @param alg     In case of built-in functions, pointer to the
 *                routine that creates the algebra representation
 *                of the function. 
 * @param atURI   URI of the module definition file, to which the
 *                function belongs, given by the 'at'-hint.
 * @return a pointer to the newly allocated struct
 */
PFfun_t *
PFfun_new (PFqname_t      qn,
           unsigned int   arity,
           bool           builtin,
           unsigned int sig_count,
           PFfun_sig_t   *sigs,
           struct PFla_pair_t (*alg) (const struct PFla_op_t *,
                                      bool,
                                      struct PFla_pair_t *),
           PFvar_t      **params,
           char          *atURI)
{
    PFfun_t *n;

    n = (PFfun_t *) PFmalloc (sizeof (PFfun_t));

    n->qname   = qn;
    n->arity   = arity;
    n->builtin = builtin;
    n->alg     = alg;
    n->params  = params;
    n->core    = NULL;      /* initialize Core equivalent with NULL */
    n->fid     = 0;         /* needed for summer branch */
    n->sigs    = NULL;
    n->atURI   = atURI;

    n->sig_count = sig_count;
    /* assert (sig_count); */
    n->sigs =  (PFfun_sig_t *) PFmalloc (sig_count * sizeof (PFfun_sig_t));
    for (unsigned int i = 0; i < sig_count; i++) {
        /* copy array of formal parameter types (if present) */
        if (arity > 0 && sigs) {
            n->sigs[i].par_ty = (PFty_t *) PFmalloc (arity * sizeof (PFty_t));
            memcpy (n->sigs[i].par_ty, sigs[i].par_ty, 
                    arity * sizeof (PFty_t));
        }
        else
            n->sigs[i].par_ty = NULL;
        if (sigs)
            n->sigs[i].ret_ty  = sigs[i].ret_ty;
        else
            n->sigs[i].ret_ty = PFty_star (PFty_item ());
    }

    return n;
}

/**
 * Traverse the abstract syntax tree and check correct function usage.
 * Also generate a list of all XML Query functions available for this
 * XML Query expression.
 *
 * @param root The root of the abstract syntax tree.
 * @return Status code
 */
void
PFfun_check (PFpnode_t * root)
{
    /*
     *           main_mod                          lib_mod
     *          /        \                         /     \
     *     decl_imps     ...         or         mod_ns  decl_imps
     *     /      \                                     /      \
     *   ...     decl_imps                            ...      decl_imps
     *              \                                             \
     *              ...                                           ...
     *
     */
    assert (root);

    current_atURI = NULL; /* start with a fresh value */
    switch (root->kind) {
        case p_main_mod:
            assert (root->child[0]);
            add_ufuns (root->child[0]);
            break;

        case p_lib_mod:
            assert (root->child[1]);
            add_ufuns (root->child[1]);
            break;

        default:
            PFoops (OOPS_FATAL,
                    "illegal parse tree encountered during function checking.");
            break;
    }
    current_atURI = NULL; /* clean up old value */

#ifdef DEBUG_FUNCTIONS
    /* For debugging, print out all registered functions.  */
    print_functions ();
#endif

    /* now traverse the whole tree and check all function usages */
    check_fun_usage (root);
}

/* vim:set shiftwidth=4 expandtab: */
