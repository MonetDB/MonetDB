/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Walk Pathfinder's parse tree to check for semantically sound function
 * usage (this does not perform static type checking yet!):
 *
 * - are called functions present in the function environment
 *   (i.e. functions need to beither built-in XQuery F&O or user-defined)?
 *
 * - are functions given the correct number of arguments?
 *
 * This assumes that the built-in XQuery F&O have already been loaded
 * (see semantics/xquery_fo.c). 
 *
 * $Id$
 */

#ifndef FUNC_CHK_H
#define FUNC_CHK_H


/* PFpnode_t */
#include "abssyn.h"

/**
 * Clear the list of available XQuery functions
 */
void PFfun_clear (void);


/**
 * Traverse the abstract syntax tree and check for correct function
 * usage.  Also register user-defined XQuery functions with the
 * Pathfinder's function environment.
 *
 * @param root The root of the abstract syntax tree.
 */
void PFfun_check (PFpnode_t * root);

#endif    /* FUNC_CHK_H */

/* vim:set shiftwidth=4 expandtab: */
