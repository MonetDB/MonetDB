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
