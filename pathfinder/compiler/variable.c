/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Access functions for variable information structs.
 *
 * For every variable, a PFvar_t struct is created. The struct currently
 * contains the variable name, but may later contain more information
 * about the variable (e.g., type information, etc.). Several occurencies
 * of the same variable reference the same PFvar_t struct, the pointer
 * serves as a unique identifier of each variable. Variables that have
 * the same name but are distinct variables according to scoping rules
 * result in distinct PFvar_t structs.
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

#include "pathfinder.h"
#include "variable.h"

/** PFqname_t */
#include "qname.h"
/** PFty_none () */
#include "types.h"
#include "mem.h"

/**
 * Allocate a new @a PFvar_t struct to hold a variable with name @a varname,
 * the variable's type is set to @a none.
 *
 * @param varname variable name
 *   @note The content of the QName is not copied, but a
 *     pointer to the original QName is stored in the new node. The
 *     QName may not be modified from outside after this function has
 *     been called.
 * @return Pointer to the new @c PFvar_t 
 */
PFvar_t *
PFnew_var (PFqname_t varname)
{
    PFvar_t *n;

    n = (PFvar_t *) PFmalloc (sizeof (PFvar_t));

    n->qname  = varname;
    n->type   = PFty_none ();
    n->vid    = 0;
    n->global = false;

    return n;
}

/* vim:set shiftwidth=4 expandtab: */
