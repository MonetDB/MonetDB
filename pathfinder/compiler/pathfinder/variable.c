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
 * $Id$
 */

#include "pathfinder.h"

/** PFqname_t */
#include "qname.h"

/** PFty_none () */
#include "types.h"

#include "variable.h"

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

    n->qname = varname;
    n->type  = PFty_none ();

    return n;
}

/* vim:set shiftwidth=4 expandtab: */
