/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Declarations for variable.c (Variable access functions)
 *
 * $Id$
 */

#ifndef VARIABLE_H
#define VARIABLE_H

/** variable information block */
typedef struct PFvar_t PFvar_t;

/** PFqname_t */
#include "qname.h"

/** PFty_t */
#include "types.h"

/** PFmty_t */
#include "milty.h"

/**
 * Variable information block.
 *
 * Each of these blocks represents a unique variable, that can be
 * identified by a pointer to its PFvar_t block.
 */
struct PFvar_t {
    PFqname_t    qname;   /**< variable name. Note that this might change
                               to an NCName, see issue 207 of the April 30
                               draft. */
    PFty_t       type;    /**< type of value bound to this variable */
    PFmty_t      impl_ty; /**< implementation type in MIL */
};

/* Allocate a new PFvar_t struct to hold a new variable. */
PFvar_t * PFnew_var (PFqname_t varname);

#endif   /* VARIABLE_H */

/* vim:set shiftwidth=4 expandtab: */
