/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Declarations for variable.c (Variable access functions)
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

#ifndef VARIABLE_H
#define VARIABLE_H

/** variable information block */
typedef struct PFvar_t PFvar_t;

/** PFqname_t */
#include "qname.h"

/** PFty_t */
#include "types.h"

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

    /* code below is for temporary MIL code generation (summer version) */
    bool	 global;  /**< boolean indicating if var is global or local */
    signed char  used;    /**< information if variables (in for loops)
                               are used */
    char         base;    /**< the level (based on for-expression) in which
                               the variable is bound */
    int          vid;     /**< the oid the variable gets in MIL */
};

/* Allocate a new PFvar_t struct to hold a new variable. */
PFvar_t * PFnew_var (PFqname_t varname);

#endif   /* VARIABLE_H */

/* vim:set shiftwidth=4 expandtab: */
