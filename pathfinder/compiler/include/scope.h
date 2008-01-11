/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Functions and data structures to maintain nested scopes (e.g., to
 * implement variable visibility).
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

#ifndef SCOPE_H
#define SCOPE_H

/* PFarray_t */
#include "array.h"

/* PFqname_t */
#include "qname.h"

/* # of buckets in entry hash table */
#define SCOPE_HASH_SZ 31

typedef struct PFscope_t PFscope_t;

struct PFscope_t {
    PFarray_t *hash[SCOPE_HASH_SZ];      /**< entry hash table */
    PFarray_t *undo;                     /**< undo stack */
};

/** create a scope data structure */
PFscope_t *PFscope (void);

/** bring new entry key |--> value into scope */
void PFscope_into (PFscope_t *, PFqname_t, void *);

/** overall number of entries in scope data structure */ 
unsigned int PFscope_count (const PFscope_t *);

/** open a new scope */
void PFscope_open (PFscope_t *);

/** close current scope */
void PFscope_close (PFscope_t *);

/** is key currently in scope? (returns 0 if key is not in scope) */
void *PFscope_lookup (PFscope_t *, PFqname_t);

/**
 * Append all variables in scope @a s2 to scope @a s1.
 * (Destructively updates @a s1, but leaves @a s2 alone.)
 */
void PFscope_append (PFscope_t *s1, const PFscope_t *s2, bool allow_override);

#endif /* SCOPE_H */

/* vim:set shiftwidth=4 expandtab: */
