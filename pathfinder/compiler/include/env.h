/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Functions and data structures to support environments for various
 * Pathfinder modules.
 *
 * $Id$
 */

#ifndef ENV_H
#define ENV_H

/* PFqname_t */
#include "qname.h"

/* PFarray_t */
#include "array.h"

typedef PFarray_t PFenv_t;

/* create a new environment */
PFenv_t *PFenv ();

/* bind key to value in environment 
 * (return 0 if key was unbound, value otherwise) 
 */
PFarray_t *PFenv_bind (PFenv_t *, PFqname_t, void *);

/* lookup given key in environment (returns array of bindings or 0) */
PFarray_t *PFenv_lookup (PFenv_t *, PFqname_t);

/** iterate over all bound values in an environment */
void PFenv_iterate (PFenv_t *, void (*) (void *));

/** iterate over all bound keys in an environment */
void PFenv_key_iterate (PFenv_t *, void (*) (PFqname_t));

/* number of keys in environment */
unsigned int PFenv_count (PFenv_t *);

#endif /* ENV_H */


/* vim:set shiftwidth=4 expandtab: */
