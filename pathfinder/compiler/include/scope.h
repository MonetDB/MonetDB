/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Functions and data structures to maintain nested scopes (e.g., to
 * implement variable visibility).
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
PFscope_t *PFscope ();

/** bring new entry key |--> value into scope */
void PFscope_into (PFscope_t *, PFqname_t, void *);

/** overall number of entries in scope data structure */ 
unsigned int PFscope_count (PFscope_t *);

/** open a new scope */
void PFscope_open (PFscope_t *);

/** close current scope */
void PFscope_close (PFscope_t *);

/** is key currently in scope? (returns 0 if key is not in scope) */
void *PFscope_lookup (PFscope_t *, PFqname_t);

#endif /* SCOPE_H */

/* vim:set shiftwidth=4 expandtab: */
