/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Functions and data structures to maintain nested scopes (e.g., to
 * implement variable visibility).
 *
 * To implement the usual nested scope semantics, we maintain a
 * conceptual stack of entries (e.g., in the case of variable scoping,
 * a stack of variables).  Opening and closing a scope is implemented
 * by means of an undo stack, while the actual entries are maintained
 * in a hash table.  
 *
 * Entries are identified by QName keys.
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

#include <assert.h>

#include "scope.h"

/* PFarray_t */
#include "array.h"
/* PFqname_t */
#include "qname.h"
#include "mem.h"
#include "oops.h"

/** 
 * marks a new scope on undo stack (this needs to be different from
 * the hash values returned @a hash)
 */
#define SCOPE_BOUNDARY -1

/** an entry: key |--> value */
typedef struct entry_t entry_t;

struct entry_t {
    PFqname_t key;           /**< QName key to identify entry */
    void      *value;        /**< pay load, interpreted by caller only */
};

/**
 * Create a new scope data structure.  Entry hash table and undo stack
 * are both empty.
 *
 * @return pointer to scope data structure
 */
PFscope_t *
PFscope (void)
{
    PFscope_t *s;
    unsigned int i;

    s = (PFscope_t *) PFmalloc (sizeof (PFscope_t));

    /* set up the entry hash table and its buckets */
    for (i = 0; i < SCOPE_HASH_SZ; i++)
        s->hash[i] = PFarray (sizeof (entry_t));

    /* set up undo stack to maintain nested scopes */
    s->undo = PFarray (sizeof (int));

    return s;
}


/**
 * Hash value for given key.
 *
 * @param key identifying QName key
 * @return hash value
 */
static unsigned int 
hash (PFqname_t key)
{
    unsigned int i;
    char *k;

    i = 0;

    if (PFqname_uri (key))
        for (k = PFqname_uri (key); *k; k++)
            i = 131 * i + *k;

    for (k = PFqname_loc (key); *k; k++)
        i = 131 * i + *k;

    return i % SCOPE_HASH_SZ;
}

/**
 * Bring a new entry key |--> value into scope.
 *
 * @param s pointer to scope data structure
 * @param key identifying key for new entry
 * @param value pay load for new entry
 */
void
PFscope_into (PFscope_t *s, PFqname_t key, void *value)
{
    unsigned int h;

    assert (s && s->hash && s->undo);

    h = hash (key);

    /* store entry key |--> value in hash table */
    *((entry_t *) PFarray_add (s->hash[h])) =
        (entry_t) { .key = key, .value = value };
    
    /* maintain undo stack: push h */
    *((int *) PFarray_add (s->undo)) = h;
}

/** 
 * Overall number of entries in scope (not just the entries currently
 * in-scope).
 *
 * @param s pointer to scope data structure
 * @return overall number of entries in scope
 */
unsigned int
PFscope_count (const PFscope_t *s)
{
    assert (s);

    return PFarray_last (s->undo);
}

/**
 * Open a new scope.
 *
 * @param s pointer to scope data structure
 */
void
PFscope_open (PFscope_t *s)
{
    assert (s);

    /* push scope boundary marker onto undo stack */
    *((int *) PFarray_add (s->undo)) = SCOPE_BOUNDARY;
}

/**
 * Close current scope.  This pops the undo stack until we see the
 * next SCOPE_BOUNDARY and deletes all corresponding entries in the
 * entry hash table.
 *
 * @param s pointer to scope data structure
 */
void
PFscope_close (PFscope_t *s)
{
    unsigned int n;
    int h;

    assert (s);

    n = PFscope_count (s);

    if (n) {
        while ((h = *((int *) PFarray_at (s->undo, --n))) != SCOPE_BOUNDARY) {
            /* pop undo stack */
            PFarray_del (s->undo);
            
            /* delete corresponding entry in hash table */
            PFarray_del (s->hash[h]);
        }
          
        /* pop current scope's boundary marker */
        PFarray_del (s->undo);
    }
    else
        PFoops (OOPS_FATAL, "closing non-existing scope");
}

/**
 * Perform a lookup for an entry with given key in current scope.
 *
 * @param s pointer to scope data structure
 * @param key identifying key for entry
 * @return pay load for key (or 0 if key is not in scope)
 */
void *
PFscope_lookup (PFscope_t *s, PFqname_t key)
{
    unsigned int h;
    unsigned int n;
    int i;
    PFarray_t *bucket;

    assert (s);

    /* find bucket holding key */
    h = hash (key);
    bucket = s->hash[h];

    /* search bucket last to first and return first match if any (to
     * implement scope visibility) 
     */
    assert (bucket);
    n = PFarray_last (bucket);

    if (n)
        for (i = n - 1; i >= 0; i--) {
            assert (PFarray_at (bucket, i));
            
            if (PFqname_eq (key, 
                            ((entry_t *) PFarray_at (bucket, i))->key) == 0)
                return ((entry_t *) PFarray_at (bucket, i))->value;
        }

    return 0;
}

/**
 * Append all variables in scope @a s2 to scope @a s1.
 * (Destructively updates @a s1, but leaves @a s2 alone.)
 *
 * If you do not want to allow that variables in scope @a s2
 * override variables in scope @a s1, set @a allow_override
 * to false. Overriding will then lead to a PFoops(). See also
 * varscope.c:scope_lib_mod().
 */
void
PFscope_append (PFscope_t *s1, const PFscope_t *s2, bool allow_override)
{
    for (unsigned int h = 0; h < SCOPE_HASH_SZ; h++)

        for (unsigned int i = 0; i < PFarray_last (s2->hash[h]); i++) {

            if (!allow_override
                && PFscope_lookup (
                    s1, (*((entry_t *) PFarray_at (s2->hash[h], i))).key))
                PFoops (OOPS_VARREDEFINED,
                        "redefinition of $%s",
                        PFqname_str ((*((entry_t *) PFarray_at (s2->hash[h],
                                                                i))).key));
            PFscope_into (
                    s1,
                    (*((entry_t *) PFarray_at (s2->hash[h], i))).key,
                    (*((entry_t *) PFarray_at (s2->hash[h], i))).value);
        }
}

/* vim:set shiftwidth=4 expandtab: */
