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
 * $Id$
 */

#include <assert.h>

#include "scope.h"

/* PFarray_t */
#include "array.h"

/* PFqname_t */
#include "qname.h"

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
PFscope ()
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

    k = PFqname_str (key);

    for (i = 0; *k; k++)
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
PFscope_count (PFscope_t *s)
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


/* vim:set shiftwidth=4 expandtab: */
