/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Functions and data structures to support environments for various
 * Pathfinder modules.
 *
 * An environment maintains a key |--> value mapping.  (The keys are
 * QNames, the values are arbitrary (void *) and not interpreted by the
 * environment.)
 *
 * Several bindings for the same key are possible.  The environment
 * actually maintains an array of values for each key.  The actual
 * environment layout thus is, e.g.,
 *
 * @verbatim
                             key1 |--> value11
                             key2 |--> value21, value22, value23
                             key3 |--> value31, value32
                                    .
                                    .
                                    .
@endverbatim
 *
 * This environment thus supports, e.g., polymorphic functions (functions
 * sharing a common name but having differing types).
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

#include "pathfinder.h"

#include <assert.h>
#include <stdlib.h>

#include "env.h"
/* PFqname_t */
#include "qname.h"
/* PFarray_t */
#include "array.h"

/** a binding key |--> value, value, ... */
typedef struct binding_t binding_t;

struct binding_t {
    PFqname_t  key;
    PFarray_t *values;
};

/**
 * Create a new empty environment.
 * 
 * @return pointer to environment
 */
PFenv_t *
PFenv (void)
{
    return (PFenv_t *) PFarray (sizeof (binding_t));
}

/**
 * Lookup for binding with given key.  Returns the array of values
 * bound to key, or 0 if key is not bound in environment.
 *
 * @param e pointer to environment
 * @param key key to lookup
 * @return array of values bound to key (or 0 if not found)
 */
PFarray_t *
PFenv_lookup (PFenv_t *e, PFqname_t key)
{
    /*
     * The PFarray_... functions do not automatically adjust the
     * last information, except for the PFarray_add() and PFarray_del()
     * functions.  We rely on array cells automatically being initialized
     * with NULL and, hence, take care of this initialization here.
     */
    while (PFarray_last (e) <= key)
        *((PFarray_t **) PFarray_add (e)) = NULL;

    return *((PFarray_t **) PFarray_at (e, key));
}

/**
 * Bind key to value (insert binding key |--> value) in environment.
 *
 * @param  e pointer to enviroment
 * @param  key key to bind
 * @param  value to bind to key
 * @return array of values key is now bound to (or 0 if key was unbound)
 */
PFarray_t *
PFenv_bind (PFenv_t *e, PFqname_t key, void *value)
{
    PFarray_t *a;

    assert (e);

    /*
     * The PFarray_... functions do not automatically adjust the
     * last information, except for the PFarray_add() and PFarray_del()
     * functions.  We rely on array cells automatically being initialized
     * with NULL and, hence, take care of this here.
     */
    while (PFarray_last (e) <= key)
        *((PFarray_t **) PFarray_add (e)) = NULL;

    a = *((PFarray_t **) PFarray_at (e, key));

    if (! a)
        *((PFarray_t **) PFarray_at (e, key)) = PFarray (sizeof (void *));

    *((void **) PFarray_add (*((PFarray_t **) PFarray_at (e, key)))) = value;

    return a;
}

/**
 * Iterate over all bound values in environment @a e and call the
 * callback function @a fn for each value.
 *
 * @param e  environment to iterate over
 * @param fn callback function
 */
void
PFenv_iterate (PFenv_t *e, void (*fn) (PFqname_t, void *))
{
    for (unsigned int i = 0; i < PFarray_last (e); i++) {
        PFarray_t *a = *(PFarray_t **) PFarray_at (e, i);
        if (a)
            for (unsigned int j = 0; j < PFarray_last (a); j++)
                fn (i, PFarray_at (a, j));
    }
}

#if 0
/**
 * Iterate over all bound keys in environment @a e and call the
 * callback function @a fun for each of key.
 *
 * @param e  environment to iterate over
 * @param fn callback function
 */
void
PFenv_key_iterate (PFenv_t *e, void (*fn) (PFqname_t))
{
    for (unsigned int i = 0; i < PFarray_last (e); i++) {
        PFarray_t *a = *(PFarray_t **) PFarray_at (e, i);
        if (a)
            fn (i);
    }
}
#endif

/* vim:set shiftwidth=4 expandtab: */
