/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file mem.c
 * Garbage collected memory and string allocation
 * (@a no allocation of specific objects [parse tree nodes, etc.] here!)
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
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2011 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id$
 */

#include "monetdb_config.h"
#include "pathfinder.h"

#ifndef HAVE_GC
/* we need a fallback solution in case
   we have no garbage collector */
#define MEM_GC_C__INCLUDES__MEM_C
#include "mem.c"

#else
/* we have a garbage collector
   (thus use it to implement the memory interface) */

#include <string.h>
#include <stdlib.h>

#include "gc.h"
#include "mem.h"
#include "oops.h"

void
PFmem_init(void)
{
    GC_INIT ();
}

void
PFmem_destroy(void)
{
}

/**
 * Worker for #PFmalloc ().
 */
void *
PFmalloc_ (size_t n, const char *file, const char *func, const int line)
{
    void *mem;
    /* allocate garbage collected heap memory of requested size */
    mem = GC_MALLOC (n);

    if (mem == 0) {
        /* don't use PFoops () here as it tries to allocate even more memory */
        PFlog ("fatal error: insufficient memory (allocating "SZFMT" bytes failed) in %s (%s), line %d",
                n, file, func, line);
        PFexit(-OOPS_FATAL);
    }

    return mem;
}

/**
 * Worker for #PFrealloc ().
 */
void *
PFrealloc_ (void *mem, size_t old_n, size_t n,
            const char *file, const char *func, const int line)
{
    (void) old_n;
    /* resize garbage collected heap memory to requested size */
    mem = GC_REALLOC (mem, n);

    if (mem == 0) {
        /* don't use PFoops () here as it tries to allocate even more memory */
        PFlog ("fatal error: insufficient memory (re-allocating "SZFMT" bytes failed) in %s (%s), line %d",
                n, file, func, line);
        PFexit(-OOPS_FATAL);
    }

    return mem;
}

/**
 * Allocates enough memory to hold a copy of @a str
 * and return a pointer to this copy
 * If you specify @a n != 0, the copy will hold @a n characters (+ the
 * trailing '\\0') only.
 * @param str string to copy
 * @param len copy @a len characters only
 * @return pointer to newly allocated (partial) copy of @a str
 */
char *
PFstrndup (const char *str, size_t len)
{
    char *copy;

    /* + 1 to hold end of string marker '\0' */
    copy = (char *) PFmalloc (len + 1);

    (void) strncpy (copy, str, len);

    /* force end of string marker '\0' */
    copy[len] = '\0';

    return copy;
}

/**
 * Allocates enough memory to copy @a str and return a pointer to it
 * (calls #PFcopyStrn with @a n == 0),
 * @param str string to copy
 * @return pointer to newly allocated copy of @a str
 */
char *
PFstrdup (const char *str)
{
    return PFstrndup (str, strlen (str));
}

#endif

/* vim:set shiftwidth=4 expandtab: */
