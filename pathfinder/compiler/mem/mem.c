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
 *  The contents of this file are subject to the MonetDB Public
 *  License Version 1.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 *
 *  The Original Code is the ``Pathfinder'' system. The Initial
 *  Developer of the Original Code is the Database & Information
 *  Systems Group at the University of Konstanz, Germany. Portions
 *  created by U Konstanz are Copyright (C) 2000-2005 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

#include "pathfinder.h"

#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "mem.h"

#include "oops.h"

#ifdef HAVE_GC
/** undefine this for production code (GC warning messages) */
#define GC_DEBUG

/** we'll interface to Hans Boehm's C garbage collector */
#if HAVE_GC_H
#include <gc.h>
#else
#if HAVE_GC_GC_H
#include <gc/gc.h>
#else
#error "Interface to garbage collector (gc.h) not available."
#endif
#endif
#else
#define GC_MALLOC(n)	malloc(n)
#define GC_REALLOC(p, n)	realloc(p, n)
#endif  /* HAVE_GC */

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
        PFlog ("fatal error: insufficient memory in %s (%s), line %d", 
                file, func, line);
        PFexit(-OOPS_FATAL);
    }

    return mem;
}

/**
 * Worker for #PFrealloc ().
 */
void *
PFrealloc_ (size_t n, void *mem, 
	    const char *file, const char *func, const int line) 
{
    /* resize garbage collected heap memory to requested size */
    mem = GC_REALLOC (mem, n);

    if (mem == 0) {
        /* don't use PFoops () here as it tries to allocate even more memory */
        PFlog ("fatal error: insufficient memory in %s (%s), line %d", 
                file, func, line);
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


/* vim:set shiftwidth=4 expandtab: */
