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

#ifndef MEM_GC_C__INCLUDES__MEM_C
#include "monetdb_config.h"
#include "pathfinder.h"
#endif

#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "mem.h"

#include "oops.h"

typedef struct PFmem_allocator {
    size_t size;
    size_t nr;
    char **blks;
    size_t used;        /* memory used in last block */
} PFmem_allocator;

#define SA_BLOCK (4*1024*1024)

static PFmem_allocator *pf_alloc = NULL;

void
PFmem_init(void)
{
    assert (!pf_alloc);

    pf_alloc = (PFmem_allocator*)malloc(sizeof(PFmem_allocator));

    pf_alloc->size = 64;
    pf_alloc->nr = 1;
    pf_alloc->blks = (char**)malloc(pf_alloc->size*sizeof(char*));
    pf_alloc->blks[0] = (char*)malloc(SA_BLOCK);
    pf_alloc->used = 0;
}

void
PFmem_destroy(void)
{
    unsigned int i ;

    assert (pf_alloc);

    for (i = 0; i<pf_alloc->nr; i++) {
        free(pf_alloc->blks[i]);
    }
    free(pf_alloc->blks);
    free(pf_alloc);

    pf_alloc = NULL;
}

#define round16(sz) ((sz+15)&~15)
static char *
mem_alloc (PFmem_allocator *pa, size_t sz)
{
    char *r;
    sz = round16(sz);
    if (sz > SA_BLOCK) {
        char *t;
        /* malloc new big block */
        char *r = malloc(sz);
        if (pa->nr >= pa->size) {
            /* double the number of block references */
            pa->size *=2;
            pa->blks = (char**)realloc(pa->blks,pa->size*sizeof(char*));
        }
        /* fill in new big block before the last block */
        t = pa->blks[pa->nr-1];
        pa->blks[pa->nr-1] = r;
        pa->blks[pa->nr] = t;
        pa->nr ++;
        return r;
    }
    else if (sz > (SA_BLOCK-pa->used)) {
        /* there is not enough free memory in the current block
           so malloc a new block */
        char *r = malloc(SA_BLOCK);
        if (pa->nr >= pa->size) {
            /* double the number of block references */
            pa->size *=2;
            pa->blks = (char**)realloc(pa->blks,pa->size*sizeof(char*));
        }
        pa->blks[pa->nr] = r;
        pa->nr ++;
        /* reset the current `used' pointer */
        pa->used = sz;
        return r;
    }
    /* default case */
    r = pa->blks[pa->nr-1] + pa->used;
    pa->used += sz;
    return r;
}

static char *
mem_realloc (PFmem_allocator *pa, char *p, size_t old_n, size_t n)
{
        char *r = mem_alloc( pa, n);
        size_t i;
        for(i=0; i < old_n; i++) r[i] = p [i];
        return r;
}

/**
 * Worker for #PFmalloc ().
 */
void *
PFmalloc_ (size_t n, const char *file, const char *func, const int line)
{
    void *mem;
    /* allocate garbage collected heap memory of requested size */
    mem = mem_alloc (pf_alloc, n);

    if (mem == 0) {
        /* don't use PFoops () here as it tries to allocate even more memory */
        PFlog ("fatal error: insufficient memory (allocating "SZFMT" bytes failed) in %s (%s), line %d",
                n, file, func, line);
        PFexit(-OOPS_FATAL);
    }

    memset(mem, 0, n);

    return mem;
}

/**
 * Worker for #PFrealloc ().
 */
void *
PFrealloc_ (void *mem, size_t old_n, size_t n,
            const char *file, const char *func, const int line)
{
    /* resize garbage collected heap memory to requested size */
    mem = mem_realloc (pf_alloc, mem, old_n, n);

    if (mem == 0) {
        /* don't use PFoops () here as it tries to allocate even more memory */
        PFlog ("fatal error: insufficient memory (re-allocating "SZFMT" bytes failed) in %s (%s), line %d",
                n, file, func, line);
        PFexit(-OOPS_FATAL);
    }

    if (n > old_n)
        memset((char *) mem + old_n, 0, n - old_n);

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
