/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file mem.h
 * Garbage collected memory and string allocation 
 * (@a no allocation of specific objects [parse tree nodes, etc.] here!)
 *
 * $Id$
 */

#ifndef MEM_H
#define MEM_H

/* size_t */
#include <stddef.h>

#include "pathfinder.h"

/**
 * A dynamic buffer that can grow as more space as needed
 * (see #PFneed).
 */
typedef struct PFbuf_t PFbuf_t;

struct PFbuf_t {
  char        *base;  /**< base address of buffer memory */
  unsigned int size;  /**< current size of buffer (in bytes) */
  unsigned int offs;  /**< current buffer write pos */
  unsigned int chunk; /**< chunk size */
};

/** 
 * Tries to allocate @a nbytes bytes of garbage collected heap memory
 * (there is @a no need to explicity free this memory.)
 *
 * NB. __func__ is a C99'ism (gcc has __FUNCTION__).
 *
 * @param n number of bytes to allocate on the heap
 * @return pointer to garbage collected memory of @a n bytes
 */
#define PFmalloc(n) PFmalloc_ ((n), __FILE__, __func__, __LINE__)

void *PFmalloc_ (size_t, const char *, const char *, const int);

/** 
 * Resize a previously allocated memory block (pointed to by @a mem)
 * to @a nbytes.  If @a mem is 0, this behaves like PFmalloc.
 * (NB: there is @a no need to explicity free this memory.)
 *
 * NB. __func__ is a C99'ism (gcc has __FUNCTION__).
 *
 * @param n number of bytes to allocate on the heap
 * @param mem pointer to already allocated heap memory
 * @return pointer to garbage collected memory of @a n bytes
 */
#define PFrealloc(n,mem) PFrealloc_ ((n), (mem), __FILE__, __func__, __LINE__)

void *PFrealloc_ (size_t, void *, const char *, const char *, const int);

char *PFstrndup (char *str, size_t n);

char *PFstrdup (char *str);

#endif

/* vim:set shiftwidth=4 expandtab: */
