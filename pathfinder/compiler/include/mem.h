/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
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
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#ifndef MEM_H
#define MEM_H

/* size_t */
#include <stddef.h>

/**
 * Initialize everything related to the memory allocation
 */
void PFmem_init(void);

/**
 * Clean up everything related to the memory allocation
 */
void PFmem_destroy(void);

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
#define PFrealloc(mem,n) PFrealloc_ ((mem), (n), __FILE__, __func__, __LINE__)

void *PFrealloc_ (void *, size_t, const char *, const char *, const int);

char *PFstrndup (const char *str, size_t n);

char *PFstrdup (const char *str);

#endif

/* vim:set shiftwidth=4 expandtab: */
