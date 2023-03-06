/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _PP_MEM_H_
#define _PP_MEM_H_

#define SA_BLOCK 16*1024

typedef struct mallocator {
    size_t size;
    size_t nr;
    char **blks;
    size_t used;    /* memory used in last block */
    size_t usedmem; /* used memory */
} mallocator;

extern void ma_destroy(mallocator* ma);
extern mallocator *ma_create(void);
extern void *ma_alloc(mallocator *ma, size_t sz);
extern char *ma_strdup(mallocator *ma, const char *s );
extern char *ma_copy(mallocator *ma, const void *s, int l);

#endif /*_PP_MEM_H_*/
