/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * M. Raasveldt
 * This file contains a number of functions for acquiring/releasing shared memory
 */

#ifndef _SHAREDMEMORY_LIB_
#define _SHAREDMEMORY_LIB_
 
#include "monetdb_config.h"

#ifdef HAVE_FORK
#include "gdk.h"

#include <stddef.h>

str init_mmap_memory(size_t base_id, size_t id_offset, size_t maxsize, void ***return_ptr, size_t **return_size, char **single_ptr);
str release_mmap_memory(void *ptr, size_t size, size_t id);
str snprintf_mmap_file(str file, size_t max, size_t id);
size_t get_unique_id(size_t offset);

str create_process_semaphore(int id, int count, int *semid);
str get_process_semaphore(int sem_id, int count, int *semid);
str get_semaphore_value(int sem_id, int number, int *semval);
str change_semaphore_value(int sem_id, int number, int change);
str change_semaphore_value_timeout(int sem_id, int number, int change, int timeout_mseconds, bool *succeed);
str release_process_semaphore(int sem_id);
#endif

#endif /* _SHAREDMEMORY_LIB_ */
