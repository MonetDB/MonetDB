/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * M. Raasveldt
 * This file contains a number of functions for interprocess communication.
 */

#ifndef _GDK_INTERPROCES_H_
#define _GDK_INTERPROCES_H_
 
#include "monetdb_config.h"

#ifdef HAVE_FORK
#include "gdk.h"

#include <stddef.h>

//! Obtain a set of unique identifiers that can be used to create memory mapped files or semaphores
gdk_export size_t GDKuniqueid(size_t offset);

/* 
 * Memory-Mapped File operations, used for transporting data between processes 
 */

//! Create a memory mapped file if it does not exist and open it
gdk_export gdk_return GDKinitmmap(size_t id, size_t size, void **return_ptr, size_t *return_size, str *msg);
//! Release a memory mapped file that was created through GDKinitmmap
gdk_export gdk_return GDKreleasemmap(void *ptr, size_t size, size_t id, str *msg);
//! snprintf the file name of a memory mapped file (as created by GDKinitmmap)
gdk_export gdk_return GDKmmapfile(str buffer, size_t max, size_t id);

/* 
 * Interprocess-Semaphores, used for cross process lock operations 
 */

//! Create an interprocess semaphore 
gdk_export gdk_return GDKcreatesem(int id, int count, int *semid, str *msg);
//! Get an interprocess semaphore that was already created using GDKcreatesem
gdk_export gdk_return GDKgetsem(int sem_id, int count, int *semid, str *msg);
//! Gets the value of an interprocess semaphore
gdk_export gdk_return GDKgetsemval(int sem_id, int number, int *semval, str *msg);
//! Change the value of an interprocess semaphore
gdk_export gdk_return GDKchangesemval(int sem_id, int number, int change, str *msg);
//! Change the value of an interprocess semaphore with a timeout
gdk_export gdk_return GDKchangesemval_timeout(int sem_id, int number, int change, int timeout_mseconds, bool *succeed, str *msg);
//! Destroy an interprocess semaphore
gdk_export gdk_return GDKreleasesem(int sem_id, str *msg);

//str init_mmap_memory(size_t base_id, size_t id_offset, size_t maxsize, void ***return_ptr, size_t **return_size, char **single_ptr);
//str release_mmap_memory(void *ptr, size_t size, size_t id);
//str snprintf_mmap_file(str file, size_t max, size_t id);
#endif

#endif /* _GDK_INTERPROCES_H_ */
