/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * M. Raasveldt
 * This file contains a number of functions for interprocess communication.
 */

#ifndef _GDK_INTERPROCES_H_
#define _GDK_INTERPROCES_H_

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

/*
 * Operations for copying a BAT into a memory mapped file
 */

//! Returns the size of the buffer necessary to copy the BAT into
gdk_export size_t GDKbatcopysize(BAT *bat, str colname);
//! Copies a BAT into the given destination. Returns the amount of bytes copied (equiv. to GDKbatcopysize(bat))
gdk_export size_t GDKbatcopy(char *dest, BAT *bat, str colname);
//! Reads a BAT from the given source (one that was copied into by GDKbatcopy)
gdk_export size_t GDKbatread(char *src, BAT **bat, str *colname);


#endif

#endif /* _GDK_INTERPROCES_H_ */
