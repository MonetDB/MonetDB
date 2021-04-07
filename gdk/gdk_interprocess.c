/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"

#ifdef HAVE_FORK

#include "gdk_interprocess.h"
#include "gdk.h"
#include "gdk_private.h"
#include "mutils.h"

#include <string.h>

#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sched.h>
#include <sys/sem.h>
#include <time.h>

static ATOMIC_TYPE interprocess_unique_id = ATOMIC_VAR_INIT(1);
static key_t base_key = 800000000;

// Regular ftok produces too many collisions
static inline void
ftok_enhanced(int id, key_t * return_key)
{
	*return_key = base_key + id;
}

//! Obtain a set of unique identifiers that can be used to create memory mapped files or semaphores
/* offset: The amount of unique identifiers necessary
 * return: The first unique identifier reserved. The consecutive [offset] identifiers are also reserved.
 *  (ex. if offset = 5 and the return value is 10, then the identifiers 10-14 are reserved)
*/
size_t
GDKuniqueid(size_t offset)
{
	return (size_t) ATOMIC_ADD(&interprocess_unique_id, (ATOMIC_BASE_TYPE) offset);
}

//! Create a memory mapped file if it does not exist and open it
/* id: The unique identifier of the memory mapped file (use GDKuniquemmapid to get a unique identifier)
 * size: Minimum required size of the file
 * return: Return value pointing into the file, NULL if not successful
*/
void *
GDKinitmmap(size_t id, size_t size, size_t *return_size)
{
	char address[100];
	void *ptr;
	int fd;
	int mod = MMAP_READ | MMAP_WRITE | MMAP_SEQUENTIAL | MMAP_SYNC | MAP_SHARED;
	char *path;

	GDKmmapfile(address, sizeof(address), id);

	/* round up to multiple of GDK_mmap_pagesize with a
	 * minimum of one
	 size = (maxsize + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
	 if (size == 0)
	 size = GDK_mmap_pagesize; */
	path = GDKfilepath(0, BATDIR, address, "tmp");
	if (path == NULL) {
		return NULL;
	}
	fd = GDKfdlocate(NOFARM, path, "wb", NULL);
	if (fd < 0) {
		GDKfree(path);
		return NULL;
	}
	if (GDKextendf(fd, size, path) != GDK_SUCCEED) {
		close(fd);
		GDKfree(path);
		return NULL;
	}
	close(fd);
	ptr = GDKmmap(path, mod, size);
	GDKfree(path);
	if (ptr == NULL) {
		return NULL;
	}
	if (return_size != NULL) {
		*return_size = size;
	}
	return ptr;
}

//! Release a memory mapped file that was created through GDKinitmmap
/* ptr: Pointer to the file
 * size: Size of the file
 * id: Identifier of the file
 * return: GDK_SUCCEED if successful, GDK_FAIL if not successful
*/
gdk_return
GDKreleasemmap(void *ptr, size_t size, size_t id)
{
	char address[100];
	char *path;
	int ret;
	GDKmmapfile(address, sizeof(address), id);
	if (GDKmunmap(ptr, size) != GDK_SUCCEED) {
		return GDK_FAIL;
	}
	path = GDKfilepath(0, BATDIR, address, "tmp");
	if (path == NULL) {
		return GDK_FAIL;
	}
	ret = MT_remove(path);
	if (ret < 0)
		GDKsyserror("cannot remove '%s'", path);
	GDKfree(path);
	return ret < 0 ? GDK_FAIL : GDK_SUCCEED;
}

//! snprintf the file name of a memory mapped file (as created by GDKinitmmap)
/* buffer: The buffer to write the name to
 * max: The maxsize of the buffer (should be at least ~10 characters)
 * id: Identifier of the file
*/
gdk_return
GDKmmapfile(str buffer, size_t max, size_t id)
{
	snprintf(buffer, max, "pymmap%zu", id);
	return GDK_SUCCEED;
}

static gdk_return
interprocess_init_semaphore(int id, int count, int flags, int *semid)
{
	key_t key;
	ftok_enhanced(id, &key);
	*semid = semget(key, count, flags | 0666);
	if (*semid < 0) {
		GDKsyserror("semget failed");
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

//! Create an interprocess semaphore
/* id: identifier (obtain from GDKuniqueid)
 * count: amount of semaphores
 * semid: identifier of the created semaphore (only set if function returns GDK_SUCCEED)
 */
gdk_return
GDKcreatesem(int id, int count, int *semid)
{
	return interprocess_init_semaphore(id, count, IPC_CREAT, semid);
}

//! Get an interprocess semaphore that was already created using GDKcreatesem
/* id: identifier (obtain from GDKuniqueid)
 * count: amount of semaphores
 * semid: identifier of the semaphore (only set if function returns GDK_SUCCEED)
 */
gdk_return
GDKgetsem(int id, int count, int *semid)
{
	return interprocess_init_semaphore(id, count, 0, semid);
}

//! Gets the value of an interprocess semaphore
/* sem_id: semaphore identifier (obtained from GDKcreatesem or GDKgetsem)
 * number: the semaphore number (must be less than 'count' given when creating the semaphore)
 * semval: the value of the semaphore (only set if function returns GDK_SUCCEED)
 */
gdk_return
GDKgetsemval(int sem_id, int number, int *semval)
{
	*semval = semctl(sem_id, number, GETVAL, 0);
	if (*semval < 0) {
		GDKsyserror("semctl failed");
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

//! Change the value of an interprocess semaphore
/* sem_id: semaphore identifier (obtained from GDKcreatesem or GDKgetsem)
 * number: the semaphore number (must be less than 'count' given when creating the semaphore)
 * change: The change to apply to the semaphore value
 */
gdk_return
GDKchangesemval(int sem_id, int number, int change)
{
	struct sembuf buffer;
	buffer.sem_num = number;
	buffer.sem_op = change;
	buffer.sem_flg = 0;

	if (semop(sem_id, &buffer, 1) < 0) {
		GDKsyserror("semop failed");
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

//! Change the value of an interprocess semaphore with a timeout
/* sem_id: semaphore identifier (obtained from GDKcreatesem or GDKgetsem)
 * number: the semaphore number (must be less than 'count' given when creating the semaphore)
 * change: The change to apply to the semaphore value
 * timeout_mseconds: The timeout in milliseconds
 * succeed: Set to true if the value was successfully changed, or false if the timeout was reached
 */
gdk_return
GDKchangesemval_timeout(int sem_id, int number, int change, int timeout_mseconds, bool *succeed)
{
#ifdef HAVE_SEMTIMEDOP
	// Some linux installations don't have semtimedop
	// The easiest solution is to just call semop instead
	// The only reason we use semtimedop is to prevent deadlocks when there are segfaults in a subprocess, which really shouldn't happen anyway
	// So having semtimedop is not vital to the functioning of the program
	struct timespec timeout;
	struct sembuf buffer;
	buffer.sem_num = number;
	buffer.sem_op = change;
	buffer.sem_flg = 0;
	*succeed = false;

	timeout.tv_sec = (timeout_mseconds / 1000);
	timeout.tv_nsec = (timeout_mseconds % 1000) * 1000;

	if (semtimedop(sem_id, &buffer, 1, &timeout) < 0) {
		if (errno == EAGAIN || errno == EINTR) {
			// operation timed out; not an error
			errno = 0;
			return GDK_SUCCEED;
		} else {
			GDKsyserror("semtimedop failed");
			return GDK_FAIL;
		}
	}
	*succeed = true;
	return GDK_SUCCEED;
#else
	(void) timeout_mseconds;
	*succeed = true;
	return GDKchangesemval(sem_id, number, change);
#endif
}

//! Destroy an interprocess semaphore
/* sem_id: semaphore identifier (obtained from GDKcreatesem or GDKgetsem)
 */
gdk_return
GDKreleasesem(int sem_id)
{
	if (semctl(sem_id, 0, IPC_RMID) < 0) {
		GDKsyserror("semctl failed");
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

// align to 8 bytes
#define align(sz) ((sz + 7) & ~7)

size_t
GDKbatcopysize(BAT *bat, str colname)
{
	size_t size = 0;

	size += align(strlen(colname) + 1);	//[COLNAME]
	size += align(sizeof(BAT));	//[BAT]
	size += align(bat->twidth * BATcount(bat));	//[DATA]

	if (bat->tvheap != NULL) {
		size += align(sizeof(Heap));	//[VHEAP]
		size += align(bat->tvheap->size);	//[VHEAPDATA]
	}
	return size;
}

size_t
GDKbatcopy(char *dest, BAT *bat, str colname)
{
	size_t batsize = bat->twidth * BATcount(bat);
	size_t position = 0;

	//[COLNAME]
	memcpy(dest + position, colname, strlen(colname) + 1);
	position += align(strlen(colname) + 1);
	//[BAT]
	memcpy(dest + position, bat, sizeof(BAT));
	position += align(sizeof(BAT));
	//[DATA]
	memcpy(dest + position, Tloc(bat, 0), batsize);
	position += align(batsize);
	if (bat->tvheap != NULL) {
		//[VHEAP]
		memcpy(dest + position, bat->tvheap, sizeof(Heap));
		position += align(sizeof(Heap));
		//[VHEAPDATA]
		memcpy(dest + position, bat->tvheap->base, bat->tvheap->size);
		position += align(bat->tvheap->size);
	}
	return position;
}

size_t
GDKbatread(char *src, BAT **bat, str *colname)
{
	size_t position = 0;
	BAT *b;
	//load the data for this column from shared memory
	//[COLNAME]
	*colname = src + position;
	position += align(strlen(*colname) + 1);
	//[BAT]
	b = (BAT *) (src + position);
	position += align(sizeof(BAT));
	//[DATA]
	b->theap->base = (void *) (src + position);
	position += align(b->twidth * BATcount(b));
	if (b->tvheap != NULL) {
		//[VHEAP]
		b->tvheap = (Heap *) (src + position);
		position += align(sizeof(Heap));
		//[VHEAPDATA]
		b->tvheap->base = (void *) (src + position);
		position += align(b->tvheap->size);
	}
	*bat = b;
	return position;
}

#endif
