
#include "gdk_interprocess.h"

#ifdef HAVE_FORK

#include "gdk.h"
#include "gdk_private.h"
#include "../monetdb5/mal/mal_exception.h"

#include <stdlib.h>
#include <assert.h>
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
#include <errno.h>
#include <sys/sem.h>
#include <time.h>

static size_t interprocess_unique_id = 1;
static key_t base_key = 800000000;

gdk_return interprocess_init_semaphore(int id, int count, int flags, int *semid, str *msg);

// Regular ftok produces too many collisions
static gdk_return 
ftok_enhanced(int id, key_t *return_key) {
    *return_key = base_key + id;
    return GDK_SUCCEED;
}

#define interprocess_create_error(...) {             \
    *msg = (char*) GDKzalloc(500 * sizeof(char));    \
    snprintf(*msg, 500, __VA_ARGS__);                 \
    errno = 0;                                       \
}

//! Obtain a set of unique identifiers that can be used to create memory mapped files or semaphores
/* offset: The amount of unique identifiers necessary
 * return: The first unique identifier reserved. The consecutive [offset] identifiers are also reserved.
 *  (ex. if offset = 5 and the return value is 10, then the identifiers 10-14 are reserved)
*/
size_t 
GDKuniqueid(size_t offset) {
    // TODO: lock this here instead of in pyapi
    size_t id;

    id = interprocess_unique_id;
    interprocess_unique_id += offset;
    return id;
}

//! Create a memory mapped file if it does not exist and open it
/* id: The unique identifier of the memory mapped file (use GDKuniquemmapid to get a unique identifier)
 * size: Minimum required size of the file
 * return_ptr: Return value pointing into the file
 * msg: Error message (only set if function returns GDK_FAIL)
 * return: GDK_SUCCEED if successful, GDK_FAIL if not successful (with msg set to error message)
*/
gdk_return 
GDKinitmmap(size_t id, size_t size, void **return_ptr, str *msg) {
    char address[100];
    void *ptr;
    int fd;
    int mod = MMAP_READ | MMAP_WRITE | MMAP_SEQUENTIAL | MMAP_SYNC  | MAP_SHARED;
    char *path = NULL;
    snprintf_mmap_file(address, 100, id);

    /* round up to multiple of GDK_mmap_pagesize with a
     * minimum of one 
    size = (maxsize + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
    if (size == 0)
        size = GDK_mmap_pagesize; */
    fd = GDKfdlocate(0, address, "wb", "tmp");
    if (fd < 0) {
        interprocess_create_error("Failure in GDKfdlocate: %s", strerror(errno));
        goto cleanup;
    }
    path = GDKfilepath(0, BATDIR, address, "tmp");
    if (path == NULL) {
        interprocess_create_error("Failure in GDKfilepath: %s", strerror(errno));
        goto cleanup;
    }
    close(fd);
    if (GDKextend(path, size) != GDK_SUCCEED) {
        interprocess_create_error("Failure in GDKextend: %s", strerror(errno));
        goto cleanup;
    }
    ptr = GDKmmap(path, mod, size);
    if (ptr == NULL) {
        interprocess_create_error("Failure in GDKmmap: %s", strerror(errno));
        goto cleanup;
    }
    GDKfree(path);
    if (return_ptr != NULL) (*return_ptr) = ptr;
    return GDK_SUCCEED;
cleanup:
    if (path) GDKfree(path);
    return GDK_FAIL;
}

//! Release a memory mapped file that was created through GDKinitmmap
/* ptr: Pointer to the file
 * size: Size of the file
 * id: Identifier of the file
 * msg: Error message (only set if function returns GDK_FAIL)
 * return: GDK_SUCCEED if successful, GDK_FAIL if not successful (with msg set to error message)
*/
gdk_return 
GDKreleasemmap(void *ptr, size_t size, size_t id, str *msg) {
    char address[100];
    char *path;
    int ret;
    snprintf_mmap_file(address, 100, id);
    if (GDKmunmap(ptr, size) != GDK_SUCCEED) {
        interprocess_create_error("Failure in GDKmunmap: %s", strerror(errno));
        return GDK_FAIL;
    }
    path = GDKfilepath(0, BATDIR, address, "tmp");
    if (path == NULL) {
        interprocess_create_error("Failure in GDKfilepath: %s", strerror(errno));
        return GDK_FAIL;
    }
    ret = remove(path);
    GDKfree(path);
    if (ret < 0) {
        interprocess_create_error("Failure in GDKfree: %s", strerror(errno));
        return GDK_FAIL;
    }
    return GDK_SUCCEED;
}

//! snprintf the file name of a memory mapped file (as created by GDKinitmmap)
/* buffer: The buffer to write the name to
 * max: The maxsize of the buffer (should be at least ~10 characters)
 * id: Identifier of the file
*/
gdk_return 
GDKmmapfile(str buffer, size_t max, size_t id) {
    snprintf(buffer, max, "pymmap%zu", id);
    return GDK_SUCCEED;
}

gdk_return 
interprocess_init_semaphore(int id, int count, int flags, int *semid, str *msg) {
    key_t key;
    if (ftok_enhanced(id, &key) != GDK_SUCCEED) {
        interprocess_create_error("Failure in ftok_enhanced: %s", strerror(errno));
        return GDK_FAIL;
    }
    *semid = semget(key, count, flags | 0666);
    if (*semid < 0) {
        interprocess_create_error("Failure in semget: %s", strerror(errno));
        return GDK_FAIL;
    }
    return GDK_SUCCEED;
}

//! Create an interprocess semaphore 
/* id: identifier (obtain from GDKuniqueid)
 * count: amount of semaphores
 * semid: identifier of the created semaphore (only set if function returns GDK_SUCCEED)
 * msg: Error message (only set if function returns GDK_FAIL)
 */
gdk_return 
GDKcreatesem(int id, int count, int *semid, str *msg) {
    return interprocess_init_semaphore(id, count, IPC_CREAT, semid, msg);
}

//! Get an interprocess semaphore that was already created using GDKcreatesem
/* id: identifier (obtain from GDKuniqueid)
 * count: amount of semaphores
 * semid: identifier of the semaphore (only set if function returns GDK_SUCCEED)
 * msg: Error message (only set if function returns GDK_FAIL)
 */
gdk_return 
GDKgetsem(int id, int count, int *semid, str *msg) {
    return interprocess_init_semaphore(id, count, 0, semid, msg);
}

//! Gets the value of an interprocess semaphore
/* sem_id: semaphore identifier (obtained from GDKcreatesem or GDKgetsem)
 * number: the semaphore number (must be less than 'count' given when creating the semaphore)
 * semval: the value of the semaphore (only set if function returns GDK_SUCCEED)
 * msg: Error message (only set if function returns GDK_FAIL)
 */
gdk_return 
GDKgetsemval(int sem_id, int number, int *semval, str *msg) {
    *semval = semctl(sem_id, number, GETVAL, 0);
    if (*semval < 0) {
        interprocess_create_error("Failure in semctl: %s", strerror(errno));
        return GDK_FAIL;
    }
    return GDK_SUCCEED;
}

//! Change the value of an interprocess semaphore
/* sem_id: semaphore identifier (obtained from GDKcreatesem or GDKgetsem)
 * number: the semaphore number (must be less than 'count' given when creating the semaphore)
 * change: The change to apply to the semaphore value
 * msg: Error message (only set if function returns GDK_FAIL)
 */
gdk_return 
GDKchangesemval(int sem_id, int number, int change, str *msg) {
    struct sembuf buffer;
    buffer.sem_num = number;
    buffer.sem_op = change;
    buffer.sem_flg = 0;

    if (semop(sem_id, &buffer, 1) < 0) {
        interprocess_create_error("Failure in semop: %s", strerror(errno));
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
 * msg: Error message (only set if function returns GDK_FAIL)
 */
gdk_return 
GDKchangesemval_timeout(int sem_id, int number, int change, int timeout_mseconds, bool *succeed, str *msg) {
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
            interprocess_create_error("Failure in semtimedop: %s", strerror(errno));
            return GDK_FAIL;
        }
    }
    *succeed = true;
    return GDK_SUCCEED;
#else
    (void) timeout_mseconds;
    *succeed = true;
    return GDKchangesemval(sem_id, number, change, msg);
#endif
}

//! Destroy an interprocess semaphore
/* sem_id: semaphore identifier (obtained from GDKcreatesem or GDKgetsem)
 * msg: Error message (only set if function returns GDK_FAIL)
 */
gdk_return 
GDKreleasesem(int sem_id, str *msg) {
    if (semctl(sem_id, 0, IPC_RMID) < 0) {
        interprocess_create_error("Failure in semctl: %s", strerror(errno))
        return GDK_FAIL;
    }
    return GDK_SUCCEED;
}

str snprintf_mmap_file(str file, size_t max, size_t id) {
    snprintf(file, max, "pymmap%zu", id);
    return MAL_SUCCEED;
}

str init_mmap_memory(size_t base_id, size_t id_offset, size_t maxsize, void ***return_ptr, size_t **return_size, char **single_ptr) {
    char address[100];
    void *ptr;
    int fd;
    size_t size = maxsize;
    int mod = MMAP_READ | MMAP_WRITE | MMAP_SEQUENTIAL | MMAP_SYNC  | MAP_SHARED;
    char *path = NULL;
    snprintf_mmap_file(address, 100, base_id + id_offset);

    /* round up to multiple of GDK_mmap_pagesize with a
     * minimum of one 
    size = (maxsize + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
    if (size == 0)
        size = GDK_mmap_pagesize; */
    fd = GDKfdlocate(0, address, "wb", "tmp");
    if (fd < 0) {
        return createException(MAL, "interprocess.get", "Failure in GDKfdlocate(0, %s, \"wb\", NULL)", address);
    }
    path = GDKfilepath(0, BATDIR, address, "tmp");
    if (path == NULL) {
        return createException(MAL, "interprocess.get", "Failure in GDKfilepath(0, "BATDIR",%s,\"tmp\")", address);
    }
    close(fd);
    if (GDKextend(path, size) != GDK_SUCCEED) {
        return createException(MAL, "interprocess.get", "Failure in GDKextend(%s,%zu)", path, size);
    }
    ptr = GDKmmap(path, mod, size);
    if (ptr == NULL) {
        return createException(MAL, "interprocess.get", "Failure in GDKmmap(%s, %d, %zu)", path, mod, size);
    }
    GDKfree(path);
    if (return_ptr != NULL) (*return_ptr)[id_offset] = ptr;
    if (return_size != NULL) (*return_size)[id_offset] = size;
    if (single_ptr != NULL) *single_ptr = ptr;
    return MAL_SUCCEED;
}

str release_mmap_memory(void *ptr, size_t size, size_t id) {
    char address[100];
    char *path;
    int ret;
    snprintf_mmap_file(address, 100, id);
    if (GDKmunmap(ptr, size) != GDK_SUCCEED) {
        return createException(MAL, "interprocess.get", "Failure in GDKmunmap(%p, %zu)", ptr, size);
    }
    path = GDKfilepath(0, BATDIR, address, "tmp");
    if (path == NULL) {
        return createException(MAL, "interprocess.get", "Failure in GDKfilepath(0, "BATDIR",%s,\"tmp\")", address);
    }
    ret = remove(path);
    GDKfree(path);
    if (ret < 0) {
        perror(strerror(errno));
        return createException(MAL, "interprocess.get", "Failure in remove(%s)", path);
    }
    return MAL_SUCCEED;
}


#endif
