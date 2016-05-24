
#include "interprocess.h"

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

static size_t shm_unique_id = 1;
static key_t base_key = 800000000;

str ftok_enhanced(int id, key_t *return_key);
str init_process_semaphore(int id, int count, int flags, int *semid);

size_t get_unique_id(size_t offset) {
    size_t id;

    id = shm_unique_id;
    shm_unique_id += offset;
    return id;
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

str ftok_enhanced(int id, key_t *return_key) {
    *return_key = base_key + id;
    return MAL_SUCCEED;
}

str init_process_semaphore(int id, int count, int flags, int *semid) {
    str msg = MAL_SUCCEED;
    key_t key;
    msg = ftok_enhanced(id, &key);
    if (msg != MAL_SUCCEED) {
        return msg;
    }
    *semid = semget(key, count, flags | 0666);
    if (*semid < 0) {
        char *err = strerror(errno);
        errno = 0;
        return createException(MAL, "semaphore.init", "Error calling semget(key:%d,nsems:%d,semflg:%d): %s", 0, count, flags | 0666, err);
    }
    return msg;
}

str create_process_semaphore(int id, int count, int *semid) {
    return init_process_semaphore(id, count, IPC_CREAT, semid);
}

str get_process_semaphore(int sem_id, int count, int *semid) {
    return init_process_semaphore(sem_id, count, 0, semid);
}

str get_semaphore_value(int sem_id, int number, int *semval) {
    *semval = semctl(sem_id, number, GETVAL, 0);
    if (*semval < 0)
    {
        char *err = strerror(errno);
        errno = 0;
        return createException(MAL, "semaphore.init", "Error calling semctl(semid:%d,semnum:%d,cmd:%d,param:0): %s", sem_id, number, GETVAL, err);
    }
    return MAL_SUCCEED;
}

str change_semaphore_value(int sem_id, int number, int change) {
    str msg = MAL_SUCCEED;
    struct sembuf buffer;
    buffer.sem_num = number;
    buffer.sem_op = change;
    buffer.sem_flg = 0;

    if (semop(sem_id, &buffer, 1) < 0)
    {
        char *err = strerror(errno);
        errno = 0;
        return createException(MAL, "semaphore.init", "Error calling semop(semid:%d, sops: { sem_num:%d, sem_op:%d, sem_flag: %d }, nsops:1): %s", sem_id, number, change, 0, err);
    }
    return msg;
}

str change_semaphore_value_timeout(int sem_id, int number, int change, int timeout_mseconds, bool *succeed) {
#ifdef HAVE_SEMTIMEDOP
    // Some linux installations don't have semtimedop
    // The easiest solution is to just call semop instead
    // The only reason we use semtimedop is to prevent deadlocks when there are segfaults in a subprocess, which really shouldn't happen anyway
    // So having semtimedop is not vital to the functioning of the program
    str msg = MAL_SUCCEED;
    struct timespec timeout;
    struct sembuf buffer;
    buffer.sem_num = number;
    buffer.sem_op = change;
    buffer.sem_flg = 0;
    *succeed = false;

    timeout.tv_sec = (timeout_mseconds / 1000);
    timeout.tv_nsec = (timeout_mseconds % 1000) * 1000;

    if (semtimedop(sem_id, &buffer, 1, &timeout) < 0)
    {
        if (errno == EAGAIN || errno == EINTR) {
            errno = 0;
            return MAL_SUCCEED;
        } else {
            char *err = strerror(errno);
            errno = 0;
            return createException(MAL, "semaphore.init", "Error calling semtimedop(semid:%d, sops: { sem_num:%d, sem_op:%d, sem_flag: %d }, nsops:1): %s", sem_id, number, change, 0, err);
        }
    }
    *succeed = true;
    return msg;
#else
    (void) timeout_mseconds;
    *succeed = true;
    return change_semaphore_value(sem_id, number, change);
#endif
}

str release_process_semaphore(int sem_id) {
    if (semctl(sem_id, 0, IPC_RMID) < 0)
    {
        char *err = strerror(errno);
        errno = 0;
        return createException(MAL, "semaphore.init", "Error calling semctl(%d, 0, IPC_RMID): %s", sem_id, err);
    }
    return MAL_SUCCEED;
}

#endif
