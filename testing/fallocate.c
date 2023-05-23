
//#define _GNU_SOURCE             /* See feature_test_macros(7) */
#include <monetdb_config.h>
#if HAVE_FALLOCATE
#include <fcntl.h>
#include <dlfcn.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

typedef int (*fallocate_fptr)(int, int, off_t, off_t);
fallocate_fptr real_fallocate = NULL;

int
fallocate(int fd, int mode, off_t offset, off_t len)
{
	if (!real_fallocate)
		real_fallocate = (fallocate_fptr)dlsym(RTLD_NEXT, "fallocate");
	if (!real_fallocate) {
		fprintf(stderr, "mtest_allocate: could not resolve 'fallocate' in 'libc.so': %s\n", dlerror());
		exit(1);
	}
	if (len >= (512*1024*1024)) {
		errno = ENOSPC;
		return -1;
	}
	return real_fallocate(fd, mode, offset, len);
}
#endif
