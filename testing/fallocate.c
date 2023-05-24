
//#define _GNU_SOURCE             /* See feature_test_macros(7) */
#include <monetdb_config.h>
#ifdef HAVE_FALLOCATE
#include <fcntl.h>
#include <dlfcn.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

typedef int (*fallocate_fptr)(int, int, off_t, off_t);
fallocate_fptr real_fallocate = NULL;

static int brk_nr = 1;
static off_t brk_len = 1024*1024;

int
fallocate(int fd, int mode, off_t offset, off_t len)
{
	if (!real_fallocate) {
		real_fallocate = (fallocate_fptr)dlsym(RTLD_NEXT, "fallocate");
		char *v = getenv("FALLOCATE_LIMIT");
		if (v) { /* format: nr,size (in M or G) */
			char *c = strchr(v,',');
			if (c) {
				c[0] = 0;
				c++;
				brk_nr = strtol(v, NULL, 10);
				char *m = strchr(c,'M');
				if (m) {
					m[0] = 0;
					brk_len = strtol(c, NULL, 10) * 1024*1024;
				} else {
					char *g = strchr(c,'G');
					if (g) {
						g[0] = 0;
						brk_len = strtol(c, NULL, 10) * 1024*1024*1024;
					}
				}
			}
		}
	}
	if (!real_fallocate) {
		fprintf(stderr, "mtest_allocate: could not resolve 'fallocate' in 'libc.so': %s\n", dlerror());
		exit(1);
	}
	if (len >= brk_len) {
		if (brk_nr) /* once the number is reached, continue with errors */
			brk_nr--;
		if (brk_nr == 0) {
			errno = ENOSPC;
			return -1;
		}
	}
	return real_fallocate(fd, mode, offset, len);
}
#endif
