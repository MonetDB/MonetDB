/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include "monetdb_config.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#ifdef HAVE_STRINGS_H
# include <strings.h>
#endif
#include "mutils.h"

#if defined(HAVE_EXECINFO_H) && defined(HAVE_BACKTRACE)
#include <execinfo.h>
#endif

#ifdef HAVE_MACH_O_DYLD_H
# include <mach-o/dyld.h>  /* _NSGetExecutablePath on OSX >=10.5 */
#endif

#ifdef HAVE_LIMITS_H
# include <limits.h>  /* PATH_MAX on Solaris */
#endif

#ifdef HAVE_SYS_PARAM_H
# include <sys/param.h>  /* realpath on OSX, prerequisite of sys/sysctl on OpenBSD */
#endif

#ifdef HAVE_SYS_SYSCTL_H
# include <sys/sysctl.h>  /* KERN_PROC_PATHNAME on BSD */
#endif

#ifdef NATIVE_WIN32

/* Some definitions that we need to compile on Windows.
 * Note that Windows only runs on little endian architectures. */
typedef unsigned int u_int32_t;
typedef int int32_t;
#define BIG_ENDIAN	4321
#define LITTLE_ENDIAN	1234
#define BYTE_ORDER	LITTLE_ENDIAN

#ifndef HAVE_NEXTAFTERF
#include "s_nextafterf.c"
#endif

#include <stdio.h>

/* translate Windows error code (GetLastError()) to Unix-style error */
int
winerror(int e)
{
	switch (e) {
	case ERROR_BAD_ENVIRONMENT:
		return E2BIG;
	case ERROR_ACCESS_DENIED:
	case ERROR_CANNOT_MAKE:
	case ERROR_CURRENT_DIRECTORY:
	case ERROR_DRIVE_LOCKED:
	case ERROR_FAIL_I24:
	case ERROR_LOCK_FAILED:
	case ERROR_LOCK_VIOLATION:
	case ERROR_NETWORK_ACCESS_DENIED:
	case ERROR_NOT_LOCKED:
	case ERROR_SEEK_ON_DEVICE:
		return EACCES;
	case ERROR_MAX_THRDS_REACHED:
	case ERROR_NESTING_NOT_ALLOWED:
	case ERROR_NO_PROC_SLOTS:
		return EAGAIN;
	case ERROR_DIRECT_ACCESS_HANDLE:
	case ERROR_INVALID_TARGET_HANDLE:
		return EBADF;
	case ERROR_CHILD_NOT_COMPLETE:
	case ERROR_WAIT_NO_CHILDREN:
		return ECHILD;
	case ERROR_ALREADY_EXISTS:
	case ERROR_FILE_EXISTS:
		return EEXIST;
	case ERROR_INVALID_ACCESS:
	case ERROR_INVALID_DATA:
	case ERROR_INVALID_FUNCTION:
	case ERROR_INVALID_HANDLE:
	case ERROR_INVALID_PARAMETER:
	case ERROR_NEGATIVE_SEEK:
		return EINVAL;
	case ERROR_TOO_MANY_OPEN_FILES:
		return EMFILE;
	case ERROR_BAD_NET_NAME:
	case ERROR_BAD_NETPATH:
	case ERROR_BAD_PATHNAME:
	case ERROR_FILE_NOT_FOUND:
	case ERROR_FILENAME_EXCED_RANGE:
	case ERROR_INVALID_DRIVE:
	case ERROR_NO_MORE_FILES:
	case ERROR_PATH_NOT_FOUND:
		return ENOENT;
	case ERROR_BAD_FORMAT:
		return ENOEXEC;
	case ERROR_ARENA_TRASHED:
	case ERROR_INVALID_BLOCK:
	case ERROR_NOT_ENOUGH_MEMORY:
	case ERROR_NOT_ENOUGH_QUOTA:
		return ENOMEM;
	case ERROR_DISK_FULL:
		return ENOSPC;
	case ERROR_DIR_NOT_EMPTY:
		return ENOTEMPTY;
	case ERROR_BROKEN_PIPE:
		return EPIPE;
	case ERROR_NOT_SAME_DEVICE:
		return EXDEV;
	default:
		return EINVAL;
	}
}

DIR *
opendir(const char *dirname)
{
	DIR *result = NULL;
	char *mask;
	size_t k;
	DWORD e;

	if (dirname == NULL) {
		errno = EFAULT;
		return NULL;
	}

	result = (DIR *) malloc(sizeof(DIR));
	if (result == NULL) {
		errno = ENOMEM;
		return NULL;
	}
	result->find_file_data = malloc(sizeof(WIN32_FIND_DATA));
	result->dir_name = strdup(dirname);
	if (result->find_file_data == NULL || result->dir_name == NULL) {
		if (result->find_file_data)
			free(result->find_file_data);
		if (result->dir_name)
			free(result->dir_name);
		free(result);
		errno = ENOMEM;
		return NULL;
	}

	k = strlen(result->dir_name);
	if (k && result->dir_name[k - 1] == '\\') {
		result->dir_name[k - 1] = '\0';
		k--;
	}
	mask = malloc(strlen(result->dir_name) + 3);
	if (mask == NULL) {
		free(result->find_file_data);
		free(result->dir_name);
		free(result);
		errno = ENOMEM;
		return NULL;
	}
	sprintf(mask, "%s\\*", result->dir_name);

	result->find_file_handle = FindFirstFile(mask, (LPWIN32_FIND_DATA) result->find_file_data);
	if (result->find_file_handle == INVALID_HANDLE_VALUE) {
		e = GetLastError();
		free(mask);
		free(result->dir_name);
		free(result->find_file_data);
		free(result);
		SetLastError(e);
		errno = winerror(e);
		return NULL;
	}
	free(mask);
	result->just_opened = TRUE;

	return result;
}

static char *
basename(const char *file_name)
{
	const char *p;
	const char *base;

	if (file_name == NULL)
		return NULL;

	if (isalpha((int) (unsigned char) file_name[0]) && file_name[1] == ':')
		file_name += 2;	/* skip over drive letter */

	base = NULL;
	for (p = file_name; *p; p++)
		if (*p == '\\' || *p == '/')
			base = p;
	if (base)
		return (char *) base + 1;

	return (char *) file_name;
}

struct dirent *
readdir(DIR *dir)
{
	static struct dirent result;

	if (dir == NULL) {
		errno = EFAULT;
		return NULL;
	}

	if (dir->just_opened)
		dir->just_opened = FALSE;
	else if (!FindNextFile(dir->find_file_handle,
			       (LPWIN32_FIND_DATA) dir->find_file_data))
		return NULL;
	strncpy(result.d_name, basename(((LPWIN32_FIND_DATA) dir->find_file_data)->cFileName), sizeof(result.d_name));
	result.d_name[sizeof(result.d_name) - 1] = '\0';
	result.d_namelen = (int) strlen(result.d_name);

	return &result;
}

void
rewinddir(DIR *dir)
{
	char *mask;

	if (dir == NULL) {
		errno = EFAULT;
		return;
	}

	if (!FindClose(dir->find_file_handle))
		fprintf(stderr, "#rewinddir(): FindClose() failed\n");

	mask = malloc(strlen(dir->dir_name) + 3);
	if (mask == NULL) {
		errno = ENOMEM;
		dir->find_file_handle = INVALID_HANDLE_VALUE;
		return;
	}
	sprintf(mask, "%s\\*", dir->dir_name);
	dir->find_file_handle = FindFirstFile(mask, (LPWIN32_FIND_DATA) dir->find_file_data);
	free(mask);
	if (dir->find_file_handle == INVALID_HANDLE_VALUE)
		return;
	dir->just_opened = TRUE;
}

int
closedir(DIR *dir)
{
	if (dir == NULL) {
		errno = EFAULT;
		return -1;
	}

	if (!FindClose(dir->find_file_handle))
		return -1;

	free(dir->dir_name);
	free(dir->find_file_data);
	free(dir);

	return 0;
}

char *
dirname(char *path)
{
	char *p, *q;

	for (p = path, q = NULL; *p; p++)
		if (*p == '/' || *p == '\\')
			q = p;
	if (q == NULL)
		return ".";
	*q = '\0';
	return path;
}

/* see contract of unix MT_lockf */
int
MT_lockf(char *filename, int mode, off_t off, off_t len)
{
	int ret = 1, fd = -1;
	OVERLAPPED ov;
	HANDLE fh;
	static struct lockedfiles {
		struct lockedfiles *next;
		char *filename;
		int fildes;
	} *lockedfiles;
	struct lockedfiles **fpp, *fp;

	memset(&ov, 0, sizeof(ov));
#if defined(DUMMYSTRUCTNAME) && (defined(NONAMELESSUNION) || !defined(_MSC_EXTENSIONS))	/* Windows SDK v7.0 */
	ov.u.s.Offset = (unsigned int) off;
#if 0
	ov.u.s.OffsetHigh = off >> 32;
#else
	ov.u.s.OffsetHigh = 0;	/* sizeof(off) == 4, i.e. off >> 32 is not possible */
#endif
#else
	ov.Offset = (unsigned int) off;
#if 0
	ov.OffsetHigh = off >> 32;
#else
	ov.OffsetHigh = 0;	/* sizeof(off) == 4, i.e. off >> 32 is not possible */
#endif
#endif

	if (mode == F_ULOCK) {
		for (fpp = &lockedfiles; (fp = *fpp) != NULL; fpp = &fp->next) {
			if (strcmp(fp->filename, filename) == 0) {
				free(fp->filename);
				fd = fp->fildes;
				fh = (HANDLE) _get_osfhandle(fd);
				fp = *fpp;
				*fpp = fp->next;
				free(fp);
				ret = UnlockFileEx(fh, 0, len, 0, &ov);
				return ret ? 0 : -1;
			}
		}
		/* didn't find the locked file, try opening the file
		 * directly */
		fh = CreateFile(filename,
				GENERIC_READ | GENERIC_WRITE, 0,
				NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
		if (fh == INVALID_HANDLE_VALUE)
			return -2;
		ret = UnlockFileEx(fh, 0, len, 0, &ov);
		CloseHandle(fh);
		return 0;
	}

	fd = open(filename, O_CREAT | O_RDWR | O_TEXT, MONETDB_MODE);
	if (fd < 0)
		return -2;
	fh = (HANDLE) _get_osfhandle(fd);
	if (fh == INVALID_HANDLE_VALUE) {
		close(fd);
		return -2;
	}

	if (mode == F_TLOCK) {
		ret = LockFileEx(fh, LOCKFILE_FAIL_IMMEDIATELY | LOCKFILE_EXCLUSIVE_LOCK, 0, len, 0, &ov);
	} else if (mode == F_LOCK) {
		ret = LockFileEx(fh, LOCKFILE_EXCLUSIVE_LOCK, 0, len, 0, &ov);
	} else {
		close(fd);
		errno = EINVAL;
		return -2;
	}
	if (ret != 0) {
		if ((fp = malloc(sizeof(*fp))) != NULL &&
		    (fp->filename = strdup(filename)) != NULL) {
			fp->fildes = fd;
			fp->next = lockedfiles;
			lockedfiles = fp;
		}
		return fd;
	} else {
		close(fd);
		return -1;
	}
}

#else

#if defined(HAVE_LOCKF) && defined(__MACH__)
/* lockf() seems to be there, but I didn't find any header file that
   declares the prototype ... */
extern int lockf(int fd, int cmd, off_t len);
#endif

#ifndef HAVE_LOCKF
/* Cygwin implementation: struct flock is there, but lockf() is
   missing.
 */
static int
lockf(int fd, int cmd, off_t len)
{
	struct flock l;

	if (cmd == F_LOCK || cmd == F_TLOCK)
		l.l_type = F_WRLCK;
	else if (cmd == F_ULOCK)
		l.l_type = F_UNLCK;
	l.l_whence = SEEK_CUR;
	l.l_start = 0;
	l.l_len = len;
	return fcntl(fd, cmd == F_TLOCK ? F_SETLKW : F_SETLK, &l);
}
#endif

#ifndef O_TEXT
#define O_TEXT 0
#endif
/* returns -1 when locking failed,
 * returns -2 when the lock file could not be opened/created
 * returns the (open) file descriptor to the file when locking
 * returns 0 when unlocking */
int
MT_lockf(char *filename, int mode, off_t off, off_t len)
{
	int fd = open(filename, O_CREAT | O_RDWR | O_TEXT, MONETDB_MODE);

	if (fd < 0)
		return -2;

	if (lseek(fd, off, SEEK_SET) >= 0 &&
	    lockf(fd, mode, len) == 0) {
		if (mode == F_ULOCK) {
			close(fd);
			return 0;
		}
		/* do not close else we lose the lock we want */
		(void) lseek(fd, 0, SEEK_SET); /* move seek pointer back */
		return fd;
	}
	close(fd);
	return -1;
}

#endif

#ifndef PATH_MAX
# define PATH_MAX 1024
#endif
static char _bin_path[PATH_MAX];
char *
get_bin_path(void)
{
	/* getting the path to the executable's binary, isn't all that
	 * simple, unfortunately */
#ifdef WIN32
	if (GetModuleFileName(NULL, _bin_path,
			      (DWORD) sizeof(_bin_path)) != 0)
		return _bin_path;
#elif defined(HAVE__NSGETEXECUTABLEPATH)  /* Darwin/OSX */
	char buf[PATH_MAX];
	uint32_t size = PATH_MAX;
	if (_NSGetExecutablePath(buf, &size) == 0 &&
			realpath(buf, _bin_path) != NULL)
	return _bin_path;
#elif defined(HAVE_SYS_SYSCTL_H) && defined(KERN_PROC_PATHNAME)  /* BSD */
	int mib[4];
	size_t cb = sizeof(_bin_path);
	mib[0] = CTL_KERN;
	mib[1] = KERN_PROC;
	mib[2] = KERN_PROC_PATHNAME;
	mib[3] = -1;
	if (sysctl(mib, 4, _bin_path, &cb, NULL, 0) == 0)
		return _bin_path;
#elif defined(HAVE_GETEXECNAME)  /* Solaris */
	char buf[PATH_MAX];
	const char *execn = getexecname();
	/* getexecname doesn't always return an absolute path, the only
	 * thing it seems to do is strip leading ./ from the invocation
	 * string. */
	if (*execn != '/') {
		if (getcwd(buf, PATH_MAX) != NULL) {
			snprintf(buf + strlen(buf), PATH_MAX - strlen(buf), "/%s", execn);
			if (realpath(buf, _bin_path) != NULL)
				return(_bin_path);
		}
	} else {
		if (realpath(execn, _bin_path) != NULL)
			return(_bin_path);
	}
#else  /* try Linux approach */
	if (readlink("/proc/self/exe",
				_bin_path, sizeof(_bin_path)) != -1)
			return _bin_path;
#endif
	/* could use argv[0] (passed) to deduce location based on PATH, but
	 * that's a lot of work and unreliable */
	return NULL;
}
