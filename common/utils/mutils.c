/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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

#ifdef HAVE_MACH_O_DYLD_H
# include <mach-o/dyld.h>  /* _NSGetExecutablePath on OSX >=10.5 */
#endif

#ifdef HAVE_SYS_PARAM_H
# include <sys/param.h>  /* realpath on OSX */
#endif

#ifdef HAVE_LIMITS_H
# include <limits.h>  /* PATH_MAX on Solaris */
#endif

#ifdef NATIVE_WIN32

#include <stdio.h>

DIR *
opendir(const char *dirname)
{
	DIR *result = NULL;
	char *mask;
	size_t k;

	if (dirname == NULL)
		return NULL;

	result = (DIR *) malloc(sizeof(DIR));
	result->find_file_data = malloc(sizeof(WIN32_FIND_DATA));
	result->dir_name = strdup(dirname);

	k = strlen(result->dir_name);
	if (k && result->dir_name[k - 1] == '\\') {
		result->dir_name[k - 1] = '\0';
		k--;
	}
	mask = malloc(strlen(result->dir_name) + 3);
	sprintf(mask, "%s\\*", result->dir_name);

	result->find_file_handle = FindFirstFile(mask, (LPWIN32_FIND_DATA) result->find_file_data);
	free(mask);

	if (result->find_file_handle == INVALID_HANDLE_VALUE) {
		free(result->dir_name);
		free(result->find_file_data);
		free(result);
		SetLastError(ERROR_OPEN_FAILED);	/* enforce EIO */
		return NULL;
	}
	result->just_opened = TRUE;

	return result;
}

static char *
basename(const char *file_name)
{
	register char *base;

	if (file_name == NULL)
		return NULL;

	base = strrchr(file_name, '\\');
	if (base)
		return base + 1;

	if (isalpha((int) (unsigned char) file_name[0]) && file_name[1] == ':')
		return (char *) file_name + 2;

	return (char *) file_name;
}

struct dirent *
readdir(DIR *dir)
{
	static struct dirent result;

	if (dir == NULL)
		return NULL;

	if (dir->just_opened)
		dir->just_opened = FALSE;
	else {
		if (!FindNextFile(dir->find_file_handle, (LPWIN32_FIND_DATA) dir->find_file_data)) {
			int error = GetLastError();

			if (error) {
				if (error != ERROR_NO_MORE_FILES)
					SetLastError(ERROR_OPEN_FAILED);	/* enforce EIO */
				return NULL;
			}
		}
	}
	strncpy(result.d_name, basename(((LPWIN32_FIND_DATA) dir->find_file_data)->cFileName), sizeof(result.d_name));
	result.d_name[sizeof(result.d_name) - 1] = '\0';
	result.d_namelen = (int) strlen(result.d_name);

	return &result;
}

void
rewinddir(DIR *dir)
{
	char *mask;

	if (dir == NULL)
		return;

	if (!FindClose(dir->find_file_handle))
		fprintf(stderr, "#rewinddir(): FindClose() failed\n");

	mask = malloc(strlen(dir->dir_name) + 3);
	sprintf(mask, "%s\\*", dir->dir_name);
	dir->find_file_handle = FindFirstFile(mask, (LPWIN32_FIND_DATA) dir->find_file_data);
	free(mask);

	if (dir->find_file_handle == INVALID_HANDLE_VALUE) {
		SetLastError(ERROR_OPEN_FAILED);	/* enforce EIO */
		return;
	}
	dir->just_opened = TRUE;
}

int
closedir(DIR *dir)
{
	if (dir == NULL)
		return -1;

	if (!FindClose(dir->find_file_handle)) {
		SetLastError(ERROR_OPEN_FAILED);	/* enforce EIO */
		return -1;
	}

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
	int ret = 1, illegalmode = 0, fd = -1;
	OVERLAPPED ov;
	OSVERSIONINFO os;
	HANDLE fh = CreateFile(filename,
			       GENERIC_READ | GENERIC_WRITE, 0,
			       NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);

	os.dwOSVersionInfoSize = sizeof(OSVERSIONINFO);
	GetVersionEx(&os);
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

	if (fh == NULL) {
		return -2;
	}
	if (mode == F_ULOCK) {
		if (os.dwPlatformId != VER_PLATFORM_WIN32_WINDOWS)
			ret = UnlockFileEx(fh, 0, 0, len, &ov);
	} else if (mode == F_TLOCK) {
		if (os.dwPlatformId != VER_PLATFORM_WIN32_WINDOWS)
			ret = LockFileEx(fh, LOCKFILE_FAIL_IMMEDIATELY | LOCKFILE_EXCLUSIVE_LOCK, 0, 0, len, &ov);
	} else if (mode == F_LOCK) {
		if (os.dwPlatformId != VER_PLATFORM_WIN32_WINDOWS)
			ret = LockFileEx(fh, LOCKFILE_EXCLUSIVE_LOCK, 0, 0, len, &ov);
	} else {
		illegalmode = 1;
	}
	CloseHandle(fh);
	if (illegalmode) {
		SetLastError(ERROR_INVALID_DATA);
	}
	if (ret != 0) {
		fd = open(filename, O_CREAT | O_RDWR, MONETDB_MODE);
		if (fd < 0) {
			/* this is nasty, but I "trust" windows that it in this case
			 * also cannot open the file into a filehandle any more, so
			 * unlocking is in vain. */
			return -2;
		} else {
			return fd;
		}
	} else {
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

/* returns -1 when locking failed,
 * returns -2 when the lock file could not be opened/created
 * returns the (open) file descriptor to the file otherwise */
int
MT_lockf(char *filename, int mode, off_t off, off_t len)
{
	int fd = open(filename, O_CREAT | O_RDWR, MONETDB_MODE);

	if (fd < 0)
		return -2;

	if (lseek(fd, off, SEEK_SET) == off && lockf(fd, mode, len) == 0) {
		/* do not close else we lose the lock we want */
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
#if defined(_MSC_VER)		/* Windows */
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
			snprintf(buf + strlen(buf), PATH_MAX, "/%s", execn);
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
