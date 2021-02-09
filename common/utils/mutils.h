/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _MUTILS_H_
#define _MUTILS_H_

#ifdef WIN32
#if !defined(LIBMUTILS) && !defined(LIBGDK) && !defined(LIBMEROUTIL)
#define mutils_export extern __declspec(dllimport)
#else
#define mutils_export extern __declspec(dllexport)
#endif
#else
#define mutils_export extern
#endif

#ifndef S_IWGRP
/* if one doesn't exist, presumably they all don't exist - Not so on MinGW */
#define S_IRUSR 0000400		/* read permission, owner */
#define S_IWUSR 0000200		/* write permission, owner */
#define S_IXUSR 0000100		/* execute permission, owner */
#define S_IRGRP 0000040		/* read permission, group */
#define S_IWGRP 0000020		/* write permission, group */
#define S_IXGRP 0000010		/* execute permission, group */
#define S_IROTH 0000004		/* read permission, other */
#define S_IWOTH 0000002		/* write permission, other */
#define S_IXOTH 0000001		/* execute permission, other */
#endif

#define MONETDB_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)
#define MONETDB_DIRMODE		(MONETDB_MODE | S_IXUSR | S_IXGRP | S_IXOTH)

#define F_TEST	3		/* test a region for other processes locks.  */
#define F_TLOCK	2		/* test and lock a region for exclusive use */
#define F_ULOCK	0		/* unlock a previously locked region */
#define F_LOCK	1		/* lock a region for exclusive use */

#ifdef NATIVE_WIN32

#include <stdio.h>

struct DIR;

typedef struct DIR DIR;
struct dirent {
	char d_name[FILENAME_MAX];
	int d_namelen;
};

mutils_export int winerror(int);
mutils_export DIR *opendir(const char *dirname);
mutils_export struct dirent *readdir(DIR *dir);
mutils_export void rewinddir(DIR *dir);
mutils_export int closedir(DIR *dir);

mutils_export char *dirname(char *path);

mutils_export wchar_t *utf8towchar(const char *src);
mutils_export char *wchartoutf8(const wchar_t *src);

mutils_export FILE *MT_fopen(const char *filename, const char *mode);
mutils_export int MT_open(const char *filename, int flags);
mutils_export int MT_stat(const char *filename, struct stat *stb);
mutils_export int MT_rmdir(const char *dirname);
mutils_export int MT_rename(const char *old, const char *new);
mutils_export int MT_remove(const char *filename);
mutils_export int MT_mkdir(const char *dirname);
mutils_export char *MT_getcwd(char *buffer, size_t size);
mutils_export int MT_access(const char *pathname, int mode);

#else

#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#define MONETDB_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)
#define MONETDB_DIRMODE		(MONETDB_MODE | S_IXUSR | S_IXGRP | S_IXOTH)

static inline FILE *
MT_fopen(const char *filename, const char *mode)
{
	return fopen(filename, mode);
}

static inline int
MT_open(const char *filename, int flags)
{
	return open(filename, flags, MONETDB_MODE);
}

static inline int
MT_stat(const char *filename, struct stat *stb)
{
	return stat(filename, stb);
}

static inline int
MT_mkdir(const char *dirname)
{
	return mkdir(dirname, MONETDB_DIRMODE);
}

static inline int
MT_rmdir(const char *dirname)
{
	return rmdir(dirname);
}

static inline int
MT_rename(const char *old, const char *new)
{
	return rename(old, new);
}

static inline int
MT_remove(const char *filename)
{
	return remove(filename);
}

static inline char *
MT_getcwd(char *buffer, size_t size)
{
	return getcwd(buffer, size);
}

static inline int
MT_access(const char *pathname, int mode)
{
	return access(pathname, mode);
}

#endif

mutils_export int MT_lockf(const char *filename, int mode);

mutils_export void print_trace(void);

/* Retrieves the absolute path to the executable being run, with no
 * extra /, /./, or /../ sequences.  On Darwin and Solaris this function
 * needs to be called before any chdirs are performed.  Returns a
 * pointer to a static buffer that is overwritten by subsequent calls to
 * this function. */
mutils_export char *get_bin_path(void);

/* Returns the Mercurial changeset of the current checkout, if available */
mutils_export const char *mercurial_revision(void)
	__attribute__((__const__));

#endif	/* _MUTILS_H_ */
