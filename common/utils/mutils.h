/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
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

#ifdef NATIVE_WIN32

#include <stdio.h>

struct DIR {
	char *dir_name;
	int just_opened;
	HANDLE find_file_handle;
	void *find_file_data;
};

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

mutils_export int MT_lockf(char *filename, int mode, off_t off, off_t len);

mutils_export void print_trace(void);

/* Retrieves the absolute path to the executable being run, with no
 * extra /, /./, or /../ sequences.  On Darwin and Solaris this function
 * needs to be called before any chdirs are performed.  Returns a
 * pointer to a static buffer that is overwritten by subsequent calls to
 * this function. */
mutils_export char *get_bin_path(void);

/* Returns the Mercurial changeset of the current checkout, if available */
mutils_export const char *mercurial_revision(void);

#endif	/* _MUTILS_H_ */
