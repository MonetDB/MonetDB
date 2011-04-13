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

#ifndef _MUTILS_H_
#define _MUTILS_H_

#ifdef NATIVE_WIN32
#if !defined(LIBMUTILS) && !defined(LIBGDK) && !defined(LIBMEROUTIL)
#define mutils_export extern __declspec(dllimport)
#else
#define mutils_export extern __declspec(dllexport)
#endif
#else
#define mutils_export extern
#endif

#ifdef NATIVE_WIN32

struct DIR {
	char *dir_name;
	int just_opened;
	HANDLE find_file_handle;
	char *find_file_data;
};

typedef struct DIR DIR;
struct dirent {
	char d_name[256];
	int d_namelen;
};

mutils_export DIR *opendir(const char *dirname);
mutils_export struct dirent *readdir(DIR *dir);
mutils_export void rewinddir(DIR *dir);
mutils_export int closedir(DIR *dir);

mutils_export char *dirname(char *path);

#endif

#ifndef S_IRUSR
/* if one doesn't exist, presumably they all don't exist */
#define S_IRUSR 0000400		/* read permission, owner */
#define S_IWUSR 0000200		/* write permission, owner */
#define S_IRGRP 0000040		/* read permission, group */
#define S_IWGRP 0000020		/* write permission, grougroup */
#define S_IROTH 0000004		/* read permission, other */
#define S_IWOTH 0000002		/* write permission, other */
#endif

#define MONETDB_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)

#define F_TLOCK 2		/* test and lock a region for exclusive use */
#define F_ULOCK 0		/* unlock a previously locked region */
#define F_LOCK 1		/* lock a region for exclusive use */

mutils_export int MT_lockf(char *filename, int mode, off_t off, off_t len);

/* Retrieves the absolute path to the executable being run, with no
 * extra /, /./, or /../ sequences.  On Darwin and Solaris this function
 * needs to be called before any chdirs are performed.  Returns a
 * pointer to a static buffer that is overwritten by subsequent calls to
 * this function. */
mutils_export char *get_bin_path(void);

#endif	/* _MUTILS_H_ */
