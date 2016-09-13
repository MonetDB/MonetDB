/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * (author) M. Kersten
 * An include file name is also used as library name
 */
#include "monetdb_config.h"
#include "mal_module.h"
#include "mal_linker.h"
#include "mal_function.h"	/* for throw() */
#include "mal_import.h"		/* for slash_2_dir_sep() */
#include "mal_private.h"

#include "mutils.h"
#include <sys/types.h> /* opendir */
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#include <unistd.h>

#if defined(_MSC_VER) && _MSC_VER >= 1400
#define open _open
#define close _close
#endif

#define MAXMODULES 128

typedef struct{
	str modname;
	str fullname;
	void **handle;
} FileRecord;

static FileRecord filesLoaded[MAXMODULES];
static int maxfiles = MAXMODULES;
static int lastfile = 0;

/*
 * returns 1 if the file exists
 */
#ifndef F_OK
#define F_OK 0
#endif
#ifdef _MSC_VER
#define access(f, m)	_access(f, m)
#endif
static inline int
fileexists(const char *path)
{
	return access(path, F_OK) == 0;
}

/* Search for occurrence of the function in the library identified by the filename.  */
MALfcn
getAddress(stream *out, str modname, str fcnname, int silent)
{
	void *dl;
	MALfcn adr;
	static int idx=0;

	static int prev= -1;

	/* First try the last module loaded */
	if( prev >= 0){
		adr = (MALfcn) dlsym(filesLoaded[prev].handle, fcnname);
		if( adr != NULL)
			return adr; /* found it */
	}
	/*
	 * Search for occurrence of the function in any library already loaded.
	 * This deals with the case that files are linked together to reduce
	 * the loading time, while the signatures of the functions are still
	 * obtained from the source-file MAL script.
	 */
	for (idx =0; idx < lastfile; idx++)
		if (filesLoaded[idx].handle) {
			adr = (MALfcn) dlsym(filesLoaded[idx].handle, fcnname);
			if (adr != NULL)  {
				prev = idx;
				return adr; /* found it */
			}
		}

	if (lastfile)
		return NULL;
	/*
	 * Try the program libraries at large or run through all
	 * loaded files and try to resolve the functionname again.
	 *
	 * the first argument must be the same as the base name of the
	 * library that is created in src/tools */
	dl = mdlopen("libmonetdb5", RTLD_NOW | RTLD_GLOBAL);
	if (dl == NULL) {
		/* shouldn't happen, really */
		if (!silent)
			showException(out, MAL, "MAL.getAddress",
						  "address of '%s.%s' not found",
						  (modname?modname:"<unknown>"), fcnname);
		return NULL;
	}

	adr = (MALfcn) dlsym(dl, fcnname);
	filesLoaded[lastfile].modname = GDKstrdup("libmonetdb5");
	filesLoaded[lastfile].fullname = GDKstrdup("libmonetdb5");
	filesLoaded[lastfile].handle = dl;
	lastfile ++;
	if(adr != NULL)
		return adr; /* found it */

	if (!silent)
		showException(out, MAL,"MAL.getAddress", "address of '%s.%s' not found",
			(modname?modname:"<unknown>"), fcnname);
	return NULL;
}
/*
 * Module file loading
 * The default location to search for the module is in monet_mod_path
 * unless an absolute path is given.
 * Loading further relies on the Linux policy to search for the module
 * location in the following order: 1) the colon-separated list of
 * directories in the user's LD_LIBRARY_PATH, 2) the libraries specified
 * in /etc/ld.so.cache and 3) /usr/lib followed by /lib.
 * If the module contains a routine _init, then that code is executed
 * before the loader returns. Likewise the routine _fini is called just
 * before the module is unloaded.
 *
 * A module loading conflict emerges if a function is redefined.
 * A duplicate load is simply ignored by keeping track of modules
 * already loaded.
 */

str
loadLibrary(str filename, int flag)
{
	int mode = RTLD_NOW | RTLD_GLOBAL;
	char nme[PATHLENGTH];
	void *handle = NULL;
	str s;
	int idx;
	char *mod_path = GDKgetenv("monet_mod_path");

	/* AIX requires RTLD_MEMBER to load a module that is a member of an
	 * archive.  */
#ifdef RTLD_MEMBER
	mode |= RTLD_MEMBER;
#endif

	for (idx = 0; idx < lastfile; idx++)
		if (filesLoaded[idx].modname &&
		    strcmp(filesLoaded[idx].modname, filename) == 0)
			/* already loaded */
			return MAL_SUCCEED;

	/* ignore any path given */
	if ((s = strrchr(filename, DIR_SEP)) == NULL)
		s = filename;

	if (mod_path != NULL) {
		while (*mod_path == PATH_SEP)
			mod_path++;
		if (*mod_path == 0)
			mod_path = NULL;
	}
	if (mod_path == NULL) {
		if (flag)
			throw(LOADER, "loadLibrary", RUNTIME_FILE_NOT_FOUND ":%s", s);
		return MAL_SUCCEED;
	}

	while (*mod_path) {
		char *p;

		for (p = mod_path; *p && *p != PATH_SEP; p++)
			;

		/* try hardcoded SO_EXT if that is the same for modules */
#ifdef _AIX
		snprintf(nme, PATHLENGTH, "%.*s%c%s_%s%s(%s_%s.0)",
				 (int) (p - mod_path),
				 mod_path, DIR_SEP, SO_PREFIX, s, SO_EXT, SO_PREFIX, s);
#else
		snprintf(nme, PATHLENGTH, "%.*s%c%s_%s%s",
				 (int) (p - mod_path),
				 mod_path, DIR_SEP, SO_PREFIX, s, SO_EXT);
#endif
		handle = dlopen(nme, mode);
		if (handle == NULL && fileexists(nme)) {
			throw(LOADER, "loadLibrary", RUNTIME_LOAD_ERROR " failed to open library %s (from within file '%s'): %s", s, nme, dlerror());
		}
		if (handle == NULL && strcmp(SO_EXT, ".so") != 0) {
			/* try .so */
			snprintf(nme, PATHLENGTH, "%.*s%c%s_%s.so",
					 (int) (p - mod_path),
					 mod_path, DIR_SEP, SO_PREFIX, s);
			handle = dlopen(nme, mode);
			if (handle == NULL && fileexists(nme)) {
				throw(LOADER, "loadLibrary", RUNTIME_LOAD_ERROR " failed to open library %s (from within file '%s'): %s", s, nme, dlerror());
			}
		}
#ifdef __APPLE__
		if (handle == NULL && strcmp(SO_EXT, ".bundle") != 0) {
			/* try .bundle */
			snprintf(nme, PATHLENGTH, "%.*s%c%s_%s.bundle",
					 (int) (p - mod_path),
					 mod_path, DIR_SEP, SO_PREFIX, s);
			handle = dlopen(nme, mode);
			if (handle == NULL && fileexists(nme)) {
				throw(LOADER, "loadLibrary", RUNTIME_LOAD_ERROR " failed to open library %s (from within file '%s'): %s", s, nme, dlerror());
			}
		}
#endif

		if (*p == 0 || handle != NULL)
			break;
		mod_path = p + 1;
	}

	if (handle == NULL) {
		if (flag)
			throw(LOADER, "loadLibrary", RUNTIME_LOAD_ERROR " could not locate library %s (from within file '%s'): %s", s, filename, dlerror());
	}

	MT_lock_set(&mal_contextLock);
	if (lastfile == maxfiles) {
		if (handle)
			dlclose(handle);
		showException(GDKout, MAL,"loadModule", "internal error, too many modules loaded");
	} else {
		filesLoaded[lastfile].modname = GDKstrdup(filename);
		filesLoaded[lastfile].fullname = GDKstrdup(handle ? nme : "");
		filesLoaded[lastfile].handle = handle ? handle : filesLoaded[0].handle;
		lastfile ++;
	}
	MT_lock_unset(&mal_contextLock);

	return MAL_SUCCEED;
}

/*
 * For analysis of memory leaks we should cleanup the libraries before
 * we exit the server. This does not involve the libraries themselves,
 * because they may still be in use.
 */
void
mal_linker_reset(void)
{
	int i;

	MT_lock_set(&mal_contextLock);
	for (i = 0; i < lastfile; i++){
		if (filesLoaded[i].fullname) {
			/* dlclose(filesLoaded[i].handle);*/
			GDKfree(filesLoaded[i].modname);
			GDKfree(filesLoaded[i].fullname);
		}
		filesLoaded[i].modname = NULL;
		filesLoaded[i].fullname = NULL;
	}
	lastfile = 0;
	MT_lock_unset(&mal_contextLock);
}

/*
 * Handling of Module Library Search Path
 * The plausible locations of the modules can be designated by
 * an environment variable.
 */
static int
cmpstr(const void *_p1, const void *_p2)
{
	const char *p1 = *(char* const*)_p1;
	const char *p2 = *(char* const*)_p2;
	const char *f1 = strrchr(p1, (int) DIR_SEP);
	const char *f2 = strrchr(p2, (int) DIR_SEP);
	return strcmp(f1?f1:p1, f2?f2:p2);
}


#define MAXMULTISCRIPT 48
char *
locate_file(const char *basename, const char *ext, bit recurse)
{
	char *mod_path = GDKgetenv("monet_mod_path");
	char *fullname;
	size_t fullnamelen;
	size_t filelen = strlen(basename) + strlen(ext);
	str strs[MAXMULTISCRIPT]; /* hardwired limit */
	int lasts = 0;

	if (mod_path == NULL)
		return NULL;

	while (*mod_path == PATH_SEP)
		mod_path++;
	if (*mod_path == 0)
		return NULL;
	fullnamelen = 512;
	fullname = GDKmalloc(fullnamelen);
	if (fullname == NULL)
		return NULL;
	while (*mod_path) {
		size_t i;
		char *p;
		int fd;
		DIR *rdir;

		if ((p = strchr(mod_path, PATH_SEP)) != NULL) {
			i = p - mod_path;
		} else {
			i = strlen(mod_path);
		}
		while (i + filelen + 2 > fullnamelen) {
			fullnamelen += 512;
			fullname = GDKrealloc(fullname, fullnamelen);
			if (fullname == NULL)
				return NULL;
		}
		/* we are now sure the directory name, file
		   base name, extension, and separator fit
		   into fullname, so we don't need to do any
		   extra checks */
		strncpy(fullname, mod_path, i);
		fullname[i] = DIR_SEP;
		strcpy(fullname + i + 1, basename);
		/* see if this is a directory, if so, recurse */
		if (recurse == 1 && (rdir = opendir(fullname)) != NULL) {
			struct dirent *e;
			/* list *ext, sort, return */
			while ((e = readdir(rdir)) != NULL) {
				if (strcmp(e->d_name, "..") == 0 || strcmp(e->d_name, ".") == 0)
					continue;
				if (strcmp(e->d_name + strlen(e->d_name) - strlen(ext), ext) == 0) {
					strs[lasts] = GDKmalloc(strlen(fullname) + sizeof(DIR_SEP)
							+ strlen(e->d_name) + sizeof(PATH_SEP) + 1);
					if (strs[lasts] == NULL) {
						while (lasts >= 0)
							GDKfree(strs[lasts--]);
						GDKfree(fullname);
						(void)closedir(rdir);
						return NULL;
					}
					sprintf(strs[lasts], "%s%c%s%c", fullname, DIR_SEP, e->d_name, PATH_SEP);
					lasts++;
				}
				if (lasts >= MAXMULTISCRIPT)
					break;
			}
			(void)closedir(rdir);
		} else {
			strcat(fullname + i + 1, ext);
			if ((fd = open(fullname, O_RDONLY)) >= 0) {
				close(fd);
				return GDKrealloc(fullname, strlen(fullname) + 1);
			}
		}
		if ((mod_path = p) == NULL)
			break;
		while (*mod_path == PATH_SEP)
			mod_path++;
	}
	if (lasts > 0) {
		size_t i = 0;
		int c;
		/* assure that an ordering such as 10_first, 20_second works */
		qsort(strs, lasts, sizeof(char *), cmpstr);
		for (c = 0; c < lasts; c++)
			i += strlen(strs[c]) + 1; /* PATH_SEP or \0 */
		fullname = GDKrealloc(fullname, i);
		if( fullname == NULL){
			GDKerror("locate_file" MAL_MALLOC_FAIL);
			return NULL;
		}
		i = 0;
		for (c = 0; c < lasts; c++) {
			if (strstr(fullname, strs[c]) == NULL) {
				strcpy(fullname + i, strs[c]);
				i += strlen(strs[c]);
			}
			GDKfree(strs[c]);
		}
		fullname[i - 1] = '\0';
		return fullname;
	}
	/* not found */
	GDKfree(fullname);
	return NULL;
}

char *
MSP_locate_script(const char *filename)
{
	return locate_file(filename, MAL_EXT, 1);
}

char *
MSP_locate_sqlscript(const char *filename, bit recurse)
{
	/* no directory semantics (yet) */
	return locate_file(filename, SQL_EXT, recurse);
}
