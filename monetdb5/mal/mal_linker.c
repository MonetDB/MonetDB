/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

/*
 * (author) M. Kersten
 * An include file name is also used as library name
 */
#include "monetdb_config.h"
#include "mal_module.h"
#include "mal_linker.h"
#include "mal_function.h"		/* for throw() */
#include "mal_import.h"			/* for slash_2_dir_sep() */
#include "mal_private.h"
#include "mal_internal.h"

#include "mutils.h"
#include <sys/types.h>			/* opendir */
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

typedef struct {
	str modname;
	str fullname;
	void *handle;
} FileRecord;

static FileRecord filesLoaded[MAXMODULES];
static int maxfiles = MAXMODULES;
static int lastfile = 0;

#ifndef O_CLOEXEC
#ifdef _O_NOINHERIT
#define O_CLOEXEC _O_NOINHERIT	/* Windows */
#else
#define O_CLOEXEC 0
#endif
#endif

/*
 * returns 1 if the file exists
 */
#ifndef F_OK
#define F_OK 0
#endif
static inline int
fileexists(const char *path)
{
	return MT_access(path, F_OK) == 0;
}

/* Search for occurrence of the function in the library identified by the filename.  */
MALfcn
getAddress(const char *modname, const char *fcnname)
{
	MALfcn adr;
	int idx = 0;
	static int prev = -1;

	if ((adr = findFunctionImplementation(fcnname)) != NULL)
		return adr;

	/* First try the last module loaded */
	if (prev >= 0 && strcmp(filesLoaded[prev].modname, modname) == 0) {	/* test if just pointer compare could work */
		adr = (MALfcn) dlsym(filesLoaded[prev].handle, fcnname);
		if (adr != NULL)
			return adr;			/* found it */
	}
	/*
	 * Search for occurrence of the function in any library already loaded.
	 * This deals with the case that files are linked together to reduce
	 * the loading time, while the signatures of the functions are still
	 * obtained from the source-file MAL script.
	 */
	for (idx = 0; idx < lastfile; idx++)
		if (idx != prev &&		/* skip already searched module */
			filesLoaded[idx].handle &&
			strcmp(filesLoaded[idx].modname, modname) == 0 &&
			(idx == 0 || filesLoaded[idx].handle != filesLoaded[0].handle)) {
			adr = (MALfcn) dlsym(filesLoaded[idx].handle, fcnname);
			if (adr != NULL) {
				prev = idx;
				return adr;		/* found it */
			}
		}

	if (lastfile == 0) {
		char *msg = loadLibrary("monetdb5", 1);
		if (msg) {
			return NULL;
		}
	}

	/* first should be monetdb5 */
	assert(strcmp(filesLoaded[0].modname, "monetdb5") == 0
		   || strcmp(filesLoaded[0].modname, "embedded") == 0);
	adr = (MALfcn) dlsym(filesLoaded[0].handle, fcnname);
	if (adr != NULL) {
		prev = 0;
		return adr;			/* found it */
	}
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
loadLibrary(const char *filename, int flag)
{
	int mode = RTLD_NOW | RTLD_GLOBAL;
	char nme[FILENAME_MAX];
	void *handle = NULL;
	const char *s;
	int idx;
	const char *mod_path = GDKgetenv("monet_mod_path");
	bool is_mod;
	bool is_monetdb5 = strcmp(filename, "monetdb5") == 0;

	is_mod = (!is_monetdb5 && strcmp(filename, "embedded") != 0);

	if (lastfile == 0 && is_mod) {	/* first load reference to local functions */
		str msg = loadLibrary("monetdb5", flag>=0?flag:0);
		if (msg != MAL_SUCCEED)
			return msg;
	}
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
		int len;

		if (is_mod && flag < 0)
			len = snprintf(nme, sizeof(nme), ".%c%s_%s%s", DIR_SEP, SO_PREFIX, s, SO_EXT);
		else if (is_mod)
			len = snprintf(nme, sizeof(nme), "%s_%s%s", SO_PREFIX, s, SO_EXT);
		else
			len = snprintf(nme, sizeof(nme), "%s%s%s", SO_PREFIX, s, SO_EXT);
		if (len == -1 || len >= FILENAME_MAX)
			throw(LOADER, "loadLibrary",
				  RUNTIME_LOAD_ERROR "Library filename path is too large");

#ifdef __APPLE__
		handle = mdlopen(is_monetdb5 ? NULL : nme, RTLD_NOW | RTLD_GLOBAL);
#else
		handle = dlopen(is_monetdb5 ? NULL : nme, RTLD_NOW | RTLD_GLOBAL);
#endif
		if (!handle) {
			if (flag>0)
				throw(LOADER, "loadLibrary", RUNTIME_FILE_NOT_FOUND ":%s", s);
			TRC_INFO(MAL_LOADER, "Module %s not loaded\n", filename);
			return MAL_SUCCEED;
		}
		TRC_INFO_IF(MAL_LOADER) {
			if (is_monetdb5)
				TRC_INFO_ENDIF(MAL_LOADER, "Module %s loaded\n", filename);
			else
				TRC_INFO_ENDIF(MAL_LOADER, "Module %s loaded from %s\n",
							   filename, nme);
		}
	}

	while (!handle && *mod_path) {
		int len;
		const char *p;

		for (p = mod_path; *p && *p != PATH_SEP; p++)
			;

		if (is_mod)
			len = snprintf(nme, sizeof(nme), "%.*s%c%s_%s%s",
						   (int) (p - mod_path), mod_path, DIR_SEP, SO_PREFIX,
						   s, SO_EXT);
		else
			len = snprintf(nme, sizeof(nme), "%.*s%c%s%s%s",
						   (int) (p - mod_path), mod_path, DIR_SEP, SO_PREFIX,
						   s, SO_EXT);
		if (len == -1 || len >= FILENAME_MAX)
			throw(LOADER, "loadLibrary",
				  RUNTIME_LOAD_ERROR "Library filename path is too large");
		handle = dlopen(nme, mode);
		if (handle == NULL && fileexists(nme))
			throw(LOADER, "loadLibrary",
				  RUNTIME_LOAD_ERROR
				  " failed to open library %s (from within file '%s'): %s", s,
				  nme, dlerror());
		if (handle == NULL && strcmp(SO_EXT, ".so") != /* DISABLES CODE */ (0)) {
			/* try .so */
			if (is_mod)
				len = snprintf(nme, sizeof(nme), "%.*s%c%s_%s.so",
							   (int) (p - mod_path), mod_path, DIR_SEP,
							   SO_PREFIX, s);
			else
				len = snprintf(nme, sizeof(nme), "%.*s%c%s%s.so",
							   (int) (p - mod_path), mod_path, DIR_SEP,
							   SO_PREFIX, s);
			if (len == -1 || len >= FILENAME_MAX)
				throw(LOADER, "loadLibrary",
					  RUNTIME_LOAD_ERROR "Library filename path is too large");
			handle = dlopen(nme, mode);
			if (handle == NULL && fileexists(nme))
				throw(LOADER, "loadLibrary",
					  RUNTIME_LOAD_ERROR
					  " failed to open library %s (from within file '%s'): %s",
					  s, nme, dlerror());
		}
#ifdef __APPLE__
		if (handle == NULL && strcmp(SO_EXT, ".bundle") != 0) {
			/* try .bundle */
			if (is_mod)
				len = snprintf(nme, sizeof(nme), "%.*s%c%s_%s.bundle",
							   (int) (p - mod_path), mod_path, DIR_SEP,
							   SO_PREFIX, s);
			else
				len = snprintf(nme, sizeof(nme), "%.*s%c%s%s.bundle",
							   (int) (p - mod_path), mod_path, DIR_SEP,
							   SO_PREFIX, s);
			if (len == -1 || len >= FILENAME_MAX)
				throw(LOADER, "loadLibrary",
					  RUNTIME_LOAD_ERROR "Library filename path is too large");
			handle = dlopen(nme, mode);
			if (handle == NULL && fileexists(nme))
				throw(LOADER, "loadLibrary",
					  RUNTIME_LOAD_ERROR
					  " failed to open library %s (from within file '%s'): %s",
					  s, nme, dlerror());
		}
#endif
		if (handle)
			TRC_INFO(MAL_LOADER, "Module %s loaded from %s\n", filename, nme);

		if (*p == 0 || handle != NULL)
			break;
		mod_path = p + 1;
	}

	if (handle == NULL) {
		static const char *const optional[] = {
#ifdef HAVE_CUDF
			"capi",
#endif
#ifdef HAVE_FITS
			"fits",
#endif
#ifdef HAVE_GEOM
			"geom",
#endif
#ifdef HAVE_NETCDF
			"netcdf",
#endif
#ifdef HAVE_LIBPY3
			"pyapi3",
#endif
#ifdef HAVE_LIBR
			"rapi",
#endif
#ifdef HAVE_SHP
			"shp",
#endif
			NULL
		};
		for (const char * const *p = optional; *p; p++) {
			if (strcmp(filename, *p) == 0) {
				TRC_INFO(MAL_LOADER, "Optional module %s not loaded\n",
						 filename);
				return MAL_SUCCEED;
			}
		}
		if (!is_monetdb5 && strcmp(filename, "sql") != 0)
			throw(LOADER, "loadLibrary",
				  RUNTIME_LOAD_ERROR
				  " could not locate library %s (from within file '%s'): %s", s,
				  filename, dlerror());
	}

	MT_lock_set(&mal_contextLock);
	if (lastfile == maxfiles) {
		MT_lock_unset(&mal_contextLock);
		if (handle)
			dlclose(handle);
		throw(MAL, "mal.linker",
			  "loadModule internal error, too many modules loaded");
	} else {
		filesLoaded[lastfile].modname = GDKstrdup(filename);
		if (filesLoaded[lastfile].modname == NULL) {
			MT_lock_unset(&mal_contextLock);
			if (handle)
				dlclose(handle);
			throw(LOADER, "loadLibrary",
				  RUNTIME_LOAD_ERROR " could not allocate space");
		}
		filesLoaded[lastfile].fullname = GDKstrdup(handle ? nme : "");
		if (filesLoaded[lastfile].fullname == NULL) {
			GDKfree(filesLoaded[lastfile].modname);
			MT_lock_unset(&mal_contextLock);
			if (handle)
				dlclose(handle);
			throw(LOADER, "loadLibrary",
				  RUNTIME_LOAD_ERROR " could not allocate space");
		}
		filesLoaded[lastfile].handle = handle ? handle : filesLoaded[0].handle;
		lastfile++;
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
	for (i = 0; i < lastfile; i++) {
		if (filesLoaded[i].fullname) {
			/* dlclose(filesLoaded[i].handle); */
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
char *
locate_file(allocator *ma, const char *basename, const char *ext)
{
	const char *mod_path = GDKgetenv("monet_mod_path");
	char *fullname;
	size_t filelen = strlen(basename) + strlen(ext);

	if (mod_path == NULL)
		return NULL;

	while (*mod_path == PATH_SEP)
		mod_path++;
	if (*mod_path == 0)
		return NULL;
	fullname = ma_alloc(ma, PATH_MAX);
	if (fullname == NULL)
		return NULL;
	while (*mod_path) {
		size_t i;
		const char *p;
		int fd;

		if ((p = strchr(mod_path, PATH_SEP)) != NULL) {
			i = p - mod_path;
		} else {
			i = strlen(mod_path);
		}
		if (i + filelen + 2 > PATH_MAX)
			return NULL;
		/* we are now sure the directory name, file
		   base name, extension, and separator fit
		   into fullname, so we don't need to do any
		   extra checks */
		strncpy(fullname, mod_path, i);
		fullname[i] = DIR_SEP;
		strcpy(stpcpy(fullname + i + 1, basename), ext);
		if ((fd = MT_open(fullname, O_RDONLY | O_CLOEXEC)) >= 0) {
			close(fd);
			return fullname;
		}
		if ((mod_path = p) == NULL)
			break;
		while (*mod_path == PATH_SEP)
			mod_path++;
	}
	/* not found */
	return NULL;
}

char *
MSP_locate_script(allocator *ma, const char *filename)
{
	return locate_file(ma, filename, MAL_EXT);
}

char *
MSP_locate_sqlscript(allocator *ma, const char *filename)
{
	/* no directory semantics (yet) */
	return locate_file(ma, filename, SQL_EXT);
}

int
malLibraryEnabled(const char *name)
{
	if (strcmp(name, "pyapi3") == 0) {
		const char *val = GDKgetenv("embedded_py");
		return val && (strcmp(val, "3") == 0 ||
					   strcasecmp(val, "true") == 0 ||
					   strcasecmp(val, "yes") == 0);
	} else if (strcmp(name, "rapi") == 0) {
		const char *val = GDKgetenv("embedded_r");
		return val && (strcasecmp(val, "true") == 0 ||
					   strcasecmp(val, "yes") == 0);
	} else if (strcmp(name, "capi") == 0) {
		const char *val = GDKgetenv("embedded_c");
		return val && (strcasecmp(val, "true") == 0 ||
					   strcasecmp(val, "yes") == 0);
	}
	return true;
}

#define HOW_TO_ENABLE_ERROR(LANGUAGE, OPTION)						\
	do {															\
		if (malLibraryEnabled(name))								\
			return "Embedded " LANGUAGE " has not been installed. "	\
				"Please install it first, then start server with "	\
				"--set " OPTION;									\
		return "Embedded " LANGUAGE " has not been enabled. "		\
			"Start server with --set " OPTION;						\
	} while (0)

char *
malLibraryHowToEnable(const char *name)
{
	if (strcmp(name, "pyapi3") == 0) {
		HOW_TO_ENABLE_ERROR("Python 3", "embedded_py=3");
	} else if (strcmp(name, "rapi") == 0) {
		HOW_TO_ENABLE_ERROR("R", "embedded_r=true");
	} else if (strcmp(name, "capi") == 0) {
		HOW_TO_ENABLE_ERROR("C/C++", "embedded_c=true");
	}
	return "";
}
