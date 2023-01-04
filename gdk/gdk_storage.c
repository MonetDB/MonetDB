/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/*
 * @a M. L. Kersten, P. Boncz, N. Nes
 *
 * @* Database Storage Management
 * Contains routines for writing and reading GDK data to and from
 * disk.  This section contains the primitives to manage the
 * disk-based images of the BATs. It relies on the existence of a UNIX
 * file system, including memory mapped files. Solaris and IRIX have
 * different implementations of madvise().
 *
 * The current version assumes that all BATs are stored on a single
 * disk partition. This simplistic assumption should be replaced in
 * the near future by a multi-volume version. The intention is to use
 * several BAT home locations.  The files should be owned by the
 * database server. Otherwise, IO operations are likely to fail. This
 * is accomplished by setting the GID and UID upon system start.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "mutils.h"
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#ifndef O_CLOEXEC
#ifdef _O_NOINHERIT
#define O_CLOEXEC _O_NOINHERIT	/* Windows */
#else
#define O_CLOEXEC 0
#endif
#endif

/* GDKfilepath returns a newly allocated string containing the path
 * name of a database farm.
 * The arguments are the farmID or -1, the name of a subdirectory
 * within the farm (i.e., something like BATDIR or BAKDIR -- see
 * gdk.h) or NULL, the name of a BAT (i.e. the name that is stored in
 * BBP.dir -- something like 07/714), and finally the file extension.
 *
 * If farmid is >= 0, GDKfilepath returns the complete path to the
 * specified farm concatenated with the other arguments with
 * appropriate separators.  If farmid is -1, it returns the
 * concatenation of its other arguments (in this case, the result
 * cannot be used to access a file directly -- the farm needs to be
 * prepended in some other place). */
char *
GDKfilepath(int farmid, const char *dir, const char *name, const char *ext)
{
	const char *sep;
	size_t pathlen;
	char *path;

	if (GDKinmemory(farmid))
		return GDKstrdup(":memory:");

	assert(dir == NULL || *dir != DIR_SEP);
	assert(farmid == NOFARM ||
	       (farmid >= 0 && farmid < MAXFARMS && BBPfarms[farmid].dirname));
	if (!GDKembedded() && MT_path_absolute(name)) {
		GDKerror("name should not be absolute\n");
		return NULL;
	}
	if (dir && *dir == DIR_SEP)
		dir++;
	if (dir == NULL || dir[0] == 0 || dir[strlen(dir) - 1] == DIR_SEP) {
		sep = "";
	} else {
		sep = DIR_SEP_STR;
	}
	pathlen = (farmid == NOFARM ? 0 : strlen(BBPfarms[farmid].dirname) + 1) +
		(dir ? strlen(dir) : 0) + strlen(sep) + strlen(name) +
		(ext ? strlen(ext) + 1 : 0) + 1;
	path = GDKmalloc(pathlen);
	if (path == NULL)
		return NULL;
 	if (farmid == NOFARM) {
		strconcat_len(path, pathlen,
			      dir ? dir : "", sep, name,
			      ext ? "." : NULL, ext, NULL);
 	} else {
		strconcat_len(path, pathlen,
			      BBPfarms[farmid].dirname, DIR_SEP_STR,
			      dir ? dir : "", sep, name,
			      ext ? "." : NULL, ext, NULL);
 	}
	return path;
}

/* make sure the parent directory of DIR exists (the argument itself
 * is usually a file that is to be created) */
gdk_return
GDKcreatedir(const char *dir)
{
	char path[FILENAME_MAX];
	char *r;
	DIR *dirp;

	TRC_DEBUG(IO_, "GDKcreatedir(%s)\n", dir);
	assert(!GDKinmemory(0));
	if (!GDKembedded() && !MT_path_absolute(dir)) {
		GDKerror("directory '%s' is not absolute\n", dir);
		return GDK_FAIL;
	}
	if (strlen(dir) >= FILENAME_MAX) {
		GDKerror("directory name too long\n");
		return GDK_FAIL;
	}
	strcpy(path, dir);	/* we know this fits (see above) */
	/* skip initial /, if any */
	for (r = strchr(path + 1, DIR_SEP); r; r = strchr(r, DIR_SEP)) {
		*r = 0;
		if (
#ifdef WIN32
			strlen(path) > 3 &&
#endif
			MT_mkdir(path) < 0) {
			if (errno != EEXIST) {
				GDKsyserror("cannot create directory %s\n", path);
				return GDK_FAIL;
			}
			if ((dirp = opendir(path)) == NULL) {
				GDKsyserror("%s cannot open directory\n", path);
				return GDK_FAIL;
			}
			/* it's a directory, we can continue */
			closedir(dirp);
		}
		*r++ = DIR_SEP;
	}
	return GDK_SUCCEED;
}

/* remove the directory DIRNAME with its file contents; does not
 * recurse into subdirectories */
gdk_return
GDKremovedir(int farmid, const char *dirname)
{
	str dirnamestr;
	DIR *dirp;
	char *path;
	struct dirent *dent;
	int ret;

	assert(!GDKinmemory(farmid));
	if ((dirnamestr = GDKfilepath(farmid, NULL, dirname, NULL)) == NULL)
		return GDK_FAIL;

	TRC_DEBUG(IO_, "GDKremovedir(%s)\n", dirnamestr);

	if ((dirp = opendir(dirnamestr)) == NULL) {
		GDKfree(dirnamestr);
		return GDK_SUCCEED;
	}
	while ((dent = readdir(dirp)) != NULL) {
		if (dent->d_name[0] == '.' &&
		    (dent->d_name[1] == 0 ||
		     (dent->d_name[1] == '.' && dent->d_name[2] == 0))) {
			/* skip . and .. */
			continue;
		}
		path = GDKfilepath(farmid, dirname, dent->d_name, NULL);
		if (path == NULL) {
			/* most likely the rmdir will now fail causing
			 * an error return */
			break;
		}
		ret = MT_remove(path);
		if (ret == -1)
			GDKsyserror("remove(%s) failed\n", path);
		TRC_DEBUG(IO_, "Remove %s = %d\n", path, ret);
		GDKfree(path);
	}
	closedir(dirp);
	ret = MT_rmdir(dirnamestr);
	if (ret != 0)
		GDKsyserror("rmdir(%s) failed.\n", dirnamestr);
	TRC_DEBUG(IO_, "rmdir %s = %d\n", dirnamestr, ret);
	GDKfree(dirnamestr);
	return ret ? GDK_FAIL : GDK_SUCCEED;
}

#define _FUNBUF		0x040000
#define _FWRTHR		0x080000
#define _FRDSEQ		0x100000

/* open a file and return its file descriptor; the file is specified
 * using farmid, name and extension; if opening for writing, we create
 * the parent directory if necessary; if opening for reading, we don't
 * necessarily report an error if it fails, but we make sure errno is
 * set */
int
GDKfdlocate(int farmid, const char *nme, const char *mode, const char *extension)
{
	char *path = NULL;
	int fd, flags = O_CLOEXEC;

	assert(!GDKinmemory(farmid));
	if (nme == NULL || *nme == 0) {
		GDKerror("no name specified\n");
		errno = EFAULT;
		return -1;
	}

	assert(farmid != NOFARM || extension == NULL);
	if (farmid != NOFARM) {
		path = GDKfilepath(farmid, BATDIR, nme, extension);
		if (path == NULL) {
			errno = ENOMEM;
			return -1;
		}
		nme = path;
	}

	if (*mode == 'm') {	/* file open for mmap? */
		mode++;
#ifdef _CYGNUS_H_
	} else {
		flags |= _FRDSEQ;	/* WIN32 CreateFile(FILE_FLAG_SEQUENTIAL_SCAN) */
#endif
	}

	if (strchr(mode, 'w')) {
		flags |= O_WRONLY | O_CREAT;
	} else if (!strchr(mode, '+')) {
		flags |= O_RDONLY;
	} else {
		flags |= O_RDWR;
	}
#ifdef WIN32
	flags |= strchr(mode, 'b') ? O_BINARY : O_TEXT;
#endif
	fd = MT_open(nme, flags);
	if (fd < 0 && *mode == 'w') {
		/* try to create the directory, in case that was the problem */
		if (GDKcreatedir(nme) == GDK_SUCCEED) {
			fd = MT_open(nme, flags);
			if (fd < 0)
				GDKsyserror("cannot open file %s\n", nme);
		}
	}
	int err = errno;	/* save */
	/* don't generate error if we can't open a file for reading */
	GDKfree(path);
	errno = err;		/* restore */
	return fd;
}

/* like GDKfdlocate, except return a FILE pointer */
FILE *
GDKfilelocate(int farmid, const char *nme, const char *mode, const char *extension)
{
	int fd;
	FILE *f;

	if ((fd = GDKfdlocate(farmid, nme, mode, extension)) < 0)
		return NULL;
	if (*mode == 'm')
		mode++;
	if ((f = fdopen(fd, mode)) == NULL) {
		GDKsyserror("cannot fdopen file\n");
		close(fd);
		return NULL;
	}
	return f;
}

FILE *
GDKfileopen(int farmid, const char *dir, const char *name, const char *extension, const char *mode)
{
	char *path;

	/* if name is null, try to get one from dir (in case it was a path) */
	path = GDKfilepath(farmid, dir, name, extension);

	if (path != NULL) {
		FILE *f;
		TRC_DEBUG(IO_, "GDKfileopen(%s)\n", path);
		f = MT_fopen(path, mode);
		int err = errno;
		GDKfree(path);
		errno = err;
		return f;
	}
	return NULL;
}

/* remove the file */
gdk_return
GDKunlink(int farmid, const char *dir, const char *nme, const char *ext)
{
	if (nme && *nme) {
		char *path;

		path = GDKfilepath(farmid, dir, nme, ext);
		if (path == NULL)
			return GDK_FAIL;
		/* if file already doesn't exist, we don't care */
		if (MT_remove(path) != 0 && errno != ENOENT) {
			GDKsyserror("remove(%s)\n", path);
			GDKfree(path);
			return GDK_FAIL;
		}
		GDKfree(path);
		return GDK_SUCCEED;
	}
	GDKerror("no name specified");
	return GDK_FAIL;
}

/*
 * A move routine is overloaded to deal with extensions.
 */
gdk_return
GDKmove(int farmid, const char *dir1, const char *nme1, const char *ext1, const char *dir2, const char *nme2, const char *ext2, bool report)
{
	char *path1;
	char *path2;
	int ret, t0 = GDKms();

	if (nme1 == NULL || *nme1 == 0) {
		GDKerror("no file specified\n");
		return GDK_FAIL;
	}
	path1 = GDKfilepath(farmid, dir1, nme1, ext1);
	path2 = GDKfilepath(farmid, dir2, nme2, ext2);
	if (path1 && path2) {
		ret = MT_rename(path1, path2);
		if (ret < 0 && report)
			GDKsyserror("cannot rename %s to %s\n", path1, path2);

		TRC_DEBUG(IO_, "Move %s %s = %d (%dms)\n", path1, path2, ret, GDKms() - t0);
	} else {
		ret = -1;
	}
	GDKfree(path1);
	GDKfree(path2);
	return ret < 0 ? GDK_FAIL : GDK_SUCCEED;
}

gdk_return
GDKextendf(int fd, size_t size, const char *fn)
{
	struct stat stb;
	int rt = 0;
	int t0 = GDKms();

	assert(!GDKinmemory(0));
#ifdef __COVERITY__
	if (fd < 0)		/* in real life, if fd < 0, fstat will fail */
		return GDK_FAIL;
#endif
	if (fstat(fd, &stb) < 0) {
		/* shouldn't happen */
		GDKsyserror("fstat failed unexpectedly\n");
		return GDK_FAIL;
	}
	/* if necessary, extend the underlying file */
	if (stb.st_size < (off_t) size) {
#ifdef HAVE_FALLOCATE
		if ((rt = fallocate(fd, 0, stb.st_size, (off_t) size - stb.st_size)) < 0 &&
		    errno == EOPNOTSUPP)
			/* on Linux, posix_fallocate uses a slow
			 * method to allocate blocks if the underlying
			 * file system doesn't support the operation,
			 * so use fallocate instead and just resize
			 * the file if it fails */
#else
#ifdef HAVE_POSIX_FALLOCATE
		/* posix_fallocate returns error number on failure,
		 * not -1 :-( */
		if ((rt = posix_fallocate(fd, stb.st_size, (off_t) size - stb.st_size)) == EINVAL)
			/* on Solaris/OpenIndiana, this may mean that
			 * the underlying file system doesn't support
			 * the operation, so just resize the file */
#endif
#endif
			/* we get here when (posix_)fallocate fails
			 * because it is not supported on the file
			 * system, or if neither function exists */
			rt = ftruncate(fd, (off_t) size);
		if (rt != 0) {
			/* extending failed, try to reduce file size
			 * back to original */
			GDKsyserror("could not extend file\n");
			if (ftruncate(fd, stb.st_size))
				GDKsyserror("ftruncate to old size");
		}
	}
	TRC_DEBUG(IO_, "GDKextend %s %zu -> %zu %dms%s\n",
		  fn, (size_t) stb.st_size, size,
		  GDKms() - t0, rt != 0 ? " (failed)" : "");
	/* posix_fallocate returns != 0 on failure, fallocate and
	 * ftruncate return -1 on failure, but all three return 0 on
	 * success */
	return rt != 0 ? GDK_FAIL : GDK_SUCCEED;
}

gdk_return
GDKextend(const char *fn, size_t size)
{
	int fd, flags = O_RDWR;
	gdk_return rt = GDK_FAIL;

	assert(!GDKinmemory(0));
#ifdef O_BINARY
	/* On Windows, open() fails if the file is bigger than 2^32
	 * bytes without O_BINARY. */
	flags |= O_BINARY;
#endif
	if ((fd = MT_open(fn, flags | O_CLOEXEC)) >= 0) {
		rt = GDKextendf(fd, size, fn);
		close(fd);
	} else {
		GDKsyserror("cannot open file %s\n", fn);
	}
	return rt;
}

/*
 * @+ Save and load.
 * The BAT is saved on disk in several files. The extension DESC
 * denotes the descriptor, BUNs the bun heap, and HHEAP and THEAP the
 * other heaps. The storage mechanism off a file can be memory mapped
 * (STORE_MMAP) or malloced (STORE_MEM).
 *
 * These modes indicates the disk-layout and the intended mapping.
 * The primary concern here is to handle STORE_MMAP and STORE_MEM.
 */
gdk_return
GDKsave(int farmid, const char *nme, const char *ext, void *buf, size_t size, storage_t mode, bool dosync)
{
	int err = 0;

	TRC_DEBUG(IO_, "GDKsave: name=%s, ext=%s, mode %d, dosync=%d\n", nme, ext ? ext : "", (int) mode, dosync);

	assert(!GDKinmemory(farmid));
	if (mode == STORE_MMAP) {
		if (dosync && size && !(GDKdebug & NOSYNCMASK))
			err = MT_msync(buf, size);
		if (err)
			GDKerror("error on: name=%s, ext=%s, mode=%d\n",
				 nme, ext ? ext : "", (int) mode);
		TRC_DEBUG(IO_, "MT_msync(buf %p, size %zu) = %d\n",
			  buf, size, err);
	} else {
		int fd;

		if ((fd = GDKfdlocate(farmid, nme, "wb", ext)) >= 0) {
			/* write() on 64-bits Redhat for IA64 returns
			 * 32-bits signed result (= OS BUG)! write()
			 * on Windows only takes unsigned int as
			 * size */
			while (size > 0) {
				/* circumvent problems by writing huge
				 * buffers in chunks <= 1GiB */
				ssize_t ret;

				ret = write(fd, buf,
					    (unsigned) MIN(1 << 30, size));
				if (ret < 0) {
					err = -1;
					GDKsyserror("GDKsave: error %zd"
						    " on: name=%s, ext=%s, "
						    "mode=%d\n", ret, nme,
						    ext ? ext : "", (int) mode);
					break;
				}
				size -= ret;
				buf = (void *) ((char *) buf + ret);
				TRC_DEBUG(IO_, "Write(fd %d, buf %p"
					  ", size %u) = %zd\n",
					  fd, buf,
					  (unsigned) MIN(1 << 30, size),
					  ret);
			}
			if (dosync && !(GDKdebug & NOSYNCMASK)
#if defined(NATIVE_WIN32)
			    && _commit(fd) < 0
#elif defined(HAVE_FDATASYNC)
			    && fdatasync(fd) < 0
#elif defined(HAVE_FSYNC)
			    && fsync(fd) < 0
#endif
				) {
				GDKsyserror("GDKsave: error on: name=%s, "
					    "ext=%s, mode=%d\n", nme,
					    ext ? ext : "", (int) mode);
				err = -1;
			}
			err |= close(fd);
			if (err && GDKunlink(farmid, BATDIR, nme, ext) != GDK_SUCCEED) {
				/* do not tolerate corrupt heap images
				 * (BBPrecover on restart will kill
				 * them) */
				GDKerror("could not remove: name=%s, "
					 "ext=%s, mode %d\n", nme,
					 ext ? ext : "", (int) mode);
				return GDK_FAIL;
			}
		} else {
			err = -1;
			GDKerror("failed name=%s, ext=%s, mode %d\n",
				 nme, ext ? ext : "", (int) mode);
		}
	}
	return err ? GDK_FAIL : GDK_SUCCEED;
}

/*
 * Space for the load is directly allocated and the heaps are mapped.
 * Further initialization of the atom heaps require a separate action
 * defined in their implementation.
 *
 * size -- how much to read
 * *maxsize -- (in/out) how much to allocate / how much was allocated
 */
char *
GDKload(int farmid, const char *nme, const char *ext, size_t size, size_t *maxsize, storage_t mode)
{
	char *ret = NULL;

	assert(!GDKinmemory(farmid));
	assert(size <= *maxsize);
	assert(farmid != NOFARM || ext == NULL);
	TRC_DEBUG(IO_, "GDKload: name=%s, ext=%s, mode %d\n", nme, ext ? ext : "", (int) mode);

	if (mode == STORE_MEM) {
		int fd = GDKfdlocate(farmid, nme, "rb", ext);

		if (fd >= 0) {
			char *dst = ret = GDKmalloc(*maxsize);
			ssize_t n_expected, n = 0;

			if (ret) {
				/* read in chunks, some OSs do not
				 * give you all at once and Windows
				 * only accepts int */
				for (n_expected = (ssize_t) size; n_expected > 0; n_expected -= n) {
					n = read(fd, dst, (unsigned) MIN(1 << 30, n_expected));
					if (n < 0)
						GDKsyserror("GDKload: cannot read: name=%s, ext=%s, %zu bytes missing.\n", nme, ext ? ext : "", (size_t) n_expected);
#ifndef __COVERITY__
					/* Coverity doesn't seem to
					 * recognize that we're just
					 * printing the value of ptr,
					 * not its contents */
					TRC_DEBUG(IO_, "read(dst %p, n_expected %zd, fd %d) = %zd\n", (void *)dst, n_expected, fd, n);
#endif

					if (n <= 0)
						break;
					dst += n;
				}
				if (n_expected > 0) {
					/* we couldn't read all, error
					 * already generated */
					GDKfree(ret);
					GDKerror("short read from heap %s%s%s, expected %zu, missing %zd\n", nme, ext ? "." : "", ext ? ext : "", size, n_expected);
					ret = NULL;
				}
#ifndef NDEBUG
				/* just to make valgrind happy, we
				 * initialize the whole thing */
				if (ret && *maxsize > size)
					memset(ret + size, 0, *maxsize - size);
#endif
			}
			close(fd);
		} else {
			GDKsyserror("cannot open: name=%s, ext=%s\n", nme, ext ? ext : "");
		}
	} else {
		char *path = NULL;

		/* round up to multiple of GDK_mmap_pagesize with a
		 * minimum of one */
		size = (*maxsize + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
		if (size == 0)
			size = GDK_mmap_pagesize;
		if (farmid != NOFARM) {
			path = GDKfilepath(farmid, BATDIR, nme, ext);
			nme = path;
		}
		if (nme != NULL && GDKextend(nme, size) == GDK_SUCCEED) {
			int mod = MMAP_READ | MMAP_WRITE | MMAP_SEQUENTIAL;

			if (mode == STORE_PRIV)
				mod |= MMAP_COPY;
			else
				mod |= MMAP_SYNC;
			ret = GDKmmap(nme, mod, size);
			if (ret != NULL) {
				/* success: update allocated size */
				*maxsize = size;
			}
			TRC_DEBUG(IO_, "mmap(NULL, 0, maxsize %zu, mod %d, path %s, 0) = %p\n", size, mod, nme, (void *)ret);
		}
		GDKfree(path);
	}
	return ret;
}

/*
 * @+ BAT disk storage
 *
 * Between sessions the BATs comprising the database are saved on
 * disk.  To simplify code, we assume a UNIX directory called its
 * physical @%home@ where they are to be located.  The subdirectories
 * BAT and PRG contain what its name says.
 *
 * A BAT created by @%COLnew@ is considered temporary until one calls
 * the routine @%BATsave@. This routine reserves disk space and checks
 * for name clashes.
 *
 * Saving and restoring BATs is left to the upper layers. The library
 * merely copies the data into place.  Failure to read or write the
 * BAT results in a NULL, otherwise it returns the BAT pointer.
 */
static BAT *
DESCload(int i)
{
	const char *s, *nme = BBP_physical(i);
	BAT *b = NULL;
	int tt;

	TRC_DEBUG(IO_, "DESCload: %s\n", nme ? nme : "<noname>");

	b = BBP_desc(i);

	if (b == NULL) {
		GDKerror("no descriptor for BAT %d\n", i);
		return NULL;
	}

	MT_lock_set(&b->theaplock);
	tt = b->ttype;
	if (tt < 0) {
		if ((tt = ATOMindex(s = ATOMunknown_name(tt))) < 0) {
			MT_lock_unset(&b->theaplock);
			GDKerror("atom '%s' unknown, in BAT '%s'.\n", s, nme);
			return NULL;
		}
		b->ttype = tt;
	}

	/* reconstruct mode from BBP status (BATmode doesn't flush
	 * descriptor, so loaded mode may be stale) */
	b->batTransient = (BBP_status(b->batCacheid) & BBPPERSISTENT) == 0;
	b->batCopiedtodisk = true;
	MT_lock_unset(&b->theaplock);
	return b;
}

gdk_return
BATsave_iter(BAT *b, BATiter *bi, BUN size)
{
	gdk_return err = GDK_SUCCEED;
	const char *nme;
	bool dosync;
	bool locked = false;

	BATcheck(b, GDK_FAIL);

	if (MT_rwlock_rdtry(&b->thashlock))
		locked = true;

	dosync = (BBP_status(b->batCacheid) & BBPPERSISTENT) != 0;
	assert(!GDKinmemory(bi->h->farmid));
	/* views cannot be saved, but make an exception for
	 * force-remapped views */
	if (isVIEW(b)) {
		if (locked)
			MT_rwlock_rdunlock(&b->thashlock);
		GDKerror("%s is a view on %s; cannot be saved\n", BATgetId(b), BBP_logical(VIEWtparent(b)));
		return GDK_FAIL;
	}
	if (!BATdirtybi(*bi)) {
		if (locked)
			MT_rwlock_rdunlock(&b->thashlock);
		return GDK_SUCCEED;
	}

	/* start saving data */
	nme = BBP_physical(b->batCacheid);
	const char *tail = BATITERtailname(bi);
	if (bi->type != TYPE_void && bi->base == NULL) {
		assert(BBP_status(b->batCacheid) & BBPSWAPPED);
		if (dosync && !(GDKdebug & NOSYNCMASK)) {
			int fd = GDKfdlocate(bi->h->farmid, nme, "rb+", tail);
			if (fd < 0) {
				GDKsyserror("cannot open file %s.%s for sync\n",
					    nme, tail);
				err = GDK_FAIL;
			} else {
				if (
#if defined(NATIVE_WIN32)
					_commit(fd) < 0
#elif defined(HAVE_FDATASYNC)
					fdatasync(fd) < 0
#elif defined(HAVE_FSYNC)
					fsync(fd) < 0
#endif
					)
					GDKsyserror("sync failed for %s.%s\n",
						    nme, tail);
				close(fd);
			}
			if (bi->vh) {
				fd = GDKfdlocate(bi->vh->farmid, nme, "rb+", "theap");
				if (fd < 0) {
					GDKsyserror("cannot open file %s.theap for sync\n",
						    nme);
					err = GDK_FAIL;
				} else {
					if (
#if defined(NATIVE_WIN32)
						_commit(fd) < 0
#elif defined(HAVE_FDATASYNC)
						fdatasync(fd) < 0
#elif defined(HAVE_FSYNC)
						fsync(fd) < 0
#endif
						)
						GDKsyserror("sync failed for %s.theap\n", nme);
					close(fd);
				}
			}
		}
	} else {
		if (!bi->copiedtodisk || bi->hdirty)
			if (err == GDK_SUCCEED && bi->type)
				err = HEAPsave(bi->h, nme, tail, dosync, bi->hfree, &b->theaplock);
		if (bi->vh
		    && (!bi->copiedtodisk || bi->vhdirty)
		    && ATOMvarsized(bi->type)
		    && err == GDK_SUCCEED)
			err = HEAPsave(bi->vh, nme, "theap", dosync, bi->vhfree, &b->theaplock);
	}

	if (err == GDK_SUCCEED) {
		MT_lock_set(&b->theaplock);
		if (b->theap != bi->h) {
			assert(b->theap->dirty);
			b->theap->wasempty = bi->h->wasempty;
		}
		if (b->tvheap && b->tvheap != bi->vh) {
			assert(b->tvheap->dirty);
			b->tvheap->wasempty = bi->vh->wasempty;
		}
		if (size != b->batCount || b->batInserted < b->batCount) {
			/* if the sizes don't match, the BAT must be dirty */
			b->batCopiedtodisk = false;
			b->theap->dirty = true;
			if (b->tvheap)
				b->tvheap->dirty = true;
		} else {
			b->batCopiedtodisk = true;
		}
		MT_lock_unset(&b->theaplock);
		if (locked &&  b->thash && b->thash != (Hash *) 1)
			BAThashsave(b, dosync);
	}
	if (locked)
		MT_rwlock_rdunlock(&b->thashlock);
	return err;
}

gdk_return
BATsave(BAT *b)
{
	gdk_return rc;

	BATiter bi = bat_iterator(b);
	rc = BATsave_iter(b, &bi, bi.count);
	bat_iterator_end(&bi);
	return rc;
}

/*
 * TODO: move to gdk_bbp.c
 */
BAT *
BATload_intern(bat bid, bool lock)
{
	const char *nme;
	BAT *b;

	assert(!GDKinmemory(0));
	assert(bid > 0);

	nme = BBP_physical(bid);
	b = DESCload(bid);

	if (b == NULL) {
		return NULL;
	}
	assert(!GDKinmemory(b->theap->farmid));

	/* LOAD bun heap */
	if (b->ttype != TYPE_void) {
		b->theap->storage = b->theap->newstorage = STORE_INVALID;
		if ((b->batCount == 0 ?
		     HEAPalloc(b->theap, b->batCapacity, b->twidth) :
		     HEAPload(b->theap, b->theap->filename, NULL, b->batRestricted == BAT_READ)) != GDK_SUCCEED) {
			HEAPfree(b->theap, false);
			return NULL;
		}
		if (ATOMstorage(b->ttype) == TYPE_msk) {
			b->batCapacity = (BUN) (b->theap->size * 8);
		} else {
			assert(b->theap->size >> b->tshift <= BUN_MAX);
			b->batCapacity = (BUN) (b->theap->size >> b->tshift);
		}
	} else {
		b->theap->base = NULL;
	}

	/* LOAD tail heap */
	if (ATOMvarsized(b->ttype)) {
		b->tvheap->storage = b->tvheap->newstorage = STORE_INVALID;
		if ((b->tvheap->free == 0 ?
		     ATOMheap(b->ttype, b->tvheap, b->batCapacity) :
		     HEAPload(b->tvheap, nme, "theap", b->batRestricted == BAT_READ)) != GDK_SUCCEED) {
			HEAPfree(b->theap, false);
			HEAPfree(b->tvheap, false);
			return NULL;
		}
		if (ATOMstorage(b->ttype) == TYPE_str) {
			strCleanHash(b->tvheap, false);	/* ensure consistency */
		} else {
			HEAP_recover(b->tvheap, (const var_t *) Tloc(b, 0),
				     BATcount(b));
		}
	}

	/* initialize descriptor */
	b->theap->parentid = b->batCacheid;

	/* load succeeded; register it in BBP */
	if (BBPcacheit(b, lock) != GDK_SUCCEED) {
		HEAPfree(b->theap, false);
		if (b->tvheap)
			HEAPfree(b->tvheap, false);
		return NULL;
	}
	return b;
}

/*
 * @- BATdelete
 * The new behavior is to let the routine produce warnings but always
 * succeed.  rationale: on a delete, we must get rid of *all* the
 * files. We do not have to care about preserving them or be too much
 * concerned if a file that had to be deleted was not found (end
 * result is still that it does not exist). The past behavior to
 * delete some files and then fail was erroneous. The BAT would
 * continue to exist with an incorrect disk status, causing havoc
 * later on.
 *
 * NT forces us to close all files before deleting them; in case of
 * memory mapped files this means that we have to unload the BATs
 * before deleting. This is enforced now.
 */
void
BATdelete(BAT *b)
{
	bat bid = b->batCacheid;
	BAT *loaded = BBP_cache(bid);
	char o[10];

	assert(bid > 0);
	snprintf(o, sizeof(o), "%o", (unsigned) bid);
	if (loaded) {
		b = loaded;
	}
	HASHdestroy(b);
	IMPSdestroy(b);
	OIDXdestroy(b);
	PROPdestroy_nolock(b);
	STRMPdestroy(b);
	if (b->theap) {
		HEAPfree(b->theap, true);
	}
	if (b->tvheap) {
		HEAPfree(b->tvheap, true);
	}
	b->batCopiedtodisk = false;
}

/*
 * BAT specific printing
 */

gdk_return
BATprintcolumns(stream *s, int argc, BAT *argv[])
{
	int i;
	BUN n, cnt;
	struct colinfo {
		ssize_t (*s) (str *, size_t *, const void *, bool);
		BATiter i;
	} *colinfo;
	char *buf;
	size_t buflen = 0;
	ssize_t len;
	gdk_return rc = GDK_SUCCEED;

	/* error checking */
	for (i = 0; i < argc; i++) {
		if (argv[i] == NULL) {
			GDKerror("Columns missing\n");
			return GDK_FAIL;
		}
		if (BATcount(argv[0]) != BATcount(argv[i])) {
			GDKerror("Columns must be the same size\n");
			return GDK_FAIL;
		}
	}

	if ((colinfo = GDKmalloc(argc * sizeof(*colinfo))) == NULL) {
		GDKerror("Cannot allocate memory\n");
		return GDK_FAIL;
	}

	for (i = 0; i < argc; i++) {
		colinfo[i].i = bat_iterator(argv[i]);
		colinfo[i].s = BATatoms[argv[i]->ttype].atomToStr;
	}

	mnstr_write(s, "#--------------------------#\n", 1, 29);
	mnstr_write(s, "# ", 1, 2);
	for (i = 0; i < argc; i++) {
		if (i > 0)
			mnstr_write(s, "\t", 1, 1);
		const char *nm = argv[i]->tident;
		mnstr_write(s, nm, 1, strlen(nm));
	}
	mnstr_write(s, "  # name\n", 1, 9);
	mnstr_write(s, "# ", 1, 2);
	for (i = 0; i < argc; i++) {
		if (i > 0)
			mnstr_write(s, "\t", 1, 1);
		const char *nm = ATOMname(argv[i]->ttype);
		mnstr_write(s, nm, 1, strlen(nm));
	}
	mnstr_write(s, "  # type\n", 1, 9);
	mnstr_write(s, "#--------------------------#\n", 1, 29);
	buf = NULL;

	for (n = 0, cnt = BATcount(argv[0]); n < cnt; n++) {
		mnstr_write(s, "[ ", 1, 2);
		for (i = 0; i < argc; i++) {
			len = colinfo[i].s(&buf, &buflen, BUNtail(colinfo[i].i, n), true);
			if (len < 0) {
				rc = GDK_FAIL;
				goto bailout;
			}
			if (i > 0)
				mnstr_write(s, ",\t", 1, 2);
			mnstr_write(s, buf, 1, len);
		}
		mnstr_write(s, "  ]\n", 1, 4);
	}

  bailout:
	for (i = 0; i < argc; i++) {
		bat_iterator_end(&colinfo[i].i);
	}
	GDKfree(buf);
	GDKfree(colinfo);

	return rc;
}

gdk_return
BATprint(stream *fdout, BAT *b)
{
	if (complex_cand(b)) {
		struct canditer ci;
		canditer_init(&ci, NULL, b);
		oid hseq = ci.hseq;

		mnstr_printf(fdout,
			     "#--------------------------#\n"
			     "# h\t%s  # name\n"
			     "# void\toid  # type\n"
			     "#--------------------------#\n",
			     b->tident);
		for (BUN i = 0; i < ci.ncand; i++) {
			oid o = canditer_next(&ci);
			mnstr_printf(fdout,
				     "[ " OIDFMT "@0,\t" OIDFMT "@0  ]\n",
				     (oid) (i + hseq), o);
		}
		return GDK_SUCCEED;
	}

	BAT *argv[2];
	gdk_return ret = GDK_FAIL;

	argv[0] = BATdense(b->hseqbase, b->hseqbase, BATcount(b));
	if (argv[0]) {
		argv[1] = b;
		ret = BATroles(argv[0], "h");
		if (ret == GDK_SUCCEED)
			ret = BATprintcolumns(fdout, 2, argv);
		BBPunfix(argv[0]->batCacheid);
	}
	return ret;
}
