/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
 * the near future by a multi-volume version. The intension is to use
 * several BAT home locations.  The files should be owned by the
 * database server. Otherwise, IO operations are likely to fail. This
 * is accomplished by setting the GID and UID upon system start.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include <stdlib.h>
#include "gdk_storage.h"
#include "mutils.h"
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

void
GDKfilepath(str path, const char *dir, const char *name, const char *ext)
{
	*path = 0;
	if (dir && *dir && *name != DIR_SEP) {
		strcpy(path, dir);
		path += strlen(dir);
		if (path[-1] != DIR_SEP) {
			*path++ = DIR_SEP;
			*path = 0;
		}
	}
	strcpy(path, name);
	if (ext) {
		path += strlen(name);
		*path++ = '.';
		strcpy(path, ext);
	}
}

int
GDKcreatedir(const char *dir)
{
	char path[PATHLENGTH];
	char *r;
	int ret = FALSE;

	assert(strlen(dir) < sizeof(path));
	strncpy(path, dir, sizeof(path)-1);
	path[sizeof(path)-1] = 0;
	r = strrchr(path, DIR_SEP);
	IODEBUG THRprintf(GDKstdout, "#GDKcreatedir(%s)\n", path);

	if (r) {
		DIR *dirp;

		*r = 0;
		dirp = opendir(path);
		if (dirp) {
			closedir(dirp);
		} else {
			GDKcreatedir(path);
			ret = mkdir(path, 0755);
			IODEBUG THRprintf(GDKstdout, "#mkdir %s = %d\n", path, ret);
			if (ret < 0 && (dirp = opendir(path)) != NULL) {
				/* resolve race */
				ret = 0;
				closedir(dirp);
			}
		}
		*r = DIR_SEP;
	}
	return !ret;
}

int
GDKremovedir(const char *dirname)
{
	DIR *dirp = opendir(dirname);
	char path[PATHLENGTH];
	struct dirent *dent;
	int ret;

	IODEBUG THRprintf(GDKstdout, "#GDKremovedir(%s)\n", dirname);

	if (dirp == NULL)
		return 0;
	while ((dent = readdir(dirp)) != NULL) {
		if ((dent->d_name[0] == '.') && ((dent->d_name[1] == 0) || (dent->d_name[1] == '.' && dent->d_name[2] == 0))) {
			continue;
		}
		GDKfilepath(path, dirname, dent->d_name, NULL);
		ret = unlink(path);
		IODEBUG THRprintf(GDKstdout, "#unlink %s = %d\n", path, ret);
	}
	closedir(dirp);
	ret = rmdir(dirname);
	if (ret < 0) {
		GDKsyserror("GDKremovedir: rmdir(%s) failed.\n", dirname);
	}
	IODEBUG THRprintf(GDKstdout, "#rmdir %s = %d\n", dirname, ret);

	return ret;
}

#define _FUNBUF         0x040000
#define _FWRTHR         0x080000
#define _FRDSEQ         0x100000

int
GDKfdlocate(const char *nme, const char *mode, const char *extension)
{
	char path[PATHLENGTH];
	int fd, flags = 0;

	if ((nme == NULL) || (*nme == 0)) {
		return 0;
	}
	GDKfilepath(path, BATDIR, nme, extension);

	if (*mode == 'm') {	/* file open for mmap? */
		mode++;
#ifdef _CYGNUS_H_
	} else {
		flags = _FRDSEQ;	/* WIN32 CreateFile(FILE_FLAG_SEQUENTIAL_SCAN) */
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
	fd = open(path, flags, MONETDB_MODE);
	if (fd < 0 && *mode == 'w') {
		/* try to create the directory, in case that was the problem */
		if (GDKcreatedir(path)) {
			fd = open(path, flags, MONETDB_MODE);
		}
	}
	return fd;
}

FILE *
GDKfilelocate(const char *nme, const char *mode, const char *extension)
{
	int fd = GDKfdlocate(nme, mode, extension);

	if (*mode == 'm')
		mode++;
	return (fd < 0) ? NULL : fdopen(fd, mode);
}


/*
 * Unlink the file.
 */
int
GDKunlink(const char *dir, const char *nme, const char *ext)
{
	char path[PATHLENGTH];

	if (nme && *nme) {
		GDKfilepath(path, dir, nme, ext);
		/* if file already doesn't exist, we don't care */
		if (unlink(path) == -1 && errno != ENOENT) {
			GDKsyserror("GDKunlink(%s)\n", path);
			IODEBUG THRprintf(GDKstdout, "#unlink %s = -1\n", path);
			return -1;
		}
		return 0;
	}
	return -1;
}

/*
 * A move routine is overloaded to deal with extensions.
 */
int
GDKmove(const char *dir1, const char *nme1, const char *ext1, const char *dir2, const char *nme2, const char *ext2)
{
	char path1[PATHLENGTH];
	char path2[PATHLENGTH];
	int ret, t0 = 0;

	IODEBUG t0 = GDKms();

	if ((nme1 == NULL) || (*nme1 == 0)) {
		return -1;
	}
	GDKfilepath(path1, dir1, nme1, ext1);
	GDKfilepath(path2, dir2, nme2, ext2);
	ret = rename(path1, path2);

	IODEBUG THRprintf(GDKstdout, "#move %s %s = %d (%dms)\n", path1, path2, ret, GDKms() - t0);

	return ret;
}

int
GDKextendf(int fd, size_t size)
{
	struct stat stb;

	if (fstat(fd, &stb) < 0) {
		/* shouldn't happen */
		return -1;
	}
	/* if necessary, extend the underlying file */
	if (stb.st_size < (off_t) size) {
#ifdef WIN32
		return -(_chsize_s(fd, (__int64) size) != 0);
#else
		return ftruncate(fd, (off_t) size);
#endif
	}
	return 0;
}

int
GDKextend(const char *fn, size_t size)
{
	int t0 = 0;
	int rt = -1, fd;

	IODEBUG t0 = GDKms();
	rt = -1;
	if ((fd = open(fn, O_RDWR)) >= 0) {
		rt = GDKextendf(fd, size);
		close(fd);
	}
	IODEBUG fprintf(stderr, "#GDKextend %s " SZFMT " %dms%s\n", fn, size,
			GDKms() - t0, rt < 0 ? " (failed)" : "");
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
int
GDKsave(const char *nme, const char *ext, void *buf, size_t size, storage_t mode)
{
	int err = 0;

	IODEBUG THRprintf(GDKstdout, "#GDKsave: name=%s, ext=%s, mode %d\n", nme, ext ? ext : "", (int) mode);

	if (mode == STORE_MMAP) {
		if (size)
			err = MT_msync(buf, size, MMAP_SYNC);
		if (err)
			GDKsyserror("GDKsave: error on: name=%s, ext=%s, "
				    "mode=%d\n", nme, ext ? ext : "",
				    (int) mode);
		IODEBUG THRprintf(GDKstdout,
				  "#MT_msync(buf " PTRFMT ", size " SZFMT
				  ", MMAP_SYNC) = %d\n",
				  PTRFMTCAST buf, size, err);
	} else {
		int fd;

		if ((fd = GDKfdlocate(nme, "wb", ext)) >= 0) {
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
					GDKsyserror("GDKsave: error " SSZFMT
						    " on: name=%s, ext=%s, "
						    "mode=%d\n", ret, nme,
						    ext ? ext : "", (int) mode);
					break;
				}
				size -= ret;
				buf = (void *) ((char *) buf + ret);
				IODEBUG THRprintf(GDKstdout,
						  "#write(fd %d, buf " PTRFMT
						  ", size %u) = " SSZFMT "\n",
						  fd, PTRFMTCAST buf,
						  (unsigned) MIN(1 << 30, size),
						  ret);
			}
			if (!(GDKdebug & FORCEMITOMASK) &&
#if defined(NATIVE_WIN32)
			    _commit(fd) < 0
#elif defined(HAVE_FDATASYNC)
			    fdatasync(fd) < 0
#elif defined(HAVE_FSYNC)
			    fsync(fd) < 0
#else
			    0
#endif
				) {
				GDKsyserror("GDKsave: error on: name=%s, "
					    "ext=%s, mode=%d\n", nme,
					    ext ? ext : "", (int) mode);
				err = -1;
			}
			err |= close(fd);
			if (err && GDKunlink(BATDIR, nme, ext)) {
				/* do not tolerate corrupt heap images
				 * (BBPrecover on restart will kill
				 * them) */
				GDKfatal("GDKsave: could not open: name=%s, "
					 "ext=%s, mode %d\n", nme,
					 ext ? ext : "", (int) mode);
			}
		} else {
			err = -1;
			GDKerror("GDKsave: failed name=%s, ext=%s, mode %d\n",
				 nme, ext ? ext : "", (int) mode);
		}
	}
	return err;
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
GDKload(const char *nme, const char *ext, size_t size, size_t *maxsize, storage_t mode)
{
	char *ret = NULL;

	assert(size <= *maxsize);
	IODEBUG {
		THRprintf(GDKstdout, "#GDKload: name=%s, ext=%s, mode %d\n", nme, ext ? ext : "", (int) mode);
	}
	if (mode == STORE_MEM) {
		int fd = GDKfdlocate(nme, "rb", ext);

		if (fd >= 0) {
			char *dst = ret = GDKmalloc(*maxsize);
			ssize_t n_expected, n = 0;

			if (ret) {
				/* read in chunks, some OSs do not
				 * give you all at once and Windows
				 * only accepts int */
				for (n_expected = (ssize_t) size; n_expected > 0; n_expected -= n) {
					n = read(fd, dst, (unsigned) MIN(1 << 30, n_expected));
					IODEBUG THRprintf(GDKstdout, "#read(dst " PTRFMT ", n_expected " SSZFMT ", fd %d) = " SSZFMT "\n", PTRFMTCAST(void *)dst, n_expected, fd, n);

					if (n <= 0)
						break;
					dst += n;
				}
				if (n_expected > 0) {
					GDKfree(ret);
					GDKsyserror("GDKload: cannot read: name=%s, ext=%s, " SZFMT " bytes missing.\n", nme, ext ? ext : "", (size_t) n_expected);
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
			GDKsyserror("GDKload: cannot open: name=%s, ext=%s\n", nme, ext ? ext : "");
		}
	} else {
		char path[PATHLENGTH];

		/* round up to multiple of GDK_mmap_pagesize with a
		 * minimum of one */
		size = (*maxsize + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
		if (size == 0)
			size = GDK_mmap_pagesize;
		GDKfilepath(path, BATDIR, nme, ext);
		if (GDKextend(path, size) == 0) {
			int mod = MMAP_READ | MMAP_WRITE | MMAP_SEQUENTIAL | MMAP_SYNC;

			if (mode == STORE_PRIV)
				mod |= MMAP_COPY;
			ret = GDKmmap(path, mod, size);
			if (ret != NULL) {
				/* success: update allocated size */
				*maxsize = size;
			}
			IODEBUG THRprintf(GDKstdout, "#mmap(NULL, 0, maxsize " SZFMT ", mod %d, path %s, 0) = " PTRFMT "\n", size, mod, path, PTRFMTCAST(void *)ret);
		}
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
 * A BAT created by @%BATnew@ is considered temporary until one calls
 * the routine @%BATsave@. This routine reserves disk space and checks
 * for name clashes.
 *
 * Saving and restoring BATs is left to the upper layers. The library
 * merely copies the data into place.  Failure to read or write the
 * BAT results in a NULL, otherwise it returns the BAT pointer.
 */
static BATstore *
DESCload(int i)
{
	str s, nme = BBP_physical(i);
	BATstore *bs;
	BAT *b = NULL;
	int ht, tt;

	IODEBUG {
		THRprintf(GDKstdout, "#DESCload %s\n", nme ? nme : "<noname>");
	}
	bs = BBP_desc(i);

	if (bs == NULL)
		return 0;
	b = &bs->B;

	ht = b->htype;
	tt = b->ttype;
	if ((ht < 0 && (ht = ATOMindex(s = ATOMunknown_name(ht))) < 0) ||
	    (tt < 0 && (tt = ATOMindex(s = ATOMunknown_name(tt))) < 0)) {
		GDKerror("DESCload: atom '%s' unknown, in BAT '%s'.\n", s, nme);
		return NULL;
	}
	b->htype = ht;
	b->ttype = tt;
	b->H->hash = b->T->hash = NULL;
	/* mil shouldn't mess with just loaded bats */
	if (b->batStamp > 0)
		b->batStamp = -b->batStamp;

	/* reconstruct mode from BBP status (BATmode doesn't flush
	 * descriptor, so loaded mode may be stale) */
	b->batPersistence = (BBP_status(b->batCacheid) & BBPPERSISTENT) ? PERSISTENT : TRANSIENT;
	b->batCopiedtodisk = 1;
	DESCclean(b);
	return bs;
}

#define STORE_MODE(m,r,e) (((m) == STORE_MEM)?STORE_MEM:((r)&&(e))?STORE_PRIV:STORE_MMAP)
int
DESCsetmodes(BAT *b)
{
	int existing = (BBPstatus(b->batCacheid) & BBPEXISTING);
	int brestrict = (b->batRestricted == BAT_WRITE);
	int ret = 0;
	storage_t m;

	if (b->batMaphead) {
		m = STORE_MODE(b->batMaphead, brestrict, existing);
		ret |= m != b->H->heap.newstorage || m != b->H->heap.storage;
		b->H->heap.newstorage = b->H->heap.storage = m;
	}
	if (b->batMaptail) {
		m = STORE_MODE(b->batMaptail, brestrict, existing);
		ret |= b->T->heap.newstorage != m || b->T->heap.storage != m;
		b->T->heap.newstorage = b->T->heap.storage = m;
	}
	if (b->H->vheap && b->batMaphheap) {
		int hrestrict = (b->batRestricted == BAT_APPEND) && ATOMappendpriv(b->htype, b->H->vheap);
		m = STORE_MODE(b->batMaphheap, brestrict || hrestrict, existing);
		ret |= b->H->vheap->newstorage != m || b->H->vheap->storage != m;
		b->H->vheap->newstorage = b->H->vheap->storage = m;
	}
	if (b->T->vheap && b->batMaptheap) {
		int trestrict = (b->batRestricted == BAT_APPEND) && ATOMappendpriv(b->ttype, b->T->vheap);
		m = STORE_MODE(b->batMaptheap, brestrict || trestrict, existing);
		ret |= b->T->vheap->newstorage != m || b->T->vheap->storage != m;
		b->T->vheap->newstorage = b->T->vheap->storage = m;
	}
	return ret;
}

void
DESCclean(BAT *b)
{
	b->batDirtyflushed = DELTAdirty(b) ? TRUE : FALSE;
	b->batDirty = 0;
	b->batDirtydesc = 0;
	b->H->heap.dirty = 0;
	b->T->heap.dirty = 0;
	if (b->H->vheap)
		b->H->vheap->dirty = 0;
	if (b->T->vheap)
		b->T->vheap->dirty = 0;
}

BAT *
BATsave(BAT *bd)
{
	int err = 0;
	char *nme;
	BATstore bs;
	BAT *b = bd;

	BATcheck(b, "BATsave");

	/* views cannot be saved, but make an exception for
	 * force-remapped views */
	if (isVIEW(b) &&
	    !(b->H->heap.copied && b->H->heap.storage == STORE_MMAP) &&
	    !(b->T->heap.copied && b->T->heap.storage == STORE_MMAP)) {
		GDKerror("BATsave: %s is a view on %s; cannot be saved\n", BATgetId(b), VIEWhparent(b) ? BBPname(VIEWhparent(b)) : BBPname(VIEWtparent(b)));
		return NULL;
	}
	if (!BATdirty(b)) {
		return b;
	}
	if (b->batCacheid < 0) {
		b = BATmirror(b);
	}
	if (!DELTAdirty(b))
		ALIGNcommit(b);
	if (!b->halign)
		b->halign = OIDnew(1);
	if (!b->talign)
		b->talign = OIDnew(1);

	/* copy the descriptor to a local variable in order to let our
	 * messing in the BAT descriptor not affect other threads that
	 * only read it. */
	bs = *BBP_desc(b->batCacheid);
	/* fix up internal pointers */
	b = &bs.BM;		/* first the mirror */
	b->S = &bs.S;
	b->H = &bs.T;
	b->T = &bs.H;
	b = &bs.B;		/* then the unmirrored version */
	b->S = &bs.S;
	b->H = &bs.H;
	b->T = &bs.T;

	if (b->H->vheap) {
		b->H->vheap = (Heap *) GDKmalloc(sizeof(Heap));
		if (b->H->vheap == NULL)
			return NULL;
		*b->H->vheap = *bd->H->vheap;
	}
	if (b->T->vheap) {
		b->T->vheap = (Heap *) GDKmalloc(sizeof(Heap));
		if (b->T->vheap == NULL) {
			if (b->H->vheap)
				GDKfree(b->H->vheap);
			return NULL;
		}
		*b->T->vheap = *bd->T->vheap;
	}

	/* start saving data */
	nme = BBP_physical(b->batCacheid);
	if (b->batCopiedtodisk == 0 || b->batDirty || b->H->heap.dirty)
		if (err == 0 && b->htype)
			err = HEAPsave(&b->H->heap, nme, "head");
	if (b->batCopiedtodisk == 0 || b->batDirty || b->T->heap.dirty)
		if (err == 0 && b->ttype)
			err = HEAPsave(&b->T->heap, nme, "tail");
	if (b->H->vheap && (b->batCopiedtodisk == 0 || b->batDirty || b->H->vheap->dirty))
		if (b->htype && b->hvarsized) {
			if (err == 0)
				err = HEAPsave(b->H->vheap, nme, "hheap");
		}
	if (b->T->vheap && (b->batCopiedtodisk == 0 || b->batDirty || b->T->vheap->dirty))
		if (b->ttype && b->tvarsized) {
			if (err == 0)
				err = HEAPsave(b->T->vheap, nme, "theap");
		}

	if (b->H->vheap)
		GDKfree(b->H->vheap);
	if (b->T->vheap)
		GDKfree(b->T->vheap);

	if (err == 0) {
		bd->batCopiedtodisk = 1;
		DESCclean(bd);
		if (bd->htype && bd->H->heap.storage == STORE_MMAP) {
			HEAPshrink(&bd->H->heap, bd->H->heap.free);
			if (bd->batCapacity > bd->H->heap.size >> bd->H->shift)
				bd->batCapacity = (BUN) (bd->H->heap.size >> bd->H->shift);
		}
		if (bd->ttype && bd->T->heap.storage == STORE_MMAP) {
			HEAPshrink(&bd->T->heap, bd->T->heap.free);
			if (bd->batCapacity > bd->T->heap.size >> bd->T->shift)
				bd->batCapacity = (BUN) (bd->T->heap.size >> bd->T->shift);
		}
		if (bd->H->vheap && bd->H->vheap->storage == STORE_MMAP)
			HEAPshrink(bd->H->vheap, bd->H->vheap->free);
		if (bd->T->vheap && bd->T->vheap->storage == STORE_MMAP)
			HEAPshrink(bd->T->vheap, bd->T->vheap->free);
		return bd;
	}
	return NULL;
}


/*
 * TODO: move to gdk_bbp.mx
 */
BAT *
BATload_intern(bat i, int lock)
{
	bat bid = abs(i);
	str nme = BBP_physical(bid);
	BATstore *bs = DESCload(bid);
	BAT *b;
	int batmapdirty;

	if (bs == NULL) {
		return NULL;
	}
	b = &bs->B;
	batmapdirty = DESCsetmodes(b);

	/* LOAD bun heap */
	if (b->htype != TYPE_void) {
		if (HEAPload(&b->H->heap, nme, "head", b->batRestricted == BAT_READ) < 0) {
			return NULL;
		}
		assert(b->H->heap.size >> b->H->shift <= BUN_MAX);
		b->batCapacity = (BUN) (b->H->heap.size >> b->H->shift);
	} else {
		b->H->heap.base = NULL;
	}
	if (b->ttype != TYPE_void) {
		if (HEAPload(&b->T->heap, nme, "tail", b->batRestricted == BAT_READ) < 0) {
			HEAPfree(&b->H->heap);
			return NULL;
		}
		if (b->htype == TYPE_void) {
			assert(b->T->heap.size >> b->T->shift <= BUN_MAX);
			b->batCapacity = (BUN) (b->T->heap.size >> b->T->shift);
		}
		if (b->batCapacity != (b->T->heap.size >> b->T->shift)) {
			BUN cap = b->batCapacity;
			int h;
			if (cap < (b->T->heap.size >> b->T->shift)) {
				cap = (BUN) (b->T->heap.size >> b->T->shift);
				HEAPDEBUG fprintf(stderr, "#HEAPextend in BATload_inter %s " SZFMT " " SZFMT "\n", b->H->heap.filename, b->H->heap.size, headsize(b, cap));
				h = HEAPextend(&b->H->heap, headsize(b, cap), b->batRestricted == BAT_READ);
				b->batCapacity = cap;
			} else {
				HEAPDEBUG fprintf(stderr, "#HEAPextend in BATload_intern %s " SZFMT " " SZFMT "\n", b->T->heap.filename, b->T->heap.size, tailsize(b, cap));
				h = HEAPextend(&b->T->heap, tailsize(b, cap), b->batRestricted == BAT_READ);
			}
			if (h < 0) {
				HEAPfree(&b->H->heap);
				HEAPfree(&b->T->heap);
				return NULL;
			}
		}
	} else {
		b->T->heap.base = NULL;
	}

	/* LOAD head heap */
	if (ATOMvarsized(b->htype)) {
		if (HEAPload(b->H->vheap, nme, "hheap", b->batRestricted == BAT_READ) < 0) {
			HEAPfree(&b->H->heap);
			HEAPfree(&b->T->heap);
			return NULL;
		}
		if (ATOMstorage(b->htype) == TYPE_str) {
			strCleanHash(b->H->vheap, FALSE);	/* ensure consistency */
		}
	}

	/* LOAD tail heap */
	if (ATOMvarsized(b->ttype)) {
		if (HEAPload(b->T->vheap, nme, "theap", b->batRestricted == BAT_READ) < 0) {
			if (b->H->vheap)
				HEAPfree(b->H->vheap);
			HEAPfree(&b->H->heap);
			HEAPfree(&b->T->heap);
			return NULL;
		}
		if (ATOMstorage(b->ttype) == TYPE_str) {
			strCleanHash(b->T->vheap, FALSE);	/* ensure consistency */
		}
	}

	/* initialize descriptor */
	b->batDirtydesc = FALSE;
	b->H->heap.parentid = b->T->heap.parentid = 0;

	/* load succeeded; register it in BBP */
	BBPcacheit(bs, lock);

	if (!DELTAdirty(b)) {
		ALIGNcommit(b);
	}
	b->batDirtydesc |= batmapdirty;	/* if some heap mode changed, make desc dirty */

	if ((b->batRestricted == BAT_WRITE && (GDKdebug & CHECKMASK)) ||
	    (GDKdebug & PROPMASK)) {
		++b->batSharecnt;
		--b->batSharecnt;
	}
	return (i < 0) ? BATmirror(b) : b;
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
int
BATdelete(BAT *b)
{
	bat bid = abs(b->batCacheid);
	str o = BBP_physical(bid);
	BAT *loaded = BBP_cache(bid);

	if (loaded) {
		b = loaded;
		HASHdestroy(b);
		IMPSdestroy(b);
	}
	assert(!b->H->heap.base || !b->T->heap.base || b->H->heap.base != b->T->heap.base);
	if (b->batCopiedtodisk || (b->H->heap.storage != STORE_MEM)) {
		if (b->htype != TYPE_void &&
		    HEAPdelete(&b->H->heap, o, "head") &&
		    b->batCopiedtodisk)
			IODEBUG THRprintf(GDKstdout, "#BATdelete(%s): bun heap\n", BATgetId(b));
	} else if (b->H->heap.base) {
		HEAPfree(&b->H->heap);
	}
	if (b->batCopiedtodisk || (b->T->heap.storage != STORE_MEM)) {
		if (b->ttype != TYPE_void &&
		    HEAPdelete(&b->T->heap, o, "tail") &&
		    b->batCopiedtodisk)
			IODEBUG THRprintf(GDKstdout, "#BATdelete(%s): bun heap\n", BATgetId(b));
	} else if (b->T->heap.base) {
		HEAPfree(&b->T->heap);
	}
	if (b->H->vheap) {
		assert(b->H->vheap->parentid == bid);
		if (b->batCopiedtodisk || (b->H->vheap->storage != STORE_MEM)) {
			if (HEAPdelete(b->H->vheap, o, "hheap") && b->batCopiedtodisk)
				IODEBUG THRprintf(GDKstdout, "#BATdelete(%s): head heap\n", BATgetId(b));
		} else {
			HEAPfree(b->H->vheap);
		}
	}
	if (b->T->vheap) {
		assert(b->T->vheap->parentid == bid);
		if (b->batCopiedtodisk || (b->T->vheap->storage != STORE_MEM)) {
			if (HEAPdelete(b->T->vheap, o, "theap") && b->batCopiedtodisk)
				IODEBUG THRprintf(GDKstdout, "#BATdelete(%s): tail heap\n", BATgetId(b));
		} else {
			HEAPfree(b->T->vheap);
		}
	}
	b->batCopiedtodisk = FALSE;
	return 0;
}

gdk_return
BATprintcols(stream *s, int argc, BAT *argv[])
{
	int i;
	BUN n, cnt;
	struct colinfo {
		int (*s) (str *, int *, const void *);
		BATiter i;
	} *colinfo;
	char *buf;
	int buflen = 0;
	int len;

	/* error checking */
	for (i = 0; i < argc; i++) {
		if (argv[i] == NULL) {
			GDKerror("BAT missing\n");
			return GDK_FAIL;
		}
		if (!BAThdense(argv[i])) {
			GDKerror("BATs must be dense headed\n");
			return GDK_FAIL;
		}
		if (BATcount(argv[0]) != BATcount(argv[i])) {
			GDKerror("BATs must be the same size\n");
			return GDK_FAIL;
		}
		if (argv[0]->hseqbase != argv[i]->hseqbase) {
			GDKerror("BATs must be aligned\n");
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
		buf = argv[i]->tident;
		mnstr_write(s, buf, 1, strlen(buf));
	}
	mnstr_write(s, "  # name\n", 1, 9);
	mnstr_write(s, "# ", 1, 2);
	for (i = 0; i < argc; i++) {
		if (i > 0)
			mnstr_write(s, "\t", 1, 1);
		buf = ATOMname(argv[i]->ttype);
		mnstr_write(s, buf, 1, strlen(buf));
	}
	mnstr_write(s, "  # type\n", 1, 9);
	mnstr_write(s, "#--------------------------#\n", 1, 29);
	buf = NULL;

	for (n = 0, cnt = BATcount(argv[0]); n < cnt; n++) {
		mnstr_write(s, "[ ", 1, 2);
		for (i = 0; i < argc; i++) {
			len = colinfo[i].s(&buf, &buflen, BUNtail(colinfo[i].i, BUNfirst(argv[i]) + n));
			if (i > 0)
				mnstr_write(s, ",\t", 1, 2);
			mnstr_write(s, buf, 1, len);
		}
		mnstr_write(s, "  ]\n", 1, 4);
	}

	GDKfree(colinfo);

	return GDK_SUCCEED;
}

gdk_return
BATprintf(stream *s, BAT *b)
{
	BAT *argv[2];
	gdk_return ret = GDK_FAIL;

	argv[0] = BATmirror(BATmark(b, 0));
	argv[1] = BATmirror(BATmark(BATmirror(b), 0));
	if (argv[0] && argv[1]) {
		BATroles(argv[0], NULL, b->hident);
		BATroles(argv[1], NULL, b->tident);
		ret = BATprintcols(s, 2, argv);
	}
	if (argv[0])
		BBPunfix(argv[0]->batCacheid);
	if (argv[1])
		BBPunfix(argv[1]->batCacheid);
	return ret;
}

gdk_return
BATprint(BAT *b)
{
	return BATprintf(GDKstdout, b);
}

gdk_return
BATmultiprintf(stream *s, int argc, BAT *argv[], int printhead, int order, int printorder)
{
	BAT **bats;
	gdk_return ret;
	int i;

	(void) printorder;
	assert(argc >= 2);
	assert(order < argc);
	assert(order >= 0);
	argc--;
	if ((bats = GDKzalloc((argc + 1) * sizeof(BAT *))) == NULL)
		return GDK_FAIL;
	if ((bats[0] = BATmirror(BATmark(argv[order > 0 ? order - 1 : 0], 0))) == NULL)
		goto bailout;
	if ((bats[1] = BATmirror(BATmark(BATmirror(argv[0]), 0))) == NULL)
		goto bailout;
	for (i = 1; i < argc; i++) {
		BAT *a, *b, *r, *t;
		int j;

		if ((r = BATmirror(BATmark(argv[i], 0))) == NULL)
			goto bailout;
		ret = BATsubleftjoin(&a, &b, bats[0], r, NULL, NULL, 0, BUN_NONE);
		BBPunfix(r->batCacheid);
		if (ret == GDK_FAIL)
			goto bailout;
		if ((t = BATproject(a, bats[0])) == NULL) {
			BBPunfix(a->batCacheid);
			BBPunfix(b->batCacheid);
			goto bailout;
		}
		BBPunfix(bats[0]->batCacheid);
		bats[0] = t;
		for (j = 1; j <= i; j++) {
			if ((t = BATproject(a, bats[j])) == NULL) {
				BBPunfix(a->batCacheid);
				BBPunfix(b->batCacheid);
				goto bailout;
			}
			BBPunfix(bats[j]->batCacheid);
			bats[j] = t;
		}
		BBPunfix(a->batCacheid);
		if ((r = BATmirror(BATmark(BATmirror(argv[i]), 0))) == NULL) {
			BBPunfix(b->batCacheid);
			goto bailout;
		}
		t = BATproject(b, r);
		BBPunfix(b->batCacheid);
		BBPunfix(r->batCacheid);
		if (t == NULL)
			goto bailout;
		bats[i + 1] = t;
	}
	BATroles(bats[0], NULL, argv[order > 0 ? order - 1 : 0]->hident);
	for (i = 1; i <= argc; i++)
		BATroles(bats[i], NULL, argv[i - 1]->tident);
	ret = BATprintcols(s, argc + printhead, bats + !printhead);
	for (i = 0; i <= argc; i++)
		BBPunfix(bats[i]->batCacheid);
	GDKfree(bats);
	return ret;

  bailout:
	for (i = 0; i <= argc; i++) {
		if (bats[i])
			BBPunfix(bats[i]->batCacheid);
	}
	GDKfree(bats);
	return GDK_FAIL;
}
