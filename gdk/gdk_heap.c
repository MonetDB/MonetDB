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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f gdk_heap
 * @a Peter Boncz, Wilko Quak
 * @+ Atom Heaps
 * Heaps are the basic mass storage structure of Monet. A heap is a
 * handle to a large, possibly huge, contiguous area of main memory,
 * that can be allocated in various ways (discriminated by the
 * heap->storage field):
 *
 * @table @code
 * @item STORE_MEM: malloc-ed memory
 * small (or rather: not huge) heaps are allocated with GDKmalloc.
 * Notice that GDKmalloc may redirect big requests to anonymous
 * virtual memory to prevent @emph{memory fragmentation} in the malloc
 * library (see gdk_utils.mx).
 *
 * @item STORE_MMAP: read-only mapped region
 * this is a file on disk that is mapped into virtual memory.  This is
 * normally done MAP_SHARED, so we can use msync() to commit dirty
 * data using the OS virtual memory management.
 *
 * @item STORE_PRIV: read-write mapped region
 * in order to preserve ACID properties, we use a different memory
 * mapping on virtual memory that is writable. This is because in case
 * of a crash on a dirty STORE_MMAP heap, the OS may have written some
 * of the dirty pages to disk and other not (but it is impossible to
 * determine which).  The OS MAP_PRIVATE mode does not modify the file
 * on which is being mapped, rather creates substitute pages
 * dynamically taken from the swap file when modifications occur. This
 * is the only way to make writing to mmap()-ed regions safe.  To save
 * changes, we created a new file X.new; as some OS-es do not allow to
 * write into a file that has a mmap open on it (e.g. Windows).  Such
 * X.new files take preference over X files when opening them.
 * @end table
 * Read also the discussion in BATsetaccess (gdk_bat.mx).
 *
 * Todo: check DESCsetmodes/HEAPcheckmode (gdk_storage.mx).
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

static void *
HEAPcreatefile(size_t *maxsz, char *fn, storage_t mode)
{
	size_t size = *maxsz;
	void *base = NULL;
	int fd;

	size = (*maxsz + (size_t) 0xFFFF) & ~ (size_t) 0xFFFF; /* round up to 64k */
	fd = GDKfdlocate(fn, "wb", NULL);
	if (fd >= 0) {
		close(fd);
		base = GDKload(fn, NULL, size, size, mode);
		if (base)
			*maxsz = size;
	}
	return base;
}

static int HEAPload_intern(Heap *h, const char *nme, const char *ext, const char *suffix, int trunc);
static int HEAPsave_intern(Heap *h, const char *nme, const char *ext, const char *suffix);

static char *
decompose_filename(str nme)
{
	char *ext;

	ext = strchr(nme, '.');	/* extract base and ext from heap file name */
	if (ext) {
		*ext++ = 0;
	}
	return ext;
}

/*
 * @- HEAPalloc
 *
 * Normally, we use GDKmalloc for creating a new heap.  Huge heaps,
 * though, come from memory mapped files that we create with a large
 * seek. This is fast, and leads to files-with-holes on Unixes (on
 * Windows, it actually always performs I/O which is not nice).
 */
int
HEAPalloc(Heap *h, size_t nitems, size_t itemsize)
{
	char nme[PATHLENGTH];
	size_t minsize = GDK_mmap_minsize;
	struct stat st;

	h->base = NULL;
	h->size = 1;
	h->copied = 0;
	if (itemsize)
		h->size = MAX(1, nitems) * itemsize;
	h->free = 0;

	/* check for overflow */
	if (itemsize && nitems > (h->size / itemsize))
		return -1;

	if (h->filename) {
		GDKfilepath(nme, BATDIR, h->filename, NULL);
		/* if we're going to use mmap anyway (size >=
		 * GDK_mem_bigsize -- see GDKmallocmax), and the file
		 * we want to use already exists and is large enough
		 * for the size we want, force non-anonymous mmap */
		if (h->size >= GDK_mem_bigsize &&
		    stat(nme, &st) == 0 && st.st_size >= (off_t) h->size) {
			minsize = GDK_mem_bigsize; /* force mmap */
		}
	}

	if (h->filename == NULL || (h->size < minsize)) {
		h->storage = STORE_MEM;
		h->base = (char *) GDKmallocmax(h->size, &h->size, 0);
		HEAPDEBUG fprintf(stderr, "#HEAPalloc " SZFMT " " PTRFMT "\n", h->size, PTRFMTCAST h->base);
	}
	if (h->filename && h->base == NULL) {
		char *of = h->filename;

		h->filename = NULL;

		if (stat(nme, &st) != 0) {
			h->storage = STORE_MMAP;
			h->base = HEAPcreatefile(&h->size, of, h->storage);
			h->filename = of;
		} else {
			char *ext;
			int fd;

			strncpy(nme, of, sizeof(nme));
			nme[sizeof(nme) - 1] = 0;
			ext = decompose_filename(nme);
			fd = GDKfdlocate(nme, "wb", ext);
			if (fd >= 0) {
				close(fd);
				h->newstorage = STORE_MMAP;
				HEAPload(h, nme, ext, FALSE);
			}
			GDKfree(of);
		}
	}
	if (h->base == NULL) {
		GDKerror("HEAPalloc: Insufficient space for HEAP of " SZFMT " bytes.", h->size);
		return -1;
	}
	h->newstorage = h->storage;
	return 0;
}

/*
 * @- HEAPextend
 *
 * Normally (last case in the below code), we use GDKrealloc, except
 * for the case that the heap extends to a huge size, in which case we
 * open memory mapped file.
 *
 * Observe that we may assume that the BAT is writable here
 * (otherwise, why extend?).
 *
 * For memory mapped files, we may try to extend the file after the
 * end, and also extend the VM space we already have. This may fail,
 * e.g. due to VM fragmentation or no swap space (we map the new
 * segment STORE_PRIV). Also, some OS-es might not support this at all
 * (NOEXTEND_PRIVMAP).
 *
 * The other way is to just save the mmap-ed heap, free it and reload
 * it.
 */
int
HEAPextend(Heap *h, size_t size)
{
	char nme[PATHLENGTH], *ext = NULL;
	char *failure = "None";

	if (h->filename) {
		strncpy(nme, h->filename, sizeof(nme));
		nme[sizeof(nme) - 1] = 0;
		ext = decompose_filename(nme);
	}
	if (size <= h->size)
		return 0;
	else
		failure = "size > h->size";

 	if (h->storage != STORE_MEM) {
		char *p;
		long_str path;

		HEAPDEBUG fprintf(stderr, "#HEAPextend: extending %s mmapped heap (%s)\n", h->storage == STORE_MMAP ? "shared" : "privately", h->filename);
		/* extend memory mapped file */
		GDKfilepath(path, BATDIR, nme, ext);
		size = (1 + ((size - 1) >> REMAP_PAGE_MAXBITS)) << REMAP_PAGE_MAXBITS;
		p = MT_mremap(path,
			      h->storage == STORE_PRIV ?
				MMAP_COPY | MMAP_READ | MMAP_WRITE :
				MMAP_READ | MMAP_WRITE,
			      h->base, h->size, &size);
		if (p) {
			h->size = size;
			h->base = p;
 			return 0;
 		} else {
 			failure = "MT_mremap() failed";
 		}
	} else {
		/* extend a malloced heap, possibly switching over to
		 * file-mapped storage */
		Heap bak = *h;
		size_t cur = GDKmem_cursize(), tot = GDK_mem_maxsize;
		int exceeds_swap = size > (tot + tot - MIN(tot + tot, cur));
		int can_mmap = h->filename && (size >= GDK_mem_bigsize || h->newstorage != STORE_MEM);
		int small_cpy = (h->size * 4 < size) && (size >= GDK_mmap_minsize);
		/* the last condition is to use explicit MMAP instead
		 * of anonymous MMAP in GDKmalloc */
		int must_mmap = can_mmap && (small_cpy || exceeds_swap || h->newstorage != STORE_MEM || size >= GDK_mem_bigsize);

		h->size = size;

		/* try GDKrealloc if the heap size stays within
		 * reasonable limits */
		if (!must_mmap) {
			void *p = h->base;
			h->newstorage = h->storage = STORE_MEM;
			h->base = GDKreallocmax(h->base, size, &h->size, 0);
			HEAPDEBUG fprintf(stderr, "#HEAPextend: extending malloced heap " SZFMT " " SZFMT " " PTRFMT " " PTRFMT "\n", size, h->size, PTRFMTCAST p, PTRFMTCAST h->base);
			if (h->base)
				return 0;
			else
				failure = "h->storage == STORE_MEM && !must_map && !h->base";
		}
		/* too big: convert it to a disk-based temporary heap */
		if (can_mmap) {
			int fd;
			int existing = 0;

			assert(h->storage == STORE_MEM);
			h->filename = NULL;
			/* if the heap file already exists, we want to
			 * switch to STORE_PRIV (copy-on-write memory
			 * mapped files), but if the heap file doesn't
			 * exist yet, the BAT is new and we can use
			 * STORE_MMAP */
			fd = GDKfdlocate(nme, "rb", ext);
			if (fd >= 0) {
				existing = 1;
				close(fd);
			} else {
				/* no pre-existing heap file, attempt
				 * to use a file from the cache (or
				 * create a new one) */
				h->filename = GDKmalloc(strlen(nme) + strlen(ext) + 2);
				if (h->filename == NULL) {
					failure = "h->storage == STORE_MEM && can_map && h->filename == NULL";
					goto failed;
				}
				sprintf(h->filename, "%s.%s", nme, ext);
				h->base = HEAPcreatefile(&h->size, h->filename, STORE_MMAP);
				if (h->base) {
					h->newstorage = h->storage = STORE_MMAP;
					memcpy(h->base, bak.base, bak.free);
					HEAPfree(&bak);
					return 0;
				} else {
					failure = "h->storage == STORE_MEM && can_map && !h->base";
				}
			}
			fd = GDKfdlocate(nme, "wb", ext);
			if (fd >= 0) {
				close(fd);
				h->storage = h->newstorage == STORE_MMAP && existing && !h->forcemap ? STORE_PRIV : h->newstorage;
				/* make sure we really MMAP */
				if (must_mmap && h->newstorage == STORE_MEM)
					h->storage = STORE_MMAP;
				h->newstorage = h->storage;
				h->forcemap = 0;

				h->base = NULL;
				HEAPDEBUG fprintf(stderr, "#HEAPextend: converting malloced to %s mmapped heap\n", h->newstorage == STORE_MMAP ? "shared" : "privately");
				/* try to allocate a memory-mapped
				 * based heap */
				if (HEAPload(h, nme, ext, FALSE) >= 0) {
					/* copy data to heap and free
					 * old memory */
					memcpy(h->base, bak.base, bak.free);
					HEAPfree(&bak);
					return 0;
				} else {
					failure = "h->storage == STORE_MEM && can_map && fd >= 0 && HEAPload() < 0";
				}
				/* couldn't allocate, now first save
				 * data to file */
				if (HEAPsave_intern(&bak, nme, ext, ".tmp") < 0) {
					failure = "h->storage == STORE_MEM && can_map && fd >= 0 && HEAPsave_intern() < 0";
					goto failed;
				}
				/* then free memory */
				HEAPfree(&bak);
				/* and load heap back in via
				 * memory-mapped file */
				if (HEAPload_intern(h, nme, ext, ".tmp", FALSE) >= 0) {
					/* success! */
					GDKclrerr();	/* don't leak errors from e.g. HEAPload */
					return 0;
				} else {
					failure = "h->storage == STORE_MEM && can_map && fd >= 0 && HEAPload_intern() < 0";
				}
				/* we failed */
			} else {
				failure = "h->storage == STORE_MEM && can_map && fd < 0";
			}
		} else {
			failure = "h->storage == STORE_MEM && !can_map";
		}
	  failed:
		*h = bak;
	}
	GDKerror("HEAPextend: failed to extend to " SZFMT " for %s%s%s: %s\n",
		 size, nme, ext ? "." : "", ext ? ext : "", failure);
	return -1;
}

int
HEAPshrink(Heap *h, size_t size)
{
	char *p = NULL;

	assert(size >= h->free);
	assert(size <= h->size);
	if (h->storage == STORE_MEM) {
		p = GDKreallocmax(h->base, size, &size, 0);
		HEAPDEBUG fprintf(stderr, "#HEAPshrink: shrinking malloced "
				  "heap " SZFMT " " SZFMT " " PTRFMT " "
				  PTRFMT "\n", h->size, size,
				  PTRFMTCAST h->base, PTRFMTCAST p);
	} else {
		char nme[PATHLENGTH], *ext = NULL;
		long_str path;

		if (h->filename) {
			strncpy(nme, h->filename, sizeof(nme));
			nme[sizeof(nme) - 1] = 0;
			ext = decompose_filename(nme);
		}
		/* shrink memory mapped file */
		GDKfilepath(path, BATDIR, nme, ext);
		size = MAX(size, MT_pagesize()); /* at least one page */
		size = (size + MT_pagesize() - 1) & ~(MT_pagesize() - 1);
		if (size >= h->size) {
			/* don't grow */
			return 0;
		}
		p = MT_mremap(path,
			      h->storage == STORE_PRIV ?
				MMAP_COPY | MMAP_READ | MMAP_WRITE :
				MMAP_READ | MMAP_WRITE,
			      h->base, h->size, &size);
		HEAPDEBUG fprintf(stderr, "#HEAPshrink: shrinking %s mmapped "
				  "heap (%s) " SZFMT " " SZFMT " " PTRFMT " "
				  PTRFMT "\n",
				  h->storage == STORE_MMAP ? "shared" : "privately",
				  h->filename, h->size, size,
				  PTRFMTCAST h->base, PTRFMTCAST p);
	}
	if (p) {
		h->size = size;
		h->base = p;
		return 0;
	}
	return -1;
}

/* returns 1 if the file exists */
static int
file_exists(const char *dir, const char *name, const char *ext)
{
	long_str path;
	struct stat st;
	int ret;

	GDKfilepath(path, dir, name, ext);
	ret = stat(path, &st);
	IODEBUG THRprintf(GDKstdout, "#stat(%s) = %d\n", path, ret);
	return (ret == 0);
}

int
GDKupgradevarheap(COLrec *c, var_t v, int copyall)
{
	bte shift = c->shift;
	unsigned short width = c->width;
	unsigned char *pc;
	unsigned short *ps;
	unsigned int *pi;
#if SIZEOF_VAR_T == 8
	var_t *pv;
#endif
	size_t i, n;
	size_t savefree;
	const char *filename;
	bat bid;

	assert(c->heap.parentid == 0);
	assert(width != 0);
	assert(v >= GDK_VAROFFSET);
	assert(width < SIZEOF_VAR_T && (width <= 2 ? v - GDK_VAROFFSET : v) >= ((var_t) 1 << (8 * width)));
	while (width < SIZEOF_VAR_T && (width <= 2 ? v - GDK_VAROFFSET : v) >= ((var_t) 1 << (8 * width))) {
		width <<= 1;
		shift++;
	}
	assert(c->width < width);
	assert(c->shift < shift);

	/* if copyall is set, we need to convert the whole heap, since
	 * we may be in the middle of an insert loop that adjusts the
	 * free value at the end; otherwise only copy the area
	 * indicated by the "free" pointer */
	n = (copyall ? c->heap.size : c->heap.free) >> c->shift;

	/* for memory mapped files, create a backup copy before widening
	 *
	 * this solves a problem that we don't control what's in the
	 * actual file until the next commit happens, so a crash might
	 * otherwise leave the file (and the database) in an
	 * inconsistent state
	 *
	 * also see do_backup in gdk_bbp.c */
	filename = strrchr(c->heap.filename, DIR_SEP);
	if (filename == NULL)
		filename = c->heap.filename;
	else
		filename++;
	bid = strtol(filename, NULL, 8);
	if (c->heap.storage == STORE_MMAP &&
	    (BBP_status(bid) & (BBPEXISTING|BBPDELETED)) &&
	    !file_exists(BAKDIR, filename, NULL)) {
		int fd;
		ssize_t ret = 0;
		size_t size = n << c->shift;
		const char *base = c->heap.base;

		/* first save heap in file with extra .tmp extension */
		if ((fd = GDKfdlocate(c->heap.filename, "wb", "tmp")) < 0)
			return GDK_FAIL;
		while (size > 0) {
			ret = write(fd, base, (unsigned) MIN(1 << 30, size));
			if (ret < 0)
				size = 0;
			size -= ret;
			base += ret;
		}
		if (ret < 0 ||
#if defined(NATIVE_WIN32)
		    _commit(fd) < 0 ||
#elif defined(HAVE_FDATASYNC)
		    fdatasync(fd) < 0 ||
#elif defined(HAVE_FSYNC)
		    fsync(fd) < 0 ||
#endif
		    close(fd) < 0) {
			/* something went wrong: abandon ship */
			close(fd);
			GDKunlink(BATDIR, c->heap.filename, "tmp");
			return GDK_FAIL;
		}
		/* move tmp file to backup directory (without .tmp
		 * extension) */
		if (GDKmove(BATDIR, c->heap.filename, "tmp", BAKDIR, filename, NULL) < 0) {
			/* backup failed */
			GDKunlink(BATDIR, c->heap.filename, "tmp");
			return GDK_FAIL;
		}
	}

	savefree = c->heap.free;
	if (copyall)
		c->heap.free = c->heap.size;
	if (HEAPextend(&c->heap, (c->heap.size >> c->shift) << shift) < 0)
		return GDK_FAIL;
	if (copyall)
		c->heap.free = savefree;
	/* note, cast binds more closely than addition */
	pc = (unsigned char *) c->heap.base + n;
	ps = (unsigned short *) c->heap.base + n;
	pi = (unsigned int *) c->heap.base + n;
#if SIZEOF_VAR_T == 8
	pv = (var_t *) c->heap.base + n;
#endif

	/* convert from back to front so that we can do it in-place */
	switch (c->width) {
	case 1:
		switch (width) {
		case 2:
			for (i = 0; i < n; i++)
				*--ps = *--pc;
			break;
		case 4:
			for (i = 0; i < n; i++)
				*--pi = *--pc + GDK_VAROFFSET;
			break;
#if SIZEOF_VAR_T == 8
		case 8:
			for (i = 0; i < n; i++)
				*--pv = *--pc + GDK_VAROFFSET;
			break;
#endif
		}
		break;
	case 2:
		switch (width) {
		case 4:
			for (i = 0; i < n; i++)
				*--pi = *--ps + GDK_VAROFFSET;
			break;
#if SIZEOF_VAR_T == 8
		case 8:
			for (i = 0; i < n; i++)
				*--pv = *--ps + GDK_VAROFFSET;
			break;
#endif
		}
		break;
#if SIZEOF_VAR_T == 8
	case 4:
		for (i = 0; i < n; i++)
			*--pv = *--pi;
		break;
#endif
	}
	c->heap.free <<= shift - c->shift;
	c->shift = shift;
	c->width = width;
	return GDK_SUCCEED;
}

/*
 * @- HEAPcopy
 * simple: alloc and copy. Notice that we suppose a preallocated
 * dst->filename (or NULL), which might be used in HEAPalloc().
 */
int
HEAPcopy(Heap *dst, Heap *src)
{
	if (HEAPalloc(dst, src->size, 1) == 0) {
		dst->free = src->free;
		memcpy(dst->base, src->base, src->free);
		dst->hashash = src->hashash;
		return 0;
	}
	return -1;
}

/*
 * @- HEAPfree
 * Is now called even on heaps without memory, just to free the
 * pre-allocated filename.  simple: alloc and copy.
 */
int
HEAPfree(Heap *h)
{
	if (h->base) {
		if (h->storage == STORE_MEM) {	/* plain memory */
			HEAPDEBUG fprintf(stderr, "#HEAPfree " SZFMT
					  " " PTRFMT "\n",
					  h->size, PTRFMTCAST h->base);
			GDKfree(h->base);
		} else {	/* mapped file, or STORE_PRIV */
			int ret = GDKmunmap(h->base, h->size);

			if (ret < 0) {
				GDKsyserror("HEAPfree: %s was not mapped\n",
					    h->filename);
				assert(0);
			}
			HEAPDEBUG fprintf(stderr, "#munmap(base=" PTRFMT ", "
					  "size=" SZFMT ") = %d\n",
					  PTRFMTCAST(void *)h->base,
					  h->size, ret);
		}
	}
	h->base = NULL;
	if (h->filename) {
		GDKfree(h->filename);
		h->filename = NULL;
	}
	return 0;
}

/*
 * @- HEAPload
 *
 * If we find file X.new, we move it over X (if present) and open it.
 *
 * This routine initializes the h->filename without deallocating its
 * previous contents.
 */
static int
HEAPload_intern(Heap *h, const char *nme, const char *ext, const char *suffix, int trunc)
{
	size_t truncsize = (1 + (((size_t) (h->free * 1.05)) >> REMAP_PAGE_MAXBITS)) << REMAP_PAGE_MAXBITS;
	size_t minsize = (1 + ((h->size - 1) >> REMAP_PAGE_MAXBITS)) << REMAP_PAGE_MAXBITS;
	int ret = 0, desc_status = 0;
	long_str srcpath, dstpath;
	struct stat st;

	h->storage = h->newstorage;
	if (h->filename == NULL)
		h->filename = (char *) GDKmalloc(strlen(nme) + strlen(ext) + 2);
	if (h->filename == NULL)
		return -1;
	sprintf(h->filename, "%s.%s", nme, ext);

	/* round up mmap heap sizes to REMAP_PAGE_MAXSIZE (usually
	 * 512KB) segments */
	if (h->storage != STORE_MEM && minsize != h->size)
		h->size = minsize;

	/* when a bat is made read-only, we can truncate any unused
	 * space at the end of the heap */
	if (trunc && truncsize < h->size) {
		int fd = GDKfdlocate(nme, "mrb+", ext);
		if (fd >= 0) {
			ret = ftruncate(fd, (off_t) truncsize);
			HEAPDEBUG fprintf(stderr, "#ftruncate(file=%s.%s, size=" SZFMT ") = %d\n", nme, ext, truncsize, ret);
			close(fd);
			if (ret == 0) {
				h->size = truncsize;
				desc_status = 1;
			}
		}
	}

	HEAPDEBUG {
		fprintf(stderr, "#HEAPload(%s.%s,storage=%d,free=" SZFMT ",size=" SZFMT ")\n", nme, ext, (int) h->storage, h->free, h->size);
	}
	/* On some OSs (WIN32,Solaris), it is prohibited to write to a
	 * file that is open in MAP_PRIVATE (FILE_MAP_COPY) solution:
	 * we write to a file named .ext.new.  This file, if present,
	 * takes precedence. */
	GDKfilepath(srcpath, BATDIR, nme, ext);
	GDKfilepath(dstpath, BATDIR, nme, ext);
	assert(strlen(srcpath) + strlen(suffix) < sizeof(srcpath));
	strcat(srcpath, suffix);
	ret = stat(dstpath, &st);
	if (stat(srcpath, &st) == 0) {
		int t0;
		if (ret == 0) {
			t0 = GDKms();
			ret = unlink(dstpath);
			HEAPDEBUG fprintf(stderr, "#unlink %s = %d (%dms)\n", dstpath, ret, GDKms() - t0);
		}
		t0 = GDKms();
		ret = rename(srcpath, dstpath);
		if (ret < 0) {
			GDKsyserror("HEAPload: rename of %s failed\n", srcpath);
			return -1;
		}
		HEAPDEBUG fprintf(stderr, "#rename %s %s = %d (%dms)\n", srcpath, dstpath, ret, GDKms() - t0);
	}

	h->base = (char *) GDKload(nme, ext, h->free, h->size, h->newstorage);
	if (h->base == NULL)
		return -1;	/* file could  not be read satisfactorily */

	return desc_status;
}

int
HEAPload(Heap *h, const char *nme, const char *ext, int trunc)
{
	return HEAPload_intern(h, nme, ext, ".new", trunc);
}

/*
 * @- HEAPsave
 *
 * Saving STORE_MEM will do a write(fd, buf, size) in GDKsave
 * (explicit IO).
 *
 * Saving a STORE_PRIV heap X means that we must actually write to
 * X.new, thus we convert the mode passed to GDKsave to STORE_MEM.
 *
 * Saving STORE_MMAP will do a msync(buf, MSSYNC) in GDKsave (implicit
 * IO).
 *
 * After GDKsave returns successfully (>=0), we assume the heaps are
 * safe on stable storage.
 */
static int
HEAPsave_intern(Heap *h, const char *nme, const char *ext, const char *suffix)
{
	storage_t store = h->newstorage;
	long_str extension;

	if (h->base == NULL) {
		return -1;
	}
	if (h->storage != STORE_MEM && store == STORE_PRIV) {
		/* anonymous or private VM is saved as if it were malloced */
		store = STORE_MEM;
		assert(strlen(ext) + strlen(suffix) < sizeof(extension));
		snprintf(extension, sizeof(extension), "%s%s", ext, suffix);
		ext = extension;
	} else if (store != STORE_MEM) {
		store = h->storage;
	}
	HEAPDEBUG {
		fprintf(stderr, "#HEAPsave(%s.%s,storage=%d,free=" SZFMT ",size=" SZFMT ")\n", nme, ext, (int) h->newstorage, h->free, h->size);
	}
	return GDKsave(nme, ext, h->base, h->free, store);
}

int
HEAPsave(Heap *h, const char *nme, const char *ext)
{
	return HEAPsave_intern(h, nme, ext, ".new");
}

/*
 * @- HEAPdelete
 * Delete any saved heap file. For memory mapped files, also try to
 * remove any remaining X.new
 */
int
HEAPdelete(Heap *h, const char *o, const char *ext)
{
	char ext2[64];

	if (h->size <= 0) {
		assert(h->base == 0);
		return 0;
	}
	if (h->base)
		HEAPfree(h);
	if (h->copied) {
		return 0;
	}
	assert(strlen(ext) + strlen(".new") < sizeof(ext2));
	snprintf(ext2, sizeof(ext2), "%s%s", ext, ".new");
	return (GDKunlink(BATDIR, o, ext) == 0) | (GDKunlink(BATDIR, o, ext2) == 0) ? 0 : -1;
}

int
HEAPwarm(Heap *h)
{
	int bogus_result = 0;

	if (h->storage != STORE_MEM) {
		/* touch the heap sequentially */
		int *cur = (int *) h->base;
		int *lim = (int *) (h->base + h->free) - 4096;

		for (; cur < lim; cur += 4096)	/* try to schedule 4 parallel memory accesses */
			bogus_result += cur[0] + cur[1024] + cur[2048] + cur[3072];
	}
	return bogus_result;
}


/*
 * @- HEAPvmsize
 * count all memory that takes up address space.
 */
size_t
HEAPvmsize(Heap *h)
{
	if (h && h->free)
		return h->size;
	return 0;
}

/*
 * @- HEAPmemsize
 * count all memory that takes up swap space. We conservatively count
 * STORE_PRIV heaps as fully backed by swap space.
 */
size_t
HEAPmemsize(Heap *h)
{
	if (h && h->free && h->storage != STORE_MMAP)
		return h->size;
	return 0;
}


/*
 * @+ Standard Heap Library
 * This library contains some routines which implement a @emph{
 * malloc} and @emph{ free} function on the Monet @emph{Heap}
 * structure. They are useful when implementing a new @emph{
 * variable-size} atomic data type, or for implementing new search
 * accelerators.  All functions start with the prefix @emph{HEAP_}. T
 *
 * Due to non-careful design, the HEADER field was found to be
 * 32/64-bit dependent.  As we do not (yet) want to change the BAT
 * image on disk, This is now fixed by switching on-the-fly between
 * two representations. We ensure that the 64-bit memory
 * representation is just as long as the 32-bits version (20 bytes) so
 * the rest of the heap never needs to shift. The function
 * HEAP_checkformat converts at load time dynamically between the
 * layout found on disk and the memory format.  Recognition of the
 * header mode is done by looking at the first two ints: alignment
 * must be 4 or 8, and head can never be 4 or eight.
 *
 * TODO: user HEADER64 for both 32 and 64 bits (requires BAT format
 * change)
 */
/* #define DEBUG */
/* #define TRACE */

#define HEAPVERSION	20030408

typedef struct heapheader {
	size_t head;		/* index to first free block            */
	int alignment;		/* alignment of objects on heap         */
	size_t firstblock;	/* first block in heap                  */
	int version;
	int (*sizefcn)(const void *);	/* ADT function to ask length           */
} HEADER32;

typedef struct {
	int version;
	int alignment;
	size_t head;
	size_t firstblock;
	int (*sizefcn)(const void *);
} HEADER64;

#if SIZEOF_SIZE_T==8
typedef HEADER64 HEADER;
typedef HEADER32 HEADER_OTHER;
#else
typedef HEADER32 HEADER;
typedef HEADER64 HEADER_OTHER;
#endif
typedef struct hfblock {
	size_t size;		/* Size of this block in freelist        */
	size_t next;		/* index of next block                   */
} CHUNK;

#define roundup_8(x)	(((x)+7)&~7)
#define roundup_4(x)	(((x)+3)&~3)
#define blocksize(h,p)	((p)->size)

static inline size_t
roundup_num(size_t number, int alignment)
{
	size_t rval;

	rval = number + (size_t) alignment - 1;
	rval -= (rval % (size_t) alignment);
	return rval;
}

#ifdef TRACE
static void
HEAP_printstatus(Heap *heap)
{
	HEADER *hheader = HEAP_index(heap, 0, HEADER);
	size_t block, cur_free = hheader->head;
	CHUNK *blockp;

	THRprintf(GDKstdout,
		  "#HEAP has head " SZFMT " and alignment %d and size " SZFMT "\n",
		  hheader->head, hheader->alignment, heap->free);

	/* Walk the blocklist */
	block = hheader->firstblock;

	while (block < heap->free) {
		blockp = HEAP_index(heap, block, CHUNK);

		if (block == cur_free) {
			THRprintf(GDKstdout,
				  "#   free block at " PTRFMT " has size " SZFMT " and next " SZFMT "\n",
				  PTRFMTCAST(void *)block,
				  blockp->size, blockp->next);

			cur_free = blockp->next;
			block += blockp->size;
		} else {
			size_t size = blocksize(hheader, blockp);

			THRprintf(GDKstdout,
				  "#   block at " SZFMT " with size " SZFMT "\n",
				  block, size);
			block += size;
		}
	}
}
#endif /* TRACE */

static void
HEAP_empty(Heap *heap, size_t nprivate, int alignment)
{
	/* Find position of header block. */
	HEADER *hheader = HEAP_index(heap, 0, HEADER);

	/* Calculate position of first and only free block. */
	size_t head = roundup_num((size_t) (roundup_8(sizeof(HEADER)) + roundup_8(nprivate)), alignment);
	CHUNK *headp = HEAP_index(heap, head, CHUNK);

	assert(roundup_8(sizeof(HEADER)) + roundup_8(nprivate) <= VAR_MAX);

	/* Fill header block. */
	hheader->head = head;
	hheader->sizefcn = NULL;
	hheader->alignment = alignment;
	hheader->firstblock = head;
	hheader->version = HEAPVERSION;

	/* Fill first free block. */
	assert(heap->size - head <= VAR_MAX);
	headp->size = (size_t) (heap->size - head);
	headp->next = 0;
#ifdef TRACE
	THRprintf(GDKstdout, "#We created the following heap\n");
	HEAP_printstatus(heap);
#endif
}

void
HEAP_initialize(Heap *heap, size_t nbytes, size_t nprivate, int alignment)
{
	/* For now we know about two alignments. */
	if (alignment != 8) {
		alignment = 4;
	}
	if ((size_t) alignment < sizeof(size_t))
		alignment = (int) sizeof(size_t);

	/* Calculate number of bytes needed for heap + structures. */
	{
		size_t total = 100 + nbytes + nprivate + sizeof(HEADER) + sizeof(CHUNK);

		total = roundup_8(total);
		if (HEAPalloc(heap, total, 1) < 0)
			return;
		heap->free = heap->size;
	}

	/* initialize heap as empty */
	HEAP_empty(heap, nprivate, alignment);
}


var_t
HEAP_malloc(Heap *heap, size_t nbytes)
{
	size_t block, trail, ttrail;
	CHUNK *blockp;
	CHUNK *trailp;
	HEADER *hheader = HEAP_index(heap, 0, HEADER);

#ifdef TRACE
	THRprintf(GDKstdout, "#Enter malloc with " SZFMT " bytes\n", nbytes);
#endif

	/* add space for size field */
	nbytes += hheader->alignment;
	nbytes = roundup_8(nbytes);
	if (nbytes < sizeof(CHUNK))
		nbytes = (size_t) sizeof(CHUNK);

	/* block  -- points to block with acceptable size (if available).
	 * trail  -- points to predecessor of block.
	 * ttrail -- points to predecessor of trail.
	 */
	ttrail = 0;
	trail = 0;
	for (block = hheader->head; block != 0; block = HEAP_index(heap, block, CHUNK)->next) {
		blockp = HEAP_index(heap, block, CHUNK);

#ifdef TRACE
		THRprintf(GDKstdout, "#block " SZFMT " is " SZFMT " bytes\n", block, blockp->size);
#endif
		if ((trail != 0) && (block <= trail))
			GDKfatal("HEAP_malloc: Free list is not orderered\n");

		if (blockp->size >= nbytes)
			break;
		ttrail = trail;
		trail = block;
	}

	/* If no block of acceptable size is found we try to enlarge
	 * the heap. */
	if (block == 0) {
		size_t newsize;

		assert(heap->free + MAX(heap->free, nbytes) <= VAR_MAX);
		newsize = (size_t) roundup_8(heap->free + MAX(heap->free, nbytes));
		assert(heap->free <= VAR_MAX);
		block = (size_t) heap->free;	/* current end-of-heap */

#ifdef TRACE
		THRprintf(GDKstdout, "#No block found\n");
#endif

		/* Double the size of the heap.
		 * TUNE: increase heap by diffent amount. */
		HEAPDEBUG fprintf(stderr, "#HEAPextend in HEAP_malloc %s " SZFMT " " SZFMT "\n", heap->filename, heap->size, newsize);
		if (HEAPextend(heap, newsize) < 0)
			return 0;
		heap->free = newsize;
		hheader = HEAP_index(heap, 0, HEADER);

		blockp = HEAP_index(heap, block, CHUNK);
		trailp = HEAP_index(heap, trail, CHUNK);

#ifdef TRACE
		THRprintf(GDKstdout, "#New block made at pos " SZFMT " with size " SZFMT "\n", block, heap->size - block);
#endif

		blockp->next = 0;
		assert(heap->free - block <= VAR_MAX);
		blockp->size = (size_t) (heap->free - block);	/* determine size of allocated block */

		/* Try to join the last block in the freelist and the
		 * newly allocated memory */
		if ((trail != 0) && (trail + trailp->size == block)) {
#ifdef TRACE
			THRprintf(GDKstdout, "#Glue newly generated block to adjacent last\n");
#endif

			trailp->size += blockp->size;
			trailp->next = blockp->next;

			block = trail;
			trail = ttrail;
		}
	}

	/* Now we have found a block which is big enough in block.
	 * The predecessor of this block is in trail. */
	trailp = HEAP_index(heap, trail, CHUNK);
	blockp = HEAP_index(heap, block, CHUNK);

	/* If selected block is bigger than block needed split block
	 * in two.
	 * TUNE: use different amount than 2*sizeof(CHUNK) */
	if (blockp->size >= nbytes + 2 * sizeof(CHUNK)) {
		size_t newblock = block + nbytes;
		CHUNK *newblockp = HEAP_index(heap, newblock, CHUNK);

		newblockp->size = blockp->size - nbytes;
		newblockp->next = blockp->next;

		blockp->next = newblock;
		blockp->size = nbytes;
	}

	/* Delete block from freelist */
	if (trail == 0) {
		hheader->head = blockp->next;
	} else {
		trailp = HEAP_index(heap, trail, CHUNK);

		trailp->next = blockp->next;
	}

	block += hheader->alignment;
	return (var_t) (block >> GDK_VARSHIFT);
}

void
HEAP_free(Heap *heap, var_t mem)
{
	HEADER *hheader = HEAP_index(heap, 0, HEADER);
	CHUNK *beforep;
	CHUNK *blockp;
	CHUNK *afterp;
	size_t after, before, block = mem << GDK_VARSHIFT;

	if (hheader->alignment != 8 && hheader->alignment != 4) {
		GDKfatal("HEAP_free: Heap structure corrupt\n");
	}

	block -= hheader->alignment;
	blockp = HEAP_index(heap, block, CHUNK);

	/* block   -- block which we want to free
	 * before  -- first free block before block
	 * after   -- first free block after block
	 */

	before = 0;
	for (after = hheader->head; after != 0; after = HEAP_index(heap, after, CHUNK)->next) {
		if (after > block)
			break;
		before = after;
	}

	beforep = HEAP_index(heap, before, CHUNK);
	afterp = HEAP_index(heap, after, CHUNK);

	/* If it is not the last free block. */
	if (after != 0) {
		/*
		 * If this block and the block after are consecutive.
		 */
		if (block + blockp->size == after) {
			/*
			 * We unite them.
			 */
			blockp->size += afterp->size;
			blockp->next = afterp->next;
		} else
			blockp->next = after;
	} else {
		/*
		 * It is the last block in the freelist.
		 */
		blockp->next = 0;
	}

	/*
	 * If it is not the first block in the list.
	 */
	if (before != 0) {
		/*
		 * If the before block and this block are consecutive.
		 */
		if (before + beforep->size == block) {
			/*
			 * We unite them.
			 */
			beforep->size += blockp->size;
			beforep->next = blockp->next;
		} else
			beforep->next = block;
	} else {
		/*
		 * Add block at head of free list.
		 */
		hheader->head = block;
	}
}
