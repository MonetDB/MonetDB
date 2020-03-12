/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
 * library (see gdk_utils.c).
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
 * Read also the discussion in BATsetaccess (gdk_bat.c).
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

static void *
HEAPcreatefile(int farmid, size_t *maxsz, const char *fn)
{
	void *base = NULL;
	char *path = NULL;
	int fd;

	if (farmid != NOFARM) {
		/* call GDKfilepath once here instead of twice inside
		 * the calls to GDKfdlocate and GDKload */
		if ((path = GDKfilepath(farmid, BATDIR, fn, NULL)) == NULL)
			return NULL;
		fn = path;
	}
	/* round up to mulitple of GDK_mmap_pagesize */
	fd = GDKfdlocate(NOFARM, fn, "wb", NULL);
	if (fd >= 0) {
		close(fd);
		base = GDKload(NOFARM, fn, NULL, *maxsz, maxsz, STORE_MMAP);
	}
	GDKfree(path);
	return base;
}

static gdk_return HEAPload_intern(Heap *h, const char *nme, const char *ext, const char *suffix, bool trunc);
static gdk_return HEAPsave_intern(Heap *h, const char *nme, const char *ext, const char *suffix, bool dosync);

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
gdk_return
HEAPalloc(Heap *h, size_t nitems, size_t itemsize)
{
	h->base = NULL;
	h->size = 1;
	h->copied = false;
	if (itemsize)
		h->size = MAX(1, nitems) * itemsize;
	h->free = 0;
	h->cleanhash = false;

	/* check for overflow */
	if (itemsize && nitems > (h->size / itemsize)) {
		GDKerror("HEAPalloc: allocating more than heap can accomodate\n");
		return GDK_FAIL;
	}
	if (GDKinmemory() ||
	    (GDKmem_cursize() + h->size < GDK_mem_maxsize &&
	     h->size < (h->farmid == 0 ? GDK_mmap_minsize_persistent : GDK_mmap_minsize_transient))) {
		h->storage = STORE_MEM;
		h->base = GDKmalloc(h->size);
		TRC_DEBUG(HEAP, "HEAPalloc %zu %p\n", h->size, h->base);
	}
	if (!GDKinmemory() && h->base == NULL) {
		char *nme;

		nme = GDKfilepath(h->farmid, BATDIR, h->filename, NULL);
		if (nme == NULL)
			return GDK_FAIL;
		h->storage = STORE_MMAP;
		h->base = HEAPcreatefile(NOFARM, &h->size, nme);
		GDKfree(nme);
	}
	if (h->base == NULL) {
		GDKerror("HEAPalloc: Insufficient space for HEAP of %zu bytes.", h->size);
		return GDK_FAIL;
	}
	h->newstorage = h->storage;
	return GDK_SUCCEED;
}

/* Extend the allocated space of the heap H to be at least SIZE bytes.
 * If the heap grows beyond a threshold and a filename is known, the
 * heap is converted from allocated memory to a memory-mapped file.
 * When switching from allocated to memory mapped, if MAYSHARE is set,
 * the heap does not have to be copy-on-write.
 *
 * The function returns 0 on success, -1 on failure.
 *
 * When extending a memory-mapped heap, we use the function MT_mremap
 * (which see).  When extending an allocated heap, we use GDKrealloc.
 * If that fails, we switch to memory mapped, even when the size is
 * below the threshold.
 *
 * When converting from allocated to memory mapped, we try several
 * strategies.  First we try to create the memory map, and if that
 * works, copy the data and free the old memory.  If this fails, we
 * first write the data to disk, free the memory, and then try to
 * memory map the saved data. */
gdk_return
HEAPextend(Heap *h, size_t size, bool mayshare)
{
	char nme[sizeof(h->filename)], *ext;
	const char *failure = "None";

	if (GDKinmemory()) {
		strcpy_len(nme, ":inmemory", sizeof(nme));
		ext = "ext";
	} else {
		strcpy_len(nme, h->filename, sizeof(nme));
		ext = decompose_filename(nme);
	}
	if (size <= h->size)
		return GDK_SUCCEED;	/* nothing to do */

	failure = "size > h->size";

 	if (h->storage != STORE_MEM) {
		char *p;
		char *path;

		TRC_DEBUG(HEAP, "Extending %s mmapped heap (%s)\n", h->storage == STORE_MMAP ? "shared" : "privately", h->filename);
		/* extend memory mapped file */
		if ((path = GDKfilepath(h->farmid, BATDIR, nme, ext)) == NULL) {
			return GDK_FAIL;
		}
		size = (size + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
		if (size == 0)
			size = GDK_mmap_pagesize;

		p = GDKmremap(path,
			      h->storage == STORE_PRIV ?
				MMAP_COPY | MMAP_READ | MMAP_WRITE :
				MMAP_READ | MMAP_WRITE,
			      h->base, h->size, &size);
		GDKfree(path);
		if (p) {
			h->size = size;
			h->base = p;
 			return GDK_SUCCEED; /* success */
 		}
		failure = "GDKmremap() failed";
	} else {
		/* extend a malloced heap, possibly switching over to
		 * file-mapped storage */
		Heap bak = *h;
		bool exceeds_swap = size + GDKmem_cursize() >= GDK_mem_maxsize;
		bool must_mmap = !GDKinmemory() && (exceeds_swap || h->newstorage != STORE_MEM || size >= (h->farmid == 0 ? GDK_mmap_minsize_persistent : GDK_mmap_minsize_transient));

		h->size = size;

		/* try GDKrealloc if the heap size stays within
		 * reasonable limits */
		if (!must_mmap) {
			h->newstorage = h->storage = STORE_MEM;
			h->base = GDKrealloc(h->base, size);
			TRC_DEBUG(HEAP, "Extending malloced heap %zu %zu %p %p\n", size, h->size, bak.base, h->base);
			h->size = size;
			if (h->base)
				return GDK_SUCCEED; /* success */
			/* bak.base is still valid and may get restored */
			failure = "h->storage == STORE_MEM && !must_map && !h->base";
		}

		if (!GDKinmemory()) {
			/* too big: convert it to a disk-based temporary heap */
			bool existing = false;

			assert(h->storage == STORE_MEM);
			assert(ext != NULL);
			/* if the heap file already exists, we want to switch
			 * to STORE_PRIV (copy-on-write memory mapped files),
			 * but if the heap file doesn't exist yet, the BAT is
			 * new and we can use STORE_MMAP */
			int fd = GDKfdlocate(h->farmid, nme, "rb", ext);
			if (fd >= 0) {
				existing = true;
				close(fd);
			} else {
				/* no pre-existing heap file, so create a new
				 * one */
				h->base = HEAPcreatefile(h->farmid, &h->size, h->filename);
				if (h->base) {
					h->newstorage = h->storage = STORE_MMAP;
					memcpy(h->base, bak.base, bak.free);
					HEAPfree(&bak, false);
					return GDK_SUCCEED;
				}
				GDKclrerr();
			}
			fd = GDKfdlocate(h->farmid, nme, "wb", ext);
			if (fd >= 0) {
				close(fd);
				h->storage = h->newstorage == STORE_MMAP && existing && !mayshare ? STORE_PRIV : h->newstorage;
				/* make sure we really MMAP */
				if (must_mmap && h->newstorage == STORE_MEM)
					h->storage = STORE_MMAP;
				h->newstorage = h->storage;

				h->base = NULL;
				TRC_DEBUG(HEAP, "Converting malloced to %s mmapped heap\n", h->newstorage == STORE_MMAP ? "shared" : "privately");
				/* try to allocate a memory-mapped based
				 * heap */
				if (HEAPload(h, nme, ext, false) == GDK_SUCCEED) {
					/* copy data to heap and free old
					 * memory */
					memcpy(h->base, bak.base, bak.free);
					HEAPfree(&bak, false);
					return GDK_SUCCEED;
				}
				failure = "h->storage == STORE_MEM && can_map && fd >= 0 && HEAPload() != GDK_SUCCEED";
				/* couldn't allocate, now first save data to
				 * file */
				if (HEAPsave_intern(&bak, nme, ext, ".tmp", false) != GDK_SUCCEED) {
					failure = "h->storage == STORE_MEM && can_map && fd >= 0 && HEAPsave_intern() != GDK_SUCCEED";
					goto failed;
				}
				/* then free memory */
				HEAPfree(&bak, false);
				/* and load heap back in via memory-mapped
				 * file */
				if (HEAPload_intern(h, nme, ext, ".tmp", false) == GDK_SUCCEED) {
					/* success! */
					GDKclrerr();	/* don't leak errors from e.g. HEAPload */
					return GDK_SUCCEED;
				}
				failure = "h->storage == STORE_MEM && can_map && fd >= 0 && HEAPload_intern() != GDK_SUCCEED";
				/* we failed */
			} else {
				failure = "h->storage == STORE_MEM && can_map && fd < 0";
			}
		}
	  failed:
		*h = bak;
	}
	GDKerror("HEAPextend: failed to extend to %zu for %s%s%s: %s\n",
		 size, nme, ext ? "." : "", ext ? ext : "", failure);
	return GDK_FAIL;
}

gdk_return
HEAPshrink(Heap *h, size_t size)
{
	char *p = NULL;

	assert(size >= h->free);
	assert(size <= h->size);
	if (h->storage == STORE_MEM) {
		p = GDKrealloc(h->base, size);
		TRC_DEBUG(HEAP, "Shrinking malloced "
			  "heap %zu %zu %p "
			  "%p\n", h->size, size,
			  h->base, p);
	} else {
		char *path;

		/* shrink memory mapped file */
		/* round up to multiple of GDK_mmap_pagesize with
		 * minimum of one */
		size = (size + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
		if (size == 0)
			size = GDK_mmap_pagesize;
		if (size >= h->size) {
			/* don't grow */
			return GDK_SUCCEED;
		}
		if(!(path = GDKfilepath(h->farmid, BATDIR, h->filename, NULL)))
			return GDK_FAIL;
		p = GDKmremap(path,
			      h->storage == STORE_PRIV ?
				MMAP_COPY | MMAP_READ | MMAP_WRITE :
				MMAP_READ | MMAP_WRITE,
			      h->base, h->size, &size);
		GDKfree(path);
		TRC_DEBUG(HEAP, "Shrinking %s mmapped "
			  "heap (%s) %zu %zu %p %p\n",
			  h->storage == STORE_MMAP ? "shared" : "privately",
			  h->filename, h->size, size, h->base, p);
	}
	if (p) {
		h->size = size;
		h->base = p;
		return GDK_SUCCEED;
	}
	return GDK_FAIL;
}

/* returns 1 if the file exists */
static int
file_exists(int farmid, const char *dir, const char *name, const char *ext)
{
	char *path;
	struct stat st;
	int ret;

	path = GDKfilepath(farmid, dir, name, ext);
	if (path == NULL)
		return -1;
	ret = stat(path, &st);
	TRC_DEBUG(IO_, "stat(%s) = %d\n", path, ret);
	GDKfree(path);
	return (ret == 0);
}

gdk_return
GDKupgradevarheap(BAT *b, var_t v, bool copyall, bool mayshare)
{
	uint8_t shift = b->tshift;
	uint16_t width = b->twidth;
	unsigned char *pc;
	unsigned short *ps;
	unsigned int *pi;
#if SIZEOF_VAR_T == 8
	var_t *pv;
#endif
	size_t i, n;
	size_t savefree;
	const char *filename;
	bat bid = b->batCacheid;

	assert(b->theap.parentid == 0);
	assert(width != 0);
	assert(v >= GDK_VAROFFSET);
	assert(width < SIZEOF_VAR_T && (width <= 2 ? v - GDK_VAROFFSET : v) >= ((var_t) 1 << (8 * width)));
	while (width < SIZEOF_VAR_T && (width <= 2 ? v - GDK_VAROFFSET : v) >= ((var_t) 1 << (8 * width))) {
		width <<= 1;
		shift++;
	}
	assert(b->twidth < width);
	assert(b->tshift < shift);

	/* if copyall is set, we need to convert the whole heap, since
	 * we may be in the middle of an insert loop that adjusts the
	 * free value at the end; otherwise only copy the area
	 * indicated by the "free" pointer */
	n = (copyall ? b->theap.size : b->theap.free) >> b->tshift;

	/* Create a backup copy before widening.
	 *
	 * If the file is memory-mapped, this solves a problem that we
	 * don't control what's in the actual file until the next
	 * commit happens, so a crash might otherwise leave the file
	 * (and the database) in an inconsistent state.  If, on the
	 * other hand, the heap is allocated, it may happen that later
	 * on the heap is extended and converted into a memory-mapped
	 * file.  Then the same problem arises.
	 *
	 * also see do_backup in gdk_bbp.c */
	filename = strrchr(b->theap.filename, DIR_SEP);
	if (filename == NULL)
		filename = b->theap.filename;
	else
		filename++;
	int exists = 0;
	if ((BBP_status(bid) & (BBPEXISTING|BBPDELETED)) &&
	    !(exists = file_exists(b->theap.farmid, BAKDIR, filename, NULL)) &&
	    (b->theap.storage != STORE_MEM ||
	     GDKmove(b->theap.farmid, BATDIR, b->theap.filename, NULL,
		     BAKDIR, filename, NULL) != GDK_SUCCEED)) {
		int fd;
		ssize_t ret = 0;
		size_t size = n << b->tshift;
		const char *base = b->theap.base;

		/* first save heap in file with extra .tmp extension */
		if ((fd = GDKfdlocate(b->theap.farmid, b->theap.filename, "wb", "tmp")) < 0)
			return GDK_FAIL;
		while (size > 0) {
			ret = write(fd, base, (unsigned) MIN(1 << 30, size));
			if (ret < 0)
				size = 0;
			size -= ret;
			base += ret;
		}
		if (ret < 0 ||
		    (!(GDKdebug & NOSYNCMASK)
#if defined(NATIVE_WIN32)
		     && _commit(fd) < 0
#elif defined(HAVE_FDATASYNC)
		     && fdatasync(fd) < 0
#elif defined(HAVE_FSYNC)
		     && fsync(fd) < 0
#endif
			    ) ||
		    close(fd) < 0) {
			/* something went wrong: abandon ship */
			GDKsyserror("GDKupgradevarheap: syncing heap to disk failed\n");
			close(fd);
			GDKunlink(b->theap.farmid, BATDIR, b->theap.filename, "tmp");
			return GDK_FAIL;
		}
		/* move tmp file to backup directory (without .tmp
		 * extension) */
		if (GDKmove(b->theap.farmid, BATDIR, b->theap.filename, "tmp", BAKDIR, filename, NULL) != GDK_SUCCEED) {
			/* backup failed */
			GDKunlink(b->theap.farmid, BATDIR, b->theap.filename, "tmp");
			return GDK_FAIL;
		}
	}
	if (exists == -1)
		return GDK_FAIL;

	savefree = b->theap.free;
	if (copyall)
		b->theap.free = b->theap.size;
	if (HEAPextend(&b->theap, (b->theap.size >> b->tshift) << shift, mayshare) != GDK_SUCCEED)
		return GDK_FAIL;
	if (copyall)
		b->theap.free = savefree;
	/* note, cast binds more closely than addition */
	pc = (unsigned char *) b->theap.base + n;
	ps = (unsigned short *) b->theap.base + n;
	pi = (unsigned int *) b->theap.base + n;
#if SIZEOF_VAR_T == 8
	pv = (var_t *) b->theap.base + n;
#endif

	/* convert from back to front so that we can do it in-place */
	switch (width) {
	case 2:
#ifndef NDEBUG
		memset(ps, 0, b->theap.base + b->theap.size - (char *) ps);
#endif
		switch (b->twidth) {
		case 1:
			for (i = 0; i < n; i++)
				*--ps = *--pc;
			break;
		}
		break;
	case 4:
#ifndef NDEBUG
		memset(pi, 0, b->theap.base + b->theap.size - (char *) pi);
#endif
		switch (b->twidth) {
		case 1:
			for (i = 0; i < n; i++)
				*--pi = *--pc + GDK_VAROFFSET;
			break;
		case 2:
			for (i = 0; i < n; i++)
				*--pi = *--ps + GDK_VAROFFSET;
			break;
		}
		break;
#if SIZEOF_VAR_T == 8
	case 8:
#ifndef NDEBUG
		memset(pv, 0, b->theap.base + b->theap.size - (char *) pv);
#endif
		switch (b->twidth) {
		case 1:
			for (i = 0; i < n; i++)
				*--pv = *--pc + GDK_VAROFFSET;
			break;
		case 2:
			for (i = 0; i < n; i++)
				*--pv = *--ps + GDK_VAROFFSET;
			break;
		case 4:
			for (i = 0; i < n; i++)
				*--pv = *--pi;
			break;
		}
		break;
#endif
	}
	b->theap.free <<= shift - b->tshift;
	b->tshift = shift;
	b->twidth = width;
	return GDK_SUCCEED;
}

/*
 * @- HEAPcopy
 * simple: alloc and copy. Notice that we suppose a preallocated
 * dst->filename (or NULL), which might be used in HEAPalloc().
 */
gdk_return
HEAPcopy(Heap *dst, Heap *src)
{
	if (HEAPalloc(dst, src->size, 1) == GDK_SUCCEED) {
		dst->free = src->free;
		memcpy(dst->base, src->base, src->free);
		dst->hashash = src->hashash;
		dst->cleanhash = src->cleanhash;
		dst->dirty = true;
		return GDK_SUCCEED;
	}
	return GDK_FAIL;
}

/* Free the memory associated with the heap H.
 * Unlinks (removes) the associated file if the rmheap flag is set. */
void
HEAPfree(Heap *h, bool rmheap)
{
	if (h->base) {
		if (h->storage == STORE_MEM) {	/* plain memory */
			TRC_DEBUG(HEAP, "HEAPfree %zu %p\n", h->size, h->base);
			GDKfree(h->base);
		} else if (h->storage == STORE_CMEM) {
			//heap is stored in regular C memory rather than GDK memory,so we call free()
			free(h->base);
		} else {	/* mapped file, or STORE_PRIV */
			gdk_return ret = GDKmunmap(h->base, h->size);

			if (ret != GDK_SUCCEED) {
				GDKsyserror("HEAPfree: %s was not mapped\n",
					    h->filename);
				assert(0);
			}
			TRC_DEBUG(HEAP, "munmap(base=%p, size=%zu) = %d\n",
				  (void *)h->base, h->size, (int) ret);
		}
	}
	h->base = NULL;
#ifdef HAVE_FORK
	if (h->storage == STORE_MMAPABS)  {
		/* heap is stored in a mmap() file, but h->filename
		 * is the absolute path */
		if (remove(h->filename) != 0 && errno != ENOENT) {
			perror(h->filename);
		}
	} else
#endif
	if (rmheap) {
		char *path = GDKfilepath(h->farmid, BATDIR, h->filename, NULL);
		if (path && remove(path) != 0 && errno != ENOENT)
			perror(path);
		GDKfree(path);
		path = GDKfilepath(h->farmid, BATDIR, h->filename, "new");
		if (path && remove(path) != 0 && errno != ENOENT)
			perror(path);
		GDKfree(path);
	}
}

/*
 * @- HEAPload
 *
 * If we find file X.new, we move it over X (if present) and open it.
 */
static gdk_return
HEAPload_intern(Heap *h, const char *nme, const char *ext, const char *suffix, bool trunc)
{
	size_t minsize;
	int ret = 0;
	char *srcpath, *dstpath, *tmp;
	int t0;

	h->storage = h->newstorage = h->size < GDK_mmap_minsize_persistent ? STORE_MEM : STORE_MMAP;

	minsize = (h->size + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
	if (h->storage != STORE_MEM && minsize != h->size)
		h->size = minsize;

	/* when a bat is made read-only, we can truncate any unused
	 * space at the end of the heap */
	if (trunc) {
		/* round up mmap heap sizes to GDK_mmap_pagesize
		 * segments, also add some slack */
		size_t truncsize = ((size_t) (h->free * 1.05) + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
		int fd;

		if (truncsize == 0)
			truncsize = GDK_mmap_pagesize; /* minimum of one page */
		if (truncsize < h->size &&
		    (fd = GDKfdlocate(h->farmid, nme, "mrb+", ext)) >= 0) {
			ret = ftruncate(fd, truncsize);
			TRC_DEBUG(HEAP,
				  "ftruncate(file=%s.%s, size=%zu) = %d\n",
				  nme, ext, truncsize, ret);
			close(fd);
			if (ret == 0) {
				h->size = truncsize;
			}
		}
	}

	TRC_DEBUG(HEAP, "HEAPload(%s.%s,storage=%d,free=%zu,size=%zu)\n",
		  nme, ext, (int) h->storage, h->free, h->size);

	/* On some OSs (WIN32,Solaris), it is prohibited to write to a
	 * file that is open in MAP_PRIVATE (FILE_MAP_COPY) solution:
	 * we write to a file named .ext.new.  This file, if present,
	 * takes precedence. */
	srcpath = GDKfilepath(h->farmid, BATDIR, nme, ext);
	dstpath = GDKfilepath(h->farmid, BATDIR, nme, ext);
	if (srcpath == NULL ||
	    dstpath == NULL ||
	    (tmp = GDKrealloc(srcpath, strlen(srcpath) + strlen(suffix) + 1)) == NULL) {
		GDKfree(srcpath);
		GDKfree(dstpath);
		return GDK_FAIL;
	}
	srcpath = tmp;
	strcat(srcpath, suffix);

	t0 = GDKms();
	ret = rename(srcpath, dstpath);
	TRC_DEBUG(HEAP, "rename %s %s = %d %s (%dms)\n",
		  srcpath, dstpath, ret, ret < 0 ? GDKstrerror(errno, (char[128]){0}, 128) : "",
		  GDKms() - t0);
	GDKfree(srcpath);
	GDKfree(dstpath);

	h->base = GDKload(h->farmid, nme, ext, h->free, &h->size, h->newstorage);
	if (h->base == NULL)
		return GDK_FAIL; /* file could  not be read satisfactorily */

	return GDK_SUCCEED;
}

gdk_return
HEAPload(Heap *h, const char *nme, const char *ext, bool trunc)
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
static gdk_return
HEAPsave_intern(Heap *h, const char *nme, const char *ext, const char *suffix, bool dosync)
{
	storage_t store = h->newstorage;
	long_str extension;

	if (h->base == NULL) {
		GDKerror("HEAPsave_intern: no heap to save\n");
		return GDK_FAIL;
	}
	if (h->storage != STORE_MEM && store == STORE_PRIV) {
		/* anonymous or private VM is saved as if it were malloced */
		store = STORE_MEM;
		assert(strlen(ext) + strlen(suffix) < sizeof(extension));
		strconcat_len(extension, sizeof(extension), ext, suffix, NULL);
		ext = extension;
	} else if (store != STORE_MEM) {
		store = h->storage;
	}
	TRC_DEBUG(HEAP,
		  "(%s.%s,storage=%d,free=%zu,size=%zu,dosync=%s)\n",
		  nme, ext, (int) h->newstorage, h->free, h->size,
		  dosync?"true":"false");
	return GDKsave(h->farmid, nme, ext, h->base, h->free, store, dosync);
}

gdk_return
HEAPsave(Heap *h, const char *nme, const char *ext, bool dosync)
{
	return HEAPsave_intern(h, nme, ext, ".new", dosync);
}

/*
 * @- HEAPdelete
 * Delete any saved heap file. For memory mapped files, also try to
 * remove any remaining X.new
 */
gdk_return
HEAPdelete(Heap *h, const char *o, const char *ext)
{
	char ext2[64];

	if (h->size <= 0) {
		assert(h->base == 0);
		return GDK_SUCCEED;
	}
	if (h->base)
		HEAPfree(h, false);	/* we will do the unlinking */
	if (h->copied) {
		return GDK_SUCCEED;
	}
	assert(strlen(ext) + strlen(".new") < sizeof(ext2));
	strconcat_len(ext2, sizeof(ext2), ext, ".new", NULL);
	return (GDKunlink(h->farmid, BATDIR, o, ext) == GDK_SUCCEED) | (GDKunlink(h->farmid, BATDIR, o, ext2) == GDK_SUCCEED) ? GDK_SUCCEED : GDK_FAIL;
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
			bogus_result |= cur[0] | cur[1024] | cur[2048] | cur[3072];
	}
	return bogus_result;
}


/* Return the (virtual) size of the heap. */
size_t
HEAPvmsize(Heap *h)
{
	if (h && h->base && h->free)
		return h->size;
	return 0;
}

/* Return the allocated size of the heap, i.e. if the heap is memory
 * mapped and not copy-on-write (privately mapped), return 0. */
size_t
HEAPmemsize(Heap *h)
{
	if (h && h->base && h->free && h->storage != STORE_MMAP)
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

#define HEAPVERSION	20030408

typedef struct heapheader {
	size_t head;		/* index to first free block */
	int alignment;		/* alignment of objects on heap */
	size_t firstblock;	/* first block in heap */
	int version;
	int (*sizefcn)(const void *);	/* ADT function to ask length */
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
	size_t size;		/* Size of this block in freelist */
	size_t next;		/* index of next block */
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

#define HEAP_index(HEAP,INDEX,TYPE)	((TYPE *)((char *) (HEAP)->base + (INDEX)))


static void
HEAP_printstatus(Heap *heap)
{
	HEADER *hheader = HEAP_index(heap, 0, HEADER);
	size_t block, cur_free = hheader->head;
	CHUNK *blockp;

	TRC_DEBUG(TRACE,
		  "HEAP has head %zu and alignment %d and size %zu\n",
		  hheader->head, hheader->alignment, heap->free);

	/* Walk the blocklist */
	block = hheader->firstblock;

	while (block < heap->free) {
		blockp = HEAP_index(heap, block, CHUNK);

		if (block == cur_free) {
			TRC_DEBUG(TRACE,
				  "Free block at %p has size %zu and next %zu\n",
				  (void *)block,
				  blockp->size, blockp->next);

			cur_free = blockp->next;
			block += blockp->size;
		} else {
			size_t size = blocksize(hheader, blockp);

			TRC_DEBUG(TRACE,
				  "Block at %zu with size %zu\n",
				  block, size);
			block += size;
		}
	}
}

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
	TRC_DEBUG_IF(TRACE)
	{
		TRC_DEBUG(TRACE, "We created the following heap\n");
		HEAP_printstatus(heap);
	}
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
		if (HEAPalloc(heap, total, 1) != GDK_SUCCEED)
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

	TRC_DEBUG(TRACE, "Enter malloc with %zu bytes\n", nbytes);

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
	for (block = hheader->head; block != 0; block = blockp->next) {
		blockp = HEAP_index(heap, block, CHUNK);

		TRC_DEBUG(TRACE, "Block %zu is %zu bytes\n", block, blockp->size);
		
		assert(trail == 0 || block > trail);
		if (trail != 0 && block <= trail) {
			GDKerror("HEAP_malloc: Free list is not orderered\n");
			return 0;
		}

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
		newsize = MIN(heap->free, (size_t) 1 << 20);
		newsize = (size_t) roundup_8(heap->free + MAX(newsize, nbytes));
		assert(heap->free <= VAR_MAX);
		block = (size_t) heap->free;	/* current end-of-heap */

		TRC_DEBUG(TRACE, "No block found\n");

		/* Increase the size of the heap. */
		TRC_DEBUG(HEAP, "HEAPextend in HEAP_malloc %s %zu %zu\n", heap->filename, heap->size, newsize);
		if (HEAPextend(heap, newsize, false) != GDK_SUCCEED)
			return 0;
		heap->free = newsize;
		hheader = HEAP_index(heap, 0, HEADER);

		blockp = HEAP_index(heap, block, CHUNK);
		trailp = HEAP_index(heap, trail, CHUNK);

		TRC_DEBUG(TRACE, "New block made at pos %zu with size %zu\n", block, heap->size - block);

		blockp->next = 0;
		assert(heap->free - block <= VAR_MAX);
		blockp->size = (size_t) (heap->free - block);	/* determine size of allocated block */

		/* Try to join the last block in the freelist and the
		 * newly allocated memory */
		if ((trail != 0) && (trail + trailp->size == block)) {
			TRC_DEBUG(TRACE, "Glue newly generated block to adjacent last\n");

			trailp->size += blockp->size;
			trailp->next = blockp->next;

			block = trail;
			trail = ttrail;
		}
	}

	/* Now we have found a block which is big enough in block.
	 * The predecessor of this block is in trail. */
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
	return (var_t) block;
}

void
HEAP_free(Heap *heap, var_t mem)
{
	HEADER *hheader = HEAP_index(heap, 0, HEADER);
	CHUNK *beforep;
	CHUNK *blockp;
	CHUNK *afterp;
	size_t after, before, block = mem;

	assert(hheader->alignment == 8 || hheader->alignment == 4);
	if (hheader->alignment != 8 && hheader->alignment != 4) {
		GDKerror("HEAP_free: Heap structure corrupt\n");
		return;
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

void
HEAP_recover(Heap *h, const var_t *offsets, BUN noffsets)
{
	HEADER *hheader;
	CHUNK *blockp;
	size_t dirty = 0;
	var_t maxoff = 0;
	BUN i;

	if (!h->cleanhash)
		return;
	hheader = HEAP_index(h, 0, HEADER);
	assert(h->free >= sizeof(HEADER));
	assert(hheader->version == HEAPVERSION);
	assert(h->size >= hheader->firstblock);
	for (i = 0; i < noffsets; i++)
		if (offsets[i] > maxoff)
			maxoff = offsets[i];
	assert(maxoff < h->free);
	if (maxoff == 0) {
		if (hheader->head != hheader->firstblock) {
			hheader->head = hheader->firstblock;
			dirty = sizeof(HEADER);
		}
		blockp = HEAP_index(h, hheader->firstblock, CHUNK);
		if (blockp->next != 0 ||
		    blockp->size != h->size - hheader->head) {
			blockp->size = (size_t) (h->size - hheader->head);
			blockp->next = 0;
			dirty = hheader->firstblock + sizeof(CHUNK);
		}
	} else {
		size_t block = maxoff - hheader->alignment;
		size_t end = block + *HEAP_index(h, block, size_t);
		size_t trail;

		assert(end <= h->free);
		if (end + sizeof(CHUNK) <= h->free) {
			blockp = HEAP_index(h, end, CHUNK);
			if (hheader->head <= end &&
			    blockp->next == 0 &&
			    blockp->size == h->free - end)
				return;
		} else if (hheader->head == 0) {
			/* no free space after last allocated block
			 * and no free list */
			return;
		}
		block = hheader->head;
		trail = 0;
		while (block < maxoff && block != 0) {
			blockp = HEAP_index(h, block, CHUNK);
			trail = block;
			block = blockp->next;
		}
		if (trail == 0) {
			/* no free list */
			if (end + sizeof(CHUNK) > h->free) {
				/* no free space after last allocated
				 * block */
				if (hheader->head != 0) {
					hheader->head = 0;
					dirty = sizeof(HEADER);
				}
			} else {
				/* there is free space after last
				 * allocated block */
				if (hheader->head != end) {
					hheader->head = end;
					dirty = sizeof(HEADER);
				}
				blockp = HEAP_index(h, end, CHUNK);
				if (blockp->next != 0 ||
				    blockp->size != h->free - end) {
					blockp->next = 0;
					blockp->size = h->free - end;
					dirty = end + sizeof(CHUNK);
				}
			}
		} else {
			/* there is a free list */
			blockp = HEAP_index(h, trail, CHUNK);
			if (end + sizeof(CHUNK) > h->free) {
				/* no free space after last allocated
				 * block */
				if (blockp->next != 0) {
					blockp->next = 0;
					dirty = trail + sizeof(CHUNK);
				}
			} else {
				/* there is free space after last
				 * allocated block */
				if (blockp->next != end) {
					blockp->next = end;
					dirty = trail + sizeof(CHUNK);
				}
				blockp = HEAP_index(h, end, CHUNK);
				if (blockp->next != 0 ||
				    blockp->size != h->free - end) {
					blockp->next = 0;
					blockp->size = h->free - end;
					dirty = end + sizeof(CHUNK);
				}
			}
		}
	}
	h->cleanhash = false;
	if (dirty) {
		if (h->storage == STORE_MMAP) {
			if (!(GDKdebug & NOSYNCMASK))
				(void) MT_msync(h->base, dirty);
		} else
			h->dirty = true;
	}
}
