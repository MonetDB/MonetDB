/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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
#include "mutils.h"

static void *
HEAPcreatefile(int farmid, size_t *maxsz, const char *fn)
{
	void *base = NULL;
	char path[MAXPATH];
	int fd;

	if (farmid != NOFARM) {
		/* call GDKfilepath once here instead of twice inside
		 * the calls to GDKfdlocate and GDKload */
		if (GDKfilepath(path, sizeof(path), farmid, BATDIR, fn, NULL) != GDK_SUCCEED)
			return NULL;
		fn = path;
	}
	/* round up to multiple of GDK_mmap_pagesize */
	fd = GDKfdlocate(NOFARM, fn, "wb", NULL);
	if (fd >= 0) {
		close(fd);
		base = GDKload(NOFARM, fn, NULL, *maxsz, maxsz, STORE_MMAP);
	}
	return base;
}

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

/* this function is called with the theaplock held */
gdk_return
HEAPgrow(Heap **hp, size_t size, bool mayshare)
{
	Heap *new;

	ATOMIC_BASE_TYPE refs = ATOMIC_GET(&(*hp)->refs);
	if ((refs & HEAPREFS) == 1) {
		return HEAPextend(*hp, size, mayshare);
	}
	new = GDKmalloc(sizeof(Heap));
	if (new != NULL) {
		Heap *old = *hp;
		*new = (Heap) {
			.farmid = old->farmid,
			.dirty = true,
			.parentid = old->parentid,
			.wasempty = old->wasempty,
			.hasfile = old->hasfile,
			.refs = ATOMIC_VAR_INIT(1 | (refs & HEAPREMOVE)),
		};
		memcpy(new->filename, old->filename, sizeof(new->filename));
		if (HEAPalloc(new, size, 1) == GDK_SUCCEED) {
			new->free = old->free;
			new->cleanhash = old->cleanhash;
			if (old->free > 0 &&
			    (new->storage == STORE_MEM || old->storage == STORE_MEM))
				memcpy(new->base, old->base, old->free);
			/* else both are STORE_MMAP and refer to the
			 * same file and so we don't need to copy */

			/* replace old heap with new */
			HEAPdecref(*hp, false);
			*hp = new;
		} else {
			GDKfree(new);
			new = NULL;
		}
	}
	return new ? GDK_SUCCEED : GDK_FAIL;
}

/*
 * @- HEAPalloc
 *
 * Normally, we use GDKmalloc for creating a new heap.  Huge heaps,
 * though, come from memory mapped files that we create with a large
 * fallocate. This is fast, and leads to files-with-holes on Unixes (on
 * Windows, it actually always performs I/O which is not nice).
 */
gdk_return
HEAPalloc(Heap *h, size_t nitems, size_t itemsize)
{
	size_t size = 0;
	QryCtx *qc = h->farmid == 1 ? MT_thread_get_qry_ctx() : NULL;

	h->base = NULL;
	h->size = 1;
	if (itemsize) {
		/* check for overflow */
		if (nitems > BUN_NONE / itemsize) {
			GDKerror("allocating more than heap can accommodate\n");
			return GDK_FAIL;
		}
		h->size = MAX(1, nitems) * itemsize;
	}
	h->free = 0;
	h->cleanhash = false;

#ifdef SIZE_CHECK_IN_HEAPS_ONLY
	if (GDKvm_cursize() + h->size >= GDK_vm_maxsize &&
	    !MT_thread_override_limits()) {
		GDKerror("allocating too much memory (current: %zu, requested: %zu, limit: %zu)\n", GDKvm_cursize(), h->size, GDK_vm_maxsize);
		return GDK_FAIL;
	}
#endif

	size_t allocated;
	if (GDKinmemory(h->farmid) ||
	    ((allocated = GDKmem_cursize()) + h->size < GDK_mem_maxsize &&
	     h->size < (h->farmid == 0 ? GDK_mmap_minsize_persistent : GDK_mmap_minsize_transient) &&
	     h->size < ((GDK_mem_maxsize - allocated) >> 6))) {
		h->storage = STORE_MEM;
		size = h->size;
		if (qc != NULL) {
			ATOMIC_BASE_TYPE sz = ATOMIC_ADD(&qc->datasize, size);
			sz += size;
			if (qc->maxmem > 0 && sz > qc->maxmem) {
				ATOMIC_SUB(&qc->datasize, size);
				GDKerror("Query using too much memory.\n");
				return GDK_FAIL;
			}
		}
		h->base = GDKmalloc(size);
		TRC_DEBUG(HEAP, "%s %zu %p\n", h->filename, size, h->base);
		if (h->base == NULL && qc != NULL)
			ATOMIC_SUB(&qc->datasize, size);
	}

	if (h->base == NULL && !GDKinmemory(h->farmid)) {
		char nme[MAXPATH];
		if (GDKfilepath(nme, sizeof(nme), h->farmid, BATDIR, h->filename, NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		h->storage = STORE_MMAP;
		h->size = (h->size + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
		size = h->size;
		if (qc != NULL) {
			ATOMIC_BASE_TYPE sz = ATOMIC_ADD(&qc->datasize, size);
			sz += size;
			if (qc->maxmem > 0 && sz > qc->maxmem) {
				ATOMIC_SUB(&qc->datasize, size);
				GDKerror("Query using too much memory.\n");
				return GDK_FAIL;
			}
		}
		h->base = HEAPcreatefile(NOFARM, &h->size, nme);
		h->hasfile = true;
		if (h->base == NULL) {
			if (qc != NULL)
				ATOMIC_SUB(&qc->datasize, size);
			/* remove file we may just have created
			 * it may or may not exist, depending on what
			 * failed */
			(void) MT_remove(nme);
			h->hasfile = false; /* just removed it */
			GDKerror("Insufficient space for HEAP of %zu bytes.", h->size);
			return GDK_FAIL;
		}
		TRC_DEBUG(HEAP, "%s %zu %p (mmap)\n", h->filename, size, h->base);
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
	size_t osize = h->size;
	size_t xsize;
	QryCtx *qc = h->farmid == 1 ? MT_thread_get_qry_ctx() : NULL;

	if (size <= h->size)
		return GDK_SUCCEED;	/* nothing to do */

	char nme[sizeof(h->filename)], *ext;
	const char *failure = "None";

	if (GDKinmemory(h->farmid)) {
		strcpy_len(nme, ":memory:", sizeof(nme));
		ext = "ext";
	} else {
		strcpy_len(nme, h->filename, sizeof(nme));
		ext = decompose_filename(nme);
	}
	failure = "size > h->size";

#ifdef SIZE_CHECK_IN_HEAPS_ONLY
	if (GDKvm_cursize() + size - h->size >= GDK_vm_maxsize &&
	    !MT_thread_override_limits()) {
		GDKerror("allocating too much memory (current: %zu, requested: %zu, limit: %zu)\n", GDKvm_cursize(), size - h->size, GDK_vm_maxsize);
		return GDK_FAIL;
	}
#endif

	if (h->storage != STORE_MEM) {
		char *p;
		char path[MAXPATH];

		assert(h->hasfile);
		TRC_DEBUG(HEAP, "Extending %s mmapped heap (%s)\n", h->storage == STORE_MMAP ? "shared" : "privately", h->filename);
		/* extend memory mapped file */
		if (GDKfilepath(path, sizeof(path), h->farmid, BATDIR, nme, ext) != GDK_SUCCEED) {
			return GDK_FAIL;
		}
		size = (size + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
		if (size == 0)
			size = GDK_mmap_pagesize;

		xsize = size - osize;

		if (qc != NULL) {
			ATOMIC_BASE_TYPE sz = ATOMIC_ADD(&qc->datasize, xsize);
			sz += size - osize;
			if (qc->maxmem > 0 && sz > qc->maxmem) {
				GDKerror("Query using too much memory.\n");
				ATOMIC_SUB(&qc->datasize, xsize);
				return GDK_FAIL;
			}
		}
		p = GDKmremap(path,
			      h->storage == STORE_PRIV ?
				MMAP_COPY | MMAP_READ | MMAP_WRITE :
				MMAP_READ | MMAP_WRITE,
			      h->base, h->size, &size);
		if (p) {
			h->size = size;
			h->base = p;
			return GDK_SUCCEED; /* success */
		}
		if (qc != NULL)
			ATOMIC_SUB(&qc->datasize, xsize);
		failure = "GDKmremap() failed";
	} else {
		/* extend a malloced heap, possibly switching over to
		 * file-mapped storage */
		Heap bak = *h;
		size_t allocated;
		bool must_mmap = (!GDKinmemory(h->farmid) &&
				   (h->newstorage != STORE_MEM ||
				    (allocated = GDKmem_cursize()) + size >= GDK_mem_maxsize ||
				    size >= (h->farmid == 0 ? GDK_mmap_minsize_persistent : GDK_mmap_minsize_transient) ||
				    size >= ((GDK_mem_maxsize - allocated) >> 6)));

		h->size = size;
		xsize = size - osize;

		/* try GDKrealloc if the heap size stays within
		 * reasonable limits */
		if (!must_mmap) {
			if (qc != NULL) {
				ATOMIC_BASE_TYPE sz = ATOMIC_ADD(&qc->datasize, xsize);
				sz += xsize;
				if (qc->maxmem > 0 && sz > qc->maxmem) {
					GDKerror("Query using too much memory.\n");
					ATOMIC_SUB(&qc->datasize, xsize);
					*h = bak;
					return GDK_FAIL;
				}
			}
			h->newstorage = h->storage = STORE_MEM;
			h->base = GDKrealloc(h->base, size);
			TRC_DEBUG(HEAP, "Extending malloced heap %s %zu->%zu %p->%p\n", h->filename, bak.size, size, bak.base, h->base);
			if (h->base) {
				return GDK_SUCCEED; /* success */
			}
			/* bak.base is still valid and may get restored */
			failure = "h->storage == STORE_MEM && !must_map && !h->base";
			if (qc != NULL)
				ATOMIC_SUB(&qc->datasize, xsize);
		}

		if (!GDKinmemory(h->farmid)) {
			/* too big: convert it to a disk-based temporary heap */

			assert(h->storage == STORE_MEM);
			assert(ext != NULL);
			/* if the heap file already exists, we want to switch
			 * to STORE_PRIV (copy-on-write memory mapped files),
			 * but if the heap file doesn't exist yet, the BAT is
			 * new and we can use STORE_MMAP */
			int fd = GDKfdlocate(h->farmid, nme, "rb", ext);
			if (fd >= 0) {
				assert(h->hasfile);
				close(fd);
				fd = GDKfdlocate(h->farmid, nme, "wb", ext);
				if (fd >= 0) {
					gdk_return rc = GDKextendf(fd, size, nme);
					close(fd);
					if (rc != GDK_SUCCEED) {
						failure = "h->storage == STORE_MEM && can_map && fd >= 0 && GDKextendf() != GDK_SUCCEED";
						goto failed;
					}
					h->storage = h->newstorage == STORE_MMAP && !mayshare ? STORE_PRIV : h->newstorage;
					/* make sure we really MMAP */
					if (must_mmap && h->newstorage == STORE_MEM)
						h->storage = STORE_MMAP;
					h->newstorage = h->storage;

					h->base = NULL;
					TRC_DEBUG(HEAP, "Converting malloced to %s mmapped heap %s\n", h->newstorage == STORE_MMAP ? "shared" : "privately", h->filename);
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
					/* we failed */
				} else {
					failure = "h->storage == STORE_MEM && can_map && fd < 0";
				}
			} else {
				/* no pre-existing heap file, so create a new
				 * one */
				if (qc != NULL) {
					h->size = (h->size + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
					xsize = h->size;
					ATOMIC_BASE_TYPE sz = ATOMIC_ADD(&qc->datasize, xsize);
					sz += h->size;
					if (qc->maxmem > 0 && sz > qc->maxmem) {
						GDKerror("Query using too much memory.\n");
						sz = ATOMIC_ADD(&qc->datasize, xsize);
						*h = bak;
						return GDK_FAIL;
					}
				}
				h->base = HEAPcreatefile(h->farmid, &h->size, h->filename);
				h->hasfile = true;
				if (h->base) {
					h->newstorage = h->storage = STORE_MMAP;
					if (bak.free > 0)
						memcpy(h->base, bak.base, bak.free);
					HEAPfree(&bak, false);
					return GDK_SUCCEED;
				}
				failure = "h->storage == STORE_MEM && can_map && fd >= 0 && HEAPcreatefile() == NULL";
				if (qc != NULL)
					ATOMIC_SUB(&qc->datasize, xsize);
			}
		}
	  failed:
		if (h->hasfile && !bak.hasfile) {
			char path[MAXPATH];

			if (GDKfilepath(path, sizeof(path), h->farmid, BATDIR, nme, ext) == GDK_SUCCEED) {
				MT_remove(path);
			} else {
				/* couldn't remove, so now we have a file */
				bak.hasfile = true;
			}
		}
		*h = bak;
	}
	GDKerror("failed to extend to %zu for %s%s%s: %s\n",
		 size, nme, ext ? "." : "", ext ? ext : "", failure);
	return GDK_FAIL;
}

/* grow the string offset heap so that the value v fits (i.e. wide
 * enough to fit the value), and it has space for at least cap elements;
 * copy ncopy BUNs, or up to the heap size, whichever is smaller */
gdk_return
GDKupgradevarheap(BAT *b, var_t v, BUN cap, BUN ncopy)
{
	uint8_t shift = b->tshift;
	uint16_t width = b->twidth;
	uint8_t *pc;
	uint16_t *ps;
	uint32_t *pi;
#if SIZEOF_VAR_T == 8
	uint64_t *pl;
#endif
	size_t i, n;
	size_t newsize;
	bat bid = b->batCacheid;
	Heap *old, *new;

	old = b->theap;
//	assert(old->storage == STORE_MEM);
	assert(old->parentid == b->batCacheid);
	assert(b->tbaseoff == 0);
	assert(width != 0);
	assert(v >= GDK_VAROFFSET);

	while (width < SIZEOF_VAR_T && (width <= 2 ? v - GDK_VAROFFSET : v) >= ((var_t) 1 << (8 * width))) {
		width <<= 1;
		shift++;
	}
	/* if cap(acity) given (we check whether it is larger than the
	 * old), then grow to cap */
	if (cap > (old->size >> b->tshift))
		newsize = cap << shift;
	else
		newsize = (old->size >> b->tshift) << shift;
	if (b->twidth == width) {
		if (newsize <= old->size) {
			/* nothing to do */
			if (cap > b->batCapacity)
				BATsetcapacity(b, cap);
			return GDK_SUCCEED;
		}
		return BATextend(b, newsize >> shift);
	}

	n = MIN(ncopy, old->size >> b->tshift);

	MT_thread_setalgorithm(n ? "widen offset heap" : "widen empty offset heap");

	new = GDKmalloc(sizeof(Heap));
	if (new == NULL)
		return GDK_FAIL;
	*new = (Heap) {
		.farmid = old->farmid,
		.dirty = true,
		.parentid = old->parentid,
		.wasempty = old->wasempty,
		.refs = ATOMIC_VAR_INIT(1 | (ATOMIC_GET(&old->refs) & HEAPREMOVE)),
	};
	settailname(new, BBP_physical(b->batCacheid), b->ttype, width);
	if (HEAPalloc(new, newsize, 1) != GDK_SUCCEED) {
		GDKfree(new);
		return GDK_FAIL;
	}
	/* HEAPalloc initialized .free, so we need to set it after */
	new->free = old->free << (shift - b->tshift);
	/* per the above, width > b->twidth, so certain combinations are
	 * impossible */
	switch (width) {
	case 2:
		ps = (uint16_t *) new->base;
		switch (b->twidth) {
		case 1:
			pc = (uint8_t *) old->base;
			for (i = 0; i < n; i++)
				ps[i] = pc[i];
			break;
		default:
			MT_UNREACHABLE();
		}
#ifndef NDEBUG
		/* valgrind */
		memset(ps + n, 0, new->size - n * 2);
#endif
		break;
	case 4:
		pi = (uint32_t *) new->base;
		switch (b->twidth) {
		case 1:
			pc = (uint8_t *) old->base;
			for (i = 0; i < n; i++)
				pi[i] = pc[i] + GDK_VAROFFSET;
			break;
		case 2:
			ps = (uint16_t *) old->base;
			for (i = 0; i < n; i++)
				pi[i] = ps[i] + GDK_VAROFFSET;
			break;
		default:
			MT_UNREACHABLE();
		}
#ifndef NDEBUG
		/* valgrind */
		memset(pi + n, 0, new->size - n * 4);
#endif
		break;
#if SIZEOF_VAR_T == 8
	case 8:
		pl = (uint64_t *) new->base;
		switch (b->twidth) {
		case 1:
			pc = (uint8_t *) old->base;
			for (i = 0; i < n; i++)
				pl[i] = pc[i] + GDK_VAROFFSET;
			break;
		case 2:
			ps = (uint16_t *) old->base;
			for (i = 0; i < n; i++)
				pl[i] = ps[i] + GDK_VAROFFSET;
			break;
		case 4:
			pi = (uint32_t *) old->base;
			for (i = 0; i < n; i++)
				pl[i] = pi[i];
			break;
		default:
			MT_UNREACHABLE();
		}
#ifndef NDEBUG
		/* valgrind */
		memset(pl + n, 0, new->size - n * 8);
#endif
		break;
#endif
	default:
		MT_UNREACHABLE();
	}
	MT_lock_set(&b->theaplock);
	b->tshift = shift;
	b->twidth = width;
	if (cap > BATcapacity(b))
		BATsetcapacity(b, cap);
	b->theap = new;
	if (BBP_status(bid) & (BBPEXISTING|BBPDELETED) && b->oldtail == NULL) {
		b->oldtail = old;
		if ((ATOMIC_OR(&old->refs, DELAYEDREMOVE) & HEAPREFS) == 1) {
			/* we have the only reference, we can free the
			 * memory */
			HEAPfree(old, false);
		}
	} else {
		ValPtr p = BATgetprop_nolock(b, (enum prop_t) 20);
		HEAPdecref(old, p == NULL || strcmp(((Heap*) p->val.pval)->filename, old->filename) != 0);
	}
	MT_lock_unset(&b->theaplock);
	return GDK_SUCCEED;
}

/*
 * @- HEAPcopy
 * simple: alloc and copy. Notice that we suppose a preallocated
 * dst->filename (or NULL), which might be used in HEAPalloc().
 */
gdk_return
HEAPcopy(Heap *dst, Heap *src, size_t offset)
{
	if (offset > src->free)
		offset = src->free;
	if (HEAPalloc(dst, src->free - offset, 1) == GDK_SUCCEED) {
		dst->free = src->free - offset;
		memcpy(dst->base, src->base + offset, src->free - offset);
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
		if (h->farmid == 1 && (h->storage == STORE_MEM || h->storage == STORE_MMAP || h->storage == STORE_PRIV)) {
			QryCtx *qc = MT_thread_get_qry_ctx();
			if (qc)
				ATOMIC_SUB(&qc->datasize, h->size);
		}
		if (h->storage == STORE_MEM) {	/* plain memory */
			TRC_DEBUG(HEAP, "HEAPfree %s %zu %p\n", h->filename, h->size, h->base);
			GDKfree(h->base);
		} else if (h->storage == STORE_CMEM) {
			//heap is stored in regular C memory rather than GDK memory,so we call free()
			free(h->base);
		} else if (h->storage != STORE_NOWN) {	/* mapped file, or STORE_PRIV */
			gdk_return ret = GDKmunmap(h->base,
						   h->storage == STORE_PRIV ?
						   MMAP_COPY | MMAP_READ | MMAP_WRITE :
						   MMAP_READ | MMAP_WRITE,
						   h->size);

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
	if (rmheap && !GDKinmemory(h->farmid)) {
		char path[MAXPATH];

		if (h->hasfile) {
			if (GDKfilepath(path, sizeof(path), h->farmid, BATDIR, h->filename, NULL) == GDK_SUCCEED) {
				int ret = MT_remove(path);
				if (ret == -1) {
					/* unexpectedly not present */
					perror(path);
				}
				assert(ret == 0);
				h->hasfile = false;
			}
			if (GDKfilepath(path, sizeof(path), h->farmid, BATDIR, h->filename, "new") == GDK_SUCCEED) {
				/* in practice, should never be present */
				int ret = MT_remove(path);
				if (ret == -1 && errno != ENOENT)
					perror(path);
				assert(ret == -1 && errno == ENOENT);
			}
#ifndef NDEBUG
		} else if (GDKfilepath(path, sizeof(path), h->farmid, BATDIR, h->filename, NULL) == GDK_SUCCEED) {
			/* should not be present */
			struct stat st;
			assert(stat(path, &st) == -1 && errno == ENOENT);
#endif
		}
	}
}

void
HEAPdecref(Heap *h, bool remove)
{
	if (remove)
		ATOMIC_OR(&h->refs, HEAPREMOVE);
	ATOMIC_BASE_TYPE refs = ATOMIC_DEC(&h->refs);
	//printf("dec ref(%d) %p %d\n", (int)h->refs, h, h->parentid);
	switch (refs & HEAPREFS) {
	case 0:
		HEAPfree(h, (bool) (refs & HEAPREMOVE));
		GDKfree(h);
		break;
	case 1:
		if (refs & DELAYEDREMOVE) {
			/* only reference left is b->oldtail */
			HEAPfree(h, false);
		}
		break;
	default:
		break;
	}
}

void
HEAPincref(Heap *h)
{
	//printf("inc ref(%d) %p %d\n", (int)h->refs, h, h->parentid);
	ATOMIC_INC(&h->refs);
}

/*
 * @- HEAPload
 *
 * If we find file X.new, we move it over X (if present) and open it.
 */
gdk_return
HEAPload(Heap *h, const char *nme, const char *ext, bool trunc)
{
	size_t minsize;
	int ret = 0;
	char srcpath[MAXPATH], dstpath[MAXPATH];
	lng t0;
	const char suffix[] = ".new";

	if (h->storage == STORE_INVALID || h->newstorage == STORE_INVALID) {
		size_t allocated;
		h->storage = h->newstorage = h->size < (h->farmid == 0 ? GDK_mmap_minsize_persistent : GDK_mmap_minsize_transient) &&
			(allocated = GDKmem_cursize()) < GDK_mem_maxsize &&
			h->size < ((GDK_mem_maxsize - allocated) >> 6) ? STORE_MEM : STORE_MMAP;
	}

	minsize = (h->size + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
	if (h->storage != STORE_MEM && minsize != h->size)
		h->size = minsize;

	/* when a bat is made read-only, we can truncate any unused
	 * space at the end of the heap */
	if (trunc) {
		/* round up mmap heap sizes to GDK_mmap_pagesize
		 * segments, also add some slack */
		int fd;

		if (minsize == 0)
			minsize = GDK_mmap_pagesize; /* minimum of one page */
		if ((fd = GDKfdlocate(h->farmid, nme, "rb+", ext)) >= 0) {
			struct stat stb;
			if (fstat(fd, &stb) == 0 &&
			    stb.st_size > (off_t) minsize) {
				ret = ftruncate(fd, minsize);
				TRC_DEBUG(HEAP,
					  "ftruncate(file=%s.%s, size=%zu) = %d\n",
					  nme, ext, minsize, ret);
				if (ret == 0) {
					h->size = minsize;
				}
			}
			close(fd);
		}
	}

	TRC_DEBUG(HEAP, "%s%s%s,storage=%d,free=%zu,size=%zu\n",
		  nme, ext ? "." : "", ext ? ext : "",
		  (int) h->storage, h->free, h->size);

	/* On some OSs (WIN32,Solaris), it is prohibited to write to a
	 * file that is open in MAP_PRIVATE (FILE_MAP_COPY) solution:
	 * we write to a file named .ext.new.  This file, if present,
	 * takes precedence. */
	if (GDKfilepath(dstpath, sizeof(dstpath), h->farmid, BATDIR, nme, ext) != GDK_SUCCEED)
		return GDK_FAIL;
	strconcat_len(srcpath, sizeof(srcpath), dstpath, suffix, NULL);

	t0 = GDKusec();
	ret = MT_rename(srcpath, dstpath);
	TRC_DEBUG(HEAP, "rename %s %s = %d %s ("LLFMT"usec)\n",
		  srcpath, dstpath, ret, ret < 0 ? GDKstrerror(errno, (char[128]){0}, 128) : "",
		  GDKusec() - t0);

#ifdef SIZE_CHECK_IN_HEAPS_ONLY
	if (GDKvm_cursize() + h->size >= GDK_vm_maxsize &&
	    !MT_thread_override_limits()) {
		GDKerror("allocating too much memory (current: %zu, requested: %zu, limit: %zu)\n", GDKvm_cursize(), h->size, GDK_vm_maxsize);
		return GDK_FAIL;
	}
#endif

	size_t size = h->size;
	QryCtx *qc = NULL;
	if (h->storage != STORE_MEM)
		size = (size + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
	if (h->farmid == 1 && (qc = MT_thread_get_qry_ctx()) != NULL) {
		ATOMIC_BASE_TYPE sz = 0;
		sz = ATOMIC_ADD(&qc->datasize, size);
		sz += h->size;
		if (qc->maxmem > 0 && sz > qc->maxmem) {
			ATOMIC_SUB(&qc->datasize, size);
			GDKerror("Query using too much memory.\n");
			return GDK_FAIL;
		}
	}
	h->dirty = false;	/* we're about to read it, so it's clean */
	if (h->storage == STORE_MEM && h->free == 0) {
		h->base = GDKmalloc(h->size);
		h->wasempty = true;
	} else {
		if (h->free == 0) {
			int fd = GDKfdlocate(h->farmid, nme, "wb", ext);
			if (fd >= 0)
				close(fd);
			h->wasempty = true;
		}
		h->base = GDKload(h->farmid, nme, ext, h->free, &h->size, h->storage);
	}
	if (h->base == NULL) {
		if (qc != NULL)
			ATOMIC_SUB(&qc->datasize, size);
		return GDK_FAIL; /* file could  not be read satisfactorily */
	}

	return GDK_SUCCEED;
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
gdk_return
HEAPsave(Heap *h, const char *nme, const char *ext, bool dosync, BUN free, MT_Lock *lock)
{
	storage_t store = h->newstorage;
	long_str extension;
	gdk_return rc;
	const char suffix[] = ".new";

	if (h->base == NULL) {
		GDKerror("no heap to save\n");
		return GDK_FAIL;
	}
	if (free == 0) {
		/* nothing to see, please move on */
		if (lock)
			MT_lock_set(lock);
		h->wasempty = true;
		if (lock)
			MT_lock_unset(lock);
		TRC_DEBUG(HEAP,
			  "not saving: "
			  "(%s.%s,storage=%d,free=%zu,size=%zu,dosync=%s)\n",
			  nme?nme:"", ext, (int) h->newstorage, free, h->size,
			  dosync?"true":"false");
		return GDK_SUCCEED;
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
		  nme?nme:"", ext, (int) h->newstorage, free, h->size,
		  dosync?"true":"false");
	if (lock)
		MT_lock_set(lock);
	if (free == h->free)
		h->dirty = false;
	if (lock)
		MT_lock_unset(lock);
	rc = GDKsave(h->farmid, nme, ext, h->base, free, store, dosync);
	if (lock)
		MT_lock_set(lock);
	if (rc == GDK_SUCCEED) {
		h->hasfile = true;
		h->wasempty = false;
	} else {
		h->dirty = true;
		if (store != STORE_MMAP)
			h->hasfile = false;
	}
	if (lock)
		MT_lock_unset(lock);
	return rc;
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

	heap->dirty = true;
}

gdk_return
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
			return GDK_FAIL;
		heap->free = heap->size;
	}

	/* initialize heap as empty */
	HEAP_empty(heap, nprivate, alignment);
	return GDK_SUCCEED;
}


var_t
HEAP_malloc(BAT *b, size_t nbytes)
{
	Heap *heap = b->tvheap;
	size_t block, trail, ttrail;
	CHUNK *blockp;
	CHUNK *trailp;
	HEADER *hheader = HEAP_index(heap, 0, HEADER);

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

		assert(trail == 0 || block > trail);
		if (trail != 0 && block <= trail) {
			GDKerror("Free list is not orderered\n");
			return (var_t) -1;
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
#if SIZEOF_SIZE_T == 4
		newsize = MIN(heap->free, (size_t) 1 << 20);
#else
		newsize = MIN(heap->free, (size_t) 1 << 30);
#endif
		newsize = (size_t) roundup_8(heap->free + MAX(newsize, nbytes));
		assert(heap->free <= VAR_MAX);
		block = (size_t) heap->free;	/* current end-of-heap */

		/* Increase the size of the heap. */
		TRC_DEBUG(HEAP, "HEAPextend in HEAP_malloc %s %zu %zu\n", heap->filename, heap->size, newsize);
		if (HEAPgrow(&b->tvheap, newsize, false) != GDK_SUCCEED) {
			return (var_t) -1;
		}
		heap = b->tvheap;
		heap->free = newsize;
		hheader = HEAP_index(heap, 0, HEADER);

		blockp = HEAP_index(heap, block, CHUNK);
		trailp = HEAP_index(heap, trail, CHUNK);

		blockp->next = 0;
		assert(heap->free - block <= VAR_MAX);
		blockp->size = (size_t) (heap->free - block);	/* determine size of allocated block */

		/* Try to join the last block in the freelist and the
		 * newly allocated memory */
		if ((trail != 0) && (trail + trailp->size == block)) {
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

	heap->dirty = true;

	block += hheader->alignment;
	return (var_t) block;
}

void
HEAP_free(Heap *heap, var_t mem)
{
/* we cannot free pieces of the heap because we may have used
 * append_varsized_bat to create the heap; if we did, there may be
 * multiple locations in the offset heap that refer to the same entry in
 * the vheap, so we cannot free any until we free all */
	(void) heap;
	(void) mem;
#if 0
	HEADER *hheader = HEAP_index(heap, 0, HEADER);
	CHUNK *beforep;
	CHUNK *blockp;
	CHUNK *afterp;
	size_t after, before, block = mem;

	assert(hheader->alignment == 8 || hheader->alignment == 4);
	if (hheader->alignment != 8 && hheader->alignment != 4) {
		GDKerror("Heap structure corrupt\n");
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
#endif
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
			if (!(ATOMIC_GET(&GDKdebug) & NOSYNCMASK))
				(void) MT_msync(h->base, dirty);
			else
				h->dirty = true;
		} else
			h->dirty = true;
	}
}
