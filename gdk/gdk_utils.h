/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _GDK_UTILS_H_
#define _GDK_UTILS_H_

#include <setjmp.h>

gdk_export const char *GDKgetenv(const char *name);

gdk_export bool GDKgetenv_istext(const char *name, const char* text);
gdk_export bool GDKgetenv_isyes(const char *name);
gdk_export bool GDKgetenv_istrue(const char *name);

gdk_export int GDKgetenv_int(const char *name, int def);

gdk_export gdk_return GDKsetenv(const char *name, const char *value);
gdk_export gdk_return GDKcopyenv(BAT **key, BAT **val, bool writable);

/*
 * @+ Memory management
 * Memory management in GDK mostly relies on the facilities offered by
 * the underlying OS.  The below routines monitor the available memory
 * resources which consist of physical swap space and logical vm
 * space.  There are three kinds of memory, that affect these two
 * resources in different ways:
 *
 * - memory mapping
 *   which ask for a logical region of virtual memory space.  In
 *   principle, no physical memory is needed to keep the system afloat
 *   here, as the memory mapped file is swapped onto a disk object
 *   that already exists.
 *
 *   Actually, there are two kings of memory mapping used in GDK,
 *   namely read-only direct mapped and writable copy-on write. For
 *   the dirty pages, the latter actually also consumes physical
 *   memory resources, but that is ignored here for simplicity.
 *
 * - anonymous virtual memory
 *   This is virtual memory that is mapped on the swap file. Hence,
 *   this consumes both logical VM space resources and physical memory
 *   space.
 *
 * - malloced memory
 *   comes from the heap and directly consumes physical memory
 *   resources.
 *
 * The malloc routine checks the memory consumption every 1000 calls,
 * or for calls larger that 50000 bytes. Consequently, at least every
 * 50MB increase, alloc memory is checked. The VM calls always check
 * the memory consumption.
 */
/* default setting to administer everything */
#define GDK_MEM_NULLALLOWED

#if SIZEOF_VOID_P==8
#define GDK_VM_MAXSIZE	LL_CONSTANT(4398046511104)	/* :-) a 64-bit OS: 4TB */
#else
#define GDK_VM_MAXSIZE	LL_CONSTANT(1610612736)	/* :-| a 32-bit OS: 1.5GB */
#endif
/* virtual memory defines */
gdk_export size_t _MT_npages;
gdk_export size_t _MT_pagesize;

#define MT_pagesize()	_MT_pagesize
#define MT_npages()	_MT_npages

gdk_export size_t GDK_mem_maxsize;	/* max allowed size of committed memory */
gdk_export size_t GDK_vm_maxsize;	/* max allowed size of reserved vm */

gdk_export void *GDKmmap(const char *path, int mode, size_t len)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return GDKmunmap(void *addr, size_t len);

gdk_export size_t GDKmem_cursize(void);	/* RAM/swapmem that MonetDB has claimed from OS */
gdk_export size_t GDKvm_cursize(void);	/* current MonetDB VM address space usage */

gdk_export void *GDKmalloc(size_t size)
	__attribute__((__malloc__))
	__attribute__((__alloc_size__(1)))
	__attribute__((__warn_unused_result__));
gdk_export void *GDKzalloc(size_t size)
	__attribute__((__malloc__))
	__attribute__((__alloc_size__(1)))
	__attribute__((__warn_unused_result__));
gdk_export void *GDKrealloc(void *pold, size_t size)
	__attribute__((__alloc_size__(2)))
	__attribute__((__warn_unused_result__));
gdk_export void GDKfree(void *blk);
gdk_export str GDKstrdup(const char *s)
	__attribute__((__malloc__))
	__attribute__((__warn_unused_result__));
gdk_export str GDKstrndup(const char *s, size_t n)
	__attribute__((__malloc__))
	__attribute__((__warn_unused_result__));
gdk_export size_t GDKmallocated(const void *s);

gdk_export void MT_init(void);	/*  init the package. */
struct opt;
gdk_export gdk_return GDKinit(struct opt *set, int setlen, bool embedded);

/* used for testing only */
gdk_export void GDKsetmallocsuccesscount(lng count);

/*
 * Upon closing the session, all persistent BATs should be saved and
 * the transient BATs should be removed.  The buffer pool manager
 * takes care of this.
 */
gdk_export void GDKexit(int status);
gdk_export bool GDKexiting(void);

gdk_export void GDKprepareExit(void);
gdk_export void GDKreset(int status);
/* global version number */
gdk_export const char *GDKversion(void)
	__attribute__((__const__));
/* ABI version of GDK library */
gdk_export const char *GDKlibversion(void)
	__attribute__((__const__));

// these are used in embedded mode to jump out of GDKfatal
gdk_export jmp_buf GDKfataljump;
gdk_export str GDKfatalmsg;
gdk_export bit GDKfataljumpenable;

/* Timers
 * The following relative timers are available for inspection.
 * Note that they may consume recognizable overhead.
 *
 */
gdk_export lng GDKusec(void);
gdk_export int GDKms(void);


#if !defined(NDEBUG) && !defined(__COVERITY__)
/* In debugging mode, replace GDKmalloc and other functions with a
 * version that optionally prints calling information.
 *
 * We have two versions of this code: one using a GNU C extension, and
 * one using traditional C.  The GNU C version also prints the name of
 * the calling function.
 */
#ifdef __GNUC__
#define GDKmalloc(s)						\
	({							\
		size_t _size = (s);				\
		void *_res = GDKmalloc(_size);			\
		TRC_DEBUG(ALLOC, "GDKmalloc(%zu) -> %p\n",	\
			  _size, _res);				\
		_res;						\
	})
#define GDKzalloc(s)						\
	({							\
		size_t _size = (s);				\
		void *_res = GDKzalloc(_size);			\
		TRC_DEBUG(ALLOC, "GDKzalloc(%zu) -> %p\n",	\
			  _size, _res);				\
		_res;						\
	})
#define GDKrealloc(p, s)					\
	({							\
		void *_ptr = (p);				\
		size_t _size = (s);				\
		void *_res = GDKrealloc(_ptr, _size);		\
		TRC_DEBUG(ALLOC, "GDKrealloc(%p,%zu) -> %p\n",	\
			  _ptr, _size, _res);			\
		_res;						\
	 })
#define GDKfree(p)							\
	({								\
		void *_ptr = (p);					\
		if (_ptr)						\
			TRC_DEBUG(ALLOC, "GDKfree(%p)\n", _ptr);	\
		GDKfree(_ptr);						\
	})
#define GDKstrdup(s)							\
	({								\
		const char *_str = (s);					\
		void *_res = GDKstrdup(_str);				\
		TRC_DEBUG(ALLOC, "GDKstrdup(len=%zu) -> %p\n",		\
			  _str ? strlen(_str) : 0, _res);		\
		_res;							\
	})
#define GDKstrndup(s, n)					\
	({							\
		const char *_str = (s);				\
		size_t _n = (n);				\
		void *_res = GDKstrndup(_str, _n);		\
		TRC_DEBUG(ALLOC, "GDKstrndup(len=%zu) -> %p\n", \
			  _n,	_res);				\
		_res;						\
	})
#define GDKmmap(p, m, l)						\
	({								\
		const char *_path = (p);				\
		int _mode = (m);					\
		size_t _len = (l);					\
		void *_res = GDKmmap(_path, _mode, _len);		\
		TRC_DEBUG(ALLOC, "GDKmmap(%s,0x%x,%zu) -> %p\n",	\
			  _path ? _path : "NULL",			\
			  (unsigned) _mode, _len,			\
			  _res);					\
		_res;							\
	 })
#define GDKmunmap(p, l)						\
	({	void *_ptr = (p);				\
		size_t _len = (l);				\
		gdk_return _res = GDKmunmap(_ptr, _len);	\
		TRC_DEBUG(ALLOC,				\
			  "GDKmunmap(%p,%zu) -> %u\n",		\
			  _ptr, _len, _res);			\
		_res;						\
	})
#define malloc(s)					\
	({						\
		size_t _size = (s);			\
		void *_res = malloc(_size);		\
		TRC_DEBUG(ALLOC, "malloc(%zu) -> %p\n", \
			  _size, _res);			\
		_res;					\
	})
#define calloc(n, s)						\
	({							\
		size_t _nmemb = (n);				\
		size_t _size = (s);				\
		void *_res = calloc(_nmemb,_size);		\
		TRC_DEBUG(ALLOC, "calloc(%zu,%zu) -> %p\n",	\
			  _nmemb, _size, _res);			\
		_res;						\
	})
#define realloc(p, s)						\
	({							\
		void *_ptr = (p);				\
		size_t _size = (s);				\
		void *_res = realloc(_ptr, _size);		\
		TRC_DEBUG(ALLOC, "realloc(%p,%zu) -> %p\n",	\
			  _ptr, _size, _res);			\
		_res;						\
	 })
#define free(p)						\
	({						\
		void *_ptr = (p);			\
		TRC_DEBUG(ALLOC, "free(%p)\n", _ptr);	\
		free(_ptr);				\
	})
#else
static inline void *
GDKmalloc_debug(size_t size)
{
	void *res = GDKmalloc(size);
	TRC_DEBUG(ALLOC, "GDKmalloc(%zu) -> %p\n", size, res);
	return res;
}
#define GDKmalloc(s)	GDKmalloc_debug((s))
static inline void *
GDKzalloc_debug(size_t size)
{
	void *res = GDKzalloc(size);
	TRC_DEBUG(ALLOC, "GDKzalloc(%zu) -> %p\n", size, res);
	return res;
}
#define GDKzalloc(s)	GDKzalloc_debug((s))
static inline void *
GDKrealloc_debug(void *ptr, size_t size)
{
	void *res = GDKrealloc(ptr, size);
	TRC_DEBUG(ALLOC, "GDKrealloc(%p,%zu) -> %p\n", ptr, size, res);
	return res;
}
#define GDKrealloc(p, s)	GDKrealloc_debug((p), (s))
static inline void
GDKfree_debug(void *ptr)
{
	TRC_DEBUG(ALLOC, "GDKfree(%p)\n", ptr);
	GDKfree(ptr);
}
#define GDKfree(p)	GDKfree_debug((p))
static inline char *
GDKstrdup_debug(const char *str)
{
	void *res = GDKstrdup(str);
	TRC_DEBUG(ALLOC, "GDKstrdup(len=%zu) -> %p\n",
		  str ? strlen(str) : 0, res);
	return res;
}
#define GDKstrdup(s)	GDKstrdup_debug((s))
static inline char *
GDKstrndup_debug(const char *str, size_t n)
{
	void *res = GDKstrndup(str, n);
	TRC_DEBUG(ALLOC, "GDKstrndup(len=%zu) -> %p\n", n, res);
	return res;
}
#define GDKstrndup(s, n)	GDKstrndup_debug((s), (n))
static inline void *
GDKmmap_debug(const char *path, int mode, size_t len)
{
	void *res = GDKmmap(path, mode, len);
	TRC_DEBUG(ALLOC, "GDKmmap(%s,0x%x,%zu) -> %p\n",
		  path ? path : "NULL", (unsigned) mode, len, res);
	return res;
}
#define GDKmmap(p, m, l)	GDKmmap_debug((p), (m), (l))
static inline gdk_return
GDKmunmap_debug(void *ptr, size_t len)
{
	gdk_return res = GDKmunmap(ptr, len);
	TRC_DEBUG(ALLOC, "GDKmunmap(%p,%zu) -> %d\n",
			   	  ptr, len, (int) res);
	return res;
}
#define GDKmunmap(p, l)		GDKmunmap_debug((p), (l))
static inline void *
malloc_debug(size_t size)
{
	void *res = malloc(size);
	TRC_DEBUG(ALLOC, "malloc(%zu) -> %p\n", size, res);
	return res;
}
#define malloc(s)	malloc_debug((s))
static inline void *
calloc_debug(size_t nmemb, size_t size)
{
	void *res = calloc(nmemb, size);
	TRC_DEBUG(ALLOC, "calloc(%zu,%zu) -> %p\n", nmemb, size, res);
	return res;
}
#define calloc(n, s)	calloc_debug((n), (s))
static inline void *
realloc_debug(void *ptr, size_t size)
{
	void *res = realloc(ptr, size);
	TRC_DEBUG(ALLOC, "realloc(%p,%zu) -> %p \n", ptr, size, res);
	return res;
}
#define realloc(p, s)	realloc_debug((p), (s))
static inline void
free_debug(void *ptr)
{
	TRC_DEBUG(ALLOC, "free(%p)\n", ptr);
	free(ptr);
}
#define free(p)	free_debug((p))
#endif
#endif

#endif /* _GDK_UTILS_H_ */
