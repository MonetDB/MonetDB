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

/* This file should not be included in any file outside of this directory */

#ifndef LIBGDK
#error this file should not be included outside its source directory
#endif

void dump_threads(void)
	__attribute__((__visibility__("hidden")));
void join_detached_threads(void)
	__attribute__((__visibility__("hidden")));
bool MT_kill_threads(void)
	__attribute__((__visibility__("hidden")));
bool MT_thread_override_limits(void)
	__attribute__((__visibility__("hidden")));
#ifdef NATIVE_WIN32
#define GDKwinerror(...)						\
	do {								\
		char _osmsgbuf[128];					\
		FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM, NULL,		\
			      GetLastError(),				\
			      MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), \
			      (LPTSTR) _osmsgbuf, sizeof(_osmsgbuf),	\
			      NULL);					\
		GDKtracer_log(__FILE__, __func__, __LINE__,		\
			      TRC_NAME(M_ERROR), TRC_NAME(GDK),		\
			      _osmsgbuf, __VA_ARGS__);			\
		SetLastError(0);					\
	} while (0)
#endif
#define GDKwarning(...)		TRC_WARNING(GDK, __VA_ARGS__)

struct freebats {
	bat freebats;
	uint32_t nfreebats;
};
struct freebats *MT_thread_getfreebats(void)
	__attribute__((__visibility__("hidden")));

struct allocator {
	size_t size;	 /* size of the allocator in terms of blocks */
	size_t nr;	 /* number of blocks allocated */
	void **blks;
	void *first_blk;
	size_t used; 	 /* memory used in last block */
	size_t usedmem;	 /* total used memory */
	size_t objects;  /* number of objects */
	size_t inuse;    /* number of objects in use*/
	size_t free_obj_hits; /* number of object reuse*/
	void *freelist;	/* first free object */
	size_t frees;
	size_t tmp_used; /* counter for temp usage */

	exception_buffer eb;
	MT_Lock lock;    /* lock for thread-safe allocations */
	bool use_lock;
	char name[MT_NAME_LEN]; /* Name (only for display!) */
};
