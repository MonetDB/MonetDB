/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _GDK_UTILS_H_
#define _GDK_UTILS_H_

#include "monet_options.h"
#include <setjmp.h>

gdk_export BAT *GDKkey;
gdk_export BAT *GDKval;

gdk_export char *GDKgetenv(const char *name);

gdk_export int GDKgetenv_isyes(const char *name);

gdk_export int GDKgetenv_istrue(const char *name);

gdk_export int GDKgetenv_int(const char *name, int def);

gdk_export gdk_return GDKsetenv(const char *name, const char *value);

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

gdk_export void MT_init(void);	/*  init the package. */
gdk_export int GDKinit(opt *set, int setlen);

/* used for testing only */
gdk_export void GDKsetmallocsuccesscount(lng count);

/*
 * Upon closing the session, all persistent BATs should be saved and
 * the transient BATs should be removed.  The buffer pool manager
 * takes care of this.
 */
gdk_export int GDKnr_threads;
#ifndef HAVE_EMBEDDED
__declspec(noreturn) gdk_export void GDKexit(int status)
	__attribute__((__noreturn__));
#else
gdk_export void GDKexit(int status);
#endif
gdk_export int GDKexiting(void);

gdk_export void GDKregister(MT_Id pid);
gdk_export void GDKprepareExit(void);
gdk_export void GDKreset(int status, int exit);
gdk_export const char *GDKversion(void);

gdk_export gdk_return GDKextractParentAndLastDirFromPath(const char *path, char *last_dir_parent, char *last_dir);

// these are used in embedded mode to jump out of GDKfatal
gdk_export jmp_buf GDKfataljump;
gdk_export str GDKfatalmsg;
gdk_export bit GDKfataljumpenable;


#endif /* _GDK_UTILS_H_ */
