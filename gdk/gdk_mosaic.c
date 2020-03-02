/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 *  M. L. Kersten
 * Functions for the mosaic code base
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

void MOSsetLock(BAT* b) {
	MT_lock_set(&b->batIdxLock);
}

void MOSunsetLock(BAT* b) {
	MT_lock_unset(&b->batIdxLock);
}

static inline void
MOSsync(int fd) {
#if defined(NATIVE_WIN32)
        _commit(fd);
#elif defined(HAVE_FDATASYNC)
        fdatasync(fd);
#elif defined(HAVE_FSYNC)
        fsync(fd);
#endif
}

#define HEAP mosaic
#include "gdk_mosaic_templates.h"
#undef HEAP
#define HEAP vmosaic
#include "gdk_mosaic_templates.h"
#undef HEAP

void
MOSdestroy(BAT *bn) {
	MOSdestroy_mosaic(bn);
	MOSdestroy_vmosaic(bn);
}


int
BATcheckmosaic(BAT *bn) {
	return MOScheck_mosaic(bn) && MOScheck_vmosaic(bn);
}

gdk_return
BATmosaic(BAT *b, BUN cap) {
	if (BATmosaic_mosaic(b, cap) != GDK_SUCCEED) {
		return GDK_FAIL;
	}

	if (BATmosaic_vmosaic(b, 128 /*start with a small dictionary*/) != GDK_SUCCEED) {
		MOSdestroy_mosaic(b);
		return GDK_FAIL;
	}

	return GDK_SUCCEED;
}

void
MOSvirtualize(BAT *bn) {
	virtualize(bn);
}
