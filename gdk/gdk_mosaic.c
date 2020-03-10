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

#define EXT(HEAP) ("t" STRINGIFY(HEAP))

#define HEAP mosaic
#include "gdk_mosaic_templates.h"
#undef HEAP
#define HEAP vmosaic
#include "gdk_mosaic_templates.h"
#undef HEAP

void
MOSdestroy(BAT *bn) {
	MT_lock_set(&bn->batIdxLock);
	MOSdestroy_mosaic(bn);
	MOSdestroy_vmosaic(bn);
	MT_lock_unset(&bn->batIdxLock);
}

void
MOSfree(BAT *bn) {
	MT_lock_set(&bn->batIdxLock);
	MOSfree_mosaic(bn);
	MOSfree_vmosaic(bn);
	MT_lock_unset(&bn->batIdxLock);
}


int
BATcheckmosaic(BAT *bn) {
	MT_lock_set(&bn->batIdxLock);
	bool r1 = MOScheck_mosaic(bn);
	bool r2 = MOScheck_vmosaic(bn);
	MT_lock_unset(&bn->batIdxLock);
	return r1 && r2;
}

gdk_return
BATmosaic(BAT *b, BUN cap) {
	MT_lock_set(&b->batIdxLock);
	if (b->tmosaic != NULL || ( (b->tmosaic = create_mosaic_heap(b, cap)) == NULL) ) {
		MT_lock_unset(&b->batIdxLock);
		return GDK_FAIL;
	}
	b->tmosaic->parentid = b->batCacheid;
	if (b->tvmosaic != NULL || ( (b->tvmosaic = create_vmosaic_heap(b, 128)) == NULL) ) {
		MOSdestroy_mosaic(b);
		MT_lock_unset(&b->batIdxLock);
		return GDK_FAIL;
	}
	b->tvmosaic->parentid = b->batCacheid;
	MT_lock_unset(&b->batIdxLock);
	return GDK_SUCCEED;
}

void 
MOSpersist(BAT *b) {
	persist_mosaic(b);
	persist_vmosaic(b);
}

void
MOSvirtualize(BAT *bn) {
	virtualize(bn);
}
