/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 *  M. L. Kersten
 * Functions for the mosaic code base
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

gdk_return
MOSalloc(BAT *bn, BUN cap)
{
    const char *nme = BBP_physical(bn->batCacheid);

    if ( (bn->tmosaic = (Heap*)GDKzalloc(sizeof(Heap))) == NULL ||
        (bn->tmosaic->filename = GDKfilepath(NOFARM, NULL, nme, "mosaic")) == NULL)
        return GDK_FAIL;
	
    if( HEAPalloc(bn->tmosaic, cap, Tsize(bn)) != GDK_SUCCEED)
        return GDK_FAIL;
    bn->tmosaic->parentid = bn->batCacheid;
    bn->tmosaic->farmid = BBPselectfarm(bn->batRole, bn->ttype, varheap);
    return GDK_SUCCEED;
}

void
MOSdestroy(BAT *bn)
{	Heap *h;
	
	if( bn && bn->tmosaic && !VIEWtparent(bn)){
		h= bn->tmosaic;
		bn->tmosaic = NULL;
		if( HEAPdelete(h, BBP_physical(bn->batCacheid), "mosaic"))
			IODEBUG fprintf(stderr,"#MOSdestroy (%s) failed", BATgetId(bn));
		bn->tmosaic = NULL;
		GDKfree(h);
	}
}
/* return TRUE if we have a mosaic on the tail, even if we need to read
 * one from disk */
int
BATcheckmosaic(BAT *b)
{
	int ret;
	lng t;

	assert(b->batCacheid > 0);
	t = GDKusec();
	MT_lock_set(&GDKhashLock(abs(b->batCacheid)));
	t = GDKusec() - t;
	if (b->tmosaic == NULL) {
		Heap *hp;
		const char *nme = BBP_physical(b->batCacheid);
		const char *ext = "mosaic";
		int fd;

		if ((hp = GDKzalloc(sizeof(*hp))) != NULL &&
		    (hp->farmid = BBPselectfarm(b->batRole, b->ttype, mosaicheap)) >= 0 &&
		    (hp->filename = GDKmalloc(strlen(nme) + 10)) != NULL) {
			sprintf(hp->filename, "%s.%s", nme, ext);

			/* check whether a persisted mosaic can be found */
			if ((fd = GDKfdlocate(hp->farmid, nme, "rb+", ext)) >= 0) {
				struct stat st;
				int hdata;

				if (BATcount(b) > 0 && read(fd, &hdata, sizeof(hdata)) == sizeof(hdata) &&
					hdata == MOSAIC_VERSION &&
				    fstat(fd, &st) == 0 &&
				    st.st_size >= (off_t) (hp->size = hp->free = (oid) BATcount(b) * SIZEOF_OID) &&
				    HEAPload(hp, nme, ext, 0) == GDK_SUCCEED) {
					close(fd);
					b->tmosaic = hp;
					ALGODEBUG fprintf(stderr, "#BATcheckmosaic: reusing persisted mosaic %d\n", b->batCacheid);
					MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
					return 1;
				}
				close(fd);
				/* unlink unusable file */
				GDKunlink(hp->farmid, BATDIR, nme, ext);
			}
			GDKfree(hp->filename);
		}
		GDKfree(hp);
		GDKclrerr();	/* we're not currently interested in errors */
	}
	ret = b->tmosaic != NULL;
	MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
	ALGODEBUG if (ret) fprintf(stderr, "#BATcheckmosaic: already has mosaic %d, waited " LLFMT " usec\n", b->batCacheid, t);
	return ret;
}
