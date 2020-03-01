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

static Heap * 
BATmosaic_heap(BAT *bn, BUN cap, const char *ext)
{
    const char *nme;
	Heap *m =  NULL;
	char *fname = 0;

    nme = BBP_physical(bn->batCacheid);
    if ( (m = (Heap*)GDKzalloc(sizeof(Heap))) == NULL ||
		(m->farmid = BBPselectfarm(bn->batRole, bn->ttype, mosaicheap)) < 0 ||
        (fname = GDKfilepath(NOFARM, NULL, nme, ext)) == NULL){
			if( fname)
				GDKfree(fname);
			GDKfree(m);
			return NULL;
	}

	if (strlen(fname) >= sizeof(m->filename)) {
		/* TODO: check if this can actually happen.*/
		GDKfree(fname);
		GDKfree(m);
		return NULL;
	}

	strcpy(m->filename, fname);
	GDKfree(fname);

    if( HEAPalloc(m, cap, Tsize(bn)) != GDK_SUCCEED)
        return NULL;
    return m;
}

static int 
MOS_sync(BAT *bn) {
    Heap *hp;
    int fd = -1, err = 0;

	if (!((BBP_status(bn->batCacheid) & BBPEXISTING) && bn->batInserted == bn->batCount)) {
		ALGODEBUG fprintf(stderr, "#BAT NOT persisting index %d\n", bn->batCacheid);
		return err;
	}

	/*TODO: This part is normally - e.g. imprints & hash - done in a different thread, look into this.*/
	/*only for large ones  and when there is no concurrency: MT_create_thread(&tid, <some-mosaic-specific-sync-function>, bn, MT_THR_DETACHED);*/
	BBPfix(bn->batCacheid);

    hp = bn->tmosaic;
    if (HEAPsave(hp, hp->filename, NULL, true) != GDK_SUCCEED ||
        (fd = GDKfdlocate(hp->farmid, hp->filename, "rb+", NULL)) < 0) {
        GDKfree(hp);
		err = 1;
		bn->tmosaic = NULL;
    } else {
		((oid *) hp->base)[0] |= (oid) 1 << 24;
		if (write(fd, hp->base, SIZEOF_SIZE_T) < 0)
			GDKerror("write mosaic heap failed");
		else 
			MOSsync(fd);
	}
	if( fd >= 0){
		close(fd);
		fd = -1;
	}

    hp = bn->tvmosaic;
    if (HEAPsave(hp, hp->filename, NULL, true) != GDK_SUCCEED ||
        (fd = GDKfdlocate(hp->farmid, hp->filename, "rb+", NULL)) < 0) {
        GDKfree(hp);
		err = 1;
    } else {
		((oid *) hp->base)[0] |= (oid) 1 << 24;
		if (write(fd, hp->base, SIZEOF_SIZE_T) < 0)
			GDKerror("write vmosaic heap failed");
		else
			MOSsync(fd);
	}
	if( fd >= 0)
		close(fd);
    BBPunfix(bn->batCacheid);
	return err;
}

static Heap *
MOScheck_heap(BAT *b, const char *ext){
	Heap *hp;
	const char *nme = BBP_physical(b->batCacheid);
	int fd;

	if ((hp = GDKzalloc(sizeof(*hp))) != NULL &&
		(hp->farmid = BBPselectfarm(b->batRole, b->ttype, mosaicheap)) >= 0 ){

		sprintf(hp->filename, "%s.%s", nme, ext);

		/* check whether a persisted mosaic-specific heap can be found */
		if ((fd = GDKfdlocate(hp->farmid, nme, "rb+", ext)) >= 0) {
			struct stat st;
			int hdata;

			if (BATcount(b) > 0 && read(fd, &hdata, sizeof(hdata)) == sizeof(hdata) &&
				hdata == MOSAIC_VERSION &&
				fstat(fd, &st) == 0 &&
				st.st_size >= (off_t) (hp->size = hp->free = (oid) BATcount(b) * SIZEOF_OID) &&
				HEAPload(hp, nme, ext, false) == GDK_SUCCEED) {
					close(fd);
					ALGODEBUG fprintf(stderr, "#BATcheckmosaic %s: reusing persisted heap %d\n", ext, b->batCacheid);
					return hp;
				}
			close(fd);
			/* unlink unusable file */
			GDKunlink(hp->farmid, BATDIR, nme, ext);
		}
		GDKfree(hp->filename);
	}
	GDKfree(hp);
	GDKclrerr();	/* we're not currently interested in errors */
	return NULL;
}

static int
MOScheck(BAT *b)
{
    if (VIEWtparent(b)){
        b = BBPdescriptor(VIEWtparent(b));
	}

	if (b->tmosaic == (Heap *) 1) 
		b->tmosaic = MOScheck_heap(b, "mosaic");
	
	/* a vmosaic can only exist if the mosaic exists as well */
	if (b->tvmosaic == (Heap *) 1) 
		b->tvmosaic = MOScheck_heap(b, "vmosaic");
	return b->tmosaic != NULL;
}

void
MOSdestroy(BAT *bn) {
	Heap *h;
	/* If there is a view then don't drop the mosaic. However, not needed because we don;t slice over the tmosaic BAT (yet) */
    if (!bn || VIEWtparent(bn)) 
		return;
	if (bn->tvmosaic){
		h= bn->tvmosaic;
		if( HEAPdelete(h, BBP_physical(bn->batCacheid), "vmosaic"))
			GDKerror("MOSdestroy vmosaic failed");
		bn->tvmosaic = NULL;
		GDKfree(h);
    }
	if (bn->tmosaic){
		h= bn->tmosaic;
		if( HEAPdelete(h, BBP_physical(bn->batCacheid), "mosaic"))
			GDKerror("MOSdestroy mosaic failed");
		bn->tmosaic = NULL;
		GDKfree(h);
    }
}

int
BATcheckmosaic(BAT *bn) {
	/* A dictionary vmosaic is dependent on mosaic heap, no need to check */
	return MOScheck(bn);
}

gdk_return
BATmosaic(BAT *b, BUN cap) {
	Heap *m = NULL;
	if( cap < 128)
		cap = 128;

	if (b->tmosaic == NULL && (m = BATmosaic_heap(b, cap, "mosaic")) ) {
		m->parentid = b->batCacheid;
		b->tmosaic =  m;
	} else
		return GDK_FAIL;

	if (b->tvmosaic == NULL && (m = BATmosaic_heap(b, 128 /*start with a small dictionary*/, "vmosaic")) ) {
		m->parentid = b->batCacheid;
		b->tvmosaic =  m;
	} else {
		MOSdestroy(b);
		return GDK_FAIL;
	}
#ifdef PERSISTENTMOSAIC
	MOS_sync(b);
#endif
	b->batDirtydesc = TRUE;
	return GDK_SUCCEED;
}

void
MOSvirtualize(BAT *bn) {
	virtualize(bn);
}
