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

#ifdef PERSISTENTMOSAIC
struct mosaicsync {
    Heap *hp;
    bat id;
    const char *func;
};

static void
BATmosaicsync(void *arg)
{
    struct mosaicsync *hs = arg;
    Heap *hp = hs->hp;
    int fd;
    lng t0 = GDKusec();

    if (HEAPsave(hp, hp->filename, NULL) != GDK_SUCCEED ||
        (fd = GDKfdlocate(hp->farmid, hp->filename, "rb+", NULL)) < 0) {
        BBPunfix(hs->id);
        GDKfree(arg);
        return;
    }
    ((oid *) hp->base)[0] |= (oid) 1 << 24;
    if (write(fd, hp->base, SIZEOF_SIZE_T) < 0)
        perror("write mosaic");
    if (!(GDKdebug & FORCEMITOMASK)) {
#if defined(NATIVE_WIN32)
        _commit(fd);
#elif defined(HAVE_FDATASYNC)
        fdatasync(fd);
#elif defined(HAVE_FSYNC)
        fsync(fd);
#endif
    }
    close(fd);
    BBPunfix(hs->id);
    ALGODEBUG fprintf(stderr, "#%s: persisting mosaic %s (" LLFMT " usec)\n", hs->func, hp->filename, GDKusec() - t0);
    GDKfree(arg);
}
#endif

gdk_return
BATmosaic(BAT *bn, BUN cap)
{
    const char *nme;
	Heap *m;

    MT_lock_set(&GDKmosaicLock(bn->batCacheid));
	if( bn->tmosaic){
		MT_lock_unset(&GDKmosaicLock(bn->batCacheid));
		return GDK_SUCCEED;
	}

    nme = BBP_physical(bn->batCacheid);
    if ( (m = (Heap*)GDKzalloc(sizeof(Heap))) == NULL ||
		(m->farmid = BBPselectfarm(bn->batRole, bn->ttype, varheap)) < 0 ||
        (m->filename = GDKfilepath(NOFARM, NULL, nme, "mosaic")) == NULL){
			if( m)
				GDKfree(m->filename);
			GDKfree(m);
			MT_lock_unset(&GDKmosaicLock(bn->batCacheid));
			return GDK_FAIL;
		}
	
    if( HEAPalloc(m, cap, Tsize(bn)) != GDK_SUCCEED){
		MT_lock_unset(&GDKmosaicLock(bn->batCacheid));
        return GDK_FAIL;
	}
    m->parentid = bn->batCacheid;

#ifdef PERSISTENTMOSAIC
    if ((BBP_status(bn->batCacheid) & BBPEXISTING) && bn->batInserted == bn->batCount) {
        struct mosaicsync *hs = GDKzalloc(sizeof(*hs));
        if (hs != NULL) {
            BBPfix(bn->batCacheid);
            hs->id = bn->batCacheid;
            hs->hp = m;
            hs->func = "BATmosaic";
            //only for large ones  and when there is no concurrency: MT_create_thread(&tid, BATmosaicsync, hs, MT_THR_DETACHED);
			BATmosaicsync(hs);
        }
    } else
        ALGODEBUG fprintf(stderr, "#BATmosaic: NOT persisting index %d\n", bn->batCacheid);
#endif
	bn->batDirtydesc = TRUE;
	bn->tmosaic = m;
	MT_lock_unset(&GDKmosaicLock(bn->batCacheid));
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

    if (VIEWtparent(b)) {
        assert(b->tmosaic == NULL);
        b = BBPdescriptor(VIEWtparent(b));
    }

	assert(b->batCacheid > 0);
	t = GDKusec();
	MT_lock_set(&GDKmosaicLock(b->batCacheid));
	t = GDKusec() - t;
	if (b->tmosaic == (Heap *) 1) {
		Heap *hp;
		const char *nme = BBP_physical(b->batCacheid);
		const char *ext = "mosaic";
		int fd;

		b->tmosaic = NULL;
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
					MT_lock_unset(&GDKmosaicLock(b->batCacheid));
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
	MT_lock_unset(&GDKmosaicLock(b->batCacheid));
	ALGODEBUG if (ret) fprintf(stderr, "#BATcheckmosaic: already has mosaic %d, waited " LLFMT " usec\n", b->batCacheid, t);
	return ret;
}
