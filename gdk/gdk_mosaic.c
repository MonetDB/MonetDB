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
	MT_lock_set(&GDKmosaicLock(b->batCacheid));
}

void MOSunsetLock(BAT* b) {
	MT_lock_unset(&GDKmosaicLock(b->batCacheid));
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

#ifdef PERSISTENTMOSAIC
#define PERSIST_MOSAIC(HEAP, BN) MOS_##HEAP##_sync(BN)
#else
#define PERSIST_MOSAIC(HEAP, BN)
#endif

#define CREATE_(HEAP)\
\
static void \
MOS_##HEAP##_sync(void *arg) {\
	BAT *bn = arg;\
	if (!((BBP_status(bn->batCacheid) & BBPEXISTING) && bn->batInserted == bn->batCount)) {\
		ALGODEBUG fprintf(stderr, "#BAT" #HEAP ": NOT persisting index %d\n", bn->batCacheid);\
		return;\
	}\
\
	/*TODO: This part is normally - e.g. imprints & hash - done in a different thread, look into this.*/\
	/*only for large ones  and when there is no concurrency: MT_create_thread(&tid, <some-mosaic-specific-sync-function>, bn, MT_THR_DETACHED);*/\
	BBPfix(bn->batCacheid);\
\
    Heap *hp = bn->t##HEAP;\
    int fd;\
    lng t0 = GDKusec();\
\
    if (HEAPsave(hp, hp->filename, NULL) != GDK_SUCCEED ||\
        (fd = GDKfdlocate(hp->farmid, hp->filename, "rb+", NULL)) < 0) {\
        BBPunfix(bn->batCacheid);\
        GDKfree(arg);\
        return;\
    }\
    ((oid *) hp->base)[0] |= (oid) 1 << 24;\
    if (write(fd, hp->base, SIZEOF_SIZE_T) < 0)\
        perror("write " #HEAP);\
    if (!(GDKdebug & FORCEMITOMASK)) {\
		MOSsync(fd);\
    }\
    close(fd);\
    BBPunfix(bn->batCacheid);\
    ALGODEBUG fprintf(stderr, "#%s: persisting " #HEAP " %s (" LLFMT " usec)\n", "BAT" #HEAP, hp->filename, GDKusec() - t0);\
    GDKfree(arg);\
}\
\
static gdk_return \
BATmosaic_##HEAP(BAT *bn, BUN cap)\
{\
    const char *nme;\
	Heap *m;\
	char *fname = 0;\
\
	if( bn->t##HEAP){\
		return GDK_SUCCEED;\
	}\
\
    nme = BBP_physical(bn->batCacheid);\
    if ( (m = (Heap*)GDKzalloc(sizeof(Heap))) == NULL ||\
		(m->farmid = BBPselectfarm(bn->batRole, bn->ttype, mosaicheap)) < 0 ||\
        (fname = GDKfilepath(NOFARM, NULL, nme, #HEAP)) == NULL){\
			if( fname)\
				GDKfree(fname);\
			GDKfree(m);\
			return GDK_FAIL;\
	}\
\
	if (strlen(fname) >= sizeof(m->filename)) {\
		/* TODO: check if this can actually happen.*/\
		GDKfree(fname);\
		GDKfree(m);\
		return GDK_FAIL;\
	}\
\
	strcpy(m->filename, fname);\
	GDKfree(fname);\
\
    if( HEAPalloc(m, cap, Tsize(bn)) != GDK_SUCCEED){\
        return GDK_FAIL;\
	}\
    m->parentid = bn->batCacheid;\
\
	PERSIST_MOSAIC(HEAP, bn);\
	bn->batDirtydesc = TRUE;\
	bn->t##HEAP = m;\
    return GDK_SUCCEED;\
}\
\
static void \
MOSdestroy_##HEAP(BAT *bn) {\
	if (bn && bn->t##HEAP) {\
		/* Only destroy the mosaic-specific heap of the BAT if it is not sharing the mosaic-specific heap of some parent BAT.*/\
		if(!VIEW##HEAP##tparent(bn)){\
			Heap* h= bn->t##HEAP;\
			if( HEAPdelete(h, BBP_physical(bn->batCacheid), #HEAP))\
				IODEBUG fprintf(stderr,"#MOSdestroy" #HEAP " (%s) failed", BATgetId(bn));\
			bn->t##HEAP = NULL;\
			GDKfree(h);\
		}\
	}\
}\
/* return TRUE if we have a mosaic-specific heap on the tail, even if we need to read one from disk */\
static int \
MOScheck_##HEAP(BAT *b)\
{\
	int ret;\
	lng t;\
\
    if (VIEWtparent(b)) { /* TODO: does this make sense?*/\
        assert(b->t##HEAP == NULL);\
        b = BBPdescriptor(VIEWtparent(b));\
    }\
\
	assert(b->batCacheid > 0);\
	t = GDKusec();\
	t = GDKusec() - t;\
	if (b->t##HEAP == (Heap *) 1) {\
		Heap *hp;\
		const char *nme = BBP_physical(b->batCacheid);\
		const char *ext = #HEAP;\
		int fd;\
\
		b->t##HEAP = NULL;\
		if ((hp = GDKzalloc(sizeof(*hp))) != NULL &&\
		    (hp->farmid = BBPselectfarm(b->batRole, b->ttype, mosaicheap)) >= 0 ){\
\
			sprintf(hp->filename, "%s.%s", nme, ext);\
\
			/* check whether a persisted mosaic-specific heap can be found */\
			if ((fd = GDKfdlocate(hp->farmid, nme, "rb+", ext)) >= 0) {\
				struct stat st;\
				int hdata;\
\
				if (BATcount(b) > 0 && read(fd, &hdata, sizeof(hdata)) == sizeof(hdata) &&\
					hdata == MOSAIC_VERSION &&\
				    fstat(fd, &st) == 0 &&\
				    st.st_size >= (off_t) (hp->size = hp->free = (oid) BATcount(b) * SIZEOF_OID) &&\
				    HEAPload(hp, nme, ext, 0) == GDK_SUCCEED) {\
					close(fd);\
					b->t##HEAP = hp;\
					ALGODEBUG fprintf(stderr, "#BATcheckmosaic" #HEAP ": reusing persisted heap %d\n", b->batCacheid);\
					return 1;\
				}\
				close(fd);\
				/* unlink unusable file */\
				GDKunlink(hp->farmid, BATDIR, nme, ext);\
			}\
			GDKfree(hp->filename);\
		}\
		GDKfree(hp);\
		GDKclrerr();	/* we're not currently interested in errors */\
	}\
	ret = b->t##HEAP != NULL;\
	ALGODEBUG if (ret) fprintf(stderr, "#BATcheckmosaic" #HEAP ": already has " #HEAP " %d, waited " LLFMT " usec\n", b->batCacheid, t);\
	return ret;\
}

CREATE_(mosaic)
CREATE_(vmosaic)

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
