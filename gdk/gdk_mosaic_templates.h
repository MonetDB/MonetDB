

#include "gdk_commons.h"

#ifdef PERSISTENTMOSAIC
#define PERSIST_MOSAIC(HEAP, BN) CONCAT3(MOS_, HEAP, _sync)(BN)
#else
#define PERSIST_MOSAIC(HEAP, BN)
#endif

static void 
CONCAT3(MOS_, HEAP, _sync)(void *arg) {
	BAT *bn = arg;
	if (!((BBP_status(bn->batCacheid) & BBPEXISTING) && bn->batInserted == bn->batCount)) {
		TRC_DEBUG(ALGO, "#BAT NOT persisting index %d\n", bn->batCacheid);
		return;
	}

	/*TODO: This part is normally - e.g. imprints & hash - done in a different thread, look into this.*/
	/*only for large ones  and when there is no concurrency: MT_create_thread(&tid, <some-mosaic-specific-sync-function>, bn, MT_THR_DETACHED);*/
	BBPfix(bn->batCacheid);

    Heap *hp = bn->CONCAT2(t, HEAP);
    int fd;

    if (HEAPsave(hp, hp->filename, NULL, true) != GDK_SUCCEED ||
        (fd = GDKfdlocate(hp->farmid, hp->filename, "rb+", NULL)) < 0) {
        BBPunfix(bn->batCacheid);
        GDKfree(hp);
		bn->CONCAT2(t, HEAP) = NULL;
		GDKerror("Error while flushing heap " STRINGIFY(HEAP) " failed");
        return;
    }
    ((oid *) hp->base)[0] |= (oid) 1 << 24;
    if (write(fd, hp->base, SIZEOF_SIZE_T) < 0)
			GDKerror("Error while writing heap " STRINGIFY(HEAP) " failed");
    if (!(GDKdebug & FORCEMITOMASK)) {
		MOSsync(fd);
    }
    close(fd);
    BBPunfix(bn->batCacheid);
}

static gdk_return 
CONCAT2(BATmosaic_, HEAP)(BAT *bn, BUN cap)
{
    const char *nme;
	Heap *m;
	char *fname = 0;

	if( bn->CONCAT2(t, HEAP)){
		return GDK_SUCCEED;
	}

    nme = BBP_physical(bn->batCacheid);
    if ( (m = (Heap*)GDKzalloc(sizeof(Heap))) == NULL ||
		(m->farmid = BBPselectfarm(bn->batRole, bn->ttype, mosaicheap)) < 0 ||
        (fname = GDKfilepath(NOFARM, NULL, nme, STRINGIFY(HEAP))) == NULL){
			if( fname)
				GDKfree(fname);
			GDKfree(m);
			return GDK_FAIL;
	}

	if (strlen(fname) >= sizeof(m->filename)) {
		/* TODO: check if this can actually happen.*/
		GDKfree(fname);
		GDKfree(m);
		return GDK_FAIL;
	}

	strcpy(m->filename, fname);
	GDKfree(fname);

    if( HEAPalloc(m, cap, Tsize(bn)) != GDK_SUCCEED){
        return GDK_FAIL;
	}
    m->parentid = bn->batCacheid;

	bn->CONCAT2(t, HEAP) = m;
	PERSIST_MOSAIC(HEAP, bn);
	bn->batDirtydesc = TRUE;
    return GDK_SUCCEED;
}

static void 
CONCAT2(MOSdestroy_, HEAP)(BAT *bn) {
	if (bn && bn->CONCAT2(t, HEAP)) {
		/* Only destroy the mosaic-specific heap of the BAT if it is not sharing the mosaic-specific heap of some parent BAT.*/
		if(!CONCAT3(VIEW, HEAP, tparent)(bn)){
			Heap* h= bn->CONCAT2(t, HEAP);
			if( HEAPdelete(h, BBP_physical(bn->batCacheid), STRINGIFY(HEAP)) != GDK_SUCCEED){
				GDKerror(STRINGIFY(CONCAT2(MOSdestroy_, HEAP)) " failed");
			}
			GDKfree(h);
			bn->CONCAT2(t, HEAP) = NULL;
		}
	}
}
/* return TRUE if we have a mosaic-specific heap on the tail, even if we need to read one from disk */
static int 
CONCAT2(MOScheck_, HEAP)(BAT *b)
{
	int ret;
	lng t;

    if (VIEWtparent(b)) { /* TODO: does this make sense?*/
        assert(b->CONCAT2(t, HEAP) == NULL);
        b = BBPdescriptor(VIEWtparent(b));
    }

	assert(b->batCacheid > 0);
	t = GDKusec();
	if (b->CONCAT2(t, HEAP) == (Heap *) 1) {
		Heap *hp;
		const char *nme = BBP_physical(b->batCacheid);
		const char *ext = STRINGIFY(HEAP);
		int fd;

		b->CONCAT2(t, HEAP) = NULL;
		if ((hp = GDKzalloc(sizeof(*hp))) != NULL &&
		    (hp->farmid = BBPselectfarm(b->batRole, b->ttype, mosaicheap)) >= 0 ){

			sprintf(hp->filename, "%s.%s", nme, ext);

			/* check whether a persisted mosaic-specific heap can be found */
			if ((fd = GDKfdlocate(hp->farmid, nme, "rb+", ext)) >= 0) {
				struct stat st;
				int hdata;

				if (BATcount(b) > 0 && read(fd, &hdata, sizeof(hdata)) == sizeof(hdata) &&
					hdata == MOSAIC_VERSION && fstat(fd, &st) == 0 &&
				    st.st_size >= (off_t) (hp->size = hp->free = (oid) BATcount(b) * SIZEOF_OID) &&
				    HEAPload(hp, nme, ext, 0) == GDK_SUCCEED)
				{
					close(fd);
					TRC_DEBUG(ALGO, "#BATcheckmosaic %s: reusing persisted heap %d\n", ext, b->batCacheid);
					b->CONCAT2(t, HEAP) = hp;
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
	t = GDKusec() - t;
	ret = b->CONCAT2(t, HEAP) != NULL;
	return ret;
}
