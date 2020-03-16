

#include "gdk_commons.h"

#ifdef PERSISTENTMOSAIC
#define PERSIST_MOSAIC(HEAP, BN) CONCAT3(MOS_, HEAP, _sync)(BN)
static void
CONCAT3(MOS_, HEAP, _sync)(void *arg)
{
	BAT *b = arg;
	Heap *hp;
	int fd;
	lng t0  = GDKusec();
	const char *failed = " failed";

	MT_lock_set(&b->batIdxLock);
	if ((hp = b->CONCAT2(t, HEAP)) != NULL) {
		if (HEAPsave(hp, hp->filename, NULL, true) == GDK_SUCCEED) {
			if (hp->storage == STORE_MEM) {
				if ((fd = GDKfdlocate(hp->farmid, hp->filename, "rb+", NULL)) >= 0) {
					((oid *) hp->base)[0] |= (oid) 1 << 24;
					if (write(fd, hp->base, SIZEOF_OID) >= 0) {
						failed = ""; /* not failed */
						if (!(GDKdebug & NOSYNCMASK)) {
#if defined(NATIVE_WIN32)
							_commit(fd);
#elif defined(HAVE_FDATASYNC)
							fdatasync(fd);
#elif defined(HAVE_FSYNC)
							fsync(fd);
#endif
						}
						hp->dirty = false;
					} else {
						GDKerror("Error while writing heap " STRINGIFY(HEAP) " failed");
					}
					close(fd);
				}
			} else {
				((oid *) hp->base)[0] |= (oid) 1 << 24;
				if (!(GDKdebug & NOSYNCMASK) &&
				    MT_msync(hp->base, SIZEOF_OID) < 0) {
					((oid *) hp->base)[0] &= ~((oid) 1 << 24);
				} else {
					hp->dirty = false;
					failed = ""; /* not failed */
				}
			}
			TRC_DEBUG(ACCELERATOR, "#" STRINGIFY(CONCAT3(MOS_, HEAP, _sync)) "(%s): " STRINGIFY(HEAP) " index persisted"
				  " (" LLFMT " usec)%s\n",
				  BATgetId(b), GDKusec() - t0, failed);
		}
	}
	MT_lock_unset(&b->batIdxLock);
	BBPunfix(b->batCacheid);
}
#else
#define PERSIST_MOSAIC(HEAP, BN)
#endif

/* create the heap for an order index; returns NULL on failure */
static Heap *
CONCAT3(create_, HEAP, _heap)(BAT *b, BUN cap)
{
	Heap *m;
	const char *nme;


	const char* ext = "." EXT(HEAP);
	nme = GDKinmemory() ? ":inmemory" : BBP_physical(b->batCacheid);
	if ((m = GDKzalloc(sizeof(Heap))) == NULL ||
	    (m->farmid = BBPselectfarm(b->batRole, b->ttype, mosaicheap)) < 0 ||
		strconcat_len(m->filename, sizeof(m->filename), nme, ext, NULL) >= sizeof(m->filename) ||
	    HEAPalloc(m, cap, Tsize(b)) != GDK_SUCCEED) {
		GDKfree(m);
		return NULL;
	}
	return m;
}

/* maybe persist the order index heap */
static void
CONCAT2(persist_, HEAP)(BAT *b)
{
#ifdef PERSISTENTMOSAIC
	if ((BBP_status(b->batCacheid) & BBPEXISTING) &&
	    b->batInserted == b->batCount &&
	    !b->theap.dirty &&
	    !GDKinmemory()) {
		MT_Id tid;
		BBPfix(b->batCacheid);
		char name[16];
		snprintf(name, sizeof(name), STRINGIFY(CONCAT3(MOS_, HEAP, _sync)) "%d", b->batCacheid);
		if (MT_create_thread(&tid, CONCAT3(MOS_, HEAP, _sync), b,
				     MT_THR_DETACHED, name) < 0)
			BBPunfix(b->batCacheid);
	} else
		TRC_DEBUG(ACCELERATOR, "#" STRINGIFY(CONCAT2(persist, HEAP)) "(" ALGOBATFMT "): NOT persisting " STRINGIFY(HEAP) " index\n", ALGOBATPAR(b));
#else
	(void) b;
#endif
}

static void 
CONCAT2(MOSremove_, HEAP)(BAT *bn) {
	assert(bn && bn->CONCAT2(t, HEAP) != NULL);
	assert(!VIEWtparent(bn));

	Heap* h= bn->CONCAT2(t, HEAP);
	bn->CONCAT2(t, HEAP) = NULL;
	if( HEAPdelete(h, BBP_physical(bn->batCacheid), STRINGIFY(HEAP)) != GDK_SUCCEED){
		GDKerror(STRINGIFY(CONCAT2(MOSdestroy_, HEAP)) " failed");
	}
	GDKfree(h);
}

static void
CONCAT2(MOSdestroy_, HEAP)(BAT *b)
{
	if (b && b->CONCAT2(t, HEAP)) {
		if (b->CONCAT2(t, HEAP) == (Heap *) 1) {
			b->CONCAT2(t, HEAP) = NULL;
			GDKunlink(BBPselectfarm(b->batRole, b->ttype, mosaicheap),
				  BATDIR,
				  BBP_physical(b->batCacheid),
				  EXT(HEAP));
		} else if (b->CONCAT2(t, HEAP) != NULL && !VIEWtparent(b))
			CONCAT2(MOSremove_, HEAP)(b);
	}
}

static void
CONCAT2(MOSfree_, HEAP)(BAT *bn)
{
	Heap *h;

	if (bn && bn->CONCAT2(t, HEAP)) {
		assert(bn->batCacheid > 0);
		h = bn->CONCAT2(t, HEAP);
		if (h != NULL && h != (Heap *) 1) {
			if (GDKinmemory()) {
				if(!VIEWtparent(bn)) {
					HEAPfree(h, true);
					GDKfree(h);
				}
				bn->CONCAT2(t, HEAP) = NULL;
			} else {
				if(!VIEWtparent(bn)) {
					HEAPfree(h, false);
					GDKfree(h);
				}
				bn->CONCAT2(t, HEAP) = (Heap *) 1;
			}
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
		int fd;

		b->CONCAT2(t, HEAP) = NULL;
		if ((hp = GDKzalloc(sizeof(*hp))) != NULL &&
		    (hp->farmid = BBPselectfarm(b->batRole, b->ttype, mosaicheap)) >= 0 ){

			const char* ext = EXT(HEAP);
			const char* ext_wih_dot = "." EXT(HEAP);
			strconcat_len(hp->filename, sizeof(hp->filename), nme, ext_wih_dot, NULL);

			/* check whether a persisted mosaic-specific heap can be found */
			if ((fd = GDKfdlocate(hp->farmid, nme, "rb+", ext)) >= 0) {
				struct stat st;
				oid hdata[2];

				// TODO: add check on mosaic magic number.
				if (BATcount(b) > 0 && 
					read(fd, hdata, sizeof(hdata)) == sizeof(hdata) &&
					hdata[0] == MOSAIC_VERSION &&
					fstat(fd, &st) == 0 &&
					st.st_size >= (off_t) (hp->size = hp->free = hdata[1]) &&
					HEAPload(hp, nme, ext, 0) == GDK_SUCCEED)
				{
					close(fd);
					hp->parentid = b->batCacheid;
					TRC_DEBUG(ALGO, "#" STRINGIFY(CONCAT2(MOScheck_, HEAP)) " %s: reusing persisted heap %d\n", ext, b->batCacheid);
					b->CONCAT2(t, HEAP) = hp;
					return 1;
				}
				close(fd);
				/* unlink unusable file */
				GDKunlink(hp->farmid, BATDIR, nme, ext);
			}
		}
		GDKfree(hp);
		GDKclrerr();	/* we're not currently interested in errors */
	}
	t = GDKusec() - t;
	ret = b->CONCAT2(t, HEAP) != NULL;
	return ret;
}
