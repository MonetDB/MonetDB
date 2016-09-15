/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * Implementation of bloom filters.
 * L.Sidirourgos
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_bloom.h"

#define BLOOM_VERSION	1
#define BLOOM_HEADER_SIZE	4 /* nr of size_t fields in header */

int
BATcheckbloom(BAT *b)
{
	int ret;

	MT_lock_set(&GDKhashLock(abs(b->batCacheid)));

	/* if b does not have a bloom filter, but the parent has, then use it */
	if ((b->tbloom == NULL) && VIEWtparent(b)) {
		BAT *pb = BATdescriptor(VIEWtparent(b));
		b->tbloom = pb->tbloom;
		BBPunfix(pb->batCacheid);
	}
	ret = b->tbloom != NULL;
	MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));

	ALGODEBUG if (ret) fprintf(stderr, "#BATcheckbloom: already has a bloom %d\n", b->batCacheid);
	return ret;
}

static BUN
BLOOMsize(BUN cnt) {
	BUN m = cnt;

	/* smaller power of 2 that is greater or equal to cnt */
	--m;
	m |= m >> 1;
	m |= m >> 2;
	m |= m >> 4;
	m |= m >> 8;
	m |= m >> 16;
#if SIZEOF_BUN == 8
	m |= m >> 32;
#endif
	m++;

	/* double it */
	m <<= 3;

	/* if m is almost 2*cnt, double again */
	if (m / cnt == 2)
		m <<= 1;

	return m;
}

static void bloom_stats(BAT *b, Bloomfilter *bloom) {
	BUN on = 0, off=0;
	size_t i, j;
	unsigned char *filter = (unsigned char *) bloom->filter->base;


	for (i=0; i < bloom->bytes; i++)
		for (j=0;j<8;j++)
			if (filter[i] & (1 << j))
				on++;
			else
				off++;
	fprintf(stderr, "#BATbloom(b=%s#" BUNFMT ") %s: bits on = " BUNFMT ", bits off = " BUNFMT "\n",
	        BATgetId(b), BATcount(b), b->theap.filename, on, off);
}

#define BLOOM_BUILD(TYPE)						\
do {									\
	const TYPE *restrict col = (TYPE *) Tloc(b, 0);			\
	BUN p;								\
	BUN key,mv,hv,x,y,z;						\
	for (p=0; p<cnt; p++) {						\
		key = (BUN) col[p];					\
		hash_init(key, x,y,z);					\
		next_hash(hv, x,y,z);					\
		mv = modulor(hv,bloom->mask);				\
		filter[quotient8(mv)] |= (1 << remainder8(mv));		\
		next_hash(hv, x,y,z);					\
		mv = modulor(hv,bloom->mask);				\
		filter[quotient8(mv)] |= (1 << remainder8(mv));		\
		next_hash(hv, x,y,z);					\
		mv = modulor(hv,bloom->mask);				\
		filter[quotient8(mv)] |= (1 << remainder8(mv));		\
		next_hash(hv, x,y,z);					\
		mv = modulor(hv,bloom->mask);				\
		filter[quotient8(mv)] |= (1 << remainder8(mv));		\
	}								\
} while (0)

gdk_return
BATbloom(BAT *b)
{
	lng t0 = 0, t1 = 0;

	/* we only create bloom filters for types that look like types we know */
	switch (ATOMbasetype(b->ttype)) {
	case TYPE_bte:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_lng:
#ifdef HAVE_HGE
	case TYPE_hge:
#endif
	case TYPE_flt:
	case TYPE_dbl:
		break;
	default:		/* type not supported */
		/* doesn't look enough like base type: do nothing */
		/* GDKerror("BATbloom: unsupported type\n"); */
		return GDK_FAIL;
	}

	BATcheck(b, "BATbloom", GDK_FAIL);

	if (BATcheckbloom(b))
		return GDK_SUCCEED;
	assert(b->tbloom == NULL);

	MT_lock_set(&GDKhashLock(abs(b->batCacheid)));
	t0 = GDKusec();

	if (b->tbloom == NULL) {
		BUN cnt;
		Bloomfilter *bloom;
		unsigned char *filter;
		size_t i;

		bloom = (Bloomfilter *) GDKzalloc(sizeof(Bloomfilter));
		if (bloom == NULL) {
			GDKerror("#BATbloom: memory allocation error");
			MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
			return GDK_FAIL;
		}
		bloom->filter = (Heap *) GDKzalloc(sizeof(Heap));
		if (bloom->filter == NULL) {
			GDKerror("#BATbloom: memory allocation error");
			GDKfree(bloom);
			MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
			return GDK_FAIL;
		}

		cnt = BATcount(b);
		/* TODO: check also if max-min < mbits and use identiry hash */
		if ( (ATOMbasetype(b->ttype) == TYPE_bte && (bloom->mbits = (1 << 8))) ||
		     (ATOMbasetype(b->ttype) == TYPE_sht && (bloom->mbits = (1 << 16))) ) {
			bloom->kfunc = 1;
			bloom->mask = bloom->mbits-1;
			bloom->bytes = quotient8(bloom->mbits);
		} else {
			bloom->mbits = BLOOMsize(cnt);
			/* 2 functions if the ratio is close to 3, 3 otherwise */
			bloom->kfunc = bloom->mbits/cnt == 3 ? 2 : 3;
			bloom->mask = bloom->mbits-1;
			bloom->bytes = quotient8(bloom->mbits) + 1;
		}

		if (HEAPalloc(bloom->filter, bloom->bytes, 1) != GDK_SUCCEED) {
			GDKerror("#BATbloom: memory allocation error");
			GDKfree(bloom->filter);
			GDKfree(bloom);
			MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
			return GDK_FAIL;
		}

		ALGODEBUG fprintf(stderr, "#BATbloom(b=%s#" BUNFMT ") %s: "
				"create bloom filter: mbits = " BUNFMT ", ratio = " BUNFMT
				", kfunc = %d, bytes = " SZFMT "\n", BATgetId(b),
				BATcount(b), b->theap.filename,
				bloom->mbits,  bloom->mbits/BATcount(b),
				bloom->kfunc, bloom->bytes);

		filter = (unsigned char *) bloom->filter->base;

		for (i=0; i < bloom->bytes; i++)
			filter[i] = 0;

		switch (ATOMbasetype(b->ttype)) {
		case TYPE_bte:
		{
			const unsigned char *restrict col = (unsigned char *) Tloc(b, 0);
			BUN p;
			assert(bloom->kfunc == 1);
			for (p=0; p<cnt; p++)
				filter[quotient8(col[p])] |= (1 << remainder8(col[p]));
		}
		break;
		case TYPE_sht:
		{
			const unsigned short *restrict col = (unsigned short *) Tloc(b, 0);
			BUN p;
			assert(bloom->kfunc == 1);
			for (p=0; p<cnt; p++)
				filter[quotient8(col[p])] |= (1 << remainder8(col[p]));
		}
		break;
		case TYPE_int: BLOOM_BUILD(int); break;
		case TYPE_lng: BLOOM_BUILD(lng); break;
#ifdef HAVE_HGE
		case TYPE_hge: BLOOM_BUILD(hge); break;
#endif
		case TYPE_flt: BLOOM_BUILD(flt); break;
		case TYPE_dbl: BLOOM_BUILD(dbl); break;
		default:
			/* should never reach here */
			assert(0);
			HEAPfree(bloom->filter,1);
			MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
			return GDK_FAIL;
		}

		b->tbloom = bloom;
	}

	t1 = GDKusec();

	MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));

	ALGODEBUG fprintf(stderr, "#BATbloom: bloom construction " LLFMT " usec\n", t1 - t0);
	ALGODEBUG bloom_stats(b, b->tbloom);

	return GDK_SUCCEED;
}

inline
int BLOOMask(BUN v, Bloomfilter *bloom)
{
	BUN hv,mv,x,y,z;
	int ret = 0;

	const unsigned char *filter = (unsigned char *) bloom->filter->base;
	if (bloom->kfunc == 1) {
		return filter[quotient8(v)] & (1 << remainder8(v));
	}

	hash_init(v, x,y,z);
	next_hash(hv, x,y,z);
	mv = modulor(hv, bloom->mask);
	ret = filter[quotient8(mv)] & (1 << remainder8(mv));
	if (ret) {
		next_hash(hv, x,y,z);
		mv = modulor(hv, bloom->mask);
		ret = (filter[quotient8(mv)] & (1 << remainder8(mv)));
		if (ret) {
			next_hash(hv, x,y,z);
			mv = modulor(hv, bloom->mask);
			ret = (filter[quotient8(mv)] & (1 << remainder8(mv)));
			if (ret) {
				next_hash(hv, x,y,z);
				mv = modulor(hv, bloom->mask);
				ret = (filter[quotient8(mv)] & (1 << remainder8(mv)));
			}
		}
	}

	return ret;
}

#if 0
static void
BLOOMremove(BAT *b)
{
}

void
BLOOMdestroy(BAT *b)
{
}

/* free the memory associated with the imprints, do not remove the
 * heap files; indicate that imprints are available on disk by setting
 * the imprints pointer to 1 */
void
BLOOMfree(BAT *b)
{
}
#endif

#ifndef NDEBUG
/* never called, useful for debugging */

#endif
