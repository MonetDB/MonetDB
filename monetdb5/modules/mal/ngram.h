/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

static str
hist_1gram(BAT *sigs, BAT *b)
{
	BUN cnt = BATcount(b);

	NGRAM_TYPE *h = GDKzalloc(SZ_1GRAM*sizeof(NGRAM_TYPE)), sum = 0;
	int *id = GDKmalloc(SZ_1GRAM*sizeof(int));
	NGRAM_TYPE *idx = GDKmalloc(SZ_1GRAM*sizeof(NGRAM_TYPE));
	if (!h || !id || !idx) {
		GDKfree(h);
		GDKfree(id);
		throw(MAL, "ngram.histogram", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	BATiter bi = bat_iterator(b);
	for(BUN i=0; i<cnt; i++) {
		char *s = BUNtail(bi,i);
		if (!strNil(s)) { /* skipped */
			for(; *s; s++) {
				h[*s]++;
			}
		}
	}
	bat_iterator_end(&bi);

	/* dump */
	/* char ngram[2]; */
	/* ngram[1] = 0; */
	int bc = 0;
	for(int i=0; i<SZ_1GRAM; i++)
		id[i] = i;
	GDKqsort(h, id, NULL, SZ_1GRAM, sizeof(NGRAM_TYPE), sizeof(int), NGRAM_TYPEID, true, false);
	NGRAM_TYPE max = h[0], small = 175;
	int smaller = 0, nr_smaller = 0;

	/* which algo (simple use skip > 20% of count */
	for(int i=0; i<SZ_1GRAM; i++) {
		int x=id[i];
		if (h[i] > 0) {
			/* ngram[0] = (char)x; */
			/* printf("%d %s %d\n", i, ngram, h[i]); */
			sum += h[i];
			idx[x] = 0;
			if (h[i] <= max) {
				idx[x] = NGRAM_CST(1)<<bc;
				bc++;
				bc %= NGRAM_BITS;
				if (!smaller && h[i] < small)
					smaller = i;
				if (smaller)
					nr_smaller++;
			}
		}
	}
	printf("max %d, sum %d, first smaller %d %d nr smaller %d\n", (int)max, (int)sum, (int)small, smaller, nr_smaller);

	bi = bat_iterator(b);
	NGRAM_TYPE *sp = Tloc(sigs, 0);
	for(BUN i=0; i<cnt; i++) {
		char *s = BUNtail(bi,i);
		NGRAM_TYPE sig = 0;
		if (!strNil(s)) { /* skipped */
			for(; *s; s++)
				sig |= idx[*s];
			*sp = sig;
		} else {
			*sp = NGRAM_TYPENIL;
		}
		sp++;
	}
	BATsetcount(sigs, bi.count);
	BATnegateprops(sigs);
	bat_iterator_end(&bi);

	GDKfree(h);
	GDKfree(id);
	GDKfree(idx);
	return MAL_SUCCEED;
}

static str
hist_2gram(BAT *sigs, BAT *b)
{
	BUN cnt = BATcount(b);
	NGRAM_TYPE (*h)[GZ] = (NGRAM_TYPE (*)[GZ])GDKzalloc(SZ_2GRAM*sizeof(NGRAM_TYPE)),
		*hist = (NGRAM_TYPE*)h, sum = 0;
	int *id = (int*)GDKmalloc(SZ_2GRAM*sizeof(int));
	NGRAM_TYPE *idx = GDKmalloc(SZ_2GRAM*sizeof(NGRAM_TYPE));
	if (!h || !id || !idx) {
		GDKfree(h);
		GDKfree(id);
		throw(MAL, "ngram.histogram", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	BATiter bi = bat_iterator(b);
	for(BUN i=0; i<cnt; i++) {
		char *s = BUNtail(bi,i);
		if (!strNil(s) && *s) { /* skipped */
			unsigned char p = *s++;
			for(; *s; p=*s, s++) {
				h[p][*s]++;
			}
		}
	}
	bat_iterator_end(&bi);

	/* dump */
	/* char ngram[3]; */
	/* ngram[2] = 0; */
	int bc = 0;
	for(int i=0; i<SZ_2GRAM; i++)
		id[i] = i;
	GDKqsort(h, id, NULL, SZ_2GRAM, sizeof(NGRAM_TYPE), sizeof(int), NGRAM_TYPEID, true, false);
	NGRAM_TYPE max = hist[0], small = 175;
	int smaller = 0, nr_smaller = 0;

	/* which algo (simple use skip > 20% of count */
	for(int i=0; i<SZ_2GRAM; i++) {
		int x=id[i]/GZ, y=id[i]%GZ;
		/* ngram[0] = (char)x; */
		if (hist[i] > 0) {
			/* ngram[1] = (char)y; */
			/* printf("%d %s %d\n", i, ngram, hist[i]); */
			sum += hist[i];
			idx[x*GZ+y] = 0;
			if (hist[i] <= max) {
				idx[x*GZ+y] = NGRAM_CST(1)<<bc;
				bc++;
				bc %= NGRAM_BITS;
				if (!smaller && hist[i] < small)
					smaller = i;
				if (smaller)
					nr_smaller++;
			}
		}
	}
	printf("max %d, sum %d, first smaller %d %d nr smaller %d\n", (int)max, (int)sum, (int)small, smaller, nr_smaller);

	bi = bat_iterator(b);
	NGRAM_TYPE *sp = Tloc(sigs, 0);
	for(BUN i=0; i<cnt; i++) {
		char *s = BUNtail(bi,i);
		NGRAM_TYPE sig = 0;
		if (!strNil(s) && *s) { /* skipped */
			unsigned char p = *s++;
			for(; *s; p=*s, s++) {
				sig |= idx[p*GZ+*s];
			}
			*sp = sig;
		} else {
			*sp = NGRAM_TYPENIL;
		}
		sp++;
	}
	BATsetcount(sigs, bi.count);
	BATnegateprops(sigs);
	bat_iterator_end(&bi);

	GDKfree(h);
	GDKfree(id);
	GDKfree(idx);
	return MAL_SUCCEED;
}

static str
hist_3gram(BAT *sigs, BAT *b)
{
	BUN cnt = BATcount(b);
	NGRAM_TYPE (*h)[GZ][GZ] = (NGRAM_TYPE (*)[GZ][GZ])GDKzalloc(SZ_3GRAM*sizeof(NGRAM_TYPE)), *hist = (NGRAM_TYPE*)h, sum = 0;
	int *id = (int*)GDKmalloc(SZ_3GRAM*sizeof(int));
	NGRAM_TYPE *idx = GDKmalloc(SZ_3GRAM*sizeof(NGRAM_TYPE));
	if (!h || !id || !idx) {
		GDKfree(h);
		GDKfree(id);
		throw(MAL, "ngram.histogram", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	BATiter bi = bat_iterator(b);
	for(BUN i=0; i<cnt; i++) {
		char *s = BUNtail(bi,i);
		if (!strNil(s) && *s) { /* skipped */
			unsigned char pp = *s++;
			if (!*s)
			       continue;
			unsigned char p = *s++;
			for(; *s; pp=p, p=*s, s++) {
				h[pp][p][*s]++;
			}
		}
	}
	bat_iterator_end(&bi);

	/* dump */
	/* char ngram[4]; */
	/* ngram[3] = 0; */
	int bc = 0;
	for(int i=0; i<SZ_3GRAM; i++)
		id[i] = i;
	GDKqsort(h, id, NULL, SZ_3GRAM, sizeof(NGRAM_TYPE), sizeof(int), NGRAM_TYPEID, true, false);
	NGRAM_TYPE max = hist[0], small = 175;
	int smaller = 0, nr_smaller = 0;

	/* which algo (simple use skip > 20% of count */
	for(int i=0; i<SZ_3GRAM; i++) {
		int x=id[i]/(GZ*GZ), y=(id[i]/GZ)%GZ, z=id[i]%GZ;
		/* ngram[0] = (char)x; */
		/* ngram[1] = (char)y; */
		if (hist[i] > 0) {
			/* ngram[2] = (char)z; */
			/* printf("%d %s %d\n", i, ngram, hist[i]); */
			sum += hist[i];
			idx[x*GZ*GZ+y*GZ+z] = 0;
			if (hist[i] <= max) {
				idx[x*GZ*GZ+y*GZ+z] = NGRAM_CST(1)<<bc;
				assert(idx[x*GZ*GZ+y*GZ+z] > 0);
				bc++;
				bc %= NGRAM_BITS;
				if (!smaller && hist[i] < small)
					smaller = i;
				if (smaller)
					nr_smaller++;
			}
		}
	}
	printf("max %d, sum %d, first smaller %d %d nr smaller %d\n", (int)max, (int)sum, (int)small, smaller, nr_smaller);

	bi = bat_iterator(b);
	NGRAM_TYPE *sp = Tloc(sigs, 0);
	for(BUN i=0; i<cnt; i++) {
		char *s = BUNtail(bi,i);
		NGRAM_TYPE sig = 0;
		if (!strNil(s) && s[0] && s[1]) { /* skipped */
			unsigned char pp = *s++;
			unsigned char p = *s++;
			for(; *s; pp=p, p=*s, s++) {
				sig |= idx[pp*GZ*GZ+p*GZ+*s];
			}
			*sp = sig;
		} else {
			*sp = NGRAM_TYPENIL;
		}
		sp++;
	}
	BATsetcount(sigs, bi.count);
	BATnegateprops(sigs);
	bat_iterator_end(&bi);

	GDKfree(h);
	GDKfree(id);
	GDKfree(idx);
	return MAL_SUCCEED;
}

static str
NGsignature( bat *sigs, bat *strs, int *N)
{
	str msg = MAL_SUCCEED;
	int n = *N;

	if (n < 1 || n > 3)
		throw(MAL, "ngram.signature", "Only n-grams from 1 to 3 are supported\n");

	BAT *b;
	if ((b = BATdescriptor(*strs)) == NULL)
		throw(MAL, "ngram.signature", RUNTIME_OBJECT_MISSING);

	BAT *sig = COLnew(b->hseqbase, NGRAM_TYPEID, BATcount(b), TRANSIENT);
	if (!sig) {
		BBPunfix(b->batCacheid);
		throw(MAL, "ngram.signature", GDK_EXCEPTION);
	}
	switch(n){
	case 1:
		msg = hist_1gram(sig, b);
		break;
	case 2:
		msg = hist_2gram(sig, b);
		break;
	case 3:
		msg = hist_3gram(sig, b);
		break;
	}
	BBPunfix(b->batCacheid);
	*sigs = sig->batCacheid;
	BBPkeepref(sig);
	return msg;
}

static str
NGand(bit *r, NGRAM_TYPE *sigs, NGRAM_TYPE *needle)
{
	*r = ((*sigs & *needle) == *needle);
	return MAL_SUCCEED;
}

static str
NGandselect(bat *R, bat *Sigs, bat *C, NGRAM_TYPE *Needle, bit *anti)
{
	str msg = NULL;
	NGRAM_TYPE needle = *Needle;
	BAT *sigs = BATdescriptor(*Sigs), *c = NULL, *bn = NULL;

	(void)anti;/* anti not implemented */
	if (!is_bat_nil(*C))
		c = BATdescriptor(*C);
	if (!sigs || (!is_bat_nil(*C) && !c)) {
		BBPreclaim(sigs);
		throw(MAL, "andselect", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	size_t counter = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

	struct canditer ci;
	canditer_init(&ci, sigs, c);

	if (!(bn = COLnew(0, TYPE_oid, ci.ncand, TRANSIENT))) {
		BBPreclaim(sigs);
		BBPreclaim(c);
		throw(MAL, "andselect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	BUN p, q, cnt = 0;
	if (!c || BATtdense(c)) {
		if (c) {
			assert(BATtdense(c));
			p = (BUN) c->tseqbase;
			q = p + BATcount(c);
			if ((oid) p < sigs->hseqbase)
				p = sigs->hseqbase;
			if ((oid) q > sigs->hseqbase + BATcount(sigs))
				q = sigs->hseqbase + BATcount(sigs);
		} else {
			p = sigs->hseqbase;
			q = BATcount(sigs) + sigs->hseqbase;
		}
	}

	oid off = sigs->hseqbase, *restrict vals = Tloc(bn, 0);
	NGRAM_TYPE *sp = (NGRAM_TYPE*)Tloc(sigs, 0);
	/* scan select loop with or without candidates */
	/* TRC_DEBUG(ALGO, "scanselect(sigs=%s#"BUNFMT",anti=%d): andselect\n", BATgetId(sigs), BATcount(sigs), anti); */
	if (!c || BATtdense(c)) {
		for (; p < q; p++) {
			GDK_CHECK_TIMEOUT(qry_ctx, counter, GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx));
			if ((sp[p-off] & needle) == needle)
				vals[cnt++] = p;
		}
	} else {
		for (p = 0; p < ci.ncand; p++) {
			GDK_CHECK_TIMEOUT(qry_ctx, counter, GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx));
			oid o = canditer_next(&ci);
			if ((sp[o-off] & needle) == needle)
				vals[cnt++] = o;
		}
	}                                                               \
	BATsetcount(bn, cnt);
	BATnegateprops(bn);
	bn->tkey = true;
	bn->tnonil = true;
	bn->tsorted = true;
	bn->trevsorted = false;
	*R = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
bailout:
	return msg;
}
