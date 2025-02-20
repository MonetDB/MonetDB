#include <monetdb_config.h>
#include <mal_exception.h>
#include <gdk_cand.h>
#include <string.h>

#if 0
#define GZ 128
#define CHAR_MAP(s) (s&127)
#else
#define GZ 64
#define CHAR_MAP(s) (s&63)
#endif
#define SZ_1GRAM GZ
#define SZ_2GRAM (GZ*GZ)
#define SZ_3GRAM (GZ*GZ*GZ)

static str
NGcx(bit *r, str *h, str *needle)
{
	*r = strstr(*h, *needle) != NULL;
	return MAL_SUCCEED;
}

static str
NGcxselect(bat *R, bat *H, bat *C, str *Needle, bit *anti)
{
	(void)R;
	(void)H;
	(void)C;
	(void)Needle;
	(void)anti;
	return MAL_SUCCEED;
}

#if 0
#define NGRAM_TYPE hge
#define NGRAM_TYPEID TYPE_hge
#define NGRAM_TYPENIL hge_nil
#define NGRAM_CST(v) ((hge)LL_CONSTANT(v))
#define NGRAM_BITS 127
#else
#define NGRAM_TYPE lng
#define NGRAM_TYPEID TYPE_lng
#define NGRAM_TYPENIL lng_nil
#define NGRAM_CST(v) LL_CONSTANT(v)
#define NGRAM_BITS 63
#endif

#define NGRAM_MULTIPLE 16

typedef struct {
	NGRAM_TYPE max, small;
	unsigned int *h;
	unsigned int *pos;
	unsigned int *rid;
	NGRAM_TYPE *idx;
	NGRAM_TYPE *sigs;
} Ngrams;

static void
ngrams_destroy(Ngrams *n)
{
	if (n) {
		GDKfree(n->h);
		GDKfree(n->idx);
		GDKfree(n->pos);
		GDKfree(n->rid);
		GDKfree(n->sigs);
	}
	GDKfree(n);
}

static Ngrams *
ngrams_create(BAT *b, size_t ngramsize)
{
	Ngrams *n = NULL;
	size_t sz = BATcount(b);

	n = (Ngrams*)GDKmalloc(sizeof(Ngrams));
	if (n) {
		n->h = (unsigned int*)GDKmalloc(ngramsize*sizeof(int));
		n->pos=(unsigned int*)GDKzalloc(ngramsize*sizeof(int));
		n->rid=(unsigned int*)GDKmalloc(NGRAM_MULTIPLE* sz * sizeof(int));

		n->idx = (NGRAM_TYPE*)GDKmalloc(ngramsize*sizeof(NGRAM_TYPE));
		n->sigs=(NGRAM_TYPE*)GDKmalloc(sz * sizeof(NGRAM_TYPE));
	}
	if (!n || !n->h || !n->idx || !n->pos || !n->rid || !n->sigs) {
		ngrams_destroy(n);
		return NULL;
	}
	return n;
}

static int
ngrams_init_1gram(Ngrams *n, BAT *b)
{
	BUN cnt = BATcount(b);
	NGRAM_TYPE *h = (NGRAM_TYPE *)GDKzalloc(SZ_1GRAM*sizeof(NGRAM_TYPE)), *hist = (NGRAM_TYPE*)h, sum = 0;
	int *id = (int*)GDKmalloc(SZ_1GRAM*sizeof(int)), i;
	NGRAM_TYPE *idx = n->idx;

	if (!h || !id) {
		GDKfree(h);
		GDKfree(id);
		return -1;
	}

	BATiter bi = bat_iterator(b);
	for(BUN i=0; i<cnt; i++) {
		const char *s = BUNtail(bi,i);
		if (!strNil(s) && *s) { /* skipped */
			for(; *s; s++) {
				h[CHAR_MAP(*s)]++;
			}
		}
	}
	bat_iterator_end(&bi);

	int bc = 0;

	for(int i=0; i<SZ_1GRAM; i++) {
		id[i] = i;
		idx[i] = 0;
		n->h[i] = (unsigned int)hist[i];
	}
	GDKqsort(h, id, NULL, SZ_1GRAM, sizeof(NGRAM_TYPE), sizeof(int), NGRAM_TYPEID, true, false);
	for(i=SZ_1GRAM-1; i>=0; i--) {
		if ((BUN)(sum + hist[i]) >= (NGRAM_MULTIPLE*cnt)-1)
			break;
		sum += hist[i];
	}
	NGRAM_TYPE larger_cnt = hist[i];
	for(; hist[i] == larger_cnt; i++)
		;
	NGRAM_TYPE max = hist[0], small = hist[i];
	printf("max %d, first smaller %d nr of larger %d sum %ld, cnt %ld\n", (int)hist[0], (int)small, i, sum, cnt);
	n->max = max;
	n->small = small;
	for(int i=0; i<SZ_1GRAM && hist[i] > 0; i++) {
		unsigned int x=id[i];
		idx[x] = NGRAM_CST(1)<<bc;
		assert(idx[x] > 0);
		bc++;
		bc %= NGRAM_BITS;
	}

	bi = bat_iterator(b);
	NGRAM_TYPE *sp = n->sigs;
	unsigned int pos = 1;
	for(BUN i=0; i<cnt; i++) {
		const char *s = BUNtail(bi,i);
		NGRAM_TYPE sig = 0;
		if (!strNil(s) && s[0]) { /* too short skipped */
			for(; *s; s++) {
				int k = CHAR_MAP(*s);
				sig |= idx[k];
				if (n->h[k] <= n->small) {
					if (n->pos[k] == 0) {
						n->pos[k] = pos;
						pos += n->h[k];
						n->h[k] = 0;
					}
					/* deduplicate */
					int done =  (n->h[k] > 0 && n->rid[n->pos[k] + n->h[k]-1] == i);
					if (!done) {
						n->rid[n->pos[k] + n->h[k]] = i;
						n->h[k]++;
					}
				}
			}
			*sp = sig;
		} else {
			*sp = NGRAM_TYPENIL;
		}
		sp++;
	}
	bat_iterator_end(&bi);

	GDKfree(h);
	GDKfree(id);
	return 0;
}

static str
NGc1join_intern(bat *L, bat *R, bat *H, bat *N, bat *lc, bat *rc, bit *nil_matches, lng *estimate, bit *anti)
{
	(void)nil_matches;
	(void)estimate;
	BAT *h = BATdescriptor(*H);
	BAT *n = BATdescriptor(*N);

	if (lc && !is_bat_nil(*lc))
		assert(0);
	if (rc && !is_bat_nil(*rc))
		assert(0);

	if (*anti)
		throw(MAL, "gram.c1", "No anti contains yet\n");
	if (!h || !n) {
		BBPreclaim(h);
		BBPreclaim(n);
		throw(MAL, "gram.c1", RUNTIME_OBJECT_MISSING);
	}

	if (BATcount(n) < 10) {
		printf("todo fall back to select \n");
	}

	Ngrams *ngi = ngrams_create(h, SZ_1GRAM);
	if (ngi && ngrams_init_1gram(ngi, h) == 0) { /* TODO add locks and only create ngram once for full (parent bat) */
		BUN cnt = BATcount(h);
		/* create L/R */
		BAT *l = COLnew(0, TYPE_oid, 10*cnt, TRANSIENT);
		BAT *r = COLnew(0, TYPE_oid, 10*cnt, TRANSIENT);

		int ncnt = 0, ncnt1 = 0, ncnt2 = 0, ncnt3 = 0, ncnt4 = 0, ncnt5 = 0;
		BATiter ni = bat_iterator(n);
		BATiter hi = bat_iterator(h);
		NGRAM_TYPE nmax = 0;
		oid *ol = Tloc(l, 0), *el = ol + 10*cnt;
		oid *or = Tloc(r, 0);
		cnt = BATcount(n);
		/* if needed grow */
		for(BUN i = 0; i<cnt; i++) {
			const char *s = BUNtail(ni,i), *os = s;
			NGRAM_TYPE sig = 0;

			if ((ol+1000) > el)
				break;
			if (!strNil(s) && s[0]) {
				NGRAM_TYPE min = ngi->max;
				unsigned int min_pos = 0;
				for(; *s; s++) {
					unsigned int k = CHAR_MAP(*s);
					sig |= ngi->idx[k];
					if (ngi->h[k] < min) {
						min = ngi->h[k];
						min_pos = k; /* encoded min ngram */
					}
				}
				ncnt++;
				if (min <= ngi->small) {
					unsigned int rr = ngi->pos[min_pos];
					int hcnt = ngi->h[min_pos];
					ncnt1++;
					//printf("list used %d pos %d,%d, %s\n", hcnt, rr, min_pos, os);
					for(int k = 0; k<hcnt; k++, rr++) {
						unsigned int hr = ngi->rid[rr];
						if (((ngi->sigs[hr] & sig) == sig)) {
							char *hs = BUNtail(hi, hr);
							ncnt3++;
							if (strstr(hs, os) != NULL) {
								*ol++ = hr;
								*or++ = (oid)i;
							}
						}
					}
				} else {
					unsigned int hcnt = BATcount(h);
					ncnt2++;
					for(size_t k = 0; k<hcnt; k++) {
						if (((ngi->sigs[k] & sig) == sig)) {
							char *hs = BUNtail(hi, k);
							ncnt4++;
							if (strstr(hs, os) != NULL) {
								*ol++ = k;
								*or++ = (oid)i;
							}
						}
					}
				}
				//printf("%d %d\n", min_pos, (int)min);
				if (min > nmax)
					nmax = min;
			} else if (!strNil(s)) { /* skipped */
				unsigned int hcnt = BATcount(h);
				ncnt++;
				for(size_t k = 0; k<hcnt; k++) {
					char *hs = BUNtail(hi, k);
					ncnt5++;
					if (strstr(hs, os) != NULL) {
						*ol++ = k;
						*or++ = (oid)i;
					}
				}
			}
		}
		printf("max %ld\n", nmax);
		bat_iterator_end(&ni);
		bat_iterator_end(&hi);
		BBPreclaim(h);
		BBPreclaim(n);
		BATsetcount(l, ol - (oid*)Tloc(l, 0));
		BATsetcount(r, ol - (oid*)Tloc(l, 0));
		*L = l->batCacheid;
		*R = r->batCacheid;
		BBPkeepref(l);
		BBPkeepref(r);
		printf("%d, %d, %d, %d, %d, %d, %d\n", ncnt, ncnt1, ncnt2, ncnt3, ncnt4, ncnt5, (int)ngi->small);
		ngrams_destroy(ngi);
		return MAL_SUCCEED;
	}
	BBPreclaim(h);
	BBPreclaim(n);
	throw(MAL, "gram.c1", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
NGc1join1(bat *L, bat *sigs, bat *needle, bat *lc, bit *nil_matches, lng *estimate, bit *anti)
{
	return NGc1join_intern(L, NULL, sigs, needle, lc, NULL, nil_matches, estimate, anti);
}

static str
NGc1join(bat *L, bat *R, bat *sigs, bat *needle, bat *lc, bat *rc, bit *nil_matches, lng *estimate, bit *anti)
{
	return NGc1join_intern(L, R, sigs, needle, lc, rc, nil_matches, estimate, anti);
}
static int
ngrams_init_2gram(Ngrams *n, BAT *b)
{
	BUN cnt = BATcount(b);
	NGRAM_TYPE (*h)[GZ] = (NGRAM_TYPE (*)[GZ])GDKzalloc(SZ_2GRAM*sizeof(NGRAM_TYPE)), *hist = (NGRAM_TYPE*)h, sum = 0;
	int *id = (int*)GDKmalloc(SZ_2GRAM*sizeof(int)), i;
	NGRAM_TYPE *idx = n->idx;

	if (!h || !id) {
		GDKfree(h);
		GDKfree(id);
		return -1;
	}

	BATiter bi = bat_iterator(b);
	for(BUN i=0; i<cnt; i++) {
		const char *s = BUNtail(bi,i);
		if (!strNil(s) && *s) { /* skipped */
			unsigned char p = CHAR_MAP(*s++);
			for(; *s; p=CHAR_MAP(*s), s++) {
				h[p][CHAR_MAP(*s)]++;
			}
		}
	}
	bat_iterator_end(&bi);

	int bc = 0;

	for(int i=0; i<SZ_2GRAM; i++) {
		id[i] = i;
		idx[i] = 0;
		n->h[i] = (unsigned int)hist[i];
	}
	GDKqsort(h, id, NULL, SZ_2GRAM, sizeof(NGRAM_TYPE), sizeof(int), NGRAM_TYPEID, true, false);
	for(i=SZ_2GRAM-1; i>=0; i--) {
		if ((size_t)(sum + hist[i]) >= (NGRAM_MULTIPLE*cnt)-1)
			break;
		sum += hist[i];
	}
	NGRAM_TYPE larger_cnt = hist[i];
	for(; hist[i] == larger_cnt; i++)
		;
	NGRAM_TYPE max = hist[0], small = hist[i];
	printf("max %d, first smaller %d nr of larger %d sum %ld, cnt %ld\n", (int)hist[0], (int)small, i, sum, cnt);
	n->max = max;
	n->small = small;
	for(int i=0; i<SZ_2GRAM && hist[i] > 0; i++) {
		int y=(id[i]/GZ)%GZ, z=id[i]%GZ;
		idx[y*GZ+z] = NGRAM_CST(1)<<bc;
		assert(idx[y*GZ+z] > 0);
		bc++;
		bc %= NGRAM_BITS;
	}

	bi = bat_iterator(b);
	NGRAM_TYPE *sp = n->sigs;
	unsigned int pos = 1;
	for(BUN i=0; i<cnt; i++) {
		const char *s = BUNtail(bi,i);
		NGRAM_TYPE sig = 0;
		if (!strNil(s) && s[0] && s[1]) { /* too short skipped */
			unsigned char p = CHAR_MAP(*s++);
			for(; *s; p=CHAR_MAP(*s), s++) {
				int k = p*GZ+CHAR_MAP(*s);
				sig |= idx[k];
				if (n->h[k] <= n->small) {
					if (n->pos[k] == 0) {
						n->pos[k] = pos;
						pos += n->h[k];
						n->h[k] = 0;
					}
					/* deduplicate */
					int done =  (n->h[k] > 0 && n->rid[n->pos[k] + n->h[k]-1] == i);
					if (!done) {
						n->rid[n->pos[k] + n->h[k]] = i;
						n->h[k]++;
					}
				}
			}
			*sp = sig;
		} else {
			*sp = NGRAM_TYPENIL;
		}
		sp++;
	}
	bat_iterator_end(&bi);

	GDKfree(h);
	GDKfree(id);
	return 0;
}

static str
NGc2join_intern(bat *L, bat *R, bat *H, bat *N, bat *lc, bat *rc, bit *nil_matches, lng *estimate, bit *anti)
{
	(void)nil_matches;
	(void)estimate;
	BAT *h = BATdescriptor(*H);
	BAT *n = BATdescriptor(*N);

	if (lc && !is_bat_nil(*lc))
		assert(0);
	if (rc && !is_bat_nil(*rc))
		assert(0);

	if (*anti)
		throw(MAL, "gram.c2", "No anti contains yet\n");
	if (!h || !n) {
		BBPreclaim(h);
		BBPreclaim(n);
		throw(MAL, "gram.c2", RUNTIME_OBJECT_MISSING);
	}

	if (BATcount(n) < 10) {
		printf("todo fall back to select \n");
	}

	Ngrams *ngi = ngrams_create(h, SZ_2GRAM);
	if (ngi && ngrams_init_2gram(ngi, h) == 0) {
		BUN cnt = BATcount(h);
		/* create L/R */
		BAT *l = COLnew(0, TYPE_oid, 10*cnt, TRANSIENT);
		BAT *r = COLnew(0, TYPE_oid, 10*cnt, TRANSIENT);

		int ncnt = 0, ncnt1 = 0, ncnt2 = 0, ncnt3 = 0, ncnt4 = 0, ncnt5 = 0;
		lng t1 = 0, t2 = 0, t3 = 0;
	        lng ncnt6 = 0;
		BATiter ni = bat_iterator(n);
		BATiter hi = bat_iterator(h);
		NGRAM_TYPE nmax = 0;
		oid *ol = Tloc(l, 0), *el = ol + 10*cnt;
		oid *or = Tloc(r, 0);
		cnt = BATcount(n);
		/* if needed grow */
		for(BUN i = 0; i<cnt; i++) {
			const char *s = BUNtail(ni,i), *os = s;
			NGRAM_TYPE sig = 0;

			if ((ol+1000) > el)
				break;
			if (!strNil(s) && s[0] && s[1]) { /* skipped */
				NGRAM_TYPE min = ngi->max;
				unsigned int min_pos = 0;
				unsigned char p = CHAR_MAP(*s++);
				for(; *s; p=CHAR_MAP(*s), s++) {
					unsigned int k = p*GZ+CHAR_MAP(*s);
					sig |= ngi->idx[k];
					if (ngi->h[k] < min) {
						min = ngi->h[k];
						min_pos = k; /* encoded min ngram */
					}
				}
				ncnt++;
				if (min <= ngi->small) {
					unsigned int rr = ngi->pos[min_pos];
					int hcnt = ngi->h[min_pos];
					ncnt1++;
					ncnt6+=hcnt;
					//printf("list used %d pos %d,%d, %s\n", hcnt, rr, min_pos, os);
					for(int k = 0; k<hcnt; k++, rr++) {
						unsigned int hr = ngi->rid[rr];
						if (((ngi->sigs[hr] & sig) == sig)) {
							char *hs = BUNtail(hi, hr);
							ncnt3++;
							if (strstr(hs, os) != NULL) {
								*ol++ = hr;
								*or++ = (oid)i;
							}
						}
					}
				} else {
					unsigned int hcnt = BATcount(h);
					ncnt2++;
					for(size_t k = 0; k<hcnt; k++) {
						if (((ngi->sigs[k] & sig) == sig)) {
							char *hs = BUNtail(hi, k);
							ncnt4++;
							if (strstr(hs, os) != NULL) {
								*ol++ = k;
								*or++ = (oid)i;
							}
						}
					}
				}
				//printf("%d %d\n", min_pos, (int)min);
				if (min > nmax)
					nmax = min;
			} else if (!strNil(s)) {
				unsigned int hcnt = BATcount(h);
				ncnt++;
				printf("os %s\n", os);
				for(size_t k = 0; k<hcnt; k++) {
					char *hs = BUNtail(hi, k);
					ncnt5++;
					if (strstr(hs, os) != NULL) {
						*ol++ = k;
						*or++ = (oid)i;
					}
				}
			}
		}
		printf("max %ld\n", nmax);
		bat_iterator_end(&ni);
		bat_iterator_end(&hi);
		BBPreclaim(h);
		BBPreclaim(n);
		BATsetcount(l, ol - (oid*)Tloc(l, 0));
		BATsetcount(r, ol - (oid*)Tloc(l, 0));
		*L = l->batCacheid;
		*R = r->batCacheid;
		BBPkeepref(l);
		BBPkeepref(r);
		printf("%d, %d, %d, %d, %d, %d, %ld, %d\n", ncnt, ncnt1, ncnt2, ncnt3, ncnt4, ncnt5, ncnt6, (int)ngi->small);
		printf("times %ld, %ld, %ld\n", t1, t2, t3);
		ngrams_destroy(ngi);
		return MAL_SUCCEED;
	}
	BBPreclaim(h);
	BBPreclaim(n);
	throw(MAL, "gram.c2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
NGc2join1(bat *L, bat *sigs, bat *needle, bat *lc, bit *nil_matches, lng *estimate, bit *anti)
{
	return NGc2join_intern(L, NULL, sigs, needle, lc, NULL, nil_matches, estimate, anti);
}

static str
NGc2join(bat *L, bat *R, bat *sigs, bat *needle, bat *lc, bat *rc, bit *nil_matches, lng *estimate, bit *anti)
{
	return NGc2join_intern(L, R, sigs, needle, lc, rc, nil_matches, estimate, anti);
}

static int
ngrams_init_3gram(Ngrams *n, BAT *b)
{
	BUN cnt = BATcount(b);
	NGRAM_TYPE (*h)[GZ][GZ] = (NGRAM_TYPE (*)[GZ][GZ])GDKzalloc(SZ_3GRAM*sizeof(NGRAM_TYPE)), *hist = (NGRAM_TYPE*)h, sum = 0;
	int *id = (int*)GDKmalloc(SZ_3GRAM*sizeof(int)), i;
	NGRAM_TYPE *idx = n->idx;

	if (!h || !id) {
		GDKfree(h);
		GDKfree(id);
		return -1;
	}

	BATiter bi = bat_iterator(b);
	for(BUN i=0; i<cnt; i++) {
		const char *s = BUNtail(bi,i);
		if (!strNil(s) && *s) { /* skipped */
			unsigned char pp = CHAR_MAP(*s++);
			if (!*s)
			       continue;
			unsigned char p = CHAR_MAP(*s++);
			for(; *s; pp=p, p=CHAR_MAP(*s), s++) {
				h[pp][p][CHAR_MAP(*s)]++;
			}
		}
	}
	bat_iterator_end(&bi);

	int bc = 0;

	for(int i=0; i<SZ_3GRAM; i++) {
		id[i] = i;
		idx[i] = 0;
		n->h[i] = (unsigned int)hist[i];  /* TODO check for overflow ? */
	}
	GDKqsort(h, id, NULL, SZ_3GRAM, sizeof(NGRAM_TYPE), sizeof(int), NGRAM_TYPEID, true, false);
	for(i=SZ_3GRAM-1; i>=0; i--) {
		if ((size_t)(sum + hist[i]) >= (NGRAM_MULTIPLE*cnt)-1)
			break;
		sum += hist[i];
	}
	NGRAM_TYPE larger_cnt = hist[i];
	for(; hist[i] == larger_cnt; i++)
		;
	NGRAM_TYPE max = hist[0], small = hist[i];
	printf("max %d, first smaller %d nr of larger %d sum %ld, cnt %ld\n", (int)hist[0], (int)small, i, sum, cnt);
	n->max = max;
	n->small = small;
	for(int i=0; i<SZ_3GRAM && hist[i] > 0; i++) {
		unsigned int x=id[i]/(GZ*GZ), y=(id[i]/GZ)%GZ,
			     z=id[i]%GZ;
		idx[x*GZ*GZ+y*GZ+z] = NGRAM_CST(1)<<bc;
		assert(idx[x*GZ*GZ+y*GZ+z] > 0);
		bc++;
		bc %= NGRAM_BITS;
	}

	bi = bat_iterator(b);
	NGRAM_TYPE *sp = n->sigs;
	unsigned int pos = 1;
	for(BUN i=0; i<cnt; i++) {
		const char *s = BUNtail(bi,i);
		NGRAM_TYPE sig = 0;
		if (!strNil(s) && s[0] && s[1] && s[2]) { /* too short skipped */
			unsigned char pp = CHAR_MAP(*s++);
			unsigned char p = CHAR_MAP(*s++);
			for(; *s; pp=p, p=CHAR_MAP(*s), s++) {
				int k = pp*GZ*GZ+p*GZ+CHAR_MAP(*s);
				sig |= idx[k];
				if (n->h[k] <= n->small) {
					if (n->pos[k] == 0) {
						n->pos[k] = pos;
						pos += n->h[k];
						n->h[k] = 0;
					}
					/* deduplicate */
					int done =  (n->h[k] > 0 && n->rid[n->pos[k] + n->h[k]-1] == i);
					if (!done) {
						n->rid[n->pos[k] + n->h[k]] = i;
						n->h[k]++;
					}
				}
			}
			*sp = sig;
		} else {
			*sp = NGRAM_TYPENIL;
		}
		sp++;
	}
	bat_iterator_end(&bi);

	GDKfree(h);
	GDKfree(id);
	return 0;
}

static str
NGc3join_intern(bat *L, bat *R, bat *H, bat *N, bat *lc, bat *rc, bit *nil_matches, lng *estimate, bit *anti)
{
	(void)nil_matches;
	(void)estimate;
	BAT *h = BATdescriptor(*H);
	BAT *n = BATdescriptor(*N);

	if (lc && !is_bat_nil(*lc))
		assert(0);
	if (rc && !is_bat_nil(*rc))
		assert(0);

	if (*anti)
		throw(MAL, "gram.c3", "No anti contains yet\n");
	if (!h || !n) {
		BBPreclaim(h);
		BBPreclaim(n);
		throw(MAL, "gram.c3", RUNTIME_OBJECT_MISSING);
	}

	if (BATcount(n) < 10) {
		printf("todo fall back to select \n");
	}

	Ngrams *ngi = ngrams_create(h, SZ_3GRAM);
	if (ngi && ngrams_init_3gram(ngi, h) == 0) { /* TODO add locks and only create ngram once for full (parent bat) */
		BUN cnt = BATcount(h);
		/* create L/R */
		BAT *l = COLnew(0, TYPE_oid, 10*cnt, TRANSIENT);
		BAT *r = COLnew(0, TYPE_oid, 10*cnt, TRANSIENT);

		int ncnt = 0, ncnt1 = 0, ncnt2 = 0, ncnt3 = 0, ncnt4 = 0, ncnt5 = 0;
		lng t1 = 0, t2 = 0, t3 = 0;
	        lng ncnt6 = 0;
		BATiter ni = bat_iterator(n);
		BATiter hi = bat_iterator(h);
		NGRAM_TYPE nmax = 0;
		oid *ol = Tloc(l, 0), *el = ol + 10*cnt;
		oid *or = Tloc(r, 0);
		cnt = BATcount(n);
		/* if needed grow */
		for(BUN i = 0; i<cnt; i++) {
			const char *s = BUNtail(ni,i), *os = s;
			NGRAM_TYPE sig = 0;

			if ((ol+1000) > el)
				break;
			if (!strNil(s) && s[0] && s[1] && s[2]) { /* skipped */
				NGRAM_TYPE min = ngi->max;
				unsigned int min_pos = 0;
				unsigned char pp = CHAR_MAP(*s++);
				unsigned char p = CHAR_MAP(*s++);
				for(; *s; pp=p, p=CHAR_MAP(*s), s++) {
					unsigned int k = pp*GZ*GZ+p*GZ+CHAR_MAP(*s);
					sig |= ngi->idx[k];
					if (ngi->h[k] < min) {
						min = ngi->h[k];
						min_pos = k; /* encoded min ngram */
					}
				}
				ncnt++;
				if (min <= ngi->small) {
					unsigned int rr = ngi->pos[min_pos];
					int hcnt = ngi->h[min_pos];
					ncnt1++;
					ncnt6+=hcnt;
					//printf("list used %d pos %d,%d, %s\n", hcnt, rr, min_pos, os);
					for(int k = 0; k<hcnt; k++, rr++) {
						unsigned int hr = ngi->rid[rr];
						if (((ngi->sigs[hr] & sig) == sig)) {
							char *hs = BUNtail(hi, hr);
							ncnt3++;
							if (strstr(hs, os) != NULL) {
								*ol++ = hr;
								*or++ = (oid)i;
							}
						}
					}
				} else {
					unsigned int hcnt = BATcount(h);
					ncnt2++;
					for(size_t k = 0; k<hcnt; k++) {
						if (((ngi->sigs[k] & sig) == sig)) {
							char *hs = BUNtail(hi, k);
							ncnt4++;
							if (strstr(hs, os) != NULL) {
								*ol++ = k;
								*or++ = (oid)i;
							}
						}
					}
				}
				//printf("%d %d\n", min_pos, (int)min);
				if (min > nmax)
					nmax = min;
			} else if (!strNil(s)) { /* skipped */
				unsigned int hcnt = BATcount(h);
				ncnt++;
				for(size_t k = 0; k<hcnt; k++) {
					char *hs = BUNtail(hi, k);
					ncnt5++;
					if (strstr(hs, os) != NULL) {
						*ol++ = k;
						*or++ = (oid)i;
					}
				}
			}
		}
		printf("max %ld\n", nmax);
		bat_iterator_end(&ni);
		bat_iterator_end(&hi);
		BBPreclaim(h);
		BBPreclaim(n);
		BATsetcount(l, ol - (oid*)Tloc(l, 0));
		BATsetcount(r, ol - (oid*)Tloc(l, 0));
		*L = l->batCacheid;
		*R = r->batCacheid;
		BBPkeepref(l);
		BBPkeepref(r);
		printf("%d, %d, %d, %d, %d, %d, %ld, %d\n", ncnt, ncnt1, ncnt2, ncnt3, ncnt4, ncnt5, ncnt6, (int)ngi->small);
		printf("times %ld, %ld, %ld\n", t1, t2, t3);
		ngrams_destroy(ngi);
		return MAL_SUCCEED;
	}
	BBPreclaim(h);
	BBPreclaim(n);
	throw(MAL, "gram.c3", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
NGc3join1(bat *L, bat *sigs, bat *needle, bat *lc, bit *nil_matches, lng *estimate, bit *anti)
{
	return NGc3join_intern(L, NULL, sigs, needle, lc, NULL, nil_matches, estimate, anti);
}

static str
NGc3join(bat *L, bat *R, bat *sigs, bat *needle, bat *lc, bat *rc, bit *nil_matches, lng *estimate, bit *anti)
{
	return NGc3join_intern(L, R, sigs, needle, lc, rc, nil_matches, estimate, anti);
}

#include "mel.h"
static mel_func ngram_init_funcs[] = {

	command("ngram", "c1", NGcx, false,
		"predicate if value and needle equal needle",
		args(1, 3, arg("res", bit), arg("h", str), arg("needle", str))),
	command("ngram", "c1select", NGcxselect, false,
		"predicate if value and needle equal needle",
		args(1, 5, batarg("res", oid), batarg("h", str), batarg("s", oid), arg("needle", str), arg("anti", bit))),
	command("ngram", "c1join", NGc1join1, false,
		"predicate if value and needle equal needle (using 1gram)",
		args(1, 8, batarg("l", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),
	command("ngram", "c1join", NGc1join, false,
		"predicate if value and needle equal needle (using 1gram)",
		args(2, 9, batarg("l", oid), batarg("r", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),

	command("ngram", "c2", NGcx, false,
		"predicate if value and needle equal needle",
		args(1, 3, arg("res", bit), arg("h", str), arg("needle", str))),
	command("ngram", "c2select", NGcxselect, false,
		"predicate if value and needle equal needle",
		args(1, 5, batarg("res", oid), batarg("h", str), batarg("s", oid), arg("needle", str), arg("anti", bit))),
	command("ngram", "c2join", NGc2join1, false,
		"predicate if value and needle equal needle (using 2gram)",
		args(1, 8, batarg("l", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),
	command("ngram", "c2join", NGc2join, false,
		"predicate if value and needle equal needle (using 2gram)",
		args(2, 9, batarg("l", oid), batarg("r", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),

	command("ngram", "c3", NGcx, false,
		"predicate if value and needle equal needle",
		args(1, 3, arg("res", bit), arg("h", str), arg("needle", str))),
	command("ngram", "c3select", NGcxselect, false,
		"predicate if value and needle equal needle",
		args(1, 5, batarg("res", oid), batarg("h", str), batarg("s", oid), arg("needle", str), arg("anti", bit))),
	command("ngram", "c3join", NGc3join1, false,
		"predicate if value and needle equal needle (using 3gram)",
		args(1, 8, batarg("l", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),
	command("ngram", "c3join", NGc3join, false,
		"predicate if value and needle equal needle (using 3gram)",
		args(2, 9, batarg("l", oid), batarg("r", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),

	{ .imp=NULL }		/* sentinel */
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_ngram)
{
	mal_module("ngram", NULL, ngram_init_funcs);
}
