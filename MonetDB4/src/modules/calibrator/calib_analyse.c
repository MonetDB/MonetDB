/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2008 CWI.
 * All Rights Reserved.
 */

/***************************************************************************\
*              RESULTS ANALYSIS                                             *
\***************************************************************************/
typedef struct {
	caliblng levels;
	caliblng size[MAXLEVELS];
	caliblng linesize[MAXLEVELS];
	caliblng latency1[MAXLEVELS];
	caliblng latency2[MAXLEVELS];
} cacheInfo;

static cacheInfo *
analyzeCache(caliblng ** result1, caliblng ** result2, caliblng MHz)
{
	caliblng x, y, xx, yy, range, lastrange, stride, level, a, l, n;
	caliblng last[LENPLATEAU], time1, time2, lasttime1, lasttime2;
	caliblng diff;
	cacheInfo *draft, *cache;

	if (!(draft = (cacheInfo *) malloc(4 * sizeof(cacheInfo))))
		ErrXit("analyzeCache: 'draft = malloc(%ld)` failed", 4 * sizeof(cacheInfo));
	if (!(cache = (cacheInfo *) malloc(sizeof(cacheInfo))))
		ErrXit("analyzeCache: 'cache = malloc(%ld)` failed", sizeof(cacheInfo));
	memset(draft, 0, 4 * sizeof(cacheInfo));
	memset(cache, 0, sizeof(cacheInfo));

	xx = (result1[0][0] & 0xffffff) - 1;
	yy = (result1[0][0] >> 24) - 1;
	level = 0;
	memset(last, 0, LENPLATEAU * sizeof(last[0]));
	a = LENPLATEAU;
	lastrange = 0;
	lasttime1 = 0;
	lasttime2 = 0;
	for (y = 1; y <= yy; y++) {
		range = result1[y][0];
		for (x = 1; x <= xx; x++) {
			stride = result1[0][x];
			time1 = result1[y][x];
			time2 = result2[y][x];
			if (draft[1].linesize[level] && last[a] && (range == draft[1].size[level])) {
				if (
#ifdef EPSILON1
					   (fabs(time1 - last[a]) / (calibdbl) time1) < EPSILON1
#endif
#ifdef EPSILON3
					   fabs(CYperIt(time1) - CYperIt(last[a])) < EPSILON3
#endif
				    ) {
					draft[0].linesize[level] = stride;
					draft[1].linesize[level] = stride;
				}
			}
			if (draft[2].linesize[level] && last[0] && lastrange && (lastrange == draft[2].size[level])) {
				if (
#ifdef EPSILON1
					   (fabs(time1 - last[0]) / (calibdbl) time1) < EPSILON1
#endif
#ifdef EPSILON3
					   fabs(CYperIt(time1) - CYperIt(last[0])) < EPSILON3
#endif
				    ) {
					draft[2].linesize[level] = stride;
					draft[3].linesize[level] = stride;
					if (x == xx) {
						level++;
						memset(last, 0, LENPLATEAU * sizeof(last[0]));
						a = LENPLATEAU;
					}
				} else {
					level++;
					memset(last, 0, LENPLATEAU * sizeof(last[0]));
					a = LENPLATEAU;
				}
			}
			if (
#ifdef EPSILON2
				   (x == 1) && (!draft[2].linesize[level]) && ((last[0] && ((FABS(time1 - last[LENPLATEAU - 1]) / (calibdbl) last[LENPLATEAU - 1]) > EPSILON2)) || (y == yy))
#endif
#ifdef EPSILON4
				   (x == 1) && (!draft[2].linesize[level]) && ((last[0] && (FABS(CYperIt(time1) - CYperIt(last[LENPLATEAU - 1])) >= EPSILON4)) || (y == yy))
#endif
			    ) {
				draft[2].linesize[level] = draft[1].linesize[level];
				draft[2].size[level] = lastrange;
				draft[2].latency1[level] = lasttime1;
				draft[2].latency2[level] = lasttime2;
				draft[3].linesize[level] = stride;
				draft[3].size[level] = range;
				draft[3].latency1[level] = time1;
				draft[3].latency2[level] = time2;
				last[0] = time1;
			}
			if ((x == 1) && (a < LENPLATEAU) && (!last[0])) {
				if (
#ifdef EPSILON2
					   (FABS(time1 - last[LENPLATEAU - 1]) / (calibdbl) last[LENPLATEAU - 1]) < EPSILON2
#endif
#ifdef EPSILON4
					   FABS(CYperIt(time1) - CYperIt(last[LENPLATEAU - 1])) <= EPSILON4
#endif
				    ) {
					last[--a] = time1;
				} else {
					memset(last, 0, LENPLATEAU * sizeof(last[0]));
					a = LENPLATEAU;
				}
			}
			if ((x == 1) && (a == LENPLATEAU)) {
				last[--a] = time1;
				draft[0].linesize[level] = stride;
				draft[0].size[level] = lastrange;
				draft[0].latency1[level] = lasttime1;
				draft[0].latency2[level] = lasttime2;
				draft[1].linesize[level] = stride;
				draft[1].size[level] = range;
				draft[1].latency1[level] = time1;
				draft[1].latency2[level] = time2;
			}
			if (x == 1) {
				lasttime1 = time1;
				lasttime2 = time2;
			}
		}
		lastrange = range;
	}

#ifdef CALIBRATOR_PRINT_OUTPUT
#ifdef DEBUG
	{
		caliblng ll;

		for (l = 0; l < level; l++) {
			for (ll = 0; ll < 4; ll++) {
				fprintf(stderr, "%2ld %5ld %3ld  %05.1f %05.1f\n", l, draft[ll].size[l] / 1024, draft[ll].linesize[l], NSperIt(draft[ll].latency1[l]), CYperIt(draft[ll].latency1[l]));
			}
			fprintf(stderr, "\n");
		}
		fflush(stderr);
	}
#endif
#endif

	for (l = n = 0; n < level; n++) {
		if (l == 0) {
			cache->latency1[l] = MIN(draft[2].latency1[n], draft[1].latency1[n]);
			cache->latency2[l] = MIN(draft[2].latency2[n], draft[1].latency2[n]);
		} else {
			cache->latency1[l] = ((calibdbl) (draft[2].latency1[n] + draft[1].latency1[n]) / 2.0);
			cache->latency2[l] = ((calibdbl) (draft[2].latency2[n] + draft[1].latency2[n]) / 2.0);
		}
		if ((l == 0) || ((log10(cache->latency1[l]) - log10(cache->latency1[l - 1])) > 0.3)) {
			cache->linesize[l] = draft[1].linesize[n];
			diff = -1;
			for (range = 1; range < result1[1][0]; range *= 2) ;
			for (y = 1; result1[y][0] < range; y++) ;
			if (l) {
				int yyy = 1;

				for (; y <= yy; y += yyy) {
					range = result1[y][0];
					if ((draft[2].size[n - 1] <= range) && (range < draft[1].size[n])) {
						if ((y > yyy) && (((result1[y][1]) - (result1[y - yyy][1])) > diff)) {
							diff = (result1[y][1]) - (result1[y - yyy][1]);
							cache->size[l - 1] = range;
						}
						if (((y + yyy) <= yy) && (((result1[y + yyy][1]) - (result1[y][1])) > diff)) {
							diff = (result1[y + yyy][1]) - (result1[y][1]);
							cache->size[l - 1] = range;
						}
					}
				}
			}
			l++;
		}
	}
	cache->size[--l] = draft[3].size[--n];
	cache->levels = l;

#ifdef CALIBRATOR_PRINT_OUTPUT
#ifdef DEBUG
	for (l = 0; l <= cache->levels; l++) {
		fprintf(stderr, "%2ld %5ld %3ld  %05.1f %05.1f\n", l, cache->size[l] / 1024, cache->linesize[l], NSperIt(cache->latency1[l]), CYperIt(cache->latency1[l]));
	}
	fprintf(stderr, "\n");
	fflush(stderr);
#endif
#endif

	free(draft);
	draft = 0;

	return cache;
}

typedef struct {
	caliblng levels;
	caliblng shift;
	caliblng mincachelines;
	caliblng entries[MAXLEVELS];
	caliblng pagesize[MAXLEVELS];
	caliblng latency1[MAXLEVELS];
	caliblng latency2[MAXLEVELS];
} TLBinfo;

static TLBinfo *
analyzeTLB(caliblng ** result1, caliblng ** result2, caliblng shift, caliblng mincachelines, caliblng MHz)
{
	caliblng x, y, xx, yy, spots, lastspots, stride, level, a, l, limit = 0, top, n;
	caliblng last[LENPLATEAU], time1, time2, lasttime1, lasttime2;

/*
	caliblng	pgsz = MAX( getpagesize() + shift, result1[0][3] );
*/
	calibdbl diff;
	TLBinfo *draft, *TLB;

	if (!(draft = (TLBinfo *) malloc(4 * sizeof(TLBinfo))))
		ErrXit("analyzeCache: 'draft = malloc(%ld)` failed", 4 * sizeof(TLBinfo));
	if (!(TLB = (TLBinfo *) malloc(sizeof(TLBinfo))))
		ErrXit("analyzeCache: 'TLB = malloc(%ld)` failed", sizeof(TLBinfo));
	memset(draft, 0, 4 * sizeof(TLBinfo));
	memset(TLB, 0, sizeof(TLBinfo));
	TLB->shift = shift;
	TLB->mincachelines = mincachelines;

	xx = (result1[0][0] & 0xffffff) - 1;
	yy = (result1[0][0] >> 24) - 1;
	level = 0;
	memset(last, 0, LENPLATEAU * sizeof(last[0]));
	a = LENPLATEAU;
	lastspots = 0;
	lasttime1 = 0;
	lasttime2 = 0;
	for (y = 2; !limit; y++) {
		spots = result1[y][0];
		limit = (y >= yy) || (spots >= (TLB->mincachelines * 1.25));
		for (x = 1; x <= xx; x++) {
			stride = result1[0][x];
			time1 = result1[y][x];
			time2 = result2[y][x];
			top = (x == 1);
/*
			top = ((x > 1) && (stride == pgsz));
*/
			if (draft[1].pagesize[level] && last[a] && (spots == draft[1].entries[level])) {
				if (
#ifdef EPSILON1
					   ((fabs(time1 - last[a]) / (calibdbl) time1) < EPSILON1) || (stride >= result1[0][1])
#endif
#ifdef EPSILON3
					   (fabs(CYperIt(time1) - CYperIt(last[a])) < EPSILON3) || (stride >= result1[0][1])
#endif
				    ) {
					draft[0].pagesize[level] = stride;
					draft[1].pagesize[level] = stride;
				}
			}
			if (draft[2].pagesize[level] && last[0] && lastspots && (lastspots == draft[2].entries[level])) {
				if (
#ifdef EPSILON1
					   ((fabs(time1 - last[0]) / (calibdbl) time1) < EPSILON1) || (stride >= result1[0][1])
#endif
#ifdef EPSILON3
					   (fabs(CYperIt(time1) - CYperIt(last[0])) < EPSILON3) || (stride >= result1[0][1])
#endif
				    ) {
					draft[2].pagesize[level] = stride;
					draft[3].pagesize[level] = stride;
					if (x == xx) {
						level++;
						memset(last, 0, LENPLATEAU * sizeof(last[0]));
						a = LENPLATEAU;
					}
				} else {
					level++;
					memset(last, 0, LENPLATEAU * sizeof(last[0]));
					a = LENPLATEAU;
				}
			}
			if (
#ifdef EPSILON2
				   (top) && (!draft[2].pagesize[level]) && ((last[0] && ((FABS(time1 - last[LENPLATEAU - 1]) / (calibdbl) last[LENPLATEAU - 1]) > EPSILON2)) || limit)
#endif
#ifdef EPSILON4
				   (top) && (!draft[2].pagesize[level]) && ((last[0] && (FABS(CYperIt(time1) - CYperIt(last[LENPLATEAU - 1])) >= EPSILON4)) || limit)
#endif
			    ) {
				draft[2].pagesize[level] = draft[1].pagesize[level];
				draft[2].entries[level] = lastspots;
				draft[2].latency1[level] = lasttime1;
				draft[2].latency2[level] = lasttime2;
				draft[3].pagesize[level] = stride;
				draft[3].entries[level] = spots;
				draft[3].latency1[level] = time1;
				draft[3].latency2[level] = time2;
				last[0] = time1;
			}
			if ((top) && (a < LENPLATEAU) && (!last[0])) {
				if (
#ifdef EPSILON2
					   (FABS(time1 - last[LENPLATEAU - 1]) / (calibdbl) last[LENPLATEAU - 1]) < EPSILON2
#endif
#ifdef EPSILON4
					   FABS(CYperIt(time1) - CYperIt(last[LENPLATEAU - 1])) <= EPSILON4
#endif
				    ) {
					last[--a] = time1;
				} else {
					memset(last, 0, LENPLATEAU * sizeof(last[0]));
					a = LENPLATEAU;
				}
			}
			if ((top) && (a == LENPLATEAU)) {
				last[--a] = time1;
				draft[0].pagesize[level] = stride;
				draft[0].entries[level] = lastspots;
				draft[0].latency1[level] = lasttime1;
				draft[0].latency2[level] = lasttime2;
				draft[1].pagesize[level] = stride;
				draft[1].entries[level] = spots;
				draft[1].latency1[level] = time1;
				draft[1].latency2[level] = time2;
			}
			if (top) {
				lasttime1 = time1;
				lasttime2 = time2;
			}
		}
		lastspots = spots;
	}

#ifdef CALIBRATOR_PRINT_OUTPUT
#ifdef DEBUG
	{
		caliblng ll;

		for (l = 0; l < level; l++) {
			for (ll = 0; ll < 4; ll++) {
				fprintf(stderr, "%2ld %5ld %5ld  %05.1f %05.1f\n", l, draft[ll].entries[l], draft[ll].pagesize[l], NSperIt(draft[ll].latency1[l]), CYperIt(draft[ll].latency1[l]));
			}
			fprintf(stderr, "\n");
		}
		fflush(stderr);
	}
#endif
#endif
	top = 1;
/*
	#ifdef DEBUG
	fprintf(stderr, "pgsz = %5ld\n", pgsz);
	#endif
	for (x = 2; x <= xx; x++) {
		#ifdef DEBUG
		fprintf(stderr, "%2ld: %5ld\n", x, result1[0][x]);
		#endif
		if (result1[0][x] == pgsz)
			top = x;
	}
	#ifdef DEBUG
	fprintf(stderr, "top = %5ld\n", top);
	fflush(stderr);
	#endif
*/
	for (l = n = 0; n < level; n++) {
		if (l == 0) {
			TLB->latency1[l] = MIN(draft[2].latency1[n], draft[1].latency1[n]);
			TLB->latency2[l] = MIN(draft[2].latency2[n], draft[1].latency2[n]);
		} else {
			TLB->latency1[l] = ((calibdbl) (draft[2].latency1[n] + draft[1].latency1[n]) / 2.0);
			TLB->latency2[l] = ((calibdbl) (draft[2].latency2[n] + draft[1].latency2[n]) / 2.0);
		}
		if ((l == 0) || (((log10(TLB->latency1[l]) - log10(TLB->latency1[l - 1])) > 0.3) && (draft[2].entries[l] > draft[1].entries[l]))) {
			TLB->pagesize[l] = draft[1].pagesize[n];
			diff = -1.0;
			for (spots = 1; spots < result1[2][0]; spots *= 2) ;
			for (y = 2; result1[y][0] < spots; y++) ;
			if (l) {
				int yyy = 1;

				for (; y <= yy; y += yyy) {
					spots = result1[y][0];
					if ((draft[2].entries[n - 1] <= spots) && (spots < draft[1].entries[n])) {
						if ((y > 4) && ((log(result1[y][top]) - log(result1[y - yyy][top])) > diff)) {
							diff = log(result1[y][top]) - log(result1[y - yyy][top]);
							TLB->entries[l - 1] = spots;
						}
						if (((y + yyy) <= yy) && ((log(result1[y + yyy][top]) - log(result1[y][top])) > diff)) {
							diff = log(result1[y + yyy][top]) - log(result1[y][top]);
							TLB->entries[l - 1] = spots;
						}
					}
				}
			}
			l++;
		}
	}
	TLB->entries[--l] = draft[3].entries[--n];
	TLB->levels = l;

#ifdef CALIBRATOR_PRINT_OUTPUT
#ifdef DEBUG
	for (l = 0; l <= TLB->levels; l++) {
		fprintf(stderr, "%2ld %5ld %5ld  %05.1f %05.1f\n", l, TLB->entries[l], TLB->pagesize[l], NSperIt(TLB->latency1[l]), CYperIt(TLB->latency1[l]));
	}
	fprintf(stderr, "\n");
	fflush(stderr);
#endif
#endif

	free(draft);
	draft = 0;

	return TLB;
}

typedef struct {
	caliblng levels;
	caliblng shift;
	caliblng minTLBentries;
	caliblng entries[MAXLEVELS];
	caliblng pagesize[MAXLEVELS];
	caliblng latency1[MAXLEVELS];
	caliblng latency2[MAXLEVELS];
} AssoInfo;

static AssoInfo *
analyzeAsso(caliblng ** result1, caliblng ** result2, caliblng shift, caliblng minTLBentries, caliblng maxLevels, caliblng MHz)
{
	caliblng X, x, y, xx, yy, spots, lastspots, stride, level, a, l, limit = 0, top, n;
	caliblng last[LENPLATEAU], time1, time2, lasttime1, lasttime2;

	/*calibdbl      diff; */
	AssoInfo *draft, *Asso;

	if (!(draft = (AssoInfo *) malloc(4 * sizeof(AssoInfo))))
		ErrXit("analyzeCache: 'draft = malloc(%ld)` failed", 4 * sizeof(AssoInfo));
	if (!(Asso = (AssoInfo *) malloc(sizeof(AssoInfo))))
		ErrXit("analyzeCache: 'Asso = malloc(%ld)` failed", sizeof(AssoInfo));
	memset(draft, 0, 4 * sizeof(AssoInfo));
	memset(Asso, 0, sizeof(AssoInfo));
	Asso->shift = shift;
	Asso->minTLBentries = minTLBentries;

	xx = (result1[0][0] & 0xffffff) - 1;
	yy = (result1[0][0] >> 24) - 1;
	level = 0;
	memset(last, 0, LENPLATEAU * sizeof(last[0]));
	a = LENPLATEAU;
	lastspots = 0;
	lasttime1 = 0;
	lasttime2 = 0;
	for (y = 2; !limit; y++) {
		spots = result1[y][0];
		limit = (y >= yy) || (spots >= (Asso->minTLBentries * 1.25));
		for (X = x = 1; x <= xx; x++) {
			if (result1[y][x] && result2[y][x]) {
				stride = result1[0][x];
				time1 = result1[y][x];
				time2 = result2[y][x];
				top = (X == 1);
				if (draft[1].pagesize[level] && last[a] && (spots == draft[1].entries[level])) {
					if (
#ifdef EPSILON1
						   ((fabs(time1 - last[a]) / (calibdbl) time1) < EPSILON1) || (stride >= result1[0][1])
#endif
#ifdef EPSILON3
						   (fabs(CYperIt(time1) - CYperIt(last[a])) < EPSILON3) || (stride >= result1[0][1])
#endif
					    ) {
						draft[0].pagesize[level] = stride;
						draft[1].pagesize[level] = stride;
					}
				}
				if (draft[2].pagesize[level] && last[0] && lastspots && (lastspots == draft[2].entries[level])) {
					if (
#ifdef EPSILON1
						   ((fabs(time1 - last[0]) / (calibdbl) time1) < EPSILON1) || (stride >= result1[0][1])
#endif
#ifdef EPSILON3
						   (fabs(CYperIt(time1) - CYperIt(last[0])) < EPSILON3) || (stride >= result1[0][1])
#endif
					    ) {
						draft[2].pagesize[level] = stride;
						draft[3].pagesize[level] = stride;
						if (x == xx) {
							level++;
							memset(last, 0, LENPLATEAU * sizeof(last[0]));
							a = LENPLATEAU;
						}
					} else {
						level++;
						memset(last, 0, LENPLATEAU * sizeof(last[0]));
						a = LENPLATEAU;
					}
				}
				if (
#ifdef EPSILON2
					   (top) && (!draft[2].pagesize[level]) && ((last[0] && ((FABS(time1 - last[LENPLATEAU - 1]) / (calibdbl) last[LENPLATEAU - 1]) > EPSILON2)) || limit)
#endif
#ifdef EPSILON4
					   (top) && (!draft[2].pagesize[level]) && ((last[0] && (FABS(CYperIt(time1) - CYperIt(last[LENPLATEAU - 1])) >= EPSILON4)) || limit)
#endif
				    ) {
					draft[2].pagesize[level] = draft[1].pagesize[level];
					draft[2].entries[level] = lastspots;
					draft[2].latency1[level] = lasttime1;
					draft[2].latency2[level] = lasttime2;
					draft[3].pagesize[level] = stride;
					draft[3].entries[level] = spots;
					draft[3].latency1[level] = time1;
					draft[3].latency2[level] = time2;
					last[0] = time1;
				}
				if ((top) && (a < LENPLATEAU) && (!last[0])) {
					if (
#ifdef EPSILON2
						   (FABS(time1 - last[LENPLATEAU - 1]) / (calibdbl) last[LENPLATEAU - 1]) < EPSILON2
#endif
#ifdef EPSILON4
						   FABS(CYperIt(time1) - CYperIt(last[LENPLATEAU - 1])) <= EPSILON4
#endif
					    ) {
						last[--a] = time1;
					} else {
						memset(last, 0, LENPLATEAU * sizeof(last[0]));
						a = LENPLATEAU;
					}
				}
				if ((top) && (a == LENPLATEAU)) {
					last[--a] = time1;
					draft[0].pagesize[level] = stride;
					draft[0].entries[level] = lastspots;
					draft[0].latency1[level] = lasttime1;
					draft[0].latency2[level] = lasttime2;
					draft[1].pagesize[level] = stride;
					draft[1].entries[level] = spots;
					draft[1].latency1[level] = time1;
					draft[1].latency2[level] = time2;
				}
				if (top) {
					lasttime1 = time1;
					lasttime2 = time2;
				}
				X++;
			}
		}
		lastspots = spots;
	}

#ifdef CALIBRATOR_PRINT_OUTPUT
#ifdef DEBUG
	{
		caliblng ll;

		for (l = 0; l < level; l++) {
			for (ll = 0; ll < 4; ll++) {
				fprintf(stderr, "%2ld %5ld %5ld  %05.1f %05.1f\n", l, draft[ll].entries[l], draft[ll].pagesize[l], NSperIt(draft[ll].latency1[l]), CYperIt(draft[ll].latency1[l]));
			}
			fprintf(stderr, "\n");
		}
		fflush(stderr);
	}
#endif
#endif

/*+
	for (l = n = 0; n < level; n++) {
		if (l == 0) {
			Asso->latency1[l] = MIN( draft[2].latency1[n] , draft[1].latency1[n] );
			Asso->latency2[l] = MIN( draft[2].latency2[n] , draft[1].latency2[n] );
		} else {
			Asso->latency1[l] = ((calibdbl)(draft[2].latency1[n] + draft[1].latency1[n]) / 2.0);
			Asso->latency2[l] = ((calibdbl)(draft[2].latency2[n] + draft[1].latency2[n]) / 2.0);
		}
		if ((l == 0) || (((log10(Asso->latency1[l]) - log10(Asso->latency1[l - 1])) > 0.3) && (draft[2].entries[l] > draft[1].entries[l]))) {
			Asso->pagesize[l] = draft[1].pagesize[n];
			diff = -1.0;
			for (spots = 1; spots < result1[2][0]; spots *= 2);
			for (y = 2; result1[y][0] < spots; y++);
			if (l) {
				int yyy = 1;
				for (; y <= yy; y += yyy) {
					spots = result1[y][0];
					if ((draft[2].entries[n - 1] <= spots) && (spots < draft[1].entries[n])) {
						if ((y > 4) && ((log(result1[y][1]) - log(result1[y - yyy][1])) > diff)) {
							diff = log(result1[y][1]) - log(result1[y - yyy][1]);
							Asso->entries[l - 1] = spots;
						}
						if (((y + yyy) <= yy) && ((log(result1[y + yyy][1]) - log(result1[y][1])) > diff)) {
							diff = log(result1[y + yyy][1]) - log(result1[y][1]);
							Asso->entries[l - 1] = spots;
						}
					}
				}
			}
			l++;
		}
	}
	Asso->entries[--l] = draft[3].entries[--n];
	Asso->levels = l;
+*/
/*-
	Asso->levels = 1;
	if (draft[0].entries[0])
		Asso->entries[0] = draft[0].entries[0];
	else
		Asso->entries[0] = draft[2].entries[0];
-*/
	l = 0;
	if (draft[0].entries[0])
		Asso->entries[l++] = draft[0].entries[0];
	for (n = 0; (n < MIN(level, maxLevels)) && (draft[2].entries[n] < Asso->minTLBentries); n++)
		Asso->entries[l++] = draft[2].entries[n];
	for (; l < maxLevels; l++)
		Asso->entries[l] = (l ? Asso->entries[l - 1] : 0);
	Asso->levels = maxLevels;

#ifdef CALIBRATOR_PRINT_OUTPUT
#ifdef DEBUG
	for (l = 0; l <= Asso->levels; l++) {
		fprintf(stderr, "%2ld %5ld %5ld  %05.1f %05.1f\n", l, Asso->entries[l], Asso->pagesize[l], NSperIt(Asso->latency1[l]), CYperIt(Asso->latency1[l]));
	}
	fprintf(stderr, "\n");
	fflush(stderr);
#endif
#endif

	free(draft);
	draft = 0;

	return Asso;
}
