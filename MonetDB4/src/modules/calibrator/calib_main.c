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
 * Portions created by CWI are Copyright (C) 1997-2006 CWI.
 * All Rights Reserved.
 */

/***************************************************************************\
*              MAIN  TESTING  PROCEDURE                                     *
\***************************************************************************/

#ifdef CALIBRATOR_CHECK_SMP
#include "calib_smp.c"
#endif

struct fullInfo {
#ifdef CALIBRATOR_CHECK_SMP
	SMPinfo smp;
#endif
	caliblng delayC;
	cacheInfo *cache;
	TLBinfo *TLB;
	AssoInfo *Asso;
};

struct fullInfo *
mainRun(caliblng MHz, caliblng maxrange, char *fname)
{
	caliblng align = 0;
	caliblng mincachelines, minTLBentries, maxlinesize, mincachesize,
	    /*maxcachesize, */ minstride = (caliblng) sizeof(char *), yy, y;
	caliblng maxCstride = 0, maxTstride = 0, maxAstride = 0, delayC /*,delayT */ ;
	char *array0, *array;
	caliblng **result1, **result2;
	cacheInfo *cache;
	TLBinfo *TLB;
	AssoInfo *Asso;
	caliblng pgsz = getpagesize();
	struct fullInfo *calibratorInfo;

#ifdef CALIBRATOR_CREATE_PLOTS
	FILE *fp;
	char fnn1[1024], fnx1[1024], fnn2[1024], fnx2[1024];
#endif
	(void) fname;

	if (MHz == 0) {
		MHz = 1000;	/* arbitrary. should never be printed */
	}

	if (!(array0 = (char *) malloc(maxrange + pgsz)))
		ErrXit("main: 'array0 = malloc(%ld)` failed", maxrange + pgsz);

	array = array0;
#ifdef CALIBRATOR_PRINT_OUTPUT
	fprintf(stderr, "\n%lx %ld %ld %5ld\n", (long) array, (caliblng) array, pgsz, (caliblng) array % pgsz);
#endif
	while (((caliblng) array % pgsz) != align) {
#ifdef CALIBRATOR_PRINT_OUTPUT
		fprintf(stderr, "\r%lx %ld %ld %5ld", (long) array, (caliblng) array, pgsz, (caliblng) array % pgsz);
		fflush(stderr);
#endif
		array++;
	}
#ifdef CALIBRATOR_PRINT_OUTPUT
	fprintf(stderr, "\n%lx %ld %ld %5ld\n\n", (long) array, (caliblng) array, pgsz, (caliblng) array % pgsz);
	fflush(stderr);

	fprintf(stderr, "now        = %ld\n", now());
	fprintf(stderr, "getMINTIME = %ld\n", getMINTIME());
#endif
#ifdef WIN32
	MINTIME = 100000;
#else
	MINTIME = MAX(MINTIME, 10 * getMINTIME());
#endif
#ifdef CALIBRATOR_PRINT_OUTPUT
	fprintf(stderr, "MINTIME    = %ld\n\n", MINTIME);
	fflush(stderr);
#endif

#ifdef CALIBRATOR_CREATE_PLOTS
	sprintf(fnn1, "%s.cache-replace-time", fname);
	sprintf(fnx1, "%s.data", fnn1);
	if (!(fp = fopen(fnx1, "w")))
		ErrXit("main: 'fp = fopen(%s,\"w\")` failed", fnx1);
	result1 = runCache(array, maxrange, minstride, MHz, fp, &maxCstride);
	fclose(fp);
#else
	result1 = runCache(array, maxrange, minstride, MHz, 0, &maxCstride);
#endif

#ifdef CALIBRATOR_CREATE_PLOTS
	sprintf(fnn2, "%s.cache-miss-latency", fname);
	sprintf(fnx2, "%s.data", fnn2);
	if (!(fp = fopen(fnx2, "w")))
		ErrXit("main: 'fp = fopen(%s,\"w\")` failed", fnx2);
	result2 = runCache(array, maxrange, minstride, MHz, fp, &maxCstride);
	fclose(fp);
#else
	result2 = runCache(array, maxrange, minstride, MHz, 0, &maxCstride);
#endif

	cache = analyzeCache(result1, result2, MHz);
	mincachelines = (cache->size[0] && cache->linesize[1] ? cache->size[0] / cache->linesize[1] : 1024);
	maxlinesize = (cache->linesize[cache->levels] ? cache->linesize[cache->levels] : maxCstride / 2);
	mincachesize = (cache->size[0] ? cache->size[0] : 0);
	/*maxcachesize = ( cache->levels && cache->size[cache->levels - 1] ? cache->size[cache->levels - 1] : 0 ); */
	delayC = cache->latency2[0] - cache->latency1[0];

#ifdef CALIBRATOR_CREATE_PLOTS
	sprintf(fnx1, "%s.gp", fnn1);
	if (!(fp = fopen(fnx1, "w")))
		ErrXit("main: 'fp = fopen(%s,\"w\")` failed", fnx1);
	plotCache(cache, result1, MHz, fnn1, fp, 0);
	fclose(fp);

	sprintf(fnx2, "%s.gp", fnn2);
	if (!(fp = fopen(fnx2, "w")))
		ErrXit("main: 'fp = fopen(%s,\"w\")` failed", fnx2);
	plotCache(cache, result2, MHz, fnn2, fp, delayC);
	fclose(fp);
#endif

	yy = (result1[0][0] >> 24) - 1;
	for (y = 0; y <= yy; y++) {
		free(result1[y]);
		result1[y] = 0;
	}
	free(result1);
	result1 = 0;

	yy = (result2[0][0] >> 24) - 1;
	for (y = 0; y <= yy; y++) {
		free(result2[y]);
		result2[y] = 0;
	}
	free(result2);
	result2 = 0;

#ifdef CALIBRATOR_CREATE_PLOTS
	sprintf(fnn1, "%s.TLB-miss-latency", fname);
	sprintf(fnx1, "%s.data", fnn1);
	if (!(fp = fopen(fnx1, "w")))
		ErrXit("main: 'fp = fopen(%s,\"w\")` failed", fnx1);
	result1 = runTLB(array, maxrange, 1024, maxlinesize, mincachelines, MHz, fp, &maxTstride);
	fclose(fp);
#else
	result1 = runTLB(array, maxrange, 1024, maxlinesize, mincachelines, MHz, 0, &maxTstride);
#endif
/*
        sprintf(fnn2, "%s.TLB2", fname);
        sprintf(fnx2, "%s.data", fnn2);
	if (!(fp = fopen(fnx2,"w"))) ErrXit("main: 'fp = fopen(%s,\"w\")` failed", fnx2);
	result2 = runTLB(array, maxrange, 1024, maxlinesize, mincachelines, MHz, fp, &maxTstride);
	fclose(fp);
*/
	result2 = result1;

	TLB = analyzeTLB(result1, result2, maxlinesize, mincachelines, MHz);
	minTLBentries = (TLB->levels && TLB->entries[0] ? TLB->entries[0] : mincachelines);
	/*delayT = TLB->latency2[0] - TLB->latency1[0]; */

#ifdef CALIBRATOR_CREATE_PLOTS
	sprintf(fnx1, "%s.gp", fnn1);
	if (!(fp = fopen(fnx1, "w")))
		ErrXit("main: 'fp = fopen(%s,\"w\")` failed", fnx1);
	plotTLB(TLB, result1, MHz, fnn1, fp, 0);
	fclose(fp);
#endif

/*
        sprintf(fnx2, "%s.gp", fnn2);
	if (!(fp = fopen(fnx2,"w"))) ErrXit("main: 'fp = fopen(%s,\"w\")` failed", fnx2);
	plotTLB(TLB, result2, MHz, fnn2, fp, delayT);
	fclose(fp);
*/

	yy = (result1[0][0] >> 24) - 1;
	for (y = 0; y <= yy; y++) {
		free(result1[y]);
		result1[y] = 0;
	}
	free(result1);
	result1 = 0;

/*
	yy = (result2[0][0] >> 24) - 1;
	for (y = 0; y <= yy; y++) {
		free(result2[y]);
		result2[y] = 0;
	}
	free(result2);
*/
	result2 = 0;


	maxAstride = 1;
	while (maxAstride <= /*MIN(maxcachesize*2, */ (maxrange / 2))
		maxAstride *= 2;
/*
	maxAstride *= 2;
*/
	mincachesize = 1024;
/*-
	fprintf(stderr,"runAsso(array, maxrange=%ld, mincachesize=%ld, 0=%ld, minTLBentries=%ld, MHz=%ld, fp, &maxAstride=%ld)\n",maxrange, mincachesize, 0, minTLBentries, MHz, maxAstride);
-*/
#ifdef CALIBRATOR_CREATE_PLOTS
	sprintf(fnn1, "%s.cache-associativity", fname);
	sprintf(fnx1, "%s.data", fnn1);
	if (!(fp = fopen(fnx1, "w")))
		ErrXit("main: 'fp = fopen(%s,\"w\")` failed", fnx1);
	result1 = runAsso(array, maxrange, mincachesize, 0, minTLBentries, MHz, fp, &maxAstride);
	fclose(fp);
#else
	result1 = runAsso(array, maxrange, mincachesize, 0, minTLBentries, MHz, 0, &maxAstride);
#endif
	result2 = result1;

	Asso = analyzeAsso(result1, result2, maxlinesize, minTLBentries, cache->levels, MHz);
	/*delayT = Asso->latency2[0] - Asso->latency1[0]; */

#ifdef CALIBRATOR_CREATE_PLOTS
	sprintf(fnx1, "%s.gp", fnn1);
	if (!(fp = fopen(fnx1, "w")))
		ErrXit("main: 'fp = fopen(%s,\"w\")` failed", fnx1);
	plotAsso(Asso, result1, MHz, fnn1, fp, 0, cache);
	fclose(fp);
#endif

	yy = (result1[0][0] >> 24) - 1;
	for (y = 0; y <= yy; y++) {
		free(result1[y]);
		result1[y] = 0;
	}
	free(result1);
	result1 = 0;

	result2 = 0;

#ifdef CALIBRATOR_PRINT_OUTPUT
	fflush(stderr);
	fprintf(stdout, "\n");
#endif

/*
	printAsso(Asso, MHz);
*/

	if (!(calibratorInfo = (struct fullInfo *) malloc(sizeof(struct fullInfo))))
		fatalex("malloc");
	calibratorInfo->delayC = delayC;
	calibratorInfo->cache = cache;
	calibratorInfo->Asso = Asso;
	calibratorInfo->TLB = TLB;
#ifdef CALIBRATOR_CHECK_SMP
	checkSMP(&(calibratorInfo->smp));
#endif
	return (calibratorInfo);

}

void
freeFullInfo(struct fullInfo *caliInfo)
{
	free(caliInfo->cache);
	caliInfo->cache = 0;
	free(caliInfo->Asso);
	caliInfo->Asso = 0;
	free(caliInfo->TLB);
	caliInfo->TLB = 0;
	free(caliInfo);
}
