/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * Foteini Alvanaki
 */

#include "geom.h"
#include <omp.h>

#define GEOMBULK_DEBUG 0
#define OPENCL_DYNAMIC 0
#define OPENCL_THREADS 1

/*******************************/
/********** One input **********/
/*******************************/

str
geom_2_geom_bat(bat *outBAT_id, bat *inBAT_id, int *columnType, int *columnSRID)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;
    wkb **outs = NULL;
	str msg = MAL_SUCCEED;
#ifdef GEOMBULK_DEBUG
    static struct timeval start, stop;
    unsigned long long t;
#endif

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batcalc.wkb", RUNTIME_OBJECT_MISSING);
	}

	//create a new BAT, aligned with input BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batcalc.wkb", MAL_MALLOC_FAIL);
	}

	//iterator over the BAT
	inBAT_iter = bat_iterator(inBAT);

    omp_set_dynamic(OPENCL_DYNAMIC);     // Explicitly disable dynamic teams
    omp_set_num_threads(OPENCL_THREADS);
    q = BUNlast(inBAT);
#ifdef GEOMBULK_DEBUG
    fprintf(stdout, "batcalc.wkb %d %d\n", p, q);
    gettimeofday(&start, NULL);
#endif
    outs = (wkb**) GDKmalloc(sizeof(wkb*) * BATcount(inBAT));
	//BATloop(inBAT, p, q) {	//iterate over all valid elements
    #pragma omp parallel for
    for (p = 0; p < q; p++) {
		str err = NULL;
	    wkb *inWKB = NULL, *outWKB = NULL;

		//if for used --> inWKB = (wkb *) BUNtail(inBATi, i + BUNfirst(inBAT));
		inWKB = (wkb *) BUNtail(inBAT_iter, p);
		if ((err = geom_2_geom(&outWKB, &inWKB, columnType, columnSRID)) != MAL_SUCCEED) {	//check type
            msg = err;
            #pragma omp cancelregion
		}
        outs[p] = outWKB;
		//BUNappend(outBAT, outWKB, TRUE);	//add the point to the new BAT
		//GDKfree(outWKB);
		//outWKB = NULL;
	}
#ifdef GEOMBULK_DEBUG
    gettimeofday(&stop, NULL);
    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
    fprintf(stdout, "batcalc.wkb %llu ms\n", t);
#endif

	BBPunfix(inBAT->batCacheid);

    if (msg != MAL_SUCCEED) {
        BBPunfix(outBAT->batCacheid);
        return msg;
    }

#ifdef GEOMBULK_DEBUG
    gettimeofday(&start, NULL);
#endif
    for (p = 0; p < q; p++) {
		BUNappend(outBAT, outs[p], TRUE);	//add the point to the new BAT
		GDKfree(outs[p]);
		outs[p] = NULL;
    }

    if (outs)
        GDKfree(outs);
#ifdef GEOMBULK_DEBUG
    gettimeofday(&stop, NULL);
    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
    fprintf(stdout, "batcalc.wkb BUNappend %llu ms\n", t);
#endif

	BATsetcount(outBAT, BATcount(inBAT));
	BATderiveProps(outBAT, FALSE);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

/*create WKB from WKT */
str
wkbFromText_bat(bat *outBAT_id, bat *inBAT_id, int *srid, int *tpe)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	char *inWKB = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.wkbFromText", RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.wkbFromText", MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;
		wkb *outSingle;

		inWKB = (char *) BUNtail(inBAT_iter, p);
		if ((err = wkbFromText(&outSingle, &inWKB, srid, tpe)) != MAL_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		BUNappend(outBAT, outSingle, TRUE);	//add the result to the new BAT
		GDKfree(outSingle);
		outSingle = NULL;
	}

	//set the number of elements in the outBAT
	BATsetcount(outBAT, BATcount(inBAT));

	BBPunfix(inBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

/*****************************************************************************/
/********************* IN: mbr - OUT: double - FLAG :int *********************/
/*****************************************************************************/
str
wkbCoordinateFromMBR_bat(bat *outBAT_id, bat *inBAT_id, int *coordinateIdx)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	mbr *inMBR = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;
	str msg = MAL_SUCCEED;
#ifdef GEOMBULK_DEBUG
    static struct timeval start, stop;
    unsigned long long t;
#endif
    double *outs;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.coordinateFromMBR", RUNTIME_OBJECT_MISSING);
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("dbl"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.coordinateFromMBR", MAL_MALLOC_FAIL);
	}

	//iterator over the BAT
	inBAT_iter = bat_iterator(inBAT);

    omp_set_dynamic(OPENCL_DYNAMIC);     // Explicitly disable dynamic teams
    omp_set_num_threads(OPENCL_THREADS);
    q = BUNlast(inBAT);
#ifdef GEOMBULK_DEBUG
    fprintf(stdout, "batgeom.coordinateFromMBR %d %d\n", p, q);
    gettimeofday(&start, NULL);
#endif
    //BATloop(inBAT, p, q) {	//iterate over all valid elements
    outs = (double*) Tloc(outBAT, 0);
    #pragma omp parallel for
	//BATloop(inBAT, p, q) {	//iterate over all valid elements
    for (p = 0; p < q; p++) {
		str err = NULL;
	    //double outDbl = 0.0;

		inMBR = (mbr *) BUNtail(inBAT_iter, p);
		//if ((err = wkbCoordinateFromMBR(&outDbl, &inMBR, coordinateIdx)) != MAL_SUCCEED) {
		if ((err = wkbCoordinateFromMBR(&outs[p], &inMBR, coordinateIdx)) != MAL_SUCCEED) {
            msg = err;
            #pragma omp cancelregion
		}
		//BUNappend(outBAT, &outDbl, TRUE);
	}
#ifdef GEOMBULK_DEBUG
    gettimeofday(&stop, NULL);
    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
    fprintf(stdout, "batgeom.coordinateFromMBR %llu ms\n", t);
#endif

	BBPunfix(inBAT->batCacheid);

    if (msg != MAL_SUCCEED) {
        BBPunfix(outBAT->batCacheid);
        return msg;
    }

	//set some properties of the new BAT
	BATsetcount(outBAT, BATcount(inBAT));
	BATsettrivprop(outBAT);
	//BATderiveProps(outBAT, FALSE);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

/**************************************************************************/
/********************* IN: wkb - OUT: str - FLAG :int *********************/
/**************************************************************************/
static str
WKBtoSTRflagINT_bat(bat *outBAT_id, bat *inBAT_id, int *flag, str (*func) (char **, wkb **, int *), const char *name)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("str"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;
		char *outSingle;

		inWKB = (wkb *) BUNtail(inBAT_iter, p);
		if ((err = (*func) (&outSingle, &inWKB, flag)) != MAL_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		BUNappend(outBAT, outSingle, TRUE);	//add the result to the new BAT
		GDKfree(outSingle);
		outSingle = NULL;
	}

	//set the number of elements in the outBAT
	BATsetcount(outBAT, BATcount(inBAT));

	BBPunfix(inBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

/*create textual representation of the wkb */
str
wkbAsText_bat(bat *outBAT_id, bat *inBAT_id, int *withSRID)
{
	return WKBtoSTRflagINT_bat(outBAT_id, inBAT_id, withSRID, wkbAsText, "batgeom.wkbAsText");
}

str
wkbGeometryType_bat(bat *outBAT_id, bat *inBAT_id, int *flag)
{
	return WKBtoSTRflagINT_bat(outBAT_id, inBAT_id, flag, wkbGeometryType, "batgeom.wkbGeometryType");
}

/***************************************************************************/
/*************************** IN: wkb - OUT: dbl ****************************/
/***************************************************************************/
static str
WKBtoDBL_bat(bat *outBAT_id, bat *inBAT_id, str (*func) (dbl *, wkb **), const char *name)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;
	str msg = MAL_SUCCEED;
#ifdef GEOMBULK_DEBUG
    static struct timeval start, stop;
    unsigned long long t;
#endif
    double *outs;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("dbl"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
	
    omp_set_dynamic(OPENCL_DYNAMIC);     // Explicitly disable dynamic teams
    omp_set_num_threads(OPENCL_THREADS);
    q = BUNlast(inBAT);
#ifdef GEOMBULK_DEBUG
    fprintf(stdout, "%s %d %d\n", name, p, q);
    gettimeofday(&start, NULL);
#endif
    //BATloop(inBAT, p, q) {	//iterate over all valid elements
    outs = (double*) Tloc(outBAT, 0);
    #pragma omp parallel for
    for (p = 0; p < q; p++) {
		str err = NULL;
	    wkb *inWKB = NULL;
		//double outSingle;

		inWKB = (wkb *) BUNtail(inBAT_iter, p);
		//if ((err = (*func) (&outSingle, &inWKB)) != MAL_SUCCEED) {
		if ((err = (*func) (&outs[p], &inWKB)) != MAL_SUCCEED) {
            msg = err;
            #pragma omp cancelregion
		}
		//BUNappend(outBAT, &outSingle, TRUE);	//add the result to the new BAT
	}
#ifdef GEOMBULK_DEBUG
    gettimeofday(&stop, NULL);
    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
    fprintf(stdout, "%s %llu ms\n", name, t);
#endif

	BBPunfix(inBAT->batCacheid);

    if (msg != MAL_SUCCEED) {
        BBPunfix(outBAT->batCacheid);
        return msg;
    }

	BATsetcount(outBAT, BATcount(inBAT));
    BATderiveProps(outBAT, FALSE);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

str 
wkbArea_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoDBL_bat(outBAT_id, inBAT_id, wkbArea, "batgeom.wkbArea");
}


/***************************************************************************/
/*************************** IN: wkb - OUT: wkb ****************************/
/***************************************************************************/

static str
WKBtoWKB_bat(bat *outBAT_id, bat *inBAT_id, str (*func) (wkb **, wkb **), const char *name)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;
    wkb **outs = NULL;
	str msg = MAL_SUCCEED;
#ifdef GEOMBULK_DEBUG
    static struct timeval start, stop;
    unsigned long long t;
#endif

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
	
    omp_set_dynamic(OPENCL_DYNAMIC);     // Explicitly disable dynamic teams
    omp_set_num_threads(OPENCL_THREADS);
    q = BUNlast(inBAT);
#ifdef GEOMBULK_DEBUG
    fprintf(stdout, "%s %d %d\n", name, p, q);
    gettimeofday(&start, NULL);
#endif
    outs = (wkb**) GDKmalloc(sizeof(wkb*) * BATcount(inBAT));
    //BATloop(inBAT, p, q) {	//iterate over all valid elements
    #pragma omp parallel for
    for (p = 0; p < q; p++) {
		str err = NULL;
		wkb *outSingle;

		inWKB = (wkb *) BUNtail(inBAT_iter, p);
		if ((err = (*func) (&outSingle, &inWKB)) != MAL_SUCCEED) {
            msg = err;
            #pragma omp cancelregion
		}
        outs[p] = outSingle;
		//BUNappend(outBAT, outSingle, TRUE);	//add the result to the new BAT
		//GDKfree(outSingle);
		//outSingle = NULL;
	}
#ifdef GEOMBULK_DEBUG
    gettimeofday(&stop, NULL);
    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
    fprintf(stdout, "%s %llu ms\n", name, t);
#endif

	BBPunfix(inBAT->batCacheid);

    if (msg != MAL_SUCCEED) {
        BBPunfix(outBAT->batCacheid);
        return msg;
    }

#ifdef GEOMBULK_DEBUG
    gettimeofday(&start, NULL);
#endif
    for (p = 0; p < q; p++) {
		BUNappend(outBAT, outs[p], TRUE);	//add the point to the new BAT
		GDKfree(outs[p]);
		outs[p] = NULL;
    }

    if (outs)
        GDKfree(outs);
#ifdef GEOMBULK_DEBUG
    gettimeofday(&stop, NULL);
    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
    fprintf(stdout, "batcalc.wkb BUNappend %llu ms\n", t);
#endif

	//set the number of elements in the outBAT
	BATsetcount(outBAT, BATcount(inBAT));
    BATderiveProps(outBAT, FALSE);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

str
wkbBoundary_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoWKB_bat(outBAT_id, inBAT_id, wkbBoundary, "batgeom.wkbBoundary");
}

str
wkbCentroid_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoWKB_bat(outBAT_id, inBAT_id, wkbCentroid, "batgeom.wkbCentroid");
}

/**************************************************************************************/
/*************************** IN: wkb - OUT: wkb - FLAG:int ****************************/
/**************************************************************************************/

static str
WKBtoWKBflagINT_bat(bat *outBAT_id, bat *inBAT_id, const int *flag, str (*func) (wkb **, wkb **, const int *), const char *name)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;
    wkb **outs = NULL;
	str msg = MAL_SUCCEED;
#ifdef GEOMBULK_DEBUG
    static struct timeval start, stop;
    unsigned long long t;
#endif

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);

    omp_set_dynamic(OPENCL_DYNAMIC);     // Explicitly disable dynamic teams
    omp_set_num_threads(OPENCL_THREADS);
    q = BUNlast(inBAT);
#ifdef GEOMBULK_DEBUG
    fprintf(stdout, "%s %d %d\n", name, p, q);
    gettimeofday(&start, NULL);
#endif
    outs = (wkb**) GDKmalloc(sizeof(wkb*) * BATcount(inBAT));
    //BATloop(inBAT, p, q) {	//iterate over all valid elements
    #pragma omp parallel for
    for (p = 0; p < q; p++) {
		str err = NULL;
		wkb *outSingle;

		inWKB = (wkb *) BUNtail(inBAT_iter, p);
		if ((err = (*func) (&outSingle, &inWKB, flag)) != MAL_SUCCEED) {
            msg = err;
            #pragma omp cancelregion
		}
        outs[p] = outSingle;
		//BUNappend(outBAT, outSingle, TRUE);	//add the result to the new BAT
		//GDKfree(outSingle);
		//outSingle = NULL;
	}
#ifdef GEOMBULK_DEBUG
    gettimeofday(&stop, NULL);
    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
    fprintf(stdout, "%s %llu ms\n", name, t);
#endif

	BBPunfix(inBAT->batCacheid);

    if (msg != MAL_SUCCEED) {
        BBPunfix(outBAT->batCacheid);
        return msg;
    }

#ifdef GEOMBULK_DEBUG
    gettimeofday(&start, NULL);
#endif
    for (p = 0; p < q; p++) {
		BUNappend(outBAT, outs[p], TRUE);	//add the point to the new BAT
		GDKfree(outs[p]);
		outs[p] = NULL;
    }

    if (outs)
        GDKfree(outs);
#ifdef GEOMBULK_DEBUG
    gettimeofday(&stop, NULL);
    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
    fprintf(stdout, "batcalc.wkb BUNappend %llu ms\n", t);
#endif

	BATsetcount(outBAT, BATcount(inBAT));
    BATderiveProps(outBAT, FALSE);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

str
wkbGeometryN_bat(bat *outBAT_id, bat *inBAT_id, const int *flag)
{
	return WKBtoWKBflagINT_bat(outBAT_id, inBAT_id, flag, wkbGeometryN, "batgeom.wkbGeometryN");
}

str
wkbForceDim_bat(bat *outBAT_id, bat *inBAT_id, const int *flag)
{
    return WKBtoWKBflagINT_bat(outBAT_id, inBAT_id, flag, wkbForceDim, "batgeom.wkbForceDim");
}
/***************************************************************************/
/*************************** IN: wkb - OUT: bit ****************************/
/***************************************************************************/

static str
WKBtoBIT_bat(bat *outBAT_id, bat *inBAT_id, str (*func) (bit *, wkb **), const char *name)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;
	str msg = MAL_SUCCEED;
#ifdef GEOMBULK_DEBUG
    static struct timeval start, stop;
    unsigned long long t;
#endif
    bit *outs;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("bit"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);

    omp_set_dynamic(OPENCL_DYNAMIC);     // Explicitly disable dynamic teams
    omp_set_num_threads(16);
    q = BUNlast(inBAT);
#ifdef GEOMBULK_DEBUG
    fprintf(stdout, "%s %d %d\n", name, p, q);
    gettimeofday(&start, NULL);
#endif
	outs = (bit *) Tloc(outBAT, 0);
	//BATloop(inBAT, p, q) {	//iterate over all valid elements
    #pragma omp parallel for
    for (p = 0; p < q; p++) {
        str err = NULL;
	    wkb *inWKB = NULL;
        //bit outSingle;

        inWKB = (wkb *) BUNtail(inBAT_iter, p);
        //if ((err = (*func) (&outSingle, &inWKB)) != MAL_SUCCEED) {
        if ((err = (*func) (&outs[p], &inWKB)) != MAL_SUCCEED) {
            msg = err;
            #pragma omp cancelregion
        }
        //BUNappend(outBAT, &outSingle, TRUE);	//add the result to the new BAT
    }
#ifdef GEOMBULK_DEBUG
    gettimeofday(&stop, NULL);
    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
    fprintf(stdout, "%s %llu ms\n", name, t);
#endif

    BBPunfix(inBAT->batCacheid);

    if (msg != MAL_SUCCEED) {
        BBPunfix(outBAT->batCacheid);
        return msg;
    }

	BATsetcount(outBAT, BATcount(inBAT));
    BATderiveProps(outBAT, FALSE);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

str
wkbIsClosed_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoBIT_bat(outBAT_id, inBAT_id, wkbIsClosed, "batgeom.wkbIsClosed");
}

str
wkbIsEmpty_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoBIT_bat(outBAT_id, inBAT_id, wkbIsEmpty, "batgeom.wkbIsEmpty");
}

str
wkbIsSimple_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoBIT_bat(outBAT_id, inBAT_id, wkbIsSimple, "batgeom.wkbIsSimple");
}

str
wkbIsRing_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoBIT_bat(outBAT_id, inBAT_id, wkbIsRing, "batgeom.wkbIsRing");
}

str
wkbIsValid_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoBIT_bat(outBAT_id, inBAT_id, wkbIsValid, "batgeom.wkbIsValid");
}

/***************************************************************************/
/*************************** IN: wkb wkb - OUT: bit ****************************/
/***************************************************************************/

static str
WKBWKBtoBIT_bat(bat *outBAT_id, bat *aBAT_id, bat *bBAT_id, str (*func) (bit *, wkb **, wkb **), const char *name)
{
	BAT *outBAT = NULL, *aBAT = NULL, *bBAT = NULL;
	BUN p = 0, q = 0;
	BATiter aBAT_iter, bBAT_iter;
	str msg = MAL_SUCCEED;
#ifdef GEOMBULK_DEBUG
    static struct timeval start, stop;
    unsigned long long t;
#endif
    bit *outs = NULL;

	//get the descriptor of the BAT
	if ((aBAT = BATdescriptor(*aBAT_id)) == NULL) {
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}
	if ((bBAT = BATdescriptor(*bBAT_id)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(aBAT->hseqbase, ATOMindex("bit"), BATcount(aBAT), TRANSIENT)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, name, MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

    omp_set_dynamic(OPENCL_DYNAMIC);     // Explicitly disable dynamic teams
    omp_set_num_threads(OPENCL_THREADS);

    q = BUNlast(aBAT);
#ifdef GEOMBULK_DEBUG
    fprintf(stdout, "%s %d %d\n", name, p, q);
    gettimeofday(&start, NULL);
#endif
	outs = (bit *) Tloc(outBAT, 0);
	//BATloop(inBAT, p, q) {	//iterate over all valid elements
    #pragma omp parallel for
    for (p = 0; p < q; p++) {
        wkb *aWKB = NULL, *bWKB = NULL;
        //bit out;
        str err = NULL;

        aWKB = (wkb *) BUNtail(aBAT_iter, p);
        bWKB = (wkb *) BUNtail(bBAT_iter, p);
        //if ((err = (*func) (&out, &aWKB, &bWKB)) != MAL_SUCCEED) {
        if ((err = (*func) (&outs[p], &aWKB, &bWKB)) != MAL_SUCCEED) {
            msg = err;
            #pragma omp cancelregion
        }
        //outs[p] = out;
        //BUNappend(outBAT, &out, TRUE);	//add the result to the new BAT
    }
#ifdef GEOMBULK_DEBUG
    gettimeofday(&stop, NULL);
    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
    fprintf(stdout, "%s %llu ms\n", name, t);
#endif

    BBPunfix(aBAT->batCacheid);
    BBPunfix(bBAT->batCacheid);

    if (msg != MAL_SUCCEED) {
        BBPunfix(outBAT->batCacheid);
        return msg;
    }

    BATsetcount(outBAT, q);
    BATderiveProps(outBAT, FALSE);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

str
wkbIntersects_bat(bat *outBAT_id, bat *aBAT_id, bat *bBAT_id)
{
	return WKBWKBtoBIT_bat(outBAT_id, aBAT_id, bBAT_id, wkbIntersects, "batgeom.wkbIntersects");
}

/******************************************************************************************/
/************************* IN: wkb dbl dbl dbl - OUT: bit - SRID **************************/
/******************************************************************************************/

static str
WKBtoBITxyzDBL_bat(bat *outBAT_id, bat *inBAT_id, bat *inXBAT_id, double *dx, bat *inYBAT_id, double *dy, bat *inZBAT_id, double * dz, int *srid, str (*func) (bit *, wkb **, double *x, double *y, double *z, int *srid), const char *name)
{
	BAT *outBAT = NULL, *inBAT = NULL, *inXBAT = NULL, *inYBAT = NULL, *inZBAT = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter, inXBAT_iter, inYBAT_iter, inZBAT_iter;
	str msg = MAL_SUCCEED;
#ifdef GEOMBULK_DEBUG
    static struct timeval start, stop;
    unsigned long long t;
#endif
    bit *outs = NULL;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}
	if (*inXBAT_id != bat_nil && (inXBAT = BATdescriptor(*inXBAT_id)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}
	if (*inYBAT_id != bat_nil && (inYBAT = BATdescriptor(*inYBAT_id)) == NULL) {
		BBPunfix(inBAT->batCacheid);
        if (*inXBAT_id != bat_nil)
    		BBPunfix(inXBAT->batCacheid);
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}
	if (*inZBAT_id != bat_nil && (inZBAT = BATdescriptor(*inZBAT_id)) == NULL) {
		BBPunfix(inBAT->batCacheid);
        if (*inXBAT_id != bat_nil)
    		BBPunfix(inXBAT->batCacheid);
        if (*inYBAT_id != bat_nil)
	    	BBPunfix(inYBAT->batCacheid);
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("bit"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
        if (*inXBAT_id != bat_nil)
		    BBPunfix(inXBAT->batCacheid);
        if (*inYBAT_id != bat_nil)
    		BBPunfix(inYBAT->batCacheid);
        if (*inZBAT_id != bat_nil)
    		BBPunfix(inZBAT->batCacheid);
		throw(MAL, name, MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
    
    if (*inXBAT_id != bat_nil)
    	inXBAT_iter = bat_iterator(inXBAT);
    if (*inYBAT_id != bat_nil)
    	inYBAT_iter = bat_iterator(inYBAT);
    if (*inZBAT_id != bat_nil)
        inZBAT_iter = bat_iterator(inZBAT);

    omp_set_dynamic(OPENCL_DYNAMIC);     // Explicitly disable dynamic teams
    omp_set_num_threads(OPENCL_THREADS);
    q = BUNlast(inBAT);
#ifdef GEOMBULK_DEBUG
    fprintf(stdout, "%s, %d %d\n", name, p, q);
    gettimeofday(&start, NULL);
#endif
	outs = (bit *) Tloc(outBAT, 0);
	//BATloop(inBAT, p, q) {	//iterate over all valid elements
    #pragma omp parallel for
    for (p = 0; p < q; p++) {
		str err = NULL;
	    wkb *inWKB = NULL;
		//bit outSingle;
        double x, y, z;


        inWKB = (wkb *) BUNtail(inBAT_iter, p);

        if (*inXBAT_id != bat_nil)
            x = *(double *) BUNtail(inXBAT_iter, p);
        else
            x = *dx;
        if (*inYBAT_id != bat_nil)
            y = *(double *) BUNtail(inYBAT_iter, p);
        else
            y = *dy;
        if (*inZBAT_id != bat_nil)
            z = *(double *) BUNtail(inZBAT_iter, p);
        else
            z = *dz;

        //if ((err = (*func) (&outSingle, &inWKB, &x, &y, &z, srid)) != MAL_SUCCEED) {
        if ((err = (*func) (&outs[p], &inWKB, &x, &y, &z, srid)) != MAL_SUCCEED) {
            msg = err;
            #pragma omp cancelregion
		}
		//BUNappend(outBAT, &outSingle, TRUE);	//add the result to the new BAT
	}
#ifdef GEOMBULK_DEBUG
    gettimeofday(&stop, NULL);
    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
    fprintf(stdout, "%s %llu ms\n", name, t);
#endif

    BBPunfix(inBAT->batCacheid);
    if (*inXBAT_id != bat_nil)
        BBPunfix(inXBAT->batCacheid);
    if (*inYBAT_id != bat_nil)
        BBPunfix(inYBAT->batCacheid);
    if (*inZBAT_id != bat_nil)
        BBPunfix(inZBAT->batCacheid);

    if (msg != MAL_SUCCEED) {
        BBPunfix(outBAT->batCacheid);
    }

	BATsetcount(outBAT, BATcount(inBAT));
    BATderiveProps(outBAT, FALSE);
    BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

str
wkbIntersectsXYZ_bat(bat *outBAT_id, bat *inBAT_id, bat *inXBAT_id, double *dx, bat *inYBAT_id, double *dy, bat *inZBAT_id, double *dz, int* srid)
{
	return WKBtoBITxyzDBL_bat(outBAT_id, inBAT_id, inXBAT_id, dx, inYBAT_id, dy, inZBAT_id, dz, srid, wkbIntersectsXYZ, "batgeom.IntersectsXYZ");
}

/***************************************************************************/
/*************************** IN: wkb - OUT: int ****************************/
/***************************************************************************/

static str
WKBtoINT_bat(bat *outBAT_id, bat *inBAT_id, str (*func) (int *, wkb **), const char *name)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;
	str msg = MAL_SUCCEED;
#ifdef GEOMBULK_DEBUG
    static struct timeval start, stop;
    unsigned long long t;
#endif
    int *outs;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("int"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);

    omp_set_dynamic(OPENCL_DYNAMIC);     // Explicitly disable dynamic teams
    omp_set_num_threads(OPENCL_THREADS);
    q = BUNlast(inBAT);
#ifdef GEOMBULK_DEBUG
    fprintf(stdout, "%s %d %d\n", name, p, q);
    gettimeofday(&start, NULL);
#endif
    //BATloop(inBAT, p, q) {	//iterate over all valid elements
    outs = (int*) Tloc(outBAT, 0);
    #pragma omp parallel for
    for (p = 0; p < q; p++) {
		str err = NULL;
		//int outSingle;

		inWKB = (wkb *) BUNtail(inBAT_iter, p);
		//if ((err = (*func) (&outSingle, &inWKB)) != MAL_SUCCEED) {
		if ((err = (*func) (&outs[p], &inWKB)) != MAL_SUCCEED) {
            msg = err;
            #pragma omp cancelregion
		}
		//BUNappend(outBAT, &outSingle, TRUE);	//add the result to the new BAT
	}
#ifdef GEOMBULK_DEBUG
    gettimeofday(&stop, NULL);
    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
    fprintf(stdout, "%s %llu ms\n", name, t);
#endif

	BBPunfix(inBAT->batCacheid);

    if (msg != MAL_SUCCEED) {
        BBPunfix(outBAT->batCacheid);
        return msg;
    }

	BATsetcount(outBAT, BATcount(inBAT));
    BATderiveProps(outBAT, FALSE);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;

}

str
wkbDimension_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoINT_bat(outBAT_id, inBAT_id, wkbDimension, "batgeom.wkbDimension");
}

str
wkbNumGeometries_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoINT_bat(outBAT_id, inBAT_id, wkbNumGeometries, "batgeom.wkbNumGeometries");
}

/***************************************************************************************/
/*************************** IN: wkb - OUT: int - FLAG: int ****************************/
/***************************************************************************************/

static str
WKBtoINTflagINT_bat(bat *outBAT_id, bat *inBAT_id, int *flag, str (*func) (int *, wkb **, int *), const char *name)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;
	str msg = MAL_SUCCEED;
#ifdef GEOMBULK_DEBUG
    static struct timeval start, stop;
    unsigned long long t;
#endif
    int *outs;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("int"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);

    omp_set_dynamic(OPENCL_DYNAMIC);     // Explicitly disable dynamic teams
    omp_set_num_threads(OPENCL_THREADS);
    q = BUNlast(inBAT);
#ifdef GEOMBULK_DEBUG
    fprintf(stdout, "%s %d %d\n", name, p, q);
    gettimeofday(&start, NULL);
#endif
    //BATloop(inBAT, p, q) {	//iterate over all valid elements
    outs = (int*) Tloc(outBAT, 0);
    #pragma omp parallel for
    for (p = 0; p < q; p++) {
		str err = NULL;
		//int outSingle;

		inWKB = (wkb *) BUNtail(inBAT_iter, p);
		//if ((err = (*func) (&outSingle, &inWKB, flag)) != MAL_SUCCEED) {
		if ((err = (*func) (&outs[p], &inWKB, flag)) != MAL_SUCCEED) {
            msg = err;
            #pragma omp cancelregion
		}
		//BUNappend(outBAT, &outSingle, TRUE);	//add the result to the new BAT
	}
#ifdef GEOMBULK_DEBUG
    gettimeofday(&stop, NULL);
    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
    fprintf(stdout, "%s %llu ms\n", name, t);
#endif

	BBPunfix(inBAT->batCacheid);

    if (msg != MAL_SUCCEED) {
        BBPunfix(outBAT->batCacheid);
        return msg;
    }

	//set the number of elements in the outBAT
	BATsetcount(outBAT, BATcount(inBAT));
    BATderiveProps(outBAT, FALSE);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

str
wkbNumPoints_bat(bat *outBAT_id, bat *inBAT_id, int *flag)
{
	return WKBtoINTflagINT_bat(outBAT_id, inBAT_id, flag, wkbNumPoints, "batgeom.wkbNumPoints");
}

str
wkbNumRings_bat(bat *outBAT_id, bat *inBAT_id, int *flag)
{
	return WKBtoINTflagINT_bat(outBAT_id, inBAT_id, flag, wkbNumRings, "batgeom.wkbNumRings");
}

/******************************************************************************************/
/*************************** IN: wkb - OUT: double - FLAG: int ****************************/
/******************************************************************************************/

str
wkbGetCoordinate_bat(bat *outBAT_id, bat *inBAT_id, int *flag)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;
	str msg = MAL_SUCCEED;
#ifdef GEOMBULK_DEBUG
    static struct timeval start, stop;
    unsigned long long t;
#endif
    double *outs;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.wkbGetCoordinate", RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("dbl"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.wkbGetCoordinate", MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);

    omp_set_dynamic(OPENCL_DYNAMIC);     // Explicitly disable dynamic teams
    omp_set_num_threads(OPENCL_THREADS);
    q = BUNlast(inBAT);
#ifdef GEOMBULK_DEBUG
    fprintf(stdout, "batgeom.wkbGetCoordinate %d %d\n", p, q);
    gettimeofday(&start, NULL);
#endif
    //BATloop(inBAT, p, q) {	//iterate over all valid elements
    outs = (double*) Tloc(outBAT, 0);
    #pragma omp parallel for
    for (p = 0; p < q; p++) {
		str err = NULL;
		double outSingle;

		inWKB = (wkb *) BUNtail(inBAT_iter, p);
		//if ((err = wkbGetCoordinate(&outSingle, &inWKB, flag)) != MAL_SUCCEED) {
		if ((err = wkbGetCoordinate(&outs[p], &inWKB, flag)) != MAL_SUCCEED) {
            msg = err;
            #pragma omp cancelregion
		}
		//BUNappend(outBAT, &outSingle, TRUE);	//add the result to the new BAT
	}
#ifdef GEOMBULK_DEBUG
    gettimeofday(&stop, NULL);
    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
    fprintf(stdout, "batgeom.wkbGetCoordinate %llu ms\n", t);
#endif

	BBPunfix(inBAT->batCacheid);

    if (msg != MAL_SUCCEED) {
        BBPunfix(outBAT->batCacheid);
        return msg;
    }

	BATsetcount(outBAT, BATcount(inBAT));
    BATderiveProps(outBAT, FALSE);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

/******************************************************************************************/
/***************************** IN: wkb dbl dbl dbl - OUT: wkb *****************************/
/******************************************************************************************/

static str
WKBtoWKBxyzDBL_bat(bat *outBAT_id, bat *inBAT_id, bat *inXBAT_id, double *dx, bat *inYBAT_id, double *dy, bat *inZBAT_id, double * dz, str (*func) (wkb **, wkb **, double *x, double *y, double *z), const char *name)
{
	BAT *outBAT = NULL, *inBAT = NULL, *inXBAT = NULL, *inYBAT = NULL, *inZBAT = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter, inXBAT_iter, inYBAT_iter, inZBAT_iter;
	str msg = MAL_SUCCEED;
    wkb **outs = NULL;
#ifdef GEOMBULK_DEBUG
    static struct timeval start, stop;
    unsigned long long t;
#endif

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}
	if (*inXBAT_id != bat_nil && (inXBAT = BATdescriptor(*inXBAT_id)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}
	if (*inYBAT_id != bat_nil && (inYBAT = BATdescriptor(*inYBAT_id)) == NULL) {
		BBPunfix(inBAT->batCacheid);
        if (*inXBAT_id != bat_nil)
    		BBPunfix(inXBAT->batCacheid);
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}
	if (*inZBAT_id != bat_nil && (inZBAT = BATdescriptor(*inZBAT_id)) == NULL) {
		BBPunfix(inBAT->batCacheid);
        if (*inXBAT_id != bat_nil)
    		BBPunfix(inXBAT->batCacheid);
        if (*inYBAT_id != bat_nil)
	    	BBPunfix(inYBAT->batCacheid);
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
        if (*inXBAT_id != bat_nil)
		    BBPunfix(inXBAT->batCacheid);
        if (*inYBAT_id != bat_nil)
    		BBPunfix(inYBAT->batCacheid);
        if (*inZBAT_id != bat_nil)
    		BBPunfix(inZBAT->batCacheid);
		throw(MAL, name, MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
    
    if (*inXBAT_id != bat_nil)
    	inXBAT_iter = bat_iterator(inXBAT);
    if (*inYBAT_id != bat_nil)
    	inYBAT_iter = bat_iterator(inYBAT);
    if (*inZBAT_id != bat_nil)
        inZBAT_iter = bat_iterator(inZBAT);


    omp_set_dynamic(OPENCL_DYNAMIC);     // Explicitly disable dynamic teams
    omp_set_num_threads(OPENCL_THREADS);
    q = BUNlast(inBAT);
#ifdef GEOMBULK_DEBUG
    fprintf(stdout, "%s %d %d\n", name, p, q);
    gettimeofday(&start, NULL);
#endif
    outs = (wkb**) GDKmalloc(sizeof(wkb*) * BATcount(inBAT));
    //BATloop(inBAT, p, q) {	//iterate over all valid elements
    #pragma omp parallel for
    for (p = 0; p < q; p++) {
		str err = NULL;
	    wkb *inWKB = NULL;
		wkb *outSingle;
        double x, y, z;


        inWKB = (wkb *) BUNtail(inBAT_iter, p);

        if (*inXBAT_id != bat_nil)
            x = *(double *) BUNtail(inXBAT_iter, p);
        else
            x = *dx;
        if (*inYBAT_id != bat_nil)
            y = *(double *) BUNtail(inYBAT_iter, p);
        else
            y = *dy;
        if (*inZBAT_id != bat_nil)
            z = *(double *) BUNtail(inZBAT_iter, p);
        else
            z = *dz;

        if ((err = (*func) (&outSingle, &inWKB, &x, &y, &z)) != MAL_SUCCEED) {
            msg = err;
            #pragma omp cancelregion
		}
        outs[p] = outSingle;
		//BUNappend(outBAT, outSingle, TRUE);	//add the result to the new BAT
		//GDKfree(outSingle);
		//outSingle = NULL;
	}
#ifdef GEOMBULK_DEBUG
    gettimeofday(&stop, NULL);
    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
    fprintf(stdout, "batcalc.wkb %llu ms\n", t);
#endif

    BBPunfix(inBAT->batCacheid);
    if (*inXBAT_id != bat_nil)
        BBPunfix(inXBAT->batCacheid);
    if (*inYBAT_id != bat_nil)
        BBPunfix(inYBAT->batCacheid);
    if (*inZBAT_id != bat_nil)
        BBPunfix(inZBAT->batCacheid);

    if (msg != MAL_SUCCEED) {
        BBPunfix(outBAT->batCacheid);
        return msg;
    }

#ifdef GEOMBULK_DEBUG
    gettimeofday(&start, NULL);
#endif
    for (p = 0; p < q; p++) {
		BUNappend(outBAT, outs[p], TRUE);	//add the point to the new BAT
		GDKfree(outs[p]);
		outs[p] = NULL;
    }

    if (outs)
        GDKfree(outs);
#ifdef GEOMBULK_DEBUG
    gettimeofday(&stop, NULL);
    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
    fprintf(stdout, "batcalc.wkb BUNappend %llu ms\n", t);
#endif

	BATsetcount(outBAT, BATcount(inBAT));
    BATderiveProps(outBAT, FALSE);
    BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

str
wkbTranslate_bat(bat *outBAT_id, bat *inBAT_id, bat *inXBAT_id, double *dx, bat *inYBAT_id, double *dy, bat *inZBAT_id, double *dz)
{
	return WKBtoWKBxyzDBL_bat(outBAT_id, inBAT_id, inXBAT_id, dx, inYBAT_id, dy, inZBAT_id, dz, wkbTranslate, "batgeom.Translate");
}

/*******************************/
/********* Two inputs **********/
/*******************************/

str
wkbBox2D_bat(bat *outBAT_id, bat *aBAT_id, bat *bBAT_id)
{
	BAT *outBAT = NULL, *aBAT = NULL, *bBAT = NULL;
	BATiter aBAT_iter, bBAT_iter;
	BUN i = 0;
	str ret = MAL_SUCCEED;

	//get the BATs
	if ((aBAT = BATdescriptor(*aBAT_id)) == NULL || (bBAT = BATdescriptor(*bBAT_id)) == NULL) {
		ret = createException(MAL, "batgeom.wkbBox2D", "Problem retrieving BATs");
		goto clean;
	}
	//check if the BATs are aligned
	if (aBAT->hseqbase != bBAT->hseqbase || BATcount(aBAT) != BATcount(bBAT)) {
		ret = createException(MAL, "batgeom.wkbBox2D", "BATs must be aligned");
		goto clean;
	}
	//create a new BAT for the output
	if ((outBAT = COLnew(aBAT->hseqbase, ATOMindex("mbr"), BATcount(aBAT), TRANSIENT)) == NULL) {
		ret = createException(MAL, "batgeom.wkbBox2D", "Error creating new BAT");
		goto clean;
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = BUNfirst(aBAT); i < BATcount(aBAT); i++) {
		mbr *outSingle;

		wkb *aWKB = (wkb *) BUNtail(aBAT_iter, i + BUNfirst(aBAT));
		wkb *bWKB = (wkb *) BUNtail(bBAT_iter, i + BUNfirst(bBAT));

		if ((ret = wkbBox2D(&outSingle, &aWKB, &bWKB)) != MAL_SUCCEED) {
			BBPunfix(outBAT->batCacheid);
			goto clean;
		}
		BUNappend(outBAT, outSingle, TRUE);	//add the result to the outBAT
		GDKfree(outSingle);
		outSingle = NULL;
	}

	BBPkeepref(*outBAT_id = outBAT->batCacheid);

      clean:
	if (aBAT)
		BBPunfix(aBAT->batCacheid);
	if (bBAT)
		BBPunfix(bBAT->batCacheid);

	return ret;
}

str
wkbContains_bat(bat *outBAT_id, bat *aBAT_id, bat *bBAT_id)
{
	BAT *outBAT = NULL, *aBAT = NULL, *bBAT = NULL;
	BATiter aBAT_iter, bBAT_iter;
	BUN i = 0;
	str ret = MAL_SUCCEED;

	//get the BATs
	if ((aBAT = BATdescriptor(*aBAT_id)) == NULL || (bBAT = BATdescriptor(*bBAT_id)) == NULL) {
		ret = createException(MAL, "batgeom.Contains", "Problem retrieving BATs");
		goto clean;
	}
	//check if the BATs are aligned
	if (aBAT->hseqbase != bBAT->hseqbase || BATcount(aBAT) != BATcount(bBAT)) {
		ret = createException(MAL, "batgeom.Contains", "BATs must be aligned");
		goto clean;
	}
	//create a new BAT for the output
	if ((outBAT = COLnew(aBAT->hseqbase, ATOMindex("bit"), BATcount(aBAT), TRANSIENT)) == NULL) {
		ret = createException(MAL, "batgeom.Contains", "Error creating new BAT");
		goto clean;
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = BUNfirst(aBAT); i < BATcount(aBAT); i++) {
		bit outBIT;

		wkb *aWKB = (wkb *) BUNtail(aBAT_iter, i + BUNfirst(aBAT));
		wkb *bWKB = (wkb *) BUNtail(bBAT_iter, i + BUNfirst(bBAT));

		if ((ret = wkbContains(&outBIT, &aWKB, &bWKB)) != MAL_SUCCEED) {
			BBPunfix(outBAT->batCacheid);
			goto clean;
		}
		BUNappend(outBAT, &outBIT, TRUE);	//add the result to the outBAT
	}

	BBPkeepref(*outBAT_id = outBAT->batCacheid);

      clean:
	if (aBAT)
		BBPunfix(aBAT->batCacheid);
	if (bBAT)
		BBPunfix(bBAT->batCacheid);

	return ret;
}

str
wkbContains_geom_bat(bat *outBAT_id, wkb **geomWKB, bat *inBAT_id)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	BATiter inBAT_iter;
	BUN p = 0, q = 0;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.Contains", "Problem retrieving BAT");
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("bit"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.Contains", "Error creating new BAT");
	}

	//iterator over the BATs
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {
		str err = NULL;
		bit outBIT;

		wkb *inWKB = (wkb *) BUNtail(inBAT_iter, p);

		if ((err = wkbContains(&outBIT, geomWKB, &inWKB)) != MAL_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		BUNappend(outBAT, &outBIT, TRUE);	//add the result to the outBAT
	}

	BBPunfix(inBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;

}

str
wkbContains_bat_geom(bat *outBAT_id, bat *inBAT_id, wkb **geomWKB)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	BATiter inBAT_iter;
	BUN p = 0, q = 0;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.Contains", "Problem retrieving BAT");
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("bit"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.Contains", "Error creating new BAT");
	}

	//iterator over the BATs
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {
		str err = NULL;
		bit outBIT;

		wkb *inWKB = (wkb *) BUNtail(inBAT_iter, p);

		if ((err = wkbContains(&outBIT, &inWKB, geomWKB)) != MAL_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		BUNappend(outBAT, &outBIT, TRUE);	//add the result to the outBAT
	}

	BBPunfix(inBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}



/*
str
wkbFromWKB_bat(bat *outBAT_id, bat *inBAT_id)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb **inWKB = NULL, *outWKB = NULL;
	BUN i;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.wkb", RUNTIME_OBJECT_MISSING);
	}

	//create a new BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT))) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.wkb", MAL_MALLOC_FAIL);
	}

	//pointers to the first valid elements of the x and y BATS
	inWKB = (wkb **) Tloc(inBAT, BUNfirst(inBAT));
	for (i = 0; i < BATcount(inBAT); i++) {	//iterate over all valid elements
		str err = NULL;
		if ((err = wkbFromWKB(&outWKB, &inWKB[i])) != MAL_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		BUNappend(outBAT, outWKB, TRUE);	//add the point to the new BAT
		GDKfree(outWKB);
		outWKB = NULL;
	}

	//set some properties of the new BAT
	BATsetcount(outBAT, BATcount(inBAT));
	BATsettrivprop(outBAT);
	BATderiveProps(outBAT, FALSE);
	BBPunfix(inBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	return MAL_SUCCEED;

}
*/

/************************************/
/********* Multiple inputs **********/
/************************************/
str
wkbMakePoint_bat(bat *outBAT_id, bat *xBAT_id, bat *yBAT_id, bat *zBAT_id, bat *mBAT_id, int *zmFlag)
{
	BAT *outBAT = NULL, *xBAT = NULL, *yBAT = NULL, *zBAT = NULL, *mBAT = NULL;
	BATiter xBAT_iter, yBAT_iter, zBAT_iter, mBAT_iter;
	BUN i;
	str ret = MAL_SUCCEED;

	if (*zmFlag == 11)
		throw(MAL, "batgeom.wkbMakePoint", "POINTZM is not supported");

	//get the BATs
	if ((xBAT = BATdescriptor(*xBAT_id)) == NULL || (yBAT = BATdescriptor(*yBAT_id)) == NULL || (*zmFlag == 10 && (zBAT = BATdescriptor(*zBAT_id)) == NULL)
	    || (*zmFlag == 1 && (mBAT = BATdescriptor(*mBAT_id)) == NULL)) {

		ret = createException(MAL, "batgeom.wkbMakePoint", "Problem retrieving BATs");
		goto clean;
	}
	//check if the BATs are aligned
	if (xBAT->hseqbase != yBAT->hseqbase ||
	    BATcount(xBAT) != BATcount(yBAT) ||
	    (zBAT && (xBAT->hseqbase != zBAT->hseqbase || BATcount(xBAT) != BATcount(zBAT))) ||
	    (mBAT && (xBAT->hseqbase != mBAT->hseqbase || BATcount(xBAT) != BATcount(mBAT)))) {
		ret = createException(MAL, "batgeom.wkbMakePoint", "BATs must be aligned");
		goto clean;
	}
	//create a new BAT for the output
	if ((outBAT = COLnew(xBAT->hseqbase, ATOMindex("wkb"), BATcount(xBAT), TRANSIENT)) == NULL) {
		ret = createException(MAL, "batgeom.wkbMakePoint", "Error creating new BAT");
		goto clean;
	}

	//iterator over the BATs
	xBAT_iter = bat_iterator(xBAT);
	yBAT_iter = bat_iterator(yBAT);
	if (zBAT)
		zBAT_iter = bat_iterator(zBAT);
	if (mBAT)
		mBAT_iter = bat_iterator(mBAT);

	for (i = BUNfirst(xBAT); i < BATcount(xBAT); i++) {
		wkb *pointWKB = NULL;

		double x = *((double *) BUNtail(xBAT_iter, i + BUNfirst(xBAT)));
		double y = *((double *) BUNtail(yBAT_iter, i + BUNfirst(yBAT)));
		double z = 0.0;
		double m = 0.0;

		if (zBAT)
			z = *((double *) BUNtail(zBAT_iter, i + BUNfirst(zBAT)));
		if (mBAT)
			m = *((double *) BUNtail(mBAT_iter, i + BUNfirst(mBAT)));

        if ((ret = wkbMakePoint(&pointWKB, &x, &y, &z, &m, zmFlag)) != MAL_SUCCEED) {	//check
            BBPunfix(outBAT->batCacheid);
            goto clean;
        }
        BUNappend(outBAT, pointWKB, TRUE);	//add the result to the outBAT
        if (pointWKB)
            GDKfree(pointWKB);
        pointWKB = NULL;
    }

	//set the number of elements in the outBAT
	BATsetcount(outBAT, BATcount(xBAT));
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

      clean:
	if (xBAT)
		BBPunfix(xBAT->batCacheid);
	if (yBAT)
		BBPunfix(yBAT->batCacheid);
	if (zBAT)
		BBPunfix(zBAT->batCacheid);
	if (mBAT)
		BBPunfix(mBAT->batCacheid);

	return ret;
}


/* sets the srid of the geometry - BULK version*/
str
wkbSetSRID_bat(bat *outBAT_id, bat *inBAT_id, int *srid)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.SetSRID", "Problem retrieving BAT");
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.SetSRID", "Error creating new BAT");
	}

	//iterator over the BATs
	inBAT_iter = bat_iterator(inBAT);

	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;
		wkb *outWKB = NULL;

		wkb *inWKB = (wkb *) BUNtail(inBAT_iter, p);

        if ((err = wkbSetSRID(&outWKB, &inWKB, srid)) != MAL_SUCCEED) {	//set SRID
            BBPunfix(inBAT->batCacheid);
            BBPunfix(outBAT->batCacheid);
            return err;
		}
		BUNappend(outBAT, outWKB, TRUE);	//add the point to the new BAT
		GDKfree(outWKB);
		outWKB = NULL;
	}

	BBPunfix(inBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

str
wkbDistance_bat(bat *outBAT_id, bat *aBAT_id, bat *bBAT_id)
{
	BAT *outBAT = NULL, *aBAT = NULL, *bBAT = NULL;
	BATiter aBAT_iter, bBAT_iter;
	BUN i = 0;
	str ret = MAL_SUCCEED;

	//get the BATs
	if ((aBAT = BATdescriptor(*aBAT_id)) == NULL || (bBAT = BATdescriptor(*bBAT_id)) == NULL) {
		ret = createException(MAL, "batgeom.Distance", "Problem retrieving BATs");
		goto clean;
	}
	//check if the BATs are aligned
	if (aBAT->hseqbase != bBAT->hseqbase || BATcount(aBAT) != BATcount(bBAT)) {
		ret = createException(MAL, "batgeom.Distance", "BATs must be aligned");
		goto clean;
	}
	//create a new BAT for the output
	if ((outBAT = COLnew(aBAT->hseqbase, ATOMindex("dbl"), BATcount(aBAT), TRANSIENT)) == NULL) {
		ret = createException(MAL, "batgeom.Distance", "Error creating new BAT");
		goto clean;
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = BUNfirst(aBAT); i < BATcount(aBAT); i++) {
		double distanceVal = 0;

		wkb *aWKB = (wkb *) BUNtail(aBAT_iter, i + BUNfirst(aBAT));
		wkb *bWKB = (wkb *) BUNtail(bBAT_iter, i + BUNfirst(bBAT));

		if ((ret = wkbDistance(&distanceVal, &aWKB, &bWKB)) != MAL_SUCCEED) {	//check
			BBPunfix(outBAT->batCacheid);

			goto clean;
		}
		BUNappend(outBAT, &distanceVal, TRUE);	//add the result to the outBAT
	}

	BBPkeepref(*outBAT_id = outBAT->batCacheid);

      clean:
	if (aBAT)
		BBPunfix(aBAT->batCacheid);
	if (bBAT)
		BBPunfix(bBAT->batCacheid);

	return ret;

}

str
wkbDistance_geom_bat(bat *outBAT_id, wkb **geomWKB, bat *inBAT_id)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	BATiter inBAT_iter;
	BUN p = 0, q = 0;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.Distance", "Problem retrieving BAT");
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("dbl"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.Distance", "Error creating new BAT");
	}

	//iterator over the BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {
		str err = NULL;
		double distanceVal = 0;

		wkb *inWKB = (wkb *) BUNtail(inBAT_iter, p);

		if ((err = wkbDistance(&distanceVal, geomWKB, &inWKB)) != MAL_SUCCEED) {	//check
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		BUNappend(outBAT, &distanceVal, TRUE);	//add the result to the outBAT
	}

	BBPunfix(inBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

str
wkbDistance_bat_geom(bat *outBAT_id, bat *inBAT_id, wkb **geomWKB)
{
	return wkbDistance_geom_bat(outBAT_id, geomWKB, inBAT_id);
}

static str
wkbDump_bat(bat *parentBAT_id, bat *idBAT_id, bat *geomBAT_id, bat *inGeomBAT_id, bat *inParentBAT_id)
{
	BAT *idBAT = NULL, *geomBAT = NULL, *parentBAT = NULL, *inParentBAT = NULL, *inGeomBAT = NULL;
    BATiter inGeomBAT_iter, inParentBAT_iter;
	BUN p = 0, q = 0;
	int geometriesCnt, i;
	str err;

    //Open input BATs
	if ((inGeomBAT = BATdescriptor(*inGeomBAT_id)) == NULL) {
		throw(MAL, "batgeom.Dump", "Problem retrieving BAT");
	}
    if (inParentBAT_id) {
        if ((inParentBAT = BATdescriptor(*inParentBAT_id)) == NULL) {
            BBPunfix(inGeomBAT->batCacheid);
            throw(MAL, "batgeom.Dump", "Problem retrieving BAT");
        }
    }

    //create new empty BAT for the output
    if ((idBAT = COLnew(0, TYPE_str, 0, TRANSIENT)) == NULL) {
        *idBAT_id = bat_nil;
	    BBPunfix(inGeomBAT->batCacheid);
        if (inParentBAT_id)
    	    BBPunfix(inParentBAT->batCacheid);
        throw(MAL, "geom.Dump", "Error creating new BAT");
    }

    if ((geomBAT = COLnew(0, ATOMindex("wkb"), 0, TRANSIENT)) == NULL) {
	    BBPunfix(inGeomBAT->batCacheid);
        if (inParentBAT_id)
	        BBPunfix(inParentBAT->batCacheid);
        BBPunfix(idBAT->batCacheid);
        *geomBAT_id = bat_nil;
        throw(MAL, "geom.Dump", "Error creating new BAT");
    }

    if (inParentBAT_id) {
        if ((parentBAT = COLnew(0, ATOMindex("int"), 0, TRANSIENT)) == NULL) {
	        BBPunfix(inGeomBAT->batCacheid);
    	    BBPunfix(inParentBAT->batCacheid);
            BBPunfix(idBAT->batCacheid);
            BBPunfix(geomBAT->batCacheid);
            *parentBAT_id = bat_nil;
            throw(MAL, "geom.Dump", "Error creating new BAT");
        }
    }

	//iterator over the BATs
	inGeomBAT_iter = bat_iterator(inGeomBAT);
    if (inParentBAT_id)
    	inParentBAT_iter = bat_iterator(inParentBAT);

	BATloop(inGeomBAT, p, q) {	//iterate over all valid elements
		str err = NULL;
		int parent = 0, geometriesNum;
	    GEOSGeom geosGeometry;

		wkb *geomWKB = (wkb *) BUNtail(inGeomBAT_iter, p);
        if (inParentBAT_id)
		    parent = *(int *) BUNtail(inParentBAT_iter, p);

	    geosGeometry = wkb2geos(geomWKB);

    	//count the number of geometries
    	geometriesNum = GEOSGetNumGeometries(geosGeometry);

        /*Get the tail and add parentID geometriesNum types*/
        if (inParentBAT_id) {
            for (i = 0; i < geometriesNum; i++) {
                if (BUNappend(parentBAT, &parent, TRUE) != GDK_SUCCEED) {
                    err = createException(MAL, "geom.Dump", "BUNappend failed");
	                BBPunfix(inGeomBAT->batCacheid);
                    if (inParentBAT_id)
            	        BBPunfix(inParentBAT->batCacheid);
                    BBPunfix(idBAT->batCacheid);
                    BBPunfix(geomBAT->batCacheid);
                    if (inParentBAT_id)
                        BBPunfix(parentBAT->batCacheid);
                    return err;
                }
            }
        }

        geometriesCnt += geometriesNum;

        if ((err = dumpGeometriesGeometry(idBAT, geomBAT, geosGeometry, "")) != MAL_SUCCEED) {
            BBPunfix(inGeomBAT->batCacheid);
            if (inParentBAT_id)
                BBPunfix(inParentBAT->batCacheid);
            BBPunfix(idBAT->batCacheid);
            BBPunfix(geomBAT->batCacheid);
            if (inParentBAT_id)
                BBPunfix(parentBAT->batCacheid);
    		return err;
    	}
    }

    /*Release input BATs*/
    BBPunfix(inGeomBAT->batCacheid);
    if (inParentBAT_id)
        BBPunfix(inParentBAT->batCacheid);

    /*Set counts*/
	BATsetcount(idBAT, geometriesCnt);
    BATderiveProps(idBAT, FALSE);
	BATsetcount(geomBAT, geometriesCnt);
    BATderiveProps(geomBAT, FALSE);
    if (inParentBAT_id) {
        BATsetcount(parentBAT, geometriesCnt);
        BATderiveProps(parentBAT, FALSE);
    }

    /*Keep refs for output BATs*/
	BBPkeepref(*idBAT_id = idBAT->batCacheid);
	BBPkeepref(*geomBAT_id = geomBAT->batCacheid);
    if (inParentBAT_id)
	    BBPkeepref(*parentBAT_id = parentBAT->batCacheid);

	return MAL_SUCCEED;
}



/**
 * It filters the geometry in the second BAT with respect to the MBR of the geometry in the first BAT.
 **/
/*
str
wkbFilter_bat(bat *aBATfiltered_id, bat *bBATfiltered_id, bat *aBAT_id, bat *bBAT_id)
{
	BAT *aBATfiltered = NULL, *bBATfiltered = NULL, *aBAT = NULL, *bBAT = NULL;
	wkb *aWKB = NULL, *bWKB = NULL;
	bit outBIT;
	BATiter aBAT_iter, bBAT_iter;
	BUN i = 0;
	int remainingElements = 0;

	//get the descriptor of the BAT
	if ((aBAT = BATdescriptor(*aBAT_id)) == NULL) {
		throw(MAL, "batgeom.MBRfilter", RUNTIME_OBJECT_MISSING);
	}
	if ((bBAT = BATdescriptor(*bBAT_id)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		throw(MAL, "batgeom.MBRfilter", RUNTIME_OBJECT_MISSING);
	}

	if (aBAT->hseqbase != bBAT->hseqbase ||	//the idxs of the headers of the BATs are not the same
	    BATcount(aBAT) != BATcount(bBAT)) {	//the number of valid elements in the BATs are not the same
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.MBRfilter", "The arguments must have dense and aligned heads");
	}
	//create two new BATs for the output
	if ((aBATfiltered = COLnew(aBAT->hseqbase, ATOMindex("wkb"), BATcount(aBAT), TRANSIENT)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.MBRfilter", MAL_MALLOC_FAIL);
	}
	if ((bBATfiltered = COLnew(bBAT->hseqbase, ATOMindex("wkb"), BATcount(bBAT), TRANSIENT)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.MBRfilter", MAL_MALLOC_FAIL);
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = BUNfirst(aBAT); i < BATcount(aBAT); i++) {
		str err = NULL;
		aWKB = (wkb *) BUNtail(aBAT_iter, i + BUNfirst(aBAT));
		bWKB = (wkb *) BUNtail(bBAT_iter, i + BUNfirst(bBAT));

		//check the containment of the MBRs
		if ((err = mbrOverlaps_wkb(&outBIT, &aWKB, &bWKB)) != MAL_SUCCEED) {
			BBPunfix(aBAT->batCacheid);
			BBPunfix(bBAT->batCacheid);
			BBPunfix(aBATfiltered->batCacheid);
			BBPunfix(bBATfiltered->batCacheid);
			return err;
		}
		if (outBIT) {
			BUNappend(aBATfiltered, aWKB, TRUE);	//add the result to the aBAT
			BUNappend(bBATfiltered, bWKB, TRUE);	//add the result to the bBAT
			remainingElements++;
		}
	}

	//set some properties of the new BATs
	BATsetcount(aBATfiltered, remainingElements);
	BATsettrivprop(aBATfiltered);
	BATderiveProps(aBATfiltered, FALSE);

	BATsetcount(bBATfiltered, remainingElements);
	BATsettrivprop(bBATfiltered);
	BATderiveProps(bBATfiltered, FALSE);

	BBPunfix(aBAT->batCacheid);
	BBPunfix(bBAT->batCacheid);
	BBPkeepref(*aBATfiltered_id = aBATfiltered->batCacheid);
	BBPkeepref(*bBATfiltered_id = bBATfiltered->batCacheid);

	return MAL_SUCCEED;


}
*/

/**
 * It filters the geometry in the second BAT with respect to the MBR of the geometry in the first BAT.
 **/
str
wkbFilter_geom_bat(bat *BATfiltered_id, wkb **geomWKB, bat *BAToriginal_id)
{
	BAT *BATfiltered = NULL, *BAToriginal = NULL;
	wkb *WKBoriginal = NULL;
	BATiter BAToriginal_iter;
	BUN i = 0;
	mbr *geomMBR;
	int remainingElements = 0;
	str err = NULL;

	//get the descriptor of the BAT
	if ((BAToriginal = BATdescriptor(*BAToriginal_id)) == NULL) {
		throw(MAL, "batgeom.MBRfilter", RUNTIME_OBJECT_MISSING);
	}

	//create the new BAT
	if ((BATfiltered = COLnew(BAToriginal->hseqbase, ATOMindex("wkb"), BATcount(BAToriginal), TRANSIENT)) == NULL) {
		BBPunfix(BAToriginal->batCacheid);
		throw(MAL, "batgeom.MBRfilter", MAL_MALLOC_FAIL);
	}

	//iterator over the BAT
	BAToriginal_iter = bat_iterator(BAToriginal);

	//create the MBR of the geom
	if ((err = wkbMBR(&geomMBR, geomWKB)) != MAL_SUCCEED) {
		BBPunfix(BAToriginal->batCacheid);
		BBPunfix(BATfiltered->batCacheid);
		return err;
	}

	for (i = BUNfirst(BAToriginal); i < BATcount(BAToriginal); i++) {
		str err = NULL;
		mbr *MBRoriginal;
		bit outBIT = 0;

		WKBoriginal = (wkb *) BUNtail(BAToriginal_iter, i + BUNfirst(BAToriginal));

		//create the MBR for each geometry in the BAT
		if ((err = wkbMBR(&MBRoriginal, &WKBoriginal)) != MAL_SUCCEED) {
			BBPunfix(BAToriginal->batCacheid);
			BBPunfix(BATfiltered->batCacheid);
			GDKfree(geomMBR);
			return err;
		}
		//check the containment of the MBRs
		if ((err = mbrOverlaps(&outBIT, &geomMBR, &MBRoriginal)) != MAL_SUCCEED) {
			BBPunfix(BAToriginal->batCacheid);
			BBPunfix(BATfiltered->batCacheid);
			GDKfree(geomMBR);
			GDKfree(MBRoriginal);
			return err;
		}

		if (outBIT) {
			BUNappend(BATfiltered, WKBoriginal, TRUE);	//add the result to the bBAT
			remainingElements++;
		}

		GDKfree(MBRoriginal);
	}

	//set some properties of the new BATs
	BATsetcount(BATfiltered, remainingElements);
	BATsettrivprop(BATfiltered);
	BATderiveProps(BATfiltered, FALSE);

	BBPunfix(BAToriginal->batCacheid);
	BBPkeepref(*BATfiltered_id = BATfiltered->batCacheid);

	return MAL_SUCCEED;

}

str
wkbFilter_bat_geom(bat *BATfiltered_id, bat *BAToriginal_id, wkb **geomWKB)
{
	return wkbFilter_geom_bat(BATfiltered_id, geomWKB, BAToriginal_id);
}

/* MBR */

/* Creates the BAT with mbrs from the BAT with geometries. */
str
wkbMBR_bat(bat *outBAT_id, bat *inBAT_id)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	mbr *outMBR = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.mbr", RUNTIME_OBJECT_MISSING);
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("mbr"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.mbr", MAL_MALLOC_FAIL);
	}

	//iterator over the BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;

		inWKB = (wkb *) BUNtail(inBAT_iter, p);
		if ((err = wkbMBR(&outMBR, &inWKB)) != MAL_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		BUNappend(outBAT, outMBR, TRUE);	//add the point to the new BAT
		GDKfree(outMBR);
		outMBR = NULL;
	}

	//set some properties of the new BAT
	BATsetcount(outBAT, BATcount(inBAT));
	BATsettrivprop(outBAT);
	BATderiveProps(outBAT, FALSE);
	BBPunfix(inBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	return MAL_SUCCEED;
}


str
wkbCoordinateFromWKB_bat(bat *outBAT_id, bat *inBAT_id, int *coordinateIdx)
{
	str err = NULL;
	int inBAT_mbr_id = 0;	//the id of the bat with the mbrs

	if ((err = wkbMBR_bat(&inBAT_mbr_id, inBAT_id)) != MAL_SUCCEED) {
		return err;
	}
	//call the bulk version of wkbCoordinateFromMBR
	return wkbCoordinateFromMBR_bat(outBAT_id, &inBAT_mbr_id, coordinateIdx);
}

str
wkbMakeLine_bat(bat *outBAT_id, bat *aBAT_id, bat *bBAT_id)
{
	BAT *outBAT = NULL, *aBAT = NULL, *bBAT = NULL;
	BATiter aBAT_iter, bBAT_iter;
	BUN i;

	//get the BATs
	if ((aBAT = BATdescriptor(*aBAT_id)) == NULL || (bBAT = BATdescriptor(*bBAT_id)) == NULL) {
		if (aBAT)
			BBPunfix(aBAT->batCacheid);
		if (bBAT)
			BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.MakeLine", "Problem retrieving BATs");
	}
	//check if the BATs are aligned
	if (aBAT->hseqbase != bBAT->hseqbase || BATcount(aBAT) != BATcount(bBAT)) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.MakeLine", "BATs must be aligned");
	}
	//create a new BAT for the output
	if ((outBAT = COLnew(aBAT->hseqbase, ATOMindex("wkb"), BATcount(aBAT), TRANSIENT)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.MakeLine", "Error creating new BAT");
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = BUNfirst(aBAT); i < BATcount(aBAT); i++) {
		str err = NULL;
		wkb *aWKB = NULL, *bWKB = NULL, *outWKB = NULL;

		aWKB = (wkb *) BUNtail(aBAT_iter, i + BUNfirst(aBAT));
		bWKB = (wkb *) BUNtail(bBAT_iter, i + BUNfirst(bBAT));

		if ((err = wkbMakeLine(&outWKB, &aWKB, &bWKB)) != MAL_SUCCEED) {	//check
			BBPunfix(outBAT->batCacheid);
			BBPunfix(aBAT->batCacheid);
			BBPunfix(bBAT->batCacheid);
			return err;
		}
		BUNappend(outBAT, outWKB, TRUE);	//add the result to the outBAT
		GDKfree(outWKB);
		outWKB = NULL;
	}

	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	BBPunfix(aBAT->batCacheid);
	BBPunfix(bBAT->batCacheid);

	return MAL_SUCCEED;
}

str
wkbUnion_bat(bat *outBAT_id, bat *aBAT_id, bat *bBAT_id)
{
	BAT *outBAT = NULL, *aBAT = NULL, *bBAT = NULL;
	BATiter aBAT_iter, bBAT_iter;
	BUN i;

	//get the BATs
	if ((aBAT = BATdescriptor(*aBAT_id)) == NULL || (bBAT = BATdescriptor(*bBAT_id)) == NULL) {
		if (aBAT)
			BBPunfix(aBAT->batCacheid);
		if (bBAT)
			BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.Union", "Problem retrieving BATs");
	}
	//check if the BATs are aligned
	if (aBAT->hseqbase != bBAT->hseqbase || BATcount(aBAT) != BATcount(bBAT)) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.Union", "BATs must be aligned");
	}
	//create a new BAT for the output
	if ((outBAT = COLnew(aBAT->hseqbase, ATOMindex("wkb"), BATcount(aBAT), TRANSIENT)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.Union", "Error creating new BAT");
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = BUNfirst(aBAT); i < BATcount(aBAT); i++) {
		str err = NULL;
		wkb *aWKB = NULL, *bWKB = NULL, *outWKB = NULL;

		aWKB = (wkb *) BUNtail(aBAT_iter, i + BUNfirst(aBAT));
		bWKB = (wkb *) BUNtail(bBAT_iter, i + BUNfirst(bBAT));

		if ((err = wkbUnion(&outWKB, &aWKB, &bWKB)) != MAL_SUCCEED) {	//check
			BBPunfix(outBAT->batCacheid);
			BBPunfix(aBAT->batCacheid);
			BBPunfix(bBAT->batCacheid);
			return err;
		}
		BUNappend(outBAT, outWKB, TRUE);	//add the result to the outBAT
		GDKfree(outWKB);
		outWKB = NULL;
	}

	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	BBPunfix(aBAT->batCacheid);
	BBPunfix(bBAT->batCacheid);

	return MAL_SUCCEED;
}

/* sets the srid of the geometry - BULK version*/
str
wkbAsX3D_bat(bat *outBAT_id, bat *inBAT_id, int *maxDecDigits, int *options)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.SetSRID", "Problem retrieving BAT");
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("str"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.SetSRID", "Error creating new BAT");
	}
	//set the first idx of the output BAT equal to that of the input BAT

	//iterator over the BATs
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {
		str err = NULL;
		str outWKB = NULL;

		wkb *inWKB = (wkb *) BUNtail(inBAT_iter, p);

		if ((err = wkbAsX3D(&outWKB, &inWKB, maxDecDigits, options)) != MAL_SUCCEED) {	//set SRID
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
        if (outWKB) {
            BUNappend(outBAT, outWKB, TRUE);	//add the point to the new BAT
            GDKfree(outWKB);
            outWKB = NULL;
        }
	}

	BBPunfix(inBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

str
wkbIntersection_bat(bat *outBAT_id, bat *aBAT_id, bat *bBAT_id)
{
	BAT *outBAT = NULL, *aBAT = NULL, *bBAT = NULL;
	BATiter aBAT_iter, bBAT_iter;
    uint32_t i = 0;

	//get the descriptors of the input BATs
	if ((aBAT = BATdescriptor(*aBAT_id)) == NULL) {
		throw(MAL, "batgeom.Intersection", "Problem retrieving BAT");
	}

	if ((bBAT = BATdescriptor(*bBAT_id)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		throw(MAL, "batgeom.Intersection", "Problem retrieving BAT");
	}

    if (BATcount(aBAT) != BATcount(bBAT)) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.Intersection", "The BATs should be aligned");
    }
    
	//create a new BAT for the output
	if ((outBAT = COLnew(aBAT->hseqbase, ATOMindex("wkb"), BATcount(aBAT), TRANSIENT)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.Intersection", "Error creating new BAT");
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = BUNfirst(aBAT); i < BATcount(aBAT); i++) {
		str err = NULL;
		wkb *aWKB = NULL, *bWKB = NULL, *outWKB = NULL;

		aWKB = (wkb *) BUNtail(aBAT_iter, i + BUNfirst(aBAT));
		bWKB = (wkb *) BUNtail(bBAT_iter, i + BUNfirst(bBAT));

		if ((err = wkbIntersection(&outWKB, &aWKB, &bWKB)) != MAL_SUCCEED) {	//set SRID
			BBPunfix(aBAT->batCacheid);
			BBPunfix(bBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}

        BUNappend(outBAT, outWKB, TRUE);	//add the point to the new BAT
        GDKfree(outWKB);
        outWKB = NULL;
	}

	BBPunfix(aBAT->batCacheid);
	BBPunfix(bBAT->batCacheid);

	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

/*Bulk version with candidates lists*/
str
wkbIntersection_bat_s(bat *outBAT_id, bat *aBAT_id, bat *bBAT_id, bat *saBAT_id, bat *sbBAT_id)
{
	BAT *outBAT = NULL, *aBAT = NULL, *bBAT = NULL, *saBAT = NULL, *sbBAT = NULL;
	BATiter aBAT_iter, bBAT_iter, saBAT_iter, sbBAT_iter;
    BUN i = 0;

	//get the descriptors of the input BATs
	if ((aBAT = BATdescriptor(*aBAT_id)) == NULL) {
		throw(MAL, "batgeom.Intersection", "Problem retrieving BAT");
	}

	if ((bBAT = BATdescriptor(*bBAT_id)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		throw(MAL, "batgeom.Intersection", "Problem retrieving BAT");
	}

    if (BATcount(aBAT) != BATcount(bBAT)) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.Intersection", "The BATs should be aligned");
    }
   
    if (BATcount(aBAT) != BATcount(bBAT)) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.Intersection", "The BATs should be aligned");
    }

    //Get descriptors of the candidate list BATs
	if ((saBAT = BATdescriptor(*saBAT_id)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.Intersection", "Problem retrieving BAT");
	}

	if ((sbBAT = BATdescriptor(*sbBAT_id)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		BBPunfix(saBAT->batCacheid);
		throw(MAL, "batgeom.Intersection", "Problem retrieving BAT");
	}
    
	//create a new BAT for the output
	if ((outBAT = COLnew(aBAT->hseqbase, ATOMindex("wkb"), BATcount(aBAT), TRANSIENT)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		BBPunfix(saBAT->batCacheid);
		BBPunfix(sbBAT->batCacheid);
		throw(MAL, "batgeom.Intersection", "Error creating new BAT");
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);
	saBAT_iter = bat_iterator(saBAT);
	sbBAT_iter = bat_iterator(sbBAT);

	for (i = BUNfirst(saBAT); i < BATcount(saBAT); i++) {
		str err = NULL;
        oid aOID = 0, bOID = 0;
		wkb *aWKB = NULL, *bWKB = NULL, *outWKB = NULL;

		aOID = *(oid *) BUNtail(saBAT_iter, i + BUNfirst(saBAT));
		bOID = *(oid *) BUNtail(sbBAT_iter, i + BUNfirst(sbBAT));

		aWKB = (wkb *) BUNtail(aBAT_iter, aOID);
		bWKB = (wkb *) BUNtail(bBAT_iter, bOID);

		if ((err = wkbIntersection(&outWKB, &aWKB, &bWKB)) != MAL_SUCCEED) {	//set SRID
			BBPunfix(aBAT->batCacheid);
			BBPunfix(bBAT->batCacheid);
			BBPunfix(saBAT->batCacheid);
			BBPunfix(sbBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}

        BUNappend(outBAT, outWKB, TRUE);	//add the point to the new BAT
        GDKfree(outWKB);
        outWKB = NULL;
	}

	BBPunfix(aBAT->batCacheid);
	BBPunfix(bBAT->batCacheid);
	BBPunfix(saBAT->batCacheid);
	BBPunfix(sbBAT->batCacheid);

	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

