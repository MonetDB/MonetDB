 /* The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * Foteini Alvanaki
 */

#include "geom.h"
#include "time.h"
#include "unistd.h"


typedef struct pbsm_ptr {
	BUN offset;
	unsigned long count;
} pbsm_ptr;

typedef struct {
	double xmin;
	double ymin;
	double xmax;
	double ymax;
	unsigned int oidsNum;
} pbsm_info;

static pbsm_ptr *pbsm_idx = NULL;
static oid *oids = NULL;
static pbsm_info *limits = NULL;

//hard coded filename
static char* filename = "../pbsmIndex_20m";
static char* idxEnding = ".idx";
static char* dataEnding = ".data";
static char* limitsEnding =".info";

//static char* idxFilename;
//static char* dataFilename;
//static char* limitsFilename;

//it gets two BATs with x,y coordinates and returns a new BAT with the points
static BAT* BATMakePoint2D(BAT* xBAT, BAT* yBAT) {
	BAT *outBAT = NULL;
	BATiter xBAT_iter, yBAT_iter; 
	BUN i;
	oid head=0;
//	clock_t startTime, endTime;
//	float seconds = 0.0;

	//check if the BATs have dense heads and are aligned
	if (!BAThdense(xBAT) || !BAThdense(yBAT)) {
		GDKerror("BATMakePoint2D: BATs must have dense heads");
		return NULL;
	}
	if(xBAT->hseqbase != yBAT->hseqbase || BATcount(xBAT) != BATcount(yBAT)) {
		GDKerror("BATMakePoint2D: BATs must be aligned");
		return NULL;
	}

	//iterator over the BATs	
	xBAT_iter = bat_iterator(xBAT);
	yBAT_iter = bat_iterator(yBAT);
	
	//create a new BAT
	if ((outBAT = BATnew(TYPE_void, ATOMindex("wkb"), BATcount(xBAT), TRANSIENT)) == NULL) {
		GDKerror("BATMakePoint2D: Could not create new BAT for the output");
		return NULL;
	}

	//set the first idx of the new BAT equal to that of the x BAT (which is equal to the y BAT)
	BATseqbase(outBAT, xBAT->hseqbase);
		
	for (i = BUNfirst(xBAT); i < BATcount(xBAT); i++) { 
		str err = NULL;
		wkb* point = NULL;
		double *x = (double*) BUNtail(xBAT_iter, i + BUNfirst(xBAT));
		double *y = (double*) BUNtail(yBAT_iter, i + BUNfirst(yBAT));
	
		if ((err = geomMakePoint2D(&point, x, y)) != MAL_SUCCEED) {
			BBPreleaseref(outBAT->batCacheid);
			GDKerror("BATMakePoint2D: %s", err);
			GDKfree(err);
			return NULL;
		}
//		startTime = clock();
		BUNfastins(outBAT, &head, point);
		head++;
		//BUNappend(outBAT,point,TRUE); //add the result to the outBAT
//		endTime = clock();
//		seconds += (float)(endTime - startTime); 
		GDKfree(point);
	}

//seconds /= CLOCKS_PER_SEC;
//fprintf(stderr, "BATMakePoint2D: %f secs\n", seconds);
	return outBAT;

}

static BAT* BATSetSRID(BAT* pointsBAT, int srid) {
	BAT *outBAT = NULL;
	BATiter pointsBAT_iter;	
	BUN p=0, q=0;
	wkb *pointWKB = NULL;
	
	oid head = 0;
//	clock_t startTime, endTime;
//	float seconds = 0.0;

	//check if the BAT has dense heads and are aligned
	if (!BAThdense(pointsBAT)) {
		GDKerror("BATSetSRID: BAT must have dense heads");
		return NULL;
	}

	//create a new BAT
	if ((outBAT = BATnew(TYPE_void, ATOMindex("wkb"), BATcount(pointsBAT), TRANSIENT)) == NULL) {
		GDKerror("BATSetSRID: Could not create new BAT for the output");
		return NULL;
	}

	//set the first idx of the new BAT equal to that of the x BAT (which is equal to the y BAT)
	BATseqbase(outBAT, pointsBAT->hseqbase);

	//iterator over the BATs	
	pointsBAT_iter = bat_iterator(pointsBAT);
	 
	BATloop(pointsBAT, p, q) { //iterate over all valid elements
		str err = NULL;
		wkb *outWKB = NULL;

		pointWKB = (wkb*) BUNtail(pointsBAT_iter, p);
		if ((err = wkbSetSRID(&outWKB, &pointWKB, &srid)) != MAL_SUCCEED) { //set SRID
			BBPreleaseref(outBAT->batCacheid);
			GDKerror("BATSetSRID: %s", err);
			GDKfree(err);
			return NULL;
		}
//		startTime = clock();
		BUNfastins(outBAT, &head, outWKB);
		head++;
		//BUNappend(outBAT,outWKB,TRUE); //add the result to the outBAT
//		endTime = clock();
//		seconds += (float)(endTime - startTime);
		GDKfree(outWKB);
		outWKB = NULL;
	}
//seconds /= CLOCKS_PER_SEC;
//fprintf(stderr, "BATSetSRID: %f secs\n", seconds);

	return outBAT;
}

static BAT* BATContains(wkb** geomWKB, BAT* geometriesBAT) {
	BAT *outBAT = NULL;
	BATiter geometriesBAT_iter;	
	BUN p=0, q=0;
	wkb *geometryWKB = NULL;

	oid head = 0;
//	clock_t startTime, endTime;
//	float seconds = 0.0;

	//check if the BAT has dense heads and are aligned
	if (!BAThdense(geometriesBAT)) {
		GDKerror("BATContains: BAT must have dense heads");
		return NULL;
	}

	//create a new BAT
	if ((outBAT = BATnew(TYPE_void, ATOMindex("bit"), BATcount(geometriesBAT), TRANSIENT)) == NULL) {
		GDKerror("BATContains: Could not create new BAT for the output");
		return NULL;
	}

	
	//iterator over the BATs	
	geometriesBAT_iter = bat_iterator(geometriesBAT);
	 
	BATloop(geometriesBAT, p, q) { //iterate over all valid elements
		str err = NULL;
		bit outBIT = 0;

		geometryWKB = (wkb*) BUNtail(geometriesBAT_iter, p);
		if ((err = wkbContains(&outBIT, geomWKB, &geometryWKB)) != MAL_SUCCEED) { //set SRID
			BBPreleaseref(outBAT->batCacheid);
			GDKerror("BATContains: %s", err);
			GDKfree(err);
			return NULL;
		}
//		startTime = clock();
		BUNfastins(outBAT, &head, &outBIT);
		head++;
		//BUNappend(outBAT,&outBIT,TRUE); //add the result to the outBAT
//		endTime = clock();
//		seconds += (float)(endTime - startTime);
	}
//set some properties
//set the first idx of the new BAT equal to that of the x BAT (which is equal to the y BAT)
BATseqbase(outBAT, geometriesBAT->hseqbase);
outBAT->tsorted = false;
outBAT->trevsorted = false;
//seconds /= CLOCKS_PER_SEC;
//fprintf(stderr, "BATContains: %f secs\n", seconds);

	return outBAT;

}

static str wkbPointsGeomContains_geom_bat(bat* outBAT_id, wkb** geomWKB, bat* xBAT_id, bat* yBAT_id, int* srid) {
	BAT *xBAT=NULL, *yBAT=NULL, *outBAT=NULL;
	BAT *pointsBAT = NULL, *pointsWithSRIDBAT=NULL;
	str ret=MAL_SUCCEED;

	//get the descriptors of the BATs
	if ((xBAT = BATdescriptor(*xBAT_id)) == NULL) {
		throw(MAL, "batgeom.Contains", RUNTIME_OBJECT_MISSING);
	}
	if ((yBAT = BATdescriptor(*yBAT_id)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		throw(MAL, "batgeom.Contains", RUNTIME_OBJECT_MISSING);
	}

	//check if the BATs have dense heads and are aligned
	if (!BAThdense(xBAT) || !BAThdense(yBAT)) {
		ret = createException(MAL, "batgeom.Contains", "BATs must have dense heads");
		goto clean;
	}
	if(xBAT->hseqbase != yBAT->hseqbase || BATcount(xBAT) != BATcount(yBAT)) {
		ret=createException(MAL, "batgeom.Contains", "BATs must be aligned");
		goto clean;
	}

	//here the BAT version of some contain function that takes the BATs of the x y coordinates should be called
	//create the points BAT
	if((pointsBAT = BATMakePoint2D(xBAT, yBAT)) == NULL) {
		ret = createException(MAL, "batgeom.Contains", "Problem creating the points from the coordinates");
		goto clean;
	}

	if((pointsWithSRIDBAT = BATSetSRID(pointsBAT, *srid)) == NULL) {
		ret = createException(MAL, "batgeom.Contains", "Problem setting srid to the points");
		goto clean;
	}

	if((outBAT = BATContains(geomWKB, pointsWithSRIDBAT)) == NULL) {
		ret = createException(MAL, "batgeom.Contains", "Problem evalauting the contains");
		goto clean;
	}

	BBPkeepref(*outBAT_id = outBAT->batCacheid);

//	fprintf(stderr, "Contains1: IN %u - OUT %u\n", (unsigned int)BATcount(xBAT), (unsigned int)BATcount(outBAT));

	goto clean;

clean:
	if(xBAT)
		BBPreleaseref(xBAT->batCacheid);
	if(yBAT)
		BBPreleaseref(yBAT->batCacheid);
	if(pointsBAT)
		BBPreleaseref(pointsBAT->batCacheid);
	if(pointsWithSRIDBAT)
		BBPreleaseref(pointsWithSRIDBAT->batCacheid);
	return ret;
}

//Aternative implementation of contains using the winding number method
static inline int isLeft( double P0x, double P0y, double P1x, double P1y, double P2x, double P2y) {
	//borders are not included
    return ( ((P1x - P0x) * (P2y - P0y) - (P2x -  P0x) * (P1y - P0y)) > 0.0 ); 
}

#define isRight(x0, y0, x1, y1, x2, y2) isLeft(x0, y0, x1, y1, x2, y2)-1

static str pnpoly_(int *out, const GEOSGeometry *geosGeometry, int *point_x, int *point_y) {
    BAT *bo = NULL, *bpx = NULL, *bpy = NULL;
    dbl *px = NULL, *py = NULL;
    BUN i = 0;
    unsigned int j = 0;
//    struct timeval stop, start;
//    unsigned long long t;
    bte *cs = NULL;

	const GEOSCoordSequence *coordSeq;
	unsigned int geometryPointsNum=0 ;
	double *xPoints, *yPoints; //arrays with the points of the ring


	/* get the coordinates of the points comprising the geometry */
	if(!(coordSeq = GEOSGeom_getCoordSeq(geosGeometry)))
		return createException(MAL, "batgeom.Contains", "GEOSGeom_getCoordSeq failed");
	
	/* get the number of points in the geometry */
	GEOSCoordSeq_getSize(coordSeq, &geometryPointsNum);

	//allocate space for the coordinates
	xPoints = (double*)GDKmalloc(sizeof(double*)*geometryPointsNum);
	yPoints = (double*)GDKmalloc(sizeof(double*)*geometryPointsNum);

	/*Get the BATs*/
	if ((bpx = BATdescriptor(*point_x)) == NULL) {
        	throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
   	}

    	if ((bpy = BATdescriptor(*point_y)) == NULL) {
        	BBPreleaseref(bpx->batCacheid);
        	throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
    	}

   	/*Check BATs alignment*/
    	if ( bpx->htype != TYPE_void || bpy->htype != TYPE_void ||bpx->hseqbase != bpy->hseqbase || BATcount(bpx) != BATcount(bpy)) {
      		BBPreleaseref(bpx->batCacheid);
        	BBPreleaseref(bpy->batCacheid);
        	throw(MAL, "geom.point", "both point bats must have dense and aligned heads");
    	}
;
    	/*Create output BAT*/
    	if ((bo = BATnew(TYPE_void, ATOMindex("bte"), BATcount(bpx), TRANSIENT)) == NULL) {
        	BBPreleaseref(bpx->batCacheid);
        	BBPreleaseref(bpy->batCacheid);
        	throw(MAL, "geom.point", MAL_MALLOC_FAIL);
    	}
    	BATseqbase(bo, bpx->hseqbase);

    	/*Iterate over the Point BATs and determine if they are in Polygon represented by vertex BATs*/
    	px = (dbl *) Tloc(bpx, BUNfirst(bpx));
    	py = (dbl *) Tloc(bpy, BUNfirst(bpx));

//    	gettimeofday(&start, NULL);
    	cs = (bte*) Tloc(bo,BUNfirst(bo));
    	for (i = 0; i < BATcount(bpx); i++) {
        	int wn = 0;

		if(i==0) {
			//read the first point from the external ring
			if(GEOSCoordSeq_getX(coordSeq, j, &xPoints[0]) == -1) 
				return createException(MAL, "batgeom.Contains", "GEOSCoordSeq_getX failed");
			if(GEOSCoordSeq_getY(coordSeq, j, &yPoints[0]) == -1)
				return createException(MAL, "batgeom.Contains", "GEOSCoordSeq_getY failed");
		}
        	/*Check if the point is inside the polygon*/
		for (j = 1; j < geometryPointsNum; j++) { //check each point in the exterior ring)
			//double xCurrent=0.0, yCurrent=0.0, xNext=0.0, yNext=0.0;
			if( i == 0) {
				//it is the first iteration, I need to read the points
				if(GEOSCoordSeq_getX(coordSeq, j, &xPoints[j]) == -1) 
					return createException(MAL, "batgeom.Contains", "GEOSCoordSeq_getX failed");
				if(GEOSCoordSeq_getY(coordSeq, j, &yPoints[j]) == -1)
					return createException(MAL, "batgeom.Contains", "GEOSCoordSeq_getY failed");

			}
	        	//the edge goes from small y to big y (upward direction) and the point is somewhere in there
		   	if (yPoints[j-1] <= py[i] && yPoints[j] >= py[i]) {
				wn+=isLeft(xPoints[j-1], yPoints[j-1], xPoints[j], yPoints[j], px[i], py[i]); 
            		}
			//the edge goes from big y to small y (downward direction) and the point is somewhere in there
        	    	else if (yPoints[j-1] >= py[i] && yPoints[j] <= py[i]) {
				wn+=isRight(xPoints[j-1], yPoints[j-1], xPoints[j], yPoints[j], px[i], py[i]); 
            		}	
        	}
        	*cs++ = wn & 1;
    	}
//    gettimeofday(&stop, NULL);
//    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
//    printf("took %llu ms\n", t);

//    gettimeofday(&start, NULL);
    BATsetcount(bo,BATcount(bpx));
    BATderiveProps(bo,FALSE);
//    gettimeofday(&stop, NULL);
//    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
//    printf("Append took %llu ms\n", t);

    BBPreleaseref(bpx->batCacheid);
    BBPreleaseref(bpy->batCacheid);
    BBPkeepref(*out = bo->batCacheid);

	GDKfree(xPoints);
	GDKfree(yPoints);
//fprintf(stderr, "Contains2: IN %u - OUT %u\n", (unsigned int)BATcount(bpx), (unsigned int)BATcount(bo));
fprintf(stderr, "Contains2: %u, %u\n", (unsigned int)BATcount(bo), (unsigned int)bo->hseqbase);


    return MAL_SUCCEED;
}

//static str pnpolyWithHoles_(int *out, int nvert, dbl *vx, dbl *vy, int nholes, dbl **hx, dbl **hy, int *hn, int *point_x, int *point_y) {
static str pnpolyWithHoles_(int *out, GEOSGeom geosGeometry, unsigned int interiorRingsNum, int *point_x, int *point_y) {
    BAT *bo = NULL, *bpx = NULL, *bpy;
    dbl *px = NULL, *py = NULL;
    BUN i = 0;
    unsigned int j = 0, h = 0;
    bte *cs = NULL;

	const GEOSGeometry *exteriorRingGeometry;
	const GEOSCoordSequence *exteriorRingCoordSeq;
	double **xPoints, **yPoints; //arrays with the points of the rings so that we do not read them every time
	unsigned int *pointsNum; //array with the number of points in each ring
	bte checked = 0; //used to know when the internal rings have been checked

    /*Get the BATs*/
    if ((bpx = BATdescriptor(*point_x)) == NULL) {
        throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
    }
    if ((bpy = BATdescriptor(*point_y)) == NULL) {
        BBPreleaseref(bpx->batCacheid);
        throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
    }

    /*Check BATs alignment*/
    if ( bpx->htype != TYPE_void ||
            bpy->htype != TYPE_void ||
            bpx->hseqbase != bpy->hseqbase ||
            BATcount(bpx) != BATcount(bpy)) {
        BBPreleaseref(bpx->batCacheid);
        BBPreleaseref(bpy->batCacheid);
        throw(MAL, "geom.point", "both point bats must have dense and aligned heads");
    }

    /*Create output BAT*/
    if ((bo = BATnew(TYPE_void, ATOMindex("bte"), BATcount(bpx), TRANSIENT)) == NULL) {
        BBPreleaseref(bpx->batCacheid);
        BBPreleaseref(bpy->batCacheid);
        throw(MAL, "geom.point", MAL_MALLOC_FAIL);
    }
    BATseqbase(bo, bpx->hseqbase);
	
	xPoints = (double**)GDKmalloc(sizeof(double**)*(interiorRingsNum+1));
	yPoints = (double**)GDKmalloc(sizeof(double**)*(interiorRingsNum+1));
	pointsNum =  (unsigned int*)GDKmalloc(sizeof(unsigned int*)*(interiorRingsNum+1));

	//get the exterior ring of the geometry	
	if(!(exteriorRingGeometry = GEOSGetExteriorRing(geosGeometry)))
		return createException(MAL, "batgeom.Contains", "GEOSGetExteriorRing failed");

	/* get the coordinates of the points comprising the exteriorRing */
	if(!(exteriorRingCoordSeq = GEOSGeom_getCoordSeq(exteriorRingGeometry)))
		return createException(MAL, "batgeom.Contains", "GEOSGeom_getCoordSeq failed");
	
	/* get the number of points in the exterior ring */
	GEOSCoordSeq_getSize(exteriorRingCoordSeq, &pointsNum[0]);

	//the exteriorRing is in position 0
	xPoints[0] = (double*)GDKmalloc(sizeof(double*)*pointsNum[0]);
	yPoints[0] = (double*)GDKmalloc(sizeof(double*)*pointsNum[0]);

    /*Iterate over the Point BATs and determine if they are in Polygon represented by vertex BATs*/
    px = (dbl *) Tloc(bpx, BUNfirst(bpx));
    py = (dbl *) Tloc(bpy, BUNfirst(bpx));
    cs = (bte*) Tloc(bo,BUNfirst(bo));
    for (i = 0; i < BATcount(bpx); i++) {
        int wn = 0;

	if(i==0 && pointsNum[0]>0) {
		//read the first point from the external ring
		if(GEOSCoordSeq_getX(exteriorRingCoordSeq, 0, &xPoints[0][0]) == -1) 
			return createException(MAL, "batgeom.Contains", "GEOSCoordSeq_getX failed");
		if(GEOSCoordSeq_getY(exteriorRingCoordSeq, 0, &yPoints[0][0]) == -1)
			return createException(MAL, "batgeom.Contains", "GEOSCoordSeq_getY failed");
	}
        /*Check if the point is inside the polygon*/
	for (j = 1; j < pointsNum[0]; j++) { //check each point in the exterior ring)
		//double xCurrent=0.0, yCurrent=0.0, xNext=0.0, yNext=0.0;
		if( i == 0) {
			//it is the first iteration, I need to read the points
			if(GEOSCoordSeq_getX(exteriorRingCoordSeq, j, &xPoints[0][j]) == -1) 
				return createException(MAL, "batgeom.Contains", "GEOSCoordSeq_getX failed");
			if(GEOSCoordSeq_getY(exteriorRingCoordSeq, j, &yPoints[0][j]) == -1)
				return createException(MAL, "batgeom.Contains", "GEOSCoordSeq_getY failed");

		}
        	//the edge goes from small y to big y (upward direction) and the point is somewhere in there
	   	if (yPoints[0][j-1] <= py[i] && yPoints[0][j] >= py[i]) {
			wn+=isLeft(xPoints[0][j-1], yPoints[0][j-1], xPoints[0][j], yPoints[0][j], px[i], py[i]); 
            	}
		//the edge goes from big y to small y (downward direction) and the point is somewhere in there
            	else if (yPoints[0][j-1] >= py[i] && yPoints[0][j] <= py[i]) {
			wn+=isRight(xPoints[0][j-1], yPoints[0][j-1], xPoints[0][j], yPoints[0][j], px[i], py[i]); 
            	}	
        }
	
	*cs++ = wn&1;

	//not inside the external ring. There is no need to continue checking the holes
	if(!wn) 
		continue;
	
	//inside the polygon, check the holes
        for (h = 0; h < interiorRingsNum; h++) {
		const GEOSCoordSequence *interiorRingCoordSeq = NULL;

		if(!checked) {
			const GEOSGeometry *interiorRingGeometry;

			//get the interior ring
			if(!(interiorRingGeometry = GEOSGetInteriorRingN(geosGeometry, h)))
				return createException(MAL, "batgeom.Contains", "GEOSGetInteriorRingN failed");
		
			// get the coordinates of the points comprising the interior ring
			if(!(interiorRingCoordSeq = GEOSGeom_getCoordSeq(interiorRingGeometry)))
				return createException(MAL, "batgeom.Contains", "GEOSGeom_getCoordSeq failed");
	
			// get the number of points in the interior ring
			GEOSCoordSeq_getSize(interiorRingCoordSeq, &pointsNum[h+1]);

			xPoints[h+1] = (double*)GDKmalloc(sizeof(double*)*pointsNum[h+1]);
			yPoints[h+1] = (double*)GDKmalloc(sizeof(double*)*pointsNum[h+1]);

			//read the first point from the ring
			if(GEOSCoordSeq_getX(interiorRingCoordSeq, 0, &xPoints[h+1][0]) == -1)
				return createException(MAL, "batgeom.Contains", "GEOSCoordSeq_getX failed");
			if(GEOSCoordSeq_getY(interiorRingCoordSeq, 0, &yPoints[h+1][0]) == -1)
				return createException(MAL, "batgeom.Contains", "GEOSCoordSeq_getY failed");
		}
          
		wn = 0;

            	for (j = 1; j < pointsNum[h+1]; j++) { //check each point in the interior ring
			if(!checked) {
				//it is the first iteration, I need to read the points
				if(GEOSCoordSeq_getX(interiorRingCoordSeq, j, &xPoints[h+1][j]) == -1) 
					return createException(MAL, "batgeom.Contains", "GEOSCoordSeq_getX failed");
				if(GEOSCoordSeq_getY(interiorRingCoordSeq, j, &yPoints[h+1][j]) == -1)
					return createException(MAL, "batgeom.Contains", "GEOSCoordSeq_getY failed");
			}
        		//the edge goes from small y to big y (upward direction) and the point is somewhere in there
	   		if (yPoints[h+1][j-1] <= py[i] && yPoints[h+1][j] >= py[i]) {
				wn+=isLeft(xPoints[h+1][j-1], yPoints[h+1][j-1], xPoints[h+1][j], yPoints[h+1][j], px[i], py[i]); 
            		}
			//the edge goes from big y to small y (downward direction) and the point is somewhere in there
        	    	else if (yPoints[h+1][j-1] >= py[i] && yPoints[h+1][j] <= py[i]) {
				wn+=isRight(xPoints[h+1][j-1], yPoints[h+1][j-1], xPoints[h+1][j], yPoints[h+1][j], px[i], py[i]); 
            		}
		}

            	//It is in one of the holes no reason to check the others
            	if (wn) {
			*(cs-1)=0; //reset to 0;
                	break;
		}
        }

	checked = 1;
    }

    BATsetcount(bo,BATcount(bpx));
    BATderiveProps(bo,FALSE);
    BBPreleaseref(bpx->batCacheid);
    BBPreleaseref(bpy->batCacheid);
    BBPkeepref(*out = bo->batCacheid);

	GDKfree(xPoints[0]);
	GDKfree(yPoints[0]);

	//if there are no points this is not allocated and thus does not need to be freed
	if(checked) {
		for(i = 1; i< interiorRingsNum+1; i++) {
			GDKfree(xPoints[i]);
			GDKfree(yPoints[i]);
		}
	}

	GDKfree(xPoints);
	GDKfree(yPoints);
	GDKfree(pointsNum);

    return MAL_SUCCEED;
}

#define POLY_NUM_VERT 120
#define POLY_NUM_HOLE 10

static str wkbPointsWindingContains_geom_bat(bat* out, wkb** geomWKB, bat* point_x, bat* point_y, int* srid) {
	int interiorRingsNum = 0;
	GEOSGeom geosGeometry;
	str msg = NULL;

	//check if geometry a and the points have the same srid
	if((*geomWKB)->srid != *srid) 
		return createException(MAL, "batgeom.Contains", "Geometry and points should have the same srid");

	//get the GEOS representation of the geometry
	if(!(geosGeometry = wkb2geos(*geomWKB)))
		return createException(MAL, "batgeom.Contains", "wkb2geos failed");
	//check if the geometry is a polygon
	if((GEOSGeomTypeId(geosGeometry)+1) != wkbPolygon)
		return createException(MAL, "batgeom.Contains", "Geometry should be a polygon");

	//get the number of interior rings of the polygon
	if((interiorRingsNum = GEOSGetNumInteriorRings(geosGeometry)) == -1) {
		return createException(MAL, "batgeom.Contains", "GEOSGetNumInteriorRings failed");
	}

	if(interiorRingsNum > 0) {
		msg = pnpolyWithHoles_(out, geosGeometry, interiorRingsNum, point_x, point_y);
	} else {
		//get the exterior ring
		const GEOSGeometry *exteriorRingGeometry;
		if(!(exteriorRingGeometry = GEOSGetExteriorRing(geosGeometry)))
			return createException(MAL, "batgeom.Contains", "GEOSGetExteriorRing failed");
		msg = pnpoly_(out, exteriorRingGeometry, point_x, point_y);
	}
	return msg;
}

static BAT* BATDistance(wkb** geomWKB, BAT* geometriesBAT) {
	BAT *outBAT = NULL;
	BATiter geometriesBAT_iter;	
	BUN p=0, q=0;

	//check if the BAT has dense heads and are aligned
	if (!BAThdense(geometriesBAT)) {
		GDKerror("BATDistance: BAT must have dense heads");
		return NULL;
	}

	//create a new BAT
	if ((outBAT = BATnew(TYPE_void, ATOMindex("dbl"), BATcount(geometriesBAT), TRANSIENT)) == NULL) {
		GDKerror("BATDistance: Could not create new BAT for the output");
		return NULL;
	}

	//set the first idx of the new BAT equal to that of the geometries BAT
	BATseqbase(outBAT, geometriesBAT->hseqbase);

	//iterator over the BATs	
	geometriesBAT_iter = bat_iterator(geometriesBAT);
	 
	BATloop(geometriesBAT, p, q) { //iterate over all valid elements
		str err = NULL;
		double val = 0.0;

		wkb *geometryWKB = (wkb*) BUNtail(geometriesBAT_iter, p);
		if ((err = wkbDistance(&val, geomWKB, &geometryWKB)) != MAL_SUCCEED) {
			BBPreleaseref(outBAT->batCacheid);
			GDKerror("BATDistance: %s", err);
			GDKfree(err);
			return NULL;
		}
		BUNappend(outBAT,&val,TRUE);
	}

	return outBAT;
}

static str wkbPointsGeomDistance_geom_bat(bat* outBAT_id, wkb** geomWKB, bat* xBAT_id, bat* yBAT_id, int* srid) {
	BAT *xBAT=NULL, *yBAT=NULL, *outBAT=NULL;
	BAT *pointsBAT = NULL, *pointsWithSRIDBAT=NULL;
	str ret=MAL_SUCCEED;

	//get the descriptors of the BATs
	if ((xBAT = BATdescriptor(*xBAT_id)) == NULL) {
		throw(MAL, "batgeom.Distance", RUNTIME_OBJECT_MISSING);
	}
	if ((yBAT = BATdescriptor(*yBAT_id)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		throw(MAL, "batgeom.Distance", RUNTIME_OBJECT_MISSING);
	}
	
	//check if the BATs have dense heads and are aligned
	if (!BAThdense(xBAT) || !BAThdense(yBAT)) {
		ret = createException(MAL, "batgeom.Distance", "BATs must have dense heads");
		goto clean;
	}
	if(xBAT->hseqbase != yBAT->hseqbase || BATcount(xBAT) != BATcount(yBAT)) {
		ret=createException(MAL, "batgeom.Distance", "BATs must be aligned");
		goto clean;
	}
	
	//here the BAT version of some contain function that takes the BATs of the x y coordinates should be called
	//create the points BAT
	if((pointsBAT = BATMakePoint2D(xBAT, yBAT)) == NULL) {
		ret = createException(MAL, "batgeom.Distance", "Problem creating the points from the coordinates");
		goto clean;
	}

	if((pointsWithSRIDBAT = BATSetSRID(pointsBAT, *srid)) == NULL) {
		ret = createException(MAL, "batgeom.Distance", "Problem setting srid to the points");
		goto clean;
	}

	if((outBAT = BATDistance(geomWKB, pointsWithSRIDBAT)) == NULL) {
		ret = createException(MAL, "batgeom.Distance", "Problem evalauting the contains");
		goto clean;
	}

	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	goto clean;

clean:
	if(xBAT)
		BBPreleaseref(xBAT->batCacheid);
	if(yBAT)
		BBPreleaseref(yBAT->batCacheid);
	if(pointsBAT)
		BBPreleaseref(pointsBAT->batCacheid);
	if(pointsWithSRIDBAT)
		BBPreleaseref(pointsWithSRIDBAT->batCacheid);
	return ret;
}


/* Alternative implementation of distance that computes the euclidean distance of two points
 * (other geometries are not yet supported)
 * Using the Euclidean distance is appropriate when the geometries are expressed in projected
 * reference system, but what happens with the rest? Maybe we should check nd report an error
 * when a geographic srid is used. PostGIS, however, does not perform any check on the srid
 * and always computes the Euclidean distance 
 * */

static BAT* point2point_distance(GEOSGeom geosGeometry, BAT *xBAT, BAT *yBAT) {
	BAT *outBAT = NULL;
	const GEOSCoordSequence *coordSeq;
	double xPointCoordinate = 0.0, yPointCoordinate = 0.0;
	double *xBATCoordinate = NULL, *yBATCoordinate = NULL, *distancesArray = NULL;
	BUN i=0;

	//create a new BAT
	if ((outBAT = BATnew(TYPE_void, ATOMindex("dbl"), BATcount(xBAT), TRANSIENT)) == NULL) {
		GDKerror("BATDistance: Could not create new BAT for the output");
		return NULL;
	}

	//set the first idx of the new BAT equal to that of the x BAT (which is equal to the y BAT)
	BATseqbase(outBAT, xBAT->hseqbase);


	//the geometry is a point. Get the x, y coordinates of it
	if(!(coordSeq = GEOSGeom_getCoordSeq(geosGeometry))) {
		GDKerror("batgeom.Distance: GEOSGeom_getCoordSeq failed");
		return NULL;
	}
	if(GEOSCoordSeq_getX(coordSeq, 0, &xPointCoordinate) == -1) {
		GDKerror("batgeom.Distance: GEOSCoordSeq_getX failed");
		return NULL;
	}	
	if(GEOSCoordSeq_getY(coordSeq, 0, &yPointCoordinate) == -1) {
		GDKerror("batgeom.Distance: GEOSCoordSeq_getY failed");
		return NULL;
	}

	//get the x and y coordinates from the BATs (fixed size)
	xBATCoordinate = (double*) Tloc(xBAT, BUNfirst(xBAT));
	yBATCoordinate = (double*) Tloc(yBAT, BUNfirst(yBAT));
	distancesArray = (double*) Tloc(outBAT,BUNfirst(outBAT));
	//iterate
	for (i = 0; i < BATcount(xBAT); i++) {
       		distancesArray[i] = sqrt(pow((xBATCoordinate[i]-xPointCoordinate), 2.0)+pow((yBATCoordinate[i]-yPointCoordinate), 2.0));
	}

	BATsetcount(outBAT,BATcount(xBAT));
	BATderiveProps(outBAT,FALSE);

	return outBAT;


}

static str wkbPointsCartesianDistance_geom_bat(bat* outBAT_id, wkb** geomWKB, bat* xBAT_id, bat* yBAT_id, int* srid) {
	BAT *xBAT=NULL, *yBAT=NULL, *outBAT=NULL;
	GEOSGeom geosGeometry;
	str ret=MAL_SUCCEED;

	//check if geometry a and the points have the same srid
	if((*geomWKB)->srid != *srid) 
		return createException(MAL, "batgeom.Distance", "Geometry and points should have the same srid");

	//get the descriptors of the BATs
	if ((xBAT = BATdescriptor(*xBAT_id)) == NULL) {
		throw(MAL, "batgeom.Distance", RUNTIME_OBJECT_MISSING);
	}
	if ((yBAT = BATdescriptor(*yBAT_id)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		throw(MAL, "batgeom.Distance", RUNTIME_OBJECT_MISSING);
	}
	
	//check if the BATs have dense heads and are aligned
	if (!BAThdense(xBAT) || !BAThdense(yBAT)) {
		ret = createException(MAL, "batgeom.Distance", "BATs must have dense heads");
		goto clean;
	}
	if(xBAT->hseqbase != yBAT->hseqbase || BATcount(xBAT) != BATcount(yBAT)) {
		ret=createException(MAL, "batgeom.Distance", "BATs must be aligned");
		goto clean;
	}

	//get the GEOS representation of the geometry
	if(!(geosGeometry = wkb2geos(*geomWKB)))
		return createException(MAL, "batgeom.Contains", "wkb2geos failed");

	//chech the type of the geometry and choose the appropriate distance function
	switch(GEOSGeomTypeId(geosGeometry)+1) {
	case wkbPoint:
		if((outBAT = point2point_distance(geosGeometry, xBAT, yBAT)) == NULL) {
			ret = createException(MAL, "batgeom.Distance", "Problem evalauting the contains");
			goto clean;
		}
		break;
	default:
		return createException(MAL, "batgeom.Distance", "This Geometry type is not supported");
	}

	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	goto clean;

clean:
	if(xBAT)
		BBPreleaseref(xBAT->batCacheid);
	if(yBAT)
		BBPreleaseref(yBAT->batCacheid);
	return ret;
}

static str wkbPointsFilterWithImprints_geom_bat(bat* candidateOIDsBAT_id, wkb** geomWKB, bat* xBAT_id, bat* yBAT_id) {
	BAT *xBAT=NULL, *yBAT=NULL, *xCandidateOIDsBAT=NULL, *candidateOIDsBAT=NULL;
	mbr* geomMBR;
	str err;
	double xmin=0.0, xmax=0.0, ymin=0.0, ymax=0.0;

	//get the descriptors of the BATs
	if ((xBAT = BATdescriptor(*xBAT_id)) == NULL) {
		throw(MAL, "batgeom.Filter", RUNTIME_OBJECT_MISSING);
	}
	if ((yBAT = BATdescriptor(*yBAT_id)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		throw(MAL, "batgeom.Filter", RUNTIME_OBJECT_MISSING);
	}

	//check if the BATs have dense heads and are aligned
	if (!BAThdense(xBAT) || !BAThdense(yBAT)) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		return createException(MAL, "batgeom.Filter", "BATs must have dense heads");
	}
	if(xBAT->hseqbase != yBAT->hseqbase || BATcount(xBAT) != BATcount(yBAT)) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		return createException(MAL, "batgeom.Filter", "BATs must be aligned");
	}

	//create the MBR of the geom
	if((err = wkbMBR(&geomMBR, geomWKB)) != MAL_SUCCEED) {
		str msg;
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		msg = createException(MAL, "batgeom.Filter", "%s", err);
		GDKfree(err);
		return msg;
	}
	
	//get candidateOIDs from xBAT (limits are considred to be inclusive)
	xmin = geomMBR->xmin;
	xmax = geomMBR->xmax;
	xCandidateOIDsBAT = BATsubselect(xBAT, NULL, &xmin, &xmax, 1, 1, 0);
	if(xCandidateOIDsBAT == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		return createException(MAL,"batgeom.Filter","Problem filtering xBAT");
	}
	
	//get candidateOIDs using yBAT and xCandidateOIDsBAT
	ymin = geomMBR->ymin;
	ymax = geomMBR->ymax;
	candidateOIDsBAT = BATsubselect(yBAT, xCandidateOIDsBAT, &ymin, &ymax, 1, 1, 0);
	if(candidateOIDsBAT == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		return createException(MAL,"batgeom.Filter","Problem filtering yBAT");
	}

//fprintf(stderr, "Original MBR contains %u points\n", (unsigned int)BATcount(candidateOIDsBAT));
//BATMBRfilter(xmin, ymin, xmax, ymax, geomWKB, (*geomWKB)->srid);
	BBPreleaseref(xBAT->batCacheid);
	BBPreleaseref(yBAT->batCacheid);
	BBPkeepref(*candidateOIDsBAT_id = candidateOIDsBAT->batCacheid);
	return MAL_SUCCEED;
}


/* PBSM */

static str store(void) {
	FILE *f;
	str ret = MAL_SUCCEED;
	char *idxFilename = NULL, *dataFilename = NULL, *infoFilename = NULL;

	if((infoFilename = (char*) GDKmalloc(strlen(filename)+strlen(limitsEnding)+1)) == NULL)
		return createException(MAL, " geomPoints:store", " Problem allocating space for infoFilename");
	strcpy(infoFilename, filename);
	strcpy(infoFilename+strlen(filename), limitsEnding);

	if ((f = fopen(infoFilename, "wb"))) {
		if (fwrite(limits, sizeof(*limits), 1, f) != 1) {
			fclose(f);
			ret =  createException(IO, " geomPoints:store", " Could not save the PBSM index to disk (target: %s)", infoFilename);
			GDKfree(infoFilename);
			infoFilename = NULL;
			return ret;
		}
                fflush(f);
                fclose(f);
		GDKfree(infoFilename);
		infoFilename = NULL;

	} else {
		ret = createException(IO, " geomPoints:store", " Could not open for writting (target: %s)", infoFilename);
		
		GDKfree(infoFilename);
		infoFilename = NULL;
		return ret;
	}

	if((idxFilename = (char*) GDKmalloc(strlen(filename)+strlen(idxEnding)+1)) == NULL)
		return createException(MAL, " geomPoints:store", " Problem allocating space for idxFilename");
	strcpy(idxFilename, filename);
	strcpy(idxFilename+strlen(filename), idxEnding);

	if ((f = fopen(idxFilename, "wb"))) {
		if (fwrite(pbsm_idx, sizeof(pbsm_idx[0]), USHRT_MAX,f) != USHRT_MAX) {
			fclose(f);
			ret = createException(IO, " geomPoints:store", " Could not save the PBSM index to disk (target: %s)", idxFilename);
			GDKfree(idxFilename);
			idxFilename = NULL;
			return ret; 
		}
                fflush(f);
                fclose(f);

		GDKfree(idxFilename);
		idxFilename = NULL;

	} else {
		ret = createException(MAL, " geomPoints:store", " Could not open for writting (target: %s)", idxFilename);

		GDKfree(idxFilename);
		idxFilename = NULL;
		return ret;		
	}

	if((dataFilename = (char*) GDKmalloc(strlen(filename)+strlen(dataEnding)+1)) == NULL)
		return createException(MAL, "geomPoints:store", " Problem allocating space for dataFilename");
	strcpy(dataFilename, filename);
	strcpy(dataFilename+strlen(filename), dataEnding);

	if ((f = fopen(dataFilename, "wb"))) {
		if (fwrite(oids, sizeof(oids[0]), limits->oidsNum, f) != limits->oidsNum) {
			fclose(f);
			ret = createException(IO, " geomPoints:store", " Could not save the PBSM index to disk (target: %s)", dataFilename);
			GDKfree(dataFilename);
			dataFilename = NULL;
			return ret; 
		}
                fflush(f);
                fclose(f);

		GDKfree(dataFilename);
		dataFilename = NULL;

	} else {
		ret = createException(IO, " geomPoints:store", " Could not open for writting (target: %s)", dataFilename);

		GDKfree(dataFilename);
		dataFilename = NULL;
		return ret; 
	}
	
	return MAL_SUCCEED;
}

static str load(void) {
	FILE *f;
	char *idxFilename = NULL, *dataFilename = NULL, *infoFilename = NULL;
	str ret = MAL_SUCCEED;

	if((infoFilename = (char*) GDKmalloc(strlen(filename)+strlen(limitsEnding)+1)) == NULL)
		return createException(MAL, " geomPoints:load", " Problem allocating space for infoFilename");
	strcpy(infoFilename, filename);
	strcpy(infoFilename+strlen(filename), limitsEnding);
	
	if ((f = fopen(infoFilename, "rb"))) {
		//allocate space only if the file exists
		if ((limits = (pbsm_info*)GDKmalloc(sizeof(pbsm_info))) == NULL) {
			GDKfree(infoFilename);
			infoFilename = NULL;
			return createException(MAL, " geomPoints:load", " Problem allocating space for info");
		}
	
		if (fread(limits, sizeof(*limits), 1, f) != 1) {
			fclose(f);
			ret = createException(IO, " geomPoints:load", " Could not read the PBSM index from disk (source: %s)", infoFilename);

			GDKfree(limits);
			limits = NULL;
			GDKfree(infoFilename);
			infoFilename = NULL;
			return ret;
		}

		fclose(f);
		GDKfree(infoFilename);
		infoFilename = NULL;
	} else {
		ret = createException(IO, " geomPoints:load", " Could not open file for reading (source: %s)", infoFilename);

		GDKfree(infoFilename);
		infoFilename = NULL;
		return ret;
	}
	
	if((idxFilename = (char*) GDKmalloc(strlen(filename)+strlen(idxEnding)+1)) == NULL)
		return createException(MAL, " geomPoints:load", " Problem allocating space for idxFilename");
	strcpy(idxFilename, filename);
	strcpy(idxFilename+strlen(filename), idxEnding);
	
	if ((f = fopen(idxFilename, "rb"))) {
		//allocate space only if file exists
		if ((pbsm_idx = (pbsm_ptr*)GDKmalloc(USHRT_MAX * sizeof(pbsm_ptr))) == NULL) {
			GDKfree(limits);
			limits = NULL;
			GDKfree(idxFilename);
			idxFilename = NULL;
			return createException(MAL, " geomPoints:load", " Problem allocating space for pbsm_idx");
		}
		if (fread(pbsm_idx, sizeof(pbsm_idx[0]), USHRT_MAX, f) != USHRT_MAX) {
			fclose(f);
			GDKfree(limits);
			limits = NULL;
			GDKfree(pbsm_idx);
			pbsm_idx = NULL;
			GDKfree(idxFilename);
			idxFilename = NULL;
			return createException(IO, " geomPoints:load", " Could not read the PBSM index from disk (source: %s)", idxFilename);
		}
		fclose(f);
		GDKfree(idxFilename);
		idxFilename = NULL;
	} else {
		ret = createException(IO, " geomPoints:load", " Could not open file for reading (source: %s)", idxFilename);
	
		GDKfree(limits);
		limits = NULL;
		GDKfree(idxFilename);
		idxFilename = NULL;
		return ret;
	}

	
		
	if((dataFilename = (char*) GDKmalloc(strlen(filename)+strlen(dataEnding)+1)) == NULL)
		return createException(MAL, "geomPoints:load", " Problem allocating space for dataFilename");
	strcpy(dataFilename, filename);
	strcpy(dataFilename+strlen(filename), dataEnding);

	if ((f = fopen(dataFilename, "rb"))) {
		//allocate space only if file existis
		if ((oids = (oid*)GDKmalloc(limits->oidsNum * sizeof(oid))) == NULL) {
			GDKfree(limits);
			limits = NULL;
			GDKfree(pbsm_idx);
			pbsm_idx = NULL;
			GDKfree(dataFilename);
			dataFilename = NULL;
			return createException(MAL, " geomPoints:load", " Problem allocating space for oids");
		}
		if (fread(oids, sizeof(oids[0]), limits->oidsNum, f) != limits->oidsNum) {
			fclose(f);
			ret = createException(IO, " geomPoints:load", " Could not read the PBSM index from disk (source: %s)", dataFilename);

			GDKfree(limits);
			limits = NULL;
			GDKfree(pbsm_idx);
			pbsm_idx = NULL;
			GDKfree(oids);
			oids = NULL;
			GDKfree(dataFilename);
			dataFilename = NULL;
			return ret; 
		}

		fclose(f);
		GDKfree(dataFilename);
		dataFilename = NULL;
	} else {
		ret = createException(IO, " geomPoints:load", " Could not open file for reading (source: %s)", dataFilename);
	
		GDKfree(limits);
		limits = NULL;
		GDKfree(pbsm_idx);
		pbsm_idx = NULL;
		GDKfree(dataFilename);
		dataFilename = NULL;
		return ret;
	}

	return MAL_SUCCEED;
}
 
static char *
PBSMcomputeindex1(const dbl *x, const dbl *y, BUN n, double minx, double maxx, double miny, double maxy, oid seqbase) {
	sht *cells;
	BAT *pbsm;
	unsigned long i;
	int shift = sizeof(sht) * 8 / 2;
        sht prevCell, cell;
        unsigned long m = 0, prevO;


	if((pbsm = BATnew(TYPE_void, TYPE_sht, n, TRANSIENT)) == NULL)
		throw(MAL, "pbsm.createindex", MAL_MALLOC_FAIL);
	cells = (sht*) Tloc(pbsm, BUNfirst(pbsm));
	
	// calculate the pbsm values
	for (i = 0; i < n; i++) {
		unsigned char cellx = (unsigned char)((x[i] - minx)/(maxx - minx))*UCHAR_MAX;
                unsigned char celly = (unsigned char)((y[i] - miny)/(maxy - miny))*UCHAR_MAX;
		cells[i] = ((((unsigned short) cellx) << shift)) | ((unsigned short) celly);
	}

	// order the BAT according to the cell values
	/* set some properties */
	BATsetcount(pbsm,n);
	BATseqbase(pbsm, 0);
	pbsm->hsorted = 1; 
	pbsm->hrevsorted = (BATcount(pbsm) <= 1);
	pbsm->tsorted = 0;
	pbsm->trevsorted = 0;
	pbsm->hdense = true;
	pbsm->tdense = false;
	BATseqbase(pbsm, seqbase);
	BATmaterialize(pbsm);
	//BATderiveProps(pbsm, false);
	//BATassertProps(pbsm);
	pbsm = BATorder(BATmirror(pbsm));
	//BATprint(pbsm);
	
	// compress the head
        cells = (sht*) Hloc(pbsm, BUNfirst(pbsm));
	oids  = (oid*) Tloc(pbsm, BUNfirst(pbsm));

        prevCell = cells[0];
        cell = cells[0];
        m = 0;
        prevO = 0;
        for (i = 0; i < n; i++) {
                cell = cells[i];

                if (cell == prevCell) {
                        m++;
                } else {
                        pbsm_idx[cell - SHRT_MIN].offset = prevO;
                        pbsm_idx[cell - SHRT_MIN].count = m;
                        prevCell = cell;
                        prevO = i;
                        m = 1;
                }
        }
        pbsm_idx[cell - SHRT_MIN].offset = prevO;
        pbsm_idx[cell - SHRT_MIN].count = m;
	
	// clean up
	pbsm->T->heap.base = NULL; // need to keep the oids array
        BBPreleaseref(pbsm->batCacheid);

	return MAL_SUCCEED;
}


static char *
PBSMcomputeindex2(const dbl *x, const dbl *y, BUN n, double minx, double maxx, double miny, double maxy, oid seqbase) {
	unsigned long *tmpCount;
	unsigned long i;
	int shift = sizeof(sht) * 8 / 2;
	
	if ((pbsm_idx = (pbsm_ptr*)GDKmalloc(USHRT_MAX * sizeof(pbsm_ptr))) == NULL)
		return createException(MAL, " geomPoints:PBSMcomputeindex2", " Problem allocating space for pbsm_idx");
	if ((tmpCount = (unsigned long*)GDKmalloc(USHRT_MAX * sizeof(unsigned long))) == NULL) {
		GDKfree(pbsm_idx);
		pbsm_idx = NULL;
		return createException(MAL, " geomPoints:PBSMcomputeindex2"," Problem allocating space for tmpCount");
	}

	for (i = 0; i < USHRT_MAX; i++) {
		pbsm_idx[i].count = 0;
		pbsm_idx[i].offset = 0;
		tmpCount[i] = 0;
	}

	if ((limits = (pbsm_info*)GDKmalloc(sizeof(pbsm_info))) == NULL) {
		GDKfree(pbsm_idx);
		pbsm_idx = NULL;
		GDKfree(tmpCount);
		tmpCount = NULL;
		return createException(MAL, " geomPoints:PBSMcomputeindex2", " Problem allocating space for limits");
	}

	limits->xmin = minx;
	limits->xmax = maxx;
	limits->ymin = miny;
	limits->ymax = maxy;
	limits->oidsNum = n;
	
	if ((oids = (oid*)GDKmalloc(n * sizeof(oid))) == NULL) {
		GDKfree(pbsm_idx);
		pbsm_idx = NULL;
		GDKfree(limits);
		limits = NULL;
		GDKfree(tmpCount);
		tmpCount = NULL;
		return createException(MAL, " geomPoints:PBSMcomputeindex2"," Problem allocating space for oids");
	}

	
	// count pbsm values per cell
	for (i = 0; i < n; i++) {
		unsigned char cellx = ((x[i] - minx)/(maxx - minx))*UCHAR_MAX;
                unsigned char celly = ((y[i] - miny)/(maxy - miny))*UCHAR_MAX;
		sht cell = ((((unsigned short) cellx) << shift)) | ((unsigned short) celly);
		pbsm_idx[cell - SHRT_MIN].count++;	
	}

	// compute the offset values before filling in the oid array
	pbsm_idx[0].offset = 0;
	for (i = 1; i < USHRT_MAX; i++) {
		pbsm_idx[i].offset = pbsm_idx[i-1].offset + pbsm_idx[i-1].count;
	}

	// fill in the oid array
	for (i = 0; i < n; i++) {
		unsigned char cellx = ((x[i] - minx)/(maxx - minx))*UCHAR_MAX;
                unsigned char celly = ((y[i] - miny)/(maxy - miny))*UCHAR_MAX;
		sht cell = ((((unsigned short) cellx) << shift)) | ((unsigned short) celly);
		unsigned long position = pbsm_idx[cell - SHRT_MIN].offset + tmpCount[cell - SHRT_MIN];
		oids[position] = i + seqbase;
		tmpCount[cell - SHRT_MIN]++;
	}

	GDKfree(tmpCount);
	tmpCount = NULL;

	return MAL_SUCCEED;
}


static char *
PBSMarraycontains16(BAT **bres, const dbl *x, BAT *batx, const dbl *y,  BAT *baty, mbr *mbb) {
	unsigned long csize = 0, u;
	oid *candidates;
	unsigned char mbrcellxmin, mbrcellxmax, mbrcellymin, mbrcellymax, k,l;
	int shift = sizeof(sht) * 8 / 2;
	unsigned long i;

        /* assert calling sanity */
        assert(*bres != NULL && x != NULL && y != NULL && batx != NULL && baty != NULL);
	assert(batx->hseqbase == baty->hseqbase);
	
	/* load the pbsm index to memory */
	if (pbsm_idx == NULL || oids == NULL) {
		clock_t t = clock();
		str err;

		if((err = load()) != MAL_SUCCEED) {
			str msg = createException(MAL, " geomPoints:PBSMarraycontains16", " %s", err);
			GDKfree(err);
			return msg;
		}
		t = clock() - t;
		fprintf(stderr, "[PBSM] Index loading: %u clicks - %f seconds\n", (unsigned int)t, ((float)t)/CLOCKS_PER_SEC);
	}

	/* generate a pbsm value from the geometry */
	mbrcellxmin = (unsigned char)((mbb->xmin - limits->xmin)/(limits->xmax - limits->xmin) * UCHAR_MAX);
	mbrcellxmax = (unsigned char)((mbb->xmax - limits->xmin)/(limits->xmax - limits->xmin) * UCHAR_MAX);
	mbrcellymin = (unsigned char)((mbb->ymin - limits->ymin)/(limits->ymax - limits->ymin) * UCHAR_MAX);
	mbrcellymax = (unsigned char)((mbb->ymax - limits->ymin)/(limits->ymax - limits->ymin) * UCHAR_MAX);

	csize = 0;
	for (k = mbrcellxmin; k <= mbrcellxmax; k++) {
		for (l = mbrcellymin; l <= mbrcellymax; l++) {
			sht mbrc = ((((unsigned short) k) << shift)) | ((unsigned short) l);
			//sht mbrc = ((((sht) k) << shift)) | ((sht) l);
			csize += pbsm_idx[mbrc - SHRT_MIN].count;
		}
	}

	/* get candidate oid from the pbsm index */
	if ((candidates = GDKmalloc(csize * sizeof(oid))) == NULL)
		return createException(MAL, "batgeom.Filter", "Problem allocating space for %lu oids", csize);
	i = 0;
	for (k = mbrcellxmin; k <= mbrcellxmax; k++) {
		for (l = mbrcellymin; l <= mbrcellymax; l++) {
			sht mbrc = ((((unsigned short) k) << shift)) | ((unsigned short) l);
			unsigned short mcell = mbrc - SHRT_MIN;
			
			for (u = 0; u < pbsm_idx[mcell].count; u++) {
				//fprintf(stderr,"[PBSM] Copying cell %d (offset %ld, count %ld)\n", mcell,pbsm_idx[mcell].offset,pbsm_idx[mcell].count);
				candidates[i] = oids[pbsm_idx[mcell].offset + u];
				i++;
			}

		}
	}

	assert(BAThdense(batx) && BAThdense(baty));

	if ((*bres = BATnew(TYPE_void, TYPE_oid, csize, TRANSIENT)) == NULL) {
		return createException(MAL, "batgeom.Filter", "Problem allocating space for the output BAT");
	}

	for (i = 0; i < csize; i++) {
		oid *o = &(candidates[i]);
		BUNfastins(*bres, NULL, o);
	}

	/* candidates are expected to be ordered */
	BATseqbase(*bres, oid_nil); // avoid materialization of the void head
	*bres = BATmirror(BATorder(BATmirror(*bres)));
	BATseqbase(*bres, 0);

	//BATkey(BATmirror(*bres), TRUE);
	(*bres)->hdense = 1;
	(*bres)->hsorted = 1;
	(*bres)->tsorted = 1;
	//(*bres)->tkey = TRUE;
	BATderiveProps(*bres, false);
	
	/* clean up */
	//GDKfree(pbsm_idx);
	//GDKfree(oids);

        return MAL_SUCCEED;
}

static char *
PBSMselect_(BAT **ret, BAT *bx, BAT *by, mbr *g) {
	BAT *bres = NULL;
	BUN n;
	char *msg = NULL;
	dbl *x = NULL, *y = NULL;

	assert (ret != NULL);
        assert (bx != NULL && by != NULL);

	n = BATcount(bx);

	if (bx->ttype != by->ttype)
		throw(MAL, "batpbsm.contains16", "tails of input BATs must be identical");

	/* get direct access to the tail arrays */
        x = (dbl*) Tloc(bx, BUNfirst(bx));
        y = (dbl*) Tloc(by, BUNfirst(by));

	/* allocate result BAT */
	bres = BATnew(TYPE_void, TYPE_oid, n, TRANSIENT);
	if (bres == NULL)
		throw(MAL, "batpbsm.contains16", MAL_MALLOC_FAIL);

	msg = PBSMarraycontains16( &bres, x, bx, y , by, g);

	if (msg != MAL_SUCCEED) {
		return msg;
	} else {
		*ret = bres;
	}

	return msg;
}

static str wkbPointsFilterWithPBSM_geom_bat(bat* candidateOIDsBAT_id, wkb** geomWKB, bat* xBAT_id, bat* yBAT_id) {
	BAT *xBAT=NULL, *yBAT=NULL, *candidateOIDsBAT=NULL;
	mbr* geomMBR;
	str err;
	clock_t t;

	//get the descriptors of the BATs
	if ((xBAT = BATdescriptor(*xBAT_id)) == NULL) {
		throw(MAL, "batgeom.Filter", RUNTIME_OBJECT_MISSING);
	}
	if ((yBAT = BATdescriptor(*yBAT_id)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		throw(MAL, "batgeom.Filter", RUNTIME_OBJECT_MISSING);
	}

	//check if the BATs have dense heads and are aligned
	if (!BAThdense(xBAT) || !BAThdense(yBAT)) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		return createException(MAL, "batgeom.Filter", "BATs must have dense heads");
	}
	if(xBAT->hseqbase != yBAT->hseqbase || BATcount(xBAT) != BATcount(yBAT)) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		return createException(MAL, "batgeom.Filter", "BATs must be aligned");
	}

	//create the MBR of the geom
	if((err = wkbMBR(&geomMBR, geomWKB)) != MAL_SUCCEED) {
		str msg;
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		msg = createException(MAL, "batgeom.Filter", "%s", err);
		GDKfree(err);
		return msg;
	}
	t = clock();
	if(((err = PBSMselect_(&candidateOIDsBAT, xBAT, yBAT, geomMBR)) != MAL_SUCCEED)
		|| (candidateOIDsBAT == NULL)) {
		str msg;
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		msg = createException(MAL, "batgeom.Filter", "%s", err);
		GDKfree(err);
		return msg;
	}
	t = clock() - t;
	fprintf(stderr, "[PREFILTERING] PBSM: %u clicks - %f seconds\n", (unsigned int)t, ((float)t)/CLOCKS_PER_SEC);


	BBPreleaseref(xBAT->batCacheid);
	BBPreleaseref(yBAT->batCacheid);
	BBPkeepref(*candidateOIDsBAT_id = candidateOIDsBAT->batCacheid);

	return MAL_SUCCEED;
}

str pbsmIndex_bat(bat* outBAT_id, bat* xBAT_id, bat* yBAT_id, double* xmin, double* ymin, double* xmax, double* ymax) {
	BAT *xBAT=NULL, *yBAT=NULL, *outBAT=NULL;
	str err ;
	double *x = NULL, *y = NULL;
	BUN n;
 
	//get the descriptors of the BATs
	if ((xBAT = BATdescriptor(*xBAT_id)) == NULL) {
		return createException(MAL, " geomPoints:pbsmIndex_bat", " Problem obtaining xBAT descriptor");
	}

	if ((yBAT = BATdescriptor(*yBAT_id)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		return createException(MAL, " geomPoints:pbsmIndex_bat", " Problem obtaining yBAT descriptor");
	}

	//check if the BATs have dense heads and are aligned
	if (!BAThdense(xBAT) || !BAThdense(yBAT)) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		return createException(MAL, " geomPoints:pbsmIndex_bat", " BATs must have dense heads");
	}
	if(xBAT->hseqbase != yBAT->hseqbase || BATcount(xBAT) != BATcount(yBAT)) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		return createException(MAL, " geomPoints:pbsmIndex_bat", " BATs must be aligned");
	}

	n = BATcount(xBAT);

	x = (double*) Tloc(xBAT, BUNfirst(xBAT));
        y = (double*) Tloc(yBAT, BUNfirst(yBAT));

	if ((err = PBSMcomputeindex2(x, y, n, *xmin, *xmax, *ymin, *ymax, xBAT->hseqbase)) != MAL_SUCCEED) {
		str msg = createException(MAL, " geomPoints:pbsmIndex_bat", " %s", err);
		GDKfree(err);
		return msg;
	}

	//Store the indices to files for future use
	if((err = store()) != MAL_SUCCEED) {
		str msg = createException(MAL, " geomPoints:pbsmIndex_bat", " %s", err);
		GDKfree(err);
		return msg;
	}

	//create a dummy BAT to return
	if ((outBAT = BATnew(TYPE_void, TYPE_int, n, TRANSIENT)) == NULL) {
		return createException(MAL, " geomPoints:pbsmIndex_bat", " Problem creating output BAT");
	}

	//set the first idx of the new BAT equal to that of the x BAT (which is equal to the y BAT)
	BATseqbase(outBAT, xBAT->hseqbase);
	BATsetcount(outBAT, 0);

	BBPreleaseref(xBAT->batCacheid);
	BBPreleaseref(yBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

if(false) PBSMcomputeindex1(x, y, n, *xmin, *xmax, *ymin, *ymax, xBAT->hseqbase);

	return MAL_SUCCEED;
}


/*Wrappers that choose the version of the spatial function and the filter that should be used*/

str wkbPointsContains_geom_bat(bat* outBAT_id, wkb** geomWKB, bat* xBAT_id, bat* yBAT_id, int* srid, int* filterVersion, int* spatialVersion) {
	(void) *filterVersion; //not used here but in the MAL optimiser
	switch(*spatialVersion) {
	case 1: 
		return wkbPointsGeomContains_geom_bat(outBAT_id, geomWKB, xBAT_id, yBAT_id, srid);	
	case 2:
		return wkbPointsWindingContains_geom_bat(outBAT_id, geomWKB, xBAT_id, yBAT_id, srid);
	default:
		return createException(MAL, "batgeom.Contains", "Unknown Contains version");
	}
}

str wkbPointsDistance_geom_bat(bat* outBAT_id, wkb** geomWKB, bat* xBAT_id, bat* yBAT_id, int* srid, int* filterVersion, int* spatialVersion) {
	(void) *filterVersion; //not used here but in the MAL optimiser
	switch(*spatialVersion) {
	case 1: 
		return wkbPointsGeomDistance_geom_bat(outBAT_id, geomWKB, xBAT_id, yBAT_id, srid);	
	case 2:
		return wkbPointsCartesianDistance_geom_bat(outBAT_id, geomWKB, xBAT_id, yBAT_id, srid);
	default:
		return createException(MAL, "batgeom.Distance", "Unknown Distance version");
	}
}

str wkbPointsFilter_geom_bat(bat* candidatesBAT_id, wkb** geomWKB, bat* xBAT_id, bat* yBAT_id, int *filterVersion) {
	switch(*filterVersion) {
	case 1:
		return wkbPointsFilterWithImprints_geom_bat(candidatesBAT_id, geomWKB, xBAT_id, yBAT_id);
	case 2:
		return wkbPointsFilterWithPBSM_geom_bat(candidatesBAT_id, geomWKB, xBAT_id, yBAT_id);
	default:
		return createException(MAL, "batgeom.Filter", "Unknown Filter version");
	}
}
