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

/*
typedef struct pbsm_ptr {
	BUN offset;
	unsigned long count;
} pbsm_ptr;

static pbsm_ptr *pbsm_idx = NULL;
static oid *oids = NULL;
static mbr *limits = NULL;

//hard coded filename
static char* filename = "../pbsmIndex_20m";
*/

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
/*		if(i%1000 == 0) {
			fprintf(stderr, "%u: %f\n", (unsigned int)i, distancesArray[i]);
			fprintf(stderr, "\t(%f, %f), (%f, %f)\n", xPointCoordinate, yPointCoordinate, xBATCoordinate[i], yBATCoordinate[i]);
    		}
*/
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
	//case 2:
	//	return wkbPointsFilterWithPBSM_geom_bat(candidatesBAT_id, geomWKB, xBAT_id, yBAT_id);
	default:
		return createException(MAL, "batgeom.Filter", "Unknown Filter version");
	}
}
