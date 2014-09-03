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

//it gets two BATs with x,y coordinates and returns a new BAT with the points
static BAT* BATMakePoint2D(BAT* xBAT, BAT* yBAT) {
	BAT *outBAT = NULL;
	BATiter xBAT_iter, yBAT_iter;	
	BUN i;

	//check if the BATs have dense heads and are aligned
	if (!BAThdense(xBAT) || !BAThdense(yBAT)) {
		GDKerror("BATMakePoint2D: BATs must have dense heads");
		return NULL;
	}
	if(xBAT->hseqbase != yBAT->hseqbase || BATcount(xBAT) != BATcount(yBAT)) {
		GDKerror("BATMakePoint2D: BATs must be aligned");
		return NULL;
	}

	//create a new BAT
	if ((outBAT = BATnew(TYPE_void, ATOMindex("wkb"), BATcount(xBAT), TRANSIENT)) == NULL) {
		GDKerror("BATMakePoint2D: Could not create new BAT for the output");
		return NULL;
	}

	//set the first idx of the new BAT equal to that of the x BAT (which is equal to the y BAT)
	BATseqbase(outBAT, xBAT->hseqbase);

	//iterator over the BATs	
	xBAT_iter = bat_iterator(xBAT);
	yBAT_iter = bat_iterator(yBAT);

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
		BUNappend(outBAT,point,TRUE); //add the result to the outBAT
		GDKfree(point);
	}

	return outBAT;

}

static BAT* BATSetSRID(BAT* pointsBAT, int srid) {
	BAT *outBAT = NULL;
	BATiter pointsBAT_iter;	
	BUN p=0, q=0;
	wkb *pointWKB = NULL;

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
		BUNappend(outBAT,outWKB,TRUE); //add the point to the new BAT
		GDKfree(outWKB);
		outWKB = NULL;
	}

	return outBAT;
}

static BAT* BATContains(wkb** geomWKB, BAT* geometriesBAT) {
	BAT *outBAT = NULL;
	BATiter geometriesBAT_iter;	
	BUN p=0, q=0;
	wkb *geometryWKB = NULL;

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

	//set the first idx of the new BAT equal to that of the x BAT (which is equal to the y BAT)
	BATseqbase(outBAT, geometriesBAT->hseqbase);

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
		BUNappend(outBAT,&outBIT,TRUE); //add the point to the new BAT
	}

	return outBAT;

}

str wkbPointsContains_geom_bat(bat* outBAT_id, wkb** geomWKB, bat* xBAT_id, bat* yBAT_id, int* srid) {
	BAT *xBAT=NULL, *yBAT=NULL, *outBAT=NULL;
	BAT *pointsBAT = NULL, *pointsWithSRIDBAT=NULL;
	str ret=MAL_SUCCEED;

	//get the descriptors of the BATs
	if ((xBAT = BATdescriptor(*xBAT_id)) == NULL) {
		throw(MAL, "batgeom.wkbContainsFiltered", RUNTIME_OBJECT_MISSING);
	}
	if ((yBAT = BATdescriptor(*yBAT_id)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		throw(MAL, "batgeom.wkbContainsFiltered", RUNTIME_OBJECT_MISSING);
	}
	
	//check if the BATs have dense heads and are aligned
	if (!BAThdense(xBAT) || !BAThdense(yBAT)) {
		ret = createException(MAL, "batgeom.wkbContainsFiltered", "BATs must have dense heads");
		goto clean;
	}
	if(xBAT->hseqbase != yBAT->hseqbase || BATcount(xBAT) != BATcount(yBAT)) {
		ret=createException(MAL, "batgeom.wkbContainsFiltered", "BATs must be aligned");
		goto clean;
	}

	//here the BAT version of some contain function that takes the BATs of the x y coordinates should be called
	//create the points BAT
	if((pointsBAT = BATMakePoint2D(xBAT, yBAT)) == NULL) {
		ret = createException(MAL, "batgeom.wkbContainsFiltered", "Problem creating the points from the coordinates");
		goto clean;
	}

	if((pointsWithSRIDBAT = BATSetSRID(pointsBAT, *srid)) == NULL) {
		ret = createException(MAL, "batgeom.wkbContainsFiltered", "Problem setting srid to the points");
		goto clean;
	}

	if((outBAT = BATContains(geomWKB, pointsWithSRIDBAT)) == NULL) {
		ret = createException(MAL, "batgeom.wkbContainsFiltered", "Problem evalauting the contains");
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

str wkbFilteredPointsContains_geom_bat(bat* outBAT_id, wkb** geomWKB, bat* xBAT_id, bat* yBAT_id, bat* OIDsBAT_id, int* srid) {
	BAT *xBAT=NULL, *yBAT=NULL, *OIDsBAT=NULL, *outBAT=NULL, *xFilteredBAT=NULL, *yFilteredBAT=NULL;
	BAT *pointsBAT = NULL, *pointsWithSRIDBAT=NULL;
	str ret=MAL_SUCCEED;

	//get the descriptors of the BATs
	if ((xBAT = BATdescriptor(*xBAT_id)) == NULL) {
		throw(MAL, "batgeom.wkbContainsFiltered", RUNTIME_OBJECT_MISSING);
	}
	if ((yBAT = BATdescriptor(*yBAT_id)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		throw(MAL, "batgeom.wkbContainsFiltered", RUNTIME_OBJECT_MISSING);
	}
	if ((OIDsBAT = BATdescriptor(*OIDsBAT_id)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		throw(MAL, "batgeom.wkbContainsFiltered", RUNTIME_OBJECT_MISSING);
	}
	
	//check if the BATs have dense heads and are aligned
	if (!BAThdense(xBAT) || !BAThdense(yBAT) || !BAThdense(OIDsBAT)) {
		ret = createException(MAL, "batgeom.wkbContainsFiltered", "BATs must have dense heads");
		goto clean;
	}
	if(xBAT->hseqbase != yBAT->hseqbase || BATcount(xBAT) != BATcount(yBAT)) {
		ret=createException(MAL, "batgeom.wkbContainsFiltered", "BATs must be aligned");
		goto clean;
	}

	//project the x and y BATs
	xFilteredBAT = BATproject(OIDsBAT, xBAT);
	if(xFilteredBAT == NULL) {
		ret=createException(MAL,"batgeom.wkbContainsFiltered","Problem projecting xBAT");
		goto clean;
	}
	yFilteredBAT = BATproject(OIDsBAT, yBAT);
	if(xFilteredBAT == NULL) {
		ret=createException(MAL,"batgeom.wkbContainsFiltered","Problem projecting yBAT");
		goto clean;
	}

	//here the BAT version of some contain function that takes the BATs of the x y coordinates should be called
	//create the points BAT
	if((pointsBAT = BATMakePoint2D(xFilteredBAT, yFilteredBAT)) == NULL) {
		ret = createException(MAL, "batgeom.wkbContainsFiltered", "Problem creating the points from the coordinates");
		goto clean;
	}
	//set the srid	
	if((pointsWithSRIDBAT = BATSetSRID(pointsBAT, *srid)) == NULL) {
		ret = createException(MAL, "batgeom.wkbContainsFiltered", "Problem setting srid to the points");
		goto clean;
	}
	//check the contains
	if((outBAT = BATContains(geomWKB, pointsWithSRIDBAT)) == NULL) {
		ret = createException(MAL, "batgeom.wkbContainsFiltered", "Problem evalauting the contains");
		goto clean;
	}


	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	goto clean;

clean:
	if(xBAT)
		BBPreleaseref(xBAT->batCacheid);
	if(yBAT)
		BBPreleaseref(yBAT->batCacheid);
	if(OIDsBAT)
		BBPreleaseref(OIDsBAT->batCacheid);
	if(xFilteredBAT)
		BBPreleaseref(xFilteredBAT->batCacheid);
	if(yFilteredBAT)
		BBPreleaseref(yFilteredBAT->batCacheid);
	if(pointsBAT)
		BBPreleaseref(pointsBAT->batCacheid);
	if(pointsWithSRIDBAT)
		BBPreleaseref(pointsWithSRIDBAT->batCacheid);
	return ret;
}

str wkbFilterWithImprints_geom_bat(bat* candidateOIDsBAT_id, wkb** geomWKB, bat* xBAT_id, bat* yBAT_id) {
	BAT *xBAT=NULL, *yBAT=NULL, *xCandidateOIDsBAT=NULL, *candidateOIDsBAT=NULL;
	mbr* geomMBR;
	str err;
	double xmin=0.0, xmax=0.0, ymin=0.0, ymax=0.0;

	//get the descriptors of the BATs
	if ((xBAT = BATdescriptor(*xBAT_id)) == NULL) {
		throw(MAL, "batgeom.wkbFilterWithImprints", RUNTIME_OBJECT_MISSING);
	}
	if ((yBAT = BATdescriptor(*yBAT_id)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		throw(MAL, "batgeom.wkbFilterWithImprints", RUNTIME_OBJECT_MISSING);
	}

	//check if the BATs have dense heads and are aligned
	if (!BAThdense(xBAT) || !BAThdense(yBAT)) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		return createException(MAL, "batgeom.wkbFilterWithImprints", "BATs must have dense heads");
	}
	if(xBAT->hseqbase != yBAT->hseqbase || BATcount(xBAT) != BATcount(yBAT)) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		return createException(MAL, "batgeom.wkbFilterWithImprints", "BATs must be aligned");
	}

	//create the MBR of the geom
	if((err = wkbMBR(&geomMBR, geomWKB)) != MAL_SUCCEED) {
		str msg;
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		msg = createException(MAL, "batgeom.wkbFilterWithImprints", "%s", err);
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
		return createException(MAL,"batgeom.wkbFilterWithImprints","Problem filtering xBAT");
	}
	
	//get candidateOIDs using yBAT and xCandidateOIDsBAT
	ymin = geomMBR->ymin;
	ymax = geomMBR->ymax;
	candidateOIDsBAT = BATsubselect(yBAT, xCandidateOIDsBAT, &ymin, &ymax, 1, 1, 0);
	if(candidateOIDsBAT == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		return createException(MAL,"batgeom.wkbFilterWithImprints","Problem filtering yBAT");
	}

	BBPreleaseref(xBAT->batCacheid);
	BBPreleaseref(yBAT->batCacheid);
	BBPkeepref(*candidateOIDsBAT_id = candidateOIDsBAT->batCacheid);

	return MAL_SUCCEED;
}
