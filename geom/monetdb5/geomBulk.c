/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * Foteini Alvanaki
 */

#include "geom.h"

/*******************************/
/********** One input **********/
/*******************************/

str
geom_2_geom_bat(bat *outBAT_id, bat *inBAT_id, int *columnType, int *columnSRID)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL, *outWKB = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batcalc.wkb", SQLSTATE(38000) RUNTIME_OBJECT_MISSING);
	}

	//create a new BAT, aligned with input BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batcalc.wkb", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	//iterator over the BAT
	inBAT_iter = bat_iterator(inBAT);
	//for (i = 0; i < BATcount(inBAT); i++) {
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;

		//if for used --> inWKB = (wkb *) BUNtail(inBATi, i);
		inWKB = (wkb *) BUNtail(inBAT_iter, p);
		if ((err = geom_2_geom(&outWKB, &inWKB, columnType, columnSRID)) != MAL_SUCCEED) {	//check type
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, outWKB, FALSE) != GDK_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			GDKfree(outWKB);
			throw(MAL, "batcalc.wkb", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		GDKfree(outWKB);
		outWKB = NULL;
	}

	BBPunfix(inBAT->batCacheid);
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
		throw(MAL, "batgeom.wkbFromText", SQLSTATE(38000) RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.wkbFromText", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		if (BUNappend(outBAT, outSingle, FALSE) != GDK_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			GDKfree(outSingle);
			throw(MAL, "batgeom.wkbFromText", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
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
	double outDbl = 0.0;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.coordinateFromMBR", SQLSTATE(38000) RUNTIME_OBJECT_MISSING);
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("dbl"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.coordinateFromMBR", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	//iterator over the BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;

		inMBR = (mbr *) BUNtail(inBAT_iter, p);
		if ((err = wkbCoordinateFromMBR(&outDbl, &inMBR, coordinateIdx)) != MAL_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, &outDbl, FALSE) != GDK_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			throw(MAL, "batgeom.coordinateFromMBR", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	}

	BBPunfix(inBAT->batCacheid);
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
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("str"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		if (BUNappend(outBAT, outSingle, FALSE) != GDK_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			GDKfree(outSingle);
			throw(MAL, name, SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
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
/*************************** IN: wkb - OUT: wkb ****************************/
/***************************************************************************/

static str
WKBtoWKB_bat(bat *outBAT_id, bat *inBAT_id, str (*func) (wkb **, wkb **), const char *name)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;
		wkb *outSingle;

		inWKB = (wkb *) BUNtail(inBAT_iter, p);
		if ((err = (*func) (&outSingle, &inWKB)) != MAL_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, outSingle, FALSE) != GDK_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			GDKfree(outSingle);
			throw(MAL, name, SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		GDKfree(outSingle);
		outSingle = NULL;
	}

	//set the number of elements in the outBAT
	BATsetcount(outBAT, BATcount(inBAT));

	BBPunfix(inBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

str
wkbBoundary_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoWKB_bat(outBAT_id, inBAT_id, wkbBoundary, "batgeom.wkbBoundary");
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

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;
		wkb *outSingle;

		inWKB = (wkb *) BUNtail(inBAT_iter, p);
		if ((err = (*func) (&outSingle, &inWKB, flag)) != MAL_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, outSingle, FALSE) != GDK_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			GDKfree(outSingle);
			throw(MAL, name, SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		GDKfree(outSingle);
		outSingle = NULL;
	}

	//set the number of elements in the outBAT
	BATsetcount(outBAT, BATcount(inBAT));

	BBPunfix(inBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}

str
wkbGeometryN_bat(bat *outBAT_id, bat *inBAT_id, const int *flag)
{
	return WKBtoWKBflagINT_bat(outBAT_id, inBAT_id, flag, wkbGeometryN, "batgeom.wkbGeometryN");
}

/***************************************************************************/
/*************************** IN: wkb - OUT: bit ****************************/
/***************************************************************************/

static str
WKBtoBIT_bat(bat *outBAT_id, bat *inBAT_id, str (*func) (bit *, wkb **), const char *name)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("bit"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;
		bit outSingle;

		inWKB = (wkb *) BUNtail(inBAT_iter, p);
		if ((err = (*func) (&outSingle, &inWKB)) != MAL_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, &outSingle, FALSE) != GDK_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			throw(MAL, name, SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	}

	//set the number of elements in the outBAT
	BATsetcount(outBAT, BATcount(inBAT));

	BBPunfix(inBAT->batCacheid);
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
/*************************** IN: wkb - OUT: int ****************************/
/***************************************************************************/

static str
WKBtoINT_bat(bat *outBAT_id, bat *inBAT_id, str (*func) (int *, wkb **), const char *name)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("int"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;
		int outSingle;

		inWKB = (wkb *) BUNtail(inBAT_iter, p);
		if ((err = (*func) (&outSingle, &inWKB)) != MAL_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, &outSingle, FALSE) != GDK_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			throw(MAL, name, SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	}

	//set the number of elements in the outBAT
	BATsetcount(outBAT, BATcount(inBAT));

	BBPunfix(inBAT->batCacheid);
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

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("int"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;
		int outSingle;

		inWKB = (wkb *) BUNtail(inBAT_iter, p);
		if ((err = (*func) (&outSingle, &inWKB, flag)) != MAL_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, &outSingle, FALSE) != GDK_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			throw(MAL, name, SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	}

	//set the number of elements in the outBAT
	BATsetcount(outBAT, BATcount(inBAT));

	BBPunfix(inBAT->batCacheid);
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

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.wkbGetCoordinate", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("dbl"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.wkbGetCoordinate", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;
		double outSingle;

		inWKB = (wkb *) BUNtail(inBAT_iter, p);
		if ((err = wkbGetCoordinate(&outSingle, &inWKB, flag)) != MAL_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, &outSingle, FALSE) != GDK_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			throw(MAL, "batgeom.wkbGetCoordinate", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	}

	//set the number of elements in the outBAT
	BATsetcount(outBAT, BATcount(inBAT));

	BBPunfix(inBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;

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
		ret = createException(MAL, "batgeom.wkbBox2D", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto clean;
	}
	//check if the BATs are aligned
	if (aBAT->hseqbase != bBAT->hseqbase || BATcount(aBAT) != BATcount(bBAT)) {
		ret = createException(MAL, "batgeom.wkbBox2D", SQLSTATE(38000) "Columns must be aligned");
		goto clean;
	}
	//create a new BAT for the output
	if ((outBAT = COLnew(aBAT->hseqbase, ATOMindex("mbr"), BATcount(aBAT), TRANSIENT)) == NULL) {
		ret = createException(MAL, "batgeom.wkbBox2D", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto clean;
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = 0; i < BATcount(aBAT); i++) {
		mbr *outSingle;

		wkb *aWKB = (wkb *) BUNtail(aBAT_iter, i);
		wkb *bWKB = (wkb *) BUNtail(bBAT_iter, i);

		if ((ret = wkbBox2D(&outSingle, &aWKB, &bWKB)) != MAL_SUCCEED) {
			BBPreclaim(outBAT);
			goto clean;
		}
		if (BUNappend(outBAT, outSingle, FALSE) != GDK_SUCCEED) {
			BBPreclaim(outBAT);
			GDKfree(outSingle);
			ret = createException(MAL, "batgeom.wkbBox2D", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			goto clean;
		}
		GDKfree(outSingle);
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
		ret = createException(MAL, "batgeom.Contains", SQLSTATE(38000) "Problem retrieving BATs");
		goto clean;
	}
	//check if the BATs are aligned
	if (aBAT->hseqbase != bBAT->hseqbase || BATcount(aBAT) != BATcount(bBAT)) {
		ret = createException(MAL, "batgeom.Contains", SQLSTATE(38000) "Columns must be aligned");
		goto clean;
	}
	//create a new BAT for the output
	if ((outBAT = COLnew(aBAT->hseqbase, ATOMindex("bit"), BATcount(aBAT), TRANSIENT)) == NULL) {
		ret = createException(MAL, "batgeom.Contains", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto clean;
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = 0; i < BATcount(aBAT); i++) {
		bit outBIT;

		wkb *aWKB = (wkb *) BUNtail(aBAT_iter, i);
		wkb *bWKB = (wkb *) BUNtail(bBAT_iter, i);

		if ((ret = wkbContains(&outBIT, &aWKB, &bWKB)) != MAL_SUCCEED) {
			BBPreclaim(outBAT);
			goto clean;
		}
		if (BUNappend(outBAT, &outBIT, FALSE) != GDK_SUCCEED) {
			BBPreclaim(outBAT);
			ret = createException(MAL, "batgeom.Contains", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			goto clean;
		}
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
		throw(MAL, "batgeom.Contains", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("bit"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.Contains", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		if (BUNappend(outBAT, &outBIT, FALSE) != GDK_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			throw(MAL, "batgeom.Contains", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
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
		throw(MAL, "batgeom.Contains", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("bit"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.Contains", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		if (BUNappend(outBAT, &outBIT, FALSE) != GDK_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			throw(MAL, "batgeom.Contains", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
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
		throw(MAL, "batgeom.wkb", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT))) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.wkb", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	//pointers to the first valid elements of the x and y BATS
	inWKB = (wkb **) Tloc(inBAT, 0);
	for (i = 0; i < BATcount(inBAT); i++) {	//iterate over all valid elements
		str err = NULL;
		if ((err = wkbFromWKB(&outWKB, &inWKB[i])) != MAL_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, outWKB, FALSE) != GDK_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			GDKfree(outWKB);
			throw(MAL, "batgeom.wkb", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		GDKfree(outWKB);
		outWKB = NULL;
	}

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
		throw(MAL, "batgeom.wkbMakePoint", SQLSTATE(38000) "POINTZM is not supported");

	//get the BATs
	if ((xBAT = BATdescriptor(*xBAT_id)) == NULL || (yBAT = BATdescriptor(*yBAT_id)) == NULL || (*zmFlag == 10 && (zBAT = BATdescriptor(*zBAT_id)) == NULL)
	    || (*zmFlag == 1 && (mBAT = BATdescriptor(*mBAT_id)) == NULL)) {

		ret = createException(MAL, "batgeom.wkbMakePoint", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto clean;
	}
	//check if the BATs are aligned
	if (xBAT->hseqbase != yBAT->hseqbase ||
	    BATcount(xBAT) != BATcount(yBAT) ||
	    (zBAT && (xBAT->hseqbase != zBAT->hseqbase || BATcount(xBAT) != BATcount(zBAT))) ||
	    (mBAT && (xBAT->hseqbase != mBAT->hseqbase || BATcount(xBAT) != BATcount(mBAT)))) {
		ret = createException(MAL, "batgeom.wkbMakePoint", SQLSTATE(38000) "Columns must be aligned");
		goto clean;
	}
	//create a new BAT for the output
	if ((outBAT = COLnew(xBAT->hseqbase, ATOMindex("wkb"), BATcount(xBAT), TRANSIENT)) == NULL) {
		ret = createException(MAL, "batgeom.wkbMakePoint", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto clean;
	}

	//iterator over the BATs
	xBAT_iter = bat_iterator(xBAT);
	yBAT_iter = bat_iterator(yBAT);
	if (zBAT)
		zBAT_iter = bat_iterator(zBAT);
	if (mBAT)
		mBAT_iter = bat_iterator(mBAT);

	for (i = 0; i < BATcount(xBAT); i++) {
		wkb *pointWKB = NULL;

		double x = *((double *) BUNtail(xBAT_iter, i));
		double y = *((double *) BUNtail(yBAT_iter, i));
		double z = 0.0;
		double m = 0.0;

		if (zBAT)
			z = *((double *) BUNtail(zBAT_iter, i));
		if (mBAT)
			m = *((double *) BUNtail(mBAT_iter, i));

		if ((ret = wkbMakePoint(&pointWKB, &x, &y, &z, &m, zmFlag)) != MAL_SUCCEED) {	//check

			BBPreclaim(outBAT);
			goto clean;
		}
		if (BUNappend(outBAT, pointWKB, FALSE) != GDK_SUCCEED) {
			BBPreclaim(outBAT);
			GDKfree(pointWKB);
			ret = createException(MAL, "batgeom.WkbMakePoint", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			goto clean;
		}
		GDKfree(pointWKB);
	}

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
		throw(MAL, "batgeom.SetSRID", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.SetSRID", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	//iterator over the BATs
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {
		str err = NULL;
		wkb *outWKB = NULL;

		wkb *inWKB = (wkb *) BUNtail(inBAT_iter, p);

		if ((err = wkbSetSRID(&outWKB, &inWKB, srid)) != MAL_SUCCEED) {	//set SRID
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, outWKB, FALSE) != GDK_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			GDKfree(outWKB);
			throw(MAL, "batgeom.SetSRID", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
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
		ret = createException(MAL, "batgeom.Distance", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto clean;
	}
	//check if the BATs are aligned
	if (aBAT->hseqbase != bBAT->hseqbase || BATcount(aBAT) != BATcount(bBAT)) {
		ret = createException(MAL, "batgeom.Distance", SQLSTATE(38000) "Columns must be aligned");
		goto clean;
	}
	//create a new BAT for the output
	if ((outBAT = COLnew(aBAT->hseqbase, ATOMindex("dbl"), BATcount(aBAT), TRANSIENT)) == NULL) {
		ret = createException(MAL, "batgeom.Distance", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto clean;
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = 0; i < BATcount(aBAT); i++) {
		double distanceVal = 0;

		wkb *aWKB = (wkb *) BUNtail(aBAT_iter, i);
		wkb *bWKB = (wkb *) BUNtail(bBAT_iter, i);

		if ((ret = wkbDistance(&distanceVal, &aWKB, &bWKB)) != MAL_SUCCEED) {	//check

			BBPreclaim(outBAT);
			goto clean;
		}
		if (BUNappend(outBAT, &distanceVal, FALSE) != GDK_SUCCEED) {
			BBPreclaim(outBAT);
			ret = createException(MAL, "batgeom.Distance", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			goto clean;
		}
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
		throw(MAL, "batgeom.Distance", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("dbl"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.Distance", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		if (BUNappend(outBAT, &distanceVal, FALSE) != GDK_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			throw(MAL, "batgeom.Distance", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
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
		throw(MAL, "batgeom.MBRfilter", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((bBAT = BATdescriptor(*bBAT_id)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		throw(MAL, "batgeom.MBRfilter", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	if (aBAT->hseqbase != bBAT->hseqbase ||	//the idxs of the headers of the BATs are not the same
	    BATcount(aBAT) != BATcount(bBAT)) {	//the number of valid elements in the BATs are not the same
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.MBRfilter", SQLSTATE(38000) "The arguments must have dense and aligned heads");
	}
	//create two new BATs for the output
	if ((aBATfiltered = COLnew(aBAT->hseqbase, ATOMindex("wkb"), BATcount(aBAT), TRANSIENT)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.MBRfilter", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	if ((bBATfiltered = COLnew(bBAT->hseqbase, ATOMindex("wkb"), BATcount(bBAT), TRANSIENT)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.MBRfilter", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = 0; i < BATcount(aBAT); i++) {
		str err = NULL;
		aWKB = (wkb *) BUNtail(aBAT_iter, i);
		bWKB = (wkb *) BUNtail(bBAT_iter, i);

		//check the containment of the MBRs
		if ((err = mbrOverlaps_wkb(&outBIT, &aWKB, &bWKB)) != MAL_SUCCEED) {
			BBPunfix(aBAT->batCacheid);
			BBPunfix(bBAT->batCacheid);
			BBPunfix(aBATfiltered->batCacheid);
			BBPunfix(bBATfiltered->batCacheid);
			return err;
		}
		if (outBIT) {
			if (BUNappend(aBATfiltered, aWKB, FALSE) != GDK_SUCCEED ||
			    BUNappend(bBATfiltered, bWKB, FALSE) != GDK_SUCCEED) {
				BBPunfix(aBAT->batCacheid);
				BBPunfix(bBAT->batCacheid);
				BBPunfix(aBATfiltered->batCacheid);
				BBPunfix(bBATfiltered->batCacheid);
				throw(MAL, "batgeom.MBRfilter", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
			remainingElements++;
		}
	}

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
		throw(MAL, "batgeom.MBRfilter", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create the new BAT
	if ((BATfiltered = COLnew(BAToriginal->hseqbase, ATOMindex("wkb"), BATcount(BAToriginal), TRANSIENT)) == NULL) {
		BBPunfix(BAToriginal->batCacheid);
		throw(MAL, "batgeom.MBRfilter", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	//iterator over the BAT
	BAToriginal_iter = bat_iterator(BAToriginal);

	//create the MBR of the geom
	if ((err = wkbMBR(&geomMBR, geomWKB)) != MAL_SUCCEED) {
		BBPunfix(BAToriginal->batCacheid);
		BBPunfix(BATfiltered->batCacheid);
		return err;
	}

	for (i = 0; i < BATcount(BAToriginal); i++) {
		str err = NULL;
		mbr *MBRoriginal;
		bit outBIT = 0;

		WKBoriginal = (wkb *) BUNtail(BAToriginal_iter, i);

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
			if (BUNappend(BATfiltered, WKBoriginal, FALSE) != GDK_SUCCEED) {
				BBPunfix(BAToriginal->batCacheid);
				BBPunfix(BATfiltered->batCacheid);
				GDKfree(geomMBR);
				GDKfree(MBRoriginal);
				throw(MAL, "batgeom.MBRfilter", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
			remainingElements++;
		}

		GDKfree(MBRoriginal);
	}

	GDKfree(geomMBR);
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
		throw(MAL, "batgeom.mbr", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("mbr"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.mbr", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		if (BUNappend(outBAT, outMBR, FALSE) != GDK_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			GDKfree(outMBR);
			throw(MAL, "batgeom.mbr", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		GDKfree(outMBR);
		outMBR = NULL;
	}

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
	aBAT = BATdescriptor(*aBAT_id);
	bBAT = BATdescriptor(*bBAT_id);
	if (aBAT == NULL || bBAT == NULL) {
		if (aBAT)
			BBPunfix(aBAT->batCacheid);
		if (bBAT)
			BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.MakeLine", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	//check if the BATs are aligned
	if (aBAT->hseqbase != bBAT->hseqbase || BATcount(aBAT) != BATcount(bBAT)) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.MakeLine", SQLSTATE(38000) "Columns must be aligned");
	}
	//create a new BAT for the output
	if ((outBAT = COLnew(aBAT->hseqbase, ATOMindex("wkb"), BATcount(aBAT), TRANSIENT)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.MakeLine", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = 0; i < BATcount(aBAT); i++) {
		str err = NULL;
		wkb *aWKB = NULL, *bWKB = NULL, *outWKB = NULL;

		aWKB = (wkb *) BUNtail(aBAT_iter, i);
		bWKB = (wkb *) BUNtail(bBAT_iter, i);

		if ((err = wkbMakeLine(&outWKB, &aWKB, &bWKB)) != MAL_SUCCEED) {	//check
			BBPunfix(outBAT->batCacheid);
			BBPunfix(aBAT->batCacheid);
			BBPunfix(bBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, outWKB, FALSE) != GDK_SUCCEED) {
			BBPunfix(outBAT->batCacheid);
			BBPunfix(aBAT->batCacheid);
			BBPunfix(bBAT->batCacheid);
			GDKfree(outWKB);
			throw(MAL, "batgeom.MakeLine", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
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
	aBAT = BATdescriptor(*aBAT_id);
	bBAT = BATdescriptor(*bBAT_id);
	if (aBAT == NULL || bBAT == NULL) {
		if (aBAT)
			BBPunfix(aBAT->batCacheid);
		if (bBAT)
			BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.Union", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	//check if the BATs are aligned
	if (aBAT->hseqbase != bBAT->hseqbase || BATcount(aBAT) != BATcount(bBAT)) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.Union", SQLSTATE(38000) "Columns must be aligned");
	}
	//create a new BAT for the output
	if ((outBAT = COLnew(aBAT->hseqbase, ATOMindex("wkb"), BATcount(aBAT), TRANSIENT)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.Union", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = 0; i < BATcount(aBAT); i++) {
		str err = NULL;
		wkb *aWKB = NULL, *bWKB = NULL, *outWKB = NULL;

		aWKB = (wkb *) BUNtail(aBAT_iter, i);
		bWKB = (wkb *) BUNtail(bBAT_iter, i);

		if ((err = wkbUnion(&outWKB, &aWKB, &bWKB)) != MAL_SUCCEED) {	//check
			BBPunfix(outBAT->batCacheid);
			BBPunfix(aBAT->batCacheid);
			BBPunfix(bBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, outWKB, FALSE) != GDK_SUCCEED) {
			BBPunfix(outBAT->batCacheid);
			BBPunfix(aBAT->batCacheid);
			BBPunfix(bBAT->batCacheid);
			GDKfree(outWKB);
			throw(MAL, "batgeom.Union", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		GDKfree(outWKB);
		outWKB = NULL;
	}

	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	BBPunfix(aBAT->batCacheid);
	BBPunfix(bBAT->batCacheid);

	return MAL_SUCCEED;
}
