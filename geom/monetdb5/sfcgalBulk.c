/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * Romulo Goncalves
 */

#include "sfcgal.h"

/* triangulates geometry - BULK version*/
str
geom_sfcgal_triangulate2DZ_bat(bat *outBAT_id, bat *inBAT_id, int *flag)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.geom_sfcgal_triangle2DZ", "Problem retrieving BAT");
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.geom_sfcgal_triangle2DZ", "Error creating new BAT");
	}

	//iterator over the BATs
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {
		str err = NULL;
		wkb *outWKB = NULL;

		wkb *inWKB = (wkb *) BUNtail(inBAT_iter, p);

		if ((err = geom_sfcgal_triangulate2DZ(&outWKB, &inWKB, flag)) != MAL_SUCCEED) {
            BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
        if (outWKB) {
            BUNappend(outBAT, outWKB, TRUE);
            GDKfree(outWKB);
            outWKB = NULL;
        }
	}

	BBPunfix(inBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);

	return MAL_SUCCEED;
}
