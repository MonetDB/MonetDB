#include "geom.h"
#include "geom_io.h"
#include "wkb.h"

/* Input functions (from type to geom) */
/* From text */

/* Creates WKB representation (including srid) from WKT representation */
/* return number of parsed characters. */
str
wkbFROMSTR_withSRID(const char *geomWKT, size_t *len, wkb **geomWKB, int srid, size_t *nread)
{
	GEOSGeom geosGeometry = NULL;	/* The geometry object that is parsed from the src string. */
	GEOSWKTReader *WKT_reader;
	const char *polyhedralSurface = "POLYHEDRALSURFACE";
	const char *multiPolygon = "MULTIPOLYGON";
	char *geomWKT_new = NULL;
	size_t parsedCharacters = 0;

	*nread = 0;

	/* we always allocate new memory */
	GDKfree(*geomWKB);
	*len = 0;
	*geomWKB = NULL;

	if (strNil(geomWKT)) {
		*geomWKB = wkbNULLcopy();
		if (*geomWKB == NULL)
			throw(MAL, "wkb.FromText", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		*len = sizeof(wkb_nil);
		return MAL_SUCCEED;
	}
	//check whether the representation is binary (hex)
	if (geomWKT[0] == '0') {
		str ret = wkbFromBinary(geomWKB, &geomWKT);

		if (ret != MAL_SUCCEED)
			return ret;
		*nread = strlen(geomWKT);
		*len = (size_t) wkb_size((*geomWKB)->len);
		return MAL_SUCCEED;
	}
	//check whether the geometry type is polyhedral surface
	//geos cannot handle this type of geometry but since it is
	//a special type of multipolygon I just change the type before
	//continuing. Of course this means that isValid for example does
	//not work correctly.
	if (strncasecmp(geomWKT, polyhedralSurface, strlen(polyhedralSurface)) == 0) {
		size_t sizeOfInfo = strlen(geomWKT) - strlen(polyhedralSurface) + strlen(multiPolygon) + 1;
		geomWKT_new = GDKmalloc(sizeOfInfo);
		if (geomWKT_new == NULL)
			throw(MAL, "wkb.FromText", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		snprintf(geomWKT_new, sizeOfInfo, "%s%s", multiPolygon, geomWKT + strlen(polyhedralSurface));
		geomWKT = geomWKT_new;
	}
	////////////////////////// UP TO HERE ///////////////////////////

	WKT_reader = GEOSWKTReader_create();
	if (WKT_reader == NULL) {
		if (geomWKT_new)
			GDKfree(geomWKT_new);
		throw(MAL, "wkb.FromText", SQLSTATE(38000) "Geos operation GEOSWKTReader_create failed");
	}
	geosGeometry = GEOSWKTReader_read(WKT_reader, geomWKT);
	GEOSWKTReader_destroy(WKT_reader);

	if (geosGeometry == NULL) {
		if (geomWKT_new)
			GDKfree(geomWKT_new);
		throw(MAL, "wkb.FromText", SQLSTATE(38000) "Geos operation GEOSWKTReader_read failed");
	}

	if (GEOSGeomTypeId(geosGeometry) == -1) {
		if (geomWKT_new)
			GDKfree(geomWKT_new);
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "wkb.FromText", SQLSTATE(38000) "Geos operation GEOSGeomTypeId failed");
	}

	GEOSSetSRID(geosGeometry, srid);
	/* the srid was lost with the transformation of the GEOSGeom to wkb
	 * so we decided to store it in the wkb */

	/* we have a GEOSGeometry with number of coordinates and SRID and we
	 * want to get the wkb out of it */
	*geomWKB = geos2wkb(geosGeometry);
	GEOSGeom_destroy(geosGeometry);
	if (*geomWKB == NULL) {
		if (geomWKT_new)
			GDKfree(geomWKT_new);
		throw(MAL, "wkb.FromText", SQLSTATE(38000) "Geos operation geos2wkb failed");
	}

	*len = (size_t) wkb_size((*geomWKB)->len);
	parsedCharacters = strlen(geomWKT);
	assert(parsedCharacters <= GDK_int_max);

	GDKfree(geomWKT_new);

	*nread = parsedCharacters;
	return MAL_SUCCEED;
}

/* creates a wkb from the given textual representation */
/* int* tpe is needed to verify that the type of the FromText function used is the
 * same with the type of the geometry created from the wkt representation */
str
wkbFromText(wkb **geomWKB, str *geomWKT, int *srid, int *tpe)
{
	size_t len = 0;
	int te = 0;
	str err;
	size_t parsedBytes;

	*geomWKB = NULL;
	if (strNil(*geomWKT) || is_int_nil(*srid) || is_int_nil(*tpe)) {
		if ((*geomWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "wkb.FromText", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	err = wkbFROMSTR_withSRID(*geomWKT, &len, geomWKB, *srid, &parsedBytes);
	if (err != MAL_SUCCEED)
		return err;

	if (is_wkb_nil(*geomWKB) || *tpe == 0 ||
	    *tpe == wkbGeometryCollection_mdb ||
	    ((te = *((*geomWKB)->data + 1) & 0x0f) + (*tpe > 2)) == *tpe) {
		return MAL_SUCCEED;
	}

	GDKfree(*geomWKB);
	*geomWKB = NULL;

	te += (te > 2);
	if (*tpe > 0 && te != *tpe)
		throw(SQL, "wkb.FromText", SQLSTATE(38000) "Geometry not type '%d: %s' but '%d: %s' instead", *tpe, geom_type2str(*tpe, 0), te, geom_type2str(te, 0));
	throw(MAL, "wkb.FromText", SQLSTATE(38000) "%s", "cannot parse string");
}

/* From Binary */
static int
decit(char hex)
{
	switch (hex) {
	case '0':
		return 0;
	case '1':
		return 1;
	case '2':
		return 2;
	case '3':
		return 3;
	case '4':
		return 4;
	case '5':
		return 5;
	case '6':
		return 6;
	case '7':
		return 7;
	case '8':
		return 8;
	case '9':
		return 9;
	case 'A':
	case 'a':
		return 10;
	case 'B':
	case 'b':
		return 11;
	case 'C':
	case 'c':
		return 12;
	case 'D':
	case 'd':
		return 13;
	case 'E':
	case 'e':
		return 14;
	case 'F':
	case 'f':
		return 15;
	default:
		return -1;
	}
}

str
wkbFromBinary(wkb **geomWKB, const char **inStr)
{
	size_t strLength, wkbLength, i;
	wkb *w;

	if (strNil(*inStr)) {
		if ((*geomWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.FromBinary", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	strLength = strlen(*inStr);
	if (strLength & 1)
		throw(MAL, "geom.FromBinary", SQLSTATE(38000) "Geos odd length input string");

	wkbLength = strLength / 2;
	assert(wkbLength <= GDK_int_max);

	w = GDKmalloc(wkb_size(wkbLength));
	if (w == NULL)
		throw(MAL, "geom.FromBinary", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	//compute the value for s
	for (i = 0; i < strLength; i += 2) {
		int firstHalf = decit((*inStr)[i]);
		int secondHalf = decit((*inStr)[i + 1]);
		if (firstHalf == -1 || secondHalf == -1) {
			GDKfree(w);
			throw(MAL, "geom.FromBinary", SQLSTATE(38000) "Geos incorrectly formatted input string");
		}
		w->data[i / 2] = (firstHalf << 4) | secondHalf;
	}

	w->len = (int) wkbLength;
	w->srid = 0;
	*geomWKB = w;

	return MAL_SUCCEED;
}

/* Output functions (from geom to type) */
/* AsText */
/* create textual representation of the wkb */
str
wkbAsText(char **txt, wkb **geomWKB, int *withSRID)
{
	size_t len = 0;
	char *wkt = NULL;
	const char *sridTxt = "SRID:";

	if (is_wkb_nil(*geomWKB) || (withSRID && is_int_nil(*withSRID))) {
		if ((*txt = GDKstrdup(str_nil)) == NULL)
			throw(MAL, "geom.AsText", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	if ((*geomWKB)->srid < 0)
		throw(MAL, "geom.AsText", SQLSTATE(38000) "Geod negative SRID");

	if (wkbTOSTR(&wkt, &len, *geomWKB, false) < 0)
		throw(MAL, "geom.AsText", SQLSTATE(38000) "Geos failed to create Text from Well Known Format");

	if (withSRID == NULL || *withSRID == 0) {	//accepting NULL withSRID to make internal use of it easier
		*txt = wkt;
		return MAL_SUCCEED;
	}

	/* 10 for maximum number of digits to represent an INT */
	len = strlen(wkt) + 10 + strlen(sridTxt) + 2;
	*txt = GDKmalloc(len);
	if (*txt == NULL) {
		GDKfree(wkt);
		throw(MAL, "geom.AsText", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	snprintf(*txt, len, "%s%d;%s", sridTxt, (*geomWKB)->srid, wkt);

	GDKfree(wkt);
	return MAL_SUCCEED;
}

/* AsBinary */
//Returns the wkb in a hex representation */
static char hexit[] = "0123456789ABCDEF";

str
wkbAsBinary(char **toStr, wkb **geomWKB)
{
	char *s;
	int i;

	if (is_wkb_nil(*geomWKB)) {
		if ((*toStr = GDKstrdup(str_nil)) == NULL)
			throw(MAL, "geom.AsBinary", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	if ((*toStr = GDKmalloc(1 + (*geomWKB)->len * 2)) == NULL)
		throw(MAL, "geom.AsBinary", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	s = *toStr;
	for (i = 0; i < (*geomWKB)->len; i++) {
		int val = ((*geomWKB)->data[i] >> 4) & 0xf;
		*s++ = hexit[val];
		val = (*geomWKB)->data[i] & 0xf;
		*s++ = hexit[val];
		TRC_DEBUG(GEOM, "%d: First: %c - Second: %c ==> Original %c (%d)\n", i, *(s-2), *(s-1), (*geomWKB)->data[i], (int)((*geomWKB)->data[i]));
	}
	*s = '\0';
	return MAL_SUCCEED;
}
