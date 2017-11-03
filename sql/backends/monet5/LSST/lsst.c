/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "lsst.h"

/* The remainder of the code base is taken over from MySQLSpatialUdf.c */

static double const QSERV_DEG_PER_RAD = 180.0 / M_PI;
static double const QSERV_RAD_PER_DEG = M_PI / 180.0;
static double const QSERV_ARCSEC_PER_DEG = 3600.0;


/* -- Angular separation -------- */

/** Returns D^2/4, where D is the euclidian distance between the two
	input points on the unit sphere. */
static double _qserv_dist(double ra1, double dec1, double ra2, double dec2) {
	double x, y, z, dist;
	x = sin((ra1 - ra2) * QSERV_RAD_PER_DEG * 0.5);
	x *= x;
	y = sin((dec1 - dec2) * QSERV_RAD_PER_DEG * 0.5);
	y *= y;
	z = cos((dec1 + dec2) * QSERV_RAD_PER_DEG * 0.5);
	z *= z;
	dist = x * (z - y) + y;
	return dist < 0.0 ? 0.0 : (dist > 1.0 ? 1.0 : dist);
}

static double _qserv_angSep(double ra1, double dec1, double ra2, double dec2) {
	double dist;
	dist = _qserv_dist(ra1, dec1, ra2, dec2);
	return 2.0 * QSERV_DEG_PER_RAD * asin(sqrt(dist));
}

/** Returns the angular separation in degrees between two spherical
  * coordinate pairs (ra1, dec1) and (ra2, dec2).
  *
  * Consumes 4 arguments ra1, dec1, ra2 and dec2 all of type REAL:
  * @li ra1:  right ascension of the first position (deg)
  * @li dec1: declination of the first position (deg)
  * @li ra2:  right ascension of the second position (deg)
  * @li dec2: declination of the second position (deg)
  *
  * Also:
  * @li If any parameter is NULL, NULL is returned.
  * @li If dec1 or dec2 lies outside of [-90, 90], this is an error
  *     and NULL is returned.
  */
str 
qserv_angSep(dbl *sep, dbl *ra1, dbl *dec1, dbl *ra2, dbl *dec2)
{
	/* If any input is null, the result is null. */
	if ( is_dbl_nil(*ra1) || is_dbl_nil(*dec1) || is_dbl_nil(*ra2) || is_dbl_nil(*dec2)){
		*sep = dbl_nil;
		return MAL_SUCCEED;
	}
	
	/* Check that dec lies in range. */
	if (*dec1 < -90.0 || *dec1 > 90.0 || *dec2 < -90.0 || *dec2 > 90.0)
		throw(MAL,"lsst.qserv_angSep", "Illegal angulars");

	*sep = _qserv_angSep(*ra1, *dec1, *ra2, *dec2);
	return MAL_SUCCEED;
}


/* -- Point in spherical box test -------- */

/** Range reduces the given angle to lie in the range [0.0, 360.0). */
static double _qserv_reduceRa(double theta) {
	if (theta  < 0.0 || theta >= 360.0) {
		theta = fmod(theta, 360.0);
		if (theta  < 0.0) {
			theta += 360.0;
		}
	}
	return theta;
}

/** Returns 1 if the given spherical longitude/latitude box contains
  * the given position, and 0 otherwise.
  *
  * Consumes 6 arguments ra, dec, ra_min, dec_min, ra_max and dec_max, in
  * that order, all of type REAL and in units of degrees. (ra, dec) is the
  * position to test - the remaining parameters specify the spherical box.
  *
  * Note that:
  * @li If any parameter is NULL, the return value is 0.
  * @li If dec, dec_min or dec_max lies outside of [-90, 90],
  *     this is an error and NULL is returned.
  * @li If dec_min is greater than dec_max, the spherical box is empty
  *     and 0 is returned.
  * @li If both ra_min and ra_max lie in the range [0, 360], then ra_max
  *     can be less than ra_min. For example, a box with ra_min = 350
  *     and ra_max = 10 includes points with right ascensions in the ranges
  *     [350, 360) and [0, 10].
  * @li If either ra_min or ra_max lies outside of [0, 360], then ra_min
  *     must be = ra_max (otherwise, NULL is returned), though the values
  *     can be arbitrary. If the two are separated by 360 degrees or more,
  *     then the box spans [0, 360). Otherwise, both values are range reduced.
  *     For example, a spherical box with ra_min = 350 and ra_max = 370
  *     includes points with right ascensions in the rnages [350, 360) and
  *     [0, 10].
  */
str qserv_ptInSphBox(int *ret, dbl *ra, dbl *dec, dbl *ra_min, dbl *dec_min, dbl *ra_max, dbl *dec_max)
{
	dbl lra, lra_min, lra_max;

	if (is_dbl_nil(*ra) || is_dbl_nil(*dec) || is_dbl_nil(*ra_min) || is_dbl_nil(*dec_min) || is_dbl_nil(*ra_max) || is_dbl_nil(*dec_max)){
		*ret = int_nil;
		return MAL_SUCCEED;
	}
	/* Check arguments. */
	if (*dec < -90.0 || *dec_min < -90.0 || *dec_max < -90.0 ||
		*dec > 90.0 || *dec_min > 90.0 || *dec_max > 90.0) {
		*ret = int_nil;
		return MAL_SUCCEED;
	}
	if ( *ra_max  <  *ra_min && ( *ra_max < 0.0 ||  *ra_min > 360.0)) {
		*ret = int_nil;
		return MAL_SUCCEED;
	}
	if ( *dec_min >  *dec_max ||  *dec <  *dec_min ||  *dec >  *dec_max) {
		*ret = 0;
		return MAL_SUCCEED;
	}
	/* Range-reduce longitude angles */
	lra = _qserv_reduceRa(*ra);
	if ( *ra_max - *ra_min >= 360.0) {
		lra_min = 0.0;
		lra_max = 360.0;
	} else {
		lra_min = _qserv_reduceRa(*ra_min);
		lra_max = _qserv_reduceRa(*ra_max);
	}
	if (lra_min <= lra_max)
		*ret = lra >= lra_min && lra <= lra_max;
	else 
		*ret = lra >= lra_min || lra <= lra_max;
	return MAL_SUCCEED;
}


/* -- Point in spherical circle test -------- */

/** Returns 1 if the given circle on the unit sphere contains
  * the specified position and 0 otherwise.
  *
  * Consumes 5 arguments, all of type REAL:
  * @li ra:       right ascension of position to test (deg)
  * @li dec:      declination of position to test (deg)
  * @li ra_cen:   right ascension of circle center (deg)
  * @li dec_cen:  declination of circle center (deg)
  * @li radius:   radius (opening angle) of circle (deg)
  *
  * Note that:
  * @li If any parameter is NULL, the return value is 0.
  * @li If dec or dec_cen lies outside of [-90, 90],
  *     this is an error and NULL is returned.
  * @li If radius is negative or greater than 180, this is
  *     an error and NULL is returned.
  */
str
qserv_ptInSphCircle(int *ret, dbl *ra, dbl *dec, dbl *ra_cen, dbl *dec_cen, dbl *radius)
{
	if (is_dbl_nil(*ra) || is_dbl_nil(*dec) || is_dbl_nil(*ra_cen) || is_dbl_nil(*dec_cen) || is_dbl_nil(*radius) ){
		*ret = int_nil;
		return MAL_SUCCEED;
	}
	/* Check arguments */
	if ( *dec < -90.0 || *dec > 90.0 || *dec_cen < -90.0 || *dec_cen > 90.0) {
		*ret = int_nil;
		return MAL_SUCCEED;
	}
	if (*radius < 0.0 || *radius > 180.0) {
		*ret = int_nil;
		return MAL_SUCCEED;
	}
	/* Fail-fast if declination delta exceeds the radius. */
	if ( fabs(*dec - *dec_cen) > *radius) {
		*ret = 0;
		return MAL_SUCCEED;
	}
	*ret = _qserv_angSep(*ra, *dec, *ra_cen, *dec_cen) <= *radius;
	return MAL_SUCCEED;
}


/* -- Point in spherical ellipse test -------- */

typedef struct {
	double sinRa;     /** sine of ellipse center longitude angle */
	double cosRa;     /** cosine of ellipse center longitude angle */
	double sinDec;    /** sine of ellipse center latitude angle */
	double cosDec;    /** cosine of ellipse center latitude angle */
	double sinAng;    /** sine of ellipse position angle */
	double cosAng;    /** cosine of ellipse position angle */
	double invMinor2; /** 1/(m*m); m = semi-minor axis length (rad) */
	double invMajor2; /** 1/(M*M); M = semi-major axis length (rad) */
	int valid;
} _qserv_sphEllipse_t;

/** Returns 1 if the given ellipse on the unit sphere contains
  * the specified position and 0 otherwise.
  *
  * Consumes 7 arguments, all of type REAL:
  * @li ra:       right ascension of position to test (deg)
  * @li dec:      declination of position to test (deg)
  * @li ra_cen:   right ascension of ellipse center (deg)
  * @li dec_cen:  declination of ellipse center (deg)
  * @li smaa:     semi-major axis length (arcsec)
  * @li smia:     semi-minor axis length (arcsec)
  * @li ang:      ellipse position angle (deg)
  *
  * Note that:
  * @li If any parameter is NULL, the return value is 0.
  * @li If dec or dec_cen lies outside of [-90, 90],
  *     this is an error and NULL is returned.
  * @li If smia is negative or greater than smaa, this is
  *     an error and NULL is returned.
  * @li If smaa is greater than 36000 arcsec (10 deg), this
  *     is an error and NULL is returned.
  */
str
qserv_ptInSphEllipse(int *ret, dbl *ra, dbl *dec, dbl *ra_cen, dbl *dec_cen, dbl *smaa, dbl *smia, dbl *ang)
{
	double m, M, lra, ldec,  x, y, z, w, xne, yne;
	double raCen, decCen, angle;

	if (is_dbl_nil(*ra) || is_dbl_nil(*dec) || is_dbl_nil(*ra_cen) || is_dbl_nil(*dec_cen) || is_dbl_nil(*smaa) || is_dbl_nil(*smia) || is_dbl_nil(*ang) ){
		*ret = int_nil;
		return MAL_SUCCEED;
	}
	/* Check arguments */
	if (*dec < -90.0 || *dec > 90.0 || *dec_cen < -90.0 || *dec_cen > 90.0) {
		*ret = int_nil;
		return MAL_SUCCEED;
	}
	/* Semi-minor axis length m and semi-major axis length M must satisfy 0 <= m <= M <= 10 deg */
	m = *smia;
	M = *smaa;
	if (m < 0.0 || m > M || M > 10.0 * QSERV_ARCSEC_PER_DEG) {
		*ret = int_nil;
		return MAL_SUCCEED;
	}
	raCen = *ra_cen * QSERV_RAD_PER_DEG;
	angle = *ang * QSERV_RAD_PER_DEG;
	decCen = *dec_cen *QSERV_RAD_PER_DEG;
	m = m * QSERV_RAD_PER_DEG / QSERV_ARCSEC_PER_DEG;
	M = M * QSERV_RAD_PER_DEG / QSERV_ARCSEC_PER_DEG;

	/* Transform input position from spherical coordinates
	   to a unit cartesian vector. */
	lra = *ra * QSERV_RAD_PER_DEG;
	ldec= *dec * QSERV_RAD_PER_DEG;

	x = cos(lra);
	y = sin(lra);
	z = sin(ldec);
	w = cos(ldec);

	x *= w;
	y *= w;
	/* get coords of input point in (N,E) basis at ellipse center */
	xne = cos(decCen) * z - sin(decCen) * (sin(raCen) * y + cos(raCen) * x);
	yne = cos(raCen)  * y - sin(raCen) * x;
	/* rotate by negated position angle */
	x = sin(raCen) * yne + cos(angle) * xne;
	y = cos(raCen) * yne - sin(angle) * xne;
	/* perform standard 2D axis-aligned point-in-ellipse test */
	*ret = (x * x * (1.0/ (m * m)) + y * y * (1.0 / ( M * M)) <= 1.0);
	return MAL_SUCCEED;
}


/* -- Point in spherical convex polygon test -------- */

typedef struct {
	double *edges;
	int nedges;
} _qserv_sphPoly_t;

static void _qserv_computeEdges( double *edges, double *verts, int nv)
{
	double x, y, z, w, xp, yp, zp, xl, yl, zl;
	int i;
	/* Transform last vertex to a unit 3 vector */
#ifdef QSERV_HAVE_SINCOS
	sincos(verts[nv*2 - 2], &yl, &xl);
	sincos(verts[nv*2 - 1], &zl, &w);
#else
	xl = cos(verts[nv*2 - 2]);
	yl = sin(verts[nv*2 - 2]);
	zl = sin(verts[nv*2 - 1]);
	w = cos(verts[nv*2 - 1]);
#endif
	xl *= w;
	yl *= w;
	xp = xl; yp = yl; zp = zl;
	for (i = 0; i < nv - 1; ++i) {
		/* Transform current vertex to a unit 3 vector */
#ifdef QSERV_HAVE_SINCOS
		sincos(verts[i*2], &y, &x);
		sincos(verts[i*2 + 1], &z, &w);
#else
		x = cos(verts[i*2]);
		y = sin(verts[i*2]);
		z = sin(verts[i*2 + 1]);
		w = cos(verts[i*2 + 1]);
#endif
		x *= w;
		y *= w;
		/* Edge plane equation is the cross product of the 2 vertices */
		edges[i*3] = yp * z - zp * y;
		edges[i*3 + 1] = zp * x - xp * z;
		edges[i*3 + 2] = xp * y - yp * x;
		xp = x; yp = y; zp = z;
	}
	/* Compute last edge plane equation */
	edges[i*3] = yp * zl - zp * yl;
	edges[i*3 + 1] = zp * xl - xp * zl;
	edges[i*3 + 2] = xp * yl - yp * xl;
}

/** Returns 1 if the given spherical convex polygon contains
  * the specified position and 0 otherwise.
  *
  * Consumes 3 arguments ra, dec and poly. The ra and dec parameters
  * must be convertible to a REAL, and poly must be a STRING.
  *
  * @li ra:    right ascension of position to test (deg)
  * @li dec:   declination of position to test (deg)
  * @li poly:  polygon specification
  *
  * Note that:
  * @li If any input parameter is NULL, 0 is returned.
  * @li If dec is outside of [-90,90], this is an error and NULL is returned.
  * @li If the polygon spec is invalid or cannot be parsed (e.g. because the
  *     the server has run out of memory), this is an error and the return
  *     value is NULL.
  *
  * A polygon specification consists of a space separated list of vertex
  * coordinate pairs: "ra_0 dec_0 ra_1 dec_1 .... ra_n dec_n". There must
  * be at least 3 coordinate pairs and declinations must lie in the range
  * [-90,90]. Also, if the following conditions are not satisfied, then the
  * return value of the function is undefined:
  *
  * @li vertices are hemispherical
  * @li vertices form a convex polygon when connected with great circle edges
  * @li vertices lie in counter-clockwise order when viewed from a position
  * @li outside the unit sphere and inside the half-space containing them.
  */
str qserv_ptInSphPoly(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = getArgReference_int(stk,pci,0);
	dbl ra = *getArgReference_dbl(stk,pci,1);
	dbl dec = *getArgReference_dbl(stk,pci,2);

	double x, y, z, w;
	dbl *edges, *nv;
	int i, nedges= (pci->argc-3)/2;

	(void) mb;
	(void) cntxt;
	/* If any input is null, the result is 0. */
	for (i = 1; i <pci->argc; ++i) {
		if ( is_dbl_nil(*getArgReference_dbl(stk,pci,i))){
			*ret = int_nil;
			return MAL_SUCCEED;
		}
	}
	/* Check that dec is in range */
	if (dec < -90.0 || dec > 90.0) {
		*ret = int_nil;
		return MAL_SUCCEED;
	}
	/* Have at least 3 coordinate pairs */
	if ( nedges < 3 )
		throw(MAL,"lsst.ptInSPhPoly","Not enough edges");
	ra *= QSERV_RAD_PER_DEG;
	dec *= QSERV_RAD_PER_DEG;

	/* Parse polygon spec if it isn't constant */
	edges = (dbl*) GDKmalloc( pci->argc -3 * sizeof(dbl));
	if ( edges == NULL)
		throw(MAL,"lsst.ptInSPhPoly", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	nv = (dbl*) GDKmalloc( pci->argc -3 * sizeof(dbl));
	if ( nv == NULL){
		GDKfree(edges);
		throw(MAL,"lsst.ptInSPhPoly", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	for (i = 3; i <pci->argc; ++i) 
		nv[i-3] =  *getArgReference_dbl(stk,pci,i);
	_qserv_computeEdges(edges,nv, nedges);
	GDKfree(nv);

	/* Transform input position from spherical coordinates
	   to a unit cartesian vector. */
	x = cos(ra);
	y = sin(ra);
	z = sin(dec);
	w = cos(dec);
	x *= w;
	y *= w;
	*ret = 1;
	for (i = 0; i < nedges; ++i) {
		/* compute dot product of edge plane normal and input position */
		double dp = x * edges[i*3 + 0] +
					y * edges[i*3 + 1] +
					z * edges[i*3 + 2];
		if (dp < 0.0) {
			*ret = 0;
			break;
		}
	}
	GDKfree(edges);
	return MAL_SUCCEED;
}
/* 
 * the remainder is an example of hooking up a fast cross match operation
 * using two HtmID columns bounded by the htm delta distance.
 * The delta indicates the number of triangulat divisions should be ignored.
 * For delta =0 the pairs match when their HtmID is equal
 * for delta =1 the pairs match if their HtmID shifted 2 bits match and so on.
 * The two columns should be sorted upfront.
*/

static str
LSSTxmatch_intern(bat *lres, bat *rres, bat *lid, bat *rid, int *delta)
{
	BAT *xl, *xr, *bl, *br;
	lng *l, *r;
	lng lhtm, rhtm;
	lng *lend, *rend;
	int shift;
	oid lo, ro;

	if( *delta < 0 || *delta >31)
         	throw(MAL, "algebra.xmatch", "delta not in 0--31");
	shift = 2 * *delta; 

	if( (bl= BATdescriptor(*lid)) == NULL )
		throw(MAL, "algebra.xmatch", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if( !bl->tsorted){
		BBPunfix(*lid);
		throw(MAL, "algebra.xmatch", "sorted input required");
	}

	if( (br= BATdescriptor(*rid)) == NULL ){
		BBPunfix(*lid);
		throw(MAL, "algebra.xmatch", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if( !br->tsorted){
		BBPunfix(*lid);
		BBPunfix(*rid);
		throw(MAL, "algebra.xmatch", "sorted input required");
	}

	l= (lng*) Tloc(bl, 0);
	lend= (lng*) Tloc(bl, BUNlast(bl));
	rend= (lng*) Tloc(br, BUNlast(br));

	xl = COLnew(0, TYPE_oid, MIN(BATcount(bl), BATcount(br)), TRANSIENT);
	if ( xl == NULL){
		BBPunfix(*lid);
		BBPunfix(*rid);
		throw(MAL, "algebra.xmatch", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	xr = COLnew(0, TYPE_oid, MIN(BATcount(bl), BATcount(br)), TRANSIENT);
	if ( xr == NULL){
		BBPunfix(*lid);
		BBPunfix(*rid);
		BBPunfix(xl->batCacheid);
		throw(MAL, "algebra.xmatch", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	for (lo = bl->hseqbase; l < lend; lo++, l++) {
		if (!is_lng_nil(*l)) {
			lhtm = *l >> shift;
			r= (lng*) Tloc(br, 0);
			ro = br->hseqbase;
			for(; r < rend; ro++, r++) {
				if (!is_lng_nil(*r)) {
					rhtm = *r >> shift;
					if (lhtm == rhtm){
						/* match */
						if (BUNappend(xl, &lo, FALSE) != GDK_SUCCEED ||
						    BUNappend(xr, &ro, FALSE) != GDK_SUCCEED) {
							BBPunfix(*lid);
							BBPunfix(*rid);
							BBPunfix(xl->batCacheid);
							BBPunfix(xr->batCacheid);
							throw(MAL, "algebra.xmatch", SQLSTATE(HY001) MAL_MALLOC_FAIL);
						}
					} else if (lhtm < rhtm) {
						lhtm = lhtm << shift;
						while (*l < lhtm && l < lend) {
							lo++;
							l++;
						}
						lhtm = lhtm >> shift;
					} else {
						rhtm = rhtm << shift;
						while (*r < rhtm && r < rend) {
							ro++;
							r++;
						}
						rhtm = rhtm >> shift;
					}
				}
			}
		}
	}
	BBPunfix(*lid);
	BBPunfix(*rid);
	BBPkeepref(*lres = xl->batCacheid);
	BBPkeepref(*rres = xr->batCacheid);
	return MAL_SUCCEED;
}

str
LSSTxmatchjoin(bat *lres, bat *rres, bat *lid, bat *rid, int *delta, bat *sl, bat *sr, bit *nil_matches, lng *estimate)
{
	(void)sl;
	(void)sr;
	(void)nil_matches;
	(void)estimate;
	return LSSTxmatch_intern(lres, rres, lid, rid, delta);
}

str
LSSTxmatch(bit *res, lng *l, lng *r, int *delta)
{
	int shift;

	if (*delta < 0 || *delta > 31)
         	throw(MAL, "lsst.xmatch", "delta not in 0--31");
	shift = 2 * *delta;

	*res = !is_lng_nil(*l) && !is_lng_nil(*r) && (*l >> shift) == (*r >> shift);
	return MAL_SUCCEED;
}

str
LSSTxmatchselect(bat *res, bat *bid, bat *sid, lng *r, int *delta, bit *anti)
{
	int shift;
	BAT *b, *s = NULL, *bn;
	const lng *l;
	lng lhtm, rhtm;

	if (*delta < 0 || *delta > 31)
         	throw(MAL, "lsst.xmatch", "delta not in 0--31");
	shift = 2 * *delta;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.xmatch", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	assert(b->ttype == TYPE_lng);
	if (sid && *sid && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "algebra.xmatch", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((bn = COLnew(0, TYPE_oid, 0, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, "algebra.xmatch", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	if (is_lng_nil(*r)) {
		BBPunfix(b->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		BBPkeepref(*res = bn->batCacheid);
		return MAL_SUCCEED;
	}
	rhtm = *r >> shift;
	l = (const lng *) Tloc(b, 0);
	if (s && !BATtdense(s)) {
		const oid *sval = (const oid *) Tloc(s, 0);
		oid o;
		BUN i, scnt = BATcount(s);

		for (i = 0; i < scnt && sval[i] < b->hseqbase; i++)
			;
		for (; i < scnt; i++) {
			o = sval[i];
			if (o >= b->hseqbase + BATcount(b))
				break;
			lhtm = l[o - b->hseqbase];
			if (!is_lng_nil(lhtm) &&
			    ((lhtm >> shift) == rhtm) != *anti &&
			    BUNappend(bn, &o, FALSE) != GDK_SUCCEED) {
				BBPunfix(b->batCacheid);
				if (s)
					BBPunfix(s->batCacheid);
				BBPunfix(bn->batCacheid);
				throw(MAL, "algebra.xmatch", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
		}
	} else {
		oid o = b->hseqbase;
		oid e = o + BATcount(b);

		if (s) {
			if (s->tseqbase > o)
				o = s->tseqbase;
			if (s->tseqbase + BATcount(s) < e)
				e = s->tseqbase + BATcount(s);
		}
		while (o < e) {
			lhtm = l[o - b->hseqbase];
			if (!is_lng_nil(lhtm) &&
			    ((lhtm >> shift) == rhtm) != *anti &&
			    BUNappend(bn, &o, FALSE) != GDK_SUCCEED) {
				BBPunfix(b->batCacheid);
				if (s)
					BBPunfix(s->batCacheid);
				BBPunfix(bn->batCacheid);
				throw(MAL, "algebra.xmatch", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
			o++;
		}
	}
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	BBPkeepref(*res = bn->batCacheid);
	return MAL_SUCCEED;
}
