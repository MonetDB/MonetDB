/*
 * The contents of this file are subject to the MonetDB Public License
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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
*/

/*
 *  A. de Rijke, B. Scheers
 * The gsl module
 * The gsl module contains wrappers for functions in
 * gsl.
 */

#ifndef AGSL
#define AGSL

#include "monetdb_config.h"
#include "mal.h"
#include "mal_exception.h"
#include <gsl/gsl_cdf.h>

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define gsl_export extern __declspec(dllimport)
#else
#define gsl_export extern __declspec(dllexport)
#endif
#else
#define gsl_export extern
#endif

gsl_export str GSLchisqProb(double *retval, double * chi2, double * datapoints);
gsl_export str GSLbat_chisqProb_cst(bat *retval, bat * chi2, double * datapoints);
gsl_export str GSLcst_chisqProb_bat(bat *retval, double * chi2, bat * datapoints);
gsl_export str GSLbat_chisqProb_bat(bat *retval, bat * chi2, bat * datapoints);

static str
gsl_chisqprob(double * retval, double chi2, double datapoints) 
{
	*retval = dbl_nil;
	if ((chi2 == dbl_nil) || (chi2 < 0))
		throw(MAL, "gsl.chi2prob", "Wrong value for chi2");
	if ((datapoints == dbl_nil) || (datapoints < 0))
		throw(MAL, "gsl.chi2prob", "Wrong value for datapoints");
	*retval = gsl_cdf_chisq_Q(chi2, datapoints);
	return MAL_SUCCEED;
}

static str
gsl_bat_chisqprob_cst(bat * retval, bat chi2, double datapoints) 
{
	BAT *b, *bn;
	BATiter bi;
	BUN p,q;
	dbl r;
	char *msg = NULL;

	if (datapoints == dbl_nil) {
		throw(MAL, "GSLbat_chisqprob_cst", "Parameter datapoints should not be nil");
	}
	if (datapoints < 0)
		throw(MAL, "gsl.chi2prob", "Wrong value for datapoints");

	if ((b = BATdescriptor(chi2)) == NULL) {
		throw(MAL, "chisqprob", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	bn = BATnew(b->htype, TYPE_dbl, BATcount(b));
	if (bn == NULL){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "gsl.chisqprob", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	BATloop(b,p,q) {
		dbl d = *(dbl*)BUNtail(bi,p);
		if ((d == dbl_nil) || (d < 0))
			throw(MAL, "gsl.chi2prob", "Wrong value for chi2");
		r = gsl_cdf_chisq_Q(d, datapoints);
		BUNins(bn, BUNhead(bi,p), &r, FALSE);
	}
	*retval = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

static str
gsl_cst_chisqprob_bat(bat * retval, double chi2, bat datapoints) 
{
	BAT *b, *bn;
	BATiter bi;
	BUN p,q;
	dbl r;
	char *msg = NULL;

	if( (b = BATdescriptor(datapoints)) == NULL) {
		throw(MAL, "chisqprob", "Cannot access descriptor");
	}
	if (chi2 == dbl_nil) {
		throw(MAL, "GSLbat_chisqprob_cst", "Parameter chi2 should not be nil");
	}
	if (chi2 < 0)
		throw(MAL, "gsl.chi2prob", "Wrong value for chi2");
	bi = bat_iterator(b);
	bn = BATnew(b->htype, TYPE_dbl, BATcount(b));
	if( bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "gsl.chisqprob", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	BATloop(b,p,q) {
		dbl datapoints = *(dbl*)BUNtail(bi,p);

		if ((datapoints == dbl_nil) || (datapoints < 0))
			throw(MAL, "gsl.chi2prob", "Wrong value for datapoints");
		r = gsl_cdf_chisq_Q(chi2, datapoints);
		BUNins(bn, BUNhead(bi,p), &r, FALSE);
	}
	BBPkeepref( *retval = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

static str
gsl_bat_chisqprob_bat(bat * retval, bat chi2, bat datapoints) 
{
	BAT *b, *c, *bn;
	dbl r, *chi2p, *datapointsp;
	char *msg = NULL;
	size_t cnt = 0, i;

	if( (b = BATdescriptor(chi2)) == NULL) {
		throw(MAL, "chisqprob", "Cannot access descriptor chi2");
	}
	if( (c = BATdescriptor(datapoints)) == NULL) {
		throw(MAL, "chisqprob", "Cannot access descriptor datapoints");
	}
	assert(b->htype == TYPE_void);
	bn = BATnew(TYPE_void, TYPE_dbl, cnt = BATcount(b));
	if( bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "gsl.chisqprob", MAL_MALLOC_FAIL);
	}
	chi2p = (dbl*)Tloc(b, 0);
	datapointsp = (dbl*)Tloc(c, 0);
	BATseqbase(bn, b->hseqbase);
	for(i = 0; i<cnt; i++) { 
		if ((chi2p[i] == dbl_nil) || (chi2p[i] < 0))
			throw(MAL, "gsl.chi2prob", "Wrong value for chi2");
		if ((datapointsp[i] == dbl_nil) || (datapointsp[i] < 0))
			throw(MAL, "gsl.chi2prob", "Wrong value for datapoints");
		r = gsl_cdf_chisq_Q(chi2p[i], datapointsp[i]);
		BUNappend(bn, &r, FALSE);
	}
	BBPkeepref( *retval = bn->batCacheid);
	BBPunfix(b->batCacheid);
	BBPunfix(c->batCacheid);
	return msg;
}

str
GSLchisqProb(double *retval, double * chi2, double * datapoints) 
{
	return gsl_chisqprob(retval, *chi2, *datapoints);
}

str
GSLbat_chisqProb_cst(bat *retval, bat * chi2, double * datapoints) 
{
	return gsl_bat_chisqprob_cst(retval, *chi2, *datapoints);
}

str
GSLcst_chisqProb_bat(bat *retval, double * chi2, bat * datapoints) 
{
	return gsl_cst_chisqprob_bat(retval, *chi2, *datapoints);
}

str
GSLbat_chisqProb_bat(bat *retval, bat * chi2, bat * datapoints) 
{
	return gsl_bat_chisqprob_bat(retval, *chi2, *datapoints);
}

#endif /* AGSL */
