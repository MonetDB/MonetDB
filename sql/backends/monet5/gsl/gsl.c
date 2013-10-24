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

static str
gsl_chisqprob(double * retval, double chi2, double datapoints) 
{
	if (chi2 < 0)
		throw(MAL, "gsl.chi2prob", "Wrong value for chi2");
	if (datapoints <= 1)
		throw(MAL, "gsl.chi2prob", "Wrong value for datapoints");

	*retval = gsl_cdf_chisq_Q(chi2, datapoints);

	return MAL_SUCCEED;
}

str
GSLchisqProb(double *retval, double * chi2, double * datapoints) 
{
	return gsl_chisqprob(retval, *chi2, *datapoints);
}

#endif /* AGSL */
