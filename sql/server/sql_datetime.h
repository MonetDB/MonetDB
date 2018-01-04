/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _SQL_DATETIME_H_
#define _SQL_DATETIME_H_

#include "sql_mvc.h"
#include "sql_symbol.h"

typedef enum inttype {
	iyear = 1,
	imonth,
	iday,
	ihour,
	imin,
	isec
} itype;

int parse_interval_qualifier(mvc *sql, struct dlist *pers, int *sk, int *ek, int *sp, int *ep);
/* returns 0 for month intervals, 
 *         1 for sec intervals, 
 * 	   in both cases sk/ek contain the start and end qualifiers 
 *         <0 for errors */

lng qualifier2multiplier( int sk );
/* returns the multiplier for the given interval qualifier */

int parse_interval(mvc *sql, lng sign, char *str, int sk, int ek, int sp, int ep, lng *i);
/* returns 0 for month intervals and value in val, 
 *         1 for sec intervals and value in val, 
 *         <0 for errors */

int interval_from_str(char *str, int d, int p, lng *val);
/* returns 0 for month intervals and value in val, 
 *         1 for sec intervals and value in val, 
 *         <0 for errors */

char *datetime_field(itype field);
/* returns the datetime_field string representation */

int inttype2digits( int sk, int ek );
int digits2sk( int digits);
int digits2ek( int digits );

#endif /*_SQL_DATETIME_H_*/

