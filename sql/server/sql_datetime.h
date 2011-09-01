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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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

