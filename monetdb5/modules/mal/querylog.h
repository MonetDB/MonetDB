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
#ifndef _QLOG_H
#define _QLOG_H
#include "mal.h"
#include "mal_interpreter.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define qlog_export extern __declspec(dllimport)
#else
#define qlog_export extern __declspec(dllexport)
#endif
#else
#define qlog_export extern
#endif

qlog_export int initQlog(void);
qlog_export void QLOGcatalog(BAT **r);
qlog_export void QLOGcalls(BAT **r);
qlog_export str QLOGinit(int *ret);
qlog_export str QLOGinitThreshold(int *ret, int *threshold);
qlog_export str QLOGdone(int *ret);
qlog_export int QLOGisset(void);
qlog_export str QLOGissetFcn(int *ret);
qlog_export str QLOGreset(int *ret);
qlog_export str QLOGdefine(oid *ret, oid *idx, str *q, str *pipe, lng *optimize, str  *usr, lng *tick, int *mal);
qlog_export str QLOGcall(int *ret, oid *idx, lng *tick1, lng *tick2, str *arg, wrd *tuples, lng *xtime, lng *rtime, int *cpu, int *iowait, lng *space);

#endif /* _QLOG_H */
