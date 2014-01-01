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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
*/
#ifndef _SYS_H_
#define _SYS_H_

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define status_export extern __declspec(dllimport)
#else
#define status_export extern __declspec(dllexport)
#endif
#else
#define status_export extern
#endif

status_export str SYSgetmem_cursize(lng *num);
status_export str SYSgetmem_maxsize(lng *num);
status_export str SYSsetmem_maxsize(int *ret, lng *num);
status_export str SYSgetvm_cursize(lng *num);
status_export str SYSgetvm_maxsize(lng *num);
status_export str SYSsetvm_maxsize(lng *num);
status_export str SYSioStatistics(int *ret,int *ret2);
status_export str SYScpuStatistics(int *ret, int *ret2);
status_export str SYSmemStatistics(int *ret, int *ret2);
status_export str SYSmem_usage(int *ret, int *ret2, lng *minsize);
status_export str SYSvm_usage(int *ret, int *ret2, lng *minsize);
status_export str SYSgdkEnv(int *ret, int *ret2);
status_export str SYSgdkThread(int *ret, int *ret2);

#endif
