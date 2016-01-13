/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
status_export str SYSsetmem_maxsize(void *ret, const lng *num);
status_export str SYSgetvm_cursize(lng *num);
status_export str SYSgetvm_maxsize(lng *num);
status_export str SYSsetvm_maxsize(void *ret, const lng *num);
status_export str SYSioStatistics(bat *ret, bat *ret2);
status_export str SYScpuStatistics(bat *ret, bat *ret2);
status_export str SYSmemStatistics(bat *ret, bat *ret2);
status_export str SYSmem_usage(bat *ret, bat *ret2, const lng *minsize);
status_export str SYSvm_usage(bat *ret, bat *ret2, const lng *minsize);
status_export str SYSgdkEnv(bat *ret, bat *ret2);
status_export str SYSgdkThread(bat *ret, bat *ret2);

#endif
