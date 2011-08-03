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

/*
 * @+ Implementation
 *
 */
#ifndef _LOCK_H
#define _LOCK_H

#include <gdk.h>

typedef ptr lock;
typedef ptr monet_lock;

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define lock_export extern __declspec(dllimport)
#else
#define lock_export extern __declspec(dllexport)
#endif
#else
#define lock_export extern
#endif

lock_export int lockToStr(char **dst, int *len, ptr *src);
lock_export str LCKcreate(monet_lock *l);
lock_export str LCKset(int *res, monet_lock *l);
lock_export str LCKtry(int *res, monet_lock *l);
lock_export str LCKunset(int *res, monet_lock *l);
lock_export str LCKdestroy(int *res, monet_lock *l);

#endif /* _LOCK_H */

