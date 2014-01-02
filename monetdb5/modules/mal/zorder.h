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

#ifndef _ZORDER_H
#define _ZORDER_H


#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define zorder_export extern __declspec(dllimport)
#else
#define zorder_export extern __declspec(dllexport)
#endif
#else
#define zorder_export extern
#endif

zorder_export str ZORDencode_int_oid(oid *z, int *x, int *y);
zorder_export str ZORDbatencode_int_oid(int *z, int *x, int *y);
zorder_export str ZORDdecode_int_oid(int *x, int *y, oid *z);
zorder_export str ZORDdecode_int_oid_x(int *x, oid *z);
zorder_export str ZORDdecode_int_oid_y(int *y, oid *z);
zorder_export str ZORDbatdecode_int_oid(int *x, int *y, int *z);
zorder_export str ZORDbatdecode_int_oid_x(int *x, int *z);
zorder_export str ZORDbatdecode_int_oid_y(int *y, int *z);
zorder_export str ZORDslice_int(int *r, int *xb, int *yb, int *xt, int *yt);

#endif /* _ZORDER_H */
