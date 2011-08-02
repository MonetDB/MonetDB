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

#ifndef _COLOR_H
#define _COLOR_H
typedef unsigned int color;

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define color_export extern __declspec(dllimport)
#else
#define color_export extern __declspec(dllexport)
#endif
#else
#define color_export extern
#endif

color_export str CLRstr(str *val, color *c);
color_export str CLRcolor(color *c, str *val);
color_export str CLRred(int *r, color *c);
color_export str CLRgreen(int *g, color *c);
color_export str CLRblue(int *b, color *c);
color_export str CLRhue(flt *r, color *c);
color_export str CLRsaturation(flt *g, color *c);
color_export str CLRvalue(flt *b, color *c);
color_export str CLRhueInt(int *r, color *c);
color_export str CLRsaturationInt(int *g, color *c);
color_export str CLRvalueInt(int *b, color *c);
color_export str CLRluminance(int *r, color *c);
color_export str CLRcr(int *r, color *c);
color_export str CLRcb(int *g, color *c);
color_export str CLRhsv(color *c, flt *h, flt *s, flt *v);
color_export str CLRrgb(color *rgb, int *r, int *g, int *b);
color_export str CLRycc(color *c, int *y, int *cr, int *cb);
color_export int color_fromstr(char *colorStr, int *len, color **c);
color_export int color_tostr(char **colorStr, int *len, color *c);

#define color_nil ((color)int_nil)

#endif
