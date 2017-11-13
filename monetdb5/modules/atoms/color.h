/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _COLOR_H
#define _COLOR_H
typedef unsigned int color;

mal_export str CLRstr(str *val, const color *c);
mal_export str CLRcolor(color *c, const char **val);
mal_export str CLRred(int *r, const color *c);
mal_export str CLRgreen(int *g, const color *c);
mal_export str CLRblue(int *b, const color *c);
mal_export str CLRhue(flt *r, const color *c);
mal_export str CLRsaturation(flt *g, const color *c);
mal_export str CLRvalue(flt *b, const color *c);
mal_export str CLRhueInt(int *r, const color *c);
mal_export str CLRsaturationInt(int *g, const color *c);
mal_export str CLRvalueInt(int *b, const color *c);
mal_export str CLRluminance(int *r, const color *c);
mal_export str CLRcr(int *r, const color *c);
mal_export str CLRcb(int *g, const color *c);
mal_export str CLRhsv(color *c, const flt *h, const flt *s, const flt *v);
mal_export str CLRrgb(color *rgb, const int *r, const int *g, const int *b);
mal_export str CLRycc(color *c, const int *y, const int *cr, const int *cb);
mal_export ssize_t color_fromstr(const char *colorStr, size_t *len, color **c);
mal_export ssize_t color_tostr(char **colorStr, size_t *len, const color *c);

#define color_nil ((color)int_nil)
#define is_color_nil(v)		is_int_nil((int) (v))

#endif
