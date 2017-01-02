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

mal_export str CLRstr(str *val, color *c);
mal_export str CLRcolor(color *c, str *val);
mal_export str CLRred(int *r, color *c);
mal_export str CLRgreen(int *g, color *c);
mal_export str CLRblue(int *b, color *c);
mal_export str CLRhue(flt *r, color *c);
mal_export str CLRsaturation(flt *g, color *c);
mal_export str CLRvalue(flt *b, color *c);
mal_export str CLRhueInt(int *r, color *c);
mal_export str CLRsaturationInt(int *g, color *c);
mal_export str CLRvalueInt(int *b, color *c);
mal_export str CLRluminance(int *r, color *c);
mal_export str CLRcr(int *r, color *c);
mal_export str CLRcb(int *g, color *c);
mal_export str CLRhsv(color *c, flt *h, flt *s, flt *v);
mal_export str CLRrgb(color *rgb, int *r, int *g, int *b);
mal_export str CLRycc(color *c, int *y, int *cr, int *cb);
mal_export int color_fromstr(char *colorStr, int *len, color **c);
mal_export int color_tostr(char **colorStr, int *len, color *c);

#define color_nil ((color)int_nil)

#endif
