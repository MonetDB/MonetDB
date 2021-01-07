/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _COLOR_H
#define _COLOR_H
typedef unsigned int color;

extern str CLRstr(str *val, const color *c)
	__attribute__((__visibility__("hidden")));
extern str CLRcolor(color *c, const char **val)
	__attribute__((__visibility__("hidden")));
extern str CLRred(int *r, const color *c)
	__attribute__((__visibility__("hidden")));
extern str CLRgreen(int *g, const color *c)
	__attribute__((__visibility__("hidden")));
extern str CLRblue(int *b, const color *c)
	__attribute__((__visibility__("hidden")));
extern str CLRhue(flt *r, const color *c)
	__attribute__((__visibility__("hidden")));
extern str CLRsaturation(flt *g, const color *c)
	__attribute__((__visibility__("hidden")));
extern str CLRvalue(flt *b, const color *c)
	__attribute__((__visibility__("hidden")));
extern str CLRhueInt(int *r, const color *c)
	__attribute__((__visibility__("hidden")));
extern str CLRsaturationInt(int *g, const color *c)
	__attribute__((__visibility__("hidden")));
extern str CLRvalueInt(int *b, const color *c)
	__attribute__((__visibility__("hidden")));
extern str CLRluminance(int *r, const color *c)
	__attribute__((__visibility__("hidden")));
extern str CLRcr(int *r, const color *c)
	__attribute__((__visibility__("hidden")));
extern str CLRcb(int *g, const color *c)
	__attribute__((__visibility__("hidden")));
extern str CLRhsv(color *c, const flt *h, const flt *s, const flt *v)
	__attribute__((__visibility__("hidden")));
extern str CLRrgb(color *rgb, const int *r, const int *g, const int *b)
	__attribute__((__visibility__("hidden")));
extern str CLRycc(color *c, const int *y, const int *cr, const int *cb)
	__attribute__((__visibility__("hidden")));

#define color_nil ((color)int_nil)
#define is_color_nil(v)		is_int_nil((int) (v))

#endif
