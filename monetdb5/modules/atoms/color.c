/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * @f color
 * @t The color module
 * @a Alex van Ballegooij
 * @v 1.0
 * @* Introduction
 * The color atom is a simple 32bit (24bit+zeros) encoding of a standard RGB
 * color encoding, consisting of the red component in bits 16-23,
 * the green component in bits 8-15, and the blue component in bits 0-7.
 *
 * The module contains a number of color conversion methods to construct
 * colors in various colorspaces and extract relevant color channels.
 *
 * @enumerate
 * @item rgb
 * = (byte,byte,byte) colorspace r,g,b=[0..255]
 * @item hsv
 * = (flt,flt,flt) colorspace h=[0..360], s,v=[0..1]
 * @item ycc
 * = (byte,byte,byte) colorspace y,c,c=[0..255]
 * @end enumerate
 *
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_exception.h"
#include <math.h>
#include "color.h"

/*
 * @- Atom commands
 */

static int
CLRhextoint(char h, char l)
{
	int r = 0;

	if (h >= '0' && h <= '9')
		r = 16 * (int) (h - '0');
	if (h >= 'a' && h <= 'f')
		r = 16 * (int) (10 + h - 'a');
	if (h >= 'A' && h <= 'F')
		r = 16 * (int) (10 + h - 'A');
	if (l >= '0' && l <= '9')
		r += (int) (l - '0');
	if (l >= 'a' && l <= 'f')
		r += (int) (10 + l - 'a');
	if (l >= 'A' && l <= 'F')
		r += (int) (10 + l - 'A');
	return r;
}

int
color_fromstr(char *colorStr, int *len, color **c)
{
	char *p = colorStr;

	if (*len < (int) sizeof(color) || *c == NULL) {
		GDKfree(*c);
		*c = GDKmalloc(sizeof(color));
		if( *c == NULL)
			return 0;
		*len = sizeof(color);
	}

	while (GDKisspace(*p))
		p++;
	if (p[0] == 'n' && p[1] == 'i' && p[2] == 'l') {
		color **sc = (color **) c;

		**sc = color_nil;
		p += 3;
	} else {
		if (p[0] == '0' && p[1] == 'x' && p[2] == '0' && p[3] == '0') {
			int r = CLRhextoint(p[4], p[5]);
			int g = CLRhextoint(p[6], p[7]);
			int b = CLRhextoint(p[8], p[9]);

			**c = (color) (r << 16 | g << 8 | b);
		} else
			**c = color_nil;
	}
	return (int) (p - colorStr);
}

int
color_tostr(char **colorStr, int *len, color *c)
{
	color sc = *c;

	/* allocate and fill a new string */

	if (*len < 11 || *colorStr == NULL) {
		GDKfree(*colorStr);
		*colorStr = GDKmalloc(11);
		if( *colorStr == NULL)
			return 0;
		*len = 11;
	}

	if (sc == color_nil) {
		strcpy(*colorStr, "nil");
		return 3;
	}
	snprintf(*colorStr, *len, "0x%08X", (unsigned int) sc);

	return (int) strlen(*colorStr);
}

str
CLRstr(str *s, color *c)
{
	int len = 0;
	str t = 0;

	color_tostr(&t, &len, c);
	*s = (str) t;
	return MAL_SUCCEED;
}

str
CLRrgb(color *rgb, int *r, int *g, int *b)
{
	*rgb = (color) (((*r & 0xFF) << 16) | ((*g & 0xFF) << 8) | (*b & 0xFF));
	return (MAL_SUCCEED);
}

str
CLRred(int *r, color *c)
{
	*r = (int) ((*c >> 16) & 0xFF);
	return (MAL_SUCCEED);
}

str
CLRgreen(int *g, color *c)
{
	*g = (int) ((*c >> 8) & 0xFF);
	return (MAL_SUCCEED);
}

str
CLRblue(int *b, color *c)
{
	*b = (int) (*c & 0xFF);
	return (MAL_SUCCEED);
}

#define max2(a,b) ((a)>(b)?(a):(b))
#define max3(a,b,c) max2(max2(a,b),(c))

#define min2(a,b) ((a)<(b)?(a):(b))
#define min3(a,b,c) min2(min2(a,b),(c))

#define EPS 0.001f

static void
color_rgb2hsv(float *h, float *s, float *v, int R, int G, int B)
{
	register float H, S, V, max;
	register float Rtmp = ((float) (R)) / 255.0f;
	register float Gtmp = ((float) (G)) / 255.0f;
	register float Btmp = ((float) (B)) / 255.0f;

	max = max3(Rtmp, Gtmp, Btmp);
	V = max;
	if (fabs(max) <= EPS) {
		S = 0;
		H = 0;
	} else {
		register float min, delta;

		min = min3(Rtmp, Gtmp, Btmp);
		delta = max - min;
		S = delta / max;
		if (Rtmp == max)
			H = (Gtmp - Btmp) / delta;
		else if (Gtmp == max)
			H = 2 + (Btmp - Rtmp) / delta;
		else		/* Btmp == max */
			H = 4 + (Rtmp - Gtmp) / delta;
		H *= 60;
		if (H < 0)
			H += 360;
	}
	*h = H;
	*s = S;
	*v = V;
}

str
CLRhsv(color *c, flt *h, flt *s, flt *v)
{
	int r, g, b;
	float Rtmp, Gtmp, Btmp;

	if (fabs(*s) <= EPS) {
		Rtmp = Gtmp = Btmp = (*v);
	} else {
		float Htmp = (*h) / 60;
		float f = Htmp - ((int) Htmp);
		float p = (*v) * (1 - (*s));
		float q = (*v) * (1 - (*s) * f);
		float t = (*v) * (1 - (*s) * (1 - f));

		switch ((int) floor(Htmp)) {
		case 0:
			Rtmp = *v;
			Gtmp = t;
			Btmp = p;
			break;
		case 1:
			Rtmp = q;
			Gtmp = *v;
			Btmp = p;
			break;
		case 2:
			Rtmp = p;
			Gtmp = *v;
			Btmp = t;
			break;
		case 3:
			Rtmp = p;
			Gtmp = q;
			Btmp = *v;
			break;
		case 4:
			Rtmp = t;
			Gtmp = p;
			Btmp = *v;
			break;
		default:	/* case 5: */
			Rtmp = *v;
			Gtmp = p;
			Btmp = q;
			break;
		}
	}
	r = (int) ((Rtmp * 255.0f) + 0.5f);
	g = (int) ((Gtmp * 255.0f) + 0.5f);
	b = (int) ((Btmp * 255.0f) + 0.5f);
	return CLRrgb(c, &r, &g, &b);
}

str
CLRhue(flt *f, color *c)
{
	float h, s, v;

	color_rgb2hsv(&h, &s, &v, (*c >> 16) & 0xFF, (*c >> 8) & 0xFF, (*c) & 0xFF);
	*f = h;
	return (MAL_SUCCEED);
}

str
CLRhueInt(int *f, color *c)
{
	float h, s, v;

	color_rgb2hsv(&h, &s, &v, (*c >> 16) & 0xFF, (*c >> 8) & 0xFF, (*c) & 0xFF);
	*f = (int) h;
	return (MAL_SUCCEED);
}

str
CLRsaturation(flt *f, color *c)
{
	float h, s, v;

	color_rgb2hsv(&h, &s, &v, (*c >> 16) & 0xFF, (*c >> 8) & 0xFF, (*c) & 0xFF);
	*f = s;
	return (MAL_SUCCEED);
}

str
CLRsaturationInt(int *f, color *c)
{
	float h, s, v;

	color_rgb2hsv(&h, &s, &v, (*c >> 16) & 0xFF, (*c >> 8) & 0xFF, (*c) & 0xFF);
	*f = (int) s;
	return (MAL_SUCCEED);
}

str
CLRvalue(flt *f, color *c)
{
	float h, s, v;

	color_rgb2hsv(&h, &s, &v, (*c >> 16) & 0xFF, (*c >> 8) & 0xFF, (*c) & 0xFF);
	*f = v;
	return (MAL_SUCCEED);
}

str
CLRvalueInt(int *f, color *c)
{
	float h, s, v;

	color_rgb2hsv(&h, &s, &v, (*c >> 16) & 0xFF, (*c >> 8) & 0xFF, (*c) & 0xFF);
	*f = (int) v;
	return (MAL_SUCCEED);
}


#ifndef CLIP
#define CLIP(X)     ((unsigned char)(((X)&~0xFF)?(((X)<0x00)?0x00:0xFF):(X)))
#endif

str
CLRycc(color *c, int *y, int *cr, int *cb)
{
	int r, g, b;
	float Y = (float) *y;
	float CR = (float) (*cr - 128);
	float CB = (float) (*cb - 128);

	r = (int) (Y + CR * 1.4022f);
	r = CLIP(r);
	g = (int) (Y - CB * 0.3456f - CR * 0.7145f);
	g = CLIP(g);
	b = (int) (Y + CB * 1.7710f);
	b = CLIP(b);
	return CLRrgb(c, &r, &g, &b);
}

str
CLRluminance(int *y, color *c)
{
	int r = (int) ((*c >> 16) & 0xFF);
	int g = (int) ((*c >> 8) & 0xFF);
	int b = (int) (*c & 0xFF);

	*y = (int) (0.2989f * (float) (r) + 0.5866f * (float) (g) + 0.1145f * (float) (b));
	*y = CLIP(*y);
	return (MAL_SUCCEED);
}

str
CLRcr(int *cr, color *c)
{
	int r = (int) ((*c >> 16) & 0xFF);
	int g = (int) ((*c >> 8) & 0xFF);
	int b = (int) (*c & 0xFF);

	*cr = (int) (0.5000f * (float) (r) - 0.4183f * (float) (g) - 0.0816f * (float) (b)) + 128;
	return (MAL_SUCCEED);
}

str
CLRcb(int *cb, color *c)
{
	int r = (int) ((*c >> 16) & 0xFF);
	int g = (int) ((*c >> 8) & 0xFF);
	int b = (int) (*c & 0xFF);

	*cb = (int) (-0.1687f * (float) (r) - 0.3312f * (float) (g) + 0.5000f * (float) (b)) + 128;
	return (MAL_SUCCEED);
}

str
CLRcolor(color *c, str *val)
{
	int len = (int) strlen(*val);

	color_fromstr(*val, &len, &c);
	return MAL_SUCCEED;
}

