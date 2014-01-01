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
#ifndef _BATCOLOR_H_
#define _BATCOLOR_H_

#include <gdk.h>
#include <string.h>
#include <mal.h>
#include <color.h>
#include "mal_exception.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define batcolor_export extern __declspec(dllimport)
#else
#define batcolor_export extern __declspec(dllexport)
#endif
#else
#define batcolor_export extern
#endif

#ifdef HAVE_LANGINFO_H
#include <langinfo.h>
#endif
#ifdef HAVE_ICONV_H
#include <iconv.h>
#endif

batcolor_export str CLRbatColor(int *ret, int *l);
batcolor_export str CLRbatStr(int *ret, int *l);
batcolor_export str CLRbatRed(int *ret, int *l);
batcolor_export str CLRbatGreen(int *ret, int *l);
batcolor_export str CLRbatBlue(int *ret, int *l);
batcolor_export str CLRbatSaturation(int *ret, int *l);
batcolor_export str CLRbatValue(int *ret, int *l);
batcolor_export str CLRbatHue(int *ret, int *l);
batcolor_export str CLRbatHueInt(int *ret, int *l);
batcolor_export str CLRbatSaturationInt(int *ret, int *l);
batcolor_export str CLRbatValueInt(int *ret, int *l);
batcolor_export str CLRbatLuminance(int *ret, int *l);
batcolor_export str CLRbatCr(int *ret, int *l);
batcolor_export str CLRbatCb(int *ret, int *l);

batcolor_export str CLRbatHsv(int *ret, int *l, int *bid2, int *bid3);
batcolor_export str CLRbatRgb(int *ret, int *l, int *bid2, int *bid3);
batcolor_export str CLRbatycc(int *ret, int *l, int *bid2, int *bid3);
#endif 
