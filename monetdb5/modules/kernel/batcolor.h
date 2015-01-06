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
 * Copyright August 2008-2015 MonetDB B.V.
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

batcolor_export str CLRbatColor(bat *ret, const bat *l);
batcolor_export str CLRbatStr(bat *ret, const bat *l);
batcolor_export str CLRbatRed(bat *ret, const bat *l);
batcolor_export str CLRbatGreen(bat *ret, const bat *l);
batcolor_export str CLRbatBlue(bat *ret, const bat *l);
batcolor_export str CLRbatSaturation(bat *ret, const bat *l);
batcolor_export str CLRbatValue(bat *ret, const bat *l);
batcolor_export str CLRbatHue(bat *ret, const bat *l);
batcolor_export str CLRbatHueInt(bat *ret, const bat *l);
batcolor_export str CLRbatSaturationInt(bat *ret, const bat *l);
batcolor_export str CLRbatValueInt(bat *ret, const bat *l);
batcolor_export str CLRbatLuminance(bat *ret, const bat *l);
batcolor_export str CLRbatCr(bat *ret, const bat *l);
batcolor_export str CLRbatCb(bat *ret, const bat *l);

batcolor_export str CLRbatHsv(bat *ret, const bat *l, const bat *bid2, const bat *bid3);
batcolor_export str CLRbatRgb(bat *ret, const bat *l, const bat *bid2, const bat *bid3);
batcolor_export str CLRbatycc(bat *ret, const bat *l, const bat *bid2, const bat *bid3);
#endif 
