/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _BATCOLOR_H_
#define _BATCOLOR_H_

#include <gdk.h>
#include <string.h>
#include <mal.h>
#include <color.h>
#include "mal_exception.h"

mal_export str CLRbatColor(bat *ret, const bat *l);
mal_export str CLRbatStr(bat *ret, const bat *l);
mal_export str CLRbatRed(bat *ret, const bat *l);
mal_export str CLRbatGreen(bat *ret, const bat *l);
mal_export str CLRbatBlue(bat *ret, const bat *l);
mal_export str CLRbatSaturation(bat *ret, const bat *l);
mal_export str CLRbatValue(bat *ret, const bat *l);
mal_export str CLRbatHue(bat *ret, const bat *l);
mal_export str CLRbatHueInt(bat *ret, const bat *l);
mal_export str CLRbatSaturationInt(bat *ret, const bat *l);
mal_export str CLRbatValueInt(bat *ret, const bat *l);
mal_export str CLRbatLuminance(bat *ret, const bat *l);
mal_export str CLRbatCr(bat *ret, const bat *l);
mal_export str CLRbatCb(bat *ret, const bat *l);

mal_export str CLRbatHsv(bat *ret, const bat *l, const bat *bid2, const bat *bid3);
mal_export str CLRbatRgb(bat *ret, const bat *l, const bat *bid2, const bat *bid3);
mal_export str CLRbatycc(bat *ret, const bat *l, const bat *bid2, const bat *bid3);
#endif 
