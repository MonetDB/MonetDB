/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (c) Martin Kersten
 */

#ifndef _JSON_H_
#define _JSON_H_

#include "json.h"

mal_export str
JSONresultSet(json *res,bat *u, bat *rev, bat *js);

#endif
