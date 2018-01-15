/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _MAL_FACTORY_H
#define _MAL_FACTORY_H

/* #define DEBUG_MAL_FACTORY  */

#include "mal.h"
#include "mal_client.h"

mal_export str shutdownFactory(Client cntxt, MalBlkPtr mb);
mal_export str shutdownFactoryByName(Client cntxt, Module m,str nme);
#endif /*  _MAL_FACTORY_H */
