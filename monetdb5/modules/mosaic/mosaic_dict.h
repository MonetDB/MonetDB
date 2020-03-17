/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * authors Martin Kersten, Aris Koning
 */

#ifndef _MOSAIC_VAR_
#define _MOSAIC_VAR_

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"
#include "mosaic_utility.h"
#include "mosaic_dictionary.h"

#define DICTTHRESHOLD 4192
#define DICTSIZE 256

bool MOStypes_dict(BAT* b);
mal_export void MOScreatevar(MOStask* task);

#define MosaicBlkHeader_DEF_dict(TPE) MosaicBlkHeader_DEF_dictionary(dict, TPE)

ALGEBRA_INTERFACES_ALL_TYPES_WITH_DICTIONARY(dict);
#define TYPE_IS_SUPPORTED_dict(TPE) ALL_TYPES_SUPPORTED(TPE)

#define DO_OPERATION_ON_dict(OPERATION, TPE, ...) DO_OPERATION_ON_ALL_TYPES(OPERATION, dict, TPE, __VA_ARGS__)

#endif /* _MOSAIC_VAR_ */
