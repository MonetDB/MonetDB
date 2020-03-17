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

#ifndef _MOSAIC_RAW_
#define _MOSAIC_RAW_

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"
#include "mosaic_utility.h"

bool MOStypes_raw(BAT* b);

#define MosaicBlkHeader_DEF_raw(TPE)\
typedef struct {\
	MosaicBlkRec rec;\
	char padding;\
    TPE init;\
} MOSBlockHeader_raw_##TPE;

#define GET_INIT_raw(task, TPE) (((MOSBlockHeaderTpe(raw, TPE)*) (task)->blk)->init)

ALGEBRA_INTERFACES_ALL_TYPES(raw);
#define TYPE_IS_SUPPORTED_raw(TPE) ALL_TYPES_SUPPORTED(TPE)

#define DO_OPERATION_ON_raw(OPERATION, TPE, ...) DO_OPERATION_ON_ALL_TYPES(OPERATION, raw, TPE, __VA_ARGS__)

#endif /* _MOSAIC_RAW_ */
