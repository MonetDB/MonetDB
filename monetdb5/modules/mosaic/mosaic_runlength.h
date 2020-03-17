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

#ifndef _MOSAIC_RLE_
#define _MOSAIC_RLE_

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"
#include "mosaic_utility.h"

bool MOStypes_runlength(BAT* b);

#define MosaicBlkHeader_DEF_runlength(TPE)\
typedef struct {\
	MosaicBlkRec rec;\
	char padding;\
	TPE val;\
} MOSBlockHeader_runlength_##TPE;

ALGEBRA_INTERFACES_ALL_TYPES(runlength);
#define TYPE_IS_SUPPORTED_runlength(TPE) ALL_TYPES_SUPPORTED(TPE)

#define DO_OPERATION_ON_runlength(OPERATION, TPE, ...) DO_OPERATION_ON_ALL_TYPES(OPERATION, runlength, TPE, __VA_ARGS__)

#define GET_VAL_runlength(task, TPE) (((MOSBlockHeaderTpe(runlength, TPE)*) (task)->blk)->val)

#endif /* _MOSAIC_RLE_ */
