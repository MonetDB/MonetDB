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

#ifndef _MOSAIC_LINEAR_
#define _MOSAIC_LINEAR_

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"
#include "mosaic_utility.h"

bool MOStypes_linear(BAT* b);

#define MosaicBlkHeader_DEF_linear(TPE)\
typedef struct {\
	MosaicBlkRec rec;\
	char padding;\
	DeltaTpe(TPE) offset;\
	DeltaTpe(TPE) step;\
} MOSBlockHeader_linear_##TPE;

ALGEBRA_INTERFACES_INTEGERS_ONLY(linear);
#define TYPE_IS_SUPPORTED_linear(TPE) INTEGERS_ONLY(TPE)
#define DO_OPERATION_ON_linear(OPERATION, TPE, ...) DO_OPERATION_ON_INTEGERS_ONLY(OPERATION, linear, TPE, __VA_ARGS__)

#define linear_offset(TPE, task)	(((MOSBlockHeaderTpe(linear, TPE)*) (task)->blk)->offset)
#define linear_step(TPE, task)		(((MOSBlockHeaderTpe(linear, TPE)*) (task)->blk)->step)

#define MOScodevectorFrame(task, TPE) ((BitVector) &((MOSBlockHeaderTpe(frame, TPE)*) (task)->blk)->bitvector)

#endif /* _MOSAIC_LINEAR_ */
