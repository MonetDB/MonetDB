/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * (c)2014 author Martin Kersten
 */

#ifndef _MOSAIC_PREFIX_
#define _MOSAIC_PREFIX_

/* #define _DEBUG_PREFIX_*/

#include <mal.h>
#include "mal_interpreter.h"
#include "mal_client.h"
#include "mosaic_utility.h"
#include "gdk_bitvector.h"

bool MOStypes_prefix(BAT* b);
mal_export void MOSlayout_prefix(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties);

#define Prefixbte uint8_t
#define Prefixsht uint16_t
#define Prefixint uint32_t
#define Prefixlng uint64_t
#define Prefixoid uint64_t
#define Prefixflt uint32_t
#define Prefixdbl uint64_t
#ifdef HAVE_HGE
#define Prefixhge uhge
#endif

#define PrefixTpe(TPE) Prefix##TPE

#define MosaicBlkHeader_DEF_prefix(TPE)\
typedef struct {\
	MosaicBlkHdrGeneric base;\
	int suffix_bits;\
	PrefixTpe(TPE) prefix;\
	BitVectorChunk bitvector; /*First chunk of bitvector to force correct alignment.*/\
} MOSBlockHeader_prefix_##TPE;

ALGEBRA_INTERFACES_INTEGERS_ONLY(prefix);
#define DO_OPERATION_ON_prefix(OPERATION, TPE, ...) DO_OPERATION_ON_INTEGERS_ONLY(OPERATION, prefix, TPE, __VA_ARGS__)

#define MOScodevectorPrefix(task, TPE) ((BitVector) &((MOSBlockHeaderTpe(prefix, TPE)*) (task)->blk)->bitvector)

#define join_inner_loop_prefix(TPE, HAS_NIL, RIGHT_CI_NEXT)\
{\
    MOSBlockHeaderTpe(prefix, TPE)* parameters = (MOSBlockHeaderTpe(prefix, TPE)*) task->blk;\
	BitVector base = (BitVector) MOScodevectorPrefix(task, TPE);\
	PrefixTpe(TPE) prefix = parameters->prefix;\
	int suffix_bits = parameters->suffix_bits;\
    for (oid ro = canditer_peekprev(task->ci); !is_oid_nil(ro) && ro < last; ro = RIGHT_CI_NEXT(task->ci)) {\
		BUN i = (BUN) (ro - first);\
		TPE rval =  (TPE) (prefix | getBitVector(base,i,suffix_bits));\
        IF_EQUAL_APPEND_RESULT(HAS_NIL, TPE);\
	}\
}

#endif /* _MOSAIC_PREFIX_ */
