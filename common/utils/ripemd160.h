/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef _SEEN_RIPEMD160_H
#define _SEEN_RIPEMD160_H 1

#include "mcrypt.h"				/* for mcrypt_export */

typedef struct RIPEMD160Context {
	uint32_t digest[5];
	uint8_t overflow[64];
	unsigned noverflow;
	size_t length;
} RIPEMD160Context;

#define RIPEMD160_DIGEST_LENGTH 20

void RIPEMD160Reset(RIPEMD160Context *ctxt);
void RIPEMD160Input(RIPEMD160Context *ctxt,
					const uint8_t *bytes, unsigned bytecount);
void RIPEMD160Result(RIPEMD160Context *ctxt,
					 uint8_t digest[RIPEMD160_DIGEST_LENGTH]);

#endif
