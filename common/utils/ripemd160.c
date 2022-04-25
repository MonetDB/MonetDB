/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rmd160.h"
#include "ripemd160.h"

void
RIPEMD160Reset(RIPEMD160Context *ctxt)
{
	MDinit(ctxt->digest);
	ctxt->noverflow = 0;
	ctxt->length = 0;
}

void
RIPEMD160Input(RIPEMD160Context *ctxt, const uint8_t *bytes, unsigned bytecount)
{
	dword X[16];

	ctxt->length += bytecount;
	if (ctxt->noverflow > 0) {
		if (ctxt->noverflow + bytecount < 64) {
			memcpy(ctxt->overflow + ctxt->noverflow, bytes, bytecount);
			ctxt->noverflow += bytecount;
			return;
		}
		memcpy(ctxt->overflow + ctxt->noverflow, bytes, bytecount - ctxt->noverflow);
		const uint8_t *x = ctxt->overflow;
		for (int i = 0; i < 16; i++) {
			X[i] = BYTES_TO_DWORD(x);
			x += 4;
		}
		bytecount -= ctxt->noverflow;
		bytes += ctxt->noverflow;
		ctxt->noverflow = 0;
		MDcompress(ctxt->digest, X);
	}
	while (bytecount >= 64) {
		for (int i = 0; i < 16; i++) {
			X[i] = BYTES_TO_DWORD(bytes);
			bytes += 4;
		}
		bytecount -= 64;
		MDcompress(ctxt->digest, X);
	}
	if (bytecount > 0)
		memcpy(ctxt->overflow, bytes, bytecount);
	ctxt->noverflow = bytecount;
}

void
RIPEMD160Result(RIPEMD160Context *ctxt, uint8_t digest[RIPEMD160_DIGEST_LENGTH])
{
	MDfinish(ctxt->digest, ctxt->overflow, (dword) ctxt->length, 0);
	for (int i = 0; i < RIPEMD160_DIGEST_LENGTH; i += 4) {
		digest[i] = (uint8_t) ctxt->digest[i >> 2];
		digest[i + 1] = (uint8_t) (ctxt->digest[i >> 2] >> 8);
		digest[i + 2] = (uint8_t) (ctxt->digest[i >> 2] >> 16);
		digest[i + 3] = (uint8_t) (ctxt->digest[i >> 2] >> 24);
	}
}
