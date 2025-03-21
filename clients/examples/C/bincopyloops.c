/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "bincopydata.h"


void convert16(void *start, void *end);
void convert32(void *start, void *end);
void convert64(void *start, void *end);
void convert128(void *start, void *end);

void
convert16(void *start, void *end)
{
	for (uint16_t *p = start; p < (uint16_t*)end; p++)
		copy_binary_convert16(p);
}

void
convert32(void *start, void *end)
{
	for (uint32_t *p = start; p < (uint32_t*)end; p++)
		copy_binary_convert32(p);
}

void
convert64(void *start, void *end)
{
	for (uint64_t *p = start; p < (uint64_t*)end; p++)
		copy_binary_convert64(p);
}

void
convert128(void *start, void *end)
{
#ifdef HAVE_HGE
	for (uhge *p = start; p < (uhge*)end; p++) {
		copy_binary_convert128(p);
	}
#else
	(void)start;
	(void)end;
#endif
}



int
main(void)
{
	(void)convert16;
	(void)convert32;
	(void)convert64;
	(void)convert128;
	return 0;
}
