/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/**
    tomo :  A 3D Volumetric tomograph, a medical scan.
            8bit unsigned integer (byte) intensity values
            256 * 256 * 154 cells

            Used for 'index-based' tests, thus the actual values
            are irrelevant.
**/

#include <stdio.h>

int
main()
{
	int x, y, z;
	int val = 0;

	FILE *file = fopen("tomo.tab", "wt");

	for (x = 0; x < 256; x++) {
		for (y = 0; y < 256; y++) {
			for (z = 0; z < 154; z++) {
				val = (val + 1) & 0xFF;
				fprintf(file, "%4d,%4d,%4d,%4d\n", x, y, z, val);
			}
		}
	}

	fclose(file);

	return 0;
}
