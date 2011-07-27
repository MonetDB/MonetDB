/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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
