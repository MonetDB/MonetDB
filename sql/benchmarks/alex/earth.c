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
    earth : 15 Sattelite photographs of earth
            8bit unsigned  integer (byte) intensity values
            800 * 800 * 15 pixels

            9 images contain images with values ranging from 0..249
            3 images contain a single value, 253, 254 ,255 resp.
            3 images contain values from 0..249 in all but one cell,
                     with a single cell of value 250, 251 ,252 resp.

            Used in value-based selection, using the values 250..255
            to 'mplicitly select 0..6 images.
**/


#include <stdio.h>

void
output_default_array(FILE *file, int id)
{
	int x, y;
	int num_249_cnt = id;

	for (x = 0; x < 800; x++) {
		for (y = 0; y < 800; y++) {
			num_249_cnt = (num_249_cnt + 1) % 250;
			fprintf(file, "%4d,%4d,%4d,%4d\n", id, x, y, num_249_cnt);
		}
	}
}

void
output_oneval_array(FILE *file, int id, int oneval)
{
	int x, y;
	int num_249_cnt = id;

	for (x = 0; x < 800; x++) {
		for (y = 0; y < 800; y++) {
			if (x == id *50 && y == id *40) {
				fprintf(file, "%4d,%4d,%4d,%4d\n", id, x, y, oneval);
			} else {
				num_249_cnt = (num_249_cnt + 1) % 250;
				fprintf(file, "%4d,%4d,%4d,%4d\n", id, x, y, num_249_cnt);
			}
		}
	}
}

void
output_const_array(FILE *file, int id, int val)
{
	int x, y;

	for (x = 0; x < 800; x++) {
		for (y = 0; y < 800; y++) {
			fprintf(file, "%4d,%4d,%4d,%4d\n", id, x, y, val);
		}
	}
}

int
main()
{
	FILE *file = fopen("earth.tab", "wt");

	output_default_array(file, 0);
	output_const_array(file, 1, 254);
	output_default_array(file, 2);
	output_default_array(file, 3);
	output_oneval_array(file, 4, 251);
	output_default_array(file, 5);
	output_oneval_array(file, 6, 250);
	output_default_array(file, 7);
	output_const_array(file, 8, 253);
	output_default_array(file, 9);
	output_default_array(file, 10);
	output_const_array(file, 11, 255);
	output_default_array(file, 12);
	output_oneval_array(file, 13, 252);
	output_default_array(file, 14);

	fclose(file);

	return 0;
}
