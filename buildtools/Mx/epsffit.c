/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2005 CWI.
 * All Rights Reserved.
 */

/* epsffit.c
 * AJCD 6 Dec 90
 * fit epsf file into constrained size
 * Usage:
 *       epsffit [-c] [-r] [-a] [-s] llx lly urx ury [infile [outfile]]
 *               -c centres the image in the bounding box given
 *               -r rotates the image by 90 degrees anti-clockwise
 *               -a alters the aspect ratio to fit the bounding box
 *               -s adds a showpage at the end of the image
 *
 * Added filename spec (from Larry Weissman) 5 Feb 93
 * Accepts double %%BoundingBox input, outputs proper BB, 4 Jun 93. (I don't
 * like this; developers should read the Big Red Book before writing code which
 * outputs PostScript.
 */

#include <mx_config.h>
#include "Mx.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
/* #include "patchlev.h" */

#ifndef min
#define min(x,y) ((x) > (y) ? (y) : (x))
#endif
#ifndef max
#define max(x,y) ((x) > (y) ? (x) : (y))
#endif

static char *prog;

void
usage(void)
{
	fprintf(stderr, "Usage: %s [-c] [-r] [-a] [-s] llx lly urx ury [infile [outfile]]\n", prog);
	exit(1);
}

int
main(int argc, char **argv)
{
	int bbfound = 0;	/* %%BoundingBox: found */
	int urx = 0, ury = 0, llx = 0, lly = 0;
	int furx, fury, fllx, flly;
	int showpage = 0, centre = 0, rotate = 0, aspect = 0, maximise = 0;
	char buf[BUFSIZ];
	FILE *input = stdin;
	FILE *output = stdout;

	prog = *argv++;
	argc--;

	while (argc > 0 && argv[0][0] == '-') {
		switch (argv[0][1]) {
		case 'c':
			centre = 1;
			break;
		case 's':
			showpage = 1;
			break;
		case 'r':
			rotate = 1;
			break;
		case 'a':
			aspect = 1;
			break;
		case 'm':
			maximise = 1;
			break;
		case 'v':
		default:
			usage();
		}
		argc--;
		argv++;
	}

	if (argc < 4 || argc > 6)
		usage();
	fllx = atoi(argv[0]);
	flly = atoi(argv[1]);
	furx = atoi(argv[2]);
	fury = atoi(argv[3]);

	if (argc > 4) {
		if (!(input = fopen(argv[4], "r"))) {
			fprintf(stderr, "%s: can't open input file %s\n", prog, argv[4]);
			exit(1);
		}
	}
	if (argc > 5) {
		if (!(output = fopen(argv[5], "w"))) {
			fprintf(stderr, "%s: can't open output file %s\n", prog, argv[5]);
			exit(1);
		}
	}

	while (fgets(buf, BUFSIZ, input)) {
		if (buf[0] == '%' && (buf[1] == '%' || buf[1] == '!')) {
			/* still in comment section */
			if (!strncmp(buf, "%%BoundingBox:", 14)) {
				double illx, illy, iurx, iury;	/* input bbox parameters */

				if (sscanf(buf, "%%%%BoundingBox:%lf %lf %lf %lf\n", &illx, &illy, &iurx, &iury) == 4) {
					bbfound = 1;
					llx = (int) illx;	/* accept doubles, but convert to int */
					lly = (int) illy;
					urx = (int) (iurx + 0.5);
					ury = (int) (iury + 0.5);
				}
			} else if (!strncmp(buf, "%%EndComments", 13)) {
				strcpy(buf, "\n");	/* don't repeat %%EndComments */
				break;
			} else
				fputs(buf, output);
		} else
			break;
	}

	if (bbfound) {		/* put BB, followed by scale&translate */
		int fwidth, fheight;
		double xscale, yscale;
		double xoffset = fllx, yoffset = flly;
		double width = urx - llx, height = ury - lly;

		if (maximise)
			if ((width > height && fury - flly > furx - fllx) || (width < height && fury - flly < furx - fllx))
				rotate = 1;

		if (rotate) {
			fwidth = fury - flly;
			fheight = furx - fllx;
		} else {
			fwidth = furx - fllx;
			fheight = fury - flly;
		}

		xscale = fwidth / width;
		yscale = fheight / height;

		if (!aspect) {	/* preserve aspect ratio ? */
			xscale = yscale = min(xscale, yscale);
		}
		width *= xscale;	/* actual width and height after scaling */
		height *= yscale;
		if (centre) {
			if (rotate) {
				xoffset += (fheight - height) / 2;
				yoffset += (fwidth - width) / 2;
			} else {
				xoffset += (fwidth - width) / 2;
				yoffset += (fheight - height) / 2;
			}
		}
		fprintf(output, "%%%%BoundingBox: %d %d %d %d\n", (int) xoffset, (int) yoffset, (int) (xoffset + (rotate ? height : width)), (int) (yoffset + (rotate ? width : height)));
		if (rotate) {	/* compensate for original image shift */
			xoffset += height + lly * yscale;	/* displacement for rotation */
			yoffset -= llx * xscale;
		} else {
			xoffset -= llx * xscale;
			yoffset -= lly * yscale;
		}
		fputs("%%EndComments\n", output);
		if (showpage)
			fputs("save /showpage{}def /copypage{}def /erasepage{}def\n", output);
		else
			fputs("%%BeginProcSet: epsffit 1 0\n", output);
		fputs("gsave\n", output);
		fprintf(output, "%.3f %.3f translate\n", xoffset, yoffset);
		if (rotate)
			fputs("90 rotate\n", output);
		fprintf(output, "%.3f %.3f scale\n", xscale, yscale);
		if (!showpage)
			fputs("%%EndProcSet\n", output);
	}
	do {
		fputs(buf, output);
	} while (fgets(buf, BUFSIZ, input));
	if (bbfound) {
		fputs("grestore\n", output);
		if (showpage)
			fputs("restore showpage\n", output);	/* just in case */
	} else {
		fprintf(stderr, "%s: no %%%%BoundingBox:\n", prog);
		exit(1);
	}
	exit(0);
	return 0;
}
