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
 * Portions created by CWI are Copyright (C) 1997-2008 CWI.
 * All Rights Reserved.
 */

#include "mx_config.h"

#include "Mx.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include "disclaimer.h"

#define END_OF_HEADER_MARKER  "@'EOHMARKER (DO NOT EDIT THIS LINE)"

#include "mx_getopt.h"

char *inputfile, *outputfile, *prefixfile;

void
printUsage(char *progname)
{
	fprintf(stderr, "Usage: %s [-f <prefix>] [-o <outfile>] mxfile \n", progname);
}

FILE *
stripFile(FILE *fp)
{
	char line[16384];
	size_t ret;
	int i;

	fseek(fp, 0, 0);
	while (!feof(fp)) {
		i = 0;
		do {
			ret = fread(&line[i++], 1, 1, fp);
		}
		while (ret && line[i - 1] != '\n');
		if (strstr(line, END_OF_HEADER_MARKER))
			return fp;
	}
	fseek(fp, 0, 0);
	return fp;
}

FILE *
openFile(char *name, char *mode)
{
	FILE *fp = fopen(name, mode);

	if (!fp) {
		fprintf(stderr, "Can't open inputfile '%s' (aborting).\n", inputfile);
		exit(1);
	}
	return fp;
}

void
processFile(void)
{
	FILE *fp, *op;
	int temp = 0;
	size_t ret;
	char letter, cmd[256];

	if (!outputfile) {
		outputfile = malloc(strlen(inputfile) + 2);
		outputfile[0] = '.';
		strncpy(outputfile + 1, inputfile, strlen(inputfile) + 1);
		temp = 1;
	}
	fp = stripFile(openFile(inputfile, "r"));
	op = openFile(outputfile, "w");

	if (prefixfile)
		disclaimerfile = prefixfile;
	else
		disclaimerfile = "";

	insertDisclaimer(op, outputfile);
	fwrite(END_OF_HEADER_MARKER, strlen(END_OF_HEADER_MARKER), 1, op);
	fwrite("\n", 1, 1, op);


	while (!feof(fp)) {
		ret = fread(&letter, 1, 1, fp);
		if (!ret)
			break;
		fwrite(&letter, 1, 1, op);
	}
	fclose(fp);
	fclose(op);
	if (temp) {
		snprintf(cmd, sizeof(cmd), "mv '%s' '%s'", outputfile, inputfile);
		system(cmd);
	}
}

int
main(int argc, char **argv)
{
	int option;

	while ((option = getopt(argc, argv, "f:o:")) != EOF)
		switch (option) {
		case 'f':
			prefixfile = optarg;
			break;
		case 'o':
			outputfile = strdup(optarg);
			break;
		default:
			printUsage(argv[0]);
			exit(0);
		}
	if (optind < argc) {
		inputfile = strdup(argv[optind]);
	} else {
		printUsage(argv[0]);
		exit(1);
	}
	if (outputfile)
		if (!strcmp(outputfile, inputfile)) {
			printf("inputfile and outputfile must be different names (aborting).\n");
			exit(1);
		}

	processFile();
	return 0;
}
