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
 * Portions created by CWI are Copyright (C) 1997-2007 CWI.
 * All Rights Reserved.
 */

/***************************************************************************\
*              PRINTING INFO TO THE SCREEN                                  *
\***************************************************************************/

void
printCPU(cacheInfo * cache, caliblng MHz, caliblng delay)
{
	FILE *fp = stderr;

	fprintf(fp, "CPU loop + L1 access:    ");
	fprintf(fp, " %6.2f ns = %3ld cy\n", NSperIt(cache->latency1[0]), cround(CYperIt(cache->latency1[0])));
	fprintf(fp, "             ( delay:    ");
	fprintf(fp, " %6.2f ns = %3ld cy )\n", NSperIt(delay), cround(CYperIt(delay)));
	fprintf(fp, "\n");
	fflush(fp);
}

void
printCache(cacheInfo * cache, AssoInfo * Asso, caliblng MHz)
{
	caliblng l;
	FILE *fp = stderr;


	fprintf(fp, "caches:\n");
	fprintf(fp, "level  size    linesize   associativity  miss-latency        replace-time\n");
	for (l = 0; l < cache->levels; l++) {
		fprintf(fp, "  %1ld   ", l + 1);
		if (cache->size[l] >= (1024 * 1024 * 1024)) {
			fprintf(fp, " %3ld GB ", cache->size[l] / (1024 * 1024 * 1024));
		} else if (cache->size[l] >= (1024 * 1024)) {
			fprintf(fp, " %3ld MB ", cache->size[l] / (1024 * 1024));
		} else {
			fprintf(fp, " %3ld KB ", cache->size[l] / 1024);
		}
		fprintf(fp, " %3ld bytes ", cache->linesize[l + 1]);
		fprintf(fp, "    %3ld-way    ", Asso->entries[l]);
		fprintf(fp, " %6.2f ns = %3ld cy ", NSperIt(cache->latency2[l + 1] - cache->latency2[l]), cround(CYperIt(cache->latency2[l + 1] - cache->latency2[l])));
		fprintf(fp, " %6.2f ns = %3ld cy\n", NSperIt(cache->latency1[l + 1] - cache->latency1[l]), cround(CYperIt(cache->latency1[l + 1] - cache->latency1[l])));
	}
	fprintf(fp, "\n");
	fflush(fp);
}

void
printTLB(TLBinfo * TLB, caliblng MHz)
{
	caliblng l;
	FILE *fp = stderr;

	fprintf(fp, "TLBs:\n");
	fprintf(fp, "level #entries  pagesize  miss-latency");
/*
	fprintf(fp, "        replace-time");
*/
	fprintf(fp, "\n");
	for (l = 0; l < TLB->levels; l++) {
		fprintf(fp, "  %1ld   ", l + 1);
		fprintf(fp, "   %3ld   ", TLB->entries[l]);
		if (TLB->pagesize[l + 1] >= (1024 * 1024 * 1024)) {
			fprintf(fp, "  %3ld GB  ", TLB->pagesize[l + 1] / (1024 * 1024 * 1024));
		} else if (TLB->pagesize[l + 1] >= (1024 * 1024)) {
			fprintf(fp, "  %3ld MB  ", TLB->pagesize[l + 1] / (1024 * 1024));
		} else {
			fprintf(fp, "  %3ld KB  ", TLB->pagesize[l + 1] / 1024);
		}
		fprintf(fp, " %6.2f ns = %3ld cy ", NSperIt(TLB->latency2[l + 1] - TLB->latency2[l]), cround(CYperIt(TLB->latency2[l + 1] - TLB->latency2[l])));
/*
		fprintf(fp, " %6.2f ns = %3ld cy" , NSperIt(TLB->latency1[l + 1] - TLB->latency1[l]), cround(CYperIt(TLB->latency1[l + 1] - TLB->latency1[l])));
*/
		fprintf(fp, "\n");
	}
	fprintf(fp, "\n");
	fflush(fp);
}

void
printAsso(AssoInfo * Asso, caliblng MHz)
{
	caliblng l;
	FILE *fp = stderr;

	(void) MHz;

	fprintf(fp, "Assos:\n");
/*+
	fprintf(fp, "level #entries  pagesize  miss-latency");
+*/
	fprintf(fp, "level associativity");
/*
	fprintf(fp, "        replace-time");
*/
	fprintf(fp, "\n");
	for (l = 0; l < Asso->levels; l++) {
		fprintf(fp, "  %1ld   ", l + 1);
		fprintf(fp, "   %3ld-way ", Asso->entries[l]);
/*+
		if (Asso->pagesize[l + 1] >= (1024 * 1024 * 1024)) {
			fprintf(fp, "  %3ld GB  ", Asso->pagesize[l + 1] / (1024 * 1024 * 1024));
		} else if (Asso->pagesize[l + 1] >= (1024 * 1024)) {
			fprintf(fp, "  %3ld MB  ", Asso->pagesize[l + 1] / (1024 * 1024));
		} else {
			fprintf(fp, "  %3ld KB  ", Asso->pagesize[l + 1] / 1024);
		}
		fprintf(fp, " %6.2f ns = %3ld cy ", NSperIt(Asso->latency2[l + 1] - Asso->latency2[l]), cround(CYperIt(Asso->latency2[l + 1] - Asso->latency2[l])));
+*/
/*
		fprintf(fp, " %6.2f ns = %3ld cy" , NSperIt(Asso->latency1[l + 1] - Asso->latency1[l]), cround(CYperIt(Asso->latency1[l + 1] - Asso->latency1[l])));
*/
		fprintf(fp, "\n");
	}
	fprintf(fp, "\n");
	fflush(fp);
}
