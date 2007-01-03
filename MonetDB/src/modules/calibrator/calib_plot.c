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
*              PLOTTING DIAGRAMS                                            *
\***************************************************************************/


void
plotCache(cacheInfo * cache, caliblng ** result, caliblng MHz, char *fn, FILE *fp, caliblng delay)
{
	caliblng l, x, xx = (result[0][0] & 0xffffff) - 1, y, yy = (result[0][0] >> 24) - 1;
	calibdbl xl, xh, yl, yh, z;
	char *s, *flnm = strdup(fn), top = 0;

	xl = (calibdbl) result[1][0] / 1024.0;
	xh = (calibdbl) result[yy][0] / 1024.0;
	for (yl = 1.0; yl > (caliblng) NSperIt(result[1][1] - delay); yl /= 10) ;
	for (yh = 1000; yh < (caliblng) NSperIt(result[yy][1] - delay); yh *= 10) ;
	if ((log10(yh) - log10((caliblng) NSperIt(result[yy][1] - delay))) > (log10((caliblng) NSperIt(result[1][1] - delay)) - log10(yl)))
		top = 1;
	fprintf(fp, "# Calibrator v%s\n", CALIB_VERSION);
	fprintf(fp, "# (by Stefan.Manegold@cwi.nl, http://www.cwi.nl/~manegold/)\n");
	if (delay)
		fprintf(fp, "# ( delay: %6.2f ns = %3ld cy )\n", NSperIt(delay), cround(CYperIt(delay)));
	fprintf(fp, " set term postscript portrait enhanced\t# PS\n");
	fprintf(fp, " set output '%s.ps'\t# PS\n", fn);
	fprintf(fp, " set title '");
	fprintf(fp, "%s", strtok(flnm, "_"));
	while ((s = strtok(NULL, "_")))
		fprintf(fp, "\\_%s", s);
	fprintf(fp, "'\t# PS\n");
	if (top)
		fprintf(fp, " set label %d '^{ Calibrator v%s (Stefan.Manegold\\@cwi.nl, www.cwi.nl/~manegold) }' at graph 0.5,graph 0.96 center\t# PS\n", 1, CALIB_VERSION);
	else
		fprintf(fp, " set label %d '^{ Calibrator v%s (Stefan.Manegold\\@cwi.nl, www.cwi.nl/~manegold) }' at graph 0.5,graph 0.02 center\t# PS\n", 1, CALIB_VERSION);
	fprintf(fp, "#set term gif transparent interlace small size 500, 707 # xFFFFFF x333333 x333333 x0055FF x005522 x660000 xFF0000 x00FF00 x0000FF\t# GIF\n");
	fprintf(fp, "#set output '%s.gif'\t# GIF\n", fn);
	fprintf(fp, "#set title '%s'\t# GIF\n", fn);
	if (top)
		fprintf(fp, "#set label %d    'Calibrator v%s (Stefan.Manegold@cwi.nl, www.cwi.nl/~manegold)'    at graph 0.5,graph 0.97 center\t# GIF\n", 1, CALIB_VERSION);
	else
		fprintf(fp, "#set label %d    'Calibrator v%s (Stefan.Manegold@cwi.nl, www.cwi.nl/~manegold)'    at graph 0.5,graph 0.03 center\t# GIF\n", 1, CALIB_VERSION);
	fprintf(fp, "set data style linespoints\n");
	fprintf(fp, "set key below\n");
	fprintf(fp, "set xlabel 'memory range [bytes]'\n");
	fprintf(fp, "set x2label ''\n");
	fprintf(fp, "set ylabel 'nanosecs per iteration'\n");
	fprintf(fp, "set y2label 'cycles per iteration'\n");
	fprintf(fp, "set logscale x 2\n");
	fprintf(fp, "set logscale x2 2\n");
	fprintf(fp, "set logscale y 10\n");
	fprintf(fp, "set logscale y2 10\n");
	fprintf(fp, "set format x '%%1.0f'\n");
	fprintf(fp, "set format x2 '%%1.0f'\n");
	fprintf(fp, "set format y '%%1.0f'\n");
	fprintf(fp, "set format y2 ''\n");
	fprintf(fp, "set xrange[%f:%f]\n", xl, xh);
	fprintf(fp, "#set x2range[%f:%f]\n", xl, xh);
	fprintf(fp, "set yrange[%f:%f]\n", yl, yh);
	fprintf(fp, "#set y2range[%f:%f]\n", yl, yh);
	fprintf(fp, "set grid x2tics\n");
	fprintf(fp, "set xtics mirror");
	for (x = 1, l = 1, s = " ("; x <= xh; x *= 2, l++, s = ", ") {
		if (l & 1) {
			if (x >= (1024 * 1024)) {
				fprintf(fp, "%s'%ldG' %ld", s, x / (1024 * 1024), x);
			} else if (x >= 1024) {
				fprintf(fp, "%s'%ldM' %ld", s, x / 1024, x);
			} else {
				fprintf(fp, "%s'%ldk' %ld", s, x, x);
			}
		} else {
			fprintf(fp, "%s'' %ld", s, x);
		}
	}
	fprintf(fp, ")\n");
	fprintf(fp, "set x2tics mirror");
	for (l = 0, s = " ("; l < cache->levels; l++, s = ", ") {
		if (cache->size[l] >= (1024 * 1024 * 1024)) {
			fprintf(fp, "%s'[%ldG]' %ld", s, cache->size[l] / (1024 * 1024 * 1024), cache->size[l] / 1024);
		} else if (cache->size[l] >= (1024 * 1024)) {
			fprintf(fp, "%s'[%ldM]' %ld", s, cache->size[l] / (1024 * 1024), cache->size[l] / 1024);
		} else {
			fprintf(fp, "%s'[%ldk]' %ld", s, cache->size[l] / 1024, cache->size[l] / 1024);
		}
	}
	fprintf(fp, ")\n");
	fprintf(fp, "set y2tics");
	for (l = 0, s = " ("; l <= cache->levels; l++, s = ", ") {
		if (!delay)
			fprintf(fp, "%s'(%ld)' %f", s, cround(CYperIt(cache->latency1[l] - delay)), NSperIt(cache->latency1[l] - delay));
		else
			fprintf(fp, "%s'(%ld)' %f", s, cround(CYperIt(cache->latency2[l] - delay)), NSperIt(cache->latency2[l] - delay));
	}
	for (y = 1; y <= yh; y *= 10) {
		fprintf(fp, "%s'%1.4g' %ld", s, (calibdbl) (y * MHz) / 1000.0, y);
	}
	fprintf(fp, ")\n");
	for (l = 0; l <= cache->levels; l++) {
		if (!delay)
			z = cround(CYperIt(cache->latency1[l] - delay)) * 1000.0 / (calibdbl) MHz;
		else
			z = cround(CYperIt(cache->latency2[l] - delay)) * 1000.0 / (calibdbl) MHz;
		fprintf(fp, " set label %ld '(%1.4g)  ' at %f,%f right\t# PS\n", l + 2, z, xl, z);
		fprintf(fp, "#set label %ld '(%1.4g)'   at %f,%f right\t# GIF\n", l + 2, z, xl, z);
		fprintf(fp, "set arrow %ld from %f,%f to %f,%f nohead lt 0\n", l + 2, xl, z, xh, z);
	}
	fprintf(fp, "plot \\\n0.1 title 'stride:' with points pt 0 ps 0");
	for (x = 1, l = cache->levels; x <= xx; x++) {
		fprintf(fp, " , \\\n'%s.data' using 1:($%ld-%f) title '", fn, (6 * x) + 1, NSperIt(delay));
		if ((l > 0) && (result[0][x] == cache->linesize[l])) {
			fprintf(fp, "\\{%ld\\}", result[0][x]);
			while ((--l >= 0) && (result[0][x] == cache->linesize[l])) ;
		} else {
			fprintf(fp, "%ld", result[0][x]);
		}
		fprintf(fp, "' with linespoints lt %ld pt %ld", x, x + 2);
	}
	fprintf(fp, "\n");
	fprintf(fp, "set nolabel\n");
	fprintf(fp, "set noarrow\n");
	fflush(fp);

	free(flnm);
}

void
plotTLB(TLBinfo * TLB, caliblng ** result, caliblng MHz, char *fn, FILE *fp, caliblng delay)
{
	caliblng l, x, xx = (result[0][0] & 0xffffff) - 1, y, yy = (result[0][0] >> 24) - 1;
	calibdbl xl, xh, yl, yh, z;
	char *s, *flnm = strdup(fn), top = 0;

	xl = (calibdbl) result[2][0];
	xh = (calibdbl) result[yy][0];
	for (yl = 1.0; yl > (caliblng) NSperIt(result[2][2] - delay); yl /= 10) ;
	for (yh = 1000; yh < (caliblng) NSperIt(result[yy][2] - delay); yh *= 10) ;
	if ((log10(yh) - log10((caliblng) NSperIt(result[yy][1] - delay))) > (log10((caliblng) NSperIt(result[1][1] - delay)) - log10(yl)))
		top = 1;
	fprintf(fp, "# Calibrator v%s\n", CALIB_VERSION);
	fprintf(fp, "# (by Stefan.Manegold@cwi.nl, http://www.cwi.nl/~manegold/)\n");
	if (delay)
		fprintf(fp, "# ( delay: %6.2f ns = %3ld cy )\n", NSperIt(delay), cround(CYperIt(delay)));
	fprintf(fp, " set term postscript portrait enhanced\t# PS\n");
	fprintf(fp, " set output '%s.ps'\t# PS\n", fn);
	fprintf(fp, " set title '");
	fprintf(fp, "%s", strtok(flnm, "_"));
	while ((s = strtok(NULL, "_")))
		fprintf(fp, "\\_%s", s);
	fprintf(fp, "'\t# PS\n");
	if (top)
		fprintf(fp, " set label %d '^{ Calibrator v%s (Stefan.Manegold\\@cwi.nl, www.cwi.nl/~manegold) }' at graph 0.5,graph 0.96 center\t# PS\n", 1, CALIB_VERSION);
	else
		fprintf(fp, " set label %d '^{ Calibrator v%s (Stefan.Manegold\\@cwi.nl, www.cwi.nl/~manegold) }' at graph 0.5,graph 0.02 center\t# PS\n", 1, CALIB_VERSION);
	fprintf(fp, "#set term gif transparent interlace small size 500, 707 # xFFFFFF x333333 x333333 x0055FF x005522 x660000 xFF0000 x00FF00 x0000FF\t# GIF\n");
	fprintf(fp, "#set output '%s.gif'\t# GIF\n", fn);
	fprintf(fp, "#set title '%s'\t# GIF\n", fn);
	if (top)
		fprintf(fp, "#set label %d    'Calibrator v%s (Stefan.Manegold@cwi.nl, www.cwi.nl/~manegold)'    at graph 0.5,graph 0.97 center\t# GIF\n", 1, CALIB_VERSION);
	else
		fprintf(fp, "#set label %d    'Calibrator v%s (Stefan.Manegold@cwi.nl, www.cwi.nl/~manegold)'    at graph 0.5,graph 0.03 center\t# GIF\n", 1, CALIB_VERSION);
	fprintf(fp, "set data style linespoints\n");
	fprintf(fp, "set key below\n");
	fprintf(fp, "set xlabel 'spots accessed'\n");
	fprintf(fp, "set x2label ''\n");
	fprintf(fp, "set ylabel 'nanosecs per iteration'\n");
	fprintf(fp, "set y2label 'cycles per iteration'\n");
	fprintf(fp, "set logscale x 2\n");
	fprintf(fp, "set logscale x2 2\n");
	fprintf(fp, "set logscale y 10\n");
	fprintf(fp, "set logscale y2 10\n");
	fprintf(fp, "set format x '%%1.0f'\n");
	fprintf(fp, "set format x2 '%%1.0f'\n");
	fprintf(fp, "set format y '%%1.0f'\n");
	fprintf(fp, "set format y2 ''\n");
	fprintf(fp, "set xrange[%f:%f]\n", xl, xh);
	fprintf(fp, "#set x2range[%f:%f]\n", xl, xh);
	fprintf(fp, "set yrange[%f:%f]\n", yl, yh);
	fprintf(fp, "#set y2range[%f:%f]\n", yl, yh);
	fprintf(fp, "set grid x2tics\n");
	fprintf(fp, "set xtics mirror");
	for (x = 1, l = 1, s = " ("; x <= xh; x *= 2, l++, s = ", ") {
		if (l | 1) {
			if (x >= (1024 * 1024)) {
				fprintf(fp, "%s'%ldM' %ld", s, x / (1024 * 1024), x);
			} else if (x >= 1024) {
				fprintf(fp, "%s'%ldk' %ld", s, x / 1024, x);
			} else {
				fprintf(fp, "%s'%ld' %ld", s, x, x);
			}
		} else {
			fprintf(fp, "%s'' %ld", s, x);
		}
	}
	fprintf(fp, ")\n");
	fprintf(fp, "set x2tics mirror");
	for (l = 0, s = " ("; l < TLB->levels; l++, s = ", ") {
		if (TLB->entries[l] >= (1024 * 1024)) {
			fprintf(fp, "%s'[%ldM]' %ld", s, TLB->entries[l] / (1024 * 1024), TLB->entries[l]);
		} else if (TLB->entries[l] >= 1024) {
			fprintf(fp, "%s'[%ldk]' %ld", s, TLB->entries[l] / 1024, TLB->entries[l]);
		} else {
			fprintf(fp, "%s'[%ld]' %ld", s, TLB->entries[l], TLB->entries[l]);
		}
	}
	fprintf(fp, "%s'<L1>' %ld)\n", s, TLB->mincachelines);
	fprintf(fp, "set y2tics");
	for (l = 0, s = " ("; l <= TLB->levels; l++, s = ", ") {
		if (!delay)
			fprintf(fp, "%s'(%ld)' %f", s, cround(CYperIt(TLB->latency1[l] - delay)), NSperIt(TLB->latency1[l] - delay));
		else
			fprintf(fp, "%s'(%ld)' %f", s, cround(CYperIt(TLB->latency2[l] - delay)), NSperIt(TLB->latency2[l] - delay));
	}
	for (y = 1; y <= yh; y *= 10) {
		fprintf(fp, "%s'%1.4g' %ld", s, (calibdbl) (y * MHz) / 1000.0, y);
	}
	fprintf(fp, ")\n");
	for (l = 0; l <= TLB->levels; l++) {
		if (!delay)
			z = cround(CYperIt(TLB->latency1[l] - delay)) * 1000.0 / (calibdbl) MHz;
		else
			z = cround(CYperIt(TLB->latency2[l] - delay)) * 1000.0 / (calibdbl) MHz;
		fprintf(fp, " set label %ld '(%1.4g)  ' at %f,%f right\t# PS\n", l + 2, z, xl, z);
		fprintf(fp, "#set label %ld '(%1.4g)'   at %f,%f right\t# GIF\n", l + 2, z, xl, z);
		fprintf(fp, "set arrow %ld from %f,%f to %f,%f nohead lt 0\n", l + 2, xl, z, xh, z);
	}
	fprintf(fp, "plot \\\n0.1 title 'stride:' with points pt 0 ps 0");
	for (x = 2, l = TLB->levels; x <= xx; x++) {
		fprintf(fp, " , \\\n'%s.data' using 1:($%ld-%f) title '", fn, (6 * (x - 1)) + 1, NSperIt(delay));
		if ((l > 0) && (result[0][x] == TLB->pagesize[l])) {
			fprintf(fp, "\\{%ld\\}", result[0][x]);
			while ((--l >= 0) && (result[0][x] == TLB->pagesize[l])) ;
		} else {
			fprintf(fp, "%ld", result[0][x]);
		}
		fprintf(fp, "' with linespoints lt %ld pt %ld", x, x + 2);
	}
	fprintf(fp, "\n");
	fprintf(fp, "set nolabel\n");
	fprintf(fp, "set noarrow\n");
	fflush(fp);

	free(flnm);
}

void
plotAsso(AssoInfo * Asso, caliblng ** result, caliblng MHz, char *fn, FILE *fp, caliblng delay, cacheInfo * cache)
{
	caliblng l, ll = 0, x, xx = (result[0][0] & 0xffffff) - 1, y, yy = (result[0][0] >> 24) - 1;
	calibdbl xl, xh, yl, yh, z;
	char *s, *flnm = strdup(fn), top = 0;

	xl = (calibdbl) result[2][0];
	xh = (calibdbl) result[yy][0];
	for (yl = 1.0; yl > (caliblng) NSperIt(result[2][1] - delay); yl /= 10) ;
	for (yh = 1000; yh < (caliblng) NSperIt(result[yy][1] - delay); yh *= 10) ;
	if ((log10(yh) - log10((caliblng) NSperIt(result[yy][1] - delay))) > (log10((caliblng) NSperIt(result[1][1] - delay)) - log10(yl)))
		top = 1;
	fprintf(fp, "# Calibrator v%s\n", CALIB_VERSION);
	fprintf(fp, "# (by Stefan.Manegold@cwi.nl, http://www.cwi.nl/~manegold/)\n");
	if (delay)
		fprintf(fp, "# ( delay: %6.2f ns = %3ld cy )\n", NSperIt(delay), cround(CYperIt(delay)));
	fprintf(fp, " set term postscript portrait enhanced\t# PS\n");
	fprintf(fp, " set output '%s.ps'\t# PS\n", fn);
	fprintf(fp, " set title '");
	fprintf(fp, "%s", strtok(flnm, "_"));
	while ((s = strtok(NULL, "_")))
		fprintf(fp, "\\_%s", s);
	fprintf(fp, "'\t# PS\n");
	ll += 1;
	if (top)
		fprintf(fp, " set label %ld '^{ Calibrator v%s (Stefan.Manegold\\@cwi.nl, www.cwi.nl/~manegold) }' at graph 0.5,graph 0.96 center\t# PS\n", ll, CALIB_VERSION);
	else
		fprintf(fp, " set label %ld '^{ Calibrator v%s (Stefan.Manegold\\@cwi.nl, www.cwi.nl/~manegold) }' at graph 0.5,graph 0.02 center\t# PS\n", ll, CALIB_VERSION);
	fprintf(fp, "#set term gif transparent interlace small size 500, 707 # xFFFFFF x333333 x333333 x0055FF x005522 x660000 xFF0000 x00FF00 x0000FF\t# GIF\n");
	fprintf(fp, "#set output '%s.gif'\t# GIF\n", fn);
	fprintf(fp, "#set title '%s'\t# GIF\n", fn);
	/* ll += 1; */
	if (top)
		fprintf(fp, "#set label %ld    'Calibrator v%s (Stefan.Manegold@cwi.nl, www.cwi.nl/~manegold)'    at graph 0.5,graph 0.97 center\t# GIF\n", ll, CALIB_VERSION);
	else
		fprintf(fp, "#set label %ld    'Calibrator v%s (Stefan.Manegold@cwi.nl, www.cwi.nl/~manegold)'    at graph 0.5,graph 0.03 center\t# GIF\n", ll, CALIB_VERSION);
	fprintf(fp, "set data style linespoints\n");
	fprintf(fp, "set key below\n");
	fprintf(fp, "set xlabel 'spots accessed'\n");
	fprintf(fp, "set x2label ''\n");
	fprintf(fp, "set ylabel 'nanosecs per iteration'\n");
	fprintf(fp, "set y2label 'cycles per iteration'\n");
	fprintf(fp, "set logscale x 2\n");
	fprintf(fp, "set logscale x2 2\n");
	fprintf(fp, "set logscale y 10\n");
	fprintf(fp, "set logscale y2 10\n");
	fprintf(fp, "set format x '%%1.0f'\n");
	fprintf(fp, "set format x2 '%%1.0f'\n");
	fprintf(fp, "set format y '%%1.0f'\n");
	fprintf(fp, "set format y2 ''\n");
	fprintf(fp, "set xrange[%f:%f]\n", xl, xh);
	fprintf(fp, "#set x2range[%f:%f]\n", xl, xh);
	fprintf(fp, "set yrange[%f:%f]\n", yl, yh);
	fprintf(fp, "#set y2range[%f:%f]\n", yl, yh);
	fprintf(fp, "set grid x2tics\n");
	fprintf(fp, "set xtics mirror");
	for (x = 1, l = 1, s = " ("; x <= xh; x *= 2, l++, s = ", ") {
		if (l | 1) {
			if (x >= (1024 * 1024)) {
				fprintf(fp, "%s'%ldM' %ld", s, x / (1024 * 1024), x);
			} else if (x >= 1024) {
				fprintf(fp, "%s'%ldk' %ld", s, x / 1024, x);
			} else {
				fprintf(fp, "%s'%ld' %ld", s, x, x);
			}
		} else {
			fprintf(fp, "%s'' %ld", s, x);
		}
	}
	fprintf(fp, ")\n");
	fprintf(fp, "set x2tics mirror");
	for (l = 0, s = " ("; l < Asso->levels; l++, s = ", ") {
		if (Asso->entries[l] >= (1024 * 1024)) {
			fprintf(fp, "%s'[%ldM]' %ld", s, Asso->entries[l] / (1024 * 1024), Asso->entries[l]);
		} else if (Asso->entries[l] >= 1024) {
			fprintf(fp, "%s'[%ldk]' %ld", s, Asso->entries[l] / 1024, Asso->entries[l]);
		} else {
			fprintf(fp, "%s'[%ld]' %ld", s, Asso->entries[l], Asso->entries[l]);
		}
	}
	fprintf(fp, "%s'<TLB>' %ld)\n", s, Asso->minTLBentries);
	fprintf(fp, "set y2tics");
	s = " (";
/*+
	for (l = 0; l <= Asso->levels; l++, s = ", ") {
		if (!delay)	fprintf(fp, "%s'(%ld)' %f", s, cround(CYperIt(Asso->latency1[l] - delay)), NSperIt(Asso->latency1[l] - delay));
			else	fprintf(fp, "%s'(%ld)' %f", s, cround(CYperIt(Asso->latency2[l] - delay)), NSperIt(Asso->latency2[l] - delay));
	}
+*/
	for (l = 1; l <= cache->levels; l++, s = ", ") {
		if (!delay)
			fprintf(fp, "%s'<L%ld>' %f", s, l, NSperIt(cache->latency1[l] - delay));
		else
			fprintf(fp, "%s'<L%ld>' %f", s, l, NSperIt(cache->latency2[l] - delay));
	}
	for (y = 1; y <= yh; y *= 10, s = ", ") {
		fprintf(fp, "%s'%1.4g' %ld", s, (calibdbl) (y * MHz) / 1000.0, y);
	}
	fprintf(fp, ")\n");
/*+
	for (l = 0; l <= Asso->levels; l++) {
		if (!delay)	z = cround(CYperIt(Asso->latency1[l] - delay)) * 1000.0 / (calibdbl)MHz;
			else	z = cround(CYperIt(Asso->latency2[l] - delay)) * 1000.0 / (calibdbl)MHz;
		ll += 1;
		fprintf(fp, " set label %ld '(%1.4g)  ' at %f,%f right\t# PS\n" , ll, z, xl, z);
		fprintf(fp, "#set label %ld '(%1.4g)'   at %f,%f right\t# GIF\n", ll, z, xl, z);
		fprintf(fp, "set arrow %ld from %f,%f to %f,%f nohead lt 0\n", ll, xl, z, xh, z);
	}
+*/
	for (l = 1; l <= cache->levels; l++) {
		if (!delay)
			z = cround(CYperIt(cache->latency1[l] - delay)) * 1000.0 / (calibdbl) MHz;
		else
			z = cround(CYperIt(cache->latency2[l] - delay)) * 1000.0 / (calibdbl) MHz;
		ll += 1;
		fprintf(fp, " set label %ld '<L%ld>  ' at %f,%f right\t# PS\n", ll, l, xl, z);
		fprintf(fp, "#set label %ld '<L%ld>'   at %f,%f right\t# GIF\n", ll, l, xl, z);
		fprintf(fp, "set arrow %ld from %f,%f to %f,%f nohead lt 0\n", ll, xl, z, xh, z);
	}
	fprintf(fp, "plot \\\n0.1 title 'stride:' with points pt 0 ps 0");
	for (x = 2, l = Asso->levels; x <= xx; x++) {
		fprintf(fp, " , \\\n'%s.data' using 1:($%ld-%f) title '", fn, (6 * (x - 1)) + 1, NSperIt(delay));
/*+
		if ((l > 0) && (result[0][x] == Asso->pagesize[l])) {
			fprintf(fp, "\\{%ld\\}", result[0][x]);
			while ((--l >= 0) && (result[0][x] == Asso->pagesize[l]));
		} else {
+*/
		fprintf(fp, "%ld", result[0][x]);
/*+
		}
+*/
		fprintf(fp, "' with linespoints lt %ld pt %ld", x, x + 2);
	}
	fprintf(fp, "\n");
	fprintf(fp, "set nolabel\n");
	fprintf(fp, "set noarrow\n");
	fflush(fp);

	free(flnm);
}
