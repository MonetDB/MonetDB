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

/***************************************************************************\
*              RUNNING TESTS                                                *
\***************************************************************************/
#define NSperIt(t)	(((calibdbl)(t)) / (((calibdbl)NUMLOADS) / 1000.0))
#define	CYperIt(t)	(((calibdbl)((t) * MHz)) / ((calibdbl)NUMLOADS))

long use_result_dummy;		/* !static for optimizers. */
static void
use_pointer(void *result)
{
	use_result_dummy += (long) result;
}

static void
loads_nodelay(char **P, caliblng * j, caliblng * best)
{
	register char **p = P;
	caliblng tries, i, time;

#define	ONE	 p = (char **)*p;
#define	TEN	 ONE ONE ONE ONE ONE ONE ONE ONE ONE ONE
#define	HUNDRED	 TEN TEN TEN TEN TEN TEN TEN TEN TEN TEN
/*
	#define THOUSAND HUNDRED HUNDRED HUNDRED HUNDRED HUNDRED \
			 HUNDRED HUNDRED HUNDRED HUNDRED HUNDRED
*/
	for (tries = 0; tries < NUMTRIES; ++tries) {
		i = ((*j) * NUMLOADS);
		time = now();
		while (i > 0) {
			HUNDRED i -= 100;
		}
		time = now() - time;
		use_pointer((void *) p);
		if (time <= MINTIME) {
			(*j) *= 2;
			tries--;
		} else {
			time /= (*j);
			if (time < (*best)) {
				(*best) = time;
			}
		}
	}
}

static void
loads_delay(char **P, caliblng * j, caliblng * best)
{
	register char **p = P;
	caliblng tries, i, time;

#define	FILL	 p++; p--;  p++; p--;  p++; p--;  p++; p--;  p++; p--;
#define	ONEx	 p = (char **)*p; \
			 FILL FILL FILL FILL FILL FILL FILL FILL FILL FILL
/*
	#define	TENx	 ONEx ONEx ONEx ONEx ONEx ONEx ONEx ONEx ONEx ONEx
	#define	HUNDREDx TENx TENx TENx TENx TENx TENx TENx TENx TENx TENx
*/

	for (tries = 0; tries < NUMTRIES; ++tries) {
		i = ((*j) * NUMLOADS) / REDUCE;
		time = now();
		while (i > 0) {
			ONEx i -= 1;
		}
		time = now() - time;
		use_pointer((void *) p);
		if (time <= MINTIME) {
			(*j) *= 2;
			tries--;
		} else {
			time *= REDUCE;
			time /= (*j);
			if (time < (*best)) {
				(*best) = time;
			}
		}
	}
}

static caliblng
loads(char *array, caliblng range, caliblng stride, caliblng MHz, FILE *fp, int delay)
{
	register char **p = 0;
	caliblng i, k, j = 1, best = 2000000000;
	caliblng numloads = NUMLOADS / (delay ? REDUCE : 1);

#ifdef CALIBRATOR_PRINT_OUTPUT
	fprintf(stderr, "\r%11ld %11ld %11ld", range, stride, range / stride);
	fflush(stderr);
#endif

	k = range / stride;
	while ((j * numloads) < k)
		j++;

/*
	for (i = stride; i < range; i += stride) {
		p = (char **)&(array[i]);
		*p = &(array[i - stride]);
	}
	p = (char **)&(array[0]);
	*p = &(array[i - stride]);
*/

/*
	for (i = stride; i < range; i += stride);
	k = i - stride;
*/
	k = ((caliblng) ((range - 1) / stride)) * stride;
	for (i = k; i >= stride; i -= stride) {
		p = (char **) &(array[i]);
		*p = &(array[i - stride]);
/*
		if (i < stride) {
			*p = &(array[k]);
		} else {
			*p = &(array[i - stride]);
		}
*/
	}
	p = (char **) &(array[i]);
	*p = &(array[k]);

	if (delay)
		loads_delay(p, &j, &best);
	else
		loads_nodelay(p, &j, &best);

#ifdef CALIBRATOR_PRINT_OUTPUT
	fprintf(stderr, " %11ld %11ld %11ld", j * numloads, (best * j) / (delay ? REDUCE : 1), best);
/*
	if ((j*numloads) < (range/stride)) {
		fprintf(stderr, " %11ld", -1*(j*numloads)*stride);
	} else {
		fprintf(stderr, " %11ld", range);
	}
*/
	fflush(stderr);
#endif
	if (fp) {
		fprintf(fp, " %06ld %05.1f %05.1f", best	/* elapsed time [microseconds] */
			, NSperIt(best)	/* nanoseconds per iteration */
			, CYperIt(best)	/* clocks per iteration */
		    );
		fflush(fp);
	}

	return best;
}

static caliblng **
runCache(char *array, caliblng maxrange, caliblng minstride, caliblng MHz, FILE *fp, caliblng * maxstride)
{
	caliblng i, r, x=0, y, z, range, stride = minstride / 2;
	calibdbl f = 0.25;
	caliblng last, time = 0, **result;
	caliblng pgsz = getpagesize();
	int delay;

#ifdef CALIBRATOR_PRINT_OUTPUT
	if (*maxstride) {
		fprintf(stderr, "analyzing cache latency...\n");
	} else {
		fprintf(stderr, "analyzing cache throughput...\n");
	}
	fprintf(stderr, "      range      stride       spots       loads     brutto-  netto-time\n");
	fflush(stderr);
#endif
	if (!(*maxstride)) {
		for (range = MINRANGE; (range * 2) <= maxrange; range *= 2) ;
		if ((range * 1.25) <= maxrange)
			range *= 1.25;
		do {
			stride *= 2;
			last = time;
			time = loads(array, range, stride, MHz, 0, 0);
			if (!time)
				ErrXit("runCache: 'loads(%x(array), %ld(range), %ld(stride), %ld(MHz), 0(fp), 0(delay))` returned elapsed time of 0us", array, range, stride, MHz);
		} while (
#ifdef EPSILON1
				((fabs(time - last) / (calibdbl) time) > EPSILON1) && (stride <= (maxrange / 2))
#endif
#ifdef EPSILON3
				(fabs(CYperIt(time) - CYperIt(last)) > EPSILON3) && (stride <= (maxrange / 2))
#endif
		    );
		*maxstride = stride;
		delay = 0;
	} else if (*maxstride < 0) {
		*maxstride *= -1;
		delay = 0;
	} else {
		delay = 1;
	}

	for (r = MINRANGE, y = 1; r <= maxrange; r *= 2) {
		for (i = 3; i <= 5; i++) {
			range = r * f * i;
			if ((*maxstride <= range) && (range <= maxrange)) {
				for (stride = *maxstride, x = 1; stride >= minstride; stride /= 2, x++) {
				}
				y++;
			}
		}
	}
	if (!(result = (caliblng **) malloc(y * sizeof(caliblng *))))
		ErrXit("runCache: 'result = malloc(%ld)` failed", y * sizeof(caliblng *));
	for (z = 0; z < y; z++) {
		if (!(result[z] = (caliblng *) malloc(x * sizeof(caliblng))))
			ErrXit("runCache: 'result[%ld] = malloc(%ld)` failed", z, x * sizeof(caliblng));
		memset(result[z], 0, x * sizeof(caliblng));
	}
	result[0][0] = (y << 24) | x;

	if (fp) {
		fprintf(fp, "# Calibrator v%s\n", CALIB_VERSION);
		fprintf(fp, "# (by Stefan.Manegold@cwi.nl, http://www.cwi.nl/~manegold/)\n");
		fprintf(fp, "# ( MINTIME = %ld )\n", MINTIME);
	}

	for (r = MINRANGE, y = 1; r <= maxrange; r *= 2) {
		for (i = 3; i <= 5; i++) {
			range = r * f * i;
			if ((*maxstride <= range) && (range <= maxrange)) {
				result[y][0] = range;
				if (fp) {
					fprintf(fp, "%08.1f %05ld", range / 1024.0	/* range in KB */
						, range / pgsz	/* # pages covered */
					    );
					fflush(fp);
				}
				for (stride = *maxstride, x = 1; stride >= minstride; stride /= 2, x++) {
					if (!result[0][x]) {
						result[0][x] = stride;
					}
					if (fp) {
						fprintf(fp, "  %03ld %09ld %08.1f", stride	/* stride */
							, range / stride	/* # spots accessed  */
							, ((calibdbl) NUMLOADS) / ((calibdbl) (range / stride))	/* # accesses per spot */
						    );
						fflush(fp);
					}
					result[y][x] = loads(array, range, stride, MHz, fp, delay);
				}
				if (fp) {
					fprintf(fp, "\n");
					fflush(fp);
				}
				y++;
			}
		}
	}

#ifdef CALIBRATOR_PRINT_OUTPUT
	fprintf(stderr, "\n\n");
	fflush(stderr);
#endif
	return result;
}

static caliblng **
runTLB(char *array, caliblng maxrange, caliblng minstride, caliblng shift, caliblng mincachelines, caliblng MHz, FILE *fp, caliblng * maxstride)
{
	caliblng i, x=0, y, z, stride, minspots, maxspots, p;
	caliblng range = maxrange, s = minstride / 2, spots = mincachelines / 2;
	calibdbl f = 0.25;
	caliblng tmax, smin, xmin, last, time = 0, **result;
	caliblng pgsz = getpagesize();
	int delay;

#ifdef CALIBRATOR_PRINT_OUTPUT
	fprintf(stderr, "analyzing TLB latency...\n");
	fprintf(stderr, "      range      stride       spots       loads     brutto-  netto-time\n");
	fflush(stderr);
#endif
	if (!(*maxstride)) {
		do {
			s *= 2;
			stride = s + shift;
			range = stride * spots;
			last = time;
			time = loads(array, range, stride, MHz, 0, 0);
			if (!time)
				ErrXit("runTLB: 'loads(%x(array), %ld(range), %ld(stride), %ld(MHz), 0(fp), 0(delay))` returned elapsed time of 0us", array, range, stride, MHz);
		} while (
#ifdef EPSILON1
				(((fabs(time - last) / (calibdbl) time) > EPSILON1) || (stride < (pgsz / 1))) && (range <= (maxrange / 2))
				/*      ((fabs(time - last) / (calibdbl)time) > EPSILON1) && (range <= (maxrange / 2)));        */
#endif
#ifdef EPSILON3
				((fabs(CYperIt(time) - CYperIt(last)) > EPSILON3) || (stride < (pgsz / 1))) && (range <= (maxrange / 2))
				/*      (fabs(CYperIt(time) - CYperIt(last)) > EPSILON3) && (range <= (maxrange / 2))   */
#endif
		    );
		*maxstride = s;
		delay = 0;
	} else {
		delay = 1;
	}
	minspots = MAX(MINRANGE / (minstride + shift), 4);
	maxspots = maxrange / (*maxstride + shift);
/*	maxspots = mincachelines;	*/

	for (p = minspots, y = 2; p <= maxspots; p *= 2) {
		for (i = 3; i <= 5; i++) {
			spots = p * f * i;
			if ((spots * (*maxstride + shift)) <= maxrange) {
				for (s = *maxstride, x = 2; s >= minstride; s /= 2, x++) {
				}
				y++;
			}
		}
	}
	if (!(result = (caliblng **) malloc(y * sizeof(caliblng *))))
		ErrXit("runTLB: 'result = malloc(%ld)` failed", y * sizeof(caliblng *));
	for (z = 0; z < y; z++) {
		if (!(result[z] = (caliblng *) malloc(x * sizeof(caliblng))))
			ErrXit("runTLB: 'result[%ld] = malloc(%ld)` failed", z, x * sizeof(caliblng *));
		memset(result[z], 0, x * sizeof(caliblng));
	}
	result[0][0] = (y << 24) | x;

	if (fp) {
		fprintf(fp, "# Calibrator v%s\n", CALIB_VERSION);
		fprintf(fp, "# (by Stefan.Manegold@cwi.nl, http://www.cwi.nl/~manegold/)\n");
		fprintf(fp, "# ( MINTIME = %ld )\n", MINTIME);
	}

	for (p = minspots, y = 2; p <= maxspots; p *= 2) {
		for (i = 3; i <= 5; i++) {
			spots = p * f * i;
			if ((spots * (*maxstride + shift)) <= maxrange) {
				result[y][0] = spots;
				if (fp) {
					fprintf(fp, "%09ld %08.1f", spots	/* # spots accessed  */
						, ((float) NUMLOADS) / ((float) spots)	/* # accesses per spot */
					    );
					fflush(fp);
				}
				tmax = 0;
				smin = *maxstride + shift;
				xmin = 2;
				for (s = *maxstride, x = 2; s >= minstride; s /= 2, x++) {
					stride = s + shift;
					if (!result[0][x]) {
						result[0][x] = stride;
					}
					range = stride * spots;
					if (fp) {
						fprintf(fp, "  %06ld %08.1f %05ld", stride	/* stride */
							, range / 1024.0	/* range in KB */
							, range / pgsz	/* # pages covered */
						    );
						fflush(fp);
					}
					result[y][x] = loads(array, range, stride, MHz, fp, delay);
					if ((x > 2) && (result[y][x] > tmax)) {
						tmax = result[y][x];
						if (stride < smin) {
							smin = stride;
							xmin = x;
						}
					}
				}
				result[y][1] = tmax;
				result[1][xmin]++;
				if (fp) {
					fprintf(fp, "\n");
					fflush(fp);
				}
				y++;
			}
		}
	}
	xmin = --x;
	for (--x; x >= 2; x--) {
		if (result[1][x] > result[1][xmin]) {
			xmin = x;
		}
	}
	result[0][1] = result[0][xmin];

#ifdef CALIBRATOR_PRINT_OUTPUT
	fprintf(stderr, "\n\n");
	fflush(stderr);
#endif
	return result;
}

static caliblng **
runAsso(char *array, caliblng maxrange, caliblng minstride, caliblng shift, caliblng mincachelines, caliblng MHz, FILE *fp, caliblng * maxstride)
{
	caliblng i, x=0, y, z, stride, minspots, maxspots, p;
	caliblng range = maxrange, s = minstride / 2, spots = mincachelines / 2;
	calibdbl f = 0.25;
	caliblng tmax, smin, xmin, *ymax, last, time = 0, **result;
	caliblng pgsz = getpagesize();
	int delay = 0;

#ifdef CALIBRATOR_PRINT_OUTPUT
/*-
	fprintf(stderr, "runAsso(char *array, caliblng maxrange=%ld, caliblng minstride=%ld, caliblng shift=%ld, caliblng mincachelines=%ld, caliblng MHz=%ld, FILE *fp, caliblng *maxstride=%ld)\n", maxrange, minstride, shift, mincachelines, MHz, *maxstride);
-*/
	fprintf(stderr, "analyzing cache-associativity...\n");
	fprintf(stderr, "      range      stride       spots       loads     brutto-  netto-time\n");
	fflush(stderr);
#endif

	if (!(*maxstride)) {
		do {
			s *= 2;
			stride = s + shift;
			range = stride * spots;
			last = time;
			time = loads(array, range, stride, MHz, 0, 0);
			if (!time)
				ErrXit("runAsso: 'loads(%x(array), %ld(range), %ld(stride), %ld(MHz), 0(fp), 0(delay))` returned elapsed time of 0us", array, range, stride, MHz);
		} while (
#ifdef EPSILON1
				(((fabs(time - last) / (calibdbl) time) > EPSILON1) || (stride < (pgsz / 1))) && (range <= (maxrange / 2))
				/*      ((fabs(time - last) / (calibdbl)time) > EPSILON1) && (range <= (maxrange / 2))  */
#endif
#ifdef EPSILON3
				((fabs(CYperIt(time) - CYperIt(last)) > EPSILON3) || (stride < (pgsz / 1))) && (range <= (maxrange / 2))
				/*      (fabs(CYperIt(time) - CYperIt(last)) > EPSILON3) && (range <= (maxrange / 2))   */
#endif
		    );
		*maxstride = s;
/*
		delay = 0;
	} else {
		delay = 1;
*/
	}
	minspots = MAX(MINRANGE / (minstride + shift), 2);
/*	maxspots = maxrange / (*maxstride + shift);	*/
	maxspots = mincachelines * 2.5;

	for (p = minspots, y = 2; p <= maxspots; p *= 2) {
		for (i = 3; i <= 5; i++) {
			spots = p * f * i;
/*
			if ((spots * (*maxstride + shift)) <= maxrange) {
*/
			for (s = *maxstride, x = 2; s >= minstride; s /= 2, x++) {
/*
					if ((spots * (s + shift)) <= maxrange)	x++;
*/
			}
			y++;
/*
			}
*/
		}
	}
	if (!(result = (caliblng **) malloc(y * sizeof(caliblng *))))
		ErrXit("runAsso: 'result = malloc(%ld)` failed", y * sizeof(caliblng *));
	for (z = 0; z < y; z++) {
		if (!(result[z] = (caliblng *) malloc(x * sizeof(caliblng))))
			ErrXit("runAsso: 'result[%ld] = malloc(%ld)` failed", z, x * sizeof(caliblng *));
		memset(result[z], 0, x * sizeof(caliblng));
	}
	result[0][0] = (y << 24) | x;
	if (!(ymax = (caliblng *) malloc(x * sizeof(caliblng))))
		ErrXit("runAsso: 'ymax = malloc(%ld) failed", x * sizeof(caliblng *));
	memset(ymax, 0, x * sizeof(caliblng));

	if (fp) {
		fprintf(fp, "# Calibrator v%s\n", CALIB_VERSION);
		fprintf(fp, "# (by Stefan.Manegold@cwi.nl, http://www.cwi.nl/~manegold/)\n");
		fprintf(fp, "# ( MINTIME = %ld )\n", MINTIME);
	}

	for (p = minspots, y = 2; p <= maxspots; p *= 2) {
		for (i = 3; i <= 5; i++) {
			spots = p * f * i;
/*
			if ((spots * (*maxstride + shift)) <= maxrange) {
*/
			result[y][0] = spots;
			if (fp) {
				fprintf(fp, "%09ld %08.1f", spots	/* # spots accessed  */
					, ((float) NUMLOADS) / ((float) spots)	/* # accesses per spot */
				    );
				fflush(fp);
			}
			tmax = 0;
			smin = *maxstride + shift;
			xmin = 2;
			for (s = *maxstride, x = 2; s >= minstride; s /= 2, x++) {
				stride = s + shift;
				if (!result[0][x]) {
					result[0][x] = stride;
				}
				range = stride * spots;
				if (fp) {
					fprintf(fp, "  %06ld %08.1f %05ld", stride	/* stride */
						, range / 1024.0	/* range in KB */
						, range / pgsz	/* # pages covered */
					    );
					fflush(fp);
				}
				if (stride <= (maxrange / spots)) {
					result[y][x] = loads(array, range, stride, MHz, fp, delay);
					ymax[x] = y;
				} else {
					result[y][x] = 0;
					if (fp) {
						fprintf(fp, " %06ld %05.1f %05.1f", result[ymax[x]][x]	/* elapsed time [microseconds] */
							, NSperIt(result[ymax[x]][x])	/* nanoseconds per iteration */
							, CYperIt(result[ymax[x]][x])	/* clocks per iteration */
						    );
						fflush(fp);
					}
				}
				if (result[y][x] > tmax) {
					tmax = result[y][x];
					if (stride < smin) {
						smin = stride;
						xmin = x;
					}
				}
			}
			result[y][1] = tmax;
			result[1][xmin]++;
			if (fp) {
				fprintf(fp, "\n");
				fflush(fp);
			}
			y++;
/*
			}
*/
		}
	}
	xmin = --x;
	for (--x; x >= 2; x--) {
		if (result[1][x] > result[1][xmin]) {
			xmin = x;
		}
	}
	result[0][1] = result[0][xmin];

#ifdef CALIBRATOR_PRINT_OUTPUT
	fprintf(stderr, "\n\n");
	fflush(stderr);
#endif

	return result;
}
