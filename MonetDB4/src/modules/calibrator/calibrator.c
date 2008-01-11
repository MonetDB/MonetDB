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

/*
 * Calibrator v0.9i
 * by Stefan.Manegold@cwi.nl, http://www.cwi.nl/~manegold/Calibrator/
 *
 * All rights reserved.
 * No warranties.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above notice, this list
 *    of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above notice, this
 *    list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *	This product includes software developed by
 *	Stefan.Manegold@cwi.nl, http://www.cwi.nl/~manegold/.
 * 4. Any publication of result obtained by use of this software must
 *    display a reference as follows:
 *	Results produced by Calibrator v0.9i
 *	(Stefan.Manegold@cwi.nl, http://www.cwi.nl/~manegold/)
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS `AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "monetdb4_config.h"
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif
#include <stdio.h>
#include <math.h>
#ifdef HAVE_STRING_H
#include <string.h>
#else
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif
#endif
#include <stdarg.h>
#ifdef HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif

#define NUMLOADS 100000
#define REDUCE	 10
#define	NUMTRIES 3
#define	MINRANGE 1024
#define MAXLEVELS 9
#define LENPLATEAU 3

#define EPSILON1 0.1
/*	#define EPSILON3 2.0	*/

/*	#define EPSILON2 0.04	*/
#define EPSILON4 1.0


#define CALIBRATOR_CREATE_PLOTS
/* #undef CALIBRATOR_CREATE_PLOTS */
#define CALIBRATOR_PRINT_OUTPUT
/* #undef CALIBRATOR_PRINT_OUTPUT */
#define CALIBRATOR_CHECK_SMP
/* #undef CALIBRATOR_CHECK_SMP */

#include "calib_common.c"
caliblng MINTIME = 10000;

#include "calib_run.c"
#include "calib_analyse.c"
#include "calib_plot.c"
#include "calib_print.c"
#include "calib_main.c"

static char
last(char *s)
{
	while (*s++) ;
	return (s[-2]);
}

static caliblng
bytes(char *s)
{
	caliblng n = atoi(s);

	if ((last(s) == 'k') || (last(s) == 'K'))
		n *= 1024;
	if ((last(s) == 'm') || (last(s) == 'M'))
		n *= (1024 * 1024);
	if ((last(s) == 'g') || (last(s) == 'G'))
		n *= (1024 * 1024 * 1024);
	return (n);
}

int
main(int ac, char **av)
{
	caliblng MHz, maxrange;
	struct fullInfo *caliInfo;

	fprintf(stdout, "\nCalibrator v%s\n(by Stefan.Manegold@cwi.nl, http://www.cwi.nl/~manegold/)\n", CALIB_VERSION);
	fflush(stdout);

	if (ac < 4)
		ErrXit("usage: '%s <MHz> <size>[k|M|G] <filename>`", av[0]);

	MHz = atoi(av[1]);
	maxrange = bytes(av[2]) * 1.25;

/*
	if (ac > 4) align = atoi(av[4]) % pgsz;
	if (ac > 5) maxCstride = -1 * abs(atoi(av[5]));
*/

	caliInfo = mainRun(MHz, maxrange, av[3]);
	printCPU(caliInfo->cache, MHz, caliInfo->delayC);
	printCache(caliInfo->cache, caliInfo->Asso, MHz);
	printTLB(caliInfo->TLB, MHz);

#ifdef CALIBRATOR_CHECK_SMP
	printf("SMP: CPUs found: %ld\n", caliInfo->smp.nrCpus);
#endif

	freeFullInfo(caliInfo);

	return (0);
}
