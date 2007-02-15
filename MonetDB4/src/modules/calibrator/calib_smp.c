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
*                     SMP CHECK                                             *
\***************************************************************************/
typedef struct {
	caliblng nrCpus;
} SMPinfo;

#define SMP_TOLERANCE 0.30
#define SMP_ROUNDS 1024*1024*64
static caliblng
checkNrCpus(void)
{
	caliblng i, curr = 1, lasttime = 0, thistime, cpus=0;
	struct timeval t0, t1;

	for (;;) {
#ifdef CALIBRATOR_PRINT_OUTPUT
		printf("SMP: Checking %ld CPUs\n", curr);
#endif
		gettimeofday(&t0, 0);
		for (i = 0; i < curr; i++) {
			switch (fork()) {
			case -1:
				fatalex("fork");
			case 0:
			{
				unsigned int s = 1, r;

				for (r = 0; r < SMP_ROUNDS; r++)
					s = s * r + r;
				exit(0);

			}
			}
		}
		for (i = 0; i < curr; i++)
			wait(0);
		gettimeofday(&t1, 0);
		thistime = ((t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec);
		if (lasttime > 0 && (float) thistime / lasttime > 1 + SMP_TOLERANCE)
			break;
		lasttime = thistime;
		cpus = curr;
		curr *= 2;	/* only check for powers of 2 */
	}
#ifdef CALIBRATOR_PRINT_OUTPUT
	printf("\n");
#endif
	return cpus;
}

static void
checkSMP(SMPinfo * smp)
{
	smp->nrCpus = checkNrCpus();
}
