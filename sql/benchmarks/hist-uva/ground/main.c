/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/param.h>
#include <sys/times.h>
#include <limits.h>
#include <unistd.h>


struct tms cpuTime;
time_t sysTime;
time_t usrTime;

void
startTime()
{
    times(&cpuTime);
    usrTime = cpuTime.tms_utime;
    sysTime = cpuTime.tms_stime;
}

double
lastTime()
{
    return (double) (usrTime+sysTime) / HZ;
}

void
stopTime(int print)
{
    times(&cpuTime);
    usrTime = cpuTime.tms_utime - usrTime;
    sysTime = cpuTime.tms_stime - sysTime;
	if (print) {
        printf("usr: %d, sys: %d, time: %f\n", usrTime, sysTime, lastTime());
	}
}


int entries = 93390;
int bins = 256;
int nElem = 23907840;
float *data = new float[23907840];
char filename[80];

void
fillData()
{
	for (int e=0 ; e<entries ; e++) {
		float *ent = data + e*bins;
		for (int b=0 ; b<bins ; b++) {
			ent[b] = b + (float)b/100;
		}
		ent[0] = e;
	}
}

void
printSomeData(int column, int beginRow, int endRow)
{
	int nRow = endRow - beginRow;
	for (int r=0 ; r<nRow ; r++) {
		float *ent = data + r*bins + beginRow*bins;
		printf("row %d col %d = %f\n", beginRow + r, column, ent[column]);
	}
}

void
sumCol1()
{
	double sum = 0;
	for (int e=0 ; e<entries ; e++) {
		float *ent = data + e*bins;
		sum += ent[0];
	}
	printf("sumCol1 : %f\n", sum);
}

void
sumCol2()
{
	double sum = 0;
	for (int e=0 ; e<entries ; e++) {
		float *ent = data + e*bins;
		sum += ent[1];
	}
	printf("sumCol2 : %f\n", sum);
}

void
sum5Cols()
{
	double sum = 0;
	for (int e=0 ; e<entries ; e++) {
		float *ent = data + e*bins;
		sum += ent[0] + ent[49] + ent[99] + ent[149] + ent[199];
	}
	printf("sum5Cols : %f\n", sum);
}

void
sum256Cols()
{
	double sum[256];
	for (int i=0 ; i<bins ; i++)
		sum[i] = 0;
	for (int e=0 ; e<entries ; e++) {
		float *ent = data + e*bins;
		for (int i=0 ; i<bins ; i++)
			sum[i] += ent[i];
	}
	printf("sum256Cols: sum0 : %f, sum1 : %f\n", sum[0], sum[1]);
}

void
sumCols(int number, int incr)
{
	double sum[256];
	for (int i=0 ; i<bins ; i++)
		sum[i] = 0;
	for (int e=0 ; e<entries ; e++) {
		float *ent = data + e*bins;
		int nr = 0;
		for (int i=0 ; i<number ; i++) {
			sum[i] += ent[nr];
			nr += incr;
		}
	}
	printf("sum%dCols: sum0 : %f\n", number, sum[0]);
}

void
makeGround()
{

	FILE *fp = fopen(filename, "wb");
	if (!fp) {
		printf("Open failed");
		exit(-1);
	}

	size_t nWritten;
	nWritten = fwrite(data, sizeof(float), nElem, fp);
	if (nWritten != nElem) {
		printf("Wrote %d instead of %d elements.\n", nWritten, nElem);
	}

	fclose(fp);
}

void
makeGround2()
{
	int i,j;
	float *p = data;
	FILE *fp = fopen(filename, "w");
	if (!fp) {
		printf("Open failed");
		exit(-1);
	}

	for(j=0;j<entries;j++){
		for(i=0;i<(bins-1);i++){
			fprintf(fp, "%f,", p[i]);
		}
		fprintf(fp, "%f\n", p[bins-1]);
		p+=bins;
	}
	fclose(fp);
}

void
readGround()
{
	char filename[80];
	sprintf(filename, "ground_%d_%d", entries, bins);

	FILE *fp = fopen(filename, "rb");
	if (!fp) {
		printf("Open failed");
		exit(-1);
	}

	size_t nRead;
	nRead = fread(data, sizeof(float), nElem, fp);
	if (nRead != nElem) {
		printf("Read %d instead of %d elements.\n", nRead, nElem);
	}

	fclose(fp);
}

int
main()
{
	data = new float[23907840];
	sprintf(filename, "ground_%d_%d", entries, bins);

	startTime();
	fillData();
	makeGround2();
	exit(0);
	/*
	*/

	//
	readGround();
	stopTime(1);

	/*
	printSomeData(0, 0, 10);
	printSomeData(1, 0, 10);
	printSomeData(0, 100, 110);
	printSomeData(1, 100, 110);
	printSomeData(90, 100, 110);
	*/

	startTime();
	sumCol1();
	stopTime(1);

	startTime();
	sum5Cols();
	stopTime(1);

	startTime();
	sumCols(5, 50);
	stopTime(1);

	startTime();
	sumCols(25, 10);
	stopTime(1);

	startTime();
	sumCols(50, 5);
	stopTime(1);

	startTime();
	sumCols(100, 2);
	stopTime(1);

	startTime();
	sumCols(150, 1);
	stopTime(1);

	startTime();
	sumCols(200, 1);
	stopTime(1);

	startTime();
	sum256Cols();
	stopTime(1);

	return 0;
}
