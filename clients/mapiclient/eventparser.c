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
 * Copyright August 2008-2015 MonetDB B.V.
 * All Rights Reserved.
 */

/* (c) M Kersten, S Manegold */

#include "eventparser.h"

char *statenames[]= {"","start","done","action","ping","wait","iostat","gccollect"};

char *malarguments[MAXMALARGS];
int malargtop;
int debug;

static void
parseArguments(char *call)
{
	int i;
	char  *c = call, *l, ch;
	
	malargtop = 0;
	if( debug)
		fprintf(stderr,"call:%s\n",call);
	for( ; c && *c && malargtop < MAXMALARGS;  c++){
		if (*c ==')')
			break;
		if (*c == ',')
			continue;
		if (*c == 'X'){
			// skip variable
			c= strchr(c,'=');
			if( c == 0)
				break;
			c++;
			if( debug)
				fprintf(stderr,"arg:%s\n",c);
		}
		if (*c== '\\' && *(c+1) =='"'){
			c++; c++;
			// parse string skipping escapes
			for(l=c; *l; l++){
				if( *l =='\\' &&  *(l+1) =='"') break;
				if( *l == '\\') l++;
			}
			*l= 0;
			malarguments[malargtop++] = strdup(c);
			c= l+1;
		} else {
			l = strchr(c, ch = ',');
			if( l == 0){
				l = strchr(c, ch = ')');
				break;
				}
			*l=0;
			malarguments[malargtop++] = strdup(c);
			*l=ch;
			c= l;
			l = strchr(malarguments[malargtop-1],')');
			if(l)
				*l =0;
		}
	}
	if( debug)
	for(i=0; i < malargtop; i++)
		fprintf(stderr,"arg[%d] %s\n",i,malarguments[i]);
}
int
eventparser(char *row, EventRecord *ev)
{
#ifdef HAVE_STRPTIME
	char *c, *cc, *v =0;
	struct tm stm;

	/* check basic validaty first */
	if (row[0] =='#')
		return 0;
	if (row[0] != '[')
		return -1;
	if ((cc= strrchr(row,']')) == 0 || *(cc+1) !=0)
		return -1;

	/* scan event record number */
	c = row+1;
	if (c == 0)
		return -2;
	ev->eventnr = atoi(c + 1);

	/* scan event time" */
	c = strchr(c + 1, '"');
	if (c) {
		/* convert time to epoch in seconds*/
		cc =c;
		memset(&stm, 0, sizeof(struct tm));
		c = strptime(c + 1, "%H:%M:%S", &stm);
		ev->clkticks = (((lng) stm.tm_hour * 60 + stm.tm_min) * 60 + stm.tm_sec) * 1000000;
		if (c == 0)
			return -3;
		if (*c == '.') {
			lng usec;
			/* microseconds */
			usec = strtoll(c + 1, NULL, 10);
			assert(usec >= 0 && usec < 1000000);
			ev->clkticks += usec;
		}
		c = strchr(c + 1, '"');
		if (ev->clkticks < 0) {
			fprintf(stderr, "parser: read negative value "LLFMT" from\n'%s'\n", ev->clkticks, cc);
		}
		c++;
	} else
		return -3;

	/* skip pc tag */
	{	// decode pc
		c= strchr(c+1,'[');
		if( c == 0)
			return -4;
		ev->pc = atoi(c+1);
	}
	c = strchr(c+1, ',');
	if (c == 0)
		return -4;

	/* scan thread */
	ev->thread = atoi(c+1);

	/* scan status */
	c = strchr(c, '"');
	if (c == 0)
		return -5;
	if (strncmp(c + 1, "start", 5) == 0) {
		ev->state = START;
		c += 6;
	} else if (strncmp(c + 1, "done", 4) == 0) {
		ev->state = DONE;
		c += 5;
	} else if (strncmp(c + 1, "ping", 4) == 0) {
		ev->state = PING;
		c += 5;
	} else if (strncmp(c + 1, "stat", 4) == 0) {
		ev->state = IOSTAT;
		c += 6;
	} else if (strncmp(c + 1, "wait", 4) == 0) {
		ev->state = WAIT;
		c += 5;
	} else {
		ev->state = 0;
		c = strchr(c + 1, '"');
		if (c == 0)
			return -5;
	}


	/* scan usec */
	c = strchr(c + 1, ',');
	if (c == 0)
		return -6;
	ev->ticks = strtoll(c + 1, NULL, 10);

	/* scan rssMB */
	c = strchr(c + 1, ',');
	if (c == 0)
		return -7;
	ev->memory = strtoll(c + 1, NULL, 10);

	/* scan vmMB */
	c = strchr(c + 1, ',');
	if (c == 0)
		return -8;
	ev->vmmemory = strtoll(c + 1, NULL, 10);

#ifdef NUMAPROFILING
	for(; *c && *c !='"'; c++) ;
	ev->numa = c+1;
	for(c++; *c && *c !='"'; c++)
		;
	if (*c == 0)
		return -1;
	*c = 0;
	ev->numa= strdup(numa);
	*c = '"';
#endif

	/* scan inblock */
	c = strchr(c + 1, ',');
	if (c == 0) 
		return -9;
	ev->inblock = strtoll(c + 1, NULL, 10);

	/* scan oublock */
	c = strchr(c + 1, ',');
	if (c == 0) 
		return -10;
	ev->oublock = strtoll(c + 1, NULL, 10);

	/* scan majflt */
	c = strchr(c + 1, ',');
	if (c == 0) 
		return -11;
	ev->majflt = strtoll(c + 1, NULL, 10);

	/* scan swaps */
	c = strchr(c + 1, ',');
	if (c == 0) 
		return -12;
	ev->swaps = strtoll(c + 1, NULL, 10);

	/* scan context switches */
	c = strchr(c + 1, ',');
	if (c == 0) 
		return -13;
	ev->csw = strtoll(c + 1, NULL, 10);

	/* parse the MAL call, check basic validity */
	c = strchr(c, '"');
	if (c == 0)
		return -15;
	c++;
	ev->fcn = strdup(c);
	ev->stmt = strdup(ev->fcn);
	c= ev->fcn;
	if( *c != '[')
	{
		c = strstr(c + 1, ":=");
		if (c) {
			ev->fcn = c + 2;
			/* find genuine function calls */
			while (isspace((int) *ev->fcn) && *ev->fcn)
				ev->fcn++;
			if (strchr(ev->fcn, '.') == 0) 
				ev->fcn = 0;
		} 
		if( ev->fcn){
			v=  strchr(ev->fcn+1,';');
			if ( v ) *v = 0;
		}
	}

	if (ev->fcn && (v=strchr(ev->fcn, '(')))
		*v = 0;
	if( v)
		parseArguments(v+1);
	if (ev->stmt && (v=strstr(ev->stmt, ";\",\t")))
		*v = 0;
#else
	(void) row;
#endif
	return 0;
}
