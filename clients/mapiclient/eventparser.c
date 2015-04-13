/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/* (c) M Kersten, S Manegold */

#include "eventparser.h"

char *statenames[]= {"","start","done","action","ping","wait","system"};

char *malarguments[MAXMALARGS];
int malargtop;
char *malvariables[MAXMALARGS];
int malvartop;
int debug;
char *monet_characteristics;

void
clearArguments(void)
{
	int i;
	for(i = 0; i < malargtop; i++)
	if( malarguments[i])
		free(malarguments[i]);
	malargtop = 0;
	for(i = 0; i < malvartop; i++)
	if( malvariables[i])
		free(malvariables[i]);
	malvartop = 0;
}

static void
parseArguments(char *call)
{
	int i;
	char  *c = call, *l, ch;
	char *v, *w;
	
	malargtop = 0;
	malvartop = 0;
	if( debug)
		fprintf(stderr,"%s\n",call);
	memset(malarguments, 0, sizeof(malarguments));
	memset(malvariables, 0, sizeof(malvariables));
	for( ; c && *c && malargtop < MAXMALARGS;  c++){
		if (*c ==')')
			break;
		if (*c == ',')
			continue;
		if(isalpha((int)*c) &&  strncmp(c,"nil",3) && strncmp(c,"true",4) && strncmp(c,"false",5) ){
			// remember variable in its own structure
			v=  c;
			c= strchr(c,'=');
			if( c == 0)
				break;
			*c = 0;
			malvariables[malvartop++] = strdup(v);
			c++;
		}
		if (*c && *c== '\\' && *(c+1) =='"'){
			c++; c++;
			// parse string skipping escapes
			for(l=c; *l; l++){
				if( *l =='\\' &&  *(l+1) =='"') break;
				if( *l == '\\') l++;
			}
			*l= 0;
			malarguments[malargtop++] = strdup(c);
			c= l+1;
			// we could find a type descriptor here, which we skip
			while( *c && *c !=',' && *c !=')') c++;
		} else if(*c) {
			l = strchr(c, ch = ',');
			if( l == 0){
				l = strchr(c, ch = ')');
				break;
				}
			*l=0;
			// we could find a type descriptor as well, which we skip
			w= strchr(c,':');
			if( w) *w = 0;
			malarguments[malargtop++] = strdup(c);
			*l=ch;
			c= l;
			l = strchr(malarguments[malargtop-1],')');
			if(l)
				*l =0;
		}
	}
	if( debug){
		for(i=0; i < malargtop; i++)
			fprintf(stderr,"arg[%d] %s\n",i,malarguments[i]);
		for(i=0; i < malvartop; i++)
			fprintf(stderr,"var[%d] %s\n",i,malvariables[i]);
	}
}
int
eventparser(char *row, EventRecord *ev)
{
#ifdef HAVE_STRPTIME
	char *c, *cc, *v =0;
	struct tm stm;

	/* check basic validaty first */
	if (row[0] =='#'){
		if( row[1] =='{')
			monet_characteristics = strdup(row+1);
		return 0;
	}
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
	{	// decode qry[pc]tag
		char *nme = c;
		c= strchr(c+1,'[');
		if( c == 0)
			return -4;
		*c = 0;
		ev->blk= strdup(nme);
		*c = '[';
		ev->pc = atoi(c+1);
		c= strchr(c+1,']');
		if ( c == 0)
			return -4;
		ev->tag = atoi(c+1);
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
		ev->state = MDB_START;
		c += 6;
	} else if (strncmp(c + 1, "done", 4) == 0) {
		ev->state = MDB_DONE;
		c += 5;
	} else if (strncmp(c + 1, "ping", 4) == 0) {
		ev->state = MDB_PING;
		c += 5;
	} else if (strncmp(c + 1, "system", 6) == 0) {
		ev->state = MDB_SYSTEM;
		c += 5;
	} else if (strncmp(c + 1, "wait", 4) == 0) {
		ev->state = MDB_WAIT;
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

	/* scan tmpMB */
	c = strchr(c + 1, ',');
	if (c == 0)
		return -8;
	ev->tmpspace = strtoll(c + 1, NULL, 10);

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
