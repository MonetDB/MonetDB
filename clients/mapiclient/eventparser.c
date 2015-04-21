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
char *maltypes[MAXMALARGS];
int malpc[MAXMALARGS];
int malcount[MAXMALARGS];
int malargc;
int malretc;
char *malvariables[MAXMALARGS];
int malvartop;
int debug=0;
char *monetdb_characteristics;

void
clearArguments(void)
{
	int i;
	for(i = 0; i < malargc; i++)
	if( malarguments[i]){
		free(malarguments[i]);
		malarguments[i] = 0;
	}
	malargc = 0;
	malretc = 0;
	for(i = 0; i < malvartop; i++)
	if( malvariables[i]){
		free(malvariables[i]);
		malvariables[i] = 0;
		free(maltypes[i]);
		maltypes[i] = 0;
	}
	malvartop = 0;
}

static void
parseArguments(char *call, int m)
{
	int i, argc= m < 0? -1:0;
	char  *c = call, *l, ch;
	char *v, *w;
	
	if( debug)
		fprintf(stderr,"%s\n",call);
	// also skip keywords
	if( strncmp(c,"function ",10) == 0 ) c +=10;
	if( strncmp(c,"end ",4) == 0 ) c +=4;
	if( strncmp(c,"barrier ",8) == 0 ) c +=8;
	if( strncmp(c,"redo ",5) == 0 ) c +=5;
	if( strncmp(c,"leave ",6) == 0 ) c +=6;
	if( strncmp(c,"return ",7) == 0 ) c +=7;
	if( strncmp(c,"yield ",6) == 0 ) c +=6;
	if( strncmp(c,"catch ",6) == 0 ) c +=6;
	if( strncmp(c,"raise ",6) == 0 ) c +=6;

	if( *c == '(') c++;
	if (*c !=')')
	for( ; c && *c && malargc < MAXMALARGS;  c++){
		if(isalpha((int)*c) &&  strncmp(c,"nil",3) && strncmp(c,"true",4) && strncmp(c,"false",5) ){
			// remember variable in its own structure, it ends with :=
			v=  c;
			c= strchr(c,'=');
			if( c == 0)
				break;
			*c = 0;
			malvariables[malvartop++] = strdup(v);
			c++;
		}
		// all arguments have a value
		if (*c && *c== '\\' && *(c+1) =='"'){
			c++; c++;
			// parse string skipping escapes
			for(l=c; *l; l++){
				if( *l =='\\' &&  *(l+1) =='"') break;
				if( *l == '\\') l++;
			}
			*l= 0;
			malarguments[malargc] = strdup(c);
			c= l+1;
			// we skip to the type or end of term
			while( *c && *c != ':' && *c !=',' && *c !=')' && *c != ';') c++;
		} else if(*c =='<') {
			// extract BAT count
			l=c;
			while(*c && *c != '>') c++;
			c++;
			*c =0;
			malarguments[malargc] = strdup(l);
			*c = '[';
			malcount[malargc]=atoi(c+1);
			while( *c && *c != ':' && *c !=',' && *c !=')' && *c != ';') c++;
			if( *c == ';') break;
		} else if(*c) {
			l=c;
			while( *c && *c != ':' && *c !=',' && *c !=')' && *c != ';') c++;
			ch = *c;
			*c=0;
			malarguments[malargc] = strdup(l);
			*c = ch;
			if( ch == ';') break;
		}
		// consume the type and bat count
		if (*c == ':'){
			if( strncmp(c,":bat[:oid,:",11) == 0){
				c+= 10;
				*c = '[';
				w=c;
				while(*c && *c != ']') c++;
			} else
				w= c+1;
			while(*c && *c != ';' && *c != '{' && *c != ',' && *c != ')' ) c++;
			ch = *c;
			*c =0;
			malpc[malargc] = argc;
			argc+= m;
			maltypes[malargc++] = strdup(w);
			*c = ch;
			if( ch == ';') break;
		} else malargc++;
		if( *c == '{') {
			while(*c && *c != '}') c++;
			c++;
		}
		if (*c == 0 || *c ==')' )
			break;
	}
	if( debug){
		for(i=0; i < malargc; i++)
			fprintf(stderr,"arg[%d] %s %s %d\n",i,malarguments[i], maltypes[i], malcount[i]);
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

	malargc = 0;
	malvartop = 0;
	memset(malarguments, 0, sizeof(malarguments));
	memset(malvariables, 0, sizeof(malvariables));
	/* check basic validaty first */
	if (row[0] =='#'){
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
	if( ev->state == MDB_SYSTEM){
		monetdb_characteristics = strdup(ev->stmt);
	} else
	if( *c != '[')
	{
		v=c;
		c = strstr(c + 1, ":= ");
		if (c) {
			*c = 0;
			parseArguments( (*v == '('? v++:v),-1);
			malretc =malargc;
			*c=':';
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

	if (ev->fcn && (v=strchr(ev->fcn, '('))){
		*v = 0;
		if( v)
			parseArguments(v+1,1);
	 } else { //assigment statements
		v= ev->stmt;
		v = strstr(ev->stmt, ":= ");
		if( v)
			parseArguments(v+3,1);
	}
	if (ev->stmt && (v=strstr(ev->stmt, ";\",\t")))
		*v = 0;
#else
	(void) row;
#endif
	return 0;
}
