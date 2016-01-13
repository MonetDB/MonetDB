/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/* (c) M Kersten */

#include "monetdb_config.h"
#include "eventparser.h"

char *statenames[]= {"","start","done","action","ping","wait","system"};

char *maltypes[MAXMALARGS];
char *malvariables[MAXMALARGS];
char *malvalues[MAXMALARGS];

int malsize;
int debug=0;
char *currentquery=0;
int eventcounter = 0;

#ifndef HAVE_STRPTIME
extern char *strptime(const char *, const char *, struct tm *);
#include "strptime.c"
#endif

static void
clearArguments(void)
{
	int i;

	if( currentquery){
		free(currentquery);
		currentquery = 0;
	}

	for(i = 0; i < MAXMALARGS; i++){
		if( malvariables[i]){
			free(malvariables[i]);
			malvariables[i] = 0;
		}
		if( malvalues[i]){
			free(malvalues[i]);
			malvalues[i] = 0;
		}
		if( maltypes[i]){
			free(maltypes[i]);
			maltypes[i] = 0;
		}
	}
}

static void
dumpArguments(void)
{
	int i;
	for( i=0; i < MAXMALARGS; i++)
	if( maltypes[i])
		printf("[%d] variable %s value %s type %s\n", i, (malvariables[i]?malvariables[i]:""), malvalues[i], maltypes[i]);
}

char * 
stripQuotes(char *currentquery)
{
	const char *c;
	char *q, *qry;
	if( currentquery ==0)
		return NULL;
	q = qry = (char *) malloc(strlen(currentquery) * 2);
	if( q == NULL){
		fprintf(stderr,"Could not allocate query buffer of size "SZFMT"\n", strlen(currentquery) * 2);
		exit(-1);
	}
	c= currentquery;
	if( *c == '"') c++;
	for (; *c; ){
		if ( strncmp(c,"\\\\t",3) == 0){
			*q++ = '\t';
			c+=3;
		} else
			if ( strncmp(c,"\\\\n",3) == 0){
				*q++ = '\n';
				c+=3;
			} else if ( strncmp(c,"\\\"",2) == 0){
				*q++= '"';
				c+=2;
			} else if ( strncmp(c,"\\\\",2) == 0){
				c+= 2;
			} else *q++ = *c++;
	}
	*q =0;
	return qry;
}
 

void
resetEventRecord(EventRecord *ev)
{
	if( ev->version) free(ev->version);
	if( ev->release) free(ev->release);
	if( ev->memory) free(ev->memory);
	if( ev->threads) free(ev->threads);
	if( ev->host) free(ev->host);
	if( ev->package) free(ev->package);

	if( ev->function) free(ev->function);
	if( ev->user) free(ev->user);
	if( ev->time) free(ev->time);
	if( ev->stmt) free(ev->stmt);
	if( ev->fcn) free(ev->fcn);
	if( ev->numa) free(ev->numa);
	memset( (char*) ev, 0, sizeof(EventRecord));
	ev->eventnr = -1;
	clearArguments();
}

/* simple json key:value object parser for event record.
 * each event pair on a single row, which is required for dealing with string values
 * It avoids lots of escaped charactor recognition, simply take with mserver delivers
 * Returns 1 if the closing bracket is found. 0 to continue, -1 upon error
 */

#define skipto(C) { while(*c && *c != C) c++; if (*c != C) return -1;}
#define skipstr() { while (*c && *c !='"') {if (*c =='\\') c++;if(*c)c++;} if (*c != '"') return -1;}

/*
 * The decomposition of the argument components is postponed
 * We just keep the concatenated json string
 */

static int 
parseArgument(char *txt, EventRecord *ev)
{
	char *s,*t;
	int i=0;
	// assume single strictly formatted key-value list line
	(void) txt;
	(void) ev;
	s= strstr(txt,"index\":\"");
	if( s){
		i = atoi(s + 8);
		if( i <0 || i >= MAXMALARGS )
			return 0;
	}
	t= strstr(txt,"name\":\"");
	s= strstr(txt,"\",value\":\"");
	if( s && t){
		t+= 7;
		*s =0;
		malvariables[i] = strdup(t);
		s+= 10;
	}
	t= strstr(txt,"\",type\":\"");
	if( s && t){
		*t = 0;
		*s =0;
		malvariables[i] = strdup(t);
		t+= 7;
	}
	if( s && t){
		*t = 0;
		t+= 9;
		s+= 10;
		maltypes[i] = strdup(t);
	}
	return 0;
}

int
keyvalueparser(char *txt, EventRecord *ev)
{
	char *c, *s, *key, *val;
	struct tm stm;

	c= txt;

	if( strstr(c,"\"argument\":") || strstr(c,"\"result\":"))
		return parseArgument(txt,ev);
	if( *c == '{'){
		resetEventRecord(ev);
		memset(malvariables, 0, sizeof(malvariables));
		memset(malvalues, 0, sizeof(malvalues));
		ev->eventnr= eventcounter++;
		return 0;
	}
	if( *c == '}'){
		dumpArguments();
		return 1;
	}

	skipto('"');
	key = ++c;
	skipstr();
	*c++ = 0;
	skipto(':');
	c++;
	while( *c && isspace((int)*c)) c++;
	if( *c == '"'){
		val = ++c;
		skipstr();
		*c = 0;
	} else val =c;

	if( strstr(key,"clk")){
		ev->clk = atol(val); 
		return 0;
	}
	if( strstr(key,"ctime")){
		/* convert time to epoch in seconds*/
		ev->time= strdup(val);
		memset(&stm, 0, sizeof(struct tm));
		c = strptime(val + 1, "%H:%M:%S", &stm);
		ev->clkticks = (((lng) stm.tm_hour * 60 + stm.tm_min) * 60 + stm.tm_sec) * 1000000;
		c=  strchr(val,(int)'.');
		if (*c == '.') {
			lng usec;
			/* microseconds */
			usec = strtoll(c + 1, NULL, 10);
			assert(usec >= 0 && usec < 1000000);
			ev->clkticks += usec;
		}
		if (ev->clkticks < 0) {
			fprintf(stderr, "parser: read negative value "LLFMT" from\n'%s'\n", ev->clkticks, val);
		}
		return 0;
	}
	if( strstr(key,"function")){ ev->function= strdup(val); return 0;}
	if( strstr(key,"user")){ ev->user= strdup(val); return 0;}
	if( strstr(key,"tag")){ ev->tag= atoi(val); return 0;}
	if( strstr(key,"thread")){ ev->thread= atoi(val); return 0;}
	if( strstr(key,"pc")){ ev->pc= atoi(val); return 0;}
	if( strstr(key,"state")){
		if( strstr(val,"start")) ev->state= MDB_START;
		if( strstr(val,"done")) ev->state= MDB_DONE;
		if( strstr(val,"ping")) ev->state= MDB_PING;
		if( strstr(val,"wait")) ev->state= MDB_WAIT;
		if( strstr(val,"system")) ev->state= MDB_SYSTEM;
		return 0;
	}

	if( strstr(key,"usec")) { ev->ticks= atoi(val); return 0;}
	if( strstr(key,"rss")) { ev->rss= atol(val); return 0;}
	if( strstr(key,"size")) { ev->size= atol(val); return 0;}
	if( strstr(key,"inblock")) { ev->inblock= atol(val); return 0;}
	if( strstr(key,"oublock")) { ev->oublock= atol(val); return 0;}
	if( strstr(key,"majflt")) { ev->majflt= atol(val); return 0;}
	if( strstr(key,"swaps")) { ev->swaps= atol(val); return 0;}
	if( strstr(key,"nvcsw")) { ev->csw= atol(val); return 0;}
	if( strstr(key,"stmt")) { 
		ev->stmt= strdup(val); 
		if( (key = strstr(val,"querylog.define(") ) ){
			s =c= strstr(key,"\\\":str");
			if( s){
				s = strstr(c+6,":str,");
				if( s)
					malsize = atol(s+5);
			}
			c= strstr(key,"\\\":str");
			if(c) *c = 0;
			c= strchr(key,(int)'(');
			while(*c && *c != '"') c++;
			if( *c == '"') c++;
			currentquery = strdup(stripQuotes(c));
		}
		s= strstr(val," := ");
		if( s) {
			s += 4;
			c= strchr(s,(int)'(');
			if( c){
				*c =0;
				ev->fcn= strdup(s); 
			}
		} else{
			c= strchr(val,(int)'(');
			if( c){
				*c =0;
				ev->fcn= strdup(val); 
			}
		}
		return 0;
	}
	if( strstr(key,"short")) { ev->beauty= strdup(val); return 0; }
	if( strstr(key,"prereq")) { ev->prereq= strdup(val); return 0;}
	if( strstr(key,"cpuload")) { 
		ev->function= strdup(""); 
		ev->stmt= strdup(val); return 0;
	}

	if( strstr(key,"version")) { ev->version= strdup(val); return 0;}
	if( strstr(key,"release")) { ev->release= strdup(val); return 0;}
	if( strstr(key,"host")) { ev->host= strdup(val); return 0;}
	if( strstr(key,"memory")) { ev->memory= strdup(val); return 0;}
	if( strstr(key,"threads")) { ev->threads= strdup(val); return 0;}
	if( strstr(key,"oid")) { ev->oid= atoi(val); return 0;}
	if( strstr(key,"package")) { ev->package= strdup(val); return 0;}
	return 0;
}

