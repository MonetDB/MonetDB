/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/* (c) M Kersten */

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

#ifndef HAVE_STRPTIME
extern char *strptime(const char *, const char *, struct tm *);
#include "strptime.c"
#endif

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

char * 
stripQuotes(const char *currentquery)
{
	const char *c;
	char *q, *qry;
	q = qry = (char *) malloc(strlen(currentquery) * 2);
	if( q == NULL){
		fprintf(stderr,"Could not allocate query buffer of size "SZFMT"\n", strlen(currentquery) * 2);
		exit(-1);
	}
	for (c= currentquery; *c; ){
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
eventdump(void)
{	int i;
	fprintf(stderr,"Event analysis\n");
	for(i=0; i < malargc; i++)
		fprintf(stderr,"arg[%d] %s %s %d\n",i,malarguments[i], maltypes[i], malcount[i]);
	for(i=0; i < malvartop; i++)
		fprintf(stderr,"var[%d] %s\n",i,malvariables[i]);
}

static void
parseArguments(char *call, int m)
{
	int argc= m < 0? -1:0;
	char  *c = call, *l, ch;
	char *v, *w;
	
	if( debug)
		fprintf(stderr,"%s\n",call);
	// skip MAL keywords
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
			// remember variable in its own structure, it ends with =
			v=  c;
			c= strchr(c,'=');
			if( c == 0)
				break;
			*c = 0;
			malvariables[malvartop++] = strdup(v);
			if( malvariables[malvartop-1] == NULL){
				fprintf(stderr,"Could not allocate memory\n");
				exit(-1);
			}
			*c = '=';
			c++;
		}
		// all arguments have a value
		if (*c && *c== '\\' && *(c+1) =='"'){
			c++; c++;
			// parse string skipping escapes
			for(l=c; *l; l++){
				if( *l =='\\' &&  *(l+1) =='\\' && *(l+2) =='\\' &&  *(l+3) =='"') { l+=3; continue;}
				if( *l =='\\' &&  *(l+1) =='\\') { l++; continue;}
				if( *l =='\\' &&  *(l+1) =='n') { l++; continue;}
				if( *l =='\\' &&  *(l+1) =='t') { l++; continue;}
				if( *l =='\\' &&  *(l+1) =='"') break;
			}
			*l= 0;
			malarguments[malargc] = strdup(c);
			if( malarguments[malargc] == NULL){
				fprintf(stderr,"Could not allocate memory\n");
				exit(-1);
			}
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
			if( malarguments[malargc] == NULL){
				fprintf(stderr,"Could not allocate memory\n");
				exit(-1);
			}
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
			if( malarguments[malargc] == NULL){
				fprintf(stderr,"Could not allocate memory\n");
				exit(-1);
			}
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
			if( malarguments[malargc-1] == NULL){
				fprintf(stderr,"Could not allocate memory\n");
				exit(-1);
			}
			*c = ch;
			if( ch == ';') break;
		} else malargc++;

		if( *c == '{') { // skip property lists
			while(*c && *c != '}') c++;
			c++;
		}
		if (*c == 0 || *c ==')' )
			break;
	}
	if( debug)
		eventdump();
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
	if( ev->clk) free(ev->clk);
	if( ev->stmt) free(ev->stmt);
	if( ev->fcn) free(ev->fcn);
	if( ev->numa) free(ev->numa);
	memset( (char*) ev, 0, sizeof(EventRecord));
}

/* simple json key:value object parser for event record.
 * each event pair on a single row, which is required for dealing with string values
 * It avoids lots of escaped charactor recognition, simply take with mserver delivers
 * Returns 1 if the closing bracket is found. 0 to continue, -1 upon error
 */
#define skipto(C) { while(*c && *c != C) c++; if (*c != C) return -1;}
#define skipstr() { c= strrchr(c, (int) '"'); if (*c != '"') return -1;}
int
keyvalueparser(char *txt, EventRecord *ev)
{
	char *c, *key, *val;
	struct tm stm;

	c= txt;

	if( *c == '{'){
		resetEventRecord(ev);
		malargc = 0;
		malvartop = 0;
		memset(malarguments, 0, sizeof(malarguments));
		memset(malvariables, 0, sizeof(malvariables));
		return 0;
	}
	if( *c == '}')
		return 1;

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

	if( strstr(key,"version")) { ev->version= strdup(val); return 0;}
	if( strstr(key,"release")) { ev->release= strdup(val); return 0;}
	if( strstr(key,"host")) { ev->host= strdup(val); return 0;}
	if( strstr(key,"memory")) { ev->memory= strdup(val); return 0;}
	if( strstr(key,"threads")) { ev->threads= strdup(val); return 0;}
	if( strstr(key,"oid")) { ev->oid= atoi(val); return 0;}
	if( strstr(key,"package")) { ev->package= strdup(val); return 0;}

	if( strstr(key,"event")) { ev->eventnr= atol(val); return 0;}
	if( strstr(key,"time")){
		/* convert time to epoch in seconds*/
		ev->clk= strdup(val);
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

	if( strstr(key,"ticks")) { ev->ticks= atoi(val); return 0;}
	if( strstr(key,"rss")) { ev->rss= atol(val); return 0;}
	if( strstr(key,"size")) { ev->size= atol(val); return 0;}
	if( strstr(key,"inblock")) { ev->inblock= atol(val); return 0;}
	if( strstr(key,"oublock")) { ev->oublock= atol(val); return 0;}
	if( strstr(key,"majflt")) { ev->majflt= atol(val); return 0;}
	if( strstr(key,"swaps")) { ev->swaps= atol(val); return 0;}
	if( strstr(key,"nvcsw")) { ev->csw= atol(val); return 0;}
	if( strstr(key,"stmt")) { ev->stmt= strdup(val); 
		c = ev->stmt;
		parseArguments((*c ==')'?c++:c),-1);
		malretc = malargc;
		ev->fcn = strdup(val);
		c = strchr(ev->fcn, (int) '(');
		if (c) {
			parseArguments(c+1,1);
			*c =0;
		}
		return 0;
	}
	if( strstr(key,"short")) { ev->beauty= strdup(val); return 0;}
	return 0;
}

