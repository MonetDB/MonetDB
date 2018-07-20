/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/* (c) M Kersten */

#include "monetdb_config.h"
#include "eventparser.h"

char *statenames[]= {"","start","done","action","ping","wait","system"};

char *maltypes[MAXMALARGS];
char *malvariables[MAXMALARGS];
char *malvalues[MAXMALARGS];
int malcount[MAXMALARGS];
int malargc;
int malretc;

int malsize;
int debug=0;
char *currentquery=0;
int eventcounter = 0;

#define DATETIME_CHAR_LENGTH 27

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
		fprintf(stderr,"Could not allocate query buffer of size %zu\n", strlen(currentquery) * 2);
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
	if(ev->beauty) free(ev->beauty);
	if(ev->prereq) free(ev->prereq);
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
	c = txt;

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
	while( *c && isspace((unsigned char) *c)) c++;
	if( *c == '"'){
		val = ++c;
		skipstr();
		*c = 0;
	} else val =c;

	if( strstr(key,"clk")){
		ev->usec = atol(val);
		return 0;
	}
	if( strstr(key,"ctime")){
		time_t sec;
		struct tm curr_time;

		c = strchr(val,'.');
		if (c != NULL) {
			*c = '\0';
			c++;
		}

		sec = atol(val);
#ifdef HAVE_LOCALTIME_R
		(void)localtime_r(&sec, &curr_time);
#else
		curr_time = *localtime(&sec);
#endif
		ev->time = malloc(DATETIME_CHAR_LENGTH*sizeof(char));
		snprintf(ev->time, DATETIME_CHAR_LENGTH, "%d/%02d/%02d %02d:%02d:%02d.%s",
				 curr_time.tm_year + 1900, curr_time.tm_mon, curr_time.tm_mday,
				 curr_time.tm_hour, curr_time.tm_min, curr_time.tm_sec,
				 c);
		ev->clkticks = sec * 1000000;
		if (c != NULL) {
			int64_t usec;
			/* microseconds */
			usec = strtoll(c, NULL, 10);
			assert(usec >= 0 && usec < 1000000);
			ev->clkticks += usec;
		}
		if (ev->clkticks < 0) {
			fprintf(stderr, "parser: read negative value %"PRId64" from\n'%s'\n", ev->clkticks, val);
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
			c= strchr(key,'(');
			if (c) {
				while(*c && *c != '"') c++;
				if( *c == '"') c++;
				currentquery = stripQuotes(c);
			}
		}
		s= strstr(val," := ");
		if( s) {
			s += 4;
			c= strchr(s,'(');
			if( c){
				*c =0;
				ev->fcn= strdup(s); 
			}
		} else{
			c= strchr(val,'(');
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

void
eventdump(void)
{   int i;
    for(i=0; i < malargc; i++)
        fprintf(stderr,"arg[%d] %s %s %d\n",i,malvariables[i], maltypes[i], malcount[i]);
    for(i=0; i < malretc; i++)
        fprintf(stderr,"var[%d] %s\n",i,malvariables[i]);
}

int
lineparser(char *row, EventRecord *ev)
{
	char *c, *cc, *v =0;
	struct tm stm;

	malargc = 0;
	malretc = 0;
	memset(malvariables, 0, sizeof(malvariables));
	/* check basic validaty first */
	if (row[0] =='#'){
		return 1;	/* ok, but nothing filled in */
	}
	if (row[0] != '[')
		return -1;
	if ((cc= strrchr(row,']')) == 0 || *(cc+1) !=0)
		return -1;

	/* scan event record number */
	c = row+1;
	ev->eventnr = atoi(c + 1);

	/* scan event time" */
	c = strchr(c + 1, '"');
	if (c == NULL)
		return -3;
	/* convert time to epoch in seconds*/
	cc =c;
	memset(&stm, 0, sizeof(struct tm));
#ifdef HAVE_STRPTIME
	c = strptime(c + 1, "%H:%M:%S", &stm);
	ev->clkticks = (((int64_t) stm.tm_hour * 60 + stm.tm_min) * 60 + stm.tm_sec) * 1000000;
	if (c == NULL)
		return -3;
#else
	int pos;
	if (sscanf(c + 1, "%d:%d:%d%n", &stm.tm_hour, &stm.tm_min, &stm.tm_sec, &pos) < 3)
		return -3;
	c += pos + 1;
#endif
	if (*c == '.') {
		int64_t usec;
		/* microseconds */
		usec = strtoll(c + 1, NULL, 10);
		assert(usec >= 0 && usec < 1000000);
		ev->clkticks += usec;
	}
	c = strchr(c + 1, '"');
	if (c == NULL)
		return -3;
	if (ev->clkticks < 0) {
		fprintf(stderr, "parser: read negative value %"PRId64" from\n'%s'\n", ev->clkticks, cc);
	}
	c++;

	/* skip pc tag */
	{	// decode qry[pc]tag
		char *nme = c;
		c= strchr(c+1,'[');
		if( c == 0)
			return -4;
		*c = 0;
		ev->function= strdup(nme);
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
	ev->rss = strtoll(c + 1, NULL, 10);

	/* scan tmpMB */
	c = strchr(c + 1, ',');
	if (c == 0)
		return -8;
	ev->size = strtoll(c + 1, NULL, 10);

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
		v=c;
		c = strstr(c + 1, ":= ");
		if (c) {
			*c = 0;
			parseArgument( (*v == '('? v++:v),ev);
			malretc =malargc;
			*c=':';
			ev->fcn = c + 2;
			/* find genuine function calls */
			while (isspace((unsigned char) *ev->fcn) && *ev->fcn)
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
			parseArgument(v+1,ev);
	 } else { //assigment statements
		v= ev->stmt;
		v = strstr(ev->stmt, ":= ");
		if( v)
			parseArgument(v+3,ev);
	}
	if (ev->stmt && (v=strstr(ev->stmt, ";\",\t")))
		*v = 0;
	return 0;
}

void
renderJSONevent(FILE *fd, EventRecord *ev, int notfirst)
{	
	int i;

	if( notfirst)
		fprintf(fd,"},\n{");
	fprintf(fd,"\"user\":\"%s\",\n",ev->user?ev->user:"monetdb");
	fprintf(fd,"\"clk\":%"PRId64",\n",ev->usec);
	fprintf(fd,"\"ctime\":\"%s\",\n",ev->time);
	fprintf(fd,"\"thread\":%d,\n",ev->thread);
	fprintf(fd,"\"function\":\"%s\",\n",ev->function);
	fprintf(fd,"\"pc\":%d,\n",ev->pc);
	fprintf(fd,"\"tag\":%d,\n",ev->tag);
	switch(ev->state){
	case MDB_START:
		fprintf(fd,"\"state\":\"start\",\n");
		break;
	case MDB_DONE:
		fprintf(fd,"\"state\":\"done\",\n");
		break;
	case MDB_PING:
		fprintf(fd,"\"state\":\"ping\",\n");
		break;
	case MDB_WAIT:
		fprintf(fd,"\"state\":\"wait\",\n");
		break;
	case MDB_SYSTEM:
		fprintf(fd,"\"state\":\"system\",\n");
		break;
	}
	fprintf(fd,"\"usec\":%"PRId64",\n",ev->ticks);
	fprintf(fd,"\"rss\":%"PRId64",\n",ev->rss);
	fprintf(fd,"\"size\":%"PRId64",\n",ev->size);
	if( strstr(ev->stmt," ]"))
		*strstr(ev->stmt," ]") = 0;
	fprintf(fd,"\"stmt\":\"%s\",\n",ev->stmt);
	fprintf(fd,"\"short\":\"%s\",\n",ev->beauty?ev->beauty:ev->stmt);
	fprintf(fd,"\"prereq\":[]");
	if(malretc > 0){
		fprintf(fd,",\n\"ret\":[");
	} 
	for(i=0; i<malretc; i++){
		if(i== malretc)
			fprintf(fd,"],\n\"arg\":[");
		else
			if( i) fprintf(fd,",\n");
		fprintf(fd,"{\"index\":%d,\"name\":\"%s\",\"type\":\"%s\", \"value\":\"%s\",\"eol\":%d}",	i, "","","",i);
	}
	if(malretc > 0)
		fprintf(fd,"],\n");
	else fprintf(fd,"\n");
}
