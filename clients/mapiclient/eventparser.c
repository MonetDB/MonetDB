/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/* (c) M Kersten */

#include "monetdb_config.h"
#include "eventparser.h"

int debug=0;

#define DATETIME_CHAR_LENGTH 27

#define FREE(X)  if(X){ free(X); X= 0;}

static void
resetEventRecord(EventRecord *ev)
{	int i;

	FREE(ev->version);

	// event state
	FREE(ev->version);
	FREE(ev->user);
	FREE(ev->session);
	FREE(ev->function);
	FREE(ev->module);
	FREE(ev->instruction);
	FREE(ev->state);
	FREE(ev->stmt);
	FREE(ev->time);
	for(i=0; i< ev->maxarg; i++){
		FREE(ev->args[i].alias);
		FREE(ev->args[i].name);
		FREE(ev->args[i].type);
		FREE(ev->args[i].view);
		FREE(ev->args[i].parent);
		FREE(ev->args[i].persistence);
		FREE(ev->args[i].file);
		FREE(ev->args[i].seqbase);
		FREE(ev->args[i].sorted);
		FREE(ev->args[i].revsorted);
		FREE(ev->args[i].nonil);
		FREE(ev->args[i].nil);
		FREE(ev->args[i].key);
		FREE(ev->args[i].unique);
		FREE(ev->args[i].value);
		FREE(ev->args[i].debug);
	}
	ev->maxarg = 0;
}

/* simple json key:value object parser for event record.
 * It is a restricted json parser, which uses the knowledge of the mal profiler.
 * Returns 1 if the closing bracket is found. 0 to continue, -1 upon error
 */

#define skipto(H,C) { while(*H && *H != C) H++;}
#define skipstr() { while (*c && *c !='"') {if (*c =='\\') c++;if(*c)c++;} }

/*
 * Also parse the argument array structure
 */

static char *
getstr(char *val){
	val[strlen(val) -1] = 0;
	return strdup(val + 1);
}

static int
argparser(char *txt, EventRecord *ev){
	char *c = NULL, *key = NULL,*val=NULL;
	int cnt = 0, arg = -1;
	c = txt;

	(void) ev;
	(void) key;
	/* First determine the number arguments to deal with */
	while(*c){
		skipto(c, '\t');
		if(*c){
			c++;
			if(*c == '}')
				cnt ++;
		}
	}

	/* Allocate the space for their properties */
	if(ev->args) free(ev->args);
	ev->args = (Argrecord*) malloc(cnt * sizeof(Argrecord));
	memset(ev->args, 0, cnt * sizeof(Argrecord));
	ev->maxarg = cnt;

	/* parse the event argument structures, using the \t field separator */
	c=  txt + 1;
	while(*c){
		if(*c == '{' || *c == '[')
			c++;
		if(*c == '}' || *c == ']')
			break;

		skipto(c, '"');
		key = ++c;
		skipstr();
		*c++ = 0;
		skipto(c, ':');
		c++;
		val = c;
		/* we know that the value is terminated with a hard tab */
		skipto(c, '\t');
		if(*c){ --c; *c = 0; c++;}

		/* These components should be the first */
		if(strstr(key,"ret")) {
			arg = atoi(val);
			ev->args[arg].kind = MDB_RET;
			continue;
		}
		if(strstr(key,"arg")) {
			arg = atoi(val);
			ev->args[arg].kind = MDB_ARG;
			continue;
		}
		assert(arg> -1 && arg < ev->maxarg);
		if(strstr(key,"bid")) { ev->args[arg].bid = atoi(val); continue;}
		if(strstr(key,"alias")) { ev->args[arg].alias = getstr(val); continue;}
		if(strstr(key,"name")) { ev->args[arg].name = getstr(val); continue;}
		if(strstr(key,"type")) { ev->args[arg].type = getstr(val);continue;}
		if(strstr(key,"view")) { ev->args[arg].view = getstr(val); continue;}
		if(strstr(key,"parent")) { ev->args[arg].parent = getstr(val); continue;}
		if(strstr(key,"persistence")) { ev->args[arg].persistence = getstr(val); continue;}
		if(strstr(key,"file")) { ev->args[arg].file = getstr(val); continue;}
		if(strstr(key,"seqbase")) { ev->args[arg].seqbase = getstr(val); continue;}
		if(strstr(key,"sorted")) { ev->args[arg].sorted = getstr(val); continue;}
		if(strstr(key,"revsorted")) { ev->args[arg].revsorted = getstr(val); continue;}
		if(strstr(key,"nonil")) { ev->args[arg].nonil = getstr(val); continue;}
		if(strstr(key,"nil")) { ev->args[arg].nil = getstr(val); continue;}
		if(strstr(key,"key")) { ev->args[arg].key = getstr(val); continue;}
		if(strstr(key,"unique")) { ev->args[arg].unique = getstr(val); continue;}
		if(strstr(key,"count")) { ev->args[arg].count = atol(val); continue;}
		if(strstr(key,"size")) { ev->args[arg].size = getstr(val); continue;}
		if(strstr(key,"value")) { ev->args[arg].value = getstr(val); continue;}
		if(strstr(key,"debug")) { ev->args[arg].debug = getstr(val); continue;}
		if(strstr(key,"const")) { ev->args[arg].constant = atoi(val); continue;}
	}
	return 1;
}

int
keyvalueparser(char *txt, EventRecord *ev)
{
	char *c, *key, *val;
	c = txt;

	if(*c == '{'){
		resetEventRecord(ev);
		return 0;
	}
	if(*c == '}')
		return 1;

	skipto(c, '"');
	key = ++c;
	skipstr();
	*c++ = 0;
	skipto(c, ':');
	c++;
	while(*c && isspace((unsigned char) *c)) c++;
	if(*c == '"'){
		val = ++c;
		skipstr();
		*c = 0;
	} else val =c;

	if(strstr(key,"mclk")){
		ev->usec = atol(val);
		return 0;
	}
	if(strstr(key,"clk")){
		time_t sec;
		uint64_t microsec;
		struct tm curr_time = (struct tm) {0};

		c = strchr(val,'.');
		if (c != NULL) {
			*c = '\0';
			c++;
		}

		sec = atol(val);
		microsec = sec % 1000000;
		sec /= 1000000;
#ifdef HAVE_LOCALTIME_R
		(void)localtime_r(&sec, &curr_time);
#else
		curr_time = *localtime(&sec);
#endif
		ev->time = malloc(DATETIME_CHAR_LENGTH*sizeof(char));
		snprintf(ev->time, DATETIME_CHAR_LENGTH, "%d/%02d/%02d %02d:%02d:%02d.%"PRIu64,
				 curr_time.tm_year + 1900, curr_time.tm_mon, curr_time.tm_mday,
			 curr_time.tm_hour, curr_time.tm_min, curr_time.tm_sec, microsec);
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
	if(strstr(key,"version")) { ev->version= strdup(val); return 0;}
	if(strstr(key,"user")){ ev->user= strdup(val); return 0;}
	if(strstr(key,"session")) { ev->session= strdup(val); return 0;}
	if(strstr(key,"function")){ ev->function= strdup(val); return 0;}
	if(strstr(key,"tag")){ ev->tag= atoi(val); return 0;}
	if(strstr(key,"thread")){ ev->thread= atoi(val); return 0;}
	if(strstr(key,"module")){ ev->module= strdup(val); return 0;}
	if(strstr(key,"instruction")){ ev->instruction= strdup(val); return 0;}
	if(strstr(key,"pc")){ ev->pc= atoi(val); return 0;}
	if(strstr(key,"state")){ ev->state= strdup(val);}
	if(strstr(key,"stmt")){ ev->stmt= strdup(val);}
	if(strstr(key,"usec")) { ev->ticks= atoi(val); return 0;}
	if(strstr(key,"rss")) { ev->rss= atol(val); return 0;}
	if(strstr(key,"inblock")) { ev->inblock= atol(val); return 0;}
	if(strstr(key,"oublock")) { ev->oublock= atol(val); return 0;}
	if(strstr(key,"majflt")) { ev->majflt= atol(val); return 0;}
	if(strstr(key,"swaps")) { ev->swaps= atol(val); return 0;}
	if(strstr(key,"nvcsw")) { ev->nvcsw= atol(val); return 0;}

	if (strstr(key,"args"))
		argparser(val,ev);
	return 0;
}

void
renderHeader(FILE *fd){
	fprintf(fd, "[ctime\t\t\t\tfunction\t\tstate\tthread\tticks\tsize\tstatement]\n");
}

/*
static int
renderArgs(FILE *fd, EventRecord *ev, int start, int kind)
{
	int i;
	for(i=start; i< ev->maxarg && ev->args[i].kind == kind; i++){
		if(ev->args[i].constant)
			fprintf(fd, "%s:%s ", ev->args[i].value, ev->args[i].type);
		else {
			if(strncmp(ev->args[i].type, "bat",3) == 0)
				fprintf(fd, "%s=[%ld]:%s ", ev->args[i].name, ev->args[i].count, ev->args[i].type);
			else
				fprintf(fd, "%s=%s:%s ", ev->args[i].name, ev->args[i].value, ev->args[i].type);
		}
		if(i< ev->maxarg-1 && ev->args[i+1].kind == kind) fprintf(fd,",");
	}
	return i;
}
*/

void
renderSummary(FILE *fd, EventRecord *ev, char * filter)
{
	char *c, *t;
	(void) filter;
	fprintf(fd,"%s\t",ev->time);
	fprintf(fd,"%s[%3d]%d\t",ev->function, ev->pc,ev->tag);
	fprintf(fd,"%s\t",ev->state);
	fprintf(fd,"%d\t",ev->thread);
	fprintf(fd,"%"PRId64"\t",ev->ticks);
	for (c = t = ev->stmt; *c; c++){
		if( *c == '\\' && *(c+1) =='"'){
				c++;
				*t++ = *c;
		} else
			*t++ = *c;
	}
	*t = 0;
	fprintf(fd,"%s\t",ev->stmt);
	fprintf(fd,"\n");
}
