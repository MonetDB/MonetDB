#ifndef _VAR_H_
#define _VAR_H_

#include "list.h"

typedef struct tvar {
	list *columns;
	struct stmt *s;
	struct table *t;
	char *tname;
	int refcnt;
} tvar;

typedef struct cvar {
	struct stmt *s;
	struct column *c;
	tvar *table;
	char *tname;
	char *cname;
	int refcnt;
} cvar;

typedef struct var {
	struct stmt *s;
	char *name;
	int refcnt;
} var;


#endif /*_VAR_H_*/
