#ifndef _VAR_H_
#define _VAR_H_

#include "sym.h"

typedef struct var {
	symtype type;
	symdata data;
	char *tname;
	char *cname;
	int nr;
	int refcnt;
} var;

#endif /*_VAR_H_*/
