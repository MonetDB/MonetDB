#ifndef _VAR_H_
#define _VAR_H_

#include "sym.h"

typedef struct var {
	symtype type;
	symdata data;
	char *vname;
	int nr;
} var;

#endif /*_VAR_H_*/
