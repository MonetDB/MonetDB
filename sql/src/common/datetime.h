#ifndef _DATETIME_H_
#define _DATETIME_H_

#include "context.h"
#include "symbol.h"

typedef enum inttype {
	iyear,
	imonth,
	iday,
	ihour,
	imin,
	isec
} itype;

int parse_interval( context *sql, int sign, char *str, struct dlist *pers, int *val );
/* returns 0 for month intervals and value in val, 
 *         1 for sec intervals and value in val, 
 *         <0 for errors */

const char *datetime_field( itype field );

#endif /*_DATETIME_H_*/
