#ifndef _SQLSCAN_H_
#define _SQLSCAN_H_

#include <stdio.h>
#include "list.h"
#include "catalog.h"
#include "sql.h"
#include "context.h"
#include "sqlparser.tab.h"

extern void init_keywords();
extern void exit_keywords();
extern void keywords_insert(char *k, int token);
extern int  keyword_exists(char *yytext);

#endif	/* _SQLSCAN_H_ */
