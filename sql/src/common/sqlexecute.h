#ifndef _SQLEXECUTE_H_
#define _SQLEXECUTE_H_

#include <stdio.h>
#include "list.h"
#include "catalog.h"
#include "statement.h"
#include "sql.h"

sql_export stmt *sqlnext(context * lc, stream * in, int *err);
sql_export stmt *sqlexecute(context * lc, char *inbuf, int *err);

sql_export void sql_init_context(context * lc, stream * out, int debug,
			     catalog * cat);
sql_export void sql_exit_context(context * lc);

extern void sql_statement_init(context * c);

#endif				/* _SQLEXECUTE_H_ */
