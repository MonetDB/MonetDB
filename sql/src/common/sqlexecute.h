#ifndef _SQLEXECUTE_H_
#define _SQLEXECUTE_H_

#include <stdio.h>
#include "list.h"
#include "catalog.h"
#include "statement.h"
#include "sql.h"

extern statement *sqlnext( context *lc, stream *in, int *err );
extern statement *sqlexecute( context *lc, char *statement );

extern void sql_init_context(context *lc, stream *out, int debug, catalog *cat);
extern void sql_exit_context(context *lc );

extern void sql_statement_init( context *c );

#endif /* _SQLEXECUTE_H_ */
