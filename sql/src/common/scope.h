#ifndef _SCOPE_H_
#define _SCOPE_H_

#include "var.h"
#include "catalog.h"
#include "statement.h"

typedef struct scope {
	list *vars;
	list *lifted; /* list of lifted (columns,var) */
	struct scope *p;
	int nr;
} scope;

typedef struct lifted {
	var *v;
	column *c;
} lifted;

extern scope *scope_open( scope *p );
extern scope *scope_close( scope *s );
extern void scope_add_statement( scope *scp, statement *s, char *name );
extern void scope_add_table( scope *scp, table *t, char *name );
extern var *scope_bind_table( scope *scp, char *name );
extern column *scope_bind_column( scope *scp, char *name, var **v );
extern column *scope_bind_table_column( scope *scp, char *tname, char *cname, var **v );
extern statement *scope_bind_statement( scope *scp, char *name );
extern statement *scope_first_column( scope *scp );
extern var *scope_first_table( scope *scp );

/* returns a list of vars (one per base table) */
extern list *scope_unique_lifted_vars( scope *s );

#endif /*_SCOPE_H_*/
