#ifndef _SCOPE_H_
#define _SCOPE_H_

#include "var.h"
#include "catalog.h"
#include "statement.h"

typedef struct scope {
	stmt *ptable;		/* result pivot table */
	list *basetables;	/* from part */	
	list *pivots;		/* pivot result */
	list *outers;		/* outer references */

	list *tables;		
	list *aliases;		/* list of aliased statements */		
	struct scope *p;
} scope;

extern scope *scope_open(scope * p);
extern scope *scope_close(scope * s);

/*
 * scope_find_pivot finds the virtual pivot for the statement t
 * */
extern stmt *scope_find_pivot(scope *s, stmt *t);

/*
 * scope_first_pivot, return the first virtual pivot
 * */
extern stmt *scope_first_pivot(scope *s);

/* 
 * table_add_column adds a column (cvar) to the table t (tname could be NULL).
 * */
extern cvar *table_add_column(tvar * t, stmt * s, char *tname, char *cname ); 

/* 
 * scope_add_table adds a table (tvar) to the scope scp (name should be set). 
 * */
extern tvar *scope_add_table(scope * scp, stmt *table, char *name); 

/* 
 * scope_add_alias adds a alias for a stmt.
 * */
extern var *scope_add_alias(scope * scp, stmt * s, char *alias ); 


/* 
 * scope_bind_table finds a table in the scp with the given name, only needed
 * for name.* queries 
 * */
extern tvar *scope_bind_table( scope *scp, char *name ); 

/* 
 * scope_bind_column finds a column in the scp with the given tname.cname 
 * (where tname could be NULL) 
 * */
extern cvar *scope_bind_column( scope *scp, char *tname, char *cname ); 

/* 
 * scope_bind finds a column or alias in the scp the given name 
 * */
extern stmt *scope_bind( scope *scp, char *tname, char *cname ); 

extern cvar *scope_first_column(scope * scp);
extern tvar *scope_first_table(scope * scp);

extern void scope_dump(scope * s);

extern void tvar_destroy(tvar *t);

#endif /*_SCOPE_H_*/
