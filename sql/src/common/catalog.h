#ifndef CATALOG_H
#define CATALOG_H

#include "list.h"

typedef struct type {
	char *sqlname;
	char *name;
	struct type *cast;
	int nr;
} type;

typedef struct schema {
	long id;
	char *name;
	char *auth;
	list *tables;
} schema;

typedef struct table {
	long id;
	char *name;
	schema *schema;
	int temp;
	struct list *columns;
	char *sql; /* sql code */
} table;

typedef struct column {
	long id;
	char *name;
	table *table;
	char *default_value; 
	type *tpe;
	int null;
	int colnr;
	struct statement *s;
} column;

typedef struct aggr {
	char *name;
	char *imp;
	type *tpe;
	type *res;
	int nr;
} aggr;

typedef struct func {
	char *name;
	char *imp;
	type *tpe1;
	type *tpe2;
	type *tpe3;
	type *res;
	int nr;
} func;

typedef struct catalog {
	long nr; /* nr of oids left */
	long nextid;

	schema *cur_schema;
	struct list *schemas;
	struct list *types;
	struct list *aggrs;
	struct list *funcs;

	/* set by the specific implementation (client catalog)*/
	long (*cc_oidrange)( struct catalog *cat, int nr);
	void (*cc_getschema)( struct catalog *cat, char *schema, char *user );
	void (*cc_destroy)(struct catalog *cat );

	char *cc; /* to put specific catalog data */
} catalog;

extern catalog *default_catalog_create( );
extern void catalog_destroy ( catalog *cat );

extern	schema *cat_bind_schema( catalog *cat, char *name );
extern	schema *cat_create_schema( catalog *cat, long id, 
		char *name, char *auth);
extern	void cat_destroy_schema( catalog *cat );

extern	table *cat_bind_table( catalog *cat, schema *s, char *name );
extern	table *cat_create_table( catalog *cat, long id, schema *s, 
		char *name, int temp, char *sql );
extern	void cat_destroy_table( catalog *cat, schema *s, char *name );

extern	column *cat_bind_column( catalog *cat, table *t, char *name);
extern	column *cat_create_column( catalog *cat, long id, table *t, 
		char *name, char *type, char *def, int null_check, int colnr );

extern	type *cat_bind_type( catalog *cat, char *name);
extern	type *cat_create_type( catalog *cat, char *sqlname, 
				char *name, char *cast, int nr);

extern	aggr *cat_bind_aggr( catalog *cat, char *name, char *type);
extern	aggr *cat_create_aggr( catalog *cat, char *name, char *imp, 
						char *tpe, char *res, int nr);

extern	func *cat_bind_func( catalog *cat, char *name, 
				char *tp1, char *tp2, char *tp3);
extern	func *cat_bind_func_result( catalog *cat, char *name, 
				char *tp1, char *tp2, char *tp3, char *res);

extern	func *cat_create_func( catalog *cat, char *name, char *imp, 
				char *tpe1, char *tpe2, char *tp3, 
				char *res, int nr);

extern void catalog_initoid( catalog *cat );
extern long catalog_getoid( catalog *cat );


#endif /*CATALOG_H*/
