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
	int nr;
} aggr;

typedef struct func {
	char *name;
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

	schema *(*bind_schema)( struct catalog *cat, char *name );
	schema *(*create_schema)( struct catalog *cat, long id, char *name, char *auth);
	void (*destroy_schema)( struct catalog *cat );

	table *(*bind_table)( struct catalog *cat, schema *s, char *name );
	table *(*create_table)( struct catalog *cat, long id, schema *s, char *name, int temp, char *sql );
	void (*destroy_table)( struct catalog *cat, schema *s, char *name );

	column *(*bind_column)( struct catalog *cat, table *t, char *name);
	column *(*create_column)( struct catalog *cat, long id, table *t, char *name, char *type, char *def, int null_check, int colnr );

	type *(*bind_type)( struct catalog *cat, char *name);
	type *(*create_type)( struct catalog *cat, char *sqlname, char *name, char *cast, int nr);

	aggr *(*bind_aggr)( struct catalog *cat, char *name);
	aggr *(*create_aggr)( struct catalog *cat, char *name, int nr);

	func *(*bind_func)( struct catalog *cat, char *name);
	func *(*create_func)( struct catalog *cat, char *name, int nr);

	/* set by the specific implementation (client catalog)*/
	long (*cc_oidrange)( struct catalog *cat, int nr);
	void (*cc_getschema)( struct catalog *cat, char *schema, char *user );
	void (*cc_destroy)(struct catalog *cat );

	char *cc; /* to put specific catalog data */
} catalog;

catalog *default_catalog_create( );
void catalog_destroy ( catalog *cat );

#endif /*CATALOG_H*/
