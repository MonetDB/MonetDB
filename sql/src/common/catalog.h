#ifndef CATALOG_H
#define CATALOG_H

#include "types.h"

#define cur_user 1
#define cur_role 2

typedef enum key_type {
	pkey,
	ukey,
	fkey
} key_type;

typedef struct key {
	key_type type;
	long id;
	char *name;
	struct table *t;
	struct list *columns;
	struct key *rkey;
} key;

typedef struct schema {
	long id;
	char *name;
	char *auth;
	list *tables;
} schema;

typedef enum table_type {
	tt_base = 0,
	tt_system = 1,
	tt_view = 2,
	tt_session = 3,
	tt_temp = 4
} table_type;

typedef struct table {
	long id;
	char *name;
	schema *schema;
	table_type type; 		
	struct list *columns;
	key *pkey;
	struct list *keys; 	/* all keys (primary,unique and foreign) */
	char *sql;		/* sql code */
	struct stmt *s;
} table;

typedef struct column {
	long id;
	char *name;
	table *table;
	char *default_value;
	sql_subtype *tpe;
	int null;
	int colnr;
	struct stmt *s;
} column;

typedef struct catalog {

	schema *cur_schema;
	struct list *schemas;

	void (*cc_getschemas) (struct catalog * cat, char *schema, char *user);
	void (*cc_destroy) (struct catalog * cat);

	void *cc;		/* to put specific catalog data */
} catalog;

extern catalog *default_catalog_create();
extern void catalog_destroy(catalog * cat);

extern schema *cat_bind_schema(catalog * cat, char *name);
extern schema *cat_create_schema(catalog * cat, long id,
				 char *name, char *auth);
extern void cat_drop_schema(schema * s);

extern table *cat_bind_table(catalog * cat, schema * s, char *name);
extern table *cat_create_table(catalog * cat, long id, schema * s,
			       char *name, int type, char *sql);

extern key *cat_table_add_key(table *t, key_type kt, char *name, key *rk );
extern key *cat_key_add_column(key *k, column *c );

#define cat_table_bind_pkey(t) t->pkey
extern key *cat_table_bind_ukey(table *t, list *colnames);
extern key *cat_table_bind_sukey(table *t, char *cname);

extern void cat_drop_table(catalog * cat, schema * s, char *name);

extern column *cat_bind_column(catalog * cat, table * t, char *name);
extern column *cat_create_column(catalog * cat, long id, table * t,
				 char *name, sql_subtype *type, char *def,
				 int null_check);

#endif				/*CATALOG_H */
