/* multi version catalog */

#include <gdk.h>

typedef struct batinfo {
	BAT *b;
	int rtime;
	int wtime;
} batinfo;

typedef struct mvc {
	int debug;

	BAT *type_sql;
	BAT *type_db;

	BAT *schema_id; 
	BAT *schema_name; 
	BAT *schema_auth;

	BAT *table_id; /* table, id */
	BAT *table_name; /* table, name */
	BAT *table_temp;
	BAT *table_schema;
	BAT *table_query;

	BAT *column_id; /* column, id */
	BAT *column_name; /* column, name */
	BAT *column_type;
	BAT *column_table; /* column, table */
	BAT *column_bat; /* column, bat */
	BAT *column_default;
	BAT *column_null;
	BAT *column_number;

	batinfo *bats; /* keep info about used bats */
	int 	size;
} mvc;

extern mvc *mvc_create( int debug );

extern BAT *mvc_bind( mvc *c, oid cid );
extern oid  mvc_create_schema( mvc *c, oid sid, char *name, char *auth);
extern void mvc_drop_schema( mvc *c, oid sid );
extern oid  mvc_create_table( mvc *c, oid tid, oid sid, char *name, bit temp);
extern void mvc_drop_table( mvc *c, oid tid, bit cascade );
extern oid  mvc_create_view( mvc *c, oid tid, oid sid, char *name, char *sql);
extern oid  mvc_create_column( mvc *c, oid cid, oid tid, 
					char *name, char *type, int seqnr );
extern void mvc_drop_column( mvc *c, oid cid );
extern oid mvc_not_null( mvc *c, oid cid );
extern oid mvc_default( mvc *c, oid cid, char *val );
extern void mvc_fast_insert( mvc *c, char *insert_string );
extern void mvc_insert( mvc *c, oid tid, char *insert_string );


