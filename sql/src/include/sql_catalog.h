#ifndef SQL_CATALOG_H
#define SQL_CATALOG_H

#include <sql_mem.h>
#include <sql_list.h>
#include <stream.h>

#define PRIV_SELECT 1
#define PRIV_UPDATE 2
#define PRIV_INSERT 4
#define PRIV_DELETE 8
#define PRIV_EXECUTE 16
#define PRIV_GRANT 32

#define ROLE_PUBLIC   1
#define ROLE_SYSADMIN 2
#define USER_MONETDB  3

#define ISO_READ_UNCOMMITED 1
#define ISO_READ_COMMITED   2
#define ISO_READ_REPEAT	    3
#define ISO_SERIALIZABLE    4

#define SCALE_NONE	0
#define SCALE_FIX	1
#define SCALE_NOFIX	2
#define SCALE_ADD	3
#define SCALE_SUB	4
#define DIGITS_ADD	5
#define INOUT		6	/* output type equals input type */

#define TR_OLD 0
#define TR_NEW 1

#define RDONLY 0
#define INS 1
#define DEL 2
#define UPD 3

#define cur_user 1
#define cur_role 2

#define sql_max(i1,i2) ((i1)<(i2))?(i2):(i1)

typedef int sqlid;

typedef struct sql_base {
	int wtime;
	int rtime;
	int flag;
	sqlid id;
	char *name;
} sql_base;

sqlcommon_export void base_init(sql_base * b, sqlid id, int flag, char *name);
extern void base_set_name(sql_base * b, char *name);
sqlcommon_export void base_destroy(sql_base * b);

typedef struct sql_type {
	sql_base base;

	char *sqlname;
	unsigned int digits;
	unsigned int scale;	/* indicates how scale is used in functions */
	int localtype;		/* localtype, need for coersions */
	unsigned char radix;
	unsigned int bits;
	unsigned char fixed;
} sql_type;

typedef struct sql_alias {
	char *name;
	char *alias;
} sql_alias;

typedef struct sql_subtype {
	sql_ref ref;

	sql_type *type;
	unsigned int digits;
	unsigned int scale;
	unsigned char fixed;
} sql_subtype;

typedef struct sql_aggr {
	sql_ref ref;

	char *name;
	char *imp;
	sql_subtype *tpe;
	sql_subtype *res;
	int nr;
} sql_aggr;

typedef struct sql_subaggr {
	sql_ref ref;

	sql_aggr *aggr;
	sql_subtype *res;
} sql_subaggr;

/* sql_func need type transform rules
 * types are equal if underlying types are equal +
	scale is equal
 * if types do not mach we try type conversions
 * 	which means for simple 1 arg functions
 *
 *
 */

typedef struct sql_arg {
	char *name;
	sql_subtype *type;
} sql_arg;

typedef struct sql_func {
	sql_ref ref;

	char *name;
	char *imp;
	list *ops;		/* param list */
	sql_subtype *res;
	/* res->scale
	   SCALE_NOFIX/SCALE_NONE => nothing
	   SCALE_FIX => input scale fixing,
	   SCALE_ADD => leave inputs as is and do add scales
	   example numerical multiplication
	   SCALE_SUB => first input scale, fix with second scale
	   result scale is equal to first input
	   example numerical division
	   DIGITS_ADD => result digits, sum of args
	   example string concat
	 */
	int nr;
	int sql;		/* simple sql or native implementation */
} sql_func;

typedef struct sql_subfunc {
	sql_ref ref;

	sql_func *func;
	sql_subtype *res;
} sql_subfunc;

typedef struct sql_bat {
	oid bid;
	oid ibid;		/* insert bat ! */
	oid ubid;		/* bat with updates */
} sql_bat;

typedef enum key_type {
	pkey,
	ukey,
	fkey
} key_type;

typedef struct sql_kc {
	struct sql_column *c;
	int trunc;		/* 0 not truncated, >0 colum is truncated */
} sql_kc;

typedef enum idx_type {
	unique,
	join_idx,
	new_idx_types
} idx_type;

typedef struct sql_idx {
	sql_base base;
	idx_type type;		/* unique */
	struct list *columns;	/* list of sql_kc */
	struct sql_table *t;
	sql_bat bat;
	struct sql_key *key;	/* key */
} sql_idx;

/* fkey consists of two of these */
typedef struct sql_key {	/* pkey, ukey, fkey */
	sql_base base;
	key_type type;		/* pkey, ukey, fkey */
	sql_idx *idx;		/* idx to accelerate key check */

	struct list *columns;	/* list of sql_kc */
	struct sql_table *t;
} sql_key;

typedef struct sql_ukey {	/* pkey, ukey */
	sql_key k;
	list *keys;
} sql_ukey;

typedef struct sql_fkey {	/* fkey */
	sql_key k;
	struct sql_ukey *rkey;	/* only set for fkey and rkey */
} sql_fkey;

typedef struct sql_column {
	sql_base base;
	sql_subtype *type;
	int colnr;
	bit null;
	char *def;

	struct sql_table *t;
	sql_bat bat;

} sql_column;

typedef struct changeset {
	fdestroy destroy;
	struct list *set;
	struct list *dset;
	node *nelm;
} changeset;

sqlcommon_export void cs_init(changeset * cs, fdestroy destroy);
sqlcommon_export void cs_destroy(changeset * cs);
sqlcommon_export void cs_add(changeset * cs, void *elm, int flag);
sqlcommon_export void cs_del(changeset * cs, node *elm, int flag);
sqlcommon_export int cs_size(changeset * cs);
sqlbat_export node *cs_find_name(changeset * cs, char *name);
sqlcommon_export node *cs_first_node(changeset * cs);

typedef struct sql_table {
	sql_base base;
	bit table;		/* table or view */
	bit system;		/* system or user table */
	bit persists;		/* persistent or temporary table */
	bit clear;		/* clear on transaction boundaries ? */
	/* clear on a temp table == session table! */
	char *query;
	int sz;

	changeset columns;
	changeset idxs;
	changeset keys;
	sql_ukey *pkey;

	int cleared;		/* cleared in the current transaction */
	oid dbid;		/* bat with deletes */

	struct sql_schema *s;
} sql_table;

typedef struct sql_schema {
	sql_base base;
	int auth_id;

	changeset tables;
	list *keys;		/* the names for keys and idxs are global, but */
	list *idxs;		/* these objects are only useful within a table */
} sql_schema;

typedef struct sql_module {
	sql_base base;
	changeset types;
} sql_module;

typedef struct res_col {
	char *tn;
	char *name;
	sql_subtype *type;
	bat b;
	int mtype;
	ptr *p;
} res_col;

typedef struct res_table {
	int id;
	int query_type;
	int nr_cols;
	int cur_col;
	res_col *cols;
	bat order;
	struct res_table *next;
} res_table;

typedef void *backend_code;
typedef int backend_stack;

typedef struct sql_trans {
	char *name;
	int stime;		/* transaction time stamp (aka start time) */
	int rtime;
	int wtime;
	int level;

	sql_schema *schema;
	changeset schemas;
	changeset modules;
	/* also need a current module, normaly main but during create module
	 * different */
	sql_module *module;
	struct bm *bm;
	backend_stack stk;

	struct sql_trans *parent;	/* multilevel transaction support */
} sql_trans;

extern void module_destroy(sql_module * m);
extern void schema_destroy(sql_schema *s);
extern void table_destroy(sql_table *t);
extern void column_destroy(sql_column *c);
extern void kc_destroy(sql_kc *kc);
extern void key_destroy(sql_key *k);
extern void idx_destroy(sql_idx * i);

sqlbat_export node *list_find_name(list *l, char *name);
extern node *find_key_node(sql_table *t, char *kname);
extern sql_key *find_key(sql_table *t, char *kname);
extern node *find_idx_node(sql_table *t, char *kname);
extern sql_idx *find_idx(sql_table *t, char *kname);
extern node *find_column_node(sql_table *t, char *cname);
sqlbat_export sql_column *find_column(sql_table *t, char *cname);
extern node *find_table_node(sql_schema *s, char *tname);
sqlbat_export sql_table *find_table(sql_schema *s, char *tname);
extern node *find_schema_node(sql_trans *t, char *sname);
sqlbat_export sql_schema *find_schema(sql_trans *t, char *sname);
extern node *find_module_node(sql_trans *t, char *mname);
sqlbat_export sql_module *find_module(sql_trans *t, char *mname);
extern node *find_type_node(sql_module * s, char *tname);
sqlbat_export sql_type *find_type(sql_module * s, char *tname);
sqlbat_export sql_type *sql_trans_bind_type(sql_trans *tr, char *name);

#endif /* SQL_CATALOG_H */
