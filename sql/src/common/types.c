
#include <mem.h>
#include <string.h>
#include "types.h"
#include "sqlscan.h"

#include <gdk.h>

static int sql_debug = 0;

list *types = NULL;
static list *aggrs = NULL;
static list *funcs = NULL;

static void sqltypeinit();

sql_subtype *sql_create_subtype( sql_type *t, int digits, int scale )
{
	sql_subtype *res = NEW(sql_subtype);
	res->type = t;
	res->digits = digits;
	res->scale = scale;
	return res;
}

sql_subtype *sql_dup_subtype( sql_subtype *t )
{
	sql_subtype *res = NEW(sql_subtype);
	*res = *t;
	return res;
}

sql_subtype *sql_bind_subtype( char *name, int digits, int scale )
	/* todo add approximate info 
	 * if digits/scale == 0 and no approximate with digits/scale == 0
	 * exits we could return the type with largest digits 
	 *
	 * returning the largest when no exact match is found is now the
	 * (wrong?) default
	 */
{
	/* assumes the types are ordered on name,digits,scale where is always
	 * 0 > n
	 */
	node *m, *n;
	for ( n = types->h; n; n = n->next ) {
		sql_type *t = n->data;
		if (strcasecmp(t->sqlname, name) == 0){
			if ((digits && t->digits >= digits) || 
					(digits == t->digits)){
				return sql_create_subtype(t, digits, scale);
			}
			for (m = n->next; m; m = m->next ) {
				t = m->data;
				if (strcasecmp(t->sqlname, name) != 0){
					break;
				}
				n = m;
				if ((digits && t->digits >= digits) || 
					(digits == t->digits)){
					return sql_create_subtype(t, digits, scale);
				}
			}
			t = n->data;
			return sql_create_subtype(t, digits, scale);
		}
	}
	return NULL;
}

void sql_subtype_destroy(sql_subtype * t)
{
	_DELETE(t);
}

sql_subtype *sql_bind_localtype( char *name )
{
	node *n = types->h;
	while (n) {
		sql_type *t = n->data;
		if (strcmp(t->name, name) == 0){
			return sql_create_subtype(t, 0, 0);
		}
		n = n->next;
	}
	return NULL;
}

static int type_cmp( sql_type *t1, sql_type *t2)
{
	if (!t1 || !t2){	
		return -1;
	}
	/* types are only equal if they map onto the same systemtype */
	return strcmp(t1->name, t2->name);
}

int subtype_cmp( sql_subtype *t1, sql_subtype *t2 )
{
	int res = type_cmp( t1->type, t2->type);
	return res;
}


sql_aggr *sql_bind_aggr(char *sqlaname, sql_subtype *type)
{
	char *name = toLower(sqlaname);
	node *n = aggrs->h;
	while (n) {
		sql_aggr *t = n->data;
		if (strcmp(t->name, name) == 0 &&
		    (!t->tpe
		     || (type && strcmp(t->tpe, type->type->name) == 0))){
			_DELETE(name);
			return t;
		}
		n = n->next;
	}
	_DELETE(name);
	return NULL;
}

sql_func *sql_bind_func(char *sqlfname, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3)
{
	char *name = toLower(sqlfname);
	node *n = funcs->h;
	while (n) {
		sql_func *t = n->data;
		if (strcmp(t->name, name) == 0 &&
		    ((tp1 && t->tpe1 && strcmp(t->tpe1, tp1->type->name) == 0) 
		     || (!tp1 && !t->tpe1)) && 
		    ((tp2 && t->tpe2 && strcmp(t->tpe2, tp2->type->name) == 0)
		     || (!tp2 && !t->tpe2)) && 
		    ((tp3 && t->tpe3 && strcmp(t->tpe3, tp3->type->name) == 0)
		     || (!tp3 && !t->tpe3))){
			_DELETE(name);
			return t;
		}
		n = n->next;
	}
	_DELETE(name);
	return NULL;
}

sql_func *sql_bind_func_result(char *sqlfname, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3, sql_subtype *res)
{
	char *name = toLower(sqlfname);
	node *n = funcs->h;
	while (n) {
		sql_func *t = n->data;
		if (strcmp(t->name, name) == 0 &&
		    strcmp(t->tpe1, tp1->type->name) == 0 &&
		    ((tp2 && t->tpe2 && strcmp(t->tpe2, tp2->type->name) == 0)
		     || (!tp2 && !t->tpe2)) && ((tp3 && t->tpe3
			 && strcmp(t->tpe3, tp3->type->name) == 0)
					|| (!tp3 && !t->tpe3))
		        	&& strcmp(t->res, res->type->name) == 0){
			_DELETE(name);
			return t;
		}
		n = n->next;
	}
	_DELETE(name);
	return NULL;
}


sql_type *sql_create_type(char *sqlname, int digits, int scale, int radix, char *name)
{
	sql_type *t = NEW(sql_type);

	t->sqlname = toLower(sqlname);
	t->digits = digits;
	t->scale = scale;
	t->radix = radix;
	t->name = _strdup(name);
	t->nr = list_length(types);
	if (!keyword_exists(t->sqlname))
		keywords_insert(t->sqlname, TYPE);
	list_append(types, t);

	return t;
}

static void type_destroy(sql_type * t)
{
	_DELETE(t->sqlname);
	_DELETE(t->name);
	_DELETE(t);
}

sql_aggr *sql_create_aggr(char *name, char *imp, char *tpe, char *res )
{
	sql_aggr *t = NEW(sql_aggr);

	t->name = toLower(name);
	t->imp = _strdup(imp);
	if (strlen(tpe)) {
		t->tpe = _strdup(tpe);
	} else {
		t->tpe = NULL;
	}
	t->res = _strdup(res);
	t->nr = list_length(aggrs);
	list_append(aggrs, t);
	return t;
}

static void aggr_destroy(sql_aggr * t)
{
	_DELETE(t->name);
	_DELETE(t->imp);
	if (t->tpe) _DELETE(t->tpe);
	_DELETE(t->res);
	_DELETE(t);
}

sql_func *sql_create_func(char *name, char *imp, char *tpe1,
		      char *tpe2, char *tpe3, char *res )
{
	sql_func *t = NEW(sql_func);

	t->name = toLower(name);
	t->imp = _strdup(imp);
	if (strlen(tpe1)) {
		t->tpe1 = _strdup(tpe1);
	} else {
		t->tpe1 = NULL;
	}
	if (strlen(tpe2)) {
		t->tpe2 = _strdup(tpe2);
	} else {
		t->tpe2 = NULL;
	}
	if (strlen(tpe3)) {
		t->tpe3 = _strdup(tpe3);
	} else {
		t->tpe3 = NULL;
	}
	t->res = _strdup(res);
	t->nr = list_length(funcs);
	list_append(funcs, t);
	return t;
}

static void func_destroy(sql_func * t)
{
	_DELETE(t->name);
	_DELETE(t->imp);
	if (t->tpe1) _DELETE(t->tpe1);
	if (t->tpe2) _DELETE(t->tpe2);
	if (t->tpe3) _DELETE(t->tpe3);
	_DELETE(t->res);
	_DELETE(t);
}

void parser_init(int debug)
{
	sql_debug = debug;

	init_keywords();
	types = list_create((fdestroy)&type_destroy);
	aggrs = list_create((fdestroy)&aggr_destroy);
	funcs = list_create((fdestroy)&func_destroy);
	sqltypeinit();
}

/* SQL service initialization
This C-code version initializes the 
parser catalogs with typing information. Although, in principle,
many of the function signatures can be obtained from the underlying
database kernel, we have chosen for this explicit scheme for one
simple reason. The SQL standard dictates the types and we have to
check their availability in the kernel only. The kernel itself could
include manyfunctions for which their is no standard.
lead to unexpected 
*/


typedef struct{
		char * h,  *t;
} pair;

void sql_type_cmd(str sqlname, int digits, int scale, int radix, str name){
	(void)sql_create_type( sqlname, digits, scale, radix, name );
}
void sql_func_cmd(str name, str imp, str tp1, str tp2, str tp3, str rtp){
	(void)sql_create_func( name, imp, tp1, tp2, tp3, rtp );
}
void sql_aggr_cmd( str name, str imp, str atp, str rtp){
	(void)sql_create_aggr( name, imp, atp, rtp );
}


void sqltypeinit()
{	int i,j;
	pair strings[]= {
		{"CHAR",	"chr"}, 
		{"VARCHAR",	"str"}, 
		{  0,	0}
	};
	pair numerical[]= {
		{"int",	"uchr"}, 
		{"int",	"sht"},
		{"lng",	"int"},
		{"lng",	"lng"},
		{"dbl",	"flt"}, 
		{"dbl",	"dbl"}, 
		{  0,	0}
	};
	pair floats[]= {
		{"dbl",	"flt"}, 
		{"dbl",	"dbl"}, 
		{  0,	0}
	};
	/*
	pair dates[]= {
		{"MONTH_INTERVAL",	"int"}, 
		{"SEC_INTERVAL",	"int"}, 
		{"DATE",		"date"}, 
		{"TIME",		"time"}, 
		{"TIMESTAMP",		"timestamp"}, 
		{  0,	0}
	};
	*/
	/* packing strings,numerical,dates */
	pair sql_types []= {
		{"CHAR",	"chr"}, 
		{"VARCHAR",	"str"}, 
		{"int",	"uchr"}, 
		{"int",	"sht"},
		{"lng",	"int"},
		{"lng",	"lng"},
		{"dbl",	"flt"}, 
		{"dbl",	"dbl"}, 
		{"MONTH_INTERVAL",	"int"}, 
		{"SEC_INTERVAL",	"int"}, 
		{"DATE",		"date"}, 
		{"TIME",		"time"}, 
		{"TIMESTAMP",		"timestamp"}, 
		{  0,	0}
	};

	sql_type_cmd("OID", 0, 0, 2, 	  	"oid");
	sql_type_cmd("BOOL", 0, 0, 2,	  	"bit");
	sql_type_cmd("BOOLEAN", 0, 0, 2,	"bit");

	sql_type_cmd("CHAR", 0, 0, 0,  	"str"); 
	sql_type_cmd("CHARACTER", 0, 0, 0, 	"str");
	sql_type_cmd("VARCHAR", 0, 0, 0, 	"str");

	sql_type_cmd("TEXT", 0, 0, 0, 		"str");
	sql_type_cmd("TINYTEXT", 0, 0, 0, 	"str");
	sql_type_cmd("STRING", 0, 0, 0, 	"str");

/*
	 *INT(n) n <= 2 -> TINYINT
		  n <= 5 -> SMALLINT
		  n <= 9 -> MEDIUMINT
		  n <= 19 -> BIGINT
*/

	sql_type_cmd("UBYTE", 2, 0, 2,		"uchr");
	sql_type_cmd("TINYINT", 2, 0, 2, 	"sht"); /* sht as sum(uchr) isn't implemented */
	sql_type_cmd("SMALLINT", 5, 0, 2,	"sht");
	sql_type_cmd("MEDIUMINT", 9, 0, 2,	"int");
	sql_type_cmd("INTEGER", 9, 0, 2,	"int");
	sql_type_cmd("NUMBER", 9, 0, 2,	"int");
	sql_type_cmd("BIGINT", 19, 0, 2,	"lng");

	/*sql_type("INT", 2, 0, 2, 		"uchr"); */
	sql_type_cmd("INT", 5, 0, 2,		"sht");
	sql_type_cmd("INT", 9, 0, 2,		"int");
	sql_type_cmd("INT", 19, 0, 2,		"lng");

	/* float(n) (n indicates precision of atleast n digits)*/
	/* ie n <= 23 -> flt */
	/*    n <= 51 -> dbl */
	/*    n <= 62 -> long long dbl (with -ieee) (not supported) */
	/* this requires a type definition */

	sql_type_cmd("FLOAT", 23, 0, 2, 	"flt");
	sql_type_cmd("FLOAT", 51, 0, 2, 	"dbl");

	sql_type_cmd("DOUBLE", 51, 0, 2, 	"dbl");
	sql_type_cmd("REAL", 51, 0, 2, 	"dbl");

	/* decimal(n) == int(n)*/

	/*#sql_type("DECIMAL", 2, 0, 10, 		"uchr"); */
	sql_type_cmd("DECIMAL", 5, 0, 10,		"sht");
	sql_type_cmd("DECIMAL", 9, 0, 10,		"int");
	sql_type_cmd("DECIMAL", 19, 0, 10,		"lng");

/*
	# decimal(d,s) (d indicates nr digits, s scale indicates nr of digits after the dot .)
	#sql_type_cmd("DECIMAL", 51, 50, "decimal");		# (fixed precision) requires decimal module
*/
	sql_type_cmd("DECIMAL", 23, 22, 10,		"flt");
	sql_type_cmd("DECIMAL", 51, 50, 10,		"dbl");

	/*sql_type_cmd("NUMERIC", 2, 0, 		"uchr"); */
	sql_type_cmd("NUMERIC", 5, 0, 10,		"sht");
	sql_type_cmd("NUMERIC", 9, 0, 10,		"int");
	sql_type_cmd("NUMERIC", 19, 0, 10,		"lng");

	sql_type_cmd("NUMERIC", 23, 22, 10,		"flt");
	sql_type_cmd("NUMERIC", 51, 50, 10,		"dbl");



	sql_type_cmd("MONTH_INTERVAL", 0, 0, 10, 	"int");
	sql_type_cmd("SEC_INTERVAL", 0, 0, 10, 		"int");
	sql_type_cmd("DATE", 0, 0, 0, 			"date");
	sql_type_cmd("TIME", 0, 0, 0, 			"time");
	sql_type_cmd("DATETIME", 0, 0, 0, 		"datetime");
	sql_type_cmd("TIMESTAMP", 0, 0,0, 		"timestamp");


	for(i=0; sql_types[i].h; i++)
		sql_func_cmd("hash","hash",sql_types[i].t,"","","int");
	for(i=0; sql_types[i].h; i++)
		sql_func_cmd("=","=",sql_types[i].t,sql_types[i].t,"","bit");
	for(i=0; sql_types[i].h; i++)
		sql_func_cmd("<>","!=",sql_types[i].t,sql_types[i].t,"","bit");

	for(i=0; sql_types[i].h; i++)
		sql_aggr_cmd("min","min",sql_types[i].t,sql_types[i].t);
	for(i=0; sql_types[i].h; i++)
		sql_aggr_cmd("max","max",sql_types[i].t,sql_types[i].t);

	for(i=0; numerical[i].h; i++){
		char buf[50];
		snprintf(buf,50,"sum_%s",numerical[i].h);
		sql_aggr_cmd("sum",buf,numerical[i].t, numerical[i].h);
	}
	for(i=0; numerical[i].h; i++){
		sql_aggr_cmd("avg","avg",numerical[i].t, "dbl");
	}

	sql_aggr_cmd( "count", "count", "", "int" ); 

	for(i=0; numerical[i].h; i++){
	sql_func_cmd("sql_sub","-",numerical[i].t,numerical[i].t,"",numerical[i].t);
	sql_func_cmd("sql_add","+",numerical[i].t,numerical[i].t,"",numerical[i].t);
	sql_func_cmd("sql_mul","*",numerical[i].t,numerical[i].t,"",numerical[i].t);
	sql_func_cmd("sql_div","/",numerical[i].t,numerical[i].t,"",numerical[i].t);
	sql_func_cmd("sql_max","max",numerical[i].t,numerical[i].t,"",numerical[i].t);
	sql_func_cmd("sql_min","min",numerical[i].t,numerical[i].t,"",numerical[i].t);
	sql_func_cmd("and","and",numerical[i].t,numerical[i].t,"",numerical[i].t);
	sql_func_cmd("or","or",numerical[i].t,numerical[i].t,"",numerical[i].t);
	sql_func_cmd("xor","xor",numerical[i].t,numerical[i].t,"",numerical[i].t);
	}

	for(i=0; numerical[i].h; i++)
		sql_func_cmd("sql_neg","-",numerical[i].t,"","",numerical[i].t);


	for(i=0; numerical[i].h; i++)
	for(j=0; numerical[j].h;j++)
	if( strcmp(numerical[i].t,numerical[j].t) )
	    sql_func_cmd("convert", numerical[j].t, numerical[i].t, "","", numerical[j].t );
	

	for(i=0; strings[i].h; i++)
	for(j=0; strings[j].h;j++)
	if( strcmp(strings[i].t,strings[j].t) )
	    sql_func_cmd("convert", strings[j].t, strings[i].t, "","", strings[j].t );
	

	for(i=0; strings[i].h; i++)
	for(j=0; numerical[j].h;j++)
	if( strcmp(strings[i].t,numerical[j].t) )
	    sql_func_cmd("convert", numerical[j].t, strings[i].t, "","", numerical[j].t );
	

	for(i=0; floats[i].h;i++){
		sql_func_cmd( "floor", "floor", floats[i].t, "", "", floats[i].t ); 
		sql_func_cmd( "ceil", "ceil", floats[i].t, "", "", floats[i].t ); 
		sql_func_cmd( "sin", "sin", floats[i].t, "", "", floats[i].t ); 
		sql_func_cmd( "cos", "cos", floats[i].t, "", "", floats[i].t ); 
		sql_func_cmd( "tan", "tan", floats[i].t, "", "", floats[i].t ); 
		sql_func_cmd( "asin", "asin", floats[i].t, "", "", floats[i].t ); 
		sql_func_cmd( "acos", "acos", floats[i].t, "", "", floats[i].t ); 
		sql_func_cmd( "atan", "atan", floats[i].t, "", "", floats[i].t ); 
		sql_func_cmd( "sinh", "sinh", floats[i].t, "", "", floats[i].t ); 
		sql_func_cmd( "cosh", "cosh", floats[i].t, "", "", floats[i].t ); 
		sql_func_cmd( "tanh", "tanh", floats[i].t, "", "", floats[i].t ); 
		sql_func_cmd( "sqrt", "sqrt", floats[i].t, "", "", floats[i].t ); 
		sql_func_cmd( "exp", "exp", floats[i].t, "", "", floats[i].t ); 
		sql_func_cmd( "log", "log", floats[i].t, "", "", floats[i].t ); 
		sql_func_cmd( "log10", "log10", floats[i].t, "", "", floats[i].t ); 
	}

	sql_func_cmd( "current_date", "current_date", "", "", "" , "date" );
	sql_func_cmd( "current_time", "current_time", "", "", "" , "time" );
	sql_func_cmd( "current_timestamp", "current_timestamp", "", "", "" , "timestamp" );

	sql_func_cmd( "sql_sub", "date_sub_sec_interval", "date", "int", "", "date");
	sql_func_cmd( "sql_sub", "date_sub_month_interval", "date", "int", "", "date");

	sql_func_cmd( "sql_add", "date_add_sec_interval", "date", "int", "", "date");
	sql_func_cmd( "sql_add", "addmonths", "date", "int", "", "date");

	sql_func_cmd( ">", ">", "date", "date", "", "bit");
	sql_func_cmd( "<", "<", "date", "date", "", "bit");

	sql_func_cmd( "year", "year", "date", "", "", "int");
	sql_func_cmd( "month", "month", "date", "", "", "int");

	sql_func_cmd( "substring", "string", "str", "int", "int", "str");
	sql_func_cmd( "strconcat", "+", "str", "str", "", "str");
}

void parser_exit()
{
	list_destroy(aggrs);
	list_destroy(funcs);
	list_destroy(types);

	exit_keywords();
}
