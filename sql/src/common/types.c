
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
	assert(0);
	return NULL;
}

sql_type *sql_bind_type( char *name )
{
	node *n = types->h;
	while (n) {
		sql_type *t = n->data;
		if (strcmp(t->name, name) == 0){
			return t;
		}
		n = n->next;
	}
	assert(0);
	return NULL;
}

static int type_cmp( sql_type *t1, sql_type *t2)
{
	int res = 0;
	if (!t1 || !t2)
		return -1;
	/* types are only equal 
		iff they map onto the same systemtype */
	res = (t1->localtype-t2->localtype);
	if (res) return res;
	/* and
		iff they have the same sqlname */
	return (strcmp(t1->sqlname, t2->sqlname));
}

int subtype_cmp( sql_subtype *t1, sql_subtype *t2 )
{
	if (!t1 || !t2)
		return -1; 
	/* subtypes are only equal iff 
			they map onto the same systemtype */
	return (type_cmp( t1->type, t2->type));
}


sql_subaggr *sql_bind_aggr(char *sqlaname, sql_subtype *type)
{
	char *name = toLower(sqlaname);
	node *n = aggrs->h;
	while (n) {
		sql_aggr *a = n->data;
		if (strcmp(a->name, name) == 0 &&
		    (!a->tpe
		     || (type && subtype_cmp(a->tpe, type) == 0))){
			sql_subaggr *ares = NEW(sql_subaggr);
			_DELETE(name);
			ares -> aggr = a;
			ares -> res = *a->res;
			/* same scale as the input */
			if (type)
				ares -> res . scale = type->scale; 
			return ares;
		}
		n = n->next;
	}
	_DELETE(name);
	return NULL;
}

sql_subfunc *sql_bind_func(char *sqlfname, sql_subtype *tp1, sql_subtype *tp2)
{
	char *name = toLower(sqlfname);
	node *n = funcs->h;
	while (n) {
		sql_func *f = n->data;
		if (strcmp(f->name, name) == 0 &&
		    ((tp1 && f->tpe1 && subtype_cmp(f->tpe1, tp1) == 0) 
		     || (!tp1 && !f->tpe1)) && 
		    ((tp2 && f->tpe2 && subtype_cmp(f->tpe2, tp2) == 0)
		     || (!tp2 && !f->tpe2))){
			sql_subfunc *fres = NEW(sql_subfunc);
			_DELETE(name);
			fres -> func = f;
			fres -> res = *f->res;
			fres -> res . scale = 0;
			/* same scale as the input */
			if (tp1 && tp1->scale > fres->res.scale)
				fres -> res . scale = tp1->scale; 
			if (tp2 && tp2->scale > fres->res.scale)
				fres -> res . scale = tp2->scale; 
			return fres;
		}
		n = n->next;
	}
	_DELETE(name);
	return NULL;
}

sql_subfunc *sql_bind_func_(char *sqlfname, list *ops )
{
	char *name = toLower(sqlfname);
	node *n = funcs->h;
	while (n) {
		sql_func *f = n->data;
		if (strcmp(f->name, name) == 0 &&
		    list_cmp(f->ops, ops, (fcmp)&subtype_cmp) == 0){
			sql_subfunc *fres = NEW(sql_subfunc);
			_DELETE(name);
			fres -> func = f;
			fres -> res = *f->res;
			fres -> res . scale = 0;
			for (n=ops->h; n; n=n->next){
				sql_subtype *t = n->data;
				/* same scale as the input */
				if (t && t->scale > fres->res.scale)
					fres -> res . scale = t->scale; 
			}
			return fres;
		}
		n = n->next;
	}
	_DELETE(name);
	return NULL;
}

sql_subfunc *sql_bind_func_result(char *sqlfname, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *res)
{
	char *name = toLower(sqlfname);
	node *n = funcs->h;
	while (n) {
		sql_func *f = n->data;
		if (strcmp(f->name, name) == 0 &&
		    subtype_cmp(f->tpe1, tp1) == 0 &&
		    ((tp2 && f->tpe2 && subtype_cmp(f->tpe2, tp2) == 0)
		     || (!tp2 && !f->tpe2)) && 
		    subtype_cmp(f->res, res) == 0){
			sql_subfunc *fres = NEW(sql_subfunc);
			_DELETE(name);
			fres -> func = f;
			fres -> res = *f->res;
			fres -> res . scale = 0;
			/* same scale as the input */
			if (tp1 && tp1->scale > fres->res.scale)
				fres -> res . scale = tp1->scale; 
			if (tp2 && tp2->scale > fres->res.scale)
				fres -> res . scale = tp2->scale; 
			return fres;
		}
		n = n->next;
	}
	_DELETE(name);
	return NULL;
}

sql_subfunc *sql_bind_func_result_(char *sqlfname, list *ops, sql_subtype *res )
{
	char *name = toLower(sqlfname);
	node *n = funcs->h;
	while (n) {
		sql_func *f = n->data;
		if (strcmp(f->name, name) == 0 &&
		    subtype_cmp(f->res, res) == 0 &&
		    list_cmp(f->ops, ops, (fcmp)&subtype_cmp) == 0){
			sql_subfunc *fres = NEW(sql_subfunc);
			_DELETE(name);
			fres -> func = f;
			fres -> res = *f->res;
			fres -> res . scale = 0;
			for (n=ops->h; n; n=n->next){
				sql_subtype *t = n->data;
				/* same scale as the input */
				if (t && t->scale > fres->res.scale)
					fres -> res . scale = t->scale; 
			}
			return fres;
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
	if (t->nr){
		sql_type *pt = types->t->data;
		t->nr = pt->nr;
		if (strcmp(pt->sqlname, t->sqlname) != 0)
			t->nr ++;
	}
	t->localtype = ATOMindex(t->name);
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

sql_aggr *sql_create_aggr(char *name, char *imp, sql_type *tpe, sql_type *res )
{
	sql_aggr *t = NEW(sql_aggr);

	t->name = toLower(name);
	t->imp = _strdup(imp);
	if (tpe) {
		t->tpe = sql_create_subtype(tpe, 0, 0);
	} else {
		t->tpe = NULL;
	}
	assert(res);
	t->res = sql_create_subtype(res, 0, 0);
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

sql_func *sql_create_func(char *name, char *imp, sql_type *tpe1,
		      sql_type *tpe2, sql_type *res, int scale_fixing )
{
	sql_func *t = NEW(sql_func);

	t->name = toLower(name);
	t->imp = _strdup(imp);
	if (tpe1) {
		t->tpe1 = sql_create_subtype(tpe1, 0, 0);
	} else {
		t->tpe1 = NULL;
	}
	if (tpe2) {
		t->tpe2 = sql_create_subtype(tpe2, 0, 0);
	} else {
		t->tpe2 = NULL;
	}
	t->ops = NULL;
	assert(res);
	t->res = sql_create_subtype(res, 0, scale_fixing);
	t->nr = list_length(funcs);
	list_append(funcs, t);
	return t;
}

sql_func *sql_create_func_(char *name, char *imp, list *ops, sql_subtype *res )
{
	sql_func *t = NEW(sql_func);

	assert(res && ops);
	t->name = toLower(name);
	t->imp = _strdup(imp);
	t->tpe1 = NULL;
	t->tpe2 = NULL;
	t->ops = ops;
	t->res = res;
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
	if (t->ops) list_destroy(t->ops);
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

static char *local_result(char *s){
	int i;
	pair type_map [] = {
		{"uchr", "int"},
		{"sht", "int"},
		{"int", "lng"},
		{"lng", "lng"},
		{"flt", "dbl"},
		{"dbl", "dbl"},
		{0, 0}
	};

	for(i = 0; type_map[i].h; i++){
		if (strcmp(s, type_map[i].h) == 0){
			return type_map[i].t;
		}	
	}
	return NULL;
}

void sqltypeinit()
{	int i,j;

	sql_type *ts[100];
	sql_type **misc, **strings, **numerical, **decimals, **floats, **dates;
	sql_type **t, *INT, *BIT, *DBL, *STR;
	sql_type *SEC, *MON, *DTE, *TME, *TMESTAMP;

	misc = t = ts;
	*t++ = sql_create_type("OID", 0, 0, 2, 	  		"oid");
	BIT = *t++ = sql_create_type("BOOLEAN", 0, 0, 2,	"bit");
	*t++ = sql_create_type("BOOL", 0, 0, 2,	  		"bit");
	*t++ = sql_create_type("UBYTE", 2, 0, 2,		"uchr");

	strings = t;
	*t++ = sql_create_type("CHAR", 0, 0, 0,  	"str"); 
	*t++ = sql_create_type("CHARACTER", 0, 0, 0, 	"str");
	*t++ = sql_create_type("VARCHAR", 0, 0, 0, 	"str");

	*t++ = sql_create_type("TEXT", 0, 0, 0, 		"str");
	*t++ = sql_create_type("TINYTEXT", 0, 0, 0, 	"str");
	STR = *t++ = sql_create_type("STRING", 0, 0, 0, 	"str");

	/* INT(n) n <= 2 -> TINYINT
		  n <= 5 -> SMALLINT
		  n <= 9 -> MEDIUMINT
		  n <= 19 -> BIGINT
	*/

	numerical = t;
	*t++ = sql_create_type("TINYINT", 2, 0, 2, 	"sht"); /* sht as sum(uchr) isn't implemented */
	*t++ = sql_create_type("SMALLINT", 5, 0, 2,	"sht");
	INT = *t++ = sql_create_type("MEDIUMINT", 9, 0, 2,	"int");
	*t++ = sql_create_type("INTEGER", 9, 0, 2,	"int");
	*t++ = sql_create_type("NUMBER",  9, 0, 2,	"int");
	*t++ = sql_create_type("BIGINT", 19, 0, 2,	"lng");

	/*sql_type("INT", 2, 0, 2, 		"uchr"); */
	*t++ = sql_create_type("INT", 5, 0, 2,		"sht");
	*t++ = sql_create_type("INT", 9, 0, 2,		"int");
	*t++ = sql_create_type("INT", 19, 0, 2,		"lng");

	decimals = t;
	/* decimal(d,s) (d indicates nr digits, 
			s scale indicates nr of digits after the dot .) */
	/*#sql_type("DECIMAL", 2, 1, 10, 		"uchr"); */
	*t++ = sql_create_type("DECIMAL", 4, 1, 10,		"sht");
	*t++ = sql_create_type("DECIMAL", 9, 1, 10,		"int");
	*t++ = sql_create_type("DECIMAL", 19, 1, 10,		"lng");

	/*sql_create_type("NUMERIC", 2, 1, 		"uchr"); */
	*t++ = sql_create_type("NUMERIC", 4, 1, 10,		"sht");
	*t++ = sql_create_type("NUMERIC", 9, 1, 10,		"int");
	*t++ = sql_create_type("NUMERIC", 19, 1, 10,		"lng");

	/* float(n) (n indicates precision of atleast n digits)*/
	/* ie n <= 23 -> flt */
	/*    n <= 51 -> dbl */
	/*    n <= 62 -> long long dbl (with -ieee) (not supported) */
	/* this requires a type definition */

	floats = t;
	*t++ = sql_create_type("FLOAT", 23, 2, 2, 	"flt");
	*t++ = sql_create_type("FLOAT", 51, 2, 2, 	"dbl");

	DBL = *t++ = sql_create_type("DOUBLE", 51, 2, 2, 	"dbl");
	*t++ = sql_create_type("REAL",   51, 2, 2, 	"dbl");

	dates = t;
	MON = *t++ = sql_create_type("MONTH_INTERVAL", 0, 0, 10, 	"int");
	SEC = *t++ = sql_create_type("SEC_INTERVAL", 0, 0, 10, 	"lng");
	DTE = *t++ = sql_create_type("DATE", 0, 0, 0, 		"date");
	TME = *t++ = sql_create_type("TIME", 0, 0, 0, 		"time");
	*t++ = sql_create_type("DATETIME", 0, 0, 0, 		"datetime");
	TMESTAMP = *t++ = sql_create_type("TIMESTAMP", 0, 0,0, 		"timestamp");
	*t = NULL;

	for(i=0; ts[i]; i++){
		sql_create_func("hash", "hash", ts[i], NULL, INT, SCALE_FIX);
		sql_create_func("=", "=", ts[i], ts[i], BIT, SCALE_FIX);
		sql_create_func("<>", "!=", ts[i], ts[i], BIT, SCALE_FIX);
	}

	for(i=0; ts[i]; i++){
		sql_create_aggr("min", "min", ts[i], ts[i]);
		sql_create_aggr("max", "max", ts[i], ts[i]);
	}

	for(t=numerical; t < decimals; t++){
		char buf[50];
		char *lt = local_result((*t)->name);
		snprintf(buf,50,"sum_%s", lt);
		sql_create_aggr("sum", buf, *t, sql_bind_localtype(lt)->type);
		sql_create_aggr("avg", "avg", *t, DBL);
	}
	for(t=decimals; t < floats; t+=3){
		char buf[50];
		snprintf(buf,50,"sum_%s", (*(t+1))->name);
		sql_create_aggr("sum", buf, *(t), *(t+1));
		snprintf(buf,50,"sum_%s", (*(t+2))->name);
		sql_create_aggr("sum", buf, *(t+1), *(t+2));
		sql_create_aggr("sum", buf, *(t+2), *(t+2));
		sql_create_aggr("avg", "avg", *(t), DBL);
		sql_create_aggr("avg", "avg", *(t+1), DBL);
		sql_create_aggr("avg", "avg", *(t+2), DBL);
	}
	for(t=floats; t < dates; t++){
		char buf[50];
		char *lt = local_result((*t)->name);
		snprintf(buf,50,"sum_%s", lt);
		sql_create_aggr("sum", buf, *t, sql_bind_localtype(lt)->type);
		sql_create_aggr("avg", "avg", *t, DBL);
	}

	sql_create_aggr( "count_no_nil", "count_no_nil", NULL, INT ); 
	sql_create_aggr( "count", "count", NULL, INT ); 

	for(t=numerical; t < dates; t++){
		sql_create_func("sql_sub","-", *t, *t, *t, SCALE_FIX);
		sql_create_func("sql_add","+", *t, *t, *t, SCALE_FIX);
		sql_create_func("sql_mul","*", *t, *t, *t, SCALE_ADD);
		sql_create_func("sql_div","/", *t, *t, *t, SCALE_SUB);
		sql_create_func("sql_max","max", *t, *t, *t, SCALE_FIX);
		sql_create_func("sql_min","min", *t, *t, *t, SCALE_FIX);
		sql_create_func("and","and", *t, *t, *t, SCALE_FIX);
		sql_create_func("or","or", *t, *t, *t, SCALE_FIX);
		sql_create_func("xor","xor", *t, *t, *t, SCALE_FIX);
		sql_create_func("sql_neg","-", *t, NULL, *t, SCALE_FIX);
	}

	/* scale fixing for all numbers */
	for(t=numerical; t < dates; t++){
	      	sql_create_func("scale_up", "*", *t, sql_bind_localtype((*t)->name)->type, *t, SCALE_NONE );
	      	sql_create_func("scale_down", "/", *t, sql_bind_localtype((*t)->name)->type, *t, SCALE_NONE );
	}

	/* convert numericals */
	for(t=numerical; t < dates; t++){
	  sql_type **u;
	  for(u=numerical; u < dates; u++){
	    if (*t != *u)
	      	sql_create_func("convert", (*u)->name, *t, NULL, *u, SCALE_FIX );
	  }
	}


	/* convert strings */
	for(t=strings; t < numerical; t++){
	  sql_type **u;
	  for(u=strings; u < numerical; u++){
	    if (*t != *u)
	      	sql_create_func("convert", (*u)->name, *t, NULL, *u, SCALE_FIX );
	  }
	}
	

	/* convert string to numerical */
	for(t=strings; t < numerical; t++){
	  sql_type **u;
	  for(u=numerical; u < dates; u++){
	      	sql_create_func("convert", (*u)->name, *t, NULL, *u, SCALE_FIX );
	  }
	}

	/* convert numerical to string */
	for(t=numerical; t < dates; t++){
	  sql_type **u;
	  for(u=strings; u < numerical; u++){
	      	sql_create_func("convert", (*u)->name, *t, NULL, *u, SCALE_FIX);
	  }
	}

	for(t=floats; t < dates; t++){
		sql_create_func( "floor", "floor", *t, NULL, *t, SCALE_FIX ); 
		sql_create_func( "ceil", "ceil", *t, NULL, *t, SCALE_FIX ); 
		sql_create_func( "sin", "sin", *t, NULL, *t, SCALE_FIX ); 
		sql_create_func( "cos", "cos", *t, NULL, *t, SCALE_FIX ); 
		sql_create_func( "tan", "tan", *t, NULL, *t, SCALE_FIX ); 
		sql_create_func( "asin", "asin", *t, NULL, *t, SCALE_FIX ); 
		sql_create_func( "acos", "acos", *t, NULL, *t, SCALE_FIX ); 
		sql_create_func( "atan", "atan", *t, NULL, *t, SCALE_FIX ); 
		sql_create_func( "sinh", "sinh", *t, NULL, *t, SCALE_FIX ); 
		sql_create_func( "cosh", "cosh", *t, NULL, *t, SCALE_FIX ); 
		sql_create_func( "tanh", "tanh", *t, NULL, *t, SCALE_FIX ); 
		sql_create_func( "sqrt", "sqrt", *t, NULL, *t, SCALE_FIX ); 
		sql_create_func( "exp", "exp", *t, NULL, *t, SCALE_FIX ); 
		sql_create_func( "log", "log", *t, NULL, *t, SCALE_FIX ); 
		sql_create_func( "log10", "log10", *t, NULL, *t, SCALE_FIX ); 
	}

	sql_create_func( "current_date", "current_date", NULL, NULL, DTE, SCALE_NONE );
	sql_create_func( "current_time", "current_time", NULL, NULL, TME, SCALE_NONE );
	sql_create_func( "current_timestamp", "current_timestamp", NULL, NULL, TMESTAMP, SCALE_NONE );

	sql_create_func( "sql_sub", "date_sub_sec_interval", DTE, SEC, DTE, SCALE_FIX);
	sql_create_func( "sql_sub", "date_sub_month_interval", DTE, MON, DTE, SCALE_FIX);

	sql_create_func( "sql_add", "date_add_sec_interval", DTE, SEC, DTE, SCALE_FIX);
	sql_create_func( "sql_add", "addmonths", DTE, MON, DTE, SCALE_FIX);

	sql_create_func( ">", ">", DTE, DTE, BIT, SCALE_FIX);
	sql_create_func( "<", "<", DTE, DTE, BIT, SCALE_FIX);

	sql_create_func( "year", "year", DTE, NULL, INT, SCALE_FIX);
	sql_create_func( "month", "month", DTE, NULL, INT, SCALE_FIX);

	for(t=strings; t < numerical; t++){
	  sql_create_func_( "substring", "string", 
		list_append(
	 	  list_append( 
	  	    list_append(list_create((fdestroy)&sql_subtype_destroy),
	  	    sql_create_subtype(*t, 0, 0)),
	 	  sql_create_subtype(INT, 0, 0)),
 		sql_create_subtype(INT, 0, 0)), 
	    sql_create_subtype(*t, 0, 0));


	  sql_create_func( "strconcat", "+", *t, *t, *t, SCALE_FIX);
	}
}

void parser_exit()
{
	list_destroy(aggrs);
	list_destroy(funcs);
	list_destroy(types);

	exit_keywords();
}
