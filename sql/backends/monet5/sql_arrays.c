#include "monetdb_config.h"
#include "sql_arrays.h"
#include "sql.h"

static gdk_analytic_dimension* generaliseDimension_analytic(sql_dimension *dim) {
    atom_cast(dim->min, &dim->type);
    atom_cast(dim->step, &dim->type);
    atom_cast(dim->max, &dim->type);
    switch(dim->type.type->localtype) {
        case TYPE_bte:
			return createAnalyticDimension_bte(dim->dimnr, dim->min->data.val.btval, dim->max->data.val.btval, dim->step->data.val.btval);
        case TYPE_sht:
   			return createAnalyticDimension_sht(dim->dimnr, dim->min->data.val.shval, dim->max->data.val.shval, dim->step->data.val.shval);
        case TYPE_int:
   			return createAnalyticDimension_int(dim->dimnr, dim->min->data.val.ival, dim->max->data.val.ival, dim->step->data.val.ival);
        case TYPE_wrd:
   			return createAnalyticDimension_wrd(dim->dimnr, dim->min->data.val.wval, dim->max->data.val.wval, dim->step->data.val.wval);
        case TYPE_oid:
   			return createAnalyticDimension_oid(dim->dimnr, dim->min->data.val.oval, dim->max->data.val.oval, dim->step->data.val.oval);
        case TYPE_lng:
   			return createAnalyticDimension_lng(dim->dimnr, dim->min->data.val.lval, dim->max->data.val.lval, dim->step->data.val.lval);
        case TYPE_dbl:
   			return createAnalyticDimension_dbl(dim->dimnr, dim->min->data.val.dval, dim->max->data.val.dval, dim->step->data.val.dval);
        case TYPE_flt:
   			return createAnalyticDimension_flt(dim->dimnr, dim->min->data.val.fval, dim->max->data.val.fval, dim->step->data.val.fval);
		default:
			fprintf(stderr, "generaliseDimension_analytic: type not found\n");
			return NULL; \
    } 
    return NULL;
}

static gdk_dimension* generaliseDimension(sql_dimension *dim)  {
    atom_cast(dim->min, &dim->type);
    atom_cast(dim->step, &dim->type);
    atom_cast(dim->max, &dim->type);
    switch(dim->type.type->localtype) {
        case TYPE_bte:
			return createDimension_bte(dim->min->data.val.btval, dim->max->data.val.btval, dim->step->data.val.btval);
        case TYPE_sht:
   			return createDimension_sht(dim->min->data.val.shval, dim->max->data.val.shval, dim->step->data.val.shval); 
        case TYPE_int:
   			return createDimension_int(dim->min->data.val.ival, dim->max->data.val.ival, dim->step->data.val.ival);
        case TYPE_wrd:
   			return createDimension_wrd(dim->min->data.val.wval, dim->max->data.val.wval, dim->step->data.val.wval);
        case TYPE_oid:
   			return createDimension_oid(dim->min->data.val.oval, dim->max->data.val.oval, dim->step->data.val.oval);
        case TYPE_lng:
   			return createDimension_lng(dim->min->data.val.lval, dim->max->data.val.lval, dim->step->data.val.lval);
        case TYPE_dbl:
   			return createDimension_dbl(dim->min->data.val.dval, dim->max->data.val.dval, dim->step->data.val.dval);
        case TYPE_flt:
   			return createDimension_flt(dim->min->data.val.fval, dim->max->data.val.fval, dim->step->data.val.fval);
		default:
			fprintf(stderr, "generaliseDimension: type not found\n");
			return NULL;
    }
	return NULL;
}

static gdk_array* get_array(sql_table *t) {
	gdk_array *array = arrayNew(t->dimensions.set->cnt);
	node *n;
	unsigned short i;

	for(i=0, n=t->dimensions.set->h; n; n=n->next, i++) {
		sql_dimension *dim_sql = (sql_dimension*)n->data;
		array->dims[i] = generaliseDimension(dim_sql);
	}
	
	return array;
}

str mvc_bind_array_dimension(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	ptr* dim = getArgReference_ptr(stk, pci, 0);
	ptr* array = getArgReference_ptr(stk, pci, 1);
	mvc *m = NULL;
	str msg;
	sql_schema *s = NULL;
	sql_table *t = NULL;
	sql_dimension *d = NULL;
	str *sname = getArgReference_str(stk, pci, 3);
	str *tname = getArgReference_str(stk, pci, 4);
	str *dname = getArgReference_str(stk, pci, 5);
	
	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	
	s = mvc_bind_schema(m, *sname);
	if (s == NULL)
		throw(SQL, "sql.bind_array_dimension", "unable to find %s.%s(%s)", *sname, *tname, *dname);
	t = mvc_bind_table(m, s, *tname);
	if (t == NULL)
		throw(SQL, "sql.bind_array_dimension", "unable to find %s.%s(%s)", *sname, *tname, *dname);
	d = mvc_bind_dimension(m, t, *dname);
	if (dim == NULL)
		throw(SQL, "sql.bind_array_dimension", "unable to find %s.%s(%s)", *sname, *tname, *dname);

	*array = get_array(t);
	*dim = generaliseDimension_analytic(d);	

	return MAL_SUCCEED;
}

str mvc_bind_array_column(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	BAT *b = NULL;
	bat *bid = getArgReference_bat(stk, pci, 0);
	ptr* array = getArgReference_ptr(stk, pci, 1);

	mvc *m = NULL;
	str msg;
	str *sname = getArgReference_str(stk, pci, 3);
	str *tname = getArgReference_str(stk, pci, 4);
	str *cname = getArgReference_str(stk, pci, 5);
	
	sql_schema *s = NULL;
	sql_table *t = NULL;
	sql_column *c = NULL;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	s = mvc_bind_schema(m, *sname);
	if (s == NULL)
		throw(SQL, "sql.bind_array_column", "unable to find %s.%s(%s)", *sname, *tname, *cname);
	t = mvc_bind_table(m, s, *tname);
	if (t == NULL)
		throw(SQL, "sql.bind_array_column", "unable to find %s.%s(%s)", *sname, *tname, *cname);
	c = mvc_bind_column(m, t, *cname);
	if (c == NULL)
		throw(SQL, "sql.bind_array_column", "unable to find %s.%s(%s)", *sname, *tname, *cname);

	*array = get_array(t);

	/*bind the column*/
	b = store_funcs.bind_col(m->session->tr, c, 0);

	if(b) {
		BBPkeepref(*bid = b->batCacheid);
		return MAL_SUCCEED;
	}
	if (*sname && strcmp(*sname, str_nil) != 0)
		throw(SQL, "sql.bind_array_column", "unable to find %s.%s(%s)", *sname, *tname, *cname);
	throw(SQL, "sql.bind_array_column", "unable to find %s(%s)", *tname, *cname);
}
