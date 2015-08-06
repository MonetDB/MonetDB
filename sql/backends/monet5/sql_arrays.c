#include "monetdb_config.h"
#include "sql_arrays.h"
#include "sql.h"

#define generaliseDimension_analytic(dim) \
({ \
    gdk_analytic_dimension *resDim = GDKmalloc(sizeof(gdk_analytic_dimension)); \
    atom_cast(dim->min, &dim->type); \
    atom_cast(dim->step, &dim->type); \
    atom_cast(dim->max, &dim->type); \
    switch(dim->type.type->localtype) { \
        case TYPE_bte: \
			resDim = createAnalyticDimension_bte(dim->dimnr, dim->min->data.val.btval, dim->max->data.val.btval, dim->step->data.val.btval);\
            break; \
        case TYPE_sht: \
   			resDim = createAnalyticDimension_sht(dim->dimnr, dim->min->data.val.shval, dim->max->data.val.shval, dim->step->data.val.shval); \
            break; \
        case TYPE_int: \
   			resDim = createAnalyticDimension_int(dim->dimnr, dim->min->data.val.ival, dim->max->data.val.ival, dim->step->data.val.ival); \
            break; \
        case TYPE_wrd: \
   			resDim = createAnalyticDimension_wrd(dim->dimnr, dim->min->data.val.wval, dim->max->data.val.wval, dim->step->data.val.wval); \
            break; \
        case TYPE_oid: \
   			resDim = createAnalyticDimension_oid(dim->dimnr, dim->min->data.val.oval, dim->max->data.val.oval, dim->step->data.val.oval); \
            break; \
        case TYPE_lng: \
   			resDim = createAnalyticDimension_lng(dim->dimnr, dim->min->data.val.lval, dim->max->data.val.lval, dim->step->data.val.lval); \
            break; \
        case TYPE_dbl: \
   			resDim = createAnalyticDimension_dbl(dim->dimnr, dim->min->data.val.dval, dim->max->data.val.dval, dim->step->data.val.dval); \
            break; \
        case TYPE_flt: \
   			resDim = createAnalyticDimension_flt(dim->dimnr, dim->min->data.val.fval, dim->max->data.val.fval, dim->step->data.val.fval); \
	        break; \
		default: \
			fprintf(stderr, "generaliseDimension: type not found\n"); \
			resDim = NULL; \
    } \
    resDim; \
})

#define generaliseDimension(dim) \
({ \
    gdk_dimension *resDim = GDKmalloc(sizeof(gdk_dimension)); \
    atom_cast(dim->min, &dim->type); \
    atom_cast(dim->step, &dim->type); \
    atom_cast(dim->max, &dim->type); \
    switch(dim->type.type->localtype) { \
        case TYPE_bte: \
			resDim = createDimension_bte(dim->dimnr, 0, dim->min->data.val.btval, dim->max->data.val.btval, dim->step->data.val.btval);\
            break; \
        case TYPE_sht: \
   			resDim = createDimension_sht(dim->dimnr, 0, dim->min->data.val.shval, dim->max->data.val.shval, dim->step->data.val.shval); \
            break; \
        case TYPE_int: \
   			resDim = createDimension_int(dim->dimnr, 0, dim->min->data.val.ival, dim->max->data.val.ival, dim->step->data.val.ival); \
            break; \
        case TYPE_wrd: \
   			resDim = createDimension_wrd(dim->dimnr, 0, dim->min->data.val.wval, dim->max->data.val.wval, dim->step->data.val.wval); \
            break; \
        case TYPE_oid: \
   			resDim = createDimension_oid(dim->dimnr, 0, dim->min->data.val.oval, dim->max->data.val.oval, dim->step->data.val.oval); \
            break; \
        case TYPE_lng: \
   			resDim = createDimension_lng(dim->dimnr, 0, dim->min->data.val.lval, dim->max->data.val.lval, dim->step->data.val.lval); \
            break; \
        case TYPE_dbl: \
   			resDim = createDimension_dbl(dim->dimnr, 0, dim->min->data.val.dval, dim->max->data.val.dval, dim->step->data.val.dval); \
            break; \
        case TYPE_flt: \
   			resDim = createDimension_flt(dim->dimnr, 0, dim->min->data.val.fval, dim->max->data.val.fval, dim->step->data.val.fval); \
	        break; \
		default: \
			fprintf(stderr, "generaliseDimension: type not found\n"); \
			resDim = NULL; \
    } \
    resDim; \
})

static ptr get_array(sql_table *t) {
	gdk_array *array = arrayNew(t->dimensions.set->cnt);
	node *n;
	int i;

	for(i=0, n=t->dimensions.set->h; n; n=n->next, i++) {
		sql_dimension *dim_sql = (sql_dimension*)n->data;
		array->dimSizes[i] = generaliseDimension(dim_sql);
	}
	
	return array;
}

str mvc_bind_array_dimension(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	ptr* dim_res = getArgReference_ptr(stk, pci, 0);
	ptr* dims_res = getArgReference_ptr(stk, pci, 1);
	mvc *m = NULL;
	str msg;
	sql_schema *s = NULL;
	sql_table *t = NULL;
	sql_dimension *dim = NULL;
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
	dim = mvc_bind_dimension(m, t, *dname);
	if (dim == NULL)
		throw(SQL, "sql.bind_array_dimension", "unable to find %s.%s(%s)", *sname, *tname, *dname);


	*dim_res = generaliseDimension_analytic(dim)	
	*dims_res = get_array(t);

	return MAL_SUCCEED;
}

str mvc_bind_array_column(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	BAT *b = NULL;
	bat *bid = getArgReference_bat(stk, pci, 0);
	ptr* dims_res = getArgReference_ptr(stk, pci, 1);

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

	*dims_res = get_array(t);

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


#if 0
//when the column belongs to an array extra values should be added in order to 
//have values for all cells int he array
BAT* mvc_fill_values(sql_column *c, BAT *b_in, unsigned int cellsNum, void* defVal) {
	BAT *b = NULL;

#define fillVals(TPE, def)                     \
    do {                                \
			TPE *elements_in = NULL, *elements = NULL; \
			BUN i; \
\
        	if((b = BATnew(TYPE_void, TYPE_##TPE, cellsNum, TRANSIENT)) == NULL)   \
        		return NULL;                   \
\
        	elements = (TPE*) Tloc(b, BUNfirst(b));          \
			elements_in = (TPE*) Tloc(b_in, BUNfirst(b_in)); \
\
			/*Add the elements that have been inserted into the cells*/ \
			for(i=0; i<BATcount(b_in); i++) { \
                elements[i] = elements_in[i]; \
    		} \
			/*Fill the rest of the cells with the default value or NULL if no \
 			* default values is provided*/ \
			for(;i<cellsNum; i++) { \
				elements[i] = def; \
			}	\
\
        	b->tsorted = 0;              \
        	b->trevsorted = 0;           \
    } while (0)

	switch (c->type.type->localtype) {
    	case TYPE_bte:
			if(!defVal)
    	    	fillVals(bte, bte_nil);
			else
				fillVals(bte, *(bte*)defVal);
        	break;
    	case TYPE_sht:
			if(!defVal)
				fillVals(sht, sht_nil);
			else
        		fillVals(sht, *(sht*)defVal);
			break;
    	case TYPE_int:
			if(!defVal)
				fillVals(int, int_nil);
			else
        		fillVals(int, *(int*)defVal);
			break;
    	case TYPE_lng:
			if(!defVal)
				fillVals(lng, lng_nil);
			else
        		fillVals(lng, *(lng*)defVal);
			break;
#ifdef HAVE_HGE
    	case TYPE_hge:
			if(!defVal)
				fillVals(hge, hge_nil);
			else
       			fillVals(hge, *(hge*)defVal);
			break;
#endif
		case TYPE_flt:
			if(!defVal)
				fillVals(flt, flt_nil);
			else
        		fillVals(flt, *(flt*)defVal);
			break;
    	case TYPE_dbl:
			if(!defVal)
				fillVals(dbl, dbl_nil);
			else
   				fillVals(dbl, *(dbl*)defVal);
			break;
		case TYPE_str: {
			BATiter iter;
			BUN p = 0, q = 0;

			if((b = BATnew(TYPE_void, TYPE_str, cellsNum, TRANSIENT)) == NULL)   
                return NULL;                   
			
			/* create BAT iterator over the existing vals*/
			iter = bat_iterator(b_in);

			BATloop(b_in, p, q) {
        		char *t = (char *) BUNtail(iter, p);

        		/* insert original head and tail in result BAT */
        		BUNappend(b, t, TRUE);
			}

			for(p=BATcount(b_in); p<cellsNum; p++) {
				if(!defVal)
					BUNappend(b,str_nil, TRUE);
				else
					BUNappend(b, (char*)defVal, TRUE);
			}

            b->tsorted = 0;              
            b->trevsorted = 0;           

			}

			break;
	    default:
			fprintf(stderr, "mvc_fill_values: non-dimensional column type not handled\n");
			return NULL;
	}

	BATsetcount(b,cellsNum);
	BATseqbase(b,0);    
	BATderiveProps(b,FALSE);

	return b;
}

static BAT* mvc_create_cells_bat(mvc *m, char *sname, char *tname, char *cname, void* defVal) { 
	sql_trans *tr = m->session->tr;
	BAT *b = NULL;
	sql_schema *s = NULL;
	sql_table *t = NULL;
	sql_column *c = NULL;

	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		return NULL;
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		return NULL;
	c = mvc_bind_column(m, t, cname);
	if (c == NULL)
		return NULL;

	b = store_funcs.bind_col(tr, c, 0);

	return mvc_fill_values(c, b, t->cellsNum, defVal);
}

static BAT*
mvc_create_dimension_bat(mvc *m, char *sname, char *tname, char *dname) {
	BAT *b = NULL;
	sql_schema *s = NULL;
	sql_table *t = NULL;
	sql_dimension *dim = NULL;

	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		return NULL;
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		return NULL;
	dim = mvc_bind_dimension(m, t, dname);
	if (dim == NULL)
		return NULL;
	
#define createDim(TPE, min, step, max)                     \
    do {                                \
			TPE /*it,*/ *elements = NULL; \
			long repeat1, repeat2, i, j; \
\
			repeat1 = dim->lvl1_repeatsNum; \
			repeat2 = dim->lvl2_repeatsNum; \
\
			if((b = BATnew(TYPE_void, TYPE_##TPE, repeat1+repeat2+1, TRANSIENT)) == NULL) \
        		return NULL;                   \
\
 	      	elements = (TPE*) Tloc(b, BUNfirst(b));          \
			for(i=0; i<repeat1; i++) { \
				*elements = min; \
				elements++; \
			} \
\
			for(j=0; j<repeat2; j++) { \
				*elements = max; \
				elements++; \
			} \
\
			*elements = step; \
\
        	b->tsorted = 0;              \
        	b->trevsorted = 0;           \
			b->batArray =1; \
    } while (0)

	atom_cast(dim->min, &dim->type);
	atom_cast(dim->step, &dim->type);
	atom_cast(dim->max, &dim->type);

	switch (dim->type.type->localtype) {
    	case TYPE_bte:
    	    createDim(bte, dim->min->data.val.btval, dim->step->data.val.btval, dim->max->data.val.btval);
        	break;
    	case TYPE_sht:
        	createDim(sht, dim->min->data.val.shval, dim->step->data.val.shval, dim->max->data.val.shval);
        	break;
    	case TYPE_int:
    		createDim(int, dim->min->data.val.ival, dim->step->data.val.ival, dim->max->data.val.ival);
        	break;
    	case TYPE_lng:
        	createDim(lng, dim->min->data.val.lval, dim->step->data.val.lval, dim->max->data.val.lval);
        	break;
#ifdef HAVE_HGE
    	case TYPE_hge:
        	createDim(hge, dim->min->data.val.hval, dim->step->data.val.hval, dim->max->data.val.hval);
       	break;
#endif
		case TYPE_flt:
        	createDim(flt, dim->min->data.val.fval, dim->step->data.val.fval, dim->max->data.val.fval);
        	break;
    	case TYPE_dbl:
        	createDim(dbl, dim->min->data.val.dval, dim->step->data.val.dval, dim->max->data.val.dval);
        	break;
    	case TYPE_str:
			fprintf(stderr, "mvc_create_dimension_bat: str dimension needs special handling\n");
        	return NULL;
	    default:
			fprintf(stderr, "mvc_create_dimension_bat: dimension type not handled\n");
			return NULL;
	}

//	BATsetcount(b,t->cellsNum);
	BATsetcount(b,dim->lvl1_repeatsNum+dim->lvl2_repeatsNum+1); 
	BATseqbase(b,0);    
	BATderiveProps(b,FALSE);

	return b;
}

static BAT* mvc_subselect_dimension_bat(mvc *m, char* sname, char* tname, char* dname, bat* cand, void* low, void* high, bit li, bit hi, bit anti) {
	BAT *b = NULL, *candBAT, *b_tmp;
	sql_schema *s = NULL;
	sql_table *t = NULL;
	sql_dimension *dim = NULL;
	long elements_in_result = 0;

	s = mvc_bind_schema(m, sname);
	if (s == NULL)
		return NULL;
	t = mvc_bind_table(m, s, tname);
	if (t == NULL)
		return NULL;
	dim = mvc_bind_dimension(m, t, dname);
	if (dim == NULL)
		return NULL;

(void)li;
(void)hi;
(void)anti;
(void)*cand;

#define indices(min, step, max) dim->elementsNum = floor((max-min)/step) +1;

	switch (dim->type.type->localtype) {
    	case TYPE_bte:
    	    indices(dim->min->data.val.btval, dim->step->data.val.btval, dim->max->data.val.btval);
        	break;
    	case TYPE_sht:
        	indices(dim->min->data.val.shval, dim->step->data.val.shval, dim->max->data.val.shval);
        	break;
    	case TYPE_int:
    		indices(dim->min->data.val.ival, dim->step->data.val.ival, dim->max->data.val.ival);
        	break;
    	case TYPE_lng:
        	indices(dim->min->data.val.lval, dim->step->data.val.lval, dim->max->data.val.lval);
        	break;
#ifdef HAVE_HGE
    	case TYPE_hge:
        	indices(dim->min->data.val.hval, dim->step->data.val.hval, dim->max->data.val.hval);
       	break;
#endif
		case TYPE_flt:
        	indices(dim->min->data.val.fval, dim->step->data.val.fval, dim->max->data.val.fval);
        	break;
    	case TYPE_dbl:
        	indices(dim->min->data.val.dval, dim->step->data.val.dval, dim->max->data.val.dval);
        	break;
    	case TYPE_str:
			fprintf(stderr, "mvc_create_dimension_bat: str dimension needs special handling\n");
        	return NULL;
	    default:
			fprintf(stderr, "mvc_create_dimension_bat: dimension type not handled\n");
			return NULL;
	}
	

	if(ATOMcmp(dim->type.type->localtype, low, high) == 0) { //point select
		long element_id, elements_per_group;
		oid *res = NULL;
		long i, j;

		fprintf(stderr, "point select\n");

#define element(TPE, min, step, max, el) \
	do { \
			TPE it; \
			element_id = 0; \
			for(it = min ; it <= max ; it += step) { \
				if(it != el) \
					element_id++; \
				else \
					break; \
            } \
			element_id *= dim->lvl1_repeatsNum; \
		} while(0)


		switch (dim->type.type->localtype) {
        case TYPE_bte:
            element(bte, dim->min->data.val.btval, dim->step->data.val.btval, dim->max->data.val.btval, *(bte*)low);
            break;
        case TYPE_sht:
            element(sht, dim->min->data.val.shval, dim->step->data.val.shval, dim->max->data.val.shval, *(sht*)low);
            break;
        case TYPE_int:
            element(int, dim->min->data.val.ival, dim->step->data.val.ival, dim->max->data.val.ival, *(int*)low);
            break;
        case TYPE_lng:
            element(lng, dim->min->data.val.lval, dim->step->data.val.lval, dim->max->data.val.lval, *(lng*)low);
            break;
#ifdef HAVE_HGE
        case TYPE_hge:
            element(hge, dim->min->data.val.hval, dim->step->data.val.hval, dim->max->data.val.hval, *(hge*)low);
        break;
#endif
        case TYPE_flt:
            element(flt, dim->min->data.val.fval, dim->step->data.val.fval, dim->max->data.val.fval, *(flt*)low);
            break;
        case TYPE_dbl:
            element(dbl, dim->min->data.val.dval, dim->step->data.val.dval, dim->max->data.val.dval, *(dbl*)low);
            break;
        case TYPE_str:
            fprintf(stderr, "mvc_create_dimension_bat: str dimension needs special handling\n");
            return NULL;
        default:
            fprintf(stderr, "mvc_create_dimension_bat: dimension type not handled\n");
            return NULL;
    	}		
	
		elements_per_group = dim->elementsNum * dim->lvl1_repeatsNum;
		fprintf(stderr, "elements per group  %ld\n", elements_per_group);	

		elements_in_result = dim->lvl1_repeatsNum*dim->lvl2_repeatsNum;
		//create new BAT
		if((b_tmp = BATnew(TYPE_void, TYPE_oid, elements_in_result, TRANSIENT)) == NULL)   \
            return NULL; 

		res = (oid*) Tloc(b_tmp, BUNfirst(b_tmp)); 
		//add the oids in the result
		for(j=0; j<dim->lvl2_repeatsNum; j++) {
			for(i=0; i<dim->lvl1_repeatsNum; i++) {
fprintf(stderr, "Added oid: %ld\n", element_id);
				*res = element_id;
				res++;
				element_id++;
			}
 
			element_id += (elements_per_group-dim->lvl1_repeatsNum);
		}
		
		BATsetcount(b_tmp,elements_in_result);

	} else {
		if((b_tmp = BATnew(TYPE_void, TYPE_oid, elements_in_result, TRANSIENT)) == NULL)   \
            return NULL; 

	}

	
	if(cand) {
		oid *current_elements, *cand_elements, *elements;
		oid i, j;

		if ((candBAT = BATdescriptor(*cand)) == NULL) {
			return NULL;
    	}
		
		elements_in_result = (BATcount(b_tmp) > BATcount(candBAT))?BATcount(candBAT):BATcount(b_tmp);

		if((b = BATnew(TYPE_void, TYPE_oid, elements_in_result, TRANSIENT)) == NULL)   \
            return NULL;

		cand_elements = (oid*)Tloc(candBAT, BUNfirst(candBAT));
		current_elements = (oid*)Tloc(b_tmp, BUNfirst(b_tmp));
		elements = (oid*)Tloc(b, BUNfirst(b));
		elements_in_result = 0;

		//compare the results in the two BATs and keep only the common ones
		for(i=0,j=0; i<BATcount(b_tmp) && j<BATcount(candBAT); ) {
			if(cand_elements[j] == current_elements[i]) {		
				elements[elements_in_result] = current_elements[i];
fprintf(stderr, "Final element: %ld\n", current_elements[i]);

				elements_in_result++;
				i++;
				j++;
			} else if(cand_elements[j] < current_elements[i]) 
				j++;
			else
				i++;
		}
	} else
		b = b_tmp;


	BATsetcount(b,elements_in_result);
	BATseqbase(b,0);    
	BATderiveProps(b,FALSE);

	return b;

}

str mvc_create_cells_bat_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	BAT *b = NULL;
	bat *bid = getArgReference_bat(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	str *sname = getArgReference_str(stk, pci, 2);
	str *tname = getArgReference_str(stk, pci, 3);
	str *cname = getArgReference_str(stk, pci, 4);
	ptr def = getArgReference(stk, pci, 5);
	int tpe = getArgType(mb, pci, 5);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	if (ATOMextern(tpe))
		def = *(ptr *) def;

	b = mvc_create_cells_bat(m, *sname, *tname, *cname, def);
	if(b) {
		BBPkeepref(*bid = b->batCacheid);
		return MAL_SUCCEED;
	}
	if (*sname && strcmp(*sname, str_nil) != 0)
		throw(SQL, "sql.create_celss", "unable to find %s.%s(%s)", *sname, *tname, *cname);
	throw(SQL, "sql.create_cells", "unable to find %s(%s)", *tname, *cname);
}

str mvc_get_cells(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	ptr *res = getArgReference_ptr(stk, pci, 0);
	BAT *oidsBAT = NULL; //at this point it is empty. It is just initialised
	bat *oids_bid = getArgReference_bat(stk, pci, 1);
	mvc *m = NULL;
	str msg;
	sql_schema *s = NULL;
	sql_table *t = NULL;
	str *sname = getArgReference_str(stk, pci, 3);
	str *tname = getArgReference_str(stk, pci, 4);

	*res = 0; //not found

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	
	s = mvc_bind_schema(m, *sname);
	if (s == NULL)
		throw(SQL, "sql.get_cells", "unable to find %s.%s", *sname, *tname);
	t = mvc_bind_table(m, s, *tname);
	if (t == NULL)
		throw(SQL, "sql.get_cells", "unable to find %s.%s", *sname, *tname);

	*res = &t->dimensions;

	/*empty BAT*/
	oidsBAT = BATnew(TYPE_void, TYPE_oid, 0, TRANSIENT);
	if(!oidsBAT)
		throw(SQL, "sql.get_cells", "Problem creating new BAT");
	BATseqbase(oidsBAT, 0);
    BATseqbase(BATmirror(oidsBAT), 0);
	BBPkeepref(*oids_bid = oidsBAT->batCacheid);

	return MAL_SUCCEED;
}

str
mvc_create_dimension_bat_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b = NULL;
	bat *bid = getArgReference_bat(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	str *sname = getArgReference_str(stk, pci, 2);
	str *tname = getArgReference_str(stk, pci, 3);
	str *dname = getArgReference_str(stk, pci, 4);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = mvc_create_dimension_bat(m, *sname, *tname, *dname);
	if(b) {
		BBPkeepref(*bid = b->batCacheid);
		return MAL_SUCCEED;
	}
	if (*sname && strcmp(*sname, str_nil) != 0)
		throw(SQL, "sql.create_dimension", "unable to find %s.%s(%s)", *sname, *tname, *dname);
	throw(SQL, "sql.create_dimension", "unable to find %s(%s)", *tname, *dname);
}

str materialiseDimension(bat* res, bat* in) {
	BAT *dimensionBAT, *resBAT;

 	if ((dimensionBAT = BATdescriptor(*in)) == NULL) {
		throw(MAL, "sql.materialise_dimension", RUNTIME_OBJECT_MISSING);
	}

	if(!isBATarray(dimensionBAT)) {
		//ti was planned to be a dimension but somewhere the rules were not
		//suttisfied and it was treated as a normal BAT
		resBAT = dimensionBAT;
		BBPkeepref(*res = resBAT->batCacheid);
    	return MAL_SUCCEED;
	}
	if(BATcount(dimensionBAT)) {
			resBAT=materialiseDimensionBAT(dimensionBAT);
	} else {
		if((resBAT = BATnew(TYPE_void, dimensionBAT->ttype, 0, TRANSIENT)) == NULL)
        	throw(MAL, "sql.materialise_dimension", "Unable to create output BAT");
        BATseqbase(resBAT,0);
        BATsetcount(resBAT,0);
		BATderiveProps(resBAT, FALSE);
	}
	
	BBPunfix(dimensionBAT->batCacheid);

	BBPkeepref(*res = resBAT->batCacheid);
    return MAL_SUCCEED;

}

str
mvc_dimension_subselect_with_cand_bat_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b = NULL;
	bat *bid = getArgReference_bat(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	str *sname = getArgReference_str(stk, pci, 2);
	str *tname = getArgReference_str(stk, pci, 3);
	str *dname = getArgReference_str(stk, pci, 4);
	bat* cand = getArgReference(stk, pci, 5);
	ptr low = getArgReference(stk, pci, 6);
	ptr high = getArgReference(stk, pci, 7);
	bit* li = getArgReference_bit(stk, pci, 8);
	bit* hi = getArgReference_bit(stk, pci, 9);
	bit* anti = getArgReference_bit(stk, pci, 10);


	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = mvc_subselect_dimension_bat(m, *sname, *tname, *dname, cand, low, high, *li, *hi, *anti);
	if(b) {
		BBPkeepref(*bid = b->batCacheid);
		return MAL_SUCCEED;
	}
	if (*sname && strcmp(*sname, str_nil) != 0)
		throw(SQL, "sql.dimension_subselect", "unable to find %s.%s(%s)", *sname, *tname, *dname);
	throw(SQL, "sql.dimension_subselect", "unable to find %s(%s)", *tname, *dname);
}

str
mvc_dimension_subselect_bat_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b = NULL;
	bat *bid = getArgReference_bat(stk, pci, 0);
	mvc *m = NULL;
	str msg;
	str *sname = getArgReference_str(stk, pci, 2);
	str *tname = getArgReference_str(stk, pci, 3);
	str *dname = getArgReference_str(stk, pci, 4);
	ptr low = getArgReference(stk, pci, 5);
	ptr high = getArgReference(stk, pci, 6);
	bit* li = getArgReference_bit(stk, pci, 7);
	bit* hi = getArgReference_bit(stk, pci, 8);
	bit* anti = getArgReference_bit(stk, pci, 9);


	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	b = mvc_subselect_dimension_bat(m, *sname, *tname, *dname, NULL, low, high, *li, *hi, *anti);
	if(b) {
		BBPkeepref(*bid = b->batCacheid);
		return MAL_SUCCEED;
	}
	if (*sname && strcmp(*sname, str_nil) != 0)
		throw(SQL, "sql.dimension_subselect", "unable to find %s.%s(%s)", *sname, *tname, *dname);
	throw(SQL, "sql.dimension_subselect", "unable to find %s(%s)", *tname, *dname);
}
#endif
