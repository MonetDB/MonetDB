#include "monetdb_config.h"
#include "gdk_arrays.h"

#define createAnalyticDim(TPE) \
gdk_analytic_dimension* createAnalyticDimension_##TPE(unsigned short dimNum, TPE min, TPE max, TPE step) { \
	gdk_analytic_dimension *dim = GDKmalloc(sizeof(gdk_analytic_dimension)); \
\
	void *minVoid = GDKmalloc(sizeof(TPE)); \
    void *maxVoid = GDKmalloc(sizeof(TPE)); \
    void *stepVoid = GDKmalloc(sizeof(TPE)); \
\
	if(!dim || !minVoid || !maxVoid || !stepVoid) { \
		if(dim) \
			GDKfree(dim); \
		if(minVoid) \
			GDKfree(minVoid); \
		if(maxVoid) \
			GDKfree(maxVoid); \
		if(stepVoid) \
			GDKfree(stepVoid); \
		return NULL; \
	} \
    memcpy(minVoid, &min, sizeof(TPE)); \
    memcpy(maxVoid, &max, sizeof(TPE)); \
    memcpy(stepVoid, &step, sizeof(TPE)); \
\
	dim->type = TYPE_##TPE; \
	dim->dimNum = dimNum; \
	dim->min = minVoid; \
    dim->max = maxVoid; \
    dim->step = stepVoid; \
	dim->elsNum = floor((max - min )/ step)+1; \
\
	return dim; \
}

createAnalyticDim(bte);
createAnalyticDim(sht);
createAnalyticDim(int);
createAnalyticDim(wrd);
createAnalyticDim(oid);
createAnalyticDim(lng);
createAnalyticDim(dbl);
createAnalyticDim(flt);

#define createDim(TPE) \
gdk_dimension* createDimension_##TPE(TPE min, TPE max, TPE step) { \
	gdk_dimension *dim = GDKmalloc(sizeof(gdk_dimension)); \
	dim->min = 0; \
    dim->max = floor((max - min ) / step); \
	dim->step = 1; \
	dim->elsNum = dim->max +1; \
	dim->idxs = NULL; \
	return dim; \
}

createDim(bte);
createDim(sht);
createDim(int);
createDim(wrd);
createDim(oid);
createDim(lng);
createDim(dbl);
createDim(flt);


gdk_array* arrayNew(unsigned short dimsNum) {
	gdk_array *array = (gdk_array*)GDKmalloc(sizeof(gdk_array));
	if(!array)
		return NULL;
	array->dimsNum = dimsNum;
	array->dims = (gdk_dimension**)GDKmalloc(sizeof(gdk_dimension*)*dimsNum);
	if(!array->dims)
		return NULL;
	return array;
}

gdk_array* arrayCopy(gdk_array *array) {
	unsigned short i;
	gdk_array *array_copy = arrayNew(array->dimsNum);
	if(!array_copy)
		return NULL;
	for(i=0; i<array_copy->dimsNum; i++) {
		array_copy->dims[i] = createDimension_oid(array->dims[i]->min, array->dims[i]->max, array->dims[i]->step);
	}

	return array_copy;
}

gdk_return arrayDelete(gdk_array *array) {
	unsigned short i=0;
	for(i=0; i<array->dimsNum; i++) {
		if(array->dims[i]->idxs)
			GDKfree(array->dims[i]->idxs);
		GDKfree(array->dims[i]);
	}
	GDKfree(array->dims);
	GDKfree(array);

	return GDK_SUCCEED;
}

gdk_return analyticDimensionDelete(gdk_analytic_dimension *dim) {
	GDKfree(dim->min);
	GDKfree(dim->max);
	GDKfree(dim->step);
	GDKfree(dim);

	return GDK_SUCCEED;
}


gdk_return gdk_error_msg(errors errorCode, const char* funcName, const char *msg) {
	switch(errorCode) {
		case general_error:
			GDKerror("%s :%s", funcName, msg);
		case new_bat:
			GDKerror("%s: Problem allocating space for new BAT", funcName);
		case dimension_type:
			GDKerror("%s: Dimension type not handled", funcName);
		case value_type:
			GDKerror("%s: Column type not handled", funcName);
		default:
			GDKerror("%s: Unknown Problem", funcName);				
	}
	return GDK_FAIL;
}

BAT* materialise_nonDimensional_column(int columntype, unsigned int cellsNum, char* defVal) {
    BAT *b = NULL;

#define fillVals(TPE, def)                     \
    do {                                \
            TPE *elements = NULL; \
            BUN i; \
\
            if((b = BATnew(TYPE_void, TYPE_##TPE, cellsNum, TRANSIENT)) == NULL)   \
                return NULL;                   \
\
            elements = (TPE*) Tloc(b, BUNfirst(b));          \
\
            /*Fill the rest of the cells with the default value or NULL if no \
 *  *             * default values is provided*/ \
            for(i=0;i<cellsNum; i++) { \
                elements[i] = def; \
            }   \
\
            b->tsorted = 0;              \
            b->trevsorted = 0;           \
    } while (0)

	switch (columntype) {
        case TYPE_bte: {
            bte val = bte_nil;
            if(defVal)
                val = atoi(defVal);
            fillVals(bte, val);
        }   break;
        case TYPE_sht: {
            short val = sht_nil;
            if(defVal)
                val = atoi(defVal);
            fillVals(sht, val);
        }   break;
        case TYPE_int: {
            int val = int_nil;
            if(defVal)
                val = atoi(defVal);
            fillVals(int, val);
        }   break;
        case TYPE_lng: {
            long val = lng_nil;
            if(defVal)
                val = atol(defVal);
            fillVals(lng, val);
        }   break;
#ifdef HAVE_HGE
        case TYPE_hge: {
            hge val = hge_nil;
            if(defVal)
                val = atol(defVal);
            fillVals(hge, val);
        }   break;
#endif
        case TYPE_flt: {
            float val = flt_nil;
            if(defVal)
                val = atof(defVal);
            fillVals(flt, val);
        }    break;
        case TYPE_dbl: {
            double val = dbl_nil;
            if(defVal)
                val = atof(defVal);
            fillVals(dbl, val);
        }    break;
        case TYPE_str: {
            BUN i;

            if((b = BATnew(TYPE_void, TYPE_str, cellsNum, TRANSIENT)) == NULL)
                return NULL;

            /*Fill the rest of the cells with the default value or NULL if no \
 *              * default values is provided*/
            for(i=0; i<cellsNum; i++) {
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
            fprintf(stderr, "materialise_nonDimensional_column: non-dimensional column type not handled\n");
            return NULL;
    }

    BATsetcount(b,cellsNum);
    BATseqbase(b,0);
    BATderiveProps(b,FALSE);

    return b;
}

/*I need the array to access the info of the dimensions that are not in the dimCands */
BAT *projectCells(gdk_array* dimCands, gdk_array *array) {
	BAT *resBAT = NULL;
	unsigned int resSize = 1;
	oid *resOIDs = NULL;

	unsigned int i=0, j=0, k=0;
	unsigned int elsR = 1, elsRi=0;
	unsigned int jumpSize = 1;
	gdk_dimension *dim;

	for(i=0; i<dimCands->dimsNum; i++)
		resSize*=dimCands->dims[i]->elsNum;

	if(!(resBAT = BATnew(TYPE_void, TYPE_oid, resSize, TRANSIENT)))
		return NULL;
	resOIDs = (oid*)Tloc(resBAT, BUNfirst(resBAT));

	//add the oids as defined usnig only the first dimension
	/* in the first dimension each element is repeated only once
 	* so we just need to make sure that we repeat the indices as many times
 	* as needed to fill the resOIDs */
	dim = dimCands->dims[0];
	if(dim->idxs) {
//		for(j=0; j<resSize;)
		/*add each element one */
		for(i=0; i<dim->elsNum ; i++, j++)
			resOIDs[j] = dim->idxs[i];
	} else {
//		for(j=0; j<resSize;)
		/* add each element once */
		for(i=dim->min; i<=dim->max ; i+=dim->step, j++)
			resOIDs[j] = i;
	}
	/* iterate over the rest of the dimensoins and add the values needed to make the correct oids */
	for(i=1; i<dimCands->dimsNum; i++) {
		dim = dimCands->dims[i];
		jumpSize *= array->dims[i-1]->elsNum; //each oid is increased by jumpSize * the idx of the dimension
		elsR *= dimCands->dims[i-1]->elsNum; //each element is repeated elsR times, this equals the number of elements that have been aready in the array

		if(dim->idxs) {
//			for(j=0; j<resSize; ) //until all elements have been set, repeat the groups
			for(k=1; k<dim->elsNum; k++) //skip the first qualifying idx to avoid updating cells which need to be re-used
				for(elsRi=0; elsRi<elsR; elsRi++, j++) //repeat it elsR times
					resOIDs[j] = resOIDs[elsRi] + jumpSize*dim->idxs[k];
			/* now update the first elsR values */
			for(elsRi=0; elsRi<elsR; elsRi++)
				resOIDs[elsRi] += jumpSize*dim->idxs[0];

		} else {
//			for(j=0; j<resSize; ) //until all elements have been set, repeat the groups
			for(k=dim->min+dim->step; k<=dim->max; k+=dim->step) //skip the first value to avoid updating cells which need to be re-used
				for(elsRi=0; elsRi<elsR; elsRi++, j++) //repeat it jumpSize times
					resOIDs[j] = resOIDs[elsRi]+jumpSize*k;
			/* update the first elsR elements */
			for(elsRi=0; elsRi<elsR; elsRi++, j++) //repeat it jumpSize times
				resOIDs[elsRi] += jumpSize*dim->min;
		}
	}

	BATsetcount(resBAT, resSize);
	BATseqbase(resBAT, 0);
	BATderiveProps(resBAT, FALSE);

	return resBAT;
}


#if 0
/*
#define valueInOID(oidVal, min, max, step) \
do {\
	
} while(0);

BAT* projectDimension(gdk_dimension *oidsDim, gdk_dimension *valuesDim) {
	switch(ATOMtype(valuesDim->type)) {
        case TYPE_bte:
            dim->elementsNum = floor((*(bte*)max - *(bte*)min )/ *(bte*)step)+1;
            break;
        case TYPE_sht:
            dim->elementsNum = floor((*(sht*)max - *(sht*)min )/ *(sht*)step)+1;
            break;
        case TYPE_int:
            dim->elementsNum = floor((*(int*)max - *(int*)min )/ *(int*)step)+1;
            break;
        case TYPE_flt:
            dim->elementsNum = floor((*(flt*)max - *(flt*)min )/ *(flt*)step)+1;
            break;
        case TYPE_dbl:
            dim->elementsNum = floor((*(dbl*)max - *(dbl*)min )/ *(dbl*)step)+1;
            break;
        case TYPE_lng:
            dim->elementsNum = floor((*(lng*)max - *(lng*)min )/ *(lng*)step)+1;
            break;
#ifdef HAVE_HGE
        case TYPE_hge:
            dim->elementsNum = floor((*(hge*)max - *(hge*)min )/ *(hge*)step)+1;
            break;
#endif
        case TYPE_oid:
            dim->elementsNum = floor((*(oid*)max - *(oid*)min )/ *(oid*)step)+1;
#if SIZEOF_OID == SIZEOF_INT
#else
            dim->elementsNum = floor((*(int*)max - *(int*)min )/ *(int*)step)+1;
#endif
            break;
        default:
            fprintf(stderr, "projectDimension: dimension type not handled\n");
            return NULL;
    }

    return dim;
}
*/

#define createDimension(TPE, min, max, step, elementRepeats, groupRepeats) \
    ({ \
        long i; \
        TPE* vls; \
        BAT *resBAT = BATnew(TYPE_void, TYPE_##TPE, elementRepeats+groupRepeats+1, TRANSIENT); \
        if(resBAT) { \
fprintf(stderr, "createDimension: %ld total elements\n", (elementRepeats+groupRepeats+1)); \
            vls = (TPE*)Tloc(resBAT, BUNfirst(resBAT)); \
            for(i=0; i<elementRepeats; i++) { \
                *vls = min; \
                vls++; \
            } \
            for(i=0; i<groupRepeats; i++) { \
                *vls = max; \
                vls++; \
            } \
            *vls = step; \
\
            BATsetcount(resBAT,elementRepeats+groupRepeats+1); \
            BATseqbase(resBAT,0); \
            BATderiveProps(resBAT,FALSE); \
            resBAT->batArray=1; \
        } \
        resBAT; \
    })

#define dimensionBATsizeTPE(TPE, dimensionBAT) \
    ({\
        TPE min, max, step; \
        long elementRepeats, groupRepeats; \
        BUN elementsNum; \
        dimensionCharacteristics(TPE, dimensionBAT, &min, &max, &step, &elementRepeats, &groupRepeats); \
        elementsNum = dimensionElementsNum(min, max, step); \
        elementsNum*elementRepeats*groupRepeats; \
    })


#define materialiseDimensionTPE(TPE, dimensionBAT) \
    ({ \
        /*find the min, max, step in the dimension*/ \
        long elementRepeats, groupRepeats, elementsNum; \
        TPE min, max, step; \
        oid i, j; \
        TPE *el_out, el; \
        BAT *resBAT; \
\
        dimensionCharacteristics(TPE, dimensionBAT, &min, &max, &step, &elementRepeats, &groupRepeats); \
/*fprintf(stderr, "materialise: elementRepeats = %ld - groupRepeats = %ld\n", elementRepeats, groupRepeats); */\
        elementsNum = dimensionElementsNum(min, max, step); \
/*fprintf(stderr, "materialise elementsNum = %ld\n", elementsNum); */\
\
        if((resBAT = BATnew(TYPE_void, TYPE_##TPE, elementRepeats*elementsNum*groupRepeats, TRANSIENT)) == NULL) \
            GDKerror("materialiseDimensionTPE: Unable to create output BAT"); \
\
        el_out = (TPE*)Tloc(resBAT, BUNfirst(resBAT)); \
        for(j=0; j<(unsigned long)groupRepeats; j++) { \
/*fprintf(stderr, "materialise: group repetition %ld\n", j); */\
            for(el=min; el<=max; el+=step) { \
                for(i=0; i<(unsigned long)elementRepeats; i++) { \
/*fprintf(stderr, "materialise: element repetition %ld\n", i); */\
                    *el_out = el; \
                    el_out++; \
                } \
            } \
        } \
\
        BATseqbase(resBAT,0); \
        BATsetcount(resBAT, elementRepeats*elementsNum*groupRepeats);                  \
        BATderiveProps(resBAT,FALSE); \
        resBAT; \
    })

/*materialise a dimension BAT*/
BAT* materialiseDimensionBAT(BAT *dimensionBAT) {
    if(!isBATarray(dimensionBAT))
        return dimensionBAT;

     switch(ATOMtype(dimensionBAT->ttype)) {
        case TYPE_bte:
            return materialiseDimensionTPE(bte, dimensionBAT);
        case TYPE_sht:
            return materialiseDimensionTPE(sht, dimensionBAT);
        case TYPE_int:
            return materialiseDimensionTPE(int, dimensionBAT);
        case TYPE_flt:
            return materialiseDimensionTPE(flt, dimensionBAT);
        case TYPE_dbl:
            return materialiseDimensionTPE(dbl, dimensionBAT);
        case TYPE_lng:
            return materialiseDimensionTPE(lng, dimensionBAT);
#ifdef HAVE_HGE
        case TYPE_hge:
            return materialiseDimensionTPE(hge, dimensionBAT);
#endif
        case TYPE_oid:
#if SIZEOF_OID == SIZEOF_INT
            return materialiseDimensionTPE(int, dimensionBAT);
#else
            return materialiseDimensionTPE(lng, dimensionBAT);
#endif
        break;
        default:
            fprintf(stderr, "materialiseDimensionBAT: dimension type not handled\n");
            return NULL;
    }
    return NULL;
}


/*the size of the BAT after the materialisation of the dimension*/
BUN dimensionBATsize(BAT *dimensionBAT) {
    if(!isBATarray(dimensionBAT))
        return BATcount(dimensionBAT);

     switch(ATOMtype(dimensionBAT->ttype)) {
        case TYPE_bte:
            return dimensionBATsizeTPE(bte, dimensionBAT);
        case TYPE_sht:
            return dimensionBATsizeTPE(sht, dimensionBAT);
        case TYPE_int:
            return dimensionBATsizeTPE(int, dimensionBAT);
        case TYPE_flt:
            return dimensionBATsizeTPE(flt, dimensionBAT);
        case TYPE_dbl:
            return dimensionBATsizeTPE(dbl, dimensionBAT);
        case TYPE_lng:
            return dimensionBATsizeTPE(lng, dimensionBAT);
#ifdef HAVE_HGE
        case TYPE_hge:
            return dimensionBATsizeTPE(hge, dimensionBAT);
#endif
        case TYPE_oid:
#if SIZEOF_OID == SIZEOF_INT
            return dimensionBATsizeTPE(int, dimensionBAT);
#else
            return dimensionBATsizeTPE(lng, dimensionBAT);
#endif
        break;
        default:
            fprintf(stderr, "dimensionBATsize: dimension type not handled\n");
    }
    return 0;
}


/*The number of indices in the dimension, i.e. dimension[1:2:7] -> 4 elements (1,3,5,7)*/
BUN dimensionBATelementsNum(BAT* dimensionBAT) {
#define num(TPE) \
    ({ \
        TPE min, max, step; \
        long elementRepeats, groupRepeats; \
        dimensionCharacteristics(TPE, dimensionBAT, &min, &max, &step, &elementRepeats, &groupRepeats); \
        dimensionElementsNum(min, max, step); \
    })
    switch(ATOMtype(BATttype(dimensionBAT))) {
        case TYPE_bte:
            return num(bte);
        case TYPE_sht:
            return num(sht);
        case TYPE_int:
            return num(int);
        case TYPE_flt:
            return num(flt);
        case TYPE_dbl:
            return num(dbl);
        case TYPE_lng:
            return num(lng);
#ifdef HAVE_HGE
        case TYPE_hge:
            return num(hge);
#endif
        case TYPE_oid:
#if SIZEOF_OID == SIZEOF_INT
            return num(int);
#else
            return num(lng);
#endif
        break;
        default:
            fprintf(stderr, "dimensionBATelementsNum: dimension type not handled\n");
    }

    return 0;
}


/*get the oids in the tail of oidsBAT and for each oid the correposdig value from 
 * dimensionBAT. Set the the oid position in resBAT to this value */
BUN dimension_void_replace_bat(BAT *resBAT, BAT *oidsBAT, BAT *dimensionBAT, bit force) {
    BUN nr = 0;
    BUN r, s;
    BATiter oidsBAT_iter = bat_iterator(oidsBAT);

#define replace(TPE) \
    do {\
        TPE dimMin, dimMax, dimStep; \
        long dimGroupRepeats, dimElementRepeats; \
        dimensionCharacteristics(TPE, dimensionBAT, &dimMin, &dimMax, &dimStep, &dimElementRepeats, &dimGroupRepeats); \
\
fprintf(stderr, "%d-%d-%d, %ld, %ld\n", (int)dimMin, (int)dimStep, (int)dimMax, dimElementRepeats, dimGroupRepeats); \
        BATloop(oidsBAT, r, s) { \
            oid updid = *(oid *) BUNtail(oidsBAT_iter, r); \
            TPE val = dimensionElement(dimMin, dimMax, dimStep, dimElementRepeats, r); \
\
            if (void_inplace(resBAT, updid, &val, force) == GDK_FAIL) \
                return BUN_NONE; \
            nr++; \
        } \
    } while(0)

    switch (ATOMtype(ATOMbasetype(dimensionBAT->ttype))) {
        case TYPE_bte:
            replace(bte);
            break;
        case TYPE_sht:
            replace(sht);
            break;
        case TYPE_int:
            replace(int);
            break;
        case TYPE_flt:
            replace(flt);
            break;
        case TYPE_dbl:
            replace(dbl);
            break;
        case TYPE_lng:
            replace(lng);
            break;
#ifdef HAVE_HGE
        case TYPE_hge:
            replace(hge);
            break;
#endif
        case TYPE_oid:
#if SIZEOF_OID == SIZEOF_INT
            replace(int);
#else
            replace(lng);
#endif
        break;
        default:
            fprintf(stderr, "dimension_void_replace_bat: dimension type not handled\n");
            return BUN_NONE;
    }
    return nr;
}


gdk_return dimensionBATsubselect(BAT** outBAT, BAT *dimensionBAT, BAT *candsBAT, const void *low, const void *high, int includeLow, int includeHigh, int anti) {
	oid elements_in_result =0;
	int type;
	const void *nil;
	BAT *b_tmp, *resBAT;

	long elementRepeats, groupRepeats, elementsNum, qualifyingElementsNum;
	long element_oid =0;
	long i,j;
	oid *res = NULL;

	type = dimensionBAT->ttype;
	nil = ATOMnilptr(type);
	type = ATOMbasetype(type);

	if(ATOMcmp(type, low, high) == 0) { //point selection	
#define equal(TPE, el) \
	do { \
		TPE min, max, step; \
		dimensionCharacteristics(TPE, dimensionBAT, &min, &max, &step, &elementRepeats, &groupRepeats); \
		elementsNum = dimensionElementsNum(min, max, step); \
		element_oid = dimensionFndValuePos(el, min, step); \
	} while(0)
		
		switch (ATOMtype(type)) {
		case TYPE_bte:
        	equal(bte, *(bte*)low);
	        break;
	    case TYPE_sht:
    	    equal(sht, *(sht*)low);
	        break;
    	case TYPE_int:
        	equal(int, *(int*)low);
	        break;
    	case TYPE_lng:
        	equal(lng, *(lng*)low);
	       break;
#ifdef HAVE_HGE
    	case TYPE_hge:
        	equal(hge, *(hge*)low);
	        break;
#endif
    	case TYPE_flt:
        	equal(flt, *(flt*)low);
	        break;
    	case TYPE_dbl:
        	equal(dbl, *(dbl*)low);
	        break;
    	case TYPE_oid:
        	equal(oid, *(oid*)low);
	        break;
		default:
			return gdk_error_msg(dimension_type, "dimensionBATsubselect", NULL);
    	}
		if(element_oid >= elementsNum && !anti) {//the element does not exist
			if(!(resBAT = BATnew(TYPE_void, TYPE_void, 0, TRANSIENT)))
				return gdk_error_msg(new_bat, "dimensionBATsubselect", NULL);
    		BATseqbase(resBAT, 0);
    		BATseqbase(BATmirror(resBAT), 0);

			*outBAT = resBAT;
			return GDK_SUCCEED;
		} else if(element_oid >= elementsNum && anti)
			qualifyingElementsNum = 0;	
		else
			qualifyingElementsNum =1;
	} else if(ATOMcmp(type, high, nil) == 0) { //find values greater than low
#define greater(TPE, el) \
	do { \
		TPE min, max, step; \
		dimensionCharacteristics(TPE, dimensionBAT, &min, &max, &step, &elementRepeats, &groupRepeats); \
		elementsNum = dimensionElementsNum(min, max, step); \
		element_oid = dimensionFndGreaterValuePos(el, min, step, includeLow>0); \
	} while(0)
		
		switch (ATOMtype(type)) {
		case TYPE_bte:
        	greater(bte, *(bte*)low);
	        break;
	    case TYPE_sht:
    	    greater(sht, *(sht*)low);
	        break;
    	case TYPE_int:
        	greater(int, *(int*)low);
	        break;
    	case TYPE_lng:
        	greater(lng, *(lng*)low);
	       break;
#ifdef HAVE_HGE
    	case TYPE_hge:
        	greater(hge, *(hge*)low);
	        break;
#endif
    	case TYPE_flt:
        	greater(flt, *(flt*)low);
	        break;
    	case TYPE_dbl:
        	greater(dbl, *(dbl*)low);
	        break;
    	case TYPE_oid:
        	greater(oid, *(oid*)low);
	        break;
		default:
			return gdk_error_msg(dimension_type, "dimensionBATsubselect", NULL);
    	}
		if(element_oid >= elementsNum && !anti) { //low greater than max
			if(!(resBAT = BATnew(TYPE_void, TYPE_void, 0, TRANSIENT)))
				return gdk_error_msg(new_bat, "dimensionBATsubselect", NULL);
    		BATseqbase(resBAT, 0);
    		BATseqbase(BATmirror(resBAT), 0);

			*outBAT = resBAT;
			return GDK_SUCCEED;
		}
		qualifyingElementsNum = (elementsNum-element_oid);
	} else if(ATOMcmp(type, low, nil) == 0) { //find values lower than high
#define lower(TPE, el) \
	do { \
		TPE min, max, step; \
		dimensionCharacteristics(TPE, dimensionBAT, &min, &max, &step, &elementRepeats, &groupRepeats); \
		elementsNum = dimensionElementsNum(min, max, step); \
		element_oid = dimensionFndLowerValuePos(el, min, max, step, includeHigh>0); \
		if(element_oid >= elementsNum) \
			element_oid = elementsNum-1; /* all elements are included*/\
	} while(0)
		
		switch (ATOMtype(type)) {
		case TYPE_bte:
        	lower(bte, *(bte*)high);
	        break;
	    case TYPE_sht:
    	    lower(sht, *(sht*)high);
	        break;
    	case TYPE_int:
        	lower(int, *(int*)high);
	        break;
    	case TYPE_lng:
        	lower(lng, *(lng*)high);
	       break;
#ifdef HAVE_HGE
    	case TYPE_hge:
        	lower(hge, *(hge*)high);
	        break;
#endif
    	case TYPE_flt:
        	lower(flt, *(flt*)high);
	        break;
    	case TYPE_dbl:
        	lower(dbl, *(dbl*)high);
	        break;
    	case TYPE_oid:
        	lower(oid, *(oid*)high);
	        break;
		default:
			return gdk_error_msg(dimension_type, "dimensionBATsubselect", NULL);
    	}

		if(element_oid < 0 && !anti)  { //high lower than min
			if(!(resBAT = BATnew(TYPE_void, TYPE_void, 0, TRANSIENT)))
				return gdk_error_msg(new_bat, "dimensionBATsubselect", NULL);
    		BATseqbase(resBAT, 0);
    		BATseqbase(BATmirror(resBAT), 0);

			*outBAT = resBAT;
			return GDK_SUCCEED;
		}
		qualifyingElementsNum = element_oid+1;
		element_oid=0; //it should add qualifying elementsNum from the beginning
	} else if(ATOMcmp(type, low, nil) && ATOMcmp(type, high, nil)) { //values greater than low and lower than high 
		switch (ATOMtype(type)) {
		case TYPE_bte:
        	lower(bte, *(bte*)high);
			qualifyingElementsNum = element_oid+1;
        	greater(bte, *(bte*)low);
	        break;
	    case TYPE_sht:
    	    lower(sht, *(sht*)high);
			qualifyingElementsNum = element_oid+1;
        	greater(sht, *(sht*)low);
	        break;
    	case TYPE_int:
        	lower(int, *(int*)high);
			qualifyingElementsNum = element_oid+1;
        	greater(int, *(int*)low);
			break;
    	case TYPE_lng:
        	lower(lng, *(lng*)high);
			qualifyingElementsNum = element_oid+1;
        	greater(lng, *(lng*)low);
	       break;
#ifdef HAVE_HGE
    	case TYPE_hge:
        	lower(hge, *(hge*)high);
			qualifyingElementsNum = element_oid+1;
        	greater(hge, *(hge*)low);
	        break;
#endif
    	case TYPE_flt:
        	lower(flt, *(flt*)high);
			qualifyingElementsNum = element_oid+1;
        	greater(flt, *(flt*)low);
	        break;
    	case TYPE_dbl:
        	lower(dbl, *(dbl*)high);
			qualifyingElementsNum = element_oid+1;
        	greater(dbl, *(dbl*)low);
	        break;
    	case TYPE_oid:
        	lower(oid, *(oid*)high);
			qualifyingElementsNum = element_oid+1;
        	greater(oid, *(oid*)low);
	        break;
		default:
			return gdk_error_msg(dimension_type, "dimensionBATsubselect", NULL);
    	}

		if((qualifyingElementsNum == 0 || element_oid >= elementsNum) && !anti) { //high lower than min or low greater than max
			if(!(resBAT = BATnew(TYPE_void, TYPE_void, 0, TRANSIENT)))
				return gdk_error_msg(new_bat, "dimensionBATsubselect", NULL);
    		BATseqbase(resBAT, 0);
    		BATseqbase(BATmirror(resBAT), 0);

			*outBAT = resBAT;
			return GDK_SUCCEED;
		}
		qualifyingElementsNum -= element_oid;
	} else {
		if(!(resBAT = BATnew(TYPE_void, TYPE_void, 0, TRANSIENT)))
			return gdk_error_msg(new_bat, "dimensionBATsubselect", NULL);
    	BATseqbase(resBAT, 0);
    	BATseqbase(BATmirror(resBAT), 0);

		*outBAT = resBAT;
		return GDK_SUCCEED;
	}
	/*Add the qualifying oids to a BAT*/	
	elements_in_result = qualifyingElementsNum*elementRepeats*groupRepeats;
	element_oid *= elementRepeats;

	//create new BAT
	if((b_tmp = BATnew(TYPE_void, TYPE_oid, elements_in_result, TRANSIENT)) == NULL)   \
		return gdk_error_msg(new_bat, "dimensionBATsubselect", NULL);
		
	res = (oid*) Tloc(b_tmp, BUNfirst(b_tmp));
	//add the oids in the result
	for(j=0; j<groupRepeats; j++) {
		for(i=0; i<qualifyingElementsNum*elementRepeats; i++) {
			//fprintf(stderr, "Added oid: %ld\n", element_oid);
			*res = element_oid;
			res++;
			element_oid++;
		}
		//skip the non-qualifying elements
		element_oid += (elementsNum-qualifyingElementsNum)*elementRepeats;
	}
	BATsetcount(b_tmp,elements_in_result);

	if(ATOMcmp(type, low, high) == 0 && anti) { //the oids that do not qualify are those that should be kept
		oid io, jo;
		oid endOid = elementsNum*elementRepeats*groupRepeats;
		oid* res_anti;
		BAT *b_anti = BATnew(TYPE_void, TYPE_oid, endOid-elements_in_result, TRANSIENT);

		if(!b_anti)
			return gdk_error_msg(new_bat, "dimensionBATsubselect", NULL);

		res_anti = (oid*)Tloc(b_anti, BUNfirst(b_anti));
		res = (oid*) Tloc(b_tmp, BUNfirst(b_tmp));

		for(io=0, jo=0; io<endOid && jo<elements_in_result; io++) {
			if(io == res[jo]) { //the oid is in the result -> skip it
				jo++;
			} else {
				fprintf(stderr, "Added anti oid: %ld\n", io);
				*res_anti = io;
				res_anti++;	
			}
		}

		//add any oids that are greater than the last qualifying
		for(; io<endOid; io++) {
			fprintf(stderr, "Added anti oid: %ld\n", io);
			*res_anti=io;
			res_anti++;
		}

		elements_in_result = endOid-elements_in_result;
		b_tmp = b_anti;
		BATsetcount(b_tmp,elements_in_result);
	}
			
	//if the result should be 
	if(candsBAT) {
        oid *current_elements, *cand_elements, *elements;
        oid i, j;

        elements_in_result = (BATcount(b_tmp) > BATcount(candsBAT))?BATcount(candsBAT):BATcount(b_tmp);

        if((resBAT = BATnew(TYPE_void, TYPE_oid, elements_in_result, TRANSIENT)) == NULL)
			return gdk_error_msg(new_bat, "dimensionBATsubsleect", NULL);

        cand_elements = (oid*)Tloc(candsBAT, BUNfirst(candsBAT));
        current_elements = (oid*)Tloc(b_tmp, BUNfirst(b_tmp));
        elements = (oid*)Tloc(resBAT, BUNfirst(resBAT));
        elements_in_result = 0;

        //compare the results in the two BATs and keep only the common ones
        for(i=0,j=0; i<BATcount(b_tmp) && j<BATcount(candsBAT); ) {
        	if(cand_elements[j] == current_elements[i]) {
	        	elements[elements_in_result] = current_elements[i];
//	    	    fprintf(stderr, "Final element: %ld\n", current_elements[i]);
        
    	    	elements_in_result++;
        		i++;
        		j++;
    		} else if(cand_elements[j] < current_elements[i])
	        	j++;
       		else
    	    	i++;
        }
    } else
        resBAT = b_tmp;       
        
    BATsetcount(resBAT,elements_in_result);
    BATseqbase(resBAT,0);
    BATderiveProps(resBAT,FALSE);
        
	*outBAT = resBAT;

    return GDK_SUCCEED;
}

gdk_return BATmbrsubselect(BAT **resBAT, BAT *dimensionBAT, BAT *oidsBAT, BAT *candsBAT) {
	int gdkRetVal = GDK_SUCCEED;
    (void)*candsBAT;

#define mbr(TPE) \
    do { \
        TPE dimMin, dimMax, dimStep; \
        long dimElementRepeats, dimGroupRepeats; \
        TPE resMin, resMax; \
        BATiter oidsIter = bat_iterator(oidsBAT); \
        BUN p, q; \
\
        dimensionCharacteristics(TPE, dimensionBAT, &dimMin, &dimMax, &dimStep, &dimElementRepeats, &dimGroupRepeats); \
        /*loop over the elemets corresponding to the oids and find min and max*/ \
        /*check also the number of groups*/ \
        resMax = resMin = dimensionElement(dimMin, dimMax, dimStep, dimElementRepeats, *(oid*)BUNtail(oidsIter, BUNfirst(oidsBAT))); \
        BATloop(oidsBAT, p, q) { \
            TPE el_cur = dimensionElement(dimMin, dimMax, dimStep, dimElementRepeats, *(oid*)BUNtail(oidsIter, p)); \
            resMax = (el_cur > resMax)?el_cur:resMax; \
            resMin = (el_cur < resMin)?el_cur:resMin; \
        } \
fprintf(stderr, "resMin = %d - resMax = %d\n", (int)resMin, (int)resMax); \
\
    /*find the oids that satisfy the min max values*/ \
    gdkRetVal = dimensionBATsubselect(resBAT, dimensionBAT, candsBAT, &resMin, &resMax, 1, 1, 0); \
    } while(0) 

    switch (ATOMtype(dimensionBAT->ttype)) {
        case TYPE_bte:
            mbr(bte);
            break;
        case TYPE_sht:
            mbr(sht);
            break;
        case TYPE_int:
            mbr(int);
            break;
        case TYPE_flt:
            mbr(flt);
            break;
        case TYPE_dbl:
            mbr(dbl);
            break;
        case TYPE_lng:
            mbr(lng);
            break;
#ifdef HAVE_HGE
        case TYPE_hge:
            mbr(hge);
            break;
#endif
        case TYPE_oid:
#if SIZEOF_OID == SIZEOF_INT
            mbr(int);
#else
            mbr(lng);
#endif
        break;
        default:
			return gdk_error_msg(dimension_type, "mbrBATsubselect", NULL);
    }
	
	return gdkRetVal;
}

BAT *BATdimensionThetasubselect(BAT *b, BAT *s, const void *val, const char *op)
{
    const void *nil;

    BATcheck(b, "BATdimensionThetasubselect", NULL);
    BATcheck(val, "BATdimensionThetasubselect", NULL);
    BATcheck(op, "BATdimensionThetasubselect", NULL);

    nil = ATOMnilptr(b->ttype);
    if (ATOMcmp(b->ttype, val, nil) == 0)
        return newempty("BATdimensionThetasubselect");
    if (op[0] == '=' && ((op[1] == '=' && op[2] == 0) || op[2] == 0)) {
        /* "=" or "==" */
        return BATdimensionSubselect(b, s, val, NULL, 1, 1, 0);
    }
    if (op[0] == '!' && op[1] == '=' && op[2] == 0) {
        /* "!=" (equivalent to "<>") */ 
        return BATdimensionSubselect(b, s, val, NULL, 1, 1, 1);
    }
    if (op[0] == '<') { 
        if (op[1] == 0) {
            /* "<" */
            return BATdimensionSubselect(b, s, nil, val, 0, 0, 0);
        }
        if (op[1] == '=' && op[2] == 0) {
            /* "<=" */
            return BATdimensionSubselect(b, s, nil, val, 0, 1, 0);
        }
        if (op[1] == '>' && op[2] == 0) {
            /* "<>" (equivalent to "!=") */ 
            return BATdimensionSubselect(b, s, val, NULL, 1, 1, 1);
        }
    }
    if (op[0] == '>') { 
        if (op[1] == 0) {
            /* ">" */
            return BATdimensionSubselect(b, s, val, nil, 0, 0, 0);
        }
        if (op[1] == '=' && op[2] == 0) {
            /* ">=" */
            return BATdimensionSubselect(b, s, val, nil, 1, 0, 0);
        }
    }
    GDKerror("BATdimensionThetasubselect: unknown operator.\n");
    return NULL;
}

gdk_return dimensionBATgroup(BAT **groups, BAT **extents, BAT **histo, BAT *dimensionBAT, BAT *g, BAT *e, BAT *h) {
    BAT* resBAT;
    BAT* bordersBAT;
    BUN resSize = 0;


	if(!dimensionBAT || !BAThdense(dimensionBAT))
		return gdk_error_msg(general_error, "dimensionBATgroup", "dimensionBAT not dense");

	if(!groups || !extents)
		return gdk_error_msg(general_error, "dimensionBATgroup", "groups and extents should not be NULL");

	if(g || e || h)
    	return gdk_error_msg(general_error, "dimensionBATgroup", "Unhandled BATs");

#define groups(TPE) \
    do { \
        TPE min, max, step, it; \
        long elementRepeats, groupRepeats; \
        oid pos; \
\
        dimensionCharacteristics(TPE, dimensionBAT, &min, &max, &step, &elementRepeats, &groupRepeats); \
        resSize = dimensionElementsNum(min, max, step); \
fprintf(stderr, "elementRepeats = %ld, groupRepeats = %ld - size = %ld\n", elementRepeats, groupRepeats, resSize); \
        if((bordersBAT = BATnew(TYPE_void, TYPE_oid, resSize, TRANSIENT))) { \
fprintf(stderr, "resSize = %d - eR = %ld - gR = %ld\n", (unsigned int)resSize, elementRepeats, groupRepeats); \
            /*groups have the same repeats with the dimensionBAT*/ \
            if((resBAT = createDimension(oid, 0, resSize-1, 1, elementRepeats, groupRepeats))) { \
	            /*create the groups*/ \
    	        for(it = min, pos=0; it<=max; it+=step, pos+=elementRepeats) { \
        	        BUNappend(bordersBAT, &pos, 1); \
	            } \
\
    	        BATsetcount(bordersBAT, resSize); \
        	    BATseqbase(bordersBAT, 0); \
            	BATderiveProps(bordersBAT, false); \
			} \
        } \
    } while(0)

	switch (ATOMtype(BATttype(dimensionBAT))) {
        case TYPE_bte:
            groups(bte);
            break;
        case TYPE_sht:
            groups(sht);
            break;
        case TYPE_int:
            groups(int);
            break;
        case TYPE_lng:
            groups(lng);
           break;
#ifdef HAVE_HGE
        case TYPE_hge:
            groups(hge);
            break;
#endif
        case TYPE_flt:
            groups(flt);
            break;
        case TYPE_dbl:
            groups(dbl);
            break;
        case TYPE_oid:
            groups(oid);
            break;
        default:
            return gdk_error_msg(dimension_type,"dimensionBATgroup", NULL);
        }

    if(!resBAT || !bordersBAT) {
		if(resBAT)
			BBPunfix(resBAT->batCacheid);
		if(bordersBAT)
			BBPunfix(bordersBAT->batCacheid);
        return GDK_FAIL;
	}
    *groups = resBAT;
    *extents = bordersBAT;
	if(histo) {
		//dimensions do NOT have/create histograms
		BAT *hn = BATnew(TYPE_void, TYPE_wrd, 0, TRANSIENT);
		if (hn == NULL) {
			BBPunfix(resBAT->batCacheid);
			BBPunfix(bordersBAT->batCacheid);
			return GDK_FAIL;		
		}
		BATsetcount(hn, 0);
		BATseqbase(hn,0);
		*histo = hn;

	}
    return GDK_SUCCEED;
}

gdk_return dimensionBATproject(BAT** outBAT, BAT* oidsBAT, BAT* dimensionBAT) {
	BAT *resBAT;
	int tpe = ATOMtype(BATttype(dimensionBAT));//, nilcheck = 1, sortcheck = 1, stringtrick = 0;
	
	assert(BAThdense(oidsBAT));
    assert(BAThdense(dimensionBAT));
    assert(ATOMtype(oidsBAT->ttype) == TYPE_oid);

#define dimensionise(TYPE) \
	do { \
		long flg=0, cnt=0, elementsInGroup=-1, r=0; \
		TYPE dimMin, dimMax, dimStep; \
		TYPE resMin, resMax, resStep; \
		long dimGroupRepeats, dimElementRepeats; \
		long resGroupRepeats, resElementRepeats; \
		bool foundMax =0; \
		TYPE el_cur, el_prev; \
		BATiter oidsIter = bat_iterator(oidsBAT); \
		BUN p, q, j, groupStart; \
		bool validDimension = 1; \
\
		dimensionCharacteristics(TYPE, dimensionBAT, &dimMin, &dimMax, &dimStep, &dimElementRepeats, &dimGroupRepeats); \
		/*min, step, max implied by oids*/ \
		resMin = dimensionElement(dimMin, dimMax, dimStep, dimElementRepeats, *(oid*)BUNtail(oidsIter, BUNfirst(oidsBAT))); \
		resMax = dimensionElement(dimMin, dimMax, dimStep, dimElementRepeats, *(oid*)BUNtail(oidsIter, BUNlast(oidsBAT)-1)); \
\
		/*min and max should appear the same number of times*/ \
		resGroupRepeats = 1; /*there is at least one group*/ \
		cnt = 0; \
		BATloop(oidsBAT, p, q) { \
			el_cur = dimensionElement(dimMin, dimMax, dimStep, dimElementRepeats, *(oid*)BUNtail(oidsIter, p)); \
			cnt++; \
			if(el_cur == resMin) { \
				flg++; \
				if(foundMax) { /*a groups has been completed*/ \
					foundMax=0; \
\
					if(elementsInGroup >= 0 && elementsInGroup != (cnt-1)) { \
			/*			GDKerror("BATdimensionProject: dimension not regular (different number of elements among groups %ld vs %ld)\n", elementsInGroup, cnt); */\
						validDimension = 0; \
					} \
					elementsInGroup = cnt-1; /*the current element does not belong in the group*/\
					resGroupRepeats++; \
					cnt = 1; \
				} \
			} \
			/*max and min can be the same. Thus they should be checked in different ifs and not in an if-else*/\
			if(el_cur == resMax) { \
				flg--; \
				foundMax = 1; \
			} \
		} \
		if(flg>0) { \
/*			GDKerror("BATdimensionProject: dimension not regular (max %ld more times than min)\n", flg); */\
			validDimension = 0; \
		} else if(flg<0) { \
/*			GDKerror("BATdimensionProject: dimension not regular (min %ld more times than max)\n", flg); */\
			validDimension = 0; \
		} \
		/*check the last group*/ \
		if(elementsInGroup >= 0 && elementsInGroup != cnt) { \
/*			GDKerror("BATdimensionProject: dimension not regular (different number of elements among groups %ld vs %ld)\n", elementsInGroup, cnt); */\
			validDimension = 0; \
		} \
		elementsInGroup = cnt; \
\
fprintf(stderr, "dimensionise: groupRepeats = %ld, elementsInGroup = %ld\n", resGroupRepeats, elementsInGroup); \
		/*check that the step is the same between the elements and each element is repeated the same number of times*/ \
		resElementRepeats = -1; \
		foundMax = 0; \
		BATloop(oidsBAT, p, q) { \
			if(dimensionElement(dimMin, dimMax, dimStep, dimElementRepeats, *(oid*)BUNtail(oidsIter, p)) == resMax) \
				foundMax = 1; \
			else if(dimensionElement(dimMin, dimMax, dimStep, dimElementRepeats, *(oid*)BUNtail(oidsIter, p)) == resMin && foundMax) {\
				/*if there is only one element then it will never enter this block, element will be equal to max*/ \
				/*iterate over the elements in the group*/ \
				foundMax =0 ; \
				r=0; \
				groupStart = p-elementsInGroup; \
				el_prev = dimensionElement(dimMin, dimMax, dimStep, dimElementRepeats, *(oid*)BUNtail(oidsIter, groupStart)); \
fprintf(stderr, "dimensionise: Group:[%ld, %ld]\n", groupStart, p-1); \
				for(j=groupStart; j<p; j++) { \
					el_cur = dimensionElement(dimMin, dimMax, dimStep, dimElementRepeats, *(oid*)BUNtail(oidsIter, j)); \
/*fprintf(stderr, "dimensionise: In group element %ld\n", j); */\
					if(el_prev == el_cur)  {\
						r++; \
/*fprintf(stderr, "dimensionise: same element %ld\n", r); */\
					} \
					else { \
						if(resElementRepeats >=0 && resElementRepeats != r) { \
/*							GDKerror("BATdimensionProject: dimension not regular (different number of repetitions %ld vs %ld)\n", resElementRepeats, r); */\
							validDimension = 0; \
						} else if(resElementRepeats <0) { \
							resElementRepeats = r; \
							resStep = el_cur - el_prev; \
						} \
						if(resStep != (el_cur - el_prev)) { \
/*							GDKerror("BATdimensionProject: dimension not regular (not equal steps)\n"); */\
							validDimension = 0; \
						} \
						el_prev=el_cur; \
						r=1; \
					} \
				} \
				/*check the last element in the group*/ \
				if(resElementRepeats >=0 && resElementRepeats != r) { \
/*					GDKerror("BATdimensionProject: dimension not regular (different number of repetitions %ld vs %ld)\n", resElementRepeats, r); */\
					validDimension = 0; \
				} else if(resElementRepeats <0) { \
					/*It should never reach this*/ \
					resElementRepeats = r; \
					resStep = 0; /*if here then there is only one element in the group*/\
				} \
			} \
		} \
		/*check the last group*/ \
		r=0; \
		groupStart = p-elementsInGroup; \
		el_prev = dimensionElement(dimMin, dimMax, dimStep, dimElementRepeats, *(oid*)BUNtail(oidsIter, groupStart)); \
fprintf(stderr, "dimensionise: Group:[%ld, %ld]\n", groupStart, p-1); \
		for(j=groupStart; j<p; j++) { \
			el_cur = dimensionElement(dimMin, dimMax, dimStep, dimElementRepeats, *(oid*)BUNtail(oidsIter, j)); \
/*fprintf(stderr, "dimensionise: In group element %ld\n", j); */\
			if(el_prev == el_cur)  {\
				r++; \
/*fprintf(stderr, "dimensionise: same element %ld\n", r); */\
			} \
			else { \
				if(resElementRepeats >=0 && resElementRepeats != r) { \
/*					GDKerror("BATdimensionProject: dimension not regular (different number of repetitionsi %ld vs %ld)\n", resElementRepeats, r); */\
					validDimension = 0; \
				} else if(resElementRepeats <0) { \
					resElementRepeats = r; \
					resStep = el_cur - el_prev; \
				} \
				if(resStep != (el_cur - el_prev)) { \
/*					GDKerror("BATdimensionProject: dimension not regular (not equal steps)\n"); */\
					validDimension = 0; \
				} \
				el_prev=el_cur; \
				r=1; \
			} \
		} \
		/*check the last element in the group*/ \
		if(resElementRepeats >=0 && resElementRepeats != r) { \
/*			GDKerror("BATdimensionProject: dimension not regular (different number of repetitions %ld vs %ld)\n", resElementRepeats, r); */\
			validDimension = 0; \
		} else if(resElementRepeats <0) { \
			resElementRepeats = r; \
			resStep = 0; /*if here then there is only one element in the group*/\
		} \
\
fprintf(stderr, "dimensionise: elementRepeats=%ld\n", resElementRepeats); \
		/*create the BAT*/ \
		if(validDimension) \
			resBAT = createDimension(TYPE, resMin, resMax, resStep, resElementRepeats, resGroupRepeats); \
		else resBAT = NULL; \
	} while(0) 

	if(BATcount(oidsBAT)) {	
		switch (tpe) {
    	case TYPE_bte:
        	dimensionise(bte);
	        break;
    	case TYPE_sht:
        	dimensionise(sht);
	        break;
    	case TYPE_int:
        	dimensionise(int);
	        break;
    	case TYPE_flt:
        	dimensionise(flt);
	        break;
    	case TYPE_dbl:
        	dimensionise(dbl);
	        break;
    	case TYPE_lng:
        	dimensionise(lng);
	        break;
#ifdef HAVE_HGE
    	case TYPE_hge:
        	dimensionise(hge);
	        break;
#endif
    	case TYPE_oid:
#if SIZEOF_OID == SIZEOF_INT
        	dimensionise(int);
#else
        	dimensionise(lng);
#endif
        break;
    	default:
			return gdk_error_msg(dimension_type, "dimensionBATproject", NULL);
	    }
	} else {
		//no oids. Empty BAT
		if((resBAT = BATnew(TYPE_void, tpe, 0, TRANSIENT)) == NULL) 
			return gdk_error_msg(new_bat, "dimensionBATproject", NULL);

		BATsetcount(resBAT,0);
        BATseqbase(resBAT,0);
        BATderiveProps(resBAT,FALSE);
	}

	*outBAT = resBAT;
	
	return GDK_SUCCEED;
}

BAT* dimensionBATproject_wrap(BAT* oidsBAT, BAT* dimensionBAT) {
	BAT* resBAT = NULL;
	if(dimensionBATproject(&resBAT, oidsBAT, dimensionBAT) != GDK_SUCCEED)
		resBAT = NULL;
	return resBAT;
}


gdk_return BATmbrproject(BAT **outBAT, BAT *b, BAT *oidsToProjectBAT, BAT *subselectBAT) {
	BUN c, l, subC, subL;
	//execute the projection using the subselection result
	BAT *projectBAT = BATproject(subselectBAT, b);

	BATiter oidsToProjectBAT_iter = bat_iterator(oidsToProjectBAT);    
	BATiter sub_iter = bat_iterator(subselectBAT);
	BATiter proj_iter = bat_iterator(projectBAT);

    //set NULL to all values  in s but not is subselectBAT
    BAT *resBAT = BATnew(TYPE_void, b->ttype, BATcount(oidsToProjectBAT), TRANSIENT);
    if(!resBAT)
	    return gdk_error_msg(new_bat, "BATmbrproject", NULL);

	subC = BUNfirst(subselectBAT);
	subL = BUNlast(subselectBAT);
	BATloop(oidsToProjectBAT, c, l) {
		oid oid_pr = *(oid*)BUNtail(oidsToProjectBAT_iter, c);
		//check if the oid_ptr exists in subselectBAT
		if(subC < subL) {
			oid oid_sub = *(oid*)BUNtail(sub_iter, subC);
			while(oid_pr<oid_sub && subC<subL){
					switch (ATOMtype(b->ttype)) {
				        case TYPE_bte:
							BUNappend(resBAT, &bte_nil, 1);	
				            break;
				        case TYPE_sht:
				            BUNappend(resBAT, &sht_nil, 1);	
				            break;
				        case TYPE_int:
				            BUNappend(resBAT, &int_nil, 1);	
				            break;
				        case TYPE_flt:
				            BUNappend(resBAT, &flt_nil, 1);	
				            break;
				        case TYPE_dbl:
				            BUNappend(resBAT, &dbl_nil, 1);	
				            break;
				        case TYPE_lng:
				            BUNappend(resBAT, &lng_nil, 1);	
				            break;
#ifdef HAVE_HGE
				        case TYPE_hge:
				            BUNappend(resBAT, &hge_nil, 1);	
				            break;
#endif
				        case TYPE_oid:
#if SIZEOF_OID == SIZEOF_INT
				            BUNappend(resBAT, &int_nil, 1);	
#else
				            BUNappend(resBAT, &lng_nil, 1);
#endif
					        break;
				        default:
				            return gdk_error_msg(value_type, "BATmbrproject", NULL);
        			}
					c++;
					oid_pr = *(oid*)BUNtail(oidsToProjectBAT_iter, c);
				//BUNappend(resBAT, NULL, 1);
			}
			BUNappend(resBAT, BUNtail(proj_iter, subC), 1);
			subC++;
		} else { //put null values at the end
			switch (ATOMtype(b->ttype)) {
		        case TYPE_bte:
					BUNappend(resBAT, &bte_nil, 1);	
		            break;
		        case TYPE_sht:
		            BUNappend(resBAT, &sht_nil, 1);	
		            break;
		        case TYPE_int:
		            BUNappend(resBAT, &int_nil, 1);	
		            break;
		        case TYPE_flt:
		            BUNappend(resBAT, &flt_nil, 1);	
		            break;
		        case TYPE_dbl:
		            BUNappend(resBAT, &dbl_nil, 1);	
		            break;
		        case TYPE_lng:
		            BUNappend(resBAT, &lng_nil, 1);	
		            break;
#ifdef HAVE_HGE
		        case TYPE_hge:
		            BUNappend(resBAT, &hge_nil, 1);	
		            break;
#endif
		        case TYPE_oid:
#if SIZEOF_OID == SIZEOF_INT
		            BUNappend(resBAT, &int_nil, 1);	
#else
		            BUNappend(resBAT, &lng_nil, 1);
#endif
			        break;
		        default:
				    return gdk_error_msg(value_type, "BATmbrproject", NULL);
    		}
		}
	}
	BATsetcount(resBAT,BATcount(oidsToProjectBAT));
    BATseqbase(resBAT,0);
    BATderiveProps(resBAT,FALSE);

	*outBAT = resBAT;
	return GDK_SUCCEED;
}

BAT* BATnonDimensionProject(BAT* oidsBAT, BAT* dimensionBAT) {
	BAT *resBAT;
	int tpe = ATOMtype(dimensionBAT->ttype);//, nilcheck = 1, sortcheck = 1, stringtrick = 0;
	
	assert(BAThdense(oidsBAT));
    assert(BAThdense(dimensionBAT));
    assert(ATOMtype(oidsBAT->ttype) == TYPE_oid);
	
#define project(TYPE) \
	do { \
    	TYPE *bt;                      \
\
		long repeat1, elementsNum, elementsPerGroup; \
		TYPE min_orig, max_orig, step; \
		TYPE min_new, max_new; /*the step remains the same*/ \
        oid i, previous_oid, jj; \
		TYPE *el_in; \
		long repeat1_new, repeat2_new, j; \
		long currentRepeat; \
		TYPE currentEl; \
		long startGroup, endGroup, currentGroup; \
		BUN p, q; \
		oid currentOid; \
		BATiter oidsBAT_iter;\
		bool first = true; \
\
		/*find the min, max step of the input BAT*/ \
		el_in = (TYPE*)Tloc(dimensionBAT, BUNfirst(dimensionBAT)); \
        min_orig = el_in[0]; \
        step = el_in[BATcount(dimensionBAT)-1]; \
\
        repeat1 = 1; \
        for(i=1; i<BATcount(dimensionBAT); i++) { \
            if(min_orig == el_in[i]) \
                repeat1++; \
            else { \
                max_orig = el_in[i]; \
                break; \
            } \
        } \
fprintf(stderr, "BATnondimensionProject: original repeat1 = %ld\n", repeat1); \
\
        elementsNum = floor((max_orig-min_orig)/step) + 1; \
		elementsPerGroup = repeat1*elementsNum; \
fprintf(stderr, "BATnondimensionProject: original elements per group = %ld\n", elementsPerGroup); \
\
		oidsBAT_iter = bat_iterator(oidsBAT); \
		BATloop(oidsBAT, p, q) { \
			currentOid = *((oid*) BUNtail(oidsBAT_iter, p)); \
			if(first) { \
fprintf(stderr, "BATnondimensionProject: original elements per group = %u\n", (unsigned int)currentOid); \
				startGroup = floor(currentOid/elementsPerGroup); \
fprintf(stderr, "start group = %ld\n", startGroup); \
\
				/*check the elements per group*/ \
				currentGroup = floor(currentOid/elementsPerGroup); \
				min_new = max_new = currentEl = (currentOid%(repeat1*elementsNum))/repeat1; \
				repeat1_new = currentRepeat = 1; \
fprintf(stderr, "BATnondimensionProject: (first) group %ld -> original[%u]\n", currentGroup, (unsigned int)currentOid); \
				first = false; \
			} else { \
				TYPE el = (currentOid%(repeat1*elementsNum))/repeat1; \
				long grp = floor(currentOid/elementsPerGroup); \
\
				if(currentGroup == grp) { /*still in the same group*/\
					/*there should not be any missing oids inside the same element of the same group*/ \
					if(currentEl == el) { /*same group, same element*/\
						for(jj=previous_oid+1; jj<=currentOid; jj++) { \
fprintf(stderr, "BATnondimensionProject: (same-same) group %ld -> original[%u]\n", grp, (unsigned int)jj); \
							currentRepeat++; \
						} \
					} else {/*same group but other element*/ \
fprintf(stderr, "BATnondimensionProject: (same-new) group %ld -> original[%u]\n", grp, (unsigned int)currentOid); \
						if(currentRepeat > repeat1_new) \
							repeat1_new = currentRepeat; \
						currentEl = el; \
						currentRepeat = 1; \
						if(min_new > el) \
							min_new = el; \
						if(max_new < el) \
							max_new = el; \
					} \
				} else { /*new group*/ \
fprintf(stderr, "BATnondimensionProject: (new) group %ld -> original[%u]\n", grp, (unsigned int)currentOid); \
					if(currentRepeat > repeat1_new) \
						repeat1_new = currentRepeat; \
					currentEl = el; \
					currentRepeat = 1; \
					currentGroup = grp; \
					if(min_new > el) \
						min_new = el; \
					if(max_new < el) \
						max_new = el; \
				} \
			} \
			previous_oid = currentOid; \
			if(currentRepeat>repeat1_new) \
				repeat1_new = currentRepeat; \
fprintf(stderr, "BATnondimensionProject: new repeat1 = %ld\n", repeat1_new); \
		} \
\
		endGroup = floor(currentOid/elementsPerGroup); \
fprintf(stderr, "end group = %ld\n", endGroup); \
		repeat2_new = endGroup - startGroup +1; \
fprintf(stderr, "BATnondimensionProject: new repeat2 = %ld\n", repeat2_new); \
\
		/*create the resBAT*/ \
		if((resBAT = BATnew(TYPE_void, tpe, repeat1_new+repeat2_new+1, TRANSIENT)) == NULL) \
			return NULL; \
\
		bt = (TYPE*)Tloc(resBAT, BUNfirst(resBAT)); \
		for(j=0; j<repeat1_new; j++) { \
			*bt = min_new; \
			bt++; \
		} \
		for(j=0; j<repeat2_new; j++) { \
			*bt = max_new; \
			bt++; \
		} \
		*bt = step; \
\
		BATsetcount(resBAT,repeat1_new+repeat2_new+1); \
        BATseqbase(resBAT,0); \
        BATderiveProps(resBAT,FALSE); \
\
	} while(0)

	if(BATcount(oidsBAT)) {	
		switch (tpe) {
    	case TYPE_bte:
        	project(bte);
	        break;
    	case TYPE_sht:
        	project(sht);
	        break;
    	case TYPE_int:
        	project(int);
	        break;
    	case TYPE_flt:
        	project(flt);
	        break;
    	case TYPE_dbl:
        	project(dbl);
	        break;
    	case TYPE_lng:
        	project(lng);
	        break;
#ifdef HAVE_HGE
    	case TYPE_hge:
        	project(hge);
	        break;
#endif
    	case TYPE_oid:
#if SIZEOF_OID == SIZEOF_INT
        	project(int);
#else
        	project(lng);
#endif
        break;
    	default:
			fprintf(stderr, "BATnonDimensionProject: type not handled\n");
			return NULL;
	    }
	} else {
		//no oids. Empty BAT
		if((resBAT = BATnew(TYPE_void, tpe, 0, TRANSIENT)) == NULL) 
			return NULL;

		BATsetcount(resBAT,0);
        BATseqbase(resBAT,0);
        BATderiveProps(resBAT,FALSE);
	}

	return resBAT;
}

gdk_return dimensionBATsubjoin(BAT **outBATl, BAT **outBATr, BAT *dimensionBATl, BAT *dimensionBATr, BAT *sl, BAT *sr, int nil_matches, BUN estimate) {
	BAT *resBATl, *resBATr;
	oid *qualifyingL, *qualifyingR;
	BUN resSize;

	if(sr || sl)
	    return gdk_error_msg(general_error, "dimensionBATsubjoin", "Unhandled BATs");

(void)nil_matches;
(void)estimate;

#define join(TPE) \
do { \
	TPE minL, maxL, stepL, minR, maxR, stepR, min, max, step, it; \
	unsigned long elRepeatsL, grRepeatsL, elRepeatsR, grRepeatsR; \
	BUN elsNumR, elsPerGroupR, totalElsNumR, elsNumL, elsPerGroupL, totalElsNumL; \
\
	dimensionCharacteristics(TPE, dimensionBATl, &minL, &maxL, &stepL, &elRepeatsL, &grRepeatsL); \
	elsNumL = dimensionElementsNum(minL, maxL, stepL); \
	elsPerGroupL = elsNumL*elRepeatsL; \
	totalElsNumL = elsPerGroupL*grRepeatsL; \
	dimensionCharacteristics(TPE, dimensionBATr, &minR, &maxR, &stepR, &elRepeatsR, &grRepeatsR); \
	elsNumR = dimensionElementsNum(minR, maxR, stepR); \
	elsPerGroupR = elsNumR*elRepeatsR; \
	totalElsNumR = elsPerGroupR*grRepeatsR; \
	/*use for the iteration the smallest dimension*/ \
	if( elsNumR > elsNumL ) { \
		min = minL; \
		max = maxL; \
		step = stepL; \
		resSize = elRepeatsL*grRepeatsL*elRepeatsR*grRepeatsR*elsNumL; \
	} else {\
		min = minR; \
		max = maxR; \
		step = stepR; \
		resSize = elRepeatsL*grRepeatsL*elRepeatsR*grRepeatsR*elsNumR; \
	} \
\
fprintf(stderr, "resSize = %lu\n", resSize); \
\
	resBATl = BATnew(TYPE_void, TYPE_oid, resSize, TRANSIENT); \
	qualifyingL = (oid*)Tloc(resBATl, BUNfirst(resBATl)); \
	resBATr = BATnew(TYPE_void, TYPE_oid, resSize, TRANSIENT); \
	qualifyingR = (oid*)Tloc(resBATr, BUNfirst(resBATr)); \
	resSize = 0; \
\
	/*for each index*/ \
	for(it=min; it<=max; it+=step) { \
		/*find the position of the index in each dimension*/ \
		BUN lPos = dimensionFndValuePos(it, minL, stepL)*elRepeatsL; \
		BUN rPos = dimensionFndValuePos(it, minR, stepR)*elRepeatsR; \
fprintf(stderr, "Checking %d : lPos = %lu, rPos = %lu\n", (int)it, lPos, rPos); \
		if(lPos != BUN_NONE && rPos != BUN_NONE) { \
			/*match all occurrences in left dimension to all occurences in right dimension */ \
			BUN i, j; \
			for(i=lPos; i<totalElsNumL; i+=elsPerGroupL) { \
				for(j=rPos; j<totalElsNumR; j+=elsPerGroupR) { \
					unsigned long ii, jj; \
					for(ii=0; ii<elRepeatsL; ii++) { \
						for(jj=0; jj<elRepeatsR; jj++) { \
							*qualifyingL++ = i+ii; \
							*qualifyingR++ = j+jj; \
							resSize++; \
fprintf(stderr, "%lu: Added (%lu, %lu)\n", resSize, (i+ii), (j+jj)); \
						} \
					} \
				}\
			} \
		}\
	} \
} while(0)

	switch (ATOMtype(BATttype(dimensionBATl))) {
        case TYPE_bte:
            join(bte);
            break;
        case TYPE_sht:
            join(sht);
            break;
        case TYPE_int:
            join(int);
            break;
        case TYPE_flt:
            join(flt);
            break;
        case TYPE_dbl:
            join(dbl);
            break;
        case TYPE_lng:
            join(lng);
            break;
#ifdef HAVE_HGE
        case TYPE_hge:
            join(hge);
            break;
#endif
        case TYPE_oid:
#if SIZEOF_OID == SIZEOF_INT
            join(int);
#else
            join(lng);
#endif
    	    break;
        default:
            return gdk_error_msg(dimension_type, "dimensionBATsubjoin", NULL);
	}

	BATsetcount(resBATl,resSize);
    BATseqbase(resBATl,0);
    BATderiveProps(resBATl,FALSE);
	BATsetcount(resBATr,resSize);
    BATseqbase(resBATr,0);
    BATderiveProps(resBATr,FALSE);

	*outBATl = resBATl;
	*outBATr = resBATr;

	return GDK_SUCCEED;
}

/*the groupsBAT has been created using a dimensionBAT and is thus a dimension BAT*/
gdk_return dimensionBATgroupavg(BAT **outBAT, BAT **cntsp, BAT *valuesBAT, BAT *groupsBAT, BAT *extentsBAT, BAT *s, int tp, int skip_nils, int abort_on_error) {
	dbl *restrict dbls;
	BAT *resBAT;
	oid minGroupId, maxGroupId;
	BUN groupsNum;
	const char* err;

	/*used for candidates (I do not handle this case at the moment*/
	BUN start,end, cnt;
	const oid *cand, *candend;

	if(cntsp)
		return gdk_error_msg(general_error, "dimensionBATgroupavg", "cntsp is not NULL");

	/* initialise the minimum and maximum group is and the number of groups
 	 * and the variables for the candidates selection */
	if ((err = BATgroupaggrinit(valuesBAT, groupsBAT, extentsBAT, s, &minGroupId, &maxGroupId, &groupsNum,
					&start, &end, &cnt, &cand, &candend)) != NULL) {
        return gdk_error_msg(general_error, "dimensionBATgroupavg", err);
    }

	resBAT = BATnew(TYPE_void, TYPE_dbl, groupsNum, TRANSIENT);
	if (resBAT == NULL)
		return GDK_FAIL;
	dbls = (dbl *) Tloc(resBAT, BUNfirst(resBAT));

(void)skip_nils;
(void)abort_on_error;
(void)start;
(void)end;
(void)cnt;
(void)*cand;
(void)*candend;

(void) tp;      /* compatibility (with other BATgroup*
                 * functions) argument */

#define dimensionAggrAvg(TPE) \
    do {\
        oid minD, maxD, stepD; \
        long elR, grpR, el, grp; \
        BUN elsNum, aggrGrp; \
        const TPE *restrict vals = (const TPE *) Tloc(valuesBAT, BUNfirst(valuesBAT)); \
        double elsPerAggrGrp =0 ; \
\
        dimensionCharacteristics(oid, groupsBAT, &minD, &maxD, &stepD, &elR, &grpR); \
        elsNum = dimensionElementsNum(minD, maxD, stepD); \
        elsPerAggrGrp = elR*grpR; \
\
        for(aggrGrp=0; aggrGrp<elsNum; aggrGrp++) { \
            oid start = aggrGrp*elR;\
fprintf(stderr, "%u: %u\n", (unsigned int)aggrGrp, (unsigned int)start); \
            dbls[aggrGrp] = 0; /*initialise to 0*/\
            for(grp=0; grp<grpR; grp++) { \
                for(el=0; el<elR; el++) { \
                    dbls[aggrGrp]+=vals[start+el]; \
fprintf(stderr, "avg: group %u - (%u,%f) => %f\n", (unsigned int)aggrGrp, (unsigned int)(start+el), (double)vals[start+el], dbls[aggrGrp]); \
                }\
                start+=elsNum*elR; \
            } \
            dbls[aggrGrp]/=elsPerAggrGrp; \
        } \
    } while(0)

     switch(ATOMtype(BATttype(valuesBAT))) {
        case TYPE_bte:
            dimensionAggrAvg(bte);
			break;
        case TYPE_sht:
            dimensionAggrAvg(sht);
			break;
        case TYPE_int:
            dimensionAggrAvg(int);
			break;
        case TYPE_flt:
            dimensionAggrAvg(flt);
			break;
        case TYPE_dbl:
            dimensionAggrAvg(dbl);
			break;
        case TYPE_lng:
            dimensionAggrAvg(lng);
			break;
#ifdef HAVE_HGE
        case TYPE_hge:
            dimensionAggrAvg(hge);
			break;
#endif
        case TYPE_oid:
#if SIZEOF_OID == SIZEOF_INT
            dimensionAggrAvg(int);
#else
            dimensionAggrAvg(lng);
#endif
        	break;
        default:
            return gdk_error_msg(value_type, "dimensionBATgroupavg", NULL);
    }

	BATsetcount(resBAT, groupsNum);
	BATseqbase(resBAT, 0);
	BATderiveProps(resBAT, FALSE);
	*outBAT = resBAT;

    return GDK_SUCCEED;
}


/*takes a set of dimensions of any type and returns the corresponding dimensions in indices format */
gdk_cells* arrayToCells(gdk_array *array) {
    gdk_cells *resDims = cells_new();
    int i=0;

    for(i=0; i<array->dimsNum; i++) {
        gdk_dimension *dim = createDimension_oid(i, array->dimSizes[i], 0, array->dimSizes[i]-1, 1);
        resDims = cells_add_dimension(resDims, dim);
    }
    return resDims;
}

gdk_array *cellsToArray(gdk_cells *cells) {
	dim_node *n;
	int i=0;
	gdk_array *array = GDKmalloc(sizeof(gdk_array));
	if(!array)
		return NULL;
	//count the dimensions
	array->dimsNum = 0;
	for(n=cells->h ; n ; n=n->next)
		array->dimsNum++;
	//get the size of each dimension (the initial size)
	for(n=cells->h , i=0; n ; n= n->next, i++)
		array->dimSizes[i] = n->data->initialElementsNum;
	return array;
}
#endif
