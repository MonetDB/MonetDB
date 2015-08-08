#include "monetdb_config.h"
#include "mal_exception.h"
#include "arrays.h"

#include <gdk_arrays.h>

/*UPDATED*/
static unsigned int jumpSize(gdk_array *array, unsigned int dimNum) {
	unsigned short i=0;
	unsigned int skip = 1;
	for(i=0; i<dimNum; i++)
		skip *= array->dims[i]->elsNum;
	return skip;
}

static int arrayCellsNum(gdk_array *array) {
	return jumpSize(array, array->dimsNum);
}

#if 0
/*UPDATED*/
static BUN oidToIdx(oid oidVal, int dimNum, int currentDimNum, BUN skipCells, gdk_array *array) {
	BUN oid = 0;

	while(currentDimNum < dimNum) {
		skipCells*=array->dims[currentDimNum]->elsNum;
		currentDimNum++;
	}

	if(currentDimNum == array->dimsNum-1)
		oid = oidVal;
	else
		oid = oidToIdx(oidVal, dimNum, currentDimNum+1, skipCells*array->dims[currentDimNum]->elsNum, array);

	if(currentDimNum == dimNum) //in the last one we do not compute module
		return oid/skipCells;
	return oid%skipCells;
}

/*UPDATED*/
static BUN* oidToIdx_bulk(oid* oidVals, int valsNum, int dimNum, int currentDimNum, BUN skipCells, gdk_array *array) {
	BUN *oids = GDKmalloc(valsNum*sizeof(BUN));
	int i;

	if(!oids) {
		GDKerror("Problem allocating space");
		return NULL;
	}

	while(currentDimNum < dimNum) {
		skipCells*=array->dims[currentDimNum]->elsNum;
		currentDimNum++;
	}

	if(currentDimNum == array->dimsNum-1) { //last dimension, do not go any deeper
		if(currentDimNum == dimNum) {//in the dimension of interest we do not compute the module
			for(i=0; i<valsNum; i++)
				oids[i] = oidVals[i]/skipCells;
		} else {
			for(i=0; i<valsNum; i++)
				oids[i] = oidVals[i]%skipCells;
		}	
	}
	else {
		BUN *oidRes = oidToIdx_bulk(oidVals, valsNum, dimNum, currentDimNum+1, skipCells*array->dims[currentDimNum]->elsNum, array);

		if(currentDimNum == dimNum) {//in the dimension of interest we do not compute the module
			for(i=0; i<valsNum; i++)
				oids[i] = oidRes[i]/skipCells;
		} else {
			for(i=0; i<valsNum; i++)
				oids[i] = oidRes[i]%skipCells;
		}	

		GDKfree(oidRes);
	}

	return oids;
}

#endif

static gdk_dimension* updateDimCandRange(gdk_dimension *dimCand, unsigned int min, unsigned int max) {
	dimCand->min = min > dimCand->min ? min : dimCand->min;
	dimCand->max = max < dimCand->max ? max : dimCand->max;
	dimCand->elsNum = min>max ? 0 : max-min+1; //if 0 then the dimension is empty
	return dimCand;
}

static gdk_dimension* updateDimCandIdxs(gdk_dimension *dimCand, unsigned int min, unsigned int max) {
	unsigned int elsNum = 0; 
	unsigned int *idxs = GDKmalloc(sizeof(unsigned int)*dimCand->elsNum); //at most (quailfyingIdx might alresy be out of the idxs)
	unsigned int i;

	for(i=0 ; i<dimCand->elsNum; i++) {
		if(dimCand->idxs[i] >= min && dimCand->idxs[i] <= max) {
			idxs[elsNum] = dimCand->idxs[i];
			elsNum++;
		}
	}
				
	//release the previous idxs
	GDKfree(dimCand->idxs);
	//store the new idxs
	dimCand->elsNum = elsNum; //if 0 then the dimension is empty
	dimCand->idxs = idxs;
	//upadte the min max if needed
	dimCand->min = min > dimCand->min ? min : dimCand->min;
	dimCand->max = max < dimCand->max ? max : dimCand->max;

	return dimCand;
}

str ALGdimensionSubselect2(ptr *dimsRes, const ptr *dim, const ptr* dims, const ptr *dimCands, 
							const void *low, const void *high, const bit *li, const bit *hi, const bit *anti) {
	gdk_array *array = (gdk_array*)*dims;
	gdk_analytic_dimension *dimension = (gdk_analytic_dimension*)*dim;
	gdk_dimension *dimCand = NULL;
	gdk_array *dimCands_in = NULL;

	oid qualifyingIdx_min, qualifyingIdx_max;
	int type;
	const void* nil;

	if(dimCands) 
		dimCands_in = (gdk_array*)*dimCands;
	else //all dimensions are candidates
		dimCands_in = arrayCopy(array);
	dimCand = dimCands_in->dims[dimension->dimNum];

	*dimsRes = dimCands_in; //the same pointers will be used but the dimension over which the selection will be performed might change 
	if(!dimCands_in) { //empty results
		arrayDelete(array); //I am not gona use it later I can delete it
		return MAL_SUCCEED;
	}

    type = dimension->type;
    nil = ATOMnilptr(type);
    type = ATOMbasetype(type);

    if(ATOMcmp(type, low, high) == 0) { //point selection   
		//find the idx of the value
		oid qualifyingIdx = equalIdx(dimension, low); 
		if(*anti) {
			//two ranges qualify for the result [0, quaifyingIdx-1] and [qualifyingIdx+1, max]
			unsigned int elsNum =0;
			unsigned int i=0;
			unsigned int *idxs = NULL;

			if(dimCand->idxs) { 
				idxs = GDKmalloc(sizeof(unsigned int)*dimCand->elsNum); //the qualifyingIdx might be already absent from the idxs
				for(i=0; i<dimCand->elsNum; i++) {
					if(dimCand->idxs[i] != qualifyingIdx) {
						idxs[elsNum] = i;
						elsNum++;
					}
				} 

				//release the previous idxs
				GDKfree(dimCand->idxs);
			} else {
				idxs = GDKmalloc(sizeof(unsigned int)*dimCand->elsNum-1); 
				for(i=dimCand->min ; i<=dimCand->max; i+=dimCand->step) { //we shoud be carefull to consider the limits because they might not be the default ones
					if(i != qualifyingIdx) {
						idxs[elsNum] = i;
						elsNum++;
					}
				}
			}

			//store the new idxs
			dimCand->elsNum = elsNum;
			dimCand->idxs = idxs;
			//upadte the min max if needed
			dimCand->min = qualifyingIdx > dimCand->min ? qualifyingIdx : dimCand->min;
			dimCand->max = qualifyingIdx < dimCand->max ? qualifyingIdx : dimCand->max;

			if(elsNum == 0) { //empty result
				arrayDelete(dimCands_in);
				arrayDelete(array); //I am not gonna use it again I can delete it
				*dimsRes = NULL;
			}

			return MAL_SUCCEED;

		} else { //a single element qualifies for the result
			qualifyingIdx_min = qualifyingIdx; 
			qualifyingIdx_max = qualifyingIdx; 	
		}
	} else if(ATOMcmp(type, low, nil) == 0) { //no lower bound
		qualifyingIdx_min = 0; 
		qualifyingIdx_max = greaterIdx(dimension, high, *hi); 
	} else if(ATOMcmp(type, high, nil) == 0) { //no upper bound
		qualifyingIdx_min = lowerIdx(dimension, low, *li); 
		qualifyingIdx_max = dimension->elsNum-1;
	} else if(ATOMcmp(type, low, nil) != 0 && ATOMcmp(type, high, nil) != 0) {
		qualifyingIdx_min = lowerIdx(dimension, low, *li); 
		qualifyingIdx_max = greaterIdx(dimension, high, *hi); 
	} else {
		arrayDelete(dimCands_in);
		*dimsRes = NULL;
		arrayDelete(array);
		return MAL_SUCCEED;
	}

	//Now that the new range has been found update the results of the dimCand
	if(dimCand->idxs) {
		dimCand = updateDimCandIdxs(dimCand, qualifyingIdx_min, qualifyingIdx_max);
	} else {
		dimCand = updateDimCandRange(dimCand, qualifyingIdx_min, qualifyingIdx_max);
	}	 

	if(dimCand->elsNum == 0) { //empty result
		arrayDelete(dimCands_in);
		arrayDelete(array);
		*dimsRes = NULL;
	}	

	return MAL_SUCCEED;
}

str ALGdimensionSubselect1(ptr *dimsRes, const ptr *dim, const ptr* dims, 
							const void *low, const void *high, const bit *li, const bit *hi, const bit *anti) {
	return ALGdimensionSubselect2(dimsRes, dim, dims, NULL, low, high, li, hi, anti);
}

str ALGdimensionThetasubselect2(ptr *dimsRes, const ptr *dim, const ptr* dims, const ptr *dimCands, const void *val, const char **opp) {
	bit li = 0;
	bit hi = 0;
	bit anti = 0;
	const char *op = *opp;
	gdk_analytic_dimension *dimension = *dim;
	const void *nil = ATOMnilptr(dimension->type);

	if (op[0] == '=' && ((op[1] == '=' && op[2] == 0) || op[2] == 0)) {
        /* "=" or "==" */
		li = hi = 1;
		anti = 0;
        return ALGdimensionSubselect2(dimsRes, dim, dims, dimCands, val, nil, &li, &hi, &anti);
    }
    if (op[0] == '!' && op[1] == '=' && op[2] == 0) {
        /* "!=" (equivalent to "<>") */ 
		li = hi = anti = 1;
        return ALGdimensionSubselect2(dimsRes, dim, dims, dimCands, val, nil, &li, &hi, &anti);
    }
    if (op[0] == '<') { 
        if (op[1] == 0) {
            /* "<" */
			li = hi = anti = 0;
            return ALGdimensionSubselect2(dimsRes, dim, dims, dimCands, nil, val, &li, &hi, &anti);
        }
        if (op[1] == '=' && op[2] == 0) {
            /* "<=" */
			li = anti = 0;
			hi = 1;
            return ALGdimensionSubselect2(dimsRes, dim, dims, dimCands, nil, val, &li, &hi, &anti);
        }
        if (op[1] == '>' && op[2] == 0) {
            /* "<>" (equivalent to "!=") */ 
			li = hi = anti = 1;
            return ALGdimensionSubselect2(dimsRes, dim, dims, dimCands, val, nil, &li, &hi, &anti);
        }
    }
    if (op[0] == '>') { 
        if (op[1] == 0) {
            /* ">" */
			li = hi = anti = 0;
            return ALGdimensionSubselect2(dimsRes, dim, dims, dimCands, val, nil, &li, &hi, &anti);
        }
        if (op[1] == '=' && op[2] == 0) {
            /* ">=" */
			li = 1;
			hi = anti = 0;
            return ALGdimensionSubselect2(dimsRes, dim, dims, dimCands, val, nil, &li, &hi, &anti);
        }
    }

    throw(MAL, "algebra.dimensionThetasubselect", "BATdimensionThetasubselect: unknown operator.\n");
}

str ALGdimensionThetasubselect1(ptr *dimsRes, const ptr *dim, const ptr* dims, const void *val, const char **op) {
	return ALGdimensionThetasubselect2(dimsRes, dim, dims, NULL, val, op);
}

str ALGdimensionLeftfetchjoin1(bat *result, const ptr *dimCands, const ptr *dim, const ptr *dims) {
	gdk_array *array = (gdk_array*)*dims;
	gdk_analytic_dimension *dimension = (gdk_analytic_dimension*)*dim;
	gdk_array *dimCands_in = (gdk_array*)*dimCands;
	gdk_dimension *dimCand = dimCands_in->dims[dimension->dimNum];

	BAT *resBAT = NULL;
	BUN resSize = 0;

	if(!dimCands_in) { //empty
		if(!(resBAT = BATnew(TYPE_void, TYPE_void, 0, TRANSIENT)))
			throw(MAL, "algebra.leftfetchjoin", "Problem allocating new BAT");
	} else {
		unsigned short i=0;

		unsigned int elsR = 1;
		unsigned int grpR = 1;

		for(i=0; i<dimension->dimNum; i++)
			elsR *= dimCands_in->dims[i]->elsNum;
		for(i=dimension->dimNum+1 ; i<array->dimsNum; i++)
			grpR *= dimCands_in->dims[i]->elsNum;

		resSize = dimCand->elsNum*elsR*grpR;
	
#define computeValues(TPE) \
do { \
	TPE min = *(TPE*)dimension->min; \
	TPE step = *(TPE*)dimension->step; \
	TPE *vals; \
\
	unsigned int i=0, elsRi=0, grpRi = 0; \
\
	if(!(resBAT = BATnew(TYPE_void, TYPE_##TPE, resSize, TRANSIENT))) \
    	    throw(MAL, "algebra.leftfetchjoin", "Problem allocating new BAT"); \
	vals = (TPE*)Tloc(resBAT, BUNfirst(resBAT)); \
\
	if(dimCand->idxs) { \
		/* iterate over the idxs and use each one as many times as defined by elsR */ \
		/* repeat the procedure as many times as defined my grpR */ \
		for(grpRi=0; grpRi<grpR; grpRi++)\
			for(i=0; i<dimCand->elsNum; i++) \
				for(elsRi=0; elsRi<elsR; elsRi++) \
					*vals++ = min + dimCand->idxs[i]*step; \
	} else { \
		/* get the elements in the range and use each one as many times as defined by elsR */ \
		/* repeat the procedure as many times as defined my grpR */ \
		for(grpRi=0; grpRi<grpR; grpRi++)\
			for(i=dimCand->min; i<=dimCand->max; i+=dimCand->step)\
				for(elsRi=0; elsRi<elsR; elsRi++)\
					*vals++ = min + i*step; \
	}\
} while(0)


		/*for each oid in the candsBAT find the real value of the dimension */
 		switch(dimension->type) { \
    	    case TYPE_bte: \
				computeValues(bte);
    	        break;
    	    case TYPE_sht: 
   				computeValues(sht);
    	    	break;
    	    case TYPE_int:
   				computeValues(int);
    	    	break;
    	    case TYPE_wrd:
   				computeValues(wrd);
    	    	break;
    	    case TYPE_oid:
   				computeValues(oid);
    	    	break;
    	    case TYPE_lng:
   				computeValues(lng);
    		    break;
    	    case TYPE_dbl:
   				computeValues(dbl);
		        break;
    	    case TYPE_flt:
   				computeValues(flt);
   		    	break;
			default:
				throw(MAL, "algebra.dimensionLeftfetchjoin", "Dimension type not supported\n");
	    }
	}

	BATsetcount(resBAT, resSize);
	BATseqbase(resBAT, 0);
    BATderiveProps(resBAT, FALSE);    

	*result = resBAT->batCacheid;
    BBPkeepref(*result);

    return MAL_SUCCEED;

}

str ALGnonDimensionLeftfetchjoin1(bat* result, const ptr *dimCands, const bat *vals, const ptr *dims) {
	/* projecting a non-dimensional column does not differ from projecting any relational column */
	BAT *candsBAT, *valsBAT, *resBAT= NULL;
	gdk_array *dimCands_in = (gdk_array*)*dimCands;
	gdk_array *array = (gdk_array*)*dims;

    if ((valsBAT = BATdescriptor(*vals)) == NULL) {
        throw(MAL, "algebra.leftfetchjoin", RUNTIME_OBJECT_MISSING);
    }

	//create the oids using the candidates
	candsBAT = projectCells(dimCands_in, array);	

    resBAT = BATproject(candsBAT, valsBAT);

    BBPunfix(candsBAT->batCacheid);
	BBPunfix(valsBAT->batCacheid);

    if (resBAT == NULL)
        throw(MAL, "algebra.leftfetchjoin", GDK_EXCEPTION);
    *result = resBAT->batCacheid;
    BBPkeepref(*result);

	return MAL_SUCCEED;
}

str ALGnonDimensionLeftfetchjoin2(bat* result, ptr *dimsRes, const bat *tids, const bat *vals, const ptr *dims) {
	BAT *materialisedBAT = NULL;
	BAT *nonDimensionalBAT = NULL;
	BUN totalCellsNum, neededCellsNum;

	gdk_array *array = (gdk_array*)*dims;

	if ((nonDimensionalBAT = BATdescriptor(*vals)) == NULL) {
        throw(MAL, "algebra.leftfecthjoin", RUNTIME_OBJECT_MISSING);
    }
	(void)*tids; //ignore the tids

	totalCellsNum = arrayCellsNum(array);
	neededCellsNum = totalCellsNum - BATcount(nonDimensionalBAT);
	
	/*TODO: fix this so that I can have the real default value of the column */
	materialisedBAT = materialise_nonDimensional_column(ATOMtype(BATttype(nonDimensionalBAT)), neededCellsNum, NULL);
	if(!materialisedBAT) {
		BBPunfix(nonDimensionalBAT->batCacheid);
		throw(MAL, "algebra.leftfetchjoin", "Problem materialising non-dimensional column");
	}


	/*append the missing values to the BAT */
	BATappend(nonDimensionalBAT, materialisedBAT, TRUE);
	BATsetcount(nonDimensionalBAT, totalCellsNum);
	
	BBPunfix(materialisedBAT->batCacheid);
	BBPkeepref(*result = nonDimensionalBAT->batCacheid);

	//sent this to the output so that we do not lose it afterwards     
	*dimsRes = *dims;
   
	return MAL_SUCCEED;
}

#if 0
str ALGmbrproject(bat *result, const bat *bid, const bat *sid, const bat* rid) {
    BAT *b, *s, *r, *bn;

    if ((b = BATdescriptor(*bid)) == NULL) {
        throw(MAL, "algebra.mbrproject", RUNTIME_OBJECT_MISSING);
    }
    if ((s = BATdescriptor(*sid)) == NULL) {
        BBPunfix(b->batCacheid);
        throw(MAL, "algebra.mbrproject", RUNTIME_OBJECT_MISSING);
    }
    if ((r = BATdescriptor(*rid)) == NULL) {
        BBPunfix(b->batCacheid);
        BBPunfix(s->batCacheid);
        throw(MAL, "algebra.mbrproject", RUNTIME_OBJECT_MISSING);
    }
    if(BATmbrproject(&bn, b, s, r) != GDK_SUCCEED)
        bn = NULL;
    BBPunfix(b->batCacheid);
    BBPunfix(s->batCacheid);
    BBPunfix(r->batCacheid);

    if (bn == NULL)
        throw(MAL, "algebra.mbrproject", GDK_EXCEPTION);
    if (!(bn->batDirty&2)) BATsetaccess(bn, BAT_READ);
    *result = bn->batCacheid;
    BBPkeepref(bn->batCacheid);
    return MAL_SUCCEED;

}

str ALGmbrsubselect(bat *result, const ptr *dims, const ptr* dim, const bat *sid, const bat *cid) {
    BAT *b=NULL, *s = NULL, *c = NULL, *bn;

	(void)*dims;
	(void)*dim;

//    if ((b = BATdescriptor(*bid)) == NULL) {
//      throw(MAL, "algebra.mbrsubselect", RUNTIME_OBJECT_MISSING);
//    }
    if (sid && *sid != bat_nil && (s = BATdescriptor(*sid)) == NULL) {
        BBPunfix(b->batCacheid);
        throw(MAL, "algebra.mbrsubselect", RUNTIME_OBJECT_MISSING);
    }
    if (cid && *cid != bat_nil && (c = BATdescriptor(*cid)) == NULL) {
        BBPunfix(b->batCacheid);
        BBPunfix(s->batCacheid);
        throw(MAL, "algebra.mbrsubselect", RUNTIME_OBJECT_MISSING);
    }
    if(BATmbrsubselect(&bn, b, s, c) != GDK_SUCCEED)
        bn = NULL;
    BBPunfix(b->batCacheid);
    BBPunfix(s->batCacheid);
    if (c)
        BBPunfix(c->batCacheid);
    if (bn == NULL)
        throw(MAL, "algebra.mbrsubselect", GDK_EXCEPTION);
    if (!(bn->batDirty&2)) BATsetaccess(bn, BAT_READ);
    *result = bn->batCacheid;
    BBPkeepref(bn->batCacheid);
    return MAL_SUCCEED;
}

str ALGmbrsubselect2(bat *result, const ptr *dims, const ptr* dim, const bat *sid) {
    return ALGmbrsubselect(result, dims, dim, sid, NULL);
}

str ALGproject(bat *result, const ptr* candDims, const bat* candBAT) {	
	gdk_cells *candidatesDimensions = (gdk_cells*)*candDims;
	BAT *candidatesBAT, *resBAT;

	if(!candidatesDimensions) { //empty result
		//create an empy candidates BAT
		 if((resBAT = BATnew(TYPE_void, TYPE_oid, 0, TRANSIENT)) == NULL)
            throw(MAL, "algebra.cellsProject", GDK_EXCEPTION);
		BATsetcount(resBAT, 0);
		BATseqbase(resBAT, 0);
		BATderiveProps(resBAT, FALSE);

    	BBPkeepref((*result= resBAT->batCacheid));

		return MAL_SUCCEED;
	}

	if ((candidatesBAT = BATdescriptor(*candBAT)) == NULL) {
       	throw(MAL, "algebra.cellsProject", RUNTIME_OBJECT_MISSING);
    }

	resBAT = projectCells(candidatesDimensions, candidatesBAT);
	if(!resBAT)
		throw(MAL, "algebra.cellsProject", "Problem allocating new array");
	*result = resBAT->batCacheid;
    BBPkeepref(*result);

	//clean the candidates
	BBPunfix(candidatesBAT->batCacheid);
	freeCells(candidatesDimensions);

	
	return MAL_SUCCEED;
}


str ALGnonDimensionSubselect2(ptr *dimsRes, bat* oidsRes, const ptr *dims, const bat* values, const ptr *dimCands, const bat* oidCands,
                        const void *low, const void *high, const bit *li, const bit *hi, const bit *anti) {
	gdk_array *array = (gdk_array*)*dims;
	BAT *valuesBAT = NULL, *candidatesBAT = NULL;
	BAT *projectedDimsBAT = NULL;
	BAT *oidsResBAT = NULL;
	gdk_cells *dimensionsCandidates = NULL;
	gdk_cells *mbrRes = cells_new();
	
	oid* oids;
	BUN oidsNum;
	BUN i=0;

	if ((valuesBAT = BATdescriptor(*values)) == NULL) {
       	throw(MAL, "algebra.nonDimensionSubselect", RUNTIME_OBJECT_MISSING);
    }

	//read any canidates coming from a previous selection
	readCands(&dimensionsCandidates, &candidatesBAT, dimCands, oidCands, array);

	//find the oids corresponding to the candidates
	projectedDimsBAT = projectCells(dimensionsCandidates, candidatesBAT);	
	
	//get the oids that satisfy the condition. Use the input candidates as candidates
	if(!(oidsResBAT = BATsubselect(valuesBAT, projectedDimsBAT, low, high, *li, *hi, *anti))) {
		BBPunfix(valuesBAT->batCacheid);
		BBPunfix(candidatesBAT->batCacheid);
		freeCells(dimensionsCandidates);
		GDKfree(array->dimSizes);
		GDKfree(array);
		throw(MAL, "algebra.nonDimensionSubselect", GDK_EXCEPTION);
	}
	if(BATcount(oidsResBAT) == 0) { //empty results
		BBPunfix(valuesBAT->batCacheid);
		BBPunfix(candidatesBAT->batCacheid);
		freeCells(dimensionsCandidates);
		BBPunfix(oidsResBAT->batCacheid);
		GDKfree(array->dimSizes);
		GDKfree(array);
		return emptyCandidateResults(dimsRes, oidsRes);
	}

	//find the mbr around the qualifying oids
	oids = (oid*)Tloc(oidsResBAT, BUNfirst(oidsResBAT));
	oidsNum = BATcount(oidsResBAT);
	for(i=0 ; i<array->dimsNum ; i++) {
		BUN min, max;
		gdk_dimension *dimNew ;
		BUN *idxs = oidToIdx_bulk(oids, oidsNum, i, 0, 1, array); \
		//find the min and the max of the dimension
		min=max=idxs[0];
		for(i=1; i<oidsNum; i++) {
			BUN newIdx = idxs[i];
			min = min > newIdx ? newIdx : min;
			max = max < newIdx ? newIdx : max;
		} 
		dimNew = createDimension_oid(i, array->dimSizes[i], 0, array->dimSizes[i]-1, 1);
		mbrRes = cells_add_dimension(mbrRes, dimNew);

	}

	BBPunfix(valuesBAT->batCacheid);
	BBPunfix(candidatesBAT->batCacheid);
	freeCells(dimensionsCandidates);
	BBPkeepref((*oidsRes = oidsResBAT->batCacheid));
	*dimsRes = mbrRes;

	return MAL_SUCCEED;
/*
error:
	BBPunfix(valuesBAT->batCacheid);
	BBPunfix(candidatesBAT->batCacheid);
	freeCells(dimensionsCandidates);
	BBPunfix(oidsResBAT->barcaheid);
	GDKree(array->dimSizes);
	GDKfree(array);

	throw(MAL, "algebra.nonDimensionSubselect", GDK_EXCEPTION);
*/
}

str ALGnonDimensionSubselect1(ptr *dimsRes, bat* oidsRes, const ptr *dims, const bat* values,
                        const void *low, const void *high, const bit *li, const bit *hi, const bit *anti) {
	return ALGnonDimensionSubselect2(dimsRes, oidsRes, dims, values, NULL, NULL, low, high, li, hi, anti);
}

#endif
