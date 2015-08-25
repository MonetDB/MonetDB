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

static BAT*
newempty(const char *func)
{
    BAT *bn = BATnew(TYPE_void, TYPE_void, 0, TRANSIENT);
    if (bn == NULL) {
        GDKerror("%s: memory allocation error", func);
        return NULL;
    }
    BATseqbase(bn, 0);
    BATseqbase(BATmirror(bn), 0);
    return bn;
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

str ALGdimensionSubselect2(ptr *dimsRes, bat *oidsRes, const ptr *dim, const ptr* dims, const ptr *dimsCands, const bat *oidsCands,
							const void *low, const void *high, const bit *li, const bit *hi, const bit *anti) {
	gdk_array *array = (gdk_array*)*dims;
	gdk_analytic_dimension *dimension = (gdk_analytic_dimension*)*dim;
	gdk_dimension *dimCand = NULL;
	gdk_array *dimCands_in = NULL;

	oid qualifyingIdx_min, qualifyingIdx_max;
	int type;
	const void* nil;

	BAT *oidsResBAT = newempty("leftfetchjoin"); //nothing returned in the oids
	BBPkeepref(*oidsRes = oidsResBAT->batCacheid);

	/*ingnore oidsCands. When projecting a dimension only the dimsCands are of interest */
	(void)*oidsCands;

	if(dimsCands) 
		dimCands_in = (gdk_array*)*dimsCands;
	else //all dimensions are candidates
		dimCands_in = arrayCopy(array);
	
	*dimsRes = dimCands_in; //the same pointers will be used but the dimension over which the selection will be performed might change 
	if(!dimCands_in) { //empty results
		arrayDelete(array); //I am not gona use it later I can delete it
		return MAL_SUCCEED;
	}
	dimCand = dimCands_in->dims[dimension->dimNum];

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

str ALGdimensionSubselect1(ptr *dimsRes, bat *oidsRes, const ptr *dim, const ptr* dims, 
							const void *low, const void *high, const bit *li, const bit *hi, const bit *anti) {
	return ALGdimensionSubselect2(dimsRes, oidsRes, dim, dims, NULL, NULL, low, high, li, hi, anti);
}

str ALGdimensionThetasubselect2(ptr *dimsRes, bat *oidsRes, const ptr *dim, const ptr* dims, const ptr *dimsCands, const bat *oidsCands, const void *val, const char **opp) {
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
        return ALGdimensionSubselect2(dimsRes, oidsRes, dim, dims, dimsCands, oidsCands, val, nil, &li, &hi, &anti);
    }
    if (op[0] == '!' && op[1] == '=' && op[2] == 0) {
        /* "!=" (equivalent to "<>") */ 
		li = hi = anti = 1;
        return ALGdimensionSubselect2(dimsRes, oidsRes, dim, dims, dimsCands, oidsCands, val, nil, &li, &hi, &anti);
    }
    if (op[0] == '<') { 
        if (op[1] == 0) {
            /* "<" */
			li = hi = anti = 0;
            return ALGdimensionSubselect2(dimsRes, oidsRes, dim, dims, dimsCands, oidsCands, nil, val, &li, &hi, &anti);
        }
        if (op[1] == '=' && op[2] == 0) {
            /* "<=" */
			li = anti = 0;
			hi = 1;
            return ALGdimensionSubselect2(dimsRes, oidsRes, dim, dims, dimsCands, oidsCands, nil, val, &li, &hi, &anti);
        }
        if (op[1] == '>' && op[2] == 0) {
            /* "<>" (equivalent to "!=") */ 
			li = hi = anti = 1;
            return ALGdimensionSubselect2(dimsRes, oidsRes, dim, dims, dimsCands, oidsCands, val, nil, &li, &hi, &anti);
        }
    }
    if (op[0] == '>') { 
        if (op[1] == 0) {
            /* ">" */
			li = hi = anti = 0;
            return ALGdimensionSubselect2(dimsRes, oidsRes, dim, dims, dimsCands, oidsCands, val, nil, &li, &hi, &anti);
        }
        if (op[1] == '=' && op[2] == 0) {
            /* ">=" */
			li = 1;
			hi = anti = 0;
            return ALGdimensionSubselect2(dimsRes, oidsRes, dim, dims, dimsCands, oidsCands, val, nil, &li, &hi, &anti);
        }
    }

    throw(MAL, "algebra.dimensionThetasubselect", "BATdimensionThetasubselect: unknown operator.\n");
}

str ALGdimensionThetasubselect1(ptr *dimsRes, bat *oidsRes, const ptr *dim, const ptr* dims, const void *val, const char **op) {
	return ALGdimensionThetasubselect2(dimsRes, oidsRes, dim, dims, NULL, NULL, val, op);
}

str ALGdimensionLeftfetchjoin(bat *result, const ptr *dimsCands, const bat *oidsCands, const ptr *dim, const ptr *dims) {
	gdk_array *array = (gdk_array*)*dims;
	gdk_analytic_dimension *dimension = (gdk_analytic_dimension*)*dim;
	gdk_array *dimCands_in = (gdk_array*)*dimsCands;
	
	BAT *resBAT = NULL;
	BUN resSize = 0;

	/*when projecting a dimension we ignore the oidsCands*/
	(void)*oidsCands;

	if(!dimCands_in) { //empty
		if(!(resBAT = newempty("dimensionLeftfetchjoin")))
			throw(MAL, "algebra.leftfetchjoin", "Problem allocating new BAT");
		BBPkeepref(*result = resBAT->batCacheid);
		return MAL_SUCCEED;
	} else {
		gdk_dimension *dimCand = dimCands_in->dims[dimension->dimNum];

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

str ALGnonDimensionLeftfetchjoin1(bat* result, const ptr *dimsCands, const bat *oidsCands, const bat *vals, const ptr *dims) {
	/* projecting a non-dimensional column does not differ from projecting any relational column */
	BAT *mbrCandsBAT = NULL, *oidsCandsBAT = NULL, *valsBAT = NULL, *resBAT= NULL;
	gdk_array *dimCands_in = (gdk_array*)*dimsCands;
	gdk_array *array = (gdk_array*)*dims;


 	if(!dimCands_in) { //empty
        if(!(resBAT = newempty("nonDimensionLeftfetchjoin1")))
            throw(MAL, "algebra.leftfetchjoin", "Problem allocating new BAT");
		BBPkeepref(*result = resBAT->batCacheid);

    	return MAL_SUCCEED;
    }

    if ((oidsCandsBAT = BATdescriptor(*oidsCands)) == NULL) {
        throw(MAL, "algebra.leftfetchjoin", RUNTIME_OBJECT_MISSING);
    }

	 if ((valsBAT = BATdescriptor(*vals)) == NULL) {
		BBPunfix(oidsCandsBAT->batCacheid);
        throw(MAL, "algebra.leftfetchjoin", RUNTIME_OBJECT_MISSING);
    }

	//create the oids using the candidates
	mbrCandsBAT = projectCells(dimCands_in, array);	

    resBAT = BATproject(mbrCandsBAT, valsBAT);

	if(BATcount(oidsCandsBAT) && BATcount(mbrCandsBAT) > BATcount(oidsCandsBAT)) {
		/*iterate over mbrCandsBAT and oidsCandsBAT and set NULL to all 
	 	* values in resBAT that are found in mbrCandsBAT but not in oidsCandsBAT */
		oid *mbrOids = (oid*)Tloc(mbrCandsBAT, BUNfirst(mbrCandsBAT));
		oid *oidsOids = (oid*)Tloc(oidsCandsBAT, BUNfirst(oidsCandsBAT));
	
		void *nilPtr = ATOMnilptr(BATttype(valsBAT));		

		BUN i,j;
		for(i=0, j=0; i<BATcount(oidsCandsBAT); i++, j++)	{
			while(mbrOids[i] != oidsOids[j]) {
				BUNreplace(resBAT, &i, nilPtr, TRUE); //replace with NULL
				i++;
			}
		}
	}

    BBPunfix(mbrCandsBAT->batCacheid);
	BBPunfix(valsBAT->batCacheid);
	BBPunfix(oidsCandsBAT->batCacheid);

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

#endif

str ALGnonDimensionSubselect2(ptr *dimsRes, bat *oidsRes, const bat* values, const ptr *dims, const ptr *dimsCands, const bat* oidsCands,
                            const void *low, const void *high, const bit *li, const bit *hi, const bit *anti) {
	gdk_array *array = (gdk_array*)*dims;
	gdk_array *mbrIn = NULL, *mbrOut = NULL;
	BAT *valuesBAT = NULL, *oidsCandsBAT = NULL, *oidsResBAT;

	if(dimsCands) {
		mbrIn = (gdk_array*)*dimsCands;
		mbrOut = arrayCopy(mbrIn);
		arrayDelete(mbrIn);
	} else
		mbrOut = arrayCopy(array);

	if ((valuesBAT = BATdescriptor(*values)) == NULL) {
       	throw(MAL, "algebra.nonDimensionSubselect", RUNTIME_OBJECT_MISSING);
    }

	if(!oidsCands)
		oidsCandsBAT = NULL;	
	else if ((oidsCandsBAT = BATdescriptor(*oidsCands)) == NULL) {
		BBPunfix(valuesBAT->batCacheid);
       	throw(MAL, "algebra.nonDimensionSubselect", RUNTIME_OBJECT_MISSING);
    }

	/*find the values that satisfy the predicate*/
	if(!(oidsResBAT = BATsubselect(valuesBAT, oidsCandsBAT, low, high, *li, *hi, *anti))) {
		BBPunfix(valuesBAT->batCacheid);
		if(oidsCandsBAT)
			BBPunfix(oidsCandsBAT->batCacheid);

		arrayDelete(array);
		*dimsRes = NULL;		

		throw(MAL, "algebra.nonDimensionSubselect", "Problen in BATsubselect");
	}

	if(BATcount(oidsResBAT) == 0) { //empty results
		mbrOut = NULL;
	} else {
		//find the mbr around the qualifying oids
		oid* oids = (oid*)Tloc(oidsResBAT, BUNfirst(oidsResBAT));
		oid currentOid = oids[0];
		BUN i=0;
		unsigned short d;
		unsigned int dimIdx;
		unsigned int totalElsNum = 1, elsNum;
		for(d=0; d<array->dimsNum; d++) {
				totalElsNum *= array->dims[d]->elsNum;
		}
		
		//initialise the mbr
		elsNum = totalElsNum;
		for(d = array->dimsNum-1; d>0; d--) {
			//get the correct elsNum
			elsNum /= array->dims[d]->elsNum;
			//get the idx for the dimension
			dimIdx = currentOid/elsNum;
			//update the dimension
			mbrOut->dims[d]->min = dimIdx;
			mbrOut->dims[d]->max = dimIdx;
			//update the oid
			currentOid %= elsNum;
		}
		//update the last dimension
		mbrOut->dims[d]->min = currentOid;
		mbrOut->dims[d]->max = currentOid;


		for(i=1; i<BATcount(oidsResBAT); i++) {
			elsNum = totalElsNum;
			currentOid = oids[i];

			for(d = array->dimsNum-1; d>0; d--) {
				//get the correct elsNum
				elsNum /= array->dims[d]->elsNum;
				//get the idx for the dimension
				dimIdx = currentOid/elsNum;
				//update the dimension
				mbrOut->dims[d]->min = dimIdx < mbrOut->dims[d]->min ? dimIdx : mbrOut->dims[d]->min;
				mbrOut->dims[d]->max = dimIdx > mbrOut->dims[d]->max ? dimIdx : mbrOut->dims[d]->max;
				//update the oid
				currentOid %= elsNum;
			}
			//update the last dimension
			mbrOut->dims[d]->min = currentOid < mbrOut->dims[d]->min ? currentOid : mbrOut->dims[d]->min;
			mbrOut->dims[d]->max = currentOid > mbrOut->dims[d]->max ? currentOid : mbrOut->dims[d]->max;

		}

		//update the elsNum
		for(d=0; d<mbrOut->dimsNum; d++) {
			mbrOut->dims[d]->elsNum = floor((mbrOut->dims[d]->max-mbrOut->dims[d]->min)/mbrOut->dims[d]->step) + 1;
		}
	}

	BBPunfix(valuesBAT->batCacheid);
	if(oidsCandsBAT)
		BBPunfix(oidsCandsBAT->batCacheid);
	BBPkeepref((*oidsRes = oidsResBAT->batCacheid));

	*dimsRes = mbrOut;

	return MAL_SUCCEED;
}

str ALGnonDimensionSubselect1(ptr *dimsRes, bat *oidsRes, const bat *values, const ptr *dims,
                            const void *low, const void *high, const bit *li, const bit *hi, const bit *anti) {
	return ALGnonDimensionSubselect2(dimsRes, oidsRes, values, dims, NULL, NULL, low, high, li, hi, anti);
}

str ALGnonDimensionThetasubselect2(ptr *dimsRes, bat *oidsRes, const bat *values, const ptr* dims, const ptr *dimsCands, const bat *oidsCands, const void *val, const char **opp) {
	bit li = 0;
	bit hi = 0;
	bit anti = 0;
	const char *op = *opp;
	BAT *valuesBAT =  BATdescriptor(*values);
	const void *nil;

	if(!valuesBAT) {
		throw(MAL, "algebra.nonDimensionThetasubselect", RUNTIME_OBJECT_MISSING);
	}

	nil = ATOMnilptr(BATttype(valuesBAT));
	BBPunfix(valuesBAT->batCacheid);

	if (op[0] == '=' && ((op[1] == '=' && op[2] == 0) || op[2] == 0)) {
        /* "=" or "==" */
		li = hi = 1;
		anti = 0;
        return ALGnonDimensionSubselect2(dimsRes, oidsRes, values, dims, dimsCands, oidsCands, val, nil, &li, &hi, &anti);
    }
    if (op[0] == '!' && op[1] == '=' && op[2] == 0) {
        /* "!=" (equivalent to "<>") */ 
		li = hi = anti = 1;
        return ALGnonDimensionSubselect2(dimsRes, oidsRes, values, dims, dimsCands, oidsCands, val, nil, &li, &hi, &anti);
    }
    if (op[0] == '<') { 
        if (op[1] == 0) {
            /* "<" */
			li = hi = anti = 0;
            return ALGnonDimensionSubselect2(dimsRes, oidsRes, values, dims, dimsCands, oidsCands, nil, val, &li, &hi, &anti);
        }
        if (op[1] == '=' && op[2] == 0) {
            /* "<=" */
			li = anti = 0;
			hi = 1;
            return ALGnonDimensionSubselect2(dimsRes, oidsRes, values, dims, dimsCands, oidsCands, nil, val, &li, &hi, &anti);
        }
        if (op[1] == '>' && op[2] == 0) {
            /* "<>" (equivalent to "!=") */ 
			li = hi = anti = 1;
            return ALGnonDimensionSubselect2(dimsRes, oidsRes, values, dims, dimsCands, oidsCands, val, nil, &li, &hi, &anti);
        }
    }
    if (op[0] == '>') { 
        if (op[1] == 0) {
            /* ">" */
			li = hi = anti = 0;
            return ALGnonDimensionSubselect2(dimsRes, oidsRes, values, dims, dimsCands, oidsCands, val, nil, &li, &hi, &anti);
        }
        if (op[1] == '=' && op[2] == 0) {
            /* ">=" */
			li = 1;
			hi = anti = 0;
            return ALGnonDimensionSubselect2(dimsRes, oidsRes, values, dims, dimsCands, oidsCands, val, nil, &li, &hi, &anti);
        }
    }

    throw(MAL, "algebra.dimensionThetasubselect", "BATdimensionThetasubselect: unknown operator.\n");
}

str ALGnonDimensionThetasubselect1(ptr *dimsRes, bat *oidsRes, const bat *values, const ptr* dims, const void *val, const char **op) {
	return ALGnonDimensionThetasubselect2(dimsRes, oidsRes, values, dims, NULL, NULL, val, op);
}


