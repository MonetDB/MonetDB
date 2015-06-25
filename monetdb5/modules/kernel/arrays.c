#include "monetdb_config.h"
#include "mal_exception.h"
#include "arrays.h"

#include <gdk_arrays.h>

static gdk_dimension* getDimension(gdk_cells *dims, int dimNum) {
	dim_node *n;

	for(n=dims->h; n->data->dimNum<dimNum; n=n->next);
	return n->data;
}

/*takes a set of dimensions of any type and returns the correspondin 
 * dimensions in indices format */
static gdk_cells* sizesToDimensions(gdk_array *array) {
	gdk_cells *resDims = cells_new();
	int i=0;
	
	for(i=0; i<array->dimsNum; i++) {
		gdk_dimension *dim = createDimension_oid(i, array->dimSizes[i], 0, array->dimSizes[i]-1, 1);
		resDims = cells_add_dimension(resDims, dim);
	}
	return resDims;
}

static bool compatibleRanges(gdk_dimension *dim1, gdk_dimension *dim2) {
	if(*(oid*)dim1->max < *(oid*)dim2->min || *(oid*)dim2->max < *(oid*)dim1->min) {
		//disjoint ranges. empty result
		return 0;
	}
	return 1;
}

static gdk_dimension* merge2CandidateDimensions(gdk_dimension *dim1, gdk_dimension *dim2) {
	oid min, max, step;
	
	//check whether the merge creates a dimension or not
	//In case it does not return NULL and then a BAT should be created	

	/*the biggest of the mins and the smallest of the maximums */
	min = *(oid*)dim1->min > *(oid*)dim2->min ? *(oid*)dim1->min : *(oid*)dim2->min;
	max = *(oid*)dim1->max < *(oid*)dim2->max ? *(oid*)dim1->max : *(oid*)dim2->max;
	step = *(oid*)dim1->step ; //step is always 1

	//the dimensions that are merged should have the same order
	//they also have the same number of initial elements because the came from the same dimension
	return createDimension_oid(dim1->dimNum, dim1->initialElementsNum, min, max, step); }

static gdk_dimension* mergeCandidateDimensions(gdk_dimension *dim1, gdk_dimension *dim2) {
	return merge2CandidateDimensions(dim1, dim2);	
}

static BUN oidToIdx(oid oidVal, int dimNum, int currentDimNum, BUN skipCells, gdk_array *dims) {
	BUN oid = 0;

	while(currentDimNum < dimNum) {
		skipCells*=dims->dimSizes[currentDimNum];
		currentDimNum++;
	}

	if(currentDimNum == dims->dimsNum-1)
		oid = oidVal;
	else
		oid = oidToIdx(oidVal, dimNum, currentDimNum+1, skipCells*dims->dimSizes[currentDimNum], dims);

	if(currentDimNum == dimNum) //in the last one we do not compute module
		return oid/skipCells;
	return oid%skipCells;
}

static BUN qualifyingOIDs(int dimNum, int skipSize, gdk_cells* oidDims, oid **resOIDs ) {
	BUN sz = 0;
	BUN j;
	oid io;

	oid *qOIDS = NULL;

	gdk_dimension *oidsDim;
	dim_node *n;
	for(n=oidDims->h; n && n->data->dimNum<dimNum; n = n->next);
	oidsDim = n->data;

	if(dimNum == oidDims->dimsNum-1) { //last dimension
		qOIDS = GDKmalloc(sizeof(oid)*oidsDim->elementsNum);
		for(io=*(oid*)oidsDim->min, sz=0; io<=*(oid*)oidsDim->max; io+=*(oid*)oidsDim->step, sz++) {
			qOIDS[sz] = skipSize*io;
fprintf(stderr, "%u = %u\n", (unsigned int)sz, (unsigned int)qOIDS[sz]);
		}
		*resOIDs = qOIDS;
	} else {
		oid *resOIDs_local = NULL;
		BUN addedEls = qualifyingOIDs(dimNum+1, skipSize*oidsDim->initialElementsNum, oidDims, &resOIDs_local);
		if(dimNum == 0)
			qOIDS = *resOIDs;
		else
			qOIDS = GDKmalloc(sizeof(oid)*oidsDim->elementsNum*addedEls);

		for(j=0, sz=0; j<addedEls; j++) {
fprintf(stderr, "-> %u = %u\n", (unsigned int)j, (unsigned int)resOIDs_local[j]);
			for(io=*(oid*)oidsDim->min; io<=*(oid*)oidsDim->max; io+=*(oid*)oidsDim->step, sz++) {
				qOIDS[sz] = resOIDs_local[j] + skipSize*io;
fprintf(stderr, "%u = %u\n", (unsigned int)sz, (unsigned int)qOIDS[sz]);
			}
		}
		*resOIDs = qOIDS;
	}

	return sz;
}

#define computeValues(TPE) \
do { \
	TPE min = *(TPE*)dimension->min; \
	TPE step = *(TPE*)dimension->step; \
	BUN p, q; \
	BATiter candsIter = bat_iterator(candsBAT); \
	TPE *vals; \
	if(!(resBAT = BATnew(TYPE_void, TYPE_##TPE, resSize, TRANSIENT))) \
        throw(MAL, "algebra.dimensionLeftfetchjoin", "Problem allocating new BAT"); \
	vals = (TPE*)Tloc(resBAT, BUNfirst(resBAT)); \
\
	BATloop(candsBAT, p, q) { \
    	oid qOid = *(oid *) BUNtail(candsIter, p); \
		BUN idx = oidToIdx(qOid, dimension->dimNum, 0, 1, array); \
		*vals++ = min +idx*step; \
fprintf(stderr, "%d - %d - %d\n", (int)qOid, (int)idx, (int)vals[-1]); \
    } \
} while(0)

str ALGdimensionLeftfetchjoin(bat *result, const bat *cands, const ptr *dims, const ptr *dim) {
	gdk_array *array = (gdk_array*)*dims;
	gdk_dimension *dimension = (gdk_dimension*)*dim;
	BAT *candsBAT = NULL, *resBAT = NULL;
	BUN resSize = 0;

	if ((candsBAT = BATdescriptor(*cands)) == NULL) {
        throw(MAL, "algebra.dimensionLeftfetchjoin", RUNTIME_OBJECT_MISSING);
    }
	resSize = BATcount(candsBAT);
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

	BATsetcount(resBAT, resSize);
	BATseqbase(resBAT, 0);
    BATderiveProps(resBAT, FALSE);    

	*result = resBAT->batCacheid;
    BBPkeepref(*result);

	//free the space occupied by the dimension
	freeDimension(dimension);
    return MAL_SUCCEED;

}

static str emptyCandidateResults(ptr *candsRes_dims, bat* candsRes_bid) {
	BAT *candidatesBAT = NULL;

	if((candidatesBAT = BATnew(TYPE_void, TYPE_oid, 0, TRANSIENT)) == NULL)
        throw(MAL, "algebra.dimensionSubselect", GDK_EXCEPTION);
	BATsetcount(candidatesBAT, 0);
	BATseqbase(candidatesBAT, 0);
	BATderiveProps(candidatesBAT, FALSE);    

	BBPkeepref(*candsRes_bid = candidatesBAT->batCacheid);
	*candsRes_dims = NULL;

	return MAL_SUCCEED;
}

static bool updateCandidateResults(gdk_cells** dimensionsCandidates_out, BAT** candidatesBAT_out, 
									gdk_cells *dimensionsCandidates_in, BAT* candidatesBAT_in,
									int dimNum, BUN elsNum, oid min, oid max) {
	//the dimension comes from the original the elements and initial elements num is the same
	gdk_dimension *dimensionCand_out = createDimension_oid(dimNum, elsNum, min, max, 1); //step cannot be 0 or infinite loop
	gdk_dimension *dimensionCand_in = getDimension(dimensionsCandidates_in, dimNum);
	
	//if the existing results for the dimension and the new computed results can be combined in a new dimension
	if(compatibleRanges(dimensionCand_in, dimensionCand_out)) {
		//merge the dimension in the candidates with the result of this operation
		dimensionCand_out = mergeCandidateDimensions(dimensionCand_in, dimensionCand_out);
		if(!dimensionCand_out) {
			//the dimensions cannot be merged to a new dimension. Create a BAT
			*dimensionsCandidates_out = cells_remove_dimension(dimensionsCandidates_in, dimNum);
		} else {
			*dimensionsCandidates_out = cells_replace_dimension(dimensionsCandidates_in, dimensionCand_out);
			//the candidatesBAT does not change
			*candidatesBAT_out = candidatesBAT_in;
		}
	} else //cannot produce candidates. Empty result
		return 0;

	return 1;	
}

str ALGdimensionSubselect2(ptr *dimsRes, bat* oidsRes, const ptr *dims, const ptr* dim, const ptr *dimsCand, const bat* oidsCand, 
							const void *low, const void *high, const bit *li, const bit *hi, const bit *anti) {
	gdk_array *array = (gdk_array*)*dims; //the sizes of all the dimensions (I treat them as indices and I do not care about the exact values)
	gdk_dimension *dimension = (gdk_dimension*)*dim;

	gdk_cells *dimensionsCandidates_in = NULL, *dimensionsCandidates_out = NULL;
	BAT *candidatesBAT_in = NULL, *candidatesBAT_out = NULL;

	int type;
	const void *nil;

	(void)(bit*)anti;
	
	if(oidsCand) {
		if ((candidatesBAT_in = BATdescriptor(*oidsCand)) == NULL) {
        	throw(MAL, "algebra.dimensionSubselect", RUNTIME_OBJECT_MISSING);
    	}
	}

	if(dimsCand)
		dimensionsCandidates_in = (gdk_cells*)*dimsCand;

	//if there are no candidates then everything is a candidate
	if(!dimsCand && !oidsCand) {
		dimensionsCandidates_in = sizesToDimensions(array);
		//create an empy candidates BAT
		 if((candidatesBAT_in = BATnew(TYPE_void, TYPE_oid, 0, TRANSIENT)) == NULL)
            throw(MAL, "algebra.dimensionSubselect", GDK_EXCEPTION);
		BATsetcount(candidatesBAT_in, 0);
		BATseqbase(candidatesBAT_in, 0);
		BATderiveProps(candidatesBAT_in, FALSE);    
	} 

	if(!dimensionsCandidates_in) //empty results
		return emptyCandidateResults(dimsRes, oidsRes);

    type = dimension->type;
    nil = ATOMnilptr(type);
    type = ATOMbasetype(type);

    if(ATOMcmp(type, low, high) == 0) { //point selection   
		//find the idx of the value
		oid qualifyingIdx = equalIdx(dimension, low); 
		if(qualifyingIdx >= dimension->initialElementsNum) {
			//remove all the dimensions, there will be no results in the output
			freeCells(dimensionsCandidates_in);
			return emptyCandidateResults(dimsRes, oidsRes);
		}  else {
			if(!updateCandidateResults(&dimensionsCandidates_out, &candidatesBAT_out, dimensionsCandidates_in, candidatesBAT_in, dimension->dimNum, dimension->initialElementsNum, qualifyingIdx, qualifyingIdx)) {
				//remove all the dimensions, there will be no results in the output
				freeCells(dimensionsCandidates_in);
				return emptyCandidateResults(dimsRes, oidsRes);
			}
		}
	} else if(ATOMcmp(type, low, nil) == 0) { //no lower bound
		oid qualifyingIdx_min = 0; 
		oid qualifyingIdx_max = greaterIdx(dimension, high, *hi); 
	
		if(qualifyingIdx_max >= dimension->initialElementsNum) {
			freeCells(dimensionsCandidates_in);
			return emptyCandidateResults(dimsRes, oidsRes);
		} else {
			if(!updateCandidateResults(&dimensionsCandidates_out, &candidatesBAT_out, dimensionsCandidates_in, candidatesBAT_in, dimension->dimNum, dimension->initialElementsNum, qualifyingIdx_min, qualifyingIdx_max)) {
				//remove all the dimensions, there will be no results in the output
				freeCells(dimensionsCandidates_in);
				return emptyCandidateResults(dimsRes, oidsRes);
			}
		} 
	} else if(ATOMcmp(type, high, nil) == 0) { //no upper bound
		oid qualifyingIdx_min = lowerIdx(dimension, low, *li); 
		oid qualifyingIdx_max = dimension->initialElementsNum-1;

		if(qualifyingIdx_min >= dimension->initialElementsNum) {
			freeCells(dimensionsCandidates_in);
			return emptyCandidateResults(dimsRes, oidsRes);
		} else {
			if(!updateCandidateResults(&dimensionsCandidates_out, &candidatesBAT_out, dimensionsCandidates_in, candidatesBAT_in, dimension->dimNum, dimension->initialElementsNum, qualifyingIdx_min, qualifyingIdx_max)) {
				//remove all the dimensions, there will be no results in the output
				freeCells(dimensionsCandidates_in);
				return emptyCandidateResults(dimsRes, oidsRes);
			}
		} 
	} else if(ATOMcmp(type, low, nil) != 0 && ATOMcmp(type, high, nil) != 0) {
		oid qualifyingIdx_min = lowerIdx(dimension, low, *li); 
		oid qualifyingIdx_max = greaterIdx(dimension, high, *hi); 

		if(qualifyingIdx_max >= dimension->initialElementsNum || qualifyingIdx_min >= dimension->initialElementsNum) {
			freeCells(dimensionsCandidates_in);
			return emptyCandidateResults(dimsRes, oidsRes);
		} else {
			if(!updateCandidateResults(&dimensionsCandidates_out, &candidatesBAT_out, dimensionsCandidates_in, candidatesBAT_in, dimension->dimNum, dimension->initialElementsNum, qualifyingIdx_min, qualifyingIdx_max)) {
				//remove all the dimensions, there will be no results in the output
				freeCells(dimensionsCandidates_in);
				return emptyCandidateResults(dimsRes, oidsRes);
			}
		} 
	} else {
		//both values are NULL. Empty result
		return emptyCandidateResults(dimsRes, oidsRes);
	}

	if(oidsCand && candidatesBAT_in != candidatesBAT_out) //there was a candidatesBAT in the input that is not sent in the output
		BBPunfix(candidatesBAT_in->batCacheid);

	BBPkeepref(*oidsRes = candidatesBAT_out->batCacheid);

	*dimsRes = dimensionsCandidates_out;

	return MAL_SUCCEED;
}

str ALGdimensionSubselect1(ptr *dimsRes, bat* oidsRes, const ptr *dims, const ptr* dim, 
							const void *low, const void *high, const bit *li, const bit *hi, const bit *anti) {
	return ALGdimensionSubselect2(dimsRes, oidsRes, dims, dim, NULL, NULL, low, high, li, hi, anti);
}

str ALGdimensionThetasubselect2(ptr *dimsRes, bat* oidsRes, const ptr *dims, const ptr* dim, const ptr *dimsCand, const bat* oidsCand, const void *val, const char **opp) {
	bit li = 0;
	bit hi = 0;
	bit anti = 0;
	const char *op = *opp;
	gdk_dimension *dimension = *dim;
	const void *nil = ATOMnilptr(dimension->type);

	if (op[0] == '=' && ((op[1] == '=' && op[2] == 0) || op[2] == 0)) {
        /* "=" or "==" */
		li = hi = 1;
		anti = 0;
        return ALGdimensionSubselect2(dimsRes, oidsRes, dims, dim, dimsCand, oidsCand, val, nil, &li, &hi, &anti);
    }
    if (op[0] == '!' && op[1] == '=' && op[2] == 0) {
        /* "!=" (equivalent to "<>") */ 
		li = hi = anti = 1;
        return ALGdimensionSubselect2(dimsRes, oidsRes, dims, dim, dimsCand, oidsCand, val, nil, &li, &hi, &anti);
    }
    if (op[0] == '<') { 
        if (op[1] == 0) {
            /* "<" */
			li = hi = anti = 0;
            return ALGdimensionSubselect2(dimsRes, oidsRes, dims, dim, dimsCand, oidsCand, nil, val, &li, &hi, &anti);
        }
        if (op[1] == '=' && op[2] == 0) {
            /* "<=" */
			li = anti = 0;
			hi = 1;
            return ALGdimensionSubselect2(dimsRes, oidsRes, dims, dim, dimsCand, oidsCand, nil, val, &li, &hi, &anti);
        }
        if (op[1] == '>' && op[2] == 0) {
            /* "<>" (equivalent to "!=") */ 
			li = hi = anti = 1;
            return ALGdimensionSubselect2(dimsRes, oidsRes, dims, dim, dimsCand, oidsCand, val, nil, &li, &hi, &anti);
        }
    }
    if (op[0] == '>') { 
        if (op[1] == 0) {
            /* ">" */
			li = hi = anti = 0;
            return ALGdimensionSubselect2(dimsRes, oidsRes, dims, dim, dimsCand, oidsCand, val, nil, &li, &hi, &anti);
        }
        if (op[1] == '=' && op[2] == 0) {
            /* ">=" */
			li = 1;
			hi = anti = 0;
            return ALGdimensionSubselect2(dimsRes, oidsRes, dims, dim, dimsCand, oidsCand, val, nil, &li, &hi, &anti);
        }
    }

    throw(MAL, "algebra.dimensionThetasubselect", "BATdimensionThetasubselect: unknown operator.\n");
}

str ALGdimensionThetasubselect1(ptr *dimsRes, bat* oidsRes, const ptr *dims, const ptr* dim, const void *val, const char **op) {
	return ALGdimensionThetasubselect2(dimsRes, oidsRes, dims, dim, NULL, NULL, val, op);
}

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

str ALGmbrsubselect(bat *result, const bat *bid, const bat *sid, const bat *cid) {
    BAT *b, *s = NULL, *c = NULL, *bn;

    if ((b = BATdescriptor(*bid)) == NULL) {
        throw(MAL, "algebra.mbrsubselect", RUNTIME_OBJECT_MISSING);
    }
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

str ALGmbrsubselect2(bat *result, const bat *bid, const bat *sid) {
    return ALGmbrsubselect(result, bid, sid, NULL);
}

str ALGproject(bat *result, const ptr* candDims, const bat* candBAT) {	
	gdk_cells *candidatesDimensions = (gdk_cells*)*candDims;
	BAT *candidatesBAT, *resBAT;
	BUN resSize = 1;
	dim_node *n;
	oid *resOIDs = NULL;

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

	/*combine the oidsDimensions in order to get the global oids (the cells)*/
	for(n=candidatesDimensions->h; n; n=n->next)
		resSize *= n->data->elementsNum;	
fprintf(stderr, "size = %u\n", (unsigned int)resSize);		
	/*the size of the result is the same as the number of cells in the candidatesDimensions */
	if(!(resBAT = BATnew(TYPE_void, TYPE_oid, resSize, TRANSIENT)))
		throw(MAL, "algebra.cellsProject", "Problem allocating new array");
	resOIDs = (oid*)Tloc(resBAT, BUNfirst(resBAT));
	resSize = qualifyingOIDs(0, 1, candidatesDimensions, &resOIDs);
	BATsetcount(resBAT, resSize);
	BATseqbase(resBAT, 0);
	BATderiveProps(resBAT, FALSE);    

	*result = resBAT->batCacheid;
    BBPkeepref(*result);

	//clean the candidates
	BBPunfix(candidatesBAT->batCacheid);
	freeCells(candidatesDimensions);

	
	return MAL_SUCCEED;
}

