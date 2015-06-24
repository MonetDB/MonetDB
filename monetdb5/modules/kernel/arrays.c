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

static gdk_dimension* merge2CandidateDimensions(gdk_dimension *dim1, gdk_dimension *dim2) {
	oid min, max, step;
	
	if(*(oid*)dim1->max < *(oid*)dim2->min || *(oid*)dim2->max < *(oid*)dim1->min) {
		//disjoint ranges. cannot merge
		return NULL;
	}

	/*the biggest of the mins and the smallest of the maximums */
	min = *(oid*)dim1->min > *(oid*)dim2->min ? *(oid*)dim1->min : *(oid*)dim2->min;
	max = *(oid*)dim1->max < *(oid*)dim2->max ? *(oid*)dim1->max : *(oid*)dim2->max;
	step = *(oid*)dim1->step ; //step is always 1

	//the dimensions that are merged should have the same order
	//they also have the same number of initial elements because the came from the same dimension
	return createDimension_oid(dim1->dimNum, dim1->initialElementsNum, min, max, step); }

static gdk_cells* mergeCandidateDimensions(gdk_cells *dims, gdk_dimension *dim) {
	gdk_dimension *candDim = getDimension(dims, dim->dimNum);
	gdk_dimension *mergedDim = merge2CandidateDimensions(candDim, dim);	


	//check the limits of the two dimensions
	if(!mergedDim) {
		fprintf(stderr, "Disjoint ranges. Create a BAT");
		//remove the dimension from the candidates
		return cells_remove_dimension(dims, candDim->dimNum);
	}

	//create a new dimension that is the combined result of the candidates and the dim
	return cells_replace_dimension(dims, mergedDim);
}

static BUN oidToIdx(oid oidVal, int currentDimNum, BUN skipCells, gdk_array *dims) {
	if(currentDimNum == dims->dimsNum-1)
		return oidVal%skipCells;
	else {
		if(currentDimNum == 0)
			return oidToIdx(oidVal, currentDimNum+1, skipCells*dims->dimSizes[currentDimNum], dims);
		return oidToIdx(oidVal, currentDimNum+1, skipCells*dims->dimSizes[currentDimNum], dims)%skipCells;
	}
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
		BUN idx = oidToIdx(qOid, 0, 1, array); \
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
    *result = resBAT->batCacheid;
    BBPkeepref(*result);

	//free the space occupied by the dimension
	freeDimension(dimension);
    return MAL_SUCCEED;

}

#if 0
static str
ALGbinary(bat *result, const bat *lid, const bat *rid, BAT* (*func)(BAT *, BAT *), const char *name)
{
    BAT *left, *right,*bn= NULL;

    if ((left = BATdescriptor(*lid)) == NULL) {
        throw(MAL, name, RUNTIME_OBJECT_MISSING);
    }
    if ((right = BATdescriptor(*rid)) == NULL) {
        BBPunfix(left->batCacheid);
        throw(MAL, name, RUNTIME_OBJECT_MISSING);
    }
    bn = (*func)(left, right);
    BBPunfix(left->batCacheid);
    BBPunfix(right->batCacheid);
    if (bn == NULL)
        throw(MAL, name, GDK_EXCEPTION);
    if (!(bn->batDirty&2))
        BATsetaccess(bn, BAT_READ);
    *result = bn->batCacheid;
    BBPkeepref(*result);
    return MAL_SUCCEED;
}

str ALGdimensionLeftfetchjoin(bat *result, const bat *lid, const bat *rid) {
    return ALGbinary(result, lid, rid, dimensionBATproject_wrap, "algebra.dimension_leftfetchjoin");
}
#endif

str ALGdimensionSubselect2(ptr *dimsRes, bat* oidsRes, const ptr *dims, const ptr* dim, const ptr *dimsCand, const bat* oidsCand, 
							const void *low, const void *high, const bit *li, const bit *hi, const bit *anti) {
	gdk_array *array = (gdk_array*)*dims; //the sizes of all the dimensions (I treat them as indices and I do not care about the exact values)
	gdk_dimension *dimension = (gdk_dimension*)*dim;

	gdk_cells *dimensionsCandidates = NULL;
	BAT *candidatesBAT_in = NULL, *candidatesBAT_out = NULL;
	gdk_cells *dimensionsResult = cells_new();

	int type;
	const void *nil;

	(void)(bit*)li;
	(void)(bit*)hi;
	(void)(bit*)anti;
	
	if(oidsCand) {
		if ((candidatesBAT_in = BATdescriptor(*oidsCand)) == NULL) {
        	throw(MAL, "algebra.dimensionSubselect", RUNTIME_OBJECT_MISSING);
    	}
	}

	if(dimsCand)
		dimensionsCandidates = (gdk_cells*)*dimsCand;

	//if there are no candidates then everything is a candidate
	if(!dimsCand && !oidsCand) {
		dimensionsCandidates = sizesToDimensions(array);
		//create an empy candidates BAT
		 if((candidatesBAT_in = BATnew(TYPE_void, TYPE_oid, 0, TRANSIENT)) == NULL)
            throw(MAL, "algebra.dimensionSubselect", GDK_EXCEPTION);
		BATsetcount(candidatesBAT_in, 0);
		BATseqbase(candidatesBAT_in, 0);
	} 

	if(!dimensionsCandidates) { //empty results
		//create an empy candidates BAT
		if((candidatesBAT_out = BATnew(TYPE_void, TYPE_oid, 0, TRANSIENT)) == NULL)
            throw(MAL, "algebra.dimensionSubselect", GDK_EXCEPTION);
		BATsetcount(candidatesBAT_out, 0);
		BATseqbase(candidatesBAT_out, 0);

		*dimsRes = dimensionsResult;
		BBPkeepref(*oidsRes = candidatesBAT_out->batCacheid);
	
		return MAL_SUCCEED;			
	}


    type = dimension->type;
    nil = ATOMnilptr(type);
    type = ATOMbasetype(type);

    if(ATOMcmp(type, low, high) == 0) { //point selection   
		//find the idx of the value
		gdk_dimension *resDimension = NULL;
		oid qualifyingOID = equalIdx(dimension, low); 
		if(qualifyingOID >= dimension->initialElementsNum) {
			//no results. create empty candidates BAT
			if((candidatesBAT_out = BATnew(TYPE_void, TYPE_oid, 0, TRANSIENT)) == NULL)
            	throw(MAL, "algebra.dimensionSubselect", GDK_EXCEPTION);
			BATsetcount(candidatesBAT_out, 0);
			BATseqbase(candidatesBAT_out, 0);

			//remove all the dimensions, there will be no results in the output
			dimensionsResult = NULL;
			freeCells(dimensionsCandidates);
		}  else {
			//the dimension comes from the original this the elemenst and initial elements num is the same
			resDimension = createDimension_oid(dimension->dimNum, dimension->initialElementsNum, qualifyingOID, qualifyingOID, 1); //step cannot be 0 or infinite loop
		
			//merge the dimension in the candidates with the result of this operation
			dimensionsResult = mergeCandidateDimensions(dimensionsCandidates, resDimension);		
		}
	} else if(ATOMcmp(type, high, nil) == 0) { //find values greater than low
		return MAL_SUCCEED;
	} else if(ATOMcmp(type, low, nil) == 0) { //find values lower than high
		return MAL_SUCCEED;
	}

	if(!candidatesBAT_out)  //the BAT did not change
		candidatesBAT_out = candidatesBAT_in;
	else if(oidsCand)
		BBPunfix(candidatesBAT_in->batCacheid);

	BBPkeepref(*oidsRes = candidatesBAT_out->batCacheid);

	*dimsRes = dimensionsResult;

	return MAL_SUCCEED;
}

str ALGdimensionSubselect1(ptr *dimsRes, bat* oidsRes, const ptr *dims, const ptr* dim, 
							const void *low, const void *high, const bit *li, const bit *hi, const bit *anti) {
	return ALGdimensionSubselect2(dimsRes, oidsRes, dims, dim, NULL, NULL, low, high, li, hi, anti);
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

