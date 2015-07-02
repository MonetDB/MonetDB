#include "monetdb_config.h"
#include "mal_exception.h"
#include "arrays.h"

#include <gdk_arrays.h>

static gdk_dimension* getDimension(gdk_cells *dims, int dimNum) {
	dim_node *n;

	for(n=dims->h; n && n->data->dimNum<dimNum; n=n->next);
	return n->data;
}

static int jumpSize(gdk_array *array, int dimNum) {
	int i=0;
	BUN skip = 1;
	for(i=0; i<dimNum; i++)
		skip *= array->dimSizes[i];
	return skip;
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

static bool overlapingRanges(gdk_dimension *dim1, gdk_dimension *dim2) {
	if(*(oid*)dim1->max < *(oid*)dim2->min || *(oid*)dim2->max < *(oid*)dim1->min) {
		//disjoint ranges. empty result
		return 0;
	}
	return 1;
}

static gdk_dimension* mergeCandidateDimensions(gdk_dimension *dim1, gdk_dimension *dim2) {
	oid min, max, step;
	
	/*the biggest of the mins and the smallest of the maximums */
	min = *(oid*)dim1->min > *(oid*)dim2->min ? *(oid*)dim1->min : *(oid*)dim2->min;
	max = *(oid*)dim1->max < *(oid*)dim2->max ? *(oid*)dim1->max : *(oid*)dim2->max;
	step = *(oid*)dim1->step ; //step is always 1

	//the dimensions that are merged should have the same order
	//they also have the same number of initial elements because they came from the same dimension
	return createDimension_oid(dim1->dimNum, dim1->initialElementsNum, min, max, step); }

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

static BUN qualifyingOIDs(int dimNum, int skipSize, gdk_cells* oidDims, BAT* oidsBAT, oid **resOIDs ) {
	BUN sz = 0;
	BUN j;
	oid io;

	oid *qOIDS = NULL;

	gdk_dimension *oidsDim;
	dim_node *n;
	for(n=oidDims->h; n && n->data->dimNum<dimNum; n = n->next);
	oidsDim = n->data;

	if(dimNum == oidDims->dimsNum-1) { //last dimension
		if(oidsDim->elementsNum > 0) {
			qOIDS = GDKmalloc(sizeof(oid)*oidsDim->elementsNum);
			for(io=*(oid*)oidsDim->min, sz=0; io<=*(oid*)oidsDim->max; io+=*(oid*)oidsDim->step, sz++) {
				qOIDS[sz] = skipSize*io;
fprintf(stderr, "%u = %u\n", (unsigned int)sz, (unsigned int)qOIDS[sz]);
			}
		} else {
			//the oids are in the BAT
			//iterate over the elements in the BAT to find the indices of the dimension that have survided
			//and add and idx for each one of these elements
			oid* candOIDs = (oid*)Tloc(oidsBAT, BUNfirst(oidsBAT));
			int previousIdx = -1;
			BUN i;
			qOIDS = GDKmalloc(sizeof(oid)*oidsDim->initialElementsNum);

			for(i=0; i<BATcount(oidsBAT); i++) {
				int idx = candOIDs[i]/skipSize;
				if(idx > previousIdx) {
					qOIDS[sz] = skipSize*idx;
fprintf(stderr, "%u = %u\n", (unsigned int)sz, (unsigned int)qOIDS[sz]);
					sz++;
					previousIdx = idx;
				}
			}
		}
		*resOIDs = qOIDS;
	} else {
		oid *resOIDs_local = NULL;
		BUN addedEls = qualifyingOIDs(dimNum+1, skipSize*oidsDim->initialElementsNum, oidDims, oidsBAT, &resOIDs_local);
		if(dimNum == 0)
			qOIDS = *resOIDs;
		else {
			if(oidsDim->elementsNum > 0) 
				qOIDS = GDKmalloc(sizeof(oid)*oidsDim->elementsNum*addedEls);
			else
				qOIDS = GDKmalloc(sizeof(oid)*oidsDim->initialElementsNum*addedEls);
		}

		for(j=0, sz=0; j<addedEls; j++) {
fprintf(stderr, "-> %u = %u\n", (unsigned int)j, (unsigned int)resOIDs_local[j]);
			if(oidsDim->elementsNum > 0) {
				for(io=*(oid*)oidsDim->min; io<=*(oid*)oidsDim->max; io+=*(oid*)oidsDim->step, sz++) {
					qOIDS[sz] = resOIDs_local[j] + skipSize*io;
fprintf(stderr, "%u = %u\n", (unsigned int)sz, (unsigned int)qOIDS[sz]);
				}
			} else {
				//check the BAT
				oid* candOIDs = (oid*)Tloc(oidsBAT, BUNfirst(oidsBAT));
				int previousIdx = -1;
				BUN i;		
				for(i=0; i<BATcount(oidsBAT); i++) {
					int idx = (candOIDs[i]%(skipSize*oidsDim->initialElementsNum))/skipSize;
					if(idx > previousIdx) {
						qOIDS[sz] = resOIDs_local[j]+skipSize*idx;
fprintf(stderr, "%u = %u\n", (unsigned int)sz, (unsigned int)qOIDS[sz]);
						sz++;
						previousIdx = idx;
					}
				}
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

static BAT* joinBATs(BAT *candsBAT, BAT* dimBAT, gdk_array *array, int dimNum) {
	oid *candsOIDs, *dimOIDs, *mergedOIDs;
	BAT* mergedBAT;

	BUN i=0, j=0, k=0, minPos;
	BUN minIdx =0 ;
	BUN dimSkip = jumpSize(array, dimNum);

	oid dimOIDs_min = 0;
	bool set = 0;
	BUN moduloRes;

	candsOIDs = (oid*)Tloc(candsBAT, BUNfirst(candsBAT));
	dimOIDs = (oid*)Tloc(dimBAT, BUNfirst(dimBAT));

	//the oids in dimBAT have been computed assuming that 0 is allowed in all other dimensions
	//is this really true? I can verify that using the first oid in candsBAT
	//if a dimension after filtering is expressed again as a dimension the min might
	//not be 0 but I do not care about it when it comes to the BAT. I will resolve tha at the end
	//when projecting the cells where dimensions and BAT will be combined
	for(j=0; j< (unsigned long)dimNum; j++) {
		BUN skipCells = 0;
		//find the min oid of this dimension in the the first qualifying oid
		minIdx = oidToIdx(candsOIDs[0], j+1, 0, 1, array);
		if(minIdx == 0)
			continue;
		//all oids in the dimOIDs should be updated to comply with the min oid of the dimension
		skipCells = jumpSize(array, j);
		for(i=0; i<BATcount(dimBAT); i++)
			dimOIDs[i] += skipCells*minIdx;
	}
	
	if(!(mergedBAT = BATnew(TYPE_void, TYPE_oid, BATcount(candsBAT)+BATcount(dimBAT), TRANSIENT)))
		return NULL;
	mergedOIDs = (oid*)Tloc(mergedBAT, BUNfirst(mergedBAT));

	moduloRes = dimOIDs[0]%dimSkip;
	/* find the oids in cands that are there to reflect dimNum and keep only those that dim and cand have in common */
	for(i=0, j=0; i<BATcount(candsBAT) && j<BATcount(dimBAT);) {
		/* oids in this dimension should be multiples of dimSkip */	
		if(candsOIDs[i]%dimSkip == moduloRes) {
			if(candsOIDs[i] < dimOIDs[j]) //it exists in one but not in the other
				i++;
			else if (candsOIDs[i] > dimOIDs[j])
				j++;
			else { //common
				mergedOIDs[k] = candsOIDs[i];

				if(!set) {
					dimOIDs_min = candsOIDs[i];
					minPos = k;
					set = 1;
				}

				i++;
				j++;
				k++;
			}
		} else {
			/*not related with the dimension. send it to the output*/
			mergedOIDs[k] = candsOIDs[i];
			i++;
			k++;
		}
	}

	BATseqbase(mergedBAT, 0);
	BATsetcount(mergedBAT, k);
	BATderiveProps(mergedBAT, FALSE);

	//adapt the candidates BAT to reflect the minimum value for the new dimension
	//only the ones that are not reflecting the y dimension should be updates
	minIdx = oidToIdx(dimOIDs_min, dimNum, 0, 1,  array);
	if(minIdx > 0) {
		BUN skipCells = jumpSize(array, dimNum);
		/*split it in 2 parts. Firts update all oids that are before the minPos position
 		* all those oids will be increased and become greater than the oid in minPos */
		if(minPos > 0) {
			for(i=minPos-1; i>0; i--) {
				/*all will change because they are above the minimum oid regarding the dimension*/
				mergedOIDs[i+1] = mergedOIDs[i]+skipCells*minIdx;			
			}
			/* excluded from the loop because i>=0 always true (infinite loop)*/
			mergedOIDs[1] = mergedOIDs[0]+skipCells*minIdx;			
		}
		mergedOIDs[0] = dimOIDs_min;
		for(i=minPos+1; i<BATcount(mergedBAT) ; i++)
			if(mergedOIDs[i]%dimSkip != moduloRes)
				mergedOIDs[i] += skipCells*minIdx;
	}

	return mergedBAT;
}

static BAT* mergeBATs(BAT *candsBAT, BAT* dimBAT, gdk_array *array, int dimNum) {
	oid *candsOIDs, *dimOIDs, *mergedOIDs;
	BAT* mergedBAT;

	BUN i=0, j=0, k=0;
	BUN minIdx =0 ;

	candsOIDs = (oid*)Tloc(candsBAT, BUNfirst(candsBAT));
	dimOIDs = (oid*)Tloc(dimBAT, BUNfirst(dimBAT));

	//the oids in dimBAT have been computed assuming that 0 is allowed in all other dimensions
	//is this really true? I can verify that using the first oid in candsBAT
	//if a dimension after filtering is expressed again as a dimension the min might
	//not be 0 but I do not care about it when it comes to the BAT. I will resolve tha at the end
	//when projecting the cells where dimensions and BAT will be combined
	for(j=0; j< (unsigned long)dimNum; j++) {
		BUN skipCells = 0;
		//find the min oid of this dimension in the the first qualifying oid
		minIdx = oidToIdx(candsOIDs[0], j+1, 0, 1, array);
		if(minIdx == 0)
			continue;
		//all oids in the dimOIDs should be updated to comply with the min oid of the dimension
		skipCells = jumpSize(array, j+1);
		for(i=0; i<BATcount(dimBAT); i++)
			dimOIDs[i] += skipCells*minIdx;
	}
	//adapt the candidates BAT to reflect the minimum value for the new dimension
	minIdx = oidToIdx(dimOIDs[0], dimNum, 0, 1,  array);
	if(minIdx > 0) {
		BUN skipCells = jumpSize(array, dimNum);
		for(i=0; i<BATcount(candsBAT); i++)
			candsOIDs[i] += skipCells*minIdx;
	}

	//finaly merge the two BATs
	if(!(mergedBAT = BATnew(TYPE_void, TYPE_oid, BATcount(candsBAT)+BATcount(dimBAT), TRANSIENT)))
		return NULL;
	mergedOIDs = (oid*)Tloc(mergedBAT, BUNfirst(mergedBAT));
	for(i=0, j=0; i<BATcount(candsBAT) && j<BATcount(dimBAT);) {
		if(candsOIDs[i] < dimOIDs[j]) {
			mergedOIDs[k] = candsOIDs[i];
			i++;
		} else if(candsOIDs[i] > dimOIDs[j]) {
			mergedOIDs[k] = dimOIDs[j];
			j++;
		} else {
			mergedOIDs[k] = candsOIDs[i];
			i++;
			j++;
		}
		k++;

		if(i == BATcount(candsBAT)) {
			for(; j<BATcount(dimBAT); j++, k++)
				mergedOIDs[k] = dimOIDs[j];
		}

		if(j == BATcount(dimBAT)) {
			for(; i<BATcount(dimBAT); i++, k++)
				mergedOIDs[k] = candsOIDs[i];
		}
	}

	BATseqbase(mergedBAT, 0);
	BATsetcount(mergedBAT, k);
	BATderiveProps(mergedBAT, FALSE);

	return mergedBAT;
}

static bool updateCandidateResults(gdk_array* array, 
									gdk_cells** dimensionsCandidates_out, BAT** candidatesBAT_out, 
									gdk_cells *dimensionsCandidates_in, BAT* candidatesBAT_in,
									int dimNum, oid min, oid max) {
	gdk_dimension *dimensionCand_in = getDimension(dimensionsCandidates_in, dimNum);
	gdk_dimension *dimensionCand_out = createDimension_oid(dimNum, dimensionCand_in->initialElementsNum, min, max, 1); //step cannot be 0 or infinite loop
	
	if(dimensionCand_in->elementsNum == 0) {
		//the dimension is in the BAT
		//express the results in BAT and merge
		BUN i, skipCells;
		oid j;
		oid *dimOIDs = NULL;
		BAT *dimBAT = BATnew(TYPE_void, TYPE_oid, max-min+1, TRANSIENT);
		if(!dimBAT) {
			throw(MAL, "algebra.dimensionSubselect", "Prolbem creating new BAT");
			return 0;
		}
		dimOIDs  = (oid*)Tloc(dimBAT, BUNfirst(dimBAT));
		skipCells = jumpSize(array, dimNum);
		i=0;
		for(j=min; j<=max; j++) {
			dimOIDs[i] = j*skipCells;
			i++;
		}
		
		BATsetcount(dimBAT, i);
		BATseqbase(dimBAT, 0);
		BATderiveProps(dimBAT, FALSE);    
		
		*candidatesBAT_out = joinBATs(candidatesBAT_in, dimBAT, array, dimNum);

		//the dimensions do not change
		*dimensionsCandidates_out = dimensionsCandidates_in;

		return 1;

	}	
	//if the existing results for the dimension and the new computed results can be combined in a new dimension
	if(overlapingRanges(dimensionCand_in, dimensionCand_out)) {
		//merge the dimension in the candidates with the result of this operation
		dimensionCand_out = mergeCandidateDimensions(dimensionCand_in, dimensionCand_out);
		if(!dimensionCand_out) {
			//something went wrong while merging (some memory problem) 
			throw(MAL, "algebra.dimensionSubselect", "Something went wrong while megind dimensions"); 
			return 0;
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
		if(qualifyingIdx >= dimension->initialElementsNum && !*anti) {
			//remove all the dimensions, there will be no results in the output
			freeCells(dimensionsCandidates_in);
			return emptyCandidateResults(dimsRes, oidsRes);
		}  if(qualifyingIdx >= dimension->initialElementsNum && *anti) {
			//the whole dimension qualifies for the outpu
			//we send out whatever came int
			dimensionsCandidates_out = dimensionsCandidates_in;
			candidatesBAT_out = candidatesBAT_in;
		} else if(*anti) {
			//two ranges qualify for the result [0, quaifyingIdx-1] and [qualifyingIdx+1, max]
			BUN i=0;
			gdk_dimension *dimensionCand_out;
			BUN skipCells = jumpSize(array, dimension->dimNum);
			oid* dimOIDs = NULL;
			BAT *dimBAT = BATnew(TYPE_void, TYPE_oid, dimension->initialElementsNum-1, TRANSIENT);
			if(!dimBAT)
				throw(MAL, "algebra.dimensionSubselect", "Proble creating new BAT");
			dimOIDs = (oid*)Tloc(dimBAT, BUNfirst(dimBAT));

			for(i=0; i<dimension->initialElementsNum; i++) {
				if(i==qualifyingIdx)
					continue;
				*dimOIDs++ = i*skipCells; //I assume all other dimensions are zero
			}
			BATseqbase(dimBAT, 0);
			BATsetcount(dimBAT, dimension->initialElementsNum-1);
			BATderiveProps(dimBAT, FALSE);

			if(BATcount(candidatesBAT_in) == 0) //nothing in the BAT, send to the output the BAT just created
				candidatesBAT_out = dimBAT;
			else {
				candidatesBAT_out = mergeBATs(candidatesBAT_in, dimBAT, array, dimension->dimNum);
				if(!candidatesBAT_out)
					throw(MAL, "algebra.dimensionSubselect", "Problen when merging BATs");
				BBPunfix(dimBAT->batCacheid);
			}
			//fix the dimensions. Keep all in the output but put 0 els in the current one
			dimensionCand_out = createDimension_oid(dimension->dimNum, dimension->initialElementsNum, 0, 0, 1);
			dimensionCand_out->elementsNum =0;
			//replace the dimension with the new one
			dimensionsCandidates_out = cells_replace_dimension(dimensionsCandidates_in, dimensionCand_out);
		} else {
			if(!updateCandidateResults(array, &dimensionsCandidates_out, &candidatesBAT_out, dimensionsCandidates_in, candidatesBAT_in, dimension->dimNum, qualifyingIdx, qualifyingIdx)) {
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
			if(!updateCandidateResults(array, &dimensionsCandidates_out, &candidatesBAT_out, dimensionsCandidates_in, candidatesBAT_in, dimension->dimNum, qualifyingIdx_min, qualifyingIdx_max)) {
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
			if(!updateCandidateResults(array, &dimensionsCandidates_out, &candidatesBAT_out, dimensionsCandidates_in, candidatesBAT_in, dimension->dimNum, qualifyingIdx_min, qualifyingIdx_max)) {
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
			if(!updateCandidateResults(array, &dimensionsCandidates_out, &candidatesBAT_out, dimensionsCandidates_in, candidatesBAT_in, dimension->dimNum, qualifyingIdx_min, qualifyingIdx_max)) {
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
	for(n=candidatesDimensions->h; n; n=n->next) {
		BUN sz = n->data->elementsNum;
		if(sz > 0)
			resSize *= sz;	
		else
			resSize *= n->data->initialElementsNum;
	}
	resSize += BATcount(candidatesBAT); //this is not accurate but I believe it is ok
fprintf(stderr, "estiamted size = %u\n", (unsigned int)resSize);		
	/*the size of the result is the same as the number of cells in the candidatesDimensions */
	if(!(resBAT = BATnew(TYPE_void, TYPE_oid, resSize, TRANSIENT)))
		throw(MAL, "algebra.cellsProject", "Problem allocating new array");
	resOIDs = (oid*)Tloc(resBAT, BUNfirst(resBAT));
	resSize = qualifyingOIDs(0, 1, candidatesDimensions, candidatesBAT, &resOIDs);
fprintf(stderr, "real size = %u\n", (unsigned int)resSize);		
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

