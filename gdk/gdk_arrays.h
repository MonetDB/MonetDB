#ifndef _GDK_ARRAYS_H
#define _GDK_ARRAYS_H

#include "gdk.h"
#include <math.h>

typedef struct dimensionAnalyticStruct {
	bte type;
	int dimNum;
	void *min;
	void *max;
	void *step;
	unsigned int elsNum;
} gdk_analytic_dimension;

/*
typedef struct dim_node {
    struct dim_node *next;
    gdk_dimension *data;
} dim_node;

typedef struct cells {
    dim_node *h;
    dim_node *t;
    int dimsNum;
} gdk_cells;
*/

//As long as the dimension is not projected the following info is enough
typedef struct dimensionStruct {
	unsigned int min; //initialy this is set to 0
	unsigned int max;
	unsigned int step; //initialy this is set to 1
	unsigned int elsNum;
} gdk_dimension;

typedef struct arrayStruct {
	unsigned short dimsNum; //the number of dimensions (0 to 65,535)
	gdk_dimension **dims; //an array having the dimensions. The dimNum of each dimension is implicit, its position in the aray
} gdk_array;

typedef enum errors {
	general_error,
    new_bat,
    dimension_type,
    value_type
} errors;

gdk_return gdk_error_msg(errors errorCode, const char* funcName, const char *msg);

gdk_export gdk_dimension* createDimension_bte(bte min, bte max, bte step);
gdk_export gdk_dimension* createDimension_sht(sht min, sht max, sht step);
gdk_export gdk_dimension* createDimension_int(int min, int max, int step);
gdk_export gdk_dimension* createDimension_wrd(wrd min, wrd max, wrd step);
gdk_export gdk_dimension* createDimension_oid(oid min, oid max, oid step);
gdk_export gdk_dimension* createDimension_lng(lng min, lng max, lng step);
gdk_export gdk_dimension* createDimension_dbl(dbl min, dbl max, dbl step);
gdk_export gdk_dimension* createDimension_flt(flt min, flt max, flt step);

gdk_export gdk_analytic_dimension* createAnalyticDimension_bte(unsigned short dimNum, bte min, bte max, bte step);
gdk_export gdk_analytic_dimension* createAnalyticDimension_sht(unsigned short dimNum, sht min, sht max, sht step);
gdk_export gdk_analytic_dimension* createAnalyticDimension_int(unsigned short dimNum, int min, int max, int step);
gdk_export gdk_analytic_dimension* createAnalyticDimension_wrd(unsigned short dimNum, wrd min, wrd max, wrd step);
gdk_export gdk_analytic_dimension* createAnalyticDimension_oid(unsigned short dimNum, oid min, oid max, oid step);
gdk_export gdk_analytic_dimension* createAnalyticDimension_lng(unsigned short dimNum, lng min, lng max, lng step);
gdk_export gdk_analytic_dimension* createAnalyticDimension_dbl(unsigned short dimNum, dbl min, dbl max, dbl step);
gdk_export gdk_analytic_dimension* createAnalyticDimension_flt(unsigned short dimNum, flt min, flt max, flt step);

gdk_export gdk_array* arrayNew(unsigned short dimsNum);
gdk_export gdk_return arrayDelete(gdk_array *array);
gdk_export gdk_return analyticDimensionDelete(gdk_analytic_dimension *dim);
gdk_export gdk_array* arrayCopy(gdk_array* array);


/*find the position in the dimension indices (no repetitions) of the given value*/
#define dimensionFndValuePos(value, min, step) fmod((value-min), step)? BUN_NONE : (BUN)(value-min)/step

/*find the position in the dimension indices (no repetitions) of the  given value
 * or the position of the index that is closest to the given value and greater than it*/
#define dimensionFndGreaterValuePos(value, min, step, eq) \
	({\
		BUN pos = 0; \
		if(value >= min) { \
			/*the index of the value greater or equal to a value < min is the index of the min (0)*/ \
			pos = (BUN)(value-min)/step; \
			fmod((value-min), step) ? ++pos : (pos +=(1-eq)); \
		} \
		pos; \
	})

/*find the position in the dimension indices (no repetitions) of the  given value
 * or the position of the index that is closest to the given value and smaller than it*/
#define dimensionFndLowerValuePos(value, min, max, step, eq) \
	({\
		BUN pos = 0; \
		if(value <= max) { \
			if(value < min) \
				pos = BUN_NONE; \
			pos = (BUN)(value-min)/step; \
			fmod((value-min), step) ? pos : (pos -= (1-eq)); \
		} else {\
			/*the index of the value  <= to a value > max is the index of the max*/ \
			pos = (BUN)(max-min)/step; \
		} \
		pos; \
	})


#define equalIdx(dim, value) \
({\
	BUN idx = 0; \
	switch(dim->type) { \
        case TYPE_bte: \
			idx = dimensionFndValuePos(*(bte*)value, *(bte*)dim->min, *(bte*)dim->step); \
			break; \
        case TYPE_sht: \
			idx = dimensionFndValuePos(*(sht*)value, *(sht*)dim->min, *(sht*)dim->step); \
            break; \
        case TYPE_int:\
			idx = dimensionFndValuePos(*(int*)value, *(int*)dim->min, *(int*)dim->step); \
            break; \
        case TYPE_flt:\
			idx = dimensionFndValuePos(*(flt*)value, *(flt*)dim->min, *(flt*)dim->step); \
            break; \
        case TYPE_dbl:\
			idx = dimensionFndValuePos(*(dbl*)value, *(dbl*)dim->min, *(dbl*)dim->step); \
            break; \
        case TYPE_lng:\
			idx = dimensionFndValuePos(*(lng*)value, *(lng*)dim->min, *(lng*)dim->step); \
            break; \
        case TYPE_hge:\
			idx = dimensionFndValuePos(*(hge*)value, *(hge*)dim->min, *(hge*)dim->step); \
			break; \
        case TYPE_oid:\
			idx = dimensionFndValuePos(*(oid*)value, *(oid*)dim->min, *(oid*)dim->step); \
            break; \
	} \
	idx; \
})

#define greaterIdx(dim, value, eq) \
({\
	BUN idx = 0; \
	switch(dim->type) { \
        case TYPE_bte: \
			idx = dimensionFndLowerValuePos(*(bte*)value, *(bte*)dim->min, *(bte*)dim->max, *(bte*)dim->step, eq); \
			break; \
        case TYPE_sht: \
			idx = dimensionFndLowerValuePos(*(sht*)value, *(sht*)dim->min, *(sht*)dim->max, *(sht*)dim->step, eq); \
            break; \
        case TYPE_int:\
			idx = dimensionFndLowerValuePos(*(int*)value, *(int*)dim->min, *(int*)dim->max, *(int*)dim->step, eq); \
            break; \
        case TYPE_flt:\
			idx = dimensionFndLowerValuePos(*(flt*)value, *(flt*)dim->min, *(flt*)dim->max, *(flt*)dim->step, eq); \
            break; \
        case TYPE_dbl:\
			idx = dimensionFndLowerValuePos(*(dbl*)value, *(dbl*)dim->min, *(dbl*)dim->max, *(dbl*)dim->step, eq); \
            break; \
        case TYPE_lng:\
			idx = dimensionFndLowerValuePos(*(lng*)value, *(lng*)dim->min, *(lng*)dim->max, *(lng*)dim->step, eq); \
            break; \
        case TYPE_hge:\
			idx = dimensionFndLowerValuePos(*(hge*)value, *(hge*)dim->min, *(hge*)dim->max, *(hge*)dim->step, eq); \
			break; \
        case TYPE_oid:\
			idx = dimensionFndLowerValuePos(*(oid*)value, *(oid*)dim->min, *(oid*)dim->max, *(oid*)dim->step, eq); \
            break; \
	} \
	idx; \
})

#define lowerIdx(dim, value, eq) \
({\
	BUN idx = 0; \
	switch(dim->type) { \
        case TYPE_bte: \
			idx = dimensionFndGreaterValuePos(*(bte*)value, *(bte*)dim->min, *(bte*)dim->step, eq); \
			break; \
        case TYPE_sht: \
			idx = dimensionFndGreaterValuePos(*(sht*)value, *(sht*)dim->min, *(sht*)dim->step, eq); \
            break; \
        case TYPE_int:\
			idx = dimensionFndGreaterValuePos(*(int*)value, *(int*)dim->min, *(int*)dim->step, eq); \
            break; \
        case TYPE_flt:\
			idx = dimensionFndGreaterValuePos(*(flt*)value, *(flt*)dim->min, *(flt*)dim->step, eq); \
            break; \
        case TYPE_dbl:\
			idx = dimensionFndGreaterValuePos(*(dbl*)value, *(dbl*)dim->min, *(dbl*)dim->step, eq); \
            break; \
        case TYPE_lng:\
			idx = dimensionFndGreaterValuePos(*(lng*)value, *(lng*)dim->min, *(lng*)dim->step, eq); \
            break; \
        case TYPE_hge:\
			idx = dimensionFndGreaterValuePos(*(hge*)value, *(hge*)dim->min, *(hge*)dim->step, eq); \
			break; \
        case TYPE_oid:\
			idx = dimensionFndGreaterValuePos(*(oid*)value, *(oid*)dim->min, *(oid*)dim->step, eq); \
            break; \
	} \
	idx; \
})







#if 0
gdk_cells* cells_new(void);
gdk_cells* cells_add_dimension(gdk_cells* cells, gdk_dimension *dim);
gdk_cells* cells_remove_dimension(gdk_cells* cells, int dimNum);
gdk_cells* cells_replace_dimension(gdk_cells* cells, gdk_dimension* dim);
gdk_export gdk_return freeDimension(gdk_dimension *dim);
gdk_export gdk_return freeCells(gdk_cells *cells);
#endif




#if 0
#define dimensionElsNum(dim) \
({ \
	BUN els = 0; \
	switch(dim->type.type->localtype) { \
		case TYPE_bte: \
            els = floor((dim->max->data.val.btval - dim->min->data.val.btval )/ dim->step->data.val.btval)+1; \
            break; \
        case TYPE_sht: \
            els = floor((dim->max->data.val.shval - dim->min->data.val.shval )/ dim->step->data.val.shval)+1; \
            break; \
        case TYPE_int: \
            els = floor((dim->max->data.val.ival - dim->min->data.val.ival )/ dim->step->data.val.ival)+1; \
            break; \
        case TYPE_wrd: \
            els = floor((dim->max->data.val.wval - dim->min->data.val.wval )/ dim->step->data.val.wval)+1; \
            break; \
        case TYPE_oid: \
            els = floor((dim->max->data.val.oval - dim->min->data.val.oval )/ dim->step->data.val.oval)+1; \
            break; \
        case TYPE_lng: \
            els = floor((dim->max->data.val.lval - dim->min->data.val.lval )/ dim->step->data.val.lval)+1; \
            break; \
        case TYPE_dbl: \
            els = floor((dim->max->data.val.dval - dim->min->data.val.dval )/ dim->step->data.val.dval)+1; \
            break; \
        case TYPE_flt: \
            els = floor((dim->max->data.val.fval - dim->min->data.val.fval )/ dim->step->data.val.fval)+1; \
            break; \
	} \
	els; \
})
#endif

#if 0
#define dimensionCharacteristics(TPE, dimensionBAT, min, max, step, elementRepeats, groupRepeats) \
do {\
    TPE *vls; \
    BUN i; \
    vls = (TPE*)Tloc(dimensionBAT, BUNfirst(dimensionBAT)); \
    *min = vls[0]; \
    *step = vls[BATcount(dimensionBAT)-1]; \
    *max = vls[BATcount(dimensionBAT)-2]; \
\
    *elementRepeats = *groupRepeats = 0; \
    vls = (TPE*)Tloc(dimensionBAT, BUNfirst(dimensionBAT)); \
\
    for(i=0; i<BATcount(dimensionBAT)-2; i++) { /*last element is the step and at least one element is max*/\
        if(vls[i] != *min) \
            break; \
        (*elementRepeats)++; \
    } \
    *groupRepeats = BATcount(dimensionBAT)-i-1; \
\
	/*a zero step shows a single element in the dimension but leads to infinite loops and we do not want that*/ \
	if(!(*step)) \
		*step = 1; \
} while(0)

#define dimensionElement(min, max, step, elementRepeats, oid) \
    ({ \
        long elementsNum = floor((max-min)/step) + 1; \
        long elementsPerGroup = elementsNum*elementRepeats; \
        long elementInGroup = floor((oid%elementsPerGroup)/elementRepeats); \
/*fprintf(stderr, "dimension element at %d is %d\n", (int)oid, (int)(min+elementInGroup*step)); */\
        min+elementInGroup*step; \
    })

#define dimensionElementsNum(min, max, step)\
    ({\
        long num = 1; \
        if(!step) { \
            if(min!=max) { \
                GDKerror("dimensionElementsNum: step is 0 but min and max are not equal\n"); \
                num = 0; \
            } \
        } else \
            num = floor((max-min)/step) + 1; \
        num; \
    })


BUN dimension_void_replace_bat(BAT *resBAT, BAT *oidsBAT, BAT *dimensionBAT, bit force);

//BAT* projectDimension(sql_dimension *oidsDim, sql_dimension *valuesDim);

BAT* materialiseDimensionBAT(BAT* dimensionBAT);
BUN dimensionBATsize(BAT* dimensionBAT);
BUN dimensionBATelementsNum(BAT* dimensionBAT);

gdk_return dimensionBATsubselect(BAT** outBAT, BAT *dimensionBAT, BAT *candsBAT, const void *low, const void *high, int includeLow, int includeHigh, int anti);
gdk_return BATmbrsubselect(BAT **outBAT, BAT *dimensionBAT, BAT *oidsBAT, BAT *candsBAT);
gdk_return dimensionBATproject(BAT** outBAT, BAT* oidsBAT, BAT* dimensionBAT);
BAT* dimensionBATproject_wrap(BAT* oidsBAT, BAT* dimensionBAT);
gdk_return BATmbrproject(BAT **outBAT, BAT *b, BAT *oidsToProjectBAT, BAT *subselectBAT);

gdk_return dimensionBATgroup(BAT **groups, BAT **extents, BAT **histo, BAT *dimensionBAT, BAT *g, BAT *e, BAT *h);
gdk_return dimensionBATgroupavg(BAT **bnp, BAT **cntsp, BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_return dimensionBATsubjoin(BAT **outBATl, BAT **outBATr, BAT *dimensionBATl, BAT *dimensionBATr, BAT *sl, BAT *sr, int nil_matches, BUN estimate);
#endif

/*NEW*/
gdk_export BAT *projectCells(gdk_array* dims, BAT* oidsBAT);
#if 0
gdk_export gdk_cells* arrayToCells(gdk_array *array);
gdk_export gdk_array *cellsToArray(gdk_cells *cells);
#endif

gdk_export BAT* materialise_nonDimensional_column(int columntype, unsigned int cellsNum, char* defVal);

#endif /* _GDK_ARRAYS_H */

