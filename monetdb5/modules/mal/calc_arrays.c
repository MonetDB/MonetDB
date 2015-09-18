#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_arrays.h"
#include "math.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

#include "calc_arrays.h"

str CMDdimensionCONVERT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
    char buf[20];
	ptr *dimConverted = getArgReference_ptr(stk, pci, 0);
	ptr *array_out = getArgReference_ptr(stk, pci, 1);
	ptr *dimOriginal = getArgReference_ptr(stk, pci, 2);
	ptr *array_in = getArgReference_ptr(stk, pci, 3);

	gdk_analytic_dimension *dim_out = NULL;
	gdk_analytic_dimension *dim_in = (gdk_analytic_dimension*)*dimOriginal;

	//the array is not affected by the convertion
	//I just need to pass it along
	*array_out = *array_in;

    (void) cntxt;
    (void) mb;



	(void)*buf;
	(void)*dim_in;
	(void)*dim_out;	
	/*
    if (VARconvert(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], 1) != GDK_SUCCEED) {
        snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
        return mythrow(MAL, buf, OPERATION_FAILED);
    }*/

	*dimConverted = dim_out;

    return MAL_SUCCEED;
}


str CMDdimensionMULsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	ptr *dimConverted = getArgReference_ptr(stk, pci, 0);
	ptr *array_out = getArgReference_ptr(stk, pci, 1);
	void *val = getArgReference(stk, pci, 2);
	ptr *dimOriginal = getArgReference_ptr(stk, pci, 3);
	ptr *array_in = getArgReference_ptr(stk, pci, 4);

	gdk_analytic_dimension *dim_out = NULL;
	gdk_analytic_dimension *dim_in = (gdk_analytic_dimension*)*dimOriginal;

	//the array is not affected by the convertion
	//I just need to pass it along
	*array_out = *array_in;

    (void) cntxt;
    (void) mb;

	(void)*(int*)val;
	(void)*dim_in;
	(void)*dim_out;

/*
    if (VARcalcmul(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 1) != GDK_SUCCEED)
        return mythrow(MAL, "calc.*", OPERATION_FAILED);
*/ 
	
	*dimConverted = dim_out;
   return MAL_SUCCEED;
}

str CMDdimensionEQ(ptr* dimsRes, bat* oidsRes, const ptr* dim1, const ptr* dims1, const ptr* dim2, const ptr* dims2) {
	(void)*dimsRes;
	(void)*oidsRes;
	(void)*dim1;
	(void)*dims1;
	(void)*dim2;
	(void)*dims2;
/*
    if (VARcalceq(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) != GDK_SUCCEED)
        return mythrow(MAL, "calc.==", OPERATION_FAILED);
*/
    return MAL_SUCCEED;
}

