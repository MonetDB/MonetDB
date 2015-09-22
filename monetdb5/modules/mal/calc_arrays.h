#ifndef _CALC_ARRAYS_H
#define _CALC_ARRAYS_H


//str CMDdimensionCONVERT_void(ptr *dimRes, ptr *dimsRes, const ptr *dim, const ptr *dims);
//str CMDdimensionCONVERT_bit(ptr *dimRes, ptr *dimsRes, const ptr *dim, const ptr *dims);
str CMDdimensionCONVERT_bte(ptr *dimRes, ptr *dimsRes, const ptr *dim, const ptr *dims);
str CMDdimensionCONVERT_sht(ptr *dimRes, ptr *dimsRes, const ptr *dim, const ptr *dims);
str CMDdimensionCONVERT_int(ptr *dimRes, ptr *dimsRes, const ptr *dim, const ptr *dims);
str CMDdimensionCONVERT_wrd(ptr *dimRes, ptr *dimsRes, const ptr *dim, const ptr *dims);
str CMDdimensionCONVERT_lng(ptr *dimRes, ptr *dimsRes, const ptr *dim, const ptr *dims);
str CMDdimensionCONVERT_flt(ptr *dimRes, ptr *dimsRes, const ptr *dim, const ptr *dims);
str CMDdimensionCONVERT_dbl(ptr *dimRes, ptr *dimsRes, const ptr *dim, const ptr *dims);
str CMDdimensionCONVERT_oid(ptr *dimRes, ptr *dimsRes, const ptr *dim, const ptr *dims);
//str CMDdimensionCONVERT_str(ptr *dimRes, ptr *dimsRes, const ptr *dim, const ptr *dims);

str CMDdimensionMULsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
str CMDdimensionEQ(ptr* dimsRes, bat* batRes, const ptr* dim1, const ptr* dims1, const ptr* dim2, const ptr* dims2);

#endif /*_CALC_ARRAYS_H*/
