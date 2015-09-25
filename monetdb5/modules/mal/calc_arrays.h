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

str CMDnonDimensionalCONVERTsignal_bit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
str CMDnonDimensionalCONVERTsignal_bte(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
str CMDnonDimensionalCONVERTsignal_sht(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
str CMDnonDimensionalCONVERTsignal_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
str CMDnonDimensionalCONVERTsignal_wrd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
str CMDnonDimensionalCONVERTsignal_lng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
str CMDnonDimensionalCONVERTsignal_flt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
str CMDnonDimensionalCONVERTsignal_dbl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
str CMDnonDimensionalCONVERTsignal_oid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str CMDdimensionMULsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
str CMDscalarMULsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
str CMDscalarMULenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);


str CMDdimensionsADDsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
str CMDscalarADDsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
str CMDscalarADDenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
str CMDarrayADDsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
str CMDarrayADDenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str CMDdimensionsEQ(ptr* dimsRes, bat* batRes, const ptr* dim1, const ptr* dims1, const ptr* dim2, const ptr* dims2);

#endif /*_CALC_ARRAYS_H*/
