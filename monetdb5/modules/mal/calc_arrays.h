#ifndef _CALC_ARRAYS_H
#define _CALC_ARRAYS_H


str CMDdimensionCONVERT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
str CMDdimensionMULsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
str CMDdimensionEQ(ptr* dimsRes, bat* oidsRes, const ptr* dim1, const ptr* dims1, const ptr* dim2, const ptr* dims2);

#endif /*_CALC_ARRAYS_H*/
