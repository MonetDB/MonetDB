
#ifndef _DICT_H
#define _DICT_H

#include "sql.h"

extern str DICTcompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str DICTdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str DICTconvert(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _DICT_H */

