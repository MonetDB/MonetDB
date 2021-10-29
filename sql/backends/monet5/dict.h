
#ifndef _DICT_H
#define _DICT_H

#include "sql.h"

extern str FORcompress_col(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str FORdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

extern str DICTcompress_col(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

extern str DICTcompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str DICTdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str DICTconvert(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str DICTjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str DICTthetaselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str DICTselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str DICTrenumber(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _DICT_H */

