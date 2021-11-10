
#ifndef _FOR_H
#define _FOR_H

#include "sql.h"

extern str FORcompress_col(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str FORdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _FOR_H */

