/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _SQL_SUBQUERY_H
#define _SQL_SUBQUERY_H

#include "sql.h"

extern str zero_or_one_error(Client ctx, ptr ret, const bat *bid, const bit *err );
extern str zero_or_one_error_bat(Client ctx, ptr ret, const bat *bid, const bat *err );
extern str zero_or_one(Client ctx, ptr ret, const bat *bid);
extern str SQLsubzero_or_one(Client ctx, bat *ret, const bat *b, const bat *gp, const bat *gpe, bit *no_nil);
extern str SQLall(Client ctx, ptr ret, const bat *bid);
extern str SQLall_grp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLnil(Client ctx, bit *ret, const bat *bid);
extern str SQLnil_grp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLany_cmp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLall_cmp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLanyequal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLanyequal_grp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLanyequal_grp2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLallnotequal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLallnotequal_grp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLallnotequal_grp2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

extern str SQLexist(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLsubexist(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

extern str SQLnot_exist(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLsubnot_exist(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _SQL_SUBQUERY_H */
