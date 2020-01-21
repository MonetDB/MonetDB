/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#ifndef _SQL_SUBQUERY_H
#define _SQL_SUBQUERY_H

#include "sql.h"

sql5_export str zero_or_one_error(ptr ret, const bat *bid, const bit *err );
sql5_export str zero_or_one_error_bat(ptr ret, const bat *bid, const bat *err );
sql5_export str zero_or_one(ptr ret, const bat *bid);
sql5_export str SQLsubzero_or_one(bat *ret, const bat *b, const bat *gp, const bat *gpe, bit *no_nil);
sql5_export str SQLall(ptr ret, const bat *bid);
sql5_export str SQLall_grp(bat *ret, const bat *l, const bat *gp, const bat *gpe, bit *no_nil);
sql5_export str SQLnil(bit *ret, const bat *bid);
sql5_export str SQLnil_grp(bat *ret, const bat *l, const bat *gp, const bat *gpe, bit *no_nil);
sql5_export str SQLany_cmp(bit *ret, const bit *cmp, const bit *nl, const bit *nr);
sql5_export str SQLall_cmp(bit *ret, const bit *cmp, const bit *nl, const bit *nr);
sql5_export str SQLanyequal(bit *ret, const bat *l, const bat *r);
sql5_export str SQLanyequal_grp(bat *ret, const bat *l, const bat *r, const bat *gp, const bat *gpe, bit *no_nil);
sql5_export str SQLanyequal_grp2(bat *ret, const bat *l, const bat *r, const bat *rid, const bat *gp, const bat *gpe, bit *no_nil);
sql5_export str SQLallnotequal(bit *ret, const bat *l, const bat *r);
sql5_export str SQLallnotequal_grp(bat *ret, const bat *l, const bat *r, const bat *gp, const bat *gpe, bit *no_nil);
sql5_export str SQLallnotequal_grp2(bat *ret, const bat *l, const bat *r, const bat *rid, const bat *gp, const bat *gpe, bit *no_nil);

sql5_export str SQLexist(bit *res, bat *id);
sql5_export str SQLexist_val(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLsubexist(bat *ret, const bat *b, const bat *gp, const bat *gpe, bit *no_nil);

sql5_export str SQLnot_exist(bit *res, bat *id);
sql5_export str SQLnot_exist_val(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLsubnot_exist(bat *ret, const bat *b, const bat *gp, const bat *gpe, bit *no_nil);

#endif /* _SQL_SUBQUERY_H */
