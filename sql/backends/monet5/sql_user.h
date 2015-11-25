/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#ifndef _SQL_USER_H_
#define _SQL_USER_H_
#include "sql.h"		/* includes sql_backend.h */

sql5_export void monet5_user_init(backend_functions *be_funcs);
sql5_export str monet5_user_set_def_schema(mvc *m, oid user /* mal user id */);
sql5_export str monet5_user_get_def_schema(mvc *m, int user /* sql user id */);

sql5_export str db_users_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str db_password_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _SQL_USER_H_ */
