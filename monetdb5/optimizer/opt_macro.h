/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef _MAL_MACRO_H_
#define _MAL_MACRO_H_

extern int inlineMALblock(MalBlkPtr mb, int pc, MalBlkPtr mc);
extern str OPTmacro(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
extern str OPTorcam(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

#endif /* _MAL_MACRO_H_ */
