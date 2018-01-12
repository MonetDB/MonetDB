/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _MAL_MACRO_H_
#define _MAL_MACRO_H_

mal_export str MACROprocessor(Client cntxt, MalBlkPtr mb, Symbol t);
mal_export int inlineMALblock(MalBlkPtr mb, int pc, MalBlkPtr mc);
mal_export str OPTmacroImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str OPTorcamImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str OPTmacro(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str OPTorcam(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

#endif /* _MAL_MACRO_H_ */
