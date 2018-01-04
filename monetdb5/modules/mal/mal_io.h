/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _PRINT_H_
#define _PRINT_H_

#include "mal.h"
#include "mal_instruction.h"
#include "mal_interpreter.h"

typedef struct {
	int up, down;
} bipipe;

typedef FILE *fstream;

mal_export str IOprint_val(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str IOprintf(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str IOprintfStream(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str IOtable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str IOexport(void *ret, bat *bid, str *fnme);
mal_export str IOimport(void *ret, bat *bid, str *fnme);
mal_export str io_stdin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str io_stdout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str io_stderr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

mal_export str IOsetmallocsuccesscount(void *res, lng *nbytes);

#endif /* _PRINT_H_ */
