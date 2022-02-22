/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _COPY_H_
#define _COPY_H_


#define MAX_LINE_LENGTH (32 * 1024 * 1024)

#define bailout(f, ...) do { \
		msg = createException(SQL, f,  __VA_ARGS__); \
		goto end; \
	} while (0)

extern str COPYparse_generic(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);


#endif /*_COPY_H_*/
