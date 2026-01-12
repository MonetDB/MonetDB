/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

/*
 * Author M. Kersten
 * The MAL Interpreter
 */

#ifndef _MAL_INTERPRET_H
#define _MAL_INTERPRET_H

#include "mal_client.h"
#include "mal_profiler.h"
#include "mal_arguments.h"

/*
 * Activation of a thread requires construction of the argument list
 * to be passed by a handle.
 */

mal_export MalStkPtr prepareMALstack(allocator *pa, MalBlkPtr mb, int size);
mal_export str runMAL(Client c, MalBlkPtr mb, MalBlkPtr mbcaller,
					  MalStkPtr env);
mal_export str runMALsequence(Client cntxt, MalBlkPtr mb, int startpc,
							  int stoppc, MalStkPtr stk, MalStkPtr env,
							  InstrPtr pcicaller);
mal_export str reenterMAL(Client cntxt, MalBlkPtr mb, int startpc, int stoppc,
						  MalStkPtr stk);
mal_export str callMAL(Client cntxt, MalBlkPtr mb, MalStkPtr *glb,
					   ValPtr argv[]);
mal_export void garbageElement(Client cntxt, ValPtr v);
mal_export void garbageCollector(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
								 int flag);

mal_export ptr getArgReference(MalStkPtr stk, InstrPtr pci, int k);

#endif /*  _MAL_INTERPRET_H */
