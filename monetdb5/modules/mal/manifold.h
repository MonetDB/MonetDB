/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/*
 * M. Kersten
 * Default multiplex operator implementation
 */
#ifndef _MANIFOLD_LIB_
#define _MANIFOLD_LIB_
#include <string.h>

#include "mal.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

mal_export MALfcn MANIFOLDtypecheck(Client cntxt, MalBlkPtr mb, InstrPtr pci,
									int checkprops);

#endif /* _MANIFOLD_LIB_ */
