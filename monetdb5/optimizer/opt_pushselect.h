/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef OPT_PUSHSELECT_H
#define OPT_PUSHSELECT_H
#include "opt_support.h"

extern str OPTpushselectImplementation(Client cntxt, MalBlkPtr mb,
									   MalStkPtr stk, InstrPtr pci);

#endif
