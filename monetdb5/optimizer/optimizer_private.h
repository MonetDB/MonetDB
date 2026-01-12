/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

/* This file should not be included in any file outside of this directory */

#ifndef LIBOPTIMIZER
#error this file should not be included outside its source directory
#endif

str OPTwrapper(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
