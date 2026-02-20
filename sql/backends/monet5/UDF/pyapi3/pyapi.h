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
 * M.Raasveldt & H. Muehleisen
 * The Python interface
 */

#ifndef _PYPI_LIB_
#define _PYPI_LIB_

#include "pyheader.h"

extern str PYAPI3PyAPIevalStd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
	__attribute__((__visibility__("hidden")));
extern str PYAPI3PyAPIevalAggr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
	__attribute__((__visibility__("hidden")));
extern str PYAPI3PyAPIevalStdMap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
	__attribute__((__visibility__("hidden")));
extern str PYAPI3PyAPIevalAggrMap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
	__attribute__((__visibility__("hidden")));
extern str PYAPI3PyAPIevalLoader(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
	__attribute__((__visibility__("hidden")));

extern bool PYAPI3PyAPIInitialized(void)
	__attribute__((__visibility__("hidden")));

extern str _loader_init(void)
	__attribute__((__visibility__("hidden")));

extern char *PyError_CreateException(char *error_text, char *pycall)
	__attribute__((__visibility__("hidden")));

pyapi_export bool option_disable_fork;

#endif /* _PYPI_LIB_ */
