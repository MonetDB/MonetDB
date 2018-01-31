/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/* Python 3 UDFs use the exact same source code as Python 2 UDFs */
/* Except they are linked to a different library (Python 3/Python 2 libraries respectively) */
/* We simply include the source code from the Python 2 UDF directory */

#include "type_conversion.c"
