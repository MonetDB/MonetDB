/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @+ Tutorial
 * This section explains how to use this module.
 *
 */
#ifndef __example_H__
#define __example_H__

#include <mal.h>

#ifdef WIN32
#ifndef LIBEXAMPLE
#define example_export extern __declspec(dllimport)
#else
#define example_export extern __declspec(dllexport)
#endif
#else
#define example_export extern
#endif

example_export str hello(str *ret0, str *arg0);

#endif /* __example_H__ */

