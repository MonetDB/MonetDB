/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
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
 * @f example
 * @a N.J. Nes
 * @* The simple example module
 * In this section we should explain this simple module
 *
 */
/*
 * @+ Implementation Code for example module.
 */
#include <template_config.h>
#include "example.h"
#include <mal_exception.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>

str
hello(str *res, str *arg0)
{
	str src = *arg0;
	size_t len = strlen(src) + strlen("hello ");

	*res = GDKmalloc(len + 1);
	*res[0] = '\0';
	strcat(*res, "hello ");
	strcat(*res, src);
	return MAL_SUCCEED;
}

