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

#include <sql_config.h>
#include "rel_semantic.h"
#include "rel_prop.h"

void
prop_destroy( prop *p )
{
	prop *q;

	while (p) {
		q = p->p;
		_DELETE(p);
		p = q;
	}
}

prop *
prop_create( int kind, prop *pre )
{
	prop *p = NEW(prop);
	
	p->kind = kind;
	p->value = 0;
	p->p = pre;
	return p;
}

prop *
find_prop( prop *p, int kind)
{
	while(p) {
		if (p->kind == kind)
			return p;
		p = p->p;
	}
	return p;
}
