/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _REL_XML_H_
#define _REL_XML_H_

#include "rel_semantic.h"
#include "sql_semantic.h"
#include "sql_query.h"

extern sql_exp *rel_xml(sql_query *query, sql_rel **rel, symbol *s, int f, exp_kind knd);

#endif /*_REL_XML_H_ */
