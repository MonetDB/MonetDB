/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @- Implementation
 * The implementation of the XML atomary type is based
 * on linking in a portable library, e.g. libxml2 ?
 */
#ifndef XML_H
#define XML_H

#include <gdk.h>
#include "mal.h"
#include "mal_client.h"
#include "mal_instruction.h"
#include "mal_exception.h"

typedef str xml;

mal_export int TYPE_xml;

mal_export int XMLfromString(str src, int *len, xml *x);
mal_export int XMLtoString(str *s, int *len, xml src);

mal_export str XMLxml2str(str *s, xml *x);
mal_export str XMLstr2xml(xml *x, str *s);
mal_export str XMLxmltext(str *s, xml *x);
mal_export str XMLxml2xml(xml *x, xml *s);
mal_export str XMLdocument(xml *x, str *s);
mal_export str XMLcontent(xml *x, str *s);
mal_export str XMLisdocument(bit *x, str *s);
mal_export str XMLcomment(xml *x, str *s);
mal_export str XMLpi(xml *x, str *target, str *s);
mal_export str XMLroot(xml *x, xml *v, str *version, str *standalone);
mal_export str XMLparse(xml *x, str *doccont, str *s, str *option);
mal_export str XMLattribute(xml *ret, str *name, str *val);
mal_export str XMLelement(xml *ret, str *name, xml *nspace, xml *attr, xml *val);
mal_export str XMLelementSmall(xml *ret, str *name, xml *val);
mal_export str XMLconcat(xml *ret, xml *left, xml *right);
mal_export str XMLforest(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

mal_export size_t XMLquotestring(const char *s, char *buf, size_t len);
mal_export size_t XMLunquotestring(const char **p, char q, char *buf);

mal_export str XMLprelude(void *ret);

#endif /* XML_H */
