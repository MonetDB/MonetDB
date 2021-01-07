/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * @- Implementation
 * The implementation of the XML atomary type is based
 * on linking in a portable library, e.g. libxml2 ?
 */
#ifndef XML_H
#define XML_H

#include "gdk.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_instruction.h"
#include "mal_exception.h"

typedef str xml;

mal_export int TYPE_xml;

extern str XMLxml2str(str *s, xml *x);
extern str XMLstr2xml(xml *x, const char **s);
extern str XMLxmltext(str *s, xml *x);
extern str XMLxml2xml(xml *x, xml *s);
extern str XMLdocument(xml *x, str *s);
extern str XMLcontent(xml *x, str *s);
extern str XMLisdocument(bit *x, str *s);
extern str XMLcomment(xml *x, str *s);
extern str XMLpi(xml *x, str *target, str *s);
extern str XMLroot(xml *x, xml *v, str *version, str *standalone);
extern str XMLparse(xml *x, str *doccont, str *s, str *option);
extern str XMLattribute(xml *ret, str *name, str *val);
extern str XMLelement(xml *ret, str *name, xml *nspace, xml *attr, xml *val);
extern str XMLelementSmall(xml *ret, str *name, xml *val);
extern str XMLconcat(xml *ret, xml *left, xml *right);
extern str XMLforest(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

extern size_t XMLquotestring(const char *s, char *buf, size_t len);
extern size_t XMLunquotestring(const char **p, char q, char *buf);

extern str XMLprelude(void *ret);
extern str XMLepilogue(void *ret);

#endif /* XML_H */
