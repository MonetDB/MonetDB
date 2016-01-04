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

#ifdef WIN32
#ifndef LIBATOMS
#define xml_export extern __declspec(dllimport)
#else
#define xml_export extern __declspec(dllexport)
#endif
#else
#define xml_export extern
#endif

xml_export int TYPE_xml;

xml_export int XMLfromString(str src, int *len, xml *x);
xml_export int XMLtoString(str *s, int *len, xml src);

xml_export str XMLxml2str(str *s, xml *x);
xml_export str XMLstr2xml(xml *x, str *s);
xml_export str XMLxmltext(str *s, xml *x);
xml_export str XMLxml2xml(xml *x, xml *s);
xml_export str XMLdocument(xml *x, str *s);
xml_export str XMLcontent(xml *x, str *s);
xml_export str XMLisdocument(bit *x, str *s);
xml_export str XMLcomment(xml *x, str *s);
xml_export str XMLpi(xml *x, str *target, str *s);
xml_export str XMLroot(xml *x, xml *v, str *version, str *standalone);
xml_export str XMLparse(xml *x, str *doccont, str *s, str *option);
xml_export str XMLattribute(xml *ret, str *name, str *val);
xml_export str XMLelement(xml *ret, str *name, xml *nspace, xml *attr, xml *val);
xml_export str XMLelementSmall(xml *ret, str *name, xml *val);
xml_export str XMLconcat(xml *ret, xml *left, xml *right);
xml_export str XMLforest(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

xml_export size_t XMLquotestring(const char *s, char *buf, size_t len);
xml_export size_t XMLunquotestring(const char **p, char q, char *buf);

xml_export str XMLprelude(void *ret);

#endif /* XML_H */
