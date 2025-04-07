/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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

extern str XMLxml2str(Client ctx, str *s, const xml *x);
extern str XMLstr2xml(Client ctx, xml *x, const char *const*s);
extern str XMLxmltext(Client ctx, str *s, const xml *x);
extern str XMLxml2xml(Client ctx, xml *x, const xml *s);
extern str XMLdocument(Client ctx, xml *x, const char * const *s);
extern str XMLcontent(Client ctx, xml *x, const char * const *s);
extern str XMLisdocument(Client ctx, bit *x, const char * const *s);
extern str XMLcomment(Client ctx, xml *x, const char * const *s);
extern str XMLpi(Client ctx, xml *x, const char * const *target, const char * const *s);
extern str XMLroot(Client ctx, xml *x, const xml *v, const char * const *version, const char * const *standalone);
extern str XMLparse(Client ctx, xml *x, const char * const *doccont, const char * const *s, const char * const *option);
extern str XMLattribute(Client ctx, xml *ret, const char * const *name, const char * const *val);
extern str XMLelement(Client ctx, xml *ret, const char * const *name, const xml *nspace, const xml *attr, const xml *val);
extern str XMLelementSmall(Client ctx, xml *ret, const char * const *name, const xml *val);
extern str XMLconcat(Client ctx, xml *ret, const xml *left, const xml *right);
extern str XMLforest(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

extern size_t XMLquotestring(const char *s, char *buf, size_t len);
extern size_t XMLunquotestring(const char **p, char q, char *buf);

extern str XMLepilogue(Client ctx, void *ret);

#endif /* XML_H */
