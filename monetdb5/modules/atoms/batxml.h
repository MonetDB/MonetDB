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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
*/
#ifndef _BATXML_H_
#define _BATXML_H_
#include <gdk.h>
#include "ctype.h"
#include <string.h>
#include "mal_interpreter.h"
#include "mal_function.h"
#include "xml.h"


#ifdef WIN32
#ifndef LIBATOMS
#define batxml_export extern __declspec(dllimport)
#else
#define batxml_export extern __declspec(dllexport)
#endif
#else
#define batxml_export extern
#endif

batxml_export str BATXMLxml2str(int *ret, int *bid);
batxml_export str BATXMLxmltext(int *ret, int *bid);
batxml_export str BATXMLstr2xml(int *x, int *s);
batxml_export str BATXMLdocument(int *x, int *s);
batxml_export str BATXMLcontent(int *x, int *s);
batxml_export str BATXMLisdocument(int *x, int *s);
batxml_export str BATXMLelementSmall(int *x, str *name, int *s);
batxml_export str BATXMLoptions(int *x, str *name, str *options, int *s);
batxml_export str BATXMLcomment(int *x, int *s);
batxml_export str BATXMLparse(int *x, str *doccont, int *s, str *option);
batxml_export str BATXMLxquery(int *x, int *s, str *expr);
batxml_export str BATXMLpi(int *x, str *tgt, int *s);
batxml_export str BATXMLroot(int *ret, int *bid, str *version, str *standalone);
batxml_export str BATXMLattribute(int *ret, str *name, int *bid);
batxml_export str BATXMLelement(int *ret, str *name, xml *ns, xml *attr, int *bid);
batxml_export str BATXMLconcat(int *ret, int *bid, int *rid);
batxml_export str BATXMLforest(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
batxml_export str BATXMLagg(int *ret, int *bid, int *grp);
batxml_export str BATXMLagg3(int *ret, int *bid, int *grp, int *e);
batxml_export str BATXMLgroup(xml *ret, int *bid);
#endif /* _BATXML_H_ */
