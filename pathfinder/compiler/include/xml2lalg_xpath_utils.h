/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * 
 * Some Convenience-Wrappers around the libxml2-xpath API 
 *
 *
 * Copyright Notice:
 * -----------------
 *
 * The contents of this file are subject to the Pathfinder Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/PathfinderLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the Pathfinder system.
 *
 * The Original Code has initially been developed by the Database &
 * Information Systems Group at the University of Konstanz, Germany and
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id: xml2lalg_xpath_utils.h,v 1.0 2007/10/31 22:00:00 ts-tum 
 * Exp $ 
 */



#ifndef LIBXML_XPATH_UTILS_H
#define LIBXML_XPATH_UTILS_H

#include "pathfinder.h"


#include <stdio.h>
#ifdef HAVE_STDBOOL_H
    #include <stdbool.h>
#endif



/* SAX parser interface (libxml2) */
#include <libxml/parser.h>
#include <libxml/parserInternals.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>





xmlXPathObjectPtr 
PFxml2la_xpath_evalXPathFromDocCtx(xmlXPathContextPtr xpathCtx, 
                                   const char* xpathExpression);

xmlXPathObjectPtr 
PFxml2la_xpath_evalXPathFromNodeCtx(xmlXPathContextPtr docXPathCtx, 
                     xmlNodePtr nodeXpathCtx, 
                     const char* xpathExpression);


int 
PFxml2la_xpath_getNodeCount(xmlXPathObjectPtr xpathObjPtr);

xmlNodePtr 
PFxml2la_xpath_getNthNode(xmlXPathObjectPtr xpathObjPtr, int n);


char* 
PFxml2la_xpath_getAttributeValueFromElementNode(xmlNodePtr nodePtr, 
                                                const char* attributeName);

char* 
PFxml2la_xpath_getAttributeValueFromAttributeNode(xmlNodePtr nodePtr);

PFarray_t *
PFxml2la_xpath_getAttributeValuesFromAttributeNodes(
    xmlXPathObjectPtr xpathObjectPtr);

char* 
PFxml2la_xpath_getElementValue(xmlNodePtr nodePtr);


int 
PFxml2la_xpath_getIntValue(char* s);

double 
PFxml2la_xpath_getFloatValue(char* s);

bool 
PFxml2la_xpath_getBoolValue(char* s);



#endif

