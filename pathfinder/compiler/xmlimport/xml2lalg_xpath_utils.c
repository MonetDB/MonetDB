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
 * $Id: xml2lalg_xpath_utils.c,v 1.0 2007/10/31 22:00:00 ts-tum 
 * Exp $ 
 */




#include "pathfinder.h"


#include <stdio.h>
#ifdef HAVE_STDBOOL_H
    #include <stdbool.h>
#endif

#include <string.h>
#include <assert.h>
#include <stdlib.h>



/* SAX parser interface (libxml2) */
#include <libxml/parser.h>
#include <libxml/parserInternals.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>

#include "mem.h"
#include "oops.h"

#include "xml2lalg_xpath_utils.h"
 



/******************************************************************************/
/******************************************************************************/

xmlXPathObjectPtr 
PFxml2la_xpath_evalXPathFromDocCtx(xmlXPathContextPtr xpathCtx, 
                                   const char* xpathExpression)
{

    xmlXPathObjectPtr xpathObjPtr = 
        xmlXPathEvalExpression((const xmlChar*)xpathExpression, xpathCtx);
    if (xpathObjPtr == NULL)
    {
        PFoops (OOPS_FATAL,  
                "Error: unable to evaluate xpath expression \"%s\"\n", 
                xpathExpression);
    }

    return xpathObjPtr;
}

/******************************************************************************/
/******************************************************************************/

xmlXPathObjectPtr 
PFxml2la_xpath_evalXPathFromNodeCtx(
    xmlXPathContextPtr docXPathCtx, 
    xmlNodePtr nodeXpathCtx, 
    const char* xpathExpression)
{

    xmlChar* uniqueXPATHPrefix = xmlGetNodePath(nodeXpathCtx);
        
    xmlBufferPtr buffer = xmlBufferCreate();
    xmlBufferAdd(buffer, uniqueXPATHPrefix, xmlStrlen(uniqueXPATHPrefix));
    xmlBufferAdd(buffer, (xmlChar*)xpathExpression, strlen(xpathExpression));

    char* absoluteXPath = (char*) xmlBufferContent(buffer); 

    xmlXPathObjectPtr result =  PFxml2la_xpath_evalXPathFromDocCtx(docXPathCtx, absoluteXPath);

    xmlBufferFree(buffer);

    return result;
}



/******************************************************************************/
/******************************************************************************/


int 
PFxml2la_xpath_getNodeCount(xmlXPathObjectPtr xpathObjPtr)
{
    if (xpathObjPtr)
    {
        xmlNodeSetPtr nodeSetPtr =  xpathObjPtr->nodesetval;

        int size = (nodeSetPtr) ? nodeSetPtr->nodeNr : 0;
        return size;

    }
    else
    {
        return 0;
    }

}

/******************************************************************************/
/******************************************************************************/


xmlNodePtr 
PFxml2la_xpath_getNthNode(xmlXPathObjectPtr xpathObjPtr, int n)
{


    if (xpathObjPtr)
    {

        int nodeCount = 
            PFxml2la_xpath_getNodeCount(xpathObjPtr);
        if (n < nodeCount)
        {

            xmlNodePtr nodePtr = xpathObjPtr->nodesetval->nodeTab[n]; 
            return nodePtr;
            
        }
        else
        {
            return NULL;
        }
    }
    else
    {
        return NULL;
    }


}

/******************************************************************************/
/******************************************************************************/


char* 
PFxml2la_xpath_getAttributeValueFromElementNode(xmlNodePtr nodePtr, 
                                                const char* attributeName) 
{

    struct _xmlAttr * attribute =  nodePtr->properties;

    while (attribute)
    {

        const xmlChar* name = attribute->name;
        
        if (strcmp(attributeName, (const char*)name) == 0)
        {

            const xmlChar* value = attribute->children->content;
            
            return (char*) value;
        }



        attribute = attribute->next;
    }

    return NULL;

}



/******************************************************************************/
/******************************************************************************/


char* 
PFxml2la_xpath_getAttributeValueFromAttributeNode(xmlNodePtr nodePtr)
{
    return(char*)nodePtr->children->content;
}

/******************************************************************************/
/******************************************************************************/


PFarray_t *
PFxml2la_xpath_getAttributeValuesFromAttributeNodes(
    xmlXPathObjectPtr xpathObjectPtr)
{

    PFarray_t * values = PFarray (sizeof (char*));

    int nodeCount = PFxml2la_xpath_getNodeCount(xpathObjectPtr);

    for (int i = 0; i < nodeCount; i++)
    {

        xmlNodePtr nodePtr = PFxml2la_xpath_getNthNode(xpathObjectPtr, i);        
        char* value = PFxml2la_xpath_getAttributeValueFromAttributeNode(nodePtr);
        char* copiedValue = PFstrdup(value);
        *(char**) PFarray_add (values) = copiedValue;
    }             

   
    return values;

}
 

/******************************************************************************/
/******************************************************************************/


char* 
PFxml2la_xpath_getElementValue(xmlNodePtr nodePtr)
{
    return(char*)nodePtr->children->content;
}




/******************************************************************************/
/******************************************************************************/



int 
PFxml2la_xpath_getIntValue(char* s)
{
    return atoi(s);
}


double 
PFxml2la_xpath_getFloatValue(char* s)
{
    return atof(s);
}


bool 
PFxml2la_xpath_getBoolValue(char* s)
{

    if (strcmp(s, "true") == 0)
    {
        return true;
    }
    else if (strcmp(s, "false") == 0)
    {
        return false;
    }
    else
    {
        PFoops (OOPS_FATAL, "expected \"true\" or \"false\", but got (%s)", s);
        /* pacify picky compilers */
        return false;

    }

}


/******************************************************************************/
/******************************************************************************/



