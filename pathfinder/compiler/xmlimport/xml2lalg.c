/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * 
 * Importing of XML-serialized logical Algebra Plans
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
 * $Id$
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
#include <libxml/relaxng.h>


	

#include "oops.h"
#include "xml2lalg.h"

#include "mem.h"

#include "array.h"


#include "algebra.h"
#include "logical.h"
#include "logical_mnemonic.h"


#include "xml2lalg_xpath_utils.h"
#include "xml2lalg_converters.h"





/* 
  =============================================================================
  =============================================================================
  some convenience macros for a non-verbose xml-data-access/transformation
  =============================================================================
  =============================================================================
*/




/**       
 * ATTENTION:
 * Use this macros only inside the
 * "createAndStoreAlgOpNode"-function or inside a context where 
 * "XML2LALGContext* ctx" and "xmlNodePtr nodePtr" are declared,
 * as this macros are dependent from the existence of this 
 * symbols... 
 */


#define XPATH(xpath) \
    PFxml2la_xpath_evalXPathFromNodeCtx( \
        ctx->docXPathCtx, nodePtr, xpath)

#define NODECOUNT(xpath) \
    PFxml2la_xpath_getNodeCount(XPATH(xpath))


#define CHILDNODE(pos) \
    getChildNode(ctx, nodePtr, pos)


/**              
 * Macro return-type:  PFla_op_kind_t 
 */ 
#define PFLA_OP_KIND \
    getPFLAOpKind(ctx, nodePtr)



/*------------------------------------------------------------------------------
 Macros for a non-verbose usage of the logical algebra node ctors.
 Use this macros for converting xpath-adressed xml-data to pf-data needed by 
 the logical algebra node ctors.
------------------------------------------------------------------------------*/

/**              
 * Macro return-type:  PFalg_att_t 
 * XPath return-type:  attribute(){1}
 */
#define PFLA_ATT(xpath) \
    ctx->convert2PFLA_attributeName( \
        PFxml2la_xpath_getAttributeValueFromAttributeNode( \
            PFxml2la_xpath_getNthNode(XPATH(xpath), 0)))

/**              
 * Macro return-type:  PFalg_att_t 
 * XPath return-type:  attribute()?
 */
#define PFLA_ATT_O(xpath, fallback) \
            getPFLA_OptionalAttribute(ctx, nodePtr, xpath, fallback)

/**              
 * Macro return-type:  PFalg_attlist_t 
 * XPath return-type:  element(column)*
 */
#define PFLA_ATT_LST(xpath) \
            getPFLA_AttributeList(ctx, nodePtr, xpath)

/**              
 * Macro return-type:  PFalg_atom_t 
 * XPath return-type:  element(value){1}
 */
#define PFLA_ATM(xpath) \
            getPFLA_Atom(ctx, nodePtr, xpath)

/**              
 * Macro return-type:  PFalg_tuple_t* 
 * XPath return-type:  element(value)+
 */
#define PFLA_TUPLES(xpath, rowCount, columnCount) \
            getPFLA_Tuples(ctx, nodePtr, xpath, rowCount, columnCount)

/**              
 * Macro return-type:  PFalg_axis_t 
 * XPath return-type:  attribute(){1}
 */
#define PFLA_AXIS(xpath) \
            PFxml2la_conv_2PFLA_xpathaxis( \
                PFxml2la_xpath_getAttributeValueFromAttributeNode( \
                    PFxml2la_xpath_getNthNode(XPATH(xpath), 0)))

/**              
 * Macro return-type:  PFalg_simple_type_t 
 * XPath return-type:  attribute(){1}
 */
#define PFLA_ATMTY(xpath) \
            PFxml2la_conv_2PFLA_atomType( \
                PFxml2la_xpath_getAttributeValueFromAttributeNode( \
                    PFxml2la_xpath_getNthNode(XPATH(xpath), 0)))

/**              
 * Macro return-type:  PFty_t 
 * XPath return-type:  attribute(){1}
 */
#define PFLA_SEQTY(xpath) \
            PFxml2la_conv_2PFLA_sequenceType( \
                PFxml2la_xpath_getAttributeValueFromAttributeNode( \
                    PFxml2la_xpath_getNthNode(XPATH(xpath), 0)))

/**              
 * Macro return-type:  PFalg_fun_t 
 * XPath return-type:  attribute(){1}
 */
#define PFLA_FUNTY(xpath) \
            PFxml2la_conv_2PFLA_functionType( \
                PFxml2la_xpath_getAttributeValueFromAttributeNode( \
                    PFxml2la_xpath_getNthNode(XPATH(xpath), 0)))

/**              
 * Macro return-type:  PFalg_doc_t 
 * XPath return-type:  attribute(){1}
 */
#define PFLA_DOCTY(xpath) \
                PFxml2la_conv_2PFLA_docType( \
                    PFxml2la_xpath_getAttributeValueFromAttributeNode( \
                        PFxml2la_xpath_getNthNode(XPATH(xpath), 0)))

/**              
 * Macro return-type:  PFord_ordering_t 
 * XPath return-type:  element()+
 */
#define PFLA_ORDERING(xpath) \
                    getPFLA_Ordering(ctx, nodePtr, xpath)

/**              
 * Macro return-type:  PFalg_proj_t* 
 * XPath return-type:  element()+
 */
#define PFLA_PROJECTION(xpath) \
                    getPFLA_Projection(ctx, nodePtr, xpath)

/**              
 * Macro return-type:  PFalg_sel_t* 
 * XPath return-type:  element(comparison)+
 */
#define PFLA_PREDICATES(xpath) \
                    getPFLA_Predicates(ctx, nodePtr, xpath)


/**              
 * Macro return-type:  int 
 * XPath return-type:  attribute()?
 */
#define A2INT_O(xpath, fallback) \
   getOptionalAttributeValueAsInt(ctx, nodePtr, xpath, fallback)

/**              
 * Macro return-type:  int 
 * XPath return-type:  element()?
 */
#define E2INT(xpath) \
   PFxml2la_xpath_getIntValue( \
       PFxml2la_xpath_getElementValue( \
           PFxml2la_xpath_getNthNode(XPATH(xpath), 0)))

/**              
 * Macro return-type:  char* 
 * XPath return-type:  element()?
 */
#define E2STR(xpath) \
  PFxml2la_xpath_getElementValue(PFxml2la_xpath_getNthNode(XPATH(xpath), 0))



/* 
  =============================================================================
  =============================================================================
  declarations of internal auxiliary-functions
  (definitions are at the end of this file)  
  =============================================================================
  =============================================================================
*/


/******************************************************************************/
/******************************************************************************/

PFla_op_t* 
importXML(XML2LALGContext* ctx, xmlDocPtr doc);

void 
createAndStoreAlgOpNode(XML2LALGContext* ctx, xmlNodePtr nodePtr);

/******************************************************************************/
/******************************************************************************/

PFla_op_t* 
lookupAlgNode(XML2LALGContext* ctx, int nodeID);

void 
storeAlgNode(XML2LALGContext* ctx, PFla_op_t* node, int nodeID);

/******************************************************************************/
/******************************************************************************/

int 
getOptionalAttributeValueAsInt(
  XML2LALGContext* ctx, 
  xmlNodePtr nodePtr, 
  const char* xpathExpression, 
  int fallback);

/******************************************************************************/
/******************************************************************************/

int 
getNodeID(XML2LALGContext* ctx, xmlNodePtr nodePtr);

PFla_op_t* 
getChildNode(XML2LALGContext* ctx, xmlNodePtr nodePtr, int pos);

PFla_op_t* 
getRootNode(XML2LALGContext* ctx); 

/******************************************************************************/
/******************************************************************************/

PFla_op_kind_t 
getPFLAOpKind(XML2LALGContext* ctx, xmlNodePtr nodePtr);

PFalg_att_t 
getPFLA_OptionalAttribute(
    XML2LALGContext* ctx, 
    xmlNodePtr nodePtr, 
    const char* xpathExpression, 
    PFalg_att_t fallback);

PFalg_atom_t 
getPFLA_Atom(
    XML2LALGContext* ctx, 
    xmlNodePtr nodePtr, 
    const char* xpathExpression);

PFalg_attlist_t 
getPFLA_AttributeList(
    XML2LALGContext* ctx, 
    xmlNodePtr nodePtr, 
    const char* xpathExpression);

PFalg_tuple_t* 
getPFLA_Tuples(
    XML2LALGContext* ctx, 
    xmlNodePtr nodePtr, 
    const char* xpathExpression, 
    int rowCount, 
    int columnCount);

PFalg_sel_t* 
getPFLA_Predicates(
    XML2LALGContext* ctx, 
    xmlNodePtr nodePtr, 
    const char* xpathExpression);

PFalg_proj_t* 
getPFLA_Projection(
    XML2LALGContext* ctx, 
    xmlNodePtr nodePtr, 
    const char* xpathExpression);

PFord_ordering_t 
getPFLA_Ordering(
    XML2LALGContext* ctx, 
    xmlNodePtr nodePtr, 
    const char* xpathExpression);

/******************************************************************************/
/******************************************************************************/


void 
validateByRELAXNG(xmlDocPtr doc);

/******************************************************************************/
/******************************************************************************/

void 
info1(XML2LALGContext* ctx, xmlNodePtr nodePtr);



/* 
  =============================================================================
  =============================================================================
  implementation of the XML2ALG interface
  =============================================================================
  =============================================================================
*/






XML2LALGContext* 
PFxml2la_xml2lalgContext() 
{

    XML2LALGContext* ctx = (XML2LALGContext*) PFmalloc (sizeof (XML2LALGContext));

    ctx->nodeStore = PFarray(sizeof (PFla_op_t*));

    return ctx;  
}



PFla_op_t* 
PFxml2la_importXMLFromFile(XML2LALGContext* ctx, const char* filename)
{

    xmlDocPtr doc = xmlParseFile(filename);
    if (!doc)
    {
        PFoops (OOPS_FATAL,  "unable to parse file \"%s\"", filename);
    }


    /* validate the document against a schema*/
    /*
    validateByRELAXNG(doc);
    */

    return importXML(ctx, doc);

}



PFla_op_t* 
PFxml2la_importXMLFromMemory(XML2LALGContext* ctx, const char* xml, int size)
{

    /* Load XML document */
    xmlDocPtr doc = xmlParseMemory(xml, size);
    if (!doc)
    {
        PFoops (OOPS_FATAL,  "unable to parse xml (%s)", xml);
    }

    /* validate the document against a schema*/
    /*
    validateByRELAXNG(doc);
    */


    return importXML(ctx, doc);
}



/* 
  =============================================================================
  =============================================================================
  Driver of the central createAndStoreAlgOpNode function  
  =============================================================================
  =============================================================================
*/



PFla_op_t* 
importXML(XML2LALGContext* ctx, xmlDocPtr doc)
{

        /* Create xpath evaluation context */
    xmlXPathContextPtr docXPathCtx = xmlXPathNewContext(doc);
    if (!docXPathCtx)
    {
        PFoops (OOPS_FATAL,  "unable to create a XPath context");
    }


    /******************************************************************************/
    ctx->docXPathCtx = docXPathCtx;

    /**
     * dependent from the value of the unique_names-atrribute of the
     * logical_query_plan-element we chose the appropriate function
     * for converting strings to PF-Attribute-Names
     */
    bool uniqueNames = 
        PFxml2la_xpath_getBoolValue(
            PFxml2la_xpath_getAttributeValueFromAttributeNode(
                PFxml2la_xpath_getNthNode(
                    PFxml2la_xpath_evalXPathFromDocCtx(
                            docXPathCtx, "/logical_query_plan/@unique_names"), 0)));
    if (uniqueNames)
    {
        ctx->convert2PFLA_attributeName = PFxml2la_conv_2PFLA_attributeName_unq;
    }
    else
    {
        ctx->convert2PFLA_attributeName = PFxml2la_conv_2PFLA_attributeName;
    }
    /******************************************************************************/


    /* fetch the serialized algebra nodes from xml */
    xmlXPathObjectPtr xpathObjPtr = 
        PFxml2la_xpath_evalXPathFromDocCtx(docXPathCtx, "//node");

    if (xpathObjPtr)
    {

        /**       
         * Bottom-up processing of the serialized algebra nodes: 
         * The nodes are passed to the central processing function in
         * document-order, which must reflect the 
         * "bottom-up"-order of the algebra dag.         
         */   
        xmlNodeSetPtr nodeSetPtr =  xpathObjPtr->nodesetval;
        int nodeCount = (nodeSetPtr) ? nodeSetPtr->nodeNr : 0;
        for (int i = 0; i < nodeCount; i++)
        {

            xmlNodePtr nodePtr = nodeSetPtr->nodeTab[i];   
            /* pass the node to the central processing function */
            createAndStoreAlgOpNode(ctx, nodePtr);


        }

        PFla_op_t* rootNode = getRootNode(ctx);

        /*
        printf("Importing XML DONE\n");
        */
        
        if(xpathObjPtr)
            xmlXPathFreeObject(xpathObjPtr);
        /*
        if(nodeSetPtr) 
            xmlXPathFreeNodeSet(nodeSetPtr);
        */

        if(docXPathCtx)
            xmlXPathFreeContext(docXPathCtx);	


        return rootNode;

    }

    return NULL;

}



/* 
  =============================================================================
  =============================================================================
  The central createAndStoreAlgOpNode function: 
  - construction of the algebra dag from the xml-serialized nodes 
  =============================================================================
  =============================================================================
*/


/**
 * This is the core of this source file. 
 * This function constructs the logical algebra DAG in a 
 * bottom-up-fashion by successive translation and integration 
 * of single XML-nodes (representing single algebra operator 
 * nodes) 
 */
void createAndStoreAlgOpNode(XML2LALGContext* ctx, xmlNodePtr nodePtr)
{

   /*
   info1(ctx, nodePtr);
   */



    /**
     * the logical algebra node which must be constructed from the
     * currently to be processed xml node
     */
    PFla_op_t * newAlgNode = NULL;

    /**
     * which logical algebra node does the currently to be processed 
     * xml node represent? 
     */
    PFla_op_kind_t  algOpKindID  = PFLA_OP_KIND;

    /**
     * let's construct an logical algebra node from the currently to
     * be processed xml node 
     */
    switch (algOpKindID)
    {

/******************************************************************************/
/******************************************************************************/
    
    case la_serialize_seq            : 

        {
            /*
             <content>
               <column name="COLNAME" new="false" function="pos"/>
               <column name="COLNAME" new="false" function="item"/>
             </content>
            */

            newAlgNode = PFla_serialize_seq 
             (
             CHILDNODE(0), CHILDNODE(1),
             PFLA_ATT("/content/column[@new='false' and @function='pos']/@name"), 
             PFLA_ATT("/content/column[@new='false' and @function='item']/@name")
             );


        }  
        break;

/******************************************************************************/
/******************************************************************************/
    
    case la_serialize_rel            : 

        {
            /*
             <content>
               <column name="COLNAME" new="false" function="iter"/>
               <column name="COLNAME" new="false" function="pos"/>
              (<column name="COLNAME" new="false" function="item" position="[0..n]"/>)+
             </content>
            */

            newAlgNode = PFla_serialize_rel 
             (
             CHILDNODE(0),
             PFLA_ATT("/content/column[@new='false' and @function='iter']/@name"), 
             PFLA_ATT("/content/column[@new='false' and @function='pos']/@name"),
             PFLA_ATT_LST("/content/column[@new='false' and @function='item']")
             );
                                      
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_lit_tbl              : 

        {
            /*
             <content>
               (<column name="COLNAME" new="true"/>
                 (<value type="DATATYPE">VALUE</value>)+
                </column>)+
             </content>
             */

            /* how many columns does the literal table have? */
            int columnCount =  NODECOUNT("/content/column[@new='true']");
            /* how many atomns does the literal table have !in total!? */
            int atomnCount =   NODECOUNT("/content/column[@new='true']/value");
            /* how many rows (= tuples) does the literal table have?
             - as each column of the table *must* have the same
               amount of atoms, we simply can divide the atomnCount by the
               columnCount in order to get the rowCount (= tuple count) */
            int rowCount =  atomnCount / columnCount; 

            newAlgNode = PFla_lit_tbl_ 
             (
             PFLA_ATT_LST("/content/column[@new='true']"), 
             rowCount, 
             PFLA_TUPLES("/content/column[@new='true']/value", rowCount, columnCount)
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_empty_tbl            : 

        {
            /*
            <content>
              (<column name="COLNAME" new="true"/>)*
            </content>            
            */

            newAlgNode = PFla_empty_tbl 
             (
             PFLA_ATT_LST("/content/column[@new='true']")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_attach               : 

        {
            /*
            <content>
               <column name="COLNAME" new="true">
                 <value type="DATATYPE">VALUE</value>
               </column>
             </content>            
            */

            newAlgNode = PFla_attach 
             (
             CHILDNODE(0), 
             PFLA_ATT("/content/column[@new='true']/@name"), 
             PFLA_ATM("/content/column/value")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_cross                : 

        {
            newAlgNode = PFla_cross(CHILDNODE(0), CHILDNODE(1));
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_eqjoin               : 

        {
            /*
             <content>
               <column name="COLNAME" new="false" position="1"/>
               <column name="COLNAME" new="false" position="2"/>
             </content>            
            */

            newAlgNode = PFla_eqjoin 
             (
             CHILDNODE(0), CHILDNODE(1), 
             PFLA_ATT("/content/column[@new='false' and @position='1']/@name"), 
             PFLA_ATT("/content/column[@new='false' and @position='2']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_semijoin             : 

        {
            /*
            <content>
               <column name="COLNAME" new="false" position="1"/>
               <column name="COLNAME" new="false" position="2"/>
            </content>             
            */

            newAlgNode = PFla_semijoin 
             (
             CHILDNODE(0), CHILDNODE(1), 
             PFLA_ATT("/content/column[@new='false' and @position='1']/@name"), 
             PFLA_ATT("/content/column[@new='false' and @position='2']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_thetajoin            : 

        {
            /*
              <content>
               (<comparison kind="KIND">
                  <column name="COLNAME" new="false" position="1"/>
                  <column name="COLNAME" new="false" position="2"/>
                </comparison>)*
             </content>             
            */       
            
            newAlgNode = PFla_thetajoin 
             (
             CHILDNODE(0), CHILDNODE(1), 
             NODECOUNT("/content/comparison"), 
             PFLA_PREDICATES("/content/comparison")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_project              : 

        {
            /*
            <content>
             (<column name="COLNAME" old_name="COLNAME" new="true"/>
            | <column name="COLNAME" new="false"/>)*
            </content>
            */

            newAlgNode = PFla_project_ 
             (
             CHILDNODE(0), 
             NODECOUNT("/content/column"),
             PFLA_PROJECTION("/content/column")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_select               : 

        {
            /*
            <content>
               <column name="COLNAME" new="false"/>
             </content>
            */

            newAlgNode = PFla_select 
             (
             CHILDNODE(0), 
             PFLA_ATT("/content/column[@new='false']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_pos_select           : 

        {
            /*
            <content>
               <position>POSITION</position>
              (<column name="COLNAME" function="sort" position="[0..n]" direction="DIRECTION" new="false"/>)+
              (<column name="COLNAME" function="partition" new="false"/>)?
            </content>
            */

            newAlgNode = PFla_pos_select
             (
             CHILDNODE(0), 
             E2INT("/content/position"), 
             PFLA_ORDERING("/content/column[@function='sort']"), 
             PFLA_ATT_O("/content/column[@function='partition']/@name", -1)
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_disjunion            :   

        {
            newAlgNode = PFla_disjunion(CHILDNODE(0), CHILDNODE(1));
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_intersect            : 

        {
            newAlgNode = PFla_intersect(CHILDNODE(0), CHILDNODE(1));
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_difference           : 

        {
            newAlgNode = PFla_difference(CHILDNODE(0), CHILDNODE(1));
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_distinct             : 

        {
            newAlgNode = PFla_distinct(CHILDNODE(0));
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_fun_1to1             : 

        {
            /*
            <content>
               <kind name="FUNCTION"/>
               <column name="COLNAME" new="true"/>
              (<column name="COLNAME" new="false" position="[0..n]"/>)*
             </content>
             */

            newAlgNode = PFla_fun_1to1 
             (
             CHILDNODE(0),
             PFLA_FUNTY("/content/kind/@name"), 
             PFLA_ATT("/content/column[@new='true']/@name"),  
             PFLA_ATT_LST("/content/column[@new='false']")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_num_eq               : 

        {
            /*
           <content>
              <column name="COLNAME" new="true"/>
              <column name="COLNAME" new="false" position="1"/>
              <column name="COLNAME" new="false" position="2"/>
           </content>
           */

            newAlgNode = PFla_eq 
             (
             CHILDNODE(0), 
             PFLA_ATT("/content/column[@new='true']/@name"), 
             PFLA_ATT("/content/column[@new='false' and @position='1']/@name"), 
             PFLA_ATT("/content/column[@new='false' and @position='2']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_num_gt               : 

        {
            /*
           <content>
              <column name="COLNAME" new="true"/>
              <column name="COLNAME" new="false" position="1"/>
              <column name="COLNAME" new="false" position="2"/>
           </content>
           */

            newAlgNode = PFla_gt 
             (
             CHILDNODE(0), 
             PFLA_ATT("/content/column[@new='true']/@name"), 
             PFLA_ATT("/content/column[@new='false' and @position='1']/@name"), 
             PFLA_ATT("/content/column[@new='false' and @position='2']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_bool_and             : 

        {
            /*
           <content>
              <column name="COLNAME" new="true"/>
              <column name="COLNAME" new="false" position="1"/>
              <column name="COLNAME" new="false" position="2"/>
           </content>
           */

            newAlgNode = PFla_and 
             (
             CHILDNODE(0), 
             PFLA_ATT("/content/column[@new='true']/@name"), 
             PFLA_ATT("/content/column[@new='false' and @position='1']/@name"), 
             PFLA_ATT("/content/column[@new='false' and @position='2']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_bool_or              : 

        {
            /*
            <content>
               <column name="COLNAME" new="true"/>
               <column name="COLNAME" new="false" position="1"/>
               <column name="COLNAME" new="false" position="2"/>
            </content>
            */

            newAlgNode = PFla_or 
             (
             CHILDNODE(0), 
             PFLA_ATT("/content/column[@new='true']/@name"), 
             PFLA_ATT("/content/column[@new='false' and @position='1']/@name"), 
             PFLA_ATT("/content/column[@new='false' and @position='2']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_bool_not             : 

        {
            /*
            <content>
               <column name="COLNAME" new="true"/>
               <column name="COLNAME" new="false"/>
             </content>
           */
            newAlgNode = PFla_not 
             (
             CHILDNODE(0), 
             PFLA_ATT("/content/column[@new='true']/@name"), 
             PFLA_ATT("/content/column[@new='false']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_to                   : 

        {
            /*
            <content>
               <column name="COLNAME" new="true"/>
               <column name="COLNAME" new="false" function="start"/>
               <column name="COLNAME" new="false" function="end"/>
              (<column name="COLNAME" new="false" function="partition"/>)?
           </content>
           */

            newAlgNode = PFla_to 
             (
             CHILDNODE(0), 
             PFLA_ATT("/content/column[@new='true']/@name"), 
             PFLA_ATT("/content/column[@function='start']/@name"), 
             PFLA_ATT("/content/column[@function='end']/@name"), 
             PFLA_ATT_O("/content/column[@function='partition']/@name", -1)
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_avg                  : 
    case la_max                  :
    case la_min                  :
    case la_sum                  :
        {
            /*
           <content>
              <column name="COLNAME" new="true"/>
              <column name="COLNAME" new="false" function="item"/>
             (<column name="COLNAME" function="partition" new="false"/>)?
           </content>
           */

            newAlgNode = PFla_aggr 
             (
             algOpKindID, 
             CHILDNODE(0), 
             PFLA_ATT("/content/column[@new='true']/@name"), 
             PFLA_ATT("/content/column[@function='item']/@name"), 
             PFLA_ATT_O("/content/column[@function='partition']/@name", -1)
             );
        }  
        break;                       

/******************************************************************************/
/******************************************************************************/

    case la_count                : 

        {
            /*
            <content>
               <column name="COLNAME" new="true"/>
              (<column name="COLNAME" function="partition" new="false"/>)?
            </content>
            */

            newAlgNode = PFla_count
             (
             CHILDNODE(0), 
             PFLA_ATT("/content/column[@new='true']/@name"), 
             PFLA_ATT_O("/content/column[@function='partition']/@name", -1)
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_rownum               : 

        {
            /*
            <content>
               <column name="COLNAME" new="true"/>
              (<column name="COLNAME" function="sort" position="[0..n]" direction="DIRECTION" new="false"/>)+
              (<column name="COLNAME" function="partition" new="false"/>)?
            </content>
            */

            newAlgNode = PFla_rownum
             (
             CHILDNODE(0), 
             PFLA_ATT("/content/column[@new='true']/@name"), 
             PFLA_ORDERING("/content/column[@function='sort']"), 
             PFLA_ATT_O("/content/column[@function='partition']/@name", -1)
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_rank                 : 

        {
            /*
            <content>
               <column name="COLNAME" new="true"/>
              (<column name="COLNAME" function="sort" position="[0..n]" direction="DIRECTION" new="false"/>)+
            </content>
            */

            newAlgNode = PFla_rank
             (
             CHILDNODE(0), 
             PFLA_ATT("/content/column[@new='true']/@name"), 
             PFLA_ORDERING("/content/column[@function='sort']")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_number               : 

        {
            /*
             <content>
               <column name="COLNAME" new="true"/>
             </content>
            */

            newAlgNode = PFla_number
             (
             CHILDNODE(0), 
             PFLA_ATT("/content/column[@new='true']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_type                 : 

        {
            /*
             <content>
               <column name="COLNAME" new="true"/>
               <column name="COLNAME" new="false"/>
               <type name="DATATYPE"/>
             </content>
            */

            newAlgNode = PFla_type
             (
             CHILDNODE(0), 
             PFLA_ATT("/content/column[@new='true']/@name"), 
             PFLA_ATT("/content/column[@new='false']/@name"), 
             PFLA_ATMTY("/content/type/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_type_assert          : 

        {
            /*
             <content>
               <column name="COLNAME" new="false"/>
               <type name="DATATYPE"/>
             </content>
            */

            newAlgNode = PFla_type_assert
             (
             CHILDNODE(0), 
             PFLA_ATT("/content/column[@new='false']/@name"), 
             PFLA_ATMTY("/content/type/@name"), 
             true
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_cast                 : 

        {
            /*
            <content>
               <column name="COLNAME" new="true"/>
               <column name="COLNAME" new="false"/>
               <type name="DATATYPE"/>
            </content>
            */

            newAlgNode = PFla_cast
             (
             CHILDNODE(0), 
             PFLA_ATT("/content/column[@new='true']/@name"), 
             PFLA_ATT("/content/column[@new='false']/@name"), 
             PFLA_ATMTY("/content/type/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_seqty1               : 

        {

            /*
            <content>
               <column name="COLNAME" new="true"/>
               <column name="COLNAME" new="false" function="item"/>
              (<column name="COLNAME" function="partition" new="false"/>)?
            </content>
            */

            newAlgNode = PFla_seqty1 
             (
             CHILDNODE(0), 
             PFLA_ATT("/content/column[@new='true']/@name"), 
             PFLA_ATT("/content/column[@function='item']/@name"), 
             PFLA_ATT_O("/content/column[@function='partition']/@name", -1)
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_all                  : 

        {
            /*
            <content>
               <column name="COLNAME" new="true"/>
               <column name="COLNAME" new="false" function="item"/>
              (<column name="COLNAME" function="partition" new="false"/>)?
            </content>
            */

            newAlgNode = PFla_all 
             (
             CHILDNODE(0), 
             PFLA_ATT("/content/column[@new='true']/@name"), 
             PFLA_ATT("/content/column[@function='item']/@name"), 
             PFLA_ATT_O("/content/column[@function='partition']/@name", -1)
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_step                 : 

        {
            /*
            <content>
               <step axis="AXIS" type="NODE TEST" (level="LEVEL")? />
               <column name="COLNAME" new="true"/>
               <column name="COLNAME" function="iter"/>
               <column name="COLNAME" function="item"/>
            </content>
            */

            newAlgNode = PFla_step
             (
             CHILDNODE(0), CHILDNODE(1), 
             PFLA_AXIS("/content/step/@axis"), 
             PFLA_SEQTY("/content/step/@type"), 
             A2INT_O("/content/step/@level", -1), 
             PFLA_ATT("/content/column[@function='iter']/@name"), 
             PFLA_ATT("/content/column[@function='item']/@name"), 
             PFLA_ATT("/content/column[@new='true']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_step_join            : 

        {
            /*
             <content>
               <step axis="AXIS" type="NODE TEST" (level="LEVEL")?/>
               <column name="COLNAME" new="true"/>
               <column name="COLNAME" function="item"/>
             </content>
            */

            newAlgNode = PFla_step_join
             (
             CHILDNODE(0), CHILDNODE(1), 
             PFLA_AXIS("/content/step/@axis"), 
             PFLA_SEQTY("/content/step/@type"), 
             A2INT_O("/content/step/@level", -1), 
             PFLA_ATT("/content/column[@function='item']/@name"), 
             PFLA_ATT("/content/column[@new='true']/@name")
             );
        }  
        break;


/******************************************************************************/
/******************************************************************************/
        /*
         todo: implement
         needed: "Guide-File address" as global param in the query plan, tokenizing, etc.
        */
    case la_guide_step           : 

        {
            /*
            <content>
               <step axis="AXIS" type="NODE TEST" guide="GUIDELIST"/>
               <column name="COLNAME" function="iter"/>
               <column name="COLNAME" function="item"/>
            </content>
            */


           PFoops (OOPS_FATAL, "Importing of la_guide_step operator is not implemented yet");

        }  
        break;

/******************************************************************************/
/******************************************************************************/
        /*
        todo: implement, see la_guide_step
        */
    case la_guide_step_join      : 

        {
            /*
            <content>
               <step axis="AXIS" type="NODE TEST" guide="GUIDELIST"/>
               <column name="COLNAME" new="true"/>
               <column name="COLNAME" function="item"/>
            </content>
            */

            PFoops (OOPS_FATAL, "Importing of la_guide_step_join operator is not implemented yet");


        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_doc_index_join                :

        {
            PFoops (OOPS_FATAL, "Importing of la_doc_index_join operator is not implemented yet");
        }
        break;

/******************************************************************************/
/******************************************************************************/

    case la_doc_tbl              : 

        {
            /*
            <content>
               <column name="COLNAME" new="true"/>
               <column name="COLNAME" function="iter"/>
               <column name="COLNAME" function="item"/>
            </content>
            */

            newAlgNode = PFla_doc_tbl
             (
             CHILDNODE(0),
             PFLA_ATT("/content/column[@function='iter']/@name"),
             PFLA_ATT("/content/column[@function='item']/@name"),
             PFLA_ATT("/content/column[@new='true']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_doc_access           : 

        {
            /*
            <content>
               <column name="COLNAME" new="true"/>
               <column name="COLNAME" new="false"/>
               <column name="DOCTYPE" new="false" function="document column" />
            </content>

            */

            newAlgNode = PFla_doc_access
             (
             CHILDNODE(0), CHILDNODE(1),
             PFLA_ATT("/content/column[@new='true']/@name"),
             PFLA_ATT("/content/column[@new='false' and not(@function)]/@name"),
             PFLA_DOCTY("/content/column[@new='false' and @function='document column']/@name")
             );
        }  
        break;


/******************************************************************************/
/******************************************************************************/

    case la_twig                 : 

        {
            /*
             <content>
               <column name="COLNAME" function="iter"/>
               <column name="COLNAME" function="item"/>
             </content>
            */

            newAlgNode = PFla_twig
             (
             CHILDNODE(0),
             PFLA_ATT("/content/column[@function='iter']/@name"),
             PFLA_ATT("/content/column[@function='item']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_fcns                 : 

        {
            newAlgNode = PFla_fcns(CHILDNODE(0), CHILDNODE(1));
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_docnode              : 

        {
            /*
             <content>
               <column name="COLNAME" function="iter"/>
             </content>
            */

            newAlgNode = PFla_docnode
             (
             CHILDNODE(0), CHILDNODE(1),
             PFLA_ATT("/content/column[@function='iter']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_element              : 

        {
            /*
             <content>
               <column name="COLNAME" function="iter"/>
               <column name="COLNAME" function="item"/>
             </content>
            */

            newAlgNode = PFla_element
             (
             CHILDNODE(0), CHILDNODE(1),
             PFLA_ATT("/content/column[@function='iter']/@name"),
             PFLA_ATT("/content/column[@function='item']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_attribute            : 

        {
            /*
             <content>
               <column name="COLNAME" function="iter"/>
               <column name="COLNAME" function="qname item"/>
               <column name="COLNAME" function="content item"/>
             </content>
            */

            newAlgNode = PFla_attribute
             (
             CHILDNODE(0),
             PFLA_ATT("/content/column[@function='iter']/@name"),
             PFLA_ATT("/content/column[@function='qname item']/@name"),
             PFLA_ATT("/content/column[@function='content item']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_textnode             : 

        {
            /*
             <content>
               <column name="COLNAME" function="iter"/>
               <column name="COLNAME" function="item"/>
             </content>
            */

            newAlgNode = PFla_textnode
             (
             CHILDNODE(0),
             PFLA_ATT("/content/column[@function='iter']/@name"),
             PFLA_ATT("/content/column[@function='item']/@name")
             );
        }  

        break;

/******************************************************************************/
/******************************************************************************/

    case la_comment              : 

        {
            /*
             <content>
               <column name="COLNAME" function="iter"/>
               <column name="COLNAME" function="item"/>
             </content>
            */

            newAlgNode = PFla_comment
             (
             CHILDNODE(0),
             PFLA_ATT("/content/column[@function='iter']/@name"),
             PFLA_ATT("/content/column[@function='item']/@name")
             );
        }  

        break;

/******************************************************************************/
/******************************************************************************/

    case la_processi             : 

        {
            /*
             <content>
               <column name="COLNAME" function="iter"/>
               <column name="COLNAME" function="target item"/>
               <column name="COLNAME" function="value item"/>
             </content>
            */

            newAlgNode = PFla_processi
             (
             CHILDNODE(0),
             PFLA_ATT("/content/column[@function='iter']/@name"),
             PFLA_ATT("/content/column[@function='target item']/@name"),
             PFLA_ATT("/content/column[@function='value item']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_content              : 

        {
            /*
             <content>
               <column name="COLNAME" function="iter"/>
               <column name="COLNAME" function="pos"/>
               <column name="COLNAME" function="item"/>
             </content>
            */

            newAlgNode = PFla_content
             (
             CHILDNODE(0), CHILDNODE(1),
             PFLA_ATT("/content/column[@function='iter']/@name"),
             PFLA_ATT("/content/column[@function='pos']/@name"),
             PFLA_ATT("/content/column[@function='item']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/
    case la_merge_adjacent       : 

        {
            /*
             <content>
               <column name="COLNAME" function="iter"/>
               <column name="COLNAME" function="pos"/>
               <column name="COLNAME" function="item"/>
             </content>
            */

            newAlgNode = PFla_pf_merge_adjacent_text_nodes
             (
             CHILDNODE(0), CHILDNODE(1),
             PFLA_ATT("/content/column[@function='iter']/@name"),
             PFLA_ATT("/content/column[@function='pos']/@name"),
             PFLA_ATT("/content/column[@function='item']/@name"),
             PFLA_ATT("/content/column[@function='iter']/@name"),
             PFLA_ATT("/content/column[@function='pos']/@name"),
             PFLA_ATT("/content/column[@function='item']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_roots                : 

        {
            newAlgNode = PFla_roots(CHILDNODE(0));
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_fragment             : 

        {
            newAlgNode = PFla_fragment(CHILDNODE(0));
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_frag_union           : 

        {
            newAlgNode = PFla_frag_union(CHILDNODE(0), CHILDNODE(1));
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_empty_frag           : 

        {
            newAlgNode = PFla_empty_frag();
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_cond_err             : 

        {
            /*
             <content>
               <column name="COLNAME" new="false"/>
               <error>ERROR</error>
             </content>
            */

            newAlgNode = PFla_cond_err
             (
             CHILDNODE(0), CHILDNODE(1),
             PFLA_ATT("/content/column[@new='false']/@name"),
             E2STR("/content/error")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_nil                  : 

        {
            newAlgNode = PFla_nil();
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_trace                : 

        {
            /*
             <content>
               <column name="COLNAME" function="iter"/>
               <column name="COLNAME" function="pos"/>
               <column name="COLNAME" function="item"/>
             </content>
            */

            newAlgNode = PFla_trace
             (
             CHILDNODE(0), CHILDNODE(1),
             PFLA_ATT("/content/column[@function='iter']/@name"),
             PFLA_ATT("/content/column[@function='pos']/@name"),
             PFLA_ATT("/content/column[@function='item']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_trace_msg            : 

        {
            /*
             <content>
               <column name="COLNAME" function="iter"/>
               <column name="COLNAME" function="item"/>
             </content>
            */

            newAlgNode = PFla_trace_msg
             (
             CHILDNODE(0), CHILDNODE(1),
             PFLA_ATT("/content/column[@function='iter']/@name"),
             PFLA_ATT("/content/column[@function='item']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_trace_map            : 

        {
            /*
             <content>
               <column name="COLNAME" function="inner"/>
               <column name="COLNAME" function="outer"/>
             </content>
            */

            newAlgNode = PFla_trace_map
             (
             CHILDNODE(0), CHILDNODE(1),
             PFLA_ATT("/content/column[@function='iner']/@name"),
             PFLA_ATT("/content/column[@function='outer']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_rec_fix              : 

        {

            PFoops (OOPS_FATAL, "Importing of la_rec_fix operator is not implemented yet");
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_rec_param            : 

        {
            PFoops (OOPS_FATAL, "Importing of la_rec_param operator is not implemented yet");
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_rec_arg              : 

        {
            PFoops (OOPS_FATAL, "Importing of la_rec_arg operator is not implemented yet");
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_rec_base             : 

        {
            PFoops (OOPS_FATAL, "Importing of la_rec_base operator is not implemented yet");
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_proxy                : 

        {
            PFoops (OOPS_FATAL, "Importing of la_proxy operator is not implemented yet");
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_proxy_base           : 

        {
            newAlgNode = PFla_proxy_base(CHILDNODE(0));
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_cross_mvd            :
        {
            newAlgNode = PFla_cross_clone(CHILDNODE(0), CHILDNODE(1));
        }
        break;


/******************************************************************************/
/******************************************************************************/

    case la_eqjoin_unq           : 

        {
            /*
             <content>
               <column name="COLNAME" new="false" keep="BOOL" position="1"/>
               <column name="COLNAME" new="false" keep="BOOL" position="2"/>
             </content>
            */

            newAlgNode = PFla_eqjoin_clone            
             (
             CHILDNODE(0), CHILDNODE(1),
             PFLA_ATT("/content/column[@new='false' and @position='1']/@name"),
             PFLA_ATT("/content/column[@new='false' and @position='2']/@name"),
             PFLA_ATT("/content/column[@new='false' and @keep='true']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_string_join          : 

        {   
            /*
             <content>
               <column name="COLNAME" function"iter"/>
               <column name="COLNAME" function"pos"/>
               <column name="COLNAME" function"item"/>
             </content>
            */

            newAlgNode = PFla_fn_string_join
             (
             CHILDNODE(0), CHILDNODE(1),
             PFLA_ATT("/content/column[@function='iter']/@name"),
             PFLA_ATT("/content/column[@function='pos']/@name"),
             PFLA_ATT("/content/column[@function='item']/@name"),
             PFLA_ATT("/content/column[@function='iter']/@name"),            
             PFLA_ATT("/content/column[@function='item']/@name"),
             PFLA_ATT("/content/column[@function='iter']/@name"),
             PFLA_ATT("/content/column[@function='item']/@name")
             );
        }  
        break;

/******************************************************************************/
/******************************************************************************/

    case la_dummy                : 

        {

            newAlgNode = PFla_dummy(CHILDNODE(0));
        }  
        break;

/******************************************************************************/
/******************************************************************************/



    }



    if (!newAlgNode)
    {
        PFoops (OOPS_FATAL, "could not generate a node");
    }

    /* store the new logical algebra Node in the node store  */
    storeAlgNode(ctx, newAlgNode, getNodeID(ctx, nodePtr));


}




/* 
  =============================================================================
  =============================================================================
  auxiliary-functions
  =============================================================================
  =============================================================================
*/



/******************************************************************************/
/******************************************************************************/

PFla_op_t* 
lookupAlgNode(XML2LALGContext* ctx, int nodeID)
{

    return  * ((PFla_op_t**) (PFarray_at (ctx->nodeStore, nodeID)));
}


void 
storeAlgNode(XML2LALGContext* ctx, PFla_op_t* node, int nodeID)
{
    *((PFla_op_t**)PFarray_at(ctx->nodeStore, nodeID)) = node;
}


/******************************************************************************/
/******************************************************************************/

int 
getOptionalAttributeValueAsInt(
    XML2LALGContext* ctx, xmlNodePtr nodePtr, 
    const char* xpathExpression, 
    int fallback)
{

    int value = fallback;

    xmlNodePtr xml_att =  PFxml2la_xpath_getNthNode(XPATH(xpathExpression), 0);
    if (xml_att)
    {
        value =  PFxml2la_xpath_getIntValue(
            PFxml2la_xpath_getAttributeValueFromAttributeNode(xml_att));

    }

    return  value;  
}

/******************************************************************************/
/******************************************************************************/
int 
getNodeID(XML2LALGContext* ctx, xmlNodePtr nodePtr)
{

    /*
     fetch the node id from xml 
     (the corresponding algebra node will be stored in the node store 
     at this position)
    */
    int nodeID =  PFxml2la_xpath_getIntValue(
            PFxml2la_xpath_getAttributeValueFromAttributeNode(
                    PFxml2la_xpath_getNthNode(XPATH("/@id"), 0)) );
    return nodeID;
}


PFla_op_t* 
getChildNode(XML2LALGContext* ctx, xmlNodePtr nodePtr, int pos)
{


    PFla_op_t* childNode = NULL; 


    /* fetch the node id(s) of the child node(s) from xml */
    xmlXPathObjectPtr  childNodeIDs_xml =  XPATH("//edge/@to");
    if (childNodeIDs_xml)
    {

        xmlNodePtr childNodeID_xml = 
            PFxml2la_xpath_getNthNode(childNodeIDs_xml, pos);
        if (childNodeID_xml)
        {
            /* fetch the childe node id from xml */
            int childNodeID = PFxml2la_xpath_getIntValue(
                    PFxml2la_xpath_getAttributeValueFromAttributeNode(
                        childNodeID_xml));
            /* lookup the child algebra node in the node store */
            childNode =  lookupAlgNode(ctx, childNodeID);
        }

        if(childNodeIDs_xml)
            xmlXPathFreeObject(childNodeIDs_xml);
    }

    return childNode;   

}



PFla_op_t* 
getRootNode(XML2LALGContext* ctx) 
{


    xmlXPathObjectPtr xpathObjPtr = 
        PFxml2la_xpath_evalXPathFromDocCtx(ctx->docXPathCtx, "//node");

    xmlNodeSetPtr nodeSetPtr =  xpathObjPtr->nodesetval;
    int nodeCount = nodeSetPtr->nodeNr;

    xmlNodePtr rootNodePtr = nodeSetPtr->nodeTab[nodeCount-1];
    int rootNodeID = getNodeID(ctx, rootNodePtr);

    PFla_op_t* rootNode = lookupAlgNode(ctx, rootNodeID);

    /*
    if(nodeSetPtr)
        xmlXPathFreeNodeSet(nodeSetPtr);
    */
    if(xpathObjPtr)
        xmlXPathFreeObject(xpathObjPtr);

    return rootNode;
}



/******************************************************************************/
/******************************************************************************/

PFla_op_kind_t 
getPFLAOpKind(XML2LALGContext* ctx, xmlNodePtr nodePtr)
{

    /* fetch the node kind string from xml */
    char* nodeKindAsXMLString = PFxml2la_xpath_getAttributeValueFromAttributeNode(
        PFxml2la_xpath_getNthNode(XPATH("/@kind"), 0));
    /* convert the node kind string to the corresponding algebra operator kind id */
    PFla_op_kind_t  algOpKindID = PFxml2la_conv_2PFLA_OpKind(nodeKindAsXMLString);
    return algOpKindID;
}

PFalg_att_t 
getPFLA_OptionalAttribute(
    XML2LALGContext* ctx, 
    xmlNodePtr nodePtr, 
    const char* xpathExpression, 
    PFalg_att_t fallback)
{

    PFalg_att_t value = fallback;

    xmlNodePtr xml_att =  
        PFxml2la_xpath_getNthNode(XPATH(xpathExpression), 0);
    if (xml_att)
    {
        value =  ctx->convert2PFLA_attributeName(
            PFxml2la_xpath_getAttributeValueFromAttributeNode(xml_att));

    }

    return  value;  
}



PFalg_atom_t 
getPFLA_Atom(
    XML2LALGContext* ctx, 
    xmlNodePtr nodePtr, 
    const char* xpathExpression)
{
    /* fetch the atom from xml and transfer it to the corresponding PF-Type */
    xmlNodePtr atom_xml = PFxml2la_xpath_getNthNode(XPATH(xpathExpression), 0);
    PFalg_atom_t atom = PFxml2la_conv_2PFLA_atom(
        PFxml2la_xpath_getAttributeValueFromElementNode(
            atom_xml, "type"), PFxml2la_xpath_getElementValue(atom_xml));
    return atom;
}


/*
todo: check order...
*/
PFalg_attlist_t 
getPFLA_AttributeList(
    XML2LALGContext* ctx, 
    xmlNodePtr nodePtr, 
    const char* xpathExpression)
{


    /* fetch all attribute columns from xml */
    xmlXPathObjectPtr atts_xml =  XPATH(xpathExpression);

    /* how many attributes do we have? */
    int attsCount = PFxml2la_xpath_getNodeCount(atts_xml);;



    /* (1) fetch the attributes from xml */
    PFalg_att_t* atts = PFmalloc (attsCount * sizeof (PFalg_att_t));
    for (int i = 0; i < attsCount; i++)
    {
        /*
         fetch the attribute name from xml, 
         transfer it to the corresponding PF-Type, and
         store it in the attribute list
        */
        atts[i] = ctx->convert2PFLA_attributeName(
            PFxml2la_xpath_getAttributeValueFromElementNode(
                PFxml2la_xpath_getNthNode(atts_xml, i), "name"));
    }             

    /* (2) construct the attribute list  */
    PFalg_attlist_t attlist = PFalg_attlist_(attsCount, atts);

    if(atts_xml)
        xmlXPathFreeObject(atts_xml);

    return attlist;

}


PFalg_tuple_t* 
getPFLA_Tuples(
    XML2LALGContext* ctx, 
    xmlNodePtr nodePtr, 
    const char* xpathExpression, 
    int rowCount, 
    int columnCount)
{


    /* fetch all atoms from !all! columns at once from xml */
    xmlXPathObjectPtr atoms_xml =  XPATH(xpathExpression);

    /* allocate the tuple list (a list of atom lists)*/
    PFalg_tuple_t* tuples =  
        (PFalg_tuple_t*) PFmalloc (rowCount * sizeof (PFalg_tuple_t));
    for (int row = 0; row < rowCount; row++)
    {
        /* we have columnCount atoms in each tuple */
        tuples[row].count = columnCount;
        /* and therefore we must allocate a tuple able to hold columnCount atoms */
        tuples[row].atoms = PFmalloc (columnCount * sizeof (PFalg_atom_t));
    }


    /* fetch and relate the atoms from xml to the corresponding tuples */ 
    for (int row = 0; row < rowCount; row++)
    {
        for (int column = 0; column < columnCount; column++)
        {
            /* fetch the next atom from xml*/
            xmlNodePtr atom_xml = PFxml2la_xpath_getNthNode(
                atoms_xml, (column * rowCount + row));
            /* convert the atom-data to pf-atom-data and 
            store the atom in the correct tuple position */
            tuples[row].atoms[column] = 
                PFxml2la_conv_2PFLA_atom(
                    PFxml2la_xpath_getAttributeValueFromElementNode(
                        atom_xml, "type"),
                            PFxml2la_xpath_getElementValue(atom_xml));
        }
    }

    if(atoms_xml)
        xmlXPathFreeObject(atoms_xml);

    return  tuples;
}


/* todo: check order... */
PFalg_sel_t* 
getPFLA_Predicates(
    XML2LALGContext* ctx, 
    xmlNodePtr nodePtr, 
    const char* xpathExpression)
{
    /*
     <content>
      (<comparison kind="KIND">
         <column name="COLNAME" new="false" position="1"/>
         <column name="COLNAME" new="false" position="2"/>
       </comparison>)*
    </content>             
     */



    /* fetch the predicates (and indirectly the predicate count) from xml */
    xmlXPathObjectPtr predicates_xml =  XPATH(xpathExpression);

    /* how many predicates do we have? */
    int predicateCount =  PFxml2la_xpath_getNodeCount(predicates_xml);

    /*
    ******************************************************************
    ******************************************************************
    * construct the Predicate List
    ******************************************************************
    ******************************************************************
    */

    /* allocate the predicate list*/
    PFalg_sel_t* predicates =  
        (PFalg_sel_t*) PFmalloc (predicateCount * sizeof (PFalg_sel_t));
    /* fetch and relate the predicates from xml to 
    the corresponding pf predicates */
    for (int p = 0; p < predicateCount; p++)
    {
        xmlNodePtr predicate_xml = 
            PFxml2la_xpath_getNthNode(predicates_xml, p);


        predicates[p].comp = PFxml2la_conv_2PFLA_comparisonType(
            PFxml2la_xpath_getAttributeValueFromElementNode(
                predicate_xml, "kind"));


        xmlNodePtr predicateColumn = predicate_xml->children->next;
        xmlNodePtr predicateOperand_left_xml = predicateColumn;
        predicateColumn = predicate_xml->children->next->next->next;
        xmlNodePtr predicateOperand_right_xml = predicateColumn;

        

        predicates[p].left = ctx->convert2PFLA_attributeName(
            PFxml2la_xpath_getAttributeValueFromElementNode(
                predicateOperand_left_xml, "name"));
        predicates[p].right = ctx->convert2PFLA_attributeName(
            PFxml2la_xpath_getAttributeValueFromElementNode(
                predicateOperand_right_xml, "name"));

        
    }


    if(predicates_xml)
        xmlXPathFreeObject(predicates_xml);

    return predicates;

}


PFalg_proj_t* 
getPFLA_Projection(
    XML2LALGContext* ctx, 
    xmlNodePtr nodePtr, 
    const char* xpathExpression)
{


    /* fetch the column elements from xml */
    xmlXPathObjectPtr columnNames_xml =  XPATH(xpathExpression);

    /* how many projection columns do we have? */
    int columnCount = PFxml2la_xpath_getNodeCount(columnNames_xml);


    /* allocate the projection list*/
    PFalg_proj_t* projections =  
        (PFalg_proj_t*) PFmalloc (columnCount * sizeof (PFalg_proj_t));
    /* fetch and relate the projections from xml 
    to the corresponding pf projections */ 
    for (int c = 0; c < columnCount; c++)
    {
        xmlNodePtr projection_xml = 
            PFxml2la_xpath_getNthNode(columnNames_xml, c);


        char* newName = PFxml2la_xpath_getAttributeValueFromElementNode(
            projection_xml, "name");
        char* oldName = PFxml2la_xpath_getAttributeValueFromElementNode(
            projection_xml, "old_name");
        if (oldName == NULL)
        {
            oldName = newName;
        }

        projections[c].new = ctx->convert2PFLA_attributeName(newName);
        projections[c].old = ctx->convert2PFLA_attributeName(oldName);
    }

    if(columnNames_xml)
        xmlXPathFreeObject(columnNames_xml);

    return projections;
}





/* todo: check order... */
PFord_ordering_t 
getPFLA_Ordering(
    XML2LALGContext* ctx, 
    xmlNodePtr nodePtr, 
    const char* xpathExpression)
{

    /* fetch the sorting columns from xml */
    xmlXPathObjectPtr sortColumns_xml =  XPATH(xpathExpression);

    /* how many orderings do we have? */
    int orderingsCount = PFxml2la_xpath_getNodeCount(sortColumns_xml);


    /* create an empty ordering */
    PFord_ordering_t ordering = PFord_order_intro_(0, NULL);

    /* successively refine the ordering according to the specified sort columns */
    for (int i = 0; i < orderingsCount; i++)
    {
        PFalg_att_t attribute = ctx->convert2PFLA_attributeName(
            PFxml2la_xpath_getAttributeValueFromElementNode(
                PFxml2la_xpath_getNthNode(
                    sortColumns_xml, i), "name")); 
        bool direction = PFxml2la_conv_2PFLA_orderingDirection(
            PFxml2la_xpath_getAttributeValueFromElementNode(
                PFxml2la_xpath_getNthNode(
                    sortColumns_xml, i), "direction"));
        ordering = PFord_refine (ordering, attribute, direction);
    }

    if(sortColumns_xml)
        xmlXPathFreeObject(sortColumns_xml);

    return ordering;

}





extern char* schema_relaxNG;


/* validate a document against the dtd*/
void validateByRELAXNG(xmlDocPtr doc)
{


    /* create buffer for reading the RelaxNG Schema from memory */
    xmlRelaxNGParserCtxtPtr relaxNGParserCtxtPtr = 
        xmlRelaxNGNewMemParserCtxt(schema_relaxNG, strlen(schema_relaxNG));
    /* Load and parse the RelaxNG Schema*/
    xmlRelaxNGPtr relaxNGPtr = xmlRelaxNGParse(relaxNGParserCtxtPtr);
    /* Allocate a validation context structure */
    xmlRelaxNGValidCtxtPtr  relaxNGValidCtxtPtr  = 
        xmlRelaxNGNewValidCtxt(relaxNGPtr);


    xmlRelaxNGSetParserErrors 
    (
    relaxNGParserCtxtPtr ,
    (xmlRelaxNGValidityErrorFunc) fprintf,
    (xmlRelaxNGValidityWarningFunc) fprintf,
    stderr
    );

    xmlRelaxNGSetValidErrors
    (
    relaxNGValidCtxtPtr,
    (xmlRelaxNGValidityErrorFunc) fprintf,
    (xmlRelaxNGValidityWarningFunc) fprintf,
    stderr
    );




    int result = xmlRelaxNGValidateDoc(relaxNGValidCtxtPtr, doc);

    if (result != 0)
    {
        PFoops (OOPS_FATAL, "XML input is invalid.\n");
    }

}








/* 
  =============================================================================
  =============================================================================
  print debug infos
  =============================================================================
  =============================================================================
*/

void info1(XML2LALGContext* ctx, xmlNodePtr nodePtr)
{



    /* lookup the node kind in the xml document */
    char* nodeKindAsXMLString = 
        PFxml2la_xpath_getAttributeValueFromAttributeNode(
            PFxml2la_xpath_getNthNode(
                PFxml2la_xpath_evalXPathFromNodeCtx(
                    ctx->docXPathCtx, nodePtr, "/@kind"), 0));

    int  algOpKindID = getPFLAOpKind (ctx,nodePtr);

    /* lookup the node id in the xml document */
    int nodeID =  PFxml2la_xpath_getIntValue(
        PFxml2la_xpath_getAttributeValueFromAttributeNode(
            PFxml2la_xpath_getNthNode(
                PFxml2la_xpath_evalXPathFromNodeCtx(
                    ctx->docXPathCtx, nodePtr, "/@id"), 0)) );




    printf("\n-------------------------------\n");
    printf("createAndStoreAlgOpNode: %s, %i, %i\n", 
           nodeKindAsXMLString, nodeID, algOpKindID);
    printf("line: %i\n", nodePtr->line);



    /* lookup the two child node ids in the xml document */
    xmlXPathObjectPtr xpathObjPtr =  
        PFxml2la_xpath_evalXPathFromNodeCtx(
            ctx->docXPathCtx, nodePtr, "//edge/@to");
    if (xpathObjPtr)
    {

        xmlNodeSetPtr nodeSetPtr =  xpathObjPtr->nodesetval;

        int size = (nodeSetPtr) ? nodeSetPtr->nodeNr : 0;

        for (int i = 0; i <size; i++)
        {

            xmlNodePtr nodePtr = nodeSetPtr->nodeTab[i];   

            int childNodeID = PFxml2la_xpath_getIntValue( 
                PFxml2la_xpath_getAttributeValueFromAttributeNode(nodePtr ));
            printf("\t edge-to: %i\n", childNodeID);



        }

        /*
        if(nodeSetPtr)
            xmlXPathFreeNodeSet(nodeSetPtr);
       */
    }


    if(xpathObjPtr)
        xmlXPathFreeObject(xpathObjPtr);

    printf("\n-------------------------------\n");



}








