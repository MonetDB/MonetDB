/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Importing of XML-serialized logical Algebra Plans.
 *
 * The XML Importer makes use of the libxml2 xpath facilities:
 *  - http://xmlsoft.org/html/libxml-xpath.html
 *  - http://xmlsoft.org/html/libxml-tree.html
 *  - http://xmlsoft.org/html/libxml-xmlstring.html
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
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2010 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id$
 */



#include "pf_config.h"
#include "pathfinder.h"

#include <stdio.h>
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

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"


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


#define XPATH2(nodePtr, xpath) \
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
 * Macro return-type:  PFalg_col_t
 * XPath return-type:  attribute(){1}
 */
#define PFLA_ATT(xpath) \
    ctx->convert2PFLA_attributeName( \
        PFxml2la_xpath_getAttributeValueFromAttributeNode( \
            PFxml2la_xpath_getNthNode(XPATH(xpath), 0)))

/**
 * Macro return-type:  PFalg_col_t
 * XPath return-type:  attribute()?
 */
#define PFLA_ATT_O(xpath, fallback) \
            getPFLA_OptionalAttribute(ctx, nodePtr, xpath, fallback)

/**
 * Macro return-type:  PFalg_collist_t *
 * XPath return-type:  element(column)*
 */
#define PFLA_ATT_LST(xpath) \
            getPFLA_AttributeList(ctx, nodePtr, xpath)


/**
 * Macro return-type:  PFalg_schema_t
 * XPath return-type:  element(column)+
 */
#define PFLA_SCHEMA(xpath) \
            getPFLA_Schema(ctx, nodePtr, xpath)


/**
 * Macro return-type:  PFarray_t*
 * XPath return-type:  element(key)+
 */
#define PFLA_KEYINFOS(xpath, schema) \
            getPFLA_KeyInfos(ctx, nodePtr, xpath, schema)


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
 * Macro return-type:  PFalg_node_kind_t
 * XPath return-type:  attribute(){1}
 */
#define PFLA_NODEKIND(xpath) \
            PFxml2la_conv_2PFLA_nodekind( \
                PFxml2la_xpath_getAttributeValueFromAttributeNode( \
                    PFxml2la_xpath_getNthNode(XPATH(xpath), 0)))

/**
 * Macro return-type:  PFalg_fun_call_t
 * XPath return-type:  attribute(){1}
 */
#define PFLA_FUNCALLKIND(xpath) \
            PFxml2la_conv_2PFLA_fun_callkind( \
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
 * Macro return-type:  PFalg_doc_tbl_kind_t
 * XPath return-type:  attribute(){1}
 */
#define PFLA_DOCTBLTY(xpath) \
                PFxml2la_conv_2PFLA_doctblType( \
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
 * Macro return-type:  PFalg_sel_t*
 * XPath return-type:  element(comparison)+
 */
#define PFLA_AGGREGATES(xpath) \
                    getPFLA_Aggregates(ctx, nodePtr, xpath)


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


/**
 * Macro return-type:  char*
 * XPath return-type:  attribute(){1}
 */
#define A2STR(xpath) \
  (char*)xmlXPathCastToString(XPATH(xpath))

/**
 * Macro return-type:  char*
 * XPath return-type:  attribute()?
 */
#define A2STR_O(xpath, fallback) \
   getOptionalAttributeValueAsStr(ctx, nodePtr, xpath, fallback)



/**
 * Macro return-type:  PFarray_t* (with element-type char*)
 * XPath return-type:  attribute()+
 */
#define AS2STRLST(xpath) \
  PFxml2la_xpath_getAttributeValuesFromAttributeNodes(XPATH(xpath))




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

PFla_pb_t*
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

char *
getOptionalAttributeValueAsStr(
  XML2LALGContext* ctx,
  xmlNodePtr nodePtr,
  const char* xpathExpression,
  char *fallback);

/******************************************************************************/
/******************************************************************************/

int
getNodeID(XML2LALGContext* ctx, xmlNodePtr nodePtr);

PFla_op_t*
getChildNode(XML2LALGContext* ctx, xmlNodePtr nodePtr, int pos);

PFla_op_t*
getRootNode(XML2LALGContext* ctx, xmlNodePtr nodePtr);

/******************************************************************************/
/******************************************************************************/

PFla_op_kind_t
getPFLAOpKind(XML2LALGContext* ctx, xmlNodePtr nodePtr);

PFalg_col_t
getPFLA_OptionalAttribute(
    XML2LALGContext* ctx,
    xmlNodePtr nodePtr,
    const char* xpathExpression,
    PFalg_col_t fallback);

PFalg_atom_t
getPFLA_Atom(
    XML2LALGContext* ctx,
    xmlNodePtr nodePtr,
    const char* xpathExpression);

PFalg_collist_t *
getPFLA_AttributeList(
    XML2LALGContext* ctx,
    xmlNodePtr nodePtr,
    const char* xpathExpression);


PFalg_schema_t
getPFLA_Schema(
    XML2LALGContext* ctx,
    xmlNodePtr nodePtr,
    const char* xpathExpression);


PFarray_t *
getPFLA_KeyInfos(
    XML2LALGContext* ctx,
    xmlNodePtr nodePtr,
    const char* xpathExpression,
    PFalg_schema_t schema);

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

PFalg_aggr_t*
getPFLA_Aggregates(
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

PFla_pb_item_property_t
getPFLA_qp_property(
    XML2LALGContext* ctx,
    xmlNodePtr nodePtr);

PFarray_t*
getPFLA_qp_properties(
    XML2LALGContext* ctx,
    xmlNodePtr nodePtr);

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
PFxml2la_xml2lalgContext(void)
{
    XML2LALGContext* ctx;

    ctx = (XML2LALGContext*) PFmalloc (sizeof (XML2LALGContext));

    ctx->nodeStore = PFarray(sizeof (PFla_op_t*), 200);

    return ctx;
}



PFla_pb_t*
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



PFla_pb_t*
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

static PFla_op_t *
importLogicalQueryPlan (XML2LALGContext* ctx, xmlNodePtr nodePtr)
{
    /**
     * dependent from the value of the unique_names-atrribute of the
     * logical_query_plan-element we chose the appropriate function
     * for converting strings to PF-Attribute-Names
     */
    bool uniqueNames = PFxml2la_xpath_getBoolValue(
                           PFxml2la_xpath_getAttributeValueFromAttributeNode(
                               PFxml2la_xpath_getNthNode(
                                   XPATH("/@unique_names"), 0)));
    if (uniqueNames)
    {
        ctx->convert2PFLA_attributeName = PFxml2la_conv_2PFLA_attributeName_unq;
    }
    else
    {
        ctx->convert2PFLA_attributeName = PFxml2la_conv_2PFLA_attributeName;
    }

    /* reset node store */
    PFarray_last (ctx->nodeStore) = 0;

    /**************************************************************************/


    /* fetch the serialized algebra nodes from xml */
    xmlXPathObjectPtr xpathObjPtr = XPATH("//node");

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

        PFla_op_t* rootNode = getRootNode(ctx, nodePtr);

		assert(rootNode);

		assert(rootNode->kind == la_serialize_seq
			|| rootNode->kind == la_serialize_rel);

        return rootNode;
    }

    return NULL;
}


static void
importQueryPlanBundle (XML2LALGContext* ctx, xmlNodePtr nodePtr, PFla_pb_t *lapb)
{
    PFla_op_t *op;
    PFarray_t *properties;
    int        id     = A2INT_O ("/@id", -1),
               idref  = A2INT_O ("/@idref", -1),
               colref = A2INT_O ("/@colref", -1);

    if (id == -1)
        PFoops (OOPS_FATAL,
                "could not find query plan id in plan bundle");

    properties = getPFLA_qp_properties(ctx, nodePtr);

    op = importLogicalQueryPlan (
            ctx,
            PFxml2la_xpath_getNthNode(
                XPATH ("/logical_query_plan"), 0));

    assert (op);

    PFla_pb_add (lapb) = (PFla_pb_item_t) { .op       = op,
                                            .id       = id,
                                            .idref    = idref,
                                            .colref   = colref,
                                            .properties = properties};
}


PFla_pb_t*
importXML(XML2LALGContext* ctx, xmlDocPtr doc)
{
    PFla_pb_t *lapb = PFla_pb (5);

        /* Create xpath evaluation context */
    xmlXPathContextPtr docXPathCtx = xmlXPathNewContext(doc);
    if (!docXPathCtx)
    {
        PFoops (OOPS_FATAL,  "unable to create a XPath context");
    }


    /**************************************************************************/
    ctx->docXPathCtx = docXPathCtx;

    xmlXPathObjectPtr query_plans = PFxml2la_xpath_evalXPathFromDocCtx(
                                        docXPathCtx,
                                        "//query_plan");
    if (query_plans) {
        xmlNodeSetPtr nodeSetPtr = query_plans->nodesetval;
        int nodeCount = (nodeSetPtr) ? nodeSetPtr->nodeNr : 0;
        for (int i = 0; i < nodeCount; i++)
        {
            xmlNodePtr nodePtr = nodeSetPtr->nodeTab[i];
            /* pass the node to the plan bundle processing function */
            importQueryPlanBundle (ctx, nodePtr, lapb);
        }
    }

    if (PFla_pb_size (lapb) == 0) {
        PFla_op_t *root = importLogicalQueryPlan (
                             ctx,
                             PFxml2la_xpath_getNthNode(
                                 PFxml2la_xpath_evalXPathFromDocCtx(
                                     docXPathCtx, "/logical_query_plan"), 0));

        assert (root);

        PFla_pb_add (lapb) = (PFla_pb_item_t) { .op     = root,
                                                .id     = -1,
                                                .idref  = -1,
                                                .colref = -1 };
    }
    if(query_plans)
        xmlXPathFreeObject(query_plans);
    if(docXPathCtx)
        xmlXPathFreeContext(docXPathCtx);

    return lapb;
}



/**
 * Legacy XML code import for old aggregate representations.
 */
static PFla_op_t *
aggregate_legacy_detect (XML2LALGContext* ctx, xmlNodePtr nodePtr)
{
    PFalg_aggr_t      aggr;
    PFalg_aggr_kind_t kind = alg_aggr_count;
    char             *kind_str;

    /* fetch the node kind string from xml */
    kind_str = PFxml2la_xpath_getAttributeValueFromAttributeNode (
                   PFxml2la_xpath_getNthNode(XPATH("/@kind"), 0));

    /* standard case */
    if (!strcmp ("aggr", kind_str))
        return NULL;
    /* legacy code */
    else if (!strcmp ("count", kind_str))
        kind = alg_aggr_count;
    else if (!strcmp ("sum", kind_str))
        kind = alg_aggr_sum;
    else if (!strcmp ("min", kind_str))
        kind = alg_aggr_min;
    else if (!strcmp ("max", kind_str))
        kind = alg_aggr_max;
    else if (!strcmp ("avg", kind_str))
        kind = alg_aggr_avg;
    else if (!strcmp ("prod", kind_str))
        kind = alg_aggr_prod;
    else if (!strcmp ("seqty1", kind_str))
        kind = alg_aggr_seqty1;
    else if (!strcmp ("all", kind_str))
        kind = alg_aggr_all;
    else
        PFoops (OOPS_FATAL,
                "could not recognize aggregate kind (%s)",
                kind_str);

    /*
    <content>
       <column name="COLNAME" new="true"/>
       <column name="COLNAME" new="false" function="item"/>
      (<column name="COLNAME" function="partition" new="false"/>)?
    </content>
    */

    aggr = PFalg_aggr (kind,
                       PFLA_ATT("/content/column[@new='true']/@name"),
                       PFLA_ATT_O("/content/column[@function='item']/@name",
                                  col_NULL));

    return PFla_aggr (CHILDNODE(0),
                      PFLA_ATT_O("/content/column[@function='partition']/@name",
                                 col_NULL),
                      1,
                      &aggr);
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
             PFLA_ATT(
                 "/content/column[@new='false' and @function='pos']/@name"),
             PFLA_ATT(
                 "/content/column[@new='false' and @function='item']/@name")
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
              (<column name="COLNAME" new="false" function="item"
                       position="[0..n]"/>)+
             </content>
            */

            newAlgNode = PFla_serialize_rel
             (
             CHILDNODE(0),
             CHILDNODE(1),
             PFLA_ATT(
                 "/content/column[@new='false' and @function='iter']/@name"),
             PFLA_ATT(
                 "/content/column[@new='false' and @function='pos']/@name"),
             PFLA_ATT_LST("/content/column[@new='false' and @function='item']")
             );

        }
        break;

/******************************************************************************/
/******************************************************************************/

    case la_side_effects             :

        {
            newAlgNode = PFla_side_effects(CHILDNODE(0), CHILDNODE(1));
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
             PFLA_TUPLES("/content/column[@new='true']/value",
                         rowCount, columnCount)
             );
        }
        break;

/******************************************************************************/
/******************************************************************************/

    case la_empty_tbl            :

        {
            /*
            <content>
              (<column name="COLNAME" type="DATATYPE" new="true"/>)*
            </content>
            */

            newAlgNode = PFla_empty_tbl_
             (
             PFLA_SCHEMA("/content/column[@new='true']")
             );
        }
        break;


/******************************************************************************/
/******************************************************************************/

    case la_ref_tbl              :

        {
            /*
             <properties>
                <keys>
                    (<key>
                        (<column name="COLNAME" position="[0..n]"/>)+
                    </key>)+
                </keys>
             </properties>

             <content>
                <table name="TABLENAME">
                    (<column name="COLNAME" tname="TCOLNAME"
                             type="DATATYPE"/>)+
                </table>
             </content>
            */

            PFalg_schema_t schema = PFLA_SCHEMA("/content/table/column");

            newAlgNode = PFla_ref_tbl_
             (
             A2STR("/content/table/@name"),
             schema,
             AS2STRLST("/content/table/column/@tname"),
             PFLA_KEYINFOS("/properties/keys/key", schema)
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
              (<column name="COLNAME" function="sort" position="[0..n]"
                       direction="DIRECTION" new="false"/>)+
              (<column name="COLNAME" function="partition" new="false"/>)?
            </content>
            */

            newAlgNode = PFla_pos_select
             (
             CHILDNODE(0),
             E2INT("/content/position"),
             PFLA_ORDERING("/content/column[@function='sort']"),
             PFLA_ATT_O("/content/column[@function='partition']/@name",
                        col_NULL)
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
               <column name="COLNAME" new="false" position="1"/>
               <column name="COLNAME" new="false" position="2"/>
            </content>
            */

            newAlgNode = PFla_to
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

    case la_aggr                 :
        {
            /* check for legacy aggregate representations */
            if ((newAlgNode = aggregate_legacy_detect (ctx, nodePtr)))
                break;

            /*
              <content>
               (<column name="COLNAME" function="partition" new="false"/>)?
               (<aggregate kind="KIND">
                  <column name="COLNAME" new="true"/>
                  <column name="COLNAME" new="false"/>
                </aggregate>)+
             </content>
            */

            newAlgNode = PFla_aggr
             (
             CHILDNODE(0),
             PFLA_ATT_O("/content/column[@function='partition']/@name",
                        col_NULL),
             NODECOUNT("/content/aggregate"),
             PFLA_AGGREGATES("/content/aggregate")
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
              (<column name="COLNAME" function="sort" position="[0..n]"
                       direction="DIRECTION" new="false"/>)+
              (<column name="COLNAME" function="partition" new="false"/>)?
            </content>
            */

            newAlgNode = PFla_rownum
             (
             CHILDNODE(0),
             PFLA_ATT("/content/column[@new='true']/@name"),
             PFLA_ORDERING("/content/column[@function='sort']"),
             PFLA_ATT_O("/content/column[@function='partition']/@name",
                        col_NULL)
             );
        }
        break;

/******************************************************************************/
/******************************************************************************/

    case la_rowrank              :

        {
            /*
            <content>
               <column name="COLNAME" new="true"/>
              (<column name="COLNAME" function="sort" position="[0..n]"
                       direction="DIRECTION" new="false"/>)+
            </content>
            */

            newAlgNode = PFla_rowrank
             (
             CHILDNODE(0),
             PFLA_ATT("/content/column[@new='true']/@name"),
             PFLA_ORDERING("/content/column[@function='sort']")
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
              (<column name="COLNAME" function="sort" position="[0..n]"
                       direction="DIRECTION" new="false"/>)+
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

    case la_rowid                :

        {
            /*
             <content>
               <column name="COLNAME" new="true"/>
             </content>
            */

            newAlgNode = PFla_rowid
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

    case la_step                 :

        {
            /*
            <content>
               <step axis="AXIS" kind="KIND TEST"
                     (prefix="NS TEST" uri="NS TEST")?
                     (name="NAME TEST")? (level="LEVEL")? />
               <column name="COLNAME" function="iter"/>
               <column name="COLNAME" function="item"/>
            </content>
            */
            PFalg_step_spec_t spec;
            spec.axis = PFLA_AXIS("/content/step/@axis");
            spec.kind = PFLA_NODEKIND("/content/step/@kind");
            spec.qname = PFqname (
                             (PFns_t)
                             { .prefix = A2STR_O("/content/step/@prefix", NULL),
                               .uri    = A2STR_O("/content/step/@uri", NULL) },
                             A2STR_O("/content/step/@name", NULL));

            newAlgNode = PFla_step
             (
             CHILDNODE(0), CHILDNODE(1),
             spec,
             A2INT_O("/content/step/@level", UNKNOWN_LEVEL),
             PFLA_ATT("/content/column[@function='iter']/@name"),
             PFLA_ATT("/content/column[@function='item']/@name"),
             PFLA_ATT("/content/column[@function='item']/@name")
             );
        }
        break;

/******************************************************************************/
/******************************************************************************/

    case la_step_join            :

        {
            /*
             <content>
               <step axis="AXIS" kind="KIND TEST"
                     (prefix="NS TEST" uri="NS TEST")?
                     (name="NAME TEST")? (level="LEVEL")? />
               <column name="COLNAME" new="true"/>
               <column name="COLNAME" function="item"/>
             </content>
            */
            PFalg_step_spec_t spec;
            spec.axis = PFLA_AXIS("/content/step/@axis");
            spec.kind = PFLA_NODEKIND("/content/step/@kind");
            spec.qname = PFqname (
                             (PFns_t)
                             { .prefix = A2STR_O("/content/step/@prefix", NULL),
                               .uri    = A2STR_O("/content/step/@uri", NULL) },
                             A2STR_O("/content/step/@name", NULL));

            newAlgNode = PFla_step_join
             (
             CHILDNODE(0), CHILDNODE(1),
             spec,
             A2INT_O("/content/step/@level", UNKNOWN_LEVEL),
             PFLA_ATT("/content/column[@function='item']/@name"),
             PFLA_ATT("/content/column[@new='true']/@name")
             );
        }
        break;


/******************************************************************************/
/******************************************************************************/
        /*
         todo: implement
         needed: "Guide-File address" as global param in the query plan,
                 tokenizing, etc.
        */
    case la_guide_step           :

        {
            /*
            <content>
               <step axis="AXIS" kind="KIND TEST"
                     (prefix="NS TEST" uri="NS TEST")?
                     (name="NAME TEST")? (level="LEVEL")?
                     guide="GUIDELIST"/>
               <column name="COLNAME" function="iter"/>
               <column name="COLNAME" function="item"/>
            </content>
            */


           PFoops (OOPS_FATAL,
                   "Import of la_guide_step operator is not implemented yet");

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
               <step axis="AXIS" kind="KIND TEST"
                     (prefix="NS TEST" uri="NS TEST")?
                     (name="NAME TEST")? (level="LEVEL")?
                     guide="GUIDELIST"/>
               <column name="COLNAME" new="true"/>
               <column name="COLNAME" function="item"/>
            </content>
            */

            PFoops (OOPS_FATAL,
                    "Import of la_guide_step_join operator "
                    "is not implemented yet");


        }
        break;

/******************************************************************************/
/******************************************************************************/

    case la_doc_index_join                :

        {
            PFoops (OOPS_FATAL,
                    "Import of la_doc_index_join operator "
                    "is not implemented yet");
        }
        break;

/******************************************************************************/
/******************************************************************************/

    case la_doc_tbl              :

        {
            /*
            <content>
               <kind name="FUNCTION"/>
               <column name="COLNAME" new="true"/>
               <column name="COLNAME" new="false"/>
               <column name="COLNAME" function="iter"/>
               <column name="COLNAME" function="item"/>
            </content>
            */

            newAlgNode = PFla_doc_tbl
             (
             CHILDNODE(0),
             PFLA_ATT("/content/column[@new='true']/@name"),
             PFLA_ATT("/content/column[@new='false']/@name"),
             PFLA_DOCTBLTY("/content/kind/@name")
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

    case la_frag_extract         :

        {
            newAlgNode = PFla_frag_extract
             (
             CHILDNODE(0),
             A2INT_O("/content/column/@reference", -1)
             );
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

    case la_error                :
        {
            newAlgNode = PFla_error (CHILDNODE(0), CHILDNODE(1),
                                     PFLA_ATT("/content/column/@name"));
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

    case la_cache                :
        {
            newAlgNode = PFla_cache (
                             CHILDNODE(0),
                             CHILDNODE(1),
                             PFxml2la_xpath_getElementValue (
                                 PFxml2la_xpath_getNthNode(
                                     XPATH("/content/id"),
                                     0)),
                             PFLA_ATT("/content/column[@function='pos']/@name"),
                             PFLA_ATT("/content/column[@function='item']/@name"));
        }
        break;

/******************************************************************************/
/******************************************************************************/

    case la_trace                :

        {
            newAlgNode = PFla_trace (CHILDNODE(0), CHILDNODE(1));
        }
        break;

/******************************************************************************/
/******************************************************************************/

    case la_trace_items          :

        {
            /*
             <content>
               <column name="COLNAME" function="iter"/>
               <column name="COLNAME" function="pos"/>
               <column name="COLNAME" function="item"/>
             </content>
            */

            newAlgNode = PFla_trace_items
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

            PFoops (OOPS_FATAL,
                    "Import of la_rec_fix operator is not implemented yet");
        }
        break;

/******************************************************************************/
/******************************************************************************/

    case la_rec_param            :

        {
            PFoops (OOPS_FATAL,
                    "Import of la_rec_param operator is not implemented yet");
        }
        break;

/******************************************************************************/
/******************************************************************************/

    case la_rec_arg              :

        {
            PFoops (OOPS_FATAL,
                    "Import of la_rec_arg operator is not implemented yet");
        }
        break;

/******************************************************************************/
/******************************************************************************/

    case la_rec_base             :

        {
            PFoops (OOPS_FATAL,
                    "Import of la_rec_base operator is not implemented yet");
        }
        break;

/******************************************************************************/
/******************************************************************************/

    case la_fun_call             :

        {
            /* get occurrence indicator */
            unsigned int    min = A2INT_O("/content/output/@min",0),
                            max = A2INT_O("/content/output/@min",2);
            PFalg_occ_ind_t occ_ind;

            if (min == 1) { if (max == 1) occ_ind = alg_occ_exactly_one;
                            else          occ_ind = alg_occ_one_or_more; }
            else {          if (max == 1) occ_ind = alg_occ_zero_or_one;
                            else          occ_ind = alg_occ_unknown; }

            newAlgNode = PFla_fun_call
             (
             CHILDNODE(0), CHILDNODE(1),
             PFLA_SCHEMA("/content/output/column"),
             PFLA_FUNCALLKIND("/content/kind/@name"),
             PFqname (
                 (PFns_t)
                 { .prefix = NULL,
                   .uri    = A2STR_O("/content/function/@uri", NULL) },
                 A2STR_O("/content/function/@name", NULL)),
             NULL,
             PFLA_ATT("/content/column[@function='iter']/@name"),
             occ_ind
             );
        }
        break;

/******************************************************************************/
/******************************************************************************/

    case la_fun_param            :

        {
            newAlgNode = PFla_fun_param
             (
             CHILDNODE(0), CHILDNODE(1),
             PFLA_SCHEMA("/content/column")
             );
        }
        break;

/******************************************************************************/
/******************************************************************************/

    case la_fun_frag_param       :

        {
            newAlgNode = PFla_fun_frag_param
             (
             CHILDNODE(0), CHILDNODE(1),
             A2INT_O("/content/column/@reference", -1)
             );
        }
        break;

/******************************************************************************/
/******************************************************************************/

    case la_proxy                :

        {
            PFoops (OOPS_FATAL,
                    "Import of la_proxy operator is not implemented yet");
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

    case la_internal_op          :
        PFoops (OOPS_FATAL,
                "internal optimization operator is not allowed here");
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
               <column name="COLNAME" function"iter sep"/>
               <column name="COLNAME" function"item sep"/>
             </content>
            */

            newAlgNode = PFla_fn_string_join
             (
             CHILDNODE(0), CHILDNODE(1),
             PFLA_ATT("/content/column[@function='iter']/@name"),
             PFLA_ATT("/content/column[@function='pos']/@name"),
             PFLA_ATT("/content/column[@function='item']/@name"),
             PFLA_ATT("/content/column[@function='iter sep']/@name"),
             PFLA_ATT("/content/column[@function='item sep']/@name"),
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

char *
getOptionalAttributeValueAsStr(
    XML2LALGContext* ctx, xmlNodePtr nodePtr,
    const char* xpathExpression,
    char *fallback)
{

    char *value = fallback;

    xmlNodePtr xml_att =  PFxml2la_xpath_getNthNode(XPATH(xpathExpression), 0);
    if (xml_att)
    {
        value = PFxml2la_xpath_getAttributeValueFromAttributeNode(xml_att);

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
getRootNode(XML2LALGContext* ctx, xmlNodePtr nodePtr)
{

    xmlXPathObjectPtr xpathObjPtr = XPATH("//node");

    xmlNodeSetPtr nodeSetPtr =  xpathObjPtr->nodesetval;
    int nodeCount = nodeSetPtr->nodeNr;

    xmlNodePtr rootNodePtr = nodeSetPtr->nodeTab[nodeCount-1];
    int rootNodeID = getNodeID(ctx, rootNodePtr);

    PFla_op_t* rootNode = lookupAlgNode(ctx, rootNodeID);

    /*
    if(nodeSetPtr)
        xmlXPathFreeNodeSet(nodeSetPtr);
    */
    xmlXPathFreeObject(xpathObjPtr);

    return rootNode;
}



/******************************************************************************/
/******************************************************************************/

PFla_op_kind_t
getPFLAOpKind(XML2LALGContext* ctx, xmlNodePtr nodePtr)
{

    /* fetch the node kind string from xml */
    char* nodeKindAsXMLString;
    nodeKindAsXMLString = PFxml2la_xpath_getAttributeValueFromAttributeNode (
                              PFxml2la_xpath_getNthNode(XPATH("/@kind"), 0));
    /* convert the node kind string
       to the corresponding algebra operator kind id */
    return PFxml2la_conv_2PFLA_OpKind(nodeKindAsXMLString);
}

PFalg_col_t
getPFLA_OptionalAttribute(
    XML2LALGContext* ctx,
    xmlNodePtr nodePtr,
    const char* xpathExpression,
    PFalg_col_t fallback)
{

    PFalg_col_t value = fallback;

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
    PFalg_simple_type_t type;
    PFalg_atom_t atom;
    /* fetch the atom from xml and transfer it to the corresponding PF-Type */
    xmlNodePtr atom_xml = PFxml2la_xpath_getNthNode(XPATH(xpathExpression), 0);

    type = PFxml2la_conv_2PFLA_atomType (
               (char *) xmlXPathCastToString(XPATH2(atom_xml, "/@type")));

    if (type == aat_qname) {
        /* special treating of QNames as it is split up into three attributes */
        char *prefix, *uri, *local;

        prefix = (char *) xmlXPathCastToString (XPATH2 (atom_xml,
                                                        "/qname/@prefix"));
        uri    = (char *) xmlXPathCastToString (XPATH2 (atom_xml,
                                                        "/qname/@uri"));
        local  = (char *) xmlXPathCastToString (XPATH2 (atom_xml,
                                                        "/qname/@local"));
        atom = PFxml2la_conv_2PFLA_atom (type, prefix, uri, local);
    }
    else
        atom = PFxml2la_conv_2PFLA_atom(
                   type, NULL, NULL,
                   PFxml2la_xpath_getElementValue (atom_xml));

    return atom;
}


/*
todo: check order...
*/
PFalg_collist_t *
getPFLA_AttributeList(
    XML2LALGContext* ctx,
    xmlNodePtr nodePtr,
    const char* xpathExpression)
{
    /* fetch all attribute columns from xml */
    xmlXPathObjectPtr atts_xml =  XPATH(xpathExpression);

    /* how many attributes do we have? */
    int attsCount = PFxml2la_xpath_getNodeCount(atts_xml);;

    PFalg_collist_t *collist = PFalg_collist (attsCount);

    /* (1) fetch the attributes from xml */
    for (int i = 0; i < attsCount; i++)
    {
        /*
         fetch the attribute name from xml,
         transfer it to the corresponding PF-Type, and
         store it in the attribute list
        */
        cladd (collist) = ctx->convert2PFLA_attributeName(
            PFxml2la_xpath_getAttributeValueFromElementNode(
                PFxml2la_xpath_getNthNode(atts_xml, i), "name"));
    }

    if(atts_xml)
        xmlXPathFreeObject(atts_xml);

    return collist;
}



PFalg_schema_t
getPFLA_Schema(
    XML2LALGContext* ctx,
    xmlNodePtr nodePtr,
    const char* xpathExpression)
{

    /*
    (<column name="COLNAME" tname="TCOLNAME" type="DATATYPE"/>)+
    */

    PFalg_schema_t schema;

    xmlXPathObjectPtr columns_xml =  XPATH(xpathExpression);
    int columnCount = PFxml2la_xpath_getNodeCount(columns_xml);

    schema.count = columnCount;
    schema.items = PFmalloc (columnCount * sizeof (*(schema.items)));

    for (int i = 0; i < columnCount; i++) {

        xmlNodePtr column_xml = PFxml2la_xpath_getNthNode(columns_xml, i);

        PFalg_col_t columnName = ctx->convert2PFLA_attributeName(
            PFxml2la_xpath_getAttributeValueFromElementNode(
                column_xml, "name"));

        PFalg_simple_type_t columnType = PFxml2la_conv_2PFLA_atomType(
            PFxml2la_xpath_getAttributeValueFromElementNode(
                column_xml, "type"));

        /* in case we have a schema in a fixed order
           we respect that order */
        unsigned int pos = i;
        char *pos_str =
            PFxml2la_xpath_getAttributeValueFromElementNode(
                column_xml, "pos");

        if (pos_str)
            pos = atoi (pos_str);

        schema.items[pos].name = columnName;
        schema.items[pos].type = columnType;
    }

    return  schema;
}



/*
todo: check order...
*/
PFarray_t *
getPFLA_KeyInfos(
    XML2LALGContext* ctx,
    xmlNodePtr nodePtr,
    const char* xpathExpression,
    PFalg_schema_t schema)
{
    /*
    (<key>
        (<column name="COLNAME" position="[0..n]"/>)+
    </key>)+
    */

    PFarray_t * keys = PFarray (sizeof (PFarray_t*), 5);

    xmlXPathObjectPtr keys_xml =  XPATH(xpathExpression);
    int keysCount = PFxml2la_xpath_getNodeCount(keys_xml);

    for (int i = 0; i < keysCount; i++)
    {

        xmlNodePtr key_xml = PFxml2la_xpath_getNthNode(keys_xml, i);

        xmlXPathObjectPtr keyColumns_xml =  XPATH2(key_xml, "/column");
        int keyColumnsCount = PFxml2la_xpath_getNodeCount(keyColumns_xml);
        PFarray_t * keyPositions = PFarray (sizeof (int), keyColumnsCount);

        for (int j = 0; j < keyColumnsCount; j++)
        {
            xmlNodePtr  keyColumn_xml;
            char       *columnName;
            PFalg_col_t keyAttribute;

            keyColumn_xml = PFxml2la_xpath_getNthNode(keyColumns_xml, j);
            columnName = (char*) xmlXPathCastToString(
                                     XPATH2(keyColumn_xml, "/@name"));
            keyAttribute = ctx->convert2PFLA_attributeName(columnName);

            int keyPos = -1;
            for (unsigned int k = 0; k < schema.count; k++) {

                PFalg_schm_item_t schemaItem = schema.items[k];
                if(schemaItem.name == keyAttribute)
                {
                    keyPos = k;
                    break;
                }
            }

            assert(keyPos >= 0);

            *(int*) PFarray_add (keyPositions) = keyPos;

        }

        *(PFarray_t**) PFarray_add (keys) = keyPositions;

    }

    return keys;
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
        /* and therefore we must allocate a tuple
           able to hold columnCount atoms */
        tuples[row].atoms = PFmalloc (columnCount * sizeof (PFalg_atom_t));
    }


    /* fetch and relate the atoms from xml to the corresponding tuples */
    for (int row = 0; row < rowCount; row++)
    {
        for (int column = 0; column < columnCount; column++)
        {
            PFalg_simple_type_t type;

            /* fetch the next atom from xml*/
            xmlNodePtr atom_xml = PFxml2la_xpath_getNthNode(
                atoms_xml, (column * rowCount + row));

            /* convert the atom-data to pf-atom-data and
            store the atom in the correct tuple position */
            type = PFxml2la_conv_2PFLA_atomType (
                       (char *) xmlXPathCastToString(
                                  XPATH2(atom_xml, "/@type")));

            if (type == aat_qname) {
                /* special treating of QNames as it
                   is split up into three attributes */
                char *prefix, *uri, *local;

                prefix = (char *) xmlXPathCastToString (
                                      XPATH2 (atom_xml, "/qname/@prefix"));
                uri    = (char *) xmlXPathCastToString (
                                      XPATH2 (atom_xml, "/qname/@uri"));
                local  = (char *) xmlXPathCastToString (
                                      XPATH2 (atom_xml, "/qname/@local"));

                tuples[row].atoms[column] = PFxml2la_conv_2PFLA_atom (
                                                type, prefix, uri, local);
            }
            else
                tuples[row].atoms[column] = PFxml2la_conv_2PFLA_atom(
                           type, NULL, NULL,
                           PFxml2la_xpath_getElementValue (atom_xml));
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



PFalg_aggr_t *
getPFLA_Aggregates(
    XML2LALGContext* ctx,
    xmlNodePtr nodePtr,
    const char* xpathExpression)
{
    /*
     <content>
      (<aggregate kind="KIND">
         <column name="COLNAME" new="true"/>
         <column name="COLNAME" new="false"/>
       </aggregate>)*
    </content>
     */



    /* fetch the aggregates (and indirectly the predicate count) from xml */
    xmlXPathObjectPtr aggregates_xml =  XPATH(xpathExpression);

    /* how many aggregates do we have? */
    int aggregateCount =  PFxml2la_xpath_getNodeCount(aggregates_xml);

    /*
    ******************************************************************
    ******************************************************************
    * construct the Predicate List
    ******************************************************************
    ******************************************************************
    */

    /* allocate the aggregate list*/
    PFalg_aggr_t* aggregates =
        (PFalg_aggr_t*) PFmalloc (aggregateCount * sizeof (PFalg_aggr_t));
    /* fetch and relate the aggregates from xml to
    the corresponding pf aggregates */
    for (int p = 0; p < aggregateCount; p++)
    {
        xmlNodePtr aggregate_xml =
            PFxml2la_xpath_getNthNode(aggregates_xml, p);


        aggregates[p].kind = PFxml2la_conv_2PFLA_aggregateType(
            PFxml2la_xpath_getAttributeValueFromElementNode(
                aggregate_xml, "kind"));

        /* PFLA_ATT & PFLA_ATT_O macros use variable nodePtr
           as starting point for XPath expression */
        nodePtr = aggregate_xml;
        aggregates[p].res = PFLA_ATT ("/column[@new='true']/@name");
        aggregates[p].col = PFLA_ATT_O ("/column[@new='false']/@name", col_NULL);
    }

    if(aggregates_xml)
        xmlXPathFreeObject(aggregates_xml);

    return aggregates;

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

    /* successively refine the ordering according
       to the specified sort columns */
    for (int i = 0; i < orderingsCount; i++)
    {
        PFalg_col_t attribute = ctx->convert2PFLA_attributeName(
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






PFla_pb_item_property_t
getPFLA_qp_property(
    XML2LALGContext* ctx,
    xmlNodePtr nodePtr)
{
    /*
     <property name="foo" value="foo">
      (<property name="foo" value="foo">...</property>)*
    </property>
     */
	char* name =  PFstrdup(
					  PFxml2la_xpath_getAttributeValueFromElementNode(
							  nodePtr, "name"));
	char* value = PFstrdup(
					  PFxml2la_xpath_getAttributeValueFromElementNode(
							  nodePtr, "value"));

	PFarray_t*	properties = NULL;

	xmlXPathObjectPtr xpathObjPtr = PFxml2la_xpath_evalXPathFromNodeCtx(
										ctx->docXPathCtx, nodePtr, "/property");

	unsigned int nodeCount = PFxml2la_xpath_getNodeCount(xpathObjPtr);

	if (nodeCount > 0)
	{
		properties = PFarray (sizeof (PFla_pb_item_property_t), nodeCount);
		for (unsigned int i = 0; i < nodeCount; i++)
		{
			xmlNodePtr subItemNodePtr =
					PFxml2la_xpath_getNthNode(xpathObjPtr, i);

			PFla_pb_item_property_t subProperty =
					getPFLA_qp_property(ctx, subItemNodePtr);

			*(PFla_pb_item_property_t*) PFarray_add (properties) = subProperty;
		}
	}


	PFla_pb_item_property_t result = (PFla_pb_item_property_t) {
													.name = name,
	                                                .value    = value,
	                                                .properties = properties};
	return result;

}


PFarray_t*
getPFLA_qp_properties(
    XML2LALGContext* ctx,
    xmlNodePtr nodePtr)
{
    /*
     <properties>
      (<property name="foo" value="foo">...</property>)*
    </properties>
     */

	PFarray_t*	qp_properties = NULL;

	xmlXPathObjectPtr propertiesXPathObjPtr =
			PFxml2la_xpath_evalXPathFromNodeCtx(
					ctx->docXPathCtx, nodePtr, "/properties");

	unsigned int propertiesNodeCount =
			PFxml2la_xpath_getNodeCount(propertiesXPathObjPtr);

 	if (propertiesNodeCount == 1)
	{
 			xmlNodePtr propertiesNodePtr =
					PFxml2la_xpath_getNthNode(propertiesXPathObjPtr, 0);


			xmlXPathObjectPtr propertyXPathObjPtr =
					PFxml2la_xpath_evalXPathFromNodeCtx(
							ctx->docXPathCtx, propertiesNodePtr, "/property");

			unsigned int propertyNodeCount =
					PFxml2la_xpath_getNodeCount(propertyXPathObjPtr);

			if (propertyNodeCount > 0)
			{
				qp_properties = PFarray (sizeof (PFla_pb_item_property_t),
													propertyNodeCount);
				for (unsigned int i = 0; i < propertyNodeCount; i++)
				{
					xmlNodePtr propertyNodePtr =
							PFxml2la_xpath_getNthNode(propertyXPathObjPtr, i);

					PFla_pb_item_property_t property =
							getPFLA_qp_property(ctx, propertyNodePtr);

					*(PFla_pb_item_property_t*) PFarray_add (qp_properties) =
																	property;
				}
			}
	}

	return qp_properties;
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

/* vim:set shiftwidth=4 expandtab: */
