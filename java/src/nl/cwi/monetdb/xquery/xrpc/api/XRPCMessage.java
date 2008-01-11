/**
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html

 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and
 * limitations under the License.

 * The Original Code is the MonetDB Database System.

 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2008 CWI.
 * All Rights Reserved.
**/

package nl.cwi.monetdb.xquery.xrpc.api;

import java.io.*;
import java.util.*;
import javax.xml.namespace.*;
import javax.xml.xpath.*;
import org.w3c.dom.*;
import org.xml.sax.*;

/**
 * This class provides functions to generate XRPC request messages and
 * retrieve values from XRPC response messages.
 *
 * @author Ying Zhang <Y.Zhang@cwi.nl>
 * @version 0.1
 */

public class XRPCMessage {
    /**
     * XRPC request message type
     */
    public static final String XRPC_MSG_TYPE_REQ  = "request";
    /**
     * XRPC response message type
     */
    public static final String XRPC_MSG_TYPE_RESP = "response";

    /**
     * The SOAP Namespace 
     */
	public static final String SOAP_NS  = "http://www.w3.org/2003/05/soap-envelope";
    /**
     * The XPath Data Type Namespace
     */
	public static final String XDT_NS   = "http://www.w3.org/2005/xpath-datatypes";
    /**
     * The XML Schema Namespace
     */
	public static final String XS_NS    = "http://www.w3.org/2001/XMLSchema";
    /**
     * The XML Schema Instance Namespace
     */
	public static final String XSI_NS   = "http://www.w3.org/2001/XMLSchema-instance";
    /**
     * The XRPC Namespace
     */
	public static final String XRPC_NS  = "http://monetdb.cwi.nl/XQuery";
    /**
     * The location of the XRPC Namespace
     */
	public static final String XRPC_LOC = "http://monetdb.cwi.nl/XQuery/XRPC.xsd";

    /**
     * The XML declaration
     */
    public static final String XML_DECL =
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n";
    /**
     * Common header of XRPC (request/response) messages
     */
    public static final String XRPC_MSG_START =
        "<env:Envelope" + 
        " xmlns:env=\"" + SOAP_NS + "\"" +
        " xmlns:xrpc=\"" + XRPC_NS + "\"" +
        " xmlns:xs=\"" + XS_NS + "\"" +
        " xmlns:xsi=\"" + XSI_NS + "\"" +
        " xsi:schemaLocation=\"" + XRPC_NS + " " + XRPC_LOC + "\">" +
        "<env:Body>";
    /**
     * Footer of XRPC request messages
     */
    public static final String XRPC_REQUEST_END =
        "</xrpc:request></env:Body></env:Envelope>\n";
    /**
     * Footer of XRPC response  messages
     */
    public static final String XRPC_RESPONSE_END =
        "</xrpc:response></env:Body></env:Envelope>\n";
    /**
     * Header of SOAP Fault messages
     */
    public static final String SOAP_FAULT_START =
        XML_DECL +
        "<env:Envelope xmlns:env=\""+SOAP_NS+"\">" +
        "<env:Body><env:Fault>";
    /**
     * Footer of SOAP Fault messages
     */
    public static final String SOAP_FAULT_END =
        "</env:Fault></env:Body></env:Envelope>\n";

    /***********************************************/
    /*  Private Functions for Generating Messages  */
    /***********************************************/
    private static String xrpc_req(String module,
                                   String location,
                                   String method,
                                   int arity,
                                   int iterc,
                                   String updCall,
                                   String body)
    {
        StringBuffer header = new StringBuffer(512);
        header.append("<xrpc:request xrpc:module=\"");
        header.append(module);
        header.append("\" xrpc:location=\"");
        header.append(location);
        header.append("\" xrpc:method=\"");
        header.append(method);
        header.append("\" xrpc:arity=\"");
        header.append(arity);

        if (iterc > 0){
            header.append("\" xrpc:iter-count=\"");
            header.append(iterc);
        }
        if (updCall != null){
            header.append("\" xrpc:updCall=\"");
            header.append(updCall);
        }

        header.append("\">");

        return XML_DECL + XRPC_MSG_START + header + body +
               XRPC_REQUEST_END;
    }

    /***********************************************/
    /*     Public Message Generating Functions     */
    /***********************************************/

    /**
     * Constructs an XRPC request message (with all attributes defined
     * by the <a href="http://monetdb.cwi.nl/XQuery/XRPC.xsd">XRPC
     * schema</a>) with the information passed by the parameters.
     *
     * @param module Namespace URI of the XQuery module
     * @param location the location (i.e. at-hint) where the module file is stored.
     * @param method the called XQuery function
     * @param arity number of parameters the called function has
     * @param iterc number of iterations the called function should be executed
     * @param updCall indicates if the called function is read-only or updating
     * @param body body of the request message
     * @return an XRPC request message
     */
    public static String XRPC_REQUEST(String module,
                                      String location,
                                      String method,
                                      int arity,
                                      int iterc,
                                      boolean updCall,
                                      String body)
    {
        return xrpc_req(module, location, method, arity, iterc,
                        updCall?"true":"false", body);
    }

    /**
     * Constructs an XRPC request message (without the optional
     * attribute <code>updCall</code>) with the information passed by
     * the parameters.
     *
     * @param module Namespace URI of the XQuery module
     * @param location the location (i.e. at-hint) where the module file is stored.
     * @param method the called XQuery function
     * @param arity number of parameters the called function has
     * @param iterc number of iterations the called function should be executed
     * @param body body of the request message
     * @return an XRPC request message
     */
    public static String XRPC_REQUEST(String module,
                                      String location,
                                      String method,
                                      int arity,
                                      int iterc,
                                      String body)
    {
        return xrpc_req(module, location, method, arity, iterc,
                        null, body);
    }

    /**
     * Constructs an XRPC request message (without the optional
     * attribute <code>iter-count</code>) with the information passed by
     * the parameters.
     *
     * @param module Namespace URI of the XQuery module
     * @param location the location (i.e. at-hint) where the module file is stored.
     * @param method the called XQuery function
     * @param arity number of parameters the called function has
     * @param updCall indicates if the called function is read-only or updating
     * @param body body of the request message
     * @return an XRPC request message
     */
    public static String XRPC_REQUEST(String module,
                                      String location,
                                      String method,
                                      int arity,
                                      boolean updCall,
                                      String body)
    {
        return xrpc_req(module, location, method, arity, 0,
                        updCall?"true":"false", body);
    }

    /**
     * Constructs an XRPC request message (without the optional
     * attributes <code>updCall</code> and <code>iter-count</code>) with
     * the information passed by the parameters.
     *
     * @param module Namespace URI of the XQuery module
     * @param location the location (i.e. at-hint) where the module file is stored.
     * @param method the called XQuery function
     * @param arity number of parameters the called function has
     * @param body body of the request message
     * @return an XRPC request message
     */
    public static String XRPC_REQUEST(String module,
                                      String location,
                                      String method,
                                      int arity,
                                      String body)
    {
        return xrpc_req(module, location, method, arity, 0,
                        null, body);
    }

    /**
     * Constructs an XRPC response message with the information passed
     * by the parameters.
     *
     * @param module Namespace URI of the XQuery module
     * @param method the called XQuery function
     * @param body body of the response message
     * @return an XRPC response message
     */
    public static String XRPC_RESPONSE(String module,
                                       String method,
                                       String body)
    {
        return XML_DECL + XRPC_MSG_START +
               "<xrpc:response xrpc:module=\"" + module + "\"" +
                             " xrpc:method=\"" + method + "\">" +
               body + XRPC_RESPONSE_END;
    }

    /**
     * Constructs a SOAP Fault message with the information passed by
     * the parameters.
     * The message is constructed according to
     * <a
     * href="http://www.w3.org/TR/2007/REC-soap12-part1-20070427/">SOAP
     * Version 1.2 Part 1: Messaging Framework</a>.
     *
     * @param faultCode which side has caused the fault,
     * <code>env:Sender</code> or <code>env:Receiver</code>
     * @param faultReason some explanation of the fault.
     * @return a SOAP Fault message
     */
    public static String SOAP_FAULT(String faultCode,
                                    String faultReason)
    {
        return SOAP_FAULT_START +
            "<env:Code>" +
            "<env:Value>" + faultCode + "</env:Value>" +
            "</env:Code>" +
            "<env:Reason>" +
            "<env:Text xml:lang=\"en\">" + faultReason +
            "</env:Text>" +
            "</env:Reason>" +
            SOAP_FAULT_END;
    }

    /**
     * Constructs an <code>xrpc:call</code> element with the information
     * passed by the parameters.
     *
     * @param params parameters of one iteration of the called XQuery
     * function, consists of zero or more <code>xrpc:sequence</code>
     * element(s).
     * @return an <code>xrpc:call</code> element
     */
    public static String XRPC_CALL(String params)
    {
        if (params == null || params.length() == 0){
            return "<xrpc:call/>";
        }
        return "<xrpc:call>" + params + "</xrpc:call>";
    }

    /**
     * Constructs an <code>xrpc:sequence</code> element with the
     * information passed by the parameters.
     * Sequence values are either atomics of an <code>"xs:"<type></code>
     * or XML nodes (e.g. elements, documents, attribute, comment, text,
     * processing-instruction, etc)
     *
     * @param seq zero or more values element(s) of one parameter
     * sequence, each of which can be an atomic-typed or an XML
     * node-typed value.
     * @return an <code>xrpc:sequence</code> element
     */
    public static String XRPC_SEQ(String seq)
    {
        if (seq == null || seq.length() == 0){
            return "<xrpc:sequence/>";
        }
        return "<xrpc:sequence>" + seq + "</xrpc:sequence>";
    }

    /**
     * Constructs an <code>xrpc:atomic-value</code> element with the
     * information passed by the parameters.
     * The parameter <code>type</code> is used as the value of the
     * attribute <code>xsi:type</code>.
     *
     * @param type XQuery type of the atomic value
     * @param value String representation of an atomic value
     * @return an <code>xrpc:atomic-value</code> element
     */
    public static String XRPC_ATOM(String type, String value)
    {
        /* FIXME: do we need to escape the string value "value"?
         *        See the XRPC web client. */
        return "<xrpc:atomic-value xsi:type=\"xs:" + type + "\">" +
            value + "</xrpc:atomic-value>";
    }

    /**
     * Constructs an <code>xrpc:element</code> element with the
     * information passed by the parameters.
     *
     * @param value String representation of an XML element
     * @return an <code>xrpc:element</code> element
     */
    public static String XRPC_ELEMENT(String value)
    {
        return "<xrpc:element>" + value + "</xrpc:element>";
    }

    /**
     * Constructs an <code>xrpc:document</code> element with the
     * information passed by the parameters.
     *
     * @param value String representation of an XML document
     * @return an <code>xrpc:document</code> element
     */
    public static String XRPC_DOCUMENT(String value)
    {
        return "<xrpc:document>" + value + "</xrpc:document>";
    }

    /**
     * Constructs an <code>xrpc:text</code> element with the
     * information passed by the parameters.
     *
     * @param value the text string
     * @return an <code>xrpc:text</code> element
     */
    public static String XRPC_TEXT(String value)
    {
        return "<xrpc:text>" + value + "</xrpc:text>";
    }

    /**
     * Constructs an <code>xrpc:comment</code> element with the
     * information passed by the parameters.
     *
     * @param value String representation of an XQuery <code>comment</code> element
     * @return an <code>xrpc:comment</code> element
     */
    public static String XRPC_COMMENT(String value)
    {
        return "<xrpc:comment>" + value + "</xrpc:comment>";
    }

    /**
     * Constructs an <code>xrpc:processing-instruction</code> element
     * with the information passed by the parameters.
     *
     * @param value String representation of an XQuery
     * <code>processing-instruction</code> element
     * @return an <code>xrpc:processing-instruction</code> element
     */
    public static String XRPC_PI(String value)
    {
        return "<xrpc:processing-instruction>" + value +
               "</xrpc:processing-instruction>";
    }

    /**
     * Constructs an <code>xrpc:attribute</code> element with the
     * information passed by the parameters.
     *
     * @param attrName name of an attribute
     * @param attrVal value of the attribute
     * @return an <code>xrpc:attribute</code> element
     */
    public static String XRPC_ATTRIBUTE(String attrName, String attrVal)
    {
        return "<xrpc:attribute " + attrName+"=\""+attrVal+"\" />";
    }

    /***********************************************/
    /*     Public Message Parsing Functions        */
    /***********************************************/

    /**
     * Find the prefix definition of the given Namespace URI in an XRPC
     * message, by searching in the SOAP Envelope tag and the XRPC
     * request/response tag.
     *
     * @param msgType type of the message,
     * <code>XRPC_MSG_TYPE_REQ</code> or <code>XRPC_MSG_TYPE_RESP</code>
     * @param msg the message
     * @param namespaceURI URI of the Namespace
     * @return the prefix defined in the message for the Namespace
     * @throws XRPCException If the prefix could not be found, or the
     * XRPC message is not well-formed.
     **/
    public static String getNamespacePrefix(String msgType,
                                            String msg,
                                            String namespaceURI)
        throws XRPCException
    {
        int start = -1, end = -1;

        /* Find the start and end of the tag "Envelope" */
        start = msg.indexOf("Envelope");
        if(start < 0){
            throw new XRPCException(
                    "XRPC " + msgType + " message not well-formed:" +
                    "could not find the \"Envelope\" tag.");
        }
        end = msg.indexOf(">", (start + 8));
        if(end < 0){
            throw new XRPCException(
                    "XRPC " + msgType + " message nog well-formed: " +
                    "could not find the end of the \"Envelope\" tag.");
        }

        /* Find the end of the tag msgType */
        end = msg.indexOf(msgType, end);
        if(end < 0){
            throw new XRPCException(
                    "XRPC " + msgType + " message not well-formed:" +
                    "could not find the \"" + msgType + "\" tag.");
        }
        end = msg.indexOf(">", (end + 7));
        if(end < 0){
            throw new XRPCException(
                    "XRPC " + msgType + " message nog well-formed: " +
                    "could not find the end of the \"" +
                    msgType + "\" tag.");
        }

        /* Cut off the namespace declarations from the "Envelope" tag
         * until the end of the msgType tag and remove all white space
         * characters. */
        String str = msg.substring(start+9, end).replaceAll("[ \t\r\n]", "");
        start = 0;
        end = str.length();
        int nsLen = namespaceURI.length();
        do{
            start = str.indexOf(namespaceURI, start);
            if(str.charAt(start + nsLen) == '"'){
                /* search backward */
                end = str.lastIndexOf("=", start);
                start = str.lastIndexOf("\"", end) + 1;
                if(str.indexOf("xmlns",start) != start){
                    throw new XRPCException(
                            "XRPC " + msgType + " message not " +
                            "well-formed: \"xmlns\" expected in a " +
                            "namespace declaration.");
                }
                /* Skip "xmlns:" */
                return str.substring(start + 6, end);
            }
            start += nsLen;
        } while(start < end);

        throw new XRPCException(
                "XRPC " + msgType + " message nog well-formed: " +
                "declaration of the namespace \"" + namespaceURI +
                "\" not found.");
    }

    /**
     * Given a prefix and an XRPC message, find the Namespace URI the
     * prefix represents.
     *
     * @param msg the XRPC message
     * @param prefix prefix of the Namespace URI
     * @return the Namespace URI
     * @throws XRPCException If the prefix could not be found, or the
     * XRPC message is not well-formed.
     **/
    public static String getNamespaceURI(String msg,
                                         String prefix)
        throws XRPCException
    {
        String searchStr = "xmlns:" + prefix;

        int i = msg.indexOf(searchStr);
        if(i < 0){
            throw new XRPCException("Prefix definition not found: \"" +
                    prefix + "\"");
        }

        /* Skip whitespaces between "xmlns:"prefix and '=' */
        i += searchStr.length();
        while(Character.isWhitespace(msg.charAt(i))) i++;
        if(msg.charAt(i) != '=') {
            throw new XRPCException("Expected '=', found '" +
                    msg.charAt(i) + "'");
        }

        /* Skip whitespaces between '=' and '\"'*/
        i++;
        while(Character.isWhitespace(msg.charAt(i))) i++;
        if(msg.charAt(i) != '\"') {
            throw new XRPCException("Expected '\"', found '" +
                    msg.charAt(i) + "'");
        }

        i++; /* start of the namespace URI */
        int j = msg.indexOf("\"", i);
        if(j < 0) {
            throw new XRPCException("Could not find the end '\"' of " +
                    "the namespace URI string");
        }

        return msg.substring(i, j);
    }

    /**
     * Retrieves the values of the parameters of the called XQuery
     * function contained in an XRPC request message into a String
     * array.
     * Note that: the parameter values <b>must</b> be single-item
     * sequences containing <b>only</b> atomic values.
     *
     * @param request an XRPC request message
     * @param soapPrefix the prefix of the SOAP namespace defined in the
     * XRPC message
     * @param xrpcPrefix the prefix of the XRPC namespace defined in the
     * XRPC message
     * @param xPath an XPath object used to evaluate XPath expression on
     * the given XRPC requst message to retrieve parameter values
     * @param arity number of parameters the called function has
     * @return values of the parameters
     * @throws XRPCSenderException If the parameter values could not be
     * found, because the execution of XPath expression failed
     */
    public static String[] getSingleItemParamValues(String request,
                                                    String soapPrefix,
                                                    String xrpcPrefix,
                                                    XPath xPath,
                                                    int arity)
        throws XRPCSenderException
    {
        String[] values = new String[arity];
        int j = 1;

        /* TODO: may be we can use NodeSet to replace multiple calls to
         *       'evaluate'. */
        try {
            for(int i = 0; i < arity; i++, j++){
                values[i] = xPath.evaluate(
                        "/" + soapPrefix + ":Envelope/" +
                              soapPrefix + ":Body/" +
                              xrpcPrefix + ":request/" +
                              xrpcPrefix + ":call/" +
                              xrpcPrefix + ":sequence[" + j + "]/" +
                              xrpcPrefix + ":atomic-value/text()",
                        new InputSource(new StringReader(request)));
            }
        } catch (XPathExpressionException e){
            throw new XRPCSenderException(
                    "Failed to execute XPath query:" +
                    e.getClass().getName() +
                    ". Only found " + j + " of " + arity +
                    "parameter value(s).");
        }
        return values;
    }

    /**
     * Finds all attribute nodes belonging to a particular node in an
     * XML string.
     * This function first evaluates <code>xPathExpr</code> on
     * <code>xmlStr</code> using <code>xPath</code> into a
     * <code>NodeSet</code>, which should contain only <b>one</b> node,
     * then returns all attributes of the resulting node in a
     * <code>NamedNodeMap</code>.
     *
     * @param xPath an XPath object, which is used to execute the XPath
     * expression <code>xPathExpr</code>
     * @param xmlStr the XML data as a <code>String</code> to work on
     * @param xPathExpr the XPath expression that will be evaluated
     * @return A <code>NamedNodeMap</code> that contains all attribute
     * nodes of the node that is returned by evaluating the given
     * <code>XPath</code> expression <code>xPathExpr</code> on the given
     * XML data <code>xmlStr</code>.
     * @throws XRPCException If the result of evaluating
     * <code>xPathExpr</code> is not exactly one node
     */
    public static NamedNodeMap getNodeAttributes(XPath xPath,
                                                 String xmlStr,
                                                 String xPathExpr)
        throws XRPCException
    {
        InputSource inputSource = new InputSource(new StringReader(xmlStr));
        NodeList nodeList;
        try{
            nodeList = (NodeList) xPath.evaluate(xPathExpr, inputSource,
                    XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            throw new XRPCException("Exactly 1 node is expected, " +
                    "got XPathExpressionException instead:" +
                    e.getMessage());
        }
        if(nodeList.getLength() != 1){
            throw new XRPCException("Exactly 1 node is expected, got " +
                                    nodeList.getLength());
        }

        return nodeList.item(0).getAttributes();
    }

    /**
     * Finds values of the same attribute with name
     * <code>attrName</code> in all nodes selected by the XPath
     * expression <code>xPathExpr</code>.
     * This function first evaluates <code>xPathExpr</code> on
     * <code>xmlStr</code> using <code>xPath</code> into a
     * <code>NodeSet</code>, which can contain one or more nodes, then
     * returns all values of the attribute <code>attrName</code> of the
     * resulting nodes in the <code>NodeSet</code>.
     *
     * @param xPath an XPath object, which is used to execute the XPath
     * expression <code>xPathExpr</code>
     * @param xmlStr the XML data as a <code>String</code> to work on
     * @param xPathExpr the XPath expression that will be evaluated
     * @param attrName the name of the attribute, which values should be
     * retrieved.
     * @return A <code>NamedNodeMap</code> that contains all attribute
     * nodes of the node that is returned by evaluating the given
     * <code>XPath</code> expression <code>xPathExpr</code> on the given
     * XML data <code>xmlStr</code>.
     * @throws XPathExpressionException If the evaluating of
     * <code>xPathExpr</code> failed
     */
    public static String[] getNodeListAttribute(XPath xPath,
                                                String xmlStr,
                                                String xPathExpr,
                                                String attrName)
        throws XPathExpressionException
    {
        InputSource inputSource = new InputSource(new StringReader(xmlStr));
        NodeList nodeList = (NodeList) xPath.evaluate(xPathExpr,
                inputSource, XPathConstants.NODESET);

        String[] attrValues= new String[nodeList.getLength()];
        for(int i = 0; i < nodeList.getLength(); i++){
            Node n = nodeList.item(i);
            NamedNodeMap nodeMap = n.getAttributes();
            Node attrNode = nodeMap.getNamedItem(attrName);
            attrValues[i] = attrNode.getNodeValue();
        }

        return attrValues;
    }
}
