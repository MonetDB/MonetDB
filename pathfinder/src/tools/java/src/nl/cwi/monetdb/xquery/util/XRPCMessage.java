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
 * Portions created by CWI are Copyright (C) 1997-2007 CWI.
 * All Rights Reserved.
**/

package nl.cwi.monetdb.xquery.util;

import java.io.*;
import java.util.*;
import javax.xml.namespace.*;
import javax.xml.xpath.*;
import org.w3c.dom.*;
import org.xml.sax.*;

/**
 * @author Ying Zhang <Y.Zhang@cwi.nl>
 * @version 0.1
 */

public class XRPCMessage {
    public static final String XRPC_MSG_TYPE_REQ  = "request";
    public static final String XRPC_MSG_TYPE_RESP = "response";

    /**
     * Namespace definitions used in an XRPC message
     **/
	public static final String SOAP_NS  = "http://www.w3.org/2003/05/soap-envelope";
	public static final String XDT_NS   = "http://www.w3.org/2005/xpath-datatypes";
	public static final String XS_NS    = "http://www.w3.org/2001/XMLSchema";
	public static final String XSI_NS   = "http://www.w3.org/2001/XMLSchema-instance";
	public static final String XRPC_NS  = "http://monetdb.cwi.nl/XQuery";
	public static final String XRPC_LOC = "http://monetdb.cwi.nl/XQuery/XRPC.xsd";

    /**
     * Start and End of the messages
     **/
    public static final String XML_DECL =
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n";
    public static final String XRPC_MSG_START =
        "<env:Envelope" + 
        " xmlns:env=\"" + SOAP_NS + "\"" +
        " xmlns:xrpc=\"" + XRPC_NS + "\"" +
        " xmlns:xs=\"" + XS_NS + "\"" +
        " xmlns:xsi=\"" + XSI_NS + "\"" +
        " xsi:schemaLocation=\"" + XRPC_NS + " " + XRPC_LOC + "\">" +
        "<env:Body>";
    public static final String XRPC_REQUEST_END =
        "</xrpc:request></env:Body></env:Envelope>\n";
    public static final String XRPC_RESPONSE_END =
        "</xrpc:response></env:Body></env:Envelope>\n";
    public static final String SOAP_FAULT_START =
        XML_DECL +
        "<env:Envelope xmlns:env=\""+SOAP_NS+"\">" +
        "<env:Body><env:Fault>";
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

    public static String XRPC_REQUEST(String module,
                                      String location,
                                      String method,
                                      int arity,
                                      String body)
    {
        return xrpc_req(module, location, method, arity, 0,
                        null, body);
    }

    public static String XRPC_RESPONSE(String module,
                                       String method,
                                       String body)
    {
        return XML_DECL + XRPC_MSG_START +
               "<xrpc:response xrpc:module=\"" + module + "\"" +
                             " xrpc:method=\"" + method + "\">" +
               body + XRPC_RESPONSE_END;
    }

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

    /* a body consists of one or more calls */
    public static String XRPC_CALL(String params)
    {
        if (params == null || params.length() == 0){
            return "<xrpc:call/>";
        }
        return "<xrpc:call>" + params + "</xrpc:call>";
    }

    /* each parameter is an XQuery sequence */
    public static String XRPC_SEQ(String seq)
    {
        if (seq == null || seq.length() == 0){
            return "<xrpc:sequence/>";
        }
        return "<xrpc:sequence>" + seq + "</xrpc:sequence>";
    }

    /* Sequence values are either atomics of an "xs:"<type> or XML nodes
     * (e.g. elements, documents, attribute, comment, text,
     * processing-instruction, etc)
     */
    public static String XRPC_ATOM(String type, String value)
    {
        /* FIXME: do we need to escape the string value "value"?
         *        See the XRPC web client. */
        return "<xrpc:atomic-value xsi:type=\"xs:" + type + "\">" +
            value + "</xrpc:atomic-value>";
    }

    public static String XRPC_ELEMENT(String value)
    {
        return "<xrpc:element>" + value + "</xrpc:element>";
    }

    public static String XRPC_DOCUMENT(String value)
    {
        return "<xrpc:document>" + value + "</xrpc:document>";
    }

    public static String XRPC_TEXT(String value)
    {
        return "<xrpc:text>" + value + "</xrpc:text>";
    }

    public static String XRPC_COMMENT(String value)
    {
        return "<xrpc:comment>" + value + "</xrpc:comment>";
    }

    public static String XRPC_PI(String value)
    {
        return "<xrpc:processing-instruction>" + value +
               "</xrpc:processing-instruction>";
    }

    public static String XRPC_ATTRIBUTE(String attrName, String attrVal)
    {
        return "<xrpc:attribute " + attrName+"=\""+attrVal+"\" />";
    }

    /***********************************************/
    /*     Public Message Parsing Functions        */
    /***********************************************/

    /**
     * Find the prefix definition of the given namespace URI in an XRPC
     * message, by searching in the SOAP Envelope tag and the XRPC
     * request/response tag.
     *
     * Returns: the prefix, or throws XRPCException
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
            throw new XRPCSenderException(
                    "XRPC " + msgType + " message not well-formed:" +
                    "could not find the \"Envelope\" tag.");
        }
        end = msg.indexOf(">", (start + 8));
        if(end < 0){
            throw new XRPCSenderException(
                    "XRPC " + msgType + " message nog well-formed: " +
                    "could not find the end of the \"Envelope\" tag.");
        }

        /* Find the end of the tag msgType */
        end = msg.indexOf(msgType, end);
        if(end < 0){
            throw new XRPCSenderException(
                    "XRPC " + msgType + " message not well-formed:" +
                    "could not find the \"" + msgType + "\" tag.");
        }
        end = msg.indexOf(">", (end + 7));
        if(end < 0){
            throw new XRPCSenderException(
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
                    throw new XRPCSenderException(
                            "XRPC " + msgType + " message not " +
                            "well-formed: \"xmlns\" expected in a " +
                            "namespace declaration.");
                }
                /* Skip "xmlns:" */
                return str.substring(start + 6, end);
            }
            start += nsLen;
        } while(start < end);

        throw new XRPCSenderException(
                "XRPC " + msgType + " message nog well-formed: " +
                "declaration of the namespace \"" + namespaceURI +
                "\" not found.");
    }

    /**
     * Given a prefix and an XRPC message, find the Namespace URI it
     * represents.
     *
     * Returns: the Namespace URI, or throws XRPCException
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

    /* Retrieves the values of the parameters of a called XQuery
     * function into a String array.
     *
     * NOTE: the parameter values MUST be single-item sequences
     *       containing ONLY atomic values.
     */
    public static String[] getSingleItemParamValues(String request,
                                                    String soapPrefix,
                                                    String xrpcPrefix,
                                                    XPath xPath,
                                                    int arity)
        throws XRPCException
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
        } catch (Exception e){
            throw new XRPCSenderException(
                    "Failed to execute XPath query:" +
                    e.getClass().getName() +
                    ". Only found " + j + " of " + arity +
                    "parameter value(s).");
        }
        return values;
    }

    /* Evaluate 'xPathExpr' on 'xmlStr' using 'xPath' into a NodeSet,
     * which should contain only *one* node, then return all
     * attributes of the resulting node in a NamedNodeMap.
     *
     * Return a NamedNodeMap
     */
    public static NamedNodeMap getNodeAttributes(XPath xPath,
                                                 String xmlStr,
                                                 String xPathExpr)
        throws Exception
    {
        InputSource inputSource = new InputSource(new StringReader(xmlStr));
        NodeList nodeList = (NodeList) xPath.evaluate(xPathExpr,
                inputSource, XPathConstants.NODESET);
        if(nodeList.getLength() != 1){
            throw new XRPCException("Exactly 1 node is expected, got " +
                                    nodeList.getLength());
        }

        return nodeList.item(0).getAttributes();
    }

    /* Evaluate 'xPathExpr' on 'xmlStr' using 'xPath' into a NodeSet,
     * then retrieve the value of the attribute 'attrName' of each node
     * in the NodeSet.  With this function, one can, e.g., retrieve the
     * values of the ID attribute of all 'person' nodes.
     *
     * Return a String[] with the values of the attribute 'attrName'
     */
    public static String[] getNodeListAttribute(XPath xPath,
                                                String xmlStr,
                                                String xPathExpr,
                                                String attrName)
        throws Exception
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
