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

package nl.cwi.monetdb.xrpc.wrapper;

import java.io.*;
import java.net.*;
import nl.cwi.monetdb.util.*;

public class XrpcWrapperWorker extends Thread{
    public static final String XRPCD_CALLBACK = "/xrpc";
    public static final String SOAP_NS =
        "http://www.w3.org/2003/05/soap-envelope";
    public static final String XDT_NS =
        "http://www.w3.org/2005/xpath-datatypes";
    public static final String XS_NS =
        "http://www.w3.org/2001/XMLSchema";
    public static final String XSI_NS =
        "http://www.w3.org/2001/XMLSchema-instance";
    public static final String XRPC_NS =
        "http://monetdb.cwi.nl/XQuery";
    public static final String XRPC_LOC =
        "http://monetdb.cwi.nl/XQuery/XRPC.xsd";
    public static final String SOAP_BODY_START =
        "<env:Envelope xsi:schemaLocation=\"" + XRPC_NS + " " +
                                                XRPC_LOC + "\"> \n" +
          "<env:Body> \n";
    public static final String SOAP_BODY_END =
          "</env:Body>\n" +
        "</env:Envelope>\n";
    public static final String SOAP_FAULT_START =
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
        "<env:Envelope xmlns:env=\""+SOAP_NS+"\">\n" +
          "<env:Body>\n" +
            "<env:Fault>\n";
    public static final String SOAP_FAULT_END =
            "</env:Fault>\n" +
          "</env:Body>\n" +
        "</env:Envelope>";
    public static final String HTTP_OK_HEADER =
        "HTTP/1.1 200 OK\r\n" +
        "Content-type: text/xml; charset=\"utf-8\"\r\n\r\n";
    public static final String HTTP_ERR_400_HEADER =
        "HTTP/1.1 400 Bad Request\r\n" +
        "Content-type: text/xml; charset=\"utf-8\"\r\n\r\n";
    public static final String HTTP_ERR_500_HEADER =
        "HTTP/1.1 500 Internal Server Error\r\n" +
        "Content-type: text/xml; charset=\"utf-8\"\r\n\r\n";

    class SenderException extends Exception {
        public SenderException(String reason) {
            super(reason);
        }
    }

    class ReceiverException extends Exception {
        public ReceiverException(String reason) {
            super(reason);
        }
    }

    private Socket sock;
    private CmdLineOpts opts;
    private long tid;
    private String rootdir;
    private int contentLength;
    private boolean debug;

    XrpcWrapperWorker(Socket s, CmdLineOpts o)
        throws Exception
    {
        sock = s;
        opts = o;
        tid = this.getId();
        rootdir = o.getOption("rootdir").getArgument();
        debug = o.getOption("debug").isPresent();
    }

    private void DEBUG(String msg)
    {
        if(debug) System.out.print(msg);
    }

    private void sendError(Writer sockOut,
                           String httpErrorHeader,
                           String faultCode,
                           InputStream errorStream,
                           String errMsg)
    {
        String errHeader = "ERROR: (TID "+tid+") sendError(): ";

        try{
            DEBUG(errHeader + "SOAP Fault message to send:\n" +
                    httpErrorHeader + SOAP_FAULT_START +
                    "<env:Code>\n" +
                      "<env:Value>" + faultCode + "</env:Value>\n" +
                    "</env:Code>\n" +
                    "<env:Reason>\n" +
                      "<env:Text xml:lang=\"en\">\n");

            sockOut.write(httpErrorHeader + SOAP_FAULT_START +
                    "<env:Code>\n" +
                      "<env:Value>" + faultCode + "</env:Value>\n" +
                    "</env:Code>\n" +
                    "<env:Reason>\n" +
                      "<env:Text xml:lang=\"en\">");

            if(errMsg != null) {
                sockOut.write(errMsg);
                DEBUG(errMsg);
            } else {
                if(errorStream == null) {
                    throw new NullPointerException(
                            "Either an InputStream or a String must " +
                            "be specified.");
                }

                BufferedReader errIn = new BufferedReader(new
                        InputStreamReader(errorStream));
                int c;
                while((c = errIn.read()) >= 0) {
                    sockOut.write(c);
                    DEBUG(new Character((char)c).toString());
                }
                errIn.close();
            }
            sockOut.write("\n</env:Text>\n</env:Reason>\n" +
                    SOAP_FAULT_END);
            DEBUG("\n</env:Text>\n</env:Reason>\n" + SOAP_FAULT_END);
        } catch (Exception e) {
            System.err.println(errHeader + "caught exception.\n");
            e.printStackTrace();
        }
    }

    /**
     * Read the HTTP header of a request and validate the header.
     *
     **/
    private void handleHttpReqHeader(BufferedReader sockIn)
        throws Exception
    {
        String infoHeader = "INFO: (TID "+tid+") handleHttpReqHeader(): ";
        String errHeader = "ERROR: (TID "+tid+") handleHttpReqHeader(): ";

        boolean foundPostHeader = false, foundClHeader = false;

        DEBUG(infoHeader + "HTTP header of XRPC request:\n");
        String ln = sockIn.readLine();
        /* TODO: should check 'HOST' as well! */
        while(ln.length() > 0){
            DEBUG(infoHeader + ln + "\n");
            if(ln.startsWith("POST")){
                if(!ln.startsWith(XRPCD_CALLBACK, 5)){
                    throw new SenderException(
                            "Unsupported Request: \"" + ln + "\"");
                }
                foundPostHeader = true;
            } else if(ln.startsWith("Content-Length:")) {
                try{
                    contentLength = Integer.parseInt(ln.substring(16));
                } catch(NumberFormatException e) {
                    throw new SenderException("Invalid value of " +
                            "\"Content-Length\": \"" +
                            ln.substring(16) + "\": " + e.getMessage());
                }
                foundClHeader = true;
            }
            ln = sockIn.readLine();
        }

        if(!foundPostHeader){
            throw new SenderException("HTTP header does not contain " +
                    "a \"POST\" method definition.");
        } else if (!foundClHeader){
            throw new SenderException("HTTP header does not contain " +
                    "the mandatory \"Content-Length\" field.");
        }
    }

    /**
     * Read an XRPC request and store it on disk under the given
     * filename.  This function also stores the request header (from the
     * "Envelope" tag until the "request" tag) in a StringBuffer so that
     * the attribute values can be retrieved.
     *
     * Returns: a StringBuffer if no error occurred;
     *          throws a new Exception otherwise.
     **/
    private StringBuffer storeXrpcRequest(BufferedReader sockIn,
                                          String filename)
        throws Exception
    {
        String infoHeader = "INFO: (TID "+tid+") getXrpcRequest(): ";
        String errHeader = "ERROR: (TID "+tid+") getXrpcRequest(): ";

        StringBuffer buf = new
            StringBuffer(XrpcWrapper.DEFAULT_BUFSIZE);
        BufferedWriter fileOut = new
            BufferedWriter(new FileWriter(filename, false));
        int len = 0, index = -1;

        DEBUG(infoHeader + "reading XRPC request...\n");

        while (buf.length() < contentLength && index < 0 ) {
            for(int i = 0 ; i < 10; i++)
                buf.append((char)(sockIn.read()));
            index = buf.indexOf("request");
        }

        /* find the closing symbol of the "request" tag */
        index = buf.indexOf(">", index + 7);
        while (buf.length() < contentLength && 
               buf.charAt(buf.length()-1) != '>'){
            buf.append((char)(sockIn.read()));
        }
        fileOut.write(buf.toString());

        len = buf.length();
        while (len < contentLength){
            fileOut.write(sockIn.read());
            len++;
        }
        fileOut.close();

        if (len != contentLength) {
            new File(filename).delete();
            throw new SenderException("bytes received: " + len +
                                      "should be: " + contentLength);
        }

        DEBUG(infoHeader + "request (" + len + " bytes) stored in: "
                + filename + "\n");

        /* TODO: remove the temporary file 'filename' if necessary */
        return buf;
    }

    /**
     * Retrieve the value of given 'attribute' from the given
     * 'request'.
     *
     * Returns: a new string containing the value of the attribute;
     *          throws Exception if error occurred.
     **/
    private String getAttributeValue(StringBuffer request,
                                     String attribute)
        throws Exception
    {
        String infoHeader = "INFO: (TID "+tid+") getAttributeValue(): ";
        String errHeader = "ERROR: (TID "+tid+") getAttributeValue(): ";

        int start, end;

        start = request.indexOf(attribute);
        if(start < 0){
            throw new SenderException("invalid XRPC request: could " +
                    "not find the attribute \"" + attribute + "\".");
        }
        start += attribute.length();
        start = request.indexOf("\"", start);
        if(start < 0){
            throw new SenderException("invalid XRPC request: " +
                    "attribute \"" + attribute +
                    "\" does not have a string value.");
        }
        end = request.indexOf("\"", (++start));
        if(end < 0){
            throw new SenderException("invalid XRPC request: " +
                    "attribute \"" + attribute +
                    "\" does not have a string value.");
        }

        DEBUG(infoHeader + attribute + " = " +
              request.substring(start, end) + "\n");
        return request.substring(start, end);
    }

    /**
     * Find the prefix of the XRPC namespace
     *
     * Returns: the prefix if succeeded,
     *          throws new Exception otherwise.
     **/
    private String getXrpcPrefix(StringBuffer request)
        throws Exception
    {
        String infoHeader = "INFO: (TID "+tid+") getXrpcPrefix(): ";
        String errHeader = "ERROR: (TID "+tid+") getXrpcPrefix(): ";
        int nsLen = XRPC_NS.length();

        int start = request.indexOf("Envelope");
        if(start < 0){
            throw new SenderException(
                    "XRPC request message not well-formed:" +
                    "could not find the \"Envelope\" tag.");
        }

        int end = request.indexOf(">", (start += 8));
        if(end < 0){
            throw new SenderException(
                    "XRPC request message nog well-formed: " +
                    "could not find the end of the \"Envelope\" tag.");
        }

        /* Cut off the name space declarations in the "Envelope" tag and
         * remove all white space characters. */
        String str = request.substring(start, end).replaceAll("[ \t\r\n]", "");
        start = 0;
        end = str.length();
        do{
            start = str.indexOf(XRPC_NS, start);
            if(str.charAt(start + nsLen) == '"'){
                end = str.lastIndexOf("=", start);
                start = str.lastIndexOf("\"", end) + 1;
                if(str.indexOf("xmlns",start) != start){
                    throw new SenderException(
                            "XRPC request message nog well-formed: " +
                            "\"xmlns\" expected in a namespace declaration.");
                }
                DEBUG(infoHeader + "found XRPC namespace identifier: " +
                        str.substring(start + 6, end) + "\n");
                return str.substring(start + 6, end);
            }
            start += nsLen;
        } while(start < end);

        throw new SenderException(
                "XRPC request message nog well-formed: " +
                "declaration of the XRPC namespace \"" + XRPC_NS +
                "\" not found.");
    }

    /**
     * Generate an XQuery query for the request and write the query
     * to a file.
     **/
    private void generateQuery(String requestFilename,
                              String queryFilename,
                              StringBuffer requestHeader)
        throws Exception
    {
        String infoHeader = "INFO: (TID "+tid+") generateQuery(): ";
        String errHeader = "ERROR: (TID "+tid+") generateQuery(): ";

        String xrpcPrefix = getXrpcPrefix(requestHeader);
        String xqModule = getAttributeValue(requestHeader, xrpcPrefix+":module");
        String xqMethod = getAttributeValue(requestHeader, xrpcPrefix+":method");
        String xqLocation = getAttributeValue(requestHeader, xrpcPrefix+":location");
        String arityStr = getAttributeValue(requestHeader, "xrpc:arity");
        long arity = Long.parseLong(arityStr);

        try{ 
            FileWriter fileOut = new FileWriter(queryFilename, false);

            fileOut.write(
                    "import module namespace modfunc = \"" + xqModule + "\"\n" +
                    "            at \"" + xqLocation + "\";\n\n" +
                    "import module namespace wf = \"xrpcwrapper\" \n" +
                    "       at \"" + rootdir + XrpcWrapper.WF_FILE + "\";\n\n" +
                    "declare namespace env=\"" + SOAP_NS + "\";\n" +
                    "declare namespace xs=\"" + XS_NS + "\";\n\n" +
                    "declare namespace xrpc=\"" + XRPC_NS + "\";\n\n" +
                    SOAP_BODY_START + 
                    "<xrpc:response xrpc:module=\"" + xqModule + "\" " +
                    "xrpc:method=\"" + xqMethod + "\">{\n" +
                    "  for $call in doc(\"" + requestFilename + "\")" +
                    "//" + xrpcPrefix + ":call\n");
            for(int i = 1; i <= arity; i++){
                /* XQuery index of item sequences starts from 1 */
                fileOut.write("  let $param" + i + " := " +
                        "wf:n2s(\"" + xrpcPrefix + "\", " +
                                "$call/" + xrpcPrefix + ":sequence["+i+"]" +
                        ")\n");
            }
            fileOut.write("  return wf:s2n(modfunc:"+xqMethod+"(");
            for(int i = 1; i <= (arity - 1); i++){
                fileOut.write("$param" + i + ", ");
            }
            if(arity > 0){
                fileOut.write("$param" + arity);
            }
            fileOut.write("))\n}</xrpc:response>\n" + SOAP_BODY_END);
            fileOut.close();
        } catch (Exception e){
            throw new ReceiverException(
                    "failed to generate query for the request.");
        }

        DEBUG(infoHeader + "query generated in "+queryFilename+"\n");
    }

    /**
     * Call the XQuery engine to execute the query and send the
     * results back
     **/
    private void execAndSend(String queryFilename,
                             BufferedWriter sockOut)
        throws Exception
    {
        String infoHeader = "INFO: (TID "+tid+") execAndSend(): ";
        String errHeader = "ERROR: (TID "+tid+") execAndSend(): ";
        int ret = -1, len = -1;
        char[] cbuf = new char[XrpcWrapper.MIN_BUFSIZE];
        String command = null;
        Process proc = null;

        try{
            command = opts.getOption("command").getArgument() + " " +
                queryFilename;
            DEBUG(infoHeader + "executing: " + command + "\n");
            proc = Runtime.getRuntime().exec(command);
        } catch (OptionsException oe) {
            throw new ReceiverException("This should not happen: " +
                    "XRPC wrapper does not know how to execute the " +
                    "XQuery engine: " + oe.getMessage());
        } catch (Exception e){
            throw new ReceiverException("Error occurred during " +
                    "execution of command \"" + command + "\":" +
                    e.getMessage());
        }

        BufferedReader procIn = new BufferedReader(new
                InputStreamReader(proc.getInputStream()));
        try{
            len = procIn.read(cbuf, 0, XrpcWrapper.MIN_BUFSIZE);
        } catch (Exception e){
            /* Don't throw exception, try to read proc's ErrorStream */
            len = -1;
        }

        if (len >= 0){
            DEBUG(infoHeader + "query execution seems succeeded: " +
                    "got output from the process' InputStream.\n");
            DEBUG(infoHeader + "sending XRPC response message:\n");

            sockOut.write(HTTP_OK_HEADER);
            sockOut.write(new String(cbuf, 0, len));

            DEBUG(HTTP_OK_HEADER);
            DEBUG(new String(cbuf, 0, len));

            int c;
            while ( (c = procIn.read()) >= 0) {
                sockOut.write(c);
                DEBUG(new Character((char)c).toString());
            }
        } else { /* send SOAP Fault message */
            sendError(sockOut, HTTP_ERR_500_HEADER, "env:Receiver",
                    proc.getErrorStream(), null);
        }

        ret = proc.waitFor();
        DEBUG(infoHeader + "query execution exits with: " + ret + "\n");
    }

    /**
     * Main function to handle an XRPC request:
     *  1. read the request and write it to a file
     *  2. generate an XQuery query for the request and write the
     *     query to a file
     *  3. Call the XQuery engine to execute the query and send the
     *     results back
     **/
    private void handleXrpcReq(BufferedReader sockIn, BufferedWriter sockOut)
        throws Exception
    {
        String infoHeader = "INFO: (TID "+tid+") handleXrpcReq(): ";
        String warnHeader = "WARNING: (TID "+tid+") handleXrpcReq(): ";

        String requestFilename = rootdir+"xrpc_request_"+tid+".xml";
        String queryFilename = rootdir+"xrpc_wrapper_query_"+tid+".xq";

        StringBuffer requestHeader = storeXrpcRequest(sockIn, requestFilename);
        generateQuery(requestFilename, queryFilename, requestHeader);
        execAndSend(queryFilename, sockOut);
        DEBUG(infoHeader + "DONE: " + sock + "\n\n");

        try {
            File fQ = new File(queryFilename);
            File fR = new File(requestFilename);

            if(!opts.getOption("keep").isPresent()){
                fQ.delete();
                fR.delete();
            } else {
                String arg = opts.getOption("keep").getArgument();
                if(arg.equals("request")) {
                    fQ.delete();
                } else if(arg.equals("query")) {
                    fR.delete();
                } 
            }
        } catch (Exception e) {
            System.out.println(warnHeader +
                    "failed to delete temporary file(s):");
            e.printStackTrace();
        }
    }

    public void run()
    {
        String warnHeader = "WARNING: (TID "+tid+") run(): ";
        String errHeader = "ERROR: (TID "+tid+") run(): ";

        BufferedReader sockIn = null;
        BufferedWriter sockOut = null;

        try{
            sockIn = new BufferedReader(new
                    InputStreamReader(sock.getInputStream()));
            sockOut = new BufferedWriter(new
                    OutputStreamWriter(sock.getOutputStream()));

            handleHttpReqHeader(sockIn);
            handleXrpcReq(sockIn, sockOut);
        } catch (SenderException se) {
            sendError(sockOut, HTTP_ERR_400_HEADER, "env:Sender",
                        null, se.getMessage());
        } catch (ReceiverException re) {
            sendError(sockOut, HTTP_ERR_500_HEADER, "env:Receiver",
                        null, re.getMessage());
        } catch (Exception e1){
            System.err.println(errHeader + "caught exception:");
            e1.printStackTrace();
        } finally {
            try{
                sockOut.close();
                sockIn.close();
                sock.close();
            } catch (Exception e2) {
                System.err.println(warnHeader +
                        "caught exception in FINAL block:");
                e2.printStackTrace();
            }
        }
    }
}
