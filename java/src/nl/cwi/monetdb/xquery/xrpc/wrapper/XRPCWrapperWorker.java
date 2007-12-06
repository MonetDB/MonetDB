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

package nl.cwi.monetdb.xquery.xrpc.wrapper;

import java.io.*;
import java.net.*;
import javax.xml.xpath.*;
import org.w3c.dom.*;

import nl.cwi.monetdb.util.*;
import nl.cwi.monetdb.xquery.xrpc.api.*;

public class XRPCWrapperWorker extends Thread {
	/** Simple counter for XRPC worker threads. */
	private static int tid = 0;

	private Socket sock;
	private CmdLineOpts opts;
	private String rootdir;
	private String soapPrefix;
	private String xrpcPrefix;
	private int contentLength;
	private boolean debug;

	XRPCWrapperWorker(Socket s, CmdLineOpts o)
		throws OptionsException
	{
		super("XRPCWrapperWorkerThread-" + tid++);
		sock = s;
		opts = o;
		rootdir = o.getOption("rootdir").getArgument();
		debug = o.getOption("debug").isPresent();
        soapPrefix = "";
        xrpcPrefix = "";
	}

	private void DEBUG(String msg)
	{
		if(debug) System.out.print(msg);
	}

	/**
	 * Generate an XQuery query for the request and write the query
	 * to a file.
	 */
	private void generateQuery(String requestFilename,
			                   String queryFilename,
                               String request)
		throws XRPCException
	{
		String infoHeader = "INFO: ("+getName()+") generateQuery(): ";
		String errHeader = "ERROR: ("+getName()+") generateQuery(): ";

        try {
            soapPrefix = XRPCMessage.getNamespacePrefix(
                    XRPCMessage.XRPC_MSG_TYPE_REQ, request,
                    XRPCMessage.SOAP_NS);
            xrpcPrefix = XRPCMessage.getNamespacePrefix(
                    XRPCMessage.XRPC_MSG_TYPE_REQ, request,
                    XRPCMessage.XRPC_NS);
        } catch (XRPCException xe){
            throw new XRPCSenderException(xe.getMessage());
        }

        NamespaceContextImpl namespaceContext = new NamespaceContextImpl();
        namespaceContext.add(soapPrefix, XRPCMessage.SOAP_NS);
        namespaceContext.add(xrpcPrefix, XRPCMessage.XRPC_NS);

        XPathFactory factory = XPathFactory.newInstance();
        XPath xPath = factory.newXPath();
        xPath.setNamespaceContext(namespaceContext);

        String xPathExpr = "/" + soapPrefix + ":Envelope/" +
                                 soapPrefix + ":Body/" +
                                 xrpcPrefix + ":request";
        NamedNodeMap attrs;
        try{
            attrs = XRPCMessage.getNodeAttributes(xPath, request, xPathExpr);
        } catch (XRPCException xe){
            throw new XRPCSenderException(xe.getMessage());
        }

		String xqModule = attrs.getNamedItem(xrpcPrefix+":module").getNodeValue();
		String xqMethod = attrs.getNamedItem(xrpcPrefix+":method").getNodeValue();
		String xqLocation = attrs.getNamedItem(xrpcPrefix+":location").getNodeValue();
		String arityStr = attrs.getNamedItem(xrpcPrefix+":arity").getNodeValue();

		int arity = -1;
        try{
            arity = Integer.parseInt(arityStr);
        } catch(NumberFormatException nfe) {
            throw new XRPCSenderException("Invalid value of the " +
                    "\"arity\" attribute: \"" + arityStr + "\": " +
                    nfe.getMessage());
        }

		try{ 
			FileWriter fileOut = new FileWriter(queryFilename, false);

			fileOut.write(
					"import module namespace modfunc = \"" + xqModule + "\"" +
                        " at \"" + xqLocation + "\";\n\n" +
					"import module namespace wf = \"xrpcwrapper\"" +
                        " at \"" + rootdir + XRPCWrapper.WF_FILE + "\";\n\n" +
					"declare namespace env=\"" + XRPCMessage.SOAP_NS + "\";\n" +
					"declare namespace xrpc=\"" + XRPCMessage.XRPC_NS + "\";\n\n" +
					"declare namespace xs=\"" + XRPCMessage.XS_NS + "\";\n" +
                    
                    "<env:Envelope" + 
                    " xsi:schemaLocation=\"" +
                        XRPCMessage.XRPC_NS + " " +
                        XRPCMessage.XRPC_LOC + "\">" +
                    "<env:Body>" +
					"<xrpc:response" +
                    " xrpc:module=\"" + xqModule + "\" " +
					" xrpc:method=\"" + xqMethod + "\">{\n" +
					"  for $call in doc(\"" + requestFilename + "\")" +
					"//" + xrpcPrefix + ":call\n");
			for(int i = 1; i <= arity; i++){
				/* NB: XQuery index of item sequences starts from 1 */
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
			fileOut.write("))\n}" + XRPCMessage.XRPC_RESPONSE_END);
			fileOut.close();
		} catch (IOException ioe){
			throw new XRPCReceiverException(
                    "failed to generate query for the request: " +
                    ioe.getMessage());
		}

		DEBUG(infoHeader + "query generated in "+queryFilename+"\n");
	}

	/**
	 * Call the XQuery engine to execute the query and send the
	 * results back.
	 */
	private void execAndSend(String queryFilename,
			BufferedWriter sockOut)
		throws XRPCReceiverException
	{
		String infoHeader = "INFO: ("+getName()+") execAndSend(): ";
		String errHeader = "ERROR: ("+getName()+") execAndSend(): ";
		String command = null;
		Process proc = null;

		try{
			command = opts.getOption("command").getArgument() + " " +
				queryFilename;
			DEBUG(infoHeader + "executing: " + command + "\n");
			proc = Runtime.getRuntime().exec(command);
		} catch (OptionsException oe) {
            /* This exception should never happen */
			throw new XRPCReceiverException("This should not happen: " +
					"XRPC wrapper does not know how to execute the " +
					"XQuery engine: " + oe.getMessage());
		} catch (IOException ioe){
			throw new XRPCReceiverException(
                    "Failed to start executing command \"" +
                    command + "\":" + ioe.getMessage());
		}

		BufferedReader procIn = new BufferedReader(new
				InputStreamReader(proc.getInputStream()));
		int c = -1;
		try{
			c = procIn.read();
		} catch (IOException ioe){
            System.out.println(infoHeader + "caught IOException " +
                    "while reading proc's output:");
            ioe.printStackTrace();
			/* Don't throw exception, try to read proc's ErrorStream */
			c = -1;
		}

		if (c >= 0){
            try{
                DEBUG(infoHeader + "query execution seems succeeded: " +
                        "got output from the process' InputStream.\n");
                DEBUG(infoHeader + "sending XRPC response message:\n");

                sockOut.write(XRPCHTTPConnection.HTTP_OK_HEADER);
                DEBUG(XRPCHTTPConnection.HTTP_OK_HEADER);

                /* Add the XML declaration, if necessary */
                char[] cbuf = new char[4];
                procIn.read(cbuf, 0, 4);

                if((char)c != '<' || cbuf[0] != '?' || cbuf[1] != 'x' || cbuf[2] != 'm' || cbuf[3] != 'l') {
                    sockOut.write(XRPCMessage.XML_DECL);
                    DEBUG(XRPCMessage.XML_DECL);
                }

                sockOut.write(new Character((char)c).toString());
                sockOut.write(cbuf, 0, 4);
                DEBUG(new Character((char)c).toString());
                DEBUG("" + cbuf[0] + cbuf[1] + cbuf[2] + cbuf[3]);

                while ((c = procIn.read()) >= 0) {
                    sockOut.write(new Character((char)c).toString());
                    DEBUG(new Character((char)c).toString());
                }
            } catch (IOException ioe){
                throw new XRPCReceiverException(
                        "Error occurred while reading query results: " +
                        ioe.getMessage());
            }
		} else { /* send SOAP Fault message */
            try {
                StringBuffer faultReason = new StringBuffer(8192);
                BufferedReader errIn = new BufferedReader(new
                        InputStreamReader(proc.getErrorStream()));
                while ((c = errIn.read()) >= 0) {
                    faultReason.append(new Character((char)c).toString());
                }
                errIn.close();

                String faultMsg =
                    XRPCMessage.SOAP_FAULT(soapPrefix+":Receiver",
                            faultReason.toString());
                DEBUG(XRPCHTTPConnection.HTTP_ERR_500_HEADER + faultMsg);
                XRPCHTTPConnection.send(sockOut,
                        XRPCHTTPConnection.HTTP_ERR_500_HEADER, faultMsg);
            } catch (IOException ioe){
                System.err.println(errHeader + "caught exception:");
                ioe.printStackTrace();
                System.exit(1);
            }
		}

        try{
            int ret = proc.waitFor();
            DEBUG(infoHeader + "query execution exits with: " + ret + "\n");
        } catch (InterruptedException ie){
            System.err.println(errHeader + "caught exception:");
            ie.printStackTrace();
            System.exit(1);
        }
	}

	/**
	 * Main function to handle an XRPC request:
	 *  1. read the request and write it to a file
	 *  2. generate an XQuery query for the request and write the
	 *     query to a file
	 *  3. Call the XQuery engine to execute the query and send the
	 *     results back
	 */
	private void handleXRPCReq(String request, BufferedWriter sockOut)
		throws XRPCException
	{
		String infoHeader = "INFO: ("+getName()+") handleXRPCReq(): ";
		String warnHeader = "WARNING: ("+getName()+") handleXRPCReq(): ";

		String requestFilename = rootdir+"xrpc_request_"+getName()+".xml";
		String queryFilename = rootdir+"xrpc_wrapper_query_"+getName()+".xq";

        try {
            BufferedWriter fileOut = new BufferedWriter(new FileWriter(requestFilename, false));
            fileOut.write(request.toString());
            fileOut.close();
            DEBUG(infoHeader + "request (" + request.length() +
                    " bytes) stored in: " + requestFilename + "\n");
        } catch (IOException ioe){
            throw new XRPCReceiverException(
                    "Failed to store request message: " +
                    ioe.getMessage());
        }

		generateQuery(requestFilename, queryFilename, request);
		execAndSend(queryFilename, sockOut);
		DEBUG(infoHeader + "DONE: " + sock + "\n\n");

        try{
            if(opts.getOption("remove").isPresent()){
                try {
                    File fQ = new File(queryFilename);
                    File fR = new File(requestFilename);

                    String arg = opts.getOption("remove").getArgument();
                    if(arg.equals("request")) {
                        fQ.delete();
                        DEBUG(infoHeader + "request file \"" +
                                requestFilename + "\" deleted\n");
                    } else if(arg.equals("query")) {
                        fR.delete();
                        DEBUG(infoHeader + "query file \"" +
                                queryFilename + "\" deleted\n");
                    } else if(arg.equals("all")) {
                        fQ.delete();
                        fR.delete();
                        DEBUG(infoHeader + "query file \"" +
                                queryFilename + "\" deleted\n");
                        DEBUG(infoHeader + "request file \"" +
                                requestFilename + "\" deleted\n");
                    }
                } catch (Exception e) {
                    System.out.println(warnHeader +
                            "failed to delete temporary file(s):");
                    e.printStackTrace();
                }
            }
        } catch (OptionsException oe){
            System.out.println(warnHeader +
                    "caught OptionsException: " + oe.getMessage());
        }
	}

	public void run()
	{
		String warnHeader = "WARNING: ("+getName()+") run(): ";
		String errHeader = "ERROR: ("+getName()+") run(): ";

		BufferedReader sockIn = null;
		BufferedWriter sockOut = null;

        try{
            sockIn = new BufferedReader(new
                    InputStreamReader(sock.getInputStream()));
            sockOut = new BufferedWriter(new
                    OutputStreamWriter(sock.getOutputStream()));

            String reqMsg = XRPCHTTPConnection.receive(sockIn, XRPCWrapper.XRPCD_CALLBACK);
            handleXRPCReq(reqMsg, sockOut);
        } catch (XRPCException xe){
            String faultMsg = "", httpHeader = "";

            try{
                if (xe instanceof XRPCSenderException) {
                    faultMsg = XRPCMessage.SOAP_FAULT(soapPrefix+":Sender", xe.getMessage());
                    httpHeader = XRPCHTTPConnection.HTTP_ERR_404_HEADER;
                } else if (xe instanceof XRPCReceiverException) {
                    faultMsg = XRPCMessage.SOAP_FAULT(soapPrefix+":Receiver", xe.getMessage());
                    httpHeader = XRPCHTTPConnection.HTTP_ERR_500_HEADER;
                } else {
                    System.err.println(errHeader + "caught exception:");
                    xe.printStackTrace();
                    sockOut.close();
                    sockIn.close();
                    sock.close();
                    System.exit(1);
                }

                XRPCHTTPConnection.send(sockOut, httpHeader, faultMsg);
                System.err.println(errHeader + "caught exception:");
                xe.printStackTrace();
                System.err.println(errHeader + "sent SOAP Fault message:");
                DEBUG(httpHeader + faultMsg);
            } catch (IOException ioe){
                System.err.println(errHeader + "caught exception:");
                ioe.printStackTrace();
            }
        } catch (IOException ioe){
            System.err.println(errHeader + "caught exception:");
            ioe.printStackTrace();
        } finally {
            try{
                sockOut.close();
                sockIn.close();
                sock.close();
            } catch (IOException ioe) {
                System.err.println(warnHeader +
                        "caught exception in FINAL block:");
                ioe.printStackTrace();
            }
        }
    }
}
