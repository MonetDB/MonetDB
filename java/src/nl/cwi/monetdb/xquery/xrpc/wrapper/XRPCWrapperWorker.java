/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
 * Copyright August 2008-2010 MonetDB B.V.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.xquery.xrpc.wrapper;

import java.io.*;
import java.net.*;
import java.util.*;
import javax.xml.xpath.*;
import org.w3c.dom.*;
import org.xml.sax.*;

import nl.cwi.monetdb.util.*;
import nl.cwi.monetdb.xquery.xrpc.api.*;

public class XRPCWrapperWorker extends Thread {
	/** Simple counter for XRPC worker threads. */
	private static int tid = 0;

	private Socket sock;
	private CmdLineOpts opts;
	private String rootdir;
	private String logdir;
	private String soapPrefix;
	private String xrpcPrefix;
	private String wscoorPrefix;
	private int contentLength;
	private boolean debug;
	private String hostport;
	private long xrpc_seqnr;
	private boolean repeatable;
	private boolean trace;
	private long xrpc_start;
	private long xrpc_timeout;

	XRPCWrapperWorker(
			Socket s,
			CmdLineOpts o,
			String hostport,
			long seqnr,
			String rootdir,
			boolean debug)
	{
		super("XRPCWrapperWorkerThread-" + tid++);

		sock = s;
		opts = o;
		this.hostport = hostport;
		xrpc_seqnr = seqnr;
		this.rootdir = rootdir;
		this.debug = debug;

		soapPrefix = "";
		xrpcPrefix = "";
		wscoorPrefix = "";
		repeatable = false;
		trace = false;
		xrpc_start = new Date().getTime();
		xrpc_timeout = 0;

		logdir = rootdir + "logs" + XRPCMessage.DIR_SEP;
		File logdirF = new File(logdir);
		if(!logdirF.exists()) logdirF.mkdirs();
	}

	/**
	 * Generate an XQuery query for the request and write the query
	 * to a file.
	 */
	private void generateQuery(
			String requestFilename,
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
			wscoorPrefix = XRPCMessage.getNamespacePrefix(
					XRPCMessage.XRPC_MSG_TYPE_REQ, request,
					XRPCMessage.XRPC_NS);
		} catch (XRPCException xe){
			throw new XRPCSenderException(xe.getMessage());
		}
		if(soapPrefix == null || xrpcPrefix == null) {
			throw new XRPCException(
					"XRPC request message nog well-formed: " +
					"declaration of the XRPC or the SOAP namespace " +
					"not found.");
		}

		NamespaceContextImpl nsCtx = new NamespaceContextImpl();
		nsCtx.add(soapPrefix, XRPCMessage.SOAP_NS);
		nsCtx.add(xrpcPrefix, XRPCMessage.XRPC_NS);

		XPathFactory factory = XPathFactory.newInstance();
		XPath xPath = factory.newXPath();
		xPath.setNamespaceContext(nsCtx);

		String xqModule = null, xqMethod = null, xqLocation = null;
		String xqMode = null, caller = null, qid = null;
		int arity = -1;
		try {
			if(wscoorPrefix != null) {
				InputSource is = new InputSource(new
						StringReader(request));
				NodeList wsCoorCtx = (NodeList) xPath.evaluate(
						"/" + soapPrefix + ":Envelope/" +
							  soapPrefix + ":Header/" +
							  wscoorPrefix + ":CoordinationContext/*",
						is, XPathConstants.NODESET);
				for(int i = 0; i < wsCoorCtx.getLength(); i++) {
					Node n = wsCoorCtx.item(i);
					String nname = n.getNodeName();
					if(nname.equals("Identifier")) {
						qid = n.getFirstChild().getNodeValue();
					} else if(nname.equals("Expires")) {
						xrpc_timeout = Integer.parseInt(
								n.getFirstChild().getNodeValue());
					}
				}
			}

			NamedNodeMap attrs = XRPCMessage.getNodeAttributes(xPath,
					request, "/" + soapPrefix + ":Envelope/" +
								   soapPrefix + ":Body/" +
								   xrpcPrefix + ":request");
			Node n = attrs.getNamedItem(xrpcPrefix+":module");
			if(n != null) xqModule = n.getNodeValue();

			n = attrs.getNamedItem(xrpcPrefix+":method");
			if(n != null) xqMethod = n.getNodeValue();

			n = attrs.getNamedItem(xrpcPrefix+":location");
			if(n != null) xqLocation = n.getNodeValue();

			n = attrs.getNamedItem(xrpcPrefix+":arity");
			if(n != null) arity = Integer.parseInt(n.getNodeValue());

			n = attrs.getNamedItem(xrpcPrefix+":mode");
			if(n != null) xqMode = n.getNodeValue();
			if(xqMode.indexOf("trace") >= 0) trace = true;
			if(xqMode.indexOf("repeatable") >= 0) repeatable = true;

			n = attrs.getNamedItem(xrpcPrefix+":caller");
			if(n != null) caller = n.getNodeValue();
		} catch (Exception e) {
			throw new XRPCSenderException("Error occurred when " +
					"parsing the WS CoordinationContext element and " +
					"the XRPC request element.\n" +
					"Caught " + e.getClass().getName() + ": " +
					e.getMessage());
		}
		if(xqModule == null || xqMethod == null || xqLocation == null ||
		   arity < 0 || (trace && caller == null) ||
		   (repeatable && wscoorPrefix == null)){
			throw new XRPCSenderException(
					"One or more of the required elements/attributes " +
					"were not found.  The XRPC request element must " +
					"have the following attributes: module, method, " +
					"location, artiy.  In trace mode, the XRPC " +
					"request element must have a caller attribute.  " +
					"In repeatable mode, the request must contain a " +
					"WS CoordinationContext in SOAP Header.");
		}
		caller = caller == null ? "query" : caller;

		try{ 
			FileWriter fileOut = new FileWriter(queryFilename, false);

			fileOut.write(
					"import module namespace modfunc = " +
						"\"" + xqModule + "\"" +
						" at \"" + xqLocation + "\";\n\n" +
					"import module namespace wf = \"xrpcwrapper\"" +
						" at \"" + rootdir +
							XRPCWrapper.WF_FILE + "\";\n\n" +
					"declare namespace env=\"" +
							XRPCMessage.SOAP_NS + "\";\n" +
					"declare namespace xrpc=\"" +
							XRPCMessage.XRPC_NS + "\";\n\n" +
					"declare namespace xs=\"" +
							XRPCMessage.XS_NS + "\";\n" +
					
					"<env:Envelope" +
						" xmlns:xrpc=\"" + XRPCMessage.XRPC_NS + "\" " +
						" xsi:schemaLocation=\"" +
								XRPCMessage.XRPC_NS + " " +
								XRPCMessage.XRPC_LOC + "\">" +
					"<env:Header>" +
						XRPCMessage.XRPC_PART(caller, hostport,
							xrpc_seqnr, xqMethod) +
						(repeatable ?
						XRPCMessage.XRPC_WS_QID(qid, xrpc_timeout):"")+
					"</env:Header>" +
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
			fileOut.write("))\n}" + XRPCMessage.XRPC_RES_END);
			fileOut.close();
		} catch (Exception e){
			throw new XRPCReceiverException(
					"failed to generate query for the request: " +
					"caught " + e.getClass().getName() + ": " +
					e.getMessage());
		}

		if(debug)
			System.out.println(infoHeader + "query generated in " +
					queryFilename);
	}

	/**
	 * Call the XQuery engine to execute the query and send the
	 * results back.
	 */
	private void execAndSend(
			String queryFilename,
			BufferedWriter sockOut)
		throws XRPCReceiverException
	{
		String infoHeader = "INFO: ("+getName()+") execAndSend(): ";
		String errHeader = "ERROR: ("+getName()+") execAndSend(): ";

		Process proc = null;
		BufferedReader procIn = null;
		char[] cbuf = new char[19];
		BufferedWriter resFile = null;
		String resFilename = null;

		try{
			if(trace || debug) {
				resFilename	= logdir + "res_" + xrpc_seqnr + ".xml";
				resFile = new BufferedWriter(new
						FileWriter(resFilename));
			}

			String command = opts.getOption("command").getArgument() +
				" " + queryFilename;
			if(debug) System.out.println(infoHeader + "executing: " + command);
			proc = Runtime.getRuntime().exec(command);
			procIn = new BufferedReader(new
					InputStreamReader(proc.getInputStream()));
			cbuf[0] = (char)procIn.read();
		} catch (IOException ioe){
			System.out.println(infoHeader + "caught IOException " +
					"while reading proc's output:");
			ioe.printStackTrace();
			/* Don't throw exception, try to read proc's ErrorStream */
			cbuf[0] = (char)-1;
		} catch (Exception e) {
			throw new XRPCReceiverException("Caught " +
					e.getClass().getName() + ": " + e.getMessage());
		}

		if (cbuf[0] != (char)-1){
			try{
				if(debug) System.out.println(infoHeader +
						"query execution seems succeeded: " +
						"got output from the process' InputStream.\n" +
						infoHeader + "send XRPC response message:\n");

				sockOut.write(XRPCHTTPConnection.HTTP_OK_HEADER);

				/* Add the XML declaration, if necessary */
				procIn.read(cbuf, 1, 4);
				if(cbuf[0] != '<' || cbuf[1] != '?' || cbuf[2] != 'x' ||
						cbuf[3] != 'm' || cbuf[4] != 'l') {
					sockOut.write(XRPCMessage.XML_DECL);
					if(resFile != null)
						resFile.write(XRPCMessage.XML_DECL);
				}
				sockOut.write(cbuf, 0, 5);
				if(resFile != null) resFile.write(cbuf, 0, 5);

				/* search for </xrpc:participant> to add exec time */
				int i = 0; /* how many chars did we read */
				while((cbuf[0] = (char)procIn.read()) != (char) -1) {
					i = 1;
					if(cbuf[0] == '<') {
						cbuf[i++] = (char)procIn.read();
						if(cbuf[1] == '/') {
							i += procIn.read(cbuf, 2, 17);
							if(i == 19 && cbuf[2]  == 'x' &&
								cbuf[3]  == 'r' && cbuf[4]  == 'p' &&
								cbuf[5]  == 'c' && cbuf[6]  == ':' &&
								cbuf[7]  == 'p' && cbuf[8]  == 'a' &&
								cbuf[9]  == 'r' && cbuf[10] == 't' &&
								cbuf[11] == 'i' && cbuf[12] == 'c' &&
								cbuf[13] == 'i' && cbuf[14] == 'p' &&
								cbuf[15] == 'a' && cbuf[16] == 'n' &&
								cbuf[17] == 't' && cbuf[18] == '>') {
								String time_exec = "," +
									(new Date().getTime() - xrpc_start) + ",0";
								sockOut.write(time_exec,
										0, time_exec.length());
								if(resFile != null)
									resFile.write(time_exec, 0,
											time_exec.length());
							}
						}
					}
					sockOut.write(cbuf, 0, i);
					if(resFile != null) resFile.write(cbuf, 0, i);
				}

				int c = -1;
				while((c = procIn.read()) >= 0) {
					sockOut.write(c);
					if(resFile != null) resFile.write(c);
				}
				resFile.close();

				if(debug) System.out.println(infoHeader +
						"response message saved in " + resFilename);
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
				int c = -1;
				while ((c = errIn.read()) >= 0) {
					faultReason.append((char)c);
				}
				errIn.close();

				String faultMsg =
					XRPCMessage.SOAP_FAULT(soapPrefix+":Receiver",
							faultReason.toString());
				XRPCHTTPConnection.send(sockOut,
						XRPCHTTPConnection.HTTP_ERR_500_HEADER, faultMsg);
				if(resFile != null) {
					resFile.write(faultMsg);
					resFile.close();
					if(debug) System.out.println(infoHeader +
							"HTTP ERR 500 sent, SOAP Fault message saved in " +
							resFilename);
				}
			} catch (IOException ioe){
				System.err.println(errHeader + "caught exception:");
				ioe.printStackTrace();
				System.exit(1);
			}
		}

		try{
			int ret = proc.waitFor();
			if(debug) System.out.println(infoHeader +
					"query execution exits with: " + ret);
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
	 *	 query to a file
	 *  3. Call the XQuery engine to execute the query and send the
	 *	 results back
	 *
	 * @param request the XRPC request message
	 * @param sockOut the response writer
	 */
	private void handleXRPCReq(String request, BufferedWriter sockOut)
		throws XRPCException
	{
		String infoHeader = "INFO: ("+getName()+") handleXRPCReq(): ";
		String warnHeader = "WARNING: ("+getName()+") handleXRPCReq(): ";

		String requestFilename = logdir + "req_" + xrpc_seqnr + ".xml";
		String queryFilename = logdir + "query_" + xrpc_seqnr + ".xq";

		try {
			BufferedWriter fileOut = new BufferedWriter(new
					FileWriter(requestFilename, false));
			fileOut.write(request.toString());
			fileOut.close();
			if(debug) System.out.println(infoHeader + "request (" +
					request.length() + " bytes) stored in: " +
					requestFilename);
		} catch (IOException ioe){
			throw new XRPCReceiverException(
					"Failed to store request message: " +
					ioe.getMessage());
		}

		generateQuery(requestFilename, queryFilename, request);
		execAndSend(queryFilename, sockOut);
		if(debug) System.out.println(infoHeader + "DONE: " + sock + "\n");

		try{
			if(opts.getOption("remove").isPresent()){
				try {
					File fQ = new File(queryFilename);
					File fR = new File(requestFilename);

					String arg = opts.getOption("remove").getArgument();
					if(arg.equals("request")) {
						fQ.delete();
						if(debug) System.out.println(infoHeader +
								"request file \"" + requestFilename +
								"\" deleted");
					} else if(arg.equals("query")) {
						fR.delete();
						if(debug) System.out.println(infoHeader +
								"query file \"" + queryFilename +
								"\" deleted");
					} else if(arg.equals("all")) {
						fQ.delete();
						fR.delete();
						if(debug) System.out.println(infoHeader +
								"query file \"" + queryFilename +
								"\" deleted\n" +
								infoHeader + "request file \"" +
								requestFilename + "\" deleted");
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

	/**
	 * A simple HTTPD server, which only handles HTTP GET requests to
	 * files in the log directory.
	 *
	 * @param request the HTTP GET URI
	 * @param sockOut the response writer
	 */
	private void handleHttpGetReq(
			String request,
			BufferedWriter sockOut)
	{
		String infoHeader = "INFO: ("+getName()+") handleHttpGetReq(): ";
		String warnHeader = "WARNING: ("+getName()+") handleHttpGetReq(): ";

		if(debug)
			System.out.println(infoHeader + "GET URI: " + request);

		try{
			if(request.length() == 0) {
				sockOut.write(
						"HTTP/1.1 400 Bad Request\r\n" +
						"Content-Type: text/plain; charset=\"utf-8\"\r\n\r\n");
				sockOut.write("The HTTP GET request did not contain a " +
						"URI.\n");
				sockOut.flush();
				return;
			}

			if(!request.startsWith("/logs")) {
				sockOut.write(
						"HTTP/1.1 403 Forbidden\r\n" +
						"Content-Type: text/plain; charset=\"utf-8\"\r\n\r\n");
				sockOut.write("Access to file \"" + request +
						"\" denied.\n");
				sockOut.flush();
				return;
			}

			String filename = logdir + request.substring(6);
			String ctntType = "";
			if(request.endsWith(".xml"))
				ctntType = "Context-Type: text/xml; charset=\"utf-8\"\r\n";
			else if (request.endsWith(".gif"))
				ctntType = "Context-Type: image/gif\r\n";
			else {
				sockOut.write(
						"HTTP/1.1 403 Forbidden\r\n" +
						"Content-Type: text/plain; charset=\"utf-8\"\r\n\r\n");
				sockOut.write("Access to file \"" + request +
						"\" denied.\nOnly access to XML and GIF files " +
						"are allowed.\n");
				sockOut.flush();
				return;
			}

			File f = new File(filename);
			if(!f.exists()) {
				sockOut.write(
						"HTTP/1.1 404 Not Found\r\n" +
						"Content-Type: text/plain; charset=\"utf-8\"\r\n\r\n");
				sockOut.write("File \"" + request +
						"\" not found on this server.\n");
				sockOut.flush();
				return;
			}

			/* Finally, we can send the file */
			FileReader fr = new FileReader(f);
			sockOut.write("HTTP/1.1 200 OK\r\n" + ctntType + "\r\n");
			int c = -1;
			while((c = fr.read()) > 0)
				sockOut.write(c);
			sockOut.flush();
			fr.close();
		} catch (Exception e) {
			System.out.println(warnHeader);
			e.printStackTrace();
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
			if(reqMsg.startsWith("<"))
				handleXRPCReq(reqMsg, sockOut);
			else
				handleHttpGetReq(reqMsg, sockOut);
		} catch (XRPCException xe){
			String faultMsg = "", httpHeader = "";

			try{
				if (xe instanceof XRPCSenderException) {
					faultMsg = XRPCMessage.SOAP_FAULT(soapPrefix+":Sender", xe.getMessage());
					httpHeader = XRPCHTTPConnection.HTTP_ERR_400_HEADER;
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
				if(debug) System.out.println(httpHeader + faultMsg);
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

