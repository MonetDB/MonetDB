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

package nl.cwi.monetdb.xquery.xrpc.client;

import java.io.*;
import java.net.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;
import javax.xml.xpath.*;
import org.w3c.dom.*;
import org.xml.sax.*;

import nl.cwi.monetdb.util.*;
import nl.cwi.monetdb.xquery.xrpc.api.*;

/**
 * This is an XRPC client program that can be used to test the XRPC
 * Wrapper.
 *
 * This client can call all XQuery functions defined in
 * 'xrpcwrapper_testfunctions.xq'.  The function to call MUST be
 * specified using a command line option.
 *
 * By default, the call is sent to  the XRPC Wrapper running on
 * localhost:50002.  If the XRPC Wrapper is running on a remote host,
 * please take care that the XQuery module file
 * 'xrpcwrapper_testfunctions.xq' is stored in a location that is
 * accessable for the XRPC Wrapper and use the command line option
 * "--location" to change the default location of this file.
 *
 * This client uses functions provided by
 * nl.cwi.monetdb.xquery.util.XRPCMessage to construct the request
 * messages and uses the function 'sendReceive' provided by
 * nl.cwi.monetdb.xquery.util.XRPCHTTPConnection to send the request and
 * return the response message.
 *
 * @author Ying Zhang <Y.Zhang@cwi.nl>
 * @version 0.1
 */

public class XRPCTestClient {
	private static final String FILE_SEPARATOR =
		System.getProperty("file.separator");
	private static final String DEFAULT_ROOTDIR =
		System.getProperty("java.io.tmpdir") + FILE_SEPARATOR;
	private static final String XRPCD_CALLBACK = "/xrpc";
	private static final String DEFAULT_SERVER = "http://localhost:50002";
    private static final String PACKAGE_PATH = "/nl/cwi/monetdb/xquery/xrpc/client/";

    private static final int AUCT = 0;
    private static final int BIB  = 1;
    private static final int PERS = 2;
    private static final int MODU = 3;
    private static final String[] FILES = {"auctions.xml",
                                           "bib.xml",
                                           "persons.xml",
                                           "xrpcwrapper_testfunctions.xq"};

    private CmdLineOpts opts;

	XRPCTestClient()
        throws Exception
    {
        opts = initCmdLineOpts();
    }

    private static CmdLineOpts initCmdLineOpts()
        throws Exception
    {
		CmdLineOpts copts = new CmdLineOpts();

        /* arguments which take exactly one argument */
        copts.addOption("f", "function", CmdLineOpts.CAR_ONE, null,
                "This option is MANDATORY!  This option should " +
                "specify one of the XQuery functions declared in " +
                "the XQuery module file " +
                "\"xrpcwrapper_testfunctions.xq\", " +
                "which will be executed." +
                "Currently, the following functions are declared: " +
                "echoVoid(); echoInteger(); echoDouble(); " +
                "echoString(); echoParam(); getPerson(); getDoc(); " +
                "firstClosedAuction(), buyerAndAuction(); " +
                "auctionOfBuyer();");
        copts.addOption("i", "iterations", CmdLineOpts.CAR_ONE, "1",
                "Number of iterations the function should be called " +
                "(dflt: 1).");
        copts.addOption("l", "location", CmdLineOpts.CAR_ONE,
                DEFAULT_ROOTDIR+FILES[MODU],
                "Location where the XQuery test module file is stored " +
                "(dflt: " + DEFAULT_ROOTDIR + FILES[MODU] + ").");
        copts.addOption("s", "server", CmdLineOpts.CAR_ONE, DEFAULT_SERVER,
                "The host URL (<host>[:port]) of the XRPC handler " +
                "(dflt: " + DEFAULT_SERVER + ").");
        copts.addOption("r", "rootdir", CmdLineOpts.CAR_ONE, DEFAULT_ROOTDIR,
                "The root directory to store temporary files " +
                "(dflt: " + DEFAULT_ROOTDIR + ").");
        copts.addOption("k", "keep", CmdLineOpts.CAR_ONE, null,
                "Do not remove the extracted temporary " +
                "(data+query) files before exit");

        /* arguments which have no argument(s) */
        copts.addOption("v", "verbose", CmdLineOpts.CAR_ZERO, null,
                "Print additional information, such as the XRPC " +
                "request/response message.");
        copts.addOption("h", "help", CmdLineOpts.CAR_ZERO, null,
                "Print this help message.");

        return copts;
    }

    private void parseOptions(String[] args)
        throws Exception
    {
        String usage = 
            "Usage: java -jar xrpcwrapper-test.jar [options]\n" +
            "OPTIONS:\n" + opts.produceHelpMessage();

        opts.processArgs(args);
        if (opts.getOption("help").isPresent()) {
            System.out.print(usage);
            System.exit(0);
        } else if (opts.getOption("server").isPresent()){
            CmdLineOpts.OptionContainer servOpt =
                opts.getOption("server");
            String serv = servOpt.getArgument();
            servOpt.resetArguments();
            servOpt.addArgument(serv);
        } else if (opts.getOption("rootdir").isPresent()){
            CmdLineOpts.OptionContainer rootdirOpt =
                opts.getOption("rootdir");
            String rootdir = rootdirOpt.getArgument();
            if(!rootdir.endsWith(FILE_SEPARATOR)) {
                rootdir += FILE_SEPARATOR;
            }
            rootdirOpt.resetArguments();
            rootdirOpt.addArgument(rootdir);
        }

        if (!opts.getOption("function").isPresent()) {
            System.err.println(
                    "ERROR: missing mandatory option: --function\n" +
                    "Don't know which function to execute.\n\n" +
                    usage);
            System.exit(-1);
        }
    }

    private void extractFiles()
    {
        String fromFile, toFile;

        try {
            for(int i = 0; i < FILES.length; i++) {
                fromFile = PACKAGE_PATH + FILES[i];
                toFile = opts.getOption("rootdir").getArgument() + FILES[i];
                Extract.extractFile(fromFile, toFile);
                if(opts.getOption("verbose").isPresent()) {
                    System.out.println("extractFiles(): \"" + FILES[i] +
                            "\" extracted to \"" + toFile + "\"");
                }
            }
        } catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void deleteFiles()
    {
        try {
            if(opts.getOption("keep").isPresent()) return;

            for(int i = 0; i < FILES.length; i++) {
                String fn = opts.getOption("rootdir").getArgument() + FILES[i];
                new File(fn).delete();
                if(opts.getOption("verbose").isPresent()) {
                    System.out.println("deleteFiles(): \"" +
                            fn + "\" deleted");
                }
            }
        } catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }
    }

    private String generateRequestMessage()
        throws Exception
    {
        String method = opts.getOption("function").getArgument();
        String rootdir = opts.getOption("rootdir").getArgument();

        int arity = -1;
        int iterc = 1;
        if(opts.getOption("iterations").isPresent()){
            iterc = Integer.parseInt(
                    opts.getOption("iterations").getArgument());
        }
    
        StringBuffer callBody = new StringBuffer(8192);
        for(int i = 0; i < iterc; i++){
            if(method.equals("echoVoid")) {

                /* echoVoid() */
                arity = 0;
                callBody.append(XRPCMessage.XRPC_CALL(null));

            } else if (method.equals("echoInteger")) {

                /* echoInteger($v as xs:integer*) as xs:integer */
                arity = 1;
                callBody.append(XRPCMessage.XRPC_CALL(
                        XRPCMessage.XRPC_SEQ(
                            XRPCMessage.XRPC_ATOM("integer", "234") )));

            } else if (method.equals("echoDouble")) {

                /* echoDouble($v as xs:double*) as xs:double */
                arity = 1;
                callBody.append(XRPCMessage.XRPC_CALL(
                        XRPCMessage.XRPC_SEQ(
                            XRPCMessage.XRPC_ATOM("double", "234.43") )));

            } else if (method.equals("echoString")) {

                /* echoString($v as xs:string*) as xs:string */
                arity = 1;
                callBody.append(XRPCMessage.XRPC_CALL(
                        XRPCMessage.XRPC_SEQ(
                            XRPCMessage.XRPC_ATOM("string", "Hello") )));

            } else if (method.equals("echoParam")) {

                /* echoParam($v1 as xs:double*, $v2 as item()*) as node() */
                arity = 2;
                callBody.append(XRPCMessage.XRPC_CALL(
                        XRPCMessage.XRPC_SEQ(
                            XRPCMessage.XRPC_ATOM("integer", "23") +
                            XRPCMessage.XRPC_ATOM("double", "45.1")
                        ) +
                        XRPCMessage.XRPC_SEQ(
                            XRPCMessage.XRPC_ELEMENT("<hello><world/></hello>") +
                            XRPCMessage.XRPC_ATOM("string", "hello") +
                            XRPCMessage.XRPC_ELEMENT("<foo><bar/></foo>")
                        ) ));

            } else if (method.equals("getPerson")) {

                /* getPerson($personDoc as xs:string, $pid as xs:string) */
                arity = 2;
                callBody.append(XRPCMessage.XRPC_CALL(
                        XRPCMessage.XRPC_SEQ(
                            XRPCMessage.XRPC_ATOM("string", rootdir+FILES[PERS]) ) +
                        XRPCMessage.XRPC_SEQ(
                            XRPCMessage.XRPC_ATOM("string", "person66")
                        ) ));

            } else if (method.equals("getDoc")) {

                /* getDoc($doc as xs:string) as document-node()*/
                arity = 1;
                callBody.append(XRPCMessage.XRPC_CALL(
                        XRPCMessage.XRPC_SEQ(
                            XRPCMessage.XRPC_ATOM("string", rootdir+FILES[BIB]) )));

            } else if (method.equals("firstClosedAuction")) {

                /* firstClosedAuction($auctionDoc as xs:string) as node()*/
                arity = 1;
                callBody.append(XRPCMessage.XRPC_CALL(
                        XRPCMessage.XRPC_SEQ(
                            XRPCMessage.XRPC_ATOM("string", rootdir+FILES[AUCT]) )));

            } else if (method.equals("buyerAndAuction")) {

                /* buyerAndAuction($personDoc as xs:string,
                 *                    $auctionDoc as xs:string) as node()*/
                arity = 2;
                callBody.append(XRPCMessage.XRPC_CALL(
                        XRPCMessage.XRPC_SEQ(
                            XRPCMessage.XRPC_ATOM("string", rootdir+FILES[PERS]) ) +
                        XRPCMessage.XRPC_SEQ(
                            XRPCMessage.XRPC_ATOM("string", rootdir+FILES[AUCT]) ) ));

            } else if (method.equals("auctionsOfBuyer")) {

                /* auctionsOfBuyer($auctionDoc as xs:string,
                 *                 $pid as xs:string) as node() */
                arity = 2;
                callBody.append(XRPCMessage.XRPC_CALL(
                        XRPCMessage.XRPC_SEQ(
                            XRPCMessage.XRPC_ATOM("string", rootdir+FILES[AUCT]) ) +
                        XRPCMessage.XRPC_SEQ(
                            XRPCMessage.XRPC_ATOM("string", "person66")
                        ) ));

            } else {
                throw new Exception("generateRequestMessage(): " +
                        "unknow function " + method);
            }
        }

        return XRPCMessage.XRPC_REQUEST(
                "xrpcwrapper-testfunctions",
                opts.getOption("location").getArgument(),
                method,
                arity,
                iterc,
                false,
                callBody.toString());
    }

    private void extractResults(StringBuffer response)
        throws Exception
    {
        String msg = response.toString();
        
        /* A simple check to see if the response is a SOAP Fault message
         * or not */
        String soapPrefix = XRPCMessage.getNamespacePrefix(
                XRPCMessage.XRPC_MSG_TYPE_RESP, msg,
                XRPCMessage.SOAP_NS);
        if(response.indexOf(soapPrefix+":Fault") > 0) {
            System.out.println(response);
            return;
        }

        /* Find and check the prefix of the XRPC namespace URI */
        int i = response.indexOf(":sequence");
        if (i < 0) {
            throw new Exception("Invalid response message: " +
                    "no sequence element found");
        }
        int j = i - 1;
        while (response.charAt(j) != '<') j--;
        String xrpcPrefix = response.substring(j+1, i);
        /* check the namespace */
        String nsURI = XRPCMessage.getNamespaceURI(msg, xrpcPrefix);
        if(!nsURI.equals(XRPCMessage.XRPC_NS)){
            throw new Exception("Expected namespace URI: " +
                    XRPCMessage.XRPC_NS + "; " +
                    "found namespace URI: " + nsURI);
        }

        NamespaceContextImpl namespaceContext = new NamespaceContextImpl();
        namespaceContext.add(soapPrefix, XRPCMessage.SOAP_NS);
        namespaceContext.add(xrpcPrefix, XRPCMessage.XRPC_NS);

        XPathFactory factory = XPathFactory.newInstance();
        XPath xPath = factory.newXPath();
        xPath.setNamespaceContext(namespaceContext);

        String xPathExpr = "/" + soapPrefix + ":Envelope/" +
                                 soapPrefix + ":Body/" +
                                 xrpcPrefix + ":response/" +
                                 xrpcPrefix + ":sequence/child::*";
        InputSource inputSource = new InputSource(new StringReader(msg));
        NodeList nodeList = (NodeList) xPath.evaluate(xPathExpr,
                inputSource, XPathConstants.NODESET);
        
                TransformerFactory tf = TransformerFactory.newInstance();
                Transformer serializer = tf.newTransformer();
                serializer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");

                StreamResult strRes = new StreamResult(System.out);

        for(i = 0; i < nodeList.getLength(); i++){
            NodeList children = nodeList.item(i).getChildNodes();
            for(j = 0; j < children.getLength(); j++){
                DOMSource domSource = new DOMSource(children.item(j));
                serializer.transform(domSource, strRes);
                System.out.toString();
                System.out.println();
            }
        }
    }

    private void doXRPCCall()
        throws Exception
    {
        String reqMsg = generateRequestMessage();
        if(opts.getOption("verbose").isPresent()){
            System.out.println("doXRPCCall(): " +
                    "request message to send: \n");
            System.out.print(reqMsg.toString());
        }

        StringBuffer respMsg = XRPCHTTPConnection.sendReceive(
                opts.getOption("server").getArgument() + XRPCD_CALLBACK,
                reqMsg);
        if(opts.getOption("verbose").isPresent()){
            System.out.println("doXRPCCall(): " +
                    "response message received: \n");
            System.out.println(respMsg.toString());
        }

        extractResults(respMsg);
    }

    private void callAllFunctions()
        throws Exception
    {
        String[] functions = {
            "echoVoid",
            "echoInteger",
            "echoDouble",
            "echoString",
            "echoParam",
            "getPerson",
            "getDoc",
            "firstClosedAuction",
            "buyerAndAuction",
            "auctionsOfBuyer"};

        CmdLineOpts.OptionContainer funcOpt = opts.getOption("function");
        for(int i = 0; i < functions.length; i++){
            funcOpt.resetArguments();
            funcOpt.addArgument(functions[i]);
            System.out.println("\n********** callAllFunctions(): " +
                    "calling function \"" + functions[i] + "\" **********");
            doXRPCCall();
        }
    }

    public static void main (String[] args)
    {
        try{
            XRPCTestClient tc = new XRPCTestClient();
            tc.parseOptions(args);

            /* Extract data/query files to the tmp directory */
            tc.extractFiles();

            if(tc.opts.getOption("function").getArgument().equals("all"))
                tc.callAllFunctions();
            else
                tc.doXRPCCall();

            /* delete temporiry files, if necessary */
            tc.deleteFiles();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
