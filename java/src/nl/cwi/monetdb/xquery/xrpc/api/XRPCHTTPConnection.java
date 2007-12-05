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

package nl.cwi.monetdb.xquery.xrpc.api;

import java.io.*;
import java.net.*;

/**
 * @author Ying Zhang <Y.Zhang@cwi.nl>
 * @version 0.1
 */

public class XRPCHTTPConnection {
    public static final String HTTP_OK_HEADER =
        "HTTP/1.1 200 OK\r\n" +
        "Content-type: text/xml; charset=\"utf-8\"\r\n\r\n";
    public static final String HTTP_ERR_404_HEADER =
        "HTTP/1.1 404 Bad Request\r\n" +
        "Content-type: text/xml; charset=\"utf-8\"\r\n\r\n";
    public static final String HTTP_ERR_500_HEADER =
        "HTTP/1.1 500 Internal Server Error\r\n" +
        "Content-type: text/xml; charset=\"utf-8\"\r\n\r\n";

    /* Sends XRPC request messages and SOAP Fault messages.
     *
     * It is up to the caller to catch the possible exception and print
     * proper error message.  It is also up to the caller to close the
     * socket and the associated streams. */
    public static void send(BufferedWriter socketWriter,
                            String httpHeader,
                            String xrpcMessage)
        throws Exception
    {
        socketWriter.write(httpHeader);
        socketWriter.write(xrpcMessage);
        socketWriter.flush();
    }

    /* Receives XRPC request messages.
     *
     * Returns: the received XRPC message
     *
     * It is up to the caller to catch the possible exception and print
     * proper error message.  It is also up to the caller to close the
     * socket and the associated streams. */
    public static String receive(BufferedReader socketReader,
                                 String reqURI)
        throws XRPCException
    {
        int contentLength = 0;
        String ln = null;
        boolean foundPostHeader = false;
        boolean foundClHeader = false;
        boolean foundHostHeader = false;
        
        /* read the HTTP header */
        try{
            while( ((ln = socketReader.readLine()) != null) &&
                   (ln.length() > 0) ){
                if(ln.startsWith("POST")){
                    if( !ln.startsWith(reqURI, 5) ){
                        throw new XRPCSenderException(
                                "Unsupported Request: \"" + ln + "\"");
                    }
                    foundPostHeader = true;
                } else if(ln.startsWith("Content-Length:")) {
                    contentLength = Integer.parseInt(ln.substring(16));
                    foundClHeader = true;
                } else if(ln.startsWith("Host:")) {
                    /* TODO: save the value of Host for later use??? */
                    foundHostHeader = true;
                }
            }
        } catch(NumberFormatException nfe) {
            throw new XRPCSenderException("Invalid value of " +
                    "\"Content-Length\": \"" +
                    ln.substring(16) + "\": " + nfe.getMessage());
        } catch (IOException ioe) {
            throw new XRPCSenderException(
                    "Could not read HTTP request header.");
        }

        if(!foundPostHeader){
            throw new XRPCSenderException("HTTP header does not contain " +
                    "a \"POST\" method definition.");
        } else if (!foundClHeader){
            throw new XRPCSenderException("HTTP header does not contain " +
                    "the mandatory \"Content-Length\" field.");
        } else if (!foundHostHeader){
            throw new XRPCSenderException("HTTP header does not contain " +
                    "the mandatory \"Host\" field.");
        }

        /* read the XRPC request message */
        StringBuffer reqMsg = new StringBuffer(contentLength + 10);
        ln = null;
        try{
            while( (reqMsg.length() < contentLength) &&
                   ((ln = socketReader.readLine()) != null) ){
                reqMsg.append(ln).append('\n');
            }
        } catch (IOException e) {
            throw new XRPCReceiverException(
                    "Failed to receive request.");
        }

        if(reqMsg.length() != contentLength){
            throw new XRPCReceiverException(
                    "Failed to receive all request: " +
                    contentLength + "bytes expected, " +
                    reqMsg.length() + " bytes received.");
        }

        return reqMsg.toString();
    }

    /* Send an XRPC request, returns the XRPC response message or the
     * SOAP Fault message */
    public static StringBuffer sendReceive(String server,
                                           String request)
        throws Exception
    {
        URL url = new URL(server);
        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
        httpConn.setDoInput(true);
        httpConn.setDoOutput(true);
        httpConn.setUseCaches(false);
        httpConn.setRequestMethod("POST");
        httpConn.setRequestProperty("Content-Type",
                                    "application/x-www-form-urlencoded");
        httpConn.setRequestProperty("Content-Length", request.length()+"");
        httpConn.connect();

        /* Send POST output. */
        DataOutputStream printout = new
            DataOutputStream(httpConn.getOutputStream());
        printout.writeBytes(request);
        printout.flush ();

        /* Get response data. */
        InputStreamReader isReader;
        StringBuffer response = new StringBuffer(16384);
        if(httpConn.getResponseCode() != HttpURLConnection.HTTP_OK){
            /* Read the SOAP Fault message. */
            isReader = new InputStreamReader(httpConn.getErrorStream());
        } else {
            /* Read the response message, which can also be a SOAP Fault
             * message. */
            isReader = new InputStreamReader(httpConn.getInputStream());
        }
        int c;
        while( (c = isReader.read()) >= 0) response.append((char)c);
        isReader.close();
        printout.close();
        return response;
    }
}
