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

import nl.cwi.monetdb.xquery.util.*;

/**
 * The XRPC wrapper is a SOAP service handler that stores the incoming
 * SOAP XRPC request messages in a temporary location, generates an
 * XQuery query for this request, and executes it on an XQuery
 * processor.
 *
 * The generated query is crafted to compute the result of a Bulk XRPC
 * by calling the requested function on the parameters found in the
 * message, and to generate the SOAP response message in XML using
 * element construction.
 *
 * @author Ying Zhang <Y.Zhang@cwi.nl>
 * @version 0.1
 */

public class XRPCWrapper {
	public static final int MIN_BUFSIZE = 128;
	public static final String FILE_SEPARATOR =
		System.getProperty("file.separator");
	public static final String DEFAULT_ROOTDIR =
		System.getProperty("java.io.tmpdir") + FILE_SEPARATOR;

	public static final String XRPC_WRAPPER_VERSION = "0.1";
	public static final String XRPCD_CALLBACK = "/xrpc";
	public static final String DEFAULT_PORT = "50002";
	public static final String WF_FILE = "wrapper_functions.xq";
    public static final String WELCOME_MSG =
            "# XRPC Wrapper v" + XRPC_WRAPPER_VERSION + "\n" +
            "# Copyright (c) 1993-2007, CWI. All rights reserved.\n\n";

    CmdLineOpts opts;

	XRPCWrapper(CmdLineOpts o)
    {
        opts = o;
    }

	private void run()
	{
		ServerSocket server = null;

		try {
            int port = Integer.parseInt(
                    opts.getOption("port").getArgument());
			server = new ServerSocket(port);
			if(!opts.getOption("quiet").isPresent()){
				System.out.println(
						"# XRPC Wrapper v" + XRPC_WRAPPER_VERSION + "\n" +
						"# Copyright (c) 1993-2007, CWI. All rights reserved.\n" +
						"# Listening on port " + port + "\n" +
						"# Type Ctrl-C to stop\n");
			}

			String toFile = opts.getOption("rootdir").getArgument()+WF_FILE;

            /* Extract the XQuery module file to a temporary directory. */
			Extract.extractFile(WF_FILE, toFile);
            if(opts.getOption("debug").isPresent()) {
                System.out.println("# XQuery module file \"" + WF_FILE +
                        "\" extracted to \"" + toFile + "\"");
            }
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

		for(;;){ /* Run server for ever, until someone kills it */
			try{
				Socket clntsock = server.accept();
				XRPCWrapperWorker worker =
					new XRPCWrapperWorker(clntsock, opts);
				worker.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

    private static CmdLineOpts initCmdLineOpts()
    {
		CmdLineOpts copts = new CmdLineOpts();

        try{
            /* arguments which take exactly one argument */
            copts.addOption("c", "command", CmdLineOpts.CAR_ONE, null,
                    "This option is MANDATORY!  This option specifies " +
                    "the command for executing the XQuery engine and all " +
                    "options that should be passed to the XQuery engine. " +
                    "The command and all options MUST be specified in " +
                    "ONE string.");
            /* For example:
             *      --command "galax-run -serialize wf"
             *      --command "java -cp <pathto>/saxon8.jar net.sf.saxon.Query"
             */
            copts.addOption("p", "port", CmdLineOpts.CAR_ONE, DEFAULT_PORT,
                    "The port number to listen to\n" +
                    "(dflt: " + DEFAULT_PORT + ")");
            copts.addOption("r", "rootdir", CmdLineOpts.CAR_ONE, DEFAULT_ROOTDIR,
                    "The root directory to store temporary files\n" +
                    "(dflt: " + DEFAULT_ROOTDIR + ").");
            copts.addOption("R", "remove", CmdLineOpts.CAR_ONE, null,
                    "Remove the temporary files (<request|query|all>) " +
                    "that contain the XRPC request message " +
                    "(--remove request) and/or the generated XQuery " +
                    "query (--remove query) after a request has been " +
                    "handled.");

            /* arguments which have no argument(s) */
            copts.addOption("d", "debug", CmdLineOpts.CAR_ZERO, null,
                    "Turn on DEBUG mode to get more information printed.");
            copts.addOption("h", "help", CmdLineOpts.CAR_ZERO, null,
                    "Print this help message.");
            copts.addOption("q", "quiet", CmdLineOpts.CAR_ZERO, null,
                    "Suppress printing the welcome header.");
            copts.addOption("v", "version", CmdLineOpts.CAR_ZERO, null,
                    "Print version number and exit.");
        } catch (OptionsException oe) {
            System.err.println(WELCOME_MSG);
            System.err.println("Internal error: " + oe.getMessage());
            System.exit(1);
        }
        return copts;
    }

    private static void parseOptions(String[] args, CmdLineOpts copts)
    {
        String usage = WELCOME_MSG +
            "Usage: java -jar xrpcwrapper.jar [options]\n" +
            "OPTIONS:\n" + copts.produceHelpMessage();
        try{
            copts.processArgs(args);
            if (copts.getOption("help").isPresent() ||
                    copts.getOption("version").isPresent()) {
                System.out.print(usage);
                System.exit(0);
            } else if (!copts.getOption("command").isPresent()) {
                System.err.println(
                        "ERROR: missing mandatory option: --command\n" +
                        "Don't know how to execute the XQuery " +
                        "engine.\n\n" + usage);
                System.exit(-1);
            } else if (copts.getOption("port").isPresent()){
                CmdLineOpts.OptionContainer portOpt =
                    copts.getOption("port");
                String port = portOpt.getArgument();
                portOpt.resetArguments();
                portOpt.addArgument(port);
            } else if (copts.getOption("rootdir").isPresent()){
                CmdLineOpts.OptionContainer rootdirOpt =
                    copts.getOption("rootdir");
                String rootdir = rootdirOpt.getArgument();
                if(!rootdir.endsWith(FILE_SEPARATOR)) {
                    rootdir += FILE_SEPARATOR;
                }
                rootdirOpt.resetArguments();
                rootdirOpt.addArgument(rootdir);
            }
        } catch (OptionsException oe){
            System.out.println("Invalide option: " + oe.getMessage());
            System.out.println("\n" + usage);
            System.exit(1);
        }
    }

	public static void main (String[] args)
	{
        CmdLineOpts opts = initCmdLineOpts();
        parseOptions(args, opts);

		XRPCWrapper wrapper = new XRPCWrapper(opts);
		wrapper.run();
	}
}
