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

public class XrpcWrapper {
	public static final int MIN_BUFSIZE = 128;
	public static final int DEFAULT_BUFSIZE = 8192;

	public static final String XRPC_WRAPPER_VERSION = "0.1";
	public static final String DEFAULT_PORT = "50002";
	public static final String WF_FILE = "wrapper_functions.xq";
	public static final String FILE_SEPARATOR =
		System.getProperty("file.separator");
	public static final String DEFAULT_ROOTDIR =
		System.getProperty("java.io.tmpdir") + FILE_SEPARATOR;

	private static final String COMMAND_GALAX=
		"galax-run -serialize wf";
	private static final String COMMAND_SAXON=
		"java -cp /ufs/zhang/saxon8-9j/saxon8.jar net.sf.saxon.Query";


	XrpcWrapper(){}

	private void run(CmdLineOpts opts)
	{
		ServerSocket server = null;

		try {
			int port = Integer.parseInt(
					opts.getOption("port").getArgument());
			server = new ServerSocket(port);
			if(!opts.getOption("quiet").isPresent()){
				System.out.println(
						"# XRPC Wrapper " + XRPC_WRAPPER_VERSION + "\n" +
						"# Copyright (c) 1993-2007, CWI. All rights reserved.\n" +
						"# Listening on port " + port + "\n" +
						"# Type Ctrl-C to stop\n");
			}

			String toFile = opts.getOption("rootdir").getArgument()+WF_FILE;
			extractFileFromJar(opts, WF_FILE, toFile);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

		for(;;){
			try{
				Socket clntsock = server.accept();
				XrpcWrapperWorker worker =
					new XrpcWrapperWorker(clntsock, opts);
				worker.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void extractFileFromJar(CmdLineOpts opts,
			String fromFile,
			String toFile)
		throws Exception
	{
		String infoHeader = "INFO: XrpcWrapper.extractFileFromJar(): ";

		char[] cbuf = new char[DEFAULT_BUFSIZE];
		int ret = 0;

		InputStream is = getClass().getResourceAsStream(fromFile);
		if(is == null) {
			throw new Exception("File " + fromFile +
					" does not exist in the JAR file.");
		}

		BufferedReader reader = new BufferedReader
			(new InputStreamReader(is));
		FileWriter writer = new FileWriter(toFile, false);

		ret = reader.read(cbuf, 0, DEFAULT_BUFSIZE);
		while(ret > 0){
			writer.write(cbuf, 0, ret);
			ret = reader.read(cbuf, 0, DEFAULT_BUFSIZE);
		}
		reader.close();
		writer.close();

		/* TODO: remove the extracted file during shut-down.  For
		 *       this we need to catch Ctrl-C. */
		if(opts.getOption("debug").isPresent()){
			System.out.println(infoHeader + fromFile + " extracted to " + toFile);
		}
	}

	public static void main (String[] args)
		throws Exception
	{
		String errHeader = "ERROR: XrpcWrapper.main(): ";

		CmdLineOpts copts = new CmdLineOpts();
		/* arguments which take exactly one argument */
		copts.addOption("x", "command", CmdLineOpts.CAR_ONE, null,
				"How to executed the XQuery engine. " +
				"Specify command + options in *one* string");
		copts.addOption("d", "dump", CmdLineOpts.CAR_ONE, "no",
				"Dump the XRPC request/response message.");
		copts.addOption("e", "engine", CmdLineOpts.CAR_ONE, null,
				"Specify which XQuery engine to use.");
		copts.addOption("k", "keep", CmdLineOpts.CAR_ONE, null,
				"Do not remove the temporary files that contain the " +
				"XRPC request message and/or the generated XQuery " +
				"query after a request has been handled.");
		copts.addOption("p", "port", CmdLineOpts.CAR_ONE, DEFAULT_PORT,
				"The port number to listen to.");
		copts.addOption("r", "rootdir", CmdLineOpts.CAR_ONE,
				DEFAULT_ROOTDIR,
				"The root directory to store temporary files.");
		/* arguments which have no argument(s) */
		copts.addOption("D", "debug", CmdLineOpts.CAR_ZERO, null,
				"Turn on DEBUG mode.");
		copts.addOption("h", "help", CmdLineOpts.CAR_ZERO, null,
				"This help screen.");
		copts.addOption("q", "quiet", CmdLineOpts.CAR_ZERO, null,
				"Suppress printing the welcome header.");
		copts.addOption("t", "timing", CmdLineOpts.CAR_ZERO, null,
				"Display time measurements.");
		copts.addOption("v", "version", CmdLineOpts.CAR_ZERO, null,
				"Display version number and exit.");

		/* process the command line arguments */
		copts.processArgs(args);
		if (copts.getOption("help").isPresent()) {
			System.out.print(
					"Usage java XrpcWrapper\n" +
					"           [-x command_STRING] " + 
					"[-d request|response] [-k request|query|both] " +
					"[-p port] [-r rootdir]\n" +
					"           [-D] [-h] [-q] [-t] [-v]\n" +
					"or using long option equivalents:\n" +
					"--command --dump --keep --port --rootdir\n" +
					"--debug --help --quiet --timing " +
					"--version.\n" +
					"\n" +
					"The option -c (--command) is obligatory.  " +
					"This option specifies the command, " +
					"together with the options, " +
					"with which the XQuery engine can be executed " +
					"with the XQuery query being stored in a file.\n" +
					"If no port is given, 50002 is assumed.\n" +
					"\n" +
					"OPTIONS\n" + copts.produceHelpMessage() );
			System.exit(0);
		} else if (copts.getOption("version").isPresent()) {
			System.out.println("XRPC Wrapper version " + XRPC_WRAPPER_VERSION);
			System.exit(0);
		} else if (copts.getOption("engine").isPresent()){
			CmdLineOpts.OptionContainer engineOpt =
				copts.getOption("engine");
			CmdLineOpts.OptionContainer commandOpt =
				copts.getOption("command");

			String engine = engineOpt.getArgument().toLowerCase();
			if(engine.equals("saxon")){
				commandOpt.addArgument(COMMAND_SAXON);
			} else if (engine.equals("galax")){
				commandOpt.addArgument(COMMAND_GALAX);
			} else {
				System.err.println(errHeader + "unknown engine: " +
						engine);
			}
			commandOpt.setPresent();
		} else if (!copts.getOption("command").isPresent()) {
			System.err.println(
					errHeader + "missing mandatory option: --command\n" +
					errHeader + "don't know how to execute the XQuery engine.\n" +
					errHeader + "Use --help to get more information.\n");
			System.exit(-1);
		} else if (copts.getOption("rootdir").isPresent()){
			CmdLineOpts.OptionContainer rootdirOpt =
				copts.getOption("rootdir");
			String rootdir = rootdirOpt.getArgument();
			if(!rootdir.endsWith(FILE_SEPARATOR)) {
				rootdir += FILE_SEPARATOR;
				rootdirOpt.resetArguments();
				rootdirOpt.addArgument(rootdir);
			}
		}

		XrpcWrapper wrapper = new XrpcWrapper();
		wrapper.run(copts);
	}
}
